//! main.rs — row-tracking read example, extended with `row_index` and a
//! **derived** `base_row_id` column.
//!
//! Companion to `main-mock.rs`. That file stops at reading `row_id`. This
//! one also asks the scan for `row_index` and then computes the per-file
//! `base_row_id` ourselves from the identity:
//!
//!     row_id = base_row_id + row_index     (per file)
//!
//! `base_row_id` is a file-level property that lives on each `add` action
//! in the `_delta_log/` JSON, NOT a row-level metadata column. The kernel
//! exposes `RowId`, `RowIndex`, `RowCommitVersion`, `FilePath` through
//! `MetadataColumnSpec` — there's no `BaseRowId` variant — so we back it
//! out arithmetically after the scan.
//!
//! Along the way this file is annotated for a Rust newcomer: look for
//! notes on `Arc`, `Box`, the `?` operator, trait objects, iterator
//! chaining, macros, shadowing, closures, and so on.

use delta_kernel::arrow::array::{Array, Int32Array, Int64Array, RecordBatch};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use delta_kernel::arrow::util::pretty::pretty_format_batches;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::schema::{DataType, MetadataColumnSpec, SchemaRef, StructField, StructType};
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult, Error, Snapshot};
use std::sync::Arc;
use tempfile::tempdir;

use test_utils::{create_table, engine_store_setup, read_scan};

use url::Url;

#[tokio::main]
// Procedural macro. At compile time it rewrites `async fn main` into a sync
// `fn main` that spins up a Tokio runtime and `block_on`s the original body.
// `async fn` itself returns an anonymous `Future`; something has to drive it.
async fn main() -> DeltaResult<()> {
    // `let _ = ...` discards the return value on purpose. `try_init` returns
    // `Err` if a subscriber was already installed — we don't care.
    let _ = tracing_subscriber::fmt::try_init();

    // `?` is the error-propagation operator. On `Err`, it returns from `main`
    // with that error converted into `DeltaResult`'s error type via `From`.
    let tmp_dir = tempdir()?;
    // `println!` is a MACRO, not a function (note the `!`). `{}` picks
    // `Display`, `{:?}` picks `Debug`.
    println!("Working in: {}", tmp_dir.path().display());

    // Build the logical schema: one nullable INTEGER column named "number".
    // `vec![...]` is another macro; it expands to `Vec::from([...])`-ish code.
    // Outer `Arc::new(...)` gives us shared ownership — we'll hand clones of
    // `schema` to both the data generator and the writer.
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);

    // `Url::from_directory_path` wants an absolute path. It returns `Result<Url, ()>`
    // — a unit-typed error — so we `map_err` into our richer `Error` type.
    // The closure `|_| ...` ignores the meaningless `()` payload.
    let tmp_url =
        Url::from_directory_path(tmp_dir.path()).map_err(|_| Error::generic("bad tmp path"))?;

    // Destructure a 3-tuple in one `let`. Tuples are positional; named structs
    // are another option but helpers like this often return tuples.
    let (store, engine, table_url) = engine_store_setup("row_tracking_demo", Some(&tmp_url));

    // SHADOWING: we re-bind the name `engine` to a new value (wrapped in `Arc`).
    // The old `engine` is now unreachable by that name. Shadowing is not
    // mutation — it's introducing a fresh binding that happens to reuse the
    // identifier. Common Rust pattern for "same conceptual thing, different
    // wrapping".
    let engine = Arc::new(engine);

    // CREATE TABLE with row tracking on. Same ceremony as main-mock.rs.
    // The `vec!["domainMetadata", "rowTracking"]` line flips on the writer
    // features required by the Delta spec for row tracking.
    //
    // Another shadow: `table_url` starts as the URL we handed in, and is
    // re-bound to whatever `create_table` resolved it to.
    let table_url = create_table(
        store.clone(), // `Arc::clone` — cheap, just refcount bump.
        table_url,
        schema.clone(),
        &[],                                   // partition columns — none
        true,                                  // use protocol version 3/7 (needed for row tracking)
        vec![],                                // reader features — none extra
        vec!["domainMetadata", "rowTracking"], // writer features — the magic bit
    )
    .await
    .map_err(|e| Error::generic(format!("failed to create table: {e}")))?;
    // `.await` suspends this async fn until the future resolves. Only legal
    // inside `async fn` / `async { ... }` blocks.

    // Two batches → two parquet files → two distinct `baseRowId`s:
    //   file 1 [10, 20, 30]  → baseRowId = 0, rows get row_id 0, 1, 2
    //   file 2 [40, 50]      → baseRowId = 3, rows get row_id 3, 4
    let data = generate_data(
        schema.clone(),
        vec![
            vec![int32_array(vec![10, 20, 30])],
            vec![int32_array(vec![40, 50])],
        ],
    )?;

    let commit = write_data_to_table(&table_url, engine.clone(), data).await?;
    // `assert!` is a macro — on `false`, it panics (unwinds the thread). Use
    // `assert!` for invariants, return `Err(...)` for expected failures.
    assert!(commit.is_committed(), "commit should succeed");
    println!("Commit succeeded.");

    // A `Snapshot` is "the table as of some version". Builder pattern:
    //   builder_for(url) -> SnapshotBuilder -> .build(engine)? -> Snapshot
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    // `engine.as_ref()` takes `&Arc<DefaultEngine<...>>` and produces
    // `&DefaultEngine<...>`. Many kernel APIs take `&dyn Engine`, not `Arc`.

    // Take the snapshot's logical schema (`number`) and tack on two
    // METADATA COLUMNS. These aren't stored in parquet — the scanner
    // synthesizes them per row:
    //   RowId     → Int64, globally unique per row (base + index)
    //   RowIndex  → Int64, 0-based position within this row's file
    let scan_schema = Arc::new(
        snapshot
            .schema()
            .add_metadata_column("row_id", MetadataColumnSpec::RowId)?
            .add_metadata_column("row_index", MetadataColumnSpec::RowIndex)?,
    );

    // Build a scan (= query plan) with our augmented schema.
    let scan = snapshot.scan_builder().with_schema(scan_schema).build()?;

    // Execute the scan, then pipe every batch through `with_base_row_id` to
    // append the derived column.
    //
    // Iterator chain breakdown:
    //   read_scan(...)?              -> Vec<RecordBatch>
    //   .into_iter()                 -> owning iterator (moves each batch out)
    //   .map(with_base_row_id)       -> Iterator<Item = DeltaResult<RecordBatch>>
    //   .collect::<DeltaResult<_>>() -> short-circuit: first Err wins,
    //                                   otherwise gives Ok(Vec<RecordBatch>)
    // The `::<DeltaResult<_>>` is the TURBOFISH — it tells `collect` which
    // container to build. `_` lets the compiler infer the inner type.
    let batches: Vec<RecordBatch> = read_scan(&scan, engine)?
        .into_iter()
        .map(with_base_row_id)
        .collect::<DeltaResult<_>>()?;

    // Pretty print. Expected output:
    //
    //   +--------+--------+-----------+-------------+
    //   | number | row_id | row_index | base_row_id |
    //   +--------+--------+-----------+-------------+
    //   | 10     | 0      | 0         | 0           |
    //   | 20     | 1      | 1         | 0           |
    //   | 30     | 2      | 2         | 0           |
    //   | 40     | 3      | 0         | 3           |
    //   | 50     | 4      | 1         | 3           |
    //   +--------+--------+-----------+-------------+
    println!("\n{}", pretty_format_batches(&batches)?);

    Ok(())
}

/// Wrap a `Vec<i32>` as an Arrow array behind `Arc<dyn Array>`.
///
/// Returning `Arc<dyn Array>` erases the concrete type — callers treat every
/// column uniformly regardless of whether it's Int32, Utf8, etc. This is
/// Rust's version of polymorphism via TRAIT OBJECTS.
fn int32_array(data: Vec<i32>) -> Arc<dyn Array> {
    Arc::new(Int32Array::from_iter_values(data))
}

/// Given a `RecordBatch` that already has `row_id` and `row_index` columns,
/// append a third column `base_row_id = row_id - row_index`.
///
/// Why arithmetic works: every row in a given parquet file shares the same
/// `baseRowId` (file-level), and `row_index` counts from 0 within that file,
/// so subtracting them yields the file's baseRowId for every row in it.
fn with_base_row_id(batch: RecordBatch) -> DeltaResult<RecordBatch> {
    // `column_by_name` returns `Option<&ArrayRef>`. `and_then` is the
    // monadic bind — given `Option<T>`, apply `T -> Option<U>` and flatten.
    //
    // `as_any()` + `downcast_ref::<Int64Array>()` is how you recover a
    // concrete type from a `dyn Array` trait object. Returns `Option<&T>`
    // (None if the real type doesn't match). This is Rust's explicit form
    // of a "runtime type cast".
    //
    // `ok_or_else` converts `Option<T>` -> `Result<T, E>` by calling a
    // closure on `None`. Closure form (`|| ...`) is lazy — only invoked
    // when actually needed, which matters when error construction is
    // expensive.
    let row_id = batch
        .column_by_name("row_id")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| Error::generic("row_id column missing or not Int64"))?;
    let row_index = batch
        .column_by_name("row_index")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| Error::generic("row_index column missing or not Int64"))?;

    // `(0..batch.num_rows())` is a `Range<usize>`, which implements `Iterator`.
    // `.map(...)` transforms each `i` into an `i64`.
    // `.collect::<Int64Array>()` works because `Int64Array` implements
    // `FromIterator<i64>` — `collect` discovers that impl via the target type.
    //
    // Type annotation on `base_row_id` is what disambiguates `collect`.
    let base_row_id: Int64Array = (0..batch.num_rows())
        .map(|i| row_id.value(i) - row_index.value(i))
        .collect();

    // Rebuild the schema with an extra field tacked on.
    // `batch.schema().fields()` gives `&Fields` (~= `&[Arc<Field>]`).
    // `.iter().cloned().collect()` clones each `Arc<Field>` (cheap refcount
    // bump) into a fresh `Vec<Arc<Field>>` we can push onto.
    let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
    // Third arg `false` = NOT nullable. baseRowId is always present when
    // row tracking is on; every row has a value, so we mark it non-null.
    fields.push(Arc::new(Field::new(
        "base_row_id",
        ArrowDataType::Int64,
        false,
    )));
    let schema = Arc::new(ArrowSchema::new(fields));

    // `columns()` returns `&[ArrayRef]`, so `.to_vec()` makes an owned copy
    // (still cheap — each element is an `Arc` that just bumps refcounts).
    let mut cols: Vec<Arc<dyn Array>> = batch.columns().to_vec();
    cols.push(Arc::new(base_row_id));

    // `RecordBatch::try_new` validates: column count matches schema field
    // count, each column's type matches its field's type, and all columns
    // have equal length. Any mismatch returns `Err`.
    Ok(RecordBatch::try_new(schema, cols)?)
}

/// Turn a list of "batch columns" into `ArrowEngineData` (what the writer wants).
///
/// GENERIC over `I`, with a `where` clause asserting `I` is something you can
/// iterate into, producing `Vec<Arc<dyn Array>>` per batch. That lets callers
/// pass a `Vec<Vec<...>>`, an array, or any custom iterator — no allocation
/// forced on the caller.
fn generate_data<I>(schema: SchemaRef, batches: I) -> DeltaResult<Vec<ArrowEngineData>>
where
    I: IntoIterator<Item = Vec<Arc<dyn Array>>>,
{
    // `schema.as_ref()` gets `&StructType` from `SchemaRef` (= `Arc<StructType>`).
    // `.try_into_arrow()?` is the extension method from `TryIntoArrow`.
    let arrow_schema: Arc<ArrowSchema> = Arc::new(schema.as_ref().try_into_arrow()?);
    batches
        .into_iter()
        .map(|cols| {
            // `?` inside a closure that returns `Result<...>` works fine.
            let batch = RecordBatch::try_new(arrow_schema.clone(), cols)?;
            Ok(ArrowEngineData::new(batch))
        })
        // Collecting `Iterator<Item = Result<T, E>>` into `Result<Vec<T>, E>`
        // is a classic Rust trick — early-return on first Err.
        .collect()
}

/// Open a transaction, spawn one async write per batch, then commit.
/// This is the pattern the real kernel tests use — writes run in parallel
/// as separate tokio tasks to simulate a distributed executor.
async fn write_data_to_table(
    table_url: &Url,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    data: Vec<ArrowEngineData>,
) -> DeltaResult<CommitResult> {
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;

    // Builder pattern with consuming `self` methods (`.with_data_change` takes
    // `self`, not `&mut self`, then returns a new `Transaction`). The chain
    // reads left-to-right but is really nested calls.
    //
    // `Box::new(FileSystemCommitter::new())` — `Box<dyn Committer>` is a
    // heap-allocated trait object. Boxing is needed when the caller wants a
    // trait as a value (unknown size at compile time → must live on the heap).
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_data_change(true);

    // Shared write context — Arc so every spawned task gets a cheap handle.
    let write_context = Arc::new(txn.unpartitioned_write_context()?);

    // One tokio task per batch → one parquet file per batch → one baseRowId each.
    //
    // `tokio::task::spawn` needs a `'static + Send` future, which is why we
    // `.clone()` the Arcs INSIDE the closure and then `move` them into the
    // `async move` block. No borrowed references escape the loop body.
    let tasks = data.into_iter().map(|d| {
        let engine = engine.clone();
        let ctx = write_context.clone();
        tokio::task::spawn(async move { engine.write_parquet(&d, ctx.as_ref()).await })
    });

    // `join_all` polls every task concurrently and resolves to a `Vec<JoinResult>`.
    // `into_iter().flatten()` here peels off the outer `Result` from `JoinHandle`
    // (`Result<T, JoinError>`) — if any task panicked, `flatten` drops it.
    let add_files = futures::future::join_all(tasks).await.into_iter().flatten();

    // Each `meta` is still a `DeltaResult<...>` from `write_parquet`; `?` unwraps it.
    for meta in add_files {
        txn.add_files(meta?);
    }

    // This is where the `_delta_log/NNN.json` file actually gets written.
    // The commit is atomic from the reader's POV — either the whole file
    // appears or nothing does.
    txn.commit(engine.as_ref())
}
