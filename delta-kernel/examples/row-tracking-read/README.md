# Row Tracking Read Tests — Learning Notes

A personal study guide for my contribution to `delta-kernel-rs`:

- **Commit:** `b9df6011` — `feat(tests): add read-path integration tests for row tracking (#2316)`
- **Issue:** [delta-io/delta-kernel-rs#2289](https://github.com/delta-io/delta-kernel-rs/issues/2289)
- **File changed:** `kernel/tests/row_tracking.rs` (+ small tweak in `test-utils/src/lib.rs`)
- **Local path (sibling repo):** `../../../delta-kernel-rs/kernel/tests/row_tracking.rs`

Delta Lake's Row Tracking feature assigns a stable, globally-unique `row_id` to every row in a
table. Writers produce a `baseRowId` per file and a table-wide high watermark; readers expose
`row_id = baseRowId + row_index` as a metadata column. Before this PR the kernel had **write-path**
tests only — this PR adds the missing **read-path** tests.

## Getting started

Bootstrap the project from this directory:

```bash
cargo init --name row-tracking-read
```

### Dependencies

| Crate | Why |
|-------|-----|
| `delta_kernel` | The library my PR lives in. Pulled from **git**, not crates.io — `test_utils` is a workspace-internal crate that depends on `delta_kernel` by `path`, so if our project pulls `delta_kernel` from crates.io we end up with **two copies** of the crate and every type stops unifying (`expected DefaultEngine<...>, found a different DefaultEngine<...>`). Both deps must point at the same git source. |
| `test_utils` | Workspace helper from the same repo — gives us `create_table`, `engine_store_setup`, `read_scan` so the narrative in `main.rs` stays short. Also git-only (not published). |
| `tokio` | Async runtime. `rt-multi-thread` is needed for the `#[tokio::test(flavor = "multi_thread")]` checkpoint test pattern; `macros` is for `#[tokio::main]` / `#[tokio::test]`. |
| `futures` | `join_all` for fanning writes across tasks. |
| `tempfile` | Throwaway directories. |
| `url` | Delta table locations are `Url`s, not paths. |
| `serde_json` | JSON actions in the Delta log. |
| `tracing-subscriber` | So `tracing_subscriber::fmt::try_init()` in `main()` prints kernel logs. Regular dep (not `--dev`) because `main.rs` uses it. |

Feature flags on `delta_kernel`:

| Feature | Enables |
|---------|---------|
| `default-engine-rustls` | `DefaultEngine` backed by `object_store`, using rustls for HTTPS. Swap to `default-engine-native-tls` on macOS if you prefer SecureTransport. |
| `tokio` | `TokioBackgroundExecutor` and `TokioMultiThreadExecutor`. |
| `arrow` | The `delta_kernel::arrow::*` re-exports (`RecordBatch`, `Array`, etc). |
| `prettyprint` | `delta_kernel::arrow::util::pretty::pretty_format_batches` — prints a `&[RecordBatch]` as an ASCII box, very close to polars' df look. |

Install commands:

```bash
cargo add delta_kernel --git https://github.com/delta-io/delta-kernel-rs --branch main \
  --features default-engine-rustls,tokio,arrow,prettyprint
cargo add test_utils --git https://github.com/delta-io/delta-kernel-rs --branch main
cargo add tokio --features rt-multi-thread,macros
cargo add futures tempfile url serde_json tracing-subscriber
```

### Run it

```bash
cargo build     # first build is slow; kernel + arrow are chunky
cargo run
```

## Misc. 

### Lint with clippy

Rust's opinionated linter. Run it before pushing.

```bash
# lint main + all targets (tests, examples, benches)
cargo clippy --all-targets

# treat warnings as errors — same bar CI uses in most repos
cargo clippy --all-targets -- -D warnings

# auto-fix what can be auto-fixed (review the diff afterwards)
cargo clippy --all-targets --fix
```

Standard formatter too:

```bash
cargo fmt           # reformat in place
cargo fmt -- --check  # check-only, exits non-zero if anything is unformatted
```

