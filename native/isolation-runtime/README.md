# Native isolation runtime

This crate is the Windows-only isolation boundary for Issue #1. It launches a trusted native program in an ephemeral Windows AppContainer, attaches the process to an outer Job object, waits with a bounded timeout, and emits one JSON receipt on stdout.

The launcher validates canonical workspace containment, rejects reparse-point path components and hard-linked files, holds trusted path handles while temporary ACLs are changed and restored, verifies trusted path identity with `GetFinalPathNameByHandleW`, and requires an explicit `--program-access-root` for the executable tree. The child receives an AppContainer token through `STARTUPINFOEXW` and `PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES`. The outer Job object contains bounded descendants (32 active processes), kills the tree on timeout or close, and terminates unhandled exceptions.

Usage from this directory:

    cargo run --release --bin isolation-runtime -- --program PATH --workspace WORKSPACE --program-access-root PROGRAM_ROOT --arg ARG --timeout-ms 10000

Supported path controls:

    --allow PATH    grant the AppContainer access to an existing file or directory
    --deny PATH     add an explicit AppContainer deny entry for an existing path
    --report PATH   allow the child to write a report file and include its contents in the receipt
    --working-directory PATH

The receipt records the schema version, AppContainer SID and token identity, path identities, Job counts, shutdown kind, exit code, custody and DACL cleanup state, handle ledger, and child report. `shutdown` is `normal`, `abnormal`, or `timeout`. Per-run DACL changes are restored before the receipt is returned; the runtime does not leave fixture ACL entries behind.

Child arguments may use `{workspace}` and `{custody}` placeholders; the launcher expands them before creating the process.

The bundled `sqlite-fixture` binary is a bounded evidence fixture. It uses the bundled native SQLite library to write an allowed database and read a denied database outside the granted workspace under the same AppContainer identity. `tests/runtime_evidence.rs` also covers normal, abnormal, and timeout shutdown receipts and verifies that an inherited child is counted by the Job and is gone after timeout cleanup.

Validation:

    cargo fmt --all -- --check
    cargo check --all-targets
    cargo test --all-targets -- --nocapture

These checks validate the native runtime and fixture only. They do not establish full product, CI, device, VM, or release acceptance.
