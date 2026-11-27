Issue #1 native isolation runtime worker receipt

Worktree
- Root: D:/Dev/src/civilization-theatre-workers/i1
- Branch: issue/i1
- Base HEAD at entry: 3126c4544a33a5561e7785be94a3f699f388a14e
- Scope: native Windows AppContainer runtime only

Inherited state preserved
- The inherited .gitignore change native/isolation-runtime/target/ was preserved.
- The complete inherited native/isolation-runtime tree was inspected before edits.
- Preserved and left uncommitted: native/isolation-runtime/.probe/cmd.exe, .probe/marker.txt, src/bin/official_probe.rs, and all native/isolation-runtime/target/ outputs.
- Preserved and left uncommitted: WORKER-HANDOFF.md, WORKER-PROMPT.md, WORKER-STAGE-PROMPT.md, WORKER-STAGE-i1.log, and WORKER-STAGE-i1-r2.log.
- No reset, clean, delete, overwrite, or history rewrite was used.

Implemented in the staged candidate
- Ephemeral AppContainer profile/SID creation, token identity verification, and profile cleanup.
- Canonical workspace/program containment, reparse-component rejection, final-path identity receipts, and hard-link rejection.
- Trusted path handles held while temporary ACLs are changed and restored.
- ACL ledger capture/restore, explicit AppContainer allow entries, and deny-entry construction that preserves the existing non-AppContainer DACL.
- Outer Job configuration with active-process bound, kill-on-close, unhandled-exception handling, descendant accounting, and cleanup guard.
- Normal, abnormal, timeout, and inherited-child cleanup receipts with process identity, Job counts, custody state, ACL state, and handle ledger.
- Native SQLite fixture proving allowed access and denied access from the same restricted AppContainer process identity.
- Trusted program access-root rejection contract.

Exact validation commands and exits
- cargo fmt --all -- --check — exit 0
- cargo check --locked --all-targets — exit 0
- cargo test --locked --all-targets -- --nocapture — exit 0; 4 integration tests passed
- cargo test --locked --release --all-targets -- --nocapture — exit 0; 4 integration tests passed
- git diff --check — exit 0

Staged intended files
- .gitignore
- native/isolation-runtime/Cargo.lock
- native/isolation-runtime/Cargo.toml
- native/isolation-runtime/README.md
- native/isolation-runtime/src/bin/sqlite-fixture.rs
- native/isolation-runtime/src/lib.rs
- native/isolation-runtime/src/main.rs
- native/isolation-runtime/tests/runtime_evidence.rs
- WORKER-RECEIPT.md

Not staged by design
- Inherited diagnostic source/evidence: native/isolation-runtime/src/bin/official_probe.rs and native/isolation-runtime/.probe/*.
- All native/isolation-runtime/target/ build outputs.
- Worker prompts, handoff files, and stage logs.

Blockers and unverified layers
- Full product acceptance, CI, browser, device, VM, release, and publication acceptance were not run or claimed.
- No global ACL/firewall/permission changes, credentials, paid calls, product databases, or unrelated services were used.
- The passing SQLite acceptance fixture denies an outside-workspace database by withholding AppContainer access; the optional explicit --deny CLI path is not used as the acceptance proof.
- LPAC-specific policy-mode evidence is not claimed; the verified runtime boundary is Windows AppContainer identity.
- No merge or self-review was performed.
- Completion relay JSON was not written: HERMES_COMPLETION_RELAY_STAGE_ID and HERMES_COMPLETION_RELAY_COMPLETION_FILE were absent, so the required stage_id could not be populated without fabrication. The fallback JSON path remains absent.

External delivery fields are verified separately after commit/push/PR readback and are reported in the final response.
