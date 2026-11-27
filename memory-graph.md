# Issue 8 source-candidate continuity

Worktree: D:/Dev/src/civilization-theatre-workers/i8
Branch: issue/i8
Repository: HKLHaoBin/civilization-theatre
Baseline: 3126c4544a33a5561e7785be94a3f699f388a14e, generation 1446, 158 manifest members.
Original manifest LF SHA-256: 248b31fd3402673e2fed83ec3b6c8ef5c71c3f96c13706d05c6310978e308150.

This graph describes only this isolated contributor worktree. Historical project evidence and other worktrees are not inputs to this task and are not modified.

Source/distribution boundary

- distribution/source-inventory.json maps the baseline source, license, dependencies and distribution decisions; distribution/distribution-manifest.1446.json binds the clean candidate.
- The allowlist is five Python source files and one generated CANDIDATE.md; it is not a product runtime or complete world/data/attestation set.
- Geography and all thirteen geography-derived scenario JSON are excluded pending redistribution-rights review, as are runtime/credential/network/test/verifier surfaces.
- The recorded upstream LICENSE digest differs from the observed public raw digest; exact snapshot licensing remains unresolved and this is not license clearance.
- Publication remains blocked pending project authorization, external-reuse review and unresolved/excluded optional dependency/runtime surfaces.
- distribution/build_candidate.py rebuilds and verifies offline. distribution/check_static.py exercises in-memory checkout/manifest/path/secret checks without product imports, databases or fixture writes.
- WORKER-RECEIPT.md is the delivery evidence/readback, not CI, device, browser, model, runtime-isolation or release acceptance.

Source/license disposition of this graph: Issue-8-authored documentation; no license grant asserted; publication requires maintainer authorization. Dependency disposition: plain documentation, no runtime dependency. This graph is not part of the clean candidate payload.
