# I8 distribution candidate

This directory contains the strict source/license/dependency inventory and the whitelist builder for the generation-1446 baseline.

Status: INTERNAL_REVIEW_ONLY
Publication: BLOCKED

The checked-in candidate is deliberately a dependency-closed static source subset. It is not a product runtime, installer, public release, browser acceptance, device build, or release receipt. The original baseline remains preparation-only and the source bytes are not altered.

Inputs

- `baseline/generation-manifest.1446.original.json` is the 158-member source pin.
- `app/data/SOURCES.json` is the recorded provenance for the excluded geography snapshot.
- `BASELINE.md` records the original root and preparation-only state.

Deliverables

- `source-inventory.json` maps every baseline member to source, license, dependency, and distribution dispositions. It also records the I8-authored/generated artifacts.
- `distribution-manifest.1446.json` records the generated candidate file set, hashes, static checks, exclusions, and publication blockers.
- `build_candidate.py` is a stdlib-only builder/verifier.
- `candidate-1446/` is the independent, clean static candidate directory.

Build and verify

From the repository root:

    python3 distribution/build_candidate.py --force
    python3 distribution/build_candidate.py --check
    python3 distribution/build_candidate.py --check-publication

The first command writes only the exact generated paths under `distribution/`. The second command is the permitted static candidate check and must exit 0. The third command is intentionally blocked and exits 2 while printing the unresolved publication blockers; a blocked publication check is not a failed candidate hygiene check.

Allowlist

The candidate contains only:

- `app/__init__.py`
- `app/disease.py`
- `app/execution.py`
- `app/scenario.py`
- `app/source_attestation.py`
- the tracked scenario JSON files under `app/data/`
- the generated `candidate-1446/CANDIDATE.md`

The builder preserves each baseline entry's exact pinned bytes before hashing and writing. Most text is LF; six scenario fixtures retain their recorded CRLF bytes. This avoids changing the original generation manifest's byte identity on a Windows checkout.

Explicit exclusions and blockers

- `app/data/geography.json` is excluded. `app/data/SOURCES.json` records upstream hashes and a commit, but citations and hashes do not establish redistribution rights.
- The recorded upstream license hash for the geography snapshot does not match the public `LICENSE` bytes at the recorded upstream commit; the exact snapshot license remains unresolved.
- The baseline has no tracked `LICENSE`, `COPYING`, or equivalent license grant. External publication therefore requires maintainer authorization.
- `app/requirements-siwc.txt` declares optional `PyJWT==2.10.1` and `cryptography==46.0.3`; those packages are not bundled, installed, or executed by this task.
- `app/pi_worker.mjs` refers to `@earendil-works/pi-agent-core`, `@earendil-works/pi-ai`, and `typebox`, which are not present in the repository. The Pi/runtime surface is excluded.
- The excluded test surface imports `psutil` without a tracked requirement declaration; tests are not part of the candidate.
- Runtime, database, credential, private-evidence, integration-test, and verifier paths are excluded from the whitelist. No databases, credentials, private evidence, unreviewed media, or missing dependencies are copied into the clean candidate.

Validation boundary

Only static inspection, baseline hash verification, AST import-closure checks, candidate path checks, secret-pattern checks, and manifest readback are permitted here. Product services, real models, paid calls, browser acceptance, device builds, runtime isolation, database execution, and release acceptance remain unverified.
