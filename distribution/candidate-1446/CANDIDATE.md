1446 source candidate — internal review only

Status: INTERNAL_REVIEW_ONLY
Publication: BLOCKED

This directory is a dependency-closed static source review subset, not a product runtime, installer, release, or acceptance receipt.

Source disposition:
- The copied source bytes come from the 1446 baseline manifest and preserve each entry's exact pinned bytes; most text is LF and the six pinned scenario fixtures retain their recorded CRLF bytes.
- app/data/geography.json is intentionally absent because its external reuse has no redistribution-rights evidence.

License disposition:
- No license grant is asserted by this candidate. The baseline has no tracked license file; maintainer authorization is required before external publication.

Dependency disposition:
- Included Python files use only the Python standard library and candidate-internal imports.
- Optional SIWC and Pi/Node surfaces are intentionally absent because their dependencies and authorization were not cleared.

Source inventory: distribution/source-inventory.json (sha256 a66284526be09b9bf7422fd8fd4c910a9a74eb7a6913d90845307c4db928fdb5)

Unverified by design: product services, real models, paid calls, browser acceptance, device builds, runtime isolation, databases, and release acceptance.
