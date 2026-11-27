1446 source candidate — internal review only

Status: INTERNAL_REVIEW_ONLY
Publication: BLOCKED

This directory is a dependency-closed static source review subset, not a product runtime, installer, release, or acceptance receipt.

Source disposition:
- The five copied Python source files preserve their exact generation-1446 baseline pins when built. Verification accepts only equivalent LF/CRLF checkout transformations, not other edits.
- No data JSON is included. app/data/geography.json and all thirteen geography-derived scenario JSON are excluded pending redistribution-rights review.

License disposition:
- No license grant is asserted by this candidate. The baseline has no tracked license file; maintainer authorization is required before external publication.

Dependency disposition:
- Included Python files use only the Python standard library and candidate-internal imports.
- Optional SIWC and Pi/Node surfaces are intentionally absent because their dependencies and authorization were not cleared.

Source inventory: distribution/source-inventory.json (LF-canonical sha256 bd83f63fe53e4db6d8c38a76f7e76fa4b306d0d9d51ab5bef625c65e5ef478c7)

Unverified by design: product services, real models, paid calls, browser acceptance, device builds, runtime isolation, databases, and release acceptance.
