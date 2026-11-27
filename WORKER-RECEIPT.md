# Issue #7 worker receipt

Stage
- Issue: #7 — Complete role/DM configuration and real-service seam
- Worktree: D:/Dev/src/civilization-theatre-workers/i7
- Branch: issue/i7
- Exact supplied base: 9179892d86d6344615e79c684d28ab6949ffa34c
- Source delivery commit: 15cd9f9377b093c31d6bda56c47b27755ba97a37
- PR: https://github.com/HKLHaoBin/civilization-theatre/pull/11
- PR base/head readback: main / issue/i7
- PR state at readback: OPEN
- Review: not started; no merge performed

Delivered files
- ISSUE7-REAL-SERVICE.md
- app/actors.py
- app/credential_adapter.py
- app/product_cli.py
- app/provider_adapter.py
- app/real_settings.py
- app/redaction.py
- app/tests/test_issue7_contract.py
- app/usage_accounting.py
- app/web/settings.html
- memory-graph.md

Implementation evidence
- ProviderRouter has independent roles and dm adapters and rejects real-mode Pi fallback.
- Product real mode binds the RealGateway through the explicit router; missing routes fail before the legacy Pi start path.
- WindowsCredentialAdapter retains the native-shaped opaque-secret boundary; InMemoryCredentialAdapter is synthetic-test-only.
- UsageJournal bounds event shape, file/line size, and request indexes; identical event replays are idempotent and conflicting replays fail with USAGE_EVENT_CONFLICT.
- Existing gateway retry budget and unknown-usage behavior are exercised through fake transports; no cost is converted from unknown to zero.
- Existing host/coordinator failure path remains the fail-closed technical-pause boundary; contract tests verify the pause hook and no-fallback route.
- Settings UI states that saving does not test a connection and real call/connection/billing acceptance is unverified.
- No credential, token, cookie, private log, database, or .env file was read. Test values are synthetic/redacted only.

Permitted checks and exits
- `python3 -m py_compile app/redaction.py app/usage_accounting.py app/provider_adapter.py app/credential_adapter.py app/real_settings.py app/real_provider.py app/actors.py app/product_cli.py app/tests/test_issue7_contract.py` — exit 0.
- `node --check app/web/settings.js && node --check app/pi_worker.mjs` — exit 0.
- `git diff --check` — exit 0.
- `python3 -m unittest -v app.tests.test_issue7_contract app.tests.test_real_provider967.BindingTests app.tests.test_real_provider967.SettingsTests app.tests.test_real_provider967.GatewayTests app.tests.test_real_lifecycle967.LifecycleTests` — exit 0; 26 tests passed.
- `python3 -m unittest -v app.tests.test_siwc1008_settings` — exit 0; 5 synthetic settings-lock tests passed.
- `node app/tests/test_settings992.cjs` — exit 0; 13 VM/fakeDOM contract tests passed.

Additional bounded checks not used as Issue #7 acceptance
- `python3 -m unittest -v app.tests.test_web_assets` — exit 1. The unchanged pre-existing observation HTML contract expected the literal `未接入`; the failure is outside this Issue #7 scope and no web observation asset was modified.
- `python3 -m unittest -v app.tests.test_siwc992_backend` — exit 1 with 2770 synthetic SIWC boundary failures. This was not used for Issue #7 acceptance; the SIWC implementation was not changed in this stage and the failure was not investigated because I1–I6 surfaces are excluded.

Delivery readback
- `git push -u origin issue/i7` — exit 0.
- Remote head after source delivery: 15cd9f9377b093c31d6bda56c47b27755ba97a37.
- `gh pr create --repo HKLHaoBin/civilization-theatre --base main --head issue/i7 ...` — exit 0.
- `gh pr view 11 --json ...` — exit 0; URL and head read back as above.
- No CI, release, device, browser, live provider, Pi SDK, account, or billing acceptance was run or claimed.

Completion relay blocker
- `HERMES_COMPLETION_RELAY_STAGE_ID` was absent from the stage environment.
- The required fallback path `D:/Dev/src/civilization-theatre-workers/i7/.worker-stage-i7.json` remains absent.
- No completion JSON was fabricated or written without the actual stage ID.
- `HERMES_SESSION_ID` was available as `local-392510cf7c1139f22b8d487a15b5646b42c42066d5ac08bcc246464762ccff30`; the missing stage ID is the only relay-field blocker observed after PR/head readback.

Blockers and unverified layers
- Real Credential Manager behavior and authorized credential access remain unverified.
- Live provider/Pi calls, account authorization, model output, billing/usage confirmation, and duplicate external billing behavior remain unverified.
- Cross-process crash recovery and end-to-end technical pause acceptance remain unverified.
- CI, release packaging, browser/device acceptance, and product/world/database acceptance remain unverified.
- The PR is open for maintainer review; this worker did not self-review or merge.

The completion relay is written only after the final receipt update push and remote-head/PR readback succeed.
