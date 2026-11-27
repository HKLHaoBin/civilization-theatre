# Issue #7 real-service seam

This stage adds an explicit provider/credential boundary without claiming live
provider, billing, browser, device, database, or product acceptance.

## Routing

`services.json` has exactly two independent service descriptors: `roles` for
private character intent generation and `dm` for independent world adjudication.
Each descriptor carries its own provider, model, API base, timeout, retry count,
and opaque credential reference. `RealGateway` selects the requested route; it
does not reuse role history for DM requests.

`app/provider_adapter.py` makes the mode boundary explicit:

- `ProviderRouter('real', roles=..., dm=...)` has no Pi fallback. A missing
  route fails with a fixed error before a provider call.
- `ProviderRouter('scripted-pi', pi=...)` is a separate offline mode.
- `RealProviderAdapter` wraps the existing bounded gateway.
- `PiProviderAdapter` is an injected callable seam for fake/scripted tests.

The existing Node worker remains an offline scripted Pi test double. It is not
used to satisfy real-service acceptance.

## Credentials and redaction

`WindowsCredentialAdapter` is the production-shaped Windows Credential Manager
adapter. `InMemoryCredentialAdapter` is synthetic-test-only. Settings writes
fresh game-owned references and never expose or delete an old credential.
`OpaqueSecret` is closed by the gateway after each attempt.

`redaction.py` exposes only fixed known diagnostic prefixes and recursively
redacts credential-like fields. Provider errors never cross the boundary using
arbitrary exception text. Service status and usage events contain metadata only;
API keys, tokens, prompt bodies, responses, URLs with credentials, and opaque
credential references are not exported.

## Retry and usage accounting

Descriptor validation bounds timeouts to 1–120 seconds and retries to 0–3. The
same request budget is used across all attempts. Unknown usage and cost remain
`null`; an unknown value is never treated as zero.

`UsageJournal` records sanitized `request_started`, `request_returned`, and
`request_failed` events. The identity `(event, kind, request_id,
request_index)` makes an identical replay idempotent and turns a conflicting
replay into `USAGE_EVENT_CONFLICT`. The file and each line are bounded. This is
request-attempt accounting, not proof of provider billing or exactly-once
external billing.

## Fail-closed behavior

Provider failures do not switch to the scripted chooser or another service.
The existing host/coordinator failure path retains the original fixed code,
requests a technical pause when the captured execution is still valid, and
refuses automatic re-execution of a technically blocked activation. Authority,
cancellation, deadline, and recovery-proof failures remain local stop paths and
cannot be converted into a stronger-root pause.

## Verification boundary

The Issue #7 contract tests use only redacted strings, temporary metadata files,
synthetic credentials, and injected provider callables. They do not make model,
paid, network, browser, device, or database calls.

Real Credential Manager behavior, a live provider request, Pi SDK installation,
account authorization, usage/billing confirmation, cross-process crash recovery,
and end-to-end technical pause acceptance remain unverified and require a
separately authorized acceptance stage.
