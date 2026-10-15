# JEV provider wire conformance

**Status:** Phase A/B private wire and local-durability prerequisite; synthetic/offline evidence only. This note does not close AH-05, approve a model/account, enable live execution, or satisfy Finance, tenant-authorization, shared-contract, engine, or release gates.

## Private adapter boundary

`src/adapters/jev/` contains private TypeSafe request/response validation, one fixed-target HTTP send, retry recommendation data, and a pure in-memory observation helper. The Phase B extension in `src/internal/durable-store.js` records the private observation in the existing SQLite store. These private modules remain unexported. The separately composed [public DecisionPort](typesafe-host-integration.md) reuses this transport under the application's existing canonical validators, authorization and Finance controls. It does not enable the private local runtime, which continues to reject `mode: "live"`.

Every fixture and controlled HTTP/DNS response under `tests/fixtures/jev/` is synthetic. Tests replace DNS resolution and `https.request`; they do not contact TypeSafe or another external service. The API key supplied to the private send helper is an explicit internal argument. There is no `.env` reader or credential loader in this phase, and no real credential or spend authority was used.

`createLocalAttemptObservation` returns an immutable in-memory summary with a random local observation ID and separate logical-operation, opaque billable-attempt, and transport-attempt references. Its `persistence: "memory_only"` label is deliberate. It remains a pure helper and is not the durable capture path.

No credential or spending authority is established here. Existing `authorityHoldRef` values remain opaque references, not grants. An HTTP status, timeout, rejected answer, or connection loss does not show that a provider call was free. No local outcome settles usage, releases a hold, or authorizes a retry.

## Phase B SQLite evidence boundary

The durable extension reuses the schema-v4 workflow database and migrates it atomically to schema v5 by adding `jev_dispatch_markers`, `jev_provider_observations`, and `jev_local_observation_outbox`. For source schemas v1, v2, v3, and v4, the complete legacy migration chain and v5 extension run in one transaction. A v5 DDL failure restores the source version, schema objects, rows, receipts, checkpoints, events/outbox, and telemetry cursor as one rollback. Successful migration leaves existing workflow/checkpoint/event/outbox/receipt/attempt/hold references and telemetry cursor values intact; future schema versions remain rejected before writes.

An intent and transport identity are already recorded in `operation_attempts` and `transport_attempts`. Before the offline test sender can run, a fenced `jev_dispatch_markers` event is committed. An observation row then binds workflow, logical operation, billable attempt, and transport attempt, and stores only the pinned requested model, a bounded resolved model/request ID when available, valid integer token usage or a typed unknown reason, and a local decision-conformance summary. It stores no prompt, response body, credential, provider error text, or arbitrary request metadata. Exact source identity/fingerprint repeats deduplicate; a reused source identity with different content is rejected. A later known observation needs a distinct source reference and does not overwrite the earlier unknown record.

The immutable observation, `jev_local_observation_recorded` workflow event, normal safe event-outbox row, and separate `jev_local_observation_outbox` delivery intent commit in one SQLite transaction. Local page reads are SQL-bounded. The local outbox label explicitly says `local_only_not_finance_acceptance`; its acknowledgement only marks local delivery and cannot acknowledge a Finance contract. The telemetry projector's existing event-type allowlist rejects JEV-specific event types, and JEV event rows contain no observation payload.

The test-only orchestrator lives under the contract tests. It persists the durable intent and pre-send marker before invoking the real `sendJevOnce` code against controlled DNS/HTTPS fixtures. Failure to persist either prerequisite makes zero HTTP writes. If observation/outbox persistence fails after a possible write or response, the attempt remains unresolved and the helper never resends. Recovery stores explicit unknown evidence without provider dispatch, response receipt, success promotion, hold release, or an external `no_effect` claim. Late evidence can be recorded after cancellation, lease expiry, or fence change without changing workflow/checkpoint state. This is local operational evidence only; canonical authorization/reservation binding and Finance reconciliation remain unavailable external contracts.

## TypeSafe wire target and source facts

The fixed request target is `POST https://api.typesafe.ai:443/v1/systemone` with Bearer authorization and `application/json`. The request body is exactly `{ model, state, questions }`. The response envelope requires `{ model, answers, usage }`; usage contains integer `input_tokens` and `output_tokens`. The response model must match the requested model. The current explicit wire fixture identity is `jev-1.13.0`; this is not a product-selection approval. Moving aliases such as `jev-latest` and `jev-preview` are rejected.

The API documents text-only state as a string, object, or array. Question instructions and structured descriptions can also be strings, objects, or arrays. Noul criteria may describe `true` and/or `false`; Choice criteria are a map with at most 255 options and nullable structured descriptions, with the local validator requiring at least one option; Score criteria are an ordered array of 2–10 structured descriptions.

Questions in one request are evaluated independently against the same state. A later question cannot see a sibling answer from the same request. The wire adapter cannot infer semantic dependencies from arbitrary instructions. A dependent question must be built as a later request after code receives the earlier response; local persistence still does not provide canonical decision promotion or authority.

Sources checked 2026-10-10:

- [TypeSafe API reference](https://docs.typesafe.ai/api)
- [Models](https://docs.typesafe.ai/models)
- [Choice](https://docs.typesafe.ai/primitives/choice)
- [Score](https://docs.typesafe.ai/primitives/score)
- [Noul](https://docs.typesafe.ai/primitives/noul)
- [Question primitives and batching](https://docs.typesafe.ai/primitives)
- [Confidence semantics](https://docs.typesafe.ai/confidence)
- [JavaScript SDK RetryPolicy](https://docs.typesafe.ai/sdk/javascript/api/interfaces/RetryPolicy)
- [RFC 5214 ISATAP addressing](https://www.rfc-editor.org/rfc/rfc5214.html)
- [IANA IPv6 Special-Purpose Address Registry](https://www.iana.org/assignments/iana-ipv6-special-registry)

These documents specify wire behavior only. They do not verify account eligibility, live access, idempotency, no-charge error semantics, reservations, or spending authority.

## Validation and bounded parsing policy

Requests are snapshotted from own enumerable data properties only. Proxies, accessors, symbols, cycles, non-finite numbers, unsupported values, and caller `toJSON` behavior are rejected without invoking getters or serializers. The snapshot uses null-prototype maps, then serializes only the bounded copied data.

The local request ceiling is 256 KiB; response bytes are capped at 512 KiB while streaming. The parser obtains Buffer byte length through the typed-array intrinsic before decoding, rejects shadowed byte-view metadata and custom Buffer prototypes, and bounds nesting to 48, JSON nodes to 12,000, object keys to 4,096, string units to 262,144, and array items to 4,096. UTF-8 decoding is fatal; duplicate JSON members, BOMs, trailing data, non-JSON numbers, and oversized/deep values fail closed with typed safe errors. These byte/structure limits are local safety bounds; they do not prove TypeSafe's documented token-context limits.

The response requires an exact model and answer-ID set. Unknown envelope/answer fields fail conformance. Choice must be one of the requested options and have the complete requested distribution, with the selected option at the maximum probability (ties are accepted). Score must have exact zero-based legend/probability keys, a structurally equal description at each level, a finite fractional value in `[0, levelCount - 1]`, and a weighted value consistent with the returned distribution. Every probability and Choice/Score confidence must be finite and in `[0,1]`. No confidence is recomputed or inferred. Noul accepts `[0,1]` including `0.5` and rejects a separate confidence field.

The documented API summary describes Score `legend` values as strings, while the detailed Score documentation permits structured level descriptions and presents matching legends. The validator preserves structured JSON values and checks structural equality by key/value rather than coercing objects to strings. This conservative choice is synthetic-only until live conformance is separately authorized and recorded.

Numerical safety tolerances are local policy, not provider guarantees:

- Probability totals may differ from `1` by at most `0.01` to allow rounded decimal serialization; all members and exact keys are still required.
- Returned Score may differ from its probability-weighted index by at most `0.02` for rounding.
- Choice's selected probability must be within `1e-12` of the maximum so exact ties remain valid despite binary floating-point representation.

Boundary tests cover accepted and rejected values around the first two tolerances. Live fixtures may require a reviewed tolerance change; no automatic adjustment occurs.

Usage is validated independently of answer conformance. If answers are malformed but both token counts are valid safe integers, the local validation result retains that usage with an invalid decision status. Missing or invalid usage is explicitly `unknown`, never fabricated as zero. A malformed/duplicate JSON document cannot safely yield usage, so its observation is unknown. Provider request IDs are optional bounded opaque metadata and never the local observation identity.

## Fixed-surface transport and recovery recommendation

The private transport accepts only a validated wire request, an API-key value, and an optional native `AbortSignal`. It fixes HTTPS, host, port, path, method, headers, SNI, certificate verification, and TLS minimum internally. It does not accept endpoint, host, method, header, proxy, TLS, redirect, retry, or network-injection settings from request/profile/state data. It does not fetch URLs found in the request state.

Before creating an HTTPS request, it resolves A and AAAA once, rejects the destination if any result is not public global unicast, and pins the selected vetted IP in the HTTPS lookup callback. Loopback, private, link-local, unspecified, multicast, documentation, benchmarking, mapped, transition (including ISATAP interface identifiers), and other special-purpose ranges are rejected. RFC 5214 defines the ISATAP interface identifier as `0000:5EFE` or `0200:5EFE` followed by an embedded IPv4 address; the adapter rejects both forms regardless of that embedded value. TLS hostname verification remains for `api.typesafe.ai`; a fresh private Agent avoids environment/global-agent routing. Redirects are not followed.

One monotonic 8-second budget covers DNS, connect, TLS, request write, headers, and body. Abort/deadline tears down DNS/request/socket work and listeners. An abort before the HTTP body write is `not_dispatched`; after a possible write it is an unresolved/unknown dispatch. Response headers are capped at 128 entries and 16 KiB; body bytes are capped during streaming. Non-200 error bodies are discarded, and safe results carry only status, bounded Retry-After, and a sanitized optional provider request ID. The helper issues one HTTP request at most.

`planJevRetry` is pure recommendation data. It proposes at most one additional attempt for 429/529, applies bounded exponential fallback delay starting at 250 ms, respects valid Retry-After seconds/dates, and returns `defer` when the delay exceeds policy or the remaining deadline. An invalid/missing header uses the bounded fallback. Cancellation takes precedence even when an HTTP status is also supplied: pre-dispatch cancellation stops, while post-write cancellation stays in review. Timeout, connection reset, and interrupted-body evidence also stays in review when combined with a status. It stops on 401/422 or exhausted attempts. 408 and other 5xx stay in review because they do not establish a free attempt. Every result has `dispatchAuthorized: false`; no retry is automatically sent.

## Offline evidence and pending obligations

The owned synthetic suite is part of the normal runtime test command and CI workflow. The named evidence target runs that suite with controlled DNS/HTTPS and emits a sanitized, revision-bound report. Local evidence accepts an attached branch or a detached checkout: detached evidence labels both source and candidate as `refs/detached/HEAD` and binds them to the verified `HEAD` commit and tree, with the base selected from the merge-base against local `main`. It does not synthesize GitHub event metadata. The target requires Node v22.23.3 and accepts only the offline `mock` fixture:

```sh
PATH="${ACV1_NODE22_BIN:?Set to the Node 22.23.3 bin directory}:$PATH" \
  make verify CASE=ACV1-AH05 MODE=mock FIXTURE=jev-phase-b-offline
```

The suite names the nine acceptance groups: `wire_positive`, `wire_adversarial`, `batch_semantics`, `fixed_surface_ssrf`, `deadline_cancel`, `retry_faults`, `durable_evidence`, `privacy_identity`, and `gates`. Its evidence report distinguishes dynamically executed test cases and controlled DNS/HTTPS/crash activity from static assertion call-site counts. The cumulative runtime suite separately covers v1/v2/v3 migrations, legacy runtime behavior, build, and strict types.

No AH-05/JEV, Finance, contract, provider-account, tenant-authorization, engine-injection, evaluation, live, or release gate is closed by these private modules or fixtures. Required downstream artifacts include canonical shared-contract request/result/error/version fixtures and consumer acknowledgment; Finance-approved reservation/attempt-binding and known/unknown usage reconciliation contracts; server-derived tenant authorization; provider account/model/client eligibility evidence plus separately authorized spend; and evaluation-owned thresholds/held-out results. Full AH-05 and downstream handoffs remain open.


## Immutable pull-request candidate provenance

Runtime CI checks out the exact `github.sha`, rather than following a PR merge
ref that can move after the event. The evidence resolver requires that exact
candidate to be HEAD, verifies its two parents and the event's exact PR source
head, and records its actual first parent as the evaluated base. The webhook's
base commit remains separate metadata; it is never substituted for the evaluated
base or used to transfer a passing source-push receipt to a different merge tree.

An advanced first parent is accepted only when the webhook base is its ancestor
and it is an ancestor of the checked fetched base-branch commit. Missing refs,
unsafe branch names, unrelated or invented bases, wrong source parents, and a
checkout SHA/tree differing from the immutable event candidate fail closed.
The manifest retains webhook/fetched base references and commits, actual parent
commits, and the verified ancestry policy beside the exact candidate SHA/tree.
The descriptive PR merge ref is not used as a floating source of authority.

The regression fixture constructs a real merge after an independent base change.
A separate local reconstruction of failed candidate
`90b26c917a62b056db9d0c8d87ba19d60d79c815` uses its actual Git objects and the
independently observed metadata: evaluated base `0c28ece...`, webhook base
`19cdefe...`, and source `98e24f1...`. The original resolver rejects that case;
the correction verifies it. This reconstruction does not claim possession of
the original runner event JSON or replace a fresh passing PR-candidate CI run.
