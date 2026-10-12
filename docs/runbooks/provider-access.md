# Provider access readiness

This runbook describes the local, stdlib-only prerequisite profile for ACV1-ACCESS. It records provider configuration and sanitized account evidence. It does not implement provider calls, replace shared-contracts, or verify the complete Agent & Creative v1 acceptance gate.

## Run the offline profile check

From the repository root:

```sh
python3 tools/verify_provider_readiness.py --profile fixtures/provider-profile.json --mode offline
```

The committed schema 1.1 fixture is intentionally incomplete. A valid fixture with pending external prerequisites prints deterministic JSON and exits `2`; that is the expected result for this fixture. `network_accessed` is always `false` for the current tool. Readiness report version 1.1 adds API-family and client-selection fields. The report includes the fixture ID, component Git revision and dirty-tree indicator, independent capability cells, blocked consumers, owner role, next action, evidence still needed, dependency mode, API family, client-selection mode, and typed diagnostics.

Schema 1.0 profiles remain readable. In 1.0, an absent client selection means `unselected` and remains pending; an existing SDK record must still have a supported name, exact version, and immutable source revision to count as selected. Schema 1.0 cannot express the direct-HTTP selection. Schema 1.1 requires `client_selection` in every capability: choose `unselected`, `sdk`, or `direct_http`. The direct-HTTP form records an API version and a safe policy reference; those fields identify a local configuration choice and do not prove approval, entitlement, or a request. Unknown API/provider/endpoint combinations are rejected with sanitized diagnostics.

Exit codes:

| Code | Meaning |
| --- | --- |
| `0` | Reserved for a future reviewed readiness result; this prerequisite-only version does not emit it. |
| `1` | Profile is invalid, unsafe, unsupported, or has mismatched identity evidence. |
| `2` | Profile is well-formed but access, eligibility, configuration, or downstream live evidence remains pending or denied. |
| `3` | Requested metadata or live mode is unsupported; no provider request was made. |

CI expects exit `2` from the pending fixture and separately requires the offline adversarial suite to pass. After both checks pass, it uploads the sanitized JSON report as a seven-day `provider-readiness-report` artifact. Passing those tests establishes only that the local validator handles its inputs as specified.

## Credential handling

The checker accepts only environment-variable reference names. It enumerates process-environment names and checks the name set; it never indexes, prints, hashes, or logs credential values. It does not load `.env` files. A credential reference that is blank, malformed, or a placeholder is rejected. Presence is not authentication: the checker cannot distinguish an empty environment variable from a non-empty one, and it does not claim that a key is valid or entitled for an operation.

The fixture records `TYPESAFE_API_KEY` as the TypeSafe server-injection name from the assigned packet. Account/project identity, value, and entitlement remain unknown. Do not add key values or customer records to a profile, test, CI variable, report, issue, or commit.

The offline check rejects unknown fields, inline credential fields, non-HTTPS or non-allowlisted endpoints, unsupported API-family/provider/endpoint tuples, selected SDKs without exact pins, model identities outside its bounded documented inventory, stale identity evidence, and account/project/model mismatches. An unselected client stays pending until its owner selects a supported SDK or direct-HTTP policy. Diagnostic messages do not echo rejected values. JSON input is limited to 1 MiB, nesting to 100 levels, and integer tokens to 4,096 digits; excessive nesting returns `PROFILE_JSON_DEPTH_LIMIT` and oversized integers return `PROFILE_JSON_INTEGER_LIMIT`, both as safe JSON errors without tracebacks in every mode. The parser keeps the interpreter's integer-string safety limit enabled. The API and endpoint allowlists are local verifier policy; they are not domain contracts or account approvals. The local supported surfaces are TypeSafe `systemone_v1` at `/v1/systemone`, OpenAI `images` at `/v1/images/generations`, OpenAI `responses` or `chat_completions` for separate planner and baseline cells, and Neatlogs `public_api` or `trace_ingest`. The verifier does not treat the TypeSafe JEV surface as a general planner. Entire uses its local provenance-artifact path and has no provider API endpoint.

## Bounded model identity policy

The verifier accepts exact model identities from this small local policy, inspected on 2026-10-10. These are validation examples and compatibility facts, not selected production models, account access evidence, or owner approvals. Other model IDs, including IDs that look valid by prefix or date pattern, fail closed until this table, its source review, and its tests are updated.

| Capability/API | Accepted request | Alias target or pin |
| --- | --- | --- |
| JEV / TypeSafe `systemone_v1` | `jev-latest` | alias resolves to `jev-1.13.0` |
| JEV / TypeSafe `systemone_v1` | `jev-preview` | currently resolves to `jev-1.13.0`; it may move independently and needs fresh owner approval/revalidation |
| JEV / TypeSafe `systemone_v1` | `jev-1.13.0` | dated versioned identity pin |
| Planner or generative baseline / OpenAI `responses` or `chat_completions` | `gpt-4.1` | alias resolves to `gpt-4.1-2025-04-14` |
| Planner or generative baseline / OpenAI `responses` or `chat_completions` | `gpt-4.1-2025-04-14` | dated pin |
| GPT Image / OpenAI `images` | `gpt-image-2` | alias resolves to `gpt-image-2-2026-04-21` |
| GPT Image / OpenAI `images` | `gpt-image-2-2026-04-21` | dated pin |

For aliases, `identity_kind` must be `alias`, `policy_status` must be `approved`, `approved_model` must equal the requested alias, and `expected_resolved` must equal the exact target above. Owner role, approval reference, approval date, and revalidation date remain mandatory. The recorded configuration resolution, if present, and any response resolution must match that target. Labeling a bare alias such as `gpt-4.1` as `pinned` is rejected. Pinned requests must be an exact dated identity in the table and must not carry alias-resolution fields. These entries do not approve any model for the project; the fixture keeps model selections empty.

Text and image identities are capability/API-specific. The direct Images API accepts only the listed GPT Image identity; text-model identities such as GPT-4.1 and unrelated IDs such as `o3-2025-04-16` are rejected there. Planner and baseline cells accept the listed GPT-4.1 text identity on Responses or Chat Completions and reject GPT Image identities. This is a modality/API compatibility check, not a claim that GPT-4.1 meets the planner's quality requirements. OpenAI currently lists GPT Image 2 on multiple API endpoints, including Images, Responses, and Chat Completions; this local profile maps its separate image-engine capability only to the direct Images API path. The verifier does not model the other endpoints as image-engine integrations. The optional `GPT_IMAGE_ACCESS_MODE` preference describes development tooling only. The harness selects no `GPT_IMAGE_MODEL` default and does not configure deployed image execution. The built-in development and programmatic API paths have separate access and billing requirements; see [the development driver boundary](../driver.md).

For successful observed inference, the record must first bind to a supported capability, provider, API family, and configured endpoint. Missing API selection or an unselected client cannot make response evidence complete: the verifier rejects observations without that API binding and still requires the returned model identity on TypeSafe `systemone_v1` and OpenAI Responses/Chat Completions. The OpenAI Images API response object does not contain a model field, so that surface may leave `resolved_model` null; the verifier never fills it from the requested model and checks it if supplied. A denied/no-response record does not require an actual response identity. Metadata/model-list records cannot establish inference resolution.

The local SDK-name candidates are `typesafe-sdk-js` and `openai`; an SDK selection requires an exact semantic version and a 40- or 64-character immutable source revision. An owner may instead explicitly select direct HTTP with the API family's matching version (`v1` for the currently allowlisted surfaces) and a safe policy reference. `sdk: null` plus an unselected client stays pending. `not_applicable` billing is accepted only for Entire's local provenance path; an observed live service capability requires `api_metered` billing and `dependency_mode: live`. Sandbox or simulated evidence remains nonlive.

## Modes

### `offline`

Reads only the specified local profile. It makes no network calls and does not infer account eligibility, authentication, inference, image generation, sponsor availability, or metering from configured names or fixture data.

### `metadata`

Currently returns `METADATA_MODE_UNSUPPORTED` and exit `3`. No authorized nonbillable metadata adapter or account context is established here, so it makes no request even if a credential variable happens to exist. The profile format can record a future sanitized metadata result, including its exact endpoint, account/project identity, requested model where applicable, revalidation dates, and an external evidence reference. A model-list or authentication observation remains separate from a successful inference/image run.

### `live`

Currently never dispatches a request. Without external authorization and a reservation reference it returns `LIVE_AUTHORIZATION_MISSING`; with those records it still returns `LIVE_EXECUTOR_UNSUPPORTED`. No reservation protocol, authority, amount, price, or spend target is invented by this checker. The separately composed [public TypeSafe DecisionPort](../decisions/typesafe-host-integration.md) uses the application consumer's existing canonical validators, authorization and Finance authority; checker readiness never enables it. Subscription access cannot serve as an API billing fallback.

## Independent capability cells

The profile keeps these v1 obligations separate:

- **JEV:** TypeSafe API configuration and account eligibility. The API requires a model and its successful response includes the versioned model identity. The documented `jev-latest` and `jev-preview` aliases currently resolve to `jev-1.13.0`; these are moving aliases, so an alias use requires an exact owner-approved target and revalidation. A model listing does not prove versioned inference access.
- **GPT Image engine integration:** OpenAI documents `gpt-image-2` and its dated snapshot for the Images API. The owner still has to select an API family/model and establish organization/project access; the actual creative-engine generation receipt remains pending. No API billing route is chosen.
- **Codex development image execution:** The optional preference is `GPT_IMAGE_ACCESS_MODE=codex_subscription` for development tools. It selects no deployed model and is excluded from production requirements. OpenAI's current Learn page says built-in image generation uses `gpt-image-2` and counts toward general Codex usage limits; availability and limits depend on plan and workspace settings. This development execution surface does not configure a creative-engine API adapter, establish organization API eligibility, create a metering record, or satisfy the actual image-generation acceptance gate.
- **Planner/reasoning provider and generative evaluation baseline:** Each has its own unselected provider, endpoint, model, account/project, quota, owner-approved future usage budget, and live-test owner. Neither is inferred from GPT Image or JEV access.
- **Neatlogs:** Ingestion and readback configuration, commands and outcome semantics are in [neatlogs.md](neatlogs.md). This checker records either the canonical `NEATLOGS_INGEST_KEY` name or the legacy `NEATLOGS_API_KEY` alias, checks only whether that exact recorded name is present, and does not authenticate it. The master tracker records SPN-01 as verified and owner-accepted on 2026-10-11 for a `getticketstub.com` `ticketstub.ops/1` operational exporter run. Its [qualified evidence record](https://github.com/Jevelopers-neatHack-2026/integration-evals/blob/56018576a9450a2bb46aa55ed3b2f9d9ae97e50d/evidence/neatlogs/2026-10-11/README.md) documents the evidence and its limits. That record does not accept this harness's separate `ticketstub.event/1` durable SQLite outbox path. This profile and an environment-variable name do not authenticate or reproduce either run.
- **Entire:** The local profile still has no confirmed Entire installation/configuration and cannot verify a provenance artifact. Separately, the master tracker records SPN-02 as verified and owner-accepted on 2026-10-11. That accepted record does not turn this local profile or an ordinary Git commit into the recorded checkpoint/repository provenance artifact.

Each capability records its own environment, billing mode, and quota status/evidence. Those values remain unknown in the committed fixture. Planner/reasoning and the generative baseline also have separate pending budget and reservation-protocol references and a live-test owner role. No numeric spend target or API billing fallback is selected here.

The report distinguishes configuration status, administrator eligibility, metadata observation, and downstream live-capability evidence. `observed` means an external record is entered in the profile; this checker does not independently authenticate the record or the account. A downstream implementation leaf owns its actual provider/sponsor receipt.

## Evidence references reviewed

Provider documentation was inspected on 2026-10-10. These sources explain interfaces, model IDs, and documented behavior, not account entitlement or live service availability:

- TypeSafe [API reference](https://docs.typesafe.ai/api) and [models](https://docs.typesafe.ai/models): the response `model` is required; current aliases map to a versioned model ID; aliases can move.
- OpenAI [GPT-4.1 model page](https://developers.openai.com/api/docs/models/gpt-4.1): documents text input/output and the `gpt-4.1` alias with snapshot `gpt-4.1-2025-04-14`.
- OpenAI [GPT Image 2 model page](https://developers.openai.com/api/docs/models/gpt-image-2) and [Images API create reference](https://developers.openai.com/api/reference/resources/images/methods/generate): document the `gpt-image-2` image model, dated snapshot `gpt-image-2-2026-04-21`, and Images API model list/response shape.
- OpenAI [Responses create reference](https://developers.openai.com/api/reference/resources/responses/methods/create) and [Chat Completions create reference](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create): text response surfaces whose returned response object identifies its model.
- OpenAI [API image generation guide](https://developers.openai.com/api/docs/guides/image-generation) and [Codex/ChatGPT Learn image generation](https://learn.chatgpt.com/docs/image-generation): API generation is a programmatic path; built-in Codex generation uses `gpt-image-2`, counts against general Codex usage limits, and has plan/workspace-dependent availability. Codex subscription access does not establish API eligibility.
- Neatlogs [Public API](https://docs.neatlogs.com/api-reference) and [quickstart](https://docs.neatlogs.com/docs/quickstart/introduction): scoped service-account/project context and application trace instrumentation are documented. This repository has its own local integration, and the separate SPN-01 record is owner-accepted in the [master checklist snapshot](https://github.com/Jevelopers-neatHack-2026/integration-evals/blob/56018576a9450a2bb46aa55ed3b2f9d9ae97e50d/planning/acceptance-checklist.md#spn-01-neatlogs-use); this profile does not authenticate its key or reproduce that record.
- Entire [CLI documentation](https://github.com/entireio/cli) and [security/privacy notes](https://github.com/entireio/cli/blob/main/docs/security-and-privacy.md): checkpoints carry agent session context in repository storage. This profile has no confirmed local installation/configuration; the separate SPN-02 record is owner-accepted in the [master checklist snapshot](https://github.com/Jevelopers-neatHack-2026/integration-evals/blob/56018576a9450a2bb46aa55ed3b2f9d9ae97e50d/planning/acceptance-checklist.md#spn-02-entire-use), and is not verified by this runbook.

The authoritative acceptance meanings, owner roles, and statuses remain in the [master checklist](https://github.com/Jevelopers-neatHack-2026/integration-evals/blob/56018576a9450a2bb46aa55ed3b2f9d9ae97e50d/planning/acceptance-checklist.md) and its JSON tracker. At that snapshot, CON-03 is planned; SPN-01 and SPN-02 are verified, owner-accepted. This prerequisite profile does not create or independently verify those external records. Check the current master before relying on this dated snapshot. The maintained shared-contract catalog is design documentation, not an implemented schema, and this runbook/profile does not copy or replace canonical domain contracts.
