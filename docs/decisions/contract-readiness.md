# Contract release readiness

## Current state

ACV1-CONTRACTS (issue #5) remains blocked. The maintained [shared-contracts catalog](https://github.com/Jevelopers-neatHack-2026/shared-contracts/blob/c4caaccc2ea63b273eeac17ed89af8ac64f6bbde/docs/contract-catalog.md) is design documentation; the inspected owner trees did not contain executable schemas, generated validators, or the DecisionPort/approval/budget adapters required for fixture conformance. `fixtures/contracts/release.json` records the exact external owner actions without copying those contracts.

The pending descriptor is a harness readiness inventory, not a runtime schema or a new contract authority. A canonical release must come from shared-contracts and carry owner-reviewed consumer handoffs. `tools/verify_contract_release.py` accepts such a descriptor only when it pins every artifact and review, checks hashes and crosswalk coverage, and runs the owner-published offline fixture adapter. It does not generate schemas or infer field aliases.

The local adapter request format is `agent-creative.contract-fixture-adapter/v1`. It is a narrow tool exchange for passing pinned fixture paths and expected owner decisions to the published validator, not a DecisionPort or application port. The adapter must be pinned by hash and explicitly attested by its owner as offline. The verifier runs it only with `--offline`, invokes Python without a shell, passes a reduced environment, and requires typed JSON output tied to the pinned release revision. The verifier itself makes no network/provider calls. An offline fixture pass proves only fixture conformance; producer/consumer runtime integration, server authorization, ledger behavior, live access, and global acceptance remain separate.

## External owner actions

| Owner reference | Required artifact or decision |
| --- | --- |
| [shared-contracts #1](https://github.com/Jevelopers-neatHack-2026/shared-contracts/issues/1) | Canonical dependency-light schemas, DecisionPort and approval contract, serialization rules, executable validator, and current/invalid/older/additive/semantic fixtures. |
| [cost-intelligence #1](https://github.com/Jevelopers-neatHack-2026/cost-intelligence/issues/1) | Reviewed attempt-bound reservation and usage protocol, unknown outcomes, duplicate observations, exact precision, billing modes, and conservative holds. |
| [getticketstub.com #1](https://github.com/Jevelopers-neatHack-2026/getticketstub.com/issues/1) | Trusted server identity and exact approval verification interface and fixtures. Harness consumes this authority. |
| [event-intelligence #3](https://github.com/Jevelopers-neatHack-2026/event-intelligence/issues/3), [#4](https://github.com/Jevelopers-neatHack-2026/event-intelligence/issues/4) | EI-X01 research/scope/DecisionPort handoff and EI-X02 attempt/reservation/metering/trace handoff. |
| [creative-engine #3](https://github.com/Jevelopers-neatHack-2026/creative-engine/issues/3), [#4](https://github.com/Jevelopers-neatHack-2026/creative-engine/issues/4), [#5](https://github.com/Jevelopers-neatHack-2026/creative-engine/issues/5), [#6](https://github.com/Jevelopers-neatHack-2026/creative-engine/issues/6) | CE-C01 package/scope/brief/control/version; CE-C02 DecisionPort and actual Choice/Score/Noul fixtures; CE-C03 budget/attempt/usage/unknown outcomes; CE-C04 facts/artwork/composition/approval binding. |

These are narrow handoffs, not reciprocal issue-closure prerequisites. The early contract leaf does not wait for complete specialist runtime or release epics. External owner decisions and fixture publication remain required for this readiness gate.

## Crosswalk required from owners

The published descriptor must pin schema artifact bytes and include one accepted row per serialized field. Each row records canonical field spelling, schema SHA-256, meaning, producer role and leaf, consumer role and leaf, trusted derivation, omitted-versus-unknown behavior, precision/rounding (or owner-approved not-applicable reason), privacy/retention, compatibility rule, and accepted producer/consumer reviews.

The field set includes the catalog common envelope (`schema_version`, `event_id`, `tenant_id`, `project_id`, `correlation_id`, `producer`, `occurred_at`, `ingested_at`, `environment`, `billing_mode`, `source_refs`) and applicable workflow/run/order/design identifiers. It also covers separate `logical_operation_id`, `billable_attempt_id`, `transport_attempt_id`, `provider_request_id`, and source observation identity; immutable facts/package and artifact/version/hash bindings; externally authenticated actor/scope/action/expiry/revocation; reservation binding/lifecycle; decision/question/policy/model identities; and trace/usage references. Unknown or unavailable values must follow the canonical owner's omitted/explicit-unknown semantics.

The required case inventory in the verifier follows issue #5: current valid and invalid, supported older and unsupported major versions, additive unknown fields, forged scope, exact artifact/fact approval binding, invalid actor/action/expiry/revocation/replay, distinct attempt identities, duplicate observation, missing provider identity, unknown usage versus zero, in-flight hold after lease expiry, subcent precision, billing-mode isolation, provenance conflict, unapproved asset, malformed control, wrong-project result, and cancelled/deadline outcome. A fixture success cannot claim runtime authorization, ledger, or producer/consumer behavior.

## Compatibility and migration

The owner must declare whether unknown additive fields are preserved or ignored, publish current and supported older consumer fixtures, and reject unsupported major versions. Breaking changes require a new supported version and a migration plan. Generated package revisions must be pinned and tested with affected producers and consumers. The harness makes no silent migration or model-alias policy change and does not infer aliases where owners disagree.

The app compatibility decision remains external and must preserve trusted server-derived scope plus exact approval/hash binding. Budget and approval authority remain with their owning systems. No financial ledger, authorization service, UI, or provider adapter is implemented here.

## Verification

```sh
python3 tools/verify_contract_release.py --manifest fixtures/contracts/release.json --offline
```

At this revision the command must exit `2` with `status=blocked`, because the canonical release and all nine external owner decisions are absent. Diagnostics identify the exact owner and action and contain no fixture/customer content. A future zero exit means only `offline_conformance_passed`; it cannot close issue #5 or satisfy CON-01/02/03, ART-02, FIN-01/02/03/04/07/10, BUD-01/02/03, or global Agent & Creative v1 acceptance. Runtime API/review enforcement remains with AH-02/AH-07; reservation contention and ledger behavior remain with AH-09 and cost-intelligence; consumer execution remains with the implementation leaves.

The global acceptance IDs and owner roles remain unchanged: CON-01/02/03 are Agent and research; ART-02 is Product and frontend; FIN-01/02/03/04/07/10 and BUD-01/02/03 are Finance and reliability. This prerequisite does not claim full verification of any global criterion.
