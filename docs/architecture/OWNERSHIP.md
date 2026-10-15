# `agent-harness`

## Current repository role

This repository owns the agent's internal orchestration and durability surfaces: workflow and checkpoint storage, bounded local execution, recovery classification, operational scheduling, the TypeSafe decision integration, the development invocation boundary, and telemetry projection and delivery mechanics. The harness records execution state and evidence references; it does not become the authority for customer orders, identity, project membership, provider eligibility, financial authorization, spend reservations, or settlement.

The code at [`main` revision `fce81e4`](https://github.com/Jevelopers-neatHack-2026/agent-harness/tree/fce81e4f31f4047827d7de69a4e2da35415bc4c8) is an experimental internal implementation. `package.json` declares package version `0.1.0`, `private: true`, and Node `>=22.23.3 <23`; it is not published as a registry package. Its module exports are the package root, `/ticketstub`, `/decisions`, and `/development`. They are package boundaries, not a hosted customer API or a stable public contract.

Current implementation surfaces include:

- `src/internal/`: file-backed SQLite workflow, checkpoint, receipt, event, and outbox primitives.
- `src/orchestrator/internal/`, `src/tools/internal/`, and `src/recovery/internal/`: bounded local synthetic planning, fixed pure-operation execution, and recovery classification. These modules do not expose provider or shell effects.
- `src/worker/private/` and `src/health/private/`: the private local operations supervisor and fixed-schema health snapshot.
- `src/adapters/jev/` and `src/decisions/`: the private TypeSafe wire adapter and a typed DecisionPort integration composed with the host application's existing validators, authorization, and Finance controls.
- `src/development/` and `src/integrations/ticketstub/`: the development-only call into the authenticated application host and the TicketStub integration surface.
- `src/observability/private/`: telemetry projection, local Neatlogs SDK capture, bounded file-outbox admission, and immutable per-batch send/recovery intents with recorded receiver retry minima.

The private synthetic controller rejects `mode: "live"`. Development and local tests do not grant an issuer permission, provider entitlement, Finance reservation, spending authority, or a live execution path. Package checks and mock fixtures are internal evidence only. Runtime/setup constraints and the commands that actually exist are recorded in the [runtime setup runbook](../runbooks/runtime-setup.md), [local runtime runbook](../local-runtime-runbook.md), and [`package.json`](../../package.json).

## Ownership boundaries

Agent and research owns the harness implementation and its internal state transitions, retry/recovery controls, and operational trace production. Other owners retain their decision rights:

| Domain | Authority | Harness boundary |
| --- | --- | --- |
| Customer operations, verified identity, tenant/project scope, and application host | `getticketstub.com` and its accountable owners | Consume an explicitly authenticated host surface; do not create or reinterpret identity or application state. |
| Provider/model eligibility and provider execution authorization | Provider/account owner and the approved host integration | Keep credentials, account eligibility, retry billing semantics, and live access outside private local fixtures. |
| Reservations, cost attribution, settlement, and financial reconciliation | Finance / cost-intelligence owners | Store opaque references and execution identity only; do not infer authorization or cost from a status, timeout, or mock. |
| Canonical shared contracts and schemas | `shared-contracts` and affected producer/consumer owners | Consume exact accepted revisions and fixtures; do not promote proposals or local DTOs into canonical contracts. |
| Product acceptance, evidence classification, and release disposition | `integration-evals` | Contribute scoped evidence and keep the master checklist, JSON tracker, and owner decisions authoritative. |

The current organization architecture belongs in `getticketstub.com`. The [architecture-kit v1.0 document](https://github.com/Jevelopers-neatHack-2026/getticketstub.com/blob/main/docs/architecture-kit/v1.0/docs/architecture.md) is an immutable historical intent snapshot; it is not this repository's current implemented-state description. Use the [maintained current-state architecture](https://github.com/Jevelopers-neatHack-2026/getticketstub.com/blob/main/docs/architecture/CURRENT-STATE.md), source, tests, and the current master acceptance tracker for implementation and acceptance state. The companion is published in the architecture owner’s repository; this page does not replace or duplicate that canonical home.

## Acceptance boundary

The master checklist and JSON tracker in [`integration-evals` at `5601857`](https://github.com/Jevelopers-neatHack-2026/integration-evals/tree/56018576a9450a2bb46aa55ed3b2f9d9ae97e50d/planning) govern statuses and evidence. The [local acceptance view](ACCEPTANCE.md) identifies selected harness-related criteria and records the tracker revision it reflects. At that revision, CON-03, AGT-02, AGT-03, and AGT-04 are planned; SPN-01 and SPN-02 are owner-accepted in the master tracker with the qualification recorded there. Local code, documentation, a merged implementation PR, or mock tests do not independently update any master status.

The full event-to-approved-artwork journey, revision handling, bounded external failure recovery, and integrated durable evidence remain broader product objectives. Issue [#13](https://github.com/Jevelopers-neatHack-2026/agent-harness/issues/13) remains open. Its merged [STATUS host agreement](https://github.com/Jevelopers-neatHack-2026/agent-harness/blob/0d5e5b1e379ecc0fb504d54c5d4f6ebf79ab09dd/docs/runtime/status-host-agreement.md) is explicitly an unaccepted proposal for one narrow read-only snapshot; it does not provide an authenticated public command API, a schema agreement, a grant, or consumer acceptance.

## Contributor handoff

Before changing cross-repository behavior, review the local [acceptance view](ACCEPTANCE.md), this ownership boundary, the current [shared contract catalog](https://github.com/Jevelopers-neatHack-2026/shared-contracts/blob/main/docs/contract-catalog.md), and the [central contributor guidance](https://github.com/Jevelopers-neatHack-2026/.github/blob/main/standards/agent-contributors.md) and [security guidance](https://github.com/Jevelopers-neatHack-2026/.github/blob/main/standards/security-secret-handling.md). Preserve the exact producer and consumer revisions, mode, commands, test results, evidence class, and unresolved owner inputs. Keep issue references neutral; do not close acceptance issues or imply a maintainer decision from a proposal or pull request merge.
