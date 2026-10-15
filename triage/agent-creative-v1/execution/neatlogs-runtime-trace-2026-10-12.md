# Neatlogs runtime application trace: 2026-10-12 evidence

Scope: getticketstub.com#27 Neatlogs item "Replace the offline exporter with real ingestion/readback and retain an actual application trace". This stacks on agent-harness PR #38.

## What counts as an actual application trace here

The trace was emitted by the harness runtime while it executed a genuine workflow through its normal entry point. It carries no `evidence:synthetic` tag.

- **Entry point:**
  1. `createOperationsWorker` → `submitSyntheticWork`, the worker's only intake today. Its scope strings carry no authority, because no application authorization exists yet.
  2. The owned runner child process (`src/worker/private/runner.js`).
  3. `runBoundedLocalPlan`.
- **Workflow:** one bounded local plan with two steps, `number.add` (20 + 22 = 42) and `text.concat` ("agent-harness runtime trace"). The job reached `completed`.
- **Telemetry path:** durable workflow events → the existing `TelemetryExporter` projection (HMAC tokens, fixed allowlist) → Neatlogs outbox sink → pinned SDK capture → `planNeatlogsExport` → `sendNeatlogsPlan` → `readbackNeatlogsPlan`. The PR #38 synthetic builder was not used.
- **Model mode:** none. The harness ran in `mock` dependency mode. The registry marks provider, network and shell operations unavailable, so there were 0 provider or model calls.
- **What is not exported:** 4 of the 8 durable events were policy-dropped by the existing telemetry allowlist (`private_local_step_admitted` ×2 and `private_local_step_completed` ×2). Step results and plan arguments are not exported. Only tokens, enums and counters leave the process.
- **Tags:** `agent-harness@157048a32b793fd7af7af1931ff9e9e76a7810a1`, `evidence:runtime`, `harness-mode:mock`.

## Component revisions

- agent-harness `157048a32b793fd7af7af1931ff9e9e76a7810a1`: the run, the send and the first readback.
- agent-harness `c1deb8e891f3a5cff7ba0079062d893291c7d610`: adds `--recover` to the drain. It ran the single recovery send and the final readback.
- `neatlogs` SDK `1.1.28`. Node `22.23.3`.

## Commands

```bash
node tools/neatlogs-runtime-trace.mjs --mode send-and-readback --env-file ../agent-harness/.env \
  --outbox <scratch>/live-outbox --component-revision 157048a32b793fd7af7af1931ff9e9e76a7810a1 \
  --evidence <scratch>/live-evidence.json
node tools/neatlogs-runtime-trace.mjs --mode drain --env-file ../agent-harness/.env \
  --outbox <scratch>/live-outbox --component-revision 157048a32b793fd7af7af1931ff9e9e76a7810a1 \
  --evidence <scratch>/live-drain1.json --max-polls 30          # readback only, no send
node tools/neatlogs-runtime-trace.mjs --mode drain --recover --env-file ../agent-harness/.env \
  --outbox <scratch>/live-outbox --component-revision c1deb8e891f3a5cff7ba0079062d893291c7d610 \
  --evidence <scratch>/live-recovery.json --max-polls 15
```

## Results

Session (workflow token): `tok1_bb4496f3…c008ac`. Target: Neatlogs US.

| Seq | Event | Trace ID | Gen | Send | Readback |
| --- | --- | --- | --- | --- | --- |
| 1 | `workflow_created` | `02e7eebf500996d1602bb89daa7781d1` | 0 | batch 1: HTTP 200, 1 attempt, 0 of 6 rejected | `verified`, finalized |
| 2 | `operations_work_admitted` | `c84307f324dd5edd32dc47853706de18` | 0 | batch 1 | `verified`, finalized |
| 3 | `operations_work_claimed` | `e76909df83e0aad526532bd2032dc6c5` | 0 | batch 1 | `verified`, finalized |
| 8 | `operations_work_completed` | `a67605147354a759e0194d1c351a646a` | 0 | batch 2: HTTP 200, 1 attempt, 0 of 2 rejected | `missing` (404 for the whole window) in readback 1 (10 × 2 s) and readback 2 (30 × 2 s) |
| 8 | `operations_work_completed` | `0a3fc7a4bbccd9b982a51c9d46943cbc` | 1 | recovery: HTTP 200, 1 attempt, 0 of 2 rejected | `verified`, finalized; `duplicateTraceIds: []` |

Final state: `deliveryConfirmed: true`. All 4 exported events are verified through the Public API: IDs, session, workflow name, tags, span tree and exact canonical input match. The completion marker is not stored as a span (`markerStored: false`, spans 1), the same as in PR #38.

Evidence files:

- [send](neatlogs-runtime-2026-10-12-send-evidence.json): run plus send plus first readback.
- [readback](neatlogs-runtime-2026-10-12-readback-evidence.json): readback only.
- [recovery](neatlogs-runtime-2026-10-12-recovery-evidence.json): one recovery send plus final readback.

Leak checks:

- Before each send, the export body was scanned for all 15 raw run identifiers: job, workflow, tenant and project scope, owner, idempotency key, run ID and every raw event ID.
- Each evidence file was scanned for those identifiers, both credentials and the project UUID before it was written.
- Before commit, the files were rescanned against every credential in `.env`. They contain no project UUID.

## Observations and limits

- **Second first-delivery loss.** This is the second time a first delivery was accepted (HTTP 200, 0 rejected) but not exposed within the bounded window. The first was trace 3 in PR #38. Both times, recovery under a new ID verified. The cause is server-side and not visible to the client. As in PR #38, bounded-window non-observation does not prove permanent loss. If generation 0 appears later, the event will exist twice.
- **Send budget.** One workflow run. One recovery retry for diagnosis. No other sends.
- **Tokens are per run.** A random HMAC key is used for each process, so tokens cannot be linked across runs. A stable tokenization key needs an approved secret store.
- **Delivery timing.** The exporter flushes to the outbox at worker stop. Network delivery is a separate drain, so the worker makes no network calls. A long-running deployment would need a scheduled drain.
- **Customer app.** The getticketstub.com app integration (`exportToHarness()`) is still separate and downstream.
