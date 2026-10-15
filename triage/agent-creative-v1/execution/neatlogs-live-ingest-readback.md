# Neatlogs live ingestion and readback: 2026-10-11 evidence

Scope: ACV1-AH10 prerequisite and getticketstub.com#27, Neatlogs setup items. All data is synthetic. No customer activity was exported.

## Component revisions

- agent-harness `52db7edcc2fb7a69c24b56c5362eb2fec5b28778`. This is the code that sent the first run's traces. The tag `agent-harness@52db7ed…` was attached to every trace in that run.
- agent-harness `0301e9ba6c79ece15da7e7428fab9b17d83bde2e` (delivery generations). This is the code that ran the Public API readback, the recovery and the first end-to-end run.
- agent-harness `fb2a5f7c6d7ad2a67178f76e22c2d284185a55fe` (hardened retry and validation; current code on this branch). This is the code that ran the final end-to-end run.
- `neatlogs` SDK `1.1.28` (pinned; capture path unchanged).
- Node `22.23.3`.

## Target

- Neatlogs US (`https://ingest.neatlogs.com`, `https://app.neatlogs.com`). The project is the existing single project [personal project display name redacted], confirmed in the signed-in dashboard.
- No workspace, project or ingest key was created.
- Ingest used the existing project API key from the git-ignored `agent-harness/.env`, through `--env-file`.
- With owner approval, one service account was created and bound to the project with read-only project access. The owner minted its token. The token sits only in the git-ignored `.env` as `NEATLOGS_TOKEN`. `GET /whoami` reports kind `service_account`, basis `service_account_project_binding`, effective permission `project:read`, and read-only scopes including `context:read` and `observability:read`.

## Commands

```bash
npm ci --ignore-scripts
npm run build
npm run test:neatlogs-live   # 47/47 pass at fb2a5f7, fake transport, no network
node tools/neatlogs-live-verify.mjs --mode offline --component-revision <sha> \
  --evidence <scratch>/offline-evidence.json --plan <scratch>/offline-plan.json
node tools/neatlogs-live-verify.mjs --mode send --env-file ../agent-harness/.env \
  --component-revision 52db7edcc2fb7a69c24b56c5362eb2fec5b28778 \
  --evidence <scratch>/send-evidence.json --plan <scratch>/plan.json
```

## Results

Evidence files: [send evidence](neatlogs-live-2026-10-11-send-evidence.json) (`live-synthetic`) and [export plan](neatlogs-live-2026-10-11-plan.json). Both passed the tool's leak scan for canaries and credential values, and were rescanned before commit.

- **Redaction gate:** 6 synthetic canaries were seeded into the durable workflow's raw fields: private URL, email, token-like string, customer text, raw ID and error text. None appeared in the encoded export body (`exportBodyFindings: []`). The dashboard shows only `tok1_` HMAC tokens and enums.
- **Send:** one request, `receiver_accepted`, HTTP 200, `rejectedSpans: 0` of 6 expected spans (3 roots plus 3 completion markers). `deliveryConfirmed: false` by design.
- **Dashboard readback** (signed-in browser session, independent of the ingest key, at 2026-10-11 ~18:00Z):

| Seq | Event | Trace ID | Dashboard |
| --- | --- | --- | --- |
| 1 | `workflow_created` | `42d52c183ddf75c568a56010c884b82f` | Present. URL trace ID matches; input event token and workflow token match the plan; span `agent_harness.workflow_event` (WORKFLOW); 1 span; 2 tags; session turn 1/2 |
| 2 | `lease_acquired` | `8b5edad544e2d08b14a20aa478ee3d09` | Present. Input event token and workflow token match the plan |
| 3 | `state_transition` | `31fedb2dafb257d9f1c965d8973db03d` | **Not found** at `/traces/<id>` after more than 2 minutes, despite HTTP 200 with 0 rejected spans |

  - Session `tok1_52648f98…3b60` (workflow token) groups traces 1 and 2.
  - The completion marker is not stored as a span. The readback verifier already accepts this case and records `markerStored: false`.
  - Trace reference: `https://app.neatlogs.com/traces/42d52c183ddf75c568a56010c884b82f`. Project access is required.

## Trace 3 root-cause investigation (2026-10-11 ~18:20Z)

Checked through the dashboard's own backend: the signed-in session, read-only `/api/traces/v3/<id>/view` and `/api/sessions/<session>`.

| Test | Sent | Result |
| --- | --- | --- |
| Original | `31fedb2d…`, with its root start in the same millisecond as trace 2 | 404. The session lists 2 traces, and the session `lastActivityAt` equals trace 1 `updatedAt`, so there is no sign trace 3 was ever cataloged |
| Re-send of trace 3 alone | Same IDs and input, fresh timestamps | HTTP 200, 0 rejected; still 404 |
| Trace 3 content under new IDs (`46d6764e…`), plus a trace 2 control (`823c3df3…`) | New IDs, starts 5 ms apart | Both finalized |
| Two new-ID traces with identical start times (`18279145…`, `990448b3…`) | Same session, same start nanosecond | Both finalized |

Conclusions:

- **Root cause (proven):** the receiver lost the first delivery of trace ID `31fedb2d…` and silently discards later deliveries of that ID while still answering HTTP 200 with 0 rejected spans. Content and start-time collision are ruled out.
- **First-loss trigger:** server-side and not visible to the client. About 5 hours later the Public API readback (below) still returned 404 for the ID, with no `dlq` or `pending` record.
- **Harness defect:** purely deterministic IDs made the loss unrecoverable. Fixed by delivery generations: readback reports `missing` and `--recover` re-sends under the next generation.
- **Extra data:** the tests added 4 synthetic traces and 1 dropped re-send to the same session, all tagged `evidence:synthetic`.

## Public API readback and recovery (2026-10-11 ~23:20Z)

Commands (the plan copies were run from scratch paths; the results are committed below):

```bash
node tools/neatlogs-live-verify.mjs --mode readback --env-file .env \
  --plan neatlogs-live-2026-10-11-plan.json --evidence <scratch>/readback.json
node tools/neatlogs-live-verify.mjs --mode readback --recover --env-file .env \
  --plan <copy of neatlogs-live-2026-10-11-plan.json> --evidence <scratch>/recovery.json
node tools/neatlogs-live-verify.mjs --mode send-and-readback --recover --env-file .env \
  --component-revision 0301e9ba6c79ece15da7e7428fab9b17d83bde2e \
  --evidence <scratch>/e2e.json --plan <scratch>/e2e-plan.json
```

| Run | Evidence | Result |
| --- | --- | --- |
| Readback of the first run's plan | [readback](neatlogs-live-2026-10-11-readback-evidence.json) | `missing`. Traces 1 and 2 are `verified` (finalized; IDs, session, workflow name, tags, span tree and input match; `markerStored: false`). Trace 3 `31fedb2d…` is 404 `NOT_FOUND` on the Public API |
| Recovery of that plan | [recovery](neatlogs-live-2026-10-11-recovery-evidence.json), [recovered plan](neatlogs-live-2026-10-11-recovered-plan.json) | `verified`, `deliveryConfirmed: true`. Trace 3 re-sent once as generation 1 `62e2ef5c4ad980989b79705ef3f3de97` (HTTP 200, 0 rejected), then read back finalized and matching. `duplicateEventDeliveries: 0` |
| Fresh end-to-end run | [e2e evidence](neatlogs-live-2026-10-11-e2e-evidence.json), [e2e plan](neatlogs-live-2026-10-11-e2e-plan.json) | Send `receiver_accepted` (1 attempt, HTTP 200, 0 of 6 rejected). Readback `verified`, `deliveryConfirmed: true`. Traces `f48747c2…` (workflow_created), `96478001…` (lease_acquired), `143e4416…` (state_transition), all generation 0, finalized. Session `tok1_5b7b93f0…e585`. Recovery not needed |
| Final end-to-end run at `fb2a5f7` (~23:24Z) | [evidence](neatlogs-live-2026-10-11-e2e-fb2a5f7-evidence.json), [plan](neatlogs-live-2026-10-11-e2e-fb2a5f7-plan.json) | Send `receiver_accepted` (1 attempt, HTTP 200, 0 of 6 rejected). Readback `verified`, `deliveryConfirmed: true`. Traces `cda66d2f…`, `a8d9cae8…`, `6e66d49c…`, all generation 0, finalized. Recovery not needed |

All seven files passed the tool's canary and credential leak scan and were rescanned against both configured credential values before commit.

First-loss trigger: about 5 hours after the send, the Public API returned 404 for `31fedb2d…`, with no `dlq` or `pending` record, so Neatlogs exposes no client-visible reason. The harness response worked live: readback reported `missing`, and `--recover` delivered the same event under a new ID, which then verified.

## Limitations and open items

- **Trace 3 cause unexplained.** One of 3 first deliveries was not exposed in the first run, and 0 of 3 in the end-to-end run. Only a Neatlogs support request (trace ID `31fedb2d…`, send ~18:00Z) could explain why.
- **Extra synthetic data in the project.** Investigation and recovery left extra synthetic copies of the trace 3 event (`46d6764e…`, `62e2ef5c…`) and test traces, all tagged `evidence:synthetic`.
- **Durable cursor not wired.** `TelemetryExporter` (500 ms sink cap, retry on timeout) is not wired to this network path. Follow-up: a durable "sent, awaiting readback" state.
- **Correlation limited by the projection.** It contains job (workflow), event, sequence, revision and fence. Operation, billable-attempt, provider and artifact references are absent upstream and are not invented.
- **Customer app integration pending.** getticketstub.com `exportToHarness()` is still a no-op stub. Customer-app integration through a harness intake contract remains downstream.
- **Pre-existing test failure.** `OPS04_REAL_SIGTERM_JEV_TELEMETRY` (tests/operations) fails identically on unmodified `origin/main` `cdd52b1` in this environment, so it is unrelated to this change. Full suite result: 264/265.

## Historical interpretation qualification (2026-10-11)

The bounded readback observation and later-generation observation above remain the author’s historical observations. They support a hypothesis that the earlier trace ID was not exposed during that observation window; they do not establish permanent loss, permanent deduplication suppression, or exclude delayed projection or correlation issues.
