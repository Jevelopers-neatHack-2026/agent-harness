# Neatlogs delivery queue and TicketStub hand-off: live evidence (2026-10-12)

Code: agent-harness branch `launch/neatlogs-outbox-ticketstub-intake`; the hand-off row ran at `ff642c0` (module then named `neatlogs-outbox.js`), and the application run at `4dc24f5`. Node 22.23.3. Neatlogs US, the existing project, ingest key and read-only service-account token from the git-ignored local `.env`. All data is synthetic, and no customer activity was read or exported.

| Path | Evidence | Result |
| --- | --- | --- |
| TicketStub hand-off: 4 synthetic `ticketstub.event/1` events (`intake_submitted`, `research_started`, `art_requested`, `art_ready`) through `createTicketstubHarnessExporter` → delivery queue → drain | [evidence](neatlogs-outbox-2026-10-12-ticketstub-handoff-evidence.json) | 4 accepted, 0 invalid; 1 send request, `receiver_accepted`; readback through the Public API settled all 4 as `verified` (workflow `agent-harness.ticketstub-events`, one session for the synthetic project) |

The script used a scratch database and the `AGENT_HARNESS_TELEMETRY_KEY` generated into the local `.env`. Both evidence files were scanned against every configured credential value before commit. The durable telemetry cursor path is #43's file outbox (its own runtime trace evidence: `neatlogs-runtime-trace-2026-10-12.md`). This branch's first commits carried a duplicate SQLite cursor sink, which was removed when #43 merged; the module was renamed `neatlogs-delivery-queue.js`. Offline after merging main: 81/81 Neatlogs tests pass; the full suite fails only `OPS04_REAL_SIGTERM_JEV_TELEMETRY`, which fails identically on `origin/main`.

## TicketStub application run

[Evidence](neatlogs-outbox-2026-10-12-ticketstub-app-run-evidence.json). The production build of getticketstub.com `eaee5781944187aa6dabedcd0bb6db0777f152d2` (PR getticketstub.com#62, harness files vendored from this branch at `4dc24f5`) ran on localhost in sandbox mode with `TICKETSTUB_HARNESS_EXPORT=neatlogs`. One `POST /api/art` went through the app's own `recordEvent()` and produced `art_requested` (`3cec81864287295e4c7fe1e7410f4b6c`) and `art_ready` (`bfdd775dd7ea9b624207357a451d97dc`). The server's background drain sent both and its Public API readback settled both as `verified`, generation 0, one send each. The same check passed on the two earlier commits of both branches (before the rename).

An earlier run on the same code, before it was committed, was read back independently with the service-account token. Both traces were finalized under workflow `agent-harness.ticketstub-events`, with tags `ticketstub`, `ticketstub@<commit>` and `environment:development` and one shared session token. The inputs contained only type, time, mode, actor kind, fields and tokens, and not the raw `x-request-id`.

Not covered here: the deployed GoDaddy app. That needs the settings in GoDaddy secret storage and a publish, which are owner steps.
