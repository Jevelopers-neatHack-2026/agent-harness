# Neatlogs SDK local capture — PR31 findings handoff

**Status:** The three blocking findings from the independent PR31 review have been fixed and covered by real-SDK regressions. Local checks pass on the committed source. This work remains a local prerequisite only; no service delivery, sponsor acceptance, or global AH-10 completion is claimed.

**Repository:** `agent-harness`
**Branch:** `launch/acv1-neatlogs-sdk-local`
**Worktree:** `/Users/completetech/agent-harness-worktrees/acv1-neatlogs-sdk-local`
**Inspected base:** `bb280f2bfcf390b5d228148fb961ed0b2af326c9` (tree `50d9c05c0311f443961f40a0e39d9a56c6b0e52e`)
**Source fix commit:** `f92623511ecfc934c0d25ffd7b897867c7876821` (parent `e15d0cf94e2f8359e9a75597348dd187b15405be`, tree `655a82bfeb2e630d65a4428417d71031c8880a07`)
**Dependency:** `neatlogs@1.1.28`; unchanged lock SHA-256 `9c136cb70e980c19daf05978ce98a372cf2825fcbbd37f098d583aa7083fff7d`
**Current after-fix local source verification:** Node `v22.23.3`, npm `10.9.9`; all listed commands used the pinned Node toolchain path.

The requested launcher profile was gpt-6-luna/max, strict, Fast, non-UI. The supported launcher control was accepted; the resolved backend tier is not exposed here. No nested delegation was used. The final documentation commit for this handoff is recorded in the external root handoff at `/Users/completetech/agent-harness/triage/agent-creative-v1/execution/neatlogs-pr31-r1-all-findings-full-handoff.md`.

## Scope, ownership, and boundaries

This branch implements actual local mapping through the pinned public SDK APIs. The SDK is imported only after projected input and unsafe-environment name checks. The implementation keeps the service bridge disconnected and Entire disabled. It does not edit telemetry source/cursor behavior, store schema, public entrypoints, JEV/provider source, financial authority, or canonical producer correlations.

The prior explicit shared-file transfer remains limited to `package.json`, `package-lock.json`, `Makefile`, and `.github/workflows/runtime-foundation.yml`. The bounded worker retains ownership of `src/store/errors/types`; none of those paths were changed. Contract readiness paths remain separately owned. No changes were made to the four transferred shared paths during these fixes.

No push, merge, PR/issue update or closure, deployment, provider/service/model call, or secret/environment value read occurred. `.env` files were not opened. The synthetic placeholder key is used only in isolated local SDK tests. Darwin subprocesses run under `sandbox-exec` with network denied. No default OTLP exporter, global OTel provider, provider wrapper, global monkeypatch, signal hook, Doctor, example, prepack/pretest, prompt, framework integration, `init()`, `flushAll()`, or unexported SDK API is used.

## PR31 findings fixed

### NL31-R1 — automatic SDK exports remain inside their request generation

The public `Client.batchSize` is now `totalSpans + 1` (65), while input remains capped at 32 projected records and output at 64 total spans. The full admitted root-plus-completion-marker batch therefore stays below the SDK automatic export threshold and is drained by that request's explicit public `flush()`. The request latch remains active until filtering and the owned exporter callback finish; a new generation cannot reset exporter state while prior work is pending.

`max-batch.test.js` drives actual SQLite/HMAC-projected 31- and 32-record batches through the real SDK with callback delays of 0 ms and 150 ms and both successful and failing callbacks. It also places abort, timeout, changed-record retry, immediate busy retry, and idempotent close at the 32-record export barrier. Tests require exact root/marker counts, no pending callbacks after settlement, and stable counters after capture/close. Durable cursor, source page, and outbox remain unchanged.

### NL31-R2 — signals and cleanup fail safely

Signals are rejected if proxy/forged or if the intrinsic `AbortSignal.aborted` getter cannot brand-check them. State inspection and listener add/remove use captured platform intrinsics, so caller-owned `aborted`, `addEventListener`, and `removeEventListener` overrides are not invoked. Invalid signals return bounded `INVALID_CAPTURE_SIGNAL` before acquiring the request latch. All post-latch work is inside cleanup; nested `finally` blocks settle the request, clear its timer, release the active request, and settle `request.done` even if cleanup fails. The first abort/timeout cause is preserved while owned export work drains.

`signal-controls.test.js` covers native, pre-aborted, proxied, forged, and method/accessor-overridden signals; it asserts no hostile hooks execute, a subsequent capture succeeds, and close settles. Existing normal abort, timeout, concurrent request, and close tests remain enabled.

### NL31-R3 — artifacts are recursively immutable

The total mask now freezes both reconstructed event/link arrays, including empty arrays generated when SDK fields are absent. The actual-SDK capture subprocess recursively checks the returned result graph and attempts push/splice/index mutation on both arrays. It asserts serialization remains byte-for-byte unchanged and no synthetic mutation marker appears. Direct mask tests exercise the same boundaries.

## SQLite kill/reopen/reproject regression

`restart.test.js` now creates a durable four-event SQLite workflow ending with an unresolved synthetic authority hold, closes the parent connection, and launches isolated children that reopen that actual database. The parent kills children before SDK capture, during a delayed real-SDK exporter callback, and after a sanitized real-SDK capture. After every kill it opens a fresh `DurableWorkflowStore`, compares every SQLite table against a read-only pre-kill snapshot, verifies cursor `0`, outbox rows, attempt state `intent`, and retained unresolved hold, then reprojects the same HMAC event tokens and replays all four records through a fresh real-SDK subprocess. No JSON-only source stand-in is used for these restart phases.

`.runtime-artifacts/neatlogs-sdk-sqlite-restart.json` records three phases, each with four replayed source records, four roots, four markers, unchanged all-table snapshots, stable source tokens, cursor `0`, and all remote/sponsor/Entire/Finance/live claims false. Its SHA-256 and nested capture-artifact hashes are in the external root handoff.

## Ten-group acceptance status

| Group | Local result after fixes | Boundary |
| --- | --- | --- |
| `REAL_PACKAGE` | Pass | Node 22.23.3 loads actual locked `neatlogs@1.1.28`; public ESM imports and strict TypeScript pass. The local capture artifact records 57 dependency entries, 24 source hashes, and the actual SDK output. |
| `EVENT_MAPPING` | Pass | Actual durable SQLite events pass through the unchanged HMAC projector and real SDK. Max input is 32 records; roots and completion markers are counted separately. SDK-generated IDs are not claimed stable. |
| `PRIVACY_CANARIES` | Pass for synthetic local fixtures | Raw ID, URL, suffix, customer, error, session, input/output, and SDK mutation markers are absent from sanitized output/stdout/stderr. Recursive immutability is asserted. No live account/customer audit is claimed. |
| `MASK_DROPS_ARE_FAILURE` | Pass locally | Real package null, undefined, invalid, throw, reject, timeout, and all-dropped behavior remain classified as local capture failure/loss, never source acceptance. |
| `ENQUEUE_FLUSH_NOT_ACK` | Pass locally | Delayed/failing local callbacks and the 32-record boundary are exercised. Flush/callback success creates no accepted receipt; cursor and outbox remain unchanged. Stock OTLP partial acceptance remains an explicit gap. |
| `BOUNDS_AND_CANCEL` | Pass locally | Exact 32/64 cap, 128 KiB output, 65-span SDK threshold, serialized byte bounds, request latch, abort/timeout/retry/close barrier, and late-work stability are tested. No second durable queue is introduced. |
| `CONTEXT_LIFECYCLE` | Pass locally | Existing concurrent Client/listener checks plus new native/forged/override signal tests pass. SDK namespaced symbols are documented; no claim of zero SDK global state is made. |
| `NO_SECONDARY_EGRESS` | Pass on this Darwin host | Minimal child environment, synthetic placeholder, explicit custom local exporter, fixed `.invalid` option, and OS network deny. No service or provider call occurred. |
| `STOCK_ACK_REGRESSION` | Source-pinned pass; official transport runtime variant remains pending | Pinned SDK/OTel source and existing real-package mask regressions retain partial-success and malformed-body evidence. No network or transport monkeypatch was used. |
| `RESTART_AND_EVIDENCE` | Pass locally | Actual SQLite close/reopen/reproject around three process-kill points; all tables unchanged; four source tokens replay into four roots and markers; no cursor or Finance acknowledgement. |

The full service bridge remains disconnected. A strict parsed full/partial transport contract, bounded response/retry and real abort semantics, durable partial-rejection quarantine/recovery, explicit approved region/credentials, and actual receiver evidence are still required. Local enqueue, callback, trace completion, or SDK flush is never represented as remote acceptance.

## Dependency and compatibility record

`package.json` pins `neatlogs` exactly to `1.1.28`; the lock is unchanged from the previously reviewed graph. Offline `npm ci --ignore-scripts` installed all 57 locked packages. No install scripts ran and no direct OpenTelemetry import was added. The sanitized local capture artifact includes every installed/locked package path, version, SHA-512 integrity and license record; it also includes all 24 source hashes and is validated by the test suite. The earlier independent lock audit verified all 57 cached tarballs, installed manifests/licenses, and 9,473 installed files; this fix changes no dependency.

Key records remain:

| Package | Version | Integrity | License |
| --- | --- | --- | --- |
| `neatlogs` | `1.1.28` | `sha512-KeOEzjufhhTaxF7pckw08JOaILQvjj8HpS+Xb+3owMzQfjEtjYG01Oo2WtOGurifuTNMMabjw3WSRDi8a0thmg==` | MIT |
| `@opentelemetry/api` | `1.9.1` | `sha512-gLyJlPHPZYdAk1JENA9LeHejZe1Ti77/pTeFm/nMXmQH/HFZlcS/O2XJB+L8fkbrNSqhdtlvjBVjxwUYanNH5Q==` | Apache-2.0 |
| `@opentelemetry/sdk-trace-base` | `1.30.1` | `sha512-jVPgBbH1gCy2Lb7X0AVQ8XAfgg0pJ4nvl8/IiQA6nxOsPvS+0zMJaFSs2ltXe0J6C8dqjcnpyqINDJmU30+uOg==` | Apache-2.0 |
| `@opentelemetry/otlp-exporter-base` | `0.57.2` | `sha512-XdxEzL23Urhidyebg5E6jZoaiW5ygP/mRjxLHixogbqwDy2Faduzb5N0o/Oi+XTIJu+iyxXdVORjXax+Qgfxag==` | Apache-2.0 |
| `typescript` | `5.9.3` | Present in the full 57-entry locked dependency graph in the capture artifact | Apache-2.0 |

The SDK package’s optional AI/framework branch remains present in the lock graph but was not imported, configured, or called. Package examples, Doctor, scripts, prompt APIs, and provider/service routes were not exercised.

## Commands and evidence artifacts

All runtime commands below used `PATH=/Users/completetech/.cache/acv1-toolchains/node-v22.23.3-darwin-arm64/bin:$PATH`.

| Command | Exit | Result |
| --- | ---: | --- |
| `make bootstrap check test` | 0 | Offline scripts-disabled install of 57 packages; syntax/import/build and strict public type checks pass; full repository suite 107 passed, 0 failed, 0 skipped. |
| `make verify CASE=AH-10 MODE=mock FIXTURE=neatlogs-sdk-local` | 0 | Real-SDK suite 11 passed, 0 failed, 0 skipped; includes all three finding regressions and SQLite kill/reopen/reproject. |
| `make verify CASE=ACV1-AH05 MODE=mock FIXTURE=jev-phase-b-offline` | 0 | 59 passed, 0 failed, 0 skipped; manifest binds clean worktree and source commit `f92623511ecfc934c0d25ffd7b897867c7876821`; 52 controlled DNS resolutions, 19 HTTPS requests, 18 writes, zero provider/live calls. |
| `python3 -m unittest discover -s tests/config/provider-profile -p 'test*.py'` | 0 | 48 passed. |
| `git diff --check HEAD^ HEAD` | 0 | No whitespace errors. |

SHA-256 artifacts from the final source verification:

| Artifact | SHA-256 | Evidence |
| --- | --- | --- |
| `.runtime-artifacts/neatlogs-sdk-local-capture.json` | `d98a23ba2c5d4022cdb4840e6b6f21840b4cf2b9f18f15c093cff33d05d33873` | Actual Node 22.23.3 / Neatlogs 1.1.28 capture; 3 roots + 3 markers; 57 locked dependencies; 24 source pins; Darwin network deny; all remote/service/Finance/Entire claims false. |
| `.runtime-artifacts/neatlogs-sdk-sqlite-restart.json` | `5cd8d3fbcc77cdf5abf29972ec0282f93c7497457495c68bf5ff5802ec3c2b55` | Three real SQLite kill/reopen/reproject phases; each replay has 4 records, 4 roots, 4 markers, cursor 0, all tables unchanged. |
| `.runtime-artifacts/bootstrap-check-test.log` | `d455bfab7163e900d9435b153d2f5264445d445e968e9993776131039c5d5b1f` | Combined final bootstrap, check, and 107-test output. |
| `.runtime-artifacts/neatlogs-sdk-local-command.log` | `e184072f9a907e5f0d056109ee3e07c6988ccfca7fe9ad27c8159d6a5e61665f` | Final named AH-10 local SDK suite output, 11/11. |
| `.runtime-artifacts/ACV1-AH05-jev-phase-b-offline.tap` | `aff1ec1f898f2b59b8ab28012ba7a8571bf72449e46d0900ccc2a24047e60716` | 59/59 JEV offline TAP. |
| `.runtime-artifacts/ACV1-AH05-jev-phase-b-offline.json` | `6235dda406ea7a9baa4af86e49ddd0795ad37a4ea657514afcff24a97ede07fb` | Clean commit-bound JEV manifest; 9 acceptance groups; no missing groups. |
| `.runtime-artifacts/ACV1-AH05-jev-phase-b-offline.json.sha256` | `376cbbe39c0095278992529784ba97f8d0fda5a75dd4c02ded9b35dc0631c26c` | Adjacent manifest digest record. |
| `.runtime-artifacts/provider-profile-test.log` | `725c578d3371a5cad67a2a168339a48f2999f88d8f7384e987938161e038b320` | 48/48 provider profile tests. |
| `package-lock.json` | `9c136cb70e980c19daf05978ce98a372cf2825fcbbd37f098d583aa7083fff7d` | Unchanged complete 57-package lock. |

The full source graph, individual package integrity/license entries, capture claims, and the restart child artifact hashes are retained in the referenced sanitized local artifacts. `.runtime-artifacts` is ignored by Git. The after-fix source commit has no published CI run because this task did not push; current CI artifact inspection is still required after root publishes a candidate.

## Original acceptance and remaining work

The full AH-10 acceptance remains open. The local projection includes only event/workflow HMAC tokens, sequence/revision, event type, state, fence, and source time. It lacks operation, billable, provider, decision, artifact, usage, fallback-topology, and evaluated-run correlations; none are invented here.

- **AH-10 original case 1:** actual workflow with JEV fallback, provider retry, final verification, and joined artifact/usage references — pending canonical producer fields and an evaluated live/sandbox run.
- **AH-10 original case 2:** synthetic secret/customer canaries pass locally — global evidence/history review, applicable scanning, and owner review remain pending.
- **AH-10 original case 3:** local size, callback, timeout, abort, process-kill, source replay, cursor, and financial outbox preservation are covered — real service outage/retry/volume/remote-flush behavior and the strict bridge remain pending.
- **SPN-01:** actual Neatlogs agent run and inspected service artifact — pending Finance/reliability access, approved account/region/retention, and acceptance.
- **SPN-02:** actual permitted Entire provenance/checkpoint — pending; Entire stays disabled.
- **EVAL-03:** reviewed global evidence index/classification — pending Product/frontend.
- **EVAL-04:** published evidence/history minimization and scanning review — pending Finance/reliability.
- Sponsor acceptance, approved regional retention, model/provider budget, accessible live artifact, dashboard visibility, and cross-repository producer handoffs remain pending.

AH-10 prerequisites and issue ownership/relationships are unchanged. No issue was closed. The two original named unavailable gates remain unavailable: `make verify CASE=AH-10 MODE=mock FIXTURE=redaction-outage` does not become satisfied by the differently named local SDK fixture, and `make verify CASE=AH-10 MODE=live FIXTURE=neatlogs-correlated-run` remains gated on real authorities and service acceptance. SPN-02 recorded Entire provenance remains pending.

## Review boundary

The previous independent Astra review identified NL31-R1/R2/R3 and requested changes; all three now have repository regressions. A fresh full independent review of the exact final base/head/tree pair is still required. This author did not claim an independent Astra review, publish CI, merge, push, or approve the PR. Any root integration after the bounded merge and current exact-candidate CI artifact inspection remain separate required steps.
