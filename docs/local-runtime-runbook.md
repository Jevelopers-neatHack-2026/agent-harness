# Local worker operations runbook

This runbook covers the private mock worker and its SQLite mechanics. It does not enable application authentication, Finance authorization, or provider dispatch. The current worker accepts a synthetic internal scope and the reviewed bounded local plan operations (`number.add` and `text.concat`). It has no HTTP listener or public package export.

## Prerequisites and limits

- Use the pinned Node `22.23.3` toolchain (`.node-version`) and repository lockfile. The runtime requires Node 22.23.3 or a later Node 22 patch.
- Use a local filesystem with SQLite WAL locking. Network filesystems, server SQLite builds, deployment memory/CPU/FD/disk ceilings, and host process limits have not been verified.
- Keep the database path local and restrict its filesystem permissions to the service owner. Do not put credentials, customer source material, or account data in these synthetic fixtures.
- Configured record, byte, queue, worker, lease, and shutdown values are bounded fixture defaults. They are not production capacity recommendations.

The store is schema 6 with checkpoint envelope 2. Schema 6 adds private tenant/project admission, quota counters, persistent fairness cursors, and a nullable worker-policy binding to the same SQLite database. Opening supported schemas 1 through 5 migrates them to 6 in one transaction. A schema 6 store created before the policy-binding column existed receives that column in a transactional compatibility migration; the retained queue, reservations, event/checkpoint bytes, and unknown holds are preserved. Binding a worker policy validates retained active work and reservations before recording the policy. A future schema is refused before the database is changed. Back up the database before an owner-approved migration; recovery uses a compatible binary and owner-approved backup, not downgrade SQL.

## Clean start, bounded local work, and restart

Run the executable clean-start fixture. It creates a new temporary file-backed database, starts the worker, accepts a synthetic plan, verifies its result and safe health snapshot, drains the worker, reopens the same database, and confirms the completed job is returned by the same scoped idempotency key.

```sh
node tests/operations/runbook-clean-start.mjs
```

Expected output is one JSON record with `fixture: "runbook-clean-start"`, `mode: "mock"`, `schemaVersion: 6`, `sigtermDrain: "pass"`, `restart: "pass"`, separate stop-return and process-close durations, and `providerCalls: 0`. The record does not contain tenant or project identifiers.

To start the idle private worker directly:

```sh
node src/worker/private/worker.js --mode mock --database-path ./local-state/workflow.sqlite
```

Send `SIGTERM` from its supervisor or another terminal. It synchronously closes acquisition, drains and reaps its owned children against one shutdown budget, and closes the store only after child writers and durable cleanup have completed. `SIGINT` uses the same path. Signal handlers stay installed during async cleanup, so repeated and mixed signals share the same stop operation. If that bounded attempt cannot finish, the CLI emits an `operations_worker_stop_incomplete` JSON record, keeps the store and signal handlers open, and waits for another `SIGTERM` or `SIGINT` to retry cleanup. The final `operations_worker_stopped` record is emitted only after the store closes; a forced `SIGKILL` cannot claim a clean shutdown. Each owned child has a parent-monitored monotonic runtime deadline independent of runner cooperation; expiry sends `SIGTERM`, then bounded `SIGKILL`, and waits for the actual child close event. The queue stores the owned runner PID with its generation and fence; after lease expiry, a new supervisor checks that PID is no longer live before it reclaims the local slot and advances the fence. If the current owner reaps its own child after its lease expires, it may settle only the matching generation and fence; a newer claim remains protected. An expired run with an unresolved external attempt becomes a reconciliation item; its attempt and opaque hold remain intact and it releases only the local CPU slot. A job is parked for review after 16 owned runner generations rather than adding unbounded event history. The controller resumes only from its durable bounded checkpoint and consumed retry budget.

The direct command has no request listener. Intake in this increment is available only as a private in-process synthetic-mechanics method used by the fixture and tests. Do not expose it as an application API or treat its scope strings as authorization.

## Backpressure and scheduling

Admission stores the workflow, initial checkpoint, immutable work identity, queue counters, event outbox rows, and a conservative whole-lifecycle reservation in one SQLite transaction. The reservation covers bounded checkpoint history/replay, local events, outbox/index growth, worker generations, and terminal cleanup. Each later owned write consumes part of that reservation inside the same transaction; if physical bytes plus outstanding reservations cannot fit, the write fails before mutation and leaves accepted evidence intact. Capacity counts database, WAL, SHM, retained history, outstanding reservations, and free-space headroom. Exact duplicates return the existing job; conflicting use of a scoped idempotency key is rejected. Global, tenant, project, record, UTF-8 byte, event, WAL, database, and free-space limits are checked before admission. Saturation returns typed bounded `OPS_BACKPRESSURE` with `retryAfterMs`; it does not persist an accepted job. Immutable checkpoints and unresolved evidence are never pruned to recover capacity.

Claims use a persistent tenant round cursor followed by a project round cursor, then FIFO within a project. A tenant is limited to at most half of configured queue records and bytes to preserve intake room for another tenant. Claims compare current workflow fence, generation, cancellation flag, lease, and indexed unresolved-attempt barriers in one transaction. The local scheduler installs no provider handler. A bounded scope scan that cannot prove its full result is reported incomplete rather than empty.

## Health and diagnostics

Health is an internal fixed-schema snapshot, not a public endpoint. It reports capped counts, store/schema/resource facts, telemetry cursor/backlog status, and safe status codes. It never returns work payloads, tenant/project IDs, checkpoints, provider responses, opaque hold references, or raw exception strings. A store failure, unsupported/migration-required schema, stale heartbeat, pending external reconciliation, or resource shortage fails local readiness. `externalReady` and `externalDispatchEnabled` remain false in mock mode.

`localReady` means the current snapshot satisfies the shared predicates for one minimum bounded local admission, including record/byte, lifecycle store reservation, WAL/free-space, event-reservation, policy, schema, and freshness checks. It does not promise that a particular larger plan will fit; each submission reserves its own complete lifecycle cost atomically. The aggregate store counters and reserved bytes remain visible separately so operators can distinguish current capacity from the next-admission probe.

`readStoreCompatibility` is a read-only schema probe. It does not create directories or run migrations. A missing store is `store_unavailable`; a supported older store is `migration_required`; a future version is `schema_unsupported`.

## Shutdown, unknown outcomes, and SQLite busy state

Shutdown stops new claims before signaling children. It sends `SIGTERM` only to children created by this worker, escalates to `SIGKILL` after the configured grace period, waits for the owned child close event, and only then fences or parks work. It reports stop-return elapsed time, whether all children were reaped, whether telemetry closed, and whether the store closed. The clean-start fixture measures actual process close separately. Timeout does not mean cleanup completed. `waitForOwnedChildrenIdle()` only observes this supervisor's child processes; it does not mean the durable queue is empty or accepted work has completed.

If a second SQLite writer keeps a reaped runner's outcome from committing through the shutdown deadline, in-process `stop()` returns `stopped: false`, reports pending durable cleanup, and keeps the store and signal handlers open. The CLI emits `operations_worker_stop_incomplete` and remains alive for a retry. The work row stays under its existing generation and fence; it is not reported as requeued or settled. After the lock is released, call `stop()` again or send another termination signal. The retry persists that same child outcome before closing the store. Do not reopen the store as if the first stop completed.

The local executor is a static bounded pure-operation registry. Arbitrary callbacks do not run in-process. Existing JEV controlled tests exercise `sendJevOnce` through the private test-only orchestrator closure using actual child `SIGTERM` and synthetic DNS/HTTPS responders. Pre-send aborts remain not dispatched; after a possible write, the outcome remains unknown. The transport is not called twice. Unknown attempts and opaque holds remain available for reconciliation and cannot become zero usage through queue cleanup or lease expiry.

`STORE_BUSY` at admission is converted to typed bounded backpressure. During an already-admitted local pure step only, the private controller can classify the exact completion attempt after a confirmed inactive transaction; it may recognize the same committed event/checkpoint or retry that same step result and event identity within the configured busy, lease, plan, and parent-runtime budgets. A retry rebuilds checkpoint budget metadata from the refreshed clocks, so its full serialized checkpoint may differ. It does not rerun the operation or retry admission, and an uncertain transaction/read result stops for owner review. This narrow completion recovery does not change busy handling for other store operations. A store unavailable, a lock extending beyond these bounds, migration mismatch, or future schema disables local readiness and leaves durable rows untouched. Do not delete SQLite WAL files, edit the schema manually, or clear attempts/holds to recover capacity. Preserve the database and use the owner path for compatible migration, backup, or unresolved external outcome review.

## Telemetry and retention

Worker lifecycle events use a fixed event allowlist and existing HMAC-tokenized local telemetry projection. A failed or hung sink leaves its cursor pending; it does not overlap another sink call or release a financial hold. The current worker CLI does not configure a remote sink. The test suite injects a bounded local sink to verify timeout and cursor retention; this is not Neatlogs ingestion or sponsor evidence. Opt-in Neatlogs export composes the same exporter with a durable local outbox sink outside the worker. See `docs/runbooks/neatlogs.md`, section "Runtime path". The worker itself still performs no network I/O.

Retention is `retention_not_configured`. The harness exposes a bounded owner request record but performs no deletion, settlement, or hold release. Immutable checkpoints, workflow events, receipts, JEV markers, observations, telemetry outbox rows, and unknown outcomes remain retained until their authoritative owners provide an accepted policy and acknowledgment contract. Keep any stable telemetry tokenization key in its approved secret store; this worker supplies no key and uses no raw-ID fallback.

## Exact offline checks

Both required fixture targets execute all 13 parent groups and 52 named local cases (65 named test cases total), and fail on any missing case, test error, or skip. Run them through the fixed owner routes so the process-group registry, watchdog, gated launch, and cleanup receipt are established:

```sh
python3 .github/scripts/run_ops_owned_command.py --route ops-quota-shutdown-restart
python3 .github/scripts/run_ops_owned_command.py --route ops-runbook-clean-start
```

The quota route runs the existing inner command `make verify CASE=AH-11-OPS MODE=mock FIXTURE=quota-shutdown-restart`; the clean-start route runs `make verify CASE=AH-11-OPS MODE=mock FIXTURE=runbook-clean-start`. Keep these inner commands unchanged. Running either Make command directly remains fail-closed when the owner registry is missing.

The tests use synthetic SQLite data and controlled DNS/HTTPS objects only. Provider/API calls are zero. Test instrumentation blocks uncontrolled Node network APIs inside the JEV boundary child, but is not OS-wide egress enforcement. Hosted Linux, the eventual deployment host, and external service or Finance acceptance require separate evidence.
