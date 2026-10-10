# Bounded private runtime: Phase B/C

**Status:** private durable local-runtime prerequisite only. This increment does not pass full AH-04 or AH-08.

## Scope and ownership

Phase A supplies private bounded plan, registry, limit, and recovery-classification mechanics. Phase B/C adds a durable controller and read-only recovery inspection over the existing SQLite store. The controller and inspection functions remain private and are not exported from the package root. The package test target includes the planning and recovery tests, and the Makefile has a clearly labeled private prerequisite target; neither is a full AH-04/AH-08 acceptance target.

The integrated Phase B base is `bb280f2bfcf390b5d228148fb961ed0b2af326c9`, which includes telemetry and merged JEV PR 29. The database schema is 5 and the checkpoint envelope schema is 2. This increment adds no database migration and does not change existing telemetry, JEV, receipt, observation, cursor, privacy, event, or outbox semantics. The controller checkpoint kind below is an independent nested private payload version.

The private controller uses a consistent file-backed SQLite workflow/checkpoint/unknown-attempt snapshot, then commits each admission or completion through one fenced `BEGIN IMMEDIATE` transaction. `recordDispatchIntent` remains an external operation path and is never used for local steps. A local step creates no operation attempt, transport attempt, response receipt, hold, provider observation, or JEV observation.

The merged telemetry and JEV contracts remain untouched. Local checkpoint commits reuse the existing checkpoint, workflow event, and normal event outbox tables without changing telemetry cursor, redaction, queue, JEV observation, hold, receipt, or migration semantics.

## Private plan and registry

Plans are JSON text with this exact private shape:

~~~json
{
  "kind": "agent-harness.private-synthetic-plan",
  "version": 1,
  "steps": [
    {
      "id": "first",
      "operation": "number.add",
      "dependsOn": [],
      "args": { "left": 19, "right": 23 }
    },
    {
      "id": "second",
      "operation": "number.add",
      "dependsOn": ["first"],
      "args": { "left": { "$result": "first" }, "right": 1 }
    }
  ]
}
~~~

Only JSON text is accepted at the plan boundary. Objects, accessors, proxies, toJSON hooks, unknown fields, nonfinite numbers, duplicate IDs, invalid references, cycles, and oversized/deep graphs are rejected before a local operation is admitted. Result references name a whole prior result, and that step must also be an explicit dependency. Dependencies are evaluated in stable ASCII step-ID order. Unknown operations are rejected. The registry is a frozen static manifest with no registration, plugin, dynamic import, code, URL, shell, or function-injection path.

The only executable operations are bounded pure functions:

| Operation | Input | Computation |
| --- | --- | --- |
| number.add | Two bounded safe integers | Adds them and validates the bounded integer result |
| text.concat | Up to 32 bounded strings | Concatenates them and validates the UTF-8 output size |

The manifest also records network.fetch as external, provider.image and provider.reason as provider effects, and system.shell as privileged. Each is explicitly unavailable. The registry has no dispatch handler for any of these effects, and no dependency mode changes that result. These names are fixed examples of unavailable effects; they do not create a provider contract.

compilePlan returns an opaque frozen handle pinned to the canonical plan hash, static registry version, tightened-limit hash, and a workflow-scoped run identity. executeLocalStep and executePlanLocally compute actual local values. Each result carries hashes bound to the plan, registry, workflow-scoped run identity, resolved input, and output. Those hashes provide consistency and identity; they do not prove that an arbitrary saved output was computed. Every retained output is recomputed by the fixed registry handler before reuse, recovery classification, or SQLite completion, then compared with the saved value. Owned result data is frozen.

Each operation definition has a semantic version, and the registry has an explicit semantic version. Version 1 preserves the existing shipped registry hash for checkpoint compatibility. A handler or validation semantic change must bump the registry semantic version so recovery stops on a changed registry pin. This revision is explicit metadata, not a hash of JavaScript function source.

Local evaluation is in-process. `runBoundedLocalPlan` uses only the static registry, persists an allowance before executing a pure step, and atomically commits a validated result with the next checkpoint/event/outbox. Each new admission is fenced to the current workflow revision and checkpoint sequence, live lease, cancellation state, external-attempt barrier, plan, registry, limits, and base checkpoint. Completion repeats the fresh state and fence checks. A committed result is returned as private synthetic evidence while the workflow remains running; it does not promote workflow status or imply canonical approval.

## Limits, clocks, and diagnostics

The default finite ceilings are fixed in limits.js. Callers may lower them through JSON limit configuration; plan content cannot raise them.

| Limit | Default ceiling |
| --- | ---: |
| Plan UTF-8 bytes | 32,768 |
| Steps / dependency edges | 32 / 64 |
| JSON or dependency depth | 12 |
| Ready queue | 16 |
| Per-step input / result bytes | 8,192 / 8,192 |
| Total result / saved-results bytes | 65,536 / 131,072 |
| Step attempts / local replays | 64 / 32 |
| Replans / total iterations | 8 / 64 |
| Elapsed time / observed step duration | 30,000 ms / 1,000 ms |
| Recovery attempt-state records | 32 |

Limits and counters only stop work. They are not authorization, provider capacity, finance reservation, or spending permission. There is no cost counter or usage claim in this increment.

Budget snapshots include the pinned wall deadline, last wall sample, elapsed allowance, limit-set hash, and consumed counters. `advanceBudget` returns a new snapshot; the controller persists it atomically with admission before invoking a step, and loads the saved value after restart. A replay after an uncommitted pure computation consumes another attempt and retry allowance, and cannot exist without the original persisted step admission. Validation requires `stepAttempts = stepsAdmitted + localRetries`, `iterations >= stepAttempts + replans`, and `stepsAdmitted` to equal completed results plus the one optional pending admission. Retained output bytes are recomputed from each serialized output and must equal the persisted counter. A result already present in the completed checkpoint is returned without replay or additional writes. Limits and counts remain stopping facts only.

Within one process, elapsed time uses a monotonic clock. Expiry is checked before computation, after computation, and again immediately before durable completion, including the final step. Admission and completion also recheck the fresh transaction clock against the prepared sample and persisted workflow update timestamp. Any backward movement fails with CLOCK_ROLLBACK; only forward snapshot drift up to 250 ms is tolerated, and expiry still fails closed. Across restarts, the persisted wall deadline is checked against the injected wall clock, which must remain a trusted source. The reviewed local handlers have bounded input loops; a duration breach is observed after a handler returns and does not terminate it. No Promise.race or AbortSignal is claimed to preempt arbitrary synchronous JavaScript.

Plan, limit, result, recovery, and stored checkpoint JSON rejects duplicate object members. Plan and limit numbers must use canonical safe-integer tokens; integer spellings such as decimals or exponents that normalize to the same JavaScript value, negative zero, unsafe integers, and non-finite numbers are rejected before identity calculation. Recovery keeps that strict numeric domain for workflow fields, controller metadata, budgets, limits, plans, and results. It applies the existing schema 5 finite-number domain only below `checkpoint.data.baseCheckpointData`, where the original checkpoint is opaque to this controller. This scoped exception preserves large finite values from an existing generic checkpoint through recovery serialization and SQLite reopen; duplicate keys, non-finite values, negative zero, byte/depth/node limits, and strict controller-owned numeric fields remain enforced.

Plan and recovery errors expose fixed safe codes/messages only. Recovery output contains only a classification, a fixed reason code, and actionable: false; it does not copy source text, provider payloads, attempt IDs, tenant data, or hold references into diagnostics.

## Checkpoint round-trip and result growth

The schema-5 generic store bounds remain depth 128, 10,000 nodes, 256 Ki UTF-16 source units, and 1 MiB of serialized checkpoint JSON. Recovery derives its projected-snapshot bounds from that same contract, adding four wrapper levels and 8 KiB for workflow and attempt metadata. Controller restore accepts private state through the store's 1 MiB domain instead of imposing the earlier 220,000-byte cap.

Before the first durable admission, each later admission, and each completion, the controller checks the actual checkpoint hash with the same schema-5/envelope-2 serializer used by the store, then checks the recovery projection bounds. This preserves JavaScript's numeric ordering for array-index object keys while retaining the store's existing schema and bytes. The controller also reserves the configured plan's maximum remaining result JSON, its wrapper-escaping overhead, the longest step IDs, replay admission labels, and the largest legal elapsed-time and counter values. The projection covers every future controller state under the pinned plan and limits without running a step or creating a result. Reservation strings are used for measurement only and are never persisted. Completion is checked again using the actual computed results before its transaction.

A generic checkpoint remains readable by the store even when its depth or size cannot fit after the private wrapper and complete future-state reserve are added. In that case private adoption stops before a workflow revision, checkpoint, event, outbox row, consumed counter, or SQLite sequence changes. The original checkpoint is retained in full; it is not truncated, rewritten, or reinterpreted. The private reserve may conservatively reject a plan/base combination that cannot be proven to fit within the existing store bounds.

Crash-fixture markers are written to a private temporary path and atomically renamed only after the full JSON message is present. The parent always kills and reaps its owned paused child in a `finally` path, including marker wait, parse, assertion, and timeout failures; process-kill tests retain their actual SIGKILL boundaries.

## Private recovery classification

classifyRecovery accepts bounded JSON text that projects two workflow reads (before and after), the selected checkpoint, the known attempt states, and a trusted observation time. The two workflow projections must agree on ID, state, revision, and checkpoint sequence before a local classification is returned. The controller and read-only inspector project one consistent store snapshot into that classifier input.

A supported private checkpoint is nested under the existing version 2 checkpoint envelope and uses kind `agent-harness.private-bounded-local-checkpoint`, nested version 2. It pins workflow ID, the original base checkpoint, plan JSON/hash, registry version, run identity, limits/hash, budget snapshot, completed-result JSON, and an optional pending step ID. The classifier verifies the checkpoint SHA-256 and these identities. The store controller reads workflow/checkpoint/unknown attempts from one SQLite snapshot. Recovery inspection uses stable keyset pagination bound to a workflow revision, returns a fixed maximum page, and exposes `hasMore`/`incomplete` explicitly. It does not copy opaque hold references or response bodies into its output. It returns one of:

- local-ready-candidate: a deterministic next step can be admitted; nothing is admitted by the classifier.
- local-replay-candidate: an admitted pure step is incomplete and another replay allowance is available; nothing is replayed by the classifier.
- durable-evidence-present: all private local results match the pinned identities; no workflow state is promoted.
- external-reconciliation-required: an existing intent, unknown outcome, response-received attempt, awaiting-response state, or reconciliation-pending state remains a barrier.
- stop or quarantine: missing, foreign, corrupt, unsupported, stale, terminal, review, cancelled, or invalid local data cannot be adopted.

Terminal states and review_required never become runnable. Unresolved external attempt evidence takes precedence over local recovery. The classifier keeps its hard 32-record scan bound and also enforces the pinned, potentially lower `maxRecoveryAttemptStates` cap before any local candidate. A truncated unresolved-attempt snapshot remains a barrier; recovery pages expose `hasMore` and `incomplete` and are revision-pinned. Recovery inspection is read-only; it is not a restart sweeper and cannot query providers, update attempts, ingest external responses, settle/release holds, or promote receipts. A stale page cursor/revision stops safely. A checkpoint marked with `lastAdmissionKind: replay` must show at least one consumed local retry in both the classifier and durable-store saved-state validator, for pending and completed checkpoints; a later ordinary step can still retain earlier retries.

The private controller has no caller-provided `AbortSignal` parameter. A `signal` field is rejected as an unknown input before its value is inspected. On Node 22, the intrinsic `AbortSignal.prototype.aborted` getter reads an internal symbol through its receiver; a proxy can intercept that symbol and mask even the intrinsic getter. Neither native nor wrapped signals are used as controller authority. Cancellation remains the durable workflow cancellation/CAS state checked before admission and before commit. The test probes this runtime behavior and confirms that the controller input rejects all signal values without reading them, without treating a private caller as an attacker.

## Phase B/C acceptance evidence

The following table maps all 13 mandatory groups. These local cases exercise private mechanics over the actual merged store. They do not replace canonical, Finance, tenant-authority, planner, provider, live, evaluation, or release acceptance.

| Group | Phase B/C local evidence | Remaining acceptance |
| --- | --- | --- |
| PLAN_BOUNDS | Controller and planner tests reject hostile inputs, bad graphs, unknown operations, unsupported versions, duplicate JSON members, unsafe/normalized integer spellings, and graph/input/result limit overruns before durable admission. Deterministic dependency order is checked. | Re-run through any canonical planner integration; no production planner is installed. |
| NO_CAPABILITY_GRANT | Unknown, external, provider, and privileged operations stop before admission in mock/recorded/sandbox/live labels; intercepted `fetch` count stays zero. The private API accepts no injected registry, mode, grant, approval, or hold. | Actual tenant and Finance authorization remain external gates; offline judgments cannot grant them. |
| EXECUTABLE_LOCAL_PATH | File-backed controller computes dependent `number.add` results, persists admission/completion, recomputes retained outputs on reuse, and returns committed results without another event or attempt. A SQLite-reopen probe with self-rehashed `900`/`902` dependent outputs is quarantined with no new event. The B3 matrix also resumes generic depth-20/24/30 and 200,000/230,000-character checkpoints without losing their original data. B4 round-trips top-level, nested, array-contained, and mixed numeric-index keys plus named-key controls through normal completion, both real kill boundaries, reopen, and reuse. | This is private synthetic computation; canonical request/result and authenticated planner flow remain absent. |
| CRASH_BUDGET | Four child-process SIGKILL points cover before admission, after admission, after compute/before completion, and after completion. Reopen preserves counts; only an admitted pure step replays with another allowance; committed results do not repeat. Recovery rejects pending work without an admission and counter histories inconsistent with step/replay/replan transitions. After-admission and after-compute crashes also round-trip safe-integer boundaries, unsafe finite values, fractions, negative values, and large finite exponents in the opaque original checkpoint, through read-only inspection, resume, and completed reuse. B3 repeats actual process-kill/reopen paths for supported generic depth and byte controls. B5 exercises maximum-length later IDs, replay metadata, counter digit growth, and escaped UTF-8 results across ten dependent steps and both crash boundaries. | No arbitrary synchronous-code preemption is claimed. Repeat on supported platform/SQLite configurations. |
| CAS_CANCEL_LEASE | Independent file-backed processes race two admissions, completion/cancellation, and stale lease/takeover. Separate admission-versus-cancellation cases cover concurrent release, admission-first, and cancel-first with the same workflow revision and fence, then reopen and compare workflow/checkpoint/event/outbox state. Cancellation is also placed at admission and post-compute boundaries. One valid CAS wins and late completion cannot resurrect work. | Canonical authority and external reservation revocation races need accepted fixtures. |
| LIMITS_TIME | Planner tests cover exact caps and check monotonic expiry after final and intermediate computation. The file-backed controller rejects a final result at the controlled 10 ms sample against a 5 ms deadline. Admission and completion transactions reject 1/100/250 ms backward wall-clock samples after the controller's prior check, preserve the four-table snapshot and consumed replay allowance after reopen, accept unchanged/1/250 ms forward controls, reject 251 ms forward drift, and reject a sample at the pinned deadline. | Trusted wall time across restarts remains an assumption; uncooperative handlers remain excluded. |
| RESULT_VALIDITY | Accessor inputs and provider-shaped/foreign results are rejected without invoking getters or copying markers; self-rehashed altered outputs fail after independent recomputation; per-result and aggregate serialized-byte caps are reconciled with persisted counters, including multibyte and exact-boundary cases. | Real provider result contracts remain external. |
| EXTERNAL_UNKNOWN_BARRIER | Actual store intent, outcome-unknown, and response-received fixtures block local work. Attempt state, receipt/checkpoint, and opaque hold evidence remain unchanged by the controller; it creates no local attempt, receipt, retry, or hold write. | Provider status lookup and financial reconciliation remain unimplemented. Receipt creation in tests is only an offline fixture via the existing store API. |
| RECOVERY_COMPATIBILITY | Private v1/foreign-prefix and missing-plan checkpoints stop without writes; classifier tests cover missing/old/future/corrupt/foreign data and terminal/review states, duplicate JSON, the exact pinned recovery-attempt cap, and the scoped preservation of generic finite values in `baseCheckpointData` while controller-owned numeric fields stay strict. The durable-store validator rejects replay-kind pending and completed checkpoint state without a consumed retry. B3 aligns recovery depth/bytes with generic schema-5 bounds and proves a generic maximum-depth value remains readable while private adoption rejects wrapper overflow without writes. B4 uses the exact schema-5 store serializer for recovery hashes, preserving JavaScript array-index key ordering without changing envelope-2 bytes. Existing schema 1-to-5 migration tests remain in the full store suite. Read-only attempt inspection pins workflow revision, pages in binary key order, and reports incompleteness; unresolved rows outside a displayed page still block local recovery. | No schema or envelope migration is added; future schemas remain unsupported by this branch. |
| ROLLOUT_AND_GATE | No production planner, canonical decision port, JEV route, promotion, Finance input, or live capability is installed. Package-root checks keep private entrypoints inaccessible. | Full AH-04/AH-06/AH-08 and release gates require accepted external artifacts below. |
| TRANSACTION_LOCAL_CHECKPOINT_EVENT | File-database triggers fail after each workflow/checkpoint/event/outbox write for admission and completion; full table snapshots match before/after and after reopen. Duplicate event IDs cannot add checkpoints. Four SIGKILL boundaries distinguish pending from committed results. Reopened, correctly rehashed but fabricated or over-limit private checkpoints stop/quarantine before any new event. B3 proves the byte-adjacent checkpoint whose reserved completion growth crosses 1 MiB leaves every table and `sqlite_sequence` unchanged before and after independent reopen. B5 proves that a future-state capacity rejection leaves all tables and `sqlite_sequence` unchanged before and after reopen, and that the adjacent supported long-ID plan completes and reuses. B6 publishes complete markers atomically and kills/reaps owned children when marker parsing or waiting fails. | Re-run against supported CI SQLite builds and future migrations. |
| REVOCATION_READMISSION | Controlled cancellation, multiprocess cancellation/completion and lease races, changed plan/registry pins, and final post-compute expiry stop stale work with counters unchanged. Native, masked-proxy, revoked-proxy, and aborted signals are not read as authority; a `signal` input is rejected before store writes. External operations remain unavailable in every mode. | Real authority/reservation revocation and cached-rank behavior remain unavailable without canonical contracts. |
| UNKNOWN_EXTERNAL_QUARANTINE | Provider-shaped, cross-workflow, altered-plan, and altered-registry result data fail before completion; raw fixture markers do not appear in diagnostics. Unknown/late attempt evidence is not used as local completion. | Durable provider-response ingestion remains in the owned JEV/canonical pathways; this controller is not an ingestion shortcut. |

## Issue and release acceptance remains open

This increment supplies a narrow private prerequisite for issue 19 (ACV1-AH04), issue 18 (ACV1-AH08), and a limited input to issue 17 (ACV1-AH06). All original acceptance conditions remain unchanged:

- **AH-04 / CON-03 / AGT-03 / AGT-04 / ACV1-JEV-08:** This increment contributes private local bounds, deterministic computation, and durable progress. It does not implement an authenticated ProjectRequest, canonical action DTO, actual planner provider/model, JEV ranking, budget metering, source retrieval, authorization, or a live planner happy path. The required bounded-planner-happy-path must use a real configured provider call with explicit model/snapshot, authorized identity, metered attempt, usage-or-unknown evidence, and persisted progress.
- **AH-08 / AGT-02 / AGT-03:** This increment persists only local pure-step admission/completion and read-only recovery evidence. It does not retry, reconcile, or settle provider effects.
- **FIN-03 / FIN-04 / BUD-03:** No distinct billed retries, unknown-billing evidence, conservative held-budget proof, finance ledger, or hold lifecycle is implemented. Existing opaque hold references remain external-owned and untouched.
- **AH-02:** No trusted tenant/project/artifact lookup, authenticated create/status/cancel/correct/resume command, scoped command idempotency, progress cursor, or authorization is implemented. A workflow ID is not tenant authority.
- **AH-06 / ACV1-JEV-04/05:** No canonical decision receipt, policy, cache, promotion, rollback, or active/shadow route is introduced. Synthetic local result hashes are not DecisionReceipts or cache authority.
- **SPN-01, SPN-02, provider/live, full AH-04/AH-08 and final-journey acceptance:** No Neatlogs/Entire artifact, paid provider call, live run, or external application acknowledgement is produced. Mock/private tests cannot satisfy those criteria.

Required external artifacts remain: accepted ProjectRequest/AuthorizationPort and versioned consumer commands; atomic Finance reservation/attempt and usage-or-unknown contracts; canonical DecisionPort/action/result/receipt/policy/promotion/cache contracts; accessible evaluation promotion and rollback authority; selected planner and JEV provider/model/account access plus budget and verified retry/status semantics. No missing DTO or authority is inferred here.

## Sequenced follow-up

**Phase B/C — private durable local controller and recovery:** implemented over merged schema 5 without a database or checkpoint-envelope migration. It uses fenced admission/completion over the existing checkpoint/event/outbox tables, persists limits/counters/deadline in a private nested version, and exposes bounded read-only attempt pages. No external receipt, operation attempt, transport, hold, or JEV observation is created by local work. Existing external unknown work stays unresolved.

**Phase D — canonical integration:** after the external contracts and owners above deliver accepted fixtures and consumer acknowledgements. Add real authorization, planner/JEV and finance boundaries only against their owned interfaces. Re-run all 13 groups through the integrated controller and the unchanged full issue criteria. A fresh independent review must inspect the exact integrated base/head before any merge.
