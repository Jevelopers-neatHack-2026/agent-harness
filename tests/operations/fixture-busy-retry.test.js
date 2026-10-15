import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import childProcess from 'node:child_process';
import { syncBuiltinESMExports } from 'node:module';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { test } from 'node:test';
import { DurableWorkflowStore } from '../../src/internal/index.js';
import { runtimeError } from '../../src/internal/errors.js';
import { OPERATIONS_STORAGE_CAPABILITY as CAP } from '../../src/internal/private-ops-store-capability.js';
import { operationsStoragePolicy, readOperationsConfig, validateLocalPlanWork } from '../../src/worker/private/config.js';
import { createOperationsWorker } from '../../src/worker/private/worker.js';
import { encodeFixtureRetryDiagnostic, observeFixtureTransaction, parseFixtureRetryDiagnostic,
  publishFixtureRetryDiagnostic, runFixtureBusyRetry } from './fixture-busy-retry.js';
import { encodeC04CompletionDiagnostic, readC04FinalRow } from './fixture-recovery-diagnostics.js';

const EXEC = Object.getOwnPropertyDescriptor(DatabaseSync.prototype, 'exec');
const PLAN = JSON.stringify({ kind: 'agent-harness.private-synthetic-plan', version: 1,
  steps: [{ id: 'sum', operation: 'number.add', dependsOn: [], args: { left: 20, right: 22 } }] });
const busyClaim = (extra = {}) => runtimeError({ module: 'state', operation: '_operationsClaim',
  code: 'STORE_BUSY', retryable: true, safe_message: 'Synthetic native contention control.', ...extra });
const refusal = () => ({ accepted: false, code: 'OPS_BACKPRESSURE', reasonCode: 'STORE_BUSY', retryAfterMs: 40 });
const options = (invoke, extra = {}) => ({ kind: 'claim', invoke, deadlineNs: process.hrtime.bigint() + 5_000_000_000n,
  busyTimeoutMs: 1, delayMs: 5, ...extra });

function locked(t) {
  const directory = mkdtempSync(join(tmpdir(), 'ops-fixture-native-busy-'));
  const path = join(directory, 'fixture.sqlite');
  const holder = new DatabaseSync(path, { timeout: 1 });
  holder.exec('CREATE TABLE control (value INTEGER)');
  const contender = new DatabaseSync(path, { timeout: 1 });
  holder.exec('BEGIN IMMEDIATE');
  let released = false;
  const release = () => { if (!released) { holder.exec('ROLLBACK'); released = true; } };
  t.after(() => { release(); holder.close(); contender.close(); rmSync(directory, { recursive: true }); });
  return { holder, contender, release, path };
}

function mappedClaim(db, error = busyClaim()) {
  return () => { try { db.exec('BEGIN IMMEDIATE'); } catch { throw error; } return null; };
}

test('fixture observer preserves native return, error, receiver and original descriptor', t => {
  const { contender } = locked(t);
  let caught;
  const observed = observeFixtureTransaction(() => {
    try { contender.exec('BEGIN IMMEDIATE'); } catch (error) { caught = error; throw error; }
  });
  assert.equal(observed.error, caught);
  assert.equal(observed.failed, true);
  assert.equal(observed.proof.eligible, true);
  assert.deepEqual(observed.proof, { eligible: true, restored: true, boundary: 'begin', nativePrimaryCode: 5,
    beforeInactive: true, afterInactive: true });
  assert.deepEqual(Object.getOwnPropertyDescriptor(DatabaseSync.prototype, 'exec'), EXEC);
  const sentinel = Object.freeze({ sentinel: true });
  assert.equal(observeFixtureTransaction(() => sentinel).value, sentinel);
  const wrongReceiver = observeFixtureTransaction(() => DatabaseSync.prototype.exec.call({}, 'BEGIN IMMEDIATE'));
  assert.equal(wrongReceiver.error instanceof TypeError, true);
  assert.equal(wrongReceiver.proof.eligible, false);
  assert.equal(wrongReceiver.proof.beforeInactive, null);
});

test('actual expired recovery retries only inactive native BEGIN and preserves exact generation, fence and cap', async t => {
  const directory = mkdtempSync(join(tmpdir(), 'ops-fixture-recovery-'));
  const path = join(directory, 'fixture.sqlite');
  let now = 1_000;
  const config = readOperationsConfig({ workerCount: 1, maxInFlightGlobal: 1, maxInFlightTenant: 1, maxInFlightProject: 1 });
  const policy = operationsStoragePolicy(config);
  const store = DurableWorkflowStore.open({ databasePath: path, busyTimeoutMs: 20, clock: () => now });
  store._operationsSetResourceCeilings(CAP, { maxStoreBytes: config.maxStoreBytes,
    maxWalBytes: config.maxWalBytes, minFreeDiskBytes: config.minFreeDiskBytes });
  const work = validateLocalPlanWork({ planJson: PLAN, limitsJson: '{}' }, 'retry-workflow', config);
  const admitted = store._operationsAdmit(CAP, { tenantId: 'retry-tenant', projectId: 'retry-project',
    idempotencyKey: 'same-recovery', workJson: work.workJson, policy });
  const first = store._operationsClaim(CAP, { ownerId: 'initial-owner', policy }).claim;
  now = first.leaseExpiresAtMs + 1;
  const writer = new DatabaseSync(path, { timeout: 20 });
  writer.exec('BEGIN IMMEDIATE');
  let active = true;
  t.after(() => { if (active) writer.exec('ROLLBACK'); writer.close(); store.close(); rmSync(directory, { recursive: true }); });
  const input = Object.freeze({ ownerId: 'recovery-owner', policy, recoverableJobIds: [admitted.jobId] });
  let sleeps = 0;
  const result = await runFixtureBusyRetry(options(() => store._operationsClaim(CAP, input), {
    busyTimeoutMs: 20, delayMs: config.pollIntervalMs,
    sleep: async ms => {
      sleeps += 1;
      assert.equal(ms, config.pollIntervalMs);
      assert.deepEqual(Object.getOwnPropertyDescriptor(DatabaseSync.prototype, 'exec'), EXEC);
      assert.equal(writer.prepare('SELECT COUNT(*) AS n FROM workflow_events WHERE event_type=?').get('operations_work_claimed').n, 1);
      writer.exec('ROLLBACK'); active = false;
    }
  }));
  assert.equal(result.failed, false);
  assert.equal(result.value.claim.jobId, admitted.jobId);
  assert.equal(result.value.claim.generation, first.generation + 1);
  assert.equal(result.value.claim.fence, first.fence + 1);
  assert.equal(sleeps, 1);
  assert.deepEqual(result.diagnostic.attempts.map(a => [a.outcome, a.code, a.eligible, a.retryScheduled]),
    [['error', 'STORE_BUSY', true, true], ['claim', 'none', false, false]]);
  assert.equal(writer.prepare('SELECT COUNT(*) AS n FROM workflow_events WHERE event_type=?').get('operations_work_claimed').n, 2);
  assert.equal(writer.prepare('SELECT in_flight FROM operations_meta').get().in_flight, 1);
});

test('actual public admission uses its original retryAfter and same request without recreating a worker or launching children', async t => {
  const directory = mkdtempSync(join(tmpdir(), 'ops-fixture-admission-'));
  const path = join(directory, 'fixture.sqlite');
  const worker = createOperationsWorker({ databasePath: path, ownerId: 'same-admission-owner' });
  const originalSpawn = childProcess.spawn;
  let childStarts = 0;
  childProcess.spawn = () => { childStarts += 1; throw new Error('This admission-only control may not launch a child.'); };
  syncBuiltinESMExports();
  void worker.start();
  const writer = new DatabaseSync(path, { timeout: 20 });
  writer.exec('BEGIN IMMEDIATE');
  let active = true;
  t.after(async () => {
    try {
      if (active) writer.exec('ROLLBACK'); writer.close();
      assert.equal((await worker.stop('test')).storeClosed, true);
      assert.equal(childStarts, 0);
    } finally { childProcess.spawn = originalSpawn; syncBuiltinESMExports(); rmSync(directory, { recursive: true }); }
  });
  const input = Object.freeze({ trustedSyntheticScope: Object.freeze({ tenantId: 'retry-tenant', projectId: 'retry-project' }),
    idempotencyKey: 'same-admission', planJson: PLAN });
  let sleeps = 0;
  const result = await runFixtureBusyRetry(options(() => worker.submitSyntheticWork(input), {
    kind: 'admission', busyTimeoutMs: 20,
    sleep: async ms => {
      sleeps += 1; assert.equal(ms, 40);
      assert.deepEqual(Object.getOwnPropertyDescriptor(DatabaseSync.prototype, 'exec'), EXEC);
      assert.equal(writer.prepare('SELECT COUNT(*) AS n FROM operations_work').get().n, 0);
      writer.exec('ROLLBACK'); active = false;
    }
  }));
  assert.equal(result.failed, false);
  assert.equal(result.value.accepted, true);
  assert.equal(sleeps, 1);
  assert.deepEqual(result.diagnostic.attempts.map(a => [a.outcome, a.reasonCode, a.eligible, a.retryScheduled]),
    [['refused', 'STORE_BUSY', true, true], ['accepted', 'none', false, false]]);
  assert.equal(worker.submitSyntheticWork(input).jobId, result.value.jobId);
  assert.equal(writer.prepare('SELECT COUNT(*) AS n FROM operations_work').get().n, 1);
});

test('spoofed typed/raw BUSY, public refusal and unrelated claim errors cannot authorize retry', async t => {
  const { contender } = locked(t);
  const raw = Object.assign(new Error('canary'), { code: 'ERR_SQLITE_ERROR', errcode: 5 });
  const cases = [
    options(() => { throw busyClaim(); }), options(() => { throw raw; }),
    options(() => refusal(), { kind: 'admission' }),
    options(mappedClaim(contender, busyClaim({ module: 'other' }))),
    options(mappedClaim(contender, busyClaim({ operation: '_operationsFinish' }))),
    options(mappedClaim(contender, busyClaim({ retryable: false }))),
    options(mappedClaim(contender, busyClaim({ code: 'STALE_FENCE' }))),
    options(() => contender.exec('BEGIN IMMEDIATE'))
  ];
  for (const input of cases) {
    let calls = 0; let sleeps = 0;
    const result = await runFixtureBusyRetry({ ...input, invoke: () => { calls += 1; return input.invoke(); },
      sleep: async () => { sleeps += 1; } });
    assert.equal(calls, 1); assert.equal(sleeps, 0);
    assert.equal(result.diagnostic.attempts[0].eligible, false);
    assert.equal(result.diagnostic.attempts[0].retryScheduled, false);
  }
});

test('real body, commit and rollback histories and nested transactions remain ineligible', t => {
  const { contender, holder, release } = locked(t);
  release();
  const body = observeFixtureTransaction(() => {
    contender.exec('BEGIN IMMEDIATE');
    try { throw busyClaim(); } finally { contender.exec('ROLLBACK'); }
  });
  assert.equal(body.failed, true); assert.equal(body.proof.eligible, false);
  holder.exec('BEGIN');
  holder.prepare('SELECT * FROM control').all();
  const commit = observeFixtureTransaction(() => {
    contender.exec('BEGIN IMMEDIATE');
    contender.prepare('INSERT INTO control VALUES (?)').run(1);
    try { contender.exec('COMMIT'); } finally { contender.exec('ROLLBACK'); }
  });
  assert.equal(commit.failed, true); assert.equal(commit.error.errcode & 0xff, 5);
  assert.equal(commit.proof.eligible, false);
  holder.exec('ROLLBACK');
  contender.exec('BEGIN IMMEDIATE');
  const nested = observeFixtureTransaction(mappedClaim(contender));
  assert.equal(nested.proof.beforeInactive, false); assert.equal(nested.proof.eligible, false);
  contender.exec('ROLLBACK');
  const rollback = observeFixtureTransaction(() => {
    contender.exec('BEGIN IMMEDIATE'); contender.exec('ROLLBACK'); contender.exec('ROLLBACK');
  });
  assert.equal(rollback.failed, true); assert.equal(rollback.proof.eligible, false);
});

test('observation overflow, close/unknown state and descriptor mutation never permit retry', t => {
  const { contender, path } = locked(t);
  const overflow = observeFixtureTransaction(() => {
    for (let i = 0; i < 6; i += 1) { try { contender.exec('BEGIN IMMEDIATE'); } catch {} }
    throw busyClaim();
  });
  assert.equal(overflow.proof.eligible, false);
  assert.equal(overflow.proof.boundary, 'unproven');
  const closed = new DatabaseSync(path, { timeout: 1 });
  const unknown = observeFixtureTransaction(() => {
    try { closed.exec('BEGIN IMMEDIATE'); } catch { closed.close(); throw busyClaim(); }
  });
  assert.equal(unknown.proof.eligible, false);
  const mutated = observeFixtureTransaction(() => {
    try { contender.exec('BEGIN IMMEDIATE'); } catch {
      Object.defineProperty(DatabaseSync.prototype, 'exec', EXEC); throw busyClaim();
    }
  });
  assert.equal(mutated.proof.eligible, false);
  assert.equal(mutated.proof.restored, true);
  assert.deepEqual(Object.getOwnPropertyDescriptor(DatabaseSync.prototype, 'exec'), EXEC);
});

test('descriptor restoration failure retains primary and cleanup errors without another invocation', () => {
  const helperUrl = new URL('./fixture-busy-retry.js', import.meta.url).href;
  const source = `import assert from 'node:assert/strict';
    import { DatabaseSync } from 'node:sqlite';
    import { observeFixtureTransaction } from ${JSON.stringify(helperUrl)};
    const primary = new Error('fixed test primary');
    const result = observeFixtureTransaction(() => {
      Object.defineProperty(DatabaseSync.prototype, 'exec', { value() {}, configurable: false, writable: false });
      throw primary;
    });
    assert.equal(result.failed, true); assert.equal(result.proof.restored, false);
    assert.equal(result.proof.eligible, false); assert.equal(result.error instanceof AggregateError, true);
    assert.equal(result.error.errors[0], primary); assert.equal(result.error.errors[1] instanceof TypeError, true);
    process.stdout.write('restoration failure retained\\n');`;
  const child = spawnSync(process.execPath, ['--input-type=module', '-e', source], {
    env: { PATH: process.env.PATH, TMPDIR: process.env.TMPDIR ?? '/tmp', NODE_NO_WARNINGS: '1' },
    encoding: 'utf8', timeout: 5_000, maxBuffer: 8_192
  });
  assert.equal(child.error, undefined); assert.equal(child.status, 0, child.stderr);
  assert.equal(child.signal, null); assert.equal(child.stdout, 'restoration failure retained\n');
});

test('async callbacks, unavailable native methods and reentrant observation fail closed', async () => {
  let calls = 0;
  assert.throws(() => observeFixtureTransaction(async () => { calls += 1; }), TypeError);
  assert.equal(calls, 0);
  const asynchronous = observeFixtureTransaction(() => Promise.resolve(null));
  assert.equal(asynchronous.failed, true); assert.equal(asynchronous.proof.eligible, false);
  await asynchronous.value;
  const reentrant = observeFixtureTransaction(() => observeFixtureTransaction(() => null));
  assert.equal(reentrant.failed, true); assert.equal(reentrant.proof.eligible, false);
  try {
    Object.defineProperty(DatabaseSync.prototype, 'exec', { ...EXEC, value() { calls += 1; } });
    assert.throws(() => observeFixtureTransaction(() => { calls += 1; }), TypeError);
    assert.equal(calls, 0);
  } finally { Object.defineProperty(DatabaseSync.prototype, 'exec', EXEC); }
});

test('last BUSY attempt is retained at the finite eight-attempt cap with seven delays', async t => {
  const { contender } = locked(t);
  const held = busyClaim();
  let sleeps = 0;
  const result = await runFixtureBusyRetry(options(mappedClaim(contender, held), {
    clock: () => 0n, deadlineNs: 5_000_000_000n, sleep: async () => { sleeps += 1; }
  }));
  assert.equal(result.error, held); assert.equal(result.failed, true);
  assert.equal(result.diagnostic.stopReason, 'attempt_limit');
  assert.equal(result.diagnostic.attempts.length, 8); assert.equal(sleeps, 7);
  assert.equal(result.diagnostic.attempts.at(-1).retryScheduled, false);
  assert.ok(result.diagnostic.attempts.every(a => a.code === 'STORE_BUSY' && a.eligible));
});

test('deadline includes gate time, full next busy wait and elapsed retry time without a larger window', async t => {
  const { contender } = locked(t);
  let calls = 0; let now = 0n; let sleeps = 0;
  const invoke = () => { calls += 1; now += 100_000_000n; return mappedClaim(contender)(); };
  const first = await runFixtureBusyRetry(options(invoke, { busyTimeoutMs: 100, delayMs: 40,
    clock: () => now, deadlineNs: 220_000_000n, sleep: async () => { sleeps += 1; } }));
  assert.equal(first.diagnostic.stopReason, 'deadline'); assert.equal(calls, 1); assert.equal(sleeps, 0);
  const gated = await runFixtureBusyRetry(options(() => { calls += 1; }, {
    busyTimeoutMs: 100, clock: () => 1_000_000_000n, deadlineNs: 1_050_000_000n }));
  assert.equal(gated.failed, true); assert.equal(gated.diagnostic.attempts.length, 0); assert.equal(calls, 1);
  now = 0n;
  const bounded = await runFixtureBusyRetry(options(invoke, { busyTimeoutMs: 100, delayMs: 500,
    clock: () => now, deadlineNs: 9_000_000_000n,
    sleep: async ms => { now += BigInt(ms) * 1_000_000n; } }));
  assert.equal(bounded.diagnostic.stopReason, 'deadline'); assert.equal(bounded.diagnostic.attempts.length, 3);
  assert.ok(now < 1_500_000_000n);
});

test('invalid actual retryAfter, wait failure and clock rollback retain the refusal and do not invoke again', async t => {
  const { contender } = locked(t);
  let calls = 0;
  const invoke = wait => () => {
    calls += 1;
    try { contender.exec('BEGIN IMMEDIATE'); } catch { return { ...refusal(), retryAfterMs: wait }; }
  };
  for (const wait of [0, -1, 1.5, 1_001, '40', NaN]) {
    const before = calls;
    const result = await runFixtureBusyRetry(options(invoke(wait), { kind: 'admission' }));
    assert.equal(result.diagnostic.stopReason, 'invalid_delay'); assert.equal(calls, before + 1);
    assert.equal(result.diagnostic.attempts[0].retryScheduled, false);
  }
  const primary = busyClaim(); const cleanup = new Error('synthetic timer failure');
  const failedWait = await runFixtureBusyRetry(options(mappedClaim(contender, primary), {
    sleep: async () => { throw cleanup; }
  }));
  assert.equal(failedWait.diagnostic.stopReason, 'error');
  assert.deepEqual(failedWait.error.errors, [primary, cleanup]);
  assert.equal(failedWait.diagnostic.attempts.length, 1);
  let reads = 0;
  const rollback = await runFixtureBusyRetry(options(mappedClaim(contender, primary), {
    clock: () => ++reads < 3 ? 20n : 10n
  }));
  assert.equal(rollback.diagnostic.stopReason, 'clock_after_call'); assert.equal(rollback.diagnostic.attempts.length, 1);
  assert.equal(rollback.error, primary);
  assert.equal(rollback.qualificationError instanceof TypeError, true);
});

test('closed bounded retry encoding retains complete failure history and rejects hostile or truncated frames', async t => {
  const { contender } = locked(t);
  const result = await runFixtureBusyRetry(options(mappedClaim(contender), {
    clock: () => 0n, deadlineNs: 5_000_000_000n, sleep: async () => {}
  }));
  const line = encodeFixtureRetryDiagnostic(result.diagnostic);
  assert.ok(Buffer.byteLength(line) < 8_192);
  assert.deepEqual(parseFixtureRetryDiagnostic(line), result.diagnostic);
  assert.equal(parseFixtureRetryDiagnostic(line + line), null);
  assert.equal(parseFixtureRetryDiagnostic(line.slice(0, -1)), null);
  assert.equal(parseFixtureRetryDiagnostic('x'.repeat(16_385) + '\n'), null);
  assert.equal(encodeFixtureRetryDiagnostic({ ...result.diagnostic, secret: 'canary' }), null);
  assert.equal(encodeFixtureRetryDiagnostic({ ...result.diagnostic, attempts: [...result.diagnostic.attempts, {}] }), null);
  let reads = 0;
  const getter = { ...result.diagnostic, get attempts() { reads += 1; throw new Error('canary'); } };
  assert.equal(encodeFixtureRetryDiagnostic(getter), null);
  const revoked = Proxy.revocable({}, {}); revoked.revoke();
  assert.equal(encodeFixtureRetryDiagnostic(revoked.proxy), null);
  assert.equal(reads, 0);
  assert.doesNotMatch(line, /canary|safe_message|diagnostic_ref|workflow|tenant|fixture\.sqlite/u);
  let written = null;
  publishFixtureRetryDiagnostic(result, output => { written = output; });
  assert.equal(written, line);
  const publication = new Error('synthetic publication failure');
  assert.throws(() => publishFixtureRetryDiagnostic(result, () => { throw publication; }),
    error => error instanceof AggregateError && error.errors[0] === result.error && error.errors[1] === publication);
});

test('C04 point observation distinguishes parked, queued and completed without reading checkpoint payloads or changing state', () => {
  const db = new DatabaseSync(':memory:');
  try {
    db.exec(`CREATE TABLE workflows (workflow_id TEXT PRIMARY KEY, state TEXT, fence_token INTEGER, checkpoint_sequence INTEGER);
      CREATE TABLE operations_work (job_id TEXT PRIMARY KEY, workflow_id TEXT, state TEXT, reason_code TEXT, generation INTEGER, fence_token INTEGER);
      INSERT INTO workflows VALUES ('private-workflow-canary', 'review_required', 2, 19);
      INSERT INTO operations_work VALUES ('private-job-canary', 'private-workflow-canary', 'parked', 'LOCAL_EXECUTION_STOPPED', 2, 2)`);
    const before = db.prepare('SELECT * FROM operations_work').get();
    const row = readC04FinalRow(db, 'private-job-canary');
    assert.deepEqual(row, { status: 'observed', errorCode: 'none', workState: 'parked', workflowState: 'review_required',
      reason: 'LOCAL_EXECUTION_STOPPED', generation: 2, operationFence: 2, workflowFence: 2, checkpointSequence: 19 });
    assert.deepEqual(db.prepare('SELECT * FROM operations_work').get(), before);
    assert.equal(readC04FinalRow(db, 'absent').status, 'missing');
    assert.equal(readC04FinalRow(null, 'absent').status, 'not_observed');
    for (const [work, workflow, reason] of [['queued', 'running', 'SAFE_RESTART_RESUME'], ['completed', 'succeeded', null]]) {
      db.prepare('UPDATE operations_work SET state=?, reason_code=?').run(work, reason);
      db.prepare('UPDATE workflows SET state=?').run(workflow);
      const current = readC04FinalRow(db, 'private-job-canary');
      assert.equal(current.workState, work); assert.equal(current.workflowState, workflow);
      assert.equal(current.reason, reason ?? 'none');
    }
  } finally { db.close(); }
  const unavailable = readC04FinalRow(db, 'private-job-canary');
  assert.equal(unavailable.status, 'error'); assert.equal(unavailable.workState, 'unobserved');
});

test('C04 diagnostic keeps actual error phase, exit and incomplete capture distinct from durable row and cleanup', () => {
  const capture = { schema: 'agent-harness.ops07-session.v1',
    flags: { complete: false, missingChildCapture: true, malformed: false, overflow: false,
      writeFailed: false, abruptExit: true, cleanupIncomplete: true },
    slots: [{ slot: 1, footer: true, writerComplete: true, records: [
      { stage: 'runner_phase', phase: 'runner_start' },
      { stage: 'store_open', ok: true },
      { stage: 'controller_error', phase: 'completion', code: 'STORE_BUSY', primaryCode: 5 },
      { stage: 'controller_result', status: 'stopped', reason: 'LOCAL_COMPLETION_BUSY', checkpointSequence: 19 },
      { stage: 'store_close', ok: true }
    ] }],
    supervisor: [{ slot: 0, stage: 'child_exit', exitCode: null, signal: 'SIGKILL' },
      { slot: 1, stage: 'child_exit', exitCode: 2, signal: null },
      { slot: 1, stage: 'finish_result', outcome: 'stopped', settled: true }] };
  const line = encodeC04CompletionDiagnostic({ phase: 'restart_wait', primaryFailed: true,
    cleanupComplete: true, row: { status: 'observed', workState: 'parked', workflowState: 'review_required',
      reason: 'LOCAL_EXECUTION_STOPPED', generation: 2, operationFence: 2, workflowFence: 2,
      checkpointSequence: 19, errorCode: 'none' }, capture });
  assert.ok(line.startsWith('OPS07_C04_COMPLETION_V1 '));
  assert.doesNotMatch(line, /OPS07_CAPTURE_V1/u);
  const value = JSON.parse(line.slice(line.indexOf(' ') + 1));
  assert.equal(value.diagnosticOnly, true); assert.equal(value.primaryFailed, true);
  assert.equal(value.row.workState, 'parked'); assert.equal(value.cleanupComplete, true);
  assert.equal(value.capture.sessionCleanupIncomplete, true);
  assert.equal(value.capture.slots[0].childObserved, false);
  assert.equal(value.capture.slots[0].signal, 'SIGKILL');
  assert.equal(value.capture.slots[1].errors[0].phase, 'completion');
  assert.equal(value.capture.slots[1].lastPhase, 'store_close');
  assert.equal(value.capture.slots[1].errors[0].code, 'STORE_BUSY');
  assert.equal(value.capture.slots[1].errors[0].sqlitePrimaryCode, 5);
  assert.equal(value.capture.slots[1].exitCode, 2);
  assert.equal(value.capture.slots[1].finishOutcome, 'stopped');
  assert.equal(value.capture.slots[1].finishSettled, true);
});

test('C04 closed projection bounds records and rejects private strings, hostile properties and missing evidence', () => {
  let reads = 0;
  const revocable = Proxy.revocable({}, {}); revocable.revoke();
  const hostile = { get capture() { reads += 1; throw new Error('private-canary'); },
    row: { get workState() { reads += 1; throw new Error('private-canary'); }, reason: 'private-canary',
      generation: '999', checkpointSequence: Infinity }, primaryFailed: true };
  for (const input of [hostile, revocable.proxy, { capture: { slots: new Array(3), supervisor: new Array(101) } }]) {
    const line = encodeC04CompletionDiagnostic(input);
    assert.ok(Buffer.byteLength(line) <= 4_096);
    assert.doesNotMatch(line, /private-canary|Infinity|"generation":"999"/u);
    const value = JSON.parse(line.slice(line.indexOf(' ') + 1));
    assert.equal(value.capture.available, false);
    assert.equal(value.row.workState, 'unobserved');
  }
  assert.equal(reads, 0);
});
