import { readLocalControllerClockDiagnostic } from '../../src/internal/durable-store.js';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { spawn, spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { performance } from 'node:perf_hooks';
import { test } from 'node:test';
import { Worker } from 'node:worker_threads';
import { DurableWorkflowStore, RuntimeError } from '@jevelopers/agent-harness-runtime';
import {
  MAX_CHECKPOINT_DEPTH,
  MAX_CHECKPOINT_SERIALIZED_BYTES
} from '../../src/internal/checkpoint-bounds.js';
import { LOCAL_CONTROLLER_STORAGE_CAPABILITY } from '../../src/internal/private-local-store-capability.js';
import { OPERATIONS_STORAGE_CAPABILITY } from '../../src/internal/private-ops-store-capability.js';
import {
  checkpointPayloadForIntegrity,
  consumeLocalAdmissionBusyAfterConfirmedInactive,
  consumeLocalCompletionBusyAfterConfirmedInactive,
  inLocalAdmissionTransaction,
  inLocalCompletionTransaction,
  measureCheckpointPayload
} from '../../src/internal/durable-store.js';
import {
  closeOperationsChildCapture,
  createOperationsDiagnosticSession,
  disposeOperationsDiagnosticSession,
  OPS07_CAPTURE_LINE_PREFIX,
  parseOperationsCaptureFromTap,
  prepareOperationsChildCapture,
  readOperationsDiagnosticSession,
  recordOperationsSupervisor
} from '../../src/internal/private-ops-diagnostics.js';
import { operationsStoragePolicy, readOperationsConfig, validateLocalPlanWork } from '../../src/worker/private/config.js';
import {
  buildCompletedCheckpoint,
  buildInitialPrivateCheckpoint,
  runBoundedLocalPlan
} from '../../src/orchestrator/internal/controller.js';
import {
  advanceBudget,
  createBudgetSnapshot,
  createLimitSet,
  getLimitValues,
  recordResultBytes
} from '../../src/orchestrator/internal/limits.js';
import {
  compilePlan,
  executeLocalStep,
  getMaximumCompletedResultsJsonBytes,
  getPlanStepIds,
  getNextReadyStep
} from '../../src/orchestrator/internal/plan.js';
import { inspectOperation, createDefaultRegistry } from '../../src/tools/internal/registry.js';
import { inspectWorkflowRecovery } from '../../src/recovery/internal/controller.js';
import { spawnFixture, temporaryDatabase, waitForFiles } from './helpers.js';

const PLAN_KIND = 'agent-harness.private-synthetic-plan';
const DEPENDENT_PLAN = JSON.stringify({
  kind: PLAN_KIND,
  version: 1,
  steps: [
    { id: 'second', operation: 'number.add', dependsOn: ['first'], args: { left: { $result: 'first' }, right: 4 } },
    { id: 'first', operation: 'number.add', dependsOn: [], args: { left: 2, right: 3 } }
  ]
});
const SINGLE_PLAN = JSON.stringify({
  kind: PLAN_KIND,
  version: 1,
  steps: [{ id: 'only', operation: 'number.add', dependsOn: [], args: { left: 4, right: 5 } }]
});
const TWO_STEP_PLAN = JSON.stringify({
  kind: PLAN_KIND,
  version: 1,
  steps: [
    { id: 'first', operation: 'number.add', dependsOn: [], args: { left: 4, right: 5 } },
    { id: 'second', operation: 'number.add', dependsOn: ['first'], args: { left: { $result: 'first' }, right: 6 } }
  ]
});
const THIRTY_TWO_STEP_PLAN = JSON.stringify({
  kind: PLAN_KIND,
  version: 1,
  steps: Array.from({ length: 32 }, (_, index) => {
    const id = 'step-' + String(index + 1).padStart(2, '0');
    const chainOffset = Math.floor(index / 8) * 8;
    const chainIndex = index % 8;
    const previousId = 'step-' + String(index).padStart(2, '0');
    return {
      id,
      operation: 'number.add',
      dependsOn: chainIndex === 0 ? [] : [previousId],
      args: {
        left: chainIndex === 0 ? chainOffset : { $result: previousId },
        right: 1
      }
    };
  })
});
const DEPTH_THIRTY_TWO_PLAN = JSON.stringify({
  kind: PLAN_KIND,
  version: 1,
  steps: Array.from({ length: 32 }, (_, index) => {
    const id = 'step-' + String(index + 1).padStart(2, '0');
    const previousId = 'step-' + String(index).padStart(2, '0');
    return {
      id,
      operation: 'number.add',
      dependsOn: index === 0 ? [] : [previousId],
      args: {
        left: index === 0 ? 0 : { $result: previousId },
        right: 1
      }
    };
  })
});
const SELF_REHASH_PLAN = JSON.stringify({
  kind: PLAN_KIND,
  version: 1,
  steps: [
    { id: 'second', operation: 'number.add', dependsOn: ['first'], args: { left: { $result: 'first' }, right: 2 } },
    { id: 'first', operation: 'number.add', dependsOn: [], args: { left: 1, right: 2 } }
  ]
});
const LONG_FUTURE_STEP_ID = 'z'.repeat(48);
const LONG_FUTURE_PLAN = JSON.stringify({
  kind: PLAN_KIND,
  version: 1,
  steps: [
    { id: 'a', operation: 'number.add', dependsOn: [], args: { left: 1, right: 2 } },
    {
      id: LONG_FUTURE_STEP_ID,
      operation: 'number.add',
      dependsOn: ['a'],
      args: { left: { $result: 'a' }, right: 2 }
    }
  ]
});

function tenStepEscapedTextPlan() {
  const ids = [
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', LONG_FUTURE_STEP_ID
  ];
  const suffixes = ['\\', '\0', 'é', '"', '\\é', '\0"', 'é\\', '"\0', 'tail'];
  return JSON.stringify({
    kind: PLAN_KIND,
    version: 1,
    steps: ids.map((id, index) => ({
      id,
      operation: 'text.concat',
      dependsOn: index === 0 ? [] : [ids[index - 1]],
      args: {
        parts: index === 0
          ? ['start"\\\0é']
          : [{ $result: ids[index - 1] }, suffixes[index - 1]]
      }
    }))
  });
}

function mutableClock(startAtMs, { monotonic = false } = {}) {
  let current = startAtMs;
  let overridden = false;
  const startedAt = performance.now();
  return {
    read: () => overridden || !monotonic ? current : current + Math.floor(performance.now() - startedAt),
    set: value => { current = value; overridden = true; }
  };
}

function openStore(databasePath, clock = Date.now, busyTimeoutMs = 5000) {
  let id = 0;
  return DurableWorkflowStore.open({
    databasePath,
    clock,
    busyTimeoutMs,
    idFactory: () => 'local-controller-' + (++id)
  });
}

function admitAndClaimLocalOperationsPlan(store, suffix, planJson = SINGLE_PLAN) {
  const config = readOperationsConfig();
  const policy = operationsStoragePolicy(config);
  store._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY, {
    maxStoreBytes: config.maxStoreBytes,
    maxWalBytes: config.maxWalBytes,
    minFreeDiskBytes: config.minFreeDiskBytes
  });
  const validated = validateLocalPlanWork({ planJson, limitsJson: '{}' }, 'ah11-' + suffix, config);
  const accepted = store._operationsAdmit(OPERATIONS_STORAGE_CAPABILITY, {
    tenantId: 'ah11-local-completion-tenant',
    projectId: 'ah11-local-completion-project',
    idempotencyKey: suffix,
    workJson: validated.workJson,
    policy
  });
  assert.equal(accepted.accepted, true);
  const ownerId = 'ah11-local-completion-worker';
  const selected = store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, { ownerId, policy });
  assert.ok(selected?.claim);
  const claim = selected.claim;
  const work = store._operationsReadClaim(OPERATIONS_STORAGE_CAPABILITY, {
    jobId: claim.jobId,
    ownerId,
    generation: claim.generation,
    fence: claim.fence
  });
  return Object.freeze({ claim, work });
}

function localCompletionAccounting(databasePath, workflowId) {
  const db = new DatabaseSync(databasePath, { timeout: 20 });
  try {
    const meta = db.prepare(`
      SELECT reserved_event_slots, reserved_disk_bytes
      FROM operations_meta WHERE singleton = 1
    `).get();
    const work = db.prepare(`
      SELECT COUNT(*) AS count,
        COALESCE(SUM(reserved_disk_bytes), 0) AS bytes,
        COALESCE(SUM(reserved_event_slots), 0) AS eventSlots
      FROM operations_work WHERE workflow_id = ?
    `).get(workflowId);
    const events = db.prepare('SELECT COUNT(*) AS count FROM workflow_events WHERE workflow_id = ?').get(workflowId);
    const checkpoints = db.prepare('SELECT COUNT(*) AS count FROM workflow_checkpoints WHERE workflow_id = ?')
      .get(workflowId);
    const outbox = db.prepare(`
      SELECT COUNT(*) AS count FROM outbox o
      JOIN workflow_events e ON e.event_id = o.event_id
      WHERE e.workflow_id = ?
    `).get(workflowId);
    return Object.freeze({
      reservedEventSlots: Number(meta.reserved_event_slots),
      reservedDiskBytes: Number(meta.reserved_disk_bytes),
      operationsWorkReservedDiskBytes: Number(work.bytes),
      operationsWorkReservedEventSlots: Number(work.eventSlots),
      operationsWorkRows: Number(work.count),
      workflowEvents: Number(events.count),
      workflowCheckpoints: Number(checkpoints.count),
      workflowOutboxRows: Number(outbox.count)
    });
  } finally {
    db.close();
  }
}

function localCompletionExternalCustody(databasePath, workflowId) {
  const db = new DatabaseSync(databasePath, { timeout: 20 });
  try {
    const attempts = db.prepare(`
      SELECT COUNT(*) AS count, COUNT(DISTINCT authority_hold_ref) AS holds
      FROM operation_attempts WHERE workflow_id = ?
    `).get(workflowId);
    const transports = db.prepare(`
      SELECT COUNT(*) AS count
      FROM transport_attempts t JOIN operation_attempts a ON a.attempt_id = t.attempt_id
      WHERE a.workflow_id = ?
    `).get(workflowId);
    const receipts = db.prepare(`
      SELECT COUNT(*) AS count
      FROM response_receipts r JOIN operation_attempts a ON a.attempt_id = r.attempt_id
      WHERE a.workflow_id = ?
    `).get(workflowId);
    return Object.freeze({
      operationAttempts: Number(attempts.count),
      authorityHoldRefs: Number(attempts.holds),
      transportAttempts: Number(transports.count),
      responseReceipts: Number(receipts.count)
    });
  } finally {
    db.close();
  }
}

function localExternalCustodyRows(databasePath, workflowId) {
  const db = new DatabaseSync(databasePath, { timeout: 20 });
  const rows = statement => statement.all(workflowId).map(row => {
    const output = {};
    for (const key of Object.keys(row).sort()) {
      const value = row[key];
      output[key] = ArrayBuffer.isView(value) ? Buffer.from(value).toString('hex') : value;
    }
    return output;
  });
  try {
    return Object.freeze({
      operationAttempts: rows(db.prepare(`
        SELECT * FROM operation_attempts WHERE workflow_id = ? ORDER BY attempt_id
      `)),
      transportAttempts: rows(db.prepare(`
        SELECT t.* FROM transport_attempts t
        JOIN operation_attempts a ON a.attempt_id = t.attempt_id
        WHERE a.workflow_id = ? ORDER BY t.transport_attempt_id
      `)),
      responseReceipts: rows(db.prepare(`
        SELECT r.receipt_id, r.attempt_id, r.outcome, r.next_state,
          hex(r.response_bytes) AS response_bytes_hex, r.response_sha256,
          r.next_checkpoint_json, r.next_checkpoint_sha256, r.created_at_ms
        FROM response_receipts r
        JOIN operation_attempts a ON a.attempt_id = r.attempt_id
        WHERE a.workflow_id = ? ORDER BY r.receipt_id
      `)),
      attemptObservations: rows(db.prepare(`
        SELECT o.* FROM attempt_observations o
        JOIN operation_attempts a ON a.attempt_id = o.attempt_id
        WHERE a.workflow_id = ? ORDER BY o.source_observation_ref
      `))
    });
  } finally {
    db.close();
  }
}

function expectedLocalControllerWriteDiskDebit(controllerState) {
  const checkpointBytes = Buffer.byteLength(checkpointPayloadForIntegrity(controllerState), 'utf8');
  return Math.max(16_384, checkpointBytes * 3 + 32_768);
}

function syntheticSqliteBusy(options = {}) {
  const code = Object.hasOwn(options, 'code') ? options.code : 'ERR_SQLITE_ERROR';
  const errcode = Object.hasOwn(options, 'errcode') ? options.errcode : 5;
  const error = new Error('synthetic SQLite busy control');
  error.code = code;
  if (errcode !== undefined) error.errcode = errcode;
  return error;
}

function localCompletionBusy() {
  return new RuntimeError({
    module: 'state',
    operation: '_localControllerComplete',
    code: 'STORE_BUSY',
    retryable: true,
    safe_message: 'The durable store is busy; retry after the current write finishes.'
  });
}

function localAdmissionBusy() {
  return new RuntimeError({
    module: 'state',
    operation: '_localControllerAdmit',
    code: 'STORE_BUSY',
    retryable: true,
    safe_message: 'The durable store is busy; retry after the current write finishes.'
  });
}

function localCompletionRetryBudget(busyTimeoutMs = 20, runtimeMs = 5000) {
  return {
    busyTimeoutMs,
    runtimeDeadlineNs: process.hrtime.bigint() + BigInt(runtimeMs) * 1_000_000n
  };
}

function localAdmissionRetryBudget(busyTimeoutMs = 20, runtimeMs = 5000) {
  return {
    busyTimeoutMs,
    runtimeDeadlineNs: process.hrtime.bigint() + BigInt(runtimeMs) * 1_000_000n
  };
}

function sqliteWriterBlockerSync(databasePath) {
  const stateBuffer = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT * 2);
  const state = new Int32Array(stateBuffer);
  const worker = new Worker(`
    const { workerData } = require('node:worker_threads');
    const { DatabaseSync } = require('node:sqlite');
    let db;
    try {
      db = new DatabaseSync(workerData.databasePath, { timeout: 1000 });
      db.exec('BEGIN IMMEDIATE');
      Atomics.store(new Int32Array(workerData.stateBuffer), 0, 1);
      Atomics.notify(new Int32Array(workerData.stateBuffer), 0);
      Atomics.wait(new Int32Array(workerData.stateBuffer), 1, 0, 10000);
      if (db.isTransaction) db.exec('ROLLBACK');
      db.close();
      db = null;
      Atomics.store(new Int32Array(workerData.stateBuffer), 0, 2);
      Atomics.notify(new Int32Array(workerData.stateBuffer), 0);
    } catch {
      Atomics.store(new Int32Array(workerData.stateBuffer), 0, -1);
      Atomics.notify(new Int32Array(workerData.stateBuffer), 0);
      throw new Error('synthetic SQLite writer failed');
    } finally {
      try {
        if (db?.isTransaction) db.exec('ROLLBACK');
        db?.close();
      } catch {}
    }
  `, { eval: true, workerData: { databasePath, stateBuffer } });
  const exited = new Promise(resolve => worker.once('exit', resolve));
  worker.on('error', () => {});
  const waitForState = expected => {
    const deadline = Date.now() + 5000;
    while (Atomics.load(state, 0) !== expected) {
      const current = Atomics.load(state, 0);
      if (current === -1) throw new Error('The synthetic SQLite writer failed.');
      const remaining = deadline - Date.now();
      if (remaining <= 0 || Atomics.wait(state, 0, current, remaining) === 'timed-out') {
        throw new Error('The synthetic SQLite writer exceeded its setup/close bound.');
      }
    }
  };
  waitForState(1);
  return {
    exited,
    releaseAndWait() {
      Atomics.store(state, 1, 1);
      Atomics.notify(state, 1);
      waitForState(2);
    },
    async cleanup() {
      if (Atomics.load(state, 0) === 1) {
        Atomics.store(state, 1, 1);
        Atomics.notify(state, 1);
      }
      await worker.terminate();
    }
  };
}

function createRunningWorkflow(
  store,
  workflowId,
  ownerId = 'worker-local',
  ttlMs = 60_000,
  checkpoint = { preserved: 'synthetic-original-checkpoint' }
) {
  store.createWorkflow({
    workflowId,
    checkpoint,
    eventId: workflowId + ':created'
  });
  const lease = store.acquireLease({ workflowId, ownerId, ttlMs, eventId: workflowId + ':lease' });
  const running = store.transitionWorkflow({
    ...lease,
    expectedRevision: 0,
    toState: 'running',
    eventId: workflowId + ':running'
  });
  return { ...lease, revision: running.revision };
}

function nestedCheckpointData(levels) {
  let value = { preserved: 'bounded-depth-control' };
  for (let index = 0; index < levels; index += 1) value = { child: value };
  return value;
}

function initialCheckpointSizeForBase(workflowId, baseCheckpointData) {
  const { limitSet, plan } = storeTestPlan({ workflowId, planJson: SINGLE_PLAN });
  const startedWallMs = 1_790_000_000_000;
  const deadlineWallMs = startedWallMs + getLimitValues(limitSet).maxElapsedMs;
  const budget = createBudgetSnapshot(limitSet, startedWallMs, deadlineWallMs);
  const stepId = getNextReadyStep(plan, '{}');
  const admittedBudget = advanceBudget(budget, 'step', startedWallMs, 0, limitSet);
  const state = buildInitialPrivateCheckpoint({
    workflowId,
    planJson: SINGLE_PLAN,
    plan,
    limitSet,
    baseCheckpointData,
    budget: admittedBudget.state,
    stepId
  });
  const resultReservationBytes = getMaximumCompletedResultsJsonBytes(plan, state.completedResultsJson);
  const limits = getLimitValues(limitSet);
  const stepIds = getPlanStepIds(plan);
  const longestStepId = stepIds.reduce((longest, stepId) => (
    Buffer.byteLength(stepId, 'utf8') > Buffer.byteLength(longest, 'utf8') ? stepId : longest
  ), stepIds[0]);
  const sizeOnlyState = {
    ...state,
    budget: {
      ...state.budget,
      lastWallMs: state.budget.deadlineWallMs,
      elapsedMs: limits.maxElapsedMs,
      stepsAdmitted: stepIds.length,
      stepAttempts: limits.maxStepAttempts,
      localRetries: limits.maxLocalRetries,
      replans: limits.maxReplans,
      iterations: limits.maxIterations,
      totalResultBytes: limits.maxTotalResultBytes
    },
    completedResultsJson: '\\'.repeat(resultReservationBytes),
    pendingStepId: longestStepId,
    lastAdmissionKind: 'replay',
    lastAdmissionStepId: longestStepId
  };
  return Object.freeze({
    initial: measureCheckpointPayload(state),
    reserved: measureCheckpointPayload(sizeOnlyState)
  });
}

function storeTestPlan({ workflowId, planJson = SINGLE_PLAN, limitsJson = '{}' }) {
  const limitSet = createLimitSet(limitsJson);
  const plan = compilePlan(planJson, limitSet, undefined, workflowId);
  return { limitSet, plan };
}

function canonical(value) {
  if (value === null || typeof value === 'boolean' || typeof value === 'string') return JSON.stringify(value);
  if (typeof value === 'number') return JSON.stringify(value);
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  return '{' + Object.keys(value).sort()
    .map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}';
}

function savedResult(plan, stepId, args, output) {
  const inputHash = 'sha256_' + createHash('sha256').update(canonical({
    kind: 'agent-harness.private-local-input.v1',
    stepId,
    planHash: plan.planHash,
    registryVersion: plan.registryVersion,
    runIdentity: plan.runIdentity,
    args
  }), 'utf8').digest('hex');
  const resultHash = 'sha256_' + createHash('sha256').update(canonical({
    kind: 'agent-harness.private-local-result.v1',
    stepId,
    planHash: plan.planHash,
    registryVersion: plan.registryVersion,
    runIdentity: plan.runIdentity,
    inputHash,
    output
  }), 'utf8').digest('hex');
  return {
    stepId,
    planHash: plan.planHash,
    registryVersion: plan.registryVersion,
    runIdentity: plan.runIdentity,
    inputHash,
    resultHash,
    output
  };
}

function rewriteLatestCheckpointJson(databasePath, workflowId, payload) {
  const digest = createHash('sha256').update(payload, 'utf8').digest('hex');
  const database = new DatabaseSync(databasePath);
  database.exec('DROP TRIGGER workflow_checkpoints_no_update');
  const latest = database.prepare(
    'SELECT sequence FROM workflow_checkpoints WHERE workflow_id = ? ORDER BY sequence DESC LIMIT 1'
  ).get(workflowId);
  database.prepare(
    'UPDATE workflow_checkpoints SET payload_json = ?, sha256 = ? WHERE workflow_id = ? AND sequence = ?'
  ).run(payload, digest, workflowId, Number(latest.sequence));
  database.close();
}

function rewriteLatestPrivateCheckpoint(databasePath, workflowId, data) {
  rewriteLatestCheckpointJson(databasePath, workflowId, '{"schemaVersion":2,"data":' + canonical(data) + '}');
}

function makeAdmission(store, lease, planJson = SINGLE_PLAN, limitsJson = '{}', eventId = 'local-test:admit') {
  const workflowId = lease.workflowId;
  const snapshot = store._localControllerSnapshot(LOCAL_CONTROLLER_STORAGE_CAPABILITY, workflowId);
  const { limitSet, plan } = storeTestPlan({ workflowId, planJson, limitsJson });
  const now = store._localControllerNow(LOCAL_CONTROLLER_STORAGE_CAPABILITY);
  const budget = createBudgetSnapshot(limitSet, now, now + getLimitValues(limitSet).maxElapsedMs);
  const stepId = getNextReadyStep(plan, '{}');
  const admissionBudget = advanceBudget(budget, 'step', now, 0, limitSet);
  assert.equal(admissionBudget.allowed, true);
  const controllerState = buildInitialPrivateCheckpoint({
    workflowId,
    planJson,
    plan,
    limitSet,
    baseCheckpointData: snapshot.checkpoint.data,
    budget: admissionBudget.state,
    stepId
  });
  const stored = store._localControllerAdmit(LOCAL_CONTROLLER_STORAGE_CAPABILITY, {
    workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    expectedRevision: snapshot.workflow.revision,
    expectedCheckpointSequence: snapshot.checkpoint.sequence,
    stepId,
    admissionKind: 'step',
    eventId,
    controllerState
  });
  return { stored, state: controllerState, plan, limitSet, stepId };
}

function makeCompletion(store, lease, admission, eventId = 'local-test:complete') {
  const result = executeLocalStep(admission.plan, admission.stepId, admission.state.completedResultsJson);
  const bytes = Buffer.byteLength(JSON.stringify(result.output), 'utf8');
  const byteRecord = recordResultBytes(admission.state.budget, bytes, admission.limitSet);
  assert.equal(byteRecord.allowed, true);
  const now = store._localControllerNow(LOCAL_CONTROLLER_STORAGE_CAPABILITY);
  const budget = Object.freeze({ ...byteRecord.state, lastWallMs: now });
  const controllerState = buildCompletedCheckpoint(admission.state, admission.plan, result, budget);
  const workflow = store.getWorkflow(lease.workflowId);
  const checkpoint = store.getCheckpoint(lease.workflowId);
  const input = {
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    expectedRevision: workflow.revision,
    expectedCheckpointSequence: checkpoint.sequence,
    stepId: admission.stepId,
    eventId,
    controllerState
  };
  return { input, result, controllerState };
}

function fixturePath(name) {
  return join(new URL('./fixtures/', import.meta.url).pathname, name);
}

function spawnWithSignal(scriptPath, args = []) {
  const child = spawn(process.execPath, [scriptPath, ...args], {
    cwd: process.cwd(),
    env: { ...process.env, NODE_NO_WARNINGS: '1' },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  let stdout = '';
  let stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', chunk => { stdout += chunk; });
  child.stderr.on('data', chunk => { stderr += chunk; });
  let closed = false;
  const done = new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('close', (code, signal) => {
      closed = true;
      resolve({ code, signal, stdout, stderr });
    });
  });
  return { child, done, isClosed: () => closed };
}

async function reapOwnedChild(child) {
  if (!child.isClosed() && child.child.pid !== undefined) {
    try {
      child.child.kill('SIGKILL');
    } catch {
      // The process may have exited between the state check and kill.
    }
  }
  await child.done.catch(() => undefined);
}

async function killAtMarker(child, markerPath, timeoutMs = 5_000, assertMarker = () => {}) {
  try {
    await waitForFiles([markerPath], timeoutMs);
    const marker = JSON.parse(readFileSync(markerPath, 'utf8'));
    assertMarker(marker);
    if (!child.isClosed()) child.child.kill('SIGKILL');
    const result = await child.done;
    assert.equal(result.signal, 'SIGKILL');
    return { marker, result };
  } finally {
    await reapOwnedChild(child);
  }
}

function startRaceWorker({ mode, databasePath, workflowId, ownerId, fence, payloadPath, gatePath, workerId, resultPath, newOwnerId }) {
  return spawnFixture(fixturePath('local-controller-race.mjs'), [
    mode,
    databasePath,
    workflowId,
    ownerId,
    String(fence),
    payloadPath,
    gatePath,
    workerId,
    resultPath,
    newOwnerId ?? '-'
  ]);
}

function snapshotDatabase(databasePath) {
  const tables = [
    'workflows', 'workflow_checkpoints', 'workflow_events', 'outbox',
    'operation_attempts', 'transport_attempts', 'response_receipts',
    'attempt_observations', 'immutable_artifacts', 'telemetry_cursors',
    'jev_dispatch_markers', 'jev_provider_observations', 'jev_local_observation_outbox'
  ];
  const db = new DatabaseSync(databasePath);
  try {
    const result = Object.create(null);
    for (const table of tables) {
      const rows = db.prepare(`SELECT * FROM ${table}`).all().map(row => {
        const output = Object.create(null);
        for (const key of Object.keys(row).sort()) {
          const value = row[key];
          output[key] = ArrayBuffer.isView(value) ? Buffer.from(value).toString('hex') : value;
        }
        return output;
      });
      rows.sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right)));
      result[table] = rows;
    }
    result.sqliteSequence = db.prepare('SELECT name, seq FROM sqlite_sequence ORDER BY name').all();
    return JSON.stringify(result);
  } finally {
    db.close();
  }
}

function interceptLocalControllerTransactionClock(store, methodName, databasePath, clock, transactionNow) {
  const original = store[methodName].bind(store);
  let beforeTransaction = null;
  store[methodName] = (...args) => {
    beforeTransaction = snapshotDatabase(databasePath);
    clock.set(transactionNow);
    return original(...args);
  };
  return () => beforeTransaction;
}

function installWriteFailure(databasePath, table, workflowId, suffix) {
  const trigger = 'fail_local_' + suffix;
  const db = new DatabaseSync(databasePath);
  try {
    const sql = table === 'workflows'
      ? `CREATE TRIGGER ${trigger} AFTER UPDATE ON workflows WHEN NEW.workflow_id = '${workflowId}' AND NEW.checkpoint_sequence > OLD.checkpoint_sequence BEGIN SELECT RAISE(ABORT, 'synthetic local write failure'); END;`
      : table === 'workflow_checkpoints'
        ? `CREATE TRIGGER ${trigger} AFTER INSERT ON workflow_checkpoints WHEN NEW.workflow_id = '${workflowId}' AND NEW.sequence > 1 BEGIN SELECT RAISE(ABORT, 'synthetic local write failure'); END;`
        : table === 'workflow_events'
          ? `CREATE TRIGGER ${trigger} AFTER INSERT ON workflow_events WHEN NEW.workflow_id = '${workflowId}' AND NEW.event_type LIKE 'private_local_%' BEGIN SELECT RAISE(ABORT, 'synthetic local write failure'); END;`
          : `CREATE TRIGGER ${trigger} AFTER INSERT ON outbox WHEN NEW.event_id LIKE 'rollback:%' BEGIN SELECT RAISE(ABORT, 'synthetic local write failure'); END;`;
    db.exec(sql);
  } finally {
    db.close();
  }
  return trigger;
}

function dropTrigger(databasePath, trigger) {
  const db = new DatabaseSync(databasePath);
  try {
    db.exec(`DROP TRIGGER ${trigger}`);
  } finally {
    db.close();
  }
}

function countRows(databasePath, table) {
  const db = new DatabaseSync(databasePath);
  try {
    return Number(db.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get().count);
  } finally {
    db.close();
  }
}

test('the durable controller computes dependent local results and never repeats committed completion', async t => {
  const temporary = temporaryDatabase('bounded-local-controller-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'controller-executable-run');

  const result = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    planJson: DEPENDENT_PLAN
  });
  assert.equal(result.status, 'completed');
  assert.deepEqual(result.results.map(record => record.output), [5, 9]);
  assert.equal(result.stoppingFacts.stepsAdmitted, 2);
  assert.equal(result.stoppingFacts.stepAttempts, 2);
  assert.equal(result.stoppingFacts.localRetries, 0);
  assert.equal(result.stoppingFacts.replans, 0);
  assert.equal(result.stoppingFacts.iterations, 2);
  assert.equal(store.getWorkflow(lease.workflowId).state, 'running');
  assert.equal(store.getWorkflow(lease.workflowId).checkpointSequence, 5);
  assert.equal(store.getCheckpoint(lease.workflowId).data.baseCheckpointData.preserved, 'synthetic-original-checkpoint');
  assert.deepEqual(store.listEvents({ workflowId: lease.workflowId }).slice(-4).map(event => event.type), [
    'private_local_step_admitted',
    'private_local_step_completed',
    'private_local_step_admitted',
    'private_local_step_completed'
  ]);
  assert.equal(store.readOutbox().length, store.listEvents({ workflowId: lease.workflowId }).length);
  assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
  assert.equal(countRows(temporary.databasePath, 'transport_attempts'), 0);
  assert.equal(countRows(temporary.databasePath, 'response_receipts'), 0);

  const eventsBeforeReplay = store.listEvents({ workflowId: lease.workflowId }).length;
  const sameResult = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence
  });
  assert.equal(sameResult.resultHash, result.resultHash, JSON.stringify(sameResult));
  assert.equal(store.listEvents({ workflowId: lease.workflowId }).length, eventsBeforeReplay);
});

test('local admission retry eligibility requires a raw SQLite BUSY and confirmed inactive disposition', () => {
  for (const phase of ['begin', 'body', 'commit']) {
    let active = false;
    let callbackCalls = 0;
    const db = {
      get isTransaction() { return active; },
      exec(sql) {
        if (sql === 'BEGIN IMMEDIATE') {
          if (phase === 'begin') throw syntheticSqliteBusy();
          active = true;
          return;
        }
        if (sql === 'COMMIT') {
          if (phase === 'commit') throw syntheticSqliteBusy();
          active = false;
          return;
        }
        if (sql === 'ROLLBACK') {
          active = false;
          return;
        }
        throw new Error('unexpected synthetic admission transaction SQL');
      }
    };
    let caught = null;
    try {
      inLocalAdmissionTransaction(db, '_localControllerAdmit', () => {
        callbackCalls += 1;
        if (phase === 'body') throw syntheticSqliteBusy();
        return 'synthetic admission';
      });
    } catch (error) {
      caught = error;
    }
    assert.ok(caught instanceof RuntimeError, phase);
    assert.equal(caught.operation, '_localControllerAdmit', phase);
    assert.equal(caught.code, 'STORE_BUSY', phase);
    assert.equal(consumeLocalAdmissionBusyAfterConfirmedInactive(caught), true, phase);
    assert.equal(callbackCalls, phase === 'begin' ? 0 : 1, phase);
  }

  let active = false;
  const rollbackFailure = {
    get isTransaction() { return active; },
    exec(sql) {
      if (sql === 'BEGIN IMMEDIATE') { active = true; return; }
      if (sql === 'ROLLBACK') throw syntheticSqliteBusy();
      throw new Error('unexpected synthetic admission transaction SQL');
    }
  };
  let rollbackError = null;
  try {
    inLocalAdmissionTransaction(rollbackFailure, '_localControllerAdmit', () => {
      throw syntheticSqliteBusy();
    });
  } catch (error) {
    rollbackError = error;
  }
  assert.ok(rollbackError instanceof RuntimeError);
  assert.equal(rollbackError.code, 'STORE_BUSY');
  assert.equal(consumeLocalAdmissionBusyAfterConfirmedInactive(rollbackError), false,
    'a rollback failure takes precedence and cannot authorize admission recovery');

  let transactionReads = 0;
  const unknownDisposition = {
    get isTransaction() {
      transactionReads += 1;
      if (transactionReads === 1) return false;
      if (transactionReads === 2) return undefined;
      return false;
    },
    exec(sql) {
      if (sql === 'BEGIN IMMEDIATE') return;
      throw new Error('unexpected synthetic admission transaction SQL');
    }
  };
  let unknownStateError = null;
  try {
    inLocalAdmissionTransaction(unknownDisposition, '_localControllerAdmit', () => {
      throw syntheticSqliteBusy();
    });
  } catch (error) {
    unknownStateError = error;
  }
  assert.ok(unknownStateError instanceof RuntimeError);
  assert.equal(consumeLocalAdmissionBusyAfterConfirmedInactive(unknownStateError), false,
    'an unknown catch-time transaction state cannot authorize admission recovery');

  active = false;
  const forged = localAdmissionBusy();
  let forgedResult = null;
  try {
    inLocalAdmissionTransaction({
      get isTransaction() { return active; },
      exec(sql) {
        if (sql === 'BEGIN IMMEDIATE') { active = true; return; }
        if (sql === 'ROLLBACK') { active = false; return; }
        throw new Error('unexpected synthetic admission transaction SQL');
      }
    }, '_localControllerAdmit', () => { throw forged; });
  } catch (error) {
    forgedResult = error;
  }
  assert.equal(forgedResult, forged);
  assert.equal(consumeLocalAdmissionBusyAfterConfirmedInactive(forgedResult), false,
    'a STORE_BUSY lookalike without raw SQLite provenance is not retry eligible');

  const makeDriver = () => {
    let transaction = false;
    return {
      get isTransaction() { return transaction; },
      exec(sql) {
        if (sql === 'BEGIN IMMEDIATE') { transaction = true; return; }
        if (sql === 'ROLLBACK') { transaction = false; return; }
        throw new Error('unexpected admission marker SQL');
      }
    };
  };
  const mappedFromRawBusy = () => {
    let caught;
    try {
      inLocalAdmissionTransaction(makeDriver(), '_localControllerAdmit', () => {
        throw syntheticSqliteBusy();
      });
    } catch (error) {
      caught = error;
    }
    assert.ok(caught instanceof RuntimeError);
    assert.equal(caught.code, 'STORE_BUSY');
    return caught;
  };
  const consumed = mappedFromRawBusy();
  assert.equal(consumeLocalAdmissionBusyAfterConfirmedInactive(consumed), true);
  assert.equal(consumeLocalAdmissionBusyAfterConfirmedInactive(consumed), false);
  let consumedReuse;
  try {
    inLocalAdmissionTransaction(makeDriver(), '_localControllerAdmit', () => { throw consumed; });
  } catch (error) {
    consumedReuse = error;
  }
  assert.equal(consumedReuse, consumed);
  assert.equal(consumeLocalAdmissionBusyAfterConfirmedInactive(consumedReuse), false,
    'a consumed mapped error cannot be reused to mint raw SQLite provenance');

  const marked = mappedFromRawBusy();
  let markedReuse;
  try {
    inLocalAdmissionTransaction(makeDriver(), '_localControllerAdmit', () => { throw marked; });
  } catch (error) {
    markedReuse = error;
  }
  assert.equal(markedReuse, marked);
  assert.equal(consumeLocalAdmissionBusyAfterConfirmedInactive(markedReuse), false,
    'a previously marked mapped error cannot be marked again');

  let nested;
  try {
    inLocalAdmissionTransaction(makeDriver(), '_localControllerAdmit', () => {
      try {
        inLocalAdmissionTransaction(makeDriver(), '_localControllerAdmit', () => {
          throw syntheticSqliteBusy();
        });
      } catch (error) {
        nested = error;
        throw error;
      }
    });
  } catch (error) {
    assert.equal(error, nested);
  }
  assert.equal(consumeLocalAdmissionBusyAfterConfirmedInactive(nested), false,
    'an inner transaction marker cannot be laundered through an outer admission transaction');

  const nonEligible = [
    { error: syntheticSqliteBusy({ code: 'BUSY', errcode: undefined }), code: 'STORE_BUSY' },
    { error: syntheticSqliteBusy({ code: 'LOCKED', errcode: undefined }), code: 'STORE_BUSY' },
    { error: syntheticSqliteBusy({ code: 'SQLITE_BUSY', errcode: 5 }), code: 'STORE_BUSY' },
    { error: syntheticSqliteBusy({ code: 'SQLITE_LOCKED', errcode: undefined }), code: 'STORE_BUSY' },
    { error: syntheticSqliteBusy({ code: 'ERR_SQLITE_ERROR', errcode: 6 }), code: 'STORE_BUSY' },
    { error: syntheticSqliteBusy({ code: 'ERR_SQLITE_ERROR', errcode: undefined }), code: 'STORE_WRITE_FAILED' },
    { error: { code: 'ERR_SQLITE_ERROR', errcode: 5 }, code: 'STORE_BUSY' },
    { error: localAdmissionBusy(), code: 'STORE_BUSY' }
  ];
  for (const control of nonEligible) {
    let caught;
    try {
      inLocalAdmissionTransaction(makeDriver(), '_localControllerAdmit', () => { throw control.error; });
    } catch (error) {
      caught = error;
    }
    assert.ok(caught instanceof RuntimeError);
    assert.equal(caught.code, control.code);
    assert.equal(consumeLocalAdmissionBusyAfterConfirmedInactive(caught), false);
  }
});

test('an exact local admission committed before BUSY reconciliation is accepted once', async t => {
  const temporary = temporaryDatabase('bounded-local-admission-committed-resolution-');
  const store = openStore(temporary.databasePath, Date.now, 20);
  const blockers = [];
  t.after(async () => {
    await Promise.all(blockers.map(blocker => blocker.cleanup()));
    store.close();
    temporary.cleanup();
  });
  const lease = createRunningWorkflow(store, 'local-admission-committed-resolution');
  const originalAdmit = store._localControllerAdmit.bind(store);
  const originalResolve = store._localControllerResolveAdmission.bind(store);
  let admissionCalls = 0;
  let transactionCalls = 0;
  let firstBusy = null;
  let resolverCalls = 0;
  store._localControllerAdmit = (...args) => {
    admissionCalls += 1;
    const blocker = sqliteWriterBlockerSync(temporary.databasePath);
    blockers.push(blocker);
    try {
      transactionCalls += 1;
      return originalAdmit(...args);
    } catch (error) {
      firstBusy = error;
      blocker.releaseAndWait();
      transactionCalls += 1;
      originalAdmit(...args);
      throw error;
    }
  };
  store._localControllerResolveAdmission = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  const result = await runBoundedLocalPlan({
    store, workflowId: lease.workflowId, ownerId: lease.ownerId, fence: lease.fence,
    planJson: SINGLE_PLAN, limitsJson: '{}', ...localAdmissionRetryBudget()
  });
  await Promise.all(blockers.map(blocker => blocker.exited));
  assert.ok(firstBusy instanceof RuntimeError);
  assert.equal(firstBusy.operation, '_localControllerAdmit');
  assert.equal(firstBusy.code, 'STORE_BUSY');
  assert.equal(result.status, 'completed');
  assert.equal(admissionCalls, 1, 'the controller observes one failed call and resolves its exact commit');
  assert.equal(transactionCalls, 2, 'the test fault-injection commits once after the confirmed rollback');
  assert.equal(resolverCalls, 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_admitted').length, 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_completed').length, 1);
  assert.equal(result.stoppingFacts.stepAttempts, 1);
  assert.equal(result.stoppingFacts.localRetries, 0);
  assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
});

test('a mismatched local admission commit remains unknown and is never duplicated', async t => {
  const temporary = temporaryDatabase('bounded-local-admission-partial-resolution-');
  const store = openStore(temporary.databasePath, Date.now, 20);
  const blockers = [];
  t.after(async () => {
    await Promise.all(blockers.map(blocker => blocker.cleanup()));
    store.close();
    temporary.cleanup();
  });
  const lease = createRunningWorkflow(store, 'local-admission-partial-resolution');
  const originalAdmit = store._localControllerAdmit.bind(store);
  const originalResolve = store._localControllerResolveAdmission.bind(store);
  let admissionCalls = 0;
  let transactionCalls = 0;
  let firstBusy = null;
  let resolverCalls = 0;
  store._localControllerAdmit = (...args) => {
    admissionCalls += 1;
    const blocker = sqliteWriterBlockerSync(temporary.databasePath);
    blockers.push(blocker);
    try {
      transactionCalls += 1;
      return originalAdmit(...args);
    } catch (error) {
      firstBusy = error;
      blocker.releaseAndWait();
      transactionCalls += 1;
      originalAdmit(...args);
      const raw = new DatabaseSync(temporary.databasePath);
      try {
        const removed = raw.prepare('DELETE FROM outbox WHERE event_id = ?').run(args[1].eventId);
        assert.equal(Number(removed.changes), 1);
      } finally {
        raw.close();
      }
      throw error;
    }
  };
  store._localControllerResolveAdmission = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  const result = await runBoundedLocalPlan({
    store, workflowId: lease.workflowId, ownerId: lease.ownerId, fence: lease.fence,
    planJson: SINGLE_PLAN, limitsJson: '{}', ...localAdmissionRetryBudget()
  });
  await Promise.all(blockers.map(blocker => blocker.exited));
  assert.ok(firstBusy instanceof RuntimeError);
  assert.equal(firstBusy.operation, '_localControllerAdmit');
  assert.equal(firstBusy.code, 'STORE_BUSY');
  assert.equal(result.status, 'stopped');
  assert.equal(result.reason, 'LOCAL_ADMISSION_OUTCOME_UNKNOWN');
  assert.equal(admissionCalls, 1);
  assert.equal(transactionCalls, 2, 'one original rollback and one injected mismatched commit');
  assert.equal(resolverCalls, 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_admitted').length, 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_completed').length, 0);
  assert.equal(countRows(temporary.databasePath, 'workflow_checkpoints'), 2);
  assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
});

test('all-present wrong admission event metadata stays unknown without retry, completion, or custody changes', async t => {
  const corruptions = [
    { name: 'revision', column: 'revision', mutate: (candidate, lease) => candidate.expectedRevision + 2 },
    { name: 'event type', column: 'event_type', mutate: () => 'private_test_wrong_admission_type' },
    { name: 'fence', column: 'fence_token', mutate: (candidate, lease) => lease.fence + 1 }
  ];
  for (const corruption of corruptions) {
    await t.test(corruption.name, async subtest => {
      const temporary = temporaryDatabase('bounded-local-admission-wrong-event-' + corruption.name + '-');
      const store = openStore(temporary.databasePath, Date.now, 20);
      const blockers = [];
      subtest.after(async () => {
        await Promise.all(blockers.map(blocker => blocker.cleanup()));
        store.close();
        temporary.cleanup();
      });
      const lease = createRunningWorkflow(store, 'local-admission-wrong-event-' + corruption.name);
      const custodyBefore = localCompletionExternalCustody(temporary.databasePath, lease.workflowId);
      const originalAdmit = store._localControllerAdmit.bind(store);
      const originalResolve = store._localControllerResolveAdmission.bind(store);
      let firstBusy = null;
      let admissionCalls = 0;
      let transactionCalls = 0;
      let resolverCalls = 0;
      let allRowsPresentAtResolution = false;
      let stateAtResolution = null;
      store._localControllerAdmit = (...args) => {
        admissionCalls += 1;
        const candidate = args[1];
        const blocker = sqliteWriterBlockerSync(temporary.databasePath);
        blockers.push(blocker);
        try {
          transactionCalls += 1;
          return originalAdmit(...args);
        } catch (error) {
          firstBusy = error;
          blocker.releaseAndWait();
          transactionCalls += 1;
          originalAdmit(...args);
          const raw = new DatabaseSync(temporary.databasePath);
          try {
            raw.exec('DROP TRIGGER workflow_events_no_update');
            raw.prepare(`UPDATE workflow_events SET ${corruption.column} = ? WHERE event_id = ?`)
              .run(corruption.mutate(candidate, lease), candidate.eventId);
            raw.exec(`
              CREATE TRIGGER workflow_events_no_update
              BEFORE UPDATE ON workflow_events BEGIN
                SELECT RAISE(ABORT, 'event log is immutable');
              END;
            `);
          } finally {
            raw.close();
          }
          throw error;
        }
      };
      store._localControllerResolveAdmission = (...args) => {
        resolverCalls += 1;
        const candidate = args[1];
        const raw = new DatabaseSync(temporary.databasePath);
        try {
          const event = raw.prepare(`
            SELECT sequence, event_id, workflow_id, revision, event_type, fence_token
            FROM workflow_events WHERE event_id = ?
          `).get(candidate.eventId);
          const outbox = raw.prepare(`
            SELECT event_id, event_sequence FROM outbox WHERE event_id = ?
          `).get(candidate.eventId);
          const checkpoint = raw.prepare(`
            SELECT sequence, revision FROM workflow_checkpoints WHERE workflow_id = ? AND sequence = ?
          `).get(candidate.workflowId, candidate.expectedCheckpointSequence + 1);
          allRowsPresentAtResolution = Boolean(event && outbox && checkpoint);
        } finally {
          raw.close();
        }
        stateAtResolution = snapshotDatabase(temporary.databasePath);
        const resolution = originalResolve(...args);
        assert.deepEqual(snapshotDatabase(temporary.databasePath), stateAtResolution,
          'resolving all-present mismatched metadata is read-only');
        return resolution;
      };

      const result = await runBoundedLocalPlan({
        store, workflowId: lease.workflowId, ownerId: lease.ownerId, fence: lease.fence,
        planJson: SINGLE_PLAN, limitsJson: '{}', ...localAdmissionRetryBudget()
      });
      await Promise.all(blockers.map(blocker => blocker.exited));
      assert.ok(firstBusy instanceof RuntimeError);
      assert.equal(firstBusy.operation, '_localControllerAdmit');
      assert.equal(firstBusy.code, 'STORE_BUSY');
      assert.equal(result.status, 'stopped');
      assert.equal(result.reason, 'LOCAL_ADMISSION_OUTCOME_UNKNOWN');
      assert.equal(admissionCalls, 1);
      assert.equal(transactionCalls, 2, 'one rolled-back attempt and one committed wrong-metadata fixture');
      assert.equal(resolverCalls, 1);
      assert.equal(allRowsPresentAtResolution, true);
      assert.equal(store.listEvents({ workflowId: lease.workflowId })
        .filter(event => event.type === 'private_local_step_completed').length, 0);
      assert.deepEqual(localCompletionExternalCustody(temporary.databasePath, lease.workflowId), custodyBefore);
      assert.deepEqual(snapshotDatabase(temporary.databasePath), stateAtResolution,
        'the stopped unknown outcome does not write or retry');
    });
  }
});

test('the local admission resolver rejects a wrong event, checkpoint digest, or outbox sequence', async t => {
  const temporary = temporaryDatabase('bounded-local-admission-resolution-identities-');
  const store = openStore(temporary.databasePath, Date.now, 20);
  t.after(() => {
    store.close();
    temporary.cleanup();
  });
  const lease = createRunningWorkflow(store, 'local-admission-resolution-identities');
  const previous = store._localControllerSnapshot(LOCAL_CONTROLLER_STORAGE_CAPABILITY, lease.workflowId);
  const eventId = lease.workflowId + ':exact-resolution-target';
  const candidate = makeAdmissionCandidate(store, lease, SINGLE_PLAN, eventId);
  const resolverInput = {
    ...candidate,
    previousCheckpointSha256: previous.checkpoint.sha256
  };
  store._localControllerAdmit(LOCAL_CONTROLLER_STORAGE_CAPABILITY, candidate);

  const exact = store._localControllerResolveAdmission(LOCAL_CONTROLLER_STORAGE_CAPABILITY, resolverInput);
  assert.equal(exact.kind, 'committed');
  assert.equal(exact.eventSequence, store.listEvents({ workflowId: lease.workflowId })
    .find(event => event.eventId === eventId).sequence);

  const wrongEvent = store._localControllerResolveAdmission(LOCAL_CONTROLLER_STORAGE_CAPABILITY, {
    ...resolverInput,
    eventId: eventId + ':wrong'
  });
  assert.equal(wrongEvent.kind, 'unknown');
  assert.equal(wrongEvent.reason, 'partial_or_mismatched_commit');

  const wrongDigestState = {
    ...resolverInput.controllerState,
    budget: {
      ...resolverInput.controllerState.budget,
      elapsedMs: resolverInput.controllerState.budget.elapsedMs + 1,
      lastWallMs: resolverInput.controllerState.budget.lastWallMs + 1
    }
  };
  const wrongDigest = store._localControllerResolveAdmission(LOCAL_CONTROLLER_STORAGE_CAPABILITY, {
    ...resolverInput,
    controllerState: wrongDigestState
  });
  assert.equal(wrongDigest.kind, 'unknown');
  assert.equal(wrongDigest.reason, 'partial_or_mismatched_commit');

  const raw = new DatabaseSync(temporary.databasePath);
  try {
    const event = raw.prepare('SELECT sequence FROM workflow_events WHERE event_id = ?').get(eventId);
    assert.ok(event);
    const inserted = raw.prepare(`
      INSERT INTO workflow_events (
        event_id, workflow_id, revision, event_type, from_state, to_state, fence_token, occurred_at_ms
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      eventId + ':outbox-sequence-witness', lease.workflowId, previous.workflow.revision + 1,
      'private_test_unpaired_event', 'running', 'running', lease.fence, Date.now()
    );
    const wrongSequence = Number(inserted.lastInsertRowid);
    assert.notEqual(wrongSequence, Number(event.sequence));
    const changed = raw.prepare('UPDATE outbox SET event_sequence = ? WHERE event_id = ?')
      .run(wrongSequence, eventId);
    assert.equal(Number(changed.changes), 1);
  } finally {
    raw.close();
  }
  const wrongOutboxSequence = store._localControllerResolveAdmission(
    LOCAL_CONTROLLER_STORAGE_CAPABILITY, resolverInput
  );
  assert.equal(wrongOutboxSequence.kind, 'unknown');
  assert.equal(wrongOutboxSequence.reason, 'partial_or_mismatched_commit');
});

test('an admission resolver read failure leaves the outcome unknown without a retry write', async t => {
  const temporary = temporaryDatabase('bounded-local-admission-resolver-read-failure-');
  const store = openStore(temporary.databasePath, Date.now, 20);
  const blockers = [];
  t.after(async () => {
    await Promise.all(blockers.map(blocker => blocker.cleanup()));
    store.close();
    temporary.cleanup();
  });
  const lease = createRunningWorkflow(store, 'local-admission-resolver-read-failure');
  const before = snapshotDatabase(temporary.databasePath);
  const originalAdmit = store._localControllerAdmit.bind(store);
  let firstBusy = null;
  let admissionCalls = 0;
  let resolverCalls = 0;
  store._localControllerAdmit = (...args) => {
    admissionCalls += 1;
    const blocker = sqliteWriterBlockerSync(temporary.databasePath);
    blockers.push(blocker);
    try {
      return originalAdmit(...args);
    } catch (error) {
      firstBusy = error;
      blocker.releaseAndWait();
      throw error;
    }
  };
  store._localControllerResolveAdmission = () => {
    resolverCalls += 1;
    throw new RuntimeError({
      module: 'state', operation: '_localControllerResolveAdmission', code: 'STORE_BUSY', retryable: true,
      safe_message: 'The durable store is busy.'
    });
  };

  const result = await runBoundedLocalPlan({
    store, workflowId: lease.workflowId, ownerId: lease.ownerId, fence: lease.fence,
    planJson: SINGLE_PLAN, limitsJson: '{}', ...localAdmissionRetryBudget()
  });
  await Promise.all(blockers.map(blocker => blocker.exited));
  assert.ok(firstBusy instanceof RuntimeError);
  assert.equal(firstBusy.operation, '_localControllerAdmit');
  assert.equal(firstBusy.code, 'STORE_BUSY');
  assert.equal(result.status, 'stopped');
  assert.equal(result.reason, 'LOCAL_ADMISSION_OUTCOME_UNKNOWN');
  assert.equal(admissionCalls, 1);
  assert.equal(resolverCalls, 1);
  assert.equal(snapshotDatabase(temporary.databasePath), before,
    'a resolver read failure cannot authorize a second admission write');
  assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
});

test('an exact admission commit observed after its owner fence changed is not accepted as current', async t => {
  const temporary = temporaryDatabase('bounded-local-admission-stale-committed-resolution-');
  const store = openStore(temporary.databasePath, Date.now, 20);
  const blockers = [];
  t.after(async () => {
    await Promise.all(blockers.map(blocker => blocker.cleanup()));
    store.close();
    temporary.cleanup();
  });
  const lease = createRunningWorkflow(store, 'local-admission-stale-committed-resolution');
  const originalAdmit = store._localControllerAdmit.bind(store);
  const originalResolve = store._localControllerResolveAdmission.bind(store);
  let firstBusy = null;
  let admissionCalls = 0;
  let transactionCalls = 0;
  let resolverCalls = 0;
  store._localControllerAdmit = (...args) => {
    admissionCalls += 1;
    const blocker = sqliteWriterBlockerSync(temporary.databasePath);
    blockers.push(blocker);
    try {
      transactionCalls += 1;
      return originalAdmit(...args);
    } catch (error) {
      firstBusy = error;
      blocker.releaseAndWait();
      transactionCalls += 1;
      originalAdmit(...args);
      store.releaseLease({
        workflowId: lease.workflowId, ownerId: lease.ownerId, fence: lease.fence,
        eventId: lease.workflowId + ':stale-resolution-release'
      });
      store.acquireLease({
        workflowId: lease.workflowId, ownerId: 'replacement-after-admission', ttlMs: 60_000,
        eventId: lease.workflowId + ':stale-resolution-takeover'
      });
      throw error;
    }
  };
  store._localControllerResolveAdmission = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  const result = await runBoundedLocalPlan({
    store, workflowId: lease.workflowId, ownerId: lease.ownerId, fence: lease.fence,
    planJson: SINGLE_PLAN, limitsJson: '{}', ...localAdmissionRetryBudget()
  });
  await Promise.all(blockers.map(blocker => blocker.exited));
  assert.ok(firstBusy instanceof RuntimeError);
  assert.equal(firstBusy.code, 'STORE_BUSY');
  assert.equal(result.status, 'stopped');
  assert.equal(result.reason, 'WORKFLOW_SNAPSHOT_CHANGED');
  assert.equal(admissionCalls, 1);
  assert.equal(transactionCalls, 2, 'the second store call is the injected exact commit');
  assert.equal(resolverCalls, 1);
  assert.equal(store.getWorkflow(lease.workflowId).leaseOwner, 'replacement-after-admission');
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_admitted').length, 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_completed').length, 0);
  assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
});

test('a transient initial admission BUSY retries the same single allowance', async t => {
  const temporary = temporaryDatabase('bounded-local-admission-initial-retry-');
  const store = openStore(temporary.databasePath, Date.now, 20);
  const blockers = [];
  t.after(async () => {
    await Promise.all(blockers.map(blocker => blocker.cleanup()));
    store.close();
    temporary.cleanup();
  });
  const lease = createRunningWorkflow(store, 'local-admission-initial-retry');
  const originalAdmit = store._localControllerAdmit.bind(store);
  const originalResolve = store._localControllerResolveAdmission.bind(store);
  const attempted = [];
  let firstBusy = null;
  let admissionCalls = 0;
  let resolverCalls = 0;
  store._localControllerAdmit = (...args) => {
    admissionCalls += 1;
    attempted.push(args[1]);
    if (admissionCalls !== 1) return originalAdmit(...args);
    const blocker = sqliteWriterBlockerSync(temporary.databasePath);
    blockers.push(blocker);
    try {
      return originalAdmit(...args);
    } catch (error) {
      firstBusy = error;
      blocker.releaseAndWait();
      throw error;
    }
  };
  store._localControllerResolveAdmission = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  const result = await runBoundedLocalPlan({
    store, workflowId: lease.workflowId, ownerId: lease.ownerId, fence: lease.fence,
    planJson: SINGLE_PLAN, limitsJson: '{}', ...localAdmissionRetryBudget()
  });
  await Promise.all(blockers.map(blocker => blocker.exited));
  assert.ok(firstBusy instanceof RuntimeError);
  assert.equal(firstBusy.operation, '_localControllerAdmit');
  assert.equal(firstBusy.code, 'STORE_BUSY');
  assert.equal(result.status, 'completed');
  assert.equal(attempted.length, 2, 'one initial attempt and one retry');
  assert.equal(attempted[0].admissionKind, 'step');
  assert.equal(attempted[0].eventId, attempted[1].eventId);
  assert.equal(attempted[0].stepId, attempted[1].stepId);
  assert.equal(attempted[0].controllerState.budget.stepsAdmitted,
    attempted[1].controllerState.budget.stepsAdmitted);
  for (const field of [
    'stepAttempts', 'localRetries', 'replans', 'iterations', 'totalResultBytes'
  ]) {
    assert.equal(attempted[0].controllerState.budget[field],
      attempted[1].controllerState.budget[field], field);
  }
  const { lastWallMs: initialWall, elapsedMs: initialElapsed, ...initialCounters } =
    attempted[0].controllerState.budget;
  const { lastWallMs: retryWall, elapsedMs: retryElapsed, ...retryCounters } =
    attempted[1].controllerState.budget;
  assert.deepEqual(retryCounters, initialCounters, 'only wall and elapsed clocks may refresh');
  assert.ok(retryWall >= initialWall);
  assert.ok(retryElapsed >= initialElapsed);
  assert.equal(resolverCalls, 1);
  assert.equal(result.stoppingFacts.stepsAdmitted, 1);
  assert.equal(result.stoppingFacts.stepAttempts, 1);
  assert.equal(result.stoppingFacts.localRetries, 0);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_admitted').length, 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_completed').length, 1);
  assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
});

test('an initial admission BUSY without the caller retry window is not reconciled or retried', async t => {
  const temporary = temporaryDatabase('bounded-local-admission-no-retry-window-');
  const store = openStore(temporary.databasePath, Date.now, 20);
  const blockers = [];
  t.after(async () => {
    await Promise.all(blockers.map(blocker => blocker.cleanup()));
    store.close();
    temporary.cleanup();
  });
  const lease = createRunningWorkflow(store, 'local-admission-no-retry-window');
  const originalAdmit = store._localControllerAdmit.bind(store);
  let firstBusy = null;
  let admissionCalls = 0;
  let resolverCalls = 0;
  store._localControllerAdmit = (...args) => {
    admissionCalls += 1;
    const blocker = sqliteWriterBlockerSync(temporary.databasePath);
    blockers.push(blocker);
    try {
      return originalAdmit(...args);
    } catch (error) {
      firstBusy = error;
      blocker.releaseAndWait();
      throw error;
    }
  };
  const originalResolve = store._localControllerResolveAdmission.bind(store);
  store._localControllerResolveAdmission = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  const result = await runBoundedLocalPlan({
    store, workflowId: lease.workflowId, ownerId: lease.ownerId, fence: lease.fence,
    planJson: SINGLE_PLAN, limitsJson: '{}'
  });
  await Promise.all(blockers.map(blocker => blocker.exited));
  assert.ok(firstBusy instanceof RuntimeError);
  assert.equal(firstBusy.operation, '_localControllerAdmit');
  assert.equal(firstBusy.code, 'STORE_BUSY');
  assert.equal(result.status, 'stopped');
  assert.equal(result.reason, 'LOCAL_CONTROLLER_STOPPED');
  assert.equal(admissionCalls, 1);
  assert.equal(resolverCalls, 0);
  assert.equal(store.getCheckpoint(lease.workflowId).sequence, 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .some(event => event.type === 'private_local_step_admitted'), false);
  assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
});

test('a transient resumed replay admission BUSY preserves its replay allowance and event identity', async t => {
  const temporary = temporaryDatabase('bounded-local-admission-replay-retry-');
  const store = openStore(temporary.databasePath, Date.now, 20);
  const blockers = [];
  t.after(async () => {
    await Promise.all(blockers.map(blocker => blocker.cleanup()));
    store.close();
    temporary.cleanup();
  });
  const lease = createRunningWorkflow(store, 'local-admission-replay-retry');
  makeAdmission(store, lease, SINGLE_PLAN, '{}', lease.workflowId + ':original-admission');
  const before = store.getCheckpoint(lease.workflowId).data;
  assert.equal(inspectWorkflowRecovery({ store, workflowId: lease.workflowId }).kind,
    'local-replay-candidate');
  const originalAdmit = store._localControllerAdmit.bind(store);
  const originalResolve = store._localControllerResolveAdmission.bind(store);
  const attempted = [];
  let firstBusy = null;
  let admissionCalls = 0;
  let resolverCalls = 0;
  store._localControllerAdmit = (...args) => {
    admissionCalls += 1;
    attempted.push(args[1]);
    if (admissionCalls !== 1) return originalAdmit(...args);
    const blocker = sqliteWriterBlockerSync(temporary.databasePath);
    blockers.push(blocker);
    try {
      return originalAdmit(...args);
    } catch (error) {
      firstBusy = error;
      blocker.releaseAndWait();
      throw error;
    }
  };
  store._localControllerResolveAdmission = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  const result = await runBoundedLocalPlan({
    store, workflowId: lease.workflowId, ownerId: lease.ownerId, fence: lease.fence,
    ...localAdmissionRetryBudget()
  });
  await Promise.all(blockers.map(blocker => blocker.exited));
  assert.ok(firstBusy instanceof RuntimeError);
  assert.equal(firstBusy.operation, '_localControllerAdmit');
  assert.equal(firstBusy.code, 'STORE_BUSY');
  assert.equal(result.status, 'completed');
  assert.equal(attempted.length, 2, 'one failed replay write and one replay retry');
  assert.equal(attempted[0].admissionKind, 'replay');
  assert.equal(attempted[1].admissionKind, 'replay');
  assert.equal(attempted[0].eventId, attempted[1].eventId);
  assert.equal(attempted[0].stepId, attempted[1].stepId);
  assert.equal(attempted[0].controllerState.budget.stepsAdmitted,
    attempted[1].controllerState.budget.stepsAdmitted);
  for (const field of [
    'stepAttempts', 'localRetries', 'replans', 'iterations', 'totalResultBytes'
  ]) {
    assert.equal(attempted[0].controllerState.budget[field],
      attempted[1].controllerState.budget[field], field);
  }
  const { lastWallMs: initialWall, elapsedMs: initialElapsed, ...initialCounters } =
    attempted[0].controllerState.budget;
  const { lastWallMs: retryWall, elapsedMs: retryElapsed, ...retryCounters } =
    attempted[1].controllerState.budget;
  assert.deepEqual(retryCounters, initialCounters, 'only wall and elapsed clocks may refresh');
  assert.ok(retryWall >= initialWall);
  assert.ok(retryElapsed >= initialElapsed);
  assert.equal(resolverCalls, 1);
  const after = store.getCheckpoint(lease.workflowId).data;
  assert.equal(after.budget.stepsAdmitted, before.budget.stepsAdmitted);
  assert.equal(after.budget.stepAttempts, before.budget.stepAttempts + 1);
  assert.equal(after.budget.localRetries, before.budget.localRetries + 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_admitted').length, 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_replayed').length, 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_completed').length, 1);
  assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
});

test('a transient next-step admission BUSY retries one immutable allowance with no duplicate accounting', async t => {
  const temporary = temporaryDatabase('bounded-local-admission-next-step-retry-');
  const store = openStore(temporary.databasePath, Date.now, 20);
  const blockers = [];
  t.after(async () => {
    await Promise.all(blockers.map(blocker => blocker.cleanup()));
    store.close();
    temporary.cleanup();
  });
  const lease = createRunningWorkflow(store, 'local-admission-next-step-retry');
  const originalAdmit = store._localControllerAdmit.bind(store);
  const originalResolve = store._localControllerResolveAdmission.bind(store);
  const attempted = [];
  let firstBusy = null;
  let resolverCalls = 0;
  let admissionCalls = 0;
  store._localControllerAdmit = (...args) => {
    admissionCalls += 1;
    attempted.push(args[1]);
    if (admissionCalls !== 2) return originalAdmit(...args);
    const blocker = sqliteWriterBlockerSync(temporary.databasePath);
    blockers.push(blocker);
    try {
      return originalAdmit(...args);
    } catch (error) {
      firstBusy = error;
      blocker.releaseAndWait();
      throw error;
    }
  };
  store._localControllerResolveAdmission = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  const result = await runBoundedLocalPlan({
    store, workflowId: lease.workflowId, ownerId: lease.ownerId, fence: lease.fence,
    planJson: TWO_STEP_PLAN, limitsJson: '{}', ...localAdmissionRetryBudget()
  });
  await Promise.all(blockers.map(blocker => blocker.exited));
  assert.ok(firstBusy instanceof RuntimeError);
  assert.equal(firstBusy.module, 'state');
  assert.equal(firstBusy.operation, '_localControllerAdmit');
  assert.equal(firstBusy.code, 'STORE_BUSY');
  assert.equal(firstBusy.retryable, true);
  assert.equal(result.status, 'completed');
  assert.equal(admissionCalls, 3, 'initial write, one failed next-step write, then one retry');
  assert.equal(resolverCalls, 1);
  assert.deepEqual(result.results.map(record => record.output), [9, 15]);
  assert.equal(attempted[1].eventId, attempted[2].eventId);
  assert.equal(attempted[1].stepId, attempted[2].stepId);
  assert.equal(attempted[1].controllerState.completedResultsJson,
    attempted[2].controllerState.completedResultsJson);
  assert.equal(JSON.parse(attempted[1].controllerState.completedResultsJson).first.output, 9,
    'the committed first result is retained in the immutable next-step allowance');
  assert.equal(attempted[1].controllerState.budget.stepsAdmitted,
    attempted[2].controllerState.budget.stepsAdmitted);
  for (const field of [
    'stepAttempts', 'localRetries', 'replans', 'iterations', 'totalResultBytes'
  ]) {
    assert.equal(attempted[1].controllerState.budget[field], attempted[2].controllerState.budget[field], field);
  }
  const { lastWallMs: failedWall, elapsedMs: failedElapsed, ...failedCounters } =
    attempted[1].controllerState.budget;
  const { lastWallMs: retryWall, elapsedMs: retryElapsed, ...retryCounters } =
    attempted[2].controllerState.budget;
  assert.deepEqual(retryCounters, failedCounters, 'only wall and elapsed clocks may refresh');
  assert.ok(retryWall >= failedWall);
  assert.ok(retryElapsed >= failedElapsed);
  assert.equal(result.stoppingFacts.stepsAdmitted, 2);
  assert.equal(result.stoppingFacts.stepAttempts, 2);
  assert.equal(result.stoppingFacts.localRetries, 0);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_admitted').length, 2);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_completed').length, 2);
  assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
});

test('the original single-chain 32-step plan remains rejected before local admission', async t => {
  const temporary = temporaryDatabase('bounded-local-admission-depth-thirty-two-');
  const store = openStore(temporary.databasePath, Date.now, 20);
  t.after(() => {
    store.close();
    temporary.cleanup();
  });
  const lease = createRunningWorkflow(store, 'local-admission-depth-thirty-two');
  const checkpointBefore = store.getCheckpoint(lease.workflowId);
  const eventsBefore = store.listEvents({ workflowId: lease.workflowId }).length;
  let admissionCalls = 0;
  let resolverCalls = 0;
  const originalAdmit = store._localControllerAdmit.bind(store);
  const originalResolve = store._localControllerResolveAdmission.bind(store);
  store._localControllerAdmit = (...args) => {
    admissionCalls += 1;
    return originalAdmit(...args);
  };
  store._localControllerResolveAdmission = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  const result = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    planJson: DEPTH_THIRTY_TWO_PLAN,
    limitsJson: '{}',
    ...localAdmissionRetryBudget()
  });

  assert.equal(result.status, 'stopped');
  assert.equal(result.reason, 'PLAN_BOUNDS_EXCEEDED');
  assert.equal(admissionCalls, 0);
  assert.equal(resolverCalls, 0);
  assert.equal(store.getWorkflow(lease.workflowId).revision, lease.revision);
  assert.equal(store.getCheckpoint(lease.workflowId).sha256, checkpointBefore.sha256);
  assert.equal(store.listEvents({ workflowId: lease.workflowId }).length, eventsBefore);
  assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
});

test('two maximum-step local controllers finish under a transient held SQLite writer', async t => {
  const temporary = temporaryDatabase('bounded-local-admission-two-controllers-');
  const firstStore = openStore(temporary.databasePath, Date.now, 20);
  const secondStore = openStore(temporary.databasePath, Date.now, 20);
  const blockers = [];
  t.after(async () => {
    await Promise.all(blockers.map(blocker => blocker.cleanup()));
    firstStore.close();
    secondStore.close();
    temporary.cleanup();
  });
  const firstLease = createRunningWorkflow(firstStore, 'local-admission-two-controllers-first');
  const secondLease = createRunningWorkflow(secondStore, 'local-admission-two-controllers-second');
  const originalAdmit = firstStore._localControllerAdmit.bind(firstStore);
  const originalResolve = firstStore._localControllerResolveAdmission.bind(firstStore);
  let admissionCalls = 0;
  let resolverCalls = 0;
  let firstBusy = null;
  firstStore._localControllerAdmit = (...args) => {
    admissionCalls += 1;
    if (admissionCalls !== 2) return originalAdmit(...args);
    const blocker = sqliteWriterBlockerSync(temporary.databasePath);
    blockers.push(blocker);
    try {
      return originalAdmit(...args);
    } catch (error) {
      firstBusy = error;
      blocker.releaseAndWait();
      throw error;
    }
  };
  firstStore._localControllerResolveAdmission = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  const [firstResult, secondResult] = await Promise.all([
    runBoundedLocalPlan({
      store: firstStore,
      workflowId: firstLease.workflowId,
      ownerId: firstLease.ownerId,
      fence: firstLease.fence,
      planJson: THIRTY_TWO_STEP_PLAN,
      limitsJson: '{}',
      ...localAdmissionRetryBudget()
    }),
    runBoundedLocalPlan({
      store: secondStore,
      workflowId: secondLease.workflowId,
      ownerId: secondLease.ownerId,
      fence: secondLease.fence,
      planJson: THIRTY_TWO_STEP_PLAN,
      limitsJson: '{}',
      ...localAdmissionRetryBudget()
    })
  ]);
  await Promise.all(blockers.map(blocker => blocker.exited));
  assert.ok(firstBusy instanceof RuntimeError);
  assert.equal(firstBusy.operation, '_localControllerAdmit');
  assert.equal(firstBusy.code, 'STORE_BUSY');
  assert.equal(firstResult.status, 'completed');
  assert.equal(secondResult.status, 'completed');
  assert.equal(firstResult.results.length, 32);
  assert.equal(secondResult.results.length, 32);
  const orderedOutputs = Array.from({ length: 32 }, (_, index) => index + 1);
  assert.deepEqual(firstResult.results.map(record => record.output), orderedOutputs);
  assert.deepEqual(secondResult.results.map(record => record.output), orderedOutputs);
  assert.equal(admissionCalls, 33);
  assert.equal(resolverCalls, 1);
  assert.equal(firstResult.stoppingFacts.stepsAdmitted, 32);
  assert.equal(firstResult.stoppingFacts.stepAttempts, 32);
  assert.equal(firstResult.stoppingFacts.localRetries, 0);
  assert.equal(secondResult.stoppingFacts.stepsAdmitted, 32);
  assert.equal(secondResult.stoppingFacts.stepAttempts, 32);
  assert.equal(secondResult.stoppingFacts.localRetries, 0);
  for (const [store, lease] of [[firstStore, firstLease], [secondStore, secondLease]]) {
    const events = store.listEvents({ workflowId: lease.workflowId });
    assert.equal(events.filter(event => event.type === 'private_local_step_admitted').length, 32);
    assert.equal(events.filter(event => event.type === 'private_local_step_completed').length, 32);
    assert.equal(store.getWorkflow(lease.workflowId).checkpointSequence, 65);
    const reopened = openStore(temporary.databasePath, Date.now, 20);
    try {
      const persisted = JSON.parse(reopened.getCheckpoint(lease.workflowId).data.completedResultsJson);
      assert.deepEqual(Object.keys(persisted), Array.from({ length: 32 }, (_, index) =>
        'step-' + String(index + 1).padStart(2, '0')));
      assert.deepEqual(Object.values(persisted).map(record => record.output), orderedOutputs);
      assert.equal(reopened.getCheckpoint(lease.workflowId).data.pendingStepId, null);
      assert.equal(reopened.getWorkflow(lease.workflowId).checkpointSequence, 65);
    } finally {
      reopened.close();
    }
  }
  assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
});

test('persistent next-step admission BUSY stops at the existing three-call bound without charging an allowance', async t => {
  const temporary = temporaryDatabase('bounded-local-admission-persistent-busy-');
  const store = openStore(temporary.databasePath, Date.now, 20);
  const blockers = [];
  t.after(async () => {
    await Promise.all(blockers.map(blocker => blocker.cleanup()));
    store.close();
    temporary.cleanup();
  });
  const lease = createRunningWorkflow(store, 'local-admission-persistent-busy');
  const originalAdmit = store._localControllerAdmit.bind(store);
  const originalResolve = store._localControllerResolveAdmission.bind(store);
  const attempted = [];
  let admissionCalls = 0;
  let resolverCalls = 0;
  store._localControllerAdmit = (...args) => {
    admissionCalls += 1;
    attempted.push(args[1]);
    if (admissionCalls !== 2) return originalAdmit(...args);
    blockers.push(sqliteWriterBlockerSync(temporary.databasePath));
    return originalAdmit(...args);
  };
  store._localControllerResolveAdmission = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  const result = await runBoundedLocalPlan({
    store, workflowId: lease.workflowId, ownerId: lease.ownerId, fence: lease.fence,
    planJson: TWO_STEP_PLAN, limitsJson: '{}', ...localAdmissionRetryBudget()
  });
  for (const blocker of blockers) blocker.releaseAndWait();
  await Promise.all(blockers.map(blocker => blocker.exited));
  assert.equal(result.status, 'stopped');
  assert.equal(result.reason, 'LOCAL_ADMISSION_BUSY');
  assert.equal(admissionCalls, 4, 'initial write plus exactly three bounded attempts at the next allowance');
  assert.equal(resolverCalls, 3);
  assert.equal(attempted[1].eventId, attempted[2].eventId);
  assert.equal(attempted[2].eventId, attempted[3].eventId);
  assert.equal(attempted[1].controllerState.budget.stepAttempts,
    attempted[2].controllerState.budget.stepAttempts);
  assert.equal(attempted[2].controllerState.budget.stepAttempts,
    attempted[3].controllerState.budget.stepAttempts);
  for (const field of ['stepsAdmitted', 'localRetries', 'replans', 'iterations', 'totalResultBytes']) {
    assert.equal(attempted[1].controllerState.budget[field], attempted[2].controllerState.budget[field], field);
    assert.equal(attempted[2].controllerState.budget[field], attempted[3].controllerState.budget[field], field);
  }
  const checkpoint = store.getCheckpoint(lease.workflowId).data;
  assert.equal(checkpoint.pendingStepId, null);
  assert.equal(checkpoint.budget.stepsAdmitted, 1);
  assert.equal(checkpoint.budget.stepAttempts, 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_admitted').length, 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_completed').length, 1);
  assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
});

test('a pending admission rechecks cancellation, fencing, and unresolved external custody before retry', async t => {
  const controls = [
    { name: 'cancellation', expectedReason: 'WORKFLOW_TERMINAL' },
    { name: 'fence', expectedReason: 'LEASE_FENCED' },
    { name: 'checkpoint-hash', expectedReason: 'WORKFLOW_SNAPSHOT_CHANGED' },
    { name: 'external', expectedReason: 'EXTERNAL_OUTCOME_UNRESOLVED' }
  ];
  for (const control of controls) {
    await t.test(`${control.name} after pending blocks the admission retry`, async () => {
      const temporary = temporaryDatabase('bounded-local-admission-after-pending-' + control.name + '-');
      const store = openStore(temporary.databasePath, Date.now, 20);
      const workflowId = 'local-admission-after-pending-' + control.name;
      const lease = createRunningWorkflow(store, workflowId);
      const blockers = [];
      try {
        const originalAdmit = store._localControllerAdmit.bind(store);
        const originalResolve = store._localControllerResolveAdmission.bind(store);
        let admissionCalls = 0;
        let resolverCalls = 0;
        let changedAfterPending = false;
        let workflowBeforeControl = null;
        let checkpointBeforeControl = null;
        let cancellationResult = null;
        let replacementLease = null;
        const custodyBefore = localCompletionExternalCustody(temporary.databasePath, workflowId);
        const custodyRowsBefore = localExternalCustodyRows(temporary.databasePath, workflowId);
        let custodyAtPending = null;
        let custodyRowsAtPending = null;
        store._localControllerAdmit = (...args) => {
          admissionCalls += 1;
          if (admissionCalls !== 2) return originalAdmit(...args);
          const blocker = sqliteWriterBlockerSync(temporary.databasePath);
          blockers.push(blocker);
          try {
            return originalAdmit(...args);
          } catch (error) {
            blocker.releaseAndWait();
            throw error;
          }
        };
        store._localControllerResolveAdmission = (...args) => {
          resolverCalls += 1;
          const resolution = originalResolve(...args);
          if (resolution.kind === 'pending' && !changedAfterPending) {
            changedAfterPending = true;
            const workflow = store.getWorkflow(workflowId);
            if (control.name === 'cancellation') {
              workflowBeforeControl = workflow;
              checkpointBeforeControl = store.getCheckpoint(workflowId);
              cancellationResult = store.requestCancellation({
                workflowId,
                ownerId: lease.ownerId,
                fence: lease.fence,
                expectedRevision: workflow.revision,
                eventId: workflowId + ':cancel-after-pending'
              });
              assert.equal(cancellationResult.accepted, true);
            } else if (control.name === 'fence') {
              workflowBeforeControl = workflow;
              checkpointBeforeControl = store.getCheckpoint(workflowId);
              store.releaseLease({
                workflowId,
                ownerId: lease.ownerId,
                fence: lease.fence,
                eventId: workflowId + ':release-after-pending'
              });
              replacementLease = store.acquireLease({
                workflowId,
                ownerId: 'replacement-after-pending',
                ttlMs: 60_000,
                eventId: workflowId + ':takeover-after-pending'
              });
            } else if (control.name === 'checkpoint-hash') {
              const checkpoint = store.getCheckpoint(workflowId);
              rewriteLatestPrivateCheckpoint(temporary.databasePath, workflowId, {
                ...checkpoint.data,
                budget: {
                  ...checkpoint.data.budget,
                  lastWallMs: checkpoint.data.budget.lastWallMs + 1
                }
              });
            } else {
              store.recordDispatchIntent({
                workflowId,
                ownerId: lease.ownerId,
                fence: lease.fence,
                expectedRevision: workflow.revision,
                attemptId: workflowId + ':attempt',
                logicalOperationRef: workflowId + ':operation',
                authorityHoldRef: workflowId + ':hold',
                transportAttemptId: workflowId + ':transport',
                eventId: workflowId + ':intent'
              });
              custodyAtPending = localCompletionExternalCustody(temporary.databasePath, workflowId);
              custodyRowsAtPending = localExternalCustodyRows(temporary.databasePath, workflowId);
            }
          }
          return resolution;
        };

        const result = await runBoundedLocalPlan({
          store,
          workflowId,
          ownerId: lease.ownerId,
          fence: lease.fence,
          planJson: TWO_STEP_PLAN,
          limitsJson: '{}',
          ...localAdmissionRetryBudget()
        });
        await Promise.all(blockers.map(blocker => blocker.exited));
        assert.equal(changedAfterPending, true, control.name);
        assert.equal(result.status, control.name === 'external'
          ? 'external-reconciliation-required' : 'stopped', control.name);
        assert.equal(result.reason, control.expectedReason, control.name);
        if (control.name === 'cancellation') {
          const cancelled = store.getWorkflow(workflowId);
          const checkpointAfterCancellation = store.getCheckpoint(workflowId);
          assert.equal(cancellationResult.state, 'cancelled');
          assert.equal(cancellationResult.revision, workflowBeforeControl.revision + 1);
          assert.equal(cancelled.state, 'cancelled');
          assert.equal(cancelled.cancellationRequested, true);
          assert.equal(cancelled.revision, cancellationResult.revision);
          assert.equal(cancelled.leaseOwner, null);
          assert.equal(cancelled.leaseExpiresAtMs, null);
          assert.equal(checkpointAfterCancellation.sequence, checkpointBeforeControl.sequence);
          assert.equal(checkpointAfterCancellation.sha256, checkpointBeforeControl.sha256);
          assert.deepEqual(checkpointAfterCancellation.data, checkpointBeforeControl.data);
          assert.equal(store.listEvents({ workflowId }).filter(event =>
            event.type === 'workflow_cancelled').length, 1);
        } else if (control.name === 'fence') {
          const fenced = store.getWorkflow(workflowId);
          const checkpointAfterTakeover = store.getCheckpoint(workflowId);
          assert.equal(fenced.revision, workflowBeforeControl.revision);
          assert.equal(fenced.state, workflowBeforeControl.state);
          assert.equal(fenced.cancellationRequested, workflowBeforeControl.cancellationRequested);
          assert.equal(fenced.leaseOwner, 'replacement-after-pending');
          assert.equal(fenced.fence, replacementLease.fence);
          assert.ok(fenced.fence > workflowBeforeControl.fence);
          assert.equal(checkpointAfterTakeover.sequence, checkpointBeforeControl.sequence);
          assert.equal(checkpointAfterTakeover.sha256, checkpointBeforeControl.sha256);
          assert.deepEqual(checkpointAfterTakeover.data, checkpointBeforeControl.data);
        }
        assert.equal(admissionCalls, 2, control.name);
        assert.equal(resolverCalls, 1, control.name);
        const checkpoint = store.getCheckpoint(workflowId).data;
        assert.equal(checkpoint.pendingStepId, null, control.name);
        assert.equal(checkpoint.budget.stepsAdmitted, 1, control.name);
        assert.equal(checkpoint.budget.stepAttempts, 1, control.name);
        assert.equal(store.listEvents({ workflowId }).filter(event =>
          event.type === 'private_local_step_admitted').length, 1, control.name);
        assert.equal(store.listEvents({ workflowId }).filter(event =>
          event.type === 'private_local_step_completed').length, 1, control.name);
        assert.equal(countRows(temporary.databasePath, 'operation_attempts'), control.name === 'external' ? 1 : 0);
        const custodyAfter = localCompletionExternalCustody(temporary.databasePath, workflowId);
        const custodyRowsAfter = localExternalCustodyRows(temporary.databasePath, workflowId);
        if (control.name === 'external') {
          assert.deepEqual(custodyRowsBefore, {
            operationAttempts: [], transportAttempts: [], responseReceipts: [], attemptObservations: []
          });
          assert.deepEqual(custodyAtPending, {
            operationAttempts: 1, authorityHoldRefs: 1, transportAttempts: 1, responseReceipts: 0
          });
          assert.equal(custodyRowsAtPending.operationAttempts.length, 1);
          const externalAttemptRow = custodyRowsAtPending.operationAttempts[0];
          assert.deepEqual(custodyRowsAtPending.operationAttempts[0], {
            attempt_id: workflowId + ':attempt',
            workflow_id: workflowId,
            logical_operation_ref: workflowId + ':operation',
            authority_hold_ref: workflowId + ':hold',
            state: 'intent',
            created_at_ms: externalAttemptRow.created_at_ms,
            updated_at_ms: externalAttemptRow.updated_at_ms
          });
          assert.ok(Number.isSafeInteger(externalAttemptRow.created_at_ms));
          assert.equal(externalAttemptRow.created_at_ms, externalAttemptRow.updated_at_ms);
          assert.deepEqual(custodyRowsAtPending.transportAttempts, [{
            transport_attempt_id: workflowId + ':transport',
            attempt_id: workflowId + ':attempt',
            created_at_ms: externalAttemptRow.created_at_ms
          }]);
          assert.equal(custodyRowsAtPending.responseReceipts.length, 0);
          assert.equal(custodyRowsAtPending.attemptObservations.length, 0);
          assert.deepEqual(custodyAfter, custodyAtPending,
            'the stopped controller preserves the unresolved hold without transport or receipt');
          assert.deepEqual(custodyRowsAfter, custodyRowsAtPending,
            'the stop preserves exact attempt, hold, transport and response-row identities and values');
        } else {
          assert.deepEqual(custodyAfter, custodyBefore, 'a local admission retry creates no external custody');
          assert.deepEqual(custodyRowsAfter, custodyRowsBefore,
            'a local admission retry preserves exact external custody rows');
        }
      } finally {
        await Promise.all(blockers.map(blocker => blocker.cleanup()));
        store.close();
        temporary.cleanup();
      }
    });
  }
});

test('a pending admission does not retry when resolution consumes the live parent deadline to its busy window', async t => {
  const temporary = temporaryDatabase('bounded-local-admission-deadline-');
  const store = openStore(temporary.databasePath, Date.now, 20);
  const blockers = [];
  t.after(async () => {
    await Promise.all(blockers.map(blocker => blocker.cleanup()));
    store.close();
    temporary.cleanup();
  });
  const lease = createRunningWorkflow(store, 'local-admission-deadline');
  const originalAdmit = store._localControllerAdmit.bind(store);
  const originalResolve = store._localControllerResolveAdmission.bind(store);
  const deadlineBudget = localAdmissionRetryBudget(20, 500);
  const busyWaitNs = BigInt(deadlineBudget.busyTimeoutMs) * 1_000_000n;
  let admissionCalls = 0;
  let resolverCalls = 0;
  const attempted = [];
  let beforeSecondWrite = null;
  let remainingAtPending = null;
  let remainingAfterConsumption = null;
  store._localControllerAdmit = (...args) => {
    admissionCalls += 1;
    attempted.push(args[1]);
    if (admissionCalls !== 2) return originalAdmit(...args);
    beforeSecondWrite = snapshotDatabase(temporary.databasePath);
    const blocker = sqliteWriterBlockerSync(temporary.databasePath);
    blockers.push(blocker);
    try {
      return originalAdmit(...args);
    } catch (error) {
      blocker.releaseAndWait();
      throw error;
    }
  };
  store._localControllerResolveAdmission = (...args) => {
    resolverCalls += 1;
    const resolution = originalResolve(...args);
    if (resolution.kind === 'pending') {
      remainingAtPending = deadlineBudget.runtimeDeadlineNs - process.hrtime.bigint();
      const remainingBeforeWait = remainingAtPending - busyWaitNs + 2_000_000n;
      const waitMs = Math.max(1, Number((remainingBeforeWait + 999_999n) / 1_000_000n));
      Atomics.wait(new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT)), 0, 0, waitMs);
      remainingAfterConsumption = deadlineBudget.runtimeDeadlineNs - process.hrtime.bigint();
    }
    return resolution;
  };

  const result = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    planJson: TWO_STEP_PLAN,
    limitsJson: '{}',
    ...deadlineBudget
  });
  await Promise.all(blockers.map(blocker => blocker.exited));
  assert.equal(result.status, 'stopped');
  assert.equal(result.reason, 'LOCAL_ADMISSION_BUSY');
  assert.equal(admissionCalls, 2);
  assert.equal(attempted.length, 2, 'the sole failed next-step call is not repeated');
  assert.equal(resolverCalls, 1);
  assert.ok(remainingAtPending > busyWaitNs, 'the retry window was live when the resolver found pending');
  assert.ok(remainingAfterConsumption <= busyWaitNs,
    'the original absolute caller deadline is not refreshed after pending resolution');
  assert.deepEqual(snapshotDatabase(temporary.databasePath), beforeSecondWrite,
    'deadline exhaustion leaves the failed admission transaction rolled back');
  const checkpoint = store.getCheckpoint(lease.workflowId).data;
  assert.equal(checkpoint.pendingStepId, null);
  assert.equal(checkpoint.budget.stepsAdmitted, 1);
  assert.equal(checkpoint.budget.stepAttempts, 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_admitted').length, 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_completed').length, 1);
  assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
});

test('a pending admission does not refresh an expired lease, backwards wall clock, or original budget deadline', async t => {
  const controls = [
    { name: 'lease expiry', reason: 'LEASE_FENCED', limitsJson: '{}', mutate: (clock, lease) => clock.set(lease.expiresAtMs) },
    { name: 'wall-clock rollback', reason: 'CLOCK_ROLLBACK', limitsJson: '{}', mutate: (clock, lease, candidate) => clock.set(candidate.controllerState.budget.lastWallMs - 1) },
    {
      name: 'workload deadline', reason: 'DEADLINE_EXCEEDED', limitsJson: '{"maxElapsedMs":2000}',
      waitForDeadlineAtPendingAdmission: true,
      mutate: (clock, lease, candidate) => clock.set(Math.max(
        clock.read(), candidate.controllerState.budget.deadlineWallMs
      ))
    }
  ];
  for (const control of controls) {
    await t.test(control.name, async subtest => {
      const temporary = temporaryDatabase('bounded-local-admission-clock-' + control.name + '-');
      const clock = mutableClock(100_000, { monotonic: control.name === 'workload deadline' });
      const store = openStore(temporary.databasePath, clock.read, 20);
      const blockers = [];
      subtest.after(async () => {
        await Promise.all(blockers.map(blocker => blocker.cleanup()));
        store.close();
        temporary.cleanup();
      });
      const workflowId = 'local-admission-clock-' + control.name;
      const lease = createRunningWorkflow(store, workflowId, 'worker-local', 60_000);
      const custodyBefore = localCompletionExternalCustody(temporary.databasePath, workflowId);
      const originalAdmit = store._localControllerAdmit.bind(store);
      const originalResolve = store._localControllerResolveAdmission.bind(store);
      let admissionCalls = 0;
      let resolverCalls = 0;
      let committedStateAtPending = null;
      store._localControllerAdmit = (...args) => {
        admissionCalls += 1;
        if (admissionCalls !== 2) return originalAdmit(...args);
        const blocker = sqliteWriterBlockerSync(temporary.databasePath);
        blockers.push(blocker);
        try {
          if (control.waitForDeadlineAtPendingAdmission === true) {
            const candidateBudget = args[1]?.controllerState?.budget;
            assert.ok(Number.isSafeInteger(candidateBudget?.deadlineWallMs), control.name);
            const untilDeadlineMs = Math.max(1, candidateBudget.deadlineWallMs - clock.read() + 5);
            assert.ok(untilDeadlineMs <= 2_005, control.name);
            Atomics.wait(new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT)), 0, 0, untilDeadlineMs);
          }
          return originalAdmit(...args);
        } catch (error) {
          blocker.releaseAndWait();
          throw error;
        }
      };
      store._localControllerResolveAdmission = (...args) => {
        resolverCalls += 1;
        const resolution = originalResolve(...args);
        if (resolution.kind === 'pending') {
          control.mutate(clock, lease, args[1]);
          committedStateAtPending = snapshotDatabase(temporary.databasePath);
        }
        return resolution;
      };

      const result = await runBoundedLocalPlan({
        store, workflowId, ownerId: lease.ownerId, fence: lease.fence,
        planJson: TWO_STEP_PLAN, limitsJson: control.limitsJson, ...localAdmissionRetryBudget()
      });
      await Promise.all(blockers.map(blocker => blocker.exited));
      assert.equal(result.status, 'stopped', control.name);
      assert.equal(result.reason, control.reason, control.name);
      assert.equal(admissionCalls, 2, control.name);
      assert.equal(resolverCalls, 1, control.name);
      assert.deepEqual(snapshotDatabase(temporary.databasePath), committedStateAtPending,
        control.name + ' does not produce another admission write');
      const checkpoint = store.getCheckpoint(workflowId).data;
      assert.equal(checkpoint.pendingStepId, null, control.name);
      assert.equal(checkpoint.budget.stepsAdmitted, 1, control.name);
      assert.equal(checkpoint.budget.stepAttempts, 1, control.name);
      assert.equal(store.listEvents({ workflowId }).filter(event =>
        event.type === 'private_local_step_admitted').length, 1, control.name);
      assert.equal(store.listEvents({ workflowId }).filter(event =>
        event.type === 'private_local_step_completed').length, 1, control.name);
      assert.deepEqual(localCompletionExternalCustody(temporary.databasePath, workflowId), custodyBefore,
        control.name + ' preserves an empty external custody state');
    });
  }
});

test('OPS-backed admission recovery reconciles one committed debit and preserves reservations across reopen', async t => {
  const temporary = temporaryDatabase('bounded-local-admission-ops-accounting-');
  const store = openStore(temporary.databasePath, Date.now, 20);
  let reopened = null;
  const blockers = [];
  t.after(async () => {
    await Promise.all(blockers.map(blocker => blocker.cleanup()));
    reopened?.close();
    store.close();
    temporary.cleanup();
  });
  const accepted = admitAndClaimLocalOperationsPlan(store, 'admission-ops-accounting');
  const lease = {
    workflowId: accepted.claim.workflowId,
    ownerId: accepted.claim.ownerId,
    fence: accepted.claim.fence
  };
  const checkpointSequenceBefore = store.getCheckpoint(lease.workflowId).sequence;
  const before = localCompletionAccounting(temporary.databasePath, lease.workflowId);
  const custodyBefore = localCompletionExternalCustody(temporary.databasePath, lease.workflowId);
  const databaseBefore = snapshotDatabase(temporary.databasePath);
  assert.equal(before.operationsWorkRows, 1);
  assert.deepEqual(custodyBefore, {
    operationAttempts: 0, authorityHoldRefs: 0, transportAttempts: 0, responseReceipts: 0
  });

  const originalAdmit = store._localControllerAdmit.bind(store);
  const originalResolve = store._localControllerResolveAdmission.bind(store);
  let firstBusy = null;
  let admissionCalls = 0;
  let transactionCalls = 0;
  let resolverCalls = 0;
  let accountingAfterRollback = null;
  let databaseAfterRollback = null;
  let accountingAfterAdmissionCommit = null;
  let accountingBeforeAdmissionResolution = null;
  let accountingAfterAdmissionResolution = null;
  let expectedAdmissionDiskDebit = null;
  let committedAdmission = null;
  let resolvedAdmission = null;
  store._localControllerAdmit = (...args) => {
    admissionCalls += 1;
    expectedAdmissionDiskDebit = expectedLocalControllerWriteDiskDebit(args[1].controllerState);
    const blocker = sqliteWriterBlockerSync(temporary.databasePath);
    blockers.push(blocker);
    try {
      transactionCalls += 1;
      return originalAdmit(...args);
    } catch (error) {
      blocker.releaseAndWait();
      if (admissionCalls === 1 && error instanceof RuntimeError
          && error.operation === '_localControllerAdmit' && error.code === 'STORE_BUSY') {
        firstBusy = error;
        accountingAfterRollback = localCompletionAccounting(temporary.databasePath, lease.workflowId);
        databaseAfterRollback = snapshotDatabase(temporary.databasePath);
        transactionCalls += 1;
        committedAdmission = originalAdmit(...args);
        accountingAfterAdmissionCommit = localCompletionAccounting(temporary.databasePath, lease.workflowId);
      }
      throw error;
    }
  };
  store._localControllerResolveAdmission = (...args) => {
    resolverCalls += 1;
    accountingBeforeAdmissionResolution = localCompletionAccounting(temporary.databasePath, lease.workflowId);
    resolvedAdmission = originalResolve(...args);
    accountingAfterAdmissionResolution = localCompletionAccounting(temporary.databasePath, lease.workflowId);
    return resolvedAdmission;
  };

  const result = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    planJson: accepted.work.work.planJson,
    limitsJson: accepted.work.work.limitsJson,
    ...localAdmissionRetryBudget()
  });
  await Promise.all(blockers.map(blocker => blocker.exited));
  assert.ok(firstBusy instanceof RuntimeError);
  assert.equal(firstBusy.operation, '_localControllerAdmit');
  assert.equal(firstBusy.code, 'STORE_BUSY');
  assert.equal(consumeLocalAdmissionBusyAfterConfirmedInactive(firstBusy), false,
    'the controller consumes the one-shot confirmed-inactive marker');
  assert.deepEqual(accountingAfterRollback, before,
    'the confirmed rollback leaves shared and per-job budgets intact');
  assert.deepEqual(databaseAfterRollback, databaseBefore,
    'a busy admission does not leave partial workflow or OPS writes');
  assert.equal(before.reservedDiskBytes - accountingAfterAdmissionCommit.reservedDiskBytes,
    expectedAdmissionDiskDebit,
    'one accepted local checkpoint consumes exactly one shared disk reservation charge');
  assert.equal(before.operationsWorkReservedDiskBytes - accountingAfterAdmissionCommit.operationsWorkReservedDiskBytes,
    expectedAdmissionDiskDebit,
    'the selected OPS job consumes the same single admission charge');
  assert.equal(before.reservedEventSlots - accountingAfterAdmissionCommit.reservedEventSlots, 1);
  assert.equal(before.operationsWorkReservedEventSlots - accountingAfterAdmissionCommit.operationsWorkReservedEventSlots, 1);
  assert.equal(accountingAfterAdmissionCommit.workflowEvents - before.workflowEvents, 1);
  assert.equal(accountingAfterAdmissionCommit.workflowCheckpoints - before.workflowCheckpoints, 1);
  assert.equal(accountingAfterAdmissionCommit.workflowOutboxRows - before.workflowOutboxRows, 1);
  assert.deepEqual(accountingBeforeAdmissionResolution, accountingAfterAdmissionCommit,
    'the resolver starts after the injected admission commit');
  assert.deepEqual(accountingAfterAdmissionResolution, accountingBeforeAdmissionResolution,
    'resolving the already committed admission is read-only and cannot debit a second charge');
  assert.equal(result.status, 'completed');
  assert.deepEqual(result.results.map(record => record.output), [9]);
  assert.equal(admissionCalls, 1, 'the controller reconciles the committed attempt without another admission call');
  assert.equal(transactionCalls, 2, 'one busy rollback and one committed admission');
  assert.equal(resolverCalls, 1);
  assert.equal(committedAdmission?.admissionKind, 'step');
  assert.equal(resolvedAdmission?.kind, 'committed');
  assert.equal(resolvedAdmission?.eventSequence, committedAdmission?.eventSequence);
  assert.equal(result.stoppingFacts.stepsAdmitted, 1);
  assert.equal(result.stoppingFacts.stepAttempts, 1);
  assert.equal(result.stoppingFacts.localRetries, 0);

  assert.equal(accountingAfterAdmissionCommit.workflowEvents, before.workflowEvents + 1);
  assert.equal(accountingAfterAdmissionCommit.workflowCheckpoints, before.workflowCheckpoints + 1);
  assert.equal(accountingAfterAdmissionCommit.workflowOutboxRows, before.workflowOutboxRows + 1);
  assert.equal(accountingAfterAdmissionCommit.reservedEventSlots, before.reservedEventSlots - 1);
  assert.equal(accountingAfterAdmissionCommit.operationsWorkReservedEventSlots,
    before.operationsWorkReservedEventSlots - 1);
  const diskDebit = before.reservedDiskBytes - accountingAfterAdmissionCommit.reservedDiskBytes;
  assert.ok(diskDebit > 0, 'the single new workflow write consumes its existing byte reservation');
  assert.equal(before.operationsWorkReservedDiskBytes - accountingAfterAdmissionCommit.operationsWorkReservedDiskBytes, diskDebit,
    'global and selected-job byte ledgers agree on the single debit');

  const afterCompletion = localCompletionAccounting(temporary.databasePath, lease.workflowId);
  const expectedCompletionDiskDebit = expectedLocalControllerWriteDiskDebit(
    store.getCheckpoint(lease.workflowId).data
  );
  assert.equal(afterCompletion.workflowEvents, accountingAfterAdmissionCommit.workflowEvents + 1);
  assert.equal(afterCompletion.workflowCheckpoints, accountingAfterAdmissionCommit.workflowCheckpoints + 1);
  assert.equal(afterCompletion.workflowOutboxRows, accountingAfterAdmissionCommit.workflowOutboxRows + 1);
  assert.equal(afterCompletion.reservedEventSlots, accountingAfterAdmissionCommit.reservedEventSlots - 1);
  assert.equal(afterCompletion.operationsWorkReservedEventSlots,
    accountingAfterAdmissionCommit.operationsWorkReservedEventSlots - 1);
  const completionDiskDebit = accountingAfterAdmissionCommit.reservedDiskBytes - afterCompletion.reservedDiskBytes;
  assert.equal(completionDiskDebit, expectedCompletionDiskDebit,
    'the completed local result consumes exactly one computed checkpoint-write charge');
  assert.equal(accountingAfterAdmissionCommit.operationsWorkReservedDiskBytes
    - afterCompletion.operationsWorkReservedDiskBytes, completionDiskDebit);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_admitted').length, 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_completed').length, 1);
  assert.deepEqual(localCompletionExternalCustody(temporary.databasePath, lease.workflowId), custodyBefore,
    'local admission and completion do not create external attempt or hold custody');
  const committedSnapshot = snapshotDatabase(temporary.databasePath);
  const completedResultsJson = store.getCheckpoint(lease.workflowId).data.completedResultsJson;
  store.close();
  reopened = openStore(temporary.databasePath, Date.now, 20);
  assert.deepEqual(snapshotDatabase(temporary.databasePath), committedSnapshot);
  assert.deepEqual(localCompletionAccounting(temporary.databasePath, lease.workflowId), afterCompletion);
  assert.deepEqual(localCompletionExternalCustody(temporary.databasePath, lease.workflowId), custodyBefore);
  assert.equal(reopened.getCheckpoint(lease.workflowId).sequence, checkpointSequenceBefore + 2);
  assert.equal(reopened.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_admitted').length, 1);
  assert.equal(reopened.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_completed').length, 1);
  assert.equal(reopened.getCheckpoint(lease.workflowId).data.completedResultsJson, completedResultsJson);
});

test('local completion retries only a confirmed SQLite busy after one exact pending reconciliation', async t => {
  const temporary = temporaryDatabase('bounded-local-completion-busy-retry-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, Date.now, 20);
  t.after(() => store.close());
  const accepted = admitAndClaimLocalOperationsPlan(store, 'completion-busy-retry');
  const lease = {
    workflowId: accepted.claim.workflowId,
    ownerId: accepted.claim.ownerId,
    fence: accepted.claim.fence
  };
  const reservationBeforeRun = localCompletionAccounting(temporary.databasePath, lease.workflowId);
  const custodyBeforeRun = localCompletionExternalCustody(temporary.databasePath, lease.workflowId);
  assert.equal(reservationBeforeRun.operationsWorkRows, 1);
  assert.ok(reservationBeforeRun.operationsWorkReservedDiskBytes > 0);
  assert.ok(reservationBeforeRun.operationsWorkReservedEventSlots > 0);
  assert.deepEqual(custodyBeforeRun, {
    operationAttempts: 0, authorityHoldRefs: 0, transportAttempts: 0, responseReceipts: 0
  });
  makeAdmission(store, lease, accepted.work.work.planJson, accepted.work.work.limitsJson,
    'completion-busy-retry:admit');

  const blockers = [];
  t.after(async () => Promise.all(blockers.map(blocker => blocker.cleanup())));

  const originalComplete = store._localControllerComplete.bind(store);
  const originalResolve = store._localControllerResolveCompletion.bind(store);
  let completeCalls = 0;
  let resolverCalls = 0;
  let firstBusy = null;
  const attempted = [];
  let accountingBefore = null;
  let accountingAfterRollback = null;
  let accountingCommitted = null;
  let expectedDiskDebit = null;
  store._localControllerComplete = (...args) => {
    completeCalls += 1;
    attempted.push(args[1]);
    expectedDiskDebit = expectedLocalControllerWriteDiskDebit(args[1].controllerState);
    const blocker = completeCalls === 1 ? sqliteWriterBlockerSync(temporary.databasePath) : null;
    if (blocker) blockers.push(blocker);
    if (completeCalls === 1) accountingBefore = localCompletionAccounting(temporary.databasePath, lease.workflowId);
    try {
      const completion = originalComplete(...args);
      accountingCommitted = localCompletionAccounting(temporary.databasePath, lease.workflowId);
      return completion;
    } catch (error) {
      if (completeCalls === 1) {
        firstBusy = error;
        blocker.releaseAndWait();
        accountingAfterRollback = localCompletionAccounting(temporary.databasePath, lease.workflowId);
      }
      throw error;
    }
  };
  store._localControllerResolveCompletion = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  const attemptsBefore = store.getCheckpoint(lease.workflowId).data.budget.stepAttempts;
  const retriesBefore = store.getCheckpoint(lease.workflowId).data.budget.localRetries;
  const result = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    planJson: accepted.work.work.planJson,
    limitsJson: accepted.work.work.limitsJson,
    ...localCompletionRetryBudget(20, 5000)
  });
  assert.equal(firstBusy instanceof RuntimeError, true);
  assert.equal(firstBusy.module, 'state');
  assert.equal(firstBusy.operation, '_localControllerComplete');
  assert.equal(firstBusy.code, 'STORE_BUSY');
  assert.equal(firstBusy.retryable, true);
  assert.equal(result.status, 'completed');
  assert.equal(completeCalls, 2);
  assert.equal(resolverCalls, 1);
  assert.equal(attempted[0].eventId, attempted[1].eventId);
  assert.equal(attempted[0].controllerState.completedResultsJson, attempted[1].controllerState.completedResultsJson);
  assert.equal(result.stoppingFacts.stepAttempts, attemptsBefore + 1);
  assert.equal(result.stoppingFacts.localRetries, retriesBefore + 1);
  assert.equal(store.getCheckpoint(lease.workflowId).data.budget.stepAttempts, attemptsBefore + 1);
  assert.equal(accountingCommitted.workflowEvents, accountingBefore.workflowEvents + 1);
  assert.equal(accountingCommitted.workflowCheckpoints, accountingBefore.workflowCheckpoints + 1);
  assert.equal(accountingCommitted.workflowOutboxRows, accountingBefore.workflowOutboxRows + 1);
  assert.ok(accountingBefore.operationsWorkReservedDiskBytes > 0);
  assert.deepEqual(accountingAfterRollback, accountingBefore);
  assert.equal(accountingCommitted.operationsWorkReservedDiskBytes,
    accountingBefore.operationsWorkReservedDiskBytes - expectedDiskDebit);
  assert.equal(accountingCommitted.reservedDiskBytes, accountingBefore.reservedDiskBytes - expectedDiskDebit);
  assert.equal(accountingCommitted.reservedEventSlots, accountingBefore.reservedEventSlots - 1);
  assert.equal(accountingCommitted.operationsWorkReservedEventSlots,
    accountingBefore.operationsWorkReservedEventSlots - 1);
  assert.deepEqual(localCompletionAccounting(temporary.databasePath, lease.workflowId), accountingCommitted);
  assert.deepEqual(localCompletionExternalCustody(temporary.databasePath, lease.workflowId), custodyBeforeRun);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_completed').length, 1);

  const reopened = DurableWorkflowStore.open({ databasePath: temporary.databasePath, busyTimeoutMs: 20 });
  try {
    assert.equal(reopened.getCheckpoint(lease.workflowId).data.pendingStepId, null);
    assert.equal(reopened.listEvents({ workflowId: lease.workflowId })
      .filter(event => event.type === 'private_local_step_completed').length, 1);
    assert.deepEqual(localCompletionAccounting(temporary.databasePath, lease.workflowId), accountingCommitted);
    assert.deepEqual(localCompletionExternalCustody(temporary.databasePath, lease.workflowId), custodyBeforeRun);
  } finally {
    reopened.close();
  }
});

test('completion recovery stops after three exact busy calls without changing the admitted result', async t => {
  const temporary = temporaryDatabase('bounded-local-completion-three-busy-calls-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, Date.now, 20);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'completion-three-busy-calls');
  makeAdmission(store, lease, SINGLE_PLAN, '{}', 'completion-three-busy-calls:admit');
  const originalComplete = store._localControllerComplete.bind(store);
  const originalResolve = store._localControllerResolveCompletion.bind(store);
  let completeCalls = 0;
  let resolverCalls = 0;
  const attempted = [];
  const blockers = [];
  t.after(async () => {
    await Promise.all(blockers.map(blocker => blocker.cleanup()));
  });
  store._localControllerComplete = (...args) => {
    completeCalls += 1;
    attempted.push(args[1]);
    const blocker = sqliteWriterBlockerSync(temporary.databasePath);
    blockers.push(blocker);
    try {
      return originalComplete(...args);
    } catch (error) {
      blocker.releaseAndWait();
      throw error;
    }
  };
  store._localControllerResolveCompletion = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  const attemptsBefore = store.getCheckpoint(lease.workflowId).data.budget.stepAttempts;
  const retriesBefore = store.getCheckpoint(lease.workflowId).data.budget.localRetries;
  const result = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    ...localCompletionRetryBudget(20, 5000)
  });
  await Promise.all(blockers.map(blocker => blocker.exited));
  assert.equal(result.status, 'stopped');
  assert.equal(result.reason, 'LOCAL_COMPLETION_BUSY');
  assert.equal(completeCalls, 3);
  assert.equal(resolverCalls, 3);
  assert.equal(new Set(attempted.map(item => item.eventId)).size, 1);
  const resultBytes = attempted[0].controllerState.completedResultsJson;
  assert.ok(attempted.every(item => item.controllerState.completedResultsJson === resultBytes));
  assert.equal(store.getCheckpoint(lease.workflowId).data.pendingStepId, 'only');
  assert.equal(store.getCheckpoint(lease.workflowId).data.budget.stepAttempts, attemptsBefore + 1);
  assert.equal(store.getCheckpoint(lease.workflowId).data.budget.localRetries, retriesBefore + 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .some(event => event.type === 'private_local_step_completed'), false);
});

test('local completion accepts a committed exact attempt after lost acknowledgment and outbox delivery updates', async t => {
  const temporary = temporaryDatabase('bounded-local-completion-lost-ack-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, Date.now, 20);
  t.after(() => store.close());
  const accepted = admitAndClaimLocalOperationsPlan(store, 'completion-lost-ack');
  const lease = {
    workflowId: accepted.claim.workflowId,
    ownerId: accepted.claim.ownerId,
    fence: accepted.claim.fence
  };
  const reservationBeforeRun = localCompletionAccounting(temporary.databasePath, lease.workflowId);
  const custodyBeforeRun = localCompletionExternalCustody(temporary.databasePath, lease.workflowId);
  assert.ok(reservationBeforeRun.operationsWorkReservedDiskBytes > 0);
  assert.ok(reservationBeforeRun.operationsWorkReservedEventSlots > 0);
  assert.deepEqual(custodyBeforeRun, {
    operationAttempts: 0, authorityHoldRefs: 0, transportAttempts: 0, responseReceipts: 0
  });
  makeAdmission(store, lease, accepted.work.work.planJson, accepted.work.work.limitsJson,
    'completion-lost-ack:admit');
  const blockers = [];
  t.after(async () => Promise.all(blockers.map(blocker => blocker.cleanup())));
  const originalComplete = store._localControllerComplete.bind(store);
  let firstBusy = null;
  let completeCalls = 0;
  let resolverCalls = 0;
  let accountingBefore = null;
  let accountingAfterRollback = null;
  let accountingCommitted = null;
  let expectedDiskDebit = null;
  store._localControllerComplete = (capability, input) => {
    completeCalls += 1;
    expectedDiskDebit = expectedLocalControllerWriteDiskDebit(input.controllerState);
    const blocker = completeCalls === 1 ? sqliteWriterBlockerSync(temporary.databasePath) : null;
    if (blocker) blockers.push(blocker);
    if (completeCalls === 1) accountingBefore = localCompletionAccounting(temporary.databasePath, lease.workflowId);
    try {
      return originalComplete(capability, input);
    } catch (error) {
      firstBusy = error;
      blocker.releaseAndWait();
      accountingAfterRollback = localCompletionAccounting(temporary.databasePath, lease.workflowId);
      originalComplete(capability, input);
      accountingCommitted = localCompletionAccounting(temporary.databasePath, lease.workflowId);
      const delivery = new DatabaseSync(temporary.databasePath);
      try {
        delivery.prepare('UPDATE outbox SET delivered_at_ms = ?, delivery_attempts = 1 WHERE event_id = ?')
          .run(Date.now(), input.eventId);
      } finally {
        delivery.close();
      }
      throw error;
    }
  };
  const originalResolve = store._localControllerResolveCompletion.bind(store);
  store._localControllerResolveCompletion = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  const sequenceBefore = store.getWorkflow(lease.workflowId).checkpointSequence;
  const attemptsBefore = store.getCheckpoint(lease.workflowId).data.budget.stepAttempts;
  const result = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    planJson: accepted.work.work.planJson,
    limitsJson: accepted.work.work.limitsJson,
    ...localCompletionRetryBudget()
  });
  assert.equal(firstBusy instanceof RuntimeError, true);
  assert.equal(result.status, 'completed');
  assert.equal(completeCalls, 1);
  assert.equal(resolverCalls, 1);
  assert.equal(store.getWorkflow(lease.workflowId).checkpointSequence, sequenceBefore + 2);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_completed').length, 1);
  const completionEvent = store.listEvents({ workflowId: lease.workflowId })
    .find(event => event.type === 'private_local_step_completed');
  const delivery = new DatabaseSync(temporary.databasePath);
  try {
    const outboxRow = delivery.prepare(
      'SELECT event_id, event_sequence, delivered_at_ms, delivery_attempts FROM outbox WHERE event_id = ?'
    ).get(completionEvent.eventId);
    assert.equal(outboxRow.event_id, completionEvent.eventId);
    assert.equal(Number(outboxRow.event_sequence), completionEvent.sequence);
    assert.equal(Number(outboxRow.delivery_attempts), 1);
    assert.equal(typeof outboxRow.delivered_at_ms, 'number');
  } finally {
    delivery.close();
  }
  assert.equal(accountingCommitted.workflowEvents, accountingBefore.workflowEvents + 1);
  assert.equal(accountingCommitted.workflowCheckpoints, accountingBefore.workflowCheckpoints + 1);
  assert.equal(accountingCommitted.workflowOutboxRows, accountingBefore.workflowOutboxRows + 1);
  assert.ok(accountingBefore.operationsWorkReservedDiskBytes > 0);
  assert.deepEqual(accountingAfterRollback, accountingBefore);
  assert.equal(accountingCommitted.operationsWorkReservedDiskBytes,
    accountingBefore.operationsWorkReservedDiskBytes - expectedDiskDebit);
  assert.equal(accountingCommitted.reservedDiskBytes, accountingBefore.reservedDiskBytes - expectedDiskDebit);
  assert.equal(accountingCommitted.reservedEventSlots, accountingBefore.reservedEventSlots - 1);
  assert.equal(accountingCommitted.operationsWorkReservedEventSlots,
    accountingBefore.operationsWorkReservedEventSlots - 1);
  assert.deepEqual(localCompletionAccounting(temporary.databasePath, lease.workflowId), accountingCommitted);
  assert.deepEqual(localCompletionExternalCustody(temporary.databasePath, lease.workflowId), custodyBeforeRun);

  const reopened = DurableWorkflowStore.open({ databasePath: temporary.databasePath, busyTimeoutMs: 20 });
  try {
    assert.equal(reopened.getCheckpoint(lease.workflowId).data.pendingStepId, null);
    assert.equal(reopened.listEvents({ workflowId: lease.workflowId })
      .filter(event => event.type === 'private_local_step_completed').length, 1);
    assert.deepEqual(localCompletionAccounting(temporary.databasePath, lease.workflowId), accountingCommitted);
    assert.deepEqual(localCompletionExternalCustody(temporary.databasePath, lease.workflowId), custodyBeforeRun);
  } finally {
    reopened.close();
  }
});

test('recovered completion rechecks cancellation before admitting the next local step', async t => {
  const temporary = temporaryDatabase('bounded-local-completion-recovered-cancel-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, Date.now, 20);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'completion-recovered-cancel');
  makeAdmission(store, lease, DEPENDENT_PLAN, '{}', 'completion-recovered-cancel:admit');
  const blockers = [];
  t.after(async () => Promise.all(blockers.map(blocker => blocker.cleanup())));
  const originalComplete = store._localControllerComplete.bind(store);
  const originalResolve = store._localControllerResolveCompletion.bind(store);
  let completeCalls = 0;
  let resolverCalls = 0;
  store._localControllerComplete = (capability, input) => {
    completeCalls += 1;
    const blocker = completeCalls === 1 ? sqliteWriterBlockerSync(temporary.databasePath) : null;
    if (blocker) blockers.push(blocker);
    try {
      return originalComplete(capability, input);
    } catch (error) {
      blocker.releaseAndWait();
      originalComplete(capability, input);
      const workflow = store.getWorkflow(lease.workflowId);
      const cancellation = store.requestCancellation({
        ...lease,
        expectedRevision: workflow.revision,
        eventId: 'completion-recovered-cancel:request'
      });
      assert.equal(cancellation.accepted, true);
      throw error;
    }
  };
  store._localControllerResolveCompletion = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  const attemptsBefore = store.getCheckpoint(lease.workflowId).data.budget.stepAttempts;
  const retriesBefore = store.getCheckpoint(lease.workflowId).data.budget.localRetries;
  const result = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    planJson: DEPENDENT_PLAN,
    ...localCompletionRetryBudget()
  });
  assert.equal(result.status, 'stopped');
  assert.equal(result.reason, 'WORKFLOW_TERMINAL');
  assert.equal(completeCalls, 1);
  assert.equal(resolverCalls, 1);
  const checkpoint = store.getCheckpoint(lease.workflowId).data;
  assert.equal(checkpoint.pendingStepId, null);
  assert.deepEqual(Object.keys(JSON.parse(checkpoint.completedResultsJson)), ['first']);
  assert.equal(checkpoint.budget.stepsAdmitted, 1);
  assert.equal(checkpoint.budget.stepAttempts, attemptsBefore + 1);
  assert.equal(checkpoint.budget.localRetries, retriesBefore + 1);
  const events = store.listEvents({ workflowId: lease.workflowId });
  assert.equal(events.filter(event => event.type === 'private_local_step_completed').length, 1);
  assert.equal(events.filter(event => event.type === 'private_local_step_admitted').length, 1);
  assert.equal(store.getWorkflow(lease.workflowId).cancellationRequested, true);
});

test('rollback-origin busy and unknown catch-time transaction state are never completion-retry eligible', () => {
  const makeDriver = ({ rollbackBusy = false, unknownCatchState = false } = {}) => {
    let active = false;
    let transactionReads = 0;
    return {
      get isTransaction() {
        transactionReads += 1;
        if (unknownCatchState && transactionReads === 2) throw new Error('synthetic unknown transaction state');
        return active;
      },
      exec(sql) {
        if (sql === 'BEGIN IMMEDIATE') {
          active = true;
          return;
        }
        if (sql === 'ROLLBACK') {
          active = false;
          if (rollbackBusy) throw syntheticSqliteBusy();
          return;
        }
        throw new Error('unexpected synthetic transaction SQL');
      },
      finishForUnknownDisposition() { active = false; }
    };
  };
  for (const control of [
    { label: 'rollback-origin busy', options: { rollbackBusy: true } },
    { label: 'unknown catch-time state followed by later false', options: { unknownCatchState: true } }
  ]) {
    const db = makeDriver(control.options);
    let caught = null;
    try {
      inLocalCompletionTransaction(db, '_localControllerComplete', () => {
        if (control.options.unknownCatchState) db.finishForUnknownDisposition();
        throw syntheticSqliteBusy();
      });
    } catch (error) {
      caught = error;
    }
    assert.ok(caught instanceof RuntimeError, control.label);
    assert.equal(caught.operation, '_localControllerComplete', control.label);
    assert.equal(caught.code, 'STORE_BUSY', control.label);
    assert.equal(consumeLocalCompletionBusyAfterConfirmedInactive(caught), false, control.label);
  }
});

test('completion recovery eligibility requires an unconsumed raw primary SQLite BUSY error', () => {
  const makeDriver = () => {
    let active = false;
    return {
      get isTransaction() { return active; },
      exec(sql) {
        if (sql === 'BEGIN IMMEDIATE') { active = true; return; }
        if (sql === 'ROLLBACK') { active = false; return; }
        throw new Error('unexpected synthetic transaction SQL');
      }
    };
  };
  const mappedFromRawPrimary = () => {
    let caught;
    try {
      inLocalCompletionTransaction(makeDriver(), '_localControllerComplete', () => {
        throw syntheticSqliteBusy();
      });
    } catch (error) {
      caught = error;
    }
    assert.ok(caught instanceof RuntimeError);
    assert.equal(caught.code, 'STORE_BUSY');
    return caught;
  };
  const mapped = mappedFromRawPrimary();
  assert.equal(consumeLocalCompletionBusyAfterConfirmedInactive(mapped), true);
  assert.equal(consumeLocalCompletionBusyAfterConfirmedInactive(mapped), false);

  const consumedAndReused = mappedFromRawPrimary();
  assert.equal(consumeLocalCompletionBusyAfterConfirmedInactive(consumedAndReused), true);
  let consumedReuse;
  try {
    inLocalCompletionTransaction(makeDriver(), '_localControllerComplete', () => {
      throw consumedAndReused;
    });
  } catch (error) {
    consumedReuse = error;
  }
  assert.equal(consumedReuse, consumedAndReused);
  assert.equal(consumeLocalCompletionBusyAfterConfirmedInactive(consumedReuse), false,
    'a consumed mapped RuntimeError cannot be minted as raw SQLite provenance again');

  const unconsumedMapped = mappedFromRawPrimary();
  let rethrown;
  try {
    inLocalCompletionTransaction(makeDriver(), '_localControllerComplete', () => {
      throw unconsumedMapped;
    });
  } catch (error) {
    rethrown = error;
  }
  assert.equal(rethrown, unconsumedMapped);
  assert.equal(consumeLocalCompletionBusyAfterConfirmedInactive(rethrown), false,
    'a mapped error already marked by one transaction cannot be marked again');

  let nestedError;
  try {
    inLocalCompletionTransaction(makeDriver(), '_localControllerComplete', () => {
      try {
        inLocalCompletionTransaction(makeDriver(), '_localControllerComplete', () => {
          throw syntheticSqliteBusy();
        });
      } catch (error) {
        nestedError = error;
        throw error;
      }
    });
  } catch (error) {
    assert.equal(error, nestedError);
  }
  assert.equal(consumeLocalCompletionBusyAfterConfirmedInactive(nestedError), false,
    'an inner transaction marker cannot be laundered through an outer transaction');

  for (const phase of ['begin', 'body', 'commit']) {
    let active = false;
    let callbackCalls = 0;
    const db = {
      get isTransaction() { return active; },
      exec(sql) {
        if (sql === 'BEGIN IMMEDIATE') {
          if (phase === 'begin') throw syntheticSqliteBusy();
          active = true;
          return;
        }
        if (sql === 'COMMIT') {
          if (phase === 'commit') throw syntheticSqliteBusy();
          active = false;
          return;
        }
        if (sql === 'ROLLBACK') {
          active = false;
          return;
        }
        throw new Error('unexpected synthetic transaction SQL');
      }
    };
    let caught;
    try {
      inLocalCompletionTransaction(db, '_localControllerComplete', () => {
        callbackCalls += 1;
        if (phase === 'body') throw syntheticSqliteBusy();
        return 'synthetic completion';
      });
    } catch (error) {
      caught = error;
    }
    assert.ok(caught instanceof RuntimeError, phase);
    assert.equal(caught.code, 'STORE_BUSY', phase);
    assert.equal(callbackCalls, phase === 'begin' ? 0 : 1, phase);
    assert.equal(consumeLocalCompletionBusyAfterConfirmedInactive(caught), true, phase);
  }

  const nonEligible = [
    { error: syntheticSqliteBusy({ code: 'BUSY', errcode: undefined }), mappedCode: 'STORE_BUSY' },
    { error: syntheticSqliteBusy({ code: 'LOCKED', errcode: undefined }), mappedCode: 'STORE_BUSY' },
    { error: syntheticSqliteBusy({ code: 'SQLITE_BUSY', errcode: 5 }), mappedCode: 'STORE_BUSY' },
    { error: syntheticSqliteBusy({ code: 'SQLITE_LOCKED', errcode: undefined }), mappedCode: 'STORE_BUSY' },
    { error: syntheticSqliteBusy({ code: 'ERR_SQLITE_ERROR', errcode: 6 }), mappedCode: 'STORE_BUSY' },
    { error: syntheticSqliteBusy({ code: 'ERR_SQLITE_ERROR', errcode: undefined }), mappedCode: 'STORE_WRITE_FAILED' },
    { error: localCompletionBusy(), mappedCode: 'STORE_BUSY' }
  ];
  for (const control of nonEligible) {
    let caught;
    try {
      inLocalCompletionTransaction(makeDriver(), '_localControllerComplete', () => { throw control.error; });
    } catch (error) {
      caught = error;
    }
    assert.ok(caught instanceof RuntimeError);
    assert.equal(caught.code, control.mappedCode);
    assert.equal(consumeLocalCompletionBusyAfterConfirmedInactive(caught), false);
  }
});

test('OPS07 fd3 capture and parser preserve private completion and admission resolution phases', () => {
  const temporary = temporaryDatabase('bounded-local-completion-diagnostics-');
  const directory = join(temporary.directory, 'diagnostics');
  mkdirSync(directory, { mode: 0o700 });
  const session = createOperationsDiagnosticSession({ directory });
  const diagnosticsUrl = new URL('../../src/internal/private-ops-diagnostics.js', import.meta.url).href;
  try {
    for (const slot of [0, 1]) {
      const descriptor = prepareOperationsChildCapture(session);
      assert.equal(descriptor?.slot, slot);
      const source = `
        const diagnostics = await import(${JSON.stringify(diagnosticsUrl)});
        if (!diagnostics.enableOperationsRunnerCaptureFromFd3()) process.exit(2);
        if (${slot} === 0) {
          const failure = new Error('private message must not enter the capture');
          failure.code = 'ERR_SQLITE_ERROR';
          failure.errcode = 5;
          if (!diagnostics.recordOperationsDiagnosticError('controller_error', failure, {
            phase: 'completion_resolution', checkpointSequence: 7
          })) process.exit(3);
          if (!diagnostics.recordOperationsDiagnosticResult({
            status: 'stopped', reason: 'LOCAL_COMPLETION_BUSY', checkpointSequence: 7
          })) process.exit(4);
          if (!diagnostics.recordOperationsDiagnosticResult({
            status: 'stopped', reason: 'LOCAL_COMPLETION_OUTCOME_UNKNOWN', checkpointSequence: 7
          })) process.exit(5);
        } else {
          const failure = new Error('private message must not enter the capture');
          failure.code = 'ERR_SQLITE_ERROR';
          failure.errcode = 5;
          if (!diagnostics.recordOperationsDiagnosticError('controller_error', failure, {
            phase: 'admission_resolution', checkpointSequence: 7
          })) process.exit(3);
          if (!diagnostics.recordOperationsDiagnosticError('storage_origin', failure, {
            operation: '_localControllerResolveAdmission'
          })) process.exit(4);
          if (!diagnostics.recordOperationsDiagnosticResult({
            status: 'stopped', reason: 'LOCAL_ADMISSION_OUTCOME_UNKNOWN', checkpointSequence: 7
          })) process.exit(6);
          if (!diagnostics.recordOperationsDiagnosticResult({
            status: 'stopped', reason: 'LOCAL_ADMISSION_BUSY', checkpointSequence: 7
          })) process.exit(7);
        }
        if (!diagnostics.finishOperationsRunnerCapture()) process.exit(8);
      `;
      const child = spawnSync(process.execPath, ['--input-type=module', '--eval', source], {
        cwd: process.cwd(),
        encoding: 'utf8',
        timeout: 5_000,
        maxBuffer: 8_192,
        stdio: ['ignore', 'pipe', 'pipe', descriptor.fd]
      });
      assert.equal(child.error, undefined, child.stderr);
      assert.equal(child.status, 0, child.stderr);
      assert.equal(closeOperationsChildCapture(session, slot), true);
      const generation = slot + 1;
      assert.equal(recordOperationsSupervisor(session, slot, 'spawn', {
        pidPresent: true, generation, fence: generation
      }), true);
      assert.equal(recordOperationsSupervisor(session, slot, 'child_exit', {
        exitCode: 0, signal: null, childRuntimeMs: 1, generation, fence: generation
      }), true);
      assert.equal(recordOperationsSupervisor(session, slot, 'finish_result', {
        outcome: 'completed', settled: true, code: null, generation, fence: generation
      }), true);
    }
    const job = generation => ({
      workState: 'running', workflowState: 'running', generation,
      operationFence: generation, workflowFence: generation,
      revision: 1, checkpointSequence: 7, resultCount: 0, resultHash: null,
      cancellationRequested: false, leaseOwnerPresent: true, leaseFenceMatches: true,
      leaseExpired: false, eventCounts: []
    });
    const capture = readOperationsDiagnosticSession(session, {
      finalState: {
        available: true,
        jobs: [job(1), job(2)],
        reservations: { reservedDiskBytes: 0, reservedEventSlots: 0, pendingRecords: 0 }
      },
      abruptSlots: []
    });
    assert.equal(capture.flags.complete, true);
    const parsed = parseOperationsCaptureFromTap(`${OPS07_CAPTURE_LINE_PREFIX}${JSON.stringify(capture)}\n`);
    assert.equal(parsed.valid, true, parsed.reason);
    assert.equal(parsed.capture.slots[0].records[1].stage, 'controller_error');
    assert.equal(parsed.capture.slots[0].records[1].phase, 'completion_resolution');
    assert.equal(parsed.capture.slots[0].records[1].primaryCode, 5);
    assert.equal(parsed.capture.slots[0].records[2].reason, 'LOCAL_COMPLETION_BUSY');
    assert.equal(parsed.capture.slots[0].records[3].reason, 'LOCAL_COMPLETION_OUTCOME_UNKNOWN');
    assert.equal(parsed.capture.slots[1].records[1].phase, 'admission_resolution');
    assert.equal(parsed.capture.slots[1].records[2].operation, '_localControllerResolveAdmission');
    assert.equal(parsed.capture.slots[1].records[3].reason, 'LOCAL_ADMISSION_OUTCOME_UNKNOWN');
    assert.equal(parsed.capture.slots[1].records[4].reason, 'LOCAL_ADMISSION_BUSY');
    assert.equal(JSON.stringify(parsed.capture).includes('private message must not enter'), false);

    const malformed = JSON.parse(JSON.stringify(capture));
    malformed.slots[0].records[1].phase = 'unrecognized_phase';
    const rejected = parseOperationsCaptureFromTap(
      `${OPS07_CAPTURE_LINE_PREFIX}${JSON.stringify(malformed)}\n`
    );
    assert.deepEqual(rejected, { valid: false, reason: 'invalid_shape', capture: null });
  } finally {
    disposeOperationsDiagnosticSession(session);
    temporary.cleanup();
  }
});

test('a changed lease fence before pending-outcome observation stays unknown without a second write', async t => {
  const temporary = temporaryDatabase('bounded-local-completion-fence-changed-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, Date.now, 20);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'completion-fence-changed');
  makeAdmission(store, lease, SINGLE_PLAN, '{}', 'completion-fence-changed:admit');
  const blockers = [];
  t.after(async () => Promise.all(blockers.map(blocker => blocker.cleanup())));
  const originalComplete = store._localControllerComplete.bind(store);
  const originalResolve = store._localControllerResolveCompletion.bind(store);
  let completeCalls = 0;
  let resolverCalls = 0;
  let replacementLease = null;
  let accountingAfterTakeover = null;
  store._localControllerComplete = (...args) => {
    completeCalls += 1;
    const blocker = sqliteWriterBlockerSync(temporary.databasePath);
    blockers.push(blocker);
    try {
      return originalComplete(...args);
    } catch (error) {
      blocker.releaseAndWait();
      store.releaseLease({
        workflowId: lease.workflowId,
        ownerId: lease.ownerId,
        fence: lease.fence,
        eventId: 'completion-fence-changed:release'
      });
      replacementLease = store.acquireLease({
        workflowId: lease.workflowId,
        ownerId: 'worker-replacement',
        ttlMs: 60_000,
        eventId: 'completion-fence-changed:takeover'
      });
      accountingAfterTakeover = localCompletionAccounting(temporary.databasePath, lease.workflowId);
      throw error;
    }
  };
  store._localControllerResolveCompletion = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  const attemptsBefore = store.getCheckpoint(lease.workflowId).data.budget.stepAttempts;
  const result = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    ...localCompletionRetryBudget()
  });
  assert.equal(result.status, 'stopped');
  assert.equal(result.reason, 'LOCAL_COMPLETION_OUTCOME_UNKNOWN');
  assert.equal(completeCalls, 1);
  assert.equal(resolverCalls, 1);
  assert.equal(replacementLease.ownerId, 'worker-replacement');
  assert.equal(replacementLease.fence, lease.fence + 1);
  assert.equal(store.getWorkflow(lease.workflowId).fence, replacementLease.fence);
  assert.equal(store.getCheckpoint(lease.workflowId).data.pendingStepId, 'only');
  assert.equal(store.getCheckpoint(lease.workflowId).data.budget.stepAttempts, attemptsBefore + 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .some(event => event.type === 'private_local_step_completed'), false);
  assert.deepEqual(localCompletionAccounting(temporary.databasePath, lease.workflowId), accountingAfterTakeover);
});

test('an unresolved external attempt before pending-outcome observation stays unknown', async t => {
  const temporary = temporaryDatabase('bounded-local-completion-external-unknown-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, Date.now, 20);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'completion-external-unknown');
  makeAdmission(store, lease, SINGLE_PLAN, '{}', 'completion-external-unknown:admit');
  const blockers = [];
  t.after(async () => Promise.all(blockers.map(blocker => blocker.cleanup())));
  const originalComplete = store._localControllerComplete.bind(store);
  const originalResolve = store._localControllerResolveCompletion.bind(store);
  let completeCalls = 0;
  let resolverCalls = 0;
  let intent = null;
  store._localControllerComplete = (...args) => {
    completeCalls += 1;
    const blocker = sqliteWriterBlockerSync(temporary.databasePath);
    blockers.push(blocker);
    try {
      return originalComplete(...args);
    } catch (error) {
      blocker.releaseAndWait();
      const workflow = store.getWorkflow(lease.workflowId);
      intent = store.recordDispatchIntent({
        workflowId: lease.workflowId,
        ownerId: lease.ownerId,
        fence: lease.fence,
        expectedRevision: workflow.revision,
        attemptId: 'ah11-local-completion-pending-attempt',
        logicalOperationRef: 'ah11-local-completion-pending-operation',
        authorityHoldRef: 'ah11-local-completion-pending-hold',
        transportAttemptId: 'ah11-local-completion-pending-transport',
        eventId: 'ah11-local-completion-pending-intent'
      });
      throw error;
    }
  };
  store._localControllerResolveCompletion = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  const attemptsBefore = store.getCheckpoint(lease.workflowId).data.budget.stepAttempts;
  const result = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    ...localCompletionRetryBudget()
  });
  assert.equal(result.status, 'stopped');
  assert.equal(result.reason, 'LOCAL_COMPLETION_OUTCOME_UNKNOWN');
  assert.equal(completeCalls, 1);
  assert.equal(resolverCalls, 1);
  assert.equal(intent.state, 'awaiting_response');
  assert.equal(store.getWorkflow(lease.workflowId).state, 'awaiting_response');
  assert.equal(store.getCheckpoint(lease.workflowId).data.pendingStepId, 'only');
  assert.equal(store.getCheckpoint(lease.workflowId).data.budget.stepAttempts, attemptsBefore + 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .some(event => event.type === 'private_local_step_completed'), false);
  const raw = new DatabaseSync(temporary.databasePath);
  try {
    assert.equal(Number(raw.prepare(`
      SELECT COUNT(*) AS count FROM operation_attempts
      WHERE workflow_id = ? AND state IN ('intent','outcome_unknown','response_received')
    `).get(lease.workflowId).count), 1);
    assert.equal(Number(raw.prepare('SELECT COUNT(*) AS count FROM response_receipts').get().count), 0);
  } finally {
    raw.close();
  }
});

test('pending completion rechecks cancellation, lease fence, and external custody before its next write', async t => {
  const controls = [
    {
      name: 'cancel', expectedStatus: 'stopped', expectedReason: 'WORKFLOW_TERMINAL'
    },
    {
      name: 'fence', expectedStatus: 'stopped', expectedReason: 'WORKFLOW_SNAPSHOT_CHANGED'
    },
    {
      name: 'external', expectedStatus: 'external-reconciliation-required',
      expectedReason: 'EXTERNAL_OUTCOME_UNRESOLVED'
    }
  ];
  for (const control of controls) {
    await t.test(`post-pending ${control.name} blocks the next local write`, async () => {
      const temporary = temporaryDatabase('bounded-local-completion-after-pending-' + control.name + '-');
      const store = openStore(temporary.databasePath, Date.now, 20);
      const workflowId = 'completion-after-pending-' + control.name;
      const lease = createRunningWorkflow(store, workflowId);
      makeAdmission(store, lease, SINGLE_PLAN, '{}', workflowId + ':admit');
      const checkpointBeforeRun = store.getCheckpoint(workflowId).data;
      const eventsBeforeRun = store.listEvents({ workflowId });
      const blockers = [];
      try {
        const originalComplete = store._localControllerComplete.bind(store);
        const originalResolve = store._localControllerResolveCompletion.bind(store);
        let completeCalls = 0;
        let resolverCalls = 0;
        let changedAfterPending = false;
        store._localControllerComplete = (...args) => {
          completeCalls += 1;
          const blocker = sqliteWriterBlockerSync(temporary.databasePath);
          blockers.push(blocker);
          try {
            return originalComplete(...args);
          } catch (error) {
            blocker.releaseAndWait();
            throw error;
          }
        };
        store._localControllerResolveCompletion = (...args) => {
          resolverCalls += 1;
          const resolution = originalResolve(...args);
          if (resolution.kind === 'pending' && !changedAfterPending) {
            changedAfterPending = true;
            const workflow = store.getWorkflow(workflowId);
            if (control.name === 'cancel') {
              const cancellation = store.requestCancellation({
                workflowId, ownerId: lease.ownerId, fence: lease.fence,
                expectedRevision: workflow.revision,
                eventId: workflowId + ':cancel-after-pending'
              });
              assert.equal(cancellation.accepted, true);
            } else if (control.name === 'fence') {
              store.releaseLease({
                workflowId, ownerId: lease.ownerId, fence: lease.fence,
                eventId: workflowId + ':release-after-pending'
              });
              store.acquireLease({
                workflowId, ownerId: 'replacement-after-pending', ttlMs: 60_000,
                eventId: workflowId + ':takeover-after-pending'
              });
            } else {
              store.recordDispatchIntent({
                workflowId, ownerId: lease.ownerId, fence: lease.fence,
                expectedRevision: workflow.revision,
                attemptId: workflowId + ':attempt',
                logicalOperationRef: workflowId + ':operation',
                authorityHoldRef: workflowId + ':hold',
                transportAttemptId: workflowId + ':transport',
                eventId: workflowId + ':intent'
              });
            }
          }
          return resolution;
        };

        const result = await runBoundedLocalPlan({
          store, workflowId, ownerId: lease.ownerId, fence: lease.fence,
          ...localCompletionRetryBudget()
        });
        assert.equal(changedAfterPending, true, control.name);
        assert.equal(result.status, control.expectedStatus, control.name);
        assert.equal(result.reason, control.expectedReason, control.name);
        assert.equal(completeCalls, 1, control.name);
        assert.equal(resolverCalls, 1, control.name);
        const checkpoint = store.getCheckpoint(workflowId).data;
        assert.equal(checkpoint.pendingStepId, 'only', control.name);
        assert.equal(checkpoint.budget.stepsAdmitted, checkpointBeforeRun.budget.stepsAdmitted, control.name);
        assert.equal(checkpoint.budget.stepAttempts, checkpointBeforeRun.budget.stepAttempts + 1, control.name);
        assert.equal(checkpoint.budget.localRetries, checkpointBeforeRun.budget.localRetries + 1, control.name);
        assert.equal(checkpoint.budget.iterations, checkpointBeforeRun.budget.iterations + 1, control.name);
        assert.equal(store.listEvents({ workflowId }).filter(event =>
          event.type === 'private_local_step_replayed').length, 1, control.name);
        assert.equal(store.listEvents({ workflowId }).filter(event =>
          event.type === 'private_local_step_admitted').length,
        eventsBeforeRun.filter(event => event.type === 'private_local_step_admitted').length, control.name);
        assert.equal(store.listEvents({ workflowId }).some(event =>
          event.type === 'private_local_step_completed'), false, control.name);
        if (control.name === 'cancel') {
          const cancelled = store.getWorkflow(workflowId);
          assert.equal(cancelled.state, 'cancelled');
          assert.equal(cancelled.cancellationRequested, true);
        }
      } finally {
        await Promise.all(blockers.map(blocker => blocker.cleanup()));
        store.close();
        temporary.cleanup();
      }
    });
  }
});

test('a mismatched partial completion commit is unknown and does not duplicate event or reservation accounting', async t => {
  const temporary = temporaryDatabase('bounded-local-completion-partial-commit-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, Date.now, 20);
  t.after(() => store.close());
  const accepted = admitAndClaimLocalOperationsPlan(store, 'completion-partial-commit');
  const lease = {
    workflowId: accepted.claim.workflowId,
    ownerId: accepted.claim.ownerId,
    fence: accepted.claim.fence
  };
  const reservationBeforeRun = localCompletionAccounting(temporary.databasePath, lease.workflowId);
  const custodyBeforeRun = localCompletionExternalCustody(temporary.databasePath, lease.workflowId);
  assert.ok(reservationBeforeRun.operationsWorkReservedDiskBytes > 0);
  assert.ok(reservationBeforeRun.operationsWorkReservedEventSlots > 0);
  assert.deepEqual(custodyBeforeRun, {
    operationAttempts: 0, authorityHoldRefs: 0, transportAttempts: 0, responseReceipts: 0
  });
  makeAdmission(store, lease, accepted.work.work.planJson, accepted.work.work.limitsJson,
    'completion-partial-commit:admit');
  const blockers = [];
  t.after(async () => Promise.all(blockers.map(blocker => blocker.cleanup())));
  const originalComplete = store._localControllerComplete.bind(store);
  const originalResolve = store._localControllerResolveCompletion.bind(store);
  let completeCalls = 0;
  let resolverCalls = 0;
  let accountingBefore = null;
  let accountingAfterRollback = null;
  let accountingCommitted = null;
  let expectedDiskDebit = null;
  store._localControllerComplete = (capability, input) => {
    completeCalls += 1;
    expectedDiskDebit = expectedLocalControllerWriteDiskDebit(input.controllerState);
    const blocker = sqliteWriterBlockerSync(temporary.databasePath);
    blockers.push(blocker);
    accountingBefore = localCompletionAccounting(temporary.databasePath, lease.workflowId);
    try {
      return originalComplete(capability, input);
    } catch (error) {
      blocker.releaseAndWait();
      accountingAfterRollback = localCompletionAccounting(temporary.databasePath, lease.workflowId);
      originalComplete(capability, input);
      accountingCommitted = localCompletionAccounting(temporary.databasePath, lease.workflowId);
      const raw = new DatabaseSync(temporary.databasePath);
      try {
        const deleted = raw.prepare('DELETE FROM outbox WHERE event_id = ?').run(input.eventId);
        assert.equal(Number(deleted.changes), 1);
      } finally {
        raw.close();
      }
      throw error;
    }
  };
  store._localControllerResolveCompletion = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  const result = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    planJson: accepted.work.work.planJson,
    limitsJson: accepted.work.work.limitsJson,
    ...localCompletionRetryBudget()
  });
  assert.equal(result.status, 'stopped');
  assert.equal(result.reason, 'LOCAL_COMPLETION_OUTCOME_UNKNOWN');
  assert.equal(completeCalls, 1);
  assert.equal(resolverCalls, 1);
  assert.equal(accountingCommitted.workflowEvents, accountingBefore.workflowEvents + 1);
  assert.equal(accountingCommitted.workflowCheckpoints, accountingBefore.workflowCheckpoints + 1);
  assert.equal(accountingCommitted.workflowOutboxRows, accountingBefore.workflowOutboxRows + 1);
  const after = localCompletionAccounting(temporary.databasePath, lease.workflowId);
  assert.equal(after.workflowEvents, accountingCommitted.workflowEvents);
  assert.equal(after.workflowCheckpoints, accountingCommitted.workflowCheckpoints);
  assert.equal(after.workflowOutboxRows, accountingCommitted.workflowOutboxRows - 1);
  assert.equal(after.reservedEventSlots, accountingCommitted.reservedEventSlots);
  assert.equal(accountingCommitted.operationsWorkReservedDiskBytes,
    accountingBefore.operationsWorkReservedDiskBytes - expectedDiskDebit);
  assert.equal(accountingCommitted.reservedDiskBytes, accountingBefore.reservedDiskBytes - expectedDiskDebit);
  assert.equal(accountingCommitted.reservedEventSlots, accountingBefore.reservedEventSlots - 1);
  assert.equal(after.reservedDiskBytes, accountingCommitted.reservedDiskBytes);
  assert.equal(after.operationsWorkReservedDiskBytes, accountingCommitted.operationsWorkReservedDiskBytes);
  assert.equal(after.operationsWorkReservedEventSlots, accountingCommitted.operationsWorkReservedEventSlots);
  assert.deepEqual(accountingAfterRollback, accountingBefore);
  assert.deepEqual(localCompletionExternalCustody(temporary.databasePath, lease.workflowId), custodyBeforeRun);
  assert.ok(after.operationsWorkReservedDiskBytes > 0,
    'partial uncertain completion retains the other accepted operation stages');
  assert.equal(store.getCheckpoint(lease.workflowId).data.pendingStepId, null);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_completed').length, 1);

  const reopened = DurableWorkflowStore.open({ databasePath: temporary.databasePath, busyTimeoutMs: 20 });
  try {
    assert.equal(reopened.getCheckpoint(lease.workflowId).data.pendingStepId, null);
    assert.equal(reopened.listEvents({ workflowId: lease.workflowId })
      .filter(event => event.type === 'private_local_step_completed').length, 1);
    assert.deepEqual(localCompletionAccounting(temporary.databasePath, lease.workflowId), after);
    assert.deepEqual(localCompletionExternalCustody(temporary.databasePath, lease.workflowId), custodyBeforeRun);
  } finally {
    reopened.close();
  }
});

test('unmarked, malformed, or incorrectly scoped busy lookalikes never enter completion recovery', async t => {
  const temporary = temporaryDatabase('bounded-local-completion-busy-lookalikes-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'completion-busy-lookalike');
  makeAdmission(store, lease, SINGLE_PLAN, '{}', 'completion-busy-lookalike:admit');

  const wrongOperation = new RuntimeError({
    module: 'state', operation: '_localControllerAdmit', code: 'STORE_BUSY', retryable: true,
    safe_message: 'The durable store is busy.'
  });
  const wrongModule = new RuntimeError({
    module: 'runtime', operation: '_localControllerComplete', code: 'STORE_BUSY', retryable: true,
    safe_message: 'The durable store is busy.'
  });
  const duck = {
    name: 'RuntimeError', module: 'state', operation: '_localControllerComplete',
    code: 'STORE_BUSY', retryable: true
  };
  const failures = [localCompletionBusy(), wrongOperation, wrongModule, duck];
  let completeCalls = 0;
  let resolverCalls = 0;
  store._localControllerComplete = () => {
    const failure = failures[completeCalls];
    completeCalls += 1;
    throw failure;
  };
  const originalResolve = store._localControllerResolveCompletion.bind(store);
  store._localControllerResolveCompletion = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  for (let index = 0; index < failures.length; index += 1) {
    const before = store.getCheckpoint(lease.workflowId).data.budget;
    const result = await runBoundedLocalPlan({
      store,
      workflowId: lease.workflowId,
      ownerId: lease.ownerId,
      fence: lease.fence,
      ...localCompletionRetryBudget()
    });
    assert.equal(result.status, 'stopped');
    assert.equal(result.reason, 'LOCAL_CONTROLLER_STOPPED');
    const after = store.getCheckpoint(lease.workflowId).data.budget;
    assert.equal(after.stepAttempts, before.stepAttempts + 1);
    assert.equal(after.localRetries, before.localRetries + 1);
  }
  assert.equal(completeCalls, failures.length);
  assert.equal(resolverCalls, 0);
  assert.equal(store.getCheckpoint(lease.workflowId).data.pendingStepId, 'only');
  assert.equal(store.getCheckpoint(lease.workflowId).data.budget.stepAttempts, failures.length + 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .some(event => event.type === 'private_local_step_completed'), false);
});

test('confirmed completion busy without private retry budget is not reconciled or retried', async t => {
  const temporary = temporaryDatabase('bounded-local-completion-no-retry-budget-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, Date.now, 20);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'completion-no-retry-budget');
  makeAdmission(store, lease, SINGLE_PLAN, '{}', 'completion-no-retry-budget:admit');
  const blockers = [];
  t.after(async () => Promise.all(blockers.map(blocker => blocker.cleanup())));
  const originalComplete = store._localControllerComplete.bind(store);
  let firstBusy = null;
  let completeCalls = 0;
  let resolverCalls = 0;
  store._localControllerComplete = (...args) => {
    completeCalls += 1;
    const blocker = sqliteWriterBlockerSync(temporary.databasePath);
    blockers.push(blocker);
    try {
      return originalComplete(...args);
    } catch (error) {
      firstBusy = error;
      blocker.releaseAndWait();
      throw error;
    }
  };
  const originalResolve = store._localControllerResolveCompletion.bind(store);
  store._localControllerResolveCompletion = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  const attemptsBefore = store.getCheckpoint(lease.workflowId).data.budget.stepAttempts;
  const result = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence
  });
  assert.equal(firstBusy instanceof RuntimeError, true);
  assert.equal(firstBusy.code, 'STORE_BUSY');
  assert.equal(result.status, 'stopped');
  assert.equal(result.reason, 'LOCAL_CONTROLLER_STOPPED');
  assert.equal(completeCalls, 1);
  assert.equal(resolverCalls, 0);
  assert.equal(store.getCheckpoint(lease.workflowId).data.pendingStepId, 'only');
  assert.equal(store.getCheckpoint(lease.workflowId).data.budget.stepAttempts, attemptsBefore + 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .some(event => event.type === 'private_local_step_completed'), false);
});

test('an unavailable stop-only completion reconciliation fails closed without a second write', async t => {
  const temporary = temporaryDatabase('bounded-local-completion-reconcile-busy-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, Date.now, 20);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'completion-reconcile-busy');
  makeAdmission(store, lease, SINGLE_PLAN, '{}', 'completion-reconcile-busy:admit');
  const blockers = [];
  t.after(async () => Promise.all(blockers.map(blocker => blocker.cleanup())));
  const originalComplete = store._localControllerComplete.bind(store);
  let completeCalls = 0;
  let resolverCalls = 0;
  store._localControllerComplete = (...args) => {
    completeCalls += 1;
    const blocker = sqliteWriterBlockerSync(temporary.databasePath);
    blockers.push(blocker);
    try {
      return originalComplete(...args);
    } catch (error) {
      blocker.releaseAndWait();
      throw error;
    }
  };
  store._localControllerResolveCompletion = () => {
    resolverCalls += 1;
    throw new RuntimeError({
      module: 'state', operation: '_localControllerResolveCompletion', code: 'STORE_BUSY', retryable: true,
      safe_message: 'The durable store is busy.'
    });
  };

  const attemptsBefore = store.getCheckpoint(lease.workflowId).data.budget.stepAttempts;
  const result = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    ...localCompletionRetryBudget()
  });
  assert.equal(result.status, 'stopped');
  assert.equal(result.reason, 'LOCAL_COMPLETION_OUTCOME_UNKNOWN');
  assert.equal(completeCalls, 1);
  assert.equal(resolverCalls, 1);
  assert.equal(store.getCheckpoint(lease.workflowId).data.pendingStepId, 'only');
  assert.equal(store.getCheckpoint(lease.workflowId).data.budget.stepAttempts, attemptsBefore + 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .some(event => event.type === 'private_local_step_completed'), false);
});

test('a confirmed pending completion is not retried after the absolute parent deadline', async t => {
  const temporary = temporaryDatabase('bounded-local-completion-deadline-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, Date.now, 20);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'completion-deadline');
  makeAdmission(store, lease, SINGLE_PLAN, '{}', 'completion-deadline:admit');
  const blockers = [];
  t.after(async () => Promise.all(blockers.map(blocker => blocker.cleanup())));
  const originalComplete = store._localControllerComplete.bind(store);
  const originalResolve = store._localControllerResolveCompletion.bind(store);
  let completeCalls = 0;
  let resolverCalls = 0;
  store._localControllerComplete = (...args) => {
    completeCalls += 1;
    const blocker = sqliteWriterBlockerSync(temporary.databasePath);
    blockers.push(blocker);
    try {
      return originalComplete(...args);
    } catch (error) {
      blocker.releaseAndWait();
      throw error;
    }
  };
  store._localControllerResolveCompletion = (...args) => {
    resolverCalls += 1;
    return originalResolve(...args);
  };

  const attemptsBefore = store.getCheckpoint(lease.workflowId).data.budget.stepAttempts;
  const result = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    busyTimeoutMs: 20,
    runtimeDeadlineNs: 1n
  });
  assert.equal(result.status, 'stopped');
  assert.equal(result.reason, 'LOCAL_COMPLETION_BUSY');
  assert.equal(completeCalls, 1);
  assert.equal(resolverCalls, 1);
  assert.equal(store.getCheckpoint(lease.workflowId).data.pendingStepId, 'only');
  assert.equal(store.getCheckpoint(lease.workflowId).data.budget.stepAttempts, attemptsBefore + 1);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .some(event => event.type === 'private_local_step_completed'), false);
});

test('pending completion does not retry when lease, wall, or elapsed headroom cannot cover busy wait', async t => {
  const startAtMs = 1_790_000_000_000;
  const controls = [
    { name: 'lease', leaseTtlMs: 200 },
    { name: 'wall', leaseTtlMs: 60_000 },
    { name: 'elapsed', leaseTtlMs: 60_000 }
  ];
  for (const control of controls) {
    await t.test(`completion does not retry with insufficient ${control.name} headroom`, async () => {
      const temporary = temporaryDatabase('bounded-local-completion-window-' + control.name + '-');
      const clock = mutableClock(startAtMs);
      const store = openStore(temporary.databasePath, clock.read, 20);
      const lease = createRunningWorkflow(
        store, 'completion-window-' + control.name, 'worker-local', control.leaseTtlMs
      );
      makeAdmission(store, lease, SINGLE_PLAN, '{}', 'completion-window-' + control.name + ':admit');
      const initial = store.getCheckpoint(lease.workflowId).data;
      if (control.name === 'wall') {
        const lastWallMs = initial.budget.deadlineWallMs - 200;
        rewriteLatestPrivateCheckpoint(temporary.databasePath, lease.workflowId, {
          ...initial,
          budget: { ...initial.budget, lastWallMs, elapsedMs: 0 }
        });
        clock.set(lastWallMs);
      } else if (control.name === 'elapsed') {
        rewriteLatestPrivateCheckpoint(temporary.databasePath, lease.workflowId, {
          ...initial,
          budget: { ...initial.budget, elapsedMs: initial.budget.deadlineWallMs
            - initial.budget.startedWallMs - 200 }
        });
      }

      const blockers = [];
      try {
        const originalComplete = store._localControllerComplete.bind(store);
        const originalResolve = store._localControllerResolveCompletion.bind(store);
        let completeCalls = 0;
        let resolverCalls = 0;
        store._localControllerComplete = (...args) => {
          completeCalls += 1;
          const blocker = sqliteWriterBlockerSync(temporary.databasePath);
          blockers.push(blocker);
          try {
            return originalComplete(...args);
          } catch (error) {
            blocker.releaseAndWait();
            throw error;
          }
        };
        store._localControllerResolveCompletion = (...args) => {
          resolverCalls += 1;
          return originalResolve(...args);
        };

        const result = await runBoundedLocalPlan({
          store,
          workflowId: lease.workflowId,
          ownerId: lease.ownerId,
          fence: lease.fence,
          busyTimeoutMs: 250,
          runtimeDeadlineNs: process.hrtime.bigint() + 5_000_000_000n
        });
        assert.equal(result.status, 'stopped', control.name);
        assert.equal(result.reason, 'LOCAL_COMPLETION_BUSY', control.name);
        assert.equal(completeCalls, 1, control.name);
        assert.equal(resolverCalls, 1, control.name);
        assert.equal(store.getCheckpoint(lease.workflowId).data.pendingStepId, 'only', control.name);
        assert.equal(store.listEvents({ workflowId: lease.workflowId }).some(event =>
          event.type === 'private_local_step_completed'), false, control.name);
      } finally {
        await Promise.all(blockers.map(blocker => blocker.cleanup()));
        store.close();
        temporary.cleanup();
      }
    });
  }
});

test('SQLite reopen rejects rehashed fabricated results and over-limit saved output', async t => {
  const forged = temporaryDatabase('bounded-rehashed-result-');
  t.after(() => forged.cleanup());
  let store = openStore(forged.databasePath);
  t.after(() => store.close());
  const forgedLease = createRunningWorkflow(store, 'rehashed-result-run');
  const valid = await runBoundedLocalPlan({
    store,
    workflowId: forgedLease.workflowId,
    ownerId: forgedLease.ownerId,
    fence: forgedLease.fence,
    planJson: SELF_REHASH_PLAN
  });
  assert.equal(valid.status, 'completed');
  const checkpoint = store.getCheckpoint(forgedLease.workflowId);
  const plan = compilePlan(SELF_REHASH_PLAN, createLimitSet(checkpoint.data.limitsJson), undefined, forgedLease.workflowId);
  const forgedFirst = savedResult(plan, 'first', { left: 1, right: 2 }, 900);
  const forgedSecond = savedResult(plan, 'second', { left: 900, right: 2 }, 902);
  const forgedData = {
    ...checkpoint.data,
    completedResultsJson: JSON.stringify({ first: forgedFirst, second: forgedSecond }),
    budget: { ...checkpoint.data.budget, totalResultBytes: 6 }
  };
  const eventCountBeforeForgery = store.listEvents({ workflowId: forgedLease.workflowId }).length;
  store.close();
  rewriteLatestPrivateCheckpoint(forged.databasePath, forgedLease.workflowId, forgedData);

  store = openStore(forged.databasePath);
  const stopped = await runBoundedLocalPlan({
    store,
    workflowId: forgedLease.workflowId,
    ownerId: forgedLease.ownerId,
    fence: forgedLease.fence
  });
  assert.equal(stopped.status, 'stopped');
  assert.equal(stopped.reason, 'PLAN_RESULT_IDENTITY_INVALID');
  const inspection = inspectWorkflowRecovery({ store, workflowId: forgedLease.workflowId });
  assert.equal(inspection.kind, 'quarantine');
  assert.equal(inspection.reason, 'LOCAL_RESULT_IDENTITY_INVALID');
  assert.equal(store.listEvents({ workflowId: forgedLease.workflowId }).length, eventCountBeforeForgery);

  store.close();
  const oversized = temporaryDatabase('bounded-saved-result-limit-');
  t.after(() => oversized.cleanup());
  store = openStore(oversized.databasePath);
  t.after(() => store.close());
  const oversizedLease = createRunningWorkflow(store, 'saved-result-limit-run');
  const planJson = JSON.stringify({
    kind: PLAN_KIND,
    version: 1,
    steps: [{ id: 'text', operation: 'text.concat', dependsOn: [], args: { parts: ['abc'] } }]
  });
  const limitsJson = '{"maxResultBytes":2,"maxTotalResultBytes":3}';
  const pending = await runBoundedLocalPlan({
    store,
    workflowId: oversizedLease.workflowId,
    ownerId: oversizedLease.ownerId,
    fence: oversizedLease.fence,
    planJson,
    limitsJson
  });
  assert.equal(pending.status, 'stopped');
  assert.equal(pending.reason, 'PLAN_RESULT_TOO_LARGE');
  const admitted = store.getCheckpoint(oversizedLease.workflowId);
  assert.equal(admitted.data.pendingStepId, 'text');
  assert.equal(admitted.data.budget.totalResultBytes, 0);
  const tightPlan = compilePlan(planJson, createLimitSet(limitsJson), undefined, oversizedLease.workflowId);
  const overLimit = savedResult(tightPlan, 'text', { parts: ['abc'] }, 'abc');
  const oversizedData = {
    ...admitted.data,
    completedResultsJson: JSON.stringify({ text: overLimit }),
    pendingStepId: null
  };
  store.close();
  rewriteLatestPrivateCheckpoint(oversized.databasePath, oversizedLease.workflowId, oversizedData);

  store = openStore(oversized.databasePath);
  const recovered = await runBoundedLocalPlan({
    store,
    workflowId: oversizedLease.workflowId,
    ownerId: oversizedLease.ownerId,
    fence: oversizedLease.fence
  });
  assert.equal(recovered.status, 'stopped');
  assert.equal(recovered.reason, 'PLAN_RESULT_TOO_LARGE');
  const oversizedInspection = inspectWorkflowRecovery({ store, workflowId: oversizedLease.workflowId });
  assert.equal(oversizedInspection.kind, 'quarantine');
  assert.equal(oversizedInspection.reason, 'LOCAL_RESULT_IDENTITY_INVALID');
  assert.equal(store.getCheckpoint(oversizedLease.workflowId).data.budget.totalResultBytes, 0);
});

test('checkpoint JSON rejects duplicate members while retaining generic finite-number compatibility', t => {
  const numeric = temporaryDatabase('bounded-checkpoint-number-compatibility-');
  t.after(() => numeric.cleanup());
  const numericStore = openStore(numeric.databasePath);
  numericStore.createWorkflow({
    workflowId: 'checkpoint-large-finite-number',
    checkpoint: { largeFinite: 1e21 },
    eventId: 'checkpoint-large-finite-number:created'
  });
  assert.equal(numericStore.getCheckpoint('checkpoint-large-finite-number').data.largeFinite, 1e21);
  numericStore.close();

  const duplicate = temporaryDatabase('bounded-checkpoint-duplicate-member-');
  t.after(() => duplicate.cleanup());
  const duplicateStore = openStore(duplicate.databasePath);
  duplicateStore.createWorkflow({
    workflowId: 'checkpoint-duplicate-member',
    checkpoint: { value: 1 },
    eventId: 'checkpoint-duplicate-member:created'
  });
  duplicateStore.close();
  rewriteLatestCheckpointJson(
    duplicate.databasePath,
    'checkpoint-duplicate-member',
    '{"schemaVersion":2,"data":{"value":1,"value":2}}'
  );
  const reopened = openStore(duplicate.databasePath);
  t.after(() => reopened.close());
  assert.throws(
    () => reopened.getCheckpoint('checkpoint-duplicate-member'),
    error => error.code === 'CHECKPOINT_CORRUPT'
  );
});

test('external, provider, privileged, unknown, and injected-mode plans stop before admission in every mode', async t => {
  const operations = [
    ['network.fetch', 'external'],
    ['provider.image', 'provider'],
    ['provider.reason', 'provider'],
    ['system.shell', 'privileged']
  ];
  let sends = 0;
  const priorFetch = globalThis.fetch;
  globalThis.fetch = async () => { sends += 1; throw new Error('synthetic send counter'); };
  t.after(() => { globalThis.fetch = priorFetch; });

  for (const mode of ['mock', 'recorded', 'sandbox', 'live']) {
    for (const [operation, effect] of operations) {
      const info = inspectOperation(createDefaultRegistry(), operation);
      assert.equal(info.effect, effect);
      assert.equal(info.status, 'unavailable');
      const temporary = temporaryDatabase('bounded-no-capability-');
      t.after(() => temporary.cleanup());
      const store = openStore(temporary.databasePath);
      const lease = createRunningWorkflow(store, `denied-${mode}-${operation.replace('.', '-')}`);
      const before = store.getCheckpoint(lease.workflowId);
      const planJson = JSON.stringify({
        kind: PLAN_KIND,
        version: 1,
        steps: [{ id: 'blocked', operation, dependsOn: [], args: {} }]
      });
      const result = await runBoundedLocalPlan({
        store,
        workflowId: lease.workflowId,
        ownerId: lease.ownerId,
        fence: lease.fence,
        planJson
      });
      assert.equal(result.status, 'stopped');
      assert.equal(result.reason, 'PLAN_EFFECT_UNAVAILABLE');
      assert.equal(store.getCheckpoint(lease.workflowId).sequence, before.sequence);
      assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
      assert.equal(store.listEvents({ workflowId: lease.workflowId }).some(event => event.type.startsWith('private_local_')), false);
      store.close();
    }
  }
  assert.equal(sends, 0);

  const unknown = temporaryDatabase('bounded-unknown-tool-');
  t.after(() => unknown.cleanup());
  const store = openStore(unknown.databasePath);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'unknown-tool-run');
  const result = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    planJson: JSON.stringify({
      kind: PLAN_KIND,
      version: 1,
      steps: [{ id: 'unknown', operation: 'tool.injected', dependsOn: [], args: {} }]
    })
  });
  assert.equal(result.status, 'stopped');
  assert.equal(result.reason, 'PLAN_UNKNOWN_OPERATION');

  const checkpointBeforeOversize = store.getCheckpoint(lease.workflowId);
  const oversized = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    planJson: SINGLE_PLAN + ' '.repeat(32_769)
  });
  assert.equal(oversized.reason, 'PLAN_TOO_LARGE');
  assert.equal(store.getCheckpoint(lease.workflowId).sha256, checkpointBeforeOversize.sha256);

  const externalPlan = JSON.stringify({
    kind: PLAN_KIND,
    version: 1,
    steps: [{ id: 'blocked', operation: 'network.fetch', dependsOn: [], args: {} }]
  });
  for (const authorityState of ['synthetic-grant', 'synthetic-revocation']) {
    const forgedAuthority = await runBoundedLocalPlan({
      store,
      workflowId: lease.workflowId,
      ownerId: lease.ownerId,
      fence: lease.fence,
      planJson: externalPlan,
      mode: 'mock',
      authorization: { state: authorityState, granted: true },
      opaqueHoldRef: 'not-a-capability',
      cachedRank: 1
    });
    assert.equal(forgedAuthority.reason, 'CONTROLLER_INPUT_INVALID', authorityState);
    assert.equal(store.getCheckpoint(lease.workflowId).sha256, checkpointBeforeOversize.sha256);
  }
  assert.equal(sends, 0);
});

test('the four process-kill boundaries retain counters and never rerun a committed result', async t => {
  const cases = [
    ['before-admission', 0, 1, 0],
    ['after-admission', 1, 2, 1],
    ['after-compute', 1, 2, 1],
    ['after-completion', 1, 1, 0]
  ];
  for (const [stage, expectedAdmitted, expectedAttempts, expectedRetries] of cases) {
    const temporary = temporaryDatabase('bounded-crash-' + stage + '-');
    t.after(() => temporary.cleanup());
    const planPath = join(temporary.directory, 'plan.json');
    const markerPath = join(temporary.directory, 'worker.ready');
    writeFileSync(planPath, SINGLE_PLAN);
    const setupStore = openStore(temporary.databasePath);
    const lease = createRunningWorkflow(
      setupStore,
      'crash-' + stage,
      'worker-local',
      60_000,
      stage === 'after-completion'
        ? { preserved: 'synthetic-original-checkpoint', number: 1e20 }
        : { preserved: 'synthetic-original-checkpoint' }
    );
    setupStore.close();

    const worker = spawnWithSignal(fixturePath('local-controller-crash.mjs'), [
      temporary.databasePath,
      lease.workflowId,
      lease.ownerId,
      String(lease.fence),
      planPath,
      markerPath,
      stage
    ]);
    const { marker } = await killAtMarker(worker, markerPath);
    assert.equal(marker.stage, stage);
    if (stage === 'after-compute') assert.match(marker.resultHash, /^sha256_[0-9a-f]{64}$/u);

    const recovered = openStore(temporary.databasePath);
    const beforeEvents = recovered.listEvents({ workflowId: lease.workflowId }).length;
    const result = await runBoundedLocalPlan({
      store: recovered,
      workflowId: lease.workflowId,
      ownerId: lease.ownerId,
      fence: lease.fence,
      planJson: SINGLE_PLAN
    });
    assert.equal(result.status, 'completed', stage);
    assert.equal(result.results[0].output, 9, stage);
    const state = recovered.getCheckpoint(lease.workflowId).data;
    assert.equal(state.budget.stepsAdmitted, expectedAdmitted || 1, stage);
    assert.equal(state.budget.stepAttempts, expectedAttempts, stage);
    assert.equal(state.budget.localRetries, expectedRetries, stage);
    assert.equal(state.budget.replans, 0, stage);
    if (stage === 'after-completion') {
      assert.ok(Object.is(state.baseCheckpointData.number, 1e20));
      const inspection = inspectWorkflowRecovery({ store: recovered, workflowId: lease.workflowId });
      assert.equal(inspection.kind, 'durable-evidence-present');
      assert.equal(inspection.incomplete, false);
    }
    assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0, stage);
    assert.equal(countRows(temporary.databasePath, 'transport_attempts'), 0, stage);

    if (stage === 'after-completion') {
      assert.equal(recovered.listEvents({ workflowId: lease.workflowId }).length, beforeEvents);
    }
    const eventsAtCompletion = recovered.listEvents({ workflowId: lease.workflowId }).length;
    const repeated = await runBoundedLocalPlan({
      store: recovered,
      workflowId: lease.workflowId,
      ownerId: lease.ownerId,
      fence: lease.fence
    });
    assert.equal(repeated.resultHash, result.resultHash, stage + ': ' + JSON.stringify(repeated));
    assert.equal(recovered.listEvents({ workflowId: lease.workflowId }).length, eventsAtCompletion, stage);
    recovered.close();
  }
});

test('schema-5 checkpoint integer-key ordering survives normal completion and real crash recovery', async t => {
  const cases = [
    { name: 'top-level-numeric', checkpoint: { '2': 'two', '10': 'ten' } },
    { name: 'nested-numeric', checkpoint: { nested: { '2': 'two', '10': 'ten' } } },
    { name: 'array-object-numeric', checkpoint: { rows: [{ '2': 'two', '10': 'ten' }] } },
    {
      name: 'mixed-numeric-named',
      checkpoint: { '2': 'two', '10': 'ten', alpha: 'first', zeta: { '3': 3, '12': 12 } }
    },
    {
      name: 'named-and-finite',
      checkpoint: { alpha: 'two', beta: 'ten', nested: { left: 1e21, right: -1.5 } }
    }
  ];

  for (const entry of cases) {
    const normal = temporaryDatabase('bounded-checkpoint-order-normal-' + entry.name + '-');
    t.after(() => normal.cleanup());
    const store = openStore(normal.databasePath);
    let normalResult;
    let lease;
    try {
      lease = createRunningWorkflow(store, 'key-order-' + entry.name, 'worker-local', 60_000, entry.checkpoint);
      normalResult = await runBoundedLocalPlan({
        store,
        workflowId: lease.workflowId,
        ownerId: lease.ownerId,
        fence: lease.fence,
        planJson: SINGLE_PLAN
      });
      assert.equal(normalResult.status, 'completed', entry.name);
      assert.equal(normalResult.results[0].output, 9, entry.name);
      assert.deepEqual(store.getCheckpoint(lease.workflowId).data.baseCheckpointData, entry.checkpoint);
    } finally {
      store.close();
    }

    const normalReopened = openStore(normal.databasePath);
    try {
      assert.equal(inspectWorkflowRecovery({ store: normalReopened, workflowId: lease.workflowId }).kind,
        'durable-evidence-present', entry.name);
      const beforeReuse = snapshotDatabase(normal.databasePath);
      const reused = await runBoundedLocalPlan({
        store: normalReopened,
        workflowId: lease.workflowId,
        ownerId: lease.ownerId,
        fence: lease.fence
      });
      assert.equal(reused.resultHash, normalResult.resultHash, entry.name);
      assert.equal(snapshotDatabase(normal.databasePath), beforeReuse, entry.name + ' reuse is read-only');
    } finally {
      normalReopened.close();
    }

    for (const stage of ['after-admission', 'after-compute']) {
      const temporary = temporaryDatabase('bounded-checkpoint-order-' + stage + '-' + entry.name + '-');
      t.after(() => temporary.cleanup());
      const planPath = join(temporary.directory, 'plan.json');
      const markerPath = join(temporary.directory, 'worker.ready');
      writeFileSync(planPath, SINGLE_PLAN);
      const setupStore = openStore(temporary.databasePath);
      let crashLease;
      try {
        crashLease = createRunningWorkflow(
          setupStore,
          'key-order-' + stage + '-' + entry.name,
          'worker-local',
          60_000,
          entry.checkpoint
        );
      } finally {
        setupStore.close();
      }

      const worker = spawnWithSignal(fixturePath('local-controller-crash.mjs'), [
        temporary.databasePath,
        crashLease.workflowId,
        crashLease.ownerId,
        String(crashLease.fence),
        planPath,
        markerPath,
        stage
      ]);
      const { marker } = await killAtMarker(worker, markerPath);
      assert.equal(marker.stage, stage);

      const recovered = openStore(temporary.databasePath);
      try {
        assert.deepEqual(recovered.getCheckpoint(crashLease.workflowId).data.baseCheckpointData, entry.checkpoint);
        assert.equal(inspectWorkflowRecovery({ store: recovered, workflowId: crashLease.workflowId }).kind,
          'local-replay-candidate', entry.name + ' ' + stage);
        const result = await runBoundedLocalPlan({
          store: recovered,
          workflowId: crashLease.workflowId,
          ownerId: crashLease.ownerId,
          fence: crashLease.fence
        });
        assert.equal(result.status, 'completed', entry.name + ' ' + stage);
        assert.equal(result.results[0].output, 9, entry.name + ' ' + stage);
        assert.deepEqual(recovered.getCheckpoint(crashLease.workflowId).data.baseCheckpointData, entry.checkpoint);
      } finally {
        recovered.close();
      }

      const afterCrashReopen = openStore(temporary.databasePath);
      try {
        assert.equal(inspectWorkflowRecovery({ store: afterCrashReopen, workflowId: crashLease.workflowId }).kind,
          'durable-evidence-present', entry.name + ' ' + stage);
        const beforeReuse = snapshotDatabase(temporary.databasePath);
        const reused = await runBoundedLocalPlan({
          store: afterCrashReopen,
          workflowId: crashLease.workflowId,
          ownerId: crashLease.ownerId,
          fence: crashLease.fence
        });
        assert.equal(reused.status, 'completed', entry.name + ' ' + stage);
        assert.equal(snapshotDatabase(temporary.databasePath), beforeReuse, entry.name + ' ' + stage + ' reuse');
      } finally {
        afterCrashReopen.close();
      }
    }
  }
});

test('crash markers publish atomically and owned children are reaped on parse and timeout failures', async t => {
  const temporary = temporaryDatabase('bounded-marker-publication-');
  t.after(() => temporary.cleanup());

  const markerPath = join(temporary.directory, 'fragmented.ready');
  const partialReadyPath = join(temporary.directory, 'fragmented.partial');
  const releasePath = join(temporary.directory, 'fragmented.release');
  const fragmented = spawnWithSignal(fixturePath('local-controller-marker-publication.mjs'), [
    markerPath,
    'fragmented',
    partialReadyPath,
    releasePath
  ]);
  try {
    await waitForFiles([partialReadyPath]);
    assert.equal(existsSync(markerPath), false, 'the partial bytes remain under the temporary name');
    writeFileSync(releasePath, 'publish complete marker');
    const { marker, result } = await killAtMarker(fragmented, markerPath);
    assert.deepEqual(marker, { stage: 'fragmented', complete: true });
    assert.equal(result.signal, 'SIGKILL');
  } finally {
    await reapOwnedChild(fragmented);
  }

  const malformedPath = join(temporary.directory, 'malformed.ready');
  const malformed = spawnWithSignal(fixturePath('local-controller-marker-publication.mjs'), [
    malformedPath,
    'malformed',
    join(temporary.directory, 'unused.partial')
  ]);
  await assert.rejects(killAtMarker(malformed, malformedPath), SyntaxError);
  assert.equal((await malformed.done).signal, 'SIGKILL', 'malformed JSON still terminates and reaps the owned child');

  const assertionPath = join(temporary.directory, 'assertion.ready');
  const assertionChild = spawnWithSignal(fixturePath('local-controller-marker-publication.mjs'), [
    assertionPath,
    'valid',
    join(temporary.directory, 'unused.partial')
  ]);
  await assert.rejects(
    killAtMarker(assertionChild, assertionPath, 5_000, marker => assert.equal(marker.stage, 'expected')),
    assert.AssertionError
  );
  assert.equal((await assertionChild.done).signal, 'SIGKILL', 'assertion failure still reaps the owned child');

  const missingPath = join(temporary.directory, 'missing.ready');
  const silent = spawnWithSignal(fixturePath('local-controller-marker-publication.mjs'), [
    missingPath,
    'silent',
    join(temporary.directory, 'unused.partial')
  ]);
  const started = performance.now();
  await assert.rejects(killAtMarker(silent, missingPath, 50), /Fixture processes did not reach the release barrier/u);
  assert.ok(performance.now() - started < 1_000, 'marker timeout fails promptly');
  assert.equal((await silent.done).signal, 'SIGKILL', 'marker timeout still terminates and reaps the owned child');
});

test('finite original checkpoint numbers survive admission and computation SIGKILL recovery', async t => {
  const values = [
    42,
    Number.MAX_SAFE_INTEGER,
    Number.MAX_SAFE_INTEGER + 1,
    1e20,
    1e21,
    -1e20,
    -1.5,
    1.5,
    1e100
  ];

  for (const value of values) {
    for (const stage of ['after-admission', 'after-compute']) {
      const temporary = temporaryDatabase('bounded-base-number-' + stage + '-');
      t.after(() => temporary.cleanup());
      const planPath = join(temporary.directory, 'plan.json');
      const markerPath = join(temporary.directory, 'worker.ready');
      writeFileSync(planPath, SINGLE_PLAN);
      const setupStore = openStore(temporary.databasePath);
      const lease = createRunningWorkflow(
        setupStore,
        `base-number-${stage}-${values.indexOf(value)}`,
        'worker-local',
        60_000,
        { number: value, nested: [{ preserved: value }] }
      );
      setupStore.close();

      const worker = spawnWithSignal(fixturePath('local-controller-crash.mjs'), [
        temporary.databasePath,
        lease.workflowId,
        lease.ownerId,
        String(lease.fence),
        planPath,
        markerPath,
        stage
      ]);
      const { marker } = await killAtMarker(worker, markerPath);
      assert.equal(marker.stage, stage);

      const recovered = openStore(temporary.databasePath);
      const before = recovered.getCheckpoint(lease.workflowId);
      assert.ok(Object.is(before.data.baseCheckpointData.number, value), `${stage} base number ${value}`);
      assert.ok(Object.is(before.data.baseCheckpointData.nested[0].preserved, value));
      const inspection = inspectWorkflowRecovery({ store: recovered, workflowId: lease.workflowId });
      assert.equal(inspection.kind, 'local-replay-candidate', `${stage} inspection ${value}`);
      assert.equal(inspection.incomplete, false);

      const eventsBeforeResume = recovered.listEvents({ workflowId: lease.workflowId }).length;
      const result = await runBoundedLocalPlan({
        store: recovered,
        workflowId: lease.workflowId,
        ownerId: lease.ownerId,
        fence: lease.fence
      });
      assert.equal(result.status, 'completed', `${stage} result ${value}`);
      assert.equal(result.results[0].output, 9, `${stage} output ${value}`);
      const completed = recovered.getCheckpoint(lease.workflowId);
      assert.ok(Object.is(completed.data.baseCheckpointData.number, value));
      const eventsAtCompletion = recovered.listEvents({ workflowId: lease.workflowId }).length;
      assert.equal(eventsAtCompletion, eventsBeforeResume + 2);
      assert.equal(completed.data.budget.localRetries, 1);
      assert.equal(completed.data.budget.stepAttempts, 2);
      assert.equal(inspectWorkflowRecovery({ store: recovered, workflowId: lease.workflowId }).kind,
        'durable-evidence-present');

      const repeated = await runBoundedLocalPlan({
        store: recovered,
        workflowId: lease.workflowId,
        ownerId: lease.ownerId,
        fence: lease.fence
      });
      assert.equal(repeated.resultHash, result.resultHash);
      assert.equal(recovered.getCheckpoint(lease.workflowId).sequence, completed.sequence);
      assert.equal(recovered.listEvents({ workflowId: lease.workflowId }).length, eventsAtCompletion);
      assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
      recovered.close();
    }
  }
});

test('generic checkpoint depth and byte controls survive private admission, recovery inspection, and reuse', async t => {
  const crashCases = [
    { name: 'depth20', checkpoint: nestedCheckpointData(20), stages: ['after-admission'] },
    { name: 'depth24', checkpoint: nestedCheckpointData(24), stages: ['after-admission'] },
    { name: 'depth30', checkpoint: nestedCheckpointData(30), stages: ['after-admission', 'after-compute'] },
    { name: 'bytes200000', checkpoint: { payload: 'a'.repeat(200_000) }, stages: ['after-admission'] },
    { name: 'bytes230000', checkpoint: { payload: 'a'.repeat(230_000) }, stages: ['after-admission', 'after-compute'] },
    { name: 'utf8-120000', checkpoint: { payload: 'é'.repeat(120_000) }, stages: ['after-admission'] }
  ];

  for (const entry of crashCases) {
    for (const stage of entry.stages) {
      const temporary = temporaryDatabase('bounded-b3-' + entry.name + '-' + stage + '-');
      t.after(() => temporary.cleanup());
      const planPath = join(temporary.directory, 'plan.json');
      const markerPath = join(temporary.directory, 'worker.ready');
      writeFileSync(planPath, SINGLE_PLAN);
      const setupStore = openStore(temporary.databasePath);
      const lease = createRunningWorkflow(
        setupStore,
        'b3-' + entry.name + '-' + stage,
        'worker-local',
        60_000,
        entry.checkpoint
      );
      assert.deepEqual(setupStore.getCheckpoint(lease.workflowId).data, entry.checkpoint);
      setupStore.close();

      const worker = spawnWithSignal(fixturePath('local-controller-crash.mjs'), [
        temporary.databasePath,
        lease.workflowId,
        lease.ownerId,
        String(lease.fence),
        planPath,
        markerPath,
        stage
      ]);
      const { marker } = await killAtMarker(worker, markerPath);
      assert.equal(marker.stage, stage);
      if (stage === 'after-admission') {
        assert.equal(marker.revision, 2);
        assert.equal(marker.checkpointSequence, 2);
      }
      if (stage === 'after-compute') assert.match(marker.resultHash, /^sha256_[0-9a-f]{64}$/u);

      const recovered = openStore(temporary.databasePath);
      const pending = recovered.getCheckpoint(lease.workflowId);
      assert.deepEqual(pending.data.baseCheckpointData, entry.checkpoint, entry.name + ' base preserved');
      assert.equal(pending.data.budget.stepsAdmitted, 1, entry.name);
      assert.equal(pending.data.budget.stepAttempts, 1, entry.name);
      assert.equal(pending.data.budget.localRetries, 0, entry.name);
      assert.equal(inspectWorkflowRecovery({ store: recovered, workflowId: lease.workflowId }).kind,
        'local-replay-candidate', entry.name + ' recovery inspection');
      const eventsBeforeReplay = recovered.listEvents({ workflowId: lease.workflowId }).length;

      const result = await runBoundedLocalPlan({
        store: recovered,
        workflowId: lease.workflowId,
        ownerId: lease.ownerId,
        fence: lease.fence
      });
      assert.equal(result.status, 'completed', entry.name + ': ' + JSON.stringify(result));
      assert.equal(result.results[0].output, 9, entry.name);
      const completed = recovered.getCheckpoint(lease.workflowId);
      assert.deepEqual(completed.data.baseCheckpointData, entry.checkpoint, entry.name + ' completed base');
      assert.equal(completed.data.budget.stepsAdmitted, 1, entry.name);
      assert.equal(completed.data.budget.stepAttempts, 2, entry.name);
      assert.equal(completed.data.budget.localRetries, 1, entry.name);
      assert.equal(completed.sequence, 4, entry.name);
      assert.equal(inspectWorkflowRecovery({ store: recovered, workflowId: lease.workflowId }).kind,
        'durable-evidence-present', entry.name + ' completed inspection');
      assert.equal(recovered.listEvents({ workflowId: lease.workflowId }).length, eventsBeforeReplay + 2);

      const eventsAtCompletion = recovered.listEvents({ workflowId: lease.workflowId }).length;
      const repeated = await runBoundedLocalPlan({
        store: recovered,
        workflowId: lease.workflowId,
        ownerId: lease.ownerId,
        fence: lease.fence
      });
      assert.equal(repeated.resultHash, result.resultHash, entry.name + ' completed reuse');
      assert.equal(recovered.getCheckpoint(lease.workflowId).sequence, completed.sequence, entry.name);
      assert.equal(recovered.listEvents({ workflowId: lease.workflowId }).length, eventsAtCompletion, entry.name);
      assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0, entry.name);
      recovered.close();
    }
  }
});

test('private depth overflow is rejected before admission while generic schema-5 data remains readable', async t => {
  const temporary = temporaryDatabase('bounded-b3-depth-boundary-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath);
  const base = nestedCheckpointData(MAX_CHECKPOINT_DEPTH - 1);
  const lease = createRunningWorkflow(store, 'b3-private-depth-boundary', 'worker-local', 60_000, base);
  assert.deepEqual(store.getCheckpoint(lease.workflowId).data, base, 'generic depth-128 checkpoint remains readable');
  const before = snapshotDatabase(temporary.databasePath);

  const result = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    planJson: SINGLE_PLAN
  });
  assert.equal(result.status, 'stopped');
  assert.equal(result.reason, 'CONTROLLER_CHECKPOINT_UNSUPPORTED');
  assert.equal(snapshotDatabase(temporary.databasePath), before);
  store.close();

  const reopened = openStore(temporary.databasePath);
  assert.equal(snapshotDatabase(temporary.databasePath), before, 'all rows and sqlite_sequence survive reopen unchanged');
  assert.deepEqual(reopened.getCheckpoint(lease.workflowId).data, base);
  reopened.close();
});

test('completion-result growth is reserved at the exact UTF-8 checkpoint boundary before admission', async t => {
  const workflowId = 'b3-completion-growth-boundary';
  const projectedBytes = units => initialCheckpointSizeForBase(workflowId, {
    payload: '\0'.repeat(units) + 'é'
  });
  let low = 0;
  let high = 180_000;
  while (low < high) {
    const middle = Math.ceil((low + high) / 2);
    if (projectedBytes(middle)?.reserved?.serializedBytes <= MAX_CHECKPOINT_SERIALIZED_BYTES) low = middle;
    else high = middle - 1;
  }
  const atBoundary = projectedBytes(low)?.reserved;
  const pastBoundary = projectedBytes(low + 1)?.reserved;
  const initialPastBoundary = projectedBytes(low + 1)?.initial;
  assert.ok(atBoundary);
  assert.ok(pastBoundary);
  assert.ok(initialPastBoundary);
  assert.ok(initialPastBoundary.serializedBytes <= MAX_CHECKPOINT_SERIALIZED_BYTES,
    'initial admission checkpoint fits; only the prospective result growth crosses the limit');
  assert.ok(atBoundary.serializedBytes <= MAX_CHECKPOINT_SERIALIZED_BYTES);
  assert.ok(atBoundary.serializedBytes >= MAX_CHECKPOINT_SERIALIZED_BYTES - 5);
  assert.equal(pastBoundary.serializedBytes, atBoundary.serializedBytes + 6);
  assert.ok(pastBoundary.serializedBytes > MAX_CHECKPOINT_SERIALIZED_BYTES);

  const accepted = temporaryDatabase('bounded-b3-completion-growth-accepted-');
  t.after(() => accepted.cleanup());
  const acceptedStore = openStore(accepted.databasePath);
  const acceptedBase = { payload: '\0'.repeat(low) + 'é' };
  const acceptedLease = createRunningWorkflow(
    acceptedStore,
    workflowId,
    'worker-local',
    60_000,
    acceptedBase
  );
  assert.deepEqual(acceptedStore.getCheckpoint(workflowId).data, acceptedBase);
  const acceptedResult = await runBoundedLocalPlan({
    store: acceptedStore,
    workflowId,
    ownerId: acceptedLease.ownerId,
    fence: acceptedLease.fence,
    planJson: SINGLE_PLAN
  });
  assert.equal(acceptedResult.status, 'completed');
  const acceptedCheckpoint = acceptedStore.getCheckpoint(workflowId);
  assert.deepEqual(acceptedCheckpoint.data.baseCheckpointData, acceptedBase);
  assert.ok(measureCheckpointPayload(acceptedCheckpoint.data).serializedBytes <= MAX_CHECKPOINT_SERIALIZED_BYTES);
  acceptedStore.close();
  const acceptedReopened = openStore(accepted.databasePath);
  assert.equal(inspectWorkflowRecovery({ store: acceptedReopened, workflowId }).kind, 'durable-evidence-present');
  const acceptedEvents = acceptedReopened.listEvents({ workflowId }).length;
  const acceptedReuse = await runBoundedLocalPlan({
    store: acceptedReopened,
    workflowId,
    ownerId: acceptedLease.ownerId,
    fence: acceptedLease.fence
  });
  assert.equal(acceptedReuse.resultHash, acceptedResult.resultHash);
  assert.equal(acceptedReopened.listEvents({ workflowId }).length, acceptedEvents);
  acceptedReopened.close();

  const rejected = temporaryDatabase('bounded-b3-completion-growth-rejected-');
  t.after(() => rejected.cleanup());
  const rejectedStore = openStore(rejected.databasePath);
  const rejectedBase = { payload: '\0'.repeat(low + 1) + 'é' };
  const rejectedLease = createRunningWorkflow(
    rejectedStore,
    workflowId,
    'worker-local',
    60_000,
    rejectedBase
  );
  assert.deepEqual(rejectedStore.getCheckpoint(rejectedLease.workflowId).data, rejectedBase);
  const before = snapshotDatabase(rejected.databasePath);
  const rejectedResult = await runBoundedLocalPlan({
    store: rejectedStore,
    workflowId: rejectedLease.workflowId,
    ownerId: rejectedLease.ownerId,
    fence: rejectedLease.fence,
    planJson: SINGLE_PLAN
  });
  assert.equal(rejectedResult.status, 'stopped');
  assert.equal(rejectedResult.reason, 'CONTROLLER_CHECKPOINT_TOO_LARGE');
  assert.equal(snapshotDatabase(rejected.databasePath), before);
  rejectedStore.close();
  const rejectedReopened = openStore(rejected.databasePath);
  assert.equal(snapshotDatabase(rejected.databasePath), before, 'all rows and sqlite_sequence survive reopen unchanged');
  assert.deepEqual(rejectedReopened.getCheckpoint(rejectedLease.workflowId).data, rejectedBase);
  rejectedReopened.close();
});

test('future metadata and results are reserved before admission across the maximum later step ID', async t => {
  for (const entry of [
    { name: 'one-hundred-below', payloadBytes: 259_974, expected: 'completed' },
    { name: 'at-reported-overflow', payloadBytes: 260_074, expected: 'stopped' }
  ]) {
    const temporary = temporaryDatabase('bounded-b5-future-state-' + entry.name + '-');
    t.after(() => temporary.cleanup());
    const clock = mutableClock(1_000);
    const store = openStore(temporary.databasePath, clock.read);
    const base = { payload: 'x'.repeat(entry.payloadBytes) };
    let lease;
    let result;
    let before;
    try {
      lease = createRunningWorkflow(store, 'w', 'worker-local', 60_000, base);
      before = snapshotDatabase(temporary.databasePath);
      result = await runBoundedLocalPlan({
        store,
        workflowId: lease.workflowId,
        ownerId: lease.ownerId,
        fence: lease.fence,
        planJson: LONG_FUTURE_PLAN
      });

      if (entry.expected === 'stopped') {
        assert.equal(result.status, 'stopped', entry.name);
        assert.equal(result.reason, 'CONTROLLER_CHECKPOINT_TOO_LARGE', entry.name);
        assert.equal(snapshotDatabase(temporary.databasePath), before, entry.name + ' no durable mutation');
        const untouched = store.getCheckpoint(lease.workflowId);
        assert.equal(untouched.sequence, 1, entry.name);
        assert.deepEqual(untouched.data, base, entry.name + ' opaque base retained');
      } else {
        assert.equal(result.status, 'completed', entry.name + ': ' + JSON.stringify(result));
        assert.deepEqual(result.results.map(item => item.output), [3, 5], entry.name);
        const completed = store.getCheckpoint(lease.workflowId);
        assert.equal(completed.sequence, 5, entry.name);
        assert.deepEqual(completed.data.baseCheckpointData, base, entry.name + ' opaque base retained');
        assert.equal(completed.data.budget.stepsAdmitted, 2, entry.name);
        assert.equal(completed.data.budget.stepAttempts, 2, entry.name);
        assert.equal(completed.data.budget.localRetries, 0, entry.name);
      }
    } finally {
      store.close();
    }

    const reopened = openStore(temporary.databasePath, clock.read);
    try {
      const afterOpen = snapshotDatabase(temporary.databasePath);
      if (entry.expected === 'stopped') {
        assert.equal(afterOpen, before, entry.name + ' reopen is read-only');
        assert.equal(reopened.getCheckpoint('w').sequence, 1, entry.name);
        assert.deepEqual(reopened.getCheckpoint('w').data, base, entry.name);
      } else {
        assert.equal(inspectWorkflowRecovery({ store: reopened, workflowId: 'w' }).kind,
          'durable-evidence-present', entry.name);
        const beforeReuse = snapshotDatabase(temporary.databasePath);
        const reused = await runBoundedLocalPlan({
          store: reopened,
          workflowId: 'w',
          ownerId: lease.ownerId,
          fence: lease.fence
        });
        assert.equal(reused.resultHash, result.resultHash, entry.name);
        assert.equal(snapshotDatabase(temporary.databasePath), beforeReuse, entry.name + ' completion reuse');
        assert.equal(afterOpen, beforeReuse, entry.name + ' open does not mutate SQL tables');
      }
    } finally {
      reopened.close();
    }

    if (entry.expected === 'completed') {
      for (const stage of ['after-admission', 'after-compute']) {
        const crash = temporaryDatabase('bounded-b5-near-boundary-' + stage + '-');
        t.after(() => crash.cleanup());
        const planPath = join(crash.directory, 'plan.json');
        const markerPath = join(crash.directory, 'worker.ready');
        writeFileSync(planPath, LONG_FUTURE_PLAN);
        const crashClock = mutableClock(1_000);
        const setup = openStore(crash.databasePath, crashClock.read);
        let crashLease;
        try {
          crashLease = createRunningWorkflow(setup, 'w', 'worker-local', 60_000, base);
        } finally {
          setup.close();
        }

        const worker = spawnWithSignal(fixturePath('local-controller-crash.mjs'), [
          crash.databasePath,
          crashLease.workflowId,
          crashLease.ownerId,
          String(crashLease.fence),
          planPath,
          markerPath,
          stage,
          '1000'
        ]);
        const { marker } = await killAtMarker(worker, markerPath);
        assert.equal(marker.stage, stage, entry.name);

        const recovered = openStore(crash.databasePath, crashClock.read);
        let crashResult;
        try {
          const pending = recovered.getCheckpoint('w');
          assert.equal(pending.sequence, 2, entry.name + ' ' + stage);
          assert.deepEqual(pending.data.baseCheckpointData, base, entry.name + ' ' + stage);
          assert.equal(inspectWorkflowRecovery({ store: recovered, workflowId: 'w' }).kind,
            'local-replay-candidate', entry.name + ' ' + stage);
          crashResult = await runBoundedLocalPlan({
            store: recovered,
            workflowId: 'w',
            ownerId: crashLease.ownerId,
            fence: crashLease.fence
          });
          assert.equal(crashResult.status, 'completed', entry.name + ' ' + stage);
          assert.deepEqual(crashResult.results.map(item => item.output), [3, 5], entry.name + ' ' + stage);
          const completed = recovered.getCheckpoint('w');
          assert.equal(completed.sequence, 6, entry.name + ' ' + stage);
          assert.deepEqual(completed.data.baseCheckpointData, base, entry.name + ' ' + stage);
          assert.equal(completed.data.budget.stepsAdmitted, 2, entry.name + ' ' + stage);
          assert.equal(completed.data.budget.stepAttempts, 3, entry.name + ' ' + stage);
          assert.equal(completed.data.budget.localRetries, 1, entry.name + ' ' + stage);
        } finally {
          recovered.close();
        }

        const crashReopened = openStore(crash.databasePath, crashClock.read);
        try {
          assert.equal(inspectWorkflowRecovery({ store: crashReopened, workflowId: 'w' }).kind,
            'durable-evidence-present', entry.name + ' ' + stage);
          const beforeReuse = snapshotDatabase(crash.databasePath);
          const reused = await runBoundedLocalPlan({
            store: crashReopened,
            workflowId: 'w',
            ownerId: crashLease.ownerId,
            fence: crashLease.fence
          });
          assert.equal(reused.resultHash, crashResult.resultHash, entry.name + ' ' + stage);
          assert.equal(snapshotDatabase(crash.databasePath), beforeReuse, entry.name + ' ' + stage + ' reuse');
        } finally {
          crashReopened.close();
        }
      }
    }
  }
});

test('future reservation covers replay labels, counter growth, UTF-8 results, and real SIGKILL recovery', async t => {
  const planJson = tenStepEscapedTextPlan();
  const base = {
    opaque: 'original\\\0"é'.repeat(64),
    nested: { stable: true, number: Number.MAX_SAFE_INTEGER + 1 }
  };
  const expectedOutput = ['start"\\\0é'];
  const suffixes = ['\\', '\0', 'é', '"', '\\é', '\0"', 'é\\', '"\0', 'tail'];
  for (const suffix of suffixes) expectedOutput.push(expectedOutput.at(-1) + suffix);

  for (const stage of ['after-admission', 'after-compute']) {
    const temporary = temporaryDatabase('bounded-b5-future-state-crash-' + stage + '-');
    t.after(() => temporary.cleanup());
    const planPath = join(temporary.directory, 'plan.json');
    const markerPath = join(temporary.directory, 'worker.ready');
    writeFileSync(planPath, planJson);
    const setupStore = openStore(temporary.databasePath);
    let lease;
    let result;
    try {
      lease = createRunningWorkflow(setupStore, 'b5-many-' + stage, 'worker-local', 60_000, base);
    } finally {
      setupStore.close();
    }

    const worker = spawnWithSignal(fixturePath('local-controller-crash.mjs'), [
      temporary.databasePath,
      lease.workflowId,
      lease.ownerId,
      String(lease.fence),
      planPath,
      markerPath,
      stage
    ]);
    const { marker } = await killAtMarker(worker, markerPath);
    assert.equal(marker.stage, stage);

    const recovered = openStore(temporary.databasePath);
    try {
      const pending = recovered.getCheckpoint(lease.workflowId);
      assert.deepEqual(pending.data.baseCheckpointData, base, stage + ' opaque base preserved');
      assert.equal(pending.data.budget.stepsAdmitted, 1, stage);
      assert.equal(pending.data.budget.stepAttempts, 1, stage);
      assert.equal(pending.data.budget.localRetries, 0, stage);
      assert.equal(inspectWorkflowRecovery({ store: recovered, workflowId: lease.workflowId }).kind,
        'local-replay-candidate', stage);

      result = await runBoundedLocalPlan({
        store: recovered,
        workflowId: lease.workflowId,
        ownerId: lease.ownerId,
        fence: lease.fence
      });
      assert.equal(result.status, 'completed', stage + ': ' + JSON.stringify(result));
      assert.deepEqual(result.results.map(item => item.output), expectedOutput, stage);
      const completed = recovered.getCheckpoint(lease.workflowId);
      assert.equal(completed.data.budget.stepsAdmitted, 10, stage);
      assert.equal(completed.data.budget.stepAttempts, 11, stage);
      assert.equal(completed.data.budget.localRetries, 1, stage);
      assert.equal(completed.data.budget.iterations, 11, stage);
      assert.deepEqual(completed.data.baseCheckpointData, base, stage + ' opaque base preserved');
      assert.equal(inspectWorkflowRecovery({ store: recovered, workflowId: lease.workflowId }).kind,
        'durable-evidence-present', stage);
    } finally {
      recovered.close();
    }

    const reopened = openStore(temporary.databasePath);
    try {
      const beforeReuse = snapshotDatabase(temporary.databasePath);
      const reused = await runBoundedLocalPlan({
        store: reopened,
        workflowId: lease.workflowId,
        ownerId: lease.ownerId,
        fence: lease.fence
      });
      assert.equal(reused.status, 'completed', stage);
      assert.equal(reused.resultHash, result.resultHash, stage);
      assert.equal(snapshotDatabase(temporary.databasePath), beforeReuse, stage + ' all-table reuse');
    } finally {
      reopened.close();
    }
  }
});

test('a replayed step can be followed by a later ordinary step admission', async t => {
  const temporary = temporaryDatabase('bounded-replay-followed-by-step-');
  t.after(() => temporary.cleanup());
  const planPath = join(temporary.directory, 'plan.json');
  const markerPath = join(temporary.directory, 'worker.ready');
  writeFileSync(planPath, DEPENDENT_PLAN);
  const setup = openStore(temporary.databasePath);
  const lease = createRunningWorkflow(setup, 'replay-followed-by-step-run');
  setup.close();

  const worker = spawnWithSignal(fixturePath('local-controller-crash.mjs'), [
    temporary.databasePath,
    lease.workflowId,
    lease.ownerId,
    String(lease.fence),
    planPath,
    markerPath,
    'after-admission'
  ]);
  const { marker } = await killAtMarker(worker, markerPath);
  assert.equal(marker.stage, 'after-admission');

  const recovered = openStore(temporary.databasePath);
  assert.equal(inspectWorkflowRecovery({ store: recovered, workflowId: lease.workflowId }).kind,
    'local-replay-candidate');
  const result = await runBoundedLocalPlan({
    store: recovered,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence
  });
  assert.equal(result.status, 'completed');
  assert.deepEqual(result.results.map(record => record.output), [5, 9]);
  const checkpoint = recovered.getCheckpoint(lease.workflowId);
  assert.equal(checkpoint.data.lastAdmissionKind, 'step');
  assert.equal(checkpoint.data.pendingStepId, null);
  assert.equal(checkpoint.data.budget.stepsAdmitted, 2);
  assert.equal(checkpoint.data.budget.stepAttempts, 3);
  assert.equal(checkpoint.data.budget.localRetries, 1);
  assert.equal(inspectWorkflowRecovery({ store: recovered, workflowId: lease.workflowId }).kind,
    'durable-evidence-present');
  const eventsAtCompletion = recovered.listEvents({ workflowId: lease.workflowId }).length;
  const repeated = await runBoundedLocalPlan({
    store: recovered,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence
  });
  assert.equal(repeated.resultHash, result.resultHash);
  assert.equal(recovered.listEvents({ workflowId: lease.workflowId }).length, eventsAtCompletion);
  recovered.close();
});

test('admission and cancellation races use one durable CAS winner; a stale lease cannot commit', async t => {
  const temporary = temporaryDatabase('bounded-admission-cas-');
  t.after(() => temporary.cleanup());
  const setup = openStore(temporary.databasePath);
  const lease = createRunningWorkflow(setup, 'admission-cas-run');
  const snapshot = setup._localControllerSnapshot(LOCAL_CONTROLLER_STORAGE_CAPABILITY, lease.workflowId);
  const { limitSet, plan } = storeTestPlan({ workflowId: lease.workflowId });
  const now = setup._localControllerNow(LOCAL_CONTROLLER_STORAGE_CAPABILITY);
  const initial = createBudgetSnapshot(limitSet, now, now + getLimitValues(limitSet).maxElapsedMs);
  const stepId = getNextReadyStep(plan, '{}');
  const nextBudget = advanceBudget(initial, 'step', now, 0, limitSet).state;
  const state = buildInitialPrivateCheckpoint({
    workflowId: lease.workflowId,
    planJson: SINGLE_PLAN,
    plan,
    limitSet,
    baseCheckpointData: snapshot.checkpoint.data,
    budget: nextBudget,
    stepId
  });
  const payloadPath = join(temporary.directory, 'admission.json');
  writeFileSync(payloadPath, JSON.stringify({
    expectedRevision: snapshot.workflow.revision,
    expectedCheckpointSequence: snapshot.checkpoint.sequence,
    stepId,
    admissionKind: 'step',
    eventId: 'admission-cas:admit',
    controllerState: state
  }));
  setup.close();

  const gate = join(temporary.directory, 'release-admission-race');
  const workers = [0, 1].map(index => startRaceWorker({
    mode: 'admit',
    databasePath: temporary.databasePath,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    payloadPath,
    gatePath: gate,
    workerId: 'admit-' + index,
    resultPath: join(temporary.directory, 'admit-' + index + '.json')
  }));
  await waitForFiles([gate + '.admit-0.ready', gate + '.admit-1.ready']);
  writeFileSync(gate, 'go');
  const results = await Promise.all(workers.map(worker => worker.done));
  const records = results.map((result, index) => {
    assert.ok([0, 2].includes(result.code));
    return JSON.parse(readFileSync(join(temporary.directory, 'admit-' + index + '.json'), 'utf8'));
  });
  assert.equal(records.filter(record => record.accepted).length, 1, JSON.stringify({ records, results }));
  assert.equal(records.filter(record => !record.accepted && record.code === 'CONCURRENT_MODIFICATION').length, 1);

  const after = openStore(temporary.databasePath);
  assert.equal(after.getWorkflow(lease.workflowId).checkpointSequence, 2);
  assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
  after.close();
});

test('separate processes race local admission against current-fence cancellation', async t => {
  for (const order of ['concurrent', 'admit-first', 'cancel-first']) {
    const temporary = temporaryDatabase('bounded-admission-cancel-' + order + '-');
    t.after(() => temporary.cleanup());
    const setup = openStore(temporary.databasePath);
    const lease = createRunningWorkflow(setup, 'admission-cancel-' + order);
    const snapshot = setup._localControllerSnapshot(LOCAL_CONTROLLER_STORAGE_CAPABILITY, lease.workflowId);
    const { limitSet, plan } = storeTestPlan({ workflowId: lease.workflowId });
    const now = setup._localControllerNow(LOCAL_CONTROLLER_STORAGE_CAPABILITY);
    const initial = createBudgetSnapshot(limitSet, now, now + getLimitValues(limitSet).maxElapsedMs);
    const stepId = getNextReadyStep(plan, '{}');
    const budget = advanceBudget(initial, 'step', now, 0, limitSet).state;
    const controllerState = buildInitialPrivateCheckpoint({
      workflowId: lease.workflowId,
      planJson: SINGLE_PLAN,
      plan,
      limitSet,
      baseCheckpointData: snapshot.checkpoint.data,
      budget,
      stepId
    });
    const admitPath = join(temporary.directory, 'admit.json');
    const cancelPath = join(temporary.directory, 'cancel.json');
    writeFileSync(admitPath, JSON.stringify({
      expectedRevision: snapshot.workflow.revision,
      expectedCheckpointSequence: snapshot.checkpoint.sequence,
      stepId,
      admissionKind: 'step',
      eventId: 'admission-cancel:' + order + ':admit',
      controllerState
    }));
    writeFileSync(cancelPath, JSON.stringify({
      expectedRevision: snapshot.workflow.revision,
      eventId: 'admission-cancel:' + order + ':cancel'
    }));
    setup.close();

    const admitGate = join(temporary.directory, 'release-admit');
    const cancelGate = join(temporary.directory, 'release-cancel');
    const workers = [
      startRaceWorker({
        mode: 'admit',
        databasePath: temporary.databasePath,
        workflowId: lease.workflowId,
        ownerId: lease.ownerId,
        fence: lease.fence,
        payloadPath: admitPath,
        gatePath: admitGate,
        workerId: 'admit',
        resultPath: join(temporary.directory, 'admit-result.json')
      }),
      startRaceWorker({
        mode: 'cancel',
        databasePath: temporary.databasePath,
        workflowId: lease.workflowId,
        ownerId: lease.ownerId,
        fence: lease.fence,
        payloadPath: cancelPath,
        gatePath: cancelGate,
        workerId: 'cancel',
        resultPath: join(temporary.directory, 'cancel-result.json')
      })
    ];
    await waitForFiles([admitGate + '.admit.ready', cancelGate + '.cancel.ready']);
    if (order === 'concurrent') {
      writeFileSync(admitGate, 'go');
      writeFileSync(cancelGate, 'go');
    } else {
      const first = order === 'admit-first' ? workers[0] : workers[1];
      const second = order === 'admit-first' ? workers[1] : workers[0];
      writeFileSync(order === 'admit-first' ? admitGate : cancelGate, 'go');
      await first.done;
      writeFileSync(order === 'admit-first' ? cancelGate : admitGate, 'go');
      await second.done;
    }
    const exits = await Promise.all(workers.map(worker => worker.done));
    const records = ['admit', 'cancel'].map(name => ({
      name,
      ...JSON.parse(readFileSync(join(temporary.directory, name + '-result.json'), 'utf8'))
    }));
    assert.ok(exits.every(result => result.code === 0 || result.code === 2), JSON.stringify(exits));
    assert.equal(records.filter(record => record.accepted).length, 1, `${order}: ${JSON.stringify(records)}`);
    if (order === 'admit-first') assert.equal(records.find(record => record.accepted).name, 'admit');
    if (order === 'cancel-first') assert.equal(records.find(record => record.accepted).name, 'cancel');

    const after = openStore(temporary.databasePath);
    const workflow = after.getWorkflow(lease.workflowId);
    const checkpoint = after.getCheckpoint(lease.workflowId);
    const winner = records.find(record => record.accepted).name;
    assert.equal(workflow.cancellationRequested, winner === 'cancel', order);
    assert.equal(workflow.state, winner === 'cancel' ? 'cancelled' : 'running', order);
    assert.equal(checkpoint.sequence, winner === 'admit' ? 2 : 1, order);
    const winnerEventId = 'admission-cancel:' + order + ':' + winner;
    const eventRows = after.listEvents({ workflowId: lease.workflowId })
      .filter(event => event.eventId.startsWith('admission-cancel:' + order + ':'));
    assert.deepEqual(eventRows.map(event => event.eventId), [winnerEventId], order);
    const db = new DatabaseSync(temporary.databasePath);
    try {
      const outboxRows = db.prepare(
        'SELECT event_id, event_sequence FROM outbox WHERE event_id LIKE ? ORDER BY event_id'
      ).all('admission-cancel:' + order + ':%');
      assert.deepEqual(outboxRows.map(row => row.event_id), [winnerEventId], order);
      assert.equal(outboxRows[0].event_sequence, eventRows[0].sequence, order);
    } finally {
      db.close();
    }
    assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0, order);
    after.close();
  }
});

test('durable store restore validation rejects replay history without a consumed retry', async t => {
  for (const snapshotKind of ['pending', 'completed']) {
    const temporary = temporaryDatabase('bounded-orphan-replay-' + snapshotKind + '-');
    t.after(() => temporary.cleanup());
    const setup = openStore(temporary.databasePath);
    const lease = createRunningWorkflow(setup, 'orphan-replay-' + snapshotKind);
    const admission = makeAdmission(
      setup,
      lease,
      SINGLE_PLAN,
      '{}',
      'orphan-replay:' + snapshotKind + ':admit'
    );
    let savedState = admission.state;
    if (snapshotKind === 'completed') {
      const completion = makeCompletion(
        setup,
        lease,
        admission,
        'orphan-replay:' + snapshotKind + ':complete'
      );
      setup._localControllerComplete(LOCAL_CONTROLLER_STORAGE_CAPABILITY, completion.input);
      savedState = completion.controllerState;
    }
    const storedCheckpoint = setup.getCheckpoint(lease.workflowId);
    setup.close();

    // This file-backed fixture models corrupt restored bytes. Dropping the
    // immutable trigger here is test setup only, not an external attack claim.
    rewriteLatestPrivateCheckpoint(temporary.databasePath, lease.workflowId, {
      ...storedCheckpoint.data,
      lastAdmissionKind: 'replay'
    });
    const restored = openStore(temporary.databasePath);
    const before = snapshotDatabase(temporary.databasePath);
    const workflow = restored.getWorkflow(lease.workflowId);
    const checkpoint = restored.getCheckpoint(lease.workflowId);
    assert.equal(checkpoint.data.budget.localRetries, 0, snapshotKind);
    const candidate = { ...savedState, lastAdmissionKind: 'step' };
    const attempt = () => {
      if (snapshotKind === 'pending') {
        restored._localControllerAdmit(LOCAL_CONTROLLER_STORAGE_CAPABILITY, {
          workflowId: lease.workflowId,
          ownerId: lease.ownerId,
          fence: lease.fence,
          expectedRevision: workflow.revision,
          expectedCheckpointSequence: checkpoint.sequence,
          stepId: admission.stepId,
          admissionKind: 'replay',
          eventId: 'orphan-replay:' + snapshotKind + ':retry',
          controllerState: candidate
        });
      } else {
        restored._localControllerComplete(LOCAL_CONTROLLER_STORAGE_CAPABILITY, {
          workflowId: lease.workflowId,
          ownerId: lease.ownerId,
          fence: lease.fence,
          expectedRevision: workflow.revision,
          expectedCheckpointSequence: checkpoint.sequence,
          stepId: admission.stepId,
          eventId: 'orphan-replay:' + snapshotKind + ':repeat',
          controllerState: candidate
        });
      }
    };
    assert.throws(attempt, error => error?.code === 'CHECKPOINT_CORRUPT', snapshotKind);
    assert.equal(snapshotDatabase(temporary.databasePath), before, snapshotKind);
    restored.close();
  }
});

test('completion racing cancellation and lease takeover cannot resurrect stale work', async t => {
  const temporary = temporaryDatabase('bounded-cancel-race-');
  t.after(() => temporary.cleanup());
  const setup = openStore(temporary.databasePath);
  const lease = createRunningWorkflow(setup, 'completion-cancel-run');
  const admission = makeAdmission(setup, lease, SINGLE_PLAN, '{}', 'cancel-race:admit');
  const completion = makeCompletion(setup, lease, admission, 'cancel-race:complete');
  const completePath = join(temporary.directory, 'completion.json');
  const cancelPath = join(temporary.directory, 'cancel.json');
  writeFileSync(completePath, JSON.stringify(completion.input));
  writeFileSync(cancelPath, JSON.stringify({
    expectedRevision: completion.input.expectedRevision,
    eventId: 'cancel-race:cancel'
  }));
  setup.close();

  const gate = join(temporary.directory, 'release-cancel-complete');
  const workers = [
    startRaceWorker({
      mode: 'complete', databasePath: temporary.databasePath, workflowId: lease.workflowId,
      ownerId: lease.ownerId, fence: lease.fence, payloadPath: completePath, gatePath: gate,
      workerId: 'complete', resultPath: join(temporary.directory, 'complete-result.json')
    }),
    startRaceWorker({
      mode: 'cancel', databasePath: temporary.databasePath, workflowId: lease.workflowId,
      ownerId: lease.ownerId, fence: lease.fence, payloadPath: cancelPath, gatePath: gate,
      workerId: 'cancel', resultPath: join(temporary.directory, 'cancel-result.json')
    })
  ];
  await waitForFiles([gate + '.complete.ready', gate + '.cancel.ready']);
  writeFileSync(gate, 'go');
  const results = await Promise.all(workers.map(worker => worker.done));
  const records = results.map((result, index) => {
    assert.ok([0, 2].includes(result.code));
    const name = index === 0 ? 'complete-result.json' : 'cancel-result.json';
    return JSON.parse(readFileSync(join(temporary.directory, name), 'utf8'));
  });
  assert.equal(records.filter(record => record.accepted).length, 1);

  const after = openStore(temporary.databasePath);
  const workflow = after.getWorkflow(lease.workflowId);
  const state = after.getCheckpoint(lease.workflowId).data;
  if (workflow.cancellationRequested) {
    assert.equal(workflow.state, 'cancelled');
    assert.equal(state.pendingStepId, 'only');
    assert.equal(state.budget.stepAttempts, 1);
  } else {
    assert.equal(workflow.state, 'running');
    assert.equal(state.pendingStepId, null);
    assert.equal(state.budget.stepAttempts, 1);
    assert.equal(JSON.parse(state.completedResultsJson).only.output, 9);
  }
  const postAdmissionEvents = after.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.eventId === 'cancel-race:complete' || event.eventId === 'cancel-race:cancel');
  assert.equal(postAdmissionEvents.length, 1);
  after.close();

  const takeover = temporaryDatabase('bounded-lease-takeover-');
  t.after(() => takeover.cleanup());
  const expiring = openStore(takeover.databasePath);
  const oldLease = createRunningWorkflow(expiring, 'lease-takeover-run', 'worker-old', 1_000);
  const pending = makeAdmission(expiring, oldLease, SINGLE_PLAN, '{}', 'lease-race:admit');
  const staleCompletion = makeCompletion(expiring, oldLease, pending, 'lease-race:stale-complete');
  const stalePath = join(takeover.directory, 'stale.json');
  const replacementPath = join(takeover.directory, 'replacement.json');
  writeFileSync(stalePath, JSON.stringify(staleCompletion.input));
  writeFileSync(replacementPath, JSON.stringify({ eventId: 'lease-race:replacement' }));
  expiring.close();
  await new Promise(resolve => setTimeout(resolve, 1_100));

  const takeoverGate = join(takeover.directory, 'release-takeover-race');
  const takeoverWorkers = [
    startRaceWorker({
      mode: 'complete', databasePath: takeover.databasePath, workflowId: oldLease.workflowId,
      ownerId: oldLease.ownerId, fence: oldLease.fence, payloadPath: stalePath, gatePath: takeoverGate,
      workerId: 'stale-owner', resultPath: join(takeover.directory, 'stale-result.json')
    }),
    startRaceWorker({
      mode: 'takeover', databasePath: takeover.databasePath, workflowId: oldLease.workflowId,
      ownerId: oldLease.ownerId, fence: oldLease.fence, payloadPath: replacementPath, gatePath: takeoverGate,
      workerId: 'replacement-owner', resultPath: join(takeover.directory, 'replacement-result.json'),
      newOwnerId: 'worker-replacement'
    })
  ];
  await waitForFiles([takeoverGate + '.stale-owner.ready', takeoverGate + '.replacement-owner.ready']);
  writeFileSync(takeoverGate, 'go');
  const takeoverResults = await Promise.all(takeoverWorkers.map(worker => worker.done));
  const takeoverRecords = takeoverResults.map((result, index) => {
    assert.ok([0, 2].includes(result.code));
    const name = index === 0 ? 'stale-result.json' : 'replacement-result.json';
    return JSON.parse(readFileSync(join(takeover.directory, name), 'utf8'));
  });
  assert.equal(takeoverRecords.filter(record => record.accepted).length, 1);
  const newStore = openStore(takeover.databasePath);
  assert.equal(newStore.getWorkflow(oldLease.workflowId).leaseOwner, 'worker-replacement');
  assert.equal(newStore.getCheckpoint(oldLease.workflowId).data.pendingStepId, 'only');
  assert.equal(newStore.getCheckpoint(oldLease.workflowId).data.budget.stepAttempts, 1);
  newStore.close();
});

test('cancellation at admission and post-compute barriers fences local completion', async t => {
  for (const stage of ['after-admission', 'after-compute']) {
    const temporary = temporaryDatabase('bounded-cancel-barrier-' + stage + '-');
    t.after(() => temporary.cleanup());
    const store = openStore(temporary.databasePath);
    t.after(() => store.close());
    const lease = createRunningWorkflow(store, 'cancel-barrier-' + stage);
    const run = runBoundedLocalPlan({
      store,
      workflowId: lease.workflowId,
      ownerId: lease.ownerId,
      fence: lease.fence,
      planJson: SINGLE_PLAN
    });
    let cancellation;
    if (stage === 'after-admission') {
      const workflow = store.getWorkflow(lease.workflowId);
      cancellation = store.requestCancellation({
        ...lease,
        expectedRevision: workflow.revision,
        eventId: 'cancel-barrier:' + stage
      });
    } else {
      // runBoundedLocalPlan has already queued its first setImmediate. This
      // callback runs after that boundary's synchronous computation and before
      // the separately queued pre-commit boundary.
      cancellation = await new Promise(resolve => setImmediate(() => {
        const workflow = store.getWorkflow(lease.workflowId);
        resolve(store.requestCancellation({
          ...lease,
          expectedRevision: workflow.revision,
          eventId: 'cancel-barrier:' + stage
        }));
      }));
    }
    assert.equal(cancellation.accepted, true, stage);
    const result = await run;
    assert.equal(result.status, 'stopped', stage);
    assert.equal(['WORKFLOW_TERMINAL', 'CANCELLATION_REQUESTED'].includes(result.reason), true, stage);
    const workflow = store.getWorkflow(lease.workflowId);
    const checkpoint = store.getCheckpoint(lease.workflowId).data;
    assert.equal(workflow.cancellationRequested, true, stage);
    assert.equal(checkpoint.pendingStepId, 'only', stage);
    assert.equal(checkpoint.budget.stepsAdmitted, 1, stage);
    assert.equal(checkpoint.budget.stepAttempts, 1, stage);
    assert.equal(checkpoint.budget.localRetries, 0, stage);
    assert.equal(checkpoint.completedResultsJson, '{}', stage);
    assert.equal(store.listEvents({ workflowId: lease.workflowId })
      .some(event => event.type === 'private_local_step_completed'), false, stage);
    assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0, stage);
  }
});

test('limit deadline and clock rollback remain pinned across recovery without counter reset', async t => {
  const temporary = temporaryDatabase('bounded-clock-recovery-');
  t.after(() => temporary.cleanup());
  const clock = mutableClock(10_000);
  let store = openStore(temporary.databasePath, clock.read);
  const lease = createRunningWorkflow(store, 'clock-deadline-run', 'worker-clock');
  const limitsJson = JSON.stringify({ maxElapsedMs: 100, maxLocalRetries: 1, maxStepAttempts: 2 });
  makeAdmission(store, lease, SINGLE_PLAN, limitsJson, 'clock:admit');
  const checkpointBefore = store.getCheckpoint(lease.workflowId).data;
  assert.equal(checkpointBefore.budget.deadlineWallMs, 10_100);
  clock.set(10_100);
  const expired = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence
  });
  assert.equal(expired.status, 'stopped');
  assert.equal(expired.reason, 'DEADLINE_EXCEEDED');
  const checkpointAfter = store.getCheckpoint(lease.workflowId).data;
  assert.equal(checkpointAfter.budget.deadlineWallMs, 10_100);
  assert.equal(checkpointAfter.budget.stepAttempts, 1);
  assert.equal(checkpointAfter.budget.localRetries, 0);
  store.close();

  store = openStore(temporary.databasePath, clock.read);
  clock.set(9_999);
  const rollback = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence
  });
  assert.equal(rollback.reason, 'CLOCK_ROLLBACK');
  assert.equal(store.getCheckpoint(lease.workflowId).data.budget.stepAttempts, 1);
  store.close();

  const noRetry = temporaryDatabase('bounded-no-retry-');
  t.after(() => noRetry.cleanup());
  const retryStore = openStore(noRetry.databasePath, () => 20_000);
  const retryLease = createRunningWorkflow(retryStore, 'no-retry-run', 'worker-no-retry');
  makeAdmission(retryStore, retryLease, SINGLE_PLAN,
    JSON.stringify({ maxLocalRetries: 0, maxStepAttempts: 1 }), 'no-retry:admit');
  const stopped = await runBoundedLocalPlan({
    store: retryStore,
    workflowId: retryLease.workflowId,
    ownerId: retryLease.ownerId,
    fence: retryLease.fence
  });
  assert.equal(stopped.reason, 'LOCAL_RETRY_LIMIT');
  assert.equal(retryStore.getCheckpoint(retryLease.workflowId).data.budget.stepAttempts, 1);
  assert.equal(retryStore.getCheckpoint(retryLease.workflowId).data.budget.localRetries, 0);
  retryStore.close();
});

test('admission transaction rejects 1ms, 100ms, and 250ms backward wall-clock samples atomically', async t => {
  for (const rollbackMs of [1, 100, 250]) {
    const start = 70_000;
    const temporary = temporaryDatabase('bounded-admission-clock-rollback-' + rollbackMs + '-');
    t.after(() => temporary.cleanup());
    const clock = mutableClock(start);
    const store = openStore(temporary.databasePath, clock.read);
    const workflowId = 'admission-clock-rollback-' + rollbackMs;
    const lease = createRunningWorkflow(store, workflowId, 'worker-clock-rollback');
    const before = snapshotDatabase(temporary.databasePath);
    const getBoundary = interceptLocalControllerTransactionClock(
      store,
      '_localControllerAdmit',
      temporary.databasePath,
      clock,
      start - rollbackMs
    );

    const invokeTransaction = store['_localControllerAdmit'].bind(store);
    let observedClockGuard = null;
    store['_localControllerAdmit'] = (...args) => {
      try { return invokeTransaction(...args); } catch (error) {
        observedClockGuard = readLocalControllerClockDiagnostic(error);
        throw error;
      }
    };
    const result = await runBoundedLocalPlan({
      store,
      workflowId,
      ownerId: lease.ownerId,
      fence: lease.fence,
      planJson: SINGLE_PLAN
    });
    assert.equal(result.status, 'stopped', 'rollback ' + rollbackMs + 'ms');
    assert.equal(result.reason, 'CLOCK_ROLLBACK', 'rollback ' + rollbackMs + 'ms');
    assert.equal(observedClockGuard, 'clock_rollback');
    assert.equal(getBoundary(), before, 'admission reached transaction before rollback check');
    assert.equal(snapshotDatabase(temporary.databasePath), before, 'all rows roll back at admission');
    store.close();

    const reopened = openStore(temporary.databasePath, () => start);
    assert.equal(snapshotDatabase(temporary.databasePath), before, 'all rows remain unchanged after reopen');
    assert.equal(reopened.getWorkflow(workflowId).revision, lease.revision);
    const checkpoint = reopened.getCheckpoint(workflowId);
    assert.equal(checkpoint.sequence, 1);
    assert.deepEqual(checkpoint.data, { preserved: 'synthetic-original-checkpoint' });
    assert.equal(reopened.listEvents({ workflowId })
      .some(event => event.type === 'private_local_step_admitted'), false);
    reopened.close();
  }
});

test('completion transaction rejects 1ms, 100ms, and 250ms backward wall-clock samples atomically', async t => {
  for (const rollbackMs of [1, 100, 250]) {
    const start = 80_000;
    const temporary = temporaryDatabase('bounded-completion-clock-rollback-' + rollbackMs + '-');
    t.after(() => temporary.cleanup());
    const clock = mutableClock(start);
    const store = openStore(temporary.databasePath, clock.read);
    const workflowId = 'completion-clock-rollback-' + rollbackMs;
    const lease = createRunningWorkflow(store, workflowId, 'worker-clock-rollback');
    makeAdmission(store, lease, SINGLE_PLAN, '{}', workflowId + ':first-admission');
    const getBoundary = interceptLocalControllerTransactionClock(
      store,
      '_localControllerComplete',
      temporary.databasePath,
      clock,
      start - rollbackMs
    );

    const invokeTransaction = store['_localControllerComplete'].bind(store);
    let observedClockGuard = null;
    store['_localControllerComplete'] = (...args) => {
      try { return invokeTransaction(...args); } catch (error) {
        observedClockGuard = readLocalControllerClockDiagnostic(error);
        throw error;
      }
    };
    const result = await runBoundedLocalPlan({
      store,
      workflowId,
      ownerId: lease.ownerId,
      fence: lease.fence
    });
    assert.equal(result.status, 'stopped', 'rollback ' + rollbackMs + 'ms');
    assert.equal(result.reason, 'CLOCK_ROLLBACK', 'rollback ' + rollbackMs + 'ms');
    assert.equal(observedClockGuard, 'clock_rollback');
    const beforeCompletion = getBoundary();
    assert.equal(typeof beforeCompletion, 'string', 'completion reached transaction after controller check');
    assert.equal(snapshotDatabase(temporary.databasePath), beforeCompletion, 'completion transaction rolls back');
    store.close();

    const reopened = openStore(temporary.databasePath, () => start);
    assert.equal(snapshotDatabase(temporary.databasePath), beforeCompletion, 'completion state is unchanged after reopen');
    const checkpoint = reopened.getCheckpoint(workflowId);
    assert.equal(checkpoint.sequence, 3, 'the replay admission remains consumed');
    assert.equal(checkpoint.data.pendingStepId, 'only');
    assert.equal(checkpoint.data.budget.stepsAdmitted, 1);
    assert.equal(checkpoint.data.budget.stepAttempts, 2);
    assert.equal(checkpoint.data.budget.localRetries, 1);
    assert.equal(checkpoint.data.completedResultsJson, '{}');
    assert.equal(reopened.listEvents({ workflowId })
      .some(event => event.type === 'private_local_step_completed'), false);
    reopened.close();
  }
});

test('transaction clock keeps unchanged and forward-expired admission and completion controls', async t => {
  async function runControl(boundary, adjustment) {
    const start = boundary === '_localControllerAdmit' ? 90_000 : 100_000;
    const temporary = temporaryDatabase('bounded-transaction-clock-control-' + boundary + '-' + adjustment + '-');
    t.after(() => temporary.cleanup());
    const clock = mutableClock(start);
    const store = openStore(temporary.databasePath, clock.read);
    const workflowId = 'transaction-clock-control-' + boundary + '-' + adjustment;
    const lease = createRunningWorkflow(store, workflowId, 'worker-clock-control');
    if (boundary === '_localControllerComplete') {
      makeAdmission(store, lease, SINGLE_PLAN, '{}', workflowId + ':first-admission');
    }
    const target = adjustment === 'deadline' ? start + 30_000 : start + adjustment;
    const getBoundary = interceptLocalControllerTransactionClock(
      store,
      boundary,
      temporary.databasePath,
      clock,
      target
    );
    const invokeTransaction = store[boundary].bind(store);
    let observedClockGuard = null;
    store[boundary] = (...args) => {
      try { return invokeTransaction(...args); } catch (error) {
        observedClockGuard = readLocalControllerClockDiagnostic(error);
        throw error; // Passive read does not consume BUSY eligibility.
      }
    };
    const result = await runBoundedLocalPlan({
      store,
      workflowId,
      ownerId: lease.ownerId,
      fence: lease.fence,
      ...(boundary === '_localControllerAdmit' ? { planJson: SINGLE_PLAN } : {})
    });
    assert.equal(typeof getBoundary(), 'string');

    if (adjustment === 'deadline') {
      assert.equal(result.status, 'stopped');
      assert.equal(observedClockGuard, 'deadline_and_age');
      assert.equal(snapshotDatabase(temporary.databasePath), getBoundary(), 'expired transaction writes nothing');
      store.close();
      const reopened = openStore(temporary.databasePath, () => start);
      assert.equal(snapshotDatabase(temporary.databasePath), getBoundary(), 'expired state survives reopen unchanged');
      const checkpoint = reopened.getCheckpoint(workflowId);
      if (boundary === '_localControllerComplete') {
        assert.equal(checkpoint.data.pendingStepId, 'only');
        assert.equal(checkpoint.data.budget.stepAttempts, 2);
        assert.equal(checkpoint.data.budget.localRetries, 1);
      } else {
        assert.equal(checkpoint.sequence, 1);
        assert.deepEqual(checkpoint.data, { preserved: 'synthetic-original-checkpoint' });
      }
      reopened.close();
      return;
    }

    if (adjustment === 251) {
      assert.equal(result.status, 'stopped');
      assert.equal(result.reason, 'CONTROLLER_INPUT_INVALID');
      assert.equal(observedClockGuard, 'snapshot_age_exceeded');
      assert.equal(snapshotDatabase(temporary.databasePath), getBoundary(), 'excess forward drift writes nothing');
      store.close();
      const reopened = openStore(temporary.databasePath, () => start);
      assert.equal(snapshotDatabase(temporary.databasePath), getBoundary(), 'excess forward drift remains unchanged after reopen');
      reopened.close();
      return;
    }

    assert.equal(observedClockGuard, null);
    assert.equal(result.status, 'completed', 'transaction adjustment ' + adjustment);
    assert.equal(result.results[0].output, 9);
    store.close();
  }

  for (const boundary of ['_localControllerAdmit', '_localControllerComplete']) {
    await runControl(boundary, 0);
    await runControl(boundary, 1);
    await runControl(boundary, 250);
    await runControl(boundary, 251);
    await runControl(boundary, 'deadline');
  }
});

test('a fresh controller completion survives a waited SQLite writer lock without accepting stale input', async t => {
  const temporary = temporaryDatabase('bounded-completion-clock-contention-');
  const store = openStore(temporary.databasePath, Date.now, 2_000);
  let blocker = null;
  t.after(async () => {
    await blocker?.cleanup();
    store.close();
    temporary.cleanup();
  });
  const lease = createRunningWorkflow(store, 'completion-clock-contention');
  makeAdmission(store, lease, SINGLE_PLAN, '{}', lease.workflowId + ':first-admission');
  const originalComplete = store._localControllerComplete.bind(store);
  const attempted = [];
  let firstError = null;
  let stateBeforeFirstAttempt = null;
  let stateAfterRejectedAttempt = null;
  store._localControllerComplete = (...args) => {
    attempted.push(args[1]);
    if (attempted.length === 1) {
      stateBeforeFirstAttempt = snapshotDatabase(temporary.databasePath);
      const stateBuffer = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT * 2);
      const state = new Int32Array(stateBuffer);
      const worker = new Worker(`
        const { workerData } = require('node:worker_threads');
        const { DatabaseSync } = require('node:sqlite');
        const state = new Int32Array(workerData.stateBuffer);
        let db;
        try {
          db = new DatabaseSync(workerData.databasePath, { timeout: 1000 });
          db.exec('BEGIN IMMEDIATE');
          Atomics.store(state, 0, 1);
          Atomics.notify(state, 0);
          Atomics.wait(state, 1, 0, workerData.holdMs);
          db.exec('ROLLBACK');
          db.close();
          db = null;
          Atomics.store(state, 0, 2);
          Atomics.notify(state, 0);
        } catch {
          Atomics.store(state, 0, -1);
          Atomics.notify(state, 0);
          throw new Error('synthetic SQLite writer failed');
        } finally {
          try { if (db?.isTransaction) db.exec('ROLLBACK'); } catch {}
          try { db?.close(); } catch {}
        }
      `, { eval: true, workerData: {
        databasePath: temporary.databasePath, stateBuffer, holdMs: 320
      } });
      const exited = new Promise(resolve => worker.once('exit', resolve));
      const deadline = Date.now() + 5_000;
      while (Atomics.load(state, 0) !== 1) {
        const current = Atomics.load(state, 0);
        if (current === -1) throw new Error('The synthetic SQLite writer failed.');
        const remaining = deadline - Date.now();
        if (remaining <= 0 || Atomics.wait(state, 0, current, remaining) === 'timed-out') {
          throw new Error('The synthetic SQLite writer exceeded its setup bound.');
        }
      }
      blocker = { exited, cleanup: () => worker.terminate() };
    }
    try {
      return originalComplete(...args);
    } catch (error) {
      if (attempted.length === 1) {
        firstError = error;
        stateAfterRejectedAttempt = snapshotDatabase(temporary.databasePath);
      }
      throw error;
    }
  };

  const result = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence
  });
  await blocker.exited;
  assert.equal(result.status, 'completed', `The controller stopped with ${result.reason}.`);
  assert.equal(attempted.length, 2, 'one exact completion refresh follows the waited writer lock');
  assert.equal(firstError?.code, 'CONCURRENT_MODIFICATION');
  assert.equal(readLocalControllerClockDiagnostic(firstError), 'snapshot_age_exceeded');
  assert.equal(firstError?.operation, '_localControllerComplete');
  assert.equal(firstError?.retryable, true);
  assert.equal(stateAfterRejectedAttempt, stateBeforeFirstAttempt,
    'the typed stale-snapshot rejection rolls back without changing durable state');
  assert.equal(attempted[0].eventId, attempted[1].eventId,
    'the refreshed completion keeps the original event identity');
  assert.equal(attempted[0].controllerState.completedResultsJson,
    attempted[1].controllerState.completedResultsJson,
    'the refreshed completion preserves the exact computed local result');
  const checkpoint = store.getCheckpoint(lease.workflowId);
  assert.equal(checkpoint.data.pendingStepId, null);
  assert.equal(JSON.parse(checkpoint.data.completedResultsJson).only.output, 9);
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_completed').length, 1);
  assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
});

test('a stale completion refresh does not start a second transaction after the parent runtime deadline', async t => {
  const temporary = temporaryDatabase('bounded-completion-parent-deadline-');
  const store = openStore(temporary.databasePath, Date.now, 250);
  let blocker = null;
  t.after(async () => {
    await blocker?.cleanup();
    store.close();
    temporary.cleanup();
  });
  const lease = createRunningWorkflow(store, 'completion-parent-deadline');
  makeAdmission(store, lease, SINGLE_PLAN, '{}', lease.workflowId + ':first-admission');
  const originalComplete = store._localControllerComplete.bind(store);
  const attempted = [];
  const callTimesMsAfterDeadline = [];
  const deadlineNs = process.hrtime.bigint() + 100_000_000n;
  let firstError = null;
  let stateBeforeFirstAttempt = null;
  let stateAfterRejectedAttempt = null;
  let checkpointAfterRejectedAttempt = null;
  store._localControllerComplete = (...args) => {
    attempted.push(args[1]);
    callTimesMsAfterDeadline.push(Number(process.hrtime.bigint() - deadlineNs) / 1e6);
    if (attempted.length === 1) {
      stateBeforeFirstAttempt = snapshotDatabase(temporary.databasePath);
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 140);
      const stateBuffer = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT * 2);
      const state = new Int32Array(stateBuffer);
      const worker = new Worker(`
        const { workerData } = require('node:worker_threads');
        const { DatabaseSync } = require('node:sqlite');
        const state = new Int32Array(workerData.stateBuffer);
        let db;
        try {
          db = new DatabaseSync(workerData.databasePath, { timeout: 1000 });
          db.exec('BEGIN IMMEDIATE');
          Atomics.store(state, 0, 1);
          Atomics.notify(state, 0);
          Atomics.wait(state, 1, 0, workerData.holdMs);
          db.exec('ROLLBACK');
          db.close();
          db = null;
          Atomics.store(state, 0, 2);
          Atomics.notify(state, 0);
        } catch {
          Atomics.store(state, 0, -1);
          Atomics.notify(state, 0);
          throw new Error('synthetic SQLite writer failed');
        } finally {
          try { if (db?.isTransaction) db.exec('ROLLBACK'); } catch {}
          try { db?.close(); } catch {}
        }
      `, { eval: true, workerData: {
        databasePath: temporary.databasePath, stateBuffer, holdMs: 200
      } });
      const exited = new Promise(resolve => worker.once('exit', resolve));
      const setupDeadline = Date.now() + 5_000;
      while (Atomics.load(state, 0) !== 1) {
        const current = Atomics.load(state, 0);
        if (current === -1) throw new Error('The synthetic SQLite writer failed.');
        const remaining = setupDeadline - Date.now();
        if (remaining <= 0 || Atomics.wait(state, 0, current, remaining) === 'timed-out') {
          throw new Error('The synthetic SQLite writer exceeded its setup bound.');
        }
      }
      blocker = { exited, cleanup: () => worker.terminate() };
    }
    try {
      return originalComplete(...args);
    } catch (error) {
      if (attempted.length === 1) {
        firstError = error;
        stateAfterRejectedAttempt = snapshotDatabase(temporary.databasePath);
        checkpointAfterRejectedAttempt = store.getCheckpoint(lease.workflowId);
      }
      throw error;
    }
  };

  const result = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    busyTimeoutMs: 250,
    runtimeDeadlineNs: deadlineNs
  });
  await blocker?.exited;

  assert.ok(callTimesMsAfterDeadline[0] < 0, 'the first completion begins before the parent deadline');
  assert.ok(process.hrtime.bigint() >= deadlineNs, 'the parent deadline expires while completion is blocked');
  assert.ok(store.getWorkflow(lease.workflowId).leaseExpiresAtMs > Date.now(), 'the workflow lease remains valid');
  assert.equal(result.status, 'stopped');
  assert.equal(result.reason, 'LOCAL_COMPLETION_BUSY');
  assert.equal(attempted.length, 1, 'no second completion transaction starts after parent deadline expiry');
  assert.equal(callTimesMsAfterDeadline.length, 1);
  assert.equal(firstError?.code, 'CONCURRENT_MODIFICATION');
  assert.equal(readLocalControllerClockDiagnostic(firstError), 'snapshot_age_exceeded');
  assert.equal(stateAfterRejectedAttempt, stateBeforeFirstAttempt,
    'the stale completion transaction rolls back without changing durable state');
  assert.equal(snapshotDatabase(temporary.databasePath), stateAfterRejectedAttempt,
    'deadline rejection does not debit, reserve, or persist another completion');
  const checkpoint = store.getCheckpoint(lease.workflowId);
  assert.equal(checkpoint.data.pendingStepId, 'only');
  assert.equal(checkpoint.data.completedResultsJson, '{}');
  assert.deepEqual(checkpoint, checkpointAfterRejectedAttempt,
    'the post-rejection checkpoint and budget remain unchanged');
  assert.equal(store.listEvents({ workflowId: lease.workflowId })
    .filter(event => event.type === 'private_local_step_completed').length, 0);
  assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
});

test('unmarked stale completion clocks and caller marker injection are rejected without writes', async t => {
  const temporary = temporaryDatabase('bounded-unmarked-completion-clock-');
  const clock = mutableClock(60_000);
  const store = openStore(temporary.databasePath, clock.read);
  t.after(() => {
    store.close();
    temporary.cleanup();
  });
  const lease = createRunningWorkflow(store, 'unmarked-completion-clock');
  const admission = makeAdmission(store, lease, SINGLE_PLAN, '{}', 'unmarked-clock:admit');
  const completion = makeCompletion(store, lease, admission, 'unmarked-clock:complete');
  const before = snapshotDatabase(temporary.databasePath);

  clock.set(clock.read() + 251);
  assert.throws(
    () => store._localControllerComplete(LOCAL_CONTROLLER_STORAGE_CAPABILITY, completion.input),
    error => error instanceof RuntimeError
      && error.operation === '_localControllerComplete'
      && error.code === 'INVALID_INPUT'
  );
  assert.equal(snapshotDatabase(temporary.databasePath), before,
    'an old caller-built checkpoint without private provenance is rejected unchanged');

  const suppliedStoreMarker = {
    ...completion.input,
    completionClockSample: { wallNowMs: clock.read(), monotonicNowNs: process.hrtime.bigint() }
  };
  assert.throws(
    () => store._localControllerComplete(LOCAL_CONTROLLER_STORAGE_CAPABILITY, suppliedStoreMarker),
    error => error instanceof RuntimeError
      && error.operation === '_localControllerComplete'
      && error.code === 'INVALID_INPUT'
  );
  const suppliedCheckpointMarker = {
    ...completion.input,
    controllerState: {
      ...completion.controllerState,
      completionClockSample: { wallNowMs: clock.read(), monotonicNowNs: process.hrtime.bigint() }
    }
  };
  assert.throws(
    () => store._localControllerComplete(LOCAL_CONTROLLER_STORAGE_CAPABILITY, suppliedCheckpointMarker),
    error => error instanceof RuntimeError
      && error.operation === '_localControllerComplete'
      && error.code === 'INVALID_INPUT'
  );
  const controllerMarkerAttempt = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    completionClockSample: { wallNowMs: clock.read(), monotonicNowNs: process.hrtime.bigint() }
  });
  assert.equal(controllerMarkerAttempt.reason, 'CONTROLLER_INPUT_INVALID');
  assert.equal(snapshotDatabase(temporary.databasePath), before,
    'store and controller inputs cannot carry or forge clock provenance');
  assert.equal(store.getCheckpoint(lease.workflowId).data.pendingStepId, admission.stepId);
  assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
});

test('the durable controller stops after final compute crosses the pinned elapsed deadline', async t => {
  const temporary = temporaryDatabase('bounded-final-compute-deadline-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, () => 40_000);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'final-compute-deadline-run');
  const priorNow = performance.now;
  const samples = [0, 0, 0, 10, 10];
  let index = 0;
  performance.now = () => samples[Math.min(index++, samples.length - 1)];
  let result;
  try {
    result = await runBoundedLocalPlan({
      store,
      workflowId: lease.workflowId,
      ownerId: lease.ownerId,
      fence: lease.fence,
      planJson: SINGLE_PLAN,
      limitsJson: '{"maxElapsedMs":5}'
    });
  } finally {
    performance.now = priorNow;
  }
  assert.equal(result.status, 'stopped');
  assert.equal(result.reason, 'ELAPSED_LIMIT');
  const checkpoint = store.getCheckpoint(lease.workflowId);
  assert.equal(checkpoint.data.pendingStepId, 'only');
  assert.equal(checkpoint.data.budget.stepAttempts, 1);
  assert.equal(JSON.parse(checkpoint.data.completedResultsJson).only, undefined);
  assert.equal(store.listEvents({ workflowId: lease.workflowId }).filter(event => event.type === 'private_local_step_completed').length, 0);

  const commitBoundary = temporaryDatabase('bounded-final-commit-deadline-');
  t.after(() => commitBoundary.cleanup());
  const commitStore = openStore(commitBoundary.databasePath, () => 50_000);
  t.after(() => commitStore.close());
  const commitLease = createRunningWorkflow(commitStore, 'final-commit-deadline-run');
  const commitSamples = [0, 0, 0, 0, 0, 0, 10, 10];
  let commitIndex = 0;
  performance.now = () => commitSamples[Math.min(commitIndex++, commitSamples.length - 1)];
  let commitResult;
  try {
    commitResult = await runBoundedLocalPlan({
      store: commitStore,
      workflowId: commitLease.workflowId,
      ownerId: commitLease.ownerId,
      fence: commitLease.fence,
      planJson: SINGLE_PLAN,
      limitsJson: '{"maxElapsedMs":5}'
    });
  } finally {
    performance.now = priorNow;
  }
  assert.equal(commitResult.status, 'stopped');
  assert.equal(commitResult.reason, 'ELAPSED_LIMIT');
  assert.equal(commitStore.getCheckpoint(commitLease.workflowId).data.pendingStepId, 'only');
  assert.equal(commitStore.getCheckpoint(commitLease.workflowId).data.budget.stepAttempts, 1);
  assert.equal(commitStore.listEvents({ workflowId: commitLease.workflowId })
    .filter(event => event.type === 'private_local_step_completed').length, 0);
});

test('SQL failure after each admission and completion write rolls back the actual file database', async t => {
  const writeTargets = ['workflows', 'workflow_checkpoints', 'workflow_events', 'outbox'];
  for (const target of writeTargets) {
    const temporary = temporaryDatabase('bounded-admission-rollback-' + target + '-');
    t.after(() => temporary.cleanup());
    const store = openStore(temporary.databasePath, () => 30_000);
    const lease = createRunningWorkflow(store, 'admission-rollback-' + target);
    const snapshotBefore = snapshotDatabase(temporary.databasePath);
    const admissionInput = makeAdmissionCandidate(store, lease, SINGLE_PLAN, 'rollback:admit:' + target);
    const beforeCommit = snapshotDatabase(temporary.databasePath);
    const trigger = installWriteFailure(temporary.databasePath, target, lease.workflowId, 'admit_' + target);
    assert.throws(() => store._localControllerAdmit(LOCAL_CONTROLLER_STORAGE_CAPABILITY, admissionInput), error => {
      assert.ok(error instanceof RuntimeError);
      assert.equal(error.code, 'STORE_WRITE_FAILED');
      return true;
    });
    dropTrigger(temporary.databasePath, trigger);
    assert.equal(snapshotDatabase(temporary.databasePath), beforeCommit, 'admission rollback at ' + target);
    assert.equal(beforeCommit, snapshotBefore, 'candidate preparation must be side effect free');
    store.close();
    const reopened = openStore(temporary.databasePath, () => 30_000);
    assert.equal(snapshotDatabase(temporary.databasePath), beforeCommit, 'reopened admission rollback at ' + target);
    reopened.close();
  }

  for (const target of writeTargets) {
    const temporary = temporaryDatabase('bounded-completion-rollback-' + target + '-');
    t.after(() => temporary.cleanup());
    const store = openStore(temporary.databasePath, () => 40_000);
    const lease = createRunningWorkflow(store, 'completion-rollback-' + target);
    const admission = makeAdmission(store, lease, SINGLE_PLAN, '{}', 'rollback:seed-admission:' + target);
    const completion = makeCompletion(store, lease, admission, 'rollback:complete:' + target);
    const beforeCommit = snapshotDatabase(temporary.databasePath);
    const trigger = installWriteFailure(temporary.databasePath, target, lease.workflowId, 'complete_' + target);
    assert.throws(() => store._localControllerComplete(LOCAL_CONTROLLER_STORAGE_CAPABILITY, completion.input), error => {
      assert.ok(error instanceof RuntimeError);
      assert.equal(error.code, 'STORE_WRITE_FAILED');
      return true;
    });
    dropTrigger(temporary.databasePath, trigger);
    assert.equal(snapshotDatabase(temporary.databasePath), beforeCommit, 'completion rollback at ' + target);
    store.close();
    const reopened = openStore(temporary.databasePath, () => 40_000);
    assert.equal(snapshotDatabase(temporary.databasePath), beforeCommit, 'reopened completion rollback at ' + target);
    reopened.close();
  }
});

test('duplicate admission and completion event identities cannot add checkpoints after reopen', t => {
  const temporary = temporaryDatabase('bounded-duplicate-event-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, () => 50_000);
  const lease = createRunningWorkflow(store, 'duplicate-event-run');
  const initial = snapshotDatabase(temporary.databasePath);
  const conflictingAdmission = makeAdmissionCandidate(
    store,
    lease,
    SINGLE_PLAN,
    lease.workflowId + ':created'
  );
  assert.throws(
    () => store._localControllerAdmit(LOCAL_CONTROLLER_STORAGE_CAPABILITY, conflictingAdmission),
    error => error.code === 'IDEMPOTENCY_CONFLICT'
  );
  assert.equal(snapshotDatabase(temporary.databasePath), initial);

  const admission = makeAdmission(store, lease, SINGLE_PLAN, '{}', 'duplicate-event:admission');
  const completion = makeCompletion(store, lease, admission, lease.workflowId + ':created');
  const beforeCompletionConflict = snapshotDatabase(temporary.databasePath);
  assert.throws(
    () => store._localControllerComplete(LOCAL_CONTROLLER_STORAGE_CAPABILITY, completion.input),
    error => error.code === 'IDEMPOTENCY_CONFLICT'
  );
  assert.equal(snapshotDatabase(temporary.databasePath), beforeCompletionConflict);
  store.close();

  const reopened = openStore(temporary.databasePath, () => 50_000);
  assert.equal(snapshotDatabase(temporary.databasePath), beforeCompletionConflict);
  assert.equal(reopened.getWorkflow(lease.workflowId).checkpointSequence, 2);
  assert.equal(reopened.getCheckpoint(lease.workflowId).data.pendingStepId, 'only');
  assert.equal(reopened.getCheckpoint(lease.workflowId).data.completedResultsJson, '{}');
  assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
  reopened.close();
});

function makeAdmissionCandidate(store, lease, planJson, eventId) {
  const workflowId = lease.workflowId;
  const snapshot = store._localControllerSnapshot(LOCAL_CONTROLLER_STORAGE_CAPABILITY, workflowId);
  const { limitSet, plan } = storeTestPlan({ workflowId, planJson });
  const now = store._localControllerNow(LOCAL_CONTROLLER_STORAGE_CAPABILITY);
  const budget = createBudgetSnapshot(limitSet, now, now + getLimitValues(limitSet).maxElapsedMs);
  const stepId = getNextReadyStep(plan, '{}');
  const advanced = advanceBudget(budget, 'step', now, 0, limitSet);
  return {
    workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence,
    expectedRevision: snapshot.workflow.revision,
    expectedCheckpointSequence: snapshot.checkpoint.sequence,
    stepId,
    admissionKind: 'step',
    eventId,
    controllerState: buildInitialPrivateCheckpoint({
      workflowId,
      planJson,
      plan,
      limitSet,
      baseCheckpointData: snapshot.checkpoint.data,
      budget: advanced.state,
      stepId
    })
  };
}

function makeReadOnlyRecoveryFixture(store, lease) {
  const existing = store.getCheckpoint(lease.workflowId).data;
  const first = store.recordDispatchIntent({
    ...lease,
    expectedRevision: store.getWorkflow(lease.workflowId).revision,
    attemptId: 'attempt-01-reconciled',
    logicalOperationRef: 'synthetic-logical-01',
    authorityHoldRef: 'synthetic-opaque-hold-01',
    transportAttemptId: 'synthetic-transport-01',
    eventId: 'recovery-page:first-intent'
  });
  const receipt = store.recordResponseReceipt({
    ...lease,
    expectedRevision: first.revision,
    attemptId: 'attempt-01-reconciled',
    receiptId: 'offline-fixture-receipt-01',
    outcome: 'no_effect',
    nextState: 'running',
    responseBytes: Buffer.from('offline recovery pagination fixture'),
    nextCheckpoint: existing,
    eventId: 'recovery-page:first-receipt'
  });
  const reconciled = store.reconcilePendingReceipts({
    ...lease,
    expectedRevision: receipt.revision,
    eventIdPrefix: 'recovery-page:first-reconcile'
  });
  store.recordDispatchIntent({
    ...lease,
    expectedRevision: reconciled.revision,
    attemptId: 'attempt-02-unresolved',
    logicalOperationRef: 'synthetic-logical-02',
    authorityHoldRef: 'synthetic-opaque-hold-02',
    transportAttemptId: 'synthetic-transport-02',
    eventId: 'recovery-page:second-intent'
  });
}

test('pending-attempt inspection has stable bounded keyset pages and explicit incompleteness', t => {
  const temporary = temporaryDatabase('bounded-recovery-pages-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'recovery-page-run');
  makeReadOnlyRecoveryFixture(store, lease);

  const first = inspectWorkflowRecovery({ store, workflowId: lease.workflowId, pageSize: 1 });
  assert.equal(first.kind, 'external-reconciliation-required');
  assert.equal(first.attempts.length, 1);
  assert.equal(first.attempts[0].attemptId, 'attempt-01-reconciled');
  assert.equal(first.hasMore, true);
  assert.equal(first.incomplete, true);
  assert.equal(first.nextAfterAttemptId, 'attempt-01-reconciled');
  const second = inspectWorkflowRecovery({
    store,
    workflowId: lease.workflowId,
    pageSize: 1,
    afterAttemptId: first.nextAfterAttemptId,
    workflowRevision: first.workflowRevision
  });
  assert.equal(second.kind, 'external-reconciliation-required');
  assert.equal(second.attempts.length, 1);
  assert.equal(second.attempts[0].attemptId, 'attempt-02-unresolved');
  assert.equal(second.attempts[0].state, 'intent');
  assert.equal(second.hasMore, false);
  assert.equal(second.incomplete, false);

  store.requestCancellation({
    ...lease,
    expectedRevision: store.getWorkflow(lease.workflowId).revision,
    eventId: 'recovery-page:cancel'
  });
  const staleContinuation = inspectWorkflowRecovery({
    store,
    workflowId: lease.workflowId,
    pageSize: 1,
    afterAttemptId: first.nextAfterAttemptId,
    workflowRevision: first.workflowRevision
  });
  assert.equal(staleContinuation.kind, 'stop');
  assert.equal(staleContinuation.reason, 'WORKFLOW_SNAPSHOT_CHANGED');
  assert.equal(staleContinuation.incomplete, true);
});

test('incomplete attempt pages cannot return a local recovery candidate', t => {
  const temporary = temporaryDatabase('bounded-incomplete-local-candidate-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'incomplete-local-candidate-run');
  makeAdmission(store, lease, SINGLE_PLAN, '{}', 'incomplete-local-candidate:admit');

  for (let index = 1; index <= 2; index += 1) {
    const attemptId = `incomplete-attempt-${index}`;
    const intent = store.recordDispatchIntent({
      ...lease,
      expectedRevision: store.getWorkflow(lease.workflowId).revision,
      attemptId,
      logicalOperationRef: `incomplete-logical-${index}`,
      authorityHoldRef: `incomplete-opaque-hold-${index}`,
      transportAttemptId: `incomplete-transport-${index}`,
      eventId: `incomplete-page:${index}:intent`
    });
    const receipt = store.recordResponseReceipt({
      ...lease,
      expectedRevision: intent.revision,
      attemptId,
      receiptId: `incomplete-fixture-receipt-${index}`,
      outcome: 'no_effect',
      nextState: 'running',
      responseBytes: Buffer.from(`synthetic reconciled fixture ${index}`),
      nextCheckpoint: store.getCheckpoint(lease.workflowId).data,
      eventId: `incomplete-page:${index}:receipt`
    });
    store.reconcilePendingReceipts({
      ...lease,
      expectedRevision: receipt.revision,
      eventIdPrefix: `incomplete-page:${index}:reconcile`
    });
  }

  const page = inspectWorkflowRecovery({ store, workflowId: lease.workflowId, pageSize: 1 });
  assert.equal(page.incomplete, true);
  assert.equal(page.hasMore, true);
  assert.equal(page.attempts.length, 1);
  assert.equal(page.kind, 'stop');
  assert.equal(page.reason, 'RECOVERY_INSPECTION_INCOMPLETE');
  assert.equal(page.actionable, false);
});

test('intent, unknown, and response-received attempts remain barriers with bounded evidence', async t => {
  for (const initialState of ['intent', 'outcome_unknown', 'response_received']) {
    const temporary = temporaryDatabase('bounded-external-barrier-' + initialState + '-');
    t.after(() => temporary.cleanup());
    const store = openStore(temporary.databasePath);
    t.after(() => store.close());
    const lease = createRunningWorkflow(store, 'external-barrier-' + initialState);
    const before = store.getWorkflow(lease.workflowId);
    const attemptId = 'external-barrier-attempt-' + initialState;
    const intent = store.recordDispatchIntent({
      ...lease,
      expectedRevision: before.revision,
      attemptId,
      logicalOperationRef: 'external-barrier-logical-' + initialState,
      authorityHoldRef: 'opaque-hold-must-stay-opaque',
      transportAttemptId: 'external-barrier-transport-' + initialState,
      eventId: 'external-barrier:' + initialState + ':intent'
    });
    if (initialState === 'outcome_unknown') {
      store.markOutcomeUnknown({
        ...lease,
        expectedRevision: intent.revision,
        attemptId,
        eventId: 'external-barrier:' + initialState + ':unknown'
      });
    } else if (initialState === 'response_received') {
      // This is an offline state fixture through the existing store API; it is
      // never returned by the private controller or used to settle the hold.
      store.recordResponseReceipt({
        ...lease,
        expectedRevision: intent.revision,
        attemptId,
        receiptId: 'fixture-receipt-response-received',
        outcome: 'completed',
        nextState: 'running',
        responseBytes: Buffer.from('synthetic existing-store response state'),
        nextCheckpoint: store.getCheckpoint(lease.workflowId).data,
        eventId: 'external-barrier:' + initialState + ':receipt'
      });
    }
    const atBarrier = store.getWorkflow(lease.workflowId);
    const checkpointAtBarrier = store.getCheckpoint(lease.workflowId);
    const rowsAtBarrier = {
      attempts: countRows(temporary.databasePath, 'operation_attempts'),
      receipts: countRows(temporary.databasePath, 'response_receipts')
    };

    const blocked = await runBoundedLocalPlan({
      store,
      workflowId: lease.workflowId,
      ownerId: lease.ownerId,
      fence: lease.fence,
      planJson: SINGLE_PLAN,
      limitsJson: '{"maxRecoveryAttemptStates":1}'
    });
    assert.equal(blocked.status, 'external-reconciliation-required', initialState);
    assert.equal(blocked.reason, 'EXTERNAL_OUTCOME_UNRESOLVED', initialState);
    assert.equal(store.getWorkflow(lease.workflowId).revision, atBarrier.revision, initialState);
    assert.equal(store.getCheckpoint(lease.workflowId).sha256, checkpointAtBarrier.sha256, initialState);
    assert.equal(store.getAttempt(attemptId).state, initialState, initialState);
    assert.equal(store.getAttempt(attemptId).authorityHoldRef, 'opaque-hold-must-stay-opaque', initialState);
    assert.deepEqual({
      attempts: countRows(temporary.databasePath, 'operation_attempts'),
      receipts: countRows(temporary.databasePath, 'response_receipts')
    }, rowsAtBarrier, initialState);
    assert.equal(store.listEvents({ workflowId: lease.workflowId }).some(event => event.type.startsWith('private_local_')), false);

    const inspection = inspectWorkflowRecovery({ store, workflowId: lease.workflowId, pageSize: 1 });
    assert.equal(inspection.kind, 'external-reconciliation-required', initialState);
    assert.equal(inspection.actionable, false);
    assert.equal(inspection.attempts.length, 1);
    assert.equal(inspection.attempts[0].state, initialState);
    assert.equal(inspection.attempts[0].receipt === null, initialState !== 'response_received');
    assert.equal(JSON.stringify(inspection).includes('opaque-hold-must-stay-opaque'), false);
    assert.equal(JSON.stringify(inspection).includes('synthetic existing-store response state'), false);
    assert.equal(inspection.incomplete, false);
    if (initialState === 'intent') {
      assert.equal(intent.state, 'awaiting_response');
      assert.equal(before.revision + 1, atBarrier.revision);
    }
  }
});

test('hostile controller inputs and provider-shaped outputs are quarantined without reflection', async t => {
  const temporary = temporaryDatabase('bounded-hostile-controller-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'hostile-controller-run');
  const marker = 'SYNTHETIC_RAW_RESULT_CANARY';
  let getterCalls = 0;
  const hostile = Object.defineProperty({}, 'planJson', {
    enumerable: true,
    get() { getterCalls += 1; throw new Error(marker); }
  });
  const invalid = await runBoundedLocalPlan(hostile);
  assert.equal(invalid.reason, 'CONTROLLER_INPUT_INVALID');
  assert.equal(getterCalls, 0);
  assert.equal(JSON.stringify(invalid).includes(marker), false);

  const admission = makeAdmission(store, lease, SINGLE_PLAN, '{}', 'hostile:admit');
  let outputGetterCalls = 0;
  const providerOutput = Object.defineProperty({
    stepId: admission.stepId,
    planHash: admission.plan.planHash,
    registryVersion: admission.plan.registryVersion,
    runIdentity: admission.plan.runIdentity,
    inputHash: 'sha256_' + '1'.repeat(64),
    resultHash: 'sha256_' + '2'.repeat(64)
  }, 'output', {
    enumerable: true,
    get() { outputGetterCalls += 1; throw new Error(marker); }
  });
  assert.throws(() => buildCompletedCheckpoint(admission.state, admission.plan, providerOutput, admission.state.budget), error => {
    assert.equal(error.code, 'CONTROLLER_INPUT_INVALID');
    assert.equal(error.message.includes(marker), false);
    return true;
  });
  assert.equal(outputGetterCalls, 0);

  const result = executeLocalStep(admission.plan, admission.stepId);
  const poisoned = {
    ...result,
    providerResponse: marker,
    workflowId: 'foreign-workflow'
  };
  assert.throws(() => buildCompletedCheckpoint(admission.state, admission.plan, poisoned, admission.state.budget), error => {
    assert.equal(error.code, 'CONTROLLER_INPUT_INVALID');
    assert.equal(JSON.stringify({ code: error.code, message: error.message }).includes(marker), false);
    return true;
  });
  assert.equal(store.getCheckpoint(lease.workflowId).data.pendingStepId, admission.stepId);
  assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);

  const completed = makeCompletion(store, lease, admission, 'hostile:completion-template');
  const workflow = store.getWorkflow(lease.workflowId);
  const checkpoint = store.getCheckpoint(lease.workflowId);
  const selfRehashed = savedResult(admission.plan, admission.stepId, { left: 4, right: 5 }, 900);
  const selfRehashedState = {
    ...completed.controllerState,
    completedResultsJson: JSON.stringify({ [admission.stepId]: selfRehashed }),
    budget: { ...completed.controllerState.budget, totalResultBytes: 3 }
  };
  assert.throws(() => store._localControllerComplete(LOCAL_CONTROLLER_STORAGE_CAPABILITY, {
    ...completed.input,
    expectedRevision: workflow.revision,
    expectedCheckpointSequence: checkpoint.sequence,
    controllerState: selfRehashedState
  }), error => error.code === 'CHECKPOINT_CORRUPT');
  assert.equal(store.getCheckpoint(lease.workflowId).sequence, checkpoint.sequence);
  const replacements = [
    ['planJson', SINGLE_PLAN.replace('"left":4', '"left":3')],
    ['registryVersion', 'registry1_' + '0'.repeat(64)]
  ];
  for (const [key, replacement] of replacements) {
    const tampered = { ...completed.controllerState, [key]: replacement };
    assert.throws(() => store._localControllerComplete(LOCAL_CONTROLLER_STORAGE_CAPABILITY, {
      workflowId: lease.workflowId,
      ownerId: lease.ownerId,
      fence: lease.fence,
      expectedRevision: workflow.revision,
      expectedCheckpointSequence: checkpoint.sequence,
      stepId: admission.stepId,
      eventId: 'hostile:tampered-' + key,
      controllerState: tampered
    }), error => error.code === 'CHECKPOINT_CORRUPT');
  }
  assert.equal(store.getCheckpoint(lease.workflowId).sequence, checkpoint.sequence);
  assert.equal(store.getCheckpoint(lease.workflowId).data.completedResultsJson, '{}');
});

test('AbortSignal proxies are not read as cancellation authority', async t => {
  const temporary = temporaryDatabase('bounded-abort-signal-input-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'abort-signal-input-run');
  const checkpointBefore = store.getCheckpoint(lease.workflowId);
  const eventsBefore = store.listEvents({ workflowId: lease.workflowId }).length;
  const abortedGetter = Object.getOwnPropertyDescriptor(AbortSignal.prototype, 'aborted')?.get;
  assert.equal(typeof abortedGetter, 'function');

  const controller = new AbortController();
  assert.equal(abortedGetter.call(controller.signal), false);
  controller.abort();
  assert.equal(abortedGetter.call(controller.signal), true);

  let proxyReads = 0;
  const abortedSymbol = Reflect.ownKeys(controller.signal)
    .find(key => typeof key === 'symbol' && String(key) === 'Symbol(kAborted)');
  assert.equal(typeof abortedSymbol, 'symbol');
  const masked = new Proxy(controller.signal, {
    get(target, key) {
      proxyReads += 1;
      if (key === 'aborted' || key === abortedSymbol) return false;
      return Reflect.get(target, key, target);
    }
  });
  assert.equal(masked.aborted, false);
  assert.equal(abortedGetter.call(masked), false);
  const revoked = Proxy.revocable(new AbortController().signal, {});
  revoked.revoke();
  assert.throws(() => abortedGetter.call(revoked.proxy));

  const signals = [new AbortController().signal, controller.signal, masked, revoked.proxy];
  const readsAfterProbes = proxyReads;
  for (const signal of signals) {
    const result = await runBoundedLocalPlan({
      store,
      workflowId: lease.workflowId,
      ownerId: lease.ownerId,
      fence: lease.fence,
      signal
    });
    assert.equal(result.reason, 'CONTROLLER_INPUT_INVALID');
  }
  assert.equal(proxyReads, readsAfterProbes);
  assert.equal(store.getCheckpoint(lease.workflowId).sha256, checkpointBefore.sha256);
  assert.equal(store.listEvents({ workflowId: lease.workflowId }).length, eventsBefore);
});

test('private controller entrypoint is not exported and evidence never enables rollout', async () => {
  const publicApi = await import('@jevelopers/agent-harness-runtime');
  assert.equal(publicApi.runBoundedLocalPlan, undefined);
  assert.equal(publicApi.inspectWorkflowRecovery, undefined);
  assert.equal(publicApi.registerLocalControllerCompletionClockSample, undefined);
  assert.equal(publicApi.consumeLocalCompletionClockStaleAfterConfirmedInactive, undefined);
  assert.equal(publicApi.INJECTED_DECISION_PORT, undefined);
});

test('missing and unsupported controller checkpoints stop without adoption or writes', async t => {
  for (const checkpoint of [
    { preserved: 'generic-legacy-checkpoint' },
    { kind: 'agent-harness.private-bounded-local-checkpoint', version: 1, preserved: 'old-private-version' },
    { kind: 'agent-harness.private-bounded-local-checkpoint-foreign', version: 2, preserved: 'foreign-private-kind' }
  ]) {
    const temporary = temporaryDatabase('bounded-checkpoint-compatibility-');
    t.after(() => temporary.cleanup());
    const store = openStore(temporary.databasePath);
    t.after(() => store.close());
    const workflowId = 'compatibility-' + checkpoint.preserved;
    store.createWorkflow({ workflowId, checkpoint, eventId: workflowId + ':created' });
    const lease = store.acquireLease({ workflowId, ownerId: 'worker-compatibility', ttlMs: 60_000, eventId: workflowId + ':lease' });
    const running = store.transitionWorkflow({
      ...lease,
      expectedRevision: 0,
      toState: 'running',
      eventId: workflowId + ':running'
    });
    const checkpointBefore = store.getCheckpoint(workflowId);
    const eventsBefore = store.listEvents({ workflowId }).length;
    const result = await runBoundedLocalPlan({
      store,
      workflowId,
      ownerId: lease.ownerId,
      fence: lease.fence,
      ...(checkpoint.preserved === 'generic-legacy-checkpoint' ? {} : { planJson: SINGLE_PLAN })
    });
    assert.equal(result.status, 'stopped');
    assert.equal(result.reason, checkpoint.preserved === 'generic-legacy-checkpoint'
      ? 'PLAN_INPUT_REQUIRED'
      : 'CHECKPOINT_UNSUPPORTED');
    assert.equal(store.getWorkflow(workflowId).revision, running.revision);
    assert.equal(store.getCheckpoint(workflowId).sha256, checkpointBefore.sha256);
    assert.equal(store.listEvents({ workflowId }).length, eventsBefore);
    assert.equal(countRows(temporary.databasePath, 'operation_attempts'), 0);
  }

  const missing = temporaryDatabase('bounded-checkpoint-missing-plan-');
  t.after(() => missing.cleanup());
  const store = openStore(missing.databasePath);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'missing-plan-run');
  const checkpointBefore = store.getCheckpoint(lease.workflowId);
  const result = await runBoundedLocalPlan({
    store,
    workflowId: lease.workflowId,
    ownerId: lease.ownerId,
    fence: lease.fence
  });
  assert.equal(result.reason, 'PLAN_INPUT_REQUIRED');
  assert.equal(store.getCheckpoint(lease.workflowId).sha256, checkpointBefore.sha256);
});
