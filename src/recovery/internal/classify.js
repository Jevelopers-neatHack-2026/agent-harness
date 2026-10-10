import { createHash } from 'node:crypto';
import { types as utilTypes } from 'node:util';
import {
  advanceBudget,
  checkClockBudget,
  getLimitValues,
  limitSetFromCheckpoint,
  validateBudgetSnapshot
} from '../../orchestrator/internal/limits.js';
import { parseBoundedJson } from '../../orchestrator/internal/json.js';
import {
  compilePlan,
  getNextReadyStep,
  getPlanStepIds,
  validatePrivateCheckpointResults
} from '../../orchestrator/internal/plan.js';
import {
  MAX_CHECKPOINT_DEPTH,
  MAX_CHECKPOINT_SERIALIZED_BYTES,
  RECOVERY_SNAPSHOT_WRAPPER_BYTES
} from '../../internal/checkpoint-bounds.js';
import { checkpointPayloadForIntegrity } from '../../internal/durable-store.js';

const CHECKPOINT_KIND = 'agent-harness.private-bounded-local-checkpoint';
const SNAPSHOT_MAX_BYTES = MAX_CHECKPOINT_SERIALIZED_BYTES + RECOVERY_SNAPSHOT_WRAPPER_BYTES;
const SNAPSHOT_MAX_DEPTH = MAX_CHECKPOINT_DEPTH + 4;
const WORKFLOW_STATES = new Set([
  'queued',
  'running',
  'awaiting_response',
  'reconciliation_pending',
  'review_required',
  'succeeded',
  'failed',
  'cancelled'
]);
const TERMINAL_STATES = new Set(['succeeded', 'failed', 'cancelled']);
const EXTERNAL_UNKNOWN_STATES = new Set(['intent', 'outcome_unknown', 'response_received']);

export const PRIVATE_CHECKPOINT_KIND = CHECKPOINT_KIND;

function sha256(value) {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}

function result(classification, reason) {
  return Object.freeze({
    kind: classification,
    reason,
    actionable: false
  });
}

function stop(reason) {
  return result('stop', reason);
}

function exactRecord(value, allowed, required, reason) {
  if (
    value === null
    || typeof value !== 'object'
    || Array.isArray(value)
    || utilTypes.isProxy(value)
  ) {
    throw new Error(reason);
  }
  let prototype;
  let keys;
  try {
    prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) throw new Error(reason);
    keys = Reflect.ownKeys(value);
  } catch {
    throw new Error(reason);
  }
  if (keys.length > allowed.size) throw new Error(reason);
  const output = Object.create(null);
  for (const key of keys) {
    if (typeof key !== 'string' || !allowed.has(key)) throw new Error(reason);
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      throw new Error(reason);
    }
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) throw new Error(reason);
    output[key] = descriptor.value;
  }
  for (const key of required) {
    if (!Object.hasOwn(output, key)) throw new Error(reason);
  }
  return output;
}

function parseSnapshot(snapshotJson) {
  if (typeof snapshotJson !== 'string') throw new Error('SNAPSHOT_INVALID');
  if (Buffer.byteLength(snapshotJson, 'utf8') > SNAPSHOT_MAX_BYTES) throw new Error('SNAPSHOT_TOO_LARGE');
  try {
    return parseBoundedJson(snapshotJson, {
      maxBytes: SNAPSHOT_MAX_BYTES,
      maxDepth: SNAPSHOT_MAX_DEPTH,
      maxNodes: 32_768,
      allowUnsafeFiniteNumberPaths: [['checkpoint', 'data', 'baseCheckpointData']]
    });
  } catch (error) {
    if (error?.code === 'JSON_TOO_DEEP') throw new Error('SNAPSHOT_TOO_DEEP');
    if (error?.code === 'JSON_TOO_LARGE') throw new Error('SNAPSHOT_TOO_LARGE');
    throw new Error('SNAPSHOT_INVALID');
  }
}

/** @internal Checks the projected wrapper's structural limits before admission. */
export function recoverySnapshotWithinBounds(snapshotJson) {
  try {
    parseSnapshot(snapshotJson);
    return true;
  } catch {
    return false;
  }
}

function readWorkflow(value) {
  const fields = exactRecord(
    value,
    new Set(['workflowId', 'state', 'revision', 'checkpointSequence', 'cancellationRequested']),
    ['workflowId', 'state', 'revision', 'checkpointSequence', 'cancellationRequested'],
    'WORKFLOW_SNAPSHOT_INVALID'
  );
  if (
    typeof fields.workflowId !== 'string'
    || fields.workflowId.trim().length === 0
    || fields.workflowId.length > 512
    || typeof fields.state !== 'string'
    || !WORKFLOW_STATES.has(fields.state)
    || !Number.isSafeInteger(fields.revision)
    || fields.revision < 0
    || !Number.isSafeInteger(fields.checkpointSequence)
    || fields.checkpointSequence < 1
    || typeof fields.cancellationRequested !== 'boolean'
  ) {
    throw new Error('WORKFLOW_SNAPSHOT_INVALID');
  }
  return fields;
}

function readPrivateCheckpointData(value) {
  const fields = exactRecord(
    value,
    new Set([
      'kind',
      'version',
      'workflowId',
      'planJson',
      'planHash',
      'registryVersion',
      'runIdentity',
      'limitsJson',
      'limitsHash',
      'budget',
      'completedResultsJson',
      'pendingStepId',
      'lastAdmissionKind',
      'lastAdmissionStepId',
      'baseCheckpointData'
    ]),
    [
      'kind',
      'version',
      'workflowId',
      'planJson',
      'planHash',
      'registryVersion',
      'runIdentity',
      'limitsJson',
      'limitsHash',
      'budget',
      'completedResultsJson',
      'pendingStepId',
      'lastAdmissionKind',
      'lastAdmissionStepId',
      'baseCheckpointData'
    ],
    'CHECKPOINT_SHAPE_INVALID'
  );
  return fields;
}

function checkpointPayload(checkpointData) {
  return checkpointPayloadForIntegrity(checkpointData);
}

/** @internal Verifies a projected checkpoint with the same serializer as schema-5 writes. */
export function recoverySnapshotCheckpointHashMatches(snapshotJson) {
  try {
    const root = parseSnapshot(snapshotJson);
    const projection = exactRecord(
      root,
      new Set(['before', 'after', 'checkpoint', 'externalAttemptStates', 'observedAtMs']),
      ['before', 'after', 'checkpoint', 'externalAttemptStates', 'observedAtMs'],
      'SNAPSHOT_INVALID'
    );
    const checkpoint = exactRecord(
      projection.checkpoint,
      new Set(['sequence', 'schemaVersion', 'data', 'sha256']),
      ['sequence', 'schemaVersion', 'data', 'sha256'],
      'CHECKPOINT_INVALID'
    );
    return typeof checkpoint.sha256 === 'string'
      && /^[0-9a-f]{64}$/u.test(checkpoint.sha256)
      && sha256(checkpointPayload(checkpoint.data)) === checkpoint.sha256;
  } catch {
    return false;
  }
}

function validateAttemptBarrier(states) {
  if (!Array.isArray(states) || states.length > 32 || utilTypes.isProxy(states)) {
    throw new Error('ATTEMPT_SNAPSHOT_INVALID');
  }
  let unresolved = false;
  for (let index = 0; index < states.length; index += 1) {
    const descriptor = Object.getOwnPropertyDescriptor(states, String(index));
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) {
      throw new Error('ATTEMPT_SNAPSHOT_INVALID');
    }
    const state = descriptor.value;
    if (typeof state !== 'string') throw new Error('ATTEMPT_SNAPSHOT_INVALID');
    if (EXTERNAL_UNKNOWN_STATES.has(state)) unresolved = true;
    else if (state !== 'reconciled') throw new Error('ATTEMPT_SNAPSHOT_INVALID');
  }
  if (Reflect.ownKeys(states).length !== states.length + 1) throw new Error('ATTEMPT_SNAPSHOT_INVALID');
  return unresolved;
}

function safeStopForError(code) {
  if (code === 'CHECKPOINT_UNSUPPORTED') return stop('CHECKPOINT_UNSUPPORTED');
  if (code === 'CHECKPOINT_FOREIGN') return stop('CHECKPOINT_FOREIGN');
  if (code === 'CHECKPOINT_CORRUPT') return stop('CHECKPOINT_CORRUPT');
  if (code === 'PLAN_INVALID') return stop('PLAN_INVALID');
  if (code === 'LIMITS_INVALID') return stop('LIMITS_INVALID');
  if (code === 'BUDGET_INVALID') return stop('BUDGET_INVALID');
  if (code === 'RESULTS_INVALID') return result('quarantine', 'LOCAL_RESULT_IDENTITY_INVALID');
  return stop('RECOVERY_INPUT_INVALID');
}

export function classifyRecovery(snapshotJson) {
  let root;
  try {
    root = parseSnapshot(snapshotJson);
  } catch (error) {
    return safeStopForError(error.message);
  }

  let snapshot;
  let before;
  let after;
  try {
    snapshot = exactRecord(
      root,
      new Set(['before', 'after', 'checkpoint', 'externalAttemptStates', 'observedAtMs']),
      ['before', 'after', 'checkpoint', 'externalAttemptStates', 'observedAtMs'],
      'SNAPSHOT_INVALID'
    );
    before = readWorkflow(snapshot.before);
    after = readWorkflow(snapshot.after);
    if (!Number.isSafeInteger(snapshot.observedAtMs) || snapshot.observedAtMs < 0) {
      throw new Error('SNAPSHOT_INVALID');
    }
  } catch {
    return stop('RECOVERY_INPUT_INVALID');
  }

  if (
    TERMINAL_STATES.has(before.state)
    || TERMINAL_STATES.has(after.state)
    || before.state === 'review_required'
    || after.state === 'review_required'
  ) {
    return stop(before.state === 'review_required' || after.state === 'review_required'
      ? 'REVIEW_REQUIRED_IS_TERMINAL_FOR_THIS_CONTROLLER'
      : 'WORKFLOW_TERMINAL');
  }

  let hasExternalBarrier;
  try {
    hasExternalBarrier = validateAttemptBarrier(snapshot.externalAttemptStates);
  } catch {
    return stop('ATTEMPT_SNAPSHOT_INVALID');
  }
  if (
    hasExternalBarrier
    || before.state === 'reconciliation_pending'
    || after.state === 'reconciliation_pending'
    || before.state === 'awaiting_response'
    || after.state === 'awaiting_response'
  ) {
    return result('external-reconciliation-required', 'EXTERNAL_OUTCOME_UNRESOLVED');
  }
  if (after.cancellationRequested) return stop('CANCELLATION_REQUESTED');

  if (
    before.state !== after.state
    || before.workflowId !== after.workflowId
    || before.revision !== after.revision
    || before.checkpointSequence !== after.checkpointSequence
    || before.cancellationRequested !== after.cancellationRequested
  ) {
    return stop('WORKFLOW_SNAPSHOT_CHANGED');
  }
  if (after.state !== 'running') return stop('WORKFLOW_NOT_RUNNING');

  if (snapshot.checkpoint === null) return stop('CHECKPOINT_MISSING');
  let checkpoint;
  try {
    checkpoint = exactRecord(
      snapshot.checkpoint,
      new Set(['sequence', 'schemaVersion', 'data', 'sha256']),
      ['sequence', 'schemaVersion', 'data', 'sha256'],
      'CHECKPOINT_INVALID'
    );
  } catch {
    return stop('CHECKPOINT_CORRUPT');
  }
  if (!Number.isSafeInteger(checkpoint.schemaVersion) || checkpoint.schemaVersion > 2) {
    return stop('CHECKPOINT_UNSUPPORTED');
  }
  if (checkpoint.schemaVersion !== 2) return stop('CHECKPOINT_UNSUPPORTED');
  if (
    !Number.isSafeInteger(checkpoint.sequence)
    || checkpoint.sequence !== after.checkpointSequence
    || typeof checkpoint.sha256 !== 'string'
    || !/^[0-9a-f]{64}$/.test(checkpoint.sha256)
  ) {
    return stop('CHECKPOINT_CORRUPT');
  }

  let payload;
  try {
    payload = checkpointPayload(checkpoint.data);
  } catch {
    return stop('CHECKPOINT_CORRUPT');
  }
  if (sha256(payload) !== checkpoint.sha256) return stop('CHECKPOINT_CORRUPT');

  let data;
  try {
    data = readPrivateCheckpointData(checkpoint.data);
  } catch {
    return stop('CHECKPOINT_FOREIGN');
  }
  if (data.kind !== CHECKPOINT_KIND) return stop('CHECKPOINT_FOREIGN');
  if (data.version !== 2) return stop('CHECKPOINT_UNSUPPORTED');

  if (typeof data.workflowId !== 'string' || data.workflowId !== after.workflowId) {
    return stop('CHECKPOINT_FOREIGN_WORKFLOW');
  }
  if (
    typeof data.planJson !== 'string'
    || typeof data.planHash !== 'string'
    || typeof data.registryVersion !== 'string'
    || typeof data.runIdentity !== 'string'
    || typeof data.limitsJson !== 'string'
    || typeof data.limitsHash !== 'string'
    || typeof data.completedResultsJson !== 'string'
    || !(data.pendingStepId === null || typeof data.pendingStepId === 'string')
    || !['step', 'replay'].includes(data.lastAdmissionKind)
    || !(data.lastAdmissionStepId === null || typeof data.lastAdmissionStepId === 'string')
    || data.baseCheckpointData === null
    || typeof data.baseCheckpointData !== 'object'
    || Array.isArray(data.baseCheckpointData)
  ) {
    return stop('CHECKPOINT_CORRUPT');
  }

  let limitSet;
  let plan;
  let completed;
  let budget;
  try {
    limitSet = limitSetFromCheckpoint(data.limitsJson, data.limitsHash);
  } catch {
    return safeStopForError('LIMITS_INVALID');
  }
  if (snapshot.externalAttemptStates.length > getLimitValues(limitSet).maxRecoveryAttemptStates) {
    return stop('ATTEMPT_SNAPSHOT_LIMIT');
  }
  try {
    plan = compilePlan(data.planJson, limitSet, undefined, data.workflowId);
  } catch {
    return safeStopForError('PLAN_INVALID');
  }
  if (
    data.planHash !== plan.planHash
    || data.registryVersion !== plan.registryVersion
    || data.runIdentity !== plan.runIdentity
  ) {
    return stop('CHECKPOINT_CORRUPT');
  }
  try {
    completed = validatePrivateCheckpointResults(plan, data.completedResultsJson);
  } catch {
    return safeStopForError('RESULTS_INVALID');
  }
  try {
    budget = validateBudgetSnapshot(data.budget, limitSet);
  } catch {
    return safeStopForError('BUDGET_INVALID');
  }
  if (
    budget.stepsAdmitted < completed.resultCount
    || budget.stepsAdmitted !== completed.resultCount + (data.pendingStepId === null ? 0 : 1)
    || budget.totalResultBytes !== completed.totalResultBytes
    || budget.stepAttempts < budget.stepsAdmitted
    || (data.lastAdmissionKind === 'replay' && budget.localRetries < 1)
    || (data.pendingStepId !== null
      && (data.lastAdmissionStepId !== data.pendingStepId || budget.stepAttempts < 1))
    || (data.pendingStepId === null && completed.resultCount > 0
      && !completed.stepIds.includes(data.lastAdmissionStepId))
    || (data.pendingStepId === null && completed.resultCount === 0
      && (data.lastAdmissionStepId !== null || budget.stepAttempts !== 0))
  ) {
    return stop('BUDGET_CHECKPOINT_INCONSISTENT');
  }

  const planStepIds = getPlanStepIds(plan);
  const elapsedMs = budget.elapsedMs + (snapshot.observedAtMs - budget.lastWallMs);
  const clockReason = checkClockBudget(
    budget,
    snapshot.observedAtMs,
    elapsedMs,
    limitSet
  );
  if (clockReason) return stop(clockReason);

  if (data.pendingStepId !== null) {
    if (
      !planStepIds.includes(data.pendingStepId)
      || completed.stepIds.includes(data.pendingStepId)
      || getNextReadyStep(plan, data.completedResultsJson) !== data.pendingStepId
    ) {
      return stop('PENDING_STEP_IDENTITY_INVALID');
    }
    const replay = advanceBudget(
      budget,
      'replay',
      snapshot.observedAtMs,
      elapsedMs,
      limitSet
    );
    if (!replay.allowed) return stop(replay.reason);
    return result('local-replay-candidate', 'PURE_LOCAL_REPLAY_WITH_NEW_ALLOWANCE_REQUIRED');
  }

  if (completed.resultCount === planStepIds.length) {
    return result('durable-evidence-present', 'ALL_LOCAL_RESULTS_MATCH_PINNED_IDENTITIES');
  }
  if (getNextReadyStep(plan, data.completedResultsJson) !== null) {
    const admission = advanceBudget(
      budget,
      'step',
      snapshot.observedAtMs,
      elapsedMs,
      limitSet
    );
    if (!admission.allowed) return stop(admission.reason);
    return result('local-ready-candidate', 'NEXT_LOCAL_ADMISSION_REQUIRED');
  }
  return stop('NO_DETERMINISTIC_READY_STEP');
}
