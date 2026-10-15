import { createHash, randomUUID } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { types as utilTypes } from 'node:util';
import {
  DurableWorkflowStore,
  measureCheckpointPayload,
  checkpointPayloadForIntegrity,
  consumeLocalCompletionBusyAfterConfirmedInactive,
  consumeLocalCompletionClockStaleAfterConfirmedInactive,
  registerLocalControllerCompletionClockSample,
  consumeLocalAdmissionBusyAfterConfirmedInactive
} from '../../internal/durable-store.js';
import { RuntimeError } from '../../internal/errors.js';
import {
  MAX_CHECKPOINT_SERIALIZED_BYTES
} from '../../internal/checkpoint-bounds.js';
import { LOCAL_CONTROLLER_STORAGE_CAPABILITY } from '../../internal/private-local-store-capability.js';
import {
  recordOperationsDiagnosticError,
  recordOperationsDiagnosticProgress,
  setOperationsDiagnosticPhase
} from '../../internal/private-ops-diagnostics.js';
import {
  advanceBudget,
  checkClockBudget,
  checkStepDuration,
  createBudgetSnapshot,
  createLimitSet,
  getLimitValues,
  recordResultBytes,
  serializeLimitSet,
  validateBudgetSnapshot
} from './limits.js';
import {
  compilePlan,
  executeLocalStep,
  getNextReadyStep,
  getPlanStepIds,
  getMaximumCompletedResultsJsonBytes,
  validatePrivateCheckpointResults
} from './plan.js';
import { createDefaultRegistry } from '../../tools/internal/registry.js';
import {
  classifyRecovery,
  PRIVATE_CHECKPOINT_KIND,
  recoverySnapshotWithinBounds,
  recoverySnapshotCheckpointHashMatches
} from '../../recovery/internal/classify.js';

const CHECKPOINT_VERSION = 2;
const CONTROLLER_KEYS = new Set([
  'kind', 'version', 'workflowId', 'planJson', 'planHash', 'registryVersion',
  'runIdentity', 'limitsJson', 'limitsHash', 'budget', 'completedResultsJson',
  'pendingStepId', 'lastAdmissionKind', 'lastAdmissionStepId', 'baseCheckpointData'
]);
const INPUT_KEYS = new Set([
  'store', 'workflowId', 'ownerId', 'fence', 'planJson', 'limitsJson',
  'busyTimeoutMs', 'runtimeDeadlineNs'
]);
const TERMINAL = new Set(['succeeded', 'failed', 'cancelled']);

export class LocalControllerError extends Error {
  constructor(code) {
    super('The private bounded local controller stopped safely.');
    this.name = 'LocalControllerError';
    this.code = code;
  }
}

function fail(code) {
  throw new LocalControllerError(code);
}

function sha256(value) {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}

function ownDataRecord(value, allowed, required = []) {
  if (
    value === null
    || typeof value !== 'object'
    || Array.isArray(value)
    || utilTypes.isProxy(value)
  ) fail('CONTROLLER_INPUT_INVALID');
  let prototype;
  let keys;
  try {
    prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) fail('CONTROLLER_INPUT_INVALID');
    keys = Reflect.ownKeys(value);
  } catch {
    fail('CONTROLLER_INPUT_INVALID');
  }
  if (keys.length > allowed.size) fail('CONTROLLER_INPUT_INVALID');
  const output = Object.create(null);
  for (const key of keys) {
    if (typeof key !== 'string' || !allowed.has(key)) fail('CONTROLLER_INPUT_INVALID');
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      fail('CONTROLLER_INPUT_INVALID');
    }
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) fail('CONTROLLER_INPUT_INVALID');
    output[key] = descriptor.value;
  }
  for (const key of required) {
    if (!Object.hasOwn(output, key)) fail('CONTROLLER_INPUT_INVALID');
  }
  return output;
}

function deepFreeze(value) {
  if (value === null || typeof value !== 'object' || Object.isFrozen(value)) return value;
  for (const key of Reflect.ownKeys(value)) deepFreeze(value[key]);
  return Object.freeze(value);
}

function controllerState(value, workflowId) {
  const fields = ownDataRecord(value, CONTROLLER_KEYS, [...CONTROLLER_KEYS]);
  if (
    fields.kind !== PRIVATE_CHECKPOINT_KIND
    || fields.version !== CHECKPOINT_VERSION
    || fields.workflowId !== workflowId
    || typeof fields.planJson !== 'string'
    || typeof fields.planHash !== 'string'
    || typeof fields.registryVersion !== 'string'
    || typeof fields.runIdentity !== 'string'
    || typeof fields.limitsJson !== 'string'
    || typeof fields.limitsHash !== 'string'
    || typeof fields.completedResultsJson !== 'string'
    || !(fields.pendingStepId === null || typeof fields.pendingStepId === 'string')
    || !['step', 'replay'].includes(fields.lastAdmissionKind)
    || typeof fields.lastAdmissionStepId !== 'string'
    || !fields.baseCheckpointData
    || typeof fields.baseCheckpointData !== 'object'
    || Array.isArray(fields.baseCheckpointData)
    || utilTypes.isProxy(fields.baseCheckpointData)
  ) fail('CONTROLLER_CHECKPOINT_INVALID');
  if (Buffer.byteLength(JSON.stringify(fields), 'utf8') > MAX_CHECKPOINT_SERIALIZED_BYTES) {
    fail('CONTROLLER_CHECKPOINT_TOO_LARGE');
  }
  return fields;
}

function workflowForRecovery(workflow) {
  return {
    workflowId: workflow.workflowId,
    state: workflow.state,
    revision: workflow.revision,
    checkpointSequence: workflow.checkpointSequence,
    cancellationRequested: workflow.cancellationRequested
  };
}

function checkpointForRecovery(checkpoint) {
  return {
    sequence: checkpoint.sequence,
    schemaVersion: checkpoint.schemaVersion,
    data: checkpoint.data,
    sha256: checkpoint.sha256
  };
}

function readRecovery(snapshot, observedAtMs) {
  return classifyRecovery(JSON.stringify({
    before: workflowForRecovery(snapshot.workflow),
    after: workflowForRecovery(snapshot.workflow),
    checkpoint: checkpointForRecovery(snapshot.checkpoint),
    externalAttemptStates: snapshot.externalAttemptStates,
    observedAtMs
  }));
}

function checkpointRecoveryJson(snapshot, state, observedAtMs, checkpointHash = '0'.repeat(64)) {
  return JSON.stringify({
    before: workflowForRecovery(snapshot.workflow),
    after: workflowForRecovery(snapshot.workflow),
    checkpoint: {
      sequence: snapshot.checkpoint.sequence,
      schemaVersion: snapshot.checkpoint.schemaVersion,
      data: state,
      sha256: checkpointHash
    },
    externalAttemptStates: snapshot.externalAttemptStates,
    observedAtMs
  });
}

function assertCheckpointFitsStore(state, reasonForUnbounded) {
  const measured = measureCheckpointPayload(state);
  if (!measured) fail(reasonForUnbounded);
  if (measured.serializedBytes > MAX_CHECKPOINT_SERIALIZED_BYTES) {
    fail('CONTROLLER_CHECKPOINT_TOO_LARGE');
  }
}

function maximumFutureCheckpointState(fields, plan, limitSet, maximumResultsBytes) {
  const stepIds = getPlanStepIds(plan);
  let longestStepId = stepIds[0];
  for (const stepId of stepIds) {
    if (Buffer.byteLength(stepId, 'utf8') > Buffer.byteLength(longestStepId, 'utf8')) {
      longestStepId = stepId;
    }
  }
  const limits = getLimitValues(limitSet);
  return {
    ...fields,
    budget: {
      ...fields.budget,
      lastWallMs: fields.budget.deadlineWallMs,
      elapsedMs: limits.maxElapsedMs,
      stepsAdmitted: stepIds.length,
      stepAttempts: limits.maxStepAttempts,
      localRetries: limits.maxLocalRetries,
      replans: limits.maxReplans,
      iterations: limits.maxIterations,
      totalResultBytes: limits.maxTotalResultBytes
    },
    completedResultsJson: '\\'.repeat(maximumResultsBytes),
    pendingStepId: longestStepId,
    lastAdmissionKind: 'replay',
    lastAdmissionStepId: longestStepId
  };
}

function assertPrivateCheckpointRoundTrip(snapshot, state, plan, limitSet, observedAtMs) {
  const fields = controllerState(state, snapshot.workflow.workflowId);
  assertCheckpointFitsStore(fields, 'CONTROLLER_CHECKPOINT_UNSUPPORTED');
  const actualCheckpointHash = sha256(checkpointPayloadForIntegrity(fields));
  const actualRecoveryProjection = checkpointRecoveryJson(snapshot, fields, observedAtMs, actualCheckpointHash);
  if (
    !recoverySnapshotWithinBounds(actualRecoveryProjection)
    || !recoverySnapshotCheckpointHashMatches(actualRecoveryProjection)
  ) {
    fail('CONTROLLER_CHECKPOINT_TOO_LARGE');
  }

  const maximumResultsBytes = getMaximumCompletedResultsJsonBytes(plan, fields.completedResultsJson);
  // Reserve result growth plus every still-legal checkpoint metadata value.
  // The backslash string is measurement-only and is never stored as a result.
  const reservedState = maximumFutureCheckpointState(fields, plan, limitSet, maximumResultsBytes);
  assertCheckpointFitsStore(reservedState, 'CONTROLLER_CHECKPOINT_TOO_LARGE');
  if (!recoverySnapshotWithinBounds(checkpointRecoveryJson(snapshot, reservedState, observedAtMs))) {
    fail('CONTROLLER_CHECKPOINT_TOO_LARGE');
  }
}

function unavailableSnapshot(snapshot) {
  if (TERMINAL.has(snapshot.workflow.state)) return 'WORKFLOW_TERMINAL';
  if (snapshot.workflow.state === 'review_required') return 'REVIEW_REQUIRED_IS_TERMINAL_FOR_THIS_CONTROLLER';
  if (snapshot.workflow.cancellationRequested) return 'CANCELLATION_REQUESTED';
  if (snapshot.workflow.state === 'reconciliation_pending' || snapshot.workflow.state === 'awaiting_response') {
    return 'EXTERNAL_OUTCOME_UNRESOLVED';
  }
  if (snapshot.workflow.state !== 'running') return 'WORKFLOW_NOT_RUNNING';
  if (snapshot.externalAttemptsTruncated || snapshot.hasUnresolvedExternalAttempt) {
    return 'EXTERNAL_OUTCOME_UNRESOLVED';
  }
  return null;
}

function privateCheckpoint(value) {
  return value !== null
    && typeof value === 'object'
    && !Array.isArray(value)
    && !utilTypes.isProxy(value)
    && value.kind === PRIVATE_CHECKPOINT_KIND;
}

function controllerStop(reason, workflowId, checkpointSequence) {
  return Object.freeze({
    kind: 'agent-harness.private-local-controller-result',
    version: 1,
    status: reason === 'EXTERNAL_OUTCOME_UNRESOLVED' ? 'external-reconciliation-required' : 'stopped',
    actionable: false,
    reason,
    workflowId,
    checkpointSequence: Number.isSafeInteger(checkpointSequence) ? checkpointSequence : null
  });
}

function compilePinnedPlan(state, workflowId) {
  const limitSet = createLimitSet(state.limitsJson);
  if (limitSet.hash !== state.limitsHash) fail('CHECKPOINT_LIMITS_IDENTITY_INVALID');
  const plan = compilePlan(state.planJson, limitSet, createDefaultRegistry(), workflowId);
  if (
    plan.planHash !== state.planHash
    || plan.registryVersion !== state.registryVersion
    || plan.runIdentity !== state.runIdentity
  ) fail('CHECKPOINT_PLAN_IDENTITY_INVALID');
  const budget = validateBudgetSnapshot(state.budget, limitSet);
  const completed = validatePrivateCheckpointResults(plan, state.completedResultsJson);
  if (completed.totalResultBytes !== budget.totalResultBytes) {
    fail('BUDGET_CHECKPOINT_INCONSISTENT');
  }
  return { plan, limitSet };
}

export function buildInitialPrivateCheckpoint({ workflowId, planJson, plan, limitSet, baseCheckpointData, budget, stepId }) {
  return deepFreeze({
    kind: PRIVATE_CHECKPOINT_KIND,
    version: CHECKPOINT_VERSION,
    workflowId,
    planJson,
    planHash: plan.planHash,
    registryVersion: plan.registryVersion,
    runIdentity: plan.runIdentity,
    limitsJson: serializeLimitSet(limitSet),
    limitsHash: limitSet.hash,
    budget,
    completedResultsJson: '{}',
    pendingStepId: stepId,
    lastAdmissionKind: 'step',
    lastAdmissionStepId: stepId,
    baseCheckpointData
  });
}

export function buildAdmissionCheckpoint(state, kind, stepId, budget) {
  const source = ownDataRecord(state, CONTROLLER_KEYS, [...CONTROLLER_KEYS]);
  const fields = controllerState(source, source.workflowId);
  const next = {
    ...fields,
    budget,
    pendingStepId: stepId,
    lastAdmissionKind: kind,
    lastAdmissionStepId: stepId
  };
  return deepFreeze(next);
}

export function computePendingLocalStep(state, plan) {
  if (state.pendingStepId === null) fail('LOCAL_STEP_NOT_PENDING');
  return executeLocalStep(plan, state.pendingStepId, state.completedResultsJson);
}

export function buildCompletedCheckpoint(state, plan, resultRecord, budget) {
  const source = ownDataRecord(state, CONTROLLER_KEYS, [...CONTROLLER_KEYS]);
  const fields = controllerState(source, source.workflowId);
  const resultFields = ownDataRecord(resultRecord, new Set([
    'stepId', 'planHash', 'registryVersion', 'runIdentity', 'inputHash', 'resultHash', 'output'
  ]), ['stepId', 'planHash', 'registryVersion', 'runIdentity', 'inputHash', 'resultHash', 'output']);
  if (typeof resultFields.output !== 'number' && typeof resultFields.output !== 'string') {
    fail('LOCAL_RESULT_INVALID');
  }
  const safeResult = {
    stepId: resultFields.stepId,
    planHash: resultFields.planHash,
    registryVersion: resultFields.registryVersion,
    runIdentity: resultFields.runIdentity,
    inputHash: resultFields.inputHash,
    resultHash: resultFields.resultHash,
    output: resultFields.output
  };
  const resultJson = JSON.stringify(safeResult);
  if (typeof resultJson !== 'string') fail('LOCAL_RESULT_INVALID');
  const previous = JSON.parse(fields.completedResultsJson);
  if (Object.hasOwn(previous, safeResult.stepId)) fail('LOCAL_RESULT_DUPLICATE');
  const completed = Object.create(null);
  for (const key of Object.keys(previous).sort()) completed[key] = previous[key];
  completed[safeResult.stepId] = JSON.parse(resultJson);
  const completedResultsJson = JSON.stringify(completed);
  const validated = validatePrivateCheckpointResults(plan, completedResultsJson);
  if (validated.totalResultBytes !== budget.totalResultBytes) fail('BUDGET_CHECKPOINT_INCONSISTENT');
  return deepFreeze({
    ...fields,
    budget,
    completedResultsJson,
    pendingStepId: null
  });
}

function safeFailure(error, workflowId, checkpointSequence) {
  const code = typeof error?.code === 'string' ? error.code : 'LOCAL_CONTROLLER_STOPPED';
  if (code === 'OUTCOME_UNRESOLVED') return controllerStop('EXTERNAL_OUTCOME_UNRESOLVED', workflowId, checkpointSequence);
  if (code === 'UNSUPPORTED_SCHEMA_VERSION') return controllerStop('CHECKPOINT_UNSUPPORTED', workflowId, checkpointSequence);
  if (code === 'CONCURRENT_MODIFICATION') return controllerStop('WORKFLOW_SNAPSHOT_CHANGED', workflowId, checkpointSequence);
  if (code === 'LEASE_REQUIRED' || code === 'STALE_FENCE' || code === 'LEASE_EXPIRED') {
    return controllerStop('LEASE_FENCED', workflowId, checkpointSequence);
  }
  if (code === 'ILLEGAL_TRANSITION') return controllerStop('WORKFLOW_NOT_RUNNING', workflowId, checkpointSequence);
  if (code.startsWith('PLAN_')) return controllerStop(code, workflowId, checkpointSequence);
  if (code.startsWith('CHECKPOINT_')) return controllerStop(code, workflowId, checkpointSequence);
  if (
    code.startsWith('BUDGET_')
    || code.startsWith('CLOCK_')
    || code.startsWith('CONTROLLER_')
    || code.startsWith('LOCAL_')
    || code.startsWith('LIMIT_')
    || code.endsWith('_LIMIT')
    || ['INVALID_BUDGET_SNAPSHOT', 'INVALID_DEADLINE', 'INVALID_LIMITS', 'INCONSISTENT_LIMITS'].includes(code)
  ) {
    return controllerStop(code, workflowId, checkpointSequence);
  }
  if (code === 'INVALID_INPUT') return controllerStop('CONTROLLER_INPUT_INVALID', workflowId, checkpointSequence);
  return controllerStop('LOCAL_CONTROLLER_STOPPED', workflowId, checkpointSequence);
}

function eventId(kind) {
  return 'private-local:' + randomUUID() + ':' + kind;
}

function resultEnvelope(state, plan, resultSequence, completedJson) {
  const completed = JSON.parse(completedJson);
  const results = getPlanStepIds(plan).filter(id => Object.hasOwn(completed, id)).map(id => completed[id]);
  const facts = Object.freeze({
    stepsAdmitted: state.budget.stepsAdmitted,
    stepAttempts: state.budget.stepAttempts,
    localRetries: state.budget.localRetries,
    replans: state.budget.replans,
    iterations: state.budget.iterations,
    totalResultBytes: state.budget.totalResultBytes,
    deadlineWallMs: state.budget.deadlineWallMs
  });
  const core = {
    kind: 'agent-harness.private-synthetic-local-result',
    version: 1,
    status: 'completed',
    workflowId: state.workflowId,
    planHash: plan.planHash,
    registryVersion: plan.registryVersion,
    runIdentity: plan.runIdentity,
    limitSetHash: state.limitsHash,
    checkpointSequence: resultSequence,
    results,
    stoppingFacts: facts
  };
  return deepFreeze({ ...core, resultHash: 'result1_' + sha256(JSON.stringify(core)) });
}

function assertOptionalPins(input, plan, limitSet, workflowId) {
  if (input.planJson !== undefined) {
    const proposed = compilePlan(input.planJson, limitSet, createDefaultRegistry(), workflowId);
    if (
      proposed.planHash !== plan.planHash
      || proposed.registryVersion !== plan.registryVersion
      || proposed.runIdentity !== plan.runIdentity
    ) fail('CHECKPOINT_PLAN_IDENTITY_INVALID');
  }
  if (input.limitsJson !== undefined) {
    const proposedLimits = createLimitSet(input.limitsJson);
    if (proposedLimits.hash !== limitSet.hash) fail('CHECKPOINT_LIMITS_IDENTITY_INVALID');
  }
}

async function yieldAtControllerBoundary() {
  await new Promise(resolve => setImmediate(resolve));
}

function elapsedNow(baseElapsed, processStart) {
  const delta = Math.floor(performance.now() - processStart);
  if (!Number.isSafeInteger(delta) || delta < 0) fail('CLOCK_INCONSISTENT');
  return baseElapsed + delta;
}

function isCompletionBusy(error) {
  const confirmedInactive = consumeLocalCompletionBusyAfterConfirmedInactive(error);
  return confirmedInactive
    && error instanceof RuntimeError
    && error.module === 'state'
    && error?.operation === '_localControllerComplete'
    && error?.code === 'STORE_BUSY'
    && error?.retryable === true;
}

function isCompletionClockStale(error) {
  const confirmedInactive = consumeLocalCompletionClockStaleAfterConfirmedInactive(error);
  return confirmedInactive
    && error instanceof RuntimeError
    && error.module === 'state'
    && error.operation === '_localControllerComplete'
    && error.code === 'CONCURRENT_MODIFICATION'
    && error.retryable === true;
}

function completionRetryWindowAvailable(fields, snapshot, now, elapsedMs, limitSet) {
  if (!Number.isSafeInteger(fields.busyTimeoutMs) || typeof fields.runtimeDeadlineNs !== 'bigint') return false;
  const busyWaitNs = BigInt(fields.busyTimeoutMs) * 1_000_000n;
  if (fields.runtimeDeadlineNs - process.hrtime.bigint() <= busyWaitNs) return false;
  const limits = getLimitValues(limitSet);
  const remainingWallMs = Math.min(
    snapshot.workflow.leaseExpiresAtMs - now,
    snapshot.checkpoint.data.budget.deadlineWallMs - now,
    limits.maxElapsedMs - elapsedMs
  );
  return remainingWallMs > fields.busyTimeoutMs;
}

function isLocalAdmissionBusy(error) {
  const confirmedInactive = consumeLocalAdmissionBusyAfterConfirmedInactive(error);
  return confirmedInactive
    && error instanceof RuntimeError
    && error.module === 'state'
    && error.operation === '_localControllerAdmit'
    && error.code === 'STORE_BUSY'
    && error.retryable === true;
}

function admissionRetryWindowAvailable(fields, snapshot, now, elapsedMs, limitSet, controllerState) {
  if (!Number.isSafeInteger(fields.busyTimeoutMs) || typeof fields.runtimeDeadlineNs !== 'bigint') return false;
  const busyWaitNs = BigInt(fields.busyTimeoutMs) * 1_000_000n;
  if (fields.runtimeDeadlineNs - process.hrtime.bigint() <= busyWaitNs) return false;
  const limits = getLimitValues(limitSet);
  const remainingWallMs = Math.min(
    snapshot.workflow.leaseExpiresAtMs - now,
    controllerState.budget.deadlineWallMs - now,
    limits.maxElapsedMs - elapsedMs
  );
  return remainingWallMs > fields.busyTimeoutMs;
}

function admissionResolutionStopReason(resolution) {
  if (resolution.reason === 'lease_expired') return 'LEASE_FENCED';
  if (resolution.reason === 'external_outcome_unresolved') return 'EXTERNAL_OUTCOME_UNRESOLVED';
  if ([
    'pending_state_changed', 'pending_checkpoint_changed', 'pending_base_checkpoint_changed'
  ].includes(resolution.reason)) return 'WORKFLOW_SNAPSHOT_CHANGED';
  return 'LOCAL_ADMISSION_OUTCOME_UNKNOWN';
}

async function admitWithBusyRecovery({
  store,
  fields,
  snapshot,
  previousState,
  controllerState,
  plan,
  limitSet,
  workflowId,
  ownerId,
  fence,
  stepId,
  admissionKind,
  admissionEventId,
  phase,
  elapsedAtStart,
  initialMono,
  hasBusyTimeout
}) {
  let candidate = controllerState;
  const maximumCalls = 3;
  for (let admissionCall = 1; admissionCall <= maximumCalls; admissionCall += 1) {
    try {
      setOperationsDiagnosticPhase(phase, snapshot.checkpoint.sequence);
      const admission = store._localControllerAdmit(LOCAL_CONTROLLER_STORAGE_CAPABILITY, {
        workflowId,
        ownerId,
        fence,
        expectedRevision: snapshot.workflow.revision,
        expectedCheckpointSequence: snapshot.checkpoint.sequence,
        stepId,
        admissionKind,
        eventId: admissionEventId,
        controllerState: candidate
      });
      return { kind: 'committed', admission, controllerState: candidate };
    } catch (error) {
      if (!isLocalAdmissionBusy(error) || !hasBusyTimeout) throw error;

      let resolution;
      try {
        setOperationsDiagnosticPhase('admission_resolution', snapshot.checkpoint.sequence);
        resolution = store._localControllerResolveAdmission(LOCAL_CONTROLLER_STORAGE_CAPABILITY, {
          workflowId,
          ownerId,
          fence,
          expectedRevision: snapshot.workflow.revision,
          expectedCheckpointSequence: snapshot.checkpoint.sequence,
          stepId,
          admissionKind,
          eventId: admissionEventId,
          previousCheckpointSha256: snapshot.checkpoint.sha256,
          controllerState: candidate
        });
      } catch (resolutionError) {
        recordOperationsDiagnosticError('controller_error', resolutionError, {
          phase: 'admission_resolution', checkpointSequence: snapshot.checkpoint.sequence
        });
        return {
          kind: 'stopped',
          reason: 'LOCAL_ADMISSION_OUTCOME_UNKNOWN',
          checkpointSequence: snapshot.checkpoint.sequence
        };
      }

      if (resolution.kind === 'committed') {
        const currentStop = unavailableSnapshot(resolution.snapshot);
        if (currentStop) {
          return {
            kind: 'stopped', reason: currentStop,
            checkpointSequence: resolution.snapshot.checkpoint.sequence
          };
        }
        if (
          resolution.snapshot.workflow.revision !== resolution.revision
          || resolution.snapshot.checkpoint.sequence !== resolution.checkpointSequence
          || resolution.snapshot.workflow.leaseOwner !== ownerId
          || resolution.snapshot.workflow.fence !== fence
          || resolution.snapshot.workflow.leaseExpiresAtMs === null
          || resolution.snapshot.workflow.leaseExpiresAtMs <= resolution.now
        ) {
          return {
            kind: 'stopped', reason: 'WORKFLOW_SNAPSHOT_CHANGED',
            checkpointSequence: resolution.snapshot.checkpoint.sequence
          };
        }
        return { kind: 'committed', admission: resolution, controllerState: candidate };
      }
      if (resolution.kind !== 'pending') {
        return {
          kind: 'stopped',
          reason: admissionResolutionStopReason(resolution),
          checkpointSequence: resolution.snapshot?.checkpoint.sequence ?? snapshot.checkpoint.sequence
        };
      }
      if (admissionCall === maximumCalls) {
        return {
          kind: 'stopped', reason: 'LOCAL_ADMISSION_BUSY',
          checkpointSequence: resolution.snapshot.checkpoint.sequence
        };
      }

      await yieldAtControllerBoundary();
      const retrySnapshot = store._localControllerSnapshot(LOCAL_CONTROLLER_STORAGE_CAPABILITY, workflowId);
      const retryStop = unavailableSnapshot(retrySnapshot);
      if (retryStop) {
        return { kind: 'stopped', reason: retryStop, checkpointSequence: retrySnapshot.checkpoint.sequence };
      }
      if (
        retrySnapshot.workflow.revision !== snapshot.workflow.revision
        || retrySnapshot.checkpoint.sequence !== snapshot.checkpoint.sequence
        || retrySnapshot.checkpoint.sha256 !== snapshot.checkpoint.sha256
      ) {
        return {
          kind: 'stopped', reason: 'WORKFLOW_SNAPSHOT_CHANGED',
          checkpointSequence: retrySnapshot.checkpoint.sequence
        };
      }
      const retryNow = store._localControllerNow(LOCAL_CONTROLLER_STORAGE_CAPABILITY);
      if (
        retrySnapshot.workflow.leaseOwner !== ownerId
        || retrySnapshot.workflow.fence !== fence
        || retrySnapshot.workflow.leaseExpiresAtMs === null
        || retrySnapshot.workflow.leaseExpiresAtMs <= retryNow
      ) {
        return { kind: 'stopped', reason: 'LEASE_FENCED', checkpointSequence: retrySnapshot.checkpoint.sequence };
      }
      const retryElapsed = elapsedNow(elapsedAtStart, initialMono);
      const clockReason = checkClockBudget(candidate.budget, retryNow, retryElapsed, limitSet);
      if (clockReason) {
        return { kind: 'stopped', reason: clockReason, checkpointSequence: retrySnapshot.checkpoint.sequence };
      }
      if (!admissionRetryWindowAvailable(fields, retrySnapshot, retryNow, retryElapsed, limitSet, candidate)) {
        return {
          kind: 'stopped', reason: 'LOCAL_ADMISSION_BUSY',
          checkpointSequence: retrySnapshot.checkpoint.sequence
        };
      }
      const retryBudget = Object.freeze({
        ...candidate.budget,
        lastWallMs: retryNow,
        elapsedMs: retryElapsed
      });
      candidate = previousState === null
        ? buildInitialPrivateCheckpoint({
          workflowId,
          planJson: candidate.planJson,
          plan,
          limitSet,
          baseCheckpointData: candidate.baseCheckpointData,
          budget: retryBudget,
          stepId
        })
        : buildAdmissionCheckpoint(previousState, admissionKind, stepId, retryBudget);
      assertPrivateCheckpointRoundTrip(retrySnapshot, candidate, plan, limitSet, retryNow);
      snapshot = retrySnapshot;
    }
  }
  return {
    kind: 'stopped', reason: 'LOCAL_ADMISSION_BUSY',
    checkpointSequence: snapshot.checkpoint.sequence
  };
}

/**
 * Execute one pinned private plan over the durable store. The only executable
 * handlers come from the static reviewed registry; each allowance and result
 * is checkpointed with its event/outbox row before another step can begin.
 */
export async function runBoundedLocalPlan(input) {
  let workflowId = null;
  let checkpointSequence = null;
  let diagnosticPhase = 'initial_snapshot';
  let admissionOrdinal = 0;
  let completionOrdinal = 0;
  try {
    const fields = ownDataRecord(input, INPUT_KEYS, ['store', 'workflowId', 'ownerId', 'fence']);
    workflowId = fields.workflowId;
    if (
      utilTypes.isProxy(fields.store)
      || !(fields.store instanceof DurableWorkflowStore)
      || typeof workflowId !== 'string'
      || workflowId.trim().length === 0
      || workflowId.length > 512
      || typeof fields.ownerId !== 'string'
      || fields.ownerId.trim().length === 0
      || fields.ownerId.length > 512
      || !Number.isSafeInteger(fields.fence)
      || fields.fence < 1
      || (fields.planJson !== undefined && typeof fields.planJson !== 'string')
      || (fields.limitsJson !== undefined && typeof fields.limitsJson !== 'string')
    ) fail('CONTROLLER_INPUT_INVALID');
    const hasBusyTimeout = Object.hasOwn(fields, 'busyTimeoutMs');
    const hasRuntimeDeadline = Object.hasOwn(fields, 'runtimeDeadlineNs');
    if (hasBusyTimeout !== hasRuntimeDeadline || (hasBusyTimeout && (
      !Number.isSafeInteger(fields.busyTimeoutMs)
      || fields.busyTimeoutMs < 1
      || fields.busyTimeoutMs > 250
      || typeof fields.runtimeDeadlineNs !== 'bigint'
      || fields.runtimeDeadlineNs < 1n
    ))) fail('CONTROLLER_INPUT_INVALID');

    const store = fields.store;
    setOperationsDiagnosticPhase(diagnosticPhase, checkpointSequence);
    const now = store._localControllerNow(LOCAL_CONTROLLER_STORAGE_CAPABILITY);
    let snapshot = store._localControllerSnapshot(LOCAL_CONTROLLER_STORAGE_CAPABILITY, workflowId);
    checkpointSequence = snapshot.checkpoint.sequence;
    const unavailable = unavailableSnapshot(snapshot);
    if (unavailable) return controllerStop(unavailable, workflowId, checkpointSequence);
    if (
      snapshot.workflow.leaseOwner !== fields.ownerId
      || snapshot.workflow.fence !== fields.fence
      || snapshot.workflow.leaseExpiresAtMs === null
      || snapshot.workflow.leaseExpiresAtMs <= now
    ) return controllerStop('LEASE_FENCED', workflowId, checkpointSequence);

    let state;
    let plan;
    let limitSet;
    let budget;
    let pendingStepId;
    let admissionKind;
    let elapsedAtStart;
    let initialMono;
    let previousState = null;

    if (privateCheckpoint(snapshot.checkpoint.data)) {
      if (snapshot.checkpoint.data.version !== CHECKPOINT_VERSION) {
        return controllerStop('CHECKPOINT_UNSUPPORTED', workflowId, checkpointSequence);
      }
      state = controllerState(snapshot.checkpoint.data, workflowId);
      previousState = state;
      ({ plan, limitSet } = compilePinnedPlan(state, workflowId));
      assertOptionalPins(fields, plan, limitSet, workflowId);
      const recovery = readRecovery(snapshot, now);
      if (recovery.kind === 'durable-evidence-present') {
        return resultEnvelope(state, plan, snapshot.checkpoint.sequence, state.completedResultsJson);
      }
      if (recovery.kind !== 'local-ready-candidate' && recovery.kind !== 'local-replay-candidate') {
        return controllerStop(recovery.reason, workflowId, snapshot.checkpoint.sequence);
      }
      const storedBudget = validateBudgetSnapshot(state.budget, limitSet);
      elapsedAtStart = storedBudget.elapsedMs + (now - storedBudget.lastWallMs);
      const clockReason = checkClockBudget(storedBudget, now, elapsedAtStart, limitSet);
      if (clockReason) return controllerStop(clockReason, workflowId, snapshot.checkpoint.sequence);
      initialMono = performance.now();
      admissionKind = recovery.kind === 'local-replay-candidate' ? 'replay' : 'step';
      pendingStepId = admissionKind === 'replay'
        ? state.pendingStepId
        : getNextReadyStep(plan, state.completedResultsJson);
      const next = advanceBudget(state.budget, admissionKind, now, elapsedAtStart, limitSet);
      if (!next.allowed) return controllerStop(next.reason, workflowId, snapshot.checkpoint.sequence);
      budget = next.state;
      state = buildAdmissionCheckpoint(state, admissionKind, pendingStepId, budget);
    } else {
      if (
        snapshot.checkpoint.data.kind === PRIVATE_CHECKPOINT_KIND
        || (typeof snapshot.checkpoint.data.kind === 'string'
          && snapshot.checkpoint.data.kind.startsWith('agent-harness.private-bounded-local-checkpoint'))
      ) return controllerStop('CHECKPOINT_UNSUPPORTED', workflowId, checkpointSequence);
      if (typeof fields.planJson !== 'string') return controllerStop('PLAN_INPUT_REQUIRED', workflowId, checkpointSequence);
      limitSet = createLimitSet(fields.limitsJson ?? '{}');
      plan = compilePlan(fields.planJson, limitSet, createDefaultRegistry(), workflowId);
      const limitValues = getLimitValues(limitSet);
      const deadlineWallMs = now + limitValues.maxElapsedMs;
      if (!Number.isSafeInteger(deadlineWallMs) || deadlineWallMs <= now) fail('INVALID_DEADLINE');
      const initialBudget = createBudgetSnapshot(limitSet, now, deadlineWallMs);
      pendingStepId = getNextReadyStep(plan, '{}');
      const next = advanceBudget(initialBudget, 'step', now, 0, limitSet);
      if (!next.allowed) return controllerStop(next.reason, workflowId, checkpointSequence);
      budget = next.state;
      state = buildInitialPrivateCheckpoint({
        workflowId,
        planJson: fields.planJson,
        plan,
        limitSet,
        baseCheckpointData: snapshot.checkpoint.data,
        budget,
        stepId: pendingStepId
      });
      admissionKind = 'step';
      elapsedAtStart = budget.elapsedMs;
      initialMono = performance.now();
    }

    assertPrivateCheckpointRoundTrip(snapshot, state, plan, limitSet, now);
    diagnosticPhase = 'initial_admission';
    setOperationsDiagnosticPhase(diagnosticPhase, checkpointSequence);
    const initialAdmissionEventId = eventId(admissionKind);
    const initialAdmissionResult = await admitWithBusyRecovery({
      store,
      fields,
      snapshot,
      previousState,
      controllerState: state,
      plan,
      limitSet,
      workflowId,
      ownerId: fields.ownerId,
      fence: fields.fence,
      stepId: pendingStepId,
      admissionKind,
      admissionEventId: initialAdmissionEventId,
      phase: diagnosticPhase,
      elapsedAtStart,
      initialMono,
      hasBusyTimeout
    });
    if (initialAdmissionResult.kind === 'stopped') {
      return controllerStop(initialAdmissionResult.reason, workflowId, initialAdmissionResult.checkpointSequence);
    }
    state = initialAdmissionResult.controllerState;
    let currentAdmission = initialAdmissionResult.admission;
    admissionOrdinal += 1;
    recordOperationsDiagnosticProgress('admission_committed', admissionOrdinal, currentAdmission.checkpointSequence);

    while (true) {
      await yieldAtControllerBoundary();
      diagnosticPhase = 'loop_snapshot';
      setOperationsDiagnosticPhase(diagnosticPhase, checkpointSequence);
      snapshot = store._localControllerSnapshot(LOCAL_CONTROLLER_STORAGE_CAPABILITY, workflowId);
      checkpointSequence = snapshot.checkpoint.sequence;
      const stopped = unavailableSnapshot(snapshot);
      if (stopped) return controllerStop(stopped, workflowId, checkpointSequence);
      if (
        snapshot.workflow.revision !== currentAdmission.revision
        || snapshot.checkpoint.sequence !== currentAdmission.checkpointSequence
        || !privateCheckpoint(snapshot.checkpoint.data)
      ) return controllerStop('WORKFLOW_SNAPSHOT_CHANGED', workflowId, checkpointSequence);

      state = controllerState(snapshot.checkpoint.data, workflowId);
      ({ plan, limitSet } = compilePinnedPlan(state, workflowId));
      if (state.pendingStepId !== pendingStepId) return controllerStop('PENDING_STEP_IDENTITY_INVALID', workflowId, checkpointSequence);
      const wallBeforeCompute = store._localControllerNow(LOCAL_CONTROLLER_STORAGE_CAPABILITY);
      const elapsedBeforeCompute = elapsedNow(elapsedAtStart, initialMono);
      const clockBeforeCompute = checkClockBudget(state.budget, wallBeforeCompute, elapsedBeforeCompute, limitSet);
      if (clockBeforeCompute) return controllerStop(clockBeforeCompute, workflowId, checkpointSequence);
      if (
        getNextReadyStep(plan, state.completedResultsJson) !== pendingStepId
        || state.pendingStepId === null
      ) return controllerStop('PENDING_STEP_IDENTITY_INVALID', workflowId, checkpointSequence);

      const computeStarted = performance.now();
      const resultRecord = computePendingLocalStep(state, plan);
      const duration = Math.max(0, Math.ceil(performance.now() - computeStarted));
      const durationCheck = checkStepDuration(duration, limitSet);
      if (!durationCheck.allowed) return controllerStop(durationCheck.reason, workflowId, checkpointSequence);
      const resultBytes = Buffer.byteLength(JSON.stringify(resultRecord.output), 'utf8');
      const byteRecord = recordResultBytes(state.budget, resultBytes, limitSet);
      if (!byteRecord.allowed) return controllerStop(byteRecord.reason, workflowId, checkpointSequence);

      const wallAfterCompute = store._localControllerNow(LOCAL_CONTROLLER_STORAGE_CAPABILITY);
      const elapsedAfterCompute = elapsedNow(elapsedAtStart, initialMono);
      const afterComputeClockReason = checkClockBudget(
        state.budget,
        wallAfterCompute,
        elapsedAfterCompute,
        limitSet
      );
      if (afterComputeClockReason) return controllerStop(afterComputeClockReason, workflowId, checkpointSequence);

      await yieldAtControllerBoundary();
      const beforeCommit = store._localControllerSnapshot(LOCAL_CONTROLLER_STORAGE_CAPABILITY, workflowId);
      checkpointSequence = beforeCommit.checkpoint.sequence;
      const beforeCommitStop = unavailableSnapshot(beforeCommit);
      if (beforeCommitStop) return controllerStop(beforeCommitStop, workflowId, checkpointSequence);
      if (
        beforeCommit.workflow.revision !== snapshot.workflow.revision
        || beforeCommit.checkpoint.sequence !== snapshot.checkpoint.sequence
      ) return controllerStop('WORKFLOW_SNAPSHOT_CHANGED', workflowId, checkpointSequence);

      const completionWallNow = store._localControllerNow(LOCAL_CONTROLLER_STORAGE_CAPABILITY);
      const completionMonotonicNowNs = process.hrtime.bigint();
      const completionElapsed = elapsedNow(elapsedAtStart, initialMono);
      const completionClockReason = checkClockBudget(
        byteRecord.state,
        completionWallNow,
        completionElapsed,
        limitSet
      );
      if (completionClockReason) return controllerStop(completionClockReason, workflowId, checkpointSequence);
      const completedBudget = Object.freeze({
        ...byteRecord.state,
        lastWallMs: completionWallNow,
        elapsedMs: completionElapsed
      });
      const completedState = buildCompletedCheckpoint(state, plan, resultRecord, completedBudget);
      assertPrivateCheckpointRoundTrip(beforeCommit, completedState, plan, limitSet, completionWallNow);
      diagnosticPhase = 'completion';
      setOperationsDiagnosticPhase(diagnosticPhase, checkpointSequence);
      const completionEventId = eventId('complete');
      let completionState = completedState;
      let completionStateMonotonicNowNs = completionMonotonicNowNs;
      let completionClockRefreshUsed = false;
      let completion;
      let completionResolution = null;
      for (let completionCall = 1; completionCall <= 3; completionCall += 1) {
        try {
          const callElapsed = elapsedNow(elapsedAtStart, initialMono);
          const callClockReason = checkClockBudget(
            completionState.budget,
            completionState.budget.lastWallMs,
            callElapsed,
            limitSet
          );
          if (callClockReason) return controllerStop(callClockReason, workflowId, checkpointSequence);
          registerLocalControllerCompletionClockSample(
            LOCAL_CONTROLLER_STORAGE_CAPABILITY,
            completionState,
            completionState.budget.lastWallMs,
            completionStateMonotonicNowNs
          );
          completion = store._localControllerComplete(LOCAL_CONTROLLER_STORAGE_CAPABILITY, {
            workflowId,
            ownerId: fields.ownerId,
            fence: fields.fence,
            expectedRevision: beforeCommit.workflow.revision,
            expectedCheckpointSequence: beforeCommit.checkpoint.sequence,
            stepId: pendingStepId,
            eventId: completionEventId,
            controllerState: completionState
          });
          break;
        } catch (error) {
          if (isCompletionClockStale(error)) {
            if (completionClockRefreshUsed || completionCall === 3) {
              return controllerStop('LOCAL_COMPLETION_BUSY', workflowId, checkpointSequence);
            }
            completionClockRefreshUsed = true;
            await yieldAtControllerBoundary();
            const retrySnapshot = store._localControllerSnapshot(LOCAL_CONTROLLER_STORAGE_CAPABILITY, workflowId);
            checkpointSequence = retrySnapshot.checkpoint.sequence;
            const retryStop = unavailableSnapshot(retrySnapshot);
            if (retryStop) return controllerStop(retryStop, workflowId, checkpointSequence);
            const retryWallNow = store._localControllerNow(LOCAL_CONTROLLER_STORAGE_CAPABILITY);
            const retryMonotonicNowNs = process.hrtime.bigint();
            if (
              retrySnapshot.workflow.revision !== beforeCommit.workflow.revision
              || retrySnapshot.checkpoint.sequence !== beforeCommit.checkpoint.sequence
              || retrySnapshot.checkpoint.sha256 !== beforeCommit.checkpoint.sha256
              || retrySnapshot.workflow.leaseOwner !== fields.ownerId
              || retrySnapshot.workflow.fence !== fields.fence
              || retrySnapshot.workflow.leaseExpiresAtMs === null
              || retrySnapshot.workflow.leaseExpiresAtMs <= retryWallNow
            ) return controllerStop('WORKFLOW_SNAPSHOT_CHANGED', workflowId, checkpointSequence);
            const retryElapsed = elapsedNow(elapsedAtStart, initialMono);
            const retryClockReason = checkClockBudget(byteRecord.state, retryWallNow, retryElapsed, limitSet);
            if (retryClockReason) return controllerStop(retryClockReason, workflowId, checkpointSequence);
            if (hasBusyTimeout && !completionRetryWindowAvailable(fields, retrySnapshot, retryWallNow, retryElapsed, limitSet)) {
              return controllerStop('LOCAL_COMPLETION_BUSY', workflowId, checkpointSequence);
            }
            const retryBudget = Object.freeze({
              ...byteRecord.state,
              lastWallMs: retryWallNow,
              elapsedMs: retryElapsed
            });
            completionState = buildCompletedCheckpoint(state, plan, resultRecord, retryBudget);
            assertPrivateCheckpointRoundTrip(beforeCommit, completionState, plan, limitSet, retryWallNow);
            completionStateMonotonicNowNs = retryMonotonicNowNs;
            continue;
          }
          if (!isCompletionBusy(error) || !hasBusyTimeout) throw error;
          let resolution;
          try {
            resolution = store._localControllerResolveCompletion(LOCAL_CONTROLLER_STORAGE_CAPABILITY, {
              workflowId,
              ownerId: fields.ownerId,
              fence: fields.fence,
              expectedRevision: beforeCommit.workflow.revision,
              expectedCheckpointSequence: beforeCommit.checkpoint.sequence,
              stepId: pendingStepId,
              eventId: completionEventId,
              previousCheckpointSha256: beforeCommit.checkpoint.sha256,
              controllerState: completionState
            });
          } catch (resolutionError) {
            recordOperationsDiagnosticError('controller_error', resolutionError, {
              phase: 'completion_resolution', checkpointSequence
            });
            return controllerStop('LOCAL_COMPLETION_OUTCOME_UNKNOWN', workflowId, checkpointSequence);
          }
          if (resolution.kind === 'committed') {
            completion = resolution;
            completionResolution = resolution;
            break;
          }
          if (resolution.kind !== 'pending' || completionCall === 3) {
            return controllerStop(
              resolution.kind === 'pending' ? 'LOCAL_COMPLETION_BUSY' : 'LOCAL_COMPLETION_OUTCOME_UNKNOWN',
              workflowId,
              checkpointSequence
            );
          }

          // The exact attempted checkpoint is classified before yielding or
          // refreshing mutable clocks. Recheck the same pending barrier after
          // yielding so cancellation, fencing, or an unresolved outcome wins.
          await yieldAtControllerBoundary();
          const retrySnapshot = store._localControllerSnapshot(LOCAL_CONTROLLER_STORAGE_CAPABILITY, workflowId);
          checkpointSequence = retrySnapshot.checkpoint.sequence;
          const retryStop = unavailableSnapshot(retrySnapshot);
          if (retryStop) return controllerStop(retryStop, workflowId, checkpointSequence);
          const retryWallNow = store._localControllerNow(LOCAL_CONTROLLER_STORAGE_CAPABILITY);
          const retryMonotonicNowNs = process.hrtime.bigint();
          if (
            retrySnapshot.workflow.revision !== beforeCommit.workflow.revision
            || retrySnapshot.checkpoint.sequence !== beforeCommit.checkpoint.sequence
            || retrySnapshot.checkpoint.sha256 !== beforeCommit.checkpoint.sha256
            || retrySnapshot.workflow.leaseOwner !== fields.ownerId
            || retrySnapshot.workflow.fence !== fields.fence
            || retrySnapshot.workflow.leaseExpiresAtMs === null
            || retrySnapshot.workflow.leaseExpiresAtMs <= retryWallNow
          ) return controllerStop('WORKFLOW_SNAPSHOT_CHANGED', workflowId, checkpointSequence);
          const retryElapsed = elapsedNow(elapsedAtStart, initialMono);
          const retryClockReason = checkClockBudget(byteRecord.state, retryWallNow, retryElapsed, limitSet);
          if (retryClockReason) return controllerStop(retryClockReason, workflowId, checkpointSequence);
          if (!completionRetryWindowAvailable(fields, retrySnapshot, retryWallNow, retryElapsed, limitSet)) {
            return controllerStop('LOCAL_COMPLETION_BUSY', workflowId, checkpointSequence);
          }
          const retryBudget = Object.freeze({
            ...byteRecord.state,
            lastWallMs: retryWallNow,
            elapsedMs: retryElapsed
          });
          completionState = buildCompletedCheckpoint(state, plan, resultRecord, retryBudget);
          assertPrivateCheckpointRoundTrip(beforeCommit, completionState, plan, limitSet, retryWallNow);
          completionStateMonotonicNowNs = retryMonotonicNowNs;
        }
      }
      if (!completion) return controllerStop('LOCAL_COMPLETION_OUTCOME_UNKNOWN', workflowId, checkpointSequence);
      if (completionResolution) {
        const current = completionResolution.snapshot;
        const currentStop = unavailableSnapshot(current);
        if (currentStop) return controllerStop(currentStop, workflowId, current.checkpoint.sequence);
        if (
          current.workflow.revision !== completion.revision
          || current.checkpoint.sequence !== completion.checkpointSequence
          || current.workflow.leaseOwner !== fields.ownerId
          || current.workflow.fence !== fields.fence
          || current.workflow.leaseExpiresAtMs === null
          || current.workflow.leaseExpiresAtMs <= completionResolution.now
        ) return controllerStop('WORKFLOW_SNAPSHOT_CHANGED', workflowId, current.checkpoint.sequence);
      }
      completionOrdinal += 1;
      recordOperationsDiagnosticProgress('completion_committed', completionOrdinal, completion.checkpointSequence);
      state = completionState;
      checkpointSequence = completion.checkpointSequence;
      const allResults = validatePrivateCheckpointResults(plan, state.completedResultsJson);
      if (allResults.resultCount === getPlanStepIds(plan).length) {
        return resultEnvelope(state, plan, checkpointSequence, state.completedResultsJson);
      }

      await yieldAtControllerBoundary();
      snapshot = store._localControllerSnapshot(LOCAL_CONTROLLER_STORAGE_CAPABILITY, workflowId);
      checkpointSequence = snapshot.checkpoint.sequence;
      const stopReason = unavailableSnapshot(snapshot);
      if (stopReason) return controllerStop(stopReason, workflowId, checkpointSequence);
      if (
        snapshot.workflow.revision !== completion.revision
        || snapshot.checkpoint.sequence !== completion.checkpointSequence
      ) return controllerStop('WORKFLOW_SNAPSHOT_CHANGED', workflowId, checkpointSequence);
      const stepId = getNextReadyStep(plan, state.completedResultsJson);
      if (stepId === null) return controllerStop('NO_DETERMINISTIC_READY_STEP', workflowId, checkpointSequence);
      const nextWall = store._localControllerNow(LOCAL_CONTROLLER_STORAGE_CAPABILITY);
      const nextElapsed = elapsedNow(elapsedAtStart, initialMono);
      const nextBudget = advanceBudget(state.budget, 'step', nextWall, nextElapsed, limitSet);
      if (!nextBudget.allowed) return controllerStop(nextBudget.reason, workflowId, checkpointSequence);
      const nextState = buildAdmissionCheckpoint(state, 'step', stepId, nextBudget.state);
      assertPrivateCheckpointRoundTrip(snapshot, nextState, plan, limitSet, nextWall);
      diagnosticPhase = 'next_admission';
      setOperationsDiagnosticPhase(diagnosticPhase, checkpointSequence);
      const nextAdmissionEventId = eventId('step');
      const nextAdmissionResult = await admitWithBusyRecovery({
        store,
        fields,
        snapshot,
        previousState: state,
        controllerState: nextState,
        plan,
        limitSet,
        workflowId,
        ownerId: fields.ownerId,
        fence: fields.fence,
        stepId,
        admissionKind: 'step',
        admissionEventId: nextAdmissionEventId,
        phase: diagnosticPhase,
        elapsedAtStart,
        initialMono,
        hasBusyTimeout
      });
      if (nextAdmissionResult.kind === 'stopped') {
        return controllerStop(nextAdmissionResult.reason, workflowId, nextAdmissionResult.checkpointSequence);
      }
      const nextAdmission = nextAdmissionResult.admission;
      admissionOrdinal += 1;
      recordOperationsDiagnosticProgress('admission_committed', admissionOrdinal, nextAdmission.checkpointSequence);
      state = nextAdmissionResult.controllerState;
      pendingStepId = stepId;
      // The next loop iteration uses this committed admission's revision/fence.
      currentAdmission = nextAdmission;
    }
  } catch (error) {
    recordOperationsDiagnosticError('controller_error', error, {
      phase: diagnosticPhase,
      checkpointSequence
    });
    return safeFailure(error, workflowId, checkpointSequence);
  }
}
