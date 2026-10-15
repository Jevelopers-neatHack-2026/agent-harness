import { createHash, randomUUID } from 'node:crypto';
import { createRequire } from 'node:module';
import { types as utilTypes } from 'node:util';
import { mkdirSync, statSync, statfsSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { RuntimeError, runtimeError } from './errors.js';
import { LOCAL_CONTROLLER_STORAGE_CAPABILITY } from './private-local-store-capability.js';
import { OPERATIONS_STORAGE_CAPABILITY } from './private-ops-store-capability.js';
import { recordOperationsDiagnosticError } from './private-ops-diagnostics.js';
import {
  MAX_CHECKPOINT_DEPTH,
  MAX_CHECKPOINT_NODES,
  MAX_CHECKPOINT_SERIALIZED_BYTES,
  MAX_CHECKPOINT_SOURCE_UNITS
} from './checkpoint-bounds.js';
import {
  createLimitSet,
  getLimitValues,
  validateBudgetSnapshot
} from '../orchestrator/internal/limits.js';
import {
  compilePlan,
  getMaximumCompletedResultsJsonBytes,
  getNextReadyStep,
  getPlanStepIds,
  validatePrivateCheckpointResults
} from '../orchestrator/internal/plan.js';
import { parseBoundedJson } from '../orchestrator/internal/json.js';
import { createDefaultRegistry } from '../tools/internal/registry.js';

export const INTERNAL_SCHEMA_VERSION = 6;
const CHECKPOINT_SCHEMA_VERSION = 2;
const MAX_JEV_OBSERVATION_PAGE_SIZE = 100;
const JEV_MODEL_ID = 'jev-1.13.0';
const SAFE_OPAQUE_REF = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/u;
const SAFE_MODEL_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/u;
const SAFE_PROVIDER_REQUEST_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const LOCAL_CHECKPOINT_KIND = 'agent-harness.private-bounded-local-checkpoint';
const LOCAL_STEP_ID = /^[a-z][a-z0-9_-]{0,47}$/u;
const CONFIRMED_LOCAL_COMPLETION_BUSY_ERRORS = new WeakSet();
const LOCAL_COMPLETION_CLOCK_SAMPLES = new WeakMap();
const LOCAL_COMPLETION_CLOCK_STALE_ERRORS = new WeakSet();
const CONFIRMED_LOCAL_COMPLETION_CLOCK_STALE_ERRORS = new WeakSet();
const CONFIRMED_LOCAL_ADMISSION_BUSY_ERRORS = new WeakSet();
const OPERATIONS_ADMISSION_BUSY_DIAGNOSTICS = new WeakMap();
const LOCAL_CONTROLLER_CLOCK_DIAGNOSTICS = new WeakMap();
const OPS_EVENT_RESERVATION = 96;
const OPS_MAX_WORKER_GENERATIONS = 16;
const OPS_GENERATION_WRITE_RESERVE_BYTES = 16_384;
const OPS_CHECKPOINT_WRITE_BASE_BYTES = 32_768;
const OPS_ADMISSION_WRITE_RESERVE_BYTES = 65_536;
const OPS_MIN_WORK_BYTES = 256;
const OPS_MIN_PLAN_JSON = '{"kind":"agent-harness.private-synthetic-plan","version":1,"steps":[{"id":"sum","operation":"number.add","dependsOn":[],"args":{"left":20,"right":22}}]}';
const OPS_TERMINAL_STATES = new Set(['completed', 'failed', 'cancelled']);
const OPS_SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const JEV_UNKNOWN_REASONS = new Set([
  'not_dispatched',
  'possible_dispatch',
  'missing_or_invalid_usage',
  'recovered_after_crash'
]);
const INVALID_CHECKPOINT_INPUT = Symbol('invalid checkpoint input');
const STATES = new Set([
  'queued',
  'running',
  'awaiting_response',
  'reconciliation_pending',
  'review_required',
  'succeeded',
  'failed',
  'cancelled'
]);
const TERMINAL = new Set(['succeeded', 'failed', 'cancelled']);
const TRANSITIONS = new Map([
  ['queued', new Set(['running', 'review_required'])],
  ['running', new Set(['reconciliation_pending', 'review_required', 'succeeded', 'failed'])],
  ['awaiting_response', new Set(['running', 'reconciliation_pending', 'review_required', 'succeeded', 'failed'])],
  ['reconciliation_pending', new Set(['running', 'review_required', 'succeeded', 'failed'])],
  ['review_required', new Set()],
  ['succeeded', new Set()],
  ['failed', new Set()],
  ['cancelled', new Set()]
]);
const requireBuiltin = createRequire(import.meta.url);

export function consumeLocalCompletionBusyAfterConfirmedInactive(error) {
  return error instanceof RuntimeError && CONFIRMED_LOCAL_COMPLETION_BUSY_ERRORS.delete(error);
}

export function registerLocalControllerCompletionClockSample(capability, controllerState, wallNowMs, monotonicNowNs) {
  if (capability !== LOCAL_CONTROLLER_STORAGE_CAPABILITY
      || !isRecord(controllerState)
      || !isRecord(controllerState.budget)
      || !Number.isSafeInteger(wallNowMs)
      || controllerState.budget.lastWallMs !== wallNowMs
      || typeof monotonicNowNs !== 'bigint'
      || monotonicNowNs < 1n) {
    fail('_localControllerComplete', 'INVALID_INPUT', 'The private completion clock sample is invalid.');
  }
  LOCAL_COMPLETION_CLOCK_SAMPLES.set(controllerState, Object.freeze({ wallNowMs, monotonicNowNs }));
}

export function consumeLocalCompletionClockStaleAfterConfirmedInactive(error) {
  return error instanceof RuntimeError && CONFIRMED_LOCAL_COMPLETION_CLOCK_STALE_ERRORS.delete(error);
}

export function consumeLocalAdmissionBusyAfterConfirmedInactive(error) {
  return error instanceof RuntimeError && CONFIRMED_LOCAL_ADMISSION_BUSY_ERRORS.delete(error);
}

function fail(operation, code, safe_message, retryable = false) {
  throw runtimeError({ module: 'state', operation, code, safe_message, retryable });
}

function expectString(value, field, operation, minimum = 1) {
  if (typeof value !== 'string' || value.trim().length < minimum || value.length > 512) {
    fail(operation, 'INVALID_INPUT', 'A required identifier or reference is invalid.');
  }
  return value;
}

function expectInteger(value, field, operation, minimum = 0) {
  if (!Number.isSafeInteger(value) || value < minimum) {
    fail(operation, 'INVALID_INPUT', 'A required numeric setting is outside its supported range.');
  }
  return value;
}

function isRecord(value) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function checkpointString(value, context) {
  context.sourceUnits += value.length;
  if (context.sourceUnits > MAX_CHECKPOINT_SOURCE_UNITS) throw INVALID_CHECKPOINT_INPUT;
  return value;
}

function canonicalValue(value, context, depth = 0) {
  context.nodes += 1;
  if (context.nodes > MAX_CHECKPOINT_NODES || depth > MAX_CHECKPOINT_DEPTH) {
    throw INVALID_CHECKPOINT_INPUT;
  }
  if (value === null || typeof value === 'boolean') return value;
  if (typeof value === 'string') return checkpointString(value, context);
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value !== 'object' || context.seen.has(value)) throw INVALID_CHECKPOINT_INPUT;

  context.seen.add(value);
  try {
    if (Array.isArray(value)) {
      const prototype = Object.getPrototypeOf(value);
      if (prototype !== Array.prototype && prototype !== null) throw INVALID_CHECKPOINT_INPUT;
      const keys = Reflect.ownKeys(value);
      const lengthDescriptor = Object.getOwnPropertyDescriptor(value, 'length');
      const length = lengthDescriptor?.value;
      if (
        !Number.isSafeInteger(length)
        || length < 0
        || length > MAX_CHECKPOINT_NODES
        || keys.length !== length + 1
        || context.nodes + length > MAX_CHECKPOINT_NODES
      ) {
        throw INVALID_CHECKPOINT_INPUT;
      }

      // Keep arrays as arrays for JSON encoding, then remove inherited hooks
      // so serialization cannot invoke a caller-supplied toJSON accessor.
      const result = [];
      for (let index = 0; index < length; index += 1) {
        const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
        if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) {
          throw INVALID_CHECKPOINT_INPUT;
        }
        result.push(canonicalValue(descriptor.value, context, depth + 1));
      }
      Object.setPrototypeOf(result, null);
      return result;
    }

    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) throw INVALID_CHECKPOINT_INPUT;
    const keys = Reflect.ownKeys(value);
    if (context.nodes + keys.length > MAX_CHECKPOINT_NODES) throw INVALID_CHECKPOINT_INPUT;
    for (const key of keys) {
      if (typeof key !== 'string') throw INVALID_CHECKPOINT_INPUT;
      checkpointString(key, context);
    }
    keys.sort();

    // A regular object's `__proto__` setter would mutate the prototype and
    // silently drop an own JSON property. A null-prototype container keeps
    // every key as data while preserving deterministic JSON serialization.
    const result = Object.create(null);
    for (const key of keys) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) {
        throw INVALID_CHECKPOINT_INPUT;
      }
      Object.defineProperty(result, key, {
        value: canonicalValue(descriptor.value, context, depth + 1),
        enumerable: true,
        configurable: true,
        writable: true
      });
    }
    return result;
  } finally {
    context.seen.delete(value);
  }
}

function checkpointMetrics(data) {
  if (!isRecord(data)) throw INVALID_CHECKPOINT_INPUT;
  const context = { nodes: 0, sourceUnits: 0, seen: new Set() };
  const normalized = canonicalValue(data, context);
  const encoded = JSON.stringify(normalized);
  const payload = '{"schemaVersion":' + CHECKPOINT_SCHEMA_VERSION + ',"data":' + encoded + '}';
  return {
    serializedBytes: Buffer.byteLength(payload, 'utf8'),
    sourceUnits: context.sourceUnits,
    nodes: context.nodes,
    payload
  };
}

function boundedCheckpointMetrics(data) {
  const measured = checkpointMetrics(data);
  if (measured.serializedBytes > MAX_CHECKPOINT_SERIALIZED_BYTES) throw INVALID_CHECKPOINT_INPUT;
  return measured;
}

function serializeCheckpoint(data, operation) {
  try {
    return boundedCheckpointMetrics(data).payload;
  } catch {
    fail(operation, 'INVALID_INPUT', 'Checkpoint data is invalid or exceeds the supported size and structure limits.');
  }
}

/** @internal Returns the exact schema-5/envelope-2 bytes used by checkpoint writes. */
export function checkpointPayloadForIntegrity(data) {
  try {
    return boundedCheckpointMetrics(data).payload;
  } catch {
    fail('checkpointIntegrity', 'CHECKPOINT_CORRUPT', 'The checkpoint failed its integrity check.');
  }
}

/** @internal Measures the exact schema-5 payload without writing to SQLite. */
export function measureCheckpointPayload(data) {
  try {
    const measured = checkpointMetrics(data);
    return Object.freeze({
      serializedBytes: measured.serializedBytes,
      sourceUnits: measured.sourceUnits,
      nodes: measured.nodes
    });
  } catch {
    return null;
  }
}

function readLocalControllerState(value, operation) {
  const allowed = [
    'kind', 'version', 'workflowId', 'planJson', 'planHash', 'registryVersion',
    'runIdentity', 'limitsJson', 'limitsHash', 'budget', 'completedResultsJson',
    'pendingStepId', 'lastAdmissionKind', 'lastAdmissionStepId', 'baseCheckpointData'
  ];
  const fields = readOwnDataRecord(value, allowed, operation);
  const budget = readOwnDataRecord(fields.budget, [
    'version', 'limitsHash', 'startedWallMs', 'deadlineWallMs', 'lastWallMs',
    'elapsedMs', 'stepsAdmitted', 'stepAttempts', 'localRetries', 'replans',
    'iterations', 'totalResultBytes'
  ], operation);
  const integers = [
    'version', 'startedWallMs', 'deadlineWallMs', 'lastWallMs', 'elapsedMs',
    'stepsAdmitted', 'stepAttempts', 'localRetries', 'replans', 'iterations',
    'totalResultBytes'
  ];
  if (
    fields.kind !== LOCAL_CHECKPOINT_KIND
    || fields.version !== 2
    || typeof fields.workflowId !== 'string'
    || typeof fields.planJson !== 'string'
    || Buffer.byteLength(fields.planJson, 'utf8') > 32_768
    || typeof fields.planHash !== 'string'
    || !/^plan1_[0-9a-f]{64}$/u.test(fields.planHash)
    || typeof fields.registryVersion !== 'string'
    || !/^registry1_[0-9a-f]{64}$/u.test(fields.registryVersion)
    || typeof fields.runIdentity !== 'string'
    || !/^run1_[0-9a-f]{64}$/u.test(fields.runIdentity)
    || typeof fields.limitsJson !== 'string'
    || Buffer.byteLength(fields.limitsJson, 'utf8') > 2_048
    || typeof fields.limitsHash !== 'string'
    || !/^limits1_[0-9a-f]{64}$/u.test(fields.limitsHash)
    || typeof fields.completedResultsJson !== 'string'
    || Buffer.byteLength(fields.completedResultsJson, 'utf8') > 131_072
    || !(fields.pendingStepId === null || (typeof fields.pendingStepId === 'string' && LOCAL_STEP_ID.test(fields.pendingStepId)))
    || !['step', 'replay'].includes(fields.lastAdmissionKind)
    || typeof fields.lastAdmissionStepId !== 'string'
    || !LOCAL_STEP_ID.test(fields.lastAdmissionStepId)
    || !isRecord(fields.baseCheckpointData)
    || budget.version !== 1
    || budget.limitsHash !== fields.limitsHash
    || integers.some(key => !Number.isSafeInteger(budget[key]) || budget[key] < 0)
    || budget.deadlineWallMs <= budget.startedWallMs
    || budget.lastWallMs < budget.startedWallMs
    || budget.lastWallMs > budget.deadlineWallMs
    || budget.elapsedMs > 30_000
    || budget.deadlineWallMs - budget.startedWallMs > 30_000
    || budget.stepsAdmitted > 32
    || budget.stepAttempts > 64
    || budget.localRetries > 32
    || budget.replans > 8
    || budget.iterations > 64
    || budget.totalResultBytes > 65_536
    || budget.stepAttempts < budget.stepsAdmitted
    || budget.iterations < budget.stepAttempts
    || budget.localRetries > budget.stepAttempts
    || (fields.lastAdmissionKind === 'replay' && budget.localRetries < 1)
    || budget.replans > budget.iterations
  ) {
    fail(operation, 'CHECKPOINT_CORRUPT', 'The private local checkpoint is invalid.');
  }
  try {
    const limitSet = createLimitSet(fields.limitsJson);
    if (limitSet.hash !== fields.limitsHash) throw new Error('limit identity');
    const plan = compilePlan(fields.planJson, limitSet, undefined, fields.workflowId);
    if (
      plan.planHash !== fields.planHash
      || plan.registryVersion !== fields.registryVersion
      || plan.runIdentity !== fields.runIdentity
    ) throw new Error('plan identity');
    const validatedBudget = validateBudgetSnapshot(budget, limitSet);
    const completed = validatePrivateCheckpointResults(plan, fields.completedResultsJson);
    if (
      completed.totalResultBytes !== validatedBudget.totalResultBytes
      || validatedBudget.stepsAdmitted !== completed.resultCount + (fields.pendingStepId === null ? 0 : 1)
      || (fields.pendingStepId !== null
        && (fields.lastAdmissionStepId !== fields.pendingStepId
          || getNextReadyStep(plan, fields.completedResultsJson) !== fields.pendingStepId))
      || (fields.pendingStepId === null
        && completed.resultCount > 0
        && !completed.stepIds.includes(fields.lastAdmissionStepId))
      || (fields.pendingStepId === null
        && completed.resultCount === 0
        && (validatedBudget.stepsAdmitted !== 0 || validatedBudget.stepAttempts !== 0))
    ) throw new Error('controller history');
  } catch {
    fail(operation, 'CHECKPOINT_CORRUPT', 'The private local checkpoint is invalid.');
  }
  fields.budget = Object.freeze({ ...budget });
  return Object.freeze(fields);
}

function equalCheckpointData(left, right, operation) {
  return serializeCheckpoint(left, operation) === serializeCheckpoint(right, operation);
}

/** @internal Boolean classification only; never tags an exception or grants transaction/retry authority. */
export function classifyLocalControllerClockDiagnostic(deadlineReached, snapshotAgeExceeded) {
  if (typeof deadlineReached !== 'boolean' || typeof snapshotAgeExceeded !== 'boolean') return 'other';
  return deadlineReached && snapshotAgeExceeded ? 'deadline_and_age'
    : deadlineReached ? 'deadline_reached' : snapshotAgeExceeded ? 'snapshot_age_exceeded' : 'other';
}


function assertLocalControllerTransactionClock(now, next, row, previous, operation, transactionElapsedMs = null) {
  const persistedLastWallMs = Math.max(
    Number(row.updated_at_ms),
    previous?.budget.lastWallMs ?? next.budget.startedWallMs
  );
  const lowerBound = Math.max(next.budget.lastWallMs, persistedLastWallMs);
  const rejectClock = (code, safe_message, clockGuard) => {
    const error = runtimeError({ module: 'state', operation, code, safe_message, retryable: false });
    LOCAL_CONTROLLER_CLOCK_DIAGNOSTICS.set(error, clockGuard);
    throw error;
  };
  if (now < lowerBound) {
    rejectClock('CLOCK_ROLLBACK', 'The local controller clock moved backwards during a transaction.', 'clock_rollback');
  }
  if (now >= next.budget.deadlineWallMs) {
    rejectClock('INVALID_INPUT', 'The local step clock snapshot is outside its supported boundary.',
      classifyLocalControllerClockDiagnostic(true, now - next.budget.lastWallMs > 250));
  }
  const forwardMs = now - next.budget.lastWallMs;
  if (forwardMs > 250) {
    if (operation === '_localControllerComplete'
        && transactionElapsedMs !== null
        && transactionElapsedMs > 250
        && Math.abs(forwardMs - transactionElapsedMs) <= 50) {
      const error = runtimeError({
        module: 'state',
        operation,
        code: 'CONCURRENT_MODIFICATION',
        retryable: true,
        safe_message: 'The local completion clock sample expired while waiting to enter its transaction.'
      });
      LOCAL_CONTROLLER_CLOCK_DIAGNOSTICS.set(error, 'snapshot_age_exceeded');
      LOCAL_COMPLETION_CLOCK_STALE_ERRORS.add(error);
      throw error;
    }
    rejectClock('INVALID_INPUT', 'The local step clock snapshot is outside its supported boundary.', 'snapshot_age_exceeded');
  }
}

function assertPrivateCheckpointPins(previous, next, workflowId, operation) {
  if (
    next.workflowId !== workflowId
    || previous.workflowId !== workflowId
    || previous.kind !== next.kind
    || previous.version !== next.version
    || previous.planJson !== next.planJson
    || previous.planHash !== next.planHash
    || previous.registryVersion !== next.registryVersion
    || previous.runIdentity !== next.runIdentity
    || previous.limitsJson !== next.limitsJson
    || previous.limitsHash !== next.limitsHash
    || !equalCheckpointData(previous.baseCheckpointData, next.baseCheckpointData, operation)
  ) {
    fail(operation, 'CHECKPOINT_CORRUPT', 'The private local checkpoint identity changed.');
  }
}

function assertAdmissionBudget(previous, next, kind, stepId, operation) {
  const before = previous?.budget;
  const after = next.budget;
  if (next.pendingStepId !== stepId || next.lastAdmissionKind !== kind || next.lastAdmissionStepId !== stepId) {
    fail(operation, 'INVALID_INPUT', 'The local step admission is invalid.');
  }
  if (before === undefined) {
    if (
      kind !== 'step'
      || after.stepsAdmitted !== 1
      || after.stepAttempts !== 1
      || after.localRetries !== 0
      || after.replans !== 0
      || after.iterations !== 1
      || after.totalResultBytes !== 0
      || next.completedResultsJson !== '{}'
    ) {
      fail(operation, 'CHECKPOINT_CORRUPT', 'The initial local step allowance is invalid.');
    }
    return;
  }
  if (
    before.startedWallMs !== after.startedWallMs
    || before.deadlineWallMs !== after.deadlineWallMs
    || before.limitsHash !== after.limitsHash
    || after.lastWallMs < before.lastWallMs
    || after.elapsedMs < before.elapsedMs
    || after.stepsAdmitted !== before.stepsAdmitted + (kind === 'step' ? 1 : 0)
    || after.stepAttempts !== before.stepAttempts + 1
    || after.localRetries !== before.localRetries + (kind === 'replay' ? 1 : 0)
    || after.replans !== before.replans
    || after.iterations !== before.iterations + 1
    || after.totalResultBytes !== before.totalResultBytes
    || next.completedResultsJson !== previous.completedResultsJson
    || (kind === 'step' && previous.pendingStepId !== null)
    || (kind === 'replay' && previous.pendingStepId !== stepId)
  ) {
    fail(operation, 'CHECKPOINT_CORRUPT', 'The local step counters cannot be reset or rebound.');
  }
}

function assertCompletionBudget(previous, next, stepId, operation) {
  const before = previous.budget;
  const after = next.budget;
  if (
    previous.pendingStepId !== stepId
    || previous.lastAdmissionStepId !== stepId
    || next.pendingStepId !== null
    || next.lastAdmissionKind !== previous.lastAdmissionKind
    || next.lastAdmissionStepId !== stepId
    || before.startedWallMs !== after.startedWallMs
    || before.deadlineWallMs !== after.deadlineWallMs
    || before.limitsHash !== after.limitsHash
    || after.lastWallMs < before.lastWallMs
    || after.elapsedMs < before.elapsedMs
    || after.stepsAdmitted !== before.stepsAdmitted
    || after.stepAttempts !== before.stepAttempts
    || after.localRetries !== before.localRetries
    || after.replans !== before.replans
    || after.iterations !== before.iterations
    || after.totalResultBytes < before.totalResultBytes
    || next.completedResultsJson === previous.completedResultsJson
  ) {
    fail(operation, 'CHECKPOINT_CORRUPT', 'The local step completion does not match its persisted admission.');
  }
  let beforeResults;
  let afterResults;
  try {
    beforeResults = JSON.parse(previous.completedResultsJson);
    afterResults = JSON.parse(next.completedResultsJson);
  } catch {
    fail(operation, 'CHECKPOINT_CORRUPT', 'The local result checkpoint is invalid.');
  }
  if (
    !isRecord(beforeResults)
    || !isRecord(afterResults)
    || Object.hasOwn(beforeResults, stepId)
    || Object.keys(afterResults).length !== Object.keys(beforeResults).length + 1
    || !Object.hasOwn(afterResults, stepId)
    || Object.keys(beforeResults).some(key => !Object.hasOwn(afterResults, key)
      || !equalCheckpointData(beforeResults[key], afterResults[key], operation))
  ) {
    fail(operation, 'CHECKPOINT_CORRUPT', 'The local completion must add exactly its admitted result.');
  }
}

function readOwnDataRecord(value, allowedKeys, operation, requiredKeys = allowedKeys) {
  if (value === null || typeof value !== 'object') {
    fail(operation, 'INVALID_INPUT', 'The provider observation is invalid or exceeds its safety bounds.');
  }
  try {
    if (utilTypes.isProxy(value) || Array.isArray(value)) {
      fail(operation, 'INVALID_INPUT', 'The provider observation is invalid or exceeds its safety bounds.');
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      fail(operation, 'INVALID_INPUT', 'The provider observation is invalid or exceeds its safety bounds.');
    }
    const keys = Reflect.ownKeys(value);
    if (keys.length > allowedKeys.length
        || keys.some(key => typeof key !== 'string' || !allowedKeys.includes(key))) {
      fail(operation, 'INVALID_INPUT', 'The provider observation is invalid or exceeds its safety bounds.');
    }
    const result = Object.create(null);
    for (const key of keys) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) {
        fail(operation, 'INVALID_INPUT', 'The provider observation is invalid or exceeds its safety bounds.');
      }
      result[key] = descriptor.value;
    }
    if (requiredKeys.some(key => !Object.hasOwn(result, key))) {
      fail(operation, 'INVALID_INPUT', 'The provider observation is invalid or exceeds its safety bounds.');
    }
    return result;
  } catch (error) {
    if (error instanceof RuntimeError) throw error;
    fail(operation, 'INVALID_INPUT', 'The provider observation is invalid or exceeds its safety bounds.');
  }
}

function safeJevObservationInput(input, operation) {
  const fields = readOwnDataRecord(input, [
    'workflowId', 'attemptId', 'logicalOperationRef', 'transportAttemptId',
    'sourceObservationRef', 'dispatchCertainty', 'requestedModel', 'resolvedModel',
    'providerRequestId', 'decisionConformance', 'usage'
  ], operation);
  for (const key of [
    'workflowId', 'attemptId', 'logicalOperationRef', 'transportAttemptId', 'sourceObservationRef'
  ]) {
    if (typeof fields[key] !== 'string' || !SAFE_OPAQUE_REF.test(fields[key])) {
      fail(operation, 'INVALID_INPUT', 'A provider observation identity is invalid.');
    }
  }
  if (fields.requestedModel !== JEV_MODEL_ID
      || (fields.resolvedModel !== null
        && (typeof fields.resolvedModel !== 'string' || !SAFE_MODEL_ID.test(fields.resolvedModel)))
      || (fields.providerRequestId !== null
        && (typeof fields.providerRequestId !== 'string' || !SAFE_PROVIDER_REQUEST_ID.test(fields.providerRequestId)))) {
    fail(operation, 'INVALID_INPUT', 'A provider model or request identity is invalid.');
  }
  if (!['not_dispatched', 'possible', 'response_received'].includes(fields.dispatchCertainty)
      || !['valid', 'invalid', 'unavailable'].includes(fields.decisionConformance)) {
    fail(operation, 'INVALID_INPUT', 'The provider observation classification is invalid.');
  }

  const usageFields = readOwnDataRecord(
    fields.usage,
    ['kind', 'inputTokens', 'outputTokens', 'reason'],
    operation,
    ['kind']
  );
  let usage;
  if (usageFields.kind === 'known') {
    if (Object.keys(usageFields).length !== 3
        || !Object.hasOwn(usageFields, 'inputTokens')
        || !Object.hasOwn(usageFields, 'outputTokens')
        || Object.hasOwn(usageFields, 'reason')
        || !Number.isSafeInteger(usageFields.inputTokens) || usageFields.inputTokens < 0
        || !Number.isSafeInteger(usageFields.outputTokens) || usageFields.outputTokens < 0
        || fields.dispatchCertainty !== 'response_received') {
      fail(operation, 'INVALID_INPUT', 'Known provider token usage is invalid.');
    }
    usage = Object.freeze({
      kind: 'known',
      inputTokens: usageFields.inputTokens,
      outputTokens: usageFields.outputTokens
    });
  } else if (usageFields.kind === 'unknown') {
    if (Object.keys(usageFields).length !== 2
        || !Object.hasOwn(usageFields, 'reason')
        || Object.hasOwn(usageFields, 'inputTokens')
        || Object.hasOwn(usageFields, 'outputTokens')
        || typeof usageFields.reason !== 'string'
        || !JEV_UNKNOWN_REASONS.has(usageFields.reason)) {
      fail(operation, 'INVALID_INPUT', 'Unknown provider token usage requires an explicit supported reason.');
    }
    const validUnknownReason = fields.dispatchCertainty === 'not_dispatched'
      ? usageFields.reason === 'not_dispatched'
      : fields.dispatchCertainty === 'possible'
        ? ['possible_dispatch', 'recovered_after_crash'].includes(usageFields.reason)
        : usageFields.reason === 'missing_or_invalid_usage';
    if (!validUnknownReason) {
      fail(operation, 'INVALID_INPUT', 'The unknown usage reason conflicts with the dispatch evidence.');
    }
    usage = Object.freeze({ kind: 'unknown', reason: usageFields.reason });
  } else {
    fail(operation, 'INVALID_INPUT', 'The provider usage classification is invalid.');
  }

  if (fields.decisionConformance === 'valid'
      && (fields.dispatchCertainty !== 'response_received'
        || usage.kind !== 'known'
        || fields.resolvedModel !== fields.requestedModel)) {
    fail(operation, 'INVALID_INPUT', 'A conforming provider decision requires a matching model and known usage.');
  }
  if (fields.dispatchCertainty === 'not_dispatched'
      && fields.decisionConformance === 'valid') {
    fail(operation, 'INVALID_INPUT', 'A non-dispatched provider attempt cannot contain a conforming decision.');
  }

  return Object.freeze({
    workflowId: fields.workflowId,
    attemptId: fields.attemptId,
    logicalOperationRef: fields.logicalOperationRef,
    transportAttemptId: fields.transportAttemptId,
    sourceObservationRef: fields.sourceObservationRef,
    dispatchCertainty: fields.dispatchCertainty,
    requestedModel: fields.requestedModel,
    resolvedModel: fields.resolvedModel,
    providerRequestId: fields.providerRequestId,
    decisionConformance: fields.decisionConformance,
    usage
  });
}

function observationFingerprint(observation) {
  return digest(JSON.stringify([
    observation.sourceObservationRef,
    observation.workflowId,
    observation.logicalOperationRef,
    observation.attemptId,
    observation.transportAttemptId,
    observation.dispatchCertainty,
    observation.requestedModel,
    observation.resolvedModel,
    observation.providerRequestId,
    observation.decisionConformance,
    observation.usage.kind,
    observation.usage.kind === 'known'
      ? observation.usage.inputTokens
      : observation.usage.reason,
    observation.usage.kind === 'known' ? observation.usage.outputTokens : null
  ]));
}

function mapJevObservation(row) {
  if (!row) return null;
  const usage = row.usage_kind === 'known'
    ? Object.freeze({
        kind: 'known',
        inputTokens: Number(row.input_tokens),
        outputTokens: Number(row.output_tokens)
      })
    : Object.freeze({ kind: 'unknown', reason: row.unknown_reason });
  const observation = {
    schema: 'agent-harness.jev.local-usage-observation.v1',
    sourceObservationRef: row.source_observation_ref,
    workflowId: row.workflow_id,
    logicalOperationRef: row.logical_operation_ref,
    attemptId: row.attempt_id,
    transportAttemptId: row.transport_attempt_id,
    dispatchCertainty: row.dispatch_certainty,
    requestedModel: row.requested_model,
    resolvedModel: row.resolved_model,
    providerRequestId: row.provider_request_id,
    decisionConformance: row.decision_conformance,
    usage,
    eventId: row.event_id,
    eventSequence: Number(row.event_sequence),
    recordedAtMs: Number(row.recorded_at_ms)
  };
  if (observation.eventId !== 'jev-local-observation-v1:' + digest(observation.sourceObservationRef)
      || observationFingerprint(observation) !== row.fingerprint
      || !Number.isSafeInteger(observation.eventSequence)
      || !Number.isSafeInteger(observation.recordedAtMs)) {
    throw new Error('invalid provider observation row');
  }
  return Object.freeze(observation);
}

function digest(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

function mapStorageError(operation, error) {
  recordOperationsDiagnosticError('storage_origin', error, {
    operation, localClockGuard: readLocalControllerClockDiagnostic(error) ?? 'other'
  });
  if (error instanceof RuntimeError) return error;
  const code = typeof error?.code === 'string' ? error.code : '';
  const sqlitePrimaryCode = Number.isInteger(error?.errcode) ? error.errcode & 0xff : 0;
  if (/BUSY|LOCKED/i.test(code) || sqlitePrimaryCode === 5 || sqlitePrimaryCode === 6) {
    return runtimeError({
      module: 'state',
      operation,
      code: 'STORE_BUSY',
      retryable: true,
      safe_message: 'The durable store is busy; retry after the current write finishes.'
    });
  }
  return runtimeError({
    module: 'state',
    operation,
    code: operation === 'open' ? 'STORE_OPEN_FAILED' : 'STORE_WRITE_FAILED',
    retryable: false,
    safe_message: operation === 'open'
      ? 'The durable store could not be opened safely.'
      : 'The durable state change could not be committed.'
  });
}

function isRawSqliteBusyPrimary(error) {
  if (!utilTypes.isNativeError(error) || error instanceof RuntimeError) return false;
  let code;
  let errcode;
  try {
    code = Object.getOwnPropertyDescriptor(error, 'code')?.value;
    errcode = Object.getOwnPropertyDescriptor(error, 'errcode')?.value;
  } catch {
    return false;
  }
  return code === 'ERR_SQLITE_ERROR'
    && Number.isSafeInteger(errcode)
    && errcode >= 0
    && errcode <= 0x7fffffff
    && (errcode & 0xff) === 5;
}

function sqliteBusyPrimaryCode(error) {
  if (!utilTypes.isNativeError(error) || error instanceof RuntimeError) return null;
  let code;
  let errcode;
  try {
    code = Object.getOwnPropertyDescriptor(error, 'code')?.value;
    errcode = Object.getOwnPropertyDescriptor(error, 'errcode')?.value;
  } catch {
    return null;
  }
  if (code !== 'ERR_SQLITE_ERROR' || !Number.isSafeInteger(errcode)
      || errcode < 0 || errcode > 0x7fffffff) return null;
  const primary = errcode & 0xff;
  return primary === 5 || primary === 6 ? primary : null;
}

/** @internal Passive origin tag only; never grants retry, rollback or transaction authority. */
export function readLocalControllerClockDiagnostic(error) {
  if (error === null || typeof error !== 'object' || utilTypes.isProxy(error)) return null;
  return LOCAL_CONTROLLER_CLOCK_DIAGNOSTICS.get(error) ?? null;
}

/** @internal Read the fixed native BUSY boundary captured by private operations admission. */
export function readOperationsAdmissionBusyDiagnostic(error) {
  if (error === null || typeof error !== 'object' || utilTypes.isProxy(error)) return null;
  return OPERATIONS_ADMISSION_BUSY_DIAGNOSTICS.get(error) ?? null;
}

function inTransaction(db, operation, callback) {
  let boundary = 'begin';
  let transactionActive = null;
  let rollbackDisposition = 'not_observed';
  try {
    db.exec('BEGIN IMMEDIATE');
    try {
      boundary = 'body';
      const value = callback();
      boundary = 'commit';
      db.exec('COMMIT');
      return value;
    } catch (error) {
      const failureBoundary = boundary;
      try {
        const active = db.isTransaction;
        transactionActive = typeof active === 'boolean' ? active : null;
        if (active) {
          boundary = 'rollback';
          rollbackDisposition = 'rollback_attempted';
          db.exec('ROLLBACK');
          rollbackDisposition = 'rolled_back';
        } else rollbackDisposition = transactionActive === false ? 'inactive' : 'unknown';
      } catch (rollbackError) {
        boundary = 'rollback';
        rollbackDisposition = transactionActive === true ? 'rollback_failed' : 'rollback_check_failed';
        throw rollbackError;
      }
      boundary = failureBoundary;
      throw error;
    }
  } catch (error) {
    const mapped = mapStorageError(operation, error);
    const sqlitePrimaryCode = sqliteBusyPrimaryCode(error);
    if (operation === '_operationsAdmit' && mapped instanceof RuntimeError
        && mapped.code === 'STORE_BUSY' && sqlitePrimaryCode !== null) {
      OPERATIONS_ADMISSION_BUSY_DIAGNOSTICS.set(mapped, Object.freeze({
        transactionBoundary: boundary,
        sqlitePrimaryCode,
        transactionActive,
        rollbackDisposition
      }));
    }
    throw mapped;
  }
}

/** @internal Transaction boundary used only by the private local completion path. */
export function inLocalCompletionTransaction(db, operation, callback) {
  let began = false;
  let startedFromInactive = false;
  try {
    startedFromInactive = db.isTransaction === false;
    db.exec('BEGIN IMMEDIATE');
    began = true;
    const value = callback();
    db.exec('COMMIT');
    return value;
  } catch (primaryError) {
    const rawBusyPrimary = isRawSqliteBusyPrimary(primaryError);
    const mappedPrimary = mapStorageError(operation, primaryError);
    const completionClockStale = LOCAL_COMPLETION_CLOCK_STALE_ERRORS.delete(primaryError)
      || LOCAL_COMPLETION_CLOCK_STALE_ERRORS.delete(mappedPrimary);
    const inheritedEligibility = CONFIRMED_LOCAL_COMPLETION_BUSY_ERRORS.has(mappedPrimary);
    CONFIRMED_LOCAL_COMPLETION_BUSY_ERRORS.delete(mappedPrimary);
    let rollbackError = null;
    let transactionState;
    try {
      transactionState = db.isTransaction;
    } catch {
      transactionState = undefined;
    }
    if (began && transactionState === true) {
      try {
        db.exec('ROLLBACK');
      } catch (error) {
        rollbackError = error;
      }
    }
    if (rollbackError) {
      const mappedRollback = mapStorageError(operation, rollbackError);
      CONFIRMED_LOCAL_COMPLETION_BUSY_ERRORS.delete(mappedRollback);
      throw mappedRollback;
    }
    let inactive = false;
    try {
      inactive = db.isTransaction === false;
    } catch {
      inactive = false;
    }
    const dispositionKnown = transactionState === false
      || (began && transactionState === true && rollbackError === null);
    if (
      !inheritedEligibility
      && rawBusyPrimary
      && startedFromInactive
      && dispositionKnown
      && inactive
      && mappedPrimary instanceof RuntimeError
      && mappedPrimary.module === 'state'
      && mappedPrimary.operation === operation
      && mappedPrimary.code === 'STORE_BUSY'
      && mappedPrimary.retryable === true
    ) {
      CONFIRMED_LOCAL_COMPLETION_BUSY_ERRORS.add(mappedPrimary);
    }
    if (
      completionClockStale
      && began
      && startedFromInactive
      && dispositionKnown
      && inactive
      && mappedPrimary instanceof RuntimeError
      && mappedPrimary.module === 'state'
      && mappedPrimary.operation === operation
      && mappedPrimary.code === 'CONCURRENT_MODIFICATION'
      && mappedPrimary.retryable === true
    ) {
      CONFIRMED_LOCAL_COMPLETION_CLOCK_STALE_ERRORS.add(mappedPrimary);
    }
    throw mappedPrimary;
  }
}

/** @internal Local admission BUSY is retry-eligible only after its exact transaction is inactive. */
export function inLocalAdmissionTransaction(db, operation, callback) {
  let began = false;
  let startedFromInactive = false;
  try {
    startedFromInactive = db.isTransaction === false;
    db.exec('BEGIN IMMEDIATE');
    began = true;
    const value = callback();
    db.exec('COMMIT');
    return value;
  } catch (primaryError) {
    const rawBusyPrimary = isRawSqliteBusyPrimary(primaryError);
    const mappedPrimary = mapStorageError(operation, primaryError);
    const inheritedEligibility = CONFIRMED_LOCAL_ADMISSION_BUSY_ERRORS.has(mappedPrimary);
    CONFIRMED_LOCAL_ADMISSION_BUSY_ERRORS.delete(mappedPrimary);
    let rollbackError = null;
    let transactionState;
    try {
      transactionState = db.isTransaction;
    } catch {
      transactionState = undefined;
    }
    if (began && transactionState === true) {
      try {
        db.exec('ROLLBACK');
      } catch (error) {
        rollbackError = error;
      }
    }
    if (rollbackError) {
      const mappedRollback = mapStorageError(operation, rollbackError);
      CONFIRMED_LOCAL_ADMISSION_BUSY_ERRORS.delete(mappedRollback);
      throw mappedRollback;
    }
    let inactive = false;
    try {
      inactive = db.isTransaction === false;
    } catch {
      inactive = false;
    }
    const dispositionKnown = transactionState === false
      || (began && transactionState === true && rollbackError === null);
    if (
      !inheritedEligibility
      && operation === '_localControllerAdmit'
      && rawBusyPrimary
      && startedFromInactive
      && dispositionKnown
      && inactive
      && mappedPrimary instanceof RuntimeError
      && mappedPrimary.module === 'state'
      && mappedPrimary.operation === operation
      && mappedPrimary.code === 'STORE_BUSY'
      && mappedPrimary.retryable === true
    ) {
      CONFIRMED_LOCAL_ADMISSION_BUSY_ERRORS.add(mappedPrimary);
    }
    throw mappedPrimary;
  }
}

function withConsistentReadSnapshot(db, operation, callback) {
  try {
    db.exec('BEGIN DEFERRED');
    try {
      const value = callback();
      db.exec('COMMIT');
      return value;
    } catch (error) {
      if (db.isTransaction) db.exec('ROLLBACK');
      throw error;
    }
  } catch (error) {
    throw mapStorageError(operation, error);
  }
}

function openSqliteConstructor() {
  const [major, minor, patch] = process.versions.node.split('.').map(Number);
  if (major !== 22 || minor < 23 || (minor === 23 && patch < 3)) {
    fail('open', 'UNSUPPORTED_RUNTIME', 'The durable store requires the validated Node 22.23.3 runtime line.');
  }
  let DatabaseSync;
  try {
    ({ DatabaseSync } = requireBuiltin('node:sqlite'));
  } catch {
    fail('open', 'SQLITE_UNSUPPORTED', 'This Node runtime does not provide the required built-in SQLite API.');
  }
  if (typeof DatabaseSync !== 'function') {
    fail('open', 'SQLITE_UNSUPPORTED', 'This Node runtime does not provide the required built-in SQLite API.');
  }
  return DatabaseSync;
}

function createSchemaV3(db) {
  db.exec(`
    CREATE TABLE workflows (
      workflow_id TEXT PRIMARY KEY,
      state TEXT NOT NULL CHECK (state IN ('queued','running','awaiting_response','reconciliation_pending','review_required','succeeded','failed','cancelled')),
      revision INTEGER NOT NULL CHECK (revision >= 0),
      fence_token INTEGER NOT NULL CHECK (fence_token >= 0),
      lease_owner TEXT,
      lease_expires_ms INTEGER,
      checkpoint_sequence INTEGER NOT NULL CHECK (checkpoint_sequence >= 1),
      cancellation_requested INTEGER NOT NULL CHECK (cancellation_requested IN (0,1)),
      created_at_ms INTEGER NOT NULL,
      updated_at_ms INTEGER NOT NULL
    ) STRICT;

    CREATE TABLE workflow_checkpoints (
      workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id),
      sequence INTEGER NOT NULL CHECK (sequence >= 1),
      schema_version INTEGER NOT NULL,
      payload_json TEXT NOT NULL,
      sha256 TEXT NOT NULL,
      revision INTEGER NOT NULL,
      created_at_ms INTEGER NOT NULL,
      PRIMARY KEY (workflow_id, sequence)
    ) STRICT, WITHOUT ROWID;

    CREATE TABLE workflow_events (
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id TEXT NOT NULL UNIQUE,
      workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id),
      revision INTEGER NOT NULL,
      event_type TEXT NOT NULL,
      from_state TEXT,
      to_state TEXT,
      fence_token INTEGER NOT NULL,
      occurred_at_ms INTEGER NOT NULL
    ) STRICT;

    CREATE TABLE outbox (
      event_id TEXT PRIMARY KEY REFERENCES workflow_events(event_id),
      event_sequence INTEGER NOT NULL UNIQUE REFERENCES workflow_events(sequence),
      delivered_at_ms INTEGER,
      delivery_attempts INTEGER NOT NULL DEFAULT 0 CHECK (delivery_attempts >= 0)
    ) STRICT, WITHOUT ROWID;

    CREATE TABLE operation_attempts (
      attempt_id TEXT PRIMARY KEY,
      workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id),
      logical_operation_ref TEXT NOT NULL,
      authority_hold_ref TEXT NOT NULL,
      state TEXT NOT NULL CHECK (state IN ('intent','outcome_unknown','response_received','reconciled')),
      created_at_ms INTEGER NOT NULL,
      updated_at_ms INTEGER NOT NULL
    ) STRICT;

    CREATE TABLE transport_attempts (
      transport_attempt_id TEXT PRIMARY KEY,
      attempt_id TEXT NOT NULL REFERENCES operation_attempts(attempt_id),
      created_at_ms INTEGER NOT NULL
    ) STRICT;

    CREATE TABLE response_receipts (
      receipt_id TEXT PRIMARY KEY,
      attempt_id TEXT NOT NULL UNIQUE REFERENCES operation_attempts(attempt_id),
      outcome TEXT NOT NULL CHECK (outcome IN ('completed','no_effect')),
      next_state TEXT NOT NULL CHECK (next_state IN ('running','succeeded','failed')),
      response_bytes BLOB NOT NULL,
      response_sha256 TEXT NOT NULL,
      next_checkpoint_json TEXT NOT NULL,
      next_checkpoint_sha256 TEXT,
      created_at_ms INTEGER NOT NULL
    ) STRICT;

    CREATE TABLE attempt_observations (
      source_observation_ref TEXT PRIMARY KEY,
      attempt_id TEXT NOT NULL REFERENCES operation_attempts(attempt_id),
      observation_kind TEXT NOT NULL CHECK (observation_kind IN ('known','unknown')),
      fingerprint TEXT NOT NULL,
      observed_at_ms INTEGER NOT NULL
    ) STRICT, WITHOUT ROWID;

    CREATE TABLE immutable_artifacts (
      workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id),
      artifact_id TEXT NOT NULL,
      sha256 TEXT NOT NULL,
      bytes BLOB NOT NULL,
      created_at_ms INTEGER NOT NULL,
      PRIMARY KEY (workflow_id, artifact_id)
    ) STRICT, WITHOUT ROWID;

    CREATE TRIGGER workflow_events_no_update
      BEFORE UPDATE ON workflow_events BEGIN SELECT RAISE(ABORT, 'event log is immutable'); END;
    CREATE TRIGGER workflow_events_no_delete
      BEFORE DELETE ON workflow_events BEGIN SELECT RAISE(ABORT, 'event log is immutable'); END;
    CREATE TRIGGER workflow_checkpoints_no_update
      BEFORE UPDATE ON workflow_checkpoints BEGIN SELECT RAISE(ABORT, 'checkpoints are immutable'); END;
    CREATE TRIGGER workflow_checkpoints_no_delete
      BEFORE DELETE ON workflow_checkpoints BEGIN SELECT RAISE(ABORT, 'checkpoints are immutable'); END;
    CREATE TRIGGER immutable_artifacts_no_update
      BEFORE UPDATE ON immutable_artifacts BEGIN SELECT RAISE(ABORT, 'artifacts are immutable'); END;
    CREATE TRIGGER immutable_artifacts_no_delete
      BEFORE DELETE ON immutable_artifacts BEGIN SELECT RAISE(ABORT, 'artifacts are immutable'); END;
    CREATE TRIGGER response_receipts_no_update
      BEFORE UPDATE ON response_receipts BEGIN SELECT RAISE(ABORT, 'response receipts are immutable'); END;
    CREATE TRIGGER response_receipts_no_delete
      BEFORE DELETE ON response_receipts BEGIN SELECT RAISE(ABORT, 'response receipts are immutable'); END;
  `);
}

function createTelemetryCursorSchema(db) {
  db.exec(`
    CREATE TABLE telemetry_cursors (
      consumer_id TEXT PRIMARY KEY,
      last_sequence INTEGER NOT NULL CHECK (last_sequence >= 0),
      updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= 0)
    ) STRICT, WITHOUT ROWID;
  `);
}

function createJevObservationSchema(db) {
  db.exec(`
    CREATE TABLE jev_dispatch_markers (
      transport_attempt_id TEXT PRIMARY KEY REFERENCES transport_attempts(transport_attempt_id),
      attempt_id TEXT NOT NULL REFERENCES operation_attempts(attempt_id),
      workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id),
      logical_operation_ref TEXT NOT NULL,
      event_id TEXT NOT NULL UNIQUE REFERENCES workflow_events(event_id),
      event_sequence INTEGER NOT NULL UNIQUE REFERENCES workflow_events(sequence),
      created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0)
    ) STRICT, WITHOUT ROWID;

    CREATE TRIGGER jev_dispatch_markers_validate_identity
      BEFORE INSERT ON jev_dispatch_markers BEGIN
        SELECT CASE WHEN NOT EXISTS (
          SELECT 1
          FROM operation_attempts a
          JOIN transport_attempts t ON t.attempt_id = a.attempt_id
          JOIN workflows w ON w.workflow_id = a.workflow_id
          WHERE a.attempt_id = NEW.attempt_id
            AND t.transport_attempt_id = NEW.transport_attempt_id
            AND a.workflow_id = NEW.workflow_id
            AND a.logical_operation_ref = NEW.logical_operation_ref
            AND a.state = 'intent'
            AND w.state = 'awaiting_response'
            AND w.cancellation_requested = 0
        ) THEN RAISE(ABORT, 'JEV dispatch marker identity mismatch') END;
      END;
    CREATE TRIGGER jev_dispatch_markers_no_update
      BEFORE UPDATE ON jev_dispatch_markers BEGIN SELECT RAISE(ABORT, 'JEV dispatch markers are immutable'); END;
    CREATE TRIGGER jev_dispatch_markers_no_delete
      BEFORE DELETE ON jev_dispatch_markers BEGIN SELECT RAISE(ABORT, 'JEV dispatch markers are immutable'); END;

    CREATE TABLE jev_provider_observations (
      source_observation_ref TEXT PRIMARY KEY,
      workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id),
      logical_operation_ref TEXT NOT NULL,
      attempt_id TEXT NOT NULL REFERENCES operation_attempts(attempt_id),
      transport_attempt_id TEXT NOT NULL REFERENCES transport_attempts(transport_attempt_id),
      dispatch_certainty TEXT NOT NULL CHECK (dispatch_certainty IN ('not_dispatched','possible','response_received')),
      requested_model TEXT NOT NULL CHECK (requested_model = 'jev-1.13.0'),
      resolved_model TEXT,
      provider_request_id TEXT,
      decision_conformance TEXT NOT NULL CHECK (decision_conformance IN ('valid','invalid','unavailable')),
      usage_kind TEXT NOT NULL CHECK (usage_kind IN ('known','unknown')),
      input_tokens INTEGER,
      output_tokens INTEGER,
      unknown_reason TEXT,
      event_id TEXT NOT NULL UNIQUE REFERENCES workflow_events(event_id),
      event_sequence INTEGER NOT NULL UNIQUE REFERENCES workflow_events(sequence),
      fingerprint TEXT NOT NULL CHECK (length(fingerprint) = 64 AND fingerprint NOT GLOB '*[^0-9a-f]*'),
      recorded_at_ms INTEGER NOT NULL CHECK (recorded_at_ms >= 0),
      CHECK (
        (usage_kind = 'known' AND input_tokens IS NOT NULL AND input_tokens >= 0
          AND output_tokens IS NOT NULL AND output_tokens >= 0 AND unknown_reason IS NULL
          AND dispatch_certainty = 'response_received')
        OR
        (usage_kind = 'unknown' AND input_tokens IS NULL AND output_tokens IS NULL
          AND unknown_reason IS NOT NULL
          AND (
            (dispatch_certainty = 'not_dispatched' AND unknown_reason = 'not_dispatched')
            OR (dispatch_certainty = 'possible' AND unknown_reason IN ('possible_dispatch','recovered_after_crash'))
            OR (dispatch_certainty = 'response_received' AND unknown_reason = 'missing_or_invalid_usage')
          ))
      ),
      CHECK (decision_conformance != 'valid'
        OR (dispatch_certainty = 'response_received' AND usage_kind = 'known'
          AND resolved_model = requested_model))
    ) STRICT, WITHOUT ROWID;

    CREATE INDEX jev_provider_observations_attempt_transport_idx
      ON jev_provider_observations (attempt_id, transport_attempt_id, recorded_at_ms);

    CREATE TRIGGER jev_provider_observations_validate_identity
      BEFORE INSERT ON jev_provider_observations BEGIN
        SELECT CASE WHEN NOT EXISTS (
          SELECT 1
          FROM operation_attempts a
          JOIN transport_attempts t ON t.attempt_id = a.attempt_id
          WHERE a.attempt_id = NEW.attempt_id
            AND t.transport_attempt_id = NEW.transport_attempt_id
            AND a.workflow_id = NEW.workflow_id
            AND a.logical_operation_ref = NEW.logical_operation_ref
        ) THEN RAISE(ABORT, 'JEV provider observation identity mismatch') END;
        SELECT CASE WHEN NEW.dispatch_certainty IN ('possible','response_received')
          AND NOT EXISTS (
            SELECT 1 FROM jev_dispatch_markers m
            WHERE m.attempt_id = NEW.attempt_id
              AND m.transport_attempt_id = NEW.transport_attempt_id
              AND m.workflow_id = NEW.workflow_id
          )
          THEN RAISE(ABORT, 'JEV provider observation has no durable dispatch marker') END;
      END;
    CREATE TRIGGER jev_provider_observations_no_update
      BEFORE UPDATE ON jev_provider_observations BEGIN SELECT RAISE(ABORT, 'JEV provider observations are immutable'); END;
    CREATE TRIGGER jev_provider_observations_no_delete
      BEFORE DELETE ON jev_provider_observations BEGIN SELECT RAISE(ABORT, 'JEV provider observations are immutable'); END;

    CREATE TABLE jev_local_observation_outbox (
      event_id TEXT PRIMARY KEY REFERENCES workflow_events(event_id),
      event_sequence INTEGER NOT NULL UNIQUE REFERENCES workflow_events(sequence),
      source_observation_ref TEXT NOT NULL UNIQUE REFERENCES jev_provider_observations(source_observation_ref),
      delivered_at_ms INTEGER CHECK (delivered_at_ms IS NULL OR delivered_at_ms >= 0)
    ) STRICT, WITHOUT ROWID;

    CREATE TRIGGER jev_local_observation_outbox_validate_identity
      BEFORE INSERT ON jev_local_observation_outbox BEGIN
        SELECT CASE WHEN NOT EXISTS (
          SELECT 1 FROM jev_provider_observations o
          WHERE o.source_observation_ref = NEW.source_observation_ref
            AND o.event_id = NEW.event_id
            AND o.event_sequence = NEW.event_sequence
        ) THEN RAISE(ABORT, 'JEV local outbox identity mismatch') END;
      END;
    CREATE TRIGGER jev_local_observation_outbox_no_identity_update
      BEFORE UPDATE ON jev_local_observation_outbox
      WHEN NEW.event_id != OLD.event_id
        OR NEW.event_sequence != OLD.event_sequence
        OR NEW.source_observation_ref != OLD.source_observation_ref
        OR OLD.delivered_at_ms IS NOT NULL
        OR NEW.delivered_at_ms IS NULL
      BEGIN SELECT RAISE(ABORT, 'JEV local outbox identity is immutable'); END;
    CREATE TRIGGER jev_local_observation_outbox_no_delete
      BEFORE DELETE ON jev_local_observation_outbox BEGIN SELECT RAISE(ABORT, 'JEV local outbox is immutable'); END;
  `);
}

function createOperationsSchema(db) {
  db.exec(`
    CREATE TABLE operations_meta (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      pending_records INTEGER NOT NULL CHECK (pending_records >= 0),
      pending_bytes INTEGER NOT NULL CHECK (pending_bytes >= 0),
      in_flight INTEGER NOT NULL CHECK (in_flight >= 0),
      reserved_event_slots INTEGER NOT NULL CHECK (reserved_event_slots >= 0),
      reserved_disk_bytes INTEGER NOT NULL CHECK (reserved_disk_bytes >= 0),
      last_tenant_id TEXT,
      policy_json TEXT
    ) STRICT;
    INSERT INTO operations_meta (
      singleton, pending_records, pending_bytes, in_flight,
      reserved_event_slots, reserved_disk_bytes, last_tenant_id, policy_json
    ) VALUES (1, 0, 0, 0, 0, 0, NULL, NULL);

    CREATE TABLE operations_tenants (
      tenant_id TEXT PRIMARY KEY,
      pending_records INTEGER NOT NULL CHECK (pending_records >= 0),
      pending_bytes INTEGER NOT NULL CHECK (pending_bytes >= 0),
      in_flight INTEGER NOT NULL CHECK (in_flight >= 0),
      last_project_id TEXT
    ) STRICT, WITHOUT ROWID;

    CREATE TABLE operations_projects (
      tenant_id TEXT NOT NULL REFERENCES operations_tenants(tenant_id),
      project_id TEXT NOT NULL,
      pending_records INTEGER NOT NULL CHECK (pending_records >= 0),
      pending_bytes INTEGER NOT NULL CHECK (pending_bytes >= 0),
      in_flight INTEGER NOT NULL CHECK (in_flight >= 0),
      PRIMARY KEY (tenant_id, project_id)
    ) STRICT, WITHOUT ROWID;

    CREATE TABLE operations_work (
      job_id TEXT PRIMARY KEY,
      workflow_id TEXT NOT NULL UNIQUE REFERENCES workflows(workflow_id),
      tenant_id TEXT NOT NULL,
      project_id TEXT NOT NULL,
      idempotency_key TEXT NOT NULL,
      work_json TEXT NOT NULL,
      work_bytes INTEGER NOT NULL CHECK (work_bytes >= 0),
      work_sha256 TEXT NOT NULL CHECK (length(work_sha256) = 64 AND work_sha256 NOT GLOB '*[^0-9a-f]*'),
      limits_version TEXT NOT NULL,
      state TEXT NOT NULL CHECK (state IN ('queued','running','parked','unknown','completed','failed','cancelled')),
      generation INTEGER NOT NULL CHECK (generation >= 0),
      worker_id TEXT,
      runner_pid INTEGER CHECK (runner_pid IS NULL OR runner_pid > 1),
      fence_token INTEGER,
      lease_expires_ms INTEGER,
      created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
      updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= 0),
      reserved_event_slots INTEGER NOT NULL CHECK (reserved_event_slots >= 0),
      reserved_disk_bytes INTEGER NOT NULL CHECK (reserved_disk_bytes >= 0),
      reason_code TEXT,
      UNIQUE (tenant_id, project_id, idempotency_key),
      FOREIGN KEY (tenant_id, project_id) REFERENCES operations_projects(tenant_id, project_id)
    ) STRICT, WITHOUT ROWID;

    CREATE INDEX operations_work_project_queue_idx
      ON operations_work (tenant_id, project_id, state, created_at_ms, job_id);
    CREATE INDEX operations_work_workflow_idx
      ON operations_work (workflow_id, state);
    CREATE INDEX operations_work_state_idx
      ON operations_work (state, job_id);
    CREATE INDEX operations_work_lease_idx
      ON operations_work (state, lease_expires_ms, job_id);
    CREATE INDEX operations_work_scope_page_idx
      ON operations_work (tenant_id, project_id, job_id);
    CREATE INDEX operations_attempts_workflow_state_idx
      ON operation_attempts (workflow_id, state);
    CREATE INDEX operations_attempts_state_workflow_idx
      ON operation_attempts (state, workflow_id);

    CREATE TRIGGER operations_work_payload_immutable
      BEFORE UPDATE OF job_id, workflow_id, tenant_id, project_id, idempotency_key,
        work_json, work_bytes, work_sha256, limits_version, created_at_ms
      ON operations_work BEGIN
        SELECT RAISE(ABORT, 'operations work identity and payload are immutable');
      END;

    CREATE TRIGGER operations_event_reservation AFTER INSERT ON workflow_events
      WHEN EXISTS (
        SELECT 1 FROM operations_work w
        WHERE w.workflow_id = NEW.workflow_id AND w.reserved_event_slots > 0
      ) BEGIN
        UPDATE operations_work SET reserved_event_slots = reserved_event_slots - 1
        WHERE workflow_id = NEW.workflow_id AND reserved_event_slots > 0;
        UPDATE operations_meta SET reserved_event_slots = reserved_event_slots - 1
        WHERE singleton = 1 AND reserved_event_slots > 0;
      END;
  `);
}

function operationsResourceSnapshot(databasePath) {
  try {
    const database = statSync(databasePath, { throwIfNoEntry: false });
    const wal = statSync(databasePath + '-wal', { throwIfNoEntry: false });
    const shm = statSync(databasePath + '-shm', { throwIfNoEntry: false });
    const fs = statfsSync(dirname(databasePath));
    const databaseBytes = database?.isFile() ? database.size : 0;
    const walBytes = wal?.isFile() ? wal.size : 0;
    const shmBytes = shm?.isFile() ? shm.size : 0;
    const availableBytes = fs.bavail * fs.bsize;
    if (![databaseBytes, walBytes, shmBytes, availableBytes].every(Number.isSafeInteger)) return null;
    return Object.freeze({ databaseBytes, walBytes, shmBytes, availableBytes });
  } catch {
    return null;
  }
}

function safeOperationsRow(row) {
  return Object.freeze({
    jobId: row.job_id,
    workflowId: row.workflow_id,
    workJson: row.work_json,
    workBytes: Number(row.work_bytes),
    workSha256: row.work_sha256,
    state: row.state,
    generation: Number(row.generation),
    workerId: row.worker_id,
    fence: row.fence_token === null ? null : Number(row.fence_token),
    leaseExpiresAtMs: row.lease_expires_ms === null ? null : Number(row.lease_expires_ms),
    updatedAtMs: Number(row.updated_at_ms),
    reasonCode: row.reason_code
  });
}

const OPS_POLICY_KEYS = Object.freeze([
  'maxQueueRecords', 'maxQueueBytes', 'maxPerTenantRecords', 'maxPerTenantBytes',
  'maxPerProjectRecords', 'maxPerProjectBytes', 'maxTenants', 'maxProjects',
  'maxInFlightGlobal', 'maxInFlightTenant', 'maxInFlightProject',
  'maxEventBacklog', 'maxStoreBytes', 'maxWalBytes', 'minFreeDiskBytes',
  'completionHeadroomBytes', 'maxJobBytes', 'busyTimeoutMs', 'leaseTtlMs',
  'limitsVersion'
]);

function readOperationsPolicy(value, operation) {
  let fields;
  try {
    fields = readOwnDataRecord(value, OPS_POLICY_KEYS, operation, OPS_POLICY_KEYS);
  } catch {
    fail(operation, 'INVALID_INPUT', 'The worker operations limits are invalid.');
  }
  const integerLimits = {
    maxQueueRecords: [2, 128],
    maxQueueBytes: [65_536, 67_108_864],
    maxPerTenantRecords: [1, 64],
    maxPerTenantBytes: [32_768, 33_554_432],
    maxPerProjectRecords: [1, 64],
    maxPerProjectBytes: [16_384, 16_777_216],
    maxTenants: [2, 128],
    maxProjects: [2, 512],
    maxInFlightGlobal: [1, 32],
    maxInFlightTenant: [1, 32],
    maxInFlightProject: [1, 32],
    maxEventBacklog: [OPS_EVENT_RESERVATION * 2, 65_536],
    maxStoreBytes: [16_777_216, 1_073_741_824],
    maxWalBytes: [1_048_576, 134_217_728],
    minFreeDiskBytes: [1_048_576, 1_073_741_824],
    completionHeadroomBytes: [65_536, 4_194_304],
    maxJobBytes: [256, 65_536],
    busyTimeoutMs: [1, 250],
    leaseTtlMs: [1_000, 300_000]
  };
  for (const [key, [minimum, maximum]] of Object.entries(integerLimits)) {
    if (!Number.isSafeInteger(fields[key]) || fields[key] < minimum || fields[key] > maximum) {
      fail(operation, 'INVALID_INPUT', 'The worker operations limits are outside their supported range.');
    }
  }
  if (typeof fields.limitsVersion !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$/u.test(fields.limitsVersion)
      || fields.maxPerTenantRecords > Math.floor(fields.maxQueueRecords / 2)
      || fields.maxPerTenantBytes > Math.floor(fields.maxQueueBytes / 2)
      || fields.maxPerProjectRecords > fields.maxPerTenantRecords
      || fields.maxPerProjectBytes > fields.maxPerTenantBytes
      || fields.maxInFlightTenant > fields.maxInFlightGlobal
      || fields.maxInFlightProject > fields.maxInFlightTenant
      || fields.maxEventBacklog < fields.maxQueueRecords * OPS_EVENT_RESERVATION) {
    fail(operation, 'INVALID_INPUT', 'The worker operations limits violate a required capacity relationship.');
  }
  return fields;
}

function operationsPolicyIdentity(value, operation) {
  const policy = readOperationsPolicy(value, operation);
  return JSON.stringify(OPS_POLICY_KEYS.map(key => [key, policy[key]]));
}

function validateUnboundOperationsStore(db, policy, operation, databasePath, resourceCeilings) {
  const meta = db.prepare('SELECT * FROM operations_meta WHERE singleton = 1').get();
  if (!meta) fail(operation, 'CHECKPOINT_CORRUPT', 'The worker operations store is incomplete.');
  const rows = db.prepare(`
    SELECT job_id, workflow_id, tenant_id, project_id, limits_version, state,
      work_json, work_bytes, work_sha256, reserved_event_slots, reserved_disk_bytes
    FROM operations_work INDEXED BY operations_work_state_idx
    WHERE state IN ('queued','running','parked','unknown') ORDER BY job_id LIMIT ?
  `).all(policy.maxQueueRecords + 1);
  const pendingBytes = rows.reduce((sum, item) => sum + Number(item.work_bytes), 0);
  const inFlight = rows.reduce((sum, item) => sum + (item.state === 'running' ? 1 : 0), 0);
  if (rows.length > policy.maxQueueRecords
      || rows.length !== Number(meta.pending_records)
      || pendingBytes !== Number(meta.pending_bytes)
      || inFlight !== Number(meta.in_flight)
      || Number(meta.pending_records) > policy.maxQueueRecords
      || Number(meta.pending_bytes) > policy.maxQueueBytes
      || Number(meta.in_flight) > policy.maxInFlightGlobal) {
    fail(operation, 'INVALID_INPUT', 'The retained operations queue is not compatible with this worker policy.');
  }

  const tenants = db.prepare(`
    SELECT pending_records, pending_bytes, in_flight FROM operations_tenants LIMIT ?
  `).all(policy.maxTenants + 1);
  const projects = db.prepare(`
    SELECT pending_records, pending_bytes, in_flight FROM operations_projects LIMIT ?
  `).all(policy.maxProjects + 1);
  if (tenants.length > policy.maxTenants || projects.length > policy.maxProjects
      || tenants.some(item => Number(item.pending_records) > policy.maxPerTenantRecords
        || Number(item.pending_bytes) > policy.maxPerTenantBytes
        || Number(item.in_flight) > policy.maxInFlightTenant)
      || projects.some(item => Number(item.pending_records) > policy.maxPerProjectRecords
        || Number(item.pending_bytes) > policy.maxPerProjectBytes
        || Number(item.in_flight) > policy.maxInFlightProject)) {
    fail(operation, 'INVALID_INPUT', 'The retained operations scopes exceed this worker policy.');
  }

  const cursorRow = db.prepare(
    'SELECT last_sequence FROM telemetry_cursors WHERE consumer_id = ?'
  ).get('internal-telemetry-v1');
  const cursor = cursorRow ? Number(cursorRow.last_sequence) : 0;
  const pendingEvents = Number(db.prepare(`
    SELECT COUNT(*) AS count FROM (
      SELECT sequence FROM workflow_events WHERE sequence > ? LIMIT ?
    )
  `).get(cursor, policy.maxEventBacklog + 1).count);
  if (pendingEvents + Number(meta.reserved_event_slots) > policy.maxEventBacklog) {
    fail(operation, 'INVALID_INPUT', 'The retained event reservations exceed this worker policy.');
  }

  const reservationUpgrades = [];
  let additionalDiskBytes = 0;
  let additionalEventSlots = 0;
  for (const row of rows) {
    const workBytes = Number(row.work_bytes);
    if (!Number.isSafeInteger(workBytes) || workBytes < 0 || workBytes > policy.maxJobBytes
        || Buffer.byteLength(row.work_json, 'utf8') !== workBytes
        || digest(JSON.stringify([
          'agent-harness.operations-work.v1', row.tenant_id, row.project_id,
          row.limits_version, row.work_json
        ])) !== row.work_sha256) {
      fail(operation, 'CHECKPOINT_CORRUPT', 'A retained local work item failed its integrity check.');
    }
    let work;
    let normalized;
    let lifecycleBudget;
    try {
      work = parseBoundedJson(row.work_json, { maxBytes: workBytes, maxDepth: 24, maxNodes: 2048 });
      normalized = readOwnDataRecord(work, ['type', 'version', 'planJson', 'limitsJson'], operation);
      if (normalized.type !== 'local_plan' || normalized.version !== 1
          || typeof normalized.planJson !== 'string' || typeof normalized.limitsJson !== 'string') {
        throw new Error('unsupported retained work');
      }
      lifecycleBudget = operationsLifecycleBudget(normalized.planJson, normalized.limitsJson, policy, operation);
    } catch {
      fail(operation, 'CHECKPOINT_CORRUPT', 'A retained local work item cannot be safely rebound to this policy.');
    }
    const currentDisk = Number(row.reserved_disk_bytes);
    const currentEvents = Number(row.reserved_event_slots);
    if (!Number.isSafeInteger(currentDisk) || currentDisk < 0
        || !Number.isSafeInteger(currentEvents) || currentEvents < 0) {
      fail(operation, 'CHECKPOINT_CORRUPT', 'A retained local work reservation is invalid.');
    }
    const nextDisk = Math.max(currentDisk, lifecycleBudget.diskBytes);
    const nextEvents = Math.max(currentEvents, lifecycleBudget.eventSlots);
    additionalDiskBytes += nextDisk - currentDisk;
    additionalEventSlots += nextEvents - currentEvents;
    if (nextDisk !== currentDisk || nextEvents !== currentEvents) {
      reservationUpgrades.push({
        jobId: row.job_id,
        state: row.state,
        currentDisk,
        currentEvents,
        nextDisk,
        nextEvents
      });
    }
  }

  if (reservationUpgrades.length > 0 && (!databasePath || !resourceCeilings)) {
    fail(operation, 'INVALID_INPUT', 'The retained work cannot be rebound without an active resource ceiling.');
  }
  if (databasePath && resourceCeilings) {
    const resources = operationsResourceSnapshot(databasePath);
    const capacity = operationsResourceCapacity(resources, meta, policy, {
      pendingEvents,
      additionalDiskBytes,
      additionalWalBytes: policy.completionHeadroomBytes,
      additionalEventSlots
    });
    if (!capacity.ok || policy.maxStoreBytes !== resourceCeilings.maxStoreBytes
        || policy.maxWalBytes !== resourceCeilings.maxWalBytes
        || policy.minFreeDiskBytes !== resourceCeilings.minFreeDiskBytes) {
      fail(operation, 'INVALID_INPUT', 'The retained work cannot be rebound within this worker resource policy.');
    }
  }

  for (const upgrade of reservationUpgrades) {
    const changed = db.prepare(`
      UPDATE operations_work SET reserved_disk_bytes = ?, reserved_event_slots = ?
      WHERE job_id = ? AND state = ? AND reserved_disk_bytes = ? AND reserved_event_slots = ?
    `).run(upgrade.nextDisk, upgrade.nextEvents, upgrade.jobId, upgrade.state,
      upgrade.currentDisk, upgrade.currentEvents);
    if (Number(changed.changes) !== 1) {
      fail(operation, 'STORE_WRITE_FAILED', 'A retained work reservation changed during policy binding.');
    }
  }
  if (additionalDiskBytes !== 0 || additionalEventSlots !== 0) {
    const changed = db.prepare(`
      UPDATE operations_meta SET reserved_disk_bytes = reserved_disk_bytes + ?,
        reserved_event_slots = reserved_event_slots + ?
      WHERE singleton = 1
    `).run(additionalDiskBytes, additionalEventSlots);
    if (Number(changed.changes) !== 1) {
      fail(operation, 'STORE_WRITE_FAILED', 'The retained work reservations could not be rebound.');
    }
  }
}

function ensureOperationsPolicy(db, policy, operation, bindIfUnconfigured = true,
  databasePath = null, resourceCeilings = null) {
  const identity = operationsPolicyIdentity(policy, operation);
  const row = db.prepare('SELECT policy_json FROM operations_meta WHERE singleton = 1').get();
  if (!row) fail(operation, 'CHECKPOINT_CORRUPT', 'The worker operations store is incomplete.');
  if (row.policy_json === null && bindIfUnconfigured) {
    validateUnboundOperationsStore(db, policy, operation, databasePath, resourceCeilings);
    db.prepare('UPDATE operations_meta SET policy_json = ? WHERE singleton = 1 AND policy_json IS NULL').run(identity);
  }
  const current = db.prepare('SELECT policy_json FROM operations_meta WHERE singleton = 1').get().policy_json;
  if (current !== null && current !== identity) {
    fail(operation, 'INVALID_INPUT', 'The worker policy does not match the policy bound to this operations store.');
  }
  return current === identity;
}

function opsBackpressure(reasonCode, retryAfterMs = 250) {
  return Object.freeze({
    accepted: false,
    code: 'OPS_BACKPRESSURE',
    reasonCode,
    retryAfterMs: Math.min(5_000, Math.max(25, retryAfterMs))
  });
}

function operationsResourceCapacity(resources, meta, policy, {
  pendingEvents,
  additionalDiskBytes,
  additionalWalBytes = additionalDiskBytes,
  additionalEventSlots
}) {
  const reasons = [];
  const reservedDiskBytes = Number(meta.reserved_disk_bytes);
  const reservedEventSlots = Number(meta.reserved_event_slots);
  if (!resources) {
    reasons.push('RESOURCE_PROBE_UNAVAILABLE');
  } else {
    const storeBytes = resources.databaseBytes + resources.walBytes + resources.shmBytes;
    if (resources.walBytes + additionalWalBytes > policy.maxWalBytes) {
      reasons.push('WAL_HEADROOM_LOW');
    }
    if (storeBytes + reservedDiskBytes + additionalDiskBytes > policy.maxStoreBytes) {
      reasons.push('STORE_CAPACITY');
    }
    if (resources.availableBytes < policy.minFreeDiskBytes + reservedDiskBytes + additionalDiskBytes) {
      reasons.push('DISK_HEADROOM_LOW');
    }
  }
  if (pendingEvents + reservedEventSlots + additionalEventSlots > policy.maxEventBacklog) {
    reasons.push('TELEMETRY_BACKLOG_HIGH');
  }
  return Object.freeze({ ok: reasons.length === 0, reasons: Object.freeze(reasons) });
}

function operationsLifecycleBudget(planJson, limitsJson, policy, operation) {
  let limitSet;
  let plan;
  let limits;
  let stepCount;
  let resultBytes;
  try {
    limitSet = createLimitSet(limitsJson);
    plan = compilePlan(planJson, limitSet, createDefaultRegistry(), 'ops-storage-budget');
    limits = getLimitValues(limitSet);
    stepCount = getPlanStepIds(plan).length;
    resultBytes = getMaximumCompletedResultsJsonBytes(plan, '{}');
  } catch {
    fail(operation, 'INVALID_INPUT', 'The local work resource budget could not be derived safely.');
  }
  const checkpointBytes = 12_288
    + Buffer.byteLength(planJson, 'utf8')
    + Buffer.byteLength(limitsJson, 'utf8')
    + resultBytes;
  const controllerTransactions = limits.maxIterations + Math.min(stepCount, limits.maxIterations);
  const checkpointTransactionReserve = checkpointBytes * 3 + OPS_CHECKPOINT_WRITE_BASE_BYTES;
  const controllerReserve = controllerTransactions * checkpointTransactionReserve;
  const lifecycleManagementTransactions = OPS_MAX_WORKER_GENERATIONS * 3 + 2;
  const lifecycleManagementReserve = lifecycleManagementTransactions * OPS_GENERATION_WRITE_RESERVE_BYTES;
  const diskBytes = Math.max(policy.completionHeadroomBytes, OPS_ADMISSION_WRITE_RESERVE_BYTES)
    + OPS_ADMISSION_WRITE_RESERVE_BYTES + controllerReserve + lifecycleManagementReserve;
  const eventSlots = 2 + OPS_MAX_WORKER_GENERATIONS * 2 + 1 + controllerTransactions + 1;
  if (!Number.isSafeInteger(diskBytes) || !Number.isSafeInteger(eventSlots)) {
    fail(operation, 'INVALID_INPUT', 'The local work resource budget exceeds the supported range.');
  }
  return Object.freeze({ diskBytes, eventSlots, controllerTransactions, checkpointBytes });
}

function operationsWriteCharge(checkpointBytes = 0) {
  return Math.max(OPS_GENERATION_WRITE_RESERVE_BYTES,
    checkpointBytes * 3 + OPS_CHECKPOINT_WRITE_BASE_BYTES);
}

function consumeOperationsStorageReservation(db, databasePath, ceilings, workflowId, charge, operation, remainingFloor = 0) {
  const row = db.prepare(`
    SELECT job_id, state, reserved_disk_bytes FROM operations_work WHERE workflow_id = ?
  `).get(workflowId);
  if (!row) return false;
  if (!ceilings) fail(operation, 'STORE_WRITE_FAILED', 'The local operation has no active storage ceiling.');
  const reserved = Number(row.reserved_disk_bytes);
  if (reserved < charge + remainingFloor) {
    fail(operation, 'STORE_WRITE_FAILED', 'The accepted local operation exhausted its reserved durable capacity.');
  }
  const meta = db.prepare('SELECT reserved_disk_bytes FROM operations_meta WHERE singleton = 1').get();
  const resources = operationsResourceSnapshot(databasePath);
  if (!meta || !resources) fail(operation, 'STORE_WRITE_FAILED', 'The local operation storage status is unavailable.');
  const reservedTotal = Number(meta.reserved_disk_bytes);
  const storeBytes = resources.databaseBytes + resources.walBytes + resources.shmBytes;
  if (storeBytes + reservedTotal > ceilings.maxStoreBytes
      || resources.walBytes + charge > ceilings.maxWalBytes
      || resources.availableBytes < ceilings.minFreeDiskBytes + reservedTotal) {
    fail(operation, 'STORE_WRITE_FAILED', 'The durable store no longer has the capacity reserved for this operation.');
  }
  db.prepare(`
    UPDATE operations_work SET reserved_disk_bytes = reserved_disk_bytes - ? WHERE job_id = ?
  `).run(charge, row.job_id);
  db.prepare(`
    UPDATE operations_meta SET reserved_disk_bytes = reserved_disk_bytes - ? WHERE singleton = 1
  `).run(charge);
  return true;
}

function opsSafeIdentifier(value, maximum = 128) {
  return typeof value === 'string'
    && value.length <= maximum
    && value.trim() === value
    && OPS_SAFE_ID.test(value);
}

function readOperationsJobIdList(value, maximum, operation) {
  if (value === undefined) return Object.freeze([]);
  if (!Array.isArray(value) || utilTypes.isProxy(value)) {
    fail(operation, 'INVALID_INPUT', 'The recoverable worker identities are invalid.');
  }
  let keys;
  try {
    keys = Reflect.ownKeys(value);
  } catch {
    fail(operation, 'INVALID_INPUT', 'The recoverable worker identities are invalid.');
  }
  if (value.length > maximum || keys.length !== value.length + 1 || !keys.includes('length')) {
    fail(operation, 'INVALID_INPUT', 'The recoverable worker identities exceed their bound.');
  }
  const result = [];
  const unique = new Set();
  for (let index = 0; index < value.length; index += 1) {
    const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')
        || !opsSafeIdentifier(descriptor.value, 256) || unique.has(descriptor.value)) {
      fail(operation, 'INVALID_INPUT', 'The recoverable worker identities are invalid.');
    }
    unique.add(descriptor.value);
    result.push(descriptor.value);
  }
  return Object.freeze(result);
}

function adjustOperationsCounters(db, { tenantId, projectId, pendingRecords = 0, pendingBytes = 0, inFlight = 0, reservedEvents = 0, reservedDiskBytes = 0 }) {
  db.prepare(`
    UPDATE operations_meta SET
      pending_records = pending_records + ?,
      pending_bytes = pending_bytes + ?,
      in_flight = in_flight + ?,
      reserved_event_slots = reserved_event_slots + ?,
      reserved_disk_bytes = reserved_disk_bytes + ?
    WHERE singleton = 1
  `).run(pendingRecords, pendingBytes, inFlight, reservedEvents, reservedDiskBytes);
  if (tenantId !== undefined) {
    db.prepare(`
      UPDATE operations_tenants SET
        pending_records = pending_records + ?, pending_bytes = pending_bytes + ?,
        in_flight = in_flight + ? WHERE tenant_id = ?
    `).run(pendingRecords, pendingBytes, inFlight, tenantId);
  }
  if (projectId !== undefined) {
    db.prepare(`
      UPDATE operations_projects SET
        pending_records = pending_records + ?, pending_bytes = pending_bytes + ?,
        in_flight = in_flight + ? WHERE tenant_id = ? AND project_id = ?
    `).run(pendingRecords, pendingBytes, inFlight, tenantId, projectId);
  }
}

function readUserVersion(db) {
  return Number(db.prepare('PRAGMA user_version').get().user_version);
}

function hasUserTables(db) {
  return db.prepare(
    "SELECT COUNT(*) AS count FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
  ).get().count > 0;
}

function appendMigrationEvent(db, workflowId, state, revision, occurredAtMs) {
  const eventId = 'migration:v1:' + workflowId;
  const result = db.prepare(`
    INSERT INTO workflow_events (
      event_id, workflow_id, revision, event_type, from_state, to_state, fence_token, occurred_at_ms
    ) VALUES (?, ?, ?, 'schema_migrated_v1', ?, ?, 0, ?)
  `).run(eventId, workflowId, revision, state, state, occurredAtMs);
  db.prepare('INSERT INTO outbox (event_id, event_sequence) VALUES (?, ?)').run(
    eventId,
    Number(result.lastInsertRowid)
  );
}

function migrateV1ToV4(db) {
  db.exec('ALTER TABLE workflows RENAME TO workflows_v1');
  createSchemaV3(db);
  const oldRows = db.prepare('SELECT * FROM workflows_v1 ORDER BY workflow_id').all();
  const insertWorkflow = db.prepare(`
    INSERT INTO workflows (
      workflow_id, state, revision, fence_token, lease_owner, lease_expires_ms,
      checkpoint_sequence, cancellation_requested, created_at_ms, updated_at_ms
    ) VALUES (?, ?, ?, 0, NULL, NULL, 1, 0, ?, ?)
  `);
  const insertCheckpoint = db.prepare(`
    INSERT INTO workflow_checkpoints (
      workflow_id, sequence, schema_version, payload_json, sha256, revision, created_at_ms
    ) VALUES (?, 1, ?, ?, ?, ?, ?)
  `);
  for (const row of oldRows) {
    const workflowId = String(row.workflow_id);
    const state = String(row.state);
    const revision = Number(row.revision);
    const createdAtMs = Number(row.created_at_ms);
    const updatedAtMs = Number(row.updated_at_ms);
    if (!STATES.has(state) || !Number.isSafeInteger(revision) || revision < 0) {
      fail('migrateV1ToV3', 'CHECKPOINT_CORRUPT', 'The legacy checkpoint cannot be migrated safely.');
    }
    if (
      typeof row.checkpoint_json !== 'string'
      || Buffer.byteLength(row.checkpoint_json, 'utf8') > MAX_CHECKPOINT_SERIALIZED_BYTES
    ) {
      fail('migrateV1ToV3', 'CHECKPOINT_CORRUPT', 'The legacy checkpoint cannot be migrated safely.');
    }
    let parsed;
    try {
      parsed = JSON.parse(row.checkpoint_json);
    } catch {
      fail('migrateV1ToV3', 'CHECKPOINT_CORRUPT', 'The legacy checkpoint cannot be migrated safely.');
    }
    if (!isRecord(parsed)) {
      fail('migrateV1ToV3', 'CHECKPOINT_CORRUPT', 'The legacy checkpoint cannot be migrated safely.');
    }
    let checkpointPayload;
    try {
      checkpointPayload = serializeCheckpoint(parsed, 'migrateV1ToV3');
    } catch {
      fail('migrateV1ToV3', 'CHECKPOINT_CORRUPT', 'The legacy checkpoint cannot be migrated safely.');
    }
    const created = Number.isSafeInteger(createdAtMs) ? createdAtMs : updatedAtMs;
    const updated = Number.isSafeInteger(updatedAtMs) ? updatedAtMs : created;
    insertWorkflow.run(workflowId, state, revision, created, updated);
    insertCheckpoint.run(workflowId, CHECKPOINT_SCHEMA_VERSION, checkpointPayload, digest(checkpointPayload), revision, updated);
    appendMigrationEvent(db, workflowId, state, revision, updated);
  }
  db.exec('DROP TABLE workflows_v1');
  createTelemetryCursorSchema(db);
  db.exec('PRAGMA user_version = 4');
}

function migrateV2ToV4(db) {
  // Existing receipts have no proof binding their proposed checkpoint bytes.
  // Leave them NULL: they remain readable, but reconciliation fails closed.
  db.exec('ALTER TABLE response_receipts ADD COLUMN next_checkpoint_sha256 TEXT');
  createTelemetryCursorSchema(db);
  db.exec('PRAGMA user_version = 4');
}

function migrateV3ToV4(db) {
  createTelemetryCursorSchema(db);
  db.exec('PRAGMA user_version = 4');
}

function migrateV4ToV5(db) {
  createJevObservationSchema(db);
  db.exec('PRAGMA user_version = 5');
}

function migrateV5ToV6(db) {
  createOperationsSchema(db);
  db.exec('PRAGMA user_version = 6');
}

function hasTable(db, tableName) {
  return Number(db.prepare(
    "SELECT COUNT(*) AS count FROM sqlite_master WHERE type='table' AND name=?"
  ).get(tableName).count) === 1;
}

function hasColumn(db, tableName, columnName) {
  return Number(db.prepare(
    'SELECT COUNT(*) AS count FROM pragma_table_info(?) WHERE name=?'
  ).get(tableName, columnName).count) === 1;
}

const CURRENT_V6_REQUIRED_TABLES = Object.freeze([
  'operations_meta', 'operations_tenants', 'operations_projects', 'operations_work',
  'workflow_events', 'workflows', 'workflow_checkpoints', 'operation_attempts'
]);

function hasCurrentV6OperationsSchema(db) {
  return withConsistentReadSnapshot(db, 'initializeDatabase', () => {
    if (readUserVersion(db) !== INTERNAL_SCHEMA_VERSION) return false;
    if (CURRENT_V6_REQUIRED_TABLES.some(table => !hasTable(db, table))) return false;
    return hasColumn(db, 'operations_meta', 'policy_json');
  });
}

function migrateV6OperationsPolicyBinding(db) {
  if (!hasTable(db, 'operations_meta')) {
    fail('migrateV6OperationsPolicyBinding', 'CHECKPOINT_CORRUPT', 'The version 6 operations metadata is missing.');
  }
  if (!hasColumn(db, 'operations_meta', 'policy_json')) {
    db.exec('ALTER TABLE operations_meta ADD COLUMN policy_json TEXT');
  }
  if (CURRENT_V6_REQUIRED_TABLES.some(table => !hasTable(db, table))) {
    fail('migrateV6OperationsPolicyBinding', 'CHECKPOINT_CORRUPT', 'The version 6 operations schema is incomplete.');
  }
}

function initializeDatabase(db) {
  const version = readUserVersion(db);
  if (version > INTERNAL_SCHEMA_VERSION) {
    fail('initializeDatabase', 'UNSUPPORTED_SCHEMA_VERSION', 'The durable store schema is newer than this runtime supports.');
  }
  if (version === INTERNAL_SCHEMA_VERSION && hasCurrentV6OperationsSchema(db)) return;
  if (version === 0) {
    if (hasUserTables(db)) {
      fail('initializeDatabase', 'UNVERSIONED_DATA', 'The durable store contains data without a supported schema version.');
    }
    inTransaction(db, 'initializeDatabase', () => {
      createSchemaV3(db);
      createTelemetryCursorSchema(db);
      createJevObservationSchema(db);
      createOperationsSchema(db);
      db.exec('PRAGMA user_version = 6');
    });
    return;
  }
  if (version === 1) {
    const table = db.prepare(
      "SELECT name FROM sqlite_master WHERE type='table' AND name='workflows'"
    ).get();
    if (!table) fail('initializeDatabase', 'CHECKPOINT_CORRUPT', 'The legacy workflow table is missing.');
  }
  if (version >= 1 && version <= 6) {
    inTransaction(db, 'initializeDatabase', () => {
      if (version === 1) migrateV1ToV4(db);
      else if (version === 2) migrateV2ToV4(db);
      else if (version === 3) migrateV3ToV4(db);
      if (readUserVersion(db) === 4) migrateV4ToV5(db);
      if (readUserVersion(db) === 5) migrateV5ToV6(db);
      if (readUserVersion(db) === 6) migrateV6OperationsPolicyBinding(db);
      if (readUserVersion(db) !== INTERNAL_SCHEMA_VERSION) {
        fail('initializeDatabase', 'UNSUPPORTED_SCHEMA_VERSION', 'The durable store schema could not be upgraded safely.');
      }
    });
  }
}

function mapWorkflow(row) {
  if (!row) return null;
  return {
    workflowId: row.workflow_id,
    state: row.state,
    revision: Number(row.revision),
    fence: Number(row.fence_token),
    leaseOwner: row.lease_owner,
    leaseExpiresAtMs: row.lease_expires_ms === null ? null : Number(row.lease_expires_ms),
    cancellationRequested: Boolean(row.cancellation_requested),
    checkpointSequence: Number(row.checkpoint_sequence)
  };
}

export class DurableWorkflowStore {
  #db;
  #clock;
  #idFactory;
  #databasePath;
  #operationsResourceCeilings = null;

  static open({ databasePath, clock = Date.now, idFactory = randomUUID, busyTimeoutMs = 5000 } = {}) {
    const DatabaseSync = openSqliteConstructor();
    if (typeof databasePath !== 'string' || databasePath.trim() === '' || databasePath === ':memory:') {
      fail('open', 'DURABILITY_REQUIRES_FILE', 'The durable workflow store requires a file-backed database.');
    }
    if (typeof clock !== 'function' || typeof idFactory !== 'function') {
      fail('open', 'INVALID_INPUT', 'Clock and identifier sources must be injected functions.');
    }
    expectInteger(busyTimeoutMs, 'busyTimeoutMs', 'open', 1);
    if (busyTimeoutMs > 60_000) {
      fail('open', 'INVALID_INPUT', 'The database lock wait exceeds its supported range.');
    }

    const filePath = resolve(databasePath);
    try {
      mkdirSync(dirname(filePath), { recursive: true });
      const db = new DatabaseSync(filePath, {
        timeout: busyTimeoutMs,
        enableForeignKeyConstraints: true,
        allowExtension: false
      });
      try {
        const version = readUserVersion(db);
        if (version > INTERNAL_SCHEMA_VERSION) {
          fail('open', 'UNSUPPORTED_SCHEMA_VERSION', 'The durable store schema is newer than this runtime supports.');
        }
        db.exec('PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;');
        initializeDatabase(db);
        return new DurableWorkflowStore(db, clock, idFactory, filePath);
      } catch (error) {
        if (db.isOpen) db.close();
        throw error;
      }
    } catch (error) {
      if (error instanceof RuntimeError) throw error;
      throw mapStorageError('open', error);
    }
  }

  /** Read-only compatibility check. It never creates, migrates, or writes a database. */
  static inspectCompatibility({ databasePath, busyTimeoutMs = 20 } = {}) {
    if (typeof databasePath !== 'string' || databasePath.trim() === '' || databasePath === ':memory:') {
      return Object.freeze({ status: 'store_unavailable', schemaVersion: null, readOnly: true });
    }
    if (!Number.isSafeInteger(busyTimeoutMs) || busyTimeoutMs < 1 || busyTimeoutMs > 250) {
      return Object.freeze({ status: 'store_unavailable', schemaVersion: null, readOnly: true });
    }
    const filePath = resolve(databasePath);
    let db;
    try {
      const info = statSync(filePath, { throwIfNoEntry: false });
      if (!info?.isFile()) {
        return Object.freeze({ status: 'store_unavailable', schemaVersion: null, readOnly: true });
      }
      const { DatabaseSync } = requireBuiltin('node:sqlite');
      db = new DatabaseSync(filePath, {
        readOnly: true,
        timeout: busyTimeoutMs,
        allowExtension: false
      });
      const version = readUserVersion(db);
      if (version > INTERNAL_SCHEMA_VERSION) {
        return Object.freeze({ status: 'schema_unsupported', schemaVersion: version, readOnly: true });
      }
      if (version < INTERNAL_SCHEMA_VERSION) {
        return Object.freeze({ status: 'migration_required', schemaVersion: version, readOnly: true });
      }
      const required = db.prepare(`
        SELECT COUNT(*) AS count FROM sqlite_master
        WHERE type = 'table' AND name IN (
          'operations_meta','operations_tenants','operations_projects','operations_work',
          'workflow_events','workflows','workflow_checkpoints','operation_attempts'
        )
      `).get();
      const policyColumn = db.prepare(`
        SELECT COUNT(*) AS count FROM pragma_table_info('operations_meta')
        WHERE name = 'policy_json'
      `).get();
      return Object.freeze({
        status: Number(required.count) !== 8 ? 'schema_unsupported'
          : Number(policyColumn.count) === 1 ? 'supported' : 'migration_required',
        schemaVersion: version,
        readOnly: true
      });
    } catch {
      return Object.freeze({ status: 'store_unavailable', schemaVersion: null, readOnly: true });
    } finally {
      try {
        if (db?.isOpen) db.close();
      } catch {
        // The snapshot is still read-only; close failures never change its classification.
      }
    }
  }

  constructor(db, clock, idFactory, databasePath) {
    this.#db = db;
    this.#clock = clock;
    this.#idFactory = idFactory;
    this.#databasePath = databasePath;
  }

  /** @internal Pins the owning operations policy into an isolated local child store. */
  _operationsSetResourceCeilings(capability, input) {
    const operation = '_operationsSetResourceCeilings';
    if (capability !== OPERATIONS_STORAGE_CAPABILITY) {
      fail(operation, 'INVALID_INPUT', 'The private worker operations capability is required.');
    }
    const fields = readOwnDataRecord(input,
      ['maxStoreBytes', 'maxWalBytes', 'minFreeDiskBytes', 'policy'], operation,
      ['maxStoreBytes', 'maxWalBytes', 'minFreeDiskBytes']);
    for (const [key, minimum, maximum] of [
      ['maxStoreBytes', 16_777_216, 1_073_741_824],
      ['maxWalBytes', 1_048_576, 134_217_728],
      ['minFreeDiskBytes', 1_048_576, 1_073_741_824]
    ]) {
      if (!Number.isSafeInteger(fields[key]) || fields[key] < minimum || fields[key] > maximum) {
        fail(operation, 'INVALID_INPUT', 'The local store resource ceiling is invalid.');
      }
    }
    if (fields.policy !== undefined) readOperationsPolicy(fields.policy, operation);
    this.#assertOpen(operation);
    const resourceCeilings = Object.freeze({
      maxStoreBytes: fields.maxStoreBytes,
      maxWalBytes: fields.maxWalBytes,
      minFreeDiskBytes: fields.minFreeDiskBytes
    });
    if (fields.policy !== undefined) {
      inTransaction(this.#db, operation, () => ensureOperationsPolicy(
        this.#db, fields.policy, operation, true, this.#databasePath, resourceCeilings
      ));
    }
    this.#operationsResourceCeilings = resourceCeilings;
    return Object.freeze({ configured: true });
  }

  /** @internal Atomically binds trusted synthetic scope, workflow, queue row, counters, and events. */
  _operationsAdmit(capability, input) {
    const operation = '_operationsAdmit';
    if (capability !== OPERATIONS_STORAGE_CAPABILITY) {
      fail(operation, 'INVALID_INPUT', 'The private worker operations capability is required.');
    }
    const fields = readOwnDataRecord(input, [
      'tenantId', 'projectId', 'idempotencyKey', 'workJson', 'policy'
    ], operation);
    const policy = readOperationsPolicy(fields.policy, operation);
    if (!opsSafeIdentifier(fields.tenantId)
        || !opsSafeIdentifier(fields.projectId)
        || !opsSafeIdentifier(fields.idempotencyKey, 256)
        || typeof fields.workJson !== 'string'
        || Buffer.byteLength(fields.workJson, 'utf8') > policy.maxJobBytes) {
      fail(operation, 'INVALID_INPUT', 'The local work request is invalid or exceeds its byte limit.');
    }
    let work;
    try {
      work = parseBoundedJson(fields.workJson, {
        maxBytes: policy.maxJobBytes,
        maxDepth: 24,
        maxNodes: 2048
      });
    } catch {
      fail(operation, 'INVALID_INPUT', 'The local work request is invalid or exceeds its structure limit.');
    }
    const workFields = readOwnDataRecord(work, ['type', 'version', 'planJson', 'limitsJson'], operation);
    if (workFields.type !== 'local_plan' || workFields.version !== 1
        || typeof workFields.planJson !== 'string'
        || Buffer.byteLength(workFields.planJson, 'utf8') > 32_768
        || typeof workFields.limitsJson !== 'string'
        || Buffer.byteLength(workFields.limitsJson, 'utf8') > 2_048) {
      fail(operation, 'INVALID_INPUT', 'Only the pinned bounded local-plan work type is supported.');
    }
    const lifecycleBudget = operationsLifecycleBudget(
      workFields.planJson,
      workFields.limitsJson,
      policy,
      operation
    );
    this.#assertOpen(operation);
    const workBytes = Buffer.byteLength(fields.workJson, 'utf8');
    const workHash = digest(JSON.stringify([
      'agent-harness.operations-work.v1',
      fields.tenantId,
      fields.projectId,
      policy.limitsVersion,
      fields.workJson
    ]));

    return inTransaction(this.#db, operation, () => {
      ensureOperationsPolicy(
        this.#db, policy, operation, true, this.#databasePath, this.#operationsResourceCeilings
      );
      const duplicate = this.#db.prepare(`
        SELECT job_id, workflow_id, state, work_json, work_sha256
        FROM operations_work
        WHERE tenant_id = ? AND project_id = ? AND idempotency_key = ?
      `).get(fields.tenantId, fields.projectId, fields.idempotencyKey);
      if (duplicate) {
        if (duplicate.work_json !== fields.workJson || duplicate.work_sha256 !== workHash) {
          fail(operation, 'IDEMPOTENCY_CONFLICT', 'The scoped work identity was already used for different input.');
        }
        return Object.freeze({
          accepted: true,
          duplicate: true,
          jobId: duplicate.job_id,
          workflowId: duplicate.workflow_id,
          state: duplicate.state,
          retryAfterMs: 0
        });
      }

      const meta = this.#db.prepare('SELECT * FROM operations_meta WHERE singleton = 1').get();
      if (!meta) fail(operation, 'CHECKPOINT_CORRUPT', 'The worker operations store is incomplete.');
      const tenant = this.#db.prepare(
        'SELECT * FROM operations_tenants WHERE tenant_id = ?'
      ).get(fields.tenantId);
      const project = this.#db.prepare(`
        SELECT * FROM operations_projects WHERE tenant_id = ? AND project_id = ?
      `).get(fields.tenantId, fields.projectId);
      if (!tenant) {
        const count = Number(this.#db.prepare(`
          SELECT COUNT(*) AS count FROM (
            SELECT tenant_id FROM operations_tenants LIMIT ?
          )
        `).get(policy.maxTenants + 1).count);
        if (count >= policy.maxTenants) return opsBackpressure('SCOPE_CAPACITY');
      }
      if (!project) {
        const count = Number(this.#db.prepare(`
          SELECT COUNT(*) AS count FROM (
            SELECT project_id FROM operations_projects LIMIT ?
          )
        `).get(policy.maxProjects + 1).count);
        if (count >= policy.maxProjects) return opsBackpressure('SCOPE_CAPACITY');
      }
      if (Number(meta.pending_records) >= policy.maxQueueRecords
          || Number(meta.pending_bytes) + workBytes > policy.maxQueueBytes) {
        return opsBackpressure('GLOBAL_QUEUE_FULL');
      }
      if (Number(tenant?.pending_records ?? 0) >= policy.maxPerTenantRecords
          || Number(tenant?.pending_bytes ?? 0) + workBytes > policy.maxPerTenantBytes) {
        return opsBackpressure('TENANT_QUEUE_FULL');
      }
      if (Number(project?.pending_records ?? 0) >= policy.maxPerProjectRecords
          || Number(project?.pending_bytes ?? 0) + workBytes > policy.maxPerProjectBytes) {
        return opsBackpressure('PROJECT_QUEUE_FULL');
      }
      const cursorRow = this.#db.prepare(
        'SELECT last_sequence FROM telemetry_cursors WHERE consumer_id = ?'
      ).get('internal-telemetry-v1');
      const telemetryCursor = cursorRow ? Number(cursorRow.last_sequence) : 0;
      const pendingEvents = Number(this.#db.prepare(`
        SELECT COUNT(*) AS count FROM (
          SELECT sequence FROM workflow_events WHERE sequence > ? LIMIT ?
        )
      `).get(telemetryCursor, policy.maxEventBacklog + 1).count);
      const resources = operationsResourceSnapshot(this.#databasePath);
      const resourceStatus = operationsResourceCapacity(resources, meta, policy, {
        pendingEvents,
        additionalDiskBytes: lifecycleBudget.diskBytes + workBytes,
        additionalWalBytes: policy.completionHeadroomBytes,
        additionalEventSlots: lifecycleBudget.eventSlots
      });
      if (!resourceStatus.ok) {
        const reasonCode = resourceStatus.reasons[0];
        return opsBackpressure(reasonCode, reasonCode === 'RESOURCE_PROBE_UNAVAILABLE' ? 1_000 : 500);
      }

      if (!tenant) {
        this.#db.prepare(`
          INSERT INTO operations_tenants (tenant_id, pending_records, pending_bytes, in_flight, last_project_id)
          VALUES (?, 0, 0, 0, NULL)
        `).run(fields.tenantId);
      }
      if (!project) {
        this.#db.prepare(`
          INSERT INTO operations_projects (tenant_id, project_id, pending_records, pending_bytes, in_flight)
          VALUES (?, ?, 0, 0, 0)
        `).run(fields.tenantId, fields.projectId);
      }
      const now = this.#now(operation);
      const jobId = 'ops-job:' + String(this.#idFactory());
      const workflowId = 'ops-workflow:' + String(this.#idFactory());
      if (!opsSafeIdentifier(jobId, 256) || !opsSafeIdentifier(workflowId, 256)
          || this.#db.prepare('SELECT 1 FROM workflows WHERE workflow_id = ?').get(workflowId)
          || this.#db.prepare('SELECT 1 FROM operations_work WHERE job_id = ?').get(jobId)) {
        fail(operation, 'INVALID_INPUT', 'The local work identity source did not produce a unique safe value.');
      }
      const checkpointJson = serializeCheckpoint(Object.create(null), operation);
      this.#db.prepare(`
        INSERT INTO workflows (
          workflow_id, state, revision, fence_token, lease_owner, lease_expires_ms,
          checkpoint_sequence, cancellation_requested, created_at_ms, updated_at_ms
        ) VALUES (?, 'queued', 0, 0, NULL, NULL, 1, 0, ?, ?)
      `).run(workflowId, now, now);
      this.#db.prepare(`
        INSERT INTO workflow_checkpoints (
          workflow_id, sequence, schema_version, payload_json, sha256, revision, created_at_ms
        ) VALUES (?, 1, ?, ?, ?, 0, ?)
      `).run(workflowId, CHECKPOINT_SCHEMA_VERSION, checkpointJson, digest(checkpointJson), now);
      this.#db.prepare(`
        INSERT INTO operations_work (
          job_id, workflow_id, tenant_id, project_id, idempotency_key,
          work_json, work_bytes, work_sha256, limits_version, state, generation,
          worker_id, fence_token, lease_expires_ms, created_at_ms, updated_at_ms,
          reserved_event_slots, reserved_disk_bytes, reason_code
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', 0, NULL, NULL, NULL, ?, ?, ?, ?, NULL)
      `).run(
        jobId, workflowId, fields.tenantId, fields.projectId, fields.idempotencyKey,
        fields.workJson, workBytes, workHash, policy.limitsVersion, now, now,
        lifecycleBudget.eventSlots, lifecycleBudget.diskBytes
      );
      adjustOperationsCounters(this.#db, {
        tenantId: fields.tenantId,
        projectId: fields.projectId,
        pendingRecords: 1,
        pendingBytes: workBytes,
        reservedEvents: lifecycleBudget.eventSlots,
        reservedDiskBytes: lifecycleBudget.diskBytes
      });
      this.#appendEvent({
        eventId: 'ops-created:' + jobId,
        workflowId,
        revision: 0,
        type: 'workflow_created',
        fromState: null,
        toState: 'queued',
        fence: 0,
        atMs: now
      });
      this.#appendEvent({
        eventId: 'ops-admitted:' + jobId,
        workflowId,
        revision: 0,
        type: 'operations_work_admitted',
        fromState: null,
        toState: 'queued',
        fence: 0,
        atMs: now
      });
      return Object.freeze({
        accepted: true,
        duplicate: false,
        jobId,
        workflowId,
        state: 'queued',
        retryAfterMs: 0
      });
    });
  }

  /** @internal Bounded candidates whose prior child lease expired; the supervisor proves PID exit. */
  _operationsExpiredClaims(capability, input) {
    const operation = '_operationsExpiredClaims';
    if (capability !== OPERATIONS_STORAGE_CAPABILITY) {
      fail(operation, 'INVALID_INPUT', 'The private worker operations capability is required.');
    }
    const fields = readOwnDataRecord(input, ['policy'], operation);
    const policy = readOperationsPolicy(fields.policy, operation);
    this.#assertOpen(operation);
    if (!ensureOperationsPolicy(this.#db, policy, operation, false)) {
      fail(operation, 'OPS_POLICY_UNCONFIGURED', 'The worker policy has not been bound to this operations store.');
    }
    const now = this.#now(operation);
    const rows = this.#db.prepare(`
      SELECT job_id, runner_pid, lease_expires_ms
      FROM operations_work INDEXED BY operations_work_lease_idx
      WHERE state = 'running' AND lease_expires_ms <= ?
      ORDER BY lease_expires_ms, job_id LIMIT ?
    `).all(now, policy.maxInFlightGlobal + 1);
    const incomplete = rows.length > policy.maxInFlightGlobal;
    return Object.freeze({
      items: Object.freeze(rows.slice(0, policy.maxInFlightGlobal).map(row => Object.freeze({
        jobId: row.job_id,
        runnerPid: row.runner_pid === null ? null : Number(row.runner_pid),
        leaseExpiresAtMs: Number(row.lease_expires_ms)
      }))),
      incomplete
    });
  }

  /** @internal Binds an actual child PID to a current worker generation before it can run. */
  _operationsSetRunnerPid(capability, input) {
    const operation = '_operationsSetRunnerPid';
    if (capability !== OPERATIONS_STORAGE_CAPABILITY) {
      fail(operation, 'INVALID_INPUT', 'The private worker operations capability is required.');
    }
    const fields = readOwnDataRecord(input, ['jobId', 'ownerId', 'generation', 'fence', 'runnerPid'], operation);
    if (!opsSafeIdentifier(fields.jobId, 256) || !opsSafeIdentifier(fields.ownerId, 128)) {
      fail(operation, 'INVALID_INPUT', 'The worker claim identity is invalid.');
    }
    expectInteger(fields.generation, 'generation', operation, 1);
    expectInteger(fields.fence, 'fence', operation, 1);
    expectInteger(fields.runnerPid, 'runnerPid', operation, 2);
    if (fields.runnerPid > 2_147_483_647) fail(operation, 'INVALID_INPUT', 'The owned runner identity is invalid.');
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const row = this.#db.prepare(`
        SELECT workflow_id FROM operations_work
        WHERE job_id = ? AND state = 'running' AND worker_id = ?
          AND generation = ? AND fence_token = ?
      `).get(fields.jobId, fields.ownerId, fields.generation, fields.fence);
      if (!row) fail(operation, 'STALE_FENCE', 'The worker claim is no longer current.');
      const workflow = this.#workflow(row.workflow_id, operation);
      const now = this.#now(operation);
      if (workflow.lease_owner !== fields.ownerId || Number(workflow.fence_token) !== fields.fence
          || workflow.cancellation_requested || workflow.lease_expires_ms === null
          || Number(workflow.lease_expires_ms) <= now) {
        fail(operation, 'STALE_FENCE', 'The worker claim is no longer eligible to execute.');
      }
      consumeOperationsStorageReservation(
        this.#db, this.#databasePath, this.#operationsResourceCeilings,
        workflow.workflow_id, OPS_GENERATION_WRITE_RESERVE_BYTES, operation,
        OPS_GENERATION_WRITE_RESERVE_BYTES
      );
      this.#db.prepare(`
        UPDATE operations_work SET runner_pid = ?, updated_at_ms = ?
        WHERE job_id = ? AND generation = ? AND fence_token = ?
      `).run(fields.runnerPid, now, fields.jobId, fields.generation, fields.fence);
      return Object.freeze({ registered: true, runnerPid: fields.runnerPid });
    });
  }

  /** @internal Claims one eligible job and its workflow fence in one transaction. */
  _operationsClaim(capability, input) {
    const operation = '_operationsClaim';
    if (capability !== OPERATIONS_STORAGE_CAPABILITY) {
      fail(operation, 'INVALID_INPUT', 'The private worker operations capability is required.');
    }
    const fields = readOwnDataRecord(input, ['ownerId', 'policy', 'recoverableJobIds'], operation, ['ownerId', 'policy']);
    const policy = readOperationsPolicy(fields.policy, operation);
    const recoverableJobIds = readOperationsJobIdList(fields.recoverableJobIds, policy.maxInFlightGlobal, operation);
    if (!opsSafeIdentifier(fields.ownerId, 128)) {
      fail(operation, 'INVALID_INPUT', 'The worker identity is invalid.');
    }
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      ensureOperationsPolicy(
        this.#db, policy, operation, true, this.#databasePath, this.#operationsResourceCeilings
      );
      const now = this.#now(operation);
      for (let pass = 0; pass <= policy.maxQueueRecords; pass += 1) {
      let reconciledInPass = false;
      const meta = this.#db.prepare('SELECT * FROM operations_meta WHERE singleton = 1').get();
      if (!meta) return null;
      const recoverableRows = recoverableJobIds.length === 0 ? [] : this.#db.prepare(`
        SELECT job_id, tenant_id, project_id FROM operations_work
        WHERE state = 'running' AND lease_expires_ms <= ?
          AND job_id IN (${recoverableJobIds.map(() => '?').join(',')})
        ORDER BY job_id LIMIT ?
      `).all(now, ...recoverableJobIds, policy.maxInFlightGlobal);
      const recoverableByJob = new Map(recoverableRows.map(row => [row.job_id, row]));
      const tenants = this.#db.prepare(`
        SELECT tenant_id, in_flight, last_project_id
        FROM operations_tenants WHERE pending_records > 0
        ORDER BY tenant_id LIMIT ?
      `).all(policy.maxTenants + 1);
      if (tenants.length > policy.maxTenants) return Object.freeze({ claim: null, incomplete: true });
      const allProjects = this.#db.prepare(`
        SELECT tenant_id, project_id, in_flight
        FROM operations_projects WHERE pending_records > 0
        ORDER BY tenant_id, project_id LIMIT ?
      `).all(policy.maxProjects + 1);
      if (allProjects.length > policy.maxProjects) return Object.freeze({ claim: null, incomplete: true });
      const projectsByTenant = new Map();
      for (const project of allProjects) {
        if (!projectsByTenant.has(project.tenant_id)) projectsByTenant.set(project.tenant_id, []);
        projectsByTenant.get(project.tenant_id).push(project);
      }
      const lastTenant = meta.last_tenant_id;
      const tenantOrder = lastTenant === null
        ? tenants
        : [...tenants.filter(item => item.tenant_id > lastTenant), ...tenants.filter(item => item.tenant_id <= lastTenant)];
      for (const tenant of tenantOrder) {
        const projects = projectsByTenant.get(tenant.tenant_id) ?? [];
        const projectOrder = tenant.last_project_id === null
          ? projects
          : [...projects.filter(item => item.project_id > tenant.last_project_id), ...projects.filter(item => item.project_id <= tenant.last_project_id)];
        for (const project of projectOrder) {
          const recoverable = [...recoverableByJob.values()].find(row =>
            row.tenant_id === tenant.tenant_id && row.project_id === project.project_id);
          let workRow = recoverable
            ? this.#db.prepare(`
              SELECT * FROM operations_work WHERE job_id = ? AND state = 'running'
                AND lease_expires_ms <= ?
            `).get(recoverable.job_id, now)
            : this.#db.prepare(`
              SELECT * FROM operations_work
              WHERE tenant_id = ? AND project_id = ? AND state = 'queued'
              ORDER BY created_at_ms, job_id LIMIT 1
            `).get(tenant.tenant_id, project.project_id);
          if (!workRow) continue;
          let workflow = this.#workflow(workRow.workflow_id, operation);
          if (Number(workRow.fence_token) !== Number(workflow.fence_token)) continue;
          if (workRow.state === 'running') {
            const workflowLeaseMatchesQueueClaim = workflow.lease_owner === workRow.worker_id
              && workflow.lease_expires_ms !== null && workRow.lease_expires_ms !== null
              && Number(workflow.lease_expires_ms) === Number(workRow.lease_expires_ms);
            const workflowLeaseWasReleased = workflow.lease_owner === null
              && workflow.lease_expires_ms === null;
            // A public transition may make this exact fenced generation terminal or
            // unresolved, and cancellation/release may clear its workflow lease.
            // The queue row plus matching monotonic fence still identifies the
            // generation to reconcile; a different non-null owner/lease is stale.
            if (!workflowLeaseMatchesQueueClaim && !workflowLeaseWasReleased) continue;
          } else if (!['queued', 'running'].includes(workflow.state) || workflow.lease_owner !== null
              || workflow.lease_expires_ms !== null) {
            continue;
          }
          const unresolved = this.#db.prepare(`
            SELECT 1 FROM operation_attempts
            WHERE workflow_id = ? AND state IN ('intent','outcome_unknown','response_received')
            LIMIT 1
          `).get(workRow.workflow_id);
          if (unresolved) {
            if (recoverable && workRow.state === 'running') {
              const needsReconciliationState = ![
                'reconciliation_pending', 'review_required', 'succeeded', 'failed', 'cancelled'
              ].includes(workflow.state);
              const needsLeaseClear = workflow.lease_owner !== null || workflow.lease_expires_ms !== null;
              const nextWorkflowState = needsReconciliationState ? 'reconciliation_pending' : workflow.state;
              const nextRevision = Number(workflow.revision) + (needsReconciliationState || needsLeaseClear ? 1 : 0);
              if (needsReconciliationState || needsLeaseClear) {
                consumeOperationsStorageReservation(
                  this.#db, this.#databasePath, this.#operationsResourceCeilings,
                  workflow.workflow_id, OPS_GENERATION_WRITE_RESERVE_BYTES, operation,
                  OPS_GENERATION_WRITE_RESERVE_BYTES
                );
                const changed = this.#db.prepare(`
                  UPDATE workflows SET state = ?, revision = ?,
                    lease_owner = NULL, lease_expires_ms = NULL, updated_at_ms = ?
                  WHERE workflow_id = ? AND revision = ? AND fence_token = ?
                    AND lease_owner IS ? AND lease_expires_ms IS ?
                `).run(nextWorkflowState, nextRevision, now, workflow.workflow_id, Number(workflow.revision),
                  Number(workflow.fence_token), workflow.lease_owner, workflow.lease_expires_ms);
                if (Number(changed.changes) !== 1) {
                  fail(operation, 'STALE_FENCE', 'The expired unresolved operation no longer owns its workflow revision.');
                }
              }
              this.#appendEvent({
                eventId: 'ops-expired-unknown:' + workRow.job_id + ':' + Number(workRow.generation),
                workflowId: workflow.workflow_id,
                revision: nextRevision,
                type: 'operations_work_unknown',
                fromState: workflow.state,
                toState: nextWorkflowState,
                fence: Number(workflow.fence_token),
                atMs: now
              });
              const changedWork = this.#db.prepare(`
                UPDATE operations_work SET state = 'unknown', worker_id = NULL, runner_pid = NULL,
                  lease_expires_ms = NULL, updated_at_ms = ?, reason_code = 'EXTERNAL_OUTCOME_UNRESOLVED'
                WHERE job_id = ? AND state = 'running' AND generation = ? AND fence_token = ?
                  AND worker_id = ? AND lease_expires_ms = ?
              `).run(now, workRow.job_id, Number(workRow.generation), Number(workRow.fence_token),
                workRow.worker_id, Number(workRow.lease_expires_ms));
              if (Number(changedWork.changes) !== 1) {
                fail(operation, 'STALE_FENCE', 'The expired unresolved queue claim changed before reconciliation.');
              }
              adjustOperationsCounters(this.#db, {
                tenantId: workRow.tenant_id, projectId: workRow.project_id, inFlight: -1
              });
              reconciledInPass = true;
            }
            continue;
          }
          if (workflow.cancellation_requested || TERMINAL.has(workflow.state)) {
            const terminal = workflow.state === 'succeeded' ? 'completed'
              : workflow.state === 'failed' ? 'failed' : 'cancelled';
            const wasRunning = workRow.state === 'running';
            const cancelledWorkflow = workflow.cancellation_requested && !TERMINAL.has(workflow.state);
            const terminalLeaseNeedsClear = TERMINAL.has(workflow.state)
              && (workflow.lease_owner !== null || workflow.lease_expires_ms !== null);
            const nextWorkflowState = cancelledWorkflow ? 'cancelled' : workflow.state;
            const nextRevision = Number(workflow.revision)
              + (cancelledWorkflow || terminalLeaseNeedsClear ? 1 : 0);
            if (cancelledWorkflow || terminalLeaseNeedsClear) {
              consumeOperationsStorageReservation(
                this.#db, this.#databasePath, this.#operationsResourceCeilings,
                workflow.workflow_id, OPS_GENERATION_WRITE_RESERVE_BYTES, operation,
                OPS_GENERATION_WRITE_RESERVE_BYTES
              );
              const changed = this.#db.prepare(`
                UPDATE workflows SET state = ?, cancellation_requested = ?,
                  revision = ?, lease_owner = NULL, lease_expires_ms = NULL, updated_at_ms = ?
                WHERE workflow_id = ? AND revision = ? AND fence_token = ?
                  AND lease_owner IS ? AND lease_expires_ms IS ?
              `).run(nextWorkflowState, workflow.cancellation_requested || cancelledWorkflow ? 1 : 0,
                nextRevision, now, workflow.workflow_id,
                Number(workflow.revision), Number(workflow.fence_token),
                workflow.lease_owner, workflow.lease_expires_ms);
              if (Number(changed.changes) !== 1) {
                fail(operation, 'STALE_FENCE', 'The expired cancellation no longer owns its workflow revision.');
              }
            }
            const eventId = 'ops-reconcile:' + workRow.job_id + ':' + Number(workRow.generation);
            this.#appendEvent({
              eventId,
              workflowId: workRow.workflow_id,
              revision: nextRevision,
              type: 'operations_work_' + terminal,
              fromState: workflow.state,
              toState: nextWorkflowState,
              fence: Number(workflow.fence_token),
              atMs: now
            });
            workRow = this.#db.prepare('SELECT * FROM operations_work WHERE job_id = ?').get(workRow.job_id);
            const changedWork = this.#db.prepare(`
              UPDATE operations_work SET state = ?, worker_id = NULL, runner_pid = NULL, lease_expires_ms = NULL,
                updated_at_ms = ?, reason_code = ? WHERE job_id = ? AND state = ?
                  AND generation = ? AND fence_token = ?
                  AND worker_id IS ? AND lease_expires_ms IS ?
            `).run(terminal, now, workflow.cancellation_requested ? 'CANCELLED' : null,
              workRow.job_id, workRow.state, Number(workRow.generation), Number(workRow.fence_token),
              workRow.worker_id, workRow.lease_expires_ms);
            if (Number(changedWork.changes) !== 1) {
              fail(operation, 'STALE_FENCE', 'The expired terminal queue claim changed before reconciliation.');
            }
            adjustOperationsCounters(this.#db, {
              tenantId: workRow.tenant_id,
              projectId: workRow.project_id,
              pendingRecords: -1,
              pendingBytes: -Number(workRow.work_bytes),
              inFlight: wasRunning ? -1 : 0,
              reservedEvents: -Number(workRow.reserved_event_slots),
              reservedDiskBytes: -Number(workRow.reserved_disk_bytes)
            });
            this.#db.prepare(`
              UPDATE operations_work SET reserved_event_slots = 0, reserved_disk_bytes = 0
              WHERE job_id = ?
            `).run(workRow.job_id);
            reconciledInPass = true;
            continue;
          }
          if (!['queued', 'running'].includes(workflow.state)) continue;
          const wasRunning = workRow.state === 'running';
          const currentMeta = this.#db.prepare(
            'SELECT in_flight FROM operations_meta WHERE singleton = 1'
          ).get();
          const currentTenant = this.#db.prepare(
            'SELECT in_flight FROM operations_tenants WHERE tenant_id = ?'
          ).get(workRow.tenant_id);
          const currentProject = this.#db.prepare(`
            SELECT in_flight FROM operations_projects WHERE tenant_id = ? AND project_id = ?
          `).get(workRow.tenant_id, workRow.project_id);
          if (!currentMeta || !currentTenant || !currentProject) {
            fail(operation, 'CHECKPOINT_CORRUPT', 'The worker concurrency counters are incomplete.');
          }
          const globalInFlight = Number(currentMeta.in_flight);
          const tenantInFlight = Number(currentTenant.in_flight);
          const projectInFlight = Number(currentProject.in_flight);
          if (![globalInFlight, tenantInFlight, projectInFlight].every(value => Number.isSafeInteger(value) && value >= 0)) {
            fail(operation, 'CHECKPOINT_CORRUPT', 'The worker concurrency counters are invalid.');
          }
          if (wasRunning) {
            if (globalInFlight === 0 || tenantInFlight === 0 || projectInFlight === 0) {
              fail(operation, 'CHECKPOINT_CORRUPT', 'An expired worker claim has no reserved concurrency slot.');
            }
            if (globalInFlight > policy.maxInFlightGlobal
                || tenantInFlight > policy.maxInFlightTenant
                || projectInFlight > policy.maxInFlightProject) continue;
          } else if (globalInFlight >= policy.maxInFlightGlobal
              || tenantInFlight >= policy.maxInFlightTenant
              || projectInFlight >= policy.maxInFlightProject) {
            // A recovery ID never grants a different queued job an additional slot.
            // Counters are reread after any earlier terminal/unknown reconciliation.
            continue;
          }
          if (Number(workRow.generation) >= OPS_MAX_WORKER_GENERATIONS) {
            consumeOperationsStorageReservation(
              this.#db, this.#databasePath, this.#operationsResourceCeilings,
              workflow.workflow_id, OPS_GENERATION_WRITE_RESERVE_BYTES, operation
            );
            const revision = Number(workflow.revision) + 1;
            this.#db.prepare(`
              UPDATE workflows SET state = 'review_required', revision = ?, lease_owner = NULL,
                lease_expires_ms = NULL, updated_at_ms = ?
              WHERE workflow_id = ? AND revision = ?
            `).run(revision, now, workflow.workflow_id, Number(workflow.revision));
            this.#appendEvent({
              eventId: 'ops-generation-limit:' + workRow.job_id + ':' + Number(workRow.generation),
              workflowId: workflow.workflow_id,
              revision,
              type: 'operations_work_parked',
              fromState: workflow.state,
              toState: 'review_required',
              fence: Number(workflow.fence_token),
              atMs: now
            });
            this.#db.prepare(`
              UPDATE operations_work SET state = 'parked', worker_id = NULL, runner_pid = NULL,
                lease_expires_ms = NULL, updated_at_ms = ?, reason_code = 'WORKER_GENERATION_LIMIT'
              WHERE job_id = ? AND generation = ?
            `).run(now, workRow.job_id, Number(workRow.generation));
            if (workRow.state === 'running') {
              adjustOperationsCounters(this.#db, {
                tenantId: workRow.tenant_id,
                projectId: workRow.project_id,
                inFlight: -1
              });
            }
            reconciledInPass = true;
            continue;
          }
          if (workflow.lease_owner !== null && Number(workflow.lease_expires_ms) > now) continue;
          const expiresAtMs = now + policy.leaseTtlMs;
          const fence = Number(workflow.fence_token) + 1;
          if (!Number.isSafeInteger(expiresAtMs) || !Number.isSafeInteger(fence)) {
            fail(operation, 'INVALID_INPUT', 'The worker lease is outside its supported range.');
          }
          consumeOperationsStorageReservation(
            this.#db, this.#databasePath, this.#operationsResourceCeilings,
            workflow.workflow_id, OPS_GENERATION_WRITE_RESERVE_BYTES, operation,
            OPS_GENERATION_WRITE_RESERVE_BYTES
          );
          let revision = Number(workflow.revision);
          if (workflow.state === 'queued') {
            revision += 1;
            this.#db.prepare(`
              UPDATE workflows SET state = 'running', revision = ?, fence_token = ?,
                lease_owner = ?, lease_expires_ms = ?, updated_at_ms = ?
              WHERE workflow_id = ? AND revision = ? AND state = 'queued'
            `).run(revision, fence, fields.ownerId, expiresAtMs, now, workflow.workflow_id, Number(workflow.revision));
          } else {
            this.#db.prepare(`
              UPDATE workflows SET fence_token = ?, lease_owner = ?, lease_expires_ms = ?, updated_at_ms = ?
              WHERE workflow_id = ? AND revision = ? AND state = 'running'
            `).run(fence, fields.ownerId, expiresAtMs, now, workflow.workflow_id, Number(workflow.revision));
          }
          this.#appendEvent({
            eventId: 'ops-claim:' + workRow.job_id + ':' + (Number(workRow.generation) + 1),
            workflowId: workflow.workflow_id,
            revision,
            type: 'operations_work_claimed',
            fromState: workflow.state,
            toState: 'running',
            fence,
            atMs: now
          });
          const generation = Number(workRow.generation) + 1;
          this.#db.prepare(`
            UPDATE operations_work SET state = 'running', generation = ?, worker_id = ?,
              runner_pid = NULL, fence_token = ?, lease_expires_ms = ?, updated_at_ms = ?, reason_code = NULL
            WHERE job_id = ? AND generation = ?
          `).run(generation, fields.ownerId, fence, expiresAtMs, now, workRow.job_id, Number(workRow.generation));
          if (!wasRunning) {
            adjustOperationsCounters(this.#db, {
              tenantId: workRow.tenant_id,
              projectId: workRow.project_id,
              inFlight: 1
            });
          }
          this.#db.prepare('UPDATE operations_meta SET last_tenant_id = ? WHERE singleton = 1')
            .run(tenant.tenant_id);
          this.#db.prepare(`
            UPDATE operations_tenants SET last_project_id = ? WHERE tenant_id = ?
          `).run(project.project_id, tenant.tenant_id);
          const claimRow = this.#db.prepare('SELECT * FROM operations_work WHERE job_id = ?').get(workRow.job_id);
          return Object.freeze({
            claim: Object.freeze({
              jobId: claimRow.job_id,
              workflowId: claimRow.workflow_id,
              ownerId: fields.ownerId,
              generation,
              fence,
              revision,
              leaseExpiresAtMs: expiresAtMs,
              workBytes: Number(claimRow.work_bytes),
              workSha256: claimRow.work_sha256
            }),
            incomplete: false
          });
        }
      }
      if (!reconciledInPass) return null;
      }
      return null;
    });
  }

  /** @internal A child reads only its current fenced claim and its bounded local-plan payload. */
  _operationsReadClaim(capability, input) {
    const operation = '_operationsReadClaim';
    if (capability !== OPERATIONS_STORAGE_CAPABILITY) {
      fail(operation, 'INVALID_INPUT', 'The private worker operations capability is required.');
    }
    const fields = readOwnDataRecord(input, ['jobId', 'ownerId', 'generation', 'fence'], operation);
    if (!opsSafeIdentifier(fields.jobId, 256) || !opsSafeIdentifier(fields.ownerId, 128)) {
      fail(operation, 'INVALID_INPUT', 'The worker claim identity is invalid.');
    }
    expectInteger(fields.generation, 'generation', operation, 1);
    expectInteger(fields.fence, 'fence', operation, 1);
    this.#assertOpen(operation);
    const row = this.#db.prepare('SELECT * FROM operations_work WHERE job_id = ?').get(fields.jobId);
    if (!row || row.state !== 'running' || row.worker_id !== fields.ownerId
        || Number(row.generation) !== fields.generation || Number(row.fence_token) !== fields.fence) {
      fail(operation, 'STALE_FENCE', 'The worker claim is no longer current.');
    }
    const workflow = this.#workflow(row.workflow_id, operation);
    const now = this.#now(operation);
    if (workflow.state !== 'running' || workflow.cancellation_requested
        || workflow.lease_owner !== fields.ownerId
        || Number(workflow.fence_token) !== fields.fence
        || workflow.lease_expires_ms === null || Number(workflow.lease_expires_ms) <= now) {
      fail(operation, 'STALE_FENCE', 'The worker claim is no longer eligible to execute.');
    }
    const unresolved = this.#db.prepare(`
      SELECT 1 FROM operation_attempts
      WHERE workflow_id = ? AND state IN ('intent','outcome_unknown','response_received')
      LIMIT 1
    `).get(row.workflow_id);
    if (unresolved) fail(operation, 'OUTCOME_UNRESOLVED', 'An existing external outcome blocks local dispatch.');
    const expected = digest(JSON.stringify([
      'agent-harness.operations-work.v1', row.tenant_id, row.project_id,
      row.limits_version, row.work_json
    ]));
    if (expected !== row.work_sha256 || Buffer.byteLength(row.work_json, 'utf8') !== Number(row.work_bytes)) {
      fail(operation, 'CHECKPOINT_CORRUPT', 'The accepted local work failed its integrity check.');
    }
    let work;
    try {
      work = parseBoundedJson(row.work_json, { maxBytes: Number(row.work_bytes), maxDepth: 24, maxNodes: 2048 });
    } catch {
      fail(operation, 'CHECKPOINT_CORRUPT', 'The accepted local work is invalid.');
    }
    const normalized = readOwnDataRecord(work, ['type', 'version', 'planJson', 'limitsJson'], operation);
    if (normalized.type !== 'local_plan' || normalized.version !== 1
        || typeof normalized.planJson !== 'string' || typeof normalized.limitsJson !== 'string') {
      fail(operation, 'CHECKPOINT_CORRUPT', 'The accepted local work type is unsupported.');
    }
    return Object.freeze({
      jobId: row.job_id,
      workflowId: row.workflow_id,
      ownerId: fields.ownerId,
      generation: fields.generation,
      fence: fields.fence,
      work: Object.freeze(normalized)
    });
  }

  /** @internal Fenced terminal, park, or pure-work requeue after the owned child exits. */
  _operationsFinish(capability, input) {
    const operation = '_operationsFinish';
    if (capability !== OPERATIONS_STORAGE_CAPABILITY) {
      fail(operation, 'INVALID_INPUT', 'The private worker operations capability is required.');
    }
    const fields = readOwnDataRecord(input, [
      'jobId', 'ownerId', 'generation', 'fence', 'outcome', 'childExited'
    ], operation, ['jobId', 'ownerId', 'generation', 'fence', 'outcome']);
    if (!opsSafeIdentifier(fields.jobId, 256) || !opsSafeIdentifier(fields.ownerId, 128)
        || !['completed', 'shutdown', 'stopped', 'cancelled'].includes(fields.outcome)
        || (fields.childExited !== undefined && typeof fields.childExited !== 'boolean')) {
      fail(operation, 'INVALID_INPUT', 'The worker completion record is invalid.');
    }
    expectInteger(fields.generation, 'generation', operation, 1);
    expectInteger(fields.fence, 'fence', operation, 1);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      let queue = this.#db.prepare('SELECT * FROM operations_work WHERE job_id = ?').get(fields.jobId);
      if (!queue || queue.state !== 'running' || queue.worker_id !== fields.ownerId
          || Number(queue.generation) !== fields.generation
          || Number(queue.fence_token) !== fields.fence) {
        fail(operation, 'STALE_FENCE', 'The worker result no longer matches the current job generation.');
      }
      let workflow = this.#workflow(queue.workflow_id, operation);
      const now = this.#now(operation);
      const currentOwner = workflow.lease_owner === fields.ownerId
        && Number(workflow.fence_token) === fields.fence;
      if (!currentOwner && !workflow.cancellation_requested) {
        fail(operation, 'STALE_FENCE', 'The worker result no longer owns the workflow fence.');
      }
      if (currentOwner && !TERMINAL.has(workflow.state)) {
        const leaseExpired = workflow.lease_expires_ms === null
          || Number(workflow.lease_expires_ms) <= now;
        if (leaseExpired && fields.childExited !== true) {
          fail(operation, 'LEASE_EXPIRED', 'The worker lease expired before the child exit was durably settled.');
        }
        if (!leaseExpired) this.#assertLease(workflow, { ownerId: fields.ownerId, fence: fields.fence }, operation);
      }
      const unresolved = this.#db.prepare(`
        SELECT 1 FROM operation_attempts
        WHERE workflow_id = ? AND state IN ('intent','outcome_unknown','response_received')
        LIMIT 1
      `).get(queue.workflow_id);
      let isComplete = false;
      try {
        const checkpoint = this.getCheckpoint(queue.workflow_id);
        const state = readLocalControllerState(checkpoint.data, operation);
        const limits = createLimitSet(state.limitsJson);
        const plan = compilePlan(state.planJson, limits, createDefaultRegistry(), queue.workflow_id);
        const validated = validatePrivateCheckpointResults(plan, state.completedResultsJson);
        isComplete = state.pendingStepId === null
          && getNextReadyStep(plan, state.completedResultsJson) === null
          && validated.resultCount === getPlanStepIds(plan).length;
      } catch {
        isComplete = false;
      }

      const requestedCancel = workflow.cancellation_requested === 1 || fields.outcome === 'cancelled';
      let nextState;
      let nextWorkflowState = workflow.state;
      if (unresolved) {
        nextState = 'unknown';
        if (!['reconciliation_pending', 'review_required', 'succeeded', 'failed', 'cancelled'].includes(workflow.state)) {
          nextWorkflowState = 'reconciliation_pending';
        }
      } else if (TERMINAL.has(workflow.state)) {
        nextState = workflow.state === 'succeeded' ? 'completed'
          : workflow.state === 'failed' ? 'failed' : 'cancelled';
      } else if (requestedCancel) {
        nextState = 'cancelled';
        nextWorkflowState = 'cancelled';
      } else if (isComplete) {
        nextState = 'completed';
        nextWorkflowState = 'succeeded';
      } else if (fields.outcome === 'shutdown') {
        nextState = 'queued';
      } else {
        nextState = 'parked';
        nextWorkflowState = 'review_required';
      }

      consumeOperationsStorageReservation(
        this.#db, this.#databasePath, this.#operationsResourceCeilings,
        queue.workflow_id, OPS_GENERATION_WRITE_RESERVE_BYTES, operation,
        OPS_GENERATION_WRITE_RESERVE_BYTES
      );

      const changesWorkflow = nextWorkflowState !== workflow.state;
      const nextRevision = changesWorkflow ? Number(workflow.revision) + 1 : Number(workflow.revision);
      this.#db.prepare(`
        UPDATE workflows SET state = ?, revision = ?, cancellation_requested = ?,
          lease_owner = NULL, lease_expires_ms = NULL, updated_at_ms = ?
        WHERE workflow_id = ? AND revision = ?
      `).run(
        nextWorkflowState,
        nextRevision,
        requestedCancel || workflow.cancellation_requested ? 1 : 0,
        now,
        queue.workflow_id,
        Number(workflow.revision)
      );
      const eventType = nextState === 'completed' ? 'operations_work_completed'
        : nextState === 'unknown' ? 'operations_work_unknown'
          : nextState === 'cancelled' ? 'operations_work_cancelled'
            : nextState === 'queued' ? 'operations_work_requeued'
              : nextState === 'parked' ? 'operations_work_parked'
                : 'operations_work_failed';
      this.#appendEvent({
        eventId: 'ops-finish:' + queue.job_id + ':' + fields.generation + ':' + nextState,
        workflowId: queue.workflow_id,
        revision: nextRevision,
        type: eventType,
        fromState: workflow.state,
        toState: nextWorkflowState,
        fence: fields.fence,
        atMs: now
      });
      queue = this.#db.prepare('SELECT * FROM operations_work WHERE job_id = ?').get(queue.job_id);
      const terminal = OPS_TERMINAL_STATES.has(nextState);
      this.#db.prepare(`
        UPDATE operations_work SET state = ?, worker_id = NULL, lease_expires_ms = NULL,
          runner_pid = NULL, updated_at_ms = ?, reason_code = ?
        WHERE job_id = ? AND generation = ? AND fence_token = ?
      `).run(
        nextState,
        now,
        nextState === 'unknown' ? 'EXTERNAL_OUTCOME_UNRESOLVED'
          : nextState === 'parked' ? 'LOCAL_EXECUTION_STOPPED'
            : nextState === 'queued' ? 'SAFE_RESTART_RESUME'
              : nextState === 'cancelled' ? 'CANCELLED' : null,
        queue.job_id,
        fields.generation,
        fields.fence
      );
      adjustOperationsCounters(this.#db, {
        tenantId: queue.tenant_id,
        projectId: queue.project_id,
        pendingRecords: terminal ? -1 : 0,
        pendingBytes: terminal ? -Number(queue.work_bytes) : 0,
        inFlight: -1,
        reservedEvents: terminal ? -Number(queue.reserved_event_slots) : 0,
        reservedDiskBytes: terminal ? -Number(queue.reserved_disk_bytes) : 0
      });
      if (terminal) {
        this.#db.prepare(`
          UPDATE operations_work SET reserved_event_slots = 0, reserved_disk_bytes = 0
          WHERE job_id = ?
        `).run(queue.job_id);
      }
      workflow = this.#workflow(queue.workflow_id, operation);
      return Object.freeze({
        jobId: queue.job_id,
        workflowId: queue.workflow_id,
        state: nextState,
        workflowState: workflow.state,
        generation: fields.generation,
        fence: fields.fence,
        completeCheckpointVerified: isComplete,
        unresolvedExternalEffect: Boolean(unresolved)
      });
    });
  }

  /** @internal Scope-checked cancellation CAS; active child remains counted until reaped. */
  _operationsCancel(capability, input) {
    const operation = '_operationsCancel';
    if (capability !== OPERATIONS_STORAGE_CAPABILITY) {
      fail(operation, 'INVALID_INPUT', 'The private worker operations capability is required.');
    }
    const fields = readOwnDataRecord(input, [
      'jobId', 'tenantId', 'projectId', 'expectedGeneration'
    ], operation);
    if (!opsSafeIdentifier(fields.jobId, 256) || !opsSafeIdentifier(fields.tenantId)
        || !opsSafeIdentifier(fields.projectId)) {
      fail(operation, 'INVALID_INPUT', 'The scoped cancellation identity is invalid.');
    }
    expectInteger(fields.expectedGeneration, 'expectedGeneration', operation);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const queue = this.#db.prepare('SELECT * FROM operations_work WHERE job_id = ?').get(fields.jobId);
      if (!queue || queue.tenant_id !== fields.tenantId || queue.project_id !== fields.projectId) {
        fail(operation, 'WORKFLOW_NOT_FOUND', 'The scoped work item does not exist.');
      }
      if (Number(queue.generation) !== fields.expectedGeneration) {
        fail(operation, 'CONCURRENT_MODIFICATION', 'The work item changed before cancellation was accepted.', true);
      }
      if (OPS_TERMINAL_STATES.has(queue.state)) {
        return Object.freeze({ accepted: false, state: queue.state, generation: Number(queue.generation) });
      }
      const workflow = this.#workflow(queue.workflow_id, operation);
      if (workflow.cancellation_requested) {
        return Object.freeze({ accepted: true, state: queue.state, generation: Number(queue.generation) });
      }
      const now = this.#now(operation);
      const active = queue.state === 'running';
      if (active) {
        if (workflow.lease_owner !== queue.worker_id
            || Number(workflow.fence_token) !== Number(queue.fence_token)) {
          fail(operation, 'STALE_FENCE', 'The work item no longer owns its workflow fence.');
        }
        if (workflow.lease_expires_ms === null || Number(workflow.lease_expires_ms) <= now) {
          fail(operation, 'LEASE_EXPIRED', 'The worker lease expired before cancellation.');
        }
        consumeOperationsStorageReservation(
          this.#db, this.#databasePath, this.#operationsResourceCeilings,
          queue.workflow_id, OPS_GENERATION_WRITE_RESERVE_BYTES, operation,
          OPS_GENERATION_WRITE_RESERVE_BYTES
        );
        const revision = Number(workflow.revision) + 1;
        this.#db.prepare(`
          UPDATE workflows SET cancellation_requested = 1, revision = ?, updated_at_ms = ?
          WHERE workflow_id = ? AND revision = ? AND cancellation_requested = 0
        `).run(revision, now, queue.workflow_id, Number(workflow.revision));
        this.#appendEvent({
          eventId: 'ops-cancel-requested:' + queue.job_id + ':' + queue.generation,
          workflowId: queue.workflow_id,
          revision,
          type: 'operations_cancel_requested',
          fromState: workflow.state,
          toState: workflow.state,
          fence: Number(queue.fence_token),
          atMs: now
        });
        this.#db.prepare('UPDATE operations_work SET updated_at_ms = ? WHERE job_id = ?')
          .run(now, queue.job_id);
        return Object.freeze({
          accepted: true,
          state: 'cancellation_requested',
          generation: Number(queue.generation),
          ownerId: queue.worker_id,
          fence: Number(queue.fence_token)
        });
      }

      const unresolved = this.#db.prepare(`
        SELECT 1 FROM operation_attempts
        WHERE workflow_id = ? AND state IN ('intent','outcome_unknown','response_received')
        LIMIT 1
      `).get(queue.workflow_id);
      if (unresolved) {
        consumeOperationsStorageReservation(
          this.#db, this.#databasePath, this.#operationsResourceCeilings,
          queue.workflow_id, OPS_GENERATION_WRITE_RESERVE_BYTES, operation,
          OPS_GENERATION_WRITE_RESERVE_BYTES
        );
        const nextRevision = workflow.state === 'reconciliation_pending'
          ? Number(workflow.revision) : Number(workflow.revision) + 1;
        this.#db.prepare(`
          UPDATE workflows SET cancellation_requested = 1, state = 'reconciliation_pending',
            revision = ?, updated_at_ms = ? WHERE workflow_id = ? AND revision = ?
        `).run(nextRevision, now, queue.workflow_id, Number(workflow.revision));
        this.#appendEvent({
          eventId: 'ops-cancel-unknown:' + queue.job_id + ':' + queue.generation,
          workflowId: queue.workflow_id,
          revision: nextRevision,
          type: 'operations_work_unknown',
          fromState: workflow.state,
          toState: 'reconciliation_pending',
          fence: Number(workflow.fence_token),
          atMs: now
        });
        this.#db.prepare(`
          UPDATE operations_work SET state = 'unknown', worker_id = NULL, runner_pid = NULL, lease_expires_ms = NULL,
            updated_at_ms = ?, reason_code = 'EXTERNAL_OUTCOME_UNRESOLVED' WHERE job_id = ?
        `).run(now, queue.job_id);
        adjustOperationsCounters(this.#db, {
          tenantId: queue.tenant_id,
          projectId: queue.project_id,
          inFlight: 0,
          reservedEvents: 0,
          reservedDiskBytes: 0
        });
        return Object.freeze({ accepted: true, state: 'unknown', generation: Number(queue.generation) });
      }
      consumeOperationsStorageReservation(
        this.#db, this.#databasePath, this.#operationsResourceCeilings,
        queue.workflow_id, OPS_GENERATION_WRITE_RESERVE_BYTES, operation
      );
      const revision = Number(workflow.revision) + 1;
      this.#db.prepare(`
        UPDATE workflows SET state = 'cancelled', cancellation_requested = 1, revision = ?,
          lease_owner = NULL, lease_expires_ms = NULL, updated_at_ms = ?
        WHERE workflow_id = ? AND revision = ?
      `).run(revision, now, queue.workflow_id, Number(workflow.revision));
      this.#appendEvent({
        eventId: 'ops-cancelled:' + queue.job_id + ':' + queue.generation,
        workflowId: queue.workflow_id,
        revision,
        type: 'operations_work_cancelled',
        fromState: workflow.state,
        toState: 'cancelled',
        fence: Number(workflow.fence_token),
        atMs: now
      });
      const afterEvent = this.#db.prepare('SELECT * FROM operations_work WHERE job_id = ?').get(queue.job_id);
      this.#db.prepare(`
        UPDATE operations_work SET state = 'cancelled', worker_id = NULL, runner_pid = NULL,
          lease_expires_ms = NULL, updated_at_ms = ?, reason_code = 'CANCELLED'
        WHERE job_id = ?
      `).run(now, queue.job_id);
      adjustOperationsCounters(this.#db, {
        tenantId: queue.tenant_id,
        projectId: queue.project_id,
        pendingRecords: -1,
        pendingBytes: -Number(queue.work_bytes),
        inFlight: 0,
        reservedEvents: -Number(afterEvent.reserved_event_slots),
        reservedDiskBytes: -Number(afterEvent.reserved_disk_bytes)
      });
      this.#db.prepare(`
        UPDATE operations_work SET reserved_event_slots = 0, reserved_disk_bytes = 0 WHERE job_id = ?
      `).run(queue.job_id);
      return Object.freeze({ accepted: true, state: 'cancelled', generation: Number(queue.generation) });
    });
  }

  /** @internal Exact indexed job lookup under a server-derived synthetic scope input. */
  _operationsGetScopedJob(capability, input) {
    const operation = '_operationsGetScopedJob';
    if (capability !== OPERATIONS_STORAGE_CAPABILITY) {
      fail(operation, 'INVALID_INPUT', 'The private worker operations capability is required.');
    }
    const fields = readOwnDataRecord(input, ['tenantId', 'projectId', 'jobId'], operation);
    if (!opsSafeIdentifier(fields.tenantId) || !opsSafeIdentifier(fields.projectId)
        || !opsSafeIdentifier(fields.jobId, 256)) {
      fail(operation, 'INVALID_INPUT', 'The scoped work identity is invalid.');
    }
    this.#assertOpen(operation);
    const row = this.#db.prepare(`
      SELECT job_id, workflow_id, state, generation, created_at_ms, updated_at_ms, reason_code
      FROM operations_work
      WHERE tenant_id = ? AND project_id = ? AND job_id = ?
    `).get(fields.tenantId, fields.projectId, fields.jobId);
    if (!row) return null;
    return Object.freeze({
      jobId: row.job_id,
      workflowId: row.workflow_id,
      state: row.state,
      generation: Number(row.generation),
      createdAtMs: Number(row.created_at_ms),
      updatedAtMs: Number(row.updated_at_ms),
      reasonCode: row.reason_code
    });
  }

  /** @internal Capped scope-specific keyset inspection; never returns work payloads. */
  _operationsReadPage(capability, input) {
    const operation = '_operationsReadPage';
    if (capability !== OPERATIONS_STORAGE_CAPABILITY) {
      fail(operation, 'INVALID_INPUT', 'The private worker operations capability is required.');
    }
    const fields = readOwnDataRecord(input, [
      'tenantId', 'projectId', 'afterJobId', 'limit'
    ], operation, ['tenantId', 'projectId']);
    if (!opsSafeIdentifier(fields.tenantId) || !opsSafeIdentifier(fields.projectId)
        || (fields.afterJobId !== undefined && !opsSafeIdentifier(fields.afterJobId, 256))) {
      fail(operation, 'INVALID_INPUT', 'The scoped operations page request is invalid.');
    }
    const limit = fields.limit ?? 25;
    expectInteger(limit, 'limit', operation, 1);
    if (limit > 100) fail(operation, 'INVALID_INPUT', 'The operations page exceeds its supported size.');
    this.#assertOpen(operation);
    const rows = this.#db.prepare(`
      SELECT job_id, workflow_id, state, generation, created_at_ms, updated_at_ms, reason_code
      FROM operations_work INDEXED BY operations_work_scope_page_idx
      WHERE tenant_id = ? AND project_id = ? AND (? IS NULL OR job_id > ?)
      ORDER BY job_id COLLATE BINARY LIMIT ?
    `).all(fields.tenantId, fields.projectId, fields.afterJobId ?? null,
      fields.afterJobId ?? null, limit + 1);
    const hasMore = rows.length > limit;
    const items = rows.slice(0, limit).map(row => Object.freeze({
      jobId: row.job_id,
      workflowId: row.workflow_id,
      state: row.state,
      generation: Number(row.generation),
      createdAtMs: Number(row.created_at_ms),
      updatedAtMs: Number(row.updated_at_ms),
      reasonCode: row.reason_code
    }));
    return Object.freeze({
      items: Object.freeze(items),
      hasMore,
      incomplete: hasMore,
      nextAfterJobId: hasMore && items.length > 0 ? items.at(-1).jobId : null
    });
  }

  /** @internal Fixed-schema resource, queue, telemetry, authority, and retention snapshot. */
  _operationsHealth(capability, input) {
    const operation = '_operationsHealth';
    if (capability !== OPERATIONS_STORAGE_CAPABILITY) {
      fail(operation, 'INVALID_INPUT', 'The private worker operations capability is required.');
    }
    const fields = readOwnDataRecord(input, ['policy', 'accepting', 'live', 'heartbeatAgeMs'], operation);
    const policy = readOperationsPolicy(fields.policy, operation);
    if (typeof fields.accepting !== 'boolean' || typeof fields.live !== 'boolean'
        || !Number.isSafeInteger(fields.heartbeatAgeMs) || fields.heartbeatAgeMs < 0) {
      fail(operation, 'INVALID_INPUT', 'The operations health snapshot is invalid.');
    }
    this.#assertOpen(operation);
    const meta = this.#db.prepare('SELECT * FROM operations_meta WHERE singleton = 1').get();
    if (!meta) fail(operation, 'CHECKPOINT_CORRUPT', 'The worker operations store is incomplete.');
    const cappedStatus = state => {
      const count = Number(this.#db.prepare(`
        SELECT COUNT(*) AS count FROM (
          SELECT job_id FROM operations_work WHERE state = ? LIMIT 1001
        )
      `).get(state).count);
      return Object.freeze({ count: Math.min(count, 1000), capped: count > 1000 });
    };
    const unknownCount = Number(this.#db.prepare(`
      SELECT COUNT(*) AS count FROM (
        SELECT job_id FROM (
          SELECT job_id FROM operations_work INDEXED BY operations_work_state_idx
          WHERE state = 'unknown' ORDER BY job_id LIMIT 1001
        )
        UNION
        SELECT job_id FROM (
          SELECT w.job_id FROM operation_attempts a INDEXED BY operations_attempts_state_workflow_idx
          JOIN operations_work w ON w.workflow_id = a.workflow_id
          WHERE a.state IN ('intent','outcome_unknown','response_received')
          ORDER BY w.job_id LIMIT 1001
        )
        LIMIT 1001
      )
    `).get().count);
    const statusCounts = Object.freeze({
      queued: cappedStatus('queued'),
      running: cappedStatus('running'),
      parked: cappedStatus('parked'),
      unknown: Object.freeze({ count: Math.min(unknownCount, 1000), capped: unknownCount > 1000 })
    });
    const cursorRow = this.#db.prepare(
      'SELECT last_sequence FROM telemetry_cursors WHERE consumer_id = ?'
    ).get('internal-telemetry-v1');
    const cursor = cursorRow ? Number(cursorRow.last_sequence) : 0;
    const pending = Number(this.#db.prepare(`
      SELECT COUNT(*) AS count FROM (
        SELECT sequence FROM workflow_events WHERE sequence > ? LIMIT ?
      )
    `).get(cursor, policy.maxEventBacklog + 1).count);
    const now = this.#now(operation);
    const staleLeaseCount = Number(this.#db.prepare(`
      SELECT COUNT(*) AS count FROM (
        SELECT job_id FROM operations_work INDEXED BY operations_work_lease_idx
        WHERE state = 'running' AND lease_expires_ms <= ? LIMIT 1001
      )
    `).get(now).count);
    const resources = operationsResourceSnapshot(this.#databasePath);
    const readiness = [];
    const policyIdentity = operationsPolicyIdentity(policy, operation);
    if (meta.policy_json === null) readiness.push('POLICY_UNCONFIGURED');
    else if (meta.policy_json !== policyIdentity) readiness.push('POLICY_MISMATCH');
    if (!fields.accepting) readiness.push('SHUTTING_DOWN');
    const minimumBudget = operationsLifecycleBudget(OPS_MIN_PLAN_JSON, '{}', policy, operation);
    const resourceStatus = operationsResourceCapacity(resources, meta, policy, {
      pendingEvents: pending,
      additionalDiskBytes: minimumBudget.diskBytes + OPS_MIN_WORK_BYTES,
      additionalWalBytes: policy.completionHeadroomBytes,
      additionalEventSlots: minimumBudget.eventSlots
    });
    readiness.push(...resourceStatus.reasons);
    if (Number(meta.pending_records) >= policy.maxQueueRecords
        || Number(meta.pending_bytes) + OPS_MIN_WORK_BYTES > policy.maxQueueBytes) readiness.push('QUEUE_SATURATED');
    if (unknownCount > 0) readiness.push('RECONCILIATION_PENDING');
    if (staleLeaseCount > 0) readiness.push('LEASE_STALE');
    const schemaVersion = readUserVersion(this.#db);
    if (schemaVersion > INTERNAL_SCHEMA_VERSION) readiness.push('SCHEMA_UNSUPPORTED');
    if (schemaVersion < INTERNAL_SCHEMA_VERSION) readiness.push('MIGRATION_REQUIRED');
    const state = !fields.accepting ? 'stopping'
      : readiness.includes('QUEUE_SATURATED') ? 'saturated'
        : readiness.length > 0 ? 'degraded'
          : 'accepting';
    return Object.freeze({
      schema: 'agent-harness.operations-health.v1',
      state,
      live: fields.live,
      ready: fields.accepting && readiness.length === 0,
      heartbeatAgeMs: fields.heartbeatAgeMs,
      degradedReasons: Object.freeze(readiness),
      store: Object.freeze({ status: 'available', schemaVersion, readOnlyProbe: true }),
      queue: Object.freeze({
        pendingRecords: Number(meta.pending_records),
        pendingBytes: Number(meta.pending_bytes),
        inFlight: Number(meta.in_flight),
        statusCounts
      }),
      capacity: Object.freeze({
        maxQueueRecords: policy.maxQueueRecords,
        maxQueueBytes: policy.maxQueueBytes,
        maxInFlightGlobal: policy.maxInFlightGlobal,
        maxInFlightTenant: policy.maxInFlightTenant,
        maxInFlightProject: policy.maxInFlightProject
      }),
      telemetry: Object.freeze({
        cursor,
        pendingRecords: Math.min(pending, policy.maxEventBacklog),
        pendingCapped: pending > policy.maxEventBacklog,
        reservedEventSlots: Number(meta.reserved_event_slots),
        maxPendingEvents: policy.maxEventBacklog,
        status: pending + Number(meta.reserved_event_slots) + minimumBudget.eventSlots > policy.maxEventBacklog
          ? 'telemetry_degraded' : 'local_outbox_pending'
      }),
      staleLeases: Object.freeze({ count: Math.min(staleLeaseCount, 1000), capped: staleLeaseCount > 1000 }),
      resources: resources ? Object.freeze({
        databaseBytes: resources.databaseBytes,
        walBytes: resources.walBytes,
        shmBytes: resources.shmBytes,
        storeBytes: resources.databaseBytes + resources.walBytes + resources.shmBytes,
        walLimitBytes: policy.maxWalBytes,
        freeBytes: resources.availableBytes,
        minimumFreeBytes: policy.minFreeDiskBytes,
        reservedCompletionBytes: Number(meta.reserved_disk_bytes),
        maximumStoreBytes: policy.maxStoreBytes
      }) : Object.freeze({ status: 'unknown' }),
      authority: Object.freeze({ status: 'authority_unavailable', externalDispatchEnabled: false }),
      provider: Object.freeze({ status: 'provider_unavailable', externalDispatchEnabled: false }),
      reconciliation: Object.freeze({
        count: statusCounts.unknown.count,
        capped: statusCounts.unknown.capped,
        incomplete: statusCounts.unknown.capped || statusCounts.unknown.count > 0
      }),
      retention: Object.freeze({ status: 'retention_not_configured', destructivePruningEnabled: false })
    });
  }

  /** @internal Owner handoff hook only; no local deletion or settlement is performed. */
  _operationsRetentionRequest(capability, input) {
    const operation = '_operationsRetentionRequest';
    if (capability !== OPERATIONS_STORAGE_CAPABILITY) {
      fail(operation, 'INVALID_INPUT', 'The private worker operations capability is required.');
    }
    const fields = readOwnDataRecord(input, ['ownerRole', 'policyVersion', 'recordClass', 'maximumItems'], operation);
    if (!['Agent and research', 'Finance and reliability', 'deployment operator'].includes(fields.ownerRole)
        || typeof fields.policyVersion !== 'string' || !opsSafeIdentifier(fields.policyVersion, 64)
        || typeof fields.recordClass !== 'string' || !opsSafeIdentifier(fields.recordClass, 64)) {
      fail(operation, 'INVALID_INPUT', 'The retention owner request is invalid.');
    }
    expectInteger(fields.maximumItems, 'maximumItems', operation, 1);
    if (fields.maximumItems > 100) fail(operation, 'INVALID_INPUT', 'The retention request exceeds its record cap.');
    return Object.freeze({
      accepted: false,
      status: 'retention_not_configured',
      ownerRole: fields.ownerRole,
      policyVersion: fields.policyVersion,
      recordClass: fields.recordClass,
      maximumItems: fields.maximumItems,
      deletionPerformed: false,
      settlementPerformed: false,
      reasonCode: 'OWNER_POLICY_AND_ACKNOWLEDGEMENT_REQUIRED'
    });
  }

  close() {
    if (!this.#db?.isOpen) return;
    try {
      this.#db.close();
    } catch (error) {
      throw mapStorageError('close', error);
    }
  }

  #assertOpen(operation) {
    if (!this.#db?.isOpen) fail(operation, 'STORE_CLOSED', 'The durable store is closed.');
  }

  #now(operation) {
    const value = this.#clock();
    if (!Number.isSafeInteger(value) || value < 0) {
      fail(operation, 'INVALID_INPUT', 'The injected clock returned an invalid timestamp.');
    }
    return value;
  }

  #eventId(provided, operation) {
    const value = provided === undefined ? this.#idFactory() : provided;
    expectString(value, 'eventId', operation);
    return value;
  }

  #assertEventAvailable(eventId, operation) {
    if (this.#db.prepare('SELECT 1 FROM workflow_events WHERE event_id = ?').get(eventId)) {
      fail(operation, 'IDEMPOTENCY_CONFLICT', 'The event identity has already been used.');
    }
  }

  #appendEvent({ eventId, workflowId, revision, type, fromState, toState, fence, atMs }) {
    const result = this.#db.prepare(`
      INSERT INTO workflow_events (
        event_id, workflow_id, revision, event_type, from_state, to_state, fence_token, occurred_at_ms
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(eventId, workflowId, revision, type, fromState, toState, fence, atMs);
    const sequence = Number(result.lastInsertRowid);
    this.#db.prepare('INSERT INTO outbox (event_id, event_sequence) VALUES (?, ?)').run(eventId, sequence);
    return sequence;
  }

  #workflow(workflowId, operation) {
    const row = this.#db.prepare('SELECT * FROM workflows WHERE workflow_id = ?').get(workflowId);
    if (!row) fail(operation, 'WORKFLOW_NOT_FOUND', 'The workflow does not exist.');
    return row;
  }

  #assertRevision(row, expectedRevision, operation) {
    expectInteger(expectedRevision, 'expectedRevision', operation);
    if (Number(row.revision) !== expectedRevision) {
      fail(operation, 'CONCURRENT_MODIFICATION', 'The workflow changed since this operation was prepared.', true);
    }
  }

  #assertLease(row, lease, operation) {
    expectString(lease.ownerId, 'ownerId', operation);
    expectInteger(lease.fence, 'fence', operation, 1);
    const now = this.#now(operation);
    if (Number(row.fence_token) !== lease.fence) {
      fail(operation, 'STALE_FENCE', 'This worker lease has been superseded.');
    }
    if (row.lease_owner !== lease.ownerId) {
      fail(operation, 'LEASE_REQUIRED', 'The current worker does not own this workflow lease.');
    }
    if (row.lease_expires_ms === null || Number(row.lease_expires_ms) <= now) {
      fail(operation, 'LEASE_EXPIRED', 'The worker lease has expired.');
    }
    return now;
  }

  #assertMutable(row, operation) {
    if (TERMINAL.has(row.state)) fail(operation, 'TERMINAL_STATE', 'The workflow is already terminal.');
  }

  #appendAtNewRevision(row, { eventId, type, fromState = row.state, toState = row.state, fence, atMs }) {
    const revision = Number(row.revision) + 1;
    this.#db.prepare(`
      UPDATE workflows
      SET revision = ?, state = ?, updated_at_ms = ?
      WHERE workflow_id = ? AND revision = ?
    `).run(revision, toState, atMs, row.workflow_id, Number(row.revision));
    return {
      revision,
      eventSequence: this.#appendEvent({
        eventId,
        workflowId: row.workflow_id,
        revision,
        type,
        fromState,
        toState,
        fence,
        atMs
      })
    };
  }

  #markExpiredIntents(row, now, idPrefix) {
    const attempts = this.#db.prepare(`
      SELECT attempt_id FROM operation_attempts
      WHERE workflow_id = ? AND state = 'intent'
      ORDER BY created_at_ms, attempt_id
    `).all(row.workflow_id);
    if (attempts.length === 0) return { row, changed: false };

    this.#db.prepare(`
      UPDATE operation_attempts
      SET state = 'outcome_unknown', updated_at_ms = ?
      WHERE workflow_id = ? AND state = 'intent'
    `).run(now, row.workflow_id);
    const toState = TERMINAL.has(row.state) ? row.state : 'reconciliation_pending';
    const updated = this.#appendAtNewRevision(row, {
      eventId: idPrefix + ':unknown:' + Number(row.revision + 1),
      type: 'expired_lease_outcome_unknown',
      toState,
      fence: Number(row.fence_token),
      atMs: now
    });
    let next = this.#workflow(row.workflow_id, 'markExpiredIntents');
    for (let index = 1; index < attempts.length; index += 1) {
      this.#appendEvent({
        eventId: idPrefix + ':unknown-extra:' + index + ':' + attempts[index].attempt_id,
        workflowId: row.workflow_id,
        revision: Number(next.revision),
        type: 'expired_lease_outcome_unknown',
        fromState: next.state,
        toState: next.state,
        fence: Number(next.fence_token),
        atMs: now
      });
    }
    return { row: next, changed: true, eventSequence: updated.eventSequence };
  }

  #storeArtifact(workflowId, artifactId, bytes, now, operation) {
    expectString(artifactId, 'artifactId', operation);
    if (!(bytes instanceof Uint8Array)) {
      fail(operation, 'INVALID_INPUT', 'Artifact content must be supplied as bytes.');
    }
    const copy = Buffer.from(bytes);
    const sha256 = digest(copy);
    const previous = this.#db.prepare(`
      SELECT sha256, bytes FROM immutable_artifacts WHERE workflow_id = ? AND artifact_id = ?
    `).get(workflowId, artifactId);
    if (previous) {
      if (previous.sha256 === sha256 && Buffer.from(previous.bytes).equals(copy)) {
        return { artifactId, sha256, duplicate: true };
      }
      fail(operation, 'ARTIFACT_IMMUTABLE', 'An artifact identifier cannot be reused for different bytes.');
    }
    this.#db.prepare(`
      INSERT INTO immutable_artifacts (workflow_id, artifact_id, sha256, bytes, created_at_ms)
      VALUES (?, ?, ?, ?, ?)
    `).run(workflowId, artifactId, sha256, copy, now);
    return { artifactId, sha256, duplicate: false };
  }

  createWorkflow({ workflowId, checkpoint = {}, eventId } = {}) {
    const operation = 'createWorkflow';
    expectString(workflowId, 'workflowId', operation);
    const payload = serializeCheckpoint(checkpoint, operation);
    const createdAtMs = this.#now(operation);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const identity = this.#eventId(eventId, operation);
      this.#assertEventAvailable(identity, operation);
      if (this.#db.prepare('SELECT 1 FROM workflows WHERE workflow_id = ?').get(workflowId)) {
        fail(operation, 'WORKFLOW_ALREADY_EXISTS', 'A workflow with this identity already exists.');
      }
      this.#db.prepare(`
        INSERT INTO workflows (
          workflow_id, state, revision, fence_token, lease_owner, lease_expires_ms,
          checkpoint_sequence, cancellation_requested, created_at_ms, updated_at_ms
        ) VALUES (?, 'queued', 0, 0, NULL, NULL, 1, 0, ?, ?)
      `).run(workflowId, createdAtMs, createdAtMs);
      this.#db.prepare(`
        INSERT INTO workflow_checkpoints (
          workflow_id, sequence, schema_version, payload_json, sha256, revision, created_at_ms
        ) VALUES (?, 1, ?, ?, ?, 0, ?)
      `).run(workflowId, CHECKPOINT_SCHEMA_VERSION, payload, digest(payload), createdAtMs);
      this.#appendEvent({
        eventId: identity,
        workflowId,
        revision: 0,
        type: 'workflow_created',
        fromState: null,
        toState: 'queued',
        fence: 0,
        atMs: createdAtMs
      });
      return { workflowId, state: 'queued', revision: 0, checkpointSequence: 1 };
    });
  }

  getWorkflow(workflowId) {
    const operation = 'getWorkflow';
    expectString(workflowId, 'workflowId', operation);
    this.#assertOpen(operation);
    try {
      return mapWorkflow(this.#workflow(workflowId, operation));
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  getCheckpoint(workflowId) {
    const operation = 'getCheckpoint';
    expectString(workflowId, 'workflowId', operation);
    this.#assertOpen(operation);
    try {
      const workflow = this.#workflow(workflowId, operation);
      const row = this.#db.prepare(`
        SELECT * FROM workflow_checkpoints WHERE workflow_id = ? AND sequence = ?
      `).get(workflowId, Number(workflow.checkpoint_sequence));
      if (!row) fail(operation, 'CHECKPOINT_CORRUPT', 'The latest workflow checkpoint is missing.');
      if (Number(row.schema_version) > CHECKPOINT_SCHEMA_VERSION) {
        fail(operation, 'UNSUPPORTED_SCHEMA_VERSION', 'The checkpoint schema is newer than this runtime supports.');
      }
      if (
        typeof row.payload_json !== 'string'
        || Buffer.byteLength(row.payload_json, 'utf8') > MAX_CHECKPOINT_SERIALIZED_BYTES
        || digest(row.payload_json) !== row.sha256
      ) {
        fail(operation, 'CHECKPOINT_CORRUPT', 'The latest workflow checkpoint failed its integrity check.');
      }
      let parsed;
      try {
        parsed = parseBoundedJson(row.payload_json, {
          maxBytes: MAX_CHECKPOINT_SERIALIZED_BYTES,
          maxDepth: MAX_CHECKPOINT_DEPTH + 4,
          maxNodes: MAX_CHECKPOINT_NODES * 2 + 8,
          allowUnsafeFiniteNumbers: true
        });
      } catch {
        fail(operation, 'CHECKPOINT_CORRUPT', 'The latest workflow checkpoint is invalid.');
      }
      if (parsed.schemaVersion !== CHECKPOINT_SCHEMA_VERSION || !isRecord(parsed.data)) {
        fail(operation, 'CHECKPOINT_CORRUPT', 'The latest workflow checkpoint is invalid.');
      }
      return {
        workflowId,
        sequence: Number(row.sequence),
        schemaVersion: Number(row.schema_version),
        data: parsed.data,
        sha256: row.sha256
      };
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  /** @internal The capability token is deliberately absent from the package API. */
  _localControllerNow(capability) {
    if (capability !== LOCAL_CONTROLLER_STORAGE_CAPABILITY) {
      fail('_localControllerNow', 'INVALID_INPUT', 'The private local controller capability is required.');
    }
    this.#assertOpen('_localControllerNow');
    return this.#now('_localControllerNow');
  }

  /** @internal Returns one consistent workflow/checkpoint/unknown-attempt snapshot. */
  _localControllerSnapshot(capability, workflowId) {
    const operation = '_localControllerSnapshot';
    if (capability !== LOCAL_CONTROLLER_STORAGE_CAPABILITY) {
      fail(operation, 'INVALID_INPUT', 'The private local controller capability is required.');
    }
    expectString(workflowId, 'workflowId', operation);
    this.#assertOpen(operation);
    return withConsistentReadSnapshot(this.#db, operation, () => {
      const workflow = mapWorkflow(this.#workflow(workflowId, operation));
      const checkpoint = this.getCheckpoint(workflowId);
      const unresolved = this.#db.prepare(`
        SELECT state FROM operation_attempts
        WHERE workflow_id = ? AND state IN ('intent','outcome_unknown','response_received')
        ORDER BY attempt_id LIMIT 33
      `).all(workflowId);
      return Object.freeze({
        workflow: Object.freeze(workflow),
        checkpoint: Object.freeze(checkpoint),
        externalAttemptStates: Object.freeze(unresolved.slice(0, 32).map(row => row.state)),
        externalAttemptsTruncated: unresolved.length > 32,
        hasUnresolvedExternalAttempt: unresolved.length > 0
      });
    });
  }

  /** @internal Classifies one exact local admission without writing or retrying. */
  _localControllerResolveAdmission(capability, input) {
    const operation = '_localControllerResolveAdmission';
    if (capability !== LOCAL_CONTROLLER_STORAGE_CAPABILITY) {
      fail(operation, 'INVALID_INPUT', 'The private local controller capability is required.');
    }
    const fields = readOwnDataRecord(input, [
      'workflowId', 'ownerId', 'fence', 'expectedRevision', 'expectedCheckpointSequence',
      'stepId', 'admissionKind', 'eventId', 'previousCheckpointSha256', 'controllerState'
    ], operation);
    expectString(fields.workflowId, 'workflowId', operation);
    expectString(fields.ownerId, 'ownerId', operation);
    expectInteger(fields.fence, 'fence', operation, 1);
    expectInteger(fields.expectedRevision, 'expectedRevision', operation);
    expectInteger(fields.expectedCheckpointSequence, 'expectedCheckpointSequence', operation, 1);
    if (!LOCAL_STEP_ID.test(fields.stepId) || !['step', 'replay'].includes(fields.admissionKind)) {
      fail(operation, 'INVALID_INPUT', 'The local step admission is invalid.');
    }
    expectString(fields.eventId, 'eventId', operation);
    if (typeof fields.previousCheckpointSha256 !== 'string'
        || !/^[0-9a-f]{64}$/u.test(fields.previousCheckpointSha256)) {
      fail(operation, 'INVALID_INPUT', 'The previous checkpoint identity is invalid.');
    }
    const attempted = readLocalControllerState(fields.controllerState, operation);
    if (
      attempted.workflowId !== fields.workflowId
      || attempted.pendingStepId !== fields.stepId
      || attempted.lastAdmissionKind !== fields.admissionKind
      || attempted.lastAdmissionStepId !== fields.stepId
    ) fail(operation, 'INVALID_INPUT', 'The local step admission candidate is invalid.');
    const attemptedPayload = serializeCheckpoint(attempted, operation);
    const attemptedHash = digest(attemptedPayload);
    const targetSequence = fields.expectedCheckpointSequence + 1;
    const targetRevision = fields.expectedRevision + 1;
    if (!Number.isSafeInteger(targetSequence) || !Number.isSafeInteger(targetRevision)) {
      fail(operation, 'INVALID_INPUT', 'The local admission version is invalid.');
    }
    this.#assertOpen(operation);
    if (this.#db.isTransaction !== false) {
      return Object.freeze({ kind: 'unknown', reason: 'transaction_active' });
    }

    return withConsistentReadSnapshot(this.#db, operation, () => {
      const workflowRow = this.#workflow(fields.workflowId, operation);
      const workflow = mapWorkflow(workflowRow);
      const checkpoint = this.getCheckpoint(fields.workflowId);
      const event = this.#db.prepare(`
        SELECT sequence, event_id, workflow_id, revision, event_type, from_state, to_state, fence_token
        FROM workflow_events WHERE event_id = ?
      `).get(fields.eventId);
      const outbox = this.#db.prepare(`
        SELECT event_id, event_sequence FROM outbox WHERE event_id = ?
      `).get(fields.eventId);
      const target = this.#db.prepare(`
        SELECT sequence, schema_version, payload_json, sha256, revision
        FROM workflow_checkpoints WHERE workflow_id = ? AND sequence = ?
      `).get(fields.workflowId, targetSequence);
      const unresolvedRows = this.#db.prepare(`
        SELECT state FROM operation_attempts
        WHERE workflow_id = ? AND state IN ('intent','outcome_unknown','response_received')
        ORDER BY attempt_id LIMIT 33
      `).all(fields.workflowId);
      const snapshot = Object.freeze({
        workflow: Object.freeze(workflow),
        checkpoint: Object.freeze(checkpoint),
        externalAttemptStates: Object.freeze(unresolvedRows.slice(0, 32).map(row => row.state)),
        externalAttemptsTruncated: unresolvedRows.length > 32,
        hasUnresolvedExternalAttempt: unresolvedRows.length > 0
      });

      const eventType = fields.admissionKind === 'replay'
        ? 'private_local_step_replayed'
        : 'private_local_step_admitted';
      const exactEvent = Boolean(event
        && Number(event.revision) === targetRevision
        && event.event_id === fields.eventId
        && event.workflow_id === fields.workflowId
        && event.event_type === eventType
        && event.from_state === 'running'
        && event.to_state === 'running'
        && Number(event.fence_token) === fields.fence);
      const exactOutbox = Boolean(outbox
        && outbox.event_id === fields.eventId
        && event
        && Number(outbox.event_sequence) === Number(event.sequence));
      const exactTarget = Boolean(target
        && Number(target.sequence) === targetSequence
        && Number(target.schema_version) === CHECKPOINT_SCHEMA_VERSION
        && Number(target.revision) === targetRevision
        && typeof target.payload_json === 'string'
        && Buffer.byteLength(target.payload_json, 'utf8') <= MAX_CHECKPOINT_SERIALIZED_BYTES
        && digest(target.payload_json) === target.sha256
        && target.sha256 === attemptedHash);

      if (exactEvent && exactOutbox && exactTarget) {
        return Object.freeze({
          kind: 'committed',
          workflowId: fields.workflowId,
          revision: targetRevision,
          checkpointSequence: targetSequence,
          eventSequence: Number(event.sequence),
          stepId: fields.stepId,
          admissionKind: fields.admissionKind,
          snapshot,
          now: this.#now(operation)
        });
      }
      if (event || outbox || target) {
        return Object.freeze({ kind: 'unknown', reason: 'partial_or_mismatched_commit' });
      }

      if (
        Number(workflow.revision) !== fields.expectedRevision
        || Number(workflow.checkpointSequence) !== fields.expectedCheckpointSequence
        || workflow.state !== 'running'
        || workflow.cancellationRequested
        || workflow.leaseOwner !== fields.ownerId
        || workflow.fence !== fields.fence
        || checkpoint.sequence !== fields.expectedCheckpointSequence
        || checkpoint.sha256 !== fields.previousCheckpointSha256
      ) return Object.freeze({ kind: 'unknown', reason: 'pending_state_changed' });

      let previous = null;
      if (isRecord(checkpoint.data) && checkpoint.data.kind === LOCAL_CHECKPOINT_KIND) {
        previous = readLocalControllerState(checkpoint.data, operation);
        assertPrivateCheckpointPins(previous, attempted, fields.workflowId, operation);
        if (!equalCheckpointData(previous, checkpoint.data, operation)) {
          return Object.freeze({ kind: 'unknown', reason: 'pending_checkpoint_changed' });
        }
      } else if (!equalCheckpointData(attempted.baseCheckpointData, checkpoint.data, operation)) {
        return Object.freeze({ kind: 'unknown', reason: 'pending_base_checkpoint_changed' });
      }
      const now = this.#now(operation);
      assertAdmissionBudget(previous, attempted, fields.admissionKind, fields.stepId, operation);
      if (workflow.leaseExpiresAtMs === null || workflow.leaseExpiresAtMs <= now) {
        return Object.freeze({ kind: 'unknown', reason: 'lease_expired' });
      }
      if (unresolvedRows.length > 0) {
        return Object.freeze({ kind: 'unknown', reason: 'external_outcome_unresolved' });
      }
      return Object.freeze({ kind: 'pending', snapshot, now });
    });
  }

  /** @internal Classifies one exact local completion without writing or retrying. */
  _localControllerResolveCompletion(capability, input) {
    const operation = '_localControllerResolveCompletion';
    if (capability !== LOCAL_CONTROLLER_STORAGE_CAPABILITY) {
      fail(operation, 'INVALID_INPUT', 'The private local controller capability is required.');
    }
    const fields = readOwnDataRecord(input, [
      'workflowId', 'ownerId', 'fence', 'expectedRevision', 'expectedCheckpointSequence',
      'stepId', 'eventId', 'previousCheckpointSha256', 'controllerState'
    ], operation);
    expectString(fields.workflowId, 'workflowId', operation);
    expectString(fields.ownerId, 'ownerId', operation);
    expectInteger(fields.fence, 'fence', operation, 1);
    expectInteger(fields.expectedRevision, 'expectedRevision', operation);
    expectInteger(fields.expectedCheckpointSequence, 'expectedCheckpointSequence', operation, 1);
    if (!LOCAL_STEP_ID.test(fields.stepId)) fail(operation, 'INVALID_INPUT', 'The local step identity is invalid.');
    expectString(fields.eventId, 'eventId', operation);
    if (typeof fields.previousCheckpointSha256 !== 'string'
        || !/^[0-9a-f]{64}$/u.test(fields.previousCheckpointSha256)) {
      fail(operation, 'INVALID_INPUT', 'The previous checkpoint identity is invalid.');
    }
    const attempted = readLocalControllerState(fields.controllerState, operation);
    if (
      attempted.workflowId !== fields.workflowId
      || attempted.pendingStepId !== null
      || attempted.lastAdmissionStepId !== fields.stepId
    ) fail(operation, 'INVALID_INPUT', 'The local completion candidate is invalid.');
    const attemptedPayload = serializeCheckpoint(attempted, operation);
    const attemptedHash = digest(attemptedPayload);
    const targetSequence = fields.expectedCheckpointSequence + 1;
    const targetRevision = fields.expectedRevision + 1;
    if (!Number.isSafeInteger(targetSequence) || !Number.isSafeInteger(targetRevision)) {
      fail(operation, 'INVALID_INPUT', 'The local completion version is invalid.');
    }
    this.#assertOpen(operation);
    if (this.#db.isTransaction !== false) {
      return Object.freeze({ kind: 'unknown', reason: 'transaction_active' });
    }

    return withConsistentReadSnapshot(this.#db, operation, () => {
      const workflowRow = this.#workflow(fields.workflowId, operation);
      const workflow = mapWorkflow(workflowRow);
      const checkpoint = this.getCheckpoint(fields.workflowId);
      const event = this.#db.prepare(`
        SELECT sequence, event_id, workflow_id, revision, event_type, from_state, to_state, fence_token
        FROM workflow_events WHERE event_id = ?
      `).get(fields.eventId);
      const outbox = this.#db.prepare(`
        SELECT event_id, event_sequence FROM outbox WHERE event_id = ?
      `).get(fields.eventId);
      const target = this.#db.prepare(`
        SELECT sequence, schema_version, payload_json, sha256, revision
        FROM workflow_checkpoints WHERE workflow_id = ? AND sequence = ?
      `).get(fields.workflowId, targetSequence);
      const unresolvedRows = this.#db.prepare(`
        SELECT state FROM operation_attempts
        WHERE workflow_id = ? AND state IN ('intent','outcome_unknown','response_received')
        ORDER BY attempt_id LIMIT 33
      `).all(fields.workflowId);
      const snapshot = Object.freeze({
        workflow: Object.freeze(workflow),
        checkpoint: Object.freeze(checkpoint),
        externalAttemptStates: Object.freeze(unresolvedRows.slice(0, 32).map(row => row.state)),
        externalAttemptsTruncated: unresolvedRows.length > 32,
        hasUnresolvedExternalAttempt: unresolvedRows.length > 0
      });

      const exactEvent = Boolean(event
        && Number(event.revision) === targetRevision
        && event.event_id === fields.eventId
        && event.workflow_id === fields.workflowId
        && event.event_type === 'private_local_step_completed'
        && event.from_state === 'running'
        && event.to_state === 'running'
        && Number(event.fence_token) === fields.fence);
      const exactOutbox = Boolean(outbox
        && outbox.event_id === fields.eventId
        && event
        && Number(outbox.event_sequence) === Number(event.sequence));
      const exactTarget = Boolean(target
        && Number(target.sequence) === targetSequence
        && Number(target.schema_version) === CHECKPOINT_SCHEMA_VERSION
        && Number(target.revision) === targetRevision
        && typeof target.payload_json === 'string'
        && Buffer.byteLength(target.payload_json, 'utf8') <= MAX_CHECKPOINT_SERIALIZED_BYTES
        && digest(target.payload_json) === target.sha256
        && target.sha256 === attemptedHash);

      if (exactEvent && exactOutbox && exactTarget) {
        return Object.freeze({
          kind: 'committed',
          workflowId: fields.workflowId,
          revision: targetRevision,
          checkpointSequence: targetSequence,
          eventSequence: Number(event.sequence),
          stepId: fields.stepId,
          snapshot,
          now: this.#now(operation)
        });
      }
      if (event || outbox || target) {
        return Object.freeze({ kind: 'unknown', reason: 'partial_or_mismatched_commit' });
      }

      if (
        Number(workflow.revision) !== fields.expectedRevision
        || Number(workflow.checkpointSequence) !== fields.expectedCheckpointSequence
        || workflow.state !== 'running'
        || workflow.cancellationRequested
        || workflow.leaseOwner !== fields.ownerId
        || workflow.fence !== fields.fence
        || checkpoint.sequence !== fields.expectedCheckpointSequence
        || checkpoint.sha256 !== fields.previousCheckpointSha256
      ) return Object.freeze({ kind: 'unknown', reason: 'pending_state_changed' });

      const previous = readLocalControllerState(checkpoint.data, operation);
      assertPrivateCheckpointPins(previous, attempted, fields.workflowId, operation);
      assertCompletionBudget(previous, attempted, fields.stepId, operation);
      const now = this.#now(operation);
      if (workflow.leaseExpiresAtMs === null || workflow.leaseExpiresAtMs <= now) {
        return Object.freeze({ kind: 'unknown', reason: 'lease_expired' });
      }
      if (unresolvedRows.length > 0) {
        return Object.freeze({ kind: 'unknown', reason: 'external_outcome_unresolved' });
      }
      return Object.freeze({ kind: 'pending', snapshot, now });
    });
  }

  /** @internal Persists the allowance before a reviewed pure local step runs. */
  _localControllerAdmit(capability, input) {
    const operation = '_localControllerAdmit';
    if (capability !== LOCAL_CONTROLLER_STORAGE_CAPABILITY) {
      fail(operation, 'INVALID_INPUT', 'The private local controller capability is required.');
    }
    const fields = readOwnDataRecord(input, [
      'workflowId', 'ownerId', 'fence', 'expectedRevision', 'expectedCheckpointSequence',
      'stepId', 'admissionKind', 'eventId', 'controllerState'
    ], operation);
    expectString(fields.workflowId, 'workflowId', operation);
    expectString(fields.ownerId, 'ownerId', operation);
    expectInteger(fields.fence, 'fence', operation, 1);
    expectInteger(fields.expectedRevision, 'expectedRevision', operation);
    expectInteger(fields.expectedCheckpointSequence, 'expectedCheckpointSequence', operation, 1);
    if (!LOCAL_STEP_ID.test(fields.stepId) || !['step', 'replay'].includes(fields.admissionKind)) {
      fail(operation, 'INVALID_INPUT', 'The local step admission is invalid.');
    }
    expectString(fields.eventId, 'eventId', operation);
    const next = readLocalControllerState(fields.controllerState, operation);
    if (next.workflowId !== fields.workflowId) {
      fail(operation, 'CHECKPOINT_CORRUPT', 'The private local checkpoint belongs to another workflow.');
    }
    const payload = serializeCheckpoint(next, operation);
    const payloadHash = digest(payload);
    this.#assertOpen(operation);
    return inLocalAdmissionTransaction(this.#db, operation, () => {
      const row = this.#workflow(fields.workflowId, operation);
      const now = this.#assertLease(row, { ownerId: fields.ownerId, fence: fields.fence }, operation);
      this.#assertMutable(row, operation);
      this.#assertRevision(row, fields.expectedRevision, operation);
      if (row.state !== 'running' || row.cancellation_requested) {
        fail(operation, 'ILLEGAL_TRANSITION', 'A local step requires an active uncancelled workflow.');
      }
      if (Number(row.checkpoint_sequence) !== fields.expectedCheckpointSequence) {
        fail(operation, 'CONCURRENT_MODIFICATION', 'The workflow checkpoint changed since this operation was prepared.', true);
      }
      if (this.#db.prepare(`
        SELECT 1 FROM operation_attempts
        WHERE workflow_id = ? AND state IN ('intent','outcome_unknown','response_received') LIMIT 1
      `).get(fields.workflowId)) {
        fail(operation, 'OUTCOME_UNRESOLVED', 'An external outcome must be reconciled before local execution.');
      }
      const current = this.getCheckpoint(fields.workflowId);
      let previous = null;
      if (isRecord(current.data) && current.data.kind === LOCAL_CHECKPOINT_KIND) {
        previous = readLocalControllerState(current.data, operation);
        assertPrivateCheckpointPins(previous, next, fields.workflowId, operation);
        if (!equalCheckpointData(previous, current.data, operation)) {
          fail(operation, 'CHECKPOINT_CORRUPT', 'The private local checkpoint changed while being read.');
        }
      } else if (!equalCheckpointData(next.baseCheckpointData, current.data, operation)) {
        fail(operation, 'CHECKPOINT_CORRUPT', 'The original workflow checkpoint cannot be rebound.');
      }
      assertLocalControllerTransactionClock(now, next, row, previous, operation);
      assertAdmissionBudget(previous, next, fields.admissionKind, fields.stepId, operation);

      const eventType = fields.admissionKind === 'replay'
        ? 'private_local_step_replayed'
        : 'private_local_step_admitted';
      this.#assertEventAvailable(fields.eventId, operation);
      consumeOperationsStorageReservation(
        this.#db, this.#databasePath, this.#operationsResourceCeilings,
        fields.workflowId, operationsWriteCharge(Buffer.byteLength(payload, 'utf8')), operation,
        OPS_GENERATION_WRITE_RESERVE_BYTES
      );
      const revision = Number(row.revision) + 1;
      const sequence = Number(row.checkpoint_sequence) + 1;
      const update = this.#db.prepare(`
        UPDATE workflows
        SET revision = ?, checkpoint_sequence = ?, updated_at_ms = ?
        WHERE workflow_id = ? AND revision = ? AND checkpoint_sequence = ?
          AND state = 'running' AND cancellation_requested = 0
          AND fence_token = ? AND lease_owner = ? AND lease_expires_ms > ?
      `).run(
        revision, sequence, now, fields.workflowId, fields.expectedRevision,
        fields.expectedCheckpointSequence, fields.fence, fields.ownerId, now
      );
      if (Number(update.changes) !== 1) {
        fail(operation, 'CONCURRENT_MODIFICATION', 'The workflow changed before local admission committed.', true);
      }
      this.#db.prepare(`
        INSERT INTO workflow_checkpoints (
          workflow_id, sequence, schema_version, payload_json, sha256, revision, created_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
      `).run(
        fields.workflowId, sequence, CHECKPOINT_SCHEMA_VERSION, payload, payloadHash, revision, now
      );
      const eventSequence = this.#appendEvent({
        eventId: fields.eventId,
        workflowId: fields.workflowId,
        revision,
        type: eventType,
        fromState: row.state,
        toState: row.state,
        fence: fields.fence,
        atMs: now
      });
      return Object.freeze({
        workflowId: fields.workflowId,
        revision,
        checkpointSequence: sequence,
        eventSequence,
        stepId: fields.stepId,
        admissionKind: fields.admissionKind
      });
    });
  }

  /** @internal Atomically commits one validated local result and its checkpoint event. */
  _localControllerComplete(capability, input) {
    const operation = '_localControllerComplete';
    if (capability !== LOCAL_CONTROLLER_STORAGE_CAPABILITY) {
      fail(operation, 'INVALID_INPUT', 'The private local controller capability is required.');
    }
    const fields = readOwnDataRecord(input, [
      'workflowId', 'ownerId', 'fence', 'expectedRevision', 'expectedCheckpointSequence',
      'stepId', 'eventId', 'controllerState'
    ], operation);
    const clockSample = LOCAL_COMPLETION_CLOCK_SAMPLES.get(fields.controllerState) ?? null;
    if (clockSample !== null) LOCAL_COMPLETION_CLOCK_SAMPLES.delete(fields.controllerState);
    expectString(fields.workflowId, 'workflowId', operation);
    expectString(fields.ownerId, 'ownerId', operation);
    expectInteger(fields.fence, 'fence', operation, 1);
    expectInteger(fields.expectedRevision, 'expectedRevision', operation);
    expectInteger(fields.expectedCheckpointSequence, 'expectedCheckpointSequence', operation, 1);
    if (!LOCAL_STEP_ID.test(fields.stepId)) fail(operation, 'INVALID_INPUT', 'The local step identity is invalid.');
    expectString(fields.eventId, 'eventId', operation);
    const next = readLocalControllerState(fields.controllerState, operation);
    if (clockSample !== null && clockSample.wallNowMs !== next.budget.lastWallMs) {
      fail(operation, 'INVALID_INPUT', 'The private completion clock sample does not match its checkpoint.');
    }
    if (next.workflowId !== fields.workflowId) {
      fail(operation, 'CHECKPOINT_CORRUPT', 'The private local checkpoint belongs to another workflow.');
    }
    const payload = serializeCheckpoint(next, operation);
    const payloadHash = digest(payload);
    this.#assertOpen(operation);
    return inLocalCompletionTransaction(this.#db, operation, () => {
      const row = this.#workflow(fields.workflowId, operation);
      const now = this.#assertLease(row, { ownerId: fields.ownerId, fence: fields.fence }, operation);
      let transactionElapsedMs = null;
      if (clockSample !== null) {
        const transactionNowNs = process.hrtime.bigint();
        if (transactionNowNs < clockSample.monotonicNowNs) {
          fail(operation, 'INVALID_INPUT', 'The private completion clock sample is outside its supported boundary.');
        }
        transactionElapsedMs = Number(transactionNowNs - clockSample.monotonicNowNs) / 1_000_000;
        if (!Number.isFinite(transactionElapsedMs)) {
          fail(operation, 'INVALID_INPUT', 'The private completion clock sample is outside its supported boundary.');
        }
      }
      this.#assertMutable(row, operation);
      this.#assertRevision(row, fields.expectedRevision, operation);
      if (row.state !== 'running' || row.cancellation_requested) {
        fail(operation, 'ILLEGAL_TRANSITION', 'A local step requires an active uncancelled workflow.');
      }
      if (Number(row.checkpoint_sequence) !== fields.expectedCheckpointSequence) {
        fail(operation, 'CONCURRENT_MODIFICATION', 'The workflow checkpoint changed since this operation was prepared.', true);
      }
      if (this.#db.prepare(`
        SELECT 1 FROM operation_attempts
        WHERE workflow_id = ? AND state IN ('intent','outcome_unknown','response_received') LIMIT 1
      `).get(fields.workflowId)) {
        fail(operation, 'OUTCOME_UNRESOLVED', 'An external outcome must be reconciled before local execution.');
      }
      const current = this.getCheckpoint(fields.workflowId);
      if (!isRecord(current.data) || current.data.kind !== LOCAL_CHECKPOINT_KIND) {
        fail(operation, 'CHECKPOINT_CORRUPT', 'The local completion has no private admission checkpoint.');
      }
      const previous = readLocalControllerState(current.data, operation);
      assertPrivateCheckpointPins(previous, next, fields.workflowId, operation);
      if (!equalCheckpointData(previous, current.data, operation)) {
        fail(operation, 'CHECKPOINT_CORRUPT', 'The private local checkpoint changed while being read.');
      }
      assertLocalControllerTransactionClock(now, next, row, previous, operation, transactionElapsedMs);
      assertCompletionBudget(previous, next, fields.stepId, operation);

      this.#assertEventAvailable(fields.eventId, operation);
      consumeOperationsStorageReservation(
        this.#db, this.#databasePath, this.#operationsResourceCeilings,
        fields.workflowId, operationsWriteCharge(Buffer.byteLength(payload, 'utf8')), operation,
        OPS_GENERATION_WRITE_RESERVE_BYTES
      );
      const revision = Number(row.revision) + 1;
      const sequence = Number(row.checkpoint_sequence) + 1;
      const update = this.#db.prepare(`
        UPDATE workflows
        SET revision = ?, checkpoint_sequence = ?, updated_at_ms = ?
        WHERE workflow_id = ? AND revision = ? AND checkpoint_sequence = ?
          AND state = 'running' AND cancellation_requested = 0
          AND fence_token = ? AND lease_owner = ? AND lease_expires_ms > ?
      `).run(
        revision, sequence, now, fields.workflowId, fields.expectedRevision,
        fields.expectedCheckpointSequence, fields.fence, fields.ownerId, now
      );
      if (Number(update.changes) !== 1) {
        fail(operation, 'CONCURRENT_MODIFICATION', 'The workflow changed before local completion committed.', true);
      }
      this.#db.prepare(`
        INSERT INTO workflow_checkpoints (
          workflow_id, sequence, schema_version, payload_json, sha256, revision, created_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
      `).run(
        fields.workflowId, sequence, CHECKPOINT_SCHEMA_VERSION, payload, payloadHash, revision, now
      );
      const eventSequence = this.#appendEvent({
        eventId: fields.eventId,
        workflowId: fields.workflowId,
        revision,
        type: 'private_local_step_completed',
        fromState: row.state,
        toState: row.state,
        fence: fields.fence,
        atMs: now
      });
      return Object.freeze({
        workflowId: fields.workflowId,
        revision,
        checkpointSequence: sequence,
        eventSequence,
        stepId: fields.stepId
      });
    });
  }

  /** @internal Bounded keyset page; the checkpoint/page state stays read-only. */
  _localControllerListAttempts(capability, input) {
    const operation = '_localControllerListAttempts';
    if (capability !== LOCAL_CONTROLLER_STORAGE_CAPABILITY) {
      fail(operation, 'INVALID_INPUT', 'The private local controller capability is required.');
    }
    const fields = readOwnDataRecord(input, [
      'workflowId', 'afterAttemptId', 'expectedRevision', 'limit'
    ], operation);
    expectString(fields.workflowId, 'workflowId', operation);
    const afterAttemptId = fields.afterAttemptId ?? null;
    if (afterAttemptId !== null) expectString(afterAttemptId, 'afterAttemptId', operation);
    const expectedRevision = fields.expectedRevision ?? null;
    if (expectedRevision !== null) expectInteger(expectedRevision, 'expectedRevision', operation);
    const limit = fields.limit === undefined ? 32 : fields.limit;
    expectInteger(limit, 'limit', operation, 1);
    if (limit > 100) fail(operation, 'INVALID_INPUT', 'The recovery page exceeds its supported size.');
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const row = this.#workflow(fields.workflowId, operation);
      if (expectedRevision !== null) this.#assertRevision(row, expectedRevision, operation);
      if (afterAttemptId !== null && expectedRevision === null) {
        fail(operation, 'INVALID_INPUT', 'A recovery cursor requires its pinned workflow revision.');
      }
      const rows = this.#db.prepare(`
        SELECT a.attempt_id, a.logical_operation_ref, a.state, a.created_at_ms, a.updated_at_ms,
               r.receipt_id, r.outcome, r.response_sha256
        FROM operation_attempts a
        LEFT JOIN response_receipts r ON r.attempt_id = a.attempt_id
        WHERE a.workflow_id = ? AND (? IS NULL OR a.attempt_id > ?)
        ORDER BY a.attempt_id COLLATE BINARY
        LIMIT ?
      `).all(fields.workflowId, afterAttemptId, afterAttemptId, limit + 1);
      const hasMore = rows.length > limit;
      const items = rows.slice(0, limit).map(attempt => Object.freeze({
        workflowId: fields.workflowId,
        attemptId: attempt.attempt_id,
        logicalOperationRef: attempt.logical_operation_ref,
        state: attempt.state,
        createdAtMs: Number(attempt.created_at_ms),
        updatedAtMs: Number(attempt.updated_at_ms),
        receipt: attempt.receipt_id === null ? null : Object.freeze({
          receiptId: attempt.receipt_id,
          outcome: attempt.outcome,
          responseSha256: attempt.response_sha256
        })
      }));
      return Object.freeze({
        workflowId: fields.workflowId,
        workflowRevision: Number(row.revision),
        items: Object.freeze(items),
        hasMore,
        incomplete: hasMore,
        nextAfterAttemptId: hasMore && items.length > 0 ? items.at(-1).attemptId : null
      });
    });
  }

  acquireLease({ workflowId, ownerId, ttlMs, eventId } = {}) {
    const operation = 'acquireLease';
    expectString(workflowId, 'workflowId', operation);
    expectString(ownerId, 'ownerId', operation);
    expectInteger(ttlMs, 'ttlMs', operation, 1);
    if (ttlMs > 86_400_000) fail(operation, 'INVALID_INPUT', 'The worker lease exceeds its supported duration.');
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const row = this.#workflow(workflowId, operation);
      this.#assertMutable(row, operation);
      const now = this.#now(operation);
      if (row.lease_owner !== null && Number(row.lease_expires_ms) > now) {
        fail(operation, 'LEASE_HELD', 'Another worker currently owns this workflow lease.', true);
      }
      const prefix = eventId === undefined ? 'lease:' + this.#idFactory() : expectString(eventId, 'eventId', operation);
      const expired = this.#markExpiredIntents(row, now, prefix);
      const current = expired.row;
      const acquiredEventId = prefix + ':acquired';
      this.#assertEventAvailable(acquiredEventId, operation);
      const fence = Number(current.fence_token) + 1;
      const expiresAtMs = now + ttlMs;
      if (!Number.isSafeInteger(expiresAtMs)) fail(operation, 'INVALID_INPUT', 'The worker lease expiry is outside its supported range.');
      this.#db.prepare(`
        UPDATE workflows SET fence_token = ?, lease_owner = ?, lease_expires_ms = ?, updated_at_ms = ?
        WHERE workflow_id = ?
      `).run(fence, ownerId, expiresAtMs, now, workflowId);
      this.#appendEvent({
        eventId: acquiredEventId,
        workflowId,
        revision: Number(current.revision),
        type: 'lease_acquired',
        fromState: current.state,
        toState: current.state,
        fence,
        atMs: now
      });
      return { workflowId, ownerId, fence, expiresAtMs, revision: Number(current.revision) };
    });
  }

  renewLease({ workflowId, ownerId, fence, ttlMs, eventId } = {}) {
    const operation = 'renewLease';
    expectString(workflowId, 'workflowId', operation);
    expectInteger(ttlMs, 'ttlMs', operation, 1);
    if (ttlMs > 86_400_000) fail(operation, 'INVALID_INPUT', 'The worker lease exceeds its supported duration.');
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const row = this.#workflow(workflowId, operation);
      const now = this.#assertLease(row, { ownerId, fence }, operation);
      this.#assertMutable(row, operation);
      const identity = this.#eventId(eventId, operation);
      this.#assertEventAvailable(identity, operation);
      const expiresAtMs = now + ttlMs;
      if (!Number.isSafeInteger(expiresAtMs)) fail(operation, 'INVALID_INPUT', 'The worker lease expiry is outside its supported range.');
      this.#db.prepare('UPDATE workflows SET lease_expires_ms = ?, updated_at_ms = ? WHERE workflow_id = ?')
        .run(expiresAtMs, now, workflowId);
      this.#appendEvent({
        eventId: identity,
        workflowId,
        revision: Number(row.revision),
        type: 'lease_renewed',
        fromState: row.state,
        toState: row.state,
        fence,
        atMs: now
      });
      return { workflowId, ownerId, fence, expiresAtMs, revision: Number(row.revision) };
    });
  }

  releaseLease({ workflowId, ownerId, fence, eventId } = {}) {
    const operation = 'releaseLease';
    expectString(workflowId, 'workflowId', operation);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      let row = this.#workflow(workflowId, operation);
      const now = this.#assertLease(row, { ownerId, fence }, operation);
      const prefix = eventId === undefined ? 'release:' + this.#idFactory() : expectString(eventId, 'eventId', operation);
      const unresolved = this.#db.prepare(`
        SELECT attempt_id, state FROM operation_attempts
        WHERE workflow_id = ? AND state IN ('intent','outcome_unknown','response_received')
        ORDER BY created_at_ms, attempt_id
      `).all(workflowId);
      const intents = unresolved.filter(attempt => attempt.state === 'intent');
      if (intents.length > 0) {
        this.#db.prepare(`
          UPDATE operation_attempts SET state = 'outcome_unknown', updated_at_ms = ?
          WHERE workflow_id = ? AND state = 'intent'
        `).run(now, workflowId);
      }
      if (unresolved.length > 0 && !TERMINAL.has(row.state) && row.state !== 'reconciliation_pending') {
        const stateEventId = prefix + ':pending';
        this.#assertEventAvailable(stateEventId, operation);
        this.#appendAtNewRevision(row, {
          eventId: stateEventId,
          type: 'lease_released_with_unresolved_work',
          toState: 'reconciliation_pending',
          fence,
          atMs: now
        });
        row = this.#workflow(workflowId, operation);
      } else if (intents.length > 0) {
        const stateEventId = prefix + ':unknown';
        this.#assertEventAvailable(stateEventId, operation);
        this.#appendAtNewRevision(row, {
          eventId: stateEventId,
          type: 'lease_released_with_unknown_outcome',
          fence,
          atMs: now
        });
        row = this.#workflow(workflowId, operation);
      }
      const releaseEventId = prefix + ':released';
      this.#assertEventAvailable(releaseEventId, operation);
      this.#db.prepare(`
        UPDATE workflows SET lease_owner = NULL, lease_expires_ms = NULL, updated_at_ms = ?
        WHERE workflow_id = ?
      `).run(now, workflowId);
      this.#appendEvent({
        eventId: releaseEventId,
        workflowId,
        revision: Number(row.revision),
        type: 'lease_released',
        fromState: row.state,
        toState: row.state,
        fence,
        atMs: now
      });
      return { released: true, state: row.state };
    });
  }

  transitionWorkflow({ workflowId, ownerId, fence, expectedRevision, toState, eventId } = {}) {
    const operation = 'transitionWorkflow';
    expectString(workflowId, 'workflowId', operation);
    if (!STATES.has(toState) || toState === 'cancelled') {
      fail(operation, 'ILLEGAL_TRANSITION', 'The requested state transition is not available.');
    }
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const row = this.#workflow(workflowId, operation);
      const now = this.#assertLease(row, { ownerId, fence }, operation);
      this.#assertMutable(row, operation);
      this.#assertRevision(row, expectedRevision, operation);
      if (row.cancellation_requested && toState === 'running') {
        fail(operation, 'ILLEGAL_TRANSITION', 'A cancellation request cannot return to active execution.');
      }
      if (!TRANSITIONS.get(row.state).has(toState)) {
        fail(operation, 'ILLEGAL_TRANSITION', 'The requested state transition is not allowed.');
      }
      const outstanding = this.#db.prepare(`
          SELECT 1 FROM operation_attempts
          WHERE workflow_id = ? AND state IN ('intent','outcome_unknown','response_received')
          LIMIT 1
        `).get(workflowId);
      if (outstanding && toState !== 'reconciliation_pending') {
        fail(operation, 'OUTCOME_UNRESOLVED', 'An external outcome must remain in reconciliation until resolved.');
      }
      const identity = this.#eventId(eventId, operation);
      this.#assertEventAvailable(identity, operation);
      const revision = Number(row.revision) + 1;
      this.#db.prepare(`
        UPDATE workflows SET state = ?, revision = ?, updated_at_ms = ?
        WHERE workflow_id = ? AND revision = ?
      `).run(toState, revision, now, workflowId, Number(row.revision));
      const eventSequence = this.#appendEvent({
        eventId: identity,
        workflowId,
        revision,
        type: 'state_transition',
        fromState: row.state,
        toState,
        fence,
        atMs: now
      });
      return { state: toState, revision, eventSequence };
    });
  }

  requestCancellation({ workflowId, ownerId, fence, expectedRevision, eventId } = {}) {
    const operation = 'requestCancellation';
    expectString(workflowId, 'workflowId', operation);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const row = this.#workflow(workflowId, operation);
      if (TERMINAL.has(row.state)) {
        return { accepted: false, state: row.state, revision: Number(row.revision) };
      }
      this.#assertLease(row, { ownerId, fence }, operation);
      if (row.cancellation_requested) {
        return { accepted: true, state: row.state, revision: Number(row.revision) };
      }
      this.#assertRevision(row, expectedRevision, operation);
      const now = this.#now(operation);
      const identity = this.#eventId(eventId, operation);
      this.#assertEventAvailable(identity, operation);
      const unresolved = this.#db.prepare(`
        SELECT COUNT(*) AS count FROM operation_attempts
        WHERE workflow_id = ? AND state IN ('intent','outcome_unknown','response_received')
      `).get(workflowId);
      const nextState = unresolved.count > 0 ? 'reconciliation_pending' : 'cancelled';
      const revision = Number(row.revision) + 1;
      this.#db.prepare(`
        UPDATE workflows
        SET state = ?, cancellation_requested = 1, revision = ?, updated_at_ms = ?,
            lease_owner = CASE WHEN ? = 'cancelled' THEN NULL ELSE lease_owner END,
            lease_expires_ms = CASE WHEN ? = 'cancelled' THEN NULL ELSE lease_expires_ms END
        WHERE workflow_id = ? AND revision = ?
      `).run(nextState, revision, now, nextState, nextState, workflowId, Number(row.revision));
      const eventSequence = this.#appendEvent({
        eventId: identity,
        workflowId,
        revision,
        type: unresolved.count > 0 ? 'cancellation_requested_unresolved' : 'workflow_cancelled',
        fromState: row.state,
        toState: nextState,
        fence,
        atMs: now
      });
      return { accepted: true, state: nextState, revision, eventSequence };
    });
  }

  recordDispatchIntent({
    workflowId,
    ownerId,
    fence,
    expectedRevision,
    attemptId,
    logicalOperationRef,
    authorityHoldRef,
    transportAttemptId,
    eventId
  } = {}) {
    const operation = 'recordDispatchIntent';
    expectString(workflowId, 'workflowId', operation);
    expectString(attemptId, 'attemptId', operation);
    expectString(logicalOperationRef, 'logicalOperationRef', operation);
    expectString(authorityHoldRef, 'authorityHoldRef', operation);
    expectString(transportAttemptId, 'transportAttemptId', operation);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const row = this.#workflow(workflowId, operation);
      const now = this.#assertLease(row, { ownerId, fence }, operation);
      this.#assertMutable(row, operation);
      this.#assertRevision(row, expectedRevision, operation);
      const unresolved = this.#db.prepare(`
        SELECT 1 FROM operation_attempts
        WHERE workflow_id = ? AND state IN ('intent','outcome_unknown','response_received')
        LIMIT 1
      `).get(workflowId);
      if (unresolved) fail(operation, 'OUTCOME_UNRESOLVED', 'An earlier external outcome must be reconciled before another intent.');
      const completedOperation = this.#db.prepare(`
        SELECT 1
        FROM operation_attempts a
        JOIN response_receipts r ON r.attempt_id = a.attempt_id
        WHERE a.workflow_id = ? AND a.logical_operation_ref = ? AND r.outcome = 'completed'
        LIMIT 1
      `).get(workflowId, logicalOperationRef);
      if (completedOperation) {
        fail(operation, 'LOGICAL_OPERATION_COMPLETED', 'This logical operation already has a completed durable response.');
      }
      if (row.state !== 'running' || row.cancellation_requested) {
        fail(operation, 'ILLEGAL_TRANSITION', 'External intent can only be recorded for an active uncancelled workflow.');
      }
      if (this.#db.prepare('SELECT 1 FROM operation_attempts WHERE attempt_id = ?').get(attemptId)) {
        fail(operation, 'ATTEMPT_ALREADY_EXISTS', 'This billable-attempt identity has already been recorded.');
      }
      if (this.#db.prepare('SELECT 1 FROM transport_attempts WHERE transport_attempt_id = ?').get(transportAttemptId)) {
        fail(operation, 'ATTEMPT_ALREADY_EXISTS', 'This transport-attempt identity has already been recorded.');
      }
      const identity = this.#eventId(eventId, operation);
      this.#assertEventAvailable(identity, operation);
      this.#db.prepare(`
        INSERT INTO operation_attempts (
          attempt_id, workflow_id, logical_operation_ref, authority_hold_ref, state, created_at_ms, updated_at_ms
        ) VALUES (?, ?, ?, ?, 'intent', ?, ?)
      `).run(attemptId, workflowId, logicalOperationRef, authorityHoldRef, now, now);
      this.#db.prepare(`
        INSERT INTO transport_attempts (transport_attempt_id, attempt_id, created_at_ms)
        VALUES (?, ?, ?)
      `).run(transportAttemptId, attemptId, now);
      const revision = Number(row.revision) + 1;
      this.#db.prepare("UPDATE workflows SET state = 'awaiting_response', revision = ?, updated_at_ms = ? WHERE workflow_id = ?")
        .run(revision, now, workflowId);
      const eventSequence = this.#appendEvent({
        eventId: identity,
        workflowId,
        revision,
        type: 'dispatch_intent_persisted',
        fromState: row.state,
        toState: 'awaiting_response',
        fence,
        atMs: now
      });
      return { attemptId, state: 'awaiting_response', revision, eventSequence };
    });
  }

  recordTransportAttempt({ workflowId, ownerId, fence, expectedRevision, attemptId, transportAttemptId, eventId } = {}) {
    const operation = 'recordTransportAttempt';
    expectString(workflowId, 'workflowId', operation);
    expectString(attemptId, 'attemptId', operation);
    expectString(transportAttemptId, 'transportAttemptId', operation);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const row = this.#workflow(workflowId, operation);
      const now = this.#assertLease(row, { ownerId, fence }, operation);
      this.#assertMutable(row, operation);
      this.#assertRevision(row, expectedRevision, operation);
      if (row.cancellation_requested || row.state !== 'awaiting_response') {
        fail(operation, 'ATTEMPT_STATE_INVALID', 'A cancelled or inactive workflow cannot record a new transport retry.');
      }
      const attempt = this.#db.prepare('SELECT * FROM operation_attempts WHERE attempt_id = ?').get(attemptId);
      if (!attempt || attempt.workflow_id !== workflowId) fail(operation, 'ATTEMPT_NOT_FOUND', 'The operation attempt does not exist.');
      if (attempt.state !== 'intent') fail(operation, 'ATTEMPT_STATE_INVALID', 'The operation attempt is not open for transport retries.');
      const previous = this.#db.prepare('SELECT attempt_id FROM transport_attempts WHERE transport_attempt_id = ?')
        .get(transportAttemptId);
      if (previous) {
        if (previous.attempt_id === attemptId) {
          return { transportAttemptId, revision: Number(row.revision), eventSequence: null, duplicate: true };
        }
        fail(operation, 'ATTEMPT_ALREADY_EXISTS', 'This transport-attempt identity has already been recorded.');
      }
      const identity = this.#eventId(eventId, operation);
      this.#assertEventAvailable(identity, operation);
      this.#db.prepare('INSERT INTO transport_attempts (transport_attempt_id, attempt_id, created_at_ms) VALUES (?, ?, ?)')
        .run(transportAttemptId, attemptId, now);
      const revision = Number(row.revision) + 1;
      this.#db.prepare('UPDATE workflows SET revision = ?, updated_at_ms = ? WHERE workflow_id = ?')
        .run(revision, now, workflowId);
      const eventSequence = this.#appendEvent({
        eventId: identity,
        workflowId,
        revision,
        type: 'transport_attempt_persisted',
        fromState: row.state,
        toState: row.state,
        fence,
        atMs: now
      });
      return { transportAttemptId, revision, eventSequence, duplicate: false };
    });
  }

  recordJevDispatchMarker(input) {
    const operation = 'recordJevDispatchMarker';
    const fields = readOwnDataRecord(input, [
      'workflowId', 'ownerId', 'fence', 'expectedRevision', 'attemptId', 'transportAttemptId'
    ], operation);
    for (const key of ['workflowId', 'attemptId', 'transportAttemptId']) {
      if (typeof fields[key] !== 'string' || !SAFE_OPAQUE_REF.test(fields[key])) {
        fail(operation, 'INVALID_INPUT', 'A provider dispatch identity is invalid.');
      }
    }
    if (typeof fields.ownerId !== 'string' || !SAFE_OPAQUE_REF.test(fields.ownerId)) {
      fail(operation, 'INVALID_INPUT', 'A provider dispatch identity is invalid.');
    }
    expectInteger(fields.fence, 'fence', operation, 1);
    expectInteger(fields.expectedRevision, 'expectedRevision', operation);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const row = this.#workflow(fields.workflowId, operation);
      const now = this.#assertLease(row, { ownerId: fields.ownerId, fence: fields.fence }, operation);
      this.#assertMutable(row, operation);
      this.#assertRevision(row, fields.expectedRevision, operation);
      if (row.state !== 'awaiting_response' || row.cancellation_requested) {
        fail(operation, 'ATTEMPT_STATE_INVALID', 'A cancelled or inactive workflow cannot start a provider transport.');
      }
      const attempt = this.#db.prepare(`
        SELECT a.attempt_id, a.workflow_id, a.logical_operation_ref, a.state,
               t.transport_attempt_id
        FROM operation_attempts a
        JOIN transport_attempts t ON t.attempt_id = a.attempt_id
        WHERE a.attempt_id = ? AND a.workflow_id = ? AND t.transport_attempt_id = ?
      `).get(fields.attemptId, fields.workflowId, fields.transportAttemptId);
      if (!attempt || attempt.state !== 'intent') {
        fail(operation, 'ATTEMPT_STATE_INVALID', 'The provider transport is not bound to an open durable intent.');
      }
      const existing = this.#db.prepare(`
        SELECT attempt_id, workflow_id, logical_operation_ref, event_id, event_sequence, created_at_ms
        FROM jev_dispatch_markers WHERE transport_attempt_id = ?
      `).get(fields.transportAttemptId);
      if (existing) {
        if (existing.attempt_id !== fields.attemptId || existing.workflow_id !== fields.workflowId
            || existing.logical_operation_ref !== attempt.logical_operation_ref) {
          fail(operation, 'IDEMPOTENCY_CONFLICT', 'The provider transport identity conflicts with its durable dispatch marker.');
        }
        return {
          duplicate: true,
          transportAttemptId: fields.transportAttemptId,
          eventId: existing.event_id,
          eventSequence: Number(existing.event_sequence),
          revision: Number(row.revision)
        };
      }
      const eventId = 'jev-dispatch-may-have-started:' + digest(fields.transportAttemptId);
      this.#assertEventAvailable(eventId, operation);
      const eventSequence = this.#appendEvent({
        eventId,
        workflowId: fields.workflowId,
        revision: Number(row.revision),
        type: 'jev_dispatch_may_have_started',
        fromState: row.state,
        toState: row.state,
        fence: fields.fence,
        atMs: now
      });
      this.#db.prepare(`
        INSERT INTO jev_dispatch_markers (
          transport_attempt_id, attempt_id, workflow_id, logical_operation_ref,
          event_id, event_sequence, created_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
      `).run(
        fields.transportAttemptId,
        fields.attemptId,
        fields.workflowId,
        attempt.logical_operation_ref,
        eventId,
        eventSequence,
        now
      );
      return { duplicate: false, transportAttemptId: fields.transportAttemptId, eventId, eventSequence, revision: Number(row.revision) };
    });
  }

  getJevDispatchMarker(transportAttemptId) {
    const operation = 'getJevDispatchMarker';
    if (typeof transportAttemptId !== 'string' || !SAFE_OPAQUE_REF.test(transportAttemptId)) {
      fail(operation, 'INVALID_INPUT', 'A provider transport identity is invalid.');
    }
    this.#assertOpen(operation);
    try {
      const row = this.#db.prepare(`
        SELECT transport_attempt_id, attempt_id, workflow_id, logical_operation_ref,
               event_id, event_sequence, created_at_ms
        FROM jev_dispatch_markers WHERE transport_attempt_id = ?
      `).get(transportAttemptId);
      return row ? Object.freeze({
        transportAttemptId: row.transport_attempt_id,
        attemptId: row.attempt_id,
        workflowId: row.workflow_id,
        logicalOperationRef: row.logical_operation_ref,
        eventId: row.event_id,
        eventSequence: Number(row.event_sequence),
        markedAtMs: Number(row.created_at_ms)
      }) : null;
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  recordResponseReceipt({
    workflowId,
    ownerId,
    fence,
    expectedRevision,
    attemptId,
    receiptId,
    outcome,
    nextState,
    responseBytes,
    nextCheckpoint,
    artifacts = [],
    eventId
  } = {}) {
    const operation = 'recordResponseReceipt';
    expectString(workflowId, 'workflowId', operation);
    expectString(attemptId, 'attemptId', operation);
    expectString(receiptId, 'receiptId', operation);
    if (!['completed', 'no_effect'].includes(outcome) || !['running', 'succeeded', 'failed'].includes(nextState)) {
      fail(operation, 'INVALID_INPUT', 'The response outcome or next workflow state is invalid.');
    }
    if (!(responseBytes instanceof Uint8Array) || !Array.isArray(artifacts)) {
      fail(operation, 'INVALID_INPUT', 'The response and artifact content must be supplied as bytes.');
    }
    const bytes = Buffer.from(responseBytes);
    const checkpointJson = serializeCheckpoint(nextCheckpoint, operation);
    const responseSha256 = digest(bytes);
    const checkpointSha256 = digest(checkpointJson);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const row = this.#workflow(workflowId, operation);
      const now = this.#assertLease(row, { ownerId, fence }, operation);
      this.#assertMutable(row, operation);
      this.#assertRevision(row, expectedRevision, operation);
      const attempt = this.#db.prepare('SELECT * FROM operation_attempts WHERE attempt_id = ?').get(attemptId);
      if (!attempt || attempt.workflow_id !== workflowId) fail(operation, 'ATTEMPT_NOT_FOUND', 'The operation attempt does not exist.');
      if (!['intent', 'outcome_unknown'].includes(attempt.state)) {
        fail(operation, 'ATTEMPT_STATE_INVALID', 'The operation attempt cannot accept another response receipt.');
      }
      if (this.#db.prepare('SELECT 1 FROM response_receipts WHERE receipt_id = ?').get(receiptId)) {
        fail(operation, 'IDEMPOTENCY_CONFLICT', 'This response-receipt identity has already been used.');
      }
      if (this.#db.prepare('SELECT 1 FROM response_receipts WHERE attempt_id = ?').get(attemptId)) {
        fail(operation, 'ATTEMPT_STATE_INVALID', 'A response receipt already exists for this attempt.');
      }
      const identity = this.#eventId(eventId, operation);
      this.#assertEventAvailable(identity, operation);
      for (const artifact of artifacts) {
        this.#storeArtifact(workflowId, artifact.artifactId, artifact.bytes, now, operation);
      }
      this.#db.prepare(`
        INSERT INTO response_receipts (
          receipt_id, attempt_id, outcome, next_state, response_bytes, response_sha256,
          next_checkpoint_json, next_checkpoint_sha256, created_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(receiptId, attemptId, outcome, nextState, bytes, responseSha256, checkpointJson, checkpointSha256, now);
      this.#db.prepare(`
        UPDATE operation_attempts SET state = 'response_received', updated_at_ms = ? WHERE attempt_id = ?
      `).run(now, attemptId);
      const revision = Number(row.revision) + 1;
      this.#db.prepare('UPDATE workflows SET revision = ?, updated_at_ms = ? WHERE workflow_id = ?')
        .run(revision, now, workflowId);
      const eventSequence = this.#appendEvent({
        eventId: identity,
        workflowId,
        revision,
        type: 'response_receipt_persisted',
        fromState: row.state,
        toState: row.state,
        fence,
        atMs: now
      });
      return { receiptId, responseSha256, revision, eventSequence };
    });
  }

  markOutcomeUnknown({ workflowId, ownerId, fence, expectedRevision, attemptId, eventId } = {}) {
    const operation = 'markOutcomeUnknown';
    expectString(workflowId, 'workflowId', operation);
    expectString(attemptId, 'attemptId', operation);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const row = this.#workflow(workflowId, operation);
      const now = this.#assertLease(row, { ownerId, fence }, operation);
      this.#assertMutable(row, operation);
      this.#assertRevision(row, expectedRevision, operation);
      const attempt = this.#db.prepare('SELECT * FROM operation_attempts WHERE attempt_id = ?').get(attemptId);
      if (!attempt || attempt.workflow_id !== workflowId) fail(operation, 'ATTEMPT_NOT_FOUND', 'The operation attempt does not exist.');
      if (attempt.state !== 'intent') fail(operation, 'ATTEMPT_STATE_INVALID', 'Only an unresolved dispatch intent can become unknown.');
      const identity = this.#eventId(eventId, operation);
      this.#assertEventAvailable(identity, operation);
      this.#db.prepare(`
        UPDATE operation_attempts SET state = 'outcome_unknown', updated_at_ms = ? WHERE attempt_id = ?
      `).run(now, attemptId);
      const revision = Number(row.revision) + 1;
      this.#db.prepare(`
        UPDATE workflows SET state = 'reconciliation_pending', revision = ?, updated_at_ms = ?
        WHERE workflow_id = ?
      `).run(revision, now, workflowId);
      const eventSequence = this.#appendEvent({
        eventId: identity,
        workflowId,
        revision,
        type: 'external_outcome_unknown',
        fromState: row.state,
        toState: 'reconciliation_pending',
        fence,
        atMs: now
      });
      return { state: 'reconciliation_pending', revision, eventSequence, authorityHoldRef: attempt.authority_hold_ref };
    });
  }

  reconcilePendingReceipts({ workflowId, ownerId, fence, expectedRevision, eventIdPrefix } = {}) {
    const operation = 'reconcilePendingReceipts';
    expectString(workflowId, 'workflowId', operation);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      let row = this.#workflow(workflowId, operation);
      const now = this.#assertLease(row, { ownerId, fence }, operation);
      this.#assertMutable(row, operation);
      this.#assertRevision(row, expectedRevision, operation);
      const pending = this.#db.prepare(`
        SELECT r.*, a.state AS attempt_state
        FROM response_receipts r
        JOIN operation_attempts a ON a.attempt_id = r.attempt_id
        WHERE a.workflow_id = ? AND a.state = 'response_received'
        ORDER BY r.created_at_ms, r.receipt_id
      `).all(workflowId);
      if (pending.length === 0) {
        return {
          reconciled: 0,
          state: row.state,
          revision: Number(row.revision),
          checkpointSequence: Number(row.checkpoint_sequence)
        };
      }

      let reconciled = 0;
      let revision = Number(row.revision);
      let sequence = Number(row.checkpoint_sequence);
      let currentState = row.state;
      const prefix = eventIdPrefix === undefined ? 'reconcile:' + this.#idFactory() : expectString(eventIdPrefix, 'eventIdPrefix', operation);
      for (let index = 0; index < pending.length; index += 1) {
        const receipt = pending[index];
        if (digest(Buffer.from(receipt.response_bytes)) !== receipt.response_sha256) {
          fail(operation, 'CHECKPOINT_CORRUPT', 'A durable response failed its integrity check.');
        }
        if (
          typeof receipt.next_checkpoint_json !== 'string'
          || typeof receipt.next_checkpoint_sha256 !== 'string'
          || Buffer.byteLength(receipt.next_checkpoint_json, 'utf8') > MAX_CHECKPOINT_SERIALIZED_BYTES
          || digest(receipt.next_checkpoint_json) !== receipt.next_checkpoint_sha256
        ) {
          fail(operation, 'CHECKPOINT_CORRUPT', 'A durable response checkpoint failed its integrity check.');
        }
        let envelope;
        try {
          envelope = JSON.parse(receipt.next_checkpoint_json);
        } catch {
          fail(operation, 'CHECKPOINT_CORRUPT', 'A durable response checkpoint is invalid.');
        }
        if (envelope?.schemaVersion !== CHECKPOINT_SCHEMA_VERSION || !isRecord(envelope?.data)) {
          fail(operation, 'UNSUPPORTED_SCHEMA_VERSION', 'A durable response checkpoint has an unsupported schema.');
        }
        const nextSequence = sequence + 1;
        const nextRevision = revision + 1;
        const checkpointJson = receipt.next_checkpoint_json;
        const target = row.cancellation_requested && receipt.next_state === 'running'
          ? 'cancelled'
          : receipt.next_state;
        if (target !== currentState && target !== 'cancelled' && !TRANSITIONS.get(currentState).has(target)) {
          fail(operation, 'ILLEGAL_TRANSITION', 'The recovered response conflicts with the workflow state.');
        }
        const identity = prefix + ':' + index + ':' + receipt.receipt_id;
        this.#assertEventAvailable(identity, operation);
        this.#db.prepare(`
          INSERT INTO workflow_checkpoints (
            workflow_id, sequence, schema_version, payload_json, sha256, revision, created_at_ms
          ) VALUES (?, ?, ?, ?, ?, ?, ?)
        `).run(workflowId, nextSequence, CHECKPOINT_SCHEMA_VERSION, checkpointJson, receipt.next_checkpoint_sha256, nextRevision, now);
        this.#db.prepare(`
          UPDATE operation_attempts SET state = 'reconciled', updated_at_ms = ? WHERE attempt_id = ?
        `).run(now, receipt.attempt_id);
        this.#db.prepare(`
          UPDATE workflows
          SET state = ?, revision = ?, checkpoint_sequence = ?, updated_at_ms = ?
          WHERE workflow_id = ?
        `).run(target, nextRevision, nextSequence, now, workflowId);
        this.#appendEvent({
          eventId: identity,
          workflowId,
          revision: nextRevision,
          type: 'response_checkpoint_reconciled',
          fromState: currentState,
          toState: target,
          fence,
          atMs: now
        });
        reconciled += 1;
        sequence = nextSequence;
        revision = nextRevision;
        currentState = target;
        row = this.#workflow(workflowId, operation);
        if (TERMINAL.has(currentState) && index < pending.length - 1) {
          fail(operation, 'OUTCOME_UNRESOLVED', 'A terminal response left another durable response to reconcile.');
        }
      }
      return { reconciled, state: currentState, revision, checkpointSequence: sequence };
    });
  }

  recordAttemptObservation({ attemptId, sourceObservationRef, kind } = {}) {
    const operation = 'recordAttemptObservation';
    expectString(attemptId, 'attemptId', operation);
    expectString(sourceObservationRef, 'sourceObservationRef', operation);
    if (!['known', 'unknown'].includes(kind)) fail(operation, 'INVALID_INPUT', 'The observation kind is invalid.');
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      if (!this.#db.prepare('SELECT 1 FROM operation_attempts WHERE attempt_id = ?').get(attemptId)) {
        fail(operation, 'ATTEMPT_NOT_FOUND', 'The operation attempt does not exist.');
      }
      const fingerprint = digest(attemptId + ':' + kind);
      const previous = this.#db.prepare(`
        SELECT attempt_id, observation_kind, fingerprint
        FROM attempt_observations WHERE source_observation_ref = ?
      `).get(sourceObservationRef);
      if (previous) {
        if (
          previous.attempt_id !== attemptId
          || previous.observation_kind !== kind
          || previous.fingerprint !== fingerprint
        ) {
          fail(operation, 'OBSERVATION_IDENTITY_CONFLICT', 'The source observation identity conflicts with its prior record.');
        }
        const count = this.#db.prepare(`
          SELECT COUNT(*) AS count FROM attempt_observations WHERE attempt_id = ?
        `).get(attemptId).count;
        return { duplicate: true, observationCount: Number(count) };
      }
      const now = this.#now(operation);
      this.#db.prepare(`
        INSERT INTO attempt_observations (
          source_observation_ref, attempt_id, observation_kind, fingerprint, observed_at_ms
        ) VALUES (?, ?, ?, ?, ?)
      `).run(sourceObservationRef, attemptId, kind, fingerprint, now);
      const count = this.#db.prepare(`
        SELECT COUNT(*) AS count FROM attempt_observations WHERE attempt_id = ?
      `).get(attemptId).count;
      return { duplicate: false, observationCount: Number(count) };
    });
  }

  recordJevProviderObservation(input) {
    const operation = 'recordJevProviderObservation';
    const observation = safeJevObservationInput(input, operation);
    const fingerprint = observationFingerprint(observation);
    const eventId = 'jev-local-observation-v1:' + digest(observation.sourceObservationRef);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const attempt = this.#db.prepare(`
        SELECT a.attempt_id, a.workflow_id, a.logical_operation_ref,
               t.transport_attempt_id
        FROM operation_attempts a
        JOIN transport_attempts t ON t.attempt_id = a.attempt_id
        WHERE a.attempt_id = ? AND t.transport_attempt_id = ?
      `).get(observation.attemptId, observation.transportAttemptId);
      if (!attempt) fail(operation, 'ATTEMPT_NOT_FOUND', 'The provider observation attempt or transport does not exist.');
      if (attempt.workflow_id !== observation.workflowId
          || attempt.logical_operation_ref !== observation.logicalOperationRef) {
        fail(operation, 'OBSERVATION_IDENTITY_CONFLICT', 'The provider observation identity conflicts with its durable attempt.');
      }
      if (['possible', 'response_received'].includes(observation.dispatchCertainty)
          && !this.#db.prepare(`
            SELECT 1 FROM jev_dispatch_markers
            WHERE attempt_id = ? AND transport_attempt_id = ? AND workflow_id = ?
          `).get(observation.attemptId, observation.transportAttemptId, observation.workflowId)) {
        fail(operation, 'ATTEMPT_STATE_INVALID', 'A possible provider dispatch requires a durable pre-send marker.');
      }
      const previous = this.#db.prepare(`
        SELECT fingerprint, event_id, event_sequence
        FROM jev_provider_observations WHERE source_observation_ref = ?
      `).get(observation.sourceObservationRef);
      if (previous) {
        if (previous.fingerprint !== fingerprint) {
          fail(operation, 'OBSERVATION_IDENTITY_CONFLICT', 'The provider observation source identity conflicts with its prior record.');
        }
        return {
          duplicate: true,
          sourceObservationRef: observation.sourceObservationRef,
          eventId: previous.event_id,
          eventSequence: Number(previous.event_sequence)
        };
      }
      this.#assertEventAvailable(eventId, operation);
      const workflow = this.#workflow(observation.workflowId, operation);
      const now = this.#now(operation);
      const eventSequence = this.#appendEvent({
        eventId,
        workflowId: observation.workflowId,
        revision: Number(workflow.revision),
        type: 'jev_local_observation_recorded',
        fromState: workflow.state,
        toState: workflow.state,
        fence: Number(workflow.fence_token),
        atMs: now
      });
      this.#db.prepare(`
        INSERT INTO jev_provider_observations (
          source_observation_ref, workflow_id, logical_operation_ref, attempt_id,
          transport_attempt_id, dispatch_certainty, requested_model, resolved_model,
          provider_request_id, decision_conformance, usage_kind, input_tokens,
          output_tokens, unknown_reason, event_id, event_sequence, fingerprint, recorded_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        observation.sourceObservationRef,
        observation.workflowId,
        observation.logicalOperationRef,
        observation.attemptId,
        observation.transportAttemptId,
        observation.dispatchCertainty,
        observation.requestedModel,
        observation.resolvedModel,
        observation.providerRequestId,
        observation.decisionConformance,
        observation.usage.kind,
        observation.usage.kind === 'known' ? observation.usage.inputTokens : null,
        observation.usage.kind === 'known' ? observation.usage.outputTokens : null,
        observation.usage.kind === 'unknown' ? observation.usage.reason : null,
        eventId,
        eventSequence,
        fingerprint,
        now
      );
      this.#db.prepare(`
        INSERT INTO jev_local_observation_outbox (
          event_id, event_sequence, source_observation_ref, delivered_at_ms
        ) VALUES (?, ?, ?, NULL)
      `).run(eventId, eventSequence, observation.sourceObservationRef);
      return {
        duplicate: false,
        sourceObservationRef: observation.sourceObservationRef,
        eventId,
        eventSequence
      };
    });
  }

  getJevProviderObservation(sourceObservationRef) {
    const operation = 'getJevProviderObservation';
    if (typeof sourceObservationRef !== 'string' || !SAFE_OPAQUE_REF.test(sourceObservationRef)) {
      fail(operation, 'INVALID_INPUT', 'A provider observation identity is invalid.');
    }
    this.#assertOpen(operation);
    try {
      const row = this.#db.prepare(`
        SELECT * FROM jev_provider_observations WHERE source_observation_ref = ?
      `).get(sourceObservationRef);
      if (!row) return null;
      try {
        return mapJevObservation(row);
      } catch {
        fail(operation, 'CHECKPOINT_CORRUPT', 'A durable provider observation failed its integrity check.');
      }
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  readJevObservationDeliveryPage(input) {
    const operation = 'readJevObservationDeliveryPage';
    const fields = input === undefined
      ? Object.create(null)
      : readOwnDataRecord(input, ['afterSequence', 'limit'], operation, []);
    const afterSequence = fields.afterSequence === undefined ? 0 : fields.afterSequence;
    const limit = fields.limit === undefined ? 50 : fields.limit;
    expectInteger(afterSequence, 'afterSequence', operation);
    expectInteger(limit, 'limit', operation, 1);
    if (limit > MAX_JEV_OBSERVATION_PAGE_SIZE) {
      fail(operation, 'INVALID_INPUT', 'The local provider observation page exceeds its supported record limit.');
    }
    this.#assertOpen(operation);
    try {
      const rows = this.#db.prepare(`
        SELECT o.*, d.delivered_at_ms
        FROM jev_local_observation_outbox d
        JOIN jev_provider_observations o ON o.source_observation_ref = d.source_observation_ref
        WHERE d.event_sequence > ? AND d.delivered_at_ms IS NULL
        ORDER BY d.event_sequence
        LIMIT ?
      `).all(afterSequence, limit + 1);
      const hasMore = rows.length > limit;
      if (hasMore) rows.pop();
      const items = rows.map(row => {
        try {
          return Object.freeze({
            ...mapJevObservation(row),
            deliveryIntent: 'local_only_not_finance_acceptance'
          });
        } catch {
          fail(operation, 'CHECKPOINT_CORRUPT', 'A local provider observation failed its integrity check.');
        }
      });
      return Object.freeze({
        items: Object.freeze(items),
        hasMore,
        nextAfterSequence: items.length === 0 ? afterSequence : items.at(-1).eventSequence
      });
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  acknowledgeJevObservationDelivery(eventId) {
    const operation = 'acknowledgeJevObservationDelivery';
    if (typeof eventId !== 'string' || !SAFE_OPAQUE_REF.test(eventId)) {
      fail(operation, 'INVALID_INPUT', 'A local provider delivery identity is invalid.');
    }
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const now = this.#now(operation);
      const result = this.#db.prepare(`
        UPDATE jev_local_observation_outbox
        SET delivered_at_ms = ?
        WHERE event_id = ? AND delivered_at_ms IS NULL
      `).run(now, eventId);
      if (Number(result.changes) === 1) return { acknowledged: true, duplicate: false };
      const exists = this.#db.prepare(
        'SELECT 1 FROM jev_local_observation_outbox WHERE event_id = ?'
      ).get(eventId);
      return { acknowledged: Boolean(exists), duplicate: Boolean(exists) };
    });
  }

  getAttempt(attemptId) {
    const operation = 'getAttempt';
    expectString(attemptId, 'attemptId', operation);
    this.#assertOpen(operation);
    try {
      const row = this.#db.prepare('SELECT * FROM operation_attempts WHERE attempt_id = ?').get(attemptId);
      if (!row) fail(operation, 'ATTEMPT_NOT_FOUND', 'The operation attempt does not exist.');
      const transports = this.#db.prepare(`
        SELECT transport_attempt_id, created_at_ms FROM transport_attempts
        WHERE attempt_id = ? ORDER BY created_at_ms, transport_attempt_id
      `).all(attemptId);
      return {
        attemptId: row.attempt_id,
        workflowId: row.workflow_id,
        logicalOperationRef: row.logical_operation_ref,
        authorityHoldRef: row.authority_hold_ref,
        state: row.state,
        transportAttempts: transports.map(item => ({
          transportAttemptId: item.transport_attempt_id,
          createdAtMs: Number(item.created_at_ms)
        }))
      };
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  getReceipt(attemptId) {
    const operation = 'getReceipt';
    expectString(attemptId, 'attemptId', operation);
    this.#assertOpen(operation);
    try {
      const row = this.#db.prepare('SELECT * FROM response_receipts WHERE attempt_id = ?').get(attemptId);
      if (!row) fail(operation, 'ATTEMPT_NOT_FOUND', 'The response receipt does not exist.');
      const bytes = Buffer.from(row.response_bytes);
      if (digest(bytes) !== row.response_sha256) {
        fail(operation, 'CHECKPOINT_CORRUPT', 'The durable response failed its integrity check.');
      }
      return {
        receiptId: row.receipt_id,
        attemptId,
        outcome: row.outcome,
        nextState: row.next_state,
        responseSha256: row.response_sha256,
        responseBytes: bytes,
        createdAtMs: Number(row.created_at_ms)
      };
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  getArtifact(workflowId, artifactId) {
    const operation = 'getArtifact';
    expectString(workflowId, 'workflowId', operation);
    expectString(artifactId, 'artifactId', operation);
    this.#assertOpen(operation);
    try {
      const row = this.#db.prepare(`
        SELECT * FROM immutable_artifacts WHERE workflow_id = ? AND artifact_id = ?
      `).get(workflowId, artifactId);
      if (!row) fail(operation, 'ARTIFACT_NOT_FOUND', 'The immutable artifact does not exist.');
      const bytes = Buffer.from(row.bytes);
      if (digest(bytes) !== row.sha256) fail(operation, 'CHECKPOINT_CORRUPT', 'The immutable artifact failed its integrity check.');
      return { workflowId, artifactId, sha256: row.sha256, bytes };
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  listEvents({ workflowId, afterSequence = 0 } = {}) {
    const operation = 'listEvents';
    expectInteger(afterSequence, 'afterSequence', operation);
    if (workflowId !== undefined) expectString(workflowId, 'workflowId', operation);
    this.#assertOpen(operation);
    try {
      const rows = workflowId === undefined
        ? this.#db.prepare(`
            SELECT * FROM workflow_events WHERE sequence > ? ORDER BY sequence
          `).all(afterSequence)
        : this.#db.prepare(`
            SELECT * FROM workflow_events WHERE workflow_id = ? AND sequence > ? ORDER BY sequence
          `).all(workflowId, afterSequence);
      return rows.map(row => ({
        sequence: Number(row.sequence),
        eventId: row.event_id,
        workflowId: row.workflow_id,
        revision: Number(row.revision),
        type: row.event_type,
        fromState: row.from_state,
        toState: row.to_state,
        fence: Number(row.fence_token),
        occurredAtMs: Number(row.occurred_at_ms)
      }));
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  readEventPage({ afterSequence = 0, limit = 100, workflowId } = {}) {
    const operation = 'readEventPage';
    expectInteger(afterSequence, 'afterSequence', operation);
    expectInteger(limit, 'limit', operation, 1);
    if (limit > 500) fail(operation, 'INVALID_INPUT', 'The event page exceeds its supported record limit.');
    if (workflowId !== undefined) expectString(workflowId, 'workflowId', operation);
    this.#assertOpen(operation);
    try {
      const rows = workflowId === undefined
        ? this.#db.prepare(`
            SELECT sequence, event_id, workflow_id, revision, event_type,
                   from_state, to_state, fence_token, occurred_at_ms
            FROM workflow_events WHERE sequence > ? ORDER BY sequence LIMIT ?
          `).all(afterSequence, limit + 1)
        : this.#db.prepare(`
            SELECT sequence, event_id, workflow_id, revision, event_type,
                   from_state, to_state, fence_token, occurred_at_ms
            FROM workflow_events WHERE workflow_id = ? AND sequence > ? ORDER BY sequence LIMIT ?
          `).all(workflowId, afterSequence, limit + 1);
      const hasMore = rows.length > limit;
      if (hasMore) rows.pop();
      const events = rows.map(row => ({
        sequence: Number(row.sequence),
        eventId: row.event_id,
        workflowId: row.workflow_id,
        revision: Number(row.revision),
        type: row.event_type,
        fromState: row.from_state,
        toState: row.to_state,
        fence: Number(row.fence_token),
        occurredAtMs: Number(row.occurred_at_ms)
      }));
      if (events.some(event => !Number.isSafeInteger(event.sequence)
          || !Number.isSafeInteger(event.revision)
          || !Number.isSafeInteger(event.fence)
          || !Number.isSafeInteger(event.occurredAtMs))) {
        fail(operation, 'CHECKPOINT_CORRUPT', 'The event page contains an unsupported numeric value.');
      }
      return {
        events,
        hasMore,
        nextAfterSequence: events.length === 0 ? afterSequence : events.at(-1).sequence
      };
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  countEventRowsAfter(afterSequence = 0, cap = 10_000) {
    const operation = 'countEventRowsAfter';
    expectInteger(afterSequence, 'afterSequence', operation);
    expectInteger(cap, 'cap', operation, 1);
    if (cap > 1_000_000) fail(operation, 'INVALID_INPUT', 'The event count cap exceeds its supported range.');
    this.#assertOpen(operation);
    try {
      const row = this.#db.prepare(`
        SELECT COUNT(*) AS count FROM (
          SELECT sequence FROM workflow_events WHERE sequence > ? ORDER BY sequence LIMIT ?
        )
      `).get(afterSequence, cap + 1);
      const count = Number(row.count);
      return { count: Math.min(count, cap), capped: count > cap };
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  getTelemetryCursor(consumerId = 'internal-telemetry-v1') {
    const operation = 'getTelemetryCursor';
    if (typeof consumerId !== 'string' || !/^[a-z0-9][a-z0-9._-]{0,63}$/i.test(consumerId)) {
      fail(operation, 'INVALID_INPUT', 'The telemetry consumer identity is invalid.');
    }
    this.#assertOpen(operation);
    try {
      const row = this.#db.prepare(
        'SELECT last_sequence FROM telemetry_cursors WHERE consumer_id = ?'
      ).get(consumerId);
      const cursor = row ? Number(row.last_sequence) : 0;
      if (!Number.isSafeInteger(cursor) || cursor < 0) {
        fail(operation, 'CHECKPOINT_CORRUPT', 'The telemetry cursor contains an unsupported numeric value.');
      }
      return cursor;
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  advanceTelemetryCursor({ consumerId = 'internal-telemetry-v1', expectedSequence, nextSequence } = {}) {
    const operation = 'advanceTelemetryCursor';
    if (typeof consumerId !== 'string' || !/^[a-z0-9][a-z0-9._-]{0,63}$/i.test(consumerId)) {
      fail(operation, 'INVALID_INPUT', 'The telemetry consumer identity is invalid.');
    }
    expectInteger(expectedSequence, 'expectedSequence', operation);
    expectInteger(nextSequence, 'nextSequence', operation, 1);
    if (nextSequence - expectedSequence !== 1) {
      fail(operation, 'INVALID_INPUT', 'The telemetry cursor advances one event at a time.');
    }
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const currentRow = this.#db.prepare(
        'SELECT last_sequence FROM telemetry_cursors WHERE consumer_id = ?'
      ).get(consumerId);
      const current = currentRow ? Number(currentRow.last_sequence) : 0;
      if (current !== expectedSequence) {
        fail(operation, 'CONCURRENT_MODIFICATION', 'The telemetry cursor changed before it could be advanced.');
      }
      const available = Number(this.#db.prepare(`
        SELECT COUNT(*) AS count FROM workflow_events
        WHERE sequence > ? AND sequence <= ?
      `).get(expectedSequence, nextSequence).count);
      if (available !== 1) {
        fail(operation, 'CONCURRENT_MODIFICATION', 'The telemetry cursor cannot skip an unavailable event.');
      }
      const now = this.#now(operation);
      if (currentRow) {
        const changed = this.#db.prepare(`
          UPDATE telemetry_cursors SET last_sequence = ?, updated_at_ms = ?
          WHERE consumer_id = ? AND last_sequence = ?
        `).run(nextSequence, now, consumerId, expectedSequence);
        if (Number(changed.changes) !== 1) {
          fail(operation, 'CONCURRENT_MODIFICATION', 'The telemetry cursor changed before it could be advanced.');
        }
      } else {
        if (expectedSequence !== 0) {
          fail(operation, 'CONCURRENT_MODIFICATION', 'The telemetry cursor has no matching starting point.');
        }
        this.#db.prepare(`
          INSERT INTO telemetry_cursors (consumer_id, last_sequence, updated_at_ms) VALUES (?, ?, ?)
        `).run(consumerId, nextSequence, now);
      }
      return nextSequence;
    });
  }

  readOutbox({ afterSequence = 0, limit = 100 } = {}) {
    const operation = 'readOutbox';
    expectInteger(afterSequence, 'afterSequence', operation);
    expectInteger(limit, 'limit', operation, 1);
    if (limit > 1000) fail(operation, 'INVALID_INPUT', 'The outbox read limit is too large.');
    this.#assertOpen(operation);
    try {
      return this.#db.prepare(`
        SELECT o.event_id, o.event_sequence, o.delivered_at_ms, o.delivery_attempts,
               e.workflow_id, e.event_type, e.occurred_at_ms
        FROM outbox o
        JOIN workflow_events e ON e.sequence = o.event_sequence
        WHERE o.event_sequence > ? AND o.delivered_at_ms IS NULL
        ORDER BY o.event_sequence
        LIMIT ?
      `).all(afterSequence, limit).map(row => ({
        eventId: row.event_id,
        sequence: Number(row.event_sequence),
        workflowId: row.workflow_id,
        type: row.event_type,
        occurredAtMs: Number(row.occurred_at_ms),
        deliveryAttempts: Number(row.delivery_attempts)
      }));
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  acknowledgeOutbox(eventId) {
    const operation = 'acknowledgeOutbox';
    expectString(eventId, 'eventId', operation);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const now = this.#now(operation);
      const result = this.#db.prepare(`
        UPDATE outbox
        SET delivered_at_ms = COALESCE(delivered_at_ms, ?),
            delivery_attempts = CASE WHEN delivered_at_ms IS NULL THEN delivery_attempts + 1 ELSE delivery_attempts END
        WHERE event_id = ?
      `).run(now, eventId);
      if (Number(result.changes) > 0) return { acknowledged: true };
      const exists = this.#db.prepare('SELECT 1 FROM outbox WHERE event_id = ?').get(eventId);
      return { acknowledged: Boolean(exists) };
    });
  }
}
