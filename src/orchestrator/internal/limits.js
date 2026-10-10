import { createHash } from 'node:crypto';
import { types as utilTypes } from 'node:util';
import { parseBoundedJson } from './json.js';

const DEFAULTS = Object.freeze({
  maxPlanBytes: 32_768,
  maxSteps: 32,
  maxEdges: 64,
  maxDepth: 12,
  maxReadyQueue: 16,
  maxInputBytes: 8_192,
  maxResultBytes: 8_192,
  maxTotalResultBytes: 65_536,
  maxResultsJsonBytes: 131_072,
  maxStepAttempts: 64,
  maxLocalRetries: 32,
  maxReplans: 8,
  maxIterations: 64,
  maxElapsedMs: 30_000,
  maxStepMs: 1_000,
  maxRecoveryAttemptStates: 32
});

const LIMIT_KEYS = new Set(Object.keys(DEFAULTS));
const LIMIT_SET_DATA = new WeakMap();
const LIMIT_SET_KEYS = new WeakSet();

export class LimitError extends Error {
  constructor(code) {
    super('The bounded local runtime limit input is invalid.');
    this.name = 'LimitError';
    this.code = code;
  }
}

function fail(code) {
  throw new LimitError(code);
}

function sha256(value) {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}

function plainDataRecord(value, allowedKeys, requiredKeys = []) {
  if (
    value === null
    || typeof value !== 'object'
    || Array.isArray(value)
    || utilTypes.isProxy(value)
  ) {
    fail('INVALID_LIMITS');
  }

  let prototype;
  let keys;
  try {
    prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) fail('INVALID_LIMITS');
    keys = Reflect.ownKeys(value);
  } catch {
    fail('INVALID_LIMITS');
  }

  if (keys.length > allowedKeys.size) fail('INVALID_LIMITS');
  const output = Object.create(null);
  for (const key of keys) {
    if (typeof key !== 'string' || !allowedKeys.has(key)) fail('INVALID_LIMITS');
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      fail('INVALID_LIMITS');
    }
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) fail('INVALID_LIMITS');
    output[key] = descriptor.value;
  }
  for (const key of requiredKeys) {
    if (!Object.hasOwn(output, key)) fail('INVALID_LIMITS');
  }
  return output;
}

function makeLimitSet(values) {
  const frozenValues = Object.freeze({ ...values });
  const canonical = JSON.stringify(Object.keys(frozenValues).sort().map(key => [key, frozenValues[key]]));
  const handle = Object.freeze({
    version: 1,
    hash: 'limits1_' + sha256(canonical),
    values: frozenValues
  });
  LIMIT_SET_DATA.set(handle, frozenValues);
  LIMIT_SET_KEYS.add(handle);
  return handle;
}

export function createLimitSet(overridesJson = '{}') {
  if (typeof overridesJson !== 'string' || Buffer.byteLength(overridesJson, 'utf8') > 2_048) {
    fail('INVALID_LIMITS');
  }

  let parsed;
  try {
    parsed = parseBoundedJson(overridesJson, {
      maxBytes: 2_048,
      maxDepth: 2
    });
  } catch {
    fail('INVALID_LIMITS');
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) fail('INVALID_LIMITS');

  const overrides = plainDataRecord(parsed, LIMIT_KEYS);
  const values = { ...DEFAULTS };
  for (const [key, value] of Object.entries(overrides)) {
    const minimum = key === 'maxEdges' || key === 'maxLocalRetries' || key === 'maxReplans'
      ? 0
      : 1;
    if (!Number.isSafeInteger(value) || value < minimum || value > DEFAULTS[key]) {
      fail('LIMIT_ENLARGEMENT_OR_INVALID_VALUE');
    }
    values[key] = value;
  }
  if (
    values.maxResultBytes > values.maxTotalResultBytes
    || values.maxLocalRetries > values.maxStepAttempts
  ) {
    fail('INCONSISTENT_LIMITS');
  }
  return makeLimitSet(values);
}

export const DEFAULT_LIMIT_SET = createLimitSet();

function valuesFor(limitSet) {
  if (!LIMIT_SET_KEYS.has(limitSet)) fail('INVALID_LIMITS');
  return LIMIT_SET_DATA.get(limitSet);
}

export function getLimitValues(limitSet = DEFAULT_LIMIT_SET) {
  return valuesFor(limitSet);
}

export function createBudgetSnapshot(limitSet, startedWallMs, deadlineWallMs) {
  const limits = valuesFor(limitSet);
  if (
    !Number.isSafeInteger(startedWallMs)
    || startedWallMs < 0
    || !Number.isSafeInteger(deadlineWallMs)
    || deadlineWallMs <= startedWallMs
    || deadlineWallMs - startedWallMs > limits.maxElapsedMs
  ) {
    fail('INVALID_DEADLINE');
  }

  return Object.freeze({
    version: 1,
    limitsHash: limitSet.hash,
    startedWallMs,
    deadlineWallMs,
    lastWallMs: startedWallMs,
    elapsedMs: 0,
    stepsAdmitted: 0,
    stepAttempts: 0,
    localRetries: 0,
    replans: 0,
    iterations: 0,
    totalResultBytes: 0
  });
}

const BUDGET_KEYS = new Set([
  'version',
  'limitsHash',
  'startedWallMs',
  'deadlineWallMs',
  'lastWallMs',
  'elapsedMs',
  'stepsAdmitted',
  'stepAttempts',
  'localRetries',
  'replans',
  'iterations',
  'totalResultBytes'
]);

export function validateBudgetSnapshot(snapshot, limitSet) {
  const limits = valuesFor(limitSet);
  const fields = plainDataRecord(snapshot, BUDGET_KEYS, [...BUDGET_KEYS]);
  if (
    fields.version !== 1
    || fields.limitsHash !== limitSet.hash
    || !Number.isSafeInteger(fields.startedWallMs)
    || !Number.isSafeInteger(fields.deadlineWallMs)
    || !Number.isSafeInteger(fields.lastWallMs)
    || !Number.isSafeInteger(fields.elapsedMs)
    || !Number.isSafeInteger(fields.stepsAdmitted)
    || !Number.isSafeInteger(fields.stepAttempts)
    || !Number.isSafeInteger(fields.localRetries)
    || !Number.isSafeInteger(fields.replans)
    || !Number.isSafeInteger(fields.iterations)
    || !Number.isSafeInteger(fields.totalResultBytes)
    || fields.startedWallMs < 0
    || fields.deadlineWallMs <= fields.startedWallMs
    || fields.deadlineWallMs - fields.startedWallMs > limits.maxElapsedMs
    || fields.lastWallMs < fields.startedWallMs
    || fields.lastWallMs > fields.deadlineWallMs
    || fields.elapsedMs < 0
    || fields.elapsedMs > limits.maxElapsedMs
    || fields.stepsAdmitted < 0
    || fields.stepsAdmitted > limits.maxSteps
    || fields.stepAttempts < 0
    || fields.stepAttempts > limits.maxStepAttempts
    || fields.stepAttempts !== fields.stepsAdmitted + fields.localRetries
    || fields.localRetries < 0
    || fields.localRetries > limits.maxLocalRetries
    || fields.replans < 0
    || fields.replans > limits.maxReplans
    || fields.iterations < 0
    || fields.iterations > limits.maxIterations
    || fields.iterations < fields.stepAttempts + fields.replans
    || fields.totalResultBytes < 0
    || fields.totalResultBytes > limits.maxTotalResultBytes
  ) {
    fail('INVALID_BUDGET_SNAPSHOT');
  }
  return Object.freeze({ ...fields });
}

export function checkClockBudget(snapshot, wallNowMs, elapsedMs, limitSet = DEFAULT_LIMIT_SET) {
  const limits = valuesFor(limitSet);
  const state = validateBudgetSnapshot(snapshot, limitSet);
  if (
    !Number.isSafeInteger(wallNowMs)
    || wallNowMs < 0
  ) {
    return 'CLOCK_INCONSISTENT';
  }
  if (wallNowMs < state.lastWallMs) return 'CLOCK_ROLLBACK';
  if (!Number.isSafeInteger(elapsedMs) || elapsedMs < 0) return 'CLOCK_INCONSISTENT';
  if (elapsedMs < state.elapsedMs) return 'CLOCK_ROLLBACK';
  if (wallNowMs < state.startedWallMs || state.deadlineWallMs <= state.startedWallMs) {
    return 'CLOCK_INCONSISTENT';
  }
  const wallDelta = wallNowMs - state.lastWallMs;
  const elapsedDelta = elapsedMs - state.elapsedMs;
  if (Math.abs(wallDelta - elapsedDelta) > 250) return 'CLOCK_INCONSISTENT';
  if (wallNowMs >= state.deadlineWallMs) return 'DEADLINE_EXCEEDED';
  if (elapsedMs >= limits.maxElapsedMs) return 'ELAPSED_LIMIT';
  return null;
}

function withBudget(state, changes) {
  return Object.freeze({ ...state, ...changes });
}

export function advanceBudget(snapshot, kind, wallNowMs, elapsedMs, limitSet = DEFAULT_LIMIT_SET) {
  const limits = valuesFor(limitSet);
  const state = validateBudgetSnapshot(snapshot, limitSet);
  const clockReason = checkClockBudget(state, wallNowMs, elapsedMs, limitSet);
  if (clockReason) return Object.freeze({ allowed: false, reason: clockReason, state });

  if (!['step', 'replay', 'replan', 'iteration'].includes(kind)) {
    return Object.freeze({ allowed: false, reason: 'INVALID_BUDGET_EVENT', state });
  }
  if (state.iterations >= limits.maxIterations) {
    return Object.freeze({ allowed: false, reason: 'ITERATION_LIMIT', state });
  }

  const changes = {
    lastWallMs: wallNowMs,
    elapsedMs
  };
  if (kind === 'step') {
    if (state.stepsAdmitted >= limits.maxSteps) {
      return Object.freeze({ allowed: false, reason: 'STEP_LIMIT', state });
    }
    if (state.stepAttempts >= limits.maxStepAttempts) {
      return Object.freeze({ allowed: false, reason: 'STEP_ATTEMPT_LIMIT', state });
    }
    changes.stepsAdmitted = state.stepsAdmitted + 1;
    changes.stepAttempts = state.stepAttempts + 1;
    changes.iterations = state.iterations + 1;
  } else if (kind === 'replay') {
    if (state.localRetries >= limits.maxLocalRetries) {
      return Object.freeze({ allowed: false, reason: 'LOCAL_RETRY_LIMIT', state });
    }
    if (state.stepAttempts >= limits.maxStepAttempts) {
      return Object.freeze({ allowed: false, reason: 'STEP_ATTEMPT_LIMIT', state });
    }
    changes.localRetries = state.localRetries + 1;
    changes.stepAttempts = state.stepAttempts + 1;
    changes.iterations = state.iterations + 1;
  } else if (kind === 'replan') {
    if (state.replans >= limits.maxReplans) {
      return Object.freeze({ allowed: false, reason: 'REPLAN_LIMIT', state });
    }
    changes.replans = state.replans + 1;
    changes.iterations = state.iterations + 1;
  } else {
    changes.iterations = state.iterations + 1;
  }

  return Object.freeze({ allowed: true, reason: null, state: withBudget(state, changes) });
}

export function recordResultBytes(snapshot, byteLength, limitSet = DEFAULT_LIMIT_SET) {
  const limits = valuesFor(limitSet);
  const state = validateBudgetSnapshot(snapshot, limitSet);
  if (!Number.isSafeInteger(byteLength) || byteLength < 0 || byteLength > limits.maxResultBytes) {
    return Object.freeze({ allowed: false, reason: 'RESULT_SIZE_LIMIT', state });
  }
  if (state.totalResultBytes + byteLength > limits.maxTotalResultBytes) {
    return Object.freeze({ allowed: false, reason: 'TOTAL_RESULT_SIZE_LIMIT', state });
  }
  return Object.freeze({
    allowed: true,
    reason: null,
    state: withBudget(state, { totalResultBytes: state.totalResultBytes + byteLength })
  });
}

export function checkStepDuration(durationMs, limitSet = DEFAULT_LIMIT_SET) {
  const limits = valuesFor(limitSet);
  if (!Number.isSafeInteger(durationMs) || durationMs < 0) {
    return Object.freeze({ allowed: false, reason: 'CLOCK_INCONSISTENT' });
  }
  if (durationMs >= limits.maxStepMs) {
    return Object.freeze({ allowed: false, reason: 'STEP_DURATION_LIMIT' });
  }
  return Object.freeze({ allowed: true, reason: null });
}

export function serializeLimitSet(limitSet = DEFAULT_LIMIT_SET) {
  const values = valuesFor(limitSet);
  const overrides = Object.create(null);
  for (const key of Object.keys(values).sort()) {
    if (values[key] !== DEFAULTS[key]) overrides[key] = values[key];
  }
  return JSON.stringify(overrides);
}

export function limitSetFromCheckpoint(overridesJson, expectedHash) {
  const limitSet = createLimitSet(overridesJson);
  if (limitSet.hash !== expectedHash) fail('LIMIT_IDENTITY_MISMATCH');
  return limitSet;
}

export const LIMIT_DEFAULTS = DEFAULTS;
