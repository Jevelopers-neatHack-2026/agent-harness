import { createHash } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { types as utilTypes } from 'node:util';
import {
  advanceBudget,
  checkClockBudget,
  checkStepDuration,
  createBudgetSnapshot,
  DEFAULT_LIMIT_SET,
  getLimitValues,
  recordResultBytes
} from './limits.js';
import { parseBoundedJson } from './json.js';
import {
  createDefaultRegistry,
  executeLocalOperation,
  getRegistryVersion,
  inspectOperation,
  isDefaultRegistry
} from '../../tools/internal/registry.js';

const PLAN_KIND = 'agent-harness.private-synthetic-plan';
const PLAN_HANDLES = new WeakMap();
const FORBIDDEN_KEYS = new Set(['__proto__', 'constructor', 'prototype', 'toJSON']);
const STEP_ID = /^[a-z][a-z0-9_-]{0,47}$/;
const RESULT_KEYS = new Set([
  'stepId',
  'planHash',
  'registryVersion',
  'runIdentity',
  'inputHash',
  'resultHash',
  'output'
]);

const SAFE_MESSAGES = Object.freeze({
  PLAN_INPUT_INVALID: 'The private plan input is invalid.',
  PLAN_TOO_LARGE: 'The private plan exceeds its configured byte limit.',
  PLAN_TOO_DEEP: 'The private plan exceeds its configured nesting limit.',
  PLAN_UNSUPPORTED_VERSION: 'The private plan version is unsupported.',
  PLAN_INVALID_SHAPE: 'The private plan does not match the private execution shape.',
  PLAN_UNKNOWN_OPERATION: 'The private plan names an unregistered operation.',
  PLAN_EFFECT_UNAVAILABLE: 'The requested effect class is unavailable in every mode.',
  PLAN_BOUNDS_EXCEEDED: 'The private plan exceeds a configured structural limit.',
  PLAN_DEPENDENCY_INVALID: 'The private plan has an invalid dependency graph.',
  PLAN_CYCLE: 'The private plan contains a dependency cycle.',
  PLAN_ARGUMENT_INVALID: 'A private operation argument is invalid.',
  PLAN_ARGUMENT_TOO_LARGE: 'A private operation argument exceeds its byte limit.',
  PLAN_RESULT_INVALID: 'A private operation result failed local validation.',
  PLAN_RESULT_TOO_LARGE: 'A private operation result exceeds its byte limit.',
  PLAN_LIMIT_STOP: 'A private local execution limit stopped the plan.',
  PLAN_CLOCK_STOP: 'A private local execution clock check stopped the plan.',
  PLAN_STEP_NOT_READY: 'The requested step is not the deterministic ready step.',
  PLAN_STEP_ALREADY_COMPLETE: 'The requested step already has a verified result.',
  PLAN_RESULT_IDENTITY_INVALID: 'A saved local result does not match this plan and registry.',
  PLAN_RESULTS_INVALID: 'The saved local result set is invalid.',
  PLAN_REGISTRY_INVALID: 'The private static tool registry is invalid.'
});

export class PlanError extends Error {
  constructor(code) {
    super(SAFE_MESSAGES[code] ?? 'The private bounded operation stopped safely.');
    this.name = 'PlanError';
    this.code = code;
    this.safe_message = this.message;
  }
}

function fail(code) {
  throw new PlanError(code);
}

function sha256(value) {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}

function exactRecord(value, allowedKeys, requiredKeys, code = 'PLAN_INVALID_SHAPE') {
  if (
    value === null
    || typeof value !== 'object'
    || Array.isArray(value)
    || utilTypes.isProxy(value)
  ) {
    fail(code);
  }
  let prototype;
  let keys;
  try {
    prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) fail(code);
    keys = Reflect.ownKeys(value);
  } catch {
    fail(code);
  }
  if (keys.length > allowedKeys.size) fail(code);
  const result = Object.create(null);
  for (const key of keys) {
    if (typeof key !== 'string' || !allowedKeys.has(key) || FORBIDDEN_KEYS.has(key)) fail(code);
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      fail(code);
    }
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) fail(code);
    result[key] = descriptor.value;
  }
  for (const key of requiredKeys) {
    if (!Object.hasOwn(result, key)) fail(code);
  }
  return result;
}

function cloneJson(value, limits, depth = 0, counter = { nodes: 0 }) {
  counter.nodes += 1;
  if (counter.nodes > 8_192 || depth > limits.maxDepth) fail('PLAN_BOUNDS_EXCEEDED');
  if (value === null || typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    if (Buffer.byteLength(value, 'utf8') > limits.maxInputBytes) fail('PLAN_ARGUMENT_TOO_LARGE');
    return value;
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) fail('PLAN_ARGUMENT_INVALID');
    return value;
  }
  if (typeof value !== 'object' || utilTypes.isProxy(value)) fail('PLAN_ARGUMENT_INVALID');

  if (Array.isArray(value)) {
    if (Object.getPrototypeOf(value) !== Array.prototype && Object.getPrototypeOf(value) !== null) {
      fail('PLAN_ARGUMENT_INVALID');
    }
    if (value.length > limits.maxSteps * 2) fail('PLAN_BOUNDS_EXCEEDED');
    const output = [];
    Object.setPrototypeOf(output, null);
    for (let index = 0; index < value.length; index += 1) {
      const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
      if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) fail('PLAN_ARGUMENT_INVALID');
      output[output.length] = cloneJson(descriptor.value, limits, depth + 1, counter);
    }
    if (Reflect.ownKeys(value).length !== value.length + 1) fail('PLAN_ARGUMENT_INVALID');
    return output;
  }

  const source = exactRecord(
    value,
    new Set(Reflect.ownKeys(value).filter(key => typeof key === 'string')),
    [],
    'PLAN_ARGUMENT_INVALID'
  );
  const keys = Object.keys(source);
  const output = Object.create(null);
  for (const key of keys.sort()) {
    if (FORBIDDEN_KEYS.has(key)) fail('PLAN_ARGUMENT_INVALID');
    output[key] = cloneJson(source[key], limits, depth + 1, counter);
  }
  return output;
}

function parseJson(source, maxBytes, maxDepth, sizeCode = 'PLAN_TOO_LARGE') {
  if (typeof source !== 'string') fail('PLAN_INPUT_INVALID');
  if (Buffer.byteLength(source, 'utf8') > maxBytes) fail(sizeCode);
  try {
    return parseBoundedJson(source, {
      maxBytes,
      maxDepth,
      maxNodes: 8_192,
      integerNumbersOnly: true
    });
  } catch (error) {
    if (error?.code === 'JSON_TOO_DEEP') fail('PLAN_TOO_DEEP');
    if (error?.code === 'JSON_TOO_LARGE') fail('PLAN_BOUNDS_EXCEEDED');
    fail('PLAN_INPUT_INVALID');
  }
}

function canonicalJson(value) {
  if (value === null || typeof value === 'boolean' || typeof value === 'string') {
    return JSON.stringify(value);
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) fail('PLAN_ARGUMENT_INVALID');
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    const parts = [];
    for (let index = 0; index < value.length; index += 1) {
      parts.push(canonicalJson(value[index]));
    }
    return '[' + parts.join(',') + ']';
  }
  if (value === null || typeof value !== 'object') fail('PLAN_ARGUMENT_INVALID');
  const keys = Object.keys(value).sort();
  const parts = [];
  for (const key of keys) {
    if (FORBIDDEN_KEYS.has(key)) fail('PLAN_ARGUMENT_INVALID');
    parts.push(JSON.stringify(key) + ':' + canonicalJson(value[key]));
  }
  return '{' + parts.join(',') + '}';
}

function deepFreeze(value) {
  if (value === null || typeof value !== 'object' || Object.isFrozen(value)) return value;
  for (const key of Reflect.ownKeys(value)) deepFreeze(value[key]);
  return Object.freeze(value);
}

function isReference(value) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const keys = Object.keys(value);
  return keys.length === 1 && keys[0] === '$result' && typeof value.$result === 'string';
}

function outputTypeFor(operationKey) {
  const info = inspectOperation(createDefaultRegistry(), operationKey);
  if (info.output === 'finite-safe-integer') return 'number';
  if (info.output === 'bounded-string') return 'string';
  return null;
}

function validateExpression(value, expectedType, stepsById, dependencies) {
  if (isReference(value)) {
    const sourceId = value.$result;
    const source = stepsById.get(sourceId);
    if (!source || !dependencies.has(sourceId)) fail('PLAN_DEPENDENCY_INVALID');
    if (outputTypeFor(source.operation) !== expectedType) fail('PLAN_ARGUMENT_INVALID');
    return;
  }
  if (expectedType === 'number') {
    if (!Number.isSafeInteger(value) || Math.abs(value) > 1_000_000_000) {
      fail('PLAN_ARGUMENT_INVALID');
    }
    return;
  }
  if (expectedType === 'string') {
    if (typeof value !== 'string' || value.length > 512) fail('PLAN_ARGUMENT_INVALID');
    return;
  }
  fail('PLAN_ARGUMENT_INVALID');
}

function validateOperationArguments(step, stepsById) {
  const operationInfo = inspectOperation(createDefaultRegistry(), step.operation);
  const argumentsValue = step.args;
  const dependencies = new Set(step.dependsOn);
  if (step.operation === 'number.add') {
    const args = exactRecord(argumentsValue, new Set(['left', 'right']), ['left', 'right'], 'PLAN_ARGUMENT_INVALID');
    validateExpression(args.left, 'number', stepsById, dependencies);
    validateExpression(args.right, 'number', stepsById, dependencies);
    return;
  }
  if (step.operation === 'text.concat') {
    const args = exactRecord(argumentsValue, new Set(['parts']), ['parts'], 'PLAN_ARGUMENT_INVALID');
    if (!Array.isArray(args.parts) || args.parts.length < 1 || args.parts.length > 32) {
      fail('PLAN_ARGUMENT_INVALID');
    }
    for (let index = 0; index < args.parts.length; index += 1) {
      validateExpression(args.parts[index], 'string', stepsById, dependencies);
    }
    return;
  }
  if (operationInfo.status === 'unavailable') fail('PLAN_EFFECT_UNAVAILABLE');
  fail('PLAN_UNKNOWN_OPERATION');
}

function prepareSchedule(steps, limits) {
  const remaining = new Map(steps.map(step => [step.id, step]));
  const completed = new Set();
  const schedule = [];
  const depthById = new Map();

  while (remaining.size > 0) {
    const ready = [];
    for (const step of remaining.values()) {
      if (step.dependsOn.every(dependency => completed.has(dependency))) ready.push(step);
    }
    ready.sort((left, right) => (left.id < right.id ? -1 : left.id > right.id ? 1 : 0));
    if (ready.length === 0) fail('PLAN_CYCLE');
    if (ready.length > limits.maxReadyQueue) fail('PLAN_BOUNDS_EXCEEDED');

    const step = ready[0];
    let depth = 1;
    for (const dependency of step.dependsOn) {
      depth = Math.max(depth, (depthById.get(dependency) ?? 0) + 1);
    }
    if (depth > limits.maxDepth) fail('PLAN_BOUNDS_EXCEEDED');
    depthById.set(step.id, depth);
    schedule.push(step.id);
    completed.add(step.id);
    remaining.delete(step.id);
    if (schedule.length > limits.maxIterations) fail('PLAN_BOUNDS_EXCEEDED');
  }
  return schedule;
}

function getPlanData(plan) {
  const data = PLAN_HANDLES.get(plan);
  if (!data) fail('PLAN_REGISTRY_INVALID');
  return data;
}

function freezeResultRecord(input) {
  return deepFreeze({
    stepId: input.stepId,
    planHash: input.planHash,
    registryVersion: input.registryVersion,
    runIdentity: input.runIdentity,
    inputHash: input.inputHash,
    resultHash: input.resultHash,
    output: input.output
  });
}

function resultPayload(stepId, planHash, registryVersion, runIdentity, inputHash, output) {
  return {
    kind: 'agent-harness.private-local-result.v1',
    stepId,
    planHash,
    registryVersion,
    runIdentity,
    inputHash,
    output
  };
}

function inputPayload(stepId, planHash, registryVersion, runIdentity, resolvedArgs) {
  return {
    kind: 'agent-harness.private-local-input.v1',
    stepId,
    planHash,
    registryVersion,
    runIdentity,
    args: resolvedArgs
  };
}

function normalizedCompletedMap(planData, resultsJson) {
  const limits = getLimitValues(planData.limitSet);
  const parsed = parseJson(
    resultsJson,
    limits.maxResultsJsonBytes,
    limits.maxDepth + 4,
    'PLAN_RESULTS_INVALID'
  );
  const stepIds = new Set(planData.steps.map(step => step.id));
  const fields = exactRecord(parsed, stepIds, [], 'PLAN_RESULTS_INVALID');
  const results = new Map();
  let totalResultBytes = 0;
  const orderedKeys = Object.keys(fields).sort(
    (left, right) => planData.schedule.indexOf(left) - planData.schedule.indexOf(right)
  );

  for (const id of orderedKeys) {
    const step = planData.stepsById.get(id);
    if (!step) fail('PLAN_RESULT_IDENTITY_INVALID');
    const raw = exactRecord(fields[id], RESULT_KEYS, [...RESULT_KEYS], 'PLAN_RESULT_IDENTITY_INVALID');
    if (
      raw.stepId !== id
      || raw.planHash !== planData.planHash
      || raw.registryVersion !== planData.registryVersion
      || raw.runIdentity !== planData.runIdentity
      || typeof raw.inputHash !== 'string'
      || !/^sha256_[0-9a-f]{64}$/.test(raw.inputHash)
      || typeof raw.resultHash !== 'string'
      || !/^sha256_[0-9a-f]{64}$/.test(raw.resultHash)
    ) {
      fail('PLAN_RESULT_IDENTITY_INVALID');
    }

    for (const dependency of step.dependsOn) {
      if (!results.has(dependency)) fail('PLAN_RESULTS_INVALID');
    }
    const resolvedArgs = resolveValue(step.args, results, planData);
    const argsBytes = Buffer.byteLength(canonicalJson(resolvedArgs), 'utf8');
    if (argsBytes > limits.maxInputBytes) fail('PLAN_ARGUMENT_TOO_LARGE');
    const inputHash = 'sha256_' + sha256(canonicalJson(inputPayload(
      step.id,
      planData.planHash,
      planData.registryVersion,
      planData.runIdentity,
      resolvedArgs
    )));
    if (raw.inputHash !== inputHash || !validOutput(step.operation, raw.output)) {
      fail('PLAN_RESULT_IDENTITY_INVALID');
    }
    const computed = executeLocalOperation(planData.registry, step.operation, resolvedArgs);
    if (!computed.ok || !validOutput(step.operation, computed.value)) {
      fail('PLAN_RESULT_IDENTITY_INVALID');
    }
    const resultBytes = Buffer.byteLength(JSON.stringify(computed.value), 'utf8');
    if (resultBytes > limits.maxResultBytes || totalResultBytes + resultBytes > limits.maxTotalResultBytes) {
      fail('PLAN_RESULT_TOO_LARGE');
    }
    totalResultBytes += resultBytes;
    if (!Object.is(raw.output, computed.value)) fail('PLAN_RESULT_IDENTITY_INVALID');
    const expectedResultHash = 'sha256_' + sha256(canonicalJson(resultPayload(
      step.id,
      planData.planHash,
      planData.registryVersion,
      planData.runIdentity,
      inputHash,
      raw.output
    )));
    if (raw.resultHash !== expectedResultHash) fail('PLAN_RESULT_IDENTITY_INVALID');
    results.set(id, freezeResultRecord({ ...raw, output: computed.value }));
  }
  return { results, totalResultBytes };
}

function resolveValue(value, results, planData) {
  if (isReference(value)) {
    const record = results.get(value.$result);
    if (
      !record
      || record.planHash !== planData.planHash
      || record.registryVersion !== planData.registryVersion
    ) {
      fail('PLAN_RESULTS_INVALID');
    }
    return record.output;
  }
  if (Array.isArray(value)) {
    const result = [];
    Object.setPrototypeOf(result, null);
    for (let index = 0; index < value.length; index += 1) {
      result[result.length] = resolveValue(value[index], results, planData);
    }
    return result;
  }
  if (value !== null && typeof value === 'object') {
    const result = Object.create(null);
    for (const key of Object.keys(value).sort()) result[key] = resolveValue(value[key], results, planData);
    return result;
  }
  return value;
}

function validOutput(operationKey, output) {
  if (operationKey === 'number.add') {
    return Number.isSafeInteger(output) && Math.abs(output) <= 2_000_000_000;
  }
  if (operationKey === 'text.concat') {
    return typeof output === 'string' && Buffer.byteLength(output, 'utf8') <= 2_048;
  }
  return false;
}

function nextReadyStep(planData, results) {
  const ready = planData.steps
    .filter(step => !results.has(step.id) && step.dependsOn.every(id => results.has(id)))
    .map(step => step.id)
    .sort((left, right) => (left < right ? -1 : left > right ? 1 : 0));
  return ready[0] ?? null;
}

function makeStepRecord(planData, step, resolvedArgs) {
  const limits = getLimitValues(planData.limitSet);
  const argsBytes = Buffer.byteLength(canonicalJson(resolvedArgs), 'utf8');
  if (argsBytes > limits.maxInputBytes) fail('PLAN_ARGUMENT_TOO_LARGE');

  const computed = executeLocalOperation(planData.registry, step.operation, resolvedArgs);
  if (!computed.ok) {
    if (computed.code === 'EFFECT_UNAVAILABLE') fail('PLAN_EFFECT_UNAVAILABLE');
    if (computed.code === 'UNKNOWN_OPERATION') fail('PLAN_UNKNOWN_OPERATION');
    fail('PLAN_ARGUMENT_INVALID');
  }
  const output = computed.value;
  if (!validOutput(step.operation, output)) fail('PLAN_RESULT_INVALID');
  const outputBytes = Buffer.byteLength(canonicalJson(output), 'utf8');
  if (outputBytes > limits.maxResultBytes) fail('PLAN_RESULT_TOO_LARGE');

  const inputHash = 'sha256_' + sha256(canonicalJson(inputPayload(
    step.id,
    planData.planHash,
    planData.registryVersion,
      planData.runIdentity,
    resolvedArgs
  )));
  const resultHash = 'sha256_' + sha256(canonicalJson(resultPayload(
    step.id,
    planData.planHash,
    planData.registryVersion,
    planData.runIdentity,
    inputHash,
    output
  )));
  return freezeResultRecord({
    stepId: step.id,
    planHash: planData.planHash,
    registryVersion: planData.registryVersion,
    runIdentity: planData.runIdentity,
    inputHash,
    resultHash,
    output
  });
}

export function compilePlan(
  planJson,
  limitSet = DEFAULT_LIMIT_SET,
  registry = createDefaultRegistry(),
  workflowId = 'synthetic-local-run'
) {
  let limits;
  try {
    limits = getLimitValues(limitSet);
  } catch {
    fail('PLAN_REGISTRY_INVALID');
  }
  if (!isDefaultRegistry(registry)) fail('PLAN_REGISTRY_INVALID');
  if (typeof workflowId !== 'string' || workflowId.trim().length === 0 || workflowId.length > 512) {
    fail('PLAN_REGISTRY_INVALID');
  }
  const parsed = parseJson(planJson, limits.maxPlanBytes, limits.maxDepth, 'PLAN_TOO_LARGE');
  const sourceFields = exactRecord(
    parsed,
    new Set(['kind', 'version', 'steps']),
    ['kind', 'version', 'steps']
  );
  if (sourceFields.kind !== PLAN_KIND || sourceFields.version !== 1) {
    fail('PLAN_UNSUPPORTED_VERSION');
  }
  if (
    !Array.isArray(sourceFields.steps)
    || sourceFields.steps.length < 1
    || sourceFields.steps.length > limits.maxSteps
  ) {
    fail('PLAN_BOUNDS_EXCEEDED');
  }

  const rawSteps = [];
  const seenIds = new Set();
  let edgeCount = 0;
  for (const rawStep of sourceFields.steps) {
    const fields = exactRecord(
      rawStep,
      new Set(['id', 'operation', 'dependsOn', 'args']),
      ['id', 'operation', 'dependsOn', 'args']
    );
    if (
      typeof fields.id !== 'string'
      || !STEP_ID.test(fields.id)
      || seenIds.has(fields.id)
      || typeof fields.operation !== 'string'
      || fields.operation.length > 64
      || !Array.isArray(fields.dependsOn)
    ) {
      fail('PLAN_INVALID_SHAPE');
    }
    seenIds.add(fields.id);

    const operation = inspectOperation(registry, fields.operation);
    if (operation.status === 'unknown-operation') fail('PLAN_UNKNOWN_OPERATION');
    if (operation.status !== 'available' || operation.effect !== 'local-pure') {
      fail('PLAN_EFFECT_UNAVAILABLE');
    }

    if (fields.dependsOn.length > limits.maxSteps) fail('PLAN_BOUNDS_EXCEEDED');
    const dependencies = [];
    const dependencySet = new Set();
    for (const dependency of fields.dependsOn) {
      if (
        typeof dependency !== 'string'
        || !STEP_ID.test(dependency)
        || dependency === fields.id
        || dependencySet.has(dependency)
      ) {
        fail('PLAN_DEPENDENCY_INVALID');
      }
      dependencySet.add(dependency);
      dependencies.push(dependency);
    }
    edgeCount += dependencies.length;
    if (edgeCount > limits.maxEdges) fail('PLAN_BOUNDS_EXCEEDED');

    const args = cloneJson(fields.args, limits);
    rawSteps.push({
      id: fields.id,
      operation: fields.operation,
      dependsOn: dependencies.sort(),
      args
    });
  }

  if (rawSteps.length > limits.maxIterations) fail('PLAN_BOUNDS_EXCEEDED');
  const steps = rawSteps.slice().sort((left, right) => (left.id < right.id ? -1 : left.id > right.id ? 1 : 0));
  const stepsById = new Map(steps.map(step => [step.id, step]));
  for (const step of steps) {
    for (const dependency of step.dependsOn) {
      if (!stepsById.has(dependency)) fail('PLAN_DEPENDENCY_INVALID');
    }
    validateOperationArguments(step, stepsById);
    if (Buffer.byteLength(canonicalJson(step.args), 'utf8') > limits.maxInputBytes) {
      fail('PLAN_ARGUMENT_TOO_LARGE');
    }
  }

  const schedule = prepareSchedule(steps, limits);
  const canonicalPlan = {
    kind: PLAN_KIND,
    version: 1,
    steps: steps.map(step => ({
      id: step.id,
      operation: step.operation,
      dependsOn: step.dependsOn,
      args: step.args
    }))
  };
  const canonical = canonicalJson(canonicalPlan);
  const planHash = 'plan1_' + sha256(canonical);
  const registryVersion = getRegistryVersion(registry);
  const runIdentity = 'run1_' + sha256(canonicalJson([
    workflowId,
    planHash,
    registryVersion,
    limitSet.hash
  ]));
  const planData = Object.freeze({
    canonical,
    planHash,
    registryVersion,
    runIdentity,
    workflowId,
    registry,
    limitSet,
    steps: Object.freeze(steps.map(step => deepFreeze(step))),
    stepsById,
    schedule: Object.freeze(schedule)
  });
  const handle = Object.freeze({
    kind: PLAN_KIND,
    version: 1,
    planHash,
    registryVersion,
    runIdentity,
    limitSetHash: limitSet.hash,
    stepCount: steps.length
  });
  PLAN_HANDLES.set(handle, planData);
  return handle;
}

export function getPlanStepIds(plan) {
  const data = getPlanData(plan);
  return Object.freeze(data.steps.map(step => step.id));
}

export function validateCompletedResults(plan, completedResultsJson = '{}') {
  const data = getPlanData(plan);
  const normalized = normalizedCompletedMap(data, completedResultsJson);
  return Object.freeze({
    stepIds: Object.freeze(data.schedule.filter(id => normalized.results.has(id))),
    resultCount: normalized.results.size,
    totalResultBytes: normalized.totalResultBytes
  });
}

export function getNextReadyStep(plan, completedResultsJson = '{}') {
  const data = getPlanData(plan);
  const normalized = normalizedCompletedMap(data, completedResultsJson);
  return nextReadyStep(data, normalized.results);
}

export function executeLocalStep(plan, stepId, completedResultsJson = '{}') {
  const data = getPlanData(plan);
  if (typeof stepId !== 'string' || !data.stepsById.has(stepId)) fail('PLAN_STEP_NOT_READY');
  const normalized = normalizedCompletedMap(data, completedResultsJson);
  const results = normalized.results;
  if (results.has(stepId)) fail('PLAN_STEP_ALREADY_COMPLETE');
  const nextStepId = nextReadyStep(data, results);
  if (nextStepId !== stepId) fail('PLAN_STEP_NOT_READY');
  const step = data.stepsById.get(stepId);
  const resolvedArgs = resolveValue(step.args, results, data);
  return makeStepRecord(data, step, resolvedArgs);
}

export function executePlanLocally(
  planOrJson,
  limitSet = DEFAULT_LIMIT_SET,
  workflowId = 'synthetic-local-run'
) {
  const plan = typeof planOrJson === 'string'
    ? compilePlan(planOrJson, limitSet, createDefaultRegistry(), workflowId)
    : planOrJson;
  const data = getPlanData(plan);
  const limits = getLimitValues(data.limitSet);
  const wallStart = Date.now();
  const monotonicStart = performance.now();
  const budgetLimitMs = Math.min(limits.maxElapsedMs, limits.maxStepMs * data.steps.length);
  const deadline = wallStart + budgetLimitMs;
  let budget = createBudgetSnapshot(data.limitSet, wallStart, deadline);
  const completed = Object.create(null);
  let totalResultBytes = 0;

  for (const stepId of data.schedule) {
    const elapsed = Math.max(0, Math.floor(performance.now() - monotonicStart));
    const wallNow = wallStart + elapsed;
    const admission = advanceBudget(budget, 'step', wallNow, elapsed, data.limitSet);
    if (!admission.allowed) {
      if (admission.reason.startsWith('CLOCK_')) fail('PLAN_CLOCK_STOP');
      fail('PLAN_LIMIT_STOP');
    }
    budget = admission.state;

    const stepStarted = performance.now();
    const resultRecord = executeLocalStep(plan, stepId, JSON.stringify(completed));
    const durationMs = Math.max(0, Math.ceil(performance.now() - stepStarted));
    if (!checkStepDuration(durationMs, data.limitSet).allowed) fail('PLAN_LIMIT_STOP');
    const elapsedAfterCompute = Math.max(0, Math.floor(performance.now() - monotonicStart));
    const clockReason = checkClockBudget(
      budget,
      wallStart + elapsedAfterCompute,
      elapsedAfterCompute,
      data.limitSet
    );
    if (clockReason) {
      if (clockReason.startsWith('CLOCK_')) fail('PLAN_CLOCK_STOP');
      fail('PLAN_LIMIT_STOP');
    }

    const resultBytes = Buffer.byteLength(canonicalJson(resultRecord.output), 'utf8');
    const byteRecord = recordResultBytes(budget, resultBytes, data.limitSet);
    if (!byteRecord.allowed) fail('PLAN_RESULT_TOO_LARGE');
    budget = byteRecord.state;
    completed[stepId] = resultRecord;
    totalResultBytes += resultBytes;
  }

  const records = data.schedule.map(stepId => completed[stepId]);
  const output = {
    kind: 'agent-harness.private-synthetic-local-result',
    version: 1,
    status: 'completed',
    planHash: data.planHash,
    registryVersion: data.registryVersion,
    runIdentity: data.runIdentity,
    limitSetHash: data.limitSet.hash,
    results: records,
    stoppingFacts: {
      stepsAdmitted: budget.stepsAdmitted,
      stepAttempts: budget.stepAttempts,
      localRetries: budget.localRetries,
      replans: budget.replans,
      iterations: budget.iterations,
      totalResultBytes
    }
  };
  const resultHash = 'run1_' + sha256(canonicalJson(output));
  const finalElapsed = Math.max(0, Math.floor(performance.now() - monotonicStart));
  const finalClockReason = checkClockBudget(
    budget,
    wallStart + finalElapsed,
    finalElapsed,
    data.limitSet
  );
  if (finalClockReason) {
    if (finalClockReason.startsWith('CLOCK_')) fail('PLAN_CLOCK_STOP');
    fail('PLAN_LIMIT_STOP');
  }
  return deepFreeze({ ...output, resultHash });
}

export function validatePrivateCheckpointResults(plan, completedResultsJson) {
  return validateCompletedResults(plan, completedResultsJson);
}

/**
 * Return an upper bound for the canonical completed-results JSON byte length.
 * The bound reserves the configured result budget and fixed identity metadata,
 * without running any unadmitted operation or treating a placeholder as a result.
 */
export function getMaximumCompletedResultsJsonBytes(plan, completedResultsJson = '{}') {
  const data = getPlanData(plan);
  const limits = getLimitValues(data.limitSet);
  const normalized = normalizedCompletedMap(data, completedResultsJson);
  const completed = JSON.parse(completedResultsJson);
  let minimumPendingBytes = 0;
  let additionalCapacity = 0;
  const projected = Object.create(null);
  for (const stepId of data.schedule) {
    if (Object.hasOwn(completed, stepId)) {
      projected[stepId] = completed[stepId];
      continue;
    }

    const step = data.stepsById.get(stepId);
    const isText = step.operation === 'text.concat';
    const minimumOutputBytes = isText ? 2 : 1;
    const maximumOutputBytes = isText
      ? limits.maxResultBytes
      : Math.min(limits.maxResultBytes, 11);
    if (maximumOutputBytes < minimumOutputBytes) fail('PLAN_RESULT_TOO_LARGE');

    minimumPendingBytes += minimumOutputBytes;
    additionalCapacity += maximumOutputBytes - minimumOutputBytes;
    projected[stepId] = {
      stepId,
      planHash: data.planHash,
      registryVersion: data.registryVersion,
      runIdentity: data.runIdentity,
      inputHash: 'sha256_' + '0'.repeat(64),
      resultHash: 'sha256_' + '0'.repeat(64),
      output: isText ? '' : 0
    };
  }

  const minimumMapBytes = Buffer.byteLength(JSON.stringify(projected), 'utf8');
  if (minimumMapBytes > limits.maxResultsJsonBytes) fail('PLAN_RESULT_TOO_LARGE');
  const remainingTotalBytes = limits.maxTotalResultBytes - normalized.totalResultBytes;
  const resultCapacityAfterMinimum = remainingTotalBytes - minimumPendingBytes;
  if (resultCapacityAfterMinimum < 0) fail('PLAN_RESULT_TOO_LARGE');
  const additionalBytes = Math.min(additionalCapacity, resultCapacityAfterMinimum);
  return Math.min(limits.maxResultsJsonBytes, minimumMapBytes + additionalBytes);
}

export const PRIVATE_PLAN_KIND = PLAN_KIND;
