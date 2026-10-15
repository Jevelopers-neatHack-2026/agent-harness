import { types as utilTypes } from 'node:util';
import { createLimitSet, getLimitValues } from '../../orchestrator/internal/limits.js';
import { compilePlan } from '../../orchestrator/internal/plan.js';
import { createDefaultRegistry } from '../../tools/internal/registry.js';

const DEFAULT_CONFIG = Object.freeze({
  mode: 'mock',
  workerCount: 4,
  maxQueueRecords: 32,
  maxQueueBytes: 2_097_152,
  maxPerTenantRecords: 16,
  maxPerTenantBytes: 1_048_576,
  maxPerProjectRecords: 8,
  maxPerProjectBytes: 524_288,
  maxTenants: 16,
  maxProjects: 128,
  maxInFlightGlobal: 4,
  maxInFlightTenant: 2,
  maxInFlightProject: 1,
  maxEventBacklog: 4_096,
  maxStoreBytes: 268_435_456,
  maxWalBytes: 16_777_216,
  minFreeDiskBytes: 16_777_216,
  completionHeadroomBytes: 524_288,
  maxJobBytes: 36_864,
  maxResultBytes: 65_536,
  maxIpcBytes: 0,
  maxWaiters: 16,
  busyTimeoutMs: 20,
  leaseTtlMs: 50_000,
  maxJobRuntimeMs: 35_000,
  shutdownTimeoutMs: 6_000,
  childKillGraceMs: 350,
  childReapReserveMs: 1_000,
  pollIntervalMs: 40,
  limitsVersion: 'ops-limits.v1'
});

const ALLOWED_KEYS = new Set(Object.keys(DEFAULT_CONFIG));
const INT_RANGES = Object.freeze({
  workerCount: [1, 8],
  maxQueueRecords: [2, 128],
  maxQueueBytes: [65_536, 67_108_864],
  maxPerTenantRecords: [1, 64],
  maxPerTenantBytes: [32_768, 33_554_432],
  maxPerProjectRecords: [1, 64],
  maxPerProjectBytes: [16_384, 16_777_216],
  maxTenants: [2, 128],
  maxProjects: [2, 512],
  maxInFlightGlobal: [1, 8],
  maxInFlightTenant: [1, 8],
  maxInFlightProject: [1, 8],
  maxEventBacklog: [192, 65_536],
  maxStoreBytes: [16_777_216, 1_073_741_824],
  maxWalBytes: [1_048_576, 134_217_728],
  minFreeDiskBytes: [1_048_576, 1_073_741_824],
  completionHeadroomBytes: [65_536, 4_194_304],
  maxJobBytes: [256, 65_536],
  maxResultBytes: [1, 65_536],
  maxIpcBytes: [0, 0],
  maxWaiters: [1, 128],
  busyTimeoutMs: [1, 250],
  leaseTtlMs: [1_000, 300_000],
  maxJobRuntimeMs: [100, 60_000],
  shutdownTimeoutMs: [1_000, 30_000],
  childKillGraceMs: [25, 5_000],
  childReapReserveMs: [100, 5_000],
  pollIntervalMs: [5, 1_000]
});

function invalid(message = 'The operations configuration is invalid.') {
  const error = new TypeError(message);
  error.code = 'OPS_CONFIG_INVALID';
  return error;
}

function ownDataRecord(value, allowedKeys) {
  if (value === undefined) return Object.create(null);
  if (value === null || typeof value !== 'object' || utilTypes.isProxy(value) || Array.isArray(value)) {
    throw invalid();
  }
  let prototype;
  let keys;
  try {
    prototype = Object.getPrototypeOf(value);
    keys = Reflect.ownKeys(value);
  } catch {
    throw invalid();
  }
  if ((prototype !== Object.prototype && prototype !== null) || keys.length > allowedKeys.size) {
    throw invalid();
  }
  const result = Object.create(null);
  for (const key of keys) {
    if (typeof key !== 'string' || !allowedKeys.has(key)) throw invalid();
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      throw invalid();
    }
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) throw invalid();
    result[key] = descriptor.value;
  }
  return result;
}

function ownDataOptions(value) {
  return ownDataRecord(value, ALLOWED_KEYS);
}

export function readOperationsConfig(input) {
  const supplied = ownDataOptions(input);
  const config = { ...DEFAULT_CONFIG, ...supplied };
  if (config.mode !== 'mock') throw invalid('Only explicit mock local operations are available.');
  if (typeof config.limitsVersion !== 'string'
      || !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$/u.test(config.limitsVersion)) throw invalid();
  for (const [key, [minimum, maximum]] of Object.entries(INT_RANGES)) {
    if (!Number.isSafeInteger(config[key]) || config[key] < minimum || config[key] > maximum) {
      throw invalid();
    }
  }
  if (
    config.workerCount < config.maxInFlightGlobal
    || config.maxInFlightGlobal > config.workerCount
    || config.maxInFlightTenant > config.maxInFlightGlobal
    || config.maxInFlightProject > config.maxInFlightTenant
    || config.maxPerTenantRecords > Math.floor(config.maxQueueRecords / 2)
    || config.maxPerTenantBytes > Math.floor(config.maxQueueBytes / 2)
    || config.maxPerProjectRecords > config.maxPerTenantRecords
    || config.maxPerProjectBytes > config.maxPerTenantBytes
    || config.maxEventBacklog < config.maxQueueRecords * 96
    || config.leaseTtlMs < config.maxJobRuntimeMs + config.shutdownTimeoutMs
      + config.childKillGraceMs + config.busyTimeoutMs
    || config.shutdownTimeoutMs <= config.childKillGraceMs + config.childReapReserveMs
  ) throw invalid('The operations configuration violates a capacity or deadline relationship.');
  return Object.freeze(config);
}

export function operationsStoragePolicy(configInput) {
  const config = readOperationsConfig(configInput);
  return Object.freeze({
    maxQueueRecords: config.maxQueueRecords,
    maxQueueBytes: config.maxQueueBytes,
    maxPerTenantRecords: config.maxPerTenantRecords,
    maxPerTenantBytes: config.maxPerTenantBytes,
    maxPerProjectRecords: config.maxPerProjectRecords,
    maxPerProjectBytes: config.maxPerProjectBytes,
    maxTenants: config.maxTenants,
    maxProjects: config.maxProjects,
    maxInFlightGlobal: config.maxInFlightGlobal,
    maxInFlightTenant: config.maxInFlightTenant,
    maxInFlightProject: config.maxInFlightProject,
    maxEventBacklog: config.maxEventBacklog,
    maxStoreBytes: config.maxStoreBytes,
    maxWalBytes: config.maxWalBytes,
    minFreeDiskBytes: config.minFreeDiskBytes,
    completionHeadroomBytes: config.completionHeadroomBytes,
    maxJobBytes: config.maxJobBytes,
    busyTimeoutMs: config.busyTimeoutMs,
    leaseTtlMs: config.leaseTtlMs,
    limitsVersion: config.limitsVersion
  });
}

export function validateLocalPlanWork(input, workflowId, configInput) {
  const config = readOperationsConfig(configInput);
  const value = ownDataRecord(input, new Set(['limitsJson', 'planJson']));
  if (Object.keys(value).sort().join(',') !== 'limitsJson,planJson') throw invalid('The local plan shape is invalid.');
  if (typeof workflowId !== 'string' || workflowId.length > 512 || workflowId.trim().length === 0
      || typeof value.planJson !== 'string' || Buffer.byteLength(value.planJson, 'utf8') > 32_768
      || typeof value.limitsJson !== 'string' || Buffer.byteLength(value.limitsJson, 'utf8') > 2_048) {
    throw invalid('The local plan exceeds its input bounds.');
  }
  let limits;
  try {
    limits = createLimitSet(value.limitsJson);
    const bounds = getLimitValues(limits);
    if (bounds.maxTotalResultBytes > config.maxResultBytes
        || bounds.maxElapsedMs > config.maxJobRuntimeMs
        || bounds.maxSteps > 32
        || bounds.maxStepAttempts > 64) throw invalid('The local plan exceeds the operations runtime budget.');
    compilePlan(value.planJson, limits, createDefaultRegistry(), workflowId);
  } catch (error) {
    if (error?.code === 'OPS_CONFIG_INVALID') throw error;
    throw invalid('The local plan failed deterministic validation.');
  }
  const work = Object.freeze({
    type: 'local_plan',
    version: 1,
    planJson: value.planJson,
    limitsJson: value.limitsJson
  });
  const workJson = JSON.stringify(work);
  if (Buffer.byteLength(workJson, 'utf8') > config.maxJobBytes) throw invalid('The local work request exceeds its byte cap.');
  return Object.freeze({ work, workJson, bytes: Buffer.byteLength(workJson, 'utf8') });
}

export const OPERATIONS_CONFIG_DEFAULTS = DEFAULT_CONFIG;
