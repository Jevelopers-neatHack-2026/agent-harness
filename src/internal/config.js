import { runtimeError } from './errors.js';

const REQUIRED = [
  ['HARNESS_MODE', 'mode'],
  ['HARNESS_MODEL_ID', 'modelId'],
  ['HARNESS_DEADLINE_MS', 'deadlineMs'],
  ['HARNESS_MAX_ATTEMPTS', 'maxAttempts'],
  ['HARNESS_MAX_ELAPSED_MS', 'maxElapsedMs'],
  ['HARNESS_MAX_COST_MINOR_UNITS', 'maxCostMinorUnits']
];

function invalid(operation, code, message) {
  throw runtimeError({
    module: 'config',
    operation,
    code,
    retryable: false,
    safe_message: message
  });
}

function required(env, key) {
  const value = env[key];
  if (typeof value !== 'string' || value.trim() === '') {
    invalid('parseRuntimeConfig', 'CONFIG_MISSING', 'Required setting ' + key + ' is missing.');
  }
  return value.trim();
}

function integerSetting(env, key, minimum, maximum) {
  const value = required(env, key);
  if (!/^(0|[1-9][0-9]*)$/.test(value)) {
    invalid('parseRuntimeConfig', 'CONFIG_INVALID', 'Setting ' + key + ' must be an integer.');
  }
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < minimum || number > maximum) {
    invalid('parseRuntimeConfig', 'CONFIG_INVALID', 'Setting ' + key + ' is outside its supported range.');
  }
  return number;
}

export function parseRuntimeConfig(env) {
  if (env === null || typeof env !== 'object') {
    invalid('parseRuntimeConfig', 'CONFIG_INVALID', 'Runtime settings must be supplied explicitly.');
  }

  for (const [key] of REQUIRED) required(env, key);
  const mode = env.HARNESS_MODE.trim();
  if (!['mock', 'recorded', 'sandbox', 'live'].includes(mode)) {
    invalid('parseRuntimeConfig', 'CONFIG_INVALID', 'HARNESS_MODE must name an explicit dependency mode.');
  }

  const modelId = required(env, 'HARNESS_MODEL_ID');
  if (
    modelId.length > 128
    || !/^[A-Za-z0-9][A-Za-z0-9._:/-]*$/.test(modelId)
    || ['latest', 'auto', '*'].includes(modelId.toLowerCase())
  ) {
    invalid('parseRuntimeConfig', 'CONFIG_INVALID', 'HARNESS_MODEL_ID must be an explicit model identifier.');
  }

  return Object.freeze({
    mode,
    modelId,
    deadlineMs: integerSetting(env, 'HARNESS_DEADLINE_MS', 1, 86_400_000),
    maxAttempts: integerSetting(env, 'HARNESS_MAX_ATTEMPTS', 1, 100),
    maxElapsedMs: integerSetting(env, 'HARNESS_MAX_ELAPSED_MS', 1, 604_800_000),
    maxCostMinorUnits: integerSetting(env, 'HARNESS_MAX_COST_MINOR_UNITS', 0, Number.MAX_SAFE_INTEGER)
  });
}

export function assertExecutionModeAvailable(config) {
  if (config?.mode === 'live') {
    throw runtimeError({
      module: 'config',
      operation: 'assertExecutionModeAvailable',
      code: 'LIVE_EXECUTION_UNAVAILABLE',
      retryable: false,
      safe_message: 'Live execution is unavailable until canonical ports and external authorities are integrated.'
    });
  }
  if (!['mock', 'recorded', 'sandbox'].includes(config?.mode)) {
    invalid('assertExecutionModeAvailable', 'CONFIG_INVALID', 'An explicit supported dependency mode is required.');
  }
  return true;
}
