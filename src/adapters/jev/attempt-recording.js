import { randomUUID } from 'node:crypto';
import { isPlainDataRecord } from './safe-json.js';

const REF_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/u;
const MODEL_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/u;
const UNKNOWN_USAGE = Object.freeze({ kind: 'unknown', reason: 'missing_or_invalid' });

function readDataRecord(value, requiredKeys, optionalKeys = []) {
  if (!isPlainDataRecord(value)) return null;
  let keys;
  try {
    keys = Reflect.ownKeys(value);
  } catch {
    return null;
  }
  const allowed = new Set([...requiredKeys, ...optionalKeys]);
  if (keys.length > allowed.size || keys.some(key => typeof key !== 'string' || !allowed.has(key))) return null;
  const result = Object.create(null);
  for (const key of keys) {
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      return null;
    }
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) return null;
    result[key] = descriptor.value;
  }
  if (requiredKeys.some(key => !Object.hasOwn(result, key))) return null;
  return result;
}

function reference(value) {
  return typeof value === 'string' && REF_PATTERN.test(value) ? value : null;
}

function safeUsage(value) {
  const fields = readDataRecord(value, ['kind'], ['inputTokens', 'outputTokens']);
  if (!fields || fields.kind !== 'known') return UNKNOWN_USAGE;
  const keys = Reflect.ownKeys(fields);
  if (keys.length !== 3
      || !Number.isSafeInteger(fields.inputTokens) || fields.inputTokens < 0
      || !Number.isSafeInteger(fields.outputTokens) || fields.outputTokens < 0) {
    return UNKNOWN_USAGE;
  }
  return Object.freeze({
    kind: 'known',
    inputTokens: fields.inputTokens,
    outputTokens: fields.outputTokens
  });
}

function safeRequestId(value) {
  return typeof value === 'string' && /^[A-Za-z0-9._:-]{1,128}$/u.test(value) ? value : null;
}

export function createLocalAttemptObservation(input) {
  const fields = readDataRecord(input, [
    'logicalOperationRef', 'billableAttemptRef', 'transportAttemptRef',
    'requestedModel', 'dispatchCertainty'
  ], ['responseValidation', 'providerRequestId']);
  if (!fields
      || !reference(fields.logicalOperationRef)
      || !reference(fields.billableAttemptRef)
      || !reference(fields.transportAttemptRef)
      || typeof fields.requestedModel !== 'string'
      || !MODEL_PATTERN.test(fields.requestedModel)
      || !['not_dispatched', 'possible', 'response_received'].includes(fields.dispatchCertainty)) {
    throw new TypeError('A local JEV observation requires bounded opaque references and dispatch state.');
  }

  const validation = Object.hasOwn(fields, 'responseValidation')
    ? readDataRecord(fields.responseValidation, ['kind', 'usage', 'requestedModel', 'resolvedModel'], ['code', 'answers'])
    : null;
  if (Object.hasOwn(fields, 'responseValidation') && !validation) {
    throw new TypeError('The local JEV observation response summary is invalid.');
  }
  if (fields.dispatchCertainty === 'response_received' && !validation) {
    throw new TypeError('A received JEV response summary is required.');
  }
  const usage = validation ? safeUsage(validation.usage) : UNKNOWN_USAGE;
  const resolvedModel = typeof validation?.resolvedModel === 'string' && MODEL_PATTERN.test(validation.resolvedModel)
    ? validation.resolvedModel
    : null;
  const decisionConformance = validation?.kind === 'valid'
      && validation.requestedModel === fields.requestedModel
      && usage.kind === 'known'
    ? 'valid'
    : validation
      ? 'invalid'
      : 'unavailable';
  const providerRequestId = safeRequestId(fields.providerRequestId);
  const usageRecord = usage.kind === 'known'
    ? Object.freeze({ kind: 'known', inputTokens: usage.inputTokens, outputTokens: usage.outputTokens })
    : UNKNOWN_USAGE;

  return Object.freeze({
    schema: 'agent-harness.jev.local-usage-observation.v1',
    persistence: 'memory_only',
    observationId: randomUUID(),
    logicalOperationRef: fields.logicalOperationRef,
    billableAttemptRef: fields.billableAttemptRef,
    transportAttemptRef: fields.transportAttemptRef,
    dispatchCertainty: fields.dispatchCertainty,
    requestedModel: fields.requestedModel,
    resolvedModel,
    providerRequestId,
    decisionConformance,
    usage: usageRecord
  });
}
