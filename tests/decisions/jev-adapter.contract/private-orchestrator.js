import { createHash } from 'node:crypto';
import { types as utilTypes } from 'node:util';
import { JevTransportError } from '../../../src/adapters/jev/http-transport.js';

const MODEL_ID = 'jev-1.13.0';
const SAFE_MODEL = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/u;
const SAFE_REQUEST_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const SAFE_OBSERVATION_IDENTITY = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/u;

function isSafeObservationIdentity(value) {
  return typeof value === 'string'
    && value.trim() === value
    && SAFE_OBSERVATION_IDENTITY.test(value);
}

function ownDataFields(value, allowedKeys, requiredKeys = allowedKeys) {
  try {
    if (value === null || typeof value !== 'object' || Array.isArray(value) || utilTypes.isProxy(value)) return null;
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) return null;
    const keys = Reflect.ownKeys(value);
    if (keys.length > allowedKeys.length
        || keys.some(key => typeof key !== 'string' || !allowedKeys.includes(key))) return null;
    const fields = Object.create(null);
    for (const key of keys) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) return null;
      fields[key] = descriptor.value;
    }
    if (requiredKeys.some(key => !Object.hasOwn(fields, key))) return null;
    return fields;
  } catch {
    return null;
  }
}

function safeUsage(value) {
  const fields = ownDataFields(value, ['kind', 'inputTokens', 'outputTokens', 'reason'], ['kind']);
  if (!fields) return null;
  if (fields.kind === 'known'
      && Object.keys(fields).length === 3
      && Number.isSafeInteger(fields.inputTokens) && fields.inputTokens >= 0
      && Number.isSafeInteger(fields.outputTokens) && fields.outputTokens >= 0) {
    return Object.freeze({
      kind: 'known', inputTokens: fields.inputTokens, outputTokens: fields.outputTokens
    });
  }
  if (fields.kind === 'unknown' && Object.keys(fields).length === 2
      && fields.reason === 'missing_or_invalid') {
    return Object.freeze({ kind: 'unknown', reason: 'missing_or_invalid' });
  }
  return null;
}

function summarizeTransportResult(value) {
  const fields = ownDataFields(value, [
    'kind', 'status', 'validation', 'providerRequestId', 'dispatchCertainty', 'retryAfter'
  ], ['kind']);
  if (!fields) return null;
  const providerRequestId = typeof fields.providerRequestId === 'string'
      && SAFE_REQUEST_ID.test(fields.providerRequestId)
    ? fields.providerRequestId
    : null;
  if (fields.kind === 'http_error'
      && Number.isInteger(fields.status) && fields.status >= 100 && fields.status <= 599) {
    return Object.freeze({
      kind: 'http_error',
      status: fields.status,
      providerRequestId,
      dispatchCertainty: 'possible'
    });
  }
  if (fields.kind !== 'response') return null;
  const validation = ownDataFields(fields.validation, [
    'kind', 'code', 'usage', 'requestedModel', 'resolvedModel', 'answers'
  ], ['kind', 'usage', 'requestedModel', 'resolvedModel']);
  const usage = validation ? safeUsage(validation.usage) : null;
  if (!validation || !usage || validation.requestedModel !== MODEL_ID
      || !['valid', 'invalid'].includes(validation.kind)) return null;
  const resolvedModel = validation.resolvedModel === null
      || (typeof validation.resolvedModel === 'string' && SAFE_MODEL.test(validation.resolvedModel))
    ? validation.resolvedModel
    : null;
  return Object.freeze({
    kind: 'response',
    dispatchCertainty: 'response_received',
    providerRequestId,
    validation: Object.freeze({
      kind: validation.kind,
      requestedModel: MODEL_ID,
      resolvedModel,
      usage
    })
  });
}

function safeErrorCertainty(error) {
  try {
    if (error === null || typeof error !== 'object' || utilTypes.isProxy(error)
        || Object.getPrototypeOf(error) !== JevTransportError.prototype) return 'possible';
    const descriptor = Object.getOwnPropertyDescriptor(error, 'dispatchCertainty');
    return descriptor?.enumerable && Object.hasOwn(descriptor, 'value')
        && ['not_dispatched', 'possible'].includes(descriptor.value)
      ? descriptor.value
      : 'possible';
  } catch {
    return 'possible';
  }
}

function reference({ workflowId, logicalOperationRef, attemptId, transportAttemptId }) {
  const framedIdentity = JSON.stringify([
    'agent-harness.jev.test-observation-source.v2',
    workflowId,
    logicalOperationRef,
    attemptId,
    transportAttemptId,
    'provider-result'
  ]);
  return 'jev-observation:v2:' + createHash('sha256').update(framedIdentity, 'utf8').digest('hex');
}

function unknownReason(dispatchCertainty, hasResponse) {
  if (dispatchCertainty === 'not_dispatched') return 'not_dispatched';
  return hasResponse ? 'missing_or_invalid_usage' : 'possible_dispatch';
}

function makeObservation({ workflowId, attemptId, logicalOperationRef, transportAttemptId, result, error }) {
  const response = result?.kind === 'response' ? result.validation : null;
  const dispatchCertainty = result?.dispatchCertainty ?? (error ? safeErrorCertainty(error) : null);
  const certainty = ['not_dispatched', 'possible', 'response_received'].includes(dispatchCertainty)
    ? dispatchCertainty
    : 'possible';
  const usage = response?.usage?.kind === 'known'
    ? {
        kind: 'known',
        inputTokens: response.usage.inputTokens,
        outputTokens: response.usage.outputTokens
      }
    : {
        kind: 'unknown',
        reason: unknownReason(certainty, response !== null)
      };
  return {
    workflowId,
    attemptId,
    logicalOperationRef,
    transportAttemptId,
    sourceObservationRef: reference({ workflowId, logicalOperationRef, attemptId, transportAttemptId }),
    dispatchCertainty: certainty,
    requestedModel: MODEL_ID,
    resolvedModel: response?.resolvedModel ?? null,
    providerRequestId: result?.providerRequestId ?? null,
    decisionConformance: response?.kind === 'valid'
      ? 'valid'
      : response
        ? 'invalid'
        : 'unavailable',
    usage
  };
}

// This helper exists only in the offline contract tests. Callers must supply the
// controlled HTTPS fixture; request data and runtime configuration never select it.
export async function runPrivateJevTestAttempt({
  store,
  workflowId,
  ownerId,
  fence,
  expectedRevision,
  attemptId,
  logicalOperationRef,
  transportAttemptId,
  authorityHoldRef,
  request,
  apiKey,
  testOnlySend
}) {
  if (typeof testOnlySend !== 'function') throw new TypeError('The offline test sender is required.');
  for (const identity of [workflowId, logicalOperationRef, attemptId, transportAttemptId]) {
    if (!isSafeObservationIdentity(identity)) {
      throw new TypeError('The offline attempt identity is invalid.');
    }
  }

  const intent = store.recordDispatchIntent({
    workflowId,
    ownerId,
    fence,
    expectedRevision,
    attemptId,
    logicalOperationRef,
    authorityHoldRef,
    transportAttemptId,
    eventId: 'synthetic-jev-intent:' + attemptId
  });
  const marker = store.recordJevDispatchMarker({
    workflowId,
    ownerId,
    fence,
    expectedRevision: intent.revision,
    attemptId,
    transportAttemptId
  });
  if (marker.duplicate) {
    return { dispatched: false, duplicateIntent: true, intent, marker };
  }

  let result;
  let error;
  try {
    result = await testOnlySend({ request, apiKey });
  } catch (caught) {
    error = caught;
  }
  const transportResult = summarizeTransportResult(result);
  const observation = makeObservation({
    workflowId,
    attemptId,
    logicalOperationRef,
    transportAttemptId,
    result: transportResult,
    error
  });
  const persistedObservation = store.recordJevProviderObservation(observation);
  return {
    dispatched: true,
    intent,
    marker,
    transportResult,
    observation,
    persistedObservation
  };
}
