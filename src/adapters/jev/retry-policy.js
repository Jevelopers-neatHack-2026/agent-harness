import { isPlainDataRecord } from './safe-json.js';

export const JEV_MAX_RECOMMENDED_ATTEMPTS = 2;
export const JEV_RETRY_BASE_DELAY_MS = 250;
export const JEV_RETRY_MAX_DELAY_MS = 10_000;

function readInput(value) {
  if (!isPlainDataRecord(value)) return null;
  let keys;
  try {
    keys = Reflect.ownKeys(value);
  } catch {
    return null;
  }
  const required = ['status', 'attemptNumber', 'remainingMs', 'nowEpochMs'];
  const optional = ['retryAfter', 'failureKind'];
  const allowed = new Set([...required, ...optional]);
  if (keys.some(key => typeof key !== 'string' || !allowed.has(key))) return null;
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
  if (required.some(key => !Object.hasOwn(result, key))) return null;
  return result;
}

function decision(action, reason, attemptsRemaining, delayMs = null) {
  return Object.freeze({
    action,
    reason,
    attemptsRemaining,
    delayMs,
    dispatchAuthorized: false
  });
}

function parseRetryAfter(value, nowEpochMs) {
  if (value === undefined) return { kind: 'missing' };
  if (typeof value !== 'string' || value.length > 128 || /[\u0000-\u001f\u007f]/u.test(value)) {
    return { kind: 'invalid' };
  }
  if (/^[0-9]+$/u.test(value)) {
    const seconds = Number(value);
    if (!Number.isSafeInteger(seconds) || seconds > Math.floor(JEV_RETRY_MAX_DELAY_MS / 1000)) {
      return { kind: 'too_long' };
    }
    return { kind: 'valid', delayMs: seconds * 1000 };
  }
  if (!/^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), [0-9]{2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT$/u.test(value)) {
    return { kind: 'invalid' };
  }
  const date = Date.parse(value);
  if (!Number.isFinite(date)) return { kind: 'invalid' };
  const delayMs = Math.max(0, date - nowEpochMs);
  return delayMs > JEV_RETRY_MAX_DELAY_MS
    ? { kind: 'too_long' }
    : { kind: 'valid', delayMs };
}

export function planJevRetry(input) {
  const fields = readInput(input);
  if (!fields
      || (fields.status !== null && (!Number.isInteger(fields.status) || fields.status < 100 || fields.status > 599))
      || !Number.isSafeInteger(fields.attemptNumber) || fields.attemptNumber < 1
      || !Number.isSafeInteger(fields.remainingMs) || fields.remainingMs < 0 || fields.remainingMs > 60_000
      || !Number.isSafeInteger(fields.nowEpochMs) || fields.nowEpochMs < 0
      || (Object.hasOwn(fields, 'failureKind')
        && !['timeout', 'connection_reset', 'body_interrupted', 'cancelled_before_dispatch', 'cancelled_after_possible_dispatch'].includes(fields.failureKind))) {
    throw new TypeError('The JEV retry planning input is outside its bounded policy.');
  }

  if (fields.failureKind === 'cancelled_before_dispatch') {
    return decision('stop', 'cancelled_before_dispatch', 0);
  }
  if (fields.failureKind === 'cancelled_after_possible_dispatch') {
    return decision('review', 'outcome_may_be_ambiguous', 0);
  }
  if (fields.failureKind !== undefined) {
    return decision('review', 'outcome_may_be_ambiguous', 0);
  }
  if (fields.status === null) return decision('review', 'outcome_may_be_ambiguous', 0);
  if (fields.status === 401) return decision('stop', 'authentication_rejected', 0);
  if (fields.status === 422) return decision('stop', 'request_rejected', 0);
  if (fields.status !== 429 && fields.status !== 529) {
    return decision('review', 'outcome_may_be_ambiguous', 0);
  }
  if (fields.attemptNumber >= JEV_MAX_RECOMMENDED_ATTEMPTS) {
    return decision('stop', 'recommendation_limit_reached', 0);
  }
  if (fields.remainingMs === 0) return decision('defer', 'deadline_exhausted', 1);

  const backoff = Math.min(
    JEV_RETRY_BASE_DELAY_MS * (2 ** Math.min(fields.attemptNumber - 1, 10)),
    JEV_RETRY_MAX_DELAY_MS
  );
  const header = parseRetryAfter(fields.retryAfter, fields.nowEpochMs);
  if (header.kind === 'too_long') return decision('defer', 'retry_after_exceeds_policy', 1);
  const delayMs = header.kind === 'valid' ? Math.max(backoff, header.delayMs) : backoff;
  if (delayMs > JEV_RETRY_MAX_DELAY_MS) return decision('defer', 'retry_delay_exceeds_policy', 1);
  if (delayMs > fields.remainingMs) return decision('defer', 'retry_after_exceeds_deadline', 1);
  return decision('retry_candidate', header.kind === 'invalid' ? 'invalid_retry_after_bounded_fallback' : 'rate_limit_backoff', 1, delayMs);
}
