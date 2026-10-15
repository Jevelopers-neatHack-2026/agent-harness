import { DatabaseSync } from 'node:sqlite';
import { setTimeout as delay } from 'node:timers/promises';
import { types as utilTypes } from 'node:util';
import { RuntimeError } from '../../src/internal/errors.js';

// Test fixtures only. No public backpressure response or string-shaped error
// grants retry permission: the same synchronous call must prove a native BEGIN
// refusal while its actual connection remained outside a transaction.
const EXEC = Object.getOwnPropertyDescriptor(DatabaseSync.prototype, 'exec');
// Node exposes this native receiver-checked getter on each database instance.
// Capture it from an immediately closed empty database, never from a caller's
// possibly shadowed property and never by issuing an extra SQL query.
const getterDatabase = new DatabaseSync(':memory:');
const IS_TRANSACTION = Object.getOwnPropertyDescriptor(getterDatabase, 'isTransaction').get;
getterDatabase.close();
const PREFIX = 'OPS_FIXTURE_BUSY_RETRY_V1 ';
const MAX_ATTEMPTS = 8;
const MAX_RETRY_MS = 1_500;
const MAX_DIAGNOSTIC_BYTES = 8_192;
let observing = false;

function data(value, key) {
  if (value === null || (typeof value !== 'object' && typeof value !== 'function') || utilTypes.isProxy(value)) return undefined;
  try { return Object.getOwnPropertyDescriptor(value, key)?.value; } catch { return undefined; }
}

function inactive(db) {
  try {
    if (utilTypes.isProxy(db)) return null;
    const result = Reflect.apply(IS_TRANSACTION, db, []);
    return typeof result === 'boolean' ? !result : null;
  } catch { return null; }
}

function nativeBusy(error) {
  if (!utilTypes.isNativeError(error) || error instanceof RuntimeError) return null;
  const primary = data(error, 'errcode');
  if (data(error, 'code') !== 'ERR_SQLITE_ERROR' || !Number.isSafeInteger(primary) || primary < 0) return null;
  return [5, 6].includes(primary & 0xff) ? primary & 0xff : null;
}

export function observeFixtureTransaction(invoke) {
  const current = Object.getOwnPropertyDescriptor(DatabaseSync.prototype, 'exec');
  if (typeof invoke !== 'function' || utilTypes.isAsyncFunction(invoke) || observing
      || !current || Object.keys(EXEC).some(key => current[key] !== EXEC[key])) {
    throw new TypeError('The fixture transaction observer is unavailable.');
  }
  const events = [];
  let overflow = false;
  let value;
  let error;
  let failed = false;
  let restored = false;
  let mutated = false;
  let wrapper;
  observing = true;
  try {
    wrapper = function (...args) {
      const sql = args[0];
      const boundary = sql === 'BEGIN IMMEDIATE' ? 'begin' : sql === 'COMMIT' ? 'commit' : sql === 'ROLLBACK' ? 'rollback' : 'other';
      const record = { db: this, boundary, beforeInactive: inactive(this), afterInactive: null, nativePrimaryCode: null, failed: false };
      try { return Reflect.apply(EXEC.value, this, args); }
      catch (caught) { record.failed = true; record.nativePrimaryCode = nativeBusy(caught); throw caught; }
      finally {
        record.afterInactive = inactive(this);
        if (events.length < 4) events.push(record); else overflow = true;
      }
    };
    Object.defineProperty(DatabaseSync.prototype, 'exec', { ...EXEC, value: wrapper });
    try {
      value = invoke();
      if (utilTypes.isPromise(value)) throw new TypeError('The fixture transaction call must be synchronous.');
    } catch (caught) { failed = true; error = caught; }
  } finally {
    try {
      const observedDescriptor = Object.getOwnPropertyDescriptor(DatabaseSync.prototype, 'exec');
      mutated = !observedDescriptor || observedDescriptor.value !== wrapper
        || Object.keys(EXEC).some(key => key !== 'value' && observedDescriptor[key] !== EXEC[key]);
      Object.defineProperty(DatabaseSync.prototype, 'exec', EXEC);
      restored = true;
    } catch (restoreError) {
      error = new AggregateError(failed ? [error, restoreError] : [restoreError], 'The fixture observer could not restore the native method.');
      failed = true;
    } finally { observing = false; }
  }
  const first = events[0];
  const eligible = restored && !mutated && !overflow && events.length === 1 && first.boundary === 'begin'
    && first.failed && first.nativePrimaryCode !== null
    && first.beforeInactive === true && first.afterInactive === true && inactive(first.db) === true;
  return {
    value, error, failed,
    proof: { eligible, restored, boundary: events.length === 1 ? first.boundary : 'unproven',
      nativePrimaryCode: events.length === 1 ? first.nativePrimaryCode : null,
      beforeInactive: events.length === 1 ? first.beforeInactive : null,
      afterInactive: events.length === 1 ? first.afterInactive : null }
  };
}

function claimBusy(observed) {
  const error = observed.error;
  return observed.failed && utilTypes.isNativeError(error) && !utilTypes.isProxy(error)
    && error instanceof RuntimeError && data(error, 'module') === 'state'
    && data(error, 'operation') === '_operationsClaim' && data(error, 'code') === 'STORE_BUSY'
    && data(error, 'retryable') === true;
}

function admissionBusy(observed) {
  return !observed.failed && data(observed.value, 'accepted') === false
    && data(observed.value, 'code') === 'OPS_BACKPRESSURE' && data(observed.value, 'reasonCode') === 'STORE_BUSY';
}

const CODES = new Set(['STORE_BUSY', 'STORE_WRITE_FAILED', 'STORE_CLOSED', 'STORE_OPEN_FAILED', 'STALE_FENCE',
  'LEASE_EXPIRED', 'CONCURRENT_MODIFICATION', 'CLOCK_ROLLBACK', 'INVALID_INPUT', 'OUTCOME_UNRESOLVED',
  'TERMINAL_STATE', 'OPS_BACKPRESSURE', 'OPS_UNAVAILABLE', 'OTHER', 'none']);
const REASONS = new Set(['STORE_BUSY', 'STORE_UNAVAILABLE', 'WORKER_NOT_ACCEPTING', 'OTHER', 'none']);
const STOPS = new Set(['success', 'refusal', 'error', 'deadline', 'deadline_after_call',
  'clock_after_call', 'attempt_limit', 'invalid_delay']);
const OUTCOMES = new Set(['claim', 'empty', 'accepted', 'refused', 'error']);
const BOUNDARIES = new Set(['begin', 'commit', 'rollback', 'other', 'unproven']);

function unqualifiedCall(observed, qualificationError) {
  // Timing cannot undo a successful call or establish rollback. Retain its
  // value and original primary, and expose qualification failure separately.
  return { ...observed, failed: true,
    error: observed.failed ? observed.error : qualificationError, qualificationError };
}

export async function runFixtureBusyRetry({ kind, invoke, deadlineNs, delayMs, busyTimeoutMs,
  clock = () => process.hrtime.bigint(), sleep = delay }) {
  if (!['claim', 'admission'].includes(kind) || typeof invoke !== 'function' || typeof deadlineNs !== 'bigint'
      || !Number.isSafeInteger(busyTimeoutMs) || busyTimeoutMs < 1 || busyTimeoutMs > 250
      || typeof clock !== 'function' || typeof sleep !== 'function') throw new TypeError('Invalid fixture retry options.');
  let lastClock = null;
  const readClock = () => {
    const now = clock();
    if (typeof now !== 'bigint' || now < 0n || (lastClock !== null && now < lastClock)) {
      throw new TypeError('The fixture retry clock is unavailable.');
    }
    lastClock = now;
    return now;
  };
  const started = readClock();
  const deadline = deadlineNs < started + BigInt(MAX_RETRY_MS) * 1_000_000n
    ? deadlineNs : started + BigInt(MAX_RETRY_MS) * 1_000_000n;
  const attempts = [];
  let latest = null;
  let stopReason = 'deadline';
  try {
  while (attempts.length < MAX_ATTEMPTS && readClock() + BigInt(busyTimeoutMs) * 1_000_000n < deadline) {
    try { latest = observeFixtureTransaction(invoke); }
    catch (error) { latest = { failed: true, error, value: undefined,
      proof: { eligible: false, restored: false, boundary: 'unproven', nativePrimaryCode: null, beforeInactive: null, afterInactive: null } }; }
    let finished;
    let clockFailed = false;
    let clockError;
    try { finished = readClock(); }
    catch (error) {
      clockFailed = true;
      clockError = utilTypes.isNativeError(error) ? error
        : new AggregateError([error], 'The fixture clock could not qualify the observed call.');
    }
    const result = latest.value;
    const busy = kind === 'claim' ? claimBusy(latest) : admissionBusy(latest);
    const code = latest.failed ? data(latest.error, 'code') : kind === 'admission' ? data(result, 'code') : undefined;
    const reasonCode = kind === 'admission' ? data(result, 'reasonCode') : undefined;
    const outcome = latest.failed ? 'error' : kind === 'claim' ? (data(result, 'claim') ? 'claim' : 'empty')
      : data(result, 'accepted') === true ? 'accepted' : 'refused';
    const record = { attempt: attempts.length + 1, outcome,
      code: code === undefined ? 'none' : CODES.has(code) ? code : 'OTHER',
      reasonCode: reasonCode === undefined ? 'none' : REASONS.has(reasonCode) ? reasonCode : 'OTHER',
      ...latest.proof, eligible: busy && latest.proof.eligible, retryScheduled: false, delayMs: 0 };
    attempts.push(record);
    if (clockFailed) {
      latest = unqualifiedCall(latest, clockError);
      stopReason = 'clock_after_call';
      break;
    }
    if (finished >= deadline) {
      latest = unqualifiedCall(latest, new Error(
        'The fixture call finished at or after its deadline; its observed outcome is retained without replay.'));
      stopReason = 'deadline_after_call';
      break;
    }
    if (!busy || !latest.proof.eligible) { stopReason = latest.failed ? 'error' : outcome === 'refused' ? 'refusal' : 'success'; break; }
    if (attempts.length === MAX_ATTEMPTS) { stopReason = 'attempt_limit'; break; }
    const waitMs = kind === 'admission' ? data(result, 'retryAfterMs') : delayMs;
    if (!Number.isSafeInteger(waitMs) || waitMs < 1 || waitMs > 1_000) { stopReason = 'invalid_delay'; break; }
    if (readClock() + BigInt(waitMs + busyTimeoutMs) * 1_000_000n >= deadline) { stopReason = 'deadline'; break; }
    record.retryScheduled = true;
    record.delayMs = waitMs;
    await sleep(waitMs);
  }
  } catch (retryError) {
    latest = { ...latest, failed: true,
      error: new AggregateError(latest?.failed ? [latest.error, retryError] : [retryError],
        'The fixture retry could not safely continue.') };
    stopReason = 'error';
  }
  const diagnostic = { schema: 'agent-harness.ops-fixture-busy-retry.v1', kind, stopReason,
    maxAttempts: MAX_ATTEMPTS, maxRetryMs: MAX_RETRY_MS, attempts };
  return { ...(latest ?? { failed: true, error: new Error('The fixture retry deadline expired.'), value: undefined }), diagnostic };
}

function exactRecord(value, keys) {
  if (value === null || typeof value !== 'object' || utilTypes.isProxy(value) || Array.isArray(value)) return false;
  try {
    const prototype = Object.getPrototypeOf(value);
    const own = Reflect.ownKeys(value);
    return (prototype === Object.prototype || prototype === null) && own.length === keys.length
      && own.every(key => typeof key === 'string' && keys.includes(key)
        && Object.getOwnPropertyDescriptor(value, key)?.enumerable === true
        && Object.hasOwn(Object.getOwnPropertyDescriptor(value, key), 'value'));
  } catch { return false; }
}

export function encodeFixtureRetryDiagnostic(value) {
  const keys = ['schema', 'kind', 'stopReason', 'maxAttempts', 'maxRetryMs', 'attempts'];
  if (!exactRecord(value, keys) || value.schema !== 'agent-harness.ops-fixture-busy-retry.v1'
      || !['claim', 'admission'].includes(value.kind) || !STOPS.has(value.stopReason)
      || value.maxAttempts !== MAX_ATTEMPTS || value.maxRetryMs !== MAX_RETRY_MS
      || utilTypes.isProxy(value.attempts) || !Array.isArray(value.attempts) || value.attempts.length > MAX_ATTEMPTS) return null;
  const rows = [];
  for (let i = 0; i < value.attempts.length; i += 1) {
    const descriptor = Object.getOwnPropertyDescriptor(value.attempts, String(i));
    if (!descriptor || !Object.hasOwn(descriptor, 'value')) return null;
    const row = descriptor.value;
    if (!exactRecord(row, ['attempt', 'outcome', 'code', 'reasonCode', 'eligible', 'restored', 'boundary',
      'nativePrimaryCode', 'beforeInactive', 'afterInactive', 'retryScheduled', 'delayMs'])
      || row.attempt !== i + 1 || !OUTCOMES.has(row.outcome) || !CODES.has(row.code) || !REASONS.has(row.reasonCode)
      || !BOUNDARIES.has(row.boundary) || ![null, 5, 6].includes(row.nativePrimaryCode)
      || ![null, true, false].includes(row.beforeInactive) || ![null, true, false].includes(row.afterInactive)
      || typeof row.eligible !== 'boolean' || typeof row.restored !== 'boolean' || typeof row.retryScheduled !== 'boolean'
      || !Number.isSafeInteger(row.delayMs) || row.delayMs < 0 || row.delayMs > 1_000
      || (row.retryScheduled && (!row.eligible || row.delayMs < 1))
      || (!row.retryScheduled && row.delayMs !== 0)) return null;
    rows.push({ ...row });
  }
  const line = `${PREFIX}${JSON.stringify({ ...value, attempts: rows })}\n`;
  return Buffer.byteLength(line, 'utf8') <= MAX_DIAGNOSTIC_BYTES ? line : null;
}

export function parseFixtureRetryDiagnostic(text) {
  if (typeof text !== 'string' || Buffer.byteLength(text, 'utf8') > 16_384 || !text.endsWith('\n')) return null;
  const lines = text.split(/\r?\n/u).filter(line => line.startsWith(PREFIX));
  if (lines.length !== 1) return null;
  try {
    const value = JSON.parse(lines[0].slice(PREFIX.length));
    return encodeFixtureRetryDiagnostic(value) === `${lines[0]}\n` ? value : null;
  } catch { return null; }
}

export function publishFixtureRetryDiagnostic(attempted, write) {
  try {
    const line = encodeFixtureRetryDiagnostic(attempted.diagnostic);
    if (line === null) throw new Error('The fixture retry history could not be encoded.');
    write(line);
  } catch (publicationError) {
    throw new AggregateError(attempted.failed ? [attempted.error, publicationError] : [publicationError],
      'The fixture retry history could not be published.');
  }
}
