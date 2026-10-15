import { types as utilTypes } from 'node:util';
import { readOperationsConfig } from '../../src/worker/private/config.js';
const STOP_OBSERVATION_MS = readOperationsConfig().shutdownTimeoutMs;

const PREFIX = 'AH11_OPS_CLOSED_DIAGNOSTIC_V2 ';
const SCHEMA = 'agent-harness.ops-closed-diagnostic.v2';
const MAX_RECORDS = 32;
const MAX_BYTES = 16_384;
const MAX_LINE_BYTES = 768;
const PHASES = Object.freeze({
  'OPS06.C01': ['restart_start', 'restart_wait', 'restart_stop', 'restart_verify', 'close'],
  'OPS05.C12': ['config', 'open', 'gate', 'claim', 'result', 'close', 'expired_child',
    'recovered_count', 'terminal_child', 'accepted_count', 'null_count', 'terminal_count', 'cleanup'],
  'OPS05.C08': ['worker_start', 'worker_run', 'worker_stop', 'admission', 'ready', 'progress',
    'stop', 'crash', 'runner_close', 'lease_expiry', 'restart_ready', 'completion', 'fencing', 'close', 'cleanup']
});
const CODES = new Set(['OTHER', 'ERR_SQLITE_ERROR', 'ERR_ASSERTION', 'INVALID_INPUT',
  'STORE_BUSY', 'STORE_OPEN_FAILED', 'STORE_WRITE_FAILED', 'STORE_CLOSED', 'CLOCK_ROLLBACK',
  'STALE_FENCE', 'CONCURRENT_MODIFICATION', 'LEASE_EXPIRED', 'none']);
const KINDS = new Set(['native_sqlite', 'runtime', 'assertion', 'error', 'type', 'range', 'syntax', 'other', 'none']);
const STATES = new Set(['queued', 'running', 'parked', 'unknown', 'completed', 'failed', 'cancelled', 'unobserved']);
const REASONS = new Set(['none', 'wait_deadline', 'read_failure', 'state_mismatch', 'phase_failure',
  'DEADLINE_EXCEEDED', 'CONTROLLER_INPUT_INVALID', 'LOCAL_COMPLETION_BUSY',
  'LOCAL_CONTROLLER_STOPPED', 'STORE_BUSY', 'OTHER']);
const CLOSES = new Set(['unobserved', 'confirmed', 'failed', 'unknown']);
const EXITS = new Set(['unobserved', 'zero', 'nonzero', 'signal', 'unknown']);
const PIPE_EOF = new Set(['unobserved', 'complete', 'incomplete']);
const FIELDS = Object.freeze(['schema', 'sequence', 'caseId', 'slot', 'phase', 'childPhase', 'reason', 'lastState',
  'lastReason', 'actual', 'expected', 'primaryFailed', 'errorKind', 'code', 'sqlitePrimaryCode',
  'cleanupFailed', 'cleanupKind', 'cleanupCode', 'cleanupPrimaryCode', 'nativeClose',
  'processClosed', 'exitDisposition', 'resultPresent', 'errorEvent', 'pipeEOF', 'outputPresent']);

function ownData(input, allowed, required = allowed) {
  try {
    // IsArray throws on a revoked proxy. Reject proxies first and keep the
    // complete shape boundary inside the fail-closed catch.
    if (input === null || typeof input !== 'object' || utilTypes.isProxy(input) || Array.isArray(input)) return null;
    const proto = Object.getPrototypeOf(input);
    if (proto !== Object.prototype && proto !== null) return null;
    const keys = Reflect.ownKeys(input);
    if (keys.length > allowed.length) return null;
    const result = Object.create(null);
    for (const key of keys) {
      if (typeof key !== 'string' || !allowed.includes(key)) return null;
      const descriptor = Object.getOwnPropertyDescriptor(input, key);
      if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) return null;
      result[key] = descriptor.value;
    }
    return required.every(key => Object.hasOwn(result, key)) ? result : null;
  } catch { return null; }
}

export function closedErrorProjection(error) {
  if (utilTypes.isProxy(error) || !utilTypes.isNativeError(error)) {
    return { errorKind: 'other', code: 'OTHER', sqlitePrimaryCode: null };
  }
  try {
    const code = Object.getOwnPropertyDescriptor(error, 'code')?.value;
    const errcode = Object.getOwnPropertyDescriptor(error, 'errcode')?.value;
    if (code === 'ERR_SQLITE_ERROR' && Number.isSafeInteger(errcode) && errcode >= 0 && errcode <= 0x7fffffff) {
      return { errorKind: 'native_sqlite', code, sqlitePrimaryCode: errcode & 0xff };
    }
    return { errorKind: code === 'ERR_ASSERTION' ? 'assertion' : CODES.has(code) ? 'runtime'
        : error instanceof SyntaxError ? 'syntax' : error instanceof TypeError ? 'type'
          : error instanceof RangeError ? 'range' : 'error',
      code: CODES.has(code) ? code : 'OTHER', sqlitePrimaryCode: null };
  } catch { return { errorKind: 'other', code: 'OTHER', sqlitePrimaryCode: null }; }
}

export function closedWorkObservation(item) {
  // Only values from the already read work page are projected; no new query.
  if (utilTypes.isProxy(item)) return { lastState: 'unobserved', lastReason: 'OTHER' };
  try {
    const state = Object.getOwnPropertyDescriptor(item ?? {}, 'state')?.value;
    const reason = Object.getOwnPropertyDescriptor(item ?? {}, 'reasonCode')?.value;
    return { lastState: STATES.has(state) ? state : 'unobserved',
      lastReason: reason === null || reason === undefined ? 'none' : REASONS.has(reason) ? reason : 'OTHER' };
  } catch { return { lastState: 'unobserved', lastReason: 'OTHER' }; }
}

export function makeClosedDiagnostic(input) {
  const values = ownData(input, FIELDS, ['caseId', 'phase']);
  if (!values) return null;
  const record = { schema: SCHEMA, sequence: null, slot: 0, childPhase: 'unobserved', reason: 'none', lastState: 'unobserved', lastReason: 'none',
    actual: null, expected: null, primaryFailed: false, errorKind: 'none', code: 'none', sqlitePrimaryCode: null,
    cleanupFailed: false, cleanupKind: 'none', cleanupCode: 'none', cleanupPrimaryCode: null,
    nativeClose: 'unobserved', processClosed: false, exitDisposition: 'unobserved', resultPresent: false,
    errorEvent: false, pipeEOF: 'unobserved', outputPresent: false, ...values };
  if (record.schema !== SCHEMA || typeof record.caseId !== 'string' || !Object.hasOwn(PHASES, record.caseId)
      || !PHASES[record.caseId].includes(record.phase) || !Number.isInteger(record.slot) || record.slot < 0 || record.slot > 2
      || (record.childPhase !== 'unobserved' && !PHASES[record.caseId].includes(record.childPhase))
      || !REASONS.has(record.reason) || !REASONS.has(record.lastReason) || !STATES.has(record.lastState)
      || !KINDS.has(record.errorKind) || !KINDS.has(record.cleanupKind) || !CODES.has(record.code) || !CODES.has(record.cleanupCode)
      || !CLOSES.has(record.nativeClose) || !EXITS.has(record.exitDisposition) || !PIPE_EOF.has(record.pipeEOF)) return null;
  if (record.sequence !== null && (!Number.isInteger(record.sequence) || record.sequence < 1 || record.sequence > MAX_RECORDS)) return null;
  for (const key of ['actual', 'expected']) {
    if (record[key] !== null && (!Number.isInteger(record[key]) || record[key] < 0 || record[key] > 64)) return null;
  }
  for (const key of ['sqlitePrimaryCode', 'cleanupPrimaryCode']) {
    if (record[key] !== null && (!Number.isInteger(record[key]) || record[key] < 0 || record[key] > 255)) return null;
  }
  for (const key of ['primaryFailed', 'cleanupFailed', 'processClosed', 'resultPresent', 'errorEvent', 'outputPresent']) if (typeof record[key] !== 'boolean') return null;
  return Object.freeze(record);
}

export function encodeClosedDiagnostic(input) {
  try {
    const record = makeClosedDiagnostic(input);
    if (!record) return null;
    const line = `${PREFIX}${JSON.stringify(record)}\n`;
    return Buffer.byteLength(line, 'utf8') <= MAX_LINE_BYTES ? line : null;
  } catch { return null; } // Diagnostic construction cannot replace the primary failure.
}

export function parseClosedChildDiagnostic(text, caseId) {
  if (typeof text !== 'string' || !text.endsWith('\n') || Buffer.byteLength(text, 'utf8') > MAX_BYTES) return null;
  const lines = text.split(/\r?\n/u).filter(line => line.startsWith(PREFIX));
  if (lines.length < 1 || lines.length > (caseId === 'OPS06.C01' ? 2 : 1)
      || lines.some(line => Buffer.byteLength(`${line}\n`, 'utf8') > MAX_LINE_BYTES)) return null;
  try {
    const records = lines.map(line => {
      const raw = ownData(JSON.parse(line.slice(PREFIX.length)), FIELDS);
      const record = raw ? makeClosedDiagnostic(raw) : null;
      return record?.caseId === caseId && encodeClosedDiagnostic(record) === `${line}\n` ? record : null;
    });
    if (records.some(record => record === null || record.sequence !== null || record.slot !== 0)) return null;
    if (caseId !== 'OPS06.C01') return records[0];
    const [primary, cleanup] = records;
    if (primary.phase === 'close') return null;
    // An early primary observation alone preserves failure detail, but cannot
    // establish final native cleanup. No missing frame is silently promoted.
    if (!cleanup) return { ...primary, nativeClose: 'unknown', cleanupFailed: true };
    const cleanupOnly = new Set(['phase', 'childPhase', 'nativeClose', 'cleanupFailed',
      'cleanupKind', 'cleanupCode', 'cleanupPrimaryCode']);
    if (cleanup.phase !== 'close' || cleanup.childPhase !== primary.phase
        || FIELDS.some(field => !cleanupOnly.has(field) && cleanup[field] !== primary[field])) return null;
    return { ...primary, nativeClose: cleanup.nativeClose, cleanupFailed: cleanup.cleanupFailed,
      cleanupKind: cleanup.cleanupKind, cleanupCode: cleanup.cleanupCode,
      cleanupPrimaryCode: cleanup.cleanupPrimaryCode };
  } catch { return null; }
}

export function createClosedDiagnosticChannel() {
  return { lines: '', count: 0, bytes: 0, diagnosticsComplete: true };
}

export function appendClosedDiagnostic(channel, input) {
  const values = makeClosedDiagnostic(input);
  const owned = ownData(channel, ['lines', 'count', 'bytes', 'diagnosticsComplete']);
  if (!owned || typeof owned.lines !== 'string' || !Number.isInteger(owned.count) || owned.count < 0
      || !Number.isInteger(owned.bytes) || owned.bytes < 0 || typeof owned.diagnosticsComplete !== 'boolean') return false;
  const line = values ? encodeClosedDiagnostic({ ...values, sequence: owned.count + 1 }) : null;
  if (!line || channel.count >= MAX_RECORDS || channel.bytes + Buffer.byteLength(line, 'utf8') > MAX_BYTES) {
    channel.diagnosticsComplete = false;
    return false;
  }
  channel.lines += line;
  channel.count += 1;
  channel.bytes += Buffer.byteLength(line, 'utf8');
  return true;
}

export function parseClosedDiagnosticChannel(input) {
  const incomplete = () => Object.freeze({ complete: false, recordCount: 0, records: Object.freeze([]) });
  const channel = ownData(input, ['lines', 'count', 'bytes', 'diagnosticsComplete']);
  if (!channel || channel.diagnosticsComplete !== true || typeof channel.lines !== 'string'
      || !Number.isInteger(channel.count) || channel.count < 0 || channel.count > MAX_RECORDS
      || !Number.isInteger(channel.bytes) || channel.bytes < 0 || channel.bytes > MAX_BYTES
      || Buffer.byteLength(channel.lines, 'utf8') !== channel.bytes) return incomplete();
  const lines = channel.lines === '' ? [] : channel.lines.split('\n');
  if (lines.length) { if (lines.pop() !== '') return incomplete(); }
  if (lines.length !== channel.count) return incomplete();
  const records = [];
  const seen = new Set();
  for (const line of lines) {
    // Parse a single closed record without guessing a case from untrusted text.
    let parsed;
    try { parsed = line.startsWith(PREFIX) ? makeClosedDiagnostic(ownData(JSON.parse(line.slice(PREFIX.length)), FIELDS)) : null; } catch { return incomplete(); }
    if (!parsed || encodeClosedDiagnostic(parsed) !== `${line}\n` || parsed.sequence !== records.length + 1 || Buffer.byteLength(`${line}\n`, 'utf8') > MAX_LINE_BYTES) return incomplete();
    const identity = `${parsed.caseId}:${parsed.slot}:${parsed.phase}`;
    if (seen.has(identity)) return incomplete();
    seen.add(identity); records.push(parsed);
  }
  return Object.freeze({ complete: true, recordCount: records.length, records: Object.freeze(records) });
}

export function closedExitDisposition(exit) {
  return typeof exit?.signal === 'string' ? 'signal' : exit?.code === 0 ? 'zero'
    : Number.isInteger(exit?.code) ? 'nonzero' : 'unknown';
}

export async function observeWorkerCleanup(worker, reason) {
  // Same configured default shutdown budget; timeout observes uncertainty, never releases ownership.
  let timer;
  try {
    const result = await Promise.race([worker.stop(reason), new Promise(resolve => {
      timer = setTimeout(() => resolve(null), STOP_OBSERVATION_MS);
    })]);
    return { nativeClose: result?.storeClosed === true ? 'confirmed' : 'unknown',
      cleanupFailed: result === null || result?.stopped !== true || result?.storeClosed !== true || result?.childrenReaped !== true };
  } catch (error) {
    const projection = closedErrorProjection(error);
    return { nativeClose: 'failed', cleanupFailed: true, cleanupKind: projection.errorKind,
      cleanupCode: projection.code, cleanupPrimaryCode: projection.sqlitePrimaryCode };
  } finally { clearTimeout(timer); }
}


export function observeClosedChild(child) {
  const state = { processClosed: false, errorEvent: false, stdoutEOF: false, stderrEOF: false,
    outputPresent: false, code: null, signal: null, error: undefined };
  // Register the real close/error/EOF observations immediately after spawn.
  // Error notification never resolves close, and exit alone never asserts EOF.
  child.once('error', error => { state.errorEvent = true; state.error = error; });
  for (const [stream, field] of [[child.stdout, 'stdoutEOF'], [child.stderr, 'stderrEOF']]) {
    stream?.once('end', () => { state[field] = true; });
  }
  child.stdout?.on('data', chunk => { if (chunk.length > 0) state.outputPresent = true; });
  const closed = new Promise(resolve => child.once('close', (code, signal) => {
    state.processClosed = true; state.code = code; state.signal = signal;
    resolve({ ...state });
  }));
  return { closed, snapshot: () => ({ ...state }) };
}

export function closedChildObservation(exit, outputPresent = false, resultPresent = false) {
  const fallback = { processClosed: false, errorEvent: false, pipeEOF: 'unobserved',
    exitDisposition: 'unknown', outputPresent: outputPresent === true, resultPresent: resultPresent === true };
  try {
    if (exit === null || typeof exit !== 'object' || utilTypes.isProxy(exit)) return fallback;
    const value = key => Object.getOwnPropertyDescriptor(exit, key)?.value;
    const processClosed = value('processClosed') === true;
    return { ...fallback, processClosed, errorEvent: value('errorEvent') === true,
      pipeEOF: value('stdoutEOF') === true && value('stderrEOF') === true ? 'complete'
        : processClosed ? 'incomplete' : 'unobserved',
      exitDisposition: processClosed ? closedExitDisposition({ code: value('code'), signal: value('signal') }) : 'unknown' };
  } catch { return fallback; }
}

export function requiresClosedFixtureRetention(exit, nativeClose = 'unobserved', cleanupFailed = true) {
  const observed = closedChildObservation(exit);
  return observed.processClosed !== true || observed.errorEvent === true || observed.pipeEOF !== 'complete'
    || nativeClose !== 'confirmed' || cleanupFailed !== false;
}

export function publishClosedDiagnostic(writer, input) {
  const line = encodeClosedDiagnostic(input);
  if (line === null) return { published: false, failed: true, error: null };
  try { writer(line); return { published: true, failed: false }; }
  catch (error) { return { published: false, failed: true, error }; }
}

export async function publishPrimaryThenCleanup(writer, primary, cleanup) {
  // Queue the finite primary frame before invoking potentially slow cleanup.
  // A queued write is not proof the parent consumed it; the parser checks that.
  const record = makeClosedDiagnostic(primary);
  const primaryPublication = publishClosedDiagnostic(writer, record);
  let observed;
  try { observed = await cleanup(); }
  catch (error) {
    const detail = closedErrorProjection(error);
    observed = { nativeClose: 'failed', cleanupFailed: true, cleanupKind: detail.errorKind,
      cleanupCode: detail.code, cleanupPrimaryCode: detail.sqlitePrimaryCode };
  }
  const cleanupFields = ownData(observed,
    ['nativeClose', 'cleanupFailed', 'cleanupKind', 'cleanupCode', 'cleanupPrimaryCode'], []);
  if (cleanupFields === null) observed = { nativeClose: 'unknown', cleanupFailed: true };
  else observed = cleanupFields;
  const cleanupPublication = publishClosedDiagnostic(writer, record === null ? null : {
    ...record, ...observed, phase: 'close', childPhase: record.phase
  });
  return { primaryPublication, cleanupPublication, cleanup: observed };
}

export async function waitForClosedChild(closed, timeoutMs) {
  let timer;
  try {
    return await Promise.race([closed, new Promise(resolve => {
      timer = setTimeout(() => resolve(null), timeoutMs);
    })]);
  } finally { clearTimeout(timer); }
}
