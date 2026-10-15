import { closeSync, fstatSync, lstatSync, openSync, readFileSync, writeSync } from 'node:fs';
import { constants as fsConstants } from 'node:fs';
import { performance } from 'node:perf_hooks';
import { join, resolve } from 'node:path';
import { types as utilTypes } from 'node:util';

const SESSION_SCHEMA = 'agent-harness.ops07-session.v1';
const CHILD_SCHEMA = 'agent-harness.ops07-child-record.v1';
const CAPTURE_PREFIX = '# OPS07_CAPTURE_V1 ';
const CAPTURE_FLAG = '--ops-diagnostics-v1';
const MAX_CHILD_RECORDS = 100;
const MAX_CHILD_BYTES = 20 * 1024;
const MAX_PARENT_RECORDS = 100;
const MAX_PARENT_BYTES = 24 * 1024;
const MAX_CAPTURE_BYTES = 64 * 1024;
const FOOTER_RESERVE_BYTES = 512;
const MAX_LINE_BYTES = 1_024;

const sessions = new WeakMap();
let runnerWriter = null;

const PHASES = new Set([
  'runner_start', 'store_open', 'resource_ceilings', 'claim_read', 'controller',
  'initial_snapshot', 'initial_admission', 'loop_snapshot', 'completion',
  'completion_resolution', 'admission_resolution', 'next_admission', 'store_close', 'other'
]);
const REASONS = new Set([
  'CHECKPOINT_UNSUPPORTED', 'CONTROLLER_INPUT_INVALID', 'EXTERNAL_OUTCOME_UNRESOLVED',
  'LEASE_FENCED', 'LOCAL_CONTROLLER_STOPPED', 'NO_DETERMINISTIC_READY_STEP',
  'PENDING_STEP_IDENTITY_INVALID', 'PLAN_INPUT_REQUIRED', 'WORKFLOW_NOT_RUNNING',
  'WORKFLOW_SNAPSHOT_CHANGED', 'CHECKPOINT_LIMITS_IDENTITY_INVALID',
  'CHECKPOINT_PLAN_IDENTITY_INVALID', 'CONTROLLER_CHECKPOINT_INVALID',
  'CONTROLLER_CHECKPOINT_TOO_LARGE', 'BUDGET_CHECKPOINT_INCONSISTENT',
  'CLOCK_INCONSISTENT', 'INVALID_DEADLINE', 'LOCAL_RESULT_DUPLICATE',
  'LOCAL_RESULT_INVALID', 'LOCAL_STEP_NOT_PENDING', 'LOCAL_COMPLETION_BUSY',
  'LOCAL_COMPLETION_OUTCOME_UNKNOWN', 'LOCAL_ADMISSION_BUSY', 'LOCAL_ADMISSION_OUTCOME_UNKNOWN'
]);
const ERROR_CODES = new Set([
  'STORE_BUSY', 'STORE_WRITE_FAILED', 'STORE_OPEN_FAILED', 'STORE_CLOSED',
  'UNSUPPORTED_SCHEMA_VERSION', 'UNSUPPORTED_RUNTIME', 'SQLITE_UNSUPPORTED',
  'CONCURRENT_MODIFICATION', 'STALE_FENCE', 'LEASE_REQUIRED', 'LEASE_EXPIRED',
  'ILLEGAL_TRANSITION', 'OUTCOME_UNRESOLVED', 'INVALID_INPUT',
  'CHECKPOINT_CORRUPT', 'CLOCK_ROLLBACK'
]);
const OPERATIONS = new Set([
  'open', 'close', 'initializeDatabase', '_operationsSetResourceCeilings',
  '_operationsAdmit', '_operationsExpiredClaims', '_operationsSetRunnerPid',
  '_operationsClaim', '_operationsReadClaim', '_operationsFinish', '_operationsCancel',
  '_operationsGetScopedJob', '_operationsReadPage', '_operationsHealth',
  '_operationsRetentionRequest', '_localControllerSnapshot', '_localControllerResolveCompletion', '_localControllerAdmit',
  '_localControllerResolveAdmission',
  '_localControllerComplete', 'createWorkflow', 'getWorkflow', 'getCheckpoint',
  'acquireLease', 'renewLease', 'releaseLease', 'transitionWorkflow',
  'requestCancellation', 'recordDispatchIntent', 'recordTransportAttempt',
  'recordJevDispatchMarker', 'getJevDispatchMarker', 'recordResponseReceipt',
  'markOutcomeUnknown', 'reconcilePendingReceipts', 'recordAttemptObservation',
  'recordJevProviderObservation', 'getJevProviderObservation',
  'readJevObservationDeliveryPage', 'acknowledgeJevObservationDelivery',
  'getAttempt', 'getReceipt', 'getArtifact', 'listEvents', 'readEventPage',
  'countEventRowsAfter', 'getTelemetryCursor', 'advanceTelemetryCursor',
  'readOutbox', 'acknowledgeOutbox'
]);
const SIGNALS = new Set(['SIGTERM', 'SIGKILL', 'SIGINT', 'SIGHUP', 'SIGQUIT']);
const WORK_STATES = new Set(['queued', 'running', 'parked', 'unknown', 'completed', 'failed', 'cancelled']);
const WORKFLOW_STATES = new Set([
  'queued', 'running', 'awaiting_response', 'reconciliation_pending', 'review_required',
  'succeeded', 'failed', 'cancelled'
]);
const EVENT_TYPES = new Set([
  'workflow_created', 'operations_work_admitted', 'operations_work_claimed',
  'operations_work_completed', 'operations_work_parked', 'operations_work_failed',
  'operations_work_cancelled', 'operations_work_requeued', 'operations_work_unknown',
  'private_local_step_admitted', 'private_local_step_replayed', 'private_local_step_completed'
]);

const CHILD_FIELDS = Object.freeze({
  header: [],
  runner_phase: ['phase'],
  admission_committed: ['ordinal', 'checkpointSequence'],
  completion_committed: ['ordinal', 'checkpointSequence'],
  storage_origin: ['operation', 'errorKind', 'code', 'primaryCode', 'localClockGuard'],
  controller_error: ['phase', 'checkpointSequence', 'errorKind', 'code', 'primaryCode'],
  controller_result: ['status', 'reason', 'checkpointSequence'],
  runner_error: ['phase', 'errorKind', 'code', 'primaryCode'],
  store_open: ['ok'],
  resource_ceilings: ['ok'],
  claim_read: ['ok'],
  store_close: ['ok'],
  footer: ['writerComplete', 'overflow', 'writeFailed', 'malformed']
});
const CHILD_REQUIRED = Object.freeze({
  header: [],
  runner_phase: ['phase'],
  admission_committed: ['ordinal', 'checkpointSequence'],
  completion_committed: ['ordinal', 'checkpointSequence'],
  storage_origin: ['operation', 'errorKind', 'code', 'primaryCode'],
  controller_error: ['phase', 'checkpointSequence', 'errorKind', 'code', 'primaryCode'],
  controller_result: ['status', 'reason', 'checkpointSequence'],
  runner_error: ['phase', 'errorKind', 'code', 'primaryCode'],
  store_open: ['ok'],
  resource_ceilings: ['ok'],
  claim_read: ['ok'],
  store_close: ['ok'],
  footer: ['writerComplete', 'overflow', 'writeFailed', 'malformed']
});

const SUPERVISOR_FIELDS = Object.freeze({
  spawn: ['pidPresent', 'generation', 'fence'],
  spawn_error: ['code', 'generation', 'fence'],
  deadline: ['generation', 'fence'],
  child_error: ['code', 'generation', 'fence'],
  child_exit: ['exitCode', 'signal', 'childRuntimeMs', 'generation', 'fence'],
  finish_result: ['outcome', 'settled', 'code', 'generation', 'fence'],
  runner_identity_failure: ['code', 'generation', 'fence']
});

function ownStageFields(dictionary, stage) {
  if (typeof stage !== 'string' || !Object.hasOwn(dictionary, stage)) return null;
  return dictionary[stage];
}

function isPlainRecord(value) {
  if (value === null || typeof value !== 'object' || utilTypes.isProxy(value) || Array.isArray(value)) return false;
  try {
    const prototype = Object.getPrototypeOf(value);
    return prototype === Object.prototype || prototype === null;
  } catch {
    return false;
  }
}

function ownRecord(value, allowed, required = []) {
  if (!isPlainRecord(value)) return null;
  let keys;
  try { keys = Reflect.ownKeys(value); } catch { return null; }
  if (keys.length > allowed.length) return null;
  const record = Object.create(null);
  for (const key of keys) {
    if (typeof key !== 'string' || !allowed.includes(key)) return null;
    let descriptor;
    try { descriptor = Object.getOwnPropertyDescriptor(value, key); } catch { return null; }
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) return null;
    record[key] = descriptor.value;
  }
  if (required.some(key => !Object.hasOwn(record, key))) return null;
  return record;
}

function pickOwnValues(value, requiredKeys, optionalKeys = []) {
  if (!isPlainRecord(value)) return null;
  const result = Object.create(null);
  for (const key of [...requiredKeys, ...optionalKeys]) {
    let descriptor;
    try { descriptor = Object.getOwnPropertyDescriptor(value, key); } catch { return null; }
    if (!descriptor) {
      if (requiredKeys.includes(key)) return null;
      continue;
    }
    if (!Object.hasOwn(descriptor, 'value')) return null;
    result[key] = descriptor.value;
  }
  return result;
}

function finiteInt(value, minimum = 0, maximum = Number.MAX_SAFE_INTEGER) {
  return Number.isSafeInteger(value) && value >= minimum && value <= maximum;
}

function ownDenseArrayValues(value, maximumLength, exactLength = null) {
  if (utilTypes.isProxy(value) || !Array.isArray(value)) return null;
  let lengthDescriptor;
  try { lengthDescriptor = Object.getOwnPropertyDescriptor(value, 'length'); } catch { return null; }
  const length = lengthDescriptor && Object.hasOwn(lengthDescriptor, 'value') ? lengthDescriptor.value : null;
  if (!finiteInt(length, 0, maximumLength) || (exactLength !== null && length !== exactLength)) return null;
  const values = [];
  for (let index = 0; index < length; index += 1) {
    let descriptor;
    try { descriptor = Object.getOwnPropertyDescriptor(value, String(index)); } catch { return null; }
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) return null;
    values.push(descriptor.value);
  }
  return values;
}

function freezeSnapshot(value) {
  if (value === null || typeof value !== 'object' || Object.isFrozen(value)) return value;
  for (const key of Object.keys(value)) freezeSnapshot(value[key]);
  return Object.freeze(value);
}

function validScalar(value) {
  return value === null || typeof value === 'boolean' || typeof value === 'string'
    || (typeof value === 'number' && Number.isSafeInteger(value));
}

function ownErrorValue(error, name) {
  try {
    const descriptor = Object.getOwnPropertyDescriptor(error, name);
    if (!descriptor || !Object.hasOwn(descriptor, 'value')) return undefined;
    return descriptor.value;
  } catch {
    return undefined;
  }
}

function safeErrorProjection(error) {
  if (error === null || typeof error !== 'object' || utilTypes.isProxy(error) || !utilTypes.isNativeError(error)) {
    return { errorKind: 'OTHER', code: 'OTHER', primaryCode: null };
  }
  const codeValue = ownErrorValue(error, 'code');
  const errcode = ownErrorValue(error, 'errcode');
  if (codeValue === 'ERR_SQLITE_ERROR' && finiteInt(errcode, 0, 0x7fffffff)) {
    return {
      errorKind: 'SQLITE_NATIVE',
      code: 'ERR_SQLITE_ERROR',
      primaryCode: errcode & 0xff
    };
  }
  if (typeof codeValue === 'string' && ERROR_CODES.has(codeValue)) {
    return { errorKind: 'RUNTIME', code: codeValue, primaryCode: null };
  }
  return { errorKind: 'OTHER', code: 'OTHER', primaryCode: null };
}

function normalizeChildFields(stage, input) {
  const allowed = ownStageFields(CHILD_FIELDS, stage);
  const required = ownStageFields(CHILD_REQUIRED, stage);
  if (allowed === null || required === null) return null;
  const values = ownRecord(input ?? Object.create(null), allowed, required);
  if (!values) return null;
  const result = Object.create(null);
  for (const [key, value] of Object.entries(values)) {
    if (key === 'phase') {
      if (typeof value !== 'string' || !PHASES.has(value)) return null;
      result.phase = value;
    } else if (key === 'localClockGuard') {
      if (!['deadline_reached', 'snapshot_age_exceeded', 'deadline_and_age', 'clock_rollback', 'other'].includes(value)) return null;
      result.localClockGuard = value;
    } else if (key === 'operation') {
      result.operation = typeof value === 'string' && OPERATIONS.has(value) ? value : 'OTHER';
    } else if (key === 'errorKind') {
      if (!['OTHER', 'RUNTIME', 'SQLITE_NATIVE'].includes(value)) return null;
      result.errorKind = value;
    } else if (key === 'code') {
      if (value !== 'OTHER' && value !== 'ERR_SQLITE_ERROR' && !ERROR_CODES.has(value)) return null;
      result.code = value;
    } else if (key === 'primaryCode') {
      if (value !== null && !finiteInt(value, 0, 255)) return null;
      result.primaryCode = value;
    } else if (key === 'checkpointSequence') {
      if (value !== null && !finiteInt(value, 0)) return null;
      result.checkpointSequence = value;
    } else if (key === 'ordinal') {
      if (!finiteInt(value, 1, 64)) return null;
      result.ordinal = value;
    } else if (key === 'status') {
      if (!['completed', 'stopped'].includes(value)) return null;
      result.status = value;
    } else if (key === 'reason') {
      if (value !== null && typeof value !== 'string') return null;
      if (value === null) result.reason = null;
      else if (REASONS.has(value)) result.reason = value;
      else if (value.startsWith('PLAN_')) result.reason = 'PLAN_STOP';
      else if (value.startsWith('BUDGET_')) result.reason = 'BUDGET_STOP';
      else if (value.startsWith('CLOCK_')) result.reason = 'CLOCK_STOP';
      else if (value.startsWith('LIMIT_')) result.reason = 'LIMIT_STOP';
      else result.reason = 'OTHER';
    } else if (key === 'ok' || key === 'writerComplete' || key === 'overflow' || key === 'writeFailed' || key === 'malformed') {
      if (typeof value !== 'boolean') return null;
      result[key] = value;
    }
  }
  return result;
}

function makeRecord(schema, sequence, stage, elapsedMs, fields) {
  return { schema, sequence, stage, elapsedMs, ...fields };
}

function appendLine(writer, record, reserveFooter = true) {
  if (writer.writeFailed || (writer.overflow && reserveFooter)) return false;
  let line;
  try { line = `${JSON.stringify(record)}\n`; } catch { writer.malformed = true; return false; }
  const bytes = Buffer.byteLength(line, 'utf8');
  const recordLimit = reserveFooter ? MAX_CHILD_RECORDS - 1 : MAX_CHILD_RECORDS;
  const byteLimit = reserveFooter ? MAX_CHILD_BYTES - FOOTER_RESERVE_BYTES : MAX_CHILD_BYTES;
  if (writer.records >= recordLimit || writer.bytes + bytes > byteLimit || bytes > MAX_LINE_BYTES) {
    writer.overflow = true;
    return false;
  }
  try {
    const buffer = Buffer.from(line, 'utf8');
    let written = 0;
    while (written < buffer.length) {
      const count = writeSync(writer.fd, buffer, written, buffer.length - written);
      if (!finiteInt(count, 1, buffer.length - written)) {
        writer.writeFailed = true;
        return false;
      }
      written += count;
    }
    writer.bytes += bytes;
    writer.records += 1;
    return true;
  } catch {
    writer.writeFailed = true;
    return false;
  }
}

function elapsed(startedAt) {
  const value = Math.floor(performance.now() - startedAt);
  return finiteInt(value, 0) ? value : 0;
}

export function closeOperationsDiagnosticDescriptor(state, descriptor, close = closeSync) {
  if (!descriptor) return true;
  if (descriptor.closeAttempted === true) return descriptor.closeFailed !== true;
  descriptor.closeAttempted = true;
  descriptor.closed = true;
  try {
    close(descriptor.fd);
    descriptor.fd = -1;
    return true;
  } catch {
    descriptor.closeFailed = true;
    state.cleanupUncertain = true;
    state.flags.writeFailed = true;
    state.flags.cleanupIncomplete = true;
    return false;
  }
}

export function acquireOperationsDiagnosticSlot(state, filePath, operations = { openSync, fstatSync, closeSync }, now = () => performance.now()) {
  if (state.pendingDescriptor) {
    state.flags.writeFailed = true;
    state.flags.cleanupIncomplete = true;
    state.cleanupUncertain = true;
    return null;
  }
  if (state.slots.length >= 2) {
    state.flags.overflow = true;
    return null;
  }
  const index = state.slots.length;
  let acquired = null;
  try {
    const fd = operations.openSync(filePath, 'wx', 0o600);
    acquired = {
      slot: index, path: filePath, fd, dev: null, ino: null,
      closed: false, closeAttempted: false, closeFailed: false, startedAt: 0
    };
    state.pendingDescriptor = acquired;
    const stat = operations.fstatSync(fd);
    if (!stat || typeof stat.isFile !== 'function' || !stat.isFile() || (stat.mode & 0o777) !== 0o600) {
      state.flags.writeFailed = true;
      if (closeOperationsDiagnosticDescriptor(state, acquired, operations.closeSync)) {
        state.pendingDescriptor = null;
      }
      return null;
    }
    acquired.dev = stat.dev;
    acquired.ino = stat.ino;
    acquired.startedAt = now();
    state.slots.push(acquired);
    state.pendingDescriptor = null;
    return acquired;
  } catch {
    state.flags.writeFailed = true;
    if (acquired && closeOperationsDiagnosticDescriptor(state, acquired, operations.closeSync)) {
      state.pendingDescriptor = null;
    }
    return null;
  }
}

export function closeOperationsDiagnosticSessionFiles(state, close = closeSync) {
  let confirmed = true;
  const pending = state.pendingDescriptor;
  if (pending) {
    if (!pending.closeAttempted) {
      if (closeOperationsDiagnosticDescriptor(state, pending, close)) state.pendingDescriptor = null;
      else confirmed = false;
    } else if (pending.closeFailed) {
      confirmed = false;
    } else {
      state.pendingDescriptor = null;
    }
  }
  for (const slot of state.slots) {
    if (!closeOperationsDiagnosticDescriptor(state, slot, close)) confirmed = false;
  }
  return confirmed && state.cleanupUncertain !== true;
}

function safeSlotState(state) {
  const index = state.slots.length;
  const filePath = join(state.directory, `ops07-child-${index}.jsonl`);
  return acquireOperationsDiagnosticSlot(state, filePath);
}

export function createOperationsDiagnosticSession(input) {
  const fields = ownRecord(input, ['directory'], ['directory']);
  if (!fields || typeof fields.directory !== 'string' || fields.directory.length === 0 || fields.directory.length > 1_024) {
    throw new TypeError('The OPS diagnostic session is invalid.');
  }
  const directory = resolve(fields.directory);
  const directoryStat = lstatSync(directory);
  if (!directoryStat.isDirectory() || directoryStat.isSymbolicLink()) {
    throw new TypeError('The OPS diagnostic directory is invalid.');
  }
  const token = Object.freeze(Object.create(null));
  sessions.set(token, {
    schema: SESSION_SCHEMA,
    directory,
    startedAt: performance.now(),
    slots: [],
    parentRecords: [],
    parentBytes: 0,
    pendingDescriptor: null,
    cleanupUncertain: false,
    flags: {
      enabled: true, complete: false, missingChildCapture: false, malformed: false,
      overflow: false, writeFailed: false, abruptExit: false, cleanupIncomplete: false
    },
    disposed: false
  });
  return token;
}

export function isOperationsDiagnosticSession(token) {
  if (token === null || typeof token !== 'object' || utilTypes.isProxy(token)) return false;
  return sessions.has(token);
}

export function prepareOperationsChildCapture(token) {
  const state = sessions.get(token);
  if (!state || state.disposed) return null;
  const slot = safeSlotState(state);
  if (!slot) return null;
  return Object.freeze({ slot: slot.slot, fd: slot.fd });
}

export function closeOperationsChildCapture(token, slotNumber) {
  const state = sessions.get(token);
  if (!state || !finiteInt(slotNumber, 0, 1)) return false;
  const slot = state.slots[slotNumber];
  if (!slot) return true;
  if (slot.closeAttempted === true) return slot.closeFailed !== true;
  return closeOperationsDiagnosticDescriptor(state, slot);
}

export function recordOperationsSupervisor(token, slotNumber, stage, input = Object.create(null)) {
  const state = sessions.get(token);
  if (!state || state.disposed) return false;
  try {
    const allowed = ownStageFields(SUPERVISOR_FIELDS, stage);
    if (allowed === null) { state.flags.malformed = true; return false; }
    if (!finiteInt(slotNumber, 0, 1)) return false;
    const values = ownRecord(input, allowed, allowed);
    if (!values) { state.flags.malformed = true; return false; }
    const clean = Object.create(null);
    for (const [key, value] of Object.entries(values)) {
      if (key === 'pidPresent' || key === 'settled') {
        if (typeof value !== 'boolean') return false;
        clean[key] = value;
      } else if (key === 'code') {
        clean.code = value === null ? null : typeof value === 'string' && ERROR_CODES.has(value) ? value : 'OTHER';
      } else if (key === 'outcome') {
        if (!['completed', 'stopped', 'shutdown'].includes(value)) return false;
        clean.outcome = value;
      } else if (key === 'exitCode') {
        if (value !== null && !finiteInt(value, 0, 255)) return false;
        clean.exitCode = value;
      } else if (key === 'signal') {
        if (value !== null && !SIGNALS.has(value)) return false;
        clean.signal = value;
      } else if (key === 'childRuntimeMs') {
        if (!finiteInt(value, 0, 600_000)) return false;
        clean.childRuntimeMs = value;
      } else if (key === 'generation' || key === 'fence') {
        if (!finiteInt(value, 1)) return false;
        clean[key] = value;
      }
    }
    if (state.parentRecords.length >= MAX_PARENT_RECORDS) { state.flags.overflow = true; return false; }
    const record = {
      schema: SESSION_SCHEMA, sequence: state.parentRecords.length + 1,
      stage, slot: slotNumber, elapsedMs: elapsed(state.startedAt), ...clean
    };
    const lineBytes = Buffer.byteLength(JSON.stringify(record), 'utf8') + 1;
    if (state.parentBytes + lineBytes > MAX_PARENT_BYTES - FOOTER_RESERVE_BYTES) {
      state.flags.overflow = true;
      return false;
    }
    state.parentBytes += lineBytes;
    state.parentRecords.push(record);
    return true;
  } catch {
    state.flags.malformed = true;
    return false;
  }
}

export function setOperationsDiagnosticPhase(phase, sequence = null) {
  if (!runnerWriter) return false;
  runnerWriter.phase = typeof phase === 'string' && PHASES.has(phase) ? phase : 'other';
  runnerWriter.checkpointSequence = sequence === null || finiteInt(sequence, 0) ? sequence : null;
  return true;
}

export function enableOperationsRunnerCaptureFromFd3() {
  if (runnerWriter) return false;
  try {
    const stat = fstatSync(3);
    if (!stat.isFile()) return false;
    runnerWriter = {
      fd: 3, startedAt: performance.now(), records: 0, bytes: 0,
      overflow: false, writeFailed: false, malformed: false,
      phase: 'runner_start', checkpointSequence: null, closed: false
    };
    appendLine(runnerWriter, makeRecord(CHILD_SCHEMA, 1, 'header', 0, Object.create(null)));
    return true;
  } catch {
    return false;
  }
}

export function recordOperationsDiagnostic(stage, input = Object.create(null)) {
  const writer = runnerWriter;
  if (!writer || writer.closed) return false;
  try {
    const clean = normalizeChildFields(stage, input);
    if (!clean) { writer.malformed = true; return false; }
    const record = makeRecord(CHILD_SCHEMA, writer.records + 1, stage, elapsed(writer.startedAt), clean);
    return appendLine(writer, record);
  } catch {
    writer.malformed = true;
    return false;
  }
}

export function recordOperationsDiagnosticError(stage, error, metadata = Object.create(null)) {
  if (!runnerWriter || runnerWriter.closed) return false;
  try {
    const allowed = stage === 'storage_origin' ? ['operation', 'localClockGuard']
      : stage === 'controller_error' ? ['phase', 'checkpointSequence']
        : stage === 'runner_error' ? ['phase'] : null;
    if (!allowed) { runnerWriter.malformed = true; return false; }
    const cleanMeta = ownRecord(metadata, allowed);
    if (!cleanMeta) { runnerWriter.malformed = true; return false; }
    const projection = safeErrorProjection(error);
    const values = { ...cleanMeta, ...projection };
    return recordOperationsDiagnostic(stage, values);
  } catch {
    runnerWriter.malformed = true;
    return false;
  }
}

export function recordOperationsDiagnosticResult(result) {
  if (!runnerWriter || runnerWriter.closed) return false;
  try {
    const values = pickOwnValues(result, ['status', 'checkpointSequence'], ['reason']);
    if (!values) { runnerWriter.malformed = true; return false; }
    const status = values.status === 'completed' ? 'completed'
      : ['stopped', 'external-reconciliation-required'].includes(values.status) ? 'stopped' : null;
    if (!status) { runnerWriter.malformed = true; return false; }
    const reason = values.reason ?? null;
    const checkpointSequence = values.checkpointSequence ?? null;
    const normalizedReason = reason === null ? null
      : (typeof reason === 'string' && REASONS.has(reason)) ? reason
        : (typeof reason === 'string' && reason.startsWith('PLAN_')) ? 'PLAN_STOP'
          : (typeof reason === 'string' && reason.startsWith('BUDGET_')) ? 'BUDGET_STOP'
            : (typeof reason === 'string' && reason.startsWith('CLOCK_')) ? 'CLOCK_STOP'
              : (typeof reason === 'string' && reason.startsWith('LIMIT_')) ? 'LIMIT_STOP' : 'OTHER';
    return recordOperationsDiagnostic('controller_result', { status, reason: normalizedReason, checkpointSequence });
  } catch {
    runnerWriter.malformed = true;
    return false;
  }
}

export function recordOperationsDiagnosticProgress(stage, ordinal, sequence) {
  if (!runnerWriter || runnerWriter.closed) return false;
  return recordOperationsDiagnostic(stage, { ordinal, checkpointSequence: sequence });
}

export function finishOperationsRunnerCapture() {
  const writer = runnerWriter;
  if (!writer || writer.closed) return false;
  const status = !writer.writeFailed && !writer.overflow && !writer.malformed;
  const footer = makeRecord(CHILD_SCHEMA, writer.records + 1, 'footer', elapsed(writer.startedAt), {
    writerComplete: status, overflow: writer.overflow, writeFailed: writer.writeFailed, malformed: writer.malformed
  });
  const footerWritten = appendLine(writer, footer, false);
  writer.closed = true;
  runnerWriter = null;
  try { closeSync(writer.fd); } catch { writer.writeFailed = true; }
  return footerWritten;
}

function parseChildRecords(bytes, slotState) {
  const text = bytes.toString('utf8');
  const lines = text.split('\n');
  if (lines.at(-1) === '') lines.pop();
  if (lines.length < 2 || lines.length > MAX_CHILD_RECORDS) return { records: [], malformed: true, overflow: lines.length > MAX_CHILD_RECORDS, footer: false };
  const records = [];
  let malformed = false;
  let previousElapsedMs = -1;
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    if (Buffer.byteLength(line, 'utf8') > MAX_LINE_BYTES) { malformed = true; break; }
    let value;
    try { value = JSON.parse(line); } catch { malformed = true; break; }
    if (!isPlainRecord(value)) { malformed = true; break; }
    const fieldsForStage = ownStageFields(CHILD_FIELDS, value.stage);
    if (fieldsForStage === null) { malformed = true; break; }
    const common = ownRecord(value, ['schema', 'sequence', 'stage', 'elapsedMs', ...fieldsForStage]);
    if (!common || common.schema !== CHILD_SCHEMA || common.sequence !== index + 1
        || !finiteInt(common.elapsedMs, 0, 600_000) || typeof common.stage !== 'string') {
      malformed = true; break;
    }
    const stageFields = fieldsForStage;
    if ((index === 0 && common.stage !== 'header')
        || (common.stage === 'header' && index !== 0)
        || common.stage === 'footer' && index !== lines.length - 1) { malformed = true; break; }
    if (common.elapsedMs < previousElapsedMs) { malformed = true; break; }
    previousElapsedMs = common.elapsedMs;
    const fields = Object.create(null);
    for (const key of stageFields) if (Object.hasOwn(common, key)) fields[key] = common[key];
    const clean = normalizeChildFields(common.stage, fields);
    if (!clean) { malformed = true; break; }
    records.push({ sequence: common.sequence, stage: common.stage, elapsedMs: common.elapsedMs, ...clean });
  }
  const footerRecord = records.at(-1)?.stage === 'footer' ? records.at(-1) : null;
  return {
    slot: slotState.slot, records, malformed, overflow: false,
    footer: footerRecord !== null,
    writerComplete: footerRecord?.writerComplete === true,
    footerFlags: footerRecord ? {
      overflow: footerRecord.overflow, writeFailed: footerRecord.writeFailed, malformed: footerRecord.malformed
    } : null
  };
}

function sanitizeFinalState(input) {
  if (input === undefined || input === null) return { available: false };
  const fields = ownRecord(input, ['available', 'jobs', 'reservations'], ['available']);
  if (!fields || typeof fields.available !== 'boolean') return { available: false };
  if (!fields.available) return { available: false };
  const jobRows = ownDenseArrayValues(fields.jobs, 2, 2);
  if (!jobRows) return { available: false };
  const jobs = [];
  for (let index = 0; index < 2; index += 1) {
    const row = ownRecord(jobRows[index], [
      'workState', 'workflowState', 'generation', 'operationFence', 'workflowFence', 'revision',
      'checkpointSequence', 'resultCount', 'resultHash', 'cancellationRequested',
      'leaseOwnerPresent', 'leaseFenceMatches', 'leaseExpired', 'eventCounts'
    ]);
    if (!row || !WORK_STATES.has(row.workState) || !WORKFLOW_STATES.has(row.workflowState)
        || !finiteInt(row.generation, 0) || (row.operationFence !== null && !finiteInt(row.operationFence, 0))
        || !finiteInt(row.workflowFence, 0) || !finiteInt(row.revision, 0)
        || !finiteInt(row.checkpointSequence, 1) || !finiteInt(row.resultCount, 0, 32)
        || !(row.resultHash === null || typeof row.resultHash === 'string' && /^[0-9a-f]{64}$/u.test(row.resultHash))
        || typeof row.cancellationRequested !== 'boolean' || typeof row.leaseOwnerPresent !== 'boolean'
        || typeof row.leaseFenceMatches !== 'boolean' || typeof row.leaseExpired !== 'boolean'
        || !ownDenseArrayValues(row.eventCounts, 16)) {
      return { available: false };
    }
    const eventCounts = [];
    const eventRows = ownDenseArrayValues(row.eventCounts, 16);
    for (let i = 0; i < eventRows.length; i += 1) {
      const event = ownRecord(eventRows[i], ['type', 'count'], ['type', 'count']);
      if (!event || typeof event.type !== 'string' || !EVENT_TYPES.has(event.type) || !finiteInt(event.count, 0)) return { available: false };
      eventCounts.push({ type: event.type, count: event.count });
    }
    jobs.push({
      workState: row.workState, workflowState: row.workflowState, generation: row.generation,
      operationFence: row.operationFence, workflowFence: row.workflowFence,
      revision: row.revision, checkpointSequence: row.checkpointSequence, resultCount: row.resultCount,
      resultHash: row.resultHash, cancellationRequested: row.cancellationRequested,
      leaseOwnerPresent: row.leaseOwnerPresent, leaseFenceMatches: row.leaseFenceMatches,
      leaseExpired: row.leaseExpired, eventCounts
    });
  }
  const reservations = ownRecord(fields.reservations, ['reservedDiskBytes', 'reservedEventSlots', 'pendingRecords'],
    ['reservedDiskBytes', 'reservedEventSlots', 'pendingRecords']);
  if (!reservations || !finiteInt(reservations.reservedDiskBytes, 0)
      || !finiteInt(reservations.reservedEventSlots, 0) || !finiteInt(reservations.pendingRecords, 0)) {
    return { available: false };
  }
  return { available: true, jobs, reservations };
}

function hasCompleteSupervisorLifecycle(supervisor) {
  return [0, 1].every(slot => {
    const events = supervisor.filter(record => record.slot === slot);
    const spawns = events.filter(record => record.stage === 'spawn');
    const exits = events.filter(record => record.stage === 'child_exit');
    const finishes = events.filter(record => record.stage === 'finish_result');
    if (spawns.length !== 1 || exits.length !== 1 || finishes.length === 0) return false;
    const [spawn] = spawns;
    const [exit] = exits;
    const lastFinish = finishes.at(-1);
    return spawn.sequence < exit.sequence && exit.sequence < lastFinish.sequence && lastFinish.settled === true;
  });
}

function incompleteFlags(state) {
  return {
    enabled: true,
    complete: false,
    missingChildCapture: state.flags.missingChildCapture,
    malformed: state.flags.malformed,
    overflow: state.flags.overflow,
    writeFailed: state.flags.writeFailed,
    abruptExit: state.flags.abruptExit,
    cleanupIncomplete: state.flags.cleanupIncomplete
  };
}

function overflowSummary(state) {
  const flags = incompleteFlags(state);
  flags.complete = false;
  flags.overflow = true;
  return freezeSnapshot({ schema: SESSION_SCHEMA, overflowSummary: true, flags });
}

export function readOperationsDiagnosticSession(token, input = Object.create(null)) {
  const state = sessions.get(token);
  if (!state || state.disposed) return { schema: SESSION_SCHEMA, flags: { enabled: false, complete: false } };
  const inputFields = ownRecord(input, ['finalState', 'cleanupIncomplete', 'abruptSlots']);
  if (!inputFields) state.flags.malformed = true;
  if (inputFields?.cleanupIncomplete === true) state.flags.cleanupIncomplete = true;
  let abruptSlots = [];
  if (inputFields && Object.hasOwn(inputFields, 'abruptSlots')) {
    const suppliedSlots = ownDenseArrayValues(inputFields.abruptSlots, 2);
    if (!suppliedSlots || suppliedSlots.some(slot => !finiteInt(slot, 0, 1))) state.flags.malformed = true;
    else abruptSlots = suppliedSlots;
  }
  if (state.pendingDescriptor && !state.pendingDescriptor.closeAttempted
      && closeOperationsDiagnosticDescriptor(state, state.pendingDescriptor)) {
    state.pendingDescriptor = null;
  }
  if (state.slots.length !== 2) state.flags.missingChildCapture = true;
  const children = [];
  for (let slotIndex = 0; slotIndex < state.slots.length; slotIndex += 1) {
    const slot = state.slots[slotIndex];
    if (!slot.closed) closeOperationsChildCapture(token, slotIndex);
    try {
      const stat = lstatSync(slot.path);
      if (!stat.isFile() || stat.isSymbolicLink() || stat.dev !== slot.dev || stat.ino !== slot.ino
          || stat.size > MAX_CHILD_BYTES) {
        if (stat.size > MAX_CHILD_BYTES) state.flags.overflow = true;
        else state.flags.malformed = true;
        continue;
      }
      const bytes = readFileSync(slot.path);
      const parsed = parseChildRecords(bytes, slot);
      if (parsed.malformed) state.flags.malformed = true;
      if (parsed.overflow) state.flags.overflow = true;
      if (parsed.footerFlags?.malformed) state.flags.malformed = true;
      if (parsed.footerFlags?.overflow) state.flags.overflow = true;
      if (parsed.footerFlags?.writeFailed) state.flags.writeFailed = true;
      if (!parsed.footer) state.flags.abruptExit = true;
      if (abruptSlots.includes(slotIndex)) state.flags.abruptExit = true;
      children.push(parsed);
    } catch {
      state.flags.missingChildCapture = true;
    }
  }
  if (children.length !== 2) state.flags.missingChildCapture = true;
  const finalState = sanitizeFinalState(inputFields?.finalState);
  if (!finalState.available) state.flags.cleanupIncomplete = true;
  const allFooters = children.length === 2 && children.every(child => child.footer && child.writerComplete
    && !child.malformed && !child.overflow && child.records.length >= 3
    && child.records[0]?.stage === 'header' && child.records.at(-1)?.stage === 'footer');
  const completeSupervisor = hasCompleteSupervisorLifecycle(state.parentRecords);
  const aggregate = {
    schema: SESSION_SCHEMA,
    slots: children,
    supervisor: state.parentRecords,
    finalState
  };
  const recordCount = children.reduce((count, child) => count + child.records.length, 0) + state.parentRecords.length + 1;
  const flags = incompleteFlags(state);
  flags.complete = allFooters && finalState.available && !flags.malformed && !flags.overflow
    && !flags.writeFailed && !flags.cleanupIncomplete && !flags.missingChildCapture && !flags.abruptExit
    && completeSupervisor;
  const capture = { ...aggregate, flags };
  const totalBytes = Buffer.byteLength(`${CAPTURE_PREFIX}${JSON.stringify(capture)}\n`, 'utf8');
  const supervisorBytes = Buffer.byteLength(JSON.stringify({ supervisor: state.parentRecords, finalState, flags }), 'utf8');
  if (state.parentRecords.length > MAX_PARENT_RECORDS || supervisorBytes > MAX_PARENT_BYTES
      || recordCount > 300 || totalBytes > MAX_CAPTURE_BYTES) {
    state.flags.overflow = true;
    return overflowSummary(state);
  }
  return freezeSnapshot(capture);
}

export function disposeOperationsDiagnosticSession(token) {
  const state = sessions.get(token);
  if (!state) return false;
  if (state.disposed) return state.cleanupUncertain !== true;
  const closed = closeOperationsDiagnosticSessionFiles(state);
  state.disposed = true;
  return closed;
}

export function parseOperationsCaptureFromTap(text) {
  if (typeof text !== 'string') return { valid: false, reason: 'invalid_input', capture: null };
  let found = null;
  let count = 0;
  for (const line of text.split(/\r?\n/u)) {
    const captureLine = /^ {0,16}# OPS07_CAPTURE_V1 (.*)$/u.exec(line);
    if (!captureLine) continue;
    count += 1;
    if (count > 1) return { valid: false, reason: 'duplicate', capture: null };
    const payload = captureLine[1];
    if (Buffer.byteLength(`${CAPTURE_PREFIX}${payload}\n`, 'utf8') > MAX_CAPTURE_BYTES) {
      return { valid: false, reason: 'oversize', capture: null };
    }
    try { found = JSON.parse(payload); } catch { return { valid: false, reason: 'malformed', capture: null }; }
  }
  if (count !== 1 || !isPlainRecord(found)) return { valid: false, reason: 'missing', capture: null };
  const summary = ownRecord(found, ['schema', 'overflowSummary', 'flags'], ['schema', 'overflowSummary', 'flags']);
  if (summary?.overflowSummary === true) {
    const summaryFlags = ownRecord(summary.flags, [
      'enabled', 'complete', 'missingChildCapture', 'malformed', 'overflow', 'writeFailed', 'abruptExit', 'cleanupIncomplete'
    ], ['enabled', 'complete', 'missingChildCapture', 'malformed', 'overflow', 'writeFailed', 'abruptExit', 'cleanupIncomplete']);
    if (summary.schema !== SESSION_SCHEMA || !summaryFlags || summaryFlags.enabled !== true
        || summaryFlags.complete !== false || summaryFlags.overflow !== true
        || ['missingChildCapture', 'malformed', 'writeFailed', 'abruptExit', 'cleanupIncomplete']
          .some(key => typeof summaryFlags[key] !== 'boolean')) {
      return { valid: false, reason: 'invalid_shape', capture: null };
    }
    return {
      valid: true,
      reason: null,
      capture: freezeSnapshot({ schema: SESSION_SCHEMA, overflowSummary: true, flags: { ...summaryFlags } })
    };
  }
  const flags = ownRecord(found.flags, [
    'enabled', 'complete', 'missingChildCapture', 'malformed', 'overflow', 'writeFailed', 'abruptExit', 'cleanupIncomplete'
  ], ['enabled', 'complete', 'missingChildCapture', 'malformed', 'overflow', 'writeFailed', 'abruptExit', 'cleanupIncomplete']);
  if (!flags || flags.enabled !== true || typeof flags.complete !== 'boolean'
      || ['missingChildCapture', 'malformed', 'overflow', 'writeFailed', 'abruptExit', 'cleanupIncomplete']
        .some(key => typeof flags[key] !== 'boolean')) {
    return { valid: false, reason: 'invalid_shape', capture: null };
  }
  const top = ownRecord(found, ['schema', 'flags', 'slots', 'supervisor', 'finalState'],
    ['schema', 'flags', 'slots', 'supervisor', 'finalState']);
  if (!top || top.schema !== SESSION_SCHEMA || !Array.isArray(top.slots) || utilTypes.isProxy(top.slots)
      || top.slots.length !== 2 || !Array.isArray(top.supervisor) || utilTypes.isProxy(top.supervisor)
      || top.supervisor.length > MAX_PARENT_RECORDS) {
    return { valid: false, reason: 'invalid_shape', capture: null };
  }
  const slots = [];
  let childRecordCount = 0;
  for (let index = 0; index < top.slots.length; index += 1) {
    const desc = Object.getOwnPropertyDescriptor(top.slots, String(index));
    const slot = desc && Object.hasOwn(desc, 'value') ? ownRecord(desc.value,
      ['slot', 'records', 'malformed', 'overflow', 'footer', 'writerComplete', 'footerFlags'],
      ['slot', 'records', 'malformed', 'overflow', 'footer', 'writerComplete', 'footerFlags']) : null;
    if (!slot || slot.slot !== index || typeof slot.malformed !== 'boolean' || typeof slot.overflow !== 'boolean'
        || typeof slot.footer !== 'boolean' || typeof slot.writerComplete !== 'boolean'
        || !Array.isArray(slot.records) || utilTypes.isProxy(slot.records)
        || slot.records.length < 2 || slot.records.length > MAX_CHILD_RECORDS) {
      return { valid: false, reason: 'invalid_shape', capture: null };
    }
    const records = [];
    let previousElapsedMs = -1;
    for (let sequence = 0; sequence < slot.records.length; sequence += 1) {
      const recordDesc = Object.getOwnPropertyDescriptor(slot.records, String(sequence));
      const candidate = recordDesc && Object.hasOwn(recordDesc, 'value') ? recordDesc.value : null;
      if (!isPlainRecord(candidate)) return { valid: false, reason: 'invalid_shape', capture: null };
      let recordKeys;
      try { recordKeys = Reflect.ownKeys(candidate); } catch { return { valid: false, reason: 'invalid_shape', capture: null }; }
      const stageDescriptor = Object.getOwnPropertyDescriptor(candidate, 'stage');
      const stage = stageDescriptor && Object.hasOwn(stageDescriptor, 'value') ? stageDescriptor.value : null;
      const fieldsForStage = ownStageFields(CHILD_FIELDS, stage);
      if (fieldsForStage === null) return { valid: false, reason: 'invalid_shape', capture: null };
      const row = ownRecord(candidate, ['sequence', 'stage', 'elapsedMs', ...fieldsForStage], ['sequence', 'stage', 'elapsedMs']);
      if (!row || row.sequence !== sequence + 1 || row.stage !== stage || !finiteInt(row.elapsedMs, 0, 600_000)
          || (sequence === 0 && stage !== 'header') || (stage === 'header' && sequence !== 0)
          || (stage === 'footer' && sequence !== slot.records.length - 1)
          || row.elapsedMs < previousElapsedMs) {
        return { valid: false, reason: 'invalid_shape', capture: null };
      }
      previousElapsedMs = row.elapsedMs;
      const stageFields = Object.create(null);
      for (const key of fieldsForStage) if (Object.hasOwn(row, key)) stageFields[key] = row[key];
      const normalized = normalizeChildFields(stage, stageFields);
      if (!normalized) return { valid: false, reason: 'invalid_shape', capture: null };
      records.push({ sequence: row.sequence, stage, elapsedMs: row.elapsedMs, ...normalized });
    }
    const childBytes = records.reduce((total, record) => total
      + Buffer.byteLength(`${JSON.stringify({ schema: CHILD_SCHEMA, ...record })}\n`, 'utf8'), 0);
    if (childBytes > MAX_CHILD_BYTES) return { valid: false, reason: 'invalid_shape', capture: null };
    childRecordCount += records.length;
    const footerRecord = records.at(-1)?.stage === 'footer' ? records.at(-1) : null;
    const actualFooter = footerRecord !== null;
    const actualWriterComplete = footerRecord?.writerComplete === true;
    if (actualFooter !== slot.footer || actualWriterComplete !== slot.writerComplete) {
      return { valid: false, reason: 'invalid_shape', capture: null };
    }
    const footerFlags = ownRecord(slot.footerFlags, ['overflow', 'writeFailed', 'malformed'],
      ['overflow', 'writeFailed', 'malformed']);
    if ((footerRecord && (!footerFlags || footerFlags.overflow !== footerRecord.overflow
        || footerFlags.writeFailed !== footerRecord.writeFailed || footerFlags.malformed !== footerRecord.malformed))
        || (!footerRecord && slot.footerFlags !== null)) return { valid: false, reason: 'invalid_shape', capture: null };
    slots.push({ slot: index, records, malformed: slot.malformed, overflow: slot.overflow,
      footer: slot.footer, writerComplete: slot.writerComplete,
      footerFlags: footerFlags ? { ...footerFlags } : null });
  }
  const supervisor = [];
  for (let index = 0; index < top.supervisor.length; index += 1) {
    const desc = Object.getOwnPropertyDescriptor(top.supervisor, String(index));
    const candidate = desc && Object.hasOwn(desc, 'value') ? desc.value : null;
    if (!isPlainRecord(candidate)) return { valid: false, reason: 'invalid_shape', capture: null };
    const stageDescriptor = Object.getOwnPropertyDescriptor(candidate, 'stage');
    const stage = stageDescriptor && Object.hasOwn(stageDescriptor, 'value') ? stageDescriptor.value : null;
    const allowedFields = ownStageFields(SUPERVISOR_FIELDS, stage);
    if (allowedFields === null) return { valid: false, reason: 'invalid_shape', capture: null };
    const row = ownRecord(candidate, ['schema', 'sequence', 'stage', 'slot', 'elapsedMs', ...allowedFields],
      ['schema', 'sequence', 'stage', 'slot', 'elapsedMs', ...allowedFields]);
    if (!row || row.schema !== SESSION_SCHEMA || row.sequence !== index + 1 || !finiteInt(row.slot, 0, 1)
        || !finiteInt(row.elapsedMs, 0, 600_000)) return { valid: false, reason: 'invalid_shape', capture: null };
    const cleanFields = Object.create(null);
    for (const key of allowedFields) if (Object.hasOwn(row, key)) cleanFields[key] = row[key];
    const check = Object.create(null);
    for (const [key, value] of Object.entries(cleanFields)) {
      if (key === 'pidPresent' || key === 'settled') {
        if (typeof value !== 'boolean') return { valid: false, reason: 'invalid_shape', capture: null };
        check[key] = value;
      } else if (key === 'code') {
        if (value !== null && value !== 'OTHER' && !ERROR_CODES.has(value)) return { valid: false, reason: 'invalid_shape', capture: null };
        check.code = value;
      } else if (key === 'outcome') {
        if (!['completed', 'stopped', 'shutdown'].includes(value)) return { valid: false, reason: 'invalid_shape', capture: null };
        check.outcome = value;
      } else if (key === 'exitCode') {
        if (value !== null && !finiteInt(value, 0, 255)) return { valid: false, reason: 'invalid_shape', capture: null };
        check.exitCode = value;
      } else if (key === 'signal') {
        if (value !== null && !SIGNALS.has(value)) return { valid: false, reason: 'invalid_shape', capture: null };
        check.signal = value;
      } else if (key === 'childRuntimeMs') {
        if (!finiteInt(value, 0, 600_000)) return { valid: false, reason: 'invalid_shape', capture: null };
        check.childRuntimeMs = value;
      } else if (key === 'generation' || key === 'fence') {
        if (!finiteInt(value, 1)) return { valid: false, reason: 'invalid_shape', capture: null };
        check[key] = value;
      }
    }
    supervisor.push({ sequence: row.sequence, stage, slot: row.slot, elapsedMs: row.elapsedMs, ...check });
  }
  const finalState = sanitizeFinalState(top.finalState);
  const totalRecords = childRecordCount + supervisor.length + 1;
  const capture = {
    schema: SESSION_SCHEMA,
    slots,
    supervisor,
    finalState,
    flags: { ...flags }
  };
  const payloadBytes = Buffer.byteLength(JSON.stringify(capture), 'utf8');
  const wireSupervisor = supervisor.map(row => ({
    schema: SESSION_SCHEMA, sequence: row.sequence, stage: row.stage, slot: row.slot,
    elapsedMs: row.elapsedMs, ...Object.fromEntries(Object.entries(row).filter(([key]) =>
      !['sequence', 'stage', 'slot', 'elapsedMs'].includes(key)))
  }));
  const supervisorBytes = Buffer.byteLength(JSON.stringify({ supervisor: wireSupervisor, finalState, flags }), 'utf8');
  if (totalRecords > 300 || payloadBytes + Buffer.byteLength(CAPTURE_PREFIX, 'utf8') + 1 > MAX_CAPTURE_BYTES
      || supervisorBytes > MAX_PARENT_BYTES
      || (flags.complete && (slots.some(slot => !slot.footer || !slot.writerComplete || slot.malformed || slot.overflow
        || slot.records.length < 3 || slot.records[0]?.stage !== 'header' || slot.records.at(-1)?.stage !== 'footer'
        || slot.footerFlags?.overflow || slot.footerFlags?.writeFailed || slot.footerFlags?.malformed)
        || !finalState.available || flags.missingChildCapture || flags.malformed || flags.overflow
        || flags.writeFailed || flags.abruptExit || flags.cleanupIncomplete
        || !hasCompleteSupervisorLifecycle(supervisor)))) {
    return { valid: false, reason: 'invalid_shape', capture: null };
  }
  return { valid: true, reason: null, capture: freezeSnapshot(capture) };
}

export const OPS07_CAPTURE_LINE_PREFIX = CAPTURE_PREFIX;
export const OPS07_CAPTURE_FLAG = CAPTURE_FLAG;
export const OPS07_CAPTURE_LIMITS = Object.freeze({
  childRecords: MAX_CHILD_RECORDS, childBytes: MAX_CHILD_BYTES,
  supervisorRecords: MAX_PARENT_RECORDS, supervisorBytes: MAX_PARENT_BYTES,
  totalBytes: MAX_CAPTURE_BYTES
});
