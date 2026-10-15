import { types as utilTypes } from 'node:util';
import { closedErrorProjection } from './closed-diagnostics.js';

const PREFIX = 'OPS07_C04_COMPLETION_V1 ';
const SCHEMA = 'agent-harness.ops07-c04-completion.v1';
const MAX_BYTES = 4_096;
const MAX_TAP_BYTES = 16 * 1024 * 1024;
const WORK = new Set(['queued', 'running', 'parked', 'unknown', 'completed', 'failed', 'cancelled']);
const WORKFLOW = new Set(['queued', 'running', 'awaiting_response', 'reconciliation_pending',
  'review_required', 'succeeded', 'failed', 'cancelled']);
const REASONS = new Set(['LOCAL_EXECUTION_STOPPED', 'SAFE_RESTART_RESUME', 'CANCELLED',
  'EXTERNAL_OUTCOME_UNRESOLVED', 'WORKER_GENERATION_LIMIT']);
const CONTROLLER_REASONS = new Set(['CHECKPOINT_UNSUPPORTED', 'CONTROLLER_INPUT_INVALID',
  'EXTERNAL_OUTCOME_UNRESOLVED', 'LEASE_FENCED', 'LOCAL_CONTROLLER_STOPPED', 'NO_DETERMINISTIC_READY_STEP',
  'PENDING_STEP_IDENTITY_INVALID', 'PLAN_INPUT_REQUIRED', 'WORKFLOW_NOT_RUNNING', 'WORKFLOW_SNAPSHOT_CHANGED',
  'CHECKPOINT_LIMITS_IDENTITY_INVALID', 'CHECKPOINT_PLAN_IDENTITY_INVALID', 'CONTROLLER_CHECKPOINT_INVALID',
  'CONTROLLER_CHECKPOINT_TOO_LARGE', 'BUDGET_CHECKPOINT_INCONSISTENT', 'CLOCK_INCONSISTENT', 'INVALID_DEADLINE',
  'LOCAL_RESULT_DUPLICATE', 'LOCAL_RESULT_INVALID', 'LOCAL_STEP_NOT_PENDING', 'LOCAL_COMPLETION_BUSY',
  'LOCAL_COMPLETION_OUTCOME_UNKNOWN', 'LOCAL_ADMISSION_BUSY', 'LOCAL_ADMISSION_OUTCOME_UNKNOWN',
  'PLAN_STOP', 'BUDGET_STOP', 'CLOCK_STOP', 'LIMIT_STOP']);
const PHASES = new Set(['runner_start', 'store_open', 'resource_ceilings', 'claim_read', 'controller',
  'initial_snapshot', 'initial_admission', 'loop_snapshot', 'completion', 'completion_resolution',
  'admission_resolution', 'next_admission', 'store_close', 'other']);
const TEST_PHASES = new Set(['worker_start', 'runner_wait', 'held_stop', 'recovered_stop',
  'restart_start', 'restart_wait', 'restart_stop', 'done']);
const CODES = new Set(['STORE_BUSY', 'STORE_WRITE_FAILED', 'STORE_OPEN_FAILED', 'STORE_CLOSED',
  'UNSUPPORTED_SCHEMA_VERSION', 'UNSUPPORTED_RUNTIME', 'SQLITE_UNSUPPORTED', 'CONCURRENT_MODIFICATION',
  'STALE_FENCE', 'LEASE_REQUIRED', 'LEASE_EXPIRED', 'ILLEGAL_TRANSITION', 'OUTCOME_UNRESOLVED',
  'INVALID_INPUT', 'CHECKPOINT_CORRUPT', 'CLOCK_ROLLBACK', 'ERR_SQLITE_ERROR']);
const SIGNALS = new Set(['SIGTERM', 'SIGKILL', 'SIGINT', 'SIGHUP', 'SIGQUIT']);
const LIFECYCLES = new Set(['starting', 'accepting', 'saturated', 'stopping', 'stopped', 'degraded']);
const OUTCOMES = new Set(['completed', 'stopped', 'shutdown']);
const CHILD_ERRORS = new Set(['runner_error', 'controller_error', 'storage_origin']);
const SUPERVISOR_ERRORS = new Set(['spawn_error', 'child_error', 'runner_identity_failure']);
const ERROR_STAGES = new Set([...CHILD_ERRORS, ...SUPERVISOR_ERRORS]);
const DIRECT_PHASES = new Set(['store_open', 'resource_ceilings', 'claim_read', 'store_close']);

function data(value, key) {
  if (value === null || (typeof value !== 'object' && typeof value !== 'function') || utilTypes.isProxy(value)) return undefined;
  try { return Object.getOwnPropertyDescriptor(value, key)?.value; } catch { return undefined; }
}
function integer(value, max = Number.MAX_SAFE_INTEGER) {
  return Number.isSafeInteger(value) && value >= 0 && value <= max ? value : null;
}
function choice(value, values, fallback = 'unobserved') { return values.has(value) ? value : fallback; }
function reason(value, values) { return value == null ? 'none' : choice(value, values, 'OTHER'); }
function array(value, max) {
  if (utilTypes.isProxy(value) || !Array.isArray(value) || value.length > max) return null;
  const result = [];
  for (let i = 0; i < value.length; i += 1) {
    const entry = Object.getOwnPropertyDescriptor(value, String(i));
    if (!entry || !Object.hasOwn(entry, 'value')) return null;
    result.push(entry.value);
  }
  return result;
}

// Project only the item the existing work-page poll already matched. No query,
// identifiers, payloads, or worker health/resource reads are introduced here.
export function projectC04WorkPage(item) {
  return { status: item == null ? 'missing' : 'observed', workState: choice(data(item, 'state'), WORK),
    generation: integer(data(item, 'generation')), reason: reason(data(item, 'reasonCode'), REASONS) };
}

export function observeC04PreCleanup(input) {
  const page = data(input, 'page');
  return { timing: data(input, 'observed') === true ? 'before_cleanup' : 'not_observed',
    pageSource: 'matching_last_read_work_page', workerSource: 'in_memory_before_cleanup',
    page: { status: choice(data(page, 'status'), new Set(['observed', 'missing']), 'not_observed'),
      workState: choice(data(page, 'workState'), WORK), generation: integer(data(page, 'generation')),
      reason: data(page, 'reason') === 'none' ? 'none' : choice(data(page, 'reason'), REASONS, 'OTHER') },
    worker: { role: choice(data(input, 'role'), new Set(['initial', 'restart'])),
      lifecycle: choice(data(input, 'lifecycle'), LIFECYCLES), activeChildCount: integer(data(input, 'activeChildCount')) } };
}

// This is a separate, additional point lookup before cleanup, not the row
// observed by the completed-state predicate. Its source/timing stay explicit.
export function readC04FinalRow(database, jobId) {
  let status = 'not_observed'; let errorCode = 'none'; let row;
  if (database && typeof jobId === 'string') {
    try {
      row = database.prepare(`SELECT o.state AS work_state, o.reason_code, o.generation,
        o.fence_token AS operation_fence, w.state AS workflow_state,
        w.fence_token AS workflow_fence, w.checkpoint_sequence
        FROM operations_work o JOIN workflows w ON w.workflow_id=o.workflow_id WHERE o.job_id=?`).get(jobId);
      status = row ? 'observed' : 'missing';
    } catch (error) { status = 'error'; errorCode = closedErrorProjection(error).code; }
  }
  return { status, errorCode,
    workState: choice(data(row, 'work_state'), WORK), workflowState: choice(data(row, 'workflow_state'), WORKFLOW),
    reason: reason(data(row, 'reason_code'), REASONS),
    generation: integer(data(row, 'generation')), operationFence: integer(data(row, 'operation_fence')),
    workflowFence: integer(data(row, 'workflow_fence')), checkpointSequence: integer(data(row, 'checkpoint_sequence')) };
}

// Keep the first and last failure plus the actual bounded source count. Any
// omitted intermediate failures are explicit; a later success cannot erase one.
function retainFailure(history, record) {
  if (history.length < 2) history.push(record);
  else history[1] = record;
}

function projectSlot(slot, children, supervisor) {
  const child = children?.find(item => data(item, 'slot') === slot);
  const records = array(data(child, 'records'), 100);
  const projected = { slot, childObserved: child !== undefined, recordsObserved: records?.length ?? null,
    lastPhase: 'unobserved', errorCount: 0, errorsTruncated: false, errors: [],
    controllerStatus: 'unobserved', controllerReason: 'unobserved', controllerCheckpointSequence: null,
    deadlineObserved: false, exitObserved: false, exitCode: null, signal: null,
    finishOutcome: 'unobserved', finishSettled: null, unsettledFinishCount: 0,
    unsettledFinishTruncated: false, unsettledFinishes: [],
    footerObserved: data(child, 'footer') === true, writerComplete: data(child, 'writerComplete') === true };
  const retainError = record => {
    projected.errorCount += 1;
    retainFailure(projected.errors, { stage: data(record, 'stage'),
      phase: choice(data(record, 'phase'), PHASES), code: choice(data(record, 'code'), CODES, 'OTHER'),
      sqlitePrimaryCode: integer(data(record, 'primaryCode'), 255) });
    projected.errorsTruncated = projected.errorCount > projected.errors.length;
  };
  for (const record of records ?? []) {
    const stage = data(record, 'stage');
    if (stage === 'runner_phase') projected.lastPhase = choice(data(record, 'phase'), PHASES);
    if (DIRECT_PHASES.has(stage)) projected.lastPhase = stage;
    if (stage === 'admission_committed' || stage === 'completion_committed' || stage === 'controller_result') {
      projected.lastPhase = 'controller';
    }
    if (CHILD_ERRORS.has(stage)) {
      if (PHASES.has(data(record, 'phase'))) projected.lastPhase = data(record, 'phase');
      retainError(record);
    }
    if (stage === 'controller_result') {
      projected.controllerStatus = choice(data(record, 'status'), new Set(['completed', 'stopped']));
      projected.controllerReason = reason(data(record, 'reason'), CONTROLLER_REASONS);
      projected.controllerCheckpointSequence = integer(data(record, 'checkpointSequence'));
    }
  }
  for (const record of supervisor ?? []) {
    if (data(record, 'slot') !== slot) continue;
    const stage = data(record, 'stage');
    if (SUPERVISOR_ERRORS.has(stage)) retainError(record);
    if (stage === 'deadline') projected.deadlineObserved = true;
    if (stage === 'child_exit') {
      projected.exitObserved = true;
      projected.exitCode = integer(data(record, 'exitCode'), 255);
      projected.signal = choice(data(record, 'signal'), SIGNALS, null);
    }
    if (stage === 'finish_result') {
      projected.finishOutcome = choice(data(record, 'outcome'), OUTCOMES);
      projected.finishSettled = typeof data(record, 'settled') === 'boolean' ? data(record, 'settled') : null;
      if (data(record, 'settled') === false) {
        projected.unsettledFinishCount += 1;
        retainFailure(projected.unsettledFinishes, { outcome: choice(data(record, 'outcome'), OUTCOMES),
          code: reason(data(record, 'code'), CODES) });
        projected.unsettledFinishTruncated = projected.unsettledFinishCount > projected.unsettledFinishes.length;
      }
    }
  }
  return projected;
}

// C04 deliberately kills its first runner and has one logical job. This record
// never claims the separate two-job OPS07.C01 capture's completeness oracle.
export function encodeC04CompletionDiagnostic(input) {
  const row = data(input, 'row');
  const rowStatus = choice(data(row, 'status'), new Set(['observed', 'missing', 'error', 'not_observed']), 'not_observed');
  const source = data(input, 'capture');
  const flags = data(source, 'flags');
  const children = array(data(source, 'slots'), 2);
  const supervisor = array(data(source, 'supervisor'), 100);
  const value = { schema: SCHEMA, diagnosticOnly: true,
    phase: choice(data(input, 'phase'), TEST_PHASES), primaryFailed: data(input, 'primaryFailed') === true,
    cleanupComplete: data(input, 'cleanupComplete') === true,
    preCleanup: canonicalRecord(data(input, 'preCleanup'), PRE_CLEANUP) ?? observeC04PreCleanup(null),
    row: { source: 'separate_point_lookup', timing: rowStatus === 'not_observed' ? 'not_observed' : 'before_cleanup',
      status: rowStatus,
      errorCode: data(row, 'errorCode') === 'none' ? 'none' : choice(data(row, 'errorCode'), CODES, 'OTHER'),
      workState: choice(data(row, 'workState'), WORK), workflowState: choice(data(row, 'workflowState'), WORKFLOW),
      reason: data(row, 'reason') === 'none' ? 'none' : choice(data(row, 'reason'), REASONS, 'OTHER'),
      generation: integer(data(row, 'generation')), operationFence: integer(data(row, 'operationFence')),
      workflowFence: integer(data(row, 'workflowFence')), checkpointSequence: integer(data(row, 'checkpointSequence')) },
    capture: { timing: data(input, 'captureTiming') === 'after_owned_child_reap' ? 'after_owned_child_reap' : 'not_observed',
      available: data(source, 'schema') === 'agent-harness.ops07-session.v1' && children !== null && supervisor !== null,
      missingChildCapture: data(flags, 'missingChildCapture') !== false,
      malformed: data(flags, 'malformed') !== false, overflow: data(flags, 'overflow') !== false,
      writeFailed: data(flags, 'writeFailed') !== false, abruptExit: data(flags, 'abruptExit') !== false,
      sessionCleanupIncomplete: data(flags, 'cleanupIncomplete') !== false,
      slots: [0, 1].map(slot => projectSlot(slot, children, supervisor)) } };
  const line = `${PREFIX}${JSON.stringify(value)}`;
  return Buffer.byteLength(line, 'utf8') <= MAX_BYTES ? line : null;
}

// A separate strict shape boundary is required when ingesting full TAP. The
// forgiving producer projection must never turn a hostile frame into valid data.
const INVALID = Symbol('invalid');
const boolean = value => typeof value === 'boolean' ? value : INVALID;
const oneOf = values => value => values.includes(value) ? value : INVALID;
const member = (values, extras = []) => oneOf([...values, ...extras]);
const number = (max = Number.MAX_SAFE_INTEGER) => value => value === null || integer(value, max) === value ? value : INVALID;
const nested = fields => value => canonicalRecord(value, fields) ?? INVALID;
const sequence = (fields, max, exact = null) => value => {
  const items = array(value, max);
  if (items === null || (exact !== null && items.length !== exact)) return INVALID;
  const result = items.map(item => canonicalRecord(item, fields));
  return result.some(item => item === null) ? INVALID : result;
};
function canonicalRecord(value, fields) {
  try {
    if (value === null || typeof value !== 'object' || utilTypes.isProxy(value) || Array.isArray(value)) return null;
    const proto = Object.getPrototypeOf(value);
    const keys = Reflect.ownKeys(value);
    if ((proto !== Object.prototype && proto !== null) || keys.length !== Object.keys(fields).length
        || keys.some(key => typeof key !== 'string' || !Object.hasOwn(fields, key))) return null;
    const result = {};
    for (const [key, validate] of Object.entries(fields)) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) return null;
      const clean = validate(descriptor.value);
      if (clean === INVALID) return null;
      result[key] = clean;
    }
    return result;
  } catch { return null; }
}
const PRE_CLEANUP = {
  timing: oneOf(['before_cleanup', 'not_observed']), pageSource: oneOf(['matching_last_read_work_page']),
  workerSource: oneOf(['in_memory_before_cleanup']),
  page: nested({ status: oneOf(['observed', 'missing', 'not_observed']), workState: member(WORK, ['unobserved']),
    generation: number(), reason: member(REASONS, ['none', 'OTHER']) }),
  worker: nested({ role: oneOf(['initial', 'restart', 'unobserved']), lifecycle: member(LIFECYCLES, ['unobserved']), activeChildCount: number() })
};
const ROW = { source: oneOf(['separate_point_lookup']), timing: oneOf(['before_cleanup', 'not_observed']),
  status: oneOf(['observed', 'missing', 'error', 'not_observed']), errorCode: member(CODES, ['none', 'OTHER']),
  workState: member(WORK, ['unobserved']), workflowState: member(WORKFLOW, ['unobserved']),
  reason: member(REASONS, ['none', 'OTHER']), generation: number(), operationFence: number(), workflowFence: number(), checkpointSequence: number() };
const ERROR = { stage: member(ERROR_STAGES), phase: member(PHASES, ['unobserved']), code: member(CODES, ['OTHER']), sqlitePrimaryCode: number(255) };
const FINISH = { outcome: member(OUTCOMES, ['unobserved']), code: member(CODES, ['none', 'OTHER']) };
const SLOT = { slot: oneOf([0, 1]), childObserved: boolean, recordsObserved: number(100), lastPhase: member(PHASES, ['unobserved']),
  errorCount: value => Number.isSafeInteger(value) && value >= 0 && value <= 200 ? value : INVALID,
  errorsTruncated: boolean, errors: sequence(ERROR, 2), controllerStatus: oneOf(['unobserved', 'completed', 'stopped']),
  controllerReason: member(CONTROLLER_REASONS, ['unobserved', 'none', 'OTHER']), controllerCheckpointSequence: number(),
  deadlineObserved: boolean, exitObserved: boolean, exitCode: number(255), signal: member(SIGNALS, [null]),
  finishOutcome: member(OUTCOMES, ['unobserved']), finishSettled: oneOf([null, true, false]),
  unsettledFinishCount: value => Number.isSafeInteger(value) && value >= 0 && value <= 100 ? value : INVALID,
  unsettledFinishTruncated: boolean, unsettledFinishes: sequence(FINISH, 2), footerObserved: boolean, writerComplete: boolean };
const DIAGNOSTIC = { schema: oneOf([SCHEMA]), diagnosticOnly: oneOf([true]), phase: member(TEST_PHASES, ['unobserved']),
  primaryFailed: boolean, cleanupComplete: boolean, preCleanup: nested(PRE_CLEANUP), row: nested(ROW),
  capture: nested({ timing: oneOf(['after_owned_child_reap', 'not_observed']), available: boolean,
    missingChildCapture: boolean, malformed: boolean, overflow: boolean, writeFailed: boolean, abruptExit: boolean,
    sessionCleanupIncomplete: boolean, slots: sequence(SLOT, 2, 2) }) };

export function parseC04CompletionFromTap(text) {
  const invalid = reason => ({ valid: false, reason, diagnostic: null, line: null });
  if (typeof text !== 'string') return invalid('invalid_input');
  if (Buffer.byteLength(text, 'utf8') > MAX_TAP_BYTES) return invalid('oversize');
  // Count the entire reserved family: an unknown version or malformed sibling
  // cannot disappear beside one otherwise valid V1 diagnostic.
  const candidates = text.split(/\r?\n/u).filter(line => /^\s*#\s*OPS07_C04_COMPLETION/u.test(line));
  if (candidates.length === 0) return invalid('missing');
  if (candidates.length !== 1) return invalid('duplicate');
  if (!text.endsWith('\n')) return invalid('truncated');
  if (!/^ {0,16}# OPS07_C04_COMPLETION_V1 /u.test(candidates[0])) return invalid('invalid_shape');
  const line = candidates[0].replace(/^ {0,16}# /u, '');
  if (Buffer.byteLength(line, 'utf8') > MAX_BYTES) return invalid('oversize');
  try {
    const diagnostic = canonicalRecord(JSON.parse(line.slice(PREFIX.length)), DIAGNOSTIC);
    if (diagnostic === null || diagnostic.capture.slots.some((slot, index) => slot.slot !== index
        || slot.errors.length !== Math.min(slot.errorCount, 2) || slot.errorsTruncated !== (slot.errorCount > 2)
        || slot.unsettledFinishes.length !== Math.min(slot.unsettledFinishCount, 2)
        || slot.unsettledFinishTruncated !== (slot.unsettledFinishCount > 2))) return invalid('invalid_shape');
    if (`${PREFIX}${JSON.stringify(diagnostic)}` !== line) return invalid('noncanonical');
    return { valid: true, reason: null, diagnostic, line };
  } catch { return invalid('malformed'); }
}

// Only caller-owned resources are touched. Capture is read after confirmed reap;
// failures remain separate from the test's original thrown value, even if falsey.
export async function cleanupC04Fixture({ workers, handles, readCapture, disposeCapture, removeDirectory, errors = [] }) {
  const cleanupErrors = [...errors];
  let cleanupComplete = cleanupErrors.length === 0;
  let childrenReaped = true;
  for (const owned of workers) {
    if (!owned) continue;
    try {
      if (owned.lifecycle === 'stopped') continue;
      const result = await owned.stop('test');
      if (result.childrenReaped !== true) childrenReaped = false;
      if (result.stopped !== true || result.childrenReaped !== true || result.storeClosed !== true) {
        cleanupComplete = false; cleanupErrors.push(new Error('C04 owned worker cleanup was not confirmed.'));
      }
    } catch (error) { childrenReaped = false; cleanupComplete = false; cleanupErrors.push(error); }
  }
  let capture = null; let captureTiming = 'not_observed';
  if (childrenReaped && readCapture) {
    try { capture = readCapture(); captureTiming = 'after_owned_child_reap'; }
    catch (error) { cleanupComplete = false; cleanupErrors.push(error); }
  }
  try {
    if (disposeCapture && disposeCapture() !== true) throw new Error('C04 diagnostic descriptor cleanup was not confirmed.');
  } catch (error) { cleanupComplete = false; cleanupErrors.push(error); }
  for (const handle of handles) {
    if (!handle) continue;
    try { handle.close(); }
    catch (error) { cleanupComplete = false; cleanupErrors.push(error); }
  }
  if (cleanupComplete) {
    try { removeDirectory(); }
    catch (error) { cleanupComplete = false; cleanupErrors.push(error); }
  }
  return { cleanupComplete, capture, captureTiming, cleanupErrors };
}

export function throwC04CleanupErrors({ primaryFailed, primaryError, cleanupErrors }) {
  if (cleanupErrors.length) throw new AggregateError(primaryFailed ? [primaryError, ...cleanupErrors] : cleanupErrors,
    'C04 final diagnostic or native cleanup failed.', primaryFailed ? { cause: primaryError } : undefined);
}
