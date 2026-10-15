const FAILURE_SCHEMA = 'agent-harness.ops04-child-failure.v1';
const FAILURE_FIELDS = Object.freeze({
  SHUTDOWN_STOP_INVARIANT: Object.freeze([
    'workerStopped', 'ownedChildrenReaped', 'storeClosed', 'sinkCalledOnce'
  ]),
  TELEMETRY_BACKLOG_INVARIANT: Object.freeze(['cursorIsZero', 'pendingRecordsIsPositive']),
  POSTSTOP_READ: Object.freeze(['errorKind']),
  QUEUE_ACCOUNTING_INVARIANT: Object.freeze(['workIsQueued', 'inFlightIsZero', 'attemptsUnchanged']),
  OTHER_BOUNDARY: Object.freeze([])
});
const SAFE_READ_ERROR_KINDS = new Set(['SQLITE_BUSY', 'SQLITE_ERROR', 'SQLITE_IOERR', 'SQLITE_CORRUPT', 'OTHER']);
const EXIT_DISPOSITIONS = new Set(['exit_zero', 'exit_nonzero', 'signal', 'unknown']);
const SAFE_SIGNALS = new Set([
  'SIGABRT', 'SIGALRM', 'SIGBUS', 'SIGCHLD', 'SIGCONT', 'SIGFPE', 'SIGHUP', 'SIGILL', 'SIGINFO',
  'SIGINT', 'SIGIO', 'SIGIOT', 'SIGKILL', 'SIGPIPE', 'SIGPROF', 'SIGQUIT', 'SIGSEGV', 'SIGSTOP',
  'SIGSYS', 'SIGTERM', 'SIGTRAP', 'SIGTSTP', 'SIGTTIN', 'SIGTTOU', 'SIGURG', 'SIGUSR1', 'SIGUSR2',
  'SIGVTALRM', 'SIGWINCH', 'SIGXCPU', 'SIGXFSZ'
]);
const PHASES = new Set([
  'before-marker', 'after-marker-before-bytes', 'after-write',
  'response-before-observation', 'observation-before-checkpoint'
]);
const CHILD_STAGES = new Set([
  'fixture_started', 'store_opened', 'workflow_opened', 'worker_started', 'runner_claimed', 'marker_written',
  'signal_seen', 'stop_requested', 'stop_settled', 'loop_settled', 'shutdown_verified',
  'restart_worker_started', 'restart_work_completed', 'restart_stop_settled',
  'restart_loop_settled', 'restart_store_closed', 'store_close_started', 'store_closed',
  'runner_escalation_sent', 'fixture_failure', 'fixture_finally_started', 'fixture_finally_complete'
]);
const PARENT_STAGES = new Set([
  'shutdown_spawned', 'shutdown_marker_observed', 'sigterm_sent', 'shutdown_child_closed',
  'restart_spawned', 'restart_escalation_sent', 'restart_child_closed', 'shutdown_escalation_sent',
  'cleanup_escalation_sent', 'child_close_unconfirmed', 'owned_group_empty', 'owned_group_close_unconfirmed',
  'child_capture_missing', 'child_capture_invalid', 'child_capture_partial', 'child_capture_overflow'
]);

function plainRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    && Object.getPrototypeOf(value) === Object.prototype;
}

function exactKeys(value, expected) {
  return plainRecord(value) && Object.keys(value).sort().join(',') === [...expected].sort().join(',');
}

export function ops04FailureFields(boundary) {
  return Object.hasOwn(FAILURE_FIELDS, boundary) ? [...FAILURE_FIELDS[boundary]] : null;
}

export function createOps04FailureDetails(boundary, observations = {}) {
  const fields = ops04FailureFields(boundary);
  if (!fields || !exactKeys(observations, fields)) return null;
  if (boundary === 'POSTSTOP_READ') {
    if (!SAFE_READ_ERROR_KINDS.has(observations.errorKind)) return null;
  } else if (!fields.every(field => typeof observations[field] === 'boolean')) {
    return null;
  }
  return { diagnosticSchema: FAILURE_SCHEMA, failureBoundary: boundary, ...observations };
}

export function parseOps04FailureDetails(value) {
  if (!plainRecord(value) || typeof value.failureBoundary !== 'string') return null;
  const fields = ops04FailureFields(value.failureBoundary);
  if (!fields || !exactKeys(value, ['diagnosticSchema', 'failureBoundary', ...fields])
      || value.diagnosticSchema !== FAILURE_SCHEMA) return null;
  if (value.failureBoundary === 'POSTSTOP_READ') {
    if (!SAFE_READ_ERROR_KINDS.has(value.errorKind)) return null;
  } else if (!fields.every(field => typeof value[field] === 'boolean')) {
    return null;
  }
  return { diagnosticSchema: FAILURE_SCHEMA, failureBoundary: value.failureBoundary,
    ...Object.fromEntries(fields.map(field => [field, value[field]])) };
}

export function ops04ReadErrorKind(error) {
  const code = Number.isSafeInteger(error?.errcode) ? error.errcode & 0xff : null;
  if (code === 5 || code === 6) return 'SQLITE_BUSY';
  if (code === 10) return 'SQLITE_IOERR';
  if (code === 11) return 'SQLITE_CORRUPT';
  if (error?.code === 'ERR_SQLITE_ERROR') return 'SQLITE_ERROR';
  return 'OTHER';
}

export function classifyOps04ExitDisposition(closeResult) {
  if (!exactKeys(closeResult, ['code', 'signal'])) return null;
  if (Number.isSafeInteger(closeResult.code) && closeResult.code >= 0 && closeResult.code <= 255
      && closeResult.signal === null) {
    return closeResult.code === 0 ? 'exit_zero' : 'exit_nonzero';
  }
  if (closeResult.code === null && closeResult.signal === null) return 'unknown';
  if (closeResult.code === null && typeof closeResult.signal === 'string'
      && SAFE_SIGNALS.has(closeResult.signal)) return 'signal';
  return null;
}

export function isOps04ExitDisposition(value) {
  return EXIT_DISPOSITIONS.has(value);
}

export function throwAfterOps04FailureCapture(primaryError, capture) {
  try { capture?.(); } catch {}
  throw primaryError;
}

function validatePublicPhaseRecord(record) {
  if (!plainRecord(record) || record.schema !== 1 || !PHASES.has(record.phase)
      || !['child', 'parent'].includes(record.source) || !['shutdown', 'restart'].includes(record.role)
      || !Number.isSafeInteger(record.sequence) || record.sequence < 1
      || !Number.isSafeInteger(record.elapsedMs) || record.elapsedMs < 0) return false;
  const base = ['elapsedMs', 'phase', 'role', 'schema', 'sequence', 'source', 'stage'];
  if (record.source === 'child') {
    if (!CHILD_STAGES.has(record.stage)) return false;
    if (record.stage === 'fixture_failure') {
      const details = Object.fromEntries(Object.keys(record)
        .filter(key => !base.includes(key)).map(key => [key, record[key]]));
      return parseOps04FailureDetails(details) !== null;
    }
    const hasStopResult = record.stage === 'stop_settled' || record.stage === 'restart_stop_settled';
    const stopFields = ['childrenReaped', 'stopped', 'storeClosed'];
    return exactKeys(record, hasStopResult ? [...base, ...stopFields] : base)
      && (!hasStopResult || stopFields.every(key => typeof record[key] === 'boolean'));
  }
  if (!PARENT_STAGES.has(record.stage)) return false;
  const closeStage = record.stage === 'shutdown_child_closed' || record.stage === 'restart_child_closed';
  if (closeStage && ((record.stage === 'shutdown_child_closed') !== (record.role === 'shutdown'))) return false;
  return exactKeys(record, closeStage ? [...base, 'exitDisposition'] : base)
    && (!closeStage || isOps04ExitDisposition(record.exitDisposition));
}

export function validateOps04PublicPhaseMetrics(metrics) {
  if (!plainRecord(metrics) || !Array.isArray(metrics.ci1PhaseCaptures)
      || !Number.isSafeInteger(metrics.ci1PhaseCaptureBytes) || metrics.ci1PhaseCaptureBytes < 0
      || metrics.ci1PhaseCaptures.length > 256 || metrics.ci1PhaseCaptureBytes > 24_576
      || !['ci1PhaseCaptureOverflow', 'ci1PhaseCaptureIncomplete', 'ci1PhaseCaptureChildCloseUnconfirmed',
        'ci1PhaseCaptureOwnedGroupCloseUnconfirmed'].every(key => typeof metrics[key] === 'boolean')) {
    return { valid: false, failureCount: 0 };
  }
  let bytes = 0;
  let failureCount = 0;
  let incompleteEvidenceRequired = metrics.ci1PhaseCaptureOverflow
    || metrics.ci1PhaseCaptureChildCloseUnconfirmed
    || metrics.ci1PhaseCaptureOwnedGroupCloseUnconfirmed;
  const groups = new Map();
  const failuresByInvocation = new Set();
  for (const record of metrics.ci1PhaseCaptures) {
    if (!validatePublicPhaseRecord(record)) return { valid: false, failureCount };
    bytes += Buffer.byteLength(JSON.stringify(record));
    const key = `${record.source}:${record.phase}:${record.role}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push({ sequence: record.sequence, elapsedMs: record.elapsedMs });
    if (record.stage === 'fixture_failure') {
      failureCount += 1;
      incompleteEvidenceRequired = true;
      const invocation = `${record.phase}:${record.role}`;
      if (failuresByInvocation.has(invocation)) return { valid: false, failureCount };
      failuresByInvocation.add(invocation);
    }
    if (record.source === 'child'
        && ['stop_settled', 'restart_stop_settled'].includes(record.stage)
        && (!record.stopped || !record.childrenReaped || !record.storeClosed)) {
      incompleteEvidenceRequired = true;
    }
    if (record.source === 'parent'
        && ['shutdown_child_closed', 'restart_child_closed'].includes(record.stage)
        && record.exitDisposition !== 'exit_zero') {
      incompleteEvidenceRequired = true;
    }
    if (record.source === 'parent' && record.stage.startsWith('child_capture_')) {
      incompleteEvidenceRequired = true;
    }
  }
  if (bytes !== metrics.ci1PhaseCaptureBytes
      || (incompleteEvidenceRequired && metrics.ci1PhaseCaptureIncomplete !== true)) {
    return { valid: false, failureCount };
  }
  for (const entries of groups.values()) {
    entries.sort((left, right) => left.sequence - right.sequence);
    if (entries.some((entry, index) => entry.sequence !== index + 1
        || (index > 0 && entry.elapsedMs < entries[index - 1].elapsedMs))) {
      return { valid: false, failureCount };
    }
  }
  return { valid: true, failureCount };
}
