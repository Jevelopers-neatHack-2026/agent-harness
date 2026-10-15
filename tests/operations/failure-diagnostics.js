import { fileURLToPath } from 'node:url';
import { relative, resolve, sep } from 'node:path';
import { dirname } from 'node:path';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const MAX_RECORDS = 32;
const MAX_NESTED_ERROR_ROOTS = 32;
const MAX_INPUT_BYTES = 1_000_000;
const SAFE_ERROR_NAMES = new Set([
  'AssertionError', 'Error', 'RangeError', 'RuntimeError', 'SyntaxError', 'TimeoutError', 'TypeError'
]);
const C02_CONTROL_FAILURE_PREFIX = 'AH11_OPS_C02_CONTROL_FAILURE ';
const C02_CONTROL_FAILURE_MAX_RECORDS = 8;
const C02_CONTROL_FAILURE_MAX_LINE_BYTES = 192;
const C02_CONTROL_FAILURE_MAX_BYTES = 1_536;
const C02_CONTROL_FAILURE_BOUNDARIES = new Set([
  'C02_RUNNER_READY', 'C02_MARKER_CONTROL_READY', 'C02_WATCHDOG_READY',
  'C02_WATCHDOG_TARGET_ACK', 'C02_INERT_DESCENDANT_MARKER', 'C02_WATCHDOG_RESULT'
]);
const C02_CONTROL_FAILURE_ERROR_CLASSES = new Set(['Error', 'RangeError', 'SyntaxError', 'TypeError']);
const INCOMPLETE_C02_CONTROL_DIAGNOSTICS = Object.freeze({
  complete: false, recordCount: 0, records: Object.freeze([])
});
const OPS02_RACE_FAILURE_PREFIX = 'AH11_OPS_OPS02_C02_DIAGNOSTIC_V3 ';
const OPS02_RACE_FAILURE_MAX_RECORDS = 64;
const OPS02_RACE_FAILURE_MAX_LINE_BYTES = 320;
const OPS02_RACE_FAILURE_MAX_BYTES = 16_384;
const OPS02_RACE_FAILURE_BOUNDARIES = new Set([
  'case_started', 'diagnostic_no_canary', 'diagnostic_proxy_unread',
  'diagnostic_child_exit', 'diagnostic_child_prefix', 'diagnostic_child_no_canary',
  'seed_accept', 'contender_exit', 'child_exit_group', 'accepted_count',
  'refusal_count', 'stored_counter', 'stored_rows', 'reservation'
]);
const OPS02_RACE_FAILURE_KINDS = new Set([
  'diagnostic', 'global_records', 'project_bytes_existing', 'project_bytes_new',
  'project_records', 'global_bytes'
]);
const OPS02_RACE_FAILURE_EXITS = new Set(['not_applicable', 'zero', 'nonzero', 'signal', 'unknown']);
const OPS02_RACE_FAILURE_STAGES = new Set([
  'config', 'open', 'resource', 'validation', 'admission', 'result', 'close', 'unknown', 'none', 'unobserved'
]);
const OPS02_RACE_FAILURE_CODES = new Set([
  'OPS_CONFIG_INVALID', 'STORE_OPEN_FAILED', 'STORE_WRITE_FAILED', 'STORE_BUSY', 'STORE_CLOSED',
  'UNSUPPORTED_SCHEMA_VERSION', 'UNVERSIONED_DATA', 'INVALID_INPUT', 'ERR_SQLITE_ERROR',
  'ERR_INVALID_ARG_TYPE', 'EACCES', 'ENOENT', 'ENOSPC', 'UNKNOWN', 'none', 'unobserved'
]);
const OPS02_TRANSACTION_BOUNDARIES = new Set(['begin', 'body', 'commit', 'rollback', 'none', 'unobserved']);
const OPS02_SQLITE_PRIMARY_CODES = new Set([null, 5, 6]);
const OPS02_ROLLBACK_DISPOSITIONS = new Set([
  'not_observed', 'inactive', 'unknown', 'rolled_back', 'rollback_failed', 'rollback_check_failed'
]);
const OPS02_RACE_WIRE_VALUES = Object.freeze({
  boundaries: Object.freeze([
    'case_started', 'diagnostic_no_canary', 'diagnostic_proxy_unread',
    'diagnostic_child_exit', 'diagnostic_child_prefix', 'diagnostic_child_no_canary',
    'seed_accept', 'contender_exit', 'child_exit_group', 'accepted_count',
    'refusal_count', 'stored_counter', 'stored_rows', 'reservation'
  ]),
  raceKinds: Object.freeze([
    'diagnostic', 'global_records', 'project_bytes_existing', 'project_bytes_new',
    'project_records', 'global_bytes'
  ]),
  exits: Object.freeze(['not_applicable', 'zero', 'nonzero', 'signal', 'unknown']),
  stages: Object.freeze([
    'config', 'open', 'resource', 'validation', 'admission', 'result', 'close',
    'unknown', 'none', 'unobserved'
  ]),
  codes: Object.freeze([
    'OPS_CONFIG_INVALID', 'STORE_OPEN_FAILED', 'STORE_WRITE_FAILED', 'STORE_BUSY', 'STORE_CLOSED',
    'UNSUPPORTED_SCHEMA_VERSION', 'UNVERSIONED_DATA', 'INVALID_INPUT', 'ERR_SQLITE_ERROR',
    'ERR_INVALID_ARG_TYPE', 'EACCES', 'ENOENT', 'ENOSPC', 'UNKNOWN', 'none', 'unobserved'
  ]),
  transactionBoundaries: Object.freeze(['begin', 'body', 'commit', 'rollback', 'none', 'unobserved']),
  rollbackDispositions: Object.freeze([
    'not_observed', 'inactive', 'unknown', 'rolled_back', 'rollback_failed', 'rollback_check_failed'
  ])
});
const INCOMPLETE_OPS02_RACE_DIAGNOSTICS = Object.freeze({
  complete: false, recordCount: 0, records: Object.freeze([])
});
const OPS06_CLOSE_PHASE_FAILURE_PREFIX = 'AH11_OPS_OPS06_CLOSE_PHASES ';
const OPS06_CLOSE_PHASE_FAILURE_MAX_BYTES = 512;
const OPS06_CLOSE_PHASE_KEYS = Object.freeze([
  'caseId', 'exitCode', 'exitDisposition', 'exitSignal', 'failureClass', 'finalStopElapsedMs',
  'processCloseElapsedMs', 'readyToFinalRecordElapsedMs', 'restartOwnership',
  'restartProcessCloseElapsedMs', 'restartWorkElapsedMs', 'schema',
  'signalToFirstStopElapsedMs', 'stage'
].sort());
const INCOMPLETE_OPS06_CLOSE_PHASE_DIAGNOSTICS = Object.freeze({
  complete: false, recordCount: 0, records: Object.freeze([])
});
const OPS12_RECOVERY_FAILURE_PREFIX = 'AH11_OPS_OPS12_RECOVERY_FAILURE ';
const OPS12_RECOVERY_DIAGNOSTIC_PREFIX = 'AH11_OPS_OPS12_RECOVERY_DIAGNOSTIC ';
const OPS12_RECOVERY_MAX_BYTES = 1_024;
const OPS12_RECOVERY_MAX_LINE_BYTES = 512;
const OPS12_RECOVERY_CASES = new Set([
  'busy_store_spans_stop', 'store_unavailable', 'future_schema',
  'stuck_runner_review_escalation', 'unknown_hold_restart_escalation', 'unknown'
]);
const OPS12_RECOVERY_STAGES = new Set([
  'fixture_start', 'case_setup', 'busy_write', 'worker_stop', 'reopen_validate',
  'compatibility_read', 'future_schema_write', 'worker_open', 'byte_compare',
  'runner_pause', 'await_park', 'restart', 'hold_setup', 'intent_write',
  'lease_expiry', 'hold_verify', 'case_complete', 'unknown'
]);
const OPS12_RECOVERY_ERROR_CLASSES = new Set([
  'assertion', 'sqlite_busy', 'sqlite_other', 'type', 'range', 'error', 'unknown', 'none'
]);
const OPS12_RECOVERY_EXIT_DISPOSITIONS = new Set(['zero', 'nonzero', 'signal', 'unknown']);
const OPS12_RECOVERY_SIGNALS = new Set(['SIGABRT', 'SIGINT', 'SIGKILL', 'SIGTERM']);
const INCOMPLETE_OPS12_RECOVERY_DIAGNOSTICS = Object.freeze({
  complete: false, recordCount: 0, records: Object.freeze([])
});
const SAFE_ERROR_CODES = new Set([
  'EACCES', 'EISDIR', 'ENOENT', 'ETIMEDOUT', 'ERR_ASSERTION',
  'ERR_CHILD_PROCESS_STDIO_MAXBUFFER', 'ERR_SQLITE_ERROR', 'ERR_TEST_FAILURE',
  'SQLITE_BUSY', 'SQLITE_CONSTRAINT', 'SQLITE_ERROR', 'SQLITE_IOERR', 'SQLITE_LOCKED',
  'STORE_BUSY'
]);
const SAFE_SIGNALS = new Set(['SIGABRT', 'SIGINT', 'SIGKILL', 'SIGTERM']);
const SAFE_OPERATORS = new Set([
  'deepEqual', 'deepStrictEqual', 'doesNotMatch', 'doesNotThrow', 'equal', 'fail',
  'ifError', 'match', 'notDeepEqual', 'notDeepStrictEqual', 'notEqual', 'notStrictEqual',
  'ok', 'rejects', 'strictEqual', 'throws'
]);

function safeErrorName(value) {
  return SAFE_ERROR_NAMES.has(value) ? value : null;
}

function safeErrorCode(value) {
  return SAFE_ERROR_CODES.has(value) ? value : null;
}

function safeSignal(value) {
  return SAFE_SIGNALS.has(value) ? value : null;
}

export function parseC02ControlReadDiagnostics(channel) {
  if (!channel || typeof channel !== 'object' || Array.isArray(channel)) return INCOMPLETE_C02_CONTROL_DIAGNOSTICS;
  const expectedKeys = ['bytes', 'count', 'diagnosticsComplete', 'lines'];
  const keys = Object.keys(channel).sort();
  if (keys.length !== expectedKeys.length || keys.some((key, index) => key !== expectedKeys[index])
      || expectedKeys.some(key => !Object.hasOwn(channel, key))) return INCOMPLETE_C02_CONTROL_DIAGNOSTICS;
  const { lines, count, bytes, diagnosticsComplete } = channel;
  if (typeof lines !== 'string' || !Number.isSafeInteger(count) || count < 0
      || count > C02_CONTROL_FAILURE_MAX_RECORDS || !Number.isSafeInteger(bytes) || bytes < 0
      || bytes > C02_CONTROL_FAILURE_MAX_BYTES || typeof diagnosticsComplete !== 'boolean'
      || diagnosticsComplete !== true || lines.length > C02_CONTROL_FAILURE_MAX_BYTES) {
    return INCOMPLETE_C02_CONTROL_DIAGNOSTICS;
  }
  const actualBytes = Buffer.byteLength(lines, 'utf8');
  if (actualBytes > C02_CONTROL_FAILURE_MAX_BYTES || actualBytes !== bytes
      || (count === 0 ? lines !== '' : !lines.endsWith('\n'))) {
    return INCOMPLETE_C02_CONTROL_DIAGNOSTICS;
  }
  const rawLines = count === 0 ? [] : lines.split('\n');
  if (count > 0) rawLines.pop();
  if (rawLines.length !== count || rawLines.length > C02_CONTROL_FAILURE_MAX_RECORDS) {
    return INCOMPLETE_C02_CONTROL_DIAGNOSTICS;
  }
  const records = [];
  for (const rawLine of rawLines) {
    if (Buffer.byteLength(rawLine, 'utf8') + 1 > C02_CONTROL_FAILURE_MAX_LINE_BYTES
        || !rawLine.startsWith(C02_CONTROL_FAILURE_PREFIX)) return INCOMPLETE_C02_CONTROL_DIAGNOSTICS;
    let value;
    try { value = JSON.parse(rawLine.slice(C02_CONTROL_FAILURE_PREFIX.length)); } catch {
      return INCOMPLETE_C02_CONTROL_DIAGNOSTICS;
    }
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      return INCOMPLETE_C02_CONTROL_DIAGNOSTICS;
    }
    const keys = Object.keys(value).sort();
    if (keys.length !== 4 || keys[0] !== 'boundary' || keys[1] !== 'caseId'
        || keys[2] !== 'errorClass' || keys[3] !== 'schema'
        || value.schema !== 'agent-harness.ah11-ops-c02-control-failure.v1'
        || value.caseId !== 'OPS04.C02'
        || !C02_CONTROL_FAILURE_BOUNDARIES.has(value.boundary)
        || !C02_CONTROL_FAILURE_ERROR_CLASSES.has(value.errorClass)) return INCOMPLETE_C02_CONTROL_DIAGNOSTICS;
    records.push(Object.freeze({
      caseId: 'OPS04.C02', boundary: value.boundary, errorClass: value.errorClass
    }));
  }
  return Object.freeze({
    complete: true,
    recordCount: records.length,
    records: Object.freeze(records)
  });
}

function safeC02ControlReadDiagnostics(value) {
  let complete = value?.complete === true
    && Number.isSafeInteger(value?.recordCount)
    && value.recordCount >= 0 && value.recordCount <= C02_CONTROL_FAILURE_MAX_RECORDS
    && Array.isArray(value?.records) && value.records.length === value.recordCount;
  const records = [];
  for (const record of (Array.isArray(value?.records) ? value.records : []).slice(0, C02_CONTROL_FAILURE_MAX_RECORDS)) {
    const keys = record && typeof record === 'object' && !Array.isArray(record)
      ? Object.keys(record).sort() : [];
    if (keys.length !== 3 || keys[0] !== 'boundary' || keys[1] !== 'caseId' || keys[2] !== 'errorClass'
        || record.caseId !== 'OPS04.C02'
        || !C02_CONTROL_FAILURE_BOUNDARIES.has(record.boundary)
        || !C02_CONTROL_FAILURE_ERROR_CLASSES.has(record.errorClass)) {
      complete = false;
      break;
    }
    records.push(Object.freeze({
      caseId: 'OPS04.C02', boundary: record.boundary, errorClass: record.errorClass
    }));
  }
  return Object.freeze({
    complete,
    recordCount: complete ? records.length : 0,
    records: Object.freeze(complete ? records : [])
  });
}

function safeOps02RaceRecord(record) {
  const keys = record && typeof record === 'object' && !Array.isArray(record)
    ? Object.keys(record).sort() : [];
  if (keys.length !== 13 || keys[0] !== 'boundary' || keys[1] !== 'caseId'
      || keys[2] !== 'childCode' || keys[3] !== 'childStage' || keys[4] !== 'contender'
      || keys[5] !== 'exit' || keys[6] !== 'passed' || keys[7] !== 'raceKind' || keys[8] !== 'rollbackDisposition'
      || keys[9] !== 'schema' || keys[10] !== 'sqlitePrimaryCode' || keys[11] !== 'transactionActive'
      || keys[12] !== 'transactionBoundary'
      || record.schema !== 'agent-harness.ah11-ops02-race-diagnostic.v3'
      || record.caseId !== 'OPS02.C02'
      || !OPS02_RACE_FAILURE_BOUNDARIES.has(record.boundary)
      || !OPS02_RACE_FAILURE_KINDS.has(record.raceKind)
      || !(record.contender === null || record.contender === 0 || record.contender === 1)
      || typeof record.passed !== 'boolean'
      || !OPS02_RACE_FAILURE_EXITS.has(record.exit)
      || !OPS02_RACE_FAILURE_STAGES.has(record.childStage)
      || !OPS02_RACE_FAILURE_CODES.has(record.childCode)
      || !OPS02_TRANSACTION_BOUNDARIES.has(record.transactionBoundary)
      || !OPS02_SQLITE_PRIMARY_CODES.has(record.sqlitePrimaryCode)
      || !(record.transactionActive === null || typeof record.transactionActive === 'boolean')
      || !OPS02_ROLLBACK_DISPOSITIONS.has(record.rollbackDisposition)) return null;
  const childBoundary = record.boundary === 'contender_exit' || record.boundary === 'diagnostic_child_exit';
  if (!childBoundary && (record.exit !== 'not_applicable'
      || record.childStage !== 'none' || record.childCode !== 'none' || record.contender !== null
      || record.transactionBoundary !== 'none' || record.sqlitePrimaryCode !== null
      || record.transactionActive !== null || record.rollbackDisposition !== 'not_observed')) return null;
  if ((record.transactionBoundary === 'none' || record.transactionBoundary === 'unobserved')
      !== (record.sqlitePrimaryCode === null)) return null;
  if ((record.transactionBoundary === 'none' || record.transactionBoundary === 'unobserved')
      && (record.transactionActive !== null || record.rollbackDisposition !== 'not_observed')) return null;
  if (!['none', 'unobserved'].includes(record.transactionBoundary)
      && (record.childStage !== 'admission' || record.childCode !== 'STORE_BUSY'
        || ![5, 6].includes(record.sqlitePrimaryCode))) return null;
  if (record.transactionBoundary === 'begin'
      && (record.transactionActive !== null || record.rollbackDisposition !== 'not_observed')) return null;
  if (['body', 'commit'].includes(record.transactionBoundary)
      && !((record.rollbackDisposition === 'inactive' && record.transactionActive === false)
        || (record.rollbackDisposition === 'rolled_back' && record.transactionActive === true)
        || (record.rollbackDisposition === 'unknown' && record.transactionActive === null))) return null;
  if (record.transactionBoundary === 'rollback'
      && !((record.rollbackDisposition === 'rollback_failed' && record.transactionActive === true)
        || (record.rollbackDisposition === 'rollback_check_failed' && record.transactionActive === null))) return null;
  if (record.boundary === 'contender_exit'
      && (record.contender === null || record.raceKind === 'diagnostic'
        || !['zero', 'nonzero', 'signal', 'unknown'].includes(record.exit)
        || record.passed !== (record.exit === 'zero'))) return null;
  if (record.boundary === 'contender_exit' && record.exit === 'zero'
      && (record.childStage !== 'none' || record.childCode !== 'none'
        || !['none', 'unobserved'].includes(record.transactionBoundary))) return null;
  if (record.boundary === 'diagnostic_child_exit'
      && (record.raceKind !== 'diagnostic' || record.contender !== null
        || record.exit === 'not_applicable'
        || (record.passed && record.exit !== 'nonzero'))) return null;
  if (!['none', 'unobserved'].includes(record.transactionBoundary)
      && record.boundary !== 'contender_exit') return null;
  if (record.boundary === 'child_exit_group' && record.contender !== null) return null;
  return Object.freeze({
    schema: 'agent-harness.ah11-ops02-race-diagnostic.v3',
    caseId: 'OPS02.C02', boundary: record.boundary, raceKind: record.raceKind,
    contender: record.contender, passed: record.passed, exit: record.exit,
    childStage: record.childStage, childCode: record.childCode,
    transactionBoundary: record.transactionBoundary, sqlitePrimaryCode: record.sqlitePrimaryCode,
    transactionActive: record.transactionActive, rollbackDisposition: record.rollbackDisposition
  });
}

function ops02WireIndex(values, value) {
  return values.indexOf(value);
}

function ops02WireValue(values, code) {
  return Number.isSafeInteger(code) && code >= 0 && code < values.length ? values[code] : null;
}

function decodeOps02RaceWireRecord(value) {
  if (!Array.isArray(value) || value.length !== 11) return null;
  const [boundaryCode, raceCode, contender, passed, exitCode, stageCode, codeCode,
    transactionBoundaryCode, sqlitePrimaryCode, transactionActive, rollbackCode] = value;
  const boundary = ops02WireValue(OPS02_RACE_WIRE_VALUES.boundaries, boundaryCode);
  const raceKind = ops02WireValue(OPS02_RACE_WIRE_VALUES.raceKinds, raceCode);
  const exit = ops02WireValue(OPS02_RACE_WIRE_VALUES.exits, exitCode);
  const childStage = ops02WireValue(OPS02_RACE_WIRE_VALUES.stages, stageCode);
  const childCode = ops02WireValue(OPS02_RACE_WIRE_VALUES.codes, codeCode);
  const transactionBoundary = ops02WireValue(OPS02_RACE_WIRE_VALUES.transactionBoundaries, transactionBoundaryCode);
  const rollbackDisposition = ops02WireValue(OPS02_RACE_WIRE_VALUES.rollbackDispositions, rollbackCode);
  if ([boundary, raceKind, exit, childStage, childCode, transactionBoundary, rollbackDisposition].includes(null)) return null;
  return {
    schema: 'agent-harness.ah11-ops02-race-diagnostic.v3', caseId: 'OPS02.C02',
    boundary, raceKind, contender, passed, exit, childStage, childCode,
    transactionBoundary, sqlitePrimaryCode, transactionActive, rollbackDisposition
  };
}

export function encodeOps02RaceDiagnostic(record) {
  const safe = safeOps02RaceRecord(record);
  if (!safe) return null;
  const wire = [
    ops02WireIndex(OPS02_RACE_WIRE_VALUES.boundaries, safe.boundary),
    ops02WireIndex(OPS02_RACE_WIRE_VALUES.raceKinds, safe.raceKind),
    safe.contender, safe.passed,
    ops02WireIndex(OPS02_RACE_WIRE_VALUES.exits, safe.exit),
    ops02WireIndex(OPS02_RACE_WIRE_VALUES.stages, safe.childStage),
    ops02WireIndex(OPS02_RACE_WIRE_VALUES.codes, safe.childCode),
    ops02WireIndex(OPS02_RACE_WIRE_VALUES.transactionBoundaries, safe.transactionBoundary),
    safe.sqlitePrimaryCode, safe.transactionActive,
    ops02WireIndex(OPS02_RACE_WIRE_VALUES.rollbackDispositions, safe.rollbackDisposition)
  ];
  const line = `${OPS02_RACE_FAILURE_PREFIX}${JSON.stringify(wire)}`;
  return Buffer.byteLength(`${line}\n`, 'utf8') <= OPS02_RACE_FAILURE_MAX_LINE_BYTES ? line : null;
}

export function parseOps02RaceDiagnostics(channel) {
  if (!channel || typeof channel !== 'object' || Array.isArray(channel)) return INCOMPLETE_OPS02_RACE_DIAGNOSTICS;
  const expectedKeys = ['bytes', 'count', 'diagnosticsComplete', 'lines'];
  const keys = Object.keys(channel).sort();
  if (keys.length !== expectedKeys.length || keys.some((key, index) => key !== expectedKeys[index])
      || expectedKeys.some(key => !Object.hasOwn(channel, key))) return INCOMPLETE_OPS02_RACE_DIAGNOSTICS;
  const { lines, count, bytes, diagnosticsComplete } = channel;
  if (typeof lines !== 'string' || !Number.isSafeInteger(count) || count < 0
      || count > OPS02_RACE_FAILURE_MAX_RECORDS || !Number.isSafeInteger(bytes) || bytes < 0
      || bytes > OPS02_RACE_FAILURE_MAX_BYTES || diagnosticsComplete !== true
      || lines.length > OPS02_RACE_FAILURE_MAX_BYTES) return INCOMPLETE_OPS02_RACE_DIAGNOSTICS;
  const actualBytes = Buffer.byteLength(lines, 'utf8');
  if (actualBytes !== bytes || actualBytes > OPS02_RACE_FAILURE_MAX_BYTES
      || (count === 0 ? lines !== '' : !lines.endsWith('\n'))) return INCOMPLETE_OPS02_RACE_DIAGNOSTICS;
  const rawLines = count === 0 ? [] : lines.split('\n');
  if (count > 0) rawLines.pop();
  if (rawLines.length !== count || rawLines.length > OPS02_RACE_FAILURE_MAX_RECORDS) {
    return INCOMPLETE_OPS02_RACE_DIAGNOSTICS;
  }
  const records = [];
  for (const rawLine of rawLines) {
    if (Buffer.byteLength(rawLine, 'utf8') + 1 > OPS02_RACE_FAILURE_MAX_LINE_BYTES
        || !rawLine.startsWith(OPS02_RACE_FAILURE_PREFIX)) return INCOMPLETE_OPS02_RACE_DIAGNOSTICS;
    let value;
    try { value = JSON.parse(rawLine.slice(OPS02_RACE_FAILURE_PREFIX.length)); } catch {
      return INCOMPLETE_OPS02_RACE_DIAGNOSTICS;
    }
    const safe = safeOps02RaceRecord(decodeOps02RaceWireRecord(value));
    if (!safe) return INCOMPLETE_OPS02_RACE_DIAGNOSTICS;
    records.push(safe);
  }
  if (records.length === 0 || records[0].boundary !== 'case_started'
      || records[0].caseId !== 'OPS02.C02' || records[0].passed !== true
      || records.slice(1).some(record => record.boundary === 'case_started')) {
    return INCOMPLETE_OPS02_RACE_DIAGNOSTICS;
  }
  const exitGroups = new Set();
  for (const record of records) {
    if (record.boundary !== 'child_exit_group') continue;
    if (exitGroups.has(record.raceKind)) return INCOMPLETE_OPS02_RACE_DIAGNOSTICS;
    exitGroups.add(record.raceKind);
  }
  return Object.freeze({ complete: true, recordCount: records.length, records: Object.freeze(records) });
}

function safeOps02RaceDiagnostics(value) {
  let complete = value?.complete === true
    && Number.isSafeInteger(value?.recordCount)
    && value.recordCount > 0 && value.recordCount <= OPS02_RACE_FAILURE_MAX_RECORDS
    && Array.isArray(value?.records) && value.records.length === value.recordCount;
  const records = [];
  for (const record of (Array.isArray(value?.records) ? value.records : []).slice(0, OPS02_RACE_FAILURE_MAX_RECORDS)) {
    const safe = safeOps02RaceRecord(record);
    if (!safe) {
      complete = false;
      break;
    }
    records.push(safe);
  }
  return Object.freeze({
    complete,
    recordCount: complete ? records.length : 0,
    records: Object.freeze(complete ? records : [])
  });
}

function safeOps06ClosePhaseRecord(record) {
  const keys = record && typeof record === 'object' && !Array.isArray(record)
    ? Object.keys(record).sort() : [];
  if (keys.length !== OPS06_CLOSE_PHASE_KEYS.length
      || keys.some((key, index) => key !== OPS06_CLOSE_PHASE_KEYS[index])
      || record.schema !== 'agent-harness.ah11-ops06-close-phases.v2'
      || record.caseId !== 'OPS06.C01' || record.restartOwnership !== 'fresh_process'
      || !new Set([
        'ready_wait', 'worker_start', 'runner_wait', 'runner_pause', 'worker_stop', 'stop_validate',
        'record_output', 'signal_delivery', 'flush_barrier', 'supervisor_close_wait',
        'supervisor_exit', 'stopped_record', 'restart_spawn', 'restart_start', 'restart_wait',
        'restart_stop', 'restart_verify', 'restart_close',
        'restart_exit', 'restart_record', 'restart_state', 'complete', 'unknown'
      ]).has(record.stage)
      || !new Set(['none', 'assertion', 'child_exit', 'unknown']).has(record.failureClass)
      || !new Set(['zero', 'nonzero', 'signal', 'unknown']).has(record.exitDisposition)
      || !(record.exitCode === null || (Number.isSafeInteger(record.exitCode)
        && record.exitCode >= 0 && record.exitCode <= 255))
      || !(record.exitSignal === null || OPS12_RECOVERY_SIGNALS.has(record.exitSignal))) return null;
  const numericKeys = [
    'signalToFirstStopElapsedMs', 'restartWorkElapsedMs', 'finalStopElapsedMs',
    'readyToFinalRecordElapsedMs', 'processCloseElapsedMs', 'restartProcessCloseElapsedMs'
  ];
  if (numericKeys.some(key => !(record[key] === null || (typeof record[key] === 'number'
      && Number.isFinite(record[key]) && record[key] >= 0 && record[key] <= 30_000)))) return null;
  if ((record.exitDisposition === 'zero' && (record.exitCode !== 0 || record.exitSignal !== null))
      || (record.exitDisposition === 'nonzero' && (!Number.isSafeInteger(record.exitCode)
        || record.exitCode === 0 || record.exitSignal !== null))
      || (record.exitDisposition === 'signal' && (record.exitCode !== null || record.exitSignal === null))
      || (record.exitDisposition === 'unknown' && (record.exitCode !== null || record.exitSignal !== null))) return null;
  return Object.freeze({
    schema: 'agent-harness.ah11-ops06-close-phases.v2',
    caseId: 'OPS06.C01',
    stage: record.stage, failureClass: record.failureClass,
    exitDisposition: record.exitDisposition, exitCode: record.exitCode, exitSignal: record.exitSignal,
    signalToFirstStopElapsedMs: record.signalToFirstStopElapsedMs,
    restartWorkElapsedMs: record.restartWorkElapsedMs,
    finalStopElapsedMs: record.finalStopElapsedMs,
    readyToFinalRecordElapsedMs: record.readyToFinalRecordElapsedMs,
    processCloseElapsedMs: record.processCloseElapsedMs,
    restartOwnership: record.restartOwnership,
    restartProcessCloseElapsedMs: record.restartProcessCloseElapsedMs
  });
}

export function parseOps06ClosePhaseDiagnostics(channel) {
  if (!channel || typeof channel !== 'object' || Array.isArray(channel)) return INCOMPLETE_OPS06_CLOSE_PHASE_DIAGNOSTICS;
  const expectedKeys = ['bytes', 'count', 'diagnosticsComplete', 'lines'];
  const keys = Object.keys(channel).sort();
  if (keys.length !== expectedKeys.length || keys.some((key, index) => key !== expectedKeys[index])
      || expectedKeys.some(key => !Object.hasOwn(channel, key))) return INCOMPLETE_OPS06_CLOSE_PHASE_DIAGNOSTICS;
  const { lines, count, bytes, diagnosticsComplete } = channel;
  if (typeof lines !== 'string' || count !== 1 || !Number.isSafeInteger(bytes) || bytes < 0
      || bytes > OPS06_CLOSE_PHASE_FAILURE_MAX_BYTES || diagnosticsComplete !== true
      || lines.length > OPS06_CLOSE_PHASE_FAILURE_MAX_BYTES) return INCOMPLETE_OPS06_CLOSE_PHASE_DIAGNOSTICS;
  const actualBytes = Buffer.byteLength(lines, 'utf8');
  if (actualBytes !== bytes || actualBytes > OPS06_CLOSE_PHASE_FAILURE_MAX_BYTES || !lines.endsWith('\n')) {
    return INCOMPLETE_OPS06_CLOSE_PHASE_DIAGNOSTICS;
  }
  const rawLines = lines.split('\n');
  rawLines.pop();
  if (rawLines.length !== 1 || Buffer.byteLength(`${rawLines[0]}\n`, 'utf8') > OPS06_CLOSE_PHASE_FAILURE_MAX_BYTES
      || !rawLines[0].startsWith(OPS06_CLOSE_PHASE_FAILURE_PREFIX)) return INCOMPLETE_OPS06_CLOSE_PHASE_DIAGNOSTICS;
  let value;
  try { value = JSON.parse(rawLines[0].slice(OPS06_CLOSE_PHASE_FAILURE_PREFIX.length)); } catch {
    return INCOMPLETE_OPS06_CLOSE_PHASE_DIAGNOSTICS;
  }
  const record = safeOps06ClosePhaseRecord(value);
  if (!record) return INCOMPLETE_OPS06_CLOSE_PHASE_DIAGNOSTICS;
  return Object.freeze({ complete: true, recordCount: 1, records: Object.freeze([record]) });
}

function safeOps12RecoveryFailure(record) {
  const keys = record && typeof record === 'object' && !Array.isArray(record)
    ? Object.keys(record).sort() : [];
  if (keys.length !== 5 || keys[0] !== 'caseId' || keys[1] !== 'errorClass'
      || keys[2] !== 'nativePrimaryCode' || keys[3] !== 'schema' || keys[4] !== 'stage'
      || record.schema !== 'agent-harness.ah11-ops12-recovery-failure.v1'
      || !OPS12_RECOVERY_CASES.has(record.caseId)
      || !OPS12_RECOVERY_STAGES.has(record.stage)
      || !OPS12_RECOVERY_ERROR_CLASSES.has(record.errorClass)
      || !(record.nativePrimaryCode === null || record.nativePrimaryCode === 5 || record.nativePrimaryCode === 6)) {
    return null;
  }
  return Object.freeze({
    schema: 'agent-harness.ah11-ops12-recovery-failure.v1',
    caseId: record.caseId, stage: record.stage,
    errorClass: record.errorClass, nativePrimaryCode: record.nativePrimaryCode
  });
}

export function parseOps12RecoveryFailure(stderr) {
  if (typeof stderr !== 'string' || Buffer.byteLength(stderr, 'utf8') > OPS12_RECOVERY_MAX_BYTES) return null;
  const lines = stderr.split(/\r?\n/u).filter(line => line.length > 0);
  if (lines.length !== 1 || Buffer.byteLength(lines[0], 'utf8') > OPS12_RECOVERY_MAX_LINE_BYTES
      || !lines[0].startsWith(OPS12_RECOVERY_FAILURE_PREFIX)) return null;
  let value;
  try { value = JSON.parse(lines[0].slice(OPS12_RECOVERY_FAILURE_PREFIX.length)); } catch {
    return null;
  }
  return safeOps12RecoveryFailure(value);
}

function safeOps12RecoveryRecord(record) {
  const keys = record && typeof record === 'object' && !Array.isArray(record)
    ? Object.keys(record).sort() : [];
  if (keys.length !== 8 || keys[0] !== 'caseId' || keys[1] !== 'errorClass'
      || keys[2] !== 'exitCode' || keys[3] !== 'exitDisposition' || keys[4] !== 'exitSignal'
      || keys[5] !== 'nativePrimaryCode' || keys[6] !== 'schema' || keys[7] !== 'stage'
      || record.schema !== 'agent-harness.ah11-ops12-recovery-child.v1'
      || !(record.caseId === 'none' || OPS12_RECOVERY_CASES.has(record.caseId))
      || !(record.stage === 'none' || OPS12_RECOVERY_STAGES.has(record.stage))
      || !OPS12_RECOVERY_ERROR_CLASSES.has(record.errorClass)
      || !OPS12_RECOVERY_EXIT_DISPOSITIONS.has(record.exitDisposition)
      || !(record.nativePrimaryCode === null || record.nativePrimaryCode === 5 || record.nativePrimaryCode === 6)
      || !(record.exitCode === null || (Number.isSafeInteger(record.exitCode) && record.exitCode >= 0 && record.exitCode <= 255))
      || !(record.exitSignal === null || OPS12_RECOVERY_SIGNALS.has(record.exitSignal))) return null;
  if (record.exitDisposition === 'zero'
      && (record.exitCode !== 0 || record.exitSignal !== null || record.caseId !== 'none'
        || record.stage !== 'none' || record.errorClass !== 'none' || record.nativePrimaryCode !== null)) return null;
  if (record.exitDisposition === 'nonzero'
      && (!Number.isSafeInteger(record.exitCode) || record.exitCode === 0 || record.exitSignal !== null)) return null;
  if (record.exitDisposition === 'signal'
      && (record.exitCode !== null || record.exitSignal === null)) return null;
  if (record.exitDisposition === 'unknown'
      && (record.exitCode !== null || record.exitSignal !== null)) return null;
  if ((record.caseId === 'none') !== (record.stage === 'none')) return null;
  return Object.freeze({
    schema: 'agent-harness.ah11-ops12-recovery-child.v1',
    caseId: record.caseId, stage: record.stage, errorClass: record.errorClass,
    nativePrimaryCode: record.nativePrimaryCode, exitDisposition: record.exitDisposition,
    exitCode: record.exitCode, exitSignal: record.exitSignal
  });
}

export function buildOps12RecoveryDiagnosticRecord(result, childFailure) {
  let exitDisposition = Number.isSafeInteger(result?.code) && result.code >= 0
    ? (result.code === 0 ? 'zero' : 'nonzero')
    : result?.code === null && typeof result?.signal === 'string' && result.signal.length > 0
      ? 'signal' : 'unknown';
  const exitSignal = typeof result?.signal === 'string' && OPS12_RECOVERY_SIGNALS.has(result.signal)
    ? result.signal : null;
  if (exitDisposition === 'signal' && exitSignal === null) exitDisposition = 'unknown';
  const exitCode = Number.isSafeInteger(result?.code) && result.code >= 0 && result.code <= 255
    ? result.code : null;
  return safeOps12RecoveryRecord({
    schema: 'agent-harness.ah11-ops12-recovery-child.v1',
    caseId: childFailure?.caseId ?? (exitDisposition === 'zero' ? 'none' : 'unknown'),
    stage: childFailure?.stage ?? (exitDisposition === 'zero' ? 'none' : 'unknown'),
    errorClass: childFailure?.errorClass ?? (exitDisposition === 'zero' ? 'none' : 'unknown'),
    nativePrimaryCode: childFailure?.nativePrimaryCode ?? null,
    exitDisposition, exitCode,
    exitSignal: exitDisposition === 'signal' ? exitSignal : null
  });
}

export function parseOps12RecoveryDiagnostics(channel) {
  if (!channel || typeof channel !== 'object' || Array.isArray(channel)) return INCOMPLETE_OPS12_RECOVERY_DIAGNOSTICS;
  const expectedKeys = ['bytes', 'count', 'diagnosticsComplete', 'lines'];
  const keys = Object.keys(channel).sort();
  if (keys.length !== expectedKeys.length || keys.some((key, index) => key !== expectedKeys[index])
      || expectedKeys.some(key => !Object.hasOwn(channel, key))) return INCOMPLETE_OPS12_RECOVERY_DIAGNOSTICS;
  const { lines, count, bytes, diagnosticsComplete } = channel;
  if (typeof lines !== 'string' || count !== 1 || !Number.isSafeInteger(bytes) || bytes < 0
      || bytes > OPS12_RECOVERY_MAX_BYTES || diagnosticsComplete !== true
      || Buffer.byteLength(lines, 'utf8') !== bytes || !lines.endsWith('\n')) {
    return INCOMPLETE_OPS12_RECOVERY_DIAGNOSTICS;
  }
  const rawLines = lines.slice(0, -1).split('\n');
  if (rawLines.length !== 1 || Buffer.byteLength(`${rawLines[0]}\n`, 'utf8') > OPS12_RECOVERY_MAX_LINE_BYTES
      || !rawLines[0].startsWith(OPS12_RECOVERY_DIAGNOSTIC_PREFIX)) return INCOMPLETE_OPS12_RECOVERY_DIAGNOSTICS;
  let value;
  try { value = JSON.parse(rawLines[0].slice(OPS12_RECOVERY_DIAGNOSTIC_PREFIX.length)); } catch {
    return INCOMPLETE_OPS12_RECOVERY_DIAGNOSTICS;
  }
  const record = safeOps12RecoveryRecord(value);
  if (!record) return INCOMPLETE_OPS12_RECOVERY_DIAGNOSTICS;
  return Object.freeze({ complete: true, recordCount: 1, records: Object.freeze([record]) });
}

function safeOps12RecoveryDiagnostics(value) {
  const safe = Array.isArray(value?.records) && value.records.length === 1
    ? safeOps12RecoveryRecord(value.records[0]) : null;
  const complete = value?.complete === true && value?.recordCount === 1 && safe !== null;
  return Object.freeze({
    complete, recordCount: complete ? 1 : 0,
    records: Object.freeze(complete ? [safe] : [])
  });
}

function safeOps06ClosePhaseDiagnostics(value) {
  const safe = Array.isArray(value?.records) && value.records.length === 1
    ? safeOps06ClosePhaseRecord(value.records[0]) : null;
  const complete = value?.complete === true && value?.recordCount === 1 && safe !== null;
  return Object.freeze({
    complete, recordCount: complete ? 1 : 0,
    records: Object.freeze(complete ? [safe] : [])
  });
}

function safeScenario(value) {
  return identifierFrom(value)
    ?? (['quota-shutdown-restart', 'runbook-clean-start', 'diagnostic-control'].includes(value)
      ? value : 'AH-11-OPS');
}

function sourceLocation(value) {
  if (typeof value !== 'string') return 'redacted';
  let filePath;
  let line;
  let column;
  const urlMatch = value.match(/(file:\/\/[^\s'"<>]+):(\d+):(\d+)/u);
  const pathMatch = value.match(/((?:tests|src)\/[A-Za-z0-9_./-]+):(\d+):(\d+)/u);
  try {
    if (urlMatch) {
      filePath = fileURLToPath(new URL(urlMatch[1]));
      line = urlMatch[2];
      column = urlMatch[3];
    } else if (pathMatch) {
      filePath = resolve(ROOT, pathMatch[1]);
      line = pathMatch[2];
      column = pathMatch[3];
    } else {
      return 'redacted';
    }
  } catch {
    return 'redacted';
  }
  const rel = relative(ROOT, filePath).split(sep).join('/');
  if (!/^(?:tests|src)\/[A-Za-z0-9_./-]+$/u.test(rel)
      || rel === '..' || rel.startsWith('../') || !Number.isSafeInteger(Number(line))
      || !Number.isSafeInteger(Number(column))) return 'redacted';
  return `${rel}:${line}:${column}`;
}

function identifierFrom(value) {
  return value?.match(/\bOPS\d{2}\.C\d{2}\b/u)?.[0]
    ?? value?.match(/\bOPS\d{2}_[A-Z0-9_]+\b/u)?.[0]
    ?? null;
}

function scalar(block, field, expectedIndentation = null) {
  for (const line of block.split(/\r?\n/u)) {
    const match = line.match(/^([ \t]*)([A-Za-z][A-Za-z0-9_-]*):\s*['"]?([A-Za-z0-9_./:-]{1,160})['"]?,?\s*$/u);
    if (match?.[2] === field
        && (expectedIndentation === null || match[1].length === expectedIndentation)) return match[3];
  }
  return null;
}

function sqlitePrimaryCode(value) {
  const code = typeof value === 'number' ? value
    : typeof value === 'string' && /^\d{1,9}$/u.test(value) ? Number(value) : null;
  if (!Number.isSafeInteger(code) || code < 0) return null;
  const primary = code & 0xff;
  return primary === 5 || primary === 6 ? primary : null;
}

function safeNestedScalar(value) {
  const normalized = value?.trim();
  const quoted = normalized?.match(/^'([A-Za-z0-9_.:-]{1,160})',?$/u);
  if (quoted) return quoted[1];
  const plain = normalized?.match(/^([A-Za-z0-9_.:-]{1,160}),?$/u);
  return plain?.[1] ?? null;
}

function directErrorFields(lines, indentation) {
  const fields = new Map();
  for (const line of lines) {
    const match = line.match(/^([ \t]*)([A-Za-z][A-Za-z0-9_-]*):(?:[ \t]+(.*?))?[ \t]*$/u);
    if (!match || match[1].length !== indentation) continue;
    if (!['name', 'code', 'errcode'].includes(match[2])) continue;
    if (fields.has(match[2])) return null;
    const value = match[3] === undefined ? null : safeNestedScalar(match[3]);
    if (value === null) return null;
    fields.set(match[2], value);
  }
  return fields;
}

function storageCauseFromFields(fields) {
  if (!fields) return null;
  const name = fields.get('name');
  const code = fields.get('code');
  if (name === 'RuntimeError' && code === 'STORE_BUSY') {
    return Object.freeze({
      failureCode: 'SQLITE_BUSY', errorClass: 'RuntimeError',
      errorCode: 'STORE_BUSY', sqlitePrimaryCode: null
    });
  }
  const primary = sqlitePrimaryCode(fields.get('errcode'));
  if ((name === 'Error' || name === 'SQLiteError') && code === 'ERR_SQLITE_ERROR' && primary !== null) {
    return Object.freeze({
      failureCode: 'SQLITE_BUSY', errorClass: 'Error',
      errorCode: 'ERR_SQLITE_ERROR', sqlitePrimaryCode: primary
    });
  }
  return null;
}

function nestedStorageFailure(block, parentIndentation) {
  const lines = block.split(/\r?\n/u);
  let inspectedRoots = 0;
  for (let index = 0; index < lines.length; index += 1) {
    const root = lines[index].match(/^([ \t]*)(error|cause):(?:[ \t]*(.*))?$/u);
    if (!root || root[1].length !== parentIndentation || (root[3] ?? '') !== '') continue;
    inspectedRoots += 1;
    if (inspectedRoots > MAX_NESTED_ERROR_ROOTS) break;

    const descendants = [];
    for (let next = index + 1; next < lines.length; next += 1) {
      const indentation = lines[next].match(/^[ \t]*/u)?.[0].length ?? 0;
      if (lines[next].trim() && indentation <= parentIndentation) break;
      descendants.push(lines[next]);
    }

    const firstIndentation = descendants
      .map(line => ({ line, indentation: line.match(/^[ \t]*/u)?.[0].length ?? 0 }))
      .find(item => item.line.trim())?.indentation;
    if (firstIndentation === undefined || firstIndentation <= parentIndentation) continue;

    const directCause = storageCauseFromFields(directErrorFields(descendants, firstIndentation));
    if (directCause) return directCause;

    for (let markerIndex = 0; markerIndex < descendants.length; markerIndex += 1) {
      const line = descendants[markerIndex];
      const marker = line.match(/^([ \t]*)(RuntimeError|Error|SQLiteError):[ \t]*$/u);
      if (!marker || marker[1].length !== firstIndentation) continue;
      const markerIndentation = marker[1].length;
      const markerLines = [];
      for (let nestedIndex = markerIndex + 1; nestedIndex < descendants.length; nestedIndex += 1) {
        const nestedLine = descendants[nestedIndex];
        const nestedIndentation = nestedLine.match(/^[ \t]*/u)?.[0].length ?? 0;
        if (nestedLine.trim() && nestedIndentation <= markerIndentation) break;
        markerLines.push(nestedLine);
      }
      const fields = directErrorFields(markerLines, markerIndentation + 2);
      if (fields?.get('name') !== marker[2]) continue;
      const markedCause = storageCauseFromFields(fields);
      if (markedCause) return markedCause;
    }
  }
  return null;
}

function assertionValueKind(block, field) {
  const pattern = new RegExp(`^[ \\t]*${field}:\\s*(.*?)\\s*$`, 'mu');
  const value = block.match(pattern)?.[1];
  if (value === undefined) return null;
  const normalized = value.trim();
  if (/^(?:null|~)$/iu.test(normalized)) return 'null';
  if (/^(?:true|false)$/iu.test(normalized)) return 'boolean';
  if (/^[+-]?(?:\d+\.?\d*|\.\d+)(?:e[+-]?\d+)?$/iu.test(normalized)) return 'number';
  if (normalized.startsWith('[') || normalized.startsWith('- ')) return 'array';
  if (normalized.startsWith('{')) return 'object';
  if (normalized.startsWith("'") || normalized.startsWith('"')
      || normalized.startsWith('|') || normalized.startsWith('>')) return 'string';
  if (normalized === '') return 'complex';
  return 'scalar';
}

function classify({ name, code, sqliteCode, timedOut }) {
  if (timedOut || code === 'ETIMEDOUT') return 'CHILD_TIMEOUT';
  if (code === 'ERR_CHILD_PROCESS_STDIO_MAXBUFFER') return 'OUTPUT_LIMIT';
  if (name === 'SyntaxError') return 'JSON_PARSE';
  if (name === 'AssertionError' || code === 'ERR_ASSERTION') return 'ASSERTION_FAILURE';
  if (code === 'STORE_BUSY' || code === 'SQLITE_BUSY' || code === 'SQLITE_LOCKED'
      || (code === 'ERR_SQLITE_ERROR' && sqliteCode !== null)) return 'SQLITE_BUSY';
  return 'UNCLASSIFIED_FAILURE';
}

function makeRecord({
  scenario, phase, stage, status, signal, errorCode, errorName, timedOut,
  outputBytes, outputTruncated, caseId, location, operator, failureType,
  actualValueKind, expectedValueKind, missingCases, sqliteCode = null,
  nestedStorageCause = null
}) {
  const baseFailureCode = classify({
    name: errorName,
    code: errorCode,
    timedOut,
    sqliteCode
  });
  const nestedFailureCode = errorName === 'AssertionError' || errorCode === 'ERR_ASSERTION'
    ? null : nestedStorageCause?.failureCode ?? null;
  return {
    scenario: safeScenario(scenario),
    phase,
    stage,
    caseId: identifierFrom(caseId),
    failureCode: baseFailureCode === 'UNCLASSIFIED_FAILURE' && nestedFailureCode
      ? nestedFailureCode : baseFailureCode,
    errorClass: safeErrorName(errorName) ?? 'redacted',
    errorCode: safeErrorCode(errorCode),
    sqlitePrimaryCode: sqliteCode,
    nestedStorageCause,
    failureType: ['testCodeFailure', 'testTimeoutFailure', 'subtestsFailed'].includes(failureType)
      ? failureType : null,
    sourceLocation: sourceLocation(location),
    assertionOperator: SAFE_OPERATORS.has(operator) ? operator : null,
    assertionContext: {
      actualValueKind: actualValueKind ?? null,
      expectedValueKind: expectedValueKind ?? null
    },
    status: Number.isSafeInteger(status) && status >= 0 && status <= 255 ? status : null,
    signal: safeSignal(signal),
    timedOut: Boolean(timedOut || errorCode === 'ETIMEDOUT'),
    outputBytes: Number.isSafeInteger(outputBytes) && outputBytes >= 0 ? outputBytes : 0,
    outputTruncated: Boolean(outputTruncated),
    missingCaseIds: [...new Set((missingCases ?? []).filter(value => /^OPS\d{2}\.C\d{2}$/u.test(value)))].slice(0, 52)
  };
}

export function buildSafeFailureDiagnostics({
  scenario, phase, stage, status = null, signal = null, error = null,
  stdout = '', stderr = '', missingCases = [], c02ControlReadDiagnostics = undefined,
  ops02RaceDiagnostics = undefined, ops06ClosePhaseDiagnostics = undefined,
  ops12RecoveryDiagnostics = undefined
}) {
  const combined = `${typeof stdout === 'string' ? stdout : ''}\n${typeof stderr === 'string' ? stderr : ''}`;
  const outputBytes = Buffer.byteLength(combined, 'utf8');
  const truncatedByLimit = outputBytes > MAX_INPUT_BYTES
    || error?.code === 'ERR_CHILD_PROCESS_STDIO_MAXBUFFER';
  const input = truncatedByLimit ? Buffer.from(combined).subarray(0, MAX_INPUT_BYTES).toString('utf8') : combined;
  const lines = input.split(/\r?\n/u).slice(0, 20_000);
  const records = [];
  let currentSubtest = null;

  for (let index = 0; index < lines.length && records.length < MAX_RECORDS; index += 1) {
    const subtest = lines[index].match(/^[ \t]*# Subtest: (.+)$/u);
    if (subtest) currentSubtest = subtest[1];
    const failed = lines[index].match(/^[ \t]*not ok \d+ - (.+)$/u);
    if (!failed) continue;

    const indent = lines[index].match(/^[ \t]*/u)?.[0].length ?? 0;
    const block = [];
    for (let next = index + 1; next < lines.length; next += 1) {
      const candidate = lines[next];
      if (candidate.trim() && (candidate.match(/^[ \t]*/u)?.[0].length ?? 0) <= indent
          && /^(?:not )?ok \d+ - |# (?:tests|pass|fail)\b/u.test(candidate.trim())) break;
      block.push(candidate);
    }
    const blockText = block.join('\n');
    const recordIndentation = indent + 2;
    const name = scalar(blockText, 'name', recordIndentation);
    const code = scalar(blockText, 'code', recordIndentation);
    const failureType = scalar(blockText, 'failureType', recordIndentation);
    const operator = scalar(blockText, 'operator', recordIndentation);
    const location = scalar(blockText, 'location', recordIndentation);
    const nestedCause = nestedStorageFailure(blockText, recordIndentation);
    const primaryCode = sqlitePrimaryCode(scalar(blockText, 'errcode', recordIndentation));
    records.push(makeRecord({
      scenario, phase, stage, status, signal,
      errorCode: safeErrorCode(code), errorName: safeErrorName(name),
      timedOut: error?.code === 'ETIMEDOUT', outputBytes, sqliteCode: primaryCode,
      nestedStorageCause: nestedCause,
      outputTruncated: truncatedByLimit || lines.length >= 20_000,
      caseId: identifierFrom(failed[1]) ?? identifierFrom(currentSubtest),
      location,
      operator: SAFE_OPERATORS.has(operator) ? operator : null,
      actualValueKind: assertionValueKind(blockText, 'actual'),
      expectedValueKind: assertionValueKind(blockText, 'expected'),
      failureType,
      missingCases
    }));
  }

  const processFailed = status !== 0 || signal !== null || error !== null;
  if (processFailed && records.length === 0 && records.length < MAX_RECORDS) {
    records.push(makeRecord({
      scenario, phase, stage, status, signal,
      errorCode: safeErrorCode(error?.code),
      errorName: safeErrorName(error?.name),
      timedOut: error?.code === 'ETIMEDOUT',
      outputBytes, sqliteCode: sqlitePrimaryCode(error?.errcode),
      outputTruncated: truncatedByLimit || lines.length >= 20_000,
      caseId: identifierFrom(currentSubtest),
      location: null,
      operator: null,
      failureType: null,
      missingCases,
      nestedStorageCause: null
    }));
  }

  if (missingCases.length > 0 && records.length < MAX_RECORDS) {
    records.push(makeRecord({
      scenario, phase, stage, status, signal,
      errorCode: null,
      errorName: null,
      timedOut: false,
      outputBytes,
      outputTruncated: truncatedByLimit || lines.length >= 20_000,
      caseId: identifierFrom(currentSubtest),
      location: null,
      operator: null,
      failureType: null,
      missingCases,
      nestedStorageCause: null
    }));
    records.at(-1).failureCode = 'REQUIRED_CASES_MISSING';
  }

  return Object.freeze({
    schema: 'agent-harness.ah11-ops-safe-diagnostics.v1',
    scenario: safeScenario(scenario),
    phase,
    stage,
    status: Number.isSafeInteger(status) && status >= 0 && status <= 255 ? status : null,
    signal: safeSignal(signal),
    timedOut: error?.code === 'ETIMEDOUT',
    outputBytes,
    outputTruncated: truncatedByLimit || lines.length >= 20_000,
    records: Object.freeze(records),
    ...(c02ControlReadDiagnostics === undefined ? {} : {
      c02ControlReadDiagnostics: safeC02ControlReadDiagnostics(c02ControlReadDiagnostics)
    }),
    ...(ops02RaceDiagnostics === undefined ? {} : {
      ops02RaceDiagnostics: safeOps02RaceDiagnostics(ops02RaceDiagnostics)
    }),
    ...(ops06ClosePhaseDiagnostics === undefined ? {} : {
      ops06ClosePhaseDiagnostics: safeOps06ClosePhaseDiagnostics(ops06ClosePhaseDiagnostics)
    }),
    ...(ops12RecoveryDiagnostics === undefined ? {} : {
      ops12RecoveryDiagnostics: safeOps12RecoveryDiagnostics(ops12RecoveryDiagnostics)
    })
  });
}
