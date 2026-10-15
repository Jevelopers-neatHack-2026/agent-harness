import { appendClosedDiagnostic, closedErrorProjection, closedExitDisposition, createClosedDiagnosticChannel,
  parseClosedChildDiagnostic, parseClosedDiagnosticChannel, observeClosedChild, closedChildObservation,
  requiresClosedFixtureRetention, waitForClosedChild } from './closed-diagnostics.js';
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { closeSync, copyFileSync, mkdirSync, mkdtempSync, openSync, readFileSync, readSync, readdirSync, renameSync, rmSync, writeFileSync, existsSync, writeSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { performance } from 'node:perf_hooks';
import { after, test } from 'node:test';
import {
  createHmacTokenizer,
  createTelemetryExporter,
  DurableWorkflowStore,
  INTERNAL_SCHEMA_VERSION,
  TelemetryExporter,
  projectTelemetryEvent
} from '@jevelopers/agent-harness-runtime';
import { readOperationsAdmissionBusyDiagnostic } from '../../src/internal/durable-store.js';
import { OPERATIONS_STORAGE_CAPABILITY } from '../../src/internal/private-ops-store-capability.js';
import {
  createOperationsDiagnosticSession,
  disposeOperationsDiagnosticSession,
  enableOperationsRunnerCaptureFromFd3,
  finishOperationsRunnerCapture,
  prepareOperationsChildCapture,
  closeOperationsChildCapture,
  OPS07_CAPTURE_LINE_PREFIX,
  OPS07_CAPTURE_LIMITS,
  parseOperationsCaptureFromTap,
  readOperationsDiagnosticSession,
  recordOperationsDiagnostic,
  recordOperationsSupervisor
} from '../../src/internal/private-ops-diagnostics.js';
import { runtimeError } from '../../src/internal/errors.js';
import { operationsStoragePolicy, readOperationsConfig, validateLocalPlanWork } from '../../src/worker/private/config.js';
import { createOperationsWorker } from '../../src/worker/private/worker.js';
import { readStoreCompatibility, readOperationsHealth, requestOwnerRetention } from '../../src/health/private/health.js';
import { runBoundedLocalPlan } from '../../src/orchestrator/internal/controller.js';
import {
  buildOps12RecoveryDiagnosticRecord, buildSafeFailureDiagnostics, encodeOps02RaceDiagnostic, parseC02ControlReadDiagnostics,
  parseOps02RaceDiagnostics, parseOps06ClosePhaseDiagnostics,
  parseOps12RecoveryDiagnostics, parseOps12RecoveryFailure
} from './failure-diagnostics.js';
import { createAdmissionFailureDiagnostic } from './fixtures/admission-store-child.mjs';
import { parseFixtureRetryDiagnostic } from './fixture-busy-retry.js';
import { cleanupC04Fixture, encodeC04CompletionDiagnostic, observeC04PreCleanup,
  projectC04WorkPage, readC04FinalRow, throwC04CleanupErrors } from './fixture-recovery-diagnostics.js';
import {
  classifyOps04ExitDisposition, createOps04FailureDetails, isOps04ExitDisposition,
  ops04FailureFields, ops04ReadErrorKind, parseOps04FailureDetails, throwAfterOps04FailureCapture,
  validateOps04PublicPhaseMetrics
} from './ops04-failure-diagnostics.js';

const ROOT = resolve(new URL('../..', import.meta.url).pathname);
const PLAN = JSON.stringify({
  kind: 'agent-harness.private-synthetic-plan',
  version: 1,
  steps: [{ id: 'sum', operation: 'number.add', dependsOn: [], args: { left: 20, right: 22 } }]
});
const PLAN_TWO = JSON.stringify({
  kind: 'agent-harness.private-synthetic-plan',
  version: 1,
  steps: [{ id: 'sum', operation: 'number.add', dependsOn: [], args: { left: 21, right: 22 } }]
});
const METRICS = {
  closedFixtureDiagnostics: createClosedDiagnosticChannel(),
  processRuns: 0,
  processSigtermSignalsSent: 0,
  processSigintSignalsSent: 0,
  processSigkillSignalsSent: 0,
  controlledDnsResolve4: 0,
  controlledDnsResolve6: 0,
  controlledHttpsRequests: 0,
  controlledHttpsWrites: 0,
  blockedExternalNetworkAttempts: 0,
  providerCalls: 0,
  ownedRunnerStarts: 0,
  ownedRunnerTermSignals: 0,
  ownedRunnerKillSignals: 0,
  ownedRunnerShutdownElapsedMsMax: 0,
  ownedRunnerShutdownSamples: 0,
  processSigtermDrainElapsedMsMax: 0,
  processSigtermDrainSamples: 0,
  processCloseElapsedMsMax: 0,
  processCloseSamples: 0,
  runnerDeadlineCloseElapsedMsMax: 0,
  runnerDeadlineCloseSamples: 0,
  storeHighWaterBytes: 0,
  storeReservedHighWaterBytes: 0,
  databaseHighWaterBytes: 0,
  walHighWaterBytes: 0,
  shmHighWaterBytes: 0,
  freeSpaceLowWaterBytes: Number.MAX_SAFE_INTEGER,
  pendingEventHighWaterRecords: 0,
  reservedEventHighWaterSlots: 0,
  resourceHighWaterSamples: 0,
  ownedRunnerCountHighWater: 0,
  supervisorOpenFileDescriptors: 0,
  runnerOpenFileDescriptors: 0,
  maxSupervisorRssBytes: 0,
  maxRunnerRssBytes: 0,
  ci1PhaseCaptures: [],
  ci1PhaseCaptureBytes: 0,
  ci1PhaseCaptureOverflow: false,
  ci1PhaseCaptureIncomplete: false,
  ci1PhaseCaptureChildCloseUnconfirmed: false,
  ci1PhaseCaptureOwnedGroupCloseUnconfirmed: false,
  c02ControlReadFailureLines: '',
  c02ControlReadFailureCount: 0,
  c02ControlReadFailureBytes: 0,
  c02ControlReadDiagnosticsComplete: true,
  ops02RaceDiagnosticLines: '',
  ops02RaceDiagnosticCount: 0,
  ops02RaceDiagnosticBytes: 0,
  ops02RaceDiagnosticsComplete: true,
  ops06ClosePhaseDiagnosticLines: '',
  ops06ClosePhaseDiagnosticCount: 0,
  ops06ClosePhaseDiagnosticBytes: 0,
  ops06ClosePhaseDiagnosticsComplete: true,
  ops12RecoveryDiagnosticLines: '',
  ops12RecoveryDiagnosticCount: 0,
  ops12RecoveryDiagnosticBytes: 0,
  ops12RecoveryDiagnosticsComplete: true
};

const CI1_PHASES = new Set([
  'before-marker', 'after-marker-before-bytes', 'after-write',
  'response-before-observation', 'observation-before-checkpoint'
]);
const CI1_CHILD_PHASE_STAGES = new Set([
  'fixture_started', 'store_opened', 'workflow_opened', 'worker_started', 'runner_claimed', 'marker_written',
  'signal_seen', 'stop_requested', 'stop_settled', 'loop_settled', 'shutdown_verified',
  'restart_worker_started', 'restart_work_completed', 'restart_stop_settled',
  'restart_loop_settled', 'restart_store_closed',
  'store_close_started', 'store_closed', 'runner_escalation_sent', 'fixture_failure',
  'fixture_finally_started', 'fixture_finally_complete'
]);
const CI1_PARENT_PHASE_STAGES = new Set([
  'shutdown_spawned', 'shutdown_marker_observed', 'sigterm_sent', 'shutdown_child_closed',
  'restart_spawned', 'restart_escalation_sent', 'restart_child_closed',
  'shutdown_escalation_sent', 'cleanup_escalation_sent', 'child_close_unconfirmed',
  'owned_group_empty', 'owned_group_close_unconfirmed',
  'child_capture_missing', 'child_capture_invalid', 'child_capture_partial', 'child_capture_overflow'
]);
const CI1_MAX_PHASE_RECORDS = 256;
const CI1_MAX_PHASE_BYTES = 24_576;
const CI1_TEMP_CUSTODY_SCHEMA = 'ops-ci1-temp-custody.v1';
const CI1_TEMP_CUSTODY_MAX_BYTES = 8_192;
const CI1_TEMP_CUSTODY_MAX_LAUNCHES = 16;
const CI1_TEMP_CUSTODY_GROUP_PROBE_MS = 300;
const CI1_LOCAL_WATCHDOG_POLL_MS = 20;
const CI1_LOCAL_WATCHDOG_SIGNAL_PHASE_MS = 500;
const CI1_LOCAL_WATCHDOG_RESULT_CLOSE_MARGIN_MS = 1_000;
const CI1_LOCAL_WATCHDOG_CLEANUP_COMPLETION_MS = CI1_LOCAL_WATCHDOG_POLL_MS
  + (2 * CI1_LOCAL_WATCHDOG_SIGNAL_PHASE_MS)
  + CI1_LOCAL_WATCHDOG_RESULT_CLOSE_MARGIN_MS;
const C02_CONTROL_DIAGNOSTIC_PREFIX = 'AH11_OPS_C02_CONTROL_FAILURE ';
const C02_CONTROL_DIAGNOSTIC_MAX_RECORDS = 8;
const C02_CONTROL_DIAGNOSTIC_MAX_LINE_BYTES = 192;
const C02_CONTROL_DIAGNOSTIC_MAX_BYTES = 1_536;
const C02_CONTROL_DIAGNOSTIC_BOUNDARIES = new Set([
  'C02_RUNNER_READY', 'C02_MARKER_CONTROL_READY', 'C02_WATCHDOG_READY',
  'C02_WATCHDOG_TARGET_ACK', 'C02_INERT_DESCENDANT_MARKER', 'C02_WATCHDOG_RESULT'
]);
const OPS02_RACE_DIAGNOSTIC_PREFIX = 'AH11_OPS_OPS02_C02_DIAGNOSTIC_V3 ';
const OPS02_RACE_DIAGNOSTIC_MAX_RECORDS = 64;
const OPS02_RACE_DIAGNOSTIC_MAX_LINE_BYTES = 320;
const OPS02_RACE_DIAGNOSTIC_MAX_BYTES = 16_384;
const OPS02_RACE_PRODUCER_STATES = new WeakMap();
const OPS02_RACE_DIAGNOSTIC_BOUNDARIES = new Set([
  'case_started', 'diagnostic_no_canary', 'diagnostic_proxy_unread',
  'diagnostic_child_exit', 'diagnostic_child_prefix', 'diagnostic_child_no_canary',
  'seed_accept', 'contender_exit', 'child_exit_group', 'accepted_count',
  'refusal_count', 'stored_counter', 'stored_rows', 'reservation'
]);
const OPS02_RACE_DIAGNOSTIC_RACES = new Set([
  'diagnostic', 'global_records', 'project_bytes_existing', 'project_bytes_new',
  'project_records', 'global_bytes'
]);
const OPS02_RACE_DIAGNOSTIC_EXITS = new Set([
  'not_applicable', 'zero', 'nonzero', 'signal', 'unknown'
]);
const OPS02_ADMISSION_FAILURE_STAGES = new Set([
  'config', 'open', 'resource', 'validation', 'admission', 'result', 'close', 'unknown'
]);
const OPS02_ADMISSION_FAILURE_CODES = new Set([
  'OPS_CONFIG_INVALID', 'STORE_OPEN_FAILED', 'STORE_WRITE_FAILED', 'STORE_BUSY', 'STORE_CLOSED',
  'UNSUPPORTED_SCHEMA_VERSION', 'UNVERSIONED_DATA', 'INVALID_INPUT', 'ERR_SQLITE_ERROR',
  'ERR_INVALID_ARG_TYPE', 'EACCES', 'ENOENT', 'ENOSPC', 'UNKNOWN'
]);
const OPS02_CHILD_DIAGNOSTIC_STAGES = new Set([
  ...OPS02_ADMISSION_FAILURE_STAGES, 'none', 'unobserved'
]);
const OPS02_CHILD_DIAGNOSTIC_CODES = new Set([
  ...OPS02_ADMISSION_FAILURE_CODES, 'none', 'unobserved'
]);
const OPS02_TRANSACTION_BOUNDARIES = new Set(['begin', 'body', 'commit', 'rollback', 'none', 'unobserved']);
const OPS02_SQLITE_PRIMARY_CODES = new Set([null, 5, 6]);
const OPS02_ROLLBACK_DISPOSITIONS = new Set([
  'not_observed', 'inactive', 'unknown', 'rolled_back', 'rollback_failed', 'rollback_check_failed'
]);
const OPS06_CLOSE_PHASE_DIAGNOSTIC_PREFIX = 'AH11_OPS_OPS06_CLOSE_PHASES ';
const OPS06_CLOSE_PHASE_DIAGNOSTIC_MAX_RECORDS = 1;
const OPS06_CLOSE_PHASE_DIAGNOSTIC_MAX_LINE_BYTES = 512;
const OPS06_CLOSE_PHASE_DIAGNOSTIC_MAX_BYTES = 512;
const OPS06_CLOSE_PHASE_STAGES = new Set([
  'ready_wait', 'worker_start', 'runner_wait', 'runner_pause', 'worker_stop', 'stop_validate',
  'record_output', 'signal_delivery', 'flush_barrier', 'supervisor_close_wait',
  'supervisor_exit', 'stopped_record', 'restart_spawn', 'restart_start', 'restart_wait',
  'restart_stop', 'restart_verify', 'restart_close',
  'restart_exit', 'restart_record', 'restart_state', 'complete', 'unknown'
]);
const OPS06_CLOSE_PHASE_EXITS = new Set(['zero', 'nonzero', 'signal', 'unknown']);
const OPS06_CLOSE_PHASE_SIGNALS = new Set(['SIGABRT', 'SIGINT', 'SIGKILL', 'SIGTERM']);
const OPS12_RECOVERY_DIAGNOSTIC_PREFIX = 'AH11_OPS_OPS12_RECOVERY_DIAGNOSTIC ';
const OPS12_RECOVERY_DIAGNOSTIC_MAX_RECORDS = 1;
const OPS12_RECOVERY_DIAGNOSTIC_MAX_LINE_BYTES = 512;
const OPS12_RECOVERY_DIAGNOSTIC_MAX_BYTES = 1_024;
const OPS12_RECOVERY_CASES = new Set([
  'busy_store_spans_stop', 'store_unavailable', 'future_schema',
  'stuck_runner_review_escalation', 'unknown_hold_restart_escalation', 'unknown', 'none'
]);
const OPS12_RECOVERY_STAGES = new Set([
  'fixture_start', 'case_setup', 'busy_write', 'worker_stop', 'reopen_validate',
  'compatibility_read', 'future_schema_write', 'worker_open', 'byte_compare',
  'runner_pause', 'await_park', 'restart', 'hold_setup', 'intent_write',
  'lease_expiry', 'hold_verify', 'case_complete', 'none', 'unknown'
]);
const OPS12_RECOVERY_ERROR_CLASSES = new Set([
  'assertion', 'sqlite_busy', 'sqlite_other', 'type', 'range', 'error', 'unknown', 'none'
]);
const OPS12_RECOVERY_EXIT_DISPOSITIONS = new Set(['zero', 'nonzero', 'signal', 'unknown']);
const OPS12_RECOVERY_SIGNALS = new Set(['SIGABRT', 'SIGINT', 'SIGKILL', 'SIGTERM']);

function c02OriginalErrorClass(error) {
  if (error instanceof SyntaxError) return 'SyntaxError';
  if (error instanceof TypeError) return 'TypeError';
  if (error instanceof RangeError) return 'RangeError';
  return 'Error';
}

function markC02DiagnosticCaptureIncomplete(state) {
  try { state.c02ControlReadDiagnosticsComplete = false; } catch {}
  try { state.ci1PhaseCaptureIncomplete = true; } catch {}
}

function recordC02ControlReadFailure(boundary, error, state = METRICS) {
  try {
    if (!C02_CONTROL_DIAGNOSTIC_BOUNDARIES.has(boundary)
        || !Number.isSafeInteger(state.c02ControlReadFailureCount)
        || !Number.isSafeInteger(state.c02ControlReadFailureBytes)
        || typeof state.c02ControlReadFailureLines !== 'string') {
      markC02DiagnosticCaptureIncomplete(state);
      return false;
    }
    const line = `${C02_CONTROL_DIAGNOSTIC_PREFIX}${JSON.stringify({
      schema: 'agent-harness.ah11-ops-c02-control-failure.v1',
      caseId: 'OPS04.C02', boundary, errorClass: c02OriginalErrorClass(error)
    })}\n`;
    const lineBytes = Buffer.byteLength(line, 'utf8');
    if (state.c02ControlReadFailureCount >= C02_CONTROL_DIAGNOSTIC_MAX_RECORDS
        || lineBytes > C02_CONTROL_DIAGNOSTIC_MAX_LINE_BYTES
        || state.c02ControlReadFailureBytes + lineBytes > C02_CONTROL_DIAGNOSTIC_MAX_BYTES) {
      markC02DiagnosticCaptureIncomplete(state);
      return false;
    }
    state.c02ControlReadFailureLines += line;
    state.c02ControlReadFailureCount += 1;
    state.c02ControlReadFailureBytes += lineBytes;
    return true;
  } catch {
    markC02DiagnosticCaptureIncomplete(state);
    return false;
  }
}

function markOps02RaceDiagnosticsIncomplete(state = METRICS) {
  try { state.ops02RaceDiagnosticsComplete = false; } catch {}
}

function parseOps02AdmissionFailure(stderr) {
  if (typeof stderr !== 'string' || Buffer.byteLength(stderr, 'utf8') > 512) return null;
  const lines = stderr.split(/\r?\n/u).filter(line => line.length > 0);
  if (lines.length !== 1 || !lines[0].startsWith('OPS_ADMISSION_FAILURE ')) return null;
  let record;
  try { record = JSON.parse(lines[0].slice('OPS_ADMISSION_FAILURE '.length)); } catch { return null; }
  if (!record || typeof record !== 'object' || Array.isArray(record)) return null;
  const keys = Object.keys(record).sort();
  if (record.schema === 'ops-admission-failure.v1') {
    if (keys.length !== 3 || keys[0] !== 'code' || keys[1] !== 'schema' || keys[2] !== 'stage'
        || !OPS02_ADMISSION_FAILURE_STAGES.has(record.stage)
        || !OPS02_ADMISSION_FAILURE_CODES.has(record.code)) return null;
    return {
      stage: record.stage, code: record.code, transactionBoundary: 'none', sqlitePrimaryCode: null,
      transactionActive: null, rollbackDisposition: 'not_observed'
    };
  }
  if (record.schema !== 'ops-admission-failure.v2'
      || keys.length !== 7 || keys[0] !== 'code' || keys[1] !== 'rollbackDisposition'
      || keys[2] !== 'schema' || keys[3] !== 'sqlitePrimaryCode' || keys[4] !== 'stage'
      || keys[5] !== 'transactionActive' || keys[6] !== 'transactionBoundary'
      || record.stage !== 'admission' || record.code !== 'STORE_BUSY'
      || !OPS02_TRANSACTION_BOUNDARIES.has(record.transactionBoundary)
      || ![5, 6].includes(record.sqlitePrimaryCode)
      || !(record.transactionActive === null || typeof record.transactionActive === 'boolean')
      || !OPS02_ROLLBACK_DISPOSITIONS.has(record.rollbackDisposition)) return null;
  const validDisposition = record.transactionBoundary === 'begin'
    ? record.transactionActive === null && record.rollbackDisposition === 'not_observed'
    : ['body', 'commit'].includes(record.transactionBoundary)
      ? (record.rollbackDisposition === 'inactive' && record.transactionActive === false)
        || (record.rollbackDisposition === 'rolled_back' && record.transactionActive === true)
        || (record.rollbackDisposition === 'unknown' && record.transactionActive === null)
      : record.transactionBoundary === 'rollback'
        ? (record.rollbackDisposition === 'rollback_failed' && record.transactionActive === true)
          || (record.rollbackDisposition === 'rollback_check_failed' && record.transactionActive === null)
        : false;
  if (!validDisposition) return null;
  return {
    stage: record.stage, code: record.code,
    transactionBoundary: record.transactionBoundary, sqlitePrimaryCode: record.sqlitePrimaryCode,
    transactionActive: record.transactionActive, rollbackDisposition: record.rollbackDisposition
  };
}

function recordOps02RaceDiagnostic({
  boundary, raceKind = 'diagnostic', contender = null, passed,
  exit = 'not_applicable', childStage = 'none', childCode = 'none',
  transactionBoundary = 'none', sqlitePrimaryCode = null,
  transactionActive = null, rollbackDisposition = 'not_observed'
}, state = METRICS) {
  try {
    if (state === null || typeof state !== 'object' || Array.isArray(state)
        || state.ops02RaceDiagnosticsComplete !== true) return false;
    const prior = OPS02_RACE_PRODUCER_STATES.get(state);
    if (!prior && (state.ops02RaceDiagnosticCount !== 0
        || state.ops02RaceDiagnosticBytes !== 0 || state.ops02RaceDiagnosticLines !== '')) {
      markOps02RaceDiagnosticsIncomplete(state);
      return false;
    }
    const producerState = prior ?? { started: false, childExitGroups: new Set() };
    if ((!producerState.started && boundary !== 'case_started')
        || (boundary === 'case_started' && producerState.started)
        || (boundary === 'child_exit_group' && producerState.childExitGroups.has(raceKind))) {
      markOps02RaceDiagnosticsIncomplete(state);
      return false;
    }
    if (!OPS02_RACE_DIAGNOSTIC_BOUNDARIES.has(boundary)
        || !OPS02_RACE_DIAGNOSTIC_RACES.has(raceKind)
        || !(contender === null || contender === 0 || contender === 1)
        || typeof passed !== 'boolean'
        || !OPS02_RACE_DIAGNOSTIC_EXITS.has(exit)
        || !OPS02_CHILD_DIAGNOSTIC_STAGES.has(childStage)
        || !OPS02_CHILD_DIAGNOSTIC_CODES.has(childCode)
        || !OPS02_TRANSACTION_BOUNDARIES.has(transactionBoundary)
        || !OPS02_SQLITE_PRIMARY_CODES.has(sqlitePrimaryCode)
        || !(transactionActive === null || typeof transactionActive === 'boolean')
        || !OPS02_ROLLBACK_DISPOSITIONS.has(rollbackDisposition)
        || !Number.isSafeInteger(state.ops02RaceDiagnosticCount)
        || !Number.isSafeInteger(state.ops02RaceDiagnosticBytes)
        || typeof state.ops02RaceDiagnosticLines !== 'string') {
      markOps02RaceDiagnosticsIncomplete(state);
      return false;
    }
    const encoded = encodeOps02RaceDiagnostic({
      schema: 'agent-harness.ah11-ops02-race-diagnostic.v3', caseId: 'OPS02.C02',
      boundary, raceKind, contender, passed, exit, childStage, childCode,
      transactionBoundary, sqlitePrimaryCode, transactionActive, rollbackDisposition
    });
    if (typeof encoded !== 'string') {
      markOps02RaceDiagnosticsIncomplete(state);
      return false;
    }
    const line = `${encoded}\n`;
    const lineBytes = Buffer.byteLength(line, 'utf8');
    if (state.ops02RaceDiagnosticCount >= OPS02_RACE_DIAGNOSTIC_MAX_RECORDS
        || lineBytes > OPS02_RACE_DIAGNOSTIC_MAX_LINE_BYTES
        || state.ops02RaceDiagnosticBytes + lineBytes > OPS02_RACE_DIAGNOSTIC_MAX_BYTES) {
      markOps02RaceDiagnosticsIncomplete(state);
      return false;
    }
    state.ops02RaceDiagnosticLines += line;
    state.ops02RaceDiagnosticCount += 1;
    state.ops02RaceDiagnosticBytes += lineBytes;
    if (boundary === 'case_started') producerState.started = true;
    if (boundary === 'child_exit_group') producerState.childExitGroups.add(raceKind);
    if (prior === undefined) OPS02_RACE_PRODUCER_STATES.set(state, producerState);
    return true;
  } catch {
    markOps02RaceDiagnosticsIncomplete(state);
    return false;
  }
}

function recordOps02ContenderResults(raceKind, results, state = METRICS) {
  let allPassed = Array.isArray(results) && results.every(result => result.code === 0);
  if (!Array.isArray(results)) {
    markOps02RaceDiagnosticsIncomplete(state);
    return false;
  }
  results.forEach((result, contender) => {
    const childFailure = parseOps02AdmissionFailure(result?.stderr);
    const passed = result?.code === 0;
    const exit = passed ? 'zero'
      : result?.signal ? 'signal'
        : Number.isSafeInteger(result?.code) ? 'nonzero' : 'unknown';
    if (!passed) allPassed = false;
    recordOps02RaceDiagnostic({
      boundary: 'contender_exit', raceKind, contender, passed, exit,
      childStage: childFailure?.stage ?? (exit === 'zero' ? 'none' : 'unobserved'),
      childCode: childFailure?.code ?? (exit === 'zero' ? 'none' : 'unobserved'),
      transactionBoundary: childFailure?.transactionBoundary ?? 'none',
      sqlitePrimaryCode: childFailure?.sqlitePrimaryCode ?? null,
      transactionActive: childFailure?.transactionActive ?? null,
      rollbackDisposition: childFailure?.rollbackDisposition ?? 'not_observed'
    }, state);
  });
  recordOps02RaceDiagnostic({
    boundary: 'child_exit_group', raceKind, passed: allPassed
  }, state);
  return allPassed;
}

function classifyChildExitDisposition({ code, signal } = {}) {
  if (Number.isSafeInteger(code) && code >= 0) return code === 0 ? 'zero' : 'nonzero';
  if (code === null && typeof signal === 'string' && signal.length > 0) return 'signal';
  return 'unknown';
}

function recordOps06ClosePhaseDiagnostic(record, processCloseElapsedMs, restartProcessCloseElapsedMs, state = METRICS) {
  try {
    if (state.ops06ClosePhaseDiagnosticCount !== 0
        || !Number.isSafeInteger(state.ops06ClosePhaseDiagnosticCount)
        || !Number.isSafeInteger(state.ops06ClosePhaseDiagnosticBytes)
        || typeof state.ops06ClosePhaseDiagnosticLines !== 'string'
        || !record || typeof record !== 'object' || Array.isArray(record)) {
      state.ops06ClosePhaseDiagnosticsComplete = false;
      return false;
    }
    const phases = record.phaseElapsedMs;
    if (!phases || typeof phases !== 'object' || Array.isArray(phases)
        || !OPS06_CLOSE_PHASE_STAGES.has(record.stage)
        || !OPS06_CLOSE_PHASE_EXITS.has(record.exitDisposition)
        || !(record.exitCode === null || (Number.isSafeInteger(record.exitCode)
          && record.exitCode >= 0 && record.exitCode <= 255))
        || !(record.exitSignal === null || OPS06_CLOSE_PHASE_SIGNALS.has(record.exitSignal))
        || !['none', 'assertion', 'child_exit', 'unknown'].includes(record.failureClass)) {
      state.ops06ClosePhaseDiagnosticsComplete = false;
      return false;
    }
    const value = {
      schema: 'agent-harness.ah11-ops06-close-phases.v2', caseId: 'OPS06.C01',
      stage: record.stage, failureClass: record.failureClass,
      exitDisposition: record.exitDisposition, exitCode: record.exitCode, exitSignal: record.exitSignal,
      signalToFirstStopElapsedMs: phases.signalToFirstStopElapsedMs,
      restartWorkElapsedMs: phases.restartWorkElapsedMs,
      finalStopElapsedMs: phases.finalStopElapsedMs,
      readyToFinalRecordElapsedMs: phases.readyToFinalRecordElapsedMs,
      processCloseElapsedMs, restartOwnership: 'fresh_process', restartProcessCloseElapsedMs
    };
    if (Object.values(value).some(item => typeof item === 'number'
        && (!Number.isFinite(item) || item < 0 || item > 30_000))) {
      state.ops06ClosePhaseDiagnosticsComplete = false;
      return false;
    }
    for (const key of [
      'signalToFirstStopElapsedMs', 'restartWorkElapsedMs', 'finalStopElapsedMs',
      'readyToFinalRecordElapsedMs', 'processCloseElapsedMs', 'restartProcessCloseElapsedMs'
    ]) {
      if (!(value[key] === null || (typeof value[key] === 'number'
          && Number.isFinite(value[key]) && value[key] >= 0 && value[key] <= 30_000))) {
        state.ops06ClosePhaseDiagnosticsComplete = false;
        return false;
      }
    }
    if ((value.exitDisposition === 'zero' && (value.exitCode !== 0 || value.exitSignal !== null))
        || (value.exitDisposition === 'nonzero' && (!Number.isSafeInteger(value.exitCode)
          || value.exitCode === 0 || value.exitSignal !== null))
        || (value.exitDisposition === 'signal' && (value.exitCode !== null || value.exitSignal === null))
        || (value.exitDisposition === 'unknown' && (value.exitCode !== null || value.exitSignal !== null))) {
      state.ops06ClosePhaseDiagnosticsComplete = false;
      return false;
    }
    const line = `${OPS06_CLOSE_PHASE_DIAGNOSTIC_PREFIX}${JSON.stringify(value)}\n`;
    const lineBytes = Buffer.byteLength(line, 'utf8');
    if (lineBytes > OPS06_CLOSE_PHASE_DIAGNOSTIC_MAX_LINE_BYTES
        || state.ops06ClosePhaseDiagnosticBytes + lineBytes > OPS06_CLOSE_PHASE_DIAGNOSTIC_MAX_BYTES) {
      state.ops06ClosePhaseDiagnosticsComplete = false;
      return false;
    }
    state.ops06ClosePhaseDiagnosticLines += line;
    state.ops06ClosePhaseDiagnosticCount += 1;
    state.ops06ClosePhaseDiagnosticBytes += lineBytes;
    return true;
  } catch {
    try { state.ops06ClosePhaseDiagnosticsComplete = false; } catch {}
    return false;
  }
}

function recordOps12RecoveryDiagnostic(result, childFailure, state = METRICS) {
  try {
    if (state.ops12RecoveryDiagnosticCount !== 0
        || !Number.isSafeInteger(state.ops12RecoveryDiagnosticCount)
        || !Number.isSafeInteger(state.ops12RecoveryDiagnosticBytes)
        || typeof state.ops12RecoveryDiagnosticLines !== 'string') {
      state.ops12RecoveryDiagnosticsComplete = false;
      return false;
    }
    const value = buildOps12RecoveryDiagnosticRecord(result, childFailure);
    if (!value) {
      state.ops12RecoveryDiagnosticsComplete = false;
      return false;
    }
    const line = `${OPS12_RECOVERY_DIAGNOSTIC_PREFIX}${JSON.stringify(value)}\n`;
    const lineBytes = Buffer.byteLength(line, 'utf8');
    if (lineBytes > OPS12_RECOVERY_DIAGNOSTIC_MAX_LINE_BYTES
        || state.ops12RecoveryDiagnosticBytes + lineBytes > OPS12_RECOVERY_DIAGNOSTIC_MAX_BYTES) {
      state.ops12RecoveryDiagnosticsComplete = false;
      return false;
    }
    state.ops12RecoveryDiagnosticLines += line;
    state.ops12RecoveryDiagnosticCount += 1;
    state.ops12RecoveryDiagnosticBytes += lineBytes;
    return true;
  } catch {
    try { state.ops12RecoveryDiagnosticsComplete = false; } catch {}
    return false;
  }
}

function readC02ControlJson(path, boundary, readFile = readFileSync, state = METRICS) {
  try {
    return JSON.parse(readFile(path, 'utf8'));
  } catch (error) {
    recordC02ControlReadFailure(boundary, error, state);
    throw error;
  }
}

function publishC02ControlJsonAtomically(targetPath, value, fileOps = {}) {
  const acquire = fileOps.openSync ?? openSync;
  const write = fileOps.writeSync ?? writeSync;
  const close = fileOps.closeSync ?? closeSync;
  const rename = fileOps.renameSync ?? renameSync;
  const remove = fileOps.rmSync ?? rmSync;
  const uuid = fileOps.randomUUID ?? randomUUID;
  const data = Buffer.from(JSON.stringify(value), 'utf8');
  if (data.byteLength > 1_024) throw new RangeError('C02 control marker exceeds its byte bound.');
  const temporaryPath = `${targetPath}.${uuid()}.tmp`;
  let descriptor = null;
  let descriptorAcquired = false;
  let acquiredTemporary = false;
  let failure;
  let failed = false;
  try {
    descriptor = acquire(temporaryPath, 'wx', 0o600);
    acquiredTemporary = true;
    descriptorAcquired = true;
    if (!Number.isSafeInteger(descriptor) || descriptor < 0) {
      throw new TypeError('C02 control marker temp acquisition returned an invalid descriptor.');
    }
    let offset = 0;
    let writeCalls = 0;
    while (offset < data.byteLength) {
      if (writeCalls >= data.byteLength) throw new RangeError('C02 control marker write exceeded its progress bound.');
      const written = write(descriptor, data, offset, data.byteLength - offset, null);
      writeCalls += 1;
      if (!Number.isSafeInteger(written) || written <= 0 || written > data.byteLength - offset) {
        throw new RangeError('C02 control marker write returned an invalid progress count.');
      }
      offset += written;
    }
  } catch (error) {
    failure = error;
    failed = true;
  }
  if (descriptorAcquired) {
    try { close(descriptor); } catch (error) {
      if (!failed) {
        failure = error;
        failed = true;
      }
    }
  }
  if (failed) {
    if (acquiredTemporary) {
      try { remove(temporaryPath, { force: true }); } catch {
        try { fileOps.onCleanupFailure?.(); } catch {}
      }
    }
    throw failure;
  }
  try {
    rename(temporaryPath, targetPath);
    return true;
  } catch (primaryError) {
    if (acquiredTemporary) {
      try { remove(temporaryPath, { force: true }); } catch {
        try { fileOps.onCleanupFailure?.(); } catch {}
      }
    }
    throw primaryError;
  }
}

function attemptC02WatchdogTargetAck({ groupId, watchAckPath, pid, state, probe, publish }) {
  if (state.targetAckSent || state.targetAckPublicationFailed) return false;
  try { probe(-groupId, 0); } catch { return false; }
  try {
    publish(watchAckPath, {
      schema: 'ops-owned-group-watchdog-target-ack.v1', pid, groupId, verified: true
    });
    state.targetAckSent = true;
    return true;
  } catch {
    state.targetAckPublicationFailed = true;
    return false;
  }
}

function ci1CaptureCanAppend(recordCount, currentBytes, nextBytes) {
  return recordCount < CI1_MAX_PHASE_RECORDS && currentBytes + nextBytes <= CI1_MAX_PHASE_BYTES;
}

function appendBoundedCi1PhaseCapture(state, record) {
  const encoded = JSON.stringify(record);
  const bytes = Buffer.byteLength(encoded);
  if (!ci1CaptureCanAppend(state.ci1PhaseCaptures.length, state.ci1PhaseCaptureBytes, bytes)) {
    state.ci1PhaseCaptureOverflow = true;
    return false;
  }
  state.ci1PhaseCaptures.push(record);
  state.ci1PhaseCaptureBytes += bytes;
  return true;
}

function appendCi1PhaseCapture(record, state = METRICS) {
  return appendBoundedCi1PhaseCapture(state, record);
}

function parseCi1ChildPhaseCaptureBytes(bytes, phase, role, state = METRICS) {
  const stages = [];
  let failureCode = null;
  let aggregateRejected = false;
  let stopResultsComplete = true;
  const setFailure = code => { if (!failureCode) failureCode = code; };
  if (!Buffer.isBuffer(bytes)) {
    setFailure('unreadable');
    bytes = Buffer.alloc(0);
  }
  if (bytes.length === 0) setFailure('empty');
  if (bytes.length > 4_096) {
    setFailure('child_byte_limit');
    state.ci1PhaseCaptureOverflow = true;
  }
  const boundedBytes = bytes.subarray(0, 4_096);
  const text = boundedBytes.toString('utf8');
  const endsWithNewline = text.endsWith('\n');
  if (!endsWithNewline) setFailure('truncated');
  const allLines = text.split('\n');
  allLines.pop(); // The last split item is either the empty terminator or an untrusted partial row.
  if (allLines.length === 0) setFailure('empty');
  if (allLines.length > 16) {
    setFailure('child_record_limit');
    state.ci1PhaseCaptureOverflow = true;
  }
  const lines = allLines.slice(0, 16);
  let priorElapsedMs = 0;
  for (let index = 0; index < lines.length; index += 1) {
    let item;
    try { item = JSON.parse(lines[index]); } catch {
      setFailure('malformed');
      break;
    }
    if (!item || typeof item !== 'object' || Array.isArray(item)) {
      setFailure('shape');
      break;
    }
    const hasFailureDetails = item.stage === 'fixture_failure';
    const hasStopResult = item.stage === 'stop_settled' || item.stage === 'restart_stop_settled';
    const failureDetails = hasFailureDetails
      ? parseOps04FailureDetails(Object.fromEntries(Object.entries(item)
        .filter(([key]) => !['elapsedMs', 'schema', 'sequence', 'stage'].includes(key))))
      : null;
    if (hasFailureDetails && !failureDetails) {
      setFailure('failure_shape');
      break;
    }
    const keys = Object.keys(item).sort();
    const expectedKeys = hasFailureDetails
      ? ['elapsedMs', 'schema', 'sequence', 'stage', ...Object.keys(failureDetails)].sort()
      : hasStopResult
      ? ['childrenReaped', 'elapsedMs', 'schema', 'sequence', 'stage', 'stopped', 'storeClosed']
      : ['elapsedMs', 'schema', 'sequence', 'stage'];
    if (keys.length !== expectedKeys.length || keys.some((key, keyIndex) => key !== expectedKeys[keyIndex])) {
      setFailure('shape');
      break;
    }
    if (item.schema !== 1 || item.sequence !== index + 1) {
      setFailure('sequence');
      break;
    }
    if (typeof item.stage !== 'string' || !CI1_CHILD_PHASE_STAGES.has(item.stage)) {
      setFailure('stage');
      break;
    }
    if (!Number.isSafeInteger(item.elapsedMs) || item.elapsedMs < 0 || item.elapsedMs < priorElapsedMs) {
      setFailure('clock');
      break;
    }
    if (hasStopResult && !['stopped', 'childrenReaped', 'storeClosed'].every(key => typeof item[key] === 'boolean')) {
      setFailure('stop_result');
      break;
    }
    if (hasFailureDetails && stages.includes('fixture_failure')) {
      failureCode = 'duplicate_failure';
      break;
    }
    priorElapsedMs = item.elapsedMs;
    stages.push(item.stage);
    const retained = {
      schema: 1, source: 'child', phase, role,
      sequence: item.sequence, stage: item.stage, elapsedMs: item.elapsedMs
    };
    if (hasStopResult) {
      retained.stopped = item.stopped;
      retained.childrenReaped = item.childrenReaped;
      retained.storeClosed = item.storeClosed;
      if (!item.stopped || !item.childrenReaped || !item.storeClosed) {
        stopResultsComplete = false;
        setFailure('stop_result');
      }
    }
    if (hasFailureDetails) {
      setFailure('fixture_failure');
      Object.assign(retained, failureDetails);
    }
    if (!aggregateRejected && !appendCi1PhaseCapture(retained, state)) {
      aggregateRejected = true;
      setFailure('aggregate_limit');
    }
  }
  const required = role === 'shutdown'
    ? ['fixture_started', 'store_opened', 'workflow_opened', 'worker_started', 'runner_claimed', 'marker_written',
      'signal_seen', 'stop_requested', 'stop_settled', 'loop_settled', 'shutdown_verified',
      'store_close_started', 'store_closed', 'fixture_finally_started', 'fixture_finally_complete']
    : ['fixture_started', 'restart_worker_started', 'restart_work_completed', 'restart_stop_settled',
      'restart_store_closed', 'restart_loop_settled', 'fixture_finally_started', 'fixture_finally_complete'];
  const hasRequiredStages = required.every(stage => stages.includes(stage));
  if (!hasRequiredStages) setFailure('required_stages');
  if (role === 'shutdown') {
    const ordered = [
      'fixture_started', 'store_opened', 'worker_started', 'runner_claimed', 'workflow_opened',
      'marker_written', 'signal_seen', 'stop_requested', 'stop_settled', 'loop_settled',
      'shutdown_verified', 'store_close_started', 'store_closed', 'fixture_finally_started', 'fixture_finally_complete'
    ];
    if (!ordered.every((stage, index) => index === 0 || stages.indexOf(ordered[index - 1]) < stages.indexOf(stage))) {
      setFailure('order');
    }
  }
  if (role === 'restart') {
    const ordered = ['fixture_started', 'restart_worker_started', 'restart_work_completed',
      'restart_stop_settled', 'restart_store_closed', 'restart_loop_settled',
      'fixture_finally_started', 'fixture_finally_complete'];
    if (!ordered.every((stage, index) => index === 0 || stages.indexOf(ordered[index - 1]) < stages.indexOf(stage))) {
      setFailure('order');
    }
  }
  const complete = failureCode === null && hasRequiredStages && stopResultsComplete;
  if (!complete) state.ci1PhaseCaptureIncomplete = true;
  return { stages, complete, failureCode };
}

function readCi1ChildPhaseCapture(path, phase, role, requireComplete = true, state = METRICS) {
  let fd;
  let bytes;
  try {
    fd = openSync(path, 'r');
    const buffer = Buffer.alloc(4_097);
    let length = 0;
    while (length < buffer.length) {
      const count = readSync(fd, buffer, length, buffer.length - length, length);
      if (count === 0) break;
      length += count;
    }
    bytes = buffer.subarray(0, length);
  } catch (error) {
    const failureCode = error?.code === 'ENOENT' ? 'missing' : 'unreadable';
    state.ci1PhaseCaptureIncomplete = true;
    return { stages: [], complete: false, failureCode };
  } finally {
    if (fd !== undefined) {
      try { closeSync(fd); } catch { state.ci1PhaseCaptureIncomplete = true; }
    }
  }
  const result = parseCi1ChildPhaseCaptureBytes(bytes, phase, role, state);
  if (requireComplete && !result.complete) state.ci1PhaseCaptureIncomplete = true;
  return result;
}

async function waitCi1ChildClose({ timeoutMs, escalationStage, waitForClose, reapOwnedChild, captureParent, state = METRICS }) {
  let closeResult = await waitForClose(timeoutMs);
  if (closeResult) return { closeResult, timedOut: false };
  state.ci1PhaseCaptureIncomplete = true;
  try {
    if (captureParent(escalationStage) === false) state.ci1PhaseCaptureIncomplete = true;
  } catch {
    state.ci1PhaseCaptureIncomplete = true;
  }
  try { closeResult = await reapOwnedChild(); } catch { closeResult = null; }
  return { closeResult, timedOut: true };
}

async function finalizeCi1ChildCapture({
  phase, role, closeResult, closeOwnedChild, cleanupOwnedChild,
  readCapture, captureParent, state = METRICS
}) {
  const safeParentCapture = (stage, details) => {
    try {
      if (captureParent(stage, details) === false) state.ci1PhaseCaptureIncomplete = true;
    } catch {
      state.ci1PhaseCaptureIncomplete = true;
    }
  };
  let finalCloseResult = closeResult ?? null;
  let captureResult = null;
  let ownedGroupEmpty = true;
  if (cleanupOwnedChild) {
    let cleanupResult;
    try { cleanupResult = await cleanupOwnedChild(); } catch { cleanupResult = null; }
    if (cleanupResult && typeof cleanupResult === 'object') {
      if (!finalCloseResult && cleanupResult.closeResult) finalCloseResult = cleanupResult.closeResult;
      ownedGroupEmpty = cleanupResult.groupEmpty === true;
    } else {
      ownedGroupEmpty = false;
    }
  } else if (!finalCloseResult) {
    safeParentCapture('cleanup_escalation_sent');
    try { finalCloseResult = await closeOwnedChild(); } catch { finalCloseResult = null; }
  }
  if (!ownedGroupEmpty) {
    state.ci1PhaseCaptureIncomplete = true;
    state.ci1PhaseCaptureOwnedGroupCloseUnconfirmed = true;
    safeParentCapture('owned_group_close_unconfirmed');
  }
  if (!finalCloseResult || !ownedGroupEmpty) {
    state.ci1PhaseCaptureIncomplete = true;
    if (!finalCloseResult) {
      state.ci1PhaseCaptureChildCloseUnconfirmed = true;
      safeParentCapture('child_close_unconfirmed');
    }
    try { captureResult = readCapture(); } catch { state.ci1PhaseCaptureIncomplete = true; }
    if (captureResult?.complete) {
      captureResult = {
        ...captureResult,
        complete: false,
        failureCode: ownedGroupEmpty ? 'close_unconfirmed' : 'owned_group_close_unconfirmed'
      };
    }
    if (!captureResult || !captureResult.complete) state.ci1PhaseCaptureIncomplete = true;
    return { closeResult: finalCloseResult, captureResult, reaped: false, ownedGroupEmpty };
  }
  safeParentCapture(`${role}_child_closed`, {
    exitDisposition: classifyOps04ExitDisposition(finalCloseResult)
  });
  safeParentCapture('owned_group_empty');
  try { captureResult = readCapture(); } catch { state.ci1PhaseCaptureIncomplete = true; }
  if (!captureResult || !captureResult.complete) {
    state.ci1PhaseCaptureIncomplete = true;
    const failureCode = captureResult?.failureCode;
    const stage = ['missing', 'unreadable', 'empty'].includes(failureCode)
      ? 'child_capture_missing'
      : ['child_byte_limit', 'child_record_limit', 'aggregate_limit'].includes(failureCode)
        ? 'child_capture_overflow'
        : ['required_stages', 'fixture_failure'].includes(failureCode)
          ? 'child_capture_partial'
          : 'child_capture_invalid';
    safeParentCapture(stage);
  }
  return { closeResult: finalCloseResult, captureResult, reaped: true, ownedGroupEmpty };
}

async function executeCi1ChildWithFinalization(execute, finalize, state = METRICS) {
  let execution;
  let primaryFailure;
  try { execution = await execute(); } catch (error) {
    primaryFailure = error;
    state.ci1PhaseCaptureIncomplete = true;
  }
  let finalization;
  try { finalization = await finalize(); } catch {
    state.ci1PhaseCaptureIncomplete = true;
    if (primaryFailure) throw primaryFailure;
    throw new Error('Owned CI1 child finalization did not complete.');
  }
  if (primaryFailure) throw primaryFailure;
  return { execution, finalization };
}

function ci1TemporaryCustody(directory) {
  return { directory, launches: new Map(), sidecarPath: null };
}

function ci1TemporaryCustodyPath(custody) {
  const registryPath = process.env.OPS_CI1_OWNED_GROUP_REGISTRY;
  if (typeof registryPath !== 'string' || registryPath.length === 0 || !custody) return null;
  const directoryTag = createHash('sha256').update(custody.directory).digest('hex').slice(0, 16);
  return `${registryPath}.temp-custody.${process.pid}.${directoryTag}.json`;
}

function publishCi1JsonAtomically(targetPath, temporaryPath, bytes, fileOps = {}) {
  const acquire = fileOps.openSync ?? openSync;
  const write = fileOps.writeSync ?? writeSync;
  const close = fileOps.closeSync ?? closeSync;
  const rename = fileOps.renameSync ?? renameSync;
  const remove = fileOps.rmSync ?? rmSync;
  let descriptor = null;
  let descriptorAcquired = false;
  let temporaryAcquired = false;
  let failed = false;
  try {
    descriptor = acquire(temporaryPath, 'wx', 0o600);
    temporaryAcquired = true;
    if (!Number.isSafeInteger(descriptor) || descriptor < 0) {
      throw new TypeError('CI1 custody marker acquisition returned an invalid descriptor.');
    }
    descriptorAcquired = true;
    let offset = 0;
    let writeCalls = 0;
    while (offset < bytes.byteLength) {
      if (writeCalls >= bytes.byteLength) throw new RangeError('CI1 custody marker write exceeded its progress bound.');
      const written = write(descriptor, bytes, offset, bytes.byteLength - offset, null);
      writeCalls += 1;
      if (!Number.isSafeInteger(written) || written <= 0 || written > bytes.byteLength - offset) {
        throw new RangeError('CI1 custody marker write returned invalid progress.');
      }
      offset += written;
    }
  } catch {
    failed = true;
  }
  if (descriptorAcquired) {
    try { close(descriptor); } catch { failed = true; }
  }
  if (failed) {
    if (temporaryAcquired) {
      try { remove(temporaryPath, { force: true }); } catch {}
    }
    return false;
  }
  try {
    rename(temporaryPath, targetPath);
    temporaryAcquired = false;
    return true;
  } catch {
    if (temporaryAcquired) {
      try { remove(temporaryPath, { force: true }); } catch {}
    }
    return false;
  }
}

function writeCi1TemporaryCustody(custody, fileOps = {}) {
  if (!custody) return true;
  const sidecarPath = custody.sidecarPath ?? ci1TemporaryCustodyPath(custody);
  if (!sidecarPath) return false;
  custody.sidecarPath = sidecarPath;
  const launches = [...custody.launches.values()].map(launch => ({
    launchToken: launch.launchToken,
    status: launch.status,
    spawnAttempted: launch.spawnAttempted === true,
    directPid: Number.isSafeInteger(launch.directPid) ? launch.directPid : null,
    groupId: Number.isSafeInteger(launch.groupId) ? launch.groupId : null,
    registryRegistered: launch.registryRegistered === true,
    registryCleared: launch.registryCleared === true,
    groupAbsent: launch.groupAbsent === true
  })).sort((left, right) => left.launchToken.localeCompare(right.launchToken));
  if (launches.length > CI1_TEMP_CUSTODY_MAX_LAUNCHES) return false;
  if (launches.length === 0) {
    try { rmSync(sidecarPath, { force: true }); return true; } catch { return false; }
  }
  const bytes = Buffer.from(`${JSON.stringify({
    schema: CI1_TEMP_CUSTODY_SCHEMA,
    ownerPid: process.pid,
    directory: custody.directory,
    launches
  })}\n`, 'utf8');
  if (bytes.byteLength > CI1_TEMP_CUSTODY_MAX_BYTES) return false;
  const makeUuid = fileOps.randomUUID ?? randomUUID;
  const temporaryPath = `${sidecarPath}.${makeUuid()}.tmp`;
  return publishCi1JsonAtomically(sidecarPath, temporaryPath, bytes, fileOps);
}

function markCi1OwnedGroupSpawnAttempt(custody, launchToken, persist = writeCi1TemporaryCustody) {
  const launch = custody?.launches.get(launchToken);
  if (!launch) return false;
  const previousStatus = launch.status;
  launch.spawnAttempted = true;
  launch.status = 'spawn_attempted';
  let written = false;
  try { written = persist(custody) === true; } catch {}
  if (written) return true;
  launch.spawnAttempted = false;
  launch.status = previousStatus;
  return false;
}

function rememberCi1OwnedGroupChild(custody, launchToken, child, closed) {
  const launch = custody?.launches.get(launchToken);
  if (!launch || !Number.isSafeInteger(child?.pid) || child.pid < 2) return false;
  launch.directPid = child.pid;
  launch.child = child;
  launch.closed = closed;
  launch.status = 'child_spawned';
  return writeCi1TemporaryCustody(custody);
}

function finishCi1OwnedGroupAfterAbsence(groupId, launchToken, custody = null) {
  if (!updateCi1OwnedGroupRegistry(groupId, false, launchToken)) return false;
  const launch = custody?.launches.get(launchToken);
  if (!launch) return true;
  launch.groupAbsent = true;
  launch.registryCleared = true;
  launch.status = 'group_absent';
  return writeCi1TemporaryCustody(custody);
}

function removeCi1TemporarySidecar(custody) {
  if (!custody?.sidecarPath) return true;
  try { rmSync(custody.sidecarPath, { force: true }); return true; } catch { return false; }
}

async function cleanupCi1OwnedTemporaryDirectory(temporary, overrides = {}) {
  const custody = temporary?.ci1Custody;
  const state = overrides.state ?? METRICS;
  if (!custody) {
    try { rmSync(temporary.directory, { recursive: true, force: true }); return true; }
    catch { state.ci1PhaseCaptureIncomplete = true; return false; }
  }
  const probeGroupEmpty = overrides.probeGroupEmpty
    ?? (groupId => waitForOwnedProcessGroupEmpty(groupId, CI1_TEMP_CUSTODY_GROUP_PROBE_MS));
  const removeDirectory = overrides.removeDirectory
    ?? (directory => rmSync(directory, { recursive: true, force: true }));
  const removeRegistryLaunch = overrides.removeRegistryLaunch
    ?? ((groupId, launchToken) => updateCi1OwnedGroupRegistry(groupId, false, launchToken));
  const cancelPendingLaunch = overrides.cancelPendingLaunch ?? cancelCi1OwnedGroupLaunch;
  const persist = overrides.persist ?? writeCi1TemporaryCustody;
  let incomplete = false;
  for (const launch of [...custody.launches.values()]) {
    if (launch.groupId === null && launch.spawnAttempted !== true) {
      let cancelled = launch.registryCleared === true;
      if (!cancelled) {
        try { cancelled = cancelPendingLaunch(launch.launchToken); } catch {}
      }
      if (!cancelled) {
        launch.status = 'pending_removal_unconfirmed';
        incomplete = true;
        continue;
      }
      launch.registryCleared = true;
      launch.status = 'launch_cancelled';
      custody.launches.delete(launch.launchToken);
      if (!persist(custody)) {
        launch.status = 'cancelled_custody_removal_unconfirmed';
        custody.launches.set(launch.launchToken, launch);
        incomplete = true;
      }
      continue;
    }
    if (!Number.isSafeInteger(launch.groupId) || launch.groupId < 2) {
      let verifiedGroupId = null;
      if (launch.child && launch.directPid === launch.child.pid) {
        try { verifiedGroupId = await verifyOwnedProcessGroup(launch.child, CI1_TEMP_CUSTODY_GROUP_PROBE_MS); } catch {}
      }
      if (verifiedGroupId === launch.directPid) {
        launch.groupId = verifiedGroupId;
        launch.status = 'group_verified_during_cleanup';
        if (!persist(custody)) {
          incomplete = true;
          continue;
        }
      } else {
        launch.status = 'group_unverified';
        incomplete = true;
        continue;
      }
    }
    let groupEmpty = false;
    try { groupEmpty = await probeGroupEmpty(launch.groupId); } catch { groupEmpty = false; }
    if (!groupEmpty) {
      launch.status = 'group_unconfirmed';
      incomplete = true;
      continue;
    }
    launch.groupAbsent = true;
    if (!launch.registryCleared) {
      let removed = false;
      try { removed = removeRegistryLaunch(launch.groupId, launch.launchToken); } catch {}
      if (!removed) {
        launch.status = 'registry_removal_unconfirmed';
        incomplete = true;
        continue;
      }
      launch.registryCleared = true;
    }
    launch.status = 'group_absent';
    if (!persist(custody)) incomplete = true;
    else custody.launches.delete(launch.launchToken);
  }
  if (custody.launches.size > 0) {
    state.ci1PhaseCaptureIncomplete = true;
    state.ci1PhaseCaptureOwnedGroupCloseUnconfirmed = true;
    try { persist(custody); } catch {}
    return false;
  }
  if (incomplete) state.ci1PhaseCaptureIncomplete = true;
  try { removeDirectory(temporary.directory); }
  catch {
    state.ci1PhaseCaptureIncomplete = true;
    state.ci1PhaseCaptureOwnedGroupCloseUnconfirmed = true;
    return false;
  }
  if (!removeCi1TemporarySidecar(custody)) {
    state.ci1PhaseCaptureIncomplete = true;
    return false;
  }
  return !incomplete;
}

function recordClosedFixture(input) {
  try { return appendClosedDiagnostic(METRICS.closedFixtureDiagnostics, input); }
  catch { METRICS.closedFixtureDiagnostics.diagnosticsComplete = false; return false; }
}

function recordClosedChild(stderr, caseId, slot, phase, exit, resultPresent) {
  const child = parseClosedChildDiagnostic(stderr, caseId);
  recordClosedFixture({ ...(child ?? { caseId, phase, nativeClose: 'unobserved', reason: 'phase_failure' }),
    caseId, slot, phase, childPhase: child?.phase ?? 'unobserved', sequence: null,
    ...closedChildObservation(exit, exit?.outputPresent === true, resultPresent) });
  return child;
}

function registerCi1OwnedTemporaryDirectoryFinalizer(testContext, cleanup, temporary, state) {
  testContext.after(async () => {
    let cleanupConfirmed = false;
    try { cleanupConfirmed = await cleanup(temporary) === true; } catch {}
    if (!cleanupConfirmed) {
      state.ci1PhaseCaptureIncomplete = true;
      state.ci1PhaseCaptureOwnedGroupCloseUnconfirmed = true;
      try { process.stderr.write('AH11_OPS_CI1_TEMP_CUSTODY_CLEANUP_UNCONFIRMED\n'); } catch {}
      throw new Error('AH11_OPS_CI1_TEMP_CUSTODY_CLEANUP_UNCONFIRMED');
    }
  });
}

function tempDirectory(prefix = 'ah11-ops-', options = {}) {
  const directory = mkdtempSync(join(tmpdir(), prefix));
  const custody = options.ci1OwnedGroups === true ? ci1TemporaryCustody(directory) : null;
  return {
    directory,
    databasePath: join(directory, 'workflow.sqlite'),
    ci1Custody: custody,
    cleanup() { rmSync(directory, { recursive: true, force: true }); }
  };
}

function openStore(databasePath, options = {}) {
  const store = DurableWorkflowStore.open({ databasePath, busyTimeoutMs: 20, ...options });
  const config = readOperationsConfig();
  store._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY, {
    maxStoreBytes: config.maxStoreBytes,
    maxWalBytes: config.maxWalBytes,
    minFreeDiskBytes: config.minFreeDiskBytes
  });
  return store;
}

function captureOperationsFinalState(databasePath, jobIds) {
  let raw;
  try {
    if (!Array.isArray(jobIds) || jobIds.length !== 2 || jobIds.some(id => typeof id !== 'string')) {
      return { available: false };
    }
    raw = new DatabaseSync(databasePath, { readOnly: true });
    const jobs = [];
    for (const jobId of jobIds) {
      const work = raw.prepare(`SELECT job_id, workflow_id, state, generation, fence_token
        FROM operations_work WHERE job_id=?`).get(jobId);
      if (!work) return { available: false };
      const workflow = raw.prepare(`SELECT state, revision, checkpoint_sequence, fence_token,
        lease_owner, lease_expires_ms, cancellation_requested
        FROM workflows WHERE workflow_id=?`).get(work.workflow_id);
      const checkpoint = raw.prepare(`SELECT payload_json FROM workflow_checkpoints
        WHERE workflow_id=? ORDER BY sequence DESC LIMIT 1`).get(work.workflow_id);
      if (!workflow || !checkpoint || typeof checkpoint.payload_json !== 'string') return { available: false };
      const payload = JSON.parse(checkpoint.payload_json);
      const resultsJson = payload?.data?.completedResultsJson;
      if (typeof resultsJson !== 'string') return { available: false };
      const results = JSON.parse(resultsJson);
      if (!results || typeof results !== 'object' || Array.isArray(results)) return { available: false };
      const eventCounts = raw.prepare(`SELECT event_type AS type, COUNT(*) AS count
        FROM workflow_events WHERE workflow_id=? GROUP BY event_type ORDER BY event_type`)
        .all(work.workflow_id).map(row => ({ type: row.type, count: Number(row.count) }));
      jobs.push({
        workState: work.state,
        workflowState: workflow.state,
        generation: Number(work.generation),
        operationFence: work.fence_token === null ? null : Number(work.fence_token),
        workflowFence: Number(workflow.fence_token),
        revision: Number(workflow.revision),
        checkpointSequence: Number(workflow.checkpoint_sequence),
        resultCount: Object.keys(results).length,
        resultHash: createHash('sha256').update(resultsJson, 'utf8').digest('hex'),
        cancellationRequested: Number(workflow.cancellation_requested) === 1,
        leaseOwnerPresent: workflow.lease_owner !== null,
        leaseFenceMatches: work.fence_token !== null && Number(work.fence_token) === Number(workflow.fence_token),
        leaseExpired: workflow.lease_expires_ms === null || Number(workflow.lease_expires_ms) <= Date.now(),
        eventCounts
      });
    }
    const meta = raw.prepare(`SELECT reserved_disk_bytes AS reservedDiskBytes,
      reserved_event_slots AS reservedEventSlots, pending_records AS pendingRecords
      FROM operations_meta WHERE singleton=1`).get();
    if (!meta) return { available: false };
    return {
      available: true,
      jobs,
      reservations: {
        reservedDiskBytes: Number(meta.reservedDiskBytes),
        reservedEventSlots: Number(meta.reservedEventSlots),
        pendingRecords: Number(meta.pendingRecords)
      }
    };
  } catch {
    return { available: false };
  } finally {
    try { raw?.close(); } catch {}
  }
}

function admit(store, tenantId, projectId, idempotencyKey, work = PLAN, configInput = undefined) {
  const config = readOperationsConfig(configInput);
  const validated = validateLocalPlanWork({ planJson: work, limitsJson: '{}' }, 'ops-validation-workflow', config);
  return store._operationsAdmit(OPERATIONS_STORAGE_CAPABILITY, {
    tenantId,
    projectId,
    idempotencyKey,
    workJson: validated.workJson,
    policy: operationsStoragePolicy(config)
  });
}

function recoverySnapshot(databasePath, jobId, nextJobId, workflowId, attemptId) {
  const raw = new DatabaseSync(databasePath, { readOnly: true });
  try {
    const work = raw.prepare(`SELECT tenant_id, project_id, state, worker_id, runner_pid,
      generation, fence_token, lease_expires_ms, work_bytes,
      reserved_event_slots, reserved_disk_bytes, reason_code
      FROM operations_work WHERE job_id=?`).get(jobId);
    const nextWork = raw.prepare(`SELECT state, worker_id, generation, fence_token, work_bytes,
      reserved_event_slots, reserved_disk_bytes
      FROM operations_work WHERE job_id=?`).get(nextJobId);
    const workflow = raw.prepare(`SELECT state, revision, fence_token, cancellation_requested,
      lease_owner, lease_expires_ms FROM workflows WHERE workflow_id=?`).get(workflowId);
    const meta = raw.prepare(`SELECT pending_records, pending_bytes, in_flight,
      reserved_event_slots, reserved_disk_bytes FROM operations_meta WHERE singleton=1`).get();
    const tenant = raw.prepare(`SELECT pending_records, in_flight FROM operations_tenants WHERE tenant_id=?`)
      .get(work.tenant_id);
    const nextTenant = raw.prepare(`SELECT pending_records, in_flight FROM operations_tenants WHERE tenant_id=?`)
      .get('public-next-tenant');
    const project = raw.prepare(`SELECT pending_records, in_flight FROM operations_projects
      WHERE tenant_id=? AND project_id=?`).get(work.tenant_id, work.project_id);
    const attempt = attemptId === null ? null : raw.prepare(`SELECT state, authority_hold_ref
      FROM operation_attempts WHERE attempt_id=?`).get(attemptId) ?? null;
    const events = Object.fromEntries(raw.prepare(`SELECT event_type, COUNT(*) AS count
      FROM workflow_events WHERE workflow_id=? GROUP BY event_type ORDER BY event_type`)
      .all(workflowId).map(row => [row.event_type, Number(row.count)]));
    const allWork = raw.prepare(`SELECT job_id, tenant_id, project_id, state, worker_id, runner_pid,
      generation, fence_token, lease_expires_ms, work_bytes, reserved_event_slots, reserved_disk_bytes
      FROM operations_work ORDER BY job_id`).all().map(row => ({ ...row,
      generation: Number(row.generation), fence_token: row.fence_token === null ? null : Number(row.fence_token),
      lease_expires_ms: row.lease_expires_ms === null ? null : Number(row.lease_expires_ms),
      work_bytes: Number(row.work_bytes), reserved_event_slots: Number(row.reserved_event_slots),
      reserved_disk_bytes: Number(row.reserved_disk_bytes) }));
    const allTenants = raw.prepare(`SELECT tenant_id, pending_records, pending_bytes, in_flight
      FROM operations_tenants ORDER BY tenant_id`).all().map(row => ({ ...row,
      pending_records: Number(row.pending_records), pending_bytes: Number(row.pending_bytes),
      in_flight: Number(row.in_flight) }));
    const allProjects = raw.prepare(`SELECT tenant_id, project_id, pending_records, pending_bytes, in_flight
      FROM operations_projects ORDER BY tenant_id, project_id`).all().map(row => ({ ...row,
      pending_records: Number(row.pending_records), pending_bytes: Number(row.pending_bytes),
      in_flight: Number(row.in_flight) }));
    const reservationTotals = raw.prepare(`SELECT
      (SELECT SUM(reserved_disk_bytes) FROM operations_work) AS work_disk,
      (SELECT SUM(reserved_event_slots) FROM operations_work) AS work_events`).get();
    return {
      work: { ...work, generation: Number(work.generation), fence_token: Number(work.fence_token),
        lease_expires_ms: work.lease_expires_ms === null ? null : Number(work.lease_expires_ms),
        work_bytes: Number(work.work_bytes),
        reserved_event_slots: Number(work.reserved_event_slots), reserved_disk_bytes: Number(work.reserved_disk_bytes) },
      nextWork: { ...nextWork, generation: Number(nextWork.generation), fence_token: Number(nextWork.fence_token),
        work_bytes: Number(nextWork.work_bytes),
        reserved_event_slots: Number(nextWork.reserved_event_slots), reserved_disk_bytes: Number(nextWork.reserved_disk_bytes) },
      workflow: { ...workflow, revision: Number(workflow.revision), fence_token: Number(workflow.fence_token),
        lease_expires_ms: workflow.lease_expires_ms === null ? null : Number(workflow.lease_expires_ms) },
      meta: { ...meta, pending_records: Number(meta.pending_records), pending_bytes: Number(meta.pending_bytes),
        in_flight: Number(meta.in_flight), reserved_event_slots: Number(meta.reserved_event_slots),
        reserved_disk_bytes: Number(meta.reserved_disk_bytes) },
      tenant: { ...tenant, pending_records: Number(tenant.pending_records), in_flight: Number(tenant.in_flight) },
      nextTenant: { ...nextTenant, pending_records: Number(nextTenant.pending_records), in_flight: Number(nextTenant.in_flight) },
      project: { ...project, pending_records: Number(project.pending_records), in_flight: Number(project.in_flight) },
      attempt: attempt ? { ...attempt } : null,
      events,
      allWork,
      allTenants,
      allProjects,
      reservationTotals: { work_disk: Number(reservationTotals.work_disk), work_events: Number(reservationTotals.work_events) }
    };
  } finally {
    raw.close();
  }
}

async function assertUnknownRecoveryFailsClosedAtResourceBoundary() {
  const directory = tempDirectory('ah11-unknown-recovery-resource-floor-');
  const configInput = {
    workerCount: 1, maxInFlightGlobal: 1, maxInFlightTenant: 1, maxInFlightProject: 1
  };
  const config = readOperationsConfig(configInput);
  const policy = operationsStoragePolicy(config);
  let now = 120_000;
  let store = openStore(directory.databasePath, { clock: () => now });
  try {
    const expired = admit(store, 'public-expired-tenant', 'public-expired-project',
      'resource-floor-expired', PLAN, configInput);
    const next = admit(store, 'public-next-tenant', 'public-next-project',
      'resource-floor-next', PLAN, configInput);
    const later = admit(store, 'public-next-tenant', 'public-later-project',
      'resource-floor-later', PLAN, configInput);
    const prior = store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'resource-floor-prior-owner', policy
    }).claim;
    assert.equal(prior.jobId, expired.jobId);
    store.close();
    store = null;
    const attemptId = 'resource-floor-attempt';
    const holdRef = 'resource-floor-synthetic-hold';
    crashAfterPublicWorkflowMutation(directory.databasePath, {
      mutation: 'unknown', now, workflowId: prior.workflowId, ownerId: prior.ownerId,
      fence: prior.fence, revision: prior.revision, attemptId, holdRef
    });
    now = prior.leaseExpiresAtMs + 1;
    const beforeFailure = recoverySnapshot(directory.databasePath, expired.jobId, next.jobId,
      expired.workflowId, attemptId);
    assert.equal(beforeFailure.meta.reserved_disk_bytes, beforeFailure.reservationTotals.work_disk);
    assert.ok(beforeFailure.meta.reserved_disk_bytes > 16_777_216,
      'three accepted jobs reserve more than the minimum permitted store ceiling');
    assert.equal(beforeFailure.allWork.find(row => row.job_id === later.jobId).state, 'queued');

    let restricted = openStore(directory.databasePath, { clock: () => now });
    try {
      restricted._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY, {
        maxStoreBytes: 16_777_216,
        maxWalBytes: config.maxWalBytes,
        minFreeDiskBytes: config.minFreeDiskBytes
      });
      assert.throws(() => restricted._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
        ownerId: 'resource-floor-failing-recovery', policy, recoverableJobIds: [expired.jobId]
      }), error => error.code === 'STORE_WRITE_FAILED');
      assert.deepEqual(recoverySnapshot(directory.databasePath, expired.jobId, next.jobId,
        expired.workflowId, attemptId), beforeFailure,
      'capacity refusal must roll back workflow, work, events, counters, and every reservation');
    } finally {
      restricted.close();
      restricted = null;
    }

    const retry = openStore(directory.databasePath, { clock: () => now });
    let stillQueuedJob;
    try {
      const recovered = retry._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
        ownerId: 'resource-floor-retry-owner', policy, recoverableJobIds: [expired.jobId]
      });
      assert.ok([next.jobId, later.jobId].includes(recovered.claim?.jobId));
      stillQueuedJob = recovered.claim.jobId === next.jobId ? later : next;
      const afterRetry = recoverySnapshot(directory.databasePath, expired.jobId, recovered.claim.jobId,
        expired.workflowId, attemptId);
      assert.equal(afterRetry.work.state, 'unknown');
      assert.equal(afterRetry.attempt.state, 'outcome_unknown');
      assert.equal(afterRetry.attempt.authority_hold_ref, holdRef);
      assert.equal(afterRetry.allWork.find(row => row.job_id === stillQueuedJob.jobId).state, 'queued');
      assert.equal(afterRetry.meta.pending_records, 3);
      assert.equal(afterRetry.meta.in_flight, 1);
      assert.equal(afterRetry.meta.reserved_disk_bytes, afterRetry.reservationTotals.work_disk);
      assert.equal(afterRetry.meta.reserved_event_slots, afterRetry.reservationTotals.work_events);
      assert.equal(afterRetry.events.operations_work_unknown, 1);
      assert.equal(retry._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
        ownerId: 'resource-floor-repeated-recovery', policy, recoverableJobIds: [expired.jobId]
      }), null);
      assert.deepEqual(recoverySnapshot(directory.databasePath, expired.jobId, recovered.claim.jobId,
        expired.workflowId, attemptId), afterRetry,
      'a repeated recovery does not charge capacity or duplicate the durable unknown event');
    } finally {
      retry.close();
    }
    const independentlyReopened = openStore(directory.databasePath, { clock: () => now });
    try {
      assert.equal(independentlyReopened._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
        tenantId: 'public-expired-tenant', projectId: 'public-expired-project', jobId: expired.jobId
      }).state, 'unknown');
      assert.equal(independentlyReopened.getAttempt(attemptId).authorityHoldRef, holdRef);
      assert.equal(independentlyReopened._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
        tenantId: 'public-next-tenant',
        projectId: stillQueuedJob.jobId === later.jobId ? 'public-later-project' : 'public-next-project',
        jobId: stillQueuedJob.jobId
      }).state, 'queued');
    } finally {
      independentlyReopened.close();
    }
  } finally {
    try { store?.close(); } catch {}
    directory.cleanup();
  }
}

function crashAfterPublicWorkflowMutation(databasePath, payload) {
  const source = `
import { writeSync } from 'node:fs';
import { DurableWorkflowStore } from '@jevelopers/agent-harness-runtime';
const [databasePath, payloadText] = process.argv.slice(1);
const input = JSON.parse(payloadText);
const store = DurableWorkflowStore.open({ databasePath, busyTimeoutMs: 20, clock: () => input.now });
const lease = { workflowId: input.workflowId, ownerId: input.ownerId, fence: input.fence };
let result;
if (input.mutation === 'unknown' || input.mutation === 'intent') {
  const intent = store.recordDispatchIntent({ ...lease, expectedRevision: input.revision,
    attemptId: input.attemptId, logicalOperationRef: input.attemptId + '-logical',
    authorityHoldRef: input.holdRef, transportAttemptId: input.attemptId + '-transport',
    eventId: input.attemptId + '-intent' });
  result = input.mutation === 'intent' ? intent : store.markOutcomeUnknown({ ...lease,
    expectedRevision: intent.revision, attemptId: input.attemptId, eventId: input.attemptId + '-unknown' });
} else if (input.mutation === 'succeeded' || input.mutation === 'failed') {
  result = store.transitionWorkflow({ ...lease, expectedRevision: input.revision,
    toState: input.mutation, eventId: input.attemptId + '-transition' });
} else if (input.mutation === 'cancelled') {
  result = store.requestCancellation({ ...lease, expectedRevision: input.revision,
    eventId: input.attemptId + '-cancel' });
} else {
  throw new Error('unsupported synthetic workflow mutation');
}
writeSync(1, JSON.stringify({ mutation: input.mutation, result }) + '\\n');
process.kill(process.pid, 'SIGKILL');
`;
  METRICS.processRuns += 1;
  const child = spawnSync(process.execPath, ['--input-type=module', '--eval', source,
    databasePath, JSON.stringify(payload)], {
    cwd: ROOT,
    encoding: 'utf8',
    timeout: 8_000,
    env: baseEnvironment()
  });
  assert.equal(child.signal, 'SIGKILL', child.stderr);
  assert.equal(child.status, null);
  METRICS.processSigkillSignalsSent += 1;
  const lines = child.stdout.trim().split(/\r?\n/u);
  assert.equal(lines.length, 1, child.stderr);
  return JSON.parse(lines[0]);
}

async function assertExpiredCurrentFenceReconciles(t, {
  caseName,
  mutation,
  expectedWorkflowState,
  expectedWorkflowStateAfterRecovery = expectedWorkflowState,
  expectedWorkState,
  expectedEventType,
  expectedAttemptState = null,
  afterRecovery
}) {
  const directory = tempDirectory('ah11-public-expired-reconciliation-');
  const configInput = {
    workerCount: 1, maxInFlightGlobal: 1, maxInFlightTenant: 1, maxInFlightProject: 1
  };
  const policy = operationsStoragePolicy(readOperationsConfig(configInput));
  let now = 110_000;
  let store = openStore(directory.databasePath, { clock: () => now });
  try {
    const expired = admit(store, 'public-expired-tenant', 'public-expired-project', `${caseName}-expired`, PLAN, configInput);
    const prior = store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: `${caseName}-prior-owner`, policy
    }).claim;
    assert.equal(prior.jobId, expired.jobId);
    const queued = admit(store, 'public-next-tenant', 'public-next-project', `${caseName}-queued`, PLAN, configInput);
    store.close();
    store = null;
    const attemptId = mutation === 'unknown' || mutation === 'intent' ? `${caseName}-attempt` : null;
    const holdRef = attemptId ? `${caseName}-synthetic-hold` : null;
    const mutationResult = crashAfterPublicWorkflowMutation(directory.databasePath, {
      mutation, now, workflowId: prior.workflowId, ownerId: prior.ownerId,
      fence: prior.fence, revision: prior.revision, attemptId, holdRef
    });
    assert.equal(mutationResult.mutation, mutation);
    if (mutation === 'cancelled') {
      assert.equal(mutationResult.result.accepted, true);
      assert.equal(mutationResult.result.state, 'cancelled');
    }
    const beforeRecovery = recoverySnapshot(directory.databasePath, expired.jobId, queued.jobId,
      expired.workflowId, attemptId);
    assert.equal(beforeRecovery.work.state, 'running');
    assert.equal(beforeRecovery.work.worker_id, prior.ownerId);
    assert.equal(beforeRecovery.work.generation, prior.generation);
    assert.equal(beforeRecovery.work.fence_token, prior.fence);
    assert.equal(beforeRecovery.work.lease_expires_ms, prior.leaseExpiresAtMs);
    assert.equal(beforeRecovery.workflow.state, expectedWorkflowState);
    assert.equal(beforeRecovery.workflow.fence_token, prior.fence);
    assert.equal(beforeRecovery.meta.pending_records, 2);
    assert.equal(beforeRecovery.meta.pending_bytes, beforeRecovery.work.work_bytes + beforeRecovery.nextWork.work_bytes);
    assert.equal(beforeRecovery.meta.in_flight, 1);
    assert.equal(beforeRecovery.tenant.pending_records, 1);
    assert.equal(beforeRecovery.tenant.in_flight, 1);
    assert.equal(beforeRecovery.project.pending_records, 1);
    assert.equal(beforeRecovery.nextTenant.in_flight, 0);
    assert.equal(beforeRecovery.nextWork.state, 'queued');
    assert.ok(beforeRecovery.work.reserved_disk_bytes > 0);
    assert.ok(beforeRecovery.work.reserved_event_slots > 0);
    assert.equal(beforeRecovery.meta.reserved_disk_bytes, beforeRecovery.reservationTotals.work_disk);
    assert.equal(beforeRecovery.meta.reserved_event_slots, beforeRecovery.reservationTotals.work_events);
    if (mutation === 'cancelled') {
      assert.equal(beforeRecovery.workflow.lease_owner, null, 'the public cancellation API must clear the workflow lease');
      assert.equal(beforeRecovery.workflow.lease_expires_ms, null);
    } else {
      assert.equal(beforeRecovery.workflow.lease_owner, prior.ownerId);
      assert.equal(beforeRecovery.workflow.lease_expires_ms, prior.leaseExpiresAtMs);
    }
    if (attemptId) {
      assert.deepEqual(beforeRecovery.attempt, {
        state: mutation === 'unknown' ? 'outcome_unknown' : 'intent', authority_hold_ref: holdRef
      });
    }
    now = prior.leaseExpiresAtMs + 1;
    let reopened = openStore(directory.databasePath, { clock: () => now });
    const expiredClaims = reopened._operationsExpiredClaims(OPERATIONS_STORAGE_CAPABILITY, { policy });
    assert.equal(expiredClaims.incomplete, false);
    assert.equal(expiredClaims.items[0].jobId, expired.jobId);
    try {
      const recovery = reopened._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
        ownerId: `${caseName}-recovery-owner`, policy, recoverableJobIds: [expired.jobId]
      });
      assert.equal(recovery.claim?.jobId, queued.jobId,
        'reconciliation must release only the expired generation before the next tenant claims');

      const workflow = reopened.getWorkflow(expired.workflowId);
      assert.equal(workflow.state, expectedWorkflowStateAfterRecovery);
      assert.equal(workflow.fence, prior.fence);
      if (expectedAttemptState) {
        assert.equal(workflow.cancellationRequested, false);
        const attempt = reopened.getAttempt(attemptId);
        assert.equal(attempt.state, expectedAttemptState);
        assert.equal(attempt.authorityHoldRef, holdRef);
      }
      const recoveredSnapshot = recoverySnapshot(directory.databasePath, expired.jobId, queued.jobId,
        expired.workflowId, attemptId);
      assert.equal(recoveredSnapshot.work.state, expectedWorkState);
      assert.equal(recoveredSnapshot.work.worker_id, null);
      assert.equal(recoveredSnapshot.work.runner_pid, null);
      assert.equal(recoveredSnapshot.work.lease_expires_ms, null);
      assert.equal(recoveredSnapshot.work.generation, prior.generation);
      assert.equal(recoveredSnapshot.work.fence_token, prior.fence);
      assert.equal(recoveredSnapshot.workflow.state, expectedWorkflowStateAfterRecovery);
      assert.equal(recoveredSnapshot.workflow.lease_owner, null);
      assert.equal(recoveredSnapshot.workflow.lease_expires_ms, null);
      assert.equal(recoveredSnapshot.nextWork.state, 'running');
      assert.equal(recoveredSnapshot.nextWork.worker_id, recovery.claim.ownerId);
      const unresolved = expectedWorkState === 'unknown';
      const expectedPending = unresolved ? 2 : 1;
      assert.equal(recoveredSnapshot.meta.pending_records, expectedPending);
      assert.equal(recoveredSnapshot.meta.pending_bytes, unresolved
        ? beforeRecovery.meta.pending_bytes
        : beforeRecovery.meta.pending_bytes - beforeRecovery.work.work_bytes);
      assert.equal(recoveredSnapshot.meta.in_flight, 1);
      assert.equal(recoveredSnapshot.tenant.pending_records, unresolved ? 1 : 0);
      assert.equal(recoveredSnapshot.tenant.in_flight, 0);
      assert.equal(recoveredSnapshot.project.pending_records, unresolved ? 1 : 0);
      assert.equal(recoveredSnapshot.nextTenant.in_flight, 1);
      assert.equal(recoveredSnapshot.project.in_flight, 0);
      assert.equal(recoveredSnapshot.meta.reserved_disk_bytes, recoveredSnapshot.reservationTotals.work_disk);
      assert.equal(recoveredSnapshot.meta.reserved_event_slots, recoveredSnapshot.reservationTotals.work_events);
      if (unresolved) {
        assert.equal(recoveredSnapshot.work.reserved_disk_bytes,
          beforeRecovery.work.reserved_disk_bytes - 16_384,
          'clearing the expired workflow lease consumes one bounded lifecycle-write reserve');
        assert.equal(recoveredSnapshot.work.reserved_event_slots, beforeRecovery.work.reserved_event_slots - 1,
          'the durable unknown event consumes one reserved event slot while the opaque hold remains');
        assert.equal(recoveredSnapshot.nextWork.reserved_disk_bytes,
          beforeRecovery.nextWork.reserved_disk_bytes - 16_384,
          'the queued tenant claim consumes its own lifecycle-write reserve');
        assert.equal(recoveredSnapshot.nextWork.reserved_event_slots,
          beforeRecovery.nextWork.reserved_event_slots - 1,
          'the next claim event consumes its own reserved slot');
        assert.equal(recoveredSnapshot.meta.reserved_disk_bytes,
          beforeRecovery.meta.reserved_disk_bytes
            - (beforeRecovery.work.reserved_disk_bytes - recoveredSnapshot.work.reserved_disk_bytes)
            - (beforeRecovery.nextWork.reserved_disk_bytes - recoveredSnapshot.nextWork.reserved_disk_bytes));
        assert.equal(recoveredSnapshot.meta.reserved_event_slots,
          beforeRecovery.meta.reserved_event_slots
            - (beforeRecovery.work.reserved_event_slots - recoveredSnapshot.work.reserved_event_slots)
            - (beforeRecovery.nextWork.reserved_event_slots - recoveredSnapshot.nextWork.reserved_event_slots));
        assert.equal(recoveredSnapshot.attempt.authority_hold_ref, holdRef);
      } else {
        assert.equal(recoveredSnapshot.work.reserved_disk_bytes, 0);
        assert.equal(recoveredSnapshot.work.reserved_event_slots, 0);
      }
      assert.equal(recoveredSnapshot.events[expectedEventType] ?? 0, (beforeRecovery.events[expectedEventType] ?? 0) + 1);
      assert.equal(Object.values(recoveredSnapshot.events).reduce((sum, value) => sum + value, 0),
        Object.values(beforeRecovery.events).reduce((sum, value) => sum + value, 0) + 1,
        'recovery appends exactly one workflow event');
      reopened.close();

      const independentlyReopened = openStore(directory.databasePath, { clock: () => now });
      try {
        assert.equal(independentlyReopened._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
          tenantId: 'public-expired-tenant', projectId: 'public-expired-project', jobId: expired.jobId
        }).state, expectedWorkState);
        assert.equal(independentlyReopened.getWorkflow(expired.workflowId).state, expectedWorkflowStateAfterRecovery);
        assert.deepEqual(recoverySnapshot(directory.databasePath, expired.jobId, queued.jobId,
          expired.workflowId, attemptId), recoveredSnapshot, 'a second independent reopen must preserve the reconciled snapshot');
        assert.equal(independentlyReopened._operationsExpiredClaims(OPERATIONS_STORAGE_CAPABILITY, { policy }).items.length, 0);
        assert.equal(independentlyReopened._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
          ownerId: `${caseName}-repeat-recovery`, policy, recoverableJobIds: [expired.jobId]
        }), null, 'repeated recovery cannot duplicate terminal/unknown events or bypass occupied caps');
        if (expectedAttemptState) {
          assert.equal(independentlyReopened.getAttempt(attemptId).authorityHoldRef, holdRef);
        }
      } finally {
        independentlyReopened.close();
      }
      await afterRecovery?.({ databasePath: directory.databasePath, caseName, expired, queued, prior, now, attemptId, holdRef });
    } finally {
      try { reopened.close(); } catch {}
    }
  } finally {
    try { store?.close(); } catch {}
    directory.cleanup();
  }
}

function planAtWorkByteLength(targetBytes, seedPlan = PLAN, configInput = undefined) {
  const config = readOperationsConfig(configInput);
  const initial = validateLocalPlanWork({ planJson: seedPlan, limitsJson: '{}' }, 'ops-validation-workflow', config);
  const paddingBytes = targetBytes - initial.bytes;
  assert.ok(paddingBytes >= 0, 'The selected work shape must fit below the requested boundary.');
  const padded = validateLocalPlanWork({
    planJson: seedPlan + ' '.repeat(paddingBytes), limitsJson: '{}'
  }, 'ops-validation-workflow', config);
  assert.equal(padded.bytes, targetBytes);
  return padded;
}

async function runChild(script, args, { timeoutMs = 8_000, env = {} } = {}) {
  METRICS.processRuns += 1;
  return new Promise((resolvePromise, reject) => {
    const child = spawn(process.execPath, [script, ...args], {
      cwd: ROOT,
      env: {
        PATH: process.env.PATH ?? '/usr/bin:/bin',
        TMPDIR: process.env.TMPDIR ?? '/tmp',
        NODE_NO_WARNINGS: '1',
        ...env
      },
      stdio: ['ignore', 'pipe', 'pipe']
    });
    let stdout = '';
    let stderr = '';
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', chunk => { stdout += chunk; });
    child.stderr.on('data', chunk => { stderr += chunk; });
    const timer = setTimeout(() => {
      try {
        if (child.kill('SIGKILL')) METRICS.processSigkillSignalsSent += 1;
      } catch {}
      reject(new Error(`Owned test process exceeded ${timeoutMs} ms.`));
    }, timeoutMs);
    child.once('error', error => {
      clearTimeout(timer);
      reject(error);
    });
    child.once('close', (code, signal) => {
      clearTimeout(timer);
      resolvePromise({ child, code, signal, stdout, stderr });
    });
  });
}

function startOwnedChild(script, args, { timeoutMs = 8_000, env = {} } = {}) {
  METRICS.processRuns += 1;
  const child = spawn(process.execPath, [script, ...args], {
    cwd: ROOT,
    env: {
      PATH: process.env.PATH ?? '/usr/bin:/bin',
      TMPDIR: process.env.TMPDIR ?? '/tmp',
      NODE_NO_WARNINGS: '1',
      ...env
    },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  const observation = observeClosedChild(child);
  let stdout = '';
  let stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', chunk => { stdout += chunk; });
  child.stderr.on('data', chunk => { stderr += chunk; });
  const closed = observation.closed.then(exit => ({ ...exit, child, stdout, stderr }));
  const timeout = setTimeout(() => {
    try {
      if (child.kill('SIGKILL')) METRICS.processSigkillSignalsSent += 1;
    } catch {}
  }, timeoutMs);
  void closed.then(() => clearTimeout(timeout));
  return { child, closed, snapshot: () => ({ ...observation.snapshot(), child, stdout, stderr }) };
}

async function waitForPath(path, child, timeoutMs = 6_000) {
  const end = performance.now() + timeoutMs;
  while (!existsSync(path)) {
    if (child.exitCode !== null || child.signalCode !== null) {
      throw new Error(`The owned fixture exited before its barrier (exit=${child.exitCode}; signal=${child.signalCode}).`);
    }
    if (performance.now() >= end) throw new Error('The owned fixture did not reach its barrier.');
    await new Promise(resolvePromise => setTimeout(resolvePromise, 5));
  }
}

async function waitForCloseBounded(closed, timeoutMs) {
  return waitForClosedChild(closed, timeoutMs);
}

function ownedProcessGroupMembers(groupId) {
  const result = spawnSync('pgrep', ['-g', String(groupId)], {
    encoding: 'utf8', timeout: 100, killSignal: 'SIGKILL'
  });
  if (result.status === 1) return [];
  if (result.status !== 0) throw new Error('The isolated fixture process group could not be inspected.');
  return result.stdout.trim().split(/\s+/u).filter(Boolean).map(Number);
}

async function waitForOwnedProcessGroupEmpty(groupId, maxElapsedMs = 2_000) {
  const probeTimeoutMs = 100;
  const intervalMs = 100;
  const maxProbes = Math.max(1, Math.floor((maxElapsedMs + intervalMs) / (probeTimeoutMs + intervalMs)));
  for (let probe = 0; probe < maxProbes; probe += 1) {
    if (ownedProcessGroupMembers(groupId).length === 0) return true;
    if (probe + 1 < maxProbes) await new Promise(resolvePromise => setTimeout(resolvePromise, intervalMs));
  }
  return false;
}

async function cleanupOwnedProcessGroup(child, closed, captureEscalation = () => {}, state = METRICS) {
  if (!Number.isSafeInteger(child.pid) || child.pid < 2) return false;
  const groupId = child.pid;
  try {
    if (await waitForOwnedProcessGroupEmpty(groupId, 100)) return true;
  } catch {}
  try {
    if (captureEscalation('cleanup_escalation_sent') === false) state.ci1PhaseCaptureIncomplete = true;
  } catch { state.ci1PhaseCaptureIncomplete = true; }
  try { process.kill(-groupId, 'SIGCONT'); } catch {}
  try { process.kill(-groupId, 'SIGTERM'); METRICS.processSigtermSignalsSent += 1; } catch {}
  await waitForCloseBounded(closed, 1_000);
  try { if (await waitForOwnedProcessGroupEmpty(groupId, 1_000)) return true; } catch {}
  try { process.kill(-groupId, 'SIGCONT'); } catch {}
  try { process.kill(-groupId, 'SIGKILL'); METRICS.processSigkillSignalsSent += 1; } catch {}
  await waitForCloseBounded(closed, 1_000);
  try { return await waitForOwnedProcessGroupEmpty(groupId, 1_000); } catch { return false; }
}

function readRunnerPidFromOwnedDatabase(databasePath, idempotencyKey) {
  try {
    const database = new DatabaseSync(databasePath, { readOnly: true });
    try {
      const row = database.prepare(
        'SELECT runner_pid FROM operations_work WHERE idempotency_key=?'
      ).get(idempotencyKey);
      return Number.isSafeInteger(Number(row?.runner_pid)) && Number(row.runner_pid) > 1
        ? Number(row.runner_pid) : null;
    } finally {
      database.close();
    }
  } catch {
    return null;
  }
}

function processGroupId(pid) {
  const result = spawnSync('ps', ['-o', 'pgid=', '-p', String(pid)], {
    encoding: 'utf8', timeout: 100, killSignal: 'SIGKILL'
  });
  if (result.status !== 0) return null;
  const value = Number(result.stdout.trim());
  return Number.isSafeInteger(value) && value > 1 ? value : null;
}

function readCi1OwnedGroupRegistry(registryPath) {
  if (!registryPath || !existsSync(registryPath)) return null;
  try {
    const registry = JSON.parse(readFileSync(registryPath, 'utf8'));
    if (!registry || registry.schema !== 'ops-ci1-owned-groups.v1'
        || Object.keys(registry).sort().join(',') !== 'groups,pendingLaunches,schema'
        || !Array.isArray(registry.groups) || !Array.isArray(registry.pendingLaunches)
        || registry.groups.length > 16 || registry.pendingLaunches.length > 16
        || registry.groups.some(value => !Number.isSafeInteger(value) || value < 2)
        || new Set(registry.groups).size !== registry.groups.length
        || registry.pendingLaunches.some(value => typeof value !== 'string'
          || !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u.test(value))
        || new Set(registry.pendingLaunches).size !== registry.pendingLaunches.length) return null;
    return {
      schema: registry.schema,
      groups: registry.groups,
      pendingLaunches: registry.pendingLaunches
    };
  } catch {
    return null;
  }
}

function writeCi1OwnedGroupRegistry(registryPath, registry, fileOps = {}) {
  const makeUuid = fileOps.randomUUID ?? randomUUID;
  const temporaryPath = `${registryPath}.${process.pid}.${makeUuid()}.tmp`;
  let bytes;
  try { bytes = Buffer.from(JSON.stringify(registry), 'utf8'); } catch { return false; }
  return publishCi1JsonAtomically(registryPath, temporaryPath, bytes, fileOps);
}

function beginCi1OwnedGroupLaunch(launchToken, custody = null) {
  const registryPath = process.env.OPS_CI1_OWNED_GROUP_REGISTRY;
  if (!registryPath) return false;
  if (typeof launchToken !== 'string'
      || !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u.test(launchToken)) return false;
  const registry = readCi1OwnedGroupRegistry(registryPath);
  if (!registry || registry.pendingLaunches.length >= 16) return false;
  if (!registry.pendingLaunches.includes(launchToken)) registry.pendingLaunches.push(launchToken);
  if (custody) {
    if (custody.launches.size >= CI1_TEMP_CUSTODY_MAX_LAUNCHES || custody.launches.has(launchToken)) return false;
    custody.launches.set(launchToken, {
      launchToken, status: 'launch_intent', spawnAttempted: false,
      directPid: null, groupId: null, registryRegistered: false,
      registryCleared: false, groupAbsent: false
    });
    if (!writeCi1TemporaryCustody(custody)) {
      custody.launches.delete(launchToken);
      return false;
    }
  }
  if (!writeCi1OwnedGroupRegistry(registryPath, registry)) {
    if (custody) {
      custody.launches.delete(launchToken);
      writeCi1TemporaryCustody(custody);
    }
    return false;
  }
  return true;
}

function cancelCi1OwnedGroupLaunch(launchToken) {
  const registryPath = process.env.OPS_CI1_OWNED_GROUP_REGISTRY;
  if (!registryPath || typeof launchToken !== 'string') return false;
  const registry = readCi1OwnedGroupRegistry(registryPath);
  if (!registry || !registry.pendingLaunches.includes(launchToken)) return false;
  registry.pendingLaunches = registry.pendingLaunches.filter(token => token !== launchToken);
  return writeCi1OwnedGroupRegistry(registryPath, registry);
}

function markCi1OwnedGroupRegistered(custody, launchToken, groupId) {
  const launch = custody?.launches.get(launchToken);
  if (!launch || launch.groupId !== groupId) return false;
  launch.registryRegistered = true;
  launch.status = 'group_registered';
  return writeCi1TemporaryCustody(custody);
}

function updateCi1OwnedGroupRegistry(groupId, register, launchToken = null) {
  const registryPath = process.env.OPS_CI1_OWNED_GROUP_REGISTRY;
  if (!registryPath) return false;
  if (!Number.isSafeInteger(groupId) || groupId < 2) return false;
  const registry = readCi1OwnedGroupRegistry(registryPath);
  if (!registry) return false;
  const next = new Set(registry.groups);
  if (register) {
    if (typeof launchToken !== 'string' || !registry.pendingLaunches.includes(launchToken)) return false;
    next.add(groupId);
  } else if (!next.has(groupId) && !registry.pendingLaunches.includes(launchToken)) {
    return false;
  } else {
    next.delete(groupId);
  }
  if (next.size > 16) return false;
  registry.groups = [...next];
  if (typeof launchToken === 'string') {
    registry.pendingLaunches = registry.pendingLaunches.filter(token => token !== launchToken);
  }
  return writeCi1OwnedGroupRegistry(registryPath, registry);
}

async function verifyRegisterCi1OwnedGroupBeforeReady({
  child, launchToken, readyPath,
  verifyGroup = verifyOwnedProcessGroup,
  registerGroup = updateCi1OwnedGroupRegistry,
  releaseReady = (path, ready) => writeFileSync(path, JSON.stringify(ready), { flag: 'wx', mode: 0o600 }),
  custody = null,
  closed = null
}) {
  const launch = custody?.launches.get(launchToken);
  if (custody) {
    if (!launch || launch.spawnAttempted !== true) {
      return { groupId: null, registered: false, readyReleased: false, stage: 'custody_missing' };
    }
    launch.child = child;
    launch.closed = closed;
    launch.directPid = child.pid;
    launch.status = 'child_spawned';
    if (!writeCi1TemporaryCustody(custody)) {
      return { groupId: null, registered: false, readyReleased: false, stage: 'custody_record_failed' };
    }
  }
  const groupId = await verifyGroup(child);
  if (groupId !== child.pid) return { groupId: null, registered: false, readyReleased: false, stage: 'group_unverified' };
  if (launch) {
    launch.groupId = groupId;
    launch.status = 'group_verified';
    if (!writeCi1TemporaryCustody(custody)) {
      return { groupId, registered: false, readyReleased: false, stage: 'custody_record_failed' };
    }
  }
  const registered = registerGroup(groupId, true, launchToken);
  if (!registered) return { groupId, registered: false, readyReleased: false, stage: 'registration_failed' };
  if (launch && !markCi1OwnedGroupRegistered(custody, launchToken, groupId)) {
    return { groupId, registered: true, readyReleased: false, stage: 'custody_record_failed' };
  }
  try {
    await releaseReady(readyPath, {
      schema: 'ops-ci1-group-ready.v1', pid: child.pid,
      groupId, registryConfirmed: true
    });
  } catch {
    return { groupId, registered: true, readyReleased: false, stage: 'ready_release_failed' };
  }
  if (launch) {
    launch.status = 'ready_released';
    if (!writeCi1TemporaryCustody(custody)) {
      return { groupId, registered: true, readyReleased: true, stage: 'custody_record_failed' };
    }
  }
  return { groupId, registered: true, readyReleased: true, stage: 'ready' };
}

async function verifyOwnedProcessGroup(child, timeoutMs = 1_000) {
  if (!Number.isSafeInteger(child.pid) || child.pid < 2) return null;
  const probeTimeoutMs = 100;
  const intervalMs = 5;
  const maxProbes = Math.max(1, Math.floor((timeoutMs + intervalMs) / (probeTimeoutMs + intervalMs)));
  for (let probe = 0; probe < maxProbes; probe += 1) {
    if (processGroupId(child.pid) === child.pid) return child.pid;
    if (child.exitCode !== null || child.signalCode !== null) return null;
    if (probe + 1 < maxProbes) await new Promise(resolvePromise => setTimeout(resolvePromise, intervalMs));
  }
  return null;
}

async function cleanupVerifiedCi1ProcessGroup(child, closed, groupId, captureEscalation = () => {}, state = METRICS) {
  if (groupId !== child.pid) {
    // The child PID is exact ownership even if group verification failed; never
    // signal a possibly shared group. Group absence remains explicitly unknown.
    try {
      if (captureEscalation('cleanup_escalation_sent') === false) state.ci1PhaseCaptureIncomplete = true;
    } catch { state.ci1PhaseCaptureIncomplete = true; }
    try { child.kill('SIGCONT'); } catch {}
    try { child.kill('SIGTERM'); METRICS.processSigtermSignalsSent += 1; } catch {}
    let closeResult = await waitForCloseBounded(closed, 1_000);
    if (!closeResult) {
      try { child.kill('SIGCONT'); } catch {}
      try { child.kill('SIGKILL'); METRICS.processSigkillSignalsSent += 1; } catch {}
      closeResult = await waitForCloseBounded(closed, 1_000);
    }
    return { closeResult, groupEmpty: false };
  }
  const groupEmpty = await cleanupOwnedProcessGroup(child, closed, captureEscalation, state);
  const closeResult = await waitForCloseBounded(closed, 500);
  return { closeResult, groupEmpty };
}

async function finishCi1OwnedGroupWatchdog(watchdog, watchdogGroupId) {
  let closeResult = null;
  try {
    closeResult = await waitForCloseBounded(
      watchdog.closed, CI1_LOCAL_WATCHDOG_CLEANUP_COMPLETION_MS
    );
  } catch {}
  if (watchdogGroupId !== watchdog.child.pid) {
    return { closeResult, groupEmpty: false, completionWaitExpired: !closeResult };
  }
  let cleanupResult;
  try {
    cleanupResult = await cleanupVerifiedCi1ProcessGroup(
      watchdog.child, watchdog.closed, watchdogGroupId
    );
  } catch {
    cleanupResult = null;
  }
  return {
    closeResult: cleanupResult?.closeResult ?? closeResult,
    groupEmpty: cleanupResult?.groupEmpty === true,
    completionWaitExpired: !closeResult
  };
}

function startPrelaunchedOwnedGroupWatchdog(controlPath, timeoutMs = 15_000) {
  const readyPath = `${controlPath}.ready`;
  const watchAckPath = `${controlPath}.watch-ack`;
  const resultPath = `${controlPath}.result`;
  const source = `
import { randomUUID } from 'node:crypto';
import { closeSync, openSync, readFileSync, renameSync, rmSync, writeFileSync, writeSync } from 'node:fs';
import { performance } from 'node:perf_hooks';
const publishC02ControlJsonAtomically = ${publishC02ControlJsonAtomically.toString()};
const attemptC02WatchdogTargetAck = ${attemptC02WatchdogTargetAck.toString()};
const controlPath = process.argv[1];
const readyPath = process.argv[2];
const watchAckPath = process.argv[3];
const resultPath = process.argv[4];
const timeoutMs = Number(process.argv[5]);
const deadline = performance.now() + timeoutMs;
let groupId = null;
const targetAckState = { targetAckSent: false, targetAckPublicationFailed: false };
const startedAt = performance.now();
publishC02ControlJsonAtomically(readyPath, {
  schema: 'ops-owned-group-watchdog-ready.v1', pid: process.pid, groupId: process.pid
});
let timedOut = true;
let cleanupRequested = false;
while (performance.now() < deadline) {
  try {
    const control = JSON.parse(readFileSync(controlPath, 'utf8'));
    if (Number.isSafeInteger(control.groupId) && control.groupId > 1) groupId = control.groupId;
    if (control.state === 'watch' && groupId !== null) {
      attemptC02WatchdogTargetAck({
        groupId, watchAckPath, pid: process.pid, state: targetAckState,
        probe: (target, signal) => process.kill(target, signal),
        publish: publishC02ControlJsonAtomically
      });
    }
    if (control.state === 'done') { timedOut = false; break; }
    if (control.state === 'cleanup') { timedOut = false; cleanupRequested = true; break; }
  } catch {}
  await new Promise(resolvePromise => setTimeout(resolvePromise, ${CI1_LOCAL_WATCHDOG_POLL_MS}));
}
let targetGroupEmpty = null;
if (groupId !== null && (timedOut || cleanupRequested)) {
  const signalAndWait = async (signal, timeoutMs) => {
    try { process.kill(-groupId, 'SIGCONT'); } catch {}
    try { process.kill(-groupId, signal); } catch {}
    const end = performance.now() + timeoutMs;
    while (performance.now() < end) {
      try { process.kill(-groupId, 0); } catch (error) {
        if (error?.code === 'ESRCH') return true;
      }
      await new Promise(resolvePromise => setTimeout(resolvePromise, 10));
    }
    try { process.kill(-groupId, 0); } catch (error) { return error?.code === 'ESRCH'; }
    return false;
  };
  targetGroupEmpty = await signalAndWait('SIGTERM', ${CI1_LOCAL_WATCHDOG_SIGNAL_PHASE_MS});
  if (!targetGroupEmpty) targetGroupEmpty = await signalAndWait('SIGKILL', ${CI1_LOCAL_WATCHDOG_SIGNAL_PHASE_MS});
}
writeFileSync(resultPath, JSON.stringify({ schema: 'ops-owned-group-watchdog-result.v1', pid: process.pid,
  groupId: process.pid, timedOut, cleanupRequested,
  targetAckSent: targetAckState.targetAckSent, targetGroupId: groupId,
  targetAckPublicationFailed: targetAckState.targetAckPublicationFailed, targetGroupEmpty,
  elapsedMs: Math.max(0, Math.round(performance.now() - startedAt)) }), { flag: 'wx', mode: 0o600 });
`;
  const child = spawn(process.execPath, ['--input-type=module', '--eval', source, controlPath, readyPath, watchAckPath, resultPath, String(timeoutMs)], {
    cwd: ROOT, env: baseEnvironment(), stdio: 'ignore', detached: true
  });
  const spawned = new Promise((resolvePromise, reject) => {
    child.once('spawn', resolvePromise);
    child.once('error', reject);
  });
  const closed = new Promise(resolvePromise => child.once('close', (code, signal) => resolvePromise({ code, signal })));
  return { child, spawned, closed, readyPath, watchAckPath, resultPath };
}

async function waitForPidExit(pid, timeoutMs = 1_000) {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    try { process.kill(pid, 0); } catch (error) {
      if (error?.code === 'ESRCH') return true;
      throw error;
    }
    await new Promise(resolvePromise => setTimeout(resolvePromise, 10));
  }
  try { process.kill(pid, 0); } catch (error) { return error?.code === 'ESRCH'; }
  return false;
}

async function killOwnedChildAndReap(child, closed) {
  let sent = false;
  try { sent = child.kill('SIGKILL'); } catch {}
  if (sent) METRICS.processSigkillSignalsSent += 1;
  let closeResult = await waitForCloseBounded(closed, 1_000);
  if (!closeResult && child.exitCode === null && child.signalCode === null) {
    try {
      if (child.kill('SIGKILL')) METRICS.processSigkillSignalsSent += 1;
    } catch {}
    closeResult = await waitForCloseBounded(closed, 1_000);
  }
  return closeResult;
}

async function waitForStatus(worker, scope, jobId, expected, timeoutMs = 6_000, onObservation) {
  const end = performance.now() + timeoutMs;
  while (performance.now() < end) {
    const page = worker.readScopedWorkPage({ trustedSyntheticScope: scope, limit: 100 });
    const item = page.items.find(row => row.jobId === jobId);
    onObservation?.(item);
    if (item?.state === expected) return item;
    await new Promise(resolvePromise => setTimeout(resolvePromise, 10));
  }
  throw new Error(`The durable job did not reach ${expected}.`);
}

function baseEnvironment() {
  return { PATH: process.env.PATH ?? '/usr/bin:/bin', TMPDIR: process.env.TMPDIR ?? '/tmp', NODE_NO_WARNINGS: '1' };
}

function unusedProcessId() {
  for (let candidate = 2_000_000_000; candidate < 2_000_001_000; candidate += 1) {
    try {
      process.kill(candidate, 0);
    } catch (error) {
      if (error?.code === 'ESRCH') return candidate;
    }
  }
  throw new Error('The fixture could not find an unused process identifier.');
}

function countOpenFileDescriptors(pid) {
  if (process.platform === 'linux') {
    try { return readdirSync(`/proc/${pid}/fd`).length; } catch { return null; }
  }
  if (process.platform !== 'darwin') return null;
  const result = spawnSync('/usr/sbin/lsof', ['-nP', '-p', String(pid), '-Fn'], { encoding: 'utf8', maxBuffer: 1_048_576 });
  if (result.status !== 0) return null;
  return result.stdout.split(/\r?\n/u).filter(line => /^f[^n]/u.test(line)).length;
}

test('OPS04.C03 fixed failure diagnostics preserve bounded schemas and original errors', () => {
  const boundaryCases = [
    ['SHUTDOWN_STOP_INVARIANT', {
      workerStopped: true, ownedChildrenReaped: true, storeClosed: true, sinkCalledOnce: true
    }],
    ['TELEMETRY_BACKLOG_INVARIANT', { cursorIsZero: true, pendingRecordsIsPositive: true }],
    ['POSTSTOP_READ', { errorKind: 'SQLITE_BUSY' }],
    ['QUEUE_ACCOUNTING_INVARIANT', {
      workIsQueued: true, inFlightIsZero: true, attemptsUnchanged: true
    }],
    ['OTHER_BOUNDARY', {}]
  ];
  for (const [boundary, observations] of boundaryCases) {
    const details = createOps04FailureDetails(boundary, observations);
    assert.ok(details, `${boundary} has a valid fixed schema`);
    assert.deepEqual(parseOps04FailureDetails(details), details);
    assert.deepEqual(ops04FailureFields(boundary), Object.keys(observations));
  }
  assert.equal(createOps04FailureDetails('UNLISTED', {}), null);
  assert.equal(createOps04FailureDetails('TELEMETRY_BACKLOG_INVARIANT', {
    cursorIsZero: true, pendingRecordsIsPositive: 1
  }), null);
  assert.equal(createOps04FailureDetails('POSTSTOP_READ', { errorKind: 'secret-message' }), null);
  assert.equal(ops04ReadErrorKind({ errcode: 517 }), 'SQLITE_BUSY');
  assert.equal(ops04ReadErrorKind({ errcode: 10 }), 'SQLITE_IOERR');
  assert.equal(ops04ReadErrorKind({ errcode: 11 }), 'SQLITE_CORRUPT');
  assert.equal(ops04ReadErrorKind({ code: 'ERR_SQLITE_ERROR' }), 'SQLITE_ERROR');
  assert.equal(ops04ReadErrorKind({ message: 'never published' }), 'OTHER');
  assert.equal(parseOps04FailureDetails({
    diagnosticSchema: 'agent-harness.ops04-child-failure.v1',
    failureBoundary: 'OTHER_BOUNDARY', extra: true
  }), null);

  assert.equal(classifyOps04ExitDisposition({ code: 0, signal: null }), 'exit_zero');
  assert.equal(classifyOps04ExitDisposition({ code: 1, signal: null }), 'exit_nonzero');
  assert.equal(classifyOps04ExitDisposition({ code: null, signal: 'SIGTERM' }), 'signal');
  assert.equal(classifyOps04ExitDisposition({ code: null, signal: null }), 'unknown');
  assert.equal(classifyOps04ExitDisposition({ code: -1, signal: null }), null);
  assert.equal(classifyOps04ExitDisposition({ code: 1, signal: 'SIGTERM' }), null);
  assert.equal(isOps04ExitDisposition('SIGTERM'), false);
  const normalClose = {
    schema: 1, source: 'parent', phase: 'before-marker', role: 'shutdown', sequence: 1,
    stage: 'shutdown_child_closed', elapsedMs: 0, exitDisposition: 'exit_zero'
  };
  const normalMetrics = {
    ci1PhaseCaptures: [normalClose], ci1PhaseCaptureBytes: Buffer.byteLength(JSON.stringify(normalClose)),
    ci1PhaseCaptureOverflow: false, ci1PhaseCaptureIncomplete: false,
    ci1PhaseCaptureChildCloseUnconfirmed: false, ci1PhaseCaptureOwnedGroupCloseUnconfirmed: false
  };
  assert.deepEqual(validateOps04PublicPhaseMetrics(normalMetrics), { valid: true, failureCount: 0 });
  const inconsistentClose = { ...normalClose, exitDisposition: 'exit_nonzero' };
  assert.equal(validateOps04PublicPhaseMetrics({
    ...normalMetrics,
    ci1PhaseCaptures: [inconsistentClose],
    ci1PhaseCaptureBytes: Buffer.byteLength(JSON.stringify(inconsistentClose))
  }).valid, false, 'nonzero exit cannot be published as complete evidence');

  const shutdownStages = [
    'fixture_started', 'store_opened', 'worker_started', 'runner_claimed', 'workflow_opened',
    'marker_written', 'signal_seen', 'stop_requested', 'stop_settled', 'loop_settled',
    'shutdown_verified', 'store_close_started', 'store_closed', 'fixture_finally_started',
    'fixture_finally_complete'
  ];
  const rows = shutdownStages.map((stage, index) => ({
    schema: 1, sequence: index + 1, stage, elapsedMs: index,
    ...(stage === 'stop_settled' ? { stopped: true, childrenReaped: true, storeClosed: true } : {})
  }));
  rows.push({
    schema: 1, sequence: rows.length + 1, stage: 'fixture_failure', elapsedMs: rows.length,
    ...createOps04FailureDetails('QUEUE_ACCOUNTING_INVARIANT', {
      workIsQueued: false, inFlightIsZero: true, attemptsUnchanged: true
    })
  });
  const state = {
    ci1PhaseCaptures: [], ci1PhaseCaptureBytes: 0, ci1PhaseCaptureOverflow: false,
    ci1PhaseCaptureIncomplete: false, ci1PhaseCaptureChildCloseUnconfirmed: false,
    ci1PhaseCaptureOwnedGroupCloseUnconfirmed: false
  };
  const parsed = parseCi1ChildPhaseCaptureBytes(
    Buffer.from(`${rows.map(row => JSON.stringify(row)).join('\n')}\n`), 'before-marker', 'shutdown', state
  );
  assert.equal(parsed.complete, false, 'a failure row never changes the original success oracle');
  assert.equal(parsed.failureCode, 'fixture_failure');
  assert.equal(state.ci1PhaseCaptureIncomplete, true);
  assert.deepEqual(state.ci1PhaseCaptures.at(-1), {
    schema: 1, source: 'child', phase: 'before-marker', role: 'shutdown',
    sequence: rows.length, stage: 'fixture_failure', elapsedMs: rows.length - 1,
    diagnosticSchema: 'agent-harness.ops04-child-failure.v1',
    failureBoundary: 'QUEUE_ACCOUNTING_INVARIANT',
    workIsQueued: false, inFlightIsZero: true, attemptsUnchanged: true
  });

  const malformed = [...rows.slice(0, -1), { ...rows.at(-1), unexpected: 'redacted' }];
  const malformedState = {
    ci1PhaseCaptures: [], ci1PhaseCaptureBytes: 0, ci1PhaseCaptureOverflow: false,
    ci1PhaseCaptureIncomplete: false, ci1PhaseCaptureChildCloseUnconfirmed: false,
    ci1PhaseCaptureOwnedGroupCloseUnconfirmed: false
  };
  const malformedResult = parseCi1ChildPhaseCaptureBytes(
    Buffer.from(`${malformed.map(row => JSON.stringify(row)).join('\n')}\n`),
    'before-marker', 'shutdown', malformedState
  );
  assert.equal(malformedResult.complete, false);
  assert.equal(malformedResult.failureCode, 'failure_shape');
  assert.equal(malformedState.ci1PhaseCaptures.length, rows.length - 1,
    'malformed diagnostic data does not erase the valid prefix');

  const firstFailure = {
    ...rows.at(-1), sequence: 15, elapsedMs: 14
  };
  const duplicateRows = [
    ...rows.slice(0, 14), firstFailure,
    { ...firstFailure, sequence: 16, elapsedMs: 15 }
  ];
  const duplicateState = {
    ci1PhaseCaptures: [], ci1PhaseCaptureBytes: 0, ci1PhaseCaptureOverflow: false,
    ci1PhaseCaptureIncomplete: false, ci1PhaseCaptureChildCloseUnconfirmed: false,
    ci1PhaseCaptureOwnedGroupCloseUnconfirmed: false
  };
  const duplicateResult = parseCi1ChildPhaseCaptureBytes(
    Buffer.from(`${duplicateRows.map(row => JSON.stringify(row)).join('\n')}\n`),
    'before-marker', 'shutdown', duplicateState
  );
  assert.equal(duplicateResult.complete, false);
  assert.equal(duplicateResult.failureCode, 'duplicate_failure');
  assert.equal(duplicateState.ci1PhaseCaptures.length, 15,
    'the first failure record and valid prefix survive a duplicate row');
  assert.equal(duplicateState.ci1PhaseCaptureIncomplete, true);

  const publicFailure = { ...state.ci1PhaseCaptures.at(-1), sequence: 1 };
  const parentClose = {
    schema: 1, source: 'parent', phase: 'before-marker', role: 'shutdown', sequence: 1,
    stage: 'shutdown_child_closed', elapsedMs: 20, exitDisposition: 'exit_nonzero'
  };
  const captures = [publicFailure, parentClose];
  const publicMetrics = {
    ci1PhaseCaptures: captures,
    ci1PhaseCaptureBytes: captures.reduce((sum, record) => sum + Buffer.byteLength(JSON.stringify(record)), 0),
    ci1PhaseCaptureOverflow: false, ci1PhaseCaptureIncomplete: true,
    ci1PhaseCaptureChildCloseUnconfirmed: false, ci1PhaseCaptureOwnedGroupCloseUnconfirmed: false
  };
  assert.deepEqual(validateOps04PublicPhaseMetrics(publicMetrics), { valid: true, failureCount: 1 });
  assert.deepEqual(validateOps04PublicPhaseMetrics({ ...publicMetrics, ci1PhaseCaptureIncomplete: false }),
    { valid: false, failureCount: 1 });
  const missingExit = { ...parentClose };
  delete missingExit.exitDisposition;
  assert.equal(validateOps04PublicPhaseMetrics({
    ...publicMetrics,
    ci1PhaseCaptures: [publicFailure, missingExit],
    ci1PhaseCaptureBytes: Buffer.byteLength(JSON.stringify(publicFailure)) + Buffer.byteLength(JSON.stringify(missingExit))
  }).valid, false, 'parent close records require a bounded exit disposition');

  const primaryError = new Error('primary fixture assertion');
  let captureCalled = false;
  let finallyRan = false;
  assert.throws(() => {
    try {
      throwAfterOps04FailureCapture(primaryError, () => { captureCalled = true; throw new Error('capture failed'); });
    } finally {
      finallyRan = true;
    }
  }, error => error === primaryError);
  assert.equal(captureCalled, true);
  assert.equal(finallyRan, true, 'diagnostic capture preserves primary error identity and finally cleanup');
});

test('OPS01_CONFIG_HOSTILE rejects hostile option/job objects before getters, traps, or serialization run', async t => {
  let getterCalls = 0;
  const accessor = {};
  Object.defineProperty(accessor, 'mode', { enumerable: true, get() { getterCalls += 1; return 'mock'; } });
  assert.throws(() => readOperationsConfig(accessor), /configuration/u);
  assert.equal(getterCalls, 0);

  let proxyTrapCalls = 0;
  const proxy = new Proxy({}, { ownKeys() { proxyTrapCalls += 1; throw new Error('canary'); } });
  assert.throws(() => readOperationsConfig(proxy), /configuration/u);
  assert.equal(proxyTrapCalls, 0);
  const revoked = Proxy.revocable({}, {});
  revoked.revoke();
  assert.throws(() => readOperationsConfig(revoked.proxy), /configuration/u);
  assert.throws(() => readOperationsConfig({ mode: 'live' }), /mock/u);

  let stringifyCalls = 0;
  const hostileConfig = { mode: 'mock', toJSON() { stringifyCalls += 1; return {}; } };
  assert.throws(() => readOperationsConfig(hostileConfig), /configuration/u);
  assert.equal(stringifyCalls, 0);
  assert.throws(() => validateLocalPlanWork({ planJson: '{"kind":"x","kind":"y"}', limitsJson: '{}' }, 'x'));
  assert.equal(INTERNAL_SCHEMA_VERSION, 6);

  const boundary = readOperationsConfig({ maxJobBytes: 65_536 });
  assert.equal(boundary.maxJobBytes, 65_536);
  assert.throws(() => readOperationsConfig({ maxJobBytes: 65_537 }), /configuration/u);

  awaitTestCase();
  function awaitTestCase() {
    // Kept synchronous so hostile values are rejected before any persistence API exists.
    const symbolConfig = { maxJobBytes: 36_864 };
    symbolConfig[Symbol('private-marker')] = 'must-not-be-reflected';
    assert.throws(() => readOperationsConfig(symbolConfig), /configuration/u);
    const cyclic = { mode: 'mock' };
    cyclic.self = cyclic;
    assert.throws(() => readOperationsConfig(cyclic), /configuration/u);
    const deeplyNested = { maxJobBytes: 36_864, extension: { nested: { value: 1 } } };
    assert.throws(() => readOperationsConfig(deeplyNested), /configuration/u);
  }

  await t.test('OPS01.C01 accepts the exact UTF-8 work limit and rejects one byte over before admission', () => {
    const exactPlan = PLAN + ' '.repeat(32_768 - Buffer.byteLength(PLAN, 'utf8'));
    assert.equal(Buffer.byteLength(exactPlan, 'utf8'), 32_768);
    assert.ok(validateLocalPlanWork({ planJson: exactPlan, limitsJson: '{}' }, 'ops01-boundary').bytes > 0);
    assert.throws(() => validateLocalPlanWork({ planJson: `${exactPlan} `, limitsJson: '{}' }, 'ops01-over-boundary'));
  });

  await t.test('OPS01.C02 rejects unsafe counters and hostile job/health records before side effects', async t => {
    for (const value of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
      assert.throws(() => readOperationsConfig({ maxQueueRecords: value }), /configuration/u);
    }
    assert.throws(() => readOperationsConfig({ unrecognizedLimit: 1 }), /configuration/u);
    const symbolic = { maxQueueRecords: 2 };
    symbolic[Symbol('private-marker')] = 'must-not-be-copied';
    assert.throws(() => readOperationsConfig(symbolic), /configuration/u);

    let workGetterCalls = 0;
    const accessorWork = {};
    Object.defineProperty(accessorWork, 'planJson', {
      enumerable: true, get() { workGetterCalls += 1; return PLAN; }
    });
    assert.throws(() => validateLocalPlanWork(accessorWork, 'ops01-hostile-work'));
    const revokedWork = Proxy.revocable({ planJson: PLAN }, {});
    revokedWork.revoke();
    assert.throws(() => validateLocalPlanWork(revokedWork.proxy, 'ops01-revoked-work'));
    let workToJsonCalls = 0;
    assert.throws(() => validateLocalPlanWork({ planJson: PLAN, toJSON() { workToJsonCalls += 1; return {}; } }, 'ops01-json-work'));
    assert.equal(workGetterCalls, 0);
    assert.equal(workToJsonCalls, 0);

    const hostileDir = tempDirectory('ah11-hostile-health-record-');
    t.after(() => hostileDir.cleanup());
    const hostileWorker = createOperationsWorker({ databasePath: hostileDir.databasePath, ownerId: 'review-worker-hostile-health' });
    t.after(async () => { if (hostileWorker.lifecycle !== 'stopped') await hostileWorker.stop('test'); });
    void hostileWorker.start();
    let healthGetterCalls = 0;
    const hostileHealth = {};
    Object.defineProperty(hostileHealth, 'heartbeatAgeMs', {
      enumerable: true, get() { healthGetterCalls += 1; return 0; }
    });
    assert.throws(() => readOperationsHealth(hostileHealth), /health input/u);
    const revokedHealth = Proxy.revocable({}, {});
    revokedHealth.revoke();
    assert.throws(() => readOperationsHealth(revokedHealth.proxy), /health input/u);
    let requestGetterCalls = 0;
    const hostileRequest = {};
    Object.defineProperty(hostileRequest, 'trustedSyntheticScope', {
      enumerable: true, get() { requestGetterCalls += 1; return { tenantId: 'forged', projectId: 'forged' }; }
    });
    assert.throws(() => hostileWorker.submitSyntheticWork(hostileRequest), /operations request/u);
    assert.equal(healthGetterCalls, 0);
    assert.equal(requestGetterCalls, 0);
    const check = new DatabaseSync(hostileDir.databasePath, { readOnly: true });
    assert.equal(Number(check.prepare('SELECT COUNT(*) AS count FROM operations_work').get().count), 0);
    assert.equal(Number(check.prepare('SELECT pending_records FROM operations_meta WHERE singleton=1').get().pending_records), 0);
    check.close();
  });
});

test('OPS02_ATOMIC_ADMISSION commits scope, workflow, queue, quota, event, and outbox together across processes', async t => {
  const temporary = tempDirectory('ah11-admission-race-');
  t.after(() => temporary.cleanup());
  const initial = openStore(temporary.databasePath);
  initial.close();

  const crash = await runChild(join(ROOT, 'tests/operations/fixtures/admission-commit-crash-child.mjs'), [
    temporary.databasePath, 'tenant-crash', 'project-crash', 'lost-ack', PLAN
  ]);
  assert.equal(crash.signal, 'SIGKILL');
  const afterCrash = openStore(temporary.databasePath);
  const recovered = admit(afterCrash, 'tenant-crash', 'project-crash', 'lost-ack');
  assert.equal(recovered.accepted, true);
  assert.equal(recovered.duplicate, true);
  assert.throws(() => admit(afterCrash, 'tenant-crash', 'project-crash', 'lost-ack', PLAN_TWO),
    error => error.code === 'IDEMPOTENCY_CONFLICT');
  const afterCrashRaw = new DatabaseSync(temporary.databasePath, { readOnly: true });
  assert.equal(Number(afterCrashRaw.prepare('SELECT COUNT(*) AS count FROM operations_work WHERE idempotency_key=?').get('lost-ack').count), 1);
  afterCrashRaw.close();
  afterCrash.close();

  const childScript = join(ROOT, 'tests/operations/fixtures/admission-child.mjs');
  const unlockedRace = tempDirectory('ah11-admission-unlocked-race-');
  t.after(() => unlockedRace.cleanup());
  const unlockedStore = openStore(unlockedRace.databasePath);
  unlockedStore.close();
  const unlockedOutFiles = Array.from({ length: 4 }, (_, index) => join(unlockedRace.directory, `race-${index}.json`));
  const unlockedProgressFiles = unlockedOutFiles.map(path => `${path}.progress.json`);
  const unlockedGatePath = join(unlockedRace.directory, 'race-release');
  const unlockedRetryGatePath = join(unlockedRace.directory, 'race-retry-release');
  const unlockedAdmissions = [];
  const unlockedReadyPids = [];
  let unlockedChildrenReaped = false;
  let unlockedRaces;
  try {
    for (let index = 0; index < unlockedOutFiles.length; index += 1) {
      const readyPath = join(unlockedRace.directory, `race-${index}.ready.json`);
      const running = startOwnedChild(childScript, [
        unlockedRace.databasePath, 'tenant-unlocked', 'project-unlocked', 'same-key', PLAN,
        unlockedOutFiles[index], readyPath, unlockedGatePath,
        unlockedProgressFiles[index], unlockedRetryGatePath
      ]);
      unlockedAdmissions.push(running);
      await waitForPath(readyPath, running.child, 5_000);
      const ready = JSON.parse(readFileSync(readyPath, 'utf8'));
      assert.equal(ready.storeOpen, true);
      assert.ok(Number.isSafeInteger(ready.pid) && ready.pid > 1);
      unlockedReadyPids.push(ready.pid);
    }
    assert.equal(new Set(unlockedReadyPids).size, unlockedOutFiles.length);
    writeFileSync(unlockedGatePath, 'release', { flag: 'wx', mode: 0o600 });
    writeFileSync(unlockedRetryGatePath, 'release', { flag: 'wx', mode: 0o600 });
    unlockedRaces = await Promise.all(unlockedAdmissions.map(running => running.closed));
    unlockedChildrenReaped = unlockedRaces.every(result => result.error || result.code !== null || result.signal !== null);
  } finally {
    if (!unlockedChildrenReaped) {
      const cleanup = await Promise.all(unlockedAdmissions.map(async running => {
        if (running.child.exitCode !== null || running.child.signalCode !== null) return true;
        return Boolean(await killOwnedChildAndReap(running.child, running.closed));
      }));
      unlockedChildrenReaped = cleanup.every(Boolean);
      assert.equal(unlockedChildrenReaped, true, 'all ordinary-race children are reaped on assertion failure');
    }
  }
  const unlockedReplies = unlockedOutFiles.map(path => {
    if (!existsSync(path)) return null;
    try { return JSON.parse(readFileSync(path, 'utf8')); } catch { return null; }
  });
  const unlockedSafeSummary = unlockedReplies.map((reply, index) => ({
    childStatus: unlockedRaces[index]?.code === 0 ? 'EXIT_0'
      : unlockedRaces[index]?.signal ? 'SIGNAL' : 'NONZERO',
    stopped: reply?.stopped === true,
    attempts: Array.isArray(reply?.attempts) ? reply.attempts.map(attempt => ({
      accepted: attempt?.accepted === true,
      duplicate: attempt?.duplicate === true ? true : attempt?.duplicate === false ? false : null,
      code: ['OPS_BACKPRESSURE', 'OPS_UNAVAILABLE'].includes(attempt?.code) ? attempt.code : null,
      reasonCode: ['STORE_BUSY', 'STORE_UNAVAILABLE', 'WORKER_NOT_ACCEPTING', 'SCOPE_CAPACITY',
        'GLOBAL_QUEUE_FULL', 'TENANT_QUEUE_FULL', 'PROJECT_QUEUE_FULL', 'RESOURCE_PROBE_UNAVAILABLE',
        'WAL_HEADROOM_LOW', 'STORE_CAPACITY', 'DISK_HEADROOM_LOW', 'TELEMETRY_BACKLOG_HIGH']
        .includes(attempt?.reasonCode) ? attempt.reasonCode : null,
      retryAfterMs: Number.isSafeInteger(attempt?.retryAfterMs) ? attempt.retryAfterMs : null
    })) : []
  }));
  const unlockedRetryChainsAreCoherent = unlockedReplies.every(reply => {
    const attempts = reply?.attempts;
    if (!Array.isArray(attempts) || attempts.length < 1 || attempts.length > 5) return false;
    return attempts.every((attempt, index) => attempt.accepted === true
      ? index === attempts.length - 1
      : index < attempts.length - 1 && attempt.code === 'OPS_BACKPRESSURE'
        && attempt.reasonCode === 'STORE_BUSY'
        && Number.isSafeInteger(attempt.retryAfterMs) && attempt.retryAfterMs >= 25 && attempt.retryAfterMs <= 5_000);
  });
  const unlockedFreshCount = unlockedReplies.filter(reply => reply?.result?.accepted === true
    && reply.result.duplicate === false).length;
  const unlockedDuplicateCount = unlockedReplies.filter(reply => reply?.result?.accepted === true
    && reply.result.duplicate === true).length;
  const unlockedRaw = new DatabaseSync(unlockedRace.databasePath, { readOnly: true });
  const unlockedRows = unlockedRaw.prepare(`SELECT job_id, workflow_id FROM operations_work
    WHERE tenant_id=? AND project_id=? AND idempotency_key=?`)
    .all('tenant-unlocked', 'project-unlocked', 'same-key');
  unlockedRaw.close();
  const unlockedOneIdentity = unlockedRows.length === 1 && unlockedReplies.every(reply =>
    reply?.result?.jobId === unlockedRows[0].job_id && reply.result.workflowId === unlockedRows[0].workflow_id);
  t.diagnostic(`OPS02 ordinary unlocked admission race ${JSON.stringify({
    children: unlockedSafeSummary,
    retryChainsAreCoherent: unlockedRetryChainsAreCoherent,
    freshReplies: unlockedFreshCount,
    duplicateReplies: unlockedDuplicateCount,
    durableMatchingRows: unlockedRows.length,
    oneStableIdentity: unlockedOneIdentity
  })}`);
  assert.ok(unlockedReplies.every(reply => reply !== null && reply.stopped === true
    && reply.outcome === 'accepted_and_stopped'), JSON.stringify(unlockedSafeSummary));
  assert.ok(unlockedRaces.every(result => result.code === 0), JSON.stringify(unlockedSafeSummary));
  assert.equal(unlockedRetryChainsAreCoherent, true, JSON.stringify(unlockedSafeSummary));
  assert.equal(unlockedFreshCount, 1, JSON.stringify(unlockedSafeSummary));
  assert.equal(unlockedDuplicateCount, 3, JSON.stringify(unlockedSafeSummary));
  assert.equal(unlockedRows.length, 1, JSON.stringify(unlockedSafeSummary));
  assert.equal(unlockedOneIdentity, true, JSON.stringify(unlockedSafeSummary));

  const outFiles = Array.from({ length: 4 }, (_, index) => join(temporary.directory, `race-${index}.json`));
  const progressFiles = outFiles.map(path => `${path}.progress.json`);
  const admissionGatePath = join(temporary.directory, 'race-release');
  const admissionRetryGatePath = join(temporary.directory, 'race-retry-release');
  const runningAdmissions = [];
  const readyPids = [];
  let admissionChildrenReaped = false;
  let admissionLock = null;
  let admissionLockHeld = false;
  let initialBusyRepliesObserved = false;
  let races;
  try {
    for (let index = 0; index < outFiles.length; index += 1) {
      const readyPath = join(temporary.directory, `race-${index}.ready.json`);
      const running = startOwnedChild(childScript, [
        temporary.databasePath, 'tenant-race', 'project-race', 'same-key', PLAN,
        outFiles[index], readyPath, admissionGatePath, progressFiles[index], admissionRetryGatePath
      ]);
      runningAdmissions.push(running);
      await waitForPath(readyPath, running.child, 5_000);
      const ready = JSON.parse(readFileSync(readyPath, 'utf8'));
      assert.equal(ready.storeOpen, true);
      assert.ok(Number.isSafeInteger(ready.pid) && ready.pid > 1);
      readyPids.push(ready.pid);
    }
    assert.equal(new Set(readyPids).size, outFiles.length);
    admissionLock = new DatabaseSync(temporary.databasePath, { timeout: 0 });
    admissionLock.exec('BEGIN IMMEDIATE');
    admissionLockHeld = true;
    writeFileSync(admissionGatePath, 'release', { flag: 'wx', mode: 0o600 });
    const firstBusy = attempt => attempt?.accepted === false
      && attempt.code === 'OPS_BACKPRESSURE' && attempt.reasonCode === 'STORE_BUSY'
      && Number.isSafeInteger(attempt.retryAfterMs) && attempt.retryAfterMs >= 25 && attempt.retryAfterMs <= 5_000;
    const initialDeadline = Date.now() + 5_000;
    while (Date.now() < initialDeadline) {
      const progress = progressFiles.map(path => {
        if (!existsSync(path)) return null;
        try { return JSON.parse(readFileSync(path, 'utf8')); } catch { return null; }
      });
      if (progress.every(item => firstBusy(item?.attempts?.[0]))) {
        initialBusyRepliesObserved = true;
        break;
      }
      await new Promise(resolveWait => setTimeout(resolveWait, 2));
    }
    admissionLock.exec('ROLLBACK');
    admissionLockHeld = false;
    admissionLock.close();
    admissionLock = null;
    writeFileSync(admissionRetryGatePath, 'release', { flag: 'wx', mode: 0o600 });
    races = await Promise.all(runningAdmissions.map(running => running.closed));
    admissionChildrenReaped = races.every(result => result.error || result.code !== null || result.signal !== null);
  } finally {
    if (admissionLockHeld) {
      try { admissionLock.exec('ROLLBACK'); } catch {}
    }
    try { admissionLock?.close(); } catch {}
    if (!admissionChildrenReaped) {
      const cleanup = await Promise.all(runningAdmissions.map(async running => {
        if (running.child.exitCode !== null || running.child.signalCode !== null) return true;
        return Boolean(await killOwnedChildAndReap(running.child, running.closed));
      }));
      admissionChildrenReaped = cleanup.every(Boolean);
      assert.equal(admissionChildrenReaped, true, 'all staged admission children are reaped on assertion failure');
    }
  }
  const replies = outFiles.map(path => {
    if (!existsSync(path)) return null;
    try { return JSON.parse(readFileSync(path, 'utf8')); } catch { return null; }
  });
  const safeReplySummary = replies.map((reply, index) => ({
    childStatus: races[index]?.code === 0 ? 'EXIT_0'
      : races[index]?.signal ? 'SIGNAL' : 'NONZERO',
    stopped: reply?.stopped === true,
    outcome: ['accepted_and_stopped', 'not_accepted_or_stop_incomplete', 'child_error'].includes(reply?.outcome)
      ? reply.outcome : 'MISSING_OR_INVALID',
    attempts: Array.isArray(reply?.attempts) ? reply.attempts.map(attempt => ({
      accepted: attempt?.accepted === true,
      duplicate: attempt?.duplicate === true ? true : attempt?.duplicate === false ? false : null,
      code: ['OPS_BACKPRESSURE', 'OPS_UNAVAILABLE'].includes(attempt?.code) ? attempt.code : null,
      reasonCode: typeof attempt?.reasonCode === 'string'
        && ['STORE_BUSY', 'STORE_UNAVAILABLE', 'WORKER_NOT_ACCEPTING', 'SCOPE_CAPACITY',
          'GLOBAL_QUEUE_FULL', 'TENANT_QUEUE_FULL', 'PROJECT_QUEUE_FULL',
          'RESOURCE_PROBE_UNAVAILABLE', 'WAL_HEADROOM_LOW', 'STORE_CAPACITY',
          'DISK_HEADROOM_LOW', 'TELEMETRY_BACKLOG_HIGH', 'OTHER_REASON'].includes(attempt.reasonCode)
        ? attempt.reasonCode : null,
      retryAfterMs: Number.isSafeInteger(attempt?.retryAfterMs)
        && attempt.retryAfterMs >= 0 && attempt.retryAfterMs <= 5_000 ? attempt.retryAfterMs : null
    })) : [],
    finalAccepted: reply?.result?.accepted === true,
    finalDuplicate: reply?.result?.duplicate === true ? true
      : reply?.result?.duplicate === false ? false : null
  }));
  const initialOutcomesAreContractual = replies.every(reply => {
    const initial = reply?.attempts?.[0];
    return initial?.accepted === true
      || (initial?.accepted === false && initial.code === 'OPS_BACKPRESSURE'
        && initial.reasonCode === 'STORE_BUSY'
        && Number.isSafeInteger(initial.retryAfterMs) && initial.retryAfterMs >= 25 && initial.retryAfterMs <= 5_000);
  });
  const retryChainsAreBounded = replies.every(reply => {
    const attempts = reply?.attempts;
    if (!Array.isArray(attempts) || attempts.length < 1 || attempts.length > 5) return false;
    return attempts.every((attempt, index) => attempt.accepted === true
      ? index === attempts.length - 1
      : index < attempts.length - 1 && attempt.code === 'OPS_BACKPRESSURE'
        && attempt.reasonCode === 'STORE_BUSY'
        && Number.isSafeInteger(attempt.retryAfterMs) && attempt.retryAfterMs >= 25 && attempt.retryAfterMs <= 5_000);
  });
  const allChildrenRetriedAfterLockRelease = replies.every(reply => {
    const attempts = reply?.attempts;
    return Array.isArray(attempts) && attempts.length >= 2
      && attempts[0].accepted === false && attempts[0].code === 'OPS_BACKPRESSURE'
      && attempts[0].reasonCode === 'STORE_BUSY'
      && attempts.at(-1).accepted === true;
  });
  const durableSnapshot = () => {
    const raw = new DatabaseSync(temporary.databasePath, { readOnly: true });
    try {
      const rows = raw.prepare(`SELECT job_id, workflow_id, state, generation, fence_token
        FROM operations_work WHERE tenant_id=? AND project_id=? AND idempotency_key=?`)
        .all('tenant-race', 'project-race', 'same-key');
      const meta = raw.prepare(`SELECT pending_records, pending_bytes, in_flight,
        reserved_event_slots, reserved_disk_bytes FROM operations_meta WHERE singleton=1`).get();
      const tenant = raw.prepare(`SELECT pending_records, pending_bytes, in_flight
        FROM operations_tenants WHERE tenant_id=?`).get('tenant-race');
      const project = raw.prepare(`SELECT pending_records, pending_bytes, in_flight
        FROM operations_projects WHERE tenant_id=? AND project_id=?`).get('tenant-race', 'project-race');
      const eventCount = rows.length === 1
        ? Number(raw.prepare('SELECT COUNT(*) AS count FROM workflow_events WHERE workflow_id=?').get(rows[0].workflow_id).count)
        : null;
      return {
        rows: rows.map(row => ({ ...row,
          generation: Number(row.generation), fence_token: Number(row.fence_token) })),
        meta: Object.fromEntries(Object.entries(meta).map(([key, value]) => [key, Number(value)])),
        tenant: tenant ? Object.fromEntries(Object.entries(tenant).map(([key, value]) => [key, Number(value)])) : null,
        project: project ? Object.fromEntries(Object.entries(project).map(([key, value]) => [key, Number(value)])) : null,
        eventCount
      };
    } finally {
      raw.close();
    }
  };
  const beforeReplay = durableSnapshot();
  const oneFreshReply = replies.filter(reply => reply?.result?.accepted === true
    && reply.result.duplicate === false).length === 1;
  const threeDuplicateReplies = replies.filter(reply => reply?.result?.accepted === true
    && reply.result.duplicate === true).length === 3;
  const oneStableIdentity = beforeReplay.rows.length === 1 && replies.every(reply =>
    reply?.result?.jobId === beforeReplay.rows[0]?.job_id
      && reply.result.workflowId === beforeReplay.rows[0]?.workflow_id);
  const controlledBusyReplyCount = replies.filter(reply =>
    reply?.attempts?.[0]?.accepted === false
      && reply.attempts[0].code === 'OPS_BACKPRESSURE'
      && reply.attempts[0].reasonCode === 'STORE_BUSY').length;
  const durableSummary = {
    matchingRows: beforeReplay.rows.length,
    pendingRecords: beforeReplay.meta.pending_records,
    pendingBytes: beforeReplay.meta.pending_bytes,
    inFlight: beforeReplay.meta.in_flight,
    tenantPendingRecords: beforeReplay.tenant?.pending_records ?? null,
    projectPendingRecords: beforeReplay.project?.pending_records ?? null,
    workflowEvents: beforeReplay.eventCount
  };
  t.diagnostic(`OPS02 admission race ${JSON.stringify({
    children: safeReplySummary,
    initialBusyRepliesObserved,
    initialOutcomesAreContractual,
    retryChainsAreBounded,
    allChildrenRetriedAfterLockRelease,
    controlledBusyReplyCount,
    oneFreshReply,
    threeDuplicateReplies,
    oneStableIdentity,
    durable: durableSummary
  })}`);
  assert.ok(replies.every(reply => reply !== null), 'every staged child writes a bounded outcome record');
  assert.ok(races.every(result => result.code === 0), JSON.stringify(safeReplySummary));
  assert.ok(replies.every(reply => reply.stopped === true && reply.outcome === 'accepted_and_stopped'), JSON.stringify(safeReplySummary));
  assert.equal(initialBusyRepliesObserved, true, JSON.stringify(safeReplySummary));
  assert.equal(initialOutcomesAreContractual, true, JSON.stringify(safeReplySummary));
  assert.equal(retryChainsAreBounded, true, JSON.stringify(safeReplySummary));
  assert.equal(controlledBusyReplyCount, outFiles.length, JSON.stringify(safeReplySummary));
  assert.equal(allChildrenRetriedAfterLockRelease, true, JSON.stringify(safeReplySummary));
  assert.equal(oneFreshReply, true, JSON.stringify(safeReplySummary));
  assert.equal(threeDuplicateReplies, true, JSON.stringify(safeReplySummary));
  assert.equal(oneStableIdentity, true, JSON.stringify(safeReplySummary));
  assert.equal(beforeReplay.rows.length, 1, JSON.stringify(safeReplySummary));
  assert.ok(beforeReplay.meta.pending_records <= 2, JSON.stringify(safeReplySummary));

  const reopenedStore = openStore(temporary.databasePath);
  const replay = admit(reopenedStore, 'tenant-race', 'project-race', 'same-key');
  reopenedStore.close();
  const afterReplay = durableSnapshot();
  const reopenStable = replay.accepted === true && replay.duplicate === true
    && replay.jobId === beforeReplay.rows[0].job_id
    && replay.workflowId === beforeReplay.rows[0].workflow_id
    && replay.state === beforeReplay.rows[0].state
    && JSON.stringify(afterReplay) === JSON.stringify(beforeReplay);
  t.diagnostic(`OPS02 admission reopen ${JSON.stringify({
    replayAccepted: replay.accepted === true,
    replayDuplicate: replay.duplicate === true,
    sameJob: replay.jobId === beforeReplay.rows[0].job_id,
    sameWorkflow: replay.workflowId === beforeReplay.rows[0].workflow_id,
    sameState: replay.state === beforeReplay.rows[0].state,
    countersStable: JSON.stringify(afterReplay.meta) === JSON.stringify(beforeReplay.meta),
    tenantCountersStable: JSON.stringify(afterReplay.tenant) === JSON.stringify(beforeReplay.tenant),
    projectCountersStable: JSON.stringify(afterReplay.project) === JSON.stringify(beforeReplay.project),
    workflowEventsStable: afterReplay.eventCount === beforeReplay.eventCount,
    matchingRowsBefore: beforeReplay.rows.length,
    matchingRowsAfter: afterReplay.rows.length
  })}`);
  assert.equal(reopenStable, true, JSON.stringify({
    ...durableSummary,
    replayAccepted: replay.accepted === true,
    replayDuplicate: replay.duplicate === true,
    countersStable: JSON.stringify(afterReplay.meta) === JSON.stringify(beforeReplay.meta),
    eventsStable: afterReplay.eventCount === beforeReplay.eventCount
  }));

  await t.test('OPS02.C01 applies exact UTF-8 scope caps to first and existing rows', () => {
    const quota = {
      maxQueueRecords: 4, maxPerTenantRecords: 2, maxPerProjectRecords: 1,
      maxQueueBytes: 65_536, maxPerTenantBytes: 32_768,
      maxPerProjectBytes: 16_384, maxEventBacklog: 1_024
    };
    const projectBoundary = tempDirectory('ah11-project-boundary-');
    t.after(() => projectBoundary.cleanup());
    const projectStore = openStore(projectBoundary.databasePath);
    const exactProject = planAtWorkByteLength(16_384, PLAN, quota);
    assert.equal(admit(projectStore, 'tenant-first', 'project-first', 'exact-project', exactProject.work.planJson, quota).accepted, true);
    const rawProject = new DatabaseSync(projectBoundary.databasePath, { readOnly: true });
    assert.equal(Number(rawProject.prepare('SELECT pending_bytes FROM operations_projects WHERE tenant_id=? AND project_id=?').get('tenant-first', 'project-first').pending_bytes), 16_384);
    rawProject.close();
    projectStore.close();

    const overProject = tempDirectory('ah11-project-over-');
    t.after(() => overProject.cleanup());
    const overProjectStore = openStore(overProject.databasePath);
    const over = planAtWorkByteLength(16_385, PLAN, quota);
    const rejected = admit(overProjectStore, 'tenant-first', 'project-first', 'over-project', over.work.planJson, quota);
    assert.equal(rejected.accepted, false);
    assert.equal(rejected.reasonCode, 'PROJECT_QUEUE_FULL');
    const afterReject = new DatabaseSync(overProject.databasePath, { readOnly: true });
    assert.equal(Number(afterReject.prepare('SELECT COUNT(*) AS count FROM operations_work').get().count), 0);
    assert.equal(Number(afterReject.prepare('SELECT reserved_disk_bytes FROM operations_meta WHERE singleton=1').get().reserved_disk_bytes), 0);
    assert.equal(Number(afterReject.prepare('SELECT COUNT(*) AS count FROM operations_projects').get().count), 0);
    afterReject.close();
    overProjectStore.close();

    const tenantBoundary = tempDirectory('ah11-tenant-boundary-');
    t.after(() => tenantBoundary.cleanup());
    const tenantStore = openStore(tenantBoundary.databasePath);
    const half = planAtWorkByteLength(16_384, PLAN, quota);
    assert.equal(admit(tenantStore, 'tenant-shared', 'project-one', 'tenant-half-one', half.work.planJson, quota).accepted, true);
    assert.equal(admit(tenantStore, 'tenant-shared', 'project-two', 'tenant-half-two', half.work.planJson, quota).accepted, true);
    const tenantOverflow = admit(tenantStore, 'tenant-shared', 'project-three', 'tenant-overflow', PLAN, quota);
    assert.equal(tenantOverflow.accepted, false);
    assert.equal(tenantOverflow.reasonCode, 'TENANT_QUEUE_FULL');
    const tenantRaw = new DatabaseSync(tenantBoundary.databasePath, { readOnly: true });
    assert.equal(Number(tenantRaw.prepare('SELECT pending_bytes FROM operations_tenants WHERE tenant_id=?').get('tenant-shared').pending_bytes), 32_768);
    assert.equal(Number(tenantRaw.prepare('SELECT COUNT(*) AS count FROM operations_work').get().count), 2);
    tenantRaw.close();
    tenantStore.close();

    const firstScopeOver = tempDirectory('ah11-tenant-first-over-');
    t.after(() => firstScopeOver.cleanup());
    const firstStore = openStore(firstScopeOver.databasePath);
    const tooLarge = planAtWorkByteLength(32_769, PLAN, quota);
    const firstReject = admit(firstStore, 'tenant-new', 'project-new', 'first-too-large', tooLarge.work.planJson, quota);
    assert.equal(firstReject.accepted, false);
    assert.equal(firstReject.reasonCode, 'TENANT_QUEUE_FULL');
    const firstRaw = new DatabaseSync(firstScopeOver.databasePath, { readOnly: true });
    assert.equal(Number(firstRaw.prepare('SELECT COUNT(*) AS count FROM operations_tenants').get().count), 0);
    assert.equal(Number(firstRaw.prepare('SELECT COUNT(*) AS count FROM operations_work').get().count), 0);
    firstRaw.close();
    firstStore.close();

    const multibyte = tempDirectory('ah11-multibyte-boundary-');
    t.after(() => multibyte.cleanup());
    const unicodePlan = JSON.stringify({
      kind: 'agent-harness.private-synthetic-plan', version: 1,
      steps: [{ id: 'text', operation: 'text.concat', dependsOn: [], args: { parts: ['é'.repeat(300)] } }]
    });
    const unicodeWork = planAtWorkByteLength(16_384, unicodePlan, quota);
    assert.ok(unicodeWork.workJson.includes('é'));
    const unicodeStore = openStore(multibyte.databasePath);
    assert.equal(admit(unicodeStore, 'tenant-unicode', 'project-unicode', 'unicode-exact', unicodeWork.work.planJson, quota).accepted, true);
    const unicodeRaw = new DatabaseSync(multibyte.databasePath, { readOnly: true });
    assert.equal(Number(unicodeRaw.prepare('SELECT work_bytes FROM operations_work').get().work_bytes), 16_384);
    unicodeRaw.close();
    unicodeStore.close();
  });

  await t.test('OPS02.C02 races distinct global/project record and byte caps across processes', async t => {
    recordOps02RaceDiagnostic({ boundary: 'case_started', passed: true });
    const capDirectory = tempDirectory('ah11-global-cap-race-');
    t.after(() => capDirectory.cleanup());
    const diagnosticKnown = createAdmissionFailureDiagnostic('admission', Object.assign(
      new Error('diagnostic-canary-message'), { code: 'STORE_BUSY' }
    ));
    assert.deepEqual(diagnosticKnown, {
      schema: 'ops-admission-failure.v1', stage: 'admission', code: 'STORE_BUSY'
    });
    const diagnosticUnknown = createAdmissionFailureDiagnostic('resource', {
      code: 'diagnostic-canary-code', message: 'diagnostic-canary-message'
    });
    assert.deepEqual(diagnosticUnknown, {
      schema: 'ops-admission-failure.v1', stage: 'resource', code: 'UNKNOWN'
    });
    const diagnosticCanaryIncluded = JSON.stringify([diagnosticKnown, diagnosticUnknown]).includes('diagnostic-canary');
    recordOps02RaceDiagnostic({ boundary: 'diagnostic_no_canary', passed: diagnosticCanaryIncluded === false });
    assert.equal(diagnosticCanaryIncluded, false);
    let proxyCodeReads = 0;
    const diagnosticProxy = new Proxy({}, {
      getOwnPropertyDescriptor() {
        proxyCodeReads += 1;
        throw new Error('diagnostic-canary-proxy');
      }
    });
    assert.deepEqual(createAdmissionFailureDiagnostic('diagnostic-canary-stage', diagnosticProxy), {
      schema: 'ops-admission-failure.v1', stage: 'unknown', code: 'UNKNOWN'
    });
    recordOps02RaceDiagnostic({ boundary: 'diagnostic_proxy_unread', passed: proxyCodeReads === 0 });
    assert.equal(proxyCodeReads, 0);

    const script = join(ROOT, 'tests/operations/fixtures/admission-store-child.mjs');
    const diagnosticFailure = await runChild(script, [
      capDirectory.databasePath, 'tenant-unused', 'project-unused', 'key-unused', PLAN,
      'diagnostic-canary-invalid-json', join(capDirectory.directory, 'diagnostic-unused.json')
    ]);
    const diagnosticFailureRecord = parseOps02AdmissionFailure(diagnosticFailure.stderr);
    const diagnosticFailureExit = classifyChildExitDisposition(diagnosticFailure);
    recordOps02RaceDiagnostic({
      boundary: 'diagnostic_child_exit', raceKind: 'diagnostic', passed: diagnosticFailure.code === 2,
      exit: diagnosticFailureExit,
      childStage: diagnosticFailureRecord?.stage ?? 'unobserved',
      childCode: diagnosticFailureRecord?.code ?? 'unobserved'
    });
    assert.equal(diagnosticFailure.code, 2);
    assert.equal(diagnosticFailure.stdout, '');
    const diagnosticLines = diagnosticFailure.stderr.trim().split('\n');
    assert.equal(diagnosticLines.length, 1);
    const diagnosticHasPrefix = diagnosticLines[0].startsWith('OPS_ADMISSION_FAILURE ');
    recordOps02RaceDiagnostic({ boundary: 'diagnostic_child_prefix', passed: diagnosticHasPrefix });
    assert.ok(diagnosticHasPrefix);
    assert.deepEqual(JSON.parse(diagnosticLines[0].slice('OPS_ADMISSION_FAILURE '.length)), {
      schema: 'ops-admission-failure.v1', stage: 'config', code: 'UNKNOWN'
    });
    const diagnosticChildCanaryIncluded = diagnosticFailure.stderr.includes('diagnostic-canary');
    recordOps02RaceDiagnostic({
      boundary: 'diagnostic_child_no_canary', passed: diagnosticChildCanaryIncluded === false
    });
    assert.equal(diagnosticChildCanaryIncluded, false);

    // These subprocess races verify quota serialization; OPS07 separately verifies typed bounded BUSY behavior.
    const config = {
      maxQueueRecords: 2, maxPerTenantRecords: 1, maxPerProjectRecords: 1,
      maxQueueBytes: 65_536, maxPerTenantBytes: 32_768, maxPerProjectBytes: 16_384,
      maxTenants: 16, maxProjects: 128, maxEventBacklog: 1_024, busyTimeoutMs: 250
    };
    const store = openStore(capDirectory.databasePath);
    const globalSeedAccepted = admit(store, 'tenant-seed', 'project-seed', 'seed', PLAN, config).accepted;
    recordOps02RaceDiagnostic({ boundary: 'seed_accept', raceKind: 'global_records', passed: globalSeedAccepted === true });
    assert.equal(globalSeedAccepted, true);
    store.close();
    const outputs = [join(capDirectory.directory, 'cap-a.json'), join(capDirectory.directory, 'cap-b.json')];
    const races = await Promise.all(outputs.map((output, index) => runChild(script, [
      capDirectory.databasePath, `tenant-cap-${index}`, `project-cap-${index}`, `cap-key-${index}`,
      PLAN, JSON.stringify(config), output
    ])));
    recordOps02ContenderResults('global_records', races);
    assert.ok(races.every(result => result.code === 0), races.map(result => result.stderr).join('\n'));
    const outcomes = outputs.map(path => JSON.parse(readFileSync(path, 'utf8')).result);
    const globalAcceptedCount = outcomes.filter(result => result.accepted).length;
    recordOps02RaceDiagnostic({ boundary: 'accepted_count', raceKind: 'global_records', passed: globalAcceptedCount === 1 });
    assert.equal(globalAcceptedCount, 1);
    const globalFullCount = outcomes.filter(result => !result.accepted && result.reasonCode === 'GLOBAL_QUEUE_FULL').length;
    recordOps02RaceDiagnostic({ boundary: 'refusal_count', raceKind: 'global_records', passed: globalFullCount === 1 });
    assert.equal(globalFullCount, 1);
    const reopened = openStore(capDirectory.databasePath);
    const raw = new DatabaseSync(capDirectory.databasePath, { readOnly: true });
    const globalPendingRecords = Number(raw.prepare('SELECT pending_records FROM operations_meta WHERE singleton=1').get().pending_records);
    recordOps02RaceDiagnostic({ boundary: 'stored_counter', raceKind: 'global_records', passed: globalPendingRecords === 2 });
    assert.equal(globalPendingRecords, 2);
    const globalWorkRows = Number(raw.prepare('SELECT COUNT(*) AS count FROM operations_work').get().count);
    recordOps02RaceDiagnostic({ boundary: 'stored_rows', raceKind: 'global_records', passed: globalWorkRows === 2 });
    assert.equal(globalWorkRows, 2);
    raw.close();
    reopened.close();

    const projectBytes = tempDirectory('ah11-project-byte-race-');
    t.after(() => projectBytes.cleanup());
    const bytePolicy = {
      maxQueueRecords: 8, maxPerTenantRecords: 4, maxPerProjectRecords: 4,
      maxQueueBytes: 65_536, maxPerTenantBytes: 32_768, maxPerProjectBytes: 16_384,
      maxTenants: 8, maxProjects: 32, maxEventBacklog: 2_048, busyTimeoutMs: 250
    };
    const seedStore = openStore(projectBytes.databasePath);
    const seedWork = planAtWorkByteLength(8_000, PLAN, bytePolicy);
    const contenderWork = planAtWorkByteLength(5_000, PLAN, bytePolicy);
    const projectByteSeedAccepted = admit(seedStore, 'tenant-byte-race', 'project-byte-race', 'byte-seed', seedWork.work.planJson, bytePolicy).accepted;
    recordOps02RaceDiagnostic({ boundary: 'seed_accept', raceKind: 'project_bytes_existing', passed: projectByteSeedAccepted === true });
    assert.equal(projectByteSeedAccepted, true);
    seedStore.close();
    const byteOutputs = [join(projectBytes.directory, 'byte-a.json'), join(projectBytes.directory, 'byte-b.json')];
    const byteScript = join(ROOT, 'tests/operations/fixtures/admission-store-child.mjs');
    const byteRace = await Promise.all(byteOutputs.map((output, index) => runChild(byteScript, [
      projectBytes.databasePath, 'tenant-byte-race', 'project-byte-race', `byte-race-${index}`,
      contenderWork.work.planJson, JSON.stringify(bytePolicy), output
    ])));
    recordOps02ContenderResults('project_bytes_existing', byteRace);
    assert.ok(byteRace.every(result => result.code === 0), byteRace.map(result => result.stderr).join('\n'));
    const byteResults = byteOutputs.map(path => JSON.parse(readFileSync(path, 'utf8')).result);
    const projectByteAcceptedCount = byteResults.filter(result => result.accepted).length;
    recordOps02RaceDiagnostic({ boundary: 'accepted_count', raceKind: 'project_bytes_existing', passed: projectByteAcceptedCount === 1 });
    assert.equal(projectByteAcceptedCount, 1);
    const projectByteFullCount = byteResults.filter(result => !result.accepted && result.reasonCode === 'PROJECT_QUEUE_FULL').length;
    recordOps02RaceDiagnostic({ boundary: 'refusal_count', raceKind: 'project_bytes_existing', passed: projectByteFullCount === 1 });
    assert.equal(projectByteFullCount, 1);
    const byteReopened = openStore(projectBytes.databasePath);
    const byteRaw = new DatabaseSync(projectBytes.databasePath, { readOnly: true });
    const projectBytePendingBytes = Number(byteRaw.prepare('SELECT pending_bytes FROM operations_projects WHERE tenant_id=? AND project_id=?')
      .get('tenant-byte-race', 'project-byte-race').pending_bytes);
    recordOps02RaceDiagnostic({ boundary: 'stored_counter', raceKind: 'project_bytes_existing', passed: projectBytePendingBytes === 13_000 });
    assert.equal(projectBytePendingBytes, 13_000);
    const projectByteWorkRows = Number(byteRaw.prepare('SELECT COUNT(*) AS count FROM operations_work').get().count);
    recordOps02RaceDiagnostic({ boundary: 'stored_rows', raceKind: 'project_bytes_existing', passed: projectByteWorkRows === 2 });
    assert.equal(projectByteWorkRows, 2);
    byteRaw.close();
    byteReopened.close();

    const firstScopeBytes = tempDirectory('ah11-first-project-byte-race-');
    t.after(() => firstScopeBytes.cleanup());
    const firstBytePolicy = {
      maxQueueRecords: 8, maxPerTenantRecords: 4, maxPerProjectRecords: 4,
      maxQueueBytes: 65_536, maxPerTenantBytes: 32_768, maxPerProjectBytes: 16_384,
      maxTenants: 8, maxProjects: 32, maxEventBacklog: 2_048, busyTimeoutMs: 250
    };
    const firstByteInit = openStore(firstScopeBytes.databasePath);
    firstByteInit.close();
    const firstByteWork = planAtWorkByteLength(10_000, PLAN, firstBytePolicy);
    const firstByteOutputs = [join(firstScopeBytes.directory, 'first-a.json'), join(firstScopeBytes.directory, 'first-b.json')];
    const firstByteRace = await Promise.all(firstByteOutputs.map((output, index) => runChild(byteScript, [
      firstScopeBytes.databasePath, 'tenant-new-byte-race', 'project-new-byte-race', `first-byte-${index}`,
      firstByteWork.work.planJson, JSON.stringify(firstBytePolicy), output
    ])));
    recordOps02ContenderResults('project_bytes_new', firstByteRace);
    assert.ok(firstByteRace.every(result => result.code === 0), firstByteRace.map(result => result.stderr).join('\n'));
    const firstByteResults = firstByteOutputs.map(path => JSON.parse(readFileSync(path, 'utf8')).result);
    const firstByteAcceptedCount = firstByteResults.filter(result => result.accepted).length;
    recordOps02RaceDiagnostic({ boundary: 'accepted_count', raceKind: 'project_bytes_new', passed: firstByteAcceptedCount === 1 });
    assert.equal(firstByteAcceptedCount, 1);
    const firstByteFullCount = firstByteResults.filter(result => !result.accepted && result.reasonCode === 'PROJECT_QUEUE_FULL').length;
    recordOps02RaceDiagnostic({ boundary: 'refusal_count', raceKind: 'project_bytes_new', passed: firstByteFullCount === 1 });
    assert.equal(firstByteFullCount, 1);
    const firstByteDb = openStore(firstScopeBytes.databasePath);
    const firstByteSnapshot = new DatabaseSync(firstScopeBytes.databasePath, { readOnly: true });
    const firstBytePendingBytes = Number(firstByteSnapshot.prepare('SELECT pending_bytes FROM operations_projects WHERE tenant_id=? AND project_id=?')
      .get('tenant-new-byte-race', 'project-new-byte-race').pending_bytes);
    recordOps02RaceDiagnostic({ boundary: 'stored_counter', raceKind: 'project_bytes_new', passed: firstBytePendingBytes === 10_000 });
    assert.equal(firstBytePendingBytes, 10_000);
    const firstByteWorkRows = Number(firstByteSnapshot.prepare('SELECT COUNT(*) AS count FROM operations_work').get().count);
    recordOps02RaceDiagnostic({ boundary: 'stored_rows', raceKind: 'project_bytes_new', passed: firstByteWorkRows === 1 });
    assert.equal(firstByteWorkRows, 1);
    const firstByteHasReservation = Number(firstByteSnapshot.prepare('SELECT reserved_disk_bytes FROM operations_meta WHERE singleton=1').get().reserved_disk_bytes) > 0;
    recordOps02RaceDiagnostic({ boundary: 'reservation', raceKind: 'project_bytes_new', passed: firstByteHasReservation });
    assert.equal(firstByteHasReservation, true);
    firstByteSnapshot.close();
    firstByteDb.close();

    const projectRecords = tempDirectory('ah11-project-record-race-');
    t.after(() => projectRecords.cleanup());
    const recordPolicy = {
      maxQueueRecords: 8, maxPerTenantRecords: 4, maxPerProjectRecords: 2,
      maxQueueBytes: 65_536, maxPerTenantBytes: 32_768, maxPerProjectBytes: 16_384,
      maxTenants: 8, maxProjects: 32, maxEventBacklog: 2_048, busyTimeoutMs: 250
    };
    const recordStore = openStore(projectRecords.databasePath);
    const recordSeedAccepted = admit(recordStore, 'tenant-record-race', 'project-record-race', 'record-seed', PLAN, recordPolicy).accepted;
    recordOps02RaceDiagnostic({ boundary: 'seed_accept', raceKind: 'project_records', passed: recordSeedAccepted === true });
    assert.equal(recordSeedAccepted, true);
    recordStore.close();
    const recordOutputs = [join(projectRecords.directory, 'record-a.json'), join(projectRecords.directory, 'record-b.json')];
    const recordRace = await Promise.all(recordOutputs.map((output, index) => runChild(byteScript, [
      projectRecords.databasePath, 'tenant-record-race', 'project-record-race', `project-record-${index}`,
      PLAN, JSON.stringify(recordPolicy), output
    ])));
    recordOps02ContenderResults('project_records', recordRace);
    assert.ok(recordRace.every(result => result.code === 0), recordRace.map(result => result.stderr).join('\n'));
    const recordResults = recordOutputs.map(path => JSON.parse(readFileSync(path, 'utf8')).result);
    const projectRecordAcceptedCount = recordResults.filter(result => result.accepted).length;
    recordOps02RaceDiagnostic({ boundary: 'accepted_count', raceKind: 'project_records', passed: projectRecordAcceptedCount === 1 });
    assert.equal(projectRecordAcceptedCount, 1);
    const projectRecordFullCount = recordResults.filter(result => !result.accepted && result.reasonCode === 'PROJECT_QUEUE_FULL').length;
    recordOps02RaceDiagnostic({ boundary: 'refusal_count', raceKind: 'project_records', passed: projectRecordFullCount === 1 });
    assert.equal(projectRecordFullCount, 1);
    const recordReopened = openStore(projectRecords.databasePath);
    const recordSnapshot = new DatabaseSync(projectRecords.databasePath, { readOnly: true });
    const projectRecordPendingRecords = Number(recordSnapshot.prepare('SELECT pending_records FROM operations_projects WHERE tenant_id=? AND project_id=?')
      .get('tenant-record-race', 'project-record-race').pending_records);
    recordOps02RaceDiagnostic({ boundary: 'stored_counter', raceKind: 'project_records', passed: projectRecordPendingRecords === 2 });
    assert.equal(projectRecordPendingRecords, 2);
    const projectRecordWorkRows = Number(recordSnapshot.prepare('SELECT COUNT(*) AS count FROM operations_work').get().count);
    recordOps02RaceDiagnostic({ boundary: 'stored_rows', raceKind: 'project_records', passed: projectRecordWorkRows === 2 });
    assert.equal(projectRecordWorkRows, 2);
    recordSnapshot.close();
    recordReopened.close();

    const globalBytes = tempDirectory('ah11-global-byte-race-');
    t.after(() => globalBytes.cleanup());
    const globalBytePolicy = {
      maxQueueRecords: 8, maxPerTenantRecords: 4, maxPerProjectRecords: 2,
      maxQueueBytes: 65_536, maxPerTenantBytes: 32_768, maxPerProjectBytes: 16_384,
      maxTenants: 8, maxProjects: 32, maxEventBacklog: 2_048, busyTimeoutMs: 250
    };
    const globalByteStore = openStore(globalBytes.databasePath);
    const seedGlobalWork = planAtWorkByteLength(16_000, PLAN, globalBytePolicy);
    for (const index of [0, 1, 2]) {
      const accepted = admit(globalByteStore, `tenant-global-byte-${index}`, `project-global-byte-${index}`,
        `global-byte-seed-${index}`, seedGlobalWork.work.planJson, globalBytePolicy).accepted;
      recordOps02RaceDiagnostic({ boundary: 'seed_accept', raceKind: 'global_bytes', passed: accepted === true });
      assert.equal(accepted, true);
    }
    globalByteStore.close();
    const globalContender = planAtWorkByteLength(10_000, PLAN, globalBytePolicy);
    const globalOutputs = [join(globalBytes.directory, 'global-a.json'), join(globalBytes.directory, 'global-b.json')];
    const globalRace = await Promise.all(globalOutputs.map((output, index) => runChild(byteScript, [
      globalBytes.databasePath, `tenant-global-contender-${index}`, `project-global-contender-${index}`,
      `global-byte-contender-${index}`, globalContender.work.planJson, JSON.stringify(globalBytePolicy), output
    ])));
    recordOps02ContenderResults('global_bytes', globalRace);
    assert.ok(globalRace.every(result => result.code === 0), globalRace.map(result => result.stderr).join('\n'));
    const globalResults = globalOutputs.map(path => JSON.parse(readFileSync(path, 'utf8')).result);
    const globalByteAcceptedCount = globalResults.filter(result => result.accepted).length;
    recordOps02RaceDiagnostic({ boundary: 'accepted_count', raceKind: 'global_bytes', passed: globalByteAcceptedCount === 1 });
    assert.equal(globalByteAcceptedCount, 1);
    const globalByteFullCount = globalResults.filter(result => !result.accepted && result.reasonCode === 'GLOBAL_QUEUE_FULL').length;
    recordOps02RaceDiagnostic({ boundary: 'refusal_count', raceKind: 'global_bytes', passed: globalByteFullCount === 1 });
    assert.equal(globalByteFullCount, 1);
    const globalReopened = openStore(globalBytes.databasePath);
    const globalSnapshot = new DatabaseSync(globalBytes.databasePath, { readOnly: true });
    const globalPendingBytes = Number(globalSnapshot.prepare('SELECT pending_bytes FROM operations_meta WHERE singleton=1').get().pending_bytes);
    recordOps02RaceDiagnostic({ boundary: 'stored_counter', raceKind: 'global_bytes', passed: globalPendingBytes === 58_000 });
    assert.equal(globalPendingBytes, 58_000);
    const globalByteWorkRows = Number(globalSnapshot.prepare('SELECT COUNT(*) AS count FROM operations_work').get().count);
    recordOps02RaceDiagnostic({ boundary: 'stored_rows', raceKind: 'global_bytes', passed: globalByteWorkRows === 4 });
    assert.equal(globalByteWorkRows, 4);
    globalSnapshot.close();
    globalReopened.close();
  });

  await t.test('OPS02.C03 rolls admission and local completion back at each durable write boundary', async t => {
    const admissionPoints = [
      ['operations_tenants', 'INSERT', ''], ['operations_projects', 'INSERT', ''],
      ['workflows', 'INSERT', ''], ['workflow_checkpoints', 'INSERT', ''],
      ['operations_work', 'INSERT', ''],
      ['operations_meta', 'UPDATE', ' WHEN NEW.pending_records > OLD.pending_records'],
      ['operations_tenants', 'UPDATE', ''], ['operations_projects', 'UPDATE', ''],
      ['workflow_events', 'INSERT', ''], ['outbox', 'INSERT', '']
    ];
    for (const [table, action, condition] of admissionPoints) {
      const item = tempDirectory('ah11-admission-rollback-');
      t.after(() => item.cleanup());
      const store = openStore(item.databasePath);
      const triggerDb = new DatabaseSync(item.databasePath);
      triggerDb.exec(`CREATE TRIGGER injected_failure BEFORE ${action} ON ${table}${condition} BEGIN SELECT RAISE(ABORT, 'injected durable write failure'); END`);
      assert.throws(() => admit(store, 'tenant-rollback', 'project-rollback', 'rollback', PLAN));
      triggerDb.close();
      store.close();
      const reopened = openStore(item.databasePath);
      const raw = new DatabaseSync(item.databasePath, { readOnly: true });
      for (const tableName of ['operations_tenants', 'operations_projects', 'operations_work', 'workflows', 'workflow_checkpoints', 'workflow_events', 'outbox']) {
        assert.equal(Number(raw.prepare(`SELECT COUNT(*) AS count FROM ${tableName}`).get().count), 0, `${table}.${action} left partial ${tableName} state`);
      }
      assert.deepEqual({ ...raw.prepare('SELECT pending_records, pending_bytes, in_flight, reserved_event_slots, reserved_disk_bytes FROM operations_meta WHERE singleton=1').get() }, {
        pending_records: 0, pending_bytes: 0, in_flight: 0, reserved_event_slots: 0, reserved_disk_bytes: 0
      });
      raw.close();
      reopened.close();
    }

    const completionPoints = [
      ['workflows', 'UPDATE', ' WHEN NEW.checkpoint_sequence = 3'],
      ['workflow_checkpoints', 'INSERT', ' WHEN NEW.sequence = 3'],
      ['workflow_events', 'INSERT', " WHEN NEW.event_type = 'private_local_step_completed'"],
      ['outbox', 'INSERT', " WHEN (SELECT event_type FROM workflow_events WHERE sequence=NEW.event_sequence) = 'private_local_step_completed'"]
    ];
    for (const [table, action, condition] of completionPoints) {
      const item = tempDirectory('ah11-completion-rollback-');
      t.after(() => item.cleanup());
      const store = openStore(item.databasePath);
      const accepted = admit(store, 'tenant-completion', 'project-completion', `completion-${table}`, PLAN);
      const ownerId = 'review-worker';
      const claimed = store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
        ownerId, policy: operationsStoragePolicy(readOperationsConfig())
      });
      assert.equal(claimed.claim.jobId, accepted.jobId);
      const currentClaim = store._operationsReadClaim(OPERATIONS_STORAGE_CAPABILITY, {
        jobId: accepted.jobId, ownerId, generation: claimed.claim.generation, fence: claimed.claim.fence
      });
      const triggerDb = new DatabaseSync(item.databasePath);
      triggerDb.exec(`CREATE TRIGGER injected_failure BEFORE ${action} ON ${table}${condition} BEGIN SELECT RAISE(ABORT, 'injected completion write failure'); END`);
      const result = await runBoundedLocalPlan({
        store, workflowId: accepted.workflowId, ownerId, fence: claimed.claim.fence,
        planJson: currentClaim.work.planJson, limitsJson: currentClaim.work.limitsJson
      });
      assert.equal(result.status, 'stopped');
      triggerDb.close();
      store.close();
      const reopened = openStore(item.databasePath);
      const workflow = reopened.getWorkflow(accepted.workflowId);
      const checkpoint = reopened.getCheckpoint(accepted.workflowId);
      assert.equal(workflow.revision, 2);
      assert.equal(workflow.checkpointSequence, 2);
      assert.equal(checkpoint.sequence, 2);
      assert.equal(checkpoint.data.kind, 'agent-harness.private-bounded-local-checkpoint');
      const raw = new DatabaseSync(item.databasePath, { readOnly: true });
      assert.equal(Number(raw.prepare("SELECT COUNT(*) AS count FROM workflow_events WHERE event_type='private_local_step_completed'").get().count), 0);
      assert.equal(Number(raw.prepare('SELECT in_flight FROM operations_meta WHERE singleton=1').get().in_flight), 1);
      raw.close();
      reopened._operationsFinish(OPERATIONS_STORAGE_CAPABILITY, {
        jobId: accepted.jobId, ownerId, generation: claimed.claim.generation,
        fence: claimed.claim.fence, outcome: 'stopped'
      });
      reopened.close();
    }
  });

  await t.test('OPS02.C05 rolls terminal queue, quota, workflow, event, and outbox writes back together', async t => {
    const finishPoints = [
      ['workflows', 'UPDATE', " WHEN NEW.state = 'succeeded'"],
      ['workflow_events', 'INSERT', " WHEN NEW.event_type = 'operations_work_completed'"],
      ['outbox', 'INSERT', " WHEN (SELECT event_type FROM workflow_events WHERE sequence=NEW.event_sequence) = 'operations_work_completed'"],
      ['operations_work', 'UPDATE', " WHEN NEW.state = 'completed'"],
      ['operations_meta', 'UPDATE', ' WHEN NEW.pending_records < OLD.pending_records'],
      ['operations_tenants', 'UPDATE', ' WHEN NEW.pending_records < OLD.pending_records'],
      ['operations_projects', 'UPDATE', ' WHEN NEW.pending_records < OLD.pending_records']
    ];
    for (const [table, action, condition] of finishPoints) {
      const item = tempDirectory('ah11-finish-rollback-');
      t.after(() => item.cleanup());
      const store = openStore(item.databasePath);
      const accepted = admit(store, 'tenant-finish', 'project-finish', `finish-${table}`, PLAN);
      const ownerId = 'review-worker-finish';
      const policy = operationsStoragePolicy(readOperationsConfig());
      const claim = store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, { ownerId, policy }).claim;
      const current = store._operationsReadClaim(OPERATIONS_STORAGE_CAPABILITY, {
        jobId: accepted.jobId, ownerId, generation: claim.generation, fence: claim.fence
      });
      const result = await runBoundedLocalPlan({
        store, workflowId: accepted.workflowId, ownerId, fence: claim.fence,
        planJson: current.work.planJson, limitsJson: current.work.limitsJson
      });
      assert.equal(result.status, 'completed');
      const snapshot = () => {
        const raw = new DatabaseSync(item.databasePath, { readOnly: true });
        const value = {
          workflow: raw.prepare('SELECT state, revision, fence_token, lease_owner, lease_expires_ms, cancellation_requested, checkpoint_sequence FROM workflows WHERE workflow_id=?').get(accepted.workflowId),
          checkpoint: raw.prepare('SELECT sequence, payload_json, sha256 FROM workflow_checkpoints WHERE workflow_id=? ORDER BY sequence DESC LIMIT 1').get(accepted.workflowId),
          work: raw.prepare('SELECT state, worker_id, generation, fence_token, runner_pid, reserved_event_slots, reserved_disk_bytes FROM operations_work WHERE job_id=?').get(accepted.jobId),
          meta: raw.prepare('SELECT pending_records, pending_bytes, in_flight, reserved_event_slots, reserved_disk_bytes FROM operations_meta WHERE singleton=1').get(),
          tenant: raw.prepare('SELECT pending_records, pending_bytes, in_flight FROM operations_tenants WHERE tenant_id=?').get('tenant-finish'),
          project: raw.prepare('SELECT pending_records, pending_bytes, in_flight FROM operations_projects WHERE tenant_id=? AND project_id=?').get('tenant-finish', 'project-finish'),
          events: Number(raw.prepare('SELECT COUNT(*) AS count FROM workflow_events WHERE workflow_id=?').get(accepted.workflowId).count),
          outbox: Number(raw.prepare('SELECT COUNT(*) AS count FROM outbox').get().count)
        };
        raw.close();
        return JSON.parse(JSON.stringify(value));
      };
      const before = snapshot();
      const triggerDb = new DatabaseSync(item.databasePath);
      triggerDb.exec(`CREATE TRIGGER injected_finish_failure BEFORE ${action} ON ${table}${condition} BEGIN SELECT RAISE(ABORT, 'injected terminal write failure'); END`);
      assert.throws(() => store._operationsFinish(OPERATIONS_STORAGE_CAPABILITY, {
        jobId: accepted.jobId, ownerId, generation: claim.generation,
        fence: claim.fence, outcome: 'completed', childExited: true
      }));
      triggerDb.close();
      store.close();
      const reopened = openStore(item.databasePath);
      assert.deepEqual(snapshot(), before, `${table}.${action} did not restore the exact pre-finish durable snapshot`);
      reopened.close();
    }
  });

  await t.test('OPS02.C04 rejects a second worker with a different quota policy before it can share the store', async t => {
    const item = tempDirectory('ah11-policy-mismatch-');
    t.after(() => item.cleanup());
    const firstWorker = createOperationsWorker({ databasePath: item.databasePath, ownerId: 'review-worker-policy-a' });
    t.after(async () => { if (firstWorker.lifecycle !== 'stopped') await firstWorker.stop('test'); });
    assert.throws(() => createOperationsWorker({
      databasePath: item.databasePath,
      ownerId: 'review-worker-policy-b',
      config: { maxInFlightTenant: 1 }
    }), error => error.code === 'INVALID_INPUT');
    assert.equal(firstWorker.lifecycle, 'starting');
    const stopped = await firstWorker.stop('test');
    assert.equal(stopped.stopped, true);
    const raw = new DatabaseSync(item.databasePath, { readOnly: true });
    assert.equal(raw.prepare('SELECT policy_json FROM operations_meta WHERE singleton=1').get().policy_json !== null, true);
    assert.equal(Number(raw.prepare('SELECT COUNT(*) AS count FROM operations_work').get().count), 0);
    raw.close();
  });


});

test('OPS03_FAIRNESS_BACKPRESSURE protects a second tenant and persists hierarchical turns across claims', async t => {
  const temporary = tempDirectory('ah11-fairness-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath);
  t.after(() => store.close());
  const config = readOperationsConfig();
  const scopesByWorkflow = new Map();
  for (let index = 0; index < 8; index += 1) {
    const a1 = admit(store, 'tenant-a', 'project-a1', `a1-${index}`);
    const a2 = admit(store, 'tenant-a', 'project-a2', `a2-${index}`);
    assert.equal(a1.accepted, true);
    assert.equal(a2.accepted, true);
    scopesByWorkflow.set(a1.workflowId, 'tenant-a/project-a1');
    scopesByWorkflow.set(a2.workflowId, 'tenant-a/project-a2');
  }
  const blockedA = admit(store, 'tenant-a', 'project-a1', 'a1-overflow');
  assert.deepEqual({ accepted: blockedA.accepted, code: blockedA.code, reasonCode: blockedA.reasonCode }, {
    accepted: false, code: 'OPS_BACKPRESSURE', reasonCode: 'TENANT_QUEUE_FULL'
  });
  const secondTenant = admit(store, 'tenant-b', 'project-b1', 'b1-first');
  assert.equal(secondTenant.accepted, true);
  scopesByWorkflow.set(secondTenant.workflowId, 'tenant-b/project-b1');

  const turns = [];
  for (let turn = 0; turn < 4; turn += 1) {
    const selected = store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: `ops-worker:fair-${turn}`,
      policy: operationsStoragePolicy(config)
    });
    assert.ok(selected?.claim);
    turns.push(scopesByWorkflow.get(selected.claim.workflowId));
    const row = store._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'tenant-a', projectId: 'project-a1', jobId: 'absent'
    });
    assert.equal(row, null);
    store._operationsFinish(OPERATIONS_STORAGE_CAPABILITY, {
      jobId: selected.claim.jobId, ownerId: selected.claim.ownerId,
      generation: selected.claim.generation, fence: selected.claim.fence, outcome: 'shutdown'
    });
  }
  assert.deepEqual(turns, ['tenant-a/project-a1', 'tenant-b/project-b1', 'tenant-a/project-a2', 'tenant-b/project-b1']);

  const small = readOperationsConfig({
    maxQueueBytes: 65_536, maxPerTenantBytes: 32_768, maxPerProjectBytes: 16_384,
    maxJobBytes: 36_864
  });
  const largePlan = PLAN + ' '.repeat(15_000);
  const byteStore = openStore(join(temporary.directory, 'bytes.sqlite'));
  t.after(() => byteStore.close());
  assert.equal(admit(byteStore, 'tenant-bytes', 'project-bytes', 'bytes-1', largePlan, small).accepted, true);
  const byteBackpressure = admit(byteStore, 'tenant-bytes', 'project-bytes', 'bytes-2', largePlan, small);
  assert.equal(byteBackpressure.code, 'OPS_BACKPRESSURE');
  assert.equal(byteBackpressure.reasonCode, 'PROJECT_QUEUE_FULL');

  await t.test('OPS03.C01 continuously refills both tenants, skips a blocked tenant head, and bounds hierarchical dispatch order', async t => {
    const loadDir = tempDirectory('ah11-fairness-continuous-');
    t.after(() => loadDir.cleanup());
    const limits = readOperationsConfig({
      workerCount: 2, maxQueueRecords: 16, maxPerTenantRecords: 8, maxPerProjectRecords: 4,
      maxQueueBytes: 1_048_576, maxPerTenantBytes: 524_288, maxPerProjectBytes: 262_144,
      maxTenants: 8, maxProjects: 32, maxInFlightGlobal: 2,
      maxInFlightTenant: 1, maxInFlightProject: 1
    });
    const loadStore = openStore(loadDir.databasePath);
    t.after(() => loadStore.close());
    const policy = operationsStoragePolicy(limits);
    const scopeNames = new Map();
    const add = (tenantId, projectId, key) => {
      const accepted = admit(loadStore, tenantId, projectId, key, PLAN, limits);
      assert.equal(accepted.accepted, true, `${tenantId}/${projectId}/${key}`);
      scopeNames.set(accepted.workflowId, `${tenantId}/${projectId}`);
      return accepted;
    };
    for (let index = 0; index < 4; index += 1) {
      add('tenant-a', 'project-a1', `flood-a1-${index}`);
      add('tenant-a', 'project-a2', `flood-a2-${index}`);
      add('tenant-b', 'project-b1', `flood-b-${index}`);
    }
    const first = loadStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, { ownerId: 'review-worker-a', policy }).claim;
    assert.equal(scopeNames.get(first.workflowId), 'tenant-a/project-a1');
    const second = loadStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, { ownerId: 'review-worker-b', policy }).claim;
    assert.equal(scopeNames.get(second.workflowId), 'tenant-b/project-b1');
    assert.equal(loadStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, { ownerId: 'review-worker-c', policy }), null);
    for (const claim of [first, second]) {
      loadStore._operationsFinish(OPERATIONS_STORAGE_CAPABILITY, {
        jobId: claim.jobId, ownerId: claim.ownerId, generation: claim.generation,
        fence: claim.fence, outcome: 'shutdown'
      });
    }

    const order = [];
    let admissionNumber = 0;
    for (let turn = 0; turn < 24; turn += 1) {
      const selected = loadStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
        ownerId: `review-worker-fair-${turn}`, policy
      }).claim;
      assert.ok(selected);
      const scopeName = scopeNames.get(selected.workflowId);
      order.push(scopeName);
      const acceptedWork = loadStore._operationsReadClaim(OPERATIONS_STORAGE_CAPABILITY, {
        jobId: selected.jobId, ownerId: selected.ownerId,
        generation: selected.generation, fence: selected.fence
      });
      const result = await runBoundedLocalPlan({
        store: loadStore, workflowId: selected.workflowId, ownerId: selected.ownerId,
        fence: selected.fence, planJson: acceptedWork.work.planJson, limitsJson: acceptedWork.work.limitsJson
      });
      assert.equal(result.status, 'completed');
      loadStore._operationsFinish(OPERATIONS_STORAGE_CAPABILITY, {
        jobId: selected.jobId, ownerId: selected.ownerId, generation: selected.generation,
        fence: selected.fence, outcome: 'completed', childExited: true
      });
      const [tenantId, projectId] = scopeName.split('/');
      add(tenantId, projectId, `flood-refill-${admissionNumber++}`);
    }
    assert.equal(order.filter(name => name.startsWith('tenant-a/')).length, 12);
    assert.equal(order.filter(name => name.startsWith('tenant-b/')).length, 12);
    for (let index = 0; index < order.length; index += 1) {
      assert.notEqual(order[index], order[index + 1], `The dispatcher exceeded its one-turn tenant gap at ${index}.`);
    }
    const aProjects = order.filter(name => name.startsWith('tenant-a/'));
    assert.deepEqual(aProjects.slice(0, 6), [
      'tenant-a/project-a2', 'tenant-a/project-a1', 'tenant-a/project-a2',
      'tenant-a/project-a1', 'tenant-a/project-a2', 'tenant-a/project-a1'
    ]);
    for (let drain = 0; drain < 12; drain += 1) {
      const selected = loadStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
        ownerId: `review-worker-drain-${drain}`, policy
      }).claim;
      assert.ok(selected);
      const acceptedWork = loadStore._operationsReadClaim(OPERATIONS_STORAGE_CAPABILITY, {
        jobId: selected.jobId, ownerId: selected.ownerId,
        generation: selected.generation, fence: selected.fence
      });
      const result = await runBoundedLocalPlan({
        store: loadStore, workflowId: selected.workflowId, ownerId: selected.ownerId,
        fence: selected.fence, planJson: acceptedWork.work.planJson, limitsJson: acceptedWork.work.limitsJson
      });
      assert.equal(result.status, 'completed');
      loadStore._operationsFinish(OPERATIONS_STORAGE_CAPABILITY, {
        jobId: selected.jobId, ownerId: selected.ownerId, generation: selected.generation,
        fence: selected.fence, outcome: 'completed', childExited: true
      });
    }
    assert.equal(loadStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, { ownerId: 'review-worker-empty', policy }), null);
    add('tenant-a', 'project-a1', 'after-empty-a');
    add('tenant-b', 'project-b1', 'after-empty-b');
    assert.ok(loadStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, { ownerId: 'review-worker-refill', policy }).claim);
  });

  await t.test('OPS03.C02 scope metadata caps stay explicit and do not claim to provide fair admission', () => {
    const metadataDir = tempDirectory('ah11-scope-metadata-cap-');
    t.after(() => metadataDir.cleanup());
    const capPolicy = readOperationsConfig({
      maxQueueRecords: 2, maxPerTenantRecords: 1, maxPerProjectRecords: 1,
      maxQueueBytes: 65_536, maxPerTenantBytes: 32_768, maxPerProjectBytes: 16_384,
      maxTenants: 2, maxProjects: 2
    });
    const metadataStore = openStore(metadataDir.databasePath);
    t.after(() => metadataStore.close());
    assert.equal(admit(metadataStore, 'tenant-meta-a', 'project-meta-a', 'meta-a', PLAN, capPolicy).accepted, true);
    assert.equal(admit(metadataStore, 'tenant-meta-b', 'project-meta-b', 'meta-b', PLAN, capPolicy).accepted, true);
    const tenantLimit = admit(metadataStore, 'tenant-meta-c', 'project-meta-c', 'meta-c', PLAN, capPolicy);
    assert.deepEqual({ accepted: tenantLimit.accepted, code: tenantLimit.code, reasonCode: tenantLimit.reasonCode }, {
      accepted: false, code: 'OPS_BACKPRESSURE', reasonCode: 'SCOPE_CAPACITY'
    });
    const projectLimit = admit(metadataStore, 'tenant-meta-a', 'project-meta-c', 'meta-project', PLAN, capPolicy);
    assert.deepEqual({ accepted: projectLimit.accepted, code: projectLimit.code, reasonCode: projectLimit.reasonCode }, {
      accepted: false, code: 'OPS_BACKPRESSURE', reasonCode: 'SCOPE_CAPACITY'
    });
    const rawMetadata = new DatabaseSync(metadataDir.databasePath, { readOnly: true });
    assert.equal(Number(rawMetadata.prepare('SELECT COUNT(*) AS count FROM operations_tenants').get().count), 2);
    assert.equal(Number(rawMetadata.prepare('SELECT COUNT(*) AS count FROM operations_projects').get().count), 2);
    rawMetadata.close();
  });

  await t.test('OPS03.C03 a local telemetry callback can reenter worker APIs without nesting admission or shutdown', async t => {
    const callbackDir = tempDirectory('ah11-fairness-reentrant-callback-');
    t.after(() => callbackDir.cleanup());
    const callbackStore = openStore(callbackDir.databasePath);
    t.after(() => callbackStore.close());
    const tokenizer = createHmacTokenizer(Buffer.alloc(32, 31));
    t.after(() => tokenizer.destroy());
    let callbackWorker;
    let callbackLoop;
    let nestedStop;
    let callbackAdmission;
    let callbackHealth;
    let sameStartLoop = false;
    let callbackCount = 0;
    const exporter = createTelemetryExporter({
      store: callbackStore,
      tokenizer,
      sink: async records => {
        callbackCount += 1;
        if (callbackCount === 1) {
          callbackAdmission = callbackWorker.submitSyntheticWork({
            trustedSyntheticScope: { tenantId: 'reentrant-tenant', projectId: 'reentrant-project' },
            idempotencyKey: 'reentrant-admission', planJson: PLAN
          });
          callbackHealth = callbackWorker.health();
          sameStartLoop = callbackWorker.start() === callbackLoop;
          nestedStop = callbackWorker.stop('signal');
        }
        return { accepted: true, acceptedCount: records.length };
      },
      limits: { queueRecords: 8, queueBytes: 8192, batchSize: 8, maxAttempts: 1,
        retryDelayMs: 0, sinkTimeoutMs: 100, maxBatchesPerFlush: 2, maxPolicyDropsPerFlush: 2 }
    });
    callbackWorker = createOperationsWorker({
      databasePath: callbackDir.databasePath, ownerId: 'review-worker-reentrant-callback', telemetry: exporter
    });
    t.after(async () => { if (callbackWorker.lifecycle !== 'stopped') await callbackWorker.stop('test'); });
    callbackLoop = callbackWorker.start();
    const first = callbackWorker.submitSyntheticWork({
      trustedSyntheticScope: { tenantId: 'reentrant-tenant', projectId: 'reentrant-project' },
      idempotencyKey: 'reentrant-seed', planJson: PLAN
    });
    assert.equal(first.accepted, true);
    assert.equal((await waitForStatus(callbackWorker,
      { tenantId: 'reentrant-tenant', projectId: 'reentrant-project' }, first.jobId, 'completed')).state, 'completed');
    const flush = exporter.flush();
    const flushResult = await flush;
    assert.equal(callbackCount, 1);
    assert.equal(flushResult.lastErrorCode, null);
    assert.equal(callbackAdmission?.accepted, true);
    assert.equal(sameStartLoop, true);
    assert.equal(callbackHealth?.state, 'accepting');
    assert.ok(nestedStop instanceof Promise);
    const stopResult = await nestedStop;
    await callbackLoop;
    assert.equal(stopResult.stopped, true);
    const reopened = openStore(callbackDir.databasePath);
    const acceptedDuringCallback = reopened._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'reentrant-tenant', projectId: 'reentrant-project', jobId: callbackAdmission.jobId
    });
    assert.equal(acceptedDuringCallback.state, 'queued');
    const raw = new DatabaseSync(callbackDir.databasePath, { readOnly: true });
    assert.equal(Number(raw.prepare('SELECT pending_records FROM operations_meta WHERE singleton=1').get().pending_records), 1);
    assert.equal(Number(raw.prepare('SELECT in_flight FROM operations_meta WHERE singleton=1').get().in_flight), 0);
    raw.close();
    reopened.close();
  });

  await t.test('OPS03.C04 a slow blocked tenant does not hold a second worker slot from eligible fast work', async t => {
    const mixedDir = tempDirectory('ah11-fairness-mixed-speed-');
    t.after(() => mixedDir.cleanup());
    const mixedWorker = createOperationsWorker({
      databasePath: mixedDir.databasePath, ownerId: 'review-worker-mixed-speed',
      config: { workerCount: 2, maxInFlightGlobal: 2, maxInFlightTenant: 1,
        maxInFlightProject: 1, pollIntervalMs: 10 }
    });
    t.after(async () => { if (mixedWorker.lifecycle !== 'stopped') await mixedWorker.stop('test'); });
    void mixedWorker.start();
    const slowScope = { tenantId: 'mixed-tenant-a', projectId: 'mixed-project-a1' };
    const fastScope = { tenantId: 'mixed-tenant-b', projectId: 'mixed-project-b1' };
    const slowPlan = JSON.stringify({
      kind: 'agent-harness.private-synthetic-plan', version: 1,
      steps: Array.from({ length: 32 }, (_, index) => ({
        id: `m${String(index).padStart(2, '0')}`, operation: 'text.concat',
        dependsOn: index % 12 === 0 ? [] : [`m${String(index - 1).padStart(2, '0')}`],
        args: { parts: index % 12 === 0 ? ['x'.repeat(512)] : [{ $result: `m${String(index - 1).padStart(2, '0')}` }, ''] }
      }))
    });
    const slow = mixedWorker.submitSyntheticWork({ trustedSyntheticScope: slowScope, idempotencyKey: 'mixed-slow', planJson: slowPlan });
    assert.equal(slow.accepted, true);
    const startDeadline = Date.now() + 3_000;
    while (Date.now() < startDeadline && mixedWorker.activeChildCount === 0) await new Promise(resolvePromise => setTimeout(resolvePromise, 2));
    assert.equal(mixedWorker.activeChildCount, 1);
    const runners = spawnSync('pgrep', ['-P', String(process.pid)], { encoding: 'utf8' });
    assert.equal(runners.status, 0, runners.stderr);
    const runnerPid = Number(runners.stdout.trim().split(/\s+/u).at(-1));
    assert.ok(Number.isSafeInteger(runnerPid) && runnerPid > 1);
    assert.equal(process.kill(runnerPid, 'SIGSTOP'), true);
    const slow2 = mixedWorker.submitSyntheticWork({ trustedSyntheticScope: slowScope, idempotencyKey: 'mixed-slow-queued', planJson: PLAN });
    const fast = mixedWorker.submitSyntheticWork({ trustedSyntheticScope: fastScope, idempotencyKey: 'mixed-fast', planJson: PLAN });
    assert.equal(slow2.accepted, true);
    assert.equal(fast.accepted, true);
    const fastStatus = await waitForStatus(mixedWorker, fastScope, fast.jobId, 'completed', 4_000);
    assert.equal(fastStatus.state, 'completed');
    const slowStatus = mixedWorker.readScopedWorkPage({ trustedSyntheticScope: slowScope, limit: 4 }).items;
    assert.equal(slowStatus.find(item => item.jobId === slow.jobId)?.state, 'running');
    assert.equal(slowStatus.find(item => item.jobId === slow2.jobId)?.state, 'queued');
    const raw = new DatabaseSync(mixedDir.databasePath, { readOnly: true });
    const inFlight = Number(raw.prepare('SELECT in_flight FROM operations_meta WHERE singleton=1').get().in_flight);
    assert.equal(inFlight, 1);
    assert.ok(inFlight <= 2);
    raw.close();
    try { process.kill(runnerPid, 'SIGCONT'); } catch {}
    const stopped = await mixedWorker.stop('test');
    assert.equal(stopped.stopped, true);
    assert.equal(stopped.childrenReaped, true);
  });
});

test('OPS04_REAL_SIGTERM_JEV_TELEMETRY preserves each controlled transport boundary without a second transport', async t => {
  const temporary = tempDirectory('ah11-jev-sigterm-', { ci1OwnedGroups: true });
  registerCi1OwnedTemporaryDirectoryFinalizer(t, cleanupCi1OwnedTemporaryDirectory, temporary, METRICS);
  const store = openStore(temporary.databasePath);
  store.close();
  const phases = [
    'before-marker', 'after-marker-before-bytes', 'after-write',
    'response-before-observation', 'observation-before-checkpoint'
  ];
  const results = [];
  const exactByteRecordBytes = Buffer.byteLength(JSON.stringify({ value: '' }));
  const exactBytePayload = { value: 'x'.repeat(CI1_MAX_PHASE_BYTES - exactByteRecordBytes) };
  assert.equal(Buffer.byteLength(JSON.stringify(exactBytePayload)), CI1_MAX_PHASE_BYTES);
  assert.equal(ci1CaptureCanAppend(0, 0, CI1_MAX_PHASE_BYTES), true);
  assert.equal(ci1CaptureCanAppend(0, 0, CI1_MAX_PHASE_BYTES + 1), false);
  assert.equal(ci1CaptureCanAppend(CI1_MAX_PHASE_RECORDS - 1, 0, 1), true);
  assert.equal(ci1CaptureCanAppend(CI1_MAX_PHASE_RECORDS, 0, 1), false);
  const exactByteControl = { ci1PhaseCaptures: [], ci1PhaseCaptureBytes: 0, ci1PhaseCaptureOverflow: false };
  assert.equal(appendBoundedCi1PhaseCapture(exactByteControl, exactBytePayload), true);
  assert.equal(exactByteControl.ci1PhaseCaptureBytes, CI1_MAX_PHASE_BYTES);
  assert.equal(appendBoundedCi1PhaseCapture(exactByteControl, {}), false);
  assert.equal(exactByteControl.ci1PhaseCaptureOverflow, true, 'one-byte overflow is retained as explicit failure custody');
  const exactRecordControl = { ci1PhaseCaptures: [], ci1PhaseCaptureBytes: 0, ci1PhaseCaptureOverflow: false };
  for (let index = 0; index < CI1_MAX_PHASE_RECORDS; index += 1) {
    assert.equal(appendBoundedCi1PhaseCapture(exactRecordControl, { sequence: index + 1 }), true);
  }
  assert.equal(exactRecordControl.ci1PhaseCaptures.length, CI1_MAX_PHASE_RECORDS);
  assert.equal(appendBoundedCi1PhaseCapture(exactRecordControl, { sequence: CI1_MAX_PHASE_RECORDS + 1 }), false);
  assert.equal(exactRecordControl.ci1PhaseCaptureOverflow, true, 'record overflow is retained as explicit failure custody');
  const parentStartedAt = performance.now();
  const parentSequences = new Map();
  const captureParentPhase = (phase, role, stage, details) => {
    if (!CI1_PHASES.has(phase) || !['shutdown', 'restart'].includes(role) || !CI1_PARENT_PHASE_STAGES.has(stage)) {
      METRICS.ci1PhaseCaptureIncomplete = true;
      return false;
    }
    const closeStage = stage === 'shutdown_child_closed' || stage === 'restart_child_closed';
    if ((closeStage && ((stage === 'shutdown_child_closed') !== (role === 'shutdown')
        || !details || Object.keys(details).sort().join(',') !== 'exitDisposition'
        || !isOps04ExitDisposition(details.exitDisposition)))
        || (!closeStage && details !== undefined)) {
      METRICS.ci1PhaseCaptureIncomplete = true;
      return false;
    }
    const key = `${phase}:${role}`;
    const sequence = (parentSequences.get(key) ?? 0) + 1;
    parentSequences.set(key, sequence);
    const appended = appendCi1PhaseCapture({
      schema: 1, source: 'parent', phase, role, sequence, stage,
      elapsedMs: Math.max(0, Math.round(performance.now() - parentStartedAt)),
      ...(closeStage ? { exitDisposition: details.exitDisposition } : {})
    });
    if (!appended) METRICS.ci1PhaseCaptureIncomplete = true;
    return appended;
  };

  const restartInvocationTimeoutMs = 15_000;
  const runChild = async (phase, role, expectedWorkflowId) => {
    const invocationStartedAt = performance.now();
    const restartInvocationDeadlineAt = invocationStartedAt + restartInvocationTimeoutMs;
    const marker = join(temporary.directory, `${phase}.marker`);
    const resultPath = join(temporary.directory, `${phase}.${role}.json`);
    const metricsPath = join(temporary.directory, `${phase}.${role}.metrics.jsonl`);
    const phaseCapturePath = `${metricsPath}.${role}.phases.jsonl`;
    const groupReadyPath = `${metricsPath}.${role}.group-ready.json`;
    const launchToken = randomUUID();
    assert.equal(beginCi1OwnedGroupLaunch(launchToken, temporary.ci1Custody), true,
      `${phase}/${role} launch intent and exact temp directory are recorded before spawn`);
    assert.equal(markCi1OwnedGroupSpawnAttempt(temporary.ci1Custody, launchToken), true,
      `${phase}/${role} spawn attempt is recorded before detached launch`);
    const child = spawn(process.execPath, [
      join(ROOT, 'tests/operations/fixtures/jev-sigterm-child.mjs'),
      temporary.databasePath, phase, marker, resultPath, metricsPath, role,
      ...(role === 'restart' ? [expectedWorkflowId] : [])
    ], {
      cwd: ROOT,
      env: {
        ...baseEnvironment(),
        JEV_OFFLINE_METRICS_FILE: metricsPath,
        OPS_CI1_GROUP_READY_FILE: groupReadyPath
      },
      stdio: ['ignore', 'ignore', 'pipe'],
      detached: true
    });
    let childStderr = '';
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', chunk => { childStderr += chunk; });
    let closeResult;
    const closed = new Promise(resolvePromise => child.once('close', (code, signal) => {
      closeResult = { code, signal };
      resolvePromise(closeResult);
    }));
    rememberCi1OwnedGroupChild(temporary.ci1Custody, launchToken, child, closed);
    let durationMs = null;
    let childResult;
    let ownedGroupId = null;
    const cleanupOwnedGroup = async () => {
      const cleanup = await cleanupVerifiedCi1ProcessGroup(
        child, closed, ownedGroupId, stage => captureParentPhase(phase, role, stage)
      );
      if (cleanup.groupEmpty && !finishCi1OwnedGroupAfterAbsence(child.pid, launchToken, temporary.ci1Custody)) {
        METRICS.ci1PhaseCaptureIncomplete = true;
      }
      return cleanup;
    };
    let restartTimedOut = false;
    const lifecyclePromise = executeCi1ChildWithFinalization(async () => {
      const ownership = await verifyRegisterCi1OwnedGroupBeforeReady({
        child, launchToken, readyPath: groupReadyPath, custody: temporary.ci1Custody, closed
      });
      ownedGroupId = ownership.groupId;
      assert.equal(ownership.stage, 'ready', `${phase}/${role} group verification, registration, or gate release failed`);
      captureParentPhase(phase, role, `${role}_spawned`);
      if (role === 'shutdown') {
        await waitForPath(marker, child);
        const barrier = JSON.parse(readFileSync(marker, 'utf8'));
        assert.equal(barrier.phase, phase);
        captureParentPhase(phase, role, 'shutdown_marker_observed');
        const sigtermSentAt = performance.now();
        const sent = child.kill('SIGTERM');
        assert.equal(sent, true, `${phase}; exit=${child.exitCode}; signal=${child.signalCode}; result=${existsSync(resultPath)}`);
        if (sent) METRICS.processSigtermSignalsSent += 1;
        captureParentPhase(phase, role, 'sigterm_sent');
        const shutdownClose = await waitCi1ChildClose({
          timeoutMs: 3_000, escalationStage: 'shutdown_escalation_sent',
          waitForClose: timeoutMs => waitForCloseBounded(closed, timeoutMs),
          reapOwnedChild: async () => {
            const cleanup = await cleanupOwnedGroup();
            return cleanup.groupEmpty ? cleanup.closeResult : null;
          },
          captureParent: (stage, details) => captureParentPhase(phase, role, stage, details), state: METRICS
        });
        closeResult = shutdownClose.closeResult;
        if (shutdownClose.timedOut) {
          if (!closeResult) throw new Error(`The owned ${phase} shutdown child could not be confirmed closed after bounded reaping.`);
          throw new Error(`The controlled JEV shutdown child did not close after SIGTERM at ${phase}.`);
        }
        durationMs = Math.max(0, performance.now() - sigtermSentAt);
        assert.ok(durationMs <= 3_000, `${phase} shutdown child exceeded its unchanged close deadline.`);
      } else {
        const restartClose = await waitCi1ChildClose({
          timeoutMs: Math.max(0, restartInvocationDeadlineAt - performance.now()),
          escalationStage: 'restart_escalation_sent',
          waitForClose: timeoutMs => waitForCloseBounded(closed, timeoutMs),
          reapOwnedChild: async () => {
            const cleanup = await cleanupOwnedGroup();
            return cleanup.groupEmpty ? cleanup.closeResult : null;
          },
          captureParent: (stage, details) => captureParentPhase(phase, role, stage, details), state: METRICS
        });
        closeResult = restartClose.closeResult;
        if (restartClose.timedOut) {
          if (!closeResult) throw new Error(`The owned ${phase} restart child could not be confirmed closed after bounded reaping.`);
          restartTimedOut = true;
          throw new Error(`The controlled JEV restart child exceeded its ${restartInvocationTimeoutMs} ms invocation bound at ${phase}.`);
        }
        durationMs = Math.max(0, performance.now() - invocationStartedAt);
      }
      return { durationMs };
    }, async () => finalizeCi1ChildCapture({
        phase, role, closeResult,
        cleanupOwnedChild: cleanupOwnedGroup,
        readCapture: () => readCi1ChildPhaseCapture(phaseCapturePath, phase, role, closeResult?.code === 0),
        captureParent: (stage, details) => captureParentPhase(phase, role, stage, details),
        state: METRICS
      }), METRICS);
    let lifecycle;
    try {
      lifecycle = await lifecyclePromise;
    } catch (error) {
      if (restartTimedOut && role === 'restart') {
        const lastChildCapture = METRICS.ci1PhaseCaptures.filter(record => record.source === 'child'
          && record.phase === phase && record.role === role).at(-1);
        const lastStage = typeof lastChildCapture?.stage === 'string' ? lastChildCapture.stage : 'unknown';
        const childElapsedMs = Number.isSafeInteger(lastChildCapture?.elapsedMs) ? lastChildCapture.elapsedMs : 'unknown';
        throw new Error(`${error.message}; lastChildStage=${lastStage}; childElapsedMs=${childElapsedMs}.`);
      }
      throw error;
    }
    const finalization = lifecycle.finalization;
    closeResult = finalization.closeResult;
    try {
      assert.equal(finalization.reaped, true, `The owned ${phase}/${role} child close could not be confirmed.`);
      assert.equal(finalization.ownedGroupEmpty, true, `The dedicated ${phase}/${role} process group could not be confirmed empty.`);
      assert.equal(closeResult.code, 0, `${phase}/${role}: child stderr=${childStderr.slice(0, 2048)}`);
      childResult = JSON.parse(readFileSync(resultPath, 'utf8'));
      assert.equal(childResult.phaseCaptureComplete, true, `${phase}/${role} fixture reported incomplete phase capture`);
      assert.equal(finalization.captureResult?.complete, true, `${phase}/${role} phase capture incomplete`);
    } catch (error) {
      METRICS.ci1PhaseCaptureIncomplete = true;
      throw error;
    }
    return { childResult, metricsPath, durationMs: lifecycle.execution.durationMs, childStderr };
  };

  for (const phase of phases) {
    const shutdown = await runChild(phase, 'shutdown');
    const result = shutdown.childResult;
    METRICS.processSigtermDrainElapsedMsMax = Math.max(METRICS.processSigtermDrainElapsedMsMax, shutdown.durationMs);
    METRICS.processSigtermDrainSamples += 1;
    METRICS.processCloseElapsedMsMax = Math.max(METRICS.processCloseElapsedMsMax, shutdown.durationMs);
    METRICS.processCloseSamples += 1;
    METRICS.processRuns += 1;
    assert.equal(result.termSeen, true);
    assert.equal(result.blockedExternalNetworkAttempts, 0);
    assert.equal(result.opaqueHoldRetained, phase !== 'before-marker');
    if (phase === 'before-marker') {
      assert.equal(result.attemptCount, 0);
      assert.equal(result.markerCount, 0);
      assert.equal(result.controlledHttpsWrites, 0);
    } else {
      assert.equal(result.attemptCount, 1);
      assert.equal(result.markerCount, 1);
      assert.equal(result.observationCount, 1);
    }
    if (phase === 'after-write') {
      assert.equal(result.dispatchCertainty, 'possible');
      assert.equal(result.controlledHttpsWrites, 1);
    }
    if (phase === 'after-marker-before-bytes') assert.equal(result.controlledHttpsWrites, 0);
    if (phase === 'response-before-observation' || phase === 'observation-before-checkpoint') {
      assert.equal(result.dispatchCertainty, 'response_received');
    }
    for (const [key, value] of Object.entries(JSON.parse(readFileSync(shutdown.metricsPath, 'utf8')))) {
      if (key === 'controlled_dns_resolve4') METRICS.controlledDnsResolve4 += value;
      if (key === 'controlled_dns_resolve6') METRICS.controlledDnsResolve6 += value;
      if (key === 'controlled_https_request') METRICS.controlledHttpsRequests += value;
      if (key === 'controlled_https_write') METRICS.controlledHttpsWrites += value;
      if (key === 'blocked_external_network_attempt') METRICS.blockedExternalNetworkAttempts += value;
    }
    const restart = await runChild(phase, 'restart', result.workflowId);
    assert.equal(restart.childResult.phase, phase);
    assert.equal(restart.childResult.providerAttemptCountAfterRestart, result.attemptCount);
    assert.equal(restart.childResult.authorityHoldsAfterRestart, result.authorityHoldCountAfterStop);
    assert.equal(restart.childResult.operationsInFlightAfterRestart, 0);
    assert.equal(restart.childResult.restartLoopSettled, true);
    assert.ok(restart.durationMs <= restartInvocationTimeoutMs,
      `${phase} restart invocation exceeded its ${restartInvocationTimeoutMs} ms bound`);
    results.push({ ...result, ...restart.childResult });
  }
  assert.equal(results.length, 5);
  assert.equal(METRICS.ci1PhaseCaptureOverflow, false);
  assert.equal(METRICS.ci1PhaseCaptureIncomplete, false);
  assert.equal(METRICS.ci1PhaseCaptureChildCloseUnconfirmed, false);
  assert.equal(METRICS.ci1PhaseCaptureOwnedGroupCloseUnconfirmed, false);
  assert.ok(METRICS.ci1PhaseCaptures.length > 0 && METRICS.ci1PhaseCaptureBytes <= 24_576);
  const retainedStopResults = METRICS.ci1PhaseCaptures.filter(record => record.source === 'child'
    && ['stop_settled', 'restart_stop_settled'].includes(record.stage));
  assert.equal(retainedStopResults.length, 10);
  assert.ok(retainedStopResults.every(record => record.stopped === true
    && record.childrenReaped === true && record.storeClosed === true));
  assert.equal(METRICS.providerCalls, 0);
  assert.equal(METRICS.controlledHttpsRequests, 3);
  assert.equal(METRICS.controlledHttpsWrites, 3);
  await t.test('OPS04.C01 composes the existing sole JEV transport with owned worker shutdown, telemetry outage, reopen, and higher-fence restart', () => {
    for (const result of results) {
      assert.equal(result.operationsWorkerStopped, true, result.phase);
      assert.equal(result.ownedRunnerReaped, true, result.phase);
      assert.equal(result.operationsWorkStateAfterStop, 'queued', result.phase);
      assert.equal(result.operationsInFlightAfterStop, 0, result.phase);
      assert.equal(result.telemetryCursor, 0, result.phase);
      assert.ok(result.telemetryPendingRecords > 0, result.phase);
      assert.equal(result.telemetrySinkCalls, 1, result.phase);
      assert.equal(result.operationsRestarted, true, result.phase);
      assert.equal(result.operationsWorkflowStateAfterRestart, 'succeeded', result.phase);
      assert.ok(result.operationsFenceAfterRestart > result.operationsFenceBeforeRestart, result.phase);
      assert.ok(result.operationsGenerationAfterRestart > result.operationsGenerationBeforeRestart, result.phase);
      assert.equal(result.providerAttemptCountAfterRestart, result.attemptCount, result.phase);
      assert.equal(result.blockedExternalNetworkAttempts, 0, result.phase);
      assert.ok(result.controlledHttpsWrites <= 1, result.phase);
    }
  });
  await t.test('OPS04.C02 fixture assertion failure after pausing the owned runner executes bounded finally cleanup', async t => {
    function runFakeAtomicPublication({ failAt = null, cleanupFails = false } = {}) {
      const targetPath = '/private/ops-c02/control.json';
      const files = new Map();
      const value = { schema: 'synthetic-control.v1', complete: true };
      const encoded = Buffer.from(JSON.stringify(value), 'utf8');
      const primaryFailure = new Error('SYNTHETIC_PRIVATE_C02_PRIMARY');
      const closeFailure = new Error('SYNTHETIC_PRIVATE_C02_CLOSE');
      const cleanupFailure = new Error('SYNTHETIC_PRIVATE_C02_CLEANUP');
      const acquisitionFailure = Object.assign(new Error('SYNTHETIC_PRIVATE_C02_EEXIST'), { code: 'EEXIST' });
      const temporaryPath = `${targetPath}.deterministic-token.tmp`;
      let acquiredDescriptor = null;
      let completedTempWrite = false;
      let finalWasVisibleDuringTempWrite = false;
      let renamed = false;
      let closed = false;
      let writeCalls = 0;
      let closeCalls = 0;
      let renameCalls = 0;
      let cleanedPath = null;
      let cleanupFailureObserved = false;
      let caughtFailure;
      try {
        publishC02ControlJsonAtomically(targetPath, value, {
          randomUUID: () => 'deterministic-token',
          openSync(path, flags, mode) {
            assert.equal(path, '/private/ops-c02/control.json.deterministic-token.tmp',
              'the temp is a fresh sibling of the consumer-visible marker');
            assert.equal(flags, 'wx');
            assert.equal(mode, 0o600);
            assert.equal(files.has(targetPath), false, 'the final marker is absent before temp creation');
            if (failAt === 'acquire') {
              files.set(path, Buffer.from('preexisting-sentinel'));
              throw acquisitionFailure;
            }
            files.set(path, Buffer.alloc(0));
            acquiredDescriptor = 73;
            return acquiredDescriptor;
          },
          writeSync(descriptor, bytes, offset, length, position) {
            assert.equal(descriptor, acquiredDescriptor);
            assert.equal(position, null);
            writeCalls += 1;
            if (failAt === 'zero-progress') return 0;
            if (failAt === 'invalid-write') return length + 1;
            const chunkBytes = Math.min(length, Math.max(1, Math.floor(length / 2)));
            const current = files.get(temporaryPath);
            files.set(temporaryPath, Buffer.concat([current, Buffer.from(bytes).subarray(offset, offset + chunkBytes)]));
            finalWasVisibleDuringTempWrite ||= files.has(targetPath);
            completedTempWrite = files.get(temporaryPath).byteLength === encoded.byteLength;
            if ((failAt === 'write' || failAt === 'write-close-cleanup') && !completedTempWrite) {
              throw primaryFailure;
            }
            return chunkBytes;
          },
          closeSync(descriptor) {
            assert.equal(descriptor, acquiredDescriptor);
            closeCalls += 1;
            if (failAt === 'close' || failAt === 'close-cleanup' || failAt === 'write-close-cleanup') throw closeFailure;
            closed = true;
          },
          renameSync(from, to) {
            renameCalls += 1;
            assert.equal(from, temporaryPath);
            assert.equal(to, targetPath);
            assert.equal(completedTempWrite, true, 'rename occurs only after the full temp content is visible');
            assert.equal(closed, true, 'rename occurs only after the exclusive temp descriptor is closed');
            assert.equal(files.has(to), false, 'the final marker stays absent through complete temp writing');
            if (failAt === 'rename' || failAt === 'rename-cleanup') throw primaryFailure;
            files.set(to, files.get(from));
            files.delete(from);
            renamed = true;
          },
          rmSync(path, options) {
            cleanedPath = path;
            assert.equal(path, temporaryPath);
            assert.deepEqual(options, { force: true });
            if (cleanupFails) throw cleanupFailure;
            files.delete(path);
          },
          onCleanupFailure() { cleanupFailureObserved = true; }
        });
      } catch (error) {
        caughtFailure = error;
      }
      return {
        targetPath, temporaryPath, files, value, encoded, primaryFailure, closeFailure, acquisitionFailure, caughtFailure,
        completedTempWrite, finalWasVisibleDuringTempWrite, renamed, closed,
        writeCalls, closeCalls, renameCalls, cleanedPath, cleanupFailureObserved
      };
    }

    const successfulPublication = runFakeAtomicPublication();
    assert.equal(successfulPublication.caughtFailure, undefined);
    assert.equal(successfulPublication.finalWasVisibleDuringTempWrite, false,
      'the consumer-visible final path never exists while the fake filesystem creates or fills the temp');
    assert.equal(successfulPublication.renamed, true);
    assert.ok(successfulPublication.writeCalls > 1, 'short writes are completed by the bounded progress loop');
    assert.equal(successfulPublication.closeCalls, 1);
    assert.deepEqual(JSON.parse(successfulPublication.files.get(successfulPublication.targetPath).toString('utf8')),
      successfulPublication.value);
    assert.deepEqual(successfulPublication.files.get(successfulPublication.targetPath), successfulPublication.encoded);

    const failedWrite = runFakeAtomicPublication({ failAt: 'write' });
    assert.equal(failedWrite.caughtFailure, failedWrite.primaryFailure,
      'a partial temp write rethrows its exact primary error');
    assert.equal(failedWrite.files.has(failedWrite.targetPath), false);
    assert.equal(failedWrite.files.size, 0, 'the partial temp is removed after the failed write');
    assert.equal(failedWrite.cleanedPath, '/private/ops-c02/control.json.deterministic-token.tmp');
    assert.equal(failedWrite.closeCalls, 1, 'an acquired descriptor is closed once after write failure');
    assert.equal(failedWrite.renameCalls, 0);

    const failedRename = runFakeAtomicPublication({ failAt: 'rename' });
    assert.equal(failedRename.caughtFailure, failedRename.primaryFailure,
      'a failed rename rethrows its exact primary error');
    assert.equal(failedRename.files.has(failedRename.targetPath), false);
    assert.equal(failedRename.files.size, 0, 'the complete temp is removed after the failed rename');
    assert.equal(failedRename.closeCalls, 1);
    assert.equal(failedRename.renameCalls, 1);

    const failedRenameCleanup = runFakeAtomicPublication({ failAt: 'rename-cleanup', cleanupFails: true });
    assert.equal(failedRenameCleanup.caughtFailure, failedRenameCleanup.primaryFailure,
      'a cleanup failure cannot replace the original rename error');
    assert.equal(failedRenameCleanup.cleanupFailureObserved, true);
    assert.equal(failedRenameCleanup.files.has(failedRenameCleanup.targetPath), false);
    assert.equal(failedRenameCleanup.files.size, 1,
      'a failed cleanup remains explicit while preserving the rename error');

    const failedClose = runFakeAtomicPublication({ failAt: 'close' });
    assert.equal(failedClose.caughtFailure, failedClose.closeFailure,
      'a close failure becomes primary only when earlier writes succeeded');
    assert.equal(failedClose.closeCalls, 1);
    assert.equal(failedClose.renameCalls, 0, 'rename is withheld after a failed close');
    assert.equal(failedClose.files.has(failedClose.targetPath), false);
    assert.equal(failedClose.files.size, 0);

    const failedCloseCleanup = runFakeAtomicPublication({ failAt: 'close-cleanup', cleanupFails: true });
    assert.equal(failedCloseCleanup.caughtFailure, failedCloseCleanup.closeFailure,
      'a cleanup failure cannot replace the original close error');
    assert.equal(failedCloseCleanup.cleanupFailureObserved, true);
    assert.equal(failedCloseCleanup.renameCalls, 0);
    assert.equal(failedCloseCleanup.files.size, 1);

    const acquisitionCollision = runFakeAtomicPublication({ failAt: 'acquire' });
    assert.equal(acquisitionCollision.caughtFailure, acquisitionCollision.acquisitionFailure,
      'exclusive-open EEXIST remains the original failure');
    assert.equal(acquisitionCollision.files.get(acquisitionCollision.temporaryPath).toString('utf8'),
      'preexisting-sentinel', 'failed acquisition preserves the preexisting sentinel file');
    assert.equal(acquisitionCollision.cleanedPath, null, 'a pathname is never unlinked before this call acquired it');
    assert.equal(acquisitionCollision.closeCalls, 0);
    assert.equal(acquisitionCollision.renameCalls, 0);

    for (const invalidResult of ['zero-progress', 'invalid-write']) {
      const failedProgress = runFakeAtomicPublication({ failAt: invalidResult });
      assert.ok(failedProgress.caughtFailure instanceof RangeError, `${invalidResult} fails with a fixed bounded error`);
      assert.equal(failedProgress.closeCalls, 1, `${invalidResult} closes its acquired descriptor`);
      assert.equal(failedProgress.cleanedPath, failedProgress.temporaryPath, `${invalidResult} removes only its acquired temp`);
      assert.equal(failedProgress.renameCalls, 0, `${invalidResult} never publishes the final path`);
    }

    let oversizeOpenCalls = 0;
    let oversizeFailure;
    try {
      publishC02ControlJsonAtomically('/private/ops-c02/oversize.json', { value: 'x'.repeat(1_025) }, {
        openSync() { oversizeOpenCalls += 1; throw new Error('oversize must fail before open'); }
      });
    } catch (error) { oversizeFailure = error; }
    assert.ok(oversizeFailure instanceof RangeError);
    assert.equal(oversizeOpenCalls, 0, 'the 1,024-byte marker cap is checked before any filesystem acquisition');

    const failedCleanup = runFakeAtomicPublication({ failAt: 'write', cleanupFails: true });
    assert.equal(failedCleanup.caughtFailure, failedCleanup.primaryFailure,
      'a temp cleanup error cannot replace the original write error');
    assert.equal(failedCleanup.cleanupFailureObserved, true,
      'an unremoved private temp is surfaced to the caller as an incomplete cleanup state');
    assert.equal(failedCleanup.files.has(failedCleanup.targetPath), false);
    assert.equal(failedCleanup.files.size, 1, 'the fake cleanup failure remains explicit instead of claiming removal');

    const combinedWriteCloseCleanupFailure = runFakeAtomicPublication({
      failAt: 'write-close-cleanup', cleanupFails: true
    });
    assert.equal(combinedWriteCloseCleanupFailure.caughtFailure, combinedWriteCloseCleanupFailure.primaryFailure,
      'later close and cleanup failures cannot replace the earlier partial-write error');
    assert.equal(combinedWriteCloseCleanupFailure.closeCalls, 1);
    assert.equal(combinedWriteCloseCleanupFailure.cleanupFailureObserved, true);
    assert.equal(combinedWriteCloseCleanupFailure.files.has(combinedWriteCloseCleanupFailure.targetPath), false);

    const failedAckState = { targetAckSent: false, targetAckPublicationFailed: false };
    let failedAckProbeCalls = 0;
    let failedAckPublishCalls = 0;
    const failedAckProbe = () => { failedAckProbeCalls += 1; };
    const failedAckPublish = () => { failedAckPublishCalls += 1; throw new Error('SYNTHETIC_PRIVATE_ACK_PUBLISH'); };
    assert.equal(attemptC02WatchdogTargetAck({
      groupId: 812_345, watchAckPath: '/private/ops-c02/ack.json', pid: 812_346,
      state: failedAckState, probe: failedAckProbe, publish: failedAckPublish
    }), false);
    assert.equal(failedAckState.targetAckSent, false,
      'a failed ACK publication never claims targetAckSent');
    assert.equal(failedAckState.targetAckPublicationFailed, true,
      'a failed ACK publication is an explicit terminal publication state');
    assert.equal(attemptC02WatchdogTargetAck({
      groupId: 812_345, watchAckPath: '/private/ops-c02/ack.json', pid: 812_346,
      state: failedAckState, probe: failedAckProbe, publish: failedAckPublish
    }), false);
    assert.equal(failedAckProbeCalls, 1, 'the failed one-shot ACK does not probe or publish again');
    assert.equal(failedAckPublishCalls, 1, 'the failed one-shot ACK does not create repeated temps');
    const successfulAckState = { targetAckSent: false, targetAckPublicationFailed: false };
    let publishedAck = null;
    assert.equal(attemptC02WatchdogTargetAck({
      groupId: 812_347, watchAckPath: '/private/ops-c02/ack.json', pid: 812_348,
      state: successfulAckState, probe: () => {}, publish: (path, value) => { publishedAck = { path, value }; }
    }), true);
    assert.equal(successfulAckState.targetAckSent, true);
    assert.equal(successfulAckState.targetAckPublicationFailed, false);
    assert.deepEqual(publishedAck, {
      path: '/private/ops-c02/ack.json',
      value: { schema: 'ops-owned-group-watchdog-target-ack.v1', pid: 812_348, groupId: 812_347, verified: true }
    });

    function runFakeCi1Publication(kind, { failAt = null, cleanupFails = false } = {}) {
      const targetPath = `/private/ops-ci1/${kind}.json`;
      const temporaryPath = kind === 'registry'
        ? `${targetPath}.${process.pid}.deterministic-token.tmp`
        : `${targetPath}.deterministic-token.tmp`;
      const files = new Map();
      const primaryFailure = new Error('SYNTHETIC_PRIVATE_CI1_PRIMARY');
      const closeFailure = new Error('SYNTHETIC_PRIVATE_CI1_CLOSE');
      const acquisitionFailure = Object.assign(new Error('SYNTHETIC_PRIVATE_CI1_EEXIST'), { code: 'EEXIST' });
      const fakeFaults = [];
      const fakeCheck = (condition, label) => {
        if (!condition) {
          fakeFaults.push(label);
          throw new Error(`fake filesystem control failed: ${label}`);
        }
      };
      let acquiredDescriptor = null;
      let writeCalls = 0;
      let closeCalls = 0;
      let renameCalls = 0;
      let removeCalls = 0;
      let finalVisibleDuringWrite = false;
      let closed = false;
      let result;
      const fileOps = {
        randomUUID: () => 'deterministic-token',
        openSync(path, flags, mode) {
          fakeCheck(path === temporaryPath, 'temporary path');
          fakeCheck(flags === 'wx', 'exclusive open flag');
          fakeCheck(mode === 0o600, 'private temporary mode');
          if (failAt === 'acquire') {
            files.set(path, Buffer.from('preexisting-sentinel'));
            throw acquisitionFailure;
          }
          files.set(path, Buffer.alloc(0));
          acquiredDescriptor = 91;
          return acquiredDescriptor;
        },
        writeSync(descriptor, bytes, offset, length, position) {
          fakeCheck(descriptor === acquiredDescriptor, 'owned descriptor');
          fakeCheck(position === null, 'current file position');
          writeCalls += 1;
          finalVisibleDuringWrite ||= files.has(targetPath);
          if ((failAt === 'write' || failAt === 'write-close-cleanup') && writeCalls === 2) {
            throw primaryFailure;
          }
          if (failAt === 'zero-progress') return 0;
          if (failAt === 'invalid-progress') return length + 1;
          const written = Math.max(1, Math.floor(length / 2));
          files.set(temporaryPath, Buffer.concat([
            files.get(temporaryPath), Buffer.from(bytes).subarray(offset, offset + written)
          ]));
          return written;
        },
        closeSync(descriptor) {
          fakeCheck(descriptor === acquiredDescriptor, 'closed acquired descriptor');
          closeCalls += 1;
          if (failAt === 'close' || failAt === 'write-close-cleanup') throw closeFailure;
          closed = true;
        },
        renameSync(from, to) {
          renameCalls += 1;
          fakeCheck(from === temporaryPath, 'renamed acquired temporary');
          fakeCheck(to === targetPath, 'final publication path');
          fakeCheck(closed, 'publication after descriptor close');
          fakeCheck(!finalVisibleDuringWrite, 'final path stays absent during temp writes');
          if (failAt === 'rename' || failAt === 'rename-cleanup') throw primaryFailure;
          files.set(to, files.get(from));
          files.delete(from);
        },
        rmSync(path, options) {
          removeCalls += 1;
          fakeCheck(path === temporaryPath, 'removed acquired temporary');
          fakeCheck(options?.force === true, 'forced temporary cleanup');
          if (cleanupFails || failAt === 'write-close-cleanup' || failAt === 'rename-cleanup') {
            throw new Error('SYNTHETIC_PRIVATE_CI1_CLEANUP');
          }
          files.delete(path);
        }
      };
      if (kind === 'custody') {
        const custody = {
          directory: '/private/ops-ci1/custody-dir', sidecarPath: targetPath,
          launches: new Map([['fixed-token', {
            launchToken: 'fixed-token', status: 'launch_intent', spawnAttempted: false,
            directPid: null, groupId: null, registryRegistered: false,
            registryCleared: false, groupAbsent: false
          }]])
        };
        result = writeCi1TemporaryCustody(custody, fileOps);
      } else {
        result = writeCi1OwnedGroupRegistry(targetPath, {
          schema: 'ops-ci1-owned-groups.v1', groups: [], pendingLaunches: []
        }, fileOps);
      }
      return {
        targetPath, temporaryPath, files, result, writeCalls, closeCalls, renameCalls,
        removeCalls, finalVisibleDuringWrite, fakeFaults
      };
    }

    for (const publisher of ['custody', 'registry']) {
      const complete = runFakeCi1Publication(publisher);
      assert.deepEqual(complete.fakeFaults, []);
      assert.equal(complete.result, true, `${publisher} publication succeeds after bounded short writes`);
      assert.ok(complete.writeCalls > 1);
      assert.equal(complete.closeCalls, 1);
      assert.equal(complete.renameCalls, 1);
      assert.equal(complete.finalVisibleDuringWrite, false);
      assert.equal(complete.files.has(complete.temporaryPath), false);
      const publishedValue = JSON.parse(complete.files.get(complete.targetPath).toString('utf8'));
      if (publisher === 'custody') {
        assert.deepEqual(publishedValue, {
          schema: CI1_TEMP_CUSTODY_SCHEMA,
          ownerPid: process.pid,
          directory: '/private/ops-ci1/custody-dir',
          launches: [{
            launchToken: 'fixed-token', status: 'launch_intent', spawnAttempted: false,
            directPid: null, groupId: null, registryRegistered: false,
            registryCleared: false, groupAbsent: false
          }]
        });
      } else {
        assert.deepEqual(publishedValue, {
          schema: 'ops-ci1-owned-groups.v1', groups: [], pendingLaunches: []
        });
      }

      const collision = runFakeCi1Publication(publisher, { failAt: 'acquire' });
      assert.deepEqual(collision.fakeFaults, []);
      assert.equal(collision.result, false);
      assert.equal(collision.files.get(collision.temporaryPath).toString('utf8'), 'preexisting-sentinel');
      assert.equal(collision.writeCalls, 0);
      assert.equal(collision.closeCalls, 0);
      assert.equal(collision.renameCalls, 0);
      assert.equal(collision.removeCalls, 0, 'failed exclusive acquisition never unlinks the sentinel');

      const partial = runFakeCi1Publication(publisher, { failAt: 'write' });
      assert.deepEqual(partial.fakeFaults, []);
      assert.equal(partial.result, false);
      assert.equal(partial.closeCalls, 1, 'an acquired descriptor is closed once after partial write failure');
      assert.equal(partial.renameCalls, 0);
      assert.equal(partial.removeCalls, 1);
      assert.equal(partial.files.has(partial.temporaryPath), false,
        'only the temp this publisher acquired is removed after partial write failure');

      const closeFailure = runFakeCi1Publication(publisher, { failAt: 'close' });
      assert.deepEqual(closeFailure.fakeFaults, []);
      assert.equal(closeFailure.result, false);
      assert.equal(closeFailure.closeCalls, 1);
      assert.equal(closeFailure.renameCalls, 0, 'a failed close prevents publication');
      assert.equal(closeFailure.removeCalls, 1);
      assert.equal(closeFailure.files.has(closeFailure.temporaryPath), false);

      for (const failAt of ['zero-progress', 'invalid-progress']) {
        const invalidProgress = runFakeCi1Publication(publisher, { failAt });
        assert.deepEqual(invalidProgress.fakeFaults, []);
        assert.equal(invalidProgress.result, false, `${failAt} fails closed`);
        assert.equal(invalidProgress.writeCalls, 1, `${failAt} stops without a retry loop`);
        assert.equal(invalidProgress.closeCalls, 1);
        assert.equal(invalidProgress.renameCalls, 0);
        assert.equal(invalidProgress.removeCalls, 1);
      }

      const combined = runFakeCi1Publication(publisher, {
        failAt: 'write-close-cleanup', cleanupFails: true
      });
      assert.deepEqual(combined.fakeFaults, []);
      assert.equal(combined.result, false, 'write, close and cleanup failures remain fail-closed');
      assert.equal(combined.closeCalls, 1);
      assert.equal(combined.renameCalls, 0);
      assert.equal(combined.removeCalls, 1);
      assert.equal(combined.files.has(combined.temporaryPath), true,
        'failed cleanup retains the acquired temporary for existing custody reconciliation');

      const renameCleanup = runFakeCi1Publication(publisher, {
        failAt: 'rename-cleanup', cleanupFails: true
      });
      assert.deepEqual(renameCleanup.fakeFaults, []);
      assert.equal(renameCleanup.result, false, 'rename remains failed when cleanup also fails');
      assert.equal(renameCleanup.closeCalls, 1);
      assert.equal(renameCleanup.renameCalls, 1);
      assert.equal(renameCleanup.removeCalls, 1);
      assert.equal(renameCleanup.files.has(renameCleanup.temporaryPath), true);
    }

    const diagnosticState = {
      c02ControlReadFailureLines: '', c02ControlReadFailureCount: 0,
      c02ControlReadFailureBytes: 0, c02ControlReadDiagnosticsComplete: true,
      ci1PhaseCaptureIncomplete: false
    };
    const privateReadError = new SyntaxError('SYNTHETIC_PRIVATE_C02 /private/marker.json pid=912345');
    let caughtReadError;
    try {
      readC02ControlJson('/private/marker.json', 'C02_WATCHDOG_READY', () => { throw privateReadError; }, diagnosticState);
    } catch (error) { caughtReadError = error; }
    assert.equal(caughtReadError, privateReadError, 'diagnostic recording preserves the original parse/read error');
    const safeControlDiagnostics = parseC02ControlReadDiagnostics({
      lines: diagnosticState.c02ControlReadFailureLines,
      count: diagnosticState.c02ControlReadFailureCount,
      bytes: diagnosticState.c02ControlReadFailureBytes,
      diagnosticsComplete: diagnosticState.c02ControlReadDiagnosticsComplete
    });
    assert.deepEqual(safeControlDiagnostics, {
      complete: true, recordCount: 1,
      records: [{ caseId: 'OPS04.C02', boundary: 'C02_WATCHDOG_READY', errorClass: 'SyntaxError' }]
    });
    assert.doesNotMatch(JSON.stringify(safeControlDiagnostics), /SYNTHETIC_PRIVATE|private|912345|marker\.json/u);
    const expectedIncompleteDiagnostics = {
      complete: false, recordCount: 0, records: []
    };
    const diagnosticLine = (boundary = 'C02_WATCHDOG_READY', errorClass = 'SyntaxError', extra = {}) =>
      `${C02_CONTROL_DIAGNOSTIC_PREFIX}${JSON.stringify({
        schema: 'agent-harness.ah11-ops-c02-control-failure.v1',
        caseId: 'OPS04.C02', boundary, errorClass, ...extra
      })}`;
    const diagnosticChannel = (lines, diagnosticsComplete = true) => ({
      lines, count: lines === '' ? 0 : lines.split('\n').length - 1,
      bytes: Buffer.byteLength(lines, 'utf8'), diagnosticsComplete
    });
    const assertIncompleteChannel = (channel, label) => {
      assert.deepEqual(parseC02ControlReadDiagnostics(channel), expectedIncompleteDiagnostics, label);
    };
    const validChannel = diagnosticChannel(`${diagnosticLine()}\n`);
    for (const field of ['lines', 'count', 'bytes', 'diagnosticsComplete']) {
      const missingField = { ...validChannel };
      delete missingField[field];
      assertIncompleteChannel(missingField, `missing ${field} is incomplete`);
    }
    assertIncompleteChannel(null, 'a null channel returns fixed incomplete diagnostics');
    assertIncompleteChannel({ lines: null, count: 0, bytes: -1, diagnosticsComplete: true },
      'the malformed null/negative metrics pair returns incomplete without throwing');
    for (const wrongType of [
      { ...validChannel, lines: 1 },
      { ...validChannel, count: '1' },
      { ...validChannel, bytes: 'number' },
      { ...validChannel, diagnosticsComplete: 1 }
    ]) assertIncompleteChannel(wrongType, 'wrong channel field types fail closed');
    assertIncompleteChannel({ ...validChannel, count: 0 }, 'record-count mismatch is incomplete');
    assertIncompleteChannel({ ...validChannel, bytes: validChannel.bytes + 1 }, 'byte-count mismatch is incomplete');
    assertIncompleteChannel({ ...validChannel, lines: validChannel.lines.trimEnd(), bytes: validChannel.bytes - 1 },
      'a missing final newline is incomplete');
    assertIncompleteChannel({ ...validChannel, diagnosticsComplete: false }, 'producer-incomplete status is retained');
    assertIncompleteChannel({ ...validChannel, ignored: 'extra channel property' },
      'extra channel fields are rejected');
    for (const badRecord of [
      `${C02_CONTROL_DIAGNOSTIC_PREFIX}{\n`,
      `${C02_CONTROL_DIAGNOSTIC_PREFIX}{"schema":"agent-harness.ah11-ops-c02-control-failure.v1","caseId":"OPS04.C02","boundary":"UNLISTED","errorClass":"SyntaxError"}\n`,
      `${C02_CONTROL_DIAGNOSTIC_PREFIX}{"schema":"agent-harness.ah11-ops-c02-control-failure.v1","caseId":"OPS04.C02","boundary":"C02_WATCHDOG_READY","errorClass":"PrivateError"}\n`,
      `${C02_CONTROL_DIAGNOSTIC_PREFIX}{"schema":"wrong-schema","caseId":"OPS04.C02","boundary":"C02_WATCHDOG_READY","errorClass":"SyntaxError"}\n`,
      `${diagnosticLine('C02_WATCHDOG_READY', 'SyntaxError', { path: '/private' })}\n`,
      `UNRELATED_PREFIX${diagnosticLine().slice(C02_CONTROL_DIAGNOSTIC_PREFIX.length)}\n`
    ]) assertIncompleteChannel(diagnosticChannel(badRecord), 'malformed JSON/prefix and invalid enum/class/schema/keys fail closed');

    const boundaryNames = [...C02_CONTROL_DIAGNOSTIC_BOUNDARIES];
    const validBoundaryChannel = diagnosticChannel(boundaryNames.map(boundary => `${diagnosticLine(boundary, 'Error')}\n`).join(''));
    assert.equal(parseC02ControlReadDiagnostics(validBoundaryChannel).complete, true,
      'each of the six fixed boundary enums is accepted');
    const errorClassNames = ['Error', 'RangeError', 'SyntaxError', 'TypeError'];
    const validErrorClasses = diagnosticChannel(errorClassNames.map(errorClass => `${diagnosticLine('C02_WATCHDOG_READY', errorClass)}\n`).join(''));
    assert.equal(parseC02ControlReadDiagnostics(validErrorClasses).complete, true,
      'each allowlisted original error class is accepted');

    const exactLine = diagnosticLine();
    assert.ok(Buffer.byteLength(exactLine, 'utf8') < 191);
    const exactLineWithPadding = `${exactLine}${' '.repeat(191 - Buffer.byteLength(exactLine, 'utf8'))}\n`;
    assert.equal(Buffer.byteLength(exactLineWithPadding, 'utf8'), 192);
    assert.equal(parseC02ControlReadDiagnostics(diagnosticChannel(exactLineWithPadding)).complete, true,
      'a 192-byte diagnostic line is accepted at the exact bound');
    assertIncompleteChannel(diagnosticChannel(`${exactLineWithPadding.slice(0, -1)} \n`),
      'a 193-byte diagnostic line is rejected');
    const eightLinesAtLimit = exactLineWithPadding.repeat(8);
    const eightRecordChannel = diagnosticChannel(eightLinesAtLimit);
    assert.equal(eightRecordChannel.bytes, 1_536);
    assert.equal(parseC02ControlReadDiagnostics(eightRecordChannel).complete, true,
      'eight 192-byte lines and 1,536 total bytes are accepted at their exact bounds');
    const nineRecordChannel = diagnosticChannel(`${Array(9).fill(`${diagnosticLine()}\n`).join('')}`);
    assertIncompleteChannel(nineRecordChannel, 'the ninth record fails the bounded record cap');
    assertIncompleteChannel({
      lines: 'x'.repeat(1_537), count: 8, bytes: 1_537, diagnosticsComplete: true
    }, 'the 1,537-byte total overflow fails before string splitting');

    const producerOverflowState = {
      c02ControlReadFailureLines: '', c02ControlReadFailureCount: 0,
      c02ControlReadFailureBytes: 0, c02ControlReadDiagnosticsComplete: true,
      ci1PhaseCaptureIncomplete: false
    };
    for (let index = 0; index < 8; index += 1) {
      assert.equal(recordC02ControlReadFailure('C02_WATCHDOG_READY', new SyntaxError('private'), producerOverflowState), true);
    }
    assert.equal(recordC02ControlReadFailure('C02_WATCHDOG_READY', new SyntaxError('private'), producerOverflowState), false);
    assert.equal(producerOverflowState.c02ControlReadFailureCount, 8);
    assert.equal(producerOverflowState.c02ControlReadDiagnosticsComplete, false);
    assert.equal(producerOverflowState.ci1PhaseCaptureIncomplete, true,
      'producer overflow fails closed in the shared custody completeness metric');
    assertIncompleteChannel({
      lines: producerOverflowState.c02ControlReadFailureLines,
      count: producerOverflowState.c02ControlReadFailureCount,
      bytes: producerOverflowState.c02ControlReadFailureBytes,
      diagnosticsComplete: producerOverflowState.c02ControlReadDiagnosticsComplete
    }, 'producer overflow remains an incomplete sanitized result');

    const failureDir = tempDirectory('ah11-jev-fixture-failure-', { ci1OwnedGroups: true });
    registerCi1OwnedTemporaryDirectoryFinalizer(t, cleanupCi1OwnedTemporaryDirectory, failureDir, METRICS);
    const markerDirectory = join(failureDir.directory, 'marker-is-a-directory');
    mkdirSync(markerDirectory);
    const resultPath = join(failureDir.directory, 'failure-result.json');
    const metricsPath = join(failureDir.directory, 'failure-metrics.json');
    const groupReadyPath = `${metricsPath}.group-ready.json`;
    const launchToken = randomUUID();
    assert.equal(beginCi1OwnedGroupLaunch(launchToken, failureDir.ci1Custody), true,
      'the failure launch intent and exact temp directory are recorded before spawn');
    assert.equal(markCi1OwnedGroupSpawnAttempt(failureDir.ci1Custody, launchToken), true,
      'the failure fixture spawn attempt is recorded before detached launch');
    const child = spawn(process.execPath, [
      join(ROOT, 'tests/operations/fixtures/jev-sigterm-child.mjs'),
      join(failureDir.directory, 'failure.sqlite'), 'failure-after-runner-pause',
      markerDirectory, resultPath, metricsPath
    ], {
      cwd: ROOT,
      env: {
        ...baseEnvironment(), JEV_OFFLINE_METRICS_FILE: metricsPath,
        OPS_CI1_GROUP_READY_FILE: groupReadyPath
      },
      stdio: ['ignore', 'ignore', 'pipe'], detached: true
    });
    let stderr = '';
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', chunk => { stderr += chunk; });
    const closed = new Promise(resolvePromise => child.once('close', (code, signal) => resolvePromise({ code, signal })));
    rememberCi1OwnedGroupChild(failureDir.ci1Custody, launchToken, child, closed);
    let runnerPid = null;
    let ownedGroupId = null;
    let ownedGroupEmpty = false;
    try {
      const ownership = await verifyRegisterCi1OwnedGroupBeforeReady({
        child, launchToken, readyPath: groupReadyPath, custody: failureDir.ci1Custody, closed
      });
      ownedGroupId = ownership.groupId;
      assert.equal(ownership.stage, 'ready', 'the failure fixture process group was verified and registered before release');
      const runnerMarker = `${metricsPath}.runner`;
      await waitForPath(runnerMarker, child, 5_000);
      runnerPid = Number(readC02ControlJson(runnerMarker, 'C02_RUNNER_READY').runnerPid);
      assert.ok(Number.isSafeInteger(runnerPid) && runnerPid > 1);
      const exit = await waitForCloseBounded(closed, 3_000);
      assert.ok(exit, `The failing owned fixture did not close within its bound: ${stderr.slice(0, 2048)}`);
      assert.equal(exit.code, 1, stderr.slice(0, 2048));
      assert.match(stderr, /EISDIR/u);
      assert.equal(existsSync(resultPath), false);
      assert.equal(existsSync(metricsPath), false);
      assert.equal(existsSync(`${markerDirectory}.${child.pid}.tmp`), false);
      assert.deepEqual(readdirSync(markerDirectory), []);
      assert.throws(() => process.kill(runnerPid, 0), error => error.code === 'ESRCH');
      const raw = new DatabaseSync(join(failureDir.directory, 'failure.sqlite'), { readOnly: true });
      const job = raw.prepare("SELECT job_id, workflow_id, state, runner_pid FROM operations_work WHERE idempotency_key='jev-composition-failure-after-runner-pause'").get();
      assert.equal(job.state, 'queued');
      assert.equal(job.runner_pid, null);
      assert.deepEqual({ ...raw.prepare('SELECT pending_records, in_flight FROM operations_meta WHERE singleton=1').get() }, {
        pending_records: 1, in_flight: 0
      });
      assert.equal(Number(raw.prepare('SELECT COUNT(*) AS count FROM operation_attempts WHERE workflow_id=?').get(job.workflow_id).count), 0);
      raw.close();
      ownedGroupEmpty = await waitForOwnedProcessGroupEmpty(child.pid);
      assert.equal(ownedGroupEmpty, true, 'the fixture group is empty before outer cleanup');
      if (ownedGroupEmpty) assert.equal(finishCi1OwnedGroupAfterAbsence(child.pid, launchToken, failureDir.ci1Custody), true);
    } finally {
      if (!ownedGroupEmpty) {
        const cleanup = await cleanupVerifiedCi1ProcessGroup(child, closed, ownedGroupId);
        ownedGroupEmpty = cleanup.groupEmpty;
        if (ownedGroupEmpty) finishCi1OwnedGroupAfterAbsence(child.pid, launchToken, failureDir.ci1Custody);
      }
    }

    for (const markerMode of ['missing', 'partial']) {
      const mode = `failure-runner-marker-${markerMode}`;
      const markerDir = tempDirectory(`ah11-jev-marker-${markerMode}-`, { ci1OwnedGroups: true });
      registerCi1OwnedTemporaryDirectoryFinalizer(t, cleanupCi1OwnedTemporaryDirectory, markerDir, METRICS);
      const databasePath = join(markerDir.directory, 'marker-control.sqlite');
      const markerPath = join(markerDir.directory, 'marker-control.json');
      const resultPath = join(markerDir.directory, 'marker-result.json');
      const metricsPath = join(markerDir.directory, 'marker-metrics.json');
      const groupReadyPath = `${metricsPath}.group-ready.json`;
      const launchToken = randomUUID();
      assert.equal(beginCi1OwnedGroupLaunch(launchToken, markerDir.ci1Custody), true,
        'the marker-control launch intent and exact temp directory are recorded before spawn');
      assert.equal(markCi1OwnedGroupSpawnAttempt(markerDir.ci1Custody, launchToken), true,
        'the marker-control spawn attempt is recorded before detached launch');
      const child = spawn(process.execPath, [
        join(ROOT, 'tests/operations/fixtures/jev-sigterm-child.mjs'),
        databasePath, mode, markerPath, resultPath, metricsPath
      ], {
        cwd: ROOT,
        env: {
          ...baseEnvironment(), JEV_OFFLINE_METRICS_FILE: metricsPath,
          OPS_CI1_GROUP_READY_FILE: groupReadyPath
        },
        stdio: ['ignore', 'ignore', 'pipe'], detached: true
      });
      let stderr = '';
      child.stderr.setEncoding('utf8');
      child.stderr.on('data', chunk => { stderr += chunk; });
      const closed = new Promise(resolvePromise => child.once('close', (code, signal) => resolvePromise({ code, signal })));
      rememberCi1OwnedGroupChild(markerDir.ci1Custody, launchToken, child, closed);
      let runnerPid = null;
      let ownedGroupId = null;
      let ownedGroupEmpty = false;
      try {
        const ownership = await verifyRegisterCi1OwnedGroupBeforeReady({
          child, launchToken, readyPath: groupReadyPath, custody: markerDir.ci1Custody, closed
        });
        ownedGroupId = ownership.groupId;
        assert.equal(ownership.stage, 'ready', 'the marker-control group was verified and registered before release');
        await waitForPath(markerPath, child, 5_000);
        assert.deepEqual(readC02ControlJson(markerPath, 'C02_MARKER_CONTROL_READY'), {
          phase: mode, stage: 'runner-marker-ready'
        });
        const runnerMarker = `${metricsPath}.runner`;
        if (markerMode === 'missing') assert.equal(existsSync(runnerMarker), false);
        else assert.equal(readFileSync(runnerMarker, 'utf8'), '{"runnerPid":');
        const expectedNegativeParseLines = METRICS.c02ControlReadFailureLines;
        assert.throws(() => JSON.parse(readFileSync(runnerMarker, 'utf8')),
          markerMode === 'missing' ? error => error.code === 'ENOENT' : SyntaxError);
        assert.equal(METRICS.c02ControlReadFailureLines, expectedNegativeParseLines,
          'the intentional missing/partial runner-marker assertion remains silent in unexpected-read diagnostics');
        assert.equal(runnerPid, null, 'marker failure occurs before runner PID assignment');

        runnerPid = readRunnerPidFromOwnedDatabase(databasePath, `jev-composition-${mode}`);
        assert.ok(Number.isSafeInteger(runnerPid) && runnerPid > 1);
        assert.equal(processGroupId(runnerPid), child.pid, 'durable runner PID belongs to this isolated fixture group');
        assert.equal(child.kill('SIGTERM'), true);
        const exit = await waitForCloseBounded(closed, 3_000);
        assert.ok(exit, `The marker-failure fixture did not close within its bound: ${stderr.slice(0, 2_048)}`);
        assert.equal(exit.code, 1, stderr.slice(0, 2_048));
        assert.match(stderr, /Injected (?:partial|missing) runner marker failure/u);
        assert.equal(await waitForPidExit(runnerPid), true, 'fixture finally reaps the exact durable runner PID');
        ownedGroupEmpty = await waitForOwnedProcessGroupEmpty(child.pid);
        assert.equal(ownedGroupEmpty, true, 'no owned descendants remain before outer cleanup');
        if (ownedGroupEmpty) assert.equal(finishCi1OwnedGroupAfterAbsence(child.pid, launchToken, markerDir.ci1Custody), true);

        const reopened = new DatabaseSync(databasePath, { readOnly: true });
        const job = reopened.prepare(
          'SELECT job_id, workflow_id, state, runner_pid FROM operations_work WHERE idempotency_key=?'
        ).get(`jev-composition-${mode}`);
        assert.equal(job.state, 'queued');
        assert.equal(job.runner_pid, null);
        assert.deepEqual({ ...reopened.prepare(
          'SELECT pending_records, in_flight FROM operations_meta WHERE singleton=1'
        ).get() }, { pending_records: 1, in_flight: 0 });
        assert.equal(Number(reopened.prepare(
          'SELECT COUNT(*) AS count FROM operation_attempts WHERE workflow_id=?'
        ).get(job.workflow_id).count), 0);
        reopened.close();
        assert.equal(existsSync(resultPath), false);
        assert.equal(existsSync(metricsPath), false);
      } finally {
        if (!ownedGroupEmpty) {
          const cleanup = await cleanupVerifiedCi1ProcessGroup(child, closed, ownedGroupId);
          ownedGroupEmpty = cleanup.groupEmpty;
          if (ownedGroupEmpty) finishCi1OwnedGroupAfterAbsence(child.pid, launchToken, markerDir.ci1Custody);
        }
      }
    }

    const freshCaptureState = () => ({
      ci1PhaseCaptures: [], ci1PhaseCaptureBytes: 0,
      ci1PhaseCaptureOverflow: false, ci1PhaseCaptureIncomplete: false,
      ci1PhaseCaptureChildCloseUnconfirmed: false,
      ci1PhaseCaptureOwnedGroupCloseUnconfirmed: false
    });
    const simulatedToken = randomUUID();
    const simulatedRegistryPath = join(failureDir.directory, 'registry-protocol-control.json');
    const savedRegistryPath = process.env.OPS_CI1_OWNED_GROUP_REGISTRY;
    delete process.env.OPS_CI1_OWNED_GROUP_REGISTRY;
    assert.equal(beginCi1OwnedGroupLaunch(randomUUID()), false,
      'detached CI1 launch fails closed when the outer registry handoff is absent');
    assert.equal(updateCi1OwnedGroupRegistry(812_345, true, randomUUID()), false,
      'CI1 registration cannot silently succeed without the outer registry');
    process.env.OPS_CI1_OWNED_GROUP_REGISTRY = simulatedRegistryPath;
    try {
    writeFileSync(simulatedRegistryPath, JSON.stringify({
      schema: 'ops-ci1-owned-groups.v1', groups: [], pendingLaunches: []
    }), { mode: 0o600 });
    assert.equal(beginCi1OwnedGroupLaunch(simulatedToken), true,
      'the launch token is durable before the child spawn boundary');
    const simulatedPending = readCi1OwnedGroupRegistry(simulatedRegistryPath).pendingLaunches;
    assert.deepEqual(readCi1OwnedGroupRegistry(simulatedRegistryPath), {
      schema: 'ops-ci1-owned-groups.v1', groups: [], pendingLaunches: [simulatedToken]
    }, 'begin preserves the validated registry schema while recording a launch token');
    const simulatedOrder = [];
    let simulatedDescendantCreated = false;
    const simulatedChild = { pid: 812_345 };
    const simulatedPreRegistrationDeath = {
      groups: [], pendingLaunches: simulatedPending
    };
    assert.deepEqual(simulatedPreRegistrationDeath, {
      groups: [], pendingLaunches: [simulatedToken]
    }, 'a controller death between spawn and group registration leaves only an unresolved launch token');
    assert.equal(simulatedDescendantCreated, false,
      'the finite child readiness gate has not released descendant creation');

    const unverifiedOwnership = await verifyRegisterCi1OwnedGroupBeforeReady({
      child: simulatedChild,
      launchToken: simulatedToken,
      readyPath: 'inert-ready-path',
      verifyGroup: async () => { simulatedOrder.push('verify-leader-exit'); return null; },
      registerGroup: () => { simulatedOrder.push('register'); return true; },
      releaseReady: () => { simulatedDescendantCreated = true; }
    });
    assert.deepEqual(unverifiedOwnership, {
      groupId: null, registered: false, readyReleased: false, stage: 'group_unverified'
    }, 'an exited or unverified leader never releases the child gate');
    assert.deepEqual(simulatedOrder, ['verify-leader-exit']);
    assert.deepEqual(simulatedPending, [simulatedToken], 'unverified ownership keeps the pending token');
    assert.equal(simulatedDescendantCreated, false);

    const registrationOrder = [];
    const failedRegistration = await verifyRegisterCi1OwnedGroupBeforeReady({
      child: simulatedChild,
      launchToken: simulatedToken,
      readyPath: 'inert-ready-path',
      verifyGroup: async child => { registrationOrder.push('verify'); return child.pid; },
      registerGroup: (groupId, register, token) => {
        registrationOrder.push(`register:${groupId}:${register}:${token === simulatedToken}`);
        return false;
      },
      releaseReady: () => { simulatedDescendantCreated = true; }
    });
    assert.deepEqual(failedRegistration, {
      groupId: simulatedChild.pid, registered: false, readyReleased: false, stage: 'registration_failed'
    }, 'registry failure retains the pending token and blocks the ready gate');
    assert.deepEqual(registrationOrder, ['verify', `register:${simulatedChild.pid}:true:true`]);
    assert.deepEqual(simulatedPending, [simulatedToken]);
    assert.equal(simulatedDescendantCreated, false);

    writeFileSync(simulatedRegistryPath, '{truncated', { mode: 0o600 });
    assert.equal(beginCi1OwnedGroupLaunch(randomUUID()), false,
      'a malformed ownership registry blocks new launches without being overwritten');
    const unreadableRegistryOwnership = await verifyRegisterCi1OwnedGroupBeforeReady({
      child: simulatedChild,
      launchToken: simulatedToken,
      readyPath: 'inert-ready-path',
      verifyGroup: async child => child.pid,
      registerGroup: updateCi1OwnedGroupRegistry,
      releaseReady: () => { simulatedDescendantCreated = true; }
    });
    assert.deepEqual(unreadableRegistryOwnership, {
      groupId: simulatedChild.pid, registered: false, readyReleased: false, stage: 'registration_failed'
    }, 'an unreadable ownership registry does not discard the unknown token or release the child');
    assert.equal(readCi1OwnedGroupRegistry(simulatedRegistryPath), null);
    assert.equal(simulatedDescendantCreated, false);
    writeFileSync(simulatedRegistryPath, JSON.stringify({
      schema: 'ops-ci1-owned-groups.v1', groups: [], pendingLaunches: [simulatedToken]
    }), { mode: 0o600 });

    const readyReleaseToken = randomUUID();
    assert.equal(beginCi1OwnedGroupLaunch(readyReleaseToken), true);
    const readyReleaseFailure = await verifyRegisterCi1OwnedGroupBeforeReady({
      child: simulatedChild,
      launchToken: readyReleaseToken,
      readyPath: 'inert-ready-path',
      verifyGroup: async child => child.pid,
      registerGroup: (groupId, register, token) => updateCi1OwnedGroupRegistry(groupId, register, token),
      releaseReady: () => { throw new Error('fixture-ready-write-failed'); }
    });
    assert.deepEqual(readyReleaseFailure, {
      groupId: simulatedChild.pid, registered: true, readyReleased: false, stage: 'ready_release_failed'
    }, 'a ready-file write failure retains the registered group for bounded cleanup');
    assert.deepEqual(readCi1OwnedGroupRegistry(simulatedRegistryPath), {
      schema: 'ops-ci1-owned-groups.v1',
      groups: [simulatedChild.pid], pendingLaunches: [simulatedToken]
    }, 'registering one token preserves an unrelated unresolved launch');
    assert.equal(updateCi1OwnedGroupRegistry(simulatedChild.pid, false, readyReleaseToken), true,
      'the ready-release failure record is removed only after its simulated group absence proof');
    assert.deepEqual(readCi1OwnedGroupRegistry(simulatedRegistryPath), {
      schema: 'ops-ci1-owned-groups.v1', groups: [], pendingLaunches: [simulatedToken]
    }, 'removing the failed group preserves the unrelated unresolved launch and schema');

    const successfulOrder = [];
    const verifiedOwnership = await verifyRegisterCi1OwnedGroupBeforeReady({
      child: simulatedChild,
      launchToken: simulatedToken,
      readyPath: 'inert-ready-path',
      verifyGroup: async child => { successfulOrder.push('verify'); return child.pid; },
      registerGroup: (groupId, register, token) => {
        successfulOrder.push(`register:${groupId}:${register}:${token === simulatedToken}`);
        return updateCi1OwnedGroupRegistry(groupId, register, token);
      },
      releaseReady: (path, value) => {
        successfulOrder.push(`ready:${path}`);
        assert.deepEqual(value, {
          schema: 'ops-ci1-group-ready.v1', pid: simulatedChild.pid,
          groupId: simulatedChild.pid, registryConfirmed: true
        });
        simulatedDescendantCreated = true;
      }
    });
    assert.deepEqual(verifiedOwnership, {
      groupId: simulatedChild.pid, registered: true, readyReleased: true, stage: 'ready'
    });
    assert.deepEqual(successfulOrder, [
      'verify', `register:${simulatedChild.pid}:true:true`, 'ready:inert-ready-path'
    ], 'registration is complete before the readiness gate is released');
    assert.equal(simulatedDescendantCreated, true);
    assert.deepEqual(readCi1OwnedGroupRegistry(simulatedRegistryPath), {
      schema: 'ops-ci1-owned-groups.v1',
      groups: [simulatedChild.pid], pendingLaunches: []
    }, 'the durable pending token is replaced by the verified group before readiness release');
    assert.equal(updateCi1OwnedGroupRegistry(simulatedChild.pid, false, simulatedToken), true,
      'the verified group record clears only after the test has established absence');
    assert.deepEqual(readCi1OwnedGroupRegistry(simulatedRegistryPath), {
      schema: 'ops-ci1-owned-groups.v1', groups: [], pendingLaunches: []
    });
    } finally {
      if (savedRegistryPath === undefined) delete process.env.OPS_CI1_OWNED_GROUP_REGISTRY;
      else process.env.OPS_CI1_OWNED_GROUP_REGISTRY = savedRegistryPath;
    }

    const shutdownStages = [
      'fixture_started', 'store_opened', 'worker_started', 'runner_claimed', 'workflow_opened', 'marker_written',
      'signal_seen', 'stop_requested', 'stop_settled', 'loop_settled', 'shutdown_verified',
      'store_close_started', 'store_closed', 'fixture_finally_started', 'fixture_finally_complete'
    ];
    const restartStages = [
      'fixture_started', 'restart_worker_started', 'restart_work_completed', 'restart_stop_settled',
      'restart_store_closed', 'restart_loop_settled', 'fixture_finally_started', 'fixture_finally_complete'
    ];
    const makeChildRows = (role, stages = role === 'shutdown' ? shutdownStages : restartStages, flags = {}) => stages.map((stage, index) => {
      const row = { schema: 1, sequence: index + 1, stage, elapsedMs: index };
      if (stage === 'stop_settled' || stage === 'restart_stop_settled') {
        Object.assign(row, { stopped: true, childrenReaped: true, storeClosed: true }, flags);
      }
      return row;
    });
    const encodeRows = rows => Buffer.from(`${rows.map(row => JSON.stringify(row)).join('\n')}\n`);
    const fullShutdownBytes = encodeRows(makeChildRows('shutdown'));
    const fullRestartBytes = encodeRows(makeChildRows('restart'));
    const retainedTrueState = freshCaptureState();
    const retainedTrue = parseCi1ChildPhaseCaptureBytes(fullShutdownBytes, 'before-marker', 'shutdown', retainedTrueState);
    assert.equal(retainedTrue.complete, true);
    assert.deepEqual(retainedTrueState.ci1PhaseCaptures.find(record => record.stage === 'stop_settled'), {
      schema: 1, source: 'child', phase: 'before-marker', role: 'shutdown', sequence: 9,
      stage: 'stop_settled', elapsedMs: 8, stopped: true, childrenReaped: true, storeClosed: true
    });
    assert.equal(parseCi1ChildPhaseCaptureBytes(fullRestartBytes, 'before-marker', 'restart', freshCaptureState()).complete, true);

    for (const flag of ['stopped', 'childrenReaped', 'storeClosed']) {
      const falseState = freshCaptureState();
      const falseCapture = parseCi1ChildPhaseCaptureBytes(
        encodeRows(makeChildRows('shutdown', shutdownStages, { [flag]: false })),
        'after-write', 'shutdown', falseState
      );
      const retainedFalse = falseState.ci1PhaseCaptures.find(record => record.stage === 'stop_settled');
      assert.equal(falseCapture.complete, false, `${flag}=false is a failed result after custody`);
      assert.equal(retainedFalse[flag], false, `${flag}=false is retained as a primitive boolean`);
      assert.equal(falseState.ci1PhaseCaptureIncomplete, true);
    }

    for (const malformedKind of ['nonboolean', 'missing', 'extra']) {
      const rows = makeChildRows('shutdown');
      const stop = rows.find(row => row.stage === 'stop_settled');
      if (malformedKind === 'nonboolean') stop.stopped = 1;
      if (malformedKind === 'missing') delete stop.storeClosed;
      if (malformedKind === 'extra') stop.unexpected = true;
      const state = freshCaptureState();
      const parsed = parseCi1ChildPhaseCaptureBytes(encodeRows(rows), 'after-write', 'shutdown', state);
      assert.equal(parsed.complete, false, `${malformedKind} stop result is rejected`);
      assert.equal(state.ci1PhaseCaptureIncomplete, true);
      assert.equal(state.ci1PhaseCaptures.some(record => record.stage === 'stop_settled'), false);
      assert.equal(state.ci1PhaseCaptures.length, 8, 'valid rows before malformed stop evidence remain retained');
    }

    const partialState = freshCaptureState();
    const partial = parseCi1ChildPhaseCaptureBytes(encodeRows(makeChildRows('shutdown').slice(0, 5)),
      'after-marker-before-bytes', 'shutdown', partialState);
    assert.equal(partial.complete, false, 'a nonzero child partial prefix remains incomplete');
    assert.equal(partialState.ci1PhaseCaptures.length, 5, 'the valid partial prefix remains available');
    assert.equal(partialState.ci1PhaseCaptureIncomplete, true);

    const malformedLaterState = freshCaptureState();
    const prefixRows = makeChildRows('shutdown').slice(0, 4);
    const malformedLater = Buffer.concat([encodeRows(prefixRows), Buffer.from('{\n')]);
    const malformedLaterResult = parseCi1ChildPhaseCaptureBytes(malformedLater, 'after-write', 'shutdown', malformedLaterState);
    assert.equal(malformedLaterResult.failureCode, 'malformed');
    assert.equal(malformedLaterState.ci1PhaseCaptures.length, 4, 'malformed later rows do not erase the valid prefix');
    assert.equal(malformedLaterState.ci1PhaseCaptureIncomplete, true);

    const negativeRows = makeChildRows('shutdown');
    negativeRows[0].elapsedMs = -1;
    const negativeState = freshCaptureState();
    const negative = parseCi1ChildPhaseCaptureBytes(encodeRows(negativeRows), 'after-write', 'shutdown', negativeState);
    assert.equal(negative.failureCode, 'clock');
    assert.equal(negativeState.ci1PhaseCaptures.length, 0, 'negative first clock is rejected');
    assert.equal(parseCi1ChildPhaseCaptureBytes(fullShutdownBytes, 'after-write', 'shutdown', freshCaptureState()).complete, true,
      'zero first clock is accepted');
    const backwardsRows = makeChildRows('shutdown');
    backwardsRows[0].elapsedMs = 1;
    backwardsRows[1].elapsedMs = 0;
    const backwardsState = freshCaptureState();
    assert.equal(parseCi1ChildPhaseCaptureBytes(encodeRows(backwardsRows), 'after-write', 'shutdown', backwardsState).failureCode, 'clock');
    assert.equal(backwardsState.ci1PhaseCaptures.length, 1, 'a later backwards clock preserves the valid prefix');

    const exactChildRows = makeChildRows('shutdown', [
      ...shutdownStages.slice(0, 13), 'runner_escalation_sent', ...shutdownStages.slice(13)
    ]);
    const exactChildBase = encodeRows(exactChildRows);
    const exactChildBytes = Buffer.concat([
      exactChildBase.subarray(0, exactChildBase.length - 1),
      Buffer.alloc(4_096 - exactChildBase.length, 0x20), Buffer.from('\n')
    ]);
    assert.equal(exactChildBytes.length, 4_096);
    const exactChildState = freshCaptureState();
    assert.equal(parseCi1ChildPhaseCaptureBytes(exactChildBytes, 'before-marker', 'shutdown', exactChildState).complete, true);
    assert.equal(exactChildState.ci1PhaseCaptureOverflow, false, 'the exact 4,096-byte child boundary is accepted');
    const oneOverChildBytes = Buffer.concat([
      exactChildBase.subarray(0, exactChildBase.length - 1),
      Buffer.alloc(4_097 - exactChildBase.length, 0x20), Buffer.from('\n')
    ]);
    assert.equal(oneOverChildBytes.length, 4_097);
    const oneOverChildState = freshCaptureState();
    const oneOverChild = parseCi1ChildPhaseCaptureBytes(oneOverChildBytes, 'before-marker', 'shutdown', oneOverChildState);
    assert.equal(oneOverChild.complete, false);
    assert.equal(oneOverChild.failureCode, 'child_byte_limit');
    assert.equal(oneOverChildState.ci1PhaseCaptureOverflow, true, 'one byte over the child cap is explicit overflow');
    assert.ok(oneOverChildState.ci1PhaseCaptures.length > 0, 'bounded complete prefix survives child overflow');

    const exactRecordStages = [...shutdownStages.slice(0, 13), 'runner_escalation_sent', ...shutdownStages.slice(13)];
    const exactRecordState = freshCaptureState();
    assert.equal(parseCi1ChildPhaseCaptureBytes(encodeRows(makeChildRows('shutdown', exactRecordStages)),
      'before-marker', 'shutdown', exactRecordState).complete, true);
    assert.equal(exactRecordState.ci1PhaseCaptures.length, 16);
    assert.equal(exactRecordState.ci1PhaseCaptureOverflow, false, 'exactly 16 child records are accepted');
    const overRecordState = freshCaptureState();
    const overRecordStages = [...exactRecordStages, 'runner_escalation_sent'];
    const overRecord = parseCi1ChildPhaseCaptureBytes(encodeRows(makeChildRows('shutdown', overRecordStages)),
      'before-marker', 'shutdown', overRecordState);
    assert.equal(overRecord.complete, false);
    assert.equal(overRecord.failureCode, 'child_record_limit');
    assert.equal(overRecordState.ci1PhaseCaptures.length, 16);
    assert.equal(overRecordState.ci1PhaseCaptureOverflow, true, 'the seventeenth child record is explicit overflow');

    const missingFileState = freshCaptureState();
    const missingFile = readCi1ChildPhaseCapture(join(failureDir.directory, 'missing-phase-capture.jsonl'),
      'before-marker', 'shutdown', false, missingFileState);
    assert.equal(missingFile.failureCode, 'missing');
    assert.equal(missingFileState.ci1PhaseCaptureIncomplete, true, 'missing phase custody is not reported as complete');

    for (const { role, timeoutMs, escalationStage } of [
      { role: 'shutdown', timeoutMs: 3_000, escalationStage: 'shutdown_escalation_sent' },
      { role: 'restart', timeoutMs: 8_000, escalationStage: 'restart_escalation_sent' }
    ]) {
      const timeoutState = freshCaptureState();
      const timeoutControl = { requestedMs: null, escalationStage: null, reaped: false };
      const timeoutOutcome = await waitCi1ChildClose({
        timeoutMs, escalationStage,
        waitForClose: async requestedMs => { timeoutControl.requestedMs = requestedMs; return null; },
        reapOwnedChild: async () => { timeoutControl.reaped = true; return { code: null, signal: 'SIGKILL' }; },
        captureParent: stage => { timeoutControl.escalationStage = stage; return true; },
        state: timeoutState
      });
      assert.equal(timeoutOutcome.timedOut, true, `${role} timeout remains a failure even after successful escalation`);
      assert.equal(timeoutOutcome.closeResult.signal, 'SIGKILL', `${role} escalated owned child is confirmed closed`);
      assert.equal(timeoutControl.requestedMs, timeoutMs, `${role} deadline remains unchanged`);
      assert.equal(timeoutControl.escalationStage, escalationStage);
      assert.equal(timeoutControl.reaped, true);
      assert.equal(timeoutState.ci1PhaseCaptureIncomplete, true, `${role} timeout is retained as incomplete status`);
    }

    const finalizationState = freshCaptureState();
    const finalizationOrder = [];
    const finalized = await finalizeCi1ChildCapture({
      phase: 'after-write', role: 'shutdown', closeResult: null,
      closeOwnedChild: async () => { finalizationOrder.push('reaped'); return { code: 1, signal: null }; },
      readCapture: () => { finalizationOrder.push('capture'); return { complete: false, failureCode: 'truncated' }; },
      captureParent: stage => {
        finalizationOrder.push(stage);
        if (stage === 'cleanup_escalation_sent') {
          finalizationState.ci1PhaseCaptureOverflow = true;
          throw new Error('injected parent append failure');
        }
        return true;
      },
      state: finalizationState
    });
    assert.equal(finalized.reaped, true);
    assert.ok(finalizationOrder.indexOf('reaped') < finalizationOrder.indexOf('capture'),
      'owned child reaping finishes before capture ingestion');
    assert.equal(finalizationState.ci1PhaseCaptureOverflow, true);
    assert.equal(finalizationState.ci1PhaseCaptureIncomplete, true,
      'parent append failure and partial child evidence remain explicit while cleanup completes');

    const boundedCleanupState = freshCaptureState();
    const firstChildRecordBytes = Buffer.byteLength(JSON.stringify({
      schema: 1, source: 'child', phase: 'after-write', role: 'shutdown',
      sequence: 1, stage: 'fixture_started', elapsedMs: 0
    }));
    const parentCleanupRecordBytes = Buffer.byteLength(JSON.stringify({
      schema: 1, source: 'parent', phase: 'after-write', role: 'shutdown',
      sequence: 1, stage: 'cleanup_escalation_sent', elapsedMs: 0
    }));
    assert.ok(parentCleanupRecordBytes > firstChildRecordBytes,
      'the parent cleanup marker is the first aggregate append to exceed this exact capacity');
    const emptyPadding = { padding: '' };
    const padding = { padding: 'x'.repeat(CI1_MAX_PHASE_BYTES - firstChildRecordBytes - Buffer.byteLength(JSON.stringify(emptyPadding))) };
    assert.equal(appendBoundedCi1PhaseCapture(boundedCleanupState, padding), true);
    assert.equal(boundedCleanupState.ci1PhaseCaptureBytes, CI1_MAX_PHASE_BYTES - firstChildRecordBytes);
    const boundedCleanupOrder = [];
    const boundedCleanup = await finalizeCi1ChildCapture({
      phase: 'after-write', role: 'shutdown', closeResult: null,
      closeOwnedChild: async () => { boundedCleanupOrder.push('reaped'); return { code: 1, signal: null }; },
      readCapture: () => {
        boundedCleanupOrder.push('capture');
        return parseCi1ChildPhaseCaptureBytes(
          encodeRows(makeChildRows('shutdown').slice(0, 2)), 'after-write', 'shutdown', boundedCleanupState
        );
      },
      captureParent: stage => {
        const record = {
          schema: 1, source: 'parent', phase: 'after-write', role: 'shutdown', sequence: 1,
          stage, elapsedMs: 0
        };
        if (stage === 'cleanup_escalation_sent') boundedCleanupOrder.push('cleanup-append');
        return appendBoundedCi1PhaseCapture(boundedCleanupState, record);
      },
      state: boundedCleanupState
    });
    assert.equal(boundedCleanup.reaped, true);
    assert.ok(boundedCleanupOrder.indexOf('reaped') < boundedCleanupOrder.indexOf('capture'));
    assert.equal(boundedCleanupState.ci1PhaseCaptureOverflow, true);
    assert.equal(boundedCleanupState.ci1PhaseCaptureIncomplete, true);
    assert.equal(boundedCleanupState.ci1PhaseCaptures.filter(record => record.source === 'child').length, 1,
      'the bounded valid child prefix still fits after the cleanup marker overflow');

    const parentAssertionFailure = new Error('injected marker or timing assertion failure');
    const primaryState = freshCaptureState();
    const primaryOrder = [];
    let observedPrimaryFailure;
    try {
      await executeCi1ChildWithFinalization(
        async () => { primaryOrder.push('execute'); throw parentAssertionFailure; },
        async () => {
          primaryOrder.push('finalize');
          return finalizeCi1ChildCapture({
            phase: 'after-write', role: 'shutdown', closeResult: null,
            closeOwnedChild: async () => { primaryOrder.push('reaped'); return { code: 1, signal: null }; },
            readCapture: () => { primaryOrder.push('capture'); return { complete: false, failureCode: 'truncated' }; },
            captureParent: () => true,
            state: primaryState
          });
        },
        primaryState
      );
    } catch (error) { observedPrimaryFailure = error; }
    assert.equal(observedPrimaryFailure, parentAssertionFailure,
      'the original parent assertion remains the reported failure after capture cleanup');
    assert.deepEqual(primaryOrder, ['execute', 'finalize', 'reaped', 'capture']);
    assert.equal(primaryState.ci1PhaseCaptureIncomplete, true);

    const primaryAfterCleanupFailure = new Error('injected early child failure');
    const cleanupFailureState = freshCaptureState();
    let observedAfterCleanupFailure;
    try {
      await executeCi1ChildWithFinalization(
        async () => { throw primaryAfterCleanupFailure; },
        async () => {
          await finalizeCi1ChildCapture({
            phase: 'after-write', role: 'shutdown', closeResult: null,
            closeOwnedChild: async () => ({ code: 1, signal: null }),
            readCapture: () => ({ complete: false, failureCode: 'truncated' }),
            captureParent: () => false,
            state: cleanupFailureState
          });
          throw new Error('injected capture-finalization failure after bounded cleanup');
        },
        cleanupFailureState
      );
    } catch (error) { observedAfterCleanupFailure = error; }
    assert.equal(observedAfterCleanupFailure, primaryAfterCleanupFailure,
      'a cleanup failure cannot replace the original early child failure');
    assert.equal(cleanupFailureState.ci1PhaseCaptureIncomplete, true);

    const unconfirmedState = freshCaptureState();
    let unconfirmedRead = false;
    const unconfirmed = await finalizeCi1ChildCapture({
      phase: 'after-write', role: 'restart', closeResult: null,
      closeOwnedChild: async () => null,
      readCapture: () => { unconfirmedRead = true; return { complete: true }; },
      captureParent: () => true,
      state: unconfirmedState
    });
    assert.equal(unconfirmed.reaped, false);
    assert.equal(unconfirmedRead, true, 'bounded custody is still attempted after the final bounded reap attempt');
    assert.equal(unconfirmed.captureResult.complete, false, 'an unconfirmed close never yields complete evidence');
    assert.equal(unconfirmedState.ci1PhaseCaptureChildCloseUnconfirmed, true);
    assert.equal(unconfirmedState.ci1PhaseCaptureIncomplete, true,
      'unconfirmed close is explicit rather than a zero-survivor claim');

    const inertGroupDir = tempDirectory('ah11-jev-owned-group-finalizer-', { ci1OwnedGroups: true });
    registerCi1OwnedTemporaryDirectoryFinalizer(t, cleanupCi1OwnedTemporaryDirectory, inertGroupDir, METRICS);
    const watchdogControlPath = join(inertGroupDir.directory, 'watchdog-control.json');
    writeFileSync(watchdogControlPath, JSON.stringify({ state: 'armed' }), { mode: 0o600 });
    const watchdogStartedAt = performance.now();
    const watchdogLaunchToken = randomUUID();
    assert.equal(beginCi1OwnedGroupLaunch(watchdogLaunchToken, inertGroupDir.ci1Custody), true,
      'the local watchdog launch intent is registered before spawn');
    assert.equal(markCi1OwnedGroupSpawnAttempt(inertGroupDir.ci1Custody, watchdogLaunchToken), true,
      'the local watchdog spawn attempt is recorded before detached launch');
    const watchdog = startPrelaunchedOwnedGroupWatchdog(watchdogControlPath, 15_000);
    await watchdog.spawned;
    rememberCi1OwnedGroupChild(inertGroupDir.ci1Custody, watchdogLaunchToken, watchdog.child, watchdog.closed);
    const watchdogSpawnedAt = performance.now();
    await waitForPath(watchdog.readyPath, watchdog.child, 2_000);
    const watchdogReady = readC02ControlJson(watchdog.readyPath, 'C02_WATCHDOG_READY');
    assert.deepEqual(watchdogReady, {
      schema: 'ops-owned-group-watchdog-ready.v1',
      pid: watchdog.child.pid,
      groupId: watchdog.child.pid
    }, 'the independent local watchdog publishes an exact ready acknowledgement');
    const watchdogGroupId = await verifyOwnedProcessGroup(watchdog.child);
    assert.equal(watchdogGroupId, watchdog.child.pid,
      'the independent watchdog is itself in a separate owned group');
    const watchdogCustody = inertGroupDir.ci1Custody.launches.get(watchdogLaunchToken);
    watchdogCustody.groupId = watchdogGroupId;
    watchdogCustody.status = 'group_verified';
    assert.equal(writeCi1TemporaryCustody(inertGroupDir.ci1Custody), true,
      'the watchdog group identity is persisted before registry registration');
    assert.equal(updateCi1OwnedGroupRegistry(watchdogGroupId, true, watchdogLaunchToken), true,
      'the ready local watchdog group is registered before inert descendants can be created');
    assert.equal(markCi1OwnedGroupRegistered(inertGroupDir.ci1Custody, watchdogLaunchToken, watchdogGroupId), true,
      'the watchdog registration is persisted before an inert descendant can be created');

    const inertMarkerPath = join(inertGroupDir.directory, 'paused-descendant.json');
    const inertReadyPath = join(inertGroupDir.directory, 'inert-group-ready.json');
    const descendantSource = "process.on('SIGTERM',()=>{});setInterval(()=>{},1000);";
    const inertParentSource = `
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { closeSync, existsSync, openSync, renameSync, rmSync, writeSync } from 'node:fs';
import { performance } from 'node:perf_hooks';
const publishC02ControlJsonAtomically = ${publishC02ControlJsonAtomically.toString()};
const markerPath = process.argv[1];
const readyPath = process.argv[2];
const readyDeadline = performance.now() + 2000;
while (!existsSync(readyPath) && performance.now() < readyDeadline) await new Promise(resolvePromise => setTimeout(resolvePromise, 5));
if (!existsSync(readyPath)) process.exit(24);
const descendant = spawn(process.execPath, ['--input-type=module', '--eval', ${JSON.stringify(descendantSource)}], { stdio: 'ignore' });
descendant.once('spawn', () => {
  process.kill(descendant.pid, 'SIGSTOP');
  publishC02ControlJsonAtomically(markerPath, {
    parentPid: process.pid, descendantPid: descendant.pid, groupId: process.pid
  });
  process.exit(23);
});
`;
    const inertChildLaunchAt = performance.now();
    const inertLaunchToken = randomUUID();
    assert.equal(beginCi1OwnedGroupLaunch(inertLaunchToken, inertGroupDir.ci1Custody), true,
      'the inert parent launch intent is registered before spawn');
    assert.equal(markCi1OwnedGroupSpawnAttempt(inertGroupDir.ci1Custody, inertLaunchToken), true,
      'the inert parent spawn attempt is recorded before detached launch');
    const inertParent = spawn(process.execPath, ['--input-type=module', '--eval', inertParentSource, inertMarkerPath, inertReadyPath], {
      cwd: ROOT, env: baseEnvironment(), stdio: 'ignore', detached: true
    });
    const inertClosed = new Promise(resolvePromise => inertParent.once('close', (code, signal) => resolvePromise({ code, signal })));
    rememberCi1OwnedGroupChild(inertGroupDir.ci1Custody, inertLaunchToken, inertParent, inertClosed);
    let inertGroupId = null;
    let inertGroupEmpty = false;
    let watchdogGroupEmpty = false;
    let watchdogExit = null;
    let watchdogCompletionWaitExpired = false;
    let watchdogResultValid = false;
    const inertState = freshCaptureState();
    let inertFinalization;
    try {
      const inertOwnership = await verifyRegisterCi1OwnedGroupBeforeReady({
        child: inertParent, launchToken: inertLaunchToken, readyPath: inertReadyPath,
        custody: inertGroupDir.ci1Custody, closed: inertClosed,
        releaseReady: async (readyPath, ready) => {
          writeFileSync(watchdogControlPath, JSON.stringify({ state: 'watch', groupId: ready.groupId }), { mode: 0o600 });
          await waitForPath(watchdog.watchAckPath, watchdog.child, 2_000);
          assert.deepEqual(readC02ControlJson(watchdog.watchAckPath, 'C02_WATCHDOG_TARGET_ACK'), {
            schema: 'ops-owned-group-watchdog-target-ack.v1',
            pid: watchdog.child.pid, groupId: ready.groupId, verified: true
          }, 'the external watchdog confirms the exact registered target before the fixture gate opens');
          writeFileSync(readyPath, JSON.stringify(ready), { flag: 'wx', mode: 0o600 });
        }
      });
      inertGroupId = inertOwnership.groupId;
      assert.equal(inertOwnership.stage, 'ready', 'the inert group is registered before releasing its descendant gate');
      assert.deepEqual(readC02ControlJson(watchdog.watchAckPath, 'C02_WATCHDOG_TARGET_ACK'), {
        schema: 'ops-owned-group-watchdog-target-ack.v1',
        pid: watchdog.child.pid, groupId: inertGroupId, verified: true
      });
      await waitForPath(inertMarkerPath, inertParent, 2_000);
      const inertMarker = readC02ControlJson(inertMarkerPath, 'C02_INERT_DESCENDANT_MARKER');
      assert.equal(inertMarker.parentPid, inertParent.pid);
      assert.equal(inertMarker.groupId, inertGroupId);
      assert.equal(processGroupId(inertMarker.descendantPid), inertGroupId,
        'the stopped descendant belongs to the exact parent group');
      const inertExit = await waitForCloseBounded(inertClosed, 2_000);
      assert.deepEqual(inertExit, { code: 23, signal: null }, 'the injected early parent failure is preserved');
      assert.ok(ownedProcessGroupMembers(inertGroupId).includes(inertMarker.descendantPid),
        'the inert stopped descendant remains after its parent exits');
      assert.ok(watchdogStartedAt < watchdogSpawnedAt && watchdogSpawnedAt < inertChildLaunchAt,
        'the external cleanup watchdog was running before the owned child launched');

      const inertPrimaryFailure = new Error('injected inert parent marker failure');
      const inertOrder = [];
      const inertParentCapture = stage => {
        inertOrder.push(stage);
        return stage !== 'cleanup_escalation_sent';
      };
      let inertCaptureReads = 0;
      let observedInertFailure;
      try {
        await executeCi1ChildWithFinalization(
          async () => { inertOrder.push('parent-failure'); throw inertPrimaryFailure; },
          async () => {
            inertFinalization = await finalizeCi1ChildCapture({
              phase: 'after-write', role: 'shutdown', closeResult: inertExit,
              cleanupOwnedChild: async () => {
                inertOrder.push('owned-group-cleanup');
                const result = await cleanupVerifiedCi1ProcessGroup(
                  inertParent, inertClosed, inertGroupId,
                  inertParentCapture, inertState
                );
                inertGroupEmpty = result.groupEmpty;
                if (result.groupEmpty && !finishCi1OwnedGroupAfterAbsence(
                  inertParent.pid, inertLaunchToken, inertGroupDir.ci1Custody
                )) {
                  inertState.ci1PhaseCaptureIncomplete = true;
                }
                return result;
              },
              readCapture: () => {
                inertCaptureReads += 1;
                inertOrder.push('capture');
                assert.deepEqual(ownedProcessGroupMembers(inertGroupId), [],
                  'capture follows bounded whole-group cleanup and absence proof');
                return { complete: false, failureCode: 'injected_parent_failure' };
              },
              captureParent: inertParentCapture,
              state: inertState
            });
            return inertFinalization;
          },
          inertState
        );
      } catch (error) { observedInertFailure = error; }
      assert.equal(observedInertFailure, inertPrimaryFailure,
        'whole-group cleanup and capture cannot replace the original parent failure');
      assert.equal(inertFinalization.reaped, true);
      assert.equal(inertFinalization.ownedGroupEmpty, true);
      assert.equal(inertCaptureReads, 1, 'the bounded capture reader runs once after cleanup');
      assert.ok(inertOrder.indexOf('owned-group-cleanup') < inertOrder.indexOf('capture'));
      assert.equal(inertState.ci1PhaseCaptureOwnedGroupCloseUnconfirmed, false);
      assert.equal(inertState.ci1PhaseCaptureIncomplete, true,
        'the injected aggregate failure remains explicit even though whole-group cleanup and capture complete');
      assert.deepEqual(ownedProcessGroupMembers(inertGroupId), [], 'the exact owned process group is absent');
    } finally {
      try {
        if (!inertGroupEmpty && inertGroupId === inertParent.pid) {
          inertGroupEmpty = await cleanupOwnedProcessGroup(inertParent, inertClosed);
        }
      } catch { inertState.ci1PhaseCaptureIncomplete = true; }
      if (inertGroupEmpty && inertGroupId === inertParent.pid) {
        try {
          if (!finishCi1OwnedGroupAfterAbsence(inertParent.pid, inertLaunchToken, inertGroupDir.ci1Custody)) {
            inertState.ci1PhaseCaptureIncomplete = true;
          }
        } catch { inertState.ci1PhaseCaptureIncomplete = true; }
      }
      let controlState = inertGroupEmpty ? 'done' : 'cleanup';
      try {
        writeFileSync(watchdogControlPath, JSON.stringify({
          state: controlState,
          ...(inertGroupId === inertParent.pid ? { groupId: inertGroupId } : {})
        }), { mode: 0o600 });
      } catch {
        inertState.ci1PhaseCaptureIncomplete = true;
        controlState = 'write_failed';
      }
      const watchdogFinalization = await finishCi1OwnedGroupWatchdog(watchdog, watchdogGroupId);
      watchdogExit = watchdogFinalization.closeResult;
      watchdogGroupEmpty = watchdogFinalization.groupEmpty;
      watchdogCompletionWaitExpired = watchdogFinalization.completionWaitExpired;
      if (watchdogCompletionWaitExpired) inertState.ci1PhaseCaptureIncomplete = true;
      if (watchdogExit?.code !== 0 || !watchdogExit || !watchdogGroupEmpty) {
        inertState.ci1PhaseCaptureIncomplete = true;
      }
      if (watchdogGroupEmpty && watchdogGroupId === watchdog.child.pid) {
        try {
          if (!finishCi1OwnedGroupAfterAbsence(watchdogGroupId, watchdogLaunchToken, inertGroupDir.ci1Custody)) {
            inertState.ci1PhaseCaptureIncomplete = true;
          }
        } catch { inertState.ci1PhaseCaptureIncomplete = true; }
      }
      if (!inertGroupEmpty && inertGroupId === inertParent.pid && controlState !== 'write_failed') {
        try { inertGroupEmpty = await waitForOwnedProcessGroupEmpty(inertGroupId, 1_000); }
        catch { inertGroupEmpty = false; }
      }
      try {
        if (existsSync(watchdog.resultPath)) {
          const result = readC02ControlJson(watchdog.resultPath, 'C02_WATCHDOG_RESULT');
          watchdogResultValid = !watchdogCompletionWaitExpired
            && result.schema === 'ops-owned-group-watchdog-result.v1'
            && result.pid === watchdog.child.pid
            && result.groupId === watchdog.child.pid
            && result.timedOut === false
            && result.targetAckSent === true
            && result.targetAckPublicationFailed === false
            && result.cleanupRequested === (controlState === 'cleanup')
            && result.targetGroupId === (inertGroupId === inertParent.pid ? inertGroupId : null)
            && (result.targetGroupEmpty === null || result.targetGroupEmpty === true);
        }
      } catch { watchdogResultValid = false; }
      if (!watchdogResultValid) inertState.ci1PhaseCaptureIncomplete = true;
    }
    assert.equal(inertGroupEmpty, true, 'finite cleanup or the prelaunched watchdog proves group absence');
    assert.equal(watchdogExit?.code, 0, 'the local watchdog exits successfully after finite cleanup');
    assert.equal(watchdogGroupEmpty, true, 'the local watchdog group is confirmed absent');
    assert.equal(watchdogResultValid, true, 'the local watchdog result records exact target and cleanup state');
  });
  const reopened = openStore(temporary.databasePath);
  for (const result of results.filter(item => item.attemptCount > 0)) {
    const attempt = reopened.getAttempt(`ops-attempt-${result.phase}`);
    assert.equal(attempt.authorityHoldRef, `synthetic-local-bookkeeping:hold-${result.phase}`);
  }
  reopened.close();
});

test('OPS05_LOCAL_CRASH_CANCEL_FENCE executes and checkpoints an actual child plan, then rejects stale cancellation', async t => {
  const temporary = tempDirectory('ah11-worker-run-');
  t.after(() => temporary.cleanup());
  const scope = { tenantId: 'synthetic-tenant-resume', projectId: 'synthetic-project-resume' };
  const worker = createOperationsWorker({ databasePath: temporary.databasePath, ownerId: 'ops-worker:resume' });
  t.after(async () => { await worker.stop('test'); });
  void worker.start();
  const submitted = worker.submitSyntheticWork({ trustedSyntheticScope: scope, idempotencyKey: 'resume-1', planJson: PLAN });
  assert.equal(submitted.accepted, true);
  const done = await waitForStatus(worker, scope, submitted.jobId, 'completed');
  assert.equal(done.state, 'completed');
  const duplicate = worker.submitSyntheticWork({ trustedSyntheticScope: scope, idempotencyKey: 'resume-1', planJson: PLAN });
  assert.equal(duplicate.jobId, submitted.jobId);
  assert.equal(duplicate.duplicate, true);
  assert.throws(() => worker.cancelSyntheticWork({
    trustedSyntheticScope: scope, jobId: submitted.jobId, expectedGeneration: Math.max(0, done.generation - 1)
  }), error => error.code === 'CONCURRENT_MODIFICATION');
  const repeatedCompletedCancel = worker.cancelSyntheticWork({
    trustedSyntheticScope: scope, jobId: submitted.jobId, expectedGeneration: done.generation
  });
  assert.deepEqual(repeatedCompletedCancel, { accepted: false, state: 'completed', generation: done.generation });
  const stopped = await worker.stop('test');
  assert.equal(stopped.stopped, true);
  assert.equal(worker.activeChildCount, 0);

  const reopened = openStore(temporary.databasePath);
  const workflow = reopened.getWorkflow(submitted.workflowId);
  const checkpoint = reopened.getCheckpoint(submitted.workflowId);
  assert.equal(workflow.state, 'succeeded');
  assert.equal(JSON.parse(checkpoint.data.completedResultsJson).sum.output, 42);
  reopened.close();
  assert.equal(METRICS.providerCalls, 0);

  await t.test('OPS05.C01 reaps an uncooperative owned child at its monotonic deadline and settles an expired current fence', async t => {
    const deadlineDir = tempDirectory('ah11-runtime-deadline-');
    t.after(() => deadlineDir.cleanup());
    const deadlineWorker = createOperationsWorker({
      databasePath: deadlineDir.databasePath,
      ownerId: 'review-worker',
      config: { maxJobRuntimeMs: 250, childKillGraceMs: 40, childReapReserveMs: 200, shutdownTimeoutMs: 1_000 }
    });
    const stopWorker = async () => { if (deadlineWorker.lifecycle !== 'stopped') await deadlineWorker.stop('test'); };
    t.after(stopWorker);
    void deadlineWorker.start();
    const deadlineScope = { tenantId: 'deadline-tenant', projectId: 'deadline-project' };
    const deadlinePlan = JSON.stringify({
      kind: 'agent-harness.private-synthetic-plan', version: 1,
      steps: Array.from({ length: 32 }, (_, index) => ({
        id: `d${String(index).padStart(2, '0')}`, operation: 'text.concat',
        dependsOn: index % 12 === 0 ? [] : [`d${String(index - 1).padStart(2, '0')}`],
        args: { parts: index % 12 === 0 ? ['x'.repeat(512)] : [{ $result: `d${String(index - 1).padStart(2, '0')}` }, ''] }
      }))
    });
    const accepted = deadlineWorker.submitSyntheticWork({
      trustedSyntheticScope: deadlineScope, idempotencyKey: 'deadline-child', planJson: deadlinePlan,
      limitsJson: JSON.stringify({ maxElapsedMs: 200, maxStepMs: 50 })
    });
    assert.equal(accepted.accepted, true);
    const childDeadline = Date.now() + 3_000;
    while (Date.now() < childDeadline && deadlineWorker.activeChildCount === 0) await new Promise(resolvePromise => setTimeout(resolvePromise, 1));
    assert.equal(deadlineWorker.activeChildCount, 1);
    const pgrep = spawnSync('pgrep', ['-P', String(process.pid)], { encoding: 'utf8' });
    assert.equal(pgrep.status, 0, pgrep.stderr);
    const runnerPid = Number(pgrep.stdout.trim().split(/\s+/u).at(-1));
    assert.ok(Number.isSafeInteger(runnerPid) && runnerPid > 1);
    const deadlineObservedAt = performance.now();
    assert.equal(process.kill(runnerPid, 'SIGSTOP'), true);
    let runnerStopped = false;
    const runnerStopDeadline = Date.now() + 1_000;
    while (Date.now() < runnerStopDeadline) {
      const runnerState = spawnSync('ps', ['-o', 'stat=', '-p', String(runnerPid)], { encoding: 'utf8' });
      if (runnerState.status === 0 && /^T/u.test(runnerState.stdout.trim())) {
        runnerStopped = true;
        break;
      }
      await new Promise(resolvePromise => setTimeout(resolvePromise, 2));
    }
    assert.equal(runnerStopped, true, 'The owned runner must be stopped before its durable checkpoint is sampled.');
    const pausedDb = new DatabaseSync(deadlineDir.databasePath, { readOnly: true });
    const checkpointAtPause = pausedDb.prepare(`
      SELECT f.checkpoint_sequence, c.sequence, c.payload_json, c.sha256
      FROM workflows f JOIN workflow_checkpoints c
        ON c.workflow_id = f.workflow_id AND c.sequence = f.checkpoint_sequence
      WHERE f.workflow_id = ?
    `).get(accepted.workflowId);
    pausedDb.close();
    assert.ok(checkpointAtPause);
    assert.equal(Number(checkpointAtPause.checkpoint_sequence), Number(checkpointAtPause.sequence));
    const leaseDb = new DatabaseSync(deadlineDir.databasePath);
    leaseDb.prepare('UPDATE workflows SET lease_expires_ms = 0 WHERE workflow_id = ?').run(accepted.workflowId);
    leaseDb.close();
    const parked = await waitForStatus(deadlineWorker, deadlineScope, accepted.jobId, 'parked', 2_000);
    while (deadlineWorker.activeChildCount !== 0 && Date.now() < childDeadline + 2_000) await new Promise(resolvePromise => setTimeout(resolvePromise, 2));
    assert.equal(deadlineWorker.activeChildCount, 0);
    const actualCloseElapsedMs = performance.now() - deadlineObservedAt;
    METRICS.runnerDeadlineCloseElapsedMsMax = Math.max(METRICS.runnerDeadlineCloseElapsedMsMax, actualCloseElapsedMs);
    METRICS.runnerDeadlineCloseSamples += 1;
    assert.ok(actualCloseElapsedMs < 1_500, `Owned child close took ${actualCloseElapsedMs} ms after deadline monitoring began.`);
    assert.equal(parked.reasonCode, 'LOCAL_EXECUTION_STOPPED');
    assert.throws(() => process.kill(runnerPid, 0), error => error.code === 'ESRCH');
    assert.equal(deadlineWorker.health().process.lastSafeErrorCode, 'OPS_RUNNER_DEADLINE');
    const stopped = await deadlineWorker.stop('test');
    assert.equal(stopped.stopped, true);
    assert.equal(stopped.childrenReaped, true);
    const reopened = openStore(deadlineDir.databasePath);
    const workflow = reopened.getWorkflow(accepted.workflowId);
    const checkpoint = reopened.getCheckpoint(accepted.workflowId);
    const raw = new DatabaseSync(deadlineDir.databasePath, { readOnly: true });
    assert.equal(workflow.state, 'review_required');
    assert.equal(workflow.leaseOwner, null);
    assert.equal(raw.prepare('SELECT state FROM operations_work WHERE job_id = ?').get(accepted.jobId).state, 'parked');
    assert.equal(Number(raw.prepare('SELECT in_flight FROM operations_meta WHERE singleton=1').get().in_flight), 0);
    assert.equal(Number(raw.prepare('SELECT pending_records FROM operations_meta WHERE singleton=1').get().pending_records), 1);
    assert.equal(Number(raw.prepare('SELECT reserved_disk_bytes FROM operations_meta WHERE singleton=1').get().reserved_disk_bytes) > 0, true);
    assert.equal(Number(raw.prepare('SELECT COUNT(*) AS count FROM operation_attempts WHERE workflow_id=?').get(accepted.workflowId).count), 0);
    assert.equal(checkpoint.sequence, workflow.checkpointSequence);
    const checkpointAfterPark = raw.prepare(`
      SELECT f.checkpoint_sequence, c.sequence, c.payload_json, c.sha256
      FROM workflows f JOIN workflow_checkpoints c
        ON c.workflow_id = f.workflow_id AND c.sequence = f.checkpoint_sequence
      WHERE f.workflow_id = ?
    `).get(accepted.workflowId);
    assert.deepEqual({ ...checkpointAfterPark }, { ...checkpointAtPause });
    raw.close();
    reopened.close();
  });

  await t.test('OPS05.C02 active cancellation commits one coherent terminal state after actual child exit and reopen', async t => {
    const cancelDir = tempDirectory('ah11-active-cancel-');
    t.after(() => cancelDir.cleanup());
    const cancelWorker = createOperationsWorker({ databasePath: cancelDir.databasePath, ownerId: 'review-worker' });
    t.after(async () => { if (cancelWorker.lifecycle !== 'stopped') await cancelWorker.stop('test'); });
    void cancelWorker.start();
    const cancelScope = { tenantId: 'cancel-tenant', projectId: 'cancel-project' };
    const cancelPlan = JSON.stringify({
      kind: 'agent-harness.private-synthetic-plan', version: 1,
      steps: Array.from({ length: 32 }, (_, index) => ({
        id: `c${String(index).padStart(2, '0')}`, operation: 'text.concat',
        dependsOn: index % 12 === 0 ? [] : [`c${String(index - 1).padStart(2, '0')}`],
        args: { parts: index % 12 === 0 ? ['x'.repeat(512)] : [{ $result: `c${String(index - 1).padStart(2, '0')}` }, ''] }
      }))
    });
    const accepted = cancelWorker.submitSyntheticWork({
      trustedSyntheticScope: cancelScope, idempotencyKey: 'cancel-running', planJson: cancelPlan
    });
    assert.equal(accepted.accepted, true);
    const childDeadline = Date.now() + 3_000;
    while (Date.now() < childDeadline && cancelWorker.activeChildCount === 0) await new Promise(resolvePromise => setTimeout(resolvePromise, 1));
    assert.equal(cancelWorker.activeChildCount, 1);
    const pgrep = spawnSync('pgrep', ['-P', String(process.pid)], { encoding: 'utf8' });
    assert.equal(pgrep.status, 0, pgrep.stderr);
    const runnerPid = Number(pgrep.stdout.trim().split(/\s+/u).at(-1));
    assert.ok(Number.isSafeInteger(runnerPid) && runnerPid > 1);
    assert.equal(process.kill(runnerPid, 'SIGSTOP'), true);
    const requested = cancelWorker.cancelSyntheticWork({
      trustedSyntheticScope: cancelScope, jobId: accepted.jobId, expectedGeneration: 1
    });
    assert.deepEqual(requested, {
      accepted: true, state: 'cancellation_requested', generation: 1,
      ownerId: 'review-worker', fence: 1
    });
    const during = new DatabaseSync(cancelDir.databasePath, { readOnly: true });
    assert.equal(during.prepare('SELECT state, cancellation_requested FROM workflows WHERE workflow_id=?').get(accepted.workflowId).state, 'running');
    assert.equal(during.prepare('SELECT cancellation_requested FROM workflows WHERE workflow_id=?').get(accepted.workflowId).cancellation_requested, 1);
    assert.equal(during.prepare('SELECT state FROM operations_work WHERE job_id=?').get(accepted.jobId).state, 'running');
    during.close();
    assert.equal(process.kill(runnerPid, 'SIGCONT'), true);
    const cancelled = await waitForStatus(cancelWorker, cancelScope, accepted.jobId, 'cancelled', 3_000);
    assert.equal(cancelled.generation, 1);
    assert.equal(cancelWorker.activeChildCount, 0);
    const stopped = await cancelWorker.stop('test');
    assert.equal(stopped.stopped, true);
    const reopened = openStore(cancelDir.databasePath);
    const workflow = reopened.getWorkflow(accepted.workflowId);
    const raw = new DatabaseSync(cancelDir.databasePath, { readOnly: true });
    assert.equal(workflow.state, 'cancelled');
    assert.equal(workflow.cancellationRequested, true);
    assert.equal(raw.prepare('SELECT state FROM operations_work WHERE job_id=?').get(accepted.jobId).state, 'cancelled');
    assert.deepEqual({ ...raw.prepare('SELECT pending_records, pending_bytes, in_flight FROM operations_meta WHERE singleton=1').get() }, {
      pending_records: 0, pending_bytes: 0, in_flight: 0
    });
    assert.equal(Number(raw.prepare("SELECT COUNT(*) AS count FROM workflow_events WHERE workflow_id=? AND event_type='operations_work_cancelled'").get(accepted.workflowId).count), 1);
    const beforeRepeated = Number(raw.prepare('SELECT COUNT(*) AS count FROM workflow_events WHERE workflow_id=?').get(accepted.workflowId).count);
    raw.close();
    const repeated = reopened._operationsCancel(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: cancelScope.tenantId, projectId: cancelScope.projectId,
      jobId: accepted.jobId, expectedGeneration: 1
    });
    assert.deepEqual(repeated, { accepted: false, state: 'cancelled', generation: 1 });
    const afterRepeated = new DatabaseSync(cancelDir.databasePath, { readOnly: true });
    assert.equal(Number(afterRepeated.prepare('SELECT COUNT(*) AS count FROM workflow_events WHERE workflow_id=?').get(accepted.workflowId).count), beforeRepeated);
    afterRepeated.close();
    reopened.close();
  });

  await t.test('OPS05.C03 queued cancellation is durable and idempotent without creating a claim', () => {
    const queuedDir = tempDirectory('ah11-queued-cancel-');
    t.after(() => queuedDir.cleanup());
    const queuedStore = openStore(queuedDir.databasePath);
    t.after(() => queuedStore.close());
    const queued = admit(queuedStore, 'tenant-queued-cancel', 'project-queued-cancel', 'queued-cancel');
    const cancelled = queuedStore._operationsCancel(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'tenant-queued-cancel', projectId: 'project-queued-cancel',
      jobId: queued.jobId, expectedGeneration: 0
    });
    assert.deepEqual(cancelled, { accepted: true, state: 'cancelled', generation: 0 });
    assert.equal(queuedStore.getWorkflow(queued.workflowId).state, 'cancelled');
    assert.deepEqual(queuedStore._operationsCancel(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'tenant-queued-cancel', projectId: 'project-queued-cancel',
      jobId: queued.jobId, expectedGeneration: 0
    }), { accepted: false, state: 'cancelled', generation: 0 });
    const raw = new DatabaseSync(queuedDir.databasePath, { readOnly: true });
    assert.deepEqual({ ...raw.prepare('SELECT pending_records, pending_bytes, in_flight, reserved_event_slots, reserved_disk_bytes FROM operations_meta WHERE singleton=1').get() }, {
      pending_records: 0, pending_bytes: 0, in_flight: 0, reserved_event_slots: 0, reserved_disk_bytes: 0
    });
    raw.close();
  });

  await t.test('OPS05.C04 accepted nonprefixed and maximum-length owner IDs execute in the actual child runner', async t => {
    const invalidOwnerPath = join(temporary.directory, 'invalid-owner.sqlite');
    assert.throws(() => createOperationsWorker({ databasePath: invalidOwnerPath, ownerId: 'r'.repeat(129) }), /identity/u);
    assert.equal(existsSync(invalidOwnerPath), false);
    for (const [index, ownerId] of ['review-worker', 'r'.repeat(128)].entries()) {
      const ownerDir = tempDirectory(`ah11-owner-domain-${index}-`);
      t.after(() => ownerDir.cleanup());
      const ownerWorker = createOperationsWorker({ databasePath: ownerDir.databasePath, ownerId });
      t.after(async () => { if (ownerWorker.lifecycle !== 'stopped') await ownerWorker.stop('test'); });
      void ownerWorker.start();
      const ownerScope = { tenantId: `owner-tenant-${index}`, projectId: `owner-project-${index}` };
      const ownerJob = ownerWorker.submitSyntheticWork({
        trustedSyntheticScope: ownerScope, idempotencyKey: `owner-${index}`, planJson: PLAN
      });
      assert.equal(ownerJob.accepted, true);
      const finished = await waitForStatus(ownerWorker, ownerScope, ownerJob.jobId, 'completed');
      assert.equal(finished.state, 'completed');
      const stopResult = await ownerWorker.stop('test');
      assert.equal(stopResult.stopped, true);
      const reopened = openStore(ownerDir.databasePath);
      assert.equal(reopened.getWorkflow(ownerJob.workflowId).state, 'succeeded');
      assert.equal(reopened._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
        tenantId: ownerScope.tenantId, projectId: ownerScope.projectId, jobId: ownerJob.jobId
      }).state, 'completed');
      reopened.close();
    }
  });

  await t.test('OPS05.C05 expired cleanup from a superseded owner cannot mutate a newer claim', () => {
    const ownerDir = tempDirectory('ah11-superseded-owner-');
    t.after(() => ownerDir.cleanup());
    let ownerNow = 30_000;
    const ownerStore = openStore(ownerDir.databasePath, { clock: () => ownerNow });
    t.after(() => ownerStore.close());
    const accepted = admit(ownerStore, 'fence-tenant', 'fence-project', 'fence-race');
    const policy = operationsStoragePolicy(readOperationsConfig());
    const oldClaim = ownerStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'review-worker-old-owner', policy
    }).claim;
    ownerStore._operationsSetRunnerPid(OPERATIONS_STORAGE_CAPABILITY, {
      jobId: oldClaim.jobId, ownerId: oldClaim.ownerId,
      generation: oldClaim.generation, fence: oldClaim.fence, runnerPid: unusedProcessId()
    });
    ownerNow = oldClaim.leaseExpiresAtMs + 1;
    assert.equal(ownerStore._operationsExpiredClaims(OPERATIONS_STORAGE_CAPABILITY, { policy }).items[0].jobId, accepted.jobId);
    const newer = ownerStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'review-worker-new-owner', policy, recoverableJobIds: [accepted.jobId]
    }).claim;
    assert.equal(newer.generation, oldClaim.generation + 1);
    assert.equal(newer.fence, oldClaim.fence + 1);
    assert.throws(() => ownerStore._operationsFinish(OPERATIONS_STORAGE_CAPABILITY, {
      jobId: oldClaim.jobId, ownerId: oldClaim.ownerId,
      generation: oldClaim.generation, fence: oldClaim.fence, outcome: 'stopped', childExited: true
    }), error => error.code === 'STALE_FENCE');
    const current = new DatabaseSync(ownerDir.databasePath, { readOnly: true });
    const row = current.prepare('SELECT state, worker_id, generation, fence_token FROM operations_work WHERE job_id=?').get(accepted.jobId);
    assert.deepEqual({ ...row }, {
      state: 'running', worker_id: 'review-worker-new-owner',
      generation: newer.generation, fence_token: newer.fence
    });
    assert.equal(Number(current.prepare('SELECT in_flight FROM operations_meta WHERE singleton=1').get().in_flight), 1);
    current.close();
  });

  await t.test('OPS05.C09 expired recovery reuses an occupied global slot before another tenant claims', () => {
    const recoveryDir = tempDirectory('ah11-global-recovery-slot-');
    t.after(() => recoveryDir.cleanup());
    const configInput = {
      workerCount: 1, maxInFlightGlobal: 1, maxInFlightTenant: 1, maxInFlightProject: 1
    };
    const config = readOperationsConfig(configInput);
    const policy = operationsStoragePolicy(config);
    let now = 50_000;
    const store = openStore(recoveryDir.databasePath, { clock: () => now });
    t.after(() => store.close());
    const first = admit(store, 'tenant-a', 'project-a', 'expired-slot-a', PLAN, configInput);
    const oldClaim = store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'review-owner-a', policy
    }).claim;
    assert.equal(oldClaim.jobId, first.jobId);
    const oldRunnerPid = unusedProcessId();
    store._operationsSetRunnerPid(OPERATIONS_STORAGE_CAPABILITY, {
      jobId: oldClaim.jobId, ownerId: oldClaim.ownerId,
      generation: oldClaim.generation, fence: oldClaim.fence, runnerPid: oldRunnerPid
    });
    const second = admit(store, 'tenant-b', 'project-b', 'expired-slot-b', PLAN, configInput);

    now = oldClaim.leaseExpiresAtMs + 1;
    assert.throws(() => process.kill(oldRunnerPid, 0), error => error.code === 'ESRCH');
    const expired = store._operationsExpiredClaims(OPERATIONS_STORAGE_CAPABILITY, { policy });
    assert.equal(expired.incomplete, false);
    assert.equal(expired.items[0].jobId, first.jobId);
    assert.equal(expired.items[0].runnerPid, oldRunnerPid);
    assert.equal(store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'review-owner-b', policy
    }), null);

    const recovered = store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'review-owner-b', policy, recoverableJobIds: [first.jobId]
    });
    assert.equal(recovered.claim.jobId, first.jobId);
    assert.equal(recovered.claim.generation, oldClaim.generation + 1);
    assert.equal(recovered.claim.fence, oldClaim.fence + 1);
    const afterRecovery = new DatabaseSync(recoveryDir.databasePath, { readOnly: true });
    assert.equal(afterRecovery.prepare('SELECT state FROM operations_work WHERE job_id=?').get(second.jobId).state, 'queued');
    assert.deepEqual({ ...afterRecovery.prepare('SELECT pending_records, in_flight FROM operations_meta WHERE singleton=1').get() }, {
      pending_records: 2, in_flight: 1
    });
    assert.deepEqual({ ...afterRecovery.prepare('SELECT in_flight FROM operations_tenants WHERE tenant_id=?').get('tenant-a') }, { in_flight: 1 });
    assert.deepEqual({ ...afterRecovery.prepare('SELECT in_flight FROM operations_tenants WHERE tenant_id=?').get('tenant-b') }, { in_flight: 0 });
    afterRecovery.close();
    store.close();

    const reopened = openStore(recoveryDir.databasePath, { clock: () => now });
    assert.equal(reopened._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'tenant-a', projectId: 'project-a', jobId: first.jobId
    }).state, 'running');
    assert.equal(reopened._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'tenant-b', projectId: 'project-b', jobId: second.jobId
    }).state, 'queued');
    const reopenedDb = new DatabaseSync(recoveryDir.databasePath, { readOnly: true });
    assert.deepEqual({ ...reopenedDb.prepare('SELECT pending_records, in_flight FROM operations_meta WHERE singleton=1').get() }, {
      pending_records: 2, in_flight: 1
    });
    assert.equal(reopened._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'review-owner-after-reopen', policy, recoverableJobIds: [first.jobId]
    }), null, 'An exact recovery ID cannot claim queued work while the reclaimed lease occupies the global slot.');
    reopenedDb.close();
    reopened.close();
  });

  await t.test('OPS05.C10 expired cancellation recovery atomically cancels workflow and releases reservations', () => {
    const recoveryDir = tempDirectory('ah11-cancel-expired-recovery-');
    t.after(() => recoveryDir.cleanup());
    const configInput = {
      workerCount: 1, maxInFlightGlobal: 1, maxInFlightTenant: 1, maxInFlightProject: 1
    };
    const config = readOperationsConfig(configInput);
    const policy = operationsStoragePolicy(config);
    let now = 70_000;
    const store = openStore(recoveryDir.databasePath, { clock: () => now });
    t.after(() => store.close());
    const accepted = admit(store, 'cancel-tenant', 'cancel-project', 'cancel-expired', PLAN, configInput);
    const claim = store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'review-cancel-owner', policy
    }).claim;
    const runnerPid = unusedProcessId();
    store._operationsSetRunnerPid(OPERATIONS_STORAGE_CAPABILITY, {
      jobId: claim.jobId, ownerId: claim.ownerId,
      generation: claim.generation, fence: claim.fence, runnerPid
    });
    assert.deepEqual(store._operationsCancel(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'cancel-tenant', projectId: 'cancel-project',
      jobId: accepted.jobId, expectedGeneration: claim.generation
    }), {
      accepted: true, state: 'cancellation_requested', generation: claim.generation,
      ownerId: claim.ownerId, fence: claim.fence
    });
    now = claim.leaseExpiresAtMs + 1;
    assert.equal(store._operationsExpiredClaims(OPERATIONS_STORAGE_CAPABILITY, { policy }).items[0].jobId, accepted.jobId);
    assert.equal(store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'review-cancel-recovery', policy, recoverableJobIds: [accepted.jobId]
    }), null);

    store.close();
    const reopened = openStore(recoveryDir.databasePath, { clock: () => now });
    t.after(() => reopened.close());
    const workflow = reopened.getWorkflow(accepted.workflowId);
    assert.equal(workflow.state, 'cancelled');
    assert.equal(workflow.cancellationRequested, true);
    assert.equal(workflow.leaseOwner, null);
    assert.equal(workflow.leaseExpiresAtMs, null);
    const raw = new DatabaseSync(recoveryDir.databasePath, { readOnly: true });
    assert.deepEqual({ ...raw.prepare('SELECT state, worker_id, runner_pid, lease_expires_ms, reserved_event_slots, reserved_disk_bytes FROM operations_work WHERE job_id=?').get(accepted.jobId) }, {
      state: 'cancelled', worker_id: null, runner_pid: null, lease_expires_ms: null,
      reserved_event_slots: 0, reserved_disk_bytes: 0
    });
    assert.deepEqual({ ...raw.prepare('SELECT pending_records, pending_bytes, in_flight, reserved_event_slots, reserved_disk_bytes FROM operations_meta WHERE singleton=1').get() }, {
      pending_records: 0, pending_bytes: 0, in_flight: 0, reserved_event_slots: 0, reserved_disk_bytes: 0
    });
    assert.equal(Number(raw.prepare("SELECT COUNT(*) AS count FROM workflow_events WHERE workflow_id=? AND event_type='operations_work_cancelled' AND from_state='running' AND to_state='cancelled'").get(accepted.workflowId).count), 1);
    assert.equal(Number(raw.prepare('SELECT COUNT(*) AS count FROM operation_attempts WHERE workflow_id=?').get(accepted.workflowId).count), 0);
    raw.close();
    reopened.close();
    const afterCloseReopen = openStore(recoveryDir.databasePath, { clock: () => now });
    assert.equal(afterCloseReopen.getWorkflow(accepted.workflowId).state, 'cancelled');
    assert.equal(afterCloseReopen.getWorkflow(accepted.workflowId).leaseOwner, null);
    assert.equal(afterCloseReopen._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'cancel-tenant', projectId: 'cancel-project', jobId: accepted.jobId
    }).state, 'cancelled');
    afterCloseReopen.close();
  });

  await t.test('OPS05.C11 exact expired-row recovery cannot bypass a tenant cap across projects', () => {
    const recoveryDir = tempDirectory('ah11-tenant-recovery-slot-');
    t.after(() => recoveryDir.cleanup());
    const configInput = {
      workerCount: 2, maxInFlightGlobal: 2, maxInFlightTenant: 1, maxInFlightProject: 1
    };
    const config = readOperationsConfig(configInput);
    const policy = operationsStoragePolicy(config);
    let now = 60_000;
    const store = openStore(recoveryDir.databasePath, { clock: () => now });
    t.after(() => store.close());
    const expired = admit(store, 'tenant-a', 'project-a-first', 'tenant-expired', PLAN, configInput);
    const prior = store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'tenant-slot-owner', policy
    }).claim;
    assert.equal(prior.jobId, expired.jobId);
    const queued = admit(store, 'tenant-a', 'project-z-queued', 'tenant-queued', PLAN, configInput);
    now = prior.leaseExpiresAtMs + 1;
    const recovered = store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'tenant-slot-recovery', policy, recoverableJobIds: [expired.jobId]
    });
    assert.equal(recovered.claim.jobId, expired.jobId);
    assert.equal(recovered.claim.generation, prior.generation + 1);
    const raw = new DatabaseSync(recoveryDir.databasePath, { readOnly: true });
    assert.equal(raw.prepare('SELECT state FROM operations_work WHERE job_id=?').get(queued.jobId).state, 'queued');
    assert.deepEqual({ ...raw.prepare('SELECT in_flight FROM operations_meta WHERE singleton=1').get() }, { in_flight: 1 });
    assert.deepEqual({ ...raw.prepare('SELECT in_flight FROM operations_tenants WHERE tenant_id=?').get('tenant-a') }, { in_flight: 1 });
    assert.deepEqual({ ...raw.prepare('SELECT in_flight FROM operations_projects WHERE tenant_id=? AND project_id=?').get('tenant-a', 'project-a-first') }, { in_flight: 1 });
    assert.deepEqual({ ...raw.prepare('SELECT in_flight FROM operations_projects WHERE tenant_id=? AND project_id=?').get('tenant-a', 'project-z-queued') }, { in_flight: 0 });
    raw.close();
    store.close();
    const reopened = openStore(recoveryDir.databasePath, { clock: () => now });
    assert.equal(reopened._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'tenant-slot-after-reopen', policy, recoverableJobIds: [expired.jobId]
    }), null);
    reopened.close();
  });

  await t.test('OPS05.C12 two actual SQLite connections race exact expired recoveries under all three caps', async t => {
    let c12Phase = 'open';
    let c12PrimaryFailed = false;
    let c12PrimaryError;
    let c12Retain = false;
    const c12Handles = new Set();
    const c12CleanupErrors = [];
    const openRecoveryStore = (...args) => { const handle = openStore(...args); c12Handles.add(handle); return handle; };
    const openRecoveryDb = (...args) => { const handle = new DatabaseSync(...args); c12Handles.add(handle); return handle; };
    const closeRecoveryHandle = handle => {
      try { handle.close(); c12Handles.delete(handle); }
      catch (error) { c12CleanupErrors.push(error); c12Retain = true; throw error; }
    };
    const finalizeRecoveryChildren = async (gate, processInfos, primaryFailed) => {
      const priorCleanupCount = c12CleanupErrors.length;
      try { if (!existsSync(gate)) writeFileSync(gate, 'released'); }
      catch (error) { c12CleanupErrors.push(error); c12Retain = true; }
      for (const info of processInfos) {
        try {
          if (info.child.exitCode === null && info.child.signalCode === null) info.child.kill('SIGKILL');
          const exit = await waitForCloseBounded(info.closed, 1_000);
          const observed = closedChildObservation(exit);
          if (!observed.processClosed || observed.errorEvent || observed.pipeEOF !== 'complete') c12Retain = true;
        } catch (error) { c12CleanupErrors.push(error); c12Retain = true; }
      }
      if (!primaryFailed && c12CleanupErrors.length > priorCleanupCount) {
        throw new AggregateError(c12CleanupErrors.slice(priorCleanupCount), 'C12 child cleanup failed.');
      }
    };
    let terminalRaceDir = null;
    let recoveryDir = null;
    const c12Children = new Map();
    t.after(async () => {
      for (const [child, closed] of c12Children) {
        if (child.exitCode === null && child.signalCode === null) { try { child.kill('SIGKILL'); } catch {} }
        const exit = await waitForCloseBounded(closed, 1_000);
        const observed = closedChildObservation(exit);
        if (!observed.processClosed || observed.errorEvent || observed.pipeEOF !== 'complete') c12Retain = true;
      }
      const cleanupErrors = c12CleanupErrors;
      for (const handle of c12Handles) {
        try { closeRecoveryHandle(handle); } catch { c12Retain = true; }
      }
      const cleanup = cleanupErrors.length ? closedErrorProjection(cleanupErrors[0])
        : { errorKind: 'none', code: 'none', sqlitePrimaryCode: null };
      recordClosedFixture({ caseId: 'OPS05.C12', phase: 'cleanup', primaryFailed: c12PrimaryFailed,
        ...(c12PrimaryFailed ? closedErrorProjection(c12PrimaryError) : {}),
        cleanupFailed: cleanupErrors.length > 0, cleanupKind: cleanup.errorKind, cleanupCode: cleanup.code,
        cleanupPrimaryCode: cleanup.sqlitePrimaryCode, nativeClose: c12Handles.size === 0 ? 'confirmed' : 'unknown', processClosed: c12Children.size === 0 });
      if (!c12Retain) { recoveryDir?.cleanup(); terminalRaceDir?.cleanup(); }
      if (cleanupErrors.length) throw new AggregateError(
        c12PrimaryFailed ? [c12PrimaryError, ...cleanupErrors] : cleanupErrors,
        'C12 native cleanup was not confirmed.', c12PrimaryFailed ? { cause: c12PrimaryError } : undefined);
    });
    try {
    recoveryDir = tempDirectory('ah11-concurrent-recovery-');
    const configInput = {
      workerCount: 2, maxInFlightGlobal: 2, maxInFlightTenant: 1, maxInFlightProject: 1
    };
    const config = readOperationsConfig(configInput);
    const policy = operationsStoragePolicy(config);
    let now = 80_000;
    const store = openRecoveryStore(recoveryDir.databasePath, { clock: () => now });
    const first = admit(store, 'tenant-a', 'project-a', 'race-expired-a', PLAN, configInput);
    const second = admit(store, 'tenant-b', 'project-b', 'race-expired-b', PLAN, configInput);
    const third = admit(store, 'tenant-c', 'project-c', 'race-queued-c', PLAN, configInput);
    const firstClaim = store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, { ownerId: 'race-original-a', policy }).claim;
    const secondClaim = store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, { ownerId: 'race-original-b', policy }).claim;
    assert.equal(firstClaim.jobId, first.jobId);
    assert.equal(secondClaim.jobId, second.jobId);
    now = Math.max(firstClaim.leaseExpiresAtMs, secondClaim.leaseExpiresAtMs) + 1;
    closeRecoveryHandle(store);

    const gatePath = join(recoveryDir.directory, 'release-recovery-race');
    const processes = [
      { ownerId: 'race-recovery-a', jobId: first.jobId },
      { ownerId: 'race-recovery-b', jobId: second.jobId }
    ].map(({ ownerId, jobId }) => {
      const readyPath = join(recoveryDir.directory, `${ownerId}.ready`);
      const child = spawn(process.execPath, [
        join(ROOT, 'tests/operations/fixtures/recovery-claim-child.mjs'),
        recoveryDir.databasePath, String(now), ownerId, jobId, readyPath, gatePath, JSON.stringify(configInput)
      ], { cwd: ROOT, env: baseEnvironment(), stdio: ['ignore', 'pipe', 'pipe'] });
      const observation = observeClosedChild(child);
      const closed = observation.closed.then(exit => {
        c12Children.delete(child); return exit;
      });
      c12Children.set(child, closed);
      let stdout = '';
      let stderr = '';
      child.stdout.setEncoding('utf8');
      child.stderr.setEncoding('utf8');
      child.stdout.on('data', chunk => { stdout += chunk; });
      child.stderr.on('data', chunk => { stderr += chunk; });
      return { ownerId, jobId, readyPath, child, closed, get stdout() { return stdout; }, get stderr() { return stderr; } };
    });
    METRICS.processRuns += processes.length;
    let expiredRaceFailed = false;
    try {
      for (const processInfo of processes) await waitForPath(processInfo.readyPath, processInfo.child, 5_000);
      writeFileSync(gatePath, 'released');
      const exits = await Promise.all(processes.map(async processInfo =>
        waitForCloseBounded(processInfo.closed, 5_000)));
      c12Phase = 'expired_child';
      const validated = processes.map(() => false);
      for (const [index, processInfo] of processes.entries()) {
        const detail = parseClosedChildDiagnostic(processInfo.stderr, 'OPS05.C12');
        if (requiresClosedFixtureRetention(exits[index], detail?.nativeClose, detail?.cleanupFailed)) c12Retain = true;
      }
      try {
        assert.ok(exits.every(Boolean), 'Both independent claim processes must close within their bound.');
        for (const [index, processInfo] of processes.entries()) {
          assert.equal(exits[index].code, 0, processInfo.stderr);
          assert.deepEqual(JSON.parse(processInfo.stdout.trim()), {
            ownerId: processInfo.ownerId,
            claim: { jobId: processInfo.jobId, generation: 2, fence: 2 },
            incomplete: false,
            ownedRunnerCount: 0
          });
          validated[index] = true;
        }
      } finally {
        for (const [index, processInfo] of processes.entries()) {
          t.diagnostic(`OPS05_C12_RETRY_V1 ${JSON.stringify({ race: 'expired', slot: index,
            diagnostic: parseFixtureRetryDiagnostic(processInfo.stderr) })}`);
          recordClosedChild(processInfo.stderr, 'OPS05.C12', index, c12Phase, exits[index], validated[index]);
        }
      }
    } catch (error) { expiredRaceFailed = true; c12Retain = true; throw error; }
    finally { await finalizeRecoveryChildren(gatePath, processes, expiredRaceFailed); }

    const check = openRecoveryStore(recoveryDir.databasePath, { clock: () => now });
    const after = openRecoveryDb(recoveryDir.databasePath, { readOnly: true });
    assert.deepEqual({ ...after.prepare('SELECT pending_records, in_flight FROM operations_meta WHERE singleton=1').get() }, {
      pending_records: 3, in_flight: 2
    });
    assert.deepEqual({ ...after.prepare('SELECT in_flight FROM operations_tenants WHERE tenant_id=?').get('tenant-a') }, { in_flight: 1 });
    assert.deepEqual({ ...after.prepare('SELECT in_flight FROM operations_tenants WHERE tenant_id=?').get('tenant-b') }, { in_flight: 1 });
    assert.deepEqual({ ...after.prepare('SELECT in_flight FROM operations_tenants WHERE tenant_id=?').get('tenant-c') }, { in_flight: 0 });
    for (const job of [first, second]) {
      const row = after.prepare('SELECT state, generation, fence_token FROM operations_work WHERE job_id=?').get(job.jobId);
      assert.deepEqual({ ...row }, { state: 'running', generation: 2, fence_token: 2 });
      c12Phase = 'recovered_count';
      const recoveredCount = Number(after.prepare("SELECT COUNT(*) AS count FROM workflow_events WHERE workflow_id=? AND event_type='operations_work_claimed'").get(job.workflowId).count);
      recordClosedFixture({ caseId: 'OPS05.C12', phase: c12Phase, slot: job === first ? 0 : 1, actual: recoveredCount, expected: 2 });
      assert.equal(recoveredCount, 2);
    }
    assert.equal(after.prepare('SELECT state FROM operations_work WHERE job_id=?').get(third.jobId).state, 'queued');
    closeRecoveryHandle(after);
    assert.equal(check._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'race-repeat-no-extra-slot', policy, recoverableJobIds: [first.jobId, second.jobId]
    }), null);
    closeRecoveryHandle(check);
    const reopened = openRecoveryStore(recoveryDir.databasePath, { clock: () => now });
    assert.equal(reopened._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'tenant-c', projectId: 'project-c', jobId: third.jobId
    }).state, 'queued');
    closeRecoveryHandle(reopened);

    // Two independent supervisors must serialize the terminal-public-API
    // cleanup and let exactly one claim the next tenant at a cap of one.
    terminalRaceDir = tempDirectory('ah11-terminal-recovery-race-');
    const terminalConfigInput = {
      workerCount: 1, maxInFlightGlobal: 1, maxInFlightTenant: 1, maxInFlightProject: 1
    };
    const terminalConfig = readOperationsConfig(terminalConfigInput);
    const terminalPolicy = operationsStoragePolicy(terminalConfig);
    let terminalNow = 90_000;
    const terminalStore = openRecoveryStore(terminalRaceDir.databasePath, { clock: () => terminalNow });
    const terminal = admit(terminalStore, 'public-expired-tenant', 'public-expired-project',
      'terminal-race-expired', PLAN, terminalConfigInput);
    const terminalPrior = terminalStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'terminal-race-prior-owner', policy: terminalPolicy
    }).claim;
    const nextTenantJob = admit(terminalStore, 'public-next-tenant', 'public-next-project',
      'terminal-race-next', PLAN, terminalConfigInput);
    closeRecoveryHandle(terminalStore);
    const terminalCrash = crashAfterPublicWorkflowMutation(terminalRaceDir.databasePath, {
      mutation: 'succeeded', now: terminalNow, workflowId: terminalPrior.workflowId,
      ownerId: terminalPrior.ownerId, fence: terminalPrior.fence, revision: terminalPrior.revision,
      attemptId: 'terminal-race-transition', holdRef: null
    });
    assert.equal(terminalCrash.mutation, 'succeeded');
    terminalNow = terminalPrior.leaseExpiresAtMs + 1;
    const terminalGate = join(terminalRaceDir.directory, 'release-terminal-recovery-race');
    const terminalSupervisors = ['terminal-race-supervisor-a', 'terminal-race-supervisor-b'].map(ownerId => {
      const readyPath = join(terminalRaceDir.directory, `${ownerId}.ready`);
      const child = spawn(process.execPath, [
        join(ROOT, 'tests/operations/fixtures/recovery-claim-child.mjs'),
        terminalRaceDir.databasePath, String(terminalNow), ownerId, terminal.jobId,
        readyPath, terminalGate, JSON.stringify(terminalConfigInput)
      ], { cwd: ROOT, env: baseEnvironment(), stdio: ['ignore', 'pipe', 'pipe'] });
      const observation = observeClosedChild(child);
      const closed = observation.closed.then(exit => {
        c12Children.delete(child); return exit;
      });
      c12Children.set(child, closed);
      let stdout = '';
      let stderr = '';
      child.stdout.setEncoding('utf8');
      child.stderr.setEncoding('utf8');
      child.stdout.on('data', chunk => { stdout += chunk; });
      child.stderr.on('data', chunk => { stderr += chunk; });
      return { ownerId, readyPath, child, closed, get stdout() { return stdout; }, get stderr() { return stderr; } };
    });
    METRICS.processRuns += terminalSupervisors.length;
    let terminalRaceFailed = false;
    try {
      for (const supervisor of terminalSupervisors) await waitForPath(supervisor.readyPath, supervisor.child, 5_000);
      writeFileSync(terminalGate, 'released');
      const outcomes = await Promise.all(terminalSupervisors.map(async supervisor => ({
        supervisor, exit: await waitForCloseBounded(supervisor.closed, 5_000)
      })));
      c12Phase = 'terminal_child';
      const validated = outcomes.map(() => false);
      outcomes.forEach(({ supervisor, exit }) => {
        const detail = parseClosedChildDiagnostic(supervisor.stderr, 'OPS05.C12');
        if (requiresClosedFixtureRetention(exit, detail?.nativeClose, detail?.cleanupFailed)) c12Retain = true;
      });
      let results;
      try {
        assert.ok(outcomes.every(item => item.exit), 'Both terminal recovery supervisors must finish within the bound.');
        results = outcomes.map(({ supervisor, exit }, slot) => {
          assert.equal(exit.code, 0, supervisor.stderr);
          const result = JSON.parse(supervisor.stdout.trim());
          assert.equal(result.ownerId, supervisor.ownerId);
          assert.equal(result.incomplete, false);
          assert.equal(result.ownedRunnerCount, 0);
          return result;
        });
        c12Phase = 'accepted_count';
        const acceptedCount = results.filter(result => result.claim?.jobId === nextTenantJob.jobId).length;
        recordClosedFixture({ caseId: 'OPS05.C12', phase: c12Phase, actual: acceptedCount, expected: 1 });
        assert.equal(acceptedCount, 1,
          'exactly one supervisor can claim the queued next tenant');
        c12Phase = 'null_count';
        const nullCount = results.filter(result => result.claim === null).length;
        recordClosedFixture({ caseId: 'OPS05.C12', phase: c12Phase, actual: nullCount, expected: 1 });
        assert.equal(nullCount, 1,
          'the losing supervisor observes the committed terminal reconciliation and occupied cap');
        validated.fill(true);
      } finally {
        outcomes.forEach(({ supervisor, exit }, slot) => {
          t.diagnostic(`OPS05_C12_RETRY_V1 ${JSON.stringify({ race: 'terminal', slot,
            diagnostic: parseFixtureRetryDiagnostic(supervisor.stderr) })}`);
          recordClosedChild(supervisor.stderr, 'OPS05.C12', slot, 'terminal_child', exit, validated[slot]);
        });
      }
    } catch (error) { terminalRaceFailed = true; c12Retain = true; throw error; }
    finally { await finalizeRecoveryChildren(terminalGate, terminalSupervisors, terminalRaceFailed); }
    const terminalRaw = openRecoveryDb(terminalRaceDir.databasePath, { readOnly: true });
    let terminalReadFailed = false;
    try {
      assert.equal(terminalRaw.prepare('SELECT state FROM operations_work WHERE job_id=?').get(terminal.jobId).state,
        'completed');
      assert.deepEqual({ ...terminalRaw.prepare(`SELECT pending_records, in_flight,
        reserved_event_slots, reserved_disk_bytes FROM operations_meta WHERE singleton=1`).get() }, {
        pending_records: 1, in_flight: 1,
        reserved_event_slots: Number(terminalRaw.prepare('SELECT reserved_event_slots FROM operations_work WHERE job_id=?')
          .get(nextTenantJob.jobId).reserved_event_slots),
        reserved_disk_bytes: Number(terminalRaw.prepare('SELECT reserved_disk_bytes FROM operations_work WHERE job_id=?')
          .get(nextTenantJob.jobId).reserved_disk_bytes)
      });
      c12Phase = 'terminal_count';
      const terminalCount = Number(terminalRaw.prepare(`SELECT COUNT(*) AS count FROM workflow_events
        WHERE workflow_id=? AND event_type='operations_work_completed'`).get(terminal.workflowId).count);
      recordClosedFixture({ caseId: 'OPS05.C12', phase: c12Phase, actual: terminalCount, expected: 1 });
      assert.equal(terminalCount, 1);
      assert.equal(terminalRaw.prepare('SELECT state FROM operations_work WHERE job_id=?').get(nextTenantJob.jobId).state,
        'running');
    } catch (error) { terminalReadFailed = true; throw error; }
    finally {
      try { closeRecoveryHandle(terminalRaw); }
      catch (error) { if (!terminalReadFailed) throw error; }
    }
    const terminalReopen = openRecoveryStore(terminalRaceDir.databasePath, { clock: () => terminalNow });
    assert.equal(terminalReopen.getWorkflow(terminal.workflowId).leaseOwner, null);
    assert.equal(terminalReopen._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'public-expired-tenant', projectId: 'public-expired-project', jobId: terminal.jobId
    }).state, 'completed');
    closeRecoveryHandle(terminalReopen);
    } catch (error) {
      c12PrimaryFailed = true; c12PrimaryError = error; c12Retain = true;
      recordClosedFixture({ caseId: 'OPS05.C12', slot: 2, phase: c12Phase,
        primaryFailed: true, ...closedErrorProjection(error) });
      throw error;
    }
  });

  await t.test('OPS05.C13 expired cancellation reconciliation releases capacity before later queued work', () => {
    const recoveryDir = tempDirectory('ah11-cancel-recovery-next-claim-');
    t.after(() => recoveryDir.cleanup());
    const configInput = { workerCount: 1, maxInFlightGlobal: 1, maxInFlightTenant: 1, maxInFlightProject: 1 };
    const config = readOperationsConfig(configInput);
    const policy = operationsStoragePolicy(config);
    let now = 90_000;
    const store = openStore(recoveryDir.databasePath, { clock: () => now });
    t.after(() => store.close());
    const expired = admit(store, 'a-tenant', 'a-project', 'cancel-before-queued', PLAN, configInput);
    const prior = store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, { ownerId: 'cancel-prior-owner', policy }).claim;
    const queued = admit(store, 'z-tenant', 'z-project', 'queued-after-cancel', PLAN, configInput);
    assert.equal(store._operationsCancel(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'a-tenant', projectId: 'a-project', jobId: expired.jobId,
      expectedGeneration: prior.generation
    }).accepted, true);
    now = prior.leaseExpiresAtMs + 1;
    const next = store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'queued-after-cancel-owner', policy, recoverableJobIds: [expired.jobId]
    });
    assert.equal(next.claim.jobId, queued.jobId);
    const raw = new DatabaseSync(recoveryDir.databasePath, { readOnly: true });
    assert.deepEqual({ ...raw.prepare('SELECT state, cancellation_requested, lease_owner, lease_expires_ms FROM workflows WHERE workflow_id=?').get(expired.workflowId) }, {
      state: 'cancelled', cancellation_requested: 1, lease_owner: null, lease_expires_ms: null
    });
    assert.equal(raw.prepare('SELECT state FROM operations_work WHERE job_id=?').get(expired.jobId).state, 'cancelled');
    assert.equal(raw.prepare('SELECT state FROM operations_work WHERE job_id=?').get(queued.jobId).state, 'running');
    assert.deepEqual({ ...raw.prepare('SELECT pending_records, in_flight FROM operations_meta WHERE singleton=1').get() }, {
      pending_records: 1, in_flight: 1
    });
    assert.deepEqual({ ...raw.prepare('SELECT in_flight FROM operations_tenants WHERE tenant_id=?').get('a-tenant') }, { in_flight: 0 });
    assert.deepEqual({ ...raw.prepare('SELECT in_flight FROM operations_tenants WHERE tenant_id=?').get('z-tenant') }, { in_flight: 1 });
    assert.equal(Number(raw.prepare("SELECT COUNT(*) AS count FROM workflow_events WHERE workflow_id=? AND event_type='operations_work_cancelled'").get(expired.workflowId).count), 1);
    raw.close();
  });

  await t.test('OPS05.C14 unknown expired work keeps its hold and releases only its local slot before queued work', () => {
    const recoveryDir = tempDirectory('ah11-unknown-recovery-next-claim-');
    t.after(() => recoveryDir.cleanup());
    const configInput = { workerCount: 1, maxInFlightGlobal: 1, maxInFlightTenant: 1, maxInFlightProject: 1 };
    const config = readOperationsConfig(configInput);
    const policy = operationsStoragePolicy(config);
    let now = 100_000;
    const store = openStore(recoveryDir.databasePath, { clock: () => now });
    t.after(() => store.close());
    const expired = admit(store, 'a-tenant', 'a-project', 'unknown-before-queued', PLAN, configInput);
    const prior = store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, { ownerId: 'unknown-prior-owner', policy }).claim;
    store.recordDispatchIntent({
      workflowId: prior.workflowId, ownerId: prior.ownerId, fence: prior.fence,
      expectedRevision: prior.revision, attemptId: 'unknown-recovery-attempt',
      logicalOperationRef: 'unknown-recovery-logical',
      authorityHoldRef: 'synthetic-local-bookkeeping:unknown-recovery-hold',
      transportAttemptId: 'unknown-recovery-transport', eventId: 'unknown-recovery-intent'
    });
    const queued = admit(store, 'z-tenant', 'z-project', 'queued-after-unknown', PLAN, configInput);
    assert.equal(store._operationsCancel(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'a-tenant', projectId: 'a-project', jobId: expired.jobId,
      expectedGeneration: prior.generation
    }).accepted, true);
    now = prior.leaseExpiresAtMs + 1;
    const next = store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'queued-after-unknown-owner', policy, recoverableJobIds: [expired.jobId]
    });
    assert.equal(next.claim.jobId, queued.jobId);
    const unknown = store._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'a-tenant', projectId: 'a-project', jobId: expired.jobId
    });
    assert.equal(unknown.state, 'unknown');
    assert.equal(store.getWorkflow(expired.workflowId).state, 'reconciliation_pending');
    assert.equal(store.getWorkflow(expired.workflowId).cancellationRequested, true);
    const attempt = store.getAttempt('unknown-recovery-attempt');
    assert.equal(attempt.state, 'intent');
    assert.equal(attempt.authorityHoldRef, 'synthetic-local-bookkeeping:unknown-recovery-hold');
    const raw = new DatabaseSync(recoveryDir.databasePath, { readOnly: true });
    const reservations = raw.prepare(`SELECT
      (SELECT SUM(reserved_disk_bytes) FROM operations_work) AS work_disk,
      (SELECT SUM(reserved_event_slots) FROM operations_work) AS work_events,
      reserved_disk_bytes, reserved_event_slots FROM operations_meta WHERE singleton=1`).get();
    assert.equal(Number(reservations.reserved_disk_bytes), Number(reservations.work_disk));
    assert.equal(Number(reservations.reserved_event_slots), Number(reservations.work_events));
    assert.equal(Number(raw.prepare('SELECT reserved_disk_bytes FROM operations_work WHERE job_id=?').get(expired.jobId).reserved_disk_bytes) > 0, true);
    assert.equal(Number(raw.prepare('SELECT reserved_event_slots FROM operations_work WHERE job_id=?').get(expired.jobId).reserved_event_slots) > 0, true);
    assert.equal(Number(raw.prepare("SELECT COUNT(*) AS count FROM workflow_events WHERE workflow_id=? AND event_type='operations_work_unknown'").get(expired.workflowId).count), 1);
    raw.close();
    store._operationsFinish(OPERATIONS_STORAGE_CAPABILITY, {
      jobId: next.claim.jobId, ownerId: next.claim.ownerId,
      generation: next.claim.generation, fence: next.claim.fence, outcome: 'stopped', childExited: true
    });
    assert.equal(store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'no-unknown-redispatch', policy, recoverableJobIds: [expired.jobId]
    }), null);
    assert.equal(store.getAttempt('unknown-recovery-attempt').authorityHoldRef,
      'synthetic-local-bookkeeping:unknown-recovery-hold');
    assert.equal(store._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'a-tenant', projectId: 'a-project', jobId: expired.jobId
    }).state, 'unknown');
  });

  await t.test('OPS05.C15 expired current-fence outcome-unknown work reconciles after reopen without releasing its hold', async t => {
    await assertExpiredCurrentFenceReconciles(t, {
      caseName: 'public-unknown',
      mutation: 'unknown',
      expectedWorkflowState: 'reconciliation_pending',
      expectedWorkState: 'unknown',
      expectedEventType: 'operations_work_unknown',
      expectedAttemptState: 'outcome_unknown',
      async afterRecovery({ databasePath, caseName, expired, queued, now, attemptId, holdRef }) {
        let lateStore = openStore(databasePath, { clock: () => now });
        try {
          const lateLease = lateStore.acquireLease({
            workflowId: expired.workflowId, ownerId: `${caseName}-late-owner`, ttlMs: 5_000,
            eventId: `${caseName}-late-lease`
          });
          const receipt = lateStore.recordResponseReceipt({
            workflowId: expired.workflowId, ownerId: lateLease.ownerId, fence: lateLease.fence,
            expectedRevision: lateLease.revision, attemptId,
            receiptId: `${caseName}-late-receipt`, outcome: 'completed', nextState: 'succeeded',
            responseBytes: Buffer.from('synthetic late result receipt'),
            nextCheckpoint: lateStore.getCheckpoint(expired.workflowId).data,
            eventId: `${caseName}-late-response`
          });
          assert.ok(receipt.receiptId.endsWith('-late-receipt'));
          const lateSnapshot = recoverySnapshot(databasePath, expired.jobId, queued.jobId,
            expired.workflowId, attemptId);
          assert.equal(lateSnapshot.work.state, 'unknown');
          assert.equal(lateSnapshot.work.reserved_disk_bytes > 0, true);
          assert.equal(lateSnapshot.work.reserved_event_slots > 0, true);
          assert.equal(lateSnapshot.meta.pending_records, 2);
          assert.equal(lateSnapshot.meta.in_flight, 1);
          assert.equal(lateSnapshot.attempt.state, 'response_received');
          assert.equal(lateSnapshot.attempt.authority_hold_ref, holdRef);
          assert.equal(lateSnapshot.workflow.state, 'reconciliation_pending');
          lateStore.close();
          lateStore = null;
          const lateReopen = openStore(databasePath, { clock: () => now });
          try {
            assert.equal(lateReopen._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
              tenantId: 'public-expired-tenant', projectId: 'public-expired-project', jobId: expired.jobId
            }).state, 'unknown');
            assert.equal(lateReopen.getAttempt(attemptId).state, 'response_received');
            assert.equal(lateReopen.getAttempt(attemptId).authorityHoldRef, holdRef);
            assert.equal(recoverySnapshot(databasePath, expired.jobId, queued.jobId,
              expired.workflowId, attemptId).work.reserved_disk_bytes,
            lateSnapshot.work.reserved_disk_bytes);
          } finally {
            lateReopen.close();
          }
        } finally {
          try { lateStore?.close(); } catch {}
        }
      }
    });

    // A committed dispatch intent is the complementary unresolved control: it
    // enters recovery from awaiting_response and remains an intent until an
    // authoritative local receipt exists.
    await assertExpiredCurrentFenceReconciles(t, {
      caseName: 'public-intent-control',
      mutation: 'intent',
      expectedWorkflowState: 'awaiting_response',
      expectedWorkflowStateAfterRecovery: 'reconciliation_pending',
      expectedWorkState: 'unknown',
      expectedEventType: 'operations_work_unknown',
      expectedAttemptState: 'intent'
    });
    await assertUnknownRecoveryFailsClosedAtResourceBoundary();
  });

  await t.test('OPS05.C16 expired succeeded current-fence work reconciles to completed after reopen', async t => {
    await assertExpiredCurrentFenceReconciles(t, {
      caseName: 'public-succeeded',
      mutation: 'succeeded',
      expectedWorkflowState: 'succeeded',
      expectedWorkState: 'completed',
      expectedEventType: 'operations_work_completed'
    });
  });

  await t.test('OPS05.C17 expired failed current-fence work reconciles to failed after reopen', async t => {
    await assertExpiredCurrentFenceReconciles(t, {
      caseName: 'public-failed',
      mutation: 'failed',
      expectedWorkflowState: 'failed',
      expectedWorkState: 'failed',
      expectedEventType: 'operations_work_failed'
    });
  });

  await t.test('OPS05.C18 expired cancellation with cleared workflow lease reconciles after reopen', async t => {
    await assertExpiredCurrentFenceReconciles(t, {
      caseName: 'public-cancelled',
      mutation: 'cancelled',
      expectedWorkflowState: 'cancelled',
      expectedWorkState: 'cancelled',
      expectedEventType: 'operations_work_cancelled'
    });
  });

  await t.test('OPS05.C06 cancellation of already-reconciliation-pending unknown work persists its flag without settling its hold', () => {
    const unknownDir = tempDirectory('ah11-reconciliation-cancel-');
    t.after(() => unknownDir.cleanup());
    let unknownNow = 40_000;
    const unknownStore = openStore(unknownDir.databasePath, { clock: () => unknownNow });
    const accepted = admit(unknownStore, 'unknown-cancel-tenant', 'unknown-cancel-project', 'unknown-cancel');
    const policy = operationsStoragePolicy(readOperationsConfig());
    const claim = unknownStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'review-worker-unknown-owner', policy
    }).claim;
    unknownStore._operationsSetRunnerPid(OPERATIONS_STORAGE_CAPABILITY, {
      jobId: claim.jobId, ownerId: claim.ownerId, generation: claim.generation,
      fence: claim.fence, runnerPid: unusedProcessId()
    });
    unknownStore.recordDispatchIntent({
      workflowId: claim.workflowId, ownerId: claim.ownerId, fence: claim.fence,
      expectedRevision: claim.revision, attemptId: 'unknown-cancel-attempt',
      logicalOperationRef: 'unknown-cancel-logical', authorityHoldRef: 'synthetic-local-bookkeeping:unknown-cancel-hold',
      transportAttemptId: 'unknown-cancel-transport', eventId: 'unknown-cancel-intent'
    });
    unknownNow = claim.leaseExpiresAtMs + 1;
    unknownStore._operationsExpiredClaims(OPERATIONS_STORAGE_CAPABILITY, { policy });
    assert.equal(unknownStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'review-worker-unknown-recovery', policy, recoverableJobIds: [accepted.jobId]
    }), null);
    assert.equal(unknownStore.getWorkflow(accepted.workflowId).state, 'reconciliation_pending');
    const cancellation = unknownStore._operationsCancel(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'unknown-cancel-tenant', projectId: 'unknown-cancel-project',
      jobId: accepted.jobId, expectedGeneration: claim.generation
    });
    assert.deepEqual(cancellation, { accepted: true, state: 'unknown', generation: claim.generation });
    const once = new DatabaseSync(unknownDir.databasePath, { readOnly: true });
    const workflow = unknownStore.getWorkflow(accepted.workflowId);
    assert.equal(workflow.state, 'reconciliation_pending');
    assert.equal(workflow.cancellationRequested, true);
    assert.equal(once.prepare('SELECT state FROM operations_work WHERE job_id=?').get(accepted.jobId).state, 'unknown');
    assert.equal(unknownStore.getAttempt('unknown-cancel-attempt').state, 'intent');
    assert.equal(unknownStore.getAttempt('unknown-cancel-attempt').authorityHoldRef,
      'synthetic-local-bookkeeping:unknown-cancel-hold');
    const eventCount = Number(once.prepare('SELECT COUNT(*) AS count FROM workflow_events WHERE workflow_id=?').get(accepted.workflowId).count);
    const reservation = Number(once.prepare('SELECT reserved_disk_bytes FROM operations_meta WHERE singleton=1').get().reserved_disk_bytes);
    once.close();
    const repeated = unknownStore._operationsCancel(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'unknown-cancel-tenant', projectId: 'unknown-cancel-project',
      jobId: accepted.jobId, expectedGeneration: claim.generation
    });
    assert.deepEqual(repeated, { accepted: true, state: 'unknown', generation: claim.generation });
    unknownStore.close();
    const reopened = openStore(unknownDir.databasePath, { clock: () => unknownNow });
    assert.equal(reopened.getWorkflow(accepted.workflowId).cancellationRequested, true);
    assert.equal(reopened.getAttempt('unknown-cancel-attempt').authorityHoldRef,
      'synthetic-local-bookkeeping:unknown-cancel-hold');
    const after = new DatabaseSync(unknownDir.databasePath, { readOnly: true });
    assert.equal(Number(after.prepare('SELECT COUNT(*) AS count FROM workflow_events WHERE workflow_id=?').get(accepted.workflowId).count), eventCount);
    assert.equal(Number(after.prepare('SELECT reserved_disk_bytes FROM operations_meta WHERE singleton=1').get().reserved_disk_bytes), reservation);
    assert.equal(Number(after.prepare('SELECT in_flight FROM operations_meta WHERE singleton=1').get().in_flight), 0);
    after.close();
    reopened.close();
  });

  await t.test('OPS05.C07 parked cancellation rejects stale generations and commits one terminal event', () => {
    const parkedDir = tempDirectory('ah11-parked-cancel-');
    t.after(() => parkedDir.cleanup());
    const parkedStore = openStore(parkedDir.databasePath);
    t.after(() => parkedStore.close());
    const accepted = admit(parkedStore, 'parked-tenant', 'parked-project', 'parked-cancel');
    const ownerId = 'review-worker-parked';
    const claim = parkedStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId, policy: operationsStoragePolicy(readOperationsConfig())
    }).claim;
    const parked = parkedStore._operationsFinish(OPERATIONS_STORAGE_CAPABILITY, {
      jobId: accepted.jobId, ownerId, generation: claim.generation,
      fence: claim.fence, outcome: 'stopped', childExited: true
    });
    assert.equal(parked.state, 'parked');
    assert.equal(parkedStore.getWorkflow(accepted.workflowId).state, 'review_required');
    assert.throws(() => parkedStore._operationsCancel(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'parked-tenant', projectId: 'parked-project', jobId: accepted.jobId,
      expectedGeneration: claim.generation - 1
    }), error => error.code === 'CONCURRENT_MODIFICATION');
    const cancelled = parkedStore._operationsCancel(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'parked-tenant', projectId: 'parked-project', jobId: accepted.jobId,
      expectedGeneration: claim.generation
    });
    assert.deepEqual(cancelled, { accepted: true, state: 'cancelled', generation: claim.generation });
    assert.equal(parkedStore.getWorkflow(accepted.workflowId).state, 'cancelled');
    const raw = new DatabaseSync(parkedDir.databasePath, { readOnly: true });
    assert.equal(Number(raw.prepare("SELECT COUNT(*) AS count FROM workflow_events WHERE workflow_id=? AND event_type='operations_work_cancelled'").get(accepted.workflowId).count), 1);
    raw.close();
  });

  await t.test('OPS05.C08 actual supervisor crash preserves a committed step and two restarted workers claim it once', async t => {
    const crashDir = tempDirectory('ah11-worker-crash-race-');
    let c08Phase = 'ready';
    let c08Retain = false;
    let c08PrimaryFailed = false;
    let c08PrimaryError;
    const c08Handles = new Set();
    const openCrashDb = (...args) => { const handle = new DatabaseSync(...args); c08Handles.add(handle); return handle; };
    const openCrashStore = (...args) => { const handle = openStore(...args); c08Handles.add(handle); return handle; };
    const closeCrashHandle = handle => { handle.close(); c08Handles.delete(handle); };

    const fixture = join(ROOT, 'tests/operations/fixtures/worker-supervisor-child.mjs');
    const children = new Set();
    const runnerPids = new Set();
    const fixtureObservations = [];
    const openFixture = (ownerId, idempotencyKey = 'worker-crash-recovery', slot = 0) => {
      const deadlineNs = process.hrtime.bigint() + 8_000_000_000n;
      const child = spawn(process.execPath, [fixture, crashDir.databasePath, ownerId, idempotencyKey, String(deadlineNs)], {
        cwd: ROOT, env: baseEnvironment(), stdio: ['ignore', 'pipe', 'pipe']
      });
      METRICS.processRuns += 1;
      children.add(child);
      const observation = observeClosedChild(child);
      let resultValidated = false;
      let observedExit = null;
      let output = '';
      let diagnosticStderr = '';
      const records = [];
      child.stdout.setEncoding('utf8');
      child.stderr.setEncoding('utf8');
      child.stdout.on('data', chunk => {
        output += chunk;
        const lines = output.split(/\r?\n/u);
        output = lines.pop() ?? '';
        for (const line of lines) {
          try { records.push(JSON.parse(line)); } catch {}
        }
      });
      child.stderr.on('data', chunk => {
        if (Buffer.byteLength(diagnosticStderr + chunk, 'utf8') <= 8_192) diagnosticStderr += chunk;
        else METRICS.closedFixtureDiagnostics.diagnosticsComplete = false;
      });
      const closed = observation.closed.then(exit => {
        children.delete(child); observedExit = exit;
        const detail = parseClosedChildDiagnostic(diagnosticStderr, 'OPS05.C08');
        if (requiresClosedFixtureRetention(exit, detail?.nativeClose, detail?.cleanupFailed)) c08Retain = true;
        return exit;
      });
      fixtureObservations.push(() => {
        const exit = observedExit ?? observation.snapshot();
        t.diagnostic(`OPS05_C08_RETRY_V1 ${JSON.stringify({ slot,
          diagnostic: parseFixtureRetryDiagnostic(diagnosticStderr) })}`);
        const detail = recordClosedChild(diagnosticStderr, 'OPS05.C08', slot,
          'worker_stop', exit, resultValidated);
        if (requiresClosedFixtureRetention(exit, detail?.nativeClose, detail?.cleanupFailed)) c08Retain = true;
      });
      const ready = (async () => {
        while (process.hrtime.bigint() < deadlineNs) {
          const record = records.find(item => item.event === 'ready');
          if (record) return record;
          if (child.exitCode !== null || child.signalCode !== null) break;
          await new Promise(resolvePromise => setTimeout(resolvePromise, 2));
        }
        throw new Error(`The owned supervisor did not publish its admission: ${diagnosticStderr}`);
      })();
      return { child, closed, ready, records, markResultValidated: () => { resultValidated = true; } };
    };
    const waitUntil = async (predicate, timeoutMs, message) => {
      const deadline = Date.now() + timeoutMs;
      while (Date.now() < deadline) {
        const result = await predicate(Math.max(1, deadline - Date.now()));
        if (result) return result;
        await new Promise(resolvePromise => setTimeout(resolvePromise, 2));
      }
      throw new Error(message);
    };
    const pidIsGone = async (pid, remainingMs) => {
      try { process.kill(pid, 0); } catch (error) { return error?.code === 'ESRCH'; }
      const probe = spawn('ps', ['-o', 'stat=', '-p', String(pid)], { stdio: ['ignore', 'pipe', 'pipe'] });
      children.add(probe); // Own before any fallible capture or deadline wait.
      let stdout = ''; let stderr = ''; let stdoutEof = false; let stderrEof = false; let overflow = false;
      const closed = new Promise(resolveClose => probe.once('close', (code, signal) => {
        children.delete(probe); resolveClose({ code, signal });
      }));
      for (const [stream, which] of [[probe.stdout, 'stdout'], [probe.stderr, 'stderr']]) {
        stream.setEncoding('utf8');
        stream.on('end', () => { if (which === 'stdout') stdoutEof = true; else stderrEof = true; });
        stream.on('data', chunk => {
          const value = (which === 'stdout' ? stdout : stderr) + chunk;
          if (Buffer.byteLength(value, 'utf8') > 1_024) { overflow = true; try { probe.kill('SIGKILL'); } catch {} }
          else if (which === 'stdout') stdout = value; else stderr = value;
        });
      }
      probe.on('error', () => { c08Retain = true; });
      const exit = await waitForCloseBounded(closed, remainingMs);
      if (!exit) { try { probe.kill('SIGKILL'); } catch {} c08Retain = true; return false; }
      if (overflow || !stdoutEof || !stderrEof || exit.signal !== null) { c08Retain = true; return false; }
      return exit.code === 0 && /^Z/u.test(stdout.trim());
    };
    t.after(async () => {
      for (const pid of runnerPids) {
        try { process.kill(pid, 'SIGCONT'); } catch {}
        try { process.kill(pid, 'SIGKILL'); } catch {}
      }
      for (const child of children) {
        try { child.kill('SIGKILL'); } catch {}
      }
      await Promise.all([...children].map(child => new Promise(resolvePromise => {
        if (child.exitCode !== null || child.signalCode !== null) return resolvePromise();
        child.once('close', resolvePromise);
        setTimeout(resolvePromise, 1_500).unref();
      })));
      for (const recordObservation of fixtureObservations) recordObservation();
      const cleanupErrors = [];
      for (const handle of c08Handles) {
        try { closeCrashHandle(handle); } catch (error) { cleanupErrors.push(error); c08Retain = true; }
      }
      if (children.size > 0 || runnerPids.size > 0) c08Retain = true;
      const cleanup = cleanupErrors.length ? closedErrorProjection(cleanupErrors[0])
        : { errorKind: 'none', code: 'none', sqlitePrimaryCode: null };
      recordClosedFixture({ caseId: 'OPS05.C08', phase: 'cleanup', primaryFailed: c08PrimaryFailed,
        ...(c08PrimaryFailed ? closedErrorProjection(c08PrimaryError) : {}),
        cleanupFailed: cleanupErrors.length > 0, cleanupKind: cleanup.errorKind, cleanupCode: cleanup.code,
        cleanupPrimaryCode: cleanup.sqlitePrimaryCode, nativeClose: c08Handles.size === 0 ? 'confirmed' : 'unknown',
        processClosed: children.size === 0 && runnerPids.size === 0 });
      if (!c08Retain) crashDir.cleanup();
      if (cleanupErrors.length) throw new AggregateError(
        c08PrimaryFailed ? [c08PrimaryError, ...cleanupErrors] : cleanupErrors,
        'C08 native cleanup was not confirmed.', c08PrimaryFailed ? { cause: c08PrimaryError } : undefined);
    });
    try {

    const oldOwner = 'ops-worker:crashed-supervisor';
    const crashed = openFixture(oldOwner);
    const admitted = await crashed.ready;
    assert.equal(admitted.accepted, true);
    crashed.markResultValidated();
    c08Phase = 'progress';
    const inspect = openCrashDb(crashDir.databasePath, { readOnly: true });
    const committed = await waitUntil(() => {
      const row = inspect.prepare(`
        SELECT w.state, w.runner_pid, w.generation, w.fence_token,
          f.state AS workflow_state, f.checkpoint_sequence,
          c.payload_json
        FROM operations_work w JOIN workflows f ON f.workflow_id=w.workflow_id
        JOIN workflow_checkpoints c ON c.workflow_id=f.workflow_id AND c.sequence=f.checkpoint_sequence
        WHERE w.job_id=?
      `).get(admitted.jobId);
      if (!row || row.state !== 'running' || !Number.isSafeInteger(row.runner_pid)
          || row.checkpoint_sequence < 2 || row.workflow_state !== 'running') return null;
      try {
        const payload = JSON.parse(row.payload_json);
        const state = payload.data ?? payload;
        const completed = JSON.parse(state.completedResultsJson ?? '{}');
        return Object.keys(completed).length > 0 ? { row, completedCount: Object.keys(completed).length } : null;
      } catch { return null; }
    }, 8_000, 'The actual runner did not durably commit a local step before the crash barrier.');
    const oldRunnerPid = Number(committed.row.runner_pid);
    runnerPids.add(oldRunnerPid);
    c08Phase = 'stop';
    assert.equal(process.kill(oldRunnerPid, 'SIGSTOP'), true);
    const priorGeneration = Number(committed.row.generation);
    const priorFence = Number(committed.row.fence_token);
    assert.ok(priorGeneration >= 1);
    assert.ok(priorFence >= 1);
    const beforeCrash = {
      state: committed.row.state,
      workflowState: committed.row.workflow_state,
      checkpointSequence: Number(committed.row.checkpoint_sequence),
      committedStepCount: committed.completedCount,
      generation: priorGeneration,
      fence: priorFence,
      inFlight: Number(inspect.prepare('SELECT in_flight FROM operations_meta WHERE singleton=1').get().in_flight),
      attempts: Number(inspect.prepare('SELECT COUNT(*) AS count FROM operation_attempts WHERE workflow_id=?').get(admitted.workflowId).count)
    };
    assert.equal(beforeCrash.inFlight, 1);
    assert.equal(beforeCrash.attempts, 0);

    c08Phase = 'crash';
    const crashStartedAt = performance.now();
    assert.equal(crashed.child.kill('SIGKILL'), true);
    METRICS.processSigkillSignalsSent += 1;
    const supervisorClose = await waitForCloseBounded(crashed.closed, 2_000);
    assert.ok(supervisorClose);
    assert.equal(supervisorClose.signal, 'SIGKILL');
    const supervisorCloseElapsedMs = performance.now() - crashStartedAt;
    METRICS.processCloseElapsedMsMax = Math.max(METRICS.processCloseElapsedMsMax, supervisorCloseElapsedMs);
    METRICS.processCloseSamples += 1;
    assert.equal(process.kill(oldRunnerPid, 'SIGKILL'), true);
    METRICS.processSigkillSignalsSent += 1;
    c08Phase = 'runner_close';
    const runnerCloseStartedAt = performance.now();
    const runnerGone = await waitUntil(remainingMs => pidIsGone(oldRunnerPid, remainingMs), 3_000,
      'The exact orphaned runner PID did not exit after its explicit SIGKILL.');
    assert.equal(runnerGone, true);
    const runnerCloseElapsedMs = performance.now() - runnerCloseStartedAt;
    METRICS.processCloseElapsedMsMax = Math.max(METRICS.processCloseElapsedMsMax, runnerCloseElapsedMs);
    METRICS.processCloseSamples += 1;
    runnerPids.delete(oldRunnerPid);
    closeCrashHandle(inspect);

    c08Phase = 'lease_expiry';
    const recoveryClock = openCrashDb(crashDir.databasePath);
    recoveryClock.prepare('UPDATE workflows SET lease_expires_ms=0 WHERE workflow_id=?').run(admitted.workflowId);
    recoveryClock.prepare('UPDATE operations_work SET lease_expires_ms=0 WHERE job_id=?').run(admitted.jobId);
    closeCrashHandle(recoveryClock);
    const afterCrashDb = openCrashDb(crashDir.databasePath, { readOnly: true });
    const crashedRows = afterCrashDb.prepare(`
      SELECT w.state, w.runner_pid, w.generation, f.state AS workflow_state, f.checkpoint_sequence
      FROM operations_work w JOIN workflows f ON f.workflow_id=w.workflow_id WHERE w.job_id=?
    `).get(admitted.jobId);
    assert.equal(crashedRows.state, 'running');
    assert.equal(crashedRows.workflow_state, 'running');
    assert.equal(Number(crashedRows.generation), priorGeneration);
    assert.equal(Number(afterCrashDb.prepare('SELECT in_flight FROM operations_meta WHERE singleton=1').get().in_flight), 1);
    assert.equal(Number(afterCrashDb.prepare('SELECT COUNT(*) AS count FROM operation_attempts WHERE workflow_id=?').get(admitted.workflowId).count), 0);
    closeCrashHandle(afterCrashDb);

    c08Phase = 'restart_ready';
    const raceA = openFixture('ops-worker:recovery-race-a', 'worker-crash-recovery', 1);
    const raceB = openFixture('ops-worker:recovery-race-b', 'worker-crash-recovery', 2);
    const [readyA, readyB] = await Promise.all([raceA.ready, raceB.ready]);
    assert.equal(readyA.jobId, admitted.jobId);
    assert.equal(readyB.jobId, admitted.jobId);
    assert.equal(readyA.duplicate, true);
    assert.equal(readyB.duplicate, true);
    raceA.markResultValidated();
    raceB.markResultValidated();
    c08Phase = 'completion';
    const completeDb = openCrashDb(crashDir.databasePath, { readOnly: true });
    let finalRow;
    try {
      finalRow = await waitUntil(() => {
      const row = completeDb.prepare(`
        SELECT w.state, w.worker_id, w.runner_pid, w.generation, w.fence_token,
          f.state AS workflow_state, f.fence_token AS workflow_fence, f.checkpoint_sequence
        FROM operations_work w JOIN workflows f ON f.workflow_id=w.workflow_id WHERE w.job_id=?
      `).get(admitted.jobId);
      return row?.state === 'completed' ? row : null;
      }, 12_000, 'The restarted worker race did not finish the admitted local plan.');
    } catch (error) {
      throw error; // Keep the wait failure itself; closed phase capture needs no additional query.
    }
    const claimCount = Number(completeDb.prepare("SELECT COUNT(*) AS count FROM workflow_events WHERE workflow_id=? AND event_type='operations_work_claimed'").get(admitted.workflowId).count);
    const attemptCount = Number(completeDb.prepare('SELECT COUNT(*) AS count FROM operation_attempts WHERE workflow_id=?').get(admitted.workflowId).count);
    const counters = completeDb.prepare('SELECT pending_records, pending_bytes, in_flight, reserved_event_slots, reserved_disk_bytes FROM operations_meta WHERE singleton=1').get();
    const checkpointPayload = JSON.parse(completeDb.prepare('SELECT payload_json FROM workflow_checkpoints WHERE workflow_id=? ORDER BY sequence DESC LIMIT 1').get(admitted.workflowId).payload_json);
    const checkpointState = checkpointPayload.data ?? checkpointPayload;
    const finalResults = JSON.parse(checkpointState.completedResultsJson);
    assert.equal(finalRow.workflow_state, 'succeeded');
    assert.equal(Number(finalRow.generation), priorGeneration + 1);
    assert.equal(Number(finalRow.fence_token), priorFence + 1);
    assert.equal(Number(finalRow.workflow_fence), priorFence + 1);
    assert.equal(claimCount, 2, 'One initial and one recovered claim event must exist; the competing owner cannot claim twice.');
    assert.equal(attemptCount, 0);
    assert.equal(Object.keys(finalResults).length, 32);
    assert.deepEqual({ ...counters }, {
      pending_records: 0, pending_bytes: 0, in_flight: 0, reserved_event_slots: 0, reserved_disk_bytes: 0
    });
    closeCrashHandle(completeDb);
    const racersSignalAt = performance.now();
    for (const supervisor of [raceA, raceB]) {
      assert.equal(supervisor.child.kill('SIGTERM'), true);
      METRICS.processSigtermSignalsSent += 1;
    }
    c08Phase = 'close';
    const closedRacers = await Promise.all([raceA, raceB].map(supervisor => waitForCloseBounded(
      supervisor.closed, Math.max(1, 2_000 - (performance.now() - racersSignalAt)))));
    assert.ok(closedRacers.every(Boolean), 'Both restarted supervisors must close inside the original 2000ms budget.');
    assert.deepEqual(closedRacers.map(item => item.code), [0, 0]);
    const racersCloseElapsedMs = performance.now() - racersSignalAt;
    METRICS.processCloseElapsedMsMax = Math.max(METRICS.processCloseElapsedMsMax, racersCloseElapsedMs);
    METRICS.processCloseSamples += 2;
    c08Phase = 'fencing';
    const reopened = openCrashStore(crashDir.databasePath);
    const finalWorkflow = reopened.getWorkflow(admitted.workflowId);
    const finalCheckpoint = reopened.getCheckpoint(admitted.workflowId);
    const finalEventCount = reopened.listEvents({ workflowId: admitted.workflowId }).length;
    assert.throws(() => reopened._operationsFinish(OPERATIONS_STORAGE_CAPABILITY, {
      jobId: admitted.jobId, ownerId: oldOwner, generation: priorGeneration,
      fence: priorFence, outcome: 'stopped', childExited: true
    }), error => error.code === 'STALE_FENCE');
    assert.equal(reopened.listEvents({ workflowId: admitted.workflowId }).length, finalEventCount);
    assert.equal(finalWorkflow.state, 'succeeded');
    assert.equal(Object.keys(JSON.parse(finalCheckpoint.data.completedResultsJson)).length, 32);
    closeCrashHandle(reopened);
    assert.ok(supervisorCloseElapsedMs < 2_000);
    assert.ok(runnerCloseElapsedMs < 3_000);
    assert.ok(racersCloseElapsedMs < 2_000);
    } catch (error) {
      c08PrimaryFailed = true; c08PrimaryError = error; c08Retain = true;
      recordClosedFixture({ caseId: 'OPS05.C08', phase: c08Phase, primaryFailed: true,
        ...closedErrorProjection(error), nativeClose: c08Handles.size === 0 ? 'confirmed' : 'unknown' });
      throw error;
    }
  });

  let logicalNow = 10_000;
  const stale = tempDirectory('ah11-stale-runner-');
  t.after(() => stale.cleanup());
  const staleStore = openStore(stale.databasePath, { clock: () => logicalNow });
  t.after(() => staleStore.close());
  const policy = operationsStoragePolicy(readOperationsConfig());
  const liveWork = admit(staleStore, 'tenant-a', 'project-a', 'stale-live-pid');
  const deadWork = admit(staleStore, 'tenant-b', 'project-b', 'stale-dead-pid');
  const liveClaim = staleStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
    ownerId: 'ops-worker:crashed-live', policy
  }).claim;
  const deadClaim = staleStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
    ownerId: 'ops-worker:crashed-dead', policy
  }).claim;
  staleStore._operationsSetRunnerPid(OPERATIONS_STORAGE_CAPABILITY, {
    jobId: liveClaim.jobId, ownerId: liveClaim.ownerId,
    generation: liveClaim.generation, fence: liveClaim.fence, runnerPid: process.pid
  });
  staleStore._operationsSetRunnerPid(OPERATIONS_STORAGE_CAPABILITY, {
    jobId: deadClaim.jobId, ownerId: deadClaim.ownerId,
    generation: deadClaim.generation, fence: deadClaim.fence, runnerPid: unusedProcessId()
  });
  logicalNow = Math.max(liveClaim.leaseExpiresAtMs, deadClaim.leaseExpiresAtMs) + 1;
  const staleHealth = readOperationsHealth({
    store: staleStore, config: {}, lifecycle: 'accepting', heartbeatAgeMs: 10
  });
  assert.ok(staleHealth.degradedReasons.includes('LEASE_STALE'));
  assert.equal(staleHealth.staleLeases.count, 2);
  const freshRecoveryCandidates = staleStore._operationsExpiredClaims(OPERATIONS_STORAGE_CAPABILITY, { policy });
  assert.equal(freshRecoveryCandidates.items.length, 2);
  assert.equal(freshRecoveryCandidates.items.find(item => item.jobId === liveWork.jobId).runnerPid, process.pid);
  assert.equal(staleStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
    ownerId: 'ops-worker:without-exit-proof', policy, recoverableJobIds: []
  }), null);
  const reclaimed = staleStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
    ownerId: 'ops-worker:restarted', policy, recoverableJobIds: [deadWork.jobId]
  }).claim;
  assert.equal(reclaimed.jobId, deadWork.jobId);
  assert.equal(reclaimed.generation, deadClaim.generation + 1);
  assert.equal(reclaimed.fence, deadClaim.fence + 1);
  staleStore._operationsReadClaim(OPERATIONS_STORAGE_CAPABILITY, {
    jobId: reclaimed.jobId, ownerId: reclaimed.ownerId,
    generation: reclaimed.generation, fence: reclaimed.fence
  });
  assert.throws(() => staleStore._operationsFinish(OPERATIONS_STORAGE_CAPABILITY, {
    jobId: deadClaim.jobId, ownerId: deadClaim.ownerId,
    generation: deadClaim.generation, fence: deadClaim.fence, outcome: 'stopped', childExited: true
  }), error => error.code === 'STALE_FENCE');
  staleStore._operationsFinish(OPERATIONS_STORAGE_CAPABILITY, {
    jobId: reclaimed.jobId, ownerId: reclaimed.ownerId,
    generation: reclaimed.generation, fence: reclaimed.fence, outcome: 'shutdown'
  });
  const staleCounts = new DatabaseSync(stale.databasePath, { readOnly: true });
  assert.equal(Number(staleCounts.prepare('SELECT in_flight FROM operations_meta WHERE singleton = 1').get().in_flight), 1);
  assert.equal(staleCounts.prepare('SELECT runner_pid FROM operations_work WHERE job_id = ?').get(deadWork.jobId).runner_pid, null);
  assert.equal(staleCounts.prepare('SELECT runner_pid FROM operations_work WHERE job_id = ?').get(liveWork.jobId).runner_pid, process.pid);
  staleCounts.close();

  let unknownNow = 20_000;
  const unknown = tempDirectory('ah11-stale-unknown-');
  t.after(() => unknown.cleanup());
  const unknownStore = openStore(unknown.databasePath, { clock: () => unknownNow });
  t.after(() => unknownStore.close());
  const uncertainWork = admit(unknownStore, 'tenant-unknown', 'project-unknown', 'stale-unknown-effect');
  const uncertainClaim = unknownStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
    ownerId: 'ops-worker:unknown-before-crash', policy
  }).claim;
  unknownStore._operationsSetRunnerPid(OPERATIONS_STORAGE_CAPABILITY, {
    jobId: uncertainClaim.jobId, ownerId: uncertainClaim.ownerId,
    generation: uncertainClaim.generation, fence: uncertainClaim.fence, runnerPid: unusedProcessId()
  });
  unknownStore.recordDispatchIntent({
    workflowId: uncertainClaim.workflowId,
    ownerId: uncertainClaim.ownerId,
    fence: uncertainClaim.fence,
    expectedRevision: uncertainClaim.revision,
    attemptId: 'ops-unknown-attempt',
    logicalOperationRef: 'ops-unknown-logical-operation',
    authorityHoldRef: 'synthetic-local-bookkeeping:ops-unknown-hold',
    transportAttemptId: 'ops-unknown-transport',
    eventId: 'ops-unknown-intent'
  });
  unknownNow = uncertainClaim.leaseExpiresAtMs + 1;
  const unknownCandidate = unknownStore._operationsExpiredClaims(OPERATIONS_STORAGE_CAPABILITY, { policy });
  assert.equal(unknownCandidate.items[0].jobId, uncertainWork.jobId);
  const blockedClaim = unknownStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
    ownerId: 'ops-worker:unknown-restart', policy, recoverableJobIds: [uncertainWork.jobId]
  });
  assert.equal(blockedClaim?.claim ?? null, null);
  const unknownRows = new DatabaseSync(unknown.databasePath, { readOnly: true });
  assert.equal(unknownRows.prepare('SELECT state FROM operations_work WHERE job_id = ?').get(uncertainWork.jobId).state, 'unknown');
  assert.equal(Number(unknownRows.prepare('SELECT in_flight FROM operations_meta WHERE singleton = 1').get().in_flight), 0);
  assert.equal(unknownStore.getAttempt('ops-unknown-attempt').state, 'intent');
  assert.equal(unknownStore.getAttempt('ops-unknown-attempt').authorityHoldRef, 'synthetic-local-bookkeeping:ops-unknown-hold');
  unknownRows.close();
  assert.equal(METRICS.providerCalls, 0);
});

test('OPS06_UNCOOPERATIVE_REENTRANT sends signals only to owned children and escalates with bounded reaping', async t => {
  const temporary = tempDirectory('ah11-worker-signal-');
  let retainOps06Evidence = false;

  const worker = createOperationsWorker({
    databasePath: temporary.databasePath,
    ownerId: 'ops-worker:signal',
    config: { childKillGraceMs: 40, childReapReserveMs: 250, shutdownTimeoutMs: 1_000 }
  });
  t.after(async () => {
    let cleanupFailed = false;
    let cleanupError;
    try {
      const result = await worker.stop('test');
      if (result?.stopped !== true || result?.storeClosed !== true || result?.childrenReaped !== true) retainOps06Evidence = true;
    } catch (error) { cleanupFailed = true; cleanupError = error; retainOps06Evidence = true; }
    if (!retainOps06Evidence) temporary.cleanup();
    if (cleanupFailed) throw cleanupError;
  });
  assert.throws(() => createOperationsWorker({
    databasePath: join(temporary.directory, 'callback-rejected.sqlite'), executor: () => true
  }));
  void worker.start();
  const submitted = worker.submitSyntheticWork({
    trustedSyntheticScope: { tenantId: 'signal-tenant', projectId: 'signal-project' },
    idempotencyKey: 'signal-long',
    planJson: JSON.stringify({
      kind: 'agent-harness.private-synthetic-plan', version: 1,
      steps: Array.from({ length: 32 }, (_, index) => ({
        id: `s${String(index).padStart(2, '0')}`,
        operation: 'text.concat',
        dependsOn: index % 12 === 0 ? [] : [`s${String(index - 1).padStart(2, '0')}`],
        args: { parts: index % 12 === 0 ? ['x'.repeat(512)] : [{ $result: `s${String(index - 1).padStart(2, '0')}` }, ''] }
      }))
    })
  });
  assert.equal(submitted.accepted, true);
  const deadline = Date.now() + 3_000;
  let ownedPid = null;
  while (Date.now() < deadline && worker.activeChildCount === 0) await new Promise(resolvePromise => setTimeout(resolvePromise, 1));
  assert.equal(worker.activeChildCount, 1);
  const pgrep = spawnSync('pgrep', ['-P', String(process.pid)], { encoding: 'utf8' });
  if (pgrep.status === 0) ownedPid = Number(pgrep.stdout.trim().split(/\s+/u).at(-1));
  assert.ok(Number.isSafeInteger(ownedPid) && ownedPid > 1);
  METRICS.ownedRunnerStarts += 1;
  const supervisorRssBytes = worker.health().process.rssBytes;
  METRICS.maxSupervisorRssBytes = Math.max(METRICS.maxSupervisorRssBytes, supervisorRssBytes);
  if (Number.isSafeInteger(ownedPid) && ownedPid > 1) {
    const rss = spawnSync('ps', ['-o', 'rss=', '-p', String(ownedPid)], { encoding: 'utf8' });
    const rssKilobytes = Number(rss.stdout.trim());
    if (rss.status === 0 && Number.isSafeInteger(rssKilobytes) && rssKilobytes > 0) {
      METRICS.maxRunnerRssBytes = Math.max(METRICS.maxRunnerRssBytes, rssKilobytes * 1024);
    }
  }
  assert.ok(METRICS.maxSupervisorRssBytes > 0);
  assert.ok(METRICS.maxRunnerRssBytes > 0);
  if (Number.isSafeInteger(ownedPid) && ownedPid > 1) {
    try { process.kill(ownedPid, 'SIGSTOP'); } catch { ownedPid = null; }
  }
  const waiters = Array.from({ length: 16 }, () => worker.waitForOwnedChildrenIdle(5_000));
  assert.equal(await worker.waitForOwnedChildrenIdle(5_000), false);
  const stopping = worker.stop('signal');
  assert.strictEqual(worker.stop('signal'), stopping);
  const rejectedDuringStop = worker.submitSyntheticWork({
    trustedSyntheticScope: { tenantId: 'signal-tenant', projectId: 'signal-project' },
    idempotencyKey: 'after-stop', planJson: PLAN
  });
  assert.equal(rejectedDuringStop.code, 'OPS_UNAVAILABLE');
  const stopped = await stopping;
  if (stopped?.storeClosed !== true || stopped?.childrenReaped !== true) retainOps06Evidence = true;
  METRICS.ownedRunnerTermSignals += 1;
  METRICS.ownedRunnerKillSignals += stopped.escalatedChildren;
  METRICS.ownedRunnerShutdownElapsedMsMax = Math.max(METRICS.ownedRunnerShutdownElapsedMsMax, stopped.elapsedMs);
  METRICS.ownedRunnerShutdownSamples += 1;
  assert.deepEqual(await Promise.all(waiters), Array(16).fill(false));
  if (Number.isSafeInteger(ownedPid) && ownedPid > 1) {
    try { process.kill(ownedPid, 'SIGCONT'); } catch {}
  }
  assert.equal(stopped.childrenReaped, true);
  assert.equal(stopped.childCount, 0);
  assert.ok(stopped.escalatedChildren >= 1);
  assert.ok(stopped.elapsedMs <= 1_000);
  assert.equal(worker.activeChildCount, 0);
  assert.throws(() => worker.start());

  await t.test('OPS06.C02 waitForOwnedChildrenIdle reports process-idle while durable accepted work remains queued', async t => {
    const idleDir = tempDirectory('ah11-owned-idle-semantics-');
    t.after(() => idleDir.cleanup());
    const idleWorker = createOperationsWorker({ databasePath: idleDir.databasePath, ownerId: 'review-worker-idle' });
    t.after(async () => { if (idleWorker.lifecycle !== 'stopped') await idleWorker.stop('test'); });
    const idleStore = openStore(idleDir.databasePath);
    t.after(() => idleStore.close());
    const queued = admit(idleStore, 'idle-tenant', 'idle-project', 'idle-queued');
    assert.equal(queued.accepted, true);
    assert.equal(await idleWorker.waitForOwnedChildrenIdle(1), true);
    const durable = idleStore._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'idle-tenant', projectId: 'idle-project', jobId: queued.jobId
    });
    assert.equal(durable.state, 'queued');
    assert.equal(Number(new DatabaseSync(idleDir.databasePath, { readOnly: true })
      .prepare('SELECT pending_records FROM operations_meta WHERE singleton=1').get().pending_records), 1);
    await idleWorker.stop('test');
  });

  await t.test('OPS06.C01 actual repeated mixed signals share one cleanup and close the owned process', async t => {
  const diagnostic = {
    stage: 'ready_wait', failureClass: 'unknown', exitDisposition: 'unknown', exitCode: null, exitSignal: null,
    phaseElapsedMs: {
      signalToFirstStopElapsedMs: null, restartWorkElapsedMs: null, finalStopElapsedMs: null,
      readyToFinalRecordElapsedMs: null
    }
  };
  let diagnosticRecorded = false;
  let processCloseElapsedMs = null;
  let restartProcessCloseElapsedMs = null;
  const recordDiagnostic = () => {
    if (diagnosticRecorded) return;
    diagnosticRecorded = recordOps06ClosePhaseDiagnostic(
      diagnostic, processCloseElapsedMs, restartProcessCloseElapsedMs
    );
  };
  t.after(recordDiagnostic);

  const databasePath = join(temporary.directory, 'repeated-signal.sqlite');
  const flushMarker = join(temporary.directory, 'repeated-signal-flush.json');
  const signalChild = spawn(process.execPath, [
    join(ROOT, 'tests/operations/fixtures/repeated-signal-child.mjs'), databasePath, flushMarker
  ], { cwd: ROOT, env: baseEnvironment(), stdio: ['ignore', 'pipe', 'pipe'] });
  METRICS.processRuns += 1;
  const signalObservation = observeClosedChild(signalChild);
  signalChild.once('error', () => { retainOps06Evidence = true; });
  let signalChildClosed = false;
  let signalResultValidated = false;
  let signalOutput = '';
  let readyRecord = null;
  signalChild.stdout.setEncoding('utf8');
  signalChild.stdout.on('data', chunk => {
    signalOutput += chunk;
    for (const line of signalOutput.split(/\r?\n/u)) {
      try {
        const value = JSON.parse(line);
        if (value.event === 'ready') readyRecord = value;
      } catch {}
    }
  });
  signalChild.stderr.setEncoding('utf8');
  signalChild.stderr.on('data', chunk => { signalOutput += chunk; });
  const closed = signalObservation.closed;
  t.after(async () => {
    const exit = signalChildClosed ? signalObservation.snapshot() : await killOwnedChildAndReap(signalChild, closed);
    signalChildClosed = exit?.processClosed === true;
    // Reap is consumed; a late exit cannot establish missing native/result evidence.
    if (requiresClosedFixtureRetention(exit, signalResultValidated ? 'confirmed' : 'unknown', !signalResultValidated)) retainOps06Evidence = true;
  });

  const readyDeadline = Date.now() + 5_000;
  while (readyRecord === null && Date.now() < readyDeadline && signalChild.exitCode === null) {
    await new Promise(resolvePromise => setTimeout(resolvePromise, 5));
  }
  if (readyRecord === null) {
    diagnostic.stage = 'ready_wait';
    diagnostic.failureClass = 'child_exit';
    diagnostic.exitDisposition = classifyChildExitDisposition({ code: signalChild.exitCode, signal: signalChild.signalCode });
    diagnostic.exitCode = Number.isSafeInteger(signalChild.exitCode) ? signalChild.exitCode : null;
    diagnostic.exitSignal = OPS06_CLOSE_PHASE_SIGNALS.has(signalChild.signalCode) ? signalChild.signalCode : null;
    recordDiagnostic();
  }
  assert.ok(readyRecord, signalOutput);
  diagnostic.stage = 'signal_delivery';
  diagnostic.failureClass = 'unknown';
  const firstSignalAt = performance.now();
  const firstSignalSent = signalChild.kill('SIGTERM');
  if (firstSignalSent) METRICS.processSigtermSignalsSent += 1;
  assert.equal(firstSignalSent, true);
  await new Promise(resolvePromise => setTimeout(resolvePromise, 25));
  const secondSignalSent = signalChild.kill('SIGINT');
  if (secondSignalSent) METRICS.processSigintSignalsSent += 1;
  assert.equal(secondSignalSent, true);
  diagnostic.stage = 'flush_barrier';
  await waitForPath(flushMarker, signalChild, 2_000);
  await new Promise(resolvePromise => setTimeout(resolvePromise, 25));
  const finalSignalSent = signalChild.kill('SIGTERM');
  if (finalSignalSent) METRICS.processSigtermSignalsSent += 1;
  assert.equal(finalSignalSent, true);

  diagnostic.stage = 'supervisor_close_wait';
  const signalExit = await waitForCloseBounded(closed, 2_000);
  if (signalExit) {
    signalChildClosed = true;
    processCloseElapsedMs = performance.now() - firstSignalAt;
    METRICS.processCloseElapsedMsMax = Math.max(METRICS.processCloseElapsedMsMax, processCloseElapsedMs);
    METRICS.processCloseSamples += 1;
    diagnostic.stage = 'supervisor_exit';
    diagnostic.failureClass = signalExit.code === 0 ? 'none' : 'child_exit';
    diagnostic.exitDisposition = classifyChildExitDisposition(signalExit);
    diagnostic.exitCode = Number.isSafeInteger(signalExit.code) ? signalExit.code : null;
    diagnostic.exitSignal = OPS06_CLOSE_PHASE_SIGNALS.has(signalExit.signal) ? signalExit.signal : null;
    const stopFailure = signalOutput.match(/^AH11_OPS_OPS06_STOP_FAILURE stage=(worker_start|runner_wait|runner_pause|worker_stop|stop_validate|record_output|unknown) errorClass=(?:type|range|error)$/mu);
    if (stopFailure) {
      diagnostic.stage = stopFailure[1];
      diagnostic.failureClass = 'child_exit';
    }
  } else {
    diagnostic.stage = 'supervisor_close_wait';
    diagnostic.failureClass = 'child_exit';
    diagnostic.exitDisposition = 'unknown';
    diagnostic.exitCode = null;
    diagnostic.exitSignal = null;
    recordDiagnostic();
  }
  const signalObserved = closedChildObservation(signalExit);
  if (!signalObserved.processClosed || signalObserved.errorEvent || signalObserved.pipeEOF !== 'complete') retainOps06Evidence = true;
  assert.ok(signalExit, signalOutput);
  if (signalExit.code !== 0) retainOps06Evidence = true;
  assert.equal(signalExit.code, 0, signalOutput);
  diagnostic.failureClass = 'assertion';
  diagnostic.stage = 'stopped_record';
  const stoppedLine = signalOutput.split(/\r?\n/u).find(line => line.includes('"event":"stopped"'));
  if (!stoppedLine) retainOps06Evidence = true;
  assert.ok(stoppedLine, signalOutput);
  let stoppedRecord;
  try { stoppedRecord = JSON.parse(stoppedLine); }
  catch (error) { retainOps06Evidence = true; throw error; }
  if (stoppedRecord?.stopped?.storeClosed !== true || stoppedRecord?.stopped?.childrenReaped !== true) retainOps06Evidence = true;
  diagnostic.phaseElapsedMs.signalToFirstStopElapsedMs = stoppedRecord.phaseElapsedMs?.signalToFirstStopElapsedMs ?? null;
  diagnostic.phaseElapsedMs.readyToFinalRecordElapsedMs = stoppedRecord.phaseElapsedMs?.readyToFinalRecordElapsedMs ?? null;
  diagnostic.stage = 'stopped_record';
  diagnostic.failureClass = 'assertion';

  try {
    assert.equal(stoppedRecord.stopped.stopped, true);
    assert.equal(stoppedRecord.stopped.childrenReaped, true);
    assert.equal(stoppedRecord.stopped.storeClosed, true);
    assert.equal(stoppedRecord.sinkCalls, 1);
    assert.equal(stoppedRecord.providerCalls, 0);
    assert.equal(stoppedRecord.workAfterStop.state, 'queued');
    assert.equal(stoppedRecord.attemptCount, 0);
    assert.equal(stoppedRecord.telemetry.cursor, 0);
    assert.ok(stoppedRecord.telemetry.pendingRecords > 0);
    assert.ok(processCloseElapsedMs < 1_500, `Actual process close took ${processCloseElapsedMs} ms.`);

    signalResultValidated = true;
  } catch (error) { retainOps06Evidence = true; throw error; }

  diagnostic.stage = 'restart_spawn';
  const restartStartedAt = performance.now();
  const restart = startOwnedChild(join(ROOT, 'tests/operations/fixtures/repeated-signal-restart-child.mjs'),
    [databasePath, stoppedRecord.jobId], { timeoutMs: 9_000 });
  restart.child.once('error', () => { retainOps06Evidence = true; });
  let restartClosed = false;
  let restartResultValidated = false;
  let restartExitObservation = null;
  t.after(async () => {
    const exit = restartClosed ? restartExitObservation : await killOwnedChildAndReap(restart.child, restart.closed);
    restartClosed = exit?.processClosed === true;
    const captured = exit ?? restart.snapshot();
    const detail = recordClosedChild(captured.stderr, 'OPS06.C01', 0,
      'restart_wait', captured, restartResultValidated);
    if (!restartResultValidated || requiresClosedFixtureRetention(exit, detail?.nativeClose, detail?.cleanupFailed)) retainOps06Evidence = true;
  });
  diagnostic.stage = 'restart_close';
  const restartExit = await waitForCloseBounded(restart.closed, 10_000);
  restartExitObservation = restartExit;
  if (restartExit) {
    restartClosed = restartExit.processClosed === true;
    restartProcessCloseElapsedMs = performance.now() - restartStartedAt;
    diagnostic.stage = 'restart_exit';
    diagnostic.failureClass = restartExit.code === 0 ? 'none' : 'child_exit';
    diagnostic.exitDisposition = classifyChildExitDisposition(restartExit);
    diagnostic.exitCode = Number.isSafeInteger(restartExit.code) ? restartExit.code : null;
    diagnostic.exitSignal = OPS06_CLOSE_PHASE_SIGNALS.has(restartExit.signal) ? restartExit.signal : null;
    const restartFailure = restartExit.stderr.match(/^AH11_OPS_OPS06_RESTART_FAILURE stage=(restart_start|restart_wait|restart_stop|restart_verify) errorClass=(?:assertion|error)$/mu);
    if (restartFailure) {
      diagnostic.stage = restartFailure[1];
      diagnostic.failureClass = 'child_exit';
    }
  } else {
    diagnostic.stage = 'restart_close';
    diagnostic.failureClass = 'child_exit';
    diagnostic.exitDisposition = 'unknown';
    diagnostic.exitCode = null;
    diagnostic.exitSignal = null;
    recordDiagnostic();
  }
  const capturedRestart = restartExit ?? restart.snapshot();
  const restartDetail = parseClosedChildDiagnostic(capturedRestart.stderr, 'OPS06.C01');
  if (requiresClosedFixtureRetention(restartExit, restartDetail?.nativeClose, restartDetail?.cleanupFailed)) retainOps06Evidence = true;
  // Retain unknown ownership before this assertion can abort the nested case.
  assert.ok(restartExit, 'The fresh restart process must close under its separate bounded owner wait.');
  if (restartExit.code !== 0 || restartDetail?.primaryFailed) retainOps06Evidence = true;
  assert.equal(restartExit.code, 0, restartExit.stderr);
  let restartRecord = null;
  try { restartRecord = JSON.parse(restartExit.stdout.trim()); } catch {}
  diagnostic.stage = 'restart_record';
  diagnostic.failureClass = 'assertion';
  assert.equal(restartRecord?.event, 'restarted');
  diagnostic.phaseElapsedMs.restartWorkElapsedMs = restartRecord.phaseElapsedMs.restartWorkElapsedMs;
  diagnostic.phaseElapsedMs.finalStopElapsedMs = restartRecord.phaseElapsedMs.finalStopElapsedMs;
  diagnostic.stage = 'restart_state';
  diagnostic.failureClass = 'assertion';

  assert.equal(restartRecord.providerCalls, 0);
  assert.equal(restartRecord.afterRestart.state, 'completed');
  assert.equal(restartRecord.afterRestart.workflowState, 'succeeded');
  assert.ok(restartRecord.afterRestart.generation > stoppedRecord.workAfterStop.generation);
  assert.ok(restartRecord.afterRestart.fence > stoppedRecord.workAfterStop.fence);
  assert.equal(restartRecord.attemptCount, 0);
  assert.ok(processCloseElapsedMs >= stoppedRecord.stopped.elapsedMs,
    'The parent-observed first process close must include the bounded worker stop interval.');
  restartResultValidated = true;
  diagnostic.exitDisposition = 'zero';
  diagnostic.exitCode = 0;
  diagnostic.exitSignal = null;
  diagnostic.stage = 'stopped_record';
  diagnostic.failureClass = 'assertion';
  await t.test('OPS06.C03 clears the losing shutdown timer and distinguishes stop return from process close', () => {
    try {
      assert.ok(processCloseElapsedMs >= stoppedRecord.stopped.elapsedMs,
        'The parent-observed process close must include at least the worker stop-return interval.');
      assert.ok(processCloseElapsedMs < 1_500);
      assert.equal(stoppedRecord.telemetry.cursor, 0);
      assert.ok(stoppedRecord.telemetry.pendingRecords > 0);
      assert.equal(stoppedRecord.sinkCalls, 1);
    } catch (error) {
      recordDiagnostic();
      throw error;
    }
  });
  diagnostic.stage = 'complete';
  diagnostic.failureClass = 'none';
  recordDiagnostic();
});
});

test('OPS07_STORE_RESOURCE_FAILURE maps real SQLite BUSY to typed bounded backpressure and reports headroom', async t => {
  const temporary = tempDirectory('ah11-store-busy-');
  t.after(() => temporary.cleanup());
  const worker = createOperationsWorker({ databasePath: temporary.databasePath, ownerId: 'ops-worker:busy' });
  t.after(async () => { await worker.stop('test'); });
  void worker.start();
  const diagnosticStore = openStore(temporary.databasePath);
  t.after(() => diagnosticStore.close());
  const lock = new DatabaseSync(temporary.databasePath, { timeout: 20 });
  let lockClosed = false;
  t.after(() => {
    try { if (lock.isTransaction) lock.exec('ROLLBACK'); } catch {}
    if (!lockClosed) {
      try { lock.close(); } catch {}
    }
  });
  lock.exec('BEGIN IMMEDIATE');
  let nativeAdmissionError = null;
  try {
    admit(diagnosticStore, 'busy-native-tenant', 'busy-native-project', 'busy-native', PLAN);
  } catch (error) {
    nativeAdmissionError = error;
  }
  assert.equal(nativeAdmissionError?.code, 'STORE_BUSY');
  const nativeBusy = readOperationsAdmissionBusyDiagnostic(nativeAdmissionError);
  assert.deepEqual(nativeBusy, {
    transactionBoundary: 'begin', sqlitePrimaryCode: 5,
    transactionActive: null, rollbackDisposition: 'not_observed'
  });
  assert.equal(Object.isFrozen(nativeBusy), true);
  const nativeFailure = createAdmissionFailureDiagnostic('admission', nativeAdmissionError);
  assert.deepEqual(nativeFailure, {
    schema: 'ops-admission-failure.v2', stage: 'admission', code: 'STORE_BUSY',
    transactionBoundary: 'begin', sqlitePrimaryCode: 5,
    transactionActive: null, rollbackDisposition: 'not_observed'
  });
  const forgedRuntimeBusy = runtimeError({
    module: 'operations', operation: '_operationsAdmit', code: 'STORE_BUSY', retryable: true,
    safe_message: 'synthetic only'
  });
  const forgedNativeBusy = Object.assign(new Error('synthetic only'), { code: 'STORE_BUSY', errcode: 5 });
  assert.equal(readOperationsAdmissionBusyDiagnostic(forgedRuntimeBusy), null);
  assert.equal(readOperationsAdmissionBusyDiagnostic(forgedNativeBusy), null);
  let proxyReads = 0;
  const proxyCanary = new Proxy({}, {
    getOwnPropertyDescriptor() { proxyReads += 1; throw new Error('proxy canary'); },
    get() { proxyReads += 1; throw new Error('proxy canary'); }
  });
  let getterReads = 0;
  const getterCanary = Object.defineProperty({}, 'code', {
    get() { getterReads += 1; throw new Error('getter canary'); }
  });
  assert.equal(readOperationsAdmissionBusyDiagnostic(proxyCanary), null);
  assert.equal(readOperationsAdmissionBusyDiagnostic(getterCanary), null);
  assert.equal(proxyReads, 0);
  assert.equal(getterReads, 0);
  const result = worker.submitSyntheticWork({
    trustedSyntheticScope: { tenantId: 'busy-tenant', projectId: 'busy-project' },
    idempotencyKey: 'busy-1', planJson: PLAN
  });
  assert.deepEqual({ accepted: result.accepted, code: result.code, reasonCode: result.reasonCode }, {
    accepted: false, code: 'OPS_BACKPRESSURE', reasonCode: 'STORE_BUSY'
  });
  lock.exec('ROLLBACK');
  lock.close();
  lockClosed = true;
  const health = worker.health();
  METRICS.maxSupervisorRssBytes = Math.max(METRICS.maxSupervisorRssBytes, health.process.rssBytes);
  assert.ok(Number.isSafeInteger(health.resources.freeBytes));
  assert.ok(health.resources.freeBytes > health.resources.minimumFreeBytes);
  assert.equal(health.process.workerCount, 4);
  assert.equal(worker.readScopedWorkPage({ trustedSyntheticScope: { tenantId: 'busy-tenant', projectId: 'busy-project' } }).items.length, 0);

  await t.test('OPS07.C01 reserves a concurrent valid 32-step lifecycle before admission and bounds DB, WAL, SHM, events, and reopen state', async t => {
    const nearCap = tempDirectory('ah11-lifecycle-reservation-');
    const diagnosticSession = createOperationsDiagnosticSession({ directory: nearCap.directory });
    let capacityWorker = null;
    let first = null;
    let second = null;
    t.after(async () => {
      let cleanupComplete = true;
      try {
        if (capacityWorker && capacityWorker.lifecycle !== 'stopped') {
          const stoppedForCapture = await capacityWorker.stop('test');
          cleanupComplete = stoppedForCapture.stopped === true && stoppedForCapture.childrenReaped === true;
        }
      } catch {
        cleanupComplete = false;
      }
      try {
        let capture;
        try {
          const finalState = captureOperationsFinalState(nearCap.databasePath, [first?.jobId, second?.jobId]);
          capture = readOperationsDiagnosticSession(diagnosticSession, {
            finalState,
            cleanupIncomplete: !cleanupComplete
          });
        } catch {}
        if (!capture) capture = {
          schema: 'agent-harness.ops07-session.v1',
          slots: [], supervisor: [], finalState: { available: false },
          flags: { enabled: true, complete: false, missingChildCapture: true, malformed: true,
            overflow: false, writeFailed: false, abruptExit: true, cleanupIncomplete: true }
        };
        t.diagnostic(`OPS07_CAPTURE_V1 ${JSON.stringify(capture)}`);
        assert.equal(capture.flags.complete, true, `The bounded OPS07 diagnostic capture is incomplete: ${JSON.stringify(capture.flags)}`);
        assert.equal(capture.slots.length, 2);
        for (const slot of capture.slots) {
          assert.equal(slot.records.filter(record => record.stage === 'admission_committed').length, 32);
          assert.equal(slot.records.filter(record => record.stage === 'completion_committed').length, 32);
          assert.equal(slot.records.filter(record => record.stage === 'controller_result' && record.status === 'completed').length, 1);
        }
      } finally {
        disposeOperationsDiagnosticSession(diagnosticSession);
        nearCap.cleanup();
      }
    });
    const nearCapConfig = readOperationsConfig({
      workerCount: 2, maxInFlightGlobal: 2, maxInFlightTenant: 1,
      // Stay within the configured 250 ms maximum; OPS07.C04 separately checks bounded BUSY reporting under a held writer lock.
      maxStoreBytes: 67_108_864, maxWalBytes: 16_777_216, busyTimeoutMs: 250
    });
    capacityWorker = createOperationsWorker({
      databasePath: nearCap.databasePath, ownerId: 'review-worker', config: nearCapConfig,
      diagnosticSession
    });
    void capacityWorker.start();
    const planJson = JSON.stringify({
      kind: 'agent-harness.private-synthetic-plan', version: 1,
      steps: Array.from({ length: 32 }, (_, index) => ({
        id: `h${String(index).padStart(2, '0')}`, operation: 'text.concat',
        dependsOn: index % 12 === 0 ? [] : [`h${String(index - 1).padStart(2, '0')}`],
        args: { parts: index % 12 === 0 ? ['x'.repeat(512)] : [{ $result: `h${String(index - 1).padStart(2, '0')}` }, ''] }
      }))
    });
    const scopeA = { tenantId: 'highwater-tenant-a', projectId: 'highwater-project-a' };
    const scopeB = { tenantId: 'highwater-tenant-b', projectId: 'highwater-project-b' };
    first = capacityWorker.submitSyntheticWork({ trustedSyntheticScope: scopeA, idempotencyKey: 'highwater-a', planJson });
    second = capacityWorker.submitSyntheticWork({ trustedSyntheticScope: scopeB, idempotencyKey: 'highwater-b', planJson });
    assert.equal(first.accepted, true);
    assert.equal(second.accepted, true);
    const thirdScope = { tenantId: 'highwater-tenant-c', projectId: 'highwater-project-c' };
    const rejected = capacityWorker.submitSyntheticWork({ trustedSyntheticScope: thirdScope, idempotencyKey: 'highwater-c', planJson });
    assert.equal(rejected.accepted, false);
    assert.equal(rejected.reasonCode, 'STORE_CAPACITY');
    assert.ok(capacityWorker.health().degradedReasons.includes('STORE_CAPACITY'));
    assert.equal(capacityWorker.readScopedWorkPage({ trustedSyntheticScope: thirdScope }).items.length, 0);

    const concurrentDeadline = Date.now() + 3_000;
    while (capacityWorker.activeChildCount < 2 && Date.now() < concurrentDeadline) {
      await new Promise(resolvePromise => setTimeout(resolvePromise, 2));
    }
    assert.equal(capacityWorker.activeChildCount, 2);
    const ownedChildren = spawnSync('pgrep', ['-P', String(process.pid)], { encoding: 'utf8' });
    assert.equal(ownedChildren.status, 0, ownedChildren.stderr);
    const runnerPids = ownedChildren.stdout.trim().split(/\s+/u).map(Number).filter(pid => {
      const command = spawnSync('ps', ['-o', 'command=', '-p', String(pid)], { encoding: 'utf8' });
      return command.status === 0 && command.stdout.includes('runner.js');
    });
    assert.equal(runnerPids.length, 2);
    METRICS.ownedRunnerCountHighWater = Math.max(METRICS.ownedRunnerCountHighWater, runnerPids.length);
    const supervisorFdCount = countOpenFileDescriptors(process.pid);
    assert.ok(Number.isSafeInteger(supervisorFdCount) && supervisorFdCount > 0);
    METRICS.supervisorOpenFileDescriptors = Math.max(METRICS.supervisorOpenFileDescriptors, supervisorFdCount);
    for (const pid of runnerPids) {
      const fdCount = countOpenFileDescriptors(pid);
      assert.ok(Number.isSafeInteger(fdCount) && fdCount > 0);
      METRICS.runnerOpenFileDescriptors = Math.max(METRICS.runnerOpenFileDescriptors, fdCount);
    }

    const updateHighWater = () => {
      const health = capacityWorker.health();
      const actual = health.resources.storeBytes;
      const reserved = health.resources.reservedCompletionBytes;
      METRICS.storeHighWaterBytes = Math.max(METRICS.storeHighWaterBytes, actual);
      METRICS.storeReservedHighWaterBytes = Math.max(METRICS.storeReservedHighWaterBytes, actual + reserved);
      METRICS.databaseHighWaterBytes = Math.max(METRICS.databaseHighWaterBytes, health.resources.databaseBytes);
      METRICS.walHighWaterBytes = Math.max(METRICS.walHighWaterBytes, health.resources.walBytes);
      METRICS.shmHighWaterBytes = Math.max(METRICS.shmHighWaterBytes, health.resources.shmBytes);
      METRICS.freeSpaceLowWaterBytes = Math.min(METRICS.freeSpaceLowWaterBytes, health.resources.freeBytes);
      METRICS.pendingEventHighWaterRecords = Math.max(METRICS.pendingEventHighWaterRecords, health.telemetry.pendingRecords);
      METRICS.reservedEventHighWaterSlots = Math.max(METRICS.reservedEventHighWaterSlots, health.telemetry.reservedEventSlots);
      METRICS.resourceHighWaterSamples += 1;
      assert.ok(actual + reserved <= nearCapConfig.maxStoreBytes,
        `Actual store plus retained lifecycle reservation ${actual + reserved} exceeded ${nearCapConfig.maxStoreBytes}.`);
      assert.ok(health.resources.walBytes <= nearCapConfig.maxWalBytes);
      assert.ok(health.telemetry.pendingRecords + health.telemetry.reservedEventSlots <= nearCapConfig.maxEventBacklog);
    };
    const deadline = Date.now() + 20_000;
    let rowA;
    let rowB;
    while (Date.now() < deadline) {
      updateHighWater();
      rowA = capacityWorker.readScopedWorkPage({ trustedSyntheticScope: scopeA }).items.find(row => row.jobId === first.jobId);
      rowB = capacityWorker.readScopedWorkPage({ trustedSyntheticScope: scopeB }).items.find(row => row.jobId === second.jobId);
      if (rowA?.state === 'completed' && rowB?.state === 'completed') break;
      await new Promise(resolvePromise => setTimeout(resolvePromise, 25));
    }
    const completionDiagnostic = JSON.stringify({ rowA, rowB, health: capacityWorker.health() });
    assert.equal(rowA?.state, 'completed', completionDiagnostic);
    assert.equal(rowB?.state, 'completed', completionDiagnostic);
    updateHighWater();
    const stopped = await capacityWorker.stop('test');
    assert.equal(stopped.stopped, true);
    assert.equal(stopped.childrenReaped, true);
    const reopened = openStore(nearCap.databasePath);
    for (const [scope, item] of [[scopeA, first], [scopeB, second]]) {
      const status = reopened._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
        tenantId: scope.tenantId, projectId: scope.projectId, jobId: item.jobId
      });
      assert.equal(status.state, 'completed');
      const checkpoint = reopened.getCheckpoint(item.workflowId);
      assert.equal(Object.keys(JSON.parse(checkpoint.data.completedResultsJson)).length, 32);
      assert.equal(reopened.getWorkflow(item.workflowId).state, 'succeeded');
    }
    const raw = new DatabaseSync(nearCap.databasePath, { readOnly: true });
    assert.equal(Number(raw.prepare('SELECT COUNT(*) AS count FROM operation_attempts').get().count), 0);
    assert.equal(Number(raw.prepare(`SELECT COUNT(*) AS count FROM operations_work WHERE state='completed' AND reserved_disk_bytes=0 AND reserved_event_slots=0`).get().count), 2);
    assert.equal(Number(raw.prepare('SELECT pending_records FROM operations_meta WHERE singleton=1').get().pending_records), 0);
    assert.equal(Number(raw.prepare('SELECT reserved_disk_bytes FROM operations_meta WHERE singleton=1').get().reserved_disk_bytes), 0);
    raw.close();
    const finalHealth = readOperationsHealth({ store: reopened, config: nearCapConfig, lifecycle: 'accepting', heartbeatAgeMs: 10 });
    assert.ok(finalHealth.resources.storeBytes <= nearCapConfig.maxStoreBytes);
    assert.ok(finalHealth.resources.walBytes <= nearCapConfig.maxWalBytes);
    reopened.close();
  });

  await t.test('OPS07.C02 injected SQLite write failure leaves the prior durable snapshot intact', () => {
    const failureDir = tempDirectory('ah11-resource-write-failure-');
    t.after(() => failureDir.cleanup());
    const failureStore = openStore(failureDir.databasePath);
    const baseline = new DatabaseSync(failureDir.databasePath, { readOnly: true });
    const before = {
      workflows: Number(baseline.prepare('SELECT COUNT(*) AS count FROM workflows').get().count),
      events: Number(baseline.prepare('SELECT COUNT(*) AS count FROM workflow_events').get().count),
      work: Number(baseline.prepare('SELECT COUNT(*) AS count FROM operations_work').get().count)
    };
    baseline.close();
    const trigger = new DatabaseSync(failureDir.databasePath);
    trigger.exec("CREATE TRIGGER disk_failure BEFORE INSERT ON outbox BEGIN SELECT RAISE(ABORT, 'injected local write failure'); END");
    assert.throws(() => admit(failureStore, 'failure-tenant', 'failure-project', 'write-fail'));
    trigger.close();
    failureStore.close();
    const reopened = openStore(failureDir.databasePath);
    const after = new DatabaseSync(failureDir.databasePath, { readOnly: true });
    assert.deepEqual({
      workflows: Number(after.prepare('SELECT COUNT(*) AS count FROM workflows').get().count),
      events: Number(after.prepare('SELECT COUNT(*) AS count FROM workflow_events').get().count),
      work: Number(after.prepare('SELECT COUNT(*) AS count FROM operations_work').get().count),
      ...after.prepare('SELECT pending_records, pending_bytes, in_flight, reserved_event_slots, reserved_disk_bytes FROM operations_meta WHERE singleton=1').get()
    }, { ...before, pending_records: 0, pending_bytes: 0, in_flight: 0, reserved_event_slots: 0, reserved_disk_bytes: 0 });
    after.close();
    reopened.close();
  });

  await t.test('OPS07.C03 indexed page-plus-one reports bounded incomplete visibility', () => {
    const pageDir = tempDirectory('ah11-page-plus-one-');
    t.after(() => pageDir.cleanup());
    const pageStore = openStore(pageDir.databasePath);
    t.after(() => pageStore.close());
    for (let index = 0; index < 3; index += 1) {
      assert.equal(admit(pageStore, 'page-tenant', 'page-project', `page-${index}`).accepted, true);
    }
    const first = pageStore._operationsReadPage(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'page-tenant', projectId: 'page-project', limit: 2
    });
    assert.equal(first.items.length, 2);
    assert.equal(first.hasMore, true);
    assert.equal(first.incomplete, true);
    assert.equal(first.nextAfterJobId, first.items[1].jobId);
    const second = pageStore._operationsReadPage(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'page-tenant', projectId: 'page-project', afterJobId: first.nextAfterJobId, limit: 2
    });
    assert.equal(second.items.length, 1);
    assert.equal(second.hasMore, false);
    assert.equal(second.incomplete, false);
    assert.notEqual(first.items[0].jobId, second.items[0].jobId);
    const raw = new DatabaseSync(pageDir.databasePath, { readOnly: true });
    const plan = raw.prepare(`EXPLAIN QUERY PLAN SELECT job_id FROM operations_work INDEXED BY operations_work_scope_page_idx
      WHERE tenant_id=? AND project_id=? AND (? IS NULL OR job_id>?) ORDER BY job_id COLLATE BINARY LIMIT ?`)
      .all('page-tenant', 'page-project', null, null, 3).map(row => row.detail).join(' ');
    assert.match(plan, /operations_work_scope_page_idx/u);
    assert.doesNotMatch(plan, /SCAN operations_work\b/u);
    raw.close();
  });

  await t.test('OPS07.C04 stop reports incomplete durable cleanup while a second SQLite writer lock spans child reap', async t => {
    const busyStop = tempDirectory('ah11-busy-stop-recovery-');
    let diagnosticSession;
    let worker;
    let restart;
    let raw;
    let recovered;
    let writerLock;
    let accepted;
    let c04Phase = 'worker_start';
    let c04PrimaryFailed = false;
    let c04PrimaryError;
    let c04LastPage;
    let c04PreCleanup = observeC04PreCleanup(null);
    const c04CleanupErrors = [];
    let c04Row = readC04FinalRow(null, null);
    t.after(async () => {
      const { cleanupComplete, capture, captureTiming, cleanupErrors } = await cleanupC04Fixture({
        workers: [worker, restart], handles: [writerLock, raw, recovered], errors: c04CleanupErrors,
        // The two-job final-state oracle is inapplicable to this one-job restart.
        readCapture: diagnosticSession ? () => readOperationsDiagnosticSession(diagnosticSession, { finalState: { available: false } }) : null,
        disposeCapture: diagnosticSession ? () => disposeOperationsDiagnosticSession(diagnosticSession) : null,
        removeDirectory: () => busyStop.cleanup()
      });
      try {
        const diagnostic = encodeC04CompletionDiagnostic({ phase: c04Phase, row: c04Row, preCleanup: c04PreCleanup,
          capture, captureTiming, primaryFailed: c04PrimaryFailed, cleanupComplete });
        if (diagnostic === null) throw new Error('C04 diagnostic exceeded its fixed publication bound.');
        t.diagnostic(diagnostic);
      } catch (error) { cleanupErrors.push(error); }
      throwC04CleanupErrors({ primaryFailed: c04PrimaryFailed, primaryError: c04PrimaryError, cleanupErrors });
    });
    try {
    diagnosticSession = createOperationsDiagnosticSession({ directory: busyStop.directory });
    const shutdownConfig = {
      workerCount: 1, maxInFlightGlobal: 1, maxInFlightTenant: 1, maxInFlightProject: 1,
      shutdownTimeoutMs: 1_000, childKillGraceMs: 100, childReapReserveMs: 200,
      busyTimeoutMs: 20, pollIntervalMs: 5
    };
    worker = createOperationsWorker({
      databasePath: busyStop.databasePath, ownerId: 'busy-stop-worker', config: shutdownConfig, diagnosticSession
    });
    void worker.start();
    const scope = { tenantId: 'busy-stop-tenant', projectId: 'busy-stop-project' };
    const planJson = JSON.stringify({
      kind: 'agent-harness.private-synthetic-plan', version: 1,
      steps: Array.from({ length: 32 }, (_, index) => ({
        id: `b${String(index).padStart(2, '0')}`, operation: 'text.concat',
        dependsOn: index % 12 === 0 ? [] : [`b${String(index - 1).padStart(2, '0')}`],
        args: { parts: index % 12 === 0 ? ['x'.repeat(512)] : [{ $result: `b${String(index - 1).padStart(2, '0')}` }, ''] }
      }))
    });
    accepted = worker.submitSyntheticWork({
      trustedSyntheticScope: scope, idempotencyKey: 'busy-stop-held-work', planJson
    });
    assert.equal(accepted.accepted, true);
    raw = new DatabaseSync(busyStop.databasePath, { readOnly: true });
    c04Phase = 'runner_wait';
    const runnerDeadline = Date.now() + 3_000;
    let runnerPid = null;
    while (Date.now() < runnerDeadline && runnerPid === null) {
      const row = raw.prepare('SELECT state, runner_pid FROM operations_work WHERE job_id=?').get(accepted.jobId);
      if (row?.state === 'running' && Number.isSafeInteger(row.runner_pid)) runnerPid = row.runner_pid;
      else await new Promise(resolvePromise => setTimeout(resolvePromise, 1));
    }
    assert.ok(Number.isSafeInteger(runnerPid) && runnerPid > 1);
    assert.equal(process.kill(runnerPid, 'SIGSTOP'), true);
    writerLock = new DatabaseSync(busyStop.databasePath, { timeout: 20 });
    let writerLockHeld = false;
    let heldStopFailed = false;
    let heldStopError;
    let firstStop;
    try {
      writerLock.exec('BEGIN IMMEDIATE');
      writerLockHeld = true;
      c04Phase = 'held_stop';
      firstStop = await worker.stop('test');
      assert.equal(firstStop.stopped, false);
      assert.equal(firstStop.childrenReaped, true);
      assert.equal(firstStop.durableCleanupComplete, false);
      assert.equal(firstStop.pendingDurableCleanupCount, 1);
      assert.equal(firstStop.storeClosed, false);
      assert.ok(firstStop.elapsedMs <= firstStop.budgetMs + 100);
      const retained = raw.prepare(`SELECT state, runner_pid FROM operations_work WHERE job_id=?`).get(accepted.jobId);
      assert.equal(retained.state, 'running');
      assert.equal(retained.runner_pid, runnerPid);
      assert.throws(() => process.kill(runnerPid, 0), error => error.code === 'ESRCH');
      assert.equal(Number(raw.prepare('SELECT in_flight FROM operations_meta WHERE singleton=1').get().in_flight), 1);
    } catch (error) { heldStopFailed = true; heldStopError = error; }
    finally {
      if (writerLockHeld) {
        try { writerLock.exec('ROLLBACK'); } catch (error) { c04CleanupErrors.push(error); }
      }
      try { writerLock.close(); writerLock = null; } catch (error) { c04CleanupErrors.push(error); }
    }
    if (heldStopFailed) throw heldStopError;
    if (c04CleanupErrors.length) throw new AggregateError(c04CleanupErrors, 'C04 held writer cleanup failed.');

    c04Phase = 'recovered_stop';
    const recoveredStop = await worker.stop('test');
    assert.equal(recoveredStop.stopped, true);
    assert.equal(recoveredStop.durableCleanupComplete, true);
    assert.equal(recoveredStop.pendingDurableCleanupCount, 0);
    assert.equal(recoveredStop.storeClosed, true);
    recovered = openStore(busyStop.databasePath);
    const queuedRow = recovered._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: scope.tenantId, projectId: scope.projectId, jobId: accepted.jobId
    });
    assert.equal(queuedRow.state, 'queued');
    assert.equal(raw.prepare('SELECT runner_pid FROM operations_work WHERE job_id=?').get(accepted.jobId).runner_pid, null);
    assert.equal(Number(raw.prepare('SELECT in_flight FROM operations_meta WHERE singleton=1').get().in_flight), 0);
    recovered.close(); recovered = null;

    c04Phase = 'restart_start';
    restart = createOperationsWorker({
      databasePath: busyStop.databasePath, ownerId: 'busy-stop-restart', config: shutdownConfig, diagnosticSession
    });
    void restart.start();
    c04Phase = 'restart_wait';
    try {
      const completed = await waitForStatus(restart, scope, accepted.jobId, 'completed', 8_000,
        item => { c04LastPage = projectC04WorkPage(item); });
      assert.equal(completed.state, 'completed');
    } finally {
      c04PreCleanup = observeC04PreCleanup({ observed: true, page: c04LastPage, role: 'restart',
        lifecycle: restart.lifecycle, activeChildCount: restart.activeChildCount });
      c04Row = readC04FinalRow(raw, accepted.jobId);
    }
    c04Phase = 'restart_stop';
    const restartStop = await restart.stop('test');
    assert.equal(restartStop.stopped, true);
    raw.close(); raw = null;
    c04Phase = 'done';
    } catch (error) {
      c04PrimaryFailed = true; c04PrimaryError = error;
      if (c04PreCleanup.timing === 'not_observed') {
        const observedWorker = restart ?? worker;
        c04PreCleanup = observeC04PreCleanup({ observed: true, page: c04LastPage,
          role: restart ? 'restart' : worker ? 'initial' : 'unobserved',
          lifecycle: observedWorker?.lifecycle, activeChildCount: observedWorker?.activeChildCount });
      }
      if (c04Row.status === 'not_observed') c04Row = readC04FinalRow(raw, accepted?.jobId);
      throw error;
    }
  });
  await t.test('OPS07.C05 the CLI stays open after an incomplete stop and succeeds after a second signal', async t => {
    const cliStop = tempDirectory('ah11-cli-incomplete-stop-');
    t.after(() => cliStop.cleanup());
    const scope = { tenantId: 'cli-busy-tenant', projectId: 'cli-busy-project' };
    const setup = openStore(cliStop.databasePath);
    const planJson = JSON.stringify({
      kind: 'agent-harness.private-synthetic-plan', version: 1,
      steps: Array.from({ length: 32 }, (_, index) => ({
        id: `c${String(index).padStart(2, '0')}`, operation: 'text.concat',
        dependsOn: index % 12 === 0 ? [] : [`c${String(index - 1).padStart(2, '0')}`],
        args: { parts: index % 12 === 0 ? ['x'.repeat(512)] : [{ $result: `c${String(index - 1).padStart(2, '0')}` }, ''] }
      }))
    });
    const accepted = admit(setup, scope.tenantId, scope.projectId, 'cli-busy-work', planJson);
    assert.equal(accepted.accepted, true);
    setup.close();

    const child = spawn(process.execPath, [
      join(ROOT, 'src/worker/private/worker.js'), '--mode', 'mock', '--database-path', cliStop.databasePath
    ], { cwd: ROOT, env: baseEnvironment(), stdio: ['ignore', 'pipe', 'pipe'] });
    METRICS.processRuns += 1;
    let stdout = '';
    let stderr = '';
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', chunk => { stdout += chunk; });
    child.stderr.on('data', chunk => { stderr += chunk; });
    let closeResult = null;
    const closed = new Promise(resolvePromise => child.once('close', (code, signal) => {
      closeResult = { code, signal };
      resolvePromise(closeResult);
    }));
    const raw = new DatabaseSync(cliStop.databasePath, { readOnly: true });
    t.after(() => { try { raw.close(); } catch {} });
    let runnerPid = null;
    let writerLock = null;
    try {
      const runnerDeadline = Date.now() + 4_000;
      while (Date.now() < runnerDeadline && runnerPid === null) {
        const row = raw.prepare('SELECT state, runner_pid FROM operations_work WHERE job_id=?').get(accepted.jobId);
        if (row?.state === 'running' && Number.isSafeInteger(row.runner_pid)) runnerPid = row.runner_pid;
        else await new Promise(resolvePromise => setTimeout(resolvePromise, 2));
      }
      assert.ok(Number.isSafeInteger(runnerPid) && runnerPid > 1, stderr);
      assert.equal(process.kill(runnerPid, 'SIGSTOP'), true);
      writerLock = new DatabaseSync(cliStop.databasePath, { timeout: 20 });
      writerLock.exec('BEGIN IMMEDIATE');
      assert.equal(child.kill('SIGTERM'), true);
      const incompleteDeadline = Date.now() + 10_000;
      while (!stdout.split(/\r?\n/u).some(line => {
        try { return JSON.parse(line).event === 'operations_worker_stop_incomplete'; } catch { return false; }
      }) && Date.now() < incompleteDeadline && child.exitCode === null) {
        await new Promise(resolvePromise => setTimeout(resolvePromise, 10));
      }
      const incomplete = stdout.split(/\r?\n/u).map(line => {
        try { return JSON.parse(line); } catch { return null; }
      }).find(record => record?.event === 'operations_worker_stop_incomplete');
      assert.ok(incomplete, `${stdout}\n${stderr}`);
      assert.equal(incomplete.stopped, false);
      assert.equal(incomplete.storeClosed, false);
      assert.equal(child.exitCode, null, 'CLI remains alive while its store and retry signal handler are needed');
      const retained = raw.prepare('SELECT state, runner_pid FROM operations_work WHERE job_id=?').get(accepted.jobId);
      assert.equal(retained.state, 'running');
      assert.equal(retained.runner_pid, runnerPid);

      writerLock.exec('ROLLBACK');
      writerLock.close();
      writerLock = null;
      assert.equal(child.kill('SIGTERM'), true);
      const finalClose = await waitForCloseBounded(closed, 3_000);
      assert.ok(finalClose, `${stdout}\n${stderr}`);
      assert.deepEqual(finalClose, { code: 0, signal: null });
      const records = stdout.split(/\r?\n/u).map(line => {
        try { return JSON.parse(line); } catch { return null; }
      }).filter(Boolean);
      assert.equal(records.filter(record => record.event === 'operations_worker_stop_incomplete').length, 1);
      const finalRecord = records.find(record => record.event === 'operations_worker_stopped');
      assert.equal(finalRecord?.stopped, true);
      assert.equal(finalRecord?.storeClosed, true);
      const settled = raw.prepare('SELECT state, runner_pid FROM operations_work WHERE job_id=?').get(accepted.jobId);
      assert.equal(settled.state, 'queued');
      assert.equal(settled.runner_pid, null);
      assert.equal(Number(raw.prepare('SELECT in_flight FROM operations_meta WHERE singleton=1').get().in_flight), 0);
    } finally {
      try { writerLock?.exec('ROLLBACK'); } catch {}
      try { writerLock?.close(); } catch {}
      if (runnerPid !== null) {
        try { process.kill(runnerPid, 'SIGCONT'); } catch {}
      }
      if (!closeResult && child.exitCode === null && child.signalCode === null) {
        const reaped = await killOwnedChildAndReap(child, closed);
        assert.ok(reaped, 'the test reaped its owned CLI child');
      }
    }
  });
});

test('OPS08_HEALTH_FAIL_CLOSED reports unavailable authority, provider, stale data, and secret-free status', async t => {
  const temporary = tempDirectory('ah11-health-');
  t.after(() => temporary.cleanup());
  const worker = createOperationsWorker({ databasePath: temporary.databasePath, ownerId: 'ops-worker:health' });
  t.after(async () => { await worker.stop('test'); });
  const canaryTenant = 'tenant-secret-canary-never-health';
  const canaryProject = 'project-private-canary-never-health';
  void worker.start();
  const canaryJob = worker.submitSyntheticWork({
    trustedSyntheticScope: { tenantId: canaryTenant, projectId: canaryProject },
    idempotencyKey: 'health-canary', planJson: PLAN
  });
  assert.equal(canaryJob.accepted, true);
  const before = worker.health();
  assert.equal(before.ready, false);
  assert.equal(before.externalReady, false);
  assert.equal(before.externalDispatchEnabled, false);
  assert.doesNotMatch(JSON.stringify(before), /tenant-secret-canary|project-private-canary|workflow.sqlite/u);
  const readyLocal = worker.health();
  METRICS.maxSupervisorRssBytes = Math.max(METRICS.maxSupervisorRssBytes, readyLocal.process.rssBytes);
  assert.equal(readyLocal.externalReady, false);
  const directStorePath = join(temporary.directory, 'health-direct.sqlite');
  const directStore = openStore(directStorePath);
  t.after(() => directStore.close());
  const direct = readOperationsHealth({ store: directStore, config: {}, lifecycle: 'accepting', heartbeatAgeMs: 10_000 });
  assert.ok(direct.degradedReasons.includes('DIAGNOSTICS_STALE'));
  assert.equal(direct.externalDispatchEnabled, false);
  const nonexistent = readStoreCompatibility(join(temporary.directory, 'missing.sqlite'));
  assert.equal(nonexistent.status, 'store_unavailable');

  await t.test('OPS08.C01 health truth table reports lifecycle, stale data, unavailable authority/provider, lease, unknown, and telemetry states', () => {
    const starting = readOperationsHealth({ store: directStore, config: {}, lifecycle: 'starting', heartbeatAgeMs: 10 });
    assert.equal(starting.state, 'starting');
    assert.equal(starting.live, false);
    assert.equal(starting.localReady, false);
    const acceptingStale = readOperationsHealth({ store: directStore, config: {}, lifecycle: 'accepting', heartbeatAgeMs: 10_000 });
    assert.equal(acceptingStale.state, 'degraded');
    assert.ok(acceptingStale.degradedReasons.includes('DIAGNOSTICS_STALE'));
    assert.ok(acceptingStale.degradedReasons.includes('POLICY_UNCONFIGURED'));
    const stopping = readOperationsHealth({ store: directStore, config: {}, lifecycle: 'stopping', heartbeatAgeMs: 10 });
    assert.equal(stopping.state, 'stopping');
    assert.ok(stopping.degradedReasons.includes('SHUTTING_DOWN'));
    const telemetryBad = readOperationsHealth({
      store: directStore, config: {}, lifecycle: 'accepting', heartbeatAgeMs: 10,
      telemetrySinkState: 'unavailable', telemetrySinkFailed: true, telemetrySinkPendingRecords: 2
    });
    assert.equal(telemetryBad.localReady, false);
    assert.ok(telemetryBad.degradedReasons.includes('TELEMETRY_SINK_UNAVAILABLE'));
    assert.ok(telemetryBad.degradedReasons.includes('TELEMETRY_SINK_ERROR'));
    assert.equal(telemetryBad.authority.status, 'authority_unavailable');
    assert.equal(telemetryBad.provider.status, 'provider_unavailable');
    assert.equal(telemetryBad.externalReady, false);
    assert.equal(telemetryBad.externalDispatchEnabled, false);
    const stoppedHealth = readOperationsHealth({ store: directStore, config: {}, lifecycle: 'stopped', heartbeatAgeMs: 10 });
    assert.equal(stoppedHealth.state, 'stopped');
    assert.equal(stoppedHealth.localReady, false);

    const futureVersionDb = new DatabaseSync(directStorePath);
    futureVersionDb.exec('PRAGMA user_version=99');
    futureVersionDb.close();
    const futureSchema = readOperationsHealth({ store: directStore, config: {}, lifecycle: 'accepting', heartbeatAgeMs: 10 });
    assert.ok(futureSchema.degradedReasons.includes('SCHEMA_UNSUPPORTED'));
    assert.equal(futureSchema.localReady, false);
    const restoreSchemaDb = new DatabaseSync(directStorePath);
    restoreSchemaDb.exec('PRAGMA user_version=6');
    restoreSchemaDb.close();

    let now = 50_000;
    const leaseDir = tempDirectory('ah11-health-lease-state-');
    t.after(() => leaseDir.cleanup());
    const leaseStore = openStore(leaseDir.databasePath, { clock: () => now });
    t.after(() => leaseStore.close());
    const accepted = admit(leaseStore, 'health-tenant', 'health-project', 'health-stale-lease');
    const policy = operationsStoragePolicy(readOperationsConfig());
    const claim = leaseStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, { ownerId: 'review-worker-health', policy }).claim;
    leaseStore._operationsSetRunnerPid(OPERATIONS_STORAGE_CAPABILITY, {
      jobId: claim.jobId, ownerId: claim.ownerId, generation: claim.generation,
      fence: claim.fence, runnerPid: unusedProcessId()
    });
    leaseStore.recordDispatchIntent({
      workflowId: claim.workflowId, ownerId: claim.ownerId, fence: claim.fence,
      expectedRevision: claim.revision, attemptId: 'health-unknown-attempt',
      logicalOperationRef: 'health-unknown-logical', authorityHoldRef: 'synthetic-local-bookkeeping:health-hold',
      transportAttemptId: 'health-unknown-transport', eventId: 'health-unknown-intent'
    });
    now = claim.leaseExpiresAtMs + 1;
    const leaseHealth = readOperationsHealth({ store: leaseStore, config: {}, lifecycle: 'accepting', heartbeatAgeMs: 10 });
    assert.ok(leaseHealth.degradedReasons.includes('LEASE_STALE'));
    const unknownHealth = readOperationsHealth({ store: leaseStore, config: {}, lifecycle: 'accepting', heartbeatAgeMs: 10 });
    assert.ok(unknownHealth.degradedReasons.includes('RECONCILIATION_PENDING'));
    assert.equal(unknownHealth.reconciliation.count, 1);
    assert.equal(unknownHealth.externalDispatchEnabled, false);
    assert.equal(leaseStore.getWorkflow(accepted.workflowId).state, 'awaiting_response');
  });

  await t.test('OPS08.C02 saturated event reservations match admission and stop before any accepted row', () => {
    const eventDir = tempDirectory('ah11-event-headroom-');
    t.after(() => eventDir.cleanup());
    const config = readOperationsConfig({
      maxQueueRecords: 2, maxPerTenantRecords: 1, maxPerProjectRecords: 1,
      maxQueueBytes: 65_536, maxPerTenantBytes: 32_768,
      maxPerProjectBytes: 16_384, maxEventBacklog: 1_024
    });
    const seed = DurableWorkflowStore.open({ databasePath: eventDir.databasePath, busyTimeoutMs: 20 });
    for (let index = 0; index < 1_024; index += 1) {
      seed.createWorkflow({ workflowId: `event-headroom-${index}`, checkpoint: { index }, eventId: `event-headroom-${index}` });
    }
    seed.close();
    const eventWorker = createOperationsWorker({ databasePath: eventDir.databasePath, ownerId: 'review-worker-event-cap', config });
    t.after(async () => { if (eventWorker.lifecycle !== 'stopped') await eventWorker.stop('test'); });
    void eventWorker.start();
    const health = eventWorker.health();
    assert.equal(health.localReady, false);
    assert.ok(health.degradedReasons.includes('TELEMETRY_BACKLOG_HIGH'));
    const rejected = eventWorker.submitSyntheticWork({
      trustedSyntheticScope: { tenantId: 'event-cap-tenant', projectId: 'event-cap-project' },
      idempotencyKey: 'event-cap-reject', planJson: PLAN
    });
    assert.equal(rejected.accepted, false);
    assert.equal(rejected.reasonCode, 'TELEMETRY_BACKLOG_HIGH');
    assert.equal(eventWorker.readScopedWorkPage({ trustedSyntheticScope: { tenantId: 'event-cap-tenant', projectId: 'event-cap-project' } }).items.length, 0);
    const raw = new DatabaseSync(eventDir.databasePath, { readOnly: true });
    assert.equal(Number(raw.prepare('SELECT COUNT(*) AS count FROM operations_work').get().count), 0);
    assert.equal(Number(raw.prepare('SELECT COUNT(*) AS count FROM workflow_events').get().count), 1_024);
    raw.close();
    const stopped = eventWorker.stop('test');
    return stopped.then(result => assert.equal(result.stopped, true));
  });

  await t.test('OPS08.C03 actual concurrency saturation is reported separately from minimum-admission readiness', async t => {
    const saturatedDir = tempDirectory('ah11-health-saturated-worker-');
    t.after(() => saturatedDir.cleanup());
    const saturatedWorker = createOperationsWorker({
      databasePath: saturatedDir.databasePath, ownerId: 'review-worker-saturated-health',
      config: { workerCount: 1, maxInFlightGlobal: 1, maxInFlightTenant: 1,
        maxInFlightProject: 1, pollIntervalMs: 10 }
    });
    t.after(async () => { if (saturatedWorker.lifecycle !== 'stopped') await saturatedWorker.stop('test'); });
    void saturatedWorker.start();
    const scope = { tenantId: 'saturated-health-tenant', projectId: 'saturated-health-project' };
    const planJson = JSON.stringify({
      kind: 'agent-harness.private-synthetic-plan', version: 1,
      steps: Array.from({ length: 32 }, (_, index) => ({
        id: `z${String(index).padStart(2, '0')}`, operation: 'text.concat',
        dependsOn: index % 12 === 0 ? [] : [`z${String(index - 1).padStart(2, '0')}`],
        args: { parts: index % 12 === 0 ? ['x'.repeat(512)] : [{ $result: `z${String(index - 1).padStart(2, '0')}` }, ''] }
      }))
    });
    const accepted = saturatedWorker.submitSyntheticWork({ trustedSyntheticScope: scope, idempotencyKey: 'health-saturated', planJson });
    assert.equal(accepted.accepted, true);
    const deadline = Date.now() + 3_000;
    while (Date.now() < deadline && saturatedWorker.activeChildCount === 0) await new Promise(resolvePromise => setTimeout(resolvePromise, 2));
    assert.equal(saturatedWorker.activeChildCount, 1);
    const children = spawnSync('pgrep', ['-P', String(process.pid)], { encoding: 'utf8' });
    assert.equal(children.status, 0, children.stderr);
    const runnerPid = Number(children.stdout.trim().split(/\s+/u).at(-1));
    assert.ok(Number.isSafeInteger(runnerPid) && runnerPid > 1);
    assert.equal(process.kill(runnerPid, 'SIGSTOP'), true);
    const saturatedHealthDeadline = Date.now() + 1_000;
    let health = saturatedWorker.health();
    while (Date.now() < saturatedHealthDeadline && health.state !== 'saturated') {
      await new Promise(resolvePromise => setTimeout(resolvePromise, 2));
      health = saturatedWorker.health();
    }
    assert.equal(health.state, 'saturated');
    assert.equal(health.localReady, true, 'The local queue can still admit a minimum bounded job while all child slots are occupied.');
    assert.equal(health.externalDispatchEnabled, false);
    const stopped = await saturatedWorker.stop('test');
    assert.equal(stopped.stopped, true);
    assert.equal(stopped.childrenReaped, true);
  });
  const stopped = await worker.stop('test');
  assert.equal(stopped.stopped, true);
  assert.equal(canaryTenant.includes('tenant-secret'), true);
});

test('OPS09_MIGRATION_COMPATIBILITY migrates schema 5 atomically, preserves older migration cases, and refuses future bytes', async t => {
  assert.equal(INTERNAL_SCHEMA_VERSION, 6);
  const temporary = tempDirectory('ah11-migration-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath);
  store.createWorkflow({ workflowId: 'preserve-schema5-workflow', checkpoint: { value: Number.MAX_VALUE, marker: 'opaque-preserved' }, eventId: 'preserve-created' });
  store.close();
  const old = new DatabaseSync(temporary.databasePath);
  old.exec(`
    PRAGMA foreign_keys = OFF;
    DROP TRIGGER operations_event_reservation;
    DROP TRIGGER operations_work_payload_immutable;
    DROP INDEX operations_attempts_workflow_state_idx;
    DROP INDEX operations_attempts_state_workflow_idx;
    DROP INDEX operations_work_scope_page_idx;
    DROP INDEX operations_work_lease_idx;
    DROP TABLE operations_work;
    DROP TABLE operations_projects;
    DROP TABLE operations_tenants;
    DROP TABLE operations_meta;
    PRAGMA user_version = 5;
  `);
  old.close();
  assert.equal(readStoreCompatibility(temporary.databasePath).status, 'migration_required');
  const migrated = openStore(temporary.databasePath);
  assert.deepEqual(migrated.getCheckpoint('preserve-schema5-workflow').data, { value: Number.MAX_VALUE, marker: 'opaque-preserved' });
  migrated.close();
  const raw = new DatabaseSync(temporary.databasePath, { readOnly: true });
  assert.equal(raw.prepare('PRAGMA user_version').get().user_version, 6);
  assert.equal(Number(raw.prepare("SELECT COUNT(*) AS count FROM sqlite_master WHERE type='table' AND name='operations_work'").get().count), 1);
  raw.close();

  const rollbackPath = join(temporary.directory, 'rollback.sqlite');
  const newer = openStore(rollbackPath);
  newer.close();
  const conflicting = new DatabaseSync(rollbackPath);
  conflicting.exec(`
    PRAGMA foreign_keys = OFF;
    DROP TRIGGER operations_event_reservation;
    DROP TRIGGER operations_work_payload_immutable;
    DROP INDEX operations_attempts_workflow_state_idx;
    DROP INDEX operations_attempts_state_workflow_idx;
    DROP INDEX operations_work_scope_page_idx;
    DROP INDEX operations_work_lease_idx;
    DROP TABLE operations_work;
    DROP TABLE operations_projects;
    DROP TABLE operations_tenants;
    DROP TABLE operations_meta;
    CREATE TABLE operations_meta (sentinel TEXT NOT NULL);
    INSERT INTO operations_meta VALUES ('rollback-preserved');
    PRAGMA user_version = 5;
  `);
  const beforeObjects = conflicting.prepare('SELECT type, name, sql FROM sqlite_master ORDER BY type, name').all().map(row => ({ ...row }));
  conflicting.close();
  assert.throws(() => openStore(rollbackPath));
  const after = new DatabaseSync(rollbackPath, { readOnly: true });
  assert.equal(after.prepare('PRAGMA user_version').get().user_version, 5);
  assert.deepEqual(after.prepare('SELECT type, name, sql FROM sqlite_master ORDER BY type, name').all().map(row => ({ ...row })), beforeObjects);
  assert.equal(after.prepare('SELECT sentinel FROM operations_meta').get().sentinel, 'rollback-preserved');
  after.close();

  const futurePath = join(temporary.directory, 'future.sqlite');
  const future = new DatabaseSync(futurePath);
  future.exec("CREATE TABLE future_marker (value TEXT); INSERT INTO future_marker VALUES ('preserve'); PRAGMA user_version=99;");
  future.close();
  const bytes = readFileSync(futurePath);
  assert.equal(readStoreCompatibility(futurePath).status, 'schema_unsupported');
  assert.throws(() => openStore(futurePath), error => error.code === 'UNSUPPORTED_SCHEMA_VERSION');
  assert.deepEqual(readFileSync(futurePath), bytes);
  await t.test('OPS09.C01 maps the supported schema upgrade, rollback, future refusal, and retained envelope controls', () => {
    const currentPath = join(temporary.directory, 'current-policy-bound.sqlite');
    const currentStore = openStore(currentPath);
    const currentConfig = readOperationsConfig();
    try {
      currentStore._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY, {
        maxStoreBytes: currentConfig.maxStoreBytes,
        maxWalBytes: currentConfig.maxWalBytes,
        minFreeDiskBytes: currentConfig.minFreeDiskBytes,
        policy: operationsStoragePolicy(currentConfig)
      });
    } finally {
      currentStore.close();
    }
    const beforeCurrentOpen = new DatabaseSync(currentPath, { readOnly: true });
    let currentPolicy;
    let currentSchema;
    try {
      currentPolicy = beforeCurrentOpen.prepare(
        'SELECT policy_json FROM operations_meta WHERE singleton=1'
      ).get().policy_json;
      currentSchema = beforeCurrentOpen.prepare(
        "SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
      ).all().map(row => ({ ...row }));
      assert.equal(beforeCurrentOpen.prepare('PRAGMA user_version').get().user_version, 6);
      assert.equal(typeof currentPolicy, 'string');
    } finally {
      beforeCurrentOpen.close();
    }

    const heldWriter = new DatabaseSync(currentPath, { timeout: 20 });
    let openedWhileWriterHeld;
    try {
      heldWriter.exec('BEGIN IMMEDIATE');
      openedWhileWriterHeld = DurableWorkflowStore.open({ databasePath: currentPath, busyTimeoutMs: 20 });
      assert.ok(openedWhileWriterHeld);
      assert.equal(heldWriter.isTransaction, true);
    } finally {
      try { openedWhileWriterHeld?.close(); } catch {}
      try { if (heldWriter.isTransaction) heldWriter.exec('ROLLBACK'); } catch {}
      heldWriter.close();
    }
    const afterCurrentOpen = new DatabaseSync(currentPath, { readOnly: true });
    try {
      assert.equal(afterCurrentOpen.prepare('PRAGMA user_version').get().user_version, 6);
      assert.equal(afterCurrentOpen.prepare(
        'SELECT policy_json FROM operations_meta WHERE singleton=1'
      ).get().policy_json, currentPolicy);
      assert.deepEqual(afterCurrentOpen.prepare(
        "SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
      ).all().map(row => ({ ...row })), currentSchema);
    } finally {
      afterCurrentOpen.close();
    }

    assert.equal(readStoreCompatibility(temporary.databasePath).status, 'supported');
    assert.equal(readStoreCompatibility(rollbackPath).status, 'migration_required');
    assert.equal(readStoreCompatibility(futurePath).status, 'schema_unsupported');
    const unchanged = new DatabaseSync(temporary.databasePath, { readOnly: true });
    assert.equal(unchanged.prepare('PRAGMA user_version').get().user_version, 6);
    unchanged.close();
    assert.match(readFileSync(join(ROOT, 'tests/runtime/durability.test.js'), 'utf8'), /v1 checkpoint migration/u);
    assert.equal(INTERNAL_SCHEMA_VERSION, 6);
  });

  await t.test('OPS09.C02 migrates populated schema 6 without changing queued, running, unknown, or terminal custody', async t => {
    const retained = tempDirectory('ah11-v6-policy-binding-migration-');
    t.after(() => retained.cleanup());
    let now = 90_000;
    const source = DurableWorkflowStore.open({
      databasePath: retained.databasePath, busyTimeoutMs: 20, clock: () => now
    });
    source._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY, {
      maxStoreBytes: readOperationsConfig().maxStoreBytes,
      maxWalBytes: readOperationsConfig().maxWalBytes,
      minFreeDiskBytes: readOperationsConfig().minFreeDiskBytes
    });
    const defaultPolicy = operationsStoragePolicy(readOperationsConfig());

    const running = admit(source, 'migration-running-tenant', 'migration-running-project', 'migration-running');
    const runningClaim = source._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'migration-running-owner', policy: defaultPolicy
    }).claim;
    source._operationsSetRunnerPid(OPERATIONS_STORAGE_CAPABILITY, {
      jobId: running.jobId, ownerId: runningClaim.ownerId, generation: runningClaim.generation,
      fence: runningClaim.fence, runnerPid: unusedProcessId()
    });

    const unknown = admit(source, 'migration-unknown-tenant', 'migration-unknown-project', 'migration-unknown');
    const unknownClaim = source._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'migration-unknown-owner', policy: defaultPolicy
    }).claim;
    source._operationsSetRunnerPid(OPERATIONS_STORAGE_CAPABILITY, {
      jobId: unknown.jobId, ownerId: unknownClaim.ownerId, generation: unknownClaim.generation,
      fence: unknownClaim.fence, runnerPid: unusedProcessId()
    });
    source.recordDispatchIntent({
      workflowId: unknown.workflowId, ownerId: unknownClaim.ownerId, fence: unknownClaim.fence,
      expectedRevision: unknownClaim.revision, attemptId: 'migration-unknown-attempt',
      logicalOperationRef: 'migration-unknown-logical',
      authorityHoldRef: 'synthetic-local-bookkeeping:migration-unknown-hold',
      transportAttemptId: 'migration-unknown-transport', eventId: 'migration-unknown-intent'
    });
    now = unknownClaim.leaseExpiresAtMs + 1;
    const expired = source._operationsExpiredClaims(OPERATIONS_STORAGE_CAPABILITY, { policy: defaultPolicy });
    assert.ok(expired.items.some(item => item.jobId === unknown.jobId));
    assert.equal(source._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'migration-unknown-recovery', policy: defaultPolicy, recoverableJobIds: [unknown.jobId]
    }), null);

    const terminal = admit(source, 'migration-terminal-tenant', 'migration-terminal-project', 'migration-terminal');
    const terminalClaim = source._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'migration-terminal-owner', policy: defaultPolicy
    }).claim;
    const terminalWork = source._operationsReadClaim(OPERATIONS_STORAGE_CAPABILITY, {
      jobId: terminal.jobId, ownerId: terminalClaim.ownerId,
      generation: terminalClaim.generation, fence: terminalClaim.fence
    });
    const localResult = await runBoundedLocalPlan({
      store: source, workflowId: terminal.workflowId, ownerId: terminalClaim.ownerId,
      fence: terminalClaim.fence, planJson: terminalWork.work.planJson,
      limitsJson: terminalWork.work.limitsJson
    });
    assert.equal(localResult.status, 'completed');
    source._operationsFinish(OPERATIONS_STORAGE_CAPABILITY, {
      jobId: terminal.jobId, ownerId: terminalClaim.ownerId,
      generation: terminalClaim.generation, fence: terminalClaim.fence,
      outcome: 'completed', childExited: true
    });

    const queued = admit(source, 'migration-queued-tenant', 'migration-queued-project', 'migration-queued');
    source.close();

    const snapshot = databasePath => {
      const db = new DatabaseSync(databasePath, { readOnly: true });
      const rows = (sql, ...params) => db.prepare(sql).all(...params).map(row => ({ ...row }));
      const value = {
        meta: rows(`SELECT pending_records, pending_bytes, in_flight, reserved_event_slots,
          reserved_disk_bytes, last_tenant_id FROM operations_meta WHERE singleton=1`),
        tenants: rows('SELECT * FROM operations_tenants ORDER BY tenant_id'),
        projects: rows('SELECT * FROM operations_projects ORDER BY tenant_id, project_id'),
        work: rows(`SELECT job_id, workflow_id, tenant_id, project_id, idempotency_key, work_json,
          work_bytes, work_sha256, limits_version, state, generation, worker_id, runner_pid,
          fence_token, lease_expires_ms, created_at_ms, updated_at_ms, reserved_event_slots,
          reserved_disk_bytes, reason_code FROM operations_work ORDER BY job_id`),
        workflows: rows('SELECT * FROM workflows ORDER BY workflow_id'),
        checkpoints: rows('SELECT * FROM workflow_checkpoints ORDER BY workflow_id, sequence'),
        events: rows('SELECT * FROM workflow_events ORDER BY sequence'),
        outbox: rows('SELECT * FROM outbox ORDER BY event_sequence'),
        attempts: rows('SELECT * FROM operation_attempts ORDER BY attempt_id'),
        cursors: rows('SELECT * FROM telemetry_cursors ORDER BY consumer_id')
      };
      db.close();
      return JSON.parse(JSON.stringify(value));
    };
    const beforeMigration = snapshot(retained.databasePath);
    const legacy = new DatabaseSync(retained.databasePath);
    legacy.exec('ALTER TABLE operations_meta DROP COLUMN policy_json; PRAGMA user_version=6;');
    legacy.close();
    assert.equal(readStoreCompatibility(retained.databasePath).status, 'migration_required');

    const incompatible = DurableWorkflowStore.open({ databasePath: retained.databasePath, busyTimeoutMs: 20 });
    const narrowConfig = readOperationsConfig({
      maxQueueRecords: 2, maxPerTenantRecords: 1, maxPerProjectRecords: 1,
      maxQueueBytes: 65_536, maxPerTenantBytes: 32_768, maxPerProjectBytes: 16_384,
      maxTenants: 2, maxProjects: 2, maxInFlightGlobal: 1,
      maxInFlightTenant: 1, maxInFlightProject: 1
    });
    assert.throws(() => incompatible._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY, {
      maxStoreBytes: narrowConfig.maxStoreBytes, maxWalBytes: narrowConfig.maxWalBytes,
      minFreeDiskBytes: narrowConfig.minFreeDiskBytes,
      policy: operationsStoragePolicy(narrowConfig)
    }), error => error.code === 'INVALID_INPUT');
    incompatible.close();
    const unbound = new DatabaseSync(retained.databasePath, { readOnly: true });
    assert.equal(unbound.prepare('SELECT policy_json FROM operations_meta WHERE singleton=1').get().policy_json, null);
    unbound.close();

    const resumed = createOperationsWorker({
      databasePath: retained.databasePath, ownerId: 'migration-resumed-worker'
    });
    assert.equal(readStoreCompatibility(retained.databasePath).status, 'supported');
    const afterMigration = snapshot(retained.databasePath);
    const withoutReservationCounters = value => {
      const copy = JSON.parse(JSON.stringify(value));
      for (const row of copy.meta) {
        delete row.reserved_disk_bytes;
        delete row.reserved_event_slots;
      }
      for (const row of copy.work) {
        delete row.reserved_disk_bytes;
        delete row.reserved_event_slots;
      }
      return copy;
    };
    assert.deepEqual(withoutReservationCounters(afterMigration), withoutReservationCounters(beforeMigration));
    const rebound = new DatabaseSync(retained.databasePath, { readOnly: true });
    const reservationState = rebound.prepare(`
      SELECT m.reserved_disk_bytes AS meta_disk, m.reserved_event_slots AS meta_events,
        SUM(w.reserved_disk_bytes) AS work_disk, SUM(w.reserved_event_slots) AS work_events
      FROM operations_meta m CROSS JOIN operations_work w WHERE m.singleton=1
    `).get();
    assert.equal(Number(reservationState.meta_disk), Number(reservationState.work_disk));
    assert.equal(Number(reservationState.meta_events), Number(reservationState.work_events));
    rebound.close();
    const check = new DatabaseSync(retained.databasePath, { readOnly: true });
    assert.deepEqual(check.prepare('SELECT job_id, state, runner_pid FROM operations_work ORDER BY job_id').all().map(row => ({ ...row })), [
      { job_id: queued.jobId, state: 'queued', runner_pid: null },
      { job_id: running.jobId, state: 'running', runner_pid: check.prepare('SELECT runner_pid FROM operations_work WHERE job_id=?').get(running.jobId).runner_pid },
      { job_id: terminal.jobId, state: 'completed', runner_pid: null },
      { job_id: unknown.jobId, state: 'unknown', runner_pid: null }
    ].sort((left, right) => left.job_id.localeCompare(right.job_id)));
    assert.equal(check.prepare('SELECT state FROM operation_attempts WHERE attempt_id=?').get('migration-unknown-attempt').state, 'intent');
    assert.equal(check.prepare('SELECT authority_hold_ref FROM operation_attempts WHERE attempt_id=?').get('migration-unknown-attempt').authority_hold_ref,
      'synthetic-local-bookkeeping:migration-unknown-hold');
    check.close();
    const stopped = await resumed.stop('test');
    assert.equal(stopped.stopped, true);

    const rollbackPath = join(retained.directory, 'v6-rollback.sqlite');
    const rollbackSource = DurableWorkflowStore.open({ databasePath: rollbackPath, busyTimeoutMs: 20 });
    rollbackSource.close();
    const damaged = new DatabaseSync(rollbackPath);
    damaged.exec('PRAGMA foreign_keys=OFF; ALTER TABLE operations_meta DROP COLUMN policy_json; DROP TABLE operations_work;');
    const beforeRollback = damaged.prepare('SELECT type, name, sql FROM sqlite_master ORDER BY type, name').all().map(row => ({ ...row }));
    damaged.close();
    assert.throws(() => DurableWorkflowStore.open({ databasePath: rollbackPath, busyTimeoutMs: 20 }));
    const afterRollback = new DatabaseSync(rollbackPath, { readOnly: true });
    assert.equal(afterRollback.prepare('PRAGMA user_version').get().user_version, 6);
    assert.deepEqual(afterRollback.prepare('SELECT type, name, sql FROM sqlite_master ORDER BY type, name').all().map(row => ({ ...row })), beforeRollback);
    assert.equal(Number(afterRollback.prepare("SELECT COUNT(*) AS count FROM pragma_table_info('operations_meta') WHERE name='policy_json'").get().count), 0);
    afterRollback.close();
  });

  await t.test('OPS09.C03 upgrades actual predecessor reservations atomically and keeps blocked work recoverable', async t => {
    const fixtureDirectory = tempDirectory('ah11-v6-actual-reservation-migration-');
    t.after(() => fixtureDirectory.cleanup());
    const fixturePath = join(ROOT, 'tests/operations/fixtures/legacy-v6-populated-32-step.sqlite');
    const originalConfig = readOperationsConfig();
    const originalPolicy = operationsStoragePolicy(originalConfig);
    const fixtureManifest = JSON.parse(readFileSync(join(ROOT,
      'tests/operations/fixtures/legacy-v6-states/fixture-manifest.json'), 'utf8'));
    const fixtureEntry = fixtureManifest.fixtures.find(fixture => fixture.path ===
      'tests/operations/fixtures/legacy-v6-populated-32-step.sqlite');
    const fixtureBytes = readFileSync(fixturePath);
    assert.equal(fixtureBytes.byteLength, fixtureEntry?.bytes);
    assert.equal(createHash('sha256').update(fixtureBytes).digest('hex'), fixtureEntry?.sha256);
    const copyFixture = name => {
      const path = join(fixtureDirectory.directory, name);
      copyFileSync(fixturePath, path);
      return path;
    };
    const readRetained = path => {
      const db = new DatabaseSync(path, { readOnly: true });
      const policyColumn = Number(db.prepare("SELECT COUNT(*) AS count FROM pragma_table_info('operations_meta') WHERE name='policy_json'").get().count) === 1;
      const result = {
        version: Number(db.prepare('PRAGMA user_version').get().user_version),
        meta: { ...db.prepare(`SELECT pending_records, in_flight, reserved_disk_bytes, reserved_event_slots,
          ${policyColumn ? 'policy_json' : 'NULL AS policy_json'} FROM operations_meta WHERE singleton=1`).get() },
        work: { ...db.prepare('SELECT job_id, workflow_id, state, generation, reserved_disk_bytes, reserved_event_slots FROM operations_work').get() },
        workflow: { ...db.prepare('SELECT state, checkpoint_sequence, cancellation_requested, lease_owner, lease_expires_ms FROM workflows').get() }
      };
      db.close();
      return result;
    };
    const resourceInput = (config, policy) => ({
      maxStoreBytes: config.maxStoreBytes,
      maxWalBytes: config.maxWalBytes,
      minFreeDiskBytes: config.minFreeDiskBytes,
      policy
    });

    const completionPath = copyFixture('legacy-v6-completes.sqlite');
    const original = readRetained(completionPath);
    assert.equal(original.version, 6);
    assert.equal(original.meta.policy_json, null);
    assert.equal(Number(original.meta.reserved_disk_bytes), 524_288);
    assert.equal(original.work.state, 'queued');
    const completion = DurableWorkflowStore.open({ databasePath: completionPath, busyTimeoutMs: 20 });
    completion._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY, resourceInput(originalConfig, originalPolicy));
    const rebound = readRetained(completionPath);
    assert.equal(rebound.meta.policy_json !== null, true);
    assert.ok(Number(rebound.meta.reserved_disk_bytes) > Number(original.meta.reserved_disk_bytes));
    assert.equal(Number(rebound.meta.reserved_disk_bytes), Number(rebound.work.reserved_disk_bytes));
    assert.equal(rebound.work.state, 'queued');
    const claim = completion._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'legacy-v6-migration-worker', policy: originalPolicy
    }).claim;
    const work = completion._operationsReadClaim(OPERATIONS_STORAGE_CAPABILITY, {
      jobId: claim.jobId, ownerId: claim.ownerId, generation: claim.generation, fence: claim.fence
    });
    const localResult = await runBoundedLocalPlan({
      store: completion, workflowId: claim.workflowId, ownerId: claim.ownerId,
      fence: claim.fence, planJson: work.work.planJson, limitsJson: work.work.limitsJson
    });
    assert.equal(localResult.status, 'completed');
    assert.equal(localResult.results.length, 32);
    completion._operationsFinish(OPERATIONS_STORAGE_CAPABILITY, {
      jobId: claim.jobId, ownerId: claim.ownerId, generation: claim.generation,
      fence: claim.fence, outcome: 'completed', childExited: true
    });
    completion.close();
    const completed = DurableWorkflowStore.open({ databasePath: completionPath, busyTimeoutMs: 20 });
    const completedRow = completed._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'legacy-v6-fixture-tenant', projectId: 'legacy-v6-fixture-project',
      jobId: original.work.job_id
    });
    assert.equal(completedRow.state, 'completed');
    const completedDb = new DatabaseSync(completionPath, { readOnly: true });
    assert.equal(completedDb.prepare('SELECT state FROM workflows WHERE workflow_id=?').get(original.work.workflow_id).state, 'succeeded');
    assert.deepEqual({ ...completedDb.prepare('SELECT pending_records, in_flight, reserved_disk_bytes, reserved_event_slots FROM operations_meta WHERE singleton=1').get() }, {
      pending_records: 0, in_flight: 0, reserved_disk_bytes: 0, reserved_event_slots: 0
    });
    completedDb.close();
    completed.close();

    const capacityPath = copyFixture('legacy-v6-capacity-refusal.sqlite');
    const capacityBefore = readRetained(capacityPath);
    const capacityStore = DurableWorkflowStore.open({ databasePath: capacityPath, busyTimeoutMs: 20 });
    const narrowConfig = readOperationsConfig({ maxStoreBytes: 16_777_216 });
    const narrowPolicy = operationsStoragePolicy(narrowConfig);
    assert.throws(() => capacityStore._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY,
      resourceInput(narrowConfig, narrowPolicy)), error => error.code === 'INVALID_INPUT');
    const capacityAfter = readRetained(capacityPath);
    assert.equal(capacityAfter.meta.policy_json, null);
    assert.equal(Number(capacityAfter.meta.reserved_disk_bytes), Number(capacityBefore.meta.reserved_disk_bytes));
    assert.equal(Number(capacityAfter.work.reserved_disk_bytes), Number(capacityBefore.work.reserved_disk_bytes));
    assert.equal(capacityAfter.work.state, capacityBefore.work.state);
    assert.equal(capacityAfter.workflow.state, capacityBefore.workflow.state);
    capacityStore._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY,
      resourceInput(originalConfig, originalPolicy));
    assert.equal(readRetained(capacityPath).meta.policy_json !== null, true);
    capacityStore.close();

    const rollbackPath = copyFixture('legacy-v6-write-rollback.sqlite');
    const rollbackBefore = readRetained(rollbackPath);
    const rollbackStore = DurableWorkflowStore.open({ databasePath: rollbackPath, busyTimeoutMs: 20 });
    const injected = new DatabaseSync(rollbackPath);
    injected.exec(`CREATE TRIGGER injected_legacy_reservation_failure
      BEFORE UPDATE OF reserved_disk_bytes ON operations_meta
      WHEN NEW.reserved_disk_bytes > OLD.reserved_disk_bytes
      BEGIN SELECT RAISE(ABORT, 'synthetic retained-reservation write failure'); END;`);
    injected.close();
    assert.throws(() => rollbackStore._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY,
      resourceInput(originalConfig, originalPolicy)), error => error.code === 'STORE_WRITE_FAILED');
    const rollbackAfterFailure = readRetained(rollbackPath);
    assert.equal(rollbackAfterFailure.meta.policy_json, null);
    assert.equal(Number(rollbackAfterFailure.meta.reserved_disk_bytes), Number(rollbackBefore.meta.reserved_disk_bytes));
    assert.equal(Number(rollbackAfterFailure.work.reserved_disk_bytes), Number(rollbackBefore.work.reserved_disk_bytes));
    assert.equal(rollbackAfterFailure.work.state, rollbackBefore.work.state);
    assert.equal(rollbackAfterFailure.workflow.state, rollbackBefore.workflow.state);
    const removeInjection = new DatabaseSync(rollbackPath);
    removeInjection.exec('DROP TRIGGER injected_legacy_reservation_failure;');
    removeInjection.close();
    rollbackStore._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY,
      resourceInput(originalConfig, originalPolicy));
    assert.equal(readRetained(rollbackPath).meta.policy_json !== null, true);
    rollbackStore.close();
  });

  await t.test('OPS09.C04 real predecessor binaries bind custody and resume eligible expired running work', async t => {
    const fixtureDirectory = tempDirectory('ah11-v6-state-migration-');
    t.after(() => fixtureDirectory.cleanup());
    const config = readOperationsConfig();
    const policy = operationsStoragePolicy(config);
    const fixtures = new Map([
      ['queued', 'queued'], ['running', 'running'], ['partial-running', 'running'], ['partial', 'parked'],
      ['parked', 'parked'], ['unknown', 'unknown'], ['terminal', 'completed'], ['empty', null]
    ]);
    const fixtureManifest = JSON.parse(readFileSync(join(ROOT,
      'tests/operations/fixtures/legacy-v6-states/fixture-manifest.json'), 'utf8'));
    assert.equal(fixtureManifest.sourceRevision, 'af8ddbcd3543d8576c23bc43a6af5d1d4ae40544');
    assert.equal(fixtureManifest.sourceTree, 'e9f697d1c46b46b42920da2138421ee1e97f19b9');
    assert.equal(fixtureManifest.sourceFiles.find(file => file.path === 'src/internal/durable-store.js')?.sha256,
      'ee231508165fcd5038adce6c806797062edd0c46610094d09d641a7cefece611');
    assert.equal(fixtureManifest.sourceFiles.find(file => file.path === 'src/orchestrator/internal/controller.js')?.sha256,
      'c094162ab276c92532d9f7a7a294c47d03da3475a93f65c89e790253847e9c55');
    assert.equal(fixtureManifest.sourceFiles.find(file => file.path === 'src/worker/private/config.js')?.sha256,
      'aaec3c53ba02213564cbb543ed0615ece8389993d20ca9aa97157d55559114ae');
    const fixtureByPath = new Map(fixtureManifest.fixtures.map(fixture => [fixture.path, fixture]));
    const snapshot = databasePath => {
      const db = new DatabaseSync(databasePath, { readOnly: true });
      const rows = sql => db.prepare(sql).all().map(row => ({ ...row }));
      const hasPolicy = Number(db.prepare("SELECT COUNT(*) AS count FROM pragma_table_info('operations_meta') WHERE name='policy_json'").get().count) === 1;
      const result = {
        version: Number(db.prepare('PRAGMA user_version').get().user_version),
        meta: rows(`SELECT pending_records, pending_bytes, in_flight, reserved_event_slots,
          reserved_disk_bytes, last_tenant_id, ${hasPolicy ? 'policy_json' : 'NULL AS policy_json'}
          FROM operations_meta WHERE singleton=1`),
        tenants: rows('SELECT * FROM operations_tenants ORDER BY tenant_id'),
        projects: rows('SELECT * FROM operations_projects ORDER BY tenant_id, project_id'),
        work: rows('SELECT * FROM operations_work ORDER BY job_id'),
        workflows: rows('SELECT * FROM workflows ORDER BY workflow_id'),
        checkpoints: rows('SELECT * FROM workflow_checkpoints ORDER BY workflow_id, sequence'),
        events: rows('SELECT * FROM workflow_events ORDER BY sequence'),
        outbox: rows('SELECT * FROM outbox ORDER BY event_sequence'),
        attempts: rows('SELECT * FROM operation_attempts ORDER BY attempt_id'),
        cursors: rows('SELECT * FROM telemetry_cursors ORDER BY consumer_id')
      };
      db.close();
      return JSON.parse(JSON.stringify(result));
    };
    const retainedPayload = value => {
      const copy = JSON.parse(JSON.stringify(value));
      delete copy.meta[0]?.policy_json;
      for (const row of copy.meta) {
        delete row.reserved_disk_bytes;
        delete row.reserved_event_slots;
      }
      for (const row of copy.work) {
        delete row.reserved_disk_bytes;
        delete row.reserved_event_slots;
      }
      return copy;
    };

    for (const [fixtureName, expectedState] of fixtures) {
      const sourcePath = join(ROOT, 'tests/operations/fixtures/legacy-v6-states', `legacy-v6-${fixtureName}.sqlite`);
      const databasePath = join(fixtureDirectory.directory, `migrated-${fixtureName}.sqlite`);
      const relativeSourcePath = `tests/operations/fixtures/legacy-v6-states/legacy-v6-${fixtureName}.sqlite`;
      const fixtureEntry = fixtureByPath.get(relativeSourcePath);
      const fixtureBytes = readFileSync(sourcePath);
      assert.ok(fixtureEntry, fixtureName);
      assert.equal(fixtureBytes.byteLength, fixtureEntry.bytes, fixtureName);
      assert.equal(createHash('sha256').update(fixtureBytes).digest('hex'), fixtureEntry.sha256, fixtureName);
      copyFileSync(sourcePath, databasePath);
      assert.equal(readStoreCompatibility(databasePath).status, 'migration_required', fixtureName);
      const before = snapshot(databasePath);
      assert.equal(before.version, 6, fixtureName);
      assert.equal(before.meta[0].policy_json, null, fixtureName);
      assert.equal(before.work[0]?.state ?? null, expectedState, fixtureName);
      if (fixtureName === 'partial' || fixtureName === 'partial-running') {
        const checkpoint = before.checkpoints.filter(row => row.workflow_id === before.work[0].workflow_id)
          .sort((left, right) => Number(right.sequence) - Number(left.sequence))[0];
        assert.ok(checkpoint, 'predecessor partial checkpoint exists');
        const retainedCheckpoint = JSON.parse(checkpoint.payload_json);
        assert.equal(retainedCheckpoint.schemaVersion, 2);
        assert.equal(JSON.parse(retainedCheckpoint.data.completedResultsJson).one.output, 5);
        assert.equal(before.work[0].state, fixtureName === 'partial-running' ? 'running' : 'parked');
      }
      if (fixtureName === 'unknown') {
        assert.equal(before.attempts[0].state, 'intent');
        assert.equal(before.attempts[0].authority_hold_ref, 'synthetic-local-bookkeeping:legacy-unknown-hold');
      }
      if (fixtureName === 'running') assert.equal(before.work[0].runner_pid, 2_147_483_647);

      const store = DurableWorkflowStore.open({ databasePath, busyTimeoutMs: 20 });
      store._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY, {
        maxStoreBytes: config.maxStoreBytes, maxWalBytes: config.maxWalBytes,
        minFreeDiskBytes: config.minFreeDiskBytes, policy
      });
      const after = snapshot(databasePath);
      assert.equal(readStoreCompatibility(databasePath).status, 'supported', fixtureName);
      assert.equal(after.version, 6, fixtureName);
      assert.notEqual(after.meta[0].policy_json, null, fixtureName);
      assert.deepEqual(retainedPayload(after), retainedPayload(before), fixtureName);
      const activeRows = after.work.filter(row => ['queued', 'running', 'parked', 'unknown'].includes(row.state));
      assert.equal(Number(after.meta[0].reserved_disk_bytes), activeRows.reduce((sum, row) => sum + Number(row.reserved_disk_bytes), 0), fixtureName);
      assert.equal(Number(after.meta[0].reserved_event_slots), activeRows.reduce((sum, row) => sum + Number(row.reserved_event_slots), 0), fixtureName);
      if (activeRows.length > 0) {
        assert.ok(Number(after.meta[0].reserved_disk_bytes) >= Number(before.meta[0].reserved_disk_bytes), fixtureName);
        assert.ok(Number(after.meta[0].reserved_event_slots) >= Number(before.meta[0].reserved_event_slots), fixtureName);
      }
      if (fixtureName === 'unknown') {
        assert.equal(after.attempts[0].state, 'intent');
        assert.equal(after.attempts[0].authority_hold_ref, 'synthetic-local-bookkeeping:legacy-unknown-hold');
      }
      store.close();

      const reopened = DurableWorkflowStore.open({ databasePath, busyTimeoutMs: 20 });
      const afterReopen = snapshot(databasePath);
      assert.deepEqual(afterReopen, after, fixtureName);
      if (fixtureName === 'running') {
        reopened._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY, {
          maxStoreBytes: config.maxStoreBytes, maxWalBytes: config.maxWalBytes,
          minFreeDiskBytes: config.minFreeDiskBytes, policy
        });
        const expired = reopened._operationsExpiredClaims(OPERATIONS_STORAGE_CAPABILITY, { policy });
        assert.ok(expired.items.some(item => item.jobId === after.work[0].job_id));
        const recovered = reopened._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
          ownerId: 'legacy-v6-running-recovery', policy, recoverableJobIds: [after.work[0].job_id]
        });
        assert.equal(recovered.claim.jobId, after.work[0].job_id);
        const work = reopened._operationsReadClaim(OPERATIONS_STORAGE_CAPABILITY, {
          jobId: recovered.claim.jobId, ownerId: recovered.claim.ownerId,
          generation: recovered.claim.generation, fence: recovered.claim.fence
        });
        const localResult = await runBoundedLocalPlan({
          store: reopened, workflowId: recovered.claim.workflowId, ownerId: recovered.claim.ownerId,
          fence: recovered.claim.fence, planJson: work.work.planJson, limitsJson: work.work.limitsJson
        });
        assert.equal(localResult.status, 'completed');
        reopened._operationsFinish(OPERATIONS_STORAGE_CAPABILITY, {
          jobId: recovered.claim.jobId, ownerId: recovered.claim.ownerId,
          generation: recovered.claim.generation, fence: recovered.claim.fence,
          outcome: 'completed', childExited: true
        });
        const completed = snapshot(databasePath);
        assert.equal(completed.work[0].state, 'completed');
        assert.equal(completed.work[0].runner_pid, null);
        assert.equal(completed.work[0].reserved_disk_bytes, 0);
        assert.equal(completed.work[0].reserved_event_slots, 0);
        assert.equal(completed.work[0].generation, 2);
        assert.equal(completed.work[0].fence_token, 2);
        assert.equal(completed.work[0] && completed.work[0].workflow_id, after.work[0].workflow_id);
        assert.equal(Number(completed.meta[0].pending_records), 0);
        assert.equal(Number(completed.meta[0].in_flight), 0);
        assert.equal(Number(completed.meta[0].reserved_disk_bytes), 0);
        assert.equal(Number(completed.meta[0].reserved_event_slots), 0);
        reopened.close();
        const completedReopen = DurableWorkflowStore.open({ databasePath, busyTimeoutMs: 20 });
        assert.deepEqual(snapshot(databasePath), completed);
        completedReopen.close();
      } else {
        reopened.close();
      }
    }
  });

  await t.test('OPS09.C05 a later legacy-row reservation failure rolls back earlier row upgrades and retries cleanly', t => {
    const fixtureDirectory = tempDirectory('ah11-v6-multi-row-rollback-');
    t.after(() => fixtureDirectory.cleanup());
    const config = readOperationsConfig();
    const policy = operationsStoragePolicy(config);
    const databasePath = join(fixtureDirectory.directory, 'multi-row.sqlite');
    const fixturePath = join(ROOT, 'tests/operations/fixtures/legacy-v6-states/legacy-v6-multi.sqlite');
    const manifest = JSON.parse(readFileSync(join(ROOT,
      'tests/operations/fixtures/legacy-v6-states/fixture-manifest.json'), 'utf8'));
    const entry = manifest.fixtures.find(fixture => fixture.path === 'tests/operations/fixtures/legacy-v6-states/legacy-v6-multi.sqlite');
    const fixtureBytes = readFileSync(fixturePath);
    assert.equal(fixtureBytes.byteLength, entry?.bytes);
    assert.equal(createHash('sha256').update(fixtureBytes).digest('hex'), entry?.sha256);
    copyFileSync(fixturePath, databasePath);
    const snapshot = () => {
      const db = new DatabaseSync(databasePath, { readOnly: true });
      const rows = sql => db.prepare(sql).all().map(row => ({ ...row }));
      const hasPolicy = Number(db.prepare("SELECT COUNT(*) AS count FROM pragma_table_info('operations_meta') WHERE name='policy_json'").get().count) === 1;
      const result = {
        version: Number(db.prepare('PRAGMA user_version').get().user_version),
        meta: rows(`SELECT pending_records, pending_bytes, in_flight, reserved_event_slots,
          reserved_disk_bytes, last_tenant_id, ${hasPolicy ? 'policy_json' : 'NULL AS policy_json'}
          FROM operations_meta WHERE singleton=1`),
        tenants: rows('SELECT * FROM operations_tenants ORDER BY tenant_id'),
        projects: rows('SELECT * FROM operations_projects ORDER BY tenant_id, project_id'),
        work: rows(`SELECT job_id, state, generation, worker_id, runner_pid, fence_token,
          lease_expires_ms, reserved_event_slots, reserved_disk_bytes FROM operations_work ORDER BY job_id`),
        workflows: rows('SELECT * FROM workflows ORDER BY workflow_id'),
        checkpoints: rows('SELECT * FROM workflow_checkpoints ORDER BY workflow_id, sequence'),
        events: rows('SELECT * FROM workflow_events ORDER BY sequence'),
        outbox: rows('SELECT * FROM outbox ORDER BY event_sequence'),
        attempts: rows('SELECT * FROM operation_attempts ORDER BY attempt_id')
      };
      db.close();
      return JSON.parse(JSON.stringify(result));
    };
    const before = snapshot();
    assert.equal(before.version, 6);
    assert.equal(before.meta[0].policy_json, null);
    assert.equal(before.work.length, 2);
    assert.ok(before.work.every(row => row.state === 'queued' && Number(row.reserved_disk_bytes) === 524_288));
    const failingJobId = before.work[1].job_id;
    const store = DurableWorkflowStore.open({ databasePath, busyTimeoutMs: 20 });
    const injected = new DatabaseSync(databasePath);
    injected.exec(`CREATE TRIGGER injected_later_legacy_row_failure
      BEFORE UPDATE OF reserved_disk_bytes ON operations_work
      WHEN NEW.job_id = '${failingJobId}' AND NEW.reserved_disk_bytes > OLD.reserved_disk_bytes
      BEGIN SELECT RAISE(ABORT, 'synthetic second-row reservation failure'); END;`);
    injected.close();
    assert.throws(() => store._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY, {
      maxStoreBytes: config.maxStoreBytes, maxWalBytes: config.maxWalBytes,
      minFreeDiskBytes: config.minFreeDiskBytes, policy
    }), error => error.code === 'STORE_WRITE_FAILED');
    assert.deepEqual(snapshot(), before, 'a successful first-row increase and aggregate counters roll back together');

    const removeInjection = new DatabaseSync(databasePath);
    removeInjection.exec('DROP TRIGGER injected_later_legacy_row_failure;');
    removeInjection.close();
    store._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY, {
      maxStoreBytes: config.maxStoreBytes, maxWalBytes: config.maxWalBytes,
      minFreeDiskBytes: config.minFreeDiskBytes, policy
    });
    const rebound = snapshot();
    assert.notEqual(rebound.meta[0].policy_json, null);
    assert.ok(rebound.work.every(row => row.state === 'queued'
      && Number(row.reserved_disk_bytes) > 524_288 && Number(row.reserved_event_slots) >= 94));
    assert.equal(Number(rebound.meta[0].reserved_disk_bytes), rebound.work.reduce((sum, row) => sum + Number(row.reserved_disk_bytes), 0));
    assert.equal(Number(rebound.meta[0].reserved_event_slots), rebound.work.reduce((sum, row) => sum + Number(row.reserved_event_slots), 0));
    store.close();
    const reopened = DurableWorkflowStore.open({ databasePath, busyTimeoutMs: 20 });
    assert.deepEqual(snapshot(), rebound);
    reopened.close();
  });
});

test('OPS10_TELEMETRY_JEV_CUSTODY projects real worker events and retains cursor during a hung sink', async t => {
  const temporary = tempDirectory('ah11-telemetry-');
  t.after(() => temporary.cleanup());
  const worker = createOperationsWorker({ databasePath: temporary.databasePath, ownerId: 'ops-worker:telemetry' });
  t.after(async () => { await worker.stop('test'); });
  void worker.start();
  const accepted = worker.submitSyntheticWork({
    trustedSyntheticScope: { tenantId: 'telemetry-sensitive-tenant', projectId: 'telemetry-sensitive-project' },
    idempotencyKey: 'trace-worker-event', planJson: PLAN
  });
  assert.equal(accepted.accepted, true);
  const page = worker.readScopedWorkPage({ trustedSyntheticScope: { tenantId: 'telemetry-sensitive-tenant', projectId: 'telemetry-sensitive-project' }, limit: 10 });
  const event = new DatabaseSync(temporary.databasePath, { readOnly: true });
  const rawEvent = event.prepare("SELECT sequence, event_id, workflow_id, revision, event_type, from_state, to_state, fence_token, occurred_at_ms FROM workflow_events WHERE event_type = 'operations_work_admitted' ORDER BY sequence DESC LIMIT 1").get();
  event.close();
  const projected = projectTelemetryEvent({
    sequence: Number(rawEvent.sequence), eventId: rawEvent.event_id, workflowId: rawEvent.workflow_id,
    revision: Number(rawEvent.revision), type: rawEvent.event_type, fromState: rawEvent.from_state,
    toState: rawEvent.to_state, fence: Number(rawEvent.fence_token), occurredAtMs: Number(rawEvent.occurred_at_ms)
  }, (kind, value) => `tok1_${createHash('sha256').update(`${kind}:${value}`).digest('hex')}`);
  assert.equal(projected.eventType, 'operations_work_admitted');
  assert.doesNotMatch(JSON.stringify(projected), /telemetry-sensitive|ops-job:|ops-workflow:/u);
  assert.equal(page.items.length, 1);

  const store = openStore(join(temporary.directory, 'sink.sqlite'));
  t.after(() => store.close());
  const tokenizer = createHmacTokenizer(Buffer.alloc(32, 7));
  const exporter = createTelemetryExporter({
    store,
    tokenizer,
    sink: () => new Promise(() => {}),
    limits: { queueRecords: 4, queueBytes: 4096, batchSize: 4, maxAttempts: 1, retryDelayMs: 0, sinkTimeoutMs: 40, maxBatchesPerFlush: 1, maxPolicyDropsPerFlush: 1 }
  });
  assert.ok(exporter instanceof TelemetryExporter);
  const sinkWorker = createOperationsWorker({ databasePath: join(temporary.directory, 'sink.sqlite'), ownerId: 'ops-worker:sink', telemetry: exporter });
  t.after(async () => { await sinkWorker.stop('test'); });
  void sinkWorker.start();
  const sinkAccepted = sinkWorker.submitSyntheticWork({
    trustedSyntheticScope: { tenantId: 'sink-tenant', projectId: 'sink-project' }, idempotencyKey: 'sink-hung', planJson: PLAN
  });
  assert.equal(sinkAccepted.accepted, true);
  await waitForStatus(sinkWorker, { tenantId: 'sink-tenant', projectId: 'sink-project' }, sinkAccepted.jobId, 'completed');
  const stopResult = await sinkWorker.stop('signal');
  assert.equal(stopResult.stopped, true);
  assert.equal(stopResult.telemetryClosed, true);
  const closed = exporter.diagnostics();
  assert.equal(closed.cursor, 0);
  assert.ok(closed.pendingRecords > 0);
  assert.equal(closed.lastErrorCode, 'TELEMETRY_SINK_TIMEOUT');
  assert.equal(store.getTelemetryCursor('internal-telemetry-v1'), 0);
  await t.test('OPS10.C01 a hung sink leaves its cursor and pending outbox unchanged', () => {
    const diagnostics = exporter.diagnostics();
    assert.equal(diagnostics.cursor, 0);
    assert.ok(diagnostics.pendingRecords > 0);
    assert.equal(diagnostics.lastErrorCode, 'TELEMETRY_SINK_TIMEOUT');
    assert.equal(store.getTelemetryCursor('internal-telemetry-v1'), 0);
    assert.equal(sinkWorker.health().externalDispatchEnabled, false);
  });
  tokenizer.destroy();
  store.close();
  await worker.stop('test');
});

test('OPS11_RETENTION_OWNER_HOOKS refuses deletion or settlement without accepted external owner policy', async t => {
  const temporary = tempDirectory('ah11-retention-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath);
  t.after(() => store.close());
  store.createWorkflow({ workflowId: 'retention-immutable', checkpoint: { value: 'keep' }, eventId: 'retention-created' });
  const before = store.getCheckpoint('retention-immutable');
  const response = requestOwnerRetention(store, {
    ownerRole: 'Finance and reliability', policyVersion: 'policy-v1', recordClass: 'local-events', maximumItems: 10
  });
  assert.equal(response.accepted, false);
  assert.equal(response.status, 'retention_not_configured');
  assert.equal(response.deletionPerformed, false);
  assert.equal(response.settlementPerformed, false);
  assert.deepEqual(store.getCheckpoint('retention-immutable'), before);
  await t.test('OPS11.C01 absent owner policy preserves immutable data and does not settle a hold', () => {
    assert.equal(response.status, 'retention_not_configured');
    assert.equal(response.deletionPerformed, false);
    assert.equal(response.settlementPerformed, false);
    assert.deepEqual(store.getCheckpoint('retention-immutable'), before);
    assert.equal(store.getWorkflow('retention-immutable').state, 'queued');
  });

  await t.test('OPS11.C02 cursor gaps, undelivered evidence, backups, and unknown holds remain owner-controlled', () => {
    const custodyDir = tempDirectory('ah11-retention-custody-');
    t.after(() => custodyDir.cleanup());
    let now = 80_000;
    const custodyStore = openStore(custodyDir.databasePath, { clock: () => now });
    t.after(() => custodyStore.close());
    const accepted = admit(custodyStore, 'retention-tenant', 'retention-project', 'retention-unknown');
    const policy = operationsStoragePolicy(readOperationsConfig());
    const claim = custodyStore._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId: 'review-worker-retention-unknown', policy
    }).claim;
    custodyStore._operationsSetRunnerPid(OPERATIONS_STORAGE_CAPABILITY, {
      jobId: claim.jobId, ownerId: claim.ownerId, generation: claim.generation,
      fence: claim.fence, runnerPid: unusedProcessId()
    });
    custodyStore.recordDispatchIntent({
      workflowId: accepted.workflowId, ownerId: claim.ownerId, fence: claim.fence,
      expectedRevision: claim.revision, attemptId: 'retention-unknown-attempt',
      logicalOperationRef: 'retention-unknown-logical', authorityHoldRef: 'synthetic-local-bookkeeping:retention-hold',
      transportAttemptId: 'retention-unknown-transport', eventId: 'retention-unknown-intent'
    });
    now = claim.leaseExpiresAtMs + 1;
    custodyStore._operationsExpiredClaims(OPERATIONS_STORAGE_CAPABILITY, { policy });
    const cursorBefore = custodyStore.getTelemetryCursor('internal-telemetry-v1');
    const pendingBefore = custodyStore.countEventRowsAfter(cursorBefore, 1_000);
    const checkpointBefore = custodyStore.getCheckpoint(accepted.workflowId);
    const request = requestOwnerRetention(custodyStore, {
      ownerRole: 'Finance and reliability', policyVersion: 'unaccepted-v1',
      recordClass: 'unknown-attempts-and-events', maximumItems: 100
    });
    assert.equal(request.accepted, false);
    assert.equal(request.status, 'retention_not_configured');
    assert.equal(request.deletionPerformed, false);
    assert.equal(request.settlementPerformed, false);
    custodyStore.close();
    const reopened = openStore(custodyDir.databasePath);
    assert.equal(reopened.getTelemetryCursor('internal-telemetry-v1'), cursorBefore);
    assert.deepEqual(reopened.countEventRowsAfter(cursorBefore, 1_000), pendingBefore);
    assert.deepEqual(reopened.getCheckpoint(accepted.workflowId), checkpointBefore);
    assert.equal(reopened.getAttempt('retention-unknown-attempt').state, 'intent');
    assert.equal(reopened.getAttempt('retention-unknown-attempt').authorityHoldRef,
      'synthetic-local-bookkeeping:retention-hold');
    assert.equal(reopened.getWorkflow(accepted.workflowId).state, 'awaiting_response');
    const raw = new DatabaseSync(custodyDir.databasePath, { readOnly: true });
    assert.equal(Number(raw.prepare('SELECT COUNT(*) AS count FROM workflow_events WHERE workflow_id=?').get(accepted.workflowId).count), pendingBefore.count);
    raw.close();
    reopened.close();
  });
});

test('OPS12_RUNBOOK_CLEAN_START starts, health-checks, SIGTERMs, reopens, and restarts the documented worker', async t => {
  const temporary = tempDirectory('ah11-runbook-');
  t.after(() => temporary.cleanup());
  const workerScript = join(ROOT, 'src/worker/private/worker.js');
  for (let iteration = 0; iteration < 2; iteration += 1) {
    const child = spawn(process.execPath, [workerScript, '--mode', 'mock', '--database-path', temporary.databasePath], {
      cwd: ROOT, env: baseEnvironment(), stdio: ['ignore', 'pipe', 'pipe']
    });
    let output = '';
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', chunk => { output += chunk; });
    const started = new Promise((resolvePromise, reject) => {
      const timer = setTimeout(() => reject(new Error('Documented worker startup exceeded its bound.')), 5_000);
      const check = () => {
        if (output.includes('operations_worker_started')) { clearTimeout(timer); resolvePromise(); }
      };
      child.stdout.on('data', check);
      child.once('error', reject);
      check();
    });
    await started;
    const closed = new Promise(resolvePromise => child.once('close', (code, signal) => resolvePromise({ code, signal })));
    let processClosed = false;
    t.after(async () => {
      if (!processClosed) await killOwnedChildAndReap(child, closed);
    });
    const closeStartedAt = performance.now();
    assert.equal(child.kill('SIGTERM'), true);
    METRICS.processSigtermSignalsSent += 1;
    const exit = await closed;
    processClosed = true;
    const processCloseElapsedMs = performance.now() - closeStartedAt;
    METRICS.processCloseElapsedMsMax = Math.max(METRICS.processCloseElapsedMsMax, processCloseElapsedMs);
    METRICS.processCloseSamples += 1;
    METRICS.processRuns += 1;
    assert.equal(exit.code, 0);
    assert.match(output, /"event":"operations_worker_stopped"/u);
    assert.match(output, /"childrenReaped":true/u);
    assert.equal(readStoreCompatibility(temporary.databasePath).status, 'supported');
  }
  const store = openStore(temporary.databasePath);
  const health = readOperationsHealth({ store, config: {}, lifecycle: 'accepting', heartbeatAgeMs: 10 });
  assert.equal(health.externalDispatchEnabled, false);
  assert.equal(health.authority.status, 'authority_unavailable');
  assert.equal(health.provider.status, 'provider_unavailable');
  store.close();
  await t.test('OPS12.C01 executes busy, unavailable, future-schema, stuck-review, and unknown-hold recovery in a child process', async t => {
    const transcriptPath = join(temporary.directory, 'recovery-cases.jsonl');
    const result = await runChild(join(ROOT, 'tests/operations/runbook-recovery-cases.mjs'), [transcriptPath], { timeoutMs: 20_000 });
    recordOps12RecoveryDiagnostic(result, parseOps12RecoveryFailure(result.stderr));
    assert.equal(result.code, 0, result.stderr);
    assert.deepEqual(JSON.parse(result.stdout.trim()), { cases: 5, passed: 5, providerCalls: 0 });
    const records = readFileSync(transcriptPath, 'utf8').trim().split(/\r?\n/u).map(line => JSON.parse(line));
    assert.deepEqual(records.map(record => record.case), [
      'busy_store_spans_stop', 'store_unavailable', 'future_schema',
      'stuck_runner_review_escalation', 'unknown_hold_restart_escalation'
    ]);
    assert.ok(records.every(record => record.outcome === 'pass' && record.providerCalls === 0));
    assert.equal(records.find(record => record.case === 'future_schema').originalBytesPreserved, true);
    assert.equal(records.find(record => record.case === 'stuck_runner_review_escalation').childReaped, true);
    assert.equal(records.find(record => record.case === 'unknown_hold_restart_escalation').holdRetained, true);
  });
});

test('OPS13_EVIDENCE_GATES binds all required groups to the current source and reports no unrun provider success', async t => {
  const source = readFileSync(join(ROOT, 'src/worker/private/worker.js'), 'utf8');
  const fixture = readFileSync(join(ROOT, 'tests/operations/fixtures/jev-sigterm-child.mjs'), 'utf8');
  const evidenceVerifier = readFileSync(join(ROOT, 'tests/operations/verify-fixture.js'), 'utf8');
  assert.match(source, /externalDispatchEnabled: false/u);
  assert.match(source, /SIGKILL/u);
  assert.match(source, /_operationsFinish/u);
  assert.match(fixture, /runPrivateJevTestAttempt/u);
  assert.match(fixture, /sendJevOnce\(\{ request, apiKey, signal: abortController\.signal \}\)/u);
  assert.doesNotMatch(source, /fetch\(|https\.request|Resolver\(/u);
  assert.match(evidenceVerifier, /merge-base/u);
  assert.match(evidenceVerifier, /executedHead:\s*startHead/u);
  assert.doesNotMatch(evidenceVerifier, /baseHead:\s*['"]f02f38a6/u);
  assert.deepEqual(Object.keys(METRICS).sort(), [
      'closedFixtureDiagnostics', 'blockedExternalNetworkAttempts', 'controlledDnsResolve4', 'controlledDnsResolve6',
    'controlledHttpsRequests', 'controlledHttpsWrites', 'processRuns', 'processSigkillSignalsSent',
    'processSigtermSignalsSent', 'providerCalls', 'ownedRunnerStarts', 'ownedRunnerTermSignals',
    'ownedRunnerKillSignals', 'ownedRunnerShutdownElapsedMsMax', 'ownedRunnerShutdownSamples',
      'processSigtermDrainElapsedMsMax', 'processSigtermDrainSamples', 'processCloseElapsedMsMax',
      'processCloseSamples', 'runnerDeadlineCloseElapsedMsMax', 'runnerDeadlineCloseSamples',
      'storeHighWaterBytes', 'storeReservedHighWaterBytes', 'databaseHighWaterBytes', 'walHighWaterBytes',
      'shmHighWaterBytes', 'freeSpaceLowWaterBytes', 'pendingEventHighWaterRecords',
      'reservedEventHighWaterSlots', 'resourceHighWaterSamples',
      'ownedRunnerCountHighWater', 'supervisorOpenFileDescriptors', 'runnerOpenFileDescriptors',
      'processSigintSignalsSent', 'maxSupervisorRssBytes', 'maxRunnerRssBytes',
      'ci1PhaseCaptures', 'ci1PhaseCaptureBytes', 'ci1PhaseCaptureOverflow', 'ci1PhaseCaptureIncomplete',
      'ci1PhaseCaptureChildCloseUnconfirmed', 'ci1PhaseCaptureOwnedGroupCloseUnconfirmed',
      'c02ControlReadFailureLines', 'c02ControlReadFailureCount', 'c02ControlReadFailureBytes',
      'c02ControlReadDiagnosticsComplete',
      'ops02RaceDiagnosticLines', 'ops02RaceDiagnosticCount', 'ops02RaceDiagnosticBytes',
      'ops02RaceDiagnosticsComplete', 'ops06ClosePhaseDiagnosticLines',
      'ops06ClosePhaseDiagnosticCount', 'ops06ClosePhaseDiagnosticBytes', 'ops06ClosePhaseDiagnosticsComplete',
      'ops12RecoveryDiagnosticLines', 'ops12RecoveryDiagnosticCount',
      'ops12RecoveryDiagnosticBytes', 'ops12RecoveryDiagnosticsComplete'
  ].sort());
  const closedChannel = METRICS.closedFixtureDiagnostics;
  assert.deepEqual(Object.keys(closedChannel).sort(), ['lines', 'count', 'bytes', 'diagnosticsComplete'].sort());
  assert.equal(typeof closedChannel.lines, 'string');
  assert.equal(Number.isSafeInteger(closedChannel.count), true);
  assert.ok(closedChannel.count >= 0 && closedChannel.count <= 32);
  assert.equal(Number.isSafeInteger(closedChannel.bytes), true);
  assert.ok(closedChannel.bytes >= 0 && closedChannel.bytes <= 16_384);
  assert.equal(Buffer.byteLength(closedChannel.lines, 'utf8'), closedChannel.bytes);
  assert.equal(closedChannel.diagnosticsComplete, true);
  const closedFrames = closedChannel.lines === '' ? [] : closedChannel.lines.split('\n');
  if (closedFrames.length) assert.equal(closedFrames.pop(), '');
  assert.equal(closedFrames.length, closedChannel.count);
  assert.ok(closedFrames.every(line => Buffer.byteLength(`${line}\n`, 'utf8') <= 768));
  const parsedClosedChannel = parseClosedDiagnosticChannel(closedChannel);
  assert.equal(parsedClosedChannel.complete, true);
  assert.equal(parsedClosedChannel.recordCount, closedChannel.count);
  for (const key of [
    'ci1PhaseCaptureOverflow', 'ci1PhaseCaptureIncomplete', 'ci1PhaseCaptureChildCloseUnconfirmed',
    'ci1PhaseCaptureOwnedGroupCloseUnconfirmed'
  ]) {
    assert.equal(typeof METRICS[key], 'boolean', `${key} must remain an explicit boolean custody metric`);
  }
  assert.equal(typeof METRICS.c02ControlReadFailureLines, 'string');
  assert.equal(Number.isSafeInteger(METRICS.c02ControlReadFailureCount), true);
  assert.ok(METRICS.c02ControlReadFailureCount >= 0 && METRICS.c02ControlReadFailureCount <= 8);
  assert.equal(Number.isSafeInteger(METRICS.c02ControlReadFailureBytes), true);
  assert.ok(METRICS.c02ControlReadFailureBytes >= 0 && METRICS.c02ControlReadFailureBytes <= 1_536);
  assert.equal(typeof METRICS.c02ControlReadDiagnosticsComplete, 'boolean');
  assert.equal(Buffer.byteLength(METRICS.c02ControlReadFailureLines, 'utf8'), METRICS.c02ControlReadFailureBytes);
  const c02DiagnosticLines = METRICS.c02ControlReadFailureLines.length === 0
    ? [] : METRICS.c02ControlReadFailureLines.split('\n');
  if (c02DiagnosticLines.length > 0) {
    assert.equal(c02DiagnosticLines.at(-1), '', 'nonempty diagnostics terminate on a complete line');
    c02DiagnosticLines.pop();
  }
  assert.equal(c02DiagnosticLines.length, METRICS.c02ControlReadFailureCount);
  assert.ok(c02DiagnosticLines.every(line => Buffer.byteLength(`${line}\n`, 'utf8') <= 192));
  assert.ok(Buffer.byteLength(METRICS.c02ControlReadFailureLines, 'utf8') <= 1_536);
  assert.equal(typeof METRICS.ops02RaceDiagnosticLines, 'string');
  assert.equal(Number.isSafeInteger(METRICS.ops02RaceDiagnosticCount), true);
  assert.ok(METRICS.ops02RaceDiagnosticCount > 0 && METRICS.ops02RaceDiagnosticCount <= OPS02_RACE_DIAGNOSTIC_MAX_RECORDS);
  assert.equal(Number.isSafeInteger(METRICS.ops02RaceDiagnosticBytes), true);
  assert.ok(METRICS.ops02RaceDiagnosticBytes >= 0 && METRICS.ops02RaceDiagnosticBytes <= OPS02_RACE_DIAGNOSTIC_MAX_BYTES);
  assert.equal(typeof METRICS.ops02RaceDiagnosticsComplete, 'boolean');
  assert.equal(Buffer.byteLength(METRICS.ops02RaceDiagnosticLines, 'utf8'), METRICS.ops02RaceDiagnosticBytes);
  const ops02DiagnosticLines = METRICS.ops02RaceDiagnosticLines.split('\n');
  assert.equal(ops02DiagnosticLines.at(-1), '', 'OPS02 diagnostics terminate on a complete line');
  ops02DiagnosticLines.pop();
  assert.equal(ops02DiagnosticLines.length, METRICS.ops02RaceDiagnosticCount);
  assert.ok(ops02DiagnosticLines.every(line => Buffer.byteLength(`${line}\n`, 'utf8') <= OPS02_RACE_DIAGNOSTIC_MAX_LINE_BYTES));
  assert.equal(parseOps02RaceDiagnostics({
    lines: METRICS.ops02RaceDiagnosticLines, count: METRICS.ops02RaceDiagnosticCount,
    bytes: METRICS.ops02RaceDiagnosticBytes, diagnosticsComplete: METRICS.ops02RaceDiagnosticsComplete
  }).complete, true);
  assert.equal(typeof METRICS.ops06ClosePhaseDiagnosticLines, 'string');
  assert.equal(Number.isSafeInteger(METRICS.ops06ClosePhaseDiagnosticCount), true);
  assert.equal(METRICS.ops06ClosePhaseDiagnosticCount, OPS06_CLOSE_PHASE_DIAGNOSTIC_MAX_RECORDS);
  assert.equal(Number.isSafeInteger(METRICS.ops06ClosePhaseDiagnosticBytes), true);
  assert.ok(METRICS.ops06ClosePhaseDiagnosticBytes >= 0
    && METRICS.ops06ClosePhaseDiagnosticBytes <= OPS06_CLOSE_PHASE_DIAGNOSTIC_MAX_BYTES);
  assert.equal(typeof METRICS.ops06ClosePhaseDiagnosticsComplete, 'boolean');
  assert.equal(Buffer.byteLength(METRICS.ops06ClosePhaseDiagnosticLines, 'utf8'), METRICS.ops06ClosePhaseDiagnosticBytes);
  assert.equal(parseOps06ClosePhaseDiagnostics({
    lines: METRICS.ops06ClosePhaseDiagnosticLines, count: METRICS.ops06ClosePhaseDiagnosticCount,
    bytes: METRICS.ops06ClosePhaseDiagnosticBytes, diagnosticsComplete: METRICS.ops06ClosePhaseDiagnosticsComplete
  }).complete, true);
  assert.equal(typeof METRICS.ops12RecoveryDiagnosticLines, 'string');
  assert.equal(Number.isSafeInteger(METRICS.ops12RecoveryDiagnosticCount), true);
  assert.equal(METRICS.ops12RecoveryDiagnosticCount, OPS12_RECOVERY_DIAGNOSTIC_MAX_RECORDS);
  assert.equal(Number.isSafeInteger(METRICS.ops12RecoveryDiagnosticBytes), true);
  assert.ok(METRICS.ops12RecoveryDiagnosticBytes >= 0
    && METRICS.ops12RecoveryDiagnosticBytes <= OPS12_RECOVERY_DIAGNOSTIC_MAX_BYTES);
  assert.equal(typeof METRICS.ops12RecoveryDiagnosticsComplete, 'boolean');
  assert.equal(Buffer.byteLength(METRICS.ops12RecoveryDiagnosticLines, 'utf8'), METRICS.ops12RecoveryDiagnosticBytes);
  assert.equal(parseOps12RecoveryDiagnostics({
    lines: METRICS.ops12RecoveryDiagnosticLines, count: METRICS.ops12RecoveryDiagnosticCount,
    bytes: METRICS.ops12RecoveryDiagnosticBytes, diagnosticsComplete: METRICS.ops12RecoveryDiagnosticsComplete
  }).complete, true);
  assert.equal(METRICS.providerCalls, 0);
  assert.equal(METRICS.blockedExternalNetworkAttempts, 0);
  await t.test('OPS13.C01 verifier binds explicit local case IDs, clean pins, process/resource metrics, and upload members', async t => {
    assert.match(evidenceVerifier, /REQUIRED_LOCAL_CASES/u);
    assert.equal(Object.hasOwn(METRICS, 'closedFixtureDiagnostics'), true);
    const omittedClosedChannel = { ...METRICS };
    delete omittedClosedChannel.closedFixtureDiagnostics;
    assert.notDeepEqual(Object.keys(omittedClosedChannel).sort(), Object.keys(METRICS).sort());
    assert.equal(parseClosedDiagnosticChannel(undefined).complete, false);
    const exitDispositionControls = [
      { result: { code: 0, signal: null }, exit: 'zero', passed: false },
      { result: { code: 1, signal: null }, exit: 'nonzero', passed: false },
      { result: { code: 2, signal: null }, exit: 'nonzero', passed: true },
      { result: { code: null, signal: 'SIGTERM' }, exit: 'signal', passed: false },
      { result: { code: null, signal: null }, exit: 'unknown', passed: false }
    ];
    for (const control of exitDispositionControls) {
      assert.equal(classifyChildExitDisposition(control.result), control.exit);
      assert.equal(control.result.code === 2, control.passed);
    }
    const ops02WireState = () => ({
      ops02RaceDiagnosticLines: '', ops02RaceDiagnosticCount: 0,
      ops02RaceDiagnosticBytes: 0, ops02RaceDiagnosticsComplete: true
    });
    const all48State = ops02WireState();
    const all48Expected = [];
    const appendOps02 = (boundary, raceKind = 'diagnostic', fields = {}) => {
      const record = {
        schema: 'agent-harness.ah11-ops02-race-diagnostic.v3', caseId: 'OPS02.C02',
        boundary, raceKind, contender: null, passed: true, exit: 'not_applicable',
        childStage: 'none', childCode: 'none', transactionBoundary: 'none',
        sqlitePrimaryCode: null, transactionActive: null, rollbackDisposition: 'not_observed',
        ...fields
      };
      all48Expected.push(record);
      assert.equal(recordOps02RaceDiagnostic(record, all48State), true);
    };
    for (const boundary of [
      'case_started', 'diagnostic_no_canary', 'diagnostic_proxy_unread'
    ]) appendOps02(boundary);
    appendOps02('diagnostic_child_exit', 'diagnostic', {
      exit: 'nonzero', childStage: 'config', childCode: 'UNKNOWN'
    });
    appendOps02('diagnostic_child_prefix');
    appendOps02('diagnostic_child_no_canary');
    const successfulRaces = [
      ['global_records', 1], ['project_bytes_existing', 1],
      ['project_bytes_new', 0], ['project_records', 1], ['global_bytes', 3]
    ];
    for (const [raceKind, seedCount] of successfulRaces) {
      for (let index = 0; index < seedCount; index += 1) appendOps02('seed_accept', raceKind);
      appendOps02('contender_exit', raceKind, { contender: 0, exit: 'zero' });
      appendOps02('contender_exit', raceKind, { contender: 1, exit: 'zero' });
      appendOps02('child_exit_group', raceKind);
      for (const boundary of ['accepted_count', 'refusal_count', 'stored_counter', 'stored_rows']) {
        appendOps02(boundary, raceKind);
      }
      if (raceKind === 'project_bytes_new') appendOps02('reservation', raceKind);
    }
    assert.equal(all48State.ops02RaceDiagnosticCount, 48);
    assert.equal(all48State.ops02RaceDiagnosticsComplete, true);
    const all48RawLines = all48State.ops02RaceDiagnosticLines.trimEnd().split('\n');
    assert.equal(all48RawLines.length, 48);
    assert.ok(all48RawLines.every(line => Buffer.byteLength(`${line}\n`, 'utf8') <= 320));
    assert.ok(all48State.ops02RaceDiagnosticBytes <= 16_384);
    const all48Parsed = parseOps02RaceDiagnostics({
      lines: all48State.ops02RaceDiagnosticLines, count: all48State.ops02RaceDiagnosticCount,
      bytes: all48State.ops02RaceDiagnosticBytes, diagnosticsComplete: all48State.ops02RaceDiagnosticsComplete
    });
    assert.equal(all48Parsed.complete, true);
    assert.equal(all48Parsed.recordCount, 48);
    const ops02Fields = record => [
      record.boundary, record.raceKind, record.contender, record.passed, record.exit,
      record.childStage, record.childCode, record.transactionBoundary, record.sqlitePrimaryCode,
      record.transactionActive, record.rollbackDisposition
    ];
    assert.deepEqual(all48Parsed.records.map(ops02Fields), all48Expected.map(ops02Fields));
    const maximumLegalOps02Record = {
      schema: 'agent-harness.ah11-ops02-race-diagnostic.v3', caseId: 'OPS02.C02',
      boundary: 'contender_exit', raceKind: 'global_bytes', contender: 1, passed: false,
      exit: 'unknown', childStage: 'admission', childCode: 'STORE_BUSY',
      transactionBoundary: 'rollback', sqlitePrimaryCode: 6,
      transactionActive: null, rollbackDisposition: 'rollback_check_failed'
    };
    const maximumLegalOps02 = encodeOps02RaceDiagnostic(maximumLegalOps02Record);
    assert.equal(typeof maximumLegalOps02, 'string');
    assert.ok(maximumLegalOps02.startsWith(OPS02_RACE_DIAGNOSTIC_PREFIX));
    assert.ok(Buffer.byteLength(`${maximumLegalOps02}\n`, 'utf8') <= 320);
    const maximumWithoutSchema = { ...maximumLegalOps02Record };
    delete maximumWithoutSchema.schema;
    assert.equal(encodeOps02RaceDiagnostic(maximumWithoutSchema), null);
    const maximumWithoutCaseId = { ...maximumLegalOps02Record };
    delete maximumWithoutCaseId.caseId;
    assert.equal(encodeOps02RaceDiagnostic(maximumWithoutCaseId), null);
    const maximumRoundTripState = ops02WireState();
    assert.equal(recordOps02RaceDiagnostic({ boundary: 'case_started', passed: true }, maximumRoundTripState), true);
    const maximumRoundTripLines = `${maximumRoundTripState.ops02RaceDiagnosticLines}${maximumLegalOps02}\n`;
    const maximumRoundTrip = parseOps02RaceDiagnostics({
      lines: maximumRoundTripLines, count: 2,
      bytes: Buffer.byteLength(maximumRoundTripLines, 'utf8'), diagnosticsComplete: true
    });
    assert.equal(maximumRoundTrip.complete, true);
    assert.equal(maximumRoundTrip.records[1].schema, 'agent-harness.ah11-ops02-race-diagnostic.v3');
    assert.equal(maximumRoundTrip.records[1].caseId, 'OPS02.C02');
    assert.deepEqual(ops02Fields(maximumRoundTrip.records[1]), ops02Fields(maximumLegalOps02Record));
    const wireBoundaryControls = [
      ['begin', 5, null, 'not_observed'], ['begin', 6, null, 'not_observed'],
      ['body', 5, false, 'inactive'], ['body', 6, true, 'rolled_back'],
      ['body', 6, null, 'unknown'],
      ['commit', 5, false, 'inactive'], ['commit', 6, true, 'rolled_back'], ['commit', 5, null, 'unknown'],
      ['rollback', 5, true, 'rollback_failed'], ['rollback', 6, null, 'rollback_check_failed']
    ];
    // These closed wire tuples validate format only; native provenance is asserted by the held-writer control above.
    for (const [transactionBoundary, sqlitePrimaryCode, transactionActive, rollbackDisposition] of wireBoundaryControls) {
      const state = ops02WireState();
      assert.equal(recordOps02RaceDiagnostic({ boundary: 'case_started', passed: true }, state), true);
      assert.equal(recordOps02RaceDiagnostic({
        boundary: 'contender_exit', raceKind: 'project_bytes_existing', contender: 0,
        passed: false, exit: 'nonzero', childStage: 'admission', childCode: 'STORE_BUSY',
        transactionBoundary, sqlitePrimaryCode, transactionActive, rollbackDisposition
      }, state), true);
      const parsed = parseOps02RaceDiagnostics({
        lines: state.ops02RaceDiagnosticLines, count: state.ops02RaceDiagnosticCount,
        bytes: state.ops02RaceDiagnosticBytes, diagnosticsComplete: state.ops02RaceDiagnosticsComplete
      });
      assert.equal(parsed.complete, true);
      assert.equal(parsed.records[1].sqlitePrimaryCode, sqlitePrimaryCode);
      assert.equal(parsed.records[1].transactionBoundary, transactionBoundary);
      assert.equal(parsed.records[1].transactionActive, transactionActive);
      assert.equal(parsed.records[1].rollbackDisposition, rollbackDisposition);
    }
    const unavailableOps02State = ops02WireState();
    assert.equal(recordOps02RaceDiagnostic({ boundary: 'case_started', passed: true }, unavailableOps02State), true);
    assert.equal(recordOps02RaceDiagnostic({
      boundary: 'diagnostic_child_exit', raceKind: 'diagnostic', passed: false,
      exit: 'unknown', childStage: 'unobserved', childCode: 'unobserved',
      transactionBoundary: 'unobserved', sqlitePrimaryCode: null,
      transactionActive: null, rollbackDisposition: 'not_observed'
    }, unavailableOps02State), true);
    const unavailableOps02 = parseOps02RaceDiagnostics({
      lines: unavailableOps02State.ops02RaceDiagnosticLines, count: unavailableOps02State.ops02RaceDiagnosticCount,
      bytes: unavailableOps02State.ops02RaceDiagnosticBytes, diagnosticsComplete: unavailableOps02State.ops02RaceDiagnosticsComplete
    });
    assert.equal(unavailableOps02.complete, true);
    assert.equal(unavailableOps02.records[1].exit, 'unknown');
    assert.equal(unavailableOps02.records[1].sqlitePrimaryCode, null);
    assert.equal(unavailableOps02.records[1].transactionActive, null);
    assert.equal(unavailableOps02.records[1].rollbackDisposition, 'not_observed');
    const duplicateGroupState = ops02WireState();
    assert.equal(recordOps02RaceDiagnostic({ boundary: 'case_started', passed: true }, duplicateGroupState), true);
    assert.equal(recordOps02RaceDiagnostic({ boundary: 'child_exit_group', raceKind: 'global_records', passed: true }, duplicateGroupState), true);
    assert.equal(recordOps02RaceDiagnostic({ boundary: 'child_exit_group', raceKind: 'global_records', passed: true }, duplicateGroupState), false);
    assert.equal(duplicateGroupState.ops02RaceDiagnosticsComplete, false);
    const duplicateGroupLine = duplicateGroupState.ops02RaceDiagnosticLines.split('\n')[1];
    const duplicateGroupParserLines = `${duplicateGroupState.ops02RaceDiagnosticLines}${duplicateGroupLine}\n`;
    assert.equal(parseOps02RaceDiagnostics({
      lines: duplicateGroupParserLines, count: 3,
      bytes: Buffer.byteLength(duplicateGroupParserLines, 'utf8'), diagnosticsComplete: true
    }).complete, false);
    const repeatedSeedState = ops02WireState();
    assert.equal(recordOps02RaceDiagnostic({ boundary: 'case_started', passed: true }, repeatedSeedState), true);
    for (let index = 0; index < 3; index += 1) {
      assert.equal(recordOps02RaceDiagnostic({ boundary: 'seed_accept', raceKind: 'global_bytes', passed: true }, repeatedSeedState), true);
    }
    const repeatedSeeds = parseOps02RaceDiagnostics({
      lines: repeatedSeedState.ops02RaceDiagnosticLines, count: repeatedSeedState.ops02RaceDiagnosticCount,
      bytes: repeatedSeedState.ops02RaceDiagnosticBytes, diagnosticsComplete: repeatedSeedState.ops02RaceDiagnosticsComplete
    });
    assert.equal(repeatedSeeds.complete, true);
    assert.equal(repeatedSeeds.records.filter(record => record.boundary === 'seed_accept').length, 3);
    const malformedOps02Line = `${OPS02_RACE_DIAGNOSTIC_PREFIX}[\n`;
    assert.equal(parseOps02RaceDiagnostics({
      lines: malformedOps02Line, count: 1, bytes: Buffer.byteLength(malformedOps02Line, 'utf8'), diagnosticsComplete: true
    }).complete, false);
    const wrongLengthOps02Line = `${OPS02_RACE_DIAGNOSTIC_PREFIX}${JSON.stringify([0])}\n`;
    assert.equal(parseOps02RaceDiagnostics({
      lines: wrongLengthOps02Line, count: 1, bytes: Buffer.byteLength(wrongLengthOps02Line, 'utf8'), diagnosticsComplete: true
    }).complete, false);
    const duplicateStarted = `${all48State.ops02RaceDiagnosticLines}${all48State.ops02RaceDiagnosticLines.split('\n')[0]}\n`;
    assert.equal(parseOps02RaceDiagnostics({
      lines: duplicateStarted, count: 49, bytes: Buffer.byteLength(duplicateStarted, 'utf8'), diagnosticsComplete: true
    }).complete, false);
    const truncatedOps02 = all48State.ops02RaceDiagnosticLines.slice(0, -2);
    assert.equal(parseOps02RaceDiagnostics({
      lines: truncatedOps02, count: 48, bytes: Buffer.byteLength(truncatedOps02, 'utf8'), diagnosticsComplete: true
    }).complete, false);
    const invalidOps02Wire = `${OPS02_RACE_DIAGNOSTIC_PREFIX}${JSON.stringify([99, 99, null, true, 0, 0, 0, 0, null, null, 0])}\n`;
    assert.equal(parseOps02RaceDiagnostics({
      lines: invalidOps02Wire, count: 1, bytes: Buffer.byteLength(invalidOps02Wire, 'utf8'), diagnosticsComplete: true
    }).complete, false);
    const oversizedOps02Line = `${OPS02_RACE_DIAGNOSTIC_PREFIX}${'x'.repeat(320)}\n`;
    assert.equal(parseOps02RaceDiagnostics({
      lines: oversizedOps02Line, count: 1, bytes: Buffer.byteLength(oversizedOps02Line, 'utf8'), diagnosticsComplete: true
    }).complete, false);
    const incompleteOps02State = ops02WireState();
    assert.equal(recordOps02RaceDiagnostic({ boundary: 'case_started', passed: true }, incompleteOps02State), true);
    assert.equal(recordOps02RaceDiagnostic({ boundary: 'case_started', passed: true }, incompleteOps02State), false);
    assert.equal(incompleteOps02State.ops02RaceDiagnosticsComplete, false);
    const incompleteCount = incompleteOps02State.ops02RaceDiagnosticCount;
    assert.equal(recordOps02RaceDiagnostic({ boundary: 'diagnostic_no_canary', passed: true }, incompleteOps02State), false);
    assert.equal(incompleteOps02State.ops02RaceDiagnosticCount, incompleteCount);
    assert.equal(parseOps02RaceDiagnostics({
      lines: incompleteOps02State.ops02RaceDiagnosticLines,
      count: incompleteOps02State.ops02RaceDiagnosticCount,
      bytes: incompleteOps02State.ops02RaceDiagnosticBytes,
      diagnosticsComplete: incompleteOps02State.ops02RaceDiagnosticsComplete
    }).complete, false);
    const rawOps02Line = values => `${OPS02_RACE_DIAGNOSTIC_PREFIX}${JSON.stringify(values)}\n`;
    const ops02CaseStartedWire = rawOps02Line([0, 0, null, true, 0, 8, 14, 4, null, null, 0]);
    const rejectOps02TupleAfterStart = tuple => {
      const lines = `${ops02CaseStartedWire}${rawOps02Line(tuple)}`;
      return parseOps02RaceDiagnostics({
        lines, count: 2, bytes: Buffer.byteLength(lines, 'utf8'), diagnosticsComplete: true
      }).complete === false;
    };
    assert.equal(rejectOps02TupleAfterStart([7, 2, 0, true, 1, 4, 3, 0, 5, null, 0]), true);
    assert.equal(rejectOps02TupleAfterStart([7, 1, 0, false, 0, 8, 14, 4, null, null, 0]), true);
    assert.equal(rejectOps02TupleAfterStart([7, 2, 0, false, 2, 4, 3, 1, 5, true, 4]), true);

    const ops02ControlRecords = [
      {
        schema: 'agent-harness.ah11-ops02-race-diagnostic.v3', caseId: 'OPS02.C02',
        boundary: 'case_started', raceKind: 'diagnostic', contender: null,
        passed: true, exit: 'not_applicable', childStage: 'none', childCode: 'none',
        transactionBoundary: 'none', sqlitePrimaryCode: null,
        transactionActive: null, rollbackDisposition: 'not_observed'
      },
      {
        schema: 'agent-harness.ah11-ops02-race-diagnostic.v3', caseId: 'OPS02.C02',
        boundary: 'contender_exit', raceKind: 'global_records', contender: 1,
        passed: false, exit: 'nonzero', childStage: 'admission', childCode: 'STORE_BUSY',
        transactionBoundary: 'begin', sqlitePrimaryCode: 5,
        transactionActive: null, rollbackDisposition: 'not_observed'
      },
      {
        schema: 'agent-harness.ah11-ops02-race-diagnostic.v3', caseId: 'OPS02.C02',
        boundary: 'contender_exit', raceKind: 'project_bytes_existing', contender: 0,
        passed: false, exit: 'nonzero', childStage: 'admission', childCode: 'STORE_BUSY',
        transactionBoundary: 'commit', sqlitePrimaryCode: 6,
        transactionActive: true, rollbackDisposition: 'rolled_back'
      }
    ];
    const ops02ControlLines = ops02ControlRecords
      .map(record => `${encodeOps02RaceDiagnostic(record)}\n`).join('');
    const ops02Control = parseOps02RaceDiagnostics({
      lines: ops02ControlLines, count: ops02ControlRecords.length,
      bytes: Buffer.byteLength(ops02ControlLines, 'utf8'), diagnosticsComplete: true
    });
    assert.equal(ops02Control.complete, true);
    assert.equal(ops02Control.records[1].childCode, 'STORE_BUSY');
    assert.equal(ops02Control.records[1].transactionBoundary, 'begin');
    assert.equal(ops02Control.records[1].transactionActive, null);
    assert.equal(ops02Control.records[1].rollbackDisposition, 'not_observed');
    assert.equal(ops02Control.records[2].transactionBoundary, 'commit');
    assert.equal(ops02Control.records[2].transactionActive, true);
    assert.equal(ops02Control.records[2].rollbackDisposition, 'rolled_back');
    const privateOps02Record = {
      ...ops02ControlRecords[1], childCode: 'SYNTHETIC_PRIVATE_DIAGNOSTIC_CANARY_96d'
    };
    assert.equal(encodeOps02RaceDiagnostic(privateOps02Record), null);
    assert.equal(encodeOps02RaceDiagnostic({
      ...ops02ControlRecords[1], passed: true, exit: 'nonzero'
    }), null);
    const unsafeOps02Line = `AH11_OPS_OPS02_C02_DIAGNOSTIC_V3 ${JSON.stringify(privateOps02Record)}\n`;
    const unsafeOps02 = parseOps02RaceDiagnostics({
      lines: `${ops02ControlLines.split('\n')[0]}\n${unsafeOps02Line}`,
      count: 2, bytes: Buffer.byteLength(`${ops02ControlLines.split('\n')[0]}\n${unsafeOps02Line}`, 'utf8'),
      diagnosticsComplete: true
    });
    assert.equal(unsafeOps02.complete, false);
    assert.doesNotMatch(JSON.stringify(unsafeOps02), /SYNTHETIC_PRIVATE_DIAGNOSTIC_CANARY_96d/u);
    const admissionBusy = createAdmissionFailureDiagnostic('admission', Object.assign(
      new Error('AH11_RAW_ADMISSION_PRIVATE_CANARY'), { code: 'STORE_BUSY' }
    ), {
      transactionBoundary: 'commit', sqlitePrimaryCode: 6,
      transactionActive: true, rollbackDisposition: 'rolled_back'
    });
    assert.deepEqual(admissionBusy, {
      schema: 'ops-admission-failure.v2', stage: 'admission', code: 'STORE_BUSY',
      transactionBoundary: 'commit', sqlitePrimaryCode: 6,
      transactionActive: true, rollbackDisposition: 'rolled_back'
    });
    const admissionFailureLine = `OPS_ADMISSION_FAILURE ${JSON.stringify(admissionBusy)}\n`;
    assert.deepEqual(parseOps02AdmissionFailure(admissionFailureLine), {
      stage: 'admission', code: 'STORE_BUSY', transactionBoundary: 'commit', sqlitePrimaryCode: 6,
      transactionActive: true, rollbackDisposition: 'rolled_back'
    });
    assert.doesNotMatch(admissionFailureLine, /private details/u);
    assert.doesNotMatch(admissionFailureLine, /AH11_RAW_ADMISSION_PRIVATE_CANARY/u);
    const rawAdmissionError = Object.assign(
      new Error('AH11_RAW_ADMISSION_PRIVATE_CANARY'), { code: 'STORE_BUSY' }
    );
    const rawAdmissionV1Expected = {
      schema: 'ops-admission-failure.v1', stage: 'admission', code: 'STORE_BUSY'
    };
    const parsedAdmissionV1Expected = {
      stage: 'admission', code: 'STORE_BUSY', transactionBoundary: 'none',
      sqlitePrimaryCode: null, transactionActive: null, rollbackDisposition: 'not_observed'
    };
    for (const transactionBoundary of ['body', 'commit']) {
      const impossibleRollbackFailure = {
        transactionBoundary, sqlitePrimaryCode: 5,
        transactionActive: true, rollbackDisposition: 'rollback_failed'
      };
      // The formatter's optional tuple is a synthetic control seam, not native provenance.
      const fallbackDiagnostic = createAdmissionFailureDiagnostic(
        'admission', rawAdmissionError, impossibleRollbackFailure
      );
      assert.deepEqual(fallbackDiagnostic, rawAdmissionV1Expected);
      const fallbackLine = `OPS_ADMISSION_FAILURE ${JSON.stringify(fallbackDiagnostic)}\n`;
      assert.deepEqual(parseOps02AdmissionFailure(fallbackLine), parsedAdmissionV1Expected);
      const impossibleRawV2 = {
        schema: 'ops-admission-failure.v2', stage: 'admission', code: 'STORE_BUSY',
        ...impossibleRollbackFailure
      };
      assert.equal(parseOps02AdmissionFailure(
        `OPS_ADMISSION_FAILURE ${JSON.stringify(impossibleRawV2)}\n`
      ), null);
      assert.doesNotMatch(fallbackLine, /AH11_RAW_ADMISSION_PRIVATE_CANARY/u);
    }
    const rollbackBoundaryDiagnostic = createAdmissionFailureDiagnostic('admission', rawAdmissionError, {
      transactionBoundary: 'rollback', sqlitePrimaryCode: 5,
      transactionActive: true, rollbackDisposition: 'rollback_failed'
    });
    assert.deepEqual(rollbackBoundaryDiagnostic, {
      schema: 'ops-admission-failure.v2', stage: 'admission', code: 'STORE_BUSY',
      transactionBoundary: 'rollback', sqlitePrimaryCode: 5,
      transactionActive: true, rollbackDisposition: 'rollback_failed'
    });
    const rollbackBoundaryLine = `OPS_ADMISSION_FAILURE ${JSON.stringify(rollbackBoundaryDiagnostic)}\n`;
    const rollbackBoundaryParsed = parseOps02AdmissionFailure(rollbackBoundaryLine);
    assert.deepEqual(rollbackBoundaryParsed, {
      stage: 'admission', code: 'STORE_BUSY', transactionBoundary: 'rollback',
      sqlitePrimaryCode: 5, transactionActive: true, rollbackDisposition: 'rollback_failed'
    });
    assert.doesNotMatch(rollbackBoundaryLine, /AH11_RAW_ADMISSION_PRIVATE_CANARY/u);
    const rawAdmissionChainState = ops02WireState();
    assert.equal(recordOps02RaceDiagnostic({ boundary: 'case_started', passed: true }, rawAdmissionChainState), true);
    assert.equal(recordOps02ContenderResults('project_bytes_existing', [{
      code: 1, signal: null, stderr: rollbackBoundaryLine
    }], rawAdmissionChainState), false);
    assert.doesNotMatch(rawAdmissionChainState.ops02RaceDiagnosticLines, /AH11_RAW_ADMISSION_PRIVATE_CANARY/u);
    const rawAdmissionChain = parseOps02RaceDiagnostics({
      lines: rawAdmissionChainState.ops02RaceDiagnosticLines,
      count: rawAdmissionChainState.ops02RaceDiagnosticCount,
      bytes: rawAdmissionChainState.ops02RaceDiagnosticBytes,
      diagnosticsComplete: rawAdmissionChainState.ops02RaceDiagnosticsComplete
    });
    assert.equal(rawAdmissionChain.complete, true);
    assert.equal(rawAdmissionChain.records[1].schema, 'agent-harness.ah11-ops02-race-diagnostic.v3');
    assert.equal(rawAdmissionChain.records[1].caseId, 'OPS02.C02');
    assert.deepEqual(ops02Fields(rawAdmissionChain.records[1]), [
      'contender_exit', 'project_bytes_existing', 0, false, 'nonzero',
      'admission', 'STORE_BUSY', 'rollback', 5, true, 'rollback_failed'
    ]);
    assert.equal(rawAdmissionChain.records[2].boundary, 'child_exit_group');
    assert.equal(rawAdmissionChain.records[2].passed, false);
    assert.doesNotMatch(JSON.stringify(rawAdmissionChain), /AH11_RAW_ADMISSION_PRIVATE_CANARY/u);
    const ops06ControlRecord = {
      schema: 'agent-harness.ah11-ops06-close-phases.v2', caseId: 'OPS06.C01',
      stage: 'complete', failureClass: 'none', exitDisposition: 'zero', exitCode: 0, exitSignal: null,
      signalToFirstStopElapsedMs: 12.5, restartWorkElapsedMs: 120,
      finalStopElapsedMs: 3.25, readyToFinalRecordElapsedMs: 150,
      processCloseElapsedMs: 180, restartOwnership: 'fresh_process', restartProcessCloseElapsedMs: 300
    };
    const ops06ControlLines = `AH11_OPS_OPS06_CLOSE_PHASES ${JSON.stringify(ops06ControlRecord)}\n`;
    const ops06Control = parseOps06ClosePhaseDiagnostics({
      lines: ops06ControlLines, count: 1,
      bytes: Buffer.byteLength(ops06ControlLines, 'utf8'), diagnosticsComplete: true
    });
    assert.equal(ops06Control.complete, true);
    assert.equal(ops06Control.records[0].processCloseElapsedMs, 180);
    const ops06UnknownRecord = {
      ...ops06ControlRecord, stage: 'supervisor_close_wait', failureClass: 'child_exit',
      exitDisposition: 'unknown', exitCode: null, exitSignal: null,
      signalToFirstStopElapsedMs: null, restartWorkElapsedMs: null, finalStopElapsedMs: null,
      readyToFinalRecordElapsedMs: null, processCloseElapsedMs: null, restartProcessCloseElapsedMs: null
    };
    const ops06UnknownLine = `AH11_OPS_OPS06_CLOSE_PHASES ${JSON.stringify(ops06UnknownRecord)}\n`;
    const ops06Unknown = parseOps06ClosePhaseDiagnostics({
      lines: ops06UnknownLine, count: 1,
      bytes: Buffer.byteLength(ops06UnknownLine, 'utf8'), diagnosticsComplete: true
    });
    assert.equal(ops06Unknown.complete, true);
    assert.equal(ops06Unknown.records[0].stage, 'supervisor_close_wait');
    assert.equal(ops06Unknown.records[0].exitDisposition, 'unknown');
    const unsafeOps06Line = `AH11_OPS_OPS06_CLOSE_PHASES ${JSON.stringify({
      ...ops06ControlRecord, message: 'SYNTHETIC_PRIVATE_DIAGNOSTIC_CANARY_96d'
    })}\n`;
    const unsafeOps06 = parseOps06ClosePhaseDiagnostics({
      lines: unsafeOps06Line, count: 1,
      bytes: Buffer.byteLength(unsafeOps06Line, 'utf8'), diagnosticsComplete: true
    });
    assert.equal(unsafeOps06.complete, false);
    assert.doesNotMatch(JSON.stringify(unsafeOps06), /SYNTHETIC_PRIVATE_DIAGNOSTIC_CANARY_96d/u);
    const ops12UnknownFailure = {
      schema: 'agent-harness.ah11-ops12-recovery-failure.v1',
      caseId: 'unknown', stage: 'unknown', errorClass: 'unknown', nativePrimaryCode: null
    };
    const ops12UnknownFailureLine = `AH11_OPS_OPS12_RECOVERY_FAILURE ${JSON.stringify(ops12UnknownFailure)}\n`;
    const ops12UnknownScenarios = [
      { name: 'missing marker', result: { code: 1, signal: null }, stderr: '' },
      { name: 'malformed marker', result: { code: null, signal: 'SIGTERM' }, stderr: 'AH11_OPS_OPS12_RECOVERY_FAILURE {broken}\n' },
      { name: 'duplicate marker', result: { code: 2, signal: null }, stderr: `${ops12UnknownFailureLine}${ops12UnknownFailureLine}` },
      { name: 'truncated marker', result: { code: null, signal: null }, stderr: 'AH11_OPS_OPS12_RECOVERY_FAILURE {"schema":' }
    ];
    for (const scenario of ops12UnknownScenarios) {
      const state = {
        ops12RecoveryDiagnosticLines: '', ops12RecoveryDiagnosticCount: 0,
        ops12RecoveryDiagnosticBytes: 0, ops12RecoveryDiagnosticsComplete: true
      };
      const failure = parseOps12RecoveryFailure(scenario.stderr);
      assert.equal(failure, null, scenario.name);
      assert.equal(recordOps12RecoveryDiagnostic(scenario.result, failure, state), true, scenario.name);
      const parsed = parseOps12RecoveryDiagnostics({
        lines: state.ops12RecoveryDiagnosticLines, count: state.ops12RecoveryDiagnosticCount,
        bytes: state.ops12RecoveryDiagnosticBytes, diagnosticsComplete: state.ops12RecoveryDiagnosticsComplete
      });
      assert.equal(parsed.complete, true, scenario.name);
      assert.equal(parsed.records[0].caseId, 'unknown', scenario.name);
      assert.equal(parsed.records[0].stage, 'unknown', scenario.name);
      assert.equal(parsed.records[0].exitDisposition,
        scenario.result.code === null && scenario.result.signal === null ? 'unknown'
          : scenario.result.signal ? 'signal' : 'nonzero', scenario.name);
      assert.equal(parsed.records[0].exitCode,
        Number.isSafeInteger(scenario.result.code) ? scenario.result.code : null, scenario.name);
      assert.equal(parsed.records[0].exitSignal, scenario.result.signal ?? null, scenario.name);
    }
    assert.match(evidenceVerifier, /buildOps12RecoveryDiagnosticRecord\(/u);
    assert.deepEqual(buildOps12RecoveryDiagnosticRecord({ code: null, signal: 'SIGTERM' }, null), {
      schema: 'agent-harness.ah11-ops12-recovery-child.v1', caseId: 'unknown', stage: 'unknown',
      errorClass: 'unknown', nativePrimaryCode: null,
      exitDisposition: 'signal', exitCode: null, exitSignal: 'SIGTERM'
    });
    const ops12MarkedUnknownState = {
      ops12RecoveryDiagnosticLines: '', ops12RecoveryDiagnosticCount: 0,
      ops12RecoveryDiagnosticBytes: 0, ops12RecoveryDiagnosticsComplete: true
    };
    const parsedUnknownMarker = parseOps12RecoveryFailure(ops12UnknownFailureLine);
    assert.equal(parsedUnknownMarker.caseId, 'unknown');
    assert.equal(recordOps12RecoveryDiagnostic({ code: 7, signal: null }, parsedUnknownMarker, ops12MarkedUnknownState), true);
    const parsedMarkedUnknown = parseOps12RecoveryDiagnostics({
      lines: ops12MarkedUnknownState.ops12RecoveryDiagnosticLines, count: ops12MarkedUnknownState.ops12RecoveryDiagnosticCount,
      bytes: ops12MarkedUnknownState.ops12RecoveryDiagnosticBytes,
      diagnosticsComplete: ops12MarkedUnknownState.ops12RecoveryDiagnosticsComplete
    });
    assert.equal(parsedMarkedUnknown.complete, true);
    assert.equal(parsedMarkedUnknown.records[0].caseId, 'unknown');
    assert.equal(parsedMarkedUnknown.records[0].exitDisposition, 'nonzero');
    assert.equal(parsedMarkedUnknown.records[0].exitCode, 7);
    assert.doesNotMatch(parsedMarkedUnknown.records[0].caseId, /private|canary/u);

    const ops12Failure = {
      schema: 'agent-harness.ah11-ops12-recovery-failure.v1',
      caseId: 'unknown_hold_restart_escalation', stage: 'hold_verify',
      errorClass: 'assertion', nativePrimaryCode: null
    };
    const ops12FailureLine = `AH11_OPS_OPS12_RECOVERY_FAILURE ${JSON.stringify(ops12Failure)}\n`;
    assert.deepEqual(parseOps12RecoveryFailure(ops12FailureLine), ops12Failure);
    const ops12Diagnostic = {
      ...ops12Failure, schema: 'agent-harness.ah11-ops12-recovery-child.v1',
      exitDisposition: 'nonzero', exitCode: 1, exitSignal: null
    };
    const ops12DiagnosticLine = `AH11_OPS_OPS12_RECOVERY_DIAGNOSTIC ${JSON.stringify(ops12Diagnostic)}\n`;
    const ops12Parsed = parseOps12RecoveryDiagnostics({
      lines: ops12DiagnosticLine, count: 1,
      bytes: Buffer.byteLength(ops12DiagnosticLine, 'utf8'), diagnosticsComplete: true
    });
    assert.equal(ops12Parsed.complete, true);
    assert.equal(ops12Parsed.records[0].caseId, 'unknown_hold_restart_escalation');
    assert.doesNotMatch(JSON.stringify(ops12Parsed), /private details|SYNTHETIC_PRIVATE_DIAGNOSTIC_CANARY_96d/u);
    const ops12Success = {
      schema: 'agent-harness.ah11-ops12-recovery-child.v1', caseId: 'none', stage: 'none',
      errorClass: 'none', nativePrimaryCode: null, exitDisposition: 'zero', exitCode: 0, exitSignal: null
    };
    const ops12SuccessLine = `AH11_OPS_OPS12_RECOVERY_DIAGNOSTIC ${JSON.stringify(ops12Success)}\n`;
    assert.equal(parseOps12RecoveryDiagnostics({
      lines: ops12SuccessLine, count: 1,
      bytes: Buffer.byteLength(ops12SuccessLine, 'utf8'), diagnosticsComplete: true
    }).complete, true);
    assert.match(evidenceVerifier, /missingLocalCases/u);
    assert.match(evidenceVerifier, /OPS_CI1_OWNED_GROUP_REGISTRY/u);
    assert.match(evidenceVerifier, /validatedCi1RegistryPathFromEnvironment/u);
    assert.match(evidenceVerifier, /childEnvironment\[OPS_CI1_OWNED_GROUP_REGISTRY\]/u);
    assert.match(evidenceVerifier, /processCloseElapsedMs/u);
    assert.match(evidenceVerifier, /runnerDeadlineCloseElapsedMsMax/u);
    assert.match(evidenceVerifier, /resourceHighWaterSamples/u);
    const workflow = readFileSync(join(ROOT, '.github/workflows/runtime-foundation.yml'), 'utf8');
    assert.match(workflow, /include-hidden-files: true/u);
    assert.match(workflow, /if-no-files-found: error/u);
    assert.doesNotMatch(workflow, /AH-11-OPS-\*/u);
    assert.match(evidenceVerifier, /EXPECTED_UPLOAD_MEMBERS/u);
    assert.match(evidenceVerifier, /uploadUsesExactAllowlist/u);
    assert.match(evidenceVerifier, /requiredEvidenceSetComplete/u);
    assert.match(evidenceVerifier, /diagnosticsIncludedInUpload/u);
    assert.match(evidenceVerifier, /failure-diagnostics\.js/u);
    const tapArtifactWrite = evidenceVerifier.indexOf("const tapArtifact = writeArtifact(join(artifactDirectory, `${stem}.tap`)");
    const captureParse = evidenceVerifier.indexOf('const ops07CaptureResult = parseOps07Capture(tapText)');
    assert.ok(tapArtifactWrite >= 0 && captureParse > tapArtifactWrite,
      'the sanitized TAP artifact is retained before capture validation can fail');
    assert.match(evidenceVerifier, /function parseOps07Capture\(text\)[\s\S]*?catch \{\s*return \{ valid: false, reason: 'invalid_shape', capture: null \};\s*\}/u,
      'unexpected parser failure becomes a structured invalid capture, not fixture success');
    assert.match(evidenceVerifier, /ops07Capture: ops07CaptureResult\.valid\s*\?\s*\{ valid: true, capture: ops07CaptureResult\.capture \}\s*:\s*\{ valid: false, reason: ops07CaptureResult\.reason \}/u);
    assert.match(evidenceVerifier, /&& ops07CaptureResult\.valid && ops07CaptureResult\.capture\.flags\.complete/u,
      'invalid or incomplete capture prevents fixture acceptance');
    const registryProbeDirectory = tempDirectory('ah11-ops-registry-forward-control-');
    t.after(() => registryProbeDirectory.cleanup());
    const registryProbePath = join(registryProbeDirectory.directory, 'owned-groups.json');
    writeFileSync(registryProbePath, JSON.stringify({
      schema: 'ops-ci1-owned-groups.v1', groups: [], pendingLaunches: []
    }), { mode: 0o600 });
    const verifierPath = join(ROOT, 'tests/operations/verify-fixture.js');
    const forwardedRegistry = spawnSync(process.execPath, [verifierPath, '--registry-propagation-probe'], {
      cwd: ROOT, encoding: 'utf8', timeout: 5_000, maxBuffer: 4_096,
      env: { ...baseEnvironment(), OPS_CI1_OWNED_GROUP_REGISTRY: registryProbePath }
    });
    assert.equal(forwardedRegistry.status, 0, forwardedRegistry.stderr);
    assert.equal(forwardedRegistry.signal, null);
    assert.equal(forwardedRegistry.stdout, 'forwarded',
      'the validated registry path reaches the verifier child through its explicit allowlist without printing the path');
    const missingRegistry = spawnSync(process.execPath, [verifierPath, '--registry-propagation-probe'], {
      cwd: ROOT, encoding: 'utf8', timeout: 5_000, maxBuffer: 4_096,
      env: baseEnvironment()
    });
    assert.equal(missingRegistry.status, 1);
    assert.match(missingRegistry.stderr, /ownership registry is missing or invalid/u);
    const malformedRegistryPath = join(registryProbeDirectory.directory, 'malformed-owned-groups.json');
    writeFileSync(malformedRegistryPath, '{truncated', { mode: 0o600 });
    const malformedRegistry = spawnSync(process.execPath, [verifierPath, '--registry-propagation-probe'], {
      cwd: ROOT, encoding: 'utf8', timeout: 5_000, maxBuffer: 4_096,
      env: { ...baseEnvironment(), OPS_CI1_OWNED_GROUP_REGISTRY: malformedRegistryPath }
    });
    assert.equal(malformedRegistry.status, 1);
    assert.match(malformedRegistry.stderr, /ownership registry is missing or invalid/u);

    let registeredFinalizer;
    let finalizerCleanupCalls = 0;
    const finalizerState = {
      ci1PhaseCaptureIncomplete: false,
      ci1PhaseCaptureOwnedGroupCloseUnconfirmed: false
    };
    registerCi1OwnedTemporaryDirectoryFinalizer({ after(callback) { registeredFinalizer = callback; } }, async () => {
      finalizerCleanupCalls += 1;
      return false;
    }, { directory: 'private' }, finalizerState);
    await assert.rejects(registeredFinalizer(), error =>
      error?.message === 'AH11_OPS_CI1_TEMP_CUSTODY_CLEANUP_UNCONFIRMED');
    assert.equal(finalizerCleanupCalls, 1, 'an after-hook attempts its cleanup exactly once');
    assert.equal(finalizerState.ci1PhaseCaptureIncomplete, true);
    assert.equal(finalizerState.ci1PhaseCaptureOwnedGroupCloseUnconfirmed, true);

    const finalizerDirectory = tempDirectory('ah11-ops-finalizer-control-');
    t.after(() => finalizerDirectory.cleanup());
    const finalizerHarnessPath = join(finalizerDirectory.directory, 'finalizer-control.mjs');
    const finalizerHarness = [
      "import { test } from 'node:test';",
      `const registerFinalizer = (${registerCi1OwnedTemporaryDirectoryFinalizer.toString()});`,
      'const mode = process.env.CI1_FINALIZER_CONTROL_MODE;',
      'const state = { ci1PhaseCaptureIncomplete: false, ci1PhaseCaptureOwnedGroupCloseUnconfirmed: false };',
      "test('controlled temporary cleanup finalizer', async t => {",
      '  registerFinalizer(t, async () => mode === \'success\', { directory: \'private\' }, state);',
      "  if (mode === 'primary') throw new Error('PRIMARY_TEST_FAILURE_SENTINEL');",
      '});'
    ].join('\n');
    writeFileSync(finalizerHarnessPath, finalizerHarness, { mode: 0o600 });
    const runFinalizerControl = mode => spawnSync(process.execPath, [
      '--test', '--test-reporter=tap', finalizerHarnessPath
    ], {
      cwd: ROOT, encoding: 'utf8', timeout: 5_000, maxBuffer: 8_192,
      env: { ...baseEnvironment(), CI1_FINALIZER_CONTROL_MODE: mode }
    });
    const finalizerSuccess = runFinalizerControl('success');
    assert.equal(finalizerSuccess.status, 0, 'confirmed cleanup leaves the controlled test green');
    assert.equal(finalizerSuccess.signal, null);
    const finalizerFailure = runFinalizerControl('failure');
    assert.equal(finalizerFailure.status, 1, 'an unconfirmed after-hook makes the test command fail');
    assert.equal(finalizerFailure.signal, null);
    const finalizerFailureOutput = `${finalizerFailure.stdout}\n${finalizerFailure.stderr}`;
    assert.equal(/AH11_OPS_CI1_TEMP_CUSTODY_CLEANUP_UNCONFIRMED/u.test(finalizerFailureOutput), true,
      'cleanup failure output includes the fixed custody error');
    const finalizerPrimaryFailure = runFinalizerControl('primary');
    assert.equal(finalizerPrimaryFailure.status, 1);
    assert.equal(finalizerPrimaryFailure.signal, null);
    const finalizerPrimaryOutput = `${finalizerPrimaryFailure.stdout}\n${finalizerPrimaryFailure.stderr}`;
    assert.equal(/PRIMARY_TEST_FAILURE_SENTINEL/u.test(finalizerPrimaryOutput), true,
      'cleanup failure does not erase the primary test failure');
    assert.equal(/AH11_OPS_CI1_TEMP_CUSTODY_CLEANUP_UNCONFIRMED/u.test(finalizerPrimaryOutput), true,
      'the fixed cleanup diagnostic remains visible even though Node suppresses the secondary after-hook error');

    const custodyState = () => ({
      ci1PhaseCaptureIncomplete: false,
      ci1PhaseCaptureOwnedGroupCloseUnconfirmed: false
    });
    const custodyRecord = (directory, overrides = {}) => ({
      directory,
      ci1Custody: {
        directory,
        sidecarPath: null,
        launches: new Map([['synthetic-token', {
          launchToken: 'synthetic-token', status: 'group_registered', spawnAttempted: true,
          directPid: 81_234, groupId: 81_234, registryRegistered: true,
          registryCleared: false, groupAbsent: false, ...overrides
        }]])
      }
    });
    const spawnAttemptControl = custodyRecord('/synthetic/ops-temp-spawn-attempt', {
      status: 'launch_intent', spawnAttempted: false, groupId: null, directPid: null,
      registryRegistered: false, registryCleared: false, groupAbsent: false
    });
    assert.equal(markCi1OwnedGroupSpawnAttempt(
      spawnAttemptControl.ci1Custody, 'synthetic-token', () => false
    ), false);
    assert.equal(spawnAttemptControl.ci1Custody.launches.get('synthetic-token').spawnAttempted, false,
      'a failed pre-spawn custody write does not misclassify an unstarted child as possibly spawned');
    assert.equal(spawnAttemptControl.ci1Custody.launches.get('synthetic-token').status, 'launch_intent');
    const cleanupOrder = [];
    const allAbsentState = custodyState();
    const allAbsent = custodyRecord('/synthetic/ops-temp-clean');
    assert.equal(await cleanupCi1OwnedTemporaryDirectory(allAbsent, {
      state: allAbsentState,
      probeGroupEmpty: async groupId => { cleanupOrder.push(`probe:${groupId}`); return true; },
      removeRegistryLaunch: (groupId, token) => {
        cleanupOrder.push(`unregister:${groupId}:${token}`); return true;
      },
      persist: () => true,
      removeDirectory: directory => cleanupOrder.push(`remove:${directory}`)
    }), true);
    assert.deepEqual(cleanupOrder, [
      'probe:81234', 'unregister:81234:synthetic-token', 'remove:/synthetic/ops-temp-clean'
    ], 'the directory is removed only after group absence and registry removal are confirmed');
    const retainedState = custodyState();
    const retained = custodyRecord('/synthetic/ops-temp-retained');
    let retainedRemoved = false;
    assert.equal(await cleanupCi1OwnedTemporaryDirectory(retained, {
      state: retainedState, probeGroupEmpty: async () => false,
      persist: () => true, removeDirectory: () => { retainedRemoved = true; }
    }), false);
    assert.equal(retainedRemoved, false);
    assert.equal(retainedState.ci1PhaseCaptureIncomplete, true);
    assert.equal(retainedState.ci1PhaseCaptureOwnedGroupCloseUnconfirmed, true);
    assert.equal(retained.ci1Custody.launches.get('synthetic-token').status, 'group_unconfirmed');
    const unknownState = custodyState();
    const unknown = custodyRecord('/synthetic/ops-temp-unknown');
    let unknownRemoved = false;
    assert.equal(await cleanupCi1OwnedTemporaryDirectory(unknown, {
      state: unknownState, probeGroupEmpty: async () => { throw new Error('unreadable process table'); },
      persist: () => true, removeDirectory: () => { unknownRemoved = true; }
    }), false);
    assert.equal(unknownRemoved, false);
    assert.equal(unknownState.ci1PhaseCaptureOwnedGroupCloseUnconfirmed, true,
      'an unreadable group probe remains unknown and retains the exact directory');
    const pendingState = custodyState();
    const pending = custodyRecord('/synthetic/ops-temp-pending', {
      status: 'launch_intent', spawnAttempted: false, directPid: null, groupId: null,
      registryRegistered: false, registryCleared: false, groupAbsent: false
    });
    let pendingRemoved = false;
    assert.equal(await cleanupCi1OwnedTemporaryDirectory(pending, {
      state: pendingState, cancelPendingLaunch: token => token === 'synthetic-token',
      persist: () => true, removeDirectory: () => { pendingRemoved = true; }
    }), true);
    assert.equal(pendingRemoved, true, 'a launch proven not to have spawned can be cancelled before directory removal');
    const pendingPersistFailureState = custodyState();
    const pendingPersistFailure = custodyRecord('/synthetic/ops-temp-pending-persist-failure', {
      status: 'launch_intent', spawnAttempted: false, directPid: null, groupId: null,
      registryRegistered: false, registryCleared: false, groupAbsent: false
    });
    let pendingPersistFailureRemoved = false;
    let pendingCancelCalls = 0;
    let pendingPersistCalls = 0;
    const pendingCleanupOverrides = {
      state: pendingPersistFailureState,
      cancelPendingLaunch: token => { pendingCancelCalls += 1; return token === 'synthetic-token'; },
      persist: () => { pendingPersistCalls += 1; return pendingPersistCalls !== 1; },
      removeDirectory: () => { pendingPersistFailureRemoved = true; }
    };
    assert.equal(await cleanupCi1OwnedTemporaryDirectory(pendingPersistFailure, pendingCleanupOverrides), false);
    assert.equal(pendingPersistFailureRemoved, false,
      'a failed custody update after pending-token cancellation retains the exact directory');
    assert.equal(pendingPersistFailure.ci1Custody.launches.get('synthetic-token').registryCleared, true,
      'a retry knows the pending token was already cancelled');
    assert.equal(await cleanupCi1OwnedTemporaryDirectory(pendingPersistFailure, pendingCleanupOverrides), true);
    assert.equal(pendingPersistFailureRemoved, true, 'retry removes the directory after the custody update succeeds');
    assert.equal(pendingCancelCalls, 1, 'the same pending launch is not cancelled twice on retry');

    const retryDirectory = tempDirectory('ah11-ops-temp-custody-retry-', { ci1OwnedGroups: true });
    const retryRegistryPath = join(retryDirectory.directory, 'owned-groups.json');
    const retryState = custodyState();
    const withRetryRegistry = async callback => {
      const previousRegistryPath = process.env.OPS_CI1_OWNED_GROUP_REGISTRY;
      process.env.OPS_CI1_OWNED_GROUP_REGISTRY = retryRegistryPath;
      try { return await callback(); } finally {
        if (previousRegistryPath === undefined) delete process.env.OPS_CI1_OWNED_GROUP_REGISTRY;
        else process.env.OPS_CI1_OWNED_GROUP_REGISTRY = previousRegistryPath;
      }
    };
    const cleanupRetryDirectory = (temporary, overrides = {}) =>
      withRetryRegistry(() => cleanupCi1OwnedTemporaryDirectory(temporary, overrides));
    registerCi1OwnedTemporaryDirectoryFinalizer(t, cleanupRetryDirectory, retryDirectory, retryState);
    await withRetryRegistry(async () => {
      writeFileSync(retryRegistryPath, JSON.stringify({
        schema: 'ops-ci1-owned-groups.v1', groups: [], pendingLaunches: []
      }), { mode: 0o600 });
      const retryToken = randomUUID();
      assert.equal(beginCi1OwnedGroupLaunch(retryToken, retryDirectory.ci1Custody), true,
        'the real sidecar and pending token exist before cleanup begins');
      const retainedSidecarPath = retryDirectory.ci1Custody.sidecarPath;
      assert.equal(existsSync(retainedSidecarPath), true);
      let persistCalls = 0;
      let cancelCalls = 0;
      const persistWithOneFailure = custody => {
        persistCalls += 1;
        return persistCalls === 1 ? false : writeCi1TemporaryCustody(custody);
      };
      const firstCleanup = await cleanupCi1OwnedTemporaryDirectory(retryDirectory, {
        state: retryState,
        cancelPendingLaunch: token => {
          cancelCalls += 1;
          return cancelCi1OwnedGroupLaunch(token);
        },
        persist: persistWithOneFailure
      });
      assert.equal(firstCleanup, false, 'a failed sidecar update keeps cleanup explicitly incomplete');
      assert.equal(retryState.ci1PhaseCaptureIncomplete, true);
      assert.equal(retryState.ci1PhaseCaptureOwnedGroupCloseUnconfirmed, true);
      assert.equal(persistCalls, 2, 'the failed update is followed by a bounded handoff write attempt');
      assert.equal(cancelCalls, 1);
      assert.equal(existsSync(retryDirectory.directory), true,
        'the exact temporary directory remains for the outer owner');
      assert.equal(existsSync(retainedSidecarPath), true,
        'the exact sidecar remains available for outer custody');
      const retainedCustody = JSON.parse(readFileSync(retainedSidecarPath, 'utf8'));
      assert.equal(retainedCustody.schema, CI1_TEMP_CUSTODY_SCHEMA);
      assert.equal(retainedCustody.launches.length, 1);
      assert.equal(retainedCustody.launches[0].spawnAttempted, false);
      assert.equal(retainedCustody.launches[0].registryCleared, true);
      assert.deepEqual(readCi1OwnedGroupRegistry(retryRegistryPath), {
        schema: 'ops-ci1-owned-groups.v1', groups: [], pendingLaunches: []
      }, 'the already-cancelled pending token is not mistaken for a spawned process');
      assert.equal(await cleanupCi1OwnedTemporaryDirectory(retryDirectory, { state: retryState }), true,
        'a later confirmed retry completes cleanup without cancelling twice');
      assert.equal(cancelCalls, 1);
      assert.equal(existsSync(retryDirectory.directory), false);
      assert.equal(existsSync(retainedSidecarPath), false);
    });

    const registryFailureState = custodyState();
    const registryFailure = custodyRecord('/synthetic/ops-temp-registry-failure');
    let registryFailureRemoved = false;
    assert.equal(await cleanupCi1OwnedTemporaryDirectory(registryFailure, {
      state: registryFailureState, probeGroupEmpty: async () => true,
      removeRegistryLaunch: () => false, persist: () => true,
      removeDirectory: () => { registryFailureRemoved = true; }
    }), false);
    assert.equal(registryFailureRemoved, false,
      'confirmed group absence alone does not discard a directory while registry ownership remains unresolved');
    const persistFailureState = custodyState();
    const persistFailure = custodyRecord('/synthetic/ops-temp-persist-failure');
    let persistFailureRemoved = false;
    assert.equal(await cleanupCi1OwnedTemporaryDirectory(persistFailure, {
      state: persistFailureState, probeGroupEmpty: async () => true,
      removeRegistryLaunch: () => true, persist: () => false,
      removeDirectory: () => { persistFailureRemoved = true; }
    }), false);
    assert.equal(persistFailureRemoved, false,
      'failed bounded handoff persistence retains the directory for the outer owner');
    const controls = [
      ['assertion', 'ASSERTION_FAILURE'],
      ['busy', 'SQLITE_BUSY'],
      ['runtime-busy', 'SQLITE_BUSY'],
      ['node-busy', 'SQLITE_BUSY'],
      ['node-locked', 'SQLITE_BUSY'],
      ['node-unsupported', 'UNCLASSIFIED_FAILURE'],
      ['wrapped-runtime-busy', 'ASSERTION_FAILURE'],
      ['parse', 'JSON_PARSE'],
      ['timeout', 'CHILD_TIMEOUT']
    ];
    const diagnosticsByMode = new Map();
    for (const [mode, expectedCode] of controls) {
      const result = spawnSync(process.execPath, [
        join(ROOT, 'tests/operations/fixtures/failure-diagnostic-child.mjs'), mode
      ], {
        cwd: ROOT,
        encoding: 'utf8',
        timeout: mode === 'timeout' ? 1_000 : 5_000,
        maxBuffer: 64 * 1024
      });
      assert.ok(result.status !== 0 || result.signal !== null || result.error, `${mode} control must fail`);
      const diagnostic = buildSafeFailureDiagnostics({
        scenario: 'OPS13.C01',
        phase: 'diagnostic-control',
        stage: 'node-test-runner',
        status: result.status,
        signal: result.signal,
        error: result.error,
        stdout: result.stdout,
        stderr: result.stderr
      });
      assert.equal(diagnostic.records.length, 1, mode);
      assert.equal(diagnostic.records[0].failureCode, expectedCode, mode);
      assert.deepEqual(diagnostic.records[0].assertionContext, mode === 'timeout'
        ? { actualValueKind: null, expectedValueKind: null }
        : { actualValueKind: 'string', expectedValueKind: 'string' }, mode);
      assert.equal(diagnostic.records[0].sourceLocation,
        mode === 'timeout' ? 'redacted' : 'tests/operations/operations.test.js:4400:9', mode);
      const safeBytes = JSON.stringify(diagnostic);
      assert.ok(Buffer.byteLength(safeBytes, 'utf8') <= 2_048, mode);
      assert.doesNotMatch(safeBytes, /SYNTHETIC_PRIVATE_DIAGNOSTIC_CANARY_96d|file:\/\/|\/Users\//u, mode);
      diagnosticsByMode.set(mode, diagnostic.records[0]);
    }
    assert.equal(diagnosticsByMode.get('runtime-busy').errorClass, 'RuntimeError');
    assert.equal(diagnosticsByMode.get('runtime-busy').errorCode, 'STORE_BUSY');
    assert.equal(diagnosticsByMode.get('node-busy').sqlitePrimaryCode, 5);
    assert.equal(diagnosticsByMode.get('node-locked').sqlitePrimaryCode, 6);
    assert.equal(diagnosticsByMode.get('node-unsupported').sqlitePrimaryCode, null);
    assert.deepEqual(diagnosticsByMode.get('wrapped-runtime-busy').nestedStorageCause, {
      failureCode: 'SQLITE_BUSY', errorClass: 'RuntimeError',
      errorCode: 'STORE_BUSY', sqlitePrimaryCode: null
    });
    assert.equal(diagnosticsByMode.get('wrapped-runtime-busy').failureCode, 'ASSERTION_FAILURE');
    const overlengthLocation = `file:///synthetic/private/${'x'.repeat(220)}.mjs:4400:9`;
    const overlengthLocationDiagnostics = buildSafeFailureDiagnostics({
      scenario: 'OPS13.C01', phase: 'diagnostic-control', stage: 'node-test-runner', status: 1,
      stdout: `not ok 1 - OPS13.C01 overlength location control\n  ---\n  failureType: 'testCodeFailure'\n  location: '${overlengthLocation}'\n  name: 'AssertionError'\n  code: 'ERR_ASSERTION'\n  ...\n`
    });
    assert.equal(overlengthLocationDiagnostics.records.length, 1);
    assert.equal(overlengthLocationDiagnostics.records[0].sourceLocation, 'redacted');
    assert.doesNotMatch(JSON.stringify(overlengthLocationDiagnostics), /synthetic|private|file:\/\//u);

    const ordinaryChild = spawnSync(process.execPath, [
      '--test', '--test-reporter=tap',
      join(ROOT, 'tests/operations/fixtures/ordinary-diagnostic-child.mjs')
    ], { cwd: ROOT, env: baseEnvironment(), encoding: 'utf8', timeout: 5_000, maxBuffer: 64 * 1024 });
    assert.equal(ordinaryChild.status, 1);
    assert.equal(ordinaryChild.signal, null);
    const ordinaryDiagnostics = buildSafeFailureDiagnostics({
      scenario: 'OPS13.C01', phase: 'diagnostic-control', stage: 'node-test-runner',
      status: ordinaryChild.status, signal: ordinaryChild.signal, error: ordinaryChild.error,
      stdout: ordinaryChild.stdout, stderr: ordinaryChild.stderr
    });
    assert.equal(ordinaryDiagnostics.records.length, 3);
    assert.ok(ordinaryDiagnostics.records.every(record => record.failureCode === 'ASSERTION_FAILURE'));
    assert.ok(ordinaryDiagnostics.records.every(record => record.errorClass === 'AssertionError'));
    assert.ok(ordinaryDiagnostics.records.every(record => record.errorCode === 'ERR_ASSERTION'));
    assert.ok(ordinaryDiagnostics.records.every(record => record.nestedStorageCause === null));
    assert.ok(Buffer.byteLength(JSON.stringify(ordinaryDiagnostics), 'utf8') <= 2_048);

    const makeCaptureSlot = slot => ({
      slot,
      records: [
        { sequence: 1, stage: 'header', elapsedMs: 0 },
        { sequence: 2, stage: 'footer', elapsedMs: 0,
          writerComplete: true, overflow: false, writeFailed: false, malformed: false }
      ],
      malformed: false, overflow: false, footer: true, writerComplete: true,
      footerFlags: { overflow: false, writeFailed: false, malformed: false }
    });
    const makeCapture = () => ({
      schema: 'agent-harness.ops07-session.v1',
      slots: [makeCaptureSlot(0), makeCaptureSlot(1)], supervisor: [],
      finalState: { available: false },
      flags: { enabled: true, complete: false, missingChildCapture: false, malformed: false,
        overflow: false, writeFailed: false, abruptExit: false, cleanupIncomplete: true }
    });
    const captureLine = capture => `${OPS07_CAPTURE_LINE_PREFIX}${JSON.stringify(capture)}`;
    const validCaptureLine = captureLine(makeCapture());
    const parsedIndentedCapture = parseOperationsCaptureFromTap(`    ${validCaptureLine}\n`);
    assert.equal(parsedIndentedCapture.valid, true, 'nested TAP diagnostics retain their indentation');
    assert.equal(parsedIndentedCapture.capture.flags.complete, false, 'capture validity is distinct from completeness');
    assert.equal(parseOperationsCaptureFromTap(`${' '.repeat(17)}${validCaptureLine}\n`).reason, 'missing',
      'capture indentation remains bounded');
    assert.equal(parseOperationsCaptureFromTap('TAP version 13\n').reason, 'missing');
    assert.equal(parseOperationsCaptureFromTap(`${validCaptureLine}\n${validCaptureLine}\n`).reason, 'duplicate');
    assert.equal(parseOperationsCaptureFromTap(`${OPS07_CAPTURE_LINE_PREFIX}{\n`).reason, 'malformed');
    const maxWireBytes = OPS07_CAPTURE_LIMITS.totalBytes;
    const prefixBytes = Buffer.byteLength(OPS07_CAPTURE_LINE_PREFIX, 'utf8');
    const basePayload = JSON.stringify(makeCapture());
    const exactPadding = maxWireBytes - prefixBytes - 1 - Buffer.byteLength(basePayload, 'utf8');
    const exactBoundaryPayload = `${basePayload.slice(0, -1)}${' '.repeat(exactPadding)}}`;
    assert.equal(Buffer.byteLength(`${OPS07_CAPTURE_LINE_PREFIX}${exactBoundaryPayload}\n`, 'utf8'), maxWireBytes);
    assert.equal(parseOperationsCaptureFromTap(`${OPS07_CAPTURE_LINE_PREFIX}${exactBoundaryPayload}\n`).valid, true);
    assert.equal(parseOperationsCaptureFromTap(`${OPS07_CAPTURE_LINE_PREFIX}${exactBoundaryPayload} \n`).reason, 'oversize');
    const overflowSummary = {
      schema: 'agent-harness.ops07-session.v1',
      overflowSummary: true,
      flags: { enabled: true, complete: false, missingChildCapture: false, malformed: false,
        overflow: true, writeFailed: false, abruptExit: false, cleanupIncomplete: false }
    };
    const overflowSummaryLine = captureLine(overflowSummary);
    const parsedOverflowSummary = parseOperationsCaptureFromTap(`${overflowSummaryLine}\n`);
    assert.equal(parsedOverflowSummary.valid, true);
    assert.equal(parsedOverflowSummary.capture.overflowSummary, true);
    assert.equal(parsedOverflowSummary.capture.flags.complete, false);
    assert.ok(Buffer.byteLength(`${overflowSummaryLine}\n`, 'utf8') <= maxWireBytes);
    const falseOverflowSummary = structuredClone(overflowSummary);
    falseOverflowSummary.flags.overflow = false;
    assert.equal(parseOperationsCaptureFromTap(`${captureLine(falseOverflowSummary)}\n`).reason, 'invalid_shape');
    const outputBoundDir = tempDirectory('ah11-ops-output-bound-');
    const outputBoundSession = createOperationsDiagnosticSession({ directory: outputBoundDir.directory });
    t.after(() => {
      disposeOperationsDiagnosticSession(outputBoundSession);
      outputBoundDir.cleanup();
    });
    for (let slot = 0; slot < 2; slot += 1) {
      const child = prepareOperationsChildCapture(outputBoundSession);
      assert.equal(child.slot, slot);
      const childRecords = [{
        schema: 'agent-harness.ops07-child-record.v1', sequence: 1, stage: 'header', elapsedMs: 0
      }];
      for (let sequence = 2; sequence < OPS07_CAPTURE_LIMITS.childRecords; sequence += 1) {
        childRecords.push({
          schema: 'agent-harness.ops07-child-record.v1', sequence, stage: 'runner_phase', elapsedMs: sequence,
          phase: 'loop_snapshot'
        });
      }
      childRecords.push({
        schema: 'agent-harness.ops07-child-record.v1', sequence: OPS07_CAPTURE_LIMITS.childRecords,
        stage: 'footer', elapsedMs: OPS07_CAPTURE_LIMITS.childRecords,
        writerComplete: true, overflow: false, writeFailed: false, malformed: false
      });
      const childBytes = Buffer.from(`${childRecords.map(record => JSON.stringify(record)).join('\n')}\n`, 'utf8');
      assert.ok(childBytes.byteLength <= OPS07_CAPTURE_LIMITS.childBytes);
      assert.equal(writeSync(child.fd, childBytes), childBytes.byteLength);
      assert.equal(closeOperationsChildCapture(outputBoundSession, slot), true);
    }
    for (let index = 0; index < OPS07_CAPTURE_LIMITS.supervisorRecords; index += 1) {
      assert.equal(recordOperationsSupervisor(outputBoundSession, index % 2, 'child_exit', {
        exitCode: 255, signal: 'SIGKILL', childRuntimeMs: 600_000,
        generation: Number.MAX_SAFE_INTEGER, fence: Number.MAX_SAFE_INTEGER
      }), true);
    }
    const maximumScalar = Number.MAX_SAFE_INTEGER;
    const maximumFinalState = {
      available: true,
      jobs: Array.from({ length: 2 }, () => ({
        workState: 'completed', workflowState: 'succeeded', generation: maximumScalar,
        operationFence: maximumScalar, workflowFence: maximumScalar, revision: maximumScalar,
        checkpointSequence: maximumScalar, resultCount: 32, resultHash: 'f'.repeat(64),
        cancellationRequested: false, leaseOwnerPresent: false, leaseFenceMatches: true,
        leaseExpired: true,
        eventCounts: Array.from({ length: 16 }, () => ({ type: 'private_local_step_completed', count: maximumScalar }))
      })),
      reservations: { reservedDiskBytes: maximumScalar, reservedEventSlots: maximumScalar, pendingRecords: maximumScalar }
    };
    const boundedOutput = readOperationsDiagnosticSession(outputBoundSession, {
      finalState: maximumFinalState, abruptSlots: []
    });
    assert.equal(boundedOutput.overflowSummary, true,
      'the collector must choose its bounded overflow form for an over-limit full capture');
    const boundedOutputLine = `${OPS07_CAPTURE_LINE_PREFIX}${JSON.stringify(boundedOutput)}\n`;
    assert.ok(Buffer.byteLength(boundedOutputLine, 'utf8') <= OPS07_CAPTURE_LIMITS.totalBytes,
      'the collector must return bounded output even when the full capture exceeds its budget');
    if (boundedOutput.overflowSummary) {
      const parsedBoundedOutput = parseOperationsCaptureFromTap(`${boundedOutputLine}`);
      assert.equal(parsedBoundedOutput.valid, true);
      assert.equal(parsedBoundedOutput.capture.flags.overflow, true);
      assert.equal(parsedBoundedOutput.capture.flags.complete, false);
    }
    const unorderedCapture = makeCapture();
    unorderedCapture.slots[0].records[1].sequence = 3;
    assert.equal(parseOperationsCaptureFromTap(`${captureLine(unorderedCapture)}\n`).reason, 'invalid_shape');
    const maxRecordCapture = makeCapture();
    for (const slot of maxRecordCapture.slots) {
      slot.records = [{ sequence: 1, stage: 'header', elapsedMs: 0 }];
      for (let sequence = 2; sequence < OPS07_CAPTURE_LIMITS.childRecords; sequence += 1) {
        slot.records.push({ sequence, stage: 'runner_phase', elapsedMs: 0, phase: 'controller' });
      }
      slot.records.push({ sequence: OPS07_CAPTURE_LIMITS.childRecords, stage: 'footer', elapsedMs: 0,
        writerComplete: true, overflow: false, writeFailed: false, malformed: false });
    }
    maxRecordCapture.supervisor = Array.from({ length: 99 }, (_, index) => ({
      schema: 'agent-harness.ops07-session.v1', sequence: index + 1, stage: 'spawn', slot: 0,
      elapsedMs: 0, pidPresent: true, generation: 1, fence: 1
    }));
    const parsedMaximumRecords = parseOperationsCaptureFromTap(`${captureLine(maxRecordCapture)}\n`);
    assert.equal(parsedMaximumRecords.valid, true, 'the 300-record aggregate boundary is accepted');
    assert.equal(parsedMaximumRecords.capture.slots.reduce((sum, slot) => sum + slot.records.length, 0)
      + parsedMaximumRecords.capture.supervisor.length + 1, 300);
    maxRecordCapture.supervisor.push({
      schema: 'agent-harness.ops07-session.v1', sequence: 100, stage: 'spawn', slot: 0,
      elapsedMs: 0, pidPresent: true, generation: 1, fence: 1
    });
    assert.equal(parseOperationsCaptureFromTap(`${captureLine(maxRecordCapture)}\n`).reason, 'invalid_shape');
    const makeCompleteCapture = () => ({
      schema: 'agent-harness.ops07-session.v1',
      slots: [0, 1].map(slot => ({
        slot,
        records: [
          { sequence: 1, stage: 'header', elapsedMs: 0 },
          { sequence: 2, stage: 'runner_error', elapsedMs: 1, phase: 'runner_start',
            errorKind: 'OTHER', code: 'OTHER', primaryCode: null },
          { sequence: 3, stage: 'footer', elapsedMs: 2,
            writerComplete: true, overflow: false, writeFailed: false, malformed: false }
        ],
        malformed: false, overflow: false, footer: true, writerComplete: true,
        footerFlags: { overflow: false, writeFailed: false, malformed: false }
      })),
      supervisor: [0, 1].flatMap(slot => {
        const base = slot * 3;
        return [
          { schema: 'agent-harness.ops07-session.v1', sequence: base + 1, stage: 'spawn', slot,
            elapsedMs: base + 1, pidPresent: true, generation: 1, fence: 1 },
          { schema: 'agent-harness.ops07-session.v1', sequence: base + 2, stage: 'child_exit', slot,
            elapsedMs: base + 2, exitCode: 2, signal: null, childRuntimeMs: 1, generation: 1, fence: 1 },
          { schema: 'agent-harness.ops07-session.v1', sequence: base + 3, stage: 'finish_result', slot,
            elapsedMs: base + 3, outcome: 'stopped', settled: true, code: null, generation: 1, fence: 1 }
        ];
      }),
      finalState: {
        available: true,
        jobs: Array.from({ length: 2 }, () => ({
          workState: 'completed', workflowState: 'succeeded', generation: 1, operationFence: 1,
          workflowFence: 1, revision: 1, checkpointSequence: 1, resultCount: 0, resultHash: null,
          cancellationRequested: false, leaseOwnerPresent: false, leaseFenceMatches: true,
          leaseExpired: true, eventCounts: []
        })),
        reservations: { reservedDiskBytes: 0, reservedEventSlots: 0, pendingRecords: 0 }
      },
      flags: { enabled: true, complete: true, missingChildCapture: false, malformed: false,
        overflow: false, writeFailed: false, abruptExit: false, cleanupIncomplete: false }
    });
    assert.equal(parseOperationsCaptureFromTap(`${captureLine(makeCompleteCapture())}\n`).capture.flags.complete, true);
    const invalidStages = [
      ['unknown string', 'unknown_stage'], ['prototype key', '__proto__'], ['constructor key', 'constructor'],
      ['toString key', 'toString'], ['null', null], ['number', 17], ['boolean', false], ['array', []],
      ['object with null toString', { toString: null }]
    ];
    for (const [label, stage] of invalidStages) {
      for (const target of ['child', 'supervisor']) {
        const malformedStageCapture = makeCompleteCapture();
        if (target === 'child') malformedStageCapture.slots[0].records[1].stage = stage;
        else malformedStageCapture.supervisor[0].stage = stage;
        let parsedMalformedStage;
        assert.doesNotThrow(() => {
          parsedMalformedStage = parseOperationsCaptureFromTap(`${captureLine(malformedStageCapture)}\n`);
        }, `${target} ${label} stage must not throw`);
        assert.deepEqual(parsedMalformedStage, { valid: false, reason: 'invalid_shape', capture: null },
          `${target} ${label} stage must remain structured and invalid`);
      }
    }
    const malformedWriterDirectory = tempDirectory('ah11-ops-malformed-stage-');
    const malformedWriterSession = createOperationsDiagnosticSession({ directory: malformedWriterDirectory.directory });
    let parentStageTrapCalls = 0;
    const hostileParentStage = new Proxy(Object.create(null), {
      get() { parentStageTrapCalls += 1; return undefined; },
      ownKeys() { parentStageTrapCalls += 1; return []; },
      getOwnPropertyDescriptor() { parentStageTrapCalls += 1; return undefined; },
      getPrototypeOf() { parentStageTrapCalls += 1; return null; }
    });
    try {
      assert.equal(recordOperationsSupervisor(malformedWriterSession, 0, hostileParentStage, {
        pidPresent: true, generation: 1, fence: 1
      }), false);
      assert.equal(parentStageTrapCalls, 0, 'supervisor writer rejects an object stage before coercion or reflection');
      const malformedWriterSnapshot = readOperationsDiagnosticSession(malformedWriterSession, { abruptSlots: [] });
      assert.equal(malformedWriterSnapshot.flags.malformed, true);
      assert.equal(malformedWriterSnapshot.flags.complete, false);
    } finally {
      disposeOperationsDiagnosticSession(malformedWriterSession);
      malformedWriterDirectory.cleanup();
    }
    const malformedChildDirectory = tempDirectory('ah11-ops-malformed-child-stage-');
    const malformedChildPath = join(malformedChildDirectory.directory, 'child.jsonl');
    let malformedChildFd = -1;
    try {
      malformedChildFd = openSync(malformedChildPath, 'wx', 0o600);
      const diagnosticsModuleUrl = new URL('../../src/internal/private-ops-diagnostics.js', import.meta.url).href;
      const writerProbe = `
        const diagnostics = await import(${JSON.stringify(diagnosticsModuleUrl)});
        let trapCalls = 0;
        const hostileStage = new Proxy(Object.create(null), {
          get() { trapCalls += 1; return undefined; },
          ownKeys() { trapCalls += 1; return []; },
          getOwnPropertyDescriptor() { trapCalls += 1; return undefined; },
          getPrototypeOf() { trapCalls += 1; return null; }
        });
        if (!diagnostics.enableOperationsRunnerCaptureFromFd3()) process.exit(2);
        if (diagnostics.recordOperationsDiagnostic(hostileStage, Object.create(null)) !== false) process.exit(3);
        if (!diagnostics.finishOperationsRunnerCapture() || trapCalls !== 0) process.exit(4);
      `;
      const writerProbeResult = spawnSync(process.execPath, ['--input-type=module', '--eval', writerProbe], {
        cwd: ROOT, env: baseEnvironment(), encoding: 'utf8', timeout: 5_000, maxBuffer: 8_192,
        stdio: ['ignore', 'pipe', 'pipe', malformedChildFd]
      });
      closeSync(malformedChildFd);
      malformedChildFd = -1;
      assert.equal(writerProbeResult.status, 0, writerProbeResult.stderr);
      const childRows = readFileSync(malformedChildPath, 'utf8').trim().split(/\r?\n/u).map(line => JSON.parse(line));
      assert.deepEqual(childRows.map(row => row.stage), ['header', 'footer']);
      assert.equal(childRows[1].writerComplete, false);
      assert.equal(childRows[1].malformed, true, 'child writer retains structured incomplete custody for invalid stage');
    } finally {
      if (malformedChildFd >= 0) closeSync(malformedChildFd);
      malformedChildDirectory.cleanup();
    }
    const missingChildRuntimeCapture = makeCompleteCapture();
    delete missingChildRuntimeCapture.supervisor.find(record => record.stage === 'child_exit').childRuntimeMs;
    assert.equal(parseOperationsCaptureFromTap(`${captureLine(missingChildRuntimeCapture)}\n`).reason, 'invalid_shape');
    const missingHeaderCapture = makeCompleteCapture();
    missingHeaderCapture.slots[0].records[0] = {
      sequence: 1, stage: 'runner_phase', elapsedMs: 0, phase: 'runner_start'
    };
    assert.equal(parseOperationsCaptureFromTap(`${captureLine(missingHeaderCapture)}\n`).reason, 'invalid_shape');
    const backwardsElapsedCapture = makeCompleteCapture();
    backwardsElapsedCapture.slots[0].records[2].elapsedMs = 0;
    assert.equal(parseOperationsCaptureFromTap(`${captureLine(backwardsElapsedCapture)}\n`).reason, 'invalid_shape');
    const headerFooterOnlyCapture = makeCompleteCapture();
    headerFooterOnlyCapture.slots[0].records = [
      headerFooterOnlyCapture.slots[0].records[0],
      { sequence: 2, stage: 'footer', elapsedMs: 2,
        writerComplete: true, overflow: false, writeFailed: false, malformed: false }
    ];
    assert.equal(parseOperationsCaptureFromTap(`${captureLine(headerFooterOnlyCapture)}\n`).reason, 'invalid_shape');
    const emptySupervisorCapture = makeCompleteCapture();
    emptySupervisorCapture.supervisor = [];
    assert.equal(parseOperationsCaptureFromTap(`${captureLine(emptySupervisorCapture)}\n`).reason, 'invalid_shape');

    const diagnosticDir = tempDirectory('ah11-ops-diagnostic-shape-');
    const diagnosticSession = createOperationsDiagnosticSession({ directory: diagnosticDir.directory });
    t.after(() => {
      disposeOperationsDiagnosticSession(diagnosticSession);
      diagnosticDir.cleanup();
    });
    let hashCoercions = 0;
    const unsafeHash = { toString() { hashCoercions += 1; return 'a'.repeat(64); } };
    const unsafeHashState = {
      available: true,
      jobs: Array.from({ length: 2 }, () => ({
        workState: 'completed', workflowState: 'succeeded', generation: 1, operationFence: 1,
        workflowFence: 1, revision: 1, checkpointSequence: 1, resultCount: 0, resultHash: unsafeHash,
        cancellationRequested: false, leaseOwnerPresent: false, leaseFenceMatches: true,
        leaseExpired: true, eventCounts: []
      })),
      reservations: { reservedDiskBytes: 0, reservedEventSlots: 0, pendingRecords: 0 }
    };
    const unsafeArray = [];
    let arrayGetterCalls = 0;
    Object.defineProperty(unsafeArray, '0', { enumerable: true, get() { arrayGetterCalls += 1; throw new Error('private'); } });
    unsafeArray.length = 1;
    const hostileSnapshot = readOperationsDiagnosticSession(diagnosticSession, {
      finalState: unsafeHashState, abruptSlots: unsafeArray
    });
    assert.equal(hostileSnapshot.finalState.available, false);
    assert.equal(hostileSnapshot.flags.malformed, true);
    assert.equal(hashCoercions, 0);
    assert.equal(arrayGetterCalls, 0);
    let proxyTrapCalls = 0;
    const proxySlots = new Proxy([], {
      get() { proxyTrapCalls += 1; return undefined; },
      ownKeys() { proxyTrapCalls += 1; return []; },
      getOwnPropertyDescriptor() { proxyTrapCalls += 1; return undefined; }
    });
    const proxySnapshot = readOperationsDiagnosticSession(diagnosticSession, { abruptSlots: proxySlots });
    assert.equal(proxySnapshot.flags.malformed, true);
    assert.equal(proxyTrapCalls, 0);
    const revokedSlots = Proxy.revocable([], {
      get() { proxyTrapCalls += 1; return undefined; },
      ownKeys() { proxyTrapCalls += 1; return []; },
      getOwnPropertyDescriptor() { proxyTrapCalls += 1; return undefined; }
    });
    revokedSlots.revoke();
    const revokedProxySnapshot = readOperationsDiagnosticSession(diagnosticSession, { abruptSlots: revokedSlots.proxy });
    assert.equal(revokedProxySnapshot.flags.malformed, true);
    assert.equal(proxyTrapCalls, 0);
    const clockDir = tempDirectory('ah11-ops-diagnostic-clock-');
    const clockSession = createOperationsDiagnosticSession({ directory: clockDir.directory });
    t.after(() => {
      disposeOperationsDiagnosticSession(clockSession);
      clockDir.cleanup();
    });
    assert.equal(recordOperationsSupervisor(clockSession, 0, 'spawn', {
      pidPresent: true, generation: 1, fence: 1
    }), true);
    assert.equal(recordOperationsSupervisor(clockSession, 0, 'child_exit', {
      exitCode: 0, signal: null, childRuntimeMs: 600_000, generation: 1, fence: 1
    }), true);
    assert.equal(recordOperationsSupervisor(clockSession, 0, 'finish_result', {
      outcome: 'completed', settled: true, code: null, generation: 1, fence: 1
    }), true);
    const clockSnapshot = readOperationsDiagnosticSession(clockSession, { abruptSlots: [] });
    const childExit = clockSnapshot.supervisor.find(record => record.stage === 'child_exit');
    assert.equal(childExit.childRuntimeMs, 600_000);
    assert.ok(childExit.elapsedMs < childExit.childRuntimeMs,
      'child runtime duration and session-relative event time remain separate fields');

    const structuralControls = [
      {
        id: 'cause-map-runtime-busy',
        body: '  cause:\n    name: RuntimeError\n    code: STORE_BUSY',
        expected: { failureCode: 'SQLITE_BUSY', errorClass: 'RuntimeError', errorCode: 'STORE_BUSY', sqlitePrimaryCode: null }
      },
      {
        id: 'error-marker-runtime-busy',
        body: "  error:\n    RuntimeError:\n      name: 'RuntimeError'\n      code: 'STORE_BUSY',",
        expected: { failureCode: 'SQLITE_BUSY', errorClass: 'RuntimeError', errorCode: 'STORE_BUSY', sqlitePrimaryCode: null }
      },
      {
        id: 'marker-split-runtime',
        body: '  cause:\n    RuntimeError:\n      name: RuntimeError\n    unrelated:\n      code: STORE_BUSY',
        expected: null
      },
      {
        id: 'marker-split-native',
        body: '  cause:\n    Error:\n      name: Error\n      code: ERR_SQLITE_ERROR\n    unrelated:\n      errcode: 5',
        expected: null
      },
      {
        id: 'marker-empty-then-unrelated',
        body: '  cause:\n    RuntimeError:\n    unrelated:\n      name: RuntimeError\n      code: STORE_BUSY',
        expected: null
      },
      {
        id: 'cause-map-node-extended-262',
        body: '  cause:\n    name: Error\n    code: ERR_SQLITE_ERROR\n    errcode: 262,',
        expected: { failureCode: 'SQLITE_BUSY', errorClass: 'Error', errorCode: 'ERR_SQLITE_ERROR', sqlitePrimaryCode: 6 }
      },
      {
        id: 'scalar-block-error',
        body: '  error: |-\n    RuntimeError:\n      code: STORE_BUSY',
        expected: null
      },
      {
        id: 'split-fields-across-siblings',
        body: '  cause:\n    first:\n      name: RuntimeError\n    second:\n      code: STORE_BUSY',
        expected: null
      },
      {
        id: 'split-native-fields-across-siblings',
        body: '  cause:\n    first:\n      name: Error\n      code: ERR_SQLITE_ERROR\n    second:\n      errcode: 5',
        expected: null
      },
      {
        id: 'native-fields-under-unrelated-detail',
        body: '  cause:\n    name: Error\n    code: ERR_SQLITE_ERROR\n    details:\n      errcode: 5',
        expected: null
      },
      {
        id: 'unsupported-native-primary',
        body: '  cause:\n    name: Error\n    code: ERR_SQLITE_ERROR\n    errcode: 1',
        expected: null
      },
      {
        id: 'marker-duplicate-code-field',
        body: '  cause:\n    RuntimeError:\n      name: RuntimeError\n      code: STORE_BUSY\n      code: STORE_BUSY',
        expected: null
      },
      {
        id: 'marker-malformed-scalar-value',
        body: '  cause:\n    RuntimeError: |\n      name: RuntimeError\n      code: STORE_BUSY',
        expected: null
      }
    ];
    structuralControls.push(
      {
        id: 'direct-duplicate-name-empty-last',
        body: '  cause:\n    name: RuntimeError\n    name:\n    code: STORE_BUSY',
        expected: null
      },
      {
        id: 'direct-duplicate-name-empty-first',
        body: '  cause:\n    name:\n    name: RuntimeError\n    code: STORE_BUSY',
        expected: null
      },
      {
        id: 'direct-duplicate-code-empty-last',
        body: '  cause:\n    name: RuntimeError\n    code: STORE_BUSY\n    code:',
        expected: null
      },
      {
        id: 'direct-duplicate-code-empty-first',
        body: '  cause:\n    name: RuntimeError\n    code:\n    code: STORE_BUSY',
        expected: null
      },
      {
        id: 'direct-duplicate-errcode-empty-last',
        body: '  cause:\n    name: Error\n    code: ERR_SQLITE_ERROR\n    errcode: 5\n    errcode:',
        expected: null
      },
      {
        id: 'direct-duplicate-errcode-empty-first',
        body: '  cause:\n    name: Error\n    code: ERR_SQLITE_ERROR\n    errcode:\n    errcode: 5',
        expected: null
      },
      {
        id: 'marker-duplicate-name-empty-last',
        body: '  cause:\n    RuntimeError:\n      name: RuntimeError\n      name:\n      code: STORE_BUSY',
        expected: null
      },
      {
        id: 'marker-duplicate-name-empty-first',
        body: '  cause:\n    RuntimeError:\n      name:\n      name: RuntimeError\n      code: STORE_BUSY',
        expected: null
      },
      {
        id: 'marker-duplicate-code-empty-last',
        body: '  cause:\n    RuntimeError:\n      name: RuntimeError\n      code: STORE_BUSY\n      code:',
        expected: null
      },
      {
        id: 'marker-duplicate-code-empty-first',
        body: '  cause:\n    RuntimeError:\n      name: RuntimeError\n      code:\n      code: STORE_BUSY',
        expected: null
      },
      {
        id: 'marker-duplicate-errcode-empty-last',
        body: '  cause:\n    Error:\n      name: Error\n      code: ERR_SQLITE_ERROR\n      errcode: 5\n      errcode:',
        expected: null
      },
      {
        id: 'marker-duplicate-errcode-empty-first',
        body: '  cause:\n    Error:\n      name: Error\n      code: ERR_SQLITE_ERROR\n      errcode:\n      errcode: 5',
        expected: null
      },
      {
        id: 'direct-runtime-missing-code',
        body: '  cause:\n    name: RuntimeError',
        expected: null
      },
      {
        id: 'direct-runtime-missing-name',
        body: '  cause:\n    code: STORE_BUSY',
        expected: null
      },
      {
        id: 'direct-native-missing-errcode',
        body: '  cause:\n    name: Error\n    code: ERR_SQLITE_ERROR',
        expected: null
      },
      {
        id: 'direct-native-missing-code',
        body: '  cause:\n    name: Error\n    errcode: 5',
        expected: null
      },
      {
        id: 'marker-runtime-missing-code',
        body: '  cause:\n    RuntimeError:\n      name: RuntimeError',
        expected: null
      },
      {
        id: 'marker-runtime-missing-name',
        body: '  cause:\n    RuntimeError:\n      code: STORE_BUSY',
        expected: null
      },
      {
        id: 'marker-native-missing-errcode',
        body: '  cause:\n    Error:\n      name: Error\n      code: ERR_SQLITE_ERROR',
        expected: null
      },
      {
        id: 'marker-native-missing-code',
        body: '  cause:\n    Error:\n      name: Error\n      errcode: 5',
        expected: null
      },
      {
        id: 'direct-single-empty-name',
        body: '  cause:\n    name:\n    code: STORE_BUSY',
        expected: null
      },
      {
        id: 'direct-single-empty-code',
        body: '  cause:\n    name: RuntimeError\n    code:',
        expected: null
      },
      {
        id: 'direct-single-empty-errcode',
        body: '  cause:\n    name: Error\n    code: ERR_SQLITE_ERROR\n    errcode:',
        expected: null
      },
      {
        id: 'marker-single-empty-name',
        body: '  cause:\n    RuntimeError:\n      name:\n      code: STORE_BUSY',
        expected: null
      },
      {
        id: 'marker-single-empty-code',
        body: '  cause:\n    RuntimeError:\n      name: RuntimeError\n      code:',
        expected: null
      },
      {
        id: 'marker-single-empty-errcode',
        body: '  cause:\n    Error:\n      name: Error\n      code: ERR_SQLITE_ERROR\n      errcode:',
        expected: null
      }
    );
    for (const control of structuralControls) {
      const diagnostic = buildSafeFailureDiagnostics({
        scenario: 'OPS13.C01', phase: 'diagnostic-control', stage: 'node-test-runner',
        status: 1,
        stdout: `not ok 1 - ${control.id}\n  ---\n  name: AssertionError\n  code: ERR_ASSERTION\n${control.body}\n  ...\n`
      });
      assert.equal(diagnostic.records.length, 1, control.id);
      assert.equal(diagnostic.records[0].failureCode, 'ASSERTION_FAILURE', control.id);
      assert.deepEqual(diagnostic.records[0].nestedStorageCause, control.expected, control.id);
    }

    const typedBusy = runtimeError({
      module: 'state', operation: 'write', code: 'STORE_BUSY', retryable: true,
      safe_message: 'SYNTHETIC_PRIVATE_DIAGNOSTIC_CANARY_96d'
    });
    const typedDiagnostic = buildSafeFailureDiagnostics({
      scenario: 'OPS13.C01', phase: 'diagnostic-control', stage: 'node-test-runner',
      status: 1, error: typedBusy
    });
    assert.equal(typedDiagnostic.records[0].failureCode, 'SQLITE_BUSY');
    assert.equal(typedDiagnostic.records[0].errorClass, 'RuntimeError');
    assert.equal(typedDiagnostic.records[0].errorCode, 'STORE_BUSY');
    assert.doesNotMatch(JSON.stringify(typedDiagnostic), /SYNTHETIC_PRIVATE_DIAGNOSTIC_CANARY_96d/u);

    const temporary = tempDirectory('ah11-diagnostic-node-sqlite-');
    const blocker = new DatabaseSync(temporary.databasePath, { timeout: 0 });
    const contender = new DatabaseSync(temporary.databasePath, { timeout: 0 });
    try {
      blocker.exec('CREATE TABLE diagnostic_lock (id INTEGER PRIMARY KEY)');
      blocker.exec('BEGIN IMMEDIATE');
      let sqliteBusy;
      try {
        contender.exec('BEGIN IMMEDIATE');
      } catch (error) {
        sqliteBusy = error;
      }
      assert.ok(sqliteBusy instanceof Error);
      assert.equal(sqliteBusy.code, 'ERR_SQLITE_ERROR');
      assert.equal(sqliteBusy.errcode & 0xff, 5);
      const sqliteDiagnostic = buildSafeFailureDiagnostics({
        scenario: 'OPS13.C01', phase: 'diagnostic-control', stage: 'node-test-runner',
        status: 1, error: sqliteBusy
      });
      assert.equal(sqliteDiagnostic.records[0].failureCode, 'SQLITE_BUSY');
      assert.equal(sqliteDiagnostic.records[0].sqlitePrimaryCode, 5);
      assert.doesNotMatch(JSON.stringify(sqliteDiagnostic), /\/Users\//u);
    } finally {
      try { blocker.exec('ROLLBACK'); } catch {}
      try { contender.exec('ROLLBACK'); } catch {}
      contender.close();
      blocker.close();
      temporary.cleanup();
    }
    assert.equal(METRICS.providerCalls, 0);
  });
});

after(() => {
  if (process.env.AH11_OPS_METRICS_FILE) {
    writeFileSync(process.env.AH11_OPS_METRICS_FILE, JSON.stringify(METRICS));
  }
});
