import { parseClosedDiagnosticChannel } from './closed-diagnostics.js';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { lstatSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { basename, dirname, isAbsolute, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';
import process from 'node:process';
import {
  buildOps12RecoveryDiagnosticRecord, buildSafeFailureDiagnostics, parseC02ControlReadDiagnostics,
  parseOps02RaceDiagnostics, parseOps06ClosePhaseDiagnostics,
  parseOps12RecoveryDiagnostics, parseOps12RecoveryFailure
} from './failure-diagnostics.js';
import { validateOps04PublicPhaseMetrics } from './ops04-failure-diagnostics.js';
import { parseOperationsCaptureFromTap } from '../../src/internal/private-ops-diagnostics.js';
import { parseC04CompletionFromTap } from './fixture-recovery-diagnostics.js';
import { parseFixtureAttemptArtifacts } from './fixture-attempt-artifacts.js';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const OPS12_RECOVERY_DIAGNOSTIC_PREFIX = 'AH11_OPS_OPS12_RECOVERY_DIAGNOSTIC ';
const EXPECTED_GROUPS = Object.freeze(Array.from({ length: 13 }, (_, index) => `OPS${String(index + 1).padStart(2, '0')}_`));
const REQUIRED_LOCAL_CASES = Object.freeze([
  'OPS01.C01', 'OPS01.C02',
  'OPS02.C01', 'OPS02.C02', 'OPS02.C03', 'OPS02.C04', 'OPS02.C05',
  'OPS03.C01', 'OPS03.C02', 'OPS03.C03', 'OPS03.C04',
  'OPS04.C01', 'OPS04.C02', 'OPS04.C03',
  'OPS05.C01', 'OPS05.C02', 'OPS05.C03', 'OPS05.C04', 'OPS05.C05', 'OPS05.C06', 'OPS05.C07', 'OPS05.C08', 'OPS05.C09', 'OPS05.C10',
  'OPS05.C11', 'OPS05.C12', 'OPS05.C13', 'OPS05.C14',
  'OPS05.C15', 'OPS05.C16', 'OPS05.C17', 'OPS05.C18',
  'OPS06.C01', 'OPS06.C02', 'OPS06.C03',
  'OPS07.C01', 'OPS07.C02', 'OPS07.C03', 'OPS07.C04', 'OPS07.C05',
  'OPS08.C01', 'OPS08.C02', 'OPS08.C03',
  'OPS09.C01', 'OPS09.C02', 'OPS09.C03', 'OPS09.C04', 'OPS09.C05',
  'OPS10.C01',
  'OPS11.C01', 'OPS11.C02',
  'OPS12.C01',
  'OPS13.C01'
]);
const FIXTURES = new Set(['quota-shutdown-restart', 'runbook-clean-start']);
const EXPECTED_UPLOAD_MEMBERS = Object.freeze([
  '.runtime-artifacts/runtime-foundation.tap',
  '.runtime-artifacts/ACV1-AH05-jev-phase-b-offline.tap',
  '.runtime-artifacts/ACV1-AH05-jev-phase-b-offline.json',
  '.runtime-artifacts/ACV1-AH05-jev-phase-b-offline.json.sha256',
  '.runtime-artifacts/neatlogs-sdk-local.tap',
  '.runtime-artifacts/neatlogs-sdk-local-capture.json',
  '.runtime-artifacts/AH-11-OPS-quota-shutdown-restart.tap',
  '.runtime-artifacts/AH-11-OPS-quota-shutdown-restart.json',
  '.runtime-artifacts/AH-11-OPS-quota-shutdown-restart.json.sha256',
  '.runtime-artifacts/AH-11-OPS-quota-shutdown-restart.diagnostics.json',
  '.runtime-artifacts/AH-11-OPS-quota-shutdown-restart.diagnostics.json.sha256',
  '.runtime-artifacts/AH-11-OPS-runbook-clean-start.tap',
  '.runtime-artifacts/AH-11-OPS-runbook-clean-start.json',
  '.runtime-artifacts/AH-11-OPS-runbook-clean-start.json.sha256',
  '.runtime-artifacts/AH-11-OPS-runbook-clean-start.diagnostics.json',
  '.runtime-artifacts/AH-11-OPS-runbook-clean-start.diagnostics.json.sha256',
  '.runtime-artifacts/AH-11-OPS-runbook-clean-start.jsonl',
  '.runtime-artifacts/AH-11-OPS-runbook-recovery.jsonl',
  '.runtime-artifacts/ops-ci1-runtime-foundation.json',
  '.runtime-artifacts/ops-ci1-quota-shutdown-restart.json',
  '.runtime-artifacts/ops-ci1-runbook-clean-start.json'
]);
const OPS07_INVALID_REASONS = new Set(['invalid_input', 'missing', 'duplicate', 'malformed', 'oversize', 'invalid_shape']);
const OPS_CI1_OWNED_GROUP_REGISTRY = 'OPS_CI1_OWNED_GROUP_REGISTRY';
const OPS_CI1_OWNED_GROUP_SCHEMA = 'ops-ci1-owned-groups.v1';
const OPS_CI1_OWNED_GROUP_REGISTRY_MAX_BYTES = 8_192;
let validatedCi1OwnedGroupRegistryPath = null;

function digest(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

function git(args) {
  const result = spawnSync('git', args, { cwd: ROOT, encoding: 'utf8', env: { PATH: process.env.PATH ?? '/usr/bin:/bin' } });
  if (result.status !== 0) throw new Error('Git revision metadata is unavailable.');
  return result.stdout.trim();
}

function currentBaseRevision() {
  const mergeBase = spawnSync('git', ['merge-base', 'HEAD', 'refs/remotes/origin/main'], {
    cwd: ROOT,
    encoding: 'utf8',
    env: { PATH: process.env.PATH ?? '/usr/bin:/bin' }
  });
  if (mergeBase.status === 0 && /^[a-f0-9]{40}$/u.test(mergeBase.stdout.trim())) {
    return mergeBase.stdout.trim();
  }
  return git(['rev-parse', 'HEAD^1']);
}

function child(args, env = {}) {
  const childEnvironment = {
    PATH: process.env.PATH ?? '/usr/bin:/bin',
    TMPDIR: process.env.TMPDIR ?? tmpdir(),
    NODE_NO_WARNINGS: '1',
    ...env
  };
  if (validatedCi1OwnedGroupRegistryPath) {
    childEnvironment[OPS_CI1_OWNED_GROUP_REGISTRY] = validatedCi1OwnedGroupRegistryPath;
  } else {
    delete childEnvironment[OPS_CI1_OWNED_GROUP_REGISTRY];
  }
  return spawnSync(process.execPath, args, {
    cwd: ROOT,
    encoding: 'utf8',
    maxBuffer: 2 * 1024 * 1024,
    timeout: 120_000,
    env: childEnvironment
  });
}

function validCi1OwnedGroupRegistry(registry) {
  return Boolean(registry && registry.schema === OPS_CI1_OWNED_GROUP_SCHEMA
    && Object.keys(registry).sort().join(',') === 'groups,pendingLaunches,schema'
    && Array.isArray(registry.groups) && Array.isArray(registry.pendingLaunches)
    && registry.groups.length <= 16 && registry.pendingLaunches.length <= 16
    && registry.groups.every(value => Number.isSafeInteger(value) && value >= 2)
    && new Set(registry.groups).size === registry.groups.length
    && registry.pendingLaunches.every(value => typeof value === 'string'
      && /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u.test(value))
    && new Set(registry.pendingLaunches).size === registry.pendingLaunches.length);
}

function validatedCi1RegistryPathFromEnvironment() {
  const registryPath = process.env[OPS_CI1_OWNED_GROUP_REGISTRY];
  if (typeof registryPath !== 'string' || registryPath.length === 0 || registryPath.length > 4_096
      || !isAbsolute(registryPath) || /[\u0000-\u001f\u007f]/u.test(registryPath)) return null;
  try {
    const metadata = lstatSync(registryPath);
    if (!metadata.isFile() || metadata.isSymbolicLink() || metadata.size > OPS_CI1_OWNED_GROUP_REGISTRY_MAX_BYTES) return null;
    const registry = JSON.parse(readFileSync(registryPath, 'utf8'));
    return validCi1OwnedGroupRegistry(registry) ? registryPath : null;
  } catch {
    return null;
  }
}

function parseTAP(text) {
  const names = [...text.matchAll(/^[ \t]*# Subtest: ((?:OPS\d{2}_[^\r\n]+)|(?:OPS\d{2}\.C\d{2}[^\r\n]*))/gmu)].map(match => match[1]);
  const testCount = Number(text.match(/^# tests (\d+)/mu)?.[1] ?? -1);
  const passCount = Number(text.match(/^# pass (\d+)/mu)?.[1] ?? -1);
  const skipped = Number(text.match(/^# skipped (\d+)/mu)?.[1] ?? -1);
  const failures = Number(text.match(/^# fail (\d+)/mu)?.[1] ?? -1);
  const unique = [...new Set(names)];
  const missing = EXPECTED_GROUPS.filter(prefix => !unique.some(name => name.startsWith(prefix)));
  const caseNames = unique.filter(name => /^OPS\d{2}\.C\d{2}(?:\s|$)/u.test(name)).map(name => name.match(/^OPS\d{2}\.C\d{2}/u)[0]);
  const uniqueCases = [...new Set(caseNames)];
  const missingCases = REQUIRED_LOCAL_CASES.filter(id => !uniqueCases.includes(id));
  return {
    testCount: testCount < 0 ? null : testCount,
    passCount: passCount < 0 ? null : passCount,
    failCount: failures,
    skippedCount: skipped,
    groupNames: unique,
    requiredLocalCases: REQUIRED_LOCAL_CASES,
    executedLocalCases: uniqueCases,
    missingLocalCases: missingCases,
    missingGroups: missing,
    complete: testCount === 13 + REQUIRED_LOCAL_CASES.length
      && passCount === 13 + REQUIRED_LOCAL_CASES.length
    && unique.filter(name => /^OPS\d{2}_/u.test(name)).length === 13
      && uniqueCases.length === REQUIRED_LOCAL_CASES.length
      && missingCases.length === 0 && missing.length === 0
      && failures === 0 && skipped === 0
  };
}

function currentChangedPaths() {
  const paths = git(['ls-files', '--modified', '--others', '--exclude-standard']).split('\n').filter(Boolean).sort();
  return paths.map(path => {
    const bytes = readFileSync(join(ROOT, path));
    return { path, bytes: bytes.byteLength, sha256: digest(bytes) };
  });
}

function changedPathsBetween(base, head) {
  const names = git(['diff', '--name-only', '--diff-filter=ACMRT', base, head]).split('\n').filter(Boolean).sort();
  return names.map(path => {
    const bytes = readFileSync(join(ROOT, path));
    return { path, bytes: bytes.byteLength, sha256: digest(bytes) };
  });
}

function writeArtifact(path, bytes) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, bytes);
  return { path, bytes: bytes.byteLength, sha256: digest(bytes) };
}

function parseOps07Capture(text) {
  try {
    const result = parseOperationsCaptureFromTap(text);
    if (result?.valid === true && result.capture !== null && typeof result.capture === 'object') return result;
    const reason = typeof result?.reason === 'string' && OPS07_INVALID_REASONS.has(result.reason)
      ? result.reason : 'invalid_shape';
    return { valid: false, reason, capture: null };
  } catch {
    return { valid: false, reason: 'invalid_shape', capture: null };
  }
}

function safeTap(text) {
  return `${text.split(/\r?\n/u).filter(line => line.startsWith('TAP version ')
    || line.startsWith('1..') || /^[ \t]*# Subtest: OPS/u.test(line)
    || /^\s*(?:ok|not ok) \d+ - OPS/u.test(line) || /^ok \d+ - OPS/u.test(line) || /^not ok \d+ - OPS/u.test(line)
    || /^# (tests|pass|fail|cancelled|skipped|todo) \d+/u.test(line))
    .join('\n')}\n`;
}

function fail(message) {
  process.stderr.write(`${message}\n`);
  process.exit(1);
}

const [caseId, mode, fixture] = process.argv.slice(2);
if (process.argv[2] === '--registry-propagation-probe') {
  validatedCi1OwnedGroupRegistryPath = validatedCi1RegistryPathFromEnvironment();
  if (!validatedCi1OwnedGroupRegistryPath) fail('AH-11-OPS ownership registry is missing or invalid.');
  const probe = child(['--input-type=module', '--eval',
    `process.stdout.write(process.env.${OPS_CI1_OWNED_GROUP_REGISTRY} === ${JSON.stringify(validatedCi1OwnedGroupRegistryPath)} ? 'forwarded' : 'missing')`]);
  if (probe.status !== 0 || probe.signal !== null || probe.error
      || probe.stdout !== 'forwarded') {
    fail('AH-11-OPS ownership registry propagation failed.');
  }
  process.stdout.write('forwarded');
  process.exit(0);
}
if (caseId !== 'AH-11-OPS' || mode !== 'mock' || !FIXTURES.has(fixture)) {
  fail('Usage: node tests/operations/verify-fixture.js AH-11-OPS mock quota-shutdown-restart|runbook-clean-start');
}
validatedCi1OwnedGroupRegistryPath = validatedCi1RegistryPathFromEnvironment();
if (!validatedCi1OwnedGroupRegistryPath) fail('AH-11-OPS ownership registry is missing or invalid.');
const nodeVersion = process.versions.node;
if (!/^v22\.23\.3$/u.test(`v${nodeVersion}`)) fail('AH-11-OPS evidence requires the pinned Node v22.23.3 toolchain.');

let baseHead;
let startHead;
let startTree;
let dirtyBefore;
let sourceFiles;
try {
  baseHead = currentBaseRevision();
  startHead = git(['rev-parse', 'HEAD']);
  startTree = git(['rev-parse', 'HEAD^{tree}']);
  dirtyBefore = git(['status', '--porcelain']).length > 0;
  sourceFiles = changedPathsBetween(baseHead, startHead);
} catch {
  fail('AH-11-OPS source identity could not be established.');
}

const artifactDirectory = resolve(ROOT, '.runtime-artifacts');
const stem = `AH-11-OPS-${fixture}`;
const metricsPath = join(tmpdir(), `ah11-ops-metrics-${process.pid}-${Date.now()}.json`);
const testResult = child(['--test', '--test-concurrency=1', '--test-reporter=tap', 'tests/operations/operations.test.js'], {
  AH11_OPS_METRICS_FILE: metricsPath
});
const tapText = `${testResult.stdout ?? ''}${testResult.stderr ?? ''}`;
const tapSummary = parseTAP(tapText);
const tapArtifact = writeArtifact(join(artifactDirectory, `${stem}.tap`), Buffer.from(safeTap(tapText), 'utf8'));
const ops07CaptureResult = parseOps07Capture(tapText);
const c04Completion = parseC04CompletionFromTap(tapText);
const fixtureAttempts = parseFixtureAttemptArtifacts(tapText);
let metrics = null;
try {
  metrics = JSON.parse(readFileSync(metricsPath, 'utf8'));
} catch {
  try { process.stderr.write('AH-11-OPS runtime metrics were not produced.\n'); } catch {}
}
const c02ControlMetricKeys = [
  'c02ControlReadFailureLines', 'c02ControlReadFailureCount',
  'c02ControlReadFailureBytes', 'c02ControlReadDiagnosticsComplete'
];
const hasCompleteC02ControlChannel = metrics !== null && typeof metrics === 'object'
  && !Array.isArray(metrics) && c02ControlMetricKeys.every(key => Object.hasOwn(metrics, key));
const c02ControlReadDiagnostics = parseC02ControlReadDiagnostics(hasCompleteC02ControlChannel ? {
  lines: metrics.c02ControlReadFailureLines,
  count: metrics.c02ControlReadFailureCount,
  bytes: metrics.c02ControlReadFailureBytes,
  diagnosticsComplete: metrics.c02ControlReadDiagnosticsComplete
} : null);
const ops02RaceMetricKeys = [
  'ops02RaceDiagnosticLines', 'ops02RaceDiagnosticCount',
  'ops02RaceDiagnosticBytes', 'ops02RaceDiagnosticsComplete'
];
const hasCompleteOps02RaceChannel = metrics !== null && typeof metrics === 'object'
  && !Array.isArray(metrics) && ops02RaceMetricKeys.every(key => Object.hasOwn(metrics, key));
const ops02RaceDiagnostics = parseOps02RaceDiagnostics(hasCompleteOps02RaceChannel ? {
  lines: metrics.ops02RaceDiagnosticLines,
  count: metrics.ops02RaceDiagnosticCount,
  bytes: metrics.ops02RaceDiagnosticBytes,
  diagnosticsComplete: metrics.ops02RaceDiagnosticsComplete
} : null);
const ops06ClosePhaseMetricKeys = [
  'ops06ClosePhaseDiagnosticLines', 'ops06ClosePhaseDiagnosticCount',
  'ops06ClosePhaseDiagnosticBytes', 'ops06ClosePhaseDiagnosticsComplete'
];
const hasCompleteOps06ClosePhaseChannel = metrics !== null && typeof metrics === 'object'
  && !Array.isArray(metrics) && ops06ClosePhaseMetricKeys.every(key => Object.hasOwn(metrics, key));
const ops06ClosePhaseDiagnostics = parseOps06ClosePhaseDiagnostics(hasCompleteOps06ClosePhaseChannel ? {
  lines: metrics.ops06ClosePhaseDiagnosticLines,
  count: metrics.ops06ClosePhaseDiagnosticCount,
  bytes: metrics.ops06ClosePhaseDiagnosticBytes,
  diagnosticsComplete: metrics.ops06ClosePhaseDiagnosticsComplete
} : null);
const ops12RecoveryMetricKeys = [
  'ops12RecoveryDiagnosticLines', 'ops12RecoveryDiagnosticCount',
  'ops12RecoveryDiagnosticBytes', 'ops12RecoveryDiagnosticsComplete'
];
const hasCompleteOps12RecoveryChannel = metrics !== null && typeof metrics === 'object'
  && !Array.isArray(metrics) && ops12RecoveryMetricKeys.every(key => Object.hasOwn(metrics, key));
const ops12RecoveryDiagnostics = parseOps12RecoveryDiagnostics(hasCompleteOps12RecoveryChannel ? {
  lines: metrics.ops12RecoveryDiagnosticLines,
  count: metrics.ops12RecoveryDiagnosticCount,
  bytes: metrics.ops12RecoveryDiagnosticBytes,
  diagnosticsComplete: metrics.ops12RecoveryDiagnosticsComplete
} : null);
const closedFixtureDiagnostics = parseClosedDiagnosticChannel(metrics?.closedFixtureDiagnostics);
const suiteDiagnostics = buildSafeFailureDiagnostics({
  scenario: fixture,
  phase: 'operation-suite',
  stage: 'node-test-runner',
  status: testResult.status,
  signal: testResult.signal,
  error: testResult.error,
  stdout: testResult.stdout,
  stderr: testResult.stderr,
  missingCases: tapSummary.missingLocalCases,
  c02ControlReadDiagnostics, ops02RaceDiagnostics, ops06ClosePhaseDiagnostics, ops12RecoveryDiagnostics
});
const publicMetrics = { ...(metrics ?? {}) };
delete publicMetrics.closedFixtureDiagnostics;
delete publicMetrics.c02ControlReadFailureLines;
delete publicMetrics.c02ControlReadFailureCount;
delete publicMetrics.c02ControlReadFailureBytes;
delete publicMetrics.c02ControlReadDiagnosticsComplete;
delete publicMetrics.ops02RaceDiagnosticLines;
delete publicMetrics.ops02RaceDiagnosticCount;
delete publicMetrics.ops02RaceDiagnosticBytes;
delete publicMetrics.ops02RaceDiagnosticsComplete;
delete publicMetrics.ops06ClosePhaseDiagnosticLines;
delete publicMetrics.ops06ClosePhaseDiagnosticCount;
delete publicMetrics.ops06ClosePhaseDiagnosticBytes;
delete publicMetrics.ops06ClosePhaseDiagnosticsComplete;
delete publicMetrics.ops12RecoveryDiagnosticLines;
delete publicMetrics.ops12RecoveryDiagnosticCount;
delete publicMetrics.ops12RecoveryDiagnosticBytes;
delete publicMetrics.ops12RecoveryDiagnosticsComplete;

let runbookArtifact = null;
let runbookRecoveryArtifact = null;
let runbookResult = null;
let recoveryResult = null;
let runbookDiagnostics = null;
let recoveryDiagnostics = null;
if (fixture === 'runbook-clean-start') {
  runbookResult = child(['tests/operations/runbook-clean-start.mjs']);
  runbookDiagnostics = buildSafeFailureDiagnostics({
    scenario: fixture,
    phase: 'clean-start-runbook',
    stage: 'runbook-child',
    status: runbookResult.status,
    signal: runbookResult.signal,
    error: runbookResult.error,
    stdout: runbookResult.stdout,
    stderr: runbookResult.stderr
  });
  const output = (runbookResult.stdout ?? '').trim().split(/\r?\n/u).filter(Boolean);
  let runbookRecord = null;
  try {
    if (output.length === 1) runbookRecord = JSON.parse(output[0]);
  } catch {}
  const shutdownMarkers = [...(runbookResult.stderr ?? '').matchAll(/^AH11_SHUTDOWN_SIGNAL (\d+)$/gmu)];
  const processCloseElapsedMs = shutdownMarkers.length > 0
    ? Number(process.hrtime.bigint() - BigInt(shutdownMarkers.at(-1)[1])) / 1_000_000
    : null;
  if (runbookRecord && runbookRecord.fixture === fixture && runbookRecord.mode === 'mock'
      && runbookRecord.schemaVersion === 6 && runbookRecord.providerCalls === 0
      && Number.isSafeInteger(runbookRecord.supervisorRssBytes) && runbookRecord.supervisorRssBytes > 0
      && Number.isFinite(runbookRecord.firstStopElapsedMs) && runbookRecord.firstStopElapsedMs >= 0
      && Number.isFinite(runbookRecord.secondStopElapsedMs) && runbookRecord.secondStopElapsedMs >= 0
      && runbookRecord.sigtermDrain === 'pass' && runbookRecord.restart === 'pass'
      && Number.isFinite(processCloseElapsedMs) && processCloseElapsedMs >= 0) {
    runbookArtifact = writeArtifact(join(artifactDirectory, `${stem}.jsonl`), Buffer.from(`${JSON.stringify({
      ...runbookRecord,
      processCloseElapsedMs,
      processCloseMeasuredFrom: 'last SIGTERM to parent-observed child close',
      shutdownSignalCount: shutdownMarkers.length
    })}\n`, 'utf8'));
  }
  const recoveryPath = join(artifactDirectory, 'AH-11-OPS-runbook-recovery.jsonl');
  recoveryResult = child(['tests/operations/runbook-recovery-cases.mjs', recoveryPath]);
  const recoveryFailure = parseOps12RecoveryFailure(recoveryResult.stderr);
  const recoverySignal = ['SIGABRT', 'SIGINT', 'SIGKILL', 'SIGTERM'].includes(recoveryResult.signal)
    ? recoveryResult.signal : null;
  const recoveryCode = Number.isSafeInteger(recoveryResult.status)
    && recoveryResult.status >= 0 && recoveryResult.status <= 255 ? recoveryResult.status : null;
  const recoveryDiagnosticRecord = buildOps12RecoveryDiagnosticRecord({
    code: recoveryCode, signal: recoverySignal
  }, recoveryCode === 0 ? null : recoveryFailure);
  const recoveryDiagnosticLine = `${OPS12_RECOVERY_DIAGNOSTIC_PREFIX}${JSON.stringify(recoveryDiagnosticRecord)}\n`;
  const recoveryChildDiagnostics = parseOps12RecoveryDiagnostics({
    lines: recoveryDiagnosticLine, count: 1,
    bytes: Buffer.byteLength(recoveryDiagnosticLine, 'utf8'), diagnosticsComplete: true
  });
  recoveryDiagnostics = buildSafeFailureDiagnostics({
    scenario: fixture,
    phase: 'recovery-runbook',
    stage: 'recovery-child',
    status: recoveryResult.status,
    signal: recoveryResult.signal,
    error: recoveryResult.error,
    stdout: recoveryResult.stdout,
    stderr: recoveryResult.stderr,
    ops12RecoveryDiagnostics: recoveryChildDiagnostics
  });
  const recoveryOutput = (recoveryResult.stdout ?? '').trim().split(/\r?\n/u).filter(Boolean);
  let recoverySummary = null;
  try { if (recoveryOutput.length === 1) recoverySummary = JSON.parse(recoveryOutput[0]); } catch {}
  let recoveryRows = [];
  try { recoveryRows = readFileSync(recoveryPath, 'utf8').trim().split(/\r?\n/u).map(line => JSON.parse(line)); } catch {}
  const expectedRecoveryCases = [
    'busy_store_spans_stop', 'store_unavailable', 'future_schema',
    'stuck_runner_review_escalation', 'unknown_hold_restart_escalation'
  ];
  if (recoverySummary?.cases === expectedRecoveryCases.length
      && recoverySummary?.passed === expectedRecoveryCases.length
      && recoverySummary?.providerCalls === 0
      && recoveryRows.length === expectedRecoveryCases.length
      && expectedRecoveryCases.every((caseId, index) => recoveryRows[index]?.case === caseId
        && recoveryRows[index]?.outcome === 'pass' && recoveryRows[index]?.providerCalls === 0)
      && !/tenant-|project-|hold-ref|sqlite/iu.test(recoveryRows.map(row => JSON.stringify(row)).join('\n'))) {
    runbookRecoveryArtifact = writeArtifact(recoveryPath, Buffer.from(`${recoveryRows.map(row => JSON.stringify(row)).join('\n')}\n`, 'utf8'));
  }
}

const resultCode = testResult.status ?? 1;
const runbookCode = runbookResult ? (runbookResult.status ?? 1) : null;
const diagnosticsPath = join(artifactDirectory, `${stem}.diagnostics.json`);
const diagnosticRecords = [
  ...suiteDiagnostics.records,
  ...(runbookDiagnostics?.records ?? []),
  ...(recoveryDiagnostics?.records ?? [])
];
const diagnosticsValue = {
  schema: 'agent-harness.ah11-ops-fixture-diagnostics.v1',
  fixture,
  source: 'bounded-allowlist-parser',
  publishedRawOutput: false,
  closedFixtureDiagnostics,
  suite: suiteDiagnostics,
  runbook: runbookDiagnostics,
  recovery: recoveryDiagnostics,
  ops07Capture: ops07CaptureResult.valid
    ? { valid: true, capture: ops07CaptureResult.capture }
    : { valid: false, reason: ops07CaptureResult.reason },
  c04Completion: c04Completion.valid
    ? { valid: true, diagnostic: c04Completion.diagnostic }
    : { valid: false, reason: c04Completion.reason },
  fixtureAttempts,
  records: diagnosticRecords.slice(0, 32)
};
const diagnosticsBytes = Buffer.from(`${JSON.stringify(diagnosticsValue, null, 2)}\n`, 'utf8');
const diagnosticsArtifact = writeArtifact(diagnosticsPath, diagnosticsBytes);
const diagnosticsSidecarArtifact = writeArtifact(
  `${diagnosticsPath}.sha256`,
  Buffer.from(`${diagnosticsArtifact.sha256}  ${basename(diagnosticsPath)}\n`, 'utf8')
);
const allNetworkCallCountsZero = metrics !== null
  && metrics.providerCalls === 0
  && metrics.blockedExternalNetworkAttempts === 0;
const ops04PhaseMetrics = validateOps04PublicPhaseMetrics(metrics);
const uploadWorkflow = readFileSync(join(ROOT, '.github/workflows/runtime-foundation.yml'), 'utf8');
const runbookJsonlMembers = [runbookArtifact, runbookRecoveryArtifact].filter(Boolean);
const diagnosticMembers = [diagnosticsArtifact, diagnosticsSidecarArtifact];
const referencedArtifactMembers = [tapArtifact, ...runbookJsonlMembers, ...diagnosticMembers];
const artifactMembersIntegrity = referencedArtifactMembers.every(artifact => {
  try {
    const bytes = readFileSync(artifact.path);
    return bytes.byteLength === artifact.bytes && digest(bytes) === artifact.sha256;
  } catch {
    return false;
  }
});
const uploadPathBlock = uploadWorkflow.match(/\n          path: \|\n([\s\S]*?)\n          if-no-files-found:/u)?.[1] ?? '';
const uploadedMembers = uploadPathBlock.split(/\r?\n/u).map(line => line.trim()).filter(Boolean);
const uploadUsesExactAllowlist = uploadedMembers.length === EXPECTED_UPLOAD_MEMBERS.length
  && EXPECTED_UPLOAD_MEMBERS.every((path, index) => uploadedMembers[index] === path)
  && uploadedMembers.every(path => !/[*?{}\[\]]/u.test(path));
const requiredEvidenceStep = uploadWorkflow.match(/- name: Require the complete sanitized evidence set\n([\s\S]*?)(?=\n      - name: Upload sanitized test evidence)/u)?.[1] ?? '';
const requiredEvidenceMembers = [...requiredEvidenceStep.matchAll(/\.runtime-artifacts\/[A-Za-z0-9._-]+/gu)]
  .map(match => match[0]);
const requiredEvidenceSetComplete = requiredEvidenceMembers.length === EXPECTED_UPLOAD_MEMBERS.length
  && EXPECTED_UPLOAD_MEMBERS.every((path, index) => requiredEvidenceMembers[index] === path);
const uploadActionOptions = uploadWorkflow.match(/\n          if-no-files-found: ([^\r\n]+)\n          include-hidden-files: ([^\r\n]+)/u);
const hiddenArtifactUploadConfigured = uploadActionOptions?.[1].trim() === 'error'
  && uploadActionOptions?.[2].trim() === 'true';
const jsonlMembersIncludedInUpload = runbookJsonlMembers.every(artifact =>
  artifact.path.startsWith(`${artifactDirectory}/`)
  && EXPECTED_UPLOAD_MEMBERS.includes(`.runtime-artifacts/${basename(artifact.path)}`))
  && uploadUsesExactAllowlist && hiddenArtifactUploadConfigured && requiredEvidenceSetComplete;
const diagnosticsIncludedInUpload = diagnosticMembers.every(artifact =>
  artifact.path.startsWith(`${artifactDirectory}/`)
  && EXPECTED_UPLOAD_MEMBERS.includes(`.runtime-artifacts/${basename(artifact.path)}`))
  && uploadUsesExactAllowlist && hiddenArtifactUploadConfigured && requiredEvidenceSetComplete;
const resourceMetricsComplete = metrics !== null
  && metrics.c02ControlReadDiagnosticsComplete === true
  && c02ControlReadDiagnostics.complete === true
  && metrics.ops02RaceDiagnosticsComplete === true
  && ops02RaceDiagnostics.complete === true && ops02RaceDiagnostics.recordCount > 0
  && metrics.ops06ClosePhaseDiagnosticsComplete === true
  && ops06ClosePhaseDiagnostics.complete === true && ops06ClosePhaseDiagnostics.recordCount === 1
  && metrics.ops12RecoveryDiagnosticsComplete === true
  && ops12RecoveryDiagnostics.complete === true && ops12RecoveryDiagnostics.recordCount === 1
  && ops04PhaseMetrics.valid === true
  && ops04PhaseMetrics.failureCount === 0
  && metrics.ci1PhaseCaptureOverflow === false
  && metrics.ci1PhaseCaptureIncomplete === false
  && metrics.ci1PhaseCaptureChildCloseUnconfirmed === false
  && metrics.ci1PhaseCaptureOwnedGroupCloseUnconfirmed === false
  && metrics.ownedRunnerStarts >= 1
  && metrics.ownedRunnerTermSignals >= 1
  && metrics.ownedRunnerKillSignals >= 1
  && metrics.ownedRunnerShutdownSamples >= 1
  && Number.isFinite(metrics.ownedRunnerShutdownElapsedMsMax)
  && metrics.ownedRunnerShutdownElapsedMsMax <= 1_000
  && metrics.processSigtermDrainSamples >= 5
  && Number.isFinite(metrics.processSigtermDrainElapsedMsMax)
  && metrics.processSigtermDrainElapsedMsMax <= 3_000
  && metrics.processSigintSignalsSent >= 1
  && metrics.processCloseSamples >= 2
  && Number.isFinite(metrics.processCloseElapsedMsMax)
  && metrics.processCloseElapsedMsMax <= 3_000
  && metrics.runnerDeadlineCloseSamples >= 1
  && Number.isFinite(metrics.runnerDeadlineCloseElapsedMsMax)
  && metrics.runnerDeadlineCloseElapsedMsMax <= 1_500
  && metrics.resourceHighWaterSamples >= 1
  && metrics.ownedRunnerCountHighWater >= 2
  && metrics.supervisorOpenFileDescriptors > 0
  && metrics.runnerOpenFileDescriptors > 0
  && metrics.storeReservedHighWaterBytes > 0
  && metrics.storeHighWaterBytes + 1 <= metrics.storeReservedHighWaterBytes
  && metrics.databaseHighWaterBytes > 0
  && metrics.walHighWaterBytes > 0
  && metrics.shmHighWaterBytes > 0
  && metrics.freeSpaceLowWaterBytes > 0
  && metrics.pendingEventHighWaterRecords > 0
  && metrics.reservedEventHighWaterSlots > 0
  && metrics.maxSupervisorRssBytes > 0
  && metrics.maxRunnerRssBytes > 0;
const externalMode = 'synthetic_offline';
const manifest = {
  schema: 'agent-harness.ah11-ops-evidence.v1',
  case: caseId,
  fixture,
  dependencyMode: externalMode,
  component: {
    repository: 'Jevelopers-neatHack-2026/agent-harness',
    baseHead,
    executedHead: startHead,
    executedTree: dirtyBefore ? null : startTree,
    workingTreeWasDirty: dirtyBefore,
    node: nodeVersion,
    schemaVersion: 6,
    checkpointEnvelopeVersion: 2,
    changedSourceFiles: sourceFiles
  },
  suite: {
    command: 'node --test --test-concurrency=1 --test-reporter=tap tests/operations/operations.test.js',
    exitCode: resultCode,
    ...tapSummary,
    requiredGroups: EXPECTED_GROUPS,
    requiredLocalCases: REQUIRED_LOCAL_CASES,
    skippedRequiredCases: tapSummary.skippedCount
  },
  runbook: runbookResult ? {
    command: 'node tests/operations/runbook-clean-start.mjs',
    exitCode: runbookCode,
    output: runbookArtifact,
    passed: runbookCode === 0 && runbookArtifact !== null
      && recoveryResult?.status === 0 && runbookRecoveryArtifact !== null,
    recoveryCommand: 'node tests/operations/runbook-recovery-cases.mjs <transcript-path>',
    recoveryExitCode: recoveryResult?.status ?? null,
    recoveryCases: runbookRecoveryArtifact ? 5 : 0,
    recoveryOutput: runbookRecoveryArtifact
  } : null,
  artifactIntegrity: {
    referencedMembers: referencedArtifactMembers,
    allReferencedMembersPresentAndDigestCorrect: artifactMembersIntegrity,
    jsonlMembersIncludedInWorkflowUpload: jsonlMembersIncludedInUpload,
    diagnosticsMembersIncludedInWorkflowUpload: diagnosticsIncludedInUpload,
    exactUploadAllowlist: uploadUsesExactAllowlist,
    requiredOutputSetChecked: requiredEvidenceSetComplete,
    hiddenArtifactUploadConfigured
  },
  diagnostics: diagnosticsValue,
  resourceAndNetworkEvidence: {
    ...publicMetrics,
    verifierRssBytes: process.memoryUsage().rss,
    resourceMetricsComplete,
    controlScope: 'Node DNS/HTTPS API guard and synthetic responders inside the JEV boundary child; not OS-wide or host egress enforcement',
    allProviderCallsZero: allNetworkCallCountsZero,
    paidCalls: 0
  },
  artifacts: { tap: tapArtifact, diagnostics: diagnosticsArtifact, diagnosticsSidecar: diagnosticsSidecarArtifact },
  passed: resultCode === 0 && tapSummary.complete && tapSummary.skippedCount === 0
    && ops07CaptureResult.valid && ops07CaptureResult.capture.flags.complete
    && c04Completion.valid && fixtureAttempts.complete
    && allNetworkCallCountsZero && resourceMetricsComplete
    && artifactMembersIntegrity && jsonlMembersIncludedInUpload && diagnosticsIncludedInUpload && requiredEvidenceSetComplete
    && (fixture !== 'runbook-clean-start' || (runbookCode === 0 && runbookArtifact !== null
      && recoveryResult?.status === 0 && runbookRecoveryArtifact !== null))
};
const manifestBytes = Buffer.from(`${JSON.stringify(manifest, null, 2)}\n`, 'utf8');
const manifestArtifact = writeArtifact(join(artifactDirectory, `${stem}.json`), manifestBytes);
const sidecar = Buffer.from(`${manifestArtifact.sha256}  ${stem}.json\n`, 'utf8');
const sidecarArtifact = writeArtifact(join(artifactDirectory, `${stem}.json.sha256`), sidecar);

try { rmSync(metricsPath, { force: true }); } catch {}

process.stdout.write(JSON.stringify({
  case: caseId, fixture, passed: manifest.passed,
  tests: tapSummary.testCount, pass: tapSummary.passCount, fail: tapSummary.failCount,
  localCases: tapSummary.executedLocalCases?.length ?? 0,
  skipped: tapSummary.skippedCount, providerCalls: metrics?.providerCalls ?? null,
  executedHead: startHead, executedTree: manifest.component.executedTree,
  artifacts: [tapArtifact, diagnosticsArtifact, diagnosticsSidecarArtifact, manifestArtifact, sidecarArtifact,
    ...(runbookArtifact ? [runbookArtifact] : []), ...(runbookRecoveryArtifact ? [runbookRecoveryArtifact] : [])]
}) + '\n');
if (!manifest.passed) process.exitCode = 1;
