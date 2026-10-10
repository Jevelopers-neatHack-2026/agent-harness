import { createHash } from 'node:crypto';
import { execFileSync, spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { resolveGitEvidenceProvenance } from './git-provenance.js';

const [caseId, mode, fixtureId] = process.argv.slice(2);
if (caseId !== 'ACV1-AH05' || mode !== 'mock' || fixtureId !== 'jev-phase-b-offline') {
  process.stderr.write('Unsupported JEV offline fixture arguments.\n');
  process.exit(2);
}
if (process.versions.node !== '22.23.3') {
  process.stderr.write('JEV offline evidence requires Node v22.23.3.\n');
  process.exit(2);
}

const scriptPath = fileURLToPath(import.meta.url);
const root = resolve(dirname(scriptPath), '../../..');
const suiteDirectory = join(root, 'tests/decisions/jev-adapter.contract');
const artifactDirectory = join(root, '.runtime-artifacts');
mkdirSync(artifactDirectory, { recursive: true });
const tapPath = join(artifactDirectory, 'ACV1-AH05-jev-phase-b-offline.tap');
const manifestPath = join(artifactDirectory, 'ACV1-AH05-jev-phase-b-offline.json');
const hashPath = `${manifestPath}.sha256`;
const metricsDirectory = mkdtempSync(join(tmpdir(), 'jev-offline-metrics-'));
const metricsPath = join(metricsDirectory, 'network.jsonl');
writeFileSync(metricsPath, '', { flag: 'wx' });

const testFiles = readdirSync(suiteDirectory)
  .filter(name => name.endsWith('.test.js'))
  .sort()
  .map(name => join('tests/decisions/jev-adapter.contract', name));
const testCommand = [
  'node', '--test', '--test-concurrency=1', '--test-reporter=tap', ...testFiles
];
const child = spawnSync(process.execPath, testCommand.slice(1), {
  cwd: root,
  encoding: 'utf8',
  maxBuffer: 8 * 1024 * 1024,
  env: {
    PATH: process.env.PATH ?? '',
    TMPDIR: process.env.TMPDIR ?? '/tmp',
    NODE_NO_WARNINGS: '1',
    JEV_OFFLINE_METRICS_FILE: metricsPath
  }
});
const rawTap = `${child.stdout ?? ''}${child.stderr ?? ''}`;
const summaryValue = name => Number(rawTap.match(new RegExp(`^# ${name} (\\d+)$`, 'm'))?.[1] ?? -1);
const testResults = {
  tests: summaryValue('tests'),
  passed: summaryValue('pass'),
  failed: summaryValue('fail'),
  skipped: summaryValue('skipped'),
  todo: summaryValue('todo')
};
const testNames = [...rawTap.matchAll(/^# Subtest: ([^\r\n]{1,220})$/gm)].map(match => match[1]);
const acceptanceGroups = [
  'wire_positive', 'wire_adversarial', 'batch_semantics', 'fixed_surface_ssrf',
  'deadline_cancel', 'retry_faults', 'durable_evidence', 'privacy_identity', 'gates'
];
const groupCoverage = Object.fromEntries(acceptanceGroups.map(group => [
  group,
  { executedTestCases: testNames.filter(name => name.startsWith(group)).length }
]));
const missingAcceptanceGroups = acceptanceGroups.filter(group => groupCoverage[group].executedTestCases === 0);
const metricCounts = {
  controlledDnsResolve4: 0,
  controlledDnsResolve6: 0,
  controlledHttpsRequests: 0,
  controlledHttpsWrites: 0,
  blockedUncontrolledNetworkAttempts: 0,
  crashBeforeDispatch: 0,
  crashAfterPossibleWrite: 0,
  crashAfterResponseBeforeObservation: 0,
  crashAfterObservationCommit: 0
};
const metricMap = new Map([
  ['controlled_dns_resolve4', 'controlledDnsResolve4'],
  ['controlled_dns_resolve6', 'controlledDnsResolve6'],
  ['controlled_https_request', 'controlledHttpsRequests'],
  ['controlled_https_write', 'controlledHttpsWrites'],
  ['blocked_external_network_attempt', 'blockedUncontrolledNetworkAttempts'],
  ['crash_before_dispatch', 'crashBeforeDispatch'],
  ['crash_after_possible_write', 'crashAfterPossibleWrite'],
  ['crash_after_response_before_observation', 'crashAfterResponseBeforeObservation'],
  ['crash_after_observation_commit', 'crashAfterObservationCommit']
]);
const metricRows = readFileSync(metricsPath, 'utf8').split('\n').filter(Boolean);
let invalidMetricRow = false;
for (const row of metricRows) {
  const counter = metricMap.get(row);
  if (!counter) invalidMetricRow = true;
  else metricCounts[counter] += 1;
}

const assertionMatcher = /\bassert\.(?:deepEqual|doesNotMatch|equal|notDeepEqual|notEqual|ok|rejects|throws|match)\s*\(/g;
const staticAssertionCallSites = testFiles.reduce((count, relativePath) => {
  const source = readFileSync(join(root, relativePath), 'utf8');
  return count + (source.match(assertionMatcher)?.length ?? 0);
}, 0);
const commandEnvironment = { PATH: process.env.PATH ?? '' };
const git = args => execFileSync('git', args, {
  cwd: root,
  encoding: 'utf8',
  env: commandEnvironment
}).trim();
const provenance = resolveGitEvidenceProvenance(root);
const commit = provenance.candidateCommit;
const tree = provenance.candidateTree;
const base = provenance.baseCommit;
const workingTree = git(['status', '--porcelain']);
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const safeTestLines = rawTap.split(/\r?\n/u).filter(line =>
  /^# Subtest: [A-Za-z0-9 ._:-]{1,220}$/u.test(line)
  || /^(?:not )?ok [0-9]+ - [A-Za-z0-9 ._:-]{1,220}$/u.test(line)
);
const sanitizedTap = [
  'JEV synthetic offline contract suite',
  ...safeTestLines,
  `tests=${testResults.tests} pass=${testResults.passed} fail=${testResults.failed} skipped=${testResults.skipped} todo=${testResults.todo}`,
  ''
].join('\n');
writeFileSync(tapPath, sanitizedTap);

const manifest = {
  artifactVersion: 2,
  caseId,
  fixtureId,
  evidenceMode: 'synthetic_offline',
  evidenceScope: 'private AH-05 Phase A/B prerequisite only',
  base,
  commit,
  tree,
  provenance: {
    eventName: provenance.eventName,
    candidateKind: provenance.candidateKind,
    sourceRef: provenance.sourceRef,
    sourceCommit: provenance.sourceCommit,
    baseRef: provenance.baseRef,
    baseCommit: provenance.baseCommit,
    candidateRef: provenance.candidateRef,
    candidateCommit: provenance.candidateCommit,
    candidateTree: provenance.candidateTree,
    baseSelectionPolicy: provenance.baseSelectionPolicy
  },
  workingTreeClean: workingTree.length === 0,
  dirtyPathCount: workingTree === '' ? 0 : workingTree.split('\n').length,
  nodeVersion: process.version,
  command: testCommand.join(' '),
  exitCode: child.status ?? 1,
  tests: {
    ...testResults,
    noRequiredSkips: testResults.skipped === 0 && testResults.todo === 0,
    staticAssertionCallSites,
    staticAssertionBasis: 'source call-site count across JEV .test.js files; loops can execute a site more than once',
    dynamicTestCaseNames: testNames,
    acceptanceGroupCoverage: groupCoverage,
    missingAcceptanceGroups
  },
  dynamicControlledNetworkActivity: {
    ...metricCounts,
    providerApiCalls: 0,
    liveNetworkCalls: 0,
    countBasis: 'runtime counters appended by controlled DNS and HTTPS test doubles, including subprocess crash fixtures'
  },
  durableEvidence: {
    processCrashBoundaryRuns: {
      afterIntentBeforeDispatchMarker: metricCounts.crashBeforeDispatch,
      afterPossibleWrite: metricCounts.crashAfterPossibleWrite,
      afterResponseBeforeObservation: metricCounts.crashAfterResponseBeforeObservation,
      afterObservationCommit: metricCounts.crashAfterObservationCommit
    },
    schemaMigration: 'atomic v1/v2/v3/v4-to-v5 success and v5-failure rollback snapshots run in this fixture',
    localDeliveryOnly: true,
    financialSettlementOrHoldRelease: false,
    workflowSuccessPromotion: false
  },
  gateStatus: {
    ah05Ready: false,
    decisionPortExported: false,
    productionProviderExecutor: false,
    liveModeSupported: false,
    financeAcceptedUsageContract: false
  },
  artifacts: {
    tapPath: '.runtime-artifacts/ACV1-AH05-jev-phase-b-offline.tap',
    tapSha256: hash(Buffer.from(sanitizedTap)),
    capturedRunnerOutputSha256: hash(Buffer.from(rawTap))
  }
};
const manifestBytes = Buffer.from(`${JSON.stringify(manifest, null, 2)}\n`);
writeFileSync(manifestPath, manifestBytes);
writeFileSync(hashPath, `${hash(manifestBytes)}  ${manifestPath.split('/').at(-1)}\n`);
rmSync(metricsDirectory, { recursive: true, force: true });

process.stdout.write(`ACV1-AH05 ${fixtureId}: ${testResults.passed}/${testResults.tests} passed; ${testResults.failed} failed; ${testResults.skipped} skipped; mode=synthetic_offline; commit=${commit}\n`);
process.stdout.write(`Dynamic controlled DNS=${metricCounts.controlledDnsResolve4 + metricCounts.controlledDnsResolve6}, HTTPS requests=${metricCounts.controlledHttpsRequests}, writes=${metricCounts.controlledHttpsWrites}, blocked uncontrolled attempts=${metricCounts.blockedUncontrolledNetworkAttempts}\n`);
process.stdout.write(`Static assertion call sites=${staticAssertionCallSites} (not executed assertion count)\n`);
process.stdout.write(`TAP: .runtime-artifacts/ACV1-AH05-jev-phase-b-offline.tap\nManifest: .runtime-artifacts/ACV1-AH05-jev-phase-b-offline.json\nManifest SHA-256: ${hashPath}\n`);

if (child.error || child.status !== 0 || invalidMetricRow
    || testResults.tests < 1 || testResults.failed !== 0 || testResults.skipped !== 0
    || testResults.todo !== 0 || testResults.passed !== testResults.tests
    || missingAcceptanceGroups.length > 0
    || workingTree.length !== 0
    || metricCounts.blockedUncontrolledNetworkAttempts !== 0
    || metricCounts.controlledHttpsWrites > metricCounts.controlledHttpsRequests
    || metricCounts.crashBeforeDispatch !== 1
    || metricCounts.crashAfterPossibleWrite !== 1
    || metricCounts.crashAfterResponseBeforeObservation !== 1
    || metricCounts.crashAfterObservationCommit !== 1) {
  process.exit(1);
}
