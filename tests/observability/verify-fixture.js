import { createHash } from 'node:crypto';
import { execFileSync, spawnSync } from 'node:child_process';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { TELEMETRY_LIMITS } from '@jevelopers/agent-harness-runtime';

const [caseId, mode, fixtureId] = process.argv.slice(2);
if (caseId !== 'AH-10' || mode !== 'mock' || fixtureId !== 'internal-redaction-outage') {
  process.stderr.write('Unsupported AH-10 fixture arguments.\n');
  process.exit(2);
}
if (process.versions.node !== '22.23.3') {
  process.stderr.write('AH-10 local evidence requires Node 22.23.3.\n');
  process.exit(2);
}

const root = resolve(new URL('../..', import.meta.url).pathname);
const artifactDirectory = join(root, '.runtime-artifacts');
mkdirSync(artifactDirectory, { recursive: true });
const tapPath = join(artifactDirectory, 'AH-10-internal-redaction-outage.tap');
const manifestPath = join(artifactDirectory, 'AH-10-internal-redaction-outage.json');
const child = spawnSync(process.execPath, [
  '--test',
  '--test-concurrency=1',
  '--test-reporter=tap',
  'tests/observability/observability.test.js'
], {
  cwd: root,
  encoding: 'utf8',
  maxBuffer: 4 * 1024 * 1024,
  env: {
    PATH: process.env.PATH ?? '',
    TMPDIR: process.env.TMPDIR ?? '/tmp',
    NODE_NO_WARNINGS: '1'
  }
});
const tap = `${child.stdout ?? ''}${child.stderr ?? ''}`;
writeFileSync(tapPath, tap);
const match = key => Number(tap.match(new RegExp(`^# ${key} (\\d+)$`, 'm'))?.[1] ?? -1);
const result = {
  testCount: match('tests'),
  passed: match('pass'),
  failed: match('fail'),
  skipped: match('skipped'),
  todo: match('todo')
};
const source = readFileSync(join(root, 'tests/observability/observability.test.js'));
const staticAssertionCallSites = (source.toString('utf8').match(/\bassert\.(?:equal|deepEqual|throws|ok|match)\s*\(/g) ?? []).length;
const commit = execFileSync('git', ['rev-parse', 'HEAD'], {
  cwd: root,
  encoding: 'utf8',
  env: { PATH: process.env.PATH ?? '' }
}).trim();
const workingTreeStatus = execFileSync('git', ['status', '--porcelain'], {
  cwd: root,
  encoding: 'utf8',
  env: { PATH: process.env.PATH ?? '' }
}).trim();
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const manifest = {
  artifactVersion: 1,
  evidenceMode: 'internal-mock',
  caseId,
  fixtureId,
  commit,
  workingTreeClean: workingTreeStatus.length === 0,
  dirtyPathCount: workingTreeStatus === '' ? 0 : workingTreeStatus.split('\n').length,
  nodeVersion: process.version,
  testCommand: 'node --test --test-concurrency=1 --test-reporter=tap tests/observability/observability.test.js',
  exitCode: child.status ?? 1,
  result: {
    ...result,
    noSkippedTests: result.skipped === 0,
    staticAssertionCallSites,
    assertionCountBasis: 'Static assertion call sites in the observability suite; loop executions may repeat sites.'
  },
  fixtureCounts: {
    sqlitePaginationRows: 1007,
    privacyCanaries: 7,
    processCrashWindows: 3,
    competingExporterOwners: 2,
    policyDroppedUnknownEvents: 1
  },
  coverageClaims: [
    'bounded real SQLite pagination and ordering',
    'allowlisted event and field projection with stable HMAC identity tokens',
    'secret, suffix, private URL, checkpoint, response, artifact, hold, and error canary scans',
    'accessor, proxy, cycle, unknown-field, oversize, and tokenization failures',
    'bounded queue, bytes, retries, flush work, pending diagnostics, and abort-ignoring sink behavior',
    'financial outbox, attempt, observation, workflow, and hold invariants during outages',
    'real intent, receipt, restart, cancellation, expiry, and reconciliation flows',
    'process kill before export, after sink acceptance before cursor, and after cursor commit',
    'stable replay identity and concurrent exporter cursor compare-and-swap',
    'schema v3 upgrade, v2 migration rollback, prior v1/v2 foundation migration tests, and future-schema rejection'
  ],
  limits: {
    sqlEventPageMax: 500,
    storeBusyTimeoutMs: { default: 5000, maximum: 60000 },
    maximumSinkWaitPerFlushMs: 8 * (3 * 500 + 2 * 100),
    telemetry: TELEMETRY_LIMITS.defaults,
    telemetryMaximums: TELEMETRY_LIMITS.maximums
  },
  externalEvidence: {
    neatlogsSdkWired: false,
    neatlogsIngestionVerified: false,
    sponsorAcceptance: false,
    entireEnabled: false,
    providerCalls: 0,
    networkCalls: 0
  },
  artifacts: {
    tapPath: '.runtime-artifacts/AH-10-internal-redaction-outage.tap',
    tapSha256: hash(Buffer.from(tap)),
    observabilityTestSha256: hash(source)
  }
};
const manifestBytes = Buffer.from(`${JSON.stringify(manifest, null, 2)}\n`);
writeFileSync(manifestPath, manifestBytes);
const manifestHashPath = `${manifestPath}.sha256`;
writeFileSync(manifestHashPath, `${hash(manifestBytes)}  ${manifestPath.split('/').at(-1)}\n`);

process.stdout.write(`AH-10 ${fixtureId}: ${result.passed}/${result.testCount} passed; ${result.failed} failed; ${result.skipped} skipped; mode=mock; commit=${commit}\n`);
process.stdout.write(`TAP: ${tapPath}\nManifest: ${manifestPath}\nManifest SHA-256: ${manifestHashPath}\n`);
if (child.error || child.status !== 0 || result.failed !== 0 || result.skipped !== 0 || result.passed !== result.testCount) {
  if (child.stderr) process.stderr.write(child.stderr);
  process.exit(1);
}
