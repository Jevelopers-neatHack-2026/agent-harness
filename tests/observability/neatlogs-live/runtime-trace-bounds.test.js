import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  existsSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, readlinkSync,
  renameSync, rmSync, symlinkSync, writeFileSync
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { after, before, test } from 'node:test';
import { createNeatlogsOutboxSink } from '../../../src/observability/private/neatlogs-outbox.js';
import { DurableWorkflowStore } from '../../../src/internal/durable-store.js';
import { isCompleteDeliveryEvidence } from '../../../tools/neatlogs-runtime-trace.mjs';

const TOOL = fileURLToPath(new URL('../../../tools/neatlogs-runtime-trace.mjs', import.meta.url));
const REVISION = 'b'.repeat(40);
const INGEST_KEY = 'synthetic-ingest-key-0123456789abcdef';
const READ_TOKEN = 'synthetic-readback-token-0123456789abcdef';
const PROJECT_ID = '00000000-0000-4000-8000-000000000001';
const MAX_BATCH_BYTES = 2 * 1024 * 1024;
let scratch;
let invocation = 0;

before(() => { scratch = mkdtempSync(join(tmpdir(), 'neatlogs-cli-bounds-')); });
after(() => { rmSync(scratch, { recursive: true, force: true }); });

function record(sequence) {
  return { schema: 'agent-harness.telemetry.v1', sequence,
    eventId: `tok1_${sequence.toString(16).padStart(64, '0')}`,
    workflowId: `tok1_${'f'.repeat(64)}`, revision: 0, eventType: 'workflow_created',
    fromState: null, toState: null, fence: 0, occurredAtMs: 1_760_000_000_000 };
}

function batchPath(directory, sequence) {
  const padded = String(sequence).padStart(16, '0');
  return join(directory, `batch-${padded}-${padded}.json`);
}

function seed(name, count, mutate = () => {}) {
  const directory = join(scratch, name);
  const sink = createNeatlogsOutboxSink({ directory });
  assert.deepEqual(sink([record(1)]), { accepted: true, acceptedCount: 1 });
  const template = JSON.parse(readFileSync(batchPath(directory, 1), 'utf8'));
  for (let sequence = 1; sequence <= count; sequence += 1) {
    const batch = { ...template, firstSequence: sequence, lastSequence: sequence,
      records: [record(sequence)], state: 'verified', readback: { status: 'verified' } };
    mutate(batch, sequence);
    writeFileSync(batchPath(directory, sequence), JSON.stringify(batch), { mode: 0o600 });
  }
  // Retained verification states are synthetic fixtures. They do not assert
  // actual Neatlogs observation or authorize a provider call.
  return directory;
}

function snapshot(directory) {
  return readdirSync(directory).sort().map(name => {
    const path = join(directory, name);
    const stats = lstatSync(path);
    return { name, mode: stats.mode, size: stats.size, mtimeMs: stats.mtimeMs,
      sha256: stats.isFile() ? createHash('sha256').update(readFileSync(path)).digest('hex') : null,
      target: stats.isSymbolicLink() ? readlinkSync(path) : null };
  });
}

function cli(directory, { mode = 'drain', args = [], evidencePath, forbidListing = false,
  noReadback = false, fakePost = false, observeFile = null, allowWorker = false,
  overflowAfterPost = false, refuseBatchWrites = false, failWorkerStart = false } = {}) {
  const id = ++invocation;
  evidencePath ??= join(scratch, `evidence-${id}.json`);
  const receiptPath = join(scratch, `child-${id}.json`);
  const runtimeTemporaryRoot = join(scratch, `runtime-temp-${id}`);
  mkdirSync(runtimeTemporaryRoot);
  const guard = `
    import fs from 'node:fs';
    import net from 'node:net';
    import tls from 'node:tls';
    import childProcess from 'node:child_process';
    import { syncBuiltinESMExports } from 'node:module';
    const directory = ${JSON.stringify(directory)};
    const receipt = { realNetworkAttempts: 0, fakePosts: 0, fullListings: 0,
      directoryReads: 0, directoryCloses: 0, observedFileReads: 0, subprocessStarts: 0,
      runtimeDirectories: [], runtimeCleanups: [], refusedBatchWrites: 0 };
    const deny = () => { receipt.realNetworkAttempts += 1; throw new Error('real network forbidden'); };
    globalThis.fetch = async (url, init) => {
      if (${fakePost} && new URL(url).pathname === '/v1/traces' && init?.method === 'POST') {
        receipt.fakePosts += 1;
        return new Response(null, { status: 200 });
      }
      return deny();
    };
    net.Socket.prototype.connect = deny;
    tls.connect = deny;
    const originalOpenDir = fs.opendirSync;
    fs.opendirSync = (...args) => {
      const handle = originalOpenDir(...args);
      if (String(args[0]) !== directory) return handle;
      const originalRead = handle.readSync.bind(handle);
      const originalClose = handle.closeSync.bind(handle);
      handle.readSync = () => { receipt.directoryReads += 1; return originalRead(); };
      handle.closeSync = () => { receipt.directoryCloses += 1; return originalClose(); };
      return handle;
    };
    const originalList = fs.readdirSync;
    fs.readdirSync = (...args) => {
      if (String(args[0]) === directory) {
        receipt.fullListings += 1;
        if (${forbidListing}) throw new Error('full listing forbidden before bounded preflight');
      }
      return originalList(...args);
    };
    const originalReadFile = fs.readFileSync;
    fs.readFileSync = (...args) => {
      if (String(args[0]) === ${JSON.stringify(observeFile)}) receipt.observedFileReads += 1;
      return originalReadFile(...args);
    };
    const originalTemporary = fs.mkdtempSync;
    fs.mkdtempSync = (...args) => {
      const path = originalTemporary(...args);
      if (String(args[0]).includes('/neatlogs-runtime-')) receipt.runtimeDirectories.push(path);
      return path;
    };
    const originalRemove = fs.rmSync;
    fs.rmSync = (path, ...args) => {
      const observed = receipt.runtimeDirectories.includes(path)
        ? { directory: path, databaseExistedBefore: fs.existsSync(path + '/workflow.sqlite') } : null;
      const result = originalRemove(path, ...args);
      if (observed) {
        observed.directoryExistsAfter = fs.existsSync(path);
        receipt.runtimeCleanups.push(observed);
      }
      return result;
    };
    const originalOpenFile = fs.openSync;
    fs.openSync = (...args) => {
      if (${refuseBatchWrites} && String(args[0]).startsWith(directory + '/batch-') && args[1] === 'wx') {
        receipt.refusedBatchWrites += 1;
        throw Object.assign(new Error('synthetic local admission write refused'), { code: 'ENOSPC' });
      }
      return originalOpenFile(...args);
    };
    const originalRename = fs.renameSync;
    let overflowInjected = false;
    fs.renameSync = (...args) => {
      const result = originalRename(...args);
      if (${overflowAfterPost} && receipt.fakePosts > 0 && !overflowInjected
          && String(args[1]).startsWith(directory + '/batch-')) {
        overflowInjected = true;
        fs.writeFileSync(args[1], Buffer.alloc(${MAX_BATCH_BYTES + 1}, 0x78));
      }
      return result;
    };
    for (const key of ['spawn', 'spawnSync', 'fork']) {
      const original = childProcess[key];
      childProcess[key] = (...args) => {
        receipt.subprocessStarts += 1;
        if (${failWorkerStart}) throw Object.assign(new Error('synthetic local worker start refused'), { code: 'EAGAIN' });
        if (${allowWorker}) return original(...args);
        throw new Error('unexpected subprocess');
      };
    }
    syncBuiltinESMExports();
    process.on('exit', () => fs.writeFileSync(${JSON.stringify(receiptPath)}, JSON.stringify(receipt)));
  `;
  const child = spawnSync(process.execPath, [
    '--import', `data:text/javascript,${encodeURIComponent(guard)}`, TOOL,
    '--mode', mode, '--outbox', directory, '--evidence', evidencePath,
    '--component-revision', REVISION, '--max-polls', '1', '--poll-interval-ms', '0', ...args
  ], { encoding: 'utf8', timeout: 15_000, maxBuffer: 1024 * 1024,
    env: { PATH: process.env.PATH, TMPDIR: runtimeTemporaryRoot, NODE_NO_WARNINGS: '1',
      NEATLOGS_INGEST_KEY: INGEST_KEY,
      ...(noReadback ? {} : { NEATLOGS_READ_TOKEN: READ_TOKEN, NEATLOGS_PROJECT_ID: PROJECT_ID }) } });
  assert.equal(child.error, undefined);
  assert.equal(child.signal, null);
  const receipt = JSON.parse(readFileSync(receiptPath, 'utf8'));
  assert.equal(receipt.realNetworkAttempts, 0);
  const evidenceText = existsSync(evidencePath) && lstatSync(evidencePath).isFile()
    ? readFileSync(evidencePath, 'utf8') : null;
  for (const secret of [INGEST_KEY, READ_TOKEN, PROJECT_ID]) {
    assert.ok(!`${child.stdout}\n${child.stderr}\n${evidenceText}`.includes(secret));
  }
  return { child, receipt, evidencePath, evidenceText,
    evidence: evidenceText === null ? null : JSON.parse(evidenceText),
    output: child.stdout ? JSON.parse(child.stdout) : null };
}

function refused(result, reasonCode) {
  assert.equal(result.child.status, 1, result.child.stderr);
  assert.ok(result.evidence, 'a bounded refusal receipt must be written');
  assert.equal(result.evidence.deliveryConfirmed, false);
  assert.equal(result.evidence.inspection.complete, false);
  assert.equal(result.evidence.inspection.phase, 'preflight');
  assert.equal(result.evidence.inspection.reasonCode, reasonCode);
  assert.equal(result.evidence.inspection.retainedBatchCount, null);
  assert.deepEqual(result.evidence.batches, []);
  assert.equal(result.output.deliveryConfirmed, false);
  assert.equal(result.output.inspectionComplete, false);
  assert.equal(result.receipt.fakePosts, 0);
  assert.equal(result.receipt.subprocessStarts, 0);
  assert.ok(Buffer.byteLength(result.evidenceText) <= 8 * 1024 * 1024);
  assert.ok(Buffer.byteLength(result.child.stdout) <= 64 * 1024);
}

test('default CLI preflight refuses 129 retained batches without listing, cursor or provider effects', () => {
  const directory = seed('over-default', 129);
  const before = snapshot(directory);
  const result = cli(directory);
  refused(result, 'OUTBOX_INVENTORY_LIMIT_REACHED');
  assert.equal(result.receipt.fullListings, 0);
  assert.ok(result.receipt.directoryReads <= 130);
  assert.equal(result.receipt.directoryCloses, 1);
  assert.deepEqual(snapshot(directory), before);
});

test('128 terminal synthetic batches retain complete-count evidence and the bounded drain page', () => {
  const directory = seed('at-default', 128);
  const result = cli(directory);
  assert.equal(result.child.status, 0, result.child.stderr);
  assert.equal(result.evidence.batches.length, 128);
  assert.equal(result.evidence.drainPage.length, 32);
  assert.equal(result.evidence.inspection.complete, true);
  assert.equal(result.evidence.inspection.retainedBatchCount, 128);
  assert.equal(result.evidence.batchEvidenceSource, 'persisted_outbox_snapshot');
  assert.equal(result.evidence.deliveryConfirmed, true);
  assert.equal(result.receipt.fakePosts, 0);
});

test('explicit CLI limits share the admission range and allow an operator to resume without pruning', () => {
  const directory = seed('configured', 129);
  const before = snapshot(directory);
  refused(cli(directory), 'OUTBOX_INVENTORY_LIMIT_REACHED');
  assert.deepEqual(snapshot(directory), before);
  const result = cli(directory, { args: ['--max-retained-batches', '129'] });
  assert.equal(result.child.status, 0, result.child.stderr);
  assert.equal(result.evidence.batches.length, 129);
  assert.equal(result.evidence.deliveryConfirmed, true);
  assert.deepEqual(snapshot(directory).filter(row => row.name !== 'drain-cursor.json'), before);
  for (const value of ['0', '1025', '-1', '1.5', 'NaN', '1e3']) {
    const prior = snapshot(directory);
    // The equals form passes a negative value through Node's option parser to
    // this option's value validator, rather than treating it as another flag.
    const invalid = cli(directory, { args: [`--max-retained-batches=${value}`] });
    assert.equal(invalid.child.status, 2);
    assert.equal(invalid.receipt.fullListings, 0);
    assert.equal(invalid.receipt.directoryReads, 0);
    assert.deepEqual(snapshot(directory), prior);
  }
  const one = seed('limit-endpoints', 1);
  for (const value of ['1', '1024']) {
    assert.equal(cli(one, { args: ['--max-retained-batches', value] }).child.status, 0);
  }
});

test('unexpected files, nonfiles, symlinks and invalid names refuse the whole preflight without cleanup', () => {
  const variants = [
    ['unexpected-file', directory => writeFileSync(join(directory, 'retained.tmp'), 'retain me')],
    ['nested-directory', directory => mkdirSync(join(directory, 'nested'))],
    ['symlink', directory => symlinkSync(batchPath(directory, 1), join(directory, 'batch-0000000000000002-0000000000000002.json'))],
    ['bad-range', directory => writeFileSync(join(directory, 'batch-0000000000000003-0000000000000002.json'), '{}')],
    ['zero-range', directory => writeFileSync(join(directory, 'batch-0000000000000000-0000000000000000.json'), '{}')]
  ];
  for (const [name, change] of variants) {
    const directory = seed(name, 1);
    change(directory);
    const before = snapshot(directory);
    const result = cli(directory);
    refused(result, 'OUTBOX_INVENTORY_INVALID');
    assert.equal(result.receipt.fullListings, 0);
    assert.deepEqual(snapshot(directory), before);
  }
});

test('the 2 MiB batch and 256 byte cursor limits refuse before body reads or cursor replacement', () => {
  for (const [name, target, bytes] of [
    ['large-batch', 'batch-0000000000000001-0000000000000001.json', MAX_BATCH_BYTES + 1],
    ['large-cursor', 'drain-cursor.json', 257],
    ['empty-batch', 'batch-0000000000000001-0000000000000001.json', 0]
  ]) {
    const directory = seed(name, 1);
    const path = join(directory, target);
    writeFileSync(path, Buffer.alloc(bytes, 0x78));
    const before = snapshot(directory);
    const result = cli(directory, { observeFile: path });
    refused(result, 'OUTBOX_FILE_LIMIT');
    assert.equal(result.receipt.observedFileReads, 0);
    assert.equal(result.receipt.fullListings, 0);
    assert.deepEqual(snapshot(directory), before);
  }
  const directory = seed('exact-byte-limit', 1);
  const path = batchPath(directory, 1);
  const batch = JSON.parse(readFileSync(path, 'utf8'));
  batch.recoveryHistory = [{ retainedSyntheticPadding: '' }];
  const padding = MAX_BATCH_BYTES - Buffer.byteLength(JSON.stringify(batch));
  batch.recoveryHistory[0].retainedSyntheticPadding = 'x'.repeat(padding);
  writeFileSync(path, JSON.stringify(batch));
  const before = readFileSync(path);
  assert.equal(before.length, MAX_BATCH_BYTES);
  const result = cli(directory);
  assert.equal(result.child.status, 0, result.child.stderr);
  assert.deepEqual(readFileSync(path), before);
  assert.ok(Buffer.byteLength(result.evidenceText) < 16 * 1024);
});

test('overflow detection uses bounded streamed observations even when full enumeration is forbidden', () => {
  const directory = seed('stream-overflow', 129);
  const before = snapshot(directory);
  const result = cli(directory, { forbidListing: true });
  refused(result, 'OUTBOX_INVENTORY_LIMIT_REACHED');
  assert.equal(result.receipt.fullListings, 0);
  assert.equal(result.receipt.directoryReads, 129);
  assert.equal(result.receipt.directoryCloses, 1);
  assert.deepEqual(snapshot(directory), before);
});

test('per-batch summary overflow and escaped expansion refuse before draining', () => {
  for (const [name, reason] of [
    ['long-summary', 'x'.repeat(128 * 1024)],
    ['escaped-summary', '\u0000'.repeat(32 * 1024)]
  ]) {
    const directory = seed(name, 1, batch => { batch.readback.reasonCode = reason; });
    const before = snapshot(directory);
    const result = cli(directory);
    refused(result, 'EVIDENCE_SUMMARY_LIMIT');
    assert.deepEqual(snapshot(directory), before);
  }
});

test('aggregate retained summaries have a finite evidence budget before any drain', () => {
  const directory = seed('aggregate-summary', 96, batch => {
    batch.readback.reasonCode = 'x'.repeat(96 * 1024);
  });
  const before = snapshot(directory);
  const result = cli(directory);
  refused(result, 'EVIDENCE_TOTAL_LIMIT');
  assert.deepEqual(snapshot(directory), before);
});

test('stdout has its own bound even when all individual and aggregate evidence summaries fit', () => {
  const directory = seed('stdout-summary', 80, batch => { batch.send = { status: 'x'.repeat(2048) }; });
  const before = snapshot(directory);
  const result = cli(directory);
  refused(result, 'EVIDENCE_STDOUT_LIMIT');
  assert.deepEqual(snapshot(directory), before);
});

test('run and send-and-readback refuse existing invalid inventory before starting a worker', () => {
  for (const mode of ['run', 'send-and-readback']) {
    const directory = seed(`refuse-${mode}`, 1);
    writeFileSync(join(directory, 'unexpected.json'), '{}');
    const before = snapshot(directory);
    const result = cli(directory, { mode });
    refused(result, 'OUTBOX_INVENTORY_INVALID');
    assert.equal(result.evidence.run, null);
    assert.deepEqual(snapshot(directory), before);
  }
});

test('an evidence path inside the outbox or its directory alias cannot overwrite retained data', () => {
  const directory = seed('evidence-collision', 1);
  const alias = join(scratch, 'outbox-alias');
  symlinkSync(directory, alias);
  for (const evidencePath of [batchPath(directory, 1), join(directory, 'evidence.json'),
    join(alias, 'evidence.json')]) {
    const before = snapshot(directory);
    const result = cli(directory, { evidencePath });
    assert.equal(result.child.status, 2);
    assert.equal(result.receipt.fakePosts, 0);
    assert.equal(result.receipt.fullListings, 0);
    assert.deepEqual(snapshot(directory), before);
  }
});

for (const mode of ['run', 'send-and-readback']) {
  test(`future outbox parent aliases refuse evidence containment before ${mode} work`, () => {
    for (const direction of ['evidence-alias', 'outbox-alias']) {
      for (const depth of [1, 2]) {
        const parent = join(scratch, `future-${mode}-${direction}-${depth}`);
        mkdirSync(parent);
        const alias = `${parent}-alias`;
        symlinkSync(parent, alias);
        const suffix = depth === 1 ? ['new-outbox'] : ['new-parent', 'new-outbox'];
        const physicalOutbox = join(parent, ...suffix);
        const directory = join(direction === 'outbox-alias' ? alias : parent, ...suffix);
        const evidencePath = join(direction === 'evidence-alias' ? alias : parent,
          ...suffix, 'evidence.json');
        const result = cli(directory, { mode, evidencePath, noReadback: true,
          fakePost: true, allowWorker: true });
        const observed = JSON.stringify({ receipt: result.receipt,
          outboxCreated: existsSync(physicalOutbox), evidenceExists: existsSync(evidencePath) });
        assert.equal(result.child.status, 2, result.child.stderr);
        assert.equal(result.receipt.subprocessStarts, 0, observed);
        assert.equal(result.receipt.fakePosts, 0, observed);
        assert.equal(result.receipt.fullListings, 0, observed);
        assert.equal(result.receipt.directoryReads, 0, observed);
        assert.equal(existsSync(physicalOutbox), false, observed);
        assert.equal(existsSync(evidencePath), false, observed);
        assert.equal(result.evidence, null);
      }
    }
  });

  test(`dangling evidence parent aliases refuse future outbox ${mode} work`, () => {
    const directory = join(scratch, `dangling-${mode}-outbox`);
    const alias = `${directory}-alias`;
    symlinkSync(directory, alias);
    const evidencePath = join(alias, 'evidence.json');
    const result = cli(directory, { mode, evidencePath, noReadback: true,
      fakePost: true, allowWorker: true });
    const observed = JSON.stringify({ receipt: result.receipt,
      outboxCreated: existsSync(directory), evidenceExists: existsSync(evidencePath) });
    assert.equal(result.child.status, 2, result.child.stderr);
    assert.equal(result.receipt.subprocessStarts, 0, observed);
    assert.equal(result.receipt.fakePosts, 0, observed);
    assert.equal(result.receipt.fullListings, 0, observed);
    assert.equal(result.receipt.directoryReads, 0, observed);
    assert.equal(existsSync(directory), false, observed);
    assert.equal(existsSync(evidencePath), false, observed);
    assert.equal(result.evidence, null);
    assert.equal(readlinkSync(alias), directory);
  });
}

test('a future outbox and an external evidence directory alias still permit an offline run', () => {
  const parent = join(scratch, 'future-external-parent');
  mkdirSync(parent);
  const alias = `${parent}-alias`;
  symlinkSync(parent, alias);
  const directory = join(parent, 'new-parent', 'new-outbox');
  const result = cli(directory, { mode: 'run', allowWorker: true,
    evidencePath: join(alias, 'external-evidence.json') });
  assert.equal(result.child.status, 0, result.child.stderr);
  assert.ok(result.receipt.subprocessStarts > 0);
  assert.equal(result.receipt.fakePosts, 0);
  assert.equal(result.evidence.inspection.complete, true);
  assert.equal(result.evidence.deliveryConfirmed, false);
  assert.equal(result.evidence.run.jobState, 'completed');
});

test('real CLI dispatch and explicit recovery retain uncertain delivery despite synthetic accepted POSTs', () => {
  const directory = seed('consumer-recovery', 1, batch => {
    batch.state = 'pending_send'; batch.readback = null;
  });
  const initial = cli(directory, { noReadback: true, fakePost: true });
  assert.equal(initial.child.status, 1, initial.child.stderr);
  assert.equal(initial.evidence.inspection.complete, true);
  assert.equal(initial.evidence.deliveryConfirmed, false);
  assert.equal(initial.receipt.fakePosts, 1);
  const first = JSON.parse(readFileSync(batchPath(directory, 1), 'utf8'));
  assert.equal(first.state, 'receiver_accepted');
  const originalIntent = JSON.stringify(first.dispatches[0]);
  // A synthetic retained missing result, not a real Public API observation.
  first.state = 'missing';
  first.readback = { status: 'missing', traces: first.plan.traces.map(trace => ({
    traceId: trace.traceId, status: 'missing'
  })) };
  writeFileSync(batchPath(directory, 1), JSON.stringify(first));
  const recovery = cli(directory, { noReadback: true, fakePost: true, args: ['--recover'] });
  assert.equal(recovery.child.status, 1, recovery.child.stderr);
  assert.equal(recovery.evidence.inspection.complete, true);
  assert.equal(recovery.evidence.deliveryConfirmed, false);
  assert.equal(recovery.receipt.fakePosts, 1);
  const second = JSON.parse(readFileSync(batchPath(directory, 1), 'utf8'));
  assert.equal(second.dispatches.length, 2);
  assert.equal(JSON.stringify(second.dispatches[0]), originalIntent);
  assert.equal(second.plan.traces[0].generation, 1);
  assert.notEqual(second.plan.traces[0].traceId, first.plan.traces[0].traceId);
  const resumed = cli(directory, { noReadback: true, fakePost: true });
  assert.equal(resumed.child.status, 1, resumed.child.stderr);
  assert.equal(resumed.receipt.fakePosts, 0);
  assert.equal(resumed.evidence.deliveryConfirmed, false);
  assert.deepEqual(JSON.parse(readFileSync(batchPath(directory, 1), 'utf8')).dispatches, second.dispatches);
});

test('invalid later batch content and invalid cursor data refuse before an earlier pending POST', () => {
  for (const [name, change, reason] of [
    ['invalid-later-json', directory => writeFileSync(batchPath(directory, 2), '{'), 'OUTBOX_BATCH_INVALID'],
    ['invalid-cursor', directory => writeFileSync(join(directory, 'drain-cursor.json'), '{}'), 'OUTBOX_INVENTORY_INVALID'],
    ['duplicate-cursor-key', directory => writeFileSync(join(directory, 'drain-cursor.json'),
      '{"schema":"x","schema":"y"}'), 'OUTBOX_BATCH_INVALID']
  ]) {
    const directory = seed(name, 2, batch => { batch.state = 'pending_send'; batch.readback = null; });
    change(directory);
    const before = snapshot(directory);
    const result = cli(directory, { noReadback: true, fakePost: true });
    refused(result, reason);
    assert.deepEqual(snapshot(directory), before);
  }
});

test('a file changed after a synthetic POST yields explicitly incomplete after-drain evidence', () => {
  const directory = seed('changed-after-drain', 1, batch => { batch.state = 'pending_send'; batch.readback = null; });
  const result = cli(directory, { noReadback: true, fakePost: true, overflowAfterPost: true });
  assert.equal(result.child.status, 1, result.child.stderr);
  assert.equal(result.receipt.fakePosts, 1);
  assert.equal(result.evidence.inspection.complete, false);
  assert.equal(result.evidence.inspection.phase, 'after_drain');
  assert.equal(result.evidence.inspection.reasonCode, 'OUTBOX_FILE_LIMIT');
  assert.equal(result.evidence.deliveryConfirmed, false);
  assert.equal(result.output.deliveryConfirmed, false);
  assert.equal(result.evidence.drainPage.length, 1);
  assert.deepEqual(result.evidence.batches, []);
  assert.equal(readFileSync(batchPath(directory, 1)).length, MAX_BATCH_BYTES + 1);
});

test('a bounded stdout document larger than a pipe buffer is fully flushed as valid JSON', () => {
  const directory = seed('flush-stdout', 20, batch => { batch.send = { status: 'x'.repeat(2500) }; });
  const result = cli(directory);
  assert.equal(result.child.status, 0, result.child.stderr);
  assert.equal(result.output.send.length, 20);
  assert.equal(result.output.send.at(-1).length, 2500);
  assert.ok(Buffer.byteLength(result.child.stdout) > 48 * 1024);
  assert.ok(Buffer.byteLength(result.child.stdout) <= 64 * 1024);
  assert.equal(result.evidence.inspection.complete, true);
  assert.equal(result.evidence.deliveryConfirmed, true);
});

test('the actual offline CLI still runs the local worker and publishes bounded non-delivery evidence', () => {
  const directory = join(scratch, 'offline-cli');
  const result = cli(directory, { mode: 'run', allowWorker: true });
  assert.equal(result.child.status, 0, result.child.stderr);
  assert.ok(result.receipt.subprocessStarts > 0);
  assert.equal(result.receipt.fakePosts, 0);
  assert.equal(result.evidence.run.jobState, 'completed');
  assert.deepEqual(result.evidence.run.results, { sum: 42, label: 'agent-harness runtime trace' });
  assert.equal(result.evidence.inspection.complete, true);
  assert.equal(result.evidence.deliveryConfirmed, false);
  assert.equal(result.evidence.workflow.harnessMode, 'mock');
  assert.equal(result.evidence.evidenceClass, 'offline-runtime-application');
  assert.ok(result.evidence.batches.length > 0);
  assert.ok(result.evidence.batches.every(batch => Object.keys(batch).length === 1 && typeof batch.path === 'string'));
});

function refusedFreshWorkflow(result) {
  assert.equal(result.child.status, 1, result.child.stderr);
  assert.equal(result.evidence.inspection.complete, true);
  assert.equal(result.evidence.execution.status, 'refused');
  assert.equal(result.evidence.execution.reasonCode, 'OUTBOX_REQUIRES_FRESH_WORKFLOW');
  assert.equal(result.evidence.execution.telemetryComplete, false);
  assert.deepEqual(result.evidence.execution.sourceState, { disposition: 'not_created', directory: null,
    purpose: 'private_diagnostics_only', automaticReplaySupported: false });
  assert.equal(result.evidence.run, null);
  assert.equal(result.evidence.deliveryConfirmed, false);
  assert.equal(result.output.deliveryConfirmed, false);
  assert.equal(result.output.execution.reasonCode, 'OUTBOX_REQUIRES_FRESH_WORKFLOW');
  assert.equal(result.receipt.subprocessStarts, 0);
  assert.equal(result.receipt.fakePosts, 0);
  assert.deepEqual(result.receipt.runtimeDirectories, []);
  assert.deepEqual(result.receipt.runtimeCleanups, []);
  assert.equal(result.evidence.drainPage, undefined);
}

test('fresh workflow modes refuse retained history before work at capacity and below capacity, including sequence collisions', () => {
  for (const mode of ['run', 'send-and-readback']) {
    for (const [sequence, limit] of [[1, 1], [1, 128], [1000, 1], [1000, 128]]) {
      const directory = seed(`fresh-refusal-${mode}-${sequence}-${limit}`, 1);
      if (sequence !== 1) {
        const batch = JSON.parse(readFileSync(batchPath(directory, 1), 'utf8'));
        batch.firstSequence = sequence;
        batch.lastSequence = sequence;
        batch.records = [record(sequence)];
        writeFileSync(batchPath(directory, 1), JSON.stringify(batch));
        renameSync(batchPath(directory, 1), batchPath(directory, sequence));
      }
      const before = snapshot(directory);
      const result = cli(directory, { mode, allowWorker: true,
        args: ['--max-retained-batches', String(limit)] });
      refusedFreshWorkflow(result);
      assert.deepEqual(snapshot(directory), before);
      // The unchanged retained history remains drainable. This synthetic
      // verified fixture proves neither this refused run nor a live provider.
      const drained = cli(directory, { args: ['--max-retained-batches', String(limit)] });
      assert.equal(drained.child.status, 0, drained.child.stderr);
      assert.equal(drained.evidence.deliveryConfirmed, true);
      assert.equal(drained.evidence.execution.status, 'not_requested');
      assert.equal(drained.receipt.subprocessStarts, 0);
    }
  }
});

test('a valid cursor without retained batches is history and cannot admit a fresh SQLite sequence namespace', () => {
  for (const mode of ['run', 'send-and-readback']) {
    const directory = join(scratch, `cursor-only-${mode}`);
    mkdirSync(directory);
    writeFileSync(join(directory, 'drain-cursor.json'), JSON.stringify({
      schema: 'agent-harness.neatlogs.outbox-drain-cursor.v1', lastBatch: 'batch-0000000000000001-0000000000000001.json'
    }));
    const before = snapshot(directory);
    refusedFreshWorkflow(cli(directory, { mode, allowWorker: true }));
    assert.deepEqual(snapshot(directory), before);
  }
});

test('a successful fresh offline run proves its export, accepts intentional policy drops and refuses a second fresh run', () => {
  const directory = join(scratch, 'fresh-then-reuse');
  const first = cli(directory, { mode: 'run', allowWorker: true });
  assert.equal(first.child.status, 0, first.child.stderr);
  assert.equal(first.evidence.execution.status, 'completed');
  assert.equal(first.evidence.execution.telemetryComplete, true);
  assert.equal(first.evidence.execution.sourceState.disposition, 'removed_after_complete_export');
  assert.equal(first.evidence.execution.sourceState.directory, null);
  assert.equal(first.evidence.run.telemetry.pendingRecords, 0);
  assert.equal(first.evidence.run.telemetry.pendingCapped, false);
  assert.equal(first.evidence.run.telemetry.lastErrorCode, null);
  assert.equal(first.evidence.run.telemetry.deliveredRecords, 4);
  assert.equal(first.evidence.run.telemetry.policyDroppedRecords, 4);
  assert.equal(first.evidence.run.telemetry.cursor, 8);
  assert.equal(first.receipt.runtimeCleanups.length, 1);
  assert.equal(first.receipt.runtimeCleanups[0].databaseExistedBefore, true);
  assert.equal(first.receipt.runtimeCleanups[0].directoryExistsAfter, false);
  const before = snapshot(directory);
  for (const mode of ['run', 'send-and-readback']) {
    refusedFreshWorkflow(cli(directory, { mode, allowWorker: true, noReadback: true, fakePost: true }));
    assert.deepEqual(snapshot(directory), before);
  }
});

function retainedSource(result) {
  assert.equal(result.child.status, 1, result.child.stderr);
  assert.equal(result.evidence.execution.telemetryComplete, false);
  assert.equal(result.evidence.deliveryConfirmed, false);
  assert.equal(result.output.deliveryConfirmed, false);
  assert.equal(result.receipt.fakePosts, 0);
  assert.deepEqual(result.receipt.runtimeCleanups, []);
  assert.equal(result.receipt.runtimeDirectories.length, 1);
  const state = result.evidence.execution.sourceState;
  assert.equal(state.disposition, 'retained');
  assert.equal(state.purpose, 'private_diagnostics_only');
  assert.equal(state.automaticReplaySupported, false);
  assert.equal(state.directory, result.receipt.runtimeDirectories[0]);
  assert.equal(lstatSync(state.directory).mode & 0o777, 0o700);
  assert.ok(existsSync(join(state.directory, 'workflow.sqlite')));
  assert.equal(result.evidence.drainPage, undefined);
  assert.ok(Buffer.byteLength(result.evidenceText) <= 8 * 1024 * 1024);
  assert.ok(Buffer.byteLength(result.child.stdout) <= 64 * 1024);
  return state.directory;
}

test('fresh cap-one backpressure keeps pending SQLite events and cannot turn partial export into success', () => {
  for (const mode of ['run', 'send-and-readback']) {
    const directory = join(scratch, `fresh-cap-one-${mode}`);
    const result = cli(directory, { mode, allowWorker: true, noReadback: true, fakePost: true,
      args: ['--max-retained-batches', '1'] });
    const stateDirectory = retainedSource(result);
    assert.equal(result.evidence.execution.status, 'incomplete');
    assert.equal(result.evidence.execution.reasonCode, 'RUNTIME_EXPORT_INCOMPLETE');
    assert.equal(result.evidence.run.jobState, 'completed');
    assert.equal(result.evidence.run.telemetry.deliveredRecords, 3);
    assert.equal(result.evidence.run.telemetry.policyDroppedRecords, 4);
    assert.equal(result.evidence.run.telemetry.pendingRecords, 1);
    assert.equal(result.evidence.run.telemetry.pendingCapped, false);
    assert.equal(result.evidence.run.telemetry.lastErrorCode, 'TELEMETRY_SINK_UNAVAILABLE');
    assert.equal(existsSync(join(directory, 'drain-cursor.json')), false);
    const store = DurableWorkflowStore.open({ databasePath: join(stateDirectory, 'workflow.sqlite') });
    try {
      const pending = store.readEventPage({ afterSequence: result.evidence.run.telemetry.cursor, limit: 64 }).events;
      assert.equal(pending.length, 1);
      assert.equal(pending[0].sequence, 8);
      assert.equal(pending[0].type, 'operations_work_completed');
      for (const row of store.readEventPage({ afterSequence: 0, limit: 64 }).events) {
        assert.ok(!result.evidenceText.includes(row.eventId));
        assert.ok(!result.child.stdout.includes(row.eventId));
      }
    } finally { store.close(); }
    const sourceBefore = snapshot(stateDirectory);
    refusedFreshWorkflow(cli(directory, { mode: 'run', allowWorker: true }));
    const drained = cli(directory, { noReadback: true, fakePost: true, args: ['--max-retained-batches', '2'] });
    assert.equal(drained.child.status, 1, drained.child.stderr);
    assert.equal(drained.receipt.fakePosts, 1);
    assert.equal(drained.evidence.execution.status, 'not_requested');
    assert.equal(drained.evidence.run, null);
    assert.equal(drained.evidence.deliveryConfirmed, false);
    assert.deepEqual(snapshot(stateDirectory), sourceBefore);
  }
});

test('fresh filesystem admission failure retains unknown source progress in both workflow modes', () => {
  for (const mode of ['run', 'send-and-readback']) {
    const directory = join(scratch, `fresh-admission-write-failure-${mode}`);
    const result = cli(directory, { mode, allowWorker: true, noReadback: true, fakePost: true,
      refuseBatchWrites: true });
    retainedSource(result);
    assert.equal(result.evidence.execution.reasonCode, 'RUNTIME_EXPORT_INCOMPLETE');
    assert.ok(result.receipt.refusedBatchWrites > 0);
    assert.equal(result.evidence.run.telemetry.pendingRecords, 8);
    assert.equal(result.evidence.run.telemetry.deliveredRecords, 0);
    assert.equal(result.evidence.run.telemetry.lastErrorCode, 'TELEMETRY_SINK_UNAVAILABLE');
    assert.equal(result.evidence.inspection.retainedBatchCount, 0);
    assert.deepEqual(snapshot(directory), []);
  }
});

test('a worker failure before returned export diagnostics preserves the SQLite source as unproven', () => {
  const directory = join(scratch, 'fresh-worker-start-failure');
  const result = cli(directory, { mode: 'run', allowWorker: true, failWorkerStart: true });
  retainedSource(result);
  assert.equal(result.evidence.execution.status, 'failed');
  assert.equal(result.evidence.execution.reasonCode, 'RUNTIME_WORKFLOW_FAILED');
  assert.equal(result.evidence.run, null);
});

test('retained verified batches cannot override an explicitly incomplete current execution', () => {
  const input = { network: true, batches: [{ readback: { status: 'verified' } }], batchCount: 1 };
  assert.equal(isCompleteDeliveryEvidence({ ...input, executionComplete: false }), false);
  assert.equal(isCompleteDeliveryEvidence({ ...input, executionComplete: true }), true);
});
