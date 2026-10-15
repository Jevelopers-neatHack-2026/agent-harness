import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import {
  existsSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, readdirSync,
  renameSync, rmSync, statSync, symlinkSync, writeFileSync
} from 'node:fs';
import net from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import tls from 'node:tls';
import { after, before, test } from 'node:test';
import { DurableWorkflowStore } from '../../../src/internal/durable-store.js';
import { loadNeatlogsConfig } from '../../../src/observability/private/neatlogs-config.js';
import {
  createNeatlogsOperationsTelemetry, createNeatlogsOutboxSink, drainNeatlogsOutbox,
  listNeatlogsOutbox, runtimeNeatlogsTags
} from '../../../src/observability/private/neatlogs-outbox.js';

const MODULE_URL = new URL('../../../src/observability/private/neatlogs-outbox.js', import.meta.url).href;
const REFUSED = Object.freeze({ accepted: false, acceptedCount: 0 });
const MAX_BATCH_BYTES = 2 * 1024 * 1024;
let scratch;
let realNetworkAttempts = 0;
let originalFetch;
let originalConnect;
let originalTls;

before(() => {
  scratch = mkdtempSync(join(tmpdir(), 'neatlogs-file-admission-'));
  originalFetch = globalThis.fetch;
  originalConnect = net.Socket.prototype.connect;
  originalTls = tls.connect;
  const deny = () => { realNetworkAttempts += 1; throw new Error('real network forbidden'); };
  globalThis.fetch = deny;
  net.Socket.prototype.connect = deny;
  tls.connect = deny;
});

after(() => {
  globalThis.fetch = originalFetch;
  net.Socket.prototype.connect = originalConnect;
  tls.connect = originalTls;
  rmSync(scratch, { recursive: true, force: true });
  assert.equal(realNetworkAttempts, 0);
});

function record(sequence) {
  return { schema: 'agent-harness.telemetry.v1', sequence,
    eventId: `tok1_${sequence.toString(16).padStart(64, '0')}`,
    workflowId: `tok1_${'f'.repeat(64)}`, revision: 0, eventType: 'workflow_created',
    fromState: null, toState: null, fence: 0, occurredAtMs: 1_760_000_000_000 };
}

function snapshots(directory) {
  return readdirSync(directory).sort().map(name => {
    const path = join(directory, name);
    const stats = lstatSync(path);
    return { name, mode: stats.mode, size: stats.size,
      bytes: stats.isFile() ? readFileSync(path).toString('base64') : null };
  });
}

function seed(directory, count) {
  const sink = createNeatlogsOutboxSink({ directory, maxRetainedBatches: Math.max(count, 1) });
  for (let sequence = 1; sequence <= count; sequence += 1) {
    assert.deepEqual(sink([record(sequence)]), { accepted: true, acceptedCount: 1 });
  }
}

function runChild(script, ...args) {
  const guard = `
    import net from 'node:net';
    import tls from 'node:tls';
    let realNetworkAttempts = 0;
    const deny = () => { realNetworkAttempts += 1; throw new Error('real network forbidden'); };
    globalThis.fetch = deny; net.Socket.prototype.connect = deny; tls.connect = deny;
    process.on('exit', () => { if (realNetworkAttempts !== 0) process.exitCode = 81; });
  `;
  const result = spawnSync(process.execPath,
    ['--input-type=module', '--eval', `${guard}\n${script}`, MODULE_URL, ...args], {
      encoding: 'utf8', timeout: 15_000, maxBuffer: 64 * 1024,
      env: { PATH: process.env.PATH, TMPDIR: tmpdir(), NODE_NO_WARNINGS: '1' }
    });
  assert.equal(result.error, undefined);
  assert.equal(result.signal, null);
  assert.equal(result.status, 0, result.stderr);
  return JSON.parse(result.stdout);
}

test('invalid admission limits fail before directory creation with a bounded typed error', () => {
  const invalid = [0, -1, 1.5, 1025, Number.MAX_SAFE_INTEGER, Infinity, NaN, '2', null, true, {}];
  for (const [index, maxRetainedBatches] of invalid.entries()) {
    const directory = join(scratch, `invalid-limit-${index}`);
    assert.throws(() => createNeatlogsOutboxSink({ directory, maxRetainedBatches }), error => {
      assert.equal(error.code, 'INVALID_OUTBOX_LIMIT');
      assert.equal(error.message, 'Neatlogs outbox operation failed (INVALID_OUTBOX_LIMIT).');
      return true;
    });
    assert.equal(existsSync(directory), false);
  }
  for (const maxRetainedBatches of [1, 128, 1024]) {
    const sink = createNeatlogsOutboxSink({ directory: join(scratch, `valid-limit-${maxRetainedBatches}`), maxRetainedBatches });
    assert.deepEqual(sink([record(1)]), { accepted: true, acceptedCount: 1 });
  }
});

test('the finite default admits 128 retained files and backpressures the next without pruning', () => {
  const directory = join(scratch, 'default-limit');
  const sink = createNeatlogsOutboxSink({ directory });
  for (let sequence = 1; sequence <= 128; sequence += 1) {
    assert.deepEqual(sink([record(sequence)]), { accepted: true, acceptedCount: 1 });
  }
  const before = snapshots(directory);
  assert.deepEqual(sink([record(129)]), REFUSED);
  assert.equal(listNeatlogsOutbox(directory).length, 128);
  assert.deepEqual(snapshots(directory), before);
  assert.deepEqual(sink([record(128)]), { accepted: true, acceptedCount: 1 });
  assert.deepEqual(snapshots(directory), before);
});

test('capacity preserves prefix replay, immutable conflicts and later admission after an explicit larger limit', () => {
  const directory = join(scratch, 'prefix-retry');
  const sink = createNeatlogsOutboxSink({ directory, maxRetainedBatches: 2 });
  assert.deepEqual(sink([record(1), record(2)]), { accepted: true, acceptedCount: 2 });
  assert.deepEqual(sink([record(1), record(2), record(3)]), { accepted: true, acceptedCount: 3 });
  const before = snapshots(directory);
  assert.deepEqual(sink([record(1), record(2), record(3)]), { accepted: true, acceptedCount: 3 });
  assert.deepEqual(sink([record(2), record(3)]), { accepted: true, acceptedCount: 2 });
  assert.deepEqual(sink([record(1), record(2), record(3), record(4)]), REFUSED);
  assert.throws(() => sink([{ ...record(2), occurredAtMs: 1_760_000_000_001 }]), { code: 'BATCH_CONFLICT' });
  assert.deepEqual(snapshots(directory), before);
  const reopened = createNeatlogsOutboxSink({ directory, maxRetainedBatches: 3 });
  assert.deepEqual(reopened([record(1), record(2), record(3), record(4)]), { accepted: true, acceptedCount: 4 });
  assert.deepEqual(snapshots(directory).slice(0, 2), before);
  assert.equal(listNeatlogsOutbox(directory).length, 3);
});

test('over-cap legacy, unknown and verified fixture artifacts remain intact and receive no implicit settlement', () => {
  const directory = join(scratch, 'preexisting-over-cap');
  seed(directory, 3);
  // Constructed retained-state fixtures, not provider readback or Finance acceptance.
  const paths = listNeatlogsOutbox(directory);
  const legacy = JSON.parse(readFileSync(paths[0], 'utf8'));
  legacy.schema = 'agent-harness.neatlogs.outbox-batch.v1';
  delete legacy.dispatches;
  legacy.state = 'legacy_held';
  const unknown = JSON.parse(readFileSync(paths[1], 'utf8'));
  unknown.send = { status: 'outcome_unknown' };
  const verified = JSON.parse(readFileSync(paths[2], 'utf8'));
  verified.state = 'verified';
  verified.readback = { status: 'verified' };
  [legacy, unknown, verified].forEach((batch, index) => writeFileSync(paths[index], JSON.stringify(batch)));
  const before = snapshots(directory);
  const overCap = createNeatlogsOutboxSink({ directory, maxRetainedBatches: 2 });
  assert.deepEqual(overCap([record(4)]), REFUSED);
  assert.deepEqual(overCap([record(1)]), REFUSED, 'an incomplete inventory cannot certify replay');
  assert.deepEqual(snapshots(directory), before);
  const exactCapacity = createNeatlogsOutboxSink({ directory, maxRetainedBatches: 3 });
  for (let sequence = 1; sequence <= 3; sequence += 1) {
    assert.deepEqual(exactCapacity([record(sequence)]), { accepted: true, acceptedCount: 1 });
  }
  assert.deepEqual(snapshots(directory), before);
});

test('one valid drain cursor is outside the batch count, including legacy-width batch names', () => {
  const directory = join(scratch, 'cursor-and-legacy-name');
  seed(directory, 1);
  const legacyName = 'batch-000000000001-000000000001.json';
  renameSync(listNeatlogsOutbox(directory)[0], join(directory, legacyName));
  const cursor = `${JSON.stringify({ schema: 'agent-harness.neatlogs.outbox-drain-cursor.v1', lastBatch: legacyName })}\n`;
  writeFileSync(join(directory, 'drain-cursor.json'), cursor);
  const sink = createNeatlogsOutboxSink({ directory, maxRetainedBatches: 2 });
  assert.deepEqual(sink([record(2)]), { accepted: true, acceptedCount: 1 });
  const before = snapshots(directory);
  assert.deepEqual(sink([record(1), record(2)]), { accepted: true, acceptedCount: 2 });
  assert.deepEqual(sink([record(3)]), REFUSED);
  assert.deepEqual(snapshots(directory), before);
});

test('unexpected, temporary, nonregular and oversized inventory stays intact and refuses admission', () => {
  const cases = [
    ['evidence', directory => writeFileSync(join(directory, 'evidence.json'), '{}')],
    ['temporary', directory => writeFileSync(join(directory, 'batch-0000000000000001-0000000000000001.json.1.saved.tmp'), 'partial')],
    ['directory', directory => mkdirSync(join(directory, 'batch-0000000000000002-0000000000000002.json'))],
    ['symlink', directory => symlinkSync(join(directory, 'batch-0000000000000001-0000000000000001.json'),
      join(directory, 'batch-0000000000000002-0000000000000002.json'))],
    ['oversized', directory => writeFileSync(join(directory, 'batch-0000000000000002-0000000000000002.json'), Buffer.alloc(MAX_BATCH_BYTES + 1))],
    ['oversized-cursor', directory => writeFileSync(join(directory, 'drain-cursor.json'), Buffer.alloc(257))],
    ['cursor-symlink', directory => symlinkSync(join(directory, 'batch-0000000000000001-0000000000000001.json'), join(directory, 'drain-cursor.json'))]
  ];
  for (const [name, add] of cases) {
    const directory = join(scratch, `inventory-${name}`);
    seed(directory, 1);
    add(directory);
    const before = snapshots(directory);
    const sink = createNeatlogsOutboxSink({ directory, maxRetainedBatches: 3 });
    assert.deepEqual(sink([record(3)]), REFUSED, name);
    assert.deepEqual(snapshots(directory), before, name);
  }
});

test('inventory refuses after cap plus one entries, closes the stream and never materializes a full directory array', () => {
  const directory = join(scratch, 'streamed-inventory');
  seed(directory, 6);
  const before = snapshots(directory);
  const result = runChild(`
    import assert from 'node:assert/strict';
    import fs from 'node:fs';
    import { syncBuiltinESMExports } from 'node:module';
    const { createNeatlogsOutboxSink } = await import(process.argv[1]);
    const originalOpen = fs.opendirSync;
    let reads = 0; let closes = 0;
    fs.readdirSync = () => { throw new Error('unbounded directory array forbidden'); };
    fs.opendirSync = (...args) => {
      const directory = originalOpen(...args);
      return {
        readSync() { reads += 1; return directory.readSync(); },
        closeSync() { closes += 1; directory.closeSync(); }
      };
    };
    syncBuiltinESMExports();
    const result = createNeatlogsOutboxSink({ directory: process.argv[2], maxRetainedBatches: 2 })(${JSON.stringify([record(7)])});
    assert.deepEqual(result, { accepted: false, acceptedCount: 0 });
    assert.equal(reads, 3); assert.equal(closes, 1);
    process.stdout.write(JSON.stringify({ result, reads, closes, realNetworkAttempts }));
  `, directory);
  assert.deepEqual(result.result, REFUSED);
  assert.equal(result.realNetworkAttempts, 0);
  assert.deepEqual(snapshots(directory), before);
});

test('admission accounts for retained slots as real synthetic dispatch history grows', async () => {
  const directory = join(scratch, 'growing-history');
  const sink = createNeatlogsOutboxSink({ directory, maxRetainedBatches: 1 });
  sink(Array.from({ length: 32 }, (_, index) => record(index + 1)));
  const path = listNeatlogsOutbox(directory)[0];
  const initialBytes = statSync(path).size;
  const config = loadNeatlogsConfig({ env: { NEATLOGS_INGEST_KEY: 'synthetic-admission-ingest-0123456789abcdef' } });
  const tags = runtimeNeatlogsTags({ revision: 'b'.repeat(40), harnessMode: 'mock' });
  let posts = 0;
  for (let call = 0; call < 3; call += 1) {
    await drainNeatlogsOutbox({ directory, config, tags, readback: false,
      fetch: async () => { posts += 1; return new Response(null, { status: 503 }); },
      sendOptions: { maxAttempts: 1 }
    });
  }
  assert.equal(posts, 3);
  const grown = readFileSync(path);
  const batch = JSON.parse(grown);
  assert.ok(grown.length > initialBytes);
  assert.ok(grown.length <= MAX_BATCH_BYTES);
  assert.equal(batch.dispatches[0].calls.length, 3);
  assert.equal(batch.send.status, 'outcome_unknown');
  assert.deepEqual(sink([record(33)]), REFUSED);
  assert.deepEqual(sink([record(1), record(2)]), { accepted: true, acceptedCount: 2 });
  assert.deepEqual(readFileSync(path), grown);
  assert.equal(listNeatlogsOutbox(directory).length, 1);
});

test('failure before rename or after publication can replay durably at the exact capacity', () => {
  for (const fault of ['file-fsync', 'rename', 'directory-fsync']) {
    const directory = join(scratch, `durability-${fault}`);
    const result = runChild(`
      import assert from 'node:assert/strict';
      import fs from 'node:fs';
      import { syncBuiltinESMExports } from 'node:module';
      const { createNeatlogsOutboxSink } = await import(process.argv[1]);
      const originalSync = fs.fsyncSync; const originalRename = fs.renameSync;
      const fault = process.argv[3]; let syncs = 0;
      fs.fsyncSync = fd => {
        syncs += 1;
        if ((fault === 'file-fsync' && syncs === 1) || (fault === 'directory-fsync' && syncs === 2)) {
          throw Object.assign(new Error('synthetic durability fault'), { code: 'SYNTHETIC_FSYNC' });
        }
        return originalSync(fd);
      };
      fs.renameSync = (...args) => {
        if (fault === 'rename') throw Object.assign(new Error('synthetic rename fault'), { code: 'SYNTHETIC_RENAME' });
        return originalRename(...args);
      };
      syncBuiltinESMExports();
      const sink = createNeatlogsOutboxSink({ directory: process.argv[2], maxRetainedBatches: 1 });
      assert.throws(() => sink(${JSON.stringify([record(1)])}));
      const afterFault = fs.readdirSync(process.argv[2]);
      assert.equal(afterFault.length, fault === 'directory-fsync' ? 1 : 0);
      const published = afterFault.length ? fs.readFileSync(process.argv[2] + '/' + afterFault[0]) : null;
      fs.fsyncSync = originalSync; fs.renameSync = originalRename; syncBuiltinESMExports();
      assert.deepEqual(sink(${JSON.stringify([record(1)])}), { accepted: true, acceptedCount: 1 });
      const finalNames = fs.readdirSync(process.argv[2]);
      assert.equal(finalNames.length, 1);
      if (published) assert.deepEqual(fs.readFileSync(process.argv[2] + '/' + finalNames[0]), published);
      assert.deepEqual(sink(${JSON.stringify([record(2)])}), { accepted: false, acceptedCount: 0 });
      process.stdout.write(JSON.stringify({ fault, filesAfterFault: afterFault.length, filesAfterReplay: finalNames.length, realNetworkAttempts }));
    `, directory, fault);
    assert.equal(result.filesAfterReplay, 1);
    assert.equal(result.realNetworkAttempts, 0);
  }
});

test('the actual local exporter preserves its pending cursor and synthetic authoritative store data at capacity', async () => {
  const directory = join(scratch, 'exporter-backpressure');
  const databasePath = join(scratch, 'exporter.sqlite');
  const store = DurableWorkflowStore.open({ databasePath, clock: () => 1000 });
  store.createWorkflow({ workflowId: 'synthetic-admission-workflow' });
  const lease = store.acquireLease({ workflowId: 'synthetic-admission-workflow', ownerId: 'synthetic-worker', ttlMs: 5000 });
  const running = store.transitionWorkflow({ ...lease, expectedRevision: 0, toState: 'running' });
  store.recordDispatchIntent({ ...lease, expectedRevision: running.revision,
    attemptId: 'synthetic-attempt', logicalOperationRef: 'synthetic-operation',
    authorityHoldRef: 'synthetic-conservative-hold', transportAttemptId: 'synthetic-transport' });
  store.recordAttemptObservation({ attemptId: 'synthetic-attempt', sourceObservationRef: 'synthetic-unknown', kind: 'unknown' });
  const dataSnapshot = () => {
    const db = new DatabaseSync(databasePath, { readOnly: true });
    try {
      return ['operation_attempts', 'transport_attempts', 'attempt_observations', 'outbox', 'workflow_events']
        .map(table => db.prepare(`SELECT * FROM ${table} ORDER BY 1`).all());
    } finally { db.close(); }
  };
  const before = dataSnapshot();
  const telemetry = createNeatlogsOperationsTelemetry({ databasePath, outboxDirectory: directory,
    maxRetainedBatches: 1, limits: { batchSize: 1, queueRecords: 1, maxAttempts: 1, maxBatchesPerFlush: 8, retryDelayMs: 0 } });
  try {
    const first = await telemetry.exporter.flush();
    assert.equal(first.cursor, 1);
    assert.ok(first.pendingRecords > 0);
    assert.equal(first.deliveredRecords, 1);
    assert.equal(first.policyDroppedRecords, 0);
    assert.equal(first.lastErrorCode, 'TELEMETRY_SINK_UNAVAILABLE');
    const retained = snapshots(directory);
    const second = await telemetry.exporter.flush();
    assert.equal(second.cursor, first.cursor);
    assert.equal(second.pendingRecords, first.pendingRecords);
    assert.equal(second.deliveredRecords, first.deliveredRecords);
    assert.equal(second.policyDroppedRecords, 0);
    assert.deepEqual(snapshots(directory), retained);
    assert.deepEqual(dataSnapshot(), before);
    await telemetry.exporter.close();
    assert.deepEqual(dataSnapshot(), before);
  } finally {
    telemetry.close();
    store.close();
  }
});
