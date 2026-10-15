import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  existsSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, readdirSync,
  renameSync, rmSync, writeFileSync
} from 'node:fs';
import net from 'node:net';
import { tmpdir } from 'node:os';
import { basename, join } from 'node:path';
import tls from 'node:tls';
import { after, before, test } from 'node:test';
import { loadNeatlogsConfig } from '../../../src/observability/private/neatlogs-config.js';
import {
  createNeatlogsOutboxSink, drainNeatlogsOutbox, iterateNeatlogsOutboxSnapshot,
  listNeatlogsOutbox, NEATLOGS_OUTBOX_SCHEMA, runtimeNeatlogsTags
} from '../../../src/observability/private/neatlogs-outbox.js';

const MODULE_URL = new URL('../../../src/observability/private/neatlogs-outbox.js', import.meta.url).href;
const TAGS = runtimeNeatlogsTags({ revision: 'b'.repeat(40), harnessMode: 'mock' });
const LIMIT_ERROR = 'OUTBOX_INVENTORY_LIMIT_REACHED';
const CURSOR_FILE = 'drain-cursor.json';
let scratch;
let originalFetch;
let originalConnect;
let originalTls;
let realNetworkAttempts = 0;

before(() => {
  scratch = mkdtempSync(join(tmpdir(), 'neatlogs-core-enumeration-'));
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

function batchName(sequence, width = 16) {
  const part = String(sequence).padStart(width, '0');
  return `batch-${part}-${part}.json`;
}

// Explicit synthetic on-disk fixtures, including directories predating an
// admission cap. Only the boundary test below claims an actual sink admission.
function seed(name, count) {
  const directory = join(scratch, name);
  mkdirSync(directory);
  for (let sequence = 1; sequence <= count; sequence += 1) {
    writeFileSync(join(directory, batchName(sequence)), JSON.stringify({
      schema: NEATLOGS_OUTBOX_SCHEMA, state: 'pending_send',
      firstSequence: sequence, lastSequence: sequence, records: [record(sequence)],
      dispatches: [], plan: null, send: null, readback: null
    }));
  }
  return directory;
}

function addIgnored(directory, count) {
  for (let index = 0; index < count; index += 1) {
    writeFileSync(join(directory, `retained-${index}.tmp`), `retained diagnostic ${index}\n`);
  }
}

function setCursor(directory, lastBatch) {
  writeFileSync(join(directory, CURSOR_FILE), JSON.stringify({
    schema: 'agent-harness.neatlogs.outbox-drain-cursor.v1', lastBatch
  }));
}

function snapshot(directory) {
  return readdirSync(directory).sort().map(name => {
    const path = join(directory, name);
    const stat = lstatSync(path);
    return { name, mode: stat.mode, size: stat.size,
      sha256: stat.isFile() ? createHash('sha256').update(readFileSync(path)).digest('hex') : null };
  });
}

async function observe(operation, directory, { send = false, readback = false } = {}) {
  let returned = 0;
  let posts = 0;
  let error = null;
  try {
    if (operation === 'list') returned = listNeatlogsOutbox(directory).length;
    else if (operation === 'snapshot') {
      for (const _batch of iterateNeatlogsOutboxSnapshot(directory)) returned += 1;
    } else {
      const config = loadNeatlogsConfig({
        env: { NEATLOGS_INGEST_KEY: 'synthetic-enumeration-ingest-0123456789abcdef' }
      });
      const result = await drainNeatlogsOutbox({ directory, config, tags: TAGS, send, readback,
        fetch: async () => { posts += 1; return new Response(null, { status: 200 }); },
        sendOptions: { maxAttempts: 1 }
      });
      returned = result.length;
    }
  } catch (caught) {
    error = { name: caught.name, code: caught.code, message: caught.message };
  }
  return { operation, returned, posts, error, cursorExists: existsSync(join(directory, CURSOR_FILE)) };
}

function assertRefused(observation) {
  assert.deepEqual(observation.error, { name: 'NeatlogsOutboxError', code: LIMIT_ERROR,
    message: `Neatlogs outbox operation failed (${LIMIT_ERROR}).` });
  assert.equal(observation.returned, 0, 'an incomplete inventory must not expose a prefix');
  assert.equal(observation.posts, 0, 'inventory refusal must precede provider work');
}

// A fresh process intercepts only the directory API under test. A full-array
// fallback is forbidden; the real directory stream is still read and closed.
function streamProbe(directory, operation, failRead = 0) {
  const script = `
    import fs from 'node:fs';
    import { syncBuiltinESMExports } from 'node:module';
    import net from 'node:net';
    import tls from 'node:tls';
    let network = 0;
    const deny = () => { network += 1; throw new Error('real network forbidden'); };
    globalThis.fetch = deny; net.Socket.prototype.connect = deny; tls.connect = deny;
    const [moduleUrl, root, operation, failAt] = process.argv.slice(1);
    const originalOpen = fs.opendirSync;
    const originalArray = fs.readdirSync;
    const fault = Object.assign(new Error('injected directory read failure'), { code: 'EIO' });
    let reads = 0; let closes = 0; let arrays = 0; let bufferSize = null;
    fs.readdirSync = (path, ...args) => {
      if (path === root) { arrays += 1; throw new Error('unbounded directory array forbidden'); }
      return originalArray(path, ...args);
    };
    fs.opendirSync = (path, options) => {
      const directory = originalOpen(path, options);
      if (path !== root) return directory;
      bufferSize = options?.bufferSize;
      return {
        readSync() { reads += 1; if (reads === Number(failAt)) throw fault; return directory.readSync(); },
        closeSync() { closes += 1; return directory.closeSync(); }
      };
    };
    syncBuiltinESMExports();
    const api = await import(moduleUrl);
    let returned = 0; let error = null; let originalFault = false;
    try {
      if (operation === 'list') returned = api.listNeatlogsOutbox(root).length;
      else if (operation === 'snapshot') {
        for (const batch of api.iterateNeatlogsOutboxSnapshot(root)) { returned += 1; break; }
      } else {
        const result = await api.drainNeatlogsOutbox({ directory: root, config: {},
          tags: ${JSON.stringify(TAGS)}, send: false, readback: Number(failAt) > 0 });
        returned = result.length;
      }
    } catch (caught) {
      error = { name: caught.name, code: caught.code, message: caught.message };
      originalFault = caught === fault;
    }
    process.stdout.write(JSON.stringify({ reads, closes, arrays, bufferSize, returned, error, originalFault, network }));
    if (network !== 0) process.exitCode = 81;
  `;
  const result = spawnSync(process.execPath,
    ['--input-type=module', '--eval', script, MODULE_URL, directory, operation, String(failRead)], {
      encoding: 'utf8', timeout: 15_000, maxBuffer: 64 * 1024,
      env: { PATH: process.env.PATH, TMPDIR: tmpdir(), NODE_NO_WARNINGS: '1' }
    });
  assert.equal(result.error, undefined);
  assert.equal(result.signal, null);
  assert.equal(result.status, 0, result.stderr);
  return JSON.parse(result.stdout);
}

test('the supported 1024-batch admission remains readable with and without its reserved cursor', async () => {
  const directory = seed('maximum-supported', 1023);
  const sink = createNeatlogsOutboxSink({ directory, maxRetainedBatches: 1024 });
  assert.deepEqual(sink([record(1024)]), { accepted: true, acceptedCount: 1 });
  for (const cursor of [false, true]) {
    if (cursor) setCursor(directory, batchName(1024));
    const before = snapshot(directory);
    assert.equal(listNeatlogsOutbox(directory).length, 1024);
    assert.equal(Array.from(iterateNeatlogsOutboxSnapshot(directory), item => item.state).length, 1024);
    const result = await observe('drain', directory);
    assert.equal(result.error, null);
    assert.equal(result.returned, 32);
    assert.equal(result.posts, 0);
    assert.deepEqual(snapshot(directory), before, 'inspection cannot advance the cursor or acknowledge data');
  }
  assert.deepEqual(sink([record(1024)]), { accepted: true, acceptedCount: 1 });
  assert.deepEqual(sink([record(1025)]), { accepted: false, acceptedCount: 0 });
});

for (const operation of ['list', 'snapshot', 'drain']) {
  test(`${operation} refuses an oversized retained batch inventory before exposing or mutating a prefix`, async t => {
    const directory = seed(`over-batches-${operation}`, 1025);
    setCursor(directory, batchName(7));
    const before = snapshot(directory);
    const result = await observe(operation, directory, { readback: true });
    t.diagnostic(JSON.stringify(result));
    assertRefused(result);
    assert.deepEqual(snapshot(directory), before);
  });
}

test('1024 batches plus an unexpected name cannot consume the reserved future cursor slot', async t => {
  const directory = seed('reserved-cursor-slot', 1024);
  addIgnored(directory, 1);
  const before = snapshot(directory);
  const result = await observe('drain', directory, { readback: true });
  t.diagnostic(JSON.stringify(result));
  assertRefused(result);
  assert.equal(result.cursorExists, false);
  assert.deepEqual(snapshot(directory), before);
});

test('unexpected and temporary names count toward the bound before the first synthetic POST', async t => {
  const directory = seed('over-ignored-send', 1);
  addIgnored(directory, 1024);
  const before = snapshot(directory);
  const observations = [];
  for (const operation of ['list', 'snapshot', 'drain']) {
    observations.push(await observe(operation, directory, { send: true }));
  }
  t.diagnostic(JSON.stringify(observations));
  for (const result of observations) assertRefused(result);
  assert.equal(existsSync(join(directory, CURSOR_FILE)), false);
  assert.deepEqual(snapshot(directory), before);
});

test('an oversized directory of only nonbatch names refuses instead of certifying an empty outbox', () => {
  const directory = seed('only-ignored', 0);
  addIgnored(directory, 1025);
  const before = snapshot(directory);
  assert.throws(() => listNeatlogsOutbox(directory), { code: LIMIT_ERROR });
  assert.deepEqual(snapshot(directory), before);
});

test('under-bound filtering, legacy widths and tied numeric ranges preserve deterministic order and bytes', () => {
  const directory = seed('legacy-order', 3);
  renameSync(join(directory, batchName(2)), join(directory, batchName(2, 12)));
  writeFileSync(join(directory, batchName(1, 12)), readFileSync(join(directory, batchName(1))));
  addIgnored(directory, 2);
  mkdirSync(join(directory, 'retained-diagnostics'));
  const before = snapshot(directory);
  const sameRange = [batchName(1), batchName(1, 12)].sort();
  assert.deepEqual(listNeatlogsOutbox(directory).map(path => basename(path)), [...sameRange, batchName(2, 12), batchName(3)]);
  assert.deepEqual(Array.from(iterateNeatlogsOutboxSnapshot(directory), item => item.batch.firstSequence), [1, 1, 2, 3]);
  assert.deepEqual(snapshot(directory), before);
});

test('each direct consumer streams a finite inventory and closes before returning or yielding', t => {
  const directory = seed('stream-success', 1024);
  setCursor(directory, batchName(1024));
  for (const operation of ['list', 'snapshot', 'drain']) {
    const result = streamProbe(directory, operation);
    t.diagnostic(JSON.stringify({ operation, ...result }));
    assert.equal(result.error, null);
    assert.equal(result.arrays, 0);
    assert.equal(result.reads, 1026);
    assert.equal(result.closes, 1);
    assert.equal(result.bufferSize, 32);
    assert.equal(result.returned, operation === 'list' ? 1024 : operation === 'snapshot' ? 1 : 32);
    assert.equal(result.network, 0);
  }
});

test('overflow counts nonregular and ignored entries and closes without reading the unbounded tail', t => {
  const directory = seed('stream-overflow', 0);
  addIgnored(directory, 1100);
  mkdirSync(join(directory, 'retained-directory'));
  setCursor(directory, batchName(1));
  const before = snapshot(directory);
  for (const operation of ['list', 'snapshot', 'drain']) {
    const result = streamProbe(directory, operation);
    t.diagnostic(JSON.stringify({ operation, ...result }));
    assert.equal(result.error?.code, LIMIT_ERROR);
    assert.equal(result.arrays, 0);
    assert.ok(result.reads <= 1026);
    assert.equal(result.closes, 1);
    assert.equal(result.returned, 0);
    assert.equal(result.network, 0);
  }
  assert.deepEqual(snapshot(directory), before);
});

test('directory read failure preserves the original error, closes the handle and permits a later retry', async t => {
  const directory = seed('stream-read-error', 2);
  const before = snapshot(directory);
  for (const operation of ['list', 'snapshot', 'drain']) {
    const result = streamProbe(directory, operation, 2);
    t.diagnostic(JSON.stringify({ operation, ...result }));
    assert.equal(result.error?.code, 'EIO');
    assert.equal(result.originalFault, true);
    assert.equal(result.reads, 2);
    assert.equal(result.closes, 1);
    assert.equal(result.arrays, 0);
    assert.equal(result.returned, 0);
  }
  assert.deepEqual(snapshot(directory), before);
  const retried = await observe('drain', directory, { readback: true });
  assert.equal(retried.error, null);
  assert.equal(retried.returned, 2);
  assert.equal(retried.posts, 0);
  assert.equal(JSON.parse(readFileSync(join(directory, CURSOR_FILE))).lastBatch, batchName(2));
});

test('operator repair of an oversized inventory permits later progress without rewriting retained batches', async () => {
  const directory = seed('operator-repair', 1);
  addIgnored(directory, 1024);
  const retainedBatch = readFileSync(join(directory, batchName(1)));
  assert.throws(() => listNeatlogsOutbox(directory), { code: LIMIT_ERROR });
  // Explicit test/operator action outside the implementation; no automatic pruning.
  renameSync(join(directory, 'retained-0.tmp'), join(scratch, 'operator-preserved.tmp'));
  const before = snapshot(directory);
  const result = await observe('drain', directory, { readback: true });
  assert.equal(result.error, null);
  assert.equal(result.returned, 1);
  assert.equal(result.posts, 0);
  assert.equal(readFileSync(join(directory, batchName(1))).equals(retainedBatch), true);
  assert.deepEqual(snapshot(directory).filter(item => item.name !== CURSOR_FILE), before);
  assert.equal(listNeatlogsOutbox(directory).length, 1, 'the newly created reserved cursor still fits');
});

test('under-bound parser failure still records a visit and allows later batches to progress', async () => {
  const directory = seed('parser-fairness', 33);
  const bad = join(directory, batchName(1));
  writeFileSync(bad, '{"schema":"unsupported"}');
  const badBytes = readFileSync(bad);
  const failed = await observe('drain', directory, { readback: true });
  assert.equal(failed.error?.code, 'BATCH_INVALID');
  assert.equal(JSON.parse(readFileSync(join(directory, CURSOR_FILE))).lastBatch, batchName(1));
  const retry = await observe('drain', directory, { readback: true });
  assert.equal(retry.error, null);
  assert.equal(retry.returned, 32);
  assert.equal(JSON.parse(readFileSync(join(directory, CURSOR_FILE))).lastBatch, batchName(33));
  assert.equal(readFileSync(bad).equals(badBytes), true);
});
