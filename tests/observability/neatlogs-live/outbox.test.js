import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import net from 'node:net';
import tls from 'node:tls';
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, renameSync, rmSync, symlinkSync, unlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { after, before, describe, test } from 'node:test';
import { TELEMETRY_EVENT_TYPES } from '../../../src/internal/telemetry.js';
import { loadNeatlogsConfig } from '../../../src/observability/private/neatlogs-config.js';
import {
  createNeatlogsOutboxSink,
  drainNeatlogsOutbox,
  listNeatlogsOutbox,
  loadNeatlogsTelemetryOptIn,
  readbackPlanFromRecords,
  runtimeNeatlogsTags,
  iterateNeatlogsOutboxSnapshot
} from '../../../src/observability/private/neatlogs-outbox.js';
import { NEATLOGS_SDK_EVENT_TYPES } from '../../../src/observability/private/neatlogs-sdk-mapper.js';
import { isCompleteDeliveryEvidence, runRuntimeWorkflow } from '../../../tools/neatlogs-runtime-trace.mjs';

const INGEST_KEY = 'synthetic-ingest-key-0123456789abcdef';
const READ_TOKEN = 'synthetic-readback-token-0123456789abcdef';
const PROJECT_ID = '00000000-0000-4000-8000-000000000001';
const REVISION = 'b'.repeat(40);
const TAGS = runtimeNeatlogsTags({ revision: REVISION, harnessMode: 'mock' });

// Network guard: any real fetch or socket in this file fails the test.
let originalFetch;
let originalConnect;
let originalTlsConnect;
let blockedNetworkAttempts = 0;
let scratch;
let run;
let runOutbox;

function configured() {
  return loadNeatlogsConfig({
    env: { NEATLOGS_INGEST_KEY: INGEST_KEY, NEATLOGS_READ_TOKEN: READ_TOKEN, NEATLOGS_PROJECT_ID: PROJECT_ID }
  });
}

function response(status, body = '', headers = {}) {
  return new Response(body === null ? null : body, { status, headers });
}

function copyOutbox(name) {
  const target = join(scratch, name);
  for (const path of listNeatlogsOutbox(runOutbox)) {
    const sink = createNeatlogsOutboxSink({ directory: target });
    sink(JSON.parse(readFileSync(path, 'utf8')).records);
  }
  return target;
}

function recordsIn(directory) {
  return listNeatlogsOutbox(directory).flatMap(path => JSON.parse(readFileSync(path, 'utf8')).records);
}

// These fixtures contain at most 33 bounded batches. Production evidence
// reduces one yielded batch at a time instead of retaining all raw records.
function snapshotNeatlogsOutbox(directory) {
  return Array.from(iterateNeatlogsOutboxSnapshot(directory));
}

function sourceRecord(sequence) {
  return {
    schema: 'agent-harness.telemetry.v1', sequence,
    eventId: `tok1_${sequence.toString(16).padStart(64, '0')}`,
    workflowId: `tok1_${'f'.repeat(64)}`, revision: 0, eventType: 'workflow_created',
    fromState: null, toState: null, fence: 0, occurredAtMs: 1_760_000_000_000
  };
}

// Fake ingest + Public API. Readback serves exactly what a record set derives to.
function fakeNeatlogs({ ingestStatus = 200, tags = TAGS, droppedIds = [], generations = () => ({}) } = {}) {
  const calls = [];
  const fetchImpl = async (url, init) => {
    calls.push({ url: String(url), init });
    const parsed = new URL(url);
    if (parsed.pathname === '/v1/traces') return response(ingestStatus, null);
    const path = parsed.pathname.replace('/api/v1/public', '');
    const ok = data => response(200, JSON.stringify({ success: true, requestId: 'req_test', data }), {
      'content-type': 'application/json'
    });
    if (path === '/project') return ok({ id: PROJECT_ID, name: 'Test project' });
    // The fake service can retain more than one bounded sender batch. Derive
    // each record independently instead of sending its entire history through
    // the mapper's per-batch size limit.
    const plans = fakeNeatlogs.records.map(record => readbackPlanFromRecords([record], tags, generations()));
    const match = /^\/traces\/([0-9a-f]{32})(\/spans(?:\/([0-9a-f]{16})\/payload)?)?$/.exec(path);
    const trace = droppedIds.includes(match?.[1]) ? null : plans.flatMap(plan => plan.traces).find(item => item.traceId === match?.[1]);
    if (!trace) return response(404, '{}', { 'content-type': 'application/problem+json' });
    if (!match[2]) {
      return ok({
        traceId: trace.traceId, sessionId: trace.sessionId, workflowName: plans[0].workflowName,
        traceName: 'agent_harness.workflow_event', tags: [...tags], spansCount: 1, finalizationStatus: 'finalized'
      });
    }
    if (!match[3]) {
      return ok({
        spans: [{ traceId: trace.traceId, spanId: trace.rootSpanId, parentSpanId: null,
          spanName: 'agent_harness.workflow_event', spanType: 'WORKFLOW' }],
        page: { hasMore: false, limit: 50, nextCursor: null }
      });
    }
    return ok({
      traceId: trace.traceId, spanId: trace.rootSpanId,
      fields: { input: { available: true, truncated: false, mimeType: 'application/json', value: trace.input } }
    });
  };
  return { fetchImpl, calls };
}

before(async () => {
  originalFetch = globalThis.fetch;
  originalConnect = net.Socket.prototype.connect;
  originalTlsConnect = tls.connect;
  globalThis.fetch = async () => { blockedNetworkAttempts += 1; throw new Error('network blocked in test'); };
  net.Socket.prototype.connect = function blockedConnect(...args) {
    // Local IPC (worker runner child pipes) uses paths, not TCP hosts.
    const target = args[0];
    if (target && typeof target === 'object' && (target.host || target.port)) {
      blockedNetworkAttempts += 1;
      throw new Error('network blocked in test');
    }
    return originalConnect.apply(this, args);
  };
  tls.connect = () => { blockedNetworkAttempts += 1; throw new Error('network blocked in test'); };
  scratch = mkdtempSync(join(tmpdir(), 'neatlogs-outbox-test-'));
  runOutbox = join(scratch, 'run-outbox');
  const databaseDirectory = mkdtempSync(join(scratch, 'db-'));
  run = await runRuntimeWorkflow({ outboxDirectory: runOutbox, databaseDirectory });
});

after(() => {
  globalThis.fetch = originalFetch;
  net.Socket.prototype.connect = originalConnect;
  tls.connect = originalTlsConnect;
  rmSync(scratch, { recursive: true, force: true });
  assert.equal(blockedNetworkAttempts, 0);
});

describe('opt-in configuration', () => {
  test('is off by default and reads nothing', () => {
    const env = { NEATLOGS_INGEST_KEY: INGEST_KEY };
    const result = loadNeatlogsTelemetryOptIn({ env, scrubEnv: env });
    assert.deepEqual({ ...result }, { enabled: false });
    assert.equal(env.NEATLOGS_INGEST_KEY, INGEST_KEY);
  });

  test('when enabled uses the strict loader, scrubs the environment and requires an ingest key', () => {
    const env = { NEATLOGS_INGEST_KEY: INGEST_KEY, UNRELATED: 'x' };
    const result = loadNeatlogsTelemetryOptIn({ enabled: true, env: { ...env }, scrubEnv: env });
    assert.equal(result.enabled, true);
    assert.equal(result.config.summary.ingestReady, true);
    assert.equal(Object.hasOwn(env, 'NEATLOGS_INGEST_KEY'), false);
    assert.equal(env.UNRELATED, 'x');
    assert.ok(!JSON.stringify(result.config).includes(INGEST_KEY));
    assert.throws(() => loadNeatlogsTelemetryOptIn({ enabled: true, env: {} }), { code: 'INGEST_NOT_CONFIGURED' });
  });

  test('runtime tags name the dependency mode and never claim synthetic evidence', () => {
    assert.deepEqual([...TAGS], [`agent-harness@${REVISION}`, 'evidence:runtime', 'harness-mode:mock']);
    assert.throws(() => runtimeNeatlogsTags({ revision: 'abc', harnessMode: 'mock' }), { code: 'INVALID_TAG_INPUT' });
  });

  test('mapper accepts every event type the durable telemetry projection can emit', () => {
    assert.deepEqual([...NEATLOGS_SDK_EVENT_TYPES].sort(), [...TELEMETRY_EVENT_TYPES].sort());
  });
});

describe('real worker run through the durable exporter', () => {
  test('completes the workflow and flushes projected records into the outbox', () => {
    assert.equal(run.jobState, 'completed');
    assert.deepEqual({ ...run.results }, { sum: 42, label: 'agent-harness runtime trace' });
    assert.equal(run.telemetry.pendingRecords, 0);
    assert.equal(run.telemetry.lastErrorCode, null);
    const records = recordsIn(runOutbox);
    assert.equal(records.length, run.telemetry.deliveredRecords);
    assert.deepEqual(records.map(record => record.eventType), [
      'workflow_created', 'operations_work_admitted', 'operations_work_claimed', 'operations_work_completed'
    ]);
    assert.equal(new Set(records.map(record => record.workflowId)).size, 1);
  });

  test('outbox files hold only tokens and enums, never raw run identifiers', () => {
    for (const name of readdirSync(runOutbox)) {
      const text = readFileSync(join(runOutbox, name), 'utf8');
      for (const raw of run.forbidden) assert.ok(!text.includes(raw), 'raw identifier leaked into the outbox');
      assert.doesNotMatch(text, /ops-job:|ops-workflow:|runtime-tenant|runtime-project/u);
    }
  });
});

describe('outbox sink', () => {
  test('delivery evidence requires the complete outbox, not just its drained prefix', () => {
    const verified = count => Array.from({ length: count }, () => ({ readback: { status: 'verified' } }));
    assert.equal(isCompleteDeliveryEvidence({ network: true, batches: verified(32), batchCount: 33 }), false);
    assert.equal(isCompleteDeliveryEvidence({ network: true, batches: verified(33), batchCount: 33 }), true);
    assert.equal(isCompleteDeliveryEvidence({ network: true, batches: verified(1), batchCount: 1 }), true);
    assert.equal(isCompleteDeliveryEvidence({ network: false, batches: verified(1), batchCount: 1 }), false);
    assert.equal(isCompleteDeliveryEvidence({ network: true, batches: [], batchCount: 0 }), false);
    assert.equal(isCompleteDeliveryEvidence({ network: true, batches: [{ readback: { status: 'pending' } }], batchCount: 1 }), false);
  });

  test('re-delivery of a grown page stores only the new suffix', () => {
    const directory = join(scratch, 'sink-grow');
    const sink = createNeatlogsOutboxSink({ directory });
    const records = recordsIn(runOutbox);
    assert.deepEqual(sink(records.slice(0, 2)), { accepted: true, acceptedCount: 2 });
    assert.deepEqual(sink(records.slice(0, 3)), { accepted: true, acceptedCount: 3 });
    assert.deepEqual(sink(records.slice(0, 3)), { accepted: true, acceptedCount: 3 });
    assert.equal(recordsIn(directory).length, 3);
    assert.equal(listNeatlogsOutbox(directory).length, 2);
  });

  test('refuses a record that differs from the stored copy', () => {
    const directory = join(scratch, 'sink-conflict');
    const sink = createNeatlogsOutboxSink({ directory });
    const records = recordsIn(runOutbox);
    sink(records.slice(0, 1));
    assert.throws(() => sink([{ ...records[0], revision: records[0].revision + 1 }]), { code: 'BATCH_CONFLICT' });
  });

  test('refuses an aborted call and invalid records', () => {
    const sink = createNeatlogsOutboxSink({ directory: join(scratch, 'sink-invalid') });
    const controller = new AbortController();
    controller.abort();
    assert.deepEqual(sink(recordsIn(runOutbox), { signal: controller.signal }), { accepted: false, acceptedCount: 0 });
    assert.throws(() => sink([{ schema: 'agent-harness.telemetry.v1', sequence: 1, eventId: 'raw-id' }]));
  });

  test('preserves legacy names at every accepted width and the full safe-integer range', () => {
    const directory = join(scratch, 'sink-wide-sequence');
    const sink = createNeatlogsOutboxSink({ directory });
    sink([sourceRecord(1)]);
    renameSync(listNeatlogsOutbox(directory)[0], join(directory, 'batch-000000000001-000000000001.json'));
    const legacySequences = [1_000_000_000_000, 10_000_000_000_000, 100_000_000_000_000];
    for (const sequence of legacySequences) {
      sink([sourceRecord(sequence)]);
      const path = listNeatlogsOutbox(directory).at(-1);
      renameSync(path, join(directory, `batch-${sequence}-${sequence}.json`));
    }
    sink([sourceRecord(Number.MAX_SAFE_INTEGER)]);
    const paths = listNeatlogsOutbox(directory);
    assert.equal(paths.length, 2 + legacySequences.length);
    assert.deepEqual(paths.map(path => JSON.parse(readFileSync(path, 'utf8')).firstSequence), [
      1, ...legacySequences, Number.MAX_SAFE_INTEGER
    ]);
    assert.match(paths.at(-1), /batch-9007199254740991-9007199254740991\.json$/u);
  });

  test('rejects batch metadata that disagrees with its filename and records', () => {
    const directory = join(scratch, 'sink-metadata-mismatch');
    const sink = createNeatlogsOutboxSink({ directory });
    sink([sourceRecord(1)]);
    const path = listNeatlogsOutbox(directory)[0];
    const batch = JSON.parse(readFileSync(path, 'utf8'));
    batch.firstSequence = 2;
    writeFileSync(path, `${JSON.stringify(batch)}\n`, { mode: 0o600 });
    assert.throws(() => sink([sourceRecord(1)]), { code: 'BATCH_INVALID' });
  });

  test('rejects outbox states outside the persisted state machine', () => {
    const directory = join(scratch, 'sink-unknown-state');
    const sink = createNeatlogsOutboxSink({ directory });
    sink([sourceRecord(1)]);
    const path = listNeatlogsOutbox(directory)[0];
    const batch = JSON.parse(readFileSync(path, 'utf8'));
    batch.state = 'future_unreviewed_state';
    writeFileSync(path, `${JSON.stringify(batch)}\n`, { mode: 0o600 });
    assert.throws(() => sink([sourceRecord(1)]), { code: 'BATCH_INVALID' });
  });
});

describe('drain', () => {
  test('advances beyond 32 retained verified batches on a later drain', async () => {
    const directory = join(scratch, 'drain-terminal-prefix');
    const sink = createNeatlogsOutboxSink({ directory });
    for (let sequence = 1; sequence <= 33; sequence += 1) sink([sourceRecord(sequence)]);
    for (const path of listNeatlogsOutbox(directory).slice(0, 32)) {
      const batch = JSON.parse(readFileSync(path, 'utf8'));
      batch.state = 'verified';
      batch.readback = { status: 'verified' };
      writeFileSync(path, `${JSON.stringify(batch)}\n`, { mode: 0o600 });
    }
    fakeNeatlogs.records = recordsIn(directory);
    const fake = fakeNeatlogs();
    const first = await drainNeatlogsOutbox({
      directory, config: configured(), tags: TAGS, readback: true, fetch: fake.fetchImpl,
      readbackOptions: { pollIntervalMs: 0, maxPolls: 1 }
    });
    const second = await drainNeatlogsOutbox({
      directory, config: configured(), tags: TAGS, readback: true, fetch: fake.fetchImpl,
      readbackOptions: { pollIntervalMs: 0, maxPolls: 1 }
    });
    assert.equal(first.length, 32);
    assert.equal(second.length, 32);
    assert.equal(second[0].batch.firstSequence, 33);
    assert.equal(second[0].state, 'verified');
    assert.equal(fake.calls.filter(call => call.url.endsWith('/v1/traces')).length, 1);
    assert.equal(listNeatlogsOutbox(directory).length, 33, 'retained records must not be deleted for progress');
    const snapshot = snapshotNeatlogsOutbox(directory);
    assert.equal(isCompleteDeliveryEvidence({
      network: true, batches: second.map(result => result.batch), batchCount: 33
    }), false, 'the rotating page remains incomplete evidence');
    assert.equal(isCompleteDeliveryEvidence({
      network: true, batches: snapshot.map(result => result.batch), batchCount: 33
    }), true, 'the complete persisted snapshot includes earlier verified batches');
  });

  test('rotates past 32 unresolved sends without treating unknown outcomes as delivered', async () => {
    const directory = join(scratch, 'drain-unresolved-prefix');
    const sink = createNeatlogsOutboxSink({ directory });
    for (let sequence = 1; sequence <= 33; sequence += 1) sink([sourceRecord(sequence)]);
    const fake = fakeNeatlogs({ ingestStatus: 503 });
    const options = {
      directory, config: configured(), tags: TAGS, fetch: fake.fetchImpl,
      sendOptions: { maxAttempts: 1 }
    };
    const first = await drainNeatlogsOutbox(options);
    const second = await drainNeatlogsOutbox(options);
    assert.equal(first.length, 32);
    assert.equal(second.length, 32);
    assert.equal(second[0].batch.firstSequence, 33);
    assert.equal(fake.calls.length, 64, 'each drain retains the 32-batch work limit');
    assert.ok([...first, ...second].every(result => result.state === 'pending_send'
      && result.batch.send.status === 'outcome_unknown' && result.batch.readback === null));
    assert.equal(new Set([...first, ...second].map(result => result.batch.firstSequence)).size, 33);
    assert.equal(isCompleteDeliveryEvidence({
      network: true, batches: snapshotNeatlogsOutbox(directory).map(result => result.batch), batchCount: 33
    }), false);
  });

  test('a fresh process resumes the saved page and read-only inspection does not advance it', async () => {
    const directory = join(scratch, 'drain-process-restart');
    const sink = createNeatlogsOutboxSink({ directory });
    for (let sequence = 1; sequence <= 33; sequence += 1) sink([sourceRecord(sequence)]);
    const fake = fakeNeatlogs();
    await drainNeatlogsOutbox({
      directory, config: configured(), tags: TAGS, send: false, readback: true, recover: true, fetch: fake.fetchImpl
    });
    assert.equal(fake.calls.length, 0, 'readback-only mode must not send pending batches, even with recover');
    const cursorPath = join(directory, 'drain-cursor.json');
    const cursorBefore = readFileSync(cursorPath, 'utf8');
    const moduleUrl = new URL('../../../src/observability/private/neatlogs-outbox.js', import.meta.url).href;
    const child = spawnSync(process.execPath, ['--input-type=module', '--eval', `
      const { drainNeatlogsOutbox } = await import(${JSON.stringify(moduleUrl)});
      const rows = await drainNeatlogsOutbox({
        directory: ${JSON.stringify(directory)}, config: {}, tags: ${JSON.stringify(TAGS)},
        send: false, readback: false, fetch() { throw new Error('unexpected network'); }
      });
      process.stdout.write(JSON.stringify(rows.map(row => row.batch.firstSequence)));
    `], { encoding: 'utf8', timeout: 10_000, maxBuffer: 64 * 1024,
      env: { PATH: process.env.PATH, TMPDIR: tmpdir(), NODE_NO_WARNINGS: '1' } });
    assert.equal(child.error, undefined);
    assert.equal(child.signal, null);
    assert.equal(child.status, 0, child.stderr);
    assert.deepEqual(JSON.parse(child.stdout), [33, ...Array.from({ length: 31 }, (_, index) => index + 1)]);
    assert.equal(readFileSync(cursorPath, 'utf8'), cursorBefore);
    assert.ok(snapshotNeatlogsOutbox(directory).every(result => result.state === 'pending_send'
      && result.batch.send === null && result.batch.readback === null));
  });

  test('retains rotation when the previously visited batch is removed', async () => {
    const directory = join(scratch, 'drain-removed-cursor-row');
    const sink = createNeatlogsOutboxSink({ directory });
    for (let sequence = 1; sequence <= 33; sequence += 1) sink([sourceRecord(sequence)]);
    const options = { directory, config: configured(), tags: TAGS, send: false, readback: true,
      fetch: fakeNeatlogs().fetchImpl };
    const first = await drainNeatlogsOutbox(options);
    unlinkSync(first.at(-1).path);
    const second = await drainNeatlogsOutbox(options);
    assert.equal(second.length, 32);
    assert.equal(second[0].batch.firstSequence, 33);
    assert.equal(second.at(-1).batch.firstSequence, 31);
  });

  test('cursor ordering follows numeric legacy ranges and wraps after the final range', async () => {
    const directory = join(scratch, 'drain-legacy-cursor');
    const sink = createNeatlogsOutboxSink({ directory });
    const sequences = [1, 1_000_000_000_000, Number.MAX_SAFE_INTEGER];
    for (const sequence of sequences) sink([sourceRecord(sequence)]);
    const original = listNeatlogsOutbox(directory)[1];
    const legacyName = 'batch-1000000000000-1000000000000.json';
    renameSync(original, join(directory, legacyName));
    const cursorPath = join(directory, 'drain-cursor.json');
    writeFileSync(cursorPath, JSON.stringify({
      schema: 'agent-harness.neatlogs.outbox-drain-cursor.v1', lastBatch: legacyName
    }), { mode: 0o600 });
    const options = { directory, config: configured(), tags: TAGS, send: false, readback: true,
      fetch: fakeNeatlogs().fetchImpl };
    const first = await drainNeatlogsOutbox(options);
    assert.deepEqual(first.map(result => result.batch.firstSequence), [Number.MAX_SAFE_INTEGER, 1, sequences[1]]);
    const again = await drainNeatlogsOutbox(options);
    assert.deepEqual(again.map(result => result.batch.firstSequence), first.map(result => result.batch.firstSequence));
  });

  test('an invalid batch does not pin future drains and still blocks complete evidence', async () => {
    const directory = join(scratch, 'drain-batch-exception');
    const sink = createNeatlogsOutboxSink({ directory });
    for (let sequence = 1; sequence <= 33; sequence += 1) sink([sourceRecord(sequence)]);
    const firstPath = listNeatlogsOutbox(directory)[0];
    const original = readFileSync(firstPath, 'utf8');
    const invalid = JSON.parse(original);
    invalid.state = 'unrecognized';
    writeFileSync(firstPath, JSON.stringify(invalid), { mode: 0o600 });
    const fake = fakeNeatlogs();
    const options = { directory, config: configured(), tags: TAGS, send: false, readback: true,
      fetch: fake.fetchImpl };
    await assert.rejects(drainNeatlogsOutbox(options), { code: 'BATCH_INVALID' });
    const continued = await drainNeatlogsOutbox(options);
    assert.deepEqual(continued.map(result => result.batch.firstSequence),
      Array.from({ length: 32 }, (_, index) => index + 2));
    assert.equal(fake.calls.length, 0);
    assert.throws(() => snapshotNeatlogsOutbox(directory), { code: 'BATCH_INVALID' });
    writeFileSync(firstPath, original, { mode: 0o600 });
    const repaired = await drainNeatlogsOutbox(options);
    assert.equal(repaired[0].batch.firstSequence, 1, 'the retained failed row is revisited after rotation');
  });

  test('an invalid batch filename cannot replace a valid cursor or prevent progress after repair', async () => {
    const invalidNames = [
      'batch-0000000000000000-0000000000000002.json',
      'batch-0000000000000002-0000000000000001.json',
      'batch-9007199254740992-9007199254740992.json'
    ];
    for (const [index, invalidName] of invalidNames.entries()) {
      const directory = join(scratch, `drain-invalid-batch-name-${index}`);
      const sink = createNeatlogsOutboxSink({ directory });
      sink([sourceRecord(1)]);
      sink([sourceRecord(2)]);
      const originalPath = listNeatlogsOutbox(directory)[1];
      const invalidPath = join(directory, invalidName);
      renameSync(originalPath, invalidPath);
      const cursorPath = join(directory, 'drain-cursor.json');
      const cursorBefore = JSON.stringify({ schema: 'agent-harness.neatlogs.outbox-drain-cursor.v1',
        lastBatch: 'batch-0000000000000001-0000000000000001.json' });
      writeFileSync(cursorPath, cursorBefore, { mode: 0o600 });
      const fake = fakeNeatlogs();
      const options = { directory, config: configured(), tags: TAGS, send: false, readback: true,
        fetch: fake.fetchImpl };
      await assert.rejects(drainNeatlogsOutbox(options), { code: 'BATCH_INVALID' });
      assert.equal(readFileSync(cursorPath, 'utf8'), cursorBefore);
      assert.equal(fake.calls.length, 0);
      renameSync(invalidPath, originalPath);
      const repaired = await drainNeatlogsOutbox(options);
      assert.deepEqual(repaired.map(result => result.batch.firstSequence), [2, 1]);
    }
  });

  test('refuses malformed, unsupported, oversized and unsafe cursor data without delivery or reset', async () => {
    const directory = join(scratch, 'drain-invalid-cursor');
    createNeatlogsOutboxSink({ directory })([sourceRecord(1)]);
    const cursorPath = join(directory, 'drain-cursor.json');
    const schema = 'agent-harness.neatlogs.outbox-drain-cursor.v1';
    const lastBatch = 'batch-0000000000000001-0000000000000001.json';
    const invalid = [
      '', '{', 'null', '[]', JSON.stringify({ schema }),
      JSON.stringify({ schema: 'future', lastBatch }),
      JSON.stringify({ schema, lastBatch, extra: true }),
      JSON.stringify({ schema, lastBatch: `../${lastBatch}` }),
      JSON.stringify({ schema, lastBatch: 'batch-0000000000000000-0000000000000001.json' }),
      JSON.stringify({ schema, lastBatch: 'batch-0000000000000002-0000000000000001.json' }),
      JSON.stringify({ schema, lastBatch: 'batch-9007199254740992-9007199254740992.json' }),
      `{"schema":${JSON.stringify(schema)},"lastBatch":${JSON.stringify(lastBatch)},"lastBatch":${JSON.stringify(lastBatch)}}`,
      `${JSON.stringify({ schema, lastBatch })}${' '.repeat(257)}`
    ];
    const fake = fakeNeatlogs();
    for (const value of invalid) {
      writeFileSync(cursorPath, value, { mode: 0o600 });
      await assert.rejects(drainNeatlogsOutbox({
        directory, config: configured(), tags: TAGS, fetch: fake.fetchImpl
      }), { code: 'DRAIN_CURSOR_INVALID' });
      assert.equal(readFileSync(cursorPath, 'utf8'), value);
    }
    assert.equal(fake.calls.length, 0);
    assert.equal(snapshotNeatlogsOutbox(directory)[0].batch.send, null);
  });

  test('refuses cursor directories and symlinks instead of replacing them', async () => {
    const directory = join(scratch, 'drain-nonfile-cursor');
    createNeatlogsOutboxSink({ directory })([sourceRecord(1)]);
    const cursorPath = join(directory, 'drain-cursor.json');
    const fake = fakeNeatlogs();
    const options = { directory, config: configured(), tags: TAGS, fetch: fake.fetchImpl };
    mkdirSync(cursorPath);
    await assert.rejects(drainNeatlogsOutbox(options), { code: 'DRAIN_CURSOR_INVALID' });
    rmSync(cursorPath, { recursive: true });
    const target = join(scratch, 'cursor-symlink-target.json');
    const contents = JSON.stringify({ schema: 'agent-harness.neatlogs.outbox-drain-cursor.v1',
      lastBatch: 'batch-0000000000000001-0000000000000001.json' });
    writeFileSync(target, contents, { mode: 0o600 });
    symlinkSync(target, cursorPath);
    await assert.rejects(drainNeatlogsOutbox(options), { code: 'DRAIN_CURSOR_INVALID' });
    assert.equal(readFileSync(target, 'utf8'), contents);
    assert.equal(fake.calls.length, 0);
  });

  test('an empty outbox and a read-only first visit create no cursor or network work', async () => {
    const directory = join(scratch, 'drain-empty');
    const sink = createNeatlogsOutboxSink({ directory });
    const fake = fakeNeatlogs();
    const options = { directory, config: configured(), tags: TAGS, fetch: fake.fetchImpl };
    assert.deepEqual(await drainNeatlogsOutbox(options), []);
    assert.equal(existsSync(join(directory, 'drain-cursor.json')), false);
    sink([sourceRecord(1)]);
    const readOnly = await drainNeatlogsOutbox({ ...options, send: false, readback: false });
    assert.equal(readOnly.length, 1);
    assert.equal(readOnly[0].state, 'pending_send');
    assert.equal(existsSync(join(directory, 'drain-cursor.json')), false);
    assert.equal(fake.calls.length, 0);
  });

  test('the CLI separates a bounded drain page from complete persisted delivery evidence', async () => {
    const directory = join(scratch, 'drain-cli-snapshot');
    const sink = createNeatlogsOutboxSink({ directory });
    for (let sequence = 1; sequence <= 33; sequence += 1) sink([sourceRecord(sequence)]);
    // These are explicitly synthetic retained verification records. The child
    // must inspect them without making a real network request or asserting a
    // new provider observation.
    for (const path of listNeatlogsOutbox(directory)) {
      const batch = JSON.parse(readFileSync(path, 'utf8'));
      batch.state = 'verified';
      batch.readback = { status: 'verified' };
      writeFileSync(path, JSON.stringify(batch), { mode: 0o600 });
    }
    const evidencePath = join(scratch, 'cli-synthetic-snapshot.json');
    const toolUrl = new URL('../../../tools/neatlogs-runtime-trace.mjs', import.meta.url);
    const networkGuard = `
      import net from 'node:net';
      import tls from 'node:tls';
      globalThis.fetch = () => { throw new Error('unexpected network'); };
      net.Socket.prototype.connect = () => { throw new Error('unexpected socket'); };
      tls.connect = () => { throw new Error('unexpected TLS'); };
    `;
    const child = spawnSync(process.execPath, [
      '--import', `data:text/javascript,${encodeURIComponent(networkGuard)}`, fileURLToPath(toolUrl),
      '--mode', 'drain', '--outbox', directory, '--evidence', evidencePath, '--component-revision', REVISION
    ], { encoding: 'utf8', timeout: 10_000, maxBuffer: 64 * 1024,
      env: { PATH: process.env.PATH, TMPDIR: tmpdir(), NODE_NO_WARNINGS: '1',
        NEATLOGS_INGEST_KEY: INGEST_KEY, NEATLOGS_READ_TOKEN: READ_TOKEN, NEATLOGS_PROJECT_ID: PROJECT_ID } });
    assert.equal(child.error, undefined);
    assert.equal(child.signal, null);
    assert.equal(child.status, 0, child.stderr);
    const evidenceText = readFileSync(evidencePath, 'utf8');
    const evidence = JSON.parse(evidenceText);
    assert.equal(evidence.drainPage.length, 32);
    assert.equal(evidence.batches.length, 33);
    assert.equal(evidence.batchEvidenceSource, 'persisted_outbox_snapshot');
    assert.equal(evidence.deliveryConfirmed, true);
    assert.ok(evidence.batches.every(batch => batch.send === null));
    for (const secret of [INGEST_KEY, READ_TOKEN, PROJECT_ID]) assert.ok(!evidenceText.includes(secret));
  });

  test('sends through the OTLP sender, reads back, and records state without the project UUID', async () => {
    const directory = copyOutbox('drain-ok');
    fakeNeatlogs.records = recordsIn(directory);
    const { fetchImpl, calls } = fakeNeatlogs();
    const results = await drainNeatlogsOutbox({
      directory, config: configured(), tags: TAGS, forbidden: run.forbidden, readback: true,
      fetch: fetchImpl, readbackOptions: { pollIntervalMs: 0, maxPolls: 1 }
    });
    assert.deepEqual(results.map(result => result.state), results.map(() => 'verified'));
    const ingest = calls.filter(call => call.url === 'https://ingest.neatlogs.com/v1/traces');
    assert.equal(ingest.length, results.length);
    assert.equal(ingest[0].init.headers['x-api-key'], INGEST_KEY);
    const body = Buffer.concat(ingest.map(call => Buffer.from(call.init.body))).toString('latin1');
    assert.ok(body.includes('evidence:runtime'));
    assert.ok(!body.includes('evidence:synthetic'));
    for (const raw of run.forbidden) assert.ok(!body.includes(raw));
    const traces = results.flatMap(result => result.batch.plan.traces);
    assert.equal(traces.length, 4);
    for (const path of listNeatlogsOutbox(directory)) {
      const text = readFileSync(path, 'utf8');
      assert.ok(!text.includes(PROJECT_ID));
      assert.ok(!text.includes(INGEST_KEY) && !text.includes(READ_TOKEN));
    }
    // A second drain does not resend verified batches.
    const again = await drainNeatlogsOutbox({ directory, config: configured(), tags: TAGS, readback: true, fetch: fetchImpl });
    assert.equal(calls.filter(call => call.url.endsWith('/v1/traces')).length, ingest.length);
    assert.ok(again.every(result => result.state === 'verified'));
  });

  test('an ambiguous send stays pending and resends the same trace IDs', async () => {
    const directory = copyOutbox('drain-retry');
    fakeNeatlogs.records = recordsIn(directory);
    const failing = fakeNeatlogs({ ingestStatus: 503 });
    const first = await drainNeatlogsOutbox({
      directory, config: configured(), tags: TAGS, fetch: failing.fetchImpl,
      sendOptions: { maxAttempts: 1 }
    });
    assert.ok(first.every(result => result.state === 'pending_send'));
    assert.ok(first.every(result => result.batch.send.status === 'outcome_unknown'));
    const firstIds = first.flatMap(result => result.batch.plan.traces.map(trace => trace.traceId));
    const working = fakeNeatlogs();
    const second = await drainNeatlogsOutbox({
      directory, config: configured(), tags: TAGS, fetch: working.fetchImpl, readback: true,
      readbackOptions: { pollIntervalMs: 0, maxPolls: 1 }
    });
    assert.deepEqual(second.flatMap(result => result.batch.plan.traces.map(trace => trace.traceId)), firstIds);
    assert.ok(second.every(result => result.state === 'verified'));
  });

  test('opt-in recovery re-sends only a bounded-404 trace under the next generation', async () => {
    const directory = copyOutbox('drain-recover');
    fakeNeatlogs.records = recordsIn(directory);
    const lost = readbackPlanFromRecords(fakeNeatlogs.records, TAGS).traces.at(-1).traceId;
    let served = {};
    const fake = fakeNeatlogs({ droppedIds: [lost], generations: () => served });
    const first = await drainNeatlogsOutbox({
      directory, config: configured(), tags: TAGS, fetch: fake.fetchImpl, readback: true,
      readbackOptions: { pollIntervalMs: 0, maxPolls: 2 }
    });
    assert.equal(first.at(-1).state, 'missing');
    const sendsBefore = fake.calls.filter(call => call.url.endsWith('/v1/traces')).length;
    // Without --recover nothing is re-sent.
    await drainNeatlogsOutbox({ directory, config: configured(), tags: TAGS, fetch: fake.fetchImpl });
    assert.equal(fake.calls.filter(call => call.url.endsWith('/v1/traces')).length, sendsBefore);
    served = { [fakeNeatlogs.records.at(-1).sequence]: 1 };
    const second = await drainNeatlogsOutbox({
      directory, config: configured(), tags: TAGS, fetch: fake.fetchImpl, readback: true, recover: true,
      readbackOptions: { pollIntervalMs: 0, maxPolls: 1 }
    });
    const batch = second.at(-1).batch;
    assert.equal(second.at(-1).state, 'verified');
    assert.deepEqual(batch.recovery.regeneratedFrom, [lost]);
    assert.equal(batch.plan.traces.at(-1).generation, 1);
    assert.notEqual(batch.plan.traces.at(-1).traceId, lost);
    assert.equal(fake.calls.filter(call => call.url.endsWith('/v1/traces')).length, sendsBefore + 1);
    assert.ok(second.slice(0, -1).every(result => result.state === 'verified'));
  });

  test('the leak gate blocks a batch before any request', async () => {
    const directory = copyOutbox('drain-leak');
    const records = recordsIn(directory);
    const { fetchImpl, calls } = fakeNeatlogs();
    const results = await drainNeatlogsOutbox({
      directory, config: configured(), tags: TAGS, forbidden: [records[0].workflowId], fetch: fetchImpl
    });
    assert.ok(results.every(result => result.state === 'blocked'));
    assert.equal(calls.length, 0);
  });

  test('refuses the synthetic evidence tag', async () => {
    await assert.rejects(drainNeatlogsOutbox({
      directory: copyOutbox('drain-synthetic'), config: configured(),
      tags: [`agent-harness@${REVISION}`, 'evidence:synthetic'], fetch: fakeNeatlogs().fetchImpl
    }), { code: 'SYNTHETIC_TAG_REFUSED' });
  });
});
