import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { after, before, describe, test } from 'node:test';
import { loadNeatlogsConfig } from '../../../src/observability/private/neatlogs-config.js';
import {
  deterministicNeatlogsIds,
  encodeNeatlogsExportRequest,
  NEATLOGS_OTLP_LIMITS
} from '../../../src/observability/private/neatlogs-otlp.js';
import {
  NEATLOGS_DELIVERY_QUEUE_LIMITS as NEATLOGS_OUTBOX_LIMITS,
  NeatlogsDeliveryQueue as NeatlogsOutbox,
  NeatlogsDeliveryQueueError as NeatlogsOutboxError,
  drainNeatlogsDeliveryQueue as drainNeatlogsOutbox,
  validateNeatlogsQueueEntry as validateNeatlogsOutboxEntry
} from '../../../src/observability/private/neatlogs-delivery-queue.js';
import { FAKE_INGEST_KEY, FAKE_PROJECT_ID, FAKE_READ_TOKEN, fakeNeatlogs } from './fake-neatlogs.js';

const CANARY = 'SYNTHETIC_QUEUE_CANARY_person@example.invalid';
const token = n => `tok1_${n.toString(16).padStart(64, '0')}`;

let scratch;
let counter = 0;
before(() => { scratch = mkdtempSync(join(tmpdir(), 'neatlogs-delivery-queue-')); });
after(() => rmSync(scratch, { recursive: true, force: true }));

function dbPath() {
  counter += 1;
  return join(scratch, `outbox-${counter}.sqlite`);
}

function config({ readback = true } = {}) {
  return loadNeatlogsConfig({
    env: {
      NEATLOGS_INGEST_KEY: FAKE_INGEST_KEY,
      ...(readback ? { NEATLOGS_READ_TOKEN: FAKE_READ_TOKEN, NEATLOGS_PROJECT_ID: FAKE_PROJECT_ID } : {})
    }
  });
}

function entry(n, overrides = {}) {
  return {
    eventToken: token(n),
    workflowName: 'agent-harness.ticketstub-events',
    sessionId: token(1000 + n),
    eventType: 'art_ready',
    input: JSON.stringify({ n }),
    attributes: [['agent_harness.source', 'ticketstub'], ['ticketstub.event_type', 'art_ready']],
    occurredAtMs: 1_000 + n,
    ...overrides
  };
}

function clockAt(start) {
  const clock = { now: start };
  clock.fn = () => clock.now;
  return clock;
}

function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}

const plainRow = row => row === undefined ? undefined : { ...row };
const plainRows = rows => rows.map(plainRow);

const noSleep = async () => true;
const HELD_REQUEST_TIMEOUT_MS = 5_000;

async function awaitHeldRequest(entered, pending, label, {
  timeoutMs = HELD_REQUEST_TIMEOUT_MS,
  onTimerCleared
} = {}) {
  let timeout;
  const prematureSettlement = pending.then(
    () => { throw new Error(`${label}: drain completed before the held request entered`); },
    error => { throw new Error(`${label}: drain failed before the held request entered`, { cause: error }); }
  );
  const deadline = new Promise((_, reject) => {
    timeout = setTimeout(() => reject(new Error(
      `${label}: held request did not enter within ${timeoutMs}ms`
    )), timeoutMs);
    timeout.ref?.();
  });
  try {
    await Promise.race([entered.promise, prematureSettlement, deadline]);
  } finally {
    if (timeout) {
      clearTimeout(timeout);
      onTimerCleared?.();
    }
  }
}

function throwTestAndCleanupError(primaryError, hasPrimaryError, cleanupErrors, label) {
  if (hasPrimaryError && cleanupErrors.length > 0) {
    throw new AggregateError([primaryError, ...cleanupErrors], label, { cause: primaryError });
  }
  if (hasPrimaryError) throw primaryError;
  if (cleanupErrors.length === 1) throw cleanupErrors[0];
  if (cleanupErrors.length > 1) throw new AggregateError(cleanupErrors, label);
}

async function finishHeldQueueFixture({
  release,
  pending,
  pendingObserved,
  markPendingObserved,
  queues,
  primaryError,
  hasPrimaryError,
  label,
  onLifecycle
}) {
  const cleanupErrors = [];
  release.resolve();
  onLifecycle?.('released');
  if (pending && !pendingObserved) {
    try {
      await pending;
    } catch (error) {
      cleanupErrors.push(error);
    } finally {
      markPendingObserved?.();
      onLifecycle?.('drain-settled');
    }
  }
  for (const queue of queues) {
    if (!queue) continue;
    try {
      queue.close();
    } catch (error) {
      cleanupErrors.push(error);
    } finally {
      onLifecycle?.('queue-close-attempted');
    }
  }
  throwTestAndCleanupError(primaryError, hasPrimaryError, cleanupErrors, label);
}

describe('entries', () => {
  test('refuses reserved, malformed or oversized content', () => {
    for (const bad of [
      entry(1, { eventToken: 'raw-id' }),
      entry(1, { sessionId: CANARY }),
      entry(1, { workflowName: 'other' }),
      entry(1, { eventType: 'Bad Type' }),
      entry(1, { input: 'not json' }),
      entry(1, { input: JSON.stringify({ x: 'y'.repeat(NEATLOGS_OUTBOX_LIMITS.maxInputBytes) }) }),
      entry(1, { attributes: [['neatlogs.session.id', 'x']] }),
      entry(1, { attributes: [['agent_harness.source', 'a'], ['agent_harness.source', 'b']] }),
      entry(1, { attributes: [['agent_harness.value', -1]] }),
      entry(1, { attributes: [['agent_harness.value', 'x'.repeat(300)]] }),
      entry(1, { attributes: [[`${'a'.repeat(260_000)}.b`, 'x']] }),
      entry(1, { occurredAtMs: 0 })
    ]) {
      assert.throws(() => validateNeatlogsOutboxEntry(bad), NeatlogsOutboxError);
    }
    assert.equal(validateNeatlogsOutboxEntry(entry(1)).eventToken, token(1));
  });

  test('enforces actual encoded request width and keeps generation metadata in persisted plans', () => {
    const tags = Array.from({ length: NEATLOGS_OTLP_LIMITS.tags }, (_, index) =>
      `${String(index).padStart(2, '0')}${'x'.repeat(126)}`);
    const makePlan = (candidate, currentGeneration) => {
      const ids = deterministicNeatlogsIds(candidate.eventToken, currentGeneration);
      const start = 1_000_000_000_000 * 1e6;
      return {
        workflowName: candidate.workflowName,
        tags,
        traces: [{
          ...ids,
          generation: currentGeneration,
          priorTraceIds: Array.from({ length: currentGeneration }, (_, priorGeneration) =>
            deterministicNeatlogsIds(candidate.eventToken, priorGeneration).traceId),
          sessionId: candidate.sessionId,
          eventToken: candidate.eventToken,
          sequence: Number.MAX_SAFE_INTEGER,
          revision: 0,
          eventType: candidate.eventType,
          fence: 0,
          input: candidate.input,
          attributes: candidate.attributes,
          root: { start, end: start + 1e6 },
          marker: { start: start + 1e6, end: start + 1e6 }
        }]
      };
    };
    const withKeyLength = length => entry(31, {
      attributes: [[`${'a'.repeat(length - 2)}.b`, 'x']]
    });
    let low = 2;
    let high = 260_000;
    let largestWireFit = null;
    while (low <= high) {
      const middle = Math.floor((low + high) / 2);
      const candidate = withKeyLength(middle);
      let fits = false;
      try {
        fits = encodeNeatlogsExportRequest(makePlan(candidate, 0)).length <= NEATLOGS_OTLP_LIMITS.requestBytes;
      } catch (error) {
        if (error?.code !== 'REQUEST_TOO_LARGE') throw error;
      }
      if (fits) {
        largestWireFit = candidate;
        low = middle + 1;
      } else {
        high = middle - 1;
      }
    }
    assert.ok(largestWireFit, 'the request-width search finds an encodable entry');
    const fitBody = encodeNeatlogsExportRequest(makePlan(largestWireFit, 0));
    const generationThreeBody = encodeNeatlogsExportRequest(makePlan(largestWireFit, NEATLOGS_OUTBOX_LIMITS.maxGeneration));
    assert.ok(fitBody.length <= NEATLOGS_OTLP_LIMITS.requestBytes);
    assert.equal(generationThreeBody.length, fitBody.length,
      'generation and prior IDs are persisted metadata, not encoder fields that widen this request');
    assert.equal(makePlan(largestWireFit, NEATLOGS_OUTBOX_LIMITS.maxGeneration).traces[0].priorTraceIds.length, 3);
    assert.equal(validateNeatlogsOutboxEntry(largestWireFit).eventToken, largestWireFit.eventToken);

    const firstOver = withKeyLength(Math.floor((low + high) / 2) + 1);
    assert.throws(() => encodeNeatlogsExportRequest(makePlan(firstOver, 0)), { code: 'REQUEST_TOO_LARGE' });
    assert.throws(() => validateNeatlogsOutboxEntry(firstOver), { code: 'ENTRY_TOO_LARGE' });
  });

  test('generation-three readback metadata survives normal regeneration without widening the OTLP body', async () => {
    const clock = clockAt(10_000);
    const path = dbPath();
    const outbox = NeatlogsOutbox.open({
      databasePath: path, clock: clock.fn, readbackDelayMs: 0, missingAfterMs: 0,
      retryBaseMs: 0, retryMaxMs: 0, maxBatchesPerDrain: 1
    });
    const traceIds = Array.from({ length: NEATLOGS_OUTBOX_LIMITS.maxGeneration + 1 }, (_, generation) =>
      deterministicNeatlogsIds(token(32), generation).traceId);
    const neatlogs = fakeNeatlogs({ loseFirstDelivery: traceIds });
    try {
      outbox.enqueue([entry(32)]);
      await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      for (let generation = 1; generation <= NEATLOGS_OUTBOX_LIMITS.maxGeneration; generation += 1) {
        clock.now += 1;
        const result = await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
        assert.equal(result.regenerated, 1);
        assert.equal(result.sent, 1);
      }
      const raw = new DatabaseSync(path, { readOnly: true });
      const batch = raw.prepare('SELECT plan_json, body FROM neatlogs_delivery_batches ORDER BY created_at_ms DESC LIMIT 1').get();
      const plan = JSON.parse(batch.plan_json);
      assert.equal(plan.traces[0].generation, 3);
      assert.deepEqual(plan.traces[0].priorTraceIds, traceIds.slice(0, 3));
      assert.ok(Buffer.from(batch.body).length <= NEATLOGS_OTLP_LIMITS.requestBytes);
      assert.equal(Buffer.from(batch.body).length, neatlogs.bodies[0].length,
        'generation metadata remains durable while the encoded request width stays fixed');
      raw.close();
    } finally {
      outbox.close();
    }
  });

  test('requires a file-backed database', () => {
    assert.throws(() => NeatlogsOutbox.open({ databasePath: ':memory:' }), { code: 'DURABILITY_REQUIRES_FILE' });
  });

  test('enqueue is idempotent by event token and bounded', () => {
    const outbox = NeatlogsOutbox.open({ databasePath: dbPath(), maxUnsettled: 2 });
    try {
      assert.deepEqual({ ...outbox.enqueue([entry(1), entry(1)]) }, { accepted: 1, duplicates: 1, refused: 0, conflicts: 0 });
      assert.deepEqual({ ...outbox.enqueue([entry(1), entry(2), entry(3)]) }, { accepted: 1, duplicates: 1, refused: 1, conflicts: 0 });
      assert.deepEqual({ ...outbox.enqueue([entry(1, { input: JSON.stringify({ different: true }) })]) },
        { accepted: 0, duplicates: 0, refused: 0, conflicts: 1 }, 'content conflicts are distinct from duplicates and checked before capacity');
      assert.deepEqual({ ...outbox.enqueue([entry(2)]) }, { accepted: 0, duplicates: 1, refused: 0, conflicts: 0 },
        'matching existing content is duplicate even when the unsettled cap is full');
      assert.equal(outbox.diagnostics().counts.queued, 2);
    } finally {
      outbox.close();
    }
  });

  test('refuses a database written by a newer schema', () => {
    const path = dbPath();
    NeatlogsOutbox.open({ databasePath: path }).close();
    const raw = new DatabaseSync(path);
    raw.prepare("UPDATE neatlogs_delivery_queue_meta SET value = '99' WHERE key = 'schema_version'").run();
    raw.close();
    assert.throws(() => NeatlogsOutbox.open({ databasePath: path }), { code: 'UNSUPPORTED_SCHEMA_VERSION' });
  });

  test('version-one queued and sent bodies migrate to an immutable legacy hold', () => {
    const path = dbPath();
    const fixtures = [
      { value: entry(22), state: 'queued', attempts: 0, sentAt: null, status: null, reason: null, settledAt: null },
      { value: entry(23), state: 'sent', attempts: 1, sentAt: 2_023, status: 'outcome_unknown', reason: 'HTTP_503', settledAt: null },
      { value: entry(24), state: 'verified', attempts: 1, sentAt: 2_024, status: 'readback_verified', reason: null, settledAt: 2_025 },
      { value: entry(25), state: 'failed', attempts: 3, sentAt: 2_025, status: 'readback_mismatch', reason: 'INPUT_PAYLOAD_MISMATCH', settledAt: 2_026 }
    ];
    const raw = new DatabaseSync(path);
    raw.exec(`CREATE TABLE neatlogs_delivery_queue_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
      CREATE TABLE neatlogs_delivery_queue (
        position INTEGER PRIMARY KEY AUTOINCREMENT, event_token TEXT NOT NULL UNIQUE, workflow_name TEXT NOT NULL,
        session_id TEXT NOT NULL, event_type TEXT NOT NULL, input TEXT NOT NULL, attributes TEXT NOT NULL,
        occurred_at_ms INTEGER NOT NULL, enqueued_at_ms INTEGER NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('queued', 'sent', 'verified', 'failed')),
        generation INTEGER NOT NULL DEFAULT 0, prior_trace_ids TEXT NOT NULL DEFAULT '[]',
        duplicate_trace_ids TEXT NOT NULL DEFAULT '[]', trace_id TEXT NOT NULL, send_attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at_ms INTEGER NOT NULL, sent_at_ms INTEGER, last_status TEXT, last_reason TEXT, settled_at_ms INTEGER
      ) STRICT;
      INSERT INTO neatlogs_delivery_queue_meta VALUES ('schema_version', '1');`);
    const insert = raw.prepare(`INSERT INTO neatlogs_delivery_queue
      (event_token,workflow_name,session_id,event_type,input,attributes,occurred_at_ms,enqueued_at_ms,state,
       generation,trace_id,send_attempts,next_attempt_at_ms,sent_at_ms,last_status,last_reason,settled_at_ms)
      VALUES (?,?,?,?,?,?,?,?,?,0,?,?,?,?,?,?,?)`);
    for (const fixture of fixtures) {
      const prior = fixture.value;
      const ids = deterministicNeatlogsIds(prior.eventToken, 0);
      insert.run(prior.eventToken, prior.workflowName, prior.sessionId, prior.eventType, prior.input,
        JSON.stringify(prior.attributes), prior.occurredAtMs, prior.occurredAtMs, fixture.state,
        ids.traceId, fixture.attempts, prior.occurredAtMs, fixture.sentAt,
        fixture.status, fixture.reason, fixture.settledAt);
    }
    const oldColumns = ['position', 'event_token', 'workflow_name', 'session_id', 'event_type', 'input', 'attributes',
      'occurred_at_ms', 'enqueued_at_ms', 'state', 'generation', 'prior_trace_ids', 'duplicate_trace_ids', 'trace_id',
      'send_attempts', 'next_attempt_at_ms', 'sent_at_ms', 'last_status', 'last_reason', 'settled_at_ms'];
    const selectOld = `SELECT ${oldColumns.join(',')} FROM neatlogs_delivery_queue ORDER BY position`;
    const originalRows = plainRows(raw.prepare(selectOld).all());
    raw.close();
    const clock = clockAt(10_000);
    const outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn });
    try {
      const immediatelyMigrated = new DatabaseSync(path, { readOnly: true });
      try {
        assert.deepEqual(plainRows(immediatelyMigrated.prepare(selectOld).all()), originalRows,
          'migration preserves every legacy column before duplicate checks or normal pruning can run');
      } finally {
        immediatelyMigrated.close();
      }
      assert.equal(outbox.hasRunnableWork(), false);
      assert.equal(outbox.diagnostics().counts.legacyHeld, 2,
        'only pre-migration queued/sent rows count as active legacy holds');
      for (const fixture of fixtures) {
        assert.deepEqual({ ...outbox.enqueue([fixture.value]) },
          { accepted: 0, duplicates: 1, refused: 0, conflicts: 0 });
      }
      assert.deepEqual({ ...outbox.enqueue([{
        ...fixtures[0].value,
        input: JSON.stringify({ schema: 'agent-harness.ticketstub-intake.v2', type: 'art_ready' })
      }]) }, { accepted: 0, duplicates: 0, refused: 0, conflicts: 1 },
      'a new v2 body for a retained v1 token is an immutable-content conflict');
      const after = new DatabaseSync(path, { readOnly: true });
      assert.deepEqual(plainRows(after.prepare(selectOld).all()), originalRows,
        'migration retains every v1 identity, payload and historical status field byte-for-byte');
      const migrated = after.prepare('SELECT state, legacy_hold FROM neatlogs_delivery_queue ORDER BY position').all();
      assert.deepEqual(migrated.map(row => [row.state, row.legacy_hold]), [
        ['queued', 1], ['sent', 1], ['verified', 1], ['failed', 1]
      ]);
      after.close();
    } finally {
      outbox.close();
    }
  });

  test('version-one migration rolls back partial ALTERs and rejects partial schemas', () => {
    const path = dbPath();
    const raw = new DatabaseSync(path);
    raw.exec(`CREATE TABLE neatlogs_delivery_queue_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
      CREATE TABLE neatlogs_delivery_queue (
        position INTEGER PRIMARY KEY AUTOINCREMENT, event_token TEXT NOT NULL UNIQUE, workflow_name TEXT NOT NULL,
        session_id TEXT NOT NULL, event_type TEXT NOT NULL, input TEXT NOT NULL, attributes TEXT NOT NULL,
        occurred_at_ms INTEGER NOT NULL, enqueued_at_ms INTEGER NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('queued', 'sent', 'verified', 'failed')),
        generation INTEGER NOT NULL DEFAULT 0, prior_trace_ids TEXT NOT NULL DEFAULT '[]',
        duplicate_trace_ids TEXT NOT NULL DEFAULT '[]', trace_id TEXT NOT NULL, send_attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at_ms INTEGER NOT NULL, sent_at_ms INTEGER, last_status TEXT, last_reason TEXT, settled_at_ms INTEGER,
        next_readback_at_ms INTEGER
      ) STRICT;
      INSERT INTO neatlogs_delivery_queue_meta VALUES ('schema_version', '1');`);
    const prior = entry(26);
    const ids = deterministicNeatlogsIds(prior.eventToken, 0);
    raw.prepare(`INSERT INTO neatlogs_delivery_queue
      (event_token,workflow_name,session_id,event_type,input,attributes,occurred_at_ms,enqueued_at_ms,state,
       generation,trace_id,next_attempt_at_ms)
      VALUES (?,?,?,?,?,?,?,?, 'queued',0,?,?)`).run(prior.eventToken, prior.workflowName, prior.sessionId,
      prior.eventType, prior.input, JSON.stringify(prior.attributes), prior.occurredAtMs, prior.occurredAtMs,
      ids.traceId, prior.occurredAtMs);
    const columnsBefore = raw.prepare('PRAGMA table_info(neatlogs_delivery_queue)').all().map(row => row.name);
    const rowBefore = plainRow(raw.prepare('SELECT * FROM neatlogs_delivery_queue').get());
    raw.close();

    assert.throws(() => NeatlogsOutbox.open({ databasePath: path }), { code: 'STORE_OPEN_FAILED' },
      'the duplicate next_readback_at_ms simulates a partially upgraded schema and fails the second ALTER');
    const after = new DatabaseSync(path, { readOnly: true });
    assert.deepEqual(after.prepare('PRAGMA table_info(neatlogs_delivery_queue)').all().map(row => row.name), columnsBefore,
      'the first new column is rolled back with the failed transaction');
    assert.deepEqual(plainRow(after.prepare('SELECT * FROM neatlogs_delivery_queue').get()), rowBefore);
    assert.equal(after.prepare("SELECT value FROM neatlogs_delivery_queue_meta WHERE key='schema_version'").get().value, '1');
    assert.equal(after.prepare("SELECT COUNT(*) AS n FROM sqlite_master WHERE type='table' AND name IN ('neatlogs_delivery_batches','neatlogs_delivery_attempts')").get().n, 0,
      'new companion tables do not survive a failed upgrade');
    after.close();
  });
});

describe('drain', () => {
  test('sends, records acceptance as sent, then verifies by readback', async () => {
    const clock = clockAt(10_000);
    const outbox = NeatlogsOutbox.open({ databasePath: dbPath(), clock: clock.fn });
    const neatlogs = fakeNeatlogs();
    try {
      outbox.enqueue([entry(1), entry(2)]);
      const first = await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep, tags: ['evidence:synthetic'] });
      assert.equal(first.sent, 2);
      assert.equal(first.sendRequests, 1);
      assert.equal(first.verified, 0, 'readback waits for the readback delay');
      assert.equal(outbox.diagnostics().counts.sent, 2);

      clock.now += NEATLOGS_OUTBOX_LIMITS.defaults.readbackDelayMs;
      const second = await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep, tags: ['evidence:synthetic'] });
      assert.equal(second.verified, 2);
      assert.equal(second.readbackStatus, 'verified');
      const { counts, latest } = outbox.diagnostics();
      assert.equal(counts.verified, 2);
      assert.equal(latest[0].traceId, deterministicNeatlogsIds(token(2)).traceId);

      const stored = neatlogs.stored.get(deterministicNeatlogsIds(token(1)).traceId);
      assert.equal(stored.workflowName, 'agent-harness.ticketstub-events');
      assert.equal(stored.input, JSON.stringify({ n: 1 }));
      assert.equal(stored.attributes['ticketstub.event_type'], 'art_ready');
      assert.deepEqual(stored.tags, ['evidence:synthetic']);
      const ingest = neatlogs.calls.find(call => call.path === '/v1/traces');
      assert.equal(ingest.headers['x-api-key'], FAKE_INGEST_KEY);
    } finally {
      outbox.close();
    }
  });

  test('persists exact plan bytes and server Retry-After across restart and tag changes', async () => {
    const clock = clockAt(10_000);
    const path = dbPath();
    let outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn, retryBaseMs: 1, retryMaxMs: 100,
      maxBatchesPerDrain: 1 });
    const neatlogs = fakeNeatlogs({ ingestStatus: count => count === 1 ? { status: 429, retryAfter: '3600' } : 200 });
    const firstTags = ['evidence:synthetic', 'host:fixed'];
    try {
      outbox.enqueue([entry(28)]);
      const first = await drainNeatlogsOutbox(outbox, config({ readback: false }), {
        fetch: neatlogs.fetchImpl, sleep: noSleep, tags: firstTags
      });
      assert.equal(first.sendRequests, 1);
      assert.equal(first.sendBatches, 1, 'the bounded restart fixture permits exactly one batch in the first pass');
      assert.equal(first.sendStatus, 'not_accepted');
      const stored = outbox.diagnostics().latest[0];
      assert.equal(stored.state, 'queued');
      assert.equal(stored.postAttempts, 1);
      assert.equal(neatlogs.bodies.length, 1);
      const beforeClose = new DatabaseSync(path, { readOnly: true });
      const storedBatch = beforeClose.prepare('SELECT body FROM neatlogs_delivery_batches').get().body;
      const storedAttempt = plainRow(beforeClose.prepare('SELECT ordinal, status, retry_after_minimum_ms FROM neatlogs_delivery_attempts').get());
      assert.deepEqual(Buffer.from(storedBatch), neatlogs.bodies[0], 'the encoded body is durable before close');
      assert.deepEqual(storedAttempt, { ordinal: 1, status: 'not_accepted', retry_after_minimum_ms: 3_600_000 });
      beforeClose.close();
      outbox.close();
      outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn, retryBaseMs: 1, retryMaxMs: 100,
        maxBatchesPerDrain: 1 });
      const early = await drainNeatlogsOutbox(outbox, config({ readback: false }), {
        fetch: neatlogs.fetchImpl, sleep: noSleep, tags: ['host:changed']
      });
      assert.equal(early.sendRequests, 0, 'persisted one-hour server minimum prevents an early retry');
      clock.now += 3_600_000 - 1;
      assert.equal((await drainNeatlogsOutbox(outbox, config({ readback: false }), {
        fetch: neatlogs.fetchImpl, sleep: noSleep, tags: ['host:changed']
      })).sendRequests, 0);
      clock.now += 1;
      const due = await drainNeatlogsOutbox(outbox, config({ readback: false }), {
        fetch: neatlogs.fetchImpl, sleep: noSleep, tags: ['host:changed']
      });
      assert.equal(due.sendRequests, 1);
      assert.equal(neatlogs.bodies.length, 2);
      assert.deepEqual(neatlogs.bodies[1], neatlogs.bodies[0], 'retries reuse exact persisted OTLP bytes and tags');
      const raw = new DatabaseSync(path, { readOnly: true });
      const attempts = raw.prepare(`SELECT attempts.ordinal, attempts.status, batches.body
        FROM neatlogs_delivery_attempts AS attempts
        JOIN neatlogs_delivery_batches AS batches USING (batch_id)
        ORDER BY attempts.ordinal`).all();
      raw.close();
      assert.deepEqual(attempts.map(attempt => [attempt.ordinal, attempt.status]), [
        [1, 'not_accepted'], [2, 'receiver_accepted']
      ]);
      assert.deepEqual(attempts.map(attempt => Buffer.from(attempt.body)), neatlogs.bodies,
        'every actual POST ordinal remains paired with the exact durable body');
    } finally {
      outbox.close();
    }
  });

  test('finite Retry-After seconds stay exact through restart and never become early work', async () => {
    for (const seconds of [11, 60, 3_600]) {
      const clock = clockAt(20_000);
      const path = dbPath();
      let outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn, retryBaseMs: 0, retryMaxMs: 0 });
      const neatlogs = fakeNeatlogs({ ingestStatus: attempt => attempt === 1 ? { status: 429, retryAfter: String(seconds) } : 200 });
      try {
        outbox.enqueue([entry(60 + seconds)]);
        const first = await drainNeatlogsOutbox(outbox, config({ readback: false }), {
          fetch: neatlogs.fetchImpl, sleep: noSleep
        });
        assert.equal(first.sendStatus, 'not_accepted');
        assert.equal(first.sendRequests, 1);
        const raw = new DatabaseSync(path, { readOnly: true });
        const row = raw.prepare('SELECT retry_not_before_ms FROM neatlogs_delivery_queue').get();
        const attempt = raw.prepare('SELECT retry_after_minimum_ms FROM neatlogs_delivery_attempts').get();
        assert.equal(attempt.retry_after_minimum_ms, seconds * 1_000);
        assert.equal(row.retry_not_before_ms, clock.now + seconds * 1_000);
        raw.close();

        outbox.close();
        outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn, retryBaseMs: 0, retryMaxMs: 0 });
        assert.equal((await drainNeatlogsOutbox(outbox, config({ readback: false }), {
          fetch: neatlogs.fetchImpl, sleep: noSleep
        })).sendRequests, 0, `${seconds}s must survive reopen without an early POST`);
        clock.now += seconds * 1_000 - 1;
        assert.equal((await drainNeatlogsOutbox(outbox, config({ readback: false }), {
          fetch: neatlogs.fetchImpl, sleep: noSleep
        })).sendRequests, 0, `${seconds}s - 1ms remains too early`);
        clock.now += 1;
        assert.equal((await drainNeatlogsOutbox(outbox, config({ readback: false }), {
          fetch: neatlogs.fetchImpl, sleep: noSleep
        })).sendRequests, 1, `${seconds}s becomes eligible exactly at the stored minimum`);
        assert.equal(neatlogs.bodies.length, 2);
      } finally {
        outbox.close();
      }
    }
  });

  test('Retry-After decimal parsing preserves leading zeroes and only saturates unrepresentable delays', async () => {
    const cases = [
      { header: '0000000000000000000000000000000011', expectedMs: 11_000, expectedNotBefore: 31_000 },
      { header: '9007199254740992', expectedMs: Number.MAX_SAFE_INTEGER, expectedNotBefore: Number.MAX_SAFE_INTEGER }
    ];
    for (const [index, retryCase] of cases.entries()) {
      const clock = clockAt(20_000);
      const path = dbPath();
      const outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn, retryBaseMs: 0, retryMaxMs: 0,
        maxBatchesPerDrain: 1 });
      const neatlogs = fakeNeatlogs({ ingestStatus: () => ({ status: 429, retryAfter: retryCase.header }) });
      try {
        outbox.enqueue([entry(70 + index)]);
        const result = await drainNeatlogsOutbox(outbox, config({ readback: false }), {
          fetch: neatlogs.fetchImpl, sleep: noSleep
        });
        assert.equal(result.sendStatus, 'not_accepted');
        const raw = new DatabaseSync(path, { readOnly: true });
        const attempt = raw.prepare('SELECT retry_after_minimum_ms FROM neatlogs_delivery_attempts').get();
        const row = raw.prepare('SELECT retry_not_before_ms FROM neatlogs_delivery_queue').get();
        raw.close();
        assert.equal(attempt.retry_after_minimum_ms, retryCase.expectedMs);
        assert.equal(row.retry_not_before_ms, retryCase.expectedNotBefore,
          'the queue stores the exact finite minimum or the safe saturating value for an unrepresentable one');
      } finally {
        outbox.close();
      }
    }
  });

  test('server minimum on an ambiguous 503 survives bounded readback before generation change', async () => {
    const clock = clockAt(10_000);
    const outbox = NeatlogsOutbox.open({ databasePath: dbPath(), clock: clock.fn, retryBaseMs: 1, retryMaxMs: 100 });
    const neatlogs = fakeNeatlogs({ ingestStatus: () => ({ status: 503, retryAfter: '3600' }) });
    try {
      outbox.enqueue([entry(29)]);
      const first = await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      assert.equal(first.sendStatus, 'outcome_unknown');
      assert.equal(first.sendRequests, 1);
      clock.now += NEATLOGS_OUTBOX_LIMITS.defaults.missingAfterMs + NEATLOGS_OUTBOX_LIMITS.defaults.readbackDelayMs;
      const observe = await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      assert.equal(observe.regenerated, 1, 'a later generation follows bounded non-observation, not a blind retry');
      assert.equal(observe.sendRequests, 0, 'the persisted Retry-After still gates the next POST');
      clock.now += 3_600_000 - NEATLOGS_OUTBOX_LIMITS.defaults.missingAfterMs
        - NEATLOGS_OUTBOX_LIMITS.defaults.readbackDelayMs;
      const due = await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      assert.equal(due.sendRequests, 1);
      assert.equal(neatlogs.calls.filter(call => call.path === '/v1/traces').length, 2);
    } finally {
      outbox.close();
    }
  });

  test('the eight-attempt policy counts actual POSTs, not helper calls', async () => {
    const outbox = NeatlogsOutbox.open({
      databasePath: dbPath(), retryBaseMs: 0, retryMaxMs: 0, maxBatchesPerDrain: 1
    });
    const neatlogs = fakeNeatlogs({ ingestStatus: () => 429 });
    try {
      outbox.enqueue([entry(30)]);
      let last;
      for (let attempt = 0; attempt < NEATLOGS_OUTBOX_LIMITS.maxSendAttempts; attempt += 1) {
        last = await drainNeatlogsOutbox(outbox, config({ readback: false }), {
          fetch: neatlogs.fetchImpl, sleep: noSleep
        });
      }
      assert.equal(neatlogs.calls.filter(call => call.path === '/v1/traces').length, 8);
      assert.equal(last.sendRequests, 1);
      assert.equal(outbox.diagnostics().latest[0].postAttempts, 8);
      assert.equal(outbox.diagnostics().latest[0].state, 'failed');
    } finally {
      outbox.close();
    }
  });

  test('without a readback token, rows stay sent and are never claimed as delivered', async () => {
    const clock = clockAt(10_000);
    const outbox = NeatlogsOutbox.open({ databasePath: dbPath(), clock: clock.fn });
    const neatlogs = fakeNeatlogs();
    try {
      outbox.enqueue([entry(1)]);
      await drainNeatlogsOutbox(outbox, config({ readback: false }), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      clock.now += 3_600_000;
      const result = await drainNeatlogsOutbox(outbox, config({ readback: false }), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      assert.equal(result.readbackStatus, null);
      assert.equal(outbox.diagnostics().counts.sent, 1);
      assert.equal(outbox.diagnostics().counts.verified, 0);
    } finally {
      outbox.close();
    }
  });

  test('a lost first delivery waits out the window, then moves to the next generation and verifies', async () => {
    const clock = clockAt(10_000);
    const outbox = NeatlogsOutbox.open({ databasePath: dbPath(), clock: clock.fn });
    const lostId = deterministicNeatlogsIds(token(7)).traceId;
    const neatlogs = fakeNeatlogs({ loseFirstDelivery: [lostId] });
    const drain = () => drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
    try {
      outbox.enqueue([entry(7)]);
      await drain();
      clock.now += NEATLOGS_OUTBOX_LIMITS.defaults.readbackDelayMs;
      const early = await drain();
      assert.equal(early.waiting, 1, 'not yet past the missing window');
      assert.equal(outbox.diagnostics().counts.sent, 1);

      clock.now += NEATLOGS_OUTBOX_LIMITS.defaults.missingAfterMs;
      const regenerated = await drain();
      assert.equal(regenerated.regenerated, 1);
      assert.equal(regenerated.sent, 1, 'the next generation is sent in the same pass');
      const nextId = deterministicNeatlogsIds(token(7), 1).traceId;
      assert.equal(outbox.diagnostics().latest[0].traceId, nextId);
      assert.equal(outbox.diagnostics().latest[0].generation, 1);

      clock.now += NEATLOGS_OUTBOX_LIMITS.defaults.readbackDelayMs;
      const verified = await drain();
      assert.equal(verified.verified, 1);
      assert.deepEqual([...outbox.diagnostics().latest[0].duplicateTraceIds], []);
      assert.ok(neatlogs.stored.has(nextId));
      assert.equal(neatlogs.stored.has(lostId), false);
    } finally {
      outbox.close();
    }
  });

  test('a finite 404 after the last generation stays held and preserves identity', async () => {
    const clock = clockAt(10_000);
    const path = dbPath();
    const outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn,
      readbackDelayMs: 0, missingAfterMs: 0, pendingGiveUpMs: 1_000_000 });
    const ids = [0, 1, 2, 3].map(g => deterministicNeatlogsIds(token(9), g).traceId);
    const neatlogs = fakeNeatlogs({ loseFirstDelivery: ids });
    try {
      outbox.enqueue([entry(9)]);
      for (let round = 0; round < 8; round += 1) {
        await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
        clock.now += 1;
      }
      const [row] = outbox.diagnostics().latest;
      assert.equal(row.state, 'sent');
      assert.equal(row.lastReason, 'MISSING_AFTER_LAST_GENERATION');
      assert.equal(row.generation, 3);
      assert.equal(row.postAttempts, 1, 'a final finite 404 never creates a fourth-generation replay');
      assert.equal(outbox.diagnostics().counts.unresolvedHeld, 1);
      const raw = new DatabaseSync(path, { readOnly: true });
      const identity = raw.prepare('SELECT event_token, trace_id, prior_trace_ids, state, settled_at_ms, next_readback_at_ms FROM neatlogs_delivery_queue').get();
      assert.deepEqual(JSON.parse(identity.prior_trace_ids), ids.slice(0, 3));
      assert.equal(identity.event_token, token(9));
      assert.equal(identity.trace_id, ids[3]);
      assert.equal(identity.state, 'sent');
      assert.equal(identity.settled_at_ms, null);
      assert.equal(identity.next_readback_at_ms, null);
      raw.close();
      const callsAtHold = neatlogs.calls.length;
      clock.now += 30 * 24 * 60 * 60 * 1_000;
      await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      assert.equal(neatlogs.calls.length, callsAtHold, 'the bounded 404 result does not cause automatic re-polling');
      assert.equal(outbox.diagnostics().counts.sent, 1, 'unresolved work remains outside settled pruning');
    } finally {
      outbox.close();
    }
  });

  test('pending, conflict, finalization and projection deadlines remain held without replay or pruning', async () => {
    const cases = [
      { kind: 'pending-finalization', expectedReason: 'NOT_FINALIZED_IN_TIME', finalizationStatus: () => 'pending' },
      { kind: 'conflict', expectedReason: 'READBACK_CONFLICT_DEADLINE' },
      { kind: 'projection', expectedReason: 'READBACK_PROJECTION_DEADLINE' },
      { kind: 'transport', expectedReason: 'READBACK_RESOLUTION_DEADLINE' }
    ];
    for (const [index, outcomeCase] of cases.entries()) {
      const n = 41 + index;
      const traceId = deterministicNeatlogsIds(token(n)).traceId;
      const clock = clockAt(10_000);
      const outbox = NeatlogsOutbox.open({ databasePath: dbPath(), clock: clock.fn,
        maxUnsettled: 1, readbackDelayMs: 0, pendingGiveUpMs: 1, missingAfterMs: 0, maxSettledRows: 0 });
      const neatlogs = fakeNeatlogs({
        readbackStatus: outcomeCase.kind === 'conflict' ? path => path.endsWith(`/traces/${traceId}`) ? 409 : 200
          : outcomeCase.kind === 'projection' ? path => path.endsWith('/spans') ? 404 : 200
            : outcomeCase.kind === 'transport' ? path => path.endsWith(`/traces/${traceId}`) ? 500 : 200
              : () => 200,
        finalizationStatus: outcomeCase.finalizationStatus
      });
      try {
        const original = entry(n);
        assert.equal(outbox.enqueue([original]).accepted, 1);
        const sent = await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
        assert.equal(sent.sent, 1);
        clock.now += 1;
        const held = await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
        assert.equal(held.held, 1, `${outcomeCase.kind} remains held at the bounded stop deadline`);
        const row = outbox.diagnostics().latest[0];
        assert.equal(row.state, 'sent');
        assert.equal(row.traceId, traceId);
        assert.equal(row.generation, 0);
        assert.equal(row.postAttempts, 1);
        assert.equal(row.lastStatus, 'readback_resolution_held');
        assert.equal(row.lastReason, outcomeCase.expectedReason);
        assert.equal(outbox.diagnostics().counts.unresolvedHeld, 1);
        assert.deepEqual({ ...outbox.enqueue([original]) },
          { accepted: 0, duplicates: 1, refused: 0, conflicts: 0 }, 'a held identity remains deduplicated');
        assert.deepEqual({ ...outbox.enqueue([entry(n, { input: JSON.stringify({ changed: n }) })]) },
          { accepted: 0, duplicates: 0, refused: 0, conflicts: 1 }, 'content conflicts remain observable while held');
        assert.deepEqual({ ...outbox.enqueue([entry(n + 1000)]) },
          { accepted: 0, duplicates: 0, refused: 1, conflicts: 0 }, 'unresolved rows continue to consume unsettled capacity');
        const callsAtHold = neatlogs.calls.length;
        clock.now += 30 * 24 * 60 * 60 * 1_000;
        await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
        assert.equal(neatlogs.calls.length, callsAtHold, 'held ambiguous rows are neither re-polled nor re-posted');
        assert.equal(outbox.diagnostics().counts.sent, 1, 'settled-row pruning leaves unresolved work alone');
      } finally {
        outbox.close();
      }
    }
  });

  test('an unknown outcome is persisted and is never blindly resent after restart without readback', async () => {
    const clock = clockAt(10_000);
    const path = dbPath();
    let outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn });
    const neatlogs = fakeNeatlogs({ ingestStatus: () => 503 });
    try {
      outbox.enqueue([entry(3)]);
      const first = await drainNeatlogsOutbox(outbox, config({ readback: false }), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      assert.equal(first.retried, 1);
      assert.equal(first.sendStatus, 'outcome_unknown');
      assert.equal(first.sendRequests, 1, 'one helper call is one actual POST in the queue path');
      assert.equal(outbox.diagnostics().latest[0].postAttempts, 1);
      outbox.close();
      outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn });
      clock.now += NEATLOGS_OUTBOX_LIMITS.defaults.retryMaxMs * 2;
      const again = await drainNeatlogsOutbox(outbox, config({ readback: false }), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      assert.equal(again.sendRequests, 0, 'without readback an unknown POST remains held, not blindly retried');
      assert.equal(neatlogs.calls.filter(call => call.path === '/v1/traces').length, 1);
      const [row] = outbox.diagnostics().latest;
      assert.equal(row.state, 'sent');
      assert.equal(row.lastStatus, 'outcome_unknown');
      assert.equal(row.postAttempts, 1);
    } finally {
      outbox.close();
    }
  });

  test('reopen after plan persistence but before POST intent sends the exact stored body once', async () => {
    const clock = clockAt(15_000);
    const path = dbPath();
    let outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn, maxBatchesPerDrain: 1 });
    const lease = outbox.acquireDrainLease('prepared-before-post');
    let storedBody;
    const neatlogs = fakeNeatlogs();
    try {
      outbox.enqueue([entry(295)]);
      const [batch] = outbox.prepareSendBatches(outbox.dueForSend(), ['evidence:synthetic'], lease);
      storedBody = Buffer.from(batch.body);
      const raw = new DatabaseSync(path, { readOnly: true });
      const durable = raw.prepare('SELECT body FROM neatlogs_delivery_batches WHERE batch_id = ?').get(batch.batchId);
      assert.deepEqual(Buffer.from(durable.body), storedBody);
      assert.equal(raw.prepare('SELECT COUNT(*) AS n FROM neatlogs_delivery_attempts').get().n, 0,
        'persisting a plan alone is before the POST-intent boundary');
      raw.close();
      outbox.releaseDrainLease(lease);
      outbox.close();
      outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn, maxBatchesPerDrain: 1 });
      const resumed = await drainNeatlogsOutbox(outbox, config({ readback: false }), {
        fetch: neatlogs.fetchImpl, sleep: noSleep, tags: ['host:changed']
      });
      assert.equal(resumed.sendRequests, 1);
      assert.equal(neatlogs.bodies.length, 1);
      assert.deepEqual(neatlogs.bodies[0], storedBody,
        'reopen and tag changes reuse the already-committed request bytes');
      const after = new DatabaseSync(path, { readOnly: true });
      const attempt = after.prepare(`SELECT ordinal, status, body FROM neatlogs_delivery_attempts
        JOIN neatlogs_delivery_batches USING (batch_id)`).get();
      after.close();
      assert.equal(attempt.ordinal, 1);
      assert.equal(attempt.status, 'receiver_accepted');
      assert.deepEqual(Buffer.from(attempt.body), storedBody);
    } finally {
      outbox.close();
    }
  });

  test('lease expiry during a held POST stops the old pass and preserves prepared sibling bodies', async () => {
    const clock = clockAt(20_000);
    const path = dbPath();
    const outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn,
      drainLeaseMs: 100, maxBatchesPerDrain: 32 });
    const entered = deferred();
    const release = deferred();
    const wideAttributes = Array.from({ length: 24 }, (_, index) => [
      `test.attr${index}.${'k'.repeat(2_040)}`, '€'.repeat(256)
    ]);
    let callbackFailure;
    const neatlogs = fakeNeatlogs({ beforeFetch: async call => {
      if (call.path === '/v1/traces' && call.ingestOrdinal === 1) {
        let inspect;
        try {
          inspect = new DatabaseSync(path, { readOnly: true });
          const attempt = plainRow(inspect.prepare('SELECT status, ordinal FROM neatlogs_delivery_attempts ORDER BY ordinal DESC LIMIT 1').get());
          const durableBatch = inspect.prepare('SELECT body FROM neatlogs_delivery_batches WHERE batch_id = ?')
            .get(inspect.prepare('SELECT batch_id FROM neatlogs_delivery_attempts ORDER BY ordinal DESC LIMIT 1').get().batch_id);
          entered.resolve();
          assert.deepEqual(attempt, { status: 'outcome_unknown', ordinal: 1 }, 'intent is durable before fetch enters');
          assert.deepEqual(Buffer.from(durableBatch.body), call.body, 'the exact request bytes are durable before fetch');
        } catch (error) {
          callbackFailure ??= error;
          entered.resolve();
        } finally {
          try { inspect?.close(); } catch (error) { callbackFailure ??= error; }
        }
        await release.promise;
      }
    } });
    let newerQueue;
    let pending;
    let pendingObserved = false;
    let primaryError;
    let hasPrimaryError = false;
    let waitTimerCleared = false;
    try {
      outbox.enqueue(Array.from({ length: 32 }, (_, index) => entry(300 + index, { attributes: wideAttributes })));
      pending = drainNeatlogsOutbox(outbox, config({ readback: false }), {
        fetch: neatlogs.fetchImpl, sleep: noSleep
      });
      await awaitHeldRequest(entered, pending, 'lease expiry during held POST', {
        onTimerCleared: () => { waitTimerCleared = true; }
      });
      assert.equal(waitTimerCleared, true, 'successful rendezvous clears its referenced deadline');
      if (callbackFailure) throw callbackFailure;
      const before = new DatabaseSync(path, { readOnly: true });
      const preparedBefore = before.prepare(`SELECT position, state, batch_id FROM neatlogs_delivery_queue ORDER BY position`).all();
      const bodiesBefore = before.prepare('SELECT batch_id, body FROM neatlogs_delivery_batches ORDER BY created_at_ms, batch_id').all()
        .map(row => [row.batch_id, Buffer.from(row.body)]);
      const oldLease = JSON.parse(before.prepare("SELECT value FROM neatlogs_delivery_queue_meta WHERE key='drain_lease'").get().value);
      before.close();
      assert.ok(preparedBefore.some(row => row.state === 'queued' && row.batch_id !== null),
        'all later size-split sibling batches have persisted plans before the first POST');

      clock.now += 101;
      newerQueue = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn, drainLeaseMs: 100 });
      const newerLease = newerQueue.acquireDrainLease('new-fence-post');
      assert.ok(newerLease);
      assert.equal(newerLease.fence, oldLease.fence + 1);
      release.resolve();
      let result;
      try { result = await pending; } finally { pendingObserved = true; }
      assert.equal(result.leaseLost, true);
      assert.equal(result.sendRequests, 1);
      assert.equal(neatlogs.calls.filter(call => call.path === '/v1/traces').length, 1,
        'the stale pass sends no prepared sibling after its first in-flight request');

      const after = new DatabaseSync(path, { readOnly: true });
      const rowsAfter = after.prepare('SELECT position, state, batch_id, send_attempts FROM neatlogs_delivery_queue ORDER BY position').all();
      const bodiesAfter = after.prepare('SELECT batch_id, body FROM neatlogs_delivery_batches ORDER BY created_at_ms, batch_id').all()
        .map(row => [row.batch_id, Buffer.from(row.body)]);
      const leaseAfter = JSON.parse(after.prepare("SELECT value FROM neatlogs_delivery_queue_meta WHERE key='drain_lease'").get().value);
      after.close();
      assert.deepEqual(bodiesAfter, bodiesBefore, 'unsent prepared bodies remain byte-identical');
      assert.ok(rowsAfter.some(row => row.state === 'sent' && row.send_attempts === 1));
      assert.ok(rowsAfter.some(row => row.state === 'queued' && row.batch_id !== null && row.send_attempts === 0));
      assert.deepEqual({ owner: leaseAfter.owner, fence: leaseAfter.fence },
        { owner: newerLease.owner, fence: newerLease.fence }, 'old finally-release cannot clear the higher fence');
    } catch (error) {
      primaryError = error;
      hasPrimaryError = true;
    } finally {
      await finishHeldQueueFixture({
        release, pending, pendingObserved,
        markPendingObserved: () => { pendingObserved = true; },
        queues: [newerQueue, outbox], primaryError, hasPrimaryError,
        label: 'held POST assertion and drain/queue cleanup failed'
      });
    }
  });

  test('lease expiry during held readback keeps the sent row and newer fence intact', async () => {
    const clock = clockAt(30_000);
    const path = dbPath();
    const outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn, drainLeaseMs: 100 });
    const entered = deferred();
    const release = deferred();
    let blockedTraceId = null;
    const neatlogs = fakeNeatlogs({ beforeFetch: async call => {
      if (call.method === 'GET' && call.path === `/api/v1/public/traces/${blockedTraceId}`) {
        entered.resolve();
        await release.promise;
      }
    } });
    let newerQueue;
    let pending;
    let pendingObserved = false;
    let primaryError;
    let hasPrimaryError = false;
    try {
      outbox.enqueue([entry(340)]);
      await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      clock.now += NEATLOGS_OUTBOX_LIMITS.defaults.readbackDelayMs;
      const traceId = blockedTraceId = deterministicNeatlogsIds(token(340)).traceId;
      pending = drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      await awaitHeldRequest(entered, pending, 'lease expiry during held readback');
      const before = new DatabaseSync(path, { readOnly: true });
      const oldLease = JSON.parse(before.prepare("SELECT value FROM neatlogs_delivery_queue_meta WHERE key='drain_lease'").get().value);
      before.close();
      clock.now += 101;
      newerQueue = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn, drainLeaseMs: 100 });
      const newerLease = newerQueue.acquireDrainLease('new-fence-readback');
      assert.ok(newerLease);
      assert.equal(newerLease.fence, oldLease.fence + 1);
      release.resolve();
      let result;
      try { result = await pending; } finally { pendingObserved = true; }
      assert.equal(result.leaseLost, true);
      assert.equal(neatlogs.calls.filter(call => call.method === 'GET').length, 2,
        'stale readback stops before the next spans request');
      const after = new DatabaseSync(path, { readOnly: true });
      const row = after.prepare('SELECT state, trace_id, last_status, settled_at_ms FROM neatlogs_delivery_queue').get();
      const leaseAfter = JSON.parse(after.prepare("SELECT value FROM neatlogs_delivery_queue_meta WHERE key='drain_lease'").get().value);
      after.close();
      assert.equal(row.state, 'sent');
      assert.equal(row.trace_id, traceId);
      assert.equal(row.settled_at_ms, null);
      assert.deepEqual({ owner: leaseAfter.owner, fence: leaseAfter.fence },
        { owner: newerLease.owner, fence: newerLease.fence });
    } catch (error) {
      primaryError = error;
      hasPrimaryError = true;
    } finally {
      await finishHeldQueueFixture({
        release, pending, pendingObserved,
        markPendingObserved: () => { pendingObserved = true; },
        queues: [newerQueue, outbox], primaryError, hasPrimaryError,
        label: 'held readback assertion and drain/queue cleanup failed'
      });
    }
  });

  test('a late generation-zero response after a higher-fence generation-one send cannot overwrite it', async () => {
    const clock = clockAt(40_000);
    const path = dbPath();
    const options = { databasePath: path, clock: clock.fn, drainLeaseMs: 100,
      readbackDelayMs: 0, missingAfterMs: 0, pendingGiveUpMs: 1_000_000, maxBatchesPerDrain: 1 };
    const oldQueue = NeatlogsOutbox.open(options);
    const entered = deferred();
    const release = deferred();
    const neatlogs = fakeNeatlogs({ beforeFetch: async call => {
      if (call.path === '/v1/traces' && call.ingestOrdinal === 1) {
        entered.resolve();
        await release.promise;
      }
    } });
    let newQueue;
    let oldDrain;
    let oldDrainObserved = false;
    let primaryError;
    let hasPrimaryError = false;
    try {
      oldQueue.enqueue([entry(350)]);
      oldDrain = drainNeatlogsOutbox(oldQueue, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      await awaitHeldRequest(entered, oldDrain, 'late generation-zero held POST');
      clock.now += 101;
      newQueue = NeatlogsOutbox.open(options);
      const newDrain = await drainNeatlogsOutbox(newQueue, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      assert.equal(newDrain.regenerated, 1, 'higher fence sees a finite gen0 404 and advances within the configured window');
      assert.equal(newDrain.sent, 1, 'the new generation is sent before the old POST response returns');
      const generationOneId = deterministicNeatlogsIds(token(350), 1).traceId;
      assert.ok(neatlogs.stored.has(generationOneId));
      release.resolve();
      let oldResult;
      try { oldResult = await oldDrain; } finally { oldDrainObserved = true; }
      assert.equal(oldResult.leaseLost, true, 'the old response is not persisted after the newer fence');
      assert.ok(neatlogs.stored.has(deterministicNeatlogsIds(token(350), 0).traceId),
        'the late response may have reached the receiver after the generation decision');
      const row = newQueue.diagnostics().latest[0];
      assert.equal(row.state, 'sent');
      assert.equal(row.generation, 1);
      assert.equal(row.traceId, generationOneId);
      assert.equal(row.postAttempts, 1);
      const raw = new DatabaseSync(path, { readOnly: true });
      const attempts = plainRows(raw.prepare(`SELECT batches.generation, attempts.status
        FROM neatlogs_delivery_attempts AS attempts
        JOIN neatlogs_delivery_batches AS batches USING (batch_id)
        ORDER BY batches.generation`).all());
      raw.close();
      assert.deepEqual(attempts, [
        { generation: 0, status: 'outcome_unknown' },
        { generation: 1, status: 'receiver_accepted' }
      ], 'the stale gen0 response is not written into durable attempt state');
      assert.equal(neatlogs.calls.filter(call => call.path === '/v1/traces').length, 2);
    } catch (error) {
      primaryError = error;
      hasPrimaryError = true;
    } finally {
      await finishHeldQueueFixture({
        release, pending: oldDrain, pendingObserved: oldDrainObserved,
        markPendingObserved: () => { oldDrainObserved = true; },
        queues: [newQueue, oldQueue], primaryError, hasPrimaryError,
        label: 'late generation-zero assertion and drain/queue cleanup failed'
      });
    }
  });

  test('held-request wait rejects when a real queue drain fulfills before entry', async () => {
    const outbox = NeatlogsOutbox.open({ databasePath: dbPath(), clock: () => 60_000 });
    const entered = deferred();
    const release = deferred();
    const neatlogs = fakeNeatlogs();
    let pending;
    let pendingObserved = false;
    let primaryError;
    let hasPrimaryError = false;
    let waitTimerCleared = false;
    try {
      pending = drainNeatlogsOutbox(outbox, config({ readback: false }), {
        fetch: neatlogs.fetchImpl, sleep: noSleep
      });
      let waitFailure;
      try {
        await awaitHeldRequest(entered, pending, 'empty real queue drain', {
          timeoutMs: 1_000,
          onTimerCleared: () => { waitTimerCleared = true; }
        });
      } catch (error) {
        waitFailure = error;
      }
      assert.match(waitFailure?.message ?? '', /drain completed before the held request entered/);
      assert.equal(waitTimerCleared, true, 'early fulfillment clears the referenced deadline');
      let result;
      try { result = await pending; } finally { pendingObserved = true; }
      assert.equal(result.sendRequests, 0);
      assert.equal(neatlogs.calls.length, 0, 'an empty drain starts no fake request');
    } catch (error) {
      primaryError = error;
      hasPrimaryError = true;
    } finally {
      await finishHeldQueueFixture({
        release, pending, pendingObserved,
        markPendingObserved: () => { pendingObserved = true; },
        queues: [outbox], primaryError, hasPrimaryError,
        label: 'early-fulfillment rendezvous control cleanup failed'
      });
    }
  });

  test('held-request wait preserves the cause when a real queue drain rejects before entry', async () => {
    const outbox = NeatlogsOutbox.open({ databasePath: dbPath(), clock: () => 61_000 });
    const entered = deferred();
    const release = deferred();
    const neatlogs = fakeNeatlogs();
    let pending;
    let pendingObserved = false;
    let primaryError;
    let hasPrimaryError = false;
    let waitTimerCleared = false;
    try {
      pending = drainNeatlogsOutbox(outbox, config({ readback: false }), {
        fetch: neatlogs.fetchImpl, sleep: noSleep, tags: [null]
      });
      let waitFailure;
      try {
        await awaitHeldRequest(entered, pending, 'invalid-tags real queue drain', {
          timeoutMs: 1_000,
          onTimerCleared: () => { waitTimerCleared = true; }
        });
      } catch (error) {
        waitFailure = error;
      }
      assert.match(waitFailure?.message ?? '', /drain failed before the held request entered/);
      assert.equal(waitFailure?.cause?.code, 'INVALID_TAGS');
      assert.equal(waitTimerCleared, true, 'early rejection clears the referenced deadline');
      let drainFailure;
      try { await pending; } catch (error) { drainFailure = error; }
      finally { pendingObserved = true; }
      assert.equal(waitFailure.cause, drainFailure, 'the rendezvous retains the original rejection by identity');
      assert.equal(neatlogs.calls.length, 0, 'validation rejects before any fake request starts');
    } catch (error) {
      primaryError = error;
      hasPrimaryError = true;
    } finally {
      await finishHeldQueueFixture({
        release, pending, pendingObserved,
        markPendingObserved: () => { pendingObserved = true; },
        queues: [outbox], primaryError, hasPrimaryError,
        label: 'early-rejection rendezvous control cleanup failed'
      });
    }
  });

  test('held-request timeout releases a real callback whose entry signal never arrives', async () => {
    const clock = clockAt(62_000);
    const path = dbPath();
    const outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn });
    const entered = deferred();
    const release = deferred();
    let callbackStarted = false;
    const neatlogs = fakeNeatlogs({ beforeFetch: async call => {
      if (call.path === '/v1/traces') {
        callbackStarted = true;
        await release.promise;
      }
    } });
    let pending;
    let pendingObserved = false;
    let primaryError;
    let hasPrimaryError = false;
    let waitTimerCleared = false;
    const lifecycle = [];
    try {
      outbox.enqueue([entry(355)]);
      pending = drainNeatlogsOutbox(outbox, config({ readback: false }), {
        fetch: neatlogs.fetchImpl, sleep: noSleep
      });
      let waitFailure;
      try {
        await awaitHeldRequest(entered, pending, 'unresolved real callback entry', {
          timeoutMs: 1_000,
          onTimerCleared: () => { waitTimerCleared = true; }
        });
      } catch (error) {
        waitFailure = error;
      }
      assert.match(waitFailure?.message ?? '', /did not enter within 1000ms/);
      assert.equal(callbackStarted, true, 'the real fake-fetch callback started but never signaled entry');
      assert.equal(waitTimerCleared, true, 'timeout clears its referenced deadline');
      assert.equal(neatlogs.calls.filter(call => call.path === '/v1/traces').length, 1);
    } catch (error) {
      primaryError = error;
      hasPrimaryError = true;
    } finally {
      await finishHeldQueueFixture({
        release, pending, pendingObserved,
        markPendingObserved: () => { pendingObserved = true; },
        queues: [outbox], primaryError, hasPrimaryError,
        label: 'never-entered rendezvous control cleanup failed',
        onLifecycle: event => lifecycle.push(event)
      });
    }
    assert.deepEqual(lifecycle, ['released', 'drain-settled', 'queue-close-attempted']);
  });

  test('held-request assertion failure retains cleanup error and closes both real queue handles', async () => {
    const clock = clockAt(63_000);
    const path = dbPath();
    const outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn });
    const secondQueue = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn });
    const entered = deferred();
    const release = deferred();
    const lifecycle = [];
    const neatlogs = fakeNeatlogs({ beforeFetch: async call => {
      if (call.path === '/v1/traces') {
        lifecycle.push('request-entered');
        entered.resolve();
        await release.promise;
      }
    } });
    const cleanupFailure = new Error('synthetic first-handle close failure after real close');
    const realFirstClose = outbox.close.bind(outbox);
    outbox.close = () => {
      realFirstClose();
      lifecycle.push('first-handle-closed');
      throw cleanupFailure;
    };
    const realSecondClose = secondQueue.close.bind(secondQueue);
    secondQueue.close = () => {
      realSecondClose();
      lifecycle.push('second-handle-closed');
    };
    let pending;
    let pendingObserved = false;
    let primaryError;
    let hasPrimaryError = false;
    let waitTimerCleared = false;
    let cleanupOutcome;
    try {
      outbox.enqueue([entry(356)]);
      pending = drainNeatlogsOutbox(outbox, config({ readback: false }), {
        fetch: neatlogs.fetchImpl, sleep: noSleep
      });
      await awaitHeldRequest(entered, pending, 'intentional assertion-failure held request', {
        onTimerCleared: () => { waitTimerCleared = true; }
      });
      assert.equal(waitTimerCleared, true, 'successful held entry clears its referenced deadline');
      assert.equal(neatlogs.calls.filter(call => call.path === '/v1/traces').length, 1);
      assert.fail('synthetic assertion failure after the held POST enters');
    } catch (error) {
      primaryError = error;
      hasPrimaryError = true;
    } finally {
      try {
        await finishHeldQueueFixture({
          release, pending, pendingObserved,
          markPendingObserved: () => { pendingObserved = true; },
          queues: [outbox, secondQueue], primaryError, hasPrimaryError,
          label: 'held request assertion and real handle cleanup failed',
          onLifecycle: event => {
            if (event === 'released' || event === 'drain-settled') lifecycle.push(event);
          }
        });
      } catch (error) {
        cleanupOutcome = error;
      }
    }
    assert.ok(primaryError instanceof assert.AssertionError, 'the primary failure is the intentional assertion');
    assert.ok(cleanupOutcome instanceof AggregateError);
    assert.equal(cleanupOutcome.errors[0], primaryError, 'the original assertion keeps its identity and order');
    assert.equal(cleanupOutcome.cause, primaryError, 'the original assertion remains the aggregate cause');
    assert.equal(cleanupOutcome.errors[1], cleanupFailure, 'the first-close failure remains observable');
    assert.deepEqual(lifecycle, [
      'request-entered', 'released', 'drain-settled', 'first-handle-closed', 'second-handle-closed'
    ], 'the held request releases, settles, then both real handles close despite the first close error');
    assert.equal(neatlogs.calls.filter(call => call.path === '/v1/traces').length, 1);
  });

  test('cancellation after POST start retains unknown outcome and does not replay it', async () => {
    const clock = clockAt(50_000);
    const path = dbPath();
    let outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn });
    const controller = new AbortController();
    const neatlogs = fakeNeatlogs({ beforeFetch: async call => {
      if (call.path === '/v1/traces') {
        controller.abort();
        const error = new Error('synthetic fetch cancellation after request start');
        error.name = 'AbortError';
        throw error;
      }
    } });
    try {
      outbox.enqueue([entry(360)]);
      const cancelled = await drainNeatlogsOutbox(outbox, config({ readback: false }), {
        fetch: neatlogs.fetchImpl, sleep: noSleep, signal: controller.signal
      });
      assert.equal(cancelled.sendRequests, 1);
      assert.equal(cancelled.sendStatus, 'outcome_unknown');
      const raw = new DatabaseSync(path, { readOnly: true });
      const row = plainRow(raw.prepare('SELECT state, send_attempts, last_status FROM neatlogs_delivery_queue').get());
      const attempt = plainRow(raw.prepare('SELECT status, reason_code FROM neatlogs_delivery_attempts').get());
      raw.close();
      assert.deepEqual(row, { state: 'sent', send_attempts: 1, last_status: 'outcome_unknown' });
      assert.equal(attempt.status, 'outcome_unknown');
      assert.equal(attempt.reason_code, 'ABORTED');
      outbox.close();
      outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn });
      clock.now += NEATLOGS_OUTBOX_LIMITS.defaults.retryMaxMs * 2;
      const again = await drainNeatlogsOutbox(outbox, config({ readback: false }), {
        fetch: neatlogs.fetchImpl, sleep: noSleep
      });
      assert.equal(again.sendRequests, 0);
      assert.equal(neatlogs.calls.filter(call => call.path === '/v1/traces').length, 1);
      assert.equal(outbox.diagnostics().latest[0].state, 'sent');
    } finally {
      outbox.close();
    }
  });

  test('a readback mismatch settles the row as failed', async () => {
    const clock = clockAt(10_000);
    const outbox = NeatlogsOutbox.open({ databasePath: dbPath(), clock: clock.fn });
    const neatlogs = fakeNeatlogs();
    try {
      outbox.enqueue([entry(4)]);
      await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      neatlogs.stored.get(deterministicNeatlogsIds(token(4)).traceId).input = '{"altered":true}';
      clock.now += NEATLOGS_OUTBOX_LIMITS.defaults.readbackDelayMs;
      const result = await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      assert.equal(result.failed, 1);
      assert.equal(outbox.diagnostics().latest[0].lastReason, 'INPUT_PAYLOAD_MISMATCH');
    } finally {
      outbox.close();
    }
  });

  test('splits full-width accepted attributes into request-size-bounded batches', async () => {
    const clock = clockAt(10_000);
    const outbox = NeatlogsOutbox.open({ databasePath: dbPath(), clock: clock.fn, maxBatchesPerDrain: 32 });
    const neatlogs = fakeNeatlogs();
    const wideAttributes = Array.from({ length: 24 }, (_, index) => [
      `test.attr${index}.${'k'.repeat(2_040)}`, '€'.repeat(256)
    ]);
    const input = JSON.stringify({ payload: '\\'.repeat(2_041) });
    const tags = Array.from({ length: NEATLOGS_OTLP_LIMITS.tags }, (_, index) =>
      `${String(index).padStart(2, '0')}${'x'.repeat(126)}`);
    try {
      assert.equal(Buffer.byteLength(input, 'utf8'), 4_096);
      assert.equal(wideAttributes[0][1].length, 256, 'the fixture uses the original UTF-16 string limit');
      assert.equal(Buffer.byteLength(wideAttributes[0][1], 'utf8'), 768, 'three-byte BMP text exercises maximum UTF-8 width');
      outbox.enqueue(Array.from({ length: 32 }, (_, index) => entry(100 + index, {
        attributes: wideAttributes,
        occurredAtMs: 100_000_000_000_000,
        input
      })));
      const result = await drainNeatlogsOutbox(outbox, config({ readback: false }), {
        fetch: neatlogs.fetchImpl, sleep: noSleep, tags
      });
      assert.equal(result.sent, 32);
      assert.equal(result.sendRequests, result.sendBatches);
      assert.ok(result.sendBatches > 1, 'the 32-row prefix is split by encoded size rather than rejected');
      assert.ok(neatlogs.bodies.every(body => body.length <= 256 * 1024));
      assert.equal(neatlogs.stored.size, 32);
    } finally {
      outbox.close();
    }
  });

  test('large source event time stays JSON-only beside a normal millisecond send clock', async () => {
    const clock = clockAt(1_800_000_000_000);
    const path = dbPath();
    const outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn });
    const neatlogs = fakeNeatlogs();
    try {
      const largeSourceTime = entry(145, { occurredAtMs: 100_000_000_000_000 });
      const upperSourceTime = entry(146, { occurredAtMs: 8.64e15 - 1 });
      assert.equal(outbox.enqueue([largeSourceTime, upperSourceTime]).accepted, 2);
      const raw = new DatabaseSync(path, { readOnly: true });
      let storedSourceTimes;
      try {
        storedSourceTimes = plainRows(raw.prepare(`SELECT event_token, occurred_at_ms, input
          FROM neatlogs_delivery_queue ORDER BY position`).all());
      } finally {
        raw.close();
      }
      assert.deepEqual(storedSourceTimes, [
        { event_token: token(145), occurred_at_ms: 100_000_000_000_000, input: largeSourceTime.input },
        { event_token: token(146), occurred_at_ms: 8.64e15 - 1, input: upperSourceTime.input }
      ], 'enqueue retains both accepted source timestamps and immutable inputs');
      const result = await drainNeatlogsOutbox(outbox, config({ readback: false }), {
        fetch: neatlogs.fetchImpl, sleep: noSleep
      });
      assert.equal(result.sent, 2);
      assert.equal(result.sendRequests, 1);
      assert.equal(neatlogs.bodies.length, 1);
      for (const eventNumber of [145, 146]) {
        const trace = neatlogs.stored.get(deterministicNeatlogsIds(token(eventNumber)).traceId);
        assert.equal(trace.startTimeUnixNano, '1800000000000000000',
          'the OTLP fixed64 span timestamp comes from the 1.8e12 millisecond send clock');
        assert.equal(trace.endTimeUnixNano, '1800000000001000000');
      }
      assert.ok(neatlogs.bodies[0].length <= NEATLOGS_OTLP_LIMITS.requestBytes,
        'the accepted source timestamps remain row data and do not overflow span time');
    } finally {
      outbox.close();
    }
  });

  test('readback advances from attempted traces and prevents starvation across a 40-row batch sequence', async () => {
    const clock = clockAt(10_000);
    const path = dbPath();
    let outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn, maxBatchesPerDrain: 32 });
    const failurePositions = [1, 9, 17, 25, 33];
    const failureIds = new Set(failurePositions.map(position => deterministicNeatlogsIds(token(199 + position)).traceId));
    const failedOnce = new Set();
    const neatlogs = fakeNeatlogs({ readbackStatus: urlPath => {
      for (const traceId of failureIds) {
        if (urlPath.endsWith(`/traces/${traceId}`) && !failedOnce.has(traceId)) {
          failedOnce.add(traceId);
          return 500;
        }
      }
      return 200;
    } });
    let lease;
    try {
      outbox.enqueue(Array.from({ length: 40 }, (_, index) => entry(200 + index)));
      const sent = await drainNeatlogsOutbox(outbox, config({ readback: false }), {
        fetch: neatlogs.fetchImpl, sleep: noSleep
      });
      assert.equal(sent.sent, 40);
      assert.equal(outbox.diagnostics().counts.sent, 40);

      clock.now += NEATLOGS_OUTBOX_LIMITS.defaults.readbackDelayMs;
      lease = outbox.acquireDrainLease('cursor-page-bound');
      assert.ok(lease);
      const boundedPage = outbox.dueForReadback(32, lease);
      assert.equal(boundedPage.length, 32, 'readback selects a bounded page even when 40 rows are due');
      assert.deepEqual(boundedPage.map(row => row.position), Array.from({ length: 32 }, (_, index) => index + 1));
      outbox.releaseDrainLease(lease);
      lease = null;

      let raw = new DatabaseSync(path, { readOnly: true });
      try {
        for (const expectedPosition of failurePositions) {
          if (expectedPosition === 33) {
            const cursorBeforeRestart = Number(raw.prepare("SELECT value FROM neatlogs_delivery_queue_meta WHERE key='readback_cursor'").get().value);
            raw.close();
            outbox.close();
            outbox = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn, maxBatchesPerDrain: 32 });
            raw = new DatabaseSync(path, { readOnly: true });
            const cursorAfterRestart = Number(raw.prepare("SELECT value FROM neatlogs_delivery_queue_meta WHERE key='readback_cursor'").get().value);
            assert.equal(cursorAfterRestart, cursorBeforeRestart, 'actual attempted position survives reopening the database');
          }
          clock.now += NEATLOGS_OUTBOX_LIMITS.defaults.readbackDelayMs;
          await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
          const cursor = Number(raw.prepare("SELECT value FROM neatlogs_delivery_queue_meta WHERE key='readback_cursor'").get().value);
          assert.equal(cursor, expectedPosition,
            `the fairness cursor follows the last trace whose readback request actually started (${expectedPosition})`);
        }
        for (let pass = 0; pass < 10 && outbox.diagnostics().counts.verified < 40; pass += 1) {
          clock.now += NEATLOGS_OUTBOX_LIMITS.defaults.readbackDelayMs;
          await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
        }
        assert.equal(outbox.diagnostics().counts.verified, 40,
          'rows behind failures at 1, 9, 17, 25 and 33 become readable without cursor starvation');
        assert.equal(neatlogs.calls.filter(call => call.path === '/v1/traces').length, 2,
          'readback retries do not create new POSTs');
      } finally {
        raw.close();
      }
    } finally {
      if (lease) outbox.releaseDrainLease(lease);
      outbox.close();
    }
  });

  test('settled replay retention is bounded by both age and row count', async () => {
    const clock = clockAt(10_000);
    const outbox = NeatlogsOutbox.open({ databasePath: dbPath(), clock: clock.fn, maxSettledRows: 1 });
    const neatlogs = fakeNeatlogs();
    try {
      outbox.enqueue([entry(240), entry(241), entry(242)]);
      await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      clock.now += NEATLOGS_OUTBOX_LIMITS.defaults.readbackDelayMs;
      await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      assert.equal(outbox.diagnostics().counts.verified, 1);
      assert.deepEqual({ ...outbox.enqueue([entry(240)]) },
        { accepted: 1, duplicates: 0, refused: 0, conflicts: 0 },
        'dedupe history expires when the bounded settled-row replay horizon evicts its row');
    } finally {
      outbox.close();
    }
  });

  test('pruning one settled row retains a shared batch while a sibling is still pending', () => {
    const clock = clockAt(10_000);
    const outbox = NeatlogsOutbox.open({ databasePath: dbPath(), clock: clock.fn, maxSettledRows: 0 });
    let lease;
    try {
      outbox.enqueue([entry(250), entry(251)]);
      lease = outbox.acquireDrainLease('shared-batch-owner');
      assert.ok(lease);
      const [batch] = outbox.prepareSendBatches(outbox.dueForSend(), ['evidence:synthetic'], lease);
      outbox.beginPost(batch, lease);
      outbox.recordSend(batch, { status: 'receiver_accepted', attempts: 1, httpStatus: 200, reasonCode: null }, lease);
      clock.now += NEATLOGS_OUTBOX_LIMITS.defaults.readbackDelayMs;
      const [first, second] = outbox.dueForReadback(32, lease);
      assert.equal(outbox.recordReadback(first, { status: 'verified', duplicateTraceIds: [] }, lease), 'verified');
      assert.equal(outbox.recordReadback(second, { status: 'pending', duplicateTraceIds: [] }, lease), 'waiting');
      assert.equal(outbox.prune(lease), 1, 'the zero-row retention limit prunes the settled first member');
      clock.now += NEATLOGS_OUTBOX_LIMITS.defaults.readbackDelayMs;
      const [remaining] = outbox.dueForReadback(32, lease);
      assert.equal(remaining.event_token, token(251));
      assert.equal(outbox.batchForReadback([remaining]).traces.length, 1,
        'the retained sibling can still load its persisted plan');
    } finally {
      if (lease) outbox.releaseDrainLease(lease);
      outbox.close();
    }
  });

  test('a held drain lease keeps a second drain from sending', async () => {
    const path = dbPath();
    const a = NeatlogsOutbox.open({ databasePath: path });
    const b = NeatlogsOutbox.open({ databasePath: path });
    const neatlogs = fakeNeatlogs();
    try {
      a.enqueue([entry(5)]);
      const lease = a.acquireDrainLease('owner-a');
      assert.ok(lease);
      const blocked = await drainNeatlogsOutbox(b, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep, ownerId: 'owner-b' });
      assert.equal(blocked.leased, false);
      assert.equal(neatlogs.calls.length, 0);
      a.releaseDrainLease(lease);
      const sent = await drainNeatlogsOutbox(b, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep, ownerId: 'owner-b' });
      assert.equal(sent.sent, 1);
    } finally {
      a.close();
      b.close();
    }
  });

  test('lease fence blocks same-owner overlap and stale release after expiry', () => {
    const clock = clockAt(10_000);
    const path = dbPath();
    const a = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn, drainLeaseMs: 100 });
    const b = NeatlogsOutbox.open({ databasePath: path, clock: clock.fn, drainLeaseMs: 100 });
    try {
      const first = a.acquireDrainLease('same-owner');
      assert.ok(first);
      assert.equal(a.acquireDrainLease('same-owner'), null, 'active lease cannot be reacquired by the same owner');
      clock.now += 101;
      const next = b.acquireDrainLease('same-owner');
      assert.ok(next);
      assert.ok(next.fence > first.fence);
      assert.equal(a.renewDrainLease(first), false);
      a.releaseDrainLease(first);
      assert.equal(b.renewDrainLease(next), true, 'stale owner cannot release the newer fenced lease');
    } finally {
      a.close();
      b.close();
    }
  });

  test('settled rows are pruned after retention', async () => {
    const clock = clockAt(10_000);
    const outbox = NeatlogsOutbox.open({ databasePath: dbPath(), clock: clock.fn });
    const neatlogs = fakeNeatlogs();
    try {
      outbox.enqueue([entry(6)]);
      await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      clock.now += NEATLOGS_OUTBOX_LIMITS.defaults.readbackDelayMs;
      await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      clock.now += NEATLOGS_OUTBOX_LIMITS.defaults.retentionMs + 1;
      const result = await drainNeatlogsOutbox(outbox, config(), { fetch: neatlogs.fetchImpl, sleep: noSleep });
      assert.equal(result.pruned, 1);
      assert.equal(outbox.diagnostics().counts.verified, 0);
    } finally {
      outbox.close();
    }
  });
});
