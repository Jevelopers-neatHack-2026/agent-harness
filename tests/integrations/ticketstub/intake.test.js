import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { after, before, describe, test } from 'node:test';
import {
  HARNESS_TELEMETRY_KEY_NAME,
  TICKETSTUB_EVENT_SPEC,
  TicketstubIntakeError,
  createTicketstubHarnessExporter,
  createTicketstubTokenizer,
  parseHarnessTelemetryKey,
  ticketstubOutboxEntry,
  validateTicketstubEvent
} from '@jevelopers/agent-harness-runtime/ticketstub';
import { NEATLOGS_DELIVERY_QUEUE_LIMITS as NEATLOGS_OUTBOX_LIMITS } from '../../../src/observability/private/neatlogs-delivery-queue.js';
import {
  FAKE_INGEST_KEY,
  FAKE_PROJECT_ID,
  FAKE_READ_TOKEN,
  fakeNeatlogs
} from '../../observability/neatlogs-delivery-queue/fake-neatlogs.js';

const KEY_HEX = '3c'.repeat(32);
const REQUEST_ID = 'req_SYNTHETICcanary01';
const ACTOR_REF = '0123456789abcdef';
const PROJECT_REF = 'fedcba9876543210';

let scratch;
let counter = 0;
before(() => { scratch = mkdtempSync(join(tmpdir(), 'ticketstub-intake-')); });
after(() => rmSync(scratch, { recursive: true, force: true }));

function event(overrides = {}) {
  return {
    schema: 'ticketstub.event/1',
    type: 'art_ready',
    at: 1_760_000_000_000,
    requestId: REQUEST_ID,
    actor: 'account',
    actorRef: ACTOR_REF,
    projectRef: PROJECT_REF,
    mode: 'live',
    fields: { provider: 'live' },
    ...overrides
  };
}

const SAMPLE_FIELDS = {
  intake_submitted: { source: 'link' },
  intake_rejected: { code: 'unreadable_link' },
  research_started: { source: 'ticketmaster', facts: 4 },
  fact_corrected: { fact: '00112233aabbccdd', versions: 2 },
  art_requested: { first: true },
  art_ready: { provider: 'procedural' },
  art_failed: { code: 'provider_timeout' },
  art_limit: { retryAt: 1_760_000_100_000 },
  version_selected: { version: '00112233aabbccdd', versions: 3 },
  share_created: { share: '00112233aabbccdd' },
  share_revoked: { revoked: true },
  gift_created: { share: '00112233aabbccdd', messageChars: 40 },
  order_placed: { order: '00112233aabbccdd', lines: 1, totalMinor: 2500 },
  order_blocked: { code: 'payment_unavailable' },
  signin_link_sent: { reason: 'save', delivery: 'mail' },
  signed_in: { fresh: false },
  claim_redeemed: { status: 'claimed' },
  storage_unavailable: { code: 'sqlite_busy', route: 'api_art' }
};

function env(overrides = {}) {
  return {
    [HARNESS_TELEMETRY_KEY_NAME]: KEY_HEX,
    NEATLOGS_INGEST_KEY: FAKE_INGEST_KEY,
    NEATLOGS_READ_TOKEN: FAKE_READ_TOKEN,
    NEATLOGS_PROJECT_ID: FAKE_PROJECT_ID,
    ...overrides
  };
}

function dbPath() {
  counter += 1;
  return join(scratch, `outbox-${counter}.sqlite`);
}

function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}

const HELD_REQUEST_TIMEOUT_MS = 5_000;

async function awaitScheduledPostEntry(entered, exporter, {
  timeoutMs = HELD_REQUEST_TIMEOUT_MS,
  onTimersCleared
} = {}) {
  const baseline = exporter.status().counters;
  let timeout;
  let poll;
  const prematureCompletion = new Promise((_, reject) => {
    poll = setInterval(() => {
      try {
        const counters = exporter.status().counters;
        if (counters.drains > baseline.drains || counters.drainErrors > baseline.drainErrors) {
          reject(new Error('scheduled drain completed before its held POST entered'));
        }
      } catch (error) {
        reject(error);
      }
    }, 10);
  });
  const deadline = new Promise((_, reject) => {
    timeout = setTimeout(() => reject(new Error(
      `scheduled POST did not enter within ${timeoutMs}ms`
    )), timeoutMs);
    timeout.ref?.();
  });
  try {
    await Promise.race([entered, prematureCompletion, deadline]);
  } finally {
    if (poll) clearInterval(poll);
    if (timeout) clearTimeout(timeout);
    onTimersCleared?.({ pollCleared: Boolean(poll), deadlineCleared: Boolean(timeout) });
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

async function finishScheduledFixture({
  release,
  exporter,
  closing,
  closingObserved,
  markClosingObserved,
  primaryError,
  hasPrimaryError,
  label,
  onLifecycle
}) {
  const cleanupErrors = [];
  release?.resolve();
  if (release) onLifecycle?.('released');
  try {
    let closePromise = closing;
    if (!closePromise) closePromise = exporter.close();
    if (!closingObserved) {
      await closePromise;
      markClosingObserved?.();
    }
  } catch (error) {
    cleanupErrors.push(error);
  }
  onLifecycle?.('exporter-close-settled');
  throwTestAndCleanupError(primaryError, hasPrimaryError, cleanupErrors, label);
}

const plainRow = row => row === undefined ? undefined : { ...row };

describe('ticketstub.event/1 contract', () => {
  test('accepts every event type with its own fields', () => {
    assert.deepEqual(Object.keys(SAMPLE_FIELDS).sort(), Object.keys(TICKETSTUB_EVENT_SPEC).sort());
    for (const [type, fields] of Object.entries(SAMPLE_FIELDS)) {
      assert.equal(validateTicketstubEvent(event({ type, fields })).type, type);
    }
    assert.equal(validateTicketstubEvent(event({ at: 1 })).at, 1,
      'the first positive source millisecond remains accepted');
    assert.equal(validateTicketstubEvent(event({ at: 100_000_000_000_000 })).at, 100_000_000_000_000,
      'large safe source timestamps inside the original domain remain accepted');
    assert.equal(validateTicketstubEvent(event({ at: 8.64e15 - 1 })).at, 8.64e15 - 1,
      'the last safe integer below the original upper bound remains accepted');
    const anonymous = event({ actor: 'anonymous', actorRef: undefined, projectRef: undefined });
    delete anonymous.actorRef;
    delete anonymous.projectRef;
    assert.equal(validateTicketstubEvent(anonymous).actor, 'anonymous');
  });

  test('refuses anything outside the contract instead of repairing it', () => {
    const cases = [
      [event({ schema: 'ticketstub.event/2' }), 'UNSUPPORTED_SCHEMA'],
      [event({ type: 'page_viewed' }), 'INVALID_EVENT'],
      [event({ fields: { provider: 'live', prompt: 'hello' } }), 'INVALID_EVENT'],
      [event({ fields: { provider: 'dall-e' } }), 'INVALID_EVENT'],
      [event({ type: 'art_failed', fields: { code: 'person@example.invalid' } }), 'INVALID_EVENT'],
      [event({ type: 'art_failed', fields: { code: 'a'.repeat(30) } }), 'INVALID_EVENT'],
      [event({ type: 'order_placed', fields: { order: 'ORDER-123', lines: 1, totalMinor: 1 } }), 'INVALID_EVENT'],
      [event({ type: 'order_placed', fields: { order: '00112233aabbccdd', lines: -1, totalMinor: 1 } }), 'INVALID_EVENT'],
      [event({ actor: 'anonymous' }), 'INVALID_EVENT'],
      [event({ actorRef: 'not-a-ref' }), 'INVALID_EVENT'],
      [event({ requestId: 'https://x.invalid/?a=b' }), 'INVALID_EVENT'],
      [event({ mode: 'demo' }), 'INVALID_EVENT'],
      [event({ email: 'person@example.invalid' }), 'INVALID_EVENT'],
      [event({ at: 0 }), 'INVALID_EVENT'],
      [event({ at: -1 }), 'INVALID_EVENT'],
      [event({ at: 8.64e15 }), 'INVALID_EVENT'],
      [[], 'INVALID_EVENT']
    ];
    for (const [value, code] of cases) {
      assert.throws(() => validateTicketstubEvent(value), error => error instanceof TicketstubIntakeError && error.code === code);
    }
  });

  test('snapshots only enumerable own data and rejects getters, hidden keys, symbols, and proxies', async () => {
    let getterCalls = 0;
    const getter = event();
    Object.defineProperty(getter, 'requestId', { enumerable: true, get() { getterCalls += 1; return REQUEST_ID; } });
    assert.throws(() => validateTicketstubEvent(getter), { code: 'INVALID_EVENT' });
    assert.equal(getterCalls, 0);

    const nestedGetter = event({ type: 'art_failed', fields: {} });
    Object.defineProperty(nestedGetter.fields, 'code', {
      enumerable: true,
      get() {
        getterCalls += 1;
        return getterCalls === 1 ? 'provider_timeout' : 'person@example.invalid';
      }
    });
    assert.throws(() => validateTicketstubEvent(nestedGetter), { code: 'INVALID_EVENT' });
    assert.equal(getterCalls, 0, 'the nested field descriptor is rejected without invoking a changing getter');

    const fieldsAccessor = event();
    Object.defineProperty(fieldsAccessor, 'fields', {
      enumerable: true,
      get() { getterCalls += 1; return { provider: 'live' }; }
    });
    assert.throws(() => validateTicketstubEvent(fieldsAccessor), { code: 'INVALID_EVENT' });
    assert.equal(getterCalls, 0, 'the top-level fields accessor is not evaluated');

    const hidden = event();
    Object.defineProperty(hidden, 'private', { value: 'secret', enumerable: false });
    assert.throws(() => validateTicketstubEvent(hidden), { code: 'INVALID_EVENT' });
    const symbolic = event();
    symbolic[Symbol('extra')] = 'secret';
    assert.throws(() => validateTicketstubEvent(symbolic), { code: 'INVALID_EVENT' });
    assert.throws(() => validateTicketstubEvent(new Proxy(event(), {})), { code: 'INVALID_EVENT' });
    assert.throws(() => validateTicketstubEvent(event({ fields: new Proxy({ provider: 'live' }, {}) })), { code: 'INVALID_EVENT' });

    const neatlogs = fakeNeatlogs();
    const exporter = createTicketstubHarnessExporter({
      env: env(), databasePath: dbPath(), fetch: neatlogs.fetchImpl, drainDelayMs: 60_000
    });
    try {
      const result = exporter.accept([nestedGetter]);
      assert.deepEqual({ ...result }, { accepted: 0, duplicates: 0, refused: 0, conflicts: 0, invalid: 1 });
      assert.equal(exporter.status().outbox.counts.queued, 0);
      assert.equal(neatlogs.calls.length, 0, 'invalid accessor data produces no queue row or outbound request');
    } finally {
      await exporter.close();
    }
  });

  test('rejects nonprimitive event types before coercion and accepts null-prototype records', () => {
    let coercions = 0;
    const coercibleType = {
      [Symbol.toPrimitive]() { coercions += 1; return 'art_ready'; },
      toString() { coercions += 1; return 'art_ready'; }
    };
    assert.throws(() => validateTicketstubEvent(event({ type: coercibleType })), { code: 'INVALID_EVENT' });
    assert.equal(coercions, 0, 'type validation must not run caller coercion hooks');

    const proxyType = new Proxy({}, {
      get() { coercions += 1; return 'art_ready'; },
      getOwnPropertyDescriptor() { coercions += 1; return undefined; }
    });
    assert.throws(() => validateTicketstubEvent(event({ type: proxyType })), { code: 'INVALID_EVENT' });
    assert.equal(coercions, 0, 'type validation must not inspect proxy hooks');

    const nullPrototypeEvent = Object.assign(Object.create(null), event({ fields: { provider: 'procedural' } }));
    nullPrototypeEvent.fields = Object.assign(Object.create(null), { provider: 'procedural' });
    assert.equal(validateTicketstubEvent(nullPrototypeEvent).type, 'art_ready');
  });
});

describe('tokenization', () => {
  test('the key must be 32 to 64 bytes of hex', () => {
    for (const bad of [undefined, '', 'zz'.repeat(32), '3c'.repeat(31), '3c'.repeat(65)]) {
      assert.throws(() => parseHarnessTelemetryKey(bad), { code: 'INVALID_TELEMETRY_KEY' });
    }
    assert.equal(parseHarnessTelemetryKey(` ${KEY_HEX} `).length, 32);
  });

  test('exported entries carry tokens, never the request ID or the producer refs', () => {
    const tokenize = createTicketstubTokenizer(parseHarnessTelemetryKey(KEY_HEX));
    const entry = ticketstubOutboxEntry(event(), tokenize);
    const text = JSON.stringify(entry);
    for (const raw of [REQUEST_ID, ACTOR_REF, PROJECT_REF]) assert.equal(text.includes(raw), false);
    const input = JSON.parse(entry.input);
    assert.equal(input.schema, 'agent-harness.ticketstub-intake.v2');
    assert.equal(input.type, 'art_ready');
    assert.deepEqual(input.fields, { provider: 'live' });
    assert.match(input.requestToken, /^tok1_[0-9a-f]{64}$/);
    assert.equal(entry.sessionId, input.projectToken, 'a project groups its events');

    // Stable under one key, unlinkable under another.
    assert.equal(ticketstubOutboxEntry(event(), tokenize).eventToken, entry.eventToken);
    const other = createTicketstubTokenizer(parseHarnessTelemetryKey('7d'.repeat(32)));
    assert.notEqual(ticketstubOutboxEntry(event(), other).eventToken, entry.eventToken);
  });

  test('domain-tokenizes every product reference field under the injected key', () => {
    const tokenize = createTicketstubTokenizer(parseHarnessTelemetryKey(KEY_HEX));
    const refEvents = [
      ['fact_corrected', { fact: ACTOR_REF, versions: 1 }, 'fact'],
      ['version_selected', { version: ACTOR_REF, versions: 1 }, 'version'],
      ['share_created', { share: ACTOR_REF }, 'share'],
      ['gift_created', { share: ACTOR_REF, messageChars: 4 }, 'share'],
      ['order_placed', { order: ACTOR_REF, lines: 1, totalMinor: 1 }, 'order']
    ];
    const tokens = [];
    for (const [type, fields, domain] of refEvents) {
      const input = JSON.parse(ticketstubOutboxEntry(event({ type, fields }), tokenize).input);
      assert.match(input.fields[Object.keys(fields)[0]], /^tok1_[0-9a-f]{64}$/);
      assert.equal(JSON.stringify(input).includes(ACTOR_REF), false);
      tokens.push([domain, input.fields[Object.keys(fields)[0]]]);
    }
    assert.equal(tokens[2][1], tokens[3][1], 'a share reference stays joinable within the share domain');
    assert.notEqual(tokens[0][1], tokens[1][1], 'equal source strings have separate fact and version domains');
    const otherKey = createTicketstubTokenizer(parseHarnessTelemetryKey('7d'.repeat(32)));
    const other = JSON.parse(ticketstubOutboxEntry(event({ type: 'order_placed', fields: { order: ACTOR_REF, lines: 1, totalMinor: 1 } }), otherKey).input);
    assert.notEqual(other.fields.order, tokens[4][1]);
  });

  test('without a project, the request groups the events', () => {
    const tokenize = createTicketstubTokenizer(parseHarnessTelemetryKey(KEY_HEX));
    const loose = event();
    delete loose.projectRef;
    const entry = ticketstubOutboxEntry(loose, tokenize);
    assert.equal(entry.sessionId, JSON.parse(entry.input).requestToken);
  });
});

describe('host exporter', () => {
  test('creation refuses missing configuration with a code and no value', () => {
    assert.throws(() => createTicketstubHarnessExporter({ env: env({ [HARNESS_TELEMETRY_KEY_NAME]: '' }), databasePath: dbPath() }),
      { code: 'INVALID_TELEMETRY_KEY' });
    const noIngest = env();
    delete noIngest.NEATLOGS_INGEST_KEY;
    assert.throws(() => createTicketstubHarnessExporter({ env: noIngest, databasePath: dbPath() }), { code: 'MISSING_INGEST_KEY' });
    try {
      createTicketstubHarnessExporter({ env: env({ NEATLOGS_READ_TOKEN: 'bad value with spaces' }), databasePath: dbPath() });
      assert.fail('expected a configuration error');
    } catch (error) {
      assert.equal(error.code, 'INVALID_CREDENTIAL');
      assert.equal(String(error.message).includes('bad value'), false);
    }
  });

  test('accepts, delivers and verifies one request of real-shaped events end to end', async () => {
    const clock = { now: 10_000 };
    const neatlogs = fakeNeatlogs();
    const exporter = createTicketstubHarnessExporter({
      env: env(), databasePath: dbPath(), fetch: neatlogs.fetchImpl, clock: () => clock.now,
      tags: ['ticketstub@abcdef1'], drainDelayMs: 60_000
    });
    try {
      const events = [
        event({ type: 'art_requested', fields: { first: true }, at: 100_000_000_000_000 }),
        event({ type: 'art_ready', fields: { provider: 'live' }, at: 8.64e15 - 1 }),
        event({ type: 'art_ready', fields: { provider: 'live', prompt: 'leak' } })
      ];
      const accepted = exporter.accept(events);
      assert.deepEqual({ ...accepted }, { accepted: 2, duplicates: 0, refused: 0, conflicts: 0, invalid: 1 });
      assert.deepEqual({ ...exporter.accept(events.slice(0, 2)) }, { accepted: 0, duplicates: 2, refused: 0, conflicts: 0, invalid: 0 });

      const first = await exporter.drain();
      assert.equal(first.sent, 2);
      clock.now += NEATLOGS_OUTBOX_LIMITS.defaults.readbackDelayMs;
      const second = await exporter.drain();
      assert.equal(second.verified, 2);

      const status = exporter.status();
      assert.equal(status.outbox.counts.verified, 2);
      assert.equal(status.counters.invalid, 1);
      const traces = [...neatlogs.stored.values()];
      assert.equal(traces.length, 2);
      const largeEventTrace = traces.find(trace => JSON.parse(trace.input).type === 'art_requested');
      assert.equal(JSON.parse(largeEventTrace.input).at, 100_000_000_000_000,
        'the accepted source event time stays in the immutable exported input');
      assert.equal(largeEventTrace.startTimeUnixNano, '10000000000',
        'wire span time uses the normal 10,000ms send clock, not the source event time');
      const upperBoundTrace = traces.find(trace => JSON.parse(trace.input).type === 'art_ready');
      assert.equal(JSON.parse(upperBoundTrace.input).at, 8.64e15 - 1,
        'the last accepted event millisecond remains in the exported input');
      assert.equal(upperBoundTrace.startTimeUnixNano, '10000000000',
        'the accepted event-time boundary also uses the normal send clock on the wire');
      for (const trace of traces) {
        assert.equal(trace.workflowName, 'agent-harness.ticketstub-events');
        assert.deepEqual(trace.tags, ['ticketstub', 'ticketstub@abcdef1']);
        assert.equal(trace.attributes['agent_harness.source'], 'ticketstub');
      }
      assert.equal(new Set(traces.map(trace => trace.sessionId)).size, 1, 'one project, one session');
      const wire = Buffer.concat(neatlogs.bodies).toString('latin1');
      for (const raw of [REQUEST_ID, ACTOR_REF, PROJECT_REF, 'leak', FAKE_INGEST_KEY, KEY_HEX]) {
        assert.equal(wire.includes(raw), false, `wire body must not contain ${raw.slice(0, 8)}…`);
      }
      assert.equal(JSON.stringify(status).includes(KEY_HEX), false);
      assert.equal(JSON.stringify(exporter.summary).includes(FAKE_INGEST_KEY), false);
    } finally {
      await exporter.close();
    }
    assert.throws(() => exporter.accept([event()]), { code: 'CLOSED' });
  });

  test('a scheduled drain runs on its own and never throws into the host', async () => {
    const neatlogs = fakeNeatlogs({ ingestStatus: () => 500 });
    const databasePath = dbPath();
    const exporter = createTicketstubHarnessExporter({
      env: env(), databasePath, fetch: neatlogs.fetchImpl, drainDelayMs: 0
    });
    try {
      exporter.accept([event()]);
      for (let i = 0; i < 250 && exporter.status().counters.drains === 0; i += 1) {
        await new Promise(resolve => setTimeout(resolve, 20));
      }
      const status = exporter.status();
      assert.equal(status.counters.drains, 1);
      assert.equal(status.lastDrain.sendStatus, 'outcome_unknown');
      assert.equal(status.outbox.counts.queued, 0);
      assert.equal(status.outbox.counts.sent, 1, 'the committed POST intent remains unknown, not queued');
      assert.equal(status.outbox.latest[0].lastStatus, 'outcome_unknown');
      assert.equal(neatlogs.calls.filter(call => call.path === '/v1/traces').length, 1,
        'the scheduled failure does not trigger a blind second POST');
    } finally {
      await exporter.close();
    }
  });

  test('close waits for a held scheduled POST before closing its queue', async () => {
    const entered = deferred();
    const release = deferred();
    const clock = { now: 1_760_000_000_000 };
    const neatlogs = fakeNeatlogs({ beforeFetch: async call => {
      if (call.path === '/v1/traces') {
        entered.resolve();
        await release.promise;
      }
    } });
    const settings = env();
    delete settings.NEATLOGS_READ_TOKEN;
    delete settings.NEATLOGS_PROJECT_ID;
    const exporter = createTicketstubHarnessExporter({
      env: settings, databasePath: dbPath(), fetch: neatlogs.fetchImpl, clock: () => clock.now, drainDelayMs: 0
    });
    let closing;
    let closingObserved = false;
    let closeFinished = false;
    let primaryError;
    let hasPrimaryError = false;
    let waitCleanup;
    try {
      exporter.accept([event()]);
      await awaitScheduledPostEntry(entered.promise, exporter, {
        onTimersCleared: state => { waitCleanup = state; }
      });
      assert.deepEqual(waitCleanup, { pollCleared: true, deadlineCleared: true },
        'successful scheduled entry clears both its status poll and referenced deadline');
      closing = exporter.close().then(() => { closeFinished = true; });
      await Promise.resolve();
      assert.equal(closeFinished, false, 'close waits while the scheduled POST is still in flight');
      release.resolve();
      try { await closing; } finally { closingObserved = true; }
      assert.equal(neatlogs.calls.filter(call => call.path === '/v1/traces').length, 1,
        'closing after a held POST does not start a follow-up request');
    } catch (error) {
      primaryError = error;
      hasPrimaryError = true;
    } finally {
      await finishScheduledFixture({
        release, exporter, closing, closingObserved,
        markClosingObserved: () => { closingObserved = true; },
        primaryError, hasPrimaryError,
        label: 'scheduled held-POST assertion and cleanup failed'
      });
    }
  });

  test('scheduled entry timeout closes an exporter whose producer callback never starts', async () => {
    const entered = deferred();
    const neatlogs = fakeNeatlogs();
    const exporter = createTicketstubHarnessExporter({
      env: env(), databasePath: dbPath(), fetch: neatlogs.fetchImpl, drainDelayMs: 60_000
    });
    let primaryError;
    let hasPrimaryError = false;
    let waitFailure;
    let waitCleanup;
    const lifecycle = [];
    try {
      const waiting = awaitScheduledPostEntry(entered.promise, exporter, {
        timeoutMs: 100,
        onTimersCleared: state => { waitCleanup = state; }
      });
      exporter.accept([event()]);
      try { await waiting; } catch (error) { waitFailure = error; }
      assert.match(waitFailure?.message ?? '', /scheduled POST did not enter within 100ms/);
      assert.deepEqual(waitCleanup, { pollCleared: true, deadlineCleared: true },
        'timeout clears the status poll and referenced deadline');
      assert.equal(neatlogs.calls.length, 0, 'the long-delay producer callback never starts a request');
    } catch (error) {
      primaryError = error;
      hasPrimaryError = true;
    } finally {
      await finishScheduledFixture({
        exporter, primaryError, hasPrimaryError,
        label: 'never-started scheduled producer timeout cleanup failed',
        onLifecycle: event => lifecycle.push(event)
      });
    }
    assert.deepEqual(lifecycle, ['exporter-close-settled']);
    assert.equal(neatlogs.calls.length, 0, 'close cancels the not-yet-started scheduled producer');
  });

  test('scheduled entry wait rejects when the real producer finishes before signaling entry', async () => {
    const entered = deferred();
    const neatlogs = fakeNeatlogs();
    const exporter = createTicketstubHarnessExporter({
      env: env(), databasePath: dbPath(), fetch: neatlogs.fetchImpl, drainDelayMs: 0
    });
    let primaryError;
    let hasPrimaryError = false;
    let waitFailure;
    let waitCleanup;
    try {
      const waiting = awaitScheduledPostEntry(entered.promise, exporter, {
        timeoutMs: 1_000,
        onTimersCleared: state => { waitCleanup = state; }
      });
      exporter.accept([event()]);
      try { await waiting; } catch (error) { waitFailure = error; }
      assert.match(waitFailure?.message ?? '', /scheduled drain completed before its held POST entered/);
      assert.deepEqual(waitCleanup, { pollCleared: true, deadlineCleared: true },
        'early completion clears the status poll and referenced deadline');
      assert.equal(neatlogs.calls.filter(call => call.path === '/v1/traces').length, 1,
        'the real scheduled producer completes exactly one POST before the absent entry signal');
    } catch (error) {
      primaryError = error;
      hasPrimaryError = true;
    } finally {
      await finishScheduledFixture({
        exporter, primaryError, hasPrimaryError,
        label: 'early scheduled producer completion cleanup failed'
      });
    }
    assert.equal(neatlogs.calls.filter(call => call.path === '/v1/traces').length, 1,
      'closing after early completion starts no second POST');
  });

  test('reopening the exporter schedules an existing due row without a host replay', async () => {
    const clock = { now: 10_000 };
    const path = dbPath();
    const neatlogs = fakeNeatlogs({ ingestStatus: count => count === 1 ? 429 : 200 });
    const options = {
      env: env(), databasePath: path, fetch: neatlogs.fetchImpl, clock: () => clock.now,
      drainDelayMs: 60_000, outboxOptions: { retryBaseMs: 0, retryMaxMs: 0, maxBatchesPerDrain: 1 }
    };
    const first = createTicketstubHarnessExporter(options);
    try {
      first.accept([event()]);
      assert.equal((await first.drain()).sendStatus, 'not_accepted');
      const raw = new DatabaseSync(path, { readOnly: true });
      let row;
      let attempt;
      let body;
      try {
        row = plainRow(raw.prepare('SELECT state, send_attempts, last_status, next_attempt_at_ms FROM neatlogs_delivery_queue').get());
        attempt = plainRow(raw.prepare('SELECT ordinal, status, retry_after_minimum_ms FROM neatlogs_delivery_attempts').get());
        body = raw.prepare('SELECT body FROM neatlogs_delivery_batches').get().body;
      } finally {
        raw.close();
      }
      assert.deepEqual(row, { state: 'queued', send_attempts: 1, last_status: 'not_accepted', next_attempt_at_ms: clock.now });
      assert.deepEqual(attempt, { ordinal: 1, status: 'not_accepted', retry_after_minimum_ms: null });
      assert.deepEqual(Buffer.from(body), neatlogs.bodies[0], 'the first rejected attempt and exact body are durable before close');
    } finally {
      await first.close();
    }
    const reopened = createTicketstubHarnessExporter({ ...options, drainDelayMs: 0 });
    try {
      for (let index = 0; index < 250 && reopened.status().counters.drains === 0; index += 1) {
        await new Promise(resolve => setTimeout(resolve, 20));
      }
      assert.equal(reopened.status().counters.drains, 1);
      assert.equal(neatlogs.calls.filter(call => call.path === '/v1/traces').length, 2,
        'startup resumes durable due work without requiring the producer to resend the event');
      assert.deepEqual(neatlogs.bodies[1], neatlogs.bodies[0], 'reopen retries the durable body without producer replay');
    } finally {
      await reopened.close();
    }
  });
});
