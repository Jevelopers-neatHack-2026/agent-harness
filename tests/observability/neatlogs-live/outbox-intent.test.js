import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import net from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import tls from 'node:tls';
import { fileURLToPath } from 'node:url';
import { after, before, test } from 'node:test';
import { loadNeatlogsConfig } from '../../../src/observability/private/neatlogs-config.js';
import {
  createNeatlogsOutboxSink, drainNeatlogsOutbox, iterateNeatlogsOutboxSnapshot,
  listNeatlogsOutbox, readbackPlanFromRecords, runtimeNeatlogsTags
} from '../../../src/observability/private/neatlogs-outbox.js';

const INGEST_KEY = 'synthetic-ingest-key-0123456789abcdef';
const READ_TOKEN = 'synthetic-readback-token-0123456789abcdef';
const PROJECT_ID = '00000000-0000-4000-8000-000000000001';
const TAGS = runtimeNeatlogsTags({ revision: 'b'.repeat(40), harnessMode: 'mock' });
const CONFIG = loadNeatlogsConfig({ env: {
  NEATLOGS_INGEST_KEY: INGEST_KEY, NEATLOGS_READ_TOKEN: READ_TOKEN, NEATLOGS_PROJECT_ID: PROJECT_ID
} });
let scratch;
let realNetworkAttempts = 0;
let originalFetch;
let originalConnect;
let originalTls;
before(() => {
  scratch = mkdtempSync(join(tmpdir(), 'neatlogs-immutable-intent-'));
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
function batchDirectory(name, count = 1) {
  const directory = join(scratch, name);
  createNeatlogsOutboxSink({ directory })(Array.from({ length: count }, (_, index) => record(index + 1)));
  return directory;
}
function pathFor(directory) { return listNeatlogsOutbox(directory)[0]; }
function disk(directory) { return JSON.parse(readFileSync(pathFor(directory), 'utf8')); }
function replace(directory, value) { writeFileSync(pathFor(directory), JSON.stringify(value), { mode: 0o600 }); }
function options(directory, fake, extra = {}) {
  return { directory, config: CONFIG, tags: TAGS, fetch: fake.fetch,
    sendOptions: { maxAttempts: 1 }, readbackOptions: { maxPolls: 2, pollIntervalMs: 0 }, ...extra };
}

function service(directory, { status = 200, partial = false, tags = TAGS, visible = () => [],
  finalizationStatus = 'finalized', onPost } = {}) {
  const posts = [];
  const reads = [];
  const records = disk(directory).records;
  const ok = data => new Response(JSON.stringify({ success: true, requestId: 'req_fake', data }), {
    status: 200, headers: { 'content-type': 'application/json' }
  });
  return { posts, reads, fetch: async (url, init) => {
    const pathname = new URL(url).pathname;
    if (pathname === '/v1/traces') {
      const body = Buffer.from(init.body);
      posts.push(body);
      onPost?.({ body, batch: disk(directory) });
      return new Response(partial ? '{"partialSuccess":{"rejectedSpans":"1"}}' : null, {
        status, ...(partial ? { headers: { 'content-type': 'application/json' } } : {})
      });
    }
    reads.push(pathname);
    if (pathname.endsWith('/project')) return ok({ id: PROJECT_ID });
    const match = /\/traces\/([0-9a-f]{32})(\/spans(?:\/([0-9a-f]{16})\/payload)?)?$/.exec(pathname);
    const traces = records.flatMap(source => visible(source.sequence).map(generation =>
      readbackPlanFromRecords([source], tags, { [source.sequence]: generation }).traces[0]));
    const trace = traces.find(candidate => candidate.traceId === match?.[1]);
    if (!trace) return new Response('{}', { status: 404, headers: { 'content-type': 'application/problem+json' } });
    if (!match[2]) return ok({ traceId: trace.traceId, sessionId: trace.sessionId,
      workflowName: 'agent-harness.workflow-events', traceName: 'agent_harness.workflow_event',
      tags: [...tags], spansCount: 1, finalizationStatus });
    if (!match[3]) return ok({ spans: [{ traceId: trace.traceId, spanId: trace.rootSpanId,
      parentSpanId: null, spanName: 'agent_harness.workflow_event', spanType: 'WORKFLOW' }],
    page: { hasMore: false, limit: 50, nextCursor: null } });
    return ok({ traceId: trace.traceId, spanId: trace.rootSpanId,
      fields: { input: { available: true, truncated: false, mimeType: 'application/json', value: trace.input } } });
  } };
}

function child(directory, mode, failAt = 0) {
  const receiptPath = join(scratch, `${mode}-${failAt}-${disk(directory).firstSequence}-${Date.now()}.json`);
  const fixture = fileURLToPath(new URL('./fixtures/outbox-intent-child.mjs', import.meta.url));
  const result = spawnSync(process.execPath, [fixture, directory, mode, String(failAt), receiptPath], {
    encoding: 'utf8', timeout: 15_000, maxBuffer: 64 * 1024,
    env: { PATH: process.env.PATH, TMPDIR: tmpdir(), NODE_NO_WARNINGS: '1' }
  });
  assert.equal(result.error, undefined);
  assert.equal(result.signal, null);
  const exited = mode === 'crash' || mode === 'exit-before-post';
  assert.equal(result.status, exited ? 71 : 0, result.stderr);
  if (exited) {
    const summary = JSON.parse(readFileSync(receiptPath, 'utf8'));
    assert.equal(summary.realNetworkAttempts, 0);
    return summary;
  }
  const summary = JSON.parse(result.stdout);
  assert.equal(summary.realNetworkAttempts, 0);
  return summary;
}

function cliSnapshot(directory) {
  const evidencePath = join(scratch, `summary-cli-${Date.now()}.json`);
  const networkReceiptPath = `${evidencePath}.network.json`;
  const tool = fileURLToPath(new URL('../../../tools/neatlogs-runtime-trace.mjs', import.meta.url));
  const guard = `
    import { writeFileSync } from 'node:fs';
    import net from 'node:net';
    import tls from 'node:tls';
    let attempts = 0;
    const deny = () => { attempts += 1; throw new Error('real network forbidden'); };
    globalThis.fetch = deny;
    net.Socket.prototype.connect = deny;
    tls.connect = deny;
    process.on('exit', () => writeFileSync(${JSON.stringify(networkReceiptPath)}, JSON.stringify({ attempts })));
  `;
  const result = spawnSync(process.execPath, ['--import', `data:text/javascript,${encodeURIComponent(guard)}`,
    tool, '--mode', 'drain', '--outbox', directory, '--evidence', evidencePath,
    '--component-revision', 'b'.repeat(40)], {
    encoding: 'utf8', timeout: 10_000, maxBuffer: 64 * 1024,
    env: { PATH: process.env.PATH, TMPDIR: tmpdir(), NODE_NO_WARNINGS: '1',
      NEATLOGS_INGEST_KEY: INGEST_KEY, NEATLOGS_READ_TOKEN: READ_TOKEN, NEATLOGS_PROJECT_ID: PROJECT_ID }
  });
  assert.equal(result.error, undefined);
  assert.equal(result.signal, null);
  assert.equal(result.status, 0, result.stderr);
  assert.equal(JSON.parse(readFileSync(networkReceiptPath, 'utf8')).attempts, 0);
  const evidence = JSON.parse(readFileSync(evidencePath, 'utf8'));
  assert.equal(evidence.batchEvidenceSource, 'persisted_outbox_snapshot');
  assert.equal(evidence.batches.length, 1);
  return evidence.batches[0];
}

test('the first HTTP boundary observes a validated complete intent and durable start marker', async () => {
  const directory = batchDirectory('initial-intent');
  let observed;
  const fake = service(directory, { onPost: value => { observed = value; } });
  await drainNeatlogsOutbox(options(directory, fake));
  assert.equal(observed.batch.schema, 'agent-harness.neatlogs.outbox-batch.v2');
  const intent = observed.batch.dispatches[0];
  assert.equal(intent.purpose, 'initial');
  assert.equal(intent.calls[0].postsStarted, 1);
  assert.equal(intent.calls[0].result, null);
  assert.deepEqual(Buffer.from(intent.bodyBase64, 'base64'), observed.body);
  assert.equal(intent.bodySha256, createHash('sha256').update(observed.body).digest('hex'));
  assert.equal(observed.batch.plan.traces[0].generation, 0);
  assert.ok(Number.isInteger(intent.traces[0].root.start));
  const stored = readFileSync(pathFor(directory), 'utf8');
  for (const value of [INGEST_KEY, READ_TOKEN, PROJECT_ID]) assert.ok(!stored.includes(value));
});

test('unknown retries restore exact bytes, tags and timestamps without another SDK capture', async () => {
  const directory = batchDirectory('same-bytes');
  const first = service(directory, { status: 503 });
  await drainNeatlogsOutbox(options(directory, first));
  const original = disk(directory).dispatches[0];
  const second = service(directory, { status: 503 });
  await drainNeatlogsOutbox(options(directory, second, { captureTimeoutMs: 1 }));
  assert.deepEqual(second.posts[0], first.posts[0]);
  assert.deepEqual(disk(directory).dispatches[0].traces, original.traces);
  assert.equal(disk(directory).dispatches.length, 1);
  assert.equal(disk(directory).dispatches[0].calls.length, 2);
});

test('changed tags or a different pinned destination refuse before transport and preserve the intent', async () => {
  const directory = batchDirectory('conflicting-config');
  const first = service(directory, { status: 503 });
  await drainNeatlogsOutbox(options(directory, first));
  const before = readFileSync(pathFor(directory), 'utf8');
  const fake = service(directory);
  const tags = runtimeNeatlogsTags({ revision: 'c'.repeat(40), harnessMode: 'mock' });
  await assert.rejects(drainNeatlogsOutbox(options(directory, fake, { tags })), { code: 'DISPATCH_TAG_CONFLICT' });
  const elsewhere = loadNeatlogsConfig({ env: { NEATLOGS_INGEST_KEY: INGEST_KEY,
    NEATLOGS_READ_TOKEN: READ_TOKEN, NEATLOGS_PROJECT_ID: PROJECT_ID,
    NEATLOGS_ENDPOINT: 'https://eu.ingest.neatlogs.com', NEATLOGS_HOST: 'https://eu.app.neatlogs.com' } });
  await assert.rejects(drainNeatlogsOutbox(options(directory, fake, { config: elsewhere })), { code: 'DISPATCH_TARGET_CONFLICT' });
  const otherProject = loadNeatlogsConfig({ env: { NEATLOGS_INGEST_KEY: INGEST_KEY,
    NEATLOGS_READ_TOKEN: READ_TOKEN, NEATLOGS_PROJECT_ID: '00000000-0000-4000-8000-000000000002' } });
  await assert.rejects(drainNeatlogsOutbox(options(directory, fake, { config: otherProject })), { code: 'DISPATCH_TARGET_CONFLICT' });
  assert.equal(fake.posts.length, 0);
  assert.equal(fake.reads.length, 0);
  assert.equal(readFileSync(pathFor(directory), 'utf8'), before);
});

test('a later 403 or partial refusal cannot erase an earlier ambiguous delivery', async () => {
  for (const partial of [false, true]) {
    const directory = batchDirectory(`ambiguity-${partial}`);
    await drainNeatlogsOutbox(options(directory, service(directory, { status: 503 })));
    const refused = service(directory, { status: partial ? 200 : 403, partial });
    const [result] = await drainNeatlogsOutbox(options(directory, refused));
    assert.equal(result.state, 'pending_send');
    assert.equal(result.batch.send.status, 'outcome_unknown');
    assert.equal(result.batch.send.reasonCode, 'PRIOR_ATTEMPT_OUTCOME_UNKNOWN');
    assert.deepEqual(result.batch.dispatches[0].calls.map(call => call.result.status),
      ['outcome_unknown', partial ? 'partially_rejected' : 'rejected']);
    assert.equal(result.batch.send.lastResult.status, partial ? 'partially_rejected' : 'rejected');
  }
});

test('an initial definitive refusal stays distinct from an unknown earlier attempt', async () => {
  const directory = batchDirectory('definitive-refusal');
  const refused = service(directory, { status: 403 });
  const [result] = await drainNeatlogsOutbox(options(directory, refused, { readback: true }));
  assert.equal(result.state, 'rejected');
  assert.equal(result.batch.send.priorOutcomeUnknown, false);
  assert.equal(refused.reads.length, 0);
});

test('recovery selects its subset and generation before I/O and retries the same unknown intent', async () => {
  const directory = batchDirectory('recovery-intent', 3);
  const visible = sequence => sequence === 1 ? [0] : [];
  await drainNeatlogsOutbox(options(directory, service(directory, { visible }), { readback: true }));
  const before = disk(directory);
  assert.equal(before.state, 'missing');
  let firstBoundary;
  const unknown = service(directory, { status: 503, onPost: observation => { firstBoundary = observation; } });
  await drainNeatlogsOutbox(options(directory, unknown, { recover: true }));
  assert.equal(firstBoundary.batch.dispatches.length, 2);
  assert.equal(firstBoundary.batch.dispatches[1].calls[0].postsStarted, 1);
  assert.deepEqual(firstBoundary.batch.dispatches[1].traces.map(trace => trace.sequence), [2, 3]);
  assert.deepEqual(firstBoundary.batch.plan.traces.map(trace => trace.generation), [0, 1, 1]);
  assert.equal(firstBoundary.batch.recovery.send.status, 'outcome_unknown');
  assert.equal(firstBoundary.batch.recovery.send.deliveryConfirmed, false);
  assert.equal(disk(directory).state, 'pending_send');
  const retry = service(directory, { visible: sequence => sequence === 1 ? [0] : [1] });
  const [result] = await drainNeatlogsOutbox(options(directory, retry, { readback: true, captureTimeoutMs: 1 }));
  assert.deepEqual(retry.posts[0], unknown.posts[0]);
  assert.equal(result.state, 'verified');
  assert.equal(result.batch.dispatches.length, 2);
  assert.equal(result.batch.recoveryHistory[0].readback.status, 'missing');
  assert.equal(result.batch.recovery.duplicateDeliveryRisk, 'PRIOR_GENERATION_MAY_APPEAR_LATER');
});

test('unknown send plus missing readback retains its intent until explicit recovery or later visibility', async () => {
  const directory = batchDirectory('unknown-and-missing');
  const initial = service(directory, { status: 503 });
  await drainNeatlogsOutbox(options(directory, initial, { send: true, readback: true }));
  assert.equal(disk(directory).state, 'missing');
  assert.equal(disk(directory).send.status, 'outcome_unknown');
  const first = structuredClone(disk(directory).dispatches[0]);
  const ordinary = service(directory);
  await drainNeatlogsOutbox(options(directory, ordinary, { send: true, readback: true }));
  assert.equal(ordinary.posts.length, 0);
  assert.deepEqual(disk(directory).dispatches[0], first);

  const recovery = service(directory, { status: 503 });
  await drainNeatlogsOutbox(options(directory, recovery, { send: true, recover: true, readback: true }));
  const recovered = disk(directory);
  assert.equal(recovered.state, 'missing');
  assert.equal(recovered.send.status, 'outcome_unknown');
  assert.equal(recovered.plan.traces[0].generation, 1);
  assert.deepEqual(recovered.dispatches[0], first);
  const intended = structuredClone(recovered.dispatches[1]);
  const observed = service(directory, { visible: () => [1] });
  const [result] = await drainNeatlogsOutbox(options(directory, observed, { send: true, readback: true }));
  assert.equal(result.state, 'verified');
  assert.equal(result.batch.send.status, 'outcome_unknown');
  assert.equal(observed.posts.length, 0);
  assert.equal(result.batch.dispatches.length, 2);
  assert.deepEqual(result.batch.dispatches[1], intended);
});

test('pending readback after unknown recovery holds that generation and never silently regenerates it', async () => {
  const directory = batchDirectory('unknown-recovery-pending');
  await drainNeatlogsOutbox(options(directory, service(directory), { readback: true }));
  const pending = service(directory, { status: 503, visible: () => [1], finalizationStatus: 'pending' });
  await drainNeatlogsOutbox(options(directory, pending, { send: true, recover: true, readback: true }));
  const intended = structuredClone(disk(directory).dispatches[1]);
  assert.equal(disk(directory).state, 'pending');
  assert.equal(disk(directory).send.status, 'outcome_unknown');
  assert.equal(disk(directory).plan.traces[0].generation, 1);
  const observed = service(directory, { visible: () => [1] });
  const [result] = await drainNeatlogsOutbox(options(directory, observed, { send: true, recover: true, readback: true }));
  assert.equal(result.state, 'verified');
  assert.equal(result.batch.send.status, 'outcome_unknown');
  assert.equal(observed.posts.length, 0);
  assert.equal(result.batch.dispatches.length, 2);
  assert.deepEqual(result.batch.dispatches[1], intended);
});

test('readback inspects delayed prior generations and retains explicit duplicate evidence', async () => {
  const directory = batchDirectory('duplicate-generation');
  await drainNeatlogsOutbox(options(directory, service(directory), { readback: true }));
  const oldId = disk(directory).plan.traces[0].traceId;
  const fake = service(directory, { visible: () => [0, 1] });
  const [result] = await drainNeatlogsOutbox(options(directory, fake, { recover: true, readback: true }));
  assert.equal(result.state, 'verified');
  assert.equal(result.batch.readback.duplicateEventDeliveries, 1);
  assert.deepEqual(result.batch.readback.traces[0].duplicateTraceIds, [oldId]);
  assert.ok(fake.reads.includes(`/api/v1/public/traces/${oldId}`));
});

test('a child exit at the initial POST resumes the recorded body and keeps the uncertain attempt', async () => {
  const directory = batchDirectory('initial-process-exit');
  const observed = child(directory, 'crash');
  assert.equal(observed.realNetworkAttempts, 0);
  assert.equal(observed.batch.dispatches[0].calls[0].postsStarted, 1);
  const fake = service(directory, { status: 403 });
  const [result] = await drainNeatlogsOutbox(options(directory, fake, { captureTimeoutMs: 1 }));
  assert.equal(fake.posts[0].toString('base64'), observed.bodyBase64);
  assert.equal(result.state, 'pending_send');
  assert.equal(result.batch.send.status, 'outcome_unknown');
  assert.equal(result.batch.dispatches[0].calls[0].result, null);
  assert.equal(result.batch.dispatches[0].calls[1].result.status, 'rejected');
});

test('a child exit at recovery POST resumes that generation without recreating the payload', async () => {
  const directory = batchDirectory('recovery-process-exit');
  await drainNeatlogsOutbox(options(directory, service(directory), { readback: true }));
  const observed = child(directory, 'crash');
  assert.equal(observed.batch.plan.traces[0].generation, 1);
  assert.equal(observed.batch.dispatches[1].calls[0].result, null);
  const fake = service(directory, { status: 503 });
  await drainNeatlogsOutbox(options(directory, fake, { captureTimeoutMs: 1 }));
  assert.equal(fake.posts[0].toString('base64'), observed.bodyBase64);
  assert.equal(disk(directory).dispatches.length, 2);
  assert.equal(disk(directory).plan.traces[0].generation, 1);
});

test('readback-only restart and CLI report the unresolved initial POST without inventing its result', async t => {
  const directory = batchDirectory('initial-summary-restart');
  child(directory, 'crash');
  const retained = readFileSync(pathFor(directory), 'utf8');
  const [snapshot] = [...iterateNeatlogsOutboxSnapshot(directory)];
  assert.equal(readFileSync(pathFor(directory), 'utf8'), retained, 'inspection must stay read-only');
  const calls = structuredClone(disk(directory).dispatches[0].calls);
  const fake = service(directory, { visible: () => [0] });
  const [result] = await drainNeatlogsOutbox(options(directory, fake, { send: false, readback: true }));
  const cli = cliSnapshot(directory);
  t.diagnostic(JSON.stringify({ mode: 'synthetic', scenario: 'initial-post-interrupted',
    snapshotSend: snapshot.batch.send, readbackSend: result.batch.send,
    cliSend: cli.send, posts: fake.posts.length, readback: result.state }));
  assert.equal(result.state, 'verified');
  assert.equal(fake.posts.length, 0);
  for (const batch of [snapshot.batch, result.batch, disk(directory)]) {
    assert.equal(batch.send?.status, 'outcome_unknown');
    assert.equal(batch.send.priorOutcomeUnknown, true);
    assert.equal(batch.send.attempts, 1);
    assert.equal(batch.send.deliveryConfirmed, false);
    assert.deepEqual(batch.dispatches[0].calls, calls);
    assert.equal(batch.dispatches[0].calls[0].result, null);
    assert.equal(batch.dispatches[0].calls[0].completedAt, null);
  }
  assert.equal(cli.send?.status, 'outcome_unknown');
  assert.equal(cli.send.reasonCode, 'DISPATCH_RESULT_NOT_RECORDED');
  assert.equal(cli.traces[0].generation, 0);
});

test('readback-only recovery restart and CLI derive the current generation summary', async t => {
  const directory = batchDirectory('recovery-summary-restart');
  await drainNeatlogsOutbox(options(directory, service(directory), { readback: true }));
  child(directory, 'crash');
  const retained = readFileSync(pathFor(directory), 'utf8');
  const [snapshot] = [...iterateNeatlogsOutboxSnapshot(directory)];
  assert.equal(readFileSync(pathFor(directory), 'utf8'), retained);
  const dispatches = structuredClone(disk(directory).dispatches);
  const fake = service(directory, { visible: () => [1] });
  const [result] = await drainNeatlogsOutbox(options(directory, fake, { send: false, readback: true }));
  const cli = cliSnapshot(directory);
  t.diagnostic(JSON.stringify({ mode: 'synthetic', scenario: 'recovery-post-interrupted',
    snapshotSend: snapshot.batch.send, readbackSend: result.batch.send,
    recoverySend: result.batch.recovery.send, cliSend: cli.send, cliRecoverySend: cli.recovery.send,
    posts: fake.posts.length, generation: result.batch.plan.traces[0].generation, readback: result.state }));
  assert.equal(result.state, 'verified');
  assert.equal(fake.posts.length, 0);
  for (const batch of [snapshot.batch, result.batch, disk(directory)]) {
    assert.equal(batch.send?.status, 'outcome_unknown');
    assert.equal(batch.send.priorOutcomeUnknown, true);
    assert.equal(batch.send.attempts, 1, 'summary belongs to this recovery, not all generations');
    assert.deepEqual(batch.recovery.send, batch.send);
    assert.equal(batch.plan.traces[0].generation, 1);
    assert.deepEqual(batch.dispatches, dispatches);
    assert.equal(batch.dispatches[0].calls[0].result.status, 'receiver_accepted');
    assert.equal(batch.dispatches[1].calls[0].result, null);
    assert.equal(batch.dispatches[1].calls[0].completedAt, null);
  }
  assert.equal(cli.send?.status, 'outcome_unknown');
  assert.equal(cli.recovery.send?.status, 'outcome_unknown');
  assert.equal(cli.traces[0].generation, 1);
});

test('an interrupted eighth POST remains unknown at the cap, after readback and in the CLI', async t => {
  const directory = batchDirectory('eighth-post-summary');
  const refused = service(directory, { status: 429 });
  for (let attempt = 0; attempt < 7; attempt += 1) await drainNeatlogsOutbox(options(directory, refused));
  assert.equal(refused.posts.length, 7);
  assert.equal(disk(directory).send.status, 'not_accepted');
  assert.equal(disk(directory).send.priorOutcomeUnknown, false);
  child(directory, 'crash');
  const calls = structuredClone(disk(directory).dispatches[0].calls);
  const [snapshot] = [...iterateNeatlogsOutboxSnapshot(directory)];
  const fake = service(directory, { visible: () => [0] });
  const [held] = await drainNeatlogsOutbox(options(directory, fake));
  const persistedHold = disk(directory);
  const [observed] = await drainNeatlogsOutbox(options(directory, fake, { send: false, readback: true }));
  const cli = cliSnapshot(directory);
  t.diagnostic(JSON.stringify({ mode: 'synthetic', scenario: 'eighth-post-interrupted',
    snapshotSend: snapshot.batch.send, heldSend: held.batch.send, held: held.state,
    readbackSend: observed.batch.send, cliSend: cli.send, posts: fake.posts.length, readback: observed.state }));
  assert.equal(held.state, 'held');
  assert.equal(held.batch.deliveryHold.reasonCode, 'DISPATCH_ATTEMPT_LIMIT_REACHED');
  assert.equal(observed.state, 'verified');
  assert.equal(fake.posts.length, 0, 'a held eighth POST must never permit a ninth POST');
  for (const batch of [snapshot.batch, held.batch, persistedHold, observed.batch, disk(directory)]) {
    assert.equal(batch.send?.status, 'outcome_unknown');
    assert.equal(batch.send.priorOutcomeUnknown, true);
    assert.equal(batch.send.attempts, 8);
    assert.deepEqual(batch.dispatches[0].calls, calls);
    assert.equal(batch.dispatches[0].calls.at(-1).result, null);
    assert.equal(batch.dispatches[0].calls.at(-1).completedAt, null);
  }
  assert.equal(cli.send?.status, 'outcome_unknown');
  assert.equal(cli.send.attempts, 8);
});

test('no-call and interrupted-before-POST intents have no invented send or prior-generation acknowledgement', async () => {
  for (const recovery of [false, true]) for (const failAt of [2, 3]) {
    const directory = batchDirectory(`unposted-summary-${recovery}-${failAt}`);
    if (recovery) await drainNeatlogsOutbox(options(directory, service(directory), { readback: true }));
    const stopped = child(directory, 'fail-write', failAt);
    assert.equal(stopped.code, 'EIO');
    assert.equal(stopped.posts, 0);
    const retained = readFileSync(pathFor(directory), 'utf8');
    const dispatches = structuredClone(disk(directory).dispatches);
    const [snapshot] = [...iterateNeatlogsOutboxSnapshot(directory)];
    assert.equal(readFileSync(pathFor(directory), 'utf8'), retained);
    const current = dispatches.at(-1);
    assert.equal(current.calls.length, failAt === 2 ? 0 : 1);
    assert.ok(current.calls.every(call => call.postsStarted === 0 && call.result === null && call.completedAt === null));
    const fake = service(directory);
    const [inspected] = await drainNeatlogsOutbox(options(directory, fake, { send: false, readback: false }));
    assert.equal(readFileSync(pathFor(directory), 'utf8'), retained);
    const [readback] = await drainNeatlogsOutbox(options(directory, fake, { send: false, readback: true }));
    assert.equal(fake.posts.length, 0);
    assert.equal(fake.reads.length, 0, 'the current unposted intent has no possible delivery to read back');
    assert.equal(readback.state, 'pending_send');
    for (const batch of [snapshot.batch, inspected.batch, readback.batch]) {
      assert.equal(batch.send, null, 'no send result or possible POST exists for this intent');
      if (recovery) assert.equal(batch.recovery.send, null, 'generation zero acceptance cannot summarize generation one');
      assert.deepEqual(batch.dispatches, dispatches);
    }
  }
});

test('an incomplete zero-POST call preserves the known previous result for its current intent', async () => {
  const directory = batchDirectory('known-result-unposted-followup');
  await drainNeatlogsOutbox(options(directory, service(directory, { status: 429 })));
  const known = structuredClone(disk(directory).send);
  const stopped = child(directory, 'fail-write', 2);
  assert.equal(stopped.code, 'EIO');
  assert.equal(stopped.posts, 0);
  const dispatches = structuredClone(disk(directory).dispatches);
  assert.equal(dispatches[0].calls.length, 2);
  assert.equal(dispatches[0].calls[1].postsStarted, 0);
  assert.equal(dispatches[0].calls[1].result, null);
  const [snapshot] = [...iterateNeatlogsOutboxSnapshot(directory)];
  const fake = service(directory);
  const [result] = await drainNeatlogsOutbox(options(directory, fake, { send: false, readback: true }));
  for (const batch of [snapshot.batch, result.batch, disk(directory)]) {
    assert.deepEqual(batch.send, known);
    assert.equal(batch.send.status, 'not_accepted');
    assert.equal(batch.send.priorOutcomeUnknown, false);
    assert.deepEqual(batch.dispatches, dispatches);
  }
  assert.equal(fake.posts.length, 0);
  assert.equal(fake.reads.length, 0);
});

for (const [mode, boundary, calls] of [['fail-write', 2, 0], ['fail-write', 3, 1],
  ['exit-before-post', 1, 0], ['exit-before-post', 2, 1]]) {
  for (const priorVisible of [false, true]) {
    test(`unposted recovery survives readback and ordinary draining: ${mode}/${boundary}/prior=${priorVisible}`, async t => {
      const directory = batchDirectory(`unposted-readback-${mode}-${boundary}-${priorVisible}`, 2);
      const visible = sequence => sequence === 1 ? [0] : [];
      await drainNeatlogsOutbox(options(directory, service(directory, { visible }), { readback: true }));
      const stopped = child(directory, mode, boundary);
      assert.equal(stopped.posts, 0);
      if (mode === 'fail-write') assert.equal(stopped.code, 'EIO');
      const intended = disk(directory);
      const current = intended.dispatches.at(-1);
      assert.equal(current.calls.length, calls);
      assert.ok(current.calls.every(call => call.postsStarted === 0 && call.result === null));
      assert.deepEqual(current.traces.map(trace => trace.sequence), [2]);
      const before = readFileSync(pathFor(directory), 'utf8');
      const fake = service(directory, { visible: sequence => sequence === 1 || priorVisible ? [0] : [] });
      const [observed] = await drainNeatlogsOutbox(options(directory, fake, { send: false, readback: true }));
      const afterReadback = readFileSync(pathFor(directory), 'utf8');
      let delivered = false;
      const resumed = service(directory, {
        onPost: () => { delivered = true; },
        visible: sequence => sequence === 1 ? [0]
          : delivered ? (priorVisible ? [0, 1] : [1]) : priorVisible ? [0] : []
      });
      const [result] = await drainNeatlogsOutbox(options(directory, resumed, { readback: true, captureTimeoutMs: 1 }));
      t.diagnostic(JSON.stringify({ mode: 'synthetic', boundary: `${mode}/${boundary}`, priorVisible,
        readbackState: observed.state, readbackRequests: fake.reads.length,
        ordinaryPosts: resumed.posts.length, finalState: result.state }));
      assert.equal(observed.state, 'pending_send');
      assert.equal(observed.batch.send, null);
      assert.equal(observed.batch.recovery.send, null);
      assert.equal(observed.batch.readback, null);
      assert.equal(fake.posts.length, 0);
      assert.equal(fake.reads.length, 0);
      assert.equal(afterReadback, before);
      assert.equal(resumed.posts.length, 1);
      assert.equal(resumed.posts[0].toString('base64'), current.bodyBase64);
      assert.equal(result.state, 'verified');
      assert.equal(result.batch.dispatches.length, 2);
      assert.deepEqual(result.batch.plan.traces.map(trace => trace.generation), [0, 1]);
      assert.deepEqual(result.batch.dispatches[0], intended.dispatches[0]);
      assert.deepEqual(result.batch.dispatches[1].calls.slice(0, calls), current.calls);
      assert.equal(result.batch.send.status, 'receiver_accepted');
      assert.equal(result.batch.send.deliveryConfirmed, false);
      assert.equal(result.batch.readback.duplicateEventDeliveries, priorVisible ? 1 : 0);
    });
  }
}

const retainedRecoveryFixtures = JSON.parse(readFileSync(new URL(
  './fixtures/unposted-recovery-readback.json', import.meta.url), 'utf8'));
for (const fixture of retainedRecoveryFixtures.cases) {
  test(`ordinary recovery resumes actual retained b757 ${fixture.name} bytes`, async t => {
    const directory = batchDirectory(`retained-readback-${fixture.name}`);
    assert.equal(retainedRecoveryFixtures.sourceCommit, 'b757573aaa326e47ea7154a37ded6aa71a73a1b8');
    assert.equal(createHash('sha256').update(fixture.batchJson).digest('hex'), fixture.sha256);
    writeFileSync(pathFor(directory), fixture.batchJson, { mode: 0o600 });
    const before = disk(directory);
    const current = before.dispatches.at(-1);
    const [snapshot] = [...iterateNeatlogsOutboxSnapshot(directory)];
    assert.equal(snapshot.state, fixture.state);
    const fake = service(directory);
    await drainNeatlogsOutbox(options(directory, fake, { send: false, readback: false }));
    assert.equal(readFileSync(pathFor(directory), 'utf8'), fixture.batchJson, 'read-only inspection cannot repair state on disk');
    const [resumable] = await drainNeatlogsOutbox(options(directory, fake, { send: false, readback: true }));
    const [result] = await drainNeatlogsOutbox(options(directory, fake, { recover: true, captureTimeoutMs: 1 }));
    t.diagnostic(JSON.stringify({ mode: 'synthetic', retainedState: fixture.state,
      restoredState: resumable.state, posts: fake.posts.length, finalState: result.state }));
    assert.equal(resumable.state, 'pending_send');
    assert.deepEqual(resumable.batch.send, before.send);
    assert.deepEqual(resumable.batch.recovery.send, before.recovery.send);
    assert.equal(resumable.batch.send?.status ?? null, fixture.expectedSendStatus);
    assert.deepEqual(resumable.batch.dispatches, before.dispatches);
    assert.deepEqual(resumable.batch.readback, before.readback);
    assert.equal(fake.reads.length, 0);
    assert.equal(fake.posts.length, 1);
    assert.equal(fake.posts[0].toString('base64'), current.bodyBase64);
    assert.equal(result.state, 'receiver_accepted');
    assert.equal(result.batch.dispatches.length, 2, 'resume the existing intent instead of selecting generation two');
    assert.equal(result.batch.plan.traces[0].generation, 1);
    assert.deepEqual(result.batch.dispatches[0], before.dispatches[0]);
    assert.deepEqual(result.batch.dispatches[1].calls.slice(0, current.calls.length), current.calls);
    assert.deepEqual(result.batch.recoveryHistory, before.recoveryHistory);
    assert.deepEqual(result.batch.readback, before.readback, 'retain the actual previous observation');
  });
}

test('an ordinary send directly restores a retained unsent recovery before entering recovery selection', async () => {
  const fixture = retainedRecoveryFixtures.cases.find(item => item.state === 'missing');
  const directory = batchDirectory('retained-direct-send');
  writeFileSync(pathFor(directory), fixture.batchJson, { mode: 0o600 });
  const intended = disk(directory).dispatches[1];
  const fake = service(directory);
  const [result] = await drainNeatlogsOutbox(options(directory, fake, { recover: true, captureTimeoutMs: 1 }));
  assert.equal(fake.posts.length, 1);
  assert.equal(fake.posts[0].toString('base64'), intended.bodyBase64);
  assert.equal(result.batch.dispatches.length, 2);
  assert.equal(result.batch.plan.traces[0].generation, 1);
});

for (const failAt of [1, 2, 3]) {
  test(`restored routing and pre-POST writes remain durable at failure boundary ${failAt}`, async () => {
    const fixture = retainedRecoveryFixtures.cases.find(item => item.state === 'missing');
    const directory = batchDirectory(`retained-routing-write-${failAt}`);
    writeFileSync(pathFor(directory), fixture.batchJson, { mode: 0o600 });
    const before = disk(directory);
    const intended = before.dispatches[1];
    const stopped = child(directory, 'fail-write', failAt);
    assert.equal(stopped.code, 'EIO');
    assert.equal(stopped.posts, 0);
    if (failAt === 1) assert.equal(readFileSync(pathFor(directory), 'utf8'), fixture.batchJson);
    assert.deepEqual(disk(directory).readback, before.readback);
    let boundary;
    const fake = service(directory, { onPost: observation => { boundary = observation; } });
    const [result] = await drainNeatlogsOutbox(options(directory, fake, { recover: true, captureTimeoutMs: 1 }));
    assert.equal(fake.posts.length, 1);
    assert.equal(fake.posts[0].toString('base64'), intended.bodyBase64);
    assert.equal(boundary.batch.send.status, 'outcome_unknown');
    assert.equal(boundary.batch.send.deliveryConfirmed, false);
    assert.equal(boundary.batch.dispatches[1].calls.at(-1).postsStarted, 1);
    assert.equal(boundary.batch.dispatches[1].calls.at(-1).result, null);
    assert.deepEqual(boundary.batch.readback, before.readback);
    assert.equal(result.batch.dispatches.length, 2);
    assert.equal(result.batch.plan.traces[0].generation, 1);
    assert.deepEqual(result.batch.dispatches[1].calls.slice(0, intended.calls.length), intended.calls);
    assert.deepEqual(result.batch.recoveryHistory, before.recoveryHistory);
  });
}

for (const status of [200, 503]) {
  test(`current-intent earlier ${status} delivery remains readable after a zero-POST call`, async () => {
    const directory = batchDirectory(`current-prior-result-${status}`);
    await drainNeatlogsOutbox(options(directory, service(directory), { readback: true }));
    await drainNeatlogsOutbox(options(directory, service(directory, { status }), { recover: true }));
    const batch = disk(directory);
    // Synthetic history control: the incomplete call contributes no effect,
    // but an earlier possible delivery in this same intent still matters.
    batch.dispatches[1].calls.push({ ordinal: 2, startedAt: new Date().toISOString(),
      postsStarted: 0, completedAt: null, result: null });
    replace(directory, batch);
    const fake = service(directory, { visible: () => [0, 1] });
    const [result] = await drainNeatlogsOutbox(options(directory, fake, { send: false, readback: true }));
    assert.equal(result.state, 'verified');
    assert.equal(fake.posts.length, 0);
    assert.ok(fake.reads.length > 0);
    assert.equal(result.batch.send.status, status === 200 ? 'receiver_accepted' : 'outcome_unknown');
    assert.equal(result.batch.send.deliveryConfirmed, false);
    assert.deepEqual(result.batch.dispatches, batch.dispatches);
    assert.equal(result.batch.readback.duplicateEventDeliveries, 1);
  });
}

test('restoration does not reset terminals, explicit holds or an unmatched retained observation', async () => {
  const fixture = retainedRecoveryFixtures.cases.find(item => item.state === 'pending');
  for (const state of ['verified', 'mismatch', 'held', 'blocked', 'rejected', 'pending', 'missing']) {
    const directory = batchDirectory(`retained-negative-${state}`);
    const batch = JSON.parse(fixture.batchJson);
    batch.state = state;
    if (state === 'held' || state === 'pending') batch.deliveryHold = { reasonCode: 'DISPATCH_ATTEMPT_LIMIT_REACHED', outcomeResolved: false };
    // The missing variant deliberately disagrees with its pending observation.
    replace(directory, batch);
    const before = readFileSync(pathFor(directory), 'utf8');
    const fake = service(directory);
    const [result] = await drainNeatlogsOutbox(options(directory, fake));
    assert.equal(result.state, state);
    assert.equal(fake.posts.length, 0);
    assert.equal(fake.reads.length, 0);
    assert.equal(readFileSync(pathFor(directory), 'utf8'), before);
  }
});

for (const localStop of ['not_configured', 'aborted']) {
  test(`a completed local ${localStop} with zero POSTs keeps its recovery eligible for ordinary sending`, async () => {
    const directory = batchDirectory(`zero-post-local-${localStop}`);
    await drainNeatlogsOutbox(options(directory, service(directory), { readback: true }));
    const absentIngest = loadNeatlogsConfig({ env: { NEATLOGS_READ_TOKEN: READ_TOKEN, NEATLOGS_PROJECT_ID: PROJECT_ID } });
    const fake = service(directory, { visible: () => [0] });
    const [stopped] = await drainNeatlogsOutbox(options(directory, fake, { recover: true, readback: true,
      config: localStop === 'not_configured' ? absentIngest : CONFIG,
      sendOptions: { maxAttempts: 1, ...(localStop === 'aborted' ? { signal: AbortSignal.abort() } : {}) }
    }));
    const intended = structuredClone(stopped.batch.dispatches[1]);
    const initialPosts = fake.posts.length;
    const initialReads = fake.reads.length;
    const [observed] = await drainNeatlogsOutbox(options(directory, fake, { send: false, readback: true }));
    const [resumed] = await drainNeatlogsOutbox(options(directory, fake, { captureTimeoutMs: 1 }));
    assert.equal(stopped.state, 'pending_send');
    assert.equal(stopped.batch.send.status, localStop);
    assert.equal(initialPosts, 0);
    assert.equal(initialReads, 0);
    assert.equal(observed.state, 'pending_send');
    assert.equal(fake.reads.length, 0);
    assert.equal(fake.posts.length, 1);
    assert.equal(fake.posts[0].toString('base64'), intended.bodyBase64);
    assert.equal(resumed.batch.dispatches.length, 2);
    assert.deepEqual(resumed.batch.dispatches[1].calls[0], intended.calls[0]);
    assert.equal(intended.calls[0].postsStarted, 0);
    assert.equal(intended.calls[0].result.status, localStop);
    assert.equal(resumed.batch.send.status, 'receiver_accepted');
    assert.equal(resumed.batch.send.priorOutcomeUnknown, false);
  });
}

test('retained readback states do not reset a completed refusal or possible current delivery', async () => {
  const observation = JSON.parse(retainedRecoveryFixtures.cases.find(item => item.state === 'missing').batchJson).readback;
  for (const status of [200, 403, 429, 503]) {
    const directory = batchDirectory(`known-delivery-hold-${status}`);
    await drainNeatlogsOutbox(options(directory, service(directory), { readback: true }));
    await drainNeatlogsOutbox(options(directory, service(directory, { status }), { recover: true }));
    const batch = disk(directory);
    assert.equal(batch.dispatches[1].calls[0].postsStarted, 1);
    // Synthetic compatibility control uses a real bounded-window observation
    // for these same deterministic generation-one IDs; it keeps the actual
    // sender result and is not a fresh provider observation of this fixture.
    batch.state = 'missing';
    batch.readback = structuredClone(observation);
    replace(directory, batch);
    const before = readFileSync(pathFor(directory), 'utf8');
    const fake = service(directory);
    const [result] = await drainNeatlogsOutbox(options(directory, fake));
    assert.equal(result.state, 'missing');
    assert.equal(fake.posts.length, 0);
    assert.equal(fake.reads.length, 0);
    assert.deepEqual(result.batch.dispatches, batch.dispatches);
    assert.equal(readFileSync(pathFor(directory), 'utf8'), before);
  }
});

test('send plus readback after interrupted recovery retains uncertainty and the selected generation', async () => {
  const directory = batchDirectory('interrupted-recovery-readback');
  await drainNeatlogsOutbox(options(directory, service(directory), { readback: true }));
  const interrupted = child(directory, 'crash');
  const refused = service(directory, { status: 403 });
  await drainNeatlogsOutbox(options(directory, refused, { send: true, readback: true, captureTimeoutMs: 1 }));
  const retained = disk(directory);
  assert.equal(refused.posts[0].toString('base64'), interrupted.bodyBase64);
  assert.equal(retained.state, 'missing');
  assert.equal(retained.send.status, 'outcome_unknown');
  assert.equal(retained.send.lastResult.status, 'rejected');
  assert.equal(retained.dispatches[1].calls[0].result, null);
  assert.equal(retained.plan.traces[0].generation, 1);
  const observed = service(directory, { visible: () => [1] });
  const [result] = await drainNeatlogsOutbox(options(directory, observed, { send: true, readback: true }));
  assert.equal(result.state, 'verified');
  assert.equal(result.batch.send.status, 'outcome_unknown');
  assert.equal(observed.posts.length, 0);
  assert.equal(result.batch.dispatches.length, 2);
  assert.deepEqual(result.batch.dispatches[1], retained.dispatches[1]);
});

test('durable intent or start-marker failure prevents initial and recovery HTTP dispatch', async () => {
  for (const recovery of [false, true]) for (const failAt of [1, 3]) {
    const directory = batchDirectory(`persist-failure-${recovery}-${failAt}`);
    if (recovery) await drainNeatlogsOutbox(options(directory, service(directory), { readback: true }));
    const result = child(directory, 'fail-write', failAt);
    assert.equal(result.code, 'EIO');
    assert.equal(result.posts, 0);
    assert.equal(result.batchWrites, failAt);
    assert.equal(disk(directory).dispatches.length, recovery ? (failAt === 1 ? 1 : 2) : (failAt === 1 ? 0 : 1));
  }
});

test('result persistence failure after POST leaves an unresolved durable start marker on restart', async () => {
  const directory = batchDirectory('result-persist-failure');
  const result = child(directory, 'fail-write', 4);
  assert.equal(result.code, 'EIO');
  assert.equal(result.posts, 1);
  assert.equal(disk(directory).dispatches[0].calls[0].result, null);
  const fake = service(directory, { status: 403 });
  await drainNeatlogsOutbox(options(directory, fake, { captureTimeoutMs: 1 }));
  assert.equal(disk(directory).send.status, 'outcome_unknown');
});

test('corrupt payload, record, time, generation and history bindings reject before I/O', async () => {
  const directory = batchDirectory('invalid-intent');
  await drainNeatlogsOutbox(options(directory, service(directory, { status: 503 })));
  const original = disk(directory);
  const mutations = [
    batch => { batch.dispatches[0].bodySha256 = '0'.repeat(64); },
    batch => { batch.dispatches[0].bodyBase64 += 'A'; },
    batch => { batch.dispatches[0].traces[0].root.end += 1024; },
    batch => { batch.records[0].revision += 1; },
    batch => { batch.dispatches[0].traces[0].generation = 1; },
    batch => { batch.dispatches[0].traces[0].root.start = '0'; },
    batch => { batch.dispatches[0].calls[0].postsStarted = 0; },
    batch => { batch.dispatches[0].calls[0].result.extra = 'untrusted'; },
    batch => { batch.dispatches[0].target.projectHash = 'bad'; },
    batch => { batch.dispatches[0].previousTraceIds = ['0'.repeat(32)]; },
    batch => { batch.plan.traces[0].generation = 2; }
  ];
  const fake = service(directory);
  for (const mutate of mutations) {
    const batch = structuredClone(original);
    mutate(batch);
    replace(directory, batch);
    await assert.rejects(drainNeatlogsOutbox(options(directory, fake)), error =>
      ['DISPATCH_INTENT_INVALID', 'PLAN_IDENTITY_MISMATCH'].includes(error.code));
  }
  assert.equal(fake.posts.length, 0);
  assert.equal(fake.reads.length, 0);
  replace(directory, original);
});

test('oversized retained files are refused without reads, POST or artifact replacement', async () => {
  const directory = batchDirectory('oversized-batch');
  const original = readFileSync(pathFor(directory), 'utf8');
  writeFileSync(pathFor(directory), `${original}${' '.repeat(2 * 1024 * 1024)}`, { mode: 0o600 });
  const size = statSync(pathFor(directory)).size;
  const fake = service(directory);
  await assert.rejects(drainNeatlogsOutbox(options(directory, fake)), { code: 'BATCH_INVALID' });
  assert.equal(statSync(pathFor(directory)).size, size);
  assert.equal(fake.posts.length, 0);
  assert.equal(fake.reads.length, 0);
});

test('duplicate JSON keys and excessive nesting are rejected by the restart parser before I/O', async () => {
  const directory = batchDirectory('invalid-persisted-json');
  const original = disk(directory);
  let nested = null;
  for (let depth = 0; depth < 65; depth += 1) nested = { child: nested };
  const invalidTexts = [
    JSON.stringify(original).replace('{', '{"schema":"duplicate",'),
    JSON.stringify({ ...original, extension: nested })
  ];
  const fake = service(directory);
  for (const text of invalidTexts) {
    writeFileSync(pathFor(directory), text, { mode: 0o600 });
    await assert.rejects(drainNeatlogsOutbox(options(directory, fake)), { code: 'MALFORMED_RESPONSE' });
    assert.equal(readFileSync(pathFor(directory), 'utf8'), text);
  }
  assert.equal(fake.posts.length, 0);
  assert.equal(fake.reads.length, 0);
});

test('every sender retry records its start and respects the total POST budget across drains', async () => {
  const directory = batchDirectory('sender-retry-budget');
  const started = [];
  const fake = service(directory, { status: 503, onPost: ({ batch }) => {
    started.push(batch.dispatches[0].calls.at(-1).postsStarted);
  } });
  const sendOptions = { maxAttempts: 5, retryDelayMs: 0, sleep: async () => true };
  await drainNeatlogsOutbox(options(directory, fake, { sendOptions }));
  await drainNeatlogsOutbox(options(directory, fake, { sendOptions }));
  await drainNeatlogsOutbox(options(directory, fake, { sendOptions }));
  assert.deepEqual(started, [1, 2, 3, 4, 5, 1, 2, 3]);
  assert.equal(fake.posts.length, 8);
  for (const body of fake.posts) assert.deepEqual(body, fake.posts[0]);
  assert.deepEqual(disk(directory).dispatches[0].calls.map(call => call.result.attempts), [5, 3]);
  assert.equal(disk(directory).state, 'held');
  assert.equal(disk(directory).send.status, 'outcome_unknown');
});

test('a failed caller start hook stops before fetch and leaves a conservative unresolved marker', async () => {
  const directory = batchDirectory('start-hook-failure');
  const fake = service(directory);
  await assert.rejects(drainNeatlogsOutbox(options(directory, fake, { sendOptions: {
    maxAttempts: 5, onPostStart() { throw new Error('synthetic caller interruption'); }
  } })), /synthetic caller interruption/);
  assert.equal(fake.posts.length, 0);
  assert.equal(disk(directory).dispatches[0].calls[0].postsStarted, 1);
  assert.equal(disk(directory).dispatches[0].calls[0].result, null);
  const refused = service(directory, { status: 403 });
  await drainNeatlogsOutbox(options(directory, refused));
  assert.equal(disk(directory).send.status, 'outcome_unknown');
  assert.equal(disk(directory).send.lastResult.status, 'rejected');
});

test('cross-drain attempts are bounded and a held unknown intent remains eligible for readback', async () => {
  const directory = batchDirectory('attempt-bound');
  const unavailable = service(directory, { status: 503 });
  for (let call = 0; call < 10; call += 1) await drainNeatlogsOutbox(options(directory, unavailable));
  assert.equal(unavailable.posts.length, 8);
  const held = disk(directory);
  assert.equal(held.state, 'held');
  assert.equal(held.deliveryHold.reasonCode, 'DISPATCH_ATTEMPT_LIMIT_REACHED');
  assert.equal(held.send.status, 'outcome_unknown');
  assert.equal(held.dispatches[0].calls.length, 8);
  const readable = service(directory, { visible: () => [0] });
  const [result] = await drainNeatlogsOutbox(options(directory, readable, { send: false, readback: true }));
  assert.equal(result.state, 'verified');
  assert.equal(readable.posts.length, 0);
  assert.equal(result.batch.dispatches[0].calls.length, 8);
});

test('legacy null and attempted states stay held without reconstructing potentially sent bytes', async () => {
  for (const attempted of [false, true]) {
    const directory = batchDirectory(`legacy-${attempted}`);
    if (attempted) await drainNeatlogsOutbox(options(directory, service(directory, { status: 503 })));
    const batch = disk(directory);
    batch.schema = 'agent-harness.neatlogs.outbox-batch.v1';
    delete batch.dispatches;
    replace(directory, batch);
    const fake = service(directory);
    const [result] = await drainNeatlogsOutbox(options(directory, fake, { recover: true }));
    assert.equal(result.state, 'legacy_held');
    assert.equal(result.batch.legacyDelivery.automaticResendAllowed, false);
    assert.equal(result.batch.legacyDelivery.outcomeResolved, false);
    assert.deepEqual(result.batch.send, batch.send);
    assert.deepEqual(result.batch.plan, batch.plan);
    assert.equal(fake.posts.length, 0);
  }
});

test('legacy accepted records remain readable but missing legacy IDs never trigger recovery sends', async () => {
  for (const available of [false, true]) {
    const directory = batchDirectory(`legacy-readback-${available}`);
    await drainNeatlogsOutbox(options(directory, service(directory)));
    const batch = disk(directory);
    batch.schema = 'agent-harness.neatlogs.outbox-batch.v1';
    delete batch.dispatches;
    replace(directory, batch);
    const fake = service(directory, { visible: () => available ? [0] : [] });
    const [result] = await drainNeatlogsOutbox(options(directory, fake, { recover: true, readback: true }));
    assert.equal(result.state, available ? 'verified' : 'missing');
    await drainNeatlogsOutbox(options(directory, fake, { recover: true }));
    assert.equal(fake.posts.length, 0);
    assert.equal(Object.hasOwn(disk(directory), 'dispatches'), false);
  }
});

test('maximum record/tag/recovery histories stay within storage bounds and retain every wire snapshot', async () => {
  const directory = batchDirectory('max-retained-intents', 32);
  const tags = [...TAGS, ...Array.from({ length: 7 }, (_, index) => `${index}${'x'.repeat(127)}`)];
  const fake = service(directory, { tags });
  const unavailable = service(directory, { tags, status: 503 });
  for (let generation = 0; generation <= 3; generation += 1) {
    // 32 retained calls and eight possible POST starts per generation. The
    // pre-aborted calls exercise the call bound independently of the POST cap.
    for (let call = 0; call < 24; call += 1) await drainNeatlogsOutbox(options(directory, fake, {
      tags, recover: true, sendOptions: { maxAttempts: 1, signal: AbortSignal.abort() }
    }));
    for (let call = 0; call < 7; call += 1) await drainNeatlogsOutbox(options(directory, unavailable, { tags }));
    await drainNeatlogsOutbox(options(directory, fake, { tags, readback: true }));
    assert.ok(disk(directory).plan.traces.every(trace => trace.generation === generation));
    assert.equal(disk(directory).dispatches[generation].calls.length, 32);
    assert.equal(disk(directory).dispatches[generation].calls.reduce((sum, call) => sum + call.postsStarted, 0), 8);
  }
  const snapshots = disk(directory).dispatches.map(intent => ({ bodyBase64: intent.bodyBase64, bodySha256: intent.bodySha256 }));
  await drainNeatlogsOutbox(options(directory, fake, { tags, recover: true }));
  assert.equal(fake.posts.length, 4);
  assert.equal(disk(directory).state, 'held');
  assert.equal(disk(directory).deliveryHold.reasonCode, 'RECOVERY_GENERATION_LIMIT_REACHED');
  assert.equal(disk(directory).dispatches.length, 4);
  assert.equal(disk(directory).recoveryHistory.length, 3);
  assert.deepEqual(disk(directory).dispatches.map(intent => ({ bodyBase64: intent.bodyBase64, bodySha256: intent.bodySha256 })), snapshots);
  assert.ok(statSync(pathFor(directory)).size < 2 * 1024 * 1024);
  assert.equal(Array.from(iterateNeatlogsOutboxSnapshot(directory)).length, 1);
});
