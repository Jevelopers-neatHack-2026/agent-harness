import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import net from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import tls from 'node:tls';
import { after, before, test } from 'node:test';
import { loadNeatlogsConfig } from '../../../src/observability/private/neatlogs-config.js';
import {
  createNeatlogsOutboxSink, drainNeatlogsOutbox, listNeatlogsOutbox, runtimeNeatlogsTags
} from '../../../src/observability/private/neatlogs-outbox.js';

const ENV = Object.freeze({
  NEATLOGS_INGEST_KEY: 'synthetic-ingest-key-0123456789abcdef',
  NEATLOGS_READ_TOKEN: 'synthetic-readback-token-0123456789abcdef',
  NEATLOGS_PROJECT_ID: '00000000-0000-4000-8000-000000000001'
});
const CONFIG = loadNeatlogsConfig({ env: ENV });
const TAGS = runtimeNeatlogsTags({ revision: 'b'.repeat(40), harnessMode: 'mock' });
let scratch;
let realNetworkAttempts = 0;
const original = { fetch: globalThis.fetch, connect: net.Socket.prototype.connect, tls: tls.connect, now: Date.now };
before(() => {
  scratch = mkdtempSync(join(tmpdir(), 'neatlogs-outbox-retry-after-'));
  const deny = () => { realNetworkAttempts += 1; throw new Error('real network forbidden'); };
  globalThis.fetch = deny;
  net.Socket.prototype.connect = deny;
  tls.connect = deny;
});
after(() => {
  globalThis.fetch = original.fetch;
  net.Socket.prototype.connect = original.connect;
  tls.connect = original.tls;
  Date.now = original.now;
  rmSync(scratch, { recursive: true, force: true });
  assert.equal(realNetworkAttempts, 0);
});

function directory(name) {
  const path = join(scratch, name);
  createNeatlogsOutboxSink({ directory: path })([{
    schema: 'agent-harness.telemetry.v1', sequence: 1,
    eventId: `tok1_${'1'.repeat(64)}`, workflowId: `tok1_${'f'.repeat(64)}`,
    revision: 0, eventType: 'workflow_created', fromState: null, toState: null,
    fence: 0, occurredAtMs: 1_760_000_000_000
  }]);
  return path;
}
const pathFor = path => listNeatlogsOutbox(path)[0];
const disk = path => JSON.parse(readFileSync(pathFor(path), 'utf8'));
function options(path, fake, extra = {}) {
  return { directory: path, config: CONFIG, tags: TAGS, fetch: fake.fetch,
    sendOptions: { maxAttempts: 1 }, readbackOptions: { maxPolls: 2, pollIntervalMs: 0 }, ...extra };
}
function service(status = 200, retryAfter) {
  const posts = [];
  const reads = [];
  return { posts, reads, fetch: async (url, init) => {
    const pathname = new URL(url).pathname;
    if (pathname === '/v1/traces') {
      posts.push(Buffer.from(init.body));
      return new Response(null, { status,
        ...(retryAfter === undefined ? {} : { headers: { 'retry-after': retryAfter } }) });
    }
    reads.push(pathname);
    if (pathname.endsWith('/project')) return new Response(JSON.stringify({
      success: true, requestId: 'req_fake', data: { id: ENV.NEATLOGS_PROJECT_ID }
    }), { status: 200, headers: { 'content-type': 'application/json' } });
    return new Response('{}', { status: 404, headers: { 'content-type': 'application/problem+json' } });
  } };
}
async function at(now, run) {
  Date.now = () => now;
  try { return await run(); } finally { Date.now = original.now; }
}
function minimumDeadline(batch) {
  const call = batch.dispatches.at(-1).calls.at(-1);
  return Date.parse(call.completedAt) + call.result.retryAfterMinimumMs;
}

for (const status of [429, 503]) test(`${status} minimum survives a fresh process without another call or POST`, async () => {
  const path = directory(`restart-${status}`);
  const first = service(status, '120');
  await drainNeatlogsOutbox(options(path, first));
  const retained = readFileSync(pathFor(path), 'utf8');
  const batch = disk(path);
  assert.equal(batch.dispatches[0].calls[0].result.retryAfterMinimumMs, 120_000);
  const code = `
    import net from 'node:net';
    import tls from 'node:tls';
    const { drainNeatlogsOutbox } = await import(process.argv[1]);
    const { loadNeatlogsConfig } = await import(process.argv[2]);
    let realNetworkAttempts = 0, posts = 0;
    const deny = () => { realNetworkAttempts += 1; throw new Error('real network forbidden'); };
    globalThis.fetch = deny; net.Socket.prototype.connect = deny; tls.connect = deny;
    const args = JSON.parse(process.argv[3]);
    Date.now = () => args.now;
    const [result] = await drainNeatlogsOutbox({ directory: args.path,
      config: loadNeatlogsConfig({ env: args.env }), tags: args.tags,
      sendOptions: { maxAttempts: 1 }, fetch: async () => { posts += 1; return new Response(null, { status: 200 }); } });
    process.stdout.write(JSON.stringify({ posts, realNetworkAttempts, dispatches: result.batch.dispatches }));
  `;
  const child = spawnSync(process.execPath, ['--input-type=module', '-e', code,
    new URL('../../../src/observability/private/neatlogs-outbox.js', import.meta.url).href,
    new URL('../../../src/observability/private/neatlogs-config.js', import.meta.url).href,
    JSON.stringify({ path, env: ENV, tags: TAGS, now: minimumDeadline(batch) - 1 })], {
    encoding: 'utf8', timeout: 10_000, maxBuffer: 64 * 1024,
    env: { PATH: process.env.PATH, TMPDIR: tmpdir(), NODE_NO_WARNINGS: '1' }
  });
  assert.equal(child.error, undefined);
  assert.equal(child.signal, null);
  assert.equal(child.status, 0, child.stderr);
  const result = JSON.parse(child.stdout);
  assert.equal(result.realNetworkAttempts, 0);
  assert.equal(result.posts, 0);
  assert.deepEqual(result.dispatches, batch.dispatches);
  assert.equal(readFileSync(pathFor(path), 'utf8'), retained);
});

test('the exact recorded deadline permits identical bytes, while a backward clock and deadline minus one defer', async () => {
  const path = directory('deadline');
  const first = service(429, '120');
  await drainNeatlogsOutbox(options(path, first));
  const batch = disk(path);
  const retained = readFileSync(pathFor(path), 'utf8');
  const fake = service();
  for (const now of [Date.parse(batch.dispatches[0].calls[0].completedAt) - 1_000, minimumDeadline(batch) - 1]) {
    await at(now, () => drainNeatlogsOutbox(options(path, fake)));
    assert.equal(fake.posts.length, 0);
    assert.equal(readFileSync(pathFor(path), 'utf8'), retained);
  }
  await at(minimumDeadline(batch), () => drainNeatlogsOutbox(options(path, fake)));
  assert.equal(fake.posts.length, 1);
  assert.ok(fake.posts[0].equals(first.posts[0]));
  assert.equal(disk(path).dispatches[0].calls.length, 2);
  assert.equal(disk(path).state, 'receiver_accepted');
});

test('readback continues during the minimum without allocating a recovery generation', async () => {
  const path = directory('recovery');
  const first = service(503, '120');
  await drainNeatlogsOutbox(options(path, first, { readback: true }));
  const batch = disk(path);
  assert.equal(batch.state, 'missing');
  assert.ok(first.reads.length > 0);
  const fake = service();
  for (let index = 0; index < 5; index += 1) {
    await at(minimumDeadline(batch) - 1, () => drainNeatlogsOutbox(options(path, fake, { recover: true, readback: true })));
    assert.equal(fake.posts.length, 0);
    assert.deepEqual(disk(path).dispatches, batch.dispatches);
    assert.equal(disk(path).plan.traces[0].generation, 0);
  }
  assert.ok(fake.reads.length > 0, 'readback stays available while POSTs wait');
  await at(minimumDeadline(batch), () => drainNeatlogsOutbox(options(path, fake, { recover: true })));
  assert.equal(fake.posts.length, 1);
  assert.equal(disk(path).dispatches.length, 2);
  assert.equal(disk(path).plan.traces[0].generation, 1);
});

test('validated earlier call history controls the wait even when the current summary has no minimum', async () => {
  for (const incomplete of [true, false]) {
    const path = directory(`history-${incomplete}`);
    await drainNeatlogsOutbox(options(path, service(503, '120')));
    const batch = disk(path);
    const prior = batch.dispatches[0].calls[0];
    // Synthetic retained-history fixture: a later zero-POST refusal or interrupted
    // call cannot erase the receiver minimum attached to the actual first result.
    batch.dispatches[0].calls.push({ ordinal: 2, startedAt: prior.completedAt, postsStarted: 0,
      completedAt: incomplete ? null : prior.completedAt,
      result: incomplete ? null : { status: 'not_configured', deliveryConfirmed: false,
        reasonCode: 'INGEST_NOT_CONFIGURED', attempts: 0, expectedSpans: 2 } });
    writeFileSync(pathFor(path), JSON.stringify(batch), { mode: 0o600 });
    const fake = service();
    const [result] = await at(Date.parse(prior.completedAt) + 119_999,
      () => drainNeatlogsOutbox(options(path, fake)));
    assert.equal(result.batch.send.status, 'outcome_unknown');
    assert.equal(result.batch.send.retryAfterMinimumMs, undefined);
    assert.equal(fake.posts.length, 0);
    assert.deepEqual(result.batch.dispatches, batch.dispatches);
  }
});

test('a prior generation minimum survives a later zero-minimum result and clock rollback', async () => {
  const path = directory('generation-history');
  await drainNeatlogsOutbox(options(path, service(503, '120'), { readback: true }));
  const initial = disk(path);
  await at(minimumDeadline(initial),
    () => drainNeatlogsOutbox(options(path, service(429, '0'), { recover: true })));
  const recovered = disk(path);
  assert.equal(recovered.dispatches.length, 2);
  assert.equal(recovered.dispatches[1].calls[0].result.retryAfterMinimumMs, 0);
  const fake = service();
  await at(minimumDeadline(initial) - 1, () => drainNeatlogsOutbox(options(path, fake)));
  assert.equal(fake.posts.length, 0);
  assert.deepEqual(disk(path).dispatches, recovered.dispatches);
});

test('a saturated server minimum cannot overflow into an immediate retry', async () => {
  const path = directory('large-minimum');
  await drainNeatlogsOutbox(options(path, service(429, '999999999999999999999999999')));
  const batch = disk(path);
  assert.equal(batch.dispatches[0].calls[0].result.retryAfterMinimumMs, Number.MAX_SAFE_INTEGER);
  const fake = service();
  await at(Number.MAX_SAFE_INTEGER, () => drainNeatlogsOutbox(options(path, fake)));
  assert.equal(fake.posts.length, 0);
  assert.deepEqual(disk(path).dispatches, batch.dispatches);
});

test('missing, malformed and zero minimum headers retain immediate retry behavior', async () => {
  for (const [index, header] of [undefined, 'not-a-date', '0'].entries()) {
    const path = directory(`no-minimum-${index}`);
    const first = service(429, header);
    await drainNeatlogsOutbox(options(path, first));
    const fake = service();
    await drainNeatlogsOutbox(options(path, fake));
    assert.equal(fake.posts.length, 1);
    assert.ok(fake.posts[0].equals(first.posts[0]));
  }
});

test('configuration and tag conflicts still refuse before a deferred retry', async () => {
  const path = directory('conflicts');
  await drainNeatlogsOutbox(options(path, service(429, '120')));
  const fake = service();
  await assert.rejects(drainNeatlogsOutbox(options(path, fake, { tags: [...TAGS, 'fixture:changed'] })),
    { code: 'DISPATCH_TAG_CONFLICT' });
  const changed = loadNeatlogsConfig({ env: { ...ENV, NEATLOGS_PROJECT_ID: '00000000-0000-4000-8000-000000000002' } });
  await assert.rejects(drainNeatlogsOutbox(options(path, fake, { config: changed })),
    { code: 'DISPATCH_TARGET_CONFLICT' });
  assert.equal(fake.posts.length, 0);
});
