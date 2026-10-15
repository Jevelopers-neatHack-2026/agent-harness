import assert from 'node:assert/strict';
import { test } from 'node:test';
import { EventEmitter } from 'node:events';
import { classifyLocalControllerClockDiagnostic, readLocalControllerClockDiagnostic } from '../../src/internal/durable-store.js';
import { runtimeError } from '../../src/internal/errors.js';
import { appendClosedDiagnostic, closedErrorProjection, closedExitDisposition, closedWorkObservation,
  createClosedDiagnosticChannel, encodeClosedDiagnostic, makeClosedDiagnostic,
  observeWorkerCleanup, parseClosedChildDiagnostic, parseClosedDiagnosticChannel, observeClosedChild,
  closedChildObservation, requiresClosedFixtureRetention, publishClosedDiagnostic, publishPrimaryThenCleanup,
  waitForClosedChild } from './closed-diagnostics.js';

// Source declarations only in this packet. No native store/process/provider is started by these controls.
const fixture = overrides => ({ caseId: 'OPS05.C12', phase: 'claim', ...overrides });
const replaceLine = (channel, lines) => ({ ...channel, lines, bytes: Buffer.byteLength(lines, 'utf8') });

test('closed clock classifier separates deadline age both and other without tagging arbitrary errors', () => {
  assert.equal(classifyLocalControllerClockDiagnostic(true, false), 'deadline_reached');
  assert.equal(classifyLocalControllerClockDiagnostic(false, true), 'snapshot_age_exceeded');
  assert.equal(classifyLocalControllerClockDiagnostic(true, true), 'deadline_and_age');
  assert.equal(classifyLocalControllerClockDiagnostic(false, false), 'other');
  assert.equal(classifyLocalControllerClockDiagnostic({}, true), 'other');
  const arbitrary = runtimeError({ module: 'state', operation: '_localControllerComplete',
    code: 'INVALID_INPUT', safe_message: 'synthetic', retryable: false });
  assert.equal(readLocalControllerClockDiagnostic(arbitrary), null);
  assert.equal(readLocalControllerClockDiagnostic({ code: 'INVALID_INPUT', localClockGuard: 'deadline_reached' }), null);
  assert.equal(readLocalControllerClockDiagnostic(new Proxy(arbitrary, {})), null);
});

test('closed record distinguishes process close from observed native close and keeps primary cleanup separate', () => {
  const record = makeClosedDiagnostic(fixture({ processClosed: true, nativeClose: 'failed',
    primaryFailed: true, ...closedErrorProjection(null), cleanupFailed: true, cleanupKind: 'other',
    cleanupCode: 'OTHER', cleanupPrimaryCode: null }));
  assert.equal(record.processClosed, true);
  assert.equal(record.nativeClose, 'failed');
  assert.equal(record.primaryFailed, true);
  assert.equal(record.cleanupFailed, true);
  assert.equal(record.errorKind, 'other');
  assert.equal(record.cleanupKind, 'other');
});

test('closed native projection uses only native own code and errcode and never reads arbitrary code getters', () => {
  const native = new Error('never published');
  native.code = 'ERR_SQLITE_ERROR'; native.errcode = 261;
  assert.deepEqual(closedErrorProjection(native), { errorKind: 'native_sqlite', code: 'ERR_SQLITE_ERROR', sqlitePrimaryCode: 5 });
  let reads = 0;
  assert.equal(closedErrorProjection({ get code() { reads += 1; throw null; } }).errorKind, 'other');
  assert.equal(reads, 0);
  assert.equal(closedErrorProjection(new Proxy(native, { get() { reads += 1; throw null; } })).errorKind, 'other');
  assert.equal(reads, 0);
});

test('closed restart observation reuses the read state and rejects unlisted reasons without a new read', () => {
  assert.deepEqual(closedWorkObservation({ state: 'parked', reasonCode: 'CONTROLLER_INPUT_INVALID' }),
    { lastState: 'parked', lastReason: 'CONTROLLER_INPUT_INVALID' });
  assert.deepEqual(closedWorkObservation({ state: 'secret state', reasonCode: 'secret reason' }),
    { lastState: 'unobserved', lastReason: 'OTHER' });
  assert.deepEqual(closedWorkObservation(undefined), { lastState: 'unobserved', lastReason: 'none' });
});

test('closed producer child parser and artifact channel retain one complete finite record', () => {
  const line = encodeClosedDiagnostic(fixture({ nativeClose: 'confirmed', resultPresent: true }));
  assert.ok(Buffer.byteLength(line, 'utf8') <= 768);
  assert.equal(parseClosedChildDiagnostic(line, 'OPS05.C12').nativeClose, 'confirmed');
  const channel = createClosedDiagnosticChannel();
  assert.equal(appendClosedDiagnostic(channel, fixture()), true);
  assert.equal(appendClosedDiagnostic(channel, fixture({ phase: 'result' })), true);
  const parsed = parseClosedDiagnosticChannel(channel);
  assert.equal(parsed.complete, true);
  assert.equal(parsed.recordCount, 2);
  assert.deepEqual(parsed.records.map(record => record.sequence), [1, 2]);
  assert.equal(JSON.stringify(parsed).includes('never published'), false);
});

test('closed records refuse foreign keys hostile phases counters getters and false complete flags', () => {
  for (const input of [fixture({ path: '/private/data' }), fixture({ phase: 'unlisted' }),
    fixture({ caseId: {} }), fixture({ actual: 65 }), fixture({ cleanupFailed: 'true' })]) {
    assert.equal(makeClosedDiagnostic(input), null);
  }
  let reads = 0;
  assert.equal(makeClosedDiagnostic({ caseId: 'OPS05.C12', get phase() { reads += 1; throw undefined; } }), null);
  assert.equal(reads, 0);
  const channel = createClosedDiagnosticChannel();
  assert.equal(appendClosedDiagnostic(channel, { get phase() { reads += 1; return 'claim'; } }), false);
  assert.equal(reads, 0);
  assert.equal(parseClosedDiagnosticChannel(channel).complete, false);
});

test('closed channel refuses missing duplicate truncated reordered and noncanonical records', () => {
  const channel = createClosedDiagnosticChannel();
  appendClosedDiagnostic(channel, fixture());
  appendClosedDiagnostic(channel, fixture({ phase: 'result' }));
  const lines = channel.lines.trimEnd().split('\n');
  for (const text of [`${lines[1]}\n${lines[0]}\n`, `${lines[0]}\n${lines[0]}\n`,
    channel.lines.slice(0, -1), `${lines[0]}\n`]) {
    assert.equal(parseClosedDiagnosticChannel(replaceLine(channel, text)).complete, false);
  }
  assert.equal(parseClosedChildDiagnostic(encodeClosedDiagnostic(fixture()).slice(0, -1), 'OPS05.C12'), null);
  const duplicatedKey = encodeClosedDiagnostic(fixture()).replace('"slot":0', '"slot":0,"slot":0');
  assert.equal(parseClosedChildDiagnostic(duplicatedKey, 'OPS05.C12'), null);
  assert.equal(parseClosedChildDiagnostic('', 'OPS05.C12'), null);
});

test('closed channel overflow remains incomplete and does not increase the fixed bounds', () => {
  const channel = createClosedDiagnosticChannel();
  for (let index = 0; index < 40; index += 1) appendClosedDiagnostic(channel, fixture());
  assert.ok(channel.count <= 32);
  assert.ok(channel.bytes <= 16_384);
  assert.equal(channel.diagnosticsComplete, false);
  assert.equal(parseClosedDiagnosticChannel(channel).complete, false);
});

test('closed exit dispositions retain zero nonzero signal and missing distinctions', () => {
  assert.equal(closedExitDisposition({ code: 0, signal: null }), 'zero');
  assert.equal(closedExitDisposition({ code: 1, signal: null }), 'nonzero');
  assert.equal(closedExitDisposition({ code: null, signal: 'SIGKILL' }), 'signal');
  assert.equal(closedExitDisposition(null), 'unknown');
});

test('cleanup mock separates confirmed store close from incomplete worker child cleanup', async () => {
  assert.deepEqual(await observeWorkerCleanup({ stop: async () => ({ stopped: true, storeClosed: true, childrenReaped: true }) }, 'test'),
    { nativeClose: 'confirmed', cleanupFailed: false });
  const partial = await observeWorkerCleanup({ stop: async () => ({ stopped: true, storeClosed: true, childrenReaped: false }) }, 'test');
  assert.equal(partial.nativeClose, 'confirmed');
  assert.equal(partial.cleanupFailed, true);
});

test('cleanup mock preserves falsey close failure as separate uncertainty', async () => {
  for (const thrown of [undefined, null]) {
    const detail = await observeWorkerCleanup({ stop: async () => { throw thrown; } }, 'test');
    assert.equal(detail.nativeClose, 'failed');
    assert.equal(detail.cleanupFailed, true);
    assert.equal(detail.cleanupKind, 'other');
  }
});

test('cleanup mock timeout retains uncertainty and does not cancel or pretend to join the worker', async () => {
  let resolveStop;
  const stop = new Promise(resolve => { resolveStop = resolve; });
  const detail = await observeWorkerCleanup({ stop: () => stop }, 'test');
  assert.equal(detail.nativeClose, 'unknown');
  assert.equal(detail.cleanupFailed, true);
  resolveStop({ stopped: true, storeClosed: true, childrenReaped: true });
});

const mockChild = () => {
  const child = new EventEmitter();
  child.stdout = new EventEmitter(); child.stderr = new EventEmitter();
  return child;
};

test('actual observation does not convert error notification output or exit into close or native proof', async () => {
  const child = mockChild();
  const heldError = new Error('private mock error');
  const observation = observeClosedChild(child);
  let settled = false;
  void observation.closed.then(() => { settled = true; });
  child.stdout.emit('data', Buffer.from('unrelated \"event\":\"restarted\" output'));
  child.emit('error', heldError);
  child.emit('exit', 0, null);
  await Promise.resolve();
  assert.equal(settled, false);
  assert.equal(observation.snapshot().error, heldError);
  const early = closedChildObservation(observation.snapshot(), true, false);
  assert.equal(early.processClosed, false);
  assert.equal(early.errorEvent, true);
  assert.equal(early.pipeEOF, 'unobserved');
  assert.equal(early.outputPresent, true);
  assert.equal(early.resultPresent, false);
  assert.equal(requiresClosedFixtureRetention(observation.snapshot(), 'confirmed', false), true);
  child.stdout.emit('end'); child.stderr.emit('end'); child.emit('close', 0, null);
  const exit = await observation.closed;
  assert.equal(exit.processClosed, true);
  assert.equal(closedChildObservation(exit).pipeEOF, 'complete');
  assert.equal(requiresClosedFixtureRetention(exit, 'confirmed', false), true, 'error notification remains separately observed');
});

test('actual close and both pipe EOFs permit confirmed retention disposition only with separate native observation', async () => {
  const child = mockChild(); const observation = observeClosedChild(child);
  child.stdout.emit('end'); child.stderr.emit('end'); child.emit('close', 0, null);
  const exit = await observation.closed;
  assert.equal(requiresClosedFixtureRetention(exit, 'confirmed', false), false);
  assert.equal(requiresClosedFixtureRetention(exit, 'unknown', false), true);
  assert.equal(requiresClosedFixtureRetention(exit, 'confirmed', true), true);
  const missingEof = mockChild(); const incomplete = observeClosedChild(missingEof);
  missingEof.stdout.emit('end'); missingEof.emit('close', 0, null);
  assert.equal(closedChildObservation(await incomplete.closed).pipeEOF, 'incomplete');
  assert.equal(requiresClosedFixtureRetention(incomplete.snapshot(), 'confirmed', false), true);
});

test('genuine shared close wait times out an unresolved reap and retains before any assertion', async () => {
  const child = mockChild(); const observation = observeClosedChild(child);
  const exit = await waitForClosedChild(observation.closed, 1);
  let retained = requiresClosedFixtureRetention(exit, 'confirmed', false);
  assert.equal(retained, true);
  assert.throws(() => assert.ok(exit));
  child.stdout.emit('end'); child.stderr.emit('end'); child.emit('close', null, 'SIGKILL');
  const reaped = await observation.closed;
  // A later actual reap is consumed but cannot erase the earlier uncertainty or prove native closure.
  retained ||= requiresClosedFixtureRetention(reaped, 'unknown', true);
  assert.equal(retained, true);
  assert.equal(closedChildObservation(reaped).exitDisposition, 'signal');
});

test('primary frame is available before deliberately held cleanup and final frame binds the same primary facts', async () => {
  const lines = []; let release; let cleanupInvoked = false;
  const held = new Promise(resolve => { release = resolve; });
  const primary = { caseId: 'OPS06.C01', phase: 'restart_wait', reason: 'wait_deadline',
    lastState: 'parked', lastReason: 'CONTROLLER_INPUT_INVALID', primaryFailed: true,
    ...closedErrorProjection(new Error('private primary')) };
  const pending = publishPrimaryThenCleanup(line => lines.push(line), primary, () => {
    cleanupInvoked = true;
    assert.equal(lines.length, 1, 'primary publication precedes invocation, not just awaiting cleanup');
    return held;
  });
  assert.equal(cleanupInvoked, true);
  const early = parseClosedChildDiagnostic(lines.join(''), 'OPS06.C01');
  assert.equal(early.reason, 'wait_deadline'); assert.equal(early.lastState, 'parked');
  assert.equal(early.nativeClose, 'unknown'); assert.equal(early.cleanupFailed, true);
  release({ nativeClose: 'confirmed', cleanupFailed: false });
  const result = await pending;
  assert.equal(result.primaryPublication.failed, false);
  const complete = parseClosedChildDiagnostic(lines.join(''), 'OPS06.C01');
  assert.equal(complete.nativeClose, 'confirmed'); assert.equal(complete.primaryFailed, true);
  const wrongReason = lines[1].replace('"reason":"wait_deadline"', '"reason":"read_failure"');
  assert.equal(parseClosedChildDiagnostic(lines[0] + wrongReason, 'OPS06.C01'), null);
  const wrongSlot = lines[1].replace('"slot":0', '"slot":1');
  assert.equal(parseClosedChildDiagnostic(lines[0] + wrongSlot, 'OPS06.C01'), null);
  const wrongResult = lines[1].replace('"resultPresent":false', '"resultPresent":true');
  assert.equal(parseClosedChildDiagnostic(lines[0] + wrongResult, 'OPS06.C01'), null);
  assert.equal(parseClosedChildDiagnostic(lines[0] + lines[1] + lines[1], 'OPS06.C01'), null);
  assert.equal(parseClosedChildDiagnostic(lines[1], 'OPS06.C01'), null);
});

test('publication and falsey cleanup failures stay separate from the held primary object', async () => {
  const heldPrimary = new Error('private primary identity');
  const projection = closedErrorProjection(heldPrimary);
  for (const failure of [undefined, null, false, 0]) {
    const publication = publishClosedDiagnostic(() => { throw failure; }, fixture());
    assert.equal(publication.failed, true); assert.equal(publication.error, failure);
    const lines = [];
    const result = await publishPrimaryThenCleanup(line => lines.push(line), {
      caseId: 'OPS06.C01', phase: 'restart_verify', primaryFailed: true, ...projection
    }, () => { throw failure; });
    const record = parseClosedChildDiagnostic(lines.join(''), 'OPS06.C01');
    assert.equal(record.primaryFailed, true); assert.equal(record.errorKind, projection.errorKind);
    assert.equal(record.nativeClose, 'failed'); assert.equal(record.cleanupFailed, true);
    assert.equal(result.cleanup.cleanupKind, 'other');
    assert.equal(heldPrimary.message, 'private primary identity');
  }
});

test('expected result observation uses the original parse and identity assertions rather than output markers', () => {
  const exit = { processClosed: true, errorEvent: false, stdoutEOF: true, stderrEOF: true, code: 0, signal: null };
  const expected = { ownerId: 'expected-owner', claim: { jobId: 'expected-job', generation: 2, fence: 2 },
    incomplete: false, ownedRunnerCount: 0 };
  const observeResult = text => {
    let valid = false;
    try { assert.deepEqual(JSON.parse(text), expected); valid = true; } catch {}
    return closedChildObservation(exit, text.length > 0, valid);
  };
  for (const text of ['{', 'unrelated "event":"restarted"',
    JSON.stringify({ ...expected, ownerId: 'wrong-owner' }), JSON.stringify({ event: 'wrong-event' })]) {
    const observed = observeResult(text);
    assert.equal(observed.outputPresent, true); assert.equal(observed.resultPresent, false);
    const channel = createClosedDiagnosticChannel();
    assert.equal(appendClosedDiagnostic(channel, fixture(observed)), true);
    assert.equal(parseClosedDiagnosticChannel(channel).records[0].resultPresent, false);
  }
  assert.equal(observeResult(JSON.stringify(expected)).resultPresent, true);
});

test('actual producer and channel parser reject ordinary and revoked proxies without traps getters or throws', () => {
  let traps = 0; let getters = 0;
  const handler = { get() { traps += 1; throw null; }, ownKeys() { traps += 1; throw undefined; },
    getPrototypeOf() { traps += 1; throw false; }, getOwnPropertyDescriptor() { traps += 1; throw 0; } };
  const ordinary = new Proxy(fixture(), handler);
  const revocable = Proxy.revocable(fixture(), handler); revocable.revoke();
  const getter = { caseId: 'OPS05.C12', get phase() { getters += 1; throw undefined; } };
  for (const hostile of [ordinary, revocable.proxy, getter]) {
    assert.equal(makeClosedDiagnostic(hostile), null);
    assert.equal(encodeClosedDiagnostic(hostile), null);
    const channel = createClosedDiagnosticChannel();
    assert.equal(appendClosedDiagnostic(channel, hostile), false);
    assert.equal(channel.diagnosticsComplete, false);
    assert.equal(parseClosedDiagnosticChannel(hostile).complete, false);
    assert.equal(appendClosedDiagnostic(hostile, fixture()), false);
    assert.equal(parseClosedChildDiagnostic(hostile, 'OPS05.C12'), null);
  }
  const channelGetter = { get lines() { getters += 1; throw null; }, count: 0, bytes: 0, diagnosticsComplete: true };
  assert.equal(parseClosedDiagnosticChannel(channelGetter).complete, false);
  assert.equal(traps, 0); assert.equal(getters, 0);
});

test('primary publication rejects hostile records while still invoking owned cleanup without traps', async () => {
  let traps = 0;
  const revocable = Proxy.revocable({}, { ownKeys() { traps += 1; throw null; }, get() { traps += 1; throw undefined; } });
  revocable.revoke();
  let cleanupCalls = 0;
  let writes = 0;
  const result = await publishPrimaryThenCleanup(() => { writes += 1; }, revocable.proxy, async () => {
    cleanupCalls += 1;
    return { nativeClose: 'confirmed', cleanupFailed: false };
  });
  assert.equal(result.primaryPublication.failed, true);
  assert.equal(result.cleanupPublication.failed, true);
  assert.equal(cleanupCalls, 1);
  assert.equal(writes, 0);
  assert.equal(traps, 0);
});
