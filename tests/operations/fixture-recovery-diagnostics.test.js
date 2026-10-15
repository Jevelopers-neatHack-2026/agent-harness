import assert from 'node:assert/strict';
import { test } from 'node:test';
import { cleanupC04Fixture, encodeC04CompletionDiagnostic, observeC04PreCleanup,
  parseC04CompletionFromTap, projectC04WorkPage, throwC04CleanupErrors } from './fixture-recovery-diagnostics.js';

// Pure producer-shaped and hostile-input controls. No worker, native database,
// operations runner, provider or actual filesystem cleanup is started here.
const flags = { complete: false, missingChildCapture: false, malformed: false, overflow: false,
  writeFailed: false, abruptExit: true, cleanupIncomplete: true };
const session = (records = [], supervisor = []) => ({ schema: 'agent-harness.ops07-session.v1', flags,
  slots: [{ slot: 1, records, footer: true, writerComplete: true }], supervisor });
const tap = line => `TAP version 13\n    # ${line}\n    ok 4 - OPS07.C04 synthetic control\n1..1\n`;
function diagnostic(input = {}) {
  const line = encodeC04CompletionDiagnostic(input);
  assert.notEqual(line, null);
  assert.ok(Buffer.byteLength(line) <= 4_096);
  const parsed = parseC04CompletionFromTap(tap(line));
  assert.equal(parsed.valid, true, parsed.reason);
  assert.equal(parsed.line, line);
  return parsed.diagnostic;
}

test('C04 keeps the last matching work-page observation distinct from the later point read and cleanup', () => {
  const item = { jobId: 'private-job', state: 'running', generation: 2, reasonCode: null };
  const page = projectC04WorkPage(item);
  const preCleanup = observeC04PreCleanup({ observed: true, page, role: 'restart', lifecycle: 'accepting', activeChildCount: 1 });
  item.state = 'completed'; item.generation = 3;
  const result = diagnostic({ preCleanup, primaryFailed: true, cleanupComplete: true,
    row: { status: 'observed', workState: 'completed', workflowState: 'succeeded', reason: 'none', generation: 2,
      operationFence: 2, workflowFence: 2, checkpointSequence: 65, errorCode: 'none' } });
  assert.equal(result.preCleanup.timing, 'before_cleanup');
  assert.deepEqual(result.preCleanup.page, { status: 'observed', workState: 'running', generation: 2, reason: 'none' });
  assert.deepEqual(result.preCleanup.worker, { role: 'restart', lifecycle: 'accepting', activeChildCount: 1 });
  assert.equal(result.row.source, 'separate_point_lookup');
  assert.equal(result.row.timing, 'before_cleanup');
  assert.equal(result.row.workState, 'completed');
  assert.equal(result.capture.timing, 'not_observed');
  assert.doesNotMatch(JSON.stringify(result), /private-job/u);
  assert.equal(projectC04WorkPage(undefined).status, 'missing');
  assert.equal(observeC04PreCleanup(null).page.status, 'not_observed');
});

test('C04 recognizes actual runner stage records and keeps controller completion independent of child exit', () => {
  const capture = session([
    { stage: 'header' }, { stage: 'runner_phase', phase: 'runner_start' },
    { stage: 'store_open', ok: true }, { stage: 'resource_ceilings', ok: true }, { stage: 'claim_read', ok: true },
    { stage: 'admission_committed', ordinal: 0, checkpointSequence: 1 },
    { stage: 'completion_committed', ordinal: 0, checkpointSequence: 2 },
    { stage: 'controller_result', status: 'completed', reason: null, checkpointSequence: 65 },
    { stage: 'store_close', ok: true }, { stage: 'footer', writerComplete: true }
  ], [{ slot: 1, stage: 'child_exit', exitCode: 0, signal: null },
    { slot: 1, stage: 'finish_result', outcome: 'completed', settled: true, code: null }]);
  const result = diagnostic({ capture, captureTiming: 'after_owned_child_reap' });
  const runner = result.capture.slots[1];
  assert.equal(runner.lastPhase, 'store_close');
  assert.equal(runner.controllerStatus, 'completed');
  assert.equal(runner.controllerReason, 'none');
  assert.equal(runner.controllerCheckpointSequence, 65);
  assert.equal(runner.exitCode, 0);
  assert.equal(runner.finishSettled, true);
  assert.equal(runner.errorCount, 0);
  assert.equal(result.capture.timing, 'after_owned_child_reap');
  assert.equal(result.capture.abruptExit, true);
  assert.equal(result.capture.sessionCleanupIncomplete, true);
  assert.equal(Object.hasOwn(result.capture, 'complete'), false);
  assert.equal(diagnostic({ capture: session([], capture.supervisor) }).capture.slots[1].controllerStatus, 'unobserved');
});

test('C04 preserves actual controller errors and deadline and unsettled finishes after later successful cleanup', () => {
  const capture = session([
    { stage: 'runner_phase', phase: 'runner_start' }, { stage: 'store_open', ok: true },
    { stage: 'controller_error', phase: 'completion', code: 'STORE_BUSY', primaryCode: 5 },
    { stage: 'controller_result', status: 'stopped', reason: 'LOCAL_COMPLETION_BUSY', checkpointSequence: 64 },
    { stage: 'runner_error', phase: 'store_close', code: 'STORE_WRITE_FAILED', primaryCode: null },
    { stage: 'store_close', ok: false }
  ], [{ slot: 1, stage: 'deadline', generation: 2, fence: 2 },
    { slot: 1, stage: 'child_exit', exitCode: 2, signal: null },
    { slot: 1, stage: 'finish_result', outcome: 'stopped', settled: false, code: 'STORE_BUSY' },
    { slot: 1, stage: 'finish_result', outcome: 'stopped', settled: false, code: 'STORE_OPEN_FAILED' },
    { slot: 1, stage: 'finish_result', outcome: 'stopped', settled: false, code: 'STORE_WRITE_FAILED' },
    { slot: 1, stage: 'finish_result', outcome: 'stopped', settled: true, code: null }]);
  const runner = diagnostic({ capture }).capture.slots[1];
  assert.equal(runner.lastPhase, 'store_close');
  assert.deepEqual(runner.errors, [
    { stage: 'controller_error', phase: 'completion', code: 'STORE_BUSY', sqlitePrimaryCode: 5 },
    { stage: 'runner_error', phase: 'store_close', code: 'STORE_WRITE_FAILED', sqlitePrimaryCode: null }
  ]);
  assert.equal(runner.errorsTruncated, false);
  assert.equal(runner.controllerReason, 'LOCAL_COMPLETION_BUSY');
  assert.equal(runner.controllerCheckpointSequence, 64);
  assert.equal(runner.deadlineObserved, true);
  assert.equal(runner.finishSettled, true);
  assert.equal(runner.unsettledFinishCount, 3);
  assert.equal(runner.unsettledFinishTruncated, true);
  assert.deepEqual(runner.unsettledFinishes, [{ outcome: 'stopped', code: 'STORE_BUSY' },
    { outcome: 'stopped', code: 'STORE_WRITE_FAILED' }]);
});

test('C04 retains startup and supervisor failures without inventing a phase for phase-free storage errors', () => {
  const capture = session([{ stage: 'storage_origin', operation: 'open', code: 'STORE_OPEN_FAILED', primaryCode: 14 }],
    [{ slot: 1, stage: 'spawn_error', code: 'OTHER' }, { slot: 1, stage: 'runner_identity_failure', code: 'STALE_FENCE' }]);
  const runner = diagnostic({ capture }).capture.slots[1];
  assert.equal(runner.lastPhase, 'unobserved');
  assert.equal(runner.errorCount, 3);
  assert.equal(runner.errorsTruncated, true);
  assert.equal(runner.errors[0].phase, 'unobserved');
  assert.equal(runner.errors[0].code, 'STORE_OPEN_FAILED');
  assert.equal(runner.errors[1].stage, 'runner_identity_failure');
  assert.equal(runner.errors[1].code, 'STALE_FENCE');
  assert.equal(runner.exitObserved, false);
  assert.equal(runner.finishSettled, null);
});

test('C04 stays within 4 KiB at the original source limits and labels every truncated failure history', () => {
  const records = Array.from({ length: 100 }, () => ({ stage: 'controller_error', phase: 'completion_resolution',
    code: 'UNSUPPORTED_SCHEMA_VERSION', primaryCode: 255 }));
  const capture = { schema: 'agent-harness.ops07-session.v1', flags: { ...flags, overflow: true },
    slots: [0, 1].map(slot => ({ slot, records, footer: false, writerComplete: false })),
    supervisor: Array.from({ length: 100 }, (_, index) => ({ slot: index % 2, stage: 'finish_result',
      outcome: 'completed', settled: false, code: 'UNSUPPORTED_SCHEMA_VERSION' })) };
  const max = Number.MAX_SAFE_INTEGER;
  const result = diagnostic({ capture, captureTiming: 'after_owned_child_reap',
    preCleanup: observeC04PreCleanup({ observed: true, page: projectC04WorkPage({ state: 'completed', generation: max,
      reasonCode: 'EXTERNAL_OUTCOME_UNRESOLVED' }), role: 'restart', lifecycle: 'saturated', activeChildCount: max }),
    row: { status: 'observed', workState: 'completed', workflowState: 'reconciliation_pending',
      reason: 'EXTERNAL_OUTCOME_UNRESOLVED', generation: max, operationFence: max, workflowFence: max,
      checkpointSequence: max, errorCode: 'UNSUPPORTED_SCHEMA_VERSION' } });
  assert.equal(result.capture.overflow, true);
  for (const runner of result.capture.slots) {
    assert.equal(runner.recordsObserved, 100);
    assert.equal(runner.errorCount, 100);
    assert.equal(runner.errorsTruncated, true);
    assert.equal(runner.unsettledFinishCount, 50);
    assert.equal(runner.unsettledFinishTruncated, true);
  }
});

test('C04 full TAP parser rejects absent duplicate truncated oversized malformed and noncanonical frames', () => {
  const line = encodeC04CompletionDiagnostic({});
  assert.equal(parseC04CompletionFromTap('TAP version 13\n').reason, 'missing');
  assert.equal(parseC04CompletionFromTap(null).reason, 'invalid_input');
  assert.equal(parseC04CompletionFromTap(`${tap(line)}# ${line}\n`).reason, 'duplicate');
  assert.equal(parseC04CompletionFromTap(tap(line).slice(0, -1)).reason, 'truncated');
  assert.equal(parseC04CompletionFromTap(tap(`${line} `)).reason, 'noncanonical');
  assert.equal(parseC04CompletionFromTap(tap(line.replace('{', '{ '))).reason, 'noncanonical');
  assert.equal(parseC04CompletionFromTap(tap(line.replace('"diagnosticOnly":true', '"diagnosticOnly":true,"diagnosticOnly":true'))).reason, 'noncanonical');
  assert.equal(parseC04CompletionFromTap(tap(line.slice(0, -1))).reason, 'malformed');
  assert.equal(parseC04CompletionFromTap(tap(`${line}${' '.repeat(4_096)}`)).reason, 'oversize');
  assert.equal(parseC04CompletionFromTap(tap(line.replace('"diagnosticOnly":true', '"diagnosticOnly":false'))).reason, 'invalid_shape');
  assert.equal(parseC04CompletionFromTap(tap(line).replace(/\n/gu, '\r\n')).valid, true);
});

test('C04 full TAP parser rejects every malformed or unknown reserved-family sibling beside a valid frame', () => {
  const line = encodeC04CompletionDiagnostic({});
  for (const sibling of ['# OPS07_C04_COMPLETION_V2 {}', '# OPS07_C04_COMPLETION_V',
    '# OPS07_C04_COMPLETION_OTHER {}', '# OPS07_C04_COMPLETION_V1{}',
    '\t# OPS07_C04_COMPLETION_V1 {}', '#OPS07_C04_COMPLETION_V1 {}',
    '                 # OPS07_C04_COMPLETION_V1 {}']) {
    assert.equal(parseC04CompletionFromTap(`${tap(line)}${sibling}\n`).valid, false);
    assert.equal(parseC04CompletionFromTap(`${sibling}\n`).valid, false);
  }
});

test('C04 parser rejects unknown private keys invalid enums and false history completeness', () => {
  const original = diagnostic();
  const mutate = change => {
    const copy = structuredClone(original); change(copy);
    return parseC04CompletionFromTap(tap(`OPS07_C04_COMPLETION_V1 ${JSON.stringify(copy)}`));
  };
  for (const change of [value => { value.path = '/private-canary'; },
    value => { value.preCleanup.worker.lifecycle = 'private-canary'; },
    value => { value.capture.complete = true; }, value => { value.capture.slots[0].slot = 1; },
    value => { value.capture.slots[0].errorCount = 3; },
    value => { value.capture.slots[0].unsettledFinishTruncated = true; },
    value => { value.row.generation = '2'; }, value => { value.row.reason = 'private-canary'; }]) {
    const rejected = mutate(change);
    assert.equal(rejected.valid, false); assert.equal(rejected.reason, 'invalid_shape');
    assert.equal(rejected.diagnostic, null); assert.equal(rejected.line, null);
    assert.doesNotMatch(JSON.stringify(rejected), /private-canary/u);
  }
});

test('C04 producer projection does not execute hostile properties and preserves missing capture uncertainty', () => {
  let reads = 0;
  const proxy = Proxy.revocable({}, {}); proxy.revoke();
  const poisoned = { get preCleanup() { reads += 1; throw new Error('private-canary'); },
    get capture() { reads += 1; throw new Error('private-canary'); } };
  for (const input of [poisoned, proxy.proxy, { capture: { slots: new Array(3), supervisor: new Array(101) } }]) {
    const result = diagnostic(input);
    assert.equal(result.capture.available, false);
    assert.equal(result.capture.malformed, true);
    assert.equal(result.capture.missingChildCapture, true);
    assert.equal(result.capture.abruptExit, true);
  }
  assert.equal(reads, 0);
});

test('C04 cleanup captures only after reap and reports completion after descriptor handle and directory cleanup', async () => {
  const events = [];
  const result = await cleanupC04Fixture({ workers: [{ lifecycle: 'accepting', stop: async () => {
    events.push('reap'); return { stopped: true, childrenReaped: true, storeClosed: true };
  } }], handles: [{ close: () => { events.push('handle'); } }],
  readCapture: () => { events.push('capture'); return session(); },
  disposeCapture: () => { events.push('descriptor'); return true; }, removeDirectory: () => { events.push('directory'); } });
  assert.deepEqual(events, ['reap', 'capture', 'descriptor', 'handle', 'directory']);
  assert.equal(result.cleanupComplete, true);
  assert.equal(result.captureTiming, 'after_owned_child_reap');
  assert.deepEqual(result.cleanupErrors, []);
});

test('C04 cleanup retains evidence on unconfirmed reap while attempting every other owned resource close', async () => {
  const events = [];
  const result = await cleanupC04Fixture({ workers: [{ lifecycle: 'degraded', stop: async () => ({ stopped: false,
    childrenReaped: false, storeClosed: false }) }], handles: [{ close: () => events.push('first') }, { close: () => events.push('second') }],
  readCapture: () => { assert.fail('unreaped capture'); }, disposeCapture: () => { events.push('descriptor'); return true; },
  removeDirectory: () => { assert.fail('unconfirmed directory removal'); } });
  assert.equal(result.cleanupComplete, false);
  assert.equal(result.capture, null);
  assert.equal(result.captureTiming, 'not_observed');
  assert.deepEqual(events, ['descriptor', 'first', 'second']);
  assert.equal(result.cleanupErrors.length, 1);
});

test('C04 cleanup preserves every failure including falsey errors and never claims success before removal', async () => {
  const handleFailure = new Error('synthetic close');
  const result = await cleanupC04Fixture({ workers: [{ lifecycle: 'accepting', stop: async () => { throw undefined; } }],
    handles: [{ close: () => { throw handleFailure; } }, { close() {} }],
    readCapture: () => assert.fail('unreaped capture'), disposeCapture: () => { throw null; },
    removeDirectory: () => assert.fail('unconfirmed directory removal') });
  assert.equal(result.cleanupComplete, false);
  assert.deepEqual(result.cleanupErrors, [undefined, null, handleFailure]);
  const removal = new Error('synthetic removal');
  const removed = await cleanupC04Fixture({ workers: [], handles: [], readCapture: () => session(),
    disposeCapture: () => true, removeDirectory: () => { throw removal; } });
  assert.equal(removed.cleanupComplete, false);
  assert.deepEqual(removed.cleanupErrors, [removal]);
  assert.equal(diagnostic(removed).cleanupComplete, false);
  const captureFailure = new Error('synthetic read');
  const unread = await cleanupC04Fixture({ workers: [], handles: [], readCapture: () => { throw captureFailure; },
    disposeCapture: () => true, removeDirectory: () => assert.fail('lost unread evidence') });
  assert.equal(unread.cleanupComplete, false);
  assert.deepEqual(unread.cleanupErrors, [captureFailure]);
});

test('C04 cleanup aggregation keeps the original thrown identity and each cleanup error without changing success', () => {
  const cleanup = new Error('synthetic cleanup');
  for (const primaryError of [new Error('synthetic primary'), undefined, null, false, 0]) {
    assert.throws(() => throwC04CleanupErrors({ primaryFailed: true, primaryError, cleanupErrors: [cleanup, undefined] }),
      error => error instanceof AggregateError && error.errors[0] === primaryError && error.cause === primaryError
        && error.errors[1] === cleanup && error.errors.length === 3);
  }
  assert.throws(() => throwC04CleanupErrors({ primaryFailed: false, cleanupErrors: [cleanup] }),
    error => error instanceof AggregateError && error.errors[0] === cleanup && !Object.hasOwn(error, 'cause'));
  assert.doesNotThrow(() => throwC04CleanupErrors({ primaryFailed: false, cleanupErrors: [] }));
});
