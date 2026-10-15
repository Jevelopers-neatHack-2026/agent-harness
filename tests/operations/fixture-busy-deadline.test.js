import assert from 'node:assert/strict';
import { test } from 'node:test';
import { encodeFixtureRetryDiagnostic, parseFixtureRetryDiagnostic,
  runFixtureBusyRetry } from './fixture-busy-retry.js';

// These controls exercise timing qualification with synchronous synthetic
// outcomes. They issue no SQLite statement and claim no native retry proof.
const MS = 1_000_000n;
const primary = Object.freeze(Object.assign(new Error('Synthetic call failure.'), { code: 'STORE_WRITE_FAILED' }));
const calls = [
  { name: 'claim', kind: 'claim', value: Object.freeze({ claim: Object.freeze({ jobId: 'synthetic-claim' }) }), outcome: 'claim' },
  { name: 'empty claim', kind: 'claim', value: null, outcome: 'empty' },
  { name: 'admission', kind: 'admission', value: Object.freeze({ accepted: true, jobId: 'synthetic-admission' }), outcome: 'accepted' },
  { name: 'refusal', kind: 'admission', value: Object.freeze({ accepted: false,
    code: 'OPS_BACKPRESSURE', reasonCode: 'STORE_BUSY', retryAfterMs: 40 }), outcome: 'refused' },
  { name: 'thrown error', kind: 'claim', throws: true, error: primary, outcome: 'error' }
];
const windows = [
  { name: 'supplied deadline', deadlineNs: 100n * MS, boundNs: 100n * MS },
  { name: 'original 1500 ms cap', deadlineNs: 5_000n * MS, boundNs: 1_500n * MS }
];

async function observed(call, { deadlineNs = 500n * MS, startNs = 0n,
  finishedNs = 50n * MS, clockAfter } = {}) {
  let invokeCount = 0;
  let sleepCount = 0;
  const result = await runFixtureBusyRetry({
    kind: call.kind, deadlineNs, busyTimeoutMs: 1, delayMs: 5,
    clock: () => {
      return invokeCount === 0 ? startNs : clockAfter ? clockAfter() : finishedNs;
    },
    invoke: () => {
      invokeCount += 1;
      if (call.throws) throw call.error;
      return call.value;
    },
    sleep: async () => { sleepCount += 1; }
  });
  assert.equal(invokeCount, 1, 'qualification never replays the observed call');
  assert.equal(sleepCount, 0, 'qualification adds no wait or recovery window');
  assert.equal(result.value, call.value, 'the actual successful or refused value retains identity');
  if (call.throws) assert.equal(result.error, call.error, 'a timing failure does not replace the original primary');
  assert.equal(result.diagnostic.attempts.length, 1);
  const attempt = result.diagnostic.attempts[0];
  assert.equal(attempt.outcome, call.outcome, 'history retains the actual call outcome');
  assert.equal(attempt.code, call.throws ? call.error?.code ?? 'none'
    : call.kind === 'admission' ? call.value?.code ?? 'none' : 'none');
  assert.equal(attempt.retryScheduled, false);
  assert.equal(attempt.delayMs, 0);
  assert.equal(attempt.eligible, false, 'synthetic outcomes have no native BEGIN proof');
  const line = encodeFixtureRetryDiagnostic(result.diagnostic);
  assert.equal(typeof line, 'string');
  assert.deepEqual(parseFixtureRetryDiagnostic(line), result.diagnostic, 'the closed history retains timing disposition');
  return result;
}

for (const window of windows) {
  for (const call of calls) {
    test(`${call.name} after ${window.name} is nonqualifying without losing outcome or replaying`, async () => {
      const result = await observed(call, { deadlineNs: window.deadlineNs, finishedNs: window.boundNs + 1n });
      assert.equal(result.failed, true);
      assert.equal(result.diagnostic.stopReason, 'deadline_after_call');
      assert.ok(result.qualificationError instanceof Error);
      if (!call.throws) assert.equal(result.error, result.qualificationError,
        'a successful/refused call supplies a real qualification error to the child');
    });
  }
}

test('the exact original boundary fails while one nanosecond before it remains qualified', async () => {
  for (const window of windows) {
    for (const call of [calls[0], calls[2]]) {
      const before = await observed(call, { deadlineNs: window.deadlineNs, finishedNs: window.boundNs - 1n });
      assert.equal(before.failed, false);
      assert.equal(before.error, undefined);
      assert.equal(before.qualificationError, undefined);
      assert.equal(before.diagnostic.stopReason, 'success');
      const exact = await observed(call, { deadlineNs: window.deadlineNs, finishedNs: window.boundNs });
      assert.equal(exact.failed, true);
      assert.equal(exact.diagnostic.stopReason, 'deadline_after_call');
      assert.equal(exact.error, exact.qualificationError);
    }
  }
});

test('in-budget success, refusal and failure preserve their original classification and identity', async () => {
  for (const call of calls) {
    const result = await observed(call);
    assert.equal(result.failed, Boolean(call.throws));
    assert.equal(result.error, call.error);
    assert.equal(result.qualificationError, undefined);
    assert.equal(result.diagnostic.stopReason, call.throws ? 'error'
      : call.outcome === 'refused' ? 'refusal' : 'success');
  }
});

const clockFailure = new Error('Synthetic post-call clock failure.');
const clocks = [
  { name: 'rollback', clockAfter: () => 99n * MS, errorType: TypeError },
  { name: 'non-bigint reading', clockAfter: () => undefined, errorType: TypeError },
  { name: 'unavailable clock', clockAfter: () => { throw clockFailure; }, error: clockFailure }
];
for (const clock of clocks) {
  for (const call of [calls[0], calls[2], calls[3], calls[4]]) {
    test(`post-call ${clock.name} after ${call.name} retains the call and stops immediately`, async () => {
      const result = await observed(call, { startNs: 100n * MS, clockAfter: clock.clockAfter });
      assert.equal(result.failed, true);
      assert.equal(result.diagnostic.stopReason, 'clock_after_call');
      if (clock.error) assert.equal(result.qualificationError, clock.error);
      else assert.ok(result.qualificationError instanceof clock.errorType);
      if (!call.throws) assert.equal(result.error, result.qualificationError);
    });
  }
}

test('even a falsy thrown primary retains identity beside a post-call clock error', async () => {
  const call = { kind: 'claim', throws: true, error: 0, outcome: 'error' };
  const result = await observed(call, { clockAfter: () => { throw clockFailure; } });
  assert.equal(result.failed, true);
  assert.equal(result.error, 0);
  assert.equal(result.qualificationError, clockFailure);
  assert.equal(result.diagnostic.stopReason, 'clock_after_call');
});
