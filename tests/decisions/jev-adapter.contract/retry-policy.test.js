import assert from 'node:assert/strict';
import { test } from 'node:test';
import { planJevRetry } from '../../../src/adapters/jev/retry-policy.js';

const NOW = Date.UTC(2026, 9, 10, 12, 0, 0);

function plan(overrides = {}) {
  return planJevRetry({
    status: 429,
    attemptNumber: 1,
    remainingMs: 8_000,
    nowEpochMs: NOW,
    ...overrides
  });
}

test('retry_faults: 429/529 produce bounded candidates only; 401/422 stop', () => {
  const rateLimit = plan({ status: 429, retryAfter: '2' });
  const overloaded = plan({ status: 529, retryAfter: '1' });
  const denied = plan({ status: 401 });
  const rejected = plan({ status: 422 });

  assert.deepEqual(rateLimit, {
    action: 'retry_candidate',
    reason: 'rate_limit_backoff',
    attemptsRemaining: 1,
    delayMs: 2_000,
    dispatchAuthorized: false
  });
  assert.equal(overloaded.action, 'retry_candidate');
  assert.equal(overloaded.delayMs, 1_000);
  assert.equal(denied.action, 'stop');
  assert.equal(denied.reason, 'authentication_rejected');
  assert.equal(rejected.action, 'stop');
  assert.equal(rejected.reason, 'request_rejected');
  for (const result of [rateLimit, overloaded, denied, rejected]) {
    assert.equal(result.dispatchAuthorized, false);
    assert.ok(Object.isFrozen(result));
  }
});

test('retry_faults: seconds and HTTP dates are bounded against a monotonic remaining-time budget', () => {
  const seconds = plan({ retryAfter: '3', remainingMs: 4_000 });
  const date = plan({ retryAfter: 'Sat, 10 Oct 2026 12:00:02 GMT', remainingMs: 4_000 });
  const pastDate = plan({ retryAfter: 'Sat, 10 Oct 2026 11:59:00 GMT', remainingMs: 1_000 });

  assert.equal(seconds.action, 'retry_candidate');
  assert.equal(seconds.delayMs, 3_000);
  assert.equal(date.delayMs, 2_000);
  assert.equal(pastDate.delayMs, 250);
});

test('retry_faults: invalid Retry-After uses bounded fallback, while huge or out-of-budget values defer without clipping', () => {
  const invalid = plan({ retryAfter: 'soon' });
  const control = plan({ retryAfter: '2\r\nInjected: yes' });
  const hugeSeconds = plan({ retryAfter: '999999999999999999999999999999' });
  const hugeDate = plan({ retryAfter: 'Sat, 10 Oct 9999 12:00:00 GMT' });
  const overDeadline = plan({ retryAfter: '9', remainingMs: 1_000 });

  assert.equal(invalid.action, 'retry_candidate');
  assert.equal(invalid.reason, 'invalid_retry_after_bounded_fallback');
  assert.equal(control.delayMs, 250);
  assert.equal(hugeSeconds.action, 'defer');
  assert.equal(hugeSeconds.reason, 'retry_after_exceeds_policy');
  assert.equal(hugeDate.action, 'defer');
  assert.equal(overDeadline.action, 'defer');
  assert.equal(overDeadline.delayMs, null);
});

test('retry_faults: exhausted attempt and deadline budgets stop or defer without authorizing another send', () => {
  assert.equal(plan({ attemptNumber: 2 }).reason, 'recommendation_limit_reached');
  assert.equal(plan({ remainingMs: 0 }).reason, 'deadline_exhausted');
  assert.equal(plan({ remainingMs: 100 }).action, 'defer');
  assert.equal(plan({ remainingMs: 100 }).delayMs, null);
});

test('retry_faults: 408, other 5xx, timeout, connection loss, interruption, and possible-write cancellation remain review outcomes', () => {
  for (const status of [408, 500, 502, 503, 598]) {
    const result = plan({ status, retryAfter: '0' });
    assert.equal(result.action, 'review', String(status));
    assert.equal(result.dispatchAuthorized, false);
  }
  for (const failureKind of ['timeout', 'connection_reset', 'body_interrupted', 'cancelled_after_possible_dispatch']) {
    const result = plan({ status: null, failureKind });
    assert.equal(result.action, 'review', failureKind);
  }
  const predispatchCancel = plan({ status: null, failureKind: 'cancelled_before_dispatch' });
  assert.equal(predispatchCancel.action, 'stop');
  assert.equal(predispatchCancel.reason, 'cancelled_before_dispatch');
});

test('retry_faults: cancellation, timeout, and interrupted-body evidence overrides retryable HTTP status', () => {
  for (const status of [429, 529]) {
    const predispatch = plan({ status, failureKind: 'cancelled_before_dispatch' });
    assert.equal(predispatch.action, 'stop', `${status}: pre-dispatch cancellation`);
    assert.equal(predispatch.reason, 'cancelled_before_dispatch');
    assert.equal(predispatch.attemptsRemaining, 0);

    const possibleWrite = plan({ status, failureKind: 'cancelled_after_possible_dispatch' });
    assert.equal(possibleWrite.action, 'review', `${status}: possible-write cancellation`);
    assert.equal(possibleWrite.attemptsRemaining, 0);

    for (const failureKind of ['timeout', 'connection_reset', 'body_interrupted']) {
      const ambiguous = plan({ status, failureKind });
      assert.equal(ambiguous.action, 'review', `${status}: ${failureKind}`);
      assert.equal(ambiguous.attemptsRemaining, 0);
      assert.equal(ambiguous.dispatchAuthorized, false);
    }

    const ordinary = plan({ status });
    assert.equal(ordinary.action, 'retry_candidate');
    assert.equal(ordinary.attemptsRemaining, 1);
    assert.equal(ordinary.dispatchAuthorized, false);
  }
});

test('retry_faults: input bounds fail closed and caller objects cannot supply retry callbacks', () => {
  assert.throws(() => plan({ remainingMs: 60_001 }), TypeError);
  assert.throws(() => plan({ retry: () => {} }), TypeError);
  assert.throws(() => plan({ attemptNumber: 0 }), TypeError);
  const revoked = Proxy.revocable({}, {});
  revoked.revoke();
  assert.throws(() => plan(revoked.proxy), error => {
    assert.ok(error instanceof TypeError);
    assert.doesNotMatch(error.message, /private|secret|SYNTHETIC/u);
    return true;
  });
});
