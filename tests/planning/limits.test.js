import assert from 'node:assert/strict';
import test from 'node:test';
import {
  advanceBudget,
  checkClockBudget,
  checkStepDuration,
  createBudgetSnapshot,
  createLimitSet,
  LimitError,
  recordResultBytes,
  validateBudgetSnapshot
} from '../../src/orchestrator/internal/limits.js';

function code(error) {
  return error instanceof LimitError ? error.code : undefined;
}

test('limit configuration is explicit, versioned, and can only tighten hard bounds', () => {
  const limits = createLimitSet(JSON.stringify({
    maxSteps: 4,
    maxReplans: 0,
    maxLocalRetries: 1
  }));
  assert.equal(limits.version, 1);
  assert.match(limits.hash, /^limits1_[0-9a-f]{64}$/);
  assert.equal(limits.values.maxSteps, 4);
  assert.equal(limits.values.maxReplans, 0);

  assert.throws(
    () => createLimitSet('{"maxSteps":33}'),
    error => code(error) === 'LIMIT_ENLARGEMENT_OR_INVALID_VALUE'
  );
  assert.throws(
    () => createLimitSet('{"providerBudget":100}'),
    error => code(error) === 'INVALID_LIMITS'
  );
  assert.throws(
    () => createLimitSet('{"maxSteps":1.5}'),
    error => code(error) === 'LIMIT_ENLARGEMENT_OR_INVALID_VALUE'
  );
});

test('limit JSON rejects duplicate members and integer spellings that normalize', () => {
  for (const source of [
    '{"maxSteps":1,"maxSteps":32}',
    '{"maxSteps":1.0000000000000001}',
    '{"maxSteps":1e0}',
    '{"maxSteps":9007199254740993}'
  ]) {
    assert.throws(() => createLimitSet(source), error => code(error) === 'INVALID_LIMITS');
  }
});

test('object, accessor, and proxy limit inputs are rejected without invoking caller code', () => {
  let reads = 0;
  const accessor = {};
  Object.defineProperty(accessor, 'maxSteps', {
    enumerable: true,
    get() {
      reads += 1;
      return 1;
    }
  });
  const proxied = new Proxy({}, {
    get() {
      reads += 1;
      return 1;
    },
    ownKeys() {
      reads += 1;
      return [];
    }
  });
  assert.throws(() => createLimitSet(accessor), error => code(error) === 'INVALID_LIMITS');
  assert.throws(() => createLimitSet(proxied), error => code(error) === 'INVALID_LIMITS');
  assert.equal(reads, 0);
});

test('step, replay, retry, replan, iteration, and result allowances stop exactly at their caps', () => {
  const limits = createLimitSet(JSON.stringify({
    maxSteps: 1,
    maxStepAttempts: 4,
    maxLocalRetries: 1,
    maxReplans: 1,
    maxIterations: 4,
    maxResultBytes: 2,
    maxTotalResultBytes: 3,
    maxElapsedMs: 500
  }));
  const initial = createBudgetSnapshot(limits, 1_000, 1_500);

  const first = advanceBudget(initial, 'step', 1_000, 0, limits);
  assert.equal(first.allowed, true);
  assert.equal(first.state.stepsAdmitted, 1);
  assert.equal(first.state.stepAttempts, 1);
  const tooManySteps = advanceBudget(first.state, 'step', 1_010, 10, limits);
  assert.equal(tooManySteps.allowed, false);
  assert.equal(tooManySteps.reason, 'STEP_LIMIT');
  assert.equal(tooManySteps.state.stepsAdmitted, 1);

  const replay = advanceBudget(first.state, 'replay', 1_010, 10, limits);
  assert.equal(replay.allowed, true);
  assert.equal(replay.state.stepsAdmitted, 1);
  assert.equal(replay.state.stepAttempts, 2);
  assert.equal(replay.state.localRetries, 1);
  const replayAgain = advanceBudget(replay.state, 'replay', 1_020, 20, limits);
  assert.equal(replayAgain.allowed, false);
  assert.equal(replayAgain.reason, 'LOCAL_RETRY_LIMIT');

  const replan = advanceBudget(replay.state, 'replan', 1_020, 20, limits);
  assert.equal(replan.allowed, true);
  const replanAgain = advanceBudget(replan.state, 'replan', 1_030, 30, limits);
  assert.equal(replanAgain.allowed, false);
  assert.equal(replanAgain.reason, 'REPLAN_LIMIT');

  const bytes = recordResultBytes(replan.state, 2, limits);
  assert.equal(bytes.allowed, true);
  assert.equal(bytes.state.totalResultBytes, 2);
  const totalTooLarge = recordResultBytes(bytes.state, 2, limits);
  assert.equal(totalTooLarge.allowed, false);
  assert.equal(totalTooLarge.reason, 'TOTAL_RESULT_SIZE_LIMIT');
  assert.equal(recordResultBytes(bytes.state, 3, limits).reason, 'RESULT_SIZE_LIMIT');
});

test('pinned wall deadline and elapsed allowance fail closed at exact boundaries and on shifts', () => {
  const limits = createLimitSet(JSON.stringify({ maxElapsedMs: 100 }));
  const initial = createBudgetSnapshot(limits, 5_000, 5_100);
  assert.equal(checkClockBudget(initial, 5_099, 99, limits), null);
  assert.equal(checkClockBudget(initial, 5_100, 99, limits), 'DEADLINE_EXCEEDED');
  assert.equal(checkClockBudget(initial, 5_099, 100, limits), 'ELAPSED_LIMIT');
  assert.equal(checkClockBudget(initial, 4_999, 0, limits), 'CLOCK_ROLLBACK');
  assert.equal(checkClockBudget(initial, 5_500, 0, limits), 'CLOCK_INCONSISTENT');

  const first = advanceBudget(initial, 'step', 5_010, 10, limits);
  assert.equal(first.allowed, true);
  assert.equal(checkClockBudget(first.state, 5_009, 9, limits), 'CLOCK_ROLLBACK');
  assert.equal(checkClockBudget(first.state, 5_500, 20, limits), 'CLOCK_INCONSISTENT');
  assert.throws(
    () => createBudgetSnapshot(limits, 5_000, 5_101),
    error => code(error) === 'INVALID_DEADLINE'
  );
});

test('step duration boundary and persisted limit identity are validated', () => {
  const limits = createLimitSet(JSON.stringify({
    maxStepMs: 5,
    maxElapsedMs: 50
  }));
  const state = createBudgetSnapshot(limits, 10_000, 10_050);
  assert.equal(checkStepDuration(4, limits).allowed, true);
  assert.deepEqual(checkStepDuration(5, limits), {
    allowed: false,
    reason: 'STEP_DURATION_LIMIT'
  });
  assert.throws(
    () => validateBudgetSnapshot(state, createLimitSet('{"maxSteps":1}')),
    error => code(error) === 'INVALID_BUDGET_SNAPSHOT'
  );
  assert.throws(
    () => validateBudgetSnapshot({ ...state, stepsAdmitted: 99 }, limits),
    error => code(error) === 'INVALID_BUDGET_SNAPSHOT'
  );
});
