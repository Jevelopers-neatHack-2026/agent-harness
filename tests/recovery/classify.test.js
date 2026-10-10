import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import {
  advanceBudget,
  createBudgetSnapshot,
  createLimitSet,
  serializeLimitSet
} from '../../src/orchestrator/internal/limits.js';
import {
  compilePlan,
  executeLocalStep
} from '../../src/orchestrator/internal/plan.js';
import {
  classifyRecovery,
  PRIVATE_CHECKPOINT_KIND
} from '../../src/recovery/internal/classify.js';

const PLAN_KIND = 'agent-harness.private-synthetic-plan';
const WORKFLOW_ID = 'wf-private-test-01';

function makePlanJson() {
  return JSON.stringify({
    kind: PLAN_KIND,
    version: 1,
    steps: [
      {
        id: 'second',
        operation: 'number.add',
        dependsOn: ['first'],
        args: { left: { $result: 'first' }, right: 2 }
      },
      {
        id: 'first',
        operation: 'number.add',
        dependsOn: [],
        args: { left: 1, right: 2 }
      }
    ]
  });
}

function canonical(value) {
  if (value === null || typeof value === 'boolean' || typeof value === 'string') {
    return JSON.stringify(value);
  }
  if (typeof value === 'number') return JSON.stringify(value);
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  const keys = Object.keys(value).sort();
  return '{' + keys.map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}';
}

function savedResult(plan, stepId, args, output) {
  const inputHash = 'sha256_' + createHash('sha256').update(canonical({
    kind: 'agent-harness.private-local-input.v1',
    stepId,
    planHash: plan.planHash,
    registryVersion: plan.registryVersion,
    runIdentity: plan.runIdentity,
    args
  }), 'utf8').digest('hex');
  const resultHash = 'sha256_' + createHash('sha256').update(canonical({
    kind: 'agent-harness.private-local-result.v1',
    stepId,
    planHash: plan.planHash,
    registryVersion: plan.registryVersion,
    runIdentity: plan.runIdentity,
    inputHash,
    output
  }), 'utf8').digest('hex');
  return {
    stepId,
    planHash: plan.planHash,
    registryVersion: plan.registryVersion,
    runIdentity: plan.runIdentity,
    inputHash,
    resultHash,
    output
  };
}

function privateData({
  workflowId = WORKFLOW_ID,
  planJson = makePlanJson(),
  limitSet = createLimitSet(),
  budget,
  completedResultsJson = '{}',
  pendingStepId = null,
  version = 2,
  lastAdmissionKind = 'step',
  lastAdmissionStepId,
  baseCheckpointData = {},
  kind = PRIVATE_CHECKPOINT_KIND,
  limitsJson,
  limitsHash,
  planHash,
  registryVersion,
  runIdentity
} = {}) {
  const plan = compilePlan(planJson, limitSet, undefined, workflowId);
  let inferredLastStep = lastAdmissionStepId;
  if (inferredLastStep === undefined) {
    if (pendingStepId !== null) inferredLastStep = pendingStepId;
    else {
      const resultIds = Object.keys(JSON.parse(completedResultsJson));
      inferredLastStep = resultIds.at(-1) ?? null;
    }
  }
  return {
    kind,
    version,
    workflowId,
    planJson,
    planHash: planHash ?? plan.planHash,
    registryVersion: registryVersion ?? plan.registryVersion,
    runIdentity: runIdentity ?? plan.runIdentity,
    limitsJson: limitsJson ?? serializeLimitSet(limitSet),
    limitsHash: limitsHash ?? limitSet.hash,
    budget: budget ?? createBudgetSnapshot(limitSet, 1_000, 31_000),
    completedResultsJson,
    pendingStepId,
    lastAdmissionKind,
    lastAdmissionStepId: inferredLastStep,
    baseCheckpointData
  };
}

function recoverySnapshot({
  data = privateData(),
  workflowId = WORKFLOW_ID,
  state = 'running',
  revision = 4,
  checkpointSequence = 2,
  before = {},
  after = {},
  checkpointSequenceOverride,
  schemaVersion = 2,
  sha256,
  checkpoint = true,
  attemptStates = [],
  observedAtMs = 1_000
} = {}) {
  const beforeWorkflow = {
    workflowId,
    state,
    revision,
    checkpointSequence,
    cancellationRequested: false,
    ...before
  };
  const afterWorkflow = {
    ...beforeWorkflow,
    ...after
  };
  const checkpointRecord = checkpoint
    ? {
      sequence: checkpointSequenceOverride ?? afterWorkflow.checkpointSequence,
      schemaVersion,
      data,
      sha256: sha256 ?? createHash('sha256')
        .update('{"schemaVersion":2,"data":' + canonical(data), 'utf8')
        .update('}', 'utf8')
        .digest('hex')
    }
    : null;
  return JSON.stringify({
    before: beforeWorkflow,
    after: afterWorkflow,
    checkpoint: checkpointRecord,
    externalAttemptStates: attemptStates,
    observedAtMs
  });
}

function checkpointWithData(data, overrides = {}) {
  return recoverySnapshot({ data, ...overrides });
}

function readyRunIdentity(workflowId = WORKFLOW_ID) {
  const plan = compilePlan(makePlanJson(), undefined, undefined, workflowId);
  return plan;
}

test('recovery classifies pending pure local work as a candidate without executing it', () => {
  const limits = createLimitSet();
  let budget = createBudgetSnapshot(limits, 1_000, 31_000);
  budget = advanceBudget(budget, 'step', 1_000, 0, limits).state;
  const data = privateData({
    limitSet: limits,
    budget,
    pendingStepId: 'first'
  });
  const result = classifyRecovery(checkpointWithData(data, { observedAtMs: 1_001 }));

  assert.deepEqual(result, {
    kind: 'local-replay-candidate',
    reason: 'PURE_LOCAL_REPLAY_WITH_NEW_ALLOWANCE_REQUIRED',
    actionable: false
  });
});

test('recovery reports ready work and committed results as evidence without promoting them', () => {
  const plan = readyRunIdentity();
  const ready = classifyRecovery(checkpointWithData(privateData()));
  assert.equal(ready.kind, 'local-ready-candidate');
  assert.equal(ready.actionable, false);

  const first = executeLocalStep(plan, 'first');
  const resultsAfterFirst = JSON.stringify({ first });
  const second = executeLocalStep(plan, 'second', resultsAfterFirst);
  let budget = createBudgetSnapshot(createLimitSet(), 1_000, 31_000);
  budget = advanceBudget(budget, 'step', 1_000, 0).state;
  budget = advanceBudget(budget, 'step', 1_000, 0).state;
  budget = { ...budget, totalResultBytes: 2 };
  const complete = classifyRecovery(checkpointWithData(privateData({
    budget,
    completedResultsJson: JSON.stringify({ first, second })
  })));
  assert.deepEqual(complete, {
    kind: 'durable-evidence-present',
    reason: 'ALL_LOCAL_RESULTS_MATCH_PINNED_IDENTITIES',
    actionable: false
  });
});

test('recovery preserves finite numbers only inside the opaque original checkpoint', () => {
  const values = [
    42,
    Number.MAX_SAFE_INTEGER,
    Number.MAX_SAFE_INTEGER + 1,
    1e20,
    1e21,
    -1e20,
    -1.5,
    1.5,
    1e100
  ];

  for (const value of values) {
    const snapshot = checkpointWithData(privateData({
      baseCheckpointData: { value, nested: [{ value }] }
    }));
    const result = classifyRecovery(snapshot);
    assert.equal(result.kind, 'local-ready-candidate', String(value));
  }

  assert.equal(
    classifyRecovery(recoverySnapshot({ revision: 1e20 })).reason,
    'RECOVERY_INPUT_INVALID',
    'workflow integer fields retain their strict numeric domain'
  );
  assert.equal(
    classifyRecovery(recoverySnapshot({ observedAtMs: 1e20 })).reason,
    'RECOVERY_INPUT_INVALID',
    'trusted observation time retains its strict numeric domain'
  );
  const unsafeBudget = {
    ...createBudgetSnapshot(createLimitSet(), 1_000, 31_000),
    lastWallMs: 1e20
  };
  assert.equal(
    classifyRecovery(checkpointWithData(privateData({ budget: unsafeBudget }))).reason,
    'RECOVERY_INPUT_INVALID',
    'controller-owned checkpoint counters retain their strict numeric domain'
  );
});

test('recovery rejects self-rehashed outputs and inconsistent persisted result-byte counts', () => {
  const plan = readyRunIdentity();
  const first = savedResult(plan, 'first', { left: 1, right: 2 }, 900);
  const second = savedResult(plan, 'second', { left: 900, right: 2 }, 902);
  let forgedBudget = createBudgetSnapshot(createLimitSet(), 1_000, 31_000);
  forgedBudget = advanceBudget(forgedBudget, 'step', 1_000, 0).state;
  forgedBudget = advanceBudget(forgedBudget, 'step', 1_000, 0).state;
  forgedBudget = { ...forgedBudget, totalResultBytes: 6 };
  const forged = classifyRecovery(checkpointWithData(privateData({
    budget: forgedBudget,
    completedResultsJson: JSON.stringify({ first, second })
  })));
  assert.deepEqual(forged, {
    kind: 'quarantine',
    reason: 'LOCAL_RESULT_IDENTITY_INVALID',
    actionable: false
  });

  const forgedTextPlanJson = JSON.stringify({
    kind: PLAN_KIND,
    version: 1,
    steps: [{ id: 'text', operation: 'text.concat', dependsOn: [], args: { parts: ['left', 'right'] } }]
  });
  const textPlan = compilePlan(forgedTextPlanJson, createLimitSet(), undefined, WORKFLOW_ID);
  const forgedText = savedResult(textPlan, 'text', { parts: ['left', 'right'] }, 'rewritten');
  let forgedTextBudget = advanceBudget(
    createBudgetSnapshot(createLimitSet(), 1_000, 31_000),
    'step', 1_000, 0
  ).state;
  forgedTextBudget = { ...forgedTextBudget, totalResultBytes: Buffer.byteLength(JSON.stringify('rewritten'), 'utf8') };
  const forgedTextRecovery = classifyRecovery(checkpointWithData(privateData({
    planJson: forgedTextPlanJson,
    budget: forgedTextBudget,
    completedResultsJson: JSON.stringify({ text: forgedText })
  })));
  assert.equal(forgedTextRecovery.kind, 'quarantine');

  const tightLimits = createLimitSet(JSON.stringify({ maxResultBytes: 2, maxTotalResultBytes: 3 }));
  const oversizedPlanJson = JSON.stringify({
    kind: PLAN_KIND,
    version: 1,
    steps: [{ id: 'text', operation: 'text.concat', dependsOn: [], args: { parts: ['abc'] } }]
  });
  const oversizedPlan = compilePlan(oversizedPlanJson, tightLimits, undefined, WORKFLOW_ID);
  const oversized = savedResult(oversizedPlan, 'text', { parts: ['abc'] }, 'abc');
  let undercountedBudget = advanceBudget(
    createBudgetSnapshot(tightLimits, 1_000, 31_000),
    'step', 1_000, 0, tightLimits
  ).state;
  undercountedBudget = { ...undercountedBudget, totalResultBytes: 0 };
  const savedSize = classifyRecovery(checkpointWithData(privateData({
    planJson: oversizedPlanJson,
    limitSet: tightLimits,
    budget: undercountedBudget,
    completedResultsJson: JSON.stringify({ text: oversized })
  })));
  assert.equal(savedSize.kind, 'quarantine');
  assert.equal(savedSize.reason, 'LOCAL_RESULT_IDENTITY_INVALID');
});

test('recovery enforces plausible admission, retry, replan, and pending relationships', () => {
  const noAdmission = classifyRecovery(checkpointWithData(privateData({ pendingStepId: 'first' })));
  assert.equal(noAdmission.kind, 'stop');
  assert.equal(noAdmission.reason, 'BUDGET_CHECKPOINT_INCONSISTENT');

  const orphanReplay = classifyRecovery(checkpointWithData(privateData({
    pendingStepId: 'first',
    lastAdmissionKind: 'replay',
    budget: {
      ...createBudgetSnapshot(createLimitSet(), 1_000, 31_000),
      stepsAdmitted: 1,
      stepAttempts: 1,
      localRetries: 0,
      iterations: 1
    }
  })));
  assert.equal(orphanReplay.kind, 'stop');
  assert.equal(orphanReplay.reason, 'BUDGET_CHECKPOINT_INCONSISTENT');

  const firstPlan = readyRunIdentity();
  const first = executeLocalStep(firstPlan, 'first');
  const completedWithoutRetry = classifyRecovery(checkpointWithData(privateData({
    completedResultsJson: JSON.stringify({ first }),
    lastAdmissionKind: 'replay',
    lastAdmissionStepId: 'first',
    budget: {
      ...createBudgetSnapshot(createLimitSet(), 1_000, 31_000),
      stepsAdmitted: 1,
      stepAttempts: 1,
      localRetries: 0,
      iterations: 1,
      totalResultBytes: Buffer.byteLength(JSON.stringify(first.output), 'utf8')
    }
  })));
  assert.equal(completedWithoutRetry.kind, 'stop');
  assert.equal(completedWithoutRetry.reason, 'BUDGET_CHECKPOINT_INCONSISTENT');

  const limits = createLimitSet();
  let budget = createBudgetSnapshot(limits, 1_000, 31_000);
  budget = advanceBudget(budget, 'step', 1_000, 0, limits).state;
  budget = advanceBudget(budget, 'replay', 1_001, 1, limits).state;
  const replay = classifyRecovery(checkpointWithData(privateData({
    budget,
    pendingStepId: 'first',
    lastAdmissionKind: 'replay'
  }), { observedAtMs: 1_002 }));
  assert.equal(replay.kind, 'local-replay-candidate');

  const replannedBudget = advanceBudget(budget, 'replan', 1_002, 2, limits).state;
  const replanned = classifyRecovery(checkpointWithData(privateData({
    budget: replannedBudget,
    pendingStepId: 'first',
    lastAdmissionKind: 'replay'
  }), { observedAtMs: 1_003 }));
  assert.equal(replanned.kind, 'local-replay-candidate');

  const impossible = classifyRecovery(checkpointWithData(privateData({
    pendingStepId: 'first',
    budget: {
      ...advanceBudget(createBudgetSnapshot(limits, 1_000, 31_000), 'step', 1_000, 0, limits).state,
      stepAttempts: 1,
      localRetries: 32,
      replans: 8,
      iterations: 1
    }
  })));
  assert.equal(impossible.kind, 'stop');
  assert.equal(impossible.reason, 'BUDGET_INVALID');
});

test('recovery enforces the pinned attempt-state cap after preserving unknown precedence', () => {
  const limits = createLimitSet(JSON.stringify({ maxRecoveryAttemptStates: 1 }));
  const exactCap = classifyRecovery(checkpointWithData(privateData({ limitSet: limits }), {
    attemptStates: ['reconciled']
  }));
  assert.equal(exactCap.kind, 'local-ready-candidate');

  const overflow = classifyRecovery(checkpointWithData(privateData({ limitSet: limits }), {
    attemptStates: ['reconciled', 'reconciled']
  }));
  assert.equal(overflow.kind, 'stop');
  assert.equal(overflow.reason, 'ATTEMPT_SNAPSHOT_LIMIT');

  const unresolved = classifyRecovery(checkpointWithData(privateData({ limitSet: limits }), {
    attemptStates: ['intent', 'reconciled']
  }));
  assert.equal(unresolved.kind, 'external-reconciliation-required');
});

test('an unresolved external effect is a barrier even when the checkpoint is absent', () => {
  for (const state of ['intent', 'outcome_unknown', 'response_received']) {
    const result = classifyRecovery(recoverySnapshot({
      checkpoint: false,
      attemptStates: [state]
    }));
    assert.equal(result.kind, 'external-reconciliation-required');
    assert.equal(result.reason, 'EXTERNAL_OUTCOME_UNRESOLVED');
    assert.equal(result.actionable, false);
  }
  assert.equal(classifyRecovery(recoverySnapshot({
    state: 'reconciliation_pending',
    checkpoint: false
  })).kind, 'external-reconciliation-required');
  assert.equal(classifyRecovery(recoverySnapshot({
    state: 'awaiting_response',
    checkpoint: false
  })).kind, 'external-reconciliation-required');
});

test('missing, old, future, corrupt, and foreign checkpoints stop without adoption', () => {
  assert.equal(classifyRecovery(recoverySnapshot({ checkpoint: false })).reason, 'CHECKPOINT_MISSING');
  assert.equal(classifyRecovery(recoverySnapshot({ schemaVersion: 1 })).reason, 'CHECKPOINT_UNSUPPORTED');
  assert.equal(classifyRecovery(recoverySnapshot({ schemaVersion: 3 })).reason, 'CHECKPOINT_UNSUPPORTED');

  const corrupt = recoverySnapshot({ sha256: '0'.repeat(64) });
  assert.equal(classifyRecovery(corrupt).reason, 'CHECKPOINT_CORRUPT');

  const foreign = privateData({ kind: 'generic-workflow-checkpoint' });
  assert.equal(classifyRecovery(checkpointWithData(foreign)).reason, 'CHECKPOINT_FOREIGN');

  const unsupported = privateData({ version: 3 });
  assert.equal(classifyRecovery(checkpointWithData(unsupported)).reason, 'CHECKPOINT_UNSUPPORTED');
  const oldPrivate = privateData({ version: 1 });
  assert.equal(classifyRecovery(checkpointWithData(oldPrivate)).reason, 'CHECKPOINT_UNSUPPORTED');

  const differentWorkflow = privateData({ workflowId: 'wf-other' });
  assert.equal(
    classifyRecovery(checkpointWithData(differentWorkflow)).reason,
    'CHECKPOINT_FOREIGN_WORKFLOW'
  );
});

test('corrupt plan, limit, budget, and checkpoint identities fail closed', () => {
  const wrongPlan = privateData({ planHash: 'plan1_' + '0'.repeat(64) });
  assert.equal(classifyRecovery(checkpointWithData(wrongPlan)).reason, 'CHECKPOINT_CORRUPT');

  const wrongRegistry = privateData({ registryVersion: 'registry1_' + '0'.repeat(64) });
  assert.equal(classifyRecovery(checkpointWithData(wrongRegistry)).reason, 'CHECKPOINT_CORRUPT');

  const wrongBudget = privateData({
    budget: {
      ...createBudgetSnapshot(createLimitSet(), 1_000, 31_000),
      stepsAdmitted: 99
    }
  });
  assert.equal(classifyRecovery(checkpointWithData(wrongBudget)).reason, 'BUDGET_INVALID');

  const wrongLimits = privateData({ limitsJson: '{"maxSteps":1}' });
  assert.equal(classifyRecovery(checkpointWithData(wrongLimits)).reason, 'LIMITS_INVALID');
});

test('terminal, review, cancelled, and cancellation-requested workflows cannot be resumed', () => {
  for (const state of ['succeeded', 'failed', 'cancelled']) {
    assert.equal(classifyRecovery(recoverySnapshot({ state })).reason, 'WORKFLOW_TERMINAL');
  }
  assert.equal(
    classifyRecovery(recoverySnapshot({ state: 'review_required' })).reason,
    'REVIEW_REQUIRED_IS_TERMINAL_FOR_THIS_CONTROLLER'
  );
  assert.equal(
    classifyRecovery(recoverySnapshot({ after: { cancellationRequested: true } })).reason,
    'CANCELLATION_REQUESTED'
  );
  assert.equal(
    classifyRecovery(recoverySnapshot({ state: 'queued' })).reason,
    'WORKFLOW_NOT_RUNNING'
  );
});

test('revision, state, checkpoint-sequence, and wall-clock drift stop stale recovery', () => {
  assert.equal(
    classifyRecovery(recoverySnapshot({ after: { revision: 5 } })).reason,
    'WORKFLOW_SNAPSHOT_CHANGED'
  );
  assert.equal(
    classifyRecovery(recoverySnapshot({ after: { checkpointSequence: 3 } })).reason,
    'WORKFLOW_SNAPSHOT_CHANGED'
  );
  assert.equal(
    classifyRecovery(recoverySnapshot({ observedAtMs: 999 })).reason,
    'CLOCK_ROLLBACK'
  );
  assert.equal(
    classifyRecovery(recoverySnapshot({ observedAtMs: 31_000 })).reason,
    'DEADLINE_EXCEEDED'
  );
});

test('foreign workflow results and provider-shaped result data are quarantined', () => {
  const otherPlan = compilePlan(makePlanJson(), undefined, undefined, 'wf-other');
  const otherResult = executeLocalStep(otherPlan, 'first');
  const crossWorkflowData = privateData({
    completedResultsJson: JSON.stringify({ first: otherResult })
  });
  const crossWorkflow = classifyRecovery(checkpointWithData(crossWorkflowData));
  assert.deepEqual(crossWorkflow, {
    kind: 'quarantine',
    reason: 'LOCAL_RESULT_IDENTITY_INVALID',
    actionable: false
  });

  const providerShapedData = privateData({
    completedResultsJson: JSON.stringify({
      first: {
        stepId: 'first',
        planHash: readyRunIdentity().planHash,
        registryVersion: readyRunIdentity().registryVersion,
        runIdentity: readyRunIdentity().runIdentity,
        inputHash: 'sha256_' + '0'.repeat(64),
        resultHash: 'sha256_' + '0'.repeat(64),
        output: 3,
        providerResponse: { usage: 0 }
      }
    })
  });
  const providerShaped = classifyRecovery(checkpointWithData(providerShapedData));
  assert.deepEqual(providerShaped, {
    kind: 'quarantine',
    reason: 'LOCAL_RESULT_IDENTITY_INVALID',
    actionable: false
  });
});

test('accessor, proxy, and raw provider values are not copied into recovery diagnostics', () => {
  let reads = 0;
  const proxy = new Proxy({}, {
    get() {
      reads += 1;
      return 'running';
    },
    ownKeys() {
      reads += 1;
      return [];
    }
  });
  const result = classifyRecovery(proxy);
  assert.equal(result.kind, 'stop');
  assert.equal(result.reason, 'RECOVERY_INPUT_INVALID');
  assert.equal(JSON.stringify(result).includes('secret'), false);
  assert.equal(reads, 0);

  const raw = '{"providerResponse":"customer-secret","api_key":"secret"}';
  const safe = classifyRecovery(raw);
  assert.equal(JSON.stringify(safe).includes('customer-secret'), false);
  assert.equal(JSON.stringify(safe).includes('api_key'), false);
});

test('recovery JSON rejects duplicate members and integer precision normalization', () => {
  const duplicate = recoverySnapshot().replace(
    '"observedAtMs":1000',
    '"observedAtMs":1000,"observedAtMs":1000'
  );
  assert.equal(classifyRecovery(duplicate).reason, 'RECOVERY_INPUT_INVALID');
  const rounded = recoverySnapshot().replace('"version":2', '"version":2.0000000000000001');
  assert.equal(classifyRecovery(rounded).reason, 'RECOVERY_INPUT_INVALID');
  const negativeZero = recoverySnapshot().replace('"observedAtMs":1000', '"observedAtMs":-0');
  assert.equal(classifyRecovery(negativeZero).reason, 'RECOVERY_INPUT_INVALID');
});

test('unknown external attempt states stop rather than being treated as completed', () => {
  const result = classifyRecovery(recoverySnapshot({ attemptStates: ['provider-done?'] }));
  assert.deepEqual(result, {
    kind: 'stop',
    reason: 'ATTEMPT_SNAPSHOT_INVALID',
    actionable: false
  });
});
