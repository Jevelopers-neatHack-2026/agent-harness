import { readFileSync, renameSync, writeFileSync } from 'node:fs';
import { DurableWorkflowStore } from '@jevelopers/agent-harness-runtime';
import { LOCAL_CONTROLLER_STORAGE_CAPABILITY } from '../../../src/internal/private-local-store-capability.js';
import {
  buildCompletedCheckpoint,
  buildInitialPrivateCheckpoint,
  computePendingLocalStep
} from '../../../src/orchestrator/internal/controller.js';
import {
  advanceBudget,
  createBudgetSnapshot,
  createLimitSet,
  getLimitValues,
  recordResultBytes
} from '../../../src/orchestrator/internal/limits.js';
import { compilePlan, getNextReadyStep } from '../../../src/orchestrator/internal/plan.js';

const [databasePath, workflowId, ownerId, fenceText, planPath, markerPath, stage, clockText] = process.argv.slice(2);
const store = DurableWorkflowStore.open({
  databasePath,
  ...(clockText === undefined ? {} : { clock: () => Number(clockText) })
});
const fence = Number(fenceText);

function pauseForKill(marker) {
  const temporaryMarkerPath = markerPath + '.' + process.pid + '.tmp';
  writeFileSync(temporaryMarkerPath, JSON.stringify(marker), { flag: 'wx' });
  renameSync(temporaryMarkerPath, markerPath);
  return new Promise(() => setInterval(() => {}, 60_000));
}

if (stage === 'before-admission') {
  await pauseForKill({ stage });
} else {
  const planJson = readFileSync(planPath, 'utf8');
  const snapshot = store._localControllerSnapshot(LOCAL_CONTROLLER_STORAGE_CAPABILITY, workflowId);
  const limitSet = createLimitSet();
  const plan = compilePlan(planJson, limitSet, undefined, workflowId);
  const now = store._localControllerNow(LOCAL_CONTROLLER_STORAGE_CAPABILITY);
  const deadline = now + getLimitValues(limitSet).maxElapsedMs;
  const initial = createBudgetSnapshot(limitSet, now, deadline);
  const stepId = getNextReadyStep(plan, '{}');
  const advanced = advanceBudget(initial, 'step', now, 0, limitSet);
  const state = buildInitialPrivateCheckpoint({
    workflowId,
    planJson,
    plan,
    limitSet,
    baseCheckpointData: snapshot.checkpoint.data,
    budget: advanced.state,
    stepId
  });
  const admission = store._localControllerAdmit(LOCAL_CONTROLLER_STORAGE_CAPABILITY, {
    workflowId,
    ownerId,
    fence,
    expectedRevision: snapshot.workflow.revision,
    expectedCheckpointSequence: snapshot.checkpoint.sequence,
    stepId,
    admissionKind: 'step',
    eventId: 'crash-fixture:' + stage + ':admit',
    controllerState: state
  });
  if (stage === 'after-admission') {
    await pauseForKill({ stage, revision: admission.revision, checkpointSequence: admission.checkpointSequence });
  }

  const resultRecord = computePendingLocalStep(state, plan);
  if (stage === 'after-compute') {
    await pauseForKill({ stage, resultHash: resultRecord.resultHash });
  }

  const resultBytes = Buffer.byteLength(JSON.stringify(resultRecord.output), 'utf8');
  const recorded = recordResultBytes(state.budget, resultBytes, limitSet);
  const completedBudget = Object.freeze({
    ...recorded.state,
    lastWallMs: store._localControllerNow(LOCAL_CONTROLLER_STORAGE_CAPABILITY)
  });
  const completed = buildCompletedCheckpoint(state, plan, resultRecord, completedBudget);
  const commit = store._localControllerComplete(LOCAL_CONTROLLER_STORAGE_CAPABILITY, {
    workflowId,
    ownerId,
    fence,
    expectedRevision: admission.revision,
    expectedCheckpointSequence: admission.checkpointSequence,
    stepId,
    eventId: 'crash-fixture:' + stage + ':complete',
    controllerState: completed
  });
  await pauseForKill({ stage, revision: commit.revision, checkpointSequence: commit.checkpointSequence });
}

// This fixture is terminated by its parent at a recorded durable boundary.
await new Promise(resolve => setTimeout(resolve, 60_000));
store.close();
process.exit(0);
