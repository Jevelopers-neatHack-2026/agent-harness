import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { DurableWorkflowStore } from '@jevelopers/agent-harness-runtime';
import { LOCAL_CONTROLLER_STORAGE_CAPABILITY } from '../../../src/internal/private-local-store-capability.js';

const [mode, databasePath, workflowId, ownerId, fenceText, payloadPath, gatePath, workerId, resultPath, newOwnerId] = process.argv.slice(2);
const fence = Number(fenceText);
const payload = payloadPath === '-' ? null : JSON.parse(readFileSync(payloadPath, 'utf8'));
const controlledAdmissionTime = mode === 'admit'
  ? payload?.controllerState?.budget?.lastWallMs
  : undefined;
const store = DurableWorkflowStore.open({
  databasePath,
  ...(Number.isSafeInteger(controlledAdmissionTime) ? { clock: () => controlledAdmissionTime } : {})
});

async function waitForRelease() {
  writeFileSync(gatePath + '.' + workerId + '.ready', 'ready');
  const deadline = Date.now() + 10_000;
  while (!existsSync(gatePath)) {
    if (Date.now() >= deadline) throw new Error('race fixture release timeout');
    await new Promise(resolve => setTimeout(resolve, 2));
  }
}

let result;
try {
  await waitForRelease();
  if (mode === 'admit') {
    result = store._localControllerAdmit(LOCAL_CONTROLLER_STORAGE_CAPABILITY, {
      ...payload,
      workflowId,
      ownerId,
      fence
    });
  } else if (mode === 'complete') {
    result = store._localControllerComplete(LOCAL_CONTROLLER_STORAGE_CAPABILITY, {
      ...payload,
      workflowId,
      ownerId,
      fence
    });
  } else if (mode === 'cancel') {
    result = store.requestCancellation({
      workflowId,
      ownerId,
      fence,
      expectedRevision: payload.expectedRevision,
      eventId: payload.eventId
    });
  } else if (mode === 'takeover') {
    result = store.acquireLease({
      workflowId,
      ownerId: newOwnerId,
      ttlMs: 60_000,
      eventId: payload.eventId
    });
  } else {
    throw new Error('unknown fixture mode');
  }
  writeFileSync(resultPath, JSON.stringify({ accepted: true, result }));
  process.exitCode = 0;
} catch (error) {
  writeFileSync(resultPath, JSON.stringify({ accepted: false, code: error?.code ?? 'FIXTURE_ERROR' }));
  process.exitCode = 2;
} finally {
  store.close();
}
