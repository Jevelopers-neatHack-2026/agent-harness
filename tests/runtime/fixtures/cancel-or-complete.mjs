import { existsSync, writeFileSync } from 'node:fs';
import { DurableWorkflowStore, RuntimeError } from '@jevelopers/agent-harness-runtime';

const [databasePath, workflowId, ownerId, fenceText, expectedRevisionText, action, eventId, gatePath] =
  process.argv.slice(2);
writeFileSync(gatePath + '.' + eventId + '.ready', 'ready');
while (!existsSync(gatePath)) {
  await new Promise(resolve => setTimeout(resolve, 4));
}

const store = DurableWorkflowStore.open({
  databasePath,
  clock: () => 1000,
  idFactory: () => eventId + '-generated'
});
try {
  let result;
  if (action === 'cancel') {
    result = store.requestCancellation({
      workflowId,
      ownerId,
      fence: Number(fenceText),
      expectedRevision: Number(expectedRevisionText),
      eventId
    });
    process.stdout.write(JSON.stringify({ operation: 'cancel', ...result }));
  } else {
    result = store.transitionWorkflow({
      workflowId,
      ownerId,
      fence: Number(fenceText),
      expectedRevision: Number(expectedRevisionText),
      toState: 'succeeded',
      eventId
    });
    process.stdout.write(JSON.stringify({ operation: 'complete', accepted: true, ...result }));
  }
} catch (error) {
  if (error instanceof RuntimeError) {
    process.stdout.write(JSON.stringify({ operation: action, accepted: false, code: error.code }));
  } else {
    process.stdout.write(JSON.stringify({ operation: action, accepted: false, code: 'UNEXPECTED_ERROR' }));
  }
  process.exitCode = 2;
} finally {
  store.close();
}
