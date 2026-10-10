import { existsSync, writeFileSync } from 'node:fs';
import { DurableWorkflowStore, RuntimeError } from '@jevelopers/agent-harness-runtime';

const [databasePath, workflowId, ownerId, fenceText, expectedRevisionText, target, eventId, gatePath] =
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
  const result = store.transitionWorkflow({
    workflowId,
    ownerId,
    fence: Number(fenceText),
    expectedRevision: Number(expectedRevisionText),
    toState: target,
    eventId
  });
  process.stdout.write(JSON.stringify({ accepted: true, state: result.state, revision: result.revision }));
} catch (error) {
  if (error instanceof RuntimeError) {
    process.stdout.write(JSON.stringify({ accepted: false, code: error.code }));
  } else {
    process.stdout.write(JSON.stringify({ accepted: false, code: 'UNEXPECTED_ERROR' }));
  }
  process.exitCode = 2;
} finally {
  store.close();
}
