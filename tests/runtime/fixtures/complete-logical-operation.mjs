import { DurableWorkflowStore } from '@jevelopers/agent-harness-runtime';

const [databasePath, workflowId, ownerId, fenceText, expectedRevisionText] = process.argv.slice(2);
const store = DurableWorkflowStore.open({
  databasePath,
  clock: () => 1000,
  idFactory: () => 'logical-operation-child-generated'
});
try {
  const lease = { workflowId, ownerId, fence: Number(fenceText) };
  const intent = store.recordDispatchIntent({
    ...lease,
    expectedRevision: Number(expectedRevisionText),
    attemptId: 'completed-operation-attempt',
    logicalOperationRef: 'completed-logical-operation',
    authorityHoldRef: 'opaque-completed-operation-hold',
    transportAttemptId: 'completed-operation-transport',
    eventId: 'completed-operation-intent'
  });
  const receipt = store.recordResponseReceipt({
    ...lease,
    expectedRevision: intent.revision,
    attemptId: 'completed-operation-attempt',
    receiptId: 'completed-operation-receipt',
    outcome: 'completed',
    nextState: 'running',
    responseBytes: Buffer.from('completed-operation-restart-fixture'),
    nextCheckpoint: { step: 'completed-operation-durable' },
    eventId: 'completed-operation-response'
  });
  const reconciled = store.reconcilePendingReceipts({
    ...lease,
    expectedRevision: receipt.revision,
    eventIdPrefix: 'completed-operation-reconcile'
  });
  process.stdout.write(JSON.stringify({
    state: reconciled.state,
    revision: reconciled.revision,
    checkpointSequence: reconciled.checkpointSequence
  }));
} finally {
  store.close();
}
