import { DurableWorkflowStore } from '@jevelopers/agent-harness-runtime';

const databasePath = process.argv[2];
const store = DurableWorkflowStore.open({
  databasePath,
  clock: () => 1200,
  idFactory: () => 'child-generated-id'
});
const lease = store.acquireLease({
  workflowId: 'crash-run',
  ownerId: 'recovery-worker',
  ttlMs: 50,
  eventId: 'recovery-lease'
});
const workflow = store.getWorkflow('crash-run');
store.recordResponseReceipt({
  ...lease,
  expectedRevision: workflow.revision,
  attemptId: 'attempt-crash',
  receiptId: 'receipt-crash',
  outcome: 'completed',
  nextState: 'running',
  responseBytes: Buffer.from('internal-response-fixture'),
  nextCheckpoint: {
    step: 'response-finished',
    artifact_version_ref: 'version-7',
    approval_binding_hash: 'sha256-fixture'
  },
  artifacts: [{
    artifactId: 'result-version-7',
    bytes: Buffer.from([0, 1, 2, 255, 10])
  }],
  eventId: 'persist-crash-receipt'
});

// Exit without closing the database or advancing the checkpoint.
process.exit(73);
