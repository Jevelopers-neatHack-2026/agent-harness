import { DurableWorkflowStore, RuntimeError } from '@jevelopers/agent-harness-runtime';

const [databasePath, outputTokensText] = process.argv.slice(2);
const outputTokens = Number(outputTokensText);
const store = DurableWorkflowStore.open({ databasePath, clock: () => 2000 });
try {
  const result = store.recordJevProviderObservation({
    workflowId: 'concurrent-jev-workflow',
    logicalOperationRef: 'logical-concurrent-jev-attempt',
    attemptId: 'concurrent-jev-attempt',
    transportAttemptId: 'concurrent-jev-transport',
    sourceObservationRef: 'concurrent-jev-source',
    dispatchCertainty: 'response_received',
    requestedModel: 'jev-1.13.0',
    resolvedModel: 'jev-1.13.0',
    providerRequestId: 'req-concurrent-synthetic',
    decisionConformance: 'invalid',
    usage: { kind: 'known', inputTokens: 83, outputTokens }
  });
  process.stdout.write(JSON.stringify({ status: result.duplicate ? 'duplicate' : 'inserted' }) + '\n');
} catch (error) {
  process.stdout.write(JSON.stringify({ status: 'error', code: error instanceof RuntimeError ? error.code : 'INTERNAL' }) + '\n');
} finally {
  store.close();
}
