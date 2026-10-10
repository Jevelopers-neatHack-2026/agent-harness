import {
  DurableWorkflowStore,
  RuntimeError,
  type Lease,
  type SafeErrorRecord,
  type WorkflowState,
  parseRuntimeConfig
} from '@jevelopers/agent-harness-runtime';

function verifyInternalDeclarations(store: DurableWorkflowStore, lease: Lease): WorkflowState {
  const transition = store.transitionWorkflow({
    ...lease,
    expectedRevision: lease.revision,
    toState: 'running'
  });
  const intent = store.recordDispatchIntent({
    ...lease,
    expectedRevision: transition.revision,
    attemptId: 'typecheck-attempt',
    logicalOperationRef: 'typecheck-operation',
    authorityHoldRef: 'opaque-typecheck-hold',
    transportAttemptId: 'typecheck-transport'
  });
  const retry: ReturnType<DurableWorkflowStore['recordTransportAttempt']> = store.recordTransportAttempt({
    ...lease,
    expectedRevision: intent.revision,
    attemptId: intent.attemptId,
    transportAttemptId: 'typecheck-transport-retry'
  });
  const config = parseRuntimeConfig({
    HARNESS_MODE: 'mock',
    HARNESS_MODEL_ID: 'gpt-6-luna',
    HARNESS_DEADLINE_MS: '1000',
    HARNESS_MAX_ATTEMPTS: '2',
    HARNESS_MAX_ELAPSED_MS: '5000',
    HARNESS_MAX_COST_MINOR_UNITS: '0'
  });
  const safeError: SafeErrorRecord = new RuntimeError({
    module: 'state',
    operation: 'recordTransportAttempt',
    code: 'STORE_BUSY',
    retryable: true,
    safe_message: 'The durable store is busy; retry after the current write finishes.'
  }).toJSON();
  if (retry.eventSequence !== null && safeError.retryable && config.mode === 'mock') {
    return intent.state;
  }
  return 'failed';
}

void verifyInternalDeclarations;
