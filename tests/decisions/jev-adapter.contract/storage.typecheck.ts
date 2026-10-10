import {
  DurableWorkflowStore,
  type LocalJevObservationInput,
  type LocalJevObservationDeliveryPage
} from '@jevelopers/agent-harness-runtime';

function verifyPrivateStorageTypes(store: DurableWorkflowStore): LocalJevObservationDeliveryPage {
  const marker = store.recordJevDispatchMarker({
    workflowId: 'jev-type-workflow',
    ownerId: 'jev-type-owner',
    fence: 1,
    expectedRevision: 2,
    attemptId: 'jev-type-billable-attempt',
    transportAttemptId: 'jev-type-transport'
  });
  const observation: LocalJevObservationInput = {
    workflowId: 'jev-type-workflow',
    logicalOperationRef: 'jev-type-logical-operation',
    attemptId: 'jev-type-billable-attempt',
    transportAttemptId: 'jev-type-transport',
    sourceObservationRef: 'jev-type-local-source',
    dispatchCertainty: 'response_received',
    requestedModel: 'jev-1.13.0',
    resolvedModel: 'jev-1.13.0',
    providerRequestId: null,
    decisionConformance: 'invalid',
    usage: { kind: 'known', inputTokens: 0, outputTokens: 0 }
  };
  const persisted = store.recordJevProviderObservation(observation);
  const stored = store.getJevProviderObservation(persisted.sourceObservationRef);
  const page = store.readJevObservationDeliveryPage({ afterSequence: 0, limit: 10 });
  const ack = store.acknowledgeJevObservationDelivery(marker.eventId);
  if (stored?.usage.kind === 'unknown' && ack.acknowledged && page.items[0]?.deliveryIntent) {
    return page;
  }
  return page;
}

void verifyPrivateStorageTypes;
