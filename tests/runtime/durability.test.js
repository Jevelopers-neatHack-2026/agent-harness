import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { test } from 'node:test';
import { DurableWorkflowStore, RuntimeError } from '@jevelopers/agent-harness-runtime';
import { spawnFixture, temporaryDatabase, waitForFiles } from './helpers.js';

function mutableClock(startAtMs) {
  let current = startAtMs;
  return {
    read: () => current,
    set: value => { current = value; }
  };
}

function openStore(databasePath, clock) {
  let sequence = 0;
  return DurableWorkflowStore.open({
    databasePath,
    clock,
    idFactory: () => 'generated-' + (++sequence)
  });
}

function createRunningWorkflow(store, workflowId, ttlMs = 1000) {
  store.createWorkflow({
    workflowId,
    checkpoint: { step: 'created' },
    eventId: workflowId + ':created'
  });
  const lease = store.acquireLease({
    workflowId,
    ownerId: 'worker-primary',
    ttlMs,
    eventId: workflowId + ':lease'
  });
  const running = store.transitionWorkflow({
    ...lease,
    expectedRevision: 0,
    toState: 'running',
    eventId: workflowId + ':running'
  });
  return { ...lease, revision: running.revision };
}

function recordIntent(store, lease, {
  attemptId,
  logicalOperationRef = 'operation-ref-' + attemptId,
  authorityHoldRef = 'hold-ref-' + attemptId,
  transportAttemptId = 'transport-' + attemptId
}) {
  return store.recordDispatchIntent({
    ...lease,
    expectedRevision: lease.revision,
    attemptId,
    logicalOperationRef,
    authorityHoldRef,
    transportAttemptId,
    eventId: 'intent-event-' + attemptId
  });
}

function fixturePath(name) {
  return join(new URL('./fixtures/', import.meta.url).pathname, name);
}

function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

function assertRuntimeCode(action, expectedCode) {
  assert.throws(action, error => error instanceof RuntimeError && error.code === expectedCode);
}

function invalidCheckpointCases() {
  const deep = {};
  let cursor = deep;
  for (let index = 0; index < 10_000; index += 1) {
    cursor.next = {};
    cursor = cursor.next;
  }
  const wide = Object.fromEntries(Array.from({ length: 10_001 }, (_, index) => ['field-' + index, index]));
  const oversized = { payload: '\u0000'.repeat(200_000) };
  const privateMarker = 'SYNTHETIC_PRIVATE_MARKER';
  let accessorCalls = 0;
  const accessor = Object.defineProperty({}, 'private_value', {
    enumerable: true,
    get() {
      accessorCalls += 1;
      throw new Error(privateMarker);
    }
  });
  const unexpected = new Proxy({}, {
    ownKeys() {
      throw new Error(privateMarker);
    }
  });
  return {
    privateMarker,
    accessorCalls: () => accessorCalls,
    cases: [
      ['deep', deep],
      ['wide', wide],
      ['serialized-size', oversized],
      ['accessor', accessor],
      ['unexpected', unexpected]
    ]
  };
}

function assertSafeCheckpointFailure(action, operation, privateMarker) {
  assert.throws(action, error => {
    assert.ok(error instanceof RuntimeError);
    assert.equal(error.operation, operation);
    assert.equal(error.code, 'INVALID_INPUT');
    assert.match(error.safe_message, /supported size and structure limits/);
    assert.equal(error.message, error.safe_message);
    assert.ok(!error.safe_message.includes(privateMarker));
    assert.ok(!JSON.stringify(error.toJSON()).includes(privateMarker));
    return true;
  });
}

test('response receipt survives process exit before checkpoint and reconciles after restart', async t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const clock = mutableClock(1000);
  const initialStore = openStore(temporary.databasePath, clock.read);
  t.after(() => initialStore.close());

  const lease = createRunningWorkflow(initialStore, 'crash-run', 10_000);
  const intent = recordIntent(initialStore, lease, {
    attemptId: 'attempt-crash',
    logicalOperationRef: 'logical-operation-opaque-17',
    authorityHoldRef: 'authority-hold-opaque-91',
    transportAttemptId: 'transport-crash-1'
  });
  assert.equal(intent.state, 'awaiting_response');
  assert.equal(intent.revision, 2);

  const released = initialStore.releaseLease({ ...lease, eventId: 'release-for-recovery' });
  assert.equal(released.state, 'reconciliation_pending');
  assert.equal(initialStore.getAttempt('attempt-crash').state, 'outcome_unknown');
  assert.equal(initialStore.getAttempt('attempt-crash').authorityHoldRef, 'authority-hold-opaque-91');
  initialStore.close();

  const child = spawnFixture(fixturePath('crash-after-receipt.mjs'), [temporary.databasePath]);
  const childResult = await child.done;
  assert.equal(childResult.code, 73, childResult.stderr);

  clock.set(1300);
  const store = openStore(temporary.databasePath, clock.read);
  t.after(() => store.close());
  assert.equal(store.getWorkflow('crash-run').state, 'reconciliation_pending');
  assert.equal(store.getCheckpoint('crash-run').sequence, 1);
  const response = store.getReceipt('attempt-crash');
  assert.equal(response.outcome, 'completed');
  assert.equal(response.responseSha256, sha256(Buffer.from('internal-response-fixture')));
  assert.deepEqual(response.responseBytes, Buffer.from('internal-response-fixture'));
  const artifact = store.getArtifact('crash-run', 'result-version-7');
  assert.deepEqual(artifact.bytes, Buffer.from([0, 1, 2, 255, 10]));
  assert.equal(artifact.sha256, sha256(artifact.bytes));

  const recoveryLease = store.acquireLease({
    workflowId: 'crash-run',
    ownerId: 'worker-recovery',
    ttlMs: 1000,
    eventId: 'recovery-worker-lease'
  });
  assert.equal(recoveryLease.fence, lease.fence + 2);
  assert.equal(recoveryLease.revision, 4);
  assertRuntimeCode(() => recordIntent(store, recoveryLease, {
    attemptId: 'must-not-repeat',
    logicalOperationRef: 'must-not-repeat-operation',
    authorityHoldRef: 'must-not-release-hold',
    transportAttemptId: 'must-not-repeat-transport'
  }), 'OUTCOME_UNRESOLVED');

  const reconciliation = store.reconcilePendingReceipts({
    ...recoveryLease,
    expectedRevision: recoveryLease.revision,
    eventIdPrefix: 'restart-reconcile'
  });
  assert.deepEqual(reconciliation, {
    reconciled: 1,
    state: 'running',
    revision: 5,
    checkpointSequence: 2
  });
  assert.deepEqual(store.getCheckpoint('crash-run').data, {
    step: 'response-finished',
    artifact_version_ref: 'version-7',
    approval_binding_hash: 'sha256-fixture'
  });
  assert.equal(store.getAttempt('attempt-crash').state, 'reconciled');
  assert.equal(store.getAttempt('attempt-crash').authorityHoldRef, 'authority-hold-opaque-91');

  const events = store.listEvents({ workflowId: 'crash-run' });
  assert.deepEqual(events.map(event => event.sequence), events.map((_, index) => index + 1));
  assert.equal(events.filter(event => event.type === 'dispatch_intent_persisted').length, 1);
  assert.equal(events.filter(event => event.type === 'response_receipt_persisted').length, 1);
  assert.equal(events.filter(event => event.type === 'response_checkpoint_reconciled').length, 1);
  const outbox = store.readOutbox();
  assert.deepEqual(outbox.map(event => event.sequence), events.map(event => event.sequence));
  assert.deepEqual(store.acknowledgeOutbox(events[0].eventId), { acknowledged: true });
  assert.deepEqual(store.acknowledgeOutbox(events[0].eventId), { acknowledged: true });
  assert.equal(store.readOutbox().length, events.length - 1);
});

test('checkpoint serialization preserves prototype-named JSON keys through receipt recovery', t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, () => 1000);
  t.after(() => store.close());
  const checkpoint = JSON.parse('{"ordinary":1,"__proto__":{"preserve":"top"},"nested":{"prototype":"prototype-value","__proto__":{"preserve":"nested"},"constructor":"constructor-value"}}');
  const expectedEnvelope = '{"schemaVersion":2,"data":{"__proto__":{"preserve":"top"},"nested":{"__proto__":{"preserve":"nested"},"constructor":"constructor-value","prototype":"prototype-value"},"ordinary":1}}';

  store.createWorkflow({ workflowId: 'prototype-run', checkpoint, eventId: 'prototype-created' });
  const initial = store.getCheckpoint('prototype-run');
  assert.deepEqual(initial.data, checkpoint);
  assert.equal(Object.getPrototypeOf(initial.data), Object.prototype);
  assert.equal(Object.hasOwn(initial.data, '__proto__'), true);
  assert.equal(Object.hasOwn(initial.data.nested, '__proto__'), true);
  assert.equal(Object.hasOwn(initial.data.nested, 'constructor'), true);
  assert.equal(Object.hasOwn(initial.data.nested, 'prototype'), true);
  assert.equal(initial.sha256, sha256(Buffer.from(expectedEnvelope)));
  assert.equal(Object.hasOwn({}, 'preserve'), false);

  const lease = createRunningWorkflow(store, 'receipt-prototype-run');
  const intent = recordIntent(store, lease, {
    attemptId: 'prototype-attempt',
    logicalOperationRef: 'prototype-operation',
    authorityHoldRef: 'opaque-prototype-hold'
  });
  const receipt = store.recordResponseReceipt({
    ...lease,
    expectedRevision: intent.revision,
    attemptId: 'prototype-attempt',
    receiptId: 'prototype-receipt',
    outcome: 'completed',
    nextState: 'running',
    responseBytes: Buffer.from('prototype-safe-fixture'),
    nextCheckpoint: checkpoint,
    eventId: 'prototype-response'
  });
  const reconciled = store.reconcilePendingReceipts({
    ...lease,
    expectedRevision: receipt.revision,
    eventIdPrefix: 'prototype-reconcile'
  });
  assert.equal(reconciled.checkpointSequence, 2);
  const recovered = store.getCheckpoint('receipt-prototype-run');
  assert.deepEqual(recovered.data, checkpoint);
  assert.equal(recovered.sha256, initial.sha256);
  assert.equal(Object.hasOwn(recovered.data, '__proto__'), true);
  assert.equal(Object.hasOwn(recovered.data.nested, '__proto__'), true);
  assert.equal(Object.hasOwn({}, 'preserve'), false);
});

test('checkpoint normalization is bounded, accessor-safe, and typed on both write paths', t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, () => 1000);
  t.after(() => store.close());
  const invalid = invalidCheckpointCases();
  store.createWorkflow({ workflowId: 'checkpoint-input-baseline', eventId: 'checkpoint-input-baseline-event' });
  const baselineEvents = store.listEvents();
  const baselineOutbox = store.readOutbox();

  for (const [name, checkpoint] of invalid.cases) {
    assertSafeCheckpointFailure(() => store.createWorkflow({
      workflowId: 'invalid-initial-' + name,
      checkpoint,
      eventId: 'invalid-initial-event-' + name
    }), 'createWorkflow', invalid.privateMarker);
    assertRuntimeCode(() => store.getWorkflow('invalid-initial-' + name), 'WORKFLOW_NOT_FOUND');
    assert.deepEqual(store.listEvents(), baselineEvents, name);
    assert.deepEqual(store.readOutbox(), baselineOutbox, name);
  }
  assert.equal(invalid.accessorCalls(), 0);

  const lease = createRunningWorkflow(store, 'checkpoint-receipt-input-run');
  const intent = recordIntent(store, lease, { attemptId: 'checkpoint-receipt-input-attempt' });
  const beforeReceiptEvents = store.listEvents({ workflowId: 'checkpoint-receipt-input-run' });
  const beforeReceiptOutbox = store.readOutbox();
  for (const [name, checkpoint] of invalid.cases) {
    assertSafeCheckpointFailure(() => store.recordResponseReceipt({
      ...lease,
      expectedRevision: intent.revision,
      attemptId: 'checkpoint-receipt-input-attempt',
      receiptId: 'invalid-receipt-' + name,
      outcome: 'completed',
      nextState: 'running',
      responseBytes: Buffer.from('safe-response-fixture'),
      nextCheckpoint: checkpoint,
      artifacts: [{ artifactId: 'invalid-artifact-' + name, bytes: Buffer.from('must-not-persist') }],
      eventId: 'invalid-receipt-event-' + name
    }), 'recordResponseReceipt', invalid.privateMarker);
    assert.equal(store.getWorkflow('checkpoint-receipt-input-run').revision, intent.revision);
    assert.equal(store.getAttempt('checkpoint-receipt-input-attempt').state, 'intent');
    assert.deepEqual(store.listEvents({ workflowId: 'checkpoint-receipt-input-run' }), beforeReceiptEvents, name);
    assert.deepEqual(store.readOutbox(), beforeReceiptOutbox, name);
    assertRuntimeCode(() => store.getReceipt('checkpoint-receipt-input-attempt'), 'ATTEMPT_NOT_FOUND');
    assertRuntimeCode(() => store.getArtifact('checkpoint-receipt-input-run', 'invalid-artifact-' + name), 'ARTIFACT_NOT_FOUND');
  }
  assert.equal(invalid.accessorCalls(), 0);
});

test('recovery rejects a corrupted receipt without advancing checkpoint or attempt state', t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const firstStore = openStore(temporary.databasePath, () => 1000);
  t.after(() => firstStore.close());
  const lease = createRunningWorkflow(firstStore, 'corrupt-receipt-run', 10_000);
  const intent = recordIntent(firstStore, lease, {
    attemptId: 'corrupt-receipt-attempt',
    logicalOperationRef: 'corrupt-receipt-operation',
    authorityHoldRef: 'opaque-corrupt-receipt-hold'
  });
  const receipt = firstStore.recordResponseReceipt({
    ...lease,
    expectedRevision: intent.revision,
    attemptId: 'corrupt-receipt-attempt',
    receiptId: 'corrupt-receipt-id',
    outcome: 'completed',
    nextState: 'running',
    responseBytes: Buffer.from('valid-response-bytes'),
    nextCheckpoint: { step: 'must-not-reconcile-corrupt-bytes' },
    eventId: 'corrupt-receipt-event'
  });
  firstStore.close();

  const tamper = new DatabaseSync(temporary.databasePath);
  tamper.exec('DROP TRIGGER response_receipts_no_update');
  tamper.prepare('UPDATE response_receipts SET response_bytes = ? WHERE attempt_id = ?')
    .run(Buffer.from('corrupted-response-bytes'), 'corrupt-receipt-attempt');
  tamper.close();

  const recoveredStore = openStore(temporary.databasePath, () => 1000);
  t.after(() => recoveredStore.close());
  const events = recoveredStore.listEvents({ workflowId: 'corrupt-receipt-run' });
  assertRuntimeCode(() => recoveredStore.reconcilePendingReceipts({
    ...lease,
    expectedRevision: receipt.revision,
    eventIdPrefix: 'corrupt-receipt-reconcile'
  }), 'CHECKPOINT_CORRUPT');
  assert.equal(recoveredStore.getWorkflow('corrupt-receipt-run').revision, receipt.revision);
  assert.equal(recoveredStore.getCheckpoint('corrupt-receipt-run').sequence, 1);
  assert.equal(recoveredStore.getAttempt('corrupt-receipt-attempt').state, 'response_received');
  assert.deepEqual(recoveredStore.listEvents({ workflowId: 'corrupt-receipt-run' }), events);
});

test('recovery rejects a valid-JSON receipt checkpoint changed after restart', t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, () => 1000);
  const lease = createRunningWorkflow(store, 'receipt-checkpoint-corrupt-run', 10_000);
  const intent = recordIntent(store, lease, {
    attemptId: 'receipt-checkpoint-corrupt-attempt',
    logicalOperationRef: 'receipt-checkpoint-corrupt-operation',
    authorityHoldRef: 'opaque-receipt-checkpoint-hold'
  });
  const receipt = store.recordResponseReceipt({
    ...lease,
    expectedRevision: intent.revision,
    attemptId: 'receipt-checkpoint-corrupt-attempt',
    receiptId: 'receipt-checkpoint-corrupt-id',
    outcome: 'completed',
    nextState: 'running',
    responseBytes: Buffer.from('unchanged-response-bytes'),
    nextCheckpoint: { artifact_version_ref: 'original-v1', approval_binding_hash: 'original-hash' },
    eventId: 'receipt-checkpoint-corrupt-event'
  });
  store.close();

  const tamper = new DatabaseSync(temporary.databasePath);
  tamper.exec('DROP TRIGGER response_receipts_no_update');
  const persistedReceipt = tamper.prepare('SELECT next_checkpoint_json, next_checkpoint_sha256 FROM response_receipts WHERE receipt_id = ?')
    .get('receipt-checkpoint-corrupt-id');
  assert.equal(persistedReceipt.next_checkpoint_sha256, sha256(Buffer.from(persistedReceipt.next_checkpoint_json)));
  tamper.prepare('UPDATE response_receipts SET next_checkpoint_json = ? WHERE receipt_id = ?')
    .run(JSON.stringify({
      schemaVersion: 2,
      data: { artifact_version_ref: 'altered-v2', approval_binding_hash: 'altered-hash' }
    }), 'receipt-checkpoint-corrupt-id');
  tamper.close();

  const recovered = openStore(temporary.databasePath, () => 1000);
  t.after(() => recovered.close());
  const before = {
    workflow: recovered.getWorkflow('receipt-checkpoint-corrupt-run'),
    checkpoint: recovered.getCheckpoint('receipt-checkpoint-corrupt-run'),
    attempt: recovered.getAttempt('receipt-checkpoint-corrupt-attempt'),
    events: recovered.listEvents({ workflowId: 'receipt-checkpoint-corrupt-run' }),
    outbox: recovered.readOutbox()
  };
  assertRuntimeCode(() => recovered.reconcilePendingReceipts({
    ...lease,
    expectedRevision: receipt.revision,
    eventIdPrefix: 'receipt-checkpoint-corrupt-reconcile'
  }), 'CHECKPOINT_CORRUPT');
  assert.deepEqual({
    workflow: recovered.getWorkflow('receipt-checkpoint-corrupt-run'),
    checkpoint: recovered.getCheckpoint('receipt-checkpoint-corrupt-run'),
    attempt: recovered.getAttempt('receipt-checkpoint-corrupt-attempt'),
    events: recovered.listEvents({ workflowId: 'receipt-checkpoint-corrupt-run' }),
    outbox: recovered.readOutbox()
  }, before);
  assert.equal(before.workflow.revision, receipt.revision);
  assert.equal(before.checkpoint.sequence, 1);
  assert.equal(before.attempt.state, 'response_received');
});

test('expired lease fences stale writers and lease fencing retains holds', t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const clock = mutableClock(1000);
  const store = openStore(temporary.databasePath, clock.read);
  t.after(() => store.close());
  const oldLease = createRunningWorkflow(store, 'expiry-run', 10);
  const intent = recordIntent(store, oldLease, {
    attemptId: 'attempt-expiry',
    authorityHoldRef: 'opaque-unresolved-hold',
    transportAttemptId: 'transport-expiry-1'
  });
  const retry = store.recordTransportAttempt({
    ...oldLease,
    expectedRevision: intent.revision,
    attemptId: 'attempt-expiry',
    transportAttemptId: 'transport-expiry-2',
    eventId: 'transport-expiry-retry'
  });

  clock.set(1010);
  const replacementLease = store.acquireLease({
    workflowId: 'expiry-run',
    ownerId: 'worker-replacement',
    ttlMs: 1000,
    eventId: 'replacement-lease'
  });
  assert.equal(replacementLease.fence, oldLease.fence + 1);
  assert.equal(replacementLease.revision, retry.revision + 1);
  assert.equal(store.getWorkflow('expiry-run').state, 'reconciliation_pending');
  assert.equal(store.getAttempt('attempt-expiry').state, 'outcome_unknown');
  assert.equal(store.getAttempt('attempt-expiry').authorityHoldRef, 'opaque-unresolved-hold');
  assert.deepEqual(
    store.getAttempt('attempt-expiry').transportAttempts.map(item => item.transportAttemptId),
    ['transport-expiry-1', 'transport-expiry-2']
  );
  assertRuntimeCode(() => store.transitionWorkflow({
    ...oldLease,
    expectedRevision: replacementLease.revision,
    toState: 'review_required',
    eventId: 'stale-worker-write'
  }), 'STALE_FENCE');
  assertRuntimeCode(() => recordIntent(store, replacementLease, {
    attemptId: 'attempt-must-wait',
    logicalOperationRef: 'new-operation',
    authorityHoldRef: 'new-hold',
    transportAttemptId: 'new-transport'
  }), 'OUTCOME_UNRESOLVED');

  assert.deepEqual(store.recordAttemptObservation({
    attemptId: 'attempt-expiry',
    sourceObservationRef: 'source-observation-1',
    kind: 'unknown'
  }), { duplicate: false, observationCount: 1 });
  assert.deepEqual(store.recordAttemptObservation({
    attemptId: 'attempt-expiry',
    sourceObservationRef: 'source-observation-1',
    kind: 'unknown'
  }), { duplicate: true, observationCount: 1 });
  assertRuntimeCode(() => store.recordAttemptObservation({
    attemptId: 'attempt-expiry',
    sourceObservationRef: 'source-observation-1',
    kind: 'known'
  }), 'OBSERVATION_IDENTITY_CONFLICT');
  assert.deepEqual(store.recordAttemptObservation({
    attemptId: 'attempt-expiry',
    sourceObservationRef: 'source-observation-2',
    kind: 'unknown'
  }), { duplicate: false, observationCount: 2 });

  const receipt = store.recordResponseReceipt({
    ...replacementLease,
    expectedRevision: replacementLease.revision,
    attemptId: 'attempt-expiry',
    receiptId: 'receipt-expiry-no-effect',
    outcome: 'no_effect',
    nextState: 'running',
    responseBytes: Buffer.from('authoritative-no-effect-fixture'),
    nextCheckpoint: { step: 'reconciled-no-effect' },
    eventId: 'expiry-no-effect-receipt'
  });
  const reconciled = store.reconcilePendingReceipts({
    ...replacementLease,
    expectedRevision: receipt.revision,
    eventIdPrefix: 'expiry-reconcile'
  });
  assert.equal(reconciled.state, 'running');
  const secondIntent = recordIntent(store, {
    ...replacementLease,
    revision: reconciled.revision
  }, {
    attemptId: 'attempt-distinct',
    logicalOperationRef: 'logical-operation-distinct',
    authorityHoldRef: 'opaque-hold-distinct',
    transportAttemptId: 'transport-distinct-1'
  });
  assert.equal(secondIntent.state, 'awaiting_response');
  assert.equal(store.getAttempt('attempt-distinct').authorityHoldRef, 'opaque-hold-distinct');
  assert.equal(store.getAttempt('attempt-distinct').state, 'intent');
  assert.equal(store.getAttempt('attempt-expiry').authorityHoldRef, 'opaque-unresolved-hold');
});

test('cancellation is idempotent from every nonterminal workflow state', t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, () => 1000);
  t.after(() => store.close());

  for (const initialState of ['queued', 'running', 'awaiting_response', 'reconciliation_pending', 'review_required']) {
    const workflowId = 'cancel-matrix-' + initialState;
    const ownerId = 'owner-' + initialState;
    store.createWorkflow({ workflowId, eventId: workflowId + ':created' });
    const lease = store.acquireLease({
      workflowId,
      ownerId,
      ttlMs: 10_000,
      eventId: workflowId + ':lease'
    });
    let revision = lease.revision;
    let attemptId;
    if (initialState !== 'queued') {
      const running = store.transitionWorkflow({
        ...lease,
        expectedRevision: revision,
        toState: 'running',
        eventId: workflowId + ':running'
      });
      revision = running.revision;
    }
    if (initialState === 'awaiting_response' || initialState === 'reconciliation_pending') {
      attemptId = 'attempt-' + initialState;
      const intent = store.recordDispatchIntent({
        ...lease,
        expectedRevision: revision,
        attemptId,
        logicalOperationRef: 'operation-' + initialState,
        authorityHoldRef: 'opaque-hold-' + initialState,
        transportAttemptId: 'transport-' + initialState,
        eventId: workflowId + ':intent'
      });
      revision = intent.revision;
      if (initialState === 'reconciliation_pending') {
        const unknown = store.markOutcomeUnknown({
          ...lease,
          expectedRevision: revision,
          attemptId,
          eventId: workflowId + ':unknown'
        });
        revision = unknown.revision;
      }
    }
    if (initialState === 'review_required') {
      const review = store.transitionWorkflow({
        ...lease,
        expectedRevision: revision,
        toState: 'review_required',
        eventId: workflowId + ':review'
      });
      revision = review.revision;
    }

    const unresolved = initialState === 'awaiting_response' || initialState === 'reconciliation_pending';
    const cancelled = store.requestCancellation({
      ...lease,
      expectedRevision: revision,
      eventId: workflowId + ':cancel'
    });
    assert.equal(cancelled.accepted, true, initialState);
    assert.equal(cancelled.state, unresolved ? 'reconciliation_pending' : 'cancelled', initialState);
    assert.equal(store.getWorkflow(workflowId).cancellationRequested, true, initialState);
    if (unresolved) {
      const attempt = store.getAttempt(attemptId);
      assert.equal(attempt.authorityHoldRef, 'opaque-hold-' + initialState);
      assert.equal(attempt.state, initialState === 'awaiting_response' ? 'intent' : 'outcome_unknown');
    }

    const eventCount = store.listEvents({ workflowId }).length;
    const repeated = store.requestCancellation({
      ...lease,
      expectedRevision: cancelled.revision,
      eventId: workflowId + ':cancel-repeat'
    });
    assert.equal(repeated.accepted, unresolved, initialState);
    assert.equal(repeated.state, cancelled.state, initialState);
    assert.equal(repeated.revision, cancelled.revision, initialState);
    assert.equal(store.listEvents({ workflowId }).length, eventCount, initialState);
  }
});

test('accepted cancellation blocks new transport retries but accepts late evidence and receipts', t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, () => 1000);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'cancel-retry-run', 10_000);
  const intent = recordIntent(store, lease, {
    attemptId: 'cancel-retry-attempt',
    logicalOperationRef: 'cancel-retry-operation',
    authorityHoldRef: 'opaque-cancel-retry-hold',
    transportAttemptId: 'cancel-retry-transport-1'
  });
  const cancellation = store.requestCancellation({
    ...lease,
    expectedRevision: intent.revision,
    eventId: 'cancel-retry-request'
  });
  assert.equal(cancellation.state, 'reconciliation_pending');
  const eventCount = store.listEvents({ workflowId: 'cancel-retry-run' }).length;
  assertRuntimeCode(() => store.recordTransportAttempt({
    ...lease,
    expectedRevision: cancellation.revision,
    attemptId: 'cancel-retry-attempt',
    transportAttemptId: 'cancel-retry-transport-after-cancel',
    eventId: 'cancel-retry-forbidden-event'
  }), 'ATTEMPT_STATE_INVALID');
  assert.equal(store.getWorkflow('cancel-retry-run').revision, cancellation.revision);
  assert.equal(store.listEvents({ workflowId: 'cancel-retry-run' }).length, eventCount);
  assert.deepEqual(store.getAttempt('cancel-retry-attempt').transportAttempts.map(item => item.transportAttemptId), [
    'cancel-retry-transport-1'
  ]);

  assert.deepEqual(store.recordAttemptObservation({
    attemptId: 'cancel-retry-attempt',
    sourceObservationRef: 'late-source-observation',
    kind: 'known'
  }), { duplicate: false, observationCount: 1 });
  assert.deepEqual(store.recordAttemptObservation({
    attemptId: 'cancel-retry-attempt',
    sourceObservationRef: 'late-source-observation',
    kind: 'known'
  }), { duplicate: true, observationCount: 1 });
  const receipt = store.recordResponseReceipt({
    ...lease,
    expectedRevision: cancellation.revision,
    attemptId: 'cancel-retry-attempt',
    receiptId: 'cancel-retry-late-receipt',
    outcome: 'completed',
    nextState: 'running',
    responseBytes: Buffer.from('late-completed-response-fixture'),
    nextCheckpoint: { step: 'late-response-reconciled' },
    eventId: 'cancel-retry-late-response'
  });
  const reconciled = store.reconcilePendingReceipts({
    ...lease,
    expectedRevision: receipt.revision,
    eventIdPrefix: 'cancel-retry-reconcile'
  });
  assert.equal(reconciled.state, 'cancelled');
  assert.equal(store.getWorkflow('cancel-retry-run').cancellationRequested, true);
  assert.equal(store.getCheckpoint('cancel-retry-run').data.step, 'late-response-reconciled');
  assert.equal(store.getReceipt('cancel-retry-attempt').outcome, 'completed');
  assert.equal(store.getAttempt('cancel-retry-attempt').state, 'reconciled');
  assert.equal(store.getAttempt('cancel-retry-attempt').authorityHoldRef, 'opaque-cancel-retry-hold');
});

test('Node 22 SQLite numeric busy errors are retryable and leave no partial observation', t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const store = DurableWorkflowStore.open({
    databasePath: temporary.databasePath,
    clock: () => 1000,
    idFactory: () => 'busy-generated',
    busyTimeoutMs: 10
  });
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'busy-run', 10_000);
  recordIntent(store, lease, { attemptId: 'busy-attempt' });
  const blocker = new DatabaseSync(temporary.databasePath, { timeout: 10 });
  try {
    blocker.exec('BEGIN IMMEDIATE');
    const startedAt = Date.now();
    assert.throws(() => store.recordAttemptObservation({
      attemptId: 'busy-attempt',
      sourceObservationRef: 'busy-source-observation',
      kind: 'known'
    }), error => {
      assert.ok(error instanceof RuntimeError);
      assert.equal(error.code, 'STORE_BUSY');
      assert.equal(error.retryable, true);
      assert.match(error.safe_message, /durable store is busy/i);
      assert.doesNotMatch(JSON.stringify(error), /database is locked|SQLITE_BUSY|errstr/i);
      return true;
    });
    assert.ok(Date.now() - startedAt < 1000, 'the configured 10 ms SQLite lock wait is bounded');
  } finally {
    if (blocker.isTransaction) blocker.exec('ROLLBACK');
    blocker.close();
  }
  assert.deepEqual(store.recordAttemptObservation({
    attemptId: 'busy-attempt',
    sourceObservationRef: 'busy-source-observation',
    kind: 'known'
  }), { duplicate: false, observationCount: 1 });
  assert.deepEqual(store.recordAttemptObservation({
    attemptId: 'busy-attempt',
    sourceObservationRef: 'busy-source-observation',
    kind: 'known'
  }), { duplicate: true, observationCount: 1 });
});

test('duplicate event identities and reordered transitions leave durable order unchanged', t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, () => 1000);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'event-order-run', 10_000);
  const beforeDuplicate = store.listEvents({ workflowId: 'event-order-run' });
  assertRuntimeCode(() => store.renewLease({
    ...lease,
    ttlMs: 5000,
    eventId: 'event-order-run:running'
  }), 'IDEMPOTENCY_CONFLICT');
  assert.deepEqual(store.listEvents({ workflowId: 'event-order-run' }), beforeDuplicate);

  const advanced = store.transitionWorkflow({
    ...lease,
    expectedRevision: lease.revision,
    toState: 'review_required',
    eventId: 'event-order-run:review'
  });
  const orderedEvents = store.listEvents({ workflowId: 'event-order-run' });
  assertRuntimeCode(() => store.transitionWorkflow({
    ...lease,
    expectedRevision: lease.revision,
    toState: 'succeeded',
    eventId: 'event-order-run:stale-success'
  }), 'CONCURRENT_MODIFICATION');
  assertRuntimeCode(() => store.transitionWorkflow({
    ...lease,
    expectedRevision: advanced.revision,
    toState: 'failed',
    eventId: 'event-order-run:illegal-review-exit'
  }), 'ILLEGAL_TRANSITION');
  assert.deepEqual(store.listEvents({ workflowId: 'event-order-run' }), orderedEvents);
  assert.deepEqual(orderedEvents.map(event => event.sequence), [1, 2, 3, 4]);
});

test('terminal workflow states reject later transitions', t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, () => 1000);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'terminal-run', 10_000);
  const completed = store.transitionWorkflow({
    ...lease,
    expectedRevision: lease.revision,
    toState: 'succeeded',
    eventId: 'terminal-run:succeeded'
  });
  const events = store.listEvents({ workflowId: 'terminal-run' });
  assertRuntimeCode(() => store.transitionWorkflow({
    ...lease,
    expectedRevision: completed.revision,
    toState: 'failed',
    eventId: 'terminal-run:reversed'
  }), 'TERMINAL_STATE');
  assert.equal(store.getWorkflow('terminal-run').state, 'succeeded');
  assert.deepEqual(store.listEvents({ workflowId: 'terminal-run' }), events);
});

test('multi-process compare-and-swap preserves event and outbox order', async t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const setupStore = openStore(temporary.databasePath, () => 1000);
  const lease = createRunningWorkflow(setupStore, 'cas-run', 60_000);
  setupStore.close();

  const gatePath = join(temporary.directory, 'release-workers');
  const workers = Array.from({ length: 10 }, (_, index) => spawnFixture(
    fixturePath('concurrent-cas.mjs'),
    [
      temporary.databasePath,
      'cas-run',
      lease.ownerId,
      String(lease.fence),
      String(lease.revision),
      'review_required',
      'cas-writer-' + index,
      gatePath
    ]
  ));
  await waitForFiles(Array.from({ length: 10 }, (_, index) => gatePath + '.cas-writer-' + index + '.ready'));
  writeFileSync(gatePath, 'ready');
  const results = await Promise.all(workers.map(worker => worker.done));
  const parsed = results.map(result => ({ ...result, record: JSON.parse(result.stdout) }));
  const winners = parsed.filter(result => result.record.accepted === true);
  const rejected = parsed.filter(result => result.record.accepted === false);
  assert.equal(winners.length, 1);
  assert.equal(winners[0].code, 0);
  assert.equal(rejected.length, 9);
  assert.ok(rejected.every(result => result.code === 2 && result.record.code === 'CONCURRENT_MODIFICATION'));

  const store = openStore(temporary.databasePath, () => 1000);
  t.after(() => store.close());
  assert.equal(store.getWorkflow('cas-run').state, 'review_required');
  assert.equal(store.getWorkflow('cas-run').revision, 2);
  const events = store.listEvents({ workflowId: 'cas-run' });
  const outbox = store.readOutbox();
  assert.deepEqual(events.map(event => event.sequence), [1, 2, 3, 4]);
  assert.deepEqual(outbox.map(event => event.sequence), events.map(event => event.sequence));
  assert.deepEqual(outbox.map(event => event.eventId), events.map(event => event.eventId));
  assert.deepEqual(store.acknowledgeOutbox('cas-run:created'), { acknowledged: true });
  assert.deepEqual(store.acknowledgeOutbox('cas-run:created'), { acknowledged: true });
  assert.equal(store.readOutbox().length, 3);
});

test('cancellation and completion serialize across independent processes', async t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const setupStore = openStore(temporary.databasePath, () => 1000);
  const lease = createRunningWorkflow(setupStore, 'race-run', 60_000);
  setupStore.close();

  const gatePath = join(temporary.directory, 'release-race');
  const cancellation = spawnFixture(fixturePath('cancel-or-complete.mjs'), [
    temporary.databasePath,
    'race-run',
    lease.ownerId,
    String(lease.fence),
    String(lease.revision),
    'cancel',
    'race-cancel',
    gatePath
  ]);
  const completion = spawnFixture(fixturePath('cancel-or-complete.mjs'), [
    temporary.databasePath,
    'race-run',
    lease.ownerId,
    String(lease.fence),
    String(lease.revision),
    'complete',
    'race-complete',
    gatePath
  ]);
  await waitForFiles([
    gatePath + '.race-cancel.ready',
    gatePath + '.race-complete.ready'
  ]);
  writeFileSync(gatePath, 'ready');
  const [cancelResult, completeResult] = await Promise.all([cancellation.done, completion.done]);
  const cancelRecord = JSON.parse(cancelResult.stdout);
  const completeRecord = JSON.parse(completeResult.stdout);

  const store = openStore(temporary.databasePath, () => 1000);
  t.after(() => store.close());
  const finalWorkflow = store.getWorkflow('race-run');
  assert.ok(['cancelled', 'succeeded'].includes(finalWorkflow.state));
  assert.equal(finalWorkflow.revision, 2);
  const stateEvents = store.listEvents({ workflowId: 'race-run' })
    .filter(event => event.revision > 1);
  assert.equal(stateEvents.length, 1);
  assert.equal(stateEvents[0].toState, finalWorkflow.state);
  if (finalWorkflow.state === 'cancelled') {
    assert.equal(cancelResult.code, 0);
    assert.equal(cancelRecord.accepted, true);
    assert.equal(completeResult.code, 2);
    assert.equal(completeRecord.code, 'LEASE_REQUIRED');
  } else {
    assert.equal(completeResult.code, 0);
    assert.equal(completeRecord.accepted, true);
    assert.equal(cancelResult.code, 0);
    assert.equal(cancelRecord.accepted, false);
  }
});

test('cancellation and transport retry race with one CAS winner, then cancellation fences retries', async t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const setupStore = openStore(temporary.databasePath, () => 1000);
  const lease = createRunningWorkflow(setupStore, 'cancel-transport-race-run', 60_000);
  const intent = recordIntent(setupStore, lease, {
    attemptId: 'cancel-transport-race-attempt',
    authorityHoldRef: 'opaque-race-hold',
    transportAttemptId: 'transport-race-initial'
  });
  setupStore.close();

  const gatePath = join(temporary.directory, 'release-cancel-transport-race');
  const cancellation = spawnFixture(fixturePath('cancel-or-transport.mjs'), [
    temporary.databasePath,
    'cancel-transport-race-run',
    lease.ownerId,
    String(lease.fence),
    String(intent.revision),
    'cancel',
    'cancel-transport-race-cancel',
    gatePath
  ]);
  const transport = spawnFixture(fixturePath('cancel-or-transport.mjs'), [
    temporary.databasePath,
    'cancel-transport-race-run',
    lease.ownerId,
    String(lease.fence),
    String(intent.revision),
    'transport',
    'cancel-transport-race-retry',
    gatePath
  ]);
  await waitForFiles([
    gatePath + '.cancel-transport-race-cancel.ready',
    gatePath + '.cancel-transport-race-retry.ready'
  ]);
  writeFileSync(gatePath, 'ready');
  const [cancelResult, transportResult] = await Promise.all([cancellation.done, transport.done]);
  const cancelRecord = JSON.parse(cancelResult.stdout);
  const transportRecord = JSON.parse(transportResult.stdout);
  assert.equal([cancelRecord.accepted, transportRecord.accepted].filter(Boolean).length, 1);
  assert.equal(cancelResult.code === 0, cancelRecord.accepted);
  assert.equal(transportResult.code === 0, transportRecord.accepted);
  const losingRecord = cancelRecord.accepted ? transportRecord : cancelRecord;
  const losingResult = cancelRecord.accepted ? transportResult : cancelResult;
  assert.equal(losingResult.code, 2);
  assert.equal(losingRecord.code, 'CONCURRENT_MODIFICATION');

  const store = openStore(temporary.databasePath, () => 1000);
  t.after(() => store.close());
  let workflow = store.getWorkflow('cancel-transport-race-run');
  if (!cancelRecord.accepted) {
    const acceptedCancellation = store.requestCancellation({
      ...lease,
      expectedRevision: workflow.revision,
      eventId: 'cancel-transport-race-retry-cancel'
    });
    assert.equal(acceptedCancellation.accepted, true);
    workflow = store.getWorkflow('cancel-transport-race-run');
  }
  assert.equal(workflow.cancellationRequested, true);
  assert.equal(workflow.state, 'reconciliation_pending');
  const eventCount = store.listEvents({ workflowId: 'cancel-transport-race-run' }).length;
  assertRuntimeCode(() => store.recordTransportAttempt({
    ...lease,
    expectedRevision: workflow.revision,
    attemptId: 'cancel-transport-race-attempt',
    transportAttemptId: 'transport-race-post-cancellation',
    eventId: 'cancel-transport-race-post-cancel-event'
  }), 'ATTEMPT_STATE_INVALID');
  assert.equal(store.getWorkflow('cancel-transport-race-run').revision, workflow.revision);
  assert.equal(store.listEvents({ workflowId: 'cancel-transport-race-run' }).length, eventCount);
  assert.equal(store.getAttempt('cancel-transport-race-attempt').authorityHoldRef, 'opaque-race-hold');
  const transports = store.getAttempt('cancel-transport-race-attempt').transportAttempts
    .map(item => item.transportAttemptId);
  assert.deepEqual(transports, cancelRecord.accepted
    ? ['transport-race-initial']
    : ['transport-race-initial', 'transport-race-retry']);
});

test('completed logical operation cannot redispatch after subprocess restart', async t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const setupStore = openStore(temporary.databasePath, () => 1000);
  const lease = createRunningWorkflow(setupStore, 'completed-operation-run', 60_000);
  setupStore.close();

  const child = spawnFixture(fixturePath('complete-logical-operation.mjs'), [
    temporary.databasePath,
    'completed-operation-run',
    lease.ownerId,
    String(lease.fence),
    String(lease.revision)
  ]);
  const childResult = await child.done;
  assert.equal(childResult.code, 0, childResult.stderr);
  assert.deepEqual(JSON.parse(childResult.stdout), {
    state: 'running',
    revision: 4,
    checkpointSequence: 2
  });

  const store = openStore(temporary.databasePath, () => 1000);
  t.after(() => store.close());
  const workflow = store.getWorkflow('completed-operation-run');
  const beforeEvents = store.listEvents({ workflowId: 'completed-operation-run' });
  assert.equal(store.getCheckpoint('completed-operation-run').data.step, 'completed-operation-durable');
  assert.equal(store.getAttempt('completed-operation-attempt').state, 'reconciled');
  assert.equal(store.getAttempt('completed-operation-attempt').authorityHoldRef, 'opaque-completed-operation-hold');
  assert.equal(store.getReceipt('completed-operation-attempt').outcome, 'completed');
  assertRuntimeCode(() => recordIntent(store, { ...lease, revision: workflow.revision }, {
    attemptId: 'completed-operation-replay-attempt',
    logicalOperationRef: 'completed-logical-operation',
    authorityHoldRef: 'opaque-replay-hold',
    transportAttemptId: 'completed-operation-replay-transport'
  }), 'LOGICAL_OPERATION_COMPLETED');
  assert.equal(store.getWorkflow('completed-operation-run').revision, workflow.revision);
  assert.deepEqual(store.listEvents({ workflowId: 'completed-operation-run' }), beforeEvents);
  assertRuntimeCode(() => store.getAttempt('completed-operation-replay-attempt'), 'ATTEMPT_NOT_FOUND');

  const distinctOperation = recordIntent(store, { ...lease, revision: workflow.revision }, {
    attemptId: 'different-logical-operation-attempt',
    logicalOperationRef: 'different-logical-operation',
    authorityHoldRef: 'opaque-different-operation-hold',
    transportAttemptId: 'different-logical-operation-transport'
  });
  assert.equal(distinctOperation.state, 'awaiting_response');
  assert.equal(store.getAttempt('completed-operation-attempt').state, 'reconciled');
});

test('attempt observations deduplicate by source identity while attempts stay distinct', t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath, () => 1000);
  t.after(() => store.close());
  const lease = createRunningWorkflow(store, 'identity-run', 10_000);
  const first = recordIntent(store, lease, {
    attemptId: 'billable-attempt-one',
    logicalOperationRef: 'logical-operation-one',
    authorityHoldRef: 'opaque-hold-one',
    transportAttemptId: 'transport-one'
  });
  const transportRetry = store.recordTransportAttempt({
    ...lease,
    expectedRevision: first.revision,
    attemptId: 'billable-attempt-one',
    transportAttemptId: 'transport-one-retry',
    eventId: 'transport-retry-event'
  });
  assert.equal(transportRetry.duplicate, false);
  assert.deepEqual(store.getAttempt('billable-attempt-one').transportAttempts.map(item => item.transportAttemptId), [
    'transport-one',
    'transport-one-retry'
  ]);
  assert.deepEqual(store.recordAttemptObservation({
    attemptId: 'billable-attempt-one',
    sourceObservationRef: 'source-one',
    kind: 'known'
  }), { duplicate: false, observationCount: 1 });
  assert.deepEqual(store.recordAttemptObservation({
    attemptId: 'billable-attempt-one',
    sourceObservationRef: 'source-one',
    kind: 'known'
  }), { duplicate: true, observationCount: 1 });
  assert.deepEqual(store.recordAttemptObservation({
    attemptId: 'billable-attempt-one',
    sourceObservationRef: 'source-two',
    kind: 'known'
  }), { duplicate: false, observationCount: 2 });
  assertRuntimeCode(() => store.recordAttemptObservation({
    attemptId: 'billable-attempt-one',
    sourceObservationRef: 'source-one',
    kind: 'unknown'
  }), 'OBSERVATION_IDENTITY_CONFLICT');

  const noEffectReceipt = store.recordResponseReceipt({
    ...lease,
    expectedRevision: transportRetry.revision,
    attemptId: 'billable-attempt-one',
    receiptId: 'no-effect-receipt-one',
    outcome: 'no_effect',
    nextState: 'running',
    responseBytes: Buffer.from('authoritative-no-effect-attempt-one'),
    nextCheckpoint: { step: 'no-effect-reconciled' },
    eventId: 'no-effect-response-one'
  });
  const noEffectReconciled = store.reconcilePendingReceipts({
    ...lease,
    expectedRevision: noEffectReceipt.revision,
    eventIdPrefix: 'no-effect-reconcile-one'
  });
  assert.equal(store.getAttempt('billable-attempt-one').state, 'reconciled');

  const secondAttempt = store.recordDispatchIntent({
    ...lease,
    expectedRevision: noEffectReconciled.revision,
    attemptId: 'billable-attempt-two',
    logicalOperationRef: 'logical-operation-one',
    authorityHoldRef: 'opaque-hold-two',
    transportAttemptId: 'transport-two',
    eventId: 'intent-event-two'
  });
  assert.equal(secondAttempt.state, 'awaiting_response');
  assert.equal(store.getAttempt('billable-attempt-one').authorityHoldRef, 'opaque-hold-one');
  assert.equal(store.getAttempt('billable-attempt-one').state, 'reconciled');
  assert.equal(store.getReceipt('billable-attempt-one').outcome, 'no_effect');
  assert.equal(store.getAttempt('billable-attempt-two').authorityHoldRef, 'opaque-hold-two');
  assert.equal(store.getAttempt('billable-attempt-two').logicalOperationRef, 'logical-operation-one');
  assert.deepEqual(store.recordAttemptObservation({
    attemptId: 'billable-attempt-two',
    sourceObservationRef: 'source-three',
    kind: 'known'
  }), { duplicate: false, observationCount: 1 });
  assert.deepEqual(store.recordAttemptObservation({
    attemptId: 'billable-attempt-two',
    sourceObservationRef: 'source-three',
    kind: 'known'
  }), { duplicate: true, observationCount: 1 });
  assertRuntimeCode(() => store.recordAttemptObservation({
    attemptId: 'billable-attempt-two',
    sourceObservationRef: 'source-one',
    kind: 'known'
  }), 'OBSERVATION_IDENTITY_CONFLICT');
});

test('v1 checkpoint migration preserves data and identifiers', t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const legacyPath = temporary.databasePath;
  const legacy = new DatabaseSync(legacyPath);
  legacy.exec('CREATE TABLE workflows (workflow_id TEXT PRIMARY KEY, state TEXT NOT NULL, revision INTEGER NOT NULL, checkpoint_json TEXT NOT NULL, created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL);');
  const legacyCheckpoint = JSON.stringify({
    step: 'before-upgrade',
    artifact_version_ref: 'owned-by-content-service:version-4',
    approval_binding_hash: 'owned-by-approval-service:hash-9'
  });
  legacy.prepare('INSERT INTO workflows VALUES (?, ?, ?, ?, ?, ?)').run(
    'legacy-workflow-7',
    'running',
    7,
    legacyCheckpoint,
    111,
    222
  );
  legacy.exec('PRAGMA user_version = 1');
  legacy.close();

  const store = openStore(legacyPath, () => 300);
  t.after(() => store.close());
  const workflow = store.getWorkflow('legacy-workflow-7');
  assert.equal(workflow.state, 'running');
  assert.equal(workflow.revision, 7);
  assert.equal(workflow.fence, 0);
  assert.equal(workflow.checkpointSequence, 1);
  assert.deepEqual(store.getCheckpoint('legacy-workflow-7').data, JSON.parse(legacyCheckpoint));
  const events = store.listEvents({ workflowId: 'legacy-workflow-7' });
  assert.equal(events.length, 1);
  assert.equal(events[0].eventId, 'migration:v1:legacy-workflow-7');
  assert.equal(events[0].type, 'schema_migrated_v1');
  assert.deepEqual(store.readOutbox().map(item => item.eventId), [events[0].eventId]);
  store.close();

  const migrated = new DatabaseSync(legacyPath, { readOnly: true });
  const timestamps = migrated.prepare(
    'SELECT created_at_ms, updated_at_ms FROM workflows WHERE workflow_id = ?'
  ).get('legacy-workflow-7');
  assert.deepEqual({ ...timestamps }, { created_at_ms: 111, updated_at_ms: 222 });
  assert.equal(migrated.prepare('PRAGMA user_version').get().user_version, 6);
  migrated.close();
});

test('invalid v1 checkpoint migration rolls back every schema and row change', t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const legacy = new DatabaseSync(temporary.databasePath);
  legacy.exec('CREATE TABLE workflows (workflow_id TEXT PRIMARY KEY, state TEXT NOT NULL, revision INTEGER NOT NULL, checkpoint_json TEXT NOT NULL, created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL);');
  const insert = legacy.prepare('INSERT INTO workflows VALUES (?, ?, ?, ?, ?, ?)');
  insert.run('a-valid', 'running', 1, '{"step":"valid"}', 100, 101);
  insert.run('z-invalid', 'running', 2, '{broken-json', 200, 201);
  legacy.exec('PRAGMA user_version = 1');
  legacy.close();

  assertRuntimeCode(() => openStore(temporary.databasePath, () => 1000), 'CHECKPOINT_CORRUPT');
  const unchanged = new DatabaseSync(temporary.databasePath, { readOnly: true });
  assert.equal(unchanged.prepare('PRAGMA user_version').get().user_version, 1);
  assert.equal(unchanged.prepare("SELECT COUNT(*) AS count FROM sqlite_master WHERE type='table' AND name='workflows'").get().count, 1);
  assert.equal(unchanged.prepare("SELECT COUNT(*) AS count FROM sqlite_master WHERE type='table' AND name='workflows_v1'").get().count, 0);
  assert.deepEqual(unchanged.prepare('SELECT workflow_id, checkpoint_json FROM workflows ORDER BY workflow_id').all().map(row => ({ ...row })), [
    { workflow_id: 'a-valid', checkpoint_json: '{"step":"valid"}' },
    { workflow_id: 'z-invalid', checkpoint_json: '{broken-json' }
  ]);
  unchanged.close();
});

test('v1 migration rejects mixed BLOB and numeric checkpoints without changing rows or schema', t => {
  const invalidValues = [
    ['blob', Buffer.from('{"artifact_version_ref":"preserve-this-bytes"}')],
    ['integer', 73]
  ];
  for (const [label, invalidCheckpoint] of invalidValues) {
    const temporary = temporaryDatabase('legacy-checkpoint-' + label + '-');
    t.after(() => temporary.cleanup());
    const legacy = new DatabaseSync(temporary.databasePath);
    legacy.exec('CREATE TABLE workflows (workflow_id TEXT PRIMARY KEY, state TEXT NOT NULL, revision INTEGER NOT NULL, checkpoint_json TEXT NOT NULL, created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL);');
    const insert = legacy.prepare('INSERT INTO workflows VALUES (?, ?, ?, ?, ?, ?)');
    insert.run('a-valid', 'running', 1, '{"step":"valid-first-row"}', 100, 101);
    insert.run('z-invalid', 'running', 2, invalidCheckpoint, 200, 201);
    legacy.exec('PRAGMA user_version = 1');
    const before = {
      objects: legacy.prepare('SELECT type, name, sql FROM sqlite_master ORDER BY type, name').all().map(row => ({ ...row })),
      rows: legacy.prepare(`
        SELECT workflow_id, typeof(checkpoint_json) AS storage_type,
          quote(checkpoint_json) AS quoted_value,
          hex(CAST(checkpoint_json AS BLOB)) AS bytes_hex
        FROM workflows ORDER BY workflow_id
      `).all().map(row => ({ ...row }))
    };
    legacy.close();

    assertRuntimeCode(() => openStore(temporary.databasePath, () => 1000), 'CHECKPOINT_CORRUPT');
    const unchanged = new DatabaseSync(temporary.databasePath, { readOnly: true });
    assert.equal(unchanged.prepare('PRAGMA user_version').get().user_version, 1, label);
    assert.deepEqual(unchanged.prepare('SELECT type, name, sql FROM sqlite_master ORDER BY type, name').all().map(row => ({ ...row })), before.objects, label);
    assert.deepEqual(unchanged.prepare(`
      SELECT workflow_id, typeof(checkpoint_json) AS storage_type,
        quote(checkpoint_json) AS quoted_value,
        hex(CAST(checkpoint_json AS BLOB)) AS bytes_hex
      FROM workflows ORDER BY workflow_id
    `).all().map(row => ({ ...row })), before.rows, label);
    assert.equal(unchanged.prepare("SELECT COUNT(*) AS count FROM sqlite_master WHERE type='table' AND name='workflow_checkpoints'").get().count, 0, label);
    assert.equal(unchanged.prepare("SELECT COUNT(*) AS count FROM sqlite_master WHERE type='table' AND name='workflow_events'").get().count, 0, label);
    unchanged.close();
  }
});

test('v2 receipts without checkpoint integrity evidence migrate but cannot reconcile', t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const first = openStore(temporary.databasePath, () => 1000);
  const lease = createRunningWorkflow(first, 'legacy-receipt-run', 10_000);
  const intent = recordIntent(first, lease, {
    attemptId: 'legacy-receipt-attempt',
    logicalOperationRef: 'legacy-receipt-operation',
    authorityHoldRef: 'opaque-legacy-receipt-hold'
  });
  const receipt = first.recordResponseReceipt({
    ...lease,
    expectedRevision: intent.revision,
    attemptId: 'legacy-receipt-attempt',
    receiptId: 'legacy-receipt-id',
    outcome: 'completed',
    nextState: 'running',
    responseBytes: Buffer.from('legacy-response-bytes'),
    nextCheckpoint: { step: 'old-format-pending' },
    eventId: 'legacy-receipt-event'
  });
  first.close();

  const oldFormat = new DatabaseSync(temporary.databasePath);
  oldFormat.exec(`
    PRAGMA foreign_keys = OFF;
    DROP TRIGGER operations_event_reservation;
    DROP TRIGGER operations_work_payload_immutable;
    DROP INDEX operations_attempts_workflow_state_idx;
    DROP INDEX operations_attempts_state_workflow_idx;
    DROP INDEX operations_work_scope_page_idx;
    DROP INDEX operations_work_lease_idx;
    DROP TABLE operations_work;
    DROP TABLE operations_projects;
    DROP TABLE operations_tenants;
    DROP TABLE operations_meta;
    ALTER TABLE response_receipts DROP COLUMN next_checkpoint_sha256;
    DROP TABLE jev_local_observation_outbox;
    DROP TABLE jev_provider_observations;
    DROP TABLE jev_dispatch_markers;
    DROP TABLE telemetry_cursors;
    PRAGMA user_version = 2;
  `);
  oldFormat.close();

  const migrated = openStore(temporary.databasePath, () => 1000);
  t.after(() => migrated.close());
  const raw = new DatabaseSync(temporary.databasePath, { readOnly: true });
  assert.equal(raw.prepare('PRAGMA user_version').get().user_version, 6);
  const migratedReceipt = raw.prepare('SELECT next_checkpoint_json, next_checkpoint_sha256 FROM response_receipts WHERE receipt_id = ?').get('legacy-receipt-id');
  assert.equal(migratedReceipt.next_checkpoint_json, '{"schemaVersion":2,"data":{"step":"old-format-pending"}}');
  assert.equal(migratedReceipt.next_checkpoint_sha256, null);
  raw.close();

  const before = {
    workflow: migrated.getWorkflow('legacy-receipt-run'),
    checkpoint: migrated.getCheckpoint('legacy-receipt-run'),
    attempt: migrated.getAttempt('legacy-receipt-attempt'),
    events: migrated.listEvents({ workflowId: 'legacy-receipt-run' }),
    outbox: migrated.readOutbox()
  };
  assertRuntimeCode(() => migrated.reconcilePendingReceipts({
    ...lease,
    expectedRevision: receipt.revision,
    eventIdPrefix: 'legacy-receipt-reconcile'
  }), 'CHECKPOINT_CORRUPT');
  assert.deepEqual({
    workflow: migrated.getWorkflow('legacy-receipt-run'),
    checkpoint: migrated.getCheckpoint('legacy-receipt-run'),
    attempt: migrated.getAttempt('legacy-receipt-attempt'),
    events: migrated.listEvents({ workflowId: 'legacy-receipt-run' }),
    outbox: migrated.readOutbox()
  }, before);
  assert.equal(before.checkpoint.sequence, 1);
  assert.equal(before.attempt.state, 'response_received');
});

test('future schema is rejected without writes or memory fallback', t => {
  const temporary = temporaryDatabase();
  t.after(() => temporary.cleanup());
  const futurePath = join(temporary.directory, 'future.sqlite');
  const future = new DatabaseSync(futurePath);
  future.exec("CREATE TABLE future_marker (value TEXT); INSERT INTO future_marker VALUES ('preserve-me'); PRAGMA user_version = 99;");
  future.close();
  const before = readFileSync(futurePath);
  assertRuntimeCode(() => openStore(futurePath, () => 1000), 'UNSUPPORTED_SCHEMA_VERSION');
  assert.deepEqual(readFileSync(futurePath), before);
  assertRuntimeCode(() => DurableWorkflowStore.open({
    databasePath: ':memory:',
    clock: () => 1000
  }), 'DURABILITY_REQUIRES_FILE');
});
