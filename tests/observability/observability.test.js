import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { test } from 'node:test';
import {
  DurableWorkflowStore,
  INTERNAL_SCHEMA_VERSION,
  RuntimeError,
  createHmacTokenizer,
  createTelemetryExporter,
  projectTelemetryEvent
} from '@jevelopers/agent-harness-runtime';
import { temporaryDatabase, waitForFiles } from '../runtime/helpers.js';

const TOKEN_KEY = Buffer.alloc(32, 91);
const FIXTURE_ID = 'internal-redaction-outage';
const CRASH_FIXTURE = join(new URL('./fixtures/exporter-crash.js', import.meta.url).pathname);
const CANARIES = Object.freeze([
  'CUSTOMER_NAME_CANARY_4d1',
  'SECRET_SUFFIX_CANARY_9q7',
  'PRIVATE_URL_CANARY_https://private.invalid/customer?token=secret',
  'PROMPT_BYTES_CANARY_2c8',
  'RESPONSE_BYTES_CANARY_1v5',
  'HOLD_REFERENCE_CANARY_6b2',
  'ERROR_STACK_CANARY_3e4'
]);

function openStore(databasePath, clock = () => 1000) {
  let nextId = 0;
  return DurableWorkflowStore.open({
    databasePath,
    clock,
    idFactory: () => `generated-${randomUUID()}-${++nextId}`
  });
}

function dataSnapshot(databasePath) {
  const db = new DatabaseSync(databasePath, { readOnly: true });
  try {
    const read = sql => db.prepare(sql).all().map(row => ({ ...row }));
    return {
      workflows: read(`SELECT workflow_id, state, revision, fence_token, lease_owner,
          lease_expires_ms, checkpoint_sequence, cancellation_requested, created_at_ms, updated_at_ms
        FROM workflows ORDER BY workflow_id`),
      checkpoints: read(`SELECT workflow_id, sequence, schema_version, payload_json, sha256,
          revision, created_at_ms FROM workflow_checkpoints ORDER BY workflow_id, sequence`),
      attempts: read(`SELECT attempt_id, workflow_id, logical_operation_ref, authority_hold_ref,
          state, created_at_ms, updated_at_ms FROM operation_attempts ORDER BY attempt_id`),
      transports: read(`SELECT transport_attempt_id, attempt_id, created_at_ms
        FROM transport_attempts ORDER BY transport_attempt_id`),
      observations: read(`SELECT attempt_id, source_observation_ref, observation_kind, fingerprint, observed_at_ms
        FROM attempt_observations ORDER BY attempt_id, source_observation_ref`),
      outbox: read(`SELECT event_id, event_sequence, delivered_at_ms, delivery_attempts
        FROM outbox ORDER BY event_sequence`),
      receipts: read('SELECT * FROM response_receipts ORDER BY receipt_id'),
      artifacts: read('SELECT * FROM immutable_artifacts ORDER BY workflow_id, artifact_id'),
      events: read(`SELECT sequence, event_id, workflow_id, revision, event_type, from_state,
          to_state, fence_token, occurred_at_ms FROM workflow_events ORDER BY sequence`)
    };
  } finally {
    db.close();
  }
}

function createExporter(store, sink, limits = {}) {
  return createTelemetryExporter({
    store,
    tokenizer: createHmacTokenizer(TOKEN_KEY),
    sink,
    limits
  });
}

function eventFixture(overrides = {}) {
  return {
    sequence: 1,
    eventId: 'event-fixture-id',
    workflowId: 'workflow-fixture-id',
    revision: 2,
    type: 'state_transition',
    fromState: 'running',
    toState: 'succeeded',
    fence: 4,
    occurredAtMs: 1234,
    ...overrides
  };
}

function assertTelemetryError(action, code, canary) {
  assert.throws(action, error => {
    assert.ok(error instanceof RuntimeError);
    assert.equal(error.code, code);
    assert.equal(error.message, error.safe_message);
    assert.equal(JSON.stringify(error.toJSON()).includes(canary), false);
    return true;
  });
}

async function killFixture(stage, databasePath, directory, t) {
  const readyPath = join(directory, `${stage}.ready`);
  const acceptedPath = join(directory, `${stage}.accepted`);
  const child = spawn(process.execPath, [CRASH_FIXTURE, databasePath, stage, readyPath, acceptedPath], {
    cwd: process.cwd(),
    env: {
      PATH: process.env.PATH ?? '',
      TMPDIR: process.env.TMPDIR ?? '/tmp',
      NODE_NO_WARNINGS: '1'
    },
    stdio: 'ignore'
  });
  t.after(() => {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
  });
  const closed = new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('close', (code, signal) => resolve({ code, signal }));
  });
  await waitForFiles([readyPath], 5000);
  const acknowledged = existsSync(acceptedPath) ? JSON.parse(readFileSync(acceptedPath, 'utf8')) : [];
  child.kill('SIGKILL');
  const result = await closed;
  assert.equal(result.signal, 'SIGKILL');
  return { readyPath, acceptedPath, acknowledged };
}

test('AH-10 bounded SQLite pages preserve strict sequence order and SQL record limits', t => {
  const temporary = temporaryDatabase('ah10-pages-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath);
  t.after(() => store.close());
  const total = 1007;
  for (let index = 0; index < total; index += 1) {
    store.createWorkflow({ workflowId: `page-workflow-${String(index).padStart(4, '0')}` });
  }

  const first = store.readEventPage({ limit: 500 });
  const second = store.readEventPage({ afterSequence: first.nextAfterSequence, limit: 500 });
  const third = store.readEventPage({ afterSequence: second.nextAfterSequence, limit: 500 });
  assert.deepEqual([first.events.length, second.events.length, third.events.length], [500, 500, 7]);
  assert.equal(first.hasMore, true);
  assert.equal(second.hasMore, true);
  assert.equal(third.hasMore, false);
  const sequences = [...first.events, ...second.events, ...third.events].map(event => event.sequence);
  assert.deepEqual(sequences, Array.from({ length: total }, (_, index) => index + 1));
  assert.equal(store.countEventRowsAfter(0, 100).count, 100);
  assert.equal(store.countEventRowsAfter(0, 100).capped, true);
  assert.throws(() => store.readEventPage({ limit: 501 }), error => error.code === 'INVALID_INPUT');
  assert.throws(() => store.advanceTelemetryCursor({ expectedSequence: 0, nextSequence: 2 }), error => {
    assert.ok(error instanceof RuntimeError);
    assert.equal(error.code, 'INVALID_INPUT');
    return true;
  });
  assert.equal(store.getTelemetryCursor(), 0);
  assert.equal(INTERNAL_SCHEMA_VERSION, 5);
});

test('allowlisted projection tokenizes opaque identities and rejects nested, accessor, proxy, cycle, and size inputs without executing them', () => {
  const tokenizer = createHmacTokenizer(TOKEN_KEY);
  const privateId = `${CANARIES[0]}:${CANARIES[1]}:${CANARIES[2]}`;
  const projected = projectTelemetryEvent(eventFixture({ eventId: privateId, workflowId: privateId }), tokenizer.tokenize.bind(tokenizer));
  const encoded = JSON.stringify(projected);
  assert.match(projected.eventId, /^tok1_[0-9a-f]{64}$/);
  assert.match(projected.workflowId, /^tok1_[0-9a-f]{64}$/);
  for (const canary of CANARIES) assert.equal(encoded.includes(canary), false);
  assert.equal(encoded.includes(privateId), false);

  let getterCalls = 0;
  const accessor = eventFixture();
  Object.defineProperty(accessor, 'eventId', {
    enumerable: true,
    get() {
      getterCalls += 1;
      throw new Error(CANARIES[6]);
    }
  });
  let proxyTrapCalls = 0;
  const proxy = new Proxy(eventFixture(), {
    ownKeys() {
      proxyTrapCalls += 1;
      throw new Error(CANARIES[6]);
    },
    get() {
      proxyTrapCalls += 1;
      throw new Error(CANARIES[6]);
    }
  });
  const cyclic = {};
  cyclic.self = cyclic;
  let deeplyNested = null;
  for (let index = 0; index < 10_000; index += 1) deeplyNested = { next: deeplyNested };
  assertTelemetryError(() => projectTelemetryEvent(accessor, tokenizer.tokenize.bind(tokenizer)), 'TELEMETRY_INVALID_INPUT', CANARIES[6]);
  assertTelemetryError(() => projectTelemetryEvent(proxy, tokenizer.tokenize.bind(tokenizer)), 'TELEMETRY_INVALID_INPUT', CANARIES[6]);
  assertTelemetryError(() => projectTelemetryEvent(eventFixture({ privateNested: cyclic }), tokenizer.tokenize.bind(tokenizer)), 'TELEMETRY_INVALID_INPUT', CANARIES[6]);
  assertTelemetryError(() => projectTelemetryEvent(eventFixture({ eventId: cyclic }), tokenizer.tokenize.bind(tokenizer)), 'TELEMETRY_INVALID_INPUT', CANARIES[6]);
  assertTelemetryError(() => projectTelemetryEvent(eventFixture({ eventId: deeplyNested }), tokenizer.tokenize.bind(tokenizer)), 'TELEMETRY_INVALID_INPUT', CANARIES[6]);
  assertTelemetryError(() => projectTelemetryEvent(eventFixture({ eventId: 'x'.repeat(2049) }), tokenizer.tokenize.bind(tokenizer)), 'TELEMETRY_INVALID_INPUT', CANARIES[6]);
  assert.equal(getterCalls, 0);
  assert.equal(proxyTrapCalls, 0);
  let keyProxyTrapCalls = 0;
  const keyProxy = new Proxy(new Uint8Array(32), {
    ownKeys() {
      keyProxyTrapCalls += 1;
      throw new Error(CANARIES[6]);
    },
    get() {
      keyProxyTrapCalls += 1;
      throw new Error(CANARIES[6]);
    }
  });
  assertTelemetryError(() => createHmacTokenizer(keyProxy), 'TELEMETRY_INVALID_INPUT', CANARIES[6]);
  assert.equal(keyProxyTrapCalls, 0);

  assertTelemetryError(
    () => projectTelemetryEvent(eventFixture(), () => { throw new Error(CANARIES[6]); }),
    'TELEMETRY_REDACTION_FAILED',
    CANARIES[6]
  );
  assertTelemetryError(
    () => projectTelemetryEvent(eventFixture(), () => 'raw-event-id'),
    'TELEMETRY_REDACTION_FAILED',
    CANARIES[6]
  );
  const tokenShapedRawId = `tok1_${'a'.repeat(64)}`;
  assertTelemetryError(
    () => projectTelemetryEvent(eventFixture({ eventId: tokenShapedRawId }), (_kind, value) => value),
    'TELEMETRY_REDACTION_FAILED',
    CANARIES[6]
  );
  assertTelemetryError(
    () => projectTelemetryEvent(eventFixture({ eventId: `prefix-${tokenShapedRawId}-suffix` }), () => tokenShapedRawId),
    'TELEMETRY_REDACTION_FAILED',
    CANARIES[6]
  );
  tokenizer.destroy();
  assertTelemetryError(
    () => projectTelemetryEvent(eventFixture(), tokenizer.tokenize.bind(tokenizer)),
    'TELEMETRY_REDACTION_FAILED',
    CANARIES[6]
  );
});

test('real intent, restart, cancellation, receipt reconciliation, and expired-lease events pass only redacted records', async t => {
  const temporary = temporaryDatabase('ah10-real-flows-');
  t.after(() => temporary.cleanup());
  let now = 1000;
  let store = openStore(temporary.databasePath, () => now);
  const rawWorkflowId = `${CANARIES[0]}:${CANARIES[1]}:${CANARIES[2]}`;
  const rawEventId = `${CANARIES[1]}:${CANARIES[2]}`;
  store.createWorkflow({
    workflowId: rawWorkflowId,
    eventId: rawEventId,
    checkpoint: { nested: { prompt: CANARIES[3], privateUrl: CANARIES[2] } }
  });
  const lease = store.acquireLease({ workflowId: rawWorkflowId, ownerId: 'worker-a', ttlMs: 5000 });
  const running = store.transitionWorkflow({ ...lease, expectedRevision: 0, toState: 'running' });
  const intent = store.recordDispatchIntent({
    ...lease,
    expectedRevision: running.revision,
    attemptId: CANARIES[0],
    logicalOperationRef: CANARIES[1],
    authorityHoldRef: CANARIES[5],
    transportAttemptId: CANARIES[2]
  });
  const cancellation = store.requestCancellation({ ...lease, expectedRevision: intent.revision });
  assert.equal(cancellation.state, 'reconciliation_pending');
  const receipt = store.recordResponseReceipt({
    ...lease,
    expectedRevision: cancellation.revision,
    attemptId: CANARIES[0],
    receiptId: CANARIES[1],
    outcome: 'completed',
    nextState: 'running',
    responseBytes: Buffer.from(CANARIES[4]),
    nextCheckpoint: { result: CANARIES[3] },
    artifacts: [{ artifactId: `private-artifact:${CANARIES[1]}`, bytes: Buffer.from(CANARIES[3]) }]
  });
  store.close();

  store = openStore(temporary.databasePath, () => now);
  const reconciled = store.reconcilePendingReceipts({
    ...lease,
    expectedRevision: receipt.revision,
    eventIdPrefix: 'redacted-reconcile'
  });
  assert.equal(reconciled.state, 'cancelled');
  assert.equal(store.getAttempt(CANARIES[0]).authorityHoldRef, CANARIES[5]);

  const expiringWorkflow = 'expiry-flow-canary-workflow';
  store.createWorkflow({ workflowId: expiringWorkflow });
  const expiringLease = store.acquireLease({ workflowId: expiringWorkflow, ownerId: 'worker-b', ttlMs: 10 });
  const expiringRunning = store.transitionWorkflow({
    ...expiringLease,
    expectedRevision: 0,
    toState: 'running'
  });
  store.recordDispatchIntent({
    ...expiringLease,
    expectedRevision: expiringRunning.revision,
    attemptId: 'expiry-attempt-canary',
    logicalOperationRef: 'expiry-operation-canary',
    authorityHoldRef: CANARIES[5],
    transportAttemptId: 'expiry-transport-canary'
  });
  store.close();
  now = 2000;
  store = openStore(temporary.databasePath, () => now);
  store.acquireLease({ workflowId: expiringWorkflow, ownerId: 'worker-c', ttlMs: 1000 });
  assert.equal(store.getWorkflow(expiringWorkflow).state, 'reconciliation_pending');
  assert.equal(store.getAttempt('expiry-attempt-canary').state, 'outcome_unknown');

  const delivered = [];
  const exporter = createExporter(store, async records => {
    delivered.push(...records);
    return { accepted: true, acceptedCount: records.length };
  }, { batchSize: 8, queueRecords: 8, maxBatchesPerFlush: 8 });
  const result = await exporter.flush();
  assert.equal(result.pendingRecords, 0);
  assert.ok(delivered.some(record => record.eventType === 'dispatch_intent_persisted'));
  assert.ok(delivered.some(record => record.eventType === 'response_receipt_persisted'));
  assert.ok(delivered.some(record => record.eventType === 'response_checkpoint_reconciled'));
  assert.ok(delivered.some(record => record.eventType === 'cancellation_requested_unresolved'));
  assert.ok(delivered.some(record => record.eventType === 'expired_lease_outcome_unknown'));
  const sinkText = JSON.stringify(delivered);
  const diagnosticsText = JSON.stringify(exporter.diagnostics());
  for (const canary of CANARIES) {
    assert.equal(sinkText.includes(canary), false);
    assert.equal(diagnosticsText.includes(canary), false);
  }
  assert.equal(sinkText.includes(rawWorkflowId), false);
  assert.equal(sinkText.includes(rawEventId), false);
  await exporter.close();
  store.close();
});

test('sink rejection, bounded retry, hung abort-ignoring sink, and close preserve outbox, attempts, and holds', async t => {
  const temporary = temporaryDatabase('ah10-outage-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath);
  t.after(() => store.close());
  store.createWorkflow({ workflowId: 'outage-workflow' });
  const lease = store.acquireLease({ workflowId: 'outage-workflow', ownerId: 'outage-worker', ttlMs: 5000 });
  const running = store.transitionWorkflow({ ...lease, expectedRevision: 0, toState: 'running' });
  store.recordDispatchIntent({
    ...lease,
    expectedRevision: running.revision,
    attemptId: 'outage-attempt',
    logicalOperationRef: 'outage-operation',
    authorityHoldRef: CANARIES[5],
    transportAttemptId: 'outage-transport'
  });
  store.recordAttemptObservation({
    attemptId: 'outage-attempt',
    sourceObservationRef: 'outage-observation',
    kind: 'unknown'
  });
  store.recordResponseReceipt({
    ...lease,
    expectedRevision: 2,
    attemptId: 'outage-attempt',
    receiptId: 'outage-receipt',
    outcome: 'completed',
    nextState: 'running',
    responseBytes: Buffer.from(CANARIES[4]),
    nextCheckpoint: { prompt: CANARIES[3] },
    artifacts: [{ artifactId: 'outage-artifact', bytes: Buffer.from(CANARIES[3]) }]
  });
  const before = dataSnapshot(temporary.databasePath);
  assert.equal(before.observations.length, 1);
  assert.equal(before.receipts.length, 1);
  assert.equal(before.artifacts.length, 1);
  let failingCalls = 0;
  const rejecting = createExporter(store, async () => {
    failingCalls += 1;
    throw new Error(CANARIES[6]);
  }, { maxAttempts: 2, retryDelayMs: 1, sinkTimeoutMs: 100, batchSize: 3, queueRecords: 3 });
  const rejected = await rejecting.flush();
  assert.equal(failingCalls, 2);
  assert.equal(rejected.cursor, 0);
  assert.ok(rejected.pendingRecords > 0);
  assert.equal(rejected.lastErrorCode, 'TELEMETRY_SINK_UNAVAILABLE');
  assert.equal(JSON.stringify(rejected).includes(CANARIES[6]), false);
  assert.deepEqual(dataSnapshot(temporary.databasePath), before);
  await rejecting.close();
  assert.deepEqual(dataSnapshot(temporary.databasePath), before);

  let hungCalls = 0;
  let receivedSignal;
  const hung = createExporter(store, (_records, { signal }) => {
    hungCalls += 1;
    receivedSignal = signal;
    return new Promise(() => {});
  }, { maxAttempts: 3, sinkTimeoutMs: 20, batchSize: 3, queueRecords: 3 });
  const startedAt = Date.now();
  const first = await hung.flush();
  assert.ok(Date.now() - startedAt < 500);
  assert.equal(hungCalls, 1);
  assert.equal(first.cursor, 0);
  assert.equal(first.lastErrorCode, 'TELEMETRY_SINK_TIMEOUT');
  assert.equal(receivedSignal.aborted, true);
  await hung.flush();
  assert.equal(hungCalls, 1);
  const closed = await hung.close();
  assert.equal(closed.state, 'closed');
  assert.equal(hungCalls, 1);
  assert.deepEqual(dataSnapshot(temporary.databasePath), before);

  let retryCalls = 0;
  const retrying = createExporter(store, records => {
    retryCalls += 1;
    if (retryCalls === 1) return Promise.reject(new Error(CANARIES[6]));
    return { accepted: true, acceptedCount: records.length };
  }, {
    maxAttempts: 2,
    retryDelayMs: 1,
    sinkTimeoutMs: 100,
    batchSize: 3,
    queueRecords: 3,
    maxBatchesPerFlush: 1
  });
  const accepted = await retrying.flush();
  assert.equal(retryCalls, 2);
  assert.equal(accepted.cursor, 3);
  assert.equal(accepted.pendingRecords, 2);
  assert.equal(accepted.deliveredRecords, 3);
  assert.deepEqual(dataSnapshot(temporary.databasePath).outbox, before.outbox);
  assert.deepEqual(dataSnapshot(temporary.databasePath).attempts, before.attempts);
  await retrying.close();
});

test('bounded queue and per-flush batch budget leave a counted durable backlog', async t => {
  const temporary = temporaryDatabase('ah10-bounds-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath);
  t.after(() => store.close());
  for (let index = 0; index < 20; index += 1) store.createWorkflow({ workflowId: `bounded-${index}` });
  const batches = [];
  const exporter = createExporter(store, records => {
    batches.push(records);
    assert.ok(records.length <= 8);
    assert.ok(Buffer.byteLength(JSON.stringify(records), 'utf8') <= 1026);
    return { accepted: true, acceptedCount: records.length };
  }, {
    queueRecords: 8,
    queueBytes: 1026,
    batchSize: 8,
    maxBatchesPerFlush: 1,
    maxAttempts: 1,
    sinkTimeoutMs: 100
  });
  const first = await exporter.flush();
  assert.equal(batches.length, 1);
  assert.ok(batches[0].length < 8);
  assert.equal(first.cursor, batches[0].length);
  assert.equal(first.pendingRecords, 20 - batches[0].length);
  const second = await exporter.flush();
  assert.equal(second.cursor, batches[0].length + batches[1].length);
  assert.equal(second.pendingRecords, 20 - second.cursor);
  await exporter.close();
});

test('missing sink, redaction failure, and closed-store outage fail closed without changing workflow evidence', async t => {
  const temporary = temporaryDatabase('ah10-redaction-outage-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath);
  t.after(() => store.close());
  store.createWorkflow({ workflowId: 'redaction-failure-workflow' });
  const before = dataSnapshot(temporary.databasePath);
  assert.throws(() => createTelemetryExporter({ store }), error => {
    assert.ok(error instanceof RuntimeError);
    assert.equal(error.code, 'TELEMETRY_INVALID_INPUT');
    return true;
  });

  let sinkCalls = 0;
  const redactionFailure = createTelemetryExporter({
    store,
    tokenize() {
      throw new Error(CANARIES[6]);
    },
    sink(records) {
      sinkCalls += 1;
      return { accepted: true, acceptedCount: records.length };
    }
  });
  const redaction = await redactionFailure.flush();
  assert.equal(redaction.cursor, 0);
  assert.equal(redaction.pendingRecords, 1);
  assert.equal(redaction.lastErrorCode, 'TELEMETRY_REDACTION_FAILED');
  assert.equal(sinkCalls, 0);
  assert.equal(JSON.stringify(redaction).includes(CANARIES[6]), false);
  assert.deepEqual(dataSnapshot(temporary.databasePath), before);

  store.close();
  const closedStoreExporter = createExporter(store, () => {
    sinkCalls += 1;
    return { accepted: true, acceptedCount: 1 };
  });
  const unavailable = await closedStoreExporter.flush();
  assert.equal(unavailable.lastErrorCode, 'TELEMETRY_STORE_UNAVAILABLE');
  assert.equal(sinkCalls, 0);
  assert.deepEqual(dataSnapshot(temporary.databasePath), before);
});

test('unknown stored event types are policy drops and do not collapse drop and delivery counters', async t => {
  const temporary = temporaryDatabase('ah10-policy-drop-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath);
  t.after(() => store.close());
  store.createWorkflow({ workflowId: 'policy-drop-one' });
  store.createWorkflow({ workflowId: 'policy-drop-two' });
  const tamper = new DatabaseSync(temporary.databasePath);
  tamper.exec('DROP TRIGGER workflow_events_no_update');
  tamper.prepare('UPDATE workflow_events SET event_type = ? WHERE sequence = 1').run('unrecognized-private-event');
  tamper.close();
  const delivered = [];
  const exporter = createExporter(store, records => {
    delivered.push(...records);
    return { accepted: true, acceptedCount: records.length };
  }, { batchSize: 2, queueRecords: 2, maxBatchesPerFlush: 4 });
  const result = await exporter.flush();
  assert.equal(result.policyDroppedRecords, 1);
  assert.equal(result.deliveredRecords, 1);
  assert.equal(result.pendingRecords, 0);
  assert.equal(delivered.length, 1);
  assert.equal(JSON.stringify(delivered).includes('unrecognized-private-event'), false);
  await exporter.close();
});

test('two exporter owners use cursor compare-and-swap and report an at-least-once duplicate safely', async t => {
  const temporary = temporaryDatabase('ah10-owner-race-');
  t.after(() => temporary.cleanup());
  const firstStore = openStore(temporary.databasePath);
  const secondStore = openStore(temporary.databasePath);
  t.after(() => firstStore.close());
  t.after(() => secondStore.close());
  firstStore.createWorkflow({ workflowId: 'owner-race-workflow' });
  let sinkCalls = 0;
  let releaseBoth;
  const bothEntered = new Promise(resolve => { releaseBoth = resolve; });
  const acceptedIds = [];
  const sink = async records => {
    sinkCalls += 1;
    acceptedIds.push(...records.map(record => record.eventId));
    if (sinkCalls === 2) releaseBoth();
    await bothEntered;
    return { accepted: true, acceptedCount: records.length };
  };
  const first = createExporter(firstStore, sink, { batchSize: 1, queueRecords: 1 });
  const second = createExporter(secondStore, sink, { batchSize: 1, queueRecords: 1 });
  const results = await Promise.all([first.flush(), second.flush()]);
  assert.equal(sinkCalls, 2);
  assert.deepEqual(acceptedIds, [acceptedIds[0], acceptedIds[0]]);
  assert.equal(firstStore.getTelemetryCursor(), 1);
  assert.equal(secondStore.getTelemetryCursor(), 1);
  assert.ok(results.some(result => result.lastErrorCode === 'TELEMETRY_CURSOR_CONFLICT'));
  assert.equal(firstStore.readOutbox().length, 1);
  await first.close();
  await second.close();
});

test('process restart replays stable identity after acceptance-before-cursor and stops after cursor commit', async t => {
  const beforeTemporary = temporaryDatabase('ah10-crash-before-');
  t.after(() => beforeTemporary.cleanup());
  let store = openStore(beforeTemporary.databasePath);
  store.createWorkflow({ workflowId: 'crash-before-workflow' });
  store.close();
  await killFixture('before-export', beforeTemporary.databasePath, beforeTemporary.directory, t);
  store = openStore(beforeTemporary.databasePath);
  assert.equal(store.getTelemetryCursor(), 0);
  const preExportRecords = [];
  const preExporter = createExporter(store, records => {
    preExportRecords.push(...records);
    return { accepted: true, acceptedCount: records.length };
  });
  assert.equal((await preExporter.flush()).cursor, 1);
  assert.equal(preExportRecords.length, 1);
  await preExporter.close();
  store.close();

  const acceptedTemporary = temporaryDatabase('ah10-crash-accepted-');
  t.after(() => acceptedTemporary.cleanup());
  store = openStore(acceptedTemporary.databasePath);
  store.createWorkflow({ workflowId: 'crash-accepted-workflow' });
  store.close();
  const acceptance = await killFixture(
    'accepted-before-cursor',
    acceptedTemporary.databasePath,
    acceptedTemporary.directory,
    t
  );
  assert.equal(acceptance.acknowledged.length, 1);
  store = openStore(acceptedTemporary.databasePath);
  assert.equal(store.getTelemetryCursor(), 0);
  const replayed = [];
  const replayExporter = createExporter(store, records => {
    replayed.push(...records.map(record => record.eventId));
    return { accepted: true, acceptedCount: records.length };
  });
  assert.equal((await replayExporter.flush()).cursor, 1);
  assert.deepEqual(replayed, acceptance.acknowledged);
  await replayExporter.close();
  store.close();

  const cursorTemporary = temporaryDatabase('ah10-crash-cursor-');
  t.after(() => cursorTemporary.cleanup());
  store = openStore(cursorTemporary.databasePath);
  store.createWorkflow({ workflowId: 'crash-after-cursor-workflow' });
  store.close();
  const committed = await killFixture('after-cursor', cursorTemporary.databasePath, cursorTemporary.directory, t);
  assert.equal(readFileSync(committed.readyPath, 'utf8'), '1');
  store = openStore(cursorTemporary.databasePath);
  assert.equal(store.getTelemetryCursor(), 1);
  let duplicateCalls = 0;
  const postCursorExporter = createExporter(store, () => {
    duplicateCalls += 1;
    return { accepted: true, acceptedCount: 1 };
  });
  const postCursor = await postCursorExporter.flush();
  assert.equal(postCursor.pendingRecords, 0);
  assert.equal(duplicateCalls, 0);
  await postCursorExporter.close();
  store.close();
});

test('schema v3 upgrade adds the v4 cursor and v5 JEV evidence tables; v2 migration remains atomic', t => {
  const temporary = temporaryDatabase('ah10-v3-migration-');
  t.after(() => temporary.cleanup());
  const store = openStore(temporary.databasePath);
  store.createWorkflow({ workflowId: 'schema-preservation-canary', checkpoint: { value: 'unchanged-bytes' } });
  const before = dataSnapshot(temporary.databasePath);
  store.close();
  const oldV3 = new DatabaseSync(temporary.databasePath);
  oldV3.exec(`
    DROP TABLE jev_local_observation_outbox;
    DROP TABLE jev_provider_observations;
    DROP TABLE jev_dispatch_markers;
    DROP TABLE telemetry_cursors;
    PRAGMA user_version = 3;
  `);
  oldV3.close();
  const migrated = openStore(temporary.databasePath);
  assert.equal(INTERNAL_SCHEMA_VERSION, 5);
  const raw = new DatabaseSync(temporary.databasePath, { readOnly: true });
  assert.equal(raw.prepare('PRAGMA user_version').get().user_version, 5);
  assert.equal(raw.prepare("SELECT COUNT(*) AS count FROM sqlite_master WHERE type='table' AND name='telemetry_cursors'").get().count, 1);
  assert.equal(raw.prepare("SELECT COUNT(*) AS count FROM sqlite_master WHERE type='table' AND name IN ('jev_dispatch_markers','jev_provider_observations','jev_local_observation_outbox')").get().count, 3);
  raw.close();
  assert.deepEqual(dataSnapshot(temporary.databasePath), before);
  assert.equal(migrated.getTelemetryCursor(), 0);
  migrated.close();

  const rollback = temporaryDatabase('ah10-v2-rollback-');
  t.after(() => rollback.cleanup());
  const legacy = new DatabaseSync(rollback.databasePath);
  legacy.exec(`
    CREATE TABLE response_receipts (receipt_id TEXT PRIMARY KEY, next_checkpoint_json TEXT NOT NULL);
    INSERT INTO response_receipts VALUES ('preserve-receipt', 'preserve-checkpoint-bytes');
    CREATE TABLE telemetry_cursors (
      consumer_id TEXT PRIMARY KEY, last_sequence INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL
    ) STRICT, WITHOUT ROWID;
    INSERT INTO telemetry_cursors VALUES ('preserve-cursor', 17, 88);
    PRAGMA user_version = 2;
  `);
  const beforeObjects = legacy.prepare('SELECT type, name, sql FROM sqlite_master ORDER BY type, name').all().map(row => ({ ...row }));
  legacy.close();
  assert.throws(() => openStore(rollback.databasePath), error => error instanceof RuntimeError);
  const after = new DatabaseSync(rollback.databasePath, { readOnly: true });
  assert.equal(after.prepare('PRAGMA user_version').get().user_version, 2);
  assert.deepEqual({ ...after.prepare('SELECT * FROM response_receipts').get() }, {
    receipt_id: 'preserve-receipt',
    next_checkpoint_json: 'preserve-checkpoint-bytes'
  });
  assert.deepEqual({ ...after.prepare('SELECT * FROM telemetry_cursors').get() }, {
    consumer_id: 'preserve-cursor',
    last_sequence: 17,
    updated_at_ms: 88
  });
  assert.equal(after.prepare("SELECT COUNT(*) AS count FROM pragma_table_info('response_receipts') WHERE name='next_checkpoint_sha256'").get().count, 0);
  assert.deepEqual(after.prepare('SELECT type, name, sql FROM sqlite_master ORDER BY type, name').all().map(row => ({ ...row })), beforeObjects);
  after.close();
});

test('AH-10 fixture constants identify this as internal mock evidence only', () => {
  assert.equal(FIXTURE_ID, 'internal-redaction-outage');
  assert.equal(CRASH_FIXTURE.endsWith('exporter-crash.js'), true);
  assert.ok(CANARIES.every(value => value.includes('CANARY')));
});
