import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { test } from 'node:test';
import { DurableWorkflowStore, RuntimeError } from '@jevelopers/agent-harness-runtime';
import { LOCAL_CONTROLLER_STORAGE_CAPABILITY } from '../../src/internal/private-local-store-capability.js';

const NOW = 1_800_000_000_000;

function temporaryDatabase() {
  const directory = mkdtempSync(join(tmpdir(), 'ops-consistent-read-'));
  return { directory, databasePath: join(directory, 'workflow.sqlite') };
}

function durableWorkflowRows(databasePath, workflowId) {
  const database = new DatabaseSync(databasePath, { readOnly: true });
  try {
    const rows = {
      workflow: database.prepare('SELECT * FROM workflows WHERE workflow_id = ?').get(workflowId),
      checkpoints: database.prepare(`
        SELECT * FROM workflow_checkpoints WHERE workflow_id = ? ORDER BY sequence
      `).all(workflowId),
      attempts: database.prepare(`
        SELECT * FROM operation_attempts WHERE workflow_id = ? ORDER BY attempt_id
      `).all(workflowId),
      transports: database.prepare(`
        SELECT t.* FROM transport_attempts t
        JOIN operation_attempts a ON a.attempt_id = t.attempt_id
        WHERE a.workflow_id = ? ORDER BY t.transport_attempt_id
      `).all(workflowId),
      events: database.prepare(`
        SELECT * FROM workflow_events WHERE workflow_id = ? ORDER BY sequence
      `).all(workflowId),
      outbox: database.prepare(`
        SELECT o.* FROM outbox o JOIN workflow_events e ON e.sequence = o.event_sequence
        WHERE e.workflow_id = ? ORDER BY o.event_sequence
      `).all(workflowId),
      operationsCounters: database.prepare(`
        SELECT pending_records, pending_bytes, in_flight, reserved_event_slots, reserved_disk_bytes
        FROM operations_meta WHERE singleton = 1
      `).get()
    };
    return JSON.parse(JSON.stringify(rows));
  } finally {
    database.close();
  }
}

test('controller snapshot reads the committed WAL view without reserving the writer', t => {
  const temporary = temporaryDatabase();
  let store;
  let writer;
  t.after(() => {
    try { if (writer?.isTransaction) writer.exec('ROLLBACK'); } catch {}
    try { if (writer?.isOpen) writer.close(); } catch {}
    try { store?.close(); } catch {}
    rmSync(temporary.directory, { recursive: true, force: true });
  });

  store = DurableWorkflowStore.open({
    databasePath: temporary.databasePath,
    clock: () => NOW,
    busyTimeoutMs: 20
  });
  const workflowId = 'read-snapshot-held-writer';
  store.createWorkflow({
    workflowId,
    checkpoint: { source: 'committed-checkpoint' },
    eventId: 'read-snapshot:create'
  });
  const lease = store.acquireLease({
    workflowId,
    ownerId: 'read-snapshot-owner',
    ttlMs: 30_000,
    eventId: 'read-snapshot:lease'
  });
  const running = store.transitionWorkflow({
    ...lease,
    expectedRevision: 0,
    toState: 'running',
    eventId: 'read-snapshot:running'
  });
  const intent = store.recordDispatchIntent({
    ...lease,
    expectedRevision: running.revision,
    attemptId: 'read-snapshot-attempt',
    logicalOperationRef: 'read-snapshot-logical-operation',
    authorityHoldRef: 'read-snapshot-financial-hold',
    transportAttemptId: 'read-snapshot-transport',
    eventId: 'read-snapshot:intent'
  });

  const before = store._localControllerSnapshot(LOCAL_CONTROLLER_STORAGE_CAPABILITY, workflowId);
  assert.equal(before.workflow.state, 'awaiting_response');
  assert.equal(before.checkpoint.sequence, 1);
  assert.deepEqual(before.checkpoint.data, { source: 'committed-checkpoint' });
  assert.deepEqual(before.externalAttemptStates, ['intent']);
  assert.equal(before.externalAttemptsTruncated, false);
  assert.equal(before.hasUnresolvedExternalAttempt, true);
  assert.throws(
    () => store._localControllerSnapshot(Object.freeze({}), workflowId),
    error => error instanceof RuntimeError && error.code === 'INVALID_INPUT'
  );
  assert.throws(
    () => store._localControllerSnapshot(LOCAL_CONTROLLER_STORAGE_CAPABILITY, 'missing-read-snapshot-workflow'),
    error => error instanceof RuntimeError && error.code === 'WORKFLOW_NOT_FOUND'
  );
  const committedRows = durableWorkflowRows(temporary.databasePath, workflowId);

  writer = new DatabaseSync(temporary.databasePath, { timeout: 20 });
  writer.exec('BEGIN IMMEDIATE');
  const originalWorkflow = writer.prepare(
    'SELECT revision, checkpoint_sequence FROM workflows WHERE workflow_id = ?'
  ).get(workflowId);
  const originalCheckpoint = writer.prepare(`
    SELECT schema_version FROM workflow_checkpoints WHERE workflow_id = ? AND sequence = ?
  `).get(workflowId, Number(originalWorkflow.checkpoint_sequence));
  const nextRevision = Number(originalWorkflow.revision) + 1;
  const nextSequence = Number(originalWorkflow.checkpoint_sequence) + 1;
  const uncommittedPayload = JSON.stringify({
    schemaVersion: Number(originalCheckpoint.schema_version),
    data: { source: 'uncommitted-checkpoint' }
  });
  const payloadHash = createHash('sha256').update(uncommittedPayload, 'utf8').digest('hex');
  writer.prepare(`
    UPDATE workflows
    SET state = 'reconciliation_pending', revision = ?, checkpoint_sequence = ?, updated_at_ms = ?
    WHERE workflow_id = ?
  `).run(nextRevision, nextSequence, NOW + 1, workflowId);
  writer.prepare(`
    INSERT INTO workflow_checkpoints (
      workflow_id, sequence, schema_version, payload_json, sha256, revision, created_at_ms
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(workflowId, nextSequence, Number(originalCheckpoint.schema_version), uncommittedPayload,
    payloadHash, nextRevision, NOW + 1);
  writer.prepare(`
    UPDATE operation_attempts SET state = 'outcome_unknown', updated_at_ms = ? WHERE attempt_id = ?
  `).run(NOW + 1, 'read-snapshot-attempt');
  assert.equal(writer.prepare('SELECT state FROM workflows WHERE workflow_id = ?').get(workflowId).state,
    'reconciliation_pending');
  assert.equal(writer.prepare('SELECT state FROM operation_attempts WHERE attempt_id = ?')
    .get('read-snapshot-attempt').state, 'outcome_unknown');

  const readStartedAt = performance.now();
  const concurrentSnapshot = store._localControllerSnapshot(LOCAL_CONTROLLER_STORAGE_CAPABILITY, workflowId);
  const concurrentReadElapsedMs = performance.now() - readStartedAt;
  t.diagnostic(`held-writer committed-view read: ${concurrentReadElapsedMs.toFixed(2)} ms (busyTimeoutMs=20)`);
  assert.ok(concurrentReadElapsedMs < 250,
    `a committed-view read should finish within the bounded observation window; measured ${concurrentReadElapsedMs.toFixed(2)} ms`);
  assert.deepEqual(
    concurrentSnapshot,
    before,
    'all three snapshot groups must come from the previously committed WAL view'
  );

  assert.throws(() => store.requestCancellation({
    ...lease,
    expectedRevision: intent.revision,
    eventId: 'read-snapshot:busy-cancel'
  }), error => error instanceof RuntimeError
    && error.code === 'STORE_BUSY'
    && error.retryable === true
    && typeof error.safe_message === 'string'
    && error.safe_message.length <= 256);
  assert.deepEqual(durableWorkflowRows(temporary.databasePath, workflowId), committedRows,
    'the held-writer mutation failure must leave all committed workflow data unchanged');

  writer.exec('ROLLBACK');
  assert.deepEqual(durableWorkflowRows(temporary.databasePath, workflowId), committedRows);
  writer.exec('BEGIN IMMEDIATE');
  writer.prepare(`
    UPDATE workflows
    SET state = 'reconciliation_pending', revision = ?, checkpoint_sequence = ?, updated_at_ms = ?
    WHERE workflow_id = ?
  `).run(nextRevision, nextSequence, NOW + 1, workflowId);
  writer.prepare(`
    INSERT INTO workflow_checkpoints (
      workflow_id, sequence, schema_version, payload_json, sha256, revision, created_at_ms
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(workflowId, nextSequence, Number(originalCheckpoint.schema_version), uncommittedPayload,
    payloadHash, nextRevision, NOW + 1);
  writer.prepare(`
    UPDATE operation_attempts SET state = 'outcome_unknown', updated_at_ms = ? WHERE attempt_id = ?
  `).run(NOW + 1, 'read-snapshot-attempt');
  writer.exec('COMMIT');
  const newlyCommitted = store._localControllerSnapshot(LOCAL_CONTROLLER_STORAGE_CAPABILITY, workflowId);
  assert.equal(newlyCommitted.workflow.revision, nextRevision);
  assert.equal(newlyCommitted.workflow.state, 'reconciliation_pending');
  assert.equal(newlyCommitted.checkpoint.sequence, nextSequence);
  assert.deepEqual(newlyCommitted.checkpoint.data, { source: 'uncommitted-checkpoint' });
  assert.deepEqual(newlyCommitted.externalAttemptStates, ['outcome_unknown']);
  assert.equal(newlyCommitted.hasUnresolvedExternalAttempt, true);
  const committedTupleRows = durableWorkflowRows(temporary.databasePath, workflowId);
  assert.equal(committedTupleRows.workflow.revision, nextRevision);
  assert.equal(committedTupleRows.workflow.state, 'reconciliation_pending');
  assert.equal(committedTupleRows.checkpoints.length, 2);
  assert.equal(committedTupleRows.attempts[0].state, 'outcome_unknown');
  const cancelled = store.requestCancellation({
    ...lease,
    expectedRevision: nextRevision,
    eventId: 'read-snapshot:cancel-after-rollback'
  });
  assert.equal(cancelled.accepted, true);
  assert.equal(cancelled.state, 'reconciliation_pending');
  const afterMutation = store._localControllerSnapshot(LOCAL_CONTROLLER_STORAGE_CAPABILITY, workflowId);
  assert.equal(afterMutation.workflow.cancellationRequested, true);
  assert.deepEqual(afterMutation.externalAttemptStates, ['outcome_unknown']);
  assert.equal(afterMutation.hasUnresolvedExternalAttempt, true);
  assert.deepEqual(durableWorkflowRows(temporary.databasePath, workflowId).operationsCounters,
    committedTupleRows.operationsCounters,
    'ordinary cancellation should not change operations admission/reservation counters');
  assert.equal(durableWorkflowRows(temporary.databasePath, workflowId).outbox.length,
    committedTupleRows.outbox.length + 1,
    'the successful post-rollback mutation emits exactly one durable outbox event');
});

test('controller snapshot bounds unresolved-attempt inspection at 32 rows', t => {
  const temporary = temporaryDatabase();
  let store;
  let database;
  t.after(() => {
    try { if (database?.isOpen) database.close(); } catch {}
    try { store?.close(); } catch {}
    rmSync(temporary.directory, { recursive: true, force: true });
  });

  store = DurableWorkflowStore.open({ databasePath: temporary.databasePath, clock: () => NOW });
  const workflowId = 'read-snapshot-attempt-bound';
  store.createWorkflow({ workflowId, checkpoint: { source: 'bounded-attempt-fixture' }, eventId: 'read-bound:create' });
  database = new DatabaseSync(temporary.databasePath);
  database.exec('BEGIN IMMEDIATE');
  const insert = database.prepare(`
    INSERT INTO operation_attempts (
      attempt_id, workflow_id, logical_operation_ref, authority_hold_ref, state, created_at_ms, updated_at_ms
    ) VALUES (?, ?, ?, ?, 'intent', ?, ?)
  `);
  for (let index = 0; index < 33; index += 1) {
    const suffix = String(index).padStart(2, '0');
    insert.run(`read-bound-attempt-${suffix}`, workflowId, `read-bound-logical-${suffix}`,
      `read-bound-hold-${suffix}`, NOW, NOW);
  }
  database.exec('COMMIT');

  const snapshot = store._localControllerSnapshot(LOCAL_CONTROLLER_STORAGE_CAPABILITY, workflowId);
  assert.equal(snapshot.externalAttemptStates.length, 32);
  assert.equal(snapshot.externalAttemptsTruncated, true);
  assert.equal(snapshot.hasUnresolvedExternalAttempt, true);
  assert.deepEqual(snapshot.externalAttemptStates, Array(32).fill('intent'));
});
