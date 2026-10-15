import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { test } from 'node:test';
import { DurableWorkflowStore } from '../../src/internal/durable-store.js';
import { temporaryDatabase } from '../runtime/helpers.js';
import { pauseAtWriterBoundary } from './pause-writer-boundary.js';

function fixture(t) {
  const temporary = temporaryDatabase('ah75-pause-boundary-');
  const store = DurableWorkflowStore.open({ databasePath: temporary.databasePath, busyTimeoutMs: 20 });
  const writer = new DatabaseSync(temporary.databasePath, { timeout: 20 });
  t.after(() => { writer.close(); store.close(); temporary.cleanup(); });
  return { ...temporary, store, writer };
}

const busy = error => error.code === 'ERR_SQLITE_ERROR' && (error.errcode & 0xff) === 5;

test('a competing writer prevents pause before any signal or confirmation callback', t => {
  const { databasePath, store, writer } = fixture(t);
  writer.exec('BEGIN IMMEDIATE');
  let calls = 0;
  try {
    assert.throws(() => store.createWorkflow({ workflowId: 'blocked-setup' }), error => error.code === 'STORE_BUSY');
    assert.throws(() => pauseAtWriterBoundary(databasePath,
      () => { calls += 1; return true; }, () => { calls += 1; return true; }), busy);
    assert.equal(calls, 0);
  } finally { writer.exec('ROLLBACK'); }
  store.createWorkflow({ workflowId: 'blocked-setup' });
  assert.equal(store.getWorkflow('blocked-setup').state, 'queued');
});

test('writer fence remains held through pause confirmation and releases before subsequent setup', t => {
  const { databasePath, store, writer } = fixture(t);
  const order = [];
  const proveFenceHeld = () => assert.throws(() => writer.exec('BEGIN IMMEDIATE'), busy);
  pauseAtWriterBoundary(databasePath, () => {
    proveFenceHeld(); order.push('pause'); return true;
  }, () => {
    proveFenceHeld(); order.push('confirmed'); return true;
  });
  assert.deepEqual(order, ['pause', 'confirmed']);
  store.createWorkflow({ workflowId: 'after-pause' });
  assert.equal(store.getWorkflow('after-pause').state, 'queued');
});

test('unconfirmed pause and thrown callback release the fence and preserve failure identity', t => {
  const { databasePath, writer } = fixture(t);
  assert.throws(() => pauseAtWriterBoundary(databasePath, () => true, () => false));
  writer.exec('BEGIN IMMEDIATE'); writer.exec('ROLLBACK');
  const failure = new Error('controlled confirmation failure');
  assert.throws(() => pauseAtWriterBoundary(databasePath, () => true, () => { throw failure; }), error => error === failure);
  writer.exec('BEGIN IMMEDIATE'); writer.exec('ROLLBACK');
});
