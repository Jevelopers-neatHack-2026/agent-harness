import assert from 'node:assert/strict';
import { join } from 'node:path';
import { test } from 'node:test';
import {
  createProjectedStoreEvents,
  createTemporaryDirectory,
  readJson,
  spawnNodeFixture,
  SYNTHETIC_CANARIES,
  writeRecords
} from './helpers.js';

test('abort, timeout, exporter failure, replay generation, concurrent clients, and close stay bounded', async t => {
  const temporary = createTemporaryDirectory();
  t.after(() => temporary.cleanup());
  const { store, records } = createProjectedStoreEvents(join(temporary.directory, 'workflow.sqlite'));
  t.after(() => store.close());
  const recordsPath = writeRecords(temporary.directory, records);
  const child = spawnNodeFixture('lifecycle-controls-child.mjs', [recordsPath], { timeoutMs: 20_000 });
  const execution = await child.done;
  assert.equal(execution.code, 0, execution.stderr);
  for (const canary of SYNTHETIC_CANARIES) {
    assert.equal(execution.stdout.includes(canary), false);
    assert.equal(execution.stderr.includes(canary), false);
  }
  const result = readJsonFromOutput(execution.stdout);
  assert.equal(result.testMode, 'offline-synthetic-real-sdk');
  assert.deepEqual(result.beforeAbort, {
    status: 'failed',
    reasonCode: 'ABORTED_BEFORE_PREPARATION',
    sdkImported: false,
    clientCreated: false
  });
  assert.equal(result.delayedAbort.status, 'failed');
  assert.equal(result.delayedAbort.reasonCode, 'CAPTURE_ABORTED');
  assert.equal(result.delayedAbort.concurrentReason, 'CAPTURE_BUSY');
  assert.equal(result.delayedAbort.replayStatus, 'captured_local');
  assert.equal(result.delayedAbort.generation, 1);
  assert.equal(result.delayedAbort.replayGeneration, 2);
  assert.deepEqual(result.delayedAbort.replayTokens, records.map(record => record.eventId));
  assert.equal(result.delayedAbort.closeSamePromise, true);
  assert.equal(result.delayedAbort.closeResult, 'closed');
  assert.equal(result.delayedAbort.closeCompleted, true);
  assert.equal(result.delayedAbort.closeResultMatches, true);
  assert.equal(result.delayedAbort.postCloseReason, 'CAPTURE_CLOSED');

  assert.equal(result.timeout.status, 'failed');
  assert.equal(result.timeout.reasonCode, 'CAPTURE_TIMEOUT');
  assert.equal(result.timeout.generation, 1);
  assert.equal(result.timeout.busyReason, 'CAPTURE_BUSY');
  assert.equal(result.timeout.closeCompleted, true);
  assert.equal(result.exporterFailure.status, 'failed');
  assert.equal(result.exporterFailure.reasonCode, 'LOCAL_EXPORT_FAILED');
  assert.equal(result.exporterFailure.closeCompleted, true);

  assert.equal(result.concurrentClients.leftStatus, 'captured_local');
  assert.equal(result.concurrentClients.rightStatus, 'captured_local');
  assert.equal(result.concurrentClients.leftTokens.length, records.length);
  assert.equal(result.concurrentClients.rightTokens.length, records.length);
  assert.deepEqual(result.concurrentClients.leftTokens, records.map(record => record.eventId));
  assert.equal(result.concurrentClients.leftTokens.some(token => result.concurrentClients.rightTokens.includes(token)), false);
  assert.equal(result.concurrentClients.leftCloseCompleted, true);
  assert.equal(result.concurrentClients.rightCloseCompleted, true);

  assert.equal(result.lifecycle.listenersUnchanged, true);
  assert.equal(result.lifecycle.globalTraceProviderBefore, false);
  assert.equal(result.lifecycle.globalTraceProviderAfter, false);
  assert.equal(result.lifecycle.addedSymbolsKnown, true);
  assert.ok(result.lifecycle.globalSymbolsAfter.some(name => name === 'neatlogs.active_client_async_local_storage'));
  assert.ok(result.lifecycle.globalSymbolsAfter.some(name => name === 'neatlogs.private_provider_state'));
  assert.equal(store.getTelemetryCursor(), 0);
});

test('unsafe ambient SDK environment is refused by name before SDK import without exposing values', async t => {
  const temporary = createTemporaryDirectory();
  t.after(() => temporary.cleanup());
  const { store, records } = createProjectedStoreEvents(join(temporary.directory, 'workflow.sqlite'));
  t.after(() => store.close());
  const recordsPath = writeRecords(temporary.directory, records);
  const child = spawnNodeFixture('unsafe-environment-child.mjs', [recordsPath], { timeoutMs: 5_000 });
  const execution = await child.done;
  assert.equal(execution.code, 0, execution.stderr);
  for (const canary of SYNTHETIC_CANARIES) {
    assert.equal(execution.stdout.includes(canary), false);
    assert.equal(execution.stderr.includes(canary), false);
  }
  const result = readJsonFromOutput(execution.stdout);
  assert.deepEqual(result, {
    status: 'failed',
    reasonCode: 'UNSAFE_SDK_ENVIRONMENT',
    sdkImported: false,
    clientCreated: false
  });
  assert.equal(store.getTelemetryCursor(), 0);
});

function readJsonFromOutput(output) {
  return JSON.parse(output.trim());
}
