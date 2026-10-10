import assert from 'node:assert/strict';
import { join } from 'node:path';
import { test } from 'node:test';
import {
  createProjectedStoreEvents,
  createTemporaryDirectory,
  spawnNodeFixture,
  SYNTHETIC_CANARIES,
  writeRecords
} from './helpers.js';

test('real SDK keeps 31 and 32 source records inside one owned export generation', async t => {
  const temporary = createTemporaryDirectory();
  t.after(() => temporary.cleanup());
  const { store, records, rawEventCount } = createProjectedStoreEvents(
    join(temporary.directory, 'max-batch.sqlite'), { workflowCount: 32 }
  );
  t.after(() => store.close());
  assert.equal(records.length, 32);
  assert.equal(rawEventCount, 32);
  const recordsPath = writeRecords(temporary.directory, records);
  const child = spawnNodeFixture('max-batch-controls-child.mjs', [recordsPath], { timeoutMs: 25_000 });
  const execution = await child.done;
  assert.equal(execution.code, 0, execution.stderr);
  assert.equal(store.getTelemetryCursor(), 0);
  assert.equal(store.readEventPage({ afterSequence: 0, limit: 32 }).events.length, 32);
  assert.equal(store.readOutbox().length, 32);
  for (const canary of SYNTHETIC_CANARIES) {
    assert.equal(execution.stdout.includes(canary), false);
    assert.equal(execution.stderr.includes(canary), false);
  }
  const result = JSON.parse(execution.stdout.trim());
  assert.equal(result.testMode, 'offline-synthetic-real-sdk');
  assert.equal(result.matrix.length, 8);
  for (const row of result.matrix) {
    assert.equal(row.atResult.active, false, `${row.count}/${row.delay}/${row.mode}`);
    assert.equal(row.atResult.exporter.pendingCallbacks, 0, `${row.count}/${row.delay}/${row.mode}`);
    assert.deepEqual(row.afterSettle, row.atResult, `${row.count}/${row.delay}/${row.mode}`);
    assert.deepEqual(row.afterCloseSettle, row.afterClose, `${row.count}/${row.delay}/${row.mode}`);
    assert.equal(row.closed.sdkShutdownCompleted, true, `${row.count}/${row.delay}/${row.mode}`);
    if (row.mode === 'success') {
      assert.equal(row.result.status, 'captured_local');
      assert.equal(row.result.sourceRecordCount, row.count);
      assert.equal(row.result.roots, row.count);
      assert.equal(row.result.markers, row.count);
      assert.equal(row.result.spans, row.count * 2);
      assert.equal(row.result.counters.localExporterFailures, 0);
    } else {
      assert.equal(row.result.status, 'failed');
      assert.equal(row.result.reasonCode, 'LOCAL_EXPORT_FAILED');
      assert.equal(row.result.sourceRecordCount, undefined);
      assert.equal(row.atResult.exporter.failures, 1);
      assert.equal(row.atResult.exporter.pendingCallbacks, 0);
    }
  }

  const { abort, timeout, close } = result.barriers;
  assert.equal(abort.pending.active, true);
  assert.equal(abort.pending.exporter.pendingCallbacks, 1);
  assert.equal(abort.busyReason, 'CAPTURE_BUSY');
  assert.equal(abort.result.reasonCode, 'CAPTURE_ABORTED');
  assert.equal(abort.result.generation, 1);
  assert.equal(abort.drained.active, false);
  assert.equal(abort.drained.exporter.pendingCallbacks, 0);
  assert.equal(abort.retry.status, 'captured_local');
  assert.equal(abort.retry.generation, 2);
  assert.equal(abort.retry.sourceRecordCount, 32);
  assert.equal(abort.retry.roots, 32);
  assert.equal(abort.retry.markers, 32);
  assert.equal(abort.retryDiagnostics.exporter.pendingCallbacks, 0);
  assert.deepEqual(abort.retryAfterSettle, abort.retryDiagnostics);
  assert.equal(abort.closedDiagnostics.active, false);

  assert.equal(timeout.busyReason, 'CAPTURE_BUSY');
  assert.equal(timeout.result.reasonCode, 'CAPTURE_TIMEOUT');
  assert.equal(timeout.drained.active, false);
  assert.equal(timeout.drained.exporter.pendingCallbacks, 0);
  assert.equal(timeout.retry.status, 'failed');
  assert.equal(timeout.retry.reasonCode, 'CAPTURE_TIMEOUT');
  assert.equal(timeout.retry.generation, 2);
  assert.equal(timeout.retryDiagnostics.exporter.pendingCallbacks, 0);
  assert.deepEqual(timeout.retryAfterSettle, timeout.retryDiagnostics);
  assert.equal(timeout.close.sdkShutdownCompleted, true);
  assert.deepEqual(timeout.afterCloseSettle, timeout.closedDiagnostics);

  assert.equal(close.pending.active, true);
  assert.equal(close.pending.exporter.pendingCallbacks, 1);
  assert.equal(close.samePromise, true);
  assert.equal(close.result.reasonCode, 'CAPTURE_CLOSED');
  assert.equal(close.close.sdkShutdownCompleted, true);
  assert.equal(close.diagnostics.active, false);
  assert.equal(close.diagnostics.exporter.pendingCallbacks, 0);
  assert.deepEqual(close.afterSettle, close.diagnostics);
});
