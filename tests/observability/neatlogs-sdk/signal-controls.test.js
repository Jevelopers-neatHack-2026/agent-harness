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

test('native AbortSignals use intrinsic state and listener operations with failure-safe cleanup', async t => {
  const temporary = createTemporaryDirectory();
  t.after(() => temporary.cleanup());
  const { store, records } = createProjectedStoreEvents(join(temporary.directory, 'signals.sqlite'));
  t.after(() => store.close());
  const recordsPath = writeRecords(temporary.directory, records);
  const child = spawnNodeFixture('signal-controls-child.mjs', [recordsPath], { timeoutMs: 15_000 });
  const execution = await child.done;
  assert.equal(execution.code, 0, execution.stderr);
  for (const canary of SYNTHETIC_CANARIES) {
    assert.equal(execution.stdout.includes(canary), false);
    assert.equal(execution.stderr.includes(canary), false);
  }
  const result = JSON.parse(execution.stdout.trim());
  assert.deepEqual(result.proxy, { reasonCode: 'INVALID_CAPTURE_SIGNAL', hooks: 0 });
  assert.deepEqual(result.forged, { reasonCode: 'INVALID_CAPTURE_SIGNAL', hooks: 0 });
  assert.deepEqual(result.preAborted, {
    reasonCode: 'ABORTED_BEFORE_PREPARATION',
    sdkImported: false
  });
  assert.deepEqual(result.nativeOverrides, {
    hooks: 0,
    abortReason: 'CAPTURE_ABORTED',
    retryStatus: 'captured_local',
    retryGeneration: 2,
    activeBeforeClose: false,
    pendingBeforeClose: 0
  });
  assert.equal(result.closeSettled, true);
  assert.equal(result.closeResult.sdkShutdownCompleted, true);
  assert.equal(result.closedDiagnostics.active, false);
  assert.equal(result.closedDiagnostics.exporter.pendingCallbacks, 0);
});
