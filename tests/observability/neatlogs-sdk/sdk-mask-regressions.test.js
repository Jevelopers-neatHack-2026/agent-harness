import assert from 'node:assert/strict';
import { join } from 'node:path';
import { test } from 'node:test';
import {
  spawnNodeFixture,
  SYNTHETIC_CANARIES,
} from './helpers.js';

test('pinned SDK null, undefined, invalid, throwing, rejecting, and timeout masks are never treated as source acceptance', async t => {
  const child = spawnNodeFixture('sdk-mask-regressions-child.mjs', [], { timeoutMs: 20_000 });
  const execution = await child.done;
  assert.equal(execution.code, 0, execution.stderr);
  for (const canary of SYNTHETIC_CANARIES) {
    assert.equal(execution.stdout.includes(canary), false);
    assert.equal(execution.stderr.includes(canary), false);
  }
  const result = JSON.parse(execution.stdout.trim());
  assert.equal(result.testMode, 'offline-synthetic-real-sdk');
  assert.deepEqual(result.results.map(item => item.mode), [
    'null', 'undefined', 'invalid', 'throw', 'reject', 'timeout'
  ]);
  const byMode = new Map(result.results.map(item => [item.mode, item]));
  assert.equal(byMode.get('null').flushResult, true);
  assert.ok(byMode.get('null').maskedSpanDrops > 0);
  assert.equal(byMode.get('null').exportedSpanCount, 0);
  assert.equal(byMode.get('undefined').flushResult, true);
  assert.equal(byMode.get('undefined').rawCanaryObservedByLocalExporter, true);
  assert.ok(byMode.get('undefined').exportedSpanCount > 0);
  assert.equal(byMode.get('invalid').rawCanaryObservedByLocalExporter, false);
  for (const mode of ['throw', 'reject', 'timeout']) {
    assert.equal(byMode.get(mode).rawCanaryObservedByLocalExporter, false);
    assert.ok(byMode.get(mode).maskedSpanDrops > 0);
  }
  for (const item of result.results) assert.equal(item.closeResult, true);
});
