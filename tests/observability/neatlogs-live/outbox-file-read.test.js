import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import {
  createNeatlogsOutboxSink, iterateNeatlogsOutboxSnapshot, listNeatlogsOutbox
} from '../../../src/observability/private/neatlogs-outbox.js';

const MAX_BYTES = 2 * 1024 * 1024;
const moduleUrl = new URL('../../../src/observability/private/neatlogs-outbox.js', import.meta.url).href;

function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'neatlogs-bounded-read-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const sink = createNeatlogsOutboxSink({ directory });
  sink([{ schema: 'agent-harness.telemetry.v1', sequence: 1,
    eventId: `tok1_${'a'.repeat(64)}`, workflowId: `tok1_${'b'.repeat(64)}`,
    revision: 0, eventType: 'workflow_created', fromState: null, toState: null,
    fence: 0, occurredAtMs: 1_760_000_000_000 }]);
  return { directory, path: listNeatlogsOutbox(directory)[0] };
}

test('batch snapshot refuses symlinks without reading or changing the target', t => {
  const { directory, path } = fixture(t);
  const original = readFileSync(path);
  const target = join(directory, 'private-target');
  writeFileSync(target, original);
  rmSync(path);
  symlinkSync(target, path);
  assert.throws(() => [...iterateNeatlogsOutboxSnapshot(directory)], { code: 'BATCH_INVALID' });
  assert.deepEqual(readFileSync(target), original);
});

test('batch read remains capped when the file grows after the size sample', t => {
  const { directory, path } = fixture(t);
  // Simulate growth after fstat deterministically: actual regular bytes exceed
  // the cap, while the intercepted size sample reports the earlier length.
  const original = readFileSync(path);
  writeFileSync(path, Buffer.concat([original, Buffer.alloc(MAX_BYTES + 100, 0x20)]));
  const child = spawnSync(process.execPath, ['--input-type=module', '--eval', `
    import fs from 'node:fs';
    import { syncBuiltinESMExports } from 'node:module';
    const [url, directory] = process.argv.slice(1);
    const { iterateNeatlogsOutboxSnapshot } = await import(url);
    const fstat = fs.fstatSync;
    const read = fs.readSync;
    let requested = 0;
    let consumed = 0;
    fs.fstatSync = (...args) => { const stat = fstat(...args); stat.size = 100; return stat; };
    fs.readSync = (fd, buffer, offset, length, position) => {
      requested += length;
      const count = read(fd, buffer, offset, length, position);
      consumed += count;
      return count;
    };
    syncBuiltinESMExports();
    let code = null;
    try { [...iterateNeatlogsOutboxSnapshot(directory)]; } catch (error) { code = error.code; }
    console.log(JSON.stringify({ code, requested, consumed }));
  `, moduleUrl, directory], { encoding: 'utf8', timeout: 10_000, maxBuffer: 4096 });
  assert.equal(child.error, undefined);
  assert.equal(child.status, 0, child.stderr);
  const observed = JSON.parse(child.stdout);
  assert.equal(observed.code, 'BATCH_INVALID');
  assert.equal(observed.consumed, MAX_BYTES + 1);
  assert.equal(observed.requested, MAX_BYTES + 1);
});

test('ordinary batch snapshot preserves records and original bytes', t => {
  const { directory, path } = fixture(t);
  const original = readFileSync(path);
  const [snapshot] = [...iterateNeatlogsOutboxSnapshot(directory)];
  assert.equal(snapshot.state, 'pending_send');
  assert.equal(snapshot.batch.records[0].sequence, 1);
  assert.deepEqual(readFileSync(path), original);
});
