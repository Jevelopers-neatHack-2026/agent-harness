import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { test } from 'node:test';
import { DurableWorkflowStore } from '@jevelopers/agent-harness-runtime';
import { temporaryDatabase, waitForFiles } from '../runtime/helpers.js';

const fixtureUrl = new URL('./fixtures/exporter-crash.js', import.meta.url).href;

for (const stage of ['before-export', 'accepted-before-cursor', 'after-cursor']) {
  test(`crash barrier is invisible during its content write at ${stage}`, async t => {
    const temporary = temporaryDatabase('ah10-ready-publication-');
    const store = DurableWorkflowStore.open({ databasePath: temporary.databasePath, clock: () => 1000 });
    store.createWorkflow({ workflowId: 'ready-workflow' });
    store.close();
    const readyPath = join(temporary.directory, 'barrier.ready');
    const acceptedPath = join(temporary.directory, 'accepted.json');
    const observationPath = join(temporary.directory, 'write-observation.json');
    const child = spawn(process.execPath, ['--input-type=module', '--eval', `
      import fs from 'node:fs';
      import { syncBuiltinESMExports } from 'node:module';
      const [fixture, database, stage, ready, accepted, observation] = process.argv.slice(1);
      const write = fs.writeFileSync;
      fs.writeFileSync = (path, value, options) => {
        if (typeof path !== 'string' || (path !== ready && !path.startsWith(ready + '.'))) {
          return write(path, value, options);
        }
        // Observe the real create-before-write boundary deterministically.
        const fd = fs.openSync(path, options?.flag ?? 'w', options?.mode ?? 0o600);
        try {
          write(observation, JSON.stringify({ readyVisibleBeforeContent: fs.existsSync(ready) }));
          write(fd, value);
        } finally { fs.closeSync(fd); }
      };
      syncBuiltinESMExports();
      process.argv = [process.execPath, fixture, database, stage, ready, accepted];
      await import(fixture);
    `, fixtureUrl, temporary.databasePath, stage, readyPath, acceptedPath, observationPath], {
      env: { PATH: process.env.PATH ?? '', TMPDIR: process.env.TMPDIR ?? '/tmp', NODE_NO_WARNINGS: '1' },
      stdio: 'ignore'
    });
    const closed = new Promise((resolve, reject) => {
      child.once('error', reject);
      child.once('close', (code, signal) => resolve({ code, signal }));
    });
    t.after(async () => {
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
      await closed;
      temporary.cleanup();
    });
    await waitForFiles([readyPath], 5000);
    assert.deepEqual(JSON.parse(readFileSync(observationPath, 'utf8')), { readyVisibleBeforeContent: false });
    child.kill('SIGKILL');
    assert.equal((await closed).signal, 'SIGKILL');
    assert.equal(readFileSync(readyPath, 'utf8'), stage === 'after-cursor' ? '1' : 'ready');
    const reopened = DurableWorkflowStore.open({ databasePath: temporary.databasePath, clock: () => 20_000 });
    try { assert.equal(reopened.getTelemetryCursor(), stage === 'after-cursor' ? 1 : 0); }
    finally { reopened.close(); }
  });
}
