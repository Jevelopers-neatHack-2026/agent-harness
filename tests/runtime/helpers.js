import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';

export function temporaryDatabase(prefix = 'agent-harness-runtime-') {
  const directory = mkdtempSync(join(tmpdir(), prefix));
  return {
    directory,
    databasePath: join(directory, 'workflow.sqlite'),
    cleanup() {
      rmSync(directory, { recursive: true, force: true });
    }
  };
}

export function spawnFixture(scriptPath, args = []) {
  const child = spawn(process.execPath, [scriptPath, ...args], {
    cwd: process.cwd(),
    env: { ...process.env, NODE_NO_WARNINGS: '1' },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  let stdout = '';
  let stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', chunk => { stdout += chunk; });
  child.stderr.on('data', chunk => { stderr += chunk; });
  const done = new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('close', code => resolve({ code, stdout, stderr }));
  });
  return { child, done };
}

export async function waitForFiles(paths, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (!paths.every(existsSync)) {
    if (Date.now() >= deadline) throw new Error('Fixture processes did not reach the release barrier.');
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}
