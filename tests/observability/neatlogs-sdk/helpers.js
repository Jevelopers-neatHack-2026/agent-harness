import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import {
  DurableWorkflowStore,
  createHmacTokenizer,
  projectTelemetryEvent
} from '@jevelopers/agent-harness-runtime';
export { writeJsonAtomically } from './atomic-json-publisher.js';

export const SYNTHETIC_CANARIES = Object.freeze([
  'SYNTHETIC_RAW_ID_CANARY_41a',
  'SYNTHETIC_URL_CANARY_https://private.invalid/path?key=suffix',
  'SYNTHETIC_SECRET_SUFFIX_CANARY_72c',
  'SYNTHETIC_CUSTOMER_TEXT_CANARY_05e',
  'SYNTHETIC_ERROR_TEXT_CANARY_98d',
  'SYNTHETIC_SESSION_MARKER_CANARY_36b'
]);

export function createTemporaryDirectory(prefix = 'neatlogs-sdk-local-') {
  const directory = mkdtempSync(join(tmpdir(), prefix));
  return {
    directory,
    cleanup() {
      rmSync(directory, { recursive: true, force: true });
    }
  };
}

export function projectStoreEvents(store) {
  const page = store.readEventPage({ afterSequence: 0, limit: 32 });
  const tokenizer = createHmacTokenizer(Buffer.alloc(32, 0x5a));
  const tokenize = tokenizer.tokenize.bind(tokenizer);
  let records;
  try {
    records = page.events.map(event => projectTelemetryEvent(event, tokenize));
  } finally {
    tokenizer.destroy?.();
  }
  return { records, rawEventCount: page.events.length };
}

export function createProjectedStoreEvents(databasePath, { workflowCount = 1 } = {}) {
  let generatedId = 0;
  const store = DurableWorkflowStore.open({
    databasePath,
    clock: () => 2_000,
    idFactory: () => `local-generated-${randomUUID()}-${++generatedId}`
  });
  if (!Number.isSafeInteger(workflowCount) || workflowCount < 1 || workflowCount > 32) {
    store.close();
    throw new TypeError('Synthetic workflow count must be from 1 through 32.');
  }
  for (let index = 0; index < workflowCount; index += 1) {
    const suffix = index === 0 ? '' : `:${index}`;
    const workflowId = `workflow:${SYNTHETIC_CANARIES[0]}${suffix}`;
    store.createWorkflow({
      workflowId,
      eventId: `event:${SYNTHETIC_CANARIES[0]}${suffix}`,
      checkpoint: {
        customer: SYNTHETIC_CANARIES[3],
        privateUrl: SYNTHETIC_CANARIES[1],
        rawError: SYNTHETIC_CANARIES[4]
      }
    });
    if (workflowCount === 1) {
      const lease = store.acquireLease({
        workflowId,
        ownerId: 'synthetic-local-worker',
        ttlMs: 10_000,
        eventId: `event:${SYNTHETIC_CANARIES[2]}`
      });
      store.transitionWorkflow({
        ...lease,
        expectedRevision: 0,
        toState: 'running',
        eventId: `event:${SYNTHETIC_CANARIES[5]}`
      });
    }
  }
  const projected = projectStoreEvents(store);
  return { store, ...projected };
}

function sanitizedEnvironment() {
  return Object.freeze({
    TMPDIR: '/tmp',
    NODE_NO_WARNINGS: '1'
  });
}

export function networkGuardMode() {
  return process.platform === 'darwin' && existsSync('/usr/bin/sandbox-exec')
    ? 'darwin-sandbox-exec-deny-network'
    : 'explicit-local-exporter-no-network-path';
}

export function spawnNodeFixture(name, args = [], options = {}) {
  const script = resolve(dirname(new URL(import.meta.url).pathname), 'fixtures', name);
  const guardMode = options.networkGuard === false ? 'fixture-only' : networkGuardMode();
  const sandboxed = guardMode === 'darwin-sandbox-exec-deny-network';
  const command = sandboxed ? '/usr/bin/sandbox-exec' : process.execPath;
  const commandArgs = sandboxed
    ? ['-p', '(version 1)(allow default)(deny network*)', process.execPath, script, ...args]
    : [script, ...args];
  const child = spawn(command, commandArgs, {
    cwd: process.cwd(),
    env: sanitizedEnvironment(),
    stdio: ['ignore', 'pipe', 'pipe']
  });
  let stdout = '';
  let stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', chunk => { stdout += chunk; });
  child.stderr.on('data', chunk => { stderr += chunk; });
  const done = new Promise((resolveDone, rejectDone) => {
    child.once('error', rejectDone);
    child.once('close', (code, signal) => resolveDone({ code, signal, stdout, stderr, guardMode }));
  });
  const timeoutMs = options.timeoutMs ?? 15_000;
  const timeout = setTimeout(() => {
    child.kill('SIGKILL');
  }, timeoutMs);
  timeout.unref?.();
  return {
    child,
    guardMode,
    done: done.finally(() => clearTimeout(timeout))
  };
}

export function writeJson(path, value) {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
}

export function readJson(path) {
  return JSON.parse(readFileSync(path, 'utf8'));
}

export function writeRecords(directory, records, basename = 'projected-records.json') {
  const path = join(directory, basename);
  writeJson(path, records);
  return path;
}
