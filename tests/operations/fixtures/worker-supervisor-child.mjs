import { createOperationsWorker } from '../../../src/worker/private/worker.js';
import { closedErrorProjection, encodeClosedDiagnostic, observeWorkerCleanup } from '../closed-diagnostics.js';
import { readOperationsConfig } from '../../../src/worker/private/config.js';
import { publishFixtureRetryDiagnostic, runFixtureBusyRetry } from '../fixture-busy-retry.js';

const [databasePath, ownerId, idempotencyKey = 'worker-crash-recovery', deadlineText] = process.argv.slice(2);
if (![databasePath, ownerId, idempotencyKey].every(value => typeof value === 'string' && value.length > 0)) process.exit(2);
if (deadlineText !== undefined && !/^[0-9]{1,24}$/u.test(deadlineText)) process.exit(2);
const deadlineNs = deadlineText === undefined ? process.hrtime.bigint() + 8_000_000_000n : BigInt(deadlineText);
const scope = { tenantId: 'worker-crash-tenant', projectId: 'worker-crash-project' };
const planJson = JSON.stringify({ kind: 'agent-harness.private-synthetic-plan', version: 1,
  steps: Array.from({ length: 32 }, (_, index) => ({ id: `cr${String(index).padStart(2, '0')}`,
    operation: 'text.concat', dependsOn: index % 12 === 0 ? [] : [`cr${String(index - 1).padStart(2, '0')}`],
    args: { parts: index % 12 === 0 ? ['x'.repeat(512)] : [{ $result: `cr${String(index - 1).padStart(2, '0')}` }, ''] }
  })) });
let worker;
let phase = 'worker_start';
let primaryFailed = false;
let primaryError;
let resultPresent = false;
let nativeClose = 'unobserved';
try {
  worker = createOperationsWorker({ databasePath, ownerId });
  worker.installSignalHandlers();
  const loop = worker.start();
  phase = 'admission';
  const attempted = await runFixtureBusyRetry({ kind: 'admission', deadlineNs,
    busyTimeoutMs: readOperationsConfig().busyTimeoutMs,
    invoke: () => worker.submitSyntheticWork({ trustedSyntheticScope: scope, idempotencyKey, planJson })
  });
  publishFixtureRetryDiagnostic(attempted, line => process.stderr.write(line));
  if (attempted.failed) throw attempted.error;
  const submitted = attempted.value;
  if (!submitted.accepted) throw new Error('Work admission failed.');
  process.stdout.write(`${JSON.stringify({ event: 'ready', ownerId, ...submitted, providerCalls: 0 })}\n`);
  resultPresent = true;
  phase = 'worker_run';
  await loop;
  phase = 'worker_stop';
  const stopped = await worker.stop('signal');
  nativeClose = stopped.storeClosed === true ? 'confirmed' : 'unknown';
  process.stdout.write(`${JSON.stringify({ event: 'stopped', ownerId, stopped, providerCalls: 0 })}\n`);
  process.exitCode = stopped.stopped ? 0 : 2;
} catch (error) {
  primaryFailed = true; primaryError = error; process.exitCode = 1;
} finally {
  let cleanup = {};
  if (worker && worker.lifecycle !== 'stopped') cleanup = await observeWorkerCleanup(worker, 'test');
  const projection = primaryFailed ? closedErrorProjection(primaryError) : { errorKind: 'none', code: 'none', sqlitePrimaryCode: null };
  const line = encodeClosedDiagnostic({ caseId: 'OPS05.C08', phase, primaryFailed,
    ...projection, resultPresent, nativeClose, ...cleanup });
  try { if (line) process.stderr.write(line); } catch {}
  if (cleanup.cleanupFailed === true) process.exitCode = 1;
}
