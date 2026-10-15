import { spawnSync } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import { DatabaseSync } from 'node:sqlite';
import { DurableWorkflowStore, createTelemetryExporter, createHmacTokenizer } from '../../../src/internal/index.js';
import { createOperationsWorker } from '../../../src/worker/private/worker.js';

const [databasePath, flushMarkerPath] = process.argv.slice(2);
if (![databasePath, flushMarkerPath].every(value => typeof value === 'string' && value.length > 0)) process.exit(2);

const scope = { tenantId: 'signal-tenant', projectId: 'signal-project' };
const planJson = JSON.stringify({
  kind: 'agent-harness.private-synthetic-plan', version: 1,
  steps: Array.from({ length: 32 }, (_, index) => ({
    id: `s${String(index).padStart(2, '0')}`, operation: 'text.concat',
    dependsOn: index % 12 === 0 ? [] : [`s${String(index - 1).padStart(2, '0')}`],
    args: { parts: index % 12 === 0 ? ['x'.repeat(512)] : [{ $result: `s${String(index - 1).padStart(2, '0')}` }, ''] }
  }))
});
const store = DurableWorkflowStore.open({ databasePath, busyTimeoutMs: 20 });
const tokenizer = createHmacTokenizer(Buffer.alloc(32, 23));
let sinkCalls = 0;
const exporter = createTelemetryExporter({
  store,
  tokenizer,
  sink: () => {
    sinkCalls += 1;
    writeFileSync(flushMarkerPath, JSON.stringify({ sinkCalls }));
    return new Promise(() => {});
  },
  limits: {
    queueRecords: 4, queueBytes: 4096, batchSize: 4, maxAttempts: 1,
    retryDelayMs: 0, sinkTimeoutMs: 350, maxBatchesPerFlush: 1,
    maxPolicyDropsPerFlush: 1
  }
});
const worker = createOperationsWorker({
  databasePath,
  ownerId: 'review-worker',
  telemetry: exporter,
  config: { shutdownTimeoutMs: 1_500, childKillGraceMs: 40, childReapReserveMs: 200 }
});
let removeHandlers;
let runnerPid = null;
let firstSignalObservedAt = null;
let stage = 'worker_start';
const observeFirstSignal = () => {
  if (firstSignalObservedAt === null) firstSignalObservedAt = performance.now();
};
const elapsedMs = startedAt => {
  const elapsed = performance.now() - startedAt;
  return Number.isFinite(elapsed) && elapsed >= 0 ? Number(elapsed.toFixed(3)) : null;
};
process.on('SIGTERM', observeFirstSignal);
process.on('SIGINT', observeFirstSignal);
try {
  removeHandlers = worker.installSignalHandlers();
  stage = 'runner_wait';
  const loop = worker.start();
  const submitted = worker.submitSyntheticWork({
    trustedSyntheticScope: scope, idempotencyKey: 'repeated-signals', planJson
  });
  if (!submitted.accepted) throw new Error('The signal cleanup fixture was not admitted.');
  const startDeadline = Date.now() + 5_000;
  while (worker.activeChildCount === 0 && Date.now() < startDeadline) await delay(2);
  stage = 'runner_pause';
  if (worker.activeChildCount !== 1) throw new Error('The actual owned runner did not start.');
  const pgrep = spawnSync('pgrep', ['-P', String(process.pid)], { encoding: 'utf8' });
  if (pgrep.status !== 0) throw new Error('The actual owned runner PID was not observable in the fixture process.');
  runnerPid = Number(pgrep.stdout.trim().split(/\s+/u).at(-1));
  if (!Number.isSafeInteger(runnerPid) || runnerPid < 2 || !process.kill(runnerPid, 'SIGSTOP')) {
    throw new Error('The fixture could not pause only its actual owned runner child.');
  }
  const readyWrittenAt = performance.now();
  process.stdout.write(JSON.stringify({
    event: 'ready', providerCalls: 0, childPaused: true, jobId: submitted.jobId
  }) + '\n');
  stage = 'worker_stop';
  await loop;
  const stopped = await worker.stop('signal');
  const signalToFirstStopElapsedMs = firstSignalObservedAt === null
    ? null : elapsedMs(firstSignalObservedAt);
  if (!stopped.stopped || !stopped.childrenReaped || !stopped.storeClosed) {
    throw new Error('Repeated signals did not complete one bounded worker stop.');
  }
  const diagnostics = exporter.diagnostics();
  if (sinkCalls !== 1 || diagnostics.cursor !== 0 || diagnostics.pendingRecords <= 0) {
    throw new Error('Telemetry close did not retain its pending cursor and durable outbox.');
  }
  const afterStop = new DatabaseSync(databasePath, { readOnly: true });
  const stoppedJob = afterStop.prepare('SELECT state, generation, fence_token FROM operations_work WHERE idempotency_key=?').get('repeated-signals');
  const stoppedCounters = afterStop.prepare('SELECT pending_records, in_flight FROM operations_meta WHERE singleton=1').get();
  const attemptCount = Number(afterStop.prepare('SELECT COUNT(*) AS count FROM operation_attempts').get().count);
  afterStop.close();
  stage = 'stop_validate';
  if (stoppedJob?.state !== 'queued' || Number(stoppedCounters.in_flight) !== 0 || attemptCount !== 0) {
    throw new Error('Shutdown did not durably requeue pure local work or changed provider-attempt state.');
  }
  const phaseElapsedMs = {
    signalToFirstStopElapsedMs,
    restartWorkElapsedMs: null,
    finalStopElapsedMs: null,
    readyToFinalRecordElapsedMs: elapsedMs(readyWrittenAt)
  };
  stage = 'record_output';
  process.stdout.write(JSON.stringify({
    event: 'stopped', stopped, sinkCalls, providerCalls: 0,
    phaseElapsedMs,
    telemetry: { cursor: diagnostics.cursor, pendingRecords: diagnostics.pendingRecords, state: diagnostics.state },
    workAfterStop: { state: stoppedJob.state, generation: Number(stoppedJob.generation), fence: Number(stoppedJob.fence_token) },
    jobId: submitted.jobId,
    attemptCount
  }) + '\n');
} catch (error) {
  if (runnerPid !== null) {
    try { process.kill(runnerPid, 'SIGCONT'); } catch {}
  }
  const failureStage = new Set([
    'worker_start', 'runner_wait', 'runner_pause', 'worker_stop',
    'stop_validate', 'record_output'
  ]).has(stage) ? stage : 'unknown';
  const errorClass = error instanceof TypeError ? 'type'
    : error instanceof RangeError ? 'range' : 'error';
  process.stderr.write(`AH11_OPS_OPS06_STOP_FAILURE stage=${failureStage} errorClass=${errorClass}\n`);
  process.exitCode = 1;
} finally {
  process.off('SIGTERM', observeFirstSignal);
  process.off('SIGINT', observeFirstSignal);
  try { removeHandlers?.(); } catch {}
  try { if (worker.lifecycle !== 'stopped') await worker.stop('test'); } catch {}
  try { tokenizer.destroy(); } catch {}
  try { store.close(); } catch {}
}
