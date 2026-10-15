import { DatabaseSync } from 'node:sqlite';
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import { createOperationsWorker } from '../../../src/worker/private/worker.js';
import { closedErrorProjection, closedWorkObservation, observeWorkerCleanup, publishPrimaryThenCleanup } from '../closed-diagnostics.js';

const [databasePath, jobId] = process.argv.slice(2);
if (![databasePath, jobId].every(value => typeof value === 'string' && value.length > 0)) process.exit(2);
const scope = { tenantId: 'signal-tenant', projectId: 'signal-project' };
let worker = null;
let stopped = false;
let stage = 'restart_start';
let reason = 'none';
let primaryFailed = false;
let primaryError;
let cleanupFailed = false;
let cleanupError;
let nativeClose = 'unobserved';
let stoppedCleanup = {};
let lastObservation = { lastState: 'unobserved', lastReason: 'none' };
let resultPresent = false;
const elapsedMs = startedAt => {
  const elapsed = performance.now() - startedAt;
  return Number.isFinite(elapsed) && elapsed >= 0 ? Number(elapsed.toFixed(3)) : null;
};
try {
  worker = createOperationsWorker({ databasePath, ownerId: 'review-worker-restart' });
  const restartStartedAt = performance.now();
  const loop = worker.start();
  const deadline = Date.now() + 8_000;
  let completed = null;
  stage = 'restart_wait';
  while (Date.now() < deadline) {
    try {
      completed = worker.readScopedWorkPage({ trustedSyntheticScope: scope, limit: 10 }).items
        .find(item => item.jobId === jobId);
      lastObservation = closedWorkObservation(completed);
    } catch (error) { reason = 'read_failure'; throw error; }
    if (completed?.state === 'completed') break;
    await delay(10);
  }
  if (completed?.state !== 'completed') { reason = 'wait_deadline'; throw new Error('restart_wait'); }
  const restartWorkElapsedMs = elapsedMs(restartStartedAt);
  const finalStopStartedAt = performance.now();
  stage = 'restart_stop';
  const finalStop = await worker.stop('test');
  stopped = true;
  nativeClose = finalStop.storeClosed === true ? 'confirmed' : 'unknown';
  stoppedCleanup = { nativeClose, cleanupFailed: finalStop.stopped !== true
    || finalStop.childrenReaped !== true || finalStop.storeClosed !== true };
  await loop;
  const finalStopElapsedMs = elapsedMs(finalStopStartedAt);
  if (!finalStop.stopped || !finalStop.childrenReaped || !finalStop.storeClosed) {
    reason = 'state_mismatch'; throw new Error('restart_stop');
  }
  stage = 'restart_verify';
  const final = new DatabaseSync(databasePath, { readOnly: true });
  let completedJob;
  let completedWorkflow;
  let finalAttempts;
  let readFailed = false;
  let readError;
  try {
    completedJob = final.prepare('SELECT state, generation, fence_token FROM operations_work WHERE job_id=?').get(jobId);
    completedWorkflow = final.prepare('SELECT state, fence_token FROM workflows WHERE workflow_id=(SELECT workflow_id FROM operations_work WHERE job_id=?)').get(jobId);
    finalAttempts = Number(final.prepare('SELECT COUNT(*) AS count FROM operation_attempts').get().count);
  } catch (error) { readFailed = true; readError = error; reason = 'read_failure'; }
  finally {
    try { final.close(); } catch (error) { cleanupFailed = true; cleanupError = error; nativeClose = 'failed'; }
  }
  if (readFailed) throw readError;
  if (completedJob?.state !== 'completed' || completedWorkflow?.state !== 'succeeded' || finalAttempts !== 0) {
    reason = 'state_mismatch'; throw new Error('restart_verify');
  }
  process.stdout.write(`${JSON.stringify({
    event: 'restarted', providerCalls: 0,
    phaseElapsedMs: { restartWorkElapsedMs, finalStopElapsedMs },
    afterRestart: { state: completedJob.state, workflowState: completedWorkflow.state,
      generation: Number(completedJob.generation), fence: Number(completedWorkflow.fence_token) },
    attemptCount: finalAttempts
  })}\n`);
  resultPresent = true;
} catch (error) {
  primaryFailed = true; primaryError = error;
  if (reason === 'none') reason = 'phase_failure';
  try { process.stderr.write(`AH11_OPS_OPS06_RESTART_FAILURE stage=${stage} errorClass=error\n`); } catch {}
  process.exitCode = 1;
} finally {
  const primary = { caseId: 'OPS06.C01', phase: stage, reason,
    primaryFailed, ...(primaryFailed ? closedErrorProjection(primaryError)
      : { errorKind: 'none', code: 'none', sqlitePrimaryCode: null }),
    ...lastObservation, nativeClose, resultPresent };
  const publication = await publishPrimaryThenCleanup(line => process.stderr.write(line), primary, async () => {
    let workerCleanup = stoppedCleanup;
    if (!stopped && worker !== null) workerCleanup = await observeWorkerCleanup(worker, 'test');
    if (cleanupFailed) {
      const clean = closedErrorProjection(cleanupError);
      workerCleanup = { ...workerCleanup, cleanupFailed: true, cleanupKind: clean.errorKind,
        cleanupCode: clean.code, cleanupPrimaryCode: clean.sqlitePrimaryCode, nativeClose: 'failed' };
    }
    return workerCleanup;
  });
  if (cleanupFailed || publication.cleanup.cleanupFailed === true
      || publication.primaryPublication.failed || publication.cleanupPublication.failed) process.exitCode = 1;
}
