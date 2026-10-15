import { existsSync, renameSync, writeFileSync } from 'node:fs';
import { createOperationsWorker } from '../../../src/worker/private/worker.js';

const [databasePath, tenantId, projectId, idempotencyKey, planJson, outputPath, readyPath, releasePath,
  progressPath, retryGatePath] = process.argv.slice(2);
const MAX_BUSY_RETRIES = 4;
const MAX_RETRY_WINDOW_MS = 1_000;
const ALLOWED_CODES = new Set(['OPS_BACKPRESSURE', 'OPS_UNAVAILABLE']);
const ALLOWED_REASONS = new Set([
  'STORE_BUSY', 'STORE_UNAVAILABLE', 'WORKER_NOT_ACCEPTING', 'SCOPE_CAPACITY',
  'GLOBAL_QUEUE_FULL', 'TENANT_QUEUE_FULL', 'PROJECT_QUEUE_FULL',
  'RESOURCE_PROBE_UNAVAILABLE', 'WAL_HEADROOM_LOW', 'STORE_CAPACITY',
  'DISK_HEADROOM_LOW', 'TELEMETRY_BACKLOG_HIGH'
]);

function attemptSummary(result) {
  return Object.freeze({
    accepted: result?.accepted === true,
    duplicate: result?.duplicate === true ? true : result?.duplicate === false ? false : null,
    code: ALLOWED_CODES.has(result?.code) ? result.code : null,
    reasonCode: ALLOWED_REASONS.has(result?.reasonCode)
      ? result.reasonCode : result?.reasonCode === undefined ? null : 'OTHER_REASON',
    retryAfterMs: Number.isSafeInteger(result?.retryAfterMs) && result.retryAfterMs >= 0 && result.retryAfterMs <= 5_000
      ? result.retryAfterMs : null
  });
}

if (![databasePath, tenantId, projectId, idempotencyKey, planJson, outputPath, readyPath, releasePath]
  .concat([progressPath, retryGatePath]).every(value => typeof value === 'string')) {
  process.exit(2);
}
const worker = createOperationsWorker({ databasePath, ownerId: `ops-worker:${process.pid}` });
let didStop = false;
let result = null;
let stopResult = null;
let outcome = 'incomplete';
const attempts = [];
try {
  const readyTemporaryPath = `${readyPath}.${process.pid}.tmp`;
  writeFileSync(readyTemporaryPath, JSON.stringify({ storeOpen: true, pid: process.pid }), { flag: 'wx', mode: 0o600 });
  renameSync(readyTemporaryPath, readyPath);
  const releaseDeadline = Date.now() + 5_000;
  while (!existsSync(releasePath)) {
    if (Date.now() >= releaseDeadline) throw new Error('The bounded admission release gate expired.');
    await new Promise(resolveWait => setTimeout(resolveWait, 2));
  }
  void worker.start();
  const retryStartedAt = Date.now();
  for (let retry = 0; retry <= MAX_BUSY_RETRIES; retry += 1) {
    result = worker.submitSyntheticWork({
      trustedSyntheticScope: { tenantId, projectId },
      idempotencyKey,
      planJson
    });
    attempts.push(attemptSummary(result));
    const progressTemporaryPath = `${progressPath}.${process.pid}.tmp`;
    writeFileSync(progressTemporaryPath, JSON.stringify({ attempts }), { flag: 'w', mode: 0o600 });
    renameSync(progressTemporaryPath, progressPath);
    if (result.accepted || result.code !== 'OPS_BACKPRESSURE' || result.reasonCode !== 'STORE_BUSY'
        || retry === MAX_BUSY_RETRIES) break;
    const retryAfterMs = result.retryAfterMs;
    if (!Number.isSafeInteger(retryAfterMs) || retryAfterMs < 1 || retryAfterMs > 5_000
        || Date.now() - retryStartedAt + retryAfterMs > MAX_RETRY_WINDOW_MS) break;
    const retryAvailableAt = Date.now() + retryAfterMs;
    const retryDeadline = retryStartedAt + MAX_RETRY_WINDOW_MS;
    if (retry === 0) {
      while ((!existsSync(retryGatePath) || Date.now() < retryAvailableAt) && Date.now() < retryDeadline) {
        await new Promise(resolveWait => setTimeout(resolveWait, 2));
      }
      if (!existsSync(retryGatePath) || Date.now() < retryAvailableAt) break;
    } else {
      await new Promise(resolveWait => setTimeout(resolveWait, retryAfterMs));
    }
  }
  stopResult = await worker.stop('test');
  didStop = stopResult.stopped === true;
  outcome = result?.accepted && didStop ? 'accepted_and_stopped' : 'not_accepted_or_stop_incomplete';
  process.exitCode = outcome === 'accepted_and_stopped' ? 0 : 2;
} catch {
  outcome = 'child_error';
  process.exitCode = 2;
} finally {
  if (!didStop) {
    try {
      stopResult = await worker.stop('test');
      didStop = stopResult.stopped === true;
    } catch {}
  }
  try {
    writeFileSync(outputPath, JSON.stringify({
      result,
      attempts,
      stopped: didStop,
      outcome,
      childStatus: process.exitCode === 0 ? 0 : 2
    }), { flag: 'wx', mode: 0o600 });
  } catch {}
}
