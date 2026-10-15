import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { performance } from 'node:perf_hooks';
import { types as utilTypes } from 'node:util';
import { createOperationsWorker } from '../../src/worker/private/worker.js';
import { readStoreCompatibility } from '../../src/health/private/health.js';
import { DurableWorkflowStore } from '../../src/internal/index.js';
import { OPERATIONS_STORAGE_CAPABILITY } from '../../src/internal/private-ops-store-capability.js';
import { operationsStoragePolicy, readOperationsConfig, validateLocalPlanWork } from '../../src/worker/private/config.js';

const [transcriptPath] = process.argv.slice(2);
if (typeof transcriptPath !== 'string' || transcriptPath.length === 0) process.exit(2);

const transcript = [];
const root = mkdtempSync(join(tmpdir(), 'ah11-runbook-recovery-'));
const cleanups = [];
let activeCase = 'busy_store_spans_stop';
let activeStage = 'fixture_start';
const plan32 = JSON.stringify({
  kind: 'agent-harness.private-synthetic-plan', version: 1,
  steps: Array.from({ length: 32 }, (_, index) => ({
    id: `r${String(index).padStart(2, '0')}`, operation: 'text.concat',
    dependsOn: index % 12 === 0 ? [] : [`r${String(index - 1).padStart(2, '0')}`],
    args: { parts: index % 12 === 0 ? ['x'.repeat(512)] : [{ $result: `r${String(index - 1).padStart(2, '0')}` }, ''] }
  }))
});
const plan = JSON.stringify({
  kind: 'agent-harness.private-synthetic-plan', version: 1,
  steps: [{ id: 'sum', operation: 'number.add', dependsOn: [], args: { left: 20, right: 22 } }]
});

function add(caseId, evidence) {
  transcript.push({ case: caseId, outcome: 'pass', dependencyMode: 'synthetic_offline', providerCalls: 0, ...evidence });
}

function openOperationsStore(databasePath, config, clock = Date.now) {
  const store = DurableWorkflowStore.open({ databasePath, busyTimeoutMs: config.busyTimeoutMs, clock });
  store._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY, {
    maxStoreBytes: config.maxStoreBytes, maxWalBytes: config.maxWalBytes,
    minFreeDiskBytes: config.minFreeDiskBytes, policy: operationsStoragePolicy(config)
  });
  cleanups.push(() => { try { store.close(); } catch {} });
  return store;
}

async function waitFor(predicate, timeoutMs = 8_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = predicate();
    if (value) return value;
    await new Promise(resolve => setTimeout(resolve, 5));
  }
  throw new Error('A bounded runbook recovery case did not reach its expected state.');
}

function failureClass(error) {
  if (error !== null && typeof error === 'object' && utilTypes.isNativeError(error)) {
    try {
      const code = Object.getOwnPropertyDescriptor(error, 'code')?.value;
      const errcode = Object.getOwnPropertyDescriptor(error, 'errcode')?.value;
      if (code === 'ERR_ASSERTION') return { errorClass: 'assertion', nativePrimaryCode: null };
      if (code === 'ERR_SQLITE_ERROR' && Number.isSafeInteger(errcode)
          && errcode >= 0 && errcode <= 0x7fffffff) {
        const primary = errcode & 0xff;
        return { errorClass: primary === 5 || primary === 6 ? 'sqlite_busy' : 'sqlite_other',
          nativePrimaryCode: primary === 5 || primary === 6 ? primary : null };
      }
    } catch {}
    if (error instanceof TypeError) return { errorClass: 'type', nativePrimaryCode: null };
    if (error instanceof RangeError) return { errorClass: 'range', nativePrimaryCode: null };
    return { errorClass: 'error', nativePrimaryCode: null };
  }
  return { errorClass: 'unknown', nativePrimaryCode: null };
}

function failAt(caseId, stage) {
  activeCase = caseId;
  activeStage = stage;
}

try {
  {
    failAt('busy_store_spans_stop', 'case_setup');
    const databasePath = join(root, 'busy.sqlite');
    const worker = createOperationsWorker({ databasePath, ownerId: 'review-worker-runbook-busy' });
    cleanups.push(() => worker.stop('test'));
    const loop = worker.start();
    const lock = new DatabaseSync(databasePath, { timeout: 20 });
    cleanups.push(() => { try { if (lock.isTransaction) lock.exec('ROLLBACK'); } catch {} try { lock.close(); } catch {} });
    lock.exec('BEGIN IMMEDIATE');
    failAt('busy_store_spans_stop', 'busy_write');
    const result = worker.submitSyntheticWork({
      trustedSyntheticScope: { tenantId: 'busy-tenant', projectId: 'busy-project' },
      idempotencyKey: 'busy-recovery', planJson: plan
    });
    if (result.accepted || result.reasonCode !== 'STORE_BUSY') throw new Error('Busy-store saturation did not fail closed.');
    failAt('busy_store_spans_stop', 'worker_stop');
    const stopStarted = performance.now();
    const stopped = await worker.stop('test');
    const stopElapsedMs = performance.now() - stopStarted;
    await loop;
    if (!stopped.stopped || stopElapsedMs > 1_000) throw new Error('Shutdown did not complete while SQLite remained busy.');
    lock.exec('ROLLBACK');
    lock.close();
    failAt('busy_store_spans_stop', 'reopen_validate');
    const reopened = openOperationsStore(databasePath, readOperationsConfig());
    const rows = new DatabaseSync(databasePath, { readOnly: true });
    const count = Number(rows.prepare('SELECT COUNT(*) AS count FROM operations_work').get().count);
    rows.close();
    reopened.close();
    if (count !== 0) throw new Error('The busy rejection left a partial accepted row.');
    failAt('busy_store_spans_stop', 'case_complete');
    add('busy_store_spans_stop', { backpressure: result.reasonCode, stopped: stopped.stopped, stopElapsedMs, acceptedRowsAfterReopen: count });
  }

  {
    failAt('store_unavailable', 'compatibility_read');
    const unavailablePath = join(root, 'missing', 'workflow.sqlite');
    const result = readStoreCompatibility(unavailablePath);
    if (result.status !== 'store_unavailable') throw new Error('An unavailable store was not classified unavailable.');
    failAt('store_unavailable', 'case_complete');
    add('store_unavailable', { compatibility: result.status, dispatch: 'disabled' });
  }

  {
    failAt('future_schema', 'future_schema_write');
    const futurePath = join(root, 'future.sqlite');
    const future = new DatabaseSync(futurePath);
    future.exec("CREATE TABLE future_marker (value TEXT); INSERT INTO future_marker VALUES ('preserve'); PRAGMA user_version=99;");
    future.close();
    const before = readFileSync(futurePath);
    failAt('future_schema', 'worker_open');
    const worker = (() => {
      try { return createOperationsWorker({ databasePath: futurePath, ownerId: 'review-worker-future' }); }
      catch (error) { return error; }
    })();
    failAt('future_schema', 'byte_compare');
    const after = readFileSync(futurePath);
    if (worker?.code !== 'UNSUPPORTED_SCHEMA_VERSION' || !before.equals(after)
        || readStoreCompatibility(futurePath).status !== 'schema_unsupported') {
      throw new Error('Future-schema recovery changed bytes or exposed a worker.');
    }
    failAt('future_schema', 'case_complete');
    add('future_schema', { compatibility: 'schema_unsupported', originalBytesPreserved: true, dispatch: 'disabled' });
  }

  {
    failAt('stuck_runner_review_escalation', 'case_setup');
    const databasePath = join(root, 'stuck-review.sqlite');
    const worker = createOperationsWorker({
      databasePath, ownerId: 'review-worker-runbook-stuck',
      config: { maxJobRuntimeMs: 250, childKillGraceMs: 40, childReapReserveMs: 200, shutdownTimeoutMs: 1_000 }
    });
    cleanups.push(() => worker.stop('test'));
    const loop = worker.start();
    const scope = { tenantId: 'stuck-tenant', projectId: 'stuck-project' };
    const submitted = worker.submitSyntheticWork({
      trustedSyntheticScope: scope, idempotencyKey: 'stuck-review', planJson: plan32,
      limitsJson: JSON.stringify({ maxElapsedMs: 200, maxStepMs: 50 })
    });
    if (!submitted.accepted) throw new Error('The bounded stuck-run fixture was not admitted.');
    failAt('stuck_runner_review_escalation', 'runner_pause');
    await waitFor(() => worker.activeChildCount === 1);
    const child = spawnSync('pgrep', ['-P', String(process.pid)], { encoding: 'utf8' });
    if (child.status !== 0) throw new Error('The stuck-review runner PID was not visible.');
    const runnerPid = Number(child.stdout.trim().split(/\s+/u).at(-1));
    if (!Number.isSafeInteger(runnerPid) || runnerPid < 2 || !process.kill(runnerPid, 'SIGSTOP')) {
      throw new Error('The stuck-review case could not pause its actual owned child.');
    }
    failAt('stuck_runner_review_escalation', 'await_park');
    const row = await waitFor(() => {
      const current = worker.readScopedWorkPage({ trustedSyntheticScope: scope, limit: 4 }).items
        .find(item => item.jobId === submitted.jobId);
      return current?.state === 'parked' ? current : null;
    }, 4_000);
    if (worker.activeChildCount !== 0 || worker.health().process.lastSafeErrorCode !== 'OPS_RUNNER_DEADLINE') {
      throw new Error('The actual stuck runner was not reaped through its owner deadline.');
    }
    failAt('stuck_runner_review_escalation', 'worker_stop');
    const stopped = await worker.stop('test');
    await loop;
    failAt('stuck_runner_review_escalation', 'reopen_validate');
    const reopened = openOperationsStore(databasePath, readOperationsConfig());
    const final = reopened._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: scope.tenantId, projectId: scope.projectId, jobId: submitted.jobId
    });
    const workflow = reopened.getWorkflow(submitted.workflowId);
    if (!stopped.stopped || final.state !== 'parked' || workflow.state !== 'review_required') {
      throw new Error('The stopped work did not remain durably parked for owner review.');
    }
    reopened.close();
    failAt('stuck_runner_review_escalation', 'case_complete');
    add('stuck_runner_review_escalation', { workStateAfterReopen: row.state, workflowStateAfterReopen: workflow.state,
      childReaped: true, action: 'preserve_and_escalate_to_runtime_owner' });
  }

  {
    failAt('unknown_hold_restart_escalation', 'hold_setup');
    const databasePath = join(root, 'unknown-hold.sqlite');
    const config = readOperationsConfig();
    let logicalNow = 70_000;
    const store = openOperationsStore(databasePath, config, () => logicalNow);
    const work = validateLocalPlanWork({ planJson: plan, limitsJson: '{}' }, 'runbook-unknown-workflow', config);
    const accepted = store._operationsAdmit(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'unknown-tenant', projectId: 'unknown-project', idempotencyKey: 'unknown-recovery',
      workJson: work.workJson, policy: operationsStoragePolicy(config)
    });
    if (!accepted.accepted) throw new Error('Unknown-outcome work was not admitted.');
    const ownerId = 'review-worker-runbook-unknown';
    const claim = store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId, policy: operationsStoragePolicy(config)
    }).claim;
    store._operationsSetRunnerPid(OPERATIONS_STORAGE_CAPABILITY, {
      jobId: claim.jobId, ownerId, generation: claim.generation, fence: claim.fence,
      runnerPid: 2_147_483_647
    });
    failAt('unknown_hold_restart_escalation', 'intent_write');
    store.recordDispatchIntent({
      workflowId: claim.workflowId, ownerId, fence: claim.fence, expectedRevision: claim.revision,
      attemptId: 'runbook-unknown-attempt', logicalOperationRef: 'runbook-unknown-logical',
      authorityHoldRef: 'synthetic-local-bookkeeping:runbook-unknown-hold',
      transportAttemptId: 'runbook-unknown-transport', eventId: 'runbook-unknown-intent'
    });
    failAt('unknown_hold_restart_escalation', 'lease_expiry');
    logicalNow = claim.leaseExpiresAtMs + 1;
    store._operationsExpiredClaims(OPERATIONS_STORAGE_CAPABILITY, { policy: operationsStoragePolicy(config) });
    store.close();
    failAt('unknown_hold_restart_escalation', 'restart');
    const restarted = createOperationsWorker({ databasePath, ownerId: 'review-worker-runbook-unknown-restart', config });
    cleanups.push(() => restarted.stop('test'));
    const loop = restarted.start();
    await new Promise(resolve => setTimeout(resolve, 50));
    const health = restarted.health();
    failAt('unknown_hold_restart_escalation', 'hold_verify');
    if (!health.degradedReasons.includes('RECONCILIATION_PENDING')) throw new Error('Unknown outcome did not remain visible after restart.');
    const stopped = await restarted.stop('test');
    await loop;
    const reopened = openOperationsStore(databasePath, config);
    const attempt = reopened.getAttempt('runbook-unknown-attempt');
    const job = reopened._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: 'unknown-tenant', projectId: 'unknown-project', jobId: accepted.jobId
    });
    if (!stopped.stopped || job.state !== 'unknown' || attempt.state !== 'intent'
        || attempt.authorityHoldRef !== 'synthetic-local-bookkeeping:runbook-unknown-hold') {
      throw new Error('Unknown recovery released or replaced its synthetic hold.');
    }
    reopened.close();
    failAt('unknown_hold_restart_escalation', 'case_complete');
    add('unknown_hold_restart_escalation', { workStateAfterReopen: job.state, attemptState: attempt.state,
      holdRetained: true, dispatchAfterRestart: 0, action: 'preserve_and_escalate_to_finance_owner' });
  }

  writeFileSync(transcriptPath, `${transcript.map(item => JSON.stringify(item)).join('\n')}\n`);
  process.stdout.write(`${JSON.stringify({ cases: transcript.length, passed: transcript.length, providerCalls: 0 })}\n`);
} catch (error) {
  const safeCase = new Set([
    'busy_store_spans_stop', 'store_unavailable', 'future_schema',
    'stuck_runner_review_escalation', 'unknown_hold_restart_escalation'
  ]).has(activeCase) ? activeCase : 'busy_store_spans_stop';
  const stages = new Set([
    'fixture_start', 'case_setup', 'busy_write', 'worker_stop', 'reopen_validate',
    'compatibility_read', 'future_schema_write', 'worker_open', 'byte_compare',
    'runner_pause', 'await_park', 'restart', 'hold_setup', 'intent_write',
    'lease_expiry', 'hold_verify', 'case_complete'
  ]);
  const safeStage = stages.has(activeStage) ? activeStage : 'fixture_start';
  process.stderr.write(`AH11_OPS_OPS12_RECOVERY_FAILURE ${JSON.stringify({
    schema: 'agent-harness.ah11-ops12-recovery-failure.v1',
    caseId: safeCase, stage: safeStage, ...failureClass(error)
  })}\n`);
  process.exitCode = 1;
} finally {
  for (const cleanup of cleanups.reverse()) {
    try { await cleanup(); } catch {}
  }
  rmSync(root, { recursive: true, force: true });
}
