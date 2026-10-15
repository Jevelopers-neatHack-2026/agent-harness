import { spawn, spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { performance } from 'node:perf_hooks';
import { types as utilTypes } from 'node:util';
import { DurableWorkflowStore } from '../../internal/durable-store.js';
import { TelemetryExporter } from '../../internal/telemetry.js';
import { OPERATIONS_STORAGE_CAPABILITY } from '../../internal/private-ops-store-capability.js';
import {
  closeOperationsChildCapture,
  isOperationsDiagnosticSession,
  OPS07_CAPTURE_FLAG,
  prepareOperationsChildCapture,
  recordOperationsSupervisor
} from '../../internal/private-ops-diagnostics.js';
import {
  operationsStoragePolicy,
  readOperationsConfig,
  validateLocalPlanWork
} from './config.js';
import { readOperationsHealth, requestOwnerRetention } from '../../health/private/health.js';

const RUNNER_PATH = fileURLToPath(new URL('./runner.js', import.meta.url));
const MAX_WAITERS = 16;

function safeOwn(value, allowed, required = []) {
  if (value === null || typeof value !== 'object' || utilTypes.isProxy(value) || Array.isArray(value)) {
    throw new TypeError('The operations request is invalid.');
  }
  let prototype;
  let keys;
  try {
    prototype = Object.getPrototypeOf(value);
    keys = Reflect.ownKeys(value);
  } catch {
    throw new TypeError('The operations request is invalid.');
  }
  if ((prototype !== Object.prototype && prototype !== null) || keys.length > allowed.length) {
    throw new TypeError('The operations request is invalid.');
  }
  const result = Object.create(null);
  for (const key of keys) {
    if (typeof key !== 'string' || !allowed.includes(key)) throw new TypeError('The operations request is invalid.');
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      throw new TypeError('The operations request is invalid.');
    }
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) {
      throw new TypeError('The operations request is invalid.');
    }
    result[key] = descriptor.value;
  }
  if (required.some(key => !Object.hasOwn(result, key))) throw new TypeError('The operations request is invalid.');
  return result;
}

function safeId(value, maximum = 128) {
  return typeof value === 'string' && value.trim() === value && value.length <= maximum
    && /^[A-Za-z0-9][A-Za-z0-9._:-]*$/u.test(value);
}

function nowMono() {
  return performance.now();
}

function remaining(deadline) {
  return Math.max(0, Math.floor(deadline - nowMono()));
}

function raceWithDelay(promise, timeoutMs, timeoutValue) {
  let timer;
  const timeout = new Promise(resolvePromise => {
    timer = setTimeout(() => resolvePromise(timeoutValue), Math.max(0, timeoutMs));
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function processIsAlive(pid) {
  if (!Number.isSafeInteger(pid) || pid < 2) return false;
  try {
    process.kill(pid, 0);
  } catch (error) {
    if (error?.code === 'ESRCH') return false;
    return true;
  }
  if (process.platform === 'linux') {
    try {
      const stat = readFileSync(`/proc/${pid}/stat`, 'utf8');
      const state = stat.slice(stat.lastIndexOf(')') + 2).split(/\s+/u, 1)[0];
      return state !== 'Z' && state !== 'X';
    } catch (error) {
      return error?.code !== 'ENOENT';
    }
  }
  if (process.platform === 'darwin') {
    const result = spawnSync('ps', ['-o', 'stat=', '-p', String(pid)], {
      encoding: 'utf8', maxBuffer: 4_096, timeout: 250,
      env: { PATH: '/usr/bin:/bin' }
    });
    if (result.status === 0) {
      const state = result.stdout.trim().charAt(0);
      return state !== 'Z' && state !== 'X';
    }
    if (result.status === 1) return false;
  }
  return true;
}

async function waitForEvent(child, event, timeoutMs) {
  return new Promise(resolvePromise => {
    let done = false;
    const finish = value => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      child.off(event, onEvent);
      resolvePromise(value);
    };
    const onEvent = (...args) => finish(args);
    const timer = setTimeout(() => finish(null), Math.max(0, timeoutMs));
    child.once(event, onEvent);
    if (child.exitCode !== null || child.signalCode !== null) finish([child.exitCode, child.signalCode]);
  });
}

function normalizeScope(value) {
  const scope = safeOwn(value, ['tenantId', 'projectId'], ['tenantId', 'projectId']);
  if (!safeId(scope.tenantId) || !safeId(scope.projectId)) {
    throw new TypeError('The synthetic operations scope is invalid.');
  }
  return Object.freeze({ tenantId: scope.tenantId, projectId: scope.projectId });
}

/**
 * Private local operations supervisor. Scope is an internal mechanics precondition,
 * not application authorization; this worker never enables external dispatch.
 */
export function createOperationsWorker(input) {
  const fields = safeOwn(input, ['databasePath', 'config', 'telemetry', 'ownerId', 'diagnosticSession']);
  const diagnosticSession = fields.diagnosticSession;
  if (diagnosticSession !== undefined && !isOperationsDiagnosticSession(diagnosticSession)) {
    throw new TypeError('The operations diagnostic session is invalid.');
  }
  if (typeof fields.databasePath !== 'string' || fields.databasePath.trim() === ''
      || fields.databasePath === ':memory:' || fields.databasePath.length > 1024) {
    throw new TypeError('A file-backed operations database path is required.');
  }
  const config = readOperationsConfig(fields.config);
  const ownerId = fields.ownerId ?? `ops-worker:${randomUUID()}`;
  if (!safeId(ownerId, 128)) throw new TypeError('The operations worker identity is invalid.');
  const telemetry = fields.telemetry;
  if (telemetry !== undefined) {
    let prototype;
    let ownKeys;
    try {
      if (telemetry === null || typeof telemetry !== 'object' || utilTypes.isProxy(telemetry)) throw new Error();
      prototype = Object.getPrototypeOf(telemetry);
      ownKeys = Reflect.ownKeys(telemetry);
    } catch {
      throw new TypeError('The local telemetry exporter is invalid.');
    }
    if (prototype !== TelemetryExporter.prototype || ownKeys.length !== 0) {
      throw new TypeError('The local telemetry exporter is invalid.');
    }
  }
  const databasePath = resolve(fields.databasePath);
  const store = DurableWorkflowStore.open({ databasePath, busyTimeoutMs: config.busyTimeoutMs });
  const policy = operationsStoragePolicy(config);
  try {
    store._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY, {
      maxStoreBytes: config.maxStoreBytes,
      maxWalBytes: config.maxWalBytes,
      minFreeDiskBytes: config.minFreeDiskBytes,
      policy
    });
  } catch (error) {
    try { store.close(); } catch {}
    throw error;
  }
  const children = new Map();
  const waiters = new Set();
  let lifecycle = 'starting';
  let loopPromise = null;
  let stopPromise = null;
  let heartbeatAt = nowMono();
  let lastSafeErrorCode = null;
  let acquiredLoop = false;
  const signalHandlerRemovers = new Set();

  function removeSignalHandlers() {
    for (const remove of [...signalHandlerRemovers]) remove();
  }

  function wakeWaiters() {
    if (ownedChildCount() > 0 && lifecycle !== 'stopped') return;
    for (const waiter of waiters) {
      clearTimeout(waiter.timer);
      waiter.resolve(lifecycle === 'accepting' || lifecycle === 'saturated');
    }
    waiters.clear();
  }

  function ownedChildCount() {
    let count = 0;
    for (const active of children.values()) if (!active.reaped) count += 1;
    return count;
  }

  function pendingDurableCleanupCount() {
    let count = 0;
    for (const active of children.values()) if (active.reaped && !active.settled) count += 1;
    return count;
  }

  function finishOwnedOutcome(active) {
    if (!active.reaped || active.settled) return active.settled;
    let finishCode = null;
    try {
      store._operationsFinish(OPERATIONS_STORAGE_CAPABILITY, {
        jobId: active.claim.jobId,
        ownerId,
        generation: active.claim.generation,
        fence: active.claim.fence,
        outcome: active.outcome,
        childExited: true
      });
    } catch (error) {
      if (error?.code === 'STALE_FENCE') {
        // A newer generation already owns the durable row. Dropping this reaped local
        // process cannot mutate or release the newer claim.
        lastSafeErrorCode = 'OPS_STALE_RUNNER_RESULT_DISCARDED';
        active.settled = true;
        finishCode = 'STALE_FENCE';
      } else {
        active.finishPending = true;
        active.finishRetryable = error?.code === 'STORE_BUSY';
        active.finishErrorCode = typeof error?.code === 'string' && /^[A-Z0-9_]{1,64}$/u.test(error.code)
          ? error.code : 'OPS_RESULT_PERSIST_FAILED';
        finishCode = active.finishErrorCode;
        lastSafeErrorCode = active.finishErrorCode;
        if (diagnosticSession !== undefined && active.captureSlot !== null) {
          recordOperationsSupervisor(diagnosticSession, active.captureSlot, 'finish_result', {
            outcome: active.outcome, settled: false, code: finishCode,
            generation: active.claim.generation, fence: active.claim.fence
          });
        }
        return false;
      }
    }
    active.finishPending = false;
    active.finishRetryable = false;
    if (lastSafeErrorCode === active.finishErrorCode) lastSafeErrorCode = null;
    active.finishErrorCode = null;
    if (diagnosticSession !== undefined && active.captureSlot !== null) {
      recordOperationsSupervisor(diagnosticSession, active.captureSlot, 'finish_result', {
        outcome: active.outcome, settled: true, code: finishCode,
        generation: active.claim.generation, fence: active.claim.fence
      });
    }
    clearTimeout(active.deadlineTimer);
    clearTimeout(active.killTimer);
    children.delete(active.claim.jobId);
    heartbeatAt = nowMono();
    wakeWaiters();
    return true;
  }

  function spawnClaim(claim) {
    // The absolute monotonic deadline starts before spawn, so runner startup does
    // not receive a fresh maxJobRuntimeMs window. The parent timer remains the
    // outer enforcement boundary for a child that stops responding.
    const runtimeDeadlineNs = process.hrtime.bigint()
      + BigInt(config.maxJobRuntimeMs) * 1_000_000n;
    const argv = [
      RUNNER_PATH,
      databasePath,
      claim.jobId,
      ownerId,
      String(claim.generation),
      String(claim.fence),
      String(config.busyTimeoutMs),
      String(config.maxStoreBytes),
      String(config.maxWalBytes),
      String(config.minFreeDiskBytes),
      runtimeDeadlineNs.toString()
    ];
    if (argv.reduce((total, item) => total + Buffer.byteLength(item, 'utf8'), 0) > 4096) {
      lastSafeErrorCode = 'OPS_CHILD_ARGUMENTS_TOO_LARGE';
      store._operationsFinish(OPERATIONS_STORAGE_CAPABILITY, {
        jobId: claim.jobId, ownerId, generation: claim.generation, fence: claim.fence,
        outcome: 'stopped'
      });
      return;
    }
    let capture = null;
    if (diagnosticSession !== undefined) {
      capture = prepareOperationsChildCapture(diagnosticSession);
      if (capture) argv.push(OPS07_CAPTURE_FLAG);
    }
    let child;
    try {
      child = spawn(process.execPath, argv, {
        cwd: dirname(RUNNER_PATH),
        env: { PATH: process.env.PATH ?? '', NODE_NO_WARNINGS: '1' },
        stdio: capture ? ['ignore', 'ignore', 'ignore', capture.fd] : 'ignore',
        windowsHide: true
      });
    } catch {
      if (capture) closeOperationsChildCapture(diagnosticSession, capture.slot);
      if (capture) recordOperationsSupervisor(diagnosticSession, capture.slot, 'spawn_error', {
        code: 'OTHER', generation: claim.generation, fence: claim.fence
      });
      lastSafeErrorCode = 'OPS_CHILD_START_FAILED';
      store._operationsFinish(OPERATIONS_STORAGE_CAPABILITY, {
        jobId: claim.jobId, ownerId, generation: claim.generation, fence: claim.fence,
        outcome: 'stopped'
      });
      return;
    }
    if (capture) {
      closeOperationsChildCapture(diagnosticSession, capture.slot);
      recordOperationsSupervisor(diagnosticSession, capture.slot, 'spawn', {
        pidPresent: Number.isSafeInteger(child.pid) && child.pid >= 2,
        generation: claim.generation, fence: claim.fence
      });
    }
    const active = {
      child, claim, startedAt: nowMono(), reaped: false, settled: false,
      finishPending: false, finishRetryable: false, finishErrorCode: null, outcome: null,
      deadlineExceeded: false, deadlineTimer: null, killTimer: null,
      captureSlot: capture?.slot ?? null
    };
    children.set(claim.jobId, active);
    active.deadlineTimer = setTimeout(() => {
      if (active.settled || lifecycle === 'stopping') return;
      active.deadlineExceeded = true;
      if (diagnosticSession !== undefined && active.captureSlot !== null) {
        recordOperationsSupervisor(diagnosticSession, active.captureSlot, 'deadline', {
          generation: active.claim.generation, fence: active.claim.fence
        });
      }
      lastSafeErrorCode = 'OPS_RUNNER_DEADLINE';
      try { child.kill('SIGTERM'); } catch { lastSafeErrorCode = 'OPS_CHILD_SIGNAL_FAILED'; }
      active.killTimer = setTimeout(() => {
        if (active.settled) return;
        try { child.kill('SIGKILL'); } catch { lastSafeErrorCode = 'OPS_CHILD_KILL_FAILED'; }
      }, config.childKillGraceMs);
    }, config.maxJobRuntimeMs);
    try {
      if (!Number.isSafeInteger(child.pid) || child.pid < 2) throw new Error('child pid unavailable');
      store._operationsSetRunnerPid(OPERATIONS_STORAGE_CAPABILITY, {
        jobId: claim.jobId,
        ownerId,
        generation: claim.generation,
        fence: claim.fence,
        runnerPid: child.pid
      });
    } catch {
      lastSafeErrorCode = 'OPS_CHILD_IDENTITY_PERSIST_FAILED';
      if (diagnosticSession !== undefined && active.captureSlot !== null) {
        recordOperationsSupervisor(diagnosticSession, active.captureSlot, 'runner_identity_failure', {
          code: 'OTHER', generation: active.claim.generation, fence: active.claim.fence
        });
      }
      try { child.kill('SIGTERM'); } catch { lastSafeErrorCode = 'OPS_CHILD_SIGNAL_FAILED'; }
    }
    child.once('error', () => {
      lastSafeErrorCode = 'OPS_CHILD_PROCESS_ERROR';
      if (diagnosticSession !== undefined && active.captureSlot !== null) {
        recordOperationsSupervisor(diagnosticSession, active.captureSlot, 'child_error', {
          code: 'OTHER', generation: active.claim.generation, fence: active.claim.fence
        });
      }
    });
    child.once('close', (code, signal) => {
      if (active.reaped) return;
      active.reaped = true;
      clearTimeout(active.deadlineTimer);
      clearTimeout(active.killTimer);
      active.outcome = active.deadlineExceeded ? 'stopped'
        : code === 0 ? 'completed'
          : (lifecycle === 'stopping' || signal === 'SIGTERM' || signal === 'SIGKILL') ? 'shutdown'
          : 'stopped';
      if (diagnosticSession !== undefined && active.captureSlot !== null) {
        recordOperationsSupervisor(diagnosticSession, active.captureSlot, 'child_exit', {
          exitCode: Number.isSafeInteger(code) ? code : null,
          signal: typeof signal === 'string' ? signal : null,
        childRuntimeMs: Math.max(0, Math.ceil(nowMono() - active.startedAt)),
          generation: active.claim.generation, fence: active.claim.fence
        });
      }
      finishOwnedOutcome(active);
      wakeWaiters();
    });
  }

  async function acquisitionLoop() {
    while (lifecycle === 'accepting' || lifecycle === 'saturated') {
      heartbeatAt = nowMono();
      for (const active of [...children.values()]) {
        if (active.reaped && active.finishPending && active.finishRetryable) finishOwnedOutcome(active);
      }
      if (children.size >= config.workerCount) {
        lifecycle = 'saturated';
        await delay(config.pollIntervalMs);
        if (lifecycle === 'saturated' && children.size < config.workerCount) lifecycle = 'accepting';
        continue;
      }
      try {
        const stale = store._operationsExpiredClaims(OPERATIONS_STORAGE_CAPABILITY, { policy });
        const recoverableJobIds = [];
        let priorRunnerAlive = false;
        for (const item of stale.items) {
          if (item.runnerPid === null || !processIsAlive(item.runnerPid)) recoverableJobIds.push(item.jobId);
          else priorRunnerAlive = true;
        }
        if (stale.incomplete) lastSafeErrorCode = 'OPS_STALE_CLAIM_SCAN_INCOMPLETE';
        else if (priorRunnerAlive) lastSafeErrorCode = 'OPS_STALE_RUNNER_STILL_ACTIVE';
        else if (['OPS_STALE_CLAIM_SCAN_INCOMPLETE', 'OPS_STALE_RUNNER_STILL_ACTIVE'].includes(lastSafeErrorCode)) {
          lastSafeErrorCode = null;
        }
        const selected = store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
          ownerId,
          policy,
          recoverableJobIds
        });
        const claim = selected?.claim ?? null;
        if (claim) {
          spawnClaim(claim);
          continue;
        }
        if (selected?.incomplete) lastSafeErrorCode = 'OPS_SCHEDULER_SCAN_INCOMPLETE';
        else if (lastSafeErrorCode === 'OPS_SCHEDULER_SCAN_INCOMPLETE') lastSafeErrorCode = null;
      } catch (error) {
        lastSafeErrorCode = typeof error?.code === 'string' && /^[A-Z0-9_]{1,64}$/u.test(error.code)
          ? error.code : 'OPS_STORE_UNAVAILABLE';
      }
      await delay(config.pollIntervalMs);
    }
  }

  function start() {
    if (lifecycle === 'accepting' || lifecycle === 'saturated') return loopPromise;
    if (lifecycle !== 'starting' || acquiredLoop) throw new TypeError('The operations worker cannot be restarted after stop.');
    acquiredLoop = true;
    lifecycle = 'accepting';
    loopPromise = acquisitionLoop().catch(error => {
      lastSafeErrorCode = typeof error?.code === 'string' && /^[A-Z0-9_]{1,64}$/u.test(error.code)
        ? error.code : 'OPS_WORKER_LOOP_FAILED';
      lifecycle = 'degraded';
    });
    return loopPromise;
  }

  function submitSyntheticWork(inputWork) {
    if (lifecycle !== 'accepting' && lifecycle !== 'saturated') {
      return Object.freeze({ accepted: false, code: 'OPS_UNAVAILABLE', reasonCode: 'WORKER_NOT_ACCEPTING', retryAfterMs: config.pollIntervalMs });
    }
    const request = safeOwn(inputWork,
      ['trustedSyntheticScope', 'idempotencyKey', 'planJson', 'limitsJson'],
      ['trustedSyntheticScope', 'idempotencyKey', 'planJson']);
    const scope = normalizeScope(request.trustedSyntheticScope);
    if (!safeId(request.idempotencyKey, 256)) throw new TypeError('The work identity is invalid.');
    const validated = validateLocalPlanWork({
      planJson: request.planJson,
      limitsJson: request.limitsJson ?? '{}'
    }, 'ops-validation-workflow', config);
    let result;
    try {
      result = store._operationsAdmit(OPERATIONS_STORAGE_CAPABILITY, {
        tenantId: scope.tenantId,
        projectId: scope.projectId,
        idempotencyKey: request.idempotencyKey,
        workJson: validated.workJson,
        policy
      });
    } catch (error) {
      if (error?.code === 'STORE_BUSY' || error?.code === 'STORE_CLOSED' || error?.code === 'STORE_WRITE_FAILED') {
        return Object.freeze({
          accepted: false,
          code: 'OPS_BACKPRESSURE',
          reasonCode: error.code === 'STORE_BUSY' ? 'STORE_BUSY' : 'STORE_UNAVAILABLE',
          retryAfterMs: error.code === 'STORE_BUSY' ? config.busyTimeoutMs * 2 : 500
        });
      }
      throw error;
    }
    if (result.accepted && !result.duplicate) wakeWaiters();
    return result;
  }

  function cancelSyntheticWork(inputCancel) {
    const request = safeOwn(inputCancel,
      ['trustedSyntheticScope', 'jobId', 'expectedGeneration'],
      ['trustedSyntheticScope', 'jobId']);
    const scope = normalizeScope(request.trustedSyntheticScope);
    if (!safeId(request.jobId, 256)) throw new TypeError('The work identity is invalid.');
    const current = store._operationsGetScopedJob(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: scope.tenantId,
      projectId: scope.projectId,
      jobId: request.jobId
    });
    if (!current) return Object.freeze({ accepted: false, state: 'not_found' });
    const result = store._operationsCancel(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: scope.tenantId,
      projectId: scope.projectId,
      jobId: request.jobId,
      expectedGeneration: request.expectedGeneration ?? current.generation
    });
    if (result.ownerId) {
      const active = children.get(request.jobId);
      if (active && active.claim.generation === result.generation && active.claim.fence === result.fence) {
        try { active.child.kill('SIGTERM'); } catch { lastSafeErrorCode = 'OPS_CHILD_SIGNAL_FAILED'; }
      }
    }
    wakeWaiters();
    return result;
  }

  function readScopedWorkPage(inputPage) {
    const request = safeOwn(inputPage, ['trustedSyntheticScope', 'afterJobId', 'limit'], ['trustedSyntheticScope']);
    const scope = normalizeScope(request.trustedSyntheticScope);
    return store._operationsReadPage(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId: scope.tenantId,
      projectId: scope.projectId,
      ...(request.afterJobId === undefined ? {} : { afterJobId: request.afterJobId }),
      limit: request.limit ?? 25
    });
  }

  function health() {
    const age = Math.max(0, Math.floor(nowMono() - heartbeatAt));
    let base;
    try {
      const telemetryDiagnostics = telemetry ? telemetry.diagnostics() : null;
      base = readOperationsHealth({
        store,
        config,
        lifecycle,
        heartbeatAgeMs: age,
        telemetrySinkState: telemetryDiagnostics?.state ?? 'not_configured',
        telemetrySinkPendingRecords: telemetryDiagnostics?.pendingRecords ?? 0,
        telemetrySinkFailed: Boolean(telemetryDiagnostics?.lastErrorCode)
      });
    } catch (error) {
      const code = typeof error?.code === 'string' && /^[A-Z0-9_]{1,64}$/u.test(error.code)
        ? error.code : 'OPS_HEALTH_UNAVAILABLE';
      return Object.freeze({
        schema: 'agent-harness.operations-health.v1',
        state: 'degraded', live: lifecycle !== 'stopped', ready: false, localReady: false,
        externalReady: false, externalDispatchEnabled: false,
        degradedReasons: Object.freeze([code]),
        process: Object.freeze({
          childCount: ownedChildCount(), pendingDurableCleanupCount: pendingDurableCleanupCount(),
          workerCount: config.workerCount, waiterCount: waiters.size, rssBytes: process.memoryUsage().rss
        }),
        lastSafeErrorCode: code
      });
    }
    return Object.freeze({
      ...base,
      process: Object.freeze({
        childCount: ownedChildCount(),
        pendingDurableCleanupCount: pendingDurableCleanupCount(),
        workerCount: config.workerCount,
        waiterCount: waiters.size,
        rssBytes: process.memoryUsage().rss,
        lastSafeErrorCode
      })
    });
  }

  function requestRetention(inputRetention) {
    return requestOwnerRetention(store, inputRetention);
  }

  function waitForOwnedChildrenIdle(timeoutMs = 5_000) {
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 30_000) {
      throw new TypeError('The idle wait must be a finite bounded duration.');
    }
    if (ownedChildCount() === 0) return Promise.resolve(true);
    if (waiters.size >= Math.min(config.maxWaiters, MAX_WAITERS)) {
      return Promise.resolve(false);
    }
    return new Promise(resolvePromise => {
      const waiter = { resolve: resolvePromise, timer: null };
      waiter.timer = setTimeout(() => {
        waiters.delete(waiter);
        resolvePromise(false);
      }, timeoutMs);
      waiters.add(waiter);
    });
  }

  function stop(reason = 'requested') {
    if (stopPromise) return stopPromise;
    if (!['requested', 'signal', 'test'].includes(reason)) throw new TypeError('The stop reason is invalid.');
    // This synchronous state change is the acquisition barrier for repeated or reentrant stop.
    lifecycle = 'stopping';
    const pendingStop = (async () => {
      const shutdownStartedAt = nowMono();
      const deadline = shutdownStartedAt + config.shutdownTimeoutMs;
      let escalatedChildren = 0;
      for (const waiter of waiters) {
        clearTimeout(waiter.timer);
        waiter.resolve(false);
      }
      waiters.clear();
      let remainingChildren = [...children.values()];
      for (const active of remainingChildren) {
        clearTimeout(active.deadlineTimer);
        clearTimeout(active.killTimer);
        if (!active.reaped) {
          try { active.child.kill('SIGTERM'); } catch { lastSafeErrorCode = 'OPS_CHILD_SIGNAL_FAILED'; }
        }
      }
      remainingChildren = remainingChildren.filter(active => !active.reaped);
      const gracefulBudget = Math.min(
        config.childKillGraceMs,
        Math.max(0, remaining(deadline) - config.childReapReserveMs)
      );
      if (remainingChildren.length > 0 && gracefulBudget > 0) {
        await raceWithDelay(
          Promise.all(remainingChildren.map(active => waitForEvent(active.child, 'close', gracefulBudget))),
          gracefulBudget,
          null
        );
      }
      remainingChildren = [...children.values()];
      for (const active of remainingChildren) {
        if (active.reaped) continue;
        active.escalated = true;
        escalatedChildren += 1;
        try { active.child.kill('SIGKILL'); } catch { lastSafeErrorCode = 'OPS_CHILD_KILL_FAILED'; }
      }
      const reapBudget = Math.max(0, remaining(deadline) - config.childReapReserveMs);
      remainingChildren = remainingChildren.filter(active => !active.reaped);
      if (remainingChildren.length > 0 && reapBudget > 0) {
        await raceWithDelay(
          Promise.all(remainingChildren.map(active => waitForEvent(active.child, 'close', reapBudget))),
          reapBudget,
          null
        );
      }
      const childrenReaped = ownedChildCount() === 0;
      if (!childrenReaped) lastSafeErrorCode = 'OPS_CHILD_REAP_INCOMPLETE';

      while (childrenReaped && pendingDurableCleanupCount() > 0 && remaining(deadline) > 0) {
        for (const active of [...children.values()]) {
          if (active.reaped && !active.settled) finishOwnedOutcome(active);
        }
        if (pendingDurableCleanupCount() > 0 && remaining(deadline) > 0) {
          await delay(Math.min(config.pollIntervalMs, remaining(deadline)));
        }
      }
      const durableCleanupComplete = children.size === 0;
      if (!durableCleanupComplete && lastSafeErrorCode === null) {
        lastSafeErrorCode = 'OPS_DURABLE_CLEANUP_INCOMPLETE';
      }

      let telemetryClosed = telemetry === undefined;
      if (telemetry && childrenReaped && durableCleanupComplete && remaining(deadline) > 0) {
        const close = Promise.resolve().then(() => telemetry.close()).then(() => true, () => false);
        telemetryClosed = await raceWithDelay(close, remaining(deadline), false);
        if (!telemetryClosed) lastSafeErrorCode = 'OPS_TELEMETRY_CLOSE_INCOMPLETE';
      }

      let storeClosed = false;
      if (childrenReaped && durableCleanupComplete && telemetryClosed) {
        try {
          store.close();
          storeClosed = true;
        } catch {
          lastSafeErrorCode = 'OPS_STORE_CLOSE_FAILED';
        }
      }
      lifecycle = storeClosed ? 'stopped' : 'degraded';
      wakeWaiters();
      return Object.freeze({
        stopped: storeClosed && childrenReaped && durableCleanupComplete && telemetryClosed,
        state: lifecycle,
        childrenReaped,
        durableCleanupComplete,
        pendingDurableCleanupCount: pendingDurableCleanupCount(),
        childCount: ownedChildCount(),
        telemetryClosed,
        storeClosed,
        elapsedMs: Math.max(0, Math.ceil(nowMono() - shutdownStartedAt)),
        budgetMs: config.shutdownTimeoutMs,
        escalatedChildren,
        lastSafeErrorCode
      });
    })();
    stopPromise = pendingStop;
    pendingStop.then(result => {
      if (result.stopped) removeSignalHandlers();
      else if (stopPromise === pendingStop) stopPromise = null;
    }, () => {
      if (stopPromise === pendingStop) stopPromise = null;
    });
    return stopPromise;
  }

  function installSignalHandlers(target = process) {
    if (utilTypes.isProxy(target) || !target || typeof target.on !== 'function' || typeof target.off !== 'function') {
      throw new TypeError('A native process-like signal target is required.');
    }
    const onTerm = () => { void stop('signal'); };
    const onInt = () => { void stop('signal'); };
    target.on('SIGTERM', onTerm);
    target.on('SIGINT', onInt);
    let removed = false;
    const remove = () => {
      if (removed) return;
      removed = true;
      target.off('SIGTERM', onTerm);
      target.off('SIGINT', onInt);
      signalHandlerRemovers.delete(remove);
    };
    signalHandlerRemovers.add(remove);
    return remove;
  }

  return Object.freeze({
    start,
    stop,
    submitSyntheticWork,
    cancelSyntheticWork,
    readScopedWorkPage,
    requestRetention,
    health,
    // This measures only children owned by this supervisor; queued durable work can remain.
    waitForOwnedChildrenIdle,
    installSignalHandlers,
    get lifecycle() { return lifecycle; },
    get activeChildCount() { return ownedChildCount(); },
    get databasePath() { return databasePath; }
  });
}

function parseCli(args) {
  if (args.length !== 4 || args[0] !== '--mode' || args[1] !== 'mock' || args[2] !== '--database-path'
      || args[3].length === 0 || args[3].length > 1024) return null;
  return { mode: 'mock', databasePath: args[3] };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const cli = parseCli(process.argv.slice(2));
  if (!cli) {
    process.stderr.write('Usage: node src/worker/private/worker.js --mode mock --database-path PATH\n');
    process.exitCode = 2;
  } else {
    let worker;
    let signalWaiter = null;
    let pendingSignal = null;
    let keepAlive = null;
    const signalNames = ['SIGTERM', 'SIGINT'];
    const requestStop = signal => {
      if (signalWaiter) {
        const resolveSignal = signalWaiter;
        signalWaiter = null;
        resolveSignal(signal);
      } else {
        pendingSignal = signal;
      }
    };
    const waitForStopRequest = () => {
      if (pendingSignal) {
        const signal = pendingSignal;
        pendingSignal = null;
        return Promise.resolve(signal);
      }
      return new Promise(resolvePromise => { signalWaiter = resolvePromise; });
    };
    const removeCliSignals = () => {
      for (const signal of signalNames) process.off(signal, requestStop);
      if (keepAlive !== null) clearInterval(keepAlive);
      keepAlive = null;
    };
    try {
      worker = createOperationsWorker({ databasePath: cli.databasePath, config: { mode: 'mock' } });
      for (const signal of signalNames) process.on(signal, requestStop);
      keepAlive = setInterval(() => {}, 60_000);
      process.stdout.write(JSON.stringify({ event: 'operations_worker_started', mode: 'mock', schema: 6 }) + '\n');
      const loop = worker.start();
      let result;
      while (!result?.stopped) {
        await waitForStopRequest();
        result = await worker.stop('signal');
        if (!result.stopped) {
          process.exitCode = 2;
          process.stdout.write(JSON.stringify({ event: 'operations_worker_stop_incomplete', ...result }) + '\n');
        }
      }
      await loop;
      removeCliSignals();
      process.stdout.write(JSON.stringify({ event: 'operations_worker_stopped', ...result }) + '\n');
      process.exitCode = result.stopped ? 0 : 2;
    } catch {
      removeCliSignals();
      process.stderr.write('The local operations worker could not start safely.\n');
      process.exitCode = 2;
    }
  }
}
