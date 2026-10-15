import { DurableWorkflowStore } from '../../internal/durable-store.js';
import { OPERATIONS_STORAGE_CAPABILITY } from '../../internal/private-ops-store-capability.js';
import { runBoundedLocalPlan } from '../../orchestrator/internal/controller.js';
import {
  enableOperationsRunnerCaptureFromFd3,
  finishOperationsRunnerCapture,
  OPS07_CAPTURE_FLAG,
  recordOperationsDiagnostic,
  recordOperationsDiagnosticError,
  recordOperationsDiagnosticResult,
  setOperationsDiagnosticPhase
} from '../../internal/private-ops-diagnostics.js';

function safeArgs(args) {
  if (args.length !== 10) return null;
  const [databasePath, jobId, ownerId, generationText, fenceText, busyTimeoutText,
    maxStoreBytesText, maxWalBytesText, minFreeDiskBytesText, runtimeDeadlineNsText] = args;
  const generation = Number(generationText);
  const fence = Number(fenceText);
  const busyTimeoutMs = Number(busyTimeoutText);
  const maxStoreBytes = Number(maxStoreBytesText);
  const maxWalBytes = Number(maxWalBytesText);
  const minFreeDiskBytes = Number(minFreeDiskBytesText);
  let runtimeDeadlineNs;
  try {
    if (typeof runtimeDeadlineNsText !== 'string' || !/^(?:0|[1-9][0-9]{0,29})$/u.test(runtimeDeadlineNsText)) return null;
    runtimeDeadlineNs = BigInt(runtimeDeadlineNsText);
  } catch {
    return null;
  }
  const remainingRuntimeNs = runtimeDeadlineNs - process.hrtime.bigint();
  if (typeof databasePath !== 'string' || databasePath.length === 0 || databasePath.length > 1024
      || !/^ops-job:[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(jobId)
      || !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(ownerId)
      || !Number.isSafeInteger(generation) || generation < 1
      || !Number.isSafeInteger(fence) || fence < 1
      || !Number.isSafeInteger(busyTimeoutMs) || busyTimeoutMs < 1 || busyTimeoutMs > 250
      || !Number.isSafeInteger(maxStoreBytes) || maxStoreBytes < 16_777_216 || maxStoreBytes > 1_073_741_824
      || !Number.isSafeInteger(maxWalBytes) || maxWalBytes < 1_048_576 || maxWalBytes > 134_217_728
      || !Number.isSafeInteger(minFreeDiskBytes) || minFreeDiskBytes < 1_048_576 || minFreeDiskBytes > 1_073_741_824
      || runtimeDeadlineNs < 1n || remainingRuntimeNs > 60_000_000_000n) return null;
  return { databasePath, jobId, ownerId, generation, fence, busyTimeoutMs,
    maxStoreBytes, maxWalBytes, minFreeDiskBytes, runtimeDeadlineNs };
}

const rawArgs = process.argv.slice(2);
const captureRequested = rawArgs.length === 11 && rawArgs[10] === OPS07_CAPTURE_FLAG;
const args = safeArgs(captureRequested ? rawArgs.slice(0, 10) : rawArgs);
const captureEnabled = captureRequested && enableOperationsRunnerCaptureFromFd3();
if (!args) {
  if (captureEnabled) {
    recordOperationsDiagnostic('runner_error', {
      phase: 'runner_start', errorKind: 'OTHER', code: 'OTHER', primaryCode: null
    });
    finishOperationsRunnerCapture();
  }
  process.exitCode = 2;
} else {
  let store;
  let phase = 'runner_start';
  if (captureEnabled) recordOperationsDiagnostic('runner_phase', { phase });
  try {
    store = DurableWorkflowStore.open({
      databasePath: args.databasePath,
      busyTimeoutMs: args.busyTimeoutMs
    });
    phase = 'store_open';
    setOperationsDiagnosticPhase(phase);
    recordOperationsDiagnostic('store_open', { ok: true });
    phase = 'resource_ceilings';
    setOperationsDiagnosticPhase(phase);
    store._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY, {
      maxStoreBytes: args.maxStoreBytes,
      maxWalBytes: args.maxWalBytes,
      minFreeDiskBytes: args.minFreeDiskBytes
    });
    recordOperationsDiagnostic('resource_ceilings', { ok: true });
    phase = 'claim_read';
    setOperationsDiagnosticPhase(phase);
    const claim = store._operationsReadClaim(OPERATIONS_STORAGE_CAPABILITY, {
      jobId: args.jobId,
      ownerId: args.ownerId,
      generation: args.generation,
      fence: args.fence
    });
    recordOperationsDiagnostic('claim_read', { ok: true });
    phase = 'controller';
    setOperationsDiagnosticPhase(phase);
    const result = await runBoundedLocalPlan({
      store,
      workflowId: claim.workflowId,
      ownerId: claim.ownerId,
      fence: claim.fence,
      busyTimeoutMs: args.busyTimeoutMs,
      runtimeDeadlineNs: args.runtimeDeadlineNs,
      planJson: claim.work.planJson,
      limitsJson: claim.work.limitsJson
    });
    recordOperationsDiagnosticResult(result);
    process.exitCode = result?.status === 'completed' ? 0 : 2;
  } catch (error) {
    recordOperationsDiagnosticError('runner_error', error, { phase });
    process.exitCode = 2;
  } finally {
    try {
      store?.close();
      if (store) recordOperationsDiagnostic('store_close', { ok: true });
    } catch (error) {
      recordOperationsDiagnosticError('runner_error', error, { phase: 'store_close' });
      recordOperationsDiagnostic('store_close', { ok: false });
      process.exitCode = 2;
    }
    if (captureEnabled) finishOperationsRunnerCapture();
  }
}
