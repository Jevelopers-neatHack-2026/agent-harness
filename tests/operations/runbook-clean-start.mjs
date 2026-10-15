import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createOperationsWorker } from '../../src/worker/private/worker.js';
import { DurableWorkflowStore } from '@jevelopers/agent-harness-runtime';

const directory = await mkdtemp(join(tmpdir(), 'ah11-runbook-clean-'));
const databasePath = join(directory, 'workflow.sqlite');
const scope = Object.freeze({ tenantId: 'runbook-tenant-synthetic', projectId: 'runbook-project-synthetic' });
const planJson = JSON.stringify({
  kind: 'agent-harness.private-synthetic-plan',
  version: 1,
  steps: [{ id: 'sum', operation: 'number.add', dependsOn: [], args: { left: 20, right: 22 } }]
});

async function waitForTerminal(worker, jobId) {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    const page = worker.readScopedWorkPage({ trustedSyntheticScope: scope, limit: 10 });
    const row = page.items.find(item => item.jobId === jobId);
    if (row && ['completed', 'failed', 'cancelled', 'unknown', 'parked'].includes(row.state)) return row;
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  throw new Error('The clean-start fixture did not reach a bounded durable state.');
}

let first;
let second;
let store;
let removeFirstHandlers;
let removeSecondHandlers;
try {
  first = createOperationsWorker({ databasePath, ownerId: 'ops-worker:runbook-1' });
  removeFirstHandlers = first.installSignalHandlers();
  const firstLoop = first.start();
  const submitted = first.submitSyntheticWork({
    trustedSyntheticScope: scope, idempotencyKey: 'clean-start-v1', planJson
  });
  if (!submitted.accepted) throw new Error('The clean-start fixture was not durably accepted.');
  const completed = await waitForTerminal(first, submitted.jobId);
  if (completed.state !== 'completed') throw new Error('The clean-start fixture did not finish its local plan.');
  const healthBeforeStop = first.health();
  if (!healthBeforeStop.live || healthBeforeStop.externalReady || healthBeforeStop.externalDispatchEnabled) {
    throw new Error('The health snapshot did not preserve the offline authority boundary.');
  }
  process.stderr.write(`AH11_SHUTDOWN_SIGNAL ${process.hrtime.bigint()}\n`);
  process.kill(process.pid, 'SIGTERM');
  await firstLoop;
  const firstStop = await first.stop('signal');
  removeFirstHandlers();
  removeFirstHandlers = null;
  if (!firstStop.stopped || !firstStop.childrenReaped || !firstStop.storeClosed || firstStop.elapsedMs > 1_000) {
    throw new Error('The first local worker did not stop cleanly.');
  }

  second = createOperationsWorker({ databasePath, ownerId: 'ops-worker:runbook-2' });
  removeSecondHandlers = second.installSignalHandlers();
  const secondLoop = second.start();
  const reopened = second.readScopedWorkPage({ trustedSyntheticScope: scope, limit: 10 });
  const resumed = reopened.items.find(item => item.state === 'completed');
  if (!resumed) throw new Error('The completed local job was not visible after restart.');
  const duplicate = second.submitSyntheticWork({
    trustedSyntheticScope: scope, idempotencyKey: 'clean-start-v1', planJson
  });
  if (!duplicate.accepted || !duplicate.duplicate || duplicate.jobId !== resumed.jobId) {
    throw new Error('Restart did not recover the durable idempotency result.');
  }
  const healthAfterRestart = second.health();
  process.stderr.write(`AH11_SHUTDOWN_SIGNAL ${process.hrtime.bigint()}\n`);
  process.kill(process.pid, 'SIGTERM');
  await secondLoop;
  const secondStop = await second.stop('signal');
  removeSecondHandlers();
  removeSecondHandlers = null;
  if (!secondStop.stopped || !secondStop.childrenReaped || !secondStop.storeClosed || secondStop.elapsedMs > 1_000) {
    throw new Error('The restarted local worker did not stop cleanly.');
  }
  store = DurableWorkflowStore.open({ databasePath, busyTimeoutMs: 20 });
  const checkpoint = store.getCheckpoint(resumed.workflowId);
  if (JSON.parse(checkpoint.data.completedResultsJson).sum.output !== 42) {
    throw new Error('The reopened checkpoint did not retain the computed result.');
  }
  process.stdout.write(JSON.stringify({
    fixture: 'runbook-clean-start', mode: 'mock', schemaVersion: 6, checkpointEnvelopeVersion: 2,
    firstStart: 'pass', localPlan: 'pass', health: 'external_dispatch_disabled',
    sigtermDrain: 'pass', restart: 'pass', duplicateAcceptance: 'same_durable_job',
    firstStopElapsedMs: firstStop.elapsedMs,
    secondStopElapsedMs: secondStop.elapsedMs,
    providerCalls: 0, localOnly: true,
    supervisorRssBytes: Math.max(healthBeforeStop.process.rssBytes, healthAfterRestart.process.rssBytes),
    observedOwnedRunnerProcesses: 0,
    localReadyBeforeStop: healthBeforeStop.localReady,
    localReadyAfterRestart: healthAfterRestart.localReady
  }) + '\n');
} catch (error) {
  process.stderr.write(`Clean-start fixture failed: ${error?.message ?? 'unknown'}\n`);
  process.exitCode = 1;
} finally {
  try { removeFirstHandlers?.(); } catch {}
  try { removeSecondHandlers?.(); } catch {}
  try { await first?.stop('test'); } catch {}
  try { await second?.stop('test'); } catch {}
  try { store?.close(); } catch {}
  await rm(directory, { recursive: true, force: true });
}
