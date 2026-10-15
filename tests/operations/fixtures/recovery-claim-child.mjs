import { existsSync, writeFileSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import { DurableWorkflowStore } from '../../../src/internal/index.js';
import { OPERATIONS_STORAGE_CAPABILITY } from '../../../src/internal/private-ops-store-capability.js';
import { operationsStoragePolicy, readOperationsConfig } from '../../../src/worker/private/config.js';
import { closedErrorProjection, encodeClosedDiagnostic } from '../closed-diagnostics.js';
import { publishFixtureRetryDiagnostic, runFixtureBusyRetry } from '../fixture-busy-retry.js';

const [databasePath, nowText, ownerId, recoverableJobId, readyPath, gatePath, configText] = process.argv.slice(2);
if (![databasePath, nowText, ownerId, recoverableJobId, readyPath, gatePath, configText]
  .every(value => typeof value === 'string' && value.length > 0)) process.exit(2);
let store;
let phase = 'config';
let primaryFailed = false;
let primaryError;
let cleanupFailed = false;
let cleanupError;
let nativeClose = 'unobserved';
let resultPresent = false;
try {
  const config = readOperationsConfig(JSON.parse(configText));
  const policy = operationsStoragePolicy(config);
  phase = 'open';
  store = DurableWorkflowStore.open({ databasePath, busyTimeoutMs: 100 });
  store._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY, {
    maxStoreBytes: config.maxStoreBytes, maxWalBytes: config.maxWalBytes, minFreeDiskBytes: config.minFreeDiskBytes
  });
  writeFileSync(readyPath, JSON.stringify({ processId: process.pid }));
  phase = 'gate';
  const deadline = Date.now() + 5_000;
  const deadlineNs = process.hrtime.bigint() + 5_000_000_000n;
  while (!existsSync(gatePath) && Date.now() < deadline) await delay(2);
  if (!existsSync(gatePath)) throw new Error('The recovery race gate was not released.');
  phase = 'claim';
  const attempted = await runFixtureBusyRetry({ kind: 'claim', deadlineNs,
    busyTimeoutMs: 100, delayMs: config.pollIntervalMs,
    invoke: () => store._operationsClaim(OPERATIONS_STORAGE_CAPABILITY, {
      ownerId, policy, recoverableJobIds: [recoverableJobId]
    })
  });
  publishFixtureRetryDiagnostic(attempted, line => process.stderr.write(line));
  if (attempted.failed) throw attempted.error;
  const outcome = attempted.value;
  phase = 'result';
  process.stdout.write(JSON.stringify({ ownerId,
    claim: outcome?.claim ? { jobId: outcome.claim.jobId, generation: outcome.claim.generation, fence: outcome.claim.fence } : null,
    incomplete: Boolean(outcome?.incomplete), ownedRunnerCount: 0
  }) + '\n');
  resultPresent = true;
} catch (error) {
  primaryFailed = true; primaryError = error; process.exitCode = 1;
} finally {
  if (store !== undefined) {
    try { store.close(); nativeClose = 'confirmed'; }
    catch (error) { cleanupFailed = true; cleanupError = error; nativeClose = 'failed'; process.exitCode = 1; }
  }
  const primary = primaryFailed ? closedErrorProjection(primaryError) : { errorKind: 'none', code: 'none', sqlitePrimaryCode: null };
  const cleanup = cleanupFailed ? closedErrorProjection(cleanupError) : { errorKind: 'none', code: 'none', sqlitePrimaryCode: null };
  const line = encodeClosedDiagnostic({ caseId: 'OPS05.C12', phase, primaryFailed, ...primary,
    cleanupFailed, cleanupKind: cleanup.errorKind, cleanupCode: cleanup.code,
    cleanupPrimaryCode: cleanup.sqlitePrimaryCode, nativeClose, resultPresent });
  try { if (line) process.stderr.write(line); } catch {}
}
