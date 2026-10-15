import { writeFileSync, writeSync } from 'node:fs';
import { types as utilTypes } from 'node:util';
import { pathToFileURL } from 'node:url';
import {
  DurableWorkflowStore, readOperationsAdmissionBusyDiagnostic
} from '../../../src/internal/durable-store.js';
import { OPERATIONS_STORAGE_CAPABILITY } from '../../../src/internal/private-ops-store-capability.js';
import { operationsStoragePolicy, readOperationsConfig, validateLocalPlanWork } from '../../../src/worker/private/config.js';

const FAILURE_PREFIX = 'OPS_ADMISSION_FAILURE ';
const FAILURE_STAGES = new Set(['config', 'open', 'resource', 'validation', 'admission', 'result', 'close', 'unknown']);
const FAILURE_CODES = new Set([
  'OPS_CONFIG_INVALID', 'STORE_OPEN_FAILED', 'STORE_WRITE_FAILED', 'STORE_BUSY', 'STORE_CLOSED',
  'UNSUPPORTED_SCHEMA_VERSION', 'UNVERSIONED_DATA', 'INVALID_INPUT',
  'ERR_SQLITE_ERROR', 'ERR_INVALID_ARG_TYPE', 'EACCES', 'ENOENT', 'ENOSPC'
]);
const FAILURE_TRANSACTION_BOUNDARIES = new Set(['begin', 'body', 'commit', 'rollback']);
const FAILURE_ROLLBACK_DISPOSITIONS = new Set([
  'not_observed', 'inactive', 'unknown', 'rollback_attempted',
  'rolled_back', 'rollback_failed', 'rollback_check_failed'
]);

export function createAdmissionFailureDiagnostic(stage, error, transactionFailure = undefined) {
  const safeStage = typeof stage === 'string' && FAILURE_STAGES.has(stage) ? stage : 'unknown';
  let safeCode = 'UNKNOWN';
  if (error !== null && typeof error === 'object' && !utilTypes.isProxy(error)) {
    try {
      const descriptor = Object.getOwnPropertyDescriptor(error, 'code');
      if (descriptor && Object.hasOwn(descriptor, 'value')
          && typeof descriptor.value === 'string' && FAILURE_CODES.has(descriptor.value)) {
        safeCode = descriptor.value;
      }
    } catch {}
  }
  const transaction = transactionFailure === undefined
    ? readOperationsAdmissionBusyDiagnostic(error) : transactionFailure;
  if (safeStage === 'admission' && safeCode === 'STORE_BUSY'
      && transaction !== null && typeof transaction === 'object' && !Array.isArray(transaction)
      && !utilTypes.isProxy(transaction)) {
    try {
      const boundary = Object.getOwnPropertyDescriptor(transaction, 'transactionBoundary')?.value;
      const sqlitePrimaryCode = Object.getOwnPropertyDescriptor(transaction, 'sqlitePrimaryCode')?.value;
      const transactionActive = Object.getOwnPropertyDescriptor(transaction, 'transactionActive')?.value;
      const rollbackDisposition = Object.getOwnPropertyDescriptor(transaction, 'rollbackDisposition')?.value;
      const validDisposition = boundary === 'begin'
        ? transactionActive === null && rollbackDisposition === 'not_observed'
        : ['body', 'commit'].includes(boundary)
          ? (rollbackDisposition === 'inactive' && transactionActive === false)
            || (rollbackDisposition === 'rolled_back' && transactionActive === true)
            || (rollbackDisposition === 'unknown' && transactionActive === null)
          : boundary === 'rollback'
            ? (rollbackDisposition === 'rollback_failed' && transactionActive === true)
              || (rollbackDisposition === 'rollback_check_failed' && transactionActive === null)
            : false;
      if (FAILURE_TRANSACTION_BOUNDARIES.has(boundary)
          && (sqlitePrimaryCode === 5 || sqlitePrimaryCode === 6)
          && (transactionActive === null || typeof transactionActive === 'boolean')
          && FAILURE_ROLLBACK_DISPOSITIONS.has(rollbackDisposition) && validDisposition) {
        return Object.freeze({
          schema: 'ops-admission-failure.v2', stage: safeStage, code: safeCode,
          transactionBoundary: boundary, sqlitePrimaryCode, transactionActive, rollbackDisposition
        });
      }
    } catch {}
  }
  return Object.freeze({ schema: 'ops-admission-failure.v1', stage: safeStage, code: safeCode });
}

function emitAdmissionFailure(stage, error) {
  const record = createAdmissionFailureDiagnostic(stage, error);
  try { writeSync(2, `${FAILURE_PREFIX}${JSON.stringify(record)}\n`); } catch {}
}

function runAdmissionFixture() {
  const [databasePath, tenantId, projectId, idempotencyKey, planJson, configJson, outputPath] = process.argv.slice(2);
  if (![databasePath, tenantId, projectId, idempotencyKey, planJson, configJson, outputPath]
    .every(value => typeof value === 'string')) {
    emitAdmissionFailure('config', null);
    process.exitCode = 2;
    return;
  }

  let config;
  try {
    config = readOperationsConfig(JSON.parse(configJson));
  } catch (error) {
    emitAdmissionFailure('config', error);
    process.exitCode = 2;
    return;
  }

  let store;
  try {
    store = DurableWorkflowStore.open({ databasePath, busyTimeoutMs: config.busyTimeoutMs });
  } catch (error) {
    emitAdmissionFailure('open', error);
    process.exitCode = 1;
    return;
  }

  let stage = 'resource';
  let primaryFailure = null;
  try {
    store._operationsSetResourceCeilings(OPERATIONS_STORAGE_CAPABILITY, {
      maxStoreBytes: config.maxStoreBytes,
      maxWalBytes: config.maxWalBytes,
      minFreeDiskBytes: config.minFreeDiskBytes
    });
    stage = 'validation';
    const work = validateLocalPlanWork({ planJson, limitsJson: '{}' }, 'ops-validation-workflow', config);
    stage = 'admission';
    const result = store._operationsAdmit(OPERATIONS_STORAGE_CAPABILITY, {
      tenantId, projectId, idempotencyKey, workJson: work.workJson,
      policy: operationsStoragePolicy(config)
    });
    stage = 'result';
    writeFileSync(outputPath, JSON.stringify({ result }));
  } catch (error) {
    primaryFailure = error;
    emitAdmissionFailure(stage, error);
    process.exitCode = 1;
  }

  try {
    store.close();
  } catch (error) {
    emitAdmissionFailure('close', error);
    if (primaryFailure === null) process.exitCode = 1;
  }
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) runAdmissionFixture();
