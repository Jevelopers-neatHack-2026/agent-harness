import { DurableWorkflowStore } from '../../../src/internal/durable-store.js';
import { OPERATIONS_STORAGE_CAPABILITY } from '../../../src/internal/private-ops-store-capability.js';
import { operationsStoragePolicy, readOperationsConfig, validateLocalPlanWork } from '../../../src/worker/private/config.js';

const [databasePath, tenantId, projectId, idempotencyKey, planJson] = process.argv.slice(2);
if (![databasePath, tenantId, projectId, idempotencyKey, planJson].every(value => typeof value === 'string')) process.exit(2);
const config = readOperationsConfig();
const store = DurableWorkflowStore.open({ databasePath, busyTimeoutMs: config.busyTimeoutMs });
const work = validateLocalPlanWork({ planJson, limitsJson: '{}' }, 'ops-validation-workflow', config);
store._operationsAdmit(OPERATIONS_STORAGE_CAPABILITY, {
  tenantId,
  projectId,
  idempotencyKey,
  workJson: work.workJson,
  policy: operationsStoragePolicy(config)
});
// The durable transaction committed, but the parent never receives the acceptance response.
process.kill(process.pid, 'SIGKILL');
