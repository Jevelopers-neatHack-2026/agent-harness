import { types as utilTypes } from 'node:util';
import { DurableWorkflowStore } from '../../internal/durable-store.js';
import { LOCAL_CONTROLLER_STORAGE_CAPABILITY } from '../../internal/private-local-store-capability.js';
import { classifyRecovery } from './classify.js';

const INPUT_KEYS = new Set([
  'store', 'workflowId', 'afterAttemptId', 'workflowRevision', 'pageSize'
]);

function fail() {
  return Object.freeze({
    kind: 'recovery-inspection-invalid',
    reason: 'RECOVERY_INPUT_INVALID',
    actionable: false,
    attempts: Object.freeze([]),
    incomplete: true
  });
}

function ownDataRecord(value) {
  if (
    value === null
    || typeof value !== 'object'
    || Array.isArray(value)
    || utilTypes.isProxy(value)
  ) return null;
  let prototype;
  let keys;
  try {
    prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) return null;
    keys = Reflect.ownKeys(value);
  } catch {
    return null;
  }
  if (keys.some(key => typeof key !== 'string' || !INPUT_KEYS.has(key))) return null;
  const result = Object.create(null);
  for (const key of keys) {
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      return null;
    }
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) return null;
    result[key] = descriptor.value;
  }
  return result;
}

function projectWorkflow(workflow) {
  return {
    workflowId: workflow.workflowId,
    state: workflow.state,
    revision: workflow.revision,
    checkpointSequence: workflow.checkpointSequence,
    cancellationRequested: workflow.cancellationRequested
  };
}

function projectCheckpoint(checkpoint) {
  return {
    sequence: checkpoint.sequence,
    schemaVersion: checkpoint.schemaVersion,
    data: checkpoint.data,
    sha256: checkpoint.sha256
  };
}

/** Read-only, keyset-paginated inspection. Results never change workflow state. */
export function inspectWorkflowRecovery(input) {
  const fields = ownDataRecord(input);
  if (
    !fields
    || utilTypes.isProxy(fields.store)
    || !(fields.store instanceof DurableWorkflowStore)
    || typeof fields.workflowId !== 'string'
    || fields.workflowId.trim().length === 0
    || fields.workflowId.length > 512
    || (fields.afterAttemptId !== undefined && typeof fields.afterAttemptId !== 'string')
    || (fields.workflowRevision !== undefined
      && (!Number.isSafeInteger(fields.workflowRevision) || fields.workflowRevision < 0))
    || (fields.pageSize !== undefined
      && (!Number.isSafeInteger(fields.pageSize) || fields.pageSize < 1 || fields.pageSize > 100))
    || (fields.afterAttemptId !== undefined && fields.workflowRevision === undefined)
  ) return fail();

  try {
    const snapshot = fields.store._localControllerSnapshot(
      LOCAL_CONTROLLER_STORAGE_CAPABILITY,
      fields.workflowId
    );
    const page = fields.store._localControllerListAttempts(
      LOCAL_CONTROLLER_STORAGE_CAPABILITY,
      {
        workflowId: fields.workflowId,
        afterAttemptId: fields.afterAttemptId ?? null,
        expectedRevision: fields.workflowRevision ?? snapshot.workflow.revision,
        limit: fields.pageSize ?? 32
      }
    );
    const observedAtMs = fields.store._localControllerNow(LOCAL_CONTROLLER_STORAGE_CAPABILITY);
    let classification = classifyRecovery(JSON.stringify({
      before: projectWorkflow(snapshot.workflow),
      after: projectWorkflow(snapshot.workflow),
      checkpoint: projectCheckpoint(snapshot.checkpoint),
      externalAttemptStates: snapshot.externalAttemptStates,
      observedAtMs
    }));
    if (
      page.incomplete
      && !['external-reconciliation-required', 'quarantine'].includes(classification.kind)
    ) {
      classification = Object.freeze({
        kind: 'stop',
        reason: 'RECOVERY_INSPECTION_INCOMPLETE',
        actionable: false
      });
    }
    return Object.freeze({
      kind: classification.kind,
      reason: classification.reason,
      actionable: false,
      workflowId: fields.workflowId,
      workflowRevision: page.workflowRevision,
      attempts: page.items,
      incomplete: page.incomplete,
      hasMore: page.hasMore,
      nextAfterAttemptId: page.nextAfterAttemptId,
      externalAttemptsTruncated: snapshot.externalAttemptsTruncated
    });
  } catch (error) {
    const code = typeof error?.code === 'string' ? error.code : 'RECOVERY_INSPECTION_FAILED';
    return Object.freeze({
      kind: code === 'OUTCOME_UNRESOLVED' ? 'external-reconciliation-required' : 'stop',
      reason: code === 'CONCURRENT_MODIFICATION' ? 'WORKFLOW_SNAPSHOT_CHANGED' : code,
      actionable: false,
      workflowId: fields.workflowId,
      workflowRevision: fields.workflowRevision ?? null,
      attempts: Object.freeze([]),
      incomplete: true,
      hasMore: null,
      nextAfterAttemptId: null,
      externalAttemptsTruncated: true
    });
  }
}
