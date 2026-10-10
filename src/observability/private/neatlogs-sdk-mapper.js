import { types as utilTypes } from 'node:util';

export const NEATLOGS_LOCAL_WORKFLOW_NAME = 'agent-harness-neatlogs-local-capture';
export const NEATLOGS_LOCAL_SPAN_NAME = 'agent_harness.workflow_event';
export const NEATLOGS_COMPLETION_SPAN_NAME = 'neatlogs.trace.complete';

const TELEMETRY_SCHEMA = 'agent-harness.telemetry.v1';
const SOURCE_FIELDS = Object.freeze([
  'schema', 'sequence', 'eventId', 'workflowId', 'revision', 'eventType',
  'fromState', 'toState', 'fence', 'occurredAtMs'
]);
const SOURCE_FIELD_SET = new Set(SOURCE_FIELDS);
const EVENT_TYPES = new Set([
  'workflow_created',
  'lease_acquired',
  'lease_renewed',
  'lease_released',
  'lease_released_with_unresolved_work',
  'lease_released_with_unknown_outcome',
  'expired_lease_outcome_unknown',
  'state_transition',
  'workflow_cancelled',
  'cancellation_requested_unresolved',
  'dispatch_intent_persisted',
  'transport_attempt_persisted',
  'response_receipt_persisted',
  'external_outcome_unknown',
  'response_checkpoint_reconciled',
  'schema_migrated_v1'
]);
const STATES = new Set([
  'queued', 'running', 'awaiting_response', 'reconciliation_pending',
  'review_required', 'succeeded', 'failed', 'cancelled'
]);
const TOKEN = /^tok1_[0-9a-f]{64}$/;
const MAX_SOURCE_RECORDS = 32;
const MAX_SOURCE_RECORD_BYTES = 1024;
const MAX_SOURCE_BATCH_BYTES = 64 * 1024;

export class NeatlogsSdkInputError extends Error {
  constructor(code) {
    super('Projected telemetry records are not valid for local SDK preparation.');
    this.name = 'NeatlogsSdkInputError';
    this.code = code;
  }
}

function reject(code = 'INVALID_RECORD') {
  throw new NeatlogsSdkInputError(code);
}

function isProxy(value) {
  try {
    return utilTypes.isProxy(value);
  } catch {
    return true;
  }
}

function ownDataRecord(value, allowedKeys, requiredKeys) {
  if (value === null || typeof value !== 'object' || Array.isArray(value) || isProxy(value)) {
    reject();
  }
  let prototype;
  let keys;
  try {
    prototype = Object.getPrototypeOf(value);
    keys = Reflect.ownKeys(value);
  } catch {
    reject();
  }
  if (prototype !== Object.prototype && prototype !== null) reject();
  if (keys.length > allowedKeys.size) reject();
  const copy = Object.create(null);
  for (const key of keys) {
    if (typeof key !== 'string' || !allowedKeys.has(key)) reject();
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      reject();
    }
    if (!descriptor || !descriptor.enumerable || !Object.hasOwn(descriptor, 'value')) reject();
    copy[key] = descriptor.value;
  }
  for (const key of requiredKeys) {
    if (!Object.hasOwn(copy, key)) reject();
  }
  return copy;
}

function integer(value, minimum = 0) {
  return Number.isSafeInteger(value) && value >= minimum;
}

function state(value) {
  return value === null || (typeof value === 'string' && STATES.has(value));
}

function validateRecord(value) {
  const source = ownDataRecord(value, SOURCE_FIELD_SET, SOURCE_FIELDS);
  if (Object.keys(source).length !== SOURCE_FIELDS.length
      || source.schema !== TELEMETRY_SCHEMA
      || !integer(source.sequence, 1)
      || typeof source.eventId !== 'string' || !TOKEN.test(source.eventId)
      || typeof source.workflowId !== 'string' || !TOKEN.test(source.workflowId)
      || !integer(source.revision)
      || typeof source.eventType !== 'string' || !EVENT_TYPES.has(source.eventType)
      || !state(source.fromState)
      || !state(source.toState)
      || !integer(source.fence)
      || !integer(source.occurredAtMs)) {
    reject();
  }
  const record = Object.freeze(Object.assign(Object.create(null), source));
  const bytes = Buffer.byteLength(JSON.stringify(record), 'utf8');
  if (bytes > MAX_SOURCE_RECORD_BYTES) reject('RECORD_TOO_LARGE');
  return { record, bytes };
}

function validateArray(value) {
  if (!Array.isArray(value) || isProxy(value)) reject();
  const lengthDescriptor = Object.getOwnPropertyDescriptor(value, 'length');
  if (!lengthDescriptor || !Object.hasOwn(lengthDescriptor, 'value')) reject();
  const length = lengthDescriptor.value;
  if (!Number.isSafeInteger(length) || length < 1 || length > MAX_SOURCE_RECORDS) {
    reject('RECORD_COUNT_LIMIT');
  }
  let keys;
  try {
    keys = Reflect.ownKeys(value);
  } catch {
    reject();
  }
  if (keys.length !== length + 1 || keys.some((key, index) => key !== 'length' && key !== String(index))) {
    reject();
  }
  const records = [];
  let totalBytes = 2 + Math.max(0, length - 1);
  let previousSequence = 0;
  for (let index = 0; index < length; index += 1) {
    const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
    if (!descriptor || !descriptor.enumerable || !Object.hasOwn(descriptor, 'value')) reject();
    const item = validateRecord(descriptor.value);
    if (item.record.sequence <= previousSequence) reject('RECORD_ORDER_INVALID');
    previousSequence = item.record.sequence;
    totalBytes += item.bytes;
    records.push(item.record);
  }
  if (totalBytes > MAX_SOURCE_BATCH_BYTES) reject('BATCH_TOO_LARGE');
  return Object.freeze(records);
}

export function validateNeatlogsSourceRecords(value) {
  return validateArray(value);
}

function addState(attributes, field, value) {
  if (value === null) {
    attributes[`agent_harness.telemetry.${field}_is_null`] = true;
  } else {
    attributes[`agent_harness.telemetry.${field}`] = value;
  }
}

export function mapNeatlogsSourceRecord(record) {
  const validated = validateRecord(record).record;
  const attributes = Object.create(null);
  attributes['agent_harness.telemetry.event_token'] = validated.eventId;
  attributes['agent_harness.telemetry.workflow_token'] = validated.workflowId;
  attributes['agent_harness.telemetry.sequence'] = validated.sequence;
  attributes['agent_harness.telemetry.revision'] = validated.revision;
  attributes['agent_harness.telemetry.event_type'] = validated.eventType;
  addState(attributes, 'from_state', validated.fromState);
  addState(attributes, 'to_state', validated.toState);
  attributes['agent_harness.telemetry.fence'] = validated.fence;
  attributes['agent_harness.telemetry.source_occurred_at_ms'] = validated.occurredAtMs;
  return Object.freeze(attributes);
}

export function sourceRecordFromNeatlogsAttributes(value) {
  const fields = ownDataRecord(value, new Set([
    'agent_harness.telemetry.event_token',
    'agent_harness.telemetry.workflow_token',
    'agent_harness.telemetry.sequence',
    'agent_harness.telemetry.revision',
    'agent_harness.telemetry.event_type',
    'agent_harness.telemetry.from_state',
    'agent_harness.telemetry.from_state_is_null',
    'agent_harness.telemetry.to_state',
    'agent_harness.telemetry.to_state_is_null',
    'agent_harness.telemetry.fence',
    'agent_harness.telemetry.source_occurred_at_ms'
  ]), [
    'agent_harness.telemetry.event_token',
    'agent_harness.telemetry.workflow_token',
    'agent_harness.telemetry.sequence',
    'agent_harness.telemetry.revision',
    'agent_harness.telemetry.event_type',
    'agent_harness.telemetry.fence',
    'agent_harness.telemetry.source_occurred_at_ms'
  ]);

  const readState = field => {
    const valueKey = `agent_harness.telemetry.${field}`;
    const nullKey = `${valueKey}_is_null`;
    const hasValue = Object.hasOwn(fields, valueKey);
    const isNull = Object.hasOwn(fields, nullKey);
    if (hasValue === isNull) reject('MAPPED_ATTRIBUTE_INVALID');
    if (isNull) {
      if (fields[nullKey] !== true) reject('MAPPED_ATTRIBUTE_INVALID');
      return null;
    }
    if (typeof fields[valueKey] !== 'string' || !STATES.has(fields[valueKey])) {
      reject('MAPPED_ATTRIBUTE_INVALID');
    }
    return fields[valueKey];
  };

  const record = Object.freeze(Object.assign(Object.create(null), {
    schema: TELEMETRY_SCHEMA,
    sequence: fields['agent_harness.telemetry.sequence'],
    eventId: fields['agent_harness.telemetry.event_token'],
    workflowId: fields['agent_harness.telemetry.workflow_token'],
    revision: fields['agent_harness.telemetry.revision'],
    eventType: fields['agent_harness.telemetry.event_type'],
    fromState: readState('from_state'),
    toState: readState('to_state'),
    fence: fields['agent_harness.telemetry.fence'],
    occurredAtMs: fields['agent_harness.telemetry.source_occurred_at_ms']
  }));
  return validateRecord(record).record;
}

export const NEATLOGS_SDK_MAPPING_LIMITS = Object.freeze({
  sourceRecords: MAX_SOURCE_RECORDS,
  sourceRecordBytes: MAX_SOURCE_RECORD_BYTES,
  sourceBatchBytes: MAX_SOURCE_BATCH_BYTES,
  rootSpans: MAX_SOURCE_RECORDS,
  completionMarkerSpans: MAX_SOURCE_RECORDS,
  totalSpans: MAX_SOURCE_RECORDS * 2
});
