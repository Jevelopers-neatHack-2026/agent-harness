import { types as utilTypes } from 'node:util';
import {
  NEATLOGS_COMPLETION_SPAN_NAME,
  NEATLOGS_LOCAL_SPAN_NAME,
  NEATLOGS_LOCAL_WORKFLOW_NAME,
  mapNeatlogsSourceRecord,
  sourceRecordFromNeatlogsAttributes
} from './neatlogs-sdk-mapper.js';

const TRACE_ID = /^[0-9a-f]{32}$/;
const SPAN_ID = /^[0-9a-f]{16}$/;
const ATTRIBUTE_KEYS = new Set([
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
  'agent_harness.telemetry.source_occurred_at_ms',
  'neatlogs.trace.complete'
]);
const ENVELOPE_KEYS = new Set([
  'trace_id', 'span_id', 'parent_span_id', 'name', 'kind',
  'start_time', 'end_time', 'duration_ns', 'attributes', 'resource',
  'status', 'links', 'events', 'instrumentation_scope'
]);
const RESOURCE_KEYS = new Set([
  'service.name',
  'service.version',
  'neatlogs.workflow_name',
  'telemetry.sdk.name',
  'telemetry.sdk.language',
  'telemetry.sdk.version'
]);
const SCOPE_KEYS = new Set(['name', 'version']);
const STATUS_KEYS = new Set(['code', 'description', 'message']);
const MAX_ENVELOPE_KEYS = 16;
const MAX_ATTRIBUTE_KEYS = 128;
const MAX_RESOURCE_KEYS = 32;
const MAX_TIMESTAMP_NS = 2.5e18;
const MAX_DURATION_NS = 60_000_000_000;
const MAX_FAILURES = 64;
const PINNED_NEATLOGS_VERSION = '1.1.28';
const PINNED_OTEL_SDK_VERSION = '1.30.1';

function isProxy(value) {
  try {
    return utilTypes.isProxy(value);
  } catch {
    return true;
  }
}

function ownPlainData(value, allowedKeys, requiredKeys = [], maximum = MAX_ENVELOPE_KEYS) {
  if (value === null || typeof value !== 'object' || Array.isArray(value) || isProxy(value)) {
    throw new TypeError();
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) throw new TypeError();
  const keys = Reflect.ownKeys(value);
  if (keys.length > maximum) throw new TypeError();
  const copy = Object.create(null);
  for (const key of keys) {
    if (typeof key !== 'string' || !allowedKeys.has(key)) throw new TypeError();
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor || !descriptor.enumerable || !Object.hasOwn(descriptor, 'value')) {
      throw new TypeError();
    }
    copy[key] = descriptor.value;
  }
  for (const key of requiredKeys) {
    if (!Object.hasOwn(copy, key)) throw new TypeError();
  }
  return copy;
}

function allowedAttributeData(value, allowedKeys, maximum = MAX_ATTRIBUTE_KEYS) {
  if (value === null || typeof value !== 'object' || Array.isArray(value) || isProxy(value)) {
    throw new TypeError();
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) throw new TypeError();
  const keys = Reflect.ownKeys(value);
  if (keys.length > maximum) throw new TypeError();
  const copy = Object.create(null);
  for (const key of keys) {
    if (typeof key !== 'string') throw new TypeError();
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor || !descriptor.enumerable || !Object.hasOwn(descriptor, 'value')) {
      throw new TypeError();
    }
    if (allowedKeys.has(key)) copy[key] = descriptor.value;
  }
  return copy;
}

function emptyArray(value) {
  if (!Array.isArray(value) || isProxy(value)) throw new TypeError();
  const length = Object.getOwnPropertyDescriptor(value, 'length')?.value;
  if (length !== 0 || Reflect.ownKeys(value).length !== 1) throw new TypeError();
  return [];
}

function finiteInteger(value, minimum, maximum) {
  return typeof value === 'number'
    && Number.isFinite(value)
    && Number.isInteger(value)
    && value >= minimum
    && value <= maximum;
}

function safeResource(value) {
  const resource = ownPlainData(value, new Set(['attributes']), ['attributes'], 1);
  const raw = allowedAttributeData(resource.attributes, RESOURCE_KEYS, MAX_RESOURCE_KEYS);
  const output = Object.create(null);
  if (Object.hasOwn(raw, 'service.name')) {
    if (raw['service.name'] !== NEATLOGS_LOCAL_WORKFLOW_NAME) throw new TypeError();
    output['service.name'] = NEATLOGS_LOCAL_WORKFLOW_NAME;
  }
  if (Object.hasOwn(raw, 'service.version')) {
    if (raw['service.version'] !== PINNED_NEATLOGS_VERSION) throw new TypeError();
    output['service.version'] = PINNED_NEATLOGS_VERSION;
  }
  if (Object.hasOwn(raw, 'neatlogs.workflow_name')) {
    if (raw['neatlogs.workflow_name'] !== NEATLOGS_LOCAL_WORKFLOW_NAME) throw new TypeError();
    output['neatlogs.workflow_name'] = NEATLOGS_LOCAL_WORKFLOW_NAME;
  }
  if (Object.hasOwn(raw, 'telemetry.sdk.name')) {
    if (raw['telemetry.sdk.name'] !== 'opentelemetry') throw new TypeError();
    output['telemetry.sdk.name'] = 'opentelemetry';
  }
  if (Object.hasOwn(raw, 'telemetry.sdk.language')) {
    if (raw['telemetry.sdk.language'] !== 'nodejs') throw new TypeError();
    output['telemetry.sdk.language'] = 'nodejs';
  }
  if (Object.hasOwn(raw, 'telemetry.sdk.version')) {
    if (raw['telemetry.sdk.version'] !== PINNED_OTEL_SDK_VERSION) throw new TypeError();
    output['telemetry.sdk.version'] = PINNED_OTEL_SDK_VERSION;
  }
  return Object.freeze(output);
}

function safeStatus(value) {
  const status = ownPlainData(value, STATUS_KEYS, ['code'], 3);
  const code = status.code;
  if (!(code === 'UNSET' || code === 'OK' || code === 0 || code === 1)) throw new TypeError();
  return Object.freeze({
    code: code === 0 ? 'UNSET' : code === 1 ? 'OK' : code,
    description: null
  });
}

function validateScope(value) {
  if (value === undefined) return;
  const scope = ownPlainData(value, SCOPE_KEYS, ['name'], 2);
  if (scope.name !== 'neatlogs.internal'
      || (Object.hasOwn(scope, 'version')
        && scope.version !== undefined
        && (typeof scope.version !== 'string' || scope.version.length > 32))) {
    throw new TypeError();
  }
}

function sanitizeSpanData(value) {
  const span = ownPlainData(value, ENVELOPE_KEYS, [
    'trace_id', 'span_id', 'parent_span_id', 'name', 'kind',
    'start_time', 'end_time', 'attributes', 'resource', 'status', 'events'
  ]);
  validateScope(span.instrumentation_scope);
  if (typeof span.trace_id !== 'string' || !TRACE_ID.test(span.trace_id)
      || typeof span.span_id !== 'string' || !SPAN_ID.test(span.span_id)
      || (span.parent_span_id !== null
        && (typeof span.parent_span_id !== 'string' || !SPAN_ID.test(span.parent_span_id)))
      || !finiteInteger(span.start_time, 1, MAX_TIMESTAMP_NS)
      || !finiteInteger(span.end_time, span.start_time, MAX_TIMESTAMP_NS)
      || (Object.hasOwn(span, 'duration_ns')
        && span.duration_ns !== null
        && !finiteInteger(span.duration_ns, 0, MAX_DURATION_NS))) {
    throw new TypeError();
  }
  const isCompletion = span.name === NEATLOGS_COMPLETION_SPAN_NAME;
  if (!isCompletion && span.name !== NEATLOGS_LOCAL_SPAN_NAME) throw new TypeError();
  if (isCompletion && span.instrumentation_scope !== undefined
      && span.instrumentation_scope.name !== 'neatlogs.internal') throw new TypeError();
  const attributes = allowedAttributeData(span.attributes, ATTRIBUTE_KEYS);
  let outputAttributes;
  let kind;
  if (isCompletion) {
    if (attributes['neatlogs.trace.complete'] !== true) throw new TypeError();
    if (!(span.kind === 0 || span.kind === 'Neatlogs.INTERNAL')) throw new TypeError();
    outputAttributes = Object.freeze(Object.assign(Object.create(null), {
      'neatlogs.trace.complete': true
    }));
    kind = 'Neatlogs.INTERNAL';
  } else {
    const source = sourceRecordFromNeatlogsAttributes(attributes);
    outputAttributes = mapNeatlogsSourceRecord(source);
    if (!(span.kind === 'WORKFLOW' || span.kind === 'workflow')) throw new TypeError();
    kind = 'workflow';
  }
  const duration = Object.hasOwn(span, 'duration_ns') && span.duration_ns !== null
    ? span.duration_ns
    : span.end_time - span.start_time;
  if (!finiteInteger(duration, 0, MAX_DURATION_NS)) throw new TypeError();
  const status = safeStatus(span.status);
  const events = Object.freeze(emptyArray(span.events));
  const links = Object.freeze(Object.hasOwn(span, 'links') ? emptyArray(span.links) : []);
  return Object.freeze({
    trace_id: span.trace_id,
    span_id: span.span_id,
    parent_span_id: span.parent_span_id,
    name: isCompletion ? NEATLOGS_COMPLETION_SPAN_NAME : NEATLOGS_LOCAL_SPAN_NAME,
    kind,
    start_time: span.start_time,
    end_time: span.end_time,
    duration_ns: duration,
    attributes: outputAttributes,
    resource: Object.freeze({ attributes: safeResource(span.resource) }),
    status,
    links,
    events
  });
}

export function createNeatlogsSdkMask() {
  let malformedDrops = 0;
  const mask = value => {
    try {
      return sanitizeSpanData(value);
    } catch {
      malformedDrops = Math.min(MAX_FAILURES, malformedDrops + 1);
      return null;
    }
  };
  return Object.freeze({
    mask,
    diagnostics() {
      return Object.freeze({ malformedDrops });
    }
  });
}

export const NEATLOGS_MASK_LIMITS = Object.freeze({
  envelopeKeys: MAX_ENVELOPE_KEYS,
  attributeKeys: MAX_ATTRIBUTE_KEYS,
  resourceKeys: MAX_RESOURCE_KEYS,
  failureCounter: MAX_FAILURES
});
