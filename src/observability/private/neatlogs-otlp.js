import { createHash } from 'node:crypto';
import {
  NEATLOGS_COMPLETION_SPAN_NAME,
  NEATLOGS_LOCAL_SPAN_NAME,
  mapNeatlogsSourceRecord,
  sourceRecordFromNeatlogsAttributes
} from './neatlogs-sdk-mapper.js';

export const NEATLOGS_LIVE_WORKFLOW_NAME = 'agent-harness.workflow-events';
const SDK_VERSION = '1.1.28';
const OTEL_SDK_VERSION = '1.30.1';
const ID_DOMAIN = 'agent-harness.neatlogs.ids.v1';
const SOURCE_FIELDS = Object.freeze([
  'schema', 'sequence', 'eventId', 'workflowId', 'revision', 'eventType',
  'fromState', 'toState', 'fence', 'occurredAtMs'
]);
const TAG = /^[a-z0-9][a-z0-9._:@-]{0,127}$/;
const MAX_TAGS = 10;
const MAX_REQUEST_BYTES = 256 * 1024;
const MAX_RESPONSE_BYTES = 16 * 1024;
const SPAN_KIND_INTERNAL = 1;
const MAX_GENERATION = 3;
const STATUS_UNSET = 0;
const STATUS_OK = 1;

export class NeatlogsOtlpError extends Error {
  constructor(code) {
    super(`Neatlogs OTLP encoding failed (${code}).`);
    this.name = 'NeatlogsOtlpError';
    this.code = code;
  }
}

function digestHex(...parts) {
  return createHash('sha256').update(parts.join('\0')).digest('hex');
}

/**
 * Trace and span IDs are derived from the projected (HMAC) event token and a
 * delivery generation. Retries within a generation resend the same IDs. The
 * receiver may not expose an ID during a finite readback window. Opt-in
 * recovery uses a later generation to make another trace ID, but a delayed
 * earlier trace can still appear and produce a duplicate event. Generation 0
 * keeps the original derivation.
 */
export function deterministicNeatlogsIds(eventToken, generation = 0) {
  if (!Number.isSafeInteger(generation) || generation < 0 || generation > MAX_GENERATION) {
    throw new NeatlogsOtlpError('INVALID_GENERATION');
  }
  const parts = generation === 0 ? [eventToken] : [eventToken, `g${generation}`];
  const traceId = digestHex(ID_DOMAIN, 'trace', ...parts).slice(0, 32);
  const rootSpanId = digestHex(ID_DOMAIN, 'root', ...parts).slice(0, 16);
  const markerSpanId = digestHex(ID_DOMAIN, 'marker', ...parts).slice(0, 16);
  if (/^0+$/.test(traceId) || /^0+$/.test(rootSpanId) || /^0+$/.test(markerSpanId)
      || rootSpanId === markerSpanId) {
    throw new NeatlogsOtlpError('INVALID_DERIVED_ID');
  }
  return Object.freeze({ traceId, rootSpanId, markerSpanId });
}

/**
 * Returns a plan containing only the named traces, moved to the next delivery
 * generation with fresh IDs. Earlier trace IDs are kept so readback can detect
 * an event stored under more than one generation.
 */
export function nextNeatlogsGeneration(plan, traceIds) {
  const wanted = new Set(traceIds);
  const traces = plan.traces.filter(trace => wanted.has(trace.traceId)).map(trace => {
    const generation = trace.generation + 1;
    return Object.freeze({
      ...trace,
      ...deterministicNeatlogsIds(trace.eventToken, generation),
      generation,
      priorTraceIds: Object.freeze([...trace.priorTraceIds, trace.traceId])
    });
  });
  if (traces.length !== wanted.size) throw new NeatlogsOtlpError('UNKNOWN_TRACE');
  return Object.freeze({ ...plan, traces: Object.freeze(traces) });
}

/** The only content exported as span input: the validated projected record. */
export function canonicalNeatlogsInput(record) {
  const ordered = {};
  for (const field of SOURCE_FIELDS) ordered[field] = record[field];
  return JSON.stringify(ordered);
}

export function validateNeatlogsTags(tags) {
  if (!Array.isArray(tags) || tags.length > MAX_TAGS) throw new NeatlogsOtlpError('INVALID_TAGS');
  const unique = new Set();
  for (const tag of tags) {
    if (typeof tag !== 'string' || !TAG.test(tag) || unique.has(tag)) {
      throw new NeatlogsOtlpError('INVALID_TAGS');
    }
    unique.add(tag);
  }
  return Object.freeze([...tags]);
}

/**
 * Converts a sanitized `captured_local` envelope into the exact set of traces
 * to export. Every root must carry a valid projected record and exactly one
 * completion marker; anything else is refused before any network use.
 */
export function planNeatlogsExport(envelope, { tags = [] } = {}) {
  const validatedTags = validateNeatlogsTags(tags);
  if (envelope === null || typeof envelope !== 'object' || !Array.isArray(envelope.spans)
      || envelope.remoteIngestionVerified !== false) {
    throw new NeatlogsOtlpError('INVALID_ENVELOPE');
  }
  const roots = envelope.spans.filter(span => span.name === NEATLOGS_LOCAL_SPAN_NAME);
  const markers = envelope.spans.filter(span => span.name === NEATLOGS_COMPLETION_SPAN_NAME);
  if (roots.length < 1 || roots.length !== markers.length
      || roots.length + markers.length !== envelope.spans.length
      || roots.length !== envelope.rootSpanCount) {
    throw new NeatlogsOtlpError('INVALID_ENVELOPE');
  }
  const seenTokens = new Set();
  const traces = [];
  for (const root of roots) {
    let record;
    try {
      record = sourceRecordFromNeatlogsAttributes(root.attributes);
    } catch {
      throw new NeatlogsOtlpError('INVALID_ENVELOPE');
    }
    const ownMarkers = markers.filter(marker => marker.trace_id === root.trace_id);
    if (ownMarkers.length !== 1 || seenTokens.has(record.eventId)) {
      throw new NeatlogsOtlpError('INVALID_ENVELOPE');
    }
    seenTokens.add(record.eventId);
    const ids = deterministicNeatlogsIds(record.eventId);
    traces.push(Object.freeze({
      ...ids,
      generation: 0,
      priorTraceIds: Object.freeze([]),
      sessionId: record.workflowId,
      eventToken: record.eventId,
      sequence: record.sequence,
      revision: record.revision,
      eventType: record.eventType,
      fence: record.fence,
      input: canonicalNeatlogsInput(record),
      record,
      root: Object.freeze({ start: root.start_time, end: root.end_time }),
      marker: Object.freeze({ start: ownMarkers[0].start_time, end: ownMarkers[0].end_time })
    }));
  }
  traces.sort((left, right) => left.sequence - right.sequence);
  return Object.freeze({
    workflowName: NEATLOGS_LIVE_WORKFLOW_NAME,
    tags: validatedTags,
    traces: Object.freeze(traces)
  });
}

// --- Minimal protobuf writer for opentelemetry.proto.collector.trace.v1 ---

class Writer {
  #chunks = [];
  #length = 0;

  bytes(value) {
    this.#chunks.push(value);
    this.#length += value.length;
  }

  varint(value) {
    let remaining = BigInt(value);
    if (remaining < 0n) throw new NeatlogsOtlpError('NEGATIVE_VARINT');
    const out = [];
    while (remaining > 0x7fn) {
      out.push(Number((remaining & 0x7fn) | 0x80n));
      remaining >>= 7n;
    }
    out.push(Number(remaining));
    this.bytes(Uint8Array.from(out));
  }

  tag(field, wireType) {
    this.varint((field << 3) | wireType);
  }

  lengthDelimited(field, payload) {
    this.tag(field, 2);
    this.varint(payload.length);
    this.bytes(payload);
  }

  string(field, value) {
    this.lengthDelimited(field, Buffer.from(value, 'utf8'));
  }

  message(field, build) {
    const nested = new Writer();
    build(nested);
    this.lengthDelimited(field, nested.finish());
  }

  fixed64(field, value) {
    const buffer = Buffer.alloc(8);
    buffer.writeBigUInt64LE(BigInt(value));
    this.tag(field, 1);
    this.bytes(buffer);
  }

  finish() {
    return Buffer.concat(this.#chunks, this.#length);
  }
}

function writeAttribute(writer, key, value) {
  writer.message(9, keyValue => {
    keyValue.string(1, key);
    keyValue.message(2, any => {
      if (typeof value === 'string') any.string(1, value);
      else if (typeof value === 'boolean') {
        any.tag(2, 0);
        any.varint(value ? 1 : 0);
      } else if (Number.isSafeInteger(value) && value >= 0) {
        any.tag(3, 0);
        any.varint(value);
      } else {
        throw new NeatlogsOtlpError('UNSUPPORTED_ATTRIBUTE');
      }
    });
  });
}

function writeKeyValues(writer, field, entries) {
  for (const [key, value] of entries) {
    writer.message(field, keyValue => {
      keyValue.string(1, key);
      keyValue.message(2, any => any.string(1, value));
    });
  }
}

function nanos(value) {
  if (!Number.isFinite(value) || value <= 0) throw new NeatlogsOtlpError('INVALID_TIME');
  // Capture times are float nanoseconds; keep microsecond precision so float
  // rounding noise is not exported as sub-microsecond digits.
  return BigInt(Math.round(value / 1000)) * 1000n;
}

function writeSpan(writer, { traceId, spanId, parentSpanId, name, start, end, attributes }) {
  writer.message(2, span => {
    span.lengthDelimited(1, Buffer.from(traceId, 'hex'));
    span.lengthDelimited(2, Buffer.from(spanId, 'hex'));
    if (parentSpanId) span.lengthDelimited(4, Buffer.from(parentSpanId, 'hex'));
    span.string(5, name);
    span.tag(6, 0);
    span.varint(SPAN_KIND_INTERNAL);
    span.fixed64(7, nanos(start));
    span.fixed64(8, nanos(Math.max(start, end)));
    for (const [key, value] of attributes) writeAttribute(span, key, value);
    span.message(15, status => {
      status.tag(3, 0);
      status.varint(name === NEATLOGS_COMPLETION_SPAN_NAME ? STATUS_UNSET : STATUS_OK);
    });
  });
}

function rootAttributes(plan, trace) {
  const entries = [
    ['neatlogs.span.kind', 'workflow'],
    ['neatlogs.workflow_name', plan.workflowName],
    ['neatlogs.instrumentation.name', 'neatlogs.trace'],
    ['neatlogs.session.id', trace.sessionId],
    ['neatlogs.workflow.input', trace.input]
  ];
  if (plan.tags.length > 0) entries.push(['neatlogs.tags', plan.tags.join(',')]);
  // Outbox traces carry attributes validated when they were enqueued
  // (neatlogs-outbox.js); captured traces are mapped from their record.
  const extra = trace.attributes ?? Object.entries(mapNeatlogsSourceRecord(trace.record));
  for (const [key, value] of extra) {
    entries.push([key, value]);
  }
  return entries;
}

function markerAttributes(plan, trace) {
  const entries = [
    ['neatlogs.trace.complete', true],
    ['neatlogs.internal', true],
    ['neatlogs.span.kind', 'Neatlogs.INTERNAL'],
    ['neatlogs.session.id', trace.sessionId]
  ];
  if (plan.tags.length > 0) entries.push(['neatlogs.tags', plan.tags.join(',')]);
  return entries;
}

/** Encodes an export plan as an OTLP ExportTraceServiceRequest protobuf. */
export function encodeNeatlogsExportRequest(plan) {
  const writer = new Writer();
  writer.message(1, resourceSpans => {
    resourceSpans.message(1, resource => {
      const resourceEntries = [
        ['service.name', plan.workflowName],
        ['service.version', SDK_VERSION],
        ['neatlogs.workflow_name', plan.workflowName],
        ['telemetry.sdk.name', 'opentelemetry'],
        ['telemetry.sdk.language', 'nodejs'],
        ['telemetry.sdk.version', OTEL_SDK_VERSION]
      ];
      if (plan.tags.length > 0) resourceEntries.push(['neatlogs.tags', plan.tags.join(',')]);
      writeKeyValues(resource, 1, resourceEntries);
    });
    resourceSpans.message(2, scopeSpans => {
      scopeSpans.message(1, scope => {
        scope.string(1, 'neatlogs');
        scope.string(2, SDK_VERSION);
      });
      for (const trace of plan.traces) {
        writeSpan(scopeSpans, {
          traceId: trace.traceId,
          spanId: trace.rootSpanId,
          parentSpanId: null,
          name: NEATLOGS_LOCAL_SPAN_NAME,
          start: trace.root.start,
          end: trace.root.end,
          attributes: rootAttributes(plan, trace)
        });
      }
    });
    resourceSpans.message(2, scopeSpans => {
      scopeSpans.message(1, scope => scope.string(1, 'neatlogs.internal'));
      for (const trace of plan.traces) {
        writeSpan(scopeSpans, {
          traceId: trace.traceId,
          spanId: trace.markerSpanId,
          parentSpanId: trace.rootSpanId,
          name: NEATLOGS_COMPLETION_SPAN_NAME,
          start: trace.marker.start,
          end: trace.marker.end,
          attributes: markerAttributes(plan, trace)
        });
      }
    });
  });
  const body = writer.finish();
  if (body.length > MAX_REQUEST_BYTES) throw new NeatlogsOtlpError('REQUEST_TOO_LARGE');
  return body;
}

// --- Minimal protobuf reader for ExportTraceServiceResponse ---

function readVarint(buffer, state) {
  let result = 0n;
  let shift = 0n;
  for (let index = 0; index < 10; index += 1) {
    if (state.offset >= buffer.length) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    const byte = buffer[state.offset++];
    // A uint64 varint's tenth byte may contain only bit zero and cannot
    // continue. Reject overlong encodings instead of silently truncating.
    if (index === 9 && (byte & 0xfe) !== 0) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    result |= BigInt(byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) return result;
    shift += 7n;
  }
  throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
}

function skipGroup(buffer, state, groupField) {
  const groups = [groupField];
  while (groups.length > 0) {
    const key = readVarint(buffer, state);
    if (key > 0xffff_ffffn) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    const field = Number(key >> 3n);
    const wireType = Number(key & 7n);
    if (field < 1 || field > 0x1fff_ffff) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    if (wireType === 4) {
      if (groups.pop() !== field) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    } else if (wireType === 3) {
      if (groups.length >= 64) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
      groups.push(field);
    } else if (wireType === 0) {
      readVarint(buffer, state);
    } else if (wireType === 1 || wireType === 5) {
      const size = wireType === 1 ? 8 : 4;
      if (state.offset + size > buffer.length) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
      state.offset += size;
    } else if (wireType === 2) {
      const rawLength = readVarint(buffer, state);
      if (rawLength > BigInt(buffer.length - state.offset)) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
      state.offset += Number(rawLength);
    } else {
      throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    }
  }
}

function readFields(buffer, visit) {
  const state = { offset: 0 };
  while (state.offset < buffer.length) {
    const key = readVarint(buffer, state);
    if (key > 0xffff_ffffn) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    const field = Number(key >> 3n);
    const wireType = Number(key & 7n);
    if (field < 1 || field > 0x1fff_ffff) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    if (wireType === 0) visit(field, wireType, readVarint(buffer, state));
    else if (wireType === 1 || wireType === 5) {
      const size = wireType === 1 ? 8 : 4;
      if (state.offset + size > buffer.length) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
      visit(field, wireType, buffer.subarray(state.offset, state.offset + size));
      state.offset += size;
    } else if (wireType === 2) {
      const rawLength = readVarint(buffer, state);
      if (rawLength > BigInt(buffer.length - state.offset)) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
      const length = Number(rawLength);
      visit(field, wireType, buffer.subarray(state.offset, state.offset + length));
      state.offset += length;
    } else if (wireType === 3) {
      skipGroup(buffer, state, field);
      visit(field, wireType, null);
    } else {
      throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    }
  }
}

function assertNoDuplicateJsonKeys(text) {
  let offset = 0;
  const whitespace = () => {
    while (offset < text.length && /\s/.test(text[offset])) offset += 1;
  };
  const string = () => {
    const start = offset;
    if (text[offset++] !== '"') throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    while (offset < text.length) {
      const character = text[offset++];
      if (character === '"') return JSON.parse(text.slice(start, offset));
      if (character === '\\') {
        if (offset >= text.length) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
        offset += 1;
      }
    }
    throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
  };
  const value = depth => {
    if (depth > 64) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    whitespace();
    const character = text[offset];
    if (character === '"') {
      string();
      return;
    }
    if (character === '{') {
      offset += 1;
      whitespace();
      const keys = new Set();
      if (text[offset] === '}') { offset += 1; return; }
      for (;;) {
        whitespace();
        const key = string();
        if (keys.has(key)) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
        keys.add(key);
        whitespace();
        if (text[offset++] !== ':') throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
        value(depth + 1);
        whitespace();
        const delimiter = text[offset++];
        if (delimiter === '}') return;
        if (delimiter !== ',') throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
      }
    }
    if (character === '[') {
      offset += 1;
      whitespace();
      if (text[offset] === ']') { offset += 1; return; }
      for (;;) {
        value(depth + 1);
        whitespace();
        const delimiter = text[offset++];
        if (delimiter === ']') return;
        if (delimiter !== ',') throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
      }
    }
    const scalar = /^(?:true|false|null|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/.exec(text.slice(offset));
    if (!scalar) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    offset += scalar[0].length;
  };
  value(0);
  whitespace();
  if (offset !== text.length) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
}

export function parseNeatlogsJson(text) {
  assertNoDuplicateJsonKeys(text);
  try {
    return JSON.parse(text);
  } catch {
    throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
  }
}

/**
 * Parses an OTLP trace export response. An empty body is a full success per
 * the OTLP/HTTP specification. Any rejected span count, or any receiver
 * message, is surfaced instead of being folded into success.
 */
export function decodeNeatlogsExportResponse(body, contentType = '') {
  const buffer = Buffer.from(body ?? []);
  if (buffer.length > MAX_RESPONSE_BYTES) throw new NeatlogsOtlpError('RESPONSE_TOO_LARGE');
  if (buffer.length === 0) return Object.freeze({ rejectedSpans: 0, receiverMessagePresent: false });
  const type = String(contentType).toLowerCase();
  if (type.includes('json')) {
    let parsed;
    try {
      parsed = parseNeatlogsJson(buffer.toString('utf8'));
    } catch {
      throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    }
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    }
    if (Object.hasOwn(parsed, 'partialSuccess') && Object.hasOwn(parsed, 'partial_success')) {
      throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    }
    const partial = Object.hasOwn(parsed, 'partialSuccess') ? parsed.partialSuccess
      : Object.hasOwn(parsed, 'partial_success') ? parsed.partial_success : null;
    if (partial === null) return Object.freeze({ rejectedSpans: 0, receiverMessagePresent: false });
    if (typeof partial !== 'object' || Array.isArray(partial)) {
      throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    }
    if (Object.hasOwn(partial, 'rejectedSpans') && Object.hasOwn(partial, 'rejected_spans')) {
      throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    }
    const rawRejected = Object.hasOwn(partial, 'rejectedSpans') ? partial.rejectedSpans
      : Object.hasOwn(partial, 'rejected_spans') ? partial.rejected_spans : 0;
    const rejected = typeof rawRejected === 'string' && /^\d+$/.test(rawRejected)
      ? Number(rawRejected) : rawRejected;
    if (!Number.isSafeInteger(rejected) || rejected < 0) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    if (Object.hasOwn(partial, 'errorMessage') && Object.hasOwn(partial, 'error_message')) {
      throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    }
    const message = Object.hasOwn(partial, 'errorMessage') ? partial.errorMessage
      : Object.hasOwn(partial, 'error_message') ? partial.error_message : '';
    if (typeof message !== 'string') throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    return Object.freeze({ rejectedSpans: rejected, receiverMessagePresent: message !== '' });
  }
  if (!type.includes('protobuf')) throw new NeatlogsOtlpError('UNEXPECTED_RESPONSE_TYPE');
  let rejectedSpans = 0n;
  let receiverMessagePresent = false;
  readFields(buffer, (field, wireType, value) => {
    if (field !== 1) return;
    if (wireType !== 2) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
    readFields(value, (innerField, innerWireType, innerValue) => {
      if (innerField === 1) {
        if (innerWireType !== 0) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
        rejectedSpans = innerValue;
      } else if (innerField === 2) {
        if (innerWireType !== 2) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
        if (innerValue.length > 0) receiverMessagePresent = true;
      }
    });
  });
  if (rejectedSpans > BigInt(Number.MAX_SAFE_INTEGER)) throw new NeatlogsOtlpError('MALFORMED_RESPONSE');
  return Object.freeze({ rejectedSpans: Number(rejectedSpans), receiverMessagePresent });
}

export const NEATLOGS_OTLP_LIMITS = Object.freeze({
  requestBytes: MAX_REQUEST_BYTES,
  responseBytes: MAX_RESPONSE_BYTES,
  tags: MAX_TAGS,
  generations: MAX_GENERATION
});
