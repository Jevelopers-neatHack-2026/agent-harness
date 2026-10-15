import { createHmac } from 'node:crypto';
import { types as utilTypes } from 'node:util';
import { runtimeError } from './errors.js';
import { DurableWorkflowStore } from './durable-store.js';

const CONSUMER_ID = 'internal-telemetry-v1';
const SOURCE_FIELDS = Object.freeze([
  'sequence', 'eventId', 'workflowId', 'revision', 'type',
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
  'schema_migrated_v1',
  'operations_work_admitted',
  'operations_work_claimed',
  'operations_work_unknown',
  'operations_work_requeued',
  'operations_work_parked',
  'operations_work_completed',
  'operations_work_failed',
  'operations_work_cancelled',
  'operations_cancel_requested'
]);
const STATES = new Set([
  'queued', 'running', 'awaiting_response', 'reconciliation_pending',
  'review_required', 'succeeded', 'failed', 'cancelled'
]);
const TOKEN_KINDS = new Set(['event', 'workflow']);
const MAX_PROJECTED_RECORD_BYTES = 1024;
const DEFAULT_LIMITS = Object.freeze({
  queueRecords: 32,
  queueBytes: 16 * 1024,
  batchSize: 16,
  maxAttempts: 2,
  retryDelayMs: 25,
  sinkTimeoutMs: 250,
  maxBatchesPerFlush: 4,
  maxPolicyDropsPerFlush: 16
});
const LIMIT_BOUNDS = Object.freeze({
  queueRecords: [1, 128],
  queueBytes: [MAX_PROJECTED_RECORD_BYTES + 2, 64 * 1024],
  batchSize: [1, 32],
  maxAttempts: [1, 3],
  retryDelayMs: [0, 100],
  sinkTimeoutMs: [10, 500],
  maxBatchesPerFlush: [1, 8],
  maxPolicyDropsPerFlush: [1, 32]
});
const MAX_SOURCE_STRING_UNITS = 2048;
const MAX_PENDING_DIAGNOSTIC_ROWS = 10_000;

class PolicyDrop extends Error {}
class RedactionFailure extends Error {}

function telemetryError(code, operation, safeMessage, retryable = false) {
  return runtimeError({
    module: 'telemetry', operation, code, retryable, safe_message: safeMessage
  });
}

function isProxy(value) {
  try {
    return utilTypes.isProxy(value);
  } catch {
    return true;
  }
}

function ownDataRecord(value, allowedKeys, requiredKeys = []) {
  if (value === null || typeof value !== 'object' || Array.isArray(value) || isProxy(value)) {
    throw new PolicyDrop();
  }
  let prototype;
  let keys;
  try {
    prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) throw new PolicyDrop();
    keys = Reflect.ownKeys(value);
  } catch {
    throw new PolicyDrop();
  }
  if (keys.length > allowedKeys.size) throw new PolicyDrop();
  const result = Object.create(null);
  for (const key of keys) {
    if (typeof key !== 'string' || !allowedKeys.has(key)) throw new PolicyDrop();
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      throw new PolicyDrop();
    }
    if (!descriptor || !descriptor.enumerable || !Object.hasOwn(descriptor, 'value')) {
      throw new PolicyDrop();
    }
    result[key] = descriptor.value;
  }
  for (const key of requiredKeys) {
    if (!Object.hasOwn(result, key)) throw new PolicyDrop();
  }
  return result;
}

function boundedSourceString(value) {
  if (typeof value !== 'string' || value.length === 0 || value.length > MAX_SOURCE_STRING_UNITS) {
    throw new PolicyDrop();
  }
  return value;
}

function boundedInteger(value, minimum = 0) {
  if (!Number.isSafeInteger(value) || value < minimum) throw new PolicyDrop();
  return value;
}

function safeState(value) {
  if (value === null) return null;
  if (typeof value !== 'string' || !STATES.has(value)) throw new PolicyDrop();
  return value;
}

function validateToken(token, sourceValue) {
  if (typeof token !== 'string' || !/^tok1_[0-9a-f]{64}$/.test(token)
      || sourceValue.includes(token)) {
    throw new RedactionFailure();
  }
  return token;
}

function resolveTokenizer(value) {
  if (typeof value === 'function' && !isProxy(value)) return value;
  const fields = ownDataRecord(value, new Set(['tokenize', 'destroy']), ['tokenize']);
  if (typeof fields.tokenize !== 'function' || isProxy(fields.tokenize)) throw new PolicyDrop();
  return fields.tokenize.bind(value);
}

function readLimits(value) {
  if (value === undefined) return { ...DEFAULT_LIMITS };
  const fields = ownDataRecord(value, new Set(Object.keys(DEFAULT_LIMITS)));
  const limits = { ...DEFAULT_LIMITS };
  for (const [key, item] of Object.entries(fields)) {
    const [minimum, maximum] = LIMIT_BOUNDS[key];
    if (!Number.isSafeInteger(item) || item < minimum || item > maximum) {
      throw new PolicyDrop();
    }
    limits[key] = item;
  }
  if (limits.batchSize > limits.queueRecords) throw new PolicyDrop();
  return limits;
}

function hasDataMethod(value, name) {
  if (value === null || (typeof value !== 'object' && typeof value !== 'function') || isProxy(value)) return false;
  let cursor = value;
  for (let depth = 0; cursor !== null && depth < 8; depth += 1) {
    if (isProxy(cursor)) return false;
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(cursor, name);
      if (descriptor) {
        return Object.hasOwn(descriptor, 'value')
          && typeof descriptor.value === 'function'
          && !isProxy(descriptor.value);
      }
      cursor = Object.getPrototypeOf(cursor);
    } catch {
      return false;
    }
  }
  return false;
}

function safeOptions(value) {
  const fields = ownDataRecord(
    value,
    new Set(['store', 'sink', 'tokenize', 'tokenizer', 'limits']),
    ['store', 'sink']
  );
  if (Object.hasOwn(fields, 'tokenize') === Object.hasOwn(fields, 'tokenizer')) throw new PolicyDrop();
  if (isProxy(fields.store)
      || Object.getPrototypeOf(fields.store) !== DurableWorkflowStore.prototype
      || !hasDataMethod(fields.store, 'readEventPage')
      || !hasDataMethod(fields.store, 'getTelemetryCursor')
      || !hasDataMethod(fields.store, 'advanceTelemetryCursor')
      || !hasDataMethod(fields.store, 'countEventRowsAfter')) {
    throw new PolicyDrop();
  }
  if (typeof fields.sink !== 'function' || isProxy(fields.sink)) throw new PolicyDrop();
  const tokenize = resolveTokenizer(fields.tokenize ?? fields.tokenizer);
  return { store: fields.store, sink: fields.sink, tokenize, limits: readLimits(fields.limits) };
}

function projectWith(event, tokenize) {
  const source = ownDataRecord(event, SOURCE_FIELD_SET, SOURCE_FIELDS);
  const sequence = boundedInteger(source.sequence, 1);
  const eventId = boundedSourceString(source.eventId);
  const workflowId = boundedSourceString(source.workflowId);
  const revision = boundedInteger(source.revision);
  if (typeof source.type !== 'string' || !EVENT_TYPES.has(source.type)) throw new PolicyDrop();
  const fromState = safeState(source.fromState);
  const toState = safeState(source.toState);
  const fence = boundedInteger(source.fence);
  const occurredAtMs = boundedInteger(source.occurredAtMs);

  let eventToken;
  let workflowToken;
  try {
    eventToken = validateToken(tokenize('event', eventId), eventId);
    workflowToken = validateToken(tokenize('workflow', workflowId), workflowId);
  } catch {
    throw new RedactionFailure();
  }
  const recordData = Object.create(null);
  Object.assign(recordData, {
    schema: 'agent-harness.telemetry.v1',
    sequence,
    eventId: eventToken,
    workflowId: workflowToken,
    revision,
    eventType: source.type,
    fromState,
    toState,
    fence,
    occurredAtMs
  });
  const record = Object.freeze(recordData);
  let bytes;
  try {
    bytes = Buffer.byteLength(JSON.stringify(record), 'utf8');
  } catch {
    throw new PolicyDrop();
  }
  if (bytes > MAX_PROJECTED_RECORD_BYTES) throw new PolicyDrop();
  return { record, bytes };
}

export function projectTelemetryEvent(event, tokenize) {
  if (typeof tokenize !== 'function' || isProxy(tokenize)) {
    throw telemetryError('TELEMETRY_INVALID_INPUT', 'project', 'Telemetry input could not be safely projected.');
  }
  try {
    return projectWith(event, tokenize).record;
  } catch (error) {
    if (error instanceof RedactionFailure) {
      throw telemetryError('TELEMETRY_REDACTION_FAILED', 'project', 'Telemetry redaction could not be completed.');
    }
    throw telemetryError('TELEMETRY_INVALID_INPUT', 'project', 'Telemetry input could not be safely projected.');
  }
}

export function createHmacTokenizer(key) {
  let bytes;
  try {
    const validView = !isProxy(key) && (Buffer.isBuffer(key)
      || (ArrayBuffer.isView(key)
        && Object.getPrototypeOf(key) === Uint8Array.prototype));
    if (!validView) throw new Error();
    const ownKeys = Reflect.ownKeys(key);
    if (ownKeys.length < 32 || ownKeys.length > 64) throw new Error();
    bytes = [];
    for (let index = 0; index < ownKeys.length; index += 1) {
      const keyName = String(index);
      const descriptor = Object.getOwnPropertyDescriptor(key, keyName);
      if (!descriptor || !descriptor.enumerable || !Object.hasOwn(descriptor, 'value')
          || !Number.isInteger(descriptor.value) || descriptor.value < 0 || descriptor.value > 255) {
        throw new Error();
      }
      bytes.push(descriptor.value);
    }
  } catch {
    throw telemetryError('TELEMETRY_INVALID_INPUT', 'createTokenizer', 'A valid local tokenization key is required.');
  }
  const keyCopy = Buffer.from(bytes);
  let destroyed = false;
  const tokenizer = Object.freeze({
    tokenize(kind, value) {
      if (destroyed || !TOKEN_KINDS.has(kind) || typeof value !== 'string' || value.length > MAX_SOURCE_STRING_UNITS) {
        throw new Error('tokenization unavailable');
      }
      return 'tok1_' + createHmac('sha256', keyCopy)
        .update(kind, 'utf8').update('\0', 'utf8').update(value, 'utf8').digest('hex');
    },
    destroy() {
      if (!destroyed) keyCopy.fill(0);
      destroyed = true;
    }
  });
  return tokenizer;
}

function validateSinkAcceptance(value, recordCount) {
  try {
    const result = ownDataRecord(value, new Set(['accepted', 'acceptedCount']), ['accepted', 'acceptedCount']);
    return result.accepted === true && result.acceptedCount === recordCount;
  } catch {
    return false;
  }
}

function sleepBounded(milliseconds) {
  if (milliseconds <= 0) return Promise.resolve();
  return new Promise(resolve => setTimeout(resolve, milliseconds));
}

export class TelemetryExporter {
  #store;
  #sink;
  #tokenize;
  #limits;
  #flushPromise = null;
  #flushComplete = false;
  #activeSink = null;
  #closed = false;
  #counters = { delivered: 0, policyDropped: 0, attempts: 0 };
  #lastErrorCode = null;
  #lastCursor = 0;
  #lastPending = { count: 0, capped: false };

  constructor(options) {
    let configured;
    try {
      configured = safeOptions(options);
    } catch {
      throw telemetryError('TELEMETRY_INVALID_INPUT', 'construct', 'Telemetry exporter configuration is invalid.');
    }
    this.#store = configured.store;
    this.#sink = configured.sink;
    this.#tokenize = configured.tokenize;
    this.#limits = configured.limits;
  }

  diagnostics() {
    let cursor = this.#lastCursor;
    let pending = this.#lastPending;
    let state = this.#closed ? 'closed' : 'idle';
    try {
      cursor = this.#store.getTelemetryCursor(CONSUMER_ID);
      pending = this.#store.countEventRowsAfter(cursor, MAX_PENDING_DIAGNOSTIC_ROWS);
      this.#lastCursor = cursor;
      this.#lastPending = pending;
    } catch {
      if (!this.#closed) state = 'unavailable';
    }
    if (!this.#closed && this.#flushPromise && !this.#flushComplete) state = 'flushing';
    else if (!this.#closed && this.#activeSink) state = 'sink-pending';
    else if (!this.#closed && this.#lastErrorCode === 'TELEMETRY_STORE_UNAVAILABLE') state = 'unavailable';
    return Object.freeze({
      state,
      cursor,
      pendingRecords: pending.count,
      pendingCapped: pending.capped,
      deliveredRecords: this.#counters.delivered,
      policyDroppedRecords: this.#counters.policyDropped,
      sinkAttempts: this.#counters.attempts,
      lastErrorCode: this.#lastErrorCode
    });
  }

  async flush() {
    if (this.#closed) return this.diagnostics();
    if (this.#flushPromise || this.#activeSink) return this.diagnostics();
    this.#flushComplete = false;
    this.#flushPromise = this.#runFlush();
    try {
      return await this.#flushPromise;
    } finally {
      this.#flushPromise = null;
      this.#flushComplete = false;
    }
  }

  async #runFlush() {
    this.#lastErrorCode = null;
    let batches = 0;
    let drops = 0;
    while (batches < this.#limits.maxBatchesPerFlush && drops < this.#limits.maxPolicyDropsPerFlush) {
      let cursor;
      let page;
      try {
        cursor = this.#store.getTelemetryCursor(CONSUMER_ID);
        page = this.#store.readEventPage({
          afterSequence: cursor,
          limit: Math.min(this.#limits.batchSize, this.#limits.queueRecords)
        });
      } catch {
        this.#lastErrorCode = 'TELEMETRY_STORE_UNAVAILABLE';
        break;
      }
      if (page.events.length === 0) break;

      const queue = [];
      let queueBytes = 2; // JSON array brackets are part of the byte budget.
      let dropSequence = null;
      let expected = cursor + 1;
      for (const event of page.events) {
        if (event.sequence !== expected) {
          this.#lastErrorCode = 'TELEMETRY_CURSOR_GAP';
          break;
        }
        let projected;
        try {
          projected = projectWith(event, this.#tokenize);
        } catch (error) {
          if (error instanceof PolicyDrop) {
            dropSequence = event.sequence;
            break;
          }
          this.#lastErrorCode = 'TELEMETRY_REDACTION_FAILED';
          break;
        }
        const addedBytes = projected.bytes + (queue.length === 0 ? 0 : 1);
        if (queue.length >= this.#limits.queueRecords
            || queueBytes + addedBytes > this.#limits.queueBytes) {
          if (queue.length === 0) dropSequence = event.sequence;
          break;
        }
        queue.push(projected.record);
        queueBytes += addedBytes;
        expected += 1;
      }
      if (this.#lastErrorCode) break;

      if (queue.length > 0) {
        const delivery = await this.#deliver(queue);
        batches += 1;
        if (delivery !== 'accepted') {
          this.#lastErrorCode = delivery === 'timeout' || delivery === 'busy'
            ? 'TELEMETRY_SINK_TIMEOUT'
            : 'TELEMETRY_SINK_UNAVAILABLE';
          break;
        }
        this.#counters.delivered += queue.length;
        try {
          let acceptedCursor = cursor;
          for (const record of queue) {
            this.#store.advanceTelemetryCursor({
              consumerId: CONSUMER_ID,
              expectedSequence: acceptedCursor,
              nextSequence: record.sequence
            });
            acceptedCursor = record.sequence;
          }
        } catch {
          this.#lastErrorCode = 'TELEMETRY_CURSOR_CONFLICT';
          break;
        }
        continue;
      }

      if (dropSequence !== null) {
        try {
          this.#store.advanceTelemetryCursor({
            consumerId: CONSUMER_ID,
            expectedSequence: cursor,
            nextSequence: dropSequence
          });
          this.#counters.policyDropped += 1;
          drops += 1;
          continue;
        } catch {
          this.#lastErrorCode = 'TELEMETRY_CURSOR_CONFLICT';
          break;
        }
      }
      break;
    }
    this.#flushComplete = true;
    return this.diagnostics();
  }

  async #deliver(records) {
    for (let attempt = 1; attempt <= this.#limits.maxAttempts; attempt += 1) {
      if (this.#activeSink) return 'busy';
      const controller = new AbortController();
      const immutableBatch = Object.freeze([...records]);
      const active = { controller, promise: null };
      this.#counters.attempts += 1;
      active.promise = Promise.resolve()
        .then(() => this.#sink(immutableBatch, Object.freeze({ signal: controller.signal })))
        .then(value => validateSinkAcceptance(value, immutableBatch.length) ? 'accepted' : 'rejected')
        .catch(() => 'rejected');
      this.#activeSink = active;
      active.promise.then(() => {
        if (this.#activeSink === active) this.#activeSink = null;
      });

      let timer;
      const timeout = new Promise(resolve => {
        timer = setTimeout(() => resolve('timeout'), this.#limits.sinkTimeoutMs);
      });
      const outcome = await Promise.race([active.promise, timeout]);
      clearTimeout(timer);
      if (outcome === 'accepted') return 'accepted';
      if (outcome === 'timeout') {
        // Keep the active latch until the sink settles. An abort-ignoring sink
        // cannot cause another request to overlap this one.
        controller.abort();
        return 'timeout';
      }
      if (this.#activeSink === active) {
        await Promise.resolve();
        if (this.#activeSink === active) return 'busy';
      }
      if (attempt < this.#limits.maxAttempts) await sleepBounded(this.#limits.retryDelayMs);
    }
    return 'rejected';
  }

  async close() {
    if (this.#closed) return this.diagnostics();
    if (this.#flushPromise) {
      // The active flush is bounded by its sink timeout and batch/retry caps.
      await this.#flushPromise;
    } else if (!this.#activeSink) {
      await this.flush();
    }
    this.#closed = true;
    if (this.#activeSink) this.#activeSink.controller.abort();
    return this.diagnostics();
  }
}

export function createTelemetryExporter(options) {
  return new TelemetryExporter(options);
}

// The fixed event allowlist, exposed so downstream mappers can be checked for drift.
export const TELEMETRY_EVENT_TYPES = Object.freeze([...EVENT_TYPES]);

export const TELEMETRY_LIMITS = Object.freeze({
  defaults: DEFAULT_LIMITS,
  maximums: Object.freeze({
    queueRecords: LIMIT_BOUNDS.queueRecords[1],
    queueBytes: LIMIT_BOUNDS.queueBytes[1],
    batchSize: LIMIT_BOUNDS.batchSize[1],
    maxAttempts: LIMIT_BOUNDS.maxAttempts[1],
    retryDelayMs: LIMIT_BOUNDS.retryDelayMs[1],
    sinkTimeoutMs: LIMIT_BOUNDS.sinkTimeoutMs[1],
    maxBatchesPerFlush: LIMIT_BOUNDS.maxBatchesPerFlush[1],
    maxPolicyDropsPerFlush: LIMIT_BOUNDS.maxPolicyDropsPerFlush[1],
    maxSourceStringUnits: MAX_SOURCE_STRING_UNITS,
    projectedRecordBytes: MAX_PROJECTED_RECORD_BYTES,
    pendingDiagnosticRows: MAX_PENDING_DIAGNOSTIC_ROWS
  })
});
