import { isDeepStrictEqual, types as utilTypes } from 'node:util';
import {
  NEATLOGS_COMPLETION_SPAN_NAME,
  NEATLOGS_LOCAL_SPAN_NAME,
  NEATLOGS_LOCAL_WORKFLOW_NAME,
  NEATLOGS_SDK_MAPPING_LIMITS,
  mapNeatlogsSourceRecord,
  sourceRecordFromNeatlogsAttributes,
  validateNeatlogsSourceRecords
} from './neatlogs-sdk-mapper.js';
import { createNeatlogsSdkMask } from './neatlogs-sdk-mask.js';

const SYNTHETIC_API_KEY = 'offline-synthetic-placeholder-no-service-access';
const OFFLINE_ENDPOINT = 'https://offline.invalid/v1/traces';
const CAPTURE_SCHEMA = 'agent-harness.neatlogs.local-capture.v1';
const MAX_CAPTURE_BYTES = 128 * 1024;
const MAX_EXPORT_DELAY_MS = 150;
const MIN_CAPTURE_TIMEOUT_MS = 10;
const MAX_CAPTURE_TIMEOUT_MS = 500;
const SUCCESS = 0;
const FAILED = 1;
const COUNTER_LIMIT = 64;
// Keep the SDK automatic threshold above the admitted root + marker set so
// forceFlush remains inside the prepare() request that owns every span.
const SDK_BATCH_SIZE = NEATLOGS_SDK_MAPPING_LIMITS.totalSpans + 1;
const ABORTED_GETTER = Object.getOwnPropertyDescriptor(AbortSignal.prototype, 'aborted')?.get;
const EVENT_TARGET_ADD = EventTarget.prototype.addEventListener;
const EVENT_TARGET_REMOVE = EventTarget.prototype.removeEventListener;

function safeFailure(reasonCode, fields = {}) {
  return Object.freeze({
    status: 'failed',
    reasonCode,
    ...fields
  });
}

function isProxy(value) {
  try {
    return utilTypes.isProxy(value);
  } catch {
    return true;
  }
}

function ownOptions(value, allowed, defaults) {
  if (value === undefined) return { ...defaults };
  if (value === null || typeof value !== 'object' || Array.isArray(value) || isProxy(value)) {
    throw new TypeError();
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) throw new TypeError();
  const keys = Reflect.ownKeys(value);
  if (keys.length > allowed.size) throw new TypeError();
  const result = { ...defaults };
  for (const key of keys) {
    if (typeof key !== 'string' || !allowed.has(key)) throw new TypeError();
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor || !descriptor.enumerable || !Object.hasOwn(descriptor, 'value')) {
      throw new TypeError();
    }
    result[key] = descriptor.value;
  }
  return result;
}

function hasUnsafeSdkEnvironment() {
  // Inspect environment names only. Values are neither read nor copied.
  try {
    return Object.keys(process.env).some(name => (
      name.startsWith('NEATLOGS_') || name.startsWith('OTEL_') || name === 'NODE_OPTIONS'
    ));
  } catch {
    return true;
  }
}

function validateFactoryOptions(value) {
  const options = ownOptions(value, new Set([
    'timeoutMs', 'exporterDelayMs', 'exporterMode'
  ]), {
    timeoutMs: 250,
    exporterDelayMs: 0,
    exporterMode: 'success'
  });
  if (!Number.isSafeInteger(options.timeoutMs)
      || options.timeoutMs < MIN_CAPTURE_TIMEOUT_MS
      || options.timeoutMs > MAX_CAPTURE_TIMEOUT_MS
      || !Number.isSafeInteger(options.exporterDelayMs)
      || options.exporterDelayMs < 0
      || options.exporterDelayMs > MAX_EXPORT_DELAY_MS
      || !['success', 'failure'].includes(options.exporterMode)) {
    throw new TypeError();
  }
  return Object.freeze(options);
}

function validateCallOptions(value) {
  const options = ownOptions(value, new Set(['signal']), { signal: undefined });
  return options;
}

function inspectAbortSignal(signal) {
  if (signal === undefined) return { valid: true, aborted: false };
  if (isProxy(signal) || typeof ABORTED_GETTER !== 'function') {
    return { valid: false, aborted: false };
  }
  try {
    return { valid: true, aborted: Reflect.apply(ABORTED_GETTER, signal, []) === true };
  } catch {
    return { valid: false, aborted: false };
  }
}

function addAbortListener(signal, listener) {
  Reflect.apply(EVENT_TARGET_ADD, signal, ['abort', listener, { once: true }]);
}

function removeAbortListener(signal, listener) {
  Reflect.apply(EVENT_TARGET_REMOVE, signal, ['abort', listener]);
}

function hrTimeToNanos(value) {
  if (!Array.isArray(value) || isProxy(value) || value.length !== 2) throw new TypeError();
  const seconds = value[0];
  const nanos = value[1];
  if (!Number.isSafeInteger(seconds) || seconds < 0
      || !Number.isSafeInteger(nanos) || nanos < 0 || nanos >= 1_000_000_000) {
    throw new TypeError();
  }
  return seconds * 1_000_000_000 + nanos;
}

function readableSpanToMaskInput(span) {
  if (span === null || typeof span !== 'object' || isProxy(span)) throw new TypeError();
  const context = span.spanContext();
  const attributes = span.attributes;
  const resourceAttributes = span.resource?.attributes;
  if (context === null || typeof context !== 'object'
      || attributes === null || typeof attributes !== 'object' || isProxy(attributes)
      || resourceAttributes === null || typeof resourceAttributes !== 'object'
      || isProxy(resourceAttributes)) {
    throw new TypeError();
  }
  const copiedAttributes = Object.create(null);
  for (const key of Reflect.ownKeys(attributes)) {
    if (typeof key !== 'string') throw new TypeError();
    const descriptor = Object.getOwnPropertyDescriptor(attributes, key);
    if (!descriptor || !descriptor.enumerable || !Object.hasOwn(descriptor, 'value')) {
      throw new TypeError();
    }
    copiedAttributes[key] = descriptor.value;
  }
  const copiedResource = Object.create(null);
  for (const key of Reflect.ownKeys(resourceAttributes)) {
    if (typeof key !== 'string') throw new TypeError();
    const descriptor = Object.getOwnPropertyDescriptor(resourceAttributes, key);
    if (!descriptor || !descriptor.enumerable || !Object.hasOwn(descriptor, 'value')) {
      throw new TypeError();
    }
    copiedResource[key] = descriptor.value;
  }
  const statusCode = span.status?.code;
  const status = statusCode === 0 ? 'UNSET' : statusCode === 1 ? 'OK' : 'ERROR';
  const kind = span.name === NEATLOGS_LOCAL_SPAN_NAME
    ? 'workflow'
    : span.name === NEATLOGS_COMPLETION_SPAN_NAME ? 'Neatlogs.INTERNAL' : null;
  const maskInput = {
    trace_id: context.traceId,
    span_id: context.spanId,
    parent_span_id: span.parentSpanId ?? null,
    name: span.name,
    kind,
    start_time: hrTimeToNanos(span.startTime),
    end_time: hrTimeToNanos(span.endTime),
    duration_ns: hrTimeToNanos(span.duration),
    attributes: copiedAttributes,
    resource: { attributes: copiedResource },
    status: { code: status, description: null },
    events: span.events
  };
  if (span.links !== undefined) maskInput.links = span.links;
  return maskInput;
}

function safeSdkCounters(client, maskState, exporter) {
  const diagnostics = client?.getDeliveryDiagnostics?.() ?? {};
  const counter = name => Number.isSafeInteger(diagnostics[name]) && diagnostics[name] >= 0
    ? Math.min(COUNTER_LIMIT, diagnostics[name])
    : 0;
  return Object.freeze({
    malformedMaskDrops: maskState.diagnostics().malformedDrops,
    sdkMaskedSpanDrops: counter('maskedSpanDrops'),
    sdkSpanQueueDrops: counter('spanQueueDrops'),
    sdkSpanExportFailures: counter('spanExportFailures'),
    localExporterCallbacks: exporter.diagnostics().callbacks,
    localExporterFailures: exporter.diagnostics().failures,
    localExporterCapturedSpans: exporter.diagnostics().capturedSpans
  });
}

function sameRecord(left, right) {
  return isDeepStrictEqual(left, right);
}

function validateCapturedSet(spans, records) {
  if (!Array.isArray(spans) || spans.length > NEATLOGS_SDK_MAPPING_LIMITS.totalSpans) {
    return { reasonCode: 'CAPTURE_SPAN_LIMIT' };
  }
  const roots = spans.filter(span => span.name === NEATLOGS_LOCAL_SPAN_NAME);
  const markers = spans.filter(span => span.name === NEATLOGS_COMPLETION_SPAN_NAME);
  if (roots.length !== records.length || markers.length !== records.length
      || roots.length + markers.length !== spans.length) {
    return { reasonCode: 'CAPTURE_INCOMPLETE' };
  }
  const expectedBySequence = new Map(records.map(record => [record.sequence, record]));
  const rootsByTraceId = new Map();
  for (const root of roots) {
    let source;
    try {
      source = sourceRecordFromNeatlogsAttributes(root.attributes);
    } catch {
      return { reasonCode: 'CAPTURE_MAPPING_INVALID' };
    }
    const expected = expectedBySequence.get(source.sequence);
    if (!expected || !sameRecord(source, expected) || rootsByTraceId.has(root.trace_id)) {
      return { reasonCode: 'CAPTURE_SOURCE_MISMATCH' };
    }
    rootsByTraceId.set(root.trace_id, root);
  }
  const markerTraces = new Set();
  for (const marker of markers) {
    if (!rootsByTraceId.has(marker.trace_id)
        || markerTraces.has(marker.trace_id)
        || marker.span_id === rootsByTraceId.get(marker.trace_id).span_id) {
      return { reasonCode: 'CAPTURE_MARKER_MISMATCH' };
    }
    markerTraces.add(marker.trace_id);
  }
  if (markerTraces.size !== rootsByTraceId.size) return { reasonCode: 'CAPTURE_MARKER_MISMATCH' };
  const serialized = JSON.stringify(spans);
  if (Buffer.byteLength(serialized, 'utf8') > MAX_CAPTURE_BYTES) {
    return { reasonCode: 'CAPTURE_OUTPUT_TOO_LARGE' };
  }
  return { roots, markers };
}

class BoundedLocalSpanExporter {
  #delayMs;
  #mode;
  #maskState;
  #spans = [];
  #failure = null;
  #generation = 0;
  #pending = new Set();
  #closed = false;
  #callbackCount = 0;
  #failureCount = 0;

  constructor({ delayMs, mode, maskState }) {
    this.#delayMs = delayMs;
    this.#mode = mode;
    this.#maskState = maskState;
  }

  beginGeneration(generation) {
    if (this.#pending.size > 0 || this.#closed) return false;
    this.#generation = generation;
    this.#spans = [];
    this.#failure = null;
    this.#callbackCount = 0;
    this.#failureCount = 0;
    return true;
  }

  spans() {
    return Object.freeze([...this.#spans]);
  }

  failure() {
    return this.#failure;
  }

  export(spans, resultCallback) {
    this.#callbackCount = Math.min(COUNTER_LIMIT, this.#callbackCount + 1);
    const generation = this.#generation;
    let settled = false;
    let timer;
    let resolvePending;
    const pending = new Promise(resolve => { resolvePending = resolve; });
    this.#pending.add(pending);
    const finish = code => {
      if (settled) return;
      settled = true;
      if (timer !== undefined) clearTimeout(timer);
      this.#pending.delete(pending);
      resolvePending();
      try {
        resultCallback({ code });
      } catch {
        this.#failure = 'EXPORT_CALLBACK_FAILED';
        this.#failureCount = Math.min(COUNTER_LIMIT, this.#failureCount + 1);
      }
    };
    const work = () => {
      if (this.#closed || generation !== this.#generation) {
        this.#failure = 'STALE_EXPORT_GENERATION';
        this.#failureCount = Math.min(COUNTER_LIMIT, this.#failureCount + 1);
        finish(FAILED);
        return;
      }
      if (!Array.isArray(spans) || spans.length > NEATLOGS_SDK_MAPPING_LIMITS.totalSpans
          || this.#spans.length + spans.length > NEATLOGS_SDK_MAPPING_LIMITS.totalSpans) {
        this.#failure = 'CAPTURE_SPAN_LIMIT';
        this.#failureCount = Math.min(COUNTER_LIMIT, this.#failureCount + 1);
        finish(FAILED);
        return;
      }
      try {
        const normalized = [];
        for (const span of spans) {
          const sanitized = this.#maskState.mask(readableSpanToMaskInput(span));
          if (sanitized === null || sanitized === undefined) {
            this.#failure = 'CAPTURE_MASK_DROP';
            this.#failureCount = Math.min(COUNTER_LIMIT, this.#failureCount + 1);
            finish(FAILED);
            return;
          }
          normalized.push(sanitized);
        }
        if (this.#mode === 'failure') {
          this.#failure = 'LOCAL_EXPORT_FAILED';
          this.#failureCount = Math.min(COUNTER_LIMIT, this.#failureCount + 1);
          finish(FAILED);
          return;
        }
        const candidate = [...this.#spans, ...normalized];
        if (Buffer.byteLength(JSON.stringify(candidate), 'utf8') > MAX_CAPTURE_BYTES) {
          this.#failure = 'CAPTURE_OUTPUT_TOO_LARGE';
          this.#failureCount = Math.min(COUNTER_LIMIT, this.#failureCount + 1);
          finish(FAILED);
          return;
        }
        this.#spans = candidate;
        finish(SUCCESS);
      } catch {
        this.#failure = 'LOCAL_EXPORT_INVALID_SPAN';
        this.#failureCount = Math.min(COUNTER_LIMIT, this.#failureCount + 1);
        finish(FAILED);
      }
    };
    if (this.#delayMs === 0) work();
    else timer = setTimeout(work, this.#delayMs);
  }

  async forceFlush() {
    if (this.#pending.size > 0) await Promise.all([...this.#pending]);
  }

  async shutdown() {
    this.#closed = true;
    await this.forceFlush();
  }

  diagnostics() {
    return Object.freeze({
      callbacks: this.#callbackCount,
      failures: this.#failureCount,
      capturedSpans: Math.min(COUNTER_LIMIT, this.#spans.length),
      pendingCallbacks: Math.min(COUNTER_LIMIT, this.#pending.size)
    });
  }
}

export function createNeatlogsSdkCapture(factoryOptions) {
  let options;
  try {
    options = validateFactoryOptions(factoryOptions);
  } catch {
    throw new TypeError('Neatlogs local capture options are invalid.');
  }
  let closed = false;
  let closePromise = null;
  let activeRequest = null;
  let sdk = null;
  let client = null;
  let maskState = null;
  let exporter = null;
  let generation = 0;

  const diagnostics = () => Object.freeze({
    generation,
    active: activeRequest !== null,
    closed,
    sdkImported: sdk !== null,
    clientCreated: client !== null,
    exporter: exporter?.diagnostics() ?? Object.freeze({
      callbacks: 0, failures: 0, capturedSpans: 0, pendingCallbacks: 0
    })
  });

  const ensureSdk = async request => {
    if (sdk !== null) return true;
    if (request.aborted) return false;
    if (hasUnsafeSdkEnvironment()) return false;
    try {
      sdk = await import('neatlogs');
      if (typeof sdk.Client !== 'function' || typeof sdk.trace !== 'function'
          || sdk.__version__ !== '1.1.28') {
        sdk = null;
        return false;
      }
      if (request.aborted) {
        sdk = null;
        return false;
      }
      maskState = createNeatlogsSdkMask();
      exporter = new BoundedLocalSpanExporter({
        delayMs: options.exporterDelayMs,
        mode: options.exporterMode,
        maskState
      });
      client = new sdk.Client({
        apiKey: SYNTHETIC_API_KEY,
        workflowName: NEATLOGS_LOCAL_WORKFLOW_NAME,
        endpoint: OFFLINE_ENDPOINT,
        sampleRate: 1,
        debug: false,
        captureLogs: false,
        uploadAuthority: false,
        disableExport: false,
        mask: maskState.mask,
        batchSize: SDK_BATCH_SIZE,
        flushInterval: 3600,
        spanExporter: exporter
      });
      return true;
    } catch {
      sdk = null;
      client = null;
      exporter = null;
      maskState = null;
      return false;
    }
  };

  const prepare = async (records, callOptions) => {
    if (closed) return safeFailure('CAPTURE_CLOSED');
    if (activeRequest !== null) return safeFailure('CAPTURE_BUSY');
    let normalizedRecords;
    let controls;
    try {
      normalizedRecords = validateNeatlogsSourceRecords(records);
      controls = validateCallOptions(callOptions);
    } catch (error) {
      return safeFailure(error?.code ?? 'INVALID_CAPTURE_INPUT');
    }
    const signal = controls.signal;
    const initialSignalState = inspectAbortSignal(signal);
    if (!initialSignalState.valid) return safeFailure('INVALID_CAPTURE_SIGNAL');
    if (initialSignalState.aborted) return safeFailure('ABORTED_BEFORE_PREPARATION');
    if (hasUnsafeSdkEnvironment()) return safeFailure('UNSAFE_SDK_ENVIRONMENT');

    const request = {
      generation: generation + 1,
      aborted: false,
      timedOut: false,
      abortReason: null,
      signal,
      done: null,
      timeout: null,
      abortListener: null
    };
    generation = request.generation;
    activeRequest = request;
    let finishDone;
    request.done = new Promise(resolve => { finishDone = resolve; });
    let listenerRegistered = false;
    try {
      request.timeout = setTimeout(() => {
        if (!request.aborted) {
          request.timedOut = true;
          request.aborted = true;
          request.abortReason = 'CAPTURE_TIMEOUT';
        }
      }, options.timeoutMs);
      request.timeout.unref?.();
      if (signal) {
        request.abortListener = () => {
          if (!request.aborted) {
            request.aborted = true;
            request.abortReason = 'CAPTURE_ABORTED';
          }
        };
        try {
          addAbortListener(signal, request.abortListener);
          listenerRegistered = true;
        } catch {
          request.aborted = true;
          request.abortReason = 'CAPTURE_SIGNAL_REGISTRATION_FAILED';
        }
        const registeredSignalState = inspectAbortSignal(signal);
        if (!registeredSignalState.valid) {
          request.aborted = true;
          request.abortReason = 'INVALID_CAPTURE_SIGNAL';
        } else if (registeredSignalState.aborted && !request.aborted) {
          request.aborted = true;
          request.abortReason = 'CAPTURE_ABORTED';
        }
      }
      if (request.aborted) {
        return safeFailure(request.abortReason ?? 'CAPTURE_ABORTED', {
          generation: request.generation
        });
      }
      if (!(await ensureSdk(request))) {
        return safeFailure(request.aborted
          ? request.abortReason ?? 'CAPTURE_ABORTED'
          : 'SDK_UNAVAILABLE', { generation: request.generation });
      }
      if (request.aborted) return safeFailure(request.abortReason ?? 'CAPTURE_ABORTED', {
        generation: request.generation
      });
      if (!exporter.beginGeneration(request.generation)) {
        return safeFailure('EXPORTER_BUSY', { generation: request.generation });
      }

      for (const record of normalizedRecords) {
        if (request.aborted) break;
        const attributes = mapNeatlogsSourceRecord(record);
        try {
          await client.activate(() => sdk.trace({
            name: NEATLOGS_LOCAL_SPAN_NAME,
            kind: 'WORKFLOW',
            attributes
          }, () => undefined));
        } catch {
          request.aborted = true;
          request.abortReason = 'SDK_TRACE_FAILED';
          break;
        }
      }

      let flushResult = false;
      try {
        flushResult = await client.flush();
      } catch {
        flushResult = false;
      }
      if (request.aborted) {
        return safeFailure(request.abortReason ?? 'CAPTURE_ABORTED', {
          generation: request.generation,
          counters: safeSdkCounters(client, maskState, exporter)
        });
      }
      if (exporter.failure() !== null) {
        return safeFailure(exporter.failure(), {
          generation: request.generation,
          counters: safeSdkCounters(client, maskState, exporter)
        });
      }
      const spans = exporter.spans();
      const coverage = validateCapturedSet(spans, normalizedRecords);
      if (coverage.reasonCode) {
        return safeFailure(coverage.reasonCode, {
          generation: request.generation,
          flushResult: flushResult === true,
          counters: safeSdkCounters(client, maskState, exporter)
        });
      }
      if (flushResult !== true) {
        return safeFailure('SDK_FLUSH_FAILED', {
          generation: request.generation,
          counters: safeSdkCounters(client, maskState, exporter)
        });
      }
      const roots = coverage.roots;
      const markers = coverage.markers;
      const envelope = Object.freeze({
        schema: CAPTURE_SCHEMA,
        mode: 'offline-synthetic-real-sdk',
        sdk: Object.freeze({ name: 'neatlogs', version: sdk.__version__ }),
        sourceRecordCount: normalizedRecords.length,
        rootSpanCount: roots.length,
        completionMarkerSpanCount: markers.length,
        spans,
        remoteIngestionVerified: false,
        sponsorAcceptance: false,
        entireEnabled: false,
        telemetryCursorAdvanced: false,
        financeAcknowledgement: false,
        generatedTraceIdsStableAcrossReplay: false,
        remoteDeduplicationVerified: false
      });
      if (Buffer.byteLength(JSON.stringify(envelope), 'utf8') > MAX_CAPTURE_BYTES) {
        return safeFailure('CAPTURE_OUTPUT_TOO_LARGE', { generation: request.generation });
      }
      return Object.freeze({
        status: 'captured_local',
        generation: request.generation,
        envelope,
        counters: safeSdkCounters(client, maskState, exporter)
      });
    } finally {
      try {
        if (request.timeout !== null) clearTimeout(request.timeout);
      } catch {
        // Request release must not depend on caller-owned control cleanup.
      } finally {
        try {
          if (listenerRegistered && signal && request.abortListener) {
            removeAbortListener(signal, request.abortListener);
          }
        } catch {
          // Ignore only cleanup errors; request work has already settled.
        } finally {
          if (activeRequest === request) activeRequest = null;
          finishDone();
        }
      }
    }
  };

  const close = () => {
    if (closePromise !== null) return closePromise;
    closed = true;
    closePromise = (async () => {
      const pending = activeRequest;
      if (pending !== null) {
        if (!pending.aborted) {
          pending.aborted = true;
          pending.abortReason = 'CAPTURE_CLOSED';
        }
        await pending.done;
      }
      if (client !== null) {
        let shutdownResult = false;
        try {
          shutdownResult = await client.close('local-capture-close', MAX_CAPTURE_TIMEOUT_MS);
        } catch {
          shutdownResult = false;
        }
        return Object.freeze({
          status: shutdownResult ? 'closed' : 'closed_with_sdk_timeout',
          sdkShutdownCompleted: shutdownResult === true
        });
      }
      return Object.freeze({ status: 'closed', sdkShutdownCompleted: true });
    })();
    return closePromise;
  };

  return Object.freeze({ prepare, close, diagnostics });
}

export const NEATLOGS_LOCAL_CAPTURE_LIMITS = Object.freeze({
  timeoutMs: Object.freeze({ minimum: MIN_CAPTURE_TIMEOUT_MS, maximum: MAX_CAPTURE_TIMEOUT_MS }),
  exporterDelayMs: Object.freeze({ maximum: MAX_EXPORT_DELAY_MS }),
  outputBytes: MAX_CAPTURE_BYTES,
  totalSpans: NEATLOGS_SDK_MAPPING_LIMITS.totalSpans,
  networkEndpoint: OFFLINE_ENDPOINT
});
