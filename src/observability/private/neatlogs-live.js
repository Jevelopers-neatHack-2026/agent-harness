import { NEATLOGS_COMPLETION_SPAN_NAME, NEATLOGS_LOCAL_SPAN_NAME } from './neatlogs-sdk-mapper.js';
import {
  NeatlogsOtlpError,
  decodeNeatlogsExportResponse,
  encodeNeatlogsExportRequest,
  parseNeatlogsJson
} from './neatlogs-otlp.js';

const SEND_DEFAULTS = Object.freeze({ timeoutMs: 5_000, maxAttempts: 3, retryDelayMs: 500 });
const SEND_BOUNDS = Object.freeze({
  timeoutMs: [100, 15_000],
  maxAttempts: [1, 5],
  retryDelayMs: [0, 5_000]
});
const READBACK_DEFAULTS = Object.freeze({ timeoutMs: 5_000, maxPolls: 10, pollIntervalMs: 2_000 });
const READBACK_BOUNDS = Object.freeze({
  timeoutMs: [100, 15_000],
  maxPolls: [1, 30],
  pollIntervalMs: [0, 10_000]
});
const MAX_RETRY_AFTER_MS = 10_000;
const MAX_READ_BYTES = 256 * 1024;
const MAX_TRACES = 32;
const SHORT_WEEKDAYS = Object.freeze(['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']);
const LONG_WEEKDAYS = Object.freeze([
  'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'
]);
const SHORT_MONTHS = Object.freeze([
  'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
]);

function bounded(options, defaults, bounds) {
  const result = { ...defaults };
  for (const key of Object.keys(defaults)) {
    if (options?.[key] === undefined) continue;
    const value = options[key];
    const [minimum, maximum] = bounds[key];
    if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
      throw new TypeError(`Neatlogs option ${key} is out of bounds.`);
    }
    result[key] = value;
  }
  return Object.freeze(result);
}

function sleep(milliseconds, signal) {
  if (signal?.aborted) return Promise.resolve(false);
  if (milliseconds <= 0) return Promise.resolve(true);
  return new Promise(resolve => {
    let timer;
    const onAbort = () => done(false);
    function done(completed) {
      clearTimeout(timer);
      signal?.removeEventListener?.('abort', onAbort);
      resolve(completed);
    }
    timer = setTimeout(() => done(true), milliseconds);
    signal?.addEventListener?.('abort', onAbort, { once: true });
    if (signal?.aborted) onAbort();
  });
}

function httpDateMilliseconds(value, nowMs) {
  let match;
  let weekday;
  let day;
  let month;
  let year;
  let hour;
  let minute;
  let second;
  let weekdayNames;
  let hasTwoDigitYear = false;

  match = /^(Sun|Mon|Tue|Wed|Thu|Fri|Sat), (\d{2}) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (\d{4}) (\d{2}):(\d{2}):(\d{2}) GMT$/.exec(value);
  if (match) {
    [, weekday, day, month, year, hour, minute, second] = match;
    weekdayNames = SHORT_WEEKDAYS;
    month = SHORT_MONTHS.indexOf(month);
  } else {
    match = /^(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday), (\d{2})-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(\d{2}) (\d{2}):(\d{2}):(\d{2}) GMT$/.exec(value);
    if (match) {
      [, weekday, day, month, year, hour, minute, second] = match;
      weekdayNames = LONG_WEEKDAYS;
      month = SHORT_MONTHS.indexOf(month);
      hasTwoDigitYear = true;
      const currentYear = new Date(nowMs).getUTCFullYear();
      year = Math.floor(currentYear / 100) * 100 + Number(year);
    } else {
      match = /^(Sun|Mon|Tue|Wed|Thu|Fri|Sat) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) {1,2}(\d{1,2}) (\d{2}):(\d{2}):(\d{2}) (\d{4})$/.exec(value);
      if (!match) return null;
      [, weekday, month, day, hour, minute, second, year] = match;
      weekdayNames = SHORT_WEEKDAYS;
      month = SHORT_MONTHS.indexOf(month);
    }
  }

  day = Number(day);
  year = Number(year);
  hour = Number(hour);
  minute = Number(minute);
  second = Number(second);
  if (month < 0 || hour > 23 || minute > 59 || second > 60) return null;

  const makeDate = candidateYear => {
    const date = new Date(0);
    date.setUTCFullYear(candidateYear, month, day);
    date.setUTCHours(hour, minute, Math.min(second, 59), 0);
    if (date.getUTCFullYear() !== candidateYear || date.getUTCMonth() !== month || date.getUTCDate() !== day ||
        date.getUTCHours() !== hour || date.getUTCMinutes() !== minute ||
        date.getUTCSeconds() !== Math.min(second, 59)) return null;
    return date.getTime() + (second === 60 ? 1_000 : 0);
  };

  let dateMs = makeDate(year);
  if (dateMs === null) return null;
  if (hasTwoDigitYear) {
    const cutoff = new Date(nowMs);
    cutoff.setUTCFullYear(cutoff.getUTCFullYear() + 50);
    if (dateMs > cutoff.getTime()) {
      year -= 100;
      dateMs = makeDate(year);
      if (dateMs === null) return null;
    }
  }
  const weekdayDate = new Date(dateMs - (second === 60 ? 1_000 : 0));
  if (weekdayNames[weekdayDate.getUTCDay()] !== weekday) return null;
  return dateMs;
}

function retryDelay(response, fallback, nowMs = Date.now()) {
  const header = response.headers?.get?.('retry-after');
  if (typeof header !== 'string' || header.trim() === '') {
    return { milliseconds: Math.min(fallback, MAX_RETRY_AFTER_MS), serverMinimum: false };
  }
  const trimmed = header.trim();
  let milliseconds = null;
  if (/^\d+$/.test(trimmed)) {
    let firstNonzero = 0;
    while (firstNonzero < trimmed.length - 1 && trimmed.charCodeAt(firstNonzero) === 48) firstNonzero += 1;
    // This bound preserves every exactly representable finite delay. The
    // MAX_RETRY_AFTER_MS guard below controls local sleeping only.
    const maxSeconds = String(Math.floor(Number.MAX_SAFE_INTEGER / 1_000));
    const significantLength = trimmed.length - firstNonzero;
    let exceedsBound = significantLength > maxSeconds.length;
    if (significantLength === maxSeconds.length) {
      for (let index = 0; index < significantLength; index += 1) {
        const digit = trimmed.charCodeAt(firstNonzero + index);
        const maximumDigit = maxSeconds.charCodeAt(index);
        if (digit === maximumDigit) continue;
        exceedsBound = digit > maximumDigit;
        break;
      }
    }
    if (exceedsBound) {
      milliseconds = Number.MAX_SAFE_INTEGER;
    } else {
      let seconds = 0;
      for (let index = firstNonzero; index < trimmed.length; index += 1) {
        seconds = seconds * 10 + trimmed.charCodeAt(index) - 48;
      }
      milliseconds = Math.ceil(seconds * 1_000);
    }
  } else {
    const dateMs = httpDateMilliseconds(trimmed, nowMs);
    if (dateMs !== null) milliseconds = Math.max(0, dateMs - nowMs);
  }
  if (milliseconds === null) {
    return { milliseconds: Math.min(fallback, MAX_RETRY_AFTER_MS), serverMinimum: false };
  }
  return { milliseconds, serverMinimum: true };
}

function codedError(code) {
  const error = new Error(code);
  error.code = code;
  return error;
}

async function readBounded(response, signal) {
  if (signal?.aborted) {
    try {
      const cancellation = response.body?.cancel?.();
      cancellation?.catch?.(() => {});
    } catch {}
    throw codedError('REQUEST_ABORTED');
  }
  const declared = Number(response.headers?.get?.('content-length'));
  if (Number.isFinite(declared) && declared > MAX_READ_BYTES) {
    await response.body?.cancel?.();
    throw new NeatlogsOtlpError('RESPONSE_TOO_LARGE');
  }
  if (!response.body?.getReader) {
    const cancelBody = () => { void response.body?.cancel?.().catch(() => {}); };
    signal?.addEventListener?.('abort', cancelBody, { once: true });
    try {
      return Buffer.from(await response.arrayBuffer());
    } finally {
      signal?.removeEventListener?.('abort', cancelBody);
    }
  }
  const reader = response.body.getReader();
  const cancelReader = () => { void reader.cancel().catch(() => {}); };
  signal?.addEventListener?.('abort', cancelReader, { once: true });
  const chunks = [];
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.length;
      if (total > MAX_READ_BYTES) {
        await reader.cancel();
        throw new NeatlogsOtlpError('RESPONSE_TOO_LARGE');
      }
      chunks.push(Buffer.from(value));
    }
    return Buffer.concat(chunks, total);
  } finally {
    signal?.removeEventListener?.('abort', cancelReader);
  }
}

async function boundedFetch(fetchImpl, url, init, timeoutMs, signal, onRequestStarted = () => {}) {
  if (signal?.aborted) throw codedError('REQUEST_ABORTED');
  const controller = new AbortController();
  const onAbort = () => controller.abort();
  signal?.addEventListener?.('abort', onAbort, { once: true });
  let timer;
  let onExternalAbort;
  const operation = (async () => {
    if (signal?.aborted) throw codedError('REQUEST_ABORTED');
    onRequestStarted();
    const response = await fetchImpl(url, { ...init, redirect: 'error', signal: controller.signal });
    const body = await readBounded(response, controller.signal);
    return { response, body };
  })();
  const deadline = new Promise((_, reject) => {
    timer = setTimeout(() => {
      controller.abort();
      reject(codedError('REQUEST_TIMEOUT'));
    }, timeoutMs);
  });
  const cancelled = new Promise((_, reject) => {
    if (!signal) return;
    onExternalAbort = () => {
      controller.abort();
      reject(codedError('REQUEST_ABORTED'));
    };
    if (signal.aborted) onExternalAbort();
    else signal.addEventListener('abort', onExternalAbort, { once: true });
  });
  try {
    const result = await Promise.race([operation, deadline, cancelled]);
    if (signal?.aborted) throw codedError('REQUEST_ABORTED');
    return result;
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener?.('abort', onAbort);
    if (onExternalAbort) signal?.removeEventListener?.('abort', onExternalAbort);
  }
}

function sendResult(status, fields) {
  // `deliveryConfirmed` stays false here by construction: receiver acceptance
  // is not delivery. Only readback verification can confirm a trace.
  return Object.freeze({ status, deliveryConfirmed: false, ...fields });
}

/**
 * Sends one export plan to the configured regional OTLP/HTTP ingest route.
 * Results:
 * - receiver_accepted: HTTP 2xx and zero rejected spans (still unconfirmed);
 * - partially_rejected / rejected: the receiver refused some or all spans;
 * - outcome_unknown: timeout, network failure, 5xx, an unparseable 2xx, or
 *   cancellation after a request starts, so
 *   the spans may or may not have been stored;
 * - not_configured: the send was not configured; aborted: cancellation stopped
 *   before the next attempt started.
 * Retries reuse the identical body and deterministic IDs.
 */
export async function sendNeatlogsPlan(config, plan, options = {}) {
  const limits = bounded(options, SEND_DEFAULTS, SEND_BOUNDS);
  const fetchImpl = options.fetch ?? globalThis.fetch;
  const signal = options.signal;
  const sleepImpl = options.sleep ?? sleep;
  const now = options.now ?? Date.now;
  const expectedSpans = plan.traces.length * 2;
  if (plan.traces.length < 1 || plan.traces.length > MAX_TRACES) {
    return sendResult('rejected', { reasonCode: 'INVALID_PLAN', attempts: 0, expectedSpans });
  }
  if (!config?.summary?.ingestReady) {
    return sendResult('not_configured', { reasonCode: 'MISSING_INGEST_KEY', attempts: 0, expectedSpans });
  }
  const canonicalBody = encodeNeatlogsExportRequest(plan);
  const body = options.encodedBody === undefined ? canonicalBody : Buffer.from(options.encodedBody);
  if (!body.equals(canonicalBody)) throw new NeatlogsOtlpError('PERSISTED_BODY_MISMATCH');
  const url = `${config.summary.ingestOrigin}/v1/traces`;
  let attempts = 0;
  let last = { reasonCode: 'NOT_ATTEMPTED', httpStatus: null };
  let anyAmbiguous = false;
  let firstAmbiguous = null;
  let retryAfterMinimumMs = null;
  const refusalResult = (reasonCode, httpStatus) => anyAmbiguous
    ? sendResult('outcome_unknown', {
      reasonCode: 'PRIOR_ATTEMPT_OUTCOME_UNKNOWN', attempts, expectedSpans,
      httpStatus: firstAmbiguous?.httpStatus ?? null,
      lastResponse: Object.freeze({ reasonCode, httpStatus })
    })
    : sendResult('rejected', { reasonCode, attempts, expectedSpans, httpStatus });
  const uncertainResult = (reasonCode, httpStatus, lastResponse = null) => sendResult('outcome_unknown', {
    reasonCode, attempts, expectedSpans, httpStatus,
    ...(lastResponse ? { lastResponse: Object.freeze(lastResponse) } : {})
  });
  while (attempts < limits.maxAttempts) {
    if (signal?.aborted) {
      return sendResult(anyAmbiguous ? 'outcome_unknown' : 'aborted', {
        reasonCode: 'ABORTED', attempts, expectedSpans, httpStatus: last.httpStatus
      });
    }
    attempts += 1;
    let requestStarted = false;
    let delay = limits.retryDelayMs * 2 ** (attempts - 1);
    try {
      const { response, body: responseBody } = await boundedFetch(fetchImpl, url, {
        method: 'POST',
        headers: {
          'content-type': 'application/x-protobuf',
          'x-api-key': config.ingestKey()
        },
        body
      }, limits.timeoutMs, signal, () => {
        options.onPostStart?.(Object.freeze({ ordinal: attempts, bodyBytes: body.length }));
        requestStarted = true;
      });
      const httpStatus = response.status;
      if (httpStatus >= 200 && httpStatus < 300) {
        let parsed;
        try {
          parsed = decodeNeatlogsExportResponse(responseBody, response.headers?.get?.('content-type') ?? '');
        } catch (error) {
          return sendResult('outcome_unknown', {
            reasonCode: error?.code ?? 'MALFORMED_RESPONSE', attempts, expectedSpans, httpStatus
          });
        }
        if (parsed.rejectedSpans > 0) {
          if (anyAmbiguous) {
            return uncertainResult('PRIOR_ATTEMPT_OUTCOME_UNKNOWN', firstAmbiguous?.httpStatus ?? httpStatus, {
              reasonCode: 'RECEIVER_REJECTED_SPANS', httpStatus,
              rejectedSpans: Math.min(parsed.rejectedSpans, expectedSpans)
            });
          }
          return sendResult('partially_rejected', {
            reasonCode: 'RECEIVER_REJECTED_SPANS',
            attempts,
            expectedSpans,
            httpStatus,
            rejectedSpans: Math.min(parsed.rejectedSpans, expectedSpans),
            receiverMessagePresent: parsed.receiverMessagePresent
          });
        }
        return sendResult('receiver_accepted', {
          reasonCode: null,
          attempts,
          expectedSpans,
          httpStatus,
          rejectedSpans: 0,
          receiverMessagePresent: parsed.receiverMessagePresent
        });
      }
      if (httpStatus === 401 || httpStatus === 403) {
        return refusalResult('CREDENTIAL_REJECTED', httpStatus);
      }
      if (httpStatus === 429) {
        last = { reasonCode: 'RATE_LIMITED', httpStatus };
        const retry = retryDelay(response, delay, now());
        delay = retry.milliseconds;
        if (retry.serverMinimum) retryAfterMinimumMs = retry.milliseconds;
        if (retry.serverMinimum && delay > MAX_RETRY_AFTER_MS) {
          return anyAmbiguous
            ? sendResult('outcome_unknown', {
              reasonCode: 'PRIOR_ATTEMPT_OUTCOME_UNKNOWN', attempts, expectedSpans,
              httpStatus: firstAmbiguous?.httpStatus ?? null,
              lastResponse: Object.freeze(last), retryAfterMinimumMs: delay
            })
            : sendResult('not_accepted', {
              reasonCode: 'RETRY_AFTER_EXCEEDS_BOUND', attempts, expectedSpans,
              httpStatus, retryAfterMinimumMs: delay
            });
        }
      } else if (httpStatus >= 500) {
        anyAmbiguous = true;
        last = { reasonCode: 'RECEIVER_UNAVAILABLE', httpStatus };
        if (firstAmbiguous === null) firstAmbiguous = { ...last };
        const retry = retryDelay(response, delay, now());
        delay = retry.milliseconds;
        if (retry.serverMinimum) retryAfterMinimumMs = retry.milliseconds;
        if (retry.serverMinimum && delay > MAX_RETRY_AFTER_MS) {
          return sendResult('outcome_unknown', {
            reasonCode: 'RETRY_AFTER_EXCEEDS_BOUND', attempts, expectedSpans, httpStatus,
            lastResponse: Object.freeze(last), retryAfterMinimumMs: delay
          });
        }
      } else {
        return refusalResult('RECEIVER_REFUSED_REQUEST', httpStatus);
      }
    } catch (error) {
      if (!requestStarted) throw error;
      if (signal?.aborted || error?.code === 'REQUEST_ABORTED') {
        return sendResult(anyAmbiguous || requestStarted ? 'outcome_unknown' : 'aborted', {
          reasonCode: 'ABORTED', attempts, expectedSpans,
          httpStatus: requestStarted ? null : last.httpStatus
        });
      }
      if (error?.code === 'RESPONSE_TOO_LARGE') {
        anyAmbiguous = true;
        last = { reasonCode: 'RESPONSE_TOO_LARGE', httpStatus: null };
      } else {
        // A timeout or connection loss after the request was written leaves
        // the remote outcome unknown.
        anyAmbiguous = true;
        last = { reasonCode: error?.code === 'REQUEST_TIMEOUT' ? 'REQUEST_TIMEOUT' : 'TRANSPORT_FAILED', httpStatus: null };
      }
      if (firstAmbiguous === null) firstAmbiguous = { ...last };
    }
    if (attempts < limits.maxAttempts && !(await sleepImpl(Math.min(delay, MAX_RETRY_AFTER_MS), signal))) {
      return sendResult(anyAmbiguous ? 'outcome_unknown' : 'aborted', {
        reasonCode: 'ABORTED', attempts, expectedSpans, httpStatus: last.httpStatus
      });
    }
  }
  return sendResult(anyAmbiguous ? 'outcome_unknown' : 'not_accepted', {
    reasonCode: last.reasonCode,
    attempts,
    expectedSpans,
    httpStatus: anyAmbiguous ? (firstAmbiguous?.httpStatus ?? last.httpStatus) : last.httpStatus,
    ...(anyAmbiguous ? { lastResponse: Object.freeze({ ...last }) } : {}),
    ...(retryAfterMinimumMs === null ? {} : { retryAfterMinimumMs })
  });
}

class ReadbackStop extends Error {
  constructor(code, httpStatus = null) {
    super(code);
    this.code = code;
    this.httpStatus = httpStatus;
  }
}

function createReader(config, fetchImpl, limits, signal, sleepImpl, now, onRequestStarted) {
  const base = `${config.summary.appOrigin}/api/v1/public`;
  return async function get(path, { allowMissing = false } = {}) {
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      if (signal?.aborted) throw new ReadbackStop('READBACK_ABORTED');
      let result;
      try {
        result = await boundedFetch(fetchImpl, `${base}${path}`, {
          method: 'GET',
          headers: {
            accept: 'application/json',
            authorization: `Bearer ${config.readbackToken()}`,
            'x-project-id': config.summary.projectId
          }
        }, limits.timeoutMs, signal, () => onRequestStarted?.(path));
      } catch (error) {
        if (error?.code === 'LEASE_LOST') throw new ReadbackStop('READBACK_LEASE_LOST');
        if (signal?.aborted) throw new ReadbackStop('READBACK_ABORTED');
        if (attempt === 3) throw new ReadbackStop('READBACK_TRANSPORT_FAILED');
        if (!(await sleepImpl(500 * attempt, signal))) throw new ReadbackStop('READBACK_ABORTED');
        continue;
      }
      const { response, body } = result;
      const status = response.status;
      if (status === 401 || status === 403) throw new ReadbackStop('READBACK_UNAUTHORIZED', status);
      if (status === 404 && allowMissing) return { status, data: null };
      if (status === 409 && allowMissing) return { status, data: null };
      if (status === 429 || status === 503) {
        if (attempt === 3) throw new ReadbackStop('READBACK_RATE_LIMITED', status);
        const retry = retryDelay(response, 1_000 * attempt, now());
        if (retry.serverMinimum && retry.milliseconds > MAX_RETRY_AFTER_MS) {
          throw new ReadbackStop('READBACK_RETRY_AFTER_EXCEEDS_BOUND', status);
        }
        if (!(await sleepImpl(Math.min(retry.milliseconds, MAX_RETRY_AFTER_MS), signal))) {
          throw new ReadbackStop('READBACK_ABORTED');
        }
        continue;
      }
      if (status !== 200) throw new ReadbackStop('READBACK_HTTP_ERROR', status);
      let parsed;
      try {
        parsed = parseNeatlogsJson(body.toString('utf8'));
      } catch {
        throw new ReadbackStop('READBACK_MALFORMED_RESPONSE', status);
      }
      if (parsed?.success !== true || parsed.data === null || typeof parsed.data !== 'object'
          || Array.isArray(parsed.data)) {
        throw new ReadbackStop('READBACK_MALFORMED_RESPONSE', status);
      }
      return { status, data: parsed.data, requestId: typeof parsed.requestId === 'string' ? parsed.requestId : null };
    }
    throw new ReadbackStop('READBACK_TRANSPORT_FAILED');
  };
}

function compareTrace(plan, expected, trace, spans, payload) {
  const problems = [];
  if (!Array.isArray(spans)) {
    spans = [];
    problems.push('SPANS_MALFORMED');
  }
  if (trace.traceId !== expected.traceId) problems.push('TRACE_ID_MISMATCH');
  if (trace.sessionId !== expected.sessionId) problems.push('SESSION_ID_MISMATCH');
  if (trace.workflowName !== plan.workflowName) problems.push('WORKFLOW_NAME_MISMATCH');
  const tags = Array.isArray(trace.tags) ? trace.tags : [];
  if (!plan.tags.every(tag => tags.includes(tag))) problems.push('TAGS_MISSING');
  const records = spans.filter(span => span !== null && typeof span === 'object' && !Array.isArray(span));
  if (records.length !== spans.length || records.some(span => typeof span.spanId !== 'string'
      || span.traceId !== expected.traceId)) problems.push('SPAN_IDENTITY_MISMATCH');
  const ids = records.map(span => span.spanId);
  if (new Set(ids).size !== ids.length) problems.push('DUPLICATE_SPANS_OBSERVED');
  const root = records.filter(span => span.spanId === expected.rootSpanId);
  if (root.length !== 1 || root[0].parentSpanId !== null
      || root[0].spanName !== NEATLOGS_LOCAL_SPAN_NAME || root[0].traceId !== expected.traceId) {
    problems.push('ROOT_SPAN_MISMATCH');
  }
  const marker = records.filter(span => span.spanId === expected.markerSpanId);
  if (marker.length > 1 || (marker.length === 1
      && (marker[0].parentSpanId !== expected.rootSpanId
        || marker[0].spanName !== NEATLOGS_COMPLETION_SPAN_NAME
        || marker[0].traceId !== expected.traceId))) {
    problems.push('MARKER_SPAN_MISMATCH');
  }
  const unexpected = records.filter(span => span.spanId !== expected.rootSpanId
    && span.spanId !== expected.markerSpanId);
  if (unexpected.length > 0) problems.push('UNEXPECTED_SPANS');
  if (trace.spansCount !== spans.length) problems.push('SPANS_COUNT_MISMATCH');
  if (payload?.traceId !== expected.traceId || payload?.spanId !== expected.rootSpanId) {
    problems.push('PAYLOAD_IDENTITY_MISMATCH');
  }
  let payloadMatches = false;
  const input = payload?.fields?.input;
  if (input?.available === true && input.truncated === false && typeof input.value === 'string') {
    payloadMatches = input.value === expected.input;
  }
  if (!payloadMatches) problems.push('INPUT_PAYLOAD_MISMATCH');
  return {
    problems,
    markerStored: marker.length === 1,
    spansCount: Number.isSafeInteger(trace.spansCount) ? trace.spansCount : null
  };
}

/**
 * Reads every planned trace back through the Public API with a service-account
 * token bound to the configured project, polling until finalized. A trace is
 * `verified` only when its IDs, session, workflow, tags, span tree and exported
 * input all match what was sent.
 */
export async function readbackNeatlogsPlan(config, plan, options = {}) {
  const limits = bounded(options, READBACK_DEFAULTS, READBACK_BOUNDS);
  const fetchImpl = options.fetch ?? globalThis.fetch;
  const signal = options.signal;
  const sleepImpl = options.sleep ?? sleep;
  const now = options.now ?? Date.now;
  if (!config?.summary?.readbackReady) {
    return Object.freeze({
      status: 'readback_unavailable',
      reasonCode: config?.summary?.readbackTokenConfigured ? 'MISSING_PROJECT_ID' : 'MISSING_READBACK_TOKEN',
      traces: Object.freeze([])
    });
  }
  let currentTraceId = null;
  const attemptedTraceIds = [];
  const attempted = new Set();
  const outcomes = new Map();
  const get = createReader(config, fetchImpl, limits, signal, sleepImpl, now, path => {
    if (currentTraceId && !attempted.has(currentTraceId)) {
      attempted.add(currentTraceId);
      attemptedTraceIds.push(currentTraceId);
    }
    options.onRequestStarted?.(path, currentTraceId);
  });
  try {
    const project = await get('/project');
    if (project.data.id !== config.summary.projectId) {
      return Object.freeze({ status: 'mismatch', reasonCode: 'PROJECT_MISMATCH', traces: Object.freeze([]) });
    }
    for (const trace of plan.traces) outcomes.set(trace.traceId, {
      traceId: trace.traceId,
      rootSpanId: trace.rootSpanId,
      sessionId: trace.sessionId,
      sequence: trace.sequence,
      eventType: trace.eventType,
      revision: trace.revision,
      fence: trace.fence,
      generation: trace.generation ?? 0,
      status: 'pending',
      problems: ['NOT_FOUND'],
      pollsObserved: 0,
      notFoundPolls: 0,
      conflictPolls: 0,
      priorGenerationTraceIds: [],
      priorGenerationFinalizedTraceIds: [],
      priorGenerationConflict: false,
      priorGenerationIdentityMismatch: false,
      duplicateTraceIds: []
    });
    const observePriorGenerations = async expected => {
      const outcome = outcomes.get(expected.traceId);
      for (const priorId of expected.priorTraceIds ?? []) {
        const prior = await get(`/traces/${priorId}`, { allowMissing: true });
        if (prior.data !== null) {
          if (prior.data.traceId === priorId) {
            if (!outcome.priorGenerationTraceIds.includes(priorId)) {
              outcome.priorGenerationTraceIds.push(priorId);
            }
            if (prior.data.finalizationStatus === 'finalized'
                && !outcome.priorGenerationFinalizedTraceIds.includes(priorId)) {
              outcome.priorGenerationFinalizedTraceIds.push(priorId);
            }
          } else {
            outcome.priorGenerationIdentityMismatch = true;
          }
        } else if (prior.status === 409) {
          outcome.priorGenerationConflict = true;
        }
      }
    };
    for (let poll = 1; poll <= limits.maxPolls; poll += 1) {
      for (const expected of plan.traces) {
        const outcome = outcomes.get(expected.traceId);
        if (outcome.status === 'verified' || outcome.status === 'mismatch') continue;
        currentTraceId = expected.traceId;
        outcome.pollsObserved += 1;
        const trace = await get(`/traces/${expected.traceId}`, { allowMissing: true });
        if (signal?.aborted) throw new ReadbackStop('READBACK_ABORTED');
        if (trace.data === null) {
          if (trace.status === 409) {
            outcome.conflictPolls += 1;
            outcome.problems = ['TRACE_CONFLICT'];
          } else {
            await observePriorGenerations(expected);
            if (outcome.priorGenerationTraceIds.length > 0) {
              outcome.problems = ['PRIOR_GENERATION_FOUND'];
            } else if (outcome.priorGenerationConflict) {
              outcome.problems = ['PRIOR_GENERATION_CONFLICT'];
            } else if (outcome.priorGenerationIdentityMismatch) {
              outcome.problems = ['PRIOR_GENERATION_IDENTITY_MISMATCH'];
            } else {
              outcome.notFoundPolls += 1;
              outcome.problems = outcome.conflictPolls > 0 ? ['TRACE_CONFLICT'] : ['NOT_FOUND'];
            }
          }
          continue;
        }
        const finalizationStatus = ['pending', 'finalized', 'dlq'].includes(trace.data.finalizationStatus)
          ? trace.data.finalizationStatus : null;
        outcome.finalizationStatus = finalizationStatus;
        if (finalizationStatus === 'dlq') {
          Object.assign(outcome, { status: 'mismatch', problems: ['TRACE_DEAD_LETTERED'] });
          continue;
        }
        if (finalizationStatus !== 'finalized') {
          outcome.problems = ['NOT_FINALIZED'];
          continue;
        }
        const spans = await get(`/traces/${expected.traceId}/spans?limit=50`, { allowMissing: true });
        if (signal?.aborted) throw new ReadbackStop('READBACK_ABORTED');
        if (spans.data === null) {
          outcome.problems = ['PROJECTION_NOT_READY'];
          continue;
        }
        const payload = await get(
          `/traces/${expected.traceId}/spans/${expected.rootSpanId}/payload?fields=input`,
          { allowMissing: true }
        );
        if (signal?.aborted) throw new ReadbackStop('READBACK_ABORTED');
        if (payload.data === null) {
          outcome.problems = ['PROJECTION_NOT_READY'];
          continue;
        }
        const page = spans.data.page;
        const pageIsValid = page !== null && typeof page === 'object' && !Array.isArray(page)
          && typeof page.hasMore === 'boolean' && page.hasMore === false
          && Object.hasOwn(page, 'nextCursor') && page.nextCursor === null
          && Number.isSafeInteger(page.limit) && page.limit === 50;
        const spanList = Array.isArray(spans.data.spans) && spans.data.spans.length <= 50
          ? spans.data.spans : null;
        const comparison = compareTrace(plan, expected, trace.data, spanList ?? [], payload.data);
        if (!pageIsValid) comparison.problems.push('SPANS_PAGE_INCOMPLETE');
        if (spanList === null) comparison.problems.push('SPANS_PAGE_MALFORMED');
        // An earlier generation of the same event that did get stored means
        // the event now exists twice remotely. Report it; do not hide it.
        await observePriorGenerations(expected);
        outcome.duplicateTraceIds = [...outcome.priorGenerationFinalizedTraceIds];
        if (outcome.priorGenerationConflict) comparison.problems.push('PRIOR_GENERATION_CONFLICT');
        if (outcome.priorGenerationIdentityMismatch) comparison.problems.push('PRIOR_GENERATION_IDENTITY_MISMATCH');
        Object.assign(outcome, {
          status: comparison.problems.length === 0 ? 'verified'
            : comparison.problems.every(problem => problem === 'PRIOR_GENERATION_CONFLICT') ? 'pending' : 'mismatch',
          problems: comparison.problems,
          markerStored: comparison.markerStored,
          spansCount: comparison.spansCount,
        });
      }
      const pending = [...outcomes.values()].some(outcome => outcome.status === 'pending');
      if (!pending || poll === limits.maxPolls) break;
      if (!(await sleepImpl(limits.pollIntervalMs, signal))) throw new ReadbackStop('READBACK_ABORTED');
    }
    // Repeated 404 is bounded-window `missing`, not permanent loss. Conflicts
    // and projections that are not ready remain pending.
    for (const outcome of outcomes.values()) {
      if (outcome.status === 'pending' && outcome.problems.includes('NOT_FOUND')
          && outcome.notFoundPolls === outcome.pollsObserved) {
        outcome.status = 'missing';
        outcome.observationWindowPolls = outcome.pollsObserved;
        outcome.permanentLossEstablished = false;
      }
      delete outcome.pollsObserved;
      delete outcome.notFoundPolls;
      delete outcome.conflictPolls;
    }
    const traces = Object.freeze([...outcomes.values()].map(outcome => Object.freeze({
      ...outcome,
      problems: Object.freeze([...outcome.problems]),
      duplicateTraceIds: Object.freeze([...outcome.duplicateTraceIds])
    })));
    const status = traces.every(trace => trace.status === 'verified')
      ? 'verified'
      : traces.some(trace => trace.status === 'mismatch') ? 'mismatch'
        : traces.some(trace => trace.status === 'pending') ? 'pending' : 'missing';
    const reasonCodes = {
      verified: null, pending: 'NOT_YET_READABLE', missing: 'NOT_FOUND_AFTER_POLLS', mismatch: 'READBACK_MISMATCH'
    };
    return Object.freeze({
      status,
      reasonCode: reasonCodes[status],
      duplicateEventDeliveries: traces.filter(trace => trace.duplicateTraceIds.length > 0).length,
      project: Object.freeze({ id: config.summary.projectId }),
      traces
    });
  } catch (error) {
    if (error instanceof ReadbackStop) {
      const traces = attemptedTraceIds.map(traceId => {
        const outcome = outcomes.get(traceId);
        if (outcome?.status === 'pending') outcome.problems = [error.code];
        return Object.freeze({
          ...outcome,
          problems: Object.freeze([...(outcome?.problems ?? [])]),
          duplicateTraceIds: Object.freeze([...(outcome?.duplicateTraceIds ?? [])])
        });
      });
      return Object.freeze({
        status: 'readback_failed',
        reasonCode: error.code,
        httpStatus: error.httpStatus,
        attemptedTraceIds: Object.freeze([...attemptedTraceIds]),
        traces: Object.freeze(traces)
      });
    }
    throw error;
  }
}

export const NEATLOGS_LIVE_LIMITS = Object.freeze({
  send: Object.freeze({ defaults: SEND_DEFAULTS, bounds: SEND_BOUNDS }),
  readback: Object.freeze({ defaults: READBACK_DEFAULTS, bounds: READBACK_BOUNDS }),
  maxRetryAfterMs: MAX_RETRY_AFTER_MS,
  maxReadBytes: MAX_READ_BYTES,
  maxTraces: MAX_TRACES
});
