import { createHash } from 'node:crypto';
import { NEATLOGS_REGIONS } from './neatlogs-config.js';
import {
  canonicalNeatlogsInput, deterministicNeatlogsIds, encodeNeatlogsExportRequest,
  NEATLOGS_LIVE_WORKFLOW_NAME, NEATLOGS_OTLP_LIMITS, validateNeatlogsTags
} from './neatlogs-otlp.js';

// Private durable wire snapshots. These records confer no remote delivery or
// financial authority. They bind a bounded, sanitized body to its records and
// generation so reopening a batch never needs another SDK capture for a retry.
export const OUTBOX_INTENT_SCHEMA = 'agent-harness.neatlogs.send-intent.v1';
export const OUTBOX_INTENT_LIMITS = Object.freeze({ intents: 4, calls: 32, posts: 8 });
const HASH = /^[0-9a-f]{64}$/;
const TIME = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
const SEND_STATUS = new Set([
  'receiver_accepted', 'partially_rejected', 'rejected', 'outcome_unknown',
  'not_accepted', 'aborted', 'not_configured'
]);
const RESULT_FIELDS = new Set([
  'status', 'deliveryConfirmed', 'reasonCode', 'attempts', 'expectedSpans',
  'httpStatus', 'lastResponse', 'rejectedSpans', 'receiverMessagePresent', 'retryAfterMinimumMs'
]);

function invalid() {
  const error = new Error('Neatlogs persisted dispatch intent is invalid.');
  error.code = 'DISPATCH_INTENT_INVALID';
  throw error;
}

function keys(value, required, optional = []) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)
      || required.some(key => !Object.hasOwn(value, key))
      || Object.keys(value).some(key => !required.includes(key) && !optional.includes(key))) invalid();
}

function timestamp(value) {
  if (typeof value !== 'string' || !TIME.test(value) || !Number.isFinite(Date.parse(value))) invalid();
}

function spanTime(value) {
  keys(value, ['start', 'end']);
  for (const endpoint of [value.start, value.end]) {
    // SDK timestamps are representable integer nanoseconds, above the safe
    // integer range. JSON round trips preserve the same Number/uint64 bytes.
    if (!Number.isInteger(endpoint) || endpoint < 0 || BigInt(endpoint) > 0xffffffffffffffffn) invalid();
  }
  if (value.end < value.start) invalid();
  return Object.freeze({ start: value.start, end: value.end });
}

export function intentTarget(config) {
  const { region, ingestOrigin, appOrigin, projectId } = config?.summary ?? {};
  if (!Object.hasOwn(NEATLOGS_REGIONS, region)
      || NEATLOGS_REGIONS[region].ingest !== ingestOrigin
      || NEATLOGS_REGIONS[region].app !== appOrigin
      || (projectId !== null && (typeof projectId !== 'string'
        || !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(projectId)))) invalid();
  return Object.freeze({ region, ingestOrigin, appOrigin,
    projectHash: projectId === null ? null : createHash('sha256').update(projectId).digest('hex') });
}

function validateTarget(target) {
  keys(target, ['region', 'ingestOrigin', 'appOrigin', 'projectHash']);
  if (!Object.hasOwn(NEATLOGS_REGIONS, target.region)
      || NEATLOGS_REGIONS[target.region].ingest !== target.ingestOrigin
      || NEATLOGS_REGIONS[target.region].app !== target.appOrigin
      || (target.projectHash !== null && (typeof target.projectHash !== 'string' || !HASH.test(target.projectHash)))) invalid();
}

export function assertIntentTarget(intent, config) {
  if (JSON.stringify(intent.target) !== JSON.stringify(intentTarget(config))) {
    const error = new Error('Neatlogs dispatch destination changed.');
    error.code = 'DISPATCH_TARGET_CONFLICT';
    throw error;
  }
}

function validReason(value) { return value === null || (typeof value === 'string' && /^[A-Z][A-Z0-9_]{0,79}$/.test(value)); }
function validHttp(value) { return value === null || (Number.isInteger(value) && value >= 100 && value <= 599); }

function validateResult(result, expectedSpans) {
  keys(result, ['status', 'deliveryConfirmed', 'reasonCode', 'attempts', 'expectedSpans'], [...RESULT_FIELDS]);
  if (!SEND_STATUS.has(result.status) || result.deliveryConfirmed !== false
      || !validReason(result.reasonCode) || !Number.isSafeInteger(result.attempts)
      || result.attempts < 0 || result.attempts > 5 || result.expectedSpans !== expectedSpans) invalid();
  if (Object.hasOwn(result, 'httpStatus') && !validHttp(result.httpStatus)) invalid();
  if (Object.hasOwn(result, 'rejectedSpans') && (!Number.isSafeInteger(result.rejectedSpans)
      || result.rejectedSpans < 0 || result.rejectedSpans > expectedSpans)) invalid();
  if (Object.hasOwn(result, 'receiverMessagePresent') && typeof result.receiverMessagePresent !== 'boolean') invalid();
  if (Object.hasOwn(result, 'retryAfterMinimumMs') && (!Number.isSafeInteger(result.retryAfterMinimumMs)
      || result.retryAfterMinimumMs < 0)) invalid();
  if (Object.hasOwn(result, 'lastResponse')) {
    keys(result.lastResponse, ['reasonCode', 'httpStatus'], ['rejectedSpans']);
    if (!validReason(result.lastResponse.reasonCode) || !validHttp(result.lastResponse.httpStatus)) invalid();
    if (Object.hasOwn(result.lastResponse, 'rejectedSpans') && (!Number.isSafeInteger(result.lastResponse.rejectedSpans)
        || result.lastResponse.rejectedSpans < 0 || result.lastResponse.rejectedSpans > expectedSpans)) invalid();
  }
}

export function createOutboxIntent(plan, config, purpose, previousTraceIds = []) {
  const body = encodeNeatlogsExportRequest(plan);
  return {
    schema: OUTBOX_INTENT_SCHEMA, purpose, target: intentTarget(config),
    tags: [...plan.tags],
    traces: plan.traces.map(trace => ({ sequence: trace.sequence, generation: trace.generation,
      root: { ...trace.root }, marker: { ...trace.marker } })),
    previousTraceIds: [...previousTraceIds],
    bodyBase64: body.toString('base64'), bodySha256: createHash('sha256').update(body).digest('hex'),
    createdAt: new Date().toISOString(), calls: []
  };
}

export function restoreOutboxIntents(records, intents) {
  if (!Array.isArray(intents) || intents.length > OUTBOX_INTENT_LIMITS.intents) invalid();
  const bySequence = new Map(records.map(record => [record.sequence, record]));
  const current = new Map();
  const restored = [];
  for (const [index, intent] of intents.entries()) {
    keys(intent, ['schema', 'purpose', 'target', 'tags', 'traces', 'previousTraceIds',
      'bodyBase64', 'bodySha256', 'createdAt', 'calls']);
    if (intent.schema !== OUTBOX_INTENT_SCHEMA || intent.purpose !== (index === 0 ? 'initial' : 'recovery')) invalid();
    validateTarget(intent.target);
    if (index > 0 && JSON.stringify(intent.target) !== JSON.stringify(intents[0].target)) invalid();
    timestamp(intent.createdAt);
    const tags = validateNeatlogsTags(intent.tags);
    if (tags.includes('evidence:synthetic') || (index > 0 && JSON.stringify(tags) !== JSON.stringify(intents[0].tags))) invalid();
    if (!Array.isArray(intent.traces) || intent.traces.length < 1 || intent.traces.length > records.length
        || (index === 0 && intent.traces.length !== records.length)) invalid();
    let previousSequence = 0;
    const oldIds = [];
    const traces = intent.traces.map(trace => {
      keys(trace, ['sequence', 'generation', 'root', 'marker']);
      const record = bySequence.get(trace.sequence);
      if (!record || trace.sequence <= previousSequence) invalid();
      previousSequence = trace.sequence;
      const previous = current.get(trace.sequence);
      if (trace.generation !== (previous ? previous.generation + 1 : 0)) invalid();
      if (previous) oldIds.push(previous.traceId);
      const result = Object.freeze({
        ...deterministicNeatlogsIds(record.eventId, trace.generation), generation: trace.generation,
        priorTraceIds: Object.freeze(Array.from({ length: trace.generation },
          (_, prior) => deterministicNeatlogsIds(record.eventId, prior).traceId)),
        sessionId: record.workflowId, eventToken: record.eventId, sequence: record.sequence,
        revision: record.revision, eventType: record.eventType, fence: record.fence,
        input: canonicalNeatlogsInput(record), record,
        root: spanTime(trace.root), marker: spanTime(trace.marker)
      });
      current.set(trace.sequence, result);
      return result;
    });
    if (JSON.stringify(intent.previousTraceIds) !== JSON.stringify(oldIds)) invalid();
    const plan = Object.freeze({ workflowName: NEATLOGS_LIVE_WORKFLOW_NAME, tags, traces: Object.freeze(traces) });
    if (typeof intent.bodyBase64 !== 'string' || intent.bodyBase64.length > Math.ceil(NEATLOGS_OTLP_LIMITS.requestBytes / 3) * 4
        || typeof intent.bodySha256 !== 'string' || !HASH.test(intent.bodySha256)) invalid();
    const body = Buffer.from(intent.bodyBase64, 'base64');
    if (body.toString('base64') !== intent.bodyBase64 || body.length > NEATLOGS_OTLP_LIMITS.requestBytes
        || createHash('sha256').update(body).digest('hex') !== intent.bodySha256
        || !body.equals(encodeNeatlogsExportRequest(plan))) invalid();
    if (!Array.isArray(intent.calls) || intent.calls.length > OUTBOX_INTENT_LIMITS.calls) invalid();
    let posts = 0;
    for (const [callIndex, call] of intent.calls.entries()) {
      keys(call, ['ordinal', 'startedAt', 'postsStarted', 'completedAt', 'result']);
      if (call.ordinal !== callIndex + 1 || !Number.isSafeInteger(call.postsStarted)
          || call.postsStarted < 0 || call.postsStarted > 5) invalid();
      timestamp(call.startedAt);
      if (call.completedAt !== null) timestamp(call.completedAt);
      if ((call.result === null) !== (call.completedAt === null)) invalid();
      if (call.result !== null) {
        validateResult(call.result, traces.length * 2);
        if (call.result.attempts !== call.postsStarted) invalid();
      }
      posts += call.postsStarted;
    }
    if (posts > OUTBOX_INTENT_LIMITS.posts) invalid();
    restored.push(Object.freeze({ intent, plan, body, posts }));
  }
  return Object.freeze({ restored: Object.freeze(restored), currentPlan: intents.length === 0 ? null : Object.freeze({
    workflowName: NEATLOGS_LIVE_WORKFLOW_NAME, tags: Object.freeze([...intents[0].tags]),
    traces: Object.freeze(records.map(record => current.get(record.sequence)))
  }) });
}

export function summarizeOutboxIntent(intent) {
  // An interrupted call that never recorded a possible POST adds no effect.
  // Keep the latest known result for this intent, or no summary at all when
  // nothing has completed or possibly been sent. Do not invent a call result.
  const latest = intent.calls.findLast(call => call.result !== null || call.postsStarted > 0);
  if (!latest) return null;
  const uncertain = intent.calls.some(call => (call.postsStarted > 0 && call.result === null)
    || call.result?.status === 'outcome_unknown');
  const result = latest.result;
  if (result && (!uncertain || result.status === 'receiver_accepted')) {
    return { ...result, sentAt: latest.completedAt, priorOutcomeUnknown: uncertain };
  }
  return { status: 'outcome_unknown', deliveryConfirmed: false,
    reasonCode: result ? 'PRIOR_ATTEMPT_OUTCOME_UNKNOWN' : 'DISPATCH_RESULT_NOT_RECORDED',
    attempts: intent.calls.reduce((sum, call) => sum + call.postsStarted, 0),
    expectedSpans: intent.traces.length * 2, priorOutcomeUnknown: true,
    ...(result ? { lastResult: { ...result }, sentAt: latest.completedAt } : {}) };
}
