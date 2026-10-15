import { createHmac, randomUUID } from 'node:crypto';
import { isProxy } from 'node:util/types';
import { loadNeatlogsConfig } from '../../observability/private/neatlogs-config.js';
import {
  NEATLOGS_TICKETSTUB_WORKFLOW_NAME,
  NeatlogsDeliveryQueue,
  drainNeatlogsDeliveryQueue,
  validateNeatlogsQueueEntry
} from '../../observability/private/neatlogs-delivery-queue.js';
import { validateNeatlogsTags } from '../../observability/private/neatlogs-otlp.js';

// TicketStub hand-off: the customer app gives the harness its already
// sanitized product events (schema ticketstub.event/1), and the harness
// records them in its durable Neatlogs outbox and delivers them.
//
// The harness does not trust the producer's sanitizer. Every event is checked
// again against this copy of the ticketstub.event/1 contract and refused, not
// repaired, when anything is outside it. Request IDs and the producer's
// 16-hex refs are replaced by domain-separated HMAC tokens under the key
// explicitly injected by the host.

export const TICKETSTUB_EVENT_SCHEMA = 'ticketstub.event/1';
export const TICKETSTUB_INTAKE_SCHEMA = 'agent-harness.ticketstub-intake.v2';
export const HARNESS_TELEMETRY_KEY_NAME = 'AGENT_HARNESS_TELEMETRY_KEY';

const ENUM = values => Object.freeze(values);
/** Field kinds per event type, matching getticketstub.com src/server/events.ts. */
export const TICKETSTUB_EVENT_SPEC = Object.freeze({
  intake_submitted: Object.freeze({ source: ENUM(['link', 'search']) }),
  intake_rejected: Object.freeze({ code: 'code' }),
  research_started: Object.freeze({ source: ENUM(['fixture', 'link', 'ticketmaster']), facts: 'count' }),
  fact_corrected: Object.freeze({ fact: 'ref', versions: 'count' }),
  art_requested: Object.freeze({ first: 'flag' }),
  art_ready: Object.freeze({ provider: ENUM(['procedural', 'image', 'simulated', 'live']) }),
  art_failed: Object.freeze({ code: 'code' }),
  art_limit: Object.freeze({ retryAt: 'ts' }),
  version_selected: Object.freeze({ version: 'ref', versions: 'count' }),
  share_created: Object.freeze({ share: 'ref' }),
  share_revoked: Object.freeze({ revoked: 'flag' }),
  gift_created: Object.freeze({ share: 'ref', messageChars: 'count' }),
  order_placed: Object.freeze({ order: 'ref', lines: 'count', totalMinor: 'count' }),
  order_blocked: Object.freeze({ code: 'code' }),
  signin_link_sent: Object.freeze({ reason: ENUM(['save', 'checkout', 'claim']), delivery: ENUM(['mail', 'sandbox']) }),
  signed_in: Object.freeze({ fresh: 'flag' }),
  claim_redeemed: Object.freeze({ status: ENUM(['claimed', 'already']) }),
  storage_unavailable: Object.freeze({ code: 'code', route: 'code' })
});

const EVENT_KEYS = new Set(['schema', 'type', 'at', 'requestId', 'actor', 'actorRef', 'projectRef', 'mode', 'fields']);
const REF = /^[a-f0-9]{16}$/;
const CODE = /^[a-z][a-z0-9_]{0,47}$/;
const REQUEST_ID = /^[A-Za-z0-9_.-]{8,64}$/;
const MAX_COUNT = 1_000_000_000;
const MAX_EVENTS_PER_CALL = 256;
const EMAIL = /[^\s@"]+@[^\s@"]+\.[^\s@"]+/;
const URLISH = /(?:[a-z][a-z0-9+.-]*:\/\/|www\.|[?&][\w.%-]+=)/i;
const TOKENISH = /[A-Za-z0-9_-]{24,}/;
const KEY_HEX = /^(?:[0-9a-f]{2}){32,64}$/;

export class TicketstubIntakeError extends Error {
  constructor(code) {
    super(`TicketStub intake failed (${code}).`);
    this.name = 'TicketstubIntakeError';
    this.code = code;
  }
}

function plainRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value) && !isProxy(value)
    && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null);
}

function ownDataSnapshot(value, allowedKeys) {
  if (!plainRecord(value)) return null;
  const snapshot = {};
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key !== 'string' || !allowedKeys.has(key)) return null;
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor || !descriptor.enumerable || !Object.hasOwn(descriptor, 'value')) return null;
    snapshot[key] = descriptor.value;
  }
  return snapshot;
}

function clean(value) {
  return !EMAIL.test(value) && !URLISH.test(value) && !TOKENISH.test(value);
}

function fits(kind, value) {
  if (typeof kind !== 'string') return typeof value === 'string' && kind.includes(value);
  switch (kind) {
    case 'ref': return typeof value === 'string' && REF.test(value);
    case 'code': return typeof value === 'string' && CODE.test(value) && clean(value);
    case 'count': return Number.isSafeInteger(value) && value >= 0 && value <= MAX_COUNT;
    case 'ts': return Number.isSafeInteger(value) && value > 0 && value < 8.64e15;
    case 'flag': return typeof value === 'boolean';
    default: return false;
  }
}

/**
 * Validates one ticketstub.event/1 event. Returns a normalized copy, or
 * throws `TicketstubIntakeError` with code `INVALID_EVENT` or
 * `UNSUPPORTED_SCHEMA`. Unknown keys, unknown fields and values outside a
 * field's kind are refused.
 */
export function validateTicketstubEvent(event) {
  const input = ownDataSnapshot(event, EVENT_KEYS);
  if (!input) throw new TicketstubIntakeError('INVALID_EVENT');
  if (input.schema !== TICKETSTUB_EVENT_SCHEMA) throw new TicketstubIntakeError('UNSUPPORTED_SCHEMA');
  if (typeof input.type !== 'string') throw new TicketstubIntakeError('INVALID_EVENT');
  const spec = Object.hasOwn(TICKETSTUB_EVENT_SPEC, input.type) ? TICKETSTUB_EVENT_SPEC[input.type] : null;
  const fields = spec ? ownDataSnapshot(input.fields, new Set(Object.keys(spec))) : null;
  const actorOk = ['account', 'guest', 'anonymous'].includes(input.actor);
  if (!spec || !fields || !Number.isSafeInteger(input.at) || input.at <= 0 || input.at >= 8.64e15
      || typeof input.requestId !== 'string' || !REQUEST_ID.test(input.requestId)
      || !actorOk || !['sandbox', 'live'].includes(input.mode)) {
    throw new TicketstubIntakeError('INVALID_EVENT');
  }
  if (input.actorRef !== undefined
      && (input.actor === 'anonymous' || typeof input.actorRef !== 'string' || !REF.test(input.actorRef))) {
    throw new TicketstubIntakeError('INVALID_EVENT');
  }
  if (input.projectRef !== undefined && (typeof input.projectRef !== 'string' || !REF.test(input.projectRef))) {
    throw new TicketstubIntakeError('INVALID_EVENT');
  }
  const normalizedFields = {};
  for (const key of Object.keys(fields).sort()) {
    if (!Object.hasOwn(spec, key) || !fits(spec[key], fields[key])) {
      throw new TicketstubIntakeError('INVALID_EVENT');
    }
    normalizedFields[key] = fields[key];
  }
  return Object.freeze({
    schema: TICKETSTUB_EVENT_SCHEMA,
    type: input.type,
    at: input.at,
    requestId: input.requestId,
    actor: input.actor,
    ...(input.actorRef !== undefined ? { actorRef: input.actorRef } : {}),
    ...(input.projectRef !== undefined ? { projectRef: input.projectRef } : {}),
    mode: input.mode,
    fields: Object.freeze(normalizedFields)
  });
}

/** Parses the server-held tokenization key: 64 to 128 lowercase hex characters (32 to 64 bytes). */
export function parseHarnessTelemetryKey(value) {
  const text = typeof value === 'string' ? value.trim() : '';
  if (!KEY_HEX.test(text)) throw new TicketstubIntakeError('INVALID_TELEMETRY_KEY');
  return Buffer.from(text, 'hex');
}

const TOKEN_KINDS = new Set([
  'ticketstub.event', 'ticketstub.request', 'ticketstub.actor', 'ticketstub.project',
  'ticketstub.fact', 'ticketstub.version', 'ticketstub.share', 'ticketstub.order'
]);

export function createTicketstubTokenizer(keyBytes) {
  if (!Buffer.isBuffer(keyBytes) || keyBytes.length < 32 || keyBytes.length > 64) {
    throw new TicketstubIntakeError('INVALID_TELEMETRY_KEY');
  }
  const key = Buffer.from(keyBytes);
  return function tokenize(kind, value) {
    if (!TOKEN_KINDS.has(kind) || typeof value !== 'string') throw new TicketstubIntakeError('TOKENIZE_FAILED');
    return 'tok1_' + createHmac('sha256', key).update(kind, 'utf8').update('\0', 'utf8')
      .update(value, 'utf8').digest('hex');
  };
}

/**
 * Builds the outbox entry for one validated event. The exported input holds
 * the event type, time, mode, actor kind, fields, and tokens in place of the
 * request ID and refs. One session groups a project's events (or, without a
 * project, one request's events).
 */
export function ticketstubOutboxEntry(event, tokenize) {
  const valid = validateTicketstubEvent(event);
  const request = tokenize('ticketstub.request', valid.requestId);
  const actor = valid.actorRef === undefined ? null : tokenize('ticketstub.actor', valid.actorRef);
  const project = valid.projectRef === undefined ? null : tokenize('ticketstub.project', valid.projectRef);
  const referenceScope = project ?? request;
  const exportedFields = {};
  for (const [key, value] of Object.entries(valid.fields)) {
    if (TICKETSTUB_EVENT_SPEC[valid.type][key] !== 'ref') {
      exportedFields[key] = value;
      continue;
    }
    const kind = key === 'fact' ? 'fact' : key === 'version' ? 'version' : key === 'share' ? 'share' : key === 'order' ? 'order' : null;
    if (!kind) throw new TicketstubIntakeError('TOKENIZE_FAILED');
    exportedFields[key] = tokenize(`ticketstub.${kind}`, `${referenceScope}\0${value}`);
  }
  const input = JSON.stringify({
    schema: TICKETSTUB_INTAKE_SCHEMA,
    source: TICKETSTUB_EVENT_SCHEMA,
    type: valid.type,
    at: valid.at,
    mode: valid.mode,
    actor: valid.actor,
    actorToken: actor,
    projectToken: project,
    requestToken: request,
    fields: exportedFields
  });
  return validateNeatlogsQueueEntry({
    eventToken: tokenize('ticketstub.event', JSON.stringify(valid)),
    workflowName: NEATLOGS_TICKETSTUB_WORKFLOW_NAME,
    sessionId: project ?? request,
    eventType: valid.type,
    input,
    attributes: [
      ['agent_harness.source', 'ticketstub'],
      ['ticketstub.event_type', valid.type],
      ['ticketstub.mode', valid.mode],
      ['ticketstub.actor', valid.actor]
    ],
    occurredAtMs: valid.at
  });
}

/**
 * The host-facing exporter. The host passes an explicit environment object
 * (normally a copy of its server environment) and an explicit outbox file
 * path. `accept` is synchronous and local; `drain` does the network work.
 * Configuration problems throw at creation with a code and never a value.
 */
export function createTicketstubHarnessExporter({
  env, databasePath, tags = [], fetch, clock, outboxOptions = {}, drainDelayMs = 2_000
} = {}) {
  if (env === null || typeof env !== 'object') throw new TicketstubIntakeError('MISSING_ENV');
  const tokenize = createTicketstubTokenizer(parseHarnessTelemetryKey(env[HARNESS_TELEMETRY_KEY_NAME]));
  const config = loadNeatlogsConfig({ env });
  if (!config.summary.ingestReady) throw new TicketstubIntakeError('MISSING_INGEST_KEY');
  const validatedTags = validateNeatlogsTags(['ticketstub', ...tags.filter(tag => tag !== 'ticketstub')]);
  if (!Number.isSafeInteger(drainDelayMs) || drainDelayMs < 0 || drainDelayMs > 60_000) {
    throw new TicketstubIntakeError('INVALID_OPTION');
  }
  const outbox = NeatlogsDeliveryQueue.open({ databasePath, ...(clock ? { clock } : {}), ...outboxOptions });
  const ownerId = `ticketstub-${randomUUID()}`;
  const counters = { accepted: 0, duplicates: 0, refused: 0, conflicts: 0, invalid: 0, drains: 0, drainErrors: 0 };
  let timer = null;
  let running = null;
  let scheduledWork = Promise.resolve();
  let scheduledFailure = null;
  let closePromise = null;
  let closed = false;
  let lastDrain = null;

  async function drain() {
    if (closed) return null;
    if (running) return running;
    running = (async () => {
      try {
        lastDrain = await drainNeatlogsDeliveryQueue(outbox, config, { tags: validatedTags, fetch, ownerId });
        counters.drains += 1;
        return lastDrain;
      } catch {
        counters.drainErrors += 1;
        return null;
      } finally {
        running = null;
      }
    })();
    return running;
  }

  function scheduleDrain(delayMs = drainDelayMs) {
    if (closed || timer) return;
    timer = setTimeout(() => {
      timer = null;
      const scheduled = drain().then(() => {
        // Keep going while work remains (retries, readback windows).
        if (!closed && outbox.hasRunnableWork()) scheduleDrain(Math.max(drainDelayMs, 15_000));
      }).catch(error => {
        scheduledFailure ??= error;
        counters.drainErrors += 1;
      });
      scheduledWork = Promise.all([scheduledWork, scheduled]).then(() => undefined);
    }, delayMs);
    timer.unref?.();
  }

  if (outbox.hasRunnableWork()) scheduleDrain();

  return Object.freeze({
    summary: Object.freeze({
      workflowName: NEATLOGS_TICKETSTUB_WORKFLOW_NAME,
      tags: validatedTags,
      neatlogs: config.summary
    }),
    /** Validates and stores events; returns counts. Never sends inline. */
    accept(events) {
      if (closed) throw new TicketstubIntakeError('CLOSED');
      if (!Array.isArray(events) || events.length > MAX_EVENTS_PER_CALL) {
        throw new TicketstubIntakeError('INVALID_BATCH');
      }
      const entries = [];
      let invalid = 0;
      for (const event of events) {
        try {
          entries.push(ticketstubOutboxEntry(event, tokenize));
        } catch {
          invalid += 1;
        }
      }
      const stored = entries.length > 0 ? outbox.enqueue(entries)
        : { accepted: 0, duplicates: 0, refused: 0, conflicts: 0 };
      counters.accepted += stored.accepted;
      counters.duplicates += stored.duplicates;
      counters.refused += stored.refused;
      counters.conflicts += stored.conflicts;
      counters.invalid += invalid;
      if (stored.accepted > 0 || stored.duplicates > 0) scheduleDrain();
      return Object.freeze({ ...stored, invalid });
    },
    drain,
    scheduleDrain,
    status() {
      return Object.freeze({
        counters: Object.freeze({ ...counters }),
        outbox: outbox.diagnostics(),
        lastDrain
      });
    },
    async close() {
      if (closePromise) return closePromise;
      if (closed) return;
      closed = true;
      if (timer) clearTimeout(timer);
      timer = null;
      closePromise = (async () => {
        if (running) await running;
        await scheduledWork;
        outbox.close();
        if (scheduledFailure) throw scheduledFailure;
      })();
      return closePromise;
    }
  });
}
