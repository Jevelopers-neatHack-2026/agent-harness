import { randomUUID } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import {
  NEATLOGS_LIVE_WORKFLOW_NAME,
  deterministicNeatlogsIds,
  encodeNeatlogsExportRequest,
  NEATLOGS_OTLP_LIMITS,
  validateNeatlogsTags
} from './neatlogs-otlp.js';
import { readbackNeatlogsPlan, sendNeatlogsPlan } from './neatlogs-live.js';

// Durable Neatlogs delivery queue for long-running hosts: the "sent, awaiting
// readback" state kept in SQLite, with a drain that a host process can run on
// a schedule.
//
// Producers enqueue entries with a local SQLite insert, which is fast and
// idempotent by event token. A separate drain sends queued entries, records
// receiver acceptance as `sent` (never as delivered), and, when a readback
// token is configured, reads them back: `verified` settles an entry, a trace
// that stays unreadable past the missing window moves to the next delivery
// generation, and an unresolved trace that reaches its bounded stop condition
// remains held as `sent` until an operator or later readback resolves it.
//
// The harness runtime's own telemetry cursor uses the file outbox in
// neatlogs-outbox.js. This queue serves hosts that hand the harness events
// in-process, starting with the TicketStub intake (src/integrations/ticketstub).

export const NEATLOGS_DELIVERY_QUEUE_SCHEMA_VERSION = 2;
export const NEATLOGS_TICKETSTUB_WORKFLOW_NAME = 'agent-harness.ticketstub-events';
const WORKFLOW_NAMES = new Set([NEATLOGS_LIVE_WORKFLOW_NAME, NEATLOGS_TICKETSTUB_WORKFLOW_NAME]);
const TOKEN = /^tok1_[0-9a-f]{64}$/;
const EVENT_TYPE = /^[a-z][a-z0-9_]{0,63}$/;
const ATTRIBUTE_KEY = /^[a-z][a-z0-9_]*(?:\.[a-z0-9_]+){1,5}$/;
const MAX_ATTRIBUTES = 24;
const MAX_ATTRIBUTE_STRING = 256;
const MAX_INPUT_BYTES = 4096;
const MAX_GENERATION = 3;
const MAX_BATCH = 32;
const MAX_SEND_ATTEMPTS = 8;
const DAY_MS = 24 * 3_600_000;
const SIZE_PROBE_SEQUENCE = Number.MAX_SAFE_INTEGER;
// The encoder writes span times as uint64 nanoseconds. This representative
// send time is in milliseconds and comfortably inside that wire range. Event
// time is retained in the immutable input and is not the size-probe timestamp.
const SIZE_PROBE_SEND_TIME_MS = 1_000_000_000_000;

const DEFAULTS = Object.freeze({
  maxUnsettled: 5_000,
  maxSettledRows: 5_000,
  retentionMs: 7 * DAY_MS,
  readbackDelayMs: 30_000,
  missingAfterMs: 10 * 60_000,
  pendingGiveUpMs: DAY_MS,
  retryBaseMs: 30_000,
  retryMaxMs: 30 * 60_000,
  drainLeaseMs: 2 * 60_000,
  maxBatchesPerDrain: 4
});

export class NeatlogsDeliveryQueueError extends Error {
  constructor(code) {
    super(`Neatlogs delivery queue operation failed (${code}).`);
    this.name = 'NeatlogsDeliveryQueueError';
    this.code = code;
  }
}

function positiveInteger(value, fallback, maximum) {
  if (value === undefined) return fallback;
  if (!Number.isSafeInteger(value) || value < 0 || value > maximum) {
    throw new NeatlogsDeliveryQueueError('INVALID_OPTION');
  }
  return value;
}

function boundedPageLimit(value, fallback, maximum) {
  // SQLite treats a negative LIMIT as unbounded. Refuse invalid inputs before
  // opening the database or renewing a lease; retain positive-limit clamping.
  return Math.min(positiveInteger(value, fallback, Number.MAX_SAFE_INTEGER), maximum);
}

function validAttributeValue(value) {
  if (typeof value === 'string') return value.length <= MAX_ATTRIBUTE_STRING;
  if (typeof value === 'boolean') return true;
  return Number.isSafeInteger(value) && value >= 0;
}

/**
 * Checks one entry before it is stored. Entries carry only HMAC tokens, a
 * fixed event type, canonical input JSON and allow-listed scalar attributes;
 * Neatlogs-reserved attribute names are refused so a producer cannot alter
 * the trace envelope.
 */
export function validateNeatlogsQueueEntry(entry) {
  if (entry === null || typeof entry !== 'object' || Array.isArray(entry)) {
    throw new NeatlogsDeliveryQueueError('INVALID_ENTRY');
  }
  const { eventToken, workflowName, sessionId, eventType, input, attributes, occurredAtMs } = entry;
  if (typeof eventToken !== 'string' || !TOKEN.test(eventToken)
      || typeof sessionId !== 'string' || !TOKEN.test(sessionId)
      || !WORKFLOW_NAMES.has(workflowName)
      || typeof eventType !== 'string' || !EVENT_TYPE.test(eventType)
      || typeof input !== 'string' || Buffer.byteLength(input, 'utf8') > MAX_INPUT_BYTES
      || !Number.isSafeInteger(occurredAtMs) || occurredAtMs <= 0
      || !Array.isArray(attributes) || attributes.length > MAX_ATTRIBUTES) {
    throw new NeatlogsDeliveryQueueError('INVALID_ENTRY');
  }
  const keys = new Set();
  for (const pair of attributes) {
    if (!Array.isArray(pair) || pair.length !== 2) throw new NeatlogsDeliveryQueueError('INVALID_ENTRY');
    const [key, value] = pair;
    if (typeof key !== 'string' || !ATTRIBUTE_KEY.test(key) || key.startsWith('neatlogs.')
        || keys.has(key) || !validAttributeValue(value)) {
      throw new NeatlogsDeliveryQueueError('INVALID_ENTRY');
    }
    keys.add(key);
  }
  try {
    JSON.parse(input);
  } catch {
    throw new NeatlogsDeliveryQueueError('INVALID_ENTRY');
  }
  const validated = Object.freeze({
    eventToken, workflowName, sessionId, eventType, input,
    attributes: Object.freeze(attributes.map(([key, value]) => Object.freeze([key, value]))),
    occurredAtMs
  });
  try {
    const priorTraceIds = Object.freeze(Array.from({ length: MAX_GENERATION }, (_, generation) =>
      deterministicNeatlogsIds(eventToken, generation).traceId));
    const sizeProbe = encodeNeatlogsExportRequest(singleEntryPlan(
      validated, {
        sequence: SIZE_PROBE_SEQUENCE,
        sendTimeMs: SIZE_PROBE_SEND_TIME_MS,
        tags: worstCaseTags(),
        generation: MAX_GENERATION,
        priorTraceIds
      }
    ));
    if (sizeProbe.length > NEATLOGS_OTLP_LIMITS.requestBytes) {
      throw new NeatlogsDeliveryQueueError('ENTRY_TOO_LARGE');
    }
  } catch (error) {
    if (error instanceof NeatlogsDeliveryQueueError) throw error;
    throw new NeatlogsDeliveryQueueError('ENTRY_TOO_LARGE');
  }
  return validated;
}

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS neatlogs_delivery_queue_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS neatlogs_delivery_queue (
  position INTEGER PRIMARY KEY AUTOINCREMENT,
  event_token TEXT NOT NULL UNIQUE,
  workflow_name TEXT NOT NULL,
  session_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  input TEXT NOT NULL,
  attributes TEXT NOT NULL,
  occurred_at_ms INTEGER NOT NULL,
  enqueued_at_ms INTEGER NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('queued', 'sent', 'verified', 'failed')),
  generation INTEGER NOT NULL DEFAULT 0 CHECK (generation BETWEEN 0 AND ${MAX_GENERATION}),
  prior_trace_ids TEXT NOT NULL DEFAULT '[]',
  duplicate_trace_ids TEXT NOT NULL DEFAULT '[]',
  trace_id TEXT NOT NULL,
  send_attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at_ms INTEGER NOT NULL,
  readback_checked_at_ms INTEGER,
  next_readback_at_ms INTEGER,
  retry_not_before_ms INTEGER,
  batch_id TEXT,
  legacy_hold INTEGER NOT NULL DEFAULT 0 CHECK (legacy_hold IN (0, 1)),
  sent_at_ms INTEGER,
  last_status TEXT,
  last_reason TEXT,
  settled_at_ms INTEGER
) STRICT;
CREATE INDEX IF NOT EXISTS neatlogs_delivery_queue_due ON neatlogs_delivery_queue (state, next_attempt_at_ms, position);
CREATE TABLE IF NOT EXISTS neatlogs_delivery_batches (
  batch_id TEXT PRIMARY KEY,
  event_token TEXT NOT NULL,
  generation INTEGER NOT NULL,
  plan_json TEXT NOT NULL,
  body BLOB NOT NULL,
  tags_json TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS neatlogs_delivery_attempts (
  batch_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL CHECK (ordinal > 0),
  started_at_ms INTEGER NOT NULL,
  status TEXT NOT NULL,
  http_status INTEGER,
  reason_code TEXT,
  retry_after_minimum_ms INTEGER,
  PRIMARY KEY (batch_id, ordinal)
) STRICT;
CREATE INDEX IF NOT EXISTS neatlogs_delivery_attempts_time ON neatlogs_delivery_attempts (started_at_ms);
`;

function worstCaseTags() {
  return Object.freeze(Array.from({ length: NEATLOGS_OTLP_LIMITS.tags }, (_, index) =>
    `${String(index).padStart(2, '0')}${'x'.repeat(126)}`));
}

function singleEntryPlan(entry, { sequence, sendTimeMs, tags = [], generation = 0, priorTraceIds = [] }) {
  const ids = deterministicNeatlogsIds(entry.eventToken, generation);
  const start = sendTimeMs * 1e6;
  const trace = Object.freeze({
    ...ids,
    generation,
    priorTraceIds: Object.freeze([...priorTraceIds]),
    sessionId: entry.sessionId,
    eventToken: entry.eventToken,
    sequence,
    revision: 0,
    eventType: entry.eventType,
    fence: 0,
    input: entry.input,
    attributes: entry.attributes,
    root: Object.freeze({ start, end: start + 1e6 }),
    marker: Object.freeze({ start: start + 1e6, end: start + 1e6 })
  });
  return Object.freeze({ workflowName: entry.workflowName, tags: Object.freeze([...tags]), traces: Object.freeze([trace]) });
}

function rowTrace(row, nowMs) {
  const ids = deterministicNeatlogsIds(row.event_token, row.generation);
  const startNs = nowMs * 1e6;
  return Object.freeze({
    ...ids,
    generation: row.generation,
    priorTraceIds: Object.freeze(JSON.parse(row.prior_trace_ids)),
    sessionId: row.session_id,
    eventToken: row.event_token,
    sequence: row.position,
    revision: 0,
    eventType: row.event_type,
    fence: 0,
    input: row.input,
    attributes: Object.freeze(JSON.parse(row.attributes)),
    // Span times are the send time; the event's own time is in the input.
    root: Object.freeze({ start: startNs, end: startNs + 1e6 }),
    marker: Object.freeze({ start: startNs + 1e6, end: startNs + 1e6 })
  });
}

function planFor(rows, tags, nowMs) {
  return Object.freeze({
    workflowName: rows[0].workflow_name,
    tags,
    traces: Object.freeze(rows.map(row => rowTrace(row, nowMs)))
  });
}

function jsonPlan(json) {
  const plan = JSON.parse(json);
  return Object.freeze({
    ...plan,
    tags: Object.freeze(plan.tags),
    traces: Object.freeze(plan.traces.map(trace => Object.freeze({
      ...trace,
      priorTraceIds: Object.freeze(trace.priorTraceIds),
      attributes: Object.freeze(trace.attributes.map(pair => Object.freeze(pair))),
      root: Object.freeze(trace.root),
      marker: Object.freeze(trace.marker)
    })))
  });
}

function saturatingAdd(left, right) {
  if (!Number.isSafeInteger(right) || right < 0 || left > Number.MAX_SAFE_INTEGER - right) {
    return Number.MAX_SAFE_INTEGER;
  }
  return left + right;
}

export class NeatlogsDeliveryQueue {
  #db;
  #clock;
  #options;
  #closed = false;

  constructor(db, clock, options) {
    this.#db = db;
    this.#clock = clock;
    this.#options = options;
  }

  /**
   * Opens (creating if needed) the queue database at an explicit file path.
   * The queue is separate from the durable workflow store so it can be used
   * by a host process that does not own a harness store.
   */
  static open({ databasePath, clock = Date.now, ...options } = {}) {
    if (typeof databasePath !== 'string' || databasePath.length === 0 || databasePath === ':memory:'
        || databasePath.startsWith('file:')) {
      throw new NeatlogsDeliveryQueueError('DURABILITY_REQUIRES_FILE');
    }
    if (typeof clock !== 'function') throw new NeatlogsDeliveryQueueError('INVALID_OPTION');
    const resolved = Object.freeze({
      maxUnsettled: positiveInteger(options.maxUnsettled, DEFAULTS.maxUnsettled, 100_000),
      maxSettledRows: positiveInteger(options.maxSettledRows, DEFAULTS.maxSettledRows, 100_000),
      retentionMs: positiveInteger(options.retentionMs, DEFAULTS.retentionMs, 90 * DAY_MS),
      readbackDelayMs: positiveInteger(options.readbackDelayMs, DEFAULTS.readbackDelayMs, DAY_MS),
      missingAfterMs: positiveInteger(options.missingAfterMs, DEFAULTS.missingAfterMs, DAY_MS),
      pendingGiveUpMs: positiveInteger(options.pendingGiveUpMs, DEFAULTS.pendingGiveUpMs, 30 * DAY_MS),
      retryBaseMs: positiveInteger(options.retryBaseMs, DEFAULTS.retryBaseMs, DAY_MS),
      retryMaxMs: positiveInteger(options.retryMaxMs, DEFAULTS.retryMaxMs, DAY_MS),
      drainLeaseMs: positiveInteger(options.drainLeaseMs, DEFAULTS.drainLeaseMs, DAY_MS),
      maxBatchesPerDrain: positiveInteger(options.maxBatchesPerDrain, DEFAULTS.maxBatchesPerDrain, 32)
    });
    let db;
    try {
      db = new DatabaseSync(databasePath);
      db.exec('PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL; PRAGMA busy_timeout = 5000;');
      db.exec('BEGIN IMMEDIATE');
      const tableExists = name => db.prepare("SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);
      const queueExisted = Boolean(tableExists('neatlogs_delivery_queue'));
      const metaExisted = Boolean(tableExists('neatlogs_delivery_queue_meta'));
      const version = metaExisted
        ? db.prepare("SELECT value FROM neatlogs_delivery_queue_meta WHERE key = 'schema_version'").get()
        : null;
      if (queueExisted && !version) throw new NeatlogsDeliveryQueueError('UNVERSIONED_STORE');
      db.exec(SCHEMA_SQL);
      if (!version) {
        db.prepare("INSERT INTO neatlogs_delivery_queue_meta (key, value) VALUES ('schema_version', ?)")
          .run(String(NEATLOGS_DELIVERY_QUEUE_SCHEMA_VERSION));
      } else if (version.value === '1') {
        db.exec(`ALTER TABLE neatlogs_delivery_queue ADD COLUMN readback_checked_at_ms INTEGER;
          ALTER TABLE neatlogs_delivery_queue ADD COLUMN next_readback_at_ms INTEGER;
          ALTER TABLE neatlogs_delivery_queue ADD COLUMN retry_not_before_ms INTEGER;
          ALTER TABLE neatlogs_delivery_queue ADD COLUMN batch_id TEXT;
          ALTER TABLE neatlogs_delivery_queue ADD COLUMN legacy_hold INTEGER NOT NULL DEFAULT 1 CHECK (legacy_hold IN (0, 1));
          UPDATE neatlogs_delivery_queue SET legacy_hold = 1 WHERE state IN ('queued', 'sent');
          DROP INDEX IF EXISTS neatlogs_delivery_queue_sent;
          UPDATE neatlogs_delivery_queue_meta SET value = '${NEATLOGS_DELIVERY_QUEUE_SCHEMA_VERSION}' WHERE key = 'schema_version';`);
      } else if (version.value !== String(NEATLOGS_DELIVERY_QUEUE_SCHEMA_VERSION)) {
        throw new NeatlogsDeliveryQueueError('UNSUPPORTED_SCHEMA_VERSION');
      }
      db.exec(`CREATE INDEX IF NOT EXISTS neatlogs_delivery_queue_sent ON neatlogs_delivery_queue
        (state, sent_at_ms, readback_checked_at_ms, position);
        INSERT OR IGNORE INTO neatlogs_delivery_queue_meta (key, value) VALUES ('drain_fence', '0');
        INSERT OR IGNORE INTO neatlogs_delivery_queue_meta (key, value) VALUES ('readback_cursor', '0');`);
      db.exec('COMMIT');
    } catch (error) {
      try { db?.exec('ROLLBACK'); } catch {}
      if (error instanceof NeatlogsDeliveryQueueError) {
        try { db?.close(); } catch {}
        throw error;
      }
      try { db?.close(); } catch {}
      throw new NeatlogsDeliveryQueueError('STORE_OPEN_FAILED');
    }
    return new NeatlogsDeliveryQueue(db, clock, resolved);
  }

  get options() {
    return this.#options;
  }

  now() {
    const value = this.#clock();
    if (!Number.isSafeInteger(value) || value <= 0) throw new NeatlogsDeliveryQueueError('INVALID_CLOCK');
    return value;
  }

  #open() {
    if (this.#closed) throw new NeatlogsDeliveryQueueError('STORE_CLOSED');
    return this.#db;
  }

  #transaction(work) {
    const db = this.#open();
    db.exec('BEGIN IMMEDIATE');
    try {
      const result = work(db);
      db.exec('COMMIT');
      return result;
    } catch (error) {
      try { db.exec('ROLLBACK'); } catch {}
      throw error;
    }
  }

  /**
   * Stores entries without changing the event's HMAC identity. Existing
   * tokens are checked before capacity: matching bytes are duplicates, while
   * a token reused for different immutable content is a conflict.
   */
  enqueue(entries) {
    if (!Array.isArray(entries)) throw new NeatlogsDeliveryQueueError('INVALID_ENTRY');
    const validated = entries.map(validateNeatlogsQueueEntry);
    const now = this.now();
    return this.#transaction(db => {
      this.#pruneSettled(db, now);
      const unsettled = db.prepare(
        "SELECT COUNT(*) AS n FROM neatlogs_delivery_queue WHERE state IN ('queued', 'sent')"
      ).get().n;
      const find = db.prepare('SELECT * FROM neatlogs_delivery_queue WHERE event_token = ?');
      const insert = db.prepare(`INSERT INTO neatlogs_delivery_queue (
        event_token, workflow_name, session_id, event_type, input, attributes, occurred_at_ms,
        enqueued_at_ms, state, generation, trace_id, next_attempt_at_ms, legacy_hold
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', 0, ?, ?, 0)`);
      let accepted = 0;
      let duplicates = 0;
      let refused = 0;
      let conflicts = 0;
      for (const entry of validated) {
        const existing = find.get(entry.eventToken);
        if (existing) {
          const same = existing.workflow_name === entry.workflowName
            && existing.session_id === entry.sessionId
            && existing.event_type === entry.eventType
            && existing.input === entry.input
            && existing.attributes === JSON.stringify(entry.attributes)
            && existing.occurred_at_ms === entry.occurredAtMs;
          if (same) duplicates += 1;
          else conflicts += 1;
          continue;
        }
        if (unsettled + accepted >= this.#options.maxUnsettled) {
          refused += 1;
          continue;
        }
        const { traceId } = deterministicNeatlogsIds(entry.eventToken, 0);
        insert.run(
          entry.eventToken, entry.workflowName, entry.sessionId, entry.eventType, entry.input,
          JSON.stringify(entry.attributes), entry.occurredAtMs, now, traceId, now
        );
        accepted += 1;
      }
      return Object.freeze({ accepted, duplicates, refused, conflicts });
    });
  }

  #leaseRow(db) {
    const row = db.prepare("SELECT value FROM neatlogs_delivery_queue_meta WHERE key = 'drain_lease'").get();
    return row ? JSON.parse(row.value) : null;
  }

  #renewLease(db, lease, now) {
    const current = this.#leaseRow(db);
    if (!current || current.owner !== lease?.owner || current.fence !== lease?.fence || current.expiresAtMs <= now) {
      return false;
    }
    current.expiresAtMs = saturatingAdd(now, this.#options.drainLeaseMs);
    db.prepare("UPDATE neatlogs_delivery_queue_meta SET value = ? WHERE key = 'drain_lease'")
      .run(JSON.stringify(current));
    return true;
  }

  /** Takes a cross-process lease with a monotonically increasing fence. */
  acquireDrainLease(ownerId) {
    const now = this.now();
    return this.#transaction(db => {
      const current = this.#leaseRow(db);
      if (current && current.expiresAtMs > now) return null;
      const sequence = db.prepare("SELECT value FROM neatlogs_delivery_queue_meta WHERE key = 'drain_fence'").get();
      const fence = Number(sequence?.value ?? 0) + 1;
      if (!Number.isSafeInteger(fence)) throw new NeatlogsDeliveryQueueError('LEASE_FENCE_EXHAUSTED');
      db.prepare("UPDATE neatlogs_delivery_queue_meta SET value = ? WHERE key = 'drain_fence'").run(String(fence));
      const lease = Object.freeze({ owner: ownerId, fence, expiresAtMs: saturatingAdd(now, this.#options.drainLeaseMs) });
      db.prepare(`INSERT INTO neatlogs_delivery_queue_meta (key, value) VALUES ('drain_lease', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value`).run(JSON.stringify(lease));
      return lease;
    });
  }

  renewDrainLease(lease) {
    const now = this.now();
    return this.#transaction(db => this.#renewLease(db, lease, now));
  }

  releaseDrainLease(lease) {
    this.#transaction(db => {
      const current = this.#leaseRow(db);
      if (current && current.owner === lease?.owner && current.fence === lease?.fence) {
        db.prepare("DELETE FROM neatlogs_delivery_queue_meta WHERE key = 'drain_lease'").run();
      }
    });
  }

  dueForSend(limit = MAX_BATCH) {
    const pageLimit = boundedPageLimit(limit, MAX_BATCH, MAX_BATCH);
    return this.#open().prepare(`SELECT * FROM neatlogs_delivery_queue
      WHERE state = 'queued' AND legacy_hold = 0 AND next_attempt_at_ms <= ?
      ORDER BY position LIMIT ?`).all(this.now(), pageLimit);
  }

  dueForReadback(limit = MAX_BATCH, lease) {
    const pageLimit = boundedPageLimit(limit, MAX_BATCH, MAX_BATCH);
    const now = this.now();
    return this.#transaction(db => {
      if (!this.#renewLease(db, lease, now)) return [];
      const cursor = Number(db.prepare("SELECT value FROM neatlogs_delivery_queue_meta WHERE key = 'readback_cursor'").get()?.value ?? 0);
      const rows = db.prepare(`SELECT * FROM neatlogs_delivery_queue
        WHERE state = 'sent' AND legacy_hold = 0 AND next_readback_at_ms <= ?
        ORDER BY CASE WHEN position > ? THEN 0 ELSE 1 END, position LIMIT ?`)
        .all(now, cursor, pageLimit);
      return rows;
    });
  }

  /** Advances fairness only when a readback request actually starts. */
  advanceReadbackCursor(row, lease) {
    if (!row || !Number.isSafeInteger(row.position) || row.position < 1) return false;
    const now = this.now();
    return this.#transaction(db => {
      if (!this.#renewLease(db, lease, now)) return false;
      db.prepare("UPDATE neatlogs_delivery_queue_meta SET value = ? WHERE key = 'readback_cursor'")
        .run(String(row.position));
      return true;
    });
  }

  prepareBatch(rows, tags, lease) {
    if (!Array.isArray(rows) || rows.length < 1 || rows.length > MAX_BATCH) {
      throw new NeatlogsDeliveryQueueError('INVALID_BATCH');
    }
    if (rows[0].batch_id) {
      const stored = this.#open().prepare('SELECT batch_id, plan_json, body FROM neatlogs_delivery_batches WHERE batch_id = ?')
        .get(rows[0].batch_id);
      if (!stored) throw new NeatlogsDeliveryQueueError('BATCH_MISSING');
      return Object.freeze({
        batchId: stored.batch_id,
        plan: jsonPlan(stored.plan_json),
        body: Buffer.from(stored.body),
        rows: Object.freeze(rows.filter(row => row.batch_id === stored.batch_id))
      });
    }
    if (rows.some(row => row.batch_id || row.workflow_name !== rows[0].workflow_name
        || row.generation !== rows[0].generation)) {
      throw new NeatlogsDeliveryQueueError('INVALID_BATCH');
    }
    const plan = planFor(rows, tags, this.now());
    const body = encodeNeatlogsExportRequest(plan);
    if (body.length > NEATLOGS_OTLP_LIMITS.requestBytes) throw new NeatlogsDeliveryQueueError('BATCH_TOO_LARGE');
    const planJson = JSON.stringify(plan);
    const batchId = randomUUID();
    const now = this.now();
    this.#transaction(db => {
      if (!this.#renewLease(db, lease, now)) throw new NeatlogsDeliveryQueueError('LEASE_LOST');
      db.prepare(`INSERT INTO neatlogs_delivery_batches
        (batch_id, event_token, generation, plan_json, body, tags_json, created_at_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?)`).run(batchId, rows[0].event_token, rows[0].generation,
          planJson, body, JSON.stringify(tags), now);
      const attach = db.prepare(`UPDATE neatlogs_delivery_queue SET batch_id = ?
        WHERE position = ? AND state = 'queued' AND legacy_hold = 0 AND batch_id IS NULL`);
      for (const row of rows) {
        if (attach.run(batchId, row.position).changes !== 1) throw new NeatlogsDeliveryQueueError('STALE_BATCH');
      }
    });
    return Object.freeze({ batchId, plan, body, rows: Object.freeze(rows) });
  }

  prepareSendBatches(rows, tags, lease) {
    const partitions = new Map();
    for (const row of rows) {
      const key = row.batch_id ? `batch:${row.batch_id}` : `new:${row.workflow_name}:${row.generation}`;
      if (!partitions.has(key)) partitions.set(key, []);
      partitions.get(key).push(row);
    }
    const batches = [];
    for (const [key, members] of partitions) {
      if (key.startsWith('batch:')) {
        batches.push(this.prepareBatch(members, tags, lease));
        continue;
      }
      let candidate = [];
      for (const row of members) {
        const next = [...candidate, row];
        let fits = false;
        try {
          fits = encodeNeatlogsExportRequest(planFor(next, tags, this.now())).length <= NEATLOGS_OTLP_LIMITS.requestBytes;
        } catch (error) {
          if (error?.code !== 'REQUEST_TOO_LARGE') throw error;
        }
        if (fits && next.length <= MAX_BATCH) {
          candidate = next;
          continue;
        }
        if (candidate.length === 0) throw new NeatlogsDeliveryQueueError('ENTRY_TOO_LARGE');
        batches.push(this.prepareBatch(candidate, tags, lease));
        candidate = [row];
        try {
          const single = encodeNeatlogsExportRequest(planFor(candidate, tags, this.now()));
          if (single.length > NEATLOGS_OTLP_LIMITS.requestBytes) throw new NeatlogsDeliveryQueueError('ENTRY_TOO_LARGE');
        } catch (error) {
          if (error instanceof NeatlogsDeliveryQueueError) throw error;
          if (error?.code === 'REQUEST_TOO_LARGE') throw new NeatlogsDeliveryQueueError('ENTRY_TOO_LARGE');
          throw error;
        }
      }
      if (candidate.length > 0) batches.push(this.prepareBatch(candidate, tags, lease));
    }
    return Object.freeze(batches);
  }

  batchForReadback(rows) {
    const batchId = rows[0]?.batch_id;
    if (!batchId || rows.some(row => row.batch_id !== batchId)) throw new NeatlogsDeliveryQueueError('BATCH_MISSING');
    const stored = this.#open().prepare('SELECT plan_json FROM neatlogs_delivery_batches WHERE batch_id = ?').get(batchId);
    if (!stored) throw new NeatlogsDeliveryQueueError('BATCH_MISSING');
    const plan = jsonPlan(stored.plan_json);
    const byId = new Map(plan.traces.map(trace => [trace.traceId, trace]));
    const traces = rows.map(row => byId.get(row.trace_id));
    if (traces.some(trace => !trace)) throw new NeatlogsDeliveryQueueError('BATCH_MISSING');
    // Preserve the cursor's due order within a persisted batch. Filtering the
    // original plan alone would silently restore ascending position order and
    // let an early failing trace starve later rows on every bounded pass.
    return Object.freeze({ ...plan, traces: Object.freeze(traces) });
  }

  /** Called synchronously immediately before each actual POST. */
  beginPost(batch, lease) {
    const now = this.now();
    return this.#transaction(db => {
      if (!this.#renewLease(db, lease, now)) throw new NeatlogsDeliveryQueueError('LEASE_LOST');
      const currentRows = db.prepare(`SELECT * FROM neatlogs_delivery_queue WHERE batch_id = ?
        AND state IN ('queued', 'sent') AND legacy_hold = 0 ORDER BY position`).all(batch.batchId);
      if (currentRows.length !== batch.rows.length || currentRows.some(row => row.send_attempts >= MAX_SEND_ATTEMPTS)) {
        throw new NeatlogsDeliveryQueueError('STALE_BATCH');
      }
      const ordinal = Number(db.prepare('SELECT COALESCE(MAX(ordinal), 0) + 1 AS n FROM neatlogs_delivery_attempts WHERE batch_id = ?')
        .get(batch.batchId).n);
      db.prepare(`INSERT INTO neatlogs_delivery_attempts
        (batch_id, ordinal, started_at_ms, status, reason_code) VALUES (?, ?, ?, 'outcome_unknown', 'POST_OUTCOME_UNRESOLVED')`)
        .run(batch.batchId, ordinal, now);
      db.prepare(`UPDATE neatlogs_delivery_queue SET state = 'sent', sent_at_ms = COALESCE(sent_at_ms, ?),
        next_readback_at_ms = ?, send_attempts = send_attempts + 1, last_status = 'outcome_unknown',
        last_reason = 'POST_OUTCOME_UNRESOLVED' WHERE batch_id = ? AND state IN ('queued', 'sent') AND legacy_hold = 0`)
        .run(now, saturatingAdd(now, this.#options.readbackDelayMs), batch.batchId);
      return ordinal;
    });
  }

  /** Records a POST result only while the same owner/fence still holds. */
  recordSend(batch, result, lease) {
    const now = this.now();
    return this.#transaction(db => {
      if (!this.#renewLease(db, lease, now)) return false;
      const attempt = db.prepare(`SELECT ordinal FROM neatlogs_delivery_attempts WHERE batch_id = ?
        ORDER BY ordinal DESC LIMIT 1`).get(batch.batchId);
      if (attempt && result.attempts > 0) {
        db.prepare(`UPDATE neatlogs_delivery_attempts SET status = ?, http_status = ?, reason_code = ?,
          retry_after_minimum_ms = ? WHERE batch_id = ? AND ordinal = ?`)
          .run(result.status, result.httpStatus ?? null, result.reasonCode ?? null,
            result.retryAfterMinimumMs ?? null, batch.batchId, attempt.ordinal);
      }
      const rows = db.prepare(`SELECT * FROM neatlogs_delivery_queue WHERE batch_id = ? AND state = 'sent'
        AND legacy_hold = 0 ORDER BY position`).all(batch.batchId);
      if (rows.length === 0) return false;
      const reason = result.reasonCode ?? null;
      if (result.status === 'receiver_accepted' || result.status === 'partially_rejected'
          || result.status === 'outcome_unknown') {
        db.prepare(`UPDATE neatlogs_delivery_queue SET last_status = ?, last_reason = ?, next_readback_at_ms = ?
          WHERE batch_id = ? AND state = 'sent' AND legacy_hold = 0`)
          .run(result.status, reason, saturatingAdd(now, this.#options.readbackDelayMs), batch.batchId);
        if (result.retryAfterMinimumMs !== undefined) {
          db.prepare(`UPDATE neatlogs_delivery_queue SET retry_not_before_ms = ?
            WHERE batch_id = ? AND state = 'sent' AND legacy_hold = 0`)
            .run(saturatingAdd(now, result.retryAfterMinimumMs), batch.batchId);
        }
        return true;
      }
      const pause = result.retryAfterMinimumMs === undefined ? 0 : result.retryAfterMinimumMs;
      const retryAt = saturatingAdd(now, Math.max(
        Math.min(this.#options.retryBaseMs * 2 ** Math.min(30, rows[0].send_attempts - 1), this.#options.retryMaxMs), pause
      ));
      for (const row of rows) {
        if (result.status === 'rejected' || row.send_attempts >= MAX_SEND_ATTEMPTS) {
          db.prepare(`UPDATE neatlogs_delivery_queue SET state = 'failed', settled_at_ms = ?, last_status = ?,
            last_reason = ? WHERE position = ? AND state = 'sent'`).run(now, result.status,
            reason ?? 'SEND_ATTEMPTS_EXHAUSTED', row.position);
        } else {
          db.prepare(`UPDATE neatlogs_delivery_queue SET state = 'queued', next_attempt_at_ms = ?,
            next_readback_at_ms = NULL, sent_at_ms = NULL, last_status = ?, last_reason = ?,
            retry_not_before_ms = CASE WHEN ? > 0 THEN ? ELSE retry_not_before_ms END
            WHERE position = ? AND state = 'sent'`)
            .run(Math.max(retryAt, row.retry_not_before_ms ?? 0), result.status, reason, pause,
              saturatingAdd(now, pause), row.position);
        }
      }
      return true;
    });
  }

  /** Applies one readback outcome under the current lease fence. */
  recordReadback(row, outcome, lease) {
    const now = this.now();
    return this.#transaction(db => {
      if (!this.#renewLease(db, lease, now)) return 'stale';
      const current = db.prepare(`SELECT * FROM neatlogs_delivery_queue WHERE position = ? AND state = 'sent'
        AND legacy_hold = 0 AND trace_id = ? AND batch_id = ?`).get(row.position, row.trace_id, row.batch_id);
      if (!current) return 'stale';
      const duplicates = JSON.stringify([...(outcome.duplicateTraceIds ?? [])]);
      const settle = (state, reason) => {
        db.prepare(`UPDATE neatlogs_delivery_queue SET state = ?, settled_at_ms = ?, last_status = ?, last_reason = ?,
          duplicate_trace_ids = ?, readback_checked_at_ms = ? WHERE position = ?`)
          .run(state, now, `readback_${outcome.status}`, reason, duplicates, now, row.position);
        return state;
      };
      const age = now - current.sent_at_ms;
      if (outcome.status === 'verified') return settle('verified', null);
      if (outcome.status === 'mismatch') return settle('failed', outcome.problems?.[0] ?? 'READBACK_MISMATCH');
      const holdUnresolved = reason => {
        db.prepare(`UPDATE neatlogs_delivery_queue SET last_status = 'readback_resolution_held', last_reason = ?,
          duplicate_trace_ids = ?, readback_checked_at_ms = ?, next_readback_at_ms = NULL
          WHERE position = ? AND state = 'sent'`)
          .run(reason, duplicates, now, row.position);
        return 'held';
      };
      if (age >= this.#options.pendingGiveUpMs) {
        const reason = outcome.status === 'missing' ? 'READBACK_STOP_DEADLINE'
          : outcome.problems?.includes('TRACE_CONFLICT') ? 'READBACK_CONFLICT_DEADLINE'
            : outcome.problems?.includes('PROJECTION_NOT_READY') ? 'READBACK_PROJECTION_DEADLINE'
              : outcome.problems?.includes('NOT_FINALIZED') ? 'NOT_FINALIZED_IN_TIME'
                : 'READBACK_RESOLUTION_DEADLINE';
        return holdUnresolved(reason);
      }
      if (outcome.status === 'missing' && age >= this.#options.missingAfterMs) {
        if (current.generation >= MAX_GENERATION) return holdUnresolved('MISSING_AFTER_LAST_GENERATION');
        const generation = current.generation + 1;
        const { traceId } = deterministicNeatlogsIds(current.event_token, generation);
        const prior = JSON.stringify([...JSON.parse(current.prior_trace_ids), current.trace_id]);
        db.prepare(`UPDATE neatlogs_delivery_queue SET state = 'queued', generation = ?, trace_id = ?, prior_trace_ids = ?,
          send_attempts = 0, next_attempt_at_ms = MAX(?, COALESCE(retry_not_before_ms, 0)), sent_at_ms = NULL,
          next_readback_at_ms = NULL, readback_checked_at_ms = ?, batch_id = NULL, last_status = 'readback_missing',
          last_reason = 'NOT_OBSERVED_AFTER_WINDOW' WHERE position = ?`)
          .run(generation, traceId, prior, now, now, row.position);
        return 'regenerated';
      }
      db.prepare(`UPDATE neatlogs_delivery_queue SET last_status = ?, readback_checked_at_ms = ?, next_readback_at_ms = ?,
        last_reason = ? WHERE position = ?`).run(`readback_${outcome.status}`, now,
        saturatingAdd(now, this.#options.readbackDelayMs), outcome.problems?.[0] ?? null, row.position);
      return 'waiting';
    });
  }

  deferReadback(rows, lease, reason) {
    const now = this.now();
    return this.#transaction(db => {
      if (!this.#renewLease(db, lease, now)) return false;
      const update = db.prepare(`UPDATE neatlogs_delivery_queue SET readback_checked_at_ms = ?,
        next_readback_at_ms = ?, last_status = ? WHERE position = ? AND state = 'sent' AND batch_id = ?
          AND legacy_hold = 0 AND last_status <> 'readback_resolution_held'`);
      for (const row of rows) update.run(now, saturatingAdd(now, this.#options.readbackDelayMs), reason, row.position, row.batch_id);
      return true;
    });
  }

  #pruneSettled(db, now) {
    const cutoff = now - this.#options.retentionMs;
    const workBound = Math.max(MAX_BATCH, this.#options.maxBatchesPerDrain * MAX_BATCH);
    const expired = db.prepare(`DELETE FROM neatlogs_delivery_queue WHERE position IN (
      SELECT position FROM neatlogs_delivery_queue
      WHERE state IN ('verified', 'failed') AND settled_at_ms < ?
      ORDER BY settled_at_ms, position LIMIT ?)`)
      .run(cutoff, workBound).changes;
    const settled = Number(db.prepare("SELECT COUNT(*) AS n FROM neatlogs_delivery_queue WHERE state IN ('verified', 'failed')").get().n);
    const excess = Math.min(workBound, Math.max(0, settled - this.#options.maxSettledRows));
    const byCount = excess === 0 ? 0 : db.prepare(`DELETE FROM neatlogs_delivery_queue WHERE position IN (
      SELECT position FROM neatlogs_delivery_queue
      WHERE state IN ('verified', 'failed')
      ORDER BY settled_at_ms, position LIMIT ?)`)
      .run(excess).changes;
    db.prepare(`DELETE FROM neatlogs_delivery_attempts WHERE batch_id IN (
      SELECT batch.batch_id FROM neatlogs_delivery_batches AS batch
      WHERE NOT EXISTS (SELECT 1 FROM neatlogs_delivery_queue AS event_row
        WHERE event_row.event_token = batch.event_token)
        AND NOT EXISTS (SELECT 1 FROM neatlogs_delivery_queue AS batch_row
          WHERE batch_row.batch_id = batch.batch_id))`).run();
    db.prepare(`DELETE FROM neatlogs_delivery_batches AS batch
      WHERE NOT EXISTS (SELECT 1 FROM neatlogs_delivery_queue AS event_row
        WHERE event_row.event_token = batch.event_token)
        AND NOT EXISTS (SELECT 1 FROM neatlogs_delivery_queue AS batch_row
          WHERE batch_row.batch_id = batch.batch_id)`).run();
    return expired + byCount;
  }

  /** Prunes in a bounded page; the replay window is time- or count-limited. */
  prune(lease) {
    const now = this.now();
    return this.#transaction(db => this.#renewLease(db, lease, now) ? this.#pruneSettled(db, now) : 0);
  }

  hasRunnableWork() {
    return this.#open().prepare(`SELECT 1 AS present FROM neatlogs_delivery_queue
      WHERE legacy_hold = 0 AND (state = 'queued' OR (state = 'sent' AND next_readback_at_ms IS NOT NULL))
      LIMIT 1`).get() !== undefined;
  }

  /** Counts by state plus the most recent trace IDs per state. Holds no content. */
  diagnostics({ recent = 5 } = {}) {
    const pageLimit = boundedPageLimit(recent, 5, 50);
    const db = this.#open();
    const counts = { queued: 0, sent: 0, verified: 0, failed: 0, legacyHeld: 0, unresolvedHeld: 0 };
    for (const row of db.prepare('SELECT state, COUNT(*) AS n FROM neatlogs_delivery_queue GROUP BY state').all()) {
      counts[row.state] = row.n;
    }
    counts.legacyHeld = Number(db.prepare(`SELECT COUNT(*) AS n FROM neatlogs_delivery_queue
      WHERE legacy_hold = 1 AND state IN ('queued', 'sent')`).get().n);
    counts.unresolvedHeld = Number(db.prepare(`SELECT COUNT(*) AS n FROM neatlogs_delivery_queue
      WHERE state = 'sent' AND last_status = 'readback_resolution_held'`).get().n);
    const latest = db.prepare(`SELECT trace_id, workflow_name, event_type, state, generation, last_status, last_reason,
      duplicate_trace_ids, legacy_hold, send_attempts FROM neatlogs_delivery_queue ORDER BY position DESC LIMIT ?`)
      .all(pageLimit);
    return Object.freeze({
      counts: Object.freeze(counts),
      latest: Object.freeze(latest.map(row => Object.freeze({
        traceId: row.trace_id,
        workflowName: row.workflow_name,
        eventType: row.event_type,
        state: row.state,
        generation: row.generation,
        lastStatus: row.last_status,
        lastReason: row.last_reason,
        legacyHeld: row.legacy_hold === 1,
        postAttempts: row.send_attempts,
        duplicateTraceIds: Object.freeze(JSON.parse(row.duplicate_trace_ids))
      })))
    });
  }

  close() {
    if (this.#closed) return;
    this.#closed = true;
    this.#db.close();
  }
}

/**
 * One bounded pass over the queue: readback of sent rows (when a readback
 * token is configured), then sending of due rows, then pruning. Never throws
 * for network trouble; the returned summary carries only counts and codes.
 */
export async function drainNeatlogsDeliveryQueue(queue, config, options = {}) {
  const tags = validateNeatlogsTags(options.tags ?? []);
  const ownerId = options.ownerId ?? `drain-${randomUUID()}`;
  const summary = {
    leased: false, leaseLost: false, sent: 0, sendRequests: 0, sendBatches: 0, retried: 0, failed: 0,
    verified: 0, regenerated: 0, waiting: 0, held: 0, readbackStatus: null, sendStatus: null, pruned: 0
  };
  const lease = queue.acquireDrainLease(ownerId);
  if (!lease) return Object.freeze(summary);
  summary.leased = true;
  let passStopped = false;
  try {
    if (config?.summary?.readbackReady) {
      if (!queue.renewDrainLease(lease)) {
        summary.leaseLost = true;
        passStopped = true;
      }
      const rows = passStopped ? [] : queue.dueForReadback(MAX_BATCH, lease);
      const groups = new Map();
      for (const row of rows) {
        if (!row.batch_id) continue;
        if (!groups.has(row.batch_id)) groups.set(row.batch_id, []);
        groups.get(row.batch_id).push(row);
      }
      for (const group of groups.values()) {
        const plan = queue.batchForReadback(group);
        if (!queue.renewDrainLease(lease)) {
          summary.leaseLost = true;
          passStopped = true;
          break;
        }
        const rowByTrace = new Map(group.map(row => [row.trace_id, row]));
        const result = await readbackNeatlogsPlan(config, plan, {
          fetch: options.fetch, sleep: options.sleep, signal: options.signal, maxPolls: 1,
          onRequestStarted: (path, traceId) => {
            if (!queue.renewDrainLease(lease)) throw new NeatlogsDeliveryQueueError('LEASE_LOST');
            if (traceId && rowByTrace.has(traceId) && path.startsWith('/traces/')) {
              if (!queue.advanceReadbackCursor(rowByTrace.get(traceId), lease)) {
                throw new NeatlogsDeliveryQueueError('LEASE_LOST');
              }
            }
          }
        });
        summary.readbackStatus = result.status;
        if (result.reasonCode === 'READBACK_LEASE_LOST') {
          summary.leaseLost = true;
          passStopped = true;
          break;
        }
        if (result.status === 'mismatch' && result.reasonCode === 'PROJECT_MISMATCH') break;
        const byTrace = new Map(result.traces.map(trace => [trace.traceId, trace]));
        for (const row of group) {
          const outcome = byTrace.get(row.trace_id);
          if (!outcome) continue;
          const action = queue.recordReadback(row, outcome, lease);
          if (action === 'stale') {
            summary.leaseLost = true;
            passStopped = true;
            break;
          }
          if (action === 'verified') summary.verified += 1;
          else if (action === 'failed') summary.failed += 1;
          else if (action === 'regenerated') summary.regenerated += 1;
          else if (action === 'waiting') summary.waiting += 1;
          else if (action === 'held') summary.held += 1;
        }
        if (passStopped) break;
        if (result.status === 'readback_failed' || result.status === 'readback_unavailable') {
          if (!queue.deferReadback(group, lease, result.reasonCode ?? result.status)) {
            summary.leaseLost = true;
            passStopped = true;
          }
          break;
        }
      }
    }
    if (config?.summary?.ingestReady && !passStopped) {
      while (summary.sendBatches < queue.options.maxBatchesPerDrain) {
        const rows = queue.dueForSend();
        if (rows.length === 0) break;
        let anyAccepted = false;
        let batches;
        try {
          batches = queue.prepareSendBatches(rows, tags, lease);
        } catch (error) {
          if (error?.code !== 'LEASE_LOST') throw error;
          summary.leaseLost = true;
          passStopped = true;
          break;
        }
        if (batches.length === 0) break;
        for (const batchPlan of batches) {
          if (summary.sendBatches >= queue.options.maxBatchesPerDrain) break;
          if (!queue.renewDrainLease(lease)) {
            summary.leaseLost = true;
            passStopped = true;
            break;
          }
          summary.sendBatches += 1;
          let result;
          try {
            result = await sendNeatlogsPlan(config, batchPlan.plan, {
              fetch: options.fetch, sleep: options.sleep, signal: options.signal,
              maxAttempts: 1,
              encodedBody: batchPlan.body,
              onPostStart: () => queue.beginPost(batchPlan, lease)
            });
          } catch (error) {
            if (error?.code !== 'LEASE_LOST') throw error;
            summary.leaseLost = true;
            passStopped = true;
            break;
          }
          summary.sendRequests += result.attempts;
          summary.sendStatus = result.status;
          const saved = queue.recordSend(batchPlan, result, lease);
          if (!saved) {
            summary.leaseLost = true;
            passStopped = true;
            break;
          }
          if (result.status === 'receiver_accepted' || result.status === 'partially_rejected') {
            summary.sent += batchPlan.rows.length;
            anyAccepted = true;
          } else {
            summary.retried += batchPlan.rows.length;
          }
        }
        if (passStopped) break;
        if (!anyAccepted && queue.dueForSend().length === 0) break;
      }
    }
    if (!passStopped) summary.pruned = queue.prune(lease);
  } finally {
    queue.releaseDrainLease(lease);
  }
  return Object.freeze(summary);
}

export const NEATLOGS_DELIVERY_QUEUE_LIMITS = Object.freeze({
  defaults: DEFAULTS,
  maxBatch: MAX_BATCH,
  maxGeneration: MAX_GENERATION,
  maxSendAttempts: MAX_SEND_ATTEMPTS,
  maxAttributes: MAX_ATTRIBUTES,
  maxAttributeString: MAX_ATTRIBUTE_STRING,
  maxInputBytes: MAX_INPUT_BYTES,
  requestBytes: NEATLOGS_OTLP_LIMITS.requestBytes
});
