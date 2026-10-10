import { createHash, randomUUID } from 'node:crypto';
import { createRequire } from 'node:module';
import { mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { RuntimeError, runtimeError } from './errors.js';

export const INTERNAL_SCHEMA_VERSION = 4;
const CHECKPOINT_SCHEMA_VERSION = 2;
const MAX_CHECKPOINT_DEPTH = 128;
const MAX_CHECKPOINT_NODES = 10_000;
const MAX_CHECKPOINT_SOURCE_UNITS = 256 * 1024;
const MAX_CHECKPOINT_SERIALIZED_BYTES = 1024 * 1024;
const INVALID_CHECKPOINT_INPUT = Symbol('invalid checkpoint input');
const STATES = new Set([
  'queued',
  'running',
  'awaiting_response',
  'reconciliation_pending',
  'review_required',
  'succeeded',
  'failed',
  'cancelled'
]);
const TERMINAL = new Set(['succeeded', 'failed', 'cancelled']);
const TRANSITIONS = new Map([
  ['queued', new Set(['running', 'review_required'])],
  ['running', new Set(['reconciliation_pending', 'review_required', 'succeeded', 'failed'])],
  ['awaiting_response', new Set(['running', 'reconciliation_pending', 'review_required', 'succeeded', 'failed'])],
  ['reconciliation_pending', new Set(['running', 'review_required', 'succeeded', 'failed'])],
  ['review_required', new Set()],
  ['succeeded', new Set()],
  ['failed', new Set()],
  ['cancelled', new Set()]
]);
const requireBuiltin = createRequire(import.meta.url);

function fail(operation, code, safe_message, retryable = false) {
  throw runtimeError({ module: 'state', operation, code, safe_message, retryable });
}

function expectString(value, field, operation, minimum = 1) {
  if (typeof value !== 'string' || value.trim().length < minimum || value.length > 512) {
    fail(operation, 'INVALID_INPUT', 'A required identifier or reference is invalid.');
  }
  return value;
}

function expectInteger(value, field, operation, minimum = 0) {
  if (!Number.isSafeInteger(value) || value < minimum) {
    fail(operation, 'INVALID_INPUT', 'A required numeric setting is outside its supported range.');
  }
  return value;
}

function isRecord(value) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function checkpointString(value, context) {
  context.sourceUnits += value.length;
  if (context.sourceUnits > MAX_CHECKPOINT_SOURCE_UNITS) throw INVALID_CHECKPOINT_INPUT;
  return value;
}

function canonicalValue(value, context, depth = 0) {
  context.nodes += 1;
  if (context.nodes > MAX_CHECKPOINT_NODES || depth > MAX_CHECKPOINT_DEPTH) {
    throw INVALID_CHECKPOINT_INPUT;
  }
  if (value === null || typeof value === 'boolean') return value;
  if (typeof value === 'string') return checkpointString(value, context);
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value !== 'object' || context.seen.has(value)) throw INVALID_CHECKPOINT_INPUT;

  context.seen.add(value);
  try {
    if (Array.isArray(value)) {
      const prototype = Object.getPrototypeOf(value);
      if (prototype !== Array.prototype && prototype !== null) throw INVALID_CHECKPOINT_INPUT;
      const keys = Reflect.ownKeys(value);
      const lengthDescriptor = Object.getOwnPropertyDescriptor(value, 'length');
      const length = lengthDescriptor?.value;
      if (
        !Number.isSafeInteger(length)
        || length < 0
        || length > MAX_CHECKPOINT_NODES
        || keys.length !== length + 1
        || context.nodes + length > MAX_CHECKPOINT_NODES
      ) {
        throw INVALID_CHECKPOINT_INPUT;
      }

      // Keep arrays as arrays for JSON encoding, then remove inherited hooks
      // so serialization cannot invoke a caller-supplied toJSON accessor.
      const result = [];
      for (let index = 0; index < length; index += 1) {
        const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
        if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) {
          throw INVALID_CHECKPOINT_INPUT;
        }
        result.push(canonicalValue(descriptor.value, context, depth + 1));
      }
      Object.setPrototypeOf(result, null);
      return result;
    }

    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) throw INVALID_CHECKPOINT_INPUT;
    const keys = Reflect.ownKeys(value);
    if (context.nodes + keys.length > MAX_CHECKPOINT_NODES) throw INVALID_CHECKPOINT_INPUT;
    for (const key of keys) {
      if (typeof key !== 'string') throw INVALID_CHECKPOINT_INPUT;
      checkpointString(key, context);
    }
    keys.sort();

    // A regular object's `__proto__` setter would mutate the prototype and
    // silently drop an own JSON property. A null-prototype container keeps
    // every key as data while preserving deterministic JSON serialization.
    const result = Object.create(null);
    for (const key of keys) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) {
        throw INVALID_CHECKPOINT_INPUT;
      }
      Object.defineProperty(result, key, {
        value: canonicalValue(descriptor.value, context, depth + 1),
        enumerable: true,
        configurable: true,
        writable: true
      });
    }
    return result;
  } finally {
    context.seen.delete(value);
  }
}

function serializeCheckpoint(data, operation) {
  try {
    if (!isRecord(data)) throw INVALID_CHECKPOINT_INPUT;
    const normalized = canonicalValue(data, { nodes: 0, sourceUnits: 0, seen: new Set() });
    const encoded = JSON.stringify(normalized);
    const payload = '{"schemaVersion":' + CHECKPOINT_SCHEMA_VERSION + ',"data":' + encoded + '}';
    if (Buffer.byteLength(payload, 'utf8') > MAX_CHECKPOINT_SERIALIZED_BYTES) {
      throw INVALID_CHECKPOINT_INPUT;
    }
    return payload;
  } catch {
    fail(operation, 'INVALID_INPUT', 'Checkpoint data is invalid or exceeds the supported size and structure limits.');
  }
}

function digest(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

function mapStorageError(operation, error) {
  if (error instanceof RuntimeError) return error;
  const code = typeof error?.code === 'string' ? error.code : '';
  const sqlitePrimaryCode = Number.isInteger(error?.errcode) ? error.errcode & 0xff : 0;
  if (/BUSY|LOCKED/i.test(code) || sqlitePrimaryCode === 5 || sqlitePrimaryCode === 6) {
    return runtimeError({
      module: 'state',
      operation,
      code: 'STORE_BUSY',
      retryable: true,
      safe_message: 'The durable store is busy; retry after the current write finishes.'
    });
  }
  return runtimeError({
    module: 'state',
    operation,
    code: operation === 'open' ? 'STORE_OPEN_FAILED' : 'STORE_WRITE_FAILED',
    retryable: false,
    safe_message: operation === 'open'
      ? 'The durable store could not be opened safely.'
      : 'The durable state change could not be committed.'
  });
}

function inTransaction(db, operation, callback) {
  try {
    db.exec('BEGIN IMMEDIATE');
    try {
      const value = callback();
      db.exec('COMMIT');
      return value;
    } catch (error) {
      if (db.isTransaction) db.exec('ROLLBACK');
      throw error;
    }
  } catch (error) {
    throw mapStorageError(operation, error);
  }
}

function openSqliteConstructor() {
  const [major, minor, patch] = process.versions.node.split('.').map(Number);
  if (major !== 22 || minor < 23 || (minor === 23 && patch < 3)) {
    fail('open', 'UNSUPPORTED_RUNTIME', 'The durable store requires the validated Node 22.23.3 runtime line.');
  }
  let DatabaseSync;
  try {
    ({ DatabaseSync } = requireBuiltin('node:sqlite'));
  } catch {
    fail('open', 'SQLITE_UNSUPPORTED', 'This Node runtime does not provide the required built-in SQLite API.');
  }
  if (typeof DatabaseSync !== 'function') {
    fail('open', 'SQLITE_UNSUPPORTED', 'This Node runtime does not provide the required built-in SQLite API.');
  }
  return DatabaseSync;
}

function createSchemaV3(db) {
  db.exec(`
    CREATE TABLE workflows (
      workflow_id TEXT PRIMARY KEY,
      state TEXT NOT NULL CHECK (state IN ('queued','running','awaiting_response','reconciliation_pending','review_required','succeeded','failed','cancelled')),
      revision INTEGER NOT NULL CHECK (revision >= 0),
      fence_token INTEGER NOT NULL CHECK (fence_token >= 0),
      lease_owner TEXT,
      lease_expires_ms INTEGER,
      checkpoint_sequence INTEGER NOT NULL CHECK (checkpoint_sequence >= 1),
      cancellation_requested INTEGER NOT NULL CHECK (cancellation_requested IN (0,1)),
      created_at_ms INTEGER NOT NULL,
      updated_at_ms INTEGER NOT NULL
    ) STRICT;

    CREATE TABLE workflow_checkpoints (
      workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id),
      sequence INTEGER NOT NULL CHECK (sequence >= 1),
      schema_version INTEGER NOT NULL,
      payload_json TEXT NOT NULL,
      sha256 TEXT NOT NULL,
      revision INTEGER NOT NULL,
      created_at_ms INTEGER NOT NULL,
      PRIMARY KEY (workflow_id, sequence)
    ) STRICT, WITHOUT ROWID;

    CREATE TABLE workflow_events (
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id TEXT NOT NULL UNIQUE,
      workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id),
      revision INTEGER NOT NULL,
      event_type TEXT NOT NULL,
      from_state TEXT,
      to_state TEXT,
      fence_token INTEGER NOT NULL,
      occurred_at_ms INTEGER NOT NULL
    ) STRICT;

    CREATE TABLE outbox (
      event_id TEXT PRIMARY KEY REFERENCES workflow_events(event_id),
      event_sequence INTEGER NOT NULL UNIQUE REFERENCES workflow_events(sequence),
      delivered_at_ms INTEGER,
      delivery_attempts INTEGER NOT NULL DEFAULT 0 CHECK (delivery_attempts >= 0)
    ) STRICT, WITHOUT ROWID;

    CREATE TABLE operation_attempts (
      attempt_id TEXT PRIMARY KEY,
      workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id),
      logical_operation_ref TEXT NOT NULL,
      authority_hold_ref TEXT NOT NULL,
      state TEXT NOT NULL CHECK (state IN ('intent','outcome_unknown','response_received','reconciled')),
      created_at_ms INTEGER NOT NULL,
      updated_at_ms INTEGER NOT NULL
    ) STRICT;

    CREATE TABLE transport_attempts (
      transport_attempt_id TEXT PRIMARY KEY,
      attempt_id TEXT NOT NULL REFERENCES operation_attempts(attempt_id),
      created_at_ms INTEGER NOT NULL
    ) STRICT;

    CREATE TABLE response_receipts (
      receipt_id TEXT PRIMARY KEY,
      attempt_id TEXT NOT NULL UNIQUE REFERENCES operation_attempts(attempt_id),
      outcome TEXT NOT NULL CHECK (outcome IN ('completed','no_effect')),
      next_state TEXT NOT NULL CHECK (next_state IN ('running','succeeded','failed')),
      response_bytes BLOB NOT NULL,
      response_sha256 TEXT NOT NULL,
      next_checkpoint_json TEXT NOT NULL,
      next_checkpoint_sha256 TEXT,
      created_at_ms INTEGER NOT NULL
    ) STRICT;

    CREATE TABLE attempt_observations (
      source_observation_ref TEXT PRIMARY KEY,
      attempt_id TEXT NOT NULL REFERENCES operation_attempts(attempt_id),
      observation_kind TEXT NOT NULL CHECK (observation_kind IN ('known','unknown')),
      fingerprint TEXT NOT NULL,
      observed_at_ms INTEGER NOT NULL
    ) STRICT, WITHOUT ROWID;

    CREATE TABLE immutable_artifacts (
      workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id),
      artifact_id TEXT NOT NULL,
      sha256 TEXT NOT NULL,
      bytes BLOB NOT NULL,
      created_at_ms INTEGER NOT NULL,
      PRIMARY KEY (workflow_id, artifact_id)
    ) STRICT, WITHOUT ROWID;

    CREATE TRIGGER workflow_events_no_update
      BEFORE UPDATE ON workflow_events BEGIN SELECT RAISE(ABORT, 'event log is immutable'); END;
    CREATE TRIGGER workflow_events_no_delete
      BEFORE DELETE ON workflow_events BEGIN SELECT RAISE(ABORT, 'event log is immutable'); END;
    CREATE TRIGGER workflow_checkpoints_no_update
      BEFORE UPDATE ON workflow_checkpoints BEGIN SELECT RAISE(ABORT, 'checkpoints are immutable'); END;
    CREATE TRIGGER workflow_checkpoints_no_delete
      BEFORE DELETE ON workflow_checkpoints BEGIN SELECT RAISE(ABORT, 'checkpoints are immutable'); END;
    CREATE TRIGGER immutable_artifacts_no_update
      BEFORE UPDATE ON immutable_artifacts BEGIN SELECT RAISE(ABORT, 'artifacts are immutable'); END;
    CREATE TRIGGER immutable_artifacts_no_delete
      BEFORE DELETE ON immutable_artifacts BEGIN SELECT RAISE(ABORT, 'artifacts are immutable'); END;
    CREATE TRIGGER response_receipts_no_update
      BEFORE UPDATE ON response_receipts BEGIN SELECT RAISE(ABORT, 'response receipts are immutable'); END;
    CREATE TRIGGER response_receipts_no_delete
      BEFORE DELETE ON response_receipts BEGIN SELECT RAISE(ABORT, 'response receipts are immutable'); END;
  `);
}

function createTelemetryCursorSchema(db) {
  db.exec(`
    CREATE TABLE telemetry_cursors (
      consumer_id TEXT PRIMARY KEY,
      last_sequence INTEGER NOT NULL CHECK (last_sequence >= 0),
      updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= 0)
    ) STRICT, WITHOUT ROWID;
  `);
}

function readUserVersion(db) {
  return Number(db.prepare('PRAGMA user_version').get().user_version);
}

function hasUserTables(db) {
  return db.prepare(
    "SELECT COUNT(*) AS count FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
  ).get().count > 0;
}

function appendMigrationEvent(db, workflowId, state, revision, occurredAtMs) {
  const eventId = 'migration:v1:' + workflowId;
  const result = db.prepare(`
    INSERT INTO workflow_events (
      event_id, workflow_id, revision, event_type, from_state, to_state, fence_token, occurred_at_ms
    ) VALUES (?, ?, ?, 'schema_migrated_v1', ?, ?, 0, ?)
  `).run(eventId, workflowId, revision, state, state, occurredAtMs);
  db.prepare('INSERT INTO outbox (event_id, event_sequence) VALUES (?, ?)').run(
    eventId,
    Number(result.lastInsertRowid)
  );
}

function migrateV1ToV4(db) {
  inTransaction(db, 'migrateV1ToV4', () => {
    db.exec('ALTER TABLE workflows RENAME TO workflows_v1');
    createSchemaV3(db);
    const oldRows = db.prepare('SELECT * FROM workflows_v1 ORDER BY workflow_id').all();
    const insertWorkflow = db.prepare(`
      INSERT INTO workflows (
        workflow_id, state, revision, fence_token, lease_owner, lease_expires_ms,
        checkpoint_sequence, cancellation_requested, created_at_ms, updated_at_ms
      ) VALUES (?, ?, ?, 0, NULL, NULL, 1, 0, ?, ?)
    `);
    const insertCheckpoint = db.prepare(`
      INSERT INTO workflow_checkpoints (
        workflow_id, sequence, schema_version, payload_json, sha256, revision, created_at_ms
      ) VALUES (?, 1, ?, ?, ?, ?, ?)
    `);
    for (const row of oldRows) {
      const workflowId = String(row.workflow_id);
      const state = String(row.state);
      const revision = Number(row.revision);
      const createdAtMs = Number(row.created_at_ms);
      const updatedAtMs = Number(row.updated_at_ms);
      if (!STATES.has(state) || !Number.isSafeInteger(revision) || revision < 0) {
        fail('migrateV1ToV3', 'CHECKPOINT_CORRUPT', 'The legacy checkpoint cannot be migrated safely.');
      }
      if (
        typeof row.checkpoint_json !== 'string'
        || Buffer.byteLength(row.checkpoint_json, 'utf8') > MAX_CHECKPOINT_SERIALIZED_BYTES
      ) {
        fail('migrateV1ToV3', 'CHECKPOINT_CORRUPT', 'The legacy checkpoint cannot be migrated safely.');
      }
      let parsed;
      try {
        parsed = JSON.parse(row.checkpoint_json);
      } catch {
        fail('migrateV1ToV3', 'CHECKPOINT_CORRUPT', 'The legacy checkpoint cannot be migrated safely.');
      }
      if (!isRecord(parsed)) {
        fail('migrateV1ToV3', 'CHECKPOINT_CORRUPT', 'The legacy checkpoint cannot be migrated safely.');
      }
      let checkpointPayload;
      try {
        checkpointPayload = serializeCheckpoint(parsed, 'migrateV1ToV3');
      } catch {
        fail('migrateV1ToV3', 'CHECKPOINT_CORRUPT', 'The legacy checkpoint cannot be migrated safely.');
      }
      const created = Number.isSafeInteger(createdAtMs) ? createdAtMs : updatedAtMs;
      const updated = Number.isSafeInteger(updatedAtMs) ? updatedAtMs : created;
      insertWorkflow.run(workflowId, state, revision, created, updated);
      insertCheckpoint.run(workflowId, CHECKPOINT_SCHEMA_VERSION, checkpointPayload, digest(checkpointPayload), revision, updated);
      appendMigrationEvent(db, workflowId, state, revision, updated);
    }
    db.exec('DROP TABLE workflows_v1');
    createTelemetryCursorSchema(db);
    db.exec('PRAGMA user_version = 4');
  });
}

function migrateV2ToV4(db) {
  inTransaction(db, 'migrateV2ToV4', () => {
    // Existing receipts have no proof binding their proposed checkpoint bytes.
    // Leave them NULL: they remain readable, but reconciliation fails closed.
    db.exec('ALTER TABLE response_receipts ADD COLUMN next_checkpoint_sha256 TEXT');
    createTelemetryCursorSchema(db);
    db.exec('PRAGMA user_version = 4');
  });
}

function migrateV3ToV4(db) {
  inTransaction(db, 'migrateV3ToV4', () => {
    createTelemetryCursorSchema(db);
    db.exec('PRAGMA user_version = 4');
  });
}

function initializeDatabase(db) {
  const version = readUserVersion(db);
  if (version > INTERNAL_SCHEMA_VERSION) {
    fail('initializeDatabase', 'UNSUPPORTED_SCHEMA_VERSION', 'The durable store schema is newer than this runtime supports.');
  }
  if (version === 0) {
    if (hasUserTables(db)) {
      fail('initializeDatabase', 'UNVERSIONED_DATA', 'The durable store contains data without a supported schema version.');
    }
    inTransaction(db, 'initializeDatabase', () => {
      createSchemaV3(db);
      createTelemetryCursorSchema(db);
      db.exec('PRAGMA user_version = 4');
    });
    return;
  }
  if (version === 1) {
    const table = db.prepare(
      "SELECT name FROM sqlite_master WHERE type='table' AND name='workflows'"
    ).get();
    if (!table) fail('initializeDatabase', 'CHECKPOINT_CORRUPT', 'The legacy workflow table is missing.');
    migrateV1ToV4(db);
  } else if (version === 2) {
    migrateV2ToV4(db);
  } else if (version === 3) {
    migrateV3ToV4(db);
  }
}

function mapWorkflow(row) {
  if (!row) return null;
  return {
    workflowId: row.workflow_id,
    state: row.state,
    revision: Number(row.revision),
    fence: Number(row.fence_token),
    leaseOwner: row.lease_owner,
    leaseExpiresAtMs: row.lease_expires_ms === null ? null : Number(row.lease_expires_ms),
    cancellationRequested: Boolean(row.cancellation_requested),
    checkpointSequence: Number(row.checkpoint_sequence)
  };
}

export class DurableWorkflowStore {
  #db;
  #clock;
  #idFactory;

  static open({ databasePath, clock = Date.now, idFactory = randomUUID, busyTimeoutMs = 5000 } = {}) {
    const DatabaseSync = openSqliteConstructor();
    if (typeof databasePath !== 'string' || databasePath.trim() === '' || databasePath === ':memory:') {
      fail('open', 'DURABILITY_REQUIRES_FILE', 'The durable workflow store requires a file-backed database.');
    }
    if (typeof clock !== 'function' || typeof idFactory !== 'function') {
      fail('open', 'INVALID_INPUT', 'Clock and identifier sources must be injected functions.');
    }
    expectInteger(busyTimeoutMs, 'busyTimeoutMs', 'open', 1);
    if (busyTimeoutMs > 60_000) {
      fail('open', 'INVALID_INPUT', 'The database lock wait exceeds its supported range.');
    }

    const filePath = resolve(databasePath);
    try {
      mkdirSync(dirname(filePath), { recursive: true });
      const db = new DatabaseSync(filePath, {
        timeout: busyTimeoutMs,
        enableForeignKeyConstraints: true,
        allowExtension: false
      });
      try {
        const version = readUserVersion(db);
        if (version > INTERNAL_SCHEMA_VERSION) {
          fail('open', 'UNSUPPORTED_SCHEMA_VERSION', 'The durable store schema is newer than this runtime supports.');
        }
        db.exec('PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;');
        initializeDatabase(db);
        return new DurableWorkflowStore(db, clock, idFactory);
      } catch (error) {
        if (db.isOpen) db.close();
        throw error;
      }
    } catch (error) {
      if (error instanceof RuntimeError) throw error;
      throw mapStorageError('open', error);
    }
  }

  constructor(db, clock, idFactory) {
    this.#db = db;
    this.#clock = clock;
    this.#idFactory = idFactory;
  }

  close() {
    if (!this.#db?.isOpen) return;
    try {
      this.#db.close();
    } catch (error) {
      throw mapStorageError('close', error);
    }
  }

  #assertOpen(operation) {
    if (!this.#db?.isOpen) fail(operation, 'STORE_CLOSED', 'The durable store is closed.');
  }

  #now(operation) {
    const value = this.#clock();
    if (!Number.isSafeInteger(value) || value < 0) {
      fail(operation, 'INVALID_INPUT', 'The injected clock returned an invalid timestamp.');
    }
    return value;
  }

  #eventId(provided, operation) {
    const value = provided === undefined ? this.#idFactory() : provided;
    expectString(value, 'eventId', operation);
    return value;
  }

  #assertEventAvailable(eventId, operation) {
    if (this.#db.prepare('SELECT 1 FROM workflow_events WHERE event_id = ?').get(eventId)) {
      fail(operation, 'IDEMPOTENCY_CONFLICT', 'The event identity has already been used.');
    }
  }

  #appendEvent({ eventId, workflowId, revision, type, fromState, toState, fence, atMs }) {
    const result = this.#db.prepare(`
      INSERT INTO workflow_events (
        event_id, workflow_id, revision, event_type, from_state, to_state, fence_token, occurred_at_ms
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(eventId, workflowId, revision, type, fromState, toState, fence, atMs);
    const sequence = Number(result.lastInsertRowid);
    this.#db.prepare('INSERT INTO outbox (event_id, event_sequence) VALUES (?, ?)').run(eventId, sequence);
    return sequence;
  }

  #workflow(workflowId, operation) {
    const row = this.#db.prepare('SELECT * FROM workflows WHERE workflow_id = ?').get(workflowId);
    if (!row) fail(operation, 'WORKFLOW_NOT_FOUND', 'The workflow does not exist.');
    return row;
  }

  #assertRevision(row, expectedRevision, operation) {
    expectInteger(expectedRevision, 'expectedRevision', operation);
    if (Number(row.revision) !== expectedRevision) {
      fail(operation, 'CONCURRENT_MODIFICATION', 'The workflow changed since this operation was prepared.', true);
    }
  }

  #assertLease(row, lease, operation) {
    expectString(lease.ownerId, 'ownerId', operation);
    expectInteger(lease.fence, 'fence', operation, 1);
    const now = this.#now(operation);
    if (Number(row.fence_token) !== lease.fence) {
      fail(operation, 'STALE_FENCE', 'This worker lease has been superseded.');
    }
    if (row.lease_owner !== lease.ownerId) {
      fail(operation, 'LEASE_REQUIRED', 'The current worker does not own this workflow lease.');
    }
    if (row.lease_expires_ms === null || Number(row.lease_expires_ms) <= now) {
      fail(operation, 'LEASE_EXPIRED', 'The worker lease has expired.');
    }
    return now;
  }

  #assertMutable(row, operation) {
    if (TERMINAL.has(row.state)) fail(operation, 'TERMINAL_STATE', 'The workflow is already terminal.');
  }

  #appendAtNewRevision(row, { eventId, type, fromState = row.state, toState = row.state, fence, atMs }) {
    const revision = Number(row.revision) + 1;
    this.#db.prepare(`
      UPDATE workflows
      SET revision = ?, state = ?, updated_at_ms = ?
      WHERE workflow_id = ? AND revision = ?
    `).run(revision, toState, atMs, row.workflow_id, Number(row.revision));
    return {
      revision,
      eventSequence: this.#appendEvent({
        eventId,
        workflowId: row.workflow_id,
        revision,
        type,
        fromState,
        toState,
        fence,
        atMs
      })
    };
  }

  #markExpiredIntents(row, now, idPrefix) {
    const attempts = this.#db.prepare(`
      SELECT attempt_id FROM operation_attempts
      WHERE workflow_id = ? AND state = 'intent'
      ORDER BY created_at_ms, attempt_id
    `).all(row.workflow_id);
    if (attempts.length === 0) return { row, changed: false };

    this.#db.prepare(`
      UPDATE operation_attempts
      SET state = 'outcome_unknown', updated_at_ms = ?
      WHERE workflow_id = ? AND state = 'intent'
    `).run(now, row.workflow_id);
    const toState = TERMINAL.has(row.state) ? row.state : 'reconciliation_pending';
    const updated = this.#appendAtNewRevision(row, {
      eventId: idPrefix + ':unknown:' + Number(row.revision + 1),
      type: 'expired_lease_outcome_unknown',
      toState,
      fence: Number(row.fence_token),
      atMs: now
    });
    let next = this.#workflow(row.workflow_id, 'markExpiredIntents');
    for (let index = 1; index < attempts.length; index += 1) {
      this.#appendEvent({
        eventId: idPrefix + ':unknown-extra:' + index + ':' + attempts[index].attempt_id,
        workflowId: row.workflow_id,
        revision: Number(next.revision),
        type: 'expired_lease_outcome_unknown',
        fromState: next.state,
        toState: next.state,
        fence: Number(next.fence_token),
        atMs: now
      });
    }
    return { row: next, changed: true, eventSequence: updated.eventSequence };
  }

  #storeArtifact(workflowId, artifactId, bytes, now, operation) {
    expectString(artifactId, 'artifactId', operation);
    if (!(bytes instanceof Uint8Array)) {
      fail(operation, 'INVALID_INPUT', 'Artifact content must be supplied as bytes.');
    }
    const copy = Buffer.from(bytes);
    const sha256 = digest(copy);
    const previous = this.#db.prepare(`
      SELECT sha256, bytes FROM immutable_artifacts WHERE workflow_id = ? AND artifact_id = ?
    `).get(workflowId, artifactId);
    if (previous) {
      if (previous.sha256 === sha256 && Buffer.from(previous.bytes).equals(copy)) {
        return { artifactId, sha256, duplicate: true };
      }
      fail(operation, 'ARTIFACT_IMMUTABLE', 'An artifact identifier cannot be reused for different bytes.');
    }
    this.#db.prepare(`
      INSERT INTO immutable_artifacts (workflow_id, artifact_id, sha256, bytes, created_at_ms)
      VALUES (?, ?, ?, ?, ?)
    `).run(workflowId, artifactId, sha256, copy, now);
    return { artifactId, sha256, duplicate: false };
  }

  createWorkflow({ workflowId, checkpoint = {}, eventId } = {}) {
    const operation = 'createWorkflow';
    expectString(workflowId, 'workflowId', operation);
    const payload = serializeCheckpoint(checkpoint, operation);
    const createdAtMs = this.#now(operation);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const identity = this.#eventId(eventId, operation);
      this.#assertEventAvailable(identity, operation);
      if (this.#db.prepare('SELECT 1 FROM workflows WHERE workflow_id = ?').get(workflowId)) {
        fail(operation, 'WORKFLOW_ALREADY_EXISTS', 'A workflow with this identity already exists.');
      }
      this.#db.prepare(`
        INSERT INTO workflows (
          workflow_id, state, revision, fence_token, lease_owner, lease_expires_ms,
          checkpoint_sequence, cancellation_requested, created_at_ms, updated_at_ms
        ) VALUES (?, 'queued', 0, 0, NULL, NULL, 1, 0, ?, ?)
      `).run(workflowId, createdAtMs, createdAtMs);
      this.#db.prepare(`
        INSERT INTO workflow_checkpoints (
          workflow_id, sequence, schema_version, payload_json, sha256, revision, created_at_ms
        ) VALUES (?, 1, ?, ?, ?, 0, ?)
      `).run(workflowId, CHECKPOINT_SCHEMA_VERSION, payload, digest(payload), createdAtMs);
      this.#appendEvent({
        eventId: identity,
        workflowId,
        revision: 0,
        type: 'workflow_created',
        fromState: null,
        toState: 'queued',
        fence: 0,
        atMs: createdAtMs
      });
      return { workflowId, state: 'queued', revision: 0, checkpointSequence: 1 };
    });
  }

  getWorkflow(workflowId) {
    const operation = 'getWorkflow';
    expectString(workflowId, 'workflowId', operation);
    this.#assertOpen(operation);
    try {
      return mapWorkflow(this.#workflow(workflowId, operation));
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  getCheckpoint(workflowId) {
    const operation = 'getCheckpoint';
    expectString(workflowId, 'workflowId', operation);
    this.#assertOpen(operation);
    try {
      const workflow = this.#workflow(workflowId, operation);
      const row = this.#db.prepare(`
        SELECT * FROM workflow_checkpoints WHERE workflow_id = ? AND sequence = ?
      `).get(workflowId, Number(workflow.checkpoint_sequence));
      if (!row) fail(operation, 'CHECKPOINT_CORRUPT', 'The latest workflow checkpoint is missing.');
      if (Number(row.schema_version) > CHECKPOINT_SCHEMA_VERSION) {
        fail(operation, 'UNSUPPORTED_SCHEMA_VERSION', 'The checkpoint schema is newer than this runtime supports.');
      }
      if (
        typeof row.payload_json !== 'string'
        || Buffer.byteLength(row.payload_json, 'utf8') > MAX_CHECKPOINT_SERIALIZED_BYTES
        || digest(row.payload_json) !== row.sha256
      ) {
        fail(operation, 'CHECKPOINT_CORRUPT', 'The latest workflow checkpoint failed its integrity check.');
      }
      let parsed;
      try {
        parsed = JSON.parse(row.payload_json);
      } catch {
        fail(operation, 'CHECKPOINT_CORRUPT', 'The latest workflow checkpoint is invalid.');
      }
      if (parsed.schemaVersion !== CHECKPOINT_SCHEMA_VERSION || !isRecord(parsed.data)) {
        fail(operation, 'CHECKPOINT_CORRUPT', 'The latest workflow checkpoint is invalid.');
      }
      return {
        workflowId,
        sequence: Number(row.sequence),
        schemaVersion: Number(row.schema_version),
        data: parsed.data,
        sha256: row.sha256
      };
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  acquireLease({ workflowId, ownerId, ttlMs, eventId } = {}) {
    const operation = 'acquireLease';
    expectString(workflowId, 'workflowId', operation);
    expectString(ownerId, 'ownerId', operation);
    expectInteger(ttlMs, 'ttlMs', operation, 1);
    if (ttlMs > 86_400_000) fail(operation, 'INVALID_INPUT', 'The worker lease exceeds its supported duration.');
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const row = this.#workflow(workflowId, operation);
      this.#assertMutable(row, operation);
      const now = this.#now(operation);
      if (row.lease_owner !== null && Number(row.lease_expires_ms) > now) {
        fail(operation, 'LEASE_HELD', 'Another worker currently owns this workflow lease.', true);
      }
      const prefix = eventId === undefined ? 'lease:' + this.#idFactory() : expectString(eventId, 'eventId', operation);
      const expired = this.#markExpiredIntents(row, now, prefix);
      const current = expired.row;
      const acquiredEventId = prefix + ':acquired';
      this.#assertEventAvailable(acquiredEventId, operation);
      const fence = Number(current.fence_token) + 1;
      const expiresAtMs = now + ttlMs;
      if (!Number.isSafeInteger(expiresAtMs)) fail(operation, 'INVALID_INPUT', 'The worker lease expiry is outside its supported range.');
      this.#db.prepare(`
        UPDATE workflows SET fence_token = ?, lease_owner = ?, lease_expires_ms = ?, updated_at_ms = ?
        WHERE workflow_id = ?
      `).run(fence, ownerId, expiresAtMs, now, workflowId);
      this.#appendEvent({
        eventId: acquiredEventId,
        workflowId,
        revision: Number(current.revision),
        type: 'lease_acquired',
        fromState: current.state,
        toState: current.state,
        fence,
        atMs: now
      });
      return { workflowId, ownerId, fence, expiresAtMs, revision: Number(current.revision) };
    });
  }

  renewLease({ workflowId, ownerId, fence, ttlMs, eventId } = {}) {
    const operation = 'renewLease';
    expectString(workflowId, 'workflowId', operation);
    expectInteger(ttlMs, 'ttlMs', operation, 1);
    if (ttlMs > 86_400_000) fail(operation, 'INVALID_INPUT', 'The worker lease exceeds its supported duration.');
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const row = this.#workflow(workflowId, operation);
      const now = this.#assertLease(row, { ownerId, fence }, operation);
      this.#assertMutable(row, operation);
      const identity = this.#eventId(eventId, operation);
      this.#assertEventAvailable(identity, operation);
      const expiresAtMs = now + ttlMs;
      if (!Number.isSafeInteger(expiresAtMs)) fail(operation, 'INVALID_INPUT', 'The worker lease expiry is outside its supported range.');
      this.#db.prepare('UPDATE workflows SET lease_expires_ms = ?, updated_at_ms = ? WHERE workflow_id = ?')
        .run(expiresAtMs, now, workflowId);
      this.#appendEvent({
        eventId: identity,
        workflowId,
        revision: Number(row.revision),
        type: 'lease_renewed',
        fromState: row.state,
        toState: row.state,
        fence,
        atMs: now
      });
      return { workflowId, ownerId, fence, expiresAtMs, revision: Number(row.revision) };
    });
  }

  releaseLease({ workflowId, ownerId, fence, eventId } = {}) {
    const operation = 'releaseLease';
    expectString(workflowId, 'workflowId', operation);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      let row = this.#workflow(workflowId, operation);
      const now = this.#assertLease(row, { ownerId, fence }, operation);
      const prefix = eventId === undefined ? 'release:' + this.#idFactory() : expectString(eventId, 'eventId', operation);
      const unresolved = this.#db.prepare(`
        SELECT attempt_id, state FROM operation_attempts
        WHERE workflow_id = ? AND state IN ('intent','outcome_unknown','response_received')
        ORDER BY created_at_ms, attempt_id
      `).all(workflowId);
      const intents = unresolved.filter(attempt => attempt.state === 'intent');
      if (intents.length > 0) {
        this.#db.prepare(`
          UPDATE operation_attempts SET state = 'outcome_unknown', updated_at_ms = ?
          WHERE workflow_id = ? AND state = 'intent'
        `).run(now, workflowId);
      }
      if (unresolved.length > 0 && !TERMINAL.has(row.state) && row.state !== 'reconciliation_pending') {
        const stateEventId = prefix + ':pending';
        this.#assertEventAvailable(stateEventId, operation);
        this.#appendAtNewRevision(row, {
          eventId: stateEventId,
          type: 'lease_released_with_unresolved_work',
          toState: 'reconciliation_pending',
          fence,
          atMs: now
        });
        row = this.#workflow(workflowId, operation);
      } else if (intents.length > 0) {
        const stateEventId = prefix + ':unknown';
        this.#assertEventAvailable(stateEventId, operation);
        this.#appendAtNewRevision(row, {
          eventId: stateEventId,
          type: 'lease_released_with_unknown_outcome',
          fence,
          atMs: now
        });
        row = this.#workflow(workflowId, operation);
      }
      const releaseEventId = prefix + ':released';
      this.#assertEventAvailable(releaseEventId, operation);
      this.#db.prepare(`
        UPDATE workflows SET lease_owner = NULL, lease_expires_ms = NULL, updated_at_ms = ?
        WHERE workflow_id = ?
      `).run(now, workflowId);
      this.#appendEvent({
        eventId: releaseEventId,
        workflowId,
        revision: Number(row.revision),
        type: 'lease_released',
        fromState: row.state,
        toState: row.state,
        fence,
        atMs: now
      });
      return { released: true, state: row.state };
    });
  }

  transitionWorkflow({ workflowId, ownerId, fence, expectedRevision, toState, eventId } = {}) {
    const operation = 'transitionWorkflow';
    expectString(workflowId, 'workflowId', operation);
    if (!STATES.has(toState) || toState === 'cancelled') {
      fail(operation, 'ILLEGAL_TRANSITION', 'The requested state transition is not available.');
    }
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const row = this.#workflow(workflowId, operation);
      const now = this.#assertLease(row, { ownerId, fence }, operation);
      this.#assertMutable(row, operation);
      this.#assertRevision(row, expectedRevision, operation);
      if (row.cancellation_requested && toState === 'running') {
        fail(operation, 'ILLEGAL_TRANSITION', 'A cancellation request cannot return to active execution.');
      }
      if (!TRANSITIONS.get(row.state).has(toState)) {
        fail(operation, 'ILLEGAL_TRANSITION', 'The requested state transition is not allowed.');
      }
      const outstanding = this.#db.prepare(`
          SELECT 1 FROM operation_attempts
          WHERE workflow_id = ? AND state IN ('intent','outcome_unknown','response_received')
          LIMIT 1
        `).get(workflowId);
      if (outstanding && toState !== 'reconciliation_pending') {
        fail(operation, 'OUTCOME_UNRESOLVED', 'An external outcome must remain in reconciliation until resolved.');
      }
      const identity = this.#eventId(eventId, operation);
      this.#assertEventAvailable(identity, operation);
      const revision = Number(row.revision) + 1;
      this.#db.prepare(`
        UPDATE workflows SET state = ?, revision = ?, updated_at_ms = ?
        WHERE workflow_id = ? AND revision = ?
      `).run(toState, revision, now, workflowId, Number(row.revision));
      const eventSequence = this.#appendEvent({
        eventId: identity,
        workflowId,
        revision,
        type: 'state_transition',
        fromState: row.state,
        toState,
        fence,
        atMs: now
      });
      return { state: toState, revision, eventSequence };
    });
  }

  requestCancellation({ workflowId, ownerId, fence, expectedRevision, eventId } = {}) {
    const operation = 'requestCancellation';
    expectString(workflowId, 'workflowId', operation);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const row = this.#workflow(workflowId, operation);
      if (TERMINAL.has(row.state)) {
        return { accepted: false, state: row.state, revision: Number(row.revision) };
      }
      this.#assertLease(row, { ownerId, fence }, operation);
      if (row.cancellation_requested) {
        return { accepted: true, state: row.state, revision: Number(row.revision) };
      }
      this.#assertRevision(row, expectedRevision, operation);
      const now = this.#now(operation);
      const identity = this.#eventId(eventId, operation);
      this.#assertEventAvailable(identity, operation);
      const unresolved = this.#db.prepare(`
        SELECT COUNT(*) AS count FROM operation_attempts
        WHERE workflow_id = ? AND state IN ('intent','outcome_unknown','response_received')
      `).get(workflowId);
      const nextState = unresolved.count > 0 ? 'reconciliation_pending' : 'cancelled';
      const revision = Number(row.revision) + 1;
      this.#db.prepare(`
        UPDATE workflows
        SET state = ?, cancellation_requested = 1, revision = ?, updated_at_ms = ?,
            lease_owner = CASE WHEN ? = 'cancelled' THEN NULL ELSE lease_owner END,
            lease_expires_ms = CASE WHEN ? = 'cancelled' THEN NULL ELSE lease_expires_ms END
        WHERE workflow_id = ? AND revision = ?
      `).run(nextState, revision, now, nextState, nextState, workflowId, Number(row.revision));
      const eventSequence = this.#appendEvent({
        eventId: identity,
        workflowId,
        revision,
        type: unresolved.count > 0 ? 'cancellation_requested_unresolved' : 'workflow_cancelled',
        fromState: row.state,
        toState: nextState,
        fence,
        atMs: now
      });
      return { accepted: true, state: nextState, revision, eventSequence };
    });
  }

  recordDispatchIntent({
    workflowId,
    ownerId,
    fence,
    expectedRevision,
    attemptId,
    logicalOperationRef,
    authorityHoldRef,
    transportAttemptId,
    eventId
  } = {}) {
    const operation = 'recordDispatchIntent';
    expectString(workflowId, 'workflowId', operation);
    expectString(attemptId, 'attemptId', operation);
    expectString(logicalOperationRef, 'logicalOperationRef', operation);
    expectString(authorityHoldRef, 'authorityHoldRef', operation);
    expectString(transportAttemptId, 'transportAttemptId', operation);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const row = this.#workflow(workflowId, operation);
      const now = this.#assertLease(row, { ownerId, fence }, operation);
      this.#assertMutable(row, operation);
      this.#assertRevision(row, expectedRevision, operation);
      const unresolved = this.#db.prepare(`
        SELECT 1 FROM operation_attempts
        WHERE workflow_id = ? AND state IN ('intent','outcome_unknown','response_received')
        LIMIT 1
      `).get(workflowId);
      if (unresolved) fail(operation, 'OUTCOME_UNRESOLVED', 'An earlier external outcome must be reconciled before another intent.');
      const completedOperation = this.#db.prepare(`
        SELECT 1
        FROM operation_attempts a
        JOIN response_receipts r ON r.attempt_id = a.attempt_id
        WHERE a.workflow_id = ? AND a.logical_operation_ref = ? AND r.outcome = 'completed'
        LIMIT 1
      `).get(workflowId, logicalOperationRef);
      if (completedOperation) {
        fail(operation, 'LOGICAL_OPERATION_COMPLETED', 'This logical operation already has a completed durable response.');
      }
      if (row.state !== 'running' || row.cancellation_requested) {
        fail(operation, 'ILLEGAL_TRANSITION', 'External intent can only be recorded for an active uncancelled workflow.');
      }
      if (this.#db.prepare('SELECT 1 FROM operation_attempts WHERE attempt_id = ?').get(attemptId)) {
        fail(operation, 'ATTEMPT_ALREADY_EXISTS', 'This billable-attempt identity has already been recorded.');
      }
      if (this.#db.prepare('SELECT 1 FROM transport_attempts WHERE transport_attempt_id = ?').get(transportAttemptId)) {
        fail(operation, 'ATTEMPT_ALREADY_EXISTS', 'This transport-attempt identity has already been recorded.');
      }
      const identity = this.#eventId(eventId, operation);
      this.#assertEventAvailable(identity, operation);
      this.#db.prepare(`
        INSERT INTO operation_attempts (
          attempt_id, workflow_id, logical_operation_ref, authority_hold_ref, state, created_at_ms, updated_at_ms
        ) VALUES (?, ?, ?, ?, 'intent', ?, ?)
      `).run(attemptId, workflowId, logicalOperationRef, authorityHoldRef, now, now);
      this.#db.prepare(`
        INSERT INTO transport_attempts (transport_attempt_id, attempt_id, created_at_ms)
        VALUES (?, ?, ?)
      `).run(transportAttemptId, attemptId, now);
      const revision = Number(row.revision) + 1;
      this.#db.prepare("UPDATE workflows SET state = 'awaiting_response', revision = ?, updated_at_ms = ? WHERE workflow_id = ?")
        .run(revision, now, workflowId);
      const eventSequence = this.#appendEvent({
        eventId: identity,
        workflowId,
        revision,
        type: 'dispatch_intent_persisted',
        fromState: row.state,
        toState: 'awaiting_response',
        fence,
        atMs: now
      });
      return { attemptId, state: 'awaiting_response', revision, eventSequence };
    });
  }

  recordTransportAttempt({ workflowId, ownerId, fence, expectedRevision, attemptId, transportAttemptId, eventId } = {}) {
    const operation = 'recordTransportAttempt';
    expectString(workflowId, 'workflowId', operation);
    expectString(attemptId, 'attemptId', operation);
    expectString(transportAttemptId, 'transportAttemptId', operation);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const row = this.#workflow(workflowId, operation);
      const now = this.#assertLease(row, { ownerId, fence }, operation);
      this.#assertMutable(row, operation);
      this.#assertRevision(row, expectedRevision, operation);
      if (row.cancellation_requested || row.state !== 'awaiting_response') {
        fail(operation, 'ATTEMPT_STATE_INVALID', 'A cancelled or inactive workflow cannot record a new transport retry.');
      }
      const attempt = this.#db.prepare('SELECT * FROM operation_attempts WHERE attempt_id = ?').get(attemptId);
      if (!attempt || attempt.workflow_id !== workflowId) fail(operation, 'ATTEMPT_NOT_FOUND', 'The operation attempt does not exist.');
      if (attempt.state !== 'intent') fail(operation, 'ATTEMPT_STATE_INVALID', 'The operation attempt is not open for transport retries.');
      const previous = this.#db.prepare('SELECT attempt_id FROM transport_attempts WHERE transport_attempt_id = ?')
        .get(transportAttemptId);
      if (previous) {
        if (previous.attempt_id === attemptId) {
          return { transportAttemptId, revision: Number(row.revision), eventSequence: null, duplicate: true };
        }
        fail(operation, 'ATTEMPT_ALREADY_EXISTS', 'This transport-attempt identity has already been recorded.');
      }
      const identity = this.#eventId(eventId, operation);
      this.#assertEventAvailable(identity, operation);
      this.#db.prepare('INSERT INTO transport_attempts (transport_attempt_id, attempt_id, created_at_ms) VALUES (?, ?, ?)')
        .run(transportAttemptId, attemptId, now);
      const revision = Number(row.revision) + 1;
      this.#db.prepare('UPDATE workflows SET revision = ?, updated_at_ms = ? WHERE workflow_id = ?')
        .run(revision, now, workflowId);
      const eventSequence = this.#appendEvent({
        eventId: identity,
        workflowId,
        revision,
        type: 'transport_attempt_persisted',
        fromState: row.state,
        toState: row.state,
        fence,
        atMs: now
      });
      return { transportAttemptId, revision, eventSequence, duplicate: false };
    });
  }

  recordResponseReceipt({
    workflowId,
    ownerId,
    fence,
    expectedRevision,
    attemptId,
    receiptId,
    outcome,
    nextState,
    responseBytes,
    nextCheckpoint,
    artifacts = [],
    eventId
  } = {}) {
    const operation = 'recordResponseReceipt';
    expectString(workflowId, 'workflowId', operation);
    expectString(attemptId, 'attemptId', operation);
    expectString(receiptId, 'receiptId', operation);
    if (!['completed', 'no_effect'].includes(outcome) || !['running', 'succeeded', 'failed'].includes(nextState)) {
      fail(operation, 'INVALID_INPUT', 'The response outcome or next workflow state is invalid.');
    }
    if (!(responseBytes instanceof Uint8Array) || !Array.isArray(artifacts)) {
      fail(operation, 'INVALID_INPUT', 'The response and artifact content must be supplied as bytes.');
    }
    const bytes = Buffer.from(responseBytes);
    const checkpointJson = serializeCheckpoint(nextCheckpoint, operation);
    const responseSha256 = digest(bytes);
    const checkpointSha256 = digest(checkpointJson);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const row = this.#workflow(workflowId, operation);
      const now = this.#assertLease(row, { ownerId, fence }, operation);
      this.#assertMutable(row, operation);
      this.#assertRevision(row, expectedRevision, operation);
      const attempt = this.#db.prepare('SELECT * FROM operation_attempts WHERE attempt_id = ?').get(attemptId);
      if (!attempt || attempt.workflow_id !== workflowId) fail(operation, 'ATTEMPT_NOT_FOUND', 'The operation attempt does not exist.');
      if (!['intent', 'outcome_unknown'].includes(attempt.state)) {
        fail(operation, 'ATTEMPT_STATE_INVALID', 'The operation attempt cannot accept another response receipt.');
      }
      if (this.#db.prepare('SELECT 1 FROM response_receipts WHERE receipt_id = ?').get(receiptId)) {
        fail(operation, 'IDEMPOTENCY_CONFLICT', 'This response-receipt identity has already been used.');
      }
      if (this.#db.prepare('SELECT 1 FROM response_receipts WHERE attempt_id = ?').get(attemptId)) {
        fail(operation, 'ATTEMPT_STATE_INVALID', 'A response receipt already exists for this attempt.');
      }
      const identity = this.#eventId(eventId, operation);
      this.#assertEventAvailable(identity, operation);
      for (const artifact of artifacts) {
        this.#storeArtifact(workflowId, artifact.artifactId, artifact.bytes, now, operation);
      }
      this.#db.prepare(`
        INSERT INTO response_receipts (
          receipt_id, attempt_id, outcome, next_state, response_bytes, response_sha256,
          next_checkpoint_json, next_checkpoint_sha256, created_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(receiptId, attemptId, outcome, nextState, bytes, responseSha256, checkpointJson, checkpointSha256, now);
      this.#db.prepare(`
        UPDATE operation_attempts SET state = 'response_received', updated_at_ms = ? WHERE attempt_id = ?
      `).run(now, attemptId);
      const revision = Number(row.revision) + 1;
      this.#db.prepare('UPDATE workflows SET revision = ?, updated_at_ms = ? WHERE workflow_id = ?')
        .run(revision, now, workflowId);
      const eventSequence = this.#appendEvent({
        eventId: identity,
        workflowId,
        revision,
        type: 'response_receipt_persisted',
        fromState: row.state,
        toState: row.state,
        fence,
        atMs: now
      });
      return { receiptId, responseSha256, revision, eventSequence };
    });
  }

  markOutcomeUnknown({ workflowId, ownerId, fence, expectedRevision, attemptId, eventId } = {}) {
    const operation = 'markOutcomeUnknown';
    expectString(workflowId, 'workflowId', operation);
    expectString(attemptId, 'attemptId', operation);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const row = this.#workflow(workflowId, operation);
      const now = this.#assertLease(row, { ownerId, fence }, operation);
      this.#assertMutable(row, operation);
      this.#assertRevision(row, expectedRevision, operation);
      const attempt = this.#db.prepare('SELECT * FROM operation_attempts WHERE attempt_id = ?').get(attemptId);
      if (!attempt || attempt.workflow_id !== workflowId) fail(operation, 'ATTEMPT_NOT_FOUND', 'The operation attempt does not exist.');
      if (attempt.state !== 'intent') fail(operation, 'ATTEMPT_STATE_INVALID', 'Only an unresolved dispatch intent can become unknown.');
      const identity = this.#eventId(eventId, operation);
      this.#assertEventAvailable(identity, operation);
      this.#db.prepare(`
        UPDATE operation_attempts SET state = 'outcome_unknown', updated_at_ms = ? WHERE attempt_id = ?
      `).run(now, attemptId);
      const revision = Number(row.revision) + 1;
      this.#db.prepare(`
        UPDATE workflows SET state = 'reconciliation_pending', revision = ?, updated_at_ms = ?
        WHERE workflow_id = ?
      `).run(revision, now, workflowId);
      const eventSequence = this.#appendEvent({
        eventId: identity,
        workflowId,
        revision,
        type: 'external_outcome_unknown',
        fromState: row.state,
        toState: 'reconciliation_pending',
        fence,
        atMs: now
      });
      return { state: 'reconciliation_pending', revision, eventSequence, authorityHoldRef: attempt.authority_hold_ref };
    });
  }

  reconcilePendingReceipts({ workflowId, ownerId, fence, expectedRevision, eventIdPrefix } = {}) {
    const operation = 'reconcilePendingReceipts';
    expectString(workflowId, 'workflowId', operation);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      let row = this.#workflow(workflowId, operation);
      const now = this.#assertLease(row, { ownerId, fence }, operation);
      this.#assertMutable(row, operation);
      this.#assertRevision(row, expectedRevision, operation);
      const pending = this.#db.prepare(`
        SELECT r.*, a.state AS attempt_state
        FROM response_receipts r
        JOIN operation_attempts a ON a.attempt_id = r.attempt_id
        WHERE a.workflow_id = ? AND a.state = 'response_received'
        ORDER BY r.created_at_ms, r.receipt_id
      `).all(workflowId);
      if (pending.length === 0) {
        return {
          reconciled: 0,
          state: row.state,
          revision: Number(row.revision),
          checkpointSequence: Number(row.checkpoint_sequence)
        };
      }

      let reconciled = 0;
      let revision = Number(row.revision);
      let sequence = Number(row.checkpoint_sequence);
      let currentState = row.state;
      const prefix = eventIdPrefix === undefined ? 'reconcile:' + this.#idFactory() : expectString(eventIdPrefix, 'eventIdPrefix', operation);
      for (let index = 0; index < pending.length; index += 1) {
        const receipt = pending[index];
        if (digest(Buffer.from(receipt.response_bytes)) !== receipt.response_sha256) {
          fail(operation, 'CHECKPOINT_CORRUPT', 'A durable response failed its integrity check.');
        }
        if (
          typeof receipt.next_checkpoint_json !== 'string'
          || typeof receipt.next_checkpoint_sha256 !== 'string'
          || Buffer.byteLength(receipt.next_checkpoint_json, 'utf8') > MAX_CHECKPOINT_SERIALIZED_BYTES
          || digest(receipt.next_checkpoint_json) !== receipt.next_checkpoint_sha256
        ) {
          fail(operation, 'CHECKPOINT_CORRUPT', 'A durable response checkpoint failed its integrity check.');
        }
        let envelope;
        try {
          envelope = JSON.parse(receipt.next_checkpoint_json);
        } catch {
          fail(operation, 'CHECKPOINT_CORRUPT', 'A durable response checkpoint is invalid.');
        }
        if (envelope?.schemaVersion !== CHECKPOINT_SCHEMA_VERSION || !isRecord(envelope?.data)) {
          fail(operation, 'UNSUPPORTED_SCHEMA_VERSION', 'A durable response checkpoint has an unsupported schema.');
        }
        const nextSequence = sequence + 1;
        const nextRevision = revision + 1;
        const checkpointJson = receipt.next_checkpoint_json;
        const target = row.cancellation_requested && receipt.next_state === 'running'
          ? 'cancelled'
          : receipt.next_state;
        if (target !== currentState && target !== 'cancelled' && !TRANSITIONS.get(currentState).has(target)) {
          fail(operation, 'ILLEGAL_TRANSITION', 'The recovered response conflicts with the workflow state.');
        }
        const identity = prefix + ':' + index + ':' + receipt.receipt_id;
        this.#assertEventAvailable(identity, operation);
        this.#db.prepare(`
          INSERT INTO workflow_checkpoints (
            workflow_id, sequence, schema_version, payload_json, sha256, revision, created_at_ms
          ) VALUES (?, ?, ?, ?, ?, ?, ?)
        `).run(workflowId, nextSequence, CHECKPOINT_SCHEMA_VERSION, checkpointJson, receipt.next_checkpoint_sha256, nextRevision, now);
        this.#db.prepare(`
          UPDATE operation_attempts SET state = 'reconciled', updated_at_ms = ? WHERE attempt_id = ?
        `).run(now, receipt.attempt_id);
        this.#db.prepare(`
          UPDATE workflows
          SET state = ?, revision = ?, checkpoint_sequence = ?, updated_at_ms = ?
          WHERE workflow_id = ?
        `).run(target, nextRevision, nextSequence, now, workflowId);
        this.#appendEvent({
          eventId: identity,
          workflowId,
          revision: nextRevision,
          type: 'response_checkpoint_reconciled',
          fromState: currentState,
          toState: target,
          fence,
          atMs: now
        });
        reconciled += 1;
        sequence = nextSequence;
        revision = nextRevision;
        currentState = target;
        row = this.#workflow(workflowId, operation);
        if (TERMINAL.has(currentState) && index < pending.length - 1) {
          fail(operation, 'OUTCOME_UNRESOLVED', 'A terminal response left another durable response to reconcile.');
        }
      }
      return { reconciled, state: currentState, revision, checkpointSequence: sequence };
    });
  }

  recordAttemptObservation({ attemptId, sourceObservationRef, kind } = {}) {
    const operation = 'recordAttemptObservation';
    expectString(attemptId, 'attemptId', operation);
    expectString(sourceObservationRef, 'sourceObservationRef', operation);
    if (!['known', 'unknown'].includes(kind)) fail(operation, 'INVALID_INPUT', 'The observation kind is invalid.');
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      if (!this.#db.prepare('SELECT 1 FROM operation_attempts WHERE attempt_id = ?').get(attemptId)) {
        fail(operation, 'ATTEMPT_NOT_FOUND', 'The operation attempt does not exist.');
      }
      const fingerprint = digest(attemptId + ':' + kind);
      const previous = this.#db.prepare(`
        SELECT attempt_id, observation_kind, fingerprint
        FROM attempt_observations WHERE source_observation_ref = ?
      `).get(sourceObservationRef);
      if (previous) {
        if (
          previous.attempt_id !== attemptId
          || previous.observation_kind !== kind
          || previous.fingerprint !== fingerprint
        ) {
          fail(operation, 'OBSERVATION_IDENTITY_CONFLICT', 'The source observation identity conflicts with its prior record.');
        }
        const count = this.#db.prepare(`
          SELECT COUNT(*) AS count FROM attempt_observations WHERE attempt_id = ?
        `).get(attemptId).count;
        return { duplicate: true, observationCount: Number(count) };
      }
      const now = this.#now(operation);
      this.#db.prepare(`
        INSERT INTO attempt_observations (
          source_observation_ref, attempt_id, observation_kind, fingerprint, observed_at_ms
        ) VALUES (?, ?, ?, ?, ?)
      `).run(sourceObservationRef, attemptId, kind, fingerprint, now);
      const count = this.#db.prepare(`
        SELECT COUNT(*) AS count FROM attempt_observations WHERE attempt_id = ?
      `).get(attemptId).count;
      return { duplicate: false, observationCount: Number(count) };
    });
  }

  getAttempt(attemptId) {
    const operation = 'getAttempt';
    expectString(attemptId, 'attemptId', operation);
    this.#assertOpen(operation);
    try {
      const row = this.#db.prepare('SELECT * FROM operation_attempts WHERE attempt_id = ?').get(attemptId);
      if (!row) fail(operation, 'ATTEMPT_NOT_FOUND', 'The operation attempt does not exist.');
      const transports = this.#db.prepare(`
        SELECT transport_attempt_id, created_at_ms FROM transport_attempts
        WHERE attempt_id = ? ORDER BY created_at_ms, transport_attempt_id
      `).all(attemptId);
      return {
        attemptId: row.attempt_id,
        workflowId: row.workflow_id,
        logicalOperationRef: row.logical_operation_ref,
        authorityHoldRef: row.authority_hold_ref,
        state: row.state,
        transportAttempts: transports.map(item => ({
          transportAttemptId: item.transport_attempt_id,
          createdAtMs: Number(item.created_at_ms)
        }))
      };
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  getReceipt(attemptId) {
    const operation = 'getReceipt';
    expectString(attemptId, 'attemptId', operation);
    this.#assertOpen(operation);
    try {
      const row = this.#db.prepare('SELECT * FROM response_receipts WHERE attempt_id = ?').get(attemptId);
      if (!row) fail(operation, 'ATTEMPT_NOT_FOUND', 'The response receipt does not exist.');
      const bytes = Buffer.from(row.response_bytes);
      if (digest(bytes) !== row.response_sha256) {
        fail(operation, 'CHECKPOINT_CORRUPT', 'The durable response failed its integrity check.');
      }
      return {
        receiptId: row.receipt_id,
        attemptId,
        outcome: row.outcome,
        nextState: row.next_state,
        responseSha256: row.response_sha256,
        responseBytes: bytes,
        createdAtMs: Number(row.created_at_ms)
      };
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  getArtifact(workflowId, artifactId) {
    const operation = 'getArtifact';
    expectString(workflowId, 'workflowId', operation);
    expectString(artifactId, 'artifactId', operation);
    this.#assertOpen(operation);
    try {
      const row = this.#db.prepare(`
        SELECT * FROM immutable_artifacts WHERE workflow_id = ? AND artifact_id = ?
      `).get(workflowId, artifactId);
      if (!row) fail(operation, 'ARTIFACT_NOT_FOUND', 'The immutable artifact does not exist.');
      const bytes = Buffer.from(row.bytes);
      if (digest(bytes) !== row.sha256) fail(operation, 'CHECKPOINT_CORRUPT', 'The immutable artifact failed its integrity check.');
      return { workflowId, artifactId, sha256: row.sha256, bytes };
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  listEvents({ workflowId, afterSequence = 0 } = {}) {
    const operation = 'listEvents';
    expectInteger(afterSequence, 'afterSequence', operation);
    if (workflowId !== undefined) expectString(workflowId, 'workflowId', operation);
    this.#assertOpen(operation);
    try {
      const rows = workflowId === undefined
        ? this.#db.prepare(`
            SELECT * FROM workflow_events WHERE sequence > ? ORDER BY sequence
          `).all(afterSequence)
        : this.#db.prepare(`
            SELECT * FROM workflow_events WHERE workflow_id = ? AND sequence > ? ORDER BY sequence
          `).all(workflowId, afterSequence);
      return rows.map(row => ({
        sequence: Number(row.sequence),
        eventId: row.event_id,
        workflowId: row.workflow_id,
        revision: Number(row.revision),
        type: row.event_type,
        fromState: row.from_state,
        toState: row.to_state,
        fence: Number(row.fence_token),
        occurredAtMs: Number(row.occurred_at_ms)
      }));
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  readEventPage({ afterSequence = 0, limit = 100, workflowId } = {}) {
    const operation = 'readEventPage';
    expectInteger(afterSequence, 'afterSequence', operation);
    expectInteger(limit, 'limit', operation, 1);
    if (limit > 500) fail(operation, 'INVALID_INPUT', 'The event page exceeds its supported record limit.');
    if (workflowId !== undefined) expectString(workflowId, 'workflowId', operation);
    this.#assertOpen(operation);
    try {
      const rows = workflowId === undefined
        ? this.#db.prepare(`
            SELECT sequence, event_id, workflow_id, revision, event_type,
                   from_state, to_state, fence_token, occurred_at_ms
            FROM workflow_events WHERE sequence > ? ORDER BY sequence LIMIT ?
          `).all(afterSequence, limit + 1)
        : this.#db.prepare(`
            SELECT sequence, event_id, workflow_id, revision, event_type,
                   from_state, to_state, fence_token, occurred_at_ms
            FROM workflow_events WHERE workflow_id = ? AND sequence > ? ORDER BY sequence LIMIT ?
          `).all(workflowId, afterSequence, limit + 1);
      const hasMore = rows.length > limit;
      if (hasMore) rows.pop();
      const events = rows.map(row => ({
        sequence: Number(row.sequence),
        eventId: row.event_id,
        workflowId: row.workflow_id,
        revision: Number(row.revision),
        type: row.event_type,
        fromState: row.from_state,
        toState: row.to_state,
        fence: Number(row.fence_token),
        occurredAtMs: Number(row.occurred_at_ms)
      }));
      if (events.some(event => !Number.isSafeInteger(event.sequence)
          || !Number.isSafeInteger(event.revision)
          || !Number.isSafeInteger(event.fence)
          || !Number.isSafeInteger(event.occurredAtMs))) {
        fail(operation, 'CHECKPOINT_CORRUPT', 'The event page contains an unsupported numeric value.');
      }
      return {
        events,
        hasMore,
        nextAfterSequence: events.length === 0 ? afterSequence : events.at(-1).sequence
      };
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  countEventRowsAfter(afterSequence = 0, cap = 10_000) {
    const operation = 'countEventRowsAfter';
    expectInteger(afterSequence, 'afterSequence', operation);
    expectInteger(cap, 'cap', operation, 1);
    if (cap > 1_000_000) fail(operation, 'INVALID_INPUT', 'The event count cap exceeds its supported range.');
    this.#assertOpen(operation);
    try {
      const row = this.#db.prepare(`
        SELECT COUNT(*) AS count FROM (
          SELECT sequence FROM workflow_events WHERE sequence > ? ORDER BY sequence LIMIT ?
        )
      `).get(afterSequence, cap + 1);
      const count = Number(row.count);
      return { count: Math.min(count, cap), capped: count > cap };
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  getTelemetryCursor(consumerId = 'internal-telemetry-v1') {
    const operation = 'getTelemetryCursor';
    if (typeof consumerId !== 'string' || !/^[a-z0-9][a-z0-9._-]{0,63}$/i.test(consumerId)) {
      fail(operation, 'INVALID_INPUT', 'The telemetry consumer identity is invalid.');
    }
    this.#assertOpen(operation);
    try {
      const row = this.#db.prepare(
        'SELECT last_sequence FROM telemetry_cursors WHERE consumer_id = ?'
      ).get(consumerId);
      const cursor = row ? Number(row.last_sequence) : 0;
      if (!Number.isSafeInteger(cursor) || cursor < 0) {
        fail(operation, 'CHECKPOINT_CORRUPT', 'The telemetry cursor contains an unsupported numeric value.');
      }
      return cursor;
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  advanceTelemetryCursor({ consumerId = 'internal-telemetry-v1', expectedSequence, nextSequence } = {}) {
    const operation = 'advanceTelemetryCursor';
    if (typeof consumerId !== 'string' || !/^[a-z0-9][a-z0-9._-]{0,63}$/i.test(consumerId)) {
      fail(operation, 'INVALID_INPUT', 'The telemetry consumer identity is invalid.');
    }
    expectInteger(expectedSequence, 'expectedSequence', operation);
    expectInteger(nextSequence, 'nextSequence', operation, 1);
    if (nextSequence - expectedSequence !== 1) {
      fail(operation, 'INVALID_INPUT', 'The telemetry cursor advances one event at a time.');
    }
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const currentRow = this.#db.prepare(
        'SELECT last_sequence FROM telemetry_cursors WHERE consumer_id = ?'
      ).get(consumerId);
      const current = currentRow ? Number(currentRow.last_sequence) : 0;
      if (current !== expectedSequence) {
        fail(operation, 'CONCURRENT_MODIFICATION', 'The telemetry cursor changed before it could be advanced.');
      }
      const available = Number(this.#db.prepare(`
        SELECT COUNT(*) AS count FROM workflow_events
        WHERE sequence > ? AND sequence <= ?
      `).get(expectedSequence, nextSequence).count);
      if (available !== 1) {
        fail(operation, 'CONCURRENT_MODIFICATION', 'The telemetry cursor cannot skip an unavailable event.');
      }
      const now = this.#now(operation);
      if (currentRow) {
        const changed = this.#db.prepare(`
          UPDATE telemetry_cursors SET last_sequence = ?, updated_at_ms = ?
          WHERE consumer_id = ? AND last_sequence = ?
        `).run(nextSequence, now, consumerId, expectedSequence);
        if (Number(changed.changes) !== 1) {
          fail(operation, 'CONCURRENT_MODIFICATION', 'The telemetry cursor changed before it could be advanced.');
        }
      } else {
        if (expectedSequence !== 0) {
          fail(operation, 'CONCURRENT_MODIFICATION', 'The telemetry cursor has no matching starting point.');
        }
        this.#db.prepare(`
          INSERT INTO telemetry_cursors (consumer_id, last_sequence, updated_at_ms) VALUES (?, ?, ?)
        `).run(consumerId, nextSequence, now);
      }
      return nextSequence;
    });
  }

  readOutbox({ afterSequence = 0, limit = 100 } = {}) {
    const operation = 'readOutbox';
    expectInteger(afterSequence, 'afterSequence', operation);
    expectInteger(limit, 'limit', operation, 1);
    if (limit > 1000) fail(operation, 'INVALID_INPUT', 'The outbox read limit is too large.');
    this.#assertOpen(operation);
    try {
      return this.#db.prepare(`
        SELECT o.event_id, o.event_sequence, o.delivered_at_ms, o.delivery_attempts,
               e.workflow_id, e.event_type, e.occurred_at_ms
        FROM outbox o
        JOIN workflow_events e ON e.sequence = o.event_sequence
        WHERE o.event_sequence > ? AND o.delivered_at_ms IS NULL
        ORDER BY o.event_sequence
        LIMIT ?
      `).all(afterSequence, limit).map(row => ({
        eventId: row.event_id,
        sequence: Number(row.event_sequence),
        workflowId: row.workflow_id,
        type: row.event_type,
        occurredAtMs: Number(row.occurred_at_ms),
        deliveryAttempts: Number(row.delivery_attempts)
      }));
    } catch (error) {
      throw mapStorageError(operation, error);
    }
  }

  acknowledgeOutbox(eventId) {
    const operation = 'acknowledgeOutbox';
    expectString(eventId, 'eventId', operation);
    this.#assertOpen(operation);
    return inTransaction(this.#db, operation, () => {
      const now = this.#now(operation);
      const result = this.#db.prepare(`
        UPDATE outbox
        SET delivered_at_ms = COALESCE(delivered_at_ms, ?),
            delivery_attempts = CASE WHEN delivered_at_ms IS NULL THEN delivery_attempts + 1 ELSE delivery_attempts END
        WHERE event_id = ?
      `).run(now, eventId);
      if (Number(result.changes) > 0) return { acknowledged: true };
      const exists = this.#db.prepare('SELECT 1 FROM outbox WHERE event_id = ?').get(eventId);
      return { acknowledged: Boolean(exists) };
    });
  }
}
