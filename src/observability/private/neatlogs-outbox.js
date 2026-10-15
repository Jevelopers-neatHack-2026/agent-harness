import {
  closeSync, constants, fstatSync, fsyncSync, lstatSync, mkdirSync, opendirSync, openSync,
  readSync, renameSync, unlinkSync, writeSync
} from 'node:fs';
import { randomBytes, randomUUID } from 'node:crypto';
import { basename, dirname, join, resolve } from 'node:path';
import { DurableWorkflowStore } from '../../internal/durable-store.js';
import { createHmacTokenizer, createTelemetryExporter } from '../../internal/telemetry.js';
import { loadNeatlogsConfig, scrubNeatlogsEnvironment } from './neatlogs-config.js';
import {
  canonicalNeatlogsInput,
  deterministicNeatlogsIds,
  encodeNeatlogsExportRequest,
  NEATLOGS_LIVE_WORKFLOW_NAME,
  nextNeatlogsGeneration,
  parseNeatlogsJson,
  planNeatlogsExport,
  validateNeatlogsTags
} from './neatlogs-otlp.js';
import { readbackNeatlogsPlan, sendNeatlogsPlan } from './neatlogs-live.js';
import { validateNeatlogsSourceRecords } from './neatlogs-sdk-mapper.js';
import {
  assertIntentTarget, createOutboxIntent, OUTBOX_INTENT_LIMITS,
  restoreOutboxIntents, summarizeOutboxIntent
} from './neatlogs-outbox-intent.js';

// Durable local outbox between the harness TelemetryExporter and the Neatlogs
// OTLP sender. The sink only appends a validated batch of already-projected
// records to this outbox and performs no network I/O. Its filesystem calls are
// synchronous and cannot be preempted by an exporter timeout. Network delivery
// happens later in `drainNeatlogsOutbox`, which records each batch's send and
// readback state.
//
// Persisted batch states are restricted to this sender/readback state machine.
// Retryable send outcomes stay pending_send until readback changes the state.
// Retries restore the exact durable wire intent, including its IDs and times.

export const NEATLOGS_OUTBOX_SCHEMA = 'agent-harness.neatlogs.outbox-batch.v2';
const LEGACY_OUTBOX_SCHEMA = 'agent-harness.neatlogs.outbox-batch.v1';
const BATCH_FILE = /^batch-(\d{12,16})-(\d{12,16})\.json$/;
// Four immutable wire snapshots and their bounded call/readback history fit
// under this hard cap. Oversize persistence fails before the dependent POST.
const MAX_BATCH_FILE_BYTES = 2 * 1024 * 1024;
const MAX_BATCHES_PER_DRAIN = 32;
const DEFAULT_MAX_RETAINED_BATCHES = 128;
const MAX_RETAINED_BATCHES = 1024;
const DRAIN_CURSOR_FILE = 'drain-cursor.json';
const DRAIN_CURSOR_SCHEMA = 'agent-harness.neatlogs.outbox-drain-cursor.v1';
const MAX_DRAIN_CURSOR_BYTES = 256;
const RETRYABLE_SEND = new Set(['outcome_unknown', 'not_accepted', 'aborted', 'not_configured']);
const TERMINAL_READBACK = new Set(['verified', 'mismatch']);
const BATCH_STATES = new Set([
  'pending_send', 'receiver_accepted', 'partially_rejected', 'rejected', 'verified',
  'pending', 'missing', 'mismatch', 'readback_failed', 'readback_unavailable', 'blocked', 'held', 'legacy_held'
]);
const REVISION = /^[0-9a-f]{40}$/;
const MODE = /^[a-z][a-z0-9-]{0,31}$/;

export class NeatlogsOutboxError extends Error {
  constructor(code) {
    super(`Neatlogs outbox operation failed (${code}).`);
    this.name = 'NeatlogsOutboxError';
    this.code = code;
  }
}

function pad(sequence) {
  return String(sequence).padStart(16, '0');
}

function batchRange(path) {
  const match = BATCH_FILE.exec(basename(path));
  if (!match) throw new NeatlogsOutboxError('BATCH_INVALID');
  const first = BigInt(match[1]);
  const last = BigInt(match[2]);
  if (first < 1n || last < first || last > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new NeatlogsOutboxError('BATCH_INVALID');
  }
  return { first: Number(first), last: Number(last) };
}

function syncDirectory(path) {
  const fd = openSync(path, 'r');
  try {
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
}

function writeDurably(path, text) {
  const temporary = `${path}.${process.pid}.${randomUUID()}.tmp`;
  const fd = openSync(temporary, 'wx', 0o600);
  try {
    const bytes = Buffer.from(text, 'utf8');
    let offset = 0;
    while (offset < bytes.length) {
      const written = writeSync(fd, bytes, offset, bytes.length - offset, null);
      if (!Number.isSafeInteger(written) || written <= 0) throw new NeatlogsOutboxError('DURABLE_WRITE_FAILED');
      offset += written;
    }
    fsyncSync(fd);
    closeSync(fd);
    renameSync(temporary, path);
    syncDirectory(dirname(path));
  } catch (error) {
    try { closeSync(fd); } catch { /* already closed or close failed */ }
    try { unlinkSync(temporary); } catch { /* rename may already have published it */ }
    throw error;
  }
}

function readBatchBytes(path) {
  let fd;
  try {
    // Bind validation and reads to one regular file. A size sample alone
    // cannot cap a concurrent append, and a pathname read can follow a swap.
    fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    const stats = fstatSync(fd);
    if (!stats.isFile() || stats.size < 1 || stats.size > MAX_BATCH_FILE_BYTES) {
      throw new NeatlogsOutboxError('BATCH_INVALID');
    }
    const bytes = Buffer.alloc(MAX_BATCH_FILE_BYTES + 1);
    let length = 0;
    while (length < bytes.length) {
      const count = readSync(fd, bytes, length, bytes.length - length, length);
      if (count === 0) break;
      length += count;
    }
    if (length < 1 || length > MAX_BATCH_FILE_BYTES) throw new NeatlogsOutboxError('BATCH_INVALID');
    return bytes.subarray(0, length).toString('utf8');
  } catch {
    throw new NeatlogsOutboxError('BATCH_INVALID');
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
}

function readBatch(path) {
  const batch = parseNeatlogsJson(readBatchBytes(path));
  if (![NEATLOGS_OUTBOX_SCHEMA, LEGACY_OUTBOX_SCHEMA].includes(batch?.schema) || !BATCH_STATES.has(batch.state)) {
    throw new NeatlogsOutboxError('BATCH_INVALID');
  }
  batch.records = validateNeatlogsSourceRecords(batch.records);
  const range = batchRange(path);
  if (batch.firstSequence !== range.first || batch.lastSequence !== range.last
      || batch.firstSequence !== batch.records[0].sequence
      || batch.lastSequence !== batch.records.at(-1).sequence) {
    throw new NeatlogsOutboxError('BATCH_INVALID');
  }
  if (batch.schema === NEATLOGS_OUTBOX_SCHEMA) {
    const { currentPlan } = restoreOutboxIntents(batch.records, batch.dispatches);
    if (JSON.stringify(batch.plan) !== JSON.stringify(currentPlan === null ? null : serializablePlan(currentPlan))) {
      throw new NeatlogsOutboxError('PLAN_IDENTITY_MISMATCH');
    }
    refreshSendSummary(batch);
  }
  return batch;
}

function refreshSendSummary(batch) {
  if (batch.schema !== NEATLOGS_OUTBOX_SCHEMA || batch.dispatches.length === 0) return;
  const intent = batch.dispatches.at(-1);
  batch.send = summarizeOutboxIntent(intent);
  if (intent.purpose === 'recovery') {
    if (!batch.recovery || typeof batch.recovery !== 'object' || Array.isArray(batch.recovery)) {
      throw new NeatlogsOutboxError('BATCH_INVALID');
    }
    batch.recovery.send = batch.send === null ? null : { ...batch.send };
  }
}

function persistBatch(path, batch) {
  const text = `${JSON.stringify(batch, null, 2)}\n`;
  if (Buffer.byteLength(text, 'utf8') > MAX_BATCH_FILE_BYTES) throw new NeatlogsOutboxError('BATCH_TOO_LARGE');
  writeDurably(path, text);
}

function recordsJson(records) {
  return JSON.stringify(records.map(record => ({ ...record })));
}

// Admission must not allocate an array for an already over-cap directory.
// Only batch files and the one scheduling cursor belong in this directory.
// Unexpected artifacts (including abandoned temporary writes) are retained
// for diagnosis and refuse admission rather than escaping the inventory.
function admissionInventory(root, maxRetainedBatches) {
  const paths = [];
  const directory = opendirSync(root, { bufferSize: 32 });
  try {
    let entry;
    let examined = 0;
    while ((entry = directory.readSync()) !== null) {
      if (++examined > maxRetainedBatches + 1) return null;
      if (!entry.isFile()) return null;
      const path = join(root, entry.name);
      if (entry.name === DRAIN_CURSOR_FILE) {
        const stats = lstatSync(path);
        if (!stats.isFile() || stats.size < 1 || stats.size > MAX_DRAIN_CURSOR_BYTES) return null;
        continue;
      }
      if (!BATCH_FILE.test(entry.name) || paths.length === maxRetainedBatches) return null;
      batchRange(path);
      const stats = lstatSync(path);
      if (!stats.isFile() || stats.size < 1 || stats.size > MAX_BATCH_FILE_BYTES) return null;
      paths.push(path);
    }
    return paths.sort((left, right) => compareBatchNames(basename(left), basename(right)));
  } finally {
    directory.closeSync();
  }
}

/**
 * Tags for a trace emitted by the harness runtime itself. Never includes
 * `evidence:synthetic`; the harness dependency mode is recorded explicitly.
 */
export function runtimeNeatlogsTags({ revision, harnessMode }) {
  if (typeof revision !== 'string' || !REVISION.test(revision)
      || typeof harnessMode !== 'string' || !MODE.test(harnessMode)) {
    throw new NeatlogsOutboxError('INVALID_TAG_INPUT');
  }
  return validateNeatlogsTags([
    `agent-harness@${revision}`,
    'evidence:runtime',
    `harness-mode:${harnessMode}`
  ]);
}

/**
 * Returns a TelemetryExporter sink that durably appends each accepted batch to
 * `directory`. It performs no network I/O. Its filesystem operations are
 * synchronous and cannot be preempted by the exporter's timeout. Records
 * already stored are accepted again without rewriting; a record that differs
 * from the stored copy is refused. A bounded inventory reserves one full
 * 2-MiB slot for each retained batch; admission never prunes existing data.
 */
export function createNeatlogsOutboxSink({ directory, maxRetainedBatches = DEFAULT_MAX_RETAINED_BATCHES }) {
  if (typeof directory !== 'string' || directory.length === 0) throw new NeatlogsOutboxError('INVALID_DIRECTORY');
  if (!Number.isSafeInteger(maxRetainedBatches) || maxRetainedBatches < 1 || maxRetainedBatches > MAX_RETAINED_BATCHES) {
    throw new NeatlogsOutboxError('INVALID_OUTBOX_LIMIT');
  }
  const root = resolve(directory);
  mkdirSync(root, { recursive: true, mode: 0o700 });
  return function neatlogsOutboxSink(records, { signal } = {}) {
    if (signal?.aborted) return { accepted: false, acceptedCount: 0 };
    const validated = validateNeatlogsSourceRecords(records);
    const paths = admissionInventory(root, maxRetainedBatches);
    if (paths === null) return { accepted: false, acceptedCount: 0 };
    // A retry after a sink timeout can carry a page that has grown since the
    // first attempt. Records already in the outbox must match exactly; only
    // the uncovered suffix is written, so no event is stored twice.
    const stored = new Map();
    const requested = new Set(validated.map(record => record.sequence));
    for (const existingPath of paths) {
      const { first, last } = batchRange(existingPath);
      if (last < validated[0].sequence || first > validated.at(-1).sequence) continue;
      for (const record of readBatch(existingPath).records) {
        if (requested.has(record.sequence)) stored.set(record.sequence, record);
      }
    }
    const fresh = [];
    for (const record of validated) {
      const existing = stored.get(record.sequence);
      if (existing === undefined) fresh.push(record);
      else if (recordsJson([existing]) !== recordsJson([record])) throw new NeatlogsOutboxError('BATCH_CONFLICT');
      else if (fresh.length > 0) throw new NeatlogsOutboxError('BATCH_CONFLICT');
    }
    if (fresh.length === 0) {
      syncDirectory(root);
      return { accepted: true, acceptedCount: validated.length };
    }
    if (paths.length === maxRetainedBatches) return { accepted: false, acceptedCount: 0 };
    const first = fresh[0].sequence;
    const last = fresh.at(-1).sequence;
    const path = join(root, `batch-${pad(first)}-${pad(last)}.json`);
    persistBatch(path, {
      schema: NEATLOGS_OUTBOX_SCHEMA,
      state: 'pending_send',
      firstSequence: first,
      lastSequence: last,
      records: fresh.map(record => ({ ...record })),
      dispatches: [],
      plan: null,
      send: null,
      readback: null
    });
    return { accepted: true, acceptedCount: validated.length };
  };
}

export function listNeatlogsOutbox(directory) {
  const root = resolve(directory);
  const names = [];
  const stream = opendirSync(root, { bufferSize: 32 });
  try {
    let entry;
    let examined = 0;
    let noncursorEntries = 0;
    while ((entry = stream.readSync()) !== null) {
      // Readers support the largest allowed admission, including old files.
      // Count ignored artifacts too, reserving one slot only for the cursor
      // so an active drain cannot make its next inventory exceed this bound.
      if (++examined > MAX_RETAINED_BATCHES + 1) {
        throw new NeatlogsOutboxError('OUTBOX_INVENTORY_LIMIT_REACHED');
      }
      if (entry.name === DRAIN_CURSOR_FILE) continue;
      if (++noncursorEntries > MAX_RETAINED_BATCHES) {
        throw new NeatlogsOutboxError('OUTBOX_INVENTORY_LIMIT_REACHED');
      }
      // Keep the existing under-bound filtering and deferred range/body
      // validation: a corrupt body must still permit cursor-driven progress.
      if (BATCH_FILE.test(entry.name)) names.push(entry.name);
    }
    return names.sort(compareBatchNames).map(name => join(root, name));
  } finally {
    stream.closeSync();
  }
}

function compareBatchNames(left, right) {
  const leftMatch = BATCH_FILE.exec(left);
  const rightMatch = BATCH_FILE.exec(right);
  const first = BigInt(leftMatch[1]) - BigInt(rightMatch[1]);
  if (first !== 0n) return first < 0n ? -1 : 1;
  const last = BigInt(leftMatch[2]) - BigInt(rightMatch[2]);
  if (last !== 0n) return last < 0n ? -1 : 1;
  // Legacy and current-width names can encode the same range. Preserve a
  // deterministic order even for those names so a saved cursor has one place.
  return left < right ? -1 : left > right ? 1 : 0;
}

function readDrainCursor(root) {
  let fd;
  try {
    fd = openSync(join(root, DRAIN_CURSOR_FILE), constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  } catch (error) {
    if (error?.code === 'ENOENT') return null;
    throw new NeatlogsOutboxError('DRAIN_CURSOR_INVALID');
  }
  try {
    const stats = fstatSync(fd);
    if (!stats.isFile() || stats.size < 1 || stats.size > MAX_DRAIN_CURSOR_BYTES) {
      throw new NeatlogsOutboxError('DRAIN_CURSOR_INVALID');
    }
    const bytes = Buffer.alloc(MAX_DRAIN_CURSOR_BYTES + 1);
    let length = 0;
    while (length < bytes.length) {
      const count = readSync(fd, bytes, length, bytes.length - length, length);
      if (count === 0) break;
      length += count;
    }
    if (length > MAX_DRAIN_CURSOR_BYTES) throw new NeatlogsOutboxError('DRAIN_CURSOR_INVALID');
    const cursor = parseNeatlogsJson(bytes.subarray(0, length).toString('utf8'));
    if (cursor === null || typeof cursor !== 'object' || Array.isArray(cursor)
        || Object.keys(cursor).length !== 2 || cursor.schema !== DRAIN_CURSOR_SCHEMA
        || typeof cursor.lastBatch !== 'string' || !BATCH_FILE.test(cursor.lastBatch)) {
      throw new NeatlogsOutboxError('DRAIN_CURSOR_INVALID');
    }
    batchRange(cursor.lastBatch);
    return cursor.lastBatch;
  } catch {
    throw new NeatlogsOutboxError('DRAIN_CURSOR_INVALID');
  } finally {
    closeSync(fd);
  }
}

/**
 * Read-only inspection of retained batches for evidence, yielding one parsed
 * batch at a time. This is separate from the bounded drain page and performs
 * no sends, readbacks or transitions. Consumers should reduce each batch
 * before advancing; retaining every yielded batch defeats streaming. The
 * finite name inventory and per-file limit are not a caller heap-size limit.
 * A concurrent writer can change files during inspection; this is not an
 * atomic snapshot or a substitute for externally serialized drain ownership.
 */
export function* iterateNeatlogsOutboxSnapshot(directory) {
  for (const path of listNeatlogsOutbox(directory)) {
    const batch = readBatch(path);
    yield Object.freeze({ path, state: batch.state, batch });
  }
}

function serializablePlan(plan) {
  return {
    workflowName: plan.workflowName,
    tags: [...plan.tags],
    traces: plan.traces.map(trace => ({
      traceId: trace.traceId,
      rootSpanId: trace.rootSpanId,
      markerSpanId: trace.markerSpanId,
      generation: trace.generation,
      sessionId: trace.sessionId,
      sequence: trace.sequence,
      eventType: trace.eventType
    }))
  };
}

/**
 * Rebuilds the readback plan from the batch's validated records. IDs and the
 * canonical input are derived again rather than trusted from the file.
 */
export function readbackPlanFromRecords(records, tags, generations = {}) {
  const validated = validateNeatlogsSourceRecords(records);
  const traces = validated.map(record => {
    const generation = generations[record.sequence] ?? 0;
    return Object.freeze({
      ...deterministicNeatlogsIds(record.eventId, generation),
      generation,
      priorTraceIds: Object.freeze(Array.from({ length: generation },
        (_, prior) => deterministicNeatlogsIds(record.eventId, prior).traceId)),
      sessionId: record.workflowId,
      eventToken: record.eventId,
      sequence: record.sequence,
      revision: record.revision,
      eventType: record.eventType,
      fence: record.fence,
      input: canonicalNeatlogsInput(record),
      record
    });
  });
  return Object.freeze({
    workflowName: NEATLOGS_LIVE_WORKFLOW_NAME,
    tags: validateNeatlogsTags(tags),
    traces: Object.freeze(traces)
  });
}

function storedGenerations(batch) {
  const generations = {};
  for (const trace of batch.plan?.traces ?? []) generations[trace.sequence] = trace.generation;
  return generations;
}

/** Returns which forbidden strings (raw IDs, credentials) occur in the outbound body. */
export function outboundLeakFindings(plan, forbidden = []) {
  const text = `${JSON.stringify(serializablePlan(plan))}\n${plan.traces.map(t => t.input).join('\n')}\n`
    + encodeNeatlogsExportRequest(plan).toString('latin1');
  const findings = [];
  forbidden.forEach((value, index) => {
    if (typeof value === 'string' && value.length > 0 && text.includes(value)) findings.push(`forbidden_${index}`);
  });
  return findings;
}

async function captureEnvelope(records, captureTimeoutMs) {
  // Imported lazily so the pinned SDK sees an environment already scrubbed of NEATLOGS_*.
  const { createNeatlogsSdkCapture } = await import('./neatlogs-sdk-capture.js');
  const capture = createNeatlogsSdkCapture({ timeoutMs: captureTimeoutMs });
  try {
    const result = await capture.prepare(records);
    if (result.status !== 'captured_local') throw new NeatlogsOutboxError(`CAPTURE_${result.reasonCode ?? 'FAILED'}`);
    return result.envelope;
  } finally {
    await capture.close();
  }
}

function assertIntentTags(intent, tags) {
  if (JSON.stringify(intent.tags) !== JSON.stringify(tags)) throw new NeatlogsOutboxError('DISPATCH_TAG_CONFLICT');
}

function hasPossibleDelivery(intent) {
  return intent.calls.some(call => (call.postsStarted > 0 && call.result === null)
    || ['receiver_accepted', 'partially_rejected', 'outcome_unknown'].includes(call.result?.status));
}

function canResumeUnpostedRecovery(batch) {
  const intent = batch.dispatches.at(-1);
  // Older v2 readback could replace pending_send using an earlier generation's
  // delivery. Restore only its observed readback states and a known-unsent
  // recovery. Keep real POSTs, completed remote refusals and deliberate holds.
  return intent?.purpose === 'recovery' && !Object.hasOwn(batch, 'deliveryHold')
    && ['pending', 'missing', 'readback_failed', 'readback_unavailable'].includes(batch.state)
    && batch.readback?.status === batch.state
    && intent.calls.every(call => call.postsStarted === 0
      && (call.result === null || ['not_configured', 'aborted'].includes(call.result.status)));
}

function retryAfterPending(batch) {
  const now = Date.now();
  if (!Number.isSafeInteger(now)) throw new NeatlogsOutboxError('INVALID_RETRY_CLOCK');
  // The validated durable call history owns this constraint, not the current
  // send summary. A later interrupted/refused call or a recovery generation
  // must not erase a receiver minimum. Start conservatively at completion;
  // use integer arithmetic even when a saturated minimum exceeds Date range.
  return batch.dispatches.some(intent => intent.calls.some(call => {
    const minimum = call.result?.retryAfterMinimumMs;
    return minimum > 0
      && BigInt(now) < BigInt(Date.parse(call.completedAt)) + BigInt(minimum);
  }));
}

function holdForLeak(batch, findings) {
  batch.state = 'blocked';
  batch.deliveryHold = { reasonCode: 'LEAK_GATE_FAILED', findings };
  // Never replace an already unresolved/accepted result with a local refusal.
  if (!batch.send) batch.send = { status: 'rejected', reasonCode: 'LEAK_GATE_FAILED', findings, attempts: 0 };
}

async function sendPersistedIntent(batch, persist, config, tags, scan, fetchImpl, sendOptions) {
  const restored = restoreOutboxIntents(batch.records, batch.dispatches).restored.at(-1);
  const { intent, plan, body, posts } = restored;
  assertIntentTags(intent, tags);
  assertIntentTarget(intent, config);
  const findings = outboundLeakFindings(plan, scan);
  if (findings.length > 0) {
    holdForLeak(batch, findings);
    persist();
    return;
  }
  if (intent.calls.length >= OUTBOX_INTENT_LIMITS.calls || posts >= OUTBOX_INTENT_LIMITS.posts) {
    batch.state = 'held';
    batch.deliveryHold = { reasonCode: 'DISPATCH_ATTEMPT_LIMIT_REACHED', outcomeResolved: false };
    persist();
    return;
  }
  const requestedAttempts = sendOptions.maxAttempts ?? 3;
  if (!Number.isSafeInteger(requestedAttempts) || requestedAttempts < 1 || requestedAttempts > 5) {
    throw new NeatlogsOutboxError('INVALID_SEND_ATTEMPTS');
  }
  if (retryAfterPending(batch)) return;
  const call = { ordinal: intent.calls.length + 1, startedAt: new Date().toISOString(),
    postsStarted: 0, completedAt: null, result: null };
  intent.calls.push(call);
  // A completed initial/recovery intent and this call record exist before the
  // sender is invoked. Each actual HTTP attempt adds a durable start marker.
  persist();
  const result = await sendNeatlogsPlan(config, plan, {
    ...sendOptions, ...(fetchImpl ? { fetch: fetchImpl } : {}), encodedBody: body,
    maxAttempts: Math.min(requestedAttempts, OUTBOX_INTENT_LIMITS.posts - posts),
    onPostStart(metadata) {
      call.postsStarted += 1;
      persist();
      sendOptions.onPostStart?.(metadata);
    }
  });
  call.result = { ...result };
  call.completedAt = new Date().toISOString();
  refreshSendSummary(batch);
  batch.state = batch.send.status === 'receiver_accepted' ? 'receiver_accepted'
    : RETRYABLE_SEND.has(batch.send.status) ? 'pending_send' : batch.send.status;
  persist();
}

/**
 * Sends pending outbox batches through the PR #38 OTLP sender and, when asked,
 * verifies possibly delivered batches through the Public API readback. Every state
 * change is written back to the batch file before the next network call.
 * A durable scheduling cursor rotates at most 32 batches per call, including
 * terminal and unresolved batches. It is not a delivery acknowledgement or
 * an ownership fence; callers must serialize drainers externally.
 */
export async function drainNeatlogsOutbox({
  directory,
  config,
  tags,
  forbidden = [],
  send = true,
  readback = false,
  recover = false,
  fetch: fetchImpl,
  sendOptions = {},
  readbackOptions = {},
  captureTimeoutMs = 500
}) {
  const validatedTags = validateNeatlogsTags(tags);
  if (validatedTags.includes('evidence:synthetic')) throw new NeatlogsOutboxError('SYNTHETIC_TAG_REFUSED');
  const secrets = [];
  try { secrets.push(config.ingestKey()); } catch { /* not configured */ }
  try { secrets.push(config.readbackToken()); } catch { /* not configured */ }
  const scan = [...forbidden, ...secrets];
  const results = [];
  const root = resolve(directory);
  const cursor = readDrainCursor(root);
  const paths = listNeatlogsOutbox(root);
  const after = cursor === null ? 0 : paths.findIndex(path => compareBatchNames(basename(path), cursor) > 0);
  const start = after < 0 ? 0 : after;
  for (let offset = 0; offset < Math.min(paths.length, MAX_BATCHES_PER_DRAIN); offset += 1) {
    const path = paths[(start + offset) % paths.length];
    // A corrupt filename must not turn a valid scheduling cursor into invalid
    // cursor data. Validate its numeric range before persisting the visit.
    batchRange(path);
    if (send || readback) {
      // Advance before reading/capturing/sending: an invalid batch, thrown
      // callback or process exit must not pin every later drain to that row.
      // The retained batch is revisited after rotation; no work is acknowledged
      // by this cursor and a failed cursor write stops before dependent work.
      writeDurably(join(root, DRAIN_CURSOR_FILE), `${JSON.stringify({
        schema: DRAIN_CURSOR_SCHEMA, lastBatch: basename(path)
      })}\n`);
    }
    const batch = readBatch(path);
    const persist = () => {
      // Summaries are projections of the current validated intent. Refresh
      // before holds/readback and each durable start, including before a
      // process can exit with a newer generation or an interrupted POST.
      refreshSendSummary(batch);
      persistBatch(path, batch);
    };
    const legacy = batch.schema === LEGACY_OUTBOX_SCHEMA;
    if (!legacy && (send || readback) && canResumeUnpostedRecovery(batch)) {
      // Keep the old observation and every call verbatim. Only repair routing;
      // the next send still validates the target and durably records its start.
      batch.state = 'pending_send';
      persist();
    }
    if (legacy && send && !TERMINAL_READBACK.has(batch.state)) {
      // v1 wrote its result only after I/O. Even plan:null/send:null can be a
      // process exit after a POST, so it cannot be migrated as known-unsent.
      batch.legacyDelivery ??= {
        originalState: batch.state, reasonCode: 'LEGACY_DISPATCH_BYTES_UNAVAILABLE',
        outcomeResolved: false, automaticResendAllowed: false
      };
      batch.state = 'legacy_held';
      persist();
    }
    if (!legacy && send && batch.state === 'pending_send') {
      if (batch.dispatches.length === 0) {
        const envelope = await captureEnvelope(batch.records, captureTimeoutMs);
        const plan = planNeatlogsExport(envelope, { tags: validatedTags });
        const findings = outboundLeakFindings(plan, scan);
        if (findings.length > 0) {
          holdForLeak(batch, findings);
          persist();
          results.push({ path, state: batch.state, batch });
          continue;
        }
        batch.dispatches.push(createOutboxIntent(plan, config, 'initial'));
        batch.plan = serializablePlan(plan);
        persist();
      }
      await sendPersistedIntent(batch, persist, config, validatedTags, scan, fetchImpl, sendOptions);
    }
    // Recovery selects and durably records a bounded next-generation subset
    // before its first POST. Any later retry reuses that exact intent, including
    // timestamps/body/tags; a failed or interrupted send does not recapture it.
    if (!legacy && send && recover && batch.state === 'missing' && batch.plan) {
      const { currentPlan } = restoreOutboxIntents(batch.records, batch.dispatches);
      const previous = batch.dispatches.at(-1);
      assertIntentTags(previous, validatedTags);
      assertIntentTarget(previous, config);
      if (batch.dispatches.length >= OUTBOX_INTENT_LIMITS.intents) {
        batch.state = 'held';
        batch.deliveryHold = { reasonCode: 'RECOVERY_GENERATION_LIMIT_REACHED', outcomeResolved: false };
        persist();
      } else if (!retryAfterPending(batch)) {
        const missing = (batch.readback?.traces ?? []).filter(trace => trace.status === 'missing').map(trace => trace.traceId);
        const wanted = currentPlan.traces.filter(trace => missing.includes(trace.traceId)).map(trace => trace.traceId);
        if (wanted.length !== missing.length || wanted.length === 0) throw new NeatlogsOutboxError('RECOVERY_PLAN_MISMATCH');
        const regenerated = nextNeatlogsGeneration(currentPlan, wanted);
        const findings = outboundLeakFindings(regenerated, scan);
        if (findings.length > 0) {
          holdForLeak(batch, findings);
          persist();
          results.push({ path, state: batch.state, batch });
          continue;
        }
        const intent = createOutboxIntent(regenerated, config, 'recovery', wanted);
        batch.dispatches.push(intent);
        batch.recoveryHistory ??= [];
        batch.recoveryHistory.push({ readback: batch.readback, previousPlan: batch.plan });
        batch.plan = serializablePlan(restoreOutboxIntents(batch.records, batch.dispatches).currentPlan);
        batch.recovery = {
          regeneratedFrom: wanted, regeneratedTo: regenerated.traces.map(trace => trace.traceId),
          basis: 'NOT_OBSERVED_WITHIN_BOUNDED_POLL_WINDOW',
          duplicateDeliveryRisk: 'PRIOR_GENERATION_MAY_APPEAR_LATER', send: null
        };
        batch.readback = null;
        batch.state = 'pending_send';
        persist();
        await sendPersistedIntent(batch, persist, config, validatedTags, scan, fetchImpl, sendOptions);
      }
    }
    const mayReadback = legacy
      ? batch.plan && ['receiver_accepted', 'outcome_unknown', 'partially_rejected'].includes(batch.send?.status)
      : batch.dispatches.length > 0 && hasPossibleDelivery(batch.dispatches.at(-1));
    if (readback && mayReadback && batch.state !== 'blocked' && !TERMINAL_READBACK.has(batch.state)) {
      if (!legacy) assertIntentTarget(batch.dispatches.at(-1), config);
      const plan = readbackPlanFromRecords(batch.records, batch.plan?.tags ?? validatedTags, storedGenerations(batch));
      if (batch.plan && JSON.stringify(serializablePlan(plan).traces.map(t => t.traceId))
          !== JSON.stringify(batch.plan.traces.map(t => t.traceId))) {
        throw new NeatlogsOutboxError('PLAN_IDENTITY_MISMATCH');
      }
      const result = await readbackNeatlogsPlan(config, plan, {
        ...readbackOptions, ...(fetchImpl ? { fetch: fetchImpl } : {})
      });
      // The project UUID is configuration, not evidence; keep it out of the outbox.
      const { project: _project, ...withoutProject } = result;
      batch.readback = { ...withoutProject, readAt: new Date().toISOString() };
      batch.state = result.status;
      persist();
    }
    results.push({ path, state: batch.state, batch });
  }
  return Object.freeze(results);
}

/**
 * Explicit opt-in. Returns `{ enabled: false }` unless `enabled === true`, so
 * nothing is read or exported by default. When enabled, configuration comes
 * only from `loadNeatlogsConfig` (explicit env object plus optional named env
 * file), the NEATLOGS_* names are scrubbed from `scrubEnv`, and an ingest key
 * is required.
 */
export function loadNeatlogsTelemetryOptIn({ enabled = false, env = {}, envFile = null, scrubEnv = null } = {}) {
  if (enabled !== true) return Object.freeze({ enabled: false });
  const config = loadNeatlogsConfig({ env, envFile });
  if (scrubEnv) scrubNeatlogsEnvironment(scrubEnv);
  if (!config.summary.ingestReady) throw new NeatlogsOutboxError('INGEST_NOT_CONFIGURED');
  return Object.freeze({ enabled: true, config });
}

/**
 * Builds the harness's existing durable TelemetryExporter over its own store
 * connection, with the Neatlogs outbox as its sink, ready to pass to
 * `createOperationsWorker({ telemetry })`. Tokens use a per-process random
 * HMAC key: they are stable within one run (session = workflow token) and are
 * not linkable across runs. The caller closes the returned store after the
 * worker has stopped.
 */
export function createNeatlogsOperationsTelemetry({ databasePath, outboxDirectory, busyTimeoutMs = 20, limits, maxRetainedBatches }) {
  const sink = createNeatlogsOutboxSink({ directory: outboxDirectory, maxRetainedBatches });
  const store = DurableWorkflowStore.open({ databasePath, busyTimeoutMs });
  const tokenizer = createHmacTokenizer(randomBytes(32));
  let exporter;
  try {
    exporter = createTelemetryExporter({ store, tokenizer, sink, ...(limits ? { limits } : {}) });
  } catch (error) {
    tokenizer.destroy();
    store.close();
    throw error;
  }
  return Object.freeze({
    exporter,
    close() {
      tokenizer.destroy();
      store.close();
    }
  });
}
