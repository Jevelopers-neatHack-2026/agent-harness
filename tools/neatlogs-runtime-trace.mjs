#!/usr/bin/env node
// Runs one genuine harness workflow through the operations worker with the
// durable TelemetryExporter wired to the Neatlogs outbox, then (optionally)
// sends the outbox through the OTLP sender and verifies it via the Public API.
// See docs/runbooks/neatlogs.md. Never prints or writes credential values or
// the Neatlogs project UUID.
import {
  closeSync, constants, fstatSync, lstatSync, mkdtempSync, opendirSync, openSync,
  readSync, realpathSync, renameSync, rmSync, writeFileSync
} from 'node:fs';
import { randomUUID } from 'node:crypto';
import { tmpdir } from 'node:os';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import { DurableWorkflowStore } from '../src/internal/durable-store.js';
import { createOperationsWorker } from '../src/worker/private/worker.js';
import {
  createNeatlogsOperationsTelemetry,
  drainNeatlogsOutbox,
  loadNeatlogsTelemetryOptIn,
  runtimeNeatlogsTags,
  iterateNeatlogsOutboxSnapshot
} from '../src/observability/private/neatlogs-outbox.js';
import { parseNeatlogsJson } from '../src/observability/private/neatlogs-otlp.js';
import { NEATLOGS_SDK_MAPPING_LIMITS } from '../src/observability/private/neatlogs-sdk-mapper.js';

const EVIDENCE_SCHEMA = 'agent-harness.neatlogs.runtime-trace.v1';
const REVISION = /^[0-9a-f]{40}$/;
const HARNESS_MODE = 'mock';
// Local CLI inspection policy, matching file-admission's configurable range.
// These are artifact/scan bounds, not a filesystem latency or ownership fence.
const DEFAULT_MAX_RETAINED_BATCHES = 128;
const MAX_RETAINED_BATCHES = 1024;
const MAX_BATCH_FILE_BYTES = 2 * 1024 * 1024;
const MAX_CURSOR_BYTES = 256;
const MAX_BATCH_SUMMARY_BYTES = 128 * 1024;
const MAX_EVIDENCE_BYTES = 8 * 1024 * 1024;
const MAX_STDOUT_BYTES = 64 * 1024;
const BATCH_FILE = /^batch-(\d{12,16})-(\d{12,16})\.json$/;
const CURSOR_FILE = 'drain-cursor.json';

class InspectionError extends Error {
  constructor(code) {
    super(`Bounded outbox inspection refused (${code}).`);
    this.code = code;
  }
}

// Serialize with a budget before joining chunks. Checking an already-created
// whole JSON string is too late for retained legacy fields or escaped strings.
function boundedJson(value, limit, code) {
  const chunks = [];
  let bytes = 0;
  const append = text => {
    bytes += Buffer.byteLength(text, 'utf8');
    if (bytes > limit) throw new InspectionError(code);
    chunks.push(text);
  };
  const string = text => {
    if (Buffer.byteLength(text, 'utf8') > limit - bytes) throw new InspectionError(code);
    append(JSON.stringify(text));
  };
  const visit = (item, depth) => {
    if (depth > 64) throw new InspectionError(code);
    if (typeof item === 'string') { string(item); return; }
    if (item === null || typeof item === 'boolean' || typeof item === 'number') {
      append(JSON.stringify(item)); return;
    }
    if (Array.isArray(item)) {
      append('[');
      for (let index = 0; index < item.length; index += 1) {
        if (index > 0) append(',');
        visit(item[index] === undefined ? null : item[index], depth + 1);
      }
      append(']'); return;
    }
    if (item === null || typeof item !== 'object') throw new InspectionError(code);
    append('{');
    let first = true;
    for (const key in item) {
      if (!Object.hasOwn(item, key) || item[key] === undefined) continue;
      if (!first) append(',');
      first = false;
      string(key); append(':'); visit(item[key], depth + 1);
    }
    append('}');
  };
  visit(value, 0);
  return { text: chunks.join(''), bytes };
}

function validBatchName(name) {
  const match = BATCH_FILE.exec(name);
  return match !== null && BigInt(match[1]) >= 1n && BigInt(match[2]) >= BigInt(match[1])
    && BigInt(match[2]) <= BigInt(Number.MAX_SAFE_INTEGER);
}

function validateCursor(path) {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const stats = fstatSync(fd);
    if (!stats.isFile() || stats.size < 1 || stats.size > MAX_CURSOR_BYTES) {
      throw new InspectionError('OUTBOX_FILE_LIMIT');
    }
    const bytes = Buffer.alloc(MAX_CURSOR_BYTES + 1);
    let length = 0;
    while (length < bytes.length) {
      const count = readSync(fd, bytes, length, bytes.length - length, length);
      if (count === 0) break;
      length += count;
    }
    if (length > MAX_CURSOR_BYTES) throw new InspectionError('OUTBOX_FILE_LIMIT');
    const cursor = parseNeatlogsJson(bytes.subarray(0, length).toString('utf8'));
    if (!cursor || typeof cursor !== 'object' || Array.isArray(cursor)
        || Object.keys(cursor).length !== 2
        || cursor.schema !== 'agent-harness.neatlogs.outbox-drain-cursor.v1'
        || typeof cursor.lastBatch !== 'string' || !validBatchName(cursor.lastBatch)) {
      throw new InspectionError('OUTBOX_INVENTORY_INVALID');
    }
  } finally {
    closeSync(fd);
  }
}

function boundedInventory(directory, maxRetainedBatches, allowMissing) {
  try {
    if (!lstatSync(directory).isDirectory()) throw new InspectionError('OUTBOX_INVENTORY_INVALID');
  } catch (error) {
    if (error?.code === 'ENOENT' && allowMissing) return null;
    throw error;
  }
  const names = new Set();
  let cursorPresent = false;
  const handle = opendirSync(directory, { bufferSize: 32 });
  try {
    let entry;
    let examined = 0;
    while ((entry = handle.readSync()) !== null) {
      if (++examined > maxRetainedBatches + 1) throw new InspectionError('OUTBOX_INVENTORY_LIMIT_REACHED');
      if (!entry.isFile()) throw new InspectionError('OUTBOX_INVENTORY_INVALID');
      const path = join(directory, entry.name);
      const cursor = entry.name === CURSOR_FILE;
      if (!cursor && !validBatchName(entry.name)) throw new InspectionError('OUTBOX_INVENTORY_INVALID');
      if (!cursor && names.size === maxRetainedBatches) throw new InspectionError('OUTBOX_INVENTORY_LIMIT_REACHED');
      const stats = lstatSync(path);
      if (!stats.isFile()) throw new InspectionError('OUTBOX_INVENTORY_INVALID');
      if (stats.size < 1 || stats.size > (cursor ? MAX_CURSOR_BYTES : MAX_BATCH_FILE_BYTES)) {
        throw new InspectionError('OUTBOX_FILE_LIMIT');
      }
      if (cursor) { validateCursor(path); cursorPresent = true; }
      else names.add(entry.name);
    }
    return { names, cursorPresent };
  } finally {
    handle.closeSync();
  }
}

function inspectOutbox(directory, maxRetainedBatches, allowMissing = false) {
  try {
    // No full directory listing, batch parsing or drain may precede this scan.
    const inventory = boundedInventory(directory, maxRetainedBatches, allowMissing);
    if (inventory === null) return { batches: [], paths: [], count: 0, fresh: true };
    const { names, cursorPresent } = inventory;
    const count = names.size;
    const batches = [];
    const paths = [];
    let summaryBytes = 2;
    // The existing reader validates immutable intent and legacy compatibility.
    // Its complete name list is now count-bounded under the required external
    // writer/drainer serialization. Parsed batch bodies are reduced one at a time.
    for (const result of iterateNeatlogsOutboxSnapshot(directory)) {
      if (!names.delete(basename(result.path))) throw new InspectionError('OUTBOX_INVENTORY_CHANGED');
      const summary = sanitizeBatch(result.batch);
      summaryBytes += boundedJson(summary, MAX_BATCH_SUMMARY_BYTES, 'EVIDENCE_SUMMARY_LIMIT').bytes + 1;
      // Reserve bounded metadata and the at-most-32 drain page outside summaries.
      if (summaryBytes > MAX_EVIDENCE_BYTES - MAX_STDOUT_BYTES) throw new InspectionError('EVIDENCE_TOTAL_LIMIT');
      batches.push(summary);
      paths.push(basename(result.path));
    }
    if (names.size !== 0) throw new InspectionError('OUTBOX_INVENTORY_CHANGED');
    return { batches, paths, count, fresh: count === 0 && !cursorPresent };
  } catch (error) {
    if (error instanceof InspectionError) throw error;
    if (error?.code === 'OUTBOX_INVENTORY_LIMIT_REACHED') throw new InspectionError(error.code);
    throw new InspectionError('OUTBOX_BATCH_INVALID');
  }
}

function within(directory, path) {
  const suffix = relative(directory, path);
  return suffix === '' || (suffix !== '..' && !suffix.startsWith(`..${sep}`) && !isAbsolute(suffix));
}

function physicalDirectoryPath(path) {
  let ancestor = path;
  const suffix = [];
  while (true) {
    try {
      return join(realpathSync(ancestor), ...suffix);
    } catch (error) {
      if (error?.code !== 'ENOENT') throw error;
      // Resolve the existing ancestor even when the worker has not created the
      // outbox yet. ENOENT can also mean an existing dangling symlink: that
      // component must not be treated as a harmless missing directory suffix.
      let missing = false;
      try { lstatSync(ancestor); } catch (statError) {
        if (statError?.code !== 'ENOENT') throw statError;
        missing = true;
      }
      if (!missing) usage('--outbox and --evidence directory aliases must resolve before work.');
      const parent = dirname(ancestor);
      if (parent === ancestor) throw error;
      suffix.unshift(basename(ancestor));
      ancestor = parent;
    }
  }
}

function requireExternalEvidencePath(outbox, path) {
  if (within(outbox, path)) usage('--evidence must be outside the outbox directory.');
  if (within(physicalDirectoryPath(outbox), join(physicalDirectoryPath(dirname(path)), basename(path)))) {
    usage('--evidence must be outside the outbox directory.');
  }
}
// A bounded local plan using only the registry's local-pure operations. The
// registry marks every provider/network/shell operation unavailable, so this
// run cannot make a model or provider call.
export const RUNTIME_PLAN_JSON = JSON.stringify({
  kind: 'agent-harness.private-synthetic-plan',
  version: 1,
  steps: [
    { id: 'sum', operation: 'number.add', dependsOn: [], args: { left: 20, right: 22 } },
    { id: 'label', operation: 'text.concat', dependsOn: [], args: { parts: ['agent-harness ', 'runtime trace'] } }
  ]
});

export function isCompleteDeliveryEvidence({ network, batches, batchCount,
  inspectionComplete = true, executionComplete = true }) {
  return network === true
    && inspectionComplete === true
    && executionComplete === true
    && Array.isArray(batches)
    && Number.isSafeInteger(batchCount)
    && batches.length > 0
    && batches.length === batchCount
    && batches.every(batch => batch?.readback?.status === 'verified');
}

function completeRuntimeExport(run) {
  const telemetry = run?.telemetry;
  return run?.jobState === 'completed'
    && telemetry?.pendingRecords === 0
    && telemetry.pendingCapped === false
    && telemetry.lastErrorCode === null
    && Number.isSafeInteger(telemetry.deliveredRecords) && telemetry.deliveredRecords > 0
    && Number.isSafeInteger(telemetry.policyDroppedRecords) && telemetry.policyDroppedRecords >= 0
    && Number.isSafeInteger(telemetry.cursor)
    && telemetry.cursor === telemetry.deliveredRecords + telemetry.policyDroppedRecords;
}

function usage(message) {
  if (message) process.stderr.write(`${message}\n`);
  process.stderr.write([
    'Usage: node tools/neatlogs-runtime-trace.mjs --mode <run|send-and-readback|drain>',
    '         --outbox <dir> --evidence <path> --component-revision <sha>',
    '         [--env-file <path>] [--max-polls <n>] [--poll-interval-ms <ms>] [--recover]',
    '         [--max-retained-batches <1..1024> (default: 128)]',
    ''
  ].join('\n'));
  process.exit(2);
}

function writeJsonAtomically(path, serialized) {
  const temporary = `${path}.${process.pid}.${randomUUID()}.tmp`;
  writeFileSync(temporary, `${serialized}\n`, { mode: 0o600, flag: 'wx' });
  renameSync(temporary, path);
}

async function waitForTerminal(worker, scope, jobId, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const row = worker.readScopedWorkPage({ trustedSyntheticScope: scope, limit: 10 })
      .items.find(item => item.jobId === jobId);
    if (row && ['completed', 'failed', 'cancelled', 'unknown', 'parked'].includes(row.state)) return row;
    await new Promise(resolvePromise => setTimeout(resolvePromise, 10));
  }
  throw new Error('The runtime workflow did not reach a terminal durable state.');
}

/**
 * Executes one workflow through the normal in-process worker entry point:
 * createOperationsWorker -> submitSyntheticWork (the worker's only intake;
 * its scope strings carry no authority) -> owned runner child process ->
 * runBoundedLocalPlan. The worker's existing `telemetry` injection point
 * receives the durable TelemetryExporter, whose sink is the Neatlogs outbox.
 */
export async function runRuntimeWorkflow({ outboxDirectory, databaseDirectory,
  maxRetainedBatches = DEFAULT_MAX_RETAINED_BATCHES }) {
  const databasePath = join(databaseDirectory, 'workflow.sqlite');
  const runId = randomUUID();
  const scope = Object.freeze({ tenantId: `runtime-tenant-${runId}`, projectId: `runtime-project-${runId}` });
  const ownerId = `ops-worker:runtime-${runId}`;
  const idempotencyKey = `runtime-trace-${runId}`;
  const telemetry = createNeatlogsOperationsTelemetry({ databasePath, outboxDirectory, maxRetainedBatches });
  let runtimeWorker;
  try {
    runtimeWorker = createOperationsWorker({ databasePath, ownerId, telemetry: telemetry.exporter });
  } catch (error) {
    telemetry.close();
    throw error;
  }
  let removeHandlers;
  try {
    removeHandlers = runtimeWorker.installSignalHandlers();
    const loop = runtimeWorker.start();
    const submitted = runtimeWorker.submitSyntheticWork({ trustedSyntheticScope: scope, idempotencyKey, planJson: RUNTIME_PLAN_JSON });
    if (!submitted.accepted) throw new Error('The runtime workflow was not admitted.');
    const terminal = await waitForTerminal(runtimeWorker, scope, submitted.jobId);
    const stop = await runtimeWorker.stop('requested');
    await loop;
    if (!stop.stopped || !stop.telemetryClosed) throw new Error('The worker did not stop and flush telemetry cleanly.');
    const diagnostics = telemetry.exporter.diagnostics();
    const store = DurableWorkflowStore.open({ databasePath, busyTimeoutMs: 20 });
    let rawEventIds;
    let output;
    try {
      rawEventIds = store.readEventPage({ afterSequence: 0, limit: 64 }).events.map(event => event.eventId);
      output = JSON.parse(store.getCheckpoint(terminal.workflowId).data.completedResultsJson);
    } finally {
      store.close();
    }
    return Object.freeze({
      jobState: terminal.state,
      results: Object.freeze({ sum: output.sum?.output ?? null, label: output.label?.output ?? null }),
      telemetry: Object.freeze({
        cursor: diagnostics.cursor,
        pendingRecords: diagnostics.pendingRecords,
        pendingCapped: diagnostics.pendingCapped,
        deliveredRecords: diagnostics.deliveredRecords,
        policyDroppedRecords: diagnostics.policyDroppedRecords,
        lastErrorCode: diagnostics.lastErrorCode
      }),
      // Raw identifiers of this run. They must never appear in the export body or evidence.
      forbidden: Object.freeze([
        submitted.jobId, terminal.workflowId, scope.tenantId, scope.projectId, ownerId, idempotencyKey, runId,
        ...rawEventIds
      ])
    });
  } finally {
    removeHandlers?.();
    if (runtimeWorker.lifecycle !== 'stopped') await runtimeWorker.stop('requested');
    telemetry.close();
  }
}

function sanitizeBatch(batch) {
  const traces = batch.plan?.traces ?? [];
  const readbackTraces = batch.readback?.traces ?? [];
  if (!Array.isArray(traces) || traces.length > NEATLOGS_SDK_MAPPING_LIMITS.sourceRecords
      || !Array.isArray(readbackTraces) || readbackTraces.length > NEATLOGS_SDK_MAPPING_LIMITS.sourceRecords) {
    throw new InspectionError('EVIDENCE_SUMMARY_LIMIT');
  }
  return {
    state: batch.state,
    firstSequence: batch.firstSequence,
    lastSequence: batch.lastSequence,
    records: batch.records.length,
    workflowName: batch.plan?.workflowName ?? null,
    tags: batch.plan?.tags ?? null,
    send: batch.send && {
      status: batch.send.status,
      reasonCode: batch.send.reasonCode ?? null,
      httpStatus: batch.send.httpStatus ?? null,
      attempts: batch.send.attempts ?? null,
      expectedSpans: batch.send.expectedSpans ?? null,
      rejectedSpans: batch.send.rejectedSpans ?? null
    },
    recovery: batch.recovery ? {
      regeneratedFrom: batch.recovery.regeneratedFrom,
      regeneratedTo: batch.recovery.regeneratedTo,
      basis: batch.recovery.basis ?? null,
      duplicateDeliveryRisk: batch.recovery.duplicateDeliveryRisk ?? null,
      send: batch.recovery.send && {
        status: batch.recovery.send.status,
        httpStatus: batch.recovery.send.httpStatus ?? null,
        attempts: batch.recovery.send.attempts ?? null,
        expectedSpans: batch.recovery.send.expectedSpans ?? null,
        rejectedSpans: batch.recovery.send.rejectedSpans ?? null
      }
    } : null,
    readback: batch.readback && {
      status: batch.readback.status,
      reasonCode: batch.readback.reasonCode ?? null,
      duplicateEventDeliveries: batch.readback.duplicateEventDeliveries ?? null
    },
    traces: traces.map(trace => {
      const read = readbackTraces.find(item => item.traceId === trace.traceId) ?? null;
      return {
        traceId: trace.traceId,
        rootSpanId: trace.rootSpanId,
        generation: trace.generation,
        sequence: trace.sequence,
        eventType: trace.eventType,
        sessionId: trace.sessionId,
        readback: read && {
          status: read.status,
          finalizationStatus: read.finalizationStatus ?? null,
          spansCount: read.spansCount ?? null,
          markerStored: read.markerStored ?? null,
          duplicateTraceIds: read.duplicateTraceIds ?? [],
          problems: read.problems
        }
      };
    })
  };
}

function stdoutSummary(evidence) {
  return {
    mode: evidence.mode,
    evidenceClass: evidence.evidenceClass,
    jobState: evidence.run?.jobState ?? null,
    batches: evidence.batches.length,
    traces: evidence.batches.reduce((count, batch) => count + (batch.traces?.length ?? 0), 0),
    send: evidence.batches.map(batch => batch.send?.status ?? null),
    readback: evidence.batches.map(batch => batch.readback?.status ?? null),
    deliveryConfirmed: evidence.deliveryConfirmed,
    execution: evidence.execution,
    inspectionComplete: evidence.inspection.complete,
    reasonCode: evidence.inspection.reasonCode
  };
}

function serializeOutputs(evidence) {
  // Include each terminating newline in the published artifact limits.
  return {
    evidence: boundedJson(evidence, MAX_EVIDENCE_BYTES - 1, 'EVIDENCE_TOTAL_LIMIT').text,
    stdout: boundedJson(stdoutSummary(evidence), MAX_STDOUT_BYTES - 1, 'EVIDENCE_STDOUT_LIMIT').text
  };
}

async function main() {
  const { values } = parseArgs({
    options: {
      mode: { type: 'string' },
      outbox: { type: 'string' },
      evidence: { type: 'string' },
      'env-file': { type: 'string' },
      'component-revision': { type: 'string' },
      'max-polls': { type: 'string' },
      'poll-interval-ms': { type: 'string' },
      'max-retained-batches': { type: 'string' },
      recover: { type: 'boolean' }
    },
    strict: true
  });
  const mode = values.mode;
  if (!['run', 'send-and-readback', 'drain'].includes(mode)) usage('Unknown --mode.');
  if (!values.outbox || !values.evidence) usage('--outbox and --evidence are required.');
  const revision = values['component-revision'];
  if (!revision || !REVISION.test(revision)) usage('--component-revision must be the full 40-character commit SHA.');
  const limit = values['max-retained-batches'] ?? String(DEFAULT_MAX_RETAINED_BATCHES);
  const maxRetainedBatches = Number(limit);
  if (!/^[1-9]\d{0,3}$/.test(limit) || !Number.isSafeInteger(maxRetainedBatches)
      || maxRetainedBatches > MAX_RETAINED_BATCHES) {
    usage('--max-retained-batches must be an integer from 1 through 1024.');
  }
  const outbox = resolve(values.outbox);
  const evidencePath = resolve(values.evidence);
  requireExternalEvidencePath(outbox, evidencePath);
  const network = mode !== 'run';
  // Opt-in: configuration is read only for network modes, only through the strict loader.
  const optIn = loadNeatlogsTelemetryOptIn({
    enabled: network,
    env: { ...process.env },
    envFile: values['env-file'] ? resolve(values['env-file']) : null,
    scrubEnv: process.env
  });
  const tags = runtimeNeatlogsTags({ revision, harnessMode: HARNESS_MODE });
  const evidence = {
    schema: EVIDENCE_SCHEMA,
    generatedAt: new Date().toISOString(),
    mode,
    evidenceClass: network ? 'live-runtime-application' : 'offline-runtime-application',
    syntheticBuilder: false,
    componentRevisions: { agentHarness: revision, neatlogsSdk: '1.1.28', node: process.versions.node },
    workflow: {
      entryPoint: 'createOperationsWorker().submitSyntheticWork() -> src/worker/private/runner.js -> runBoundedLocalPlan',
      harnessMode: HARNESS_MODE,
      modelMode: 'none (mock dependency mode; local-pure registry operations number.add and text.concat; provider, network and shell operations are unavailable in the registry)',
      providerCalls: 0,
      plan: JSON.parse(RUNTIME_PLAN_JSON)
    },
    config: optIn.enabled ? {
      region: optIn.config.summary.region,
      ingestOrigin: optIn.config.summary.ingestOrigin,
      appOrigin: optIn.config.summary.appOrigin,
      readbackReady: optIn.config.summary.readbackReady
    } : null,
    tags,
    run: null,
    execution: {
      status: mode === 'drain' ? 'not_requested' : 'not_started', reasonCode: null,
      telemetryComplete: mode === 'drain' ? null : false,
      sourceState: { disposition: 'not_created', directory: null,
        purpose: 'private_diagnostics_only', automaticReplaySupported: false }
    },
    batches: [],
    inspection: {
      complete: false, phase: 'preflight', reasonCode: null, retainedBatchCount: null,
      maxRetainedBatches, maxBatchFileBytes: MAX_BATCH_FILE_BYTES,
      maxBatchSummaryBytes: MAX_BATCH_SUMMARY_BYTES, maxEvidenceBytes: MAX_EVIDENCE_BYTES,
      maxStdoutBytes: MAX_STDOUT_BYTES
    },
    deliveryConfirmed: false
  };
  let forbidden = [];
  let phase = 'preflight';
  let serialized;
  const useSnapshot = snapshot => {
    evidence.batches = network ? snapshot.batches : snapshot.paths.map(path => ({ path }));
    evidence.inspection = { ...evidence.inspection, complete: true, phase,
      reasonCode: null, retainedBatchCount: snapshot.count };
    if (network) evidence.batchEvidenceSource = 'persisted_outbox_snapshot';
  };
  try {
    const initial = inspectOutbox(outbox, maxRetainedBatches, mode !== 'drain');
    useSnapshot(initial);
    serializeOutputs(evidence); // All retained output budgets pass before any side effect.
    if (mode !== 'drain') {
      // This example creates a fresh SQLite sequence namespace and HMAC key.
      // Retained batches (including cursor-only history) belong to a different
      // source: do not collide with them or borrow their verified readback.
      if (!initial.fresh) {
        evidence.execution.status = 'refused';
        evidence.execution.reasonCode = 'OUTBOX_REQUIRES_FRESH_WORKFLOW';
      } else {
        // Keep private raw source state outside the inspected/exported outbox.
        if (within(physicalDirectoryPath(outbox), physicalDirectoryPath(tmpdir()))) {
          throw new InspectionError('RUNTIME_STATE_DIRECTORY_INVALID');
        }
        let databaseDirectory;
        try {
          databaseDirectory = mkdtempSync(join(tmpdir(), 'neatlogs-runtime-'));
          evidence.execution.sourceState = { ...evidence.execution.sourceState,
            disposition: 'retained', directory: databaseDirectory };
          const run = await runRuntimeWorkflow({ outboxDirectory: outbox, databaseDirectory, maxRetainedBatches });
          forbidden = run.forbidden;
          evidence.run = { jobState: run.jobState, results: run.results, telemetry: run.telemetry,
            rawIdentifiersScanned: forbidden.length };
          evidence.execution.telemetryComplete = completeRuntimeExport(run);
          evidence.execution.status = evidence.execution.telemetryComplete ? 'completed' : 'incomplete';
          evidence.execution.reasonCode = evidence.execution.telemetryComplete ? null : 'RUNTIME_EXPORT_INCOMPLETE';
        } catch {
          // A thrown start/stop/flush/read error leaves export completion
          // unproven. Preserve any source directory already created, without
          // serializing error messages, database rows or raw run identities.
          evidence.execution.status = 'failed';
          evidence.execution.reasonCode = 'RUNTIME_WORKFLOW_FAILED';
          evidence.execution.telemetryComplete = false;
        }
        if (evidence.execution.telemetryComplete) {
          try {
            rmSync(databaseDirectory, { recursive: true, force: true });
            evidence.execution.sourceState = { ...evidence.execution.sourceState,
              disposition: 'removed_after_complete_export', directory: null };
          } catch {
            evidence.execution.status = 'failed';
            evidence.execution.reasonCode = 'RUNTIME_STATE_CLEANUP_FAILED';
            evidence.execution.sourceState.disposition = 'cleanup_failed';
          }
        }
        phase = 'after_run';
        useSnapshot(inspectOutbox(outbox, maxRetainedBatches, true));
        serializeOutputs(evidence);
      }
    }
    const executionComplete = mode === 'drain' || evidence.execution.status === 'completed';
    if (network && executionComplete) {
      const options = {};
      if (values['max-polls']) options.maxPolls = Number(values['max-polls']);
      if (values['poll-interval-ms']) options.pollIntervalMs = Number(values['poll-interval-ms']);
      phase = 'drain';
      const results = await drainNeatlogsOutbox({
        directory: outbox,
        config: optIn.config,
        tags,
        forbidden,
        send: true,
        readback: optIn.config.summary.readbackReady,
        recover: values.recover === true,
        readbackOptions: options
      });
      evidence.drainPage = results.map(result => ({ path: basename(result.path), state: result.state }));
      phase = 'after_drain';
      useSnapshot(inspectOutbox(outbox, maxRetainedBatches));
    }
    evidence.deliveryConfirmed = isCompleteDeliveryEvidence({
      network,
      batches: evidence.batches,
      batchCount: evidence.inspection.retainedBatchCount,
      inspectionComplete: evidence.inspection.complete,
      executionComplete
    });
    serialized = serializeOutputs(evidence);
  } catch (error) {
    if (!(error instanceof InspectionError) && error?.code !== 'OUTBOX_INVENTORY_LIMIT_REACHED') throw error;
    evidence.batches = [];
    evidence.batchEvidenceSource = 'incomplete_outbox_inspection';
    evidence.inspection = { ...evidence.inspection, complete: false, phase,
      reasonCode: error.code, retainedBatchCount: null };
    evidence.deliveryConfirmed = false;
    serialized = serializeOutputs(evidence);
  }
  const secrets = [];
  if (optIn.enabled) {
    try { secrets.push(optIn.config.ingestKey()); } catch { /* not configured */ }
    try { secrets.push(optIn.config.readbackToken()); } catch { /* not configured */ }
    if (optIn.config.summary.projectId) secrets.push(optIn.config.summary.projectId);
  }
  const leaks = [...forbidden, ...secrets].filter(value => typeof value === 'string' && value
    && (serialized.evidence.includes(value) || serialized.stdout.includes(value)));
  if (leaks.length > 0) {
    process.stderr.write(`Evidence leak scan failed (${leaks.length} value(s)); evidence not written.\n`);
    process.exit(1);
  }
  requireExternalEvidencePath(outbox, evidencePath);
  writeJsonAtomically(evidencePath, serialized.evidence);
  // Even bounded JSON can exceed a pipe buffer. Await flushing instead of
  // terminating while stdout still contains an incomplete JSON document.
  await new Promise((resolveWrite, rejectWrite) => {
    process.stdout.write(`${serialized.stdout}\n`, error => error ? rejectWrite(error) : resolveWrite());
  });
  const succeeded = evidence.inspection.complete
    && (mode === 'run' ? evidence.execution.status === 'completed' : evidence.deliveryConfirmed);
  process.exitCode = succeeded ? 0 : 1;
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main().catch(error => {
    process.stderr.write(`Neatlogs runtime trace failed (${typeof error?.code === 'string' ? error.code : 'ERROR'}).\n`);
    process.exitCode = 1;
  });
}
