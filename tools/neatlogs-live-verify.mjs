#!/usr/bin/env node
// Neatlogs live ingestion/readback verifier for one synthetic harness workflow.
// See docs/runbooks/neatlogs.md. Never prints or writes credential values.
import { randomBytes, randomUUID } from 'node:crypto';
import { mkdtempSync, readFileSync, renameSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import {
  DurableWorkflowStore,
  createHmacTokenizer,
  projectTelemetryEvent
} from '@jevelopers/agent-harness-runtime';
import { loadNeatlogsConfig, scrubNeatlogsEnvironment } from '../src/observability/private/neatlogs-config.js';
import {
  canonicalNeatlogsInput,
  deterministicNeatlogsIds,
  encodeNeatlogsExportRequest,
  NEATLOGS_LIVE_WORKFLOW_NAME,
  NEATLOGS_OTLP_LIMITS,
  nextNeatlogsGeneration,
  parseNeatlogsJson,
  planNeatlogsExport,
  validateNeatlogsTags
} from '../src/observability/private/neatlogs-otlp.js';
import {
  NEATLOGS_LIVE_LIMITS,
  readbackNeatlogsPlan,
  sendNeatlogsPlan
} from '../src/observability/private/neatlogs-live.js';
import { validateNeatlogsSourceRecords } from '../src/observability/private/neatlogs-sdk-mapper.js';

const EVIDENCE_SCHEMA = 'agent-harness.neatlogs.live-verification.v1';
const PLAN_SCHEMA = 'agent-harness.neatlogs.export-plan.v1';
const REVISION = /^[0-9a-f]{40}$/;
const MAX_PLAN_BYTES = 256 * 1024;
// Synthetic canaries are written into the durable workflow's raw fields. They
// must never appear in the export body, the plan, or the evidence.
export const SYNTHETIC_CANARIES = Object.freeze([
  'SYNTHETIC_RAW_ID_CANARY_7d1',
  'SYNTHETIC_PRIVATE_URL_CANARY_https://private.invalid/order?token=suffix',
  'SYNTHETIC_EMAIL_CANARY_person@example.invalid',
  'SYNTHETIC_CUSTOMER_TEXT_CANARY_4be',
  'SYNTHETIC_TOKEN_CANARY_sk-synthetic-not-a-key',
  'SYNTHETIC_ERROR_TEXT_CANARY_e02'
]);

function usage(message) {
  if (message) process.stderr.write(`${message}\n`);
  process.stderr.write([
    'Usage: node tools/neatlogs-live-verify.mjs --mode <offline|send|readback|send-and-readback>',
    '         --evidence <path> [--env-file <path>] [--plan <path>] [--component-revision <sha>]',
    '         [--max-polls <n>] [--poll-interval-ms <ms>] [--recover]',
    ''
  ].join('\n'));
  process.exit(2);
}

function writeJsonAtomically(path, value) {
  const temporary = `${path}.${process.pid}.${randomUUID()}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600, flag: 'wx' });
  renameSync(temporary, path);
}

function buildSyntheticRecords() {
  const scratch = mkdtempSync(join(tmpdir(), 'neatlogs-live-synthetic-'));
  let generated = 0;
  const store = DurableWorkflowStore.open({
    databasePath: join(scratch, 'synthetic.sqlite'),
    clock: () => Date.now(),
    idFactory: () => `synthetic-generated-${randomUUID()}-${++generated}`
  });
  const tokenizer = createHmacTokenizer(randomBytes(32));
  try {
    const run = randomUUID();
    const workflowId = `workflow:${SYNTHETIC_CANARIES[0]}:${run}`;
    store.createWorkflow({
      workflowId,
      eventId: `event:created:${run}`,
      checkpoint: {
        customer: SYNTHETIC_CANARIES[3],
        privateUrl: SYNTHETIC_CANARIES[1],
        email: SYNTHETIC_CANARIES[2],
        bearer: SYNTHETIC_CANARIES[4],
        rawError: SYNTHETIC_CANARIES[5]
      }
    });
    const lease = store.acquireLease({
      workflowId,
      ownerId: 'synthetic-neatlogs-verifier',
      ttlMs: 10_000,
      eventId: `event:lease:${run}`
    });
    store.transitionWorkflow({
      ...lease,
      expectedRevision: 0,
      toState: 'running',
      eventId: `event:running:${run}`
    });
    const page = store.readEventPage({ afterSequence: 0, limit: 32 });
    const tokenize = tokenizer.tokenize.bind(tokenizer);
    return page.events.map(event => projectTelemetryEvent(event, tokenize));
  } finally {
    tokenizer.destroy?.();
    store.close();
    rmSync(scratch, { recursive: true, force: true });
  }
}

async function captureEnvelope(records) {
  // Imported lazily so the SDK sees a scrubbed environment.
  const { createNeatlogsSdkCapture } = await import('../src/observability/private/neatlogs-sdk-capture.js');
  const capture = createNeatlogsSdkCapture({ timeoutMs: 500 });
  try {
    const result = await capture.prepare(records);
    if (result.status !== 'captured_local') {
      throw new Error(`Local SDK capture failed (${result.reasonCode}).`);
    }
    return result.envelope;
  } finally {
    await capture.close();
  }
}

function serializablePlan(plan) {
  return {
    schema: PLAN_SCHEMA,
    workflowName: plan.workflowName,
    tags: [...plan.tags],
    traces: plan.traces.map(trace => ({
      traceId: trace.traceId,
      rootSpanId: trace.rootSpanId,
      markerSpanId: trace.markerSpanId,
      generation: trace.generation,
      priorTraceIds: [...trace.priorTraceIds],
      sessionId: trace.sessionId,
      eventToken: trace.eventToken,
      sequence: trace.sequence,
      revision: trace.revision,
      eventType: trace.eventType,
      fence: trace.fence,
      input: trace.input
    }))
  };
}

function reconstructPlan(parsed) {
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)
      || Object.keys(parsed).sort().join(',') !== 'schema,tags,traces,workflowName'
      || parsed.schema !== PLAN_SCHEMA || parsed.workflowName !== NEATLOGS_LIVE_WORKFLOW_NAME
      || !Array.isArray(parsed.traces) || parsed.traces.length < 1
      || parsed.traces.length > NEATLOGS_LIVE_LIMITS.maxTraces) {
    throw new Error('Plan file is invalid.');
  }
  const tags = validateNeatlogsTags(parsed.tags);
  if (tags.length !== 2 || !/^agent-harness@[0-9a-f]{40}$/.test(tags[0]) || tags[1] !== 'evidence:synthetic') {
    throw new Error('Plan file is invalid.');
  }
  const requiredTraceKeys = new Set([
    'eventToken', 'eventType', 'fence', 'input', 'markerSpanId', 'revision',
    'rootSpanId', 'sequence', 'sessionId', 'traceId'
  ]);
  const allowedTraceKeys = new Set([...requiredTraceKeys, 'generation', 'priorTraceIds']);
  if (parsed.traces.some(trace => !trace || typeof trace !== 'object' || Array.isArray(trace)
      || Object.keys(trace).some(key => !allowedTraceKeys.has(key))
      || [...requiredTraceKeys].some(key => !Object.hasOwn(trace, key)))) {
    throw new Error('Plan file is invalid.');
  }
  const records = validateNeatlogsSourceRecords(parsed.traces.map(trace => {
    if (typeof trace.input !== 'string' || Buffer.byteLength(trace.input, 'utf8') > 1_024) {
      throw new Error('Plan file is invalid.');
    }
    return JSON.parse(trace.input);
  }));
  const traces = parsed.traces.map((trace, index) => {
    const record = records[index];
    // The earlier v1 writer always emitted these fields, but its reader
    // explicitly supported generation-0 plans that omitted them. Keep that
    // bounded legacy shape while rejecting explicit null or malformed values.
    const generation = Object.hasOwn(trace, 'generation') ? trace.generation : 0;
    if (!Number.isSafeInteger(generation) || generation < 0 || generation > NEATLOGS_OTLP_LIMITS.generations) {
      throw new Error('Plan file is invalid.');
    }
    const hasPriorTraceIds = Object.hasOwn(trace, 'priorTraceIds');
    const priorTraceIds = hasPriorTraceIds ? trace.priorTraceIds : [];
    if ((!hasPriorTraceIds && generation !== 0)
        || !Array.isArray(priorTraceIds) || priorTraceIds.length !== generation
        || priorTraceIds.some(id => typeof id !== 'string')) {
      throw new Error('Plan file is invalid.');
    }
    const ids = deterministicNeatlogsIds(record.eventId, generation);
    const priorIds = Array.from({ length: generation }, (_, priorGeneration) => (
      deterministicNeatlogsIds(record.eventId, priorGeneration).traceId));
    const canonicalInput = canonicalNeatlogsInput(record);
    if (JSON.stringify(priorTraceIds) !== JSON.stringify(priorIds)
        || trace.traceId !== ids.traceId || trace.rootSpanId !== ids.rootSpanId
        || trace.markerSpanId !== ids.markerSpanId
        || trace.sessionId !== record.workflowId || trace.eventToken !== record.eventId
        || trace.sequence !== record.sequence || trace.revision !== record.revision
        || trace.eventType !== record.eventType || trace.fence !== record.fence
        || trace.input !== canonicalInput) {
      throw new Error('Plan file does not match its derived identifiers.');
    }
    return Object.freeze({
      ...ids,
      generation,
      priorTraceIds: Object.freeze([...priorIds]),
      sessionId: record.workflowId,
      eventToken: record.eventId,
      sequence: record.sequence,
      revision: record.revision,
      eventType: record.eventType,
      fence: record.fence,
      input: canonicalInput,
      record
    });
  });
  return Object.freeze({
    workflowName: NEATLOGS_LIVE_WORKFLOW_NAME,
    tags,
    traces: Object.freeze(traces)
  });
}

function reconstructRecoveryPlan(plan) {
  if (!plan || typeof plan !== 'object' || Array.isArray(plan)
      || Object.keys(plan).sort().join(',') !== 'tags,traces,workflowName'
      || plan.workflowName !== NEATLOGS_LIVE_WORKFLOW_NAME
      || !Array.isArray(plan.traces) || plan.traces.length < 1
      || plan.traces.length > NEATLOGS_LIVE_LIMITS.maxTraces) {
    throw new Error('Plan file is invalid.');
  }
  validateNeatlogsTags(plan.tags);
  for (const trace of plan.traces) {
    if (!trace || typeof trace !== 'object' || Array.isArray(trace)
        || typeof trace.input !== 'string' || Buffer.byteLength(trace.input, 'utf8') > 1_024
        || !Number.isSafeInteger(trace.generation) || trace.generation < 0
        || trace.generation > NEATLOGS_OTLP_LIMITS.generations
        || !Array.isArray(trace.priorTraceIds)
        || trace.priorTraceIds.length > NEATLOGS_OTLP_LIMITS.generations) {
      throw new Error('Plan file is invalid.');
    }
  }
  return reconstructPlan(serializablePlan(plan));
}

export function loadPlan(path) {
  const stat = statSync(path);
  if (!stat.isFile() || stat.size < 2 || stat.size > MAX_PLAN_BYTES) throw new Error('Plan file is invalid.');
  return reconstructPlan(parseNeatlogsJson(readFileSync(path, 'utf8')));
}

// A regenerated trace is a new remote trace, so it gets fresh export times.
function withFreshTimes(plan) {
  const now = Date.now() * 1e6;
  return Object.freeze({
    ...plan,
    traces: Object.freeze(plan.traces.map((trace, index) => Object.freeze({
      ...trace,
      root: Object.freeze({ start: now + index * 1e6, end: now + index * 1e6 + 100_000 }),
      marker: Object.freeze({ start: now + index * 1e6 + 200_000, end: now + index * 1e6 + 210_000 })
    })))
  });
}

function mergePlan(plan, regenerated, previousIds) {
  const byPrevious = new Map(regenerated.traces.map((trace, index) => [previousIds[index], trace]));
  return Object.freeze({
    ...plan,
    traces: Object.freeze(plan.traces.map(trace => byPrevious.get(trace.traceId) ?? trace))
  });
}

export async function sendRecoveryGeneration(config, plan, missingTraceIds, options = {}) {
  let canonicalPlan;
  try {
    // Rebuild the closed outbound shape from bounded, validated projected
    // inputs before selecting a new generation. Never trust caller metadata.
    canonicalPlan = reconstructRecoveryPlan(plan);
  } catch {
    return Object.freeze({
      regenerated: null,
      blocked: true,
      findings: Object.freeze(['plan_invalid']),
      send: null
    });
  }
  const regenerated = withFreshTimes(nextNeatlogsGeneration(canonicalPlan, missingTraceIds));
  const gated = await sendPlanWithPrivacyGate(config, regenerated, options);
  return Object.freeze({ regenerated, ...gated });
}

export function recoverableTraceIds(readback) {
  if (readback?.status !== 'missing' || !Array.isArray(readback.traces)) return Object.freeze([]);
  return Object.freeze(readback.traces.filter(trace => trace?.status === 'missing').map(trace => trace.traceId));
}

function leakScan(text, secrets) {
  const found = [];
  SYNTHETIC_CANARIES.forEach((canary, index) => {
    if (text.includes(canary)) found.push(`canary_${index}`);
  });
  secrets.forEach((secret, index) => {
    if (secret && text.includes(secret)) found.push(`credential_${index}`);
  });
  return found;
}

export function outboundPlanAudit(plan, secrets = []) {
  try {
    const body = encodeNeatlogsExportRequest(plan).toString('latin1');
    const serialized = JSON.stringify(serializablePlan(plan));
    const findings = leakScan(`${serialized}\n${body}`, secrets);
    return Object.freeze({ passed: findings.length === 0, findings: Object.freeze(findings) });
  } catch {
    return Object.freeze({ passed: false, findings: Object.freeze(['plan_invalid']) });
  }
}

export async function sendPlanWithPrivacyGate(config, plan, { secrets = [], fetch: fetchImpl, ...options } = {}) {
  const audit = outboundPlanAudit(plan, secrets);
  if (!audit.passed) return Object.freeze({ blocked: true, findings: audit.findings, send: null });
  const send = await sendNeatlogsPlan(config, plan, { ...options, ...(fetchImpl ? { fetch: fetchImpl } : {}) });
  return Object.freeze({ blocked: false, findings: Object.freeze([]), send });
}

async function main() {
const { values } = parseArgs({
  options: {
    mode: { type: 'string' },
    evidence: { type: 'string' },
    'env-file': { type: 'string' },
    plan: { type: 'string' },
    'component-revision': { type: 'string' },
    'max-polls': { type: 'string' },
    recover: { type: 'boolean' },
    'poll-interval-ms': { type: 'string' }
  },
  strict: true
});
const mode = values.mode;
if (!['offline', 'send', 'readback', 'send-and-readback'].includes(mode)) usage('Unknown --mode.');
if (!values.evidence) usage('--evidence is required.');
const revision = values['component-revision'] ?? null;
if (mode !== 'readback' && (revision === null || !REVISION.test(revision))) {
  usage('--component-revision must be the full 40-character agent-harness commit SHA.');
}

const config = loadNeatlogsConfig({
  env: { ...process.env },
  envFile: values['env-file'] ? resolve(values['env-file']) : null
});
scrubNeatlogsEnvironment(process.env);
const secrets = [];
try { secrets.push(config.ingestKey()); } catch { secrets.push(null); }
try { secrets.push(config.readbackToken()); } catch { secrets.push(null); }

const evidence = {
  schema: EVIDENCE_SCHEMA,
  generatedAt: new Date().toISOString(),
  mode,
  evidenceClass: mode === 'offline' ? 'offline-fixture' : 'live-synthetic',
  syntheticData: true,
  componentRevisions: { agentHarness: revision, neatlogsSdk: '1.1.28' },
  config: config.summary,
  plan: null,
  redaction: null,
  send: null,
  readback: null,
  deliveryConfirmed: false
};

let plan;
if (mode === 'readback') {
  if (!values.plan) usage('--plan is required for readback mode.');
  plan = loadPlan(resolve(values.plan));
} else {
  const records = buildSyntheticRecords();
  const envelope = await captureEnvelope(records);
  plan = planNeatlogsExport(envelope, {
    tags: [`agent-harness@${revision}`, 'evidence:synthetic']
  });
  const bodyFindings = outboundPlanAudit(plan, secrets).findings;
  evidence.redaction = {
    rawCanariesSeeded: SYNTHETIC_CANARIES.length,
    exportBodyFindings: bodyFindings,
    passed: bodyFindings.length === 0
  };
  if (bodyFindings.length > 0) {
    process.stderr.write('Redaction gate failed; nothing was sent.\n');
    process.exit(1);
  }
  if (values.plan) writeJsonAtomically(resolve(values.plan), serializablePlan(plan));
}
evidence.plan = serializablePlan(plan);

if (mode === 'send' || mode === 'send-and-readback') {
  const result = await sendPlanWithPrivacyGate(config, plan, { secrets });
  evidence.send = result.blocked
    ? { status: 'rejected', reasonCode: 'REDACTION_GATE_FAILED', findings: result.findings, attempts: 0 }
    : result.send;
}
if (mode === 'readback' || (mode === 'send-and-readback' && evidence.send?.status === 'receiver_accepted')) {
  const options = {};
  if (values['max-polls']) options.maxPolls = Number(values['max-polls']);
  if (values['poll-interval-ms']) options.pollIntervalMs = Number(values['poll-interval-ms']);
  evidence.readback = await readbackNeatlogsPlan(config, plan, options);
  // Recovery is opt-in and applies only after bounded 404 non-observation.
  // A delayed earlier trace can still appear, so a later generation can
  // create a duplicate event. Conflicts, pending projections and mismatches
  // are never re-sent.
  evidence.recovery = null;
  const missing = recoverableTraceIds(evidence.readback);
  if (values.recover && evidence.readback.status === 'missing' && missing.length > 0) {
    const gated = await sendRecoveryGeneration(config, plan, missing, { secrets });
    if (gated.blocked) {
      evidence.recovery = {
        regeneratedFrom: missing,
        regeneratedTo: [],
        basis: 'NOT_OBSERVED_WITHIN_BOUNDED_POLL_WINDOW',
        duplicateDeliveryRisk: 'PRIOR_GENERATION_MAY_APPEAR_LATER',
        send: { status: 'rejected', reasonCode: 'REDACTION_GATE_FAILED', findings: gated.findings, attempts: 0 }
      };
    } else {
      const regenerated = gated.regenerated;
      plan = mergePlan(plan, regenerated, missing);
      const nextPlan = serializablePlan(plan);
      const planAudit = outboundPlanAudit(plan, secrets);
      if (!planAudit.passed) throw new Error('Plan redaction gate failed.');
      if (values.plan) writeJsonAtomically(resolve(values.plan), nextPlan);
      evidence.plan = nextPlan;
      evidence.recovery = {
        regeneratedFrom: missing,
        regeneratedTo: regenerated.traces.map(trace => trace.traceId),
        basis: 'NOT_OBSERVED_WITHIN_BOUNDED_POLL_WINDOW',
        duplicateDeliveryRisk: 'PRIOR_GENERATION_MAY_APPEAR_LATER',
        send: gated.send
      };
      if (gated.send.status === 'receiver_accepted') {
        evidence.readback = await readbackNeatlogsPlan(config, plan, options);
      }
    }
  }
  evidence.deliveryConfirmed = evidence.readback.status === 'verified';
}

const serialized = JSON.stringify(evidence);
const evidenceFindings = leakScan(serialized, secrets);
if (evidenceFindings.length > 0) {
  process.stderr.write(`Evidence leak scan failed (${evidenceFindings.join(',')}); evidence not written.\n`);
  process.exit(1);
}
writeJsonAtomically(resolve(values.evidence), evidence);
process.stdout.write(`${JSON.stringify({
  mode,
  evidenceClass: evidence.evidenceClass,
  traces: plan.traces.length,
  send: evidence.send?.status ?? null,
  readback: evidence.readback?.status ?? null,
  deliveryConfirmed: evidence.deliveryConfirmed
})}\n`);
const succeeded = mode === 'offline'
  || (mode === 'send' && evidence.send?.status === 'receiver_accepted')
  || evidence.deliveryConfirmed;
process.exit(succeeded ? 0 : 1);
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main().catch(() => {
    process.stderr.write('Neatlogs verification failed.\n');
    process.exitCode = 1;
  });
}
