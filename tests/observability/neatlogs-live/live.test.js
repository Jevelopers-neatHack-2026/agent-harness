import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { inspect } from 'node:util';
import { after, before, describe, test } from 'node:test';
import { randomUUID } from 'node:crypto';
import {
  DurableWorkflowStore,
  createHmacTokenizer,
  projectTelemetryEvent
} from '@jevelopers/agent-harness-runtime';
import {
  NeatlogsConfigError,
  loadNeatlogsConfig,
  readNeatlogsEnvFile,
  scrubNeatlogsEnvironment
} from '../../../src/observability/private/neatlogs-config.js';
import {
  decodeNeatlogsExportResponse,
  deterministicNeatlogsIds,
  encodeNeatlogsExportRequest,
  NEATLOGS_LIVE_WORKFLOW_NAME,
  nextNeatlogsGeneration,
  planNeatlogsExport
} from '../../../src/observability/private/neatlogs-otlp.js';
import { readbackNeatlogsPlan, sendNeatlogsPlan } from '../../../src/observability/private/neatlogs-live.js';
import {
  loadPlan,
  recoverableTraceIds,
  sendRecoveryGeneration
} from '../../../tools/neatlogs-live-verify.mjs';
import { createNeatlogsSdkCapture } from '../../../src/observability/private/neatlogs-sdk-capture.js';

const require = createRequire(import.meta.url);
const otlpRoot = require(resolve('node_modules/@opentelemetry/otlp-transformer/build/src/generated/root.js'));
const ExportRequest = otlpRoot.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
const ExportResponse = otlpRoot.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;

const INGEST_KEY = 'synthetic-ingest-key-0123456789abcdef';
const READ_TOKEN = 'synthetic-readback-token-0123456789abcdef';
const PROJECT_ID = '00000000-0000-4000-8000-000000000001';
const REVISION = 'a'.repeat(40);
const CANARIES = Object.freeze([
  'SYNTHETIC_RAW_ID_CANARY_test',
  'SYNTHETIC_PRIVATE_URL_CANARY_https://private.invalid/x?token=y',
  'SYNTHETIC_EMAIL_CANARY_person@example.invalid',
  'SYNTHETIC_CUSTOMER_TEXT_CANARY_test'
]);

let scratch;
let plan;

function configured(overrides = {}) {
  return loadNeatlogsConfig({
    env: {
      NEATLOGS_INGEST_KEY: INGEST_KEY,
      NEATLOGS_READ_TOKEN: READ_TOKEN,
      NEATLOGS_PROJECT_ID: PROJECT_ID,
      ...overrides
    }
  });
}

function response(status, body = '', headers = {}) {
  return new Response(body === null ? null : body, { status, headers });
}

function recordingFetch(handler) {
  const calls = [];
  const fetchImpl = async (url, init) => {
    calls.push({ url: String(url), init });
    return handler(String(url), init, calls.length);
  };
  return { fetchImpl, calls };
}

async function buildPlan() {
  let generated = 0;
  const store = DurableWorkflowStore.open({
    databasePath: join(scratch, 'store.sqlite'),
    clock: () => 5_000,
    idFactory: () => `generated-${randomUUID()}-${++generated}`
  });
  const tokenizer = createHmacTokenizer(Buffer.alloc(32, 0x3c));
  try {
    const workflowId = `workflow:${CANARIES[0]}`;
    store.createWorkflow({
      workflowId,
      eventId: 'event:created',
      checkpoint: { url: CANARIES[1], email: CANARIES[2], text: CANARIES[3] }
    });
    const lease = store.acquireLease({ workflowId, ownerId: 'w', ttlMs: 10_000, eventId: 'event:lease' });
    store.transitionWorkflow({ ...lease, expectedRevision: 0, toState: 'running', eventId: 'event:run' });
    const page = store.readEventPage({ afterSequence: 0, limit: 32 });
    const tokenize = tokenizer.tokenize.bind(tokenizer);
    const records = page.events.map(event => projectTelemetryEvent(event, tokenize));
    const capture = createNeatlogsSdkCapture({ timeoutMs: 500 });
    try {
      const result = await capture.prepare(records);
      assert.equal(result.status, 'captured_local');
      return planNeatlogsExport(result.envelope, { tags: [`agent-harness@${REVISION}`, 'evidence:synthetic'] });
    } finally {
      await capture.close();
    }
  } finally {
    tokenizer.destroy?.();
    store.close();
  }
}

function readbackServer({
  finalizeAfter = 1, spans = 'both', tags, inputFor, projectId = PROJECT_ID,
  served = plan, droppedIds = [], alsoStored = [], page = null, traceStatus = 200,
  spansStatus = 200, payloadStatus = 200, traceOverrides = {}, spansCount,
  markerTraceId, rootTraceId, payloadTraceId, payloadSpanId,
  projectName = 'Synthetic project', workflowName, remoteTags, alsoStoredTraceId,
  alsoStoredFinalizationStatus = 'finalized'
} = {}) {
  const polls = new Map();
  return recordingFetch((url) => {
    const path = new URL(url).pathname.replace('/api/v1/public', '');
    const ok = data => response(200, JSON.stringify({ success: true, requestId: 'req_test', data }), {
      'content-type': 'application/json'
    });
    if (path === '/project') return ok({ id: projectId, name: projectName });
    const match = /^\/traces\/([0-9a-f]{32})(\/spans(?:\/([0-9a-f]{16})\/payload)?)?$/.exec(path);
    if (alsoStored.includes(match?.[1]) && !match[2]) {
      return ok({ traceId: alsoStoredTraceId ?? match[1], finalizationStatus: alsoStoredFinalizationStatus });
    }
    const trace = served.traces.find(item => item.traceId === match?.[1]);
    if (!trace || droppedIds.includes(trace.traceId)) {
      return response(404, '{}', { 'content-type': 'application/problem+json' });
    }
    if (!match[2] && traceStatus !== 200) return response(traceStatus, '{}');
    if (match[2] && !match[3] && spansStatus !== 200) return response(spansStatus, '{}');
    if (match[3] && payloadStatus !== 200) return response(payloadStatus, '{}');
    if (!match[2]) {
      const count = (polls.get(trace.traceId) ?? 0) + 1;
      polls.set(trace.traceId, count);
      return ok({
        traceId: trace.traceId,
        sessionId: trace.sessionId,
        workflowName: workflowName ?? served.workflowName,
        traceName: 'agent_harness.workflow_event',
        tags: remoteTags ?? tags ?? [...served.tags],
        spansCount: spansCount ?? (spans === 'duplicate' ? 3 : spans === 'root-only' ? 1 : 2),
        finalizationStatus: count >= finalizeAfter ? 'finalized' : 'pending',
        ...traceOverrides
      });
    }
    if (!match[3]) {
      const list = [{
        traceId: rootTraceId ?? trace.traceId, spanId: trace.rootSpanId, parentSpanId: null,
        spanName: 'agent_harness.workflow_event', spanType: 'WORKFLOW'
      }];
      if (spans !== 'root-only') {
        list.push({
          traceId: markerTraceId ?? trace.traceId, spanId: trace.markerSpanId, parentSpanId: trace.rootSpanId,
          spanName: 'neatlogs.trace.complete', spanType: 'INTERNAL'
        });
      }
      if (spans === 'duplicate') list.push({ ...list[0] });
      return ok({ spans: list, page: page ?? { hasMore: false, limit: 50, nextCursor: null } });
    }
    return ok({
      traceId: payloadTraceId ?? trace.traceId,
      spanId: payloadSpanId ?? trace.rootSpanId,
      fields: {
        input: {
          available: true,
          truncated: false,
          mimeType: 'application/json',
          value: inputFor ? inputFor(trace) : trace.input
        }
      }
    });
  });
}

function storedPlan(sourcePlan = plan) {
  return {
    schema: 'agent-harness.neatlogs.export-plan.v1',
    workflowName: sourcePlan.workflowName,
    tags: [...sourcePlan.tags],
    traces: sourcePlan.traces.map(trace => ({
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

before(async () => {
  scratch = mkdtempSync(join(tmpdir(), 'neatlogs-live-test-'));
  plan = await buildPlan();
});

after(() => rmSync(scratch, { recursive: true, force: true }));

describe('configuration', () => {
  test('loads all seven recognized names and redacts selected and ignored credentials', () => {
    const file = join(scratch, 'neatlogs.env');
    const ignoredLegacyIngest = 'synthetic-ignored-legacy-ingest-canary-0123456789';
    const ignoredLegacyRead = 'synthetic-ignored-legacy-read-canary-0123456789';
    writeFileSync(file, [
      '# comment',
      `NEATLOGS_INGEST_KEY="${INGEST_KEY}"`,
      `export NEATLOGS_PROJECT_ID=${PROJECT_ID}`,
      'NEATLOGS_READ_TOKEN=',
      `NEATLOGS_API_KEY=${ignoredLegacyIngest}`,
      `NEATLOGS_TOKEN=${ignoredLegacyRead}`,
      'NEATLOGS_ENDPOINT=https://ingest.neatlogs.com/v1/traces',
      'NEATLOGS_HOST=https://app.neatlogs.com',
      'UNRELATED_SECRET=should-not-be-read',
      ''
    ].join('\n'));
    assert.deepEqual(Object.keys(readNeatlogsEnvFile(file)).sort(), [
      'NEATLOGS_API_KEY', 'NEATLOGS_ENDPOINT', 'NEATLOGS_HOST', 'NEATLOGS_INGEST_KEY',
      'NEATLOGS_PROJECT_ID', 'NEATLOGS_READ_TOKEN', 'NEATLOGS_TOKEN'
    ]);
    const config = loadNeatlogsConfig({ env: { NEATLOGS_READ_TOKEN: READ_TOKEN }, envFile: file });
    assert.equal(config.summary.region, 'us');
    assert.equal(config.summary.ingestReady, true);
    assert.equal(config.summary.readbackReady, true);
    assert.equal(config.ingestKey(), INGEST_KEY);
    assert.equal(config.readbackToken(), READ_TOKEN);
    for (const rendered of [JSON.stringify(config), inspect(config), String(Object.values(config.summary))]) {
      assert.ok(!rendered.includes(INGEST_KEY));
      assert.ok(!rendered.includes(READ_TOKEN));
      assert.ok(!rendered.includes(ignoredLegacyIngest));
      assert.ok(!rendered.includes(ignoredLegacyRead));
    }
  });

  test('derives EU origins and refuses mixed regions or malformed values', () => {
    const eu = configured({ NEATLOGS_ENDPOINT: 'https://eu.ingest.neatlogs.com/v1/traces' });
    assert.equal(eu.summary.region, 'eu');
    assert.equal(eu.summary.appOrigin, 'https://eu.app.neatlogs.com');
    assert.throws(() => configured({
      NEATLOGS_ENDPOINT: 'https://eu.ingest.neatlogs.com',
      NEATLOGS_HOST: 'https://app.neatlogs.com'
    }), error => error instanceof NeatlogsConfigError && error.code === 'REGION_MISMATCH');
    assert.throws(() => configured({ NEATLOGS_HOST: 'http://app.neatlogs.com' }),
      error => error.code === 'INVALID_ORIGIN');
    assert.throws(() => configured({ NEATLOGS_PROJECT_ID: 'not-a-uuid' }),
      error => error.code === 'INVALID_PROJECT_ID');
    assert.throws(() => configured({ NEATLOGS_INGEST_KEY: 'has space in it 0123456789' }),
      error => error.code === 'INVALID_CREDENTIAL' && !error.message.includes('space'));
  });

  test('reads the pre-rename names as fallbacks; the new names win when both are set', () => {
    const legacy = loadNeatlogsConfig({
      env: { NEATLOGS_API_KEY: INGEST_KEY, NEATLOGS_TOKEN: READ_TOKEN, NEATLOGS_PROJECT_ID: PROJECT_ID }
    });
    assert.equal(legacy.ingestKey(), INGEST_KEY);
    assert.equal(legacy.readbackToken(), READ_TOKEN);
    assert.equal(legacy.summary.readbackReady, true);
    const both = loadNeatlogsConfig({
      env: { NEATLOGS_INGEST_KEY: `${INGEST_KEY}-new`, NEATLOGS_API_KEY: INGEST_KEY,
        NEATLOGS_READ_TOKEN: `${READ_TOKEN}-new`, NEATLOGS_TOKEN: READ_TOKEN,
        NEATLOGS_PROJECT_ID: PROJECT_ID }
    });
    assert.equal(both.ingestKey(), `${INGEST_KEY}-new`);
    assert.equal(both.readbackToken(), `${READ_TOKEN}-new`);
    assert.equal(both.summary.readbackReady, true);
    assert.throws(() => loadNeatlogsConfig({ env: {} }).ingestKey(), error => error.setting === 'NEATLOGS_INGEST_KEY' || /NEATLOGS_INGEST_KEY/.test(String(error.message)));
  });

  test('keeps readback unavailable when a read token is configured without a project ID', () => {
    const config = loadNeatlogsConfig({
      env: {
        NEATLOGS_INGEST_KEY: `${INGEST_KEY}-new`,
        NEATLOGS_API_KEY: INGEST_KEY,
        NEATLOGS_READ_TOKEN: `${READ_TOKEN}-new`,
        NEATLOGS_TOKEN: READ_TOKEN
      }
    });
    assert.equal(config.summary.ingestReady, true);
    assert.equal(config.summary.readbackTokenConfigured, true);
    assert.equal(config.summary.readbackReady, false);
    assert.throws(
      () => config.readbackToken(),
      error => error instanceof NeatlogsConfigError
        && error.code === 'MISSING_SETTING'
        && error.setting === 'NEATLOGS_PROJECT_ID'
    );
  });

  test('uses canonical env, canonical file, legacy env, then legacy file; blanks fall through', () => {
    const writeEnv = (name, entries) => {
      const path = join(scratch, name);
      writeFileSync(path, entries.map(([key, value]) => `${key}=${value}`).join('\n'));
      return path;
    };
    const canonicalFile = writeEnv('canonical.env', [
      ['NEATLOGS_INGEST_KEY', 'synthetic-canonical-file-ingest-0123456789'],
      ['NEATLOGS_READ_TOKEN', 'synthetic-canonical-file-read-0123456789'],
      ['NEATLOGS_PROJECT_ID', PROJECT_ID],
      ['NEATLOGS_API_KEY', 'synthetic-legacy-file-ingest-0123456789'],
      ['NEATLOGS_TOKEN', 'synthetic-legacy-file-read-0123456789']
    ]);
    const canonicalFileWins = loadNeatlogsConfig({
      env: {
        NEATLOGS_API_KEY: 'synthetic-legacy-env-ingest-0123456789',
        NEATLOGS_TOKEN: 'synthetic-legacy-env-read-0123456789',
        NEATLOGS_PROJECT_ID: PROJECT_ID
      },
      envFile: canonicalFile
    });
    assert.equal(canonicalFileWins.ingestKey(), 'synthetic-canonical-file-ingest-0123456789');
    assert.equal(canonicalFileWins.readbackToken(), 'synthetic-canonical-file-read-0123456789');

    const legacyFile = writeEnv('legacy.env', [
      ['NEATLOGS_API_KEY', 'synthetic-legacy-file-ingest-0123456789'],
      ['NEATLOGS_TOKEN', 'synthetic-legacy-file-read-0123456789'],
      ['NEATLOGS_PROJECT_ID', PROJECT_ID]
    ]);
    const canonicalEnvWins = loadNeatlogsConfig({
      env: {
        NEATLOGS_INGEST_KEY: 'synthetic-canonical-env-ingest-0123456789',
        NEATLOGS_READ_TOKEN: 'synthetic-canonical-env-read-0123456789',
        NEATLOGS_PROJECT_ID: PROJECT_ID
      },
      envFile: legacyFile
    });
    assert.equal(canonicalEnvWins.ingestKey(), 'synthetic-canonical-env-ingest-0123456789');
    assert.equal(canonicalEnvWins.readbackToken(), 'synthetic-canonical-env-read-0123456789');

    const legacyOnly = loadNeatlogsConfig({ envFile: legacyFile });
    assert.equal(legacyOnly.ingestKey(), 'synthetic-legacy-file-ingest-0123456789');
    assert.equal(legacyOnly.readbackToken(), 'synthetic-legacy-file-read-0123456789');

    const blankCanonical = loadNeatlogsConfig({
      env: {
        NEATLOGS_INGEST_KEY: '  ',
        NEATLOGS_READ_TOKEN: '\t',
        NEATLOGS_API_KEY: 'synthetic-legacy-env-ingest-0123456789',
        NEATLOGS_TOKEN: 'synthetic-legacy-env-read-0123456789',
        NEATLOGS_PROJECT_ID: PROJECT_ID
      },
      envFile: writeEnv('blank-canonical.env', [
        ['NEATLOGS_INGEST_KEY', '   '],
        ['NEATLOGS_READ_TOKEN', ''],
        ['NEATLOGS_API_KEY', 'synthetic-legacy-file-ingest-0123456789'],
        ['NEATLOGS_TOKEN', 'synthetic-legacy-file-read-0123456789']
      ])
    });
    assert.equal(blankCanonical.ingestKey(), 'synthetic-legacy-env-ingest-0123456789');
    assert.equal(blankCanonical.readbackToken(), 'synthetic-legacy-env-read-0123456789');

    assert.throws(() => loadNeatlogsConfig({
      env: {
        NEATLOGS_INGEST_KEY: 'synthetic-invalid-canonical 0123456789',
        NEATLOGS_API_KEY: 'synthetic-valid-legacy-ingest-0123456789',
        NEATLOGS_PROJECT_ID: PROJECT_ID
      }
    }), error => error.code === 'INVALID_CREDENTIAL');
    assert.throws(() => loadNeatlogsConfig({
      env: {
        NEATLOGS_READ_TOKEN: 'synthetic-invalid-canonical 0123456789',
        NEATLOGS_TOKEN: 'synthetic-valid-legacy-read-0123456789',
        NEATLOGS_PROJECT_ID: PROJECT_ID
      }
    }), error => error.code === 'INVALID_CREDENTIAL');
    assert.throws(() => loadNeatlogsConfig({
      env: {
        NEATLOGS_API_KEY: 'synthetic-valid-legacy-ingest-0123456789',
        NEATLOGS_PROJECT_ID: PROJECT_ID
      },
      envFile: writeEnv('invalid-canonical-ingest.env', [
        ['NEATLOGS_INGEST_KEY', 'synthetic-invalid-canonical 0123456789']
      ])
    }), error => error.code === 'INVALID_CREDENTIAL');
    assert.throws(() => loadNeatlogsConfig({
      env: {
        NEATLOGS_TOKEN: 'synthetic-valid-legacy-read-0123456789',
        NEATLOGS_PROJECT_ID: PROJECT_ID
      },
      envFile: writeEnv('invalid-canonical-read.env', [
        ['NEATLOGS_READ_TOKEN', 'synthetic-invalid-canonical 0123456789']
      ])
    }), error => error.code === 'INVALID_CREDENTIAL');
  });

  test('keeps selected and ignored credential canaries out of errors and scrubs all seven names', () => {
    const selectedIngest = 'synthetic-selected-ingest-canary-0123456789';
    const selectedRead = 'synthetic-selected-read-canary-0123456789';
    const ignoredIngest = 'synthetic-ignored-ingest-canary-0123456789';
    const ignoredRead = 'synthetic-ignored-read-canary-0123456789';
    const config = loadNeatlogsConfig({
      env: {
        NEATLOGS_INGEST_KEY: selectedIngest,
        NEATLOGS_READ_TOKEN: selectedRead,
        NEATLOGS_API_KEY: ignoredIngest,
        NEATLOGS_TOKEN: ignoredRead,
        NEATLOGS_PROJECT_ID: PROJECT_ID
      }
    });
    const configOutput = [JSON.stringify(config), inspect(config), String(Object.values(config.summary))].join('\n');
    for (const canary of [selectedIngest, selectedRead, ignoredIngest, ignoredRead]) {
      assert.equal(configOutput.includes(canary), false);
    }

    for (const env of [
      {
        NEATLOGS_INGEST_KEY: 'synthetic-invalid-selected-ingest 0123456789',
        NEATLOGS_API_KEY: ignoredIngest,
        NEATLOGS_PROJECT_ID: PROJECT_ID
      },
      {
        NEATLOGS_INGEST_KEY: '  ',
        NEATLOGS_API_KEY: 'synthetic-invalid-selected-legacy 0123456789',
        NEATLOGS_PROJECT_ID: PROJECT_ID
      }
    ]) {
      let thrown;
      try {
        loadNeatlogsConfig({ env });
      } catch (error) {
        thrown = error;
      }
      assert.ok(thrown instanceof NeatlogsConfigError);
      const errorOutput = [String(thrown), inspect(thrown), JSON.stringify(thrown)].join('\n');
      for (const canary of [
        env.NEATLOGS_INGEST_KEY,
        env.NEATLOGS_API_KEY,
        selectedIngest,
        selectedRead,
        ignoredIngest,
        ignoredRead
      ]) {
        if (canary.trim() !== '') assert.equal(errorOutput.includes(canary), false);
      }
    }

    const env = {
      NEATLOGS_INGEST_KEY: selectedIngest,
      NEATLOGS_READ_TOKEN: selectedRead,
      NEATLOGS_PROJECT_ID: PROJECT_ID,
      NEATLOGS_ENDPOINT: 'https://ingest.neatlogs.com/v1/traces',
      NEATLOGS_HOST: 'https://app.neatlogs.com',
      NEATLOGS_API_KEY: ignoredIngest,
      NEATLOGS_TOKEN: ignoredRead,
      UNRELATED: 'keep'
    };
    assert.deepEqual([...scrubNeatlogsEnvironment(env)], [
      'NEATLOGS_INGEST_KEY', 'NEATLOGS_READ_TOKEN', 'NEATLOGS_PROJECT_ID',
      'NEATLOGS_ENDPOINT', 'NEATLOGS_HOST', 'NEATLOGS_API_KEY', 'NEATLOGS_TOKEN'
    ]);
    assert.deepEqual(env, { UNRELATED: 'keep' });
  });

  test('scrubs recognized names from an environment object', () => {
    const env = { NEATLOGS_INGEST_KEY: INGEST_KEY, NEATLOGS_HOST: 'https://app.neatlogs.com', PATH: '/bin' };
    assert.deepEqual([...scrubNeatlogsEnvironment(env)], ['NEATLOGS_INGEST_KEY', 'NEATLOGS_HOST']);
    assert.deepEqual(env, { PATH: '/bin' });
  });
});

describe('export encoding', () => {
  test('encodes a valid OTLP request with deterministic IDs and no raw content', () => {
    const body = encodeNeatlogsExportRequest(plan);
    for (const canary of CANARIES) assert.ok(!body.toString('latin1').includes(canary));
    const decoded = ExportRequest.toObject(ExportRequest.decode(body), { longs: String, bytes: String });
    const spans = decoded.resourceSpans[0].scopeSpans.flatMap(scope => scope.spans);
    assert.equal(spans.length, plan.traces.length * 2);
    for (const trace of plan.traces) {
      assert.deepEqual(deterministicNeatlogsIds(trace.eventToken), {
        traceId: trace.traceId, rootSpanId: trace.rootSpanId, markerSpanId: trace.markerSpanId
      });
      const root = spans.find(span => Buffer.from(span.spanId, 'base64').toString('hex') === trace.rootSpanId);
      const attributes = Object.fromEntries(root.attributes.map(item => [item.key, Object.values(item.value)[0]]));
      assert.equal(Buffer.from(root.traceId, 'base64').toString('hex'), trace.traceId);
      assert.equal(root.parentSpanId, undefined);
      assert.equal(attributes['neatlogs.session.id'], trace.sessionId);
      assert.equal(attributes['neatlogs.workflow.input'], trace.input);
      assert.equal(attributes['neatlogs.tags'], plan.tags.join(','));
      assert.equal(attributes['agent_harness.telemetry.event_token'], trace.eventToken);
      const marker = spans.find(span => Buffer.from(span.spanId, 'base64').toString('hex') === trace.markerSpanId);
      assert.equal(Buffer.from(marker.parentSpanId, 'base64').toString('hex'), trace.rootSpanId);
    }
    assert.deepEqual(encodeNeatlogsExportRequest(plan), body, 'retries resend identical bytes');
  });

  test('decodes full success, partial rejection and malformed responses strictly', () => {
    assert.deepEqual({ ...decodeNeatlogsExportResponse(Buffer.alloc(0), '') },
      { rejectedSpans: 0, receiverMessagePresent: false });
    const partial = ExportResponse.encode({ partialSuccess: { rejectedSpans: 2, errorMessage: 'nope' } }).finish();
    assert.deepEqual({ ...decodeNeatlogsExportResponse(partial, 'application/x-protobuf') },
      { rejectedSpans: 2, receiverMessagePresent: true });
    assert.equal(decodeNeatlogsExportResponse(
      Buffer.from('{"partialSuccess":{"rejectedSpans":"1"}}'), 'application/json').rejectedSpans, 1);
    assert.equal(decodeNeatlogsExportResponse(Buffer.from('{}'), 'application/json').rejectedSpans, 0);
    assert.equal(decodeNeatlogsExportResponse(Buffer.from('{"partialSuccess":{}}'), 'application/json').rejectedSpans, 0);
    assert.equal(decodeNeatlogsExportResponse(Buffer.from('{"unknownField":true}'), 'application/json').rejectedSpans, 0);
    assert.equal(decodeNeatlogsExportResponse(Buffer.from([0x10, 0x01]), 'application/x-protobuf').rejectedSpans, 0);
    assert.equal(decodeNeatlogsExportResponse(Buffer.from([0x0a, 0x02, 0x18, 0x01]), 'application/x-protobuf').rejectedSpans, 0);
    assert.equal(decodeNeatlogsExportResponse(Buffer.from([0x13, 0x08, 0x01, 0x14]), 'application/x-protobuf').rejectedSpans, 0);
    for (const malformed of [
      Buffer.from([0x00]), // protobuf field zero
      Buffer.from([0x08, 0x00]), // known partial_success field with varint wire type
      Buffer.from([0x0a, 0x02, 0x0a, 0x00]), // rejected_spans with bytes wire type
      Buffer.from([0x0a, 0x02, 0x10, 0x00]), // error_message with varint wire type
      Buffer.from([0x13, 0x1c, 0x14]), // mismatched protobuf group terminator
      Buffer.from([0x13, 0x08, 0x01]), // unterminated protobuf group
      Buffer.from([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02])
    ]) {
      assert.throws(() => decodeNeatlogsExportResponse(malformed, 'application/x-protobuf'),
        error => error.code === 'MALFORMED_RESPONSE');
    }
    for (const malformed of [
      '{"partialSuccess":true}',
      '{"partialSuccess":[]}',
      '{"partialSuccess":{"rejectedSpans":true}}',
      '{"partialSuccess":{"rejectedSpans":""}}',
      '{"partialSuccess":{"errorMessage":false}}',
      '{"partialSuccess":{"rejectedSpans":1,"rejectedSpans":0}}',
      '{"partialSuccess":{},"partial_success":{}}'
    ]) {
      assert.throws(() => decodeNeatlogsExportResponse(Buffer.from(malformed), 'application/json'),
        error => error.code === 'MALFORMED_RESPONSE');
    }
    assert.throws(() => decodeNeatlogsExportResponse(Buffer.from([0x0a, 0x05]), 'application/x-protobuf'),
      error => error.code === 'MALFORMED_RESPONSE');
    assert.throws(() => decodeNeatlogsExportResponse(Buffer.from('<html>'), 'text/html'),
      error => error.code === 'UNEXPECTED_RESPONSE_TYPE');
  });

  test('rejects fixed-width wire types for every known protobuf response field', () => {
    for (const malformed of [
      '090000000000000000', // ExportResponse.partial_success as fixed64
      '0d00000000', // ExportResponse.partial_success as fixed32
      '0a09090000000000000000', // PartialSuccess.rejected_spans as fixed64
      '0a050d00000000', // PartialSuccess.rejected_spans as fixed32
      '0a09110000000000000000', // PartialSuccess.error_message as fixed64
      '0a051500000000' // PartialSuccess.error_message as fixed32
    ]) {
      assert.throws(
        () => decodeNeatlogsExportResponse(Buffer.from(malformed, 'hex'), 'application/x-protobuf'),
        error => error.code === 'MALFORMED_RESPONSE'
      );
    }

    assert.equal(
      decodeNeatlogsExportResponse(Buffer.from('110000000000000000', 'hex'), 'application/x-protobuf').rejectedSpans,
      0,
      'unknown fixed64 fields remain forward-compatible'
    );
    assert.equal(
      decodeNeatlogsExportResponse(Buffer.from('1d00000000', 'hex'), 'application/x-protobuf').rejectedSpans,
      0,
      'unknown fixed32 fields remain forward-compatible'
    );
  });

  test('delivery generations keep generation 0 stable and give new IDs after a loss', () => {
    const trace = plan.traces[0];
    assert.deepEqual(deterministicNeatlogsIds(trace.eventToken, 0), deterministicNeatlogsIds(trace.eventToken));
    const next = nextNeatlogsGeneration(plan, [trace.traceId]);
    assert.equal(next.traces.length, 1);
    const moved = next.traces[0];
    assert.equal(moved.generation, 1);
    assert.deepEqual([...moved.priorTraceIds], [trace.traceId]);
    assert.notEqual(moved.traceId, trace.traceId);
    assert.notEqual(moved.rootSpanId, trace.rootSpanId);
    assert.deepEqual(deterministicNeatlogsIds(trace.eventToken, 1), {
      traceId: moved.traceId, rootSpanId: moved.rootSpanId, markerSpanId: moved.markerSpanId
    });
    assert.equal(moved.input, trace.input, 'content and session are unchanged');
    assert.equal(moved.sessionId, trace.sessionId);
    const third = nextNeatlogsGeneration(next, [moved.traceId]).traces[0];
    assert.deepEqual([...third.priorTraceIds], [trace.traceId, moved.traceId]);
    assert.throws(() => deterministicNeatlogsIds(trace.eventToken, 4), error => error.code === 'INVALID_GENERATION');
    assert.throws(() => nextNeatlogsGeneration(plan, ['f'.repeat(32)]), error => error.code === 'UNKNOWN_TRACE');
  });

  test('refuses envelopes that are not complete sanitized captures', () => {
    assert.throws(() => planNeatlogsExport({ spans: [], rootSpanCount: 0, remoteIngestionVerified: false }),
      error => error.code === 'INVALID_ENVELOPE');
    assert.throws(() => planNeatlogsExport({ spans: [], rootSpanCount: 0, remoteIngestionVerified: false },
      { tags: ['Bad,Tag'] }), error => error.code === 'INVALID_TAGS');
  });

  test('reconstructs a bounded saved plan from its canonical projected records', () => {
    const path = join(scratch, 'saved-plan.json');
    const valid = storedPlan();
    writeFileSync(path, JSON.stringify(valid));
    const loaded = loadPlan(path);
    assert.equal(loaded.workflowName, NEATLOGS_LIVE_WORKFLOW_NAME);
    assert.equal(loaded.traces.length, plan.traces.length);
    assert.equal(loaded.traces[0].eventType, plan.traces[0].record.eventType);
    assert.equal(loaded.traces[0].fence, plan.traces[0].record.fence);

    const legacyGenerationZero = storedPlan();
    for (const trace of legacyGenerationZero.traces) {
      delete trace.generation;
      delete trace.priorTraceIds;
    }
    writeFileSync(path, JSON.stringify(legacyGenerationZero));
    const loadedLegacy = loadPlan(path);
    assert.ok(loadedLegacy.traces.every(trace => trace.generation === 0));
    assert.ok(loadedLegacy.traces.every(trace => trace.priorTraceIds.length === 0));
    assert.equal(loadedLegacy.traces[0].eventToken, plan.traces[0].eventToken);

    const unsafeWorkflow = { ...valid, workflowName: CANARIES[3] };
    writeFileSync(path, JSON.stringify(unsafeWorkflow));
    assert.throws(() => loadPlan(path), /Plan file is invalid/);
    writeFileSync(path, JSON.stringify(valid).replace(
      '"workflowName":"agent-harness.workflow-events"',
      `"workflowName":"${CANARIES[3]}","workflowName":"agent-harness.workflow-events"`
    ));
    assert.throws(() => loadPlan(path), error => error.code === 'MALFORMED_RESPONSE');

    const inconsistent = storedPlan();
    inconsistent.traces[0].sequence += 1;
    writeFileSync(path, JSON.stringify(inconsistent));
    assert.throws(() => loadPlan(path), /does not match its derived identifiers/);

    const missingPriorList = storedPlan();
    missingPriorList.traces[0].priorTraceIds = null;
    writeFileSync(path, JSON.stringify(missingPriorList));
    assert.throws(() => loadPlan(path), /Plan file is invalid/);

    const nullGeneration = storedPlan();
    nullGeneration.traces[0].generation = null;
    writeFileSync(path, JSON.stringify(nullGeneration));
    assert.throws(() => loadPlan(path), /Plan file is invalid/);

    writeFileSync(path, 'x'.repeat(256 * 1024 + 1));
    assert.throws(() => loadPlan(path), /Plan file is invalid/);
  });

  test('recovery rejects an arbitrary noncanonical workflow before any fetch', async () => {
    const privateWorkflow = 'PRIVATE_CUSTOMER_WORKFLOW_SECRET_q8f31';
    const contaminated = Object.freeze({ ...plan, workflowName: privateWorkflow });
    const { fetchImpl, calls } = recordingFetch(() => response(200, null));
    const result = await sendRecoveryGeneration(configured(), contaminated, [plan.traces[0].traceId], {
      fetch: fetchImpl,
      secrets: [READ_TOKEN]
    });
    assert.equal(result.blocked, true);
    assert.deepEqual([...result.findings], ['plan_invalid']);
    assert.equal(JSON.stringify(result).includes(privateWorkflow), false);
    assert.equal(calls.length, 0);
  });

  test('recovery still blocks a configured secret in the canonical outbound plan', async () => {
    const { fetchImpl, calls } = recordingFetch(() => response(200, null));
    const result = await sendRecoveryGeneration(configured(), plan, [plan.traces[0].traceId], {
      fetch: fetchImpl,
      secrets: [NEATLOGS_LIVE_WORKFLOW_NAME]
    });
    assert.equal(result.blocked, true);
    assert.deepEqual([...result.findings], ['credential_0']);
    assert.equal(calls.length, 0);
  });

  test('a valid recovery send uses only the selected next generation', async () => {
    const source = plan.traces[0];
    const { fetchImpl, calls } = recordingFetch(() => response(200, null));
    const result = await sendRecoveryGeneration(configured(), plan, [source.traceId], { fetch: fetchImpl });
    const recovered = result.regenerated.traces[0];
    assert.equal(result.blocked, false);
    assert.equal(result.send.status, 'receiver_accepted');
    assert.equal(calls.length, 1);
    assert.equal(result.regenerated.traces.length, 1);
    assert.equal(recovered.generation, source.generation + 1);
    assert.deepEqual([...recovered.priorTraceIds], [source.traceId]);
    assert.notEqual(recovered.traceId, source.traceId);
    assert.equal(recovered.eventToken, source.eventToken);
    assert.equal(recovered.input, source.input);
  });
});

describe('send', () => {
  test('makes no request without an ingest key', async () => {
    const { fetchImpl, calls } = recordingFetch(() => response(200));
    const result = await sendNeatlogsPlan(loadNeatlogsConfig({ env: {} }), plan, { fetch: fetchImpl });
    assert.equal(result.status, 'not_configured');
    assert.equal(calls.length, 0);
  });

  test('receiver acceptance is reported without claiming delivery', async () => {
    const { fetchImpl, calls } = recordingFetch(() => response(200, null));
    const result = await sendNeatlogsPlan(configured(), plan, { fetch: fetchImpl });
    assert.equal(result.status, 'receiver_accepted');
    assert.equal(result.deliveryConfirmed, false);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].url, 'https://ingest.neatlogs.com/v1/traces');
    assert.equal(calls[0].init.headers['x-api-key'], INGEST_KEY);
    assert.equal(calls[0].init.redirect, 'error');
    assert.ok(!JSON.stringify(result).includes(INGEST_KEY));
  });

  test('partial rejection is never folded into success', async () => {
    const body = ExportResponse.encode({ partialSuccess: { rejectedSpans: 1 } }).finish();
    const { fetchImpl } = recordingFetch(() => response(200, body, { 'content-type': 'application/x-protobuf' }));
    const result = await sendNeatlogsPlan(configured(), plan, { fetch: fetchImpl });
    assert.equal(result.status, 'partially_rejected');
    assert.equal(result.rejectedSpans, 1);
  });

  test('a malformed 2xx protobuf response remains an unknown outcome', async () => {
    const malformed = Buffer.from('090000000000000000', 'hex');
    const { fetchImpl, calls } = recordingFetch(() => response(200, malformed, {
      'content-type': 'application/x-protobuf'
    }));
    const result = await sendNeatlogsPlan(configured(), plan, { fetch: fetchImpl, maxAttempts: 1 });
    assert.equal(result.status, 'outcome_unknown');
    assert.equal(result.reasonCode, 'MALFORMED_RESPONSE');
    assert.equal(result.deliveryConfirmed, false);
    assert.equal(calls.length, 1);
  });

  test('credential rejection stops immediately', async () => {
    const { fetchImpl, calls } = recordingFetch(() => response(401, 'unauthorized'));
    const result = await sendNeatlogsPlan(configured(), plan, { fetch: fetchImpl, retryDelayMs: 0 });
    assert.equal(result.status, 'rejected');
    assert.equal(result.reasonCode, 'CREDENTIAL_REJECTED');
    assert.equal(calls.length, 1);
  });

  test('a terminal refusal after an ambiguous attempt stays outcome_unknown', async () => {
    for (const terminalStatus of [401, 403, 400]) {
      const { fetchImpl, calls } = recordingFetch((url, init, count) => (
        count === 1 ? response(503, 'temporary') : response(terminalStatus, 'refused')
      ));
      const result = await sendNeatlogsPlan(configured(), plan, {
        fetch: fetchImpl, retryDelayMs: 0, maxAttempts: 2, sleep: async () => true
      });
      assert.equal(result.status, 'outcome_unknown');
      assert.equal(result.reasonCode, 'PRIOR_ATTEMPT_OUTCOME_UNKNOWN');
      assert.equal(result.httpStatus, 503);
      assert.deepEqual(result.lastResponse, {
        reasonCode: terminalStatus === 401 || terminalStatus === 403
          ? 'CREDENTIAL_REJECTED' : 'RECEIVER_REFUSED_REQUEST',
        httpStatus: terminalStatus
      });
      assert.equal(calls.length, 2);
      assert.deepEqual(calls[1].init.body, calls[0].init.body);
    }
  });

  test('retries 5xx with identical bytes, then reports an unknown outcome', async () => {
    const { fetchImpl, calls } = recordingFetch(() => response(503, 'busy'));
    const result = await sendNeatlogsPlan(configured(), plan, { fetch: fetchImpl, retryDelayMs: 0, maxAttempts: 3 });
    assert.equal(result.status, 'outcome_unknown');
    assert.equal(calls.length, 3);
    assert.deepEqual(calls[1].init.body, calls[0].init.body);
  });

  test('a timeout is an unknown outcome and a later success is still unconfirmed', async () => {
    const hang = (url, init) => new Promise((_, reject) => {
      init.signal.addEventListener('abort', () => reject(new Error('aborted')));
    });
    const timedOut = await sendNeatlogsPlan(configured(), plan, {
      fetch: hang, timeoutMs: 100, maxAttempts: 1
    });
    assert.equal(timedOut.status, 'outcome_unknown');
    assert.equal(timedOut.reasonCode, 'REQUEST_TIMEOUT');
    const { fetchImpl } = recordingFetch((url, init, count) => count === 1 ? hang(url, init) : response(200, null));
    const recovered = await sendNeatlogsPlan(configured(), plan, {
      fetch: fetchImpl, timeoutMs: 100, maxAttempts: 2, retryDelayMs: 0
    });
    assert.equal(recovered.status, 'receiver_accepted');
    assert.equal(recovered.deliveryConfirmed, false);

    const started = Date.now();
    const uncooperative = await sendNeatlogsPlan(configured(), plan, {
      fetch: () => new Promise(() => {}), timeoutMs: 100, maxAttempts: 1
    });
    assert.equal(uncooperative.status, 'outcome_unknown');
    assert.equal(uncooperative.reasonCode, 'REQUEST_TIMEOUT');
    assert.ok(Date.now() - started < 1_000, 'the caller deadline settles even if the injected fetch ignores abort');

    let bodyCancelled = false;
    const stalledBody = await sendNeatlogsPlan(configured(), plan, {
      fetch: async () => new Response(new ReadableStream({
        pull() {},
        cancel() { bodyCancelled = true; }
      }), { status: 200 }),
      timeoutMs: 100,
      maxAttempts: 1
    });
    assert.equal(stalledBody.status, 'outcome_unknown');
    assert.equal(stalledBody.reasonCode, 'REQUEST_TIMEOUT');
    assert.equal(bodyCancelled, true);
  });

  test('cancellation after a POST starts leaves the receiver outcome unknown', async () => {
    for (const phase of ['awaiting-response', 'streaming-response']) {
      const controller = new AbortController();
      let calls = 0;
      let bodyCancelled = false;
      const fetchImpl = async () => {
        calls += 1;
        if (phase === 'awaiting-response') {
          queueMicrotask(() => controller.abort());
          return new Promise(() => {});
        }
        return new Response(new ReadableStream({
          start() {
            setTimeout(() => controller.abort(), 0);
          },
          cancel() { bodyCancelled = true; }
        }), { status: 200 });
      };
      const result = await sendNeatlogsPlan(configured(), plan, {
        fetch: fetchImpl, signal: controller.signal, timeoutMs: 1_000, maxAttempts: 1
      });
      assert.equal(result.status, 'outcome_unknown', phase);
      assert.equal(result.reasonCode, 'ABORTED', phase);
      assert.equal(result.attempts, 1, phase);
      assert.equal(result.deliveryConfirmed, false, phase);
      assert.equal(calls, 1, phase);
      if (phase === 'streaming-response') assert.equal(bodyCancelled, true);
    }
  });

  test('honors bounded Retry-After on 429 and reports not_accepted', async () => {
    const delays = [];
    const { fetchImpl, calls } = recordingFetch(() => response(429, '', { 'retry-after': '1' }));
    const result = await sendNeatlogsPlan(configured(), plan, {
      fetch: fetchImpl, maxAttempts: 2, sleep: async delay => { delays.push(delay); return true; }
    });
    assert.equal(result.status, 'not_accepted');
    assert.equal(calls.length, 2);
    assert.deepEqual(delays, [1_000]);
  });

  test('honors date and 503 Retry-After minima, and stops rather than retrying early', async () => {
    const now = Date.parse('2026-01-01T00:00:00Z');
    const dateDelays = [];
    const dated = recordingFetch((url, init, count) => count === 1
      ? response(503, '', { 'retry-after': 'Thu, 01 Jan 2026 00:00:02 GMT' })
      : response(200, null));
    const dateResult = await sendNeatlogsPlan(configured(), plan, {
      fetch: dated.fetchImpl, now: () => now, maxAttempts: 2,
      sleep: async delay => { dateDelays.push(delay); return true; }
    });
    assert.equal(dateResult.status, 'receiver_accepted');
    assert.deepEqual(dateDelays, [2_000]);
    assert.equal(dated.calls.length, 2);

    for (const status of [429, 503]) {
      const { fetchImpl, calls } = recordingFetch(() => response(status, '', { 'retry-after': '11' }));
      const result = await sendNeatlogsPlan(configured(), plan, {
        fetch: fetchImpl, maxAttempts: 3, sleep: async () => { throw new Error('oversized minimum must not sleep'); }
      });
      assert.equal(result.status, status === 429 ? 'not_accepted' : 'outcome_unknown');
      assert.equal(result.reasonCode, 'RETRY_AFTER_EXCEEDS_BOUND');
      assert.equal(calls.length, 1);
    }
  });

  test('honors all HTTP-date forms and stops on arbitrarily large valid delay-seconds', async () => {
    const now = Date.parse('2026-01-01T00:00:00Z');
    const dates = [
      'Thu, 01 Jan 2026 00:00:02 GMT',
      'Thursday, 01-Jan-26 00:00:02 GMT',
      'Thu Jan  1 00:00:02 2026'
    ];
    for (const retryAfter of dates) {
      const delays = [];
      const { fetchImpl, calls } = recordingFetch((url, init, count) => (
        count === 1 ? response(503, '', { 'retry-after': retryAfter }) : response(200, null)
      ));
      const result = await sendNeatlogsPlan(configured(), plan, {
        fetch: fetchImpl, now: () => now, maxAttempts: 2,
        sleep: async delay => { delays.push(delay); return true; }
      });
      assert.equal(result.status, 'receiver_accepted', retryAfter);
      assert.deepEqual(delays, [2_000], retryAfter);
      assert.equal(calls.length, 2, retryAfter);
    }

    for (const retryAfter of [
      'Thu, 01 Jan 2026 00:00:60 GMT', // syntactically valid leap second
      'Wednesday, 01-Jan-76 00:00:00 GMT' // exactly 50 years ahead is still future
    ]) {
      const { fetchImpl, calls } = recordingFetch(() => response(429, '', { 'retry-after': retryAfter }));
      const result = await sendNeatlogsPlan(configured(), plan, {
        fetch: fetchImpl, now: () => now, maxAttempts: 2,
        sleep: async () => { throw new Error('a minimum above the local bound must not sleep'); }
      });
      assert.equal(result.status, 'not_accepted', retryAfter);
      assert.equal(result.reasonCode, 'RETRY_AFTER_EXCEEDS_BOUND', retryAfter);
      assert.equal(calls.length, 1, retryAfter);
    }

    const pastDateDelays = [];
    const pastDate = recordingFetch((url, init, count) => (
      count === 1 ? response(429, '', { 'retry-after': 'Saturday, 01-Jan-77 00:00:02 GMT' }) : response(200, null)
    ));
    const pastDateResult = await sendNeatlogsPlan(configured(), plan, {
      fetch: pastDate.fetchImpl, now: () => now, maxAttempts: 2,
      sleep: async delay => { pastDateDelays.push(delay); return true; }
    });
    assert.equal(pastDateResult.status, 'receiver_accepted');
    assert.deepEqual(pastDateDelays, [0], 'a date more than 50 years ahead is interpreted in the most recent past century');
    assert.equal(pastDate.calls.length, 2);

    const hugeDelay = '9'.repeat(309);
    for (const status of [429, 503]) {
      const { fetchImpl, calls } = recordingFetch(() => response(status, '', { 'retry-after': hugeDelay }));
      const result = await sendNeatlogsPlan(configured(), plan, {
        fetch: fetchImpl, maxAttempts: 3,
        sleep: async () => { throw new Error('an unbounded valid minimum must not sleep or retry early'); }
      });
      assert.equal(result.reasonCode, 'RETRY_AFTER_EXCEEDS_BOUND');
      assert.equal(result.status, status === 429 ? 'not_accepted' : 'outcome_unknown');
      assert.equal(calls.length, 1);
    }
  });

  test('malformed Retry-After uses the bounded local backoff', async () => {
    const delays = [];
    const { fetchImpl, calls } = recordingFetch((url, init, count) => (
      count === 1 ? response(429, '', { 'retry-after': 'not-a-date' }) : response(200, null)
    ));
    const result = await sendNeatlogsPlan(configured(), plan, {
      fetch: fetchImpl, retryDelayMs: 7, maxAttempts: 2,
      sleep: async delay => { delays.push(delay); return true; }
    });
    assert.equal(result.status, 'receiver_accepted');
    assert.deepEqual(delays, [7]);
    assert.equal(calls.length, 2);
  });

  test('cancelling a Retry-After wait stops before another send', async () => {
    const controller = new AbortController();
    const { fetchImpl, calls } = recordingFetch(() => response(429, '', { 'retry-after': '1' }));
    const result = await sendNeatlogsPlan(configured(), plan, {
      fetch: fetchImpl,
      signal: controller.signal,
      maxAttempts: 3,
      sleep: async () => { controller.abort(); return false; }
    });
    assert.equal(result.status, 'aborted');
    assert.equal(calls.length, 1);
  });

  test('an abort before sending makes no request', async () => {
    const controller = new AbortController();
    controller.abort();
    const { fetchImpl, calls } = recordingFetch(() => response(200));
    const result = await sendNeatlogsPlan(configured(), plan, { fetch: fetchImpl, signal: controller.signal });
    assert.equal(result.status, 'aborted');
    assert.equal(calls.length, 0);
  });
});

describe('readback', () => {
  test('is unavailable without a service-account token and project', async () => {
    const result = await readbackNeatlogsPlan(configured({ NEATLOGS_READ_TOKEN: '' }), plan);
    assert.equal(result.status, 'readback_unavailable');
    assert.equal(result.reasonCode, 'MISSING_READBACK_TOKEN');
  });

  test('verifies IDs, session, tags, span tree and input after finalization', async () => {
    const { fetchImpl, calls } = readbackServer({ finalizeAfter: 2 });
    const result = await readbackNeatlogsPlan(configured(), plan, { fetch: fetchImpl, pollIntervalMs: 0 });
    assert.equal(result.status, 'verified');
    assert.equal(result.traces.length, plan.traces.length);
    assert.ok(result.traces.every(trace => trace.markerStored));
    const first = calls[0];
    assert.equal(first.url, 'https://app.neatlogs.com/api/v1/public/project');
    assert.equal(first.init.headers.authorization, `Bearer ${READ_TOKEN}`);
    assert.equal(first.init.headers['x-project-id'], PROJECT_ID);
    assert.ok(!JSON.stringify(result).includes(READ_TOKEN));
  });

  test('pre-aborted and mid-readback cancellation make no later requests', async () => {
    const pre = new AbortController();
    pre.abort();
    const preFetch = recordingFetch(() => response(200, JSON.stringify({ success: true, data: { id: PROJECT_ID } })));
    const preResult = await readbackNeatlogsPlan(configured(), plan, {
      fetch: preFetch.fetchImpl, signal: pre.signal
    });
    assert.equal(preResult.status, 'readback_failed');
    assert.equal(preResult.reasonCode, 'READBACK_ABORTED');
    assert.equal(preFetch.calls.length, 0);

    const during = new AbortController();
    const midFetch = recordingFetch((url, init, count) => {
      if (count === 1) {
        during.abort();
        return response(200, JSON.stringify({ success: true, data: { id: PROJECT_ID } }));
      }
      return response(200, '{}');
    });
    const midResult = await readbackNeatlogsPlan(configured(), plan, {
      fetch: midFetch.fetchImpl, signal: during.signal
    });
    assert.equal(midResult.status, 'readback_failed');
    assert.equal(midResult.reasonCode, 'READBACK_ABORTED');
    assert.equal(midFetch.calls.length, 1);
  });

  test('cancellation after each readback projection stops before the next request', async () => {
    for (const abortAfterCall of [1, 2, 3, 4]) {
      const controller = new AbortController();
      const server = readbackServer();
      const fetchImpl = async (...args) => {
        const result = await server.fetchImpl(...args);
        if (server.calls.length === abortAfterCall) controller.abort();
        return result;
      };
      const result = await readbackNeatlogsPlan(configured(), plan, {
        fetch: fetchImpl, signal: controller.signal, pollIntervalMs: 0, maxPolls: 1
      });
      assert.equal(result.status, 'readback_failed', `abort after request ${abortAfterCall}`);
      assert.equal(result.reasonCode, 'READBACK_ABORTED', `abort after request ${abortAfterCall}`);
      assert.equal(server.calls.length, abortAfterCall, `abort after request ${abortAfterCall}`);
    }
  });

  test('cancellation during polling prevents another poll', async () => {
    const controller = new AbortController();
    const server = readbackServer({ finalizeAfter: 99 });
    const result = await readbackNeatlogsPlan(configured(), plan, {
      fetch: server.fetchImpl,
      signal: controller.signal,
      pollIntervalMs: 1,
      maxPolls: 3,
      sleep: async () => { controller.abort(); return false; }
    });
    assert.equal(result.status, 'readback_failed');
    assert.equal(result.reasonCode, 'READBACK_ABORTED');
    assert.equal(server.calls.length, 1 + plan.traces.length);
  });

  test('abort during a streamed body cancels the reader and stops polling', async () => {
    const controller = new AbortController();
    let cancelled = false;
    const fetchImpl = async () => {
      queueMicrotask(() => controller.abort());
      return new Response(new ReadableStream({
        pull() {},
        cancel() { cancelled = true; }
      }), { status: 200, headers: { 'content-type': 'application/json' } });
    };
    const result = await readbackNeatlogsPlan(configured(), plan, { fetch: fetchImpl, signal: controller.signal });
    assert.equal(result.status, 'readback_failed');
    assert.equal(result.reasonCode, 'READBACK_ABORTED');
    assert.equal(cancelled, true);
  });

  test('accepts a receiver that does not store the completion marker as a span', async () => {
    const { fetchImpl } = readbackServer({ spans: 'root-only' });
    const result = await readbackNeatlogsPlan(configured(), plan, { fetch: fetchImpl, pollIntervalMs: 0 });
    assert.equal(result.status, 'verified');
    assert.ok(result.traces.every(trace => trace.markerStored === false));
  });

  test('stays pending, never verified, while traces are not finalized', async () => {
    const { fetchImpl } = readbackServer({ finalizeAfter: 99 });
    const result = await readbackNeatlogsPlan(configured(), plan, {
      fetch: fetchImpl, pollIntervalMs: 0, maxPolls: 2
    });
    assert.equal(result.status, 'pending');
  });

  test('flags duplicate spans, missing tags, altered input and the wrong project', async () => {
    const duplicate = await readbackNeatlogsPlan(configured(), plan, {
      fetch: readbackServer({ spans: 'duplicate' }).fetchImpl, pollIntervalMs: 0
    });
    assert.equal(duplicate.status, 'mismatch');
    assert.ok(duplicate.traces[0].problems.includes('DUPLICATE_SPANS_OBSERVED'));
    const tags = await readbackNeatlogsPlan(configured(), plan, {
      fetch: readbackServer({ tags: [] }).fetchImpl, pollIntervalMs: 0
    });
    assert.ok(tags.traces[0].problems.includes('TAGS_MISSING'));
    const input = await readbackNeatlogsPlan(configured(), plan, {
      fetch: readbackServer({ inputFor: () => '{"changed":true}' }).fetchImpl, pollIntervalMs: 0
    });
    assert.ok(input.traces[0].problems.includes('INPUT_PAYLOAD_MISMATCH'));
    const project = await readbackNeatlogsPlan(configured(), plan, {
      fetch: readbackServer({ projectId: '00000000-0000-4000-8000-000000000002' }).fetchImpl, pollIntervalMs: 0
    });
    assert.equal(project.reasonCode, 'PROJECT_MISMATCH');
  });

  test('requires complete, correctly identified span and payload projections', async () => {
    const cases = [
      [readbackServer({ page: { hasMore: true, limit: 50, nextCursor: 'next-page' } }), 'SPANS_PAGE_INCOMPLETE'],
      [readbackServer({ page: { hasMore: false, limit: 50 } }), 'SPANS_PAGE_INCOMPLETE'],
      [readbackServer({ spansCount: 8 }), 'SPANS_COUNT_MISMATCH'],
      [readbackServer({ markerTraceId: 'f'.repeat(32) }), 'SPAN_IDENTITY_MISMATCH'],
      [readbackServer({ payloadTraceId: 'f'.repeat(32) }), 'PAYLOAD_IDENTITY_MISMATCH'],
      [readbackServer({ payloadSpanId: 'f'.repeat(16) }), 'PAYLOAD_IDENTITY_MISMATCH'],
      [readbackServer({ traceOverrides: { spansCount: 3 } }), 'SPANS_COUNT_MISMATCH'],
      [readbackServer({ inputFor: trace => trace.input.replace('{', '{"schema":"hidden",') }), 'INPUT_PAYLOAD_MISMATCH']
    ];
    for (const [server, expectedProblem] of cases) {
      const result = await readbackNeatlogsPlan(configured(), plan, {
        fetch: server.fetchImpl, pollIntervalMs: 0, maxPolls: 1
      });
      assert.equal(result.status, 'mismatch');
      assert.ok(result.traces[0].problems.includes(expectedProblem), expectedProblem);
    }
  });

  test('keeps untrusted remote display metadata out of evidence', async () => {
    const privateValues = [
      'REMOTE_PROJECT_NAME_PRIVATE_7dd',
      'REMOTE_WORKFLOW_NAME_PRIVATE_9ac',
      'REMOTE_TAG_PRIVATE_customer@example.invalid',
      'REMOTE_TAG_CREDENTIAL_FRAGMENT_sk-synthetic-private'
    ];
    const result = await readbackNeatlogsPlan(configured(), plan, {
      fetch: readbackServer({
        projectName: privateValues[0],
        workflowName: privateValues[1],
        remoteTags: [privateValues[2], privateValues[3]]
      }).fetchImpl,
      pollIntervalMs: 0,
      maxPolls: 1
    });
    for (const rendered of [JSON.stringify(result), inspect(result)]) {
      for (const privateValue of privateValues) assert.ok(!rendered.includes(privateValue));
    }
    assert.equal(result.project.name, undefined);
    assert.ok(result.traces[0].problems.includes('WORKFLOW_NAME_MISMATCH'));
  });

  test('treats 409 as conflict/projection pending, never as bounded absence', async () => {
    const traceConflict = readbackServer({ traceStatus: 409 });
    const conflict = await readbackNeatlogsPlan(configured(), plan, {
      fetch: traceConflict.fetchImpl, pollIntervalMs: 0, maxPolls: 2
    });
    assert.equal(conflict.status, 'pending');
    assert.equal(conflict.traces[0].status, 'pending');
    assert.ok(conflict.traces[0].problems.includes('TRACE_CONFLICT'));
    assert.deepEqual(recoverableTraceIds(conflict), []);
    assert.equal(traceConflict.calls.length, 1 + 2 * plan.traces.length);

    const oneTracePlan = Object.freeze({ ...plan, traces: Object.freeze([plan.traces[0]]) });
    const spanConflict = readbackServer({ spansStatus: 409 });
    const spans = await readbackNeatlogsPlan(configured(), oneTracePlan, {
      fetch: spanConflict.fetchImpl, pollIntervalMs: 0, maxPolls: 1
    });
    assert.equal(spans.traces[0].status, 'pending');
    assert.ok(spans.traces[0].problems.includes('PROJECTION_NOT_READY'));
    assert.equal(spanConflict.calls.length, 3);

    const payloadConflict = readbackServer({ payloadStatus: 409 });
    const payload = await readbackNeatlogsPlan(configured(), oneTracePlan, {
      fetch: payloadConflict.fetchImpl, pollIntervalMs: 0, maxPolls: 1
    });
    assert.equal(payload.traces[0].status, 'pending');
    assert.ok(payload.traces[0].problems.includes('PROJECTION_NOT_READY'));
    assert.equal(payloadConflict.calls.length, 4);
  });

  test('records finite 404 absence without claiming permanent loss', async () => {
    const dropped = plan.traces[0].traceId;
    const result = await readbackNeatlogsPlan(configured(), plan, {
      fetch: readbackServer({ droppedIds: [dropped] }).fetchImpl, pollIntervalMs: 0, maxPolls: 2
    });
    const missing = result.traces.find(trace => trace.traceId === dropped);
    assert.equal(missing.status, 'missing');
    assert.equal(missing.observationWindowPolls, 2);
    assert.equal(missing.permanentLossEstablished, false);
    assert.deepEqual(recoverableTraceIds(result), [dropped]);
  });

  test('does not regenerate a bounded-missing trace while an earlier generation is visible', async () => {
    const earlier = plan.traces[0];
    const regenerated = nextNeatlogsGeneration(plan, [earlier.traceId]);
    const result = await readbackNeatlogsPlan(configured(), regenerated, {
      fetch: readbackServer({
        served: regenerated,
        droppedIds: [regenerated.traces[0].traceId],
        alsoStored: [earlier.traceId]
      }).fetchImpl,
      pollIntervalMs: 0,
      maxPolls: 1
    });
    assert.equal(result.status, 'pending');
    assert.equal(result.traces[0].status, 'pending');
    assert.ok(result.traces[0].problems.includes('PRIOR_GENERATION_FOUND'));
    assert.deepEqual([...result.traces[0].priorGenerationTraceIds], [earlier.traceId]);
    assert.deepEqual(recoverableTraceIds(result), []);
  });

  test('does not treat a mismatched prior-trace response as proof of an earlier generation', async () => {
    const earlier = plan.traces[0];
    const regenerated = nextNeatlogsGeneration(plan, [earlier.traceId]);
    const server = readbackServer({
      served: regenerated,
      droppedIds: [regenerated.traces[0].traceId],
      alsoStored: [earlier.traceId],
      alsoStoredTraceId: 'f'.repeat(32)
    });
    const result = await readbackNeatlogsPlan(configured(), regenerated, {
      fetch: server.fetchImpl,
      pollIntervalMs: 0,
      maxPolls: 1
    });
    assert.equal(result.status, 'pending');
    assert.equal(result.traces[0].status, 'pending');
    assert.ok(result.traces[0].problems.includes('PRIOR_GENERATION_IDENTITY_MISMATCH'));
    assert.deepEqual([...result.traces[0].priorGenerationTraceIds], []);
    assert.deepEqual(recoverableTraceIds(result), []);
  });

  test('does not count a merely pending prior generation as a duplicate delivery', async () => {
    const earlier = plan.traces[0];
    const regenerated = nextNeatlogsGeneration(plan, [earlier.traceId]);
    const result = await readbackNeatlogsPlan(configured(), regenerated, {
      fetch: readbackServer({
        served: regenerated,
        alsoStored: [earlier.traceId],
        alsoStoredFinalizationStatus: 'pending'
      }).fetchImpl,
      pollIntervalMs: 0,
      maxPolls: 1
    });
    assert.equal(result.status, 'verified');
    assert.deepEqual([...result.traces[0].priorGenerationTraceIds], [earlier.traceId]);
    assert.deepEqual([...result.traces[0].duplicateTraceIds], []);
    assert.equal(result.duplicateEventDeliveries, 0);
  });

  test('readback Retry-After honors server minima and stops above the local bound', async () => {
    const delays = [];
    let calls = 0;
    const fetchImpl = async url => {
      calls += 1;
      if (calls === 1) return response(503, '', { 'retry-after': '2' });
      if (calls === 2) return response(200, JSON.stringify({ success: true, data: { id: PROJECT_ID } }));
      return response(404, '{}');
    };
    const short = await readbackNeatlogsPlan(configured(), plan, {
      fetch: fetchImpl, pollIntervalMs: 0, maxPolls: 1,
      sleep: async delay => { delays.push(delay); return true; }
    });
    assert.equal(short.status, 'missing');
    assert.deepEqual(delays, [2_000]);

    calls = 0;
    const longFetch = async () => {
      calls += 1;
      return calls === 1
        ? response(200, JSON.stringify({ success: true, data: { id: PROJECT_ID } }))
        : response(429, '', { 'retry-after': '11' });
    };
    const long = await readbackNeatlogsPlan(configured(), plan, {
      fetch: longFetch, pollIntervalMs: 0, sleep: async () => { throw new Error('must not sleep early'); }
    });
    assert.equal(long.status, 'readback_failed');
    assert.equal(long.reasonCode, 'READBACK_RETRY_AFTER_EXCEEDS_BOUND');
    assert.equal(calls, 2);

    const now = Date.parse('2026-01-01T00:00:00Z');
    const dateDelays = [];
    let dateCalls = 0;
    const dateFetch = async () => {
      dateCalls += 1;
      if (dateCalls === 1) return response(200, JSON.stringify({ success: true, data: { id: PROJECT_ID } }));
      if (dateCalls === 2) return response(503, '', { 'retry-after': 'Thu, 01 Jan 2026 00:00:03 GMT' });
      return response(404, '{}');
    };
    const dated = await readbackNeatlogsPlan(configured(), plan, {
      fetch: dateFetch, now: () => now, pollIntervalMs: 0, maxPolls: 1,
      sleep: async delay => { dateDelays.push(delay); return true; }
    });
    assert.equal(dated.status, 'missing');
    assert.deepEqual(dateDelays, [3_000]);

    for (const retryAfter of [
      'Thu, 01 Jan 2026 00:00:02 GMT',
      'Thursday, 01-Jan-26 00:00:02 GMT',
      'Thu Jan  1 00:00:02 2026'
    ]) {
      let dateAttempt = 0;
      const delaysForDate = [];
      const dateFetchForFormat = async url => {
        const path = new URL(url).pathname;
        if (path.endsWith('/project')) {
          return response(200, JSON.stringify({ success: true, data: { id: PROJECT_ID } }));
        }
        dateAttempt += 1;
        return dateAttempt === 1
          ? response(503, '', { 'retry-after': retryAfter })
          : response(404, '{}');
      };
      const dateResultForFormat = await readbackNeatlogsPlan(configured(), plan, {
        fetch: dateFetchForFormat, now: () => now, pollIntervalMs: 0, maxPolls: 1,
        sleep: async delay => { delaysForDate.push(delay); return true; }
      });
      assert.equal(dateResultForFormat.status, 'missing', retryAfter);
      assert.deepEqual(delaysForDate, [2_000], retryAfter);
      assert.equal(dateAttempt, plan.traces.length + 1, retryAfter);
    }

    let boundaryAttempt = 0;
    const boundaryFetch = async url => {
      if (new URL(url).pathname.endsWith('/project')) {
        return response(200, JSON.stringify({ success: true, data: { id: PROJECT_ID } }));
      }
      boundaryAttempt += 1;
      return response(503, '', { 'retry-after': 'Thu, 01 Jan 2026 00:00:60 GMT' });
    };
    const leapSecondRead = await readbackNeatlogsPlan(configured(), plan, {
      fetch: boundaryFetch, now: () => now, pollIntervalMs: 0,
      sleep: async () => { throw new Error('a leap-second minimum must not sleep or retry early'); }
    });
    assert.equal(leapSecondRead.status, 'readback_failed');
    assert.equal(leapSecondRead.reasonCode, 'READBACK_RETRY_AFTER_EXCEEDS_BOUND');
    assert.equal(boundaryAttempt, 1);

    let hugeReadAttempts = 0;
    const hugeReadFetch = async url => {
      if (new URL(url).pathname.endsWith('/project')) {
        return response(200, JSON.stringify({ success: true, data: { id: PROJECT_ID } }));
      }
      hugeReadAttempts += 1;
      return response(429, '', { 'retry-after': '9'.repeat(309) });
    };
    const hugeRead = await readbackNeatlogsPlan(configured(), plan, {
      fetch: hugeReadFetch, pollIntervalMs: 0,
      sleep: async () => { throw new Error('an unbounded valid minimum must not sleep or retry early'); }
    });
    assert.equal(hugeRead.status, 'readback_failed');
    assert.equal(hugeRead.reasonCode, 'READBACK_RETRY_AFTER_EXCEEDS_BOUND');
    assert.equal(hugeReadAttempts, 1);

    const fallbackDelays = [];
    let fallbackCalls = 0;
    const fallbackFetch = async () => {
      fallbackCalls += 1;
      if (fallbackCalls === 1) return response(200, JSON.stringify({ success: true, data: { id: PROJECT_ID } }));
      if (fallbackCalls === 2) return response(429, '', { 'retry-after': 'malformed' });
      return response(404, '{}');
    };
    const fallback = await readbackNeatlogsPlan(configured(), plan, {
      fetch: fallbackFetch, pollIntervalMs: 0, maxPolls: 1,
      sleep: async delay => { fallbackDelays.push(delay); return true; }
    });
    assert.equal(fallback.status, 'missing');
    assert.deepEqual(fallbackDelays, [1_000]);

    const controller = new AbortController();
    const cancelledCalls = [];
    const cancelledRead = await readbackNeatlogsPlan(configured(), plan, {
      signal: controller.signal,
      maxPolls: 1,
      fetch: async url => {
        cancelledCalls.push(String(url));
        if (cancelledCalls.length === 1) {
          return response(200, JSON.stringify({ success: true, data: { id: PROJECT_ID } }));
        }
        return response(503, '', { 'retry-after': '1' });
      },
      sleep: async () => { controller.abort(); return false; }
    });
    assert.equal(cancelledRead.status, 'readback_failed');
    assert.equal(cancelledRead.reasonCode, 'READBACK_ABORTED');
    assert.equal(cancelledCalls.length, 2);
  });

  test('reports never-found traces as missing, not pending', async () => {
    const dropped = plan.traces[2].traceId;
    const result = await readbackNeatlogsPlan(configured(), plan, {
      fetch: readbackServer({ droppedIds: [dropped] }).fetchImpl, pollIntervalMs: 0, maxPolls: 2
    });
    assert.equal(result.status, 'missing');
    assert.equal(result.reasonCode, 'NOT_FOUND_AFTER_POLLS');
    assert.deepEqual(result.traces.filter(trace => trace.status === 'missing').map(trace => trace.traceId), [dropped]);
    assert.equal(result.traces.filter(trace => trace.status === 'verified').length, plan.traces.length - 1);
  });

  test('verifies a regenerated trace and reports an event stored under two generations', async () => {
    const lost = plan.traces[2];
    const regenerated = nextNeatlogsGeneration(plan, [lost.traceId]);
    const clean = await readbackNeatlogsPlan(configured(), regenerated, {
      fetch: readbackServer({ served: regenerated }).fetchImpl, pollIntervalMs: 0
    });
    assert.equal(clean.status, 'verified');
    assert.equal(clean.duplicateEventDeliveries, 0);
    const doubled = await readbackNeatlogsPlan(configured(), regenerated, {
      fetch: readbackServer({ served: regenerated, alsoStored: [lost.traceId] }).fetchImpl, pollIntervalMs: 0
    });
    assert.equal(doubled.status, 'verified');
    assert.equal(doubled.duplicateEventDeliveries, 1);
    assert.deepEqual([...doubled.traces[0].duplicateTraceIds], [lost.traceId]);
  });

  test('stops on authorization failure', async () => {
    const { fetchImpl, calls } = recordingFetch(() => response(403, '{}'));
    const result = await readbackNeatlogsPlan(configured(), plan, { fetch: fetchImpl });
    assert.equal(result.status, 'readback_failed');
    assert.equal(result.reasonCode, 'READBACK_UNAUTHORIZED');
    assert.equal(calls.length, 1);
  });
});
