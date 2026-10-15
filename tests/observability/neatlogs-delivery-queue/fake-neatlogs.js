import { createRequire } from 'node:module';
import { resolve } from 'node:path';

// An offline stand-in for Neatlogs: OTLP/HTTP ingest plus the Public API
// routes the readback uses. It decodes the real protobuf body, stores root
// spans like the receiver does (the completion marker is not stored), and
// can simulate a configured lost trace for deterministic queue tests. That
// synthetic behavior is a fixture choice and does not establish receiver
// behavior or permanent loss.

const require = createRequire(import.meta.url);
const otlpRoot = require(resolve('node_modules/@opentelemetry/otlp-transformer/build/src/generated/root.js'));
const ExportRequest = otlpRoot.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
const ExportResponse = otlpRoot.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;

export const FAKE_INGEST_KEY = 'synthetic-ingest-key-0123456789abcdef';
export const FAKE_READ_TOKEN = 'synthetic-readback-token-0123456789abcdef';
export const FAKE_PROJECT_ID = '00000000-0000-4000-8000-000000000001';

const hex = bytes => Buffer.from(bytes).toString('hex');

function attributeMap(list = []) {
  const out = {};
  for (const { key, value } of list) {
    out[key] = value.stringValue ?? value.boolValue ?? (value.intValue == null ? undefined : Number(value.intValue));
  }
  return out;
}

export function fakeNeatlogs({
  loseFirstDelivery = [],
  ingestStatus = () => 200,
  readbackStatus = () => 200,
  finalizationStatus = () => 'finalized',
  beforeFetch = async () => {}
} = {}) {
  const stored = new Map();
  const seen = new Set();
  const lost = new Set(loseFirstDelivery);
  const bodies = [];
  const calls = [];
  let ingestOrdinal = 0;
  const json = (status, data) => new Response(JSON.stringify({ success: true, requestId: 'req_fake', data }), {
    status, headers: { 'content-type': 'application/json' }
  });
  const notFound = () => new Response('{}', { status: 404, headers: { 'content-type': 'application/problem+json' } });

  async function fetchImpl(url, init) {
    const parsed = new URL(String(url));
    const call = {
      method: init.method,
      path: parsed.pathname,
      headers: init.headers,
      ingestOrdinal: parsed.pathname === '/v1/traces' ? ++ingestOrdinal : null
    };
    calls.push(call);
    const body = parsed.pathname === '/v1/traces' ? Buffer.from(init.body) : null;
    if (body) bodies.push(body);
    await beforeFetch(Object.freeze({ ...call, body }));
    if (parsed.pathname === '/v1/traces') {
      const configured = ingestStatus(call.ingestOrdinal);
      const status = typeof configured === 'number' ? configured : configured.status;
      if (status !== 200) return new Response('', {
        status,
        headers: configured?.retryAfter ? { 'retry-after': String(configured.retryAfter) } : undefined
      });
      const request = ExportRequest.decode(body);
      for (const resourceSpans of request.resourceSpans) {
        const resource = attributeMap(resourceSpans.resource.attributes);
        for (const scopeSpans of resourceSpans.scopeSpans) {
          for (const span of scopeSpans.spans) {
            const traceId = hex(span.traceId);
            const attributes = attributeMap(span.attributes);
            if (attributes['neatlogs.trace.complete']) continue;
            if (seen.has(traceId)) continue;
            seen.add(traceId);
            if (lost.has(traceId)) continue;
            stored.set(traceId, {
              traceId,
              spanId: hex(span.spanId),
              spanName: span.name,
              startTimeUnixNano: String(span.startTimeUnixNano),
              endTimeUnixNano: String(span.endTimeUnixNano),
              sessionId: attributes['neatlogs.session.id'],
              workflowName: resource['neatlogs.workflow_name'],
              tags: (attributes['neatlogs.tags'] ?? '').split(',').filter(Boolean),
              input: attributes['neatlogs.workflow.input'],
              attributes
            });
          }
        }
      }
      return new Response(ExportResponse.encode({}).finish(), {
        status: 200, headers: { 'content-type': 'application/x-protobuf' }
      });
    }
    const path = parsed.pathname.replace('/api/v1/public', '');
    const configuredReadback = readbackStatus(parsed.pathname, call);
    const readbackHttpStatus = typeof configuredReadback === 'number'
      ? configuredReadback : configuredReadback.status;
    if (readbackHttpStatus !== 200) {
      return new Response('', {
        status: readbackHttpStatus,
        headers: configuredReadback?.retryAfter
          ? { 'retry-after': String(configuredReadback.retryAfter) } : undefined
      });
    }
    if (path === '/project') return json(200, { id: FAKE_PROJECT_ID, name: 'Synthetic project' });
    const match = /^\/traces\/([0-9a-f]{32})(\/spans(?:\/([0-9a-f]{16})\/payload)?)?$/.exec(path);
    const trace = match ? stored.get(match[1]) : null;
    if (!trace) return notFound();
    if (!match[2]) {
      return json(200, {
        traceId: trace.traceId, sessionId: trace.sessionId, workflowName: trace.workflowName,
        tags: trace.tags, spansCount: 1, finalizationStatus: finalizationStatus(trace.traceId, call)
      });
    }
    if (!match[3]) {
      return json(200, {
        spans: [{ traceId: trace.traceId, spanId: trace.spanId, parentSpanId: null, spanName: trace.spanName }],
        page: { hasMore: false, limit: 50, nextCursor: null }
      });
    }
    return json(200, {
      traceId: trace.traceId,
      spanId: trace.spanId,
      fields: { input: { available: true, truncated: false, value: trace.input } }
    });
  }

  return { fetchImpl, stored, bodies, calls };
}
