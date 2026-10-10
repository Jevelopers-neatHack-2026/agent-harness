import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { test } from 'node:test';
import { sourceRecordFromNeatlogsAttributes } from '../../../src/observability/private/neatlogs-sdk-mapper.js';
import {
  createProjectedStoreEvents,
  createTemporaryDirectory,
  networkGuardMode,
  readJson,
  spawnNodeFixture,
  SYNTHETIC_CANARIES,
  writeRecords
} from './helpers.js';

test('real Neatlogs 1.1.28 maps frozen synthetic durable events into a sanitized local SDK capture', async t => {
  const temporary = createTemporaryDirectory();
  t.after(() => temporary.cleanup());
  const { store, records, rawEventCount } = createProjectedStoreEvents(join(temporary.directory, 'workflow.sqlite'));
  t.after(() => store.close());
  assert.equal(store.getTelemetryCursor(), 0);
  assert.equal(records.length, rawEventCount);
  assert.ok(rawEventCount >= 3);

  const recordsPath = writeRecords(temporary.directory, records);
  const artifactPath = join(process.cwd(), '.runtime-artifacts', 'neatlogs-sdk-local-capture.json');
  const guardMode = networkGuardMode();
  const child = spawnNodeFixture('real-capture-child.mjs', [recordsPath, artifactPath, guardMode], {
    timeoutMs: 20_000
  });
  const execution = await child.done;
  assert.equal(execution.code, 0, execution.stderr);
  for (const canary of SYNTHETIC_CANARIES) {
    assert.equal(execution.stdout.includes(canary), false);
    assert.equal(execution.stderr.includes(canary), false);
  }
  const summary = JSON.parse(execution.stdout.trim());
  assert.equal(summary.status, 'captured_local');
  assert.equal(summary.testMode, 'offline-synthetic-real-sdk');
  assert.equal(summary.nodeVersion, 'v22.23.3');
  assert.equal(summary.sdkVersion, '1.1.28');
  assert.equal(summary.sourceRecordCount, records.length);
  assert.equal(summary.rootSpanCount, records.length);
  assert.equal(summary.completionMarkerSpanCount, records.length);
  assert.equal(summary.recursivelyFrozen, true);
  assert.equal(summary.nestedCollectionMutationsRejected, true);
  assert.equal(summary.serializedEnvelopeUnchangedAfterMutationAttempts, true);
  assert.equal(summary.networkGuard, guardMode);
  assert.equal(existsSync(artifactPath), true);

  const artifactBytes = readFileSync(artifactPath);
  const artifactHash = createHash('sha256').update(artifactBytes).digest('hex');
  assert.equal(summary.artifactSha256, artifactHash);
  const artifact = readJson(artifactPath);
  assert.equal(artifact.evidenceSchema, 'agent-harness.neatlogs.local-evidence.v1');
  assert.equal(artifact.testMode, 'offline-synthetic-real-sdk');
  assert.equal(artifact.nodeVersion, 'v22.23.3');
  assert.equal(artifact.networkGuard, guardMode);
  assert.equal(artifact.sdk.version, '1.1.28');
  assert.deepEqual(artifact.claims, {
    actualSdkLocalMapping: true,
    remoteIngestionVerified: false,
    sponsorAcceptance: false,
    entireEnabled: false,
    telemetryCursorAdvanced: false,
    financeAcknowledgement: false,
    liveServiceAccess: false
  });

  const envelope = artifact.capturedEnvelope;
  assert.equal(envelope.schema, 'agent-harness.neatlogs.local-capture.v1');
  assert.equal(envelope.status, undefined);
  assert.equal(envelope.accepted, undefined);
  assert.equal(envelope.remoteIngestionVerified, false);
  assert.equal(envelope.sponsorAcceptance, false);
  assert.equal(envelope.entireEnabled, false);
  assert.equal(envelope.telemetryCursorAdvanced, false);
  assert.equal(envelope.financeAcknowledgement, false);
  assert.equal(envelope.sourceRecordCount, records.length);
  assert.equal(envelope.rootSpanCount, records.length);
  assert.equal(envelope.completionMarkerSpanCount, records.length);
  assert.equal(envelope.spans.length, records.length * 2);

  const roots = envelope.spans.filter(span => span.name === 'agent_harness.workflow_event');
  const markers = envelope.spans.filter(span => span.name === 'neatlogs.trace.complete');
  assert.equal(roots.length, records.length);
  assert.equal(markers.length, records.length);
  const reconstructed = roots
    .map(span => sourceRecordFromNeatlogsAttributes(span.attributes))
    .sort((left, right) => left.sequence - right.sequence);
  assert.deepEqual(reconstructed.map(record => ({ ...record })), records.map(record => ({ ...record })));
  const rootsByTrace = new Map(roots.map(span => [span.trace_id, span]));
  assert.equal(rootsByTrace.size, roots.length);
  for (const marker of markers) {
    assert.ok(rootsByTrace.has(marker.trace_id));
    assert.equal(marker.span_id === rootsByTrace.get(marker.trace_id).span_id, false);
  }

  const serialized = JSON.stringify(artifact);
  for (const canary of SYNTHETIC_CANARIES) assert.equal(serialized.includes(canary), false);
  assert.equal(serialized.includes('input.value'), false);
  assert.equal(serialized.includes('output.value'), false);
  assert.equal(serialized.includes('exception.message'), false);
  assert.equal(serialized.includes('neatlogs.verification.marker'), false);

  const packageGraph = artifact.lockedDependencyGraph;
  assert.equal(packageGraph.length, 57);
  for (const dependency of packageGraph) {
    assert.equal(typeof dependency.name, 'string');
    assert.equal(typeof dependency.version, 'string');
    assert.match(dependency.integrity, /^sha512-/);
    assert.equal(typeof dependency.license, 'string');
  }
  assert.equal(packageGraph.find(entry => entry.name === 'neatlogs').version, '1.1.28');
  assert.equal(packageGraph.find(entry => entry.name === 'neatlogs').license, 'MIT');
  assert.equal(packageGraph.find(entry => entry.path === 'node_modules/@opentelemetry/sdk-trace-base').version, '1.30.1');
  assert.equal(packageGraph.find(entry => entry.path === 'node_modules/@opentelemetry/otlp-exporter-base').version, '0.57.2');
  assert.equal(Object.keys(artifact.sourceHashes).length, 24);

  assert.equal(store.getTelemetryCursor(), 0);
  assert.equal(store.readEventPage({ afterSequence: 0, limit: 32 }).events.length, rawEventCount);
});
