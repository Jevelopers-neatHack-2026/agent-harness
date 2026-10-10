import { createHash } from 'node:crypto';
import { writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { DurableWorkflowStore } from '@jevelopers/agent-harness-runtime';
import { createNeatlogsSdkCapture } from '../../../../src/observability/private/neatlogs-sdk-capture.js';
import { projectStoreEvents, writeJson } from '../helpers.js';

const [databasePathInput, phase, readyPathInput, outputPathInput] = process.argv.slice(2);
const databasePath = resolve(databasePathInput);
const readyPath = resolve(readyPathInput);
const outputPath = resolve(outputPathInput);
const store = DurableWorkflowStore.open({ databasePath });
const { records, rawEventCount } = projectStoreEvents(store);
const keepAlive = () => setInterval(() => {}, 60_000);

if (phase === 'before') {
  writeJson(readyPath, { phase, rawEventCount, projectedRecordCount: records.length });
  keepAlive();
} else if (phase === 'during') {
  const capture = createNeatlogsSdkCapture({ timeoutMs: 500, exporterDelayMs: 150 });
  const pending = capture.prepare(records);
  const deadline = Date.now() + 3_000;
  while (capture.diagnostics().exporter.pendingCallbacks === 0) {
    if (Date.now() >= deadline) throw new Error('The local SDK export callback did not reach its kill barrier.');
    await new Promise(resolveWait => setTimeout(resolveWait, 1));
  }
  writeJson(readyPath, {
    phase,
    rawEventCount,
    projectedRecordCount: records.length,
    callbackPending: capture.diagnostics().exporter.pendingCallbacks
  });
  void pending;
  keepAlive();
} else if (phase === 'after' || phase === 'replay') {
  const capture = createNeatlogsSdkCapture();
  const result = await capture.prepare(records);
  const close = await capture.close();
  if (result.status !== 'captured_local' || close.sdkShutdownCompleted !== true) {
    store.close();
    process.exitCode = 2;
    process.stdout.write(JSON.stringify({ status: result.status, reasonCode: result.reasonCode ?? close.status }));
  } else {
    const artifact = Object.freeze({
      evidenceSchema: 'agent-harness.neatlogs.sqlite-restart.v1',
      testMode: 'offline-synthetic-real-sdk',
      phase,
      sourceRecordCount: records.length,
      rawEventCount,
      capturedEnvelope: result.envelope,
      claims: Object.freeze({
        actualSdkLocalMapping: true,
        remoteIngestionVerified: false,
        sponsorAcceptance: false,
        entireEnabled: false,
        telemetryCursorAdvanced: false,
        financeAcknowledgement: false,
        liveServiceAccess: false
      })
    });
    const bytes = `${JSON.stringify(artifact, null, 2)}\n`;
    writeFileSync(outputPath, bytes, { mode: 0o600 });
    const artifactSha256 = createHash('sha256').update(bytes).digest('hex');
    if (phase === 'after') {
      writeJson(readyPath, { phase, sourceRecordCount: records.length, artifactSha256 });
      keepAlive();
    } else {
      store.close();
      process.stdout.write(JSON.stringify({
        phase,
        sourceRecordCount: records.length,
        rootSpanCount: result.envelope.rootSpanCount,
        completionMarkerSpanCount: result.envelope.completionMarkerSpanCount,
        artifactSha256
      }));
    }
  }
} else {
  store.close();
  process.exitCode = 2;
  process.stdout.write(JSON.stringify({ status: 'failed', reasonCode: 'INVALID_FIXTURE_PHASE' }));
}
