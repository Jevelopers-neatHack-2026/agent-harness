import { readFileSync } from 'node:fs';
import { createNeatlogsSdkCapture } from '../../../../src/observability/private/neatlogs-sdk-capture.js';

const records = JSON.parse(readFileSync(process.argv[2], 'utf8'));
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
async function waitForCallback(capture) {
  const deadline = Date.now() + 3_000;
  while (capture.diagnostics().exporter.pendingCallbacks === 0) {
    if (Date.now() >= deadline) throw new Error('The local export callback did not reach its barrier.');
    await pause(1);
  }
}

const matrix = [];
for (const count of [31, 32]) {
  for (const delay of [0, 150]) {
    for (const mode of ['success', 'failure']) {
      const capture = createNeatlogsSdkCapture({
        timeoutMs: 500,
        exporterDelayMs: delay,
        exporterMode: mode
      });
      const result = await capture.prepare(records.slice(0, count));
      const atResult = capture.diagnostics();
      await pause(180);
      const afterSettle = capture.diagnostics();
      const closed = await capture.close();
      const afterClose = capture.diagnostics();
      await pause(10);
      const afterCloseSettle = capture.diagnostics();
      matrix.push({
        count,
        delay,
        mode,
        result: {
          status: result.status,
          reasonCode: result.reasonCode,
          generation: result.generation,
          sourceRecordCount: result.envelope?.sourceRecordCount,
          roots: result.envelope?.rootSpanCount,
          markers: result.envelope?.completionMarkerSpanCount,
          spans: result.envelope?.spans.length,
          counters: result.counters
        },
        atResult,
        afterSettle,
        closed,
        afterClose,
        afterCloseSettle
      });
    }
  }
}

const changedRecords = records.map((record, index) => ({
  ...record,
  eventId: `tok1_${(0x100 + index).toString(16).padStart(64, '0')}`
}));

const abortCapture = createNeatlogsSdkCapture({ timeoutMs: 500, exporterDelayMs: 150 });
const controller = new AbortController();
const abortPromise = abortCapture.prepare(records, { signal: controller.signal });
await waitForCallback(abortCapture);
const abortPending = abortCapture.diagnostics();
controller.abort();
const busyDuringAbort = await abortCapture.prepare(records);
const aborted = await abortPromise;
const abortDrained = abortCapture.diagnostics();
const changedRetry = await abortCapture.prepare(changedRecords);
const changedRetryDiagnostics = abortCapture.diagnostics();
await pause(180);
const changedRetryAfterSettle = abortCapture.diagnostics();
const abortClose = await abortCapture.close();
const abortClosedDiagnostics = abortCapture.diagnostics();

const timeoutCapture = createNeatlogsSdkCapture({ timeoutMs: 10, exporterDelayMs: 150 });
const timeoutPromise = timeoutCapture.prepare(records);
await waitForCallback(timeoutCapture);
await pause(20);
const busyDuringTimeout = await timeoutCapture.prepare(records);
const timedOut = await timeoutPromise;
const timeoutDrained = timeoutCapture.diagnostics();
const timeoutRetry = await timeoutCapture.prepare(changedRecords);
const timeoutRetryDiagnostics = timeoutCapture.diagnostics();
await pause(180);
const timeoutRetryAfterSettle = timeoutCapture.diagnostics();
const timeoutClose = await timeoutCapture.close();
const timeoutClosedDiagnostics = timeoutCapture.diagnostics();
await pause(10);
const timeoutAfterCloseSettle = timeoutCapture.diagnostics();

const closeCapture = createNeatlogsSdkCapture({ timeoutMs: 500, exporterDelayMs: 150 });
const closePromisePrepare = closeCapture.prepare(records);
await waitForCallback(closeCapture);
const closePending = closeCapture.diagnostics();
const closePromise = closeCapture.close();
const closePromiseAgain = closeCapture.close();
const closedRequest = await closePromisePrepare;
const closeResult = await closePromise;
const closeDiagnostics = closeCapture.diagnostics();
await pause(180);
const closeAfterSettle = closeCapture.diagnostics();

console.log(JSON.stringify({
  testMode: 'offline-synthetic-real-sdk',
  matrix,
  barriers: {
    abort: {
      pending: abortPending,
      busyReason: busyDuringAbort.reasonCode,
      result: { status: aborted.status, reasonCode: aborted.reasonCode, generation: aborted.generation },
      drained: abortDrained,
      retry: {
        status: changedRetry.status,
        reasonCode: changedRetry.reasonCode,
        generation: changedRetry.generation,
        sourceRecordCount: changedRetry.envelope?.sourceRecordCount,
        roots: changedRetry.envelope?.rootSpanCount,
        markers: changedRetry.envelope?.completionMarkerSpanCount
      },
      retryDiagnostics: changedRetryDiagnostics,
      retryAfterSettle: changedRetryAfterSettle,
      close: abortClose,
      closedDiagnostics: abortClosedDiagnostics
    },
    timeout: {
      busyReason: busyDuringTimeout.reasonCode,
      result: { status: timedOut.status, reasonCode: timedOut.reasonCode, generation: timedOut.generation },
      drained: timeoutDrained,
      retry: {
        status: timeoutRetry.status,
        reasonCode: timeoutRetry.reasonCode,
        generation: timeoutRetry.generation
      },
      retryDiagnostics: timeoutRetryDiagnostics,
      retryAfterSettle: timeoutRetryAfterSettle,
      close: timeoutClose,
      closedDiagnostics: timeoutClosedDiagnostics,
      afterCloseSettle: timeoutAfterCloseSettle
    },
    close: {
      pending: closePending,
      samePromise: closePromise === closePromiseAgain,
      result: { status: closedRequest.status, reasonCode: closedRequest.reasonCode },
      close: closeResult,
      diagnostics: closeDiagnostics,
      afterSettle: closeAfterSettle
    }
  }
}));
