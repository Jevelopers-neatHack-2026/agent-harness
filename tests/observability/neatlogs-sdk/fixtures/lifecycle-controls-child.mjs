import { createHmac } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { createNeatlogsSdkCapture } from '../../../../src/observability/private/neatlogs-sdk-capture.js';

const records = JSON.parse(readFileSync(resolve(process.argv[2]), 'utf8'));
const otherRecords = records.map(record => ({
  ...record,
  eventId: `tok1_${createHmac('sha256', Buffer.alloc(32, 0x31)).update(record.eventId).digest('hex')}`,
  workflowId: `tok1_${createHmac('sha256', Buffer.alloc(32, 0x31)).update(record.workflowId).digest('hex')}`
}));
const knownNeatlogsSymbols = new Set([
  'neatlogs.identity.storage',
  'neatlogs.active_client_async_local_storage',
  'neatlogs.private_span_async_local_storage',
  'neatlogs.private_provider_state',
  'neatlogs.media-runtime.v1',
  'neatlogs.log_runtime_state'
]);

function neatlogsGlobalSymbols() {
  return Object.getOwnPropertySymbols(globalThis)
    .map(symbol => symbol.description)
    .filter(description => typeof description === 'string'
      && (description.startsWith('neatlogs.') || description.startsWith('opentelemetry.js.api.')))
    .sort();
}

function processListenerSnapshot() {
  return Object.freeze({
    beforeExit: process.listenerCount('beforeExit'),
    sigint: process.listenerCount('SIGINT'),
    sigterm: process.listenerCount('SIGTERM')
  });
}

function hasGlobalOtelTraceProvider() {
  const key = Symbol.for('opentelemetry.js.api.1');
  const descriptor = Object.getOwnPropertyDescriptor(globalThis, key);
  return descriptor?.value?.trace !== undefined;
}

function roots(result) {
  return result?.envelope?.spans
    ?.filter(span => span.name === 'agent_harness.workflow_event')
    .map(span => span.attributes['agent_harness.telemetry.event_token']) ?? [];
}

const beforeListeners = processListenerSnapshot();
const beforeSymbols = neatlogsGlobalSymbols();
const beforeGlobalTrace = hasGlobalOtelTraceProvider();

const beforeAbortController = new AbortController();
beforeAbortController.abort();
const beforeAbortCapture = createNeatlogsSdkCapture();
const beforeAbort = await beforeAbortCapture.prepare(records, { signal: beforeAbortController.signal });
const beforeAbortDiagnostics = beforeAbortCapture.diagnostics();
await beforeAbortCapture.close();

const delayedCapture = createNeatlogsSdkCapture({ timeoutMs: 300, exporterDelayMs: 40 });
const abortController = new AbortController();
const abortedPromise = delayedCapture.prepare(records, { signal: abortController.signal });
const concurrentRequest = await delayedCapture.prepare(records);
setTimeout(() => abortController.abort(), 5);
const abortedResult = await abortedPromise;
const replayResult = await delayedCapture.prepare(records);
const closeFirst = delayedCapture.close();
const closeSecond = delayedCapture.close();
const [closedOne, closedTwo] = await Promise.all([closeFirst, closeSecond]);
const afterClosePrepare = await delayedCapture.prepare(records);

const timeoutCapture = createNeatlogsSdkCapture({ timeoutMs: 10, exporterDelayMs: 100 });
const timeoutPromise = timeoutCapture.prepare(records);
await new Promise(resolveWait => setTimeout(resolveWait, 20));
const busyDuringTimeout = await timeoutCapture.prepare(records);
const timeoutResult = await timeoutPromise;
const timeoutClose = await timeoutCapture.close();

const failedExporter = createNeatlogsSdkCapture({ exporterMode: 'failure' });
const failedExporterResult = await failedExporter.prepare(records);
const failedExporterClose = await failedExporter.close();

const leftCapture = createNeatlogsSdkCapture();
const rightCapture = createNeatlogsSdkCapture();
const [leftResult, rightResult] = await Promise.all([
  leftCapture.prepare(records),
  rightCapture.prepare(otherRecords)
]);
const leftClose = await leftCapture.close();
const rightClose = await rightCapture.close();

const afterListeners = processListenerSnapshot();
const afterSymbols = neatlogsGlobalSymbols();
const afterGlobalTrace = hasGlobalOtelTraceProvider();
const addedSymbols = afterSymbols.filter(symbol => !beforeSymbols.includes(symbol));
const output = Object.freeze({
  testMode: 'offline-synthetic-real-sdk',
  beforeAbort: {
    status: beforeAbort.status,
    reasonCode: beforeAbort.reasonCode,
    sdkImported: beforeAbortDiagnostics.sdkImported,
    clientCreated: beforeAbortDiagnostics.clientCreated
  },
  delayedAbort: {
    status: abortedResult.status,
    reasonCode: abortedResult.reasonCode,
    concurrentReason: concurrentRequest.reasonCode,
    generation: abortedResult.generation,
    replayStatus: replayResult.status,
    replayGeneration: replayResult.generation,
    replayTokens: roots(replayResult),
    closeSamePromise: closeFirst === closeSecond,
    closeResult: closedOne.status,
    closeCompleted: closedOne.sdkShutdownCompleted,
    closeResultMatches: JSON.stringify(closedOne) === JSON.stringify(closedTwo),
    postCloseReason: afterClosePrepare.reasonCode
  },
  timeout: {
    status: timeoutResult.status,
    reasonCode: timeoutResult.reasonCode,
    generation: timeoutResult.generation,
    busyReason: busyDuringTimeout.reasonCode,
    closeCompleted: timeoutClose.sdkShutdownCompleted
  },
  exporterFailure: {
    status: failedExporterResult.status,
    reasonCode: failedExporterResult.reasonCode,
    closeCompleted: failedExporterClose.sdkShutdownCompleted
  },
  concurrentClients: {
    leftStatus: leftResult.status,
    rightStatus: rightResult.status,
    leftTokens: roots(leftResult),
    rightTokens: roots(rightResult),
    leftCloseCompleted: leftClose.sdkShutdownCompleted,
    rightCloseCompleted: rightClose.sdkShutdownCompleted
  },
  lifecycle: {
    listenersUnchanged: JSON.stringify(beforeListeners) === JSON.stringify(afterListeners),
    globalTraceProviderBefore: beforeGlobalTrace,
    globalTraceProviderAfter: afterGlobalTrace,
    globalSymbolsBefore: beforeSymbols,
    globalSymbolsAfter: afterSymbols,
    addedSymbolsKnown: addedSymbols.every(symbol => knownNeatlogsSymbols.has(symbol)
      || symbol.startsWith('opentelemetry.js.api.'))
  }
});
process.stdout.write(`${JSON.stringify(output)}\n`);
