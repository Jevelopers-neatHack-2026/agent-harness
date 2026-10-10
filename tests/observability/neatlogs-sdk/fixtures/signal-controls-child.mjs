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

let proxyHooks = 0;
const proxyTarget = new AbortController().signal;
const proxySignal = new Proxy(proxyTarget, {
  getPrototypeOf() {
    proxyHooks += 1;
    return AbortSignal.prototype;
  }
});
let forgedHooks = 0;
const forgedSignal = Object.create(AbortSignal.prototype);
Object.defineProperties(forgedSignal, {
  aborted: { configurable: true, get() { forgedHooks += 1; throw new Error('synthetic-forged-aborted'); } },
  addEventListener: { configurable: true, value() { forgedHooks += 1; throw new Error('synthetic-forged-add'); } },
  removeEventListener: { configurable: true, value() { forgedHooks += 1; throw new Error('synthetic-forged-remove'); } }
});

const capture = createNeatlogsSdkCapture({ timeoutMs: 500, exporterDelayMs: 150 });
const proxyResult = await capture.prepare(records, { signal: proxySignal });
const forgedResult = await capture.prepare(records, { signal: forgedSignal });
const preAbortedController = new AbortController();
preAbortedController.abort();
const preAborted = await capture.prepare(records, { signal: preAbortedController.signal });
const beforeNative = capture.diagnostics();

let nativeHooks = 0;
const nativeController = new AbortController();
Object.defineProperties(nativeController.signal, {
  aborted: { configurable: true, get() { nativeHooks += 1; throw new Error('synthetic-own-aborted'); } },
  addEventListener: { configurable: true, value() { nativeHooks += 1; throw new Error('synthetic-own-add'); } },
  removeEventListener: { configurable: true, value() { nativeHooks += 1; throw new Error('synthetic-own-remove'); } }
});
const pending = capture.prepare(records, { signal: nativeController.signal });
await waitForCallback(capture);
nativeController.abort();
const aborted = await pending;
const retry = await capture.prepare(records);
const beforeClose = capture.diagnostics();
const close = capture.close();
const closeSettled = await Promise.race([
  close.then(() => true),
  pause(600).then(() => false)
]);
const closeResult = await close;

console.log(JSON.stringify({
  proxy: { reasonCode: proxyResult.reasonCode, hooks: proxyHooks },
  forged: { reasonCode: forgedResult.reasonCode, hooks: forgedHooks },
  preAborted: { reasonCode: preAborted.reasonCode, sdkImported: beforeNative.sdkImported },
  nativeOverrides: {
    hooks: nativeHooks,
    abortReason: aborted.reasonCode,
    retryStatus: retry.status,
    retryGeneration: retry.generation,
    activeBeforeClose: beforeClose.active,
    pendingBeforeClose: beforeClose.exporter.pendingCallbacks
  },
  closeSettled,
  closeResult,
  closedDiagnostics: capture.diagnostics()
}));
