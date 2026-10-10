import { Client, trace, __version__ } from 'neatlogs';

const MODES = Object.freeze(['null', 'undefined', 'invalid', 'throw', 'reject', 'timeout']);
const CANARIES = Object.freeze([
  'SYNTHETIC_RAW_ID_CANARY_41a',
  'SYNTHETIC_URL_CANARY_https://private.invalid/path?key=suffix',
  'SYNTHETIC_SECRET_SUFFIX_CANARY_72c',
  'SYNTHETIC_CUSTOMER_TEXT_CANARY_05e',
  'SYNTHETIC_ERROR_TEXT_CANARY_98d'
]);
const holdOpen = setInterval(() => {}, 60_000);

function containsCanary(value) {
  if (typeof value === 'string') return CANARIES.some(canary => value.includes(canary));
  if (Array.isArray(value)) return value.some(containsCanary);
  if (value !== null && typeof value === 'object') {
    return Object.values(value).some(containsCanary);
  }
  return false;
}

async function runMode(mode) {
  const observation = { rawCanaryObservedByLocalExporter: false, exportedSpanCount: 0 };
  const exporter = {
    export(spans, resultCallback) {
      observation.exportedSpanCount += spans.length;
      for (const span of spans) {
        if (containsCanary(span.attributes) || containsCanary(span.resource.attributes)) {
          observation.rawCanaryObservedByLocalExporter = true;
        }
      }
      resultCallback({ code: 0 });
    },
    async forceFlush() {},
    async shutdown() {}
  };
  const mask = mode === 'null'
    ? () => null
    : mode === 'undefined'
      ? () => undefined
      : mode === 'invalid'
        ? () => ({ unexpected: CANARIES[4] })
        : mode === 'throw'
          ? () => { throw new Error('controlled synthetic mask failure'); }
          : mode === 'reject'
            ? () => Promise.reject(new Error('controlled synthetic mask rejection'))
            : () => new Promise(() => {});
  const client = new Client({
    apiKey: 'offline-synthetic-placeholder-no-service-access',
    workflowName: 'agent-harness-sdk-mask-regression',
    endpoint: 'https://offline.invalid/v1/traces',
    sampleRate: 1,
    debug: false,
    captureLogs: false,
    uploadAuthority: false,
    disableExport: false,
    mask,
    batchSize: 2,
    flushInterval: 3600,
    spanExporter: exporter
  });
  await client.activate(() => trace({
    name: 'synthetic.safe.workflow',
    kind: 'WORKFLOW',
    input: { rawId: CANARIES[0], privateUrl: CANARIES[1], customer: CANARIES[3] },
    attributes: {
      'synthetic.raw.id': CANARIES[0],
      'synthetic.private.url': CANARIES[1],
      'synthetic.secret.suffix': CANARIES[2],
      'synthetic.customer.text': CANARIES[3],
      'synthetic.error.message': CANARIES[4]
    }
  }, () => ({ output: CANARIES[3] })));
  const flushResult = await client.flush();
  const diagnostics = client.getDeliveryDiagnostics();
  const closeResult = await client.close('synthetic-mask-regression-complete', 500);
  return Object.freeze({
    mode,
    sdkVersion: __version__,
    flushResult,
    closeResult,
    maskedSpanDrops: diagnostics.maskedSpanDrops,
    spanQueueDrops: diagnostics.spanQueueDrops,
    spanExportFailures: diagnostics.spanExportFailures,
    ...observation
  });
}

try {
  const results = [];
  for (const mode of MODES) results.push(await runMode(mode));
  process.stdout.write(`${JSON.stringify({ testMode: 'offline-synthetic-real-sdk', results })}\n`);
} finally {
  clearInterval(holdOpen);
}
