import { Client, trace, type ClientOptions } from 'neatlogs';

const exporter: NonNullable<ClientOptions['spanExporter']> = {
  export(spans, resultCallback) {
    const spanCount: number = spans.length;
    if (spanCount < 0) throw new RangeError('unreachable');
    resultCallback({ code: 0 });
  },
  async shutdown() {},
  async forceFlush() {}
};

const client = new Client({
  apiKey: 'synthetic-typecheck-only',
  workflowName: 'agent-harness-neatlogs-typecheck',
  endpoint: 'https://offline.invalid/v1/traces',
  sampleRate: 1,
  debug: false,
  captureLogs: false,
  uploadAuthority: false,
  disableExport: false,
  spanExporter: exporter
});

const traceResult: Promise<void> = Promise.resolve(client.activate(() => trace({
  name: 'agent_harness.workflow_event',
  kind: 'WORKFLOW',
  attributes: { 'agent_harness.telemetry.sequence': 1 }
}, () => undefined)));

void traceResult;
