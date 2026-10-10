import {
  DurableWorkflowStore,
  createHmacTokenizer,
  createTelemetryExporter,
  projectTelemetryEvent,
  type TelemetryRecord,
  type TelemetrySink
} from '@jevelopers/agent-harness-runtime';

declare const store: DurableWorkflowStore;
declare const localTokenKey: Uint8Array;
declare const sink: TelemetrySink;

const tokenizer = createHmacTokenizer(localTokenKey);
declare const record: TelemetryRecord;
void record.eventId;

const exporter = createTelemetryExporter({
  store,
  tokenizer,
  sink,
  limits: { batchSize: 8, queueRecords: 8, sinkTimeoutMs: 200 }
});

const page = store.readEventPage({ afterSequence: 0, limit: 100 });
void page.events[0]?.type;
const pageProjection = projectTelemetryEvent(page.events[0], tokenizer.tokenize);
const listedProjection = projectTelemetryEvent(store.listEvents()[0], tokenizer.tokenize);
void pageProjection.eventId;
void listedProjection.eventId;
void exporter.diagnostics().pendingRecords;
