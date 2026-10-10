import { writeFileSync } from 'node:fs';
import { DurableWorkflowStore, createHmacTokenizer, createTelemetryExporter } from '@jevelopers/agent-harness-runtime';

const [databasePath, stage, readyPath, acceptedPath] = process.argv.slice(2);
const store = DurableWorkflowStore.open({ databasePath, clock: () => 20_000 });
const tokenizer = createHmacTokenizer(Buffer.alloc(32, 91));
const exporter = createTelemetryExporter({
  store,
  tokenizer,
  sink: async records => {
    writeFileSync(acceptedPath, JSON.stringify(records.map(record => record.eventId)));
    if (stage === 'accepted-before-cursor') {
      writeFileSync(readyPath, 'ready');
      return holdForParentKill();
    }
    return { accepted: true, acceptedCount: records.length };
  },
  limits: { batchSize: 16, queueRecords: 16, sinkTimeoutMs: 100 }
});

function holdForParentKill() {
  // The parent owns termination in these crash-window fixtures.
  setInterval(() => {}, 1000);
  return new Promise(() => {});
}

if (stage === 'before-export') {
  writeFileSync(readyPath, 'ready');
  await holdForParentKill();
}

if (stage === 'after-cursor') {
  await exporter.flush();
  writeFileSync(readyPath, String(store.getTelemetryCursor()));
  await holdForParentKill();
}

if (stage === 'accepted-before-cursor') {
  await exporter.flush();
}

store.close();
