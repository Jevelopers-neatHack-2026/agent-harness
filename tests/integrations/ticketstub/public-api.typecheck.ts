import {
  createTicketstubHarnessExporter,
  type TicketstubAcceptResult,
  type TicketstubEventInput,
  type TicketstubHarnessExporter,
  type TicketstubHarnessExporterOptions
} from '@jevelopers/agent-harness-runtime/ticketstub';

const acceptedOutboxOptions: NonNullable<TicketstubHarnessExporterOptions['outboxOptions']>[] = [
  {},
  { maxUnsettled: 1 },
  { maxSettledRows: 1 },
  { retentionMs: 1 },
  { readbackDelayMs: 1 },
  { missingAfterMs: 1 },
  { pendingGiveUpMs: 1 },
  { retryBaseMs: 1 },
  { retryMaxMs: 1 },
  { drainLeaseMs: 1 },
  { maxBatchesPerDrain: 1 },
  { maxUnsettled: 1, retryMaxMs: 2, maxBatchesPerDrain: 3 }
];

// @ts-expect-error unknown option names stay outside the public contract
const unknownOutboxOption: TicketstubHarnessExporterOptions = { env: {}, databasePath: '/tmp/x.sqlite', outboxOptions: { maxUnsettled: 1, maxBatchPerDrain: 2 } };

// @ts-expect-error option values remain numeric
const wrongOutboxOptionValue: TicketstubHarnessExporterOptions = { env: {}, databasePath: '/tmp/x.sqlite', outboxOptions: { maxUnsettled: 'many' } };

void acceptedOutboxOptions;
void unknownOutboxOption;
void wrongOutboxOptionValue;

const event: TicketstubEventInput = {
  schema: 'ticketstub.event/1',
  type: 'art_ready',
  at: 1,
  requestId: 'req_typecheck',
  actor: 'anonymous',
  mode: 'sandbox',
  fields: { provider: 'procedural' }
};

export async function typecheck(env: Record<string, string | undefined>): Promise<number> {
  const exporter: TicketstubHarnessExporter = createTicketstubHarnessExporter({ env, databasePath: '/tmp/x.sqlite' });
  const result: TicketstubAcceptResult = exporter.accept([event]);
  const drained = await exporter.drain();
  await exporter.close();
  return result.accepted + (drained?.verified ?? 0) + exporter.status().outbox.counts.queued;
}
