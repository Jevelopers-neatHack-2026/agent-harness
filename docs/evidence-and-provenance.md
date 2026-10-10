# AH-10 internal telemetry evidence

## Scope and provenance

This increment adds an internal, redacted, bounded telemetry pipeline over the existing SQLite `workflow_events` table. It does not wire the Neatlogs SDK, send telemetry to a service, enable Entire, or claim public canonical telemetry agreement. The sink is always supplied explicitly by the caller; there is no production no-op or mock fallback.

The named local fixture is `AH-10` / `internal-redaction-outage`, run only with `MODE=mock`. Run it with:

```sh
make verify CASE=AH-10 MODE=mock FIXTURE=internal-redaction-outage
```

The command writes a TAP trace and JSON provenance manifest under `.runtime-artifacts/`. The manifest records the exact checked-out commit, Node version, test and static assertion call-site counts, pass/fail/skip counts, fixture ID, limits, source and trace hashes, and the absence of provider calls, network calls, Neatlogs ingestion, sponsor acceptance, and Entire enablement. The adjacent `.sha256` file hashes the manifest. These files are generated evidence and are ignored by Git.

The result is local internal evidence only. Full AH-10, SPN-01, SPN-02, Neatlogs, Entire, and cross-repository acceptance remain open. `neatlogsSdkWired`, `neatlogsIngestionVerified`, and `sponsorAcceptance` are false in the manifest. No provider or telemetry-ingestion call is made by this fixture.

## Runtime contract

`DurableWorkflowStore.readEventPage()` reads explicit event columns with SQL `ORDER BY sequence LIMIT ?`, returning no more than the requested page. Its maximum returned page is 500 rows. `getTelemetryCursor()` and `advanceTelemetryCursor()` use a separate schema-v4 table. Cursor writes use `BEGIN IMMEDIATE`, compare-and-swap the expected value, advance exactly one sequence at a time, and verify that event exists. No telemetry code reads, acknowledges, or changes the financial outbox, attempt, observation, hold, workflow, receipt, or artifact rows.

The exporter projects only this allowlist: event sequence, tokenized event and workflow identity, revision, event type, state transition, fence, and event time. Event types and state values are fixed allowlists. Event and workflow IDs are treated as sensitive and tokenized using domain-separated HMAC-SHA-256. Callers should keep the same local tokenization key across restarts if replay identity must remain stable, and destroy the tokenizer when its key is no longer needed. Tokenization errors fail closed and leave the durable cursor pending. There is no raw-ID fallback.

Checkpoint values, prompts, response bytes, artifact data, hold references, errors, URLs, credentials, and unknown fields are never projected. Event objects with accessors, proxies, cycles, invalid types, unexpected fields, or excessive strings are rejected before arbitrary property access or serialization. Unsupported or structurally unsafe stored events are deliberate policy drops; their count is separate from records accepted by the sink. Diagnostics contain generic state, counts, cursor position, and registered error codes only.

The exporter has no import-time work, timers, signal handlers, or background loop. The caller invokes `flush()` and `close()` explicitly. Each call is bounded by these default limits:

| Limit | Default | Maximum |
| --- | ---: | ---: |
| Store API page default / maximum returned | 100 | 500 |
| Queue records | 32 | 128 |
| Queue bytes | 16 KiB | 64 KiB |
| Records per sink batch | 16 | 32 |
| Sink attempts per batch | 2 | 3 |
| Retry delay | 25 ms | 100 ms |
| Sink timeout per attempt | 250 ms | 500 ms |
| Batches per flush | 4 | 8 |
| Policy drops per flush | 16 | 32 |
| Source string length | 2,048 UTF-16 code units | 2,048 |
| Projected record size | 1 KiB | 1 KiB |
| Pending diagnostic count | 10,000 rows, capped | 10,000 |

The exporter asks SQL for at most its configured batch size at a time (16 by default). The API's 100-row default is for direct internal callers. A flush waits no more than 2.1 seconds on sink calls and retry delays at default settings, or 13.6 seconds at maximum settings, before accounting for synchronous SQLite work. SQLite busy waits are bounded by the `DurableWorkflowStore` connection setting (5 seconds by default, 60 seconds maximum).

These deadlines bound asynchronous waiting; they cannot preempt synchronous blocking work. The injected tokenizer and sink are trusted in-process callbacks, not a sandbox. A callback that blocks synchronously before returning, or other synchronous JavaScript/SQLite/native work, can block the event loop beyond these wait limits. `AbortSignal` does not provide isolation or interrupt synchronous code.

The queue contains only sanitized records. A timed-out sink receives an aborted `AbortSignal`; if it ignores the signal, the exporter retains one active-request latch until that promise settles and does not start another request. Failed delivery leaves the cursor unchanged. If the sink accepted a batch and the process exits before cursor commit, the same tokenized event identities are replayed. This is at-least-once delivery; it is not an exactly-once guarantee. Concurrent exporters use cursor compare-and-swap, which can result in duplicate accepted records but cannot skip a pending sequence.

The sink must be a caller-provided implementation that only returns `{ accepted: true, acceptedCount }` after it has accepted the entire batch. This repository does not provide an acceptance stub or assume a remote service's API contract. Any future Neatlogs adapter requires exact official API verification and a deliberate dependency choice before it is added.

## Schema compatibility

Schema version 4 adds only `telemetry_cursors(consumer_id, last_sequence, updated_at_ms)`. Fresh databases create it with the existing v3 workflow schema in one transaction. V1-to-v4, v2-to-v4, and v3-to-v4 upgrades run transactionally; existing checkpoint/receipt protections and event/outbox rows are preserved. V1 upgrades retain the established migration event behavior. V2 receipts without checkpoint integrity evidence remain unreadable for reconciliation, as before. A failed migration rolls back its schema and row changes. Versions newer than 4 are rejected before journal-mode changes or writes.

## Downstream internal use

Import from the repository's internal package entrypoint. Supply a durable store, a stable local tokenizer (or `tokenize` function), and an explicitly implemented sink:

```js
const exporter = createTelemetryExporter({
  store,
  tokenizer: stableLocalTokenizer,
  sink: verifiedSink
});

await exporter.flush();
const status = exporter.diagnostics();
await exporter.close();
stableLocalTokenizer.destroy?.();
```

Call `flush()` at a deliberate lifecycle boundary and `close()` during bounded shutdown. Inspect `diagnostics()` for generic pending/delivered/policy-drop counts. The exporter does not own or close the store or tokenizer. These classes and the current store read APIs remain internal and unauthenticated; they are not a public application API.

## Environment limits

Local required checks use Node `22.23.3` on macOS and the offline package lock. The exact hosted Node patch, Linux behavior, and LVE constraints are not verified here. The internal mock fixture makes no claim about actual Neatlogs service acceptance, public canonical telemetry, Entire behavior, or sponsorship evidence.
