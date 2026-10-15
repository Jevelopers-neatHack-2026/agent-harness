<a href="https://github.com/Jevelopers-neatHack-2026/agent-harness">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/readme/banner-night.svg">
    <img alt="agent-harness — Durable workflow execution and recovery. TicketStub banner showing a track of punched checkpoints with a loop-back arrow for recovery." src="docs/assets/readme/banner-day.svg" width="100%">
  </picture>
</a>

# agent-harness

Node 22 and SQLite runtime components for TicketStub workflow execution, recovery, decision integration and sanitized telemetry.

<p>
  <a href="https://github.com/Jevelopers-neatHack-2026"><img alt="TicketStub ecosystem" src="https://img.shields.io/badge/TicketStub-ecosystem-E4572E?style=flat-square&labelColor=17140F"></a>
  <a href="#status"><img alt="Status: experimental internal foundation" src="https://img.shields.io/badge/status-experimental%20foundation-B4693E?style=flat-square&labelColor=17140F"></a>
  <a href=".node-version"><img alt="Node.js 22.23.3" src="https://img.shields.io/badge/node-22.23.3-26302A?style=flat-square&labelColor=17140F"></a>
  <a href="docs/decisions/runtime-foundation.md#runtime-and-package"><img alt="Storage: node:sqlite" src="https://img.shields.io/badge/storage-node%3Asqlite-7CC6D2?style=flat-square&labelColor=17140F"></a>
</p>

[Status](#status) · [Architecture](#architecture-and-responsibilities) · [Quickstart](#quickstart) · [Configuration](#configuration) · [Checks](#checks) · [Docs](#docs) · [Related](#related-repositories)

## Status

The package `@jevelopers/agent-harness-runtime` is an experimental, private, unpublished package. The repository contains local runtime foundations and explicit host integration entry points; the complete Agent & Creative v1 release remains pending.

Implemented locally, with offline tests:

- Strict local runtime configuration and safe typed errors. The internal store/controller rejects `mode: "live"`.
- File-backed SQLite workflow durability primitives on Node's built-in `node:sqlite`: checkpoints, worker fencing, recovery receipts, immutable bytes and a transactional event outbox ([runtime foundation](docs/decisions/runtime-foundation.md)).
- A private bounded local controller with plan, registry, limit and recovery-classification mechanics ([bounded runtime](docs/decisions/bounded-internal-runtime.md)).
- Redacted telemetry through the pinned `neatlogs` 1.1.28 SDK, durable delivery queues, and explicit OTLP ingestion/Public API readback tools ([Neatlogs runbook](docs/runbooks/neatlogs.md)). The tools distinguish receiver acceptance from verified readback; offline tests use controlled transports.
- A public `/decisions` entry point over the private TypeSafe JEV transport, with host-injected credentials, eligibility, creative validators and dispatch fences ([TypeSafe host integration](docs/decisions/typesafe-host-integration.md)). Tests intercept transport; they do not prove live account access or inference.
- A `/ticketstub` application-event exporter and `/development` entry point that invokes an existing authenticated development/test host with matching component source pins ([development driver](docs/driver.md)).

The internal controller's live-mode restriction is separate from the opt-in decision and telemetry transports. Historical Neatlogs receipts are qualified in the [runbook](docs/runbooks/neatlogs.md#historical-ticketstub-evidence-qualification); they do not establish a deployed end-to-end workflow. Entire development configuration and workflow files are present, but their presence alone is not checkpoint or release evidence.

Pending: AH-01 and AH-03 acceptance needs canonical fixtures, consumer agreement, cross-repository compatibility, server runtime confirmation and integrated evidence. [AH-01 #7](https://github.com/Jevelopers-neatHack-2026/agent-harness/issues/7) and [AH-03 #9](https://github.com/Jevelopers-neatHack-2026/agent-harness/issues/9) stay open until those gates pass.

The complete development journey and release evidence remain tracked in [AH-12 #23](https://github.com/Jevelopers-neatHack-2026/agent-harness/issues/23) and [release gate #24](https://github.com/Jevelopers-neatHack-2026/agent-harness/issues/24). This package does not implement application membership, artwork approval, Finance budgets/ledger, or the specialist research and creative engines.

## Architecture and responsibilities

The application composes the package with trusted specialist ports. Durable workflow state records execution and recovery; Finance remains authoritative for spending, and the application remains authoritative for customer state and authorization.

| Package entry point | Responsibility |
| --- | --- |
| `@jevelopers/agent-harness-runtime` | Internal configuration, SQLite durability and telemetry primitives |
| `@jevelopers/agent-harness-runtime/ticketstub` | Validated TicketStub event intake, HMAC tokenization and durable Neatlogs export |
| `@jevelopers/agent-harness-runtime/decisions` | Host-configured, bounded TypeSafe DecisionPort transport |
| `@jevelopers/agent-harness-runtime/development` | Invoke the existing host after validating immutable component pins |

The export map is in [`package.json`](package.json). These entry points do not start an HTTP service; the host supplies the application and external authorities.

## Requirements

Use Node **22.23.3** ([`.node-version`](.node-version); `engines` accepts `>=22.23.3 <23`), npm, Make and Python 3 for the owned verification wrapper. The process-ownership checks use POSIX process groups; CI runs on Linux. Every Make target checks the Node version first. Local durability uses Node's built-in `node:sqlite`.

## Quickstart

From a checkout of this repository:

```sh
npm ci --ignore-scripts --no-audit --no-fund
make check
python3 .github/scripts/run_ops_owned_command.py --route runtime-foundation
```

Already have the locked packages in your npm cache? `make bootstrap` installs offline and builds in one step. Toolchain pins, the full verification case list and CI details are in the [runtime setup runbook](docs/runbooks/runtime-setup.md).

## Configuration

Runtime settings are passed explicitly to `parseRuntimeConfig`; the package does not automatically load `.env`. No credentials are required for the offline quickstart.

| Setting | Purpose / safe local example |
| --- | --- |
| `HARNESS_MODE` | Explicit dependency mode; use `mock` for local fixtures |
| `HARNESS_MODEL_ID` | Explicit model identifier, such as `fixture-model`; no `latest` or `auto` alias |
| `HARNESS_DEADLINE_MS` | Operation deadline bound, such as `1000` |
| `HARNESS_MAX_ATTEMPTS` | Attempt ceiling, such as `1` |
| `HARNESS_MAX_ELAPSED_MS` | Total elapsed-time bound, such as `1000` |
| `HARNESS_MAX_COST_MINOR_UNITS` | Local bound, such as `0`; it grants no Finance authority |

The host injects `TYPESAFE_API_KEY` and independent eligibility for `/decisions`. Optional Neatlogs configuration uses `NEATLOGS_INGEST_KEY`, `NEATLOGS_READ_TOKEN`, `NEATLOGS_PROJECT_ID`, and the paired `NEATLOGS_ENDPOINT`/`NEATLOGS_HOST` origins. `/ticketstub` additionally requires the server-held `AGENT_HARNESS_TELEMETRY_KEY`. See [`.env.example`](.env.example), [TypeSafe composition](docs/decisions/typesafe-host-integration.md#composition) and [Neatlogs configuration](docs/runbooks/neatlogs.md#loading-configuration) for exact loading, legacy-alias and scope rules. Keep secret values out of commands, fixtures and committed files.

## Checks

All checks run offline in mock mode and call no provider.

```sh
make check                # syntax, entrypoint and SDK import, strict typecheck
python3 .github/scripts/run_ops_owned_command.py --route runtime-foundation
                           # offline runtime tests with OPS process and registry ownership
make neatlogs-sdk-test    # Neatlogs SDK local-capture tests only
npm run test:decisions   # intercepted TypeSafe transport and public-port checks
make verify CASE=AH-03 MODE=mock FIXTURE=state-crash-matrix
make verify CASE=ACV1-AH05 MODE=mock FIXTURE=jev-phase-b-offline
python3 tools/verify_provider_readiness.py --profile fixtures/provider-profile.json --mode offline
```

Each `make verify` case prints its scope and is internal prerequisite evidence only; unsupported cases exit `2`. The provider readiness fixture is deliberately incomplete and is expected to exit `2` (pending). See the [runbook](docs/runbooks/runtime-setup.md#named-verification-cases) and [Makefile](Makefile) for the supported cases.

Run the two AH-11-OPS fixtures through the same owner so their detached process groups, private registry and cleanup receipts are reconciled:

```sh
python3 .github/scripts/run_ops_owned_command.py --route ops-quota-shutdown-restart
python3 .github/scripts/run_ops_owned_command.py --route ops-runbook-clean-start
```

The owner accepts only these fixed routes. It caps cleanup authority at 16 unique recorded groups across both registries and reconciles at most six custody sidecars. Its configured post-command wait allowances sum to 18.4 seconds; this is a sum of bounded waits, not a universal wall-clock guarantee. Do not run `npm test` or either AH-11-OPS fixture directly; each route needs the fresh registry and private temporary directory that the owner creates.

[`runtime-foundation.yml`](.github/workflows/runtime-foundation.yml) installs the locked package, runs the build/typechecks and owned runtime/OPS routes, and retains scoped evidence. There is no separate Markdown lint script in the package.

## Repository structure

| Path | Contents |
| --- | --- |
| [`src/internal/`](src/internal/) | Durable store, configuration and telemetry primitives |
| [`src/orchestrator/`](src/orchestrator/), [`src/worker/`](src/worker/), [`src/recovery/`](src/recovery/) | Bounded local execution and recovery |
| [`src/decisions/`](src/decisions/), [`src/adapters/jev/`](src/adapters/jev/) | Public DecisionPort and private wire transport |
| [`src/observability/`](src/observability/), [`src/integrations/ticketstub/`](src/integrations/ticketstub/) | Sanitized traces, outboxes and application event intake |
| [`tests/`](tests/), [`fixtures/`](fixtures/) | Offline verification and synthetic inputs |
| [`docs/`](docs/), [`tools/`](tools/) | Decisions, runbooks and explicit verification utilities |

## Docs

| Topic | Document |
| --- | --- |
| Setup, checks and CI | [Runtime setup and verification](docs/runbooks/runtime-setup.md) |
| Runtime, durability and remaining gates | [Runtime foundation decision](docs/decisions/runtime-foundation.md) |
| Bounded local controller | [Bounded private runtime](docs/decisions/bounded-internal-runtime.md) |
| Telemetry | [AH-10 telemetry evidence](docs/evidence-and-provenance.md) · [Neatlogs SDK conformance](docs/decisions/neatlogs-sdk-conformance.md) |
| Opt-in trace delivery | [Neatlogs ingestion, readback and historical evidence scope](docs/runbooks/neatlogs.md) |
| JEV adapter | [Provider wire conformance](docs/decisions/provider-conformance.md) |
| TypeSafe application composition | [Public DecisionPort and owned execution](docs/decisions/typesafe-host-integration.md) |
| Development host | [Shared development driver boundary](docs/driver.md) |
| Provider access profile | [Provider access readiness](docs/runbooks/provider-access.md) |
| Ownership and acceptance | [Responsibilities and interfaces](docs/architecture/OWNERSHIP.md) · [Acceptance criteria](docs/architecture/ACCEPTANCE.md) |
| Architecture and plan | [Architecture kit v1.0](https://github.com/Jevelopers-neatHack-2026/getticketstub.com/blob/main/docs/architecture-kit/v1.0/README.md) · [Central implementation plan](https://github.com/Jevelopers-neatHack-2026/getticketstub.com/blob/main/docs/planning/implementation-plan.md) |

The architecture documents describe intended boundaries. They do not establish completed product functionality, passing implementation tests, connected sponsor tools or live financial activity.

## Related repositories

- [shared-contracts](https://github.com/Jevelopers-neatHack-2026/shared-contracts): maintained contract catalog and pending cross-repository schema agreements; this package does not install its proposed SDK.
- [creative-engine](https://github.com/Jevelopers-neatHack-2026/creative-engine): supplies the DecisionPort contract validators; the development test archive is pinned in `package-lock.json`, while real hosts inject their own reviewed component assembly.
- [event-intelligence](https://github.com/Jevelopers-neatHack-2026/event-intelligence): research specialist in the intended development journey; full integration remains pending.
- [cost-intelligence](https://github.com/Jevelopers-neatHack-2026/cost-intelligence): pricing, budgets and ledger, which this runtime does not implement.
- [getticketstub.com](https://github.com/Jevelopers-neatHack-2026/getticketstub.com): application host, authenticated customer scope and event producer.
- [integration-evals](https://github.com/Jevelopers-neatHack-2026/integration-evals): cross-component verification.

## Contributing

Read [AGENTS.md](AGENTS.md) and follow the organization [contributor guidance](https://github.com/Jevelopers-neatHack-2026/.github/blob/main/CONTRIBUTING.md). Keep tests offline and in mock mode, and never commit credentials or `.env` files. Document the exact validation scope in the PR; README changes do not close product acceptance gates.

## License

Original project code and associated documentation are available under the [MIT License](LICENSE).
See [copyright and team attribution](COPYRIGHT.md) for Jevelopers credits, the licensing decision,
and the scope of separately owned material and brand rights.
