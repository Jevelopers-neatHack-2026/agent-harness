# Runtime setup and verification

This runbook collects the local setup and offline verification commands for the experimental internal runtime. Commands below use the [`Makefile`](../../Makefile), [`package.json`](../../package.json), fixed OPS route owner, or CI workflows under [`.github/workflows/`](../../.github/workflows). All of it runs in mock/offline mode; none of it reaches a provider, Neatlogs, Entire or a live service. For what the runtime does and does not implement, see the [runtime foundation decision](../decisions/runtime-foundation.md).

## Toolchain

| Item | Pin | Source |
| --- | --- | --- |
| Node.js | `22.23.3` | [`.node-version`](../../.node-version), used by CI through `node-version-file` |
| Accepted Node range | `>=22.23.3 <23` | `engines` in [`package.json`](../../package.json) |
| npm | `10.9.9` (the version local tests used) | [runtime foundation decision](../decisions/runtime-foundation.md#runtime-and-package) |
| `neatlogs` | `1.1.28` (exact) | `dependencies` in [`package.json`](../../package.json) |
| TypeScript | `5.9.3` (development-only, for the `.d.ts` consumer check) | `devDependencies` in [`package.json`](../../package.json) |
| Python | 3.9 in CI, standard library only (provider readiness check) | [`provider-readiness.yml`](../../.github/workflows/provider-readiness.yml) |

The runtime uses Node's built-in `node:sqlite` module, which is still experimental in Node 22. It needs a file-backed database on storage that supports SQLite WAL locking; there is no in-memory fallback. `make runtime` fails with "Node 22.23.3 or a later Node 22 patch is required." on Node versions outside 22.23.3–22.x, and every other Make target runs it first.

Local tests on Node 22.23.3 are not evidence of the deployment server's Node patch, SQLite build or filesystem limits. Server compatibility remains pending.

## Install and build

Offline bootstrap, which works when the locked packages are already in the npm cache:

```sh
make bootstrap
```

This runs `npm ci --ignore-scripts --offline --no-audit --no-fund` followed by `npm run build`.

With network access, install the way CI does, then build:

```sh
npm ci --ignore-scripts --no-audit --no-fund
make check
```

`make check` (an alias for `npm run build`) runs `node --check` on each source module, imports the package entrypoint `@jevelopers/agent-harness-runtime`, imports the pinned `neatlogs` SDK in a cleared environment, and runs `npm run typecheck`, which is the strict TypeScript check of the consumer fixtures under `tests/`.

## Tests

| Command | What it runs |
| --- | --- |
| `python3 .github/scripts/run_ops_owned_command.py --route runtime-foundation` | `npm test`, with an invocation-private registry and bounded process cleanup |
| `make neatlogs-sdk-test` / `npm run test:neatlogs-sdk` | The Neatlogs SDK local-capture tests only |
| `npm run typecheck` | The strict `.d.ts` consumer check only |

Run the AH-11-OPS fixtures through their fixed owner routes:

```sh
python3 .github/scripts/run_ops_owned_command.py --route ops-quota-shutdown-restart
python3 .github/scripts/run_ops_owned_command.py --route ops-runbook-clean-start
```

The owner creates a fresh private `TMPDIR` and registry for every route, then reconciles both the outer and OPS13 local registries before writing a fixed receipt under `.runtime-artifacts/`. Across both namespaces it permits at most 16 unique recorded process groups and six custody sidecars (49,152 bytes total). The configured post-command waits sum to 18.4 seconds: 2.25 seconds for the target group, 1.75 seconds for one combined registry batch and final absence check, 9.6 seconds for watchdog shutdown, and up to 4.8 seconds for sixteen independent sidecar-group probes. This is the sum of bounded wait allowances, not a universal wall-clock guarantee. Bare `npm test` and direct AH-11-OPS `make verify` commands do not provide this ownership and remain fail-closed. The owner uses only the three route IDs shown above and checks the pinned Node version without installing packages or contacting a provider.

## Named verification cases

`make verify` takes `CASE`, `FIXTURE` and `MODE`. Only `MODE=mock` is supported. Each case prints its own scope note, and unsupported cases exit `2`. Passing a case is internal prerequisite evidence only, not acceptance.

| Command | Runs | What this command does not establish |
| --- | --- | --- |
| `make verify CASE=AH-01 MODE=mock FIXTURE=internal-boundary` | `tests/runtime/foundation.test.js` | Canonical compatibility |
| `make verify CASE=AH-03 MODE=mock FIXTURE=state-crash-matrix` | `tests/runtime/durability.test.js` | Global AH-03 acceptance |
| `make verify CASE=AH-03 MODE=mock FIXTURE=lease-migration` | `tests/runtime/durability.test.js` | Global AH-03 acceptance |
| `make verify CASE=AH-10 MODE=mock FIXTURE=internal-redaction-outage` | `tests/observability/verify-fixture.js` | Full AH-10 and cross-repository acceptance; this fixture makes no Neatlogs or Entire call. |
| `make verify CASE=AH-10 MODE=mock FIXTURE=neatlogs-sdk-local` | `npm run test:neatlogs-sdk` | Service delivery, Entire provenance, and full AH-10 integration; this target is local SDK capture only. |
| `make verify CASE=LOCAL-BOUNDED MODE=mock FIXTURE=private-local-controller` | Local controller, planning and recovery tests | Full AH-04/AH-08, canonical, Finance, tenant, planner and live acceptance |
| `make verify CASE=ACV1-AH05 MODE=mock FIXTURE=jev-phase-b-offline` | `tests/decisions/jev-adapter.contract/verify-offline.js` | Canonical DecisionPort, Finance, tenant authority, provider access, live conformance, evaluation and release gates |

The AH-10 and ACV1-AH05 fixtures, and the Neatlogs SDK tests, write TAP traces and JSON manifests under `.runtime-artifacts/`, which Git ignores. The master tracker at [integration-evals revision `5601857`](https://github.com/Jevelopers-neatHack-2026/integration-evals/blob/56018576a9450a2bb46aa55ed3b2f9d9ae97e50d/planning/acceptance-checklist.md) separately records SPN-01 and SPN-02 as verified, owner-accepted on 2026-10-11. Those records do not come from the local commands above. See [AH-10 internal telemetry evidence](../evidence-and-provenance.md), [JEV provider wire conformance](../decisions/provider-conformance.md) and [Neatlogs local SDK mapping conformance](../decisions/neatlogs-sdk-conformance.md) for what each manifest records.

## Provider readiness profile

The stdlib-only profile check runs without installing anything:

```sh
python3 -B -m unittest discover -s tests/config/provider-profile -p 'test_*.py' -v
python3 tools/verify_provider_readiness.py --profile fixtures/provider-profile.json --mode offline
```

The committed fixture is intentionally incomplete, so the second command exits `2` (pending). That is the expected result. Exit codes and credential handling are in the [provider access readiness runbook](provider-access.md).

## Configuration

[`.env.example`](../../.env.example) lists `TYPESAFE_API_KEY` (left blank) and two GPT Image access-preference variables. The runtime does not read `.env` files, live access is not implemented, and the image variables do not configure an adapter. Runtime configuration must state mode, model, deadline, attempt, elapsed-time and cost bounds explicitly, and `mode: "live"` is rejected.

## What CI runs

| Workflow | Steps |
| --- | --- |
| [Runtime foundation](../../.github/workflows/runtime-foundation.yml) | Node from `.node-version`; `npm ci --ignore-scripts --no-audit --no-fund`; `make check`; owner-wrapped runtime and AH-11-OPS routes; `npm run test:neatlogs-sdk`; `make verify CASE=ACV1-AH05 MODE=mock FIXTURE=jev-phase-b-offline`; uploads the exact sanitized evidence allowlist |
| [Provider profile readiness](../../.github/workflows/provider-readiness.yml) | Python 3.9; the offline adversarial unittest suite; confirms the sample profile exits `2` and stays `pending` with no network access; uploads the sanitized report |
