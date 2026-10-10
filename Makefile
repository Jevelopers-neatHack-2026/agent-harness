.PHONY: runtime bootstrap build check test verify neatlogs-sdk-test

runtime:
	@node -e 'const [major, minor, patch] = process.versions.node.split(".").map(Number); if (major !== 22 || minor < 23 || (minor === 23 && patch < 3)) { console.error("Node 22.23.3 or a later Node 22 patch is required."); process.exit(1); }'

bootstrap: runtime
	npm ci --ignore-scripts --offline --no-audit --no-fund
	npm run build

build: runtime
	npm run build

check: runtime
	npm run check

test: runtime
	npm test

neatlogs-sdk-test: runtime
	npm run test:neatlogs-sdk

verify: runtime
	@test "$(MODE)" = "mock" || { echo "Only MODE=mock is supported by this internal target."; exit 2; }
	@case "$(CASE):$(FIXTURE)" in \
	  AH-03:state-crash-matrix) echo "AH-03 internal prerequisite durability suite; global acceptance remains pending."; node --test tests/runtime/durability.test.js ;; \
	  AH-03:lease-migration) echo "AH-03 internal prerequisite durability suite; global acceptance remains pending."; node --test tests/runtime/durability.test.js ;; \
	  AH-01:internal-boundary) echo "AH-01 internal foundation suite; canonical compatibility remains pending."; node --test tests/runtime/foundation.test.js ;; \
	  AH-10:internal-redaction-outage) echo "AH-10 internal synthetic telemetry suite; Neatlogs, Entire, and cross-repository acceptance remain pending."; node tests/observability/verify-fixture.js AH-10 mock internal-redaction-outage ;; \
	  LOCAL-BOUNDED:private-local-controller) echo "Private bounded local controller prerequisite only; full AH-04/AH-08, canonical, Finance, tenant, planner, and live acceptance remain pending."; node --test --test-concurrency=1 tests/runtime/local-controller.test.js tests/planning/*.test.js tests/recovery/*.test.js ;; \
	  AH-10:neatlogs-sdk-local) echo "AH-10 offline synthetic records through the pinned real Neatlogs SDK; live Neatlogs, Entire, and cross-repository acceptance remain pending."; npm run test:neatlogs-sdk ;; \
	  ACV1-AH05:jev-phase-b-offline) echo "ACV1-AH05 Phase A/B internal prerequisite evidence only; canonical DecisionPort, Finance, tenant authority, provider access, live conformance, evaluation, and release gates remain open."; node tests/decisions/jev-adapter.contract/verify-offline.js ACV1-AH05 mock jev-phase-b-offline ;; \
	  *) echo "Unsupported case or fixture. Required canonical and cross-repository acceptance cannot pass in this increment."; exit 2 ;; \
	esac
