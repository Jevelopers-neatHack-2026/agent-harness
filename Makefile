.PHONY: runtime bootstrap build check test verify

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

verify: runtime
	@test "$(MODE)" = "mock" || { echo "Only MODE=mock is supported by this internal target."; exit 2; }
	@case "$(CASE):$(FIXTURE)" in \
	  AH-03:state-crash-matrix) echo "AH-03 internal prerequisite durability suite; global acceptance remains pending."; node --test tests/runtime/durability.test.js ;; \
	  AH-03:lease-migration) echo "AH-03 internal prerequisite durability suite; global acceptance remains pending."; node --test tests/runtime/durability.test.js ;; \
	  AH-01:internal-boundary) echo "AH-01 internal foundation suite; canonical compatibility remains pending."; node --test tests/runtime/foundation.test.js ;; \
	  AH-10:internal-redaction-outage) echo "AH-10 internal synthetic telemetry suite; Neatlogs, Entire, and cross-repository acceptance remain pending."; node tests/observability/verify-fixture.js AH-10 mock internal-redaction-outage ;; \
	  *) echo "Unsupported case or fixture. Required canonical and cross-repository acceptance cannot pass in this increment."; exit 2 ;; \
	esac
