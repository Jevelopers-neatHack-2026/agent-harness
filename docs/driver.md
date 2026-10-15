# Development driver boundary

`@jevelopers/agent-harness-runtime/development` exports
`invokeAgentCreativeDevelopment({ host, request, source_manifest })`. It delegates
to the same authenticated application's `host.invoke(request)` used by the
creative-engine public API. The host resolves server credentials, DecisionPort,
image provider, trusted scope, Finance, and durable recovery. The driver does not
create another adapter or supply an approval or reservation authority.

The host must identify `environment: "development"` or `"test"` and its loaded
`source_manifest`. The caller supplies the identical immutable Git pins for
`application`, `agent_harness`, and `creative_engine`; optional
`event_intelligence` and `finance` pins identify composed dependencies. Moving
references, mismatches, cross-environment requests, and production are rejected
before invoking the host. The application must verify its loaded vendor-file
manifest and derive authenticated principals itself.

Both manifest records require enumerable own data properties for every supplied
pin. Hidden properties, symbol keys, unknown components and accessors are rejected
before host invocation; validation does not evaluate pin getters. Equivalent key
ordering and plain records with a null prototype remain supported.

```js
import { invokeAgentCreativeDevelopment } from '@jevelopers/agent-harness-runtime/development';

const result = await invokeAgentCreativeDevelopment({
  host: authenticatedDevelopmentHost,
  request: creativeApiRequest,
  source_manifest: authenticatedDevelopmentHost.source_manifest,
});
```

Output is labeled `evidence_mode: "development_only"`; it does not convert an
intercepted transport, synthetic authority, or mock provider into live evidence.
Actual dependency modes must remain visible in the host's existing telemetry.
There is no default live CLI and no implicit billable execution.

`GPT_IMAGE_ACCESS_MODE=codex_subscription` is an optional preference for development
tools only. It does not configure this driver, grant server API access, select an
image model, or substitute for `OPENAI_API_KEY`, an OpenRouter credential,
TypeSafe entitlement, or a Finance reservation. Production excludes this setting.
The harness selects no `GPT_IMAGE_MODEL` default. Deployed studio image execution
uses `TICKETSTUB_ART_MODEL`, with `GPT_IMAGE_MODEL` retained only as its direct
OpenAI migration alias; the image owner validates conflicts and provider support.

This change resolves the shared development/application execution boundary in
#23. The full AH-12 URL research, correction, image revision, approval, exact
artifact handoff, and authorized live journey remain open requirements. A thin
driver invocation or local test cannot close that issue.
