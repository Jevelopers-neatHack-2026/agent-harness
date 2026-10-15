# Private command guard fixture

`src/internal/private-command-guard.js` is an internal, offline authorization-boundary fixture. It is not exported from the package entry point and does not define the public authorization wire format, a canonical issuer, a customer-facing request schema, or an application service.

The setup owner supplies a current-decision resolver, a clock, a bounded local policy, and a lazy downstream factory. For each accepted command, the guard validates the exact action and resource, asks the resolver again, then requires an unexpired, unrevoked decision whose action, resource, tenant/project scope, membership revision, and grant revision match the current values. A caller-provided scope is only an assertion to compare with resolver output. Roles, capabilities, query fields, and unrecognized body fields are rejected; they cannot grant access.

The guard exposes only fixed result codes. Invalid input is `INPUT_INVALID`; a valid denial is `AUTH_DENIED`; resolver or clock uncertainty is `AUTHORITY_UNAVAILABLE`. The downstream factory and effect callback are not reached before an allow decision. The factory receives the verified action, operation, resource, and minimal scope. The callback also receives only normalized bounded payload fields. Factory and callback failures produce fixed codes without copying exception text, paths, identities, or stacks.

The local policy limits are supplied by the setup owner and validated as finite bounds: at most 4096 bytes in the normalized body JSON, 1024 text bytes, eight source references of at most 128 bytes each, 128-byte identifiers, ten variants, three media types, and twelve operations. The byte check runs after the input has already been parsed and normalized; it is not a transport parser or raw request-size guard. These limits serve this test fixture only; they do not establish a public or canonical project-request policy.

The tests use recording or throwing in-memory callbacks. They cover the selected C1 forged-scope and foreign/missing denial controls, selected C2 local normalization and bounds before resolution, and selected C4 expiry/revocation and status/resume/cancel authorization checks. They also check exact action/resource binding and that denied calls do not construct downstream capabilities or invoke any of the nine business-effect callbacks. This is not execution of the proposed 26-vector spec: it does not select or implement the candidate shared request limits or older-version behavior, C3 replay/correction/reconnect, C4 durable ordering/reopen, or public result projections. Test code starts no application worker or child process and constructs no persistent store, network client, provider, database, or live issuer.

This fixture does not implement atomic authorization with a real downstream effect. The trusted resolver and downstream owner must define that integration boundary. It does not provide durable replay protection, reconnect/reply processing, public adapters, canonical agreement, customer identity, finance authority, or provider behavior. The remaining original AH02 acceptance cases and external integration gates stay open.

The [STATUS host agreement proposal](status-host-agreement.md) records the unaccepted narrow read-only slice and the exact owner inputs still needed; it does not promote this fixture into a public command or grant.

Run the focused fake-only suite with Node 22.23.3 or later in Node 22:

```sh
node --test --test-concurrency=1 tests/authorization/private-command-guard.test.js
```
