# TypeSafe host integration

The public `@jevelopers/agent-harness-runtime/decisions` entrypoint provides one
canonical DecisionPort batch through the existing private TypeSafe JEV transport.
The application injects `TYPESAFE_API_KEY` on the server, current administrator
eligibility evidence, and the existing creative-engine contract validators. This
module does not read environment variables or select credentials from image
providers. A key name, a key value, or a readiness report cannot establish an
executed TypeSafe decision or account/model access.

The current consumer is creative-engine's public API: its generate/edit
`decision_request` and inspection routing call `resolveRoute` and then `execute`.
The application owns authenticated scope and composition. Creative-engine owns
the canonical request/result contract, job and rights checks, C03 reservation,
durable dispatch intent, usage observations, and recovery. Finance owns spending
permission and reconciliation. This port never reserves, settles, releases a
hold, approves artwork, or retries a decision.

## Composition

```js
import { createJevDecisionPort } from '@jevelopers/agent-harness-runtime/decisions';
import { validateDecisionRequest, validateDecisionResult,
  decisionExecutionTargetHash } from 'creative-engine/contracts/decision-port/v1';
import { validateMeteredAttempt } from 'creative-engine/contracts/metering';

const decisionPort = createJevDecisionPort({
  apiKey: serverSecrets.TYPESAFE_API_KEY,
  environment: hostEnvironment,
  eligibility: ownerConfiguration.jevEligibility,
  contracts: { validateDecisionRequest, validateDecisionResult,
    validateMeteredAttempt, decisionExecutionTargetHash },
});
// Pass decisionPort to the same createCreativeApi composition as image execution.
```

Eligibility has explicit `provider: "typesafe"`, `model: "jev-1.13.0"`, matching
`environment`, `accountRef`, `projectRef`, `evidenceRef`, `expiresAt`, and
`eligible: true`. These are references to independently supplied administrator
evidence, not proof produced by this package. Missing, denied, mismatched, or
expired records fail closed. Preview and production must each have their own
configuration and credential injection; the port rejects environment mixing.

Eligibility expiry and execution deadlines must name real UTC calendar times in
`YYYY-MM-DDTHH:mm:ssZ` form, optionally with one to three fractional digits before
`Z`. Impossible dates and invalid clock fields fail closed before transport.
Eligibility is expired at its exact expiry time; an exact execution deadline
returns a timeout with unknown usage and no dispatch.

`resolveRoute` returns TypeSafe, `jev-1.13.0`, the fixed
`https://api.typesafe.ai/v1/systemone` surface, direct-HTTP client identity
`agent-harness-http` version `0.1.0`, and the single-send retry mapping. No moving
model alias, configurable URL, alternate provider, or subscription fallback exists.

Before reserving, the consumer prepares the immutable envelope and computes its
owned `decisionExecutionTargetHash`. Only the later reservation reference is
excluded from that target. The consumer then validates the complete canonical
envelope using the real Finance reservation and passes the admitted
`target_binding_sha256` separately to `execute`. The port recomputes and compares
that target; changing the state, questions, scope, model, deadline, identities, or
policy cannot consume the admitted grant. No placeholder reservation is used.

`execute` accepts the exact consumer attempt/reservation/dispatch context, one
independent question batch, and `attempt_number: 1` / `attempt_limit: 1`.
Route selection rejects unsupported sequential decisions before the consumer can
reserve for a request this single-batch port cannot execute.
Dependencies require a later owner-managed operation; this executor cannot
silently send additional billable batches. The request, attempt, scope, model,
environment, billing mode, reservation, and target must agree. The transport
remains bounded by the earlier of the consumer deadline and eight seconds.

After DNS and authenticated TLS, immediately before the HTTP write, the port
awaits the uncached owner `revalidateDispatch` and `recordTransportAttempt` fences.
Both must return the current exact dispatch intent reference. Cancellation,
expiry, deadline, denied fences, or a stalled callback prevent the write. A late
callback after cancellation cannot send. These callbacks must be the real owned
consumer functions, backed by durable job/Finance state; substitutes do not
establish deployed integration or distributed execution authority.
The transport then invokes the consumer's synchronous `assertDispatchCurrent`
against current owned job/lease/context state, with no further await before the
HTTP write. An async permit that became stale during a continuation cannot send.

## Responses and recovery

The existing wire decoder validates Choice, Score, and Noul. The injected
canonical validator applies the stricter contract and policy checks. Noul never
receives fabricated confidence. Low confidence or a Noul probability inside the
configured uncertainty band returns `review`. A favorable result still grants no
authorization or artifact approval.

Malformed responses preserve validated actual token counters when available.
An unexpected or missing resolved model retains those counters as incomplete
usage with `provider_model_unverified`, so Finance cannot settle a guessed model.
Missing counters, HTTP failures, connection loss, timeout, and cancellation retain
an explicit unknown usage record. Neither a failed HTTP status nor a pre-write
local failure proves that a provider call was free. The consumer's C03 outbox and
Finance path persist usage-or-unknown and keep uncertain attempts held for
reconciliation. Recovery must inspect that durable state rather than invoke the
port again with a reused billable attempt. This module does not implement a second
ledger or use its local result as settlement evidence.

Provider error content and credential values are never included in typed errors.
The returned provider request reference is hashed. Public results carry only the
canonical contract fields; admission targets remain separate execution context.

## Validation and deployment

```sh
npm ci --ignore-scripts --no-audit --no-fund
npm run test:decisions
npm run typecheck
```

Use Node `22.23.3` and the lockfile. The test-only creative-engine source archive
is pinned to an immutable commit and integrity; runtime consumers inject their own pinned owner
package. Tests intercept native DNS/HTTPS while invoking the real transport and
actual owner validators. They make no provider request and establish no live
account eligibility. Application tests must also traverse the actual CE API,
durable storage, Finance adapter, and recovery path with its tested component pins.

The focused harness assembly pins creative-engine
`e4fa66488f31577f516b27238e945a1fb3c2143f` through its exact source archive and
lockfile SHA-512 integrity. A fresh npm cache installation verified that archive
and matched the owner helper, declarations, API/runtime and C03 source hashes.
The actual `decisionExecutionTargetHash` is used in every public-port test;
there is no substitute target-hash callback. The complete returned port is
structurally assigned to `CreativeRuntimePorts["decisionPort"]` in strict
TypeScript, including the admitted target and synchronous owner fence.

The focused suite passes 27 tests at this pin. The earlier full owned runtime
regression passed 443 tests at CE source commit
`6831f02fa86a670b592cb7fbaa391d1b8d542cee`; that receipt remains attributed to
that source. The current pin includes the CE owner's inspection replay/repair
and internal snapshot-boundary corrections. Both harness suites have zero skips;
strict port assignability and SDK lock-inventory checks also pass at the current
pin. Required PR evidence binds the actual immutable merge candidate and its
evaluated base separately from a passing source-push run.
The admission tests cover an unreserved owned target, the later complete
reservation envelope, material post-admission mutation rejection and the final
reservation binding. These are intercepted component checks, not administrator
eligibility, actual Finance SQL integration, live inference, or deployed evidence.

Deploy compatible CE target preparation, this harness port, and application
composition together before enabling the TypeSafe route. The deployment manifest
must record the exact tested commits and copied-file hashes. Roll back the
application route and its matching component manifest as one assembly; retain
durable job/observation rows and financial holds. Do not resend unknown attempts.

Authorized live verification remains pending: an administrator must supply
environment-specific account/model eligibility evidence and server-held key,
Finance must authorize a bounded reservation, and the host must run one synthetic
decision through the actual public consumer. Verify its requested/resolved model,
single transport attempt, durable request/target/usage attribution and Finance
settlement or explicit held-for-reconciliation outcome. Offline tests, a model
listing, or a merged PR do not satisfy issues #4/#15 or the live journey in #23.

The [TypeSafe API reference](https://docs.typesafe.ai/api),
[model inventory](https://docs.typesafe.ai/models), and
[confidence contract](https://docs.typesafe.ai/confidence) were inspected on
2026-10-12. They still document the fixed System One endpoint, versioned
`jev-1.13.0`, per-question answer shapes, token usage, and Noul without a separate
confidence field. This confirms interface compatibility; it does not confirm
this deployment's credentials, entitlement, quota, or executed inference.
