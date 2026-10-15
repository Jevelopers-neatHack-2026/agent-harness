# STATUS host agreement — proposal 0.1

**UNACCEPTED. Documentation only.** This version proposes one read-only command;
it supplies no public port, permission, schema adoption or consumer acceptance.
Refs [AH02 #13](https://github.com/Jevelopers-neatHack-2026/agent-harness/issues/13).
The [3.A decision][scope-decision] authorizes preparing a narrow agreement first;
the [ownership clarification][owner-decision] names **@CompleteDotTech** as the
harness and agreement owner. **@lmvdz** coordinates the application issuer and
verification slice with that owner and the shared-contracts delivery owner.

## One proposed command

**STATUS: return one bounded state/version snapshot for one existing workflow
already bound by the host to the verified account's tenant and project.** This is
a proposed semantic command name, not an endpoint, wire enum or issued grant.
It cannot create a workflow, list/search workflows, stream progress, follow
cursors, retrieve sources or read private artifacts. The snapshot excludes
prompts, evidence contents, artifact identifiers/URLs, approval material, customer
orders and financial records. Its exact request, state/version and error schemas
are still absent; this document does not invent their fields.

| Boundary | Proposed rule for this slice | Decision still needed |
| --- | --- | --- |
| Trusted identity | Use the issuer-verified account and selected project. Guest, anonymous and projectless/platform access are excluded. Preserve the selected context version's actor kind/id, tenant id and project kind/id/ref exactly; caller scope, roles and correlation IDs supply no authority. | Select the context version and an exact host-owned workflow-to-tenant/project binding. No UUID conversion, truncation, prefix removal or implicit revision-string codec. |
| Modes | Only an explicitly configured harness development/test lane, producer economic mode `none`, and fixture/synthetic evidence are proposed. Each axis remains separate; version 1's combined `mode` cannot imply the missing economic axis. Unknown or mismatched axes fail closed. | Record the exact producer-to-harness environment, billing and evidence mapping. The producer uses `development/preview/live`; this is not the harness's `development/test` vocabulary. No preview-to-test or Finance adapter mapping is adopted here. |
| Action and resource | Fresh authority must allow this one STATUS read of this exact existing workflow within its trusted tenant/project binding. `agent.use`, project read capabilities, a role or successful token verification alone do not supply that grant. | Producer and harness owners must identify the actual action/resource grant and its current-state source. **Grant identifier: ABSENT.** No new capability name is proposed. |
| Private artifacts and effects | **No private-artifact lookup/read permission.** No enqueue, mutation, billing reservation/dispatch, provider call or artifact download is reachable from the slice. | Confirm that the agreed status projection can be produced without those capabilities. Any artifact-bearing response requires a separate agreement. |
| Freshness and revocation | Reverify on every STATUS invocation, including retries; do not reuse a previous allow. Bind the verified identity, exact action/resource and current membership/grant state to the read. Expired, revoked, stale or uncertain authority produces no protected result. | Pin the issuer/verifier and audience, bounded verification timeout/age, and the owner-controlled consistency point between verification and status projection. Token expiry alone is not a revocation protocol. |
| Bounded refusal | Reject unsupported commands/versions, unknown fields, malformed or oversized input before protected retrieval. Denied foreign and unknown workflow references have the same outward projection; return no existence signal, raw issuer response, exception, token or private identifier. | Agree exact byte/count limits and safe request/result/error fixtures, including a deny-before-read spy and positive control. No public error-code mapping is adopted. |

The proposal asks for a narrow read and its authorization/projection consistency
point, not the full lifecycle protocol. It does not authorize a production
verifier call or provision any issuer credential.

## Observed inputs, kept separate

The application snapshot is
[`783a9fa03e3d64b752cd8c5ad913c223db29118d`][app-tree]. Its producer supports
`RequestContext` (`ticketstub.context/1`) and `ContextV2`
(`ticketstub.context/2`). Version 2 has kind-prefixed actor/tenant identities,
project `ref`, audience binding and separate `environment`, `economicMode` and
`evidenceMode`. The existing default audience is `ticketstub.finance`; it is not
a harness audience. The selected context version and harness audience for this
proposal remain **ABSENT**.

The current [`POST /api/context/verify`][app-route] calls
`verifyContextTokenLive`. The account path checks current session and membership;
version 2 also checks project membership, audience and current modes. The offline
verifier needs supplied current state to test stale/revoked behavior. Neither
path currently establishes the missing STATUS action/resource agreement. The
fixture index retains 15 original cases, 24 version-2 cases and a separate
malformed-shape corpus. These are producer fixture inventory, not STATUS consumer
acceptance; the Finance audience/adapter fixtures cannot be relabeled as harness
fixtures.

Harness source is pinned to
[`2a59ca7c4ab0b8ceeae9652b74fc10a81e048346`][harness-tree]. The existing
[`private-command-guard.js`][guard-source] is an unexported offline fixture. Its
synthetic `job.status` action, `job` resource, string revision fields and fixed
result codes do not define the proposed public STATUS protocol. Its success
result is `EXECUTED`, not a status snapshot. The package's runtime store,
`/ticketstub` telemetry exporter, `/decisions` port and `/development` invocation
remain their existing separate modules; none is an accepted authenticated STATUS
adapter. In particular, exposing `DurableWorkflowStore.getWorkflow` directly
would not supply the missing host authorization boundary.

The selected [private shared-contracts 0.9.8 handoff][sc-handoff] is separately
reported by its owner as source
`04e506c4bf1d099b8d1638b719eedd1675175caf`, tree
`dd47aa6edd9b2d2bfef9f6e736b80118c6e7209f`, archive SHA256
`2a6048628c4018bb3db4d85b2941ef5b8702d9aa1be09df3a4c02f4b4e0526d1`.
Those private bytes have not been supplied to this checkout. They are not
replaced by public main, another version or copied local DTOs. No STATUS schema,
envelope closure or fixture from that archive is asserted accepted or tested.

### Source inventory

These are immutable **Git blob/tree identities**, not accepted fixture SHA256s.
Application paths use the application revision above; harness paths use the
harness revision above. The source links and inventory were inspected read-only.

| Repository / path | Git object identity |
| --- | --- |
| app [`src/server/context-token.ts`][app-token] | blob `d50ce68470f7d30f2a68bfb70fa7ae240055191e` |
| app [`src/server/context.ts`][app-context] | blob `c4dec5ed31f19bba69ee29fee7040a1f12a62c2b` |
| app [`src/server/context-modes.ts`][app-modes] | blob `c4aa162eaffab394b570ea9f2389ab73a23577ee` |
| app [`src/app/api/context/verify/route.ts`][app-route] | blob `964514d12ef129e1be833fa094e86ada8cddfc40` |
| app [`fixtures/context/index.json`][app-fixture-index] | blob `3804d5978a52a21d1a001ec5d3d39b17d3b9daaf` |
| app [`fixtures/context/verify.mjs`][app-fixture-verifier] | blob `6fda379504d748ac28933d7a6158a8de5121bd57` |
| app [`fixtures/context/`][app-fixtures] | tree `4d5ce225d29a22869a71670864bfb0b48a66449c` |
| harness [`src/internal/private-command-guard.js`][guard-source] | blob `8edb4e0dbab466e7dca1163f12fd6b8971d31d73` |
| harness [`tests/authorization/private-command-guard.test.js`][guard-tests] | blob `9aaaffa14e8e11c0c07dc529c69af52cc46cd759` |
| harness [`package.json`][harness-package] | blob `989eb8628d94f6f5893fbb894631062c8e612357` |

## Record required to accept this slice

Only decisions applicable to this STATUS slice are needed. The following fields
remain explicitly **ABSENT**, not silently defaulted or counted as passes:

| Owner | Missing reviewable record |
| --- | --- |
| @lmvdz, application issuer/verification | Selected context schema; verifier deployment/revision and audience; current exact STATUS action/resource authority, identity binding, freshness/revocation and timeout behavior. |
| @CompleteDotTech, harness/agreement | Exact workflow binding and bounded status projection; development/test, billing and evidence mapping; authorization/read consistency point and safe refusal policy. |
| Shared-contracts delivery owner with affected producer/consumer owners | Exact selected request/status/error schemas and transitive envelope closure, version and artifact hashes; supported older/unsupported versions and unknown-field verdicts. No unrelated command schema is a prerequisite for this one read. |
| Application producer and harness consumer | Sanitized STATUS fixtures and each SHA256/expected verdict; actual tested producer SHA/tree and consumer SHA/tree; runtime/modes; commands, output digests, negative controls and explicit scoped producer ACK and consumer ACK. All these STATUS-specific evidence fields are absent. |

The inspected source revisions and the local guard result below do not populate
the absent tested STATUS producer/consumer fields. @CompleteDotTech can record an
explicit disposition on this version with the affected owners; publishing or
merging this proposal does not constitute that acceptance.

## Compatibility and negative verification recipe

The existing harness-only check is runnable now with Node 22.23.3:

```sh
node --test --test-concurrency=1 tests/authorization/private-command-guard.test.js
```

Executed during proposal preparation on clean harness base `2a59ca7`: **13/13
passed, zero skips, exit 0**, in a clean environment with in-memory fake callbacks.
This exercises selected local C1/C2/C4 controls only. It starts no provider,
application server, worker, database or live issuer. It does not test the proposed
STATUS snapshot or producer/consumer compatibility.

In an isolated checkout of the exact application source above, the producer's
existing offline command is:

```sh
node fixtures/context/verify.mjs
```

**Not run by this documentation change.** Preserve each existing fixture's own
`expect` verdict and `verifyAt`; do not regenerate or rename Finance fixtures into
STATUS evidence. A fixture-format pass would still not satisfy the missing
harness grant, audience, schema or adapter.

After the four owner records above are supplied, exercise the exact producer and
actual proposed consumer together with network/provider effects disabled:

| Cases to bind to supplied STATUS fixture hashes | Required observation |
| --- | --- |
| Authorized account/project/workflow positive; repeated read after authority changes | One bounded status projection for the permitted read; fresh verification each time; no cached allow after revocation/expiry/membership or grant change. |
| `tenant-a-owner`/`tenant-b-owner`, `cross-project-a`/`cross-project-b`, swapped workflow and private-artifact references | Label any perturbations synthetic; denied foreign and unknown references share the same safe result; protected-read/effect spies remain zero. A valid producer token alone never becomes an allow. |
| Forged, malformed, expired, stale, revoked, unsupported-version; version-2 wrong audience and removed project membership | Apply the original producer verdicts, then the agreed bounded consumer projection; uncertainty or verifier timeout performs no protected read. |
| Selected current/older schema, unknown fields, oversized IDs/body, unsupported command/media; identity kinds with the same raw id | Exact agreed pass/refusal verdicts; no coercion, UUID mapping, truncation or cross-kind collision; no partial retrieval. Existing producer-valid length cases may still require a bounded consumer refusal. |
| Authorization revoked between verification and projection; a status request asking for source/artifact/financial detail | Exercise the agreed consistency point and exclude the forbidden projection/effects; positive controls must prevent a deny-all adapter from appearing correct. |

The STATUS consumer command, its accepted schemas/fixtures and the grant/mode
mapping do not exist in this agreement. Consequently that joint recipe **cannot
execute yet**; no placeholder command or skipped case is a pass. The issuer and
contract owners supply the exact missing inputs, then the harness owner can
implement the bounded adapter and return immutable evidence to those owners.

## Original AH02 work remains required

All four [issue #13 cases][issue-13] remain open: (1) forged tenant/project/artifact
scope denied without existence leakage; (2) bounded supported input before
retrieval/billing; (3) scoped duplicate create/cancel/reply, conflicting correction
and reconnect cursor behavior; (4) expired status/resume plus durable cancel versus
success consistency. Create, cancel, reply, correction, resume, progress/reconnect,
immutable fact revisions and dependent approval invalidation are outside this
proposal and remain required, together with durable unknown-effect/financial-hold
behavior. This slice does not close #13, its epic, any gate or a global acceptance
criterion. Whole upstream issues and the entire lifecycle need not close before
owners can agree this precise read.

[issue-13]: https://github.com/Jevelopers-neatHack-2026/agent-harness/issues/13
[scope-decision]: https://github.com/Jevelopers-neatHack-2026/agent-harness/issues/13#issuecomment-6118253569
[owner-decision]: https://github.com/Jevelopers-neatHack-2026/agent-harness/issues/13#issuecomment-6118434881
[sc-handoff]: https://github.com/Jevelopers-neatHack-2026/agent-harness/issues/5#issuecomment-6123206974
[app-tree]: https://github.com/Jevelopers-neatHack-2026/getticketstub.com/tree/783a9fa03e3d64b752cd8c5ad913c223db29118d
[app-token]: https://github.com/Jevelopers-neatHack-2026/getticketstub.com/blob/783a9fa03e3d64b752cd8c5ad913c223db29118d/src/server/context-token.ts
[app-context]: https://github.com/Jevelopers-neatHack-2026/getticketstub.com/blob/783a9fa03e3d64b752cd8c5ad913c223db29118d/src/server/context.ts
[app-modes]: https://github.com/Jevelopers-neatHack-2026/getticketstub.com/blob/783a9fa03e3d64b752cd8c5ad913c223db29118d/src/server/context-modes.ts
[app-route]: https://github.com/Jevelopers-neatHack-2026/getticketstub.com/blob/783a9fa03e3d64b752cd8c5ad913c223db29118d/src/app/api/context/verify/route.ts
[app-fixture-index]: https://github.com/Jevelopers-neatHack-2026/getticketstub.com/blob/783a9fa03e3d64b752cd8c5ad913c223db29118d/fixtures/context/index.json
[app-fixture-verifier]: https://github.com/Jevelopers-neatHack-2026/getticketstub.com/blob/783a9fa03e3d64b752cd8c5ad913c223db29118d/fixtures/context/verify.mjs
[app-fixtures]: https://github.com/Jevelopers-neatHack-2026/getticketstub.com/tree/783a9fa03e3d64b752cd8c5ad913c223db29118d/fixtures/context
[harness-tree]: https://github.com/Jevelopers-neatHack-2026/agent-harness/tree/2a59ca7c4ab0b8ceeae9652b74fc10a81e048346
[guard-source]: https://github.com/Jevelopers-neatHack-2026/agent-harness/blob/2a59ca7c4ab0b8ceeae9652b74fc10a81e048346/src/internal/private-command-guard.js
[guard-tests]: https://github.com/Jevelopers-neatHack-2026/agent-harness/blob/2a59ca7c4ab0b8ceeae9652b74fc10a81e048346/tests/authorization/private-command-guard.test.js
[harness-package]: https://github.com/Jevelopers-neatHack-2026/agent-harness/blob/2a59ca7c4ab0b8ceeae9652b74fc10a81e048346/package.json
