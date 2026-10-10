# JEV target and dataset readiness plan

## Current state

ACV1-JEV-TARGETS (issue #6) remains blocked. The required command validates owner-provided question definitions, targets, dataset references, and baselines. It does not call a classifier, choose thresholds, or mark a model ready for active routing. The checked-in target and dataset manifests intentionally contain no question IDs, labels, numeric limits, sample counts, model conclusions, or approvals. V2 additionally requires an externally pinned owner-evidence index; local `accepted` metadata cannot establish readiness.

The harness inventory labels below come from the issue #6 cross-repository closure audit. They are not engine question IDs or definitions. Research question definitions stay in event-intelligence, creative definitions stay in creative-engine, and the harness owns only its runtime eligible-action questions and the transport adapter. Each individual question must link one exact accountable definition leaf and accepted owner revision.

| Required inventory family | Definition owner role/repository |
| --- | --- |
| Source classification | Agent and research / event-intelligence |
| Relevance ranking | Agent and research / event-intelligence |
| Entity matching | Agent and research / event-intelligence |
| Factual/conflict triage | Agent and research / event-intelligence |
| Injection signal | Agent and research / event-intelligence |
| Brand-asset classification | Creative and fulfillment / creative-engine |
| Creative intent/routing | Creative and fulfillment / creative-engine |
| Quality/repair triage | Creative and fulfillment / creative-engine |
| Harness eligible-action ranking | Agent and research / agent-harness |

Every row must have a stable owner-issued question ID/version, the owner-declared Choice/Score/Noul primitive, references to the engine-owned eligible candidate set, structured evidence schema, and unknown/review/fallback behavior, sequential prerequisites, model/schema/policy pins, split hashes, coverage, numeric targets, cost rule, instrumentation/rollback owner, and owner review. The harness records references and evaluation metadata; it does not copy question instructions, candidate definitions, or evidence schemas. The pinned owner inventory's individual IDs must match the target records one for one. A broad family label cannot stand in for a missing individual question. An absent injection or brand-asset question is an explicit blocker.

## Required evaluation decisions

Question owners supply exactly four distinct ground-truth outcomes—positive, negative, unknown, and review—plus minimum train/validation/held-out sample counts, per-label and applicable-stratum coverage, a primary metric target, false-negative ceiling, abstention ceiling, p95 latency limit, and total downstream cost limit. Unknown `ground_truth` keys and row labels are rejected; only distinct eligible rows bearing one of those four validated labels count toward total, label, or stratum minima. No values are chosen in this repository. The question owner and Finance and reliability must approve the numeric values and exact cost basis before evaluation preparation can pass.

The dataset owner supplies sanitized source/provenance, collection and label protocol revisions, independent adjudication, privacy review reference, and disjoint SHA-256-pinned train/validation/held-out sample-reference lists. The local dataset-manifest format hashes sorted JSON reference rows (`sort_keys`, compact separators, UTF-8); it contains only sample IDs, content hashes, labels, eligible question IDs, and stratum tags, never source text or customer payloads. The validator checks both ID overlap and repeated content hashes across splits. Each split and the whole dataset subject must be bound to owner evidence. Finance and reliability must also approve the exact numeric evaluation budget envelope, scope, and currency before any separate paid evaluation; this readiness tool never authorizes or performs paid calls.

Each question compares a pinned deterministic baseline with a pinned generative baseline on the same frozen inputs, eligibility, split hashes, and approved budget-plan hash. Candidate input and eligibility hashes are derived from the exact per-question eligible sample IDs and content hashes in the owner-pinned dataset; caller-chosen matching hashes cannot substitute. Both baselines must match those candidate pins. Cost-stage completeness comes from a separate Finance and reliability metering-owner inventory. Every inventoried stage receives an explicit `required` or `not_applicable` decision with an owner-approved reason; equal caller-supplied lists cannot omit research, creative/image, inspection, or other induced billed work. Generative baselines require an exact model revision and configuration pin. Unknown cost must not become zero. No paid execution is authorized by this preparation plan.

Question owners explicitly classify URL quality, conflicting facts, absent evidence, malformed inputs, malicious sources, supported languages, research/creative routing, and artifact/permission contexts as required strata with numeric coverage or not applicable with a reason. Every dependent question names prerequisites and an evaluation batch; a dependent question and its prerequisite cannot share a parallel batch.

JEV scores and probabilities are evaluation evidence only. They cannot authorize budget, approve artwork, change permissions, or dispatch privileged actions. Current TypeSafe API documentation defines Noul as a yes/no question returning a `noul` value; Choice and Score answers include confidence derived from their probability distributions. The current [API reference](https://docs.typesafe.ai/api) and [confidence guide](https://docs.typesafe.ai/confidence) document this distinction. These docs establish the described interface, not account entitlement or live availability. The verifier requires `noul_has_provider_confidence=false` and forbids model confidence from serving as authority.

## Results, promotion, and rollback

An unfavorable result blocks activation or starts a follow-on redesign that preserves the original required target. Target changes require a new version and rerun; a target may not be lowered retroactively after results. A shadow-only result never counts as active-routing acceptance. Record negative findings, rollback owner, and rollback rule. The validator reports at most `evaluation_preparation_ready`, which means inputs and owner decisions are ready for an evaluation run; it says nothing about measured quality, live provider access, shadow/active rollout, or release acceptance. The manifest must link the exact ACV1-JEV-EVAL issue #22 as the pending execution follow-on; this does not claim that follow-on is complete.

## Owner actions still required

- Event-intelligence owners must publish per-question research definitions and exact owner leaves; issue #6 links EI-12P policy (#15), plus EI-06 (#16), EI-07 (#17), and EI-08 (#18) as implementation prerequisites. Issue existence is not approval.
- Creative-engine owners must publish per-question creative definitions and exact owner leaves; issue #6 links CE-Q01 policy (#8), CE-06 (#18), and CE-09 (#16) as candidate prerequisites. Issue existence is not approval.
- The Agent and research owner must publish the harness-owned runtime question IDs and pins.
- Research/data owners must provide a frozen, independently labeled and privacy-reviewed dataset with non-overlapping split hashes.
- Agent and research, Creative and fulfillment, and Finance and reliability must review the target, baselines, full downstream-cost treatment, negative-result policy, and rollback rule.
- ACV1-JEV-EVAL (#22) remains the execution follow-on; specialist acceptance remains with its owners. This discovery contribution cannot close classifier implementation or live/active evidence requirements.

The local `question_inventory` and pending manifests are an honest list of obligations. They do not invent engine question definitions. The family and stratum keys are only inventory labels drawn from issue #6; they cannot replace individual engine question IDs, owner definitions, or labeled evidence.

## Content trust and v2 compatibility

The external evidence index path and exact SHA-256 must come from protected runner configuration (`ACV1_OWNER_EVIDENCE_INDEX` and `ACV1_OWNER_EVIDENCE_SHA256`, or explicit CLI arguments) and remain outside the repository. Its owner-published evidence documents bind each approval to the exact subject content hash and revision: role reviews, question inventory and individual definitions, model/policy/target content, cost rules, dataset and each split, baselines, budget plan, and metering inventory. The index is an evidence transport only, not a new approval authority. A local status, issue URL, reference, hash, or revision cannot authenticate itself. Production readiness rejects evidence indexes, records, or documents explicitly marked synthetic or test-only. Missing or stale evidence blocks readiness.

Target and dataset descriptors moved from v1 to v2 to enforce these content identities, question-specific eligibility, candidate/baseline byte comparability, full metering inventory, and the pending issue #22 follow-on. V1 metadata is not silently migrated. Synthetic unit-test plans can receive `synthetic_test_only_validation_passed` with `ready=false`; this proves validator structure only and is never owner approval or evaluation preparation.

## Verification

```sh
python3 tools/validate_jev_targets.py --targets evals/jev/promotion-targets.json --dataset evals/jev/dataset-manifest.json
```

At this revision the command exits `2` with `status=blocked`; missing questions, owner leaf IDs, independent evidence, data hashes, coverage, numeric limits, candidate/baseline pins, complete metering inventory, budget approvals, and negative-result decisions are reported. The exit does not indicate a failed classifier. A future zero exit means evaluation preparation only, not classifier success, live execution, active routing, or global acceptance.

CI does not turn this pending result into a successful readiness result. Its classifier requires the checked-in manifests to produce `exit 2`, `status=blocked`, `ready=false`, zero candidate question definitions, all nine missing-family diagnostics, and the known owner-evidence/dataset/metering blockers. The uploaded artifact records that readiness is blocked; the green CI check means only that the expected external prerequisites are still accurately reported. Positive synthetic structure-only checks and adversarial rejection cases run separately from this classification.

The original acceptance meanings remain: AH-06 and ACV1-JEV-06 are scoped local requirements; CON-03 remains Agent and research; AGT-04 remains Agent and research; EVAL-02 remains Finance and reliability. Completing this preparation leaf does not verify those global criteria or change their owner roles.
