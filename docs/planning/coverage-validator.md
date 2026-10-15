# ACV1 coverage validator

`scripts/validate-agent-creative-coverage.py` checks an explicitly supplied coverage-map snapshot against its source pins, an externally supplied canonical checklist, timestamped issue readbacks, and (optionally) a release manifest. It uses only the Python standard library, performs no network calls, and writes no files. It emits one deterministic JSON report to stdout and exits nonzero when required checks are blocked.

The checked-in [coverage input pin reference](../../planning/agent-creative-v1/coverage-input-pins.json) records dated source identities and observed dimensions. It contains no coverage rows, issue bodies, or canonical criteria. Its historical map pin is incomplete and has no fresh issue-readback pin; it is not a ready-to-run authorization or a current acceptance source. A caller must supply fresh, exact inputs and a run-specific pin manifest before a planning result can pass.

## Commands

Run the focused synthetic suite:

```sh
python3 -B -m unittest discover -s tests/planning -p 'test_coverage*.py'
```

Planning validation requires explicit input paths:

```sh
python3 scripts/validate-agent-creative-coverage.py /caller/path/coverage-map.json \
  --source-pins /caller/path/source-pins.json \
  --canonical-checklist /caller/path/acceptance-checklist.json \
  --issue-readbacks /caller/path/issue-readbacks.json \
  --check-urls --check-dag --require-specialist-leaves
```

The release form adds a caller-supplied evidence manifest:

```sh
python3 scripts/validate-agent-creative-coverage.py /caller/path/coverage-map.json \
  --source-pins /caller/path/source-pins.json \
  --canonical-checklist /caller/path/acceptance-checklist.json \
  --issue-readbacks /caller/path/issue-readbacks.json \
  --check-urls --check-dag --require-specialist-leaves \
  --release /caller/path/release-manifest.json --require-evidence
```

URL/readback and graph checks are enabled by default; `--no-check-urls` and `--no-check-dag` are explicit diagnostic opt-outs. A report with either required check omitted remains `BLOCKED`. Use both checks for planning or release qualification. `--require-specialist-leaves` requires at least one required leaf in each of the agent-harness, event-intelligence, and creative-engine families; release mode always applies that requirement. A CLI release invocation requires `--require-evidence`, and release mode always validates its evidence manifest.

Evidence paths in the release manifest are opened exactly as supplied (relative paths resolve from the process working directory). There is no repository search, fallback directory, or path discovery. The validator follows no final-component symlinks. It reads only the four named JSON inputs and the evidence files named by the release manifest.

## Input and resource contract

JSON object keys must be unique at every depth. Each root must be an object, each adapter alias and record family must be explicit, and malformed inputs return fixed diagnostic codes and stable record keys rather than source payloads or exception text. Input streams are read with finite bounds before parsing; evidence files are hashed in 64 KiB chunks.

| Input | Maximum |
| --- | ---: |
| Coverage map JSON | 8 MiB |
| Canonical checklist JSON | 1 MiB |
| Issue readbacks JSON | 16 MiB |
| Source pins or release manifest JSON | 2 MiB each |
| Each evidence file | 256 MiB |
| Total evidence bytes | 1 GiB |
| Coverage/readback/canonical entries | 10,000 each |
| Contribution rows, ownership rows, graph and staged-edge rows | 100,000 each |
| Release revision/evidence/receipt rows | 10,000 each |
| Blocker owner/action text | 512 characters each |
| Reported blocker rows | 250 (the report marks truncation) |

The evidence reader accounts every returned read byte against the aggregate limit, including bytes read before a later read, digest, size, or close failure. It may consume one additional byte as a bounded overflow detector, then stops reading later evidence files. A release report records `evidence_bytes_read` (including such a detection byte) separately from `verified_evidence_bytes` (regular evidence files fully read, size-checked, digest-matched, and closed). The latter is a content-read metric, not evidence of agreement or a passing release. These are parser and resource bounds, separate from program scope. The observed 59 required leaves, 555 contribution rows, 91 mapped requirement IDs, 47 canonical criteria, and 16 criteria outside the scoped map are snapshot dimensions, not fixed quotas. Distinct contribution rows remain distinct even when they reference the same requirement ID. The validator compares every recomputed dimension to the caller's pin and accepts a later, correctly pinned snapshot with different counts.

The original pin manifest uses `acv1.coverage-source-reference/v1`. It pins the exact map bytes by SHA-256, the canonical checklist by Git blob ID and byte count, and issue readbacks by SHA-256, capture ID, timestamp, observed source revision, and page-completeness flag. It names the source adapter, repository, revision, and path. A missing, unknown, repeated, incomplete, or mismatched source role blocks validation. The digest proves byte identity, not source authorization or signer identity.

The original local map adapter is `agent-harness.coverage-map-current/v1`; map records require explicit `record_type` values (`implementation_leaf`, `delivery_leaf`, `epic`, `gate`, `release_gate`, or `program_gate`). The captured candidate does not yet include this adapter version or explicit record classification, and its historical pin marks it incomplete. The validator will report that candidate as blocked rather than infer types or rewrite its source. The canonical adapter is `integration-evals.acceptance-checklist/v1`; its records remain caller-supplied and are never copied into the product tree. The readback adapter is `agent-harness.issue-readbacks/v1`; it describes exactly what its complete paginated snapshot observed at its capture time, not present-day URL reachability.

## Versioned directional projection

The optional `scripts/project-agent-creative-coverage.py` creates `agent-harness.coverage-projection/v2` from explicit local source files. It does not fetch inputs, modify source files, or copy canonical criteria into the output. Its `acv1.coverage-source-reference/v2` pins the map, pair review, staged-relation review, external-artifact list, issue readbacks, canonical checklist, and canonical revision reference by byte length and SHA-256; the canonical Git blob identity and both caller-supplied revisions are also recorded. Use new output paths because the helper refuses to overwrite them.

The v2 projection keeps each source contribution row and pointer, and derives its role from separately declared accountable-delivery and required-contributor pairs. It carries source pair-review rows, reviewed typed extras, proposed additions, and unresolved associations as distinct data. A recommended association is not inserted into the authoritative rows. Missing or extra actual pairs, undeclared roles, stale pointers, or source-dimension mismatches block validation. The v2 graph stores directional consumer and producer requirement IDs independently, records external endpoints separately from repository leaves, and preserves all six stage labels. The report retains validated JSON Pointer provenance for stage and external-artifact obligations. Pointer output is bounded to 4,096 ASCII characters and the source adapter's safe path-token alphabet with RFC 6901 `~0`/`~1` escapes; malformed values receive a fixed `<invalid>` label while the source validation issue remains visible. The validator computes waves, reachability, and critical path from the graph; it checks that a copied source critical path is an actual longest path, allowing a different valid tie-break.

The source map's `tests` member is preserved as historical execution evidence; it is not treated as the complete required-test declaration. A v2 projection must include `required_test_declaration_status`: `declared_in_source` requires an explicit `required_tests` list, while `not_declared_in_source` requires an empty `required_tests` list and reports the required-test gap. Missing or invalid status and a missing declared list block validation; neither can fall back to historical `tests`. The v1 map validator retains its original input semantics. This keeps previous test results visible without converting them into future release requirements.

Issue readbacks v2 are capture-scoped and must include the exact issue identity, title, state, body digest, and `observed_revision` value pinned for that capture. The projection retains the historical body digest and stores the readback independently. A changed body, title, or state stays blocked until a source-semantic review binds both old and current values and explicitly states that implementation scope and external acceptance were not changed. Hashing a body does not establish semantic equivalence.

Planning reports expose `external_artifact_obligations` separately from local issue leaves. Each row retains its external owner, deliverable, status, declared-action status, action (or `null` when the source did not declare one), and source pointers. A deliverable is not silently substituted for an action. `stage_relation_obligations` preserve directional stage, endpoint requirements, applicability, provenance, and status. Pending external artifacts and applicable unsatisfied relations remain visible; they do not become whole upstream issue closure or accepted canonical evidence. A planning result may therefore be structurally `BACKLOG_READY` while a required release remains blocked.

The original v1 shared-edge vocabulary and checks remain isolated from v2. V1 accepts only `fixture`, `contract`, `runnable`, and `final_acceptance`; its same-requirement-at-both-endpoints rule is unchanged. V2 uses the source-declared endpoint IDs and does not reinterpret v1 edges. Release v2 requires exact directional relation evidence, the consumer outcome's declared stage, matching content digest, revision and mode, and an accepted external artifact when that relation names one. A synthetic positive release test exercises validator behavior only; it does not assert that any real external owner accepted a contract or artifact.

## Checks and status boundaries

The report keeps `planning_status`, `coordination_status`, `publication_status`, and `release_status` distinct. A structurally valid planning result may show pending owner/action blockers; those blockers remain visible and prevent a required release outcome from passing. A pending coordination or publication state is not silently promoted and is not itself a substitute for scoped release evidence. `BACKLOG_READY` means the supplied planning structure reconciles; it does not close the coordination issue or establish feature completion.

Every required leaf has a unique key, explicit type, repository, owner role, priority, requirement IDs, and a pinned issue identity when a URL is known. Required URLs are matched to the readback by repository, issue number, title, state, and body digest; issue-number text is bounded before integer conversion. A null URL remains unknown and requires a concrete owner/action blocker; the validator never guesses a URL. A closed readback cannot be hidden behind an active map state, and a map marked closed cannot have an open readback. Closed leaves need an exact commit and the leaf's complete required test set, but a valid closed leaf does not need a replacement. Deferred and not-planned required leaves do not count. A replacement is recognized only when both records name one another.

Ownership projection is one row per unique requirement ID. Contribution records have independent unique row IDs, preserve row multiplicity, and reconcile to a declared leaf and owner. For each requirement, every declared required contributor must have at least one valid row; duplicating another contributor's row cannot hide a missing contributor. Scoped contributions to a global criterion retain its canonical ID, title, priority, owner, verification, and evidence requirement; they never claim the global criterion is complete. Canonical criteria outside the local map remain counted.

Execution edges are a typed DAG over actionable leaf prerequisites, checked iteratively. Required gate fan-in is compared with the explicit gate predecessor set; a valid positive fan-in is permitted. Evidence-stage edges are checked separately and cannot satisfy or rewrite execution prerequisites. A gate may collect stage evidence without becoming an implementation-leaf dependency. The local adapter recognizes the explicit stages `fixture`, `contract`, `runnable`, and `final_acceptance`, and edge states `pending` and `satisfied`. Each stage edge explicitly marks whether it is required for v1 and identifies the source requirement IDs it relates to; a required edge must relate to requirements mapped by both endpoint leaves. A release outcome must name the exact `evaluated_stage` declared for its leaf as `required_release_stage`; an outcome cannot choose a lower stage to bypass an applicable prerequisite. For a satisfied required relation at the selected consumer stage, the source map must retain its exact declared `required_owner_receipt_ids` list. Internal-relation receipts are attached to the producer outcome; an external relation's declared receipts are attached to its consumer outcome, since the external artifact has no local producer outcome. Each required receipt must have accepted status, the exact relation and endpoint binding, and content-hashed receipt evidence. External-relation receipts and ordinary outcome receipts remain bound to their outcome revision. An internal-relation receipt is bound to the selected producer artifact's exact evidence ID, content digest, mode, and tested revision; this supports a separately versioned prerequisite artifact while the producer outcome stays at an earlier revision. That narrow revision exception applies to the relation-bound obligation only. If the same receipt is also required directly by the producer leaf, a required staged edge, or a distinct applicable relation, the independent declaration remains bound to the producer outcome revision. Same-revision evidence may satisfy overlapping declarations normally. A receipt-free relation remains receipt-free.

Directional stage proof also binds the selected consumer evidence to the consumer outcome and the selected producer artifact to an explicit `stage_binding` object containing the exact relation ID, producer leaf, producer requirement IDs, and prerequisite stage. Producer artifacts are content-hashed and must name a tested producer revision and allowed mode. They may be a separately identified prerequisite artifact rather than evidence listed on the producer's outcome; when also listed there, its revision and mode must match that outcome. This allows a required stage artifact to satisfy its own declared relation without requiring the producer issue to be closed or its outcome to claim a later stage. A pending or malformed applicable required relation still fails closed; an optional pending relation and the checked-in pending `final_acceptance` relation to the release gate do not block a leaf's separate `runnable` result. These structural checks do not authenticate the evidence producer or establish owner agreement.

Release validation requires each required outcome's exact tested repository revision, required tests, content-addressed evidence files, required mode, prerequisite status, and only those owner receipts explicitly named on its source leaf or applicable stage edge. Model and policy requirements come from the pinned leaf; the release outcome must repeat any declared expectation exactly, and its evidence must match it. Removing or changing an outcome field cannot disable a map requirement. Leaves without a model/policy requirement do not need fabricated pins. Pending blockers, stale revisions, absent or changed evidence, skipped required tests, missing required receipts, mismatched stages, and unresolved applicable stage prerequisites fail closed. Mock, recorded, sandbox, and shadow modes remain distinct from live. Successful digest/schema validation does not authenticate an owner, establish an actual provider or financial action, or prove that a claimed receipt is true.

No output can assert `SUBSYSTEM_COMPLETE`, whole-program acceptance, or global acceptance. A synthetic positive release fixture exercises local validator behavior only; it is not a real release, provider run, external contract adoption, or owner acceptance.

## Acceptance matrix represented by the suite

| Original case | Focused control |
| --- | --- |
| PL-00 | `test_synthetic_baseline_is_planning_only_and_keeps_fanin_separate`; `test_later_pinned_snapshot_with_changed_counts_is_accepted` |
| PL-01 / PL-03 | `test_duplicate_owner_or_orphan_ownership_fails` |
| PL-02 | `test_same_count_wrong_contribution_row_fails`; `test_same_count_contributor_reassignment_preserves_required_pair_check`; `test_distinct_repeated_contribution_rows_keep_multiplicity` |
| PL-04 / PL-14 | `test_required_specialist_family_and_null_url_block_without_guessing` |
| PL-05 | `test_required_specialist_family_and_null_url_block_without_guessing` |
| PL-06 / PL-15 | `test_issue_readback_mismatch_and_incomplete_pages_fail`; `test_readback_identity_rejects_wrong_repository_or_body`; `test_oversized_issue_number_is_bounded_and_cli_keeps_json_diagnostic` |
| PL-07 | `test_closed_requires_exact_evidence_deferred_never_counts`; `test_closed_readback_cannot_hide_an_active_map_leaf` |
| PL-08 | `test_supersession_requires_both_exact_link_directions`; `test_valid_supersession_requires_exact_bidirectional_relation` |
| PL-09 | `test_global_metadata_and_scope_cannot_be_rewritten_by_local_projection` |
| PL-10 | `test_missing_classifier_family_and_invalid_metadata_fail_closed`; `test_dated_source_reference_retains_snapshot_counts_without_using_them_as_limits` |
| PL-11 / PL-12 | `test_execution_graph_cycles_self_edges_unknown_and_epic_edges_fail` |
| PL-13 | `test_gate_fanin_is_checked_as_its_own_typed_relation`; `test_execution_graph_cycles_self_edges_unknown_and_epic_edges_fail` |
| RL-00 | `test_positive_release_branch_is_scoped_and_never_acceptance_claim` |
| RL-01 | `test_release_requires_exact_revisions_modes_tests_and_receipts` |
| RL-02 | `test_release_digest_model_policy_and_prerequisite_fail_closed`; `test_release_missing_artifact_is_named_without_path_echo` |
| RL-03 | `test_release_requires_exact_revisions_modes_tests_and_receipts` |
| RL-04 | `test_release_digest_model_policy_and_prerequisite_fail_closed`; `test_release_configuration_expectations_are_pinned_by_the_leaf`; `test_invalid_map_configuration_pin_types_are_reported` |
| RL-05 | `test_release_requires_exact_revisions_modes_tests_and_receipts`; `test_pending_external_blocker_keeps_planning_visible_and_fails_release` |
| RL-06 | `test_release_requires_exact_revisions_modes_tests_and_receipts` |
| RL-07 | `test_positive_release_branch_is_scoped_and_never_acceptance_claim`; `test_pending_external_blocker_keeps_planning_visible_and_fails_release`; `test_applicable_stage_prerequisites_need_revision_bound_evidence`; `test_outcome_cannot_lower_required_stage_and_final_gate_stays_outside_scope`; `test_stage_status_enum_and_required_evidence_fail_closed`; `test_required_stage_edge_requirements_are_shared_by_both_endpoints`; `test_evidence_read_budget_counts_partial_reads_and_stops_before_later_files`; `test_evidence_read_budget_counts_bytes_when_close_fails`; `test_evidence_read_budget_allows_only_one_growth_overflow_byte` |
| RL-08 | `test_readback_capture_pin_drift_is_explicit`; `test_issue_readback_mismatch_and_incomplete_pages_fail` |
| Provenance, JSON and bounds | `test_duplicate_json_keys_are_rejected_and_pin_mismatch_fails`; `test_nested_duplicate_json_keys_and_unknown_source_alias_fail`; `test_missing_readback_pin_is_blocked_and_report_does_not_guess`; `test_malformed_json_returns_fixed_diagnostic`; `test_input_byte_limit_is_checked_before_parse` |

PL-13 has no title-based or inferred “overbroad dependency” policy. The suite verifies the concrete structural boundaries: execution prerequisites must be leaf records, gate fan-in is separately declared and reconciled, and evidence-stage edges do not satisfy the execution DAG. A semantic narrowing decision requires an explicit source decision in the supplied input; the validator does not manufacture owner agreement.

## V2 projection controls

| Projection boundary | Focused control |
| --- | --- |
| Acceptance owner remains distinct from accountable delivery and contributors | `test_valid_projection_keeps_acceptance_owner_separate_and_surfaces_external_blocker` |
| Missing and extraneous contributor pairs fail against independently declared ownership | `test_missing_required_association_fails_from_independent_ownership_declarations`; `test_extraneous_untyped_contribution_is_rejected` |
| Typed extras require their own reviewed role and source row | `test_typed_extra_role_and_source_review_are_required`; `test_repeated_source_pairs_keep_distinct_rows_and_ids` |
| Directional stage endpoints retain separate requirement IDs and source direction | `test_directional_disjoint_prerequisites_are_validated_per_endpoint`; `test_wrong_side_requirement_substitution_is_rejected` |
| A source critical-path claim must be a valid longest path, without requiring one tie-break; shortcuts, missing edges, repeated nodes, and cycles fail | `test_source_critical_path_accepts_any_valid_longest_tie_but_rejects_invalid_claims` |
| V1 stage names remain isolated; v2 keeps all six source stage names | `test_shared_edge_v1_stage_vocabulary_remains_unchanged`; `test_all_six_source_stages_are_admitted_without_collapsing_their_names` |
| External artifact status/action is separate from local issue closure | `test_pending_external_artifact_is_not_mistaken_for_issue_closure`; `test_release_stays_blocked_when_external_artifact_is_pending` |
| Canonical metadata and criteria outside subsystem scope are preserved | `test_canonical_metadata_and_out_of_scope_criteria_are_preserved` |
| Body, title, or state drift stays unadjudicated; incomplete readback pages fail closed | `test_readback_body_drift_blocks_without_overwriting_historical_digest`; `test_readback_title_and_state_drift_remain_unadjudicated`; `test_incomplete_issue_readback_pages_fail_closed` |
| V2 observed source revision must match its capture pin | `test_v2_readback_revision_must_match_the_pinned_observation` |
| Only bounded valid JSON Pointers are copied into v2 obligation reports | `test_report_preserves_valid_json_pointer_provenance`; `test_malformed_json_pointers_are_redacted_from_v2_obligations` |
| Historical test results do not masquerade as required test declarations | `test_historical_test_results_are_not_treated_as_required_test_declarations` |
| V2 requires an explicit required-test declaration and never substitutes historical results | `test_v2_required_tests_need_explicit_declaration_for_planning_and_release` |
| Unresolved accountability stays unresolved | `test_unresolved_accountability_decision_blocks_pair_instead_of_auto_adding` |
| Directional release evidence binds producer artifact to its relation, endpoint requirements, content, revision, mode, and prerequisite stage | `test_v2_release_binds_directional_stage_evidence_and_external_artifact`; `test_bound_prerequisite_artifact_does_not_require_producer_outcome_at_that_stage`; `test_v2_release_cannot_lower_a_leafs_declared_stage` |
| Source-declared relation receipts are preserved and required only when declared; internal receipts bind to the exact selected artifact revision | `test_v2_relation_receipts_are_preserved_and_checked_only_when_declared`; `test_internal_relation_receipt_binds_standalone_selected_artifact_revision` |
| Projector malformed shapes, nonfinite numbers, lone surrogates and failed paired output publication use bounded diagnostics | `tests/planning/test_coverage_projector.py` |
| A map entry marked closed cannot have an OPEN readback; internal complete status remains distinct | `test_v2_map_closed_requires_closed_issue_readback`; `test_v2_internal_completion_does_not_claim_issue_closed` |

The v2 positive release control uses only temporary synthetic files and mock evidence. It validates relation binding behavior; it is not evidence of external contract agreement or a real release.
