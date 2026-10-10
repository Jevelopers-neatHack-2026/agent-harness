#!/usr/bin/env python3
"""Validate Agent & Creative v1 JEV evaluation targets and dataset pins.

The harness checks readiness metadata only. Question definitions and schemas
remain owned by their engines. This validator never contacts a model/provider,
and it cannot establish model quality, live access, or active-routing acceptance.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any


TARGET_FORMAT = "agent-creative.jev-promotion-targets/v1"
DATASET_FORMAT = "agent-creative.jev-dataset-manifest/v1"
SPLITS = ("train", "validation", "held_out")
OWNER_ORG = "Jevelopers-neatHack-2026"

# These are issue #6's inventory labels, not engine question IDs or definitions.
REQUIRED_FAMILIES = {
    "source_classification": ("event-intelligence", "Agent and research"),
    "relevance_ranking": ("event-intelligence", "Agent and research"),
    "entity_matching": ("event-intelligence", "Agent and research"),
    "factual_conflict_triage": ("event-intelligence", "Agent and research"),
    "injection_signal": ("event-intelligence", "Agent and research"),
    "brand_asset_classification": ("creative-engine", "Creative and fulfillment"),
    "creative_intent_routing": ("creative-engine", "Creative and fulfillment"),
    "quality_repair_triage": ("creative-engine", "Creative and fulfillment"),
    "harness_eligible_action_ranking": ("agent-harness", "Agent and research"),
}

# Applicability is decided per question by its owner; it may not disappear from
# the plan. These tags are derived from issue #6 and are not schema fields.
REQUIRED_STRATA = {
    "url_quality",
    "conflicting_facts",
    "absent_evidence",
    "malformed_inputs",
    "malicious_source",
    "supported_languages",
    "research_creative_routing",
    "artifact_permission_contexts",
}

REQUIRED_OWNER_ROLES = {
    "Agent and research",
    "Creative and fulfillment",
    "Finance and reliability",
}
OWNER_ROLE_REPOSITORIES = {
    "Agent and research": {"agent-harness", "event-intelligence"},
    "Creative and fulfillment": {"creative-engine"},
    "Finance and reliability": {"cost-intelligence"},
}


def _pairs_no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _reject_non_finite(value: str) -> Any:
    raise ValueError("non-finite JSON number")


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as stream:
        return json.load(stream, object_pairs_hook=_pairs_no_duplicates, parse_constant=_reject_non_finite)


def digest_ok(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def revision_ok(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{40}", value) is not None


def numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def canonical_hash(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def add_error(errors: list[dict[str, str]], code: str, path: str, owner: str, action: str) -> None:
    errors.append({"code": code, "path": path, "owner": owner, "action": action})


def require_text(value: Any, path: str, owner: str, action: str, errors: list[dict[str, str]]) -> bool:
    if isinstance(value, str) and value.strip() and value.strip().lower() not in {"tbd", "todo", "pending", "unknown"}:
        return True
    add_error(errors, "required_owner_value_missing", path, owner, action)
    return False


def validate_approval(value: Any, path: str, owner_role: str, owner_action: str,
                      errors: list[dict[str, str]]) -> None:
    if not isinstance(value, dict) or value.get("status") != "accepted" or value.get("owner_role") != owner_role:
        add_error(errors, "owner_approval_missing", path, owner_role, owner_action)
        return
    decision_ref = value.get("decision_ref")
    repositories = OWNER_ROLE_REPOSITORIES.get(owner_role, set())
    valid_ref = isinstance(decision_ref, str) and any(
        re.fullmatch(rf"https://github\.com/{OWNER_ORG}/{re.escape(repo)}/(issues|pull)/[1-9][0-9]*", decision_ref)
        for repo in repositories
    )
    if not valid_ref:
        add_error(errors, "owner_decision_reference_invalid", path + ".decision_ref", owner_role,
                  "Link the accepted owner decision in the accountable role's issue or pull request.")
    if not revision_ok(value.get("revision")):
        add_error(errors, "owner_approval_revision_missing", path + ".revision", owner_role,
                  "Pin the accepted owner decision to an exact 40-character commit revision.")


def expected_question_action(family: str) -> tuple[str, str]:
    repository, role = REQUIRED_FAMILIES.get(family, ("Agent and research", "Agent and research"))
    if repository == "event-intelligence":
        action = "Publish the individually versioned research question definition, owner leaf, candidate set, ground truth, evidence schema reference, and acceptance decision in event-intelligence."
    elif repository == "creative-engine":
        action = "Publish the individually versioned creative question definition, owner leaf, candidate set, ground truth, evidence schema reference, and acceptance decision in creative-engine."
    else:
        action = "Publish the individually versioned harness runtime question definition and accountable owner leaf in agent-harness; do not move research or creative definitions into the harness."
    return role, action


def validate_dataset(dataset: Any, errors: list[dict[str, str]]) -> tuple[dict[str, str], dict[str, list[dict[str, Any]]]]:
    hashes: dict[str, str] = {}
    examples: dict[str, list[dict[str, Any]]] = {split: [] for split in SPLITS}
    if not isinstance(dataset, dict):
        add_error(errors, "dataset_manifest_object_required", "$", "Agent and research",
                  "Publish the frozen, sanitized dataset and label provenance manifest.")
        return hashes, examples
    if dataset.get("format") != DATASET_FORMAT:
        add_error(errors, "dataset_format_unsupported", "$.format", "Agent and research",
                  f"Publish a dataset manifest using {DATASET_FORMAT}.")
    allowed_dataset_keys = {"format", "status", "dataset_id", "dataset_version", "provenance", "owner_approval", "splits", "note"}
    if set(dataset) - allowed_dataset_keys:
        add_error(errors, "dataset_manifest_contains_unapproved_fields", "$", "Agent and research",
                  "Remove unrecognized fields; the dataset manifest must not carry private customer or raw source content.")
    if dataset.get("status") != "approved":
        add_error(errors, "dataset_owner_approval_pending", "$.status", "Agent and research",
                  "Publish and approve a frozen dataset manifest with label provenance, independent review, and held-out split policy; omit private customer content.")
    for key, action in (
        ("dataset_id", "Assign a stable non-customer dataset identity and version."),
        ("dataset_version", "Pin the accepted dataset revision."),
    ):
        require_text(dataset.get(key), "$" + "." + key, "Agent and research", action, errors)
    provenance = dataset.get("provenance")
    if not isinstance(provenance, dict):
        provenance = {}
    allowed_provenance_keys = {"source_refs", "collection_protocol", "label_protocol", "adjudication", "privacy_review_ref"}
    if set(provenance) - allowed_provenance_keys:
        add_error(errors, "dataset_provenance_contains_unapproved_fields", "$.provenance", "Agent and research",
                  "Remove unrecognized provenance payloads; store only sanitized references and protocol identities.")
    for key, action in (
        ("source_refs", "Record sanitized source and rights references for the dataset."),
        ("collection_protocol", "Record the versioned sampling and collection protocol."),
        ("label_protocol", "Record the versioned ground-truth labeling protocol."),
        ("adjudication", "Record independent review and disagreement adjudication."),
        ("privacy_review_ref", "Record the privacy review reference; no private customer content belongs in this manifest."),
    ):
        value = provenance.get(key)
        if key == "source_refs":
            if not isinstance(value, list) or not value or any(not isinstance(x, str) or not x.strip() for x in value):
                add_error(errors, "dataset_provenance_missing", "$.provenance.source_refs", "Agent and research", action)
        else:
            require_text(value, "$.provenance." + key, "Agent and research", action, errors)
    validate_approval(dataset.get("owner_approval"), "$.owner_approval", "Agent and research",
                      "Record the research/data owner approval for this exact dataset and label protocol.", errors)

    split_data = dataset.get("splits")
    if not isinstance(split_data, dict):
        split_data = {}
    seen_ids: dict[str, str] = {}
    seen_content_hashes: dict[str, str] = {}
    for split in SPLITS:
        item = split_data.get(split)
        path = "$.splits." + split
        if not isinstance(item, dict):
            add_error(errors, "dataset_split_missing", path, "Agent and research",
                      "Publish disjoint train, validation, and held-out splits with exact content hashes and ground-truth labels.")
            continue
        if set(item) - {"sha256", "examples"}:
            add_error(errors, "dataset_split_contains_unapproved_fields", path, "Agent and research",
                      "Remove unrecognized split payloads; keep the manifest to hashes and sanitized sample references.")
        rows = item.get("examples")
        if not isinstance(rows, list) or not rows:
            add_error(errors, "dataset_split_empty", path + ".examples", "Agent and research",
                      "Publish labeled examples for the accepted split; sample counts and strata are owner decisions.")
            rows = []
        safe_rows: list[dict[str, Any]] = []
        for index, row in enumerate(rows):
            row_path = f"{path}.examples[{index}]"
            if not isinstance(row, dict):
                add_error(errors, "dataset_example_malformed", row_path, "Agent and research",
                          "Publish only sanitized sample IDs, hashes, labels, and stratum tags; omit prompts and source contents.")
                continue
            allowed = {"sample_id", "sample_sha256", "labels", "strata"}
            if set(row) - allowed:
                add_error(errors, "dataset_example_contains_unapproved_fields", row_path, "Agent and research",
                          "Remove raw source, prompt, customer, or other unapproved content; this manifest accepts only IDs, hashes, labels, and strata.")
            sample_id = row.get("sample_id")
            sample_sha = row.get("sample_sha256")
            if not isinstance(sample_id, str) or not sample_id.strip():
                add_error(errors, "sample_id_missing", row_path + ".sample_id", "Agent and research",
                          "Provide a stable sanitized sample identifier.")
            elif sample_id in seen_ids:
                add_error(errors, "split_sample_id_overlap", row_path + ".sample_id", "Agent and research",
                          f"Move sample IDs so train, validation, and held-out splits are disjoint (overlap with {seen_ids[sample_id]}).")
            else:
                seen_ids[sample_id] = split
            if not digest_ok(sample_sha):
                add_error(errors, "sample_hash_missing", row_path + ".sample_sha256", "Agent and research",
                          "Pin the exact sanitized sample content hash without embedding its content.")
            elif sample_sha in seen_content_hashes:
                add_error(errors, "split_sample_content_overlap", row_path + ".sample_sha256", "Agent and research",
                          f"Remove duplicate content across splits (overlap with {seen_content_hashes[sample_sha]}).")
            else:
                seen_content_hashes[sample_sha] = split
            labels = row.get("labels")
            if not isinstance(labels, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in labels.items()):
                add_error(errors, "sample_labels_invalid", row_path + ".labels", "Agent and research",
                          "Record owner-reviewed ground-truth labels by stable question ID.")
            strata = row.get("strata")
            if not isinstance(strata, dict):
                add_error(errors, "sample_strata_invalid", row_path + ".strata", "Agent and research",
                          "Record sanitized question-specific stratum tags so required coverage can be verified.")
            safe_rows.append(row)
        ordered = sorted(safe_rows, key=lambda row: str(row.get("sample_id", "")))
        actual_hash = canonical_hash(ordered)
        expected_hash = item.get("sha256")
        if not digest_ok(expected_hash):
            add_error(errors, "split_hash_missing", path + ".sha256", "Agent and research",
                      "Publish SHA-256 of the canonical sorted sample-reference rows for this split.")
        elif expected_hash != actual_hash:
            add_error(errors, "split_hash_mismatch", path + ".sha256", "Agent and research",
                      "Recompute the split SHA-256 over the exact sanitized sample-reference rows.")
        hashes[split] = str(expected_hash or "")
        examples[split] = safe_rows
    return hashes, examples


def validate_metric_number(value: Any, path: str, owner: str, *, minimum: float | None = None,
                           maximum: float | None = None, positive: bool = False,
                           action: str, errors: list[dict[str, str]]) -> None:
    if not numeric(value):
        add_error(errors, "numeric_owner_target_missing", path, owner, action)
        return
    if positive and float(value) <= 0:
        add_error(errors, "numeric_owner_target_out_of_range", path, owner, action)
    if minimum is not None and float(value) < minimum:
        add_error(errors, "numeric_owner_target_out_of_range", path, owner, action)
    if maximum is not None and float(value) > maximum:
        add_error(errors, "numeric_owner_target_out_of_range", path, owner, action)


def validate_question(question: Any, index: int, dataset_hashes: dict[str, str],
                      examples: dict[str, list[dict[str, Any]]], errors: list[dict[str, str]]) -> tuple[str | None, str | None, list[str]]:
    path = f"$.questions[{index}]"
    if not isinstance(question, dict):
        add_error(errors, "question_record_malformed", path, "Agent and research",
                  "Publish one complete owner-supplied target record per required question.")
        return None, None, []
    allowed_question_keys = {
        "question_id", "family", "question_version", "definition_owner", "owner_approval", "primitive",
        "eligible_candidate_set_ref", "evidence_schema_ref", "unknown_behavior_ref", "review_behavior_ref",
        "fallback_behavior_ref", "instrumentation_ref", "schema_version", "policy_version", "rollback_owner_role",
        "rollback_rule", "authority_boundary", "model_pin", "ground_truth", "dataset_split_sha256", "coverage",
        "promotion_metrics", "unknown_cost_rule", "sequential_prerequisites", "evaluation_batch_id", "baselines",
    }
    if set(question) - allowed_question_keys:
        add_error(errors, "engine_question_definition_embedded", path, "Agent and research",
                  "Keep question instructions, candidate definitions, and evidence schemas in the owning engine; the harness stores IDs, external references, and evaluation targets only.")
    question_id = question.get("question_id")
    if not isinstance(question_id, str) or not question_id.strip() or question_id.strip().lower() in {"tbd", "todo", "pending"}:
        add_error(errors, "question_id_missing", path + ".question_id", "Agent and research",
                  "Assign the stable versioned question ID in its owning engine; do not invent a harness alias.")
        question_id = None
    family = question.get("family")
    family_key = family if isinstance(family, str) else ""
    if family_key not in REQUIRED_FAMILIES:
        add_error(errors, "question_family_missing_or_unsupported", path + ".family", "Agent and research",
                  "Assign this record to one of the issue #6 required question inventory families.")
        family_key = ""
    expected_repository, expected_role = REQUIRED_FAMILIES.get(family_key, ("", "Agent and research"))
    owner, owner_action = expected_question_action(family_key)
    decision_owner = question.get("definition_owner")
    if not isinstance(decision_owner, dict):
        decision_owner = {}
    if decision_owner.get("repository") != expected_repository or decision_owner.get("owner_role") != expected_role:
        add_error(errors, "question_definition_owner_mismatch", path + ".definition_owner", expected_role, owner_action)
    issue_url = decision_owner.get("issue_url")
    expected_prefix = f"https://github.com/{OWNER_ORG}/{expected_repository}/issues/" if expected_repository else ""
    if not isinstance(issue_url, str) or not re.fullmatch(re.escape(expected_prefix) + r"[1-9][0-9]*", issue_url):
        add_error(errors, "question_owner_leaf_missing", path + ".definition_owner.issue_url", expected_role,
                  "Link the exact accountable question-definition leaf in the owning repository; a broad family or epic is insufficient.")
    if not revision_ok(decision_owner.get("definition_revision")):
        add_error(errors, "question_definition_revision_missing", path + ".definition_owner.definition_revision", expected_role,
                  "Pin the owner-supplied question definition to a reviewed 40-character revision.")
    validate_approval(question.get("owner_approval"), path + ".owner_approval", expected_role, owner_action, errors)
    require_text(question.get("question_version"), path + ".question_version", expected_role,
                 "Publish an explicit question version from the question owner.", errors)

    primitive = question.get("primitive")
    if primitive not in {"choice", "score", "noul"}:
        add_error(errors, "provider_question_primitive_invalid", path + ".primitive", expected_role,
                  "Use an owner-reviewed Choice, Score, or Noul primitive matching the current TypeSafe API; keep engine-specific definitions in the engine.")
    for key, action in (
        ("eligible_candidate_set_ref", "Link the owner-defined eligible candidate set; do not copy candidate definitions into the harness."),
        ("evidence_schema_ref", "Link the versioned structured evidence or visual-observation definition owned by the applicable engine."),
        ("unknown_behavior_ref", "Link the owner-defined behavior for insufficient or unknown evidence."),
        ("review_behavior_ref", "Link the owner-defined human-review or abstention behavior."),
        ("fallback_behavior_ref", "Link the owner-defined safe fallback behavior."),
        ("instrumentation_ref", "Link the versioned instrumentation plan for inputs, outcomes, latency, and all downstream billed stages."),
        ("schema_version", "Pin the accepted shared DecisionPort/schema version; do not define a replacement schema in the harness."),
        ("policy_version", "Pin the owner-approved question policy version."),
        ("rollback_owner_role", "Name the accountable rollback owner role."),
        ("rollback_rule", "Define the question-specific rollback rule."),
    ):
        require_text(question.get(key), path + "." + key, expected_role, action, errors)

    boundary = question.get("authority_boundary")
    if not isinstance(boundary, dict):
        boundary = {}
    for key in ("model_can_authorize_budget", "model_can_approve_artifact", "model_can_change_permissions",
                "model_can_dispatch_privileged_actions", "provider_confidence_can_authorize"):
        if boundary.get(key) is not False:
            add_error(errors, "model_authority_boundary_invalid", path + ".authority_boundary." + key, expected_role,
                      "Keep JEV output and confidence separate from authorization, approval, permission, budget, and privileged-action authority.")
    if primitive == "noul" and boundary.get("noul_has_provider_confidence") is not False:
        add_error(errors, "noul_confidence_field_invalid", path + ".authority_boundary.noul_has_provider_confidence", expected_role,
                  "The current TypeSafe API Noul answer exposes its yes/no probability and has no confidence field; record false and never derive authority from it.")

    model = question.get("model_pin")
    if not isinstance(model, dict):
        model = {}
    for key, action in (
        ("provider", "Record the provider used by this question."),
        ("model_id", "Pin an explicit model ID; aliases such as latest or auto are not reproducible pins."),
        ("model_revision", "Record an exact model snapshot/revision or immutable provider reference."),
        ("configuration_sha256", "Pin generation configuration by SHA-256."),
    ):
        require_text(model.get(key), path + ".model_pin." + key, expected_role, action, errors)
    if isinstance(model.get("model_id"), str) and model["model_id"].strip().lower() in {"latest", "auto", "default", "current"}:
        add_error(errors, "model_alias_not_pinned", path + ".model_pin.model_id", expected_role,
                  "Replace mutable model alias with the exact model revision accepted by the owner.")
    if isinstance(model.get("model_revision"), str) and model["model_revision"].strip().lower() in {"latest", "auto", "default", "current"}:
        add_error(errors, "model_revision_not_pinned", path + ".model_pin.model_revision", expected_role,
                  "Replace mutable model revision alias with the exact model revision accepted by the owner.")
    if not digest_ok(model.get("configuration_sha256")):
        add_error(errors, "model_configuration_hash_missing", path + ".model_pin.configuration_sha256", expected_role,
                  "Pin the exact model configuration bytes by SHA-256.")

    gt = question.get("ground_truth")
    if not isinstance(gt, dict):
        gt = {}
    gt_keys = ("positive_label", "negative_label", "unknown_label", "review_label")
    gt_values: list[str] = []
    for key in gt_keys:
        value = gt.get(key)
        if isinstance(value, str) and value.strip() and value.strip().lower() not in {"tbd", "todo", "pending"}:
            gt_values.append(value)
        else:
            add_error(errors, "ground_truth_label_missing", path + ".ground_truth." + key, expected_role,
                      "Map the question owner-reviewed positive, negative, unknown, and review ground-truth outcomes.")
    if len(gt_values) != len(set(gt_values)):
        add_error(errors, "ground_truth_outcomes_not_distinct", path + ".ground_truth", expected_role,
                  "Give positive, negative, unknown, and review ground truth explicit owner-defined labels.")

    split_pins = question.get("dataset_split_sha256")
    if not isinstance(split_pins, dict):
        split_pins = {}
    for split in SPLITS:
        value = split_pins.get(split)
        if not digest_ok(value):
            add_error(errors, "question_dataset_split_hash_missing", path + ".dataset_split_sha256." + split, expected_role,
                      "Pin each question target to the accepted train, validation, and held-out split hash.")
        elif dataset_hashes.get(split) and value != dataset_hashes.get(split):
            add_error(errors, "question_dataset_split_hash_mismatch", path + ".dataset_split_sha256." + split, expected_role,
                      "Use the exact frozen dataset split SHA-256 in the question target.")

    coverage = question.get("coverage")
    if not isinstance(coverage, dict):
        coverage = {}
    mins = coverage.get("minimum_samples_by_split")
    if not isinstance(mins, dict):
        mins = {}
    for split in SPLITS:
        action = "Have the question owner approve a positive minimum labeled sample count for each split; the harness does not choose the value."
        validate_metric_number(mins.get(split), path + ".coverage.minimum_samples_by_split." + split, expected_role,
                              minimum=1, action=action, errors=errors)
        if numeric(mins.get(split)) and len(examples.get(split, [])) < mins[split]:
            add_error(errors, "dataset_sample_count_below_owner_target", path + ".coverage.minimum_samples_by_split." + split,
                      expected_role, "Add independently reviewed labeled samples until the owner-approved minimum is met.")
    min_labels = coverage.get("minimum_per_label_by_split")
    if not isinstance(min_labels, dict):
        min_labels = {}
    for label_key in gt_keys:
        label = gt.get(label_key)
        target_by_split = min_labels.get(label) if isinstance(label, str) else None
        if not isinstance(target_by_split, dict):
            add_error(errors, "ground_truth_coverage_target_missing", path + ".coverage.minimum_per_label_by_split", expected_role,
                      "Set owner-approved numeric per-split coverage for positive, negative, unknown, and review labels.")
            continue
        for split in SPLITS:
            action = "Have the question owner approve a positive per-split sample minimum for each ground-truth outcome."
            target = target_by_split.get(split)
            validate_metric_number(target, path + f".coverage.minimum_per_label_by_split.{label}.{split}", expected_role,
                                  minimum=1, action=action, errors=errors)
            count = sum(1 for row in examples.get(split, []) if isinstance(row.get("labels"), dict) and row["labels"].get(question_id) == label)
            if numeric(target) and count < target:
                add_error(errors, "dataset_label_coverage_below_owner_target", path + ".coverage.minimum_per_label_by_split", expected_role,
                          "Add independently reviewed examples until this label meets the owner-approved per-split minimum.")
    strata_plan = coverage.get("required_strata")
    if not isinstance(strata_plan, dict):
        strata_plan = {}
    for stratum in sorted(REQUIRED_STRATA):
        entry = strata_plan.get(stratum)
        spath = path + ".coverage.required_strata." + stratum
        if not isinstance(entry, dict):
            add_error(errors, "required_stratum_decision_missing", spath, expected_role,
                      "Record this issue #6 stratum as required with owner-approved coverage or not-applicable with a reason.")
            continue
        applicability = entry.get("applicability")
        if applicability == "not_applicable":
            require_text(entry.get("reason"), spath + ".reason", expected_role,
                         "Record the question owner's reason this stratum does not apply.", errors)
        elif applicability == "required":
            bucket_mins = entry.get("minimum_by_value_by_split")
            if not isinstance(bucket_mins, dict) or not bucket_mins:
                add_error(errors, "stratum_bucket_coverage_missing", spath, expected_role,
                          "Name owner-defined stratum values and numeric minimum coverage for every split.")
                continue
            for bucket, by_split in bucket_mins.items():
                if not isinstance(by_split, dict):
                    add_error(errors, "stratum_split_target_missing", spath, expected_role,
                              "Set numeric owner-approved sample coverage per split for each stratum bucket.")
                    continue
                for split in SPLITS:
                    action = "Have the question owner approve a positive per-split minimum for this relevant stratum bucket."
                    target = by_split.get(split)
                    validate_metric_number(target, spath + f".{bucket}.{split}", expected_role,
                                          minimum=1, action=action, errors=errors)
                    count = sum(1 for row in examples.get(split, [])
                                if isinstance(row.get("strata"), dict)
                                and isinstance(row["strata"].get(question_id), dict)
                                and row["strata"][question_id].get(stratum) == bucket)
                    if numeric(target) and count < target:
                        add_error(errors, "dataset_stratum_coverage_below_owner_target", spath, expected_role,
                                  "Add independently reviewed examples until this stratum meets the owner-approved minimum.")
        else:
            add_error(errors, "stratum_applicability_invalid", spath + ".applicability", expected_role,
                      "Choose required or not_applicable; do not omit the issue #6 stratum decision.")

    metrics = question.get("promotion_metrics")
    if not isinstance(metrics, dict):
        metrics = {}
    primary = metrics.get("primary_quality")
    if not isinstance(primary, dict):
        primary = {}
    require_text(primary.get("name"), path + ".promotion_metrics.primary_quality.name", expected_role,
                 "Name the owner-approved primary quality metric.", errors)
    if primary.get("direction") not in {"minimum", "maximum"}:
        add_error(errors, "primary_metric_direction_missing", path + ".promotion_metrics.primary_quality.direction", expected_role,
                  "Record whether promotion requires a minimum or maximum for the primary metric.")
    validate_metric_number(primary.get("threshold"), path + ".promotion_metrics.primary_quality.threshold", expected_role,
                          minimum=0, action="Obtain and record the question owner's numeric primary metric target; no default threshold is approved.", errors=errors)
    validate_metric_number(metrics.get("false_negative_rate_max"), path + ".promotion_metrics.false_negative_rate_max", expected_role,
                          minimum=0, maximum=1, action="Obtain and record the question owner's numeric false-negative ceiling; no default threshold is approved.", errors=errors)
    validate_metric_number(metrics.get("abstention_rate_max"), path + ".promotion_metrics.abstention_rate_max", expected_role,
                          minimum=0, maximum=1, action="Obtain and record the question owner's numeric abstention/review ceiling; no default threshold is approved.", errors=errors)
    validate_metric_number(metrics.get("p95_latency_ms_max"), path + ".promotion_metrics.p95_latency_ms_max", expected_role,
                          positive=True, action="Obtain and record the question owner's numeric p95 latency limit; no default limit is approved.", errors=errors)
    cost = metrics.get("total_downstream_cost_max")
    if not isinstance(cost, dict):
        cost = {}
    validate_metric_number(cost.get("amount"), path + ".promotion_metrics.total_downstream_cost_max.amount", "Finance and reliability",
                          minimum=0, action="Obtain and record the Finance and reliability owner's numeric total downstream cost limit; no default target is approved.", errors=errors)
    require_text(cost.get("currency"), path + ".promotion_metrics.total_downstream_cost_max.currency", "Finance and reliability",
                 "Record the approved cost currency.", errors)
    require_text(cost.get("cost_basis_ref"), path + ".promotion_metrics.total_downstream_cost_max.cost_basis_ref", "Finance and reliability",
                 "Link the approved attempt-level total downstream cost measurement basis.", errors)
    stages = metrics.get("downstream_cost_stages")
    if not isinstance(stages, list) or not stages or any(not isinstance(stage, str) or not stage.strip() for stage in stages):
        add_error(errors, "downstream_cost_stages_missing", path + ".promotion_metrics.downstream_cost_stages", "Finance and reliability",
                  "Enumerate all billable downstream stages included in the total cost comparison; no stage may be hidden.")
    unknown_cost = question.get("unknown_cost_rule")
    if not isinstance(unknown_cost, dict):
        unknown_cost = {}
    require_text(unknown_cost.get("rule"), path + ".unknown_cost_rule.rule", "Finance and reliability",
                 "Record the owner-approved treatment for unknown or unreconciled downstream cost.", errors)
    if unknown_cost.get("unknown_is_zero") is not False:
        add_error(errors, "unknown_cost_zero_invalid", path + ".unknown_cost_rule.unknown_is_zero", "Finance and reliability",
                  "Unknown or unreconciled cost must not be converted to zero.")
    validate_approval(unknown_cost.get("owner_approval"), path + ".unknown_cost_rule.owner_approval", "Finance and reliability",
                      "Record Finance and reliability approval of the unknown-cost rule.", errors)

    prereqs = question.get("sequential_prerequisites")
    if not isinstance(prereqs, list) or any(not isinstance(item, str) for item in prereqs):
        add_error(errors, "question_prerequisites_invalid", path + ".sequential_prerequisites", expected_role,
                  "Record the exact prerequisite question IDs so dependent evaluations are sequenced.")
        prereqs = []
    batch = question.get("evaluation_batch_id")
    if not isinstance(batch, str) or not batch.strip():
        add_error(errors, "question_batch_id_missing", path + ".evaluation_batch_id", expected_role,
                  "Assign an explicit batch identity for dependency-order validation.")
        batch = None
    return question_id, batch, list(prereqs)


def validate_baselines(question: Any, index: int, target_hashes: dict[str, str],
                       target_stages: list[str], errors: list[dict[str, str]]) -> None:
    path = f"$.questions[{index}].baselines"
    owner = "Finance and reliability"
    baselines = question.get("baselines") if isinstance(question, dict) else None
    if not isinstance(baselines, dict):
        baselines = {}
    comparable: dict[str, dict[str, Any]] = {}
    for kind in ("deterministic", "generative"):
        baseline = baselines.get(kind)
        bpath = path + "." + kind
        if not isinstance(baseline, dict):
            add_error(errors, "comparable_baseline_missing", bpath, owner,
                      "Pin both the deterministic baseline and the generative baseline for the same frozen eligible inputs.")
            continue
        if baseline.get("kind") != kind:
            add_error(errors, "baseline_kind_mismatch", bpath + ".kind", owner,
                      "Declare the baseline kind explicitly as deterministic or generative.")
        for key, action in (
            ("baseline_id", "Assign a stable baseline identity."),
            ("code_ref", "Link the exact baseline implementation."),
            ("eligibility_sha256", "Pin identical case eligibility for both baselines."),
            ("input_set_sha256", "Pin the exact input set used by both baselines."),
            ("policy_revision", "Pin the accepted policy revision."),
        ):
            require_text(baseline.get(key), bpath + "." + key, owner, action, errors)
        if not revision_ok(baseline.get("code_revision")):
            add_error(errors, "baseline_code_revision_missing", bpath + ".code_revision", owner,
                      "Pin the baseline implementation to a 40-character revision.")
        if not digest_ok(baseline.get("configuration_sha256")):
            add_error(errors, "baseline_configuration_hash_missing", bpath + ".configuration_sha256", owner,
                      "Pin the baseline configuration bytes by SHA-256.")
        if not digest_ok(baseline.get("eligibility_sha256")) or not digest_ok(baseline.get("input_set_sha256")):
            add_error(errors, "baseline_comparison_hash_missing", bpath, owner,
                      "Pin the exact eligibility and input-set hashes used for fair baseline comparison.")
        comparable[kind] = baseline
        pin = baseline.get("dataset_split_sha256")
        if not isinstance(pin, dict):
            pin = {}
        for split in SPLITS:
            if pin.get(split) != target_hashes.get(split) or not digest_ok(pin.get(split)):
                add_error(errors, "baseline_dataset_pin_mismatch", bpath + ".dataset_split_sha256." + split, owner,
                          "Use the exact same frozen train, validation, and held-out split as the question target.")
        stages = baseline.get("downstream_cost_stages")
        if not isinstance(stages, list) or not stages or set(stages) != set(target_stages):
            add_error(errors, "baseline_cost_scope_not_comparable", bpath + ".downstream_cost_stages", owner,
                      "Account for the same complete downstream billed stages used by the question target and the other baseline.")
        if kind == "generative":
            model = baseline.get("model_pin")
            if not isinstance(model, dict):
                model = {}
            for key in ("provider", "model_id", "model_revision", "configuration_sha256", "sampling_policy"):
                require_text(model.get(key), bpath + ".model_pin." + key, owner,
                             "Pin the generative baseline model snapshot and sampling configuration; mutable aliases are not reproducible.", errors)
            if isinstance(model.get("model_id"), str) and model["model_id"].strip().lower() in {"latest", "auto", "default", "current"}:
                add_error(errors, "generative_model_alias_not_pinned", bpath + ".model_pin.model_id", owner,
                          "Replace mutable model alias with the exact model revision accepted by the owner.")
            if isinstance(model.get("model_revision"), str) and model["model_revision"].strip().lower() in {"latest", "auto", "default", "current"}:
                add_error(errors, "generative_model_revision_not_pinned", bpath + ".model_pin.model_revision", owner,
                          "Replace mutable model revision alias with the exact model revision accepted by the owner.")
            if not digest_ok(model.get("configuration_sha256")):
                add_error(errors, "generative_model_configuration_hash_missing", bpath + ".model_pin.configuration_sha256", owner,
                          "Pin the generative baseline model configuration by SHA-256.")
        validate_approval(baseline.get("owner_approval"), bpath + ".owner_approval", "Finance and reliability",
                          "Record Finance and reliability acceptance of baseline comparability and cost measurement.", errors)
    deterministic = comparable.get("deterministic")
    generative = comparable.get("generative")
    if deterministic is not None and generative is not None:
        for key in ("eligibility_sha256", "input_set_sha256"):
            if deterministic.get(key) != generative.get(key):
                add_error(errors, "baseline_inputs_not_comparable", path + "." + key, owner,
                          "Pin deterministic and generative baselines to exactly the same eligibility and input set.")
        if deterministic.get("dataset_split_sha256") != generative.get("dataset_split_sha256"):
            add_error(errors, "baseline_split_plans_not_comparable", path + ".dataset_split_sha256", owner,
                      "Pin deterministic and generative baselines to the same frozen splits.")
        if set(deterministic.get("downstream_cost_stages", [])) != set(generative.get("downstream_cost_stages", [])):
            add_error(errors, "baseline_cost_stages_not_comparable", path + ".downstream_cost_stages", owner,
                      "Account for the same downstream billable stages in both baselines.")


def validate_targets(targets: Any, dataset: Any) -> dict[str, Any]:
    errors: list[dict[str, str]] = []
    if not isinstance(targets, dict):
        targets = {}
        add_error(errors, "targets_object_required", "$", "Agent and research",
                  "Publish the versioned owner-reviewed JEV target manifest.")
    if targets.get("format") != TARGET_FORMAT:
        add_error(errors, "targets_format_unsupported", "$.format", "Agent and research",
                  f"Publish promotion targets using {TARGET_FORMAT}.")
    allowed_target_keys = {
        "format", "status", "owner_reviews", "dataset_split_sha256", "question_inventory", "questions",
        "negative_results_policy", "shadow_only_counts_as_active", "active_routing_claim",
        "rollback_owner_role", "rollback_rule", "evaluation_budget", "note",
    }
    if set(targets) - allowed_target_keys:
        add_error(errors, "target_manifest_contains_unapproved_fields", "$", "Agent and research",
                  "Remove unrecognized target payloads; keep engine question definitions in their owning repositories.")
    if targets.get("status") != "approved_for_evaluation_preparation":
        add_error(errors, "promotion_targets_owner_decisions_pending", "$.status", "Agent and research",
                  "Obtain question-owner, research, creative, runtime, and Finance and reliability decisions; numeric target values remain owner supplied.")
    targets_approval = targets.get("owner_reviews")
    roles_seen: set[str] = set()
    if isinstance(targets_approval, list):
        for index, review in enumerate(targets_approval):
            if not isinstance(review, dict):
                continue
            role = review.get("owner_role")
            if isinstance(role, str) and role in roles_seen:
                add_error(errors, "duplicate_owner_role_review", f"$.owner_reviews[{index}].owner_role", role,
                          "Record one unambiguous current accountable role review per owner role.")
            if isinstance(role, str):
                roles_seen.add(role)
            if review.get("status") != "accepted":
                add_error(errors, "owner_role_review_pending", f"$.owner_reviews[{index}]", str(role or "Agent and research"),
                          "Record the owner role's accepted decision reference and revision.")
            repositories = OWNER_ROLE_REPOSITORIES.get(role, set()) if isinstance(role, str) else set()
            valid_review_ref = isinstance(review.get("decision_ref"), str) and any(
                re.fullmatch(rf"https://github\.com/{OWNER_ORG}/{re.escape(repo)}/(issues|pull)/[1-9][0-9]*", review["decision_ref"])
                for repo in repositories
            )
            if not valid_review_ref or not revision_ok(review.get("revision")):
                add_error(errors, "owner_role_review_incomplete", f"$.owner_reviews[{index}]", str(role or "Agent and research"),
                          "Pin the accepted role decision reference to a 40-character revision.")
    else:
        targets_approval = []
    for role in sorted(REQUIRED_OWNER_ROLES - roles_seen):
        add_error(errors, "required_owner_role_review_missing", "$.owner_reviews", role,
                  "Record this owner role's accepted target and baseline review before evaluation preparation.")

    dataset_hashes, examples = validate_dataset(dataset, errors)
    if isinstance(targets.get("dataset_split_sha256"), dict):
        target_hashes = targets["dataset_split_sha256"]
    else:
        target_hashes = {}
    for split in SPLITS:
        if not digest_ok(target_hashes.get(split)):
            add_error(errors, "targets_dataset_split_hash_missing", "$.dataset_split_sha256." + split, "Agent and research",
                      "Pin the approved train, validation, and held-out split hashes in the target manifest.")
        elif dataset_hashes.get(split) and target_hashes.get(split) != dataset_hashes.get(split):
            add_error(errors, "targets_dataset_split_hash_mismatch", "$.dataset_split_sha256." + split, "Agent and research",
                      "Use the exact SHA-256 values published by the frozen dataset manifest.")

    inventory = targets.get("question_inventory")
    if not isinstance(inventory, list):
        inventory = []
    inventory_by_family: dict[str, dict[str, Any]] = {}
    claimed_question_ids: dict[str, str] = {}
    for index, item in enumerate(inventory):
        ipath = f"$.question_inventory[{index}]"
        if not isinstance(item, dict):
            add_error(errors, "question_inventory_entry_malformed", ipath, "Agent and research",
                      "Publish exact owner-issued question IDs and definition revisions per required question family.")
            continue
        family = item.get("family")
        if not isinstance(family, str) or family not in REQUIRED_FAMILIES:
            add_error(errors, "question_inventory_family_unsupported", ipath + ".family", "Agent and research",
                      "Use the issue #6 required inventory families without replacing engine-owned definitions.")
            continue
        if family in inventory_by_family:
            add_error(errors, "duplicate_question_inventory_family", ipath + ".family", "Agent and research",
                      "Record one authoritative owner inventory entry per required family.")
        inventory_by_family[family] = item
        expected_repository, expected_role = REQUIRED_FAMILIES[family]
        if item.get("owner_repository") != expected_repository or item.get("owner_role") != expected_role:
            add_error(errors, "question_inventory_owner_mismatch", ipath, expected_role,
                      "Keep each question family with its engine/runtime owner as specified by issue #6.")
        ids = item.get("required_question_ids")
        if not isinstance(ids, list) or not ids:
            role, action = expected_question_action(family)
            add_error(errors, "owner_question_id_inventory_missing", ipath + ".required_question_ids", role,
                      f"Publish every individual stable question ID required for {family!r}; a family label is not a substitute. {action}")
            ids = []
        for question_id in ids:
            if not isinstance(question_id, str) or not question_id.strip():
                add_error(errors, "owner_question_id_invalid", ipath + ".required_question_ids", expected_role,
                          "Use stable owner-issued individual question IDs.")
                continue
            if question_id in claimed_question_ids:
                add_error(errors, "question_id_claimed_by_multiple_owners", ipath + ".required_question_ids", expected_role,
                          "Assign each individual question ID to exactly one definition owner leaf.")
            claimed_question_ids[question_id] = family
        if not revision_ok(item.get("definition_revision")):
            add_error(errors, "question_inventory_revision_missing", ipath + ".definition_revision", expected_role,
                      "Pin the individual question inventory to the owning engine's reviewed revision.")
    for family, (repository, role) in REQUIRED_FAMILIES.items():
        if family not in inventory_by_family:
            action_role, action = expected_question_action(family)
            add_error(errors, "required_question_inventory_family_missing", "$.question_inventory", action_role,
                      f"Publish the owner inventory entry for required family {family!r} in {repository}. {action}")

    questions = targets.get("questions")
    if not isinstance(questions, list):
        questions = []
        add_error(errors, "question_registry_missing", "$.questions", "Agent and research",
                  "Publish individually identified questions and link each definition to its owning engine leaf.")
    family_counts = {family: 0 for family in REQUIRED_FAMILIES}
    seen_ids: dict[str, int] = {}
    target_ids_by_family: dict[str, set[str]] = {family: set() for family in REQUIRED_FAMILIES}
    parsed: list[tuple[str | None, str | None, list[str]]] = []
    for index, question in enumerate(questions):
        question_id, batch, prereqs = validate_question(question, index, dataset_hashes, examples, errors)
        parsed.append((question_id, batch, prereqs))
        if isinstance(question_id, str):
            if question_id in seen_ids:
                add_error(errors, "question_id_has_multiple_definitions", f"$.questions[{index}].question_id", "Agent and research",
                          "Give each stable question ID exactly one definition owner; duplicate claimed owners block readiness.")
            seen_ids[question_id] = index
        family = question.get("family") if isinstance(question, dict) else None
        if family in family_counts:
            family_counts[family] += 1
            if isinstance(question_id, str):
                target_ids_by_family[family].add(question_id)
        metrics = question.get("promotion_metrics", {}) if isinstance(question, dict) else {}
        stages = metrics.get("downstream_cost_stages", []) if isinstance(metrics, dict) else []
        validate_baselines(question, index, target_hashes, stages if isinstance(stages, list) else [], errors)
    for family, count in family_counts.items():
        if count == 0:
            role, action = expected_question_action(family)
            add_error(errors, "required_question_family_missing", "$.questions", role,
                      f"Missing required issue #6 inventory family {family!r}. {action} The owner must also supply sample, metric, false-negative, abstention, latency, and downstream-cost values; this validator supplies none.")
        inventory_entry = inventory_by_family.get(family, {})
        required_ids = inventory_entry.get("required_question_ids", []) if isinstance(inventory_entry, dict) else []
        if isinstance(required_ids, list):
            expected_ids = {item for item in required_ids if isinstance(item, str)}
            actual_ids = target_ids_by_family.get(family, set())
            for missing_id in sorted(expected_ids - actual_ids):
                role, _ = expected_question_action(family)
                add_error(errors, "owner_required_question_target_missing", "$.questions", role,
                          f"Add a target record for owner-required individual question ID {missing_id!r} from family {family!r}.")
            for extra_id in sorted(actual_ids - expected_ids):
                role, _ = expected_question_action(family)
                add_error(errors, "unowned_question_target_present", "$.questions", role,
                          f"Remove or obtain owner inventory approval for question ID {extra_id!r}; it is not in the pinned owner inventory.")

    graph: dict[str, list[str]] = {qid: prereqs for qid, _, prereqs in parsed if qid is not None}
    batches = {qid: batch for qid, batch, _ in parsed if qid is not None}
    for qid, prereqs in graph.items():
        for prereq in prereqs:
            if prereq not in graph:
                add_error(errors, "question_prerequisite_unknown", "$.questions.sequential_prerequisites", "Agent and research",
                          "Publish the prerequisite question in its owning engine or correct the stale prerequisite ID.")
            elif batches.get(qid) is not None and batches.get(qid) == batches.get(prereq):
                add_error(errors, "dependent_questions_in_same_parallel_batch", "$.questions.evaluation_batch_id", "Agent and research",
                          "Move the dependent question to a later evaluation batch; sequential prerequisites cannot run in one parallel batch.")
    visiting: set[str] = set()
    visited: set[str] = set()
    def visit(qid: str) -> None:
        if qid in visiting:
            add_error(errors, "question_prerequisite_cycle", "$.questions.sequential_prerequisites", "Agent and research",
                      "Remove the cycle and publish an acyclic owner-approved evaluation order.")
            return
        if qid in visited:
            return
        visiting.add(qid)
        for prereq in graph.get(qid, []):
            if prereq in graph:
                visit(prereq)
        visiting.remove(qid)
        visited.add(qid)
    for qid in graph:
        visit(qid)

    negative = targets.get("negative_results_policy")
    if not isinstance(negative, dict):
        negative = {}
    if negative.get("on_target_miss") not in {"block_activation", "revise_follow_on_preserving_original"}:
        add_error(errors, "negative_result_rule_missing", "$.negative_results_policy.on_target_miss", "Agent and research",
                  "Define that a miss blocks activation or triggers a follow-on redesign while preserving the original required target.")
    if negative.get("preserve_original_required_target") is not True:
        add_error(errors, "original_target_not_preserved", "$.negative_results_policy.preserve_original_required_target", "Agent and research",
                  "Keep the original required target in force; a failed result cannot be retroactively lowered.")
    if negative.get("retroactive_lowering_after_results") is not False:
        add_error(errors, "retroactive_target_lowering_allowed", "$.negative_results_policy.retroactive_lowering_after_results", "Agent and research",
                  "Forbid lowering an approved target after seeing evaluation results; issue a new version and rerun instead.")
    require_text(negative.get("target_change_policy"), "$.negative_results_policy.target_change_policy", "Agent and research",
                 "Record the versioned review and rerun policy for future target changes.", errors)
    if targets.get("shadow_only_counts_as_active") is not False:
        add_error(errors, "shadow_as_active_invalid", "$.shadow_only_counts_as_active", "Agent and research",
                  "Shadow-only evidence cannot satisfy an active-routing requirement.")
    if targets.get("active_routing_claim") is not False:
        add_error(errors, "active_routing_claim_invalid", "$.active_routing_claim", "Agent and research",
                  "This target verifier establishes preparation only; active-routing evidence belongs to ACV1-JEV-EVAL and specialist gates.")
    require_text(targets.get("rollback_owner_role"), "$.rollback_owner_role", "Agent and research",
                 "Name the accountable owner role for rollback.", errors)
    require_text(targets.get("rollback_rule"), "$.rollback_rule", "Agent and research",
                 "Record the owner-approved rollback rule.", errors)
    budget = targets.get("evaluation_budget")
    if not isinstance(budget, dict):
        budget = {}
    require_text(budget.get("budget_scope_ref"), "$.evaluation_budget.budget_scope_ref", "Finance and reliability",
                 "Record the authorized evaluation budget scope before any paid execution.", errors)
    require_text(budget.get("currency"), "$.evaluation_budget.currency", "Finance and reliability",
                 "Record the owner-approved evaluation budget currency.", errors)
    validate_metric_number(budget.get("maximum_amount"), "$.evaluation_budget.maximum_amount", "Finance and reliability",
                          positive=True, action="Obtain a Finance and reliability approved numeric evaluation budget envelope; no limit is chosen here.", errors=errors)
    validate_approval(budget.get("owner_approval"), "$.evaluation_budget.owner_approval", "Finance and reliability",
                      "Record the approved evaluation budget envelope before any paid execution.", errors)
    if budget.get("paid_execution_allowed") is not False:
        add_error(errors, "paid_execution_not_authorized_by_readiness_tool", "$.evaluation_budget.paid_execution_allowed", "Finance and reliability",
                  "This readiness tool never authorizes or performs paid calls; paid execution requires separate approved owner controls.")

    errors.sort(key=lambda item: (item["path"], item["code"], item["owner"]))
    return {
        "tool": "validate_jev_targets",
        "status": "blocked" if errors else "evaluation_preparation_ready",
        "ready": not errors,
        "scope": "owner-approved evaluation target and dataset preparation only; no classifier quality, paid execution, active routing, live evidence, or global acceptance claim",
        "question_count": len(questions),
        "dataset_split_sha256": {split: dataset_hashes.get(split, "") for split in SPLITS},
        "errors": errors,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--targets", required=True, type=Path)
    parser.add_argument("--dataset", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        targets = read_json(args.targets)
        dataset = read_json(args.dataset)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
        print(json.dumps({"tool": "validate_jev_targets", "status": "invalid_input", "ready": False,
                          "scope": "owner-approved evaluation target and dataset preparation only",
                          "errors": [{"code": "input_unreadable_or_invalid_json", "path": "$",
                                      "owner": "Agent and research",
                                      "action": "Supply readable UTF-8 JSON target and dataset manifests; diagnostics omit manifest contents."}]},
                         sort_keys=True, separators=(",", ":"), ensure_ascii=False))
        return 1
    report = validate_targets(targets, dataset)
    print(json.dumps(report, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
    return 0 if report["ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
