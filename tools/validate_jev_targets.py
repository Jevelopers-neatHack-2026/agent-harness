#!/usr/bin/env python3
"""Validate Agent & Creative v1 JEV evaluation targets and dataset pins.

The harness checks readiness metadata only. Question definitions and schemas
remain owned by their engines. This validator never contacts a model/provider,
and it cannot establish model quality, live access, or active-routing acceptance.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import os
import re
from pathlib import Path
from typing import Any


TARGET_FORMAT = "agent-creative.jev-promotion-targets/v2"
DATASET_FORMAT = "agent-creative.jev-dataset-manifest/v2"
METERING_INVENTORY_FORMAT = "agent-creative.metering-stage-inventory/v1"
OWNER_EVIDENCE_INDEX_FORMAT = "agent-creative.owner-evidence-index/v1"
OWNER_EVIDENCE_FORMAT = "agent-creative.owner-evidence/v1"
SPLITS = ("train", "validation", "held_out")
OWNER_ORG = "Jevelopers-neatHack-2026"
REQUIRED_EVALUATION_FOLLOW_ON = f"https://github.com/{OWNER_ORG}/agent-harness/issues/22"
MAX_JSON_BYTES = 16 * 1024 * 1024
MAX_NUMBER_DIGITS = 256
MAX_COUNT = 2 ** 31 - 1

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


def _parse_int(value: str) -> int:
    if len(value.lstrip("-")) > MAX_NUMBER_DIGITS:
        raise ValueError("integer too large")
    return int(value)


def _parse_float(value: str) -> float:
    if len(value) > MAX_NUMBER_DIGITS:
        raise ValueError("number too large")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("non-finite number")
    return number


def read_bounded_bytes(path: Path, limit: int = MAX_JSON_BYTES) -> bytes:
    with path.open("rb") as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("input size limit")
    return raw


def read_json(path: Path) -> Any:
    raw = read_bounded_bytes(path)
    return json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs_no_duplicates,
                      parse_int=_parse_int, parse_float=_parse_float, parse_constant=_reject_non_finite)


def digest_ok(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def revision_ok(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{40}", value) is not None


def numeric(value: Any) -> bool:
    return type(value) is int or (type(value) is float and math.isfinite(value))


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
                      errors: list[dict[str, str]], *, test_only: bool = False) -> None:
    if test_only:
        if (not isinstance(value, dict) or value.get("status") != "synthetic_test_only_unverified"
                or value.get("owner_role") != owner_role):
            add_error(errors, "synthetic_approval_shape_invalid", path, owner_role,
                      "Use explicitly synthetic, unverified approval-shaped data; test fixtures never represent owner acceptance.")
        return
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


def _external_artifact(root: Path, relative: Any) -> Path | None:
    if not isinstance(relative, str) or not relative.strip() or Path(relative).is_absolute():
        return None
    parts = Path(relative).parts
    if any(part in {"..", ""} for part in parts):
        return None
    current = root
    for part in parts:
        current = current / part
        if current.is_symlink():
            return None
    try:
        resolved = current.resolve(strict=True)
        resolved.relative_to(root.resolve(strict=True))
        if not resolved.is_file() or resolved.stat().st_size > MAX_JSON_BYTES:
            return None
        return resolved
    except (OSError, ValueError):
        return None


def _is_test_only_evidence(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    if value.get("test_only") is True:
        return True
    for marker in (value.get("classification"), value.get("status")):
        if isinstance(marker, str):
            normalized = marker.strip().lower().replace("-", "_").replace(" ", "_")
            if "synthetic" in normalized or "test_only" in normalized:
                return True
    return False


def load_trusted_evidence_index(path_value: str | None, expected_sha: str | None,
                                workspace_root: Path, errors: list[dict[str, str]]) -> dict[str, Any] | None:
    owner_action = "Provision an independently verified owner-evidence index outside the repository and pin its exact SHA-256 in trusted runner configuration; local manifest assertions and issue URLs are not approvals."
    if not path_value or not digest_ok(expected_sha):
        add_error(errors, "owner_evidence_index_unavailable", "$.owner_evidence_index", "Agent and research", owner_action)
        return None
    path = Path(path_value)
    if path.is_symlink():
        add_error(errors, "owner_evidence_index_untrusted", "$.owner_evidence_index", "Agent and research", owner_action)
        return None
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(workspace_root.resolve(strict=True))
    except ValueError:
        pass
    except OSError:
        add_error(errors, "owner_evidence_index_missing", "$.owner_evidence_index", "Agent and research", owner_action)
        return None
    else:
        add_error(errors, "owner_evidence_index_must_be_external", "$.owner_evidence_index", "Agent and research", owner_action)
        return None
    try:
        raw = read_bounded_bytes(resolved)
        if hashlib.sha256(raw).hexdigest() != expected_sha:
            raise ValueError("digest mismatch")
        data = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs_no_duplicates,
                          parse_int=_parse_int, parse_float=_parse_float, parse_constant=_reject_non_finite)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError, RecursionError):
        add_error(errors, "owner_evidence_index_untrusted_or_invalid", "$.owner_evidence_index", "Agent and research", owner_action)
        return None
    if not isinstance(data, dict) or data.get("format") != OWNER_EVIDENCE_INDEX_FORMAT:
        add_error(errors, "owner_evidence_index_format_invalid", "$.owner_evidence_index.format", "Agent and research", owner_action)
        return None
    if _is_test_only_evidence(data):
        add_error(errors, "owner_evidence_index_classification_invalid", "$.owner_evidence_index.classification",
                  "Agent and research",
                  "Production readiness rejects explicitly synthetic or test-only evidence indexes; those fixtures remain structure-only and cannot establish owner approval.")
        return None
    entries = data.get("evidence")
    if not isinstance(entries, list) or not entries:
        add_error(errors, "owner_evidence_index_empty", "$.owner_evidence_index.evidence", "Agent and research", owner_action)
        return None
    root = resolved.parent
    records: dict[str, dict[str, Any]] = {}
    for index, entry in enumerate(entries):
        ep = f"$.owner_evidence_index.evidence[{index}]"
        if not isinstance(entry, dict):
            add_error(errors, "owner_evidence_record_invalid", ep, "Agent and research", owner_action)
            continue
        evidence_id = entry.get("evidence_id")
        repository = entry.get("repository")
        revision = entry.get("revision")
        decision_ref = entry.get("decision_ref")
        subjects = entry.get("subjects")
        if (not isinstance(evidence_id, str) or not evidence_id.strip() or evidence_id in records
                or not isinstance(repository, str) or repository not in {repo for repos in OWNER_ROLE_REPOSITORIES.values() for repo in repos}
                or not revision_ok(revision) or entry.get("owner") != repository
                or not isinstance(decision_ref, str)
                or not any(re.fullmatch(rf"https://github\.com/{OWNER_ORG}/{re.escape(repository)}/(issues|pull)/[1-9][0-9]*", decision_ref) for _ in (0,))
                or entry.get("status") != "accepted" or not isinstance(subjects, dict) or not subjects
                or any(not isinstance(key, str) or not digest_ok(value) for key, value in subjects.items())):
            add_error(errors, "owner_evidence_record_invalid", ep, repository if isinstance(repository, str) else "Agent and research", owner_action)
            continue
        if _is_test_only_evidence(entry):
            add_error(errors, "owner_evidence_record_classification_invalid", ep, repository,
                      "Production readiness rejects explicitly synthetic or test-only evidence records and documents.")
            continue
        evidence_path = _external_artifact(root, entry.get("evidence_path"))
        evidence_sha = entry.get("evidence_sha256")
        if evidence_path is None or not digest_ok(evidence_sha):
            add_error(errors, "owner_evidence_artifact_missing", ep, repository, owner_action)
            continue
        try:
            evidence_bytes = read_bounded_bytes(evidence_path)
            evidence_doc = json.loads(evidence_bytes.decode("utf-8"), object_pairs_hook=_pairs_no_duplicates,
                                      parse_int=_parse_int, parse_float=_parse_float, parse_constant=_reject_non_finite)
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError, RecursionError):
            add_error(errors, "owner_evidence_artifact_invalid", ep, repository, owner_action)
            continue
        expected_doc = {"format": OWNER_EVIDENCE_FORMAT, "evidence_id": evidence_id, "owner": repository,
                        "repository": repository, "revision": revision, "decision_ref": decision_ref,
                        "status": "accepted", "subjects": subjects}
        if _is_test_only_evidence(evidence_doc):
            add_error(errors, "owner_evidence_artifact_classification_invalid", ep, repository,
                      "Production readiness rejects explicitly synthetic or test-only evidence documents.")
            continue
        if (hashlib.sha256(evidence_bytes).hexdigest() != evidence_sha
                or evidence_doc != expected_doc):
            add_error(errors, "owner_evidence_artifact_mismatch", ep, repository, owner_action)
            continue
        records[evidence_id] = dict(entry)
    if errors:
        return None
    return {"sha256": expected_sha, "records": records}


def validate_bound_approval(value: Any, path: str, owner_role: str, repository: str,
                            subject_id: str, subject_sha256: str, evidence: dict[str, Any] | None,
                            errors: list[dict[str, str]], *, test_only: bool = False) -> None:
    action = "Obtain owner-published evidence approving the exact referenced bytes, revisions, thresholds, cost scope, and policy for this subject."
    if not isinstance(value, dict):
        add_error(errors, "owner_approval_missing", path, owner_role, action)
        return
    if (value.get("owner_role") != owner_role or value.get("subject_id") != subject_id
            or value.get("subject_sha256") != subject_sha256):
        add_error(errors, "owner_approval_subject_hash_mismatch", path, owner_role,
                  "Bind this approval to the exact current subject content SHA-256 and revision; stale approval metadata does not pass.")
    if test_only:
        if value.get("status") != "synthetic_test_only_unverified":
            add_error(errors, "synthetic_approval_shape_invalid", path, owner_role,
                      "Synthetic plan tests use unverified test-only evidence and never claim owner approval.")
        return
    if evidence is None:
        add_error(errors, "owner_approval_not_independently_verified", path, owner_role, action)
        return
    evidence_id = value.get("evidence_id")
    record = evidence["records"].get(evidence_id) if isinstance(evidence_id, str) else None
    if not isinstance(record, dict):
        add_error(errors, "owner_evidence_reference_missing", path + ".evidence_id", owner_role, action)
        return
    if (value.get("status") != "accepted" or value.get("owner_repository") != repository
            or value.get("decision_ref") != record.get("decision_ref")
            or value.get("revision") != record.get("revision")
            or record.get("repository") != repository
            or record.get("subjects", {}).get(subject_id) != subject_sha256):
        add_error(errors, "owner_evidence_subject_binding_mismatch", path, owner_role,
                  "Match a trusted owner-published evidence artifact to the exact current content hash and revision.")


def expected_question_action(family: str) -> tuple[str, str]:
    repository, role = REQUIRED_FAMILIES.get(family, ("Agent and research", "Agent and research"))
    if repository == "event-intelligence":
        action = "Publish the individually versioned research question definition, owner leaf, candidate set, ground truth, evidence schema reference, and acceptance decision in event-intelligence."
    elif repository == "creative-engine":
        action = "Publish the individually versioned creative question definition, owner leaf, candidate set, ground truth, evidence schema reference, and acceptance decision in creative-engine."
    else:
        action = "Publish the individually versioned harness runtime question definition and accountable owner leaf in agent-harness; do not move research or creative definitions into the harness."
    return role, action


def validate_dataset(dataset: Any, errors: list[dict[str, str]], *, test_only: bool = False) -> tuple[dict[str, str], dict[str, list[dict[str, Any]]]]:
    hashes: dict[str, str] = {}
    examples: dict[str, list[dict[str, Any]]] = {split: [] for split in SPLITS}
    if not isinstance(dataset, dict):
        add_error(errors, "dataset_manifest_object_required", "$", "Agent and research",
                  "Publish the frozen, sanitized dataset and label provenance manifest.")
        return hashes, examples
    if dataset.get("format") != DATASET_FORMAT:
        add_error(errors, "dataset_format_unsupported", "$.format", "Agent and research",
                  f"Publish a dataset manifest using {DATASET_FORMAT}.")
    allowed_dataset_keys = {"format", "status", "dataset_id", "dataset_version", "owner_repository",
                            "provenance", "owner_approval", "split_approvals", "splits", "note"}
    if set(dataset) - allowed_dataset_keys:
        add_error(errors, "dataset_manifest_contains_unapproved_fields", "$", "Agent and research",
                  "Remove unrecognized fields; the dataset manifest must not carry private customer or raw source content.")
    if dataset.get("status") != ("test_only_synthetic" if test_only else "approved"):
        add_error(errors, "dataset_owner_approval_pending", "$.status", "Agent and research",
                  "Publish and approve a frozen dataset manifest with label provenance, independent review, and held-out split policy; omit private customer content.")
    for key, action in (
        ("dataset_id", "Assign a stable non-customer dataset identity and version."),
        ("dataset_version", "Pin the accepted dataset revision."),
    ):
        require_text(dataset.get(key), "$" + "." + key, "Agent and research", action, errors)
    dataset_repository = dataset.get("owner_repository", "event-intelligence")
    if not isinstance(dataset_repository, str) or dataset_repository not in {"event-intelligence", "agent-harness"}:
        add_error(errors, "dataset_owner_repository_invalid", "$.owner_repository", "Agent and research",
                  "Keep the dataset with its accountable research or harness owner leaf.")
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
                      "Record the research/data owner approval for this exact dataset and label protocol.", errors,
                      test_only=test_only)
    split_approvals = dataset.get("split_approvals")
    if not test_only and not isinstance(split_approvals, dict):
        add_error(errors, "dataset_split_approvals_missing", "$.split_approvals", "Agent and research",
                  "Bind each train, validation, and held-out split hash to the owner-reviewed dataset evidence.")
    if isinstance(split_approvals, dict):
        for split in SPLITS:
            if split not in split_approvals:
                add_error(errors, "dataset_split_approval_missing", "$.split_approvals." + split, "Agent and research",
                          "Bind this exact split content hash to the owner-reviewed dataset evidence.")

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
            allowed = {"sample_id", "sample_sha256", "labels", "strata", "eligible_question_ids"}
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
            eligible = row.get("eligible_question_ids")
            if (not isinstance(eligible, list) or any(not isinstance(item, str) or not item.strip() for item in eligible)
                    or len(eligible) != len(set(item for item in eligible if isinstance(item, str)))):
                add_error(errors, "sample_eligibility_invalid", row_path + ".eligible_question_ids", "Agent and research",
                          "Record distinct owner-defined eligible question IDs; only eligible rows with valid labels count toward sample coverage.")
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
        hashes[split] = expected_hash if digest_ok(expected_hash) else ""
        examples[split] = safe_rows
    return hashes, examples


def dataset_scope_hashes(question_id: str, examples: dict[str, list[dict[str, Any]]]) -> tuple[str, str]:
    """Derive candidate input and eligibility identities from the pinned sanitized split rows."""
    input_splits: dict[str, list[dict[str, Any]]] = {}
    eligibility_splits: dict[str, list[str]] = {}
    for split in SPLITS:
        selected: list[dict[str, Any]] = []
        eligible_ids: list[str] = []
        for row in examples.get(split, []):
            if not isinstance(row, dict):
                continue
            eligible = row.get("eligible_question_ids")
            if not isinstance(eligible, list) or question_id not in eligible:
                continue
            sample_id = row.get("sample_id")
            sample_hash = row.get("sample_sha256")
            safe_id = sample_id if isinstance(sample_id, str) else None
            safe_hash = sample_hash if digest_ok(sample_hash) else None
            selected.append({"sample_id": safe_id, "sample_sha256": safe_hash})
            if safe_id is not None:
                eligible_ids.append(safe_id)
        selected.sort(key=lambda row: row["sample_id"] or "")
        input_splits[split] = selected
        eligibility_splits[split] = sorted(eligible_ids)
    return canonical_hash(input_splits), canonical_hash(eligibility_splits)


def evaluation_budget_plan_hash(budget: Any) -> str | None:
    """Hash exact budget plan fields, excluding its separate owner approval envelope."""
    if not isinstance(budget, dict):
        return None
    payload = {key: value for key, value in budget.items() if key != "owner_approval"}
    return canonical_hash(payload)


def validate_metric_number(value: Any, path: str, owner: str, *, minimum: float | None = None,
                           maximum: float | None = None, positive: bool = False,
                           action: str, errors: list[dict[str, str]]) -> None:
    if not numeric(value):
        add_error(errors, "numeric_owner_target_missing", path, owner, action)
        return
    if positive and value <= 0:
        add_error(errors, "numeric_owner_target_out_of_range", path, owner, action)
    if minimum is not None and value < minimum:
        add_error(errors, "numeric_owner_target_out_of_range", path, owner, action)
    if maximum is not None and value > maximum:
        add_error(errors, "numeric_owner_target_out_of_range", path, owner, action)


def validate_sample_count(value: Any, path: str, owner: str, action: str,
                          errors: list[dict[str, str]]) -> bool:
    if type(value) is not int or value < 1 or value > MAX_COUNT:
        add_error(errors, "sample_count_invalid", path, owner, action)
        return False
    return True


def validate_question(question: Any, index: int, dataset_hashes: dict[str, str],
                      examples: dict[str, list[dict[str, Any]]], errors: list[dict[str, str]],
                      *, test_only: bool = False, metering_stage_ids: set[str] | None = None,
                      approved_budget_sha256: str | None = None) -> tuple[str | None, str | None, list[str]]:
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
        "candidate_comparison",
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
    valid_test_ref = test_only and isinstance(issue_url, str) and issue_url.startswith("synthetic://")
    if not valid_test_ref and (not isinstance(issue_url, str) or not re.fullmatch(re.escape(expected_prefix) + r"[1-9][0-9]*", issue_url)):
        add_error(errors, "question_owner_leaf_missing", path + ".definition_owner.issue_url", expected_role,
                  "Link the exact accountable question-definition leaf in the owning repository; a broad family or epic is insufficient.")
    if not revision_ok(decision_owner.get("definition_revision")):
        add_error(errors, "question_definition_revision_missing", path + ".definition_owner.definition_revision", expected_role,
                  "Pin the owner-supplied question definition to a reviewed 40-character revision.")
    validate_approval(question.get("owner_approval"), path + ".owner_approval", expected_role, owner_action, errors,
                      test_only=test_only)
    require_text(question.get("question_version"), path + ".question_version", expected_role,
                 "Publish an explicit question version from the question owner.", errors)

    primitive = question.get("primitive")
    if not isinstance(primitive, str) or primitive not in {"choice", "score", "noul"}:
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
    unsupported_gt_keys = [key for key in gt if key not in gt_keys]
    if unsupported_gt_keys:
        add_error(errors, "ground_truth_keys_unsupported", path + ".ground_truth", expected_role,
                  "Keep ground_truth limited to the four owner-defined positive, negative, unknown, and review outcomes.")
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
    # Only the four explicitly validated outcomes may contribute to sample,
    # label, or stratum coverage. Extra metadata never creates a new outcome.
    accepted_labels = set(gt_values)
    eligible_counts: dict[str, int] = {}
    valid_rows_by_split: dict[str, list[dict[str, Any]]] = {}
    for split in SPLITS:
        valid_rows: list[dict[str, Any]] = []
        seen_question_sample_ids: set[str] = set()
        for row in examples.get(split, []):
            labels = row.get("labels") if isinstance(row, dict) else None
            eligible = row.get("eligible_question_ids") if isinstance(row, dict) else None
            sample_id = row.get("sample_id") if isinstance(row, dict) else None
            label = labels.get(question_id) if isinstance(labels, dict) and isinstance(question_id, str) else None
            if isinstance(label, str) and label not in accepted_labels:
                add_error(errors, "unsupported_question_ground_truth_label",
                          path + f".dataset_splits.{split}.examples", expected_role,
                          "Use only this question's exact owner-approved positive, negative, unknown, or review label values.")
            if isinstance(label, str) and isinstance(eligible, list) and question_id not in eligible:
                add_error(errors, "question_label_outside_eligibility", path + ".ground_truth", expected_role,
                          "A sample label cannot count for a question unless the owner-approved candidate eligibility includes that question.")
            if (isinstance(sample_id, str) and sample_id not in seen_question_sample_ids
                    and isinstance(label, str) and label in accepted_labels
                    and isinstance(eligible, list) and question_id in eligible):
                seen_question_sample_ids.add(sample_id)
                valid_rows.append(row)
        valid_rows_by_split[split] = valid_rows
        eligible_counts[split] = len(valid_rows)
        action = "Have the question owner approve a positive minimum count of distinct eligible rows with a valid label for this question."
        if validate_sample_count(mins.get(split), path + ".coverage.minimum_samples_by_split." + split, expected_role, action, errors):
            if eligible_counts[split] < mins[split]:
                add_error(errors, "dataset_sample_count_below_owner_target", path + ".coverage.minimum_samples_by_split." + split,
                          expected_role, "Add independently reviewed eligible rows with valid labels until the owner-approved minimum is met.")
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
            valid_target = validate_sample_count(target, path + f".coverage.minimum_per_label_by_split.{label}.{split}", expected_role, action, errors)
            count = sum(1 for row in valid_rows_by_split.get(split, [])
                        if isinstance(row.get("labels"), dict) and row["labels"].get(question_id) == label)
            if valid_target and count < target:
                add_error(errors, "dataset_label_coverage_below_owner_target",
                          path + f".coverage.minimum_per_label_by_split.{label}.{split}", expected_role,
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
                    valid_target = validate_sample_count(target, spath + f".{bucket}.{split}", expected_role, action, errors)
                    count = sum(1 for row in valid_rows_by_split.get(split, [])
                                if isinstance(row.get("strata"), dict)
                                and isinstance(row["strata"].get(question_id), dict)
                                and row["strata"][question_id].get(stratum) == bucket)
                    if valid_target and count < target:
                        add_error(errors, "dataset_stratum_coverage_below_owner_target",
                                  spath + f".{bucket}.{split}", expected_role,
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
    if not isinstance(primary.get("direction"), str) or primary.get("direction") not in {"minimum", "maximum"}:
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
        stages = []
    elif len(stages) != len(set(stages)):
        add_error(errors, "downstream_cost_stage_duplicate", path + ".promotion_metrics.downstream_cost_stages", "Finance and reliability",
                  "List each owner-inventoried cost stage once.")
    applicability = metrics.get("downstream_cost_applicability")
    if not isinstance(applicability, dict):
        applicability = {}
        add_error(errors, "downstream_cost_applicability_missing", path + ".promotion_metrics.downstream_cost_applicability", "Finance and reliability",
                  "Record required or owner-approved not-applicable status for every metering-owner stage.")
    if metering_stage_ids is None or set(applicability) != metering_stage_ids:
        add_error(errors, "downstream_cost_inventory_mismatch", path + ".promotion_metrics.downstream_cost_applicability", "Finance and reliability",
                  "Cover every and only cost stage from the independently owner-published metering inventory.")
    applicable_ids: set[str] = set()
    for stage_id, decision in applicability.items():
        if not isinstance(stage_id, str) or not isinstance(decision, dict):
            add_error(errors, "downstream_cost_applicability_invalid", path + ".promotion_metrics.downstream_cost_applicability", "Finance and reliability",
                      "Use exact owner inventory IDs and typed applicability decisions.")
            continue
        state = decision.get("status")
        if state == "required":
            applicable_ids.add(stage_id)
        elif state == "not_applicable":
            require_text(decision.get("reason"), path + ".promotion_metrics.downstream_cost_applicability." + stage_id + ".reason",
                         "Finance and reliability", "Record an owner-approved not-applicable reason for this billed stage.", errors)
        else:
            add_error(errors, "downstream_cost_applicability_state_invalid", path + ".promotion_metrics.downstream_cost_applicability." + stage_id,
                      "Finance and reliability", "Choose required or not_applicable for every owner-inventoried stage.")
    if isinstance(stages, list) and all(isinstance(stage, str) for stage in stages) and set(stages) != applicable_ids:
        add_error(errors, "downstream_cost_stages_do_not_match_inventory", path + ".promotion_metrics.downstream_cost_stages", "Finance and reliability",
                  "List exactly the stages marked required in the independent inventory-backed applicability map.")
    candidate = question.get("candidate_comparison")
    if not isinstance(candidate, dict):
        candidate = {}
        add_error(errors, "candidate_comparison_plan_missing", path + ".candidate_comparison", "Finance and reliability",
                  "Pin the candidate's approved input, eligibility, budget bytes, and frozen split identities before baseline comparison.")
    for key in ("input_set_sha256", "eligibility_sha256", "budget_plan_sha256"):
        if not digest_ok(candidate.get(key)):
            add_error(errors, "candidate_comparison_identity_missing", path + ".candidate_comparison." + key, "Finance and reliability",
                      "Use an independently approved candidate input/eligibility/budget artifact SHA-256; no local hash assertion is approval evidence.")
    if isinstance(question_id, str):
        expected_input_hash, expected_eligibility_hash = dataset_scope_hashes(question_id, examples)
        if candidate.get("input_set_sha256") != expected_input_hash:
            add_error(errors, "candidate_input_set_not_bound_to_dataset", path + ".candidate_comparison.input_set_sha256", expected_role,
                      "Derive candidate inputs from exact eligible sample IDs and content hashes in the independently approved frozen dataset splits.")
        if candidate.get("eligibility_sha256") != expected_eligibility_hash:
            add_error(errors, "candidate_eligibility_not_bound_to_dataset", path + ".candidate_comparison.eligibility_sha256", expected_role,
                      "Derive candidate eligibility from the exact per-question eligible row identities in the approved dataset splits.")
    if approved_budget_sha256 is None or candidate.get("budget_plan_sha256") != approved_budget_sha256:
        add_error(errors, "candidate_budget_plan_not_bound_to_approved_budget", path + ".candidate_comparison.budget_plan_sha256", "Finance and reliability",
                  "Use the hash derived from the exact separately approved evaluation budget plan bytes.")
    candidate_splits = candidate.get("dataset_split_sha256")
    if not isinstance(candidate_splits, dict):
        candidate_splits = {}
    for split in SPLITS:
        if candidate_splits.get(split) != dataset_hashes.get(split) or not digest_ok(candidate_splits.get(split)):
            add_error(errors, "candidate_dataset_split_mismatch", path + ".candidate_comparison.dataset_split_sha256." + split,
                      expected_role, "Bind the candidate to the exact approved frozen dataset split bytes.")
    unknown_cost = question.get("unknown_cost_rule")
    if not isinstance(unknown_cost, dict):
        unknown_cost = {}
    require_text(unknown_cost.get("rule"), path + ".unknown_cost_rule.rule", "Finance and reliability",
                 "Record the owner-approved treatment for unknown or unreconciled downstream cost.", errors)
    if unknown_cost.get("unknown_is_zero") is not False:
        add_error(errors, "unknown_cost_zero_invalid", path + ".unknown_cost_rule.unknown_is_zero", "Finance and reliability",
                  "Unknown or unreconciled cost must not be converted to zero.")
    validate_approval(unknown_cost.get("owner_approval"), path + ".unknown_cost_rule.owner_approval", "Finance and reliability",
                      "Record Finance and reliability approval of the unknown-cost rule.", errors, test_only=test_only)

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
                       target_stages: list[str], errors: list[dict[str, str]], *,
                       candidate_comparison: dict[str, Any] | None = None,
                       candidate_applicability: dict[str, Any] | None = None,
                       metering_stage_ids: set[str] | None = None,
                       test_only: bool = False) -> None:
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
            ("budget_plan_sha256", "Pin the exact approved evaluation budget bytes used by this baseline and the candidate."),
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
        valid_stages = isinstance(stages, list) and bool(stages) and all(isinstance(stage, str) and stage.strip() for stage in stages)
        valid_target_stages = (isinstance(target_stages, list) and bool(target_stages)
                               and all(isinstance(stage, str) and stage.strip() for stage in target_stages))
        if (not valid_stages or len(stages) != len(set(stages)) or not valid_target_stages
                or set(stages) != set(target_stages)):
            add_error(errors, "baseline_cost_scope_not_comparable", bpath + ".downstream_cost_stages", owner,
                      "Match the candidate's applicable stage IDs from the independent metering-owner inventory; caller-supplied equal lists are insufficient.")
        applicability = baseline.get("downstream_cost_applicability")
        if (not isinstance(applicability, dict) or not isinstance(candidate_applicability, dict)
                or applicability != candidate_applicability
                or (metering_stage_ids is not None and set(applicability) != metering_stage_ids)):
            add_error(errors, "baseline_cost_applicability_incomplete", bpath + ".downstream_cost_applicability", owner,
                      "Record required or owner-approved not-applicable status for every independently inventoried downstream cost stage.")
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
                          "Record Finance and reliability acceptance of baseline comparability and cost measurement.", errors,
                          test_only=test_only)
    deterministic = comparable.get("deterministic")
    generative = comparable.get("generative")
    if deterministic is not None and generative is not None:
        for key in ("eligibility_sha256", "input_set_sha256", "budget_plan_sha256"):
            if deterministic.get(key) != generative.get(key):
                add_error(errors, "baseline_inputs_not_comparable", path + "." + key, owner,
                          "Pin deterministic and generative baselines to the same candidate input, eligibility, and approved budget bytes.")
        if not isinstance(candidate_comparison, dict):
            add_error(errors, "candidate_comparison_plan_missing", path, owner,
                      "Publish the approved JEV candidate input/eligibility/budget identities before comparing baselines.")
        else:
            for key in ("input_set_sha256", "eligibility_sha256", "budget_plan_sha256"):
                expected = candidate_comparison.get(key)
                if not digest_ok(expected) or deterministic.get(key) != expected or generative.get(key) != expected:
                    add_error(errors, "baseline_candidate_scope_mismatch", path + "." + key, owner,
                              "Require JEV candidate and both baselines to use the exact same pinned approved input, eligibility, and budget bytes.")
        if deterministic.get("dataset_split_sha256") != generative.get("dataset_split_sha256"):
            add_error(errors, "baseline_split_plans_not_comparable", path + ".dataset_split_sha256", owner,
                      "Pin deterministic and generative baselines to the same frozen splits.")
        det_stages = deterministic.get("downstream_cost_stages")
        gen_stages = generative.get("downstream_cost_stages")
        if (not isinstance(det_stages, list) or not isinstance(gen_stages, list)
                or any(not isinstance(item, str) for item in det_stages + gen_stages)
                or set(det_stages) != set(gen_stages)):
            add_error(errors, "baseline_cost_stages_not_comparable", path + ".downstream_cost_stages", owner,
                      "Match cost stages from the independent metering-owner inventory, including explicit not-applicable reasons.")


def validate_metering_stage_inventory(value: Any, errors: list[dict[str, str]], *,
                                      test_only: bool = False) -> set[str]:
    path = "$.metering_stage_inventory"
    owner_action = "Publish the complete owner-maintained billable-stage inventory, with explicit applicability decisions for research, creative/image, inspection, and other induced downstream work."
    if not isinstance(value, dict):
        add_error(errors, "metering_stage_inventory_missing", path, "Finance and reliability", owner_action)
        return set()
    if value.get("format") != METERING_INVENTORY_FORMAT:
        add_error(errors, "metering_stage_inventory_format_invalid", path + ".format", "Finance and reliability",
                  f"Publish the independent cost-stage inventory using {METERING_INVENTORY_FORMAT}.")
    if value.get("status") != ("test_only_synthetic" if test_only else "published"):
        add_error(errors, "metering_stage_inventory_unverified", path + ".status", "Finance and reliability", owner_action)
    if value.get("owner_repository") != "cost-intelligence" or value.get("owner_role") != "Finance and reliability":
        add_error(errors, "metering_stage_inventory_owner_invalid", path, "Finance and reliability", owner_action)
    if not revision_ok(value.get("revision")):
        add_error(errors, "metering_stage_inventory_revision_missing", path + ".revision", "Finance and reliability", owner_action)
    decision_ref = value.get("decision_ref")
    valid_synthetic_ref = test_only and isinstance(decision_ref, str) and decision_ref.startswith("synthetic://")
    if not valid_synthetic_ref and (not isinstance(decision_ref, str) or not re.fullmatch(
            rf"https://github\.com/{OWNER_ORG}/cost-intelligence/(issues|pull)/[1-9][0-9]*", decision_ref)):
        add_error(errors, "metering_stage_inventory_decision_missing", path + ".decision_ref", "Finance and reliability", owner_action)
    rows = value.get("stages")
    if not isinstance(rows, list) or not rows:
        add_error(errors, "metering_stage_inventory_empty", path + ".stages", "Finance and reliability", owner_action)
        rows = []
    result: set[str] = set()
    for index, row in enumerate(rows):
        stage_path = f"{path}.stages[{index}]"
        if not isinstance(row, dict):
            add_error(errors, "metering_stage_invalid", stage_path, "Finance and reliability", owner_action)
            continue
        stage_id = row.get("stage_id")
        if not isinstance(stage_id, str) or not stage_id.strip() or stage_id in result:
            add_error(errors, "metering_stage_id_invalid_or_duplicate", stage_path + ".stage_id", "Finance and reliability", owner_action)
            continue
        result.add(stage_id)
        require_text(row.get("scope"), stage_path + ".scope", "Finance and reliability",
                     "Describe the owner-defined billable stage scope; do not use a caller-invented cost list.", errors)
    if value.get("completeness_decision") is not True:
        add_error(errors, "metering_stage_completeness_unapproved", path + ".completeness_decision", "Finance and reliability", owner_action)
    validate_approval(value.get("owner_approval"), path + ".owner_approval", "Finance and reliability",
                      owner_action, errors, test_only=test_only)
    return result


def _without_approvals(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _without_approvals(item) for key, item in value.items()
                if key not in {"owner_approval", "owner_reviews", "split_approvals"}}
    if isinstance(value, list):
        return [_without_approvals(item) for item in value]
    return value


def approval_subjects(targets: Any, dataset: Any) -> list[tuple[str, Any, str, str, str, str]]:
    """Return (path, approval, role, repo, subject ID, exact content hash)."""
    specs: list[tuple[str, Any, str, str, str, str]] = []
    if not isinstance(targets, dict) or not isinstance(dataset, dict):
        return specs
    plan_payload = _without_approvals({key: value for key, value in targets.items() if key != "owner_reviews"})
    plan_digest = canonical_hash(plan_payload)
    reviews = targets.get("owner_reviews")
    if isinstance(reviews, list):
        for index, review in enumerate(reviews):
            role = review.get("owner_role") if isinstance(review, dict) else None
            repo = review.get("owner_repository") if isinstance(review, dict) else None
            if isinstance(role, str) and isinstance(repo, str):
                specs.append((f"$.owner_reviews[{index}]", review, role, repo, "target-plan", plan_digest))

    inventory = targets.get("question_inventory")
    if isinstance(inventory, list):
        for index, item in enumerate(inventory):
            if not isinstance(item, dict):
                continue
            role, repo = item.get("owner_role"), item.get("owner_repository")
            if isinstance(role, str) and isinstance(repo, str):
                payload = _without_approvals(item)
                specs.append((f"$.question_inventory[{index}].owner_approval", item.get("owner_approval"), role,
                              repo, "question-inventory:" + str(item.get("family", "")), canonical_hash(payload)))

    questions = targets.get("questions")
    if isinstance(questions, list):
        for index, question in enumerate(questions):
            if not isinstance(question, dict):
                continue
            qid = question.get("question_id")
            owner = question.get("definition_owner")
            if isinstance(owner, dict) and isinstance(owner.get("owner_role"), str) and isinstance(owner.get("repository"), str):
                payload = _without_approvals(question)
                specs.append((f"$.questions[{index}].owner_approval", question.get("owner_approval"), owner["owner_role"],
                              owner["repository"], "question-definition:" + str(qid or ""), canonical_hash(payload)))
            unknown = question.get("unknown_cost_rule")
            if isinstance(unknown, dict):
                specs.append((f"$.questions[{index}].unknown_cost_rule.owner_approval", unknown.get("owner_approval"),
                              "Finance and reliability", "cost-intelligence", "unknown-cost-rule:" + str(qid or ""),
                              canonical_hash(_without_approvals(unknown))))
            baselines = question.get("baselines")
            if isinstance(baselines, dict):
                for kind, baseline in baselines.items():
                    if isinstance(kind, str) and isinstance(baseline, dict):
                        specs.append((f"$.questions[{index}].baselines.{kind}.owner_approval", baseline.get("owner_approval"),
                                      "Finance and reliability", "cost-intelligence", f"baseline-plan:{qid}:{kind}",
                                      canonical_hash(_without_approvals(baseline))))

    budget = targets.get("evaluation_budget")
    if isinstance(budget, dict):
        specs.append(("$.evaluation_budget.owner_approval", budget.get("owner_approval"),
                      "Finance and reliability", "cost-intelligence", "evaluation-budget",
                      canonical_hash(_without_approvals(budget))))
    metering = targets.get("metering_stage_inventory")
    if isinstance(metering, dict):
        specs.append(("$.metering_stage_inventory.owner_approval", metering.get("owner_approval"),
                      "Finance and reliability", "cost-intelligence", "metering-stage-inventory",
                      canonical_hash(_without_approvals(metering))))

    dataset_payload = _without_approvals(dataset)
    dataset_repo = dataset.get("owner_repository", "event-intelligence")
    if isinstance(dataset_repo, str):
        specs.append(("$.dataset.owner_approval", dataset.get("owner_approval"), "Agent and research", dataset_repo,
                      "dataset", canonical_hash(dataset_payload)))
    split_approvals = dataset.get("split_approvals")
    split_data = dataset.get("splits")
    if isinstance(split_approvals, dict) and isinstance(split_data, dict):
        for split in SPLITS:
            if isinstance(split_data.get(split), dict):
                specs.append((f"$.dataset.split_approvals.{split}", split_approvals.get(split), "Agent and research", dataset_repo,
                              "dataset-split:" + split, canonical_hash(split_data[split])))
    return specs


def validate_approval_subjects(targets: Any, dataset: Any, evidence: dict[str, Any] | None,
                               errors: list[dict[str, str]], *, test_only: bool = False) -> None:
    for path, approval, role, repository, subject_id, content_hash in approval_subjects(targets, dataset):
        validate_bound_approval(approval, path, role, repository, subject_id, content_hash, evidence, errors,
                                test_only=test_only)


def validate_targets(targets: Any, dataset: Any, *, evidence: dict[str, Any] | None = None,
                     test_only: bool = False) -> dict[str, Any]:
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
        "negative_results_policy", "shadow_only_counts_as_active", "active_routing_claim", "evaluation_follow_on",
        "rollback_owner_role", "rollback_rule", "evaluation_budget", "metering_stage_inventory", "note",
    }
    if set(targets) - allowed_target_keys:
        add_error(errors, "target_manifest_contains_unapproved_fields", "$", "Agent and research",
                  "Remove unrecognized target payloads; keep engine question definitions in their owning repositories.")
    expected_target_status = "test_only_synthetic" if test_only else "approved_for_evaluation_preparation"
    if targets.get("status") != expected_target_status:
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
            expected_review_status = "synthetic_test_only_unverified" if test_only else "accepted"
            if review.get("status") != expected_review_status:
                add_error(errors, "owner_role_review_pending", f"$.owner_reviews[{index}]", str(role or "Agent and research"),
                          "Record the owner role's accepted decision reference and revision.")
            repositories = OWNER_ROLE_REPOSITORIES.get(role, set()) if isinstance(role, str) else set()
            repo = review.get("owner_repository")
            if not isinstance(repo, str) or repo not in repositories:
                add_error(errors, "owner_role_repository_invalid", f"$.owner_reviews[{index}].owner_repository", str(role or "Agent and research"),
                          "Name the exact repository assigned to this existing owner role.")
            if not test_only:
                valid_review_ref = isinstance(review.get("decision_ref"), str) and isinstance(repo, str) and re.fullmatch(
                    rf"https://github\.com/{OWNER_ORG}/{re.escape(repo)}/(issues|pull)/[1-9][0-9]*", review["decision_ref"])
                if not valid_review_ref or not revision_ok(review.get("revision")):
                    add_error(errors, "owner_role_review_incomplete", f"$.owner_reviews[{index}]", str(role or "Agent and research"),
                              "Pin the accepted role decision reference to a 40-character revision.")
    else:
        targets_approval = []
    for role in sorted(REQUIRED_OWNER_ROLES - roles_seen):
        add_error(errors, "required_owner_role_review_missing", "$.owner_reviews", role,
                  "Record this owner role's accepted target and baseline review before evaluation preparation.")

    dataset_hashes, examples = validate_dataset(dataset, errors, test_only=test_only)
    metering_stage_ids = validate_metering_stage_inventory(targets.get("metering_stage_inventory"), errors,
                                                           test_only=test_only)
    approved_budget_sha256 = evaluation_budget_plan_hash(targets.get("evaluation_budget"))
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
        if not test_only:
            validate_approval(item.get("owner_approval"), ipath + ".owner_approval", expected_role,
                              "Record the exact owner-published question ID inventory revision.", errors)
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
        question_id, batch, prereqs = validate_question(question, index, dataset_hashes, examples, errors,
                                                        test_only=test_only, metering_stage_ids=metering_stage_ids,
                                                        approved_budget_sha256=approved_budget_sha256)
        parsed.append((question_id, batch, prereqs))
        if isinstance(question_id, str):
            if question_id in seen_ids:
                add_error(errors, "question_id_has_multiple_definitions", f"$.questions[{index}].question_id", "Agent and research",
                          "Give each stable question ID exactly one definition owner; duplicate claimed owners block readiness.")
            seen_ids[question_id] = index
        family = question.get("family") if isinstance(question, dict) else None
        if isinstance(family, str) and family in family_counts:
            family_counts[family] += 1
            if isinstance(question_id, str):
                target_ids_by_family[family].add(question_id)
            definition_owner = question.get("definition_owner") if isinstance(question, dict) else None
            inventory_entry = inventory_by_family.get(family)
            if isinstance(definition_owner, dict) and isinstance(inventory_entry, dict):
                if definition_owner.get("definition_revision") != inventory_entry.get("definition_revision"):
                    add_error(errors, "question_inventory_definition_revision_mismatch",
                              f"$.questions[{index}].definition_owner.definition_revision",
                              REQUIRED_FAMILIES[family][1],
                              "Use the exact individual-definition revision published by the pinned owner question inventory.")
        metrics = question.get("promotion_metrics", {}) if isinstance(question, dict) else {}
        stage_value = metrics.get("downstream_cost_stages", []) if isinstance(metrics, dict) else []
        stages = ([stage for stage in stage_value if isinstance(stage, str) and stage.strip()]
                  if isinstance(stage_value, list) else [])
        candidate = question.get("candidate_comparison") if isinstance(question, dict) else None
        applicability = metrics.get("downstream_cost_applicability") if isinstance(metrics, dict) else None
        validate_baselines(question, index, target_hashes, stages if isinstance(stages, list) else [], errors,
                           candidate_comparison=candidate if isinstance(candidate, dict) else None,
                           candidate_applicability=applicability if isinstance(applicability, dict) else None,
                           metering_stage_ids=metering_stage_ids, test_only=test_only)
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
    if not isinstance(negative.get("on_target_miss"), str) or negative.get("on_target_miss") not in {"block_activation", "revise_follow_on_preserving_original"}:
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
    follow_on = targets.get("evaluation_follow_on")
    if (not isinstance(follow_on, dict)
            or follow_on.get("issue_ref") != REQUIRED_EVALUATION_FOLLOW_ON
            or follow_on.get("owner_role") != "Agent and research"
            or follow_on.get("status") != "pending"):
        add_error(errors, "evaluation_follow_on_missing_or_claimed_complete", "$.evaluation_follow_on", "Agent and research",
                  "Link the actionable ACV1-JEV-EVAL issue #22 and keep execution evidence pending; this preparation verifier does not prove live/active routing.")
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
                      "Record the approved evaluation budget envelope before any paid execution.", errors,
                      test_only=test_only)
    if budget.get("paid_execution_allowed") is not False:
        add_error(errors, "paid_execution_not_authorized_by_readiness_tool", "$.evaluation_budget.paid_execution_allowed", "Finance and reliability",
                  "This readiness tool never authorizes or performs paid calls; paid execution requires separate approved owner controls.")

    validate_approval_subjects(targets, dataset if isinstance(dataset, dict) else {}, evidence, errors, test_only=test_only)
    errors.sort(key=lambda item: (item["path"], item["code"], item["owner"]))
    ready = not errors and evidence is not None and not test_only
    return {
        "tool": "validate_jev_targets",
        "status": ("blocked" if errors else ("synthetic_test_only_validation_passed" if test_only else "evaluation_preparation_ready")),
        "ready": ready,
        "structurally_valid": not errors,
        "scope": "owner-approved evaluation target and dataset preparation only; no classifier quality, paid execution, active routing, live evidence, or global acceptance claim",
        "question_count": len(questions),
        "dataset_split_sha256": {split: dataset_hashes.get(split, "") for split in SPLITS},
        "errors": errors,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--targets", required=True, type=Path)
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument("--owner-evidence-index", type=Path, default=os.environ.get("ACV1_OWNER_EVIDENCE_INDEX"))
    parser.add_argument("--owner-evidence-sha256", default=os.environ.get("ACV1_OWNER_EVIDENCE_SHA256"))
    args = parser.parse_args(argv)
    try:
        targets = read_json(args.targets)
        dataset = read_json(args.dataset)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError, RecursionError):
        print(json.dumps({"tool": "validate_jev_targets", "status": "invalid_input", "ready": False,
                          "scope": "owner-approved evaluation target and dataset preparation only",
                          "errors": [{"code": "input_unreadable_or_invalid_json", "path": "$",
                                      "owner": "Agent and research",
                                      "action": "Supply bounded, readable UTF-8 JSON target and dataset manifests; diagnostics omit manifest contents."}]},
                         sort_keys=True, separators=(",", ":"), ensure_ascii=False))
        return 2
    trust_errors: list[dict[str, str]] = []
    repo_root = Path(__file__).resolve().parents[1]
    evidence = load_trusted_evidence_index(
        str(args.owner_evidence_index) if args.owner_evidence_index else None,
        args.owner_evidence_sha256, repo_root, trust_errors)
    try:
        report = validate_targets(targets, dataset, evidence=evidence)
    except Exception:
        report = {"tool": "validate_jev_targets", "status": "invalid_input", "ready": False,
                  "structurally_valid": False,
                  "scope": "owner-approved evaluation target and dataset preparation only",
                  "question_count": 0, "dataset_split_sha256": {split: "" for split in SPLITS},
                  "errors": [{"code": "input_shape_invalid", "path": "$", "owner": "Agent and research",
                              "action": "Supply bounded JSON with validated types; malformed values are sanitized and cannot pass readiness."}]}
    report["errors"].extend(trust_errors)
    report["errors"].sort(key=lambda item: (item["path"], item["code"], item["owner"]))
    if report["errors"]:
        report["ready"] = False
        report["status"] = "blocked" if report["status"] != "invalid_input" else "invalid_input"
    report["owner_evidence_verified"] = evidence is not None
    print(json.dumps(report, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
    return 0 if report["ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
