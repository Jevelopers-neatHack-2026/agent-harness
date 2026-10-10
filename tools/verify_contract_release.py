#!/usr/bin/env python3
"""Verify an owner-published Agent & Creative contract release offline.

This tool defines a readiness descriptor and an adapter invocation protocol. It
does not define runtime schemas, ports, or their semantics. Until the canonical
owner publishes a release descriptor with pinned schemas, fixtures, an offline
validator adapter, and reviewed producer/consumer decisions, it exits blocked.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


FORMAT = "agent-creative.contract-readiness/v1"
ADAPTER_PROTOCOL = "agent-creative.contract-fixture-adapter/v1"

# Case identities and expected outcomes are copied from ACV1-CONTRACTS issue #5
# and its closure audit. They are scenario requirements, not schema field names.
REQUIRED_CASES: dict[str, str] = {
    "current_valid": "accepted",
    "current_invalid": "rejected",
    "supported_older_version": "accepted",
    "unsupported_major_version": "rejected",
    "unknown_additive_fields": "compatibility_declared",
    "forged_tenant_project_scope": "rejected",
    "approval_artifact_fact_mismatch": "rejected",
    "approval_invalid_actor_action_expiry_revocation_replay": "rejected",
    "changed_fact_hash_without_artwork_change": "rejected",
    "changed_artifact_bytes_without_hash_change": "rejected",
    "logical_billable_transport_identity_swap": "rejected",
    "billable_id_reused_for_new_billed_request": "rejected",
    "duplicate_usage_observation": "idempotent",
    "provider_request_id_unavailable": "explicit_unknown",
    "unknown_usage_as_zero": "rejected",
    "expired_lease_with_unresolved_dispatch": "hold",
    "fractional_subcent_usage": "exact_amount_preserved",
    "simulated_sales_live_inference_billing": "billing_modes_separated",
    "provenance_conflict": "review_or_unknown",
    "unapproved_asset": "rejected",
    "malformed_control": "rejected",
    "wrong_project_result": "rejected",
    "cancelled_deadline": "typed_cancelled_outcome",
    "unsupported_field_alias": "rejected",
    "old_consumer_additive_field": "compatibility_declared",
}

REQUIRED_FIELDS = {
    "schema_version", "event_id", "tenant_id", "project_id", "correlation_id",
    "producer", "occurred_at", "ingested_at", "environment", "billing_mode",
    "source_refs", "workflow_id", "run_id", "order_id", "design_version_id",
    "logical_operation_id", "billable_attempt_id", "transport_attempt_id",
    "provider_request_id",
}

REQUIRED_COVERAGE_TAGS = {
    "common_envelope",
    "financial_attempt_identities",
    "source_observation_identity",
    "immutable_facts_and_package",
    "artifact_version_hash_binding",
    "authenticated_actor_scope_action_expiry_revocation",
    "reservation_attempt_binding_and_lifecycle",
    "decision_question_policy_model_identity",
    "trace_and_usage_references",
}

ROLE_REPOSITORIES = {
    "Agent and research": {"agent-harness", "event-intelligence"},
    "Creative and fulfillment": {"creative-engine"},
    "Finance and reliability": {"cost-intelligence"},
    "Product and frontend": {"getticketstub.com"},
}

OWNER_ACTIONS = [
    {
        "owner": "shared-contracts",
        "reference": "https://github.com/Jevelopers-neatHack-2026/shared-contracts/issues/1",
        "action": "Publish the reviewed dependency-light canonical schemas, DecisionPort and approval contract, deterministic serialization rule, validator adapter, and valid, invalid, older-version, additive-field, and semantic reference fixtures.",
    },
    {
        "owner": "cost-intelligence",
        "reference": "https://github.com/Jevelopers-neatHack-2026/cost-intelligence/issues/1",
        "action": "Publish the reviewed attempt-bound reservation, usage, unknown-outcome, duplicate-observation, precision, billing-mode, and conservative-hold protocol and fixtures.",
    },
    {
        "owner": "getticketstub.com",
        "reference": "https://github.com/Jevelopers-neatHack-2026/getticketstub.com/issues/1",
        "action": "Publish the trusted server identity and exact approval-verification interface and fixtures; the harness consumes the authority and does not implement it.",
    },
    {
        "owner": "event-intelligence",
        "reference": "https://github.com/Jevelopers-neatHack-2026/event-intelligence/issues/3",
        "action": "Review and publish the research, trusted-scope, package, correction, and DecisionPort contract handoff identified by EI-X01.",
    },
    {
        "owner": "event-intelligence",
        "reference": "https://github.com/Jevelopers-neatHack-2026/event-intelligence/issues/4",
        "action": "Review and publish the attempt, reservation, metering, and trace-correlation contract handoff identified by EI-X02.",
    },
    {
        "owner": "creative-engine",
        "reference": "https://github.com/Jevelopers-neatHack-2026/creative-engine/issues/3",
        "action": "Review and publish the research-package alias, trusted scope, brief, control, and version contract handoff identified by CE-C01.",
    },
    {
        "owner": "creative-engine",
        "reference": "https://github.com/Jevelopers-neatHack-2026/creative-engine/issues/4",
        "action": "Review and publish the neutral DecisionPort and actual Choice, Score, and Noul fixtures identified by CE-C02.",
    },
    {
        "owner": "creative-engine",
        "reference": "https://github.com/Jevelopers-neatHack-2026/creative-engine/issues/5",
        "action": "Review and publish the external budget, attempt, usage, and unknown-outcome contract handoff identified by CE-C03.",
    },
    {
        "owner": "creative-engine",
        "reference": "https://github.com/Jevelopers-neatHack-2026/creative-engine/issues/6",
        "action": "Review and publish the exact facts, artwork, composition, approval, and handoff contract identified by CE-C04.",
    },
]


class InputProblem(Exception):
    def __init__(self, code: str, path: str, owner: str, action: str):
        super().__init__(code)
        self.item = {"code": code, "path": path, "owner": owner, "action": action}


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
    try:
        with path.open("r", encoding="utf-8") as stream:
            return json.load(stream, object_pairs_hook=_pairs_no_duplicates, parse_constant=_reject_non_finite)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
        raise InputProblem(
            "manifest_unreadable_or_invalid_json", "$", "Agent and research",
            "Supply a readable UTF-8 JSON readiness descriptor from the canonical contract owner.",
        ) from None


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def digest_ok(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def revision_ok(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{40}", value) is not None


def owner_issue_ref(value: Any, owner: str | None = None) -> bool:
    if not isinstance(value, str):
        return False
    if owner is not None:
        repositories = {owner}
    else:
        repositories = {repo for repos in ROLE_REPOSITORIES.values() for repo in repos}
    return any(
        re.fullmatch(rf"https://github\.com/Jevelopers-neatHack-2026/{re.escape(repo)}/(issues|pull)/[1-9][0-9]*", value)
        for repo in repositories
    )


def object_at(value: Any, path: str, owner: str, action: str, errors: list[dict[str, str]]) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    errors.append({"code": "object_required", "path": path, "owner": owner, "action": action})
    return {}


def require_text(value: Any, path: str, owner: str, action: str, errors: list[dict[str, str]]) -> bool:
    if isinstance(value, str) and value.strip():
        return True
    errors.append({"code": "required_text_missing", "path": path, "owner": owner, "action": action})
    return False


def local_file(base: Path, relative: Any, path: str, owner: str, errors: list[dict[str, str]]) -> Path | None:
    if not isinstance(relative, str) or not relative.strip():
        errors.append({"code": "artifact_path_missing", "path": path, "owner": owner,
                       "action": "Publish the pinned artifact inside the canonical release package."})
        return None
    candidate = (base / relative).resolve()
    try:
        candidate.relative_to(base.resolve())
    except ValueError:
        errors.append({"code": "artifact_path_escapes_release", "path": path, "owner": owner,
                       "action": "Publish the pinned artifact within the canonical release package; external paths are not followed."})
        return None
    if not candidate.is_file():
        errors.append({"code": "pinned_artifact_missing", "path": path, "owner": owner,
                       "action": "Publish the referenced canonical artifact at the pinned package path."})
        return None
    return candidate


def check_hash(base: Path, item: dict[str, Any], name: str, json_path: str,
               owner: str, errors: list[dict[str, str]]) -> Path | None:
    path = local_file(base, item.get("path"), json_path + ".path", owner, errors)
    expected = item.get("sha256")
    if not digest_ok(expected):
        errors.append({"code": "sha256_required", "path": json_path + ".sha256", "owner": owner,
                       "action": "Publish the SHA-256 of the exact canonical artifact bytes."})
    if path is not None and digest_ok(expected):
        try:
            actual = sha256_file(path)
        except OSError:
            actual = ""
        if actual != expected:
            errors.append({"code": "artifact_sha256_mismatch", "path": json_path, "owner": owner,
                           "action": "Republish the descriptor with the canonical artifact bytes and matching SHA-256."})
            return None
    return path


def validate_owner_decisions(manifest: dict[str, Any], errors: list[dict[str, str]]) -> None:
    decisions = manifest.get("owner_decisions")
    if not isinstance(decisions, list):
        decisions = []
    found: dict[str, dict[str, Any]] = {}
    for index, decision in enumerate(decisions):
        if not isinstance(decision, dict):
            errors.append({"code": "owner_decision_malformed", "path": f"$.owner_decisions[{index}]",
                           "owner": "Agent and research", "action": "Record an accountable external owner decision."})
            continue
        reference = decision.get("reference")
        if isinstance(reference, str):
            if reference in found:
                errors.append({"code": "duplicate_owner_decision", "path": f"$.owner_decisions[{index}].reference",
                               "owner": str(decision.get("owner", "Agent and research")),
                               "action": "Record exactly one current owner decision for each external handoff."})
            found[reference] = decision
    for requirement in OWNER_ACTIONS:
        decision = found.get(requirement["reference"])
        if not decision:
            errors.append({"code": "external_owner_decision_missing", "path": "$.owner_decisions",
                           "owner": requirement["owner"], "action": requirement["action"]})
            continue
        expected_owner = requirement["owner"]
        if decision.get("owner") != expected_owner or decision.get("status") != "accepted":
            errors.append({"code": "external_owner_decision_not_accepted", "path": "$.owner_decisions",
                           "owner": expected_owner, "action": requirement["action"]})
        if not owner_issue_ref(decision.get("decision_ref"), expected_owner):
            errors.append({"code": "owner_decision_reference_invalid", "path": "$.owner_decisions.decision_ref",
                           "owner": expected_owner, "action": "Link the accepted decision in the accountable owner's repository issue or pull request."})
        if not revision_ok(decision.get("revision")):
            errors.append({"code": "owner_decision_revision_missing", "path": "$.owner_decisions.revision",
                           "owner": expected_owner, "action": "Pin the accepted decision to a 40-character commit revision."})


def validate_crosswalk(manifest: dict[str, Any], schema_hashes: set[str], errors: list[dict[str, str]]) -> None:
    rows = manifest.get("field_crosswalk")
    if not isinstance(rows, list):
        rows = []
    seen: set[tuple[str, str]] = set()
    fields: set[str] = set()
    tags: set[str] = set()
    fields_owner_action = "Publish the accepted field-by-field crosswalk with producer and consumer review."
    required = {
        "field_name": "Record the canonical field spelling from the pinned schema; do not introduce an alias.",
        "meaning": fields_owner_action,
        "producer_role": fields_owner_action,
        "producer_leaf": fields_owner_action,
        "consumer_role": fields_owner_action,
        "consumer_leaf": fields_owner_action,
        "trusted_derivation": "Record whether the field is server-derived, owner-produced, externally verified, or unavailable.",
        "unavailable_semantics": "Record omitted versus explicit-unknown behavior from the owner schema.",
        "precision": "Record exact numeric precision and the owner-approved rounding boundary, or not-applicable with reason.",
        "privacy_retention": "Record the owner-approved privacy and retention classification.",
        "compatibility_rule": "Record the owner-approved additive and breaking-change rule.",
    }
    for index, row in enumerate(rows):
        path = f"$.field_crosswalk[{index}]"
        if not isinstance(row, dict):
            errors.append({"code": "crosswalk_row_malformed", "path": path, "owner": "shared-contracts",
                           "action": fields_owner_action})
            continue
        field = row.get("field_name")
        schema_sha = row.get("schema_sha256")
        if not isinstance(field, str) or not field.strip():
            errors.append({"code": "canonical_field_name_missing", "path": path + ".field_name",
                           "owner": "shared-contracts", "action": required["field_name"]})
        if not digest_ok(schema_sha) or schema_sha not in schema_hashes:
            errors.append({"code": "crosswalk_schema_pin_missing", "path": path + ".schema_sha256",
                           "owner": "shared-contracts", "action": "Pin each row to an artifact SHA-256 published in this release."})
        elif isinstance(field, str):
            pair = (schema_sha, field)
            if pair in seen:
                errors.append({"code": "duplicate_canonical_field", "path": path + ".field_name",
                               "owner": "shared-contracts", "action": "Keep one crosswalk row per canonical field per schema artifact."})
            seen.add(pair)
            fields.add(field)
        for key, action in required.items():
            require_text(row.get(key), path + "." + key, "shared-contracts", action, errors)
        for role_key, leaf_key in (("producer_role", "producer_leaf"), ("consumer_role", "consumer_leaf")):
            role = row.get(role_key)
            leaf = row.get(leaf_key)
            repositories = ROLE_REPOSITORIES.get(role, set()) if isinstance(role, str) else set()
            if not owner_issue_ref(leaf) or not any(
                isinstance(leaf, str) and f"/Jevelopers-neatHack-2026/{repository}/" in leaf for repository in repositories
            ):
                errors.append({"code": "affected_role_leaf_reference_invalid", "path": path + "." + leaf_key,
                               "owner": role if isinstance(role, str) else "Agent and research",
                               "action": "Link the exact affected producer or consumer leaf in its authorized owner repository."})
        coverage = row.get("coverage_tags", [])
        if not isinstance(coverage, list) or any(not isinstance(tag, str) for tag in coverage):
            errors.append({"code": "crosswalk_coverage_tags_invalid", "path": path + ".coverage_tags",
                           "owner": "Agent and research", "action": "Tag the row with the issue #5 crosswalk categories it covers."})
        else:
            tags.update(coverage)
        for side in ("producer_review", "consumer_review"):
            review = row.get(side)
            if not isinstance(review, dict) or review.get("status") != "accepted":
                role_key = "producer_role" if side == "producer_review" else "consumer_role"
                role = row.get(role_key)
                errors.append({"code": "affected_role_review_missing", "path": path + "." + side,
                               "owner": str(role) if isinstance(role, str) else "Agent and research",
                               "action": "Record accepted review by the affected producer and consumer role for this canonical field."})
                continue
            for key in ("role", "reference"):
                if not isinstance(review.get(key), str) or not review[key].strip():
                    errors.append({"code": "affected_role_review_incomplete", "path": path + "." + side + "." + key,
                                   "owner": str(review.get("role", "Agent and research")),
                                   "action": "Record the reviewer role and its accountable issue reference."})
            role_key = "producer_role" if side == "producer_review" else "consumer_role"
            leaf_key = "producer_leaf" if side == "producer_review" else "consumer_leaf"
            role = row.get(role_key)
            if review.get("role") != role or review.get("reference") != row.get(leaf_key):
                errors.append({"code": "affected_role_review_reference_mismatch", "path": path + "." + side,
                               "owner": role if isinstance(role, str) else "Agent and research",
                               "action": "Bind each accepted role review to the exact producer or consumer leaf named in this field crosswalk row."})
            if not revision_ok(review.get("revision")):
                errors.append({"code": "affected_role_revision_missing", "path": path + "." + side + ".revision",
                               "owner": str(review.get("role", "Agent and research")),
                               "action": "Pin affected-role review to the reviewed commit."})
    for field in sorted(REQUIRED_FIELDS - fields):
        errors.append({"code": "required_catalog_field_missing", "path": "$.field_crosswalk",
                       "owner": "shared-contracts", "action": f"Include catalog field {field!r} in the accepted field crosswalk."})
    for tag in sorted(REQUIRED_COVERAGE_TAGS - tags):
        errors.append({"code": "required_crosswalk_topic_missing", "path": "$.field_crosswalk",
                       "owner": "shared-contracts", "action": f"Cover the required issue #5 crosswalk topic {tag!r} with canonical fields."})


def validate_and_run(manifest: Any, manifest_path: Path, errors: list[dict[str, str]]) -> dict[str, Any] | None:
    if not isinstance(manifest, dict):
        errors.append({"code": "manifest_object_required", "path": "$", "owner": "Agent and research",
                       "action": "Supply a canonical owner release descriptor as a JSON object."})
        return None
    if manifest.get("format") != FORMAT:
        errors.append({"code": "readiness_descriptor_format_unsupported", "path": "$.format",
                       "owner": "Agent and research", "action": f"Publish a descriptor using {FORMAT}."})
    status = manifest.get("status")
    if status != "published":
        owner_actions = manifest.get("owner_actions")
        if isinstance(owner_actions, list) and owner_actions:
            for index, action in enumerate(owner_actions):
                if isinstance(action, dict):
                    errors.append({
                        "code": "canonical_release_not_published",
                        "path": f"$.owner_actions[{index}]",
                        "owner": str(action.get("owner", "shared-contracts")),
                        "action": str(action.get("action", "Publish the canonical contract release and owner fixtures.")),
                    })
        else:
            for item in OWNER_ACTIONS:
                errors.append({"code": "canonical_release_not_published", "path": "$.canonical_release",
                               "owner": item["owner"], "action": item["action"]})
        return None

    base = manifest_path.resolve().parent
    release = object_at(manifest.get("canonical_release"), "$.canonical_release", "shared-contracts",
                        "Publish the pinned canonical release metadata and artifacts.", errors)
    if release.get("repository") != "Jevelopers-neatHack-2026/shared-contracts":
        errors.append({"code": "canonical_repository_mismatch", "path": "$.canonical_release.repository",
                       "owner": "shared-contracts", "action": "Publish the release from the canonical shared-contracts repository."})
    if not revision_ok(release.get("revision")):
        errors.append({"code": "canonical_revision_missing", "path": "$.canonical_release.revision",
                       "owner": "shared-contracts", "action": "Pin the canonical release to its exact 40-character commit."})
    require_text(release.get("version"), "$.canonical_release.version", "shared-contracts",
                 "Publish an explicit contract release version.", errors)

    artifacts = release.get("schema_artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        artifacts = []
        errors.append({"code": "canonical_schema_artifacts_missing", "path": "$.canonical_release.schema_artifacts",
                       "owner": "shared-contracts", "action": "Publish executable canonical schema/package artifacts and pin their SHA-256 values."})
    schema_hashes: set[str] = set()
    for index, artifact in enumerate(artifacts):
        path = f"$.canonical_release.schema_artifacts[{index}]"
        if not isinstance(artifact, dict):
            errors.append({"code": "schema_artifact_malformed", "path": path, "owner": "shared-contracts",
                           "action": "Publish each schema artifact with its path and exact SHA-256."})
            continue
        if check_hash(base, artifact, "schema", path, "shared-contracts", errors):
            schema_hashes.add(str(artifact.get("sha256")))
    validate_owner_decisions(manifest, errors)
    validate_crosswalk(manifest, schema_hashes, errors)

    compatibility = object_at(manifest.get("compatibility"), "$.compatibility", "shared-contracts",
                              "Record the owner decision for supported old consumers and additive fields.", errors)
    additive = compatibility.get("unknown_additive_fields")
    if additive not in {"preserve", "ignore"}:
        errors.append({"code": "additive_field_policy_missing", "path": "$.compatibility.unknown_additive_fields",
                       "owner": "shared-contracts", "action": "Record whether unknown additive fields are preserved or ignored, with owner approval."})

    adapter = object_at(release.get("validator_adapter"), "$.canonical_release.validator_adapter", "shared-contracts",
                        "Publish the pinned offline canonical fixture validator adapter.", errors)
    if adapter.get("protocol") != ADAPTER_PROTOCOL:
        errors.append({"code": "validator_adapter_protocol_missing", "path": "$.canonical_release.validator_adapter.protocol",
                       "owner": "shared-contracts", "action": f"Publish an offline adapter implementing {ADAPTER_PROTOCOL}; this is a readiness tool protocol, not a runtime schema."})
    if adapter.get("offline_attested") is not True:
        errors.append({"code": "validator_adapter_not_offline_attested", "path": "$.canonical_release.validator_adapter.offline_attested",
                       "owner": "shared-contracts", "action": "Publish an owner-reviewed adapter that executes only local fixtures and does not contact providers or services."})
    adapter_path = check_hash(base, adapter, "validator adapter", "$.canonical_release.validator_adapter",
                              "shared-contracts", errors)

    fixture_rows = manifest.get("fixtures")
    if not isinstance(fixture_rows, list):
        fixture_rows = []
    fixtures: dict[str, dict[str, Any]] = {}
    for index, fixture in enumerate(fixture_rows):
        path = f"$.fixtures[{index}]"
        if not isinstance(fixture, dict):
            errors.append({"code": "fixture_manifest_entry_malformed", "path": path, "owner": "shared-contracts",
                           "action": "Publish each owner fixture with its case ID, path, bytes hash, expected outcome, and optional canonical serialization hash."})
            continue
        case_id = fixture.get("case_id")
        if not isinstance(case_id, str) or case_id not in REQUIRED_CASES:
            errors.append({"code": "fixture_case_id_unsupported", "path": path + ".case_id", "owner": "Agent and research",
                           "action": "Use the exact ACV1-CONTRACTS fixture case IDs listed in the harness readiness plan."})
            continue
        if case_id in fixtures:
            errors.append({"code": "duplicate_fixture_case", "path": path + ".case_id", "owner": "shared-contracts",
                           "action": "Publish exactly one current pinned fixture for each required case ID."})
            continue
        fixtures[case_id] = fixture
        if check_hash(base, fixture, "fixture", path, "shared-contracts", errors) is None:
            continue
        if fixture.get("data_classification") not in {"synthetic", "sanitized_non_customer"}:
            errors.append({"code": "fixture_data_classification_missing", "path": path + ".data_classification",
                           "owner": "shared-contracts", "action": "Mark fixtures synthetic or sanitized non-customer data; never publish secrets or customer content."})
        expected = REQUIRED_CASES[case_id]
        if case_id == "unknown_additive_fields" or case_id == "old_consumer_additive_field":
            if additive in {"preserve", "ignore"}:
                expected = "accepted_and_preserved" if additive == "preserve" else "accepted_and_ignored"
        if fixture.get("expected_outcome") != expected:
            errors.append({"code": "fixture_expected_outcome_disagrees", "path": path + ".expected_outcome",
                           "owner": "shared-contracts", "action": f"Set the expected result to the accepted contract decision for case {case_id!r}."})
        if case_id in {"current_valid", "supported_older_version", "unknown_additive_fields", "fractional_subcent_usage"}:
            if not digest_ok(fixture.get("serialized_sha256")):
                errors.append({"code": "canonical_serialization_hash_missing", "path": path + ".serialized_sha256",
                               "owner": "shared-contracts", "action": "Publish the reference-byte hash using the canonical owner serialization rule."})
    for case_id in REQUIRED_CASES:
        if case_id not in fixtures:
            errors.append({"code": "required_fixture_missing", "path": "$.fixtures",
                           "owner": "shared-contracts", "action": f"Publish the owner fixture for required case {case_id!r}."})

    if errors:
        return None

    request = {
        "protocol": ADAPTER_PROTOCOL,
        "release_revision": release["revision"],
        "release_version": release["version"],
        "unknown_additive_fields": additive,
        "fixtures": [
            {
                "case_id": case_id,
                "path": str((base / fixtures[case_id]["path"]).resolve()),
                "sha256": fixtures[case_id]["sha256"],
                "expected_outcome": fixtures[case_id]["expected_outcome"],
                "serialized_sha256": fixtures[case_id].get("serialized_sha256"),
            }
            for case_id in REQUIRED_CASES
        ],
    }
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "PYTHONIOENCODING": "utf-8", "PYTHONHASHSEED": "0"}
    try:
        completed = subprocess.run(
            [sys.executable, str(adapter_path)],
            input=json.dumps(request, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
            text=True,
            capture_output=True,
            timeout=90,
            cwd=base,
            env=env,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        errors.append({"code": "canonical_validator_execution_failed", "path": "$.canonical_release.validator_adapter",
                       "owner": "shared-contracts", "action": "Repair and republish the pinned offline canonical validator adapter."})
        return None
    if completed.returncode != 0:
        errors.append({"code": "canonical_fixture_validation_failed", "path": "$.fixtures",
                       "owner": "shared-contracts", "action": "Correct the canonical fixture or owner-approved contract rule; inspect adapter diagnostics outside this sanitized report."})
        return None
    try:
        response = json.loads(completed.stdout, object_pairs_hook=_pairs_no_duplicates)
    except (json.JSONDecodeError, ValueError):
        errors.append({"code": "canonical_validator_output_invalid", "path": "$.canonical_release.validator_adapter",
                       "owner": "shared-contracts", "action": "Republish the adapter to return one protocol-conforming JSON result object."})
        return None
    if not isinstance(response, dict) or response.get("protocol") != ADAPTER_PROTOCOL:
        errors.append({"code": "canonical_validator_protocol_mismatch", "path": "$.canonical_release.validator_adapter",
                       "owner": "shared-contracts", "action": f"Return a result object implementing {ADAPTER_PROTOCOL}."})
        return None
    if response.get("release_revision") != release.get("revision"):
        errors.append({"code": "canonical_validator_revision_mismatch", "path": "$.canonical_release.validator_adapter",
                       "owner": "shared-contracts", "action": "Return results for exactly the pinned canonical release revision."})
        return None
    results = response.get("results")
    if not isinstance(results, list):
        errors.append({"code": "canonical_validator_results_missing", "path": "$.canonical_release.validator_adapter",
                       "owner": "shared-contracts", "action": "Return one typed result for every required fixture case."})
        return None
    result_by_id: dict[str, dict[str, Any]] = {}
    for result in results:
        if isinstance(result, dict) and isinstance(result.get("case_id"), str):
            if result["case_id"] in result_by_id:
                errors.append({"code": "canonical_validator_duplicate_result", "path": "$.canonical_release.validator_adapter.results",
                               "owner": "shared-contracts", "action": "Return exactly one result per required fixture case."})
            result_by_id[result["case_id"]] = result
    for case_id, fixture in fixtures.items():
        result = result_by_id.get(case_id)
        if result is None:
            errors.append({"code": "canonical_validator_case_result_missing", "path": "$.canonical_release.validator_adapter.results",
                           "owner": "shared-contracts", "action": f"Return a result for fixture case {case_id!r}."})
            continue
        if result.get("outcome") != fixture.get("expected_outcome"):
            errors.append({"code": "canonical_fixture_outcome_mismatch", "path": "$.canonical_release.validator_adapter.results",
                           "owner": "shared-contracts", "action": f"Make canonical fixture case {case_id!r} conform to its owner-approved expected outcome."})
        expected_serialization = fixture.get("serialized_sha256")
        if expected_serialization and result.get("serialized_sha256") != expected_serialization:
            errors.append({"code": "canonical_serialization_hash_mismatch", "path": "$.canonical_release.validator_adapter.results",
                           "owner": "shared-contracts", "action": f"Make canonical serialization for fixture case {case_id!r} match the owner-published reference bytes."})
    extras = set(result_by_id) - set(REQUIRED_CASES)
    if extras:
        errors.append({"code": "canonical_validator_unexpected_case_result", "path": "$.canonical_release.validator_adapter.results",
                       "owner": "shared-contracts", "action": "Return only case IDs declared in the owner-published release descriptor."})
    if errors:
        return None
    return {
        "canonical_revision": release["revision"],
        "canonical_version": release["version"],
        "schema_sha256": sorted(schema_hashes),
        "fixture_sha256": {case_id: fixtures[case_id]["sha256"] for case_id in REQUIRED_CASES},
        "validated_fixture_cases": len(REQUIRED_CASES),
        "scope": "offline canonical fixture conformance only; no producer/consumer runtime, live provider, or global acceptance claim",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--offline", action="store_true", help="Require local pinned artifacts and execute only the owner-attested offline validator adapter.")
    args = parser.parse_args(argv)
    errors: list[dict[str, str]] = []
    result: dict[str, Any] | None = None
    if not args.offline:
        errors.append({"code": "offline_mode_required", "path": "$", "owner": "Agent and research",
                       "action": "Run the pinned fixture verifier with --offline; live/provider execution is not part of this gate."})
    try:
        manifest = read_json(args.manifest)
        if args.offline and not errors:
            result = validate_and_run(manifest, args.manifest, errors)
    except InputProblem as problem:
        errors.append(problem.item)
    if errors:
        output = {"tool": "verify_contract_release", "status": "blocked", "ready": False,
                  "dependency_mode": "offline", "errors": sorted(errors, key=lambda item: (item["path"], item["code"], item["owner"]))}
        print(json.dumps(output, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
        return 2
    if result is None:
        output = {"tool": "verify_contract_release", "status": "blocked", "ready": False,
                  "dependency_mode": "offline", "errors": [{"code": "canonical_release_not_verified", "path": "$.canonical_release",
                  "owner": "shared-contracts", "action": "Publish canonical schemas, owner fixtures and adapter before readiness can pass."}]}
        print(json.dumps(output, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
        return 2
    print(json.dumps({"tool": "verify_contract_release", "status": "offline_conformance_passed",
                      "ready": True, "dependency_mode": "offline", "result": result},
                     sort_keys=True, separators=(",", ":"), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
