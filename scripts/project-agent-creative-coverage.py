#!/usr/bin/env python3
"""Build an explicit, source-pinned v2 view of the ACV1 coverage workflow.

This is a local projection tool. It performs no network calls and does not
modify its inputs. The canonical checklist is read only for its caller-supplied
pin; its contents are never copied into the projection.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import stat
import sys
from pathlib import Path
from typing import Any


MAX_INPUT = 8 * 1024 * 1024
MAX_OUTPUT = 8 * 1024 * 1024
MAX_ROWS = 100_000
SAFE_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
HASH = re.compile(r"^[0-9a-f]{64}$")
STAGES = {
    "accepted_policy_fixture", "canonical_agreement_and_fixture",
    "final_acceptance", "live_evidence", "readiness_config", "runnable",
}
EXTRA_ROLES = {
    "provider_configuration_readiness", "contract_fixture_prerequisite",
    "creative_consumer_readiness", "research_consumer_readiness",
    "target_policy_prerequisite", "coordination_coverage_contribution",
    "evaluation_acceptance_contribution", "release_acceptance_contribution",
    "metering_contract_prerequisite", "consumer_boundary_acceptance",
    "bounded_recovery_implementation", "cross_family_acceptance_implementation",
    "decision_port_injection_prerequisite", "harness_transport_implementation",
    "release_verification_contribution", "target_validation_acceptance",
    "selected_artifact_boundary_candidate",
}
REQUIRED_STAGE_REASONS = {
    "live body prerequisite",
    "explicit transitive closure of all 59 required leaves, not additional implementation ordering",
    "live dispatch only; no additional block on offline implementation",
    "external artifact, not external epic closure",
    "additional exact external-owner/action obligation inside authorized repositories",
    "access obligation",
    "accepted decision required, proposed values are not accepted policy",
    "mandatory declared out-of-scope boundary acceptance",
}


class ProjectionError(Exception):
    pass


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ProjectionError("duplicate-json-key")
        result[key] = value
    return result


def _finite_float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ProjectionError("invalid-number")
    return number


def _validate_json_strings(value: Any, role: str) -> None:
    pending = [value]
    while pending:
        current = pending.pop()
        if isinstance(current, str):
            try:
                current.encode("utf-8")
            except UnicodeEncodeError as exc:
                raise ProjectionError(f"{role}-invalid-string") from exc
        elif isinstance(current, dict):
            pending.extend(current.keys())
            pending.extend(current.values())
        elif isinstance(current, list):
            pending.extend(current)


def _load(path: str, role: str, limit: int = MAX_INPUT) -> tuple[Any, bytes]:
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
    except OSError as exc:
        raise ProjectionError(f"{role}-unreadable") from exc
    raw = b""
    failure: str | None = None
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
            raise ProjectionError(f"{role}-type-or-size")
        chunks: list[bytes] = []
        size = 0
        while True:
            block = os.read(fd, min(65536, limit + 1 - size))
            if not block:
                break
            chunks.append(block)
            size += len(block)
            if size > limit:
                raise ProjectionError(f"{role}-type-or-size")
        raw = b"".join(chunks)
        if len(raw) != info.st_size:
            raise ProjectionError(f"{role}-changed-during-read")
    except ProjectionError:
        raise
    except OSError as exc:
        failure = f"{role}-read-failed"
        cause = exc
    finally:
        try:
            os.close(fd)
        except OSError as exc:
            if failure is None:
                failure = f"{role}-close-failed"
                cause = exc
    if failure is not None:
        raise ProjectionError(failure) from cause
    try:
        value = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ProjectionError("invalid-number")),
            parse_float=_finite_float,
        )
    except ProjectionError:
        raise
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ProjectionError(f"{role}-invalid-json") from exc
    _validate_json_strings(value, role)
    return value, raw


def _write(path: str, raw: bytes) -> None:
    target = Path(path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(target, flags, 0o600)
    except OSError as exc:
        raise ProjectionError("output-must-be-new-regular-file") from exc
    failure: OSError | None = None
    try:
        view = memoryview(raw)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short-write")
            view = view[written:]
    except OSError as exc:
        failure = exc
    try:
        os.close(fd)
    except OSError as exc:
        if failure is None:
            failure = exc
    if failure is not None:
        try:
            target.unlink()
        except OSError:
            pass
        raise ProjectionError("output-write-failed") from failure


def _digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _git_blob(raw: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(raw)).encode("ascii") + b"\0" + raw).hexdigest()


def _need_list(value: Any, name: str) -> list[Any]:
    if not isinstance(value, list) or len(value) > MAX_ROWS:
        raise ProjectionError(f"{name}-invalid")
    return value


def _readback_index(readbacks: dict[str, Any]) -> dict[str, dict[str, Any]]:
    issues = _need_list(readbacks.get("issues"), "readback-issues")
    result: dict[str, dict[str, Any]] = {}
    for row in issues:
        if not isinstance(row, dict) or not isinstance(row.get("url"), str):
            raise ProjectionError("readback-row-invalid")
        if row["url"] in result:
            raise ProjectionError("readback-url-duplicate")
        result[row["url"]] = row
    return result


def project(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
    source, source_raw = _load(args.coverage_map, "coverage-map", 8 * 1024 * 1024)
    pair_review, pair_raw = _load(args.pair_review, "pair-review")
    stage_review, stage_raw = _load(args.stage_review, "stage-review")
    external, external_raw = _load(args.external_artifacts, "external-artifacts")
    readbacks, readbacks_raw = _load(args.issue_readbacks, "issue-readbacks", 16 * 1024 * 1024)
    canonical, canonical_raw = _load(args.canonical_checklist, "canonical-checklist", 1 * 1024 * 1024)
    canonical_ref, canonical_ref_raw = _load(args.canonical_reference, "canonical-reference", 2 * 1024 * 1024)
    if not all(isinstance(x, dict) for x in (source, pair_review, stage_review, external, readbacks, canonical, canonical_ref)):
        raise ProjectionError("input-root-invalid")
    if not isinstance(source.get("entries"), list) or not isinstance(source.get("ownership_rows"), list) or not isinstance(source.get("source_contribution_rows"), list):
        raise ProjectionError("coverage-source-families-missing")
    if len(source["entries"]) > 10_000 or len(source["ownership_rows"]) > MAX_ROWS or len(source["source_contribution_rows"]) > MAX_ROWS:
        raise ProjectionError("coverage-source-row-limit")
    if readbacks.get("pages_complete") is not True:
        raise ProjectionError("readback-pages-incomplete")
    if canonical_ref.get("repository") != args.canonical_repository or canonical_ref.get("source_path") != args.canonical_source_path:
        raise ProjectionError("canonical-reference-argument-mismatch")
    for field, expected in (("issue_pinned_revision", args.canonical_issue_revision), ("captured_main_revision", args.canonical_main_revision)):
        if canonical_ref.get(field) != expected or not isinstance(expected, str) or re.fullmatch(r"[0-9a-f]{40}", expected) is None:
            raise ProjectionError("canonical-revision-invalid")
    criteria = canonical.get("criteria")
    if not isinstance(criteria, list) or len(criteria) > 10_000:
        raise ProjectionError("canonical-criteria-invalid")
    criterion_ids = {row.get("id") for row in criteria if isinstance(row, dict) and isinstance(row.get("id"), str)}

    pair_rows = _need_list(pair_review.get("pairs"), "pair-review-rows")
    pair_review_rows: list[dict[str, Any]] = []
    extras: dict[tuple[str, str], dict[str, Any]] = {}
    additions: dict[tuple[str, str], dict[str, Any]] = {}
    unresolved: list[dict[str, Any]] = []
    for index, row in enumerate(pair_rows):
        if not isinstance(row, dict) or not isinstance(row.get("requirement_id"), str) or not isinstance(row.get("leaf"), str):
            raise ProjectionError("pair-review-row-invalid")
        pair = (row["requirement_id"], row["leaf"])
        pair_review_rows.append({**row, "review_row_id": f"PAIR-{index + 1:04d}",
                                 "source_pointer": f"/pairs/{index}"})
        decision = row.get("after_recommendation")
        if not isinstance(decision, str):
            raise ProjectionError("pair-review-decision-invalid")
        if decision == "PRESERVE_TYPED_SCOPED_CONTRIBUTION":
            role = row.get("recommended_role")
            if not isinstance(role, str) or role not in EXTRA_ROLES or pair in extras:
                raise ProjectionError("typed-extra-role-invalid")
            extras[pair] = {"requirement_id": pair[0], "leaf_key": pair[1], "role": role,
                            "source_pointer": f"/pairs/{index}", "evidence": row.get("evidence", [])}
        elif decision in {"ADD_SCOPED_ASSOCIATION", "ADD_SCOPED_ASSOCIATION_WITH_ACCOUNTABILITY_QUALIFICATION"}:
            role = row.get("recommended_role")
            if not isinstance(role, str) or not SAFE_KEY.fullmatch(role) or pair in additions:
                raise ProjectionError("addition-role-invalid")
            additions[pair] = {"requirement_id": pair[0], "leaf_key": pair[1], "recommended_role": role,
                               "source_pointer": f"/pairs/{index}", "decision": decision,
                               "evidence": row.get("evidence", [])}
        elif decision == "RETAIN_UNRESOLVED_ACCOUNTABILITY_DO_NOT_AUTO_ADD":
            unresolved.append({"requirement_id": pair[0], "leaf_key": pair[1],
                               "source_pointer": f"/pairs/{index}", "review_row_id": f"PAIR-{index + 1:04d}",
                               "decision": decision})
        else:
            raise ProjectionError("pair-review-decision-unsupported")

    readback_by_url = _readback_index(readbacks)
    entries: list[dict[str, Any]] = []
    source_entries = source["entries"]
    for index, item in enumerate(source_entries):
        if not isinstance(item, dict) or not isinstance(item.get("key"), str):
            raise ProjectionError("coverage-entry-invalid")
        url = item.get("issue_url")
        observed = readback_by_url.get(url) if isinstance(url, str) else None
        if isinstance(url, str) and observed is None:
            raise ProjectionError("required-issue-readback-missing")
        record = dict(item)
        record["record_type"] = "delivery_leaf"
        record["required_for_v1"] = item.get("required_for_v1", True) is True
        record["node_roles"] = ["required_acceptance_outcome"]
        # The legacy `tests` member records observed executions on this map;
        # it is not a declaration of every test required for release. Keep it
        # unchanged and make the absent required-test contract explicit.
        if "required_tests" in item and isinstance(item.get("required_tests"), list):
            record["required_tests"] = list(item["required_tests"])
            record["required_test_declaration_status"] = "declared_in_source"
        else:
            record["required_tests"] = []
            record["required_test_declaration_status"] = "not_declared_in_source"
        if "tests" in item:
            record["historical_test_results_pointer"] = f"/entries/{index}/tests"
        if item.get("key") == "ACV1-RELEASE-GATE":
            record["node_roles"].append("release_gate_fan_in")
        record["historical_issue_title"] = item.get("issue_title")
        record["historical_issue_state"] = item.get("live_issue_state")
        record["historical_body_sha256"] = item.get("body_sha256")
        if isinstance(observed, dict):
            body_digest = observed.get("body_sha256")
            if body_digest is None:
                body = observed.get("body")
                if not isinstance(body, str):
                    raise ProjectionError("readback-body-missing")
                body_digest = _digest(body.encode("utf-8"))
            if not isinstance(body_digest, str) or not HASH.fullmatch(body_digest):
                raise ProjectionError("readback-body-digest-invalid")
            record["current_readback"] = {
                "repository": observed.get("repository"), "number": observed.get("number"),
                "url": item["issue_url"], "title": observed.get("title"), "state": observed.get("state"),
                "body_sha256": body_digest,
            }
            record["body_semantic_disposition"] = "unadjudicated"
        record["source_pointer"] = f"/entries/{index}"
        entries.append(record)

    ownership_rows: list[dict[str, Any]] = []
    accountable_pairs: set[tuple[str, str]] = set()
    contributor_pairs: set[tuple[str, str]] = set()
    ownership_ids: set[str] = set()
    entry_keys = {item.get("key") for item in entries}
    for index, row in enumerate(source["ownership_rows"]):
        if not isinstance(row, dict) or not isinstance(row.get("requirement_id"), str):
            raise ProjectionError("ownership-row-invalid")
        requirement_id = row["requirement_id"]
        if requirement_id in ownership_ids:
            raise ProjectionError("ownership-id-duplicate")
        ownership_ids.add(requirement_id)
        accountable = row.get("accountable_delivery_leaf")
        acceptance = row.get("acceptance_owner_leaf")
        contributors = row.get("required_contributors", [])
        if (
            not isinstance(accountable, str) or accountable not in entry_keys
            or not isinstance(acceptance, str) or acceptance not in entry_keys
            or not isinstance(contributors, list)
            or any(not isinstance(x, str) or x not in entry_keys for x in contributors)
        ):
            raise ProjectionError("ownership-endpoint-invalid")
        accountable_pairs.add((requirement_id, accountable))
        contributor_pairs.update((requirement_id, leaf) for leaf in contributors)
        projected = dict(row)
        projected["scope"] = "scoped_contribution" if requirement_id in criterion_ids else "local"
        projected["source_pointer"] = f"/ownership_rows/{index}"
        ownership_rows.append(projected)

    actual_rows: list[dict[str, Any]] = []
    actual_pairs: set[tuple[str, str]] = set()
    for index, row in enumerate(source["source_contribution_rows"]):
        if not isinstance(row, dict):
            raise ProjectionError("contribution-row-invalid")
        requirement_id, leaf = row.get("requirement_id"), row.get("owning_leaf_key")
        if not isinstance(requirement_id, str) or not isinstance(leaf, str):
            raise ProjectionError("contribution-pair-invalid")
        pair = (requirement_id, leaf)
        actual_pairs.add(pair)
        if pair in extras:
            role = extras[pair]["role"]
        elif pair in accountable_pairs and pair in contributor_pairs:
            role = "accountable_delivery_and_required_contributor"
        elif pair in accountable_pairs:
            role = "accountable_delivery"
        elif pair in contributor_pairs:
            role = "required_contributor"
        else:
            raise ProjectionError("untyped-source-contribution")
        projected = dict(row)
        projected["row_id"] = f"SRC-{index + 1:04d}"
        projected["source_pointer"] = f"/source_contribution_rows/{index}"
        projected["scope"] = "scoped_contribution" if requirement_id in criterion_ids else "local"
        projected["relation_role"] = role
        actual_rows.append(projected)

    unresolved_pairs = {
        (row["requirement_id"], row["leaf_key"])
        for row in unresolved
    }
    entries_by_key = {row["key"]: row for row in entries}
    for entry in entries:
        original_ids = entry.get("requirement_ids")
        if not isinstance(original_ids, list):
            entry["source_requirement_ids"] = original_ids
            entry["requirement_ids"] = []
        else:
            entry["source_requirement_ids"] = list(original_ids)
            entry["requirement_ids"] = list(original_ids)
        entry["requirement_id_sources"] = []
    for index, row in enumerate(ownership_rows):
        ident = row["requirement_id"]
        pointer = f"/ownership_rows/{index}"
        endpoints: dict[str, list[str]] = {}
        accountable = row.get("accountable_delivery_leaf")
        if isinstance(accountable, str) and (ident, accountable) not in unresolved_pairs:
            endpoints.setdefault(accountable, []).append("accountable_delivery")
        for contributor in row.get("required_contributors", []):
            if isinstance(contributor, str) and (ident, contributor) not in unresolved_pairs:
                roles = endpoints.setdefault(contributor, [])
                if "required_contributor" not in roles:
                    roles.append("required_contributor")
        for leaf, endpoint_roles in endpoints.items():
            entry = entries_by_key.get(leaf)
            if entry is None:
                continue
            if ident not in entry["requirement_ids"]:
                entry["requirement_ids"].append(ident)
            entry["requirement_id_sources"].append({
                "requirement_id": ident,
                "ownership_row_pointer": pointer,
                "endpoint_roles": endpoint_roles,
            })

    if set(extras) - actual_pairs:
        raise ProjectionError("reviewed-extra-not-present-in-source")
    if set(additions) - (accountable_pairs | contributor_pairs):
        raise ProjectionError("addition-not-declared-by-ownership")
    association_additions = []
    for pair, row in sorted(additions.items()):
        if pair in actual_pairs:
            raise ProjectionError("addition-already-present-in-source")
        pointer_index = int(row["source_pointer"].split("/")[-1])
        association_additions.append(dict(row, association_id=f"ADD-{len(association_additions)+1:02d}",
                                           review_row_id=f"PAIR-{pointer_index + 1:04d}"))

    relation_rows = _need_list(stage_review.get("rows"), "stage-relation-rows")
    stage_relations: list[dict[str, Any]] = []
    for index, row in enumerate(relation_rows):
        if not isinstance(row, dict):
            raise ProjectionError("stage-relation-row-invalid")
        consumer, producer = row.get("consumer_key"), row.get("prerequisite_key")
        consumer_stage, producer_stage = row.get("consumer_stage"), row.get("prerequisite_stage")
        if (
            not isinstance(consumer, str) or consumer not in entry_keys
            or not isinstance(consumer_stage, str) or consumer_stage not in STAGES
            or not isinstance(producer_stage, str) or producer_stage not in STAGES
            or not isinstance(row.get("external_endpoint"), bool)
        ):
            raise ProjectionError("stage-relation-endpoint-or-stage-invalid")
        external_id = producer if row.get("external_endpoint") is True else None
        if not isinstance(producer, str) or (external_id is None and producer not in entry_keys):
            raise ProjectionError("stage-relation-producer-invalid")
        reason = row.get("reason")
        if reason is not None and not isinstance(reason, str):
            raise ProjectionError("stage-relation-reason-invalid")
        applicability = "required" if isinstance(reason, str) and reason in REQUIRED_STAGE_REASONS else "unresolved"
        receipt_ids = row.get("required_owner_receipt_ids", [])
        if (
            not isinstance(receipt_ids, list)
            or any(not isinstance(value, str) or not SAFE_KEY.fullmatch(value) for value in receipt_ids)
            or len(receipt_ids) != len(set(receipt_ids))
        ):
            raise ProjectionError("stage-relation-receipts-invalid")
        projected = {
            "relation_id": f"STG-{index:03d}", "source_pointer": row.get("source_pointer"),
            "consumer_key": consumer, "consumer_requirement_ids": row.get("consumer_requirement_ids"),
            "consumer_stage": consumer_stage, "producer_key": None if external_id else producer,
            "producer_requirement_ids": row.get("prerequisite_requirement_ids"),
            "external_artifact_id": external_id, "prerequisite_stage": producer_stage,
            "deliverable": row.get("deliverable"), "reason": reason,
            "source_evidence_file": row.get("evidence_file"), "source_evidence_line": row.get("evidence_line"),
            "shared_requirement_ids": row.get("shared_requirement_ids"),
            "external_endpoint": row.get("external_endpoint"), "disjoint_internal": row.get("disjoint_internal"),
            "status": row.get("status"), "applicability": applicability,
            "required_owner_receipt_ids": list(receipt_ids),
            "application_scope": "live_only" if reason == "live dispatch only; no additional block on offline implementation" else "declared_stage",
        }
        stage_relations.append(projected)

    external_nodes = _need_list(external.get("nodes"), "external-artifact-nodes")
    node_by_id: dict[str, dict[str, Any]] = {}
    for index, node in enumerate(external_nodes):
        if not isinstance(node, dict) or not isinstance(node.get("artifact_id"), str) or not SAFE_KEY.fullmatch(node["artifact_id"]):
            raise ProjectionError("external-artifact-row-invalid")
        ident = node["artifact_id"]
        if ident in node_by_id:
            raise ProjectionError("external-artifact-duplicate")
        if (
            not isinstance(node.get("owner_role"), str) or not node.get("owner_role", "").strip()
            or not isinstance(node.get("deliverable"), str) or not node.get("deliverable", "").strip()
        ):
            raise ProjectionError("external-artifact-row-invalid")
        status = node.get("status")
        if not isinstance(status, str):
            raise ProjectionError("external-artifact-status-invalid")
        if status not in {"pending_external", "accepted", "rejected", "unresolved"}:
            raise ProjectionError("external-artifact-status-invalid")
        action_status = node.get("action_status", "not_declared_in_source")
        action = node.get("action")
        if (
            not isinstance(action_status, str)
            or action_status not in {"declared_in_source", "not_declared_in_source"}
            or (action_status == "not_declared_in_source" and action is not None)
            or (action_status == "declared_in_source" and (not isinstance(action, str) or not action.strip()))
        ):
            raise ProjectionError("external-artifact-action-invalid")
        node_by_id[ident] = {
            **node, "action": action,
            "action_status": action_status,
            "source_pointer": f"/nodes/{index}", "independent_artifact": True,
        }
    for row in stage_relations:
        ident = row["external_artifact_id"]
        if ident is not None and ident not in node_by_id:
            raise ProjectionError("stage-relation-external-artifact-missing")

    graph = source.get("dag")
    if not isinstance(graph, dict) or not isinstance(graph.get("edges"), dict):
        raise ProjectionError("execution-dag-missing")
    gate_keys = [item["key"] for item in entries if item.get("key") == "ACV1-RELEASE-GATE"]
    edges = graph["edges"]
    if any(
        not isinstance(prerequisites, list)
        or any(not isinstance(prerequisite, str) for prerequisite in prerequisites)
        for prerequisites in edges.values()
    ):
        raise ProjectionError("execution-dag-invalid")
    projected = {
        "format": "agent-harness.coverage-projection/v2",
        "complete": source.get("complete") is True,
        "release": source.get("release"),
        "coordination_status": source.get("coordination_status", "unreported"),
        "publication_status": source.get("publication_status", "unreported"),
        "entries": entries,
        "ownership_rows": ownership_rows,
        "source_contribution_rows": actual_rows,
        "pair_review_rows": pair_review_rows,
        "source_supported_associations": association_additions,
        "reviewed_typed_extras": [
            dict(extras[pair], review_row_id=f"PAIR-{int(extras[pair]['source_pointer'].split('/')[-1]) + 1:04d}")
            for pair in sorted(extras)
        ],
        "unresolved_associations": unresolved,
        "execution_graph": {
            "edges": edges,
            "gate_outcomes": [{"key": key, "outcome_required": True,
                               "fan_in": list(edges.get(key, [])), "node_role": "release_gate_fan_in"} for key in gate_keys],
            "source_claims": {key: graph.get(key) for key in ("acyclic", "waves", "critical_path", "wave_count", "status", "all_59_required_reachable")},
        },
        "stage_relations": stage_relations,
        "source_stage_rows": relation_rows,
        "external_artifact_nodes": list(node_by_id.values()),
        "source_dimensions": {"required_issue_outcomes": len(entries), "source_contribution_rows": len(actual_rows),
                              "ownership_records": len(ownership_rows), "canonical_criteria": len(criteria),
                              "canonical_criteria_outside_scope": len(criterion_ids - ownership_ids),
                              "staged_relations": len(stage_relations), "external_artifact_nodes": len(node_by_id)},
        "projection_inputs": [],
    }
    input_rows = [
        ("coverage_map", args.coverage_map, source_raw), ("pair_review", args.pair_review, pair_raw),
        ("stage_review", args.stage_review, stage_raw), ("external_artifacts", args.external_artifacts, external_raw),
        ("issue_readbacks", args.issue_readbacks, readbacks_raw), ("canonical_checklist", args.canonical_checklist, canonical_raw),
        ("canonical_reference", args.canonical_reference, canonical_ref_raw),
    ]
    projected["projection_inputs"] = [{"role": role, "source_path": path, "bytes": len(raw), "sha256": _digest(raw)} for role, path, raw in input_rows]
    canonical_pin = {
        "role": "global-canonical-checklist", "adapter": "integration-evals.acceptance-checklist/v1",
        "repository": args.canonical_repository, "issue_pinned_revision": args.canonical_issue_revision,
        "captured_main_revision": args.canonical_main_revision, "source_path": args.canonical_source_path,
        "git_blob_sha1": _git_blob(canonical_raw), "bytes": len(canonical_raw),
    }
    map_raw = json.dumps(projected, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(map_raw) > MAX_OUTPUT:
        raise ProjectionError("projection-output-too-large")
    pins = {
        "format": "acv1.coverage-source-reference/v2",
        "snapshot_id": readbacks.get("capture_id"),
        "coverage_map": {"adapter": "agent-harness.coverage-projection/v2", "source_path": args.output_map,
                         "bytes": len(map_raw), "sha256": _digest(map_raw), "complete": projected["complete"]},
        "upstream_git_objects": [canonical_pin],
        "issue_readbacks": {"adapter": "agent-harness.issue-readbacks/v2", "capture_id": readbacks.get("capture_id"),
                            "captured_at": readbacks.get("captured_at"), "observed_revision": readbacks.get("observed_revision"),
                            "pages_complete": readbacks.get("pages_complete") is True,
                            "bytes": len(readbacks_raw), "sha256": _digest(readbacks_raw)},
        "projection_inputs": projected["projection_inputs"],
    }
    pins_raw = json.dumps(pins, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(pins_raw) > 2 * 1024 * 1024:
        raise ProjectionError("source-pins-output-too-large")
    return projected, pins


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coverage-map", required=True)
    parser.add_argument("--pair-review", required=True)
    parser.add_argument("--stage-review", required=True)
    parser.add_argument("--external-artifacts", required=True)
    parser.add_argument("--issue-readbacks", required=True)
    parser.add_argument("--canonical-checklist", required=True)
    parser.add_argument("--canonical-reference", required=True)
    parser.add_argument("--canonical-repository", required=True)
    parser.add_argument("--canonical-source-path", required=True)
    parser.add_argument("--canonical-issue-revision", required=True)
    parser.add_argument("--canonical-main-revision", required=True)
    parser.add_argument("--output-map", required=True)
    parser.add_argument("--output-pins", required=True)
    args = parser.parse_args(argv)
    try:
        projection, pins = project(args)
        map_raw = json.dumps(projection, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
        pins_raw = json.dumps(pins, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
        _write(args.output_map, map_raw)
        try:
            _write(args.output_pins, pins_raw)
        except ProjectionError:
            try:
                Path(args.output_map).unlink()
            except OSError:
                pass
            raise
    except ProjectionError as exc:
        sys.stderr.write(json.dumps({"code": str(exc)[:96]}, sort_keys=True, separators=(",", ":")) + "\n")
        return 2
    except (TypeError, ValueError, OverflowError, UnicodeError, RecursionError):
        sys.stderr.write('{"code":"projection-input-invalid"}\n')
        return 2
    except Exception:
        sys.stderr.write('{"code":"projection-failed"}\n')
        return 2
    sys.stdout.write(json.dumps({"status": "projected", "map_bytes": len(map_raw), "pins_bytes": len(pins_raw)}, sort_keys=True, separators=(",", ":")) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
