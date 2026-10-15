"""Offline synthetic coverage-validator controls; no external data is copied."""

from __future__ import annotations

import copy
import contextlib
import hashlib
import importlib.util
import io
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "planning" / "fixtures" / "coverage"
SPEC = importlib.util.spec_from_file_location(
    "coverage_validator", ROOT / "scripts" / "validate-agent-creative-coverage.py"
)
assert SPEC is not None and SPEC.loader is not None
validator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(validator)


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _git_blob_sha1(raw: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(raw)).encode("ascii") + b"\0" + raw).hexdigest()


class CoverageValidatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.work = Path(self.temp.name)
        self.map = json.loads((FIXTURES / "map.json").read_text(encoding="utf-8"))
        self.canonical = json.loads((FIXTURES / "canonical.json").read_text(encoding="utf-8"))
        self.readbacks = json.loads((FIXTURES / "readbacks.json").read_text(encoding="utf-8"))
        self.evidence_dir = self.work / "evidence"
        self.evidence_dir.mkdir()
        for name in ("evidence-ah.txt", "evidence-ei.txt", "evidence-ce.txt", "evidence-receipt-ce.txt"):
            source = FIXTURES / name
            if source.exists():
                shutil.copyfile(source, self.evidence_dir / name)
        self.map_path = self.work / "coverage-map.json"
        self.canonical_path = self.work / "canonical.json"
        self.readbacks_path = self.work / "readbacks.json"
        self.pins_path = self.work / "source-pins.json"
        self.release_path = self.work / "release.json"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def dimensions(self, mapping: dict[str, Any], canonical: dict[str, Any]) -> dict[str, int]:
        criteria = {item["id"] for item in canonical["criteria"]}
        ownership = mapping["ownership_rows"]
        ownership_ids = {item["requirement_id"] for item in ownership}
        rows = mapping["source_contribution_rows"]
        global_ids = ownership_ids & criteria
        repo_counts: dict[str, int] = {}
        for entry in mapping["entries"]:
            if entry.get("required_for_v1") is True:
                repo = entry["repository"]
                repo_counts[repo] = repo_counts.get(repo, 0) + 1
        rows_global = sum(
            1
            for row in rows
            if isinstance(row.get("requirement_id"), str) and row.get("requirement_id") in criteria
        )
        return {
            "required_leaf_records": sum(1 for row in mapping["entries"] if row.get("required_for_v1") is True),
            "agent_harness_leaf_records": repo_counts.get("agent-harness", 0),
            "specialist_leaf_records": sum(value for key, value in repo_counts.items() if key != "agent-harness"),
            "source_contribution_rows": len(rows),
            "unique_mapped_requirement_ids": len(ownership_ids),
            "local_scoped_requirement_ids": len(ownership_ids - criteria),
            "global_ids_with_scoped_contributions": len(global_ids),
            "global_criteria_total": len(criteria),
            "global_ids_outside_scoped_contribution": len(criteria - global_ids),
            "contribution_rows_to_global_ids": rows_global,
            "contribution_rows_to_local_ids": len(rows) - rows_global,
        }

    def write_inputs(
        self,
        mapping: dict[str, Any] | None = None,
        canonical: dict[str, Any] | None = None,
        readbacks: dict[str, Any] | None = None,
        *,
        refresh_pins: bool = True,
    ) -> dict[str, Any]:
        mapping = self.map if mapping is None else mapping
        canonical = self.canonical if canonical is None else canonical
        readbacks = self.readbacks if readbacks is None else readbacks
        map_raw = _canonical_bytes(mapping)
        canonical_raw = _canonical_bytes(canonical)
        readbacks_raw = _canonical_bytes(readbacks)
        self.map_path.write_bytes(map_raw)
        self.canonical_path.write_bytes(canonical_raw)
        self.readbacks_path.write_bytes(readbacks_raw)
        pins = {
            "format": "acv1.coverage-source-reference/v1",
            "snapshot_id": "synthetic-snapshot-01",
            "coverage_map": {
                "adapter": "agent-harness.coverage-map-current/v1",
                "source_path": "synthetic/coverage-map.json",
                "bytes": len(map_raw),
                "sha256": hashlib.sha256(map_raw).hexdigest(),
                "complete": mapping.get("complete") is True,
                "observed_dimensions": self.dimensions(mapping, canonical),
            },
            "upstream_git_objects": [
                {
                    "role": "global-canonical-checklist",
                    "adapter": "integration-evals.acceptance-checklist/v1",
                    "repository": "Example-Co/contracts",
                    "issue_pinned_revision": "1" * 40,
                    "captured_main_revision": "2" * 40,
                    "source_path": "planning/synthetic-checklist.json",
                    "git_blob_sha1": _git_blob_sha1(canonical_raw),
                    "bytes": len(canonical_raw),
                }
            ],
            "issue_readbacks": {
                "adapter": "agent-harness.issue-readbacks/v1",
                "capture_id": readbacks.get("capture_id"),
                "captured_at": readbacks.get("captured_at"),
                "observed_revision": readbacks.get("observed_revision"),
                "pages_complete": readbacks.get("pages_complete") is True,
                "bytes": len(readbacks_raw),
                "sha256": hashlib.sha256(readbacks_raw).hexdigest(),
            },
        }
        if not refresh_pins:
            pins["coverage_map"]["sha256"] = "0" * 64
        self.pins_path.write_bytes(_canonical_bytes(pins))
        return pins

    def validate(self, **kwargs: Any) -> dict[str, Any]:
        return validator.validate(
            self.map_path,
            self.pins_path,
            self.canonical_path,
            self.readbacks_path,
            **kwargs,
        )

    def release_manifest(self) -> dict[str, Any]:
        tests = {
            "AH-LEAF": ("test-ah", "evidence-ah.txt", "a" * 40),
            "EI-LEAF": ("test-ei", "evidence-ei.txt", "b" * 40),
            "CE-LEAF": ("test-ce", "evidence-ce.txt", "c" * 40),
        }
        revisions: list[dict[str, str]] = []
        evidence: list[dict[str, Any]] = []
        outcomes: list[dict[str, Any]] = []
        receipts: list[dict[str, Any]] = []
        for leaf, (test_id, filename, revision) in tests.items():
            entry = next(item for item in self.map["entries"] if item["key"] == leaf)
            revisions.append({"repository": entry["repository"], "revision": revision})
            path = self.evidence_dir / filename
            evidence_id = {"AH-LEAF": "ev-ah", "EI-LEAF": "ev-ei", "CE-LEAF": "ev-ce"}[leaf]
            evidence.append({
                "id": evidence_id,
                "path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "producer": "synthetic-unit-control",
                "source_revision": revision,
                "captured_at": "2026-10-11T12:10:00Z",
                "mode": "mock",
                "kind": "test",
                "test_id": test_id,
                "result": "passed",
                "model_revision": "model-v1" if leaf == "CE-LEAF" else None,
                "policy_revision": "policy-v1" if leaf == "CE-LEAF" else None,
            })
            outcome = {
                "leaf_key": leaf,
                "status": "verified",
                "revision": revision,
                "evaluated_stage": entry["required_release_stage"],
                "prerequisites_satisfied": True,
                "required_test_ids": [test_id],
                "evidence_ids": [evidence_id],
                "required_mode": "mock",
                "required_owner_receipts": [],
                "owner_receipt_ids": [],
            }
            if leaf == "CE-LEAF":
                outcome["required_model_revision"] = "model-v1"
                outcome["required_policy_revision"] = "policy-v1"
                outcome["required_owner_receipts"] = ["receipt-ce"]
                outcome["owner_receipt_ids"] = ["receipt-ce"]
                receipt_path = self.evidence_dir / "evidence-receipt-ce.txt"
                evidence.append({
                    "id": "ev-receipt-ce",
                    "path": str(receipt_path),
                    "sha256": hashlib.sha256(receipt_path.read_bytes()).hexdigest(),
                    "producer": "synthetic-unit-control",
                    "source_revision": revision,
                    "captured_at": "2026-10-11T12:10:00Z",
                    "mode": "mock",
                    "kind": "receipt",
                })
                outcome["evidence_ids"].append("ev-receipt-ce")
                receipts.append({
                    "id": "receipt-ce",
                    "evidence_id": "ev-receipt-ce",
                    "owner_role": "Creative and fulfillment",
                    "action": "accept synthetic fixture only",
                    "status": "accepted",
                    "source_revision": revision,
                })
            outcomes.append(outcome)
        return {
            "format": "acv1.release-manifest/v1",
            "tested_revisions": revisions,
            "evidence": evidence,
            "outcomes": outcomes,
            "owner_receipts": receipts,
        }

    def validate_release(self, **kwargs: Any) -> dict[str, Any]:
        release = self.release_manifest()
        self.release_path.write_bytes(_canonical_bytes(release))
        return self.validate(release_path=self.release_path, **kwargs)

    def validate_release_with_fake_io(
        self,
        *,
        evidence_total_limit: int,
        evidence_file_limit: int = 128,
        read_actions: dict[str, list[Any]] | None = None,
        close_failures: set[str] | None = None,
        fstat_sizes: dict[str, int] | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, int]]:
        self.write_inputs()
        self.release_path.write_bytes(_canonical_bytes(self.release_manifest()))
        read_actions = {} if read_actions is None else read_actions
        close_failures = set() if close_failures is None else close_failures
        fstat_sizes = {} if fstat_sizes is None else fstat_sizes
        real_open = os.open
        real_read = os.read
        real_fstat = os.fstat
        real_close = os.close
        evidence_paths: dict[int, str] = {}
        read_indexes: dict[str, int] = {}
        read_events: list[dict[str, Any]] = []
        close_counts: dict[str, int] = {}
        failed_closes: set[str] = set()

        def tracked_open(path: Any, flags: int, *args: Any, **kwargs: Any) -> int:
            fd = real_open(path, flags, *args, **kwargs)
            candidate = Path(os.fsdecode(path))
            if candidate.parent == self.evidence_dir:
                evidence_paths[fd] = candidate.name
            return fd

        def tracked_fstat(fd: int) -> Any:
            info = real_fstat(fd)
            name = evidence_paths.get(fd)
            if name in fstat_sizes:
                return SimpleNamespace(st_mode=info.st_mode, st_size=fstat_sizes[name])
            return info

        def tracked_read(fd: int, requested: int) -> bytes:
            name = evidence_paths.get(fd)
            if name is None:
                return real_read(fd, requested)
            index = read_indexes.get(name, 0)
            read_indexes[name] = index + 1
            actions = read_actions.get(name, [])
            action = actions[index] if index < len(actions) else None
            if isinstance(action, BaseException):
                read_events.append({"file": name, "requested": requested, "status": "error", "returned": 0})
                raise action
            if isinstance(action, int):
                chunk = real_read(fd, min(requested, action))
            elif isinstance(action, bytes):
                if len(action) > requested:
                    raise AssertionError("fake read returned more bytes than requested")
                chunk = action
            else:
                chunk = real_read(fd, requested)
            read_events.append({"file": name, "requested": requested, "status": "returned", "returned": len(chunk)})
            return chunk

        def tracked_close(fd: int) -> None:
            name = evidence_paths.get(fd)
            if name is None:
                real_close(fd)
                return
            close_counts[name] = close_counts.get(name, 0) + 1
            real_close(fd)
            evidence_paths.pop(fd, None)
            if name in close_failures and name not in failed_closes:
                failed_closes.add(name)
                raise OSError("synthetic evidence close failure")

        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.dict(
                validator.LIMITS,
                {"evidence-total": evidence_total_limit, "evidence-file": evidence_file_limit},
            ))
            stack.enter_context(mock.patch.object(validator.os, "open", side_effect=tracked_open))
            stack.enter_context(mock.patch.object(validator.os, "fstat", side_effect=tracked_fstat))
            stack.enter_context(mock.patch.object(validator.os, "read", side_effect=tracked_read))
            stack.enter_context(mock.patch.object(validator.os, "close", side_effect=tracked_close))
            report = self.validate(release_path=self.release_path)
        return report, read_events, close_counts

    def codes(self, report: dict[str, Any]) -> set[str]:
        return {issue["code"] for issue in report["issues"]}

    def test_synthetic_baseline_is_planning_only_and_keeps_fanin_separate(self) -> None:
        self.write_inputs()
        report = self.validate()
        self.assertEqual(report["status"], "BACKLOG_READY")
        self.assertTrue(report["planning_only"])
        self.assertFalse(report["whole_program_acceptance"])
        self.assertEqual(report["global_acceptance"], "not_asserted")
        self.assertEqual(report["counts"]["source_contribution_rows"], 3)
        self.assertEqual(report["counts"]["unique_mapped_requirement_ids"], 2)
        self.assertEqual(report["counts"]["global_ids_outside_scoped_contribution"], 0)

    def test_later_pinned_snapshot_with_changed_counts_is_accepted(self) -> None:
        mapping = copy.deepcopy(self.map)
        ah = next(item for item in mapping["entries"] if item["key"] == "AH-LEAF")
        ah["requirement_ids"].append("LOCAL-B")
        mapping["ownership_rows"].append({
            "requirement_id": "LOCAL-B",
            "scope": "local",
            "accountable_delivery_leaf": "AH-LEAF",
            "acceptance_owner_leaf": "AH-LEAF",
            "required_contributors": ["AH-LEAF"],
            "required_for_v1": True,
            "state": "planned",
        })
        mapping["source_contribution_rows"].append({
            "row_id": "ROW-4", "requirement_id": "LOCAL-B", "owning_leaf_key": "AH-LEAF", "scope": "local"
        })
        self.write_inputs(mapping)
        report = self.validate()
        self.assertEqual(report["status"], "BACKLOG_READY")
        self.assertEqual(report["counts"]["source_contribution_rows"], 4)
        self.assertEqual(report["counts"]["unique_mapped_requirement_ids"], 3)

    def test_same_count_wrong_contribution_row_fails(self) -> None:
        mapping = copy.deepcopy(self.map)
        mapping["source_contribution_rows"][1]["requirement_id"] = "MISSING-ID"
        self.write_inputs(mapping)
        self.assertIn("ORPHAN_CONTRIBUTION_REQUIREMENT", self.codes(self.validate()))

    def test_same_count_contributor_reassignment_preserves_required_pair_check(self) -> None:
        mapping = copy.deepcopy(self.map)
        mapping["source_contribution_rows"][2]["owning_leaf_key"] = "EI-LEAF"
        self.write_inputs(mapping)
        report = self.validate()
        self.assertEqual(report["counts"]["source_contribution_rows"], 3)
        self.assertEqual(report["counts"]["unique_mapped_requirement_ids"], 2)
        missing = [item for item in report["issues"] if item["code"] == "REQUIRED_CONTRIBUTOR_MISSING"]
        self.assertEqual(missing, [{
            "code": "REQUIRED_CONTRIBUTOR_MISSING",
            "record": "GLOBAL-A",
            "related_record": "CE-LEAF",
        }])

    def test_distinct_repeated_contribution_rows_keep_multiplicity(self) -> None:
        mapping = copy.deepcopy(self.map)
        repeated = copy.deepcopy(mapping["source_contribution_rows"][2])
        repeated["row_id"] = "ROW-4"
        mapping["source_contribution_rows"].append(repeated)
        self.write_inputs(mapping)
        report = self.validate()
        self.assertEqual(report["status"], "BACKLOG_READY")
        self.assertEqual(report["counts"]["source_contribution_rows"], 4)

    def test_duplicate_owner_or_orphan_ownership_fails(self) -> None:
        mapping = copy.deepcopy(self.map)
        mapping["ownership_rows"].append(copy.deepcopy(mapping["ownership_rows"][0]))
        self.write_inputs(mapping)
        self.assertIn("OWNERSHIP_ID_DUPLICATE", self.codes(self.validate()))

        mapping = copy.deepcopy(self.map)
        mapping["source_contribution_rows"].append({
            "row_id": "ROW-ORPHAN", "requirement_id": "ORPHAN", "owning_leaf_key": "AH-LEAF"
        })
        self.write_inputs(mapping)
        self.assertIn("ORPHAN_CONTRIBUTION_REQUIREMENT", self.codes(self.validate()))

    def test_required_specialist_family_and_null_url_block_without_guessing(self) -> None:
        mapping = copy.deepcopy(self.map)
        mapping["entries"] = [row for row in mapping["entries"] if row["key"] != "CE-LEAF"]
        mapping["ownership_rows"][1]["required_contributors"].remove("CE-LEAF")
        mapping["source_contribution_rows"] = [row for row in mapping["source_contribution_rows"] if row["row_id"] != "ROW-3"]
        mapping["dag"]["edges"].pop("CE-LEAF")
        mapping["dag"]["gate_fan_in"]["RELEASE-GATE"].remove("CE-LEAF")
        mapping["dag"]["required_gate_fan_in"]["RELEASE-GATE"].remove("CE-LEAF")
        mapping["staged_edges"] = []
        self.write_inputs(mapping)
        self.assertIn("REQUIRED_LEAF_FAMILY_MISSING", self.codes(self.validate()))

        mapping = copy.deepcopy(self.map)
        ce = next(row for row in mapping["entries"] if row["key"] == "CE-LEAF")
        ce["issue_url"] = None
        ce["blockers"] = [{"owner": "Creative and fulfillment", "action": "obtain issue URL"}]
        self.write_inputs(mapping)
        report = self.validate()
        self.assertIn("REQUIRED_ISSUE_URL_UNPROVEN", self.codes(report))
        self.assertNotIn("github.com", json.dumps(report))

    def test_closed_requires_exact_evidence_deferred_never_counts(self) -> None:
        mapping = copy.deepcopy(self.map)
        ah = next(row for row in mapping["entries"] if row["key"] == "AH-LEAF")
        ah["state"] = "closed"
        self.write_inputs(mapping)
        self.assertIn("CLOSED_LEAF_WITHOUT_EXACT_EVIDENCE", self.codes(self.validate()))

        ah["closure_evidence"] = [{"commit": "a" * 40, "tests": ["test-ah"]}]
        readbacks = copy.deepcopy(self.readbacks)
        readbacks["issues"][0]["state"] = "closed"
        self.write_inputs(mapping, readbacks=readbacks)
        self.assertEqual(self.validate()["status"], "BACKLOG_READY")

        mapping = copy.deepcopy(self.map)
        next(row for row in mapping["entries"] if row["key"] == "AH-LEAF")["state"] = "deferred"
        self.write_inputs(mapping)
        self.assertIn("REQUIRED_LEAF_NOT_DELIVERED", self.codes(self.validate()))

    def test_closed_readback_cannot_hide_an_active_map_leaf(self) -> None:
        mapping = copy.deepcopy(self.map)
        readbacks = copy.deepcopy(self.readbacks)
        readbacks["issues"][0]["state"] = "closed"
        self.write_inputs(mapping, readbacks=readbacks)
        report = self.validate()
        self.assertIn("ISSUE_READBACK_STATE_MISMATCH", self.codes(report))
        self.assertIn("CLOSED_LEAF_WITHOUT_EXACT_EVIDENCE", self.codes(report))

        mapping = copy.deepcopy(self.map)
        ah = next(row for row in mapping["entries"] if row["key"] == "AH-LEAF")
        ah["state"] = "closed"
        self.write_inputs(mapping)
        self.assertIn("ISSUE_READBACK_STATE_MISMATCH", self.codes(self.validate()))

    def test_supersession_requires_both_exact_link_directions(self) -> None:
        mapping = copy.deepcopy(self.map)
        replacement = copy.deepcopy(mapping["entries"][0])
        replacement.update({"key": "AH-REPLACEMENT", "issue_url": "https://github.com/Example-Co/agent-harness/issues/404", "issue_title": "Replacement", "body_sha256": "d" * 64, "supersedes": "AH-LEAF"})
        mapping["entries"].append(replacement)
        mapping["entries"][0]["required_for_v1"] = False
        mapping["entries"][0]["record_type"] = "implementation_leaf"
        readbacks = copy.deepcopy(self.readbacks)
        readbacks["issues"].append({"repository": "agent-harness", "number": 404, "url": replacement["issue_url"], "title": "Replacement", "state": "open", "body_sha256": "d" * 64})
        self.write_inputs(mapping, readbacks=readbacks)
        self.assertIn("SUPERSESSION_RELATION_UNVERIFIED", self.codes(self.validate()))

    def test_global_metadata_and_scope_cannot_be_rewritten_by_local_projection(self) -> None:
        mapping = copy.deepcopy(self.map)
        mapping["source_contribution_rows"][1]["authoritative_acceptance"]["priority"] = "P0"
        self.write_inputs(mapping)
        self.assertIn("GLOBAL_ACCEPTANCE_METADATA_MISMATCH", self.codes(self.validate()))

        mapping = copy.deepcopy(self.map)
        global_row = next(row for row in mapping["ownership_rows"] if row["requirement_id"] == "GLOBAL-A")
        global_row["global_status"] = "complete"
        self.write_inputs(mapping)
        self.assertIn("GLOBAL_SCOPE_OVERCLAIM", self.codes(self.validate()))

    def test_issue_readback_mismatch_and_incomplete_pages_fail(self) -> None:
        readbacks = copy.deepcopy(self.readbacks)
        readbacks["issues"][1]["title"] = "Wrong title"
        self.write_inputs(readbacks=readbacks)
        self.assertIn("ISSUE_READBACK_TITLE_MISMATCH", self.codes(self.validate()))

        readbacks = copy.deepcopy(self.readbacks)
        readbacks["pages_complete"] = False
        self.write_inputs(readbacks=readbacks)
        self.assertIn("ISSUE_READBACK_PAGES_INCOMPLETE", self.codes(self.validate()))

    def test_json_derived_unhashable_contribution_ids_are_total(self) -> None:
        for value in ([], {}, None, 7, True):
            with self.subTest(value_type=type(value).__name__):
                mapping = copy.deepcopy(self.map)
                mapping["source_contribution_rows"][0]["requirement_id"] = value
                self.write_inputs(mapping)
                report = self.validate()
                self.assertEqual(report["status"], "BLOCKED")
                self.assertIn("ORPHAN_CONTRIBUTION_REQUIREMENT", self.codes(report))
                self.assertEqual(report["schema"], "acv1.coverage-validation/v1")

    def test_oversized_issue_number_is_bounded_and_cli_keeps_json_diagnostic(self) -> None:
        mapping = copy.deepcopy(self.map)
        ah = next(row for row in mapping["entries"] if row["key"] == "AH-LEAF")
        ah["issue_url"] = "https://github.com/Example-Co/agent-harness/issues/" + ("9" * 5000)
        self.write_inputs(mapping)

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = validator.main([
                str(self.map_path),
                "--source-pins", str(self.pins_path),
                "--canonical-checklist", str(self.canonical_path),
                "--issue-readbacks", str(self.readbacks_path),
            ])
        rendered = stdout.getvalue()
        report = json.loads(rendered)
        self.assertEqual(exit_code, 1)
        self.assertEqual(report["status"], "BLOCKED")
        self.assertIn("ENTRY_ISSUE_URL_INVALID", self.codes(report))
        self.assertNotIn("Traceback", rendered)
        self.assertNotIn("ValueError", rendered)

    def test_duplicate_json_keys_are_rejected_and_pin_mismatch_fails(self) -> None:
        self.write_inputs()
        self.pins_path.write_text('{"format":"acv1.coverage-source-reference/v1","format":"bad"}', encoding="utf-8")
        report = self.validate()
        self.assertIn("DUPLICATE_JSON_KEY", self.codes(report))

        self.write_inputs(refresh_pins=False)
        self.assertIn("COVERAGE_MAP_PIN_MISMATCH", self.codes(self.validate()))

    def test_missing_classifier_family_and_invalid_metadata_fail_closed(self) -> None:
        mapping = copy.deepcopy(self.map)
        del next(row for row in mapping["entries"] if row["key"] == "AH-LEAF")["record_type"]
        self.write_inputs(mapping)
        self.assertIn("ENTRY_RECORD_TYPE_REQUIRED", self.codes(self.validate()))

        mapping = copy.deepcopy(self.map)
        mapping["coordination_status"] = ["pending"]
        self.write_inputs(mapping)
        self.assertIn("COORDINATION_STATUS_INVALID", self.codes(self.validate()))

        self.write_inputs()
        skipped = self.validate(check_dag=False)
        self.assertIn("REQUIRED_CHECK_NOT_RUN", self.codes(skipped))
        self.assertEqual(skipped["checks"]["typed_graph"], False)

    def test_readback_capture_pin_drift_is_explicit(self) -> None:
        self.write_inputs()
        pins = json.loads(self.pins_path.read_text(encoding="utf-8"))
        pins["issue_readbacks"]["capture_id"] = "stale-capture-02"
        self.pins_path.write_bytes(_canonical_bytes(pins))
        self.assertIn("ISSUE_READBACK_CAPTURE_MISMATCH", self.codes(self.validate()))

    def test_execution_graph_cycles_self_edges_unknown_and_epic_edges_fail(self) -> None:
        for mutation, code in (
            (lambda dag: dag["edges"].update({"AH-LEAF": ["EI-LEAF"], "EI-LEAF": ["AH-LEAF"]}), "EXECUTION_DAG_CYCLE"),
            (lambda dag: dag["edges"].update({"AH-LEAF": ["AH-LEAF"]}), "EXECUTION_EDGE_SELF"),
            (lambda dag: dag["edges"].update({"CE-LEAF": ["MISSING"]}), "EXECUTION_EDGE_UNKNOWN_NODE"),
            (lambda dag: dag["edges"].update({"EI-LEAF": ["RELEASE-GATE"]}), "EXECUTION_EDGE_PREREQUISITE_NOT_LEAF"),
        ):
            mapping = copy.deepcopy(self.map)
            mutation(mapping["dag"])
            self.write_inputs(mapping)
            self.assertIn(code, self.codes(self.validate()))

    def test_gate_fanin_is_checked_as_its_own_typed_relation(self) -> None:
        self.write_inputs()
        self.assertEqual(self.validate()["status"], "BACKLOG_READY")
        mapping = copy.deepcopy(self.map)
        mapping["dag"]["gate_fan_in"]["RELEASE-GATE"].remove("CE-LEAF")
        self.write_inputs(mapping)
        self.assertIn("GATE_FAN_IN_MISMATCH", self.codes(self.validate()))

    def test_readback_identity_rejects_wrong_repository_or_body(self) -> None:
        mapping = copy.deepcopy(self.map)
        mapping["entries"][0]["issue_url"] = "https://github.com/Example-Co/other-repo/issues/101"
        self.write_inputs(mapping)
        self.assertIn("ENTRY_REPOSITORY_URL_MISMATCH", self.codes(self.validate()))

        readbacks = copy.deepcopy(self.readbacks)
        readbacks["issues"][0]["body_sha256"] = "f" * 64
        self.write_inputs(readbacks=readbacks)
        self.assertIn("ISSUE_READBACK_BODY_MISMATCH", self.codes(self.validate()))

    def test_positive_release_branch_is_scoped_and_never_acceptance_claim(self) -> None:
        self.write_inputs()
        release = self.release_manifest()
        expected_evidence_bytes = sum(Path(item["path"]).stat().st_size for item in release["evidence"])
        self.release_path.write_bytes(_canonical_bytes(release))
        report = self.validate(release_path=self.release_path)
        self.assertEqual(report["status"], "SCOPED_RELEASE_EVIDENCE_READY", report)
        self.assertEqual(report["coordination_status"], "pending")
        self.assertEqual(report["release_status"], "SCOPED_RELEASE_EVIDENCE_READY")
        self.assertFalse(report["whole_program_acceptance"])
        self.assertEqual(report["global_acceptance"], "not_asserted")
        self.assertEqual(report["release_counts"]["outcomes"], 3)
        self.assertEqual(report["release_counts"]["evidence_bytes_read"], expected_evidence_bytes)
        self.assertEqual(report["release_counts"]["verified_evidence_bytes"], expected_evidence_bytes)

    def test_required_stage_edge_requirements_are_shared_by_both_endpoints(self) -> None:
        def make_edge() -> dict[str, Any]:
            return {
                "edge_type": "evidence_stage",
                "edge_id": "EDGE-MULTI-REQUIREMENT",
                "consumer_key": "CE-LEAF",
                "prerequisite_key": "EI-LEAF",
                "consumer_stage": "runnable",
                "prerequisite_stage": "contract",
                "status": "pending",
                "required_for_v1": True,
                "requirement_ids": ["GLOBAL-A", "LOCAL-A"],
                "required_evidence_ids": [],
                "required_owner_receipt_ids": [],
            }

        def required_requirement_issues(report: dict[str, Any]) -> set[tuple[str, str, str]]:
            return {
                (issue["code"], issue["record"], issue["related_record"])
                for issue in report["issues"]
                if issue["code"] in {
                    "STAGED_EDGE_CONSUMER_REQUIREMENT_NOT_DECLARED",
                    "STAGED_EDGE_PREREQUISITE_REQUIREMENT_NOT_DECLARED",
                }
            }

        def add_local_contribution(mapping: dict[str, Any], leaf_key: str) -> None:
            leaf = next(row for row in mapping["entries"] if row["key"] == leaf_key)
            if "LOCAL-A" not in leaf["requirement_ids"]:
                leaf["requirement_ids"].append("LOCAL-A")
            ownership = next(row for row in mapping["ownership_rows"] if row["requirement_id"] == "LOCAL-A")
            if leaf_key not in ownership["required_contributors"]:
                ownership["required_contributors"].append(leaf_key)
            mapping["source_contribution_rows"].append({
                "row_id": f"ROW-LOCAL-{leaf_key}",
                "requirement_id": "LOCAL-A",
                "owning_leaf_key": leaf_key,
                "scope": "local",
            })

        mapping = copy.deepcopy(self.map)
        mapping["staged_edges"].append(make_edge())
        self.write_inputs(mapping)
        neither_report = self.validate()
        self.assertEqual(required_requirement_issues(neither_report), {
            ("STAGED_EDGE_CONSUMER_REQUIREMENT_NOT_DECLARED", "EDGE-MULTI-REQUIREMENT", "LOCAL-A"),
            ("STAGED_EDGE_PREREQUISITE_REQUIREMENT_NOT_DECLARED", "EDGE-MULTI-REQUIREMENT", "LOCAL-A"),
        })

        mapping = copy.deepcopy(self.map)
        add_local_contribution(mapping, "CE-LEAF")
        mapping["staged_edges"].append(make_edge())
        self.write_inputs(mapping)
        one_endpoint_report = self.validate()
        self.assertEqual(required_requirement_issues(one_endpoint_report), {
            ("STAGED_EDGE_PREREQUISITE_REQUIREMENT_NOT_DECLARED", "EDGE-MULTI-REQUIREMENT", "LOCAL-A"),
        })

        mapping = copy.deepcopy(self.map)
        ce = next(row for row in mapping["entries"] if row["key"] == "CE-LEAF")
        ce["requirement_ids"] = ["LOCAL-A"]
        global_ownership = next(row for row in mapping["ownership_rows"] if row["requirement_id"] == "GLOBAL-A")
        global_ownership["required_contributors"].remove("CE-LEAF")
        mapping["source_contribution_rows"] = [
            row for row in mapping["source_contribution_rows"]
            if row["row_id"] != "ROW-3"
        ]
        add_local_contribution(mapping, "CE-LEAF")
        mapping["staged_edges"].append(make_edge())
        self.write_inputs(mapping)
        disjoint_report = self.validate()
        self.assertEqual(required_requirement_issues(disjoint_report), {
            ("STAGED_EDGE_CONSUMER_REQUIREMENT_NOT_DECLARED", "EDGE-MULTI-REQUIREMENT", "GLOBAL-A"),
            ("STAGED_EDGE_PREREQUISITE_REQUIREMENT_NOT_DECLARED", "EDGE-MULTI-REQUIREMENT", "LOCAL-A"),
        })

        mapping = copy.deepcopy(self.map)
        add_local_contribution(mapping, "CE-LEAF")
        add_local_contribution(mapping, "EI-LEAF")
        mapping["staged_edges"].append(make_edge())
        self.write_inputs(mapping)
        shared_report = self.validate()
        self.assertEqual(shared_report["status"], "BACKLOG_READY", shared_report)
        self.assertEqual(required_requirement_issues(shared_report), set())

    def test_evidence_read_budget_counts_partial_reads_and_stops_before_later_files(self) -> None:
        report, read_events, close_counts = self.validate_release_with_fake_io(
            evidence_total_limit=40,
            read_actions={"evidence-ah.txt": [12, OSError("synthetic read failure")]},
        )
        self.assertEqual(report["status"], "BLOCKED")
        self.assertIn("EVIDENCE_READ_FAILED", self.codes(report))
        self.assertIn("EVIDENCE_TOTAL_SIZE", self.codes(report))
        self.assertEqual(report["release_counts"]["evidence_bytes_read"], 12)
        self.assertEqual(report["release_counts"]["verified_evidence_bytes"], 0)
        self.assertEqual(sum(event["returned"] for event in read_events), 12)
        self.assertEqual({event["file"] for event in read_events}, {"evidence-ah.txt"})
        self.assertEqual(set(close_counts.values()), {1})

    def test_evidence_read_budget_counts_bytes_when_close_fails(self) -> None:
        report, read_events, close_counts = self.validate_release_with_fake_io(
            evidence_total_limit=100,
            close_failures={"evidence-ah.txt"},
        )
        self.assertEqual(report["status"], "BLOCKED")
        self.assertIn("EVIDENCE_CLOSE_FAILED", self.codes(report))
        self.assertIn("EVIDENCE_TOTAL_SIZE", self.codes(report))
        self.assertEqual(report["release_counts"]["evidence_bytes_read"], 72)
        self.assertEqual(report["release_counts"]["verified_evidence_bytes"], 36)
        self.assertEqual(sum(event["returned"] for event in read_events), 72)
        self.assertEqual([event["file"] for event in read_events if event["returned"]], [
            "evidence-ah.txt", "evidence-ei.txt",
        ])
        self.assertEqual(set(close_counts.values()), {1})

    def test_evidence_read_budget_allows_only_one_growth_overflow_byte(self) -> None:
        report, read_events, close_counts = self.validate_release_with_fake_io(
            evidence_total_limit=40,
            fstat_sizes={"evidence-ah.txt": 0},
            read_actions={"evidence-ah.txt": [None, b"extra"]},
        )
        self.assertEqual(report["status"], "BLOCKED")
        self.assertIn("EVIDENCE_TOTAL_SIZE", self.codes(report))
        self.assertEqual(report["release_counts"]["evidence_bytes_read"], 41)
        self.assertEqual(report["release_counts"]["verified_evidence_bytes"], 0)
        self.assertEqual(sum(event["returned"] for event in read_events), 41)
        self.assertEqual({event["file"] for event in read_events}, {"evidence-ah.txt"})
        self.assertEqual(close_counts, {"evidence-ah.txt": 1})

    def test_applicable_stage_prerequisites_need_revision_bound_evidence(self) -> None:
        mapping = copy.deepcopy(self.map)
        mapping["staged_edges"].append({
            "edge_type": "evidence_stage",
            "edge_id": "EDGE-CE-RUNNABLE-EI-CONTRACT",
            "consumer_key": "CE-LEAF",
            "prerequisite_key": "EI-LEAF",
            "consumer_stage": "runnable",
            "prerequisite_stage": "contract",
            "status": "pending",
            "required_for_v1": True,
            "requirement_ids": ["GLOBAL-A"],
            "required_evidence_ids": [],
            "required_owner_receipt_ids": [],
        })
        self.write_inputs(mapping)
        release = self.release_manifest()
        self.release_path.write_bytes(_canonical_bytes(release))
        pending_report = self.validate(release_path=self.release_path)
        self.assertEqual(pending_report["status"], "BLOCKED")
        self.assertIn("STAGED_EDGE_PREREQUISITE_PENDING", self.codes(pending_report))

        mapping["staged_edges"][-1]["status"] = "satisfied"
        mapping["staged_edges"][-1]["required_evidence_ids"] = ["ev-ei-contract"]
        mapping["staged_edges"][-1]["required_owner_receipt_ids"] = ["receipt-ei-contract"]
        ei = next(row for row in mapping["entries"] if row["key"] == "EI-LEAF")
        ei["required_owner_receipts"] = ["receipt-ei-contract"]
        self.write_inputs(mapping)

        release = self.release_manifest()
        ei_outcome = next(row for row in release["outcomes"] if row["leaf_key"] == "EI-LEAF")
        ei_outcome["evidence_ids"].extend(["ev-ei-contract", "ev-ei-contract-receipt"])
        ei_outcome["required_owner_receipts"] = ["receipt-ei-contract"]
        ei_outcome["owner_receipt_ids"] = ["receipt-ei-contract"]
        contract_file = self.evidence_dir / "evidence-ei.txt"
        receipt_file = self.evidence_dir / "evidence-receipt-ce.txt"
        release["evidence"].extend([
            {
                "id": "ev-ei-contract",
                "path": str(contract_file),
                "sha256": hashlib.sha256(contract_file.read_bytes()).hexdigest(),
                "producer": "synthetic-unit-control",
                "source_revision": "b" * 40,
                "captured_at": "2026-10-11T12:10:00Z",
                "mode": "mock",
                "kind": "configuration",
                "stage_edge_id": "EDGE-CE-RUNNABLE-EI-CONTRACT",
                "leaf_key": "EI-LEAF",
                "stage": "contract",
                "requirement_ids": ["GLOBAL-A"],
            },
            {
                "id": "ev-ei-contract-receipt",
                "path": str(receipt_file),
                "sha256": hashlib.sha256(receipt_file.read_bytes()).hexdigest(),
                "producer": "synthetic-unit-control",
                "source_revision": "b" * 40,
                "captured_at": "2026-10-11T12:10:00Z",
                "mode": "mock",
                "kind": "receipt",
            },
        ])
        release["owner_receipts"].append({
            "id": "receipt-ei-contract",
            "evidence_id": "ev-ei-contract-receipt",
            "owner_role": "Synthetic prerequisite owner",
            "action": "accept the synthetic contract-stage fixture",
            "status": "accepted",
            "source_revision": "b" * 40,
        })
        self.release_path.write_bytes(_canonical_bytes(release))
        report = self.validate(release_path=self.release_path)
        self.assertEqual(report["status"], "SCOPED_RELEASE_EVIDENCE_READY", report)

    def test_outcome_cannot_lower_required_stage_and_final_gate_stays_outside_scope(self) -> None:
        self.write_inputs()
        release = self.release_manifest()
        ce = next(row for row in release["outcomes"] if row["leaf_key"] == "CE-LEAF")
        ce["evaluated_stage"] = "fixture"
        self.release_path.write_bytes(_canonical_bytes(release))
        report = self.validate(release_path=self.release_path)
        self.assertIn("OUTCOME_EVALUATED_STAGE_MISMATCH", self.codes(report))

        mapping = copy.deepcopy(self.map)
        ce_leaf = next(row for row in mapping["entries"] if row["key"] == "CE-LEAF")
        ce_leaf["required_release_stage"] = "fixture"
        mapping["staged_edges"].append({
            "edge_type": "evidence_stage",
            "edge_id": "EDGE-CE-FIXTURE-EI-RUNNABLE",
            "consumer_key": "CE-LEAF",
            "prerequisite_key": "EI-LEAF",
            "consumer_stage": "fixture",
            "prerequisite_stage": "runnable",
            "status": "satisfied",
            "required_for_v1": True,
            "requirement_ids": ["GLOBAL-A"],
            "required_evidence_ids": ["ev-ei"],
            "required_owner_receipt_ids": [],
        })
        self.write_inputs(mapping)
        release = self.release_manifest()
        ce = next(row for row in release["outcomes"] if row["leaf_key"] == "CE-LEAF")
        ce["evaluated_stage"] = "fixture"
        ei_evidence = next(row for row in release["evidence"] if row["id"] == "ev-ei")
        ei_evidence.update({
            "stage_edge_id": "EDGE-CE-FIXTURE-EI-RUNNABLE",
            "leaf_key": "EI-LEAF",
            "stage": "runnable",
            "requirement_ids": ["GLOBAL-A"],
        })
        self.release_path.write_bytes(_canonical_bytes(release))
        report = self.validate(release_path=self.release_path)
        self.assertEqual(report["status"], "SCOPED_RELEASE_EVIDENCE_READY", report)
        self.assertEqual(mapping["staged_edges"][0]["status"], "pending")

        mapping = copy.deepcopy(self.map)
        mapping["staged_edges"].append({
            "edge_type": "evidence_stage",
            "edge_id": "EDGE-OPTIONAL-CE-RUNNABLE-EI-CONTRACT",
            "consumer_key": "CE-LEAF",
            "prerequisite_key": "EI-LEAF",
            "consumer_stage": "runnable",
            "prerequisite_stage": "contract",
            "status": "pending",
            "required_for_v1": False,
            "requirement_ids": ["GLOBAL-A"],
            "required_evidence_ids": [],
            "required_owner_receipt_ids": [],
        })
        self.write_inputs(mapping)
        self.assertEqual(self.validate_release()["status"], "SCOPED_RELEASE_EVIDENCE_READY")

    def test_stage_status_enum_and_required_evidence_fail_closed(self) -> None:
        for status, required, expected in (
            ("owner-approved", ["ev-ei-contract"], "STAGED_EDGE_STATUS_INVALID"),
            ("satisfied", [], "STAGED_EDGE_EVIDENCE_REQUIRED"),
        ):
            with self.subTest(status=status, required=required):
                mapping = copy.deepcopy(self.map)
                mapping["staged_edges"].append({
                    "edge_type": "evidence_stage",
                    "edge_id": "EDGE-CE-RUNNABLE-EI-CONTRACT",
                    "consumer_key": "CE-LEAF",
                    "prerequisite_key": "EI-LEAF",
                    "consumer_stage": "runnable",
                    "prerequisite_stage": "contract",
                    "status": status,
                    "required_for_v1": True,
                    "requirement_ids": ["GLOBAL-A"],
                    "required_evidence_ids": required,
                    "required_owner_receipt_ids": [],
                })
                self.write_inputs(mapping)
                release = self.release_manifest()
                self.release_path.write_bytes(_canonical_bytes(release))
                report = self.validate(release_path=self.release_path)
                self.assertEqual(report["status"], "BLOCKED")
                self.assertIn(expected, self.codes(report))

        for field, wrong in (
            ("stage", "fixture"),
            ("source_revision", "c" * 40),
            ("mode", "recorded"),
            ("requirement_ids", ["LOCAL-A"]),
        ):
            with self.subTest(stage_evidence_field=field):
                mapping = copy.deepcopy(self.map)
                mapping["staged_edges"].append({
                    "edge_type": "evidence_stage",
                    "edge_id": "EDGE-CE-RUNNABLE-EI-CONTRACT",
                    "consumer_key": "CE-LEAF",
                    "prerequisite_key": "EI-LEAF",
                    "consumer_stage": "runnable",
                    "prerequisite_stage": "contract",
                    "status": "satisfied",
                    "required_for_v1": True,
                    "requirement_ids": ["GLOBAL-A"],
                    "required_evidence_ids": ["ev-ei"],
                    "required_owner_receipt_ids": [],
                })
                self.write_inputs(mapping)
                release = self.release_manifest()
                ei_evidence = next(row for row in release["evidence"] if row["id"] == "ev-ei")
                ei_evidence.update({
                    "stage_edge_id": "EDGE-CE-RUNNABLE-EI-CONTRACT",
                    "leaf_key": "EI-LEAF",
                    "stage": "contract",
                    "requirement_ids": ["GLOBAL-A"],
                })
                ei_evidence[field] = wrong
                self.release_path.write_bytes(_canonical_bytes(release))
                report = self.validate(release_path=self.release_path)
                self.assertIn("STAGED_EDGE_EVIDENCE_MISMATCH", self.codes(report))

        mapping = copy.deepcopy(self.map)
        ce_leaf = next(row for row in mapping["entries"] if row["key"] == "CE-LEAF")
        ce_leaf.pop("required_release_stage")
        self.write_inputs(mapping)
        release = self.release_manifest()
        self.release_path.write_bytes(_canonical_bytes(release))
        report = self.validate(release_path=self.release_path)
        self.assertIn("REQUIRED_RELEASE_STAGE_MISSING", self.codes(report))

    def test_release_requires_exact_revisions_modes_tests_and_receipts(self) -> None:
        release = self.release_manifest()
        release["outcomes"][0]["revision"] = "stale-revision"
        release["evidence"][1]["mode"] = "shadow"
        release["evidence"][2]["result"] = "skipped"
        release["outcomes"][2]["required_mode"] = "live"
        release["owner_receipts"] = []
        self.write_inputs()
        self.release_path.write_bytes(_canonical_bytes(release))
        codes = self.codes(self.validate(release_path=self.release_path))
        self.assertIn("OUTCOME_REVISION_UNTESTED", codes)
        self.assertIn("EVIDENCE_MODE_MISMATCH", codes)
        self.assertIn("REQUIRED_TEST_NOT_PASSED", codes)
        self.assertIn("REQUIRED_OWNER_RECEIPT_MISSING", codes)

        release = self.release_manifest()
        ce = next(item for item in release["outcomes"] if item["leaf_key"] == "CE-LEAF")
        ce["required_owner_receipts"] = []
        ce["owner_receipt_ids"] = []
        self.write_inputs()
        self.release_path.write_bytes(_canonical_bytes(release))
        self.assertIn("REQUIRED_OWNER_RECEIPTS_MISMATCH", self.codes(self.validate(release_path=self.release_path)))

    def test_release_digest_model_policy_and_prerequisite_fail_closed(self) -> None:
        release = self.release_manifest()
        release["evidence"][0]["sha256"] = "0" * 64
        release["evidence"][2]["model_revision"] = "wrong-model"
        release["evidence"][2]["policy_revision"] = "wrong-policy"
        release["outcomes"][1]["prerequisites_satisfied"] = False
        self.write_inputs()
        self.release_path.write_bytes(_canonical_bytes(release))
        codes = self.codes(self.validate(release_path=self.release_path))
        self.assertIn("EVIDENCE_DIGEST_MISMATCH", codes)
        self.assertIn("EVIDENCE_CONFIGURATION_PIN_MISMATCH", codes)
        self.assertIn("OUTCOME_PREREQUISITE_UNSATISFIED", codes)

    def test_release_configuration_expectations_are_pinned_by_the_leaf(self) -> None:
        self.write_inputs()
        for field in ("required_model_revision", "required_policy_revision"):
            for value in ("missing", None, [], "changed-value"):
                with self.subTest(field=field, value_type=type(value).__name__, value=value):
                    release = self.release_manifest()
                    ce = next(item for item in release["outcomes"] if item["leaf_key"] == "CE-LEAF")
                    if value == "missing":
                        ce.pop(field)
                    else:
                        ce[field] = value
                    ce_evidence = next(item for item in release["evidence"] if item["id"] == "ev-ce")
                    evidence_field = field.removeprefix("required_")
                    ce_evidence[evidence_field] = "wrong-configuration"
                    self.release_path.write_bytes(_canonical_bytes(release))
                    report = self.validate(release_path=self.release_path)
                    self.assertIn("REQUIRED_CONFIGURATION_PIN_MISMATCH", self.codes(report))
                    self.assertIn("EVIDENCE_CONFIGURATION_PIN_MISMATCH", self.codes(report))

        # The map has no model or policy requirement for AH/EI, so no outcome
        # fields are invented for those leaves.
        release = self.release_manifest()
        for outcome in release["outcomes"]:
            if outcome["leaf_key"] != "CE-LEAF":
                self.assertNotIn("required_model_revision", outcome)
                self.assertNotIn("required_policy_revision", outcome)
        self.release_path.write_bytes(_canonical_bytes(release))
        self.assertEqual(self.validate(release_path=self.release_path)["status"], "SCOPED_RELEASE_EVIDENCE_READY")

        release = self.release_manifest()
        ah = next(item for item in release["outcomes"] if item["leaf_key"] == "AH-LEAF")
        ah["required_model_revision"] = "invented-model"
        ah_evidence = next(item for item in release["evidence"] if item["id"] == "ev-ah")
        ah_evidence["model_revision"] = "invented-model"
        self.release_path.write_bytes(_canonical_bytes(release))
        report = self.validate(release_path=self.release_path)
        self.assertIn("OUTCOME_CONFIGURATION_PIN_UNDECLARED", self.codes(report))

    def test_invalid_map_configuration_pin_types_are_reported(self) -> None:
        for field in ("required_model_revision", "required_policy_revision"):
            for value in (None, [], {}, 4):
                with self.subTest(field=field, value_type=type(value).__name__):
                    mapping = copy.deepcopy(self.map)
                    ce = next(row for row in mapping["entries"] if row["key"] == "CE-LEAF")
                    ce[field] = value
                    self.write_inputs(mapping)
                    self.assertIn("ENTRY_REQUIRED_CONFIGURATION_PIN_INVALID", self.codes(self.validate()))

    def test_release_missing_artifact_is_named_without_path_echo(self) -> None:
        release = self.release_manifest()
        release["evidence"][0]["path"] = str(self.work / "absent-secret-path.txt")
        self.write_inputs()
        self.release_path.write_bytes(_canonical_bytes(release))
        report = self.validate(release_path=self.release_path)
        self.assertIn("EVIDENCE_UNREADABLE", self.codes(report))
        self.assertNotIn("absent-secret-path", json.dumps(report))

    def test_unreadable_release_manifest_does_not_rewrite_planning_status(self) -> None:
        self.write_inputs()
        report = self.validate(release_path=self.work / "missing-release.json")
        self.assertEqual(report["planning_status"], "BACKLOG_READY")
        self.assertEqual(report["release_status"], "BLOCKED")
        self.assertIn("INPUT_UNREADABLE", self.codes(report))

    def test_nonregular_inputs_and_evidence_are_rejected_without_blocking_open(self) -> None:
        if not hasattr(os, "mkfifo"):
            self.skipTest("named pipes are unavailable on this platform")
        input_pipe = self.work / "input.pipe"
        os.mkfifo(input_pipe)
        with self.assertRaises(validator.InputError) as raised:
            validator._read_bytes(input_pipe, 1024, "coverage-map")
        self.assertEqual(raised.exception.code, "INPUT_TYPE_OR_SIZE")

        evidence_pipe = self.evidence_dir / "evidence.pipe"
        os.mkfifo(evidence_pipe)
        release = self.release_manifest()
        release["evidence"][0]["path"] = str(evidence_pipe)
        self.write_inputs()
        self.release_path.write_bytes(_canonical_bytes(release))
        report = self.validate(release_path=self.release_path)
        self.assertIn("EVIDENCE_TYPE_OR_SIZE", self.codes(report))

    def test_release_requires_outcomes_for_exact_execution_prerequisite_closure(self) -> None:
        mapping = copy.deepcopy(self.map)
        next(row for row in mapping["entries"] if row["key"] == "EI-LEAF")["required_for_v1"] = False
        release = self.release_manifest()
        release["outcomes"] = [row for row in release["outcomes"] if row["leaf_key"] != "EI-LEAF"]
        self.write_inputs(mapping)
        self.release_path.write_bytes(_canonical_bytes(release))
        report = self.validate(release_path=self.release_path, require_specialists=False)
        self.assertIn("REQUIRED_PREREQUISITE_OUTCOME_MISSING", self.codes(report))

    def test_input_byte_limit_is_checked_before_parse(self) -> None:
        json_roles = (
            "coverage-map", "canonical-checklist", "issue-readbacks", "source-pins", "release-manifest"
        )
        for role in json_roles:
            limit = validator.LIMITS[role]
            maximum = self.work / (role + "-at-limit.json")
            with maximum.open("wb") as stream:
                stream.truncate(limit)
            self.assertEqual(len(validator._read_bytes(maximum, limit, role)), limit)

            oversized = self.work / (role + "-over-limit.json")
            with oversized.open("wb") as stream:
                stream.truncate(limit + 1)
            with self.assertRaises(validator.InputError) as raised:
                validator._read_bytes(oversized, limit, role)
            self.assertEqual(raised.exception.code, "INPUT_TYPE_OR_SIZE")

    def test_nested_duplicate_json_keys_and_unknown_source_alias_fail(self) -> None:
        self.write_inputs()
        self.map_path.write_text('{"x":{"same":1,"same":2}}', encoding="utf-8")
        self.assertIn("DUPLICATE_JSON_KEY", self.codes(self.validate()))

        self.write_inputs()
        pins = json.loads(self.pins_path.read_text(encoding="utf-8"))
        pins["upstream_git_objects"].append({"role": "invented-canonical-alias"})
        self.pins_path.write_bytes(_canonical_bytes(pins))
        self.assertIn("UPSTREAM_ROLE_UNSUPPORTED", self.codes(self.validate()))

    def test_malformed_json_returns_fixed_diagnostic(self) -> None:
        self.write_inputs()
        self.map_path.write_text("{", encoding="utf-8")
        report = self.validate()
        self.assertEqual(report["status"], "BLOCKED")
        self.assertIn("INVALID_JSON", self.codes(report))
        self.assertNotIn("JSONDecodeError", json.dumps(report))

        self.map_path.write_text('{"format":"agent-harness.coverage-map-current/v1","ignored":1e999}', encoding="utf-8")
        self.assertIn("INVALID_JSON_NUMBER", self.codes(self.validate()))

    def test_dated_source_reference_retains_snapshot_counts_without_using_them_as_limits(self) -> None:
        pins = json.loads((ROOT / "planning/agent-creative-v1/coverage-input-pins.json").read_text(encoding="utf-8"))
        dimensions = pins["coverage_map"]["observed_dimensions"]
        self.assertEqual(dimensions["required_leaf_records"], 59)
        self.assertEqual(dimensions["source_contribution_rows"], 555)
        self.assertEqual(dimensions["unique_mapped_requirement_ids"], 91)
        self.assertEqual(dimensions["local_scoped_requirement_ids"], 60)
        self.assertEqual(dimensions["global_ids_with_scoped_contributions"], 31)
        self.assertEqual(dimensions["global_criteria_total"], 47)
        self.assertEqual(dimensions["global_ids_outside_scoped_contribution"], 16)
        self.assertTrue(pins["interpretation"]["counts_are_snapshot_observations_not_limits"])

    def test_missing_readback_pin_is_blocked_and_report_does_not_guess(self) -> None:
        self.write_inputs()
        pins = json.loads(self.pins_path.read_text(encoding="utf-8"))
        del pins["issue_readbacks"]
        self.pins_path.write_bytes(_canonical_bytes(pins))
        report = self.validate()
        self.assertIn("ISSUE_READBACK_PIN_REQUIRED", self.codes(report))
        self.assertEqual(report["status"], "BLOCKED")
        self.assertIsNone(report["source_pins"]["issue_readback_capture_id"])

    def test_pending_external_blocker_keeps_planning_visible_and_fails_release(self) -> None:
        mapping = copy.deepcopy(self.map)
        ce = next(row for row in mapping["entries"] if row["key"] == "CE-LEAF")
        ce["blockers"] = [{"owner": "External finance owner", "action": "confirm scoped reporting boundary"}]
        self.write_inputs(mapping)
        plan = self.validate()
        self.assertEqual(plan["status"], "BACKLOG_READY")
        self.assertEqual(plan["coordination_status"], "pending")
        self.assertEqual(plan["publication_status"], "unreported")
        self.assertEqual(plan["release_status"], "not_run")
        self.assertEqual(plan["pending_blockers"], [{
            "record": "CE-LEAF",
            "owner": "External finance owner",
            "action": "confirm scoped reporting boundary",
        }])
        release = self.validate_release()
        self.assertEqual(release["status"], "BLOCKED")
        self.assertEqual(release["planning_status"], "BACKLOG_READY")
        self.assertEqual(release["release_status"], "BLOCKED")
        self.assertIn("REQUIRED_OUTCOME_BLOCKED", self.codes(release))

    def test_valid_supersession_requires_exact_bidirectional_relation(self) -> None:
        mapping = copy.deepcopy(self.map)
        prior = mapping["entries"][0]
        prior["required_for_v1"] = False
        replacement = copy.deepcopy(prior)
        replacement.update({
            "key": "AH-REPLACEMENT",
            "issue_url": "https://github.com/Example-Co/agent-harness/issues/404",
            "issue_title": "Synthetic replacement",
            "body_sha256": "d" * 64,
            "required_for_v1": True,
            "supersedes": "AH-LEAF",
        })
        prior["superseded_by"] = "AH-REPLACEMENT"
        prior["requirement_ids"] = []
        mapping["entries"].append(replacement)
        ownership = next(row for row in mapping["ownership_rows"] if row["requirement_id"] == "LOCAL-A")
        ownership["accountable_delivery_leaf"] = "AH-REPLACEMENT"
        ownership["acceptance_owner_leaf"] = "AH-REPLACEMENT"
        ownership["required_contributors"] = ["AH-REPLACEMENT"]
        next(row for row in mapping["source_contribution_rows"] if row["row_id"] == "ROW-1")["owning_leaf_key"] = "AH-REPLACEMENT"
        readbacks = copy.deepcopy(self.readbacks)
        readbacks["issues"].append({
            "repository": "agent-harness",
            "number": 404,
            "url": replacement["issue_url"],
            "title": replacement["issue_title"],
            "state": "open",
            "body_sha256": "d" * 64,
        })
        self.write_inputs(mapping, readbacks=readbacks)
        self.assertEqual(self.validate()["status"], "BACKLOG_READY")


if __name__ == "__main__":
    unittest.main()
