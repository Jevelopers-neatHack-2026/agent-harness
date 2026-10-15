"""Offline malformed-input and publication controls for the projection CLI."""

from __future__ import annotations

import contextlib
import copy
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "coverage_projector", ROOT / "scripts" / "project-agent-creative-coverage.py"
)
assert SPEC is not None and SPEC.loader is not None
projector = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(projector)


class CoverageProjectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.work = Path(self.temp.name)
        self.files = {name: self.work / f"{name}.json" for name in (
            "coverage", "pairs", "stages", "external", "readbacks", "canonical", "canonical-ref"
        )}
        digest = "a" * 64
        url = "https://github.com/Example-Co/agent-harness/issues/1"
        self.values: dict[str, Any] = {
            "coverage": {
                "format": "agent-harness.coverage-map-current/v1", "complete": False,
                "entries": [{"key": "AH-LEAF", "repository": "agent-harness", "issue_url": url,
                             "issue_title": "Synthetic", "live_issue_state": "open", "body_sha256": digest,
                             "requirement_ids": [], "tests": []}],
                "ownership_rows": [], "source_contribution_rows": [],
                "dag": {"edges": {"AH-LEAF": []}, "acyclic": True, "wave_count": 1,
                        "waves": [["AH-LEAF"]], "critical_path": ["AH-LEAF"],
                        "all_59_required_reachable": True},
            },
            "pairs": {"pairs": []},
            "stages": {"rows": []},
            "external": {"nodes": []},
            "readbacks": {"format": "agent-harness.issue-readbacks/v2", "capture_id": "capture-01",
                           "captured_at": "2026-10-11T17:00:00Z", "observed_revision": None,
                           "pages_complete": True, "issues": [{"repository": "Example-Co/agent-harness",
                               "number": 1, "url": url, "title": "Synthetic", "state": "open",
                               "body_sha256": digest}]},
            "canonical": {"criteria": []},
            "canonical-ref": {"repository": "Example-Co/contracts", "source_path": "planning/checklist.json",
                              "issue_pinned_revision": "1" * 40, "captured_main_revision": "2" * 40},
        }
        self._write_values(self.values)
        self.outputs = {"map": self.work / "projection.json", "pins": self.work / "pins.json"}

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write_values(self, values: dict[str, Any], *, ascii_only: bool = True) -> None:
        for role, value in values.items():
            self.files[role].write_text(
                json.dumps(value, ensure_ascii=ascii_only, separators=(",", ":")), encoding="utf-8"
            )

    def _argv(self) -> list[str]:
        argv: list[str] = []
        for arg, role in (
            ("--coverage-map", "coverage"), ("--pair-review", "pairs"),
            ("--stage-review", "stages"), ("--external-artifacts", "external"),
            ("--issue-readbacks", "readbacks"), ("--canonical-checklist", "canonical"),
            ("--canonical-reference", "canonical-ref"),
        ):
            argv.extend((arg, str(self.files[role])))
        argv.extend((
            "--canonical-repository", "Example-Co/contracts",
            "--canonical-source-path", "planning/checklist.json",
            "--canonical-issue-revision", "1" * 40,
            "--canonical-main-revision", "2" * 40,
            "--output-map", str(self.outputs["map"]), "--output-pins", str(self.outputs["pins"]),
        ))
        return argv

    def _run(self) -> tuple[int, str, str]:
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            status = projector.main(self._argv())
        return status, stdout.getvalue(), stderr.getvalue()

    def _assert_fixed_failure(self, expected_code: str) -> None:
        status, stdout, stderr = self._run()
        self.assertEqual(status, 2)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr), {"code": expected_code})
        self.assertNotIn(str(self.work), stderr)

    def test_minimal_valid_projection_is_written(self) -> None:
        status, stdout, stderr = self._run()
        self.assertEqual(status, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(json.loads(stdout)["status"], "projected")
        projection = json.loads(self.outputs["map"].read_text(encoding="utf-8"))
        self.assertEqual(projection["entries"][0]["key"], "AH-LEAF")

    def test_unhashable_json_shapes_return_fixed_diagnostics(self) -> None:
        bad_values = ([], {}, None, True, 17)
        cases: list[tuple[str, Any, str, str]] = []
        for value in bad_values:
            cases.append(("pairs", {"pairs": [{"requirement_id": "REQ", "leaf": "AH-LEAF",
                                                  "after_recommendation": value}]},
                          "pair-review-decision-invalid", repr(value)))
            coverage = copy.deepcopy(self.values["coverage"])
            coverage["ownership_rows"] = [{"requirement_id": "REQ", "accountable_delivery_leaf": value,
                                           "acceptance_owner_leaf": "AH-LEAF", "required_contributors": []}]
            cases.append(("coverage", coverage, "ownership-endpoint-invalid", repr(value)))
            cases.append(("stages", {"rows": [{"consumer_key": value, "prerequisite_key": "AH-LEAF",
                                                "consumer_stage": "runnable", "prerequisite_stage": "accepted_policy_fixture",
                                                "external_endpoint": False}]},
                          "stage-relation-endpoint-or-stage-invalid", repr(value)))
            cases.append(("external", {"nodes": [{"artifact_id": "EXT-01", "owner_role": "owner",
                                                   "deliverable": "artifact", "status": value}]},
                          "external-artifact-status-invalid", repr(value)))
        cases.extend((
            ("pairs", {"pairs": [{"requirement_id": "REQ", "leaf": "AH-LEAF",
                                    "after_recommendation": "unsupported"}]},
             "pair-review-decision-unsupported", "unsupported decision"),
            ("coverage", {**self.values["coverage"], "ownership_rows": [{
                "requirement_id": "REQ", "accountable_delivery_leaf": "missing-leaf",
                "acceptance_owner_leaf": "AH-LEAF", "required_contributors": [],
            }]}, "ownership-endpoint-invalid", "unknown string endpoint"),
            ("stages", {"rows": [{"consumer_key": "missing-leaf", "prerequisite_key": "AH-LEAF",
                                    "consumer_stage": "runnable", "prerequisite_stage": "accepted_policy_fixture",
                                    "external_endpoint": False}]},
             "stage-relation-endpoint-or-stage-invalid", "unknown string consumer"),
            ("external", {"nodes": [{"artifact_id": "EXT-01", "owner_role": "owner",
                                      "deliverable": "artifact", "status": "unsupported"}]},
             "external-artifact-status-invalid", "unsupported status"),
            ("stages", {"rows": [{"consumer_key": "AH-LEAF", "prerequisite_key": "AH-LEAF",
                                    "consumer_stage": "runnable", "prerequisite_stage": "accepted_policy_fixture",
                                    "external_endpoint": False, "reason": []}]},
             "stage-relation-reason-invalid", "unhashable reason"),
            ("stages", {"rows": [{"consumer_key": "AH-LEAF", "prerequisite_key": "AH-LEAF",
                                    "consumer_stage": "runnable", "prerequisite_stage": [],
                                    "external_endpoint": False}]},
             "stage-relation-endpoint-or-stage-invalid", "unhashable prerequisite stage"),
            ("external", {"nodes": [{"artifact_id": "EXT-01", "owner_role": "owner",
                                       "deliverable": "artifact", "status": "accepted",
                                       "action_status": [], "action": None}]},
             "external-artifact-action-invalid", "unhashable action status"),
        ))
        for role, value, expected, label in cases:
            with self.subTest(role=role, value=label):
                self.outputs["map"].unlink(missing_ok=True)
                self.outputs["pins"].unlink(missing_ok=True)
                changed = copy.deepcopy(self.values)
                changed[role] = value
                self._write_values(changed)
                self._assert_fixed_failure(expected)

        changed = copy.deepcopy(self.values)
        changed["stages"] = {"rows": [{"consumer_key": "AH-LEAF", "prerequisite_key": "AH-LEAF",
                                        "consumer_stage": "runnable", "prerequisite_stage": "accepted_policy_fixture",
                                        "external_endpoint": False, "reason": "unrecognized source reason"}]}
        self._write_values(changed)
        status, stdout, stderr = self._run()
        self.assertEqual(status, 0)
        self.assertEqual(stderr, "")
        projection = json.loads(self.outputs["map"].read_text(encoding="utf-8"))
        self.assertEqual(projection["stage_relations"][0]["applicability"], "unresolved")

    def test_unhashable_dag_edges_return_fixed_diagnostic(self) -> None:
        for value in ([[]], {}, None, True, 17):
            with self.subTest(value=repr(value)):
                self.outputs["map"].unlink(missing_ok=True)
                self.outputs["pins"].unlink(missing_ok=True)
                changed = copy.deepcopy(self.values)
                changed["coverage"]["dag"]["edges"]["AH-LEAF"] = value
                self._write_values(changed)
                self._assert_fixed_failure("execution-dag-invalid")

    def test_nonfinite_overflow_is_rejected_before_projection(self) -> None:
        raw = self.files["coverage"].read_text(encoding="utf-8")
        self.files["coverage"].write_text(raw[:-1] + ',"release":1e999}', encoding="utf-8")
        self._assert_fixed_failure("invalid-number")

    def test_escaped_lone_surrogate_gets_bounded_diagnostic(self) -> None:
        changed = dict(self.values)
        changed["coverage"] = {**self.values["coverage"], "release": "\ud800"}
        self._write_values(changed, ascii_only=True)
        self._assert_fixed_failure("coverage-map-invalid-string")

    def test_declared_relation_receipt_ids_survive_projection(self) -> None:
        relation = {"source_pointer": "/staged_edges/0", "consumer_key": "AH-LEAF",
                    "prerequisite_key": "EXT-01", "consumer_stage": "runnable",
                    "prerequisite_stage": "accepted_policy_fixture", "deliverable": "External fixture",
                    "evidence_file": "issues/1.md", "evidence_line": 1,
                    "reason": "external artifact, not external epic closure", "status": "satisfied",
                    "consumer_requirement_ids": [], "prerequisite_requirement_ids": None,
                    "shared_requirement_ids": [], "external_endpoint": True,
                    "disjoint_internal": False, "required_owner_receipt_ids": ["receipt-01"]}
        self.values["stages"] = {"rows": [relation]}
        self.values["external"] = {"nodes": [{"artifact_id": "EXT-01", "owner_role": "Contracts owner",
            "deliverable": "External fixture", "status": "pending_external",
            "action_status": "not_declared_in_source", "action": None}]}
        self._write_values(self.values)
        status, stdout, stderr = self._run()
        self.assertEqual(status, 0)
        self.assertEqual(stderr, "")
        projection = json.loads(self.outputs["map"].read_text(encoding="utf-8"))
        self.assertEqual(projection["stage_relations"][0]["required_owner_receipt_ids"], ["receipt-01"])
        self.assertEqual(projection["source_stage_rows"][0]["required_owner_receipt_ids"], ["receipt-01"])

    def test_failed_second_publication_removes_only_first_new_output(self) -> None:
        self.outputs["pins"].write_text("preexisting", encoding="utf-8")
        status, stdout, stderr = self._run()
        self.assertEqual(status, 2)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr), {"code": "output-must-be-new-regular-file"})
        self.assertFalse(self.outputs["map"].exists())
        self.assertEqual(self.outputs["pins"].read_text(encoding="utf-8"), "preexisting")


if __name__ == "__main__":
    unittest.main()
