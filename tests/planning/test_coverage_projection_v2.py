"""Synthetic controls for the source-preserving directional coverage adapter."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "coverage_validator_v2", ROOT / "scripts" / "validate-agent-creative-coverage.py"
)
assert SPEC is not None and SPEC.loader is not None
validator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(validator)


def _bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _blob(raw: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(raw)).encode("ascii") + b"\0" + raw).hexdigest()


def _criterion(ident: str, title: str) -> dict[str, str]:
    return {
        "id": ident, "title": title, "priority": "P0", "owner_role": "Agent and research",
        "verification": "Run the exact producer and consumer fixture.",
        "evidence_required": "Versioned fixture result.",
    }


class CoverageProjectionV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.work = Path(self.temp.name)
        self.paths = {
            "map": self.work / "coverage-map.json", "pins": self.work / "source-pins.json",
            "canonical": self.work / "canonical.json", "readbacks": self.work / "readbacks.json",
            "release": self.work / "release.json",
        }
        self.canonical = {"criteria": [_criterion("REQ-GLOBAL", "Global scoped result"), _criterion("GLOBAL-OUTSIDE", "Out of scope") ]}
        self.readbacks = {
            "format": "agent-harness.issue-readbacks/v2", "capture_id": "synthetic-current-01",
            "captured_at": "2026-10-11T17:00:00Z", "observed_revision": None,
            "pages_complete": True, "issues": [],
        }
        self.coverage = self._coverage()
        self.write()

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def _row(entry_key: str, repository: str, number: int, reqs: list[str], digest: str) -> dict[str, Any]:
        url = f"https://github.com/Example-Co/{repository}/issues/{number}"
        return {
            "key": entry_key, "repository": repository, "issue_url": url,
            "owner_role": "Agent and research", "required_for_v1": True, "priority": "P0",
            "requirement_ids": reqs, "state": "in_progress", "historical_issue_state": "open",
            "historical_issue_title": f"Historical {entry_key}", "historical_body_sha256": digest,
            "body_semantic_disposition": "unadjudicated", "tests": [f"test-{entry_key.lower()}"],
            "required_tests": [f"test-{entry_key.lower()}"],
            "required_test_declaration_status": "declared_in_source",
            "required_evidence": [], "required_release_stage": "runnable", "required_mode": "mock",
            "required_owner_receipts": [],
            "blockers": [], "current_readback": {
                "repository": f"Example-Co/{repository}", "number": number, "url": url,
                "title": f"Historical {entry_key}", "state": "open", "body_sha256": digest,
            },
            "record_type": "delivery_leaf", "node_roles": ["required_acceptance_outcome"],
            "source_pointer": f"/entries/{number - 1}",
        }

    def _coverage(self) -> dict[str, Any]:
        historical = {key: hashlib.sha256(key.encode()).hexdigest() for key in ("AH-LEAF", "EI-LEAF", "CE-LEAF", "ACV1-RELEASE-GATE")}
        entries = [
            self._row("AH-LEAF", "agent-harness", 1, ["REQ-GLOBAL", "REQ-LOCAL"], historical["AH-LEAF"]),
            self._row("EI-LEAF", "event-intelligence", 2, ["REQ-GLOBAL", "REQ-LOCAL"], historical["EI-LEAF"]),
            self._row("CE-LEAF", "creative-engine", 4, [], historical["CE-LEAF"]),
            self._row("ACV1-RELEASE-GATE", "agent-harness", 3, ["REQ-GLOBAL"], historical["ACV1-RELEASE-GATE"]),
        ]
        entries[3]["node_roles"] = ["required_acceptance_outcome", "release_gate_fan_in"]
        ownership = [
            {"requirement_id": "REQ-GLOBAL", "scope": "scoped_contribution", "required_for_v1": True,
             "accountable_delivery_leaf": "AH-LEAF", "acceptance_owner_leaf": "ACV1-RELEASE-GATE",
             "required_contributors": ["AH-LEAF", "EI-LEAF"], "source_pointer": "/ownership_rows/0"},
            {"requirement_id": "REQ-LOCAL", "scope": "local", "required_for_v1": True,
             "accountable_delivery_leaf": "EI-LEAF", "acceptance_owner_leaf": "ACV1-RELEASE-GATE",
             "required_contributors": ["EI-LEAF", "AH-LEAF"], "source_pointer": "/ownership_rows/1"},
        ]
        global_meta = _criterion("REQ-GLOBAL", "Global scoped result")
        rows = [
            {"requirement_id": "REQ-GLOBAL", "owning_leaf_key": "AH-LEAF", "scope": "scoped_contribution",
             "authoritative_acceptance": global_meta, "row_id": "SRC-0001", "source_pointer": "/source_contribution_rows/0",
             "relation_role": "accountable_delivery_and_required_contributor"},
            {"requirement_id": "REQ-GLOBAL", "owning_leaf_key": "EI-LEAF", "scope": "scoped_contribution",
             "authoritative_acceptance": global_meta, "row_id": "SRC-0002", "source_pointer": "/source_contribution_rows/1",
             "relation_role": "required_contributor"},
            {"requirement_id": "REQ-GLOBAL", "owning_leaf_key": "AH-LEAF", "scope": "scoped_contribution",
             "authoritative_acceptance": global_meta, "row_id": "SRC-0003", "source_pointer": "/source_contribution_rows/2",
             "relation_role": "accountable_delivery_and_required_contributor"},
            {"requirement_id": "REQ-GLOBAL", "owning_leaf_key": "ACV1-RELEASE-GATE", "scope": "scoped_contribution",
             "authoritative_acceptance": global_meta, "row_id": "SRC-0004", "source_pointer": "/source_contribution_rows/3",
             "relation_role": "release_acceptance_contribution"},
            {"requirement_id": "REQ-LOCAL", "owning_leaf_key": "EI-LEAF", "scope": "local",
             "authoritative_acceptance": None, "row_id": "SRC-0005", "source_pointer": "/source_contribution_rows/4",
             "relation_role": "accountable_delivery_and_required_contributor"},
        ]
        pair_review = [
            {"review_row_id": "PAIR-0001", "source_pointer": "/pairs/0", "requirement_id": "REQ-GLOBAL", "leaf": "ACV1-RELEASE-GATE",
             "before": {"actual_contribution_row_present": True, "ownership_row_pointer": "/ownership_rows/0",
                        "contribution_row_pointer": "/source_contribution_rows/3", "accountable": False, "required_contributor": False},
             "after_recommendation": "PRESERVE_TYPED_SCOPED_CONTRIBUTION", "recommended_role": "release_acceptance_contribution",
             "evidence": [], "implementation_owner_change": False, "new_dependency_edge_implied": False},
            {"review_row_id": "PAIR-0002", "source_pointer": "/pairs/1", "requirement_id": "REQ-LOCAL", "leaf": "AH-LEAF",
             "before": {"actual_contribution_row_present": False, "ownership_row_pointer": "/ownership_rows/1",
                        "contribution_row_pointer": None, "accountable": False, "required_contributor": True},
             "after_recommendation": "ADD_SCOPED_ASSOCIATION", "recommended_role": "coordination_coverage_contribution",
             "evidence": [], "implementation_owner_change": False, "new_dependency_edge_implied": False},
        ]
        additions = [{"association_id": "ADD-01", "review_row_id": "PAIR-0002", "source_pointer": "/pairs/1",
                      "requirement_id": "REQ-LOCAL", "leaf_key": "AH-LEAF", "decision": "ADD_SCOPED_ASSOCIATION",
                      "recommended_role": "coordination_coverage_contribution", "evidence": []}]
        extras = [{"review_row_id": "PAIR-0001", "source_pointer": "/pairs/0", "requirement_id": "REQ-GLOBAL",
                   "leaf_key": "ACV1-RELEASE-GATE", "role": "release_acceptance_contribution", "evidence": []}]
        stage_sources = [
            {"source_pointer": "/staged_edges/0", "consumer_key": "EI-LEAF", "prerequisite_key": "AH-LEAF",
             "consumer_stage": "runnable", "prerequisite_stage": "canonical_agreement_and_fixture",
             "deliverable": "Consumer runnable fixture requires producer canonical fixture.",
             "evidence_file": "issues/2.md", "evidence_line": 7, "reason": "live body prerequisite", "status": "satisfied",
             "consumer_requirement_ids": ["REQ-LOCAL"], "prerequisite_requirement_ids": ["REQ-GLOBAL"],
             "required_owner_receipt_ids": [],
             "shared_requirement_ids": [], "external_endpoint": False, "disjoint_internal": True},
            {"source_pointer": "/staged_edges/1", "consumer_key": "ACV1-RELEASE-GATE", "prerequisite_key": "EXT-CANONICAL",
             "consumer_stage": "final_acceptance", "prerequisite_stage": "accepted_policy_fixture",
             "deliverable": "An external canonical artifact is required.", "evidence_file": "issues/24.md", "evidence_line": 12,
             "reason": "external artifact, not external epic closure", "status": "unverified",
             "consumer_requirement_ids": ["REQ-GLOBAL"], "prerequisite_requirement_ids": None,
             "required_owner_receipt_ids": [],
             "shared_requirement_ids": [], "external_endpoint": True, "disjoint_internal": False},
        ]
        stage_relations = [
            {"relation_id": f"STG-{i:03d}", "source_pointer": row["source_pointer"],
             "consumer_key": row["consumer_key"], "consumer_requirement_ids": row["consumer_requirement_ids"],
             "consumer_stage": row["consumer_stage"], "producer_key": None if row["external_endpoint"] else row["prerequisite_key"],
             "producer_requirement_ids": row["prerequisite_requirement_ids"],
             "external_artifact_id": row["prerequisite_key"] if row["external_endpoint"] else None,
             "prerequisite_stage": row["prerequisite_stage"], "deliverable": row["deliverable"],
             "reason": row["reason"], "status": row["status"],
             "source_evidence_file": row["evidence_file"], "source_evidence_line": row["evidence_line"],
             "shared_requirement_ids": row["shared_requirement_ids"], "external_endpoint": row["external_endpoint"],
             "disjoint_internal": row["disjoint_internal"],
             "required_owner_receipt_ids": row["required_owner_receipt_ids"],
             "applicability": "required", "application_scope": "declared_stage"}
            for i, row in enumerate(stage_sources)
        ]
        external_nodes = [{"artifact_id": "EXT-CANONICAL", "owner_role": "Shared-contracts owner",
                           "deliverable": "Accepted canonical schema fixture.", "status": "pending_external",
                           "action_status": "not_declared_in_source", "action": None,
                           "source_pointer": "/nodes/0", "upstream_source_pointer": "/external_artifact_nodes/EXT-CANONICAL",
                           "independent_artifact": True}]
        edges = {"AH-LEAF": [], "EI-LEAF": [], "CE-LEAF": [],
                 "ACV1-RELEASE-GATE": ["AH-LEAF", "EI-LEAF", "CE-LEAF"]}
        source_claims = {
            "acyclic": True, "wave_count": 2,
            "waves": [["AH-LEAF", "CE-LEAF", "EI-LEAF"], ["ACV1-RELEASE-GATE"]],
            "critical_path": ["EI-LEAF", "ACV1-RELEASE-GATE"],
            "all_59_required_reachable": True,
        }
        source_dimensions = {
            "required_issue_outcomes": len(entries), "source_contribution_rows": len(rows),
            "ownership_records": len(ownership), "canonical_criteria": len(self.canonical["criteria"]),
            "canonical_criteria_outside_scope": 1, "staged_relations": len(stage_relations),
            "external_artifact_nodes": len(external_nodes),
        }
        readbacks = []
        for entry in entries:
            self.readbacks["issues"].append({
                "repository": f"Example-Co/{entry['repository']}", "number": int(entry["issue_url"].rsplit("/", 1)[1]),
                "url": entry["issue_url"], "title": entry["historical_issue_title"], "state": "open",
                "body_sha256": entry["historical_body_sha256"],
            })
        return {
            "format": "agent-harness.coverage-projection/v2", "complete": True,
            "coordination_status": "pending", "publication_status": "unreported",
            "entries": entries, "ownership_rows": ownership, "source_contribution_rows": rows,
            "pair_review_rows": pair_review, "source_supported_associations": additions,
            "reviewed_typed_extras": extras, "unresolved_associations": [],
            "execution_graph": {"edges": edges,
                "gate_outcomes": [{"key": "ACV1-RELEASE-GATE", "outcome_required": True,
                                   "fan_in": ["AH-LEAF", "EI-LEAF", "CE-LEAF"], "node_role": "release_gate_fan_in"}],
                "source_claims": source_claims},
            "stage_relations": stage_relations, "source_stage_rows": stage_sources,
            "external_artifact_nodes": external_nodes, "source_dimensions": source_dimensions,
            "projection_inputs": [],
        }

    def write(self, *, coverage: dict[str, Any] | None = None, readbacks: dict[str, Any] | None = None) -> None:
        coverage = self.coverage if coverage is None else coverage
        readbacks = self.readbacks if readbacks is None else readbacks
        canonical_raw = _bytes(self.canonical)
        readbacks_raw = _bytes(readbacks)
        self.paths["canonical"].write_bytes(canonical_raw)
        self.paths["readbacks"].write_bytes(readbacks_raw)
        roles = ("coverage_map", "pair_review", "stage_review", "external_artifacts",
                 "issue_readbacks", "canonical_checklist", "canonical_reference")
        projection_inputs = [
            {"role": role, "source_path": f"synthetic/{role}.json", "bytes": 12,
             "sha256": hashlib.sha256(role.encode()).hexdigest()}
            for role in roles
        ]
        coverage["projection_inputs"] = projection_inputs
        map_raw = _bytes(coverage)
        self.paths["map"].write_bytes(map_raw)
        self.paths["pins"].write_bytes(_bytes({
            "format": "acv1.coverage-source-reference/v2", "snapshot_id": readbacks["capture_id"],
            "coverage_map": {"adapter": "agent-harness.coverage-projection/v2", "source_path": "synthetic/coverage-map.json",
                             "bytes": len(map_raw), "sha256": _sha(map_raw), "complete": coverage["complete"]},
            "upstream_git_objects": [{"role": "global-canonical-checklist",
                "adapter": "integration-evals.acceptance-checklist/v1", "repository": "Example-Co/contracts",
                "issue_pinned_revision": "1" * 40, "captured_main_revision": "2" * 40,
                "source_path": "planning/checklist.json", "git_blob_sha1": _blob(canonical_raw), "bytes": len(canonical_raw)}],
            "issue_readbacks": {"adapter": "agent-harness.issue-readbacks/v2", "capture_id": readbacks["capture_id"],
                "captured_at": readbacks["captured_at"], "observed_revision": readbacks.get("observed_revision"),
                "pages_complete": readbacks["pages_complete"], "bytes": len(readbacks_raw), "sha256": _sha(readbacks_raw)},
            "projection_inputs": projection_inputs,
        }))

    def validate(self, **kwargs: Any) -> dict[str, Any]:
        return validator.validate(self.paths["map"], self.paths["pins"], self.paths["canonical"], self.paths["readbacks"], **kwargs)

    @staticmethod
    def codes(report: dict[str, Any]) -> set[str]:
        return {row["code"] for row in report["issues"]}

    def test_valid_projection_keeps_acceptance_owner_separate_and_surfaces_external_blocker(self) -> None:
        report = self.validate(require_specialists=False)
        self.assertEqual(report["status"], "BACKLOG_READY")
        self.assertFalse(report["whole_program_acceptance"])
        self.assertEqual(report["counts"]["source_contribution_rows"], 5)
        self.assertEqual(report["counts"]["typed_extra_associations"], 1)
        self.assertEqual(report["pending_blockers"][0]["record"], "EXT-CANONICAL")
        self.assertIsNone(self.coverage["external_artifact_nodes"][0]["action"])
        self.assertNotIn("ACV1-RELEASE-GATE", self.coverage["ownership_rows"][1]["required_contributors"])

    def test_report_preserves_valid_json_pointer_provenance(self) -> None:
        report = self.validate(require_specialists=False)
        self.assertEqual(report["stage_relation_obligations"][0]["source_pointer"], "/staged_edges/0")
        self.assertEqual(report["external_artifact_obligations"][0]["source_pointer"], "/nodes/0")
        self.assertEqual(
            report["external_artifact_obligations"][0]["upstream_source_pointer"],
            "/external_artifact_nodes/EXT-CANONICAL",
        )

    def test_malformed_json_pointers_are_redacted_from_v2_obligations(self) -> None:
        cases = (
            ("stage_relations", "source_pointer", "/staged_edges/0~2", "stage_relation_obligations", "source_pointer",
             "STAGE_RELATION_SOURCE_NOT_FOUND"),
            ("external_artifact_nodes", "source_pointer", "/nodes/0\n", "external_artifact_obligations", "source_pointer",
             "EXTERNAL_ARTIFACT_SOURCE_POINTER_INVALID"),
            ("external_artifact_nodes", "upstream_source_pointer", [], "external_artifact_obligations", "upstream_source_pointer",
             "EXTERNAL_ARTIFACT_SOURCE_POINTER_INVALID"),
            ("external_artifact_nodes", "upstream_source_pointer", "/external_artifact_nodes/" + "x" * 4096,
             "external_artifact_obligations", "upstream_source_pointer", "EXTERNAL_ARTIFACT_SOURCE_POINTER_INVALID"),
        )
        for family, field, bad_value, result_family, result_field, issue_code in cases:
            with self.subTest(family=family, field=field, bad_value=repr(bad_value)):
                changed = copy.deepcopy(self.coverage)
                changed[family][0][field] = bad_value
                self.write(coverage=changed)
                report = self.validate(require_specialists=False)
                self.assertEqual(report["status"], "BLOCKED")
                self.assertEqual(report[result_family][0][result_field], "<invalid>")
                self.assertIn(issue_code, self.codes(report))

    def test_v2_readback_revision_must_match_the_pinned_observation(self) -> None:
        cases = (
            ("f" * 40, "e" * 40, True),
            ("f" * 40, None, True),
            ("f" * 40, "<missing>", True),
            ("f" * 40, "f" * 40, False),
            (None, None, False),
        )
        for pinned, observed, mismatch in cases:
            with self.subTest(pinned=pinned, observed=observed):
                readbacks = copy.deepcopy(self.readbacks)
                if observed == "<missing>":
                    readbacks.pop("observed_revision", None)
                else:
                    readbacks["observed_revision"] = observed
                self.write(readbacks=readbacks)
                pins = json.loads(self.paths["pins"].read_text(encoding="utf-8"))
                if pinned is None:
                    pins["issue_readbacks"].pop("observed_revision", None)
                else:
                    pins["issue_readbacks"]["observed_revision"] = pinned
                self.paths["pins"].write_bytes(_bytes(pins))

                report = self.validate(require_specialists=False)
                if mismatch:
                    self.assertEqual(report["status"], "BLOCKED")
                    self.assertIn("ISSUE_READBACK_REVISION_MISMATCH", self.codes(report))
                else:
                    self.assertNotIn("ISSUE_READBACK_REVISION_MISMATCH", self.codes(report))

    def test_v2_required_tests_need_explicit_declaration_for_planning_and_release(self) -> None:
        original_coverage = copy.deepcopy(self.coverage)
        cases = (
            ("missing_status", "missing", False),
            ("null_status", None, False),
            ("declared_without_list", "declared_in_source", True),
        )
        for label, declaration_status, declared_without_list in cases:
            with self.subTest(case=label):
                self.coverage = copy.deepcopy(original_coverage)
                release, expected_paths = self._release_manifest()
                for path, content in expected_paths.items():
                    path.write_text(content, encoding="utf-8")
                entry = self.coverage["entries"][0]
                if declaration_status == "missing":
                    entry.pop("required_test_declaration_status", None)
                else:
                    entry["required_test_declaration_status"] = declaration_status
                entry.pop("required_tests", None)
                # The historical tests field deliberately remains present in all three cases.
                self.write()
                self.paths["release"].write_bytes(_bytes(release))

                planning = self.validate(require_specialists=False)
                self.assertEqual(planning["status"], "BLOCKED")
                if declared_without_list:
                    self.assertIn("ENTRY_REQUIRED_TESTS_INVALID", self.codes(planning))
                else:
                    self.assertIn("ENTRY_REQUIRED_TEST_DECLARATION_STATUS_INVALID", self.codes(planning))

                released = self.validate(release_path=self.paths["release"], require_specialists=False)
                self.assertEqual(released["status"], "BLOCKED")
                self.assertEqual(released["planning_status"], "BLOCKED")
                self.assertEqual(released["release_status"], "BLOCKED")

    def test_missing_required_association_fails_from_independent_ownership_declarations(self) -> None:
        self.coverage["source_contribution_rows"] = [row for row in self.coverage["source_contribution_rows"]
            if not (row["requirement_id"] == "REQ-GLOBAL" and row["owning_leaf_key"] == "EI-LEAF")]
        self.coverage["source_dimensions"]["source_contribution_rows"] -= 1
        self.coverage["entries"][0]["source_pointer"] = "/entries/0"
        self.write()
        self.assertIn("REQUIRED_ASSOCIATION_MISSING", self.codes(self.validate(require_specialists=False)))

    def test_extraneous_untyped_contribution_is_rejected(self) -> None:
        row = copy.deepcopy(self.coverage["source_contribution_rows"][0])
        row.update({"requirement_id": "REQ-LOCAL", "owning_leaf_key": "ACV1-RELEASE-GATE",
                    "row_id": "SRC-0006", "source_pointer": "/source_contribution_rows/5",
                    "relation_role": "required_contributor"})
        self.coverage["source_contribution_rows"].append(row)
        self.coverage["source_dimensions"]["source_contribution_rows"] += 1
        self.write()
        self.assertIn("CONTRIBUTION_PAIR_UNDECLARED", self.codes(self.validate(require_specialists=False)))

    def test_source_critical_path_accepts_any_valid_longest_tie_but_rejects_invalid_claims(self) -> None:
        claims = self.coverage["execution_graph"]["source_claims"]
        claims["critical_path"] = ["AH-LEAF", "ACV1-RELEASE-GATE"]
        self.write()
        self.assertNotIn("EXECUTION_GRAPH_SOURCE_CLAIM_MISMATCH", self.codes(self.validate(require_specialists=False)))

        # The fixture's other longest path is valid as well, but equal length
        # alone does not make a path valid: these leaves have no edge between them.
        claims["critical_path"] = ["AH-LEAF", "CE-LEAF"]
        self.write()
        self.assertIn("EXECUTION_GRAPH_SOURCE_CLAIM_MISMATCH", self.codes(self.validate(require_specialists=False)))

        claims["critical_path"] = ["AH-LEAF", "ACV1-RELEASE-GATE", "AH-LEAF"]
        self.write()
        self.assertIn("EXECUTION_GRAPH_SOURCE_CLAIM_MISMATCH", self.codes(self.validate(require_specialists=False)))

        claims["critical_path"] = ["ACV1-RELEASE-GATE"]
        self.write()
        self.assertIn("EXECUTION_GRAPH_SOURCE_CLAIM_MISMATCH", self.codes(self.validate(require_specialists=False)))

        self.coverage["execution_graph"]["edges"]["AH-LEAF"] = ["ACV1-RELEASE-GATE"]
        claims["critical_path"] = ["AH-LEAF", "ACV1-RELEASE-GATE"]
        self.write()
        self.assertIn("EXECUTION_DAG_CYCLE", self.codes(self.validate(require_specialists=False)))

    def test_typed_extra_role_and_source_review_are_required(self) -> None:
        self.coverage["reviewed_typed_extras"][0]["role"] = "provider_configuration_readiness"
        self.write()
        codes = self.codes(self.validate(require_specialists=False))
        self.assertIn("PAIR_REVIEW_ROLE_MISMATCH", codes)
        self.assertIn("CONTRIBUTION_ROLE_MISMATCH", codes)

    def test_repeated_source_pairs_keep_distinct_rows_and_ids(self) -> None:
        report = self.validate(require_specialists=False)
        self.assertEqual(report["counts"]["source_contribution_rows"], 5)
        self.assertEqual(self.coverage["source_contribution_rows"][0]["owning_leaf_key"], self.coverage["source_contribution_rows"][2]["owning_leaf_key"])
        self.assertNotEqual(self.coverage["source_contribution_rows"][0]["row_id"], self.coverage["source_contribution_rows"][2]["row_id"])

    def test_directional_disjoint_prerequisites_are_validated_per_endpoint(self) -> None:
        report = self.validate(require_specialists=False)
        self.assertEqual(report["status"], "BACKLOG_READY")
        edge = self.coverage["stage_relations"][0]
        self.assertEqual(edge["consumer_requirement_ids"], ["REQ-LOCAL"])
        self.assertEqual(edge["producer_requirement_ids"], ["REQ-GLOBAL"])
        self.assertEqual(edge["shared_requirement_ids"], [])

    def test_wrong_side_requirement_substitution_is_rejected(self) -> None:
        self.coverage["stage_relations"][0]["consumer_requirement_ids"] = ["REQ-GLOBAL"]
        self.write()
        self.assertIn("STAGE_RELATION_CONSUMER_SOURCE_MISMATCH", self.codes(self.validate(require_specialists=False)))

    def test_shared_edge_v1_stage_vocabulary_remains_unchanged(self) -> None:
        fixture = json.loads((ROOT / "tests/planning/fixtures/coverage/map.json").read_text(encoding="utf-8"))
        fixture["staged_edges"][0]["consumer_stage"] = "accepted_policy_fixture"
        base = CoverageValidatorV1(self.work)
        base.write(fixture)
        self.assertIn("STAGED_EDGE_FIELD_INVALID", self.codes(base.validate()))

    def test_all_six_source_stages_are_admitted_without_collapsing_their_names(self) -> None:
        stages = sorted(validator.PROJECTION_STAGES - validator.STAGES)
        self.assertEqual(set(stages), {"accepted_policy_fixture", "canonical_agreement_and_fixture", "live_evidence", "readiness_config"})
        for stage in sorted(validator.PROJECTION_STAGES):
            mapping = copy.deepcopy(self.coverage)
            mapping["entries"][0]["required_release_stage"] = stage
            self.write(coverage=mapping)
            self.assertNotIn("ENTRY_REQUIRED_RELEASE_STAGE_INVALID", self.codes(self.validate(require_specialists=False)))

    def test_pending_external_artifact_is_not_mistaken_for_issue_closure(self) -> None:
        report = self.validate(require_specialists=False)
        self.assertEqual(report["planning_status"], "BACKLOG_READY")
        self.assertEqual(report["external_artifact_obligations"][0]["status"], "pending_external")
        self.assertIsNone(report["external_artifact_obligations"][0]["action"])

    def test_canonical_metadata_and_out_of_scope_criteria_are_preserved(self) -> None:
        self.coverage["source_contribution_rows"][0]["authoritative_acceptance"]["title"] = "rewritten"
        self.write()
        report = self.validate(require_specialists=False)
        self.assertIn("GLOBAL_ACCEPTANCE_METADATA_MISMATCH", self.codes(report))
        self.assertEqual(report["counts"]["global_ids_outside_scoped_contribution"], 1)

    def test_readback_body_drift_blocks_without_overwriting_historical_digest(self) -> None:
        changed = copy.deepcopy(self.readbacks)
        changed["issues"][0]["body_sha256"] = "f" * 64
        self.coverage["entries"][0]["current_readback"]["body_sha256"] = "f" * 64
        self.write(readbacks=changed)
        self.assertEqual(self.coverage["entries"][0]["historical_body_sha256"], hashlib.sha256(b"AH-LEAF").hexdigest())
        self.assertIn("ISSUE_BODY_CHANGE_UNADJUDICATED", self.codes(self.validate(require_specialists=False)))

    def test_readback_title_and_state_drift_remain_unadjudicated(self) -> None:
        changed = copy.deepcopy(self.readbacks)
        changed["issues"][0]["title"] = "Updated title"
        self.coverage["entries"][0]["current_readback"]["title"] = "Updated title"
        self.write(readbacks=changed)
        self.assertIn("ISSUE_TITLE_CHANGE_UNADJUDICATED", self.codes(self.validate(require_specialists=False)))

        changed = copy.deepcopy(self.readbacks)
        changed["issues"][0]["state"] = "closed"
        self.coverage["entries"][0]["current_readback"]["title"] = self.coverage["entries"][0]["historical_issue_title"]
        self.coverage["entries"][0]["current_readback"]["state"] = "closed"
        self.write(readbacks=changed)
        codes = self.codes(self.validate(require_specialists=False))
        self.assertIn("ISSUE_STATE_CHANGE_UNADJUDICATED", codes)
        self.assertIn("CLOSED_LEAF_WITHOUT_EXACT_EVIDENCE", codes)

    def test_incomplete_issue_readback_pages_fail_closed(self) -> None:
        changed = copy.deepcopy(self.readbacks)
        changed["pages_complete"] = False
        self.write(readbacks=changed)
        codes = self.codes(self.validate(require_specialists=False))
        self.assertIn("ISSUE_READBACK_PIN_INCOMPLETE", codes)
        self.assertIn("ISSUE_READBACK_PAGES_INCOMPLETE", codes)

    def test_historical_test_results_are_not_treated_as_required_test_declarations(self) -> None:
        entry = self.coverage["entries"][0]
        entry["tests"] = [{"command": "python -B -m unittest", "exit_status": 0, "passed": 12}]
        entry["required_tests"] = []
        entry["required_test_declaration_status"] = "not_declared_in_source"
        self.write()
        codes = self.codes(self.validate(require_specialists=False))
        self.assertIn("ENTRY_REQUIRED_TESTS_UNDECLARED", codes)
        self.assertNotIn("ENTRY_REQUIRED_TESTS_INVALID", codes)
        self.assertEqual(entry["tests"][0]["passed"], 12)

    def test_unresolved_accountability_decision_blocks_pair_instead_of_auto_adding(self) -> None:
        row = {"review_row_id": "PAIR-0003", "source_pointer": "/pairs/2", "requirement_id": "REQ-LOCAL", "leaf": "ACV1-RELEASE-GATE",
               "before": {"actual_contribution_row_present": False, "ownership_row_pointer": "/ownership_rows/1",
                          "contribution_row_pointer": None, "accountable": False, "required_contributor": True},
               "after_recommendation": "RETAIN_UNRESOLVED_ACCOUNTABILITY_DO_NOT_AUTO_ADD", "recommended_role": None,
               "evidence": [], "implementation_owner_change": False, "new_dependency_edge_implied": False}
        self.coverage["ownership_rows"][1]["required_contributors"].append("ACV1-RELEASE-GATE")
        next(entry for entry in self.coverage["entries"] if entry["key"] == "ACV1-RELEASE-GATE")["requirement_ids"].append("REQ-LOCAL")
        self.coverage["unresolved_associations"] = [{"requirement_id": "REQ-LOCAL", "leaf_key": "ACV1-RELEASE-GATE",
            "source_pointer": "/pairs/2", "review_row_id": "PAIR-0003", "decision": "RETAIN_UNRESOLVED_ACCOUNTABILITY_DO_NOT_AUTO_ADD"}]
        self.coverage["pair_review_rows"].append(row)
        self.write()
        self.assertIn("ASSOCIATION_OWNER_DECISION_UNRESOLVED", self.codes(self.validate(require_specialists=False)))

    def test_v2_release_binds_directional_stage_evidence_and_external_artifact(self) -> None:
        release, expected_paths = self._release_manifest()
        for path, content in expected_paths.items():
            path.write_text(content, encoding="utf-8")
        self.paths["release"].write_bytes(_bytes(release))
        report = self.validate(release_path=self.paths["release"], require_specialists=False)
        self.assertEqual(report["status"], "SCOPED_RELEASE_EVIDENCE_READY")
        mutations = {
            "relation_id": ("STG-UNKNOWN", "STAGE_EVIDENCE_CARDINALITY_INVALID"),
            "consumer_key": ("CE-LEAF", "STAGE_EVIDENCE_BINDING_MISMATCH"),
            "consumer_requirement_ids": (["REQ-GLOBAL"], "STAGE_EVIDENCE_BINDING_MISMATCH"),
            "consumer_stage": ("canonical_agreement_and_fixture", "STAGE_EVIDENCE_BINDING_MISMATCH"),
            "producer_key": ("CE-LEAF", "STAGE_EVIDENCE_BINDING_MISMATCH"),
            "producer_requirement_ids": (["REQ-LOCAL"], "STAGE_EVIDENCE_BINDING_MISMATCH"),
            "external_artifact_id": ("EXT-CANONICAL", "STAGE_EVIDENCE_BINDING_MISMATCH"),
            "producer_stage": ("fixture", "STAGE_EVIDENCE_BINDING_MISMATCH"),
            "evidence_id": ("ev-missing", "STAGE_EVIDENCE_BINDING_MISMATCH"),
            "producer_evidence_id": ("ev-missing", "STAGE_EVIDENCE_BINDING_MISMATCH"),
            "producer_revision": ("b" * 40, "STAGE_EVIDENCE_BINDING_MISMATCH"),
            "producer_mode": ("recorded", "STAGE_EVIDENCE_BINDING_MISMATCH"),
            "producer_content_sha256": ("f" * 64, "STAGE_EVIDENCE_BINDING_MISMATCH"),
            "content_sha256": ("f" * 64, "STAGE_EVIDENCE_BINDING_MISMATCH"),
            "source_revision": ("b" * 40, "STAGE_EVIDENCE_BINDING_MISMATCH"),
            "mode": ("recorded", "STAGE_EVIDENCE_BINDING_MISMATCH"),
        }
        for field, (value, expected_code) in mutations.items():
            with self.subTest(field=field):
                changed = copy.deepcopy(release)
                changed["stage_evidence"][0][field] = value
                self.paths["release"].write_bytes(_bytes(changed))
                report = self.validate(release_path=self.paths["release"], require_specialists=False)
                self.assertIn(expected_code, self.codes(report))

        changed = copy.deepcopy(release)
        changed["stage_evidence"][1]["external_artifact_id"] = "EXT-OTHER"
        self.paths["release"].write_bytes(_bytes(changed))
        report = self.validate(release_path=self.paths["release"], require_specialists=False)
        self.assertIn("STAGE_EVIDENCE_BINDING_MISMATCH", self.codes(report))

        changed = copy.deepcopy(release)
        stage_artifact = next(item for item in changed["evidence"] if item["id"] == "ev-stage-ah")
        stage_artifact["stage_binding"]["stage"] = "fixture"
        self.paths["release"].write_bytes(_bytes(changed))
        self.assertIn("STAGE_EVIDENCE_BINDING_MISMATCH", self.codes(
            self.validate(release_path=self.paths["release"], require_specialists=False)
        ))

        changed = copy.deepcopy(release)
        stage_artifact = next(item for item in changed["evidence"] if item["id"] == "ev-stage-ah")
        stage_artifact["stage_binding"]["requirement_ids"] = ["REQ-LOCAL"]
        self.paths["release"].write_bytes(_bytes(changed))
        self.assertIn("STAGE_EVIDENCE_BINDING_MISMATCH", self.codes(
            self.validate(release_path=self.paths["release"], require_specialists=False)
        ))

        changed = copy.deepcopy(release)
        changed["stage_evidence"][0]["producer_evidence_id"] = "ev-ce-leaf"
        self.paths["release"].write_bytes(_bytes(changed))
        self.assertIn("STAGE_EVIDENCE_BINDING_MISMATCH", self.codes(
            self.validate(release_path=self.paths["release"], require_specialists=False)
        ))

    def test_bound_prerequisite_artifact_does_not_require_producer_outcome_at_that_stage(self) -> None:
        release, expected_paths = self._release_manifest()
        for path, content in expected_paths.items():
            path.write_text(content, encoding="utf-8")
        self.coverage["entries"][0]["required_release_stage"] = "fixture"
        self.write()
        release["outcomes"][0]["evaluated_stage"] = "fixture"
        self.paths["release"].write_bytes(_bytes(release))
        report = self.validate(release_path=self.paths["release"], require_specialists=False)
        self.assertEqual(report["status"], "SCOPED_RELEASE_EVIDENCE_READY")

    def test_v2_relation_receipts_are_preserved_and_checked_only_when_declared(self) -> None:
        release, expected_paths = self._release_manifest()
        for path, content in expected_paths.items():
            path.write_text(content, encoding="utf-8")
        self.paths["release"].write_bytes(_bytes(release))
        self.assertEqual(self.validate(release_path=self.paths["release"], require_specialists=False)["status"],
                         "SCOPED_RELEASE_EVIDENCE_READY")

        receipt_id = "receipt-source-required"
        relation = self.coverage["stage_relations"][0]
        self.coverage["source_stage_rows"][0]["required_owner_receipt_ids"] = [receipt_id]
        self.write()
        self.assertIn("STAGE_RELATION_RECEIPTS_SOURCE_MISMATCH",
                      self.codes(self.validate(require_specialists=False)))
        relation["required_owner_receipt_ids"] = [receipt_id]
        self.write()
        self.paths["release"].write_bytes(_bytes(release))
        absent_codes = self.codes(self.validate(release_path=self.paths["release"], require_specialists=False))
        self.assertIn("REQUIRED_OWNER_RECEIPTS_MISMATCH", absent_codes)
        self.assertIn("STAGE_RELATION_OWNER_RECEIPT_MISSING", absent_codes)

        release, expected_paths = self._release_manifest()
        self.coverage["source_stage_rows"][0]["required_owner_receipt_ids"] = [receipt_id]
        self.coverage["stage_relations"][0]["required_owner_receipt_ids"] = [receipt_id]
        self.write()
        revision = "a" * 40
        receipt_content = "relation-bound receipt evidence"
        receipt_path = self.work / "stage-receipt.txt"
        receipt_path.write_text(receipt_content, encoding="utf-8")
        expected_paths[receipt_path] = receipt_content
        receipt_evidence_id = "ev-receipt-source"
        release["evidence"].append({
            "id": receipt_evidence_id, "path": str(receipt_path), "sha256": _sha(receipt_content.encode()),
            "producer": "synthetic-control", "source_revision": revision, "captured_at": "2026-10-11T17:00:00Z",
            "mode": "mock", "kind": "receipt",
            "receipt_binding": {"relation_id": "STG-000", "receipt_id": receipt_id,
                                "owner_leaf_key": "AH-LEAF", "external_artifact_id": None,
                                "producer_evidence_id": "ev-stage-ah", "producer_revision": revision,
                                "producer_mode": "mock",
                                "producer_content_sha256": next(
                                    item["sha256"] for item in release["evidence"] if item["id"] == "ev-stage-ah"
                                )},
        })
        ah_outcome = next(row for row in release["outcomes"] if row["leaf_key"] == "AH-LEAF")
        ah_outcome["evidence_ids"].append(receipt_evidence_id)
        ah_outcome["required_owner_receipts"] = [receipt_id]
        ah_outcome["owner_receipt_ids"] = [receipt_id]
        receipt = {
            "id": receipt_id, "status": "accepted", "source_revision": revision,
            "evidence_id": receipt_evidence_id, "owner_role": "Shared-contracts owner",
            "action": "Provide exact prerequisite receipt", "stage_relation_id": "STG-000",
            "owner_leaf_key": "AH-LEAF", "external_artifact_id": None,
        }
        release["owner_receipts"] = [receipt]
        self.paths["release"].write_bytes(_bytes(release))
        self.assertEqual(self.validate(release_path=self.paths["release"], require_specialists=False)["status"],
                         "SCOPED_RELEASE_EVIDENCE_READY")

        mutations = {
            "wrong_id": lambda doc: doc["owner_receipts"][0].update({"id": "receipt-other"}),
            "wrong_revision": lambda doc: doc["owner_receipts"][0].update({"source_revision": "b" * 40}),
            "rejected": lambda doc: doc["owner_receipts"][0].update({"status": "rejected"}),
            "wrong_evidence_kind": lambda doc: next(item for item in doc["evidence"] if item["id"] == receipt_evidence_id).update({"kind": "runtime"}),
        }
        for label, mutate in mutations.items():
            with self.subTest(receipt=label):
                changed = copy.deepcopy(release)
                mutate(changed)
                self.paths["release"].write_bytes(_bytes(changed))
                self.assertIn("STAGE_RELATION_OWNER_RECEIPT_MISSING",
                              self.codes(self.validate(release_path=self.paths["release"], require_specialists=False)))

    def test_internal_relation_receipt_binds_standalone_selected_artifact_revision(self) -> None:
        receipt_id = "receipt-standalone-prerequisite"
        revision_a = "a" * 40
        revision_b = "b" * 40

        def make_manifest(
            receipt_revision: str | None,
            *,
            selected_revision: str = revision_b,
        ) -> tuple[dict[str, Any], dict[Path, str]]:
            release, expected_paths = self._release_manifest()
            self.coverage["source_stage_rows"][0]["required_owner_receipt_ids"] = [receipt_id] if receipt_revision else []
            self.coverage["stage_relations"][0]["required_owner_receipt_ids"] = [receipt_id] if receipt_revision else []
            self.write()
            release["tested_revisions"].append({"repository": "agent-harness", "revision": revision_b})
            selected_artifact = next(item for item in release["evidence"] if item["id"] == "ev-stage-ah")
            selected_digest = selected_artifact["sha256"]
            selected_artifact["source_revision"] = selected_revision
            release["stage_evidence"][0]["producer_revision"] = selected_revision
            release["stage_evidence"][0]["producer_content_sha256"] = selected_digest
            ah_outcome = next(item for item in release["outcomes"] if item["leaf_key"] == "AH-LEAF")
            self.assertEqual(ah_outcome["revision"], revision_a)
            self.assertNotIn("ev-stage-ah", ah_outcome["evidence_ids"])
            if receipt_revision is not None:
                receipt_content = "standalone prerequisite receipt evidence"
                receipt_path = self.work / f"standalone-receipt-{receipt_revision[0]}.txt"
                receipt_path.write_text(receipt_content, encoding="utf-8")
                expected_paths[receipt_path] = receipt_content
                receipt_evidence_id = "ev-standalone-receipt"
                release["evidence"].append({
                    "id": receipt_evidence_id, "path": str(receipt_path),
                    "sha256": _sha(receipt_content.encode()), "producer": "synthetic-control",
                    "source_revision": receipt_revision, "captured_at": "2026-10-11T17:00:00Z",
                    "mode": "mock", "kind": "receipt",
                    "receipt_binding": {
                        "relation_id": "STG-000", "receipt_id": receipt_id,
                        "owner_leaf_key": "AH-LEAF", "external_artifact_id": None,
                        "producer_evidence_id": "ev-stage-ah", "producer_revision": selected_revision,
                        "producer_mode": "mock", "producer_content_sha256": selected_digest,
                    },
                })
                ah_outcome["evidence_ids"].append(receipt_evidence_id)
                ah_outcome["required_owner_receipts"] = [receipt_id]
                ah_outcome["owner_receipt_ids"] = [receipt_id]
                release["owner_receipts"] = [{
                    "id": receipt_id, "status": "accepted", "source_revision": receipt_revision,
                    "evidence_id": receipt_evidence_id, "owner_role": "Shared-contracts owner",
                    "action": "Provide receipt bound to the selected prerequisite artifact",
                    "stage_relation_id": "STG-000", "owner_leaf_key": "AH-LEAF",
                    "external_artifact_id": None,
                }]
            return release, expected_paths

        wrong_revision, expected_paths = make_manifest(revision_a)
        for path, content in expected_paths.items():
            path.write_text(content, encoding="utf-8")
        self.paths["release"].write_bytes(_bytes(wrong_revision))
        wrong_codes = self.codes(self.validate(release_path=self.paths["release"], require_specialists=False))
        self.assertIn("STAGE_RELATION_OWNER_RECEIPT_MISSING", wrong_codes)

        exact_revision, expected_paths = make_manifest(revision_b)
        for path, content in expected_paths.items():
            path.write_text(content, encoding="utf-8")
        self.paths["release"].write_bytes(_bytes(exact_revision))
        self.assertEqual(
            self.validate(release_path=self.paths["release"], require_specialists=False)["status"],
            "SCOPED_RELEASE_EVIDENCE_READY",
        )

        receipt_binding_mutations = {
            "arbitrary_receipt_identity": {"receipt_id": "receipt-arbitrary"},
            "wrong_selected_artifact": {"producer_evidence_id": "ev-ah-leaf"},
            "wrong_selected_revision": {"producer_revision": revision_a},
            "wrong_selected_mode": {"producer_mode": "recorded"},
            "wrong_selected_digest": {"producer_content_sha256": "f" * 64},
        }
        for label, fields in receipt_binding_mutations.items():
            with self.subTest(binding=label):
                changed = copy.deepcopy(exact_revision)
                evidence = next(item for item in changed["evidence"] if item["id"] == "ev-standalone-receipt")
                evidence["receipt_binding"].update(fields)
                self.paths["release"].write_bytes(_bytes(changed))
                changed_codes = self.codes(self.validate(release_path=self.paths["release"], require_specialists=False))
                self.assertIn("STAGE_RELATION_OWNER_RECEIPT_MISSING", changed_codes)
                if label == "arbitrary_receipt_identity":
                    self.assertIn("EVIDENCE_REVISION_MISMATCH", changed_codes)

        # A receipt declared independently on the producer leaf still has to
        # match the producer outcome at A. It cannot inherit the relation's B
        # exception merely because the relation reuses the same receipt ID.
        self.coverage["entries"][0]["required_owner_receipts"] = [receipt_id]
        self.write()
        self.paths["release"].write_bytes(_bytes(exact_revision))
        overlap_codes = self.codes(self.validate(release_path=self.paths["release"], require_specialists=False))
        self.assertIn("EVIDENCE_REVISION_MISMATCH", overlap_codes)
        self.assertIn("REQUIRED_OWNER_RECEIPT_MISSING", overlap_codes)

        # If both declarations name the same revision, ordinary validation
        # can satisfy both scopes without granting the revision exception.
        same_revision, expected_paths = make_manifest(revision_a, selected_revision=revision_a)
        for path, content in expected_paths.items():
            path.write_text(content, encoding="utf-8")
        self.paths["release"].write_bytes(_bytes(same_revision))
        self.assertEqual(
            self.validate(release_path=self.paths["release"], require_specialists=False)["status"],
            "SCOPED_RELEASE_EVIDENCE_READY",
        )

        # Separate receipts preserve strict A leaf evidence while the relation
        # receipt remains bound to the selected B artifact.
        ordinary_receipt_id = "receipt-ordinary-outcome"
        self.coverage["entries"][0]["required_owner_receipts"] = [ordinary_receipt_id]
        separate_receipts, expected_paths = make_manifest(revision_b)
        for path, content in expected_paths.items():
            path.write_text(content, encoding="utf-8")
        ordinary_content = "ordinary producer outcome receipt"
        ordinary_path = self.work / "ordinary-outcome-receipt.txt"
        ordinary_path.write_text(ordinary_content, encoding="utf-8")
        ordinary_evidence_id = "ev-ordinary-outcome-receipt"
        separate_receipts["evidence"].append({
            "id": ordinary_evidence_id, "path": str(ordinary_path),
            "sha256": _sha(ordinary_content.encode()), "producer": "synthetic-control",
            "source_revision": revision_a, "captured_at": "2026-10-11T17:00:00Z",
            "mode": "mock", "kind": "receipt",
        })
        ah_outcome = next(item for item in separate_receipts["outcomes"] if item["leaf_key"] == "AH-LEAF")
        ah_outcome["evidence_ids"].append(ordinary_evidence_id)
        ah_outcome["required_owner_receipts"] = [ordinary_receipt_id, receipt_id]
        ah_outcome["owner_receipt_ids"] = [ordinary_receipt_id, receipt_id]
        separate_receipts["owner_receipts"].append({
            "id": ordinary_receipt_id, "status": "accepted", "source_revision": revision_a,
            "evidence_id": ordinary_evidence_id, "owner_role": "Agent and research",
            "action": "Provide ordinary producer outcome receipt",
        })
        self.paths["release"].write_bytes(_bytes(separate_receipts))
        self.assertEqual(
            self.validate(release_path=self.paths["release"], require_specialists=False)["status"],
            "SCOPED_RELEASE_EVIDENCE_READY",
        )

        # With no declared relation receipt, an ordinary B receipt must stay at
        # the outcome revision A and cannot acquire a relation exception.
        self.coverage["entries"][0]["required_owner_receipts"] = [ordinary_receipt_id]
        ordinary_only, expected_paths = make_manifest(None)
        for path, content in expected_paths.items():
            path.write_text(content, encoding="utf-8")
        ordinary_content_b = "ordinary receipt from another revision"
        ordinary_path.write_text(ordinary_content_b, encoding="utf-8")
        ordinary_only["evidence"].append({
            "id": ordinary_evidence_id, "path": str(ordinary_path),
            "sha256": _sha(ordinary_content_b.encode()), "producer": "synthetic-control",
            "source_revision": revision_b, "captured_at": "2026-10-11T17:00:00Z",
            "mode": "mock", "kind": "receipt",
        })
        ah_outcome = next(item for item in ordinary_only["outcomes"] if item["leaf_key"] == "AH-LEAF")
        ah_outcome["evidence_ids"].append(ordinary_evidence_id)
        ah_outcome["required_owner_receipts"] = [ordinary_receipt_id]
        ah_outcome["owner_receipt_ids"] = [ordinary_receipt_id]
        ordinary_only["owner_receipts"] = [{
            "id": ordinary_receipt_id, "status": "accepted", "source_revision": revision_b,
            "evidence_id": ordinary_evidence_id, "owner_role": "Agent and research",
            "action": "Provide ordinary producer outcome receipt",
        }]
        self.paths["release"].write_bytes(_bytes(ordinary_only))
        ordinary_codes = self.codes(self.validate(release_path=self.paths["release"], require_specialists=False))
        self.assertIn("EVIDENCE_REVISION_MISMATCH", ordinary_codes)
        self.assertIn("REQUIRED_OWNER_RECEIPT_MISSING", ordinary_codes)

        self.coverage["entries"][0]["required_owner_receipts"] = []
        receipt_free, expected_paths = make_manifest(None)
        for path, content in expected_paths.items():
            path.write_text(content, encoding="utf-8")
        self.paths["release"].write_bytes(_bytes(receipt_free))
        self.assertEqual(
            self.validate(release_path=self.paths["release"], require_specialists=False)["status"],
            "SCOPED_RELEASE_EVIDENCE_READY",
        )

    def test_external_relation_receipt_is_bound_to_consumer_outcome(self) -> None:
        receipt_id = "receipt-external-required"
        self.coverage["source_stage_rows"][1]["required_owner_receipt_ids"] = [receipt_id]
        self.coverage["stage_relations"][1]["required_owner_receipt_ids"] = [receipt_id]
        release, expected_paths = self._release_manifest()
        revision = "a" * 40
        receipt_content = "synthetic external-relation receipt"
        receipt_path = self.work / "external-stage-receipt.txt"
        receipt_path.write_text(receipt_content, encoding="utf-8")
        expected_paths[receipt_path] = receipt_content
        receipt_evidence_id = "ev-external-receipt"
        release["evidence"].append({
            "id": receipt_evidence_id, "path": str(receipt_path), "sha256": _sha(receipt_content.encode()),
            "producer": "synthetic-control", "source_revision": revision,
            "captured_at": "2026-10-11T17:00:00Z", "mode": "mock", "kind": "receipt",
            "receipt_binding": {"relation_id": "STG-001", "receipt_id": receipt_id,
                                "owner_leaf_key": "ACV1-RELEASE-GATE",
                                "external_artifact_id": "EXT-CANONICAL"},
        })
        gate_outcome = next(row for row in release["outcomes"] if row["leaf_key"] == "ACV1-RELEASE-GATE")
        gate_outcome["evidence_ids"].append(receipt_evidence_id)
        gate_outcome["required_owner_receipts"] = [receipt_id]
        gate_outcome["owner_receipt_ids"] = [receipt_id]
        release["owner_receipts"] = [{
            "id": receipt_id, "status": "accepted", "source_revision": revision,
            "evidence_id": receipt_evidence_id, "owner_role": "External fixture owner",
            "action": "Provide the declared external-stage receipt", "stage_relation_id": "STG-001",
            "owner_leaf_key": "ACV1-RELEASE-GATE", "external_artifact_id": "EXT-CANONICAL",
        }]
        self.paths["release"].write_bytes(_bytes(release))
        self.assertEqual(self.validate(release_path=self.paths["release"], require_specialists=False)["status"],
                         "SCOPED_RELEASE_EVIDENCE_READY")

        changed = copy.deepcopy(release)
        changed["outcomes"][-1]["owner_receipt_ids"] = []
        self.paths["release"].write_bytes(_bytes(changed))
        self.assertIn("STAGE_RELATION_OWNER_RECEIPT_MISSING",
                      self.codes(self.validate(release_path=self.paths["release"], require_specialists=False)))

    def test_v2_map_closed_requires_closed_issue_readback(self) -> None:
        self.coverage["entries"][0]["state"] = "closed"
        self.coverage["entries"][0]["closure_evidence"] = [{"commit": "a" * 40, "tests": ["test-ah-leaf"]}]
        self.write()
        codes = self.codes(self.validate(require_specialists=False))
        self.assertIn("ISSUE_READBACK_STATE_MISMATCH", codes)

    def test_v2_closed_issue_with_exact_closure_evidence_remains_valid(self) -> None:
        entry = self.coverage["entries"][0]
        entry["state"] = "closed"
        entry["historical_issue_state"] = "closed"
        entry["current_readback"]["state"] = "closed"
        entry["closure_evidence"] = [{"commit": "a" * 40, "tests": ["test-ah-leaf"]}]
        self.readbacks["issues"][0]["state"] = "closed"
        self.write()
        self.assertEqual(self.validate(require_specialists=False)["status"], "BACKLOG_READY")

    def test_v2_internal_completion_does_not_claim_issue_closed(self) -> None:
        self.coverage["entries"][0]["state"] = "complete"
        self.write()
        self.assertEqual(self.validate(require_specialists=False)["status"], "BACKLOG_READY")

    def test_v2_release_cannot_lower_a_leafs_declared_stage(self) -> None:
        release, expected_paths = self._release_manifest()
        for path, content in expected_paths.items():
            path.write_text(content, encoding="utf-8")
        release["outcomes"][0]["evaluated_stage"] = "fixture"
        self.paths["release"].write_bytes(_bytes(release))
        report = self.validate(release_path=self.paths["release"], require_specialists=False)
        self.assertIn("OUTCOME_EVALUATED_STAGE_MISMATCH", self.codes(report))

    def test_release_stays_blocked_when_external_artifact_is_pending(self) -> None:
        release, expected_paths = self._release_manifest()
        self.coverage["external_artifact_nodes"][0]["status"] = "pending_external"
        for path, content in expected_paths.items():
            path.write_text(content, encoding="utf-8")
        self.write()
        self.paths["release"].write_bytes(_bytes(release))
        report = self.validate(release_path=self.paths["release"], require_specialists=False)
        self.assertEqual(report["planning_status"], "BACKLOG_READY")
        self.assertEqual(report["release_status"], "BLOCKED")
        self.assertIn("REQUIRED_EXTERNAL_ARTIFACT_PENDING", self.codes(report))

    def _release_manifest(self) -> tuple[dict[str, Any], dict[Path, str]]:
        self.coverage["external_artifact_nodes"][0]["status"] = "accepted"
        self.coverage["stage_relations"][1]["status"] = "satisfied"
        self.coverage["source_stage_rows"][1]["status"] = "satisfied"
        revision = "a" * 40
        leaf_data = {
            "AH-LEAF": "canonical_agreement_and_fixture",
            "EI-LEAF": "runnable",
            "CE-LEAF": "live_evidence",
            "ACV1-RELEASE-GATE": "final_acceptance",
        }
        paths: dict[Path, str] = {}
        evidence: list[dict[str, Any]] = []
        outcomes: list[dict[str, Any]] = []
        ids: dict[str, str] = {}
        for key, stage in leaf_data.items():
            entry = next(row for row in self.coverage["entries"] if row["key"] == key)
            entry["required_release_stage"] = stage
            test_id = entry["tests"][0]
            content = f"synthetic evidence for {key}"
            path = self.work / f"{key}.txt"
            path.write_text(content, encoding="utf-8")
            evidence_id = f"ev-{key.lower()}"
            ids[key] = evidence_id
            evidence.append({"id": evidence_id, "path": str(path), "sha256": _sha(content.encode()),
                             "producer": "synthetic-control", "source_revision": revision,
                             "captured_at": "2026-10-11T17:00:00Z", "mode": "mock", "kind": "test",
                             "test_id": test_id, "result": "passed"})
            outcomes.append({"leaf_key": key, "status": "verified", "revision": revision,
                             "evaluated_stage": stage, "prerequisites_satisfied": True,
                             "required_test_ids": [test_id], "evidence_ids": [evidence_id],
                             "required_mode": "mock", "owner_receipt_ids": [],
                             "required_owner_receipts": []})
            paths[path] = content
        internal, external = self.coverage["stage_relations"]
        stage_evidence = [
            {"relation_id": internal["relation_id"], "consumer_key": internal["consumer_key"],
             "consumer_requirement_ids": internal["consumer_requirement_ids"], "consumer_stage": internal["consumer_stage"],
             "producer_key": internal["producer_key"], "producer_requirement_ids": internal["producer_requirement_ids"],
             "external_artifact_id": None, "producer_stage": internal["prerequisite_stage"],
             "evidence_id": ids["EI-LEAF"], "producer_evidence_id": ids["AH-LEAF"],
             "producer_revision": revision, "producer_mode": "mock",
             "producer_content_sha256": None,
             "content_sha256": next(item["sha256"] for item in evidence if item["id"] == ids["EI-LEAF"]),
             "source_revision": revision, "mode": "mock"},
            {"relation_id": external["relation_id"], "consumer_key": external["consumer_key"],
             "consumer_requirement_ids": external["consumer_requirement_ids"], "consumer_stage": external["consumer_stage"],
             "producer_key": None, "producer_requirement_ids": None,
             "external_artifact_id": external["external_artifact_id"], "producer_stage": external["prerequisite_stage"],
             "evidence_id": ids["ACV1-RELEASE-GATE"], "producer_evidence_id": None,
             "producer_revision": None, "producer_mode": None, "producer_content_sha256": None,
             "content_sha256": next(item["sha256"] for item in evidence if item["id"] == ids["ACV1-RELEASE-GATE"]),
             "source_revision": revision, "mode": "mock"},
        ]
        stage_content = "synthetic canonical prerequisite artifact"
        stage_path = self.work / "stage-artifact-ah.txt"
        stage_path.write_text(stage_content, encoding="utf-8")
        stage_id = "ev-stage-ah"
        stage_digest = _sha(stage_content.encode())
        evidence.append({
            "id": stage_id, "path": str(stage_path), "sha256": stage_digest,
            "producer": "synthetic-control", "source_revision": revision,
            "captured_at": "2026-10-11T17:00:00Z", "mode": "mock", "kind": "configuration",
            "stage_binding": {"relation_id": internal["relation_id"], "leaf_key": "AH-LEAF",
                              "requirement_ids": ["REQ-GLOBAL"], "stage": "canonical_agreement_and_fixture"},
        })
        paths[stage_path] = stage_content
        stage_evidence[0]["producer_evidence_id"] = stage_id
        stage_evidence[0]["producer_content_sha256"] = stage_digest
        self.write()
        tested_repositories = sorted({entry["repository"] for entry in self.coverage["entries"]})
        return ({"format": "acv1.release-manifest/v2", "tested_revisions": [
                    {"repository": repository, "revision": revision} for repository in tested_repositories],
                 "evidence": evidence, "outcomes": outcomes, "owner_receipts": [], "stage_evidence": stage_evidence}, paths)


class CoverageValidatorV1:
    """Small adapter around the existing v1 synthetic fixture to prove stage isolation."""

    def __init__(self, work: Path) -> None:
        self.work = work
        self.paths = {name: work / f"v1-{name}.json" for name in ("map", "pins", "canonical", "readbacks")}

    def write(self, mapping: dict[str, Any]) -> None:
        fixture = ROOT / "tests/planning/fixtures/coverage"
        original = json.loads((fixture / "map.json").read_text(encoding="utf-8"))
        canonical = json.loads((fixture / "canonical.json").read_text(encoding="utf-8"))
        readbacks = json.loads((fixture / "readbacks.json").read_text(encoding="utf-8"))
        original["staged_edges"] = mapping.get("staged_edges", original["staged_edges"])
        raw_map, raw_canonical, raw_readbacks = _bytes(original), _bytes(canonical), _bytes(readbacks)
        self.paths["map"].write_bytes(raw_map)
        self.paths["canonical"].write_bytes(raw_canonical)
        self.paths["readbacks"].write_bytes(raw_readbacks)
        self.paths["pins"].write_bytes(_bytes({
            "format": "acv1.coverage-source-reference/v1", "snapshot_id": readbacks["capture_id"],
            "coverage_map": {"adapter": "agent-harness.coverage-map-current/v1", "source_path": "fixture/map.json",
                "bytes": len(raw_map), "sha256": _sha(raw_map), "complete": original["complete"],
                "observed_dimensions": {"required_leaf_records": 3, "agent_harness_leaf_records": 1,
                    "specialist_leaf_records": 2, "source_contribution_rows": 3,
                    "unique_mapped_requirement_ids": 2, "local_scoped_requirement_ids": 1,
                    "global_ids_with_scoped_contributions": 1, "global_criteria_total": 1,
                    "global_ids_outside_scoped_contribution": 0, "contribution_rows_to_global_ids": 2,
                    "contribution_rows_to_local_ids": 1}},
            "upstream_git_objects": [{"role": "global-canonical-checklist", "adapter": "integration-evals.acceptance-checklist/v1",
                "repository": "Example-Co/contracts", "issue_pinned_revision": "1" * 40, "captured_main_revision": "2" * 40,
                "source_path": "planning/checklist.json", "git_blob_sha1": _blob(raw_canonical), "bytes": len(raw_canonical)}],
            "issue_readbacks": {"adapter": "agent-harness.issue-readbacks/v1", "capture_id": readbacks["capture_id"],
                "captured_at": readbacks["captured_at"], "observed_revision": readbacks.get("observed_revision"),
                "pages_complete": True, "bytes": len(raw_readbacks), "sha256": _sha(raw_readbacks)},
        }))

    @staticmethod
    def codes(report: dict[str, Any]) -> set[str]:
        return {row["code"] for row in report["issues"]}

    def validate(self) -> dict[str, Any]:
        return validator.validate(self.paths["map"], self.paths["pins"], self.paths["canonical"], self.paths["readbacks"])


if __name__ == "__main__":
    unittest.main()
