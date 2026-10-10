from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tools import validate_jev_targets as jev  # noqa: E402


class ContractReleaseReadinessTests(unittest.TestCase):
    def run_contract(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(ROOT / "tools/verify_contract_release.py"), *args],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_pending_release_fails_with_all_exact_owner_actions(self) -> None:
        result = self.run_contract("--manifest", "fixtures/contracts/release.json", "--offline")
        self.assertEqual(result.returncode, 2)
        report = json.loads(result.stdout)
        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["dependency_mode"], "offline")
        self.assertFalse(report["ready"])
        codes = {error["code"] for error in report["errors"]}
        self.assertEqual(codes, {"canonical_release_not_published"})
        refs = {item["path"] for item in report["errors"]}
        self.assertEqual(len(refs), 9)
        self.assertTrue(all(error["owner"] and error["action"] for error in report["errors"]))
        self.assertNotIn("api_key", result.stdout.lower())

    def test_contract_verifier_rejects_artifacts_outside_the_pinned_package(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "release.json"
            manifest = {
                "format": "agent-creative.contract-readiness/v1",
                "status": "published",
                "canonical_release": {
                    "repository": "Jevelopers-neatHack-2026/shared-contracts",
                    "revision": "a" * 40,
                    "version": "test-only",
                    "schema_artifacts": [{"path": "../outside.json", "sha256": "0" * 64}],
                },
            }
            path.write_text(json.dumps(manifest), encoding="utf-8")
            result = self.run_contract("--manifest", str(path), "--offline")
        self.assertEqual(result.returncode, 2)
        report = json.loads(result.stdout)
        self.assertIn("artifact_path_escapes_release", {item["code"] for item in report["errors"]})
        self.assertFalse(report["ready"])

    def test_contract_verifier_rejects_schema_digest_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "schema.bin").write_bytes(b"owner fixture bytes")
            path = root / "release.json"
            manifest = {
                "format": "agent-creative.contract-readiness/v1",
                "status": "published",
                "canonical_release": {
                    "repository": "Jevelopers-neatHack-2026/shared-contracts",
                    "revision": "a" * 40,
                    "version": "test-only",
                    "schema_artifacts": [{"path": "schema.bin", "sha256": "0" * 64}],
                },
            }
            path.write_text(json.dumps(manifest), encoding="utf-8")
            result = self.run_contract("--manifest", str(path), "--offline")
        self.assertEqual(result.returncode, 2)
        report = json.loads(result.stdout)
        self.assertIn("artifact_sha256_mismatch", {item["code"] for item in report["errors"]})
        self.assertFalse(report["ready"])

    def test_contract_verifier_reports_invalid_json_without_echoing_input(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "release.json"
            path.write_text('{"credential":"do-not-echo", "credential":"duplicate"}', encoding="utf-8")
            result = self.run_contract("--manifest", str(path), "--offline")
        self.assertEqual(result.returncode, 2)
        report = json.loads(result.stdout)
        self.assertIn("manifest_unreadable_or_invalid_json", {item["code"] for item in report["errors"]})
        self.assertNotIn("do-not-echo", result.stdout)
        self.assertNotIn("duplicate", result.stdout)


class JevTargetReadinessTests(unittest.TestCase):
    def test_pending_owner_inventory_fails_and_calls_out_injection_and_brand_questions(self) -> None:
        result = subprocess.run(
            [sys.executable, str(ROOT / "tools/validate_jev_targets.py"),
             "--targets", str(ROOT / "evals/jev/promotion-targets.json"),
             "--dataset", str(ROOT / "evals/jev/dataset-manifest.json")],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 2)
        report = json.loads(result.stdout)
        self.assertEqual(report["status"], "blocked")
        self.assertFalse(report["ready"])
        actions = "\n".join(error["action"] for error in report["errors"])
        self.assertIn("injection_signal", actions)
        self.assertIn("brand_asset_classification", actions)
        self.assertIn("$.evaluation_budget.maximum_amount",
                      {item["path"] for item in report["errors"] if item["code"] == "numeric_owner_target_missing"})
        self.assertIn("dataset_owner_approval_pending", {item["code"] for item in report["errors"]})

    def test_noul_authority_and_mutable_alias_are_rejected(self) -> None:
        question = self.synthetic_question("SYNTH-TEST-NUOL", "injection_signal")
        question["authority_boundary"]["noul_has_provider_confidence"] = True
        question["model_pin"]["model_id"] = "latest"
        question["eligible_candidate_set"] = ["copied-engine-definition"]
        errors: list[dict[str, str]] = []
        question_id, _, _ = jev.validate_question(question, 0, {}, {name: [] for name in jev.SPLITS}, errors)
        self.assertEqual(question_id, "SYNTH-TEST-NUOL")
        codes = {item["code"] for item in errors}
        self.assertIn("noul_confidence_field_invalid", codes)
        self.assertIn("model_alias_not_pinned", codes)
        self.assertIn("engine_question_definition_embedded", codes)
        self.assertIn("numeric_owner_target_missing", codes)
        self.assertNotIn("ground_truth_label_missing", codes)
        self.assertTrue(all("action" in item and "owner" in item for item in errors))

    def test_split_hash_is_checked_and_duplicate_content_across_splits_is_rejected(self) -> None:
        row = {
            "sample_id": "synthetic-test-sample",
            "sample_sha256": "b" * 64,
            "labels": {"SYNTH-TEST-Q": "synthetic-label"},
            "strata": {"SYNTH-TEST-Q": {"malicious_source": "synthetic-bucket"}},
        }
        rows_hash = jev.canonical_hash([row])
        dataset = {
            "format": jev.DATASET_FORMAT,
            "status": "pending",
            "dataset_id": "synthetic-test-only",
            "dataset_version": "synthetic-test-only",
            "provenance": {},
            "owner_approval": None,
            "splits": {
                "train": {"sha256": rows_hash, "examples": [row]},
                "validation": {"sha256": rows_hash, "examples": [dict(row, sample_id="synthetic-test-sample-2")]},
                "held_out": {"sha256": "0" * 64, "examples": []},
            },
        }
        errors: list[dict[str, str]] = []
        hashes, _ = jev.validate_dataset(dataset, errors)
        codes = {item["code"] for item in errors}
        self.assertIn("split_sample_content_overlap", codes)
        self.assertIn("split_hash_mismatch", codes)
        self.assertIn("dataset_split_empty", codes)
        self.assertEqual(hashes["train"], rows_hash)

    def test_dependent_questions_cannot_share_a_parallel_batch(self) -> None:
        first = self.synthetic_question("SYNTH-TEST-A", "injection_signal")
        second = self.synthetic_question("SYNTH-TEST-B", "entity_matching")
        second["sequential_prerequisites"] = ["SYNTH-TEST-A"]
        first["evaluation_batch_id"] = second["evaluation_batch_id"] = "synthetic-parallel-batch"
        targets = {
            "format": jev.TARGET_FORMAT,
            "status": "pending",
            "owner_reviews": [],
            "question_inventory": [],
            "dataset_split_sha256": {},
            "questions": [first, second],
            "negative_results_policy": {},
            "shadow_only_counts_as_active": False,
            "active_routing_claim": False,
        }
        errors = jev.validate_targets(targets, {})["errors"]
        self.assertIn("dependent_questions_in_same_parallel_batch", {item["code"] for item in errors})

    def test_owner_question_inventory_ids_must_match_individual_targets(self) -> None:
        targets = {
            "format": jev.TARGET_FORMAT,
            "status": "pending",
            "owner_reviews": [],
            "question_inventory": [{
                "family": "injection_signal",
                "owner_repository": "event-intelligence",
                "owner_role": "Agent and research",
                "required_question_ids": ["SYNTH-TEST-OWNER-QUESTION"],
                "definition_revision": "a" * 40,
            }],
            "dataset_split_sha256": {},
            "questions": [],
            "negative_results_policy": {},
            "shadow_only_counts_as_active": False,
            "active_routing_claim": False,
        }
        errors = jev.validate_targets(targets, {})["errors"]
        self.assertIn("owner_required_question_target_missing", {item["code"] for item in errors})
        self.assertNotIn("target_manifest_contains_unapproved_fields", {item["code"] for item in errors})

    def test_baselines_must_use_same_inputs_eligibility_splits_and_cost_stages(self) -> None:
        base = {
            "code_revision": "a" * 40,
            "code_ref": "synthetic-test-only",
            "configuration_sha256": "b" * 64,
            "eligibility_sha256": "c" * 64,
            "input_set_sha256": "d" * 64,
            "dataset_split_sha256": {split: "e" * 64 for split in jev.SPLITS},
            "policy_revision": "synthetic-test-only",
            "downstream_cost_stages": ["synthetic-stage"],
            "owner_approval": None,
        }
        deterministic = dict(base, kind="deterministic", baseline_id="synthetic-test-det")
        generative = dict(base, kind="generative", baseline_id="synthetic-test-gen",
                          input_set_sha256="f" * 64, downstream_cost_stages=["unaccounted-stage"],
                          model_pin={"provider": "synthetic", "model_id": "pinned-test-model",
                                     "model_revision": "pinned-test-revision",
                                     "configuration_sha256": "1" * 64,
                                     "sampling_policy": "synthetic-test-only"})
        question = {"baselines": {"deterministic": deterministic, "generative": generative}}
        errors: list[dict[str, str]] = []
        jev.validate_baselines(question, 0, {split: "e" * 64 for split in jev.SPLITS},
                               ["synthetic-stage"], errors)
        codes = {item["code"] for item in errors}
        self.assertIn("baseline_inputs_not_comparable", codes)
        self.assertIn("baseline_cost_scope_not_comparable", codes)

    @staticmethod
    def synthetic_question(question_id: str, family: str) -> dict[str, object]:
        repository, owner_role = jev.REQUIRED_FAMILIES[family]
        return {
            "question_id": question_id,
            "family": family,
            "question_version": "synthetic-test-only",
            "definition_owner": {
                "repository": repository,
                "owner_role": owner_role,
                "issue_url": f"https://github.com/{jev.OWNER_ORG}/{repository}/issues/999999",
                "definition_revision": "a" * 40,
            },
            "owner_approval": {"status": "pending", "owner_role": owner_role},
            "primitive": "noul",
            "eligible_candidate_set_ref": "synthetic-test-only",
            "evidence_schema_ref": "synthetic-test-only",
            "instrumentation_ref": "synthetic-test-only",
            "unknown_behavior_ref": "synthetic-test-only",
            "review_behavior_ref": "synthetic-test-only",
            "fallback_behavior_ref": "synthetic-test-only",
            "schema_version": "synthetic-test-only",
            "policy_version": "synthetic-test-only",
            "rollback_owner_role": owner_role,
            "rollback_rule": "synthetic-test-only",
            "authority_boundary": {
                "model_can_authorize_budget": False,
                "model_can_approve_artifact": False,
                "model_can_change_permissions": False,
                "model_can_dispatch_privileged_actions": False,
                "provider_confidence_can_authorize": False,
                "noul_has_provider_confidence": False,
            },
            "model_pin": {
                "provider": "synthetic-test-only",
                "model_id": "pinned-test-model",
                "model_revision": "pinned-test-revision",
                "configuration_sha256": "b" * 64,
            },
            "ground_truth": {
                "positive_label": "synthetic-positive",
                "negative_label": "synthetic-negative",
                "unknown_label": "unknown",
                "review_label": "review",
            },
            "dataset_split_sha256": {split: "c" * 64 for split in jev.SPLITS},
            "coverage": {"minimum_samples_by_split": {}, "minimum_per_label_by_split": {}, "required_strata": {}},
            "promotion_metrics": {
                "primary_quality": {"name": "synthetic-metric", "direction": "minimum", "threshold": None},
                "false_negative_rate_max": None,
                "abstention_rate_max": None,
                "p95_latency_ms_max": None,
                "total_downstream_cost_max": {"amount": None, "currency": None, "cost_basis_ref": None},
                "downstream_cost_stages": ["synthetic-stage"],
            },
            "unknown_cost_rule": {"rule": "synthetic-test-only", "unknown_is_zero": False,
                                  "owner_approval": {"status": "pending", "owner_role": "Finance and reliability"}},
            "sequential_prerequisites": [],
            "evaluation_batch_id": "synthetic-test-batch",
            "baselines": {},
        }


if __name__ == "__main__":
    unittest.main()
