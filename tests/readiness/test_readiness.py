from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tools import validate_jev_targets as jev  # noqa: E402
from tools import verify_contract_release as contracts  # noqa: E402


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def validate_synthetic_fixture(schema_path: Path, fixture_path: Path) -> dict:
    """Small TEST-ONLY schema reader; it is never used for canonical release readiness."""
    schema_bytes = schema_path.read_bytes()
    fixture_bytes = fixture_path.read_bytes()
    schema = json.loads(schema_bytes.decode("utf-8"))
    data = json.loads(fixture_bytes.decode("utf-8"))
    accepted = isinstance(schema, dict) and schema.get("type") == "object" and isinstance(data, dict)
    required = schema.get("required") if isinstance(schema, dict) else None
    properties = schema.get("properties") if isinstance(schema, dict) else None
    if not isinstance(required, list) or any(not isinstance(name, str) for name in required):
        accepted = False
        required = []
    if not isinstance(properties, dict):
        accepted = False
        properties = {}
    if accepted and any(name not in data for name in required):
        accepted = False
    for name, descriptor in properties.items():
        if not isinstance(name, str) or not isinstance(descriptor, dict):
            accepted = False
            continue
        expected_type = descriptor.get("type")
        value = data.get(name)
        matches = {
            "integer": type(value) is int,
            "string": isinstance(value, str),
            "boolean": isinstance(value, bool),
            "number": type(value) in {int, float} and not isinstance(value, bool),
        }.get(expected_type, False)
        if name in data and not matches:
            accepted = False
    serialized = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                            allow_nan=False).encode("utf-8")
    return {
        "outcome": "accepted" if accepted else "rejected",
        "schema_sha256": sha(schema_bytes),
        "fixture_sha256": sha(fixture_bytes),
        "serialized_bytes": serialized,
        "serialized_output_sha256": sha(serialized),
    }


def synthetic_approval(role: str) -> dict[str, str]:
    return {"status": "synthetic_test_only_unverified", "owner_role": role}


def synthetic_question(question_id: str, family: str, role: str, repository: str,
                       split_hashes: dict[str, str], metering_stages: list[str],
                       cost_applicability: dict[str, dict[str, str]],
                       input_hash: str, eligibility_hash: str, budget_hash: str) -> dict:
    labels = {"positive_label": "positive", "negative_label": "negative",
              "unknown_label": "unknown", "review_label": "review"}
    owner_ref = "synthetic://" + repository + "/definitions/" + question_id
    question = {
        "question_id": question_id,
        "family": family,
        "question_version": "synthetic-test-only-v1",
        "definition_owner": {
            "repository": repository, "owner_role": role, "issue_url": owner_ref,
            "definition_revision": "a" * 40,
        },
        "owner_approval": synthetic_approval(role),
        "primitive": "choice",
        "eligible_candidate_set_ref": "synthetic://candidate-set/" + question_id,
        "evidence_schema_ref": "synthetic://evidence-schema/" + question_id,
        "unknown_behavior_ref": "synthetic://unknown/" + question_id,
        "review_behavior_ref": "synthetic://review/" + question_id,
        "fallback_behavior_ref": "synthetic://fallback/" + question_id,
        "instrumentation_ref": "synthetic://instrumentation/" + question_id,
        "schema_version": "synthetic-schema-revision",
        "policy_version": "synthetic-policy-revision",
        "rollback_owner_role": role,
        "rollback_rule": "synthetic-test-only-stop",
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
            "model_id": "synthetic-model-pin",
            "model_revision": "synthetic-model-revision",
            "configuration_sha256": "1" * 64,
        },
        "ground_truth": labels,
        "dataset_split_sha256": dict(split_hashes),
        "candidate_comparison": {
            "input_set_sha256": input_hash,
            "eligibility_sha256": eligibility_hash,
            "budget_plan_sha256": budget_hash,
            "dataset_split_sha256": dict(split_hashes),
        },
        "coverage": {
            "minimum_samples_by_split": {split: 4 for split in jev.SPLITS},
            "minimum_per_label_by_split": {
                label: {split: 1 for split in jev.SPLITS} for label in labels.values()
            },
            "required_strata": {
                stratum: {"applicability": "not_applicable", "reason": "synthetic-test-only"}
                for stratum in jev.REQUIRED_STRATA
            },
        },
        "promotion_metrics": {
            "primary_quality": {"name": "synthetic-test-metric", "direction": "minimum", "threshold": 0.8},
            "false_negative_rate_max": 0.2,
            "abstention_rate_max": 0.2,
            "p95_latency_ms_max": 100,
            "total_downstream_cost_max": {"amount": 1, "currency": "TEST", "cost_basis_ref": "synthetic-test-only"},
            "downstream_cost_stages": list(metering_stages),
            "downstream_cost_applicability": copy.deepcopy(cost_applicability),
        },
        "unknown_cost_rule": {
            "rule": "synthetic-test-only-unknown-remains-unknown",
            "unknown_is_zero": False,
            "owner_approval": synthetic_approval("Finance and reliability"),
        },
        "sequential_prerequisites": [],
        "evaluation_batch_id": "synthetic-batch-" + question_id,
        "baselines": {},
    }
    base = {
        "code_revision": "b" * 40,
        "code_ref": "synthetic://baseline/" + question_id,
        "configuration_sha256": "2" * 64,
        "eligibility_sha256": eligibility_hash,
        "input_set_sha256": input_hash,
        "budget_plan_sha256": budget_hash,
        "dataset_split_sha256": dict(split_hashes),
        "policy_revision": "synthetic-policy-revision",
        "downstream_cost_stages": list(metering_stages),
        "downstream_cost_applicability": copy.deepcopy(cost_applicability),
        "owner_approval": synthetic_approval("Finance and reliability"),
    }
    question["baselines"] = {
        "deterministic": dict(copy.deepcopy(base), kind="deterministic", baseline_id="synthetic-det-" + question_id),
        "generative": dict(
            copy.deepcopy(base), kind="generative", baseline_id="synthetic-gen-" + question_id,
            model_pin={"provider": "synthetic-test-only", "model_id": "synthetic-baseline-pin",
                       "model_revision": "synthetic-baseline-revision", "configuration_sha256": "3" * 64,
                       "sampling_policy": "synthetic-test-only"},
        ),
    }
    return question


def complete_synthetic_plan() -> tuple[dict, dict]:
    """Build a fully populated plan-shaped TEST ONLY input, never owner approval."""
    owner_by_family = jev.REQUIRED_FAMILIES
    question_ids = {family: "SYNTH-" + family.upper() for family in owner_by_family}
    question_id_list = list(question_ids.values())
    splits: dict[str, dict] = {}
    for split in jev.SPLITS:
        rows = []
        labels_cycle = ("positive", "negative", "unknown", "review")
        for index, label in enumerate(labels_cycle):
            sample_id = "SYNTH-" + split + "-" + str(index)
            row_labels = {qid: labels_cycle[index] for qid in question_id_list}
            row = {
                "sample_id": sample_id,
                "sample_sha256": sha((split + ":" + str(index)).encode("utf-8")),
                "labels": row_labels,
                "eligible_question_ids": list(question_id_list),
                "strata": {qid: {} for qid in question_id_list},
            }
            rows.append(row)
        rows.sort(key=lambda item: item["sample_id"])
        splits[split] = {"sha256": jev.canonical_hash(rows), "examples": rows}
    split_hashes = {split: splits[split]["sha256"] for split in jev.SPLITS}
    dataset = {
        "format": jev.DATASET_FORMAT,
        "status": "test_only_synthetic",
        "dataset_id": "synthetic-test-only-dataset",
        "dataset_version": "synthetic-test-only-revision",
        "owner_repository": "event-intelligence",
        "provenance": {
            "source_refs": ["synthetic://source"],
            "collection_protocol": "synthetic-test-only-collection",
            "label_protocol": "synthetic-test-only-labels",
            "adjudication": "synthetic-test-only-adjudication",
            "privacy_review_ref": "synthetic-test-only-privacy-review",
        },
        "owner_approval": synthetic_approval("Agent and research"),
        "split_approvals": {split: synthetic_approval("Agent and research") for split in jev.SPLITS},
        "splits": splits,
    }
    stages = ["synthetic-classification", "synthetic-research", "synthetic-creative-image", "synthetic-inspection"]
    applicability = {stage: {"status": "required"} for stage in stages}
    metering_inventory = {
        "format": jev.METERING_INVENTORY_FORMAT,
        "status": "test_only_synthetic",
        "owner_repository": "cost-intelligence",
        "owner_role": "Finance and reliability",
        "revision": "c" * 40,
        "decision_ref": "synthetic://cost-intelligence/metering-inventory",
        "completeness_decision": True,
        "stages": [{"stage_id": stage, "scope": "synthetic-test-only"} for stage in stages],
        "owner_approval": synthetic_approval("Finance and reliability"),
    }
    evaluation_budget = {
        "budget_scope_ref": "synthetic-test-only-budget-scope",
        "currency": "TEST",
        "maximum_amount": 1,
        "owner_approval": synthetic_approval("Finance and reliability"),
        "paid_execution_allowed": False,
    }
    budget_hash = jev.evaluation_budget_plan_hash(evaluation_budget)
    questions = []
    inventory = []
    for family, (repository, role) in owner_by_family.items():
        qid = question_ids[family]
        input_hash, eligibility_hash = jev.dataset_scope_hashes(
            qid, {split: entry["examples"] for split, entry in splits.items()})
        question = synthetic_question(qid, family, role, repository, split_hashes, stages, applicability,
                                      input_hash, eligibility_hash, budget_hash or "")
        questions.append(question)
        inventory.append({
            "family": family,
            "owner_repository": repository,
            "owner_role": role,
            "required_question_ids": [qid],
            "definition_revision": "a" * 40,
            "owner_approval": synthetic_approval(role),
        })
    targets = {
        "format": jev.TARGET_FORMAT,
        "status": "test_only_synthetic",
        "owner_reviews": [
            {"status": "synthetic_test_only_unverified", "owner_role": "Agent and research", "owner_repository": "agent-harness"},
            {"status": "synthetic_test_only_unverified", "owner_role": "Creative and fulfillment", "owner_repository": "creative-engine"},
            {"status": "synthetic_test_only_unverified", "owner_role": "Finance and reliability", "owner_repository": "cost-intelligence"},
        ],
        "dataset_split_sha256": dict(split_hashes),
        "question_inventory": inventory,
        "questions": questions,
        "negative_results_policy": {
            "on_target_miss": "block_activation",
            "preserve_original_required_target": True,
            "retroactive_lowering_after_results": False,
            "target_change_policy": "synthetic-test-only-version-and-rerun",
        },
        "shadow_only_counts_as_active": False,
        "active_routing_claim": False,
        "evaluation_follow_on": {"issue_ref": jev.REQUIRED_EVALUATION_FOLLOW_ON,
                                 "owner_role": "Agent and research", "status": "pending"},
        "rollback_owner_role": "Agent and research",
        "rollback_rule": "synthetic-test-only-stop",
        "evaluation_budget": evaluation_budget,
        "metering_stage_inventory": metering_inventory,
    }
    for path, approval, role, repository, subject_id, content_hash in jev.approval_subjects(targets, dataset):
        if isinstance(approval, dict):
            approval.update({"subject_id": subject_id, "subject_sha256": content_hash})
    return targets, dataset


class ContractReleaseReadinessTests(unittest.TestCase):
    def run_contract(self, *args: str) -> subprocess.CompletedProcess[str]:
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "PYTHONDONTWRITEBYTECODE": "1",
               "LC_ALL": "C.UTF-8", "PYTHONHASHSEED": "0"}
        return subprocess.run(
            [sys.executable, str(ROOT / "tools/verify_contract_release.py"), *args],
            cwd=ROOT, env=env, text=True, capture_output=True, check=False, timeout=10,
        )

    def test_pending_release_preserves_all_owner_actions_and_fails_closed_without_trusted_index(self) -> None:
        result = self.run_contract("--manifest", "fixtures/contracts/release.json", "--offline")
        self.assertEqual(result.returncode, 2)
        report = json.loads(result.stdout)
        self.assertEqual(report["status"], "blocked")
        self.assertFalse(report["ready"])
        actions = [error for error in report["errors"] if error["code"] == "canonical_release_not_published"]
        self.assertEqual(len(actions), 9)
        self.assertIn("owner_evidence_index_unavailable", {item["code"] for item in report["errors"]})
        self.assertTrue(all(error["owner"] and error["action"] for error in report["errors"]))
        self.assertNotIn("api_key", result.stdout.lower())

    def test_artifact_paths_cannot_escape_package(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            outside = root / "outside.json"
            outside.write_text('{"synthetic":true}', encoding="utf-8")
            errors: list[dict[str, str]] = []
            path, digest = contracts.check_artifact(root / "package", {"path": "../outside.json", "sha256": sha(outside.read_bytes())},
                                                    "$.schema", "shared-contracts", errors)
        self.assertIsNone(path)
        self.assertIsNone(digest)
        self.assertIn("artifact_path_invalid", {item["code"] for item in errors})

    def test_artifact_digest_substitution_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "schema.json").write_text('{"type":"object"}', encoding="utf-8")
            errors: list[dict[str, str]] = []
            path, digest = contracts.check_artifact(root, {"path": "schema.json", "sha256": "0" * 64},
                                                    "$.schema", "shared-contracts", errors)
        self.assertIsNone(path)
        self.assertIsNone(digest)
        self.assertIn("artifact_sha256_mismatch", {item["code"] for item in errors})

    def test_empty_json_schema_and_fixture_are_not_semantic_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "empty-schema.json").write_text("{}", encoding="utf-8")
            (root / "empty-fixture.json").write_text("{}", encoding="utf-8")
            manifest = {
                "format": contracts.FORMAT,
                "status": "published",
                "canonical_release": {
                    "repository": "Jevelopers-neatHack-2026/shared-contracts",
                    "revision": "a" * 40,
                    "version": "synthetic-test-only",
                    "schema_artifacts": [{"schema_id": "SYNTH", "path": "empty-schema.json",
                                          "sha256": sha((root / "empty-schema.json").read_bytes()), "media_type": "application/json"}],
                    "field_inventory": {"path": "missing-inventory.json", "sha256": "0" * 64},
                    "validator_adapter": {"protocol": contracts.ADAPTER_PROTOCOL,
                                          "path": "missing-adapter.py", "sha256": "0" * 64,
                                          "evidence_id": "not-trusted"},
                    "compatibility": {"unknown_additive_fields": ["ignore"]},
                },
                "field_crosswalk": [], "fixtures": [{"case_id": "current_valid", "path": "empty-fixture.json",
                                                    "sha256": sha((root / "empty-fixture.json").read_bytes()),
                                                    "data_classification": "synthetic"}],
                "fixture_crosswalk": [], "owner_decisions": [],
            }
            errors: list[dict[str, str]] = []
            contracts.validate_and_run(manifest, root / "release.json", errors, evidence=None)
        codes = {item["code"] for item in errors}
        self.assertIn("canonical_schema_empty", codes)
        self.assertIn("fixture_content_empty", codes)
        self.assertIn("additive_field_policy_missing", codes)

    def test_invalid_and_older_consumer_additive_cases_require_owner_serialization_hashes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-required-serialization-") as temp:
            root = Path(temp)
            fixture_path = root / "fixture.json"
            fixture_path.write_text('{"synthetic":true}', encoding="utf-8")
            fixture_hash = sha(fixture_path.read_bytes())
            fixtures = []
            for case_id, outcome in (("current_invalid", "rejected"),
                                     ("old_consumer_additive_field", "compatibility_declared")):
                fixtures.append({
                    "case_id": case_id, "path": fixture_path.name, "sha256": fixture_hash,
                    "schema_id": "SYNTH", "schema_sha256": "a" * 64,
                    "contract_id": "SYNTH-CONTRACT", "primitive": "none",
                    "expected_outcome": outcome, "data_classification": "synthetic",
                })
            errors: list[dict[str, str]] = []
            contracts._validate_fixture_map({"fixtures": fixtures, "fixture_crosswalk": []}, {},
                                            {"schemas": []}, root, errors)
        missing_hash_cases = {
            item["path"] for item in errors if item["code"] == "canonical_serialization_hash_missing"
        }
        self.assertEqual(missing_hash_cases, {
            "$.fixtures[0].serialized_sha256", "$.fixtures[1].serialized_sha256",
        })

    def test_contract_untrusted_array_values_return_typed_errors(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-contract-malformed-") as temp:
            root = Path(temp)
            (root / "schema.json").write_text('{"type":"object"}', encoding="utf-8")
            (root / "fixture.json").write_text('{"value":1}', encoding="utf-8")
            fixture_bytes = (root / "fixture.json").read_bytes()
            manifest = {
                "format": contracts.FORMAT,
                "status": "published",
                "canonical_release": {
                    "repository": "Jevelopers-neatHack-2026/shared-contracts",
                    "revision": "a" * 40,
                    "version": "synthetic-test-only",
                    "schema_artifacts": [{"schema_id": "SYNTH", "path": "schema.json",
                                          "sha256": sha((root / "schema.json").read_bytes()),
                                          "media_type": "application/json"}],
                    "field_inventory": {"path": "missing-inventory.json", "sha256": "0" * 64},
                    "validator_adapter": {"protocol": contracts.ADAPTER_PROTOCOL, "path": "missing-adapter.py",
                                          "sha256": "0" * 64, "evidence_id": "synthetic-test-only"},
                    "compatibility": {"unknown_additive_fields": []},
                },
                "field_crosswalk": [],
                "fixtures": [{"case_id": "current_valid", "path": "fixture.json", "sha256": sha(fixture_bytes),
                              "schema_id": [], "schema_sha256": "0" * 64, "contract_id": [], "primitive": [],
                              "expected_outcome": "accepted", "data_classification": [],
                              "serialized_sha256": "1" * 64}],
                "fixture_crosswalk": [{"case_id": [], "schema_id": [], "schema_sha256": [],
                                        "contract_id": [], "primitive": [], "fixture_sha256": []}],
                "owner_decisions": [],
            }
            errors: list[dict[str, str]] = []
            contracts.validate_and_run(manifest, root / "release.json", errors, evidence=None)
        codes = {item["code"] for item in errors}
        self.assertIn("fixture_schema_contract_primitive_unmapped", codes)
        self.assertIn("fixture_data_classification_missing", codes)
        self.assertIn("fixture_crosswalk_identity_invalid", codes)

    def test_exact_schema_field_crosswalk_rejects_missing_field_even_with_tags(self) -> None:
        schema_sha = "a" * 64
        inventory = {"schemas": [{"schema_id": "SYNTH-SCHEMA", "schema_sha256": schema_sha,
                                  "contract_id": "SYNTH-CONTRACT", "primitive": "none",
                                  "field_paths": ["/realField"], "aliases": []}]}
        schemas = {"SYNTH-SCHEMA": {"sha256": schema_sha}}
        all_fake_tags = ["approval", "financial", "artifact", "DecisionPort", "reservation"]
        row = {
            "schema_id": "SYNTH-SCHEMA", "schema_sha256": schema_sha,
            "contract_id": "SYNTH-CONTRACT", "primitive": "none", "field_path": "/inventedAlias",
            "coverage_tags": all_fake_tags, "meaning": "synthetic", "producer_role": "Agent and research",
            "producer_leaf": "https://github.com/Jevelopers-neatHack-2026/agent-harness/issues/1",
            "consumer_role": "Creative and fulfillment",
            "consumer_leaf": "https://github.com/Jevelopers-neatHack-2026/creative-engine/issues/1",
            "trusted_derivation": "synthetic", "unavailable_semantics": "synthetic", "precision": "synthetic",
            "privacy_retention": "synthetic", "compatibility_rule": "synthetic",
            "producer_review": {}, "consumer_review": {},
        }
        errors: list[dict[str, str]] = []
        contracts.validate_crosswalk({"field_crosswalk": [row]}, schemas, inventory, None, errors)
        codes = {item["code"] for item in errors}
        self.assertIn("crosswalk_field_not_in_pinned_inventory", codes)
        self.assertIn("crosswalk_field_missing", codes)
        self.assertIn("untrusted_coverage_tags_forbidden", codes)
        self.assertIn("actual_provider_primitive_inventory_missing", codes)

    def test_fixture_adapter_echo_without_observed_bytes_is_rejected(self) -> None:
        fixture = {"schema_id": "SYNTH-SCHEMA", "sha256": "b" * 64, "contract_id": "SYNTH-CONTRACT",
                   "primitive": "none", "expected_outcome": "accepted", "serialized_sha256": "c" * 64}
        request = {"protocol": contracts.ADAPTER_PROTOCOL, "release_revision": "d" * 40}
        echoed = {"protocol": contracts.ADAPTER_PROTOCOL, "release_revision": "d" * 40,
                  "results": [{"case_id": "current_valid", "outcome": "accepted",
                               "serialized_sha256": "c" * 64}]}
        errors: list[dict[str, str]] = []
        result = contracts._validate_adapter_result(json.dumps(echoed).encode(), request,
                                                    {"current_valid": fixture}, {"SYNTH-SCHEMA": "a" * 64}, errors)
        self.assertIsNone(result)
        self.assertIn("canonical_validator_schema_observation_mismatch", {item["code"] for item in errors})
        self.assertIn("canonical_serialized_bytes_missing", {item["code"] for item in errors})

    def test_observed_serialized_bytes_are_hashed_by_parent(self) -> None:
        serialized = b'{"value":7}'
        fixture = {"schema_id": "SYNTH-SCHEMA", "sha256": "b" * 64, "contract_id": "SYNTH-CONTRACT",
                   "primitive": "none", "expected_outcome": "accepted", "serialized_sha256": sha(serialized)}
        request = {"protocol": contracts.ADAPTER_PROTOCOL, "release_revision": "d" * 40}
        response = {"protocol": contracts.ADAPTER_PROTOCOL, "release_revision": "d" * 40,
                    "results": [{"case_id": "current_valid", "outcome": "accepted",
                                 "observed_schema_sha256": "a" * 64, "observed_fixture_sha256": "b" * 64,
                                 "contract_id": "SYNTH-CONTRACT", "primitive": "none",
                                 "serialized_bytes_base64": base64.b64encode(serialized).decode("ascii")}]}
        errors: list[dict[str, str]] = []
        result = contracts._validate_adapter_result(json.dumps(response).encode(), request,
                                                    {"current_valid": fixture}, {"SYNTH-SCHEMA": "a" * 64}, errors)
        self.assertEqual(errors, [])
        self.assertEqual(result["serialized_output_sha256"]["current_valid"], sha(serialized))

    def test_substituted_serialized_bytes_and_case_identity_are_rejected(self) -> None:
        expected_bytes = b'{"value":7}'
        fixture = {"schema_id": "SYNTH-SCHEMA", "sha256": "b" * 64, "contract_id": "SYNTH-CONTRACT",
                   "primitive": "none", "expected_outcome": "accepted", "serialized_sha256": sha(expected_bytes)}
        request = {"protocol": contracts.ADAPTER_PROTOCOL, "release_revision": "d" * 40}
        response = {"protocol": contracts.ADAPTER_PROTOCOL, "release_revision": "d" * 40,
                    "results": [{"case_id": "current_valid", "outcome": "accepted",
                                 "observed_schema_sha256": "a" * 64, "observed_fixture_sha256": "b" * 64,
                                 "contract_id": "SYNTH-CONTRACT", "primitive": "none",
                                 "serialized_bytes_base64": base64.b64encode(b'{"value":8}').decode("ascii")}]}
        errors: list[dict[str, str]] = []
        contracts._validate_adapter_result(json.dumps(response).encode(), request, {"current_valid": fixture},
                                           {"SYNTH-SCHEMA": "a" * 64}, errors)
        self.assertIn("canonical_serialization_hash_mismatch", {item["code"] for item in errors})
        response["results"][0]["serialized_bytes_base64"] = base64.b64encode(expected_bytes).decode("ascii")
        response["results"][0]["primitive"] = "score"
        identity_errors: list[dict[str, str]] = []
        contracts._validate_adapter_result(json.dumps(response).encode(), request, {"current_valid": fixture},
                                           {"SYNTH-SCHEMA": "a" * 64}, identity_errors)
        self.assertIn("canonical_validator_case_identity_mismatch", {item["code"] for item in identity_errors})

    def test_test_only_fixture_validator_reads_schema_and_fixture_and_computes_result(self) -> None:
        """No canonical schema is created: these files live only in this temp test."""
        with tempfile.TemporaryDirectory(prefix="acv1-synthetic-schema-") as temp:
            root = Path(temp)
            schema_path = root / "synthetic-only-schema.json"
            fixture_path = root / "synthetic-only-fixture.json"
            schema_path.write_text(json.dumps({"type": "object", "required": ["value"],
                                               "properties": {"value": {"type": "integer"}}}), encoding="utf-8")
            fixture_path.write_text('{"value":7}', encoding="utf-8")
            schema_bytes = schema_path.read_bytes()
            fixture_bytes = fixture_path.read_bytes()
            observation = validate_synthetic_fixture(schema_path, fixture_path)
        self.assertEqual(observation["outcome"], "accepted")
        self.assertEqual(observation["schema_sha256"], sha(schema_bytes))
        self.assertEqual(observation["fixture_sha256"], sha(fixture_bytes))
        self.assertEqual(observation["serialized_output_sha256"], sha(b'{"value":7}'))
        self.assertIn("synthetic-only", "synthetic-only-schema.json")

    def test_test_only_fixture_validator_rejects_invalid_data_and_binds_observed_bytes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-synthetic-schema-negative-") as temp:
            root = Path(temp)
            schema_path = root / "synthetic-only-schema.json"
            fixture_path = root / "synthetic-only-fixture.json"
            schema_path.write_text(json.dumps({"type": "object", "required": ["value"],
                                               "properties": {"value": {"type": "integer"}}}), encoding="utf-8")
            fixture_path.write_text('{"value":true}', encoding="utf-8")
            observation = validate_synthetic_fixture(schema_path, fixture_path)
            schema_path.write_text('{"type":"object","required":["other"],"properties":{}}', encoding="utf-8")
            substituted = validate_synthetic_fixture(schema_path, fixture_path)
        self.assertEqual(observation["outcome"], "rejected")
        self.assertEqual(observation["serialized_bytes"], b'{"value":true}')
        self.assertNotEqual(observation["schema_sha256"], substituted["schema_sha256"])
        self.assertEqual(observation["fixture_sha256"], substituted["fixture_sha256"])

    def test_adapter_must_have_external_trusted_provenance(self) -> None:
        adapter = {"evidence_id": "local-self-asserted"}
        evidence = {"records": {"local-self-asserted": {"owner": "shared-contracts", "repository": "shared-contracts",
                                                         "subjects": {"contract-validator-adapter": "f" * 64}}}}
        self.assertFalse(contracts.adapter_provenance_verified(adapter, "f" * 64, None))
        self.assertTrue(contracts.adapter_provenance_verified(adapter, "f" * 64, evidence))
        self.assertFalse(contracts.adapter_provenance_verified(adapter, "e" * 64, evidence))

    def test_local_accepted_owner_url_and_revision_do_not_authenticate_release_decision(self) -> None:
        decision = {
            "status": "accepted", "owner": "shared-contracts",
            "decision_ref": "https://github.com/Jevelopers-neatHack-2026/shared-contracts/issues/1",
            "revision": "a" * 40, "subject_id": "contract-release:shared-contracts",
            "subject_sha256": "b" * 64, "evidence_id": "synthetic-untrusted-local-assertion",
        }
        errors: list[dict[str, str]] = []
        contracts.validate_release_decision(decision, owner="shared-contracts",
                                            reference=decision["decision_ref"], subject_sha256="b" * 64,
                                            evidence=None, errors=errors)
        self.assertIn("owner_approval_not_independently_verified", {item["code"] for item in errors})

    def test_untrusted_adapter_canary_is_not_executed_on_unsupported_platform(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-untrusted-adapter-") as temp:
            root = Path(temp)
            marker = root / "should-not-exist"
            adapter = root / "untrusted.py"
            adapter.write_text("from pathlib import Path\nPath(" + repr(str(marker)) + ").write_text('ran')\n", encoding="utf-8")
            request = b'{"synthetic":true}'
            with mock.patch.object(contracts.platform, "system", return_value="Darwin"), \
                    mock.patch.object(contracts.shutil, "which", return_value="/usr/bin/bwrap"):
                command, reason = contracts._sandbox_command(root, adapter, request)
            self.assertIsNone(command)
            self.assertIn("no accepted supported", reason or "")
            self.assertFalse(marker.exists())
            with mock.patch.object(contracts.platform, "system", return_value="Linux"), \
                    mock.patch.object(contracts.shutil, "which", return_value=None):
                command, reason = contracts._sandbox_command(root, adapter, request)
            self.assertIsNone(command)
            self.assertIn("bubblewrap is unavailable", reason or "")
            self.assertFalse(marker.exists())

    def test_bubblewrap_sandbox_canaries_when_supported(self) -> None:
        if platform.system() != "Linux" or not shutil.which("bwrap"):
            reason = "bubblewrap with namespaces is not available on this platform; local readiness stays blocked"
            if os.environ.get("ACV1_REQUIRE_BWRAP") == "1":
                self.fail("CI requires the real bubblewrap filesystem, socket, and child-process canaries: " + reason)
            self.skipTest(reason)
        with tempfile.TemporaryDirectory(prefix="acv1-bwrap-probe-") as temp:
            reason = contracts.probe_sandbox(Path(temp))
        self.assertIsNone(reason)

    def test_bounded_runner_limits_stdout_and_stderr(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-output-bound-") as temp:
            code = "import sys; sys.stdout.write('x'*10000); sys.stderr.write('y'*10000)"
            result = contracts.run_bounded([sys.executable, "-I", "-B", "-S", "-c", code],
                                           cwd=Path(temp), env={"PATH": os.environ.get("PATH", "")},
                                           input_bytes=b"", timeout=2, stdout_limit=512, stderr_limit=512)
        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], "execution_output_limit_exceeded")
        self.assertLessEqual(len(result["stdout"]), 512)
        self.assertLessEqual(len(result["stderr"]), 512)

    def test_timeout_includes_stdin_delivery_for_nonreading_child(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-stdin-timeout-") as temp:
            started = time.monotonic()
            result = contracts.run_bounded([sys.executable, "-I", "-B", "-S", "-c", "import time; time.sleep(2)"],
                                           cwd=Path(temp), env={"PATH": os.environ.get("PATH", "")},
                                           input_bytes=b"x" * (1024 * 1024), timeout=0.2,
                                           stdout_limit=512, stderr_limit=512)
            elapsed = time.monotonic() - started
        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], "execution_timeout")
        self.assertLess(elapsed, 1.5)

    def test_timeout_kills_descendants_in_the_process_group(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-descendant-timeout-") as temp:
            marker = Path(temp) / "late-child-write"
            child = "import pathlib,time; time.sleep(0.8); pathlib.Path(" + repr(str(marker)) + ").write_text('late')"
            parent = "import subprocess,sys,time; subprocess.Popen([sys.executable,'-I','-B','-S','-c'," + repr(child) + "]); time.sleep(5)"
            result = contracts.run_bounded([sys.executable, "-I", "-B", "-S", "-c", parent], cwd=Path(temp),
                                           env={"PATH": os.environ.get("PATH", "")}, input_bytes=b"",
                                           timeout=0.2, stdout_limit=1024, stderr_limit=1024)
            time.sleep(1.0)
            self.assertFalse(marker.exists())
        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], "execution_timeout")


class JevTargetReadinessTests(unittest.TestCase):
    def validate_synthetic(self, targets: dict, dataset: dict) -> dict:
        return jev.validate_targets(targets, dataset, test_only=True)

    def test_pending_owner_inventory_remains_blocked_with_exact_question_families(self) -> None:
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "PYTHONDONTWRITEBYTECODE": "1",
               "LC_ALL": "C.UTF-8", "PYTHONHASHSEED": "0"}
        result = subprocess.run(
            [sys.executable, str(ROOT / "tools/validate_jev_targets.py"),
             "--targets", str(ROOT / "evals/jev/promotion-targets.json"),
             "--dataset", str(ROOT / "evals/jev/dataset-manifest.json")],
            cwd=ROOT, env=env, text=True, capture_output=True, check=False, timeout=10,
        )
        self.assertEqual(result.returncode, 2)
        report = json.loads(result.stdout)
        self.assertEqual(report["status"], "blocked")
        self.assertFalse(report["ready"])
        actions = "\n".join(error["action"] for error in report["errors"])
        self.assertIn("injection_signal", actions)
        self.assertIn("brand_asset_classification", actions)
        self.assertIn("owner_evidence_index_unavailable", {item["code"] for item in report["errors"]})

    def test_complete_plan_shaped_synthetic_data_passes_structure_only_never_readiness(self) -> None:
        targets, dataset = complete_synthetic_plan()
        report = self.validate_synthetic(targets, dataset)
        self.assertEqual(report["status"], "synthetic_test_only_validation_passed")
        self.assertTrue(report["structurally_valid"])
        self.assertFalse(report["ready"])
        self.assertEqual(report["question_count"], len(jev.REQUIRED_FAMILIES))
        self.assertNotEqual(dataset["status"], "approved")
        self.assertTrue(all(review["status"] == "synthetic_test_only_unverified" for review in targets["owner_reviews"]))

    def test_local_accepted_metadata_without_trusted_evidence_never_passes(self) -> None:
        targets, dataset = complete_synthetic_plan()
        targets["status"] = "approved_for_evaluation_preparation"
        dataset["status"] = "approved"
        for review in targets["owner_reviews"]:
            repository = review["owner_repository"]
            review.update({"status": "accepted", "decision_ref": f"https://github.com/{jev.OWNER_ORG}/{repository}/issues/999999",
                           "revision": "a" * 40})
        targets["metering_stage_inventory"]["status"] = "published"
        targets["metering_stage_inventory"]["decision_ref"] = (
            f"https://github.com/{jev.OWNER_ORG}/cost-intelligence/issues/999999")
        for question in targets["questions"]:
            repository = question["definition_owner"]["repository"]
            question["definition_owner"]["issue_url"] = (
                f"https://github.com/{jev.OWNER_ORG}/{repository}/issues/999999")
        for path, approval, role, repository, subject_id, subject_hash in jev.approval_subjects(targets, dataset):
            if isinstance(approval, dict):
                approval.update({"status": "accepted", "owner_role": role, "owner_repository": repository,
                                 "decision_ref": f"https://github.com/{jev.OWNER_ORG}/{repository}/issues/999999",
                                 "revision": "b" * 40, "evidence_id": "synthetic-untrusted-local-assertion",
                                 "subject_id": subject_id, "subject_sha256": subject_hash})
        report = jev.validate_targets(targets, dataset, evidence=None)
        self.assertFalse(report["ready"])
        self.assertIn("owner_approval_not_independently_verified", {item["code"] for item in report["errors"]})

    def test_stale_approval_rejected_after_question_policy_or_threshold_changes(self) -> None:
        targets, dataset = complete_synthetic_plan()
        first = self.validate_synthetic(targets, dataset)
        self.assertTrue(first["structurally_valid"])
        targets["questions"][0]["policy_version"] = "synthetic-mutated-policy"
        targets["questions"][0]["promotion_metrics"]["false_negative_rate_max"] = 0.9
        report = self.validate_synthetic(targets, dataset)
        self.assertFalse(report["structurally_valid"])
        codes = {item["code"] for item in report["errors"]}
        self.assertIn("owner_approval_subject_hash_mismatch", codes)

    def test_definition_inventory_revision_must_match_question_revision(self) -> None:
        targets, dataset = complete_synthetic_plan()
        targets["questions"][0]["definition_owner"]["definition_revision"] = "d" * 40
        report = self.validate_synthetic(targets, dataset)
        self.assertIn("question_inventory_definition_revision_mismatch", {item["code"] for item in report["errors"]})

    def test_baselines_matching_each_other_but_not_candidate_are_rejected(self) -> None:
        targets, dataset = complete_synthetic_plan()
        q = targets["questions"][0]
        q["baselines"]["deterministic"]["input_set_sha256"] = "4" * 64
        q["baselines"]["generative"]["input_set_sha256"] = "4" * 64
        report = self.validate_synthetic(targets, dataset)
        self.assertIn("baseline_candidate_scope_mismatch", {item["code"] for item in report["errors"]})

    def test_candidate_scope_pins_are_derived_from_dataset_and_approved_budget(self) -> None:
        targets, dataset = complete_synthetic_plan()
        question = targets["questions"][0]
        question["candidate_comparison"]["input_set_sha256"] = "9" * 64
        for baseline in question["baselines"].values():
            baseline["input_set_sha256"] = "9" * 64
        targets["evaluation_budget"]["maximum_amount"] = 2
        for path, approval, role, repository, subject_id, subject_hash in jev.approval_subjects(targets, dataset):
            if isinstance(approval, dict):
                approval.update({"subject_id": subject_id, "subject_sha256": subject_hash})
        report = self.validate_synthetic(targets, dataset)
        codes = {item["code"] for item in report["errors"]}
        self.assertIn("candidate_input_set_not_bound_to_dataset", codes)
        self.assertIn("candidate_budget_plan_not_bound_to_approved_budget", codes)

    def test_matching_arbitrary_cost_lists_cannot_omit_owner_inventoried_stage(self) -> None:
        targets, dataset = complete_synthetic_plan()
        q = targets["questions"][0]
        omitted = "synthetic-inspection"
        del q["promotion_metrics"]["downstream_cost_applicability"][omitted]
        q["promotion_metrics"]["downstream_cost_stages"].remove(omitted)
        for baseline in q["baselines"].values():
            del baseline["downstream_cost_applicability"][omitted]
            baseline["downstream_cost_stages"].remove(omitted)
        report = self.validate_synthetic(targets, dataset)
        codes = {item["code"] for item in report["errors"]}
        self.assertIn("downstream_cost_inventory_mismatch", codes)
        self.assertIn("baseline_cost_applicability_incomplete", codes)

    def test_unlabeled_rows_cannot_pad_total_question_sample_minimum(self) -> None:
        targets, dataset = complete_synthetic_plan()
        q = targets["questions"][0]
        qid = q["question_id"]
        for split in jev.SPLITS:
            for index in range(6):
                sample_id = "SYNTH-UNLABELED-" + split + "-" + str(index)
                dataset["splits"][split]["examples"].append({
                    "sample_id": sample_id,
                    "sample_sha256": sha(sample_id.encode()),
                    "labels": {}, "eligible_question_ids": [], "strata": {},
                })
            dataset["splits"][split]["examples"].sort(key=lambda row: row["sample_id"])
            dataset["splits"][split]["sha256"] = jev.canonical_hash(dataset["splits"][split]["examples"])
        hashes = {split: dataset["splits"][split]["sha256"] for split in jev.SPLITS}
        targets["dataset_split_sha256"] = dict(hashes)
        for question in targets["questions"]:
            question["dataset_split_sha256"] = dict(hashes)
            question["candidate_comparison"]["dataset_split_sha256"] = dict(hashes)
            for baseline in question["baselines"].values():
                baseline["dataset_split_sha256"] = dict(hashes)
            question["coverage"]["minimum_samples_by_split"] = {split: 10 for split in jev.SPLITS}
        for path, approval, role, repository, subject_id, digest in jev.approval_subjects(targets, dataset):
            if isinstance(approval, dict):
                approval.update({"subject_id": subject_id, "subject_sha256": digest})
        report = self.validate_synthetic(targets, dataset)
        self.assertIn("dataset_sample_count_below_owner_target", {item["code"] for item in report["errors"]})

    def test_only_question_eligible_valid_labels_count_for_strata(self) -> None:
        targets, dataset = complete_synthetic_plan()
        q = targets["questions"][0]
        qid = q["question_id"]
        row = dataset["splits"]["train"]["examples"][0]
        row["eligible_question_ids"] = [other for other in row["eligible_question_ids"] if other != qid]
        dataset["splits"]["train"]["sha256"] = jev.canonical_hash(dataset["splits"]["train"]["examples"])
        # Hash pins are updated together so this test isolates question eligibility.
        digest = dataset["splits"]["train"]["sha256"]
        targets["dataset_split_sha256"]["train"] = digest
        for question in targets["questions"]:
            question["dataset_split_sha256"]["train"] = digest
            question["candidate_comparison"]["dataset_split_sha256"]["train"] = digest
            for baseline in question["baselines"].values():
                baseline["dataset_split_sha256"]["train"] = digest
        for path, approval, role, repository, subject_id, subject_hash in jev.approval_subjects(targets, dataset):
            if isinstance(approval, dict):
                approval.update({"subject_id": subject_id, "subject_sha256": subject_hash})
        report = self.validate_synthetic(targets, dataset)
        self.assertIn("dataset_sample_count_below_owner_target", {item["code"] for item in report["errors"]})

    def test_untrusted_json_types_and_huge_sample_counts_are_typed_errors(self) -> None:
        targets, dataset = complete_synthetic_plan()
        targets["questions"][0]["primitive"] = []
        targets["questions"][0]["coverage"]["minimum_samples_by_split"]["train"] = 10 ** 2000
        targets["questions"][0]["promotion_metrics"]["downstream_cost_stages"] = [[]]
        report = self.validate_synthetic(targets, dataset)
        codes = {item["code"] for item in report["errors"]}
        self.assertIn("provider_question_primitive_invalid", codes)
        self.assertIn("sample_count_invalid", codes)
        self.assertIn("downstream_cost_stages_missing", codes)
        self.assertNotIn("TypeError", json.dumps(report))

    def test_malformed_json_huge_integer_returns_sanitized_json_not_traceback(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-invalid-json-") as temp:
            root = Path(temp)
            targets, dataset = complete_synthetic_plan()
            (root / "targets.json").write_text('{"huge":' + ("9" * 300) + "}", encoding="utf-8")
            (root / "dataset.json").write_text(json.dumps(dataset), encoding="utf-8")
            result = subprocess.run([sys.executable, str(ROOT / "tools/validate_jev_targets.py"),
                                     "--targets", str(root / "targets.json"), "--dataset", str(root / "dataset.json")],
                                    cwd=ROOT, env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                                                   "PYTHONDONTWRITEBYTECODE": "1", "LC_ALL": "C.UTF-8",
                                                   "PYTHONHASHSEED": "0"},
                                    text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 2)
        report = json.loads(result.stdout)
        self.assertEqual(report["status"], "invalid_input")
        self.assertNotIn("Traceback", result.stderr)
        self.assertNotIn("999999", result.stdout)

    def test_owner_role_reviews_and_individual_inventory_ids_are_exact(self) -> None:
        targets, dataset = complete_synthetic_plan()
        targets["question_inventory"][0]["required_question_ids"].append("SYNTH-UNOWNED")
        report = self.validate_synthetic(targets, dataset)
        self.assertIn("owner_required_question_target_missing", {item["code"] for item in report["errors"]})

    def test_dependent_questions_cannot_share_parallel_batch(self) -> None:
        targets, dataset = complete_synthetic_plan()
        targets["questions"][1]["sequential_prerequisites"] = [targets["questions"][0]["question_id"]]
        targets["questions"][1]["evaluation_batch_id"] = targets["questions"][0]["evaluation_batch_id"]
        report = self.validate_synthetic(targets, dataset)
        self.assertIn("dependent_questions_in_same_parallel_batch", {item["code"] for item in report["errors"]})

    def test_shadow_evidence_never_counts_as_active(self) -> None:
        targets, dataset = complete_synthetic_plan()
        targets["shadow_only_counts_as_active"] = True
        report = self.validate_synthetic(targets, dataset)
        self.assertIn("shadow_as_active_invalid", {item["code"] for item in report["errors"]})

    def test_execution_follow_on_is_exact_and_remains_pending(self) -> None:
        targets, dataset = complete_synthetic_plan()
        targets["evaluation_follow_on"]["status"] = "complete"
        report = self.validate_synthetic(targets, dataset)
        self.assertIn("evaluation_follow_on_missing_or_claimed_complete", {item["code"] for item in report["errors"]})

    def test_noul_confidence_and_mutable_alias_are_rejected(self) -> None:
        targets, dataset = complete_synthetic_plan()
        q = targets["questions"][0]
        q["primitive"] = "noul"
        q["authority_boundary"]["noul_has_provider_confidence"] = True
        q["model_pin"]["model_id"] = "latest"
        report = self.validate_synthetic(targets, dataset)
        codes = {item["code"] for item in report["errors"]}
        self.assertIn("noul_confidence_field_invalid", codes)
        self.assertIn("model_alias_not_pinned", codes)


if __name__ == "__main__":
    unittest.main()
