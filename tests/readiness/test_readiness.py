from __future__ import annotations

import ast
import base64
import copy
from contextlib import ExitStack, redirect_stderr, redirect_stdout
import hashlib
import io
import itertools
import json
import os
import platform
import resource
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from tools import validate_jev_targets as jev  # noqa: E402
from tools import verify_contract_release as contracts  # noqa: E402
import prepare_ubuntu_bwrap_profile as profile_setup  # noqa: E402
from synthetic_contract import write_complete_synthetic_contract  # noqa: E402


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def synthetic_apt_transport() -> dict:
    mirrors = tuple(line.split(b"\t", 1)[0].decode("ascii")
                    for line in profile_setup.APT_MIRRORLIST_BYTES.splitlines())
    return {
        "transport": "github_ubuntu_runner_root_owned_mirrorlist",
        "source_layout": "noble_archive_security_split",
        "source_file_sha256": sha(SYNTHETIC_UBUNTU_SOURCES.encode("utf-8")),
        "_source_uri": profile_setup.APT_MIRROR_URI,
        "_mirror_uris": mirrors,
        "mirrorlist_sha256": sha(profile_setup.APT_MIRRORLIST_BYTES),
        "signed_by": "pinned_private_ubuntu_archive_keyring_snapshot",
        "ubuntu_archive_keyring_sha256": profile_setup.UBUNTU_ARCHIVE_KEYRING_SHA256,
        "private_source_sha256": "d" * 64,
        "private_snapshot_custody": "root_owned_read_only_outside_checkout",
        "apt_package_version": "2.8.3",
    }


def synthetic_apt_index_target(repo_uri: str | None = None, **changes: str) -> str:
    repository = (repo_uri or profile_setup.APT_MIRROR_URI).rstrip("/")
    suite = changes.get("Suite", "noble-updates")
    component = changes.get("Component", "main")
    architecture = changes.get("Architecture", "amd64")
    metakey = changes.get("MetaKey", "main/binary-amd64/Packages")
    values = {
        "Identifier": "Packages",
        "MetaKey": "main/binary-amd64/Packages",
        "Target-Of": "deb",
        "Architecture": "amd64",
        "Component": "main",
        "Suite": "noble-updates",
        "Codename": "noble",
        "Origin": "Ubuntu",
        "Label": "Ubuntu",
        "Trusted": "yes",
        "Repo-URI": repo_uri or profile_setup.APT_MIRROR_URI,
        "URI": repository + "/dists/" + suite + "/" + metakey,
    }
    values.update(changes)
    return "".join(f"{field}: {value}\n" for field, value in values.items()) + "\n"


def append_synthetic_dependencies(manifest_path: Path, count: int,
                                  payload: bytes = b"test-only") -> list[Path]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    package_root = manifest_path.parent
    dependency_root = package_root / "cache-bound-dependencies"
    dependency_root.mkdir(exist_ok=True)
    descriptors = []
    paths = []
    for index in range(count):
        relative = f"cache-bound-dependencies/{index:04d}.bin"
        target = package_root / relative
        target.write_bytes(payload)
        paths.append(target.resolve())
        descriptors.append({"path": relative, "sha256": sha(payload)})
    manifest["canonical_release"]["validator_adapter"]["dependencies"] = descriptors
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")),
                             encoding="utf-8")
    return paths


SYNTHETIC_UBUNTU_SOURCES = (
    # TEST ONLY. This models the pinned runner's mirror+file substitution over
    # the two Noble archive/security deb822 stanzas. Native setup independently
    # validates the live root-owned source bytes before trusting APT metadata.
    "Types: deb\n"
    "URIs: mirror+file:/etc/apt/apt-mirrors.txt\n"
    "Suites: noble noble-updates noble-backports\n"
    "Components: main restricted universe multiverse\n"
    "Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg\n"
    "\n"
    "Types: deb\n"
    "URIs: mirror+file:/etc/apt/apt-mirrors.txt\n"
    "Suites: noble-security\n"
    "Components: main restricted universe multiverse\n"
    "Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg\n"
)


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
    def require_native_sandbox(self) -> None:
        reason = "native Linux bubblewrap is unavailable; local result is not sandbox evidence"
        if platform.system() == "Linux" and shutil.which("bwrap"):
            return
        if os.environ.get("ACV1_REQUIRE_BWRAP") == "1":
            self.fail("CI requires real Linux bubblewrap sandbox coverage: " + reason)
        self.skipTest(reason)

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

    def test_verified_artifact_cache_bounds_unique_files_and_deduplicates_exact_pins(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-artifact-cache-files-") as temp:
            root = Path(temp).resolve()
            first = root / "first.bin"
            second = root / "second.bin"
            first.write_bytes(b"first")
            second.write_bytes(b"second")
            cache = contracts.VerifiedArtifactCache()
            errors: list[dict[str, str]] = []
            with mock.patch.object(contracts, "MAX_SEALED_SNAPSHOT_FILES", 1):
                path, digest, raw = contracts.check_artifact_bytes(
                    root, {"path": first.name, "sha256": sha(b"first")},
                    "$.first", "shared-contracts", errors, cache)
                self.assertEqual(errors, [])
                self.assertEqual((path, digest, raw), (first, sha(b"first"), b"first"))
                self.assertEqual(cache.retained_bytes, len(b"first"))

                with mock.patch.object(contracts, "read_bounded_file",
                                       side_effect=AssertionError("cached bytes must be reused")):
                    reused = contracts.check_artifact_bytes(
                        root, {"path": first.name, "sha256": sha(b"first")},
                        "$.first_again", "shared-contracts", errors, cache)
                    self.assertEqual(reused, (first, sha(b"first"), b"first"))
                    conflict = contracts.check_artifact_bytes(
                        root, {"path": first.name, "sha256": sha(b"other")},
                        "$.conflicting_pin", "shared-contracts", errors, cache)
                    self.assertEqual(conflict, (None, None, None))
                    limited = contracts.check_artifact_bytes(
                        root, {"path": second.name, "sha256": sha(b"second")},
                        "$.second", "shared-contracts", errors, cache)
                    self.assertEqual(limited, (None, None, None))

                codes = {item["code"] for item in errors}
                self.assertIn("artifact_sha256_mismatch", codes)
                self.assertIn("artifact_cache_file_limit_exceeded", codes)
                self.assertEqual(cache.retained_bytes, len(b"first"))
                snapshot_errors: list[dict[str, str]] = []
                snapshot = contracts._read_execution_artifacts(
                    root,
                    [(first, sha(b"first"), "first copy"), (first, sha(b"first"), "reused copy")],
                    snapshot_errors, cache)
                self.assertEqual(snapshot_errors, [])
                self.assertEqual(snapshot, {"first.bin": (b"first", sha(b"first"))})

    def test_verified_artifact_cache_bounds_each_read_by_remaining_bytes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-artifact-cache-bytes-") as temp:
            root = Path(temp).resolve()
            first = root / "first.bin"
            growing = root / "growing.bin"
            first.write_bytes(b"four")
            growing.write_bytes(b"xy")
            cache: dict[Path, tuple[bytes, str]] = {}
            errors: list[dict[str, str]] = []
            original_read = contracts.read_bounded_file

            def grow_then_read(path: Path, limit: int = contracts.MAX_ARTIFACT_BYTES) -> bytes:
                if Path(path) == growing:
                    self.assertEqual(limit, 2)
                    growing.write_bytes(b"xyz")
                return original_read(path, limit)

            with mock.patch.object(contracts, "MAX_SEALED_SNAPSHOT_BYTES", 6), \
                    mock.patch.object(contracts, "read_bounded_file", side_effect=grow_then_read):
                accepted = contracts.check_artifact_bytes(
                    root, {"path": first.name, "sha256": sha(b"four")},
                    "$.first", "shared-contracts", errors, cache)
                self.assertEqual(accepted, (first, sha(b"four"), b"four"))
                rejected = contracts.check_artifact_bytes(
                    root, {"path": growing.name, "sha256": sha(b"xyz")},
                    "$.growing", "shared-contracts", errors, cache)
            self.assertEqual(rejected, (None, None, None))
            self.assertIn("artifact_cache_byte_limit_exceeded", {item["code"] for item in errors})
            self.assertEqual(len(cache), 1)

    def test_assembled_release_cache_rejects_file_limit_with_missing_owner_evidence(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-assembled-cache-files-") as temp:
            root = Path(temp).resolve()
            manifest_path, _evidence_path, _evidence_sha = write_complete_synthetic_contract(root)
            append_synthetic_dependencies(manifest_path, 520, b"x")
            result = self.run_contract("--manifest", str(manifest_path), "--offline")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        report = json.loads(result.stdout)
        codes = {item["code"] for item in report["errors"]}
        self.assertIn("artifact_cache_file_limit_exceeded", codes)
        self.assertIn("owner_evidence_index_unavailable", codes)
        self.assertFalse(report["ready"])
        self.assertIsNot(report.get("owner_evidence_verified"), True)
        self.assertNotIn("execution_sandbox_unavailable", codes)

    def test_assembled_release_byte_limit_stops_reads_after_capacity_is_filled(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-assembled-cache-bytes-") as temp:
            root = Path(temp).resolve()
            manifest_path, _evidence_path, _evidence_sha = write_complete_synthetic_contract(root)
            dependency_paths = append_synthetic_dependencies(
                manifest_path, 2, b"1234")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            release = manifest["canonical_release"]
            package_root = manifest_path.parent
            baseline_descriptors = [*release["schema_artifacts"], release["field_inventory"],
                                    release["validator_adapter"]]
            baseline_bytes = sum((package_root / row["path"]).stat().st_size
                                 for row in baseline_descriptors)
            original_read = contracts.read_bounded_file
            reads: list[Path] = []

            def track_reads(path: Path, limit: int = contracts.MAX_ARTIFACT_BYTES) -> bytes:
                reads.append(Path(path).resolve())
                return original_read(path, limit)

            errors: list[dict[str, str]] = []
            with mock.patch.object(contracts, "MAX_SEALED_SNAPSHOT_BYTES", baseline_bytes + 4), \
                    mock.patch.object(contracts, "read_bounded_file", side_effect=track_reads):
                output = contracts.validate_and_run(manifest, manifest_path, errors, evidence=None)
            codes = {item["code"] for item in errors}
            self.assertIsNone(output)
            self.assertIn("artifact_cache_byte_limit_exceeded", codes)
            self.assertIn(dependency_paths[0], reads)
            self.assertNotIn(dependency_paths[1], reads)
            self.assertIn("owner_evidence_index_unavailable", codes)

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

    def test_schema_subset_traverses_array_applicators_and_rejects_unsupported_keywords(self) -> None:
        schema = {
            "$schema": contracts.SUPPORTED_JSON_SCHEMA_DIALECT,
            "$defs": {
                "payload": {
                    "type": "object",
                    "properties": {"hidden": {"type": "integer"}},
                    "additionalProperties": False,
                },
            },
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "prefixItems": [{"$ref": "#/$defs/payload"}],
                    "items": {"$ref": "#/$defs/payload"},
                    "contains": {"$ref": "#/$defs/payload"},
                },
            },
            "additionalProperties": False,
        }
        self.assertEqual(contracts._schema_field_paths(schema), {
            "/items", "/items/0/hidden", "/items/*/hidden",
        })

        unsupported = [
            {**schema, "$schema": "http://json-schema.org/draft-07/schema#"},
            {**schema, "dependencies": {"items": {"properties": {"legacy": {"type": "string"}}}}},
            {
                "$schema": contracts.SUPPORTED_JSON_SCHEMA_DIALECT,
                "type": "object",
                "properties": {"known": {"type": "integer"}},
                "additionalProperties": False,
                "unevaluatedProperties": {"properties": {"hidden": {"type": "string"}}},
            },
            {
                "$schema": contracts.SUPPORTED_JSON_SCHEMA_DIALECT,
                "type": "object",
                "properties": {"known": {"type": "integer"}},
                "additionalProperties": False,
                "$dynamicRef": "#/$defs/payload",
            },
        ]
        for candidate in unsupported:
            with self.subTest(candidate=sorted(candidate)):
                with self.assertRaises(contracts.UnsupportedSchema):
                    contracts._schema_field_paths(candidate)

    def test_schema_enumeration_rejects_implicit_open_containers_directly_and_in_cli(self) -> None:
        def root_schema(value_schema: Any, *, definitions: dict[str, Any] | None = None) -> dict[str, Any]:
            result: dict[str, Any] = {
                "$schema": contracts.SUPPORTED_JSON_SCHEMA_DIALECT,
                "type": "object",
                "required": ["decision", "value"],
                "properties": {"decision": {"type": "string"}, "value": value_schema},
                "additionalProperties": False,
            }
            if definitions is not None:
                result["$defs"] = definitions
            return result

        candidates = [
            ("empty_schema", root_schema({})),
            ("boolean_true", root_schema(True)),
            ("unclosed_object", root_schema({"type": "object"})),
            ("array_without_items", root_schema({"type": "array"})),
            ("empty_items", root_schema({"type": "array", "items": {}})),
            ("prefix_items_open_tail", root_schema({
                "type": "array", "prefixItems": [{"type": "string"}],
            })),
            ("reference_to_unclosed_object", root_schema(
                {"$ref": "#/$defs/open_object"},
                definitions={"open_object": {"type": "object"}},
            )),
            ("composition_with_unclosed_object", root_schema({
                "anyOf": [
                    {"type": "string"},
                    {"type": "object"},
                ],
            })),
            ("all_of_with_unclosed_object_branch", root_schema({
                "allOf": [
                    {"type": "object", "additionalProperties": False},
                    {"type": "object"},
                ],
            })),
            ("empty_all_of", root_schema({"allOf": []})),
        ]

        for name, schema in candidates:
            with self.subTest(schema=name, path="direct"):
                with self.assertRaises(contracts.UnsupportedSchema):
                    contracts._schema_field_paths(schema)

            with self.subTest(schema=name, path="assembled_cli"), \
                    tempfile.TemporaryDirectory(prefix="acv1-open-schema-cli-") as temporary:
                manifest_path, evidence_path, evidence_sha = write_complete_synthetic_contract(
                    Path(temporary), schema_overrides={"none": schema},
                    adapter_source=b"raise SystemExit(77)  # TEST ONLY: rejected schema must not execute\n",
                )
                result = subprocess.run(
                    [sys.executable, str(ROOT / "tools/verify_contract_release.py"),
                     "--manifest", str(manifest_path), "--offline", "--test-only-synthetic",
                     "--owner-evidence-index", str(evidence_path), "--owner-evidence-sha256", evidence_sha],
                    cwd=ROOT,
                    env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "PYTHONDONTWRITEBYTECODE": "1",
                         "LC_ALL": "C.UTF-8", "PYTHONHASHSEED": "0"},
                    text=True, capture_output=True, check=False, timeout=10,
                )
                self.assertEqual(result.returncode, 2, result.stdout[:2000])
                report = json.loads(result.stdout)
                codes = {item["code"] for item in report["errors"]}
                self.assertEqual(report["status"], "blocked")
                self.assertFalse(report["ready"])
                self.assertIsNot(report.get("owner_evidence_verified"), True)
                self.assertIn("canonical_schema_unsupported", codes)
                self.assertIn("schema_inventory_source_unavailable", codes)
                self.assertNotIn("execution_sandbox_unavailable", codes)
                self.assertNotIn("adapter_nonzero_exit", codes)

    def test_schema_enumeration_keeps_closed_object_array_reference_and_composition_controls(self) -> None:
        schema = {
            "$schema": contracts.SUPPORTED_JSON_SCHEMA_DIALECT,
            "$defs": {
                "record": {
                    "type": "object",
                    "properties": {"id": {"type": "string"}},
                    "additionalProperties": False,
                },
            },
            "type": "object",
            "properties": {
                "reference": {"$ref": "#/$defs/record"},
                "array": {"type": "array", "prefixItems": [{"type": "integer"}], "items": False},
                "composition": {
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {"left": {"type": "string"}, "right": {"type": "integer"}},
                            "additionalProperties": False,
                        },
                        {
                            "type": "object",
                            "properties": {"left": {"type": "string"}, "right": {"type": "integer"}},
                            "additionalProperties": False,
                        },
                    ],
                },
                "choice": {"anyOf": [{"type": "string"}, {"$ref": "#/$defs/record"}]},
                "closed_by_unevaluated": {
                    "type": "object",
                    "properties": {"key": {"type": "boolean"}},
                    "unevaluatedProperties": False,
                },
                "closed_tuple": {
                    "type": "array",
                    "prefixItems": [{"type": "integer"}],
                    "unevaluatedItems": False,
                },
            },
            "additionalProperties": False,
        }
        self.assertEqual(contracts._schema_field_paths(schema), {
            "/reference", "/reference/id", "/array",
            "/composition", "/composition/left", "/composition/right",
            "/choice", "/choice/id", "/closed_by_unevaluated", "/closed_by_unevaluated/key",
            "/closed_tuple",
        })

    def test_schema_walker_bounds_fields_and_reference_expansion(self) -> None:
        definitions = {"n0": {"type": "object", "properties": {"leaf": {"type": "integer"}},
                              "additionalProperties": False}}
        for level in range(1, 31):
            definitions[f"n{level}"] = {
                "type": "object",
                "properties": {"left": {"$ref": f"#/$defs/n{level - 1}"},
                               "right": {"$ref": f"#/$defs/n{level - 1}"}},
                "additionalProperties": False,
            }
        dag = {"$schema": contracts.SUPPORTED_JSON_SCHEMA_DIALECT,
               "$defs": definitions, "$ref": "#/$defs/n30"}
        with self.assertRaises(contracts.SchemaTraversalLimit):
            contracts._schema_field_paths(dag)

        recursive = {
            "$schema": contracts.SUPPORTED_JSON_SCHEMA_DIALECT,
            "type": "object",
            "properties": {"child": {"$ref": "#"}},
            "additionalProperties": False,
        }
        with self.assertRaises(contracts.SchemaTraversalLimit):
            contracts._schema_field_paths(recursive)

    def test_schema_walker_applies_field_path_and_aggregate_byte_limits(self) -> None:
        two_fields = {
            "$schema": contracts.SUPPORTED_JSON_SCHEMA_DIALECT,
            "type": "object",
            "properties": {"a": {"type": "integer"}, "b": {"type": "string"}},
            "additionalProperties": False,
        }
        with mock.patch.object(contracts, "MAX_SCHEMA_FIELDS", 1):
            with self.assertRaises(contracts.SchemaTraversalLimit):
                contracts._schema_field_paths(two_fields)
        with mock.patch.object(contracts, "MAX_SCHEMA_AGGREGATE_PATH_BYTES", 3):
            with self.assertRaises(contracts.SchemaTraversalLimit):
                contracts._schema_field_paths(two_fields)

        nested = {
            "$schema": contracts.SUPPORTED_JSON_SCHEMA_DIALECT,
            "type": "object",
            "properties": {"a": {
                "type": "object",
                "properties": {"b": {"type": "integer"}},
                "additionalProperties": False,
            }},
            "additionalProperties": False,
        }
        with mock.patch.object(contracts, "MAX_SCHEMA_PATH_BYTES", 2):
            with self.assertRaises(contracts.SchemaTraversalLimit):
                contracts._schema_field_paths(nested)

        overescaped = {
            "$schema": contracts.SUPPORTED_JSON_SCHEMA_DIALECT,
            "type": "object",
            "properties": {"~" * 3000: {"type": "integer"}},
            "additionalProperties": False,
        }
        with self.assertRaises(contracts.SchemaTraversalLimit):
            contracts._schema_field_paths(overescaped)

        referenced = {
            "$schema": contracts.SUPPORTED_JSON_SCHEMA_DIALECT,
            "$defs": {"item": {
                "type": "object", "properties": {"value": {"type": "integer"}},
                "additionalProperties": False,
            }},
            "$ref": "#/$defs/item",
        }
        with mock.patch.object(contracts, "MAX_SCHEMA_REFERENCE_EXPANSIONS", 0):
            with self.assertRaises(contracts.SchemaTraversalLimit):
                contracts._schema_field_paths(referenced)

    def test_schema_traversal_limits_return_typed_cli_blocker_under_resource_guard(self) -> None:
        definitions = {"n0": {"type": "object", "properties": {"leaf": {"type": "integer"}},
                              "additionalProperties": False}}
        for level in range(1, 31):
            definitions[f"n{level}"] = {
                "type": "object",
                "properties": {"left": {"$ref": f"#/$defs/n{level - 1}"},
                               "right": {"$ref": f"#/$defs/n{level - 1}"}},
                "additionalProperties": False,
            }
        candidates = {
            "branching-dag": {"$schema": contracts.SUPPORTED_JSON_SCHEMA_DIALECT,
                              "$defs": definitions, "$ref": "#/$defs/n30"},
            "recursive-ref": {
                "$schema": contracts.SUPPORTED_JSON_SCHEMA_DIALECT,
                "type": "object",
                "properties": {"child": {"$ref": "#"}},
                "additionalProperties": False,
            },
        }

        def limit_child_resources() -> None:
            resource.setrlimit(resource.RLIMIT_CPU, (2, 2))
            if platform.system() == "Linux":
                memory_limit = 256 * 1024 * 1024
                resource.setrlimit(resource.RLIMIT_AS, (memory_limit, memory_limit))

        for name, schema in candidates.items():
            with self.subTest(schema=name), tempfile.TemporaryDirectory(prefix="acv1-schema-cli-bound-") as temp:
                manifest_path, evidence_path, evidence_sha = write_complete_synthetic_contract(
                    Path(temp), schema_overrides={"none": schema},
                    adapter_source=b"raise SystemExit(77)  # TEST ONLY: must not execute\n")
                started = time.monotonic()
                result = subprocess.run(
                    [sys.executable, str(ROOT / "tools/verify_contract_release.py"),
                     "--manifest", str(manifest_path), "--offline", "--test-only-synthetic",
                     "--owner-evidence-index", str(evidence_path), "--owner-evidence-sha256", evidence_sha],
                    cwd=ROOT,
                    env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "PYTHONDONTWRITEBYTECODE": "1",
                         "LC_ALL": "C.UTF-8", "PYTHONHASHSEED": "0"},
                    text=True, capture_output=True, check=False, timeout=5, preexec_fn=limit_child_resources,
                )
                self.assertLess(time.monotonic() - started, 5)
                self.assertEqual(result.returncode, 2, result.stdout[:2000])
                report = json.loads(result.stdout)
                self.assertEqual(report["status"], "blocked")
                self.assertFalse(report["ready"])
                self.assertIn("canonical_schema_traversal_limit",
                              {item["code"] for item in report["errors"]})
                self.assertNotIn("adapter_nonzero_exit", {item["code"] for item in report["errors"]})

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

    def test_assembled_release_rejects_uninventoried_schema_missing_tuple_and_missing_field_case(self) -> None:
        def assembled(root: Path) -> dict:
            manifest_path, _, _ = write_complete_synthetic_contract(root)
            return json.loads(manifest_path.read_text(encoding="utf-8"))

        def inspect(root: Path, manifest: dict) -> set[str]:
            errors: list[dict[str, str]] = []
            contracts.validate_and_run(manifest, root / "package/release.json", errors, evidence=None)
            return {item["code"] for item in errors}

        with tempfile.TemporaryDirectory(prefix="acv1-uninventoried-schema-") as temp:
            root = Path(temp)
            manifest = assembled(root)
            schema_id = "SYNTH-score"
            inventory_path = root / "package/field-inventory.json"
            inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
            inventory["schemas"] = [row for row in inventory["schemas"] if row["schema_id"] != schema_id]
            mutated_raw = json.dumps(inventory, sort_keys=True, separators=(",", ":")).encode()
            (root / "package/field-inventory-mutated.json").write_bytes(mutated_raw)
            manifest["canonical_release"]["field_inventory"].update(
                path="field-inventory-mutated.json", sha256=sha(mutated_raw))
            manifest["field_crosswalk"] = [row for row in manifest["field_crosswalk"] if row["schema_id"] != schema_id]
            manifest["fixtures"] = [row for row in manifest["fixtures"] if row["schema_id"] != schema_id]
            manifest["fixture_crosswalk"] = [row for row in manifest["fixture_crosswalk"] if row["schema_id"] != schema_id]
            self.assertIn("canonical_schema_inventory_tuple_missing", inspect(root, manifest))

        with tempfile.TemporaryDirectory(prefix="acv1-unmapped-schema-tuple-") as temp:
            root = Path(temp)
            manifest = assembled(root)
            inventory_path = root / "package/field-inventory.json"
            inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
            untested_tuple = copy.deepcopy(inventory["schemas"][0])
            untested_tuple["contract_id"] = "SYNTH-CONTRACT-WITHOUT-FIXTURE"
            inventory["schemas"].append(untested_tuple)
            mutated_raw = json.dumps(inventory, sort_keys=True, separators=(",", ":")).encode()
            (root / "package/field-inventory-mutated.json").write_bytes(mutated_raw)
            manifest["canonical_release"]["field_inventory"].update(
                path="field-inventory-mutated.json", sha256=sha(mutated_raw))
            self.assertIn("schema_contract_primitive_fixture_missing", inspect(root, manifest))

        with tempfile.TemporaryDirectory(prefix="acv1-missing-schema-field-case-") as temp:
            root = Path(temp)
            manifest = assembled(root)
            inventory_path = root / "package/field-inventory.json"
            inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
            for row in inventory["schemas"]:
                if row["schema_id"] == "SYNTH-none":
                    row["field_paths"] = ["/decision"]
            mutated_raw = json.dumps(inventory, sort_keys=True, separators=(",", ":")).encode()
            (root / "package/field-inventory-mutated.json").write_bytes(mutated_raw)
            manifest["canonical_release"]["field_inventory"].update(
                path="field-inventory-mutated.json", sha256=sha(mutated_raw))
            manifest["field_crosswalk"] = [
                row for row in manifest["field_crosswalk"]
                if not (row["schema_id"] == "SYNTH-none" and row["field_path"] == "/value")
            ]
            for row in manifest["field_crosswalk"]:
                row["coverage_tags"] = ["synthetic-tag-does-not-cover-fields"]
            manifest["fixture_crosswalk"] = [
                row for row in manifest["fixture_crosswalk"] if row["case_id"] != "current_valid"
            ]
            codes = inspect(root, manifest)
            self.assertIn("schema_inventory_field_set_mismatch", codes)
            self.assertIn("fixture_crosswalk_case_missing", codes)

    def test_verified_artifact_bytes_are_the_same_bytes_parsed_and_sealed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-verified-artifact-bytes-") as temp:
            root = Path(temp).resolve()
            target = root / "schema.json"
            original = b'{"type":"object","properties":{"decision":{"type":"string"}}}'
            replacement = b'{"type":"object","properties":{"replacement":{"type":"string"}}}'
            target.write_bytes(original)
            captured: dict[Path, tuple[bytes, str]] = {}
            errors: list[dict[str, str]] = []
            path, digest, raw = contracts.check_artifact_bytes(
                root, {"path": target.name, "sha256": sha(original)}, "$.schema", "shared-contracts",
                errors, captured)
            self.assertEqual(errors, [])
            self.assertEqual(raw, original)
            target.write_bytes(replacement)
            execution = contracts._read_execution_artifacts(
                root, [(path, digest, "runtime schema")], errors, captured)
            self.assertEqual(errors, [])
            self.assertEqual(execution, {"schema.json": (original, sha(original))})
            snapshot, reason = contracts._make_sealed_snapshot(execution or {})
            if not hasattr(os, "memfd_create"):
                self.assertIsNone(snapshot)
                self.assertIn("lacks sealed in-memory file support", reason or "")
                return
            self.assertIsNone(reason)
            self.assertIsNotNone(snapshot)
            assert snapshot is not None
            descriptor = snapshot.files["schema.json"]
            os.lseek(descriptor, 0, os.SEEK_SET)
            self.assertEqual(os.read(descriptor, len(original) + 1), original)
            snapshot.close()

    def test_assembled_schema_inventory_and_fixture_swaps_cannot_change_parsed_bytes(self) -> None:
        schema_with_omitted = {
            "$schema": contracts.SUPPORTED_JSON_SCHEMA_DIALECT,
            "type": "object",
            "required": ["decision", "value"],
            "properties": {"decision": {"type": "string"}, "value": {"type": "integer"},
                           "omitted": {"type": "integer"}},
            "additionalProperties": False,
        }

        for artifact_kind in ("schema", "inventory", "fixture"):
            for substitution_kind in ("rename", "symlink"):
                with self.subTest(artifact_kind=artifact_kind, substitution_kind=substitution_kind), \
                        tempfile.TemporaryDirectory(prefix="acv1-artifact-parse-race-") as temp:
                    root = Path(temp).resolve()
                    manifest_path, evidence_path, evidence_sha = write_complete_synthetic_contract(
                        root, schema_overrides={"none": schema_with_omitted})
                    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                    evidence_errors: list[dict[str, str]] = []
                    evidence = contracts.load_trusted_evidence_index(
                        str(evidence_path), evidence_sha, ROOT, evidence_errors, allow_test_only=True)
                    self.assertEqual(evidence_errors, [])
                    self.assertIsNotNone(evidence)

                    if artifact_kind == "schema":
                        target = root / "package/schemas/none.json"
                        pinned = target.read_bytes()
                        substitute_schema = copy.deepcopy(schema_with_omitted)
                        del substitute_schema["properties"]["omitted"]
                        substitute = json.dumps(substitute_schema, sort_keys=True, separators=(",", ":")).encode()
                    elif artifact_kind == "inventory":
                        target = root / "package/field-inventory.json"
                        pinned = target.read_bytes()
                        changed_inventory = json.loads(pinned.decode("utf-8"))
                        none_row = next(row for row in changed_inventory["schemas"]
                                        if row["schema_id"] == "SYNTH-none")
                        none_row["field_paths"].append("/omitted")
                        substitute = json.dumps(changed_inventory, sort_keys=True, separators=(",", ":")).encode()
                    else:
                        target = root / "package/fixtures/current_valid.json"
                        pinned = target.read_bytes()
                        substitute = b"not-json"
                    backup = target.with_name(target.name + ".pinned-backup")
                    alternate = target.with_name(target.name + ".substitute")
                    original_read = contracts.read_bounded_file
                    swapped = False

                    def swap_after_hash(read_path: Path, limit: int = contracts.MAX_ARTIFACT_BYTES) -> bytes:
                        nonlocal swapped
                        captured_bytes = original_read(read_path, limit)
                        if Path(read_path) == target and not swapped:
                            os.replace(target, backup)
                            alternate.write_bytes(substitute)
                            if substitution_kind == "symlink":
                                target.symlink_to(alternate.name)
                            else:
                                os.replace(alternate, target)
                            swapped = True
                        return captured_bytes

                    errors: list[dict[str, str]] = []
                    try:
                        with mock.patch.object(contracts, "read_bounded_file", side_effect=swap_after_hash), \
                                mock.patch.object(contracts, "_sandbox_prerequisite_error",
                                                  return_value="synthetic test stop before execution") as prereq, \
                                mock.patch.object(contracts, "probe_sandbox", return_value=None) as sandbox, \
                                mock.patch.object(contracts, "run_bounded", return_value={"ok": True}) as execute:
                            contracts.validate_and_run(manifest, manifest_path, errors, evidence)
                        codes = {item["code"] for item in errors}
                        self.assertTrue(swapped)
                        if artifact_kind == "fixture":
                            self.assertNotIn("fixture_json_invalid", codes)
                            self.assertNotIn("fixture_content_empty", codes)
                        else:
                            self.assertIn("schema_inventory_field_set_mismatch", codes)
                            self.assertFalse(prereq.called)
                        self.assertFalse(sandbox.called)
                        self.assertFalse(execute.called)
                    finally:
                        if target.is_symlink() or target.exists():
                            target.unlink()
                        if backup.exists():
                            os.replace(backup, target)
                        if alternate.exists():
                            alternate.unlink()
                    self.assertEqual(target.read_bytes(), pinned)

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

    def test_synthetic_evidence_index_is_rejected_by_owner_readiness_mode(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-synthetic-evidence-index-") as temp:
            _, evidence_path, evidence_sha = write_complete_synthetic_contract(Path(temp))
            errors: list[dict[str, str]] = []
            evidence = contracts.load_trusted_evidence_index(
                str(evidence_path), evidence_sha, ROOT, errors)
        self.assertIsNone(evidence)
        self.assertIn("owner_evidence_index_classification_invalid", {item["code"] for item in errors})

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
        self.require_native_sandbox()
        with tempfile.TemporaryDirectory(prefix="acv1-bwrap-probe-") as temp:
            reason = contracts.probe_sandbox(Path(temp))
        self.assertIsNone(reason)

    def test_bubblewrap_shadow_write_is_private_and_host_canary_unchanged(self) -> None:
        self.require_native_sandbox()
        with tempfile.TemporaryDirectory(prefix="acv1-bwrap-shadow-write-") as temp:
            root = Path(temp)
            outside = root / "hidden-host-parent" / "host-canary.txt"
            outside.parent.mkdir()
            outside.write_bytes(b"original synthetic host canary")
            reason = contracts._run_sandbox_canary(root, outside)
            self.assertIsNone(reason)
            self.assertEqual(outside.read_bytes(), b"original synthetic host canary")

    def test_child_filesystem_canary_distinguishes_exposed_host_content_from_private_shadow(self) -> None:
        canary_function = next(node for node in ast.parse(
            (ROOT / "tools/verify_contract_release.py").read_text(encoding="utf-8")).body
                               if isinstance(node, ast.FunctionDef) and node.name == "_run_sandbox_canary")
        child_code = next(ast.literal_eval(node.value) for node in canary_function.body
                          if isinstance(node, ast.Assign)
                          and any(isinstance(target, ast.Name) and target.id == "child_code"
                                  for target in node.targets))
        child_tree = ast.parse(child_code)
        read_branches = [node for node in child_tree.body if isinstance(node, ast.Try)][:2]
        self.assertEqual(len(read_branches), 2)
        code = compile(ast.Module(body=read_branches, type_ignores=[]), "<synthetic-child-filesystem-canary>", "exec")
        with tempfile.TemporaryDirectory(prefix="acv1-child-path-distinction-") as temp:
            root = Path(temp)
            host_path = root / "host-canary.txt"
            shadow_path = root / "private-shadow.txt"
            host_path.write_bytes(b"synthetic host-only bytes")
            shadow_path.write_bytes(b"sandbox-shadow-write")
            exposed = {"outside": str(host_path), "shadow": str(shadow_path)}
            exec(code, exposed)
            self.assertFalse(exposed["outside_blocked"])
            self.assertTrue(exposed["private_shadow_visible"])
            exposed = {"outside": str(root / "hidden-host-canary.txt"), "shadow": str(shadow_path)}
            exec(code, exposed)
            self.assertTrue(exposed["outside_blocked"])
            self.assertTrue(exposed["private_shadow_visible"])
            self.assertEqual(host_path.read_bytes(), b"synthetic host-only bytes")

    def test_sandbox_failure_diagnostics_are_classified_bounded_and_path_redacted(self) -> None:
        stderr = (b"bwrap: Creating new namespace failed: Operation not permitted "
                  b"at /home/runner/work/agent-harness/host-canary.txt")
        with tempfile.TemporaryDirectory(prefix="acv1-sandbox-diagnostic-") as temp, \
                mock.patch.object(contracts.shutil, "which", return_value="/usr/bin/bwrap"), \
                mock.patch.object(contracts.sys, "executable", "/usr/bin/python3"), \
                mock.patch.object(contracts.os, "stat", return_value=mock.Mock(st_ino=42)), \
                mock.patch.object(contracts, "run_bounded", return_value={
                    "ok": False, "code": "adapter_nonzero_exit", "returncode": 1,
                    "stdout": b"", "stderr": stderr,
                }) as mocked_run:
            reason = contracts._run_sandbox_canary(Path(temp), Path(temp) / "host-canary.txt")
            command = mocked_run.call_args.args[0]
        parent_code = command[command.index("-c") + 1]
        child_code = command[-3]
        compile(parent_code, "<synthetic-canary-parent>", "exec")
        compile(child_code, "<synthetic-canary-child>", "exec")
        nested_tree = ast.parse(parent_code)
        nested_code = next(ast.literal_eval(node.value) for node in ast.walk(nested_tree)
                           if isinstance(node, ast.Assign)
                           and any(isinstance(target, ast.Name) and target.id == "nested_code"
                                   for target in node.targets))
        shell_code = next(ast.literal_eval(node.value) for node in ast.walk(nested_tree)
                          if isinstance(node, ast.Assign)
                          and any(isinstance(target, ast.Name) and target.id == "shell_code"
                                  for target in node.targets))
        compile(nested_code, "<synthetic-canary-nested-userns>", "exec")
        shell_check = subprocess.run(["/bin/sh", "-n"], input=shell_code.encode("utf-8"),
                                     capture_output=True, check=False)
        self.assertEqual(shell_check.returncode, 0, shell_check.stderr.decode("utf-8", "replace"))
        self.assertIn("/proc/self/attr/current", parent_code)
        self.assertIn("--unshare-all", command)
        self.assertIn("--cap-drop", command)
        self.assertIn("nested_userns_setup_complete", parent_code)
        self.assertIn("nested_mount_attempted", parent_code)
        self.assertIn("nested_mount_namespace_denied", parent_code)
        self.assertNotIn("nested_cap_sys_admin_available", parent_code)
        self.assertNotIn("'--mount'", parent_code)
        self.assertIn("libc.unshare(0x00020000)", parent_code)
        self.assertIn("private_shadow_visible", child_code)
        self.assertIn("loopback: failed rtm_newaddr: operation not permitted", parent_code)
        self.assertNotIn("--share-net", command)
        self.assertEqual(mocked_run.call_args.kwargs["timeout"], 20)
        self.assertIsNotNone(reason)
        self.assertIn("namespace_operation_not_permitted", reason or "")
        self.assertIn("<path>", reason or "")
        self.assertNotIn("/home/runner", reason or "")
        self.assertLessEqual(len(reason or ""), 640)
        loopback_category, loopback_excerpt = contracts._sandbox_stderr_summary(
            b"bwrap: loopback: Failed RTM_NEWADDR: Operation not permitted")
        self.assertEqual(loopback_category, "network_namespace_loopback_setup_denied")
        self.assertIn("RTM_NEWADDR", loopback_excerpt)

    def test_native_profile_setup_accepts_only_exact_signed_ubuntu_apt_record(self) -> None:
        record = (
            "Package: apparmor-profiles\n"
            "Version: 4.0.1really4.0.1-0ubuntu0.24.04.8\n"
            "Architecture: all\n"
            "Filename: pool/main/a/apparmor/apparmor-profiles_4.0.1really4.0.1-0ubuntu0.24.04.8_all.deb\n"
            "SHA256: 4e7d728322f899a7a06e71bedd4f4bd1f20c21f0b3361120f34cf5c0feec849e\n"
            "APT-Sources: https://archive.ubuntu.com/ubuntu noble-updates/main amd64 Packages\n"
        )
        parsed = profile_setup.validate_apt_record(
            record, profile_setup.PROFILE_PACKAGE, profile_setup.PROFILE_VERSION,
            profile_setup.PROFILE_DEB_SHA256, source_text=record)
        self.assertEqual(parsed["APT-Sources"],
                         "https://archive.ubuntu.com/ubuntu noble-updates/main amd64 Packages")

    def test_native_profile_setup_accepts_only_exact_runner_mirror_source_config(self) -> None:
        source_bytes = SYNTHETIC_UBUNTU_SOURCES.replace(
            "Suites:", "# runner source comment\nSuites:").encode("utf-8")
        evidence = profile_setup._validate_runner_mirror_configuration(
            source_bytes, profile_setup.APT_MIRRORLIST_BYTES)
        self.assertEqual(evidence["transport"], "github_ubuntu_runner_root_owned_mirrorlist")
        self.assertEqual(evidence["source_layout"], "noble_archive_security_split")
        self.assertEqual(evidence["source_record_count"], 2)
        self.assertEqual(evidence["mirror_source_stanza_count"], 2)
        self.assertEqual(evidence["source_file_sha256"], sha(source_bytes))
        self.assertEqual(evidence["mirror_uri_count"], 3)
        self.assertEqual(evidence["mirror_hosts"], [
            "azure.archive.ubuntu.com", "archive.ubuntu.com", "security.ubuntu.com"])
        self.assertEqual(evidence["mirrorlist_sha256"], sha(profile_setup.APT_MIRRORLIST_BYTES))
        self.assertEqual(evidence["_source_uri"], profile_setup.APT_MIRROR_URI)
        self.assertNotIn("username", json.dumps(evidence))

    def test_native_profile_setup_accepts_exact_component_and_archive_suite_sets_in_any_order(self) -> None:
        expected_components = profile_setup.APT_RUNNER_COMPONENTS
        for permutation in itertools.permutations(expected_components):
            with self.subTest(components=permutation):
                source = SYNTHETIC_UBUNTU_SOURCES.replace(
                    "Components: " + " ".join(expected_components),
                    "Components: " + " ".join(permutation),
                ).encode("utf-8")
                evidence = profile_setup._validate_runner_mirror_configuration(
                    source, profile_setup.APT_MIRRORLIST_BYTES)
                self.assertEqual(evidence["source_layout"], "noble_archive_security_split")
                records = profile_setup._parse_deb822_records(
                    source.decode("utf-8"),
                    failure_code="synthetic_source_invalid",
                    max_bytes=64 * 1024,
                )
                diagnostics = profile_setup._runner_source_layout_diagnostics(records, source)
                self.assertEqual(diagnostics["exact_component_set_stanza_count"], 2)
                expected_order_class = (
                    "canonical" if permutation == expected_components else "permutation")
                self.assertEqual(
                    diagnostics["component_order_class_counts"][expected_order_class], 2)

        for permutation in itertools.permutations(profile_setup.APT_RUNNER_ARCHIVE_SUITES):
            with self.subTest(archive_suites=permutation):
                source = SYNTHETIC_UBUNTU_SOURCES.replace(
                    "Suites: " + " ".join(profile_setup.APT_RUNNER_ARCHIVE_SUITES),
                    "Suites: " + " ".join(permutation),
                ).encode("utf-8")
                evidence = profile_setup._validate_runner_mirror_configuration(
                    source, profile_setup.APT_MIRRORLIST_BYTES)
                self.assertEqual(evidence["source_layout"], "noble_archive_security_split")

    def test_native_profile_setup_component_diagnostics_are_allowlisted_and_distinguish_bad_sets(self) -> None:
        cases = (
            ("main main universe multiverse", 0, 2),
            ("main restricted universe", 0, 0),
            ("main restricted universe multiverse private-component", 2, 0),
        )
        for components, expected_unknown, expected_duplicates in cases:
            with self.subTest(components=components), self.assertRaises(profile_setup.SetupFailure) as raised:
                source = SYNTHETIC_UBUNTU_SOURCES.replace(
                    "Components: main restricted universe multiverse",
                    "Components: " + components,
                ).encode("utf-8")
                profile_setup._validate_runner_mirror_configuration(
                    source, profile_setup.APT_MIRRORLIST_BYTES)
            details = raised.exception.details
            self.assertEqual(details["exact_component_set_stanza_count"], 0)
            self.assertEqual(details["component_unknown_token_count"], expected_unknown)
            self.assertEqual(details["component_duplicate_token_count"], expected_duplicates)
            self.assertEqual(details["component_order_class_counts"]["invalid"], 2)
            rendered = json.dumps(details, sort_keys=True)
            self.assertNotIn("private-component", rendered)
            self.assertEqual(len(details["component_stanza_diagnostics"]), 2)
            for stanza in details["component_stanza_diagnostics"]:
                self.assertEqual(
                    set(stanza["allowed_component_counts"]),
                    set(profile_setup.APT_RUNNER_COMPONENTS),
                )

    def test_native_profile_setup_rejects_untrusted_or_changed_runner_mirror_configuration(self) -> None:
        bad_sources = (
            SYNTHETIC_UBUNTU_SOURCES.replace("noble-updates", "noble-proposed"),
            SYNTHETIC_UBUNTU_SOURCES.replace("Suites: noble-security", "Suites: noble-updates"),
            SYNTHETIC_UBUNTU_SOURCES.replace(
                "Suites: noble noble-updates noble-backports",
                "Suites: noble noble-updates noble-backports noble-security"),
            SYNTHETIC_UBUNTU_SOURCES.replace(
                "Suites: noble-security", "Suites: noble-security noble-updates"),
            SYNTHETIC_UBUNTU_SOURCES.replace(
                "Components: main restricted universe multiverse",
                "Components: main restricted universe"),
            SYNTHETIC_UBUNTU_SOURCES + "\n" + SYNTHETIC_UBUNTU_SOURCES.split("\n\n", 1)[0],
            SYNTHETIC_UBUNTU_SOURCES.replace(
                "/usr/share/keyrings/ubuntu-archive-keyring.gpg", "/tmp/untrusted.gpg"),
            SYNTHETIC_UBUNTU_SOURCES + "\nTypes: deb\nURIs: mirror+file:/etc/apt/apt-mirrors.txt\n",
            SYNTHETIC_UBUNTU_SOURCES.replace(
                "URIs: mirror+file:/etc/apt/apt-mirrors.txt",
                "URIs: mirror+file:/etc/apt/apt-mirrors.txt https://unexpected.invalid/ubuntu"),
            SYNTHETIC_UBUNTU_SOURCES.replace(
                "Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg\n",
                "Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg\nArchitectures: amd64\n"),
            SYNTHETIC_UBUNTU_SOURCES.replace(
                "Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg\n",
                "Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg\nTrusted: yes\n"),
            SYNTHETIC_UBUNTU_SOURCES.split("\n\nTypes: deb\n", 1)[0],
        )
        for source_text in bad_sources:
            with self.subTest(source_text=source_text), self.assertRaises(profile_setup.SetupFailure):
                profile_setup._validate_runner_mirror_configuration(
                    source_text.encode("utf-8"), profile_setup.APT_MIRRORLIST_BYTES)

        changed_mirrorlist = profile_setup.APT_MIRRORLIST_BYTES.replace(
            b"https://archive.ubuntu.com/ubuntu/", b"https://mirror.invalid/ubuntu/")
        with self.assertRaises(profile_setup.SetupFailure) as raised:
            profile_setup._validate_runner_mirror_configuration(
                SYNTHETIC_UBUNTU_SOURCES.encode("utf-8"), changed_mirrorlist)
        self.assertEqual(raised.exception.code, "ubuntu_runner_apt_mirrorlist_unrecognized")
        diagnostic = json.dumps(raised.exception.details, sort_keys=True)
        self.assertNotIn("mirror.invalid", diagnostic)
        self.assertNotIn("https://", diagnostic)

    def test_native_profile_setup_source_layout_diagnostics_are_bounded_and_content_free(self) -> None:
        private = SYNTHETIC_UBUNTU_SOURCES.replace(
            "noble-security", "private-suite-name") + (
                "\nTypes: deb\nURIs: https://user:secret@private.invalid/hidden\n"
                "Suites: secret\nComponents: hidden\nSigned-By: /private/keyring\n")
        with self.assertRaises(profile_setup.SetupFailure) as raised:
            profile_setup._validate_runner_mirror_configuration(
                private.encode("utf-8"), profile_setup.APT_MIRRORLIST_BYTES)
        diagnostic = json.dumps(raised.exception.details, sort_keys=True)
        self.assertEqual(raised.exception.code, "ubuntu_runner_sources_configuration_untrusted")
        self.assertEqual(raised.exception.details["source_record_count"], 3)
        self.assertEqual(raised.exception.details["mirror_source_stanza_count"], 2)
        self.assertEqual(raised.exception.details["exact_mirror_uri_stanza_count"], 2)
        self.assertEqual(raised.exception.details["archive_suite_stanza_count"], 1)
        self.assertEqual(raised.exception.details["security_suite_stanza_count"], 0)
        self.assertEqual(raised.exception.details["unknown_suite_group_stanza_count"], 2)
        self.assertEqual(raised.exception.details["recognized_layout"], "unrecognized")
        for private_value in ("private-suite-name", "private.invalid", "secret", "/private/keyring"):
            self.assertNotIn(private_value, diagnostic)

    def test_native_profile_setup_reads_only_root_owned_nonsymlink_trust_files(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-apt-root-trust-") as temporary:
            root = Path(temporary)
            target = root / "target"
            target.write_bytes(b"synthetic")
            link = root / "link"
            link.symlink_to(target)
            with self.assertRaises(profile_setup.SetupFailure) as raised:
                profile_setup._read_root_owned_file(
                    link, limit=64, failure_code="synthetic_trust_file_rejected")
            self.assertEqual(raised.exception.code, "synthetic_trust_file_rejected")
            if os.geteuid() != 0:
                with self.assertRaisesRegex(profile_setup.SetupFailure,
                                            "synthetic_trust_file_rejected"):
                    profile_setup._read_root_owned_file(
                        target, limit=64, failure_code="synthetic_trust_file_rejected")

    def test_native_profile_setup_classifies_fixed_keyring_read_failures_without_paths(self) -> None:
        keyring = profile_setup.UBUNTU_ARCHIVE_KEYRING
        expected_parents = {
            "/usr": SimpleNamespace(st_mode=profile_setup.stat.S_IFDIR | 0o755, st_uid=0,
                                     st_gid=0, st_size=0),
            "/usr/share": SimpleNamespace(st_mode=profile_setup.stat.S_IFDIR | 0o755, st_uid=0,
                                           st_gid=0, st_size=0),
            "/usr/share/keyrings": SimpleNamespace(
                st_mode=profile_setup.stat.S_IFDIR | 0o755, st_uid=0, st_gid=0, st_size=0),
        }
        regular_file = SimpleNamespace(
            st_mode=profile_setup.stat.S_IFREG | 0o644, st_uid=0, st_gid=0,
            st_size=128, st_dev=1, st_ino=2, st_mtime_ns=3, st_ctime_ns=4,
        )
        symlink_file = SimpleNamespace(
            st_mode=profile_setup.stat.S_IFLNK | 0o777, st_uid=0, st_gid=0,
            st_size=128, st_dev=1, st_ino=2, st_mtime_ns=3, st_ctime_ns=4,
        )

        def read_with(final_stat=None, *, missing=False, open_error=None):
            def fake_lstat(path):
                if str(path) in expected_parents:
                    return expected_parents[str(path)]
                if path == keyring:
                    if missing:
                        raise FileNotFoundError(profile_setup.errno.ENOENT, "private path detail")
                    return final_stat
                raise AssertionError("unexpected stat path")

            with mock.patch.object(Path, "lstat", new=fake_lstat):
                if open_error is None:
                    return profile_setup._read_root_owned_file(
                        keyring, limit=1024 * 1024,
                        failure_code="ubuntu_archive_keyring_unavailable",
                        diagnostic_id="ubuntu_archive_keyring")
                with mock.patch.object(profile_setup.os, "open", side_effect=open_error):
                    return profile_setup._read_root_owned_file(
                        keyring, limit=1024 * 1024,
                        failure_code="ubuntu_archive_keyring_unavailable",
                        diagnostic_id="ubuntu_archive_keyring")

        for case, kwargs, expected_phase, expected_component, expected_class in (
            ("missing", {"missing": True}, "file_lstat", "file", "missing"),
            ("symlink", {"final_stat": symlink_file}, "file_lstat", "file",
             "symlink_disallowed"),
            ("permission", {"final_stat": regular_file,
                            "open_error": PermissionError(profile_setup.errno.EACCES,
                                                           "private path detail")},
             "file_open", "file", "permission_denied"),
        ):
            with self.subTest(case=case), self.assertRaises(profile_setup.SetupFailure) as raised:
                read_with(**kwargs)
            self.assertEqual(raised.exception.code, "ubuntu_archive_keyring_unavailable")
            diagnostic = raised.exception.details["trust_file_diagnostics"]
            self.assertEqual(diagnostic["path_id"], "ubuntu_archive_keyring")
            self.assertEqual(diagnostic["phase"], expected_phase)
            self.assertEqual(diagnostic["component"], expected_component)
            self.assertEqual(diagnostic["failure_class"], expected_class)
            rendered = json.dumps(diagnostic, sort_keys=True)
            self.assertNotIn("/usr/share/keyrings", rendered)
            self.assertNotIn("ubuntu-archive-keyring.gpg", rendered)
            self.assertNotIn("private path detail", rendered)

    def test_native_profile_setup_classifies_keyring_parent_custody_rejection(self) -> None:
        keyring = profile_setup.UBUNTU_ARCHIVE_KEYRING
        def fake_lstat(path):
            if str(path) == "/usr":
                return SimpleNamespace(st_mode=profile_setup.stat.S_IFDIR | 0o755,
                                       st_uid=0, st_gid=0, st_size=0)
            if str(path) == "/usr/share":
                return SimpleNamespace(st_mode=profile_setup.stat.S_IFDIR | 0o755,
                                       st_uid=0, st_gid=0, st_size=0)
            if str(path) == "/usr/share/keyrings":
                return SimpleNamespace(st_mode=profile_setup.stat.S_IFLNK | 0o777,
                                       st_uid=0, st_gid=0, st_size=0)
            raise AssertionError("keyring file must not be read after parent rejection")

        with mock.patch.object(Path, "lstat", new=fake_lstat):
            with self.assertRaises(profile_setup.SetupFailure) as raised:
                profile_setup._read_root_owned_file(
                    keyring, limit=1024 * 1024,
                    failure_code="ubuntu_archive_keyring_unavailable",
                    diagnostic_id="ubuntu_archive_keyring")
        self.assertEqual(raised.exception.details["trust_file_diagnostics"], {
            "path_id": "ubuntu_archive_keyring",
            "phase": "parent_lstat",
            "component": "keyrings",
            "failure_class": "symlink_disallowed",
            "observed": {
                "object_type": "symlink", "owner": "root", "group": "root",
                "group_writable": True, "world_writable": True,
            },
        })

    def test_native_profile_setup_publishes_only_sanitized_keyring_failure_diagnostics(self) -> None:
        diagnostic = {
            "path_id": "ubuntu_archive_keyring",
            "phase": "file_lstat",
            "component": "file",
            "failure_class": "missing",
        }
        original_read_text = Path.read_text

        def fake_read_text(path, *args, **kwargs):
            if path == Path("/etc/os-release"):
                return "ID=ubuntu\nVERSION_ID=24.04\n"
            return original_read_text(path, *args, **kwargs)

        with tempfile.TemporaryDirectory(prefix="acv1-keyring-evidence-") as temporary:
            evidence_path = Path(temporary) / "profile-provenance.json"
            stdout = io.StringIO()
            stderr = io.StringIO()
            with ExitStack() as stack:
                stack.enter_context(mock.patch.object(
                    profile_setup.platform, "system", return_value="Linux"))
                stack.enter_context(mock.patch.object(
                    profile_setup.platform, "machine", return_value="x86_64"))
                stack.enter_context(mock.patch.object(Path, "read_text", new=fake_read_text))
                stack.enter_context(mock.patch.object(
                    profile_setup, "_apt_toolchain_evidence", return_value={}))
                stack.enter_context(mock.patch.object(
                    profile_setup, "_state_snapshot", return_value={}))
                stack.enter_context(mock.patch.object(
                    profile_setup, "_public_state", return_value={}))
                stack.enter_context(mock.patch.object(
                    profile_setup, "_apt_signature_configuration", return_value={}))
                stack.enter_context(mock.patch.object(
                    profile_setup, "_runner_mirror_configuration",
                    side_effect=profile_setup.SetupFailure(
                        "ubuntu_archive_keyring_unavailable",
                        details={"trust_file_diagnostics": diagnostic})))
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = profile_setup.prepare(evidence_path)

            artifact = json.loads(evidence_path.read_text(encoding="utf-8"))
            console = json.loads(stdout.getvalue())
        self.assertEqual(exit_code, 1)
        self.assertEqual(artifact["failure_code"], "ubuntu_archive_keyring_unavailable")
        self.assertEqual(artifact["trust_file_diagnostics"], diagnostic)
        self.assertEqual(console["diagnostics"]["trust_file_diagnostics"], diagnostic)
        rendered = json.dumps({"artifact": artifact, "console": console}, sort_keys=True)
        self.assertNotIn("/usr/share/keyrings", rendered)
        self.assertNotIn("ubuntu-archive-keyring.gpg", rendered)
        self.assertNotIn("key bytes", rendered)

    def test_pinned_keyring_capture_accepts_only_observed_writable_parents_for_pinned_bytes(self) -> None:
        approved = b"synthetic approved archive keyring"
        uid, gid = os.geteuid(), os.getegid()
        for writable_component in ("share", "keyrings"):
            with self.subTest(writable_component=writable_component), \
                    tempfile.TemporaryDirectory(prefix="acv1-pinned-keyring-") as temporary:
                root = Path(temporary)
                path = root / "usr" / "share" / "keyrings" / "ubuntu-archive-keyring.gpg"
                path.parent.mkdir(parents=True)
                os.chmod(root / "usr", 0o755)
                os.chmod(root / "usr" / "share", 0o777 if writable_component == "share" else 0o755)
                os.chmod(path.parent, 0o777 if writable_component == "keyrings" else 0o755)
                path.write_bytes(approved)
                os.chmod(path, 0o644)
                with mock.patch.object(profile_setup, "UBUNTU_ARCHIVE_KEYRING_SHA256", sha(approved)), \
                        mock.patch.object(profile_setup, "UBUNTU_ARCHIVE_KEYRING_SIZE", len(approved)):
                    captured, evidence = profile_setup._read_pinned_keyring_bytes(
                        path, trusted_root=root, owner_uid=uid, owner_gid=gid)
                self.assertEqual(captured, approved)
                self.assertEqual(evidence["sha256"], sha(approved))
                self.assertEqual(evidence["writable_parent_component"], writable_component)
                self.assertFalse(evidence["source_parent_reopened_by_apt"])

                # Replacing the writable source entry after capture cannot alter
                # the captured bytes or redirect APT away from the private copy.
                attacker = root / "attacker-keyring.gpg"
                attacker.write_bytes(b"x" * len(approved))
                path.unlink()
                path.symlink_to(attacker)
                private_root = Path("/run") / ("acv1-contract-apt-" + "a" * 32)
                sources = profile_setup._private_source_bytes(
                    profile_setup._parse_deb822_records(
                        SYNTHETIC_UBUNTU_SOURCES,
                        failure_code="synthetic_source_invalid", max_bytes=64 * 1024),
                    private_root / "ubuntu-archive-keyring.gpg",
                    private_root / "apt-mirrors.txt",
                )
                self.assertIn(("Signed-By: " + str(private_root
                                                   / "ubuntu-archive-keyring.gpg")).encode(), sources)
                self.assertIn(("URIs: mirror+file:" + str(private_root
                                                           / "apt-mirrors.txt")).encode(), sources)
                self.assertNotIn(str(path).encode(), sources)
                self.assertEqual(captured, approved)

    def test_pinned_keyring_capture_rejects_digest_changes(self) -> None:
        approved = b"synthetic approved archive keyring"
        uid, gid = os.geteuid(), os.getegid()
        with tempfile.TemporaryDirectory(prefix="acv1-keyring-negative-") as temporary:
            root = Path(temporary)
            parent = root / "usr" / "share" / "keyrings"
            parent.mkdir(parents=True)
            os.chmod(root / "usr", 0o755)
            os.chmod(root / "usr" / "share", 0o777)
            os.chmod(parent, 0o755)
            path = parent / "ubuntu-archive-keyring.gpg"
            path.write_bytes(b"x" * len(approved))
            os.chmod(path, 0o644)
            with mock.patch.object(profile_setup, "UBUNTU_ARCHIVE_KEYRING_SHA256", sha(approved)), \
                    mock.patch.object(profile_setup, "UBUNTU_ARCHIVE_KEYRING_SIZE", len(approved)):
                with self.assertRaisesRegex(profile_setup.SetupFailure,
                                             "ubuntu_archive_keyring_digest_mismatch"):
                    profile_setup._read_pinned_keyring_bytes(
                        path, trusted_root=root, owner_uid=uid, owner_gid=gid)

    def test_pinned_keyring_capture_still_rejects_unreviewed_writable_parent_and_keyrings_symlink(self) -> None:
        approved = b"synthetic approved archive keyring"
        uid, gid = os.geteuid(), os.getegid()
        with tempfile.TemporaryDirectory(prefix="acv1-keyring-parent-boundary-") as temporary:
            root = Path(temporary)
            path = root / "usr" / "share" / "keyrings" / "ubuntu-archive-keyring.gpg"
            path.parent.mkdir(parents=True)
            path.write_bytes(approved)
            os.chmod(path, 0o644)
            os.chmod(root / "usr", 0o777)
            with mock.patch.object(profile_setup, "UBUNTU_ARCHIVE_KEYRING_SHA256", sha(approved)), \
                    mock.patch.object(profile_setup, "UBUNTU_ARCHIVE_KEYRING_SIZE", len(approved)):
                with self.assertRaises(profile_setup.SetupFailure) as raised:
                    profile_setup._read_pinned_keyring_bytes(
                        path, trusted_root=root, owner_uid=uid, owner_gid=gid)
                diagnostics = raised.exception.details["trust_file_diagnostics"]
                self.assertEqual(diagnostics["phase"], "parent_lstat")
                self.assertEqual(diagnostics["component"], "usr")
                self.assertEqual(diagnostics["failure_class"], "writable_parent")

                os.chmod(root / "usr", 0o755)
                target = root / "real-keyrings"
                path.parent.rename(target)
                path.parent.symlink_to(target, target_is_directory=True)
                with self.assertRaises(profile_setup.SetupFailure) as raised:
                    profile_setup._read_pinned_keyring_bytes(
                        path, trusted_root=root, owner_uid=uid, owner_gid=gid)
                diagnostics = raised.exception.details["trust_file_diagnostics"]
                self.assertEqual(diagnostics["phase"], "parent_lstat")
                self.assertEqual(diagnostics["component"], "keyrings")
                self.assertEqual(diagnostics["failure_class"], "symlink_disallowed")

    def test_pinned_keyring_capture_rejects_writable_parent_and_leaf_replacement_races(self) -> None:
        approved = b"synthetic approved archive keyring"
        uid, gid = os.geteuid(), os.getegid()

        for replacement in ("keyrings_parent", "leaf_before_open", "leaf_during_read"):
            with self.subTest(replacement=replacement), \
                    tempfile.TemporaryDirectory(prefix="acv1-keyring-race-") as temporary:
                root = Path(temporary)
                path = root / "usr" / "share" / "keyrings" / "ubuntu-archive-keyring.gpg"
                path.parent.mkdir(parents=True)
                path.write_bytes(approved)
                os.chmod(path, 0o644)
                os.chmod(path.parent, 0o777)
                original_open = profile_setup.os.open
                original_read = profile_setup.os.read
                changed = False

                def replace_before_open(file: Any, flags: int, mode: int = 0o777, *,
                                        dir_fd: int | None = None) -> int:
                    nonlocal changed
                    if replacement == "keyrings_parent" and file == "keyrings" and not changed:
                        destination = root / "captured-keyrings"
                        path.parent.rename(destination)
                        path.parent.symlink_to(destination, target_is_directory=True)
                        changed = True
                    elif replacement == "leaf_before_open" and file == path.name and dir_fd is not None and not changed:
                        target = root / "attacker-keyring.gpg"
                        target.write_bytes(b"attacker-controlled-keyring")
                        path.unlink()
                        path.symlink_to(target)
                        changed = True
                    return original_open(file, flags, mode, dir_fd=dir_fd)

                def replace_during_read(fd: int, count: int) -> bytes:
                    nonlocal changed
                    if replacement == "leaf_during_read" and not changed:
                        path.write_bytes(b"replacement bytes with a different size")
                        changed = True
                    return original_read(fd, count)

                with mock.patch.object(profile_setup, "UBUNTU_ARCHIVE_KEYRING_SHA256", sha(approved)), \
                        mock.patch.object(profile_setup, "UBUNTU_ARCHIVE_KEYRING_SIZE", len(approved)), \
                        mock.patch.object(profile_setup.os, "open", side_effect=replace_before_open), \
                        mock.patch.object(profile_setup.os, "read", side_effect=replace_during_read):
                    with self.assertRaises(profile_setup.SetupFailure) as raised:
                        profile_setup._read_pinned_keyring_bytes(
                            path, trusted_root=root, owner_uid=uid, owner_gid=gid)
                self.assertTrue(changed)
                diagnostics = raised.exception.details["trust_file_diagnostics"]
                if replacement == "leaf_during_read":
                    self.assertEqual(diagnostics["phase"], "descriptor_recheck")
                    self.assertEqual(diagnostics["failure_class"], "file_changed_during_read")
                else:
                    self.assertIn(diagnostics["phase"], {"parent_open", "file_open"})
                    self.assertIn(diagnostics["failure_class"], {"symlink_disallowed", "not_directory"})

    def test_private_apt_snapshot_uses_preloaded_config_and_captured_sources(self) -> None:
        root = Path("/run") / ("acv1-contract-apt-" + "b" * 32)
        keyring = root / "ubuntu-archive-keyring.gpg"
        mirrors = root / "apt-mirrors.txt"
        records = profile_setup._parse_deb822_records(
            SYNTHETIC_UBUNTU_SOURCES, failure_code="synthetic_source_invalid",
            max_bytes=64 * 1024)
        source = profile_setup._private_source_bytes(records, keyring, mirrors).decode("utf-8")
        self.assertEqual(source.count("Types: deb\n"), 2)
        self.assertEqual(source.count("URIs: mirror+file:" + str(mirrors)), 2)
        self.assertEqual(source.count("Signed-By: " + str(keyring)), 2)
        self.assertNotIn("/etc/apt/apt-mirrors.txt", source)
        self.assertNotIn(str(profile_setup.UBUNTU_ARCHIVE_KEYRING), source)
        private_uri = "mirror+file:" + str(mirrors)
        private_transport = synthetic_apt_transport()
        private_transport["_source_uri"] = private_uri
        source_identity = private_uri + " noble-updates/main amd64 Packages"
        self.assertEqual(profile_setup._validated_apt_source(
            source_identity, private_transport), source_identity)
        wrong_private = source_identity.replace("b" * 32, "f" * 32)
        with self.assertRaisesRegex(profile_setup.SetupFailure,
                                    "signed_apt_package_origin_is_not_ubuntu_noble_updates"):
            profile_setup._validated_apt_source(wrong_private, private_transport)
        self.assertEqual(profile_setup._candidate_record_diagnostics(
            "", "Package: apparmor-profiles:all\n" +
            "Version: 1\n" + "Filename: x\n" + "SHA256: x\n" +
            "APT-Sources: " + source_identity + "\n",
            profile_setup.PROFILE_PACKAGE, "1")["source_record"]["source_host_class"],
            "ubuntu_runner_mirrorlist")

        config = profile_setup._private_apt_config_bytes(root).decode("ascii")
        for value in (str(root / "ubuntu.sources"), str(root / "apt.conf.d"),
                      str(root / "sourceparts"), str(root / "preferences"),
                      str(root / "trusted.gpg"), str(root / "lists")):
            self.assertIn(value, config)
        self.assertIn('Acquire::AllowInsecureRepositories "false";', config)
        self.assertIn('Acquire::Check-Valid-Until "true";', config)
        self.assertIn('Dir::Cache::pkgcache "";', config)
        self.assertIn('Dir::Cache::srcpkgcache "";', config)
        self.assertNotIn("/etc/apt/", config)
        cli_options = profile_setup._private_apt_cli_options(root)
        self.assertNotIn("-c", cli_options)
        option_values = {
            cli_options[index + 1].split("=", 1)[0]: cli_options[index + 1].split("=", 1)[1]
            for index, value in enumerate(cli_options[:-1]) if value == "-o"
        }
        self.assertEqual(option_values["Dir::Etc"], str(root))
        self.assertEqual(option_values["Dir::Etc::trusted"], str(root / "trusted.gpg"))
        self.assertEqual(option_values["Dir::Cache::pkgcache"], "")
        self.assertEqual(option_values["Dir::Cache::srcpkgcache"], "")
        self.assertEqual(option_values["Acquire::AllowInsecureRepositories"], "false")
        self.assertEqual(option_values["Acquire::Check-Valid-Until"], "true")

        apt_config = root / "apt.conf"
        expected_env = dict(os.environ)
        expected_env["APT_CONFIG"] = str(apt_config)
        with mock.patch.object(profile_setup.subprocess, "run", return_value=SimpleNamespace(
                returncode=0, stdout=b"ok", stderr=b"")) as run:
            result, _ = profile_setup._command(
                ["/usr/bin/apt-config", "dump"], apt_config=apt_config)
        self.assertEqual(result, b"ok")
        self.assertEqual(run.call_args.kwargs["env"], expected_env)
        self.assertEqual(profile_setup._apt_sudo_prefix({"_private_apt_dir": str(root)}), [
            "/usr/bin/sudo", "-n", "--", "/usr/bin/env",
            "APT_CONFIG=" + str(apt_config),
        ])

    def test_ubuntu_keyring_pin_matches_reviewed_official_source_member_provenance(self) -> None:
        self.assertEqual(profile_setup.UBUNTU_KEYRING_PACKAGE_VERSION, "2023.11.28.1")
        self.assertEqual(profile_setup.UBUNTU_ARCHIVE_KEYRING_SHA256,
                         "80a36b0a6de2f69f49d2df75ef473ccde121e9e190b9ea01d20a4f63778d5c31")
        self.assertEqual(profile_setup.UBUNTU_ARCHIVE_KEYRING_SIZE, 3607)
        self.assertEqual(profile_setup.UBUNTU_KEYRING_SOURCE_TARBALL_SHA256,
                         "aecd455ae15561371d6e454f121f079f0641d5e1b579a5563a2bc363fc74aa2e")
        self.assertEqual(profile_setup.UBUNTU_KEYRING_SOURCE_SIGNER_FINGERPRINT,
                         "EE240A052A231AD8925841D79B8EC849D5EF70ED")

    def test_private_apt_configuration_validation_checks_every_isolated_path_and_security_value(self) -> None:
        root = Path("/run") / ("acv1-contract-apt-" + "c" * 32)
        path_values = {
            "Dir::Etc": str(root),
            "Dir::Etc::main": "/dev/null",
            "Dir::Etc::parts": str(root / "apt.conf.d"),
            "Dir::Etc::sourcelist": str(root / "ubuntu.sources"),
            "Dir::Etc::sourceparts": str(root / "sourceparts"),
            "Dir::Etc::preferences": str(root / "preferences"),
            "Dir::Etc::preferencesparts": str(root / "preferences.d"),
            "Dir::Etc::trusted": str(root / "trusted.gpg"),
            "Dir::Etc::trustedparts": str(root / "trusted.gpg.d"),
            "Dir::State::lists": str(root / "lists"),
            "Dir::State::extended_states": str(root / "extended_states"),
            "Dir::Cache::archives": str(root / "archives"),
            "Dir::Cache::pkgcache": "",
            "Dir::Cache::srcpkgcache": "",
        }
        settings = {**path_values,
                    "APT::Get::AllowUnauthenticated": "false",
                    "Acquire::AllowInsecureRepositories": "false",
                    "Acquire::AllowWeakRepositories": "false",
                    "Acquire::AllowDowngradeToInsecureRepositories": "false",
                    "Acquire::Check-Date": "true",
                    "Acquire::Check-Valid-Until": "true"}
        text = "".join(f'{name} "{value}";\n' for name, value in settings.items())
        transport = {"_private_apt_dir": str(root)}
        with mock.patch.object(profile_setup, "_verify_private_apt_snapshot", return_value=root), \
                mock.patch.object(profile_setup, "_command", return_value=(text.encode(), b"")) as command:
            evidence = profile_setup._verify_private_apt_configuration(transport)
        self.assertEqual(evidence["source_config"], "private_exact_runner_stanzas")
        self.assertEqual(command.call_args.kwargs["apt_config"], root / "apt.conf")

        unsafe = text.replace('Acquire::Check-Date "true";',
                              'Acquire::Check-Date "false";')
        with mock.patch.object(profile_setup, "_verify_private_apt_snapshot", return_value=root), \
                mock.patch.object(profile_setup, "_command", return_value=(unsafe.encode(), b"")):
            with self.assertRaisesRegex(profile_setup.SetupFailure,
                                        "apt_source_configuration_disables_freshness_checks"):
                profile_setup._verify_private_apt_configuration(transport)
        wrong_trust_path = text.replace(
            f'Dir::Etc::trustedparts "{root / "trusted.gpg.d"}";',
            'Dir::Etc::trustedparts "/etc/apt/trusted.gpg.d";')
        with mock.patch.object(profile_setup, "_verify_private_apt_snapshot", return_value=root), \
                mock.patch.object(profile_setup, "_command",
                                  return_value=(wrong_trust_path.encode(), b"")):
            with self.assertRaisesRegex(profile_setup.SetupFailure,
                                        "private_apt_configuration_paths_mismatch"):
                profile_setup._verify_private_apt_configuration(transport)

    def test_native_profile_setup_binds_candidate_to_authenticated_apt_release_target(self) -> None:
        transport = synthetic_apt_transport()
        source = (profile_setup.APT_MIRROR_URI
                  + " noble-updates/main amd64 Packages")
        for repo_uri in (
            profile_setup.APT_MIRROR_URI,
            "http://azure.archive.ubuntu.com/ubuntu/",
            "https://archive.ubuntu.com/ubuntu/",
            "https://security.ubuntu.com/ubuntu/",
        ):
            with self.subTest(repo_uri=repo_uri):
                evidence = profile_setup._validate_signed_apt_index_target(
                    synthetic_apt_index_target(repo_uri), source, transport)
            self.assertEqual(evidence["trusted"], "yes")
            self.assertEqual(evidence["origin"], "Ubuntu")
            self.assertEqual(evidence["suite"], "noble-updates")
            self.assertEqual(evidence["component"], "main")
            self.assertEqual(evidence["architecture"], "amd64")
            self.assertEqual(evidence["signed_by"],
                             "private_captured_ubuntu_archive_keyring_snapshot")

    def test_native_profile_setup_filters_unrelated_supported_index_target_records(self) -> None:
        transport = synthetic_apt_transport()
        source = profile_setup.APT_MIRROR_URI + " noble-updates/main amd64 Packages"
        package_target = synthetic_apt_index_target()
        translation_target = synthetic_apt_index_target(**{
            "Identifier": "Translations",
            "MetaKey": "main/i18n/Translation-en",
            "Architecture": "$(ARCHITECTURE)",
            "URI": (profile_setup.APT_MIRROR_URI.rstrip("/")
                    + "/dists/noble-updates/main/i18n/Translation-en"),
        })
        contents_target = synthetic_apt_index_target(**{
            "Identifier": "Contents-deb",
            "MetaKey": "main/Contents-amd64",
            "URI": (profile_setup.APT_MIRROR_URI.rstrip("/")
                    + "/dists/noble-updates/main/Contents-amd64"),
        })
        combined = translation_target + contents_target + package_target
        evidence = profile_setup._validate_signed_apt_index_target(
            combined, source, transport)
        self.assertEqual(evidence["target"], "Packages/main/binary-amd64")
        diagnostics = profile_setup._apt_index_target_diagnostics(combined, source, transport)
        self.assertEqual(diagnostics["target_record_count"], 3)
        self.assertEqual(diagnostics["package_candidate_count"], 1)
        self.assertEqual(diagnostics["matching_ubuntu_updates_target_count"], 1)
        self.assertEqual(diagnostics["unrelated_target_record_count"], 2)
        self.assertEqual(diagnostics["unrelated_unresolved_variable_record_count"], 1)
        self.assertEqual(diagnostics["candidate_unexpanded_field_count"], 0)

    def test_native_profile_setup_selects_noble_updates_before_counting_configured_suites(self) -> None:
        transport = synthetic_apt_transport()
        source = profile_setup.APT_MIRROR_URI + " noble-updates/main amd64 Packages"
        suites = (
            "noble", "noble-updates", "noble-backports", "noble-security",
        )
        package_targets = [
            synthetic_apt_index_target(Suite=suite, Codename="noble")
            for suite in suites
        ]
        translation = synthetic_apt_index_target(**{
            "Identifier": "Translations", "MetaKey": "main/i18n/Translation-en",
            "Architecture": "$(ARCHITECTURE)",
        })
        contents = synthetic_apt_index_target(**{
            "Identifier": "Contents-deb", "MetaKey": "main/Contents-amd64",
        })
        listing = translation + contents + "".join(package_targets)
        evidence = profile_setup._validate_signed_apt_index_target(listing, source, transport)
        diagnostics = profile_setup._apt_index_target_diagnostics(listing, source, transport)
        self.assertEqual(evidence["suite"], "noble-updates")
        self.assertEqual(diagnostics["package_index_family_count"], 4)
        self.assertEqual(diagnostics["package_candidate_count"], 1)
        self.assertEqual(diagnostics["unrelated_target_record_count"], 5)
        self.assertEqual(diagnostics["unrelated_unresolved_variable_record_count"], 1)

        duplicate_updates = (translation + contents + package_targets[0]
                             + package_targets[1] + package_targets[1]
                             + package_targets[2] + package_targets[3])
        with self.assertRaisesRegex(profile_setup.SetupFailure,
                                    "apt_signed_release_target_missing_or_ambiguous"):
            profile_setup._validate_signed_apt_index_target(
                duplicate_updates, source, transport)

        private_root = Path("/run") / ("acv1-contract-apt-" + "e" * 32)
        private_transport = dict(transport)
        private_transport["_source_uri"] = (
            "mirror+file:" + str(private_root / "apt-mirrors.txt"))
        private_source = private_transport["_source_uri"] + " noble-updates/main amd64 Packages"
        azure_target = synthetic_apt_index_target("http://azure.archive.ubuntu.com/ubuntu/")
        private_evidence = profile_setup._validate_signed_apt_index_target(
            azure_target, private_source, private_transport)
        self.assertEqual(private_evidence["suite"], "noble-updates")
        self.assertEqual(private_evidence["transport"], "runner_mirrorlist")

    def test_native_profile_setup_rejects_untrusted_or_ambiguous_apt_release_targets(self) -> None:
        transport = synthetic_apt_transport()
        source = profile_setup.APT_MIRROR_URI + " noble-updates/main amd64 Packages"
        for changes in (
            {"Trusted": "no"},
            {"Origin": "Other"},
            {"Label": "Other"},
            {"Suite": "noble"},
            {"Codename": "jammy"},
            {"Component": "universe"},
            {"Architecture": "arm64"},
            {"Signed-By": "/etc/apt/trusted.gpg"},
            {"Repo-URI": "https://mirror.invalid/ubuntu/"},
            {"MetaKey": "main/binary-amd64/Sources"},
        ):
            record = synthetic_apt_index_target(**changes)
            with self.subTest(changes=changes), self.assertRaises(profile_setup.SetupFailure):
                profile_setup._validate_signed_apt_index_target(record, source, transport)
        duplicate = synthetic_apt_index_target() + synthetic_apt_index_target()
        with self.assertRaisesRegex(profile_setup.SetupFailure,
                                    "apt_signed_release_target_missing_or_ambiguous"):
            profile_setup._validate_signed_apt_index_target(duplicate, source, transport)
        missing_candidate_field = synthetic_apt_index_target().replace(
            "Architecture: amd64\n", "")
        with self.assertRaisesRegex(profile_setup.SetupFailure,
                                    "apt_signed_release_target_missing_or_ambiguous"):
            profile_setup._validate_signed_apt_index_target(
                missing_candidate_field, source, transport)
        with self.assertRaisesRegex(profile_setup.SetupFailure,
                                    "apt_signed_release_target_missing_or_ambiguous"):
            profile_setup._validate_signed_apt_index_target(
                synthetic_apt_index_target(Architecture="$(ARCHITECTURE)"), source, transport)
        with self.assertRaisesRegex(profile_setup.SetupFailure,
                                    "apt_signed_release_target_unexpanded_format_variable"):
            profile_setup._validate_signed_apt_index_target(
                synthetic_apt_index_target(Origin="$(ORIGIN)"), source, transport)
        with self.assertRaises(profile_setup.SetupFailure) as raised:
            profile_setup._validate_signed_apt_index_target(
                synthetic_apt_index_target().replace("Trusted: yes", "Trusted: no"),
                source, transport)
        diagnostics = json.dumps(raised.exception.details, sort_keys=True)
        self.assertNotIn("/etc/apt/apt-mirrors.txt", diagnostics)
        self.assertNotIn("ubuntu-archive-keyring.gpg", diagnostics)

    def test_native_profile_setup_apt_source_diagnostics_identify_runner_mirror_transport(self) -> None:
        metadata = (
            "Package: apparmor-profiles\n"
            "Version: " + profile_setup.PROFILE_VERSION + "\n"
            "Architecture: all\n"
            "Filename: pool/main/a/apparmor/apparmor-profiles_" + profile_setup.PROFILE_VERSION
            + "_all.deb\n"
            "SHA256: " + profile_setup.PROFILE_DEB_SHA256 + "\n"
        )
        source = (
            "Package: apparmor-profiles:all\n"
            "Version: " + profile_setup.PROFILE_VERSION + "\n"
            "Filename: pool/main/a/apparmor/apparmor-profiles_" + profile_setup.PROFILE_VERSION
            + "_all.deb\n"
            "SHA256: " + profile_setup.PROFILE_DEB_SHA256 + "\n"
            "APT-Sources: mirror+file:/etc/apt/apt-mirrors.txt noble-updates/main amd64 Packages\n"
        )
        diagnostics = profile_setup._candidate_record_diagnostics(
            metadata, source, profile_setup.PROFILE_PACKAGE, profile_setup.PROFILE_VERSION)
        self.assertEqual(diagnostics["source_record"]["source_scheme"], "mirror+file")
        self.assertEqual(diagnostics["source_record"]["source_host_class"], "ubuntu_runner_mirrorlist")

    def test_native_profile_setup_classifies_but_rejects_unknown_mirrorlist_path(self) -> None:
        source = (
            "Package: apparmor-profiles:all\n"
            "Version: " + profile_setup.PROFILE_VERSION + "\n"
            "Filename: pool/main/a/apparmor/apparmor-profiles_" + profile_setup.PROFILE_VERSION
            + "_all.deb\n"
            "SHA256: " + profile_setup.PROFILE_DEB_SHA256 + "\n"
            "APT-Sources: mirror+file:/etc/apt/unreviewed-mirrors.txt noble-updates/main amd64 Packages\n"
        )
        with self.assertRaises(profile_setup.SetupFailure) as raised:
            profile_setup.validate_apt_record(
                source, profile_setup.PROFILE_PACKAGE, profile_setup.PROFILE_VERSION,
                profile_setup.PROFILE_DEB_SHA256, source_text=source)
        diagnostics = raised.exception.details["source_record"]
        self.assertEqual(diagnostics["source_scheme"], "mirror+file")
        self.assertEqual(diagnostics["source_host_class"], "unrecognized_mirrorlist_path")
        rendered = json.dumps(raised.exception.details, sort_keys=True)
        self.assertNotIn("unreviewed-mirrors.txt", rendered)
        self.assertNotIn("/etc/apt", rendered)

    def test_native_profile_setup_parses_labelled_dpkg_fields_without_control_order_assumptions(self) -> None:
        labelled = (
            "Architecture: all\n"
            "Package: apparmor-profiles\n"
            "Version: " + profile_setup.PROFILE_VERSION + "\n"
        )
        self.assertEqual(profile_setup.parse_dpkg_deb_fields(labelled), {
            "Package": profile_setup.PROFILE_PACKAGE,
            "Version": profile_setup.PROFILE_VERSION,
            "Architecture": "all",
        })
        self.assertEqual(profile_setup.validate_profile_deb_metadata(labelled.encode("ascii")), {
            "Package": profile_setup.PROFILE_PACKAGE,
            "Version": profile_setup.PROFILE_VERSION,
            "Architecture": "all",
        })
        invalid_outputs = (
            "apparmor-profiles\n" + profile_setup.PROFILE_VERSION + "\nall\n",
            "Package: apparmor-profiles\nPackage: apparmor-profiles\nVersion: "
            + profile_setup.PROFILE_VERSION + "\nArchitecture: all\n",
            "Package: apparmor-profiles\nVersion: " + profile_setup.PROFILE_VERSION + "\n",
            "Package: apparmor-profiles\nVersion: " + profile_setup.PROFILE_VERSION
            + "\nArchitecture: all\nStatus: install ok installed\n",
        )
        for output in invalid_outputs:
            with self.subTest(output=output), self.assertRaisesRegex(
                    profile_setup.SetupFailure, "downloaded_profile_package_metadata_invalid"):
                profile_setup.parse_dpkg_deb_fields(output)
        for mismatch in (
            labelled.replace("Package: apparmor-profiles", "Package: other-package"),
            labelled.replace(profile_setup.PROFILE_VERSION, "stale-version"),
            labelled.replace("Architecture: all", "Architecture: amd64"),
        ):
            with self.subTest(mismatch=mismatch), self.assertRaisesRegex(
                    profile_setup.SetupFailure, "downloaded_profile_package_metadata_mismatch"):
                profile_setup.validate_profile_deb_metadata(mismatch.encode("ascii"))

    def test_native_profile_setup_reads_source_from_explicit_apt_cache_v2_record(self) -> None:
        metadata = (
            "Package: apparmor-profiles\n"
            "Version: 4.0.1really4.0.1-0ubuntu0.24.04.8\n"
            "Architecture: all\n"
            "Filename: pool/main/a/apparmor/apparmor-profiles_4.0.1really4.0.1-0ubuntu0.24.04.8_all.deb\n"
            "SHA256: 4e7d728322f899a7a06e71bedd4f4bd1f20c21f0b3361120f34cf5c0feec849e\n"
        )
        source_record = (
            "Package: apparmor-profiles:all\n"
            "Version: 4.0.1really4.0.1-0ubuntu0.24.04.8\n"
            "Filename: pool/main/a/apparmor/apparmor-profiles_4.0.1really4.0.1-0ubuntu0.24.04.8_all.deb\n"
            "SHA256: 4e7d728322f899a7a06e71bedd4f4bd1f20c21f0b3361120f34cf5c0feec849e\n"
            "APT-Sources: https://azure.archive.ubuntu.com/ubuntu noble-updates/main amd64 Packages\n"
        )
        package_target = synthetic_apt_index_target(
            "https://azure.archive.ubuntu.com/ubuntu/")
        translation_target = synthetic_apt_index_target(**{
            "Identifier": "Translations",
            "MetaKey": "main/i18n/Translation-en",
            "Architecture": "$(ARCHITECTURE)",
            "URI": "https://azure.archive.ubuntu.com/ubuntu/dists/noble-updates/main/i18n/Translation-en",
        })
        other_target = synthetic_apt_index_target(**{
            "Identifier": "Contents-deb",
            "MetaKey": "main/Contents-amd64",
            "URI": "https://azure.archive.ubuntu.com/ubuntu/dists/noble-updates/main/Contents-amd64",
        })
        index_targets = translation_target + other_target + package_target
        with mock.patch.object(profile_setup, "_apt_options", return_value=[]), \
                mock.patch.object(profile_setup, "_apt_config_path",
                                  return_value=Path("/run/acv1-contract-apt-" + "a" * 32 + "/apt.conf")), \
                mock.patch.object(profile_setup, "_command", side_effect=[
                (metadata.encode(), b""), (source_record.encode(), b""),
                (index_targets.encode(), b"")]) as command:
            parsed = profile_setup._package_record(
                profile_setup.PROFILE_PACKAGE, profile_setup.PROFILE_VERSION,
                profile_setup.PROFILE_DEB_SHA256, apt_transport=synthetic_apt_transport())
        self.assertEqual(parsed["APT-Sources"],
                         "https://azure.archive.ubuntu.com/ubuntu noble-updates/main amd64 Packages")
        self.assertEqual(parsed["_release_evidence"]["trusted"], "yes")
        self.assertEqual(command.call_args_list[0].args[0], [
            profile_setup.APT_CACHE_PATH, "-o", "APT::Cache::ShowFull=true",
            "-o", "APT::Cache::Show::Version=1", "show",
            "apparmor-profiles=4.0.1really4.0.1-0ubuntu0.24.04.8"])
        self.assertEqual(command.call_args_list[1].args[0], [
            profile_setup.APT_CACHE_PATH, "-o", "APT::Cache::ShowFull=true",
            "-o", "APT::Cache::Show::Version=2", "show",
            "apparmor-profiles=4.0.1really4.0.1-0ubuntu0.24.04.8"])
        self.assertEqual(command.call_args_list[2].args[0], [
            profile_setup.APT_GET_PATH, "-o", "APT::Get::IndexTargets::ReleaseInfo=true",
            "indextargets", "--format", profile_setup.APT_INDEXTARGETS_FORMAT])
        self.assertEqual(command.call_args_list[0].kwargs["apt_phase"], "metadata_v1")
        self.assertEqual(command.call_args_list[1].kwargs["apt_phase"], "source_v2")
        self.assertEqual(command.call_args_list[2].kwargs["apt_phase"], "index_targets")
        self.assertNotIn("-f", command.call_args_list[0].args[0])
        self.assertNotIn("-f", command.call_args_list[1].args[0])

    def test_native_profile_setup_classifies_apt_command_failures_without_raw_output(self) -> None:
        cases = (
            (b"E: Command line option 'f' [from -f] is not understood in this command", "unsupported_option"),
            (b"E: No packages found matching bubblewrap=stale-version", "pinned_package_or_version_unavailable"),
            (b"W: Failed to fetch https://user:secret@mirror.invalid/private/Packages", "apt_repository_or_index_infrastructure_failure"),
            (b"APT failed with an unrecognized condition", "unclassified_apt_command_failure"),
        )
        for stderr, expected_category in cases:
            with self.subTest(expected_category=expected_category), \
                    mock.patch.object(profile_setup.subprocess, "run", return_value=SimpleNamespace(
                        returncode=100, stdout=b"private package record", stderr=stderr)):
                with self.assertRaises(profile_setup.SetupFailure) as raised:
                    profile_setup._command(
                        ["apt-cache", "fixed", "arguments"], apt_phase="source_v2")
            self.assertEqual(raised.exception.code, "command_failed")
            diagnostic = raised.exception.details["apt_command"]
            self.assertEqual(diagnostic["tool"], "apt-cache")
            self.assertEqual(diagnostic["phase"], "source_v2")
            self.assertEqual(diagnostic["failure_kind"], "nonzero_exit")
            self.assertEqual(diagnostic["return_code"], 100)
            self.assertEqual(diagnostic["error_category"], expected_category)
            serialized = json.dumps(diagnostic, sort_keys=True)
            self.assertNotIn("secret", serialized)
            self.assertNotIn("private", serialized)
            self.assertNotIn("mirror.invalid", serialized)
            self.assertNotIn(stderr.decode(), serialized)

    def test_native_profile_setup_preserves_source_command_phase_with_candidate_shape(self) -> None:
        metadata = (
            "Package: apparmor-profiles\n"
            "Version: 4.0.1really4.0.1-0ubuntu0.24.04.8\n"
            "Architecture: all\n"
            "Filename: pool/main/a/apparmor/apparmor-profiles_4.0.1really4.0.1-0ubuntu0.24.04.8_all.deb\n"
            "SHA256: 4e7d728322f899a7a06e71bedd4f4bd1f20c21f0b3361120f34cf5c0feec849e\n"
        )
        failure = profile_setup.SetupFailure("command_failed", details={
            "apt_command": profile_setup._apt_command_diagnostics(
                "source_v2", "nonzero_exit", return_code=100,
                error_category="apt_cache_or_index_infrastructure_failure",
                stderr_bytes=91),
        })
        with mock.patch.object(profile_setup, "_apt_options", return_value=[]), \
                mock.patch.object(profile_setup, "_apt_config_path",
                                  return_value=Path("/run/acv1-contract-apt-" + "a" * 32 + "/apt.conf")), \
                mock.patch.object(profile_setup, "_command", side_effect=[
                (metadata.encode(), b""), failure]):
            with self.assertRaises(profile_setup.SetupFailure) as raised:
                profile_setup._package_record(
                    profile_setup.PROFILE_PACKAGE, profile_setup.PROFILE_VERSION,
                    profile_setup.PROFILE_DEB_SHA256, apt_transport=synthetic_apt_transport())
        self.assertEqual(raised.exception.details["apt_command"]["phase"], "source_v2")
        self.assertEqual(raised.exception.details["metadata_record"]["exact_candidate_count"], 1)
        serialized = json.dumps(raised.exception.details)
        self.assertNotIn("Failed to fetch", serialized)
        self.assertNotIn("mirror.invalid", serialized)

    def test_native_profile_setup_distinguishes_apt_get_release_target_failure(self) -> None:
        metadata = (
            "Package: apparmor-profiles\n"
            "Version: " + profile_setup.PROFILE_VERSION + "\n"
            "Architecture: all\n"
            "Filename: pool/main/a/apparmor/apparmor-profiles_" + profile_setup.PROFILE_VERSION
            + "_all.deb\n"
            "SHA256: " + profile_setup.PROFILE_DEB_SHA256 + "\n"
        )
        source = metadata.replace("Package: apparmor-profiles", "Package: apparmor-profiles:all") + (
            "APT-Sources: " + profile_setup.APT_MIRROR_URI
            + " noble-updates/main amd64 Packages\n")
        failure = profile_setup.SetupFailure("command_failed", details={
            "apt_command": profile_setup._apt_command_diagnostics(
                "index_targets", "nonzero_exit", return_code=100,
                error_category="apt_repository_or_index_infrastructure_failure",
                stderr_bytes=51),
        })
        with mock.patch.object(profile_setup, "_apt_options", return_value=[]), \
                mock.patch.object(profile_setup, "_apt_config_path",
                                  return_value=Path("/run/acv1-contract-apt-" + "a" * 32 + "/apt.conf")), \
                mock.patch.object(profile_setup, "_command", side_effect=[
                (metadata.encode(), b""), (source.encode(), b""), failure]):
            with self.assertRaises(profile_setup.SetupFailure) as raised:
                profile_setup._package_record(
                    profile_setup.PROFILE_PACKAGE, profile_setup.PROFILE_VERSION,
                    profile_setup.PROFILE_DEB_SHA256, apt_transport=synthetic_apt_transport())
        self.assertEqual(raised.exception.details["apt_command"]["tool"], "apt-get")
        self.assertEqual(raised.exception.details["apt_command"]["phase"], "index_targets")
        self.assertEqual(raised.exception.details["apt_command"]["return_code"], 100)
        diagnostic = json.dumps(raised.exception.details, sort_keys=True)
        self.assertNotIn("APT-Sources:", diagnostic)
        self.assertNotIn("noble-updates/main amd64 Packages", diagnostic)

    def test_native_profile_setup_publishes_only_sanitized_apt_failure_diagnostic(self) -> None:
        state = {
            "apparmor_enabled": "Y", "kernel_lsm": "apparmor", "sysctls": {},
            "profile_count": 1, "profile_table_sha256": "a" * 64, "profile_table": [],
        }
        failure = profile_setup.SetupFailure("command_failed", details={
            "apt_command": profile_setup._apt_command_diagnostics(
                "metadata_v1", "nonzero_exit", return_code=100,
                error_category="unsupported_option", stderr_bytes=96),
        })
        original_read_text = Path.read_text

        def read_text(path: Path, *args, **kwargs):
            if path == Path("/etc/os-release"):
                return 'ID=ubuntu\nVERSION_ID="24.04"\n'
            return original_read_text(path, *args, **kwargs)

        with tempfile.TemporaryDirectory(prefix="acv1-apt-diagnostic-") as temporary, \
                mock.patch.object(profile_setup.platform, "system", return_value="Linux"), \
                mock.patch.object(profile_setup.platform, "machine", return_value="x86_64"), \
                mock.patch.object(Path, "read_text", autospec=True, side_effect=read_text), \
                mock.patch.object(profile_setup, "_state_snapshot", return_value=state), \
                mock.patch.object(profile_setup, "_apt_toolchain_evidence", return_value={
                    "package": "apt", "package_version": "2.8.3", "apt_get_version": "2.8.3",
                    "supported_interface": "ubuntu_apt_2_7_14build2_or_2_8_3"}), \
                mock.patch.object(profile_setup, "_apt_signature_configuration", return_value={
                    "signed_repository_checks_enabled": True, "unsafe_overrides": []}), \
                mock.patch.object(profile_setup, "_runner_mirror_configuration",
                                  return_value=synthetic_apt_transport()), \
                mock.patch.object(profile_setup, "_apt_options", return_value=[]), \
                mock.patch.object(profile_setup, "_apt_sudo_prefix", return_value=[]), \
                mock.patch.object(profile_setup, "_quiet_command"), \
                mock.patch.object(profile_setup, "_package_record", side_effect=failure), \
                redirect_stdout(io.StringIO()) as captured, \
                redirect_stderr(io.StringIO()) as captured_error:
            evidence_path = Path(temporary) / "profile-evidence.json"
            exit_code = profile_setup.prepare(evidence_path)
            stdout = captured.getvalue()
            stderr = captured_error.getvalue()
            artifact = json.loads(evidence_path.read_text(encoding="utf-8"))
        logged = json.loads(stdout)
        self.assertEqual(exit_code, 1)
        self.assertEqual(artifact["status"], "blocked")
        self.assertEqual(artifact["stage"], "bubblewrap_package_record")
        self.assertEqual(artifact["apt_command_diagnostics"]["phase"], "metadata_v1")
        self.assertEqual(artifact["apt_command_diagnostics"]["return_code"], 100)
        self.assertEqual(logged["diagnostics"]["apt_command_diagnostics"]["error_category"],
                         "unsupported_option")
        self.assertEqual(stderr, "native profile setup blocked: command_failed\n")
        self.assertLessEqual(len(stdout.encode("utf-8")), 4096)

    def test_native_profile_setup_keeps_sanitized_diagnostics_when_source_is_unrecognized(self) -> None:
        metadata = (
            "Package: apparmor-profiles\n"
            "Version: 4.0.1really4.0.1-0ubuntu0.24.04.8\n"
            "Architecture: all\n"
            "Filename: pool/main/a/apparmor/apparmor-profiles_4.0.1really4.0.1-0ubuntu0.24.04.8_all.deb\n"
            "SHA256: 4e7d728322f899a7a06e71bedd4f4bd1f20c21f0b3361120f34cf5c0feec849e\n"
        )
        private_source = (
            "Package: apparmor-profiles:all\n"
            "Version: 4.0.1really4.0.1-0ubuntu0.24.04.8\n"
            "Filename: pool/main/a/apparmor/apparmor-profiles_4.0.1really4.0.1-0ubuntu0.24.04.8_all.deb\n"
            "SHA256: 4e7d728322f899a7a06e71bedd4f4bd1f20c21f0b3361120f34cf5c0feec849e\n"
            "APT-Sources: https://user:secret@azure.archive.ubuntu.com/private-token noble-updates/main amd64 Packages\n"
        )
        with self.assertRaises(profile_setup.SetupFailure) as raised:
            profile_setup.validate_apt_record(
                metadata, profile_setup.PROFILE_PACKAGE, profile_setup.PROFILE_VERSION,
                profile_setup.PROFILE_DEB_SHA256, source_text=private_source)
        self.assertEqual(raised.exception.code,
                         "signed_apt_package_origin_is_not_ubuntu_noble_updates")
        diagnostics = json.dumps(raised.exception.details, sort_keys=True)
        self.assertLessEqual(len(diagnostics), 2048)
        self.assertIn('"source_host_class": "ubuntu_archive"', diagnostics)
        self.assertNotIn("user", diagnostics)
        self.assertNotIn("secret", diagnostics)
        self.assertNotIn("private-token", diagnostics)

    def test_native_profile_setup_reports_bounded_shape_when_v2_source_is_missing(self) -> None:
        metadata = (
            "Package: apparmor-profiles\n"
            "Version: 4.0.1really4.0.1-0ubuntu0.24.04.8\n"
            "Architecture: all\n"
            "Filename: pool/main/a/apparmor/apparmor-profiles_4.0.1really4.0.1-0ubuntu0.24.04.8_all.deb\n"
            "SHA256: 4e7d728322f899a7a06e71bedd4f4bd1f20c21f0b3361120f34cf5c0feec849e\n"
        )
        source_record = (
            "Package: apparmor-profiles:all\n"
            "Version: 4.0.1really4.0.1-0ubuntu0.24.04.8\n"
            "Filename: pool/main/a/apparmor/apparmor-profiles_4.0.1really4.0.1-0ubuntu0.24.04.8_all.deb\n"
            "SHA256: 4e7d728322f899a7a06e71bedd4f4bd1f20c21f0b3361120f34cf5c0feec849e\n"
        )
        with mock.patch.object(profile_setup, "_apt_options", return_value=[]), \
                mock.patch.object(profile_setup, "_apt_config_path",
                                  return_value=Path("/run/acv1-contract-apt-" + "a" * 32 + "/apt.conf")), \
                mock.patch.object(profile_setup, "_command", side_effect=[
                (metadata.encode(), b""), (source_record.encode(), b"")]):
            with self.assertRaisesRegex(profile_setup.SetupFailure,
                                        "signed_apt_package_origin_missing") as raised:
                profile_setup._package_record(
                    profile_setup.PROFILE_PACKAGE, profile_setup.PROFILE_VERSION,
                    profile_setup.PROFILE_DEB_SHA256, apt_transport=synthetic_apt_transport())
        self.assertEqual(raised.exception.details["metadata_record"]["exact_candidate_count"], 1)
        self.assertEqual(raised.exception.details["source_record"]["fields_present"],
                         ["Package", "Version", "Filename", "SHA256"])
        self.assertEqual(raised.exception.details["source_record"]["source_host_class"], "missing")

    def test_native_profile_setup_requires_matching_exact_v2_package_identity(self) -> None:
        metadata = (
            "Package: bubblewrap\n"
            "Version: 0.9.0-1ubuntu0.3\n"
            "Architecture: amd64\n"
            "Filename: pool/main/b/bubblewrap/bubblewrap_0.9.0-1ubuntu0.3_amd64.deb\n"
            "SHA256: " + "a" * 64 + "\n"
        )
        for package_field in ("bubblewrap:arm64", "bubblewrap:amd64"):
            source_record = (
                "Package: " + package_field + "\n"
                "Version: 0.9.0-1ubuntu0.3\n"
                "Filename: pool/main/b/bubblewrap/bubblewrap_0.9.0-1ubuntu0.3_amd64.deb\n"
                "SHA256: " + "a" * 64 + "\n"
                "APT-Sources: http://azure.archive.ubuntu.com/ubuntu noble-updates/main amd64 Packages\n"
            )
            with self.subTest(package_field=package_field):
                if package_field == "bubblewrap:amd64":
                    profile_setup.validate_apt_record(
                        metadata, "bubblewrap", "0.9.0-1ubuntu0.3", "a" * 64,
                        source_text=source_record)
                else:
                    with self.assertRaisesRegex(profile_setup.SetupFailure,
                                                "exact_signed_apt_source_record_missing_or_ambiguous"):
                        profile_setup.validate_apt_record(
                            metadata, "bubblewrap", "0.9.0-1ubuntu0.3", "a" * 64,
                            source_text=source_record)
        mismatched_source = source_record.replace("a" * 64, "b" * 64)
        with self.assertRaisesRegex(profile_setup.SetupFailure,
                                    "signed_apt_source_record_metadata_mismatch"):
            profile_setup.validate_apt_record(
                metadata, "bubblewrap", "0.9.0-1ubuntu0.3", "a" * 64,
                source_text=mismatched_source)

    def test_native_profile_setup_rejects_foreign_stale_or_unpinned_apt_record(self) -> None:
        record = (
            "Package: apparmor-profiles\n"
            "Version: 4.0.1really4.0.1-0ubuntu0.24.04.8\n"
            "Architecture: all\n"
            "Filename: pool/main/a/apparmor/apparmor-profiles_4.0.1really4.0.1-0ubuntu0.24.04.8_all.deb\n"
            "SHA256: 4e7d728322f899a7a06e71bedd4f4bd1f20c21f0b3361120f34cf5c0feec849e\n"
            "APT-Sources: https://archive.ubuntu.com/ubuntu noble-updates/main amd64 Packages\n"
        )
        for replacement in (
            ("0ubuntu0.24.04.8", "0ubuntu0.24.04.7"),
            (profile_setup.PROFILE_DEB_SHA256, "0" * 64),
            ("archive.ubuntu.com", "packages.example.invalid"),
            ("archive.ubuntu.com/ubuntu", "archive.ubuntu.com:invalid/ubuntu"),
            ("noble-updates/main", "noble/main"),
            ("noble-updates/main amd64 Packages", "noble-updates/main amd64 Sources"),
            ("pool/main/a/apparmor/", "pool/universe/a/apparmor/"),
        ):
            with self.subTest(replacement=replacement[1]):
                candidate = record.replace(*replacement)
                with self.assertRaises(profile_setup.SetupFailure):
                    profile_setup.validate_apt_record(
                        candidate, profile_setup.PROFILE_PACKAGE, profile_setup.PROFILE_VERSION,
                        profile_setup.PROFILE_DEB_SHA256, source_text=candidate)

    def test_native_profile_setup_rejects_untrusted_apt_configuration_overrides(self) -> None:
        with mock.patch.dict(profile_setup.os.environ, {"APT_CONFIG": "/tmp/unreviewed-apt.conf"}):
            with self.assertRaisesRegex(profile_setup.SetupFailure,
                                        "external_apt_configuration_override_unverified"):
                profile_setup._apt_signature_configuration()
        with mock.patch.object(profile_setup, "_command", return_value=(
                b'Acquire::AllowWeakRepositories "true";\n', b"")), \
                mock.patch.object(profile_setup, "_read_apt_source_files", return_value={}):
            with self.assertRaisesRegex(profile_setup.SetupFailure,
                                        "apt_source_configuration_bypasses_signature_checks"):
                profile_setup._apt_signature_configuration()

    def test_apt_boolean_parser_matches_noble_string_to_bool_aliases_and_numeric_forms(self) -> None:
        for value in ("yes", "true", "with", "on", "enable", "YES", "TrUe", "ON",
                      "1", "01", "0x1", "+1", "0X01"):
            with self.subTest(value=value):
                self.assertIs(profile_setup._apt_boolean_value(value), True)
        for value in ("no", "false", "without", "off", "disable", "NO", "FaLsE",
                      "0", "00", "0x0", "+0", "-0"):
            with self.subTest(value=value):
                self.assertIs(profile_setup._apt_boolean_value(value), False)
        for value in ("", "enabled", "2", "-1", "0x2", "0b1", "perhaps"):
            with self.subTest(value=value):
                self.assertIsNone(profile_setup._apt_boolean_value(value))

    def test_apt_security_audit_rejects_source_aliases_and_disabled_freshness(self) -> None:
        true_spellings = ("true", "1", "on", "enable", "with", "YES", "0x1")
        source_boolean_options = (
            ("Trusted", "trusted"), ("Allow-Insecure", "allow-insecure"),
            ("Allow-Weak", "allow-weak"),
            ("Allow-Downgrade-To-Insecure", "allow-downgrade-to-insecure"),
        )
        for value in true_spellings:
            for deb822_field, one_line_option in source_boolean_options:
                source = SYNTHETIC_UBUNTU_SOURCES + f"{deb822_field}: {value}\n"
                with self.subTest(source_field=deb822_field, source_value=value), self.assertRaisesRegex(
                        profile_setup.SetupFailure,
                        "apt_source_configuration_bypasses_signature_checks"):
                    profile_setup._validate_apt_security_configuration(
                        "", {"ubuntu.sources": source})
                one_line = (
                    f"deb [{one_line_option}=\"{value}\"] "
                    "https://archive.ubuntu.com/ubuntu noble main\n")
                with self.subTest(one_line_option=one_line_option, one_line_value=value), \
                        self.assertRaisesRegex(profile_setup.SetupFailure,
                                               "apt_source_configuration_bypasses_signature_checks"):
                    profile_setup._validate_apt_security_configuration(
                        "", {"archive.list": one_line})
        for field in ("Check-Valid-Until", "Check-Date"):
            for value in ("no", "false", "without", "off", "disable", "0", "00", "0x0"):
                source = SYNTHETIC_UBUNTU_SOURCES + f"{field}: {value}\n"
                with self.subTest(field=field, value=value), self.assertRaisesRegex(
                        profile_setup.SetupFailure,
                        "apt_source_configuration_disables_freshness_checks"):
                    profile_setup._validate_apt_security_configuration(
                        "", {"ubuntu.sources": source})
                option = field.casefold()
                one_line = (
                    f"deb [{option}=\"{value}\"] https://archive.ubuntu.com/ubuntu noble main\n")
                with self.subTest(one_line_option=option, one_line_value=value), \
                        self.assertRaisesRegex(
                            profile_setup.SetupFailure,
                            "apt_source_configuration_disables_freshness_checks"):
                    profile_setup._validate_apt_security_configuration(
                        "", {"archive.list": one_line})

    def test_apt_security_audit_checks_global_aliases_defaults_and_duplicate_values(self) -> None:
        for key, policy in profile_setup.APT_SECURITY_CONFIG_KEYS.items():
            if policy == "reject_true":
                values = ("true", "1", "on", "enable", "with", "YES", "0x1")
                expected = "apt_source_configuration_bypasses_signature_checks"
            else:
                values = ("false", "0", "off", "disable", "without", "NO", "0x0")
                expected = "apt_source_configuration_disables_freshness_checks"
            for value in values:
                with self.subTest(key=key, value=value), self.assertRaisesRegex(
                        profile_setup.SetupFailure, expected):
                    profile_setup._validate_apt_security_configuration(
                        f'{key} "{value}";\n', {})
        for text in (
            'Acquire::AllowWeakRepositories "maybe";\n',
            'Acquire::Check-Date "2";\n',
            'Acquire::Check-Date "true";\nAcquire::Check-Date "false";\n',
            'Dir::Etc::sourceparts "/tmp/foreign-sources";\n',
        ):
            with self.subTest(config=text), self.assertRaises(profile_setup.SetupFailure):
                profile_setup._validate_apt_security_configuration(text, {})
        safe = (
            'Acquire::AllowWeakRepositories "without";\n'
            'Acquire::AllowInsecureRepositories "0";\n'
            'Acquire::Check-Date "enable";\n'
            'Acquire::Check-Valid-Until "1";\n'
        )
        evidence = profile_setup._validate_apt_security_configuration(safe, {})
        self.assertTrue(evidence["signed_repository_checks_enabled"])
        self.assertTrue(evidence["freshness_checks_enabled"])

    def test_apt_security_audit_rejects_malformed_and_duplicate_source_options(self) -> None:
        valid_line = (
            "deb [trusted=no allow-insecure=0 allow-weak=off "
            "allow-downgrade-to-insecure=disable check-valid-until=yes check-date=1] "
            "https://archive.ubuntu.com/ubuntu noble main\n"
        )
        self.assertTrue(profile_setup._validate_apt_security_configuration(
            "", {"sources.list": valid_line})["signed_repository_checks_enabled"])
        invalid = (
            "deb [trusted=perhaps] https://archive.ubuntu.com/ubuntu noble main\n",
            "deb [trusted=no trusted=false] https://archive.ubuntu.com/ubuntu noble main\n",
            "deb [trusted+=no] https://archive.ubuntu.com/ubuntu noble main\n",
            "deb [trusted] https://archive.ubuntu.com/ubuntu noble main\n",
            "deb [trusted=on https://archive.ubuntu.com/ubuntu noble main\n",
        )
        for text in invalid:
            with self.subTest(text=text), self.assertRaises(profile_setup.SetupFailure):
                profile_setup._validate_apt_security_configuration("", {"sources.list": text})
        duplicate_deb822 = SYNTHETIC_UBUNTU_SOURCES + (
            "Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg\n")
        with self.assertRaisesRegex(profile_setup.SetupFailure,
                                    "apt_source_configuration_invalid"):
            profile_setup._validate_apt_security_configuration(
                "", {"ubuntu.sources": duplicate_deb822})

    def test_native_profile_setup_keeps_source_split_strict_and_rejects_ambiguous_targets(self) -> None:
        valid = SYNTHETIC_UBUNTU_SOURCES.encode("utf-8")
        self.assertEqual(profile_setup._validate_runner_mirror_configuration(
            valid, profile_setup.APT_MIRRORLIST_BYTES)["mirror_source_stanza_count"], 2)
        for changed in (
            SYNTHETIC_UBUNTU_SOURCES + SYNTHETIC_UBUNTU_SOURCES.split("\n\n", 1)[0],
            SYNTHETIC_UBUNTU_SOURCES.replace("noble-security", "noble-updates"),
            SYNTHETIC_UBUNTU_SOURCES.replace(
                "mirror+file:/etc/apt/apt-mirrors.txt",
                "mirror+file:/etc/apt/apt-mirrors.txt https://extra.invalid/ubuntu"),
        ):
            with self.subTest(changed=changed), self.assertRaises(profile_setup.SetupFailure):
                profile_setup._validate_runner_mirror_configuration(
                    changed.encode(), profile_setup.APT_MIRRORLIST_BYTES)
        source = profile_setup.APT_MIRROR_URI + " noble-updates/main amd64 Packages"
        ambiguous = synthetic_apt_index_target() + synthetic_apt_index_target()
        with self.assertRaisesRegex(profile_setup.SetupFailure,
                                    "apt_signed_release_target_missing_or_ambiguous"):
            profile_setup._validate_signed_apt_index_target(
                ambiguous, source, synthetic_apt_transport())

    def test_native_profile_setup_supported_indextargets_format_omits_unavailable_signed_by(self) -> None:
        variables = set(re.findall(r"\$\(([A-Z_]+)\)", profile_setup.APT_INDEXTARGETS_FORMAT))
        self.assertEqual(variables, {
            "IDENTIFIER", "METAKEY", "TARGET_OF", "ARCHITECTURE", "COMPONENT",
            "SUITE", "CODENAME", "ORIGIN", "LABEL", "TRUSTED", "REPO_URI", "URI",
        })
        self.assertNotIn("SIGNED_BY", variables)
        output = synthetic_apt_index_target()
        self.assertNotIn("$(", output)
        self.assertNotIn("Signed-By:", output)
        source = profile_setup.APT_MIRROR_URI + " noble-updates/main amd64 Packages"
        for package_version in profile_setup.APT_SUPPORTED_PACKAGE_VERSIONS:
            transport = synthetic_apt_transport()
            transport["apt_package_version"] = package_version
            evidence = profile_setup._validate_signed_apt_index_target(
                output, source, transport)
            self.assertEqual(evidence["signer_binding"], "root_owned_source_stanza")
            self.assertEqual(evidence["apt_package_version"], package_version)

    def test_native_profile_setup_rejects_unexpanded_index_fields_and_missing_keyring_binding(self) -> None:
        source = profile_setup.APT_MIRROR_URI + " noble-updates/main amd64 Packages"
        with self.assertRaisesRegex(profile_setup.SetupFailure,
                                    "apt_signed_release_target_missing_or_ambiguous"):
            profile_setup._validate_signed_apt_index_target(
                synthetic_apt_index_target(Architecture="$(SIGNED_BY)"), source,
                synthetic_apt_transport())
        with self.assertRaisesRegex(profile_setup.SetupFailure,
                                    "apt_signed_release_target_unexpanded_format_variable"):
            profile_setup._validate_signed_apt_index_target(
                synthetic_apt_index_target(Origin="$(SIGNED_BY)"), source,
                synthetic_apt_transport())
        missing = synthetic_apt_transport()
        missing["ubuntu_archive_keyring_sha256"] = "unverified"
        with self.assertRaisesRegex(profile_setup.SetupFailure,
                                    "apt_signed_release_source_keyring_binding_missing"):
            profile_setup._validate_signed_apt_index_target(
                synthetic_apt_index_target(), source, missing)

    def test_native_profile_setup_records_only_supported_apt_package_and_cli_versions(self) -> None:
        for package_version, cli_version in profile_setup.APT_SUPPORTED_PACKAGE_VERSIONS.items():
            with self.subTest(package_version=package_version), \
                    mock.patch.object(profile_setup.shutil, "which", side_effect=lambda name: {
                        "apt-get": profile_setup.APT_GET_PATH,
                        "apt-cache": profile_setup.APT_CACHE_PATH,
                        "apt-config": profile_setup.APT_CONFIG_PATH,
                    }[name]), \
                    mock.patch.object(profile_setup, "_owner_of", return_value="apt"), \
                    mock.patch.object(profile_setup, "_package_version", return_value=package_version), \
                    mock.patch.object(profile_setup, "_verify_package"), \
                    mock.patch.object(profile_setup, "_sha256", return_value="a" * 64), \
                    mock.patch.object(profile_setup, "_version", return_value=cli_version):
                evidence = profile_setup._apt_toolchain_evidence()
            self.assertEqual(evidence["package_version"], package_version)
            self.assertEqual(evidence["apt_get_version"], cli_version)
        with mock.patch.object(profile_setup.shutil, "which", side_effect=lambda name: {
                    "apt-get": profile_setup.APT_GET_PATH,
                    "apt-cache": profile_setup.APT_CACHE_PATH,
                    "apt-config": profile_setup.APT_CONFIG_PATH,
                }[name]), \
                mock.patch.object(profile_setup, "_owner_of", return_value="apt"), \
                mock.patch.object(profile_setup, "_package_version", return_value="3.0.0"), \
                mock.patch.object(profile_setup, "_verify_package"):
            with self.assertRaisesRegex(profile_setup.SetupFailure, "apt_package_version_unsupported"):
                profile_setup._apt_toolchain_evidence()

        with mock.patch.object(profile_setup.shutil, "which", side_effect=lambda name: {
                    "apt-get": profile_setup.APT_GET_PATH,
                    "apt-cache": profile_setup.APT_CACHE_PATH,
                    "apt-config": profile_setup.APT_CONFIG_PATH,
                }[name]), \
                mock.patch.object(profile_setup, "_owner_of", return_value="apt"), \
                mock.patch.object(profile_setup, "_package_version", return_value="2.8.3"), \
                mock.patch.object(profile_setup, "_verify_package"), \
                mock.patch.object(profile_setup, "_sha256", return_value="a" * 64), \
                mock.patch.object(profile_setup, "_version", return_value="2.8.4"):
            with self.assertRaisesRegex(profile_setup.SetupFailure, "apt_cli_package_version_mismatch"):
                profile_setup._apt_toolchain_evidence()

    def test_apt_cache_records_accept_descriptive_multiline_fields_and_reject_identity_duplicates(self) -> None:
        version = profile_setup.PROFILE_VERSION
        metadata = (
            "Package: apparmor-profiles\n"
            f"Version: {version}\n"
            "Architecture: all\n"
            f"Filename: pool/main/a/apparmor/apparmor-profiles_{version}_all.deb\n"
            f"SHA256: {profile_setup.PROFILE_DEB_SHA256}\n"
            "Depends: bubblewrap,\n apparmor\n"
            "Description: profile bundle summary\n continuation detail\n .\n final detail\n"
        )
        source_record = (
            "Package: apparmor-profiles:all\n"
            f"Version: {version}\n"
            f"Filename: pool/main/a/apparmor/apparmor-profiles_{version}_all.deb\n"
            f"SHA256: {profile_setup.PROFILE_DEB_SHA256}\n"
            "APT-Sources: mirror+file:/etc/apt/apt-mirrors.txt noble-updates/main amd64 Packages\n"
            "Description: source metadata may contain continuation lines\n extra detail\n"
        )
        parsed = profile_setup.validate_apt_record(
            metadata, profile_setup.PROFILE_PACKAGE, version,
            profile_setup.PROFILE_DEB_SHA256, source_text=source_record)
        self.assertEqual(parsed["Architecture"], "all")
        self.assertEqual(parsed["SHA256"], profile_setup.PROFILE_DEB_SHA256)

        identity_lines = {
            "Package": "apparmor-profiles", "Version": version,
            "Architecture": "all",
            "Filename": f"pool/main/a/apparmor/apparmor-profiles_{version}_all.deb",
            "SHA256": profile_setup.PROFILE_DEB_SHA256,
            "APT-Sources": "mirror+file:/etc/apt/apt-mirrors.txt noble-updates/main amd64 Packages",
        }
        for field, value in identity_lines.items():
            metadata_fields = 1 if field.lower() in {"package", "version", "architecture", "filename", "sha256"} else 0
            bad_metadata = metadata + (f"{field}: {value}\n" * (2 if metadata_fields == 0 else 1))
            with self.subTest(field=field), self.assertRaisesRegex(
                    profile_setup.SetupFailure, "apt_package_record_invalid"):
                profile_setup.validate_apt_record(
                    bad_metadata, profile_setup.PROFILE_PACKAGE, version,
                    profile_setup.PROFILE_DEB_SHA256, source_text=source_record)
            folded_metadata = metadata + f"{field}: {value}\n folded\n"
            with self.subTest(folded_field=field), self.assertRaisesRegex(
                    profile_setup.SetupFailure, "apt_package_record_invalid"):
                profile_setup.validate_apt_record(
                    folded_metadata, profile_setup.PROFILE_PACKAGE, version,
                    profile_setup.PROFILE_DEB_SHA256, source_text=source_record)
            source_fields = 1 if field.lower() in {
                "package", "version", "filename", "sha256", "apt-sources"} else 0
            bad_source = source_record + (f"{field}: {value}\n" * (2 if source_fields == 0 else 1))
            with self.subTest(source_field=field), self.assertRaises(profile_setup.SetupFailure):
                profile_setup.validate_apt_record(
                    metadata, profile_setup.PROFILE_PACKAGE, version,
                    profile_setup.PROFILE_DEB_SHA256, source_text=bad_source)

        with self.assertRaisesRegex(profile_setup.SetupFailure,
                                    "exact_signed_apt_package_record_missing_or_ambiguous"):
            profile_setup.validate_apt_record(
                metadata + "\n" + metadata, profile_setup.PROFILE_PACKAGE, version,
                profile_setup.PROFILE_DEB_SHA256, source_text=source_record)
        oversized = "Description: " + ("x" * (128 * 1024)) + "\n" + metadata
        with self.assertRaisesRegex(profile_setup.SetupFailure, "apt_package_record_invalid"):
            profile_setup.validate_apt_record(
                oversized, profile_setup.PROFILE_PACKAGE, version,
                profile_setup.PROFILE_DEB_SHA256, source_text=source_record)

    def test_native_profile_setup_rejects_any_unreviewed_local_include(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-local-profile-override-") as temp:
            first = Path(temp) / "bwrap-userns-restrict"
            second = Path(temp) / "unpriv_bwrap"
            with mock.patch.object(profile_setup, "LOCAL_OVERRIDES", (first, second)):
                self.assertEqual(profile_setup._check_local_overrides(), ["absent", "absent"])
                first.write_bytes(b"")
                with self.assertRaises(profile_setup.SetupFailure):
                    profile_setup._check_local_overrides()
                first.unlink()
                second.symlink_to(first.name)
                with self.assertRaises(profile_setup.SetupFailure):
                    profile_setup._check_local_overrides()

    def test_native_profile_status_uses_only_bounded_fixed_privileged_read_on_permission_denial(self) -> None:
        original_open = Path.open

        def deny_status_open(path: Path, *args, **kwargs):
            if path == profile_setup.APPARMOR_PROFILES:
                raise PermissionError("synthetic securityfs permission denial")
            return original_open(path, *args, **kwargs)

        expected_status = b"kernel-profile-a (enforce)\nkernel-profile-b (enforce)\n"
        with mock.patch.object(Path, "open", autospec=True, side_effect=deny_status_open), \
                mock.patch.object(profile_setup, "_command", return_value=(expected_status, b"")) as command:
            self.assertEqual(profile_setup._read_profiles(), [
                "kernel-profile-a (enforce)", "kernel-profile-b (enforce)"])
        command.assert_called_once_with(
            ["/usr/bin/sudo", "-n", "--", "/usr/bin/head", "-c",
             str(profile_setup.PROFILE_TABLE_MAX_BYTES + 1), "--", "/sys/kernel/security/apparmor/profiles"],
            timeout=5, limit=profile_setup.PROFILE_TABLE_MAX_BYTES, cwd=Path("/"))

    def test_native_profile_status_avoids_privilege_when_unprivileged_read_works(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-profile-status-readable-") as temp:
            status_path = Path(temp) / "profiles"
            status_path.write_text("profile-one (enforce)\n", encoding="utf-8")
            with mock.patch.object(profile_setup, "APPARMOR_PROFILES", status_path), \
                    mock.patch.object(profile_setup, "_command") as command:
                self.assertEqual(profile_setup._read_profiles(), ["profile-one (enforce)"])
            command.assert_not_called()

    def test_native_profile_status_does_not_escalate_for_nonpermission_io_errors(self) -> None:
        original_open = Path.open

        def missing_status_open(path: Path, *args, **kwargs):
            if path == profile_setup.APPARMOR_PROFILES:
                raise FileNotFoundError("synthetic missing securityfs entry")
            return original_open(path, *args, **kwargs)

        with mock.patch.object(Path, "open", autospec=True, side_effect=missing_status_open), \
                mock.patch.object(profile_setup, "_command") as command:
            with self.assertRaisesRegex(profile_setup.SetupFailure,
                                        "apparmor_kernel_profile_status_unavailable"):
                profile_setup._read_profiles()
            command.assert_not_called()

    def test_native_profile_status_overflow_fails_closed(self) -> None:
        original_open = Path.open

        def deny_status_open(path: Path, *args, **kwargs):
            if path == profile_setup.APPARMOR_PROFILES:
                raise PermissionError("synthetic securityfs permission denial")
            return original_open(path, *args, **kwargs)

        with mock.patch.object(Path, "open", autospec=True, side_effect=deny_status_open), \
                mock.patch.object(profile_setup, "_command",
                                  side_effect=profile_setup.SetupFailure("command_output_exceeded_bound")):
            with self.assertRaisesRegex(profile_setup.SetupFailure,
                                        "apparmor_kernel_profile_status_exceeded_bound"):
                profile_setup._read_profiles()

    def test_native_profile_status_still_fails_closed_if_fixed_privileged_read_fails(self) -> None:
        original_open = Path.open

        def deny_status_open(path: Path, *args, **kwargs):
            if path == profile_setup.APPARMOR_PROFILES:
                raise PermissionError("synthetic securityfs permission denial")
            return original_open(path, *args, **kwargs)

        with mock.patch.object(Path, "open", autospec=True, side_effect=deny_status_open), \
                mock.patch.object(profile_setup, "_command", side_effect=profile_setup.SetupFailure("command_failed")) as command:
            with self.assertRaisesRegex(profile_setup.SetupFailure,
                                        "apparmor_kernel_profile_status_privileged_read_failed"):
                profile_setup._read_profiles()
        command.assert_called_once()

    def test_sandbox_command_imports_only_declared_sealed_dependency_root(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-dependency-import-root-") as temp, \
                mock.patch.object(contracts.platform, "system", return_value="Linux"), \
                mock.patch.object(contracts.shutil, "which", return_value="/usr/bin/bwrap"), \
                mock.patch.object(contracts.sys, "executable", "/usr/bin/python3"):
            root = Path(temp)
            snapshot = contracts.SealedSnapshot({
                "adapter.py": 101,
                "declared_dep.py": 102,
                "fixtures/unlisted_module.py": 103,
                "schemas/schema.json": 104,
            })
            command, error = contracts._sandbox_command(
                root, root / "adapter.py", b"{}", snapshot, ("declared_dep.py",))
            self.assertIsNone(error)
            self.assertIsNotNone(command)
            assert command is not None
            bootstrap = command[command.index("-c") + 1]
            self.assertEqual(command[command.index("-I") + 1:command.index("-I") + 4], ["-B", "-S", "-c"])
            self.assertIn("sys.path.insert(0, '/acv1-dependencies')", bootstrap)
            self.assertNotIn("sys.path.insert(0, '/release')", bootstrap)
            self.assertIn("/acv1-dependencies/declared_dep.py", command)
            self.assertNotIn("/release/declared_dep.py", command)
            self.assertIn("/release/fixtures/unlisted_module.py", command)
            self.assertNotIn("/acv1-dependencies/fixtures/unlisted_module.py", command)
            missing, missing_error = contracts._sandbox_command(
                root, root / "adapter.py", b"{}", snapshot, ("unlisted_module.py",))
            self.assertIsNone(missing)
            self.assertIn("absent or outside", missing_error or "")

    def test_native_cli_runs_complete_multischema_synthetic_release_through_sealed_sandbox(self) -> None:
        self.require_native_sandbox()
        with tempfile.TemporaryDirectory(prefix="acv1-contract-cli-sandbox-") as temp:
            manifest_path, evidence_path, evidence_sha = write_complete_synthetic_contract(Path(temp))
            result = subprocess.run(
                [sys.executable, str(ROOT / "tools/verify_contract_release.py"),
                 "--manifest", str(manifest_path), "--offline", "--test-only-synthetic",
                 "--owner-evidence-index", str(evidence_path), "--owner-evidence-sha256", evidence_sha],
                cwd=ROOT,
                env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "PYTHONDONTWRITEBYTECODE": "1",
                     "LC_ALL": "C.UTF-8", "PYTHONHASHSEED": "0"},
                text=True, capture_output=True, check=False, timeout=90,
            )
        self.assertEqual(result.returncode, 0, "synthetic runner CLI failed with sanitized status: " + result.stdout[:2000])
        report = json.loads(result.stdout)
        self.assertEqual(report["status"], "synthetic_test_only_validation_passed")
        self.assertFalse(report["ready"])
        self.assertFalse(report["owner_evidence_verified"])
        self.assertFalse(report["result"]["ready"])
        self.assertFalse(report["result"]["owner_evidence_verified"])
        self.assertIn("TEST ONLY", report["result"]["scope"])
        self.assertEqual(report["result"]["validated_fixture_cases"], len(contracts.REQUIRED_CASES))
        self.assertEqual(len(report["result"]["schema_sha256"]), 4)
        self.assertEqual(report["result"]["execution_binding"],
                         "sealed memfd snapshot exposed through read-only bubblewrap binds")

    def test_native_sandbox_executes_sealed_adapter_schema_fixture_and_dependency_after_path_swaps(self) -> None:
        self.require_native_sandbox()
        with tempfile.TemporaryDirectory(prefix="acv1-sealed-snapshot-race-") as temp:
            root = Path(temp)
            originals = {
                "schema.json": (
                    b'{"$schema":"https://json-schema.org/draft/2020-12/schema",'
                    b'"type":"object","properties":{"value":{"type":"integer"}},'
                    b'"additionalProperties":false}'
                ),
                "fixture.json": b'{"value":7}',
                "snapshot_dep.py": (
                    b"import hashlib\nfrom pathlib import Path\n"
                    b"def digest(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()\n"
                    b"def observe():\n"
                    b" root=Path('/release')\n"
                    b" import importlib.util\n"
                    b" return {'dependency':digest(__file__),'adapter':digest(root/'adapter.py'),"
                    b"'schema':digest(root/'schema.json'),'fixture':digest(root/'fixture.json'),"
                    b"'undeclared_blocked':importlib.util.find_spec('acv1_unlisted_dependency_probe') is None}\n"
                ),
                "adapter.py": (
                    b"import json\nfrom snapshot_dep import observe\n"
                    b"print(json.dumps(observe(),sort_keys=True,separators=(',',':')))\n"
                ),
            }
            for name, raw in originals.items():
                (root / name).write_bytes(raw)
            (root / "acv1_unlisted_dependency_probe.py").write_text(
                "raise RuntimeError('undeclared host module was imported')\n", encoding="utf-8")
            expected_observation = {
                "dependency": sha(originals["snapshot_dep.py"]),
                "adapter": sha(originals["adapter.py"]),
                "schema": sha(originals["schema.json"]),
                "fixture": sha(originals["fixture.json"]),
                "undeclared_blocked": True,
            }
            replacements = {
                "adapter.py": (b"print('replacement adapter executed')\n", False),
                "schema.json": (b'{"type":"object","properties":{"wrong":{"type":"string"}}}', True),
                "fixture.json": (b'{"value":999}', True),
                "snapshot_dep.py": (b"def observe(): return {'dependency':'replacement'}\n", True),
            }
            for name, (replacement, use_symlink) in replacements.items():
                snapshot_inputs = {path: (raw, sha(raw)) for path, raw in originals.items()}
                snapshot, snapshot_error = contracts._make_sealed_snapshot(snapshot_inputs)
                self.assertIsNone(snapshot_error)
                self.assertIsNotNone(snapshot)
                assert snapshot is not None
                target = root / name
                backup = root / (name + ".authentic")
                evil = root / (name + ".replacement")
                dependency_request = json.dumps({"dependencies": [{
                    "path": contracts.DEPENDENCY_IMPORT_ROOT + "/snapshot_dep.py",
                    "sha256": sha(originals["snapshot_dep.py"]),
                }]}).encode("utf-8")
                command, command_error = contracts._sandbox_command(
                    root, root / "adapter.py", dependency_request, snapshot, ("snapshot_dep.py",))
                self.assertIsNotNone(command)
                self.assertIsNone(command_error)
                try:
                    os.replace(target, backup)
                    evil.write_bytes(replacement)
                    if use_symlink:
                        target.symlink_to(evil.name)
                    else:
                        target.write_bytes(replacement)
                    run = contracts.run_bounded(
                        command or [], cwd=Path("/"),
                        env={"PATH": "/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1", "LC_ALL": "C.UTF-8"},
                        input_bytes=b"", timeout=10, stdout_limit=4096, stderr_limit=4096,
                        pass_fds=snapshot.pass_fds)
                finally:
                    if target.is_symlink() or target.exists():
                        target.unlink()
                    if backup.exists():
                        os.replace(backup, target)
                    snapshot.close()
                self.assertTrue(run["ok"], "native sandbox rejected sealed input snapshot")
                self.assertEqual(json.loads(run["stdout"].decode("utf-8")), expected_observation)
                self.assertEqual(target.read_bytes(), originals[name])
                self.assertTrue(evil.exists())

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

    def run_production_shaped_synthetic_evidence(self, root: Path, *, index_classification: str | None,
                                                 document_classification: str | None = None
                                                 ) -> subprocess.CompletedProcess[str]:
        targets, dataset = complete_synthetic_plan()
        targets["status"] = "approved_for_evaluation_preparation"
        dataset["status"] = "approved"
        targets["metering_stage_inventory"]["status"] = "published"
        targets["metering_stage_inventory"]["decision_ref"] = (
            f"https://github.com/{jev.OWNER_ORG}/cost-intelligence/issues/1")
        for question in targets["questions"]:
            repository = question["definition_owner"]["repository"]
            question["definition_owner"]["issue_url"] = (
                f"https://github.com/{jev.OWNER_ORG}/{repository}/issues/1")
        entries = []
        for index, (_path, approval, role, repository, subject_id, subject_hash) in enumerate(
                jev.approval_subjects(targets, dataset)):
            if not isinstance(approval, dict):
                continue
            reference = f"https://github.com/{jev.OWNER_ORG}/{repository}/issues/1"
            evidence_id = f"TEST-ONLY-{index}"
            revision = "a" * 40
            approval.update({"status": "accepted", "owner_role": role, "owner_repository": repository,
                             "revision": revision, "decision_ref": reference, "subject_id": subject_id,
                             "subject_sha256": subject_hash, "evidence_id": evidence_id})
            document = {"format": jev.OWNER_EVIDENCE_FORMAT, "evidence_id": evidence_id,
                        "owner": repository, "repository": repository, "revision": revision,
                        "decision_ref": reference, "status": "accepted", "subjects": {subject_id: subject_hash}}
            if document_classification is not None and not entries:
                document["classification"] = document_classification
            document_bytes = json.dumps(document, sort_keys=True, separators=(",", ":"),
                                        ensure_ascii=False).encode("utf-8")
            document_name = evidence_id + ".json"
            (root / document_name).write_bytes(document_bytes)
            entries.append({**{key: value for key, value in document.items()
                               if key not in {"format", "classification"}},
                            "evidence_path": document_name, "evidence_sha256": sha(document_bytes)})
        index = {"format": jev.OWNER_EVIDENCE_INDEX_FORMAT, "evidence": entries}
        if index_classification is not None:
            index["classification"] = index_classification
        index_bytes = json.dumps(index, sort_keys=True, separators=(",", ":"),
                                 ensure_ascii=False).encode("utf-8")
        index_path = root / "index.json"
        index_path.write_bytes(index_bytes)
        targets_path = root / "targets.json"
        targets_path.write_text(json.dumps(targets, sort_keys=True, separators=(",", ":")), encoding="utf-8")
        dataset_path = root / "dataset.json"
        dataset_path.write_text(json.dumps(dataset, sort_keys=True, separators=(",", ":")), encoding="utf-8")
        return subprocess.run(
            [sys.executable, str(ROOT / "tools/validate_jev_targets.py"),
             "--targets", str(targets_path), "--dataset", str(dataset_path),
             "--owner-evidence-index", str(index_path), "--owner-evidence-sha256", sha(index_bytes)],
            cwd=ROOT, env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                           "PYTHONDONTWRITEBYTECODE": "1", "LC_ALL": "C.UTF-8", "PYTHONHASHSEED": "0"},
            text=True, capture_output=True, check=False, timeout=10,
        )

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

    def test_production_jev_cli_rejects_explicitly_synthetic_owner_evidence_index(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-jev-synthetic-index-") as temp:
            result = self.run_production_shaped_synthetic_evidence(
                Path(temp), index_classification="synthetic_test_only")
        self.assertEqual(result.returncode, 2, result.stdout[:3000])
        report = json.loads(result.stdout)
        self.assertEqual(report["status"], "blocked")
        self.assertFalse(report["ready"])
        self.assertFalse(report["owner_evidence_verified"])
        self.assertIn("owner_evidence_index_classification_invalid",
                      {item["code"] for item in report["errors"]})

    def test_production_jev_cli_rejects_explicitly_synthetic_evidence_document(self) -> None:
        with tempfile.TemporaryDirectory(prefix="acv1-jev-synthetic-document-") as temp:
            result = self.run_production_shaped_synthetic_evidence(
                Path(temp), index_classification=None, document_classification="synthetic_test_only")
        self.assertEqual(result.returncode, 2, result.stdout[:3000])
        report = json.loads(result.stdout)
        self.assertFalse(report["ready"])
        self.assertFalse(report["owner_evidence_verified"])
        self.assertIn("owner_evidence_artifact_classification_invalid",
                      {item["code"] for item in report["errors"]})

    def test_jev_test_only_marker_detection_covers_explicit_synthetic_and_test_labels(self) -> None:
        for value in (
            {"classification": "synthetic"},
            {"classification": "synthetic_test_only"},
            {"classification": "test-only"},
            {"status": "test_only_synthetic"},
            {"test_only": True},
        ):
            with self.subTest(value=value):
                self.assertTrue(jev._is_test_only_evidence(value))
        self.assertFalse(jev._is_test_only_evidence({"classification": "owner_evidence_unverified_origin"}))

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

    def test_extra_ground_truth_key_and_label_cannot_pad_sample_or_stratum_coverage(self) -> None:
        targets, dataset = complete_synthetic_plan()
        question = targets["questions"][0]
        question_id = question["question_id"]
        question["ground_truth"]["unexpected_extra_outcome"] = "fifth-label"
        question["coverage"]["minimum_samples_by_split"] = {split: 10 for split in jev.SPLITS}
        question["coverage"]["required_strata"]["malicious_source"] = {
            "applicability": "required",
            "minimum_by_value_by_split": {"synthetic-attack": {split: 6 for split in jev.SPLITS}},
        }
        for split in jev.SPLITS:
            rows = dataset["splits"][split]["examples"]
            for index in range(6):
                sample_id = f"SYNTH-EXTRA-LABEL-{split}-{index}"
                rows.append({
                    "sample_id": sample_id,
                    "sample_sha256": sha(sample_id.encode()),
                    "labels": {question_id: "fifth-label"},
                    "eligible_question_ids": [question_id],
                    "strata": {question_id: {"malicious_source": "synthetic-attack"}},
                })
            rows.sort(key=lambda row: row["sample_id"])
            dataset["splits"][split]["sha256"] = jev.canonical_hash(rows)

        hashes = {split: dataset["splits"][split]["sha256"] for split in jev.SPLITS}
        targets["dataset_split_sha256"] = dict(hashes)
        examples = {split: dataset["splits"][split]["examples"] for split in jev.SPLITS}
        for item in targets["questions"]:
            item["dataset_split_sha256"] = dict(hashes)
            input_hash, eligibility_hash = jev.dataset_scope_hashes(item["question_id"], examples)
            item["candidate_comparison"].update(
                dataset_split_sha256=dict(hashes), input_set_sha256=input_hash,
                eligibility_sha256=eligibility_hash)
            for baseline in item["baselines"].values():
                baseline.update(dataset_split_sha256=dict(hashes), input_set_sha256=input_hash,
                                eligibility_sha256=eligibility_hash)
        for _, approval, _, _, subject_id, digest in jev.approval_subjects(targets, dataset):
            if isinstance(approval, dict):
                approval.update(subject_id=subject_id, subject_sha256=digest)

        report = self.validate_synthetic(targets, dataset)
        errors = report["errors"]
        paths = {item["path"] for item in errors}
        self.assertIn("ground_truth_keys_unsupported", {item["code"] for item in errors})
        self.assertIn("unsupported_question_ground_truth_label", {item["code"] for item in errors})
        self.assertEqual(
            {path for path in paths if path.startswith("$.questions[0].coverage.minimum_samples_by_split.")},
            {f"$.questions[0].coverage.minimum_samples_by_split.{split}" for split in jev.SPLITS},
        )
        self.assertEqual(
            {path for path in paths if path.startswith("$.questions[0].coverage.required_strata.malicious_source.synthetic-attack.")},
            {f"$.questions[0].coverage.required_strata.malicious_source.synthetic-attack.{split}" for split in jev.SPLITS},
        )
        self.assertFalse(any(item["code"] == "dataset_label_coverage_below_owner_target" for item in errors))

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
