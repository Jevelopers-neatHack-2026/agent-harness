#!/usr/bin/env python3
"""Verify pinned owner-published Agent & Creative contract evidence offline.

This is readiness tooling, not a schema, port, approval authority, ledger, or
provider integration. It accepts a contract release only when independently
trusted owner evidence binds the exact bytes and an owner-published validator
can run inside a verified OS sandbox. Missing trust evidence or sandbox support
is a typed blocker; a local status, hash, or offline attestation is not proof.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import platform
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import sysconfig
import time
from pathlib import Path, PurePosixPath
from typing import Any


FORMAT = "agent-creative.contract-readiness/v2"
FIELD_INVENTORY_FORMAT = "agent-creative.contract-field-inventory/v1"
OWNER_EVIDENCE_INDEX_FORMAT = "agent-creative.owner-evidence-index/v1"
OWNER_EVIDENCE_FORMAT = "agent-creative.owner-evidence/v1"
ADAPTER_PROTOCOL = "agent-creative.contract-fixture-adapter/v2"
MAX_JSON_BYTES = 16 * 1024 * 1024
MAX_ARTIFACT_BYTES = 32 * 1024 * 1024
MAX_SEALED_SNAPSHOT_BYTES = 128 * 1024 * 1024
MAX_SEALED_SNAPSHOT_FILES = 512
MAX_ADAPTER_STDOUT = 2 * 1024 * 1024
MAX_ADAPTER_STDERR = 64 * 1024
ADAPTER_TIMEOUT_SECONDS = 60
SUPPORTED_JSON_SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"
MAX_SCHEMA_WALK_STEPS = 20_000
MAX_SCHEMA_FIELDS = 10_000
MAX_SCHEMA_PATH_BYTES = 4096
MAX_SCHEMA_AGGREGATE_PATH_BYTES = 1024 * 1024
MAX_SCHEMA_REFERENCE_EXPANSIONS = 2048
MAX_SCHEMA_REFERENCE_BYTES = 4096
DEPENDENCY_IMPORT_ROOT = "/acv1-dependencies"

# Required scenarios are issue #5 acceptance inputs. They are not schema fields
# and the expected results remain owner supplied in the pinned release.
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
    "choice_actual_provider_shape": "accepted",
    "score_actual_provider_shape": "accepted",
    "noul_actual_provider_shape": "accepted",
}
PRIMITIVES = {"choice", "score", "noul", "none"}
REQUIRED_PRIMITIVE_FIXTURES = {
    "choice": "choice_actual_provider_shape",
    "score": "score_actual_provider_shape",
    "noul": "noul_actual_provider_shape",
}
ROLE_REPOSITORIES = {
    "Agent and research": {"agent-harness", "event-intelligence", "shared-contracts"},
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


class UnsupportedSchema(ValueError):
    """A valid-looking schema uses a dialect or keyword outside the checked subset."""


class SchemaTraversalLimit(ValueError):
    """Schema field inventory traversal exceeded fixed work or output bounds."""


class SealedSnapshot:
    """Immutable memfd copies of exact, hash-verified execution artifacts."""

    def __init__(self, files: dict[str, int]):
        self.files = files

    @property
    def pass_fds(self) -> tuple[int, ...]:
        return tuple(self.files[path] for path in sorted(self.files))

    def close(self) -> None:
        for descriptor in self.files.values():
            try:
                os.close(descriptor)
            except OSError:
                pass
        self.files.clear()


class VerifiedArtifactCache(dict[Path, tuple[bytes, str]]):
    """Hash-verified bytes retained under the snapshot's aggregate resource bounds."""

    def __init__(self) -> None:
        super().__init__()
        self.retained_bytes = 0

    def __setitem__(self, path: Path, value: tuple[bytes, str]) -> None:
        previous = self.get(path)
        if previous is not None and isinstance(previous[0], bytes):
            self.retained_bytes -= len(previous[0])
        if isinstance(value, tuple) and len(value) == 2 and isinstance(value[0], bytes):
            self.retained_bytes += len(value[0])
        super().__setitem__(path, value)

    def clear(self) -> None:
        super().clear()
        self.retained_bytes = 0


def _verified_cache_bytes(cache: dict[Path, tuple[bytes, str]]) -> int:
    if isinstance(cache, VerifiedArtifactCache):
        return cache.retained_bytes
    return sum(len(raw) for raw, _digest in cache.values() if isinstance(raw, bytes))


def _make_sealed_snapshot(artifacts: dict[str, tuple[bytes, str]]) -> tuple[SealedSnapshot | None, str | None]:
    """Seal the exact bytes whose expected hashes came from the release manifest."""
    if not hasattr(os, "memfd_create"):
        return None, "The Linux runtime lacks sealed in-memory file support; provision Linux memfd sealing before adapter execution."
    try:
        import fcntl
        allow_sealing = getattr(os, "MFD_ALLOW_SEALING", None)
        required_seals = (getattr(fcntl, "F_SEAL_SEAL", None), getattr(fcntl, "F_SEAL_SHRINK", None),
                          getattr(fcntl, "F_SEAL_GROW", None), getattr(fcntl, "F_SEAL_WRITE", None))
        add_seals = getattr(fcntl, "F_ADD_SEALS", None)
        get_seals = getattr(fcntl, "F_GET_SEALS", None)
        if (not isinstance(allow_sealing, int) or add_seals is None or get_seals is None
                or any(not isinstance(value, int) for value in required_seals)):
            return None, "The Linux runtime lacks required memfd seals; provision sealed immutable execution snapshots."
        if len(artifacts) > MAX_SEALED_SNAPSHOT_FILES:
            raise ValueError("snapshot file count limit")
        total_size = sum(len(raw) for raw, _ in artifacts.values() if isinstance(raw, bytes))
        if total_size > MAX_SEALED_SNAPSHOT_BYTES:
            raise ValueError("snapshot byte limit")
        sealed_bits = 0
        for value in required_seals:
            sealed_bits |= value
        files: dict[str, int] = {}
        for relative, (raw, expected_sha) in sorted(artifacts.items()):
            path = PurePosixPath(relative)
            if (path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts)
                    or not isinstance(raw, bytes) or not raw or len(raw) > MAX_ARTIFACT_BYTES
                    or not digest_ok(expected_sha) or sha256_bytes(raw) != expected_sha):
                raise ValueError("snapshot artifact identity or bytes invalid")
            normalized = path.as_posix()
            if normalized in files:
                raise ValueError("snapshot path duplicate")
            for existing in files:
                existing_path = PurePosixPath(existing)
                if existing_path in path.parents or path in existing_path.parents:
                    raise ValueError("snapshot path parent collision")
            descriptor = os.memfd_create("acv1-release-artifact",
                                         allow_sealing | getattr(os, "MFD_CLOEXEC", 0))
            files[normalized] = descriptor
            offset = 0
            while offset < len(raw):
                count = os.write(descriptor, raw[offset:])
                if count <= 0:
                    raise OSError("short memfd write")
                offset += count
            fcntl.fcntl(descriptor, add_seals, sealed_bits)
            if fcntl.fcntl(descriptor, get_seals) & sealed_bits != sealed_bits:
                raise OSError("memfd seals did not hold")
            os.lseek(descriptor, 0, os.SEEK_SET)
        return SealedSnapshot(files), None
    except (OSError, ValueError, TypeError):
        for descriptor in locals().get("files", {}).values():
            try:
                os.close(descriptor)
            except OSError:
                pass
        return None, "Pinned execution artifacts could not be copied into immutable sealed files; the adapter is not executed."


def _pairs_no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _parse_int(value: str) -> int:
    if len(value.lstrip("-")) > 256:
        raise ValueError("integer too large")
    return int(value)


def _parse_float(value: str) -> float:
    if len(value) > 256:
        raise ValueError("number too large")
    number = float(value)
    if number != number or number in (float("inf"), float("-inf")):
        raise ValueError("non-finite number")
    return number


def _reject_constant(_: str) -> Any:
    raise ValueError("non-finite JSON number")


def parse_json_bytes(raw: bytes) -> Any:
    if len(raw) > MAX_JSON_BYTES:
        raise ValueError("JSON input too large")
    return json.loads(
        raw.decode("utf-8"), object_pairs_hook=_pairs_no_duplicates,
        parse_int=_parse_int, parse_float=_parse_float, parse_constant=_reject_constant,
    )


def read_json(path: Path) -> Any:
    try:
        with path.open("rb") as stream:
            return parse_json_bytes(stream.read(MAX_JSON_BYTES + 1))
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError, RecursionError):
        raise InputProblem(
            "manifest_unreadable_or_invalid_json", "$", "Agent and research",
            "Supply a bounded, readable UTF-8 JSON readiness descriptor from the canonical contract owner.",
        ) from None


def digest_ok(value: Any) -> bool:
    import re
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def revision_ok(value: Any) -> bool:
    import re
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{40}", value) is not None


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def read_bounded_file(path: Path, limit: int = MAX_ARTIFACT_BYTES) -> bytes:
    with path.open("rb") as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise OSError("artifact exceeds size bound")
    return raw


def canonical_hash(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    return sha256_bytes(raw)


def add_error(errors: list[dict[str, str]], code: str, path: str, owner: str, action: str) -> None:
    errors.append({"code": code, "path": path, "owner": owner, "action": action})


def _lexical_safe_path(base: Path, relative: Any, json_path: str, owner: str,
                       errors: list[dict[str, str]], *, external_root: Path | None = None) -> Path | None:
    if not isinstance(relative, str) or not relative.strip():
        add_error(errors, "artifact_path_missing", json_path, owner,
                  "Publish an exact relative path to the owner-controlled artifact.")
        return None
    path_part = Path(relative)
    if path_part.is_absolute() or any(part in {"..", ""} for part in path_part.parts):
        add_error(errors, "artifact_path_invalid", json_path, owner,
                  "Use a relative artifact path without parent traversal.")
        return None
    candidate = base / path_part
    cursor = base
    for part in path_part.parts:
        cursor = cursor / part
        if cursor.is_symlink():
            add_error(errors, "artifact_symlink_forbidden", json_path, owner,
                      "Publish regular artifacts without symlinks so the pinned bytes cannot escape their package.")
            return None
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(base.resolve(strict=True))
        if external_root is not None:
            resolved.relative_to(external_root.resolve(strict=True))
    except (OSError, ValueError):
        add_error(errors, "artifact_path_outside_package_or_missing", json_path, owner,
                  "Publish the artifact inside its pinned owner package; missing and out-of-package paths are not followed.")
        return None
    if not resolved.is_file():
        add_error(errors, "artifact_not_regular_file", json_path, owner,
                  "Publish the exact artifact as a regular file.")
        return None
    try:
        if resolved.stat().st_size > MAX_ARTIFACT_BYTES:
            add_error(errors, "artifact_size_limit_exceeded", json_path, owner,
                      "Republish a bounded artifact within the standalone verifier's documented size limit.")
            return None
    except OSError:
        add_error(errors, "artifact_unreadable", json_path, owner,
                  "Publish readable artifact bytes within the canonical release package.")
        return None
    return resolved


def check_artifact_bytes(base: Path, item: Any, json_path: str, owner: str,
                         errors: list[dict[str, str]],
                         verified_artifacts: dict[Path, tuple[bytes, str]] | None = None
                         ) -> tuple[Path | None, str | None, bytes | None]:
    """Capture and hash one bounded artifact; callers parse and seal these same bytes."""
    if not isinstance(item, dict):
        add_error(errors, "artifact_descriptor_malformed", json_path, owner,
                  "Publish an artifact descriptor with an exact path and SHA-256.")
        return None, None, None
    path = _lexical_safe_path(base, item.get("path"), json_path + ".path", owner, errors)
    expected = item.get("sha256")
    if not digest_ok(expected):
        add_error(errors, "artifact_sha256_missing", json_path + ".sha256", owner,
                  "Publish the SHA-256 of the exact owner artifact bytes.")
        expected = None
    if path is not None and expected is not None:
        cached = verified_artifacts.get(path) if verified_artifacts is not None else None
        if cached is not None:
            raw, actual = cached
            if actual != expected:
                add_error(errors, "artifact_sha256_mismatch", json_path, owner,
                          "Reuse an artifact path only when its descriptor pins the same exact verified bytes.")
                return None, None, None
            return path, actual, raw
        read_limit = MAX_ARTIFACT_BYTES
        if verified_artifacts is not None:
            if len(verified_artifacts) >= MAX_SEALED_SNAPSHOT_FILES:
                add_error(errors, "artifact_cache_file_limit_exceeded", json_path, owner,
                          "Reduce the unique owner artifact set to the verifier's fixed 512-file execution bound.")
                return None, None, None
            retained_bytes = _verified_cache_bytes(verified_artifacts)
            remaining_bytes = MAX_SEALED_SNAPSHOT_BYTES - retained_bytes
            if remaining_bytes <= 0:
                add_error(errors, "artifact_cache_byte_limit_exceeded", json_path, owner,
                          "Reduce retained owner artifact bytes to the verifier's fixed 128 MiB execution bound.")
                return None, None, None
            read_limit = min(MAX_ARTIFACT_BYTES, remaining_bytes)
            try:
                candidate_size = path.stat().st_size
            except OSError:
                add_error(errors, "artifact_unreadable", json_path + ".path", owner,
                          "Publish readable artifact bytes within the standalone verifier's fixed size limit.")
                return None, None, None
            if candidate_size > read_limit:
                code = ("artifact_cache_byte_limit_exceeded"
                        if remaining_bytes < MAX_ARTIFACT_BYTES
                        else "artifact_size_limit_exceeded")
                action = ("Reduce retained owner artifact bytes to the verifier's fixed 128 MiB execution bound."
                          if code == "artifact_cache_byte_limit_exceeded" else
                          "Republish a bounded artifact within the standalone verifier's documented size limit.")
                add_error(errors, code, json_path + ".path", owner, action)
                return None, None, None
        try:
            # The read is bounded by both the per-artifact and remaining
            # aggregate capacity, even if a file grows after the size check.
            raw = read_bounded_file(path, limit=read_limit)
        except OSError as exc:
            if "size bound" not in str(exc):
                code = "artifact_unreadable"
            elif verified_artifacts is not None and read_limit < MAX_ARTIFACT_BYTES:
                code = "artifact_cache_byte_limit_exceeded"
            else:
                code = "artifact_size_limit_exceeded"
            action = ("Reduce retained owner artifact bytes to the verifier's fixed 128 MiB execution bound."
                      if code == "artifact_cache_byte_limit_exceeded" else
                      "Publish readable artifact bytes within the standalone verifier's fixed size limit.")
            add_error(errors, code, json_path + ".path", owner,
                      action)
            return None, None, None
        if not raw:
            add_error(errors, "artifact_empty", json_path + ".path", owner,
                      "Publish nonempty canonical schema, fixture, adapter, or inventory bytes.")
            return None, None, None
        actual = sha256_bytes(raw)
        if actual != expected:
            add_error(errors, "artifact_sha256_mismatch", json_path, owner,
                      "Republish the descriptor with the exact owner artifact SHA-256.")
            return None, None, None
        if verified_artifacts is not None:
            retained_bytes = _verified_cache_bytes(verified_artifacts)
            if (len(verified_artifacts) >= MAX_SEALED_SNAPSHOT_FILES
                    or retained_bytes + len(raw) > MAX_SEALED_SNAPSHOT_BYTES):
                code = ("artifact_cache_file_limit_exceeded"
                        if len(verified_artifacts) >= MAX_SEALED_SNAPSHOT_FILES
                        else "artifact_cache_byte_limit_exceeded")
                action = ("Reduce the unique owner artifact set to the verifier's fixed 512-file execution bound."
                          if code == "artifact_cache_file_limit_exceeded" else
                          "Reduce retained owner artifact bytes to the verifier's fixed 128 MiB execution bound.")
                add_error(errors, code, json_path, owner, action)
                return None, None, None
            verified_artifacts[path] = (raw, actual)
        return path, actual, raw
    return path, None, None


def check_artifact(base: Path, item: Any, json_path: str, owner: str,
                   errors: list[dict[str, str]],
                   verified_artifacts: dict[Path, tuple[bytes, str]] | None = None
                   ) -> tuple[Path | None, str | None]:
    path, digest, _raw = check_artifact_bytes(base, item, json_path, owner, errors, verified_artifacts)
    return path, digest


def _read_execution_artifacts(base: Path,
                              artifacts: list[tuple[Path | None, str | None, str]],
                              errors: list[dict[str, str]],
                              verified_artifacts: dict[Path, tuple[bytes, str]]) -> dict[str, tuple[bytes, str]] | None:
    """Seal the exact already-parsed verified bytes without reopening mutable paths."""
    result: dict[str, tuple[bytes, str]] = {}
    total_size = 0
    for path, expected_sha, label in artifacts:
        if path is None or not digest_ok(expected_sha):
            add_error(errors, "execution_snapshot_input_unavailable", "$.canonical_release", "shared-contracts",
                      f"Publish and pin the {label} bytes before isolated execution.")
            continue
        try:
            relative = path.relative_to(base).as_posix()
        except (OSError, ValueError):
            add_error(errors, "execution_snapshot_input_unavailable", "$.canonical_release", "shared-contracts",
                      f"Keep the pinned {label} readable inside the canonical release package.")
            continue
        captured = verified_artifacts.get(path)
        if (not isinstance(captured, tuple) or len(captured) != 2
                or not isinstance(captured[0], bytes) or captured[1] != expected_sha
                or sha256_bytes(captured[0]) != expected_sha):
            add_error(errors, "execution_snapshot_pin_mismatch", "$.canonical_release", "shared-contracts",
                      f"Use the same exact hash-verified {label} bytes for validation and immutable execution.")
            continue
        raw = captured[0]
        previous = result.get(relative)
        if previous is not None:
            if previous[1] != expected_sha or previous[0] != raw:
                add_error(errors, "execution_snapshot_path_collision", "$.canonical_release", "shared-contracts",
                          "Give each distinct adapter, dependency, schema, inventory, and fixture artifact a unique relative package path.")
            continue
        total_size += len(raw)
        if total_size > MAX_SEALED_SNAPSHOT_BYTES or len(result) >= MAX_SEALED_SNAPSHOT_FILES:
            add_error(errors, "execution_snapshot_limit_exceeded", "$.canonical_release", "shared-contracts",
                      "Reduce the published runtime adapter/schema/fixture set to the standalone verifier's fixed snapshot resource bound.")
            continue
        result[relative] = (raw, expected_sha)
    if errors:
        return None
    return result


def _repo_for_role(role: Any, leaf: Any) -> str | None:
    if not isinstance(role, str) or not isinstance(leaf, str):
        return None
    for repository in ROLE_REPOSITORIES.get(role, set()):
        if f"/Jevelopers-neatHack-2026/{repository}/" in leaf:
            return repository
    return None


def _valid_issue_ref(value: Any, repository: str) -> bool:
    import re
    return isinstance(value, str) and re.fullmatch(
        rf"https://github\.com/Jevelopers-neatHack-2026/{re.escape(repository)}/(issues|pull)/[1-9][0-9]*", value
    ) is not None


def load_trusted_evidence_index(path_value: str | None, expected_sha: str | None,
                                workspace_root: Path, errors: list[dict[str, str]], *,
                                allow_test_only: bool = False) -> dict[str, Any] | None:
    owner_action = "Provision the independently verified owner-evidence index outside the repository and pin its exact SHA-256 in trusted runner configuration; local manifest assertions are not approval evidence."
    if not path_value or not digest_ok(expected_sha):
        add_error(errors, "owner_evidence_index_unavailable", "$.owner_evidence_index", "Agent and research", owner_action)
        return None
    index_path = Path(path_value)
    if index_path.is_symlink():
        add_error(errors, "owner_evidence_index_symlink_forbidden", "$.owner_evidence_index", "Agent and research", owner_action)
        return None
    try:
        resolved = index_path.resolve(strict=True)
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
        raw = read_bounded_file(resolved, MAX_JSON_BYTES)
        if sha256_bytes(raw) != expected_sha:
            raise ValueError("index digest mismatch or size limit")
        index = parse_json_bytes(raw)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError, RecursionError):
        add_error(errors, "owner_evidence_index_untrusted_or_invalid", "$.owner_evidence_index", "Agent and research", owner_action)
        return None
    if not isinstance(index, dict) or index.get("format") != OWNER_EVIDENCE_INDEX_FORMAT:
        add_error(errors, "owner_evidence_index_format_invalid", "$.owner_evidence_index.format", "Agent and research", owner_action)
        return None
    classification = index.get("classification")
    if (classification == "synthetic_test_only") != allow_test_only:
        add_error(errors, "owner_evidence_index_classification_invalid", "$.owner_evidence_index.classification", "Agent and research",
                  "Production readiness requires an independently trusted owner index; test-only evidence is accepted only by the explicit synthetic execution mode and never establishes readiness.")
        return None
    records = index.get("evidence")
    if not isinstance(records, list) or not records:
        add_error(errors, "owner_evidence_index_empty", "$.owner_evidence_index.evidence", "Agent and research", owner_action)
        return None
    evidence_root = resolved.parent
    by_id: dict[str, dict[str, Any]] = {}
    for idx, row in enumerate(records):
        ep = f"$.owner_evidence_index.evidence[{idx}]"
        if not isinstance(row, dict):
            add_error(errors, "owner_evidence_record_invalid", ep, "Agent and research", owner_action)
            continue
        evidence_id = row.get("evidence_id")
        owner = row.get("owner")
        repository = row.get("repository")
        revision = row.get("revision")
        ref = row.get("decision_ref")
        subjects = row.get("subjects")
        allowed_repositories = {repo for repos in ROLE_REPOSITORIES.values() for repo in repos}
        if (not isinstance(evidence_id, str) or not evidence_id.strip() or evidence_id in by_id
                or not isinstance(owner, str) or owner not in allowed_repositories or repository != owner or not revision_ok(revision)
                or not _valid_issue_ref(ref, owner) or row.get("status") != "accepted"
                or not isinstance(subjects, dict) or not subjects
                or any(not isinstance(key, str) or not digest_ok(value) for key, value in subjects.items())):
            add_error(errors, "owner_evidence_record_invalid", ep, str(owner or "Agent and research"), owner_action)
            continue
        artifact_path = _lexical_safe_path(evidence_root, row.get("evidence_path"), ep + ".evidence_path",
                                           owner, errors, external_root=evidence_root)
        evidence_sha = row.get("evidence_sha256")
        if artifact_path is None or not digest_ok(evidence_sha):
            add_error(errors, "owner_evidence_artifact_pin_missing", ep, owner, owner_action)
            continue
        try:
            evidence_raw = read_bounded_file(artifact_path)
            evidence = parse_json_bytes(evidence_raw)
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError, RecursionError):
            add_error(errors, "owner_evidence_artifact_invalid", ep, owner, owner_action)
            continue
        if sha256_bytes(evidence_raw) != evidence_sha or not isinstance(evidence, dict):
            add_error(errors, "owner_evidence_artifact_hash_mismatch", ep, owner, owner_action)
            continue
        identity = {
            "format": OWNER_EVIDENCE_FORMAT,
            "evidence_id": evidence_id,
            "owner": owner,
            "repository": repository,
            "revision": revision,
            "decision_ref": ref,
            "status": "accepted",
            "subjects": subjects,
        }
        if evidence != identity:
            add_error(errors, "owner_evidence_content_identity_mismatch", ep, owner, owner_action)
            continue
        by_id[evidence_id] = dict(identity, evidence_sha256=evidence_sha)
    if errors:
        return None
    return {"sha256": expected_sha, "records": by_id,
            "classification": "synthetic_test_only" if allow_test_only else "owner_evidence_unverified_origin"}


def validate_approval(approval: Any, *, path: str, owner_role: str, subject_id: str,
                      subject_sha256: str, leaf: Any, evidence: dict[str, Any] | None,
                      errors: list[dict[str, str]]) -> None:
    owner = _repo_for_role(owner_role, leaf)
    action = "Obtain owner-published evidence outside the manifest that approves the exact serialized subject bytes and owner revision."
    if owner is None:
        add_error(errors, "approval_owner_leaf_invalid", path, owner_role,
                  "Link the exact accountable owner leaf in a repository assigned to that owner role.")
        return
    if evidence is None:
        add_error(errors, "owner_approval_not_independently_verified", path, owner_role, action)
        return
    if not isinstance(approval, dict):
        add_error(errors, "owner_approval_missing", path, owner_role, action)
        return
    evidence_id = approval.get("evidence_id")
    record = evidence["records"].get(evidence_id) if isinstance(evidence_id, str) else None
    if (approval.get("status") != "accepted" or approval.get("owner_role") != owner_role
            or approval.get("owner_repository") != owner or approval.get("decision_ref") != leaf
            or approval.get("subject_id") != subject_id or approval.get("subject_sha256") != subject_sha256):
        add_error(errors, "owner_approval_subject_mismatch", path, owner_role, action)
    if not isinstance(record, dict):
        add_error(errors, "owner_evidence_reference_missing", path + ".evidence_id", owner_role, action)
        return
    if (record.get("owner") != owner or record.get("repository") != owner
            or record.get("decision_ref") != leaf or record.get("revision") != approval.get("revision")
            or record.get("subjects", {}).get(subject_id) != subject_sha256):
        add_error(errors, "owner_evidence_subject_binding_mismatch", path, owner_role, action)


def validate_release_decision(decision: Any, *, owner: str, reference: str, subject_sha256: str,
                              evidence: dict[str, Any] | None, errors: list[dict[str, str]]) -> None:
    """Bind an external repository handoff without assigning it a harness role."""
    path = "$.owner_decisions[" + owner + "]"
    action = "Obtain the exact external owner's published decision and independently pinned evidence for these release bytes."
    if evidence is None:
        add_error(errors, "owner_approval_not_independently_verified", path, owner, action)
        return
    if not isinstance(decision, dict):
        add_error(errors, "owner_decision_malformed", path, owner, action)
        return
    subject_id = "contract-release:" + owner
    evidence_id = decision.get("evidence_id")
    record = evidence["records"].get(evidence_id) if isinstance(evidence.get("records"), dict) and isinstance(evidence_id, str) else None
    if (decision.get("status") != "accepted" or decision.get("owner") != owner
            or decision.get("decision_ref") != reference or not revision_ok(decision.get("revision"))
            or decision.get("subject_id") != subject_id or decision.get("subject_sha256") != subject_sha256):
        add_error(errors, "external_owner_decision_subject_mismatch", path, owner,
                  "Bind the decision to the exact owner handoff, release content hash, and reviewed revision.")
    if not isinstance(record, dict):
        add_error(errors, "owner_evidence_reference_missing", path + ".evidence_id", owner, action)
        return
    if (record.get("owner") != owner or record.get("repository") != owner
            or record.get("decision_ref") != reference or record.get("revision") != decision.get("revision")
            or record.get("subjects", {}).get(subject_id) != subject_sha256):
        add_error(errors, "owner_evidence_subject_binding_mismatch", path, owner,
                  "Match the independently trusted owner evidence to this exact release subject hash and revision.")


def adapter_provenance_verified(adapter: Any, adapter_sha256: str | None,
                                evidence: dict[str, Any] | None) -> bool:
    if not isinstance(adapter, dict) or not digest_ok(adapter_sha256) or evidence is None:
        return False
    evidence_id = adapter.get("evidence_id")
    record = evidence.get("records", {}).get(evidence_id) if isinstance(evidence_id, str) else None
    return bool(isinstance(record, dict)
                and record.get("owner") == "shared-contracts"
                and record.get("repository") == "shared-contracts"
                and record.get("subjects", {}).get("contract-validator-adapter") == adapter_sha256)


def _read_inventory(base: Path, descriptor: Any, errors: list[dict[str, str]],
                    verified_artifacts: dict[Path, tuple[bytes, str]] | None = None
                    ) -> tuple[dict[str, Any] | None, str | None, Path | None]:
    path, digest, raw = check_artifact_bytes(
        base, descriptor, "$.canonical_release.field_inventory", "shared-contracts", errors, verified_artifacts)
    if path is None:
        return None, digest, None
    try:
        data = parse_json_bytes(raw or b"")
    except (UnicodeError, json.JSONDecodeError, ValueError, RecursionError):
        add_error(errors, "field_inventory_invalid_json", "$.canonical_release.field_inventory", "shared-contracts",
                  "Publish a bounded machine-readable field inventory generated from the exact canonical schema artifact bytes.")
        return None, digest, path
    if not isinstance(data, dict) or data.get("format") != FIELD_INVENTORY_FORMAT:
        add_error(errors, "field_inventory_format_invalid", "$.canonical_release.field_inventory", "shared-contracts",
                  f"Publish the owner-generated machine-readable inventory using {FIELD_INVENTORY_FORMAT}.")
        return None, digest, path
    return data, digest, path


def _schema_field_paths(document: Any) -> set[str]:
    """Enumerate property paths for the bounded Draft 2020-12 subset.

    Supported schema applicators are followed iteratively, including
    ``prefixItems`` and ``contains``. Local JSON Pointer references, including
    references inside those applicators, are supported. Unknown keywords,
    legacy dialects, external/dynamic references, pattern fields, and open
    dynamically named property schemas fail closed. Work and output have hard
    limits so a small reference DAG cannot expand without bound.
    """
    from urllib.parse import unquote

    supported_keywords = {
        "$schema", "$defs", "$ref", "$comment",
        "title", "description", "default", "deprecated", "readOnly", "writeOnly", "examples",
        "type", "enum", "const", "multipleOf", "maximum", "exclusiveMaximum", "minimum",
        "exclusiveMinimum", "maxLength", "minLength", "pattern", "format", "contentEncoding",
        "contentMediaType", "maxItems", "minItems", "uniqueItems", "maxContains", "minContains",
        "maxProperties", "minProperties", "required", "dependentRequired", "properties",
        "patternProperties", "additionalProperties", "allOf", "anyOf", "oneOf", "not", "if",
        "then", "else", "dependentSchemas", "prefixItems", "items", "contains",
        "unevaluatedItems", "unevaluatedProperties",
    }
    if not isinstance(document, dict):
        raise UnsupportedSchema("schema root is not an object")
    if document.get("$schema") != SUPPORTED_JSON_SCHEMA_DIALECT:
        raise UnsupportedSchema("schema dialect is not the supported Draft 2020-12 dialect")

    paths: set[str] = set()
    root_fields: set[str] = set()
    root_required: set[str] = set()
    visited: set[tuple[int, str, bool]] = set()
    stack: list[tuple[Any, str, int, bool]] = [(document, "", 0, True)]
    work_steps = 0
    enqueued = 1
    reference_expansions = 0
    aggregate_path_bytes = 0

    def charge_step(count: int = 1) -> None:
        nonlocal work_steps
        work_steps += count
        if work_steps > MAX_SCHEMA_WALK_STEPS:
            raise SchemaTraversalLimit("schema traversal step limit exceeded")

    def enqueue(node: Any, prefix: str, prefix_bytes: int, collect_fields: bool) -> None:
        nonlocal enqueued
        enqueued += 1
        if enqueued > MAX_SCHEMA_WALK_STEPS:
            raise SchemaTraversalLimit("schema pending-node limit exceeded")
        stack.append((node, prefix, prefix_bytes, collect_fields))

    def append_segment(prefix: str, prefix_bytes: int, segment: str) -> tuple[str, int]:
        try:
            segment_bytes = len(segment.encode("utf-8", errors="strict"))
        except UnicodeError as exc:
            raise UnsupportedSchema("schema path contains invalid Unicode") from exc
        path_bytes = prefix_bytes + 1 + segment_bytes
        if path_bytes > MAX_SCHEMA_PATH_BYTES:
            raise SchemaTraversalLimit("schema field path length limit exceeded")
        return prefix + "/" + segment, path_bytes

    def property_segment(name: Any) -> str:
        if not isinstance(name, str) or not name:
            raise UnsupportedSchema("schema property name is invalid")
        try:
            original_size = len(name.encode("utf-8", errors="strict"))
        except UnicodeError as exc:
            raise UnsupportedSchema("schema property name contains invalid Unicode") from exc
        if original_size > MAX_SCHEMA_PATH_BYTES:
            raise SchemaTraversalLimit("schema property name length limit exceeded")
        escaped_size = original_size + name.count("~") + name.count("/")
        if escaped_size > MAX_SCHEMA_PATH_BYTES:
            raise SchemaTraversalLimit("schema property path length limit exceeded")
        escaped = name.replace("~", "~0").replace("/", "~1")
        return escaped

    def decode_pointer_part(part: str) -> str:
        try:
            decoded = unquote(part, errors="strict")
        except (UnicodeError, ValueError) as exc:
            raise UnsupportedSchema("schema reference pointer encoding is invalid") from exc
        index = 0
        while index < len(decoded):
            if decoded[index] == "~":
                if index + 1 >= len(decoded) or decoded[index + 1] not in "01":
                    raise UnsupportedSchema("schema reference pointer escape is invalid")
                index += 2
            else:
                index += 1
        return decoded.replace("~1", "/").replace("~0", "~")

    def resolve_local_ref(reference: Any) -> Any:
        if not isinstance(reference, str) or len(reference.encode("utf-8", errors="replace")) > MAX_SCHEMA_REFERENCE_BYTES:
            raise UnsupportedSchema("schema reference is invalid or too long")
        if not reference.startswith("#"):
            raise UnsupportedSchema("external schema references are unsupported")
        if reference == "#":
            return document
        if not reference.startswith("#/"):
            raise UnsupportedSchema("only local JSON Pointer schema references are supported")
        target: Any = document
        for segment in reference[2:].split("/"):
            key = decode_pointer_part(segment)
            if isinstance(target, dict) and key in target:
                target = target[key]
            elif isinstance(target, list) and key.isascii() and key.isdigit() and (key == "0" or not key.startswith("0")):
                index = int(key)
                if index >= len(target):
                    raise UnsupportedSchema("schema reference pointer is unresolved")
                target = target[index]
            else:
                raise UnsupportedSchema("schema reference pointer is unresolved")
        return target

    while stack:
        node, prefix, prefix_bytes, collect_fields = stack.pop()
        charge_step()
        if isinstance(node, bool):
            if node:
                raise UnsupportedSchema("unconstrained boolean schemas are not enumerable")
            continue
        if not isinstance(node, dict):
            raise UnsupportedSchema("schema node is not an object or boolean")
        marker = (id(node), prefix if collect_fields else "", collect_fields)
        if marker in visited:
            continue
        visited.add(marker)
        for keyword in node:
            charge_step()
            if keyword not in supported_keywords:
                raise UnsupportedSchema("schema keyword is outside the supported Draft 2020-12 subset")
        if "$schema" in node and node is not document:
            raise UnsupportedSchema("nested schema dialect changes are unsupported")

        schema_types: set[str] | None = None
        if "type" in node:
            raw_types = node["type"]
            if isinstance(raw_types, str):
                schema_types = {raw_types}
            elif (isinstance(raw_types, list) and 0 < len(raw_types) <= 7
                  and all(isinstance(item, str) for item in raw_types)
                  and len(set(raw_types)) == len(raw_types)):
                charge_step(len(raw_types))
                schema_types = set(raw_types)
            else:
                raise UnsupportedSchema("schema type declaration is malformed")
            if not schema_types.issubset({"array", "boolean", "integer", "null", "number", "object", "string"}):
                raise UnsupportedSchema("schema type declaration is unsupported")

        object_keywords = {
            "properties", "patternProperties", "additionalProperties", "unevaluatedProperties",
            "required", "dependentRequired", "dependentSchemas", "minProperties", "maxProperties",
        }
        array_keywords = {
            "items", "prefixItems", "contains", "unevaluatedItems", "minItems", "maxItems",
            "uniqueItems", "minContains", "maxContains",
        }
        object_scope = (schema_types is None or "object" in schema_types
                        or any(keyword in node for keyword in object_keywords))
        array_scope = (schema_types is None or "array" in schema_types
                       or any(keyword in node for keyword in array_keywords))
        container_composition = any(keyword in node for keyword in ("$ref", "allOf", "anyOf", "oneOf"))

        scalar_constant = False
        if "const" in node:
            value = node["const"]
            if isinstance(value, (dict, list)):
                raise UnsupportedSchema("container const values are not enumerable")
            scalar_constant = True
        if "enum" in node:
            values = node["enum"]
            if not isinstance(values, list):
                raise UnsupportedSchema("schema enum is malformed")
            if len(values) > MAX_SCHEMA_WALK_STEPS:
                raise SchemaTraversalLimit("schema enum member limit exceeded")
            for value in values:
                charge_step()
                if isinstance(value, (dict, list)):
                    raise UnsupportedSchema("container enum values are not enumerable")
            scalar_constant = True

        has_closed_object_boundary = (
            node.get("additionalProperties") is False
            or node.get("unevaluatedProperties") is False
        )
        has_closed_array_boundary = (
            ("items" in node and node["items"] is not True)
            or ("unevaluatedItems" in node and node["unevaluatedItems"] is not True)
        )
        if object_scope:
            if ("additionalProperties" in node
                    and node["additionalProperties"] is not False):
                raise UnsupportedSchema("schema allows dynamically named additional properties")
            # Object/array constraints in a positive reference or composition
            # are checked at their target/branch. Local property keywords still
            # need their own closure so an open sibling cannot hide a field.
            has_local_object_shape = any(keyword in node for keyword in object_keywords)
            if (has_local_object_shape or schema_types is not None and "object" in schema_types):
                if not has_closed_object_boundary:
                    raise UnsupportedSchema("object schemas require an explicit closed property boundary")

        if array_scope and (schema_types is None or "array" in schema_types):
            has_local_array_shape = any(keyword in node for keyword in array_keywords)
            if has_local_array_shape or schema_types is not None and "array" in schema_types:
                if not has_closed_array_boundary:
                    raise UnsupportedSchema("array schemas require explicit item coverage")

        # An untyped, non-composed schema otherwise admits arbitrary objects
        # and arrays by default (including an empty schema or annotation-only
        # schema). Scalar const/enum declarations are the only type-less
        # primitive form accepted without container boundaries.
        if (schema_types is None and not container_composition and not scalar_constant):
            if not has_closed_object_boundary:
                raise UnsupportedSchema("schema admits implicit open object fields")
            if not has_closed_array_boundary:
                raise UnsupportedSchema("schema admits implicit open array items")

        if "$defs" in node:
            definitions = node["$defs"]
            if not isinstance(definitions, dict):
                raise UnsupportedSchema("schema $defs must be an object")
            for name, definition in definitions.items():
                charge_step()
                if not isinstance(name, str):
                    raise UnsupportedSchema("schema definition name is invalid")
                enqueue(definition, "", 0, False)
        if "$ref" in node:
            reference_expansions += 1
            if reference_expansions > MAX_SCHEMA_REFERENCE_EXPANSIONS:
                raise SchemaTraversalLimit("schema reference expansion limit exceeded")
            enqueue(resolve_local_ref(node["$ref"]), prefix, prefix_bytes, collect_fields)

        properties = node.get("properties", {})
        if not isinstance(properties, dict):
            raise UnsupportedSchema("schema properties is not an object")
        for name, child in properties.items():
            charge_step()
            segment = property_segment(name)
            if collect_fields:
                property_path, property_path_bytes = append_segment(prefix, prefix_bytes, segment)
                if property_path not in paths:
                    if len(paths) >= MAX_SCHEMA_FIELDS:
                        raise SchemaTraversalLimit("schema generated-field limit exceeded")
                    aggregate_path_bytes += property_path_bytes
                    if aggregate_path_bytes > MAX_SCHEMA_AGGREGATE_PATH_BYTES:
                        raise SchemaTraversalLimit("schema aggregate field-path byte limit exceeded")
                    paths.add(property_path)
                if not prefix:
                    root_fields.add(property_path)
                enqueue(child, property_path, property_path_bytes, True)
            else:
                enqueue(child, "", 0, False)

        pattern_properties = node.get("patternProperties", {})
        if not isinstance(pattern_properties, dict):
            raise UnsupportedSchema("schema patternProperties is malformed")
        if pattern_properties:
            raise UnsupportedSchema("patternProperties cannot be inventoried as exact field paths")
        if (properties and "additionalProperties" not in node
                and node.get("unevaluatedProperties") is not False):
            raise UnsupportedSchema("object properties require an explicit closed property boundary")
        if "unevaluatedProperties" in node and node["unevaluatedProperties"] is not False:
            raise UnsupportedSchema("schema unevaluatedProperties is dynamic and unsupported")

        required = node.get("required", [])
        if not isinstance(required, list):
            raise UnsupportedSchema("schema required list is malformed")
        for name in required:
            charge_step()
            segment = property_segment(name)
            required_path, _ = append_segment("", 0, segment)
            if collect_fields and not prefix:
                root_required.add(required_path)

        dependent_required = node.get("dependentRequired", {})
        if not isinstance(dependent_required, dict):
            raise UnsupportedSchema("schema dependentRequired is malformed")
        for name, required_names in dependent_required.items():
            charge_step()
            property_segment(name)
            if not isinstance(required_names, list):
                raise UnsupportedSchema("schema dependentRequired entry is malformed")
            for required_name in required_names:
                charge_step()
                property_segment(required_name)

        if "dependentSchemas" in node:
            dependent = node["dependentSchemas"]
            if not isinstance(dependent, dict):
                raise UnsupportedSchema("schema dependentSchemas is malformed")
            for name, branch in dependent.items():
                charge_step()
                property_segment(name)
                enqueue(branch, prefix, prefix_bytes, collect_fields)

        for keyword in ("allOf", "anyOf", "oneOf"):
            if keyword not in node:
                continue
            branches = node[keyword]
            if not isinstance(branches, list):
                raise UnsupportedSchema("schema composition is malformed")
            if keyword == "allOf" and not branches:
                raise UnsupportedSchema("empty allOf schemas are unconstrained")
            for branch in branches:
                charge_step()
                enqueue(branch, prefix, prefix_bytes, collect_fields)
        for keyword in ("not", "if", "then", "else"):
            if keyword in node:
                enqueue(node[keyword], prefix, prefix_bytes, collect_fields)

        if "prefixItems" in node:
            prefix_items = node["prefixItems"]
            if not isinstance(prefix_items, list):
                raise UnsupportedSchema("schema prefixItems must be an array")
            for index, item in enumerate(prefix_items):
                charge_step()
                if item is True:
                    raise UnsupportedSchema("open prefixItems entries are not enumerable")
                if collect_fields:
                    item_path, item_path_bytes = append_segment(prefix, prefix_bytes, str(index))
                    enqueue(item, item_path, item_path_bytes, True)
                else:
                    enqueue(item, "", 0, False)

        if "items" in node:
            items = node["items"]
            if items is True:
                raise UnsupportedSchema("open items schemas are not enumerable")
            if isinstance(items, list):
                raise UnsupportedSchema("legacy tuple-form items is outside the supported dialect")
            if isinstance(items, (dict, bool)):
                if collect_fields:
                    item_path, item_path_bytes = append_segment(prefix, prefix_bytes, "*")
                    enqueue(items, item_path, item_path_bytes, True)
                else:
                    enqueue(items, "", 0, False)
            else:
                raise UnsupportedSchema("schema items is malformed")

        if "contains" in node:
            contains = node["contains"]
            if isinstance(contains, (dict, bool)):
                if collect_fields:
                    item_path, item_path_bytes = append_segment(prefix, prefix_bytes, "*")
                    enqueue(contains, item_path, item_path_bytes, True)
                else:
                    enqueue(contains, "", 0, False)
            else:
                raise UnsupportedSchema("schema contains is malformed")

        if "unevaluatedItems" in node:
            unevaluated_items = node["unevaluatedItems"]
            if unevaluated_items is True:
                raise UnsupportedSchema("open unevaluatedItems schemas are not enumerable")
            if isinstance(unevaluated_items, (dict, bool)):
                if collect_fields:
                    item_path, item_path_bytes = append_segment(prefix, prefix_bytes, "*")
                    enqueue(unevaluated_items, item_path, item_path_bytes, True)
                else:
                    enqueue(unevaluated_items, "", 0, False)
            else:
                raise UnsupportedSchema("schema unevaluatedItems is malformed")

    if not root_required.issubset(root_fields):
        raise UnsupportedSchema("root required property is not declared")
    if not paths:
        raise UnsupportedSchema("schema declares no runtime fields")
    return paths


def validate_crosswalk(manifest: dict[str, Any], schemas: dict[str, dict[str, Any]],
                       inventory: dict[str, Any] | None, evidence: dict[str, Any] | None,
                       errors: list[dict[str, str]]) -> None:
    expected: set[tuple[str, str, str, str, str]] = set()
    inventory_schemas = inventory.get("schemas") if isinstance(inventory, dict) else None
    if not isinstance(inventory_schemas, list) or not inventory_schemas:
        add_error(errors, "canonical_schema_field_inventory_missing", "$.canonical_release.field_inventory.schemas", "shared-contracts",
                  "Publish a nonempty exact field inventory per canonical schema, contract, and primitive; the harness will not infer field names.")
        inventory_schemas = []
    seen_inventory_keys: set[tuple[str, str, str]] = set()
    schema_inventory_keys: set[tuple[str, str]] = set()
    inventory_tuple_keys: set[tuple[str, str, str, str]] = set()
    all_primitives: set[str] = set()
    for index, item in enumerate(inventory_schemas):
        path = f"$.canonical_release.field_inventory.schemas[{index}]"
        if not isinstance(item, dict):
            add_error(errors, "schema_inventory_entry_invalid", path, "shared-contracts",
                      "Publish each actual schema/contract/primitive inventory entry with its exact field paths.")
            continue
        schema_id, schema_sha = item.get("schema_id"), item.get("schema_sha256")
        contract_id, primitive = item.get("contract_id"), item.get("primitive")
        fields, aliases = item.get("field_paths"), item.get("aliases")
        key = (schema_id, contract_id, primitive) if all(isinstance(x, str) for x in (schema_id, contract_id, primitive)) else None
        if (key is None or not schema_id.strip() or not contract_id.strip() or primitive not in PRIMITIVES
                or not digest_ok(schema_sha) or schemas.get(schema_id, {}).get("sha256") != schema_sha):
            add_error(errors, "schema_inventory_identity_invalid", path, "shared-contracts",
                      "Bind each inventory entry to an existing schema artifact SHA, exact owner contract ID, and supported primitive tag.")
            continue
        if key in seen_inventory_keys:
            add_error(errors, "schema_inventory_duplicate", path, "shared-contracts",
                      "Publish one field inventory per exact schema SHA, contract ID, and primitive.")
        seen_inventory_keys.add(key)
        schema_inventory_keys.add((schema_id, schema_sha))
        inventory_tuple_keys.add((schema_id, schema_sha, contract_id, primitive))
        if not isinstance(fields, list) or not fields or any(not isinstance(field, str) or not field.strip() for field in fields):
            add_error(errors, "schema_field_inventory_empty_or_invalid", path + ".field_paths", "shared-contracts",
                      "Publish every exact serialized field path from the owner schema; empty schemas cannot satisfy the gate.")
            fields = []
        if len(fields) != len(set(field for field in fields if isinstance(field, str))):
            add_error(errors, "schema_field_inventory_duplicate", path + ".field_paths", "shared-contracts",
                      "Remove duplicate field paths from the owner-generated schema inventory.")
        actual_schema_fields = schemas.get(schema_id, {}).get("field_paths")
        if isinstance(actual_schema_fields, set) and set(field for field in fields if isinstance(field, str)) != actual_schema_fields:
            add_error(errors, "schema_inventory_field_set_mismatch", path + ".field_paths", "shared-contracts",
                      "Inventory every property declared by the exact pinned runtime schema; extra fields, omissions, and aliases do not count as schema coverage.")
        elif not isinstance(actual_schema_fields, set):
            add_error(errors, "schema_inventory_source_unavailable", path + ".field_paths", "shared-contracts",
                      "Publish an enumerable JSON schema with exact declared fields; the harness will not infer field names from tags or aliases.")
        if not isinstance(aliases, list) or any(not isinstance(alias, str) or not alias.strip() for alias in aliases):
            add_error(errors, "schema_alias_inventory_invalid", path + ".aliases", "shared-contracts",
                      "Publish an explicit owner-reviewed alias list; aliases cannot substitute for canonical field paths.")
            aliases = []
        for field in fields:
            if isinstance(field, str) and field.strip():
                expected.add((schema_id, schema_sha, contract_id, primitive, field))
        if primitive in REQUIRED_PRIMITIVE_FIXTURES:
            all_primitives.add(primitive)
    for primitive in sorted(set(REQUIRED_PRIMITIVE_FIXTURES) - all_primitives):
        add_error(errors, "actual_provider_primitive_inventory_missing", "$.canonical_release.field_inventory.schemas", "creative-engine",
                  f"Publish the actual owner schema and fixture inventory for provider primitive {primitive!r}; do not synthesize provider semantics.")

    rows = manifest.get("field_crosswalk")
    if not isinstance(rows, list):
        rows = []
    actual: set[tuple[str, str, str, str, str]] = set()
    row_owner_action = "Publish exactly one crosswalk row per field path in the owner-generated schema/contract/primitive inventory."
    text_fields = ("meaning", "producer_role", "producer_leaf", "consumer_role", "consumer_leaf",
                  "trusted_derivation", "unavailable_semantics", "precision", "privacy_retention", "compatibility_rule")
    for index, row in enumerate(rows):
        path = f"$.field_crosswalk[{index}]"
        if not isinstance(row, dict):
            add_error(errors, "crosswalk_row_malformed", path, "shared-contracts", row_owner_action)
            continue
        if "coverage_tags" in row:
            add_error(errors, "untrusted_coverage_tags_forbidden", path + ".coverage_tags", "Agent and research",
                      "Remove caller-supplied coverage tags; exact schema field and fixture mappings determine coverage.")
        schema_id, schema_sha = row.get("schema_id"), row.get("schema_sha256")
        contract_id, primitive, field = row.get("contract_id"), row.get("primitive"), row.get("field_path")
        key = (schema_id, schema_sha, contract_id, primitive, field) if all(isinstance(x, str) for x in (schema_id, schema_sha, contract_id, primitive, field)) else None
        if key is None or key not in expected:
            add_error(errors, "crosswalk_field_not_in_pinned_inventory", path, "shared-contracts",
                      "Use an exact field path from the matching pinned schema, contract, and primitive inventory; tags cannot fill missing fields.")
        elif key in actual:
            add_error(errors, "crosswalk_field_duplicate", path, "shared-contracts", row_owner_action)
        else:
            actual.add(key)
        for field_name in text_fields:
            value = row.get(field_name)
            if not isinstance(value, str) or not value.strip():
                add_error(errors, "crosswalk_field_metadata_missing", path + "." + field_name, "shared-contracts",
                          "Record owner-defined meaning, roles, exact leaves, trust derivation, unknown semantics, precision, privacy, and compatibility for this serialized field.")
        producer_repo = _repo_for_role(row.get("producer_role"), row.get("producer_leaf"))
        consumer_repo = _repo_for_role(row.get("consumer_role"), row.get("consumer_leaf"))
        for side, role_key, leaf_key, repo in (("producer_review", "producer_role", "producer_leaf", producer_repo),
                                               ("consumer_review", "consumer_role", "consumer_leaf", consumer_repo)):
            review = row.get(side)
            subject_id = "contract-crosswalk:" + canonical_hash({k: v for k, v in row.items() if k not in {"producer_review", "consumer_review"}}) + ":" + side
            subject_hash = canonical_hash({k: v for k, v in row.items() if k not in {"producer_review", "consumer_review"}})
            if repo is None or not isinstance(review, dict):
                add_error(errors, "affected_role_review_missing", path + "." + side,
                          str(row.get(role_key) if isinstance(row.get(role_key), str) else "Agent and research"),
                          "Record the affected producer or consumer's independently verified review of this exact field row.")
                continue
            validate_approval(review, path=path + "." + side, owner_role=row[role_key], subject_id=subject_id,
                              subject_sha256=subject_hash, leaf=row[leaf_key], evidence=evidence, errors=errors)
    for schema_id, schema in schemas.items():
        schema_identity = (schema_id, schema.get("sha256"))
        if schema_identity not in schema_inventory_keys:
            add_error(errors, "canonical_schema_inventory_tuple_missing", "$.canonical_release.field_inventory.schemas", "shared-contracts",
                      "Add owner-published field inventory rows for every declared runtime schema artifact; schemas are not auxiliary unless separately modeled.")
    for missing in sorted(expected - actual):
        add_error(errors, "crosswalk_field_missing", "$.field_crosswalk", "shared-contracts",
                  "Add the exact schema/contract/primitive/field-path row missing from the pinned inventory.")
    for extra in sorted(actual - expected):
        add_error(errors, "crosswalk_field_not_in_pinned_inventory", "$.field_crosswalk", "shared-contracts",
                  "Remove fields and aliases absent from the exact owner schema inventory.")


def _validate_fixture_map(manifest: dict[str, Any], schemas: dict[str, dict[str, Any]], inventory: dict[str, Any] | None,
                          base: Path, errors: list[dict[str, str]],
                          verified_artifacts: dict[Path, tuple[bytes, str]] | None = None
                          ) -> tuple[dict[str, dict[str, Any]], dict[str, Path]]:
    fixture_rows = manifest.get("fixtures")
    if not isinstance(fixture_rows, list):
        fixture_rows = []
    fixtures: dict[str, dict[str, Any]] = {}
    fixture_paths: dict[str, Path] = {}
    schema_entries = inventory.get("schemas", []) if isinstance(inventory, dict) else []
    inventory_keys = {
        (item.get("schema_id"), item.get("schema_sha256"), item.get("contract_id"), item.get("primitive"))
        for item in schema_entries if isinstance(item, dict)
        and all(isinstance(item.get(key), str) for key in ("schema_id", "schema_sha256", "contract_id", "primitive"))
        and isinstance(schemas.get(item.get("schema_id")), dict)
        and schemas[item.get("schema_id")].get("sha256") == item.get("schema_sha256")
    }
    fixture_tuple_keys: set[tuple[str, str, str, str]] = set()
    for index, fixture in enumerate(fixture_rows):
        path = f"$.fixtures[{index}]"
        if not isinstance(fixture, dict):
            add_error(errors, "fixture_manifest_entry_malformed", path, "shared-contracts",
                      "Publish fixture path/hash plus exact owner schema, contract, primitive, and expected semantic outcome.")
            continue
        case_id = fixture.get("case_id")
        if not isinstance(case_id, str) or case_id not in REQUIRED_CASES:
            add_error(errors, "fixture_case_id_unsupported", path + ".case_id", "Agent and research",
                      "Use one exact ACV1-CONTRACTS issue #5 acceptance case ID.")
            continue
        if case_id in fixtures:
            add_error(errors, "duplicate_fixture_case", path + ".case_id", "shared-contracts",
                      "Map each required acceptance case once to an owner-published fixture.")
            continue
        fixtures[case_id] = fixture
        fixture_path, fixture_sha, fixture_raw = check_artifact_bytes(
            base, fixture, path, "shared-contracts", errors, verified_artifacts)
        schema_id, schema_sha = fixture.get("schema_id"), fixture.get("schema_sha256")
        contract_id, primitive = fixture.get("contract_id"), fixture.get("primitive")
        identity_values = (schema_id, schema_sha, contract_id, primitive)
        identity_is_typed = all(isinstance(value, str) for value in identity_values)
        mapped_identity = identity_is_typed and identity_values in inventory_keys
        schema_record = schemas.get(schema_id) if isinstance(schema_id, str) else None
        if not mapped_identity or not isinstance(schema_record, dict) or schema_record.get("sha256") != schema_sha:
            add_error(errors, "fixture_schema_contract_primitive_unmapped", path, "shared-contracts",
                      "Map each fixture to the exact pinned schema, owner contract, and primitive inventory entry it exercises.")
        else:
            fixture_tuple_keys.add((schema_id, schema_sha, contract_id, primitive))
        if fixture.get("expected_outcome") != REQUIRED_CASES[case_id]:
            add_error(errors, "fixture_expected_outcome_invalid", path + ".expected_outcome", "shared-contracts",
                      "Publish the owner-approved semantic expected outcome for this issue acceptance case.")
        classification = fixture.get("data_classification")
        if not isinstance(classification, str) or classification not in {"synthetic", "sanitized_non_customer"}:
            add_error(errors, "fixture_data_classification_missing", path + ".data_classification", "shared-contracts",
                      "Mark fixtures synthetic or sanitized non-customer data; never include secrets or customer content.")
        if fixture_path is not None and fixture_raw is not None:
            fixture_paths[case_id] = fixture_path
            try:
                media_type = fixture.get("media_type")
                if ((isinstance(media_type, str) and "json" in media_type)
                        or fixture_path.suffix.lower() == ".json"):
                    content = parse_json_bytes(fixture_raw)
                    if content is None or content == {} or content == []:
                        add_error(errors, "fixture_content_empty", path + ".path", "shared-contracts",
                                  "Publish a nonempty owner fixture that exercises real input bytes; an empty object is not a conformance case.")
            except (UnicodeError, json.JSONDecodeError, ValueError, RecursionError):
                add_error(errors, "fixture_json_invalid", path + ".path", "shared-contracts",
                          "Publish valid bounded JSON fixture bytes for a JSON-classified fixture.")
        if case_id in {"current_valid", "current_invalid", "supported_older_version",
                       "unknown_additive_fields", "old_consumer_additive_field",
                       "fractional_subcent_usage", "choice_actual_provider_shape",
                       "score_actual_provider_shape", "noul_actual_provider_shape"}:
            if not digest_ok(fixture.get("serialized_sha256")):
                add_error(errors, "canonical_serialization_hash_missing", path + ".serialized_sha256", "shared-contracts",
                          "Publish the expected SHA-256 of actual canonical serialized output bytes under the pinned owner serialization rule.")
    for case_id in REQUIRED_CASES:
        if case_id not in fixtures:
            add_error(errors, "required_fixture_missing", "$.fixtures", "shared-contracts",
                      f"Publish the owner fixture for required issue #5 case {case_id!r}.")
    for primitive, case_id in REQUIRED_PRIMITIVE_FIXTURES.items():
        fixture = fixtures.get(case_id)
        if not isinstance(fixture, dict) or fixture.get("primitive") != primitive:
            add_error(errors, "provider_primitive_fixture_missing", "$.fixtures", "creative-engine",
                      f"Provide the actual owner fixture for provider primitive {primitive!r}; do not infer provider payload or confidence semantics.")
    for missing_tuple in sorted(inventory_keys - fixture_tuple_keys):
        add_error(errors, "schema_contract_primitive_fixture_missing", "$.fixtures", "shared-contracts",
                  "Map at least one owner-published fixture to every exact runtime schema/contract/primitive inventory tuple.")
    crosswalk = manifest.get("fixture_crosswalk")
    expected_mapping = {
        (case_id, fixture.get("schema_id"), fixture.get("schema_sha256"), fixture.get("contract_id"), fixture.get("primitive"), fixture.get("sha256"))
        for case_id, fixture in fixtures.items() if isinstance(fixture, dict)
        and all(isinstance(fixture.get(key), str) for key in
                ("schema_id", "schema_sha256", "contract_id", "primitive", "sha256"))
    }
    actual_mapping: set[tuple[Any, ...]] = set()
    if not isinstance(crosswalk, list) or not crosswalk:
        add_error(errors, "fixture_crosswalk_missing", "$.fixture_crosswalk", "shared-contracts",
                  "Publish an explicit fixture-to-schema/contract/primitive mapping for every acceptance case.")
        crosswalk = []
    for index, row in enumerate(crosswalk):
        path = f"$.fixture_crosswalk[{index}]"
        if not isinstance(row, dict):
            add_error(errors, "fixture_crosswalk_row_invalid", path, "shared-contracts",
                      "Map each fixture ID to exact owner schema, contract, primitive, and fixture SHA-256.")
            continue
        key_values = (row.get("case_id"), row.get("schema_id"), row.get("schema_sha256"),
                      row.get("contract_id"), row.get("primitive"), row.get("fixture_sha256"))
        if not all(isinstance(value, str) for value in key_values):
            add_error(errors, "fixture_crosswalk_identity_invalid", path, "shared-contracts",
                      "Use bounded string IDs and hashes for each exact fixture-to-schema crosswalk identity.")
            continue
        key = key_values
        if key in actual_mapping:
            add_error(errors, "fixture_crosswalk_duplicate", path, "shared-contracts",
                      "Map each case/schema/contract/primitive tuple exactly once.")
        actual_mapping.add(key)
    for missing in expected_mapping - actual_mapping:
        add_error(errors, "fixture_crosswalk_case_missing", "$.fixture_crosswalk", "shared-contracts",
                  "Add the exact owner fixture mapping omitted from the acceptance crosswalk.")
    for extra in actual_mapping - expected_mapping:
        add_error(errors, "fixture_crosswalk_case_unowned", "$.fixture_crosswalk", "shared-contracts",
                  "Remove mappings that do not match a pinned owner fixture byte-for-byte.")
    return fixtures, fixture_paths


def _sandbox_prerequisite_error() -> str | None:
    if platform.system() != "Linux":
        return "The current platform has no accepted supported no-network filesystem sandbox backend; macOS sandbox-exec is deprecated and is not used as a security boundary. Provision reviewed bubblewrap on a supported Linux runner or another audited OS sandbox."
    bwrap = shutil.which("bwrap")
    if not bwrap:
        return "bubblewrap is unavailable. Provision a supported bubblewrap runtime with user, network, and PID namespaces; adapters are not executed without an enforceable OS sandbox."
    python_exe = os.path.realpath(sys.executable)
    if not python_exe.startswith("/usr/"):
        return "The Python runtime is outside the isolated /usr system tree; provision the approved Python runtime under /usr."
    return None


def _sandbox_command(package_root: Path, adapter: Path, request_bytes: bytes,
                     snapshot: SealedSnapshot | None = None,
                     dependency_paths: tuple[str, ...] = ()) -> tuple[list[str] | None, str | None]:
    prerequisite = _sandbox_prerequisite_error()
    if prerequisite is not None:
        return None, prerequisite
    if not request_bytes or len(request_bytes) > 512 * 1024:
        return None, "The adapter request exceeds the fixed offline execution input bound."
    python_exe = os.path.realpath(sys.executable)
    if snapshot is None:
        return None, "A sealed hash-verified execution snapshot is required; mutable package paths are never mounted."
    if not package_root.is_dir():
        return None, "The canonical package root is unavailable; republish its verified artifacts."
    try:
        adapter_relative = adapter.relative_to(package_root).as_posix()
    except ValueError:
        return None, "The pinned adapter path is outside the canonical package."
    if adapter_relative not in snapshot.files:
        return None, "The pinned adapter is absent from the sealed execution snapshot."
    if not isinstance(dependency_paths, tuple) or any(not isinstance(path, str) for path in dependency_paths):
        return None, "The declared adapter dependency list is invalid; only exact sealed dependency paths may be imported."
    normalized_dependencies: list[str] = []
    for relative in dependency_paths:
        path = PurePosixPath(relative)
        if (path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts)
                or path.as_posix() not in snapshot.files or path.as_posix() == adapter_relative):
            return None, "A declared adapter dependency is absent or outside the sealed snapshot."
        normalized_dependencies.append(path.as_posix())
    if len(normalized_dependencies) != len(set(normalized_dependencies)):
        return None, "The declared adapter dependency list contains duplicate paths."
    dependency_set = set(normalized_dependencies)
    release_paths = set(snapshot.files) - dependency_set
    if adapter_relative not in release_paths:
        return None, "The pinned adapter cannot also be a dependency import."
    bwrap = shutil.which("bwrap")
    assert bwrap is not None
    command = [
        bwrap, "--die-with-parent", "--new-session", "--unshare-all", "--cap-drop", "ALL",
        "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp",
        "--dir", "/usr", "--ro-bind", "/usr", "/usr",
    ]
    for system_dir in ("/lib", "/lib64", "/bin", "/sbin"):
        if os.path.exists(system_dir):
            command.extend(["--dir", system_dir, "--ro-bind", system_dir, system_dir])
    command.extend(["--dir", "/release", "--tmpfs", "/release",
                    "--dir", DEPENDENCY_IMPORT_ROOT, "--tmpfs", DEPENDENCY_IMPORT_ROOT])
    directories: set[str] = set()
    dependency_directories: set[str] = set()
    for relative in release_paths:
        path = PurePosixPath(relative)
        for depth in range(1, len(path.parts)):
            directories.add("/release/" + "/".join(path.parts[:depth]))
    for relative in dependency_set:
        path = PurePosixPath(relative)
        for depth in range(1, len(path.parts)):
            dependency_directories.add(DEPENDENCY_IMPORT_ROOT + "/" + "/".join(path.parts[:depth]))
    for directory in sorted(directories, key=lambda value: (value.count("/"), value)):
        command.extend(["--dir", directory])
    for directory in sorted(dependency_directories, key=lambda value: (value.count("/"), value)):
        command.extend(["--dir", directory])
    for relative in sorted(release_paths):
        command.extend(["--ro-bind-data", str(snapshot.files[relative]), "/release/" + relative])
    for relative in sorted(dependency_set):
        command.extend(["--ro-bind-data", str(snapshot.files[relative]),
                        DEPENDENCY_IMPORT_ROOT + "/" + relative])
    command.extend([
        "--remount-ro", "/release", "--remount-ro", DEPENDENCY_IMPORT_ROOT,
        "--chdir", "/", "--clearenv", "--setenv", "PATH", "/usr/bin:/bin",
        "--setenv", "HOME", "/nonexistent", "--setenv", "LC_ALL", "C.UTF-8",
        python_exe, "-I", "-B", "-S", "-c",
        "import sys\n"
        "sys.path.insert(0, '" + DEPENDENCY_IMPORT_ROOT + "')\n"
        "adapter_path = sys.argv.pop(1)\n"
        "sys.argv = [adapter_path]\n"
        "with open(adapter_path, 'rb') as source_file: source_bytes = source_file.read()\n"
        "exec(compile(source_bytes, adapter_path, 'exec'), {'__name__': '__main__', '__file__': adapter_path})\n",
        "/release/" + adapter_relative,
    ])
    return command, None


def _sandbox_stderr_summary(stderr: bytes) -> tuple[str, str]:
    """Classify bounded internal-canary stderr and return a path-redacted excerpt."""
    import re

    text = stderr[:4096].decode("utf-8", errors="replace")
    normalized = text.lower()
    if "setting up uid map" in normalized or "uid map" in normalized:
        category = "user_namespace_mapping_failure"
    elif "loopback" in normalized and "rtm_newaddr" in normalized and "operation not permitted" in normalized:
        category = "network_namespace_loopback_setup_denied"
    elif "namespace" in normalized and "operation not permitted" in normalized:
        category = "namespace_operation_not_permitted"
    elif "operation not permitted" in normalized:
        category = "operation_not_permitted"
    elif "permission denied" in normalized:
        category = "permission_denied"
    elif "no such file or directory" in normalized:
        category = "missing_file"
    elif "invalid argument" in normalized:
        category = "invalid_argument"
    elif "unknown option" in normalized or "unrecognized option" in normalized:
        category = "unsupported_bubblewrap_option"
    elif "modulenotfounderror" in normalized or "no module named" in normalized:
        category = "python_dependency_import_failure"
    elif "traceback" in normalized:
        category = "python_traceback"
    elif text.strip():
        category = "unclassified_synthetic_stderr"
    else:
        category = "empty_stderr"

    text = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", text)
    text = re.sub(r"(?<![A-Za-z0-9])/(?:[A-Za-z0-9._+~-]+/)*[A-Za-z0-9._+~-]*", "<path>", text)
    text = re.sub(r"\b(?:authorization|password|secret|token|api[_ -]?key)\b\s*[:=]\s*\S+",
                  "<redacted>", text, flags=re.IGNORECASE)
    text = " ".join("".join(char if char >= " " and char != "\x7f" else " " for char in text).split())
    return category, text[:240]


def _sandbox_process_failure(run: dict[str, Any], report: Any = None) -> str:
    code = run.get("code")
    if code not in {"execution_timeout", "execution_output_limit_exceeded", "execution_start_failed",
                    "adapter_nonzero_exit", "ok"}:
        code = "unclassified_process_result"
    exit_code = run.get("returncode")
    if type(exit_code) is not int:
        exit_code = "unavailable"
    stderr_category, stderr_excerpt = _sandbox_stderr_summary(
        run.get("stderr") if isinstance(run.get("stderr"), bytes) else b"")
    flags = {}
    if isinstance(report, dict):
        flags = {
            "outside_read_blocked": report.get("outside_read_blocked") is True,
            "private_shadow_visible": report.get("private_shadow_visible") is True,
            "shadow_write_private": report.get("sandbox_shadow_write_succeeded") is True,
            "network_blocked": report.get("network_blocked") is True,
            "network_namespace_isolated": report.get("network_namespace_isolated") is True,
            "apparmor_stack": report.get("apparmor_stack_verified") is True,
            "payload_no_new_privs": report.get("payload_no_new_privs") is True,
            "payload_capability_sets_zero": report.get("payload_capability_sets_zero") is True,
            "python_reexec_stack": report.get("python_reexec_stack_verified") is True,
            "other_exec_stack": report.get("other_exec_stack_verified") is True,
            "nested_userns_created": report.get("nested_userns_created") is True,
            "nested_capability_sets_observed": report.get("nested_capability_sets_observed") is True,
            "nested_mount_attempted": report.get("nested_mount_attempted") is True,
            "nested_capability_denied": report.get("nested_mount_namespace_denied") is True,
            "nested_mount_namespace_created": report.get("nested_mount_namespace_created") is True,
            "nested_bwrap_denied": report.get("nested_bwrap_denied") is True,
            "child_isolated": report.get("child_isolated") is True,
        }
        child_code = report.get("child_exit_code")
        flags["child_exit_code"] = child_code if type(child_code) is int else "unavailable"
        flags["child_stderr_present"] = report.get("child_stderr_present") is True
        child_stderr_class = report.get("child_stderr_class")
        allowed_stderr_classes = {"empty", "operation_not_permitted", "permission_denied", "missing_file",
                                  "python_dependency_import_failure", "python_traceback", "other"}
        flags["child_stderr_class"] = child_stderr_class if child_stderr_class in allowed_stderr_classes else "unavailable"
        nested_exit = report.get("nested_userns_process_exit_code")
        flags["nested_userns_exit_code"] = nested_exit if type(nested_exit) is int else "unavailable"
        nested_stderr = report.get("nested_userns_stderr_class")
        flags["nested_userns_stderr_class"] = nested_stderr if nested_stderr in allowed_stderr_classes else "unavailable"
        mapping_stage = report.get("nested_userns_mapping_stage")
        flags["nested_userns_mapping_stage"] = mapping_stage if mapping_stage in {
            "not_observed", "completed", "uid_mapping_failed", "gid_mapping_failed",
            "setgroups_mapping_failed", "user_namespace_creation_failed", "setup_before_payload"} else "unavailable"
        for key in ("nested_userns_errno_category", "nested_bwrap_errno_category"):
            value = report.get(key)
            flags[key] = value if value in {"operation_not_permitted", "permission_denied", "invalid_argument",
                                            "resource_limit", "missing_resource", "address_conflict",
                                            "other", "empty"} else "unavailable"
        for key in ("nested_userns_source_identity", "nested_bwrap_source_identity"):
            value = report.get(key)
            allowed_sources = {"util-linux-v2.39.3-sys-utils-unshare.c",
                               "bubblewrap-v0.9.0-bubblewrap.c+network.c", "unsupported"}
            flags[key] = value if value in allowed_sources else "unsupported"
        nested_errno = report.get("nested_mount_errno_category")
        flags["nested_mount_errno_category"] = nested_errno if nested_errno in {
            "operation_not_permitted", "permission_denied", "other", "unavailable"} else "unavailable"
        nested_cap_eff = report.get("nested_cap_eff")
        flags["nested_cap_eff"] = nested_cap_eff if isinstance(nested_cap_eff, str) and len(nested_cap_eff) <= 32 \
            and all(char in "0123456789abcdefABCDEF" for char in nested_cap_eff) else "unavailable"
    detail = {
        "process_code": code,
        "exit_code": exit_code,
        "stderr_class": stderr_category,
        "stderr": (stderr_excerpt or "<empty>")[:120],
        "canary": flags,
    }
    return ("bubblewrap internal canary failed: "
            + json.dumps(detail, sort_keys=True, separators=(",", ":")))[:640]


def _classify_namespace_setup_stderr(stderr: bytes, utility: str, utility_version: str | None = None) -> str:
    """Classify only explicit, versioned utility-stage diagnostics.

    Generic EPERM/EACCES remains a generic setup failure. The embedded Linux
    canary mirrors these finite markers and never publishes raw stderr.
    """
    text = stderr[:2048].decode("utf-8", "replace").casefold().replace("_", " ")
    if utility == "unshare":
        if utility_version not in (None, "2.39.3"):
            return "setup_before_payload"
        if "unshare failed" in text:
            return "user_namespace_creation_failed"
        if "uid map" in text:
            return "uid_mapping_failed"
        if "gid map" in text:
            return "gid_mapping_failed"
        if "setgroups" in text:
            return "setgroups_mapping_failed"
        return "setup_before_payload"
    if utility == "bubblewrap":
        if utility_version not in (None, "0.9.0"):
            return "setup_or_exec_failed"
        if ("unshare user ns" in text or "no permissions to creating new namespace" in text
                or "creating new namespace failed" in text):
            return "user_namespace_creation_failed"
        if "setting up uid map" in text:
            return "uid_mapping_failed"
        if "setting up gid map" in text:
            return "gid_mapping_failed"
        if "error writing to setgroups" in text:
            return "setgroups_mapping_failed"
        if ("loopback: failed rtm newaddr" in text
                and "operation not permitted" in text):
            return "loopback_setup_denied"
        if "loopback: failed rtm newlink" in text:
            return "loopback_link_setup_failed"
        for marker in ("failed to make / slave", "failed to mount tmpfs", "setting up newroot bind",
                       "pivot_root", "mount /proc failed", "mount /dev failed"):
            if marker in text:
                return "mount_setup_failed"
        return "setup_or_exec_failed"
    return "unclassified"


def _namespace_setup_errno_category(stderr: bytes) -> str:
    """Return only a finite errno category; never retain a utility message."""
    text = stderr[:2048].decode("utf-8", "replace").casefold()
    if "operation not permitted" in text:
        return "operation_not_permitted"
    if "permission denied" in text:
        return "permission_denied"
    if "invalid argument" in text:
        return "invalid_argument"
    if "no space left" in text or "enospc" in text:
        return "resource_limit"
    if "no such file or directory" in text:
        return "missing_resource"
    if "address already in use" in text:
        return "address_conflict"
    return "other" if text.strip() else "empty"


def _run_sandbox_canary(package_root: Path, outside: Path,
                        observed: dict[str, Any] | None = None) -> str | None:
    """Exercise real filesystem, network, profile-stack, and capability boundaries."""
    import inspect

    bwrap = shutil.which("bwrap")
    if not bwrap:
        return "bubblewrap is unavailable. Provision a supported bubblewrap runtime with user, network, and PID namespaces; adapters are not executed without an enforceable OS sandbox."
    python_exe = os.path.realpath(sys.executable)
    if not python_exe.startswith("/usr/"):
        return "The Python runtime is outside the isolated /usr system tree; provision the approved Python runtime under /usr."
    prefix = [bwrap, "--die-with-parent", "--new-session", "--unshare-all", "--cap-drop", "ALL",
              "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp",
              "--dir", "/usr", "--ro-bind", "/usr", "/usr"]
    for system_dir in ("/lib", "/lib64", "/bin", "/sbin"):
        if os.path.exists(system_dir):
            prefix.extend(["--dir", system_dir, "--ro-bind", system_dir, system_dir])
    prefix.extend(["--dir", "/release", "--ro-bind", str(package_root.resolve()), "/release",
                   "--chdir", "/release", "--clearenv", "--setenv", "PATH", "/usr/bin:/bin",
                   "--setenv", "HOME", "/nonexistent", "--setenv", "LC_ALL", "C.UTF-8"])
    child_code = r'''import json,os,socket,sys
CAPS=('CapInh','CapPrm','CapEff','CapBnd','CapAmb')
def context():
 try:
  label=open('/proc/self/attr/current',encoding='ascii').read(256).strip()
  status={line.split(':',1)[0]:line.split(':',1)[1].strip() for line in open('/proc/self/status',encoding='ascii') if ':' in line}
  values={name:int(status[name],16) for name in CAPS}
  stacked=label.endswith('(enforce)') and 'unpriv_bwrap' in label and 'bwrap' in label
  return {'label':label if stacked else 'unverified','stack':stacked,'no_new_privs':status.get('NoNewPrivs')=='1','caps_zero':all(value==0 for value in values.values())}
 except (OSError,ValueError,KeyError):
  return {'label':'unavailable','stack':False,'no_new_privs':False,'caps_zero':False}
outside=sys.argv[1];shadow=sys.argv[2]
host_net=int(sys.argv[3])
try:
 with open(outside,'rb') as source:source.read()
 outside_blocked=False
except OSError:
 outside_blocked=True
try:
 with open(shadow,'rb') as source:private_shadow_visible=source.read()==b'sandbox-shadow-write'
except OSError:
 private_shadow_visible=False
netns=os.stat('/proc/self/ns/net').st_ino != host_net
s=socket.socket(socket.AF_INET,socket.SOCK_STREAM);s.settimeout(0.2)
try:
 s.connect(('198.18.0.1',9));network_blocked=False
except OSError:
 network_blocked=True
c=context()
r={'outside_read_blocked':outside_blocked,'private_shadow_visible':private_shadow_visible,
   'network_blocked':network_blocked,'network_namespace_isolated':netns,
   'apparmor_label':c['label'],'apparmor_stack_verified':c['stack'],'no_new_privs':c['no_new_privs'],
   'capabilities_zero':c['caps_zero']}
print(json.dumps(r,sort_keys=True,separators=(',',':')))
sys.exit(0 if all((outside_blocked,private_shadow_visible,network_blocked,netns,c['stack'],c['no_new_privs'],c['caps_zero'])) else 7)
'''
    parent_code = r'''import ctypes,errno,json,os,re,selectors,signal,socket,subprocess,sys,time
CAPS=('CapInh','CapPrm','CapEff','CapBnd','CapAmb')
def context():
 try:
  label=open('/proc/self/attr/current',encoding='ascii').read(256).strip()
  status={line.split(':',1)[0]:line.split(':',1)[1].strip() for line in open('/proc/self/status',encoding='ascii') if ':' in line}
  values={name:int(status[name],16) for name in CAPS}
  stacked=label.endswith('(enforce)') and 'unpriv_bwrap' in label and 'bwrap' in label
  return {'label':label if stacked else 'unverified','stack':stacked,'no_new_privs':status.get('NoNewPrivs')=='1',
          'caps_zero':all(value==0 for value in values.values()),'values':values}
 except (OSError,ValueError,KeyError):
  return {'label':'unavailable','stack':False,'no_new_privs':False,'caps_zero':False,'values':{}}
def run_child(command,timeout=3):
 process=None;selector=None;buffers={'stdout':bytearray(),'stderr':bytearray()};failure=False
 try:
  process=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True,close_fds=True)
  selector=selectors.DefaultSelector()
  for stream,name in ((process.stdout,'stdout'),(process.stderr,'stderr')):
   os.set_blocking(stream.fileno(),False);selector.register(stream,selectors.EVENT_READ,name)
  deadline=time.monotonic()+timeout
  while selector.get_map() or process.poll() is None:
   remaining=deadline-time.monotonic()
   if remaining<=0:failure=True;break
   for key,_ in selector.select(min(0.1,remaining)):
    try:chunk=os.read(key.fileobj.fileno(),1024)
    except OSError:chunk=b''
    if not chunk:
     selector.unregister(key.fileobj);continue
    target=buffers[key.data];target.extend(chunk)
    if len(target)>2048:failure=True;break
   if failure:break
  if failure:
   os.killpg(process.pid,signal.SIGKILL)
  code=process.wait(timeout=1)
 except (OSError,subprocess.TimeoutExpired):
  failure=True;code=None
  if process is not None:
   try:os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=1);code=process.returncode
   except (OSError,subprocess.TimeoutExpired):pass
 finally:
  if selector is not None:selector.close()
  if process is not None:
   for stream in (process.stdout,process.stderr):
    try:stream.close()
    except OSError:pass
 return {'code':code,'stdout':bytes(buffers['stdout'][:2048]),'stderr':bytes(buffers['stderr'][:2048]),'bounded':not failure}
def parse_object(raw):
 try:
  value=json.loads(raw.decode('utf-8'))
  return value if isinstance(value,dict) else {}
 except (ValueError,UnicodeError,RecursionError):
  return {}
outside=sys.argv[1]; child_code=sys.argv[2]; host_net=int(sys.argv[3]); bwrap=sys.argv[4]
parent=context()
r={'outside_read_blocked':False,'private_shadow_visible':False,
   'sandbox_shadow_write_succeeded':False,'network_blocked':False,
   'network_namespace_isolated':os.stat('/proc/self/ns/net').st_ino != host_net,
   'sandbox_exec_capabilities_dropped':parent['caps_zero'],'apparmor_label':parent['label'],
   'apparmor_stack_verified':parent['stack'],'payload_no_new_privs':parent['no_new_privs'],
   'payload_capability_sets_zero':parent['caps_zero'],'python_reexec_stack_verified':False,
   'python_reexec_label':'unverified','python_reexec_capabilities_zero':False,'python_reexec_no_new_privs':False,
   'other_exec_label':'unverified',
   'other_exec_stack_verified':False,'other_exec_capabilities_zero':False,
   'other_exec_no_new_privs':False,'nested_userns_created':False,'nested_userns_setup_complete':False,
   'nested_userns_stack_verified':False,'nested_userns_label':'unverified','nested_userns_no_new_privs':False,
   'nested_userns_process_exit_code':None,'nested_userns_stderr_class':'not_run',
   'nested_userns_setup_stage':'not_run','nested_userns_payload_reached':False,
   'nested_userns_creation_attempted':False,'nested_userns_mapping_requested':False,
   'nested_userns_mapping_completed':False,
   'nested_userns_creation_observation':'not_observed','nested_userns_mapping_observation':'not_observed',
   'nested_userns_mapping_stage':'not_observed','nested_userns_errno_category':'unavailable',
   'nested_userns_source_identity':'unsupported',
   'nested_userns_utility_version':'unavailable','nested_userns_utility_version_exit_code':None,
   'nested_userns_stderr_bytes':0,
   'nested_userns_process_bounded':False,
   'nested_capability_sets_observed':False,'nested_capability_sets':{},
   'nested_mount_attempted':False,'nested_mount_namespace_denied':False,
   'nested_mount_namespace_created':False,'nested_mount_errno_category':'unavailable',
   'nested_cap_eff':'','nested_bwrap_denied':False,'nested_bwrap_setup_stage':'not_run',
   'nested_bwrap_payload_reached':False,'nested_bwrap_exit_code':None,
   'nested_bwrap_utility_version':'unavailable','nested_bwrap_utility_version_exit_code':None,
   'nested_bwrap_stderr_bytes':0,
   'nested_bwrap_stderr_class':'not_run','nested_bwrap_errno_category':'unavailable',
   'nested_bwrap_source_identity':'unsupported',
   'child_isolated':False,'child_capabilities_dropped':False,'child_exit_code':None,
   'child_stderr_present':False,'child_stderr_class':'not_run'}
try:
 open(outside,'rb').read();r['outside_read_blocked']=False
except OSError:
 r['outside_read_blocked']=True
try:
 shadow='/tmp/acv1-private-shadow-'+str(os.getpid())
 if shadow!=outside and not os.path.lexists(shadow):
  descriptor=os.open(shadow,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
  try:os.write(descriptor,b'sandbox-shadow-write')
  finally:os.close(descriptor)
  with open(shadow,'rb') as target:r['sandbox_shadow_write_succeeded']=(target.read()==b'sandbox-shadow-write')
except OSError:
 pass
try:
 s=socket.socket(socket.AF_INET,socket.SOCK_STREAM);s.settimeout(0.2);s.connect(('198.18.0.1',9))
except OSError:
 r['network_blocked']=True
child=run_child([sys.executable,'-I','-B','-S','-c',child_code,outside,shadow,str(host_net)])
child_report=parse_object(child['stdout'])
r['child_exit_code']=child['code'];r['child_stderr_present']=bool(child['stderr'])
child_error=child['stderr'].decode('utf-8','replace').lower()
r['child_stderr_class']=('operation_not_permitted' if 'operation not permitted' in child_error else
 'permission_denied' if 'permission denied' in child_error else 'missing_file' if 'no such file or directory' in child_error else
 'python_dependency_import_failure' if 'modulenotfounderror' in child_error or 'no module named' in child_error else
 'python_traceback' if 'traceback' in child_error else 'other' if child_error else 'empty')
r['private_shadow_visible']=child_report.get('private_shadow_visible') is True
r['child_isolated']=(child['code']==0 and child['bounded'] and child_report.get('outside_read_blocked') is True
 and r['private_shadow_visible'] and child_report.get('network_blocked') is True
 and child_report.get('network_namespace_isolated') is True
 and child_report.get('apparmor_stack_verified') is True and child_report.get('no_new_privs') is True
 and child_report.get('capabilities_zero') is True)
r['child_capabilities_dropped']=child_report.get('capabilities_zero') is True
r['python_reexec_label']=child_report.get('apparmor_label','unverified')
r['python_reexec_no_new_privs']=child_report.get('no_new_privs') is True
r['python_reexec_capabilities_zero']=child_report.get('capabilities_zero') is True
r['python_reexec_stack_verified']=child_report.get('apparmor_stack_verified') is True

shell_code="""profile=; IFS= read -r profile < /proc/self/attr/current
nnp=; inh=; prm=; eff=; bnd=; amb=
while IFS=: read -r key value; do
 value=\"${value#\"${value%%[![:space:]]*}\"}\"
 case \"$key\" in
  NoNewPrivs) nnp=$value;; CapInh) inh=$value;; CapPrm) prm=$value;; CapEff) eff=$value;; CapBnd) bnd=$value;; CapAmb) amb=$value;;
 esac
done < /proc/self/status
printf 'profile=%s\\nNoNewPrivs=%s\\nCapInh=%s\\nCapPrm=%s\\nCapEff=%s\\nCapBnd=%s\\nCapAmb=%s\\n' \"$profile\" \"$nnp\" \"$inh\" \"$prm\" \"$eff\" \"$bnd\" \"$amb\""""
shell=run_child(['/bin/sh','-c',shell_code])
shell_text=shell['stdout'].decode('ascii','replace')
shell_values={}
for line in shell_text.splitlines():
 if '=' in line:
  key,value=line.split('=',1);shell_values[key]=value
shell_label=shell_values.get('profile','')
shell_stack=shell_label.endswith('(enforce)') and 'bwrap' in shell_label and 'unpriv_bwrap' in shell_label
r['other_exec_stack_verified']=shell['code']==0 and shell['bounded'] and shell_stack
r['other_exec_label']=shell_label if shell_stack else 'unverified'
r['other_exec_no_new_privs']=shell_values.get('NoNewPrivs')=='1'
try:
 r['other_exec_capabilities_zero']=all(int(shell_values[name],16)==0 for name in ('CapInh','CapPrm','CapEff','CapBnd','CapAmb'))
except (KeyError,ValueError):
 r['other_exec_capabilities_zero']=False

nested_code=r"""import ctypes,errno,json,os,sys
def status():return {line.split(':',1)[0]:line.split(':',1)[1].strip() for line in open('/proc/self/status',encoding='ascii') if ':' in line}
try:
 label=open('/proc/self/attr/current',encoding='ascii').read(256).strip();s=status()
 stacked=label.endswith('(enforce)') and 'bwrap' in label and 'unpriv_bwrap' in label
 capabilities={name:s.get(name,'') for name in ('CapInh','CapPrm','CapEff','CapBnd','CapAmb')}
 libc=ctypes.CDLL(None,use_errno=True);libc.unshare.argtypes=[ctypes.c_int]
 ctypes.set_errno(0);result=libc.unshare(0x00020000);error=ctypes.get_errno()
 denied=result<0 and error in (errno.EPERM,errno.EACCES)
 errno_category=('operation_not_permitted' if error==errno.EPERM else
                 'permission_denied' if error==errno.EACCES else 'other' if result<0 else 'unavailable')
 report={'created':True,'mapping_completed':True,'payload_reached':True,
         'label':label if stacked else 'unverified','stack':stacked,
         'no_new_privs':s.get('NoNewPrivs')=='1','capabilities':capabilities,
         'cap_eff':capabilities['CapEff'],'mount_attempted':True,
         'mount_namespace_created':result==0,'mount_denied':denied,
         'mount_errno_category':errno_category}
 print(json.dumps(report,sort_keys=True,separators=(',',':')))
 sys.exit(0 if stacked and report['no_new_privs'] and denied else 9)
except (OSError,ValueError,KeyError):
 print('{"created":true,"mapping_completed":true,"payload_reached":true,'
       '"diagnostic_stage":"payload_setup_incomplete"}');sys.exit(10)
"""
unshare=__import__('shutil').which('unshare')
if unshare:
 version_run=run_child([unshare,'--version'],timeout=2)
 version_text=version_run['stdout'].decode('ascii','ignore').strip()
 version_match=re.fullmatch(r'unshare from util-linux ([0-9A-Za-z.+~-]{1,48})',version_text)
 r['nested_userns_utility_version_exit_code']=version_run['code']
 if version_run['bounded'] and version_run['code']==0 and version_match and not version_run['stderr']:
  r['nested_userns_utility_version']=version_match.group(1)
 if r['nested_userns_utility_version']=='2.39.3':
  r['nested_userns_source_identity']='util-linux-v2.39.3-sys-utils-unshare.c'
 r['nested_userns_creation_attempted']=True
 r['nested_userns_mapping_requested']=True
 nested=run_child([unshare,'--map-user=0',sys.executable,'-I','-B','-S','-c',nested_code],timeout=4)
 nested_report=parse_object(nested['stdout'])
 payload_reached=(nested_report.get('payload_reached') is True or nested_report.get('created') is True)
 r['nested_userns_process_bounded']=nested['bounded'] is True
 r['nested_userns_payload_reached']=payload_reached
 r['nested_userns_mapping_completed']=nested_report.get('mapping_completed') is True
 r['nested_userns_creation_observation']=('confirmed' if nested_report.get('created') is True else 'not_observed')
 r['nested_userns_mapping_observation']=('completed' if nested_report.get('mapping_completed') is True else 'not_observed')
 r['nested_userns_stderr_bytes']=len(nested.get('stderr',b''))
 r['nested_userns_errno_category']=_namespace_setup_errno_category(nested.get('stderr',b''))
 if nested_report.get('created') is True and nested_report.get('mount_attempted') is True:
  r['nested_userns_setup_stage']='mount_attempted'
 elif payload_reached:
  r['nested_userns_setup_stage']='payload_setup_incomplete'
 elif not nested['bounded']:
  r['nested_userns_setup_stage']='timeout_or_output_limit'
 else:
  r['nested_userns_setup_stage']=_classify_namespace_setup_stderr(
   nested.get('stderr',b''),'unshare',r['nested_userns_utility_version'])
  if r['nested_userns_setup_stage'] in ('uid_mapping_failed','gid_mapping_failed','setgroups_mapping_failed'):
   r['nested_userns_setup_stage']='mapping_setup_failed'
 r['nested_userns_created']=nested_report.get('created') is True and nested['bounded']
 r['nested_userns_setup_complete']=(nested_report.get('created') is True
  and nested_report.get('mount_attempted') is True and isinstance(nested_report.get('capabilities'),dict)
  and nested['bounded'])
 r['nested_userns_label']=nested_report.get('label','unverified')
 r['nested_userns_stack_verified']=nested_report.get('stack') is True
 r['nested_userns_no_new_privs']=nested_report.get('no_new_privs') is True
 capability_names=('CapInh','CapPrm','CapEff','CapBnd','CapAmb')
 capability_sets=nested_report.get('capabilities')
 if (isinstance(capability_sets,dict) and set(capability_sets)==set(capability_names)
  and all(isinstance(capability_sets[name],str) and 1<=len(capability_sets[name])<=32
   and all(char in '0123456789abcdefABCDEF' for char in capability_sets[name]) for name in capability_names)):
  r['nested_capability_sets_observed']=True
  r['nested_capability_sets']=capability_sets
  r['nested_cap_eff']=capability_sets['CapEff']
 r['nested_mount_attempted']=nested_report.get('mount_attempted') is True
 r['nested_mount_namespace_denied']=nested_report.get('mount_denied') is True
 r['nested_mount_namespace_created']=nested_report.get('mount_namespace_created') is True
 if nested_report.get('mount_errno_category') in ('operation_not_permitted','permission_denied','other','unavailable'):
  r['nested_mount_errno_category']=nested_report['mount_errno_category']
 r['nested_userns_process_exit_code']=nested.get('code') if type(nested.get('code')) is int else None
 nested_error=nested.get('stderr',b'').decode('utf-8','replace').lower()
 normalized_nested_error=nested_error.replace('_',' ')
 r['nested_userns_stderr_class']=('mapping_setup_failure' if any(term in normalized_nested_error for term in ('uid map','gid map','setgroups')) else
  'operation_not_permitted' if 'operation not permitted' in nested_error else
  'permission_denied' if 'permission denied' in nested_error else 'missing_file' if 'no such file or directory' in nested_error else
  'other' if nested_error else 'empty')
 if any(term in normalized_nested_error for term in ('uid map','gid map','setgroups')):
  r['nested_userns_setup_stage']='mapping_setup_failed'
  r['nested_userns_mapping_observation']='failed'
 elif r['nested_userns_setup_stage']=='user_namespace_creation_failed':
  r['nested_userns_creation_observation']=('denied' if r['nested_userns_stderr_class'] in
   ('operation_not_permitted','permission_denied') else 'failed')
 r['nested_userns_mapping_stage']=(_classify_namespace_setup_stderr(
  nested.get('stderr',b''),'unshare',r['nested_userns_utility_version'])
  if r['nested_userns_mapping_observation']=='failed' else
  'completed' if r['nested_userns_mapping_observation']=='completed' else 'not_observed')
else:
 r['nested_userns_setup_stage']='tool_unavailable'

version_run=run_child([bwrap,'--version'],timeout=2)
version_text=version_run['stdout'].decode('ascii','ignore').strip()
version_match=re.fullmatch(r'bubblewrap ([0-9A-Za-z.+~-]{1,48})',version_text)
r['nested_bwrap_utility_version_exit_code']=version_run['code']
if version_run['bounded'] and version_run['code']==0 and version_match and not version_run['stderr']:
 r['nested_bwrap_utility_version']=version_match.group(1)
if r['nested_bwrap_utility_version']=='0.9.0':
 r['nested_bwrap_source_identity']='bubblewrap-v0.9.0-bubblewrap.c+network.c'
payload_code="import os;os.write(1,b'ACV1_NESTED_PAYLOAD_REACHED\\n')"
inner=run_child([bwrap,'--die-with-parent','--new-session','--unshare-user','--unshare-ipc',
                 '--unshare-pid','--unshare-uts','--unshare-net','--cap-drop','ALL',
                 sys.executable,'-I','-B','-S','-c',payload_code],timeout=4)
inner_error=inner['stderr'].decode('utf-8','replace').lower()
inner_output=inner['stdout']
r['nested_bwrap_payload_reached']=b'ACV1_NESTED_PAYLOAD_REACHED' in inner_output
r['nested_bwrap_exit_code']=inner['code'] if type(inner.get('code')) is int else None
r['nested_bwrap_stderr_bytes']=len(inner.get('stderr',b''))
r['nested_bwrap_errno_category']=_namespace_setup_errno_category(inner.get('stderr',b''))
r['nested_bwrap_stderr_class']=('network_namespace_loopback_setup_denied'
 if 'bwrap: loopback: failed rtm_newaddr: operation not permitted' in inner_error else
 'operation_not_permitted' if 'operation not permitted' in inner_error else
 'permission_denied' if 'permission denied' in inner_error else
 'other' if inner_error else 'empty')
if r['nested_bwrap_payload_reached']:
 r['nested_bwrap_setup_stage']='payload_reached'
elif not inner['bounded']:
 r['nested_bwrap_setup_stage']='timeout_or_output_limit'
else:
 r['nested_bwrap_setup_stage']=_classify_namespace_setup_stderr(
  inner.get('stderr',b''),'bubblewrap',r['nested_bwrap_utility_version'])
 if inner['code'] is None or inner['code']==0:
  r['nested_bwrap_setup_stage']='not_observed'
r['nested_bwrap_denied']=(inner['bounded'] and inner['code'] is not None and inner['code']!=0
 and r['nested_bwrap_setup_stage']=='loopback_setup_denied')
print(json.dumps(r,sort_keys=True,separators=(',',':')))
required=('outside_read_blocked','sandbox_shadow_write_succeeded','network_blocked','network_namespace_isolated',
 'private_shadow_visible',
 'sandbox_exec_capabilities_dropped','apparmor_stack_verified','payload_no_new_privs','payload_capability_sets_zero',
 'python_reexec_stack_verified','python_reexec_capabilities_zero','python_reexec_no_new_privs',
 'other_exec_stack_verified','other_exec_capabilities_zero','other_exec_no_new_privs','nested_userns_created',
 'nested_userns_setup_complete','nested_userns_stack_verified','nested_userns_no_new_privs',
 'nested_userns_creation_attempted','nested_userns_mapping_requested',
 'nested_userns_mapping_completed','nested_userns_payload_reached',
 'nested_capability_sets_observed','nested_mount_attempted','nested_mount_namespace_denied',
 'nested_bwrap_denied','child_isolated','child_capabilities_dropped')
sys.exit(0 if all(r.get(key) is True for key in required) and r['nested_mount_errno_category'] in ('operation_not_permitted','permission_denied')
 and r['nested_userns_process_exit_code']==0 and r['nested_userns_stderr_class']=='empty' and r['child_exit_code']==0
 and not r['child_stderr_present'] and r['child_stderr_class']=='empty' else 8)
'''
    try:
        host_network_namespace = os.stat("/proc/self/ns/net").st_ino
    except OSError:
        return "The host network namespace identity cannot be inspected; no owner adapter is executed."
    command = prefix + [python_exe, "-I", "-B", "-S", "-c", parent_code,
                        str(outside), child_code, str(host_network_namespace), bwrap]
    classifier_source = (inspect.getsource(_classify_namespace_setup_stderr) + "\n"
                         + inspect.getsource(_namespace_setup_errno_category))
    parent_code = classifier_source + "\n" + parent_code
    command[command.index("-c") + 1] = parent_code
    run = run_bounded(command, cwd=package_root,
                      env={"PATH": "/usr/bin:/bin", "PYTHONIOENCODING": "utf-8", "PYTHONHASHSEED": "0",
                           "PYTHONDONTWRITEBYTECODE": "1", "HOME": "/nonexistent", "LC_ALL": "C.UTF-8"},
                      input_bytes=b"", timeout=20, stdout_limit=4096, stderr_limit=4096)
    report = None
    try:
        if run.get("stdout"):
            report = parse_json_bytes(run["stdout"])
    except (ValueError, UnicodeError, json.JSONDecodeError, RecursionError):
        report = None
    if observed is not None:
        observed.clear()
        if isinstance(report, dict):
            allowed = {
                "outside_read_blocked", "private_shadow_visible", "sandbox_shadow_write_succeeded", "network_blocked",
                "network_namespace_isolated", "sandbox_exec_capabilities_dropped", "apparmor_label",
                "python_reexec_label", "other_exec_label", "nested_userns_label",
                "apparmor_stack_verified", "payload_no_new_privs", "payload_capability_sets_zero",
                "python_reexec_stack_verified", "python_reexec_capabilities_zero",
                "python_reexec_no_new_privs", "other_exec_stack_verified", "other_exec_capabilities_zero",
                "other_exec_no_new_privs", "nested_userns_created", "nested_userns_stack_verified",
                "nested_userns_no_new_privs", "nested_userns_setup_complete", "nested_cap_eff",
                "nested_userns_process_exit_code", "nested_userns_stderr_class",
                "nested_userns_setup_stage", "nested_userns_payload_reached",
                "nested_userns_creation_attempted", "nested_userns_mapping_requested",
                "nested_userns_mapping_completed",
                "nested_userns_creation_observation", "nested_userns_mapping_observation",
                "nested_userns_utility_version", "nested_userns_utility_version_exit_code",
                "nested_userns_stderr_bytes", "nested_userns_process_bounded",
                "nested_capability_sets_observed", "nested_capability_sets",
                "nested_mount_attempted", "nested_mount_namespace_denied", "nested_mount_namespace_created",
                "nested_mount_errno_category",
                "nested_bwrap_denied", "nested_bwrap_setup_stage", "nested_bwrap_payload_reached",
                "nested_bwrap_exit_code", "nested_bwrap_utility_version",
                "nested_bwrap_utility_version_exit_code", "nested_bwrap_stderr_bytes",
                "nested_bwrap_stderr_class", "child_isolated",
                "child_capabilities_dropped", "child_exit_code", "child_stderr_present", "child_stderr_class",
            }
            observed.update({key: report[key] for key in allowed if key in report})
        observed["process_code"] = run.get("code", "unavailable")
        observed["exit_code"] = run.get("returncode")
    if not run.get("ok"):
        return _sandbox_process_failure(run, report)
    if (not isinstance(report, dict) or report.get("outside_read_blocked") is not True
            or report.get("private_shadow_visible") is not True
            or report.get("sandbox_shadow_write_succeeded") is not True
            or report.get("network_blocked") is not True
            or report.get("network_namespace_isolated") is not True
            or report.get("sandbox_exec_capabilities_dropped") is not True
            or report.get("apparmor_stack_verified") is not True
            or report.get("payload_no_new_privs") is not True
            or report.get("payload_capability_sets_zero") is not True
            or report.get("python_reexec_stack_verified") is not True
            or report.get("python_reexec_capabilities_zero") is not True
            or report.get("python_reexec_no_new_privs") is not True
            or report.get("other_exec_stack_verified") is not True
            or report.get("other_exec_capabilities_zero") is not True
            or report.get("other_exec_no_new_privs") is not True
            or report.get("nested_userns_created") is not True
            or report.get("nested_userns_setup_complete") is not True
            or report.get("nested_userns_stack_verified") is not True
            or report.get("nested_userns_no_new_privs") is not True
            or report.get("nested_userns_creation_attempted") is not True
            or report.get("nested_userns_mapping_requested") is not True
            or report.get("nested_userns_mapping_completed") is not True
            or report.get("nested_userns_payload_reached") is not True
            or report.get("nested_userns_setup_stage") != "mount_attempted"
            or report.get("nested_userns_utility_version") == "unavailable"
            or report.get("nested_capability_sets_observed") is not True
            or report.get("nested_mount_attempted") is not True
            or report.get("nested_mount_namespace_denied") is not True
            or report.get("nested_mount_namespace_created") is not False
            or report.get("nested_mount_errno_category") not in {"operation_not_permitted", "permission_denied"}
            or report.get("nested_userns_process_exit_code") != 0
            or report.get("nested_userns_stderr_class") != "empty"
            or report.get("nested_bwrap_denied") is not True
            or report.get("nested_bwrap_setup_stage") != "loopback_setup_denied"
            or report.get("nested_bwrap_utility_version") == "unavailable"
            or type(report.get("nested_bwrap_exit_code")) is not int
            or report.get("nested_bwrap_stderr_class") != "network_namespace_loopback_setup_denied"
            or report.get("child_isolated") is not True
            or report.get("child_capabilities_dropped") is not True
            or type(report.get("child_exit_code")) is not int or report.get("child_exit_code") != 0
            or report.get("child_stderr_present") is not False or report.get("child_stderr_class") != "empty"):
        return _sandbox_process_failure({"code": "adapter_nonzero_exit", "returncode": 0, "stderr": b""}, report)
    return None


def _observe_host_canary(path: Path, expected: bytes,
                         observed: dict[str, Any]) -> None:
    """Read the synthetic host canary independently of sandbox-canary success."""
    observed.pop("host_canary_unchanged", None)
    observed["host_canary_observation"] = "unavailable"
    descriptor: int | None = None
    try:
        before = os.lstat(path)
        if not stat.S_ISREG(before.st_mode):
            observed["host_canary_observation"] = "not_regular"
            return
        if before.st_size < 0 or before.st_size > 256:
            observed["host_canary_observation"] = "outside_size_bound"
            return
        nofollow = getattr(os, "O_NOFOLLOW", 0)
        if nofollow == 0:
            return
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | nofollow)
        opened = os.fstat(descriptor)
        identity_fields = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid", "st_size",
                           "st_mtime_ns", "st_ctime_ns")
        if any(getattr(before, key) != getattr(opened, key) for key in identity_fields):
            observed["host_canary_observation"] = "unstable"
            return
        data = bytearray()
        while len(data) <= 256:
            chunk = os.read(descriptor, 257 - len(data))
            if not chunk:
                break
            data.extend(chunk)
        after = os.fstat(descriptor)
        current = os.lstat(path)
        if (any(getattr(before, key) != getattr(after, key) for key in identity_fields)
                or any(getattr(before, key) != getattr(current, key) for key in identity_fields)
                or len(data) > 256 or len(data) != before.st_size):
            observed["host_canary_observation"] = "unstable"
            return
        if bytes(data) == expected:
            observed["host_canary_observation"] = "unchanged"
            observed["host_canary_unchanged"] = True
        else:
            observed["host_canary_observation"] = "changed"
            observed["host_canary_unchanged"] = False
    except OSError:
        observed["host_canary_observation"] = "unavailable"
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass


def probe_sandbox(package_root: Path, observed: dict[str, Any] | None = None) -> str | None:
    """Require an offline canary proving host paths and host networking are isolated."""
    if platform.system() != "Linux":
        return "The current platform has no accepted supported no-network filesystem sandbox backend; macOS sandbox-exec is deprecated and is not used as a security boundary. Provision reviewed bubblewrap on a supported Linux runner or another audited OS sandbox."
    prerequisite = _sandbox_prerequisite_error()
    if prerequisite is not None:
        return prerequisite
    try:
        import tempfile
        with tempfile.TemporaryDirectory(prefix="acv1-sandbox-canary-") as temp:
            outside = Path(temp) / "host-canary.txt"
            host_canary = b"synthetic-host-canary"
            outside.write_bytes(host_canary)
            canary_observations = observed if observed is not None else {}
            reason = None
            try:
                reason = _run_sandbox_canary(package_root, outside, canary_observations)
            finally:
                _observe_host_canary(outside, host_canary, canary_observations)
            if reason is not None:
                return reason
            if canary_observations.get("host_canary_unchanged") is not True:
                return "bubblewrap host-canary preservation was not verified; the owner adapter is not executed."
    except (OSError, subprocess.SubprocessError, ValueError, TypeError):
        return "bubblewrap isolation prerequisite could not be verified with synthetic canaries; the owner adapter is not executed."
    return None


def _kill_process_group(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except (OSError, ProcessLookupError):
        try:
            process.kill()
        except OSError:
            pass


def run_bounded(command: list[str], *, cwd: Path, env: dict[str, str], input_bytes: bytes,
                timeout: float, stdout_limit: int, stderr_limit: int,
                pass_fds: tuple[int, ...] = ()) -> dict[str, Any]:
    """Run a synthetic or sandboxed child with bounded pipes and group cleanup."""
    try:
        process = subprocess.Popen(command, cwd=str(cwd), env=env, stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   start_new_session=True, close_fds=True, pass_fds=pass_fds)
    except (OSError, ValueError):
        return {"ok": False, "code": "execution_start_failed", "stdout": b"", "stderr": b""}
    assert process.stdin is not None and process.stdout is not None and process.stderr is not None
    try:
        selector = selectors.DefaultSelector()
        os.set_blocking(process.stdin.fileno(), False)
        os.set_blocking(process.stdout.fileno(), False)
        os.set_blocking(process.stderr.fileno(), False)
        selector.register(process.stdout, selectors.EVENT_READ, "stdout")
        selector.register(process.stderr, selectors.EVENT_READ, "stderr")
        input_offset = 0
        if input_bytes:
            selector.register(process.stdin, selectors.EVENT_WRITE, "stdin")
        else:
            process.stdin.close()
        buffers = {"stdout": bytearray(), "stderr": bytearray()}
        limits = {"stdout": stdout_limit, "stderr": stderr_limit}
        deadline = time.monotonic() + timeout
        code: str | None = None
        while selector.get_map() or process.poll() is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                code = "execution_timeout"
                _kill_process_group(process)
                break
            events = selector.select(min(0.1, remaining))
            for key, _ in events:
                if key.data == "stdin":
                    try:
                        written = os.write(key.fileobj.fileno(), input_bytes[input_offset:input_offset + 8192])
                    except BlockingIOError:
                        continue
                    except OSError:
                        written = 0
                        input_offset = len(input_bytes)
                    else:
                        input_offset += written
                    if input_offset >= len(input_bytes):
                        try:
                            selector.unregister(key.fileobj)
                        except Exception:
                            pass
                        try:
                            key.fileobj.close()
                        except OSError:
                            pass
                    continue
                try:
                    chunk = os.read(key.fileobj.fileno(), 8192)
                except (BlockingIOError, OSError):
                    chunk = b""
                if not chunk:
                    try:
                        selector.unregister(key.fileobj)
                    except Exception:
                        pass
                    continue
                target = buffers[key.data]
                target.extend(chunk)
                if len(target) > limits[key.data]:
                    code = "execution_output_limit_exceeded"
                    _kill_process_group(process)
                    break
            if code is not None:
                break
        if code is not None:
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                _kill_process_group(process)
                process.wait()
            selector.close()
            return {"ok": False, "code": code, "stdout": bytes(buffers["stdout"][:stdout_limit]),
                    "stderr": bytes(buffers["stderr"][:stderr_limit])}
        returncode = process.wait()
        selector.close()
        return {"ok": returncode == 0, "code": "adapter_nonzero_exit" if returncode else "ok",
                "returncode": returncode, "stdout": bytes(buffers["stdout"]), "stderr": bytes(buffers["stderr"])}
    finally:
        for stream in (process.stdin, process.stdout, process.stderr):
            try:
                stream.close()
            except OSError:
                pass
        if process.poll() is None:
            _kill_process_group(process)
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def _validate_adapter_result(raw: bytes, request: dict[str, Any], fixtures: dict[str, dict[str, Any]],
                             schema_hashes: dict[str, str], errors: list[dict[str, str]]) -> dict[str, Any] | None:
    try:
        response = parse_json_bytes(raw)
    except (UnicodeError, json.JSONDecodeError, ValueError, RecursionError):
        add_error(errors, "canonical_validator_output_invalid", "$.canonical_release.validator_adapter",
                  "shared-contracts", "Return one bounded typed JSON report with observed input hashes and actual serialized output bytes.")
        return None
    if not isinstance(response, dict) or response.get("protocol") != ADAPTER_PROTOCOL:
        add_error(errors, "canonical_validator_protocol_mismatch", "$.canonical_release.validator_adapter",
                  "shared-contracts", f"Publish a validator implementing {ADAPTER_PROTOCOL}; legacy echo-response adapters are rejected.")
        return None
    if response.get("release_revision") != request.get("release_revision"):
        add_error(errors, "canonical_validator_revision_mismatch", "$.canonical_release.validator_adapter",
                  "shared-contracts", "Return observations tied to the exact pinned canonical release revision.")
        return None
    results = response.get("results")
    if not isinstance(results, list):
        add_error(errors, "canonical_validator_results_missing", "$.canonical_release.validator_adapter.results",
                  "shared-contracts", "Return an observed result for each mapped owner fixture.")
        return None
    by_id: dict[str, dict[str, Any]] = {}
    for result in results:
        if not isinstance(result, dict) or not isinstance(result.get("case_id"), str):
            add_error(errors, "canonical_validator_result_malformed", "$.canonical_release.validator_adapter.results",
                      "shared-contracts", "Return a typed case result with observed schema/fixture hashes and serialized bytes.")
            continue
        case_id = result["case_id"]
        if case_id in by_id:
            add_error(errors, "canonical_validator_duplicate_result", "$.canonical_release.validator_adapter.results",
                      "shared-contracts", "Return exactly one observed result per required fixture case.")
        by_id[case_id] = result
    output_digests: dict[str, str] = {}
    for case_id, fixture in fixtures.items():
        result = by_id.get(case_id)
        if result is None:
            add_error(errors, "canonical_validator_case_result_missing", "$.canonical_release.validator_adapter.results",
                      "shared-contracts", f"Return an observed result for fixture case {case_id!r}.")
            continue
        schema_id = fixture.get("schema_id")
        if result.get("observed_schema_sha256") != schema_hashes.get(schema_id):
            add_error(errors, "canonical_validator_schema_observation_mismatch", "$.canonical_release.validator_adapter.results",
                      "shared-contracts", f"Read and report the exact pinned schema bytes used for case {case_id!r}.")
        if result.get("observed_fixture_sha256") != fixture.get("sha256"):
            add_error(errors, "canonical_validator_fixture_observation_mismatch", "$.canonical_release.validator_adapter.results",
                      "shared-contracts", f"Read and report the exact pinned fixture bytes used for case {case_id!r}.")
        if result.get("contract_id") != fixture.get("contract_id") or result.get("primitive") != fixture.get("primitive"):
            add_error(errors, "canonical_validator_case_identity_mismatch", "$.canonical_release.validator_adapter.results",
                      "shared-contracts", f"Bind the observed result to the exact owner contract and primitive for case {case_id!r}.")
        if result.get("outcome") != fixture.get("expected_outcome"):
            add_error(errors, "canonical_fixture_outcome_mismatch", "$.canonical_release.validator_adapter.results",
                      "shared-contracts", f"Make observed fixture case {case_id!r} match the owner-published semantic decision.")
        encoded = result.get("serialized_bytes_base64")
        expected_serialized = fixture.get("serialized_sha256")
        if expected_serialized is not None:
            if not isinstance(encoded, str) or len(encoded) > MAX_ADAPTER_STDOUT:
                add_error(errors, "canonical_serialized_bytes_missing", "$.canonical_release.validator_adapter.results",
                          "shared-contracts", f"Return actual canonical serialized bytes for case {case_id!r}; a digest echo is not evidence.")
                continue
            try:
                actual_bytes = base64.b64decode(encoded.encode("ascii"), validate=True)
            except (UnicodeError, ValueError):
                actual_bytes = b""
            actual_digest = sha256_bytes(actual_bytes) if actual_bytes else ""
            if actual_digest != expected_serialized:
                add_error(errors, "canonical_serialization_hash_mismatch", "$.canonical_release.validator_adapter.results",
                          "shared-contracts", f"Hash actual serialized bytes for case {case_id!r} and match the owner-published reference bytes.")
            else:
                output_digests[case_id] = actual_digest
    extras = set(by_id) - set(REQUIRED_CASES)
    if extras:
        add_error(errors, "canonical_validator_unexpected_case_result", "$.canonical_release.validator_adapter.results",
                  "shared-contracts", "Return only fixture case IDs declared in the owner-published release descriptor.")
    if errors:
        return None
    return {"serialized_output_sha256": output_digests}


def validate_and_run(manifest: Any, manifest_path: Path, errors: list[dict[str, str]],
                     evidence: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(manifest, dict):
        add_error(errors, "manifest_object_required", "$", "Agent and research",
                  "Supply the versioned canonical owner release descriptor as a JSON object.")
        return None
    if manifest.get("format") != FORMAT:
        add_error(errors, "readiness_descriptor_format_unsupported", "$.format", "Agent and research",
                  f"Publish a descriptor using {FORMAT}; v1 manifests lack trusted content binding and exact field/fixture crosswalks.")
    if manifest.get("status") != "published":
        owner_actions = manifest.get("owner_actions")
        if isinstance(owner_actions, list) and owner_actions:
            for index, action in enumerate(owner_actions):
                if isinstance(action, dict) and isinstance(action.get("owner"), str) and isinstance(action.get("action"), str):
                    add_error(errors, "canonical_release_not_published", f"$.owner_actions[{index}]",
                              action["owner"], action["action"])
        else:
            for item in OWNER_ACTIONS:
                add_error(errors, "canonical_release_not_published", "$.canonical_release", item["owner"], item["action"])
        return None
    if evidence is None:
        add_error(errors, "owner_evidence_index_unavailable", "$.owner_evidence_index", "Agent and research",
                  "Provision the independently verified owner-evidence index outside the repository and pin its exact SHA-256 in trusted runner configuration; local manifest assertions are not approval evidence.")

    base = manifest_path.resolve().parent
    verified_artifacts: dict[Path, tuple[bytes, str]] = VerifiedArtifactCache()
    release = manifest.get("canonical_release")
    if not isinstance(release, dict):
        release = {}
        add_error(errors, "canonical_release_object_missing", "$.canonical_release", "shared-contracts",
                  "Publish exact canonical release metadata and owner artifacts.")
    if release.get("repository") != "Jevelopers-neatHack-2026/shared-contracts":
        add_error(errors, "canonical_repository_mismatch", "$.canonical_release.repository", "shared-contracts",
                  "Publish the canonical release from shared-contracts.")
    if not revision_ok(release.get("revision")):
        add_error(errors, "canonical_revision_missing", "$.canonical_release.revision", "shared-contracts",
                  "Pin the canonical release to its exact 40-character owner commit revision.")
    if not isinstance(release.get("version"), str) or not release["version"].strip():
        add_error(errors, "canonical_version_missing", "$.canonical_release.version", "shared-contracts",
                  "Publish an explicit canonical contract release version.")

    schema_rows = release.get("schema_artifacts")
    schemas: dict[str, dict[str, Any]] = {}
    schema_hashes: dict[str, str] = {}
    if not isinstance(schema_rows, list) or not schema_rows:
        add_error(errors, "canonical_schema_artifacts_missing", "$.canonical_release.schema_artifacts", "shared-contracts",
                  "Publish nonempty canonical executable schema/package artifacts; no schema is copied into this repository.")
        schema_rows = []
    for index, artifact in enumerate(schema_rows):
        path = f"$.canonical_release.schema_artifacts[{index}]"
        schema_id = artifact.get("schema_id") if isinstance(artifact, dict) else None
        artifact_path, digest, artifact_bytes = check_artifact_bytes(
            base, artifact, path, "shared-contracts", errors, verified_artifacts)
        if not isinstance(schema_id, str) or not schema_id.strip() or schema_id in schemas:
            add_error(errors, "schema_id_missing_or_duplicate", path + ".schema_id", "shared-contracts",
                      "Assign each actual owner-published schema a unique stable schema ID.")
            continue
        if artifact_path is not None and digest is not None and artifact_bytes is not None:
            media_type = artifact.get("media_type") if isinstance(artifact, dict) else None
            is_json = ((isinstance(media_type, str) and "json" in media_type)
                       or artifact_path.suffix.lower() == ".json")
            field_paths = None
            if not is_json:
                add_error(errors, "canonical_schema_format_unsupported", path + ".media_type", "shared-contracts",
                          "Publish runtime schemas in a supported enumerable JSON Schema format so exact owner field inventory can be checked.")
            else:
                try:
                    schema_content = parse_json_bytes(artifact_bytes)
                    if not isinstance(schema_content, dict) or not schema_content:
                        add_error(errors, "canonical_schema_empty", path + ".path", "shared-contracts",
                                  "Publish nonempty actual canonical schema bytes; an empty object/array cannot describe a contract.")
                    else:
                        field_paths = _schema_field_paths(schema_content)
                except SchemaTraversalLimit:
                    add_error(errors, "canonical_schema_traversal_limit", path + ".path", "shared-contracts",
                              "Reduce the owner schema's traversal, reference expansion, field count, and aggregate field-path size to the verifier's fixed limits.")
                except UnsupportedSchema:
                    add_error(errors, "canonical_schema_unsupported", path + ".path", "shared-contracts",
                              "Publish a Draft 2020-12 schema within the documented enumerable subset; unsupported applicators, dynamic fields, references, and legacy keywords fail closed.")
                except (OSError, UnicodeError, json.JSONDecodeError, ValueError, RecursionError):
                    add_error(errors, "canonical_schema_json_invalid", path + ".path", "shared-contracts",
                              "Publish valid bounded JSON Schema bytes whose declared runtime fields can be enumerated without external references or dynamic field names.")
            schemas[schema_id] = {"path": artifact_path, "sha256": digest, "descriptor": artifact,
                                  "field_paths": field_paths}
            schema_hashes[schema_id] = digest

    inventory, inventory_sha, inventory_path = _read_inventory(
        base, release.get("field_inventory"), errors, verified_artifacts)
    validate_crosswalk(manifest, schemas, inventory, evidence, errors)

    compatibility = release.get("compatibility")
    if not isinstance(compatibility, dict):
        compatibility = {}
        add_error(errors, "compatibility_policy_missing", "$.canonical_release.compatibility", "shared-contracts",
                  "Publish owner-approved current/older major and additive-field compatibility rules.")
    additive = compatibility.get("unknown_additive_fields")
    if not isinstance(additive, str) or additive not in {"preserve", "ignore"}:
        add_error(errors, "additive_field_policy_missing", "$.canonical_release.compatibility.unknown_additive_fields", "shared-contracts",
                  "Record the owner-approved behavior for additive unknown fields.")
    adapter = release.get("validator_adapter")
    adapter_path, adapter_sha = check_artifact(
        base, adapter, "$.canonical_release.validator_adapter", "shared-contracts", errors, verified_artifacts)
    dependency_paths: list[tuple[Path | None, str | None]] = []
    if not isinstance(adapter, dict) or adapter.get("protocol") != ADAPTER_PROTOCOL:
        add_error(errors, "validator_adapter_protocol_missing", "$.canonical_release.validator_adapter.protocol", "shared-contracts",
                  f"Publish the independently reviewed offline fixture adapter implementing {ADAPTER_PROTOCOL}.")
    dependency_rows = adapter.get("dependencies", []) if isinstance(adapter, dict) else []
    if not isinstance(dependency_rows, list):
        add_error(errors, "validator_adapter_dependencies_invalid", "$.canonical_release.validator_adapter.dependencies", "shared-contracts",
                  "List each required adapter dependency as an exact relative path and SHA-256; undeclared package files are not mounted.")
    else:
        for index, dependency in enumerate(dependency_rows):
            dependency_path, dependency_sha = check_artifact(
                base, dependency, f"$.canonical_release.validator_adapter.dependencies[{index}]", "shared-contracts", errors,
                verified_artifacts)
            dependency_paths.append((dependency_path, dependency_sha))
    if adapter_path is not None and adapter_sha is not None:
        if not adapter_provenance_verified(adapter, adapter_sha, evidence):
            add_error(errors, "validator_adapter_provenance_unverified", "$.canonical_release.validator_adapter", "shared-contracts",
                      "Bind adapter bytes to the independent shared-contracts owner evidence index before execution.")

    fixtures, fixture_paths = _validate_fixture_map(
        manifest, schemas, inventory, base, errors, verified_artifacts)
    # The owner-evidence digest binds exact publication bytes, not local status
    # text or a set of review references supplied by this descriptor.
    release_subject = canonical_hash({
        "release": {k: v for k, v in release.items() if k not in {"owner_decisions"}},
        "field_crosswalk": manifest.get("field_crosswalk"),
        "fixture_crosswalk": manifest.get("fixture_crosswalk"),
        "fixtures": manifest.get("fixtures"),
    })
    decisions = manifest.get("owner_decisions")
    if not isinstance(decisions, list):
        decisions = []
    decision_by_owner_and_reference: dict[tuple[str, str], dict[str, Any]] = {}
    for index, decision in enumerate(decisions):
        path = f"$.owner_decisions[{index}]"
        if not isinstance(decision, dict):
            add_error(errors, "owner_decision_malformed", path, "Agent and research",
                      "Record the independently verified external owner handoff decision.")
            continue
        owner = decision.get("owner")
        reference = decision.get("decision_ref")
        if not isinstance(owner, str) or not isinstance(reference, str):
            add_error(errors, "owner_decision_duplicate_or_invalid", path, str(owner or "Agent and research"),
                      "Record the exact owner repository and issue reference for each canonical handoff decision.")
            continue
        identity = (owner, reference)
        if identity in decision_by_owner_and_reference:
            add_error(errors, "owner_decision_duplicate_or_invalid", path, owner,
                      "Record one current decision for each exact external owner handoff; distinct issue cases from one owner remain separate.")
            continue
        decision_by_owner_and_reference[identity] = decision
    for requirement in OWNER_ACTIONS:
        owner = requirement["owner"]
        reference = requirement["reference"]
        decision = decision_by_owner_and_reference.get((owner, reference))
        if not decision or decision.get("status") != "accepted" or decision.get("decision_ref") != requirement["reference"]:
            add_error(errors, "external_owner_decision_missing_or_stale", "$.owner_decisions", owner, requirement["action"])
            continue
        validate_release_decision(decision, owner=owner, reference=reference,
                                  subject_sha256=release_subject, evidence=evidence, errors=errors)

    if errors:
        return None
    # Expected outcomes and reference output digests are intentionally withheld
    # from the adapter. It must read actual schema/fixture bytes and produce
    # observed hashes plus base64 serialized bytes, which this tool hashes.
    request_fixtures = []
    for case_id in REQUIRED_CASES:
        fixture = fixtures[case_id]
        schema_id = fixture["schema_id"]
        fixture_path = fixture_paths[case_id]
        request_fixtures.append({
            "case_id": case_id,
            "schema_id": schema_id,
            "schema_path": "/release/" + schemas[schema_id]["path"].relative_to(base).as_posix(),
            "schema_sha256": schema_hashes[schema_id],
            "fixture_path": "/release/" + fixture_path.relative_to(base).as_posix(),
            "fixture_sha256": fixture["sha256"],
            "contract_id": fixture["contract_id"],
            "primitive": fixture["primitive"],
        })
    request = {"protocol": ADAPTER_PROTOCOL, "release_revision": release["revision"],
               "release_version": release["version"], "field_inventory_sha256": inventory_sha,
               "fixtures": request_fixtures}
    request["dependencies"] = [
        {"path": DEPENDENCY_IMPORT_ROOT + "/" + path.relative_to(base).as_posix(), "sha256": digest}
        for path, digest in dependency_paths if path is not None and digest_ok(digest)
    ]
    if adapter_path is None:
        return None
    sandbox_prerequisite = _sandbox_prerequisite_error()
    if sandbox_prerequisite is not None:
        add_error(errors, "execution_sandbox_unavailable", "$.canonical_release.validator_adapter", "shared-contracts",
                  sandbox_prerequisite)
        return None
    snapshot_inputs = [(adapter_path, adapter_sha, "validator adapter"),
                       (inventory_path, inventory_sha, "canonical field inventory")]
    snapshot_inputs.extend((schema["path"], schema["sha256"], "runtime schema")
                           for schema in schemas.values())
    seen_fixture_paths: set[Path] = set()
    for case_id, fixture_path in fixture_paths.items():
        if fixture_path not in seen_fixture_paths:
            snapshot_inputs.append((fixture_path, fixtures[case_id].get("sha256"), "owner fixture"))
            seen_fixture_paths.add(fixture_path)
    dependency_relatives = {
        path.relative_to(base).as_posix() for path, digest in dependency_paths if path is not None and digest_ok(digest)
    }
    release_relatives = {
        path.relative_to(base).as_posix()
        for path, digest, _ in snapshot_inputs
        if path is not None
    }
    if dependency_relatives & release_relatives:
        add_error(errors, "execution_snapshot_path_collision", "$.canonical_release.validator_adapter.dependencies",
                  "shared-contracts",
                  "Give each adapter dependency a unique package path separate from the adapter, inventory, schemas, and fixtures.")
        return None
    snapshot_inputs.extend((path, digest, "declared adapter dependency")
                           for path, digest in dependency_paths)
    artifact_bytes = _read_execution_artifacts(base, snapshot_inputs, errors, verified_artifacts)
    if artifact_bytes is None:
        return None
    snapshot, snapshot_error = _make_sealed_snapshot(artifact_bytes)
    if snapshot is None:
        add_error(errors, "execution_snapshot_unavailable", "$.canonical_release.validator_adapter", "shared-contracts",
                  snapshot_error or "Provision Linux immutable sealed snapshots before executing owner bytes.")
        return None
    command, sandbox_error = _sandbox_command(
        base, adapter_path, json.dumps(request).encode("utf-8"), snapshot, tuple(sorted(dependency_relatives)))
    if command is None:
        snapshot.close()
        add_error(errors, "execution_sandbox_unavailable", "$.canonical_release.validator_adapter", "shared-contracts",
                  sandbox_error or "Provision a reviewed supported offline OS sandbox; never execute an owner adapter unsandboxed.")
        return None
    sandbox_error = probe_sandbox(base)
    if sandbox_error is not None:
        snapshot.close()
        add_error(errors, "execution_sandbox_unverified", "$.canonical_release.validator_adapter", "shared-contracts", sandbox_error)
        return None
    env = {"PATH": "/usr/bin:/bin", "PYTHONIOENCODING": "utf-8", "PYTHONHASHSEED": "0",
           "PYTHONDONTWRITEBYTECODE": "1", "PYTHONNOUSERSITE": "1", "HOME": "/nonexistent", "LC_ALL": "C.UTF-8"}
    try:
        run = run_bounded(command, cwd=Path("/"), env=env,
                          input_bytes=json.dumps(request, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
                          timeout=ADAPTER_TIMEOUT_SECONDS, stdout_limit=MAX_ADAPTER_STDOUT, stderr_limit=MAX_ADAPTER_STDERR,
                          pass_fds=snapshot.pass_fds)
    finally:
        snapshot.close()
    if not run["ok"]:
        add_error(errors, run.get("code", "canonical_validator_execution_failed"), "$.canonical_release.validator_adapter", "shared-contracts",
                  "Repair the trusted adapter or provision the supported isolated runtime; bounded private diagnostics are not echoed.")
        return None
    observed = _validate_adapter_result(run["stdout"], request, fixtures, schema_hashes, errors)
    if observed is None or errors:
        return None
    return {"canonical_revision": release["revision"], "canonical_version": release["version"],
            "schema_sha256": dict(sorted(schema_hashes.items())),
            "fixture_sha256": {case_id: fixtures[case_id]["sha256"] for case_id in REQUIRED_CASES},
            "serialized_output_sha256": observed["serialized_output_sha256"],
            "validated_fixture_cases": len(REQUIRED_CASES),
            "execution_binding": "sealed memfd snapshot exposed through read-only bubblewrap binds",
            "scope": "owner-evidence-verified offline fixture conformance only; no producer/consumer runtime, live provider, or global acceptance claim"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--offline", action="store_true", help="Require the verified offline owner fixture gate; live/provider execution is forbidden.")
    parser.add_argument("--owner-evidence-index", type=Path, default=os.environ.get("ACV1_OWNER_EVIDENCE_INDEX"))
    parser.add_argument("--owner-evidence-sha256", default=os.environ.get("ACV1_OWNER_EVIDENCE_SHA256"))
    parser.add_argument("--test-only-synthetic", action="store_true",
                         help="Run a fully synthetic sealed-sandbox fixture solely to exercise tooling; output is always not ready.")
    args = parser.parse_args(argv)
    errors: list[dict[str, str]] = []
    output = None
    try:
        manifest = read_json(args.manifest)
        workspace_root = Path(__file__).resolve().parents[1]
        evidence = load_trusted_evidence_index(
            str(args.owner_evidence_index) if args.owner_evidence_index else None,
            args.owner_evidence_sha256, workspace_root, errors,
            allow_test_only=args.test_only_synthetic)
        if args.test_only_synthetic and (not isinstance(manifest, dict) or manifest.get("test_only") is not True):
            add_error(errors, "synthetic_manifest_marker_missing", "$.test_only", "Agent and research",
                      "The synthetic execution mode requires an explicitly TEST ONLY manifest and never represents owner approval or release readiness.")
        if not args.test_only_synthetic and isinstance(manifest, dict) and manifest.get("test_only") is True:
            add_error(errors, "test_only_manifest_not_release_evidence", "$.test_only", "Agent and research",
                      "Synthetic test manifests cannot enter the owner release readiness path.")
        if not args.offline:
            add_error(errors, "offline_mode_required", "$", "Agent and research",
                      "Run only the verified offline fixture gate with --offline; live/provider execution is out of scope.")
        if args.offline:
            result = validate_and_run(manifest, args.manifest, errors, evidence)
            if result is not None:
                output = result
    except InputProblem as problem:
        errors.append(problem.item)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError, TypeError, KeyError, OverflowError, RecursionError):
        errors.append({"code": "input_invalid", "path": "$", "owner": "Agent and research",
                       "action": "Supply bounded, typed owner artifacts; malformed data is not echoed and cannot pass readiness."})
    except Exception:
        errors.append({"code": "verifier_internal_error", "path": "$", "owner": "Agent and research",
                       "action": "Review the standalone verifier diagnostics; private input and traceback details are suppressed."})
    if errors or output is None:
        if not errors:
            errors.append({"code": "canonical_release_not_verified", "path": "$.canonical_release", "owner": "shared-contracts",
                           "action": "Publish the canonical release, owner fixtures, independently pinned owner evidence, and supported sandbox runtime before readiness."})
        report = {"tool": "verify_contract_release", "status": "blocked", "ready": False,
                  "dependency_mode": "offline", "errors": sorted(errors, key=lambda item: (item["path"], item["code"], item["owner"]))}
        print(json.dumps(report, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
        return 2
    if args.test_only_synthetic:
        output["ready"] = False
        output["owner_evidence_verified"] = False
        output["scope"] = "TEST ONLY synthetic sealed-sandbox exercise; no canonical owner approval, release readiness, producer/consumer conformance, or global acceptance is established"
        report = {"tool": "verify_contract_release", "status": "synthetic_test_only_validation_passed",
                  "ready": False, "owner_evidence_verified": False, "dependency_mode": "offline",
                  "scope": "TEST ONLY synthetic fixtures and local synthetic evidence; real owner approval and release readiness remain unverified",
                  "result": output}
    else:
        report = {"tool": "verify_contract_release", "status": "offline_conformance_passed", "ready": True,
                  "dependency_mode": "offline", "result": output}
    print(json.dumps(report, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
