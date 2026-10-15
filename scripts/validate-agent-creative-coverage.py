#!/usr/bin/env python3
"""Validate caller-supplied ACV1 planning and release evidence snapshots offline."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import stat
import sys
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


LIMITS = {
    "coverage-map": 8 * 1024 * 1024,
    "canonical-checklist": 1 * 1024 * 1024,
    "issue-readbacks": 16 * 1024 * 1024,
    "source-pins": 2 * 1024 * 1024,
    "release-manifest": 2 * 1024 * 1024,
    "evidence-file": 256 * 1024 * 1024,
    "evidence-total": 1024 * 1024 * 1024,
}
HASH_RE = re.compile(r"^[0-9a-f]{64}$")
KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
JSON_POINTER_RE = re.compile(r"^/(?:[A-Za-z0-9_.:-]|~[01])*(?:/(?:[A-Za-z0-9_.:-]|~[01])*)*$")
REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
ISSUE_PATH_RE = re.compile(r"^/([^/]+)/([^/]+)/issues/([1-9][0-9]*)/?$")
MODES = {"mock", "recorded", "sandbox", "shadow", "live"}
STAGES = {"fixture", "contract", "runnable", "final_acceptance"}
PROJECTION_STAGES = STAGES | {
    "accepted_policy_fixture", "canonical_agreement_and_fixture",
    "live_evidence", "readiness_config",
}
PROJECTION_STAGE_ORDER = {
    "accepted_policy_fixture": 0,
    "fixture": 0,
    "canonical_agreement_and_fixture": 1,
    "contract": 1,
    "readiness_config": 2,
    "runnable": 3,
    "live_evidence": 4,
    "final_acceptance": 5,
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
STAGED_EDGE_STATES = {"pending", "satisfied"}
EVIDENCE_KINDS = {"test", "runtime", "configuration", "artifact", "receipt"}
LEAF_KINDS = {"implementation_leaf", "delivery_leaf"}
NON_LEAF_KINDS = {"epic", "gate", "release_gate", "program_gate"}
COORDINATION_STATES = {"unreported", "pending", "in_progress", "published", "reconciled", "complete"}
PUBLICATION_STATES = {"unreported", "pending", "published", "reconciled", "complete"}
JSON_READ_SIZE = 64 * 1024
MAX_ENTRIES = 10_000
MAX_CONTRIBUTION_ROWS = 100_000
MAX_OWNERSHIP_ROWS = 100_000
MAX_GRAPH_ROWS = 100_000
MAX_RELEASE_RECORDS = 10_000
MAX_BLOCKER_TEXT = 512
MAX_ISSUE_NUMBER_DIGITS = 20


class InputError(Exception):
    """A fixed diagnostic safe to include in the deterministic report."""

    def __init__(self, code: str, record: str = "input") -> None:
        self.code = code
        self.record = record if _safe_key(record) else "input"
        super().__init__(code)


def _safe_key(value: Any) -> bool:
    return isinstance(value, str) and KEY_RE.fullmatch(value) is not None


def _label(value: Any) -> str:
    return value if _safe_key(value) else "<invalid>"


def _json_pointer_label(value: Any) -> str:
    """Preserve bounded JSON Pointer provenance without widening identifier labels."""
    if isinstance(value, str) and len(value) <= 4096 and JSON_POINTER_RE.fullmatch(value):
        return value
    return "<invalid>"


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise InputError("DUPLICATE_JSON_KEY", key)
        result[key] = value
    return result


def _reject_constant(_: str) -> None:
    raise InputError("INVALID_JSON_NUMBER")


def _finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise InputError("INVALID_JSON_NUMBER")
    return parsed


def _read_bytes(path: str | Path, limit: int, role: str) -> bytes:
    """Read a single explicitly named regular file without following symlinks."""
    try:
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        if hasattr(os, "O_NONBLOCK"):
            flags |= os.O_NONBLOCK
        fd = os.open(os.fspath(path), flags)
    except OSError as exc:
        raise InputError("INPUT_UNREADABLE", role) from exc
    data: bytes | None = None
    failure: InputError | None = None
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
            raise InputError("INPUT_TYPE_OR_SIZE", role)
        chunks: list[bytes] = []
        count = 0
        while True:
            chunk = os.read(fd, min(JSON_READ_SIZE, limit + 1 - count))
            if not chunk:
                break
            chunks.append(chunk)
            count += len(chunk)
            if count > limit:
                raise InputError("INPUT_TYPE_OR_SIZE", role)
        data = b"".join(chunks)
        if len(data) != info.st_size:
            raise InputError("INPUT_CHANGED_DURING_READ", role)
    except InputError as exc:
        failure = exc
    except OSError as exc:
        failure = InputError("INPUT_UNREADABLE", role)
        failure.__cause__ = exc
    try:
        os.close(fd)
    except OSError as exc:
        if failure is None:
            failure = InputError("INPUT_CLOSE_FAILED", role)
            failure.__cause__ = exc
    if failure is not None:
        raise failure
    assert data is not None
    return data


def _read_json(path: str | Path, role: str) -> tuple[dict[str, Any], bytes]:
    raw = _read_bytes(path, LIMITS[role], role)
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
            parse_float=_finite_float,
        )
    except InputError:
        raise
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise InputError("INVALID_JSON", role) from exc
    if not isinstance(value, dict):
        raise InputError("JSON_ROOT_NOT_OBJECT", role)
    return value, raw


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _git_blob_sha1(raw: bytes) -> str:
    header = b"blob " + str(len(raw)).encode("ascii") + b"\0"
    return hashlib.sha1(header + raw).hexdigest()


def _as_dict(value: Any, code: str, record: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise InputError(code, record)
    return value


def _as_list(value: Any, code: str, record: str) -> list[Any]:
    if not isinstance(value, list):
        raise InputError(code, record)
    return value


def _text(value: Any, code: str, record: str, *, nonempty: bool = True) -> str:
    if not isinstance(value, str) or (nonempty and not value.strip()):
        raise InputError(code, record)
    return value


def _bool(value: Any, code: str, record: str) -> bool:
    if not isinstance(value, bool):
        raise InputError(code, record)
    return value


def _integer(value: Any, code: str, record: str, maximum: int | None = None) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise InputError(code, record)
    if maximum is not None and value > maximum:
        raise InputError(code, record)
    return value


def _iso_utc(value: Any, code: str, record: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise InputError(code, record)
    try:
        result = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise InputError(code, record) from exc
    if result.tzinfo != timezone.utc:
        raise InputError(code, record)
    return result


def _relative_artifact(value: Any, record: str) -> Path:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise InputError("EVIDENCE_PATH_INVALID", record)
    return Path(value)


def _add_issue(issues: list[dict[str, str]], code: str, record: Any) -> None:
    item = {"code": code, "record": _label(record)}
    if item not in issues and len(issues) < 250:
        issues.append(item)


def _add_relation_issue(
    issues: list[dict[str, str]], code: str, record: Any, related_record: Any
) -> None:
    item = {
        "code": code,
        "record": _label(record),
        "related_record": _label(related_record),
    }
    if item not in issues and len(issues) < 250:
        issues.append(item)


def _bounded_rows(
    rows: list[Any], maximum: int, code: str, record: str, issues: list[dict[str, str]]
) -> list[Any]:
    if len(rows) > maximum:
        _add_issue(issues, code, record)
        return rows[:maximum]
    return rows


def _snapshot_state(
    value: Any, allowed: set[str], field: str, issues: list[dict[str, str]]
) -> str:
    if value is None:
        return "unreported"
    if isinstance(value, str) and value in allowed:
        return value
    _add_issue(issues, field.upper() + "_INVALID", field)
    return "unreported"


def _normalized_snapshot_state(value: Any, allowed: set[str]) -> str:
    return value if isinstance(value, str) and value in allowed else "unreported"


def _normalized_utc(value: Any) -> str | None:
    if not isinstance(value, str) or len(value) > 32:
        return None
    try:
        _iso_utc(value, "TIMESTAMP_INVALID", "input")
    except InputError:
        return None
    return value


def _reported_blockers(coverage: dict[str, Any]) -> tuple[list[dict[str, str]], bool]:
    result: list[dict[str, str]] = []
    entries = coverage.get("entries")
    if not isinstance(entries, list):
        return result, False
    for entry in entries:
        if not isinstance(entry, dict) or not _safe_key(entry.get("key")):
            continue
        blockers = entry.get("blockers", [])
        if not isinstance(blockers, list):
            continue
        for blocker in blockers:
            if not isinstance(blocker, dict):
                continue
            owner, action = blocker.get("owner"), blocker.get("action")
            if not isinstance(owner, str) or not isinstance(action, str):
                continue
            if len(result) == 250:
                return result, True
            result.append({
                "record": entry["key"],
                "owner": owner[:MAX_BLOCKER_TEXT],
                "action": action[:MAX_BLOCKER_TEXT],
            })
    return result, False


def _verify_source_pins(
    pins: dict[str, Any],
    map_raw: bytes,
    canonical_raw: bytes,
    readbacks_raw: bytes,
    issues: list[dict[str, str]],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
    if pins.get("format") != "acv1.coverage-source-reference/v1":
        raise InputError("UNSUPPORTED_SOURCE_PINS_FORMAT", "source-pins")
    map_pin = _as_dict(pins.get("coverage_map"), "COVERAGE_MAP_PIN_REQUIRED", "coverage-map")
    if map_pin.get("adapter") != "agent-harness.coverage-map-current/v1":
        _add_issue(issues, "COVERAGE_MAP_ADAPTER_UNSUPPORTED", "coverage-map")
    if not isinstance(map_pin.get("source_path"), str) or not map_pin.get("source_path"):
        _add_issue(issues, "COVERAGE_MAP_SOURCE_PATH_MISSING", "coverage-map")
    expected_map_bytes = _integer(map_pin.get("bytes"), "COVERAGE_MAP_PIN_INVALID", "coverage-map", LIMITS["coverage-map"])
    expected_map_hash = map_pin.get("sha256")
    if not isinstance(expected_map_hash, str) or HASH_RE.fullmatch(expected_map_hash) is None:
        _add_issue(issues, "COVERAGE_MAP_PIN_INVALID", "coverage-map")
    else:
        if len(map_raw) != expected_map_bytes or _sha256(map_raw) != expected_map_hash:
            _add_issue(issues, "COVERAGE_MAP_PIN_MISMATCH", "coverage-map")

    upstream = _as_list(pins.get("upstream_git_objects"), "UPSTREAM_PINS_REQUIRED", "canonical-checklist")
    known_roles = {"global-canonical-checklist", "supporting-contract-catalog-metadata"}
    upstream_by_role: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(upstream):
        if not isinstance(item, dict) or not isinstance(item.get("role"), str):
            _add_issue(issues, "UPSTREAM_PIN_INVALID", f"upstream-{index}")
            continue
        role = item["role"]
        if role not in known_roles:
            _add_issue(issues, "UPSTREAM_ROLE_UNSUPPORTED", role)
            continue
        if role in upstream_by_role:
            _add_issue(issues, "UPSTREAM_ROLE_DUPLICATE", role)
            continue
        upstream_by_role[role] = item
    canonical_pin = upstream_by_role.get("global-canonical-checklist")
    if canonical_pin is None:
        _add_issue(issues, "CANONICAL_PIN_MISSING", "canonical-checklist")
        canonical_pin = {}
    if canonical_pin.get("adapter") != "integration-evals.acceptance-checklist/v1":
        _add_issue(issues, "CANONICAL_ADAPTER_UNSUPPORTED", "canonical-checklist")
    repository = canonical_pin.get("repository")
    if not isinstance(repository, str) or repository.count("/") != 1 or any(not REPOSITORY_RE.fullmatch(part) for part in repository.split("/")):
        _add_issue(issues, "CANONICAL_SOURCE_IDENTITY_INVALID", "canonical-checklist")
    for field in ("issue_pinned_revision", "captured_main_revision"):
        revision = canonical_pin.get(field)
        if not isinstance(revision, str) or re.fullmatch(r"[0-9a-f]{40}", revision) is None:
            _add_issue(issues, "CANONICAL_SOURCE_IDENTITY_INVALID", "canonical-checklist")
    if not isinstance(canonical_pin.get("source_path"), str) or not canonical_pin.get("source_path"):
        _add_issue(issues, "CANONICAL_SOURCE_IDENTITY_INVALID", "canonical-checklist")
    expected_blob = canonical_pin.get("git_blob_sha1")
    expected_bytes = canonical_pin.get("bytes")
    if (
        not isinstance(expected_blob, str)
        or re.fullmatch(r"[0-9a-f]{40}", expected_blob) is None
        or not isinstance(expected_bytes, int)
        or isinstance(expected_bytes, bool)
        or expected_bytes < 0
    ):
        _add_issue(issues, "CANONICAL_PIN_INVALID", "canonical-checklist")
    elif len(canonical_raw) != expected_bytes or _git_blob_sha1(canonical_raw) != expected_blob:
        _add_issue(issues, "CANONICAL_PIN_MISMATCH", "canonical-checklist")

    readback_value = pins.get("issue_readbacks")
    readback_pin = readback_value if isinstance(readback_value, dict) else None
    if readback_pin is None:
        _add_issue(issues, "ISSUE_READBACK_PIN_REQUIRED", "issue-readbacks")
        return map_pin, canonical_pin, None
    if readback_pin.get("adapter") != "agent-harness.issue-readbacks/v1":
        _add_issue(issues, "ISSUE_READBACK_ADAPTER_UNSUPPORTED", "issue-readbacks")
    readback_bytes = readback_pin.get("bytes")
    readback_hash = readback_pin.get("sha256")
    if (
        not isinstance(readback_bytes, int)
        or isinstance(readback_bytes, bool)
        or readback_bytes < 0
        or not isinstance(readback_hash, str)
        or HASH_RE.fullmatch(readback_hash) is None
    ):
        _add_issue(issues, "ISSUE_READBACK_PIN_INVALID", "issue-readbacks")
    elif len(readbacks_raw) != readback_bytes or _sha256(readbacks_raw) != readback_hash:
        _add_issue(issues, "ISSUE_READBACK_PIN_MISMATCH", "issue-readbacks")
    if readback_pin.get("pages_complete") is not True:
        _add_issue(issues, "ISSUE_READBACK_PIN_INCOMPLETE", "issue-readbacks")
    if not _safe_key(readback_pin.get("capture_id")):
        _add_issue(issues, "ISSUE_READBACK_PIN_INVALID", "issue-readbacks")
    try:
        _iso_utc(readback_pin.get("captured_at"), "ISSUE_READBACK_PIN_INVALID", "issue-readbacks")
    except InputError as exc:
        _add_issue(issues, exc.code, exc.record)
    revision = readback_pin.get("observed_revision")
    if revision is not None and (not isinstance(revision, str) or re.fullmatch(r"[0-9a-f]{40}", revision) is None):
        _add_issue(issues, "ISSUE_READBACK_PIN_INVALID", "issue-readbacks")
    return map_pin, canonical_pin, readback_pin


def _verify_source_pins_v2(
    pins: dict[str, Any],
    map_raw: bytes,
    canonical_raw: bytes,
    readbacks_raw: bytes,
    issues: list[dict[str, str]],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
    """Verify v2 pins while delegating common canonical/readback checks to v1."""
    if pins.get("format") != "acv1.coverage-source-reference/v2":
        raise InputError("UNSUPPORTED_SOURCE_PINS_FORMAT", "source-pins")
    map_pin = _as_dict(pins.get("coverage_map"), "COVERAGE_MAP_PIN_REQUIRED", "coverage-map")
    readback_pin = _as_dict(pins.get("issue_readbacks"), "ISSUE_READBACK_PIN_REQUIRED", "issue-readbacks")
    if pins.get("snapshot_id") != readback_pin.get("capture_id"):
        _add_issue(issues, "PROJECTION_SNAPSHOT_ID_MISMATCH", "source-pins")
    if map_pin.get("adapter") != "agent-harness.coverage-projection/v2":
        _add_issue(issues, "COVERAGE_MAP_ADAPTER_UNSUPPORTED", "coverage-map")
    if readback_pin.get("adapter") != "agent-harness.issue-readbacks/v2":
        _add_issue(issues, "ISSUE_READBACK_ADAPTER_UNSUPPORTED", "issue-readbacks")
    compatibility = dict(pins)
    compatibility["format"] = "acv1.coverage-source-reference/v1"
    compatibility["coverage_map"] = dict(map_pin, adapter="agent-harness.coverage-map-current/v1")
    compatibility["issue_readbacks"] = dict(readback_pin, adapter="agent-harness.issue-readbacks/v1")
    verified_map, canonical_pin, verified_readbacks = _verify_source_pins(
        compatibility, map_raw, canonical_raw, readbacks_raw, issues
    )
    return map_pin, canonical_pin, readback_pin if verified_readbacks is not None else None


def _validate_map_v2(
    coverage: dict[str, Any],
    pins: dict[str, Any],
    canonical: dict[str, Any],
    readbacks: dict[str, Any],
    map_pin: dict[str, Any],
    readback_pin: dict[str, Any] | None,
    *,
    check_urls: bool,
    check_dag: bool,
    require_specialists: bool,
) -> tuple[list[dict[str, str]], dict[str, int], dict[str, dict[str, Any]], dict[str, Any]]:
    """Validate a directional, source-preserving projection without changing v1 rules."""
    issues: list[dict[str, str]] = []
    if coverage.get("format") != "agent-harness.coverage-projection/v2":
        _add_issue(issues, "COVERAGE_MAP_FORMAT_UNSUPPORTED", "coverage-map")
    if not check_urls:
        _add_issue(issues, "REQUIRED_CHECK_NOT_RUN", "issue-readbacks")
    if not check_dag:
        _add_issue(issues, "REQUIRED_CHECK_NOT_RUN", "dag")
    _snapshot_state(coverage.get("coordination_status"), COORDINATION_STATES, "coordination_status", issues)
    _snapshot_state(coverage.get("publication_status"), PUBLICATION_STATES, "publication_status", issues)
    if coverage.get("complete") is not True or map_pin.get("complete") is not True:
        _add_issue(issues, "COVERAGE_SNAPSHOT_INCOMPLETE", "coverage-map")
    if pins.get("projection_inputs") != coverage.get("projection_inputs"):
        _add_issue(issues, "PROJECTION_INPUT_PINS_MISMATCH", "coverage-map")
    projection_inputs = coverage.get("projection_inputs")
    expected_input_roles = {
        "coverage_map", "pair_review", "stage_review", "external_artifacts",
        "issue_readbacks", "canonical_checklist", "canonical_reference",
    }
    if not isinstance(projection_inputs, list) or len(projection_inputs) != len(expected_input_roles):
        _add_issue(issues, "PROJECTION_INPUT_FAMILY_INVALID", "projection_inputs")
        projection_inputs = []
    seen_input_roles: set[str] = set()
    for index, item in enumerate(projection_inputs):
        if not isinstance(item, dict) or not isinstance(item.get("role"), str):
            _add_issue(issues, "PROJECTION_INPUT_INVALID", f"projection-input-{index}")
            continue
        role = item["role"]
        if role not in expected_input_roles or role in seen_input_roles:
            _add_issue(issues, "PROJECTION_INPUT_ROLE_INVALID", _label(role))
        seen_input_roles.add(role)
        if not isinstance(item.get("source_path"), str) or not item.get("source_path") or len(item.get("source_path", "")) > 4096:
            _add_issue(issues, "PROJECTION_INPUT_PATH_INVALID", _label(role))
        size = item.get("bytes")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0 or size > LIMITS["coverage-map"]:
            _add_issue(issues, "PROJECTION_INPUT_SIZE_INVALID", _label(role))
        if not isinstance(item.get("sha256"), str) or HASH_RE.fullmatch(item.get("sha256", "")) is None:
            _add_issue(issues, "PROJECTION_INPUT_DIGEST_INVALID", _label(role))
    if seen_input_roles != expected_input_roles:
        _add_issue(issues, "PROJECTION_INPUT_ROLE_SET_MISMATCH", "projection_inputs")

    criteria = _canonical_criteria(canonical, issues)
    entries_raw = coverage.get("entries")
    ownership_raw = coverage.get("ownership_rows")
    source_rows_raw = coverage.get("source_contribution_rows")
    pair_review_raw = coverage.get("pair_review_rows")
    source_stage_raw = coverage.get("source_stage_rows")
    additions_raw = coverage.get("source_supported_associations")
    extras_raw = coverage.get("reviewed_typed_extras")
    unresolved_raw = coverage.get("unresolved_associations")
    if not isinstance(entries_raw, list):
        _add_issue(issues, "ENTRY_FAMILY_MISSING", "entries")
        entries_raw = []
    if not isinstance(ownership_raw, list):
        _add_issue(issues, "OWNERSHIP_FAMILY_MISSING", "ownership_rows")
        ownership_raw = []
    if not isinstance(source_rows_raw, list):
        _add_issue(issues, "CONTRIBUTION_FAMILY_MISSING", "source_contribution_rows")
        source_rows_raw = []
    if not isinstance(pair_review_raw, list):
        _add_issue(issues, "PAIR_REVIEW_FAMILY_MISSING", "pair_review_rows")
        pair_review_raw = []
    if not isinstance(source_stage_raw, list):
        _add_issue(issues, "SOURCE_STAGE_FAMILY_MISSING", "source_stage_rows")
        source_stage_raw = []
    for family, rows in (
        ("source_supported_associations", additions_raw),
        ("reviewed_typed_extras", extras_raw),
        ("unresolved_associations", unresolved_raw),
    ):
        if not isinstance(rows, list):
            _add_issue(issues, "PROJECTION_RELATION_FAMILY_MISSING", family)
        elif len(rows) > MAX_OWNERSHIP_ROWS:
            _add_issue(issues, "PROJECTION_RELATION_ROW_LIMIT_EXCEEDED", family)
    for family, rows, maximum, code in (
        ("entries", entries_raw, MAX_ENTRIES, "ENTRY_ROW_LIMIT_EXCEEDED"),
        ("ownership_rows", ownership_raw, MAX_OWNERSHIP_ROWS, "OWNERSHIP_ROW_LIMIT_EXCEEDED"),
        ("source_contribution_rows", source_rows_raw, MAX_CONTRIBUTION_ROWS, "CONTRIBUTION_ROW_LIMIT_EXCEEDED"),
    ):
        if len(rows) > maximum:
            _add_issue(issues, code, family)
    entries_raw = entries_raw[:MAX_ENTRIES]
    ownership_raw = ownership_raw[:MAX_OWNERSHIP_ROWS]
    source_rows_raw = source_rows_raw[:MAX_CONTRIBUTION_ROWS]
    pair_review_raw = pair_review_raw[:MAX_OWNERSHIP_ROWS]
    source_stage_raw = source_stage_raw[:MAX_GRAPH_ROWS]
    additions_raw = additions_raw[:MAX_OWNERSHIP_ROWS] if isinstance(additions_raw, list) else []
    extras_raw = extras_raw[:MAX_OWNERSHIP_ROWS] if isinstance(extras_raw, list) else []
    unresolved_raw = unresolved_raw[:MAX_OWNERSHIP_ROWS] if isinstance(unresolved_raw, list) else []

    entry_by_key: dict[str, dict[str, Any]] = {}
    leaves: dict[str, dict[str, Any]] = {}
    repo_counts: dict[str, int] = {}
    for index, entry in enumerate(entries_raw):
        if not isinstance(entry, dict) or not _safe_key(entry.get("key")):
            _add_issue(issues, "ENTRY_INVALID", f"entry-{index}")
            continue
        key = entry["key"]
        if key in entry_by_key:
            _add_issue(issues, "ENTRY_KEY_DUPLICATE", key)
            continue
        entry_by_key[key] = entry
        record_type = entry.get("record_type")
        if record_type != "delivery_leaf":
            _add_issue(issues, "ENTRY_RECORD_TYPE_UNSUPPORTED", key)
        required = entry.get("required_for_v1")
        if not isinstance(required, bool):
            _add_issue(issues, "REQUIRED_LEAF_FLAG_MISSING", key)
        elif required:
            leaves[key] = entry
            repo = entry.get("repository")
            if not isinstance(repo, str) or REPOSITORY_RE.fullmatch(repo) is None:
                _add_issue(issues, "ENTRY_REPOSITORY_INVALID", key)
            else:
                repo_counts[repo] = repo_counts.get(repo, 0) + 1
            if not isinstance(entry.get("owner_role"), str) or not entry.get("owner_role", "").strip():
                _add_issue(issues, "ENTRY_OWNER_ROLE_MISSING", key)
            elif len(entry["owner_role"]) > MAX_BLOCKER_TEXT:
                _add_issue(issues, "ENTRY_OWNER_ROLE_TOO_LONG", key)
            if not isinstance(entry.get("priority"), str) or not entry.get("priority", "").strip() or len(entry.get("priority", "")) > 32:
                _add_issue(issues, "ENTRY_PRIORITY_INVALID", key)
            reqs = entry.get("requirement_ids")
            if not isinstance(reqs, list) or any(not _safe_key(value) for value in reqs):
                _add_issue(issues, "ENTRY_REQUIREMENT_IDS_INVALID", key)
            elif len(reqs) != len(set(reqs)):
                _add_issue(issues, "ENTRY_REQUIREMENT_ID_DUPLICATE", key)
            if entry.get("issue_url") is not None:
                try:
                    _entry_identity(entry, key, issues)
                except InputError as exc:
                    _add_issue(issues, exc.code, exc.record)
                for field in ("historical_body_sha256",):
                    value = entry.get(field)
                    if not isinstance(value, str) or HASH_RE.fullmatch(value) is None:
                        _add_issue(issues, "ENTRY_BODY_DIGEST_INVALID", key)
                current = entry.get("current_readback")
                if not isinstance(current, dict) or not isinstance(current.get("body_sha256"), str) or HASH_RE.fullmatch(current.get("body_sha256", "")) is None:
                    _add_issue(issues, "CURRENT_READBACK_INVALID", key)
                else:
                    body_changed = current.get("body_sha256") != entry.get("historical_body_sha256")
                    title_changed = isinstance(entry.get("historical_issue_title"), str) and current.get("title") != entry.get("historical_issue_title")
                    state_changed = isinstance(entry.get("historical_issue_state"), str) and current.get("state") != entry.get("historical_issue_state")
                    if (body_changed or title_changed or state_changed) and not _has_source_semantic_review(entry, current):
                        if body_changed:
                            _add_issue(issues, "ISSUE_BODY_CHANGE_UNADJUDICATED", key)
                        if title_changed:
                            _add_issue(issues, "ISSUE_TITLE_CHANGE_UNADJUDICATED", key)
                        if state_changed:
                            _add_issue(issues, "ISSUE_STATE_CHANGE_UNADJUDICATED", key)
            required_test_status = entry.get("required_test_declaration_status")
            if required_test_status == "not_declared_in_source":
                tests = entry.get("required_tests")
                if tests != []:
                    _add_issue(issues, "ENTRY_REQUIRED_TESTS_INVALID", key)
                _add_issue(issues, "ENTRY_REQUIRED_TESTS_UNDECLARED", key)
            elif required_test_status == "declared_in_source":
                if "required_tests" not in entry:
                    _add_issue(issues, "ENTRY_REQUIRED_TESTS_INVALID", key)
                    tests = []
                else:
                    tests = entry.get("required_tests")
            else:
                _add_issue(issues, "ENTRY_REQUIRED_TEST_DECLARATION_STATUS_INVALID", key)
                tests = entry.get("required_tests", [])
            if not isinstance(tests, list) or len(tests) > MAX_ENTRIES:
                _add_issue(issues, "ENTRY_REQUIRED_TESTS_INVALID", key)
            elif any(not isinstance(value, (str, dict)) for value in tests):
                _add_issue(issues, "ENTRY_REQUIRED_TESTS_INVALID", key)
            else:
                test_ids = _required_test_ids(entry, fallback_to_legacy_tests=False)
                if len(test_ids) != len(tests) or any(not _safe_key(value) for value in test_ids):
                    _add_issue(issues, "ENTRY_REQUIRED_TESTS_INVALID", key)
                if len(test_ids) != len(set(test_ids)):
                    _add_issue(issues, "ENTRY_REQUIRED_TESTS_DUPLICATE", key)
            evidence = entry.get("required_evidence", [])
            if not isinstance(evidence, list) or any(not _safe_key(value) for value in evidence):
                _add_issue(issues, "ENTRY_REQUIRED_EVIDENCE_INVALID", key)
            elif len(evidence) != len(set(evidence)):
                _add_issue(issues, "ENTRY_REQUIRED_EVIDENCE_DUPLICATE", key)
            receipts = entry.get("required_owner_receipts", [])
            if not isinstance(receipts, list) or any(not _safe_key(value) for value in receipts):
                _add_issue(issues, "ENTRY_REQUIRED_RECEIPTS_INVALID", key)
            elif len(receipts) != len(set(receipts)):
                _add_issue(issues, "ENTRY_REQUIRED_RECEIPTS_DUPLICATE", key)
            for field in ("required_model_revision", "required_policy_revision"):
                if field in entry and not _safe_key(entry.get(field)):
                    _add_issue(issues, "ENTRY_REQUIRED_CONFIGURATION_PIN_INVALID", key)
            for field in ("required_evidence", "required_release_stage", "required_mode"):
                if field not in entry:
                    continue
                if field == "required_evidence" and (not isinstance(entry[field], list) or any(not _safe_key(x) for x in entry[field])):
                    _add_issue(issues, "ENTRY_REQUIRED_EVIDENCE_INVALID", key)
                elif field == "required_release_stage" and (not isinstance(entry[field], str) or entry[field] not in PROJECTION_STAGES):
                    _add_issue(issues, "ENTRY_REQUIRED_RELEASE_STAGE_INVALID", key)
                elif field == "required_mode" and (not isinstance(entry[field], str) or entry[field] not in MODES):
                    _add_issue(issues, "ENTRY_REQUIRED_MODE_INVALID", key)
            state = entry.get("state")
            if not isinstance(state, str) or state not in {"planned", "in_progress", "blocked", "closed", "complete", "verified", "deferred", "not_planned", "not-planned"}:
                _add_issue(issues, "ENTRY_STATE_INVALID", key)
            elif state in {"deferred", "not_planned", "not-planned"}:
                _add_issue(issues, "REQUIRED_LEAF_NOT_DELIVERED", key)
            elif state == "closed" and not _has_exact_closure_evidence(
                entry.get("closure_evidence"), _required_test_ids(entry, fallback_to_legacy_tests=False)
            ):
                _add_issue(issues, "CLOSED_LEAF_WITHOUT_EXACT_EVIDENCE", key)
            blockers = entry.get("blockers", [])
            if not isinstance(blockers, list) or len(blockers) > MAX_GRAPH_ROWS:
                _add_issue(issues, "ENTRY_BLOCKERS_INVALID", key)
            else:
                for blocker in blockers:
                    if not isinstance(blocker, dict) or any(
                        not isinstance(blocker.get(field), str)
                        or not blocker.get(field, "").strip()
                        or len(blocker.get(field, "")) > MAX_BLOCKER_TEXT
                        for field in ("owner", "action")
                    ):
                        _add_issue(issues, "ENTRY_BLOCKER_OWNER_ACTION_REQUIRED", key)
            if entry.get("issue_url") is None and not blockers:
                _add_issue(issues, "REQUIRED_NULL_URL_BLOCKER_REQUIRED", key)
            for relation_name in ("supersedes", "superseded_by"):
                related = entry.get(relation_name)
                if related is not None and (not isinstance(related, str) or not _safe_key(related)):
                    _add_issue(issues, "SUPERSESSION_RELATION_INVALID", key)

    if check_urls:
        _validate_issue_readbacks_v2(entries_raw, readbacks, readback_pin, issues)
    if require_specialists:
        for family in ("agent-harness", "event-intelligence", "creative-engine"):
            if repo_counts.get(family, 0) < 1:
                _add_issue(issues, "REQUIRED_LEAF_FAMILY_MISSING", family)

    unresolved_pairs: set[tuple[str, str]] = set()
    for row in unresolved_raw:
        if (
            isinstance(row, dict)
            and _safe_key(row.get("requirement_id"))
            and _safe_key(row.get("leaf_key"))
        ):
            unresolved_pairs.add((row["requirement_id"], row["leaf_key"]))
        else:
            _add_issue(issues, "UNRESOLVED_ASSOCIATION_INVALID", "unresolved_associations")

    ownership_by_id: dict[str, dict[str, Any]] = {}
    accountable_pairs: set[tuple[str, str]] = set()
    contributor_pairs: set[tuple[str, str]] = set()
    for index, row in enumerate(ownership_raw):
        if not isinstance(row, dict) or not _safe_key(row.get("requirement_id")):
            _add_issue(issues, "OWNERSHIP_ROW_INVALID", f"ownership-{index}")
            continue
        ident = row["requirement_id"]
        if ident in ownership_by_id:
            _add_issue(issues, "OWNERSHIP_ID_DUPLICATE", ident)
            continue
        ownership_by_id[ident] = row
        if row.get("source_pointer") != f"/ownership_rows/{index}":
            _add_issue(issues, "OWNERSHIP_SOURCE_POINTER_INVALID", ident)
        if row.get("required_for_v1") is not True:
            _add_issue(issues, "OWNERSHIP_REQUIRED_FLAG_INVALID", ident)
        accountable, acceptance = row.get("accountable_delivery_leaf"), row.get("acceptance_owner_leaf")
        if not isinstance(accountable, str) or accountable not in leaves:
            _add_issue(issues, "ACCOUNTABLE_DELIVERY_LEAF_INVALID", ident)
        else:
            if (ident, accountable) not in unresolved_pairs and ident not in leaves[accountable].get("requirement_ids", []):
                _add_issue(issues, "ACCOUNTABLE_LEAF_REQUIREMENT_MISMATCH", ident)
            accountable_pairs.add((ident, accountable))
        if not isinstance(acceptance, str) or acceptance not in leaves:
            _add_issue(issues, "ACCEPTANCE_OWNER_LEAF_INVALID", ident)
        contributors = row.get("required_contributors", [])
        if not isinstance(contributors, list) or len(contributors) > MAX_ENTRIES or any(not isinstance(item, str) or item not in leaves for item in contributors):
            _add_issue(issues, "OWNERSHIP_CONTRIBUTOR_INVALID", ident)
        else:
            if len(contributors) != len(set(contributors)):
                _add_issue(issues, "OWNERSHIP_CONTRIBUTOR_DUPLICATE", ident)
            for contributor in contributors:
                if (ident, contributor) not in unresolved_pairs and ident not in leaves[contributor].get("requirement_ids", []):
                    _add_issue(issues, "CONTRIBUTOR_REQUIREMENT_MISMATCH", ident)
                contributor_pairs.add((ident, contributor))
        expected_scope = "scoped_contribution" if ident in criteria else "local"
        if row.get("scope") != expected_scope:
            _add_issue(issues, "OWNERSHIP_SCOPE_MISMATCH", ident)
        if ident in criteria:
            if isinstance(row.get("global_status"), str) and row.get("global_status") in {"complete", "accepted", "satisfied"}:
                _add_issue(issues, "GLOBAL_SCOPE_OVERCLAIM", ident)
        elif row.get("scope_and_global_role") not in (None, "Scoped requirement") and row.get("scope") != "local":
            _add_issue(issues, "LOCAL_SCOPE_CLASSIFICATION_INVALID", ident)

    expected_endpoint_sources: dict[str, list[tuple[str, str, tuple[str, ...]]]] = {}
    for index, row in enumerate(ownership_raw):
        if not isinstance(row, dict) or not _safe_key(row.get("requirement_id")):
            continue
        ident = row["requirement_id"]
        pointer = f"/ownership_rows/{index}"
        endpoints: dict[str, list[str]] = {}
        accountable = row.get("accountable_delivery_leaf")
        if isinstance(accountable, str) and (ident, accountable) not in unresolved_pairs:
            endpoints.setdefault(accountable, []).append("accountable_delivery")
        contributors = row.get("required_contributors", [])
        if isinstance(contributors, list):
            for contributor in contributors:
                if isinstance(contributor, str) and (ident, contributor) not in unresolved_pairs:
                    roles = endpoints.setdefault(contributor, [])
                    if "required_contributor" not in roles:
                        roles.append("required_contributor")
        for leaf, roles in endpoints.items():
            expected_endpoint_sources.setdefault(leaf, []).append((ident, pointer, tuple(roles)))
    for entry in entries_raw:
        if not isinstance(entry, dict) or not (
            "source_requirement_ids" in entry or "requirement_id_sources" in entry
        ):
            continue
        key = entry.get("key")
        if not _safe_key(key):
            continue
        source_ids = entry.get("source_requirement_ids")
        projected_ids = entry.get("requirement_ids")
        source_rows = entry.get("requirement_id_sources")
        if (
            not isinstance(source_ids, list)
            or any(not _safe_key(value) for value in source_ids)
            or len(source_ids) != len(set(source_ids))
            or not isinstance(projected_ids, list)
            or any(not _safe_key(value) for value in projected_ids)
            or len(projected_ids) != len(set(projected_ids))
            or not isinstance(source_rows, list)
        ):
            _add_issue(issues, "ENTRY_REQUIREMENT_PROJECTION_INVALID", key)
            continue
        expected_rows = expected_endpoint_sources.get(key, [])
        expected_ids = set(source_ids) | {item[0] for item in expected_rows}
        actual_rows: list[tuple[str, str, tuple[str, ...]]] = []
        valid_rows = True
        for source_row in source_rows:
            if (
                not isinstance(source_row, dict)
                or not _safe_key(source_row.get("requirement_id"))
                or not isinstance(source_row.get("ownership_row_pointer"), str)
                or not isinstance(source_row.get("endpoint_roles"), list)
                or any(role not in {"accountable_delivery", "required_contributor"} for role in source_row.get("endpoint_roles", []))
                or len(source_row.get("endpoint_roles", [])) != len(set(source_row.get("endpoint_roles", [])))
            ):
                valid_rows = False
                break
            actual_rows.append((
                source_row["requirement_id"],
                source_row["ownership_row_pointer"],
                tuple(source_row["endpoint_roles"]),
            ))
        if (
            not valid_rows
            or len(actual_rows) != len(set(actual_rows))
            or set(actual_rows) != set(expected_rows)
            or set(projected_ids) != expected_ids
        ):
            _add_issue(issues, "ENTRY_REQUIREMENT_PROJECTION_MISMATCH", key)

    additions: dict[tuple[str, str], dict[str, Any]] = {}
    for row in additions_raw:
        if not isinstance(row, dict) or not isinstance(row.get("requirement_id"), str) or not isinstance(row.get("leaf_key"), str):
            _add_issue(issues, "SOURCE_SUPPORTED_ASSOCIATION_INVALID", "source_supported_associations")
            continue
        pair = (row["requirement_id"], row["leaf_key"])
        if pair in additions:
            _add_issue(issues, "SOURCE_SUPPORTED_ASSOCIATION_DUPLICATE", pair[0])
        additions[pair] = row
    typed_extras: dict[tuple[str, str], dict[str, Any]] = {}
    for row in extras_raw:
        if not isinstance(row, dict) or not isinstance(row.get("requirement_id"), str) or not isinstance(row.get("leaf_key"), str):
            _add_issue(issues, "TYPED_EXTRA_ASSOCIATION_INVALID", "reviewed_typed_extras")
            continue
        pair = (row["requirement_id"], row["leaf_key"])
        if pair in typed_extras or not _safe_key(row.get("role")):
            _add_issue(issues, "TYPED_EXTRA_ASSOCIATION_INVALID", pair[0])
        typed_extras[pair] = row

    pair_review_by_pointer: dict[str, dict[str, Any]] = {}
    pair_review_by_id: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(pair_review_raw):
        if not isinstance(row, dict):
            _add_issue(issues, "PAIR_REVIEW_ROW_INVALID", f"pair-{index}")
            continue
        pointer = f"/pairs/{index}"
        expected_id = f"PAIR-{index + 1:04d}"
        if row.get("source_pointer") != pointer or row.get("review_row_id") != expected_id:
            _add_issue(issues, "PAIR_REVIEW_POINTER_INVALID", expected_id)
        if pointer in pair_review_by_pointer or expected_id in pair_review_by_id:
            _add_issue(issues, "PAIR_REVIEW_ID_DUPLICATE", expected_id)
        pair_review_by_pointer[pointer] = row
        pair_review_by_id[expected_id] = row
        if not isinstance(row.get("requirement_id"), str) or not _safe_key(row.get("requirement_id")):
            _add_issue(issues, "PAIR_REVIEW_REQUIREMENT_INVALID", expected_id)
        if not isinstance(row.get("leaf"), str) or not _safe_key(row.get("leaf")):
            _add_issue(issues, "PAIR_REVIEW_LEAF_INVALID", expected_id)
        before = row.get("before")
        if not isinstance(before, dict):
            _add_issue(issues, "PAIR_REVIEW_BEFORE_INVALID", expected_id)
            continue
        owner_pointer = before.get("ownership_row_pointer")
        owner_match = re.fullmatch(r"/ownership_rows/([0-9]+)", owner_pointer) if isinstance(owner_pointer, str) else None
        if owner_match is None:
            _add_issue(issues, "PAIR_REVIEW_OWNERSHIP_POINTER_INVALID", expected_id)
        else:
            owner_index = int(owner_match.group(1))
            if owner_index >= len(ownership_raw) or not isinstance(ownership_raw[owner_index], dict) or ownership_raw[owner_index].get("requirement_id") != row.get("requirement_id"):
                _add_issue(issues, "PAIR_REVIEW_OWNERSHIP_POINTER_MISMATCH", expected_id)
        actual_present = before.get("actual_contribution_row_present")
        contribution_pointer = before.get("contribution_row_pointer")
        if actual_present is True:
            contribution_match = re.fullmatch(r"/source_contribution_rows/([0-9]+)", contribution_pointer) if isinstance(contribution_pointer, str) else None
            if contribution_match is None:
                _add_issue(issues, "PAIR_REVIEW_CONTRIBUTION_POINTER_INVALID", expected_id)
            else:
                contribution_index = int(contribution_match.group(1))
                if contribution_index >= len(source_rows_raw):
                    _add_issue(issues, "PAIR_REVIEW_CONTRIBUTION_POINTER_MISMATCH", expected_id)
                else:
                    source_row = source_rows_raw[contribution_index]
                    if not isinstance(source_row, dict) or (source_row.get("requirement_id"), source_row.get("owning_leaf_key")) != (row.get("requirement_id"), row.get("leaf")):
                        _add_issue(issues, "PAIR_REVIEW_CONTRIBUTION_POINTER_MISMATCH", expected_id)
        elif actual_present is False:
            if contribution_pointer is not None:
                _add_issue(issues, "PAIR_REVIEW_CONTRIBUTION_POINTER_UNEXPECTED", expected_id)
        else:
            _add_issue(issues, "PAIR_REVIEW_BEFORE_INVALID", expected_id)

    referenced_pair_reviews: set[str] = set()
    for family, table, decisions in (
        ("source_supported_associations", additions, {"ADD_SCOPED_ASSOCIATION", "ADD_SCOPED_ASSOCIATION_WITH_ACCOUNTABILITY_QUALIFICATION"}),
        ("reviewed_typed_extras", typed_extras, {"PRESERVE_TYPED_SCOPED_CONTRIBUTION"}),
    ):
        for pair, row in table.items():
            review_id, pointer = row.get("review_row_id"), row.get("source_pointer")
            source_row = pair_review_by_id.get(review_id) if isinstance(review_id, str) else None
            if source_row is None or source_row.get("source_pointer") != pointer:
                _add_relation_issue(issues, "PAIR_REVIEW_REFERENCE_MISMATCH", pair[0], pair[1])
                continue
            if (source_row.get("requirement_id"), source_row.get("leaf")) != pair:
                _add_relation_issue(issues, "PAIR_REVIEW_PAIR_MISMATCH", pair[0], pair[1])
            if not isinstance(source_row.get("after_recommendation"), str) or source_row.get("after_recommendation") not in decisions:
                _add_relation_issue(issues, "PAIR_REVIEW_DECISION_MISMATCH", pair[0], pair[1])
            projected_role = row.get("role") if family == "reviewed_typed_extras" else row.get("recommended_role")
            if projected_role != source_row.get("recommended_role"):
                _add_relation_issue(issues, "PAIR_REVIEW_ROLE_MISMATCH", pair[0], pair[1])
            if family == "source_supported_associations" and row.get("decision") != source_row.get("after_recommendation"):
                _add_relation_issue(issues, "PAIR_REVIEW_DECISION_MISMATCH", pair[0], pair[1])
            if row.get("evidence") != source_row.get("evidence"):
                _add_relation_issue(issues, "PAIR_REVIEW_EVIDENCE_MISMATCH", pair[0], pair[1])
            if source_row.get("implementation_owner_change") is not False or source_row.get("new_dependency_edge_implied") is not False:
                _add_relation_issue(issues, "PAIR_REVIEW_SCOPE_CHANGE", pair[0], pair[1])
            before = source_row.get("before", {})
            if family == "source_supported_associations" and before.get("actual_contribution_row_present") is not False:
                _add_relation_issue(issues, "PAIR_REVIEW_ADDITION_ALREADY_PRESENT", pair[0], pair[1])
            if family == "reviewed_typed_extras" and before.get("actual_contribution_row_present") is not True:
                _add_relation_issue(issues, "PAIR_REVIEW_EXTRA_NOT_PRESENT", pair[0], pair[1])
            if isinstance(review_id, str):
                if review_id in referenced_pair_reviews:
                    _add_relation_issue(issues, "PAIR_REVIEW_DECISION_REUSED", pair[0], pair[1])
                referenced_pair_reviews.add(review_id)
    for pair in unresolved_pairs:
        rows = [row for row in unresolved_raw if isinstance(row, dict) and (row.get("requirement_id"), row.get("leaf_key")) == pair]
        if len(rows) != 1:
            _add_relation_issue(issues, "UNRESOLVED_ASSOCIATION_DUPLICATE", pair[0], pair[1])
            continue
        row = rows[0]
        review_id = row.get("review_row_id")
        source_row = pair_review_by_id.get(review_id) if isinstance(review_id, str) else None
        if (
            source_row is None
            or source_row.get("source_pointer") != row.get("source_pointer")
            or source_row.get("after_recommendation") != "RETAIN_UNRESOLVED_ACCOUNTABILITY_DO_NOT_AUTO_ADD"
            or (source_row.get("requirement_id"), source_row.get("leaf")) != pair
        ):
            _add_relation_issue(issues, "UNRESOLVED_ASSOCIATION_SOURCE_MISMATCH", pair[0], pair[1])
        if isinstance(review_id, str):
            if review_id in referenced_pair_reviews:
                _add_relation_issue(issues, "PAIR_REVIEW_DECISION_REUSED", pair[0], pair[1])
            referenced_pair_reviews.add(review_id)
    if referenced_pair_reviews != set(pair_review_by_id):
        _add_issue(issues, "PAIR_REVIEW_DECISION_NOT_PROJECTED", "pair_review_rows")

    actual_pairs: set[tuple[str, str]] = set()
    row_ids: set[str] = set()
    actual_role_pairs: set[tuple[tuple[str, str], str]] = set()
    for index, row in enumerate(source_rows_raw):
        if not isinstance(row, dict):
            _add_issue(issues, "CONTRIBUTION_ROW_INVALID", f"contribution-{index}")
            continue
        ident, leaf = row.get("requirement_id"), row.get("owning_leaf_key")
        row_id = row.get("row_id")
        pair = (ident, leaf) if isinstance(ident, str) and isinstance(leaf, str) else None
        if pair is None or not _safe_key(row_id) or not isinstance(row.get("source_pointer"), str):
            _add_issue(issues, "CONTRIBUTION_ROW_INVALID", f"contribution-{index}")
            continue
        if row_id in row_ids:
            _add_issue(issues, "CONTRIBUTION_ROW_ID_DUPLICATE", row_id)
        if row_id != f"SRC-{index + 1:04d}" or row.get("source_pointer") != f"/source_contribution_rows/{index}":
            _add_relation_issue(issues, "CONTRIBUTION_SOURCE_ID_OR_POINTER_INVALID", row_id, ident)
        row_ids.add(row_id)
        actual_pairs.add(pair)
        role = row.get("relation_role")
        if pair in typed_extras:
            expected_role = typed_extras[pair].get("role")
        elif pair in accountable_pairs and pair in contributor_pairs:
            expected_role = "accountable_delivery_and_required_contributor"
        elif pair in accountable_pairs:
            expected_role = "accountable_delivery"
        elif pair in contributor_pairs:
            expected_role = "required_contributor"
        else:
            expected_role = None
        if expected_role is None:
            _add_relation_issue(issues, "CONTRIBUTION_PAIR_UNDECLARED", row_id, ident)
        elif role != expected_role:
            _add_relation_issue(issues, "CONTRIBUTION_ROLE_MISMATCH", row_id, expected_role)
        else:
            actual_role_pairs.add((pair, expected_role))
        if ident not in ownership_by_id:
            _add_issue(issues, "ORPHAN_CONTRIBUTION_REQUIREMENT", ident)
        if leaf not in leaves:
            _add_issue(issues, "CONTRIBUTION_OWNER_NOT_LEAF", ident)
        elif pair not in typed_extras and ident not in leaves[leaf].get("requirement_ids", []):
            _add_issue(issues, "CONTRIBUTION_NOT_DECLARED_BY_LEAF", ident)
        if row.get("source_pointer") != f"/source_contribution_rows/{index}":
            _add_relation_issue(issues, "CONTRIBUTION_SOURCE_POINTER_INVALID", row_id, ident)
        expected_scope = "scoped_contribution" if ident in criteria else "local"
        if row.get("scope") != expected_scope:
            _add_relation_issue(issues, "CONTRIBUTION_SCOPE_MISMATCH", row_id, ident)
        if ident in criteria:
            acceptance = row.get("authoritative_acceptance")
            criterion = criteria[ident]
            if not isinstance(acceptance, dict) or any(acceptance.get(field) != criterion.get(field) for field in ("id", "title", "priority", "owner_role", "verification", "evidence_required")):
                _add_issue(issues, "GLOBAL_ACCEPTANCE_METADATA_MISMATCH", ident)
        elif row.get("authoritative_acceptance") is not None:
            _add_issue(issues, "LOCAL_ROW_CLAIMS_GLOBAL_ACCEPTANCE", ident)
    expected_pairs = accountable_pairs | contributor_pairs
    for pair in sorted(expected_pairs):
        if pair in actual_pairs or pair in additions:
            continue
        if pair in unresolved_pairs:
            _add_relation_issue(issues, "ASSOCIATION_OWNER_DECISION_UNRESOLVED", pair[0], pair[1])
        else:
            _add_relation_issue(issues, "REQUIRED_ASSOCIATION_MISSING", pair[0], pair[1])
    for pair in actual_pairs - expected_pairs - set(typed_extras):
        _add_relation_issue(issues, "CONTRIBUTION_PAIR_UNDECLARED", pair[0], pair[1])
    for pair, row in additions.items():
        if pair not in expected_pairs or pair in actual_pairs or pair[1] not in leaves:
            _add_relation_issue(issues, "SOURCE_SUPPORTED_ASSOCIATION_INVALID", pair[0], pair[1])
        if not isinstance(row.get("association_id"), str) or not _safe_key(row.get("association_id")):
            _add_relation_issue(issues, "SOURCE_SUPPORTED_ASSOCIATION_ID_INVALID", pair[0], pair[1])
        if not isinstance(row.get("source_pointer"), str) or not re.fullmatch(r"/pairs/[0-9]+", row.get("source_pointer", "")):
            _add_relation_issue(issues, "SOURCE_SUPPORTED_ASSOCIATION_POINTER_INVALID", pair[0], pair[1])
        if row.get("decision") not in {"ADD_SCOPED_ASSOCIATION", "ADD_SCOPED_ASSOCIATION_WITH_ACCOUNTABILITY_QUALIFICATION"}:
            _add_relation_issue(issues, "SOURCE_SUPPORTED_ASSOCIATION_DECISION_INVALID", pair[0], pair[1])
        if row.get("new_dependency_edge_implied") is True or row.get("implementation_owner_change") is True:
            _add_relation_issue(issues, "SOURCE_SUPPORTED_ASSOCIATION_SCOPE_CHANGED", pair[0], pair[1])
    for pair in typed_extras:
        if pair not in actual_pairs or pair in expected_pairs:
            _add_relation_issue(issues, "TYPED_EXTRA_ASSOCIATION_INVALID", pair[0], pair[1])
    all_entry_requirement_ids: set[str] = set()
    for entry in entries_raw:
        if isinstance(entry, dict) and isinstance(entry.get("requirement_ids"), list):
            all_entry_requirement_ids.update(value for value in entry["requirement_ids"] if isinstance(value, str))
    for ident in sorted(all_entry_requirement_ids - set(ownership_by_id)):
        _add_issue(issues, "REQUIRED_ID_WITHOUT_OWNERSHIP", ident)
    for entry in entries_raw:
        if not isinstance(entry, dict):
            continue
        key = entry.get("key")
        supersedes = entry.get("supersedes")
        if isinstance(supersedes, str):
            prior = entry_by_key.get(supersedes)
            if prior is None or prior.get("superseded_by") != key:
                _add_issue(issues, "SUPERSESSION_RELATION_UNVERIFIED", _label(key))
        superseded_by = entry.get("superseded_by")
        if isinstance(superseded_by, str):
            replacement = entry_by_key.get(superseded_by)
            if replacement is None or replacement.get("supersedes") != key:
                _add_issue(issues, "SUPERSESSION_RELATION_UNVERIFIED", _label(key))

    graph_summary = _validate_projection_graph(coverage.get("execution_graph"), entry_by_key, leaves, issues) if check_dag else {}
    stage_relations = coverage.get("stage_relations")
    if not isinstance(stage_relations, list):
        _add_issue(issues, "STAGE_RELATION_FAMILY_MISSING", "stage_relations")
        stage_relations = []
    stage_relations = _bounded_rows(stage_relations, MAX_GRAPH_ROWS, "STAGE_RELATION_ROW_LIMIT_EXCEEDED", "stage_relations", issues)
    relation_ids: set[str] = set()
    source_pointers: set[str] = set()
    if len(source_stage_raw) != len(stage_relations):
        _add_issue(issues, "STAGE_RELATION_SOURCE_CARDINALITY_MISMATCH", "source_stage_rows")
    source_stage_by_pointer: dict[str, dict[str, Any]] = {}
    for index, source_row in enumerate(source_stage_raw):
        expected_pointer = f"/staged_edges/{index}"
        if not isinstance(source_row, dict) or source_row.get("source_pointer") != expected_pointer:
            _add_issue(issues, "SOURCE_STAGE_POINTER_INVALID", f"stage-{index}")
            continue
        source_stage_by_pointer[expected_pointer] = source_row
    external_nodes = coverage.get("external_artifact_nodes")
    if not isinstance(external_nodes, list) or len(external_nodes) > MAX_ENTRIES:
        _add_issue(issues, "EXTERNAL_ARTIFACT_FAMILY_INVALID", "external_artifact_nodes")
        external_nodes = []
    external_by_id: dict[str, dict[str, Any]] = {}
    used_external_ids: set[str] = set()
    for index, node in enumerate(external_nodes):
        if not isinstance(node, dict) or not _safe_key(node.get("artifact_id")) or node.get("artifact_id") in external_by_id:
            _add_issue(issues, "EXTERNAL_ARTIFACT_INVALID", f"external-{index}")
            continue
        ident = node["artifact_id"]
        external_by_id[ident] = node
        for field in ("owner_role", "deliverable", "status", "source_pointer"):
            if not isinstance(node.get(field), str) or not node.get(field).strip():
                _add_issue(issues, "EXTERNAL_ARTIFACT_METADATA_MISSING", ident)
        if node.get("source_pointer") != f"/nodes/{index}" or node.get("upstream_source_pointer") != f"/external_artifact_nodes/{ident}":
            _add_issue(issues, "EXTERNAL_ARTIFACT_SOURCE_POINTER_INVALID", ident)
        if not isinstance(node.get("status"), str) or node.get("status") not in {"pending_external", "accepted", "rejected", "unresolved"}:
            _add_issue(issues, "EXTERNAL_ARTIFACT_STATUS_INVALID", ident)
        if not isinstance(node.get("action_status"), str) or node.get("action_status") not in {"declared_in_source", "not_declared_in_source"}:
            _add_issue(issues, "EXTERNAL_ACTION_STATUS_INVALID", ident)
        if node.get("action_status") == "not_declared_in_source" and node.get("action") is not None:
            _add_issue(issues, "EXTERNAL_ACTION_UNDECLARED", ident)
        elif node.get("action_status") == "declared_in_source" and (not isinstance(node.get("action"), str) or not node.get("action", "").strip()):
            _add_issue(issues, "EXTERNAL_ACTION_MISSING", ident)
    for index, relation in enumerate(stage_relations):
        record = f"stage-{index}"
        if not isinstance(relation, dict) or not _safe_key(relation.get("relation_id")):
            _add_issue(issues, "STAGE_RELATION_INVALID", record)
            continue
        ident = relation["relation_id"]
        if ident in relation_ids:
            _add_issue(issues, "STAGE_RELATION_ID_DUPLICATE", ident)
        relation_ids.add(ident)
        pointer = relation.get("source_pointer")
        if not isinstance(pointer, str) or pointer in source_pointers:
            _add_issue(issues, "STAGE_RELATION_SOURCE_POINTER_INVALID", ident)
        source_pointers.add(pointer if isinstance(pointer, str) else "")
        source_row = source_stage_by_pointer.get(pointer) if isinstance(pointer, str) else None
        if source_row is None:
            _add_issue(issues, "STAGE_RELATION_SOURCE_NOT_FOUND", ident)
        else:
            pointer_index = int(pointer.rsplit("/", 1)[-1]) if pointer.rsplit("/", 1)[-1].isdigit() else -1
            if relation.get("relation_id") != f"STG-{pointer_index:03d}":
                _add_issue(issues, "STAGE_RELATION_ID_SOURCE_MISMATCH", ident)
            if relation.get("consumer_key") != source_row.get("consumer_key") or relation.get("consumer_requirement_ids") != source_row.get("consumer_requirement_ids"):
                _add_issue(issues, "STAGE_RELATION_CONSUMER_SOURCE_MISMATCH", ident)
            expected_external = source_row.get("external_endpoint") is True
            expected_producer = None if expected_external else source_row.get("prerequisite_key")
            expected_external_id = source_row.get("prerequisite_key") if expected_external else None
            if relation.get("producer_key") != expected_producer or relation.get("external_artifact_id") != expected_external_id:
                _add_issue(issues, "STAGE_RELATION_PRODUCER_SOURCE_MISMATCH", ident)
            if relation.get("producer_requirement_ids") != source_row.get("prerequisite_requirement_ids"):
                _add_issue(issues, "STAGE_RELATION_PRODUCER_REQUIREMENT_SOURCE_MISMATCH", ident)
            source_receipts = source_row.get("required_owner_receipt_ids", [])
            relation_receipts = relation.get("required_owner_receipt_ids", [])
            if (
                not isinstance(source_receipts, list)
                or any(not _safe_key(value) for value in source_receipts)
                or len(source_receipts) != len(set(source_receipts))
                or not isinstance(relation_receipts, list)
                or any(not _safe_key(value) for value in relation_receipts)
                or len(relation_receipts) != len(set(relation_receipts))
            ):
                _add_issue(issues, "STAGE_RELATION_RECEIPTS_INVALID", ident)
            elif relation_receipts != source_receipts:
                _add_issue(issues, "STAGE_RELATION_RECEIPTS_SOURCE_MISMATCH", ident)
            if relation.get("shared_requirement_ids") != source_row.get("shared_requirement_ids"):
                _add_issue(issues, "STAGE_RELATION_SHARED_REQUIREMENT_SOURCE_MISMATCH", ident)
            if relation.get("external_endpoint") is not source_row.get("external_endpoint") or relation.get("disjoint_internal") is not source_row.get("disjoint_internal"):
                _add_issue(issues, "STAGE_RELATION_ENDPOINT_FLAGS_SOURCE_MISMATCH", ident)
            if relation.get("source_evidence_file") != source_row.get("evidence_file") or relation.get("source_evidence_line") != source_row.get("evidence_line"):
                _add_issue(issues, "STAGE_RELATION_PROVENANCE_SOURCE_MISMATCH", ident)
            for field in ("consumer_stage", "deliverable", "reason", "status"):
                if relation.get(field) != source_row.get(field):
                    _add_issue(issues, "STAGE_RELATION_SOURCE_MISMATCH", ident)
            if relation.get("prerequisite_stage") != source_row.get("prerequisite_stage"):
                _add_issue(issues, "STAGE_RELATION_SOURCE_MISMATCH", ident)
            reason = source_row.get("reason")
            expected_applicability = "required" if isinstance(reason, str) and reason in REQUIRED_STAGE_REASONS else "unresolved"
            if relation.get("applicability") != expected_applicability:
                _add_issue(issues, "STAGE_RELATION_APPLICABILITY_SOURCE_MISMATCH", ident)
            expected_scope = "live_only" if source_row.get("reason") == "live dispatch only; no additional block on offline implementation" else "declared_stage"
            if relation.get("application_scope") != expected_scope:
                _add_issue(issues, "STAGE_RELATION_SCOPE_SOURCE_MISMATCH", ident)
        consumer = relation.get("consumer_key")
        producer = relation.get("producer_key")
        external_id = relation.get("external_artifact_id")
        if not isinstance(consumer, str) or consumer not in leaves:
            _add_issue(issues, "STAGE_RELATION_CONSUMER_INVALID", ident)
        if not isinstance(consumer, str) or consumer not in leaves:
            consumer_req_ids: list[str] = []
        else:
            consumer_req_ids = relation.get("consumer_requirement_ids") if isinstance(relation.get("consumer_requirement_ids"), list) else []
            if not isinstance(relation.get("consumer_requirement_ids"), list) or not consumer_req_ids or any(not isinstance(value, str) or value not in leaves[consumer].get("requirement_ids", []) for value in consumer_req_ids):
                _add_issue(issues, "STAGE_RELATION_CONSUMER_REQUIREMENT_MISMATCH", ident)
            elif len(consumer_req_ids) != len(set(consumer_req_ids)):
                _add_issue(issues, "STAGE_RELATION_CONSUMER_REQUIREMENT_INVALID", ident)
        if external_id is not None:
            if not isinstance(external_id, str) or external_id not in external_by_id or producer is not None:
                _add_issue(issues, "STAGE_RELATION_EXTERNAL_ENDPOINT_INVALID", ident)
            else:
                used_external_ids.add(external_id)
            if relation.get("producer_requirement_ids") not in (None, []):
                _add_issue(issues, "STAGE_RELATION_EXTERNAL_REQUIREMENTS_INVALID", ident)
        elif not isinstance(producer, str) or producer not in leaves:
            _add_issue(issues, "STAGE_RELATION_PRODUCER_INVALID", ident)
        else:
            producer_req_ids = relation.get("producer_requirement_ids")
            if not isinstance(producer_req_ids, list) or not producer_req_ids or any(not isinstance(value, str) or value not in leaves[producer].get("requirement_ids", []) for value in producer_req_ids):
                _add_issue(issues, "STAGE_RELATION_PRODUCER_REQUIREMENT_MISMATCH", ident)
            elif len(producer_req_ids) != len(set(producer_req_ids)):
                _add_issue(issues, "STAGE_RELATION_PRODUCER_REQUIREMENT_INVALID", ident)
        shared_ids = relation.get("shared_requirement_ids")
        if not isinstance(shared_ids, list) or any(not isinstance(value, str) or not _safe_key(value) for value in shared_ids):
            _add_issue(issues, "STAGE_RELATION_SHARED_REQUIREMENT_INVALID", ident)
        elif len(shared_ids) != len(set(shared_ids)):
            _add_issue(issues, "STAGE_RELATION_SHARED_REQUIREMENT_INVALID", ident)
        else:
            consumer_declared = set(consumer_req_ids) if consumer_req_ids and all(isinstance(value, str) for value in consumer_req_ids) else set()
            producer_declared = set(producer_req_ids) if external_id is None and isinstance(producer_req_ids, list) and all(isinstance(value, str) for value in producer_req_ids) else set()
            expected_shared = consumer_declared & producer_declared
            if set(shared_ids) != expected_shared:
                _add_issue(issues, "STAGE_RELATION_SHARED_REQUIREMENT_MISMATCH", ident)
            expected_disjoint = external_id is None and not expected_shared
            if relation.get("disjoint_internal") is not expected_disjoint:
                _add_issue(issues, "STAGE_RELATION_DISJOINT_FLAG_MISMATCH", ident)
        if relation.get("consumer_stage") not in PROJECTION_STAGES or relation.get("prerequisite_stage") not in PROJECTION_STAGES:
            _add_issue(issues, "STAGE_RELATION_STAGE_INVALID", ident)
        if not isinstance(relation.get("applicability"), str) or relation.get("applicability") not in {"required", "unresolved"}:
            _add_issue(issues, "STAGE_RELATION_APPLICABILITY_INVALID", ident)
        required_receipts = relation.get("required_owner_receipt_ids", [])
        if (
            not isinstance(required_receipts, list)
            or any(not _safe_key(value) for value in required_receipts)
            or len(required_receipts) != len(set(required_receipts))
        ):
            _add_issue(issues, "STAGE_RELATION_RECEIPTS_INVALID", ident)
        if not isinstance(relation.get("status"), str) or relation.get("status") not in {"unverified", "pending", "satisfied", "rejected"}:
            _add_issue(issues, "STAGE_RELATION_STATUS_INVALID", ident)
        if not isinstance(relation.get("source_evidence_file"), str) or not relation.get("source_evidence_file", "").strip():
            _add_issue(issues, "STAGE_RELATION_PROVENANCE_INVALID", ident)
        line = relation.get("source_evidence_line")
        if not isinstance(line, int) or isinstance(line, bool) or line < 1:
            _add_issue(issues, "STAGE_RELATION_PROVENANCE_INVALID", ident)
    if used_external_ids != set(external_by_id):
        _add_issue(issues, "EXTERNAL_ARTIFACT_RELATION_SET_MISMATCH", "external_artifact_nodes")

    global_ids = set(criteria)
    mapped_global = set(ownership_by_id) & global_ids
    local_ids = set(ownership_by_id) - global_ids
    global_rows = sum(1 for row in source_rows_raw if isinstance(row, dict) and row.get("requirement_id") in global_ids)
    dimensions = {
        "required_leaf_records": len(leaves),
        "agent_harness_leaf_records": repo_counts.get("agent-harness", 0),
        "specialist_leaf_records": sum(value for key, value in repo_counts.items() if key != "agent-harness"),
        "source_contribution_rows": len(source_rows_raw),
        "unique_mapped_requirement_ids": len(ownership_by_id),
        "local_scoped_requirement_ids": len(local_ids),
        "global_ids_with_scoped_contributions": len(mapped_global),
        "global_criteria_total": len(criteria),
        "global_ids_outside_scoped_contribution": len(global_ids - mapped_global),
        "contribution_rows_to_global_ids": global_rows,
        "contribution_rows_to_local_ids": len(source_rows_raw) - global_rows,
        "source_supported_associations": len(additions),
        "typed_extra_associations": len(typed_extras),
        "unresolved_associations": len(unresolved_pairs),
        "stage_relations": len(stage_relations),
        "external_artifact_nodes": len(external_by_id),
    }
    expected_dimensions = coverage.get("source_dimensions")
    if isinstance(expected_dimensions, dict):
        checks = {
            "required_issue_outcomes": len(entries_raw), "source_contribution_rows": len(source_rows_raw),
            "ownership_records": len(ownership_by_id), "canonical_criteria": len(criteria),
            "canonical_criteria_outside_scope": len(global_ids - mapped_global),
            "staged_relations": len(stage_relations), "external_artifact_nodes": len(external_by_id),
        }
        for name, value in checks.items():
            if expected_dimensions.get(name) != value:
                _add_issue(issues, "SOURCE_DIMENSION_PIN_MISMATCH", name)
    return issues, dimensions, leaves, graph_summary


def _validate_issue_readbacks_v2(
    entries: list[Any], readbacks: dict[str, Any], pin: dict[str, Any] | None,
    issues: list[dict[str, str]],
) -> None:
    if readbacks.get("format") != "agent-harness.issue-readbacks/v2":
        _add_issue(issues, "ISSUE_READBACK_FORMAT_UNSUPPORTED", "issue-readbacks")
    if pin is None or readbacks.get("capture_id") != pin.get("capture_id"):
        _add_issue(issues, "ISSUE_READBACK_CAPTURE_MISMATCH", "issue-readbacks")
    if pin is not None and readbacks.get("captured_at") != pin.get("captured_at"):
        _add_issue(issues, "ISSUE_READBACK_TIME_MISMATCH", "issue-readbacks")
    if pin is not None and readbacks.get("observed_revision") != pin.get("observed_revision"):
        _add_issue(issues, "ISSUE_READBACK_REVISION_MISMATCH", "issue-readbacks")
    if readbacks.get("pages_complete") is not True or pin is None or pin.get("pages_complete") is not True:
        _add_issue(issues, "ISSUE_READBACK_PAGES_INCOMPLETE", "issue-readbacks")
    try:
        _iso_utc(readbacks.get("captured_at"), "ISSUE_READBACK_TIME_INVALID", "issue-readbacks")
    except InputError as exc:
        _add_issue(issues, exc.code, exc.record)
    rows = readbacks.get("issues")
    if not isinstance(rows, list):
        _add_issue(issues, "ISSUE_READBACK_ROWS_MISSING", "issue-readbacks")
        return
    rows = _bounded_rows(rows, MAX_ENTRIES, "ISSUE_READBACK_ROW_LIMIT_EXCEEDED", "issue-readbacks", issues)
    by_url: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or not isinstance(row.get("url"), str):
            _add_issue(issues, "ISSUE_READBACK_ROW_INVALID", f"readback-{index}")
            continue
        try:
            parsed = urlparse(row["url"])
        except ValueError:
            _add_issue(issues, "ISSUE_READBACK_URL_INVALID", f"readback-{index}")
            continue
        match = ISSUE_PATH_RE.fullmatch(parsed.path)
        if (
            parsed.scheme != "https" or parsed.hostname != "github.com" or match is None
            or len(match.group(3)) > MAX_ISSUE_NUMBER_DIGITS or parsed.query or parsed.fragment
            or parsed.username is not None or parsed.password is not None
        ):
            _add_issue(issues, "ISSUE_READBACK_URL_INVALID", f"readback-{index}")
            continue
        if row["url"] in by_url:
            _add_issue(issues, "ISSUE_READBACK_DUPLICATE", "issue-readbacks")
            continue
        by_url[row["url"]] = row
    for entry in entries:
        if not isinstance(entry, dict) or entry.get("required_for_v1") is not True:
            continue
        key = _label(entry.get("key"))
        identity = _entry_identity(entry, key, issues)
        if identity is None:
            _add_issue(issues, "REQUIRED_ISSUE_URL_UNPROVEN", key)
            continue
        repo_full, url, number = identity
        row = by_url.get(url)
        if row is None:
            _add_issue(issues, "ISSUE_READBACK_NOT_FOUND", key)
            continue
        current = entry.get("current_readback")
        if row.get("repository") not in (entry.get("repository"), repo_full):
            _add_issue(issues, "ISSUE_READBACK_REPOSITORY_MISMATCH", key)
        if row.get("number") != number or isinstance(row.get("number"), bool):
            _add_issue(issues, "ISSUE_READBACK_NUMBER_MISMATCH", key)
        if not isinstance(row.get("title"), str) or not row.get("title").strip():
            _add_issue(issues, "ISSUE_READBACK_TITLE_INVALID", key)
        if not isinstance(row.get("body_sha256"), str) or HASH_RE.fullmatch(row.get("body_sha256", "")) is None:
            _add_issue(issues, "ISSUE_READBACK_BODY_INVALID", key)
        if not isinstance(row.get("state"), str) or row.get("state") not in {"open", "closed"}:
            _add_issue(issues, "ISSUE_READBACK_STATE_INVALID", key)
        if isinstance(current, dict):
            for field in ("repository", "number", "url", "title", "state", "body_sha256"):
                if current.get(field) != row.get(field):
                    _add_issue(issues, "CURRENT_READBACK_MISMATCH", key)
                    break
        entry_state = entry.get("state")
        readback_state = row.get("state")
        if entry_state == "closed" and readback_state != "closed":
            _add_issue(issues, "ISSUE_READBACK_STATE_MISMATCH", key)
        elif readback_state == "closed":
            if isinstance(entry_state, str) and entry_state in {
                "planned", "in_progress", "blocked", "deferred", "not_planned", "not-planned"
            }:
                _add_issue(issues, "ISSUE_READBACK_STATE_MISMATCH", key)
            if not _has_exact_closure_evidence(
                entry.get("closure_evidence"), _required_test_ids(entry, fallback_to_legacy_tests=False)
            ):
                _add_issue(issues, "CLOSED_LEAF_WITHOUT_EXACT_EVIDENCE", key)
        current = entry.get("current_readback")
        if isinstance(current, dict):
            body_changed = row.get("body_sha256") != entry.get("historical_body_sha256")
            title_changed = isinstance(entry.get("historical_issue_title"), str) and row.get("title") != entry.get("historical_issue_title")
            state_changed = isinstance(entry.get("historical_issue_state"), str) and row.get("state") != entry.get("historical_issue_state")
            if (body_changed or title_changed or state_changed) and not _has_source_semantic_review(entry, current):
                if body_changed:
                    _add_issue(issues, "ISSUE_BODY_CHANGE_UNADJUDICATED", key)
                if title_changed:
                    _add_issue(issues, "ISSUE_TITLE_CHANGE_UNADJUDICATED", key)
                if state_changed:
                    _add_issue(issues, "ISSUE_STATE_CHANGE_UNADJUDICATED", key)


def _has_source_semantic_review(entry: dict[str, Any], current: dict[str, Any]) -> bool:
    review = entry.get("body_semantic_review")
    if not isinstance(review, dict) or entry.get("body_semantic_disposition") != "reviewed_source_semantics":
        return False
    pointers = review.get("source_pointers")
    return (
        review.get("decision") == "source_semantics_reviewed"
        and review.get("historical_body_sha256") == entry.get("historical_body_sha256")
        and review.get("current_body_sha256") == current.get("body_sha256")
        and review.get("historical_title") == entry.get("historical_issue_title")
        and review.get("current_title") == current.get("title")
        and review.get("historical_state") == entry.get("historical_issue_state")
        and review.get("current_state") == current.get("state")
        and isinstance(review.get("reviewer_role"), str) and bool(review.get("reviewer_role", "").strip())
        and isinstance(review.get("rationale"), str) and 0 < len(review.get("rationale", "")) <= MAX_BLOCKER_TEXT
        and isinstance(pointers, list) and bool(pointers) and all(isinstance(value, str) and value.startswith("/") for value in pointers)
        and review.get("implementation_scope_changed") is False
        and review.get("external_acceptance_claimed") is False
    )


def _validate_projection_graph(
    graph: Any, entry_by_key: dict[str, dict[str, Any]], leaves: dict[str, dict[str, Any]],
    issues: list[dict[str, str]],
) -> dict[str, Any]:
    if not isinstance(graph, dict) or not isinstance(graph.get("edges"), dict):
        _add_issue(issues, "EXECUTION_DAG_MISSING", "execution_graph")
        return {}
    edges_raw = graph["edges"]
    if len(edges_raw) > MAX_GRAPH_ROWS:
        _add_issue(issues, "EXECUTION_EDGE_ROW_LIMIT_EXCEEDED", "execution_graph")
        edges_raw = dict(list(edges_raw.items())[:MAX_GRAPH_ROWS])
    edges: dict[str, set[str]] = {}
    for consumer, prereqs in edges_raw.items():
        if not isinstance(consumer, str) or consumer not in leaves or not isinstance(prereqs, list) or len(prereqs) > MAX_ENTRIES:
            _add_issue(issues, "EXECUTION_EDGE_INVALID", _label(consumer))
            continue
        normalized: set[str] = set()
        for prerequisite in prereqs:
            if not isinstance(prerequisite, str) or prerequisite not in leaves:
                _add_issue(issues, "EXECUTION_EDGE_UNKNOWN_NODE", consumer)
                continue
            if prerequisite == consumer:
                _add_issue(issues, "EXECUTION_EDGE_SELF", consumer)
            if prerequisite in normalized:
                _add_issue(issues, "EXECUTION_EDGE_DUPLICATE", consumer)
            normalized.add(prerequisite)
        edges[consumer] = normalized
    for node in leaves:
        if node not in edges:
            _add_issue(issues, "EXECUTION_NODE_MISSING", node)
            edges[node] = set()
    _check_cycle(edges, issues)
    gates = graph.get("gate_outcomes")
    if not isinstance(gates, list):
        _add_issue(issues, "GATE_FAN_IN_INVALID", "execution_graph")
        gates = []
    gate_ids: set[str] = set()
    for gate in gates:
        if not isinstance(gate, dict) or not isinstance(gate.get("key"), str) or gate.get("key") not in leaves:
            _add_issue(issues, "GATE_FAN_IN_INVALID", "execution_graph")
            continue
        key = gate["key"]
        if key in gate_ids or gate.get("outcome_required") is not True or gate.get("node_role") != "release_gate_fan_in":
            _add_issue(issues, "GATE_FAN_IN_INVALID", key)
        gate_ids.add(key)
        observed = gate.get("fan_in")
        if (
            not isinstance(observed, list)
            or any(not isinstance(item, str) for item in observed)
            or set(observed) != edges.get(key, set())
            or len(observed) != len(set(observed))
        ):
            _add_issue(issues, "GATE_FAN_IN_MISMATCH", key)
    for key, entry in entry_by_key.items():
        roles = entry.get("node_roles", [])
        if not isinstance(roles, list) or "required_acceptance_outcome" not in roles:
            _add_issue(issues, "REQUIRED_OUTCOME_ROLE_MISSING", key)
        normalized_roles = roles if isinstance(roles, list) else []
        if any(not isinstance(role, str) or role not in {"required_acceptance_outcome", "release_gate_fan_in"} for role in normalized_roles):
            _add_issue(issues, "EXECUTION_NODE_ROLE_INVALID", key)
        if ("release_gate_fan_in" in normalized_roles) != (key in gate_ids):
            _add_issue(issues, "GATE_NODE_ROLE_MISMATCH", key)
    if len(gate_ids) != 1:
        _add_issue(issues, "RELEASE_GATE_NODE_CARDINALITY_INVALID", "execution_graph")

    remaining = {node: set(deps) for node, deps in edges.items()}
    waves: list[list[str]] = []
    visited: list[str] = []
    while remaining:
        ready = sorted(node for node, deps in remaining.items() if not deps)
        if not ready:
            break
        waves.append(ready)
        for node in ready:
            visited.append(node)
            remaining.pop(node)
        for deps in remaining.values():
            deps.difference_update(ready)
    if len(visited) != len(edges):
        return {"nodes": len(edges), "edges": sum(len(v) for v in edges.values()), "acyclic": False, "waves": waves}
    reachable: set[str] = set()
    pending = list(gate_ids)
    while pending:
        current = pending.pop()
        if current in reachable:
            continue
        reachable.add(current)
        pending.extend(edges.get(current, set()) - reachable)
    if set(leaves) - reachable:
        _add_issue(issues, "REQUIRED_OUTCOME_NOT_REACHABLE_FROM_GATE", sorted(set(leaves) - reachable)[0])
    depth: dict[str, int] = {}
    parent: dict[str, str | None] = {}
    for node in visited:
        prerequisites = sorted(edges[node])
        if not prerequisites:
            depth[node], parent[node] = 1, None
        else:
            best = max(prerequisites, key=lambda item: (depth.get(item, 0), item))
            depth[node], parent[node] = depth.get(best, 0) + 1, best
    terminal_candidates = [node for node in edges if not any(node in deps for deps in edges.values())]
    terminal = max(terminal_candidates, key=lambda item: (depth.get(item, 0), item)) if terminal_candidates else None
    path: list[str] = []
    cursor = terminal
    while cursor is not None:
        path.append(cursor)
        cursor = parent.get(cursor)
    path.reverse()
    source_claims = graph.get("source_claims") if isinstance(graph.get("source_claims"), dict) else {}
    if source_claims.get("acyclic") is not True:
        _add_issue(issues, "EXECUTION_GRAPH_SOURCE_CLAIM_MISMATCH", "acyclic")
    if source_claims.get("wave_count") != len(waves):
        _add_issue(issues, "EXECUTION_GRAPH_SOURCE_CLAIM_MISMATCH", "wave_count")
    if source_claims.get("waves") != waves:
        _add_issue(issues, "EXECUTION_GRAPH_SOURCE_CLAIM_MISMATCH", "waves")
    source_path = source_claims.get("critical_path")
    longest_length = max(depth.values(), default=0)
    source_path_valid = (
        isinstance(source_path, list)
        and bool(source_path)
        and all(isinstance(node, str) and node in edges for node in source_path)
        and len(source_path) == len(set(source_path))
        and not edges.get(source_path[0], set())
        and not any(source_path[-1] in prerequisites for prerequisites in edges.values())
        and all(previous in edges[current] for previous, current in zip(source_path, source_path[1:]))
        and len(source_path) == longest_length
    )
    if not source_path_valid:
        _add_issue(issues, "EXECUTION_GRAPH_SOURCE_CLAIM_MISMATCH", "critical_path")
    if source_claims.get("all_59_required_reachable") is not True:
        _add_issue(issues, "EXECUTION_GRAPH_SOURCE_CLAIM_MISMATCH", "all_59_required_reachable")
    return {"nodes": len(edges), "edges": sum(len(v) for v in edges.values()), "acyclic": True,
            "waves": waves, "wave_count": len(waves), "critical_path": path, "critical_path_length": len(path)}


def _canonical_criteria(canonical: dict[str, Any], issues: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    criteria = canonical.get("criteria")
    if not isinstance(criteria, list):
        _add_issue(issues, "CANONICAL_CRITERIA_MISSING", "canonical-checklist")
        return {}
    criteria = _bounded_rows(criteria, MAX_ENTRIES, "CANONICAL_ROW_LIMIT_EXCEEDED", "canonical-checklist", issues)
    result: dict[str, dict[str, Any]] = {}
    required = {"id", "title", "priority", "owner_role", "verification", "evidence_required"}
    for index, item in enumerate(criteria):
        if not isinstance(item, dict):
            _add_issue(issues, "CANONICAL_CRITERION_INVALID", f"criterion-{index}")
            continue
        ident = item.get("id")
        if not _safe_key(ident):
            _add_issue(issues, "CANONICAL_CRITERION_ID_INVALID", f"criterion-{index}")
            continue
        if ident in result:
            _add_issue(issues, "CANONICAL_CRITERION_DUPLICATE", ident)
            continue
        if not required.issubset(item):
            _add_issue(issues, "CANONICAL_CRITERION_FIELDS_MISSING", ident)
        for field in ("title", "priority", "owner_role", "verification", "evidence_required"):
            text = item.get(field)
            if not isinstance(text, str) or not text.strip() or len(text) > 4096:
                _add_issue(issues, "CANONICAL_CRITERION_FIELD_INVALID", ident)
                break
        result[ident] = item
    return result


def _entry_identity(
    entry: dict[str, Any], record: str, issues: list[dict[str, str]]
) -> tuple[str, str, int] | None:
    repo = entry.get("repository")
    url = entry.get("issue_url")
    if not isinstance(repo, str) or REPOSITORY_RE.fullmatch(repo) is None:
        _add_issue(issues, "ENTRY_REPOSITORY_INVALID", record)
        return None
    if url is None:
        return None
    if not isinstance(url, str):
        _add_issue(issues, "ENTRY_ISSUE_URL_INVALID", record)
        return None
    try:
        parsed = urlparse(url)
    except ValueError:
        _add_issue(issues, "ENTRY_ISSUE_URL_INVALID", record)
        return None
    match = ISSUE_PATH_RE.fullmatch(parsed.path)
    if parsed.scheme != "https" or parsed.hostname != "github.com" or not match:
        _add_issue(issues, "ENTRY_ISSUE_URL_INVALID", record)
        return None
    if parsed.query or parsed.fragment or parsed.username is not None or parsed.password is not None:
        _add_issue(issues, "ENTRY_ISSUE_URL_INVALID", record)
        return None
    owner, url_repo, number_text = match.groups()
    if len(number_text) > MAX_ISSUE_NUMBER_DIGITS:
        _add_issue(issues, "ENTRY_ISSUE_URL_INVALID", record)
        return None
    if url_repo != repo:
        _add_issue(issues, "ENTRY_REPOSITORY_URL_MISMATCH", record)
        return None
    return owner + "/" + url_repo, url, int(number_text)


def _check_issue_readbacks(
    entries: list[dict[str, Any]],
    readbacks: dict[str, Any],
    pin: dict[str, Any] | None,
    issues: list[dict[str, str]],
) -> None:
    if readbacks.get("format") != "agent-harness.issue-readbacks/v1":
        _add_issue(issues, "ISSUE_READBACK_FORMAT_UNSUPPORTED", "issue-readbacks")
    if pin is None:
        _add_issue(issues, "ISSUE_READBACK_PIN_REQUIRED", "issue-readbacks")
    elif readbacks.get("capture_id") != pin.get("capture_id"):
        _add_issue(issues, "ISSUE_READBACK_CAPTURE_MISMATCH", "issue-readbacks")
    if pin is not None and readbacks.get("captured_at") != pin.get("captured_at"):
        _add_issue(issues, "ISSUE_READBACK_TIME_MISMATCH", "issue-readbacks")
    if pin is not None and pin.get("observed_revision") is not None and readbacks.get("observed_revision") != pin.get("observed_revision"):
        _add_issue(issues, "ISSUE_READBACK_REVISION_MISMATCH", "issue-readbacks")
    try:
        _iso_utc(readbacks.get("captured_at"), "ISSUE_READBACK_TIME_INVALID", "issue-readbacks")
    except InputError as exc:
        _add_issue(issues, exc.code, exc.record)
    if readbacks.get("pages_complete") is not True or (pin is not None and pin.get("pages_complete") is not True):
        _add_issue(issues, "ISSUE_READBACK_PAGES_INCOMPLETE", "issue-readbacks")
    rows = readbacks.get("issues")
    if not isinstance(rows, list):
        _add_issue(issues, "ISSUE_READBACK_ROWS_MISSING", "issue-readbacks")
        return
    rows = _bounded_rows(rows, MAX_ENTRIES, "ISSUE_READBACK_ROW_LIMIT_EXCEEDED", "issue-readbacks", issues)
    by_url: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            _add_issue(issues, "ISSUE_READBACK_ROW_INVALID", f"readback-{index}")
            continue
        url = row.get("url")
        if not isinstance(url, str):
            _add_issue(issues, "ISSUE_READBACK_URL_INVALID", f"readback-{index}")
            continue
        try:
            parsed = urlparse(url)
        except ValueError:
            _add_issue(issues, "ISSUE_READBACK_URL_INVALID", f"readback-{index}")
            continue
        readback_match = ISSUE_PATH_RE.fullmatch(parsed.path)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "github.com"
            or not readback_match
            or len(readback_match.group(3)) > MAX_ISSUE_NUMBER_DIGITS
            or parsed.query
            or parsed.fragment
            or parsed.username is not None
            or parsed.password is not None
        ):
            _add_issue(issues, "ISSUE_READBACK_URL_INVALID", f"readback-{index}")
            continue
        if url in by_url:
            _add_issue(issues, "ISSUE_READBACK_DUPLICATE", "issue-readbacks")
            continue
        by_url[url] = row
    for entry in entries:
        if entry.get("required_for_v1") is not True:
            continue
        key = _label(entry.get("key"))
        identity = _entry_identity(entry, key, issues)
        if identity is None:
            if entry.get("issue_url") is None:
                _add_issue(issues, "REQUIRED_ISSUE_URL_UNPROVEN", key)
            continue
        repo_full, url, number = identity
        row = by_url.get(url)
        if row is None:
            _add_issue(issues, "ISSUE_READBACK_NOT_FOUND", key)
            continue
        if row.get("repository") not in (entry.get("repository"), repo_full):
            _add_issue(issues, "ISSUE_READBACK_REPOSITORY_MISMATCH", key)
        if row.get("number") != number or isinstance(row.get("number"), bool):
            _add_issue(issues, "ISSUE_READBACK_NUMBER_MISMATCH", key)
        if not isinstance(row.get("title"), str) or not row.get("title").strip():
            _add_issue(issues, "ISSUE_READBACK_TITLE_INVALID", key)
        elif entry.get("issue_title") is not None and row.get("title") != entry.get("issue_title"):
            _add_issue(issues, "ISSUE_READBACK_TITLE_MISMATCH", key)
        if not isinstance(row.get("body_sha256"), str) or HASH_RE.fullmatch(row.get("body_sha256")) is None:
            _add_issue(issues, "ISSUE_READBACK_BODY_INVALID", key)
        elif entry.get("body_sha256") is not None and row.get("body_sha256") != entry.get("body_sha256"):
            _add_issue(issues, "ISSUE_READBACK_BODY_MISMATCH", key)
        if not isinstance(row.get("state"), str) or row.get("state") not in {"open", "closed"}:
            _add_issue(issues, "ISSUE_READBACK_STATE_INVALID", key)
        else:
            entry_state = entry.get("state")
            if entry_state == "closed" and row.get("state") != "closed":
                _add_issue(issues, "ISSUE_READBACK_STATE_MISMATCH", key)
            elif row.get("state") == "closed":
                if isinstance(entry_state, str) and entry_state in {
                    "planned", "in_progress", "blocked", "deferred", "not_planned", "not-planned"
                }:
                    _add_issue(issues, "ISSUE_READBACK_STATE_MISMATCH", key)
                if not _has_exact_closure_evidence(entry.get("closure_evidence"), _required_test_ids(entry)):
                    _add_issue(issues, "CLOSED_LEAF_WITHOUT_EXACT_EVIDENCE", key)


def _validate_map(
    coverage: dict[str, Any],
    pins: dict[str, Any],
    canonical: dict[str, Any],
    readbacks: dict[str, Any],
    map_pin: dict[str, Any],
    readback_pin: dict[str, Any] | None,
    *,
    check_urls: bool,
    check_dag: bool,
    require_specialists: bool,
) -> tuple[list[dict[str, str]], dict[str, int], dict[str, dict[str, Any]]]:
    issues: list[dict[str, str]] = []
    if coverage.get("format") != "agent-harness.coverage-map-current/v1":
        _add_issue(issues, "COVERAGE_MAP_FORMAT_UNSUPPORTED", "coverage-map")
    if not check_urls:
        _add_issue(issues, "REQUIRED_CHECK_NOT_RUN", "issue-readbacks")
    if not check_dag:
        _add_issue(issues, "REQUIRED_CHECK_NOT_RUN", "dag")
    _snapshot_state(coverage.get("coordination_status"), COORDINATION_STATES, "coordination_status", issues)
    _snapshot_state(coverage.get("publication_status"), PUBLICATION_STATES, "publication_status", issues)
    criteria = _canonical_criteria(canonical, issues)
    entries_raw = coverage.get("entries")
    contributions_raw = coverage.get("source_contribution_rows")
    ownership_raw = coverage.get("ownership_rows")
    if not isinstance(entries_raw, list):
        _add_issue(issues, "ENTRY_FAMILY_MISSING", "entries")
        entries_raw = []
    if not isinstance(contributions_raw, list):
        _add_issue(issues, "CONTRIBUTION_FAMILY_MISSING", "source_contribution_rows")
        contributions_raw = []
    if not isinstance(ownership_raw, list):
        _add_issue(issues, "OWNERSHIP_FAMILY_MISSING", "ownership_rows")
        ownership_raw = []
    entries_raw = _bounded_rows(entries_raw, MAX_ENTRIES, "ENTRY_ROW_LIMIT_EXCEEDED", "entries", issues)
    contributions_raw = _bounded_rows(
        contributions_raw, MAX_CONTRIBUTION_ROWS, "CONTRIBUTION_ROW_LIMIT_EXCEEDED", "source_contribution_rows", issues
    )
    ownership_raw = _bounded_rows(
        ownership_raw, MAX_OWNERSHIP_ROWS, "OWNERSHIP_ROW_LIMIT_EXCEEDED", "ownership_rows", issues
    )

    entries: list[dict[str, Any]] = []
    entry_by_key: dict[str, dict[str, Any]] = {}
    leaf_by_key: dict[str, dict[str, Any]] = {}
    repo_counts: dict[str, int] = {}
    for index, raw in enumerate(entries_raw):
        if not isinstance(raw, dict):
            _add_issue(issues, "ENTRY_INVALID", f"entry-{index}")
            continue
        key = raw.get("key")
        if not _safe_key(key):
            _add_issue(issues, "ENTRY_KEY_INVALID", f"entry-{index}")
            continue
        if key in entry_by_key:
            _add_issue(issues, "ENTRY_KEY_DUPLICATE", key)
            continue
        entry_by_key[key] = raw
        entries.append(raw)
        record_type = raw.get("record_type")
        if not isinstance(record_type, str):
            _add_issue(issues, "ENTRY_RECORD_TYPE_REQUIRED", key)
            record_type = ""
        elif record_type in LEAF_KINDS:
            leaf_by_key[key] = raw
        elif record_type in NON_LEAF_KINDS:
            pass
        else:
            _add_issue(issues, "ENTRY_RECORD_TYPE_UNSUPPORTED", key)
        required_for_v1 = raw.get("required_for_v1")
        if not isinstance(required_for_v1, bool):
            _add_issue(issues, "REQUIRED_LEAF_FLAG_MISSING", key)
        elif required_for_v1 and record_type not in LEAF_KINDS:
            _add_issue(issues, "NON_LEAF_MARKED_REQUIRED", key)
        if record_type in LEAF_KINDS and required_for_v1 is True:
            owner = raw.get("owner_role")
            if not isinstance(owner, str) or not owner.strip():
                _add_issue(issues, "ENTRY_OWNER_ROLE_MISSING", key)
            elif len(owner) > MAX_BLOCKER_TEXT:
                _add_issue(issues, "ENTRY_OWNER_ROLE_TOO_LONG", key)
            priority = raw.get("priority")
            if not isinstance(priority, str) or not priority.strip() or len(priority) > 32:
                _add_issue(issues, "ENTRY_PRIORITY_INVALID", key)
            issue_title = raw.get("issue_title")
            body_digest = raw.get("body_sha256")
            if raw.get("issue_url") is not None and (
                not isinstance(issue_title, str) or not issue_title.strip() or len(issue_title) > MAX_BLOCKER_TEXT
            ):
                _add_issue(issues, "ENTRY_ISSUE_TITLE_INVALID", key)
            if raw.get("issue_url") is not None and (
                not isinstance(body_digest, str) or HASH_RE.fullmatch(body_digest) is None
            ):
                _add_issue(issues, "ENTRY_BODY_DIGEST_INVALID", key)
            tests_raw = raw.get("required_tests", raw.get("tests", []))
            if not isinstance(tests_raw, list):
                _add_issue(issues, "ENTRY_REQUIRED_TESTS_INVALID", key)
            else:
                tests_raw = _bounded_rows(tests_raw, MAX_ENTRIES, "ENTRY_REQUIRED_TEST_LIMIT_EXCEEDED", key, issues)
                test_ids: list[str] = []
                for test in tests_raw:
                    candidate = test if isinstance(test, str) else test.get("id", test.get("command")) if isinstance(test, dict) else None
                    if not isinstance(candidate, str) or not _safe_key(candidate):
                        _add_issue(issues, "ENTRY_REQUIRED_TESTS_INVALID", key)
                    else:
                        test_ids.append(candidate)
                if len(test_ids) != len(set(test_ids)):
                    _add_issue(issues, "ENTRY_REQUIRED_TESTS_DUPLICATE", key)
                required_mode = raw.get("required_mode")
                if required_mode is not None and (not isinstance(required_mode, str) or required_mode not in MODES):
                    _add_issue(issues, "ENTRY_REQUIRED_MODE_INVALID", key)
            required_evidence = raw.get("required_evidence")
            if not isinstance(required_evidence, list) or any(not _safe_key(value) for value in required_evidence):
                _add_issue(issues, "ENTRY_REQUIRED_EVIDENCE_INVALID", key)
            elif len(required_evidence) != len(set(required_evidence)):
                _add_issue(issues, "ENTRY_REQUIRED_EVIDENCE_DUPLICATE", key)
            required_receipts = raw.get("required_owner_receipts", [])
            if not isinstance(required_receipts, list) or any(not _safe_key(value) for value in required_receipts):
                _add_issue(issues, "ENTRY_REQUIRED_RECEIPTS_INVALID", key)
            elif len(required_receipts) != len(set(required_receipts)):
                _add_issue(issues, "ENTRY_REQUIRED_RECEIPTS_DUPLICATE", key)
            for field in ("required_model_revision", "required_policy_revision"):
                if field in raw and not _safe_key(raw.get(field)):
                    _add_issue(issues, "ENTRY_REQUIRED_CONFIGURATION_PIN_INVALID", key)
            required_release_stage = raw.get("required_release_stage")
            if "required_release_stage" in raw and (
                not isinstance(required_release_stage, str) or required_release_stage not in STAGES
            ):
                _add_issue(issues, "ENTRY_REQUIRED_RELEASE_STAGE_INVALID", key)
        repo = raw.get("repository")
        if not isinstance(repo, str) or REPOSITORY_RE.fullmatch(repo) is None:
            _add_issue(issues, "ENTRY_REPOSITORY_INVALID", key)
        else:
            repo_counts[repo] = repo_counts.get(repo, 0) + int(raw.get("required_for_v1") is True)
        req_ids = raw.get("requirement_ids")
        if not isinstance(req_ids, list):
            _add_issue(issues, "ENTRY_REQUIREMENT_IDS_INVALID", key)
        else:
            req_ids = _bounded_rows(req_ids, MAX_ENTRIES, "ENTRY_REQUIREMENT_ID_LIMIT_EXCEEDED", key, issues)
            if any(not _safe_key(item) for item in req_ids):
                _add_issue(issues, "ENTRY_REQUIREMENT_IDS_INVALID", key)
            elif len(req_ids) != len(set(req_ids)):
                _add_issue(issues, "ENTRY_REQUIREMENT_ID_DUPLICATE", key)
        state = raw.get("state")
        allowed_states = {"planned", "in_progress", "blocked", "closed", "complete", "verified", "deferred", "not_planned", "not-planned"}
        if not isinstance(state, str) or state not in allowed_states:
            _add_issue(issues, "ENTRY_STATE_INVALID", key)
        elif required_for_v1 is True and record_type in LEAF_KINDS and state in {"deferred", "not_planned", "not-planned"}:
            _add_issue(issues, "REQUIRED_LEAF_NOT_DELIVERED", key)
        elif required_for_v1 is True and state == "closed" and record_type in LEAF_KINDS:
            evidence = raw.get("closure_evidence")
            if not _has_exact_closure_evidence(evidence, _required_test_ids(raw)):
                _add_issue(issues, "CLOSED_LEAF_WITHOUT_EXACT_EVIDENCE", key)
        supersedes = raw.get("supersedes")
        if supersedes is not None and (not isinstance(supersedes, str) or not _safe_key(supersedes)):
            _add_issue(issues, "SUPERSESSION_RELATION_INVALID", key)
        superseded_by = raw.get("superseded_by")
        if superseded_by is not None and (not isinstance(superseded_by, str) or not _safe_key(superseded_by)):
            _add_issue(issues, "SUPERSESSION_RELATION_INVALID", key)
        blockers = raw.get("blockers", [])
        if not isinstance(blockers, list) or len(blockers) > MAX_GRAPH_ROWS:
            _add_issue(issues, "ENTRY_BLOCKERS_INVALID", key)
        else:
            for blocker in blockers:
                if not isinstance(blocker, dict):
                    _add_issue(issues, "ENTRY_BLOCKER_INVALID", key)
                elif (
                    not isinstance(blocker.get("owner"), str)
                    or not blocker.get("owner").strip()
                    or not isinstance(blocker.get("action"), str)
                    or not blocker.get("action").strip()
                    or len(blocker.get("owner", "")) > MAX_BLOCKER_TEXT
                    or len(blocker.get("action", "")) > MAX_BLOCKER_TEXT
                ):
                    _add_issue(issues, "ENTRY_BLOCKER_OWNER_ACTION_REQUIRED", key)
        if required_for_v1 is True and raw.get("issue_url") is None and not blockers:
            _add_issue(issues, "REQUIRED_NULL_URL_BLOCKER_REQUIRED", key)
    if check_urls:
        _check_issue_readbacks(entries, readbacks, readback_pin, issues)

    if require_specialists:
        for family in ("agent-harness", "event-intelligence", "creative-engine"):
            if repo_counts.get(family, 0) < 1:
                _add_issue(issues, "REQUIRED_LEAF_FAMILY_MISSING", family)

    ownership_by_id: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(ownership_raw):
        if not isinstance(raw, dict):
            _add_issue(issues, "OWNERSHIP_ROW_INVALID", f"ownership-{index}")
            continue
        ident = raw.get("requirement_id")
        if not _safe_key(ident):
            _add_issue(issues, "OWNERSHIP_ID_INVALID", f"ownership-{index}")
            continue
        if ident in ownership_by_id:
            _add_issue(issues, "OWNERSHIP_ID_DUPLICATE", ident)
            continue
        ownership_by_id[ident] = raw
        accountable = raw.get("accountable_delivery_leaf")
        if not isinstance(accountable, str) or accountable not in leaf_by_key:
            _add_issue(issues, "ACCOUNTABLE_DELIVERY_LEAF_INVALID", ident)
        elif ident not in raw_by_list(leaf_by_key[accountable].get("requirement_ids")):
            _add_issue(issues, "ACCOUNTABLE_LEAF_REQUIREMENT_MISMATCH", ident)
        acceptance_owner = raw.get("acceptance_owner_leaf")
        if not isinstance(acceptance_owner, str) or acceptance_owner not in leaf_by_key:
            _add_issue(issues, "ACCEPTANCE_OWNER_LEAF_INVALID", ident)
        contributors = raw.get("required_contributors", [])
        if not isinstance(contributors, list):
            _add_issue(issues, "OWNERSHIP_CONTRIBUTOR_INVALID", ident)
        else:
            contributors = _bounded_rows(contributors, MAX_ENTRIES, "OWNERSHIP_CONTRIBUTOR_LIMIT_EXCEEDED", ident, issues)
            if any(not isinstance(item, str) or item not in leaf_by_key for item in contributors):
                _add_issue(issues, "OWNERSHIP_CONTRIBUTOR_INVALID", ident)
            elif len(contributors) != len(set(contributors)):
                _add_issue(issues, "OWNERSHIP_CONTRIBUTOR_DUPLICATE", ident)
            elif isinstance(acceptance_owner, str) and acceptance_owner not in contributors:
                _add_issue(issues, "ACCEPTANCE_OWNER_NOT_CONTRIBUTOR", ident)
        if ident in criteria:
            if raw.get("scope") != "scoped_contribution":
                _add_issue(issues, "GLOBAL_SCOPE_OVERCLAIM", ident)
            if isinstance(raw.get("global_status"), str) and raw.get("global_status") in {"complete", "accepted", "satisfied"}:
                _add_issue(issues, "GLOBAL_SCOPE_OVERCLAIM", ident)
        elif raw.get("scope") != "local" and raw.get("scope_and_global_role") != "Scoped requirement":
            _add_issue(issues, "LOCAL_SCOPE_CLASSIFICATION_INVALID", ident)

    contribution_rows: list[dict[str, Any]] = []
    contributed_pairs: dict[str, set[str]] = {}
    seen_row_ids: set[str] = set()
    for index, raw in enumerate(contributions_raw):
        if not isinstance(raw, dict):
            _add_issue(issues, "CONTRIBUTION_ROW_INVALID", f"contribution-{index}")
            continue
        contribution_rows.append(raw)
        row_id = raw.get("row_id")
        if not _safe_key(row_id):
            _add_issue(issues, "CONTRIBUTION_ROW_ID_INVALID", f"contribution-{index}")
        elif row_id in seen_row_ids:
            _add_issue(issues, "CONTRIBUTION_ROW_ID_DUPLICATE", row_id)
        else:
            seen_row_ids.add(row_id)
        ident = raw.get("requirement_id")
        if not _safe_key(ident) or ident not in ownership_by_id:
            _add_issue(issues, "ORPHAN_CONTRIBUTION_REQUIREMENT", ident)
            continue
        leaf = raw.get("owning_leaf_key")
        if not isinstance(leaf, str) or leaf not in leaf_by_key:
            _add_issue(issues, "CONTRIBUTION_OWNER_NOT_LEAF", ident)
        else:
            leaf_declares_requirement = ident in raw_by_list(entry_by_key[leaf].get("requirement_ids"))
            if not leaf_declares_requirement:
                _add_issue(issues, "CONTRIBUTION_NOT_DECLARED_BY_LEAF", ident)
            required_contributors = ownership_by_id[ident].get("required_contributors", [])
            if not isinstance(required_contributors, list) or leaf not in required_contributors:
                _add_issue(issues, "CONTRIBUTION_OWNER_NOT_DECLARED", ident)
            elif leaf_declares_requirement:
                contributed_pairs.setdefault(ident, set()).add(leaf)
        if ident in criteria:
            acceptance = raw.get("authoritative_acceptance")
            if not isinstance(acceptance, dict):
                _add_issue(issues, "GLOBAL_ACCEPTANCE_METADATA_MISSING", ident)
            else:
                canonical_item = criteria[ident]
                for field in ("id", "title", "priority", "owner_role", "verification", "evidence_required"):
                    if acceptance.get(field) != canonical_item.get(field):
                        _add_issue(issues, "GLOBAL_ACCEPTANCE_METADATA_MISMATCH", ident)
                        break
        expected_scope = "scoped_contribution" if ident in criteria else "local"
        if raw.get("scope") != expected_scope and not (
            expected_scope == "local" and raw.get("scope_and_global_role") == "Scoped requirement"
        ):
            _add_issue(issues, "CONTRIBUTION_SCOPE_MISMATCH", row_id if _safe_key(row_id) else ident)
        ownership_scope = ownership_by_id[ident].get("scope")
        if ownership_scope != expected_scope and not (
            expected_scope == "local" and ownership_by_id[ident].get("scope_and_global_role") == "Scoped requirement"
        ):
            _add_issue(issues, "OWNERSHIP_SCOPE_MISMATCH", ident)

    contributed_ids = {
        row.get("requirement_id")
        for row in contribution_rows
        if isinstance(row.get("requirement_id"), str)
    }
    for ident in sorted(set(ownership_by_id) - contributed_ids):
        _add_issue(issues, "OWNERSHIP_WITHOUT_CONTRIBUTION", ident)
    for ident, ownership in ownership_by_id.items():
        required_contributors = ownership.get("required_contributors")
        if not isinstance(required_contributors, list) or any(
            not isinstance(contributor, str) or contributor not in leaf_by_key
            for contributor in required_contributors
        ):
            continue
        actual_contributors = contributed_pairs.get(ident, set())
        for contributor in required_contributors:
            if contributor not in actual_contributors:
                _add_relation_issue(
                    issues, "REQUIRED_CONTRIBUTOR_MISSING", ident, contributor
                )

    for entry in entries:
        supersedes = entry.get("supersedes")
        if isinstance(supersedes, str):
            prior = entry_by_key.get(supersedes)
            if prior is None or prior.get("superseded_by") != entry.get("key"):
                _add_issue(issues, "SUPERSESSION_RELATION_UNVERIFIED", entry.get("key"))
        superseded_by = entry.get("superseded_by")
        if superseded_by is not None:
            replacement = entry_by_key.get(superseded_by) if isinstance(superseded_by, str) else None
            if replacement is None or replacement.get("supersedes") != entry.get("key"):
                _add_issue(issues, "SUPERSESSION_RELATION_UNVERIFIED", entry.get("key"))

    all_entry_requirement_ids: set[str] = set()
    for entry in entries:
        req_ids = entry.get("requirement_ids")
        if isinstance(req_ids, list):
            all_entry_requirement_ids.update(item for item in req_ids if isinstance(item, str))
    for ident in sorted(all_entry_requirement_ids - set(ownership_by_id)):
        _add_issue(issues, "REQUIRED_ID_WITHOUT_OWNERSHIP", ident)

    if check_dag:
        _validate_graph(coverage.get("dag"), coverage.get("staged_edges"), entry_by_key, leaf_by_key, issues)
    staged_edges_for_ownership = coverage.get("staged_edges")
    if isinstance(staged_edges_for_ownership, list):
        for index, edge in enumerate(staged_edges_for_ownership):
            if not isinstance(edge, dict) or edge.get("required_for_v1") is not True:
                continue
            edge_id = edge.get("edge_id")
            requirement_ids = edge.get("requirement_ids")
            if not isinstance(requirement_ids, list):
                continue
            for requirement_id in requirement_ids:
                if isinstance(requirement_id, str) and requirement_id not in ownership_by_id:
                    _add_issue(issues, "STAGED_EDGE_REQUIREMENT_UNKNOWN", edge_id or f"stage-{index}")

    if coverage.get("complete") is not True or map_pin.get("complete") is not True:
        _add_issue(issues, "COVERAGE_SNAPSHOT_INCOMPLETE", "coverage-map")

    global_ids = set(criteria)
    mapped_global = set(ownership_by_id) & global_ids
    local_ids = set(ownership_by_id) - global_ids
    row_global = sum(
        1
        for row in contribution_rows
        if isinstance(row.get("requirement_id"), str) and row.get("requirement_id") in global_ids
    )
    row_local = len(contribution_rows) - row_global
    dimensions = {
        "required_leaf_records": sum(1 for item in entries if item.get("required_for_v1") is True),
        "agent_harness_leaf_records": repo_counts.get("agent-harness", 0),
        "specialist_leaf_records": sum(count for repo, count in repo_counts.items() if repo != "agent-harness"),
        "source_contribution_rows": len(contribution_rows),
        "unique_mapped_requirement_ids": len(ownership_by_id),
        "local_scoped_requirement_ids": len(local_ids),
        "global_ids_with_scoped_contributions": len(mapped_global),
        "global_criteria_total": len(criteria),
        "global_ids_outside_scoped_contribution": len(global_ids - mapped_global),
        "contribution_rows_to_global_ids": row_global,
        "contribution_rows_to_local_ids": row_local,
    }
    expected_dimensions = map_pin.get("observed_dimensions")
    if not isinstance(expected_dimensions, dict):
        _add_issue(issues, "DIMENSION_PINS_MISSING", "coverage-map")
    else:
        for name, actual in dimensions.items():
            expected = expected_dimensions.get(name)
            if not isinstance(expected, int) or isinstance(expected, bool) or expected != actual:
                _add_issue(issues, "DIMENSION_PIN_MISMATCH", name)
    if len(criteria) == 0:
        _add_issue(issues, "CANONICAL_CRITERIA_EMPTY", "canonical-checklist")
    return issues, dimensions, leaf_by_key


def raw_by_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _has_exact_closure_evidence(value: Any, required_tests: list[str]) -> bool:
    if not isinstance(value, list) or not value:
        return False
    return any(
        isinstance(item, dict)
        and isinstance(item.get("commit", item.get("revision")), str)
        and re.fullmatch(r"[0-9a-f]{40}", item.get("commit", item.get("revision"))) is not None
        and isinstance(item.get("tests"), list)
        and bool(item.get("tests"))
        and all(_safe_key(test_id) for test_id in item.get("tests", []))
        and bool(required_tests)
        and set(item.get("tests", [])) == set(required_tests)
        for item in value
    )


def _validate_graph(
    dag: Any,
    staged_edges: Any,
    entry_by_key: dict[str, dict[str, Any]],
    leaf_by_key: dict[str, dict[str, Any]],
    issues: list[dict[str, str]],
) -> None:
    if not isinstance(dag, dict) or not isinstance(dag.get("edges"), dict):
        _add_issue(issues, "EXECUTION_DAG_MISSING", "dag")
        return
    edges: dict[str, set[str]] = {}
    all_nodes = set(entry_by_key)
    edge_items = list(dag["edges"].items())
    if len(edge_items) > MAX_GRAPH_ROWS:
        _add_issue(issues, "EXECUTION_EDGE_ROW_LIMIT_EXCEEDED", "dag")
        edge_items = edge_items[:MAX_GRAPH_ROWS]
    for consumer, prerequisites in edge_items:
        if not isinstance(consumer, str) or consumer not in leaf_by_key:
            _add_issue(issues, "EXECUTION_EDGE_CONSUMER_NOT_LEAF", consumer)
            continue
        if not isinstance(prerequisites, list):
            _add_issue(issues, "EXECUTION_EDGE_LIST_INVALID", consumer)
            continue
        normalized: set[str] = set()
        prerequisites = _bounded_rows(prerequisites, MAX_ENTRIES, "EXECUTION_PREREQUISITE_LIMIT_EXCEEDED", consumer, issues)
        for prerequisite in prerequisites:
            if not isinstance(prerequisite, str) or prerequisite not in all_nodes:
                _add_issue(issues, "EXECUTION_EDGE_UNKNOWN_NODE", consumer)
                continue
            if prerequisite == consumer:
                _add_issue(issues, "EXECUTION_EDGE_SELF", consumer)
            if prerequisite not in leaf_by_key:
                _add_issue(issues, "EXECUTION_EDGE_PREREQUISITE_NOT_LEAF", consumer)
            if prerequisite in normalized:
                _add_issue(issues, "EXECUTION_EDGE_DUPLICATE", consumer)
            normalized.add(prerequisite)
        edges[consumer] = normalized
    for node in leaf_by_key:
        edges.setdefault(node, set())
    _check_cycle(edges, issues)
    fan_in = dag.get("required_gate_fan_in", {})
    if not isinstance(fan_in, dict):
        _add_issue(issues, "GATE_FAN_IN_INVALID", "dag")
    else:
        observed_fan_in = dag.get("gate_fan_in", {})
        if not isinstance(observed_fan_in, dict):
            _add_issue(issues, "GATE_FAN_IN_INVALID", "dag")
            observed_fan_in = {}
        if len(fan_in) > MAX_GRAPH_ROWS or len(observed_fan_in) > MAX_GRAPH_ROWS:
            _add_issue(issues, "GATE_FAN_IN_ROW_LIMIT_EXCEEDED", "dag")
        if set(observed_fan_in) != set(fan_in):
            _add_issue(issues, "GATE_FAN_IN_MISMATCH", "dag")
        for gate, expected in list(fan_in.items())[:MAX_GRAPH_ROWS]:
            if not isinstance(gate, str) or not isinstance(expected, list):
                _add_issue(issues, "GATE_FAN_IN_INVALID", gate)
                continue
            expected = _bounded_rows(expected, MAX_ENTRIES, "GATE_FAN_IN_LIMIT_EXCEEDED", gate, issues)
            gate_record = entry_by_key.get(gate)
            observed = observed_fan_in.get(gate)
            if isinstance(observed, list):
                observed = _bounded_rows(observed, MAX_ENTRIES, "GATE_FAN_IN_LIMIT_EXCEEDED", gate, issues)
            expected_set = set(item for item in expected if isinstance(item, str))
            observed_set = set(item for item in observed if isinstance(item, str)) if isinstance(observed, list) else None
            if (
                gate_record is None
                or gate_record.get("record_type") not in NON_LEAF_KINDS
                or observed_set is None
                or len(expected_set) != len(expected)
                or len(observed_set) != len(observed)
                or observed_set != expected_set
                or any(item not in leaf_by_key for item in expected_set)
            ):
                _add_issue(issues, "GATE_FAN_IN_MISMATCH", gate)
    if not isinstance(staged_edges, list):
        _add_issue(issues, "STAGED_EDGE_FAMILY_MISSING", "staged_edges")
        return
    staged_edges = _bounded_rows(staged_edges, MAX_GRAPH_ROWS, "STAGED_EDGE_ROW_LIMIT_EXCEEDED", "staged_edges", issues)
    seen_stage_edge_ids: set[str] = set()
    for index, edge in enumerate(staged_edges):
        record = f"stage-{index}"
        if not isinstance(edge, dict):
            _add_issue(issues, "STAGED_EDGE_INVALID", record)
            continue
        edge_id = edge.get("edge_id")
        if not _safe_key(edge_id):
            _add_issue(issues, "STAGED_EDGE_ID_INVALID", record)
        elif edge_id in seen_stage_edge_ids:
            _add_issue(issues, "STAGED_EDGE_ID_DUPLICATE", edge_id)
        else:
            seen_stage_edge_ids.add(edge_id)
        consumer = edge.get("consumer_key")
        prerequisite = edge.get("prerequisite_key")
        if (
            not isinstance(consumer, str)
            or not isinstance(prerequisite, str)
            or consumer not in all_nodes
            or prerequisite not in all_nodes
        ):
            _add_issue(issues, "STAGED_EDGE_UNKNOWN_NODE", record)
        for field in ("consumer_stage", "prerequisite_stage"):
            if not isinstance(edge.get(field), str) or edge.get(field) not in STAGES:
                _add_issue(issues, "STAGED_EDGE_FIELD_INVALID", record)
        if not isinstance(edge.get("status"), str) or edge.get("status") not in STAGED_EDGE_STATES:
            _add_issue(issues, "STAGED_EDGE_STATUS_INVALID", record)
        required_for_v1 = edge.get("required_for_v1")
        if not isinstance(required_for_v1, bool):
            _add_issue(issues, "STAGED_EDGE_REQUIRED_FLAG_INVALID", record)
        requirement_ids = edge.get("requirement_ids")
        if (
            not isinstance(requirement_ids, list)
            or any(not _safe_key(value) for value in requirement_ids)
        ):
            _add_issue(issues, "STAGED_EDGE_REQUIREMENT_IDS_INVALID", record)
        elif len(requirement_ids) != len(set(requirement_ids)):
            _add_issue(issues, "STAGED_EDGE_REQUIREMENT_IDS_DUPLICATE", record)
        elif required_for_v1 is True:
            if not requirement_ids:
                _add_issue(issues, "STAGED_EDGE_REQUIREMENT_IDS_REQUIRED", record)
            safe_edge_id = edge_id if _safe_key(edge_id) else record
            for endpoint_role, endpoint in (("consumer", consumer), ("prerequisite", prerequisite)):
                endpoint_leaf = leaf_by_key.get(endpoint) if isinstance(endpoint, str) else None
                endpoint_requirement_ids = endpoint_leaf.get("requirement_ids") if endpoint_leaf is not None else None
                for requirement_id in requirement_ids:
                    if (
                        endpoint_leaf is None
                        or not isinstance(endpoint_requirement_ids, list)
                        or requirement_id not in endpoint_requirement_ids
                    ):
                        code = (
                            "STAGED_EDGE_CONSUMER_REQUIREMENT_NOT_DECLARED"
                            if endpoint_role == "consumer"
                            else "STAGED_EDGE_PREREQUISITE_REQUIREMENT_NOT_DECLARED"
                        )
                        _add_relation_issue(issues, code, safe_edge_id, requirement_id)
        required_evidence = edge.get("required_evidence_ids", [])
        if (
            not isinstance(required_evidence, list)
            or any(not _safe_key(value) for value in required_evidence)
        ):
            _add_issue(issues, "STAGED_EDGE_EVIDENCE_IDS_INVALID", record)
        elif len(required_evidence) != len(set(required_evidence)):
            _add_issue(issues, "STAGED_EDGE_EVIDENCE_IDS_DUPLICATE", record)
        elif edge.get("status") == "satisfied" and not required_evidence:
            _add_issue(issues, "STAGED_EDGE_EVIDENCE_REQUIRED", record)
        required_receipts = edge.get("required_owner_receipt_ids", [])
        if (
            not isinstance(required_receipts, list)
            or any(not _safe_key(value) for value in required_receipts)
        ):
            _add_issue(issues, "STAGED_EDGE_RECEIPT_IDS_INVALID", record)
        elif len(required_receipts) != len(set(required_receipts)):
            _add_issue(issues, "STAGED_EDGE_RECEIPT_IDS_DUPLICATE", record)
        if edge.get("edge_type") != "evidence_stage":
            _add_issue(issues, "STAGED_EDGE_TYPE_INVALID", record)


def _check_cycle(edges: dict[str, set[str]], issues: list[dict[str, str]]) -> None:
    # Edges point from a consumer to its prerequisites. Kahn's algorithm is
    # iterative and uses the reverse graph so input size does not affect recursion.
    dependents: dict[str, set[str]] = {node: set() for node in edges}
    remaining: dict[str, int] = {}
    for consumer, prerequisites in edges.items():
        remaining[consumer] = len(prerequisites)
        for prerequisite in prerequisites:
            dependents.setdefault(prerequisite, set()).add(consumer)
    ready = deque(sorted(node for node, degree in remaining.items() if degree == 0))
    visited = 0
    while ready:
        node = ready.popleft()
        visited += 1
        for dependent in dependents.get(node, ()):
            remaining[dependent] -= 1
            if remaining[dependent] == 0:
                ready.append(dependent)
    if visited != len(remaining):
        cycle_nodes = sorted(node for node, degree in remaining.items() if degree > 0)
        _add_issue(issues, "EXECUTION_DAG_CYCLE", cycle_nodes[0] if cycle_nodes else "dag")


def _validate_release(
    release_path: str | Path,
    leaves: dict[str, dict[str, Any]],
    dag: Any,
    required_gate_fan_in: Any,
    staged_edges: Any,
    issues: list[dict[str, str]],
    *,
    expected_format: str = "acv1.release-manifest/v1",
    projection_relations: list[dict[str, Any]] | None = None,
    external_artifacts: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    manifest, manifest_raw = _read_json(release_path, "release-manifest")
    allowed_stages = PROJECTION_STAGES if expected_format == "acv1.release-manifest/v2" else STAGES
    if manifest.get("format") != expected_format:
        _add_issue(issues, "RELEASE_FORMAT_UNSUPPORTED", "release-manifest")
    revisions_raw = manifest.get("tested_revisions")
    evidence_raw = manifest.get("evidence")
    outcomes_raw = manifest.get("outcomes")
    receipts_raw = manifest.get("owner_receipts", [])
    if not isinstance(revisions_raw, list) or not isinstance(evidence_raw, list) or not isinstance(outcomes_raw, list):
        _add_issue(issues, "RELEASE_REQUIRED_FAMILY_MISSING", "release-manifest")
        return {
            "tested_revisions": 0,
            "evidence_files": 0,
            "evidence_bytes_read": 0,
            "verified_evidence_bytes": 0,
            "outcomes": 0,
            "manifest_sha256": _sha256(manifest_raw),
        }
    if not isinstance(receipts_raw, list):
        _add_issue(issues, "RELEASE_RECEIPTS_INVALID", "release-manifest")
        receipts_raw = []
    revisions_raw = _bounded_rows(revisions_raw, MAX_RELEASE_RECORDS, "RELEASE_ROW_LIMIT_EXCEEDED", "tested_revisions", issues)
    evidence_raw = _bounded_rows(evidence_raw, MAX_RELEASE_RECORDS, "RELEASE_ROW_LIMIT_EXCEEDED", "evidence", issues)
    outcomes_raw = _bounded_rows(outcomes_raw, MAX_ENTRIES, "RELEASE_ROW_LIMIT_EXCEEDED", "outcomes", issues)
    receipts_raw = _bounded_rows(receipts_raw, MAX_RELEASE_RECORDS, "RELEASE_ROW_LIMIT_EXCEEDED", "owner_receipts", issues)
    tested: set[tuple[str, str]] = set()
    for index, item in enumerate(revisions_raw):
        if not isinstance(item, dict):
            _add_issue(issues, "TESTED_REVISION_INVALID", f"revision-{index}")
            continue
        repo, revision = item.get("repository"), item.get("revision")
        if (
            not isinstance(repo, str)
            or REPOSITORY_RE.fullmatch(repo) is None
            or not isinstance(revision, str)
            or re.fullmatch(r"[0-9a-f]{40}", revision) is None
        ):
            _add_issue(issues, "TESTED_REVISION_INVALID", f"revision-{index}")
            continue
        pair = (repo, revision)
        if pair in tested:
            _add_issue(issues, "TESTED_REVISION_DUPLICATE", repo)
        tested.add(pair)

    evidence_by_id: dict[str, dict[str, Any]] = {}
    evidence_bytes_read = 0
    verified_evidence_bytes = 0
    for index, item in enumerate(evidence_raw):
        record = f"evidence-{index}"
        if not isinstance(item, dict):
            _add_issue(issues, "EVIDENCE_RECORD_INVALID", record)
            continue
        ident = item.get("id")
        if not _safe_key(ident):
            _add_issue(issues, "EVIDENCE_ID_INVALID", record)
            continue
        if ident in evidence_by_id:
            _add_issue(issues, "EVIDENCE_ID_DUPLICATE", ident)
            continue
        evidence_by_id[ident] = item
        if not isinstance(item.get("mode"), str) or item.get("mode") not in MODES:
            _add_issue(issues, "EVIDENCE_MODE_INVALID", ident)
        for field in ("producer", "source_revision", "kind"):
            if not isinstance(item.get(field), str) or not item.get(field).strip() or len(item.get(field)) > MAX_BLOCKER_TEXT:
                _add_issue(issues, "EVIDENCE_METADATA_MISSING", ident)
        if isinstance(item.get("kind"), str) and item.get("kind") not in EVIDENCE_KINDS:
            _add_issue(issues, "EVIDENCE_KIND_UNSUPPORTED", ident)
        if not isinstance(item.get("source_revision"), str) or re.fullmatch(r"[0-9a-f]{40}", item.get("source_revision", "")) is None:
            _add_issue(issues, "EVIDENCE_REVISION_INVALID", ident)
        try:
            _iso_utc(item.get("captured_at"), "EVIDENCE_TIME_INVALID", ident)
        except InputError as exc:
            _add_issue(issues, exc.code, exc.record)
        expected_hash = item.get("sha256")
        if not isinstance(expected_hash, str) or HASH_RE.fullmatch(expected_hash) is None:
            _add_issue(issues, "EVIDENCE_DIGEST_INVALID", ident)
            continue
        if evidence_bytes_read > LIMITS["evidence-total"]:
            _add_issue(issues, "EVIDENCE_TOTAL_SIZE", ident)
            continue
        artifact_path = _relative_artifact(item.get("path"), ident)
        try:
            flags = os.O_RDONLY
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            if hasattr(os, "O_NONBLOCK"):
                flags |= os.O_NONBLOCK
            fd = os.open(os.fspath(artifact_path), flags)
        except OSError:
            _add_issue(issues, "EVIDENCE_UNREADABLE", ident)
            continue
        digest = hashlib.sha256()
        size = 0
        readable = True
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size > LIMITS["evidence-file"]:
                _add_issue(issues, "EVIDENCE_TYPE_OR_SIZE", ident)
                readable = False
            elif evidence_bytes_read + info.st_size > LIMITS["evidence-total"]:
                _add_issue(issues, "EVIDENCE_TOTAL_SIZE", ident)
                readable = False
            while readable:
                remaining_file = LIMITS["evidence-file"] - size
                remaining_total = LIMITS["evidence-total"] - evidence_bytes_read
                read_size = min(64 * 1024, max(1, remaining_file + 1), max(1, remaining_total + 1))
                chunk = os.read(fd, read_size)
                if not chunk:
                    break
                size += len(chunk)
                evidence_bytes_read += len(chunk)
                if size > LIMITS["evidence-file"]:
                    _add_issue(issues, "EVIDENCE_TYPE_OR_SIZE", ident)
                    readable = False
                    break
                if evidence_bytes_read > LIMITS["evidence-total"]:
                    _add_issue(issues, "EVIDENCE_TOTAL_SIZE", ident)
                    readable = False
                    break
                digest.update(chunk)
            if readable and (size != info.st_size or digest.hexdigest() != expected_hash):
                _add_issue(issues, "EVIDENCE_DIGEST_MISMATCH", ident)
                readable = False
        except OSError:
            _add_issue(issues, "EVIDENCE_READ_FAILED", ident)
            readable = False
        finally:
            try:
                os.close(fd)
            except OSError:
                _add_issue(issues, "EVIDENCE_CLOSE_FAILED", ident)
                readable = False
        if readable:
            verified_evidence_bytes += size

    receipt_by_id: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(receipts_raw):
        if not isinstance(item, dict) or not _safe_key(item.get("id")):
            _add_issue(issues, "OWNER_RECEIPT_INVALID", f"receipt-{index}")
            continue
        if item["id"] in receipt_by_id:
            _add_issue(issues, "OWNER_RECEIPT_DUPLICATE", item["id"])
            continue
        receipt_by_id[item["id"]] = item

    # An internal relation can select a separately versioned prerequisite
    # artifact. Its declared owner receipt is revision-scoped to that exact
    # artifact, while the producer outcome can remain at an earlier revision.
    # Only fully bound relation receipts receive this narrow exception from the
    # ordinary outcome revision checks below.
    relation_outcomes: dict[str, dict[str, Any]] = {}
    for item in outcomes_raw:
        if not isinstance(item, dict):
            continue
        candidate_leaf = item.get("leaf_key")
        if _safe_key(candidate_leaf) and candidate_leaf in leaves:
            relation_outcomes.setdefault(candidate_leaf, item)
    relation_receipt_contexts, relation_receipt_evidence_contexts = _validated_internal_relation_receipt_contexts(
        manifest, projection_relations, evidence_by_id, relation_outcomes,
        leaves, receipt_by_id, tested, staged_edges,
    )

    outcomes: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(outcomes_raw):
        record = f"outcome-{index}"
        if not isinstance(item, dict):
            _add_issue(issues, "OUTCOME_INVALID", record)
            continue
        leaf = item.get("leaf_key")
        if not _safe_key(leaf) or leaf not in leaves:
            _add_issue(issues, "OUTCOME_UNKNOWN_LEAF", leaf)
            continue
        if leaf in outcomes:
            _add_issue(issues, "OUTCOME_DUPLICATE", leaf)
            continue
        outcomes[leaf] = item
        if item.get("status") != "verified":
            _add_issue(issues, "REQUIRED_OUTCOME_NOT_VERIFIED", leaf)
        required_stage = leaves[leaf].get("required_release_stage")
        evaluated_stage = item.get("evaluated_stage")
        if not isinstance(required_stage, str) or required_stage not in allowed_stages:
            _add_issue(issues, "REQUIRED_RELEASE_STAGE_MISSING", leaf)
        if not isinstance(evaluated_stage, str) or evaluated_stage not in allowed_stages:
            _add_issue(issues, "OUTCOME_EVALUATED_STAGE_INVALID", leaf)
        elif isinstance(required_stage, str) and required_stage in allowed_stages and evaluated_stage != required_stage:
            _add_issue(issues, "OUTCOME_EVALUATED_STAGE_MISMATCH", leaf)
        revision = item.get("revision")
        repo = leaves[leaf].get("repository")
        if not isinstance(revision, str) or re.fullmatch(r"[0-9a-f]{40}", revision) is None or (repo, revision) not in tested:
            _add_issue(issues, "OUTCOME_REVISION_UNTESTED", leaf)
        if item.get("prerequisites_satisfied") is not True:
            _add_issue(issues, "OUTCOME_PREREQUISITE_UNSATISFIED", leaf)
        required_tests = _required_test_ids(
            leaves[leaf], fallback_to_legacy_tests=expected_format == "acv1.release-manifest/v1"
        )
        required_evidence = leaves[leaf].get("required_evidence", [])
        if not isinstance(required_evidence, list) or any(not isinstance(value, str) for value in required_evidence):
            _add_issue(issues, "REQUIRED_EVIDENCE_LIST_INVALID", leaf)
            required_evidence = []
        provided_tests = item.get("required_test_ids")
        evidence_ids = item.get("evidence_ids")
        if not isinstance(provided_tests, list) or not set(required_tests).issubset(set(x for x in provided_tests if isinstance(x, str))):
            _add_issue(issues, "REQUIRED_TEST_MISSING", leaf)
        elif len([value for value in provided_tests if isinstance(value, str)]) != len(set(value for value in provided_tests if isinstance(value, str))):
            _add_issue(issues, "OUTCOME_TEST_ID_DUPLICATE", leaf)
        if not isinstance(evidence_ids, list) or not evidence_ids:
            _add_issue(issues, "REQUIRED_EVIDENCE_MISSING", leaf)
            evidence_ids = []
        if len([value for value in evidence_ids if isinstance(value, str)]) != len(set(value for value in evidence_ids if isinstance(value, str))):
            _add_issue(issues, "OUTCOME_EVIDENCE_ID_DUPLICATE", leaf)
        if not set(required_evidence).issubset(set(value for value in evidence_ids if isinstance(value, str))):
            _add_issue(issues, "REQUIRED_EVIDENCE_MISSING", leaf)
        required_mode = item.get("required_mode")
        if not isinstance(required_mode, str) or required_mode not in MODES:
            _add_issue(issues, "REQUIRED_MODE_REQUIRED_OR_INVALID", leaf)
        entry_mode = leaves[leaf].get("required_mode")
        if entry_mode is not None and required_mode != entry_mode:
            _add_issue(issues, "REQUIRED_MODE_MISMATCH", leaf)
        required_configuration: dict[str, Any] = {}
        for pin_field in ("model_revision", "policy_revision"):
            expected_pin = leaves[leaf].get("required_" + pin_field)
            if expected_pin is not None:
                required_configuration[pin_field] = expected_pin
                if item.get("required_" + pin_field) != expected_pin:
                    _add_issue(issues, "REQUIRED_CONFIGURATION_PIN_MISMATCH", leaf)
            elif "required_" + pin_field in item:
                _add_issue(issues, "OUTCOME_CONFIGURATION_PIN_UNDECLARED", leaf)
        leaf_state = leaves[leaf].get("state")
        if leaves[leaf].get("blockers") or (
            isinstance(leaf_state, str) and leaf_state in {"blocked", "deferred", "not_planned", "not-planned"}
        ):
            _add_issue(issues, "REQUIRED_OUTCOME_BLOCKED", leaf)
        passed_tests: set[str] = set()
        if isinstance(evidence_ids, list):
            for evidence_id in evidence_ids:
                evidence = evidence_by_id.get(evidence_id) if isinstance(evidence_id, str) else None
                if evidence is None:
                    _add_issue(issues, "OUTCOME_EVIDENCE_UNKNOWN", leaf)
                    continue
                relation_receipt_context = (
                    relation_receipt_evidence_contexts.get((leaf, evidence_id))
                    if isinstance(evidence_id, str) else None
                )
                expected_evidence_revision = (
                    relation_receipt_context["source_revision"]
                    if relation_receipt_context is not None else revision
                )
                expected_evidence_mode = (
                    relation_receipt_context["mode"]
                    if relation_receipt_context is not None else required_mode
                )
                if evidence.get("source_revision") != expected_evidence_revision:
                    _add_issue(issues, "EVIDENCE_REVISION_MISMATCH", leaf)
                if evidence.get("mode") != expected_evidence_mode:
                    _add_issue(issues, "EVIDENCE_MODE_MISMATCH", leaf)
                if evidence.get("kind") == "test":
                    test_id = evidence.get("test_id")
                    if test_id in required_tests and evidence.get("result") != "passed":
                        _add_issue(issues, "REQUIRED_TEST_NOT_PASSED", leaf)
                    elif test_id in required_tests:
                        passed_tests.add(test_id)
                if isinstance(evidence.get("kind"), str) and evidence.get("kind") in {"test", "runtime", "configuration"}:
                    for pin_field, required_pin in required_configuration.items():
                        if required_pin is not None and evidence.get(pin_field) != required_pin:
                            _add_issue(issues, "EVIDENCE_CONFIGURATION_PIN_MISMATCH", leaf)
        for test_id in required_tests:
            if test_id not in passed_tests:
                _add_issue(issues, "REQUIRED_TEST_EVIDENCE_MISSING", leaf)
        required_receipts = item.get("required_owner_receipts", [])
        provided_receipts = item.get("owner_receipt_ids", [])
        if not isinstance(required_receipts, list) or not isinstance(provided_receipts, list):
            _add_issue(issues, "OUTCOME_RECEIPT_LIST_INVALID", leaf)
            required_receipts, provided_receipts = [], []
        leaf_required_receipts = leaves[leaf].get("required_owner_receipts", [])
        leaf_required_set = (
            set(value for value in leaf_required_receipts if isinstance(value, str))
            if isinstance(leaf_required_receipts, list)
            else set()
        )
        if isinstance(staged_edges, list):
            for edge in staged_edges:
                if (
                    not isinstance(edge, dict)
                    or edge.get("required_for_v1") is not True
                    or edge.get("prerequisite_key") != leaf
                ):
                    continue
                consumer_key = edge.get("consumer_key")
                consumer_leaf = leaves.get(consumer_key) if isinstance(consumer_key, str) else None
                if (
                    consumer_leaf is not None
                    and edge.get("consumer_stage") == consumer_leaf.get("required_release_stage")
                ):
                    edge_receipts = edge.get("required_owner_receipt_ids")
                    if isinstance(edge_receipts, list):
                        leaf_required_set.update(value for value in edge_receipts if isinstance(value, str))
        if projection_relations is not None:
            for relation in projection_relations:
                if (
                    not isinstance(relation, dict)
                    or relation.get("applicability") != "required"
                ):
                    continue
                consumer_key = relation.get("consumer_key")
                consumer_leaf = leaves.get(consumer_key) if isinstance(consumer_key, str) else None
                consumer_stage = relation.get("consumer_stage")
                selected_stage = consumer_leaf.get("required_release_stage") if isinstance(consumer_leaf, dict) else None
                if (
                    consumer_leaf is None
                    or consumer_stage not in PROJECTION_STAGE_ORDER
                    or selected_stage not in PROJECTION_STAGE_ORDER
                    or PROJECTION_STAGE_ORDER[consumer_stage] > PROJECTION_STAGE_ORDER[selected_stage]
                    or (relation.get("application_scope") == "live_only" and consumer_leaf.get("required_mode") != "live")
                ):
                    continue
                external_id = relation.get("external_artifact_id")
                receipt_owner = consumer_key if external_id is not None else relation.get("producer_key")
                if receipt_owner == leaf:
                    relation_receipts = relation.get("required_owner_receipt_ids", [])
                    if isinstance(relation_receipts, list):
                        leaf_required_set.update(value for value in relation_receipts if isinstance(value, str))
        if set(value for value in required_receipts if isinstance(value, str)) != leaf_required_set:
            _add_issue(issues, "REQUIRED_OWNER_RECEIPTS_MISMATCH", leaf)
        provided_receipt_set = set(value for value in provided_receipts if isinstance(value, str))
        if provided_receipt_set != set(value for value in required_receipts if isinstance(value, str)):
            _add_issue(issues, "OWNER_RECEIPT_IDS_MISMATCH", leaf)
        for receipt_id in required_receipts:
            receipt = receipt_by_id.get(receipt_id) if isinstance(receipt_id, str) else None
            receipt_evidence_id = receipt.get("evidence_id") if receipt is not None else None
            receipt_evidence = evidence_by_id.get(receipt_evidence_id) if isinstance(receipt_evidence_id, str) else None
            relation_receipt_context = (
                relation_receipt_contexts.get((leaf, receipt_id))
                if isinstance(receipt_id, str) else None
            )
            expected_receipt_revision = (
                relation_receipt_context["source_revision"]
                if relation_receipt_context is not None else revision
            )
            if (
                receipt is None
                or receipt_id not in provided_receipts
                or receipt.get("status") != "accepted"
                or receipt.get("source_revision") != expected_receipt_revision
                or receipt_evidence is None
                or receipt_evidence_id not in evidence_ids
                or receipt_evidence.get("source_revision") != expected_receipt_revision
                or receipt_evidence.get("kind") != "receipt"
                or (
                    relation_receipt_context is not None
                    and receipt_evidence.get("mode") != relation_receipt_context["mode"]
                )
                or not isinstance(receipt.get("owner_role"), str)
                or not isinstance(receipt.get("action"), str)
                or not receipt.get("owner_role").strip()
                or not receipt.get("action").strip()
            ):
                _add_issue(issues, "REQUIRED_OWNER_RECEIPT_MISSING", leaf)

    if isinstance(staged_edges, list):
        for edge_index, edge in enumerate(staged_edges):
            if not isinstance(edge, dict):
                continue
            consumer = edge.get("consumer_key")
            if edge.get("required_for_v1") is not True:
                continue
            outcome = outcomes.get(consumer) if isinstance(consumer, str) else None
            consumer_leaf = leaves.get(consumer) if isinstance(consumer, str) else None
            if outcome is None or consumer_leaf is None:
                continue
            required_stage = consumer_leaf.get("required_release_stage")
            if edge.get("consumer_stage") != required_stage:
                continue
            edge_id = edge.get("edge_id")
            if edge.get("status") != "satisfied":
                _add_issue(issues, "STAGED_EDGE_PREREQUISITE_PENDING", edge_id or f"stage-{edge_index}")
                continue
            prerequisite = edge.get("prerequisite_key")
            prerequisite_outcome = outcomes.get(prerequisite) if isinstance(prerequisite, str) else None
            evidence_ids = edge.get("required_evidence_ids")
            if not isinstance(evidence_ids, list) or not evidence_ids:
                _add_issue(issues, "STAGED_EDGE_EVIDENCE_REQUIRED", edge_id or f"stage-{edge_index}")
                continue
            if prerequisite_outcome is None:
                _add_issue(issues, "STAGED_EDGE_PREREQUISITE_OUTCOME_MISSING", edge_id or f"stage-{edge_index}")
                continue
            prerequisite_outcome_ids = prerequisite_outcome.get("evidence_ids")
            if not isinstance(prerequisite_outcome_ids, list):
                prerequisite_outcome_ids = []
            expected_revision = prerequisite_outcome.get("revision")
            expected_mode = prerequisite_outcome.get("required_mode")
            edge_requirement_ids = edge.get("requirement_ids")
            for evidence_id in evidence_ids:
                evidence = evidence_by_id.get(evidence_id) if isinstance(evidence_id, str) else None
                evidence_requirement_ids = evidence.get("requirement_ids") if evidence is not None else None
                if (
                    not isinstance(evidence_id, str)
                    or evidence_id not in prerequisite_outcome_ids
                    or evidence is None
                    or evidence.get("stage_edge_id") != edge_id
                    or evidence.get("leaf_key") != prerequisite
                    or evidence.get("stage") != edge.get("prerequisite_stage")
                    or not isinstance(edge_requirement_ids, list)
                    or any(not isinstance(value, str) for value in edge_requirement_ids)
                    or not isinstance(evidence_requirement_ids, list)
                    or any(not isinstance(value, str) for value in evidence_requirement_ids)
                    or set(evidence_requirement_ids) != set(edge_requirement_ids)
                    or evidence.get("source_revision") != expected_revision
                    or evidence.get("mode") != expected_mode
                ):
                    _add_issue(issues, "STAGED_EDGE_EVIDENCE_MISMATCH", edge_id or f"stage-{edge_index}")
            required_receipts = edge.get("required_owner_receipt_ids", [])
            if isinstance(required_receipts, list) and required_receipts:
                outcome_receipt_ids = prerequisite_outcome.get("owner_receipt_ids")
                if not isinstance(outcome_receipt_ids, list):
                    outcome_receipt_ids = []
                for receipt_id in required_receipts:
                    receipt = receipt_by_id.get(receipt_id) if isinstance(receipt_id, str) else None
                    receipt_evidence_id = receipt.get("evidence_id") if receipt is not None else None
                    receipt_evidence = evidence_by_id.get(receipt_evidence_id) if isinstance(receipt_evidence_id, str) else None
                    if (
                        receipt is None
                        or receipt_id not in outcome_receipt_ids
                        or receipt.get("status") != "accepted"
                        or receipt.get("source_revision") != expected_revision
                        or receipt_evidence is None
                        or receipt_evidence_id not in prerequisite_outcome_ids
                        or receipt_evidence.get("source_revision") != expected_revision
                        or receipt_evidence.get("kind") != "receipt"
                    ):
                        _add_issue(issues, "STAGED_EDGE_OWNER_RECEIPT_MISSING", edge_id or f"stage-{edge_index}")

    if projection_relations is not None:
        _validate_projection_relation_evidence(
            manifest, projection_relations, evidence_by_id, outcomes, leaves,
            external_artifacts or {}, receipt_by_id, tested, issues,
        )

    for leaf, entry in leaves.items():
        if entry.get("required_for_v1") is True and leaf not in outcomes:
            _add_issue(issues, "REQUIRED_OUTCOME_MISSING", leaf)
    required_outcome_keys = {
        leaf for leaf, entry in leaves.items() if entry.get("required_for_v1") is True
    }
    if isinstance(required_gate_fan_in, dict):
        for expected in required_gate_fan_in.values():
            if isinstance(expected, list):
                required_outcome_keys.update(key for key in expected if isinstance(key, str) and key in leaves)
    edges = dag.get("edges") if isinstance(dag, dict) and isinstance(dag.get("edges"), dict) else {}
    pending = list(required_outcome_keys)
    while pending:
        consumer = pending.pop()
        prerequisites = edges.get(consumer, [])
        if not isinstance(prerequisites, list):
            continue
        for prerequisite in prerequisites:
            if isinstance(prerequisite, str) and prerequisite in leaves and prerequisite not in required_outcome_keys:
                required_outcome_keys.add(prerequisite)
                pending.append(prerequisite)
    for leaf in sorted(required_outcome_keys):
        if leaf not in outcomes:
            _add_issue(issues, "REQUIRED_PREREQUISITE_OUTCOME_MISSING", leaf)
    return {
        "tested_revisions": len(tested),
        "evidence_files": len(evidence_by_id),
        "evidence_bytes_read": evidence_bytes_read,
        "verified_evidence_bytes": verified_evidence_bytes,
        "outcomes": len(outcomes),
        "manifest_sha256": _sha256(manifest_raw),
    }


def _validate_projection_relation_evidence(
    manifest: dict[str, Any],
    relations: list[dict[str, Any]],
    evidence_by_id: dict[str, dict[str, Any]],
    outcomes: dict[str, dict[str, Any]],
    leaves: dict[str, dict[str, Any]],
    external_artifacts: dict[str, dict[str, Any]],
    receipt_by_id: dict[str, dict[str, Any]],
    tested: set[tuple[str, str]],
    issues: list[dict[str, str]],
) -> None:
    """Bind v2 stage evidence to the exact directional relation and content."""
    rows = manifest.get("stage_evidence", [])
    if not isinstance(rows, list):
        _add_issue(issues, "STAGE_EVIDENCE_FAMILY_INVALID", "release-manifest")
        rows = []
    rows = _bounded_rows(rows, MAX_RELEASE_RECORDS, "RELEASE_ROW_LIMIT_EXCEEDED", "stage_evidence", issues)
    by_relation: dict[str, list[dict[str, Any]]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or not _safe_key(row.get("relation_id")):
            _add_issue(issues, "STAGE_EVIDENCE_INVALID", f"stage-evidence-{index}")
            continue
        by_relation.setdefault(row["relation_id"], []).append(row)
    for relation in relations:
        if not isinstance(relation, dict):
            continue
        relation_id = relation.get("relation_id")
        consumer = relation.get("consumer_key")
        if not _safe_key(relation_id):
            continue
        outcome = outcomes.get(consumer) if isinstance(consumer, str) else None
        if outcome is None:
            continue
        selected_stage = outcome.get("evaluated_stage")
        consumer_stage = relation.get("consumer_stage")
        if (
            not isinstance(selected_stage, str) or selected_stage not in PROJECTION_STAGE_ORDER
            or not isinstance(consumer_stage, str) or consumer_stage not in PROJECTION_STAGE_ORDER
            or PROJECTION_STAGE_ORDER[consumer_stage] > PROJECTION_STAGE_ORDER[selected_stage]
        ):
            continue
        if relation.get("applicability") != "required":
            _add_issue(issues, "STAGE_RELATION_APPLICABILITY_UNRESOLVED", relation_id)
            continue
        if relation.get("application_scope") == "live_only" and outcome.get("required_mode") != "live":
            continue
        external_id = relation.get("external_artifact_id")
        if external_id is not None:
            artifact = external_artifacts.get(external_id) if isinstance(external_id, str) else None
            if artifact is None or artifact.get("status") != "accepted":
                _add_issue(issues, "REQUIRED_EXTERNAL_ARTIFACT_PENDING", relation_id)
                continue
        if relation.get("status") != "satisfied":
            _add_issue(issues, "STAGE_RELATION_PREREQUISITE_PENDING", relation_id)
            continue
        matches = by_relation.get(relation_id, [])
        if len(matches) != 1:
            _add_issue(issues, "STAGE_EVIDENCE_CARDINALITY_INVALID", relation_id)
            continue
        proof = matches[0]
        evidence_id = proof.get("evidence_id")
        evidence = evidence_by_id.get(evidence_id) if isinstance(evidence_id, str) else None
        evidence_ids = outcome.get("evidence_ids") if isinstance(outcome.get("evidence_ids"), list) else []
        expected_producer = relation.get("producer_key")
        expected_producer_evidence = proof.get("producer_evidence_id")
        producer_outcome = outcomes.get(expected_producer) if isinstance(expected_producer, str) else None
        producer_ids = producer_outcome.get("evidence_ids", []) if isinstance(producer_outcome, dict) and isinstance(producer_outcome.get("evidence_ids"), list) else []
        exact_fields = (
            proof.get("consumer_key") == consumer
            and proof.get("consumer_requirement_ids") == relation.get("consumer_requirement_ids")
            and proof.get("consumer_stage") == relation.get("consumer_stage")
            and proof.get("producer_key") == expected_producer
            and proof.get("producer_requirement_ids") == relation.get("producer_requirement_ids")
            and proof.get("external_artifact_id") == external_id
            and proof.get("producer_stage") == relation.get("prerequisite_stage")
        )
        digest_matches = (
            evidence is not None
            and proof.get("content_sha256") == evidence.get("sha256")
            and proof.get("source_revision") == evidence.get("source_revision")
            and proof.get("mode") == evidence.get("mode")
            and proof.get("source_revision") == outcome.get("revision")
            and proof.get("mode") == outcome.get("required_mode")
        )
        if external_id is not None:
            producer_bound = expected_producer_evidence is None
        else:
            producer_artifact = evidence_by_id.get(expected_producer_evidence) if isinstance(expected_producer_evidence, str) else None
            artifact_binding = producer_artifact.get("stage_binding") if isinstance(producer_artifact, dict) else None
            producer_revision = proof.get("producer_revision")
            producer_mode = proof.get("producer_mode")
            producer_digest = proof.get("producer_content_sha256")
            producer_entry = leaves.get(expected_producer) if isinstance(expected_producer, str) else None
            producer_repo = producer_entry.get("repository") if isinstance(producer_entry, dict) else None
            producer_revision_tested = (
                isinstance(producer_repo, str)
                and isinstance(producer_revision, str)
                and (producer_repo, producer_revision) in tested
            )
            expected_binding = {
                "relation_id": relation_id,
                "leaf_key": expected_producer,
                "requirement_ids": relation.get("producer_requirement_ids"),
                "stage": relation.get("prerequisite_stage"),
            }
            producer_bound = (
                producer_artifact is not None
                and artifact_binding == expected_binding
                and producer_artifact.get("source_revision") == producer_revision
                and producer_artifact.get("mode") == producer_mode
                and producer_artifact.get("sha256") == producer_digest
                and isinstance(producer_mode, str)
                and producer_mode in MODES
                and isinstance(producer_digest, str)
                and HASH_RE.fullmatch(producer_digest) is not None
                and producer_revision_tested
                and (producer_entry.get("required_mode") is None or producer_entry.get("required_mode") == producer_mode)
                and (
                    expected_producer_evidence not in producer_ids
                    or (
                        isinstance(producer_outcome, dict)
                        and producer_outcome.get("revision") == producer_revision
                        and producer_outcome.get("required_mode") == producer_mode
                    )
                )
            )
        if (
            not exact_fields or not digest_matches or not isinstance(evidence_id, str) or evidence_id not in evidence_ids
            or evidence is None or not producer_bound
        ):
            _add_issue(issues, "STAGE_EVIDENCE_BINDING_MISMATCH", relation_id)

        required_receipts = relation.get("required_owner_receipt_ids", [])
        if not isinstance(required_receipts, list) or any(not _safe_key(value) for value in required_receipts):
            _add_issue(issues, "STAGE_RELATION_RECEIPTS_INVALID", relation_id)
            continue
        receipt_owner = consumer if external_id is not None else expected_producer
        receipt_outcome = outcomes.get(receipt_owner) if isinstance(receipt_owner, str) else None
        receipt_owner_ids = receipt_outcome.get("owner_receipt_ids", []) if isinstance(receipt_outcome, dict) else []
        receipt_evidence_ids = receipt_outcome.get("evidence_ids", []) if isinstance(receipt_outcome, dict) else []
        for receipt_id in required_receipts:
            receipt = receipt_by_id.get(receipt_id)
            receipt_evidence_id = receipt.get("evidence_id") if isinstance(receipt, dict) else None
            receipt_evidence = evidence_by_id.get(receipt_evidence_id) if isinstance(receipt_evidence_id, str) else None
            receipt_binding = receipt_evidence.get("receipt_binding") if isinstance(receipt_evidence, dict) else None
            if external_id is not None:
                expected_receipt_binding = {
                    "relation_id": relation_id,
                    "receipt_id": receipt_id,
                    "owner_leaf_key": receipt_owner,
                    "external_artifact_id": external_id,
                }
                expected_receipt_revision = receipt_outcome.get("revision") if isinstance(receipt_outcome, dict) else None
                expected_receipt_mode = receipt_outcome.get("required_mode") if isinstance(receipt_outcome, dict) else None
            else:
                expected_receipt_binding = {
                    "relation_id": relation_id,
                    "receipt_id": receipt_id,
                    "owner_leaf_key": receipt_owner,
                    "external_artifact_id": None,
                    "producer_evidence_id": proof.get("producer_evidence_id"),
                    "producer_revision": proof.get("producer_revision"),
                    "producer_mode": proof.get("producer_mode"),
                    "producer_content_sha256": proof.get("producer_content_sha256"),
                }
                expected_receipt_revision = proof.get("producer_revision")
                expected_receipt_mode = proof.get("producer_mode")
            if (
                not isinstance(receipt, dict)
                or not isinstance(receipt_outcome, dict)
                or receipt_id not in receipt_owner_ids
                or receipt.get("status") != "accepted"
                or receipt.get("stage_relation_id") != relation_id
                or receipt.get("owner_leaf_key") != receipt_owner
                or receipt.get("external_artifact_id") != external_id
                or receipt.get("source_revision") != expected_receipt_revision
                or receipt_evidence is None
                or receipt_evidence_id not in receipt_evidence_ids
                or receipt_evidence.get("source_revision") != expected_receipt_revision
                or receipt_evidence.get("mode") != expected_receipt_mode
                or receipt_evidence.get("kind") != "receipt"
                or receipt_binding != expected_receipt_binding
                or not isinstance(receipt.get("owner_role"), str)
                or not receipt.get("owner_role", "").strip()
                or not isinstance(receipt.get("action"), str)
                or not receipt.get("action", "").strip()
            ):
                _add_issue(issues, "STAGE_RELATION_OWNER_RECEIPT_MISSING", relation_id)


def _validated_internal_relation_receipt_contexts(
    manifest: dict[str, Any],
    relations: list[dict[str, Any]] | None,
    evidence_by_id: dict[str, dict[str, Any]],
    outcomes: dict[str, dict[str, Any]],
    leaves: dict[str, dict[str, Any]],
    receipt_by_id: dict[str, dict[str, Any]],
    tested: set[tuple[str, str]],
    staged_edges: Any,
) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
    """Return only completely validated internal-relation receipt bindings.

    This map supplies the narrow exception to ordinary outcome revision/mode
    checks. A relation receipt earns that exception only when the selected
    artifact, relation proof, owner receipt, and receipt evidence all identify
    the same artifact bytes and tested revision.
    """
    if not isinstance(relations, list):
        return {}, {}
    rows = manifest.get("stage_evidence", [])
    if not isinstance(rows, list) or len(rows) > MAX_RELEASE_RECORDS:
        return {}, {}

    # Keep the provenance of receipt requirements. A receipt can be required
    # both by an internal relation and independently by its producer leaf or
    # a required staged edge. The selected-artifact revision exception cannot
    # discharge those ordinary obligations. Repeated declarations with the
    # same relation binding remain eligible; a distinct relation is a separate
    # binding and must not inherit this relation's exception.
    strict_receipt_uses: set[tuple[str, str]] = set()
    for leaf_key, leaf in leaves.items():
        required = leaf.get("required_owner_receipts")
        if isinstance(required, list):
            strict_receipt_uses.update(
                (leaf_key, receipt_id)
                for receipt_id in required
                if isinstance(receipt_id, str)
            )
    if isinstance(staged_edges, list):
        for edge in staged_edges:
            if not isinstance(edge, dict) or edge.get("required_for_v1") is not True:
                continue
            producer = edge.get("prerequisite_key")
            consumer = edge.get("consumer_key")
            consumer_leaf = leaves.get(consumer) if isinstance(consumer, str) else None
            receipt_ids = edge.get("required_owner_receipt_ids")
            if (
                not isinstance(producer, str)
                or producer not in leaves
                or not isinstance(consumer_leaf, dict)
                or edge.get("consumer_stage") != consumer_leaf.get("required_release_stage")
                or not isinstance(receipt_ids, list)
            ):
                continue
            strict_receipt_uses.update(
                (producer, receipt_id)
                for receipt_id in receipt_ids
                if isinstance(receipt_id, str)
            )

    declared_relation_uses: dict[tuple[str, str], list[Any]] = {}
    for relation in relations:
        if not isinstance(relation, dict) or relation.get("applicability") != "required":
            continue
        consumer = relation.get("consumer_key")
        consumer_leaf = leaves.get(consumer) if isinstance(consumer, str) else None
        consumer_stage = relation.get("consumer_stage")
        selected_stage = consumer_leaf.get("required_release_stage") if isinstance(consumer_leaf, dict) else None
        if (
            consumer_leaf is None
            or consumer_stage not in PROJECTION_STAGE_ORDER
            or selected_stage not in PROJECTION_STAGE_ORDER
            or PROJECTION_STAGE_ORDER[consumer_stage] > PROJECTION_STAGE_ORDER[selected_stage]
            or (relation.get("application_scope") == "live_only" and consumer_leaf.get("required_mode") != "live")
        ):
            continue
        receipt_owner = consumer if relation.get("external_artifact_id") is not None else relation.get("producer_key")
        receipt_ids = relation.get("required_owner_receipt_ids", [])
        if not isinstance(receipt_owner, str) or not isinstance(receipt_ids, list):
            continue
        for receipt_id in receipt_ids:
            if isinstance(receipt_id, str):
                declared_relation_uses.setdefault((receipt_owner, receipt_id), []).append(
                    relation.get("relation_id")
                )

    by_relation: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if isinstance(row, dict) and _safe_key(row.get("relation_id")):
            by_relation.setdefault(row["relation_id"], []).append(row)

    candidates: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for relation in relations:
        if not isinstance(relation, dict):
            continue
        relation_id = relation.get("relation_id")
        consumer = relation.get("consumer_key")
        producer = relation.get("producer_key")
        if (
            not _safe_key(relation_id)
            or relation.get("applicability") != "required"
            or relation.get("status") != "satisfied"
            or relation.get("external_artifact_id") is not None
            or not _safe_key(consumer)
            or not _safe_key(producer)
        ):
            continue
        consumer_outcome = outcomes.get(consumer)
        producer_outcome = outcomes.get(producer)
        consumer_leaf = leaves.get(consumer)
        producer_leaf = leaves.get(producer)
        if not isinstance(consumer_outcome, dict) or not isinstance(producer_outcome, dict):
            continue
        if not isinstance(consumer_leaf, dict) or not isinstance(producer_leaf, dict):
            continue
        producer_outcome_revision = producer_outcome.get("revision")
        producer_repo = producer_leaf.get("repository")
        producer_outcome_mode = producer_outcome.get("required_mode")
        if (
            producer_outcome.get("status") != "verified"
            or producer_outcome.get("prerequisites_satisfied") is not True
            or producer_outcome.get("evaluated_stage") != producer_leaf.get("required_release_stage")
            or not isinstance(producer_outcome_revision, str)
            or re.fullmatch(r"[0-9a-f]{40}", producer_outcome_revision) is None
            or not isinstance(producer_repo, str)
            or (producer_repo, producer_outcome_revision) not in tested
            or not isinstance(producer_outcome_mode, str)
            or producer_outcome_mode not in MODES
            or (producer_leaf.get("required_mode") is not None and producer_leaf.get("required_mode") != producer_outcome_mode)
        ):
            continue
        consumer_stage = relation.get("consumer_stage")
        selected_stage = consumer_outcome.get("evaluated_stage")
        if (
            not isinstance(consumer_stage, str)
            or consumer_stage not in PROJECTION_STAGE_ORDER
            or not isinstance(selected_stage, str)
            or selected_stage not in PROJECTION_STAGE_ORDER
            or PROJECTION_STAGE_ORDER[consumer_stage] > PROJECTION_STAGE_ORDER[selected_stage]
            or (relation.get("application_scope") == "live_only" and consumer_outcome.get("required_mode") != "live")
        ):
            continue

        proof_rows = by_relation.get(relation_id, [])
        if len(proof_rows) != 1:
            continue
        proof = proof_rows[0]
        if (
            proof.get("consumer_key") != consumer
            or proof.get("consumer_requirement_ids") != relation.get("consumer_requirement_ids")
            or proof.get("consumer_stage") != relation.get("consumer_stage")
            or proof.get("producer_key") != producer
            or proof.get("producer_requirement_ids") != relation.get("producer_requirement_ids")
            or proof.get("external_artifact_id") is not None
            or proof.get("producer_stage") != relation.get("prerequisite_stage")
        ):
            continue

        consumer_evidence_id = proof.get("evidence_id")
        consumer_evidence = evidence_by_id.get(consumer_evidence_id) if isinstance(consumer_evidence_id, str) else None
        consumer_ids = consumer_outcome.get("evidence_ids")
        if not isinstance(consumer_ids, list):
            continue
        if (
            consumer_evidence is None
            or consumer_evidence_id not in consumer_ids
            or proof.get("content_sha256") != consumer_evidence.get("sha256")
            or proof.get("source_revision") != consumer_evidence.get("source_revision")
            or proof.get("source_revision") != consumer_outcome.get("revision")
            or proof.get("mode") != consumer_evidence.get("mode")
            or proof.get("mode") != consumer_outcome.get("required_mode")
        ):
            continue

        producer_evidence_id = proof.get("producer_evidence_id")
        producer_evidence = evidence_by_id.get(producer_evidence_id) if isinstance(producer_evidence_id, str) else None
        producer_binding = producer_evidence.get("stage_binding") if isinstance(producer_evidence, dict) else None
        producer_revision = proof.get("producer_revision")
        producer_mode = proof.get("producer_mode")
        producer_digest = proof.get("producer_content_sha256")
        producer_ids = producer_outcome.get("evidence_ids")
        expected_stage_binding = {
            "relation_id": relation_id,
            "leaf_key": producer,
            "requirement_ids": relation.get("producer_requirement_ids"),
            "stage": relation.get("prerequisite_stage"),
        }
        if (
            producer_evidence is None
            or not isinstance(producer_ids, list)
            or producer_binding != expected_stage_binding
            or producer_evidence.get("source_revision") != producer_revision
            or producer_evidence.get("mode") != producer_mode
            or producer_evidence.get("sha256") != producer_digest
            or not isinstance(producer_revision, str)
            or re.fullmatch(r"[0-9a-f]{40}", producer_revision) is None
            or not isinstance(producer_repo, str)
            or (producer_repo, producer_revision) not in tested
            or not isinstance(producer_mode, str)
            or producer_mode not in MODES
            or not isinstance(producer_digest, str)
            or HASH_RE.fullmatch(producer_digest) is None
            or (producer_leaf.get("required_mode") is not None and producer_leaf.get("required_mode") != producer_mode)
            or (
                producer_evidence_id in producer_ids
                and (
                    producer_outcome.get("revision") != producer_revision
                    or producer_outcome.get("required_mode") != producer_mode
                )
            )
        ):
            continue

        receipt_ids = relation.get("required_owner_receipt_ids", [])
        if not isinstance(receipt_ids, list) or any(not _safe_key(value) for value in receipt_ids):
            continue
        owner_receipt_ids = producer_outcome.get("owner_receipt_ids")
        owner_evidence_ids = producer_outcome.get("evidence_ids")
        if not isinstance(owner_receipt_ids, list) or not isinstance(owner_evidence_ids, list):
            continue
        for receipt_id in receipt_ids:
            receipt = receipt_by_id.get(receipt_id)
            receipt_evidence_id = receipt.get("evidence_id") if isinstance(receipt, dict) else None
            receipt_evidence = evidence_by_id.get(receipt_evidence_id) if isinstance(receipt_evidence_id, str) else None
            expected_receipt_binding = {
                "relation_id": relation_id,
                "receipt_id": receipt_id,
                "owner_leaf_key": producer,
                "external_artifact_id": None,
                "producer_evidence_id": producer_evidence_id,
                "producer_revision": producer_revision,
                "producer_mode": producer_mode,
                "producer_content_sha256": producer_digest,
            }
            if (
                not isinstance(receipt, dict)
                or receipt_id not in owner_receipt_ids
                or not isinstance(receipt_evidence_id, str)
                or receipt_evidence_id not in owner_evidence_ids
                or receipt.get("status") != "accepted"
                or receipt.get("stage_relation_id") != relation_id
                or receipt.get("owner_leaf_key") != producer
                or receipt.get("external_artifact_id") is not None
                or receipt.get("source_revision") != producer_revision
                or receipt_evidence is None
                or receipt_evidence.get("source_revision") != producer_revision
                or receipt_evidence.get("mode") != producer_mode
                or receipt_evidence.get("kind") != "receipt"
                or receipt_evidence.get("receipt_binding") != expected_receipt_binding
                or not isinstance(receipt.get("owner_role"), str)
                or not receipt.get("owner_role", "").strip()
                or not isinstance(receipt.get("action"), str)
                or not receipt.get("action", "").strip()
            ):
                continue
            candidates.setdefault((producer, receipt_id), []).append({
                "evidence_id": receipt_evidence_id,
                "source_revision": producer_revision,
                "mode": producer_mode,
                "producer_evidence_id": producer_evidence_id,
                "producer_content_sha256": producer_digest,
                "relation_id": relation_id,
            })

    # A duplicated receipt/evidence reference across relations is not a basis
    # for choosing one relation's revision. Grant an exception only when all
    # applicable declarations resolve to the same complete binding.
    contexts: dict[tuple[str, str], dict[str, Any]] = {}
    evidence_bindings: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for key, values in candidates.items():
        distinct = {
            (value["evidence_id"], value["source_revision"], value["mode"],
             value["producer_evidence_id"], value["producer_content_sha256"], value["relation_id"])
            for value in values
        }
        if len(distinct) != 1:
            continue
        value = values[0]
        if (
            key in strict_receipt_uses
            or any(
                relation_id != value["relation_id"]
                for relation_id in declared_relation_uses.get(key, [])
            )
        ):
            continue
        contexts[key] = value
        evidence_bindings.setdefault((key[0], value["evidence_id"]), []).append(value)
    safe_evidence_bindings: dict[tuple[str, str], dict[str, Any]] = {}
    for key, values in evidence_bindings.items():
        signatures = {
            (value["source_revision"], value["mode"], value["producer_evidence_id"],
             value["producer_content_sha256"], value["relation_id"])
            for value in values
        }
        if len(signatures) == 1:
            safe_evidence_bindings[key] = values[0]
    return contexts, safe_evidence_bindings


def _required_test_ids(entry: dict[str, Any], *, fallback_to_legacy_tests: bool = True) -> list[str]:
    raw = entry.get("required_tests", entry.get("tests", []) if fallback_to_legacy_tests else [])
    if not isinstance(raw, list):
        return []
    result: list[str] = []
    for item in raw:
        if isinstance(item, str) and _safe_key(item):
            result.append(item)
        elif isinstance(item, dict):
            candidate = item.get("id", item.get("command"))
            if isinstance(candidate, str) and candidate.strip():
                result.append(candidate)
    return result


def validate(
    coverage_map_path: str | Path,
    source_pins_path: str | Path,
    canonical_path: str | Path,
    issue_readbacks_path: str | Path,
    *,
    release_path: str | Path | None = None,
    check_urls: bool = True,
    check_dag: bool = True,
    require_specialists: bool = True,
) -> dict[str, Any]:
    issues: list[dict[str, str]] = []
    projection_v2 = False
    try:
        pins, _ = _read_json(source_pins_path, "source-pins")
        coverage, map_raw = _read_json(coverage_map_path, "coverage-map")
        canonical, canonical_raw = _read_json(canonical_path, "canonical-checklist")
        readbacks, readbacks_raw = _read_json(issue_readbacks_path, "issue-readbacks")
        projection_v2 = coverage.get("format") == "agent-harness.coverage-projection/v2"
        if projection_v2:
            map_pin, canonical_pin, readback_pin = _verify_source_pins_v2(
                pins, map_raw, canonical_raw, readbacks_raw, issues
            )
            map_issues, dimensions, leaves, graph_summary = _validate_map_v2(
                coverage, pins, canonical, readbacks, map_pin, readback_pin,
                check_urls=check_urls, check_dag=check_dag,
                require_specialists=require_specialists or release_path is not None,
            )
        else:
            map_pin, canonical_pin, readback_pin = _verify_source_pins(
                pins, map_raw, canonical_raw, readbacks_raw, issues
            )
            map_issues, dimensions, leaves = _validate_map(
                coverage,
                pins,
                canonical,
                readbacks,
                map_pin,
                readback_pin,
                check_urls=check_urls,
                check_dag=check_dag,
                require_specialists=require_specialists or release_path is not None,
            )
            graph_summary = {}
        issues.extend(item for item in map_issues if item not in issues)
        planning_issue_count = len(issues)
        planning_clean = planning_issue_count == 0
        release_counts: dict[str, Any] | None = None
        release_issue_start = len(issues)
        if release_path is not None:
            try:
                if projection_v2:
                    execution_graph = coverage.get("execution_graph")
                    dag = execution_graph if isinstance(execution_graph, dict) else {"edges": {}}
                    gate_rows = dag.get("gate_outcomes", [])
                    fan_in = {
                        row.get("key"): row.get("fan_in")
                        for row in gate_rows if isinstance(row, dict) and isinstance(row.get("key"), str)
                    } if isinstance(gate_rows, list) else {}
                    release_relations = coverage.get("stage_relations", [])
                    external_nodes_raw = coverage.get("external_artifact_nodes", [])
                    external_nodes = {
                        row.get("artifact_id"): row
                        for row in external_nodes_raw if isinstance(row, dict) and isinstance(row.get("artifact_id"), str)
                    } if isinstance(external_nodes_raw, list) else {}
                else:
                    dag = coverage.get("dag")
                    fan_in = dag.get("required_gate_fan_in") if isinstance(dag, dict) else None
                    release_relations = None
                    external_nodes = None
                release_counts = _validate_release(
                    release_path, leaves, dag, fan_in,
                    [] if projection_v2 else coverage.get("staged_edges"), issues,
                    expected_format="acv1.release-manifest/v2" if projection_v2 else "acv1.release-manifest/v1",
                    projection_relations=release_relations,
                    external_artifacts=external_nodes,
                )
            except InputError as exc:
                _add_issue(issues, exc.code, exc.record)
        else:
            release_counts = None
        release_issue_count = len(issues) - release_issue_start
        mode = "release" if release_path is not None else "planning"
        clean = len(issues) == 0
        status = (
            "SCOPED_RELEASE_EVIDENCE_READY" if clean and mode == "release"
            else "BACKLOG_READY" if clean
            else "BLOCKED"
        )
        canonical_revision = canonical_pin.get("issue_pinned_revision")
        if not isinstance(canonical_revision, str) or re.fullmatch(r"[0-9a-f]{40}", canonical_revision) is None:
            canonical_revision = None
        pending_blockers, pending_blockers_truncated = _reported_blockers(coverage)
        external_artifact_obligations: list[dict[str, Any]] = []
        stage_relation_obligations: list[dict[str, Any]] = []
        if projection_v2:
            ext_nodes = coverage.get("external_artifact_nodes", [])
            if isinstance(ext_nodes, list):
                for node in ext_nodes:
                    if not isinstance(node, dict):
                        continue
                    external_artifact_obligations.append({
                        "artifact_id": _label(node.get("artifact_id")),
                        "owner_role": str(node.get("owner_role", "unassigned"))[:MAX_BLOCKER_TEXT],
                        "deliverable": str(node.get("deliverable", ""))[:MAX_BLOCKER_TEXT],
                        "status": _label(node.get("status")),
                        "action_status": _label(node.get("action_status")),
                        "action": node.get("action") if isinstance(node.get("action"), str) else None,
                        "source_pointer": _json_pointer_label(node.get("source_pointer")),
                        "upstream_source_pointer": _json_pointer_label(node.get("upstream_source_pointer")),
                        "independent_artifact": node.get("independent_artifact") is True,
                    })
                    if node.get("status") == "pending_external":
                        if len(pending_blockers) == 250:
                            pending_blockers_truncated = True
                        else:
                            pending_blockers.append({
                                "record": _label(node.get("artifact_id")),
                                "owner": str(node.get("owner_role", "unassigned"))[:MAX_BLOCKER_TEXT],
                                "action": (
                                    str(node.get("action"))[:MAX_BLOCKER_TEXT]
                                    if node.get("action_status") == "declared_in_source" and isinstance(node.get("action"), str)
                                    else "External action is not declared in source."
                                ),
                            })
            relation_rows = coverage.get("stage_relations", [])
            if isinstance(relation_rows, list):
                for relation in relation_rows:
                    if not isinstance(relation, dict):
                        continue
                    stage_relation_obligations.append({
                        "relation_id": _label(relation.get("relation_id")),
                        "consumer_key": _label(relation.get("consumer_key")),
                        "consumer_requirement_ids": relation.get("consumer_requirement_ids", []),
                        "consumer_stage": _label(relation.get("consumer_stage")),
                        "producer_key": _label(relation.get("producer_key")) if relation.get("producer_key") is not None else None,
                        "producer_requirement_ids": relation.get("producer_requirement_ids"),
                        "external_artifact_id": _label(relation.get("external_artifact_id")) if relation.get("external_artifact_id") is not None else None,
                        "prerequisite_stage": _label(relation.get("prerequisite_stage")),
                        "deliverable": str(relation.get("deliverable", ""))[:MAX_BLOCKER_TEXT],
                        "status": _label(relation.get("status")),
                        "applicability": _label(relation.get("applicability")),
                        "application_scope": _label(relation.get("application_scope")),
                        "reason": str(relation.get("reason", ""))[:MAX_BLOCKER_TEXT],
                        "source_pointer": _json_pointer_label(relation.get("source_pointer")),
                    })
        result = {
            "schema": "acv1.coverage-validation/v2" if projection_v2 else "acv1.coverage-validation/v1",
            "mode": mode,
            "status": status,
            "planning_status": "BACKLOG_READY" if planning_clean else "BLOCKED",
            "coordination_status": _normalized_snapshot_state(coverage.get("coordination_status"), COORDINATION_STATES),
            "publication_status": _normalized_snapshot_state(coverage.get("publication_status"), PUBLICATION_STATES),
            "release_status": status if mode == "release" else "not_run",
            "checks": {
                "issue_readbacks": check_urls,
                "typed_graph": check_dag,
                "specialist_families": require_specialists or release_path is not None,
            },
            "pending_blockers": pending_blockers,
            "pending_blockers_truncated": pending_blockers_truncated,
            "planning_only": mode == "planning",
            "whole_program_acceptance": False,
            "global_acceptance": "not_asserted",
            "source_pins": {
                "coverage_map_sha256": _sha256(map_raw),
                "canonical_git_blob_sha1": _git_blob_sha1(canonical_raw),
                "issue_readbacks_sha256": _sha256(readbacks_raw),
                "canonical_revision": canonical_revision,
                "issue_readback_capture_id": readback_pin.get("capture_id") if readback_pin is not None and _safe_key(readback_pin.get("capture_id")) else None,
                "issue_readback_captured_at": _normalized_utc(readback_pin.get("captured_at")) if readback_pin is not None else None,
            },
            "counts": dimensions,
            "computed_graph": graph_summary,
            "release_counts": release_counts,
            "planning_issue_count": planning_issue_count,
            "release_issue_count": release_issue_count,
            "issue_count": len(issues),
            "issues": issues[:250],
        }
        if projection_v2:
            result["external_artifact_obligations"] = external_artifact_obligations
            result["stage_relation_obligations"] = stage_relation_obligations
        return result
    except InputError as exc:
        issues.append({"code": exc.code, "record": exc.record})
        return {
            "schema": "acv1.coverage-validation/v2" if projection_v2 else "acv1.coverage-validation/v1",
            "mode": "release" if release_path is not None else "planning",
            "status": "BLOCKED",
            "planning_status": "BLOCKED",
            "coordination_status": "unreported",
            "publication_status": "unreported",
            "release_status": "BLOCKED" if release_path is not None else "not_run",
            "checks": {
                "issue_readbacks": check_urls,
                "typed_graph": check_dag,
                "specialist_families": require_specialists or release_path is not None,
            },
            "pending_blockers": [],
            "pending_blockers_truncated": False,
            "planning_only": release_path is None,
            "whole_program_acceptance": False,
            "global_acceptance": "not_asserted",
            "source_pins": {},
            "counts": {},
            "computed_graph": {},
            "release_counts": None,
            "planning_issue_count": 1,
            "release_issue_count": 1 if release_path is not None else 0,
            "issue_count": 1,
            "issues": issues,
            **({"external_artifact_obligations": [], "stage_relation_obligations": []} if projection_v2 else {}),
        }
    except (TypeError, ValueError, OverflowError, RecursionError):
        issues.append({"code": "VALIDATION_STRUCTURE_INVALID", "record": "input"})
        return {
            "schema": "acv1.coverage-validation/v2" if projection_v2 else "acv1.coverage-validation/v1",
            "mode": "release" if release_path is not None else "planning",
            "status": "BLOCKED",
            "planning_status": "BLOCKED",
            "coordination_status": "unreported",
            "publication_status": "unreported",
            "release_status": "BLOCKED" if release_path is not None else "not_run",
            "checks": {"issue_readbacks": check_urls, "typed_graph": check_dag,
                       "specialist_families": require_specialists or release_path is not None},
            "pending_blockers": [],
            "pending_blockers_truncated": False,
            "planning_only": release_path is None,
            "whole_program_acceptance": False,
            "global_acceptance": "not_asserted",
            "source_pins": {"coverage_map_sha256": None, "canonical_git_blob_sha1": None,
                            "issue_readbacks_sha256": None, "canonical_revision": None,
                            "issue_readback_capture_id": None, "issue_readback_captured_at": None},
            "counts": {}, "computed_graph": {}, "release_counts": None,
            "planning_issue_count": 1, "release_issue_count": 0, "issue_count": 1, "issues": issues,
            **({"external_artifact_obligations": [], "stage_relation_obligations": []} if projection_v2 else {}),
        }
    except Exception:
        return {
            "schema": "acv1.coverage-validation/v2" if projection_v2 else "acv1.coverage-validation/v1",
            "mode": "release" if release_path is not None else "planning",
            "status": "BLOCKED",
            "planning_status": "BLOCKED",
            "coordination_status": "unreported",
            "publication_status": "unreported",
            "release_status": "BLOCKED" if release_path is not None else "not_run",
            "checks": {
                "issue_readbacks": check_urls,
                "typed_graph": check_dag,
                "specialist_families": require_specialists or release_path is not None,
            },
            "pending_blockers": [],
            "pending_blockers_truncated": False,
            "planning_only": release_path is None,
            "whole_program_acceptance": False,
            "global_acceptance": "not_asserted",
            "source_pins": {},
            "counts": {},
            "computed_graph": {},
            "release_counts": None,
            "planning_issue_count": 1,
            "release_issue_count": 1 if release_path is not None else 0,
            "issue_count": 1,
            "issues": [{"code": "VALIDATION_INTERNAL_ERROR", "record": "input"}],
            **({"external_artifact_obligations": [], "stage_relation_obligations": []} if projection_v2 else {}),
        }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check explicit ACV1 coverage and evidence snapshots without network access."
    )
    parser.add_argument("coverage_map", help="explicit path to coverage-map JSON")
    parser.add_argument("--source-pins", required=True, help="explicit path to source pin JSON")
    parser.add_argument("--canonical-checklist", required=True, help="explicit path to canonical checklist JSON")
    parser.add_argument("--issue-readbacks", required=True, help="explicit path to timestamped issue readback JSON")
    parser.add_argument("--check-urls", action=argparse.BooleanOptionalAction, default=True, help="compare issue URLs and identities to readbacks (enabled by default)")
    parser.add_argument("--check-dag", action=argparse.BooleanOptionalAction, default=True, help="check typed execution and evidence-stage edges (enabled by default)")
    parser.add_argument("--require-specialist-leaves", action="store_true", help="require each supplied repo family")
    parser.add_argument("--release", help="explicit path to release-manifest JSON")
    parser.add_argument("--require-evidence", action="store_true", help="require exact revision and content-addressed evidence")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.require_evidence and not args.release:
        parser.error("--require-evidence requires --release")
    if args.release and not args.require_evidence:
        parser.error("--release requires --require-evidence")
    report = validate(
        args.coverage_map,
        args.source_pins,
        args.canonical_checklist,
        args.issue_readbacks,
        release_path=args.release,
        check_urls=args.check_urls,
        check_dag=args.check_dag,
        require_specialists=args.require_specialist_leaves,
    )
    sys.stdout.write(json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n")
    return 0 if report["status"] in {"BACKLOG_READY", "SCOPED_RELEASE_EVIDENCE_READY"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
