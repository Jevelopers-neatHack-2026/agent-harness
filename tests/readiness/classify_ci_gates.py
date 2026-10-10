#!/usr/bin/env python3
"""Classify the checked-in prerequisite inputs for CI without passing readiness.

The canonical release and owner-approved JEV plan are intentionally unavailable.
This gate succeeds only when each verifier returns its known typed blocked result.
The emitted JSON is evidence about that blocked state, not a readiness receipt.
"""

from __future__ import annotations

import argparse
import collections
import datetime
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tools import validate_jev_targets as jev  # noqa: E402
from tools import verify_contract_release as contracts  # noqa: E402

FORMAT = "agent-creative.readiness-ci-evidence/v1"
SANDBOX_OBSERVATION_KEYS = (
    "outside_read_blocked", "private_shadow_visible", "sandbox_shadow_write_succeeded", "network_blocked",
    "network_namespace_isolated", "sandbox_exec_capabilities_dropped", "apparmor_stack_verified",
    "payload_no_new_privs", "payload_capability_sets_zero", "python_reexec_stack_verified",
    "python_reexec_capabilities_zero", "python_reexec_no_new_privs", "other_exec_stack_verified",
    "other_exec_capabilities_zero", "other_exec_no_new_privs", "nested_userns_created",
    "nested_userns_setup_complete", "nested_userns_stack_verified", "nested_userns_no_new_privs",
    "nested_userns_creation_attempted", "nested_userns_mapping_requested",
    "nested_userns_mapping_completed", "nested_userns_payload_reached",
    "nested_capability_sets_observed", "nested_mount_attempted", "nested_mount_namespace_denied",
    "nested_bwrap_denied", "host_canary_unchanged",
    "child_isolated", "child_capabilities_dropped",
)
EXPECTED_CONTRACT_CODES = {
    "canonical_release_not_published": 9,
    "owner_evidence_index_unavailable": 1,
}
EXPECTED_JEV_CODES = {
    "dataset_owner_approval_pending": 1,
    "dataset_owner_repository_invalid": 1,
    "dataset_provenance_missing": 1,
    "dataset_split_approvals_missing": 1,
    "dataset_split_empty": 3,
    "metering_stage_inventory_missing": 1,
    "negative_result_rule_missing": 1,
    "numeric_owner_target_missing": 1,
    "owner_approval_missing": 21,
    "owner_evidence_index_unavailable": 1,
    "owner_question_id_inventory_missing": 9,
    "promotion_targets_owner_decisions_pending": 1,
    "question_inventory_revision_missing": 9,
    "required_owner_role_review_missing": 3,
    "required_owner_value_missing": 11,
    "required_question_family_missing": 9,
    "split_hash_missing": 3,
    "targets_dataset_split_hash_missing": 3,
}
EXPECTED_JEV_DIAGNOSTICS_SHA256 = "4d88d331c412eaa740a5faf4e1170e877522b2b8d91f15bb0b8eae564af945a2"


def _bounded_string(value: Any, limit: int = 192) -> str:
    if not isinstance(value, str):
        return "<invalid>"
    value = "".join(char for char in value if char >= " " and char != "\x7f")
    return value[:limit]


def _diagnostics(report: Any) -> Tuple[Dict[str, int], List[Dict[str, str]], bool]:
    if not isinstance(report, dict) or not isinstance(report.get("errors"), list):
        return {}, [], False
    counts: collections.Counter = collections.Counter()
    rows: List[Dict[str, str]] = []
    valid = True
    for item in report["errors"]:
        if not isinstance(item, dict) or not isinstance(item.get("code"), str):
            valid = False
            continue
        code = _bounded_string(item.get("code"), 96)
        counts[code] += 1
        rows.append({
            "code": code,
            "owner": _bounded_string(item.get("owner"), 96),
            "path": _bounded_string(item.get("path"), 192),
        })
    rows.sort(key=lambda row: (row["path"], row["code"], row["owner"]))
    return dict(sorted(counts.items())), rows, valid


def _diagnostics_fingerprint(report: Any) -> Optional[str]:
    if not isinstance(report, dict) or not isinstance(report.get("errors"), list):
        return None
    canonical_rows = []
    for item in report["errors"]:
        if not isinstance(item, dict) or any(not isinstance(item.get(key), str)
                                             for key in ("code", "path", "owner", "action")):
            return None
        canonical_rows.append({key: item[key] for key in ("code", "path", "owner", "action")})
    canonical_rows.sort(key=lambda item: (item["path"], item["code"], item["owner"], item["action"]))
    raw = json.dumps(canonical_rows, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _base_evidence(gate: str, exit_code: Optional[int], report: Any,
                   stderr_nonempty: bool) -> Dict[str, Any]:
    codes, rows, diagnostics_valid = _diagnostics(report)
    status = report.get("status") if isinstance(report, dict) else None
    ready = report.get("ready") if isinstance(report, dict) else None
    readiness = {
        "status": _bounded_string(status, 64) if isinstance(status, str) else "unclassified",
        "ready": ready if isinstance(ready, bool) else False,
    }
    return {
        "format": FORMAT,
        "gate": gate,
        "classification": "external_readiness_blocked_expected",
        "implementation_check": "unexpected_result",
        "readiness": readiness,
        "expected_cli_exit_code": 2,
        "observed_cli_exit_code": exit_code,
        "stderr_nonempty": stderr_nonempty,
        "diagnostics_valid": diagnostics_valid,
        "diagnostics": {"count": sum(codes.values()), "codes": codes, "items": rows},
        "diagnostics_sha256": _diagnostics_fingerprint(report),
        "scope": "checked-in pending owner inputs only; this artifact is not canonical conformance, approved evaluation readiness, promotion, or global acceptance",
    }


def _contract_errors(report: Any) -> List[str]:
    problems: List[str] = []
    if not isinstance(report, dict):
        return ["report_is_not_an_object"]
    codes, _, diagnostics_valid = _diagnostics(report)
    if report.get("tool") != "verify_contract_release":
        problems.append("unexpected_tool_identity")
    if report.get("status") != "blocked" or report.get("ready") is not False:
        problems.append("readiness_is_not_typed_blocked")
    if report.get("dependency_mode") != "offline":
        problems.append("dependency_mode_is_not_offline")
    if "result" in report:
        problems.append("blocked_report_contains_conformance_result")
    if not diagnostics_valid:
        problems.append("diagnostic_shape_invalid")
    if codes != EXPECTED_CONTRACT_CODES:
        problems.append("owner_blocker_code_counts_differ_from_checked_in_pending_release")

    observed = report.get("errors")
    expected = [
        ("$.owner_actions[{}]".format(index), row["owner"], row["action"])
        for index, row in enumerate(contracts.OWNER_ACTIONS)
    ]
    expected.append(("$.owner_evidence_index", "Agent and research",
                     "Provision the independently verified owner-evidence index outside the repository and pin its exact SHA-256 in trusted runner configuration; local manifest assertions are not approval evidence."))
    actual = []
    if isinstance(observed, list):
        for item in observed:
            if not isinstance(item, dict):
                continue
            actual.append((item.get("path"), item.get("owner"), item.get("action")))
    if sorted(actual) != sorted(expected):
        problems.append("owner_action_diagnostics_differ_from_known_pending_release")
    return problems


def _jev_errors(report: Any) -> List[str]:
    problems: List[str] = []
    if not isinstance(report, dict):
        return ["report_is_not_an_object"]
    codes, rows, diagnostics_valid = _diagnostics(report)
    if report.get("tool") != "validate_jev_targets":
        problems.append("unexpected_tool_identity")
    if report.get("status") != "blocked" or report.get("ready") is not False:
        problems.append("readiness_is_not_typed_blocked")
    if report.get("structurally_valid") is not False:
        problems.append("owner_approved_plan_is_not_blocked_as_incomplete")
    if report.get("question_count") != 0:
        problems.append("candidate_question_inventory_is_not_empty")
    if report.get("owner_evidence_verified") is not False:
        problems.append("owner_evidence_was_unexpectedly_claimed_verified")
    if not diagnostics_valid:
        problems.append("diagnostic_shape_invalid")
    if codes != EXPECTED_JEV_CODES:
        problems.append("diagnostic_code_counts_differ_from_checked_in_pending_target_and_dataset")
    if _diagnostics_fingerprint(report) != EXPECTED_JEV_DIAGNOSTICS_SHA256:
        problems.append("diagnostic_content_differs_from_checked_in_pending_target_and_dataset")
    expected_families = set(jev.REQUIRED_FAMILIES)
    observed_family_owners = collections.Counter(
        (row["owner"], row["path"])
        for row in rows if row["code"] == "required_question_family_missing"
    )
    expected_family_owners = collections.Counter(
        (owner_role, "$.questions") for _, owner_role in jev.REQUIRED_FAMILIES.values()
    )
    if expected_family_owners != observed_family_owners or len(expected_families) != 9:
        problems.append("required_question_family_blockers_are_not_complete")
    if "promotion" in report or "activation" in report or report.get("ready") is True:
        problems.append("blocked_report_contains_promotion_or_activation_claim")
    return problems


def classify_readiness_report(gate: str, exit_code: Optional[int], report: Any,
                              stderr_nonempty: bool = False) -> Tuple[Dict[str, Any], List[str]]:
    evidence = _base_evidence(gate, exit_code, report, stderr_nonempty)
    problems: List[str] = []
    if exit_code != 2:
        problems.append("cli_exit_code_was_not_expected_pending_exit_2")
    if stderr_nonempty:
        problems.append("cli_wrote_unexpected_stderr")
    if gate == "contracts":
        problems.extend(_contract_errors(report))
    elif gate == "jev":
        problems.extend(_jev_errors(report))
    else:
        problems.append("unknown_readiness_gate")
    if not problems:
        evidence["classification"] = "external_readiness_blocked_expected"
        evidence["implementation_check"] = "expected_pending_diagnostics_confirmed"
        evidence["owner_actions"] = [
            {"owner": row["owner"], "reference": row["reference"], "action": row["action"]}
            for row in contracts.OWNER_ACTIONS
        ] if gate == "contracts" else []
    if problems:
        evidence["classification"] = "unexpected_ci_observation"
    evidence["check_diagnostics"] = sorted(set(problems))
    return evidence, problems


def _run_checker(gate: str) -> Dict[str, Any]:
    if gate == "contracts":
        command = [sys.executable, "-I", "-B", "-S", str(ROOT / "tools/verify_contract_release.py"),
                   "--manifest", str(ROOT / "fixtures/contracts/release.json"), "--offline"]
    else:
        command = [sys.executable, "-I", "-B", "-S", str(ROOT / "tools/validate_jev_targets.py"),
                   "--targets", str(ROOT / "evals/jev/promotion-targets.json"),
                   "--dataset", str(ROOT / "evals/jev/dataset-manifest.json")]
    # Owner evidence trust roots are intentionally absent. Ambient credentials,
    # evidence overrides, Python paths, and other caller environment are not inherited.
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": "/nonexistent",
        "LC_ALL": "C.UTF-8",
        "PYTHONHASHSEED": "0",
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    run = contracts.run_bounded(command, cwd=ROOT, env=env, input_bytes=b"", timeout=30,
                                stdout_limit=2 * 1024 * 1024, stderr_limit=64 * 1024)
    stdout = run.get("stdout", b"")
    report = None
    parse_error = False
    try:
        report = json.loads(stdout.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError, TypeError, ValueError):
        parse_error = True
    return {
        "exit_code": run.get("returncode"),
        "report": report,
        "stderr_nonempty": bool(run.get("stderr")),
        "process_code": _bounded_string(run.get("code", "execution_start_failed"), 96),
        "parse_error": parse_error,
    }


def _kernel_apparmor_audit(since: str) -> Dict[str, Any]:
    """Return only bounded, sanitized bwrap capability-denial fields."""
    command = ["sudo", "-n", "journalctl", "-k", "-b", "-n", "300", "--no-pager", "-o", "cat",
               "--since", since, "--grep", 'apparmor="DENIED".*(bwrap|unpriv_bwrap)']
    result = contracts.run_bounded(command, cwd=ROOT,
                                   env={"PATH": "/usr/bin:/bin", "HOME": "/nonexistent", "LC_ALL": "C"},
                                   input_bytes=b"", timeout=4, stdout_limit=64 * 1024, stderr_limit=1024)
    status = "unavailable"
    records: List[Dict[str, str]] = []
    if result.get("ok"):
        status = "no_matching_records"
        text = result.get("stdout", b"").decode("utf-8", errors="replace")
        for line in text.splitlines():
            if 'apparmor="DENIED"' not in line or not re.search(r"(?:^|[\s\"/])(?:bwrap|unpriv_bwrap)(?:[\s\"/]|$)", line):
                continue
            operation = re.search(r'operation="([a-zA-Z0-9_-]{1,48})"', line)
            profile = re.search(r'profile="([^"\r\n]{1,160})"', line)
            capability = re.search(r'(?:capname|capability)="?([a-zA-Z0-9_-]{1,48})', line)
            if not operation or not profile:
                continue
            normalized_profile = profile.group(1)
            if "bwrap" not in normalized_profile:
                continue
            record = {"operation": operation.group(1), "profile": normalized_profile}
            if capability:
                record["capability"] = capability.group(1)
            records.append(record)
            if len(records) >= 10:
                break
        if records:
            status = "matching_bwrap_denial_records_observed"
    return {
        "status": status,
        "records": records,
        "prior_loopback_failure_cause": "unproven_from_frozen_CI_logs",
    }


def _safe_sandbox_observations(observed: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key in SANDBOX_OBSERVATION_KEYS:
        if type(observed.get(key)) is bool:
            result[key] = observed[key]
    mount_created = observed.get("nested_mount_namespace_created")
    if type(mount_created) is bool:
        result["nested_mount_namespace_created"] = mount_created
    nested_exit = observed.get("nested_userns_process_exit_code")
    if type(nested_exit) is int and -255 <= nested_exit <= 255:
        result["nested_userns_process_exit_code"] = nested_exit
    nested_stderr = observed.get("nested_userns_stderr_class")
    if nested_stderr in ("empty", "operation_not_permitted", "permission_denied", "missing_file", "other",
                         "mapping_setup_failure"):
        result["nested_userns_stderr_class"] = nested_stderr
    nested_errno = observed.get("nested_mount_errno_category")
    if nested_errno in ("operation_not_permitted", "permission_denied", "other", "unavailable"):
        result["nested_mount_errno_category"] = nested_errno
    capability_names = ("CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb")
    capability_sets = observed.get("nested_capability_sets")
    if (isinstance(capability_sets, dict) and set(capability_sets) == set(capability_names)
            and all(isinstance(capability_sets[name], str) and 1 <= len(capability_sets[name]) <= 32
                    and re.fullmatch(r"[0-9a-fA-F]+", capability_sets[name]) for name in capability_names)):
        result["nested_capability_sets"] = {name: capability_sets[name] for name in capability_names}
    for key in ("apparmor_label", "python_reexec_label", "other_exec_label", "nested_userns_label"):
        label = observed.get(key)
        if (isinstance(label, str) and len(label) <= 160 and "bwrap" in label
                and "unpriv_bwrap" in label and label.endswith("(enforce)")
                and re.fullmatch(r"[A-Za-z0-9_./&() -]+", label)):
            result[key] = label
    nested_cap_eff = observed.get("nested_cap_eff")
    if isinstance(nested_cap_eff, str) and len(nested_cap_eff) <= 32 and re.fullmatch(r"[0-9a-fA-F]+", nested_cap_eff):
        result["nested_cap_eff"] = nested_cap_eff
    if type(observed.get("host_canary_unchanged")) is bool:
        result["host_canary_unchanged"] = observed["host_canary_unchanged"]
    enum_fields = {
        "host_canary_observation": {"unchanged", "changed", "unavailable", "unstable",
                                    "not_regular", "outside_size_bound"},
        "nested_userns_setup_stage": {"not_run", "tool_unavailable", "setup_before_payload",
                                      "user_namespace_creation_failed", "mapping_setup_failed",
                                      "payload_setup_incomplete", "mount_attempted",
                                      "timeout_or_output_limit"},
        "nested_userns_creation_observation": {"confirmed", "denied", "failed", "not_observed"},
        "nested_userns_mapping_observation": {"completed", "failed", "not_observed"},
        "nested_userns_mapping_stage": {"not_observed", "completed", "uid_mapping_failed",
                                        "gid_mapping_failed", "setgroups_mapping_failed",
                                        "user_namespace_creation_failed", "setup_before_payload"},
        "nested_userns_errno_category": {"operation_not_permitted", "permission_denied", "invalid_argument",
                                         "resource_limit", "missing_resource", "address_conflict", "other", "empty"},
        "nested_bwrap_errno_category": {"operation_not_permitted", "permission_denied", "invalid_argument",
                                        "resource_limit", "missing_resource", "address_conflict", "other", "empty"},
        "nested_userns_source_identity": {"util-linux-v2.39.3-sys-utils-unshare.c", "unsupported"},
        "nested_bwrap_source_identity": {"bubblewrap-v0.9.0-bubblewrap.c+network.c", "unsupported"},
        "nested_bwrap_setup_stage": {"not_run", "payload_reached", "loopback_setup_denied",
                                     "loopback_link_setup_failed", "user_namespace_creation_failed",
                                     "uid_mapping_failed", "gid_mapping_failed", "setgroups_mapping_failed",
                                     "mount_setup_failed",
                                     "setup_or_exec_failed", "timeout_or_output_limit",
                                     "not_observed"},
        "nested_bwrap_stderr_class": {"empty", "operation_not_permitted", "permission_denied",
                                       "network_namespace_loopback_setup_denied", "other"},
    }
    for key, allowed in enum_fields.items():
        value = observed.get(key)
        if isinstance(value, str) and value in allowed:
            result[key] = value
    namespace_errno = observed.get("nested_mount_errno_category")
    if namespace_errno in {"operation_not_permitted", "permission_denied", "other", "unavailable"}:
        result["nested_mount_errno_category"] = namespace_errno
    for key in ("nested_userns_utility_version", "nested_bwrap_utility_version"):
        version = observed.get(key)
        if isinstance(version, str) and re.fullmatch(r"[0-9A-Za-z.+~-]{1,48}", version):
            result[key] = version
    for key in ("nested_bwrap_payload_reached", "nested_userns_process_bounded"):
        if type(observed.get(key)) is bool:
            result[key] = observed[key]
    for key in ("nested_bwrap_exit_code", "nested_userns_utility_version_exit_code",
                "nested_bwrap_utility_version_exit_code"):
        value = observed.get(key)
        if type(value) is int and -255 <= value <= 255:
            result[key] = value
    for key in ("nested_userns_stderr_bytes", "nested_bwrap_stderr_bytes"):
        value = observed.get(key)
        if type(value) is int and 0 <= value <= 2048:
            result[key] = value
    return result


def classify_sandbox() -> Tuple[Dict[str, Any], List[str]]:
    problems: List[str] = []
    evidence: Dict[str, Any] = {
        "format": FORMAT,
        "gate": "sandbox",
        "classification": "linux_bubblewrap_canary_result",
        "implementation_check": "unexpected_result",
        "scope": "synthetic local canaries only; this is not a general security attestation or external readiness receipt",
        "platform": _bounded_string(platform.system(), 48),
        "runtime_executable": "python3",
        "bubblewrap": {"available": False, "path": None, "version": None},
        "canary": {"status": "not_run", "details": "synthetic filesystem, socket, and descendant canaries required"},
    }
    if platform.system() != "Linux":
        problems.append("required_linux_sandbox_runner_not_used")
    bwrap = shutil.which("bwrap")
    if not bwrap:
        problems.append("bubblewrap_not_installed")
    else:
        real_bwrap = os.path.realpath(bwrap)
        evidence["bubblewrap"]["path"] = _bounded_string(real_bwrap, 192)
        if not (real_bwrap.startswith("/usr/bin/") or real_bwrap.startswith("/bin/")):
            problems.append("bubblewrap_is_not_a_system_package_path")
        if os.access(real_bwrap, os.X_OK):
            version_run = contracts.run_bounded([real_bwrap, "--version"], cwd=ROOT,
                                                env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"},
                                                input_bytes=b"", timeout=3,
                                                stdout_limit=512, stderr_limit=512)
            version_text = version_run.get("stdout", b"").decode("ascii", errors="ignore").strip()
            match = re.fullmatch(r"bubblewrap ([0-9]+(?:\.[0-9A-Za-z+-]+)*)", version_text)
            if version_run.get("ok") and match and not version_run.get("stderr"):
                evidence["bubblewrap"]["available"] = True
                evidence["bubblewrap"]["version"] = match.group(1)
            else:
                problems.append("bubblewrap_version_could_not_be_verified")
        else:
            problems.append("bubblewrap_is_not_executable")

    audit_since = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    if not problems and evidence["bubblewrap"]["available"]:
        # The probe's subprocesses use a reduced environment and the exact path
        # that was just version-checked; the child canaries execute inside bwrap.
        original_which = contracts.shutil.which
        observations: Dict[str, Any] = {}
        try:
            contracts.shutil.which = lambda name: bwrap if name == "bwrap" else original_which(name)
            reason = contracts.probe_sandbox(ROOT, observations)
        finally:
            contracts.shutil.which = original_which
        evidence["canary"]["observations"] = _safe_sandbox_observations(observations)
        evidence["canary"]["kernel_audit"] = _kernel_apparmor_audit(audit_since)
        if reason is None:
            missing = [key for key in SANDBOX_OBSERVATION_KEYS
                       if evidence["canary"]["observations"].get(key) is not True]
            if evidence["canary"]["observations"].get("host_canary_unchanged") is not True:
                missing.append("host_canary_unchanged")
            if evidence["canary"]["observations"].get("nested_mount_namespace_created") is not False:
                missing.append("nested_mount_namespace_created_false")
            if evidence["canary"]["observations"].get("host_canary_observation") != "unchanged":
                missing.append("host_canary_read_not_confirmed")
            if evidence["canary"]["observations"].get("nested_userns_setup_stage") != "mount_attempted":
                missing.append("nested_userns_stage_not_confirmed")
            if evidence["canary"]["observations"].get("nested_bwrap_setup_stage") != "loopback_setup_denied":
                missing.append("nested_bwrap_denial_stage_not_confirmed")
            if missing:
                problems.append("bubblewrap_canary_evidence_incomplete")
                evidence["canary"]["status"] = "failed"
                evidence["canary"]["details"] = "required synthetic observations were absent or false"
            else:
                evidence["canary"]["status"] = "synthetic_canaries_confirmed"
                evidence["implementation_check"] = "required_linux_isolation_canaries_confirmed"
        else:
            problems.append("bubblewrap_canary_failed")
            evidence["canary"]["status"] = "failed"
            # The verifier reason is constant sanitized text; cap it before publication.
            evidence["canary"]["details"] = _bounded_string(reason, 640)
    evidence["check_diagnostics"] = sorted(set(problems))
    return evidence, problems


def _write_evidence(path: Path, evidence: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(path.name + ".tmp")
    temp_path.write_text(json.dumps(evidence, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    temp_path.replace(path)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gate", choices=("contracts", "jev", "sandbox"), required=True)
    parser.add_argument("--output", type=Path, required=True,
                        help="write sanitized classified evidence to this path")
    args = parser.parse_args(argv)
    if args.gate == "sandbox":
        evidence, problems = classify_sandbox()
    else:
        observed = _run_checker(args.gate)
        evidence, problems = classify_readiness_report(
            args.gate, observed["exit_code"], observed["report"], observed["stderr_nonempty"])
        if observed["parse_error"]:
            problems = sorted(set(problems + ["cli_report_is_not_valid_json"]))
            evidence["implementation_check"] = "unexpected_result"
            evidence["check_diagnostics"] = problems
        if observed["process_code"] not in ("ok", "adapter_nonzero_exit"):
            problems = sorted(set(problems + ["checker_process_did_not_complete_normally"]))
            evidence["implementation_check"] = "unexpected_result"
            evidence["check_diagnostics"] = problems
    try:
        _write_evidence(args.output, evidence)
    except (OSError, UnicodeError, TypeError, ValueError):
        print("readiness CI evidence could not be written", file=sys.stderr)
        return 1
    summary = {
        "gate": args.gate,
        "implementation_check": evidence.get("implementation_check"),
        "readiness": evidence.get("readiness"),
        "diagnostic_count": evidence.get("diagnostics", {}).get("count"),
        "artifact": str(args.output),
    }
    if args.gate == "sandbox":
        summary["canary"] = evidence.get("canary")
        summary["check_diagnostics"] = evidence.get("check_diagnostics", [])
    print(json.dumps(summary, sort_keys=True, separators=(",", ":")))
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
