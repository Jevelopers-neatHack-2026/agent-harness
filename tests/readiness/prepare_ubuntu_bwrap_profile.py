#!/usr/bin/env python3
"""Provision one digest-pinned Ubuntu bubblewrap AppArmor profile for ephemeral CI."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import posixpath
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


PROFILE_PACKAGE = "apparmor-profiles"
PROFILE_VERSION = "4.0.1really4.0.1-0ubuntu0.24.04.8"
PROFILE_DEB_SHA256 = "4e7d728322f899a7a06e71bedd4f4bd1f20c21f0b3361120f34cf5c0feec849e"
PROFILE_MEMBER = "./usr/share/apparmor/extra-profiles/bwrap-userns-restrict"
PROFILE_SHA256 = "11d39094f044f0cda0febb3ad517b830301da6b2ce929664af09ee9e4dd264f9"
BWRAP_VERSION = "0.9.0-1ubuntu0.3"
INSTALLED_PROFILE = Path("/run/acv1-native-profile/bwrap-userns-restrict")
APPARMOR_PROFILES = Path("/sys/kernel/security/apparmor/profiles")
PROFILE_TABLE_MAX_BYTES = 128 * 1024
APT_PROVENANCE_FAILURE_PHASES = frozenset({"metadata_v1", "source_v2", "index_targets"})
APT_CACHE_DIAGNOSTIC_BYTES = 8 * 1024
APT_INDEX_TARGETS_LIMIT = 128 * 1024
APT_MIRROR_URI = "mirror+file:/etc/apt/apt-mirrors.txt"
APT_MIRRORLIST_PATH = Path("/etc/apt/apt-mirrors.txt")
UBUNTU_SOURCES_PATH = Path("/etc/apt/sources.list.d/ubuntu.sources")
UBUNTU_ARCHIVE_KEYRING = Path("/usr/share/keyrings/ubuntu-archive-keyring.gpg")
APT_MIRRORLIST_BYTES = (
    b"http://azure.archive.ubuntu.com/ubuntu/\tpriority:1\n"
    b"https://archive.ubuntu.com/ubuntu/\tpriority:2\n"
    b"https://security.ubuntu.com/ubuntu/\tpriority:3\n"
)
APT_RUNNER_SOURCE_FIELDS = frozenset({"types", "uris", "suites", "components", "signed-by"})
APT_RUNNER_ARCHIVE_SUITES = ("noble", "noble-updates", "noble-backports")
APT_RUNNER_SECURITY_SUITES = ("noble-security",)
APT_RUNNER_COMPONENTS = ("main", "restricted", "universe", "multiverse")
APT_RUNNER_SOURCE_FORMAT = "pinned_github_ubuntu_2404_deb822_archive_security_split_v1"
APT_INDEXTARGETS_FORMAT = (
    "Identifier: $(IDENTIFIER)\n"
    "MetaKey: $(METAKEY)\n"
    "Target-Of: $(TARGET_OF)\n"
    "Architecture: $(ARCHITECTURE)\n"
    "Component: $(COMPONENT)\n"
    "Suite: $(SUITE)\n"
    "Codename: $(CODENAME)\n"
    "Origin: $(ORIGIN)\n"
    "Label: $(LABEL)\n"
    "Trusted: $(TRUSTED)\n"
    "Repo-URI: $(REPO_URI)\n"
    "URI: $(URI)\n\n"
)
APT_INDEX_TARGET_FIELDS = frozenset({
    "identifier", "metakey", "target-of", "architecture", "component", "suite",
    "codename", "origin", "label", "trusted", "repo-uri", "uri",
})
APT_PACKAGE_IDENTITY_FIELDS = frozenset({
    "package", "version", "architecture", "filename", "sha256", "apt-sources",
})
APT_PACKAGE_RECORD_FIELDS = APT_PACKAGE_IDENTITY_FIELDS
APT_SECURITY_BOOLEAN_OPTIONS = {
    "trusted": "reject_true",
    "allow-insecure": "reject_true",
    "allow-weak": "reject_true",
    "allow-downgrade-to-insecure": "reject_true",
    "check-valid-until": "reject_false",
    "check-date": "reject_false",
}
APT_SECURITY_CONFIG_KEYS = {
    "apt::get::allowunauthenticated": "reject_true",
    "acquire::allowinsecurerepositories": "reject_true",
    "acquire::allowweakrepositories": "reject_true",
    "acquire::allowdowngradetoinsecurerepositories": "reject_true",
    "acquire::check-valid-until": "reject_false",
    "acquire::check-date": "reject_false",
}
APT_SUPPORTED_PACKAGE_VERSIONS = {
    "2.7.14build2": "2.7.14",
    "2.8.3": "2.8.3",
}
APT_GET_PATH = "/usr/bin/apt-get"
APT_CACHE_PATH = "/usr/bin/apt-cache"
APT_CONFIG_PATH = "/usr/bin/apt-config"
APT_SOURCES_LIST_PATH = Path("/etc/apt/sources.list")
APT_SOURCES_PARTS_PATH = Path("/etc/apt/sources.list.d")
APT_SOURCE_FILE_LIMIT = 64
APT_SOURCE_DIRECTORY_ENTRY_LIMIT = 512
APT_SOURCE_FILE_BYTES = 64 * 1024
APT_SOURCE_TOTAL_BYTES = 256 * 1024
LOCAL_OVERRIDES = (
    Path("/etc/apparmor.d/local/bwrap-userns-restrict"),
    Path("/etc/apparmor.d/local/unpriv_bwrap"),
)
RELEVANT_SYSCTLS = (
    "apparmor_restrict_unprivileged_userns",
    "apparmor_restrict_unprivileged_unconfined",
)


class SetupFailure(Exception):
    def __init__(self, code: str, *, details: dict[str, Any] | None = None):
        super().__init__(code)
        self.code = code
        # Callers may attach only pre-sanitized, fixed-shape diagnostics. Raw
        # APT output is intentionally never retained in exceptions or artifacts.
        self.details = details or {}


def _apt_error_category(stderr: bytes) -> str:
    """Classify a bounded APT diagnostic without returning its contents."""
    text = stderr[:APT_CACHE_DIAGNOSTIC_BYTES].decode("utf-8", "replace").lower()
    if any(marker in text for marker in (
            "not understood", "unknown option", "invalid option", "unrecognized option")):
        return "unsupported_option"
    if any(marker in text for marker in (
            "no packages found", "unable to locate package", "version "
            "was not found", "couldn't find any package")):
        return "pinned_package_or_version_unavailable"
    if any(marker in text for marker in (
            "could not open file", "cannot open", "failed to fetch", "some index files failed",
            "hash sum mismatch", "temporary failure resolving", "cache file is corrupted",
            "could not get lock", "unable to acquire the dpkg frontend lock")):
        return "apt_repository_or_index_infrastructure_failure"
    return "unclassified_apt_command_failure"


def _apt_command_diagnostics(phase: str, failure_kind: str, *, return_code: int | None = None,
                             error_category: str | None = None, stdout_bytes: int = 0,
                             stderr_bytes: int = 0) -> dict[str, Any]:
    if phase not in APT_PROVENANCE_FAILURE_PHASES:
        raise SetupFailure("apt_cache_command_phase_invalid")
    result: dict[str, Any] = {
        "tool": "apt-cache" if phase in {"metadata_v1", "source_v2"} else "apt-get",
        "phase": phase,
        "failure_kind": failure_kind,
        "stdout_bytes_capped": min(max(stdout_bytes, 0), 128 * 1024 + 1),
        "stderr_bytes_capped": min(max(stderr_bytes, 0), APT_CACHE_DIAGNOSTIC_BYTES + 1),
    }
    if return_code is not None:
        result["return_code"] = max(-255, min(return_code, 255))
    if error_category is not None:
        result["error_category"] = error_category
    return result


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
                         encoding="utf-8")
    temporary.replace(path)


def _command(args: list[str], *, timeout: float = 20, limit: int = 128 * 1024,
             cwd: Path | None = None, apt_phase: str | None = None) -> tuple[bytes, bytes]:
    if apt_phase is not None and apt_phase not in APT_PROVENANCE_FAILURE_PHASES:
        raise SetupFailure("apt_cache_command_phase_invalid")
    try:
        result = subprocess.run(args, cwd=cwd, stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                check=False, timeout=timeout, close_fds=True)
    except OSError as exc:
        details = ({"apt_command": _apt_command_diagnostics(
            apt_phase, "launch_failed", error_category="apt_tool_executable_unavailable")}
                   if apt_phase else None)
        raise SetupFailure("command_unavailable_or_timed_out", details=details) from exc
    except subprocess.TimeoutExpired as exc:
        details = ({"apt_command": _apt_command_diagnostics(
            apt_phase, "timed_out", error_category="apt_tool_command_timed_out")}
                   if apt_phase else None)
        raise SetupFailure("command_unavailable_or_timed_out", details=details) from exc
    if len(result.stdout) > limit or len(result.stderr) > 8 * 1024:
        details = ({"apt_command": _apt_command_diagnostics(
            apt_phase, "output_limit_exceeded", error_category="apt_tool_output_limit_exceeded",
            stdout_bytes=len(result.stdout), stderr_bytes=len(result.stderr))}
                   if apt_phase else None)
        raise SetupFailure("command_output_exceeded_bound", details=details)
    if result.returncode != 0:
        details = ({"apt_command": _apt_command_diagnostics(
            apt_phase, "nonzero_exit", return_code=result.returncode,
            error_category=_apt_error_category(result.stderr),
            stdout_bytes=len(result.stdout), stderr_bytes=len(result.stderr))}
                   if apt_phase else None)
        raise SetupFailure("command_failed", details=details)
    return result.stdout, result.stderr


def _quiet_command(args: list[str], *, timeout: float = 180,
                   cwd: Path | None = None) -> None:
    try:
        result = subprocess.run(args, cwd=cwd, stdin=subprocess.DEVNULL,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                check=False, timeout=timeout, close_fds=True)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SetupFailure("package_command_unavailable_or_timed_out") from exc
    if result.returncode != 0:
        raise SetupFailure("authenticated_apt_operation_failed")


def _fields(text: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in text.splitlines():
        if line[:1].isspace():
            continue
        key, separator, value = line.partition(":")
        if separator:
            result[key] = value.strip()
    return result


def _stanzas(text: str) -> list[dict[str, str]]:
    return [_fields(part) for part in re.split(r"\n\s*\n", text.strip()) if part.strip()]


def _apt_package_matches(value: str, package: str) -> bool:
    expected_architecture = "all" if package == PROFILE_PACKAGE else "amd64"
    return value in {package, package + ":" + expected_architecture}


def _candidate_record_diagnostics(metadata_text: str, source_text: str | None,
                                  package: str, version: str) -> dict[str, Any]:
    """Summarize APT's candidate shape without retaining mirror URLs or values."""
    allowed_fields = (
        "Package", "Version", "Architecture", "Filename", "SHA256", "APT-Sources",
    )

    def summarize(text: str | None) -> dict[str, Any]:
        if not text:
            return {
                "record_count": 0, "record_count_capped": False,
                "exact_candidate_count": 0, "exact_candidate_count_capped": False,
                "fields_present": [], "source_token_count": 0,
                "source_scheme": "missing", "source_host_class": "missing",
                "source_release_component": "missing", "source_index_architecture": "missing",
                "source_index_type": "missing", "parse_status": "missing",
            }
        try:
            rows = _parse_deb822_records(
                text, failure_code="apt_package_record_invalid", max_bytes=128 * 1024,
                allow_comments=False, reject_continued_fields=APT_PACKAGE_IDENTITY_FIELDS,
            )
        except SetupFailure:
            return {
                "record_count": 0, "record_count_capped": False,
                "exact_candidate_count": 0, "exact_candidate_count_capped": False,
                "fields_present": [], "source_token_count": 0,
                "source_scheme": "invalid", "source_host_class": "invalid",
                "source_release_component": "invalid", "source_index_architecture": "invalid",
                "source_index_type": "invalid", "parse_status": "invalid",
            }
        matches = [row for row in rows
                   if _apt_package_matches(row.get("package", ""), package)
                   and row.get("version") == version]
        row = matches[0] if len(matches) == 1 else {}
        source = row.get("apt-sources", "")
        tokens = source.split()
        try:
            uri = urlparse(tokens[0]) if tokens else None
            hostname = (uri.hostname or "").lower() if uri else ""
        except ValueError:
            uri = None
            hostname = ""
        if tokens and tokens[0] == APT_MIRROR_URI:
            host_class = "ubuntu_runner_mirrorlist"
            source_scheme = "mirror+file"
        elif uri and uri.scheme.lower() == "mirror+file":
            host_class = "unrecognized_mirrorlist_path"
            source_scheme = "mirror+file"
        elif uri and uri.scheme.lower() in {"mirror", "mirror+http", "mirror+https"}:
            host_class = "unrecognized_mirror_transport"
            source_scheme = uri.scheme.lower()
        elif not source:
            host_class = "missing"
            source_scheme = "missing"
        elif hostname == "archive.ubuntu.com" or hostname.endswith(".archive.ubuntu.com"):
            host_class = "ubuntu_archive"
            source_scheme = uri.scheme.lower() if uri and uri.scheme.lower() in {"http", "https"} else "other"
        elif hostname == "security.ubuntu.com":
            host_class = "ubuntu_security"
            source_scheme = uri.scheme.lower() if uri and uri.scheme.lower() in {"http", "https"} else "other"
        else:
            host_class = "other_or_invalid"
            source_scheme = uri.scheme.lower() if uri and uri.scheme.lower() in {"http", "https"} else "other"
        release_component = tokens[1] if len(tokens) > 1 else ""
        return {
            "record_count": min(len(rows), 99),
            "record_count_capped": len(rows) > 99,
            "exact_candidate_count": min(len(matches), 99),
            "exact_candidate_count_capped": len(matches) > 99,
            "fields_present": [field for field in allowed_fields if field.lower() in row],
            "source_token_count": min(len(tokens), 8),
            "source_scheme": source_scheme,
            "source_host_class": host_class,
            "source_release_component": (
                release_component if release_component == "noble-updates/main"
                else ("other" if release_component else "missing")
            ),
            "source_index_architecture": (
                tokens[2] if len(tokens) > 2 and tokens[2] in {"amd64", "all"}
                else ("other" if len(tokens) > 2 else "missing")
            ),
            "source_index_type": (
                tokens[3] if len(tokens) > 3 and tokens[3] == "Packages"
                else ("other" if len(tokens) > 3 else "missing")
            ),
            "parse_status": "valid",
        }

    return {"metadata_record": summarize(metadata_text),
            "source_record": summarize(source_text)}


def _validated_apt_source(source: str) -> str:
    source_tokens = source.split()
    if len(source_tokens) != 4:
        raise SetupFailure("signed_apt_package_origin_missing")
    uri_text = source_tokens[0]
    release_component = source_tokens[1]
    release, separator, component = release_component.partition("/")
    expected_index_architecture = "amd64"
    if (not separator or release != "noble-updates" or component != "main"
            or source_tokens[2] != expected_index_architecture or source_tokens[3] != "Packages"):
        raise SetupFailure("signed_apt_package_origin_is_not_ubuntu_noble_updates")
    if uri_text == APT_MIRROR_URI:
        return source
    try:
        parsed = urlparse(uri_text)
        hostname = (parsed.hostname or "").lower()
        port = parsed.port
    except ValueError as exc:
        raise SetupFailure("signed_apt_package_origin_is_not_ubuntu_noble_updates") from exc
    if (parsed.scheme not in {"http", "https"} or not hostname
            or parsed.username is not None or parsed.password is not None
            or parsed.query or parsed.fragment
            or (port is not None and port != (443 if parsed.scheme == "https" else 80))
            or not (hostname == "archive.ubuntu.com" or hostname.endswith(".archive.ubuntu.com")
                    or hostname == "security.ubuntu.com")
            or parsed.path.rstrip("/") != "/ubuntu"):
        raise SetupFailure("signed_apt_package_origin_is_not_ubuntu_noble_updates")
    return source


def _parse_deb822_records(text: str, *, failure_code: str,
                          max_bytes: int = APT_INDEX_TARGETS_LIMIT,
                          allow_comments: bool = True,
                          reject_continued_fields: frozenset[str] = frozenset()) -> list[dict[str, str]]:
    # Check the character count before encoding so a caller cannot make the
    # parser allocate an unbounded UTF-8 copy just to enforce its byte limit.
    if len(text) > max_bytes or len(text.encode("utf-8")) > max_bytes or "\x00" in text:
        raise SetupFailure(failure_code)
    records: list[dict[str, str]] = []
    current: dict[str, str] = {}
    last_key: str | None = None

    def finish() -> None:
        nonlocal current, last_key
        if current:
            records.append(current)
            current = {}
        last_key = None

    for line in text.splitlines():
        if len(line) > max_bytes:
            raise SetupFailure(failure_code)
        if not line.strip():
            finish()
            continue
        if line.startswith("#"):
            if allow_comments:
                continue
            raise SetupFailure(failure_code)
        if line[:1].isspace():
            if last_key is None or last_key in reject_continued_fields:
                raise SetupFailure(failure_code)
            current[last_key] += " " + line.strip()
            continue
        name, separator, value = line.partition(":")
        key = name.lower()
        if (not separator or not re.fullmatch(r"[a-z][a-z0-9-]*", key)
                or key in current):
            raise SetupFailure(failure_code)
        current[key] = value.strip()
        last_key = key
    finish()
    if not records or len(records) > 512:
        raise SetupFailure(failure_code)
    return records


def _canonical_archive_uri(value: str) -> str | None:
    try:
        parsed = urlparse(value)
        host = (parsed.hostname or "").lower()
        port = parsed.port
    except ValueError:
        return None
    if (parsed.scheme not in {"http", "https"} or not host
            or parsed.username is not None or parsed.password is not None
            or parsed.query or parsed.fragment
            or (port is not None and port != (443 if parsed.scheme == "https" else 80))
            or not (host == "archive.ubuntu.com" or host.endswith(".archive.ubuntu.com")
                    or host == "security.ubuntu.com")
            or parsed.path.rstrip("/") != "/ubuntu"):
        return None
    return f"{parsed.scheme.lower()}://{host}/ubuntu"


def _parse_runner_mirrorlist(mirrorlist: bytes) -> tuple[str, ...]:
    if mirrorlist != APT_MIRRORLIST_BYTES:
        rows = mirrorlist.splitlines()
        recognized = 0
        for row in rows[:8]:
            candidate = row.split(b"\t", 1)[0].decode("ascii", "ignore")
            if _canonical_archive_uri(candidate):
                recognized += 1
        raise SetupFailure("ubuntu_runner_apt_mirrorlist_unrecognized", details={
            "mirrorlist_line_count": min(len(rows), 99),
            "mirrorlist_line_count_capped": len(rows) > 99,
            "recognized_ubuntu_archive_uri_count": recognized,
            "expected_format": "pinned_github_ubuntu_2404_mirrorlist_v1",
        })
    return tuple(line.split(b"\t", 1)[0].decode("ascii") for line in mirrorlist.splitlines())


def _component_field_diagnostics(value: str) -> dict[str, Any]:
    """Classify runner components without copying untrusted token text to evidence."""
    tokens = value.split()
    allowed = frozenset(APT_RUNNER_COMPONENTS)
    counts = {name: tokens.count(name) for name in APT_RUNNER_COMPONENTS}
    unknown_count = sum(token not in allowed for token in tokens)
    duplicate_count = len(tokens) - len(set(tokens))
    exact_set = (
        len(tokens) == len(APT_RUNNER_COMPONENTS)
        and not unknown_count
        and not duplicate_count
        and set(tokens) == allowed
    )
    if not exact_set:
        order_class = "invalid"
    elif tokens == list(APT_RUNNER_COMPONENTS):
        order_class = "canonical"
    else:
        order_class = "permutation"
    return {
        "token_count": min(len(tokens), 99),
        "token_count_capped": len(tokens) > 99,
        "allowed_component_counts": {name: min(count, 99) for name, count in counts.items()},
        "unknown_component_count": min(unknown_count, 99),
        "unknown_component_count_capped": unknown_count > 99,
        "duplicate_component_count": min(duplicate_count, 99),
        "duplicate_component_count_capped": duplicate_count > 99,
        "order_class": order_class,
        "exact_set": exact_set,
    }


def _runner_source_layout_diagnostics(records: list[dict[str, str]], source_file: bytes) -> dict[str, Any]:
    archive_count = 0
    security_count = 0
    unknown_suite_count = 0
    exact_field_set_count = 0
    exact_types_count = 0
    exact_mirror_uri_count = 0
    exact_components_count = 0
    component_order_counts = {"canonical": 0, "permutation": 0, "invalid": 0}
    component_unknown_total = 0
    component_duplicate_total = 0
    component_stanzas: list[dict[str, Any]] = []
    exact_signed_by_count = 0
    invalid_count = 0
    mirror_count = sum(
        APT_MIRROR_URI in row.get("uris", "").split() for row in records)
    for row in records:
        uris = row.get("uris", "").split()
        suites = row.get("suites", "").split()
        archive_suites = (
            len(suites) == len(APT_RUNNER_ARCHIVE_SUITES)
            and set(suites) == set(APT_RUNNER_ARCHIVE_SUITES)
        )
        security_suites = (
            len(suites) == len(APT_RUNNER_SECURITY_SUITES)
            and set(suites) == set(APT_RUNNER_SECURITY_SUITES)
        )
        component_diagnostics = _component_field_diagnostics(row.get("components", ""))
        exact_field_set = set(row) == APT_RUNNER_SOURCE_FIELDS
        exact_types = row.get("types", "").split() == ["deb"]
        exact_mirror_uri = uris == [APT_MIRROR_URI]
        exact_components = component_diagnostics["exact_set"]
        exact_signed_by = row.get("signed-by") == str(UBUNTU_ARCHIVE_KEYRING)
        component_order_counts[component_diagnostics["order_class"]] += 1
        component_unknown_total += component_diagnostics["unknown_component_count"]
        component_duplicate_total += component_diagnostics["duplicate_component_count"]
        if len(component_stanzas) < 2:
            component_stanzas.append(component_diagnostics)
        exact_field_set_count += int(exact_field_set)
        exact_types_count += int(exact_types)
        exact_mirror_uri_count += int(exact_mirror_uri)
        exact_components_count += int(exact_components)
        exact_signed_by_count += int(exact_signed_by)
        if archive_suites:
            archive_count += 1
        elif security_suites:
            security_count += 1
        else:
            unknown_suite_count += 1
        if not (exact_field_set and exact_types and exact_mirror_uri and exact_components
                and exact_signed_by
                and (archive_suites or security_suites)):
            invalid_count += 1
    expected_layout = (
        len(records) == 2 and archive_count == 1 and security_count == 1
        and unknown_suite_count == 0 and exact_field_set_count == 2
        and exact_types_count == 2 and exact_mirror_uri_count == 2
        and exact_components_count == 2 and exact_signed_by_count == 2
        and invalid_count == 0
    )
    return {
        "expected_format": APT_RUNNER_SOURCE_FORMAT,
        "source_file_sha256": hashlib.sha256(source_file).hexdigest(),
        "source_record_count": min(len(records), 99),
        "source_record_count_capped": len(records) > 99,
        "mirror_source_stanza_count": min(mirror_count, 99),
        "mirror_source_stanza_count_capped": mirror_count > 99,
        "exact_allowed_field_set_stanza_count": exact_field_set_count,
        "exact_types_deb_stanza_count": exact_types_count,
        "exact_mirror_uri_stanza_count": exact_mirror_uri_count,
        "exact_component_set_stanza_count": exact_components_count,
        "component_order_class_counts": component_order_counts,
        "component_unknown_token_count": min(component_unknown_total, 99),
        "component_unknown_token_count_capped": component_unknown_total > 99,
        "component_duplicate_token_count": min(component_duplicate_total, 99),
        "component_duplicate_token_count_capped": component_duplicate_total > 99,
        "component_stanza_diagnostics": component_stanzas,
        "component_stanza_diagnostics_capped": len(records) > len(component_stanzas),
        "exact_archive_keyring_stanza_count": exact_signed_by_count,
        "archive_suite_stanza_count": archive_count,
        "security_suite_stanza_count": security_count,
        "unknown_suite_group_stanza_count": unknown_suite_count,
        "invalid_stanza_count": min(invalid_count, 99),
        "invalid_stanza_count_capped": invalid_count > 99,
        "missing_expected_stanza_count": min(max(0, 2 - len(records)), 2),
        "unexpected_stanza_count": min(max(0, len(records) - 2), 99),
        "recognized_layout": "noble_archive_security_split" if expected_layout else "unrecognized",
    }


def _validate_runner_mirror_configuration(source_file: bytes,
                                          mirrorlist: bytes) -> dict[str, Any]:
    try:
        source_text = source_file.decode("utf-8", "strict")
    except UnicodeError as exc:
        raise SetupFailure("ubuntu_runner_sources_configuration_invalid", details={
            "expected_format": APT_RUNNER_SOURCE_FORMAT,
            "source_file_sha256": hashlib.sha256(source_file).hexdigest(),
            "source_decode_error": True,
        }) from exc
    records = _parse_deb822_records(source_text, failure_code="ubuntu_runner_sources_configuration_invalid",
                                    max_bytes=64 * 1024)
    diagnostics = _runner_source_layout_diagnostics(records, source_file)
    if diagnostics["recognized_layout"] != "noble_archive_security_split":
        raise SetupFailure("ubuntu_runner_sources_configuration_untrusted", details=diagnostics)
    mirrors = _parse_runner_mirrorlist(mirrorlist)
    return {
        "transport": "github_ubuntu_runner_root_owned_mirrorlist",
        "source_file_sha256": hashlib.sha256(source_file).hexdigest(),
        "source_layout": diagnostics["recognized_layout"],
        "source_record_count": diagnostics["source_record_count"],
        "mirror_source_stanza_count": diagnostics["mirror_source_stanza_count"],
        "mirrorlist_sha256": hashlib.sha256(mirrorlist).hexdigest(),
        "mirror_uri_count": len(mirrors),
        "mirror_hosts": [urlparse(value).hostname for value in mirrors],
        "signed_by": "ubuntu_archive_keyring_package",
        "_source_uri": APT_MIRROR_URI,
        "_mirror_uris": mirrors,
    }


def _read_root_owned_file(path: Path, *, limit: int, failure_code: str,
                          missing_ok: bool = False) -> bytes | None:
    """Read a fixed system trust file without following links or accepting writable custody."""
    current = Path("/")
    try:
        for part in path.parts[1:-1]:
            current = current / part
            parent = current.lstat()
            if (not stat.S_ISDIR(parent.st_mode) or stat.S_ISLNK(parent.st_mode)
                    or parent.st_uid != 0 or parent.st_mode & 0o022):
                raise SetupFailure(failure_code)
        before = path.lstat()
        if (not stat.S_ISREG(before.st_mode) or stat.S_ISLNK(before.st_mode)
                or before.st_uid != 0 or before.st_gid != 0 or before.st_mode & 0o022
                or before.st_size > limit):
            raise SetupFailure(failure_code)
        if not hasattr(os, "O_NOFOLLOW"):
            raise SetupFailure(failure_code)
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW
        descriptor = os.open(str(path), flags)
        try:
            opened = os.fstat(descriptor)
            if (not stat.S_ISREG(opened.st_mode) or opened.st_uid != 0
                    or opened.st_gid != 0 or opened.st_mode & 0o022
                    or (opened.st_dev, opened.st_ino) !=
                    (before.st_dev, before.st_ino)):
                raise SetupFailure(failure_code)
            chunks: list[bytes] = []
            remaining = limit + 1
            while remaining:
                chunk = os.read(descriptor, min(64 * 1024, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            data = b"".join(chunks)
            after = os.fstat(descriptor)
            if (len(data) > limit or (opened.st_dev, opened.st_ino, opened.st_size,
                                     opened.st_mtime_ns, opened.st_ctime_ns, opened.st_uid,
                                     opened.st_gid, opened.st_mode) !=
                    (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns,
                     after.st_ctime_ns, after.st_uid, after.st_gid, after.st_mode)):
                raise SetupFailure(failure_code)
            return data
        finally:
            os.close(descriptor)
    except SetupFailure:
        raise
    except FileNotFoundError as exc:
        if missing_ok:
            return None
        raise SetupFailure(failure_code) from exc
    except OSError as exc:
        raise SetupFailure(failure_code) from exc


def _runner_mirror_configuration() -> dict[str, Any]:
    source_file = _read_root_owned_file(
        UBUNTU_SOURCES_PATH, limit=64 * 1024,
        failure_code="ubuntu_runner_sources_configuration_unavailable")
    mirrorlist = _read_root_owned_file(
        APT_MIRRORLIST_PATH, limit=4096,
        failure_code="ubuntu_runner_apt_mirrorlist_unavailable")
    evidence = _validate_runner_mirror_configuration(source_file, mirrorlist)
    keyring = _read_root_owned_file(
        UBUNTU_ARCHIVE_KEYRING, limit=1024 * 1024,
        failure_code="ubuntu_archive_keyring_unavailable")
    if not keyring:
        raise SetupFailure("ubuntu_archive_keyring_unverified")
    if _owner_of(str(UBUNTU_ARCHIVE_KEYRING)) != "ubuntu-keyring":
        raise SetupFailure("ubuntu_archive_keyring_package_owner_mismatch")
    _verify_package("ubuntu-keyring")
    evidence["ubuntu_archive_keyring_sha256"] = hashlib.sha256(keyring).hexdigest()
    evidence["ubuntu_archive_keyring_package_owner"] = "ubuntu-keyring"
    evidence["ubuntu_archive_keyring_dpkg_verify"] = "clean"
    return evidence


def _is_pinned_package_index_target(row: dict[str, str]) -> bool:
    return (row.get("identifier") == "Packages"
            and row.get("metakey") == "main/binary-amd64/Packages"
            and row.get("target-of") == "deb")


def _apt_index_target_diagnostics(text: str, source: str,
                                  transport: dict[str, Any]) -> dict[str, Any]:
    try:
        records = _parse_deb822_records(text, failure_code="apt_signed_release_target_record_invalid")
    except SetupFailure:
        records = []
    source_uri = source.split()[0] if source.split() else ""
    candidate_repositories: set[str] = set()
    canonical_source = _canonical_archive_uri(source_uri)
    if canonical_source:
        candidate_repositories.add(canonical_source)
    elif source_uri == transport.get("_source_uri"):
        candidate_repositories.add(source_uri)
        candidate_repositories.update(
            value for value in transport.get("_mirror_uris", ())
            if isinstance(value, str) and _canonical_archive_uri(value))
    candidates = [row for row in records if _is_pinned_package_index_target(row)]
    matches = [row for row in candidates
               if row.get("architecture") == "amd64"
               and row.get("component") == "main"
               and row.get("suite") == "noble-updates"
               and row.get("repo-uri", "").rstrip("/") in
               {value.rstrip("/") for value in candidate_repositories}]
    row = candidates[0] if len(candidates) == 1 else {}
    unrelated = [record for record in records
                 if not _is_pinned_package_index_target(record)]
    unsupported_field_record_count = sum(
        bool(set(record) - APT_INDEX_TARGET_FIELDS) for record in records
    )
    unresolved_candidate_fields = sum(
        "$(" in value
        for candidate in candidates for value in candidate.values()
    )
    # APT 2.8.3 can leave a format variable in an unrelated Translation target
    # because that target has no Architecture option. Count this safely without
    # treating the unrelated record as part of the pinned Packages decision.
    unrelated_unresolved_count = sum(
        any("$(" in value for value in record.values()) for record in unrelated
    )
    trusted = row.get("trusted", "").lower()
    origin = row.get("origin", "")
    label = row.get("label", "")
    suite = row.get("suite", "")
    codename = row.get("codename", "")
    signer_binding = (
        "root_owned_source_stanza"
        if transport.get("signed_by") == "ubuntu_archive_keyring_package"
        and re.fullmatch(r"[0-9a-f]{64}", str(transport.get("source_file_sha256", "")))
        and re.fullmatch(r"[0-9a-f]{64}", str(transport.get("ubuntu_archive_keyring_sha256", "")))
        else "missing_or_unverified"
    )
    return {
        "target_record_count": min(len(records), 99),
        "target_record_count_capped": len(records) > 99,
        "package_candidate_count": min(len(candidates), 99),
        "package_candidate_count_capped": len(candidates) > 99,
        "matching_ubuntu_updates_target_count": min(len(matches), 99),
        "matching_ubuntu_updates_target_count_capped": len(matches) > 99,
        "unrelated_target_record_count": min(len(unrelated), 99),
        "unrelated_target_record_count_capped": len(unrelated) > 99,
        "unrelated_unresolved_variable_record_count": min(unrelated_unresolved_count, 99),
        "unrelated_unresolved_variable_record_count_capped": unrelated_unresolved_count > 99,
        "unsupported_field_record_count": min(unsupported_field_record_count, 99),
        "unsupported_field_record_count_capped": unsupported_field_record_count > 99,
        "candidate_field_set_exact": set(row) == APT_INDEX_TARGET_FIELDS if row else False,
        "candidate_unexpanded_field_count": min(unresolved_candidate_fields, 99),
        "candidate_unexpanded_field_count_capped": unresolved_candidate_fields > 99,
        "trusted_state": trusted if trusted in {"yes", "no"} else ("other" if trusted else "missing"),
        "release_origin": "Ubuntu" if origin == "Ubuntu" else ("other" if origin else "missing"),
        "release_label": "Ubuntu" if label == "Ubuntu" else ("other" if label else "missing"),
        "release_suite": "noble-updates" if suite == "noble-updates" else ("other" if suite else "missing"),
        "release_codename": "noble" if codename == "noble" else ("other" if codename else "missing"),
        "source_keyring_binding": signer_binding,
        "apt_package_version": str(transport.get("apt_package_version", "unavailable")),
        "repo_transport": (
            "runner_mirrorlist" if row.get("repo-uri", "").rstrip("/") == APT_MIRROR_URI
            else ("ubuntu_archive_endpoint" if row else "missing_or_unrecognized")
        ),
    }


def _validate_signed_apt_index_target(text: str, source: str,
                                      transport: dict[str, Any]) -> dict[str, str]:
    diagnostics = _apt_index_target_diagnostics(text, source, transport)
    try:
        records = _parse_deb822_records(
            text, failure_code="apt_signed_release_target_record_invalid",
            allow_comments=False,
        )
    except SetupFailure as exc:
        exc.details = {"apt_release_target_diagnostics": diagnostics}
        raise
    if any(set(row) - APT_INDEX_TARGET_FIELDS for row in records):
        raise SetupFailure("apt_signed_release_target_record_fields_invalid",
                           details={"apt_release_target_diagnostics": diagnostics})
    # First identify the one pinned package-index record using stable selector
    # fields. APT's supported --format output also includes unrelated target
    # kinds (for example Translation-en); missing substitutions in those rows
    # must not be mistaken for defects in the signed Packages target.
    candidates = [row for row in records if _is_pinned_package_index_target(row)]
    if len(candidates) != 1:
        raise SetupFailure("apt_signed_release_target_missing_or_ambiguous",
                           details={"apt_release_target_diagnostics": diagnostics})
    row = candidates[0]
    if set(row) != APT_INDEX_TARGET_FIELDS:
        raise SetupFailure("apt_signed_release_target_record_fields_invalid",
                           details={"apt_release_target_diagnostics": diagnostics})
    if any("$(" in value for value in row.values()):
        raise SetupFailure("apt_signed_release_target_unexpanded_format_variable",
                           details={"apt_release_target_diagnostics": diagnostics})
    if diagnostics["source_keyring_binding"] != "root_owned_source_stanza":
        raise SetupFailure("apt_signed_release_source_keyring_binding_missing",
                           details={"apt_release_target_diagnostics": diagnostics})
    source_uri = source.split()[0] if source.split() else ""
    allowed_repositories: set[str] = set()
    canonical_source = _canonical_archive_uri(source_uri)
    if canonical_source:
        allowed_repositories.add(canonical_source)
    elif source_uri == transport.get("_source_uri"):
        allowed_repositories.add(source_uri)
        allowed_repositories.update(
            value for value in transport.get("_mirror_uris", ())
            if isinstance(value, str) and _canonical_archive_uri(value))
    if (row.get("architecture") != "amd64"
            or row.get("component") != "main"
            or row.get("suite") != "noble-updates"
            or row.get("repo-uri", "").rstrip("/") not in
            {value.rstrip("/") for value in allowed_repositories}):
        raise SetupFailure("apt_signed_release_target_missing_or_ambiguous",
                           details={"apt_release_target_diagnostics": diagnostics})
    repository_uri = row["repo-uri"].rstrip("/")
    expected_index_uri = (
        repository_uri + "/dists/noble-updates/" + row["metakey"]
    )
    if (row.get("codename") != "noble" or row.get("origin") != "Ubuntu"
            or row.get("label") != "Ubuntu" or row.get("trusted") != "yes"
            or not row.get("uri") or row.get("uri") != expected_index_uri):
        raise SetupFailure("apt_signed_release_metadata_not_ubuntu_noble_updates",
                           details={"apt_release_target_diagnostics": diagnostics})
    return {
        "target": "Packages/main/binary-amd64",
        "origin": row["origin"], "label": row["label"],
        "suite": row["suite"], "codename": row["codename"],
        "component": row["component"], "architecture": row["architecture"],
        "trusted": row["trusted"],
        "signed_by": str(UBUNTU_ARCHIVE_KEYRING),
        "signer_binding": "root_owned_source_stanza",
        "apt_package_version": str(transport.get("apt_package_version", "unavailable")),
        "transport": diagnostics["repo_transport"],
    }


def validate_apt_record(text: str, package: str, version: str,
                        sha256: str, *, source_text: str) -> dict[str, str]:
    diagnostics = _candidate_record_diagnostics(text, source_text, package, version)
    try:
        exact = _parse_deb822_records(
            text, failure_code="apt_package_record_invalid", max_bytes=128 * 1024,
            allow_comments=False, reject_continued_fields=APT_PACKAGE_IDENTITY_FIELDS,
        )
        source_records = _parse_deb822_records(
            source_text, failure_code="apt_package_source_record_invalid", max_bytes=128 * 1024,
            allow_comments=False, reject_continued_fields=APT_PACKAGE_IDENTITY_FIELDS,
        )
    except SetupFailure as exc:
        details = diagnostics
        details["record_parse_status"] = "invalid"
        raise SetupFailure(exc.code, details=details) from exc
    matches = [row for row in exact
               if _apt_package_matches(row.get("package", ""), package)
               and row.get("version") == version]
    if len(matches) != 1:
        raise SetupFailure("exact_signed_apt_package_record_missing_or_ambiguous",
                           details=diagnostics)
    row = matches[0]
    expected_architecture = "all" if package == PROFILE_PACKAGE else "amd64"
    if (row.get("architecture") != expected_architecture
            or not re.fullmatch(r"[0-9a-f]{64}", row.get("sha256", ""))
            or (sha256 and row.get("sha256") != sha256)):
        raise SetupFailure("signed_apt_package_identity_or_digest_mismatch",
                           details=diagnostics)
    source_matches = [candidate for candidate in source_records
                      if _apt_package_matches(candidate.get("package", ""), package)
                      and candidate.get("version") == version]
    if len(source_matches) != 1:
        raise SetupFailure("exact_signed_apt_source_record_missing_or_ambiguous",
                           details=diagnostics)
    source_row = source_matches[0]
    if (source_row.get("filename") != row.get("filename")
            or source_row.get("sha256") != row.get("sha256")
            or (source_row.get("architecture") is not None
                and source_row.get("architecture") != row.get("architecture"))):
        raise SetupFailure("signed_apt_source_record_metadata_mismatch", details=diagnostics)
    source = source_row.get("apt-sources", "")
    if not source:
        raise SetupFailure("signed_apt_package_origin_missing", details=diagnostics)
    try:
        _validated_apt_source(source)
    except SetupFailure as exc:
        exc.details = diagnostics
        raise
    filename = row.get("filename", "")
    expected_prefix = ("pool/main/a/apparmor/" if package == PROFILE_PACKAGE
                       else "pool/main/b/bubblewrap/")
    if not filename.startswith(expected_prefix):
        raise SetupFailure("signed_apt_package_filename_mismatch", details=diagnostics)
    result = {external: row[internal] for external, internal in (
        ("Package", "package"), ("Version", "version"),
        ("Architecture", "architecture"), ("Filename", "filename"),
        ("SHA256", "sha256"),
    )}
    result["APT-Sources"] = source
    return result


def _package_record(package: str, version: str, sha256: str, *,
                    apt_transport: dict[str, Any]) -> dict[str, Any]:
    # APT's default show format (v1) preserves the package-index fields we pin,
    # but does not synthesize APT-Sources. Request v2 separately for provenance.
    # In APT 2.7.14, -f/--full is registered only for `search`; `show` gets the
    # same full-record behavior through the generic configuration option.
    output, _ = _command([APT_CACHE_PATH, "-o", "APT::Cache::ShowFull=true",
                          "-o", "APT::Cache::Show::Version=1", "show",
                          package + "=" + version], limit=128 * 1024,
                         apt_phase="metadata_v1")
    try:
        text = output.decode("utf-8")
    except UnicodeError as exc:
        raise SetupFailure("signed_apt_package_record_encoding_invalid") from exc
    try:
        source_output, _ = _command([APT_CACHE_PATH, "-o", "APT::Cache::ShowFull=true",
                                     "-o", "APT::Cache::Show::Version=2", "show",
                                     package + "=" + version], limit=128 * 1024,
                                    apt_phase="source_v2")
    except SetupFailure as exc:
        diagnostics = _candidate_record_diagnostics(text, None, package, version)
        if exc.details.get("apt_command"):
            diagnostics["apt_command"] = exc.details["apt_command"]
        exc.details = diagnostics
        raise
    try:
        source_text = source_output.decode("utf-8")
    except UnicodeError as exc:
        raise SetupFailure("signed_apt_package_source_record_encoding_invalid") from exc
    record = validate_apt_record(text, package, version, sha256, source_text=source_text)
    try:
        target_output, _ = _command(
            [APT_GET_PATH, "-o", "APT::Get::IndexTargets::ReleaseInfo=true",
             "indextargets", "--format", APT_INDEXTARGETS_FORMAT],
            limit=APT_INDEX_TARGETS_LIMIT, apt_phase="index_targets")
    except SetupFailure as exc:
        diagnostics = _candidate_record_diagnostics(text, source_text, package, version)
        diagnostics["apt_command"] = exc.details.get(
            "apt_command", _apt_command_diagnostics("index_targets", "unclassified_failure"))
        exc.details = diagnostics
        raise
    try:
        target_text = target_output.decode("utf-8", "strict")
    except UnicodeError as exc:
        raise SetupFailure("apt_signed_release_target_encoding_invalid") from exc
    try:
        release_evidence = _validate_signed_apt_index_target(
            target_text, record["APT-Sources"], apt_transport)
    except SetupFailure as exc:
        exc.details["candidate_diagnostics"] = _candidate_record_diagnostics(
            text, source_text, package, version)
        raise
    record["_release_evidence"] = release_evidence
    return record


def parse_dpkg_deb_fields(output: str) -> dict[str, str]:
    """Parse dpkg-deb -f's labelled fields without depending on control order."""
    allowed = {"Package", "Version", "Architecture"}
    fields: dict[str, str] = {}
    for line in output.splitlines():
        name, separator, value = line.partition(":")
        if not separator or name not in allowed or name in fields:
            raise SetupFailure("downloaded_profile_package_metadata_invalid")
        normalized = value.strip()
        if not normalized:
            raise SetupFailure("downloaded_profile_package_metadata_invalid")
        fields[name] = normalized
    if set(fields) != allowed:
        raise SetupFailure("downloaded_profile_package_metadata_invalid")
    return fields


def validate_profile_deb_metadata(output: bytes) -> dict[str, str]:
    try:
        text = output.decode("ascii", errors="strict")
    except UnicodeError as exc:
        raise SetupFailure("downloaded_profile_package_metadata_invalid") from exc
    fields = parse_dpkg_deb_fields(text)
    if fields != {"Package": PROFILE_PACKAGE, "Version": PROFILE_VERSION, "Architecture": "all"}:
        raise SetupFailure("downloaded_profile_package_metadata_mismatch")
    return fields


def _public_apt_source(source: str) -> str:
    """Return useful source identity while omitting URL credentials and paths."""
    tokens = source.split()
    if tokens and tokens[0] == APT_MIRROR_URI:
        return "mirror+file:<verified Ubuntu runner mirrorlist> " + " ".join(tokens[1:])
    parsed = urlparse(tokens[0])
    host = (parsed.hostname or "").lower()
    return f"{parsed.scheme.lower()}://{host}/[path-redacted] {' '.join(tokens[1:])}"


def _apt_boolean_value(value: str) -> bool | None:
    """Mirror APT 2.7/2.8 StringToBool spellings; None means invalid/unknown."""
    normalized = value.strip().casefold()
    if normalized in {"yes", "true", "with", "on", "enable"}:
        return True
    if normalized in {"no", "false", "without", "off", "disable"}:
        return False
    numeric = re.fullmatch(r"([+-]?)(0[xX][0-9a-f]+|0[0-7]*|[1-9][0-9]*)", normalized)
    if not numeric:
        return None
    sign, digits = numeric.groups()
    if digits.startswith("0x"):
        number = int(digits[2:], 16)
    elif len(digits) > 1 and digits.startswith("0"):
        number = int(digits, 8)
    else:
        number = int(digits, 10)
    if sign == "-":
        number = -number
    return bool(number) if number in {0, 1} else None


def _check_apt_security_boolean(name: str, value: str, policy: str) -> None:
    parsed = _apt_boolean_value(value)
    if parsed is None:
        raise SetupFailure("apt_security_option_value_unrecognized")
    if policy == "reject_true" and parsed:
        raise SetupFailure("apt_source_configuration_bypasses_signature_checks")
    if policy == "reject_false" and not parsed:
        raise SetupFailure("apt_source_configuration_disables_freshness_checks")


def _parse_apt_config_dump(text: str) -> tuple[dict[str, str], dict[str, str]]:
    if len(text) > 256 * 1024 or "\x00" in text:
        raise SetupFailure("apt_security_configuration_invalid")
    booleans: dict[str, str] = {}
    paths: dict[str, str] = {}
    path_keys = {"dir", "dir::etc", "dir::etc::sourcelist", "dir::etc::sourceparts"}
    assignment = re.compile(r'^\s*([^\s;{}]+)\s+(?:"([^"\\]*)"|([^\s;]+))\s*;\s*$')
    for line in text.splitlines():
        if len(line) > 4096 or "\x00" in line:
            raise SetupFailure("apt_security_configuration_invalid")
        if not line.strip():
            continue
        match = assignment.fullmatch(line)
        if not match:
            first = re.match(r"^\s*([^\s;{}]+)", line)
            key = first.group(1).casefold() if first else ""
            if key in APT_SECURITY_CONFIG_KEYS or key in path_keys:
                raise SetupFailure("apt_security_configuration_invalid")
            continue
        key = match.group(1).casefold()
        value = match.group(2) if match.group(2) is not None else match.group(3)
        if key in APT_SECURITY_CONFIG_KEYS:
            if key in booleans:
                raise SetupFailure("apt_security_configuration_duplicate_option")
            _check_apt_security_boolean(key, value, APT_SECURITY_CONFIG_KEYS[key])
            booleans[key] = value
        elif key in path_keys:
            if key in paths:
                raise SetupFailure("apt_security_configuration_duplicate_option")
            paths[key] = value

    apt_root = paths.get("dir", "/")
    if posixpath.normpath(apt_root) != "/":
        raise SetupFailure("apt_source_configuration_path_unverified")
    etc_value = paths.get("dir::etc", "etc/apt")
    etc_path = posixpath.normpath(
        etc_value if posixpath.isabs(etc_value) else posixpath.join(apt_root, etc_value)
    )
    if etc_path != "/etc/apt":
        raise SetupFailure("apt_source_configuration_path_unverified")
    source_list = paths.get("dir::etc::sourcelist", "sources.list")
    source_parts = paths.get("dir::etc::sourceparts", "sources.list.d")
    source_list_path = posixpath.normpath(
        source_list if posixpath.isabs(source_list) else posixpath.join(etc_path, source_list)
    )
    source_parts_path = posixpath.normpath(
        source_parts if posixpath.isabs(source_parts) else posixpath.join(etc_path, source_parts)
    )
    if (source_list_path != str(APT_SOURCES_LIST_PATH)
            or source_parts_path != str(APT_SOURCES_PARTS_PATH)):
        raise SetupFailure("apt_source_configuration_path_unverified")
    return booleans, paths


def _validate_source_boolean_fields(records: list[dict[str, str]]) -> None:
    for record in records:
        for name, policy in APT_SECURITY_BOOLEAN_OPTIONS.items():
            if name in record:
                _check_apt_security_boolean(name, record[name], policy)


def _parse_one_line_source_security(text: str) -> None:
    if len(text) > APT_SOURCE_FILE_BYTES or "\x00" in text:
        raise SetupFailure("apt_source_configuration_invalid")
    entry = re.compile(r"^(?:deb|deb-src)(?:\s+\[([^\[\]]*)\])?\s+\S+\s+\S+(?:\s+.*)?$")
    for line in text.splitlines():
        if len(line) > 8192:
            raise SetupFailure("apt_source_configuration_invalid")
        uncommented = line.split("#", 1)[0].strip()
        if not uncommented:
            continue
        match = entry.fullmatch(uncommented)
        if not match:
            raise SetupFailure("apt_source_configuration_invalid")
        option_text = match.group(1)
        if option_text is None:
            if "[" in uncommented or "]" in uncommented:
                raise SetupFailure("apt_source_configuration_invalid")
            continue
        try:
            options = shlex.split(option_text, comments=False, posix=True)
        except ValueError as exc:
            raise SetupFailure("apt_source_configuration_invalid") from exc
        seen: set[str] = set()
        for option in options:
            parsed = re.fullmatch(r"([a-z][a-z0-9-]*)([+\-]?=)(.*)", option, re.IGNORECASE)
            if not parsed:
                if any(option.casefold().startswith(name) for name in APT_SECURITY_BOOLEAN_OPTIONS):
                    raise SetupFailure("apt_source_configuration_invalid")
                continue
            name, operator, value = parsed.groups()
            name = name.casefold()
            if name not in APT_SECURITY_BOOLEAN_OPTIONS:
                continue
            if name in seen:
                raise SetupFailure("apt_security_configuration_duplicate_option")
            seen.add(name)
            if operator != "=":
                raise SetupFailure("apt_source_configuration_invalid")
            _check_apt_security_boolean(name, value, APT_SECURITY_BOOLEAN_OPTIONS[name])


def _validate_apt_security_configuration(text: str,
                                         source_files: dict[str, str]) -> dict[str, Any]:
    booleans, _ = _parse_apt_config_dump(text)
    for name, source_text in source_files.items():
        if len(source_text) > APT_SOURCE_FILE_BYTES or "\x00" in source_text:
            raise SetupFailure("apt_source_configuration_invalid")
        if name.endswith(".sources"):
            records = _parse_deb822_records(
                source_text, failure_code="apt_source_configuration_invalid",
                max_bytes=APT_SOURCE_FILE_BYTES,
            )
            _validate_source_boolean_fields(records)
        elif name == "sources.list" or name.endswith(".list"):
            _parse_one_line_source_security(source_text)
        else:
            raise SetupFailure("apt_source_configuration_file_type_unrecognized")
    return {
        "signed_repository_checks_enabled": True,
        "freshness_checks_enabled": True,
        "security_boolean_options_checked": len(APT_SECURITY_CONFIG_KEYS),
        "source_control_files_checked": len(source_files),
        "source_configuration_paths": "default_etc_apt",
        "unsafe_overrides": [],
    }


def _read_apt_source_files() -> dict[str, str]:
    files: dict[str, str] = {}
    total_bytes = 0
    source_list = _read_root_owned_file(
        APT_SOURCES_LIST_PATH, limit=APT_SOURCE_FILE_BYTES,
        failure_code="apt_source_configuration_unreadable", missing_ok=True,
    )
    if source_list is not None:
        total_bytes += len(source_list)
        try:
            files["sources.list"] = source_list.decode("utf-8", "strict")
        except UnicodeError as exc:
            raise SetupFailure("apt_source_configuration_invalid") from exc

    try:
        directory = APT_SOURCES_PARTS_PATH.lstat()
    except FileNotFoundError:
        directory = None
    except OSError as exc:
        raise SetupFailure("apt_source_configuration_unreadable") from exc
    if directory is not None:
        if (not stat.S_ISDIR(directory.st_mode) or stat.S_ISLNK(directory.st_mode)
                or directory.st_uid != 0 or directory.st_gid != 0
                or directory.st_mode & 0o022):
            raise SetupFailure("apt_source_configuration_directory_unverified")
        try:
            names: list[str] = []
            entries_seen = 0
            with os.scandir(APT_SOURCES_PARTS_PATH) as entries:
                for entry in entries:
                    entries_seen += 1
                    if entries_seen > APT_SOURCE_DIRECTORY_ENTRY_LIMIT:
                        raise SetupFailure("apt_source_configuration_directory_limit_exceeded")
                    if entry.name.endswith((".list", ".sources")):
                        names.append(entry.name)
                        if len(names) > APT_SOURCE_FILE_LIMIT:
                            raise SetupFailure("apt_source_configuration_file_limit_exceeded")
        except SetupFailure:
            raise
        except OSError as exc:
            raise SetupFailure("apt_source_configuration_unreadable") from exc
        for name in sorted(names):
            path = APT_SOURCES_PARTS_PATH / name
            content = _read_root_owned_file(
                path, limit=APT_SOURCE_FILE_BYTES,
                failure_code="apt_source_configuration_unreadable",
            )
            assert content is not None
            total_bytes += len(content)
            if total_bytes > APT_SOURCE_TOTAL_BYTES:
                raise SetupFailure("apt_source_configuration_byte_limit_exceeded")
            try:
                files[name] = content.decode("utf-8", "strict")
            except UnicodeError as exc:
                raise SetupFailure("apt_source_configuration_invalid") from exc
    if total_bytes > APT_SOURCE_TOTAL_BYTES:
        raise SetupFailure("apt_source_configuration_byte_limit_exceeded")
    return files


def _apt_signature_configuration() -> dict[str, Any]:
    if "APT_CONFIG" in os.environ:
        raise SetupFailure("external_apt_configuration_override_unverified")
    output, _ = _command([APT_CONFIG_PATH, "dump"], limit=256 * 1024)
    try:
        text = output.decode("utf-8", errors="strict")
    except UnicodeError as exc:
        raise SetupFailure("apt_security_configuration_invalid") from exc
    # Reject an unsafe effective value before touching host source files; the
    # source scan below then verifies every file APT can load from the fixed
    # source-list locations.
    _parse_apt_config_dump(text)
    return _validate_apt_security_configuration(text, _read_apt_source_files())


def _read_profiles() -> list[str]:
    try:
        with APPARMOR_PROFILES.open("rb") as source:
            profile_bytes = source.read(PROFILE_TABLE_MAX_BYTES + 1)
    except PermissionError as exc:
        # Some Ubuntu runner images expose securityfs profile state to root only.
        # Elevate only a fixed, bounded head read of this constant kernel path.
        # The profile contents never become a command, shell fragment, or policy input.
        try:
            profile_bytes, _ = _command(
                ["/usr/bin/sudo", "-n", "--", "/usr/bin/head", "-c",
                 str(PROFILE_TABLE_MAX_BYTES + 1), "--", str(APPARMOR_PROFILES)],
                timeout=5, limit=PROFILE_TABLE_MAX_BYTES, cwd=Path("/"))
        except SetupFailure as privileged_error:
            code = ("apparmor_kernel_profile_status_exceeded_bound"
                    if privileged_error.code == "command_output_exceeded_bound"
                    else "apparmor_kernel_profile_status_privileged_read_failed")
            raise SetupFailure(code) from privileged_error
    except OSError as exc:
        raise SetupFailure("apparmor_kernel_profile_status_unavailable") from exc
    if len(profile_bytes) > PROFILE_TABLE_MAX_BYTES:
        raise SetupFailure("apparmor_kernel_profile_status_exceeded_bound")
    try:
        rows = profile_bytes.decode("utf-8").splitlines()
    except UnicodeError as exc:
        raise SetupFailure("apparmor_kernel_profile_status_encoding_invalid") from exc
    return sorted(line.strip() for line in rows if line.strip())


def _profile_name(line: str) -> str:
    try:
        name, _mode = line.rsplit(" ", 1)
    except ValueError:
        return line
    return name


def _state_snapshot(*, allow_designated_profiles: bool = False) -> dict[str, Any]:
    try:
        enabled = Path("/sys/module/apparmor/parameters/enabled").read_text(encoding="ascii").strip()
        lsm = Path("/sys/kernel/security/lsm").read_text(encoding="ascii").strip()
    except (OSError, UnicodeError) as exc:
        raise SetupFailure("apparmor_global_state_unavailable") from exc
    if enabled.lower() not in {"y", "yes", "1"} or "apparmor" not in lsm.split(","):
        raise SetupFailure("apparmor_is_not_enabled_in_enforcing_kernel")
    sysctls: dict[str, str] = {}
    for name in RELEVANT_SYSCTLS:
        path = Path("/proc/sys/kernel") / name
        if path.exists():
            try:
                sysctls["kernel." + name] = path.read_text(encoding="ascii").strip()
            except (OSError, UnicodeError) as exc:
                raise SetupFailure("apparmor_restriction_sysctl_unreadable") from exc
    if sysctls.get("kernel.apparmor_restrict_unprivileged_userns") != "1":
        raise SetupFailure("global_unprivileged_userns_restriction_is_not_enabled")
    profiles = _read_profiles()
    related = [line for line in profiles if "bwrap" in _profile_name(line).lower()]
    expected = {"bwrap (enforce)", "unpriv_bwrap (enforce)"} if allow_designated_profiles else set()
    if set(related) != expected:
        raise SetupFailure("conflicting_bubblewrap_apparmor_profile_already_loaded")
    return {
        "apparmor_enabled": enabled,
        "kernel_lsm": lsm,
        "sysctls": sysctls,
        "profile_count": len(profiles),
        "profile_table_sha256": hashlib.sha256("\n".join(profiles).encode("utf-8")).hexdigest(),
        "profile_table": profiles,
    }


def _public_state(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in snapshot.items() if key != "profile_table"}


def _check_local_overrides() -> list[str]:
    states: list[str] = []
    for path in LOCAL_OVERRIDES:
        try:
            path.lstat()
        except FileNotFoundError:
            states.append("absent")
            continue
        except OSError as exc:
            raise SetupFailure("apparmor_local_override_state_unreadable") from exc
        raise SetupFailure("unreviewed_apparmor_local_override_present")
    return states


def _extract_profile(deb: Path, destination: Path) -> None:
    try:
        producer = subprocess.Popen(["dpkg-deb", "--fsys-tarfile", str(deb)],
                                    stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                    stderr=subprocess.DEVNULL, close_fds=True)
        assert producer.stdout is not None
        consumer = subprocess.Popen(["tar", "-xOf", "-", PROFILE_MEMBER],
                                    stdin=producer.stdout, stdout=subprocess.PIPE,
                                    stderr=subprocess.DEVNULL, close_fds=True)
        producer.stdout.close()
        output, _ = consumer.communicate(timeout=20)
        producer_code = producer.wait(timeout=5)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SetupFailure("profile_only_package_extraction_failed") from exc
    if consumer.returncode != 0 or producer_code != 0 or len(output) > 256 * 1024:
        raise SetupFailure("profile_only_package_extraction_failed")
    if hashlib.sha256(output).hexdigest() != PROFILE_SHA256:
        raise SetupFailure("extracted_profile_digest_mismatch")
    destination.write_bytes(output)


def _version(command: list[str], pattern: str) -> str:
    output, _ = _command(command, limit=8192)
    first_line = output.decode("utf-8", errors="strict").splitlines()[0]
    match = re.fullmatch(pattern, first_line.strip())
    if not match:
        raise SetupFailure("installed_tool_version_unrecognized")
    return match.group(1)


def _package_version(name: str) -> str:
    output, _ = _command(["dpkg-query", "-W", "-f=${Version}", name], limit=8192)
    return output.decode("ascii", errors="strict").strip()


def _apt_toolchain_evidence() -> dict[str, str]:
    for command, expected_path in (
        ("apt-get", APT_GET_PATH), ("apt-cache", APT_CACHE_PATH),
        ("apt-config", APT_CONFIG_PATH),
    ):
        resolved = shutil.which(command)
        if not resolved or os.path.realpath(resolved) != expected_path:
            raise SetupFailure("apt_system_tool_path_unverified")
        owner = _owner_of(expected_path).split(":", 1)[0]
        if owner != "apt":
            raise SetupFailure("apt_system_tool_package_owner_mismatch")
    _verify_package("apt")
    package_version = _package_version("apt")
    expected_cli_version = APT_SUPPORTED_PACKAGE_VERSIONS.get(package_version)
    if expected_cli_version is None:
        raise SetupFailure("apt_package_version_unsupported", details={
            "apt_package_version": (
                package_version if re.fullmatch(r"[0-9A-Za-z.+:~\-]{1,64}", package_version)
                else "unrecognized"
            ),
            "supported_package_versions": sorted(APT_SUPPORTED_PACKAGE_VERSIONS),
        })
    cli_version = _version([APT_GET_PATH, "--version"], r"apt ([0-9.]+) \(amd64\)")
    if cli_version != expected_cli_version:
        raise SetupFailure("apt_cli_package_version_mismatch", details={
            "apt_cli_version": cli_version,
            "apt_package_version": package_version,
        })
    return {
        "package": "apt",
        "package_version": package_version,
        "apt_get_version": cli_version,
        "apt_get_sha256": _sha256(Path(APT_GET_PATH)),
        "apt_cache_sha256": _sha256(Path(APT_CACHE_PATH)),
        "apt_config_sha256": _sha256(Path(APT_CONFIG_PATH)),
        "supported_interface": "ubuntu_apt_2_7_14build2_or_2_8_3",
    }


def _owner_of(path: str) -> str:
    output, _ = _command(["dpkg-query", "-S", path], limit=8192)
    line = output.decode("utf-8", errors="strict").splitlines()[0]
    owner, separator, _ = line.partition(": ")
    if not separator:
        raise SetupFailure("system_binary_package_owner_unverified")
    return owner


def _verify_package(name: str) -> None:
    output, _ = _command(["dpkg", "--verify", name], limit=8192)
    if output.strip():
        raise SetupFailure("installed_system_package_file_verification_failed")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            while True:
                chunk = source.read(64 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
    except OSError as exc:
        raise SetupFailure("system_file_hash_unavailable") from exc
    return digest.hexdigest()


def prepare(evidence_path: Path) -> int:
    report: dict[str, Any] = {
        "format": "acv1.native_profile_setup.v1",
        "status": "blocked",
        "scope": "ephemeral Ubuntu CI profile setup; not canonical owner evidence or release readiness",
        "failure_code": "setup_not_completed",
        "stage": "initialization",
        "runner": {"image_os": os.environ.get("ImageOS", "unavailable")[:64],
                   "image_version": os.environ.get("ImageVersion", "unavailable")[:64],
                   "kernel": platform.release()[:128], "machine": platform.machine()[:32]},
    }
    _write_json(evidence_path, report)
    try:
        if platform.system() != "Linux":
            raise SetupFailure("linux_runner_required")
        release = Path("/etc/os-release").read_text(encoding="utf-8")
        if not re.search(r"(?m)^ID=ubuntu$", release) or not re.search(r'(?m)^VERSION_ID="?24\.04"?$', release):
            raise SetupFailure("ubuntu_24_04_runner_required")
        if platform.machine() not in {"x86_64", "amd64"}:
            raise SetupFailure("ubuntu_amd64_runner_required")

        report["stage"] = "apt_toolchain_version"
        apt_toolchain = _apt_toolchain_evidence()
        report["apt_toolchain"] = apt_toolchain
        _write_json(evidence_path, report)

        report["stage"] = "global_state_before_setup"
        before = _state_snapshot()
        report["global_state_before"] = _public_state(before)
        _write_json(evidence_path, report)

        report["stage"] = "apt_signature_configuration"
        apt_security = _apt_signature_configuration()
        report["apt_security"] = apt_security
        _write_json(evidence_path, report)

        report["stage"] = "trusted_ubuntu_source_configuration"
        apt_transport = _runner_mirror_configuration()
        apt_transport["apt_package_version"] = apt_toolchain["package_version"]
        report["apt_source_transport"] = {
            key: value for key, value in apt_transport.items() if not key.startswith("_")
        }
        _write_json(evidence_path, report)

        report["stage"] = "signed_apt_index_refresh"
        _quiet_command(["/usr/bin/sudo", "-n", APT_GET_PATH,
                        "-o", "APT::Update::Error-Mode=any",
                        "update", "-qq"], timeout=240)

        report["stage"] = "bubblewrap_package_record"
        bwrap_record = _package_record("bubblewrap", BWRAP_VERSION, "",
                                       apt_transport=apt_transport)
        if bwrap_record["Architecture"] != "amd64":
            raise SetupFailure("bubblewrap_signed_apt_record_architecture_mismatch")
        report["bubblewrap_apt_record"] = {
            "version": bwrap_record["Version"], "origin": _public_apt_source(bwrap_record["APT-Sources"]),
            "release_evidence": bwrap_record["_release_evidence"],
        }
        _write_json(evidence_path, report)

        report["stage"] = "bubblewrap_package_install"
        _quiet_command(["/usr/bin/sudo", "-n", APT_GET_PATH,
                        "install", "--no-install-recommends", "--yes",
                        "bubblewrap=" + BWRAP_VERSION], timeout=180)
        bwrap_path = shutil.which("bwrap")
        if not bwrap_path or os.path.realpath(bwrap_path) != "/usr/bin/bwrap":
            raise SetupFailure("package_owned_usr_bin_bwrap_required")
        bwrap_owner = _owner_of("/usr/bin/bwrap")
        if bwrap_owner != "bubblewrap":
            raise SetupFailure("bubblewrap_binary_package_owner_mismatch")
        if _package_version("bubblewrap") != BWRAP_VERSION:
            raise SetupFailure("bubblewrap_package_version_mismatch")
        bwrap_version = _version(["/usr/bin/bwrap", "--version"], r"bubblewrap ([0-9.]+)")
        if bwrap_version != "0.9.0":
            raise SetupFailure("bubblewrap_runtime_version_mismatch")
        _verify_package("bubblewrap")
        bwrap_digest = _sha256(Path("/usr/bin/bwrap"))
        report["bubblewrap"] = {
            "path": "/usr/bin/bwrap", "realpath": os.path.realpath(bwrap_path),
            "package_owner": bwrap_owner, "package_version": BWRAP_VERSION,
            "runtime_version": bwrap_version, "sha256": bwrap_digest,
            "dpkg_verify": "clean", "apt_origin": _public_apt_source(bwrap_record["APT-Sources"]),
        }
        _write_json(evidence_path, report)

        report["stage"] = "exact_profile_signed_apt_record"
        profile_record = _package_record(PROFILE_PACKAGE, PROFILE_VERSION, PROFILE_DEB_SHA256,
                                         apt_transport=apt_transport)
        report["profile_package"] = {
            "name": PROFILE_PACKAGE, "version": PROFILE_VERSION,
            "architecture": profile_record["Architecture"],
            "origin": _public_apt_source(profile_record["APT-Sources"]),
            "filename": profile_record["Filename"], "signed_index_sha256": profile_record["SHA256"],
            "release_evidence": profile_record["_release_evidence"],
        }
        _write_json(evidence_path, report)

        report["stage"] = "profile_package_authenticated_download"
        with tempfile.TemporaryDirectory(prefix="acv1-native-profile-hardening-") as temporary:
            work = Path(temporary)
            _quiet_command([APT_GET_PATH, "download", PROFILE_PACKAGE + "=" + PROFILE_VERSION],
                           timeout=120, cwd=work)
            packages = list(work.glob("*.deb"))
            if len(packages) != 1:
                raise SetupFailure("exact_profile_package_download_missing_or_ambiguous")
            deb = packages[0]
            if _sha256(deb) != PROFILE_DEB_SHA256:
                raise SetupFailure("downloaded_profile_package_digest_mismatch")
            fields_out, _ = _command(["dpkg-deb", "-f", str(deb), "Package", "Version", "Architecture"], limit=8192)
            package_fields = validate_profile_deb_metadata(fields_out)
            extracted = work / "bwrap-userns-restrict"
            _extract_profile(deb, extracted)
            report["profile_package"]["download_sha256"] = PROFILE_DEB_SHA256

            report["stage"] = "local_override_and_parser_validation"
            before_profile_load = _state_snapshot()
            if _public_state(before_profile_load) != _public_state(before):
                raise SetupFailure("global_apparmor_state_changed_before_profile_load")
            overrides = _check_local_overrides()
            parser_path = shutil.which("apparmor_parser")
            if not parser_path:
                raise SetupFailure("compatible_installed_apparmor_parser_unavailable")
            parser_realpath = os.path.realpath(parser_path)
            if not parser_realpath.startswith("/usr/"):
                raise SetupFailure("apparmor_parser_is_not_a_system_binary")
            parser_owner = _owner_of(parser_realpath)
            if parser_owner != "apparmor":
                raise SetupFailure("apparmor_parser_package_owner_mismatch")
            parser_version = _version([parser_realpath, "--version"], r"AppArmor parser version ([0-9.]+)")
            if not parser_version.startswith("4."):
                raise SetupFailure("apparmor_parser_abi4_compatible_version_required")
            parser_package_version = _package_version("apparmor")
            _verify_package("apparmor")
            if _owner_of("/usr/bin/bwrap") != "bubblewrap":
                raise SetupFailure("bubblewrap_package_ownership_changed")
            profile_bytes = extracted.read_bytes()
            if (b"abi <abi/4.0>" not in profile_bytes
                    or b"profile bwrap /usr/bin/bwrap" not in profile_bytes
                    or b"audit deny capability" not in profile_bytes):
                raise SetupFailure("expected_abi4_setup_and_child_deny_policy_missing")
            if any(state != "absent" for state in overrides):
                raise SetupFailure("local_override_state_not_approved")
            report["parser"] = {
                "path": parser_realpath, "package_owner": parser_owner,
                "version": parser_version, "package_version": parser_package_version,
                "sha256": _sha256(Path(parser_realpath)), "dpkg_verify": "clean",
                "local_overrides": overrides,
            }
            report["profile"] = {
                "source_path_in_package": PROFILE_MEMBER.removeprefix("./"),
                "source_sha256": PROFILE_SHA256, "abi": "4.0",
                "install_path": str(INSTALLED_PROFILE), "owner": "root:root", "mode": "0444",
            }
            _write_json(evidence_path, report)

            report["stage"] = "root_owned_profile_install_and_load"
            if INSTALLED_PROFILE.exists() or INSTALLED_PROFILE.is_symlink():
                raise SetupFailure("profile_install_target_already_exists")
            _quiet_command(["sudo", "-n", "install", "-d", "-o", "root", "-g", "root", "-m", "0755",
                            str(INSTALLED_PROFILE.parent)])
            _quiet_command(["sudo", "-n", "install", "-o", "root", "-g", "root", "-m", "0444",
                            str(extracted), str(INSTALLED_PROFILE)])
            installed_stat = INSTALLED_PROFILE.stat()
            if (installed_stat.st_uid != 0 or installed_stat.st_gid != 0
                    or installed_stat.st_mode & 0o777 != 0o444
                    or _sha256(INSTALLED_PROFILE) != PROFILE_SHA256):
                raise SetupFailure("root_owned_read_only_profile_verification_failed")
            installed_preprocess, _ = _command([parser_realpath, "-p", str(INSTALLED_PROFILE)], limit=256 * 1024)
            for token in (b"bwrap", b"unpriv_bwrap", b"capability"):
                if token not in installed_preprocess:
                    raise SetupFailure("effective_preprocessed_policy_missing_required_profile_content")
            preprocess_hash = hashlib.sha256(installed_preprocess).hexdigest()
            report["parser"]["preprocessed_policy_sha256"] = preprocess_hash
            report["profile"]["preprocessed_policy_sha256"] = preprocess_hash
            _quiet_command(["sudo", "-n", parser_realpath, "-a", "-T", str(INSTALLED_PROFILE)], timeout=30)

            report["stage"] = "global_state_after_profile_load"
            after = _state_snapshot(allow_designated_profiles=True)
            before_profiles = set(before["profile_table"])
            after_profiles = set(after["profile_table"])
            added = sorted(after_profiles - before_profiles)
            removed = sorted(before_profiles - after_profiles)
            expected = {"bwrap (enforce)", "unpriv_bwrap (enforce)"}
            if set(added) != expected or removed:
                raise SetupFailure("profile_load_changed_unexpected_kernel_profile_set")
            before_public = _public_state(before)
            after_public = _public_state(after)
            if (before_public["apparmor_enabled"] != after_public["apparmor_enabled"]
                    or before_public["kernel_lsm"] != after_public["kernel_lsm"]
                    or before_public["sysctls"] != after_public["sysctls"]):
                raise SetupFailure("global_apparmor_state_or_restriction_sysctl_changed")
            report["global_state_after"] = after_public
            report["loaded_profiles_added"] = added
            report["global_state_unchanged_except_two_designated_profiles"] = True
            report["profile"]["installed_sha256"] = PROFILE_SHA256
            report["status"] = "verified"
            report["failure_code"] = None
            report["stage"] = "complete"
            _write_json(evidence_path, report)
        print(json.dumps({"status": report["status"], "stage": report["stage"],
                          "artifact": str(evidence_path)}, sort_keys=True, separators=(",", ":")))
        return 0
    except SetupFailure as exc:
        report["failure_code"] = exc.code
        if exc.details:
            apt_command = exc.details.get("apt_command")
            if apt_command:
                report["apt_command_diagnostics"] = apt_command
            candidate = {key: value for key, value in exc.details.items()
                         if key != "apt_command"}
            if candidate:
                report["candidate_diagnostics"] = candidate
        report["status"] = "blocked"
        _write_json(evidence_path, report)
        print("native profile setup blocked: " + exc.code, file=sys.stderr)
        diagnostic = {key: report[key] for key in
                      ("apt_command_diagnostics", "candidate_diagnostics") if key in report}
        output = {"status": report["status"], "stage": report["stage"],
                  "failure_code": exc.code, "artifact": str(evidence_path)}
        if diagnostic:
            output["diagnostics"] = diagnostic
        print(json.dumps(output, sort_keys=True, separators=(",", ":")))
        return 1
    except Exception:
        report["failure_code"] = "unexpected_setup_error"
        report["status"] = "blocked"
        _write_json(evidence_path, report)
        print("native profile setup blocked: unexpected_setup_error", file=sys.stderr)
        print(json.dumps({"status": report["status"], "stage": report["stage"],
                          "failure_code": report["failure_code"], "artifact": str(evidence_path)},
                         sort_keys=True, separators=(",", ":")))
        return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    arguments = parser.parse_args(argv)
    return prepare(arguments.evidence)


if __name__ == "__main__":
    sys.exit(main())
