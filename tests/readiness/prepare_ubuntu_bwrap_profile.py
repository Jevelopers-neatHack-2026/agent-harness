#!/usr/bin/env python3
"""Provision one digest-pinned Ubuntu bubblewrap AppArmor profile for ephemeral CI."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
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
APT_CACHE_FAILURE_PHASES = frozenset({"metadata_v1", "source_v2"})
APT_CACHE_DIAGNOSTIC_BYTES = 8 * 1024
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


def _apt_cache_error_category(stderr: bytes) -> str:
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
        return "apt_cache_or_index_infrastructure_failure"
    return "unclassified_apt_cache_failure"


def _apt_command_diagnostics(phase: str, failure_kind: str, *, return_code: int | None = None,
                             error_category: str | None = None, stdout_bytes: int = 0,
                             stderr_bytes: int = 0) -> dict[str, Any]:
    if phase not in APT_CACHE_FAILURE_PHASES:
        raise SetupFailure("apt_cache_command_phase_invalid")
    result: dict[str, Any] = {
        "tool": "apt-cache",
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
    if apt_phase is not None and apt_phase not in APT_CACHE_FAILURE_PHASES:
        raise SetupFailure("apt_cache_command_phase_invalid")
    try:
        result = subprocess.run(args, cwd=cwd, stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                check=False, timeout=timeout, close_fds=True)
    except OSError as exc:
        details = ({"apt_command": _apt_command_diagnostics(
            apt_phase, "launch_failed", error_category="apt_cache_executable_unavailable")}
                   if apt_phase else None)
        raise SetupFailure("command_unavailable_or_timed_out", details=details) from exc
    except subprocess.TimeoutExpired as exc:
        details = ({"apt_command": _apt_command_diagnostics(
            apt_phase, "timed_out", error_category="apt_cache_command_timed_out")}
                   if apt_phase else None)
        raise SetupFailure("command_unavailable_or_timed_out", details=details) from exc
    if len(result.stdout) > limit or len(result.stderr) > 8 * 1024:
        details = ({"apt_command": _apt_command_diagnostics(
            apt_phase, "output_limit_exceeded", error_category="apt_cache_output_limit_exceeded",
            stdout_bytes=len(result.stdout), stderr_bytes=len(result.stderr))}
                   if apt_phase else None)
        raise SetupFailure("command_output_exceeded_bound", details=details)
    if result.returncode != 0:
        details = ({"apt_command": _apt_command_diagnostics(
            apt_phase, "nonzero_exit", return_code=result.returncode,
            error_category=_apt_cache_error_category(result.stderr),
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
        rows = _stanzas(text or "")
        matches = [row for row in rows
                   if _apt_package_matches(row.get("Package", ""), package)
                   and row.get("Version") == version]
        row = matches[0] if len(matches) == 1 else {}
        source = row.get("APT-Sources", "")
        tokens = source.split()
        try:
            uri = urlparse(tokens[0]) if tokens else None
            hostname = (uri.hostname or "").lower() if uri else ""
        except ValueError:
            uri = None
            hostname = ""
        if not source:
            host_class = "missing"
        elif hostname == "archive.ubuntu.com" or hostname.endswith(".archive.ubuntu.com"):
            host_class = "ubuntu_archive"
        elif hostname == "security.ubuntu.com":
            host_class = "ubuntu_security"
        else:
            host_class = "other_or_invalid"
        release_component = tokens[1] if len(tokens) > 1 else ""
        return {
            "record_count": min(len(rows), 99),
            "record_count_capped": len(rows) > 99,
            "exact_candidate_count": min(len(matches), 99),
            "exact_candidate_count_capped": len(matches) > 99,
            "fields_present": [field for field in allowed_fields if field in row],
            "source_token_count": min(len(tokens), 8),
            "source_scheme": (
                uri.scheme.lower() if uri and uri.scheme.lower() in {"http", "https"}
                else ("other" if uri else "missing")
            ),
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
        }

    return {"metadata_record": summarize(metadata_text),
            "source_record": summarize(source_text)}


def _validated_apt_source(source: str) -> str:
    source_tokens = source.split()
    if len(source_tokens) != 4:
        raise SetupFailure("signed_apt_package_origin_missing")
    try:
        parsed = urlparse(source_tokens[0])
        hostname = (parsed.hostname or "").lower()
        port = parsed.port
    except ValueError as exc:
        raise SetupFailure("signed_apt_package_origin_is_not_ubuntu_noble_updates") from exc
    expected_index_architecture = "amd64"
    release_component = source_tokens[1]
    release, separator, component = release_component.partition("/")
    if (parsed.scheme not in {"http", "https"} or not hostname
            or parsed.username is not None or parsed.password is not None
            or parsed.query or parsed.fragment
            or (port is not None and not 1 <= port <= 65535)
            or not (hostname == "archive.ubuntu.com" or hostname.endswith(".archive.ubuntu.com")
                    or hostname == "security.ubuntu.com")
            or not separator or release != "noble-updates" or component != "main"
            or source_tokens[2] != expected_index_architecture or source_tokens[3] != "Packages"):
        raise SetupFailure("signed_apt_package_origin_is_not_ubuntu_noble_updates")
    return source


def validate_apt_record(text: str, package: str, version: str,
                        sha256: str, *, source_text: str) -> dict[str, str]:
    exact = _stanzas(text)
    matches = [row for row in exact
               if _apt_package_matches(row.get("Package", ""), package)
               and row.get("Version") == version]
    diagnostics = _candidate_record_diagnostics(text, source_text, package, version)
    if len(matches) != 1:
        raise SetupFailure("exact_signed_apt_package_record_missing_or_ambiguous",
                           details=diagnostics)
    row = matches[0]
    expected_architecture = "all" if package == PROFILE_PACKAGE else "amd64"
    if (row.get("Architecture") != expected_architecture
            or not re.fullmatch(r"[0-9a-f]{64}", row.get("SHA256", ""))
            or (sha256 and row.get("SHA256") != sha256)):
        raise SetupFailure("signed_apt_package_identity_or_digest_mismatch",
                           details=diagnostics)
    source_matches = [candidate for candidate in _stanzas(source_text)
                      if _apt_package_matches(candidate.get("Package", ""), package)
                      and candidate.get("Version") == version]
    if len(source_matches) != 1:
        raise SetupFailure("exact_signed_apt_source_record_missing_or_ambiguous",
                           details=diagnostics)
    source_row = source_matches[0]
    if (source_row.get("Filename") != row.get("Filename")
            or source_row.get("SHA256") != row.get("SHA256")
            or (source_row.get("Architecture") is not None
                and source_row.get("Architecture") != row.get("Architecture"))):
        raise SetupFailure("signed_apt_source_record_metadata_mismatch", details=diagnostics)
    source = source_row.get("APT-Sources", "")
    if not source:
        raise SetupFailure("signed_apt_package_origin_missing", details=diagnostics)
    try:
        _validated_apt_source(source)
    except SetupFailure as exc:
        exc.details = diagnostics
        raise
    filename = row.get("Filename", "")
    expected_prefix = ("pool/main/a/apparmor/" if package == PROFILE_PACKAGE
                       else "pool/main/b/bubblewrap/")
    if not filename.startswith(expected_prefix):
        raise SetupFailure("signed_apt_package_filename_mismatch", details=diagnostics)
    result = {key: row[key] for key in (
        "Package", "Version", "Architecture", "Filename", "SHA256",
    )}
    result["APT-Sources"] = source
    return result


def _package_record(package: str, version: str, sha256: str) -> dict[str, str]:
    # APT's default show format (v1) preserves the package-index fields we pin,
    # but does not synthesize APT-Sources. Request v2 separately for provenance.
    # In APT 2.7.14, -f/--full is registered only for `search`; `show` gets the
    # same full-record behavior through the generic configuration option.
    output, _ = _command(["apt-cache", "-o", "APT::Cache::ShowFull=true",
                          "-o", "APT::Cache::Show::Version=1", "show",
                          package + "=" + version], limit=128 * 1024,
                         apt_phase="metadata_v1")
    try:
        text = output.decode("utf-8")
    except UnicodeError as exc:
        raise SetupFailure("signed_apt_package_record_encoding_invalid") from exc
    try:
        source_output, _ = _command(["apt-cache", "-o", "APT::Cache::ShowFull=true",
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
    return validate_apt_record(text, package, version, sha256, source_text=source_text)


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
    parsed = urlparse(tokens[0])
    host = (parsed.hostname or "").lower()
    return f"{parsed.scheme.lower()}://{host}/[path-redacted] {' '.join(tokens[1:])}"


def _apt_signature_configuration() -> dict[str, Any]:
    if "APT_CONFIG" in os.environ:
        raise SetupFailure("external_apt_configuration_override_unverified")
    output, _ = _command(["apt-config", "dump"], limit=256 * 1024)
    text = output.decode("utf-8", errors="strict")
    unsafe = re.findall(
        r"(?im)^\s*(APT::Get::AllowUnauthenticated|Acquire::AllowInsecureRepositories|"
        r"Acquire::AllowDowngradeToInsecureRepositories|Acquire::AllowWeakRepositories)"
        r"\s+\"(?:true|yes|1)\"\s*;",
        text,
    )
    if unsafe:
        raise SetupFailure("apt_signature_validation_is_disabled")
    source_files = [Path("/etc/apt/sources.list"), *Path("/etc/apt/sources.list.d").glob("*")]
    for source_file in source_files:
        if source_file.is_symlink():
            raise SetupFailure("apt_source_configuration_symlink_unverified")
        if not source_file.is_file():
            continue
        try:
            source_text = source_file.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise SetupFailure("apt_source_configuration_unreadable") from exc
        if (re.search(r"(?i)(?:trusted|allow-insecure|allow-weak)\s*=\s*yes", source_text)
                or re.search(r"(?im)^\s*(?:trusted|allow-insecure|allow-weak)\s*:\s*yes\s*$", source_text)):
            raise SetupFailure("apt_source_configuration_bypasses_signature_checks")
    return {"signed_repository_checks_enabled": True, "unsafe_overrides": []}


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

        report["stage"] = "global_state_before_setup"
        before = _state_snapshot()
        report["global_state_before"] = _public_state(before)
        _write_json(evidence_path, report)

        report["stage"] = "apt_signature_configuration"
        apt_security = _apt_signature_configuration()
        report["apt_security"] = apt_security
        _write_json(evidence_path, report)

        report["stage"] = "signed_apt_index_refresh"
        _quiet_command(["sudo", "-n", "apt-get", "-o", "APT::Update::Error-Mode=any",
                        "update", "-qq"], timeout=240)

        report["stage"] = "bubblewrap_package_record"
        bwrap_record = _package_record("bubblewrap", BWRAP_VERSION, "")
        if bwrap_record["Architecture"] != "amd64":
            raise SetupFailure("bubblewrap_signed_apt_record_architecture_mismatch")
        report["bubblewrap_apt_record"] = {
            "version": bwrap_record["Version"], "origin": _public_apt_source(bwrap_record["APT-Sources"]),
        }
        _write_json(evidence_path, report)

        report["stage"] = "bubblewrap_package_install"
        _quiet_command(["sudo", "-n", "apt-get", "install", "--no-install-recommends", "--yes",
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
        profile_record = _package_record(PROFILE_PACKAGE, PROFILE_VERSION, PROFILE_DEB_SHA256)
        report["profile_package"] = {
            "name": PROFILE_PACKAGE, "version": PROFILE_VERSION,
            "architecture": profile_record["Architecture"],
            "origin": _public_apt_source(profile_record["APT-Sources"]),
            "filename": profile_record["Filename"], "signed_index_sha256": profile_record["SHA256"],
        }
        _write_json(evidence_path, report)

        report["stage"] = "profile_package_authenticated_download"
        with tempfile.TemporaryDirectory(prefix="acv1-native-profile-hardening-") as temporary:
            work = Path(temporary)
            _quiet_command(["apt-get", "download", PROFILE_PACKAGE + "=" + PROFILE_VERSION],
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
