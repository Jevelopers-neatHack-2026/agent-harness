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
LOCAL_OVERRIDES = (
    Path("/etc/apparmor.d/local/bwrap-userns-restrict"),
    Path("/etc/apparmor.d/local/unpriv_bwrap"),
)
RELEVANT_SYSCTLS = (
    "apparmor_restrict_unprivileged_userns",
    "apparmor_restrict_unprivileged_unconfined",
)


class SetupFailure(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
                         encoding="utf-8")
    temporary.replace(path)


def _command(args: list[str], *, timeout: float = 20, limit: int = 128 * 1024,
             cwd: Path | None = None) -> tuple[bytes, bytes]:
    try:
        result = subprocess.run(args, cwd=cwd, stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                check=False, timeout=timeout, close_fds=True)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SetupFailure("command_unavailable_or_timed_out") from exc
    if len(result.stdout) > limit or len(result.stderr) > 8 * 1024:
        raise SetupFailure("command_output_exceeded_bound")
    if result.returncode != 0:
        raise SetupFailure("command_failed")
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


def validate_apt_record(text: str, package: str, version: str,
                        sha256: str) -> dict[str, str]:
    stanzas = [part for part in re.split(r"\n\s*\n", text.strip()) if part.strip()]
    exact = [_fields(part) for part in stanzas]
    matches = [row for row in exact if row.get("Package") == package and row.get("Version") == version]
    if len(matches) != 1:
        raise SetupFailure("exact_signed_apt_package_record_missing_or_ambiguous")
    row = matches[0]
    expected_architecture = "all" if package == PROFILE_PACKAGE else "amd64"
    if (row.get("Architecture") != expected_architecture
            or not re.fullmatch(r"[0-9a-f]{64}", row.get("SHA256", ""))
            or (sha256 and row.get("SHA256") != sha256)):
        raise SetupFailure("signed_apt_package_identity_or_digest_mismatch")
    source = row.get("APT-Sources", "")
    source_tokens = source.split()
    if len(source_tokens) < 2:
        raise SetupFailure("signed_apt_package_origin_missing")
    parsed = urlparse(source_tokens[0])
    hostname = (parsed.hostname or "").lower()
    if (parsed.scheme not in {"http", "https"}
            or not (hostname == "archive.ubuntu.com" or hostname.endswith(".archive.ubuntu.com"))
            or not source_tokens[1].startswith("noble-updates/")):
        raise SetupFailure("signed_apt_package_origin_is_not_ubuntu_noble_updates")
    filename = row.get("Filename", "")
    expected_prefix = ("pool/main/a/apparmor/" if package == PROFILE_PACKAGE
                       else "pool/main/b/bubblewrap/")
    if not filename.startswith(expected_prefix):
        raise SetupFailure("signed_apt_package_filename_mismatch")
    return {key: row[key] for key in ("Package", "Version", "Architecture", "Filename", "SHA256", "APT-Sources")}


def _package_record(package: str, version: str, sha256: str) -> dict[str, str]:
    output, _ = _command(["apt-cache", "show", package + "=" + version], limit=128 * 1024)
    try:
        text = output.decode("utf-8")
    except UnicodeError as exc:
        raise SetupFailure("signed_apt_package_record_encoding_invalid") from exc
    return validate_apt_record(text, package, version, sha256)


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
        rows = APPARMOR_PROFILES.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise SetupFailure("apparmor_kernel_profile_status_unavailable") from exc
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
            "version": bwrap_record["Version"], "origin": bwrap_record["APT-Sources"],
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
            "dpkg_verify": "clean", "apt_origin": bwrap_record["APT-Sources"],
        }
        _write_json(evidence_path, report)

        report["stage"] = "exact_profile_signed_apt_record"
        profile_record = _package_record(PROFILE_PACKAGE, PROFILE_VERSION, PROFILE_DEB_SHA256)
        report["profile_package"] = {
            "name": PROFILE_PACKAGE, "version": PROFILE_VERSION,
            "architecture": profile_record["Architecture"], "origin": profile_record["APT-Sources"],
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
            package_fields = fields_out.decode("ascii", errors="strict").splitlines()
            if package_fields != [PROFILE_PACKAGE, PROFILE_VERSION, "all"]:
                raise SetupFailure("downloaded_profile_package_metadata_mismatch")
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
        report["status"] = "blocked"
        _write_json(evidence_path, report)
        print("native profile setup blocked: " + exc.code, file=sys.stderr)
        print(json.dumps({"status": report["status"], "stage": report["stage"],
                          "failure_code": exc.code, "artifact": str(evidence_path)},
                         sort_keys=True, separators=(",", ":")))
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
