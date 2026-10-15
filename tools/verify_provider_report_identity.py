#!/usr/bin/env python3
"""Bind the offline CI report to this checked-out source, without changing it."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
MAX_REPORT_BYTES = 1_048_576


def git(*arguments):
    result = subprocess.run(
        ["git", *arguments], cwd=ROOT, capture_output=True, text=True,
        check=True, timeout=10,
        env={"PATH": os.defpath, "GIT_CONFIG_NOSYSTEM": "1",
             "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_SYSTEM": os.devnull,
             "GIT_OPTIONAL_LOCKS": "0", "GIT_TERMINAL_PROMPT": "0"},
    )
    return result.stdout.strip()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate field")
        result[key] = value
    return result


def verify(path):
    with Path(path).open("rb") as stream:
        data = stream.read(MAX_REPORT_BYTES + 1)
    if len(data) > MAX_REPORT_BYTES:
        raise ValueError("oversized report")
    report = json.loads(data, object_pairs_hook=unique_object)
    if Path(git("rev-parse", "--show-toplevel")).resolve() != ROOT:
        raise ValueError("wrong source root")
    revision = git("rev-parse", "HEAD")
    tree = git("rev-parse", revision + "^{tree}")
    if not re.fullmatch(r"[0-9a-f]{40}", revision) or not re.fullmatch(r"[0-9a-f]{40}", tree):
        raise ValueError("unknown source")
    expected = {"repository": "agent-harness", "revision": revision, "working_tree_modified": False}
    component = report.get("component") if isinstance(report, dict) else None
    if (not isinstance(component, dict) or component != expected
            or component.get("working_tree_modified") is not False
            or git("status", "--porcelain")):
        raise ValueError("source mismatch")
    receipt = {
        "schema": "agent-harness.provider-report-identity.v1",
        "repository": "agent-harness", "revision": revision, "tree": tree,
        "report_sha256": hashlib.sha256(data).hexdigest(),
        "verifier_sha256": hashlib.sha256((ROOT / "tools/verify_provider_readiness.py").read_bytes()).hexdigest(),
        "profile_sha256": hashlib.sha256((ROOT / "fixtures/provider-profile.json").read_bytes()).hexdigest(),
        "scope": "offline_ci_identity_only",
    }
    if git("rev-parse", "HEAD") != revision or git("status", "--porcelain"):
        raise ValueError("source changed during inspection")
    return receipt


def main():
    try:
        if len(sys.argv) != 2:
            raise ValueError("report path required")
        receipt = verify(sys.argv[1])
    except (OSError, ValueError, RecursionError, subprocess.SubprocessError):
        # Never echo report contents, paths, Git stderr or malformed field values.
        print("Provider readiness report source identity did not qualify.", file=sys.stderr)
        return 1
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
