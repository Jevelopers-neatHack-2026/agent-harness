#!/usr/bin/env python3
"""Validate local provider access profiles without contacting providers.

This is a prerequisite/readiness recorder, not an integration acceptance test.
Metadata and billable modes are intentionally fail-closed until a safe,
authorized implementation exists.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


TOOL_VERSION = "1.1"
PROFILE_VERSION = "1.1"
REPORT_VERSION = "1.1"
SUPPORTED_PROFILE_VERSIONS = frozenset({"1.0", PROFILE_VERSION})
MAX_PROFILE_JSON_BYTES = 1_048_576
MAX_PROFILE_JSON_NESTING = 100
MAX_PROFILE_JSON_INTEGER_DIGITS = 4096
EXPECTED_CAPABILITIES = (
    "jev",
    "gpt_image",
    "planner_reasoning",
    "generative_baseline",
    "neatlogs",
    "entire",
)
EXPECTED_PROVIDERS = {
    "jev": "typesafe",
    "gpt_image": "openai",
    "planner_reasoning": None,
    "generative_baseline": None,
    "neatlogs": "neatlogs",
    "entire": "entire",
}
EXPECTED_OWNER_ROLES = {
    "jev": "Agent and research",
    "gpt_image": "Creative and fulfillment",
    "planner_reasoning": "Agent and research",
    "generative_baseline": "Agent and research",
    "neatlogs": "Finance and reliability",
    "entire": "Agent and research",
}
EXPECTED_BLOCKED_CONSUMERS = {
    "jev": ["ACV1-JEV-EVAL", "AH04", "AH05", "AH10", "AH12"],
    "gpt_image": ["creative-engine:CE-01", "ACV1-AH05", "ACV1-AH10", "ACV1-AH12"],
    "planner_reasoning": ["AH04", "ACV1-JEV-EVAL"],
    "generative_baseline": ["ACV1-JEV-EVAL"],
    "neatlogs": ["SPN-01"],
    "entire": ["SPN-02"],
}
EXPECTED_ACTIONS = {
    "jev": "Provider administrator to confirm eligibility and credential injection; runtime owner to approve a pinned model policy.",
    "gpt_image": "Creative owner to select an approved engine API family and provider account access; retain Codex subscription image use as a separate development path.",
    "planner_reasoning": "Provider and runtime owners to select the bounded planner/reasoning provider and account configuration.",
    "generative_baseline": "Evaluation and provider owners to select a separate generative baseline provider, account and model.",
    "neatlogs": "Confirm workspace, service-account scope, owner and actual trace-evidence collection path.",
    "entire": "Confirm applicability, the locally installed/configured version, and an authorized artifact collection plan linked to relevant revisions.",
}
EVIDENCE_NEEDED = {
    "jev": [
        "Administrator eligibility and authorized server credential-injection evidence for the selected account/project.",
        "Owner-approved explicit model identity policy and recorded quota/revalidation evidence.",
        "Actual JEV inference evidence remains with ACV1-JEV-EVAL and the applicable runtime implementation gate.",
    ],
    "gpt_image": [
        "Creative-owner-approved engine API family/model and provider account/project eligibility evidence.",
        "Actual generated-image/provider evidence remains with creative-engine; Codex development execution does not satisfy it.",
    ],
    "planner_reasoning": [
        "Separate provider, endpoint, explicit model, account/project, quota, owner-approved future usage budget and live-test owner.",
        "Actual metered reasoning evidence remains with AH04 and ACV1-JEV-EVAL.",
    ],
    "generative_baseline": [
        "Separate baseline provider, endpoint, explicit model, account/project, quota, owner-approved future usage budget and live-test owner.",
        "Actual baseline evaluation evidence remains with ACV1-JEV-EVAL.",
    ],
    "neatlogs": [
        "Administrator confirmation of workspace and service-account scope.",
        "Actual Neatlogs artifact tied to an evaluated application run for SPN-01.",
    ],
    "entire": [
        "Applicability and locally installed/configured Entire version record.",
        "Actual checkpoint or repository-provenance artifact linked to relevant component revisions for SPN-02.",
    ],
}
EXPECTED_AUTH_OWNER = "Agent and research with provider/account administrator"
EXPECTED_CREDENTIAL_ENV = {
    "typesafe": "TYPESAFE_API_KEY",
    "openai": "OPENAI_API_KEY",
    "neatlogs": "NEATLOGS_API_KEY",
}
MODEL_REQUIRED = frozenset(
    {"jev", "gpt_image", "planner_reasoning", "generative_baseline"}
)
SECRET_ENV_REQUIRED = frozenset({"jev", "neatlogs"})
PLACEHOLDER_WORDS = frozenset(
    {"changeme", "example", "placeholder", "replace_me", "secret", "tbd", "token", "your_api_key"}
)
ENV_NAME_RE = re.compile(r"^[A-Z_][A-Z0-9_]{0,127}$")
SAFE_ID_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
SAFE_REF_RE = re.compile(r"^(?:https://[^\s?#@]+|evidence:[A-Za-z0-9._/-]+|urn:[A-Za-z0-9][A-Za-z0-9._:/-]*)$")
SLUG_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
PINNED_SDK_VERSION_RE = re.compile(
    r"^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$"
)
IMMUTABLE_REVISION_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
SUPPORTED_SDKS = {
    "typesafe": frozenset({"typesafe-sdk-js"}),
    "openai": frozenset({"openai"}),
}
# Local preflight policy, grounded in the documented endpoints already recorded
# in the runbook. This is an allowlist, not a claim of account entitlement.
API_SURFACES: dict[tuple[str, str, str], tuple[str, frozenset[str]]] = {
    ("jev", "typesafe", "systemone_v1"): (
        "v1", frozenset({"https://api.typesafe.ai/v1/systemone"})
    ),
    ("gpt_image", "openai", "images"): (
        "v1", frozenset({"https://api.openai.com/v1/images/generations"})
    ),
    ("planner_reasoning", "openai", "responses"): (
        "v1", frozenset({"https://api.openai.com/v1/responses"})
    ),
    ("planner_reasoning", "openai", "chat_completions"): (
        "v1", frozenset({"https://api.openai.com/v1/chat/completions"})
    ),
    ("generative_baseline", "openai", "responses"): (
        "v1", frozenset({"https://api.openai.com/v1/responses"})
    ),
    ("generative_baseline", "openai", "chat_completions"): (
        "v1", frozenset({"https://api.openai.com/v1/chat/completions"})
    ),
    ("neatlogs", "neatlogs", "public_api"): (
        "v1", frozenset({"https://app.neatlogs.com/api/v1/public"})
    ),
    ("neatlogs", "neatlogs", "trace_ingest"): (
        "v1",
        frozenset(
            {
                "https://ingest.neatlogs.com/v1/traces",
                "https://eu.ingest.neatlogs.com/v1/traces",
            }
        ),
    ),
}

# Bounded local identity policy, inspected against the official provider model
# references on 2026-10-10. This catalog describes supported profile identities;
# it does not select a model, approve a use, or prove an account can call it.
_TEXT_CAPABILITIES = frozenset({"planner_reasoning", "generative_baseline"})
_TEXT_APIS = frozenset({"responses", "chat_completions"})
_IMAGE_CAPABILITIES = frozenset({"gpt_image"})
_IMAGE_APIS = frozenset({"images"})
DOCUMENTED_MODEL_PINS: dict[tuple[str, str], tuple[frozenset[str], frozenset[str]]] = {
    ("typesafe", "jev-1.13.0"): (frozenset({"jev"}), frozenset({"systemone_v1"})),
    ("openai", "gpt-4.1-2025-04-14"): (_TEXT_CAPABILITIES, _TEXT_APIS),
    ("openai", "gpt-image-2-2026-04-21"): (_IMAGE_CAPABILITIES, _IMAGE_APIS),
}
DOCUMENTED_MODEL_ALIASES: dict[
    tuple[str, str], tuple[str, frozenset[str], frozenset[str]]
] = {
    ("typesafe", "jev-latest"): (
        "jev-1.13.0", frozenset({"jev"}), frozenset({"systemone_v1"})
    ),
    ("typesafe", "jev-preview"): (
        "jev-1.13.0", frozenset({"jev"}), frozenset({"systemone_v1"})
    ),
    ("openai", "gpt-4.1"): (
        "gpt-4.1-2025-04-14", _TEXT_CAPABILITIES, _TEXT_APIS
    ),
    ("openai", "gpt-image-2"): (
        "gpt-image-2-2026-04-21", _IMAGE_CAPABILITIES, _IMAGE_APIS
    ),
}

# These response surfaces document a response model identity. The Images API
# response object does not contain a model field, so absence is permitted there;
# a supplied image response identity is still checked against the pinned policy.
LIVE_RESPONSE_MODEL_REQUIRED = frozenset(
    {
        ("typesafe", "systemone_v1"),
        ("openai", "responses"),
        ("openai", "chat_completions"),
    }
)


class ProfileError(Exception):
    """A typed validation error whose message never includes input values."""

    def __init__(self, code: str, path: str, message: str):
        super().__init__(message)
        self.code = code
        self.path = path
        self.message = message


class DuplicateKeyError(ValueError):
    pass


class JsonIntegerLimitError(Exception):
    """Raised before converting an oversized JSON integer token."""


def _pairs_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateKeyError("duplicate key")
        result[key] = value
    return result


def _parse_bounded_json_integer(token: str) -> int:
    digits = token[1:] if token.startswith("-") else token
    if len(digits) > MAX_PROFILE_JSON_INTEGER_DIGITS:
        raise JsonIntegerLimitError
    try:
        return int(token)
    except ValueError:
        # Keep the interpreter's integer-string safety limit enabled and
        # translate any stricter runtime limit into the same typed rejection.
        raise JsonIntegerLimitError from None


def _json_nesting_exceeds_limit(text: str) -> bool:
    depth = 0
    in_string = False
    escaped = False
    for char in text:
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char in "[{":
            depth += 1
            if depth > MAX_PROFILE_JSON_NESTING:
                return True
        elif char in "]}" and depth:
            depth -= 1
    return False


def _fail(code: str, path: str, message: str) -> None:
    raise ProfileError(code, path, message)


def _object(value: Any, path: str, allowed: set[str], required: set[str] | None = None) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail("CONFIG_TYPE_INVALID", path, "Expected an object.")
    extra = set(value) - allowed
    if extra:
        # Do not reflect unknown key names; they may contain sensitive data.
        _fail("CONFIG_FIELD_UNSUPPORTED", path, "Object contains an unsupported field.")
    missing = (required if required is not None else allowed) - set(value)
    if missing:
        _fail("CONFIG_FIELD_MISSING", path, "Object is missing a required field.")
    return value


def _string(value: Any, path: str, *, nullable: bool = False, pattern: re.Pattern[str] | None = None) -> str | None:
    if nullable and value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        _fail("CONFIG_VALUE_INVALID", path, "Expected a non-empty string.")
    if value != value.strip() or any(ord(char) < 32 for char in value):
        _fail("CONFIG_VALUE_INVALID", path, "String contains unsafe whitespace or control characters.")
    if pattern is not None and not pattern.fullmatch(value):
        _fail("CONFIG_VALUE_INVALID", path, "String does not match the supported format.")
    return value


def _nullable_ref(value: Any, path: str) -> str | None:
    ref = _string(value, path, nullable=True)
    if ref is not None and not SAFE_REF_RE.fullmatch(ref):
        _fail("EVIDENCE_REFERENCE_UNSAFE", path, "Evidence references must be safe, non-secret identifiers.")
    return ref


def _date(value: Any, path: str, *, nullable: bool = False) -> date | None:
    if nullable and value is None:
        return None
    text_value = _string(value, path)
    try:
        parsed = date.fromisoformat(text_value)
    except ValueError:
        _fail("CONFIG_DATE_INVALID", path, "Expected an ISO calendar date (YYYY-MM-DD).")
    if parsed.isoformat() != text_value:
        _fail("CONFIG_DATE_INVALID", path, "Expected an ISO calendar date (YYYY-MM-DD).")
    return parsed


def _enum(value: Any, path: str, choices: set[str]) -> str:
    text_value = _string(value, path)
    if text_value not in choices:
        _fail("CONFIG_VALUE_UNSUPPORTED", path, "Value is outside the supported choices.")
    return text_value


def _valid_endpoint(provider: str | None, capability_id: str, endpoint: str, kind: str) -> bool:
    """Strict URL allowlist; this code never opens the URL."""
    try:
        parsed = urlsplit(endpoint)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.port is not None
            or parsed.query
            or parsed.fragment
        ):
            return False
    except ValueError:
        return False

    exact = {
        ("typesafe", "inference"): {"https://api.typesafe.ai/v1/systemone"},
        ("typesafe", "metadata"): {"https://api.typesafe.ai/v1/models"},
        ("openai", "inference"): {
            "https://api.openai.com/v1/responses",
            "https://api.openai.com/v1/chat/completions",
            "https://api.openai.com/v1/images/generations",
        },
        ("openai", "metadata"): {"https://api.openai.com/v1/models"},
        ("neatlogs", "inference"): {
            "https://app.neatlogs.com/api/v1/public",
            "https://ingest.neatlogs.com/v1/traces",
            "https://eu.ingest.neatlogs.com/v1/traces",
        },
        ("neatlogs", "metadata"): {"https://app.neatlogs.com/api/v1/public"},
    }
    if capability_id == "entire":
        return False
    return endpoint in exact.get((provider or "", kind), set())


def _validate_model(
    model_record: Any,
    capability_id: str,
    provider: str | None,
    api_family: str | None,
    path: str,
    as_of: date,
) -> None:
    fields = {
        "requested",
        "resolved",
        "identity_kind",
        "policy_status",
        "approved_model",
        "expected_resolved",
        "approval_ref",
        "approved_by_role",
        "approved_at",
        "revalidate_by",
    }
    record = _object(model_record, path, fields)
    requested = _string(record["requested"], f"{path}.requested", nullable=True)
    resolved = _string(record["resolved"], f"{path}.resolved", nullable=True)
    identity_kind = _enum(record["identity_kind"], f"{path}.identity_kind", {"unselected", "pinned", "alias"})
    policy_status = _enum(record["policy_status"], f"{path}.policy_status", {"pending", "approved"})
    approved_model = _string(record["approved_model"], f"{path}.approved_model", nullable=True)
    expected_resolved = _string(record["expected_resolved"], f"{path}.expected_resolved", nullable=True)
    approval_ref = _nullable_ref(record["approval_ref"], f"{path}.approval_ref")
    approved_by_role = _string(record["approved_by_role"], f"{path}.approved_by_role", nullable=True)
    approved_at = _date(record["approved_at"], f"{path}.approved_at", nullable=True)
    revalidate_by = _date(record["revalidate_by"], f"{path}.revalidate_by", nullable=True)

    if capability_id not in MODEL_REQUIRED:
        if identity_kind != "unselected" or policy_status != "pending" or any(
            item is not None
            for item in (
                requested,
                resolved,
                approved_model,
                expected_resolved,
                approval_ref,
                approved_by_role,
                approved_at,
                revalidate_by,
            )
        ):
            _fail(
                "MODEL_UNSUPPORTED_FOR_CAPABILITY",
                path,
                "This capability does not accept model identity or approval fields.",
            )
        return

    if requested is None:
        if identity_kind != "unselected" or policy_status != "pending" or any(
            item is not None for item in (resolved, approved_model, expected_resolved, approval_ref, approved_by_role, approved_at, revalidate_by)
        ):
            _fail("MODEL_RECORD_INCONSISTENT", path, "Unselected models cannot carry model identity or approval evidence.")
        return

    if identity_kind == "unselected":
        _fail("MODEL_RECORD_INCONSISTENT", path, "A requested model requires an explicit identity policy.")
    if provider is None:
        _fail("PROVIDER_SELECTION_REQUIRED", path, "Select the provider before recording a requested model identity.")
    alias_policy = DOCUMENTED_MODEL_ALIASES.get((provider, requested))
    if alias_policy is not None:
        identity_expected, supported_capabilities, supported_api_families = alias_policy
        alias = True
    else:
        pin_policy = DOCUMENTED_MODEL_PINS.get((provider, requested))
        if pin_policy is None:
            _fail(
                "MODEL_UNSUPPORTED",
                f"{path}.requested",
                "Model identifier is not in the bounded documented provider identity policy.",
            )
        supported_capabilities, supported_api_families = pin_policy
        identity_expected = requested
        alias = False

    if capability_id not in supported_capabilities:
        _fail(
            "MODEL_UNSUPPORTED_FOR_CAPABILITY",
            f"{path}.requested",
            "Model identity is not supported for this capability.",
        )
    if api_family is not None and api_family not in supported_api_families:
        _fail(
            "MODEL_API_MISMATCH",
            f"{path}.requested",
            "Model identity is not supported by the configured capability API surface.",
        )

    if alias and identity_kind != "alias":
        _fail(
            "MODEL_ALIAS_POLICY_REQUIRED",
            f"{path}.identity_kind",
            "Documented model aliases require an approved alias policy and exact pinned resolution.",
        )
    if alias and policy_status != "approved":
        _fail("MODEL_ALIAS_POLICY_REQUIRED", f"{path}.policy_status", "Documented model aliases require an explicit approved alias policy.")
    if alias and expected_resolved is None:
        _fail("MODEL_ALIAS_RESOLUTION_REQUIRED", f"{path}.expected_resolved", "Approved alias policy requires the exact pinned target.")
    if alias and expected_resolved != identity_expected:
        _fail(
            "MODEL_ALIAS_RESOLUTION_INVALID",
            f"{path}.expected_resolved",
            "Alias resolution must match the exact pinned target in the documented identity policy.",
        )
    if not alias and expected_resolved is not None:
        _fail("MODEL_RECORD_INCONSISTENT", f"{path}.expected_resolved", "Pinned model identities cannot carry alias-resolution policy fields.")
    if identity_kind == "alias" and not alias:
        _fail("MODEL_ALIAS_POLICY_INVALID", f"{path}.identity_kind", "Alias policy must identify a documented provider model alias.")
    if policy_status == "approved":
        if approved_model != requested or approval_ref is None or approved_by_role is None or approved_at is None or revalidate_by is None:
            _fail("MODEL_APPROVAL_EVIDENCE_INCOMPLETE", path, "Approved model policy requires matching model, owner role, approval/revalidation dates, and evidence reference.")
        if approved_by_role != EXPECTED_OWNER_ROLES[capability_id]:
            _fail("OWNER_ROLE_UNSUPPORTED", f"{path}.approved_by_role", "Model policy approval must come from the assigned acceptance owner role.")
        if approved_at > as_of or revalidate_by < approved_at:
            _fail("MODEL_APPROVAL_DATE_INVALID", path, "Model policy approval and revalidation dates are inconsistent with the inspection date.")
        if alias and expected_resolved != identity_expected:
            _fail("MODEL_ALIAS_RESOLUTION_REQUIRED", path, "Approved alias policy requires the exact documented pinned target.")
    elif any(item is not None for item in (approved_model, expected_resolved, approval_ref, approved_by_role, approved_at, revalidate_by)):
        _fail("MODEL_RECORD_INCONSISTENT", path, "Pending model policy cannot carry approval fields.")

    if resolved is not None:
        if resolved != identity_expected:
            _fail("MODEL_RESOLUTION_MISMATCH", f"{path}.resolved", "Resolved model identity does not match the approved requested identity.")


def _validate_api_surface(
    capability_id: str,
    provider: str | None,
    api_family: str | None,
    endpoint: str | None,
    path: str,
) -> tuple[str, frozenset[str]] | None:
    """Validate the capability/provider/API/endpoint tuple without opening it."""
    if capability_id == "entire":
        if api_family is not None or endpoint is not None:
            _fail(
                "API_NOT_APPLICABLE",
                path,
                "Entire provenance uses a local artifact path, not a provider API endpoint.",
            )
        return None
    if provider is None:
        if api_family is not None or endpoint is not None:
            _fail("PROVIDER_SELECTION_REQUIRED", path, "Select a provider before recording an API family or endpoint.")
        return None
    if api_family is None:
        return None
    surface = API_SURFACES.get((capability_id, provider, api_family))
    if surface is None:
        _fail("API_FAMILY_UNSUPPORTED", f"{path}.api_family", "API family is not supported for this provider capability.")
    expected_version, supported_endpoints = surface
    if endpoint is not None and endpoint not in supported_endpoints:
        _fail(
            "API_ENDPOINT_MISMATCH",
            f"{path}.endpoint",
            "Endpoint does not match the selected provider capability and API family.",
        )
    return expected_version, supported_endpoints


def _validate_client_selection(
    *,
    config: dict[str, Any],
    capability_id: str,
    provider: str | None,
    api_surface: tuple[str, frozenset[str]] | None,
    profile_version: str,
    path: str,
) -> str:
    """Validate a pinned SDK or an explicitly versioned direct-HTTP policy."""
    sdk = config["sdk"]
    selection_present = "client_selection" in config
    if profile_version == "1.0":
        if selection_present:
            _fail(
                "PROFILE_VERSION_MISMATCH",
                f"{path}.client_selection",
                "client_selection requires provider profile schema 1.1.",
            )
        if capability_id == "entire":
            if sdk is not None:
                _fail("CLIENT_SELECTION_NOT_APPLICABLE", f"{path}.sdk", "Entire provenance does not use an API client.")
            return "not_applicable"
        if sdk is not None:
            _validate_pinned_sdk(sdk, provider=provider, path=f"{path}.sdk")
            if api_surface is None:
                _fail("CLIENT_SELECTION_WITHOUT_API", f"{path}.sdk", "Select a supported provider API and endpoint before an SDK.")
            return "sdk"
        return "unselected"

    selection = _object(
        config.get("client_selection"),
        f"{path}.client_selection",
        {"mode", "api_version", "policy_ref"},
    )
    mode = _enum(selection["mode"], f"{path}.client_selection.mode", {"unselected", "sdk", "direct_http"})
    api_version = _string(selection["api_version"], f"{path}.client_selection.api_version", nullable=True)
    policy_ref = _nullable_ref(selection["policy_ref"], f"{path}.client_selection.policy_ref")

    if capability_id == "entire":
        if mode != "unselected" or sdk is not None or api_version is not None or policy_ref is not None:
            _fail("CLIENT_SELECTION_NOT_APPLICABLE", f"{path}.client_selection", "Entire provenance does not use an API client.")
        return "not_applicable"

    if mode == "unselected":
        if sdk is not None or api_version is not None or policy_ref is not None:
            _fail("CLIENT_SELECTION_INCONSISTENT", f"{path}.client_selection", "Unselected client records cannot carry SDK or direct-HTTP policy fields.")
        return "unselected"

    if provider is None or config["api_family"] is None or config["endpoint"] is None or api_surface is None:
        _fail("CLIENT_SELECTION_WITHOUT_API", f"{path}.client_selection", "Select a supported provider API and endpoint before a client implementation.")

    if mode == "sdk":
        if api_version is not None or policy_ref is not None or sdk is None:
            _fail("CLIENT_SELECTION_INCONSISTENT", f"{path}.client_selection", "SDK selection requires one pinned SDK and no direct-HTTP policy fields.")
        _validate_pinned_sdk(sdk, provider=provider, path=f"{path}.sdk")
        return "sdk"

    if sdk is not None or api_version is None or policy_ref is None:
        _fail("CLIENT_SELECTION_INCONSISTENT", f"{path}.client_selection", "Direct-HTTP selection requires a versioned API policy reference and no SDK.")
    expected_version, _ = api_surface
    if api_version != expected_version:
        _fail("DIRECT_HTTP_API_VERSION_MISMATCH", f"{path}.client_selection.api_version", "Direct-HTTP version does not match the selected provider API surface.")
    return "direct_http"


def _validate_pinned_sdk(sdk_value: Any, *, provider: str | None, path: str) -> None:
    sdk = _object(sdk_value, path, {"name", "version", "source_revision"})
    sdk_name = _string(sdk["name"], f"{path}.name", pattern=SLUG_RE)
    _string(sdk["version"], f"{path}.version", pattern=PINNED_SDK_VERSION_RE)
    _string(sdk["source_revision"], f"{path}.source_revision", pattern=IMMUTABLE_REVISION_RE)
    if sdk_name not in SUPPORTED_SDKS.get(provider or "", frozenset()):
        _fail("SDK_UNSUPPORTED", f"{path}.name", "SDK name is not in the local provider-specific supported set.")


def _validate_status_record(
    record_value: Any,
    *,
    path: str,
    keys: set[str],
    status_choices: set[str],
    nullable_fields: set[str],
) -> dict[str, Any]:
    record = _object(record_value, path, keys)
    _enum(record["status"], f"{path}.status", status_choices)
    for field in nullable_fields:
        value = record[field]
        if field in {"checked_at", "valid_until"}:
            _date(value, f"{path}.{field}", nullable=True)
        elif field == "evidence_ref":
            _nullable_ref(value, f"{path}.{field}")
        elif field == "endpoint":
            _string(value, f"{path}.{field}", nullable=True)
        else:
            _string(value, f"{path}.{field}", nullable=True)
    return record


def _check_record_identity(
    *,
    record: dict[str, Any],
    config: dict[str, Any],
    path: str,
    as_of: date,
    require_model: bool,
    resolved_policy: str | None = None,
    response_model_required: bool = False,
) -> list[dict[str, str]]:
    blockers: list[dict[str, str]] = []
    checked_at = _date(record["checked_at"], f"{path}.checked_at", nullable=True)
    valid_until = _date(record["valid_until"], f"{path}.valid_until", nullable=True)
    if checked_at is None or valid_until is None or record["evidence_ref"] is None:
        _fail("IDENTITY_RECORD_INCOMPLETE", path, "Observed records require dates and a safe evidence reference.")
    if checked_at > as_of or valid_until < checked_at:
        _fail("IDENTITY_RECORD_DATE_INVALID", path, "Evidence dates are inconsistent with the inspection date.")
    if valid_until < as_of:
        blockers.append({"code": "IDENTITY_RECORD_STALE", "path": path, "message": "Recorded identity evidence has passed its revalidation date."})

    expected_pairs = {
        "account_ref": config["account_ref"],
        "project_ref": config["project_ref"],
    }
    for field, expected in expected_pairs.items():
        if expected is None or record[field] != expected:
            _fail("IDENTITY_RECORD_MISMATCH", f"{path}.{field}", "Evidence identity does not match the configured account and project.")
    if require_model:
        expected_model = config["model"]["requested"]
        if record["requested_model"] != expected_model:
            _fail("IDENTITY_RECORD_MISMATCH", f"{path}.requested_model", "Evidence model does not match the configured requested model.")
        actual_resolved = record["resolved_model"]
        if resolved_policy == "metadata" and actual_resolved is not None:
            _fail("METADATA_RESOLUTION_UNSUPPORTED", f"{path}.resolved_model", "Metadata checks cannot prove inference model resolution.")
        if resolved_policy == "live" and response_model_required and actual_resolved is None:
            _fail(
                "LIVE_RESPONSE_MODEL_MISSING",
                f"{path}.resolved_model",
                "This successful provider response must include its actual model identity.",
            )
        if resolved_policy == "live" and actual_resolved is not None:
            expected_resolved = config["model"]["expected_resolved"] or config["model"]["requested"]
            if actual_resolved != expected_resolved:
                _fail("MODEL_RESOLUTION_MISMATCH", f"{path}.resolved_model", "Live evidence resolved an unapproved model identity.")
    return blockers


def validate_profile(profile: Any, *, as_of: date) -> tuple[list[dict[str, str]], dict[str, Any] | None]:
    """Validate local profile shape and cross-field identity invariants."""
    try:
        root_keys = {
            "schema_version",
            "profile_id",
            "environment",
            "development_execution",
            "live_test_authorization",
            "capabilities",
        }
        root = _object(profile, "$", root_keys)
        profile_version = _string(root["schema_version"], "$.schema_version")
        if profile_version not in SUPPORTED_PROFILE_VERSIONS:
            _fail("PROFILE_VERSION_UNSUPPORTED", "$.schema_version", "Only local provider profile schema 1.0 or 1.1 is supported.")
        _string(root["profile_id"], "$.profile_id", pattern=SAFE_ID_RE)
        _enum(root["environment"], "$.environment", {"development", "test", "production"})

        dev = _object(
            root["development_execution"],
            "$.development_execution",
            {"access_preference", "surface", "model", "status", "source_url", "inspected_on", "scope"},
        )
        if dev["access_preference"] != "codex_subscription" or dev["surface"] != "codex_builtin_image_generation" or dev["model"] != "gpt-image-2":
            _fail("DEVELOPMENT_EXECUTION_PROFILE_UNSUPPORTED", "$.development_execution", "Development image execution preference is outside the recorded Codex profile.")
        if dev["status"] != "documented_preference_not_live_tested" or dev["scope"] != "development_execution_only":
            _fail("DEVELOPMENT_EXECUTION_SCOPE_INVALID", "$.development_execution", "Codex image preference must remain development-only and unverified.")
        if dev["source_url"] != "https://learn.chatgpt.com/docs/image-generation":
            _fail("DEVELOPMENT_EXECUTION_SOURCE_INVALID", "$.development_execution.source_url", "Expected the inspected official image-generation documentation reference.")
        _date(dev["inspected_on"], "$.development_execution.inspected_on")

        auth = _object(
            root["live_test_authorization"],
            "$.live_test_authorization",
            {"status", "owner_role", "reservation_ref", "evidence_ref", "authorized_capabilities"},
        )
        auth_status = _enum(auth["status"], "$.live_test_authorization.status", {"pending", "approved", "denied"})
        auth_owner = _string(auth["owner_role"], "$.live_test_authorization.owner_role")
        if auth_owner != EXPECTED_AUTH_OWNER:
            _fail("OWNER_ROLE_UNSUPPORTED", "$.live_test_authorization.owner_role", "Use the accountable owner role; named administrator assignment remains pending.")
        reservation = _nullable_ref(auth["reservation_ref"], "$.live_test_authorization.reservation_ref")
        auth_evidence = _nullable_ref(auth["evidence_ref"], "$.live_test_authorization.evidence_ref")
        if not isinstance(auth["authorized_capabilities"], list):
            _fail("CONFIG_TYPE_INVALID", "$.live_test_authorization.authorized_capabilities", "Expected a list of capability IDs.")
        for index, item in enumerate(auth["authorized_capabilities"]):
            _enum(item, f"$.live_test_authorization.authorized_capabilities[{index}]", set(EXPECTED_CAPABILITIES))
        if len(set(auth["authorized_capabilities"])) != len(auth["authorized_capabilities"]):
            _fail("LIVE_AUTHORIZATION_DUPLICATE", "$.live_test_authorization.authorized_capabilities", "Capability authorization entries must be unique.")
        if auth_status == "approved" and (reservation is None or auth_evidence is None):
            _fail("LIVE_AUTHORIZATION_INCOMPLETE", "$.live_test_authorization", "Approved live checks require external reservation and authorization references.")
        if auth_status == "approved" and not auth["authorized_capabilities"]:
            _fail("LIVE_AUTHORIZATION_INCOMPLETE", "$.live_test_authorization.authorized_capabilities", "Approved live checks must name the authorized capability cells.")

        if not isinstance(root["capabilities"], list):
            _fail("CONFIG_TYPE_INVALID", "$.capabilities", "Expected a list of capability records.")
        capabilities: dict[str, dict[str, Any]] = {}
        for index, cell_value in enumerate(root["capabilities"]):
            path = f"$.capabilities[{index}]"
            cell_keys = {
                "id", "required", "provider", "owner_role", "next_action", "blocked_consumers",
                "configuration", "eligibility", "metadata", "live", "future_usage_authorization", "sponsor_integration",
            }
            cell = _object(cell_value, path, cell_keys)
            cell_id = _enum(cell["id"], f"{path}.id", set(EXPECTED_CAPABILITIES))
            if cell_id in capabilities:
                _fail("CAPABILITY_DUPLICATE", f"{path}.id", "Capability IDs must be unique.")
            if not isinstance(cell["required"], bool):
                _fail("CONFIG_TYPE_INVALID", f"{path}.required", "Expected a boolean.")
            if cell["required"] is not True:
                _fail("REQUIRED_CAPABILITY_DISABLED", f"{path}.required", "Every listed access gate remains required for v1.")
            expected_provider = EXPECTED_PROVIDERS[cell_id]
            actual_provider = _string(cell["provider"], f"{path}.provider", nullable=True)
            allowed_providers = {expected_provider} if expected_provider else {None, "openai", "typesafe"}
            if actual_provider not in allowed_providers:
                _fail("PROVIDER_UNSUPPORTED", f"{path}.provider", "Provider selection is not supported for this capability.")
            owner_role = _string(cell["owner_role"], f"{path}.owner_role")
            if owner_role != EXPECTED_OWNER_ROLES[cell_id]:
                _fail("OWNER_ROLE_UNSUPPORTED", f"{path}.owner_role", "Owner role does not match the authoritative acceptance ownership.")
            next_action = _string(cell["next_action"], f"{path}.next_action")
            if next_action != EXPECTED_ACTIONS[cell_id]:
                _fail("NEXT_ACTION_UNSUPPORTED", f"{path}.next_action", "Next action must preserve the assigned provider owner decision.")
            if not isinstance(cell["blocked_consumers"], list) or not cell["blocked_consumers"]:
                _fail("CONFIG_TYPE_INVALID", f"{path}.blocked_consumers", "Expected a non-empty consumer list.")
            if cell["blocked_consumers"] != EXPECTED_BLOCKED_CONSUMERS[cell_id]:
                _fail("BLOCKED_CONSUMERS_MISMATCH", f"{path}.blocked_consumers", "Blocked consumers must preserve the assigned issue relationships and order.")
            for consumer_index, consumer in enumerate(cell["blocked_consumers"]):
                _string(consumer, f"{path}.blocked_consumers[{consumer_index}]", pattern=re.compile(r"^[A-Za-z][A-Za-z0-9:-]{0,63}$"))

            future_usage = cell["future_usage_authorization"]
            if cell_id in {"planner_reasoning", "generative_baseline"}:
                usage = _object(
                    future_usage, f"{path}.future_usage_authorization",
                    {"status", "owner_role", "live_test_owner_role", "budget_ref", "reservation_protocol_ref"},
                )
                usage_status = _enum(usage["status"], f"{path}.future_usage_authorization.status", {"pending", "approved"})
                if usage["owner_role"] != "Agent and research" or usage["live_test_owner_role"] != "Agent and research":
                    _fail("OWNER_ROLE_UNSUPPORTED", f"{path}.future_usage_authorization", "Planner and baseline budget ownership stays with Agent and research.")
                budget_ref = _nullable_ref(usage["budget_ref"], f"{path}.future_usage_authorization.budget_ref")
                reservation_protocol_ref = _nullable_ref(usage["reservation_protocol_ref"], f"{path}.future_usage_authorization.reservation_protocol_ref")
                if usage_status == "approved" and (budget_ref is None or reservation_protocol_ref is None):
                    _fail("FUTURE_USAGE_EVIDENCE_INCOMPLETE", f"{path}.future_usage_authorization", "Approved future usage requires separate budget and reservation-protocol evidence references.")
                if usage_status == "pending" and (budget_ref is not None or reservation_protocol_ref is not None):
                    _fail("FUTURE_USAGE_RECORD_INCONSISTENT", f"{path}.future_usage_authorization", "Pending future usage cannot carry approval references.")
            elif future_usage is not None:
                _fail("CONFIG_FIELD_UNSUPPORTED", f"{path}.future_usage_authorization", "Future usage authorization is tracked only for planner and evaluation-baseline cells.")

            sponsor_integration = cell["sponsor_integration"]
            if cell_id in {"neatlogs", "entire"}:
                sponsor = _object(
                    sponsor_integration, f"{path}.sponsor_integration",
                    {"applicability", "status", "tool_version", "workspace_ref", "artifact_plan_ref"},
                )
                if sponsor["applicability"] != "required":
                    _fail("REQUIRED_CAPABILITY_DISABLED", f"{path}.sponsor_integration.applicability", "SPN-01 and SPN-02 remain required for v1.")
                sponsor_status = _enum(sponsor["status"], f"{path}.sponsor_integration.status", {"pending", "configured"})
                _string(sponsor["tool_version"], f"{path}.sponsor_integration.tool_version", nullable=True)
                sponsor_workspace = _nullable_ref(sponsor["workspace_ref"], f"{path}.sponsor_integration.workspace_ref")
                sponsor_plan = _nullable_ref(sponsor["artifact_plan_ref"], f"{path}.sponsor_integration.artifact_plan_ref")
                if sponsor_status == "configured" and (sponsor["tool_version"] is None or sponsor_plan is None or (cell_id == "neatlogs" and sponsor_workspace is None)):
                    _fail("SPONSOR_CONFIGURATION_INCOMPLETE", f"{path}.sponsor_integration", "Configured sponsor access requires a version, evidence plan, and applicable workspace reference.")
            elif sponsor_integration is not None:
                _fail("CONFIG_FIELD_UNSUPPORTED", f"{path}.sponsor_integration", "Sponsor setup applies only to Neatlogs and Entire cells.")

            config_path = f"{path}.configuration"
            config_fields = {
                "provider", "environment", "billing_mode", "api_family", "endpoint", "sdk",
                "client_selection", "model", "credential_env", "account_ref", "project_ref",
                "quota_status", "quota_summary", "quota_evidence_ref",
            }
            config_required = config_fields - {"client_selection"}
            if profile_version == PROFILE_VERSION:
                config_required.add("client_selection")
            config = _object(
                cell["configuration"], config_path,
                config_fields,
                required=config_required,
            )
            if config["provider"] != actual_provider:
                _fail("PROVIDER_IDENTITY_MISMATCH", f"{config_path}.provider", "Capability and configuration provider identities must match.")
            config_environment = _enum(config["environment"], f"{config_path}.environment", {"development", "test", "production"})
            if config_environment != root["environment"]:
                _fail("ENVIRONMENT_IDENTITY_MISMATCH", f"{config_path}.environment", "Per-provider environment must match the selected profile environment.")
            billing_mode = _enum(config["billing_mode"], f"{config_path}.billing_mode", {"unknown", "api_metered", "sandbox", "simulated", "not_applicable"})
            if billing_mode == "not_applicable" and cell_id != "entire":
                _fail(
                    "BILLING_MODE_NOT_APPLICABLE_UNSUPPORTED",
                    f"{config_path}.billing_mode",
                    "Non-applicable billing is supported only for the local Entire provenance capability.",
                )
            api_family = _string(config["api_family"], f"{config_path}.api_family", nullable=True, pattern=SLUG_RE)
            endpoint = _string(config["endpoint"], f"{config_path}.endpoint", nullable=True)
            if endpoint is not None and not _valid_endpoint(actual_provider, cell_id, endpoint, "inference"):
                _fail("ENDPOINT_UNSAFE", f"{config_path}.endpoint", "Endpoint is not on the provider-specific HTTPS allowlist.")
            api_surface = _validate_api_surface(cell_id, actual_provider, api_family, endpoint, config_path)
            client_selection_mode = _validate_client_selection(
                config=config,
                capability_id=cell_id,
                provider=actual_provider,
                api_surface=api_surface,
                profile_version=profile_version,
                path=config_path,
            )
            _validate_model(
                config["model"],
                cell_id,
                actual_provider,
                api_family,
                f"{config_path}.model",
                as_of,
            )
            credential_env = _string(config["credential_env"], f"{config_path}.credential_env", nullable=True)
            if credential_env is not None:
                lowered_env_name = credential_env.lower()
                has_placeholder = lowered_env_name in PLACEHOLDER_WORDS or any(
                    token in lowered_env_name for token in ("placeholder", "changeme", "replace_me", "your_")
                )
                if (
                    not ENV_NAME_RE.fullmatch(credential_env)
                    or has_placeholder
                    or actual_provider is None
                    or EXPECTED_CREDENTIAL_ENV.get(actual_provider) != credential_env
                ):
                    _fail("CREDENTIAL_REFERENCE_INVALID", f"{config_path}.credential_env", "Credential references must be environment variable names, not values or placeholders.")
            if cell_id in SECRET_ENV_REQUIRED and credential_env is None:
                # Missing is a pending readiness state, not malformed structure.
                pass
            account_ref = _nullable_ref(config["account_ref"], f"{config_path}.account_ref")
            project_ref = _nullable_ref(config["project_ref"], f"{config_path}.project_ref")
            _enum(config["quota_status"], f"{config_path}.quota_status", {"unknown", "recorded"})
            _string(config["quota_summary"], f"{config_path}.quota_summary", nullable=True)
            quota_evidence = _nullable_ref(config["quota_evidence_ref"], f"{config_path}.quota_evidence_ref")
            if config["quota_status"] == "recorded" and (quota_evidence is None or config["quota_summary"] is None):
                _fail("QUOTA_EVIDENCE_MISSING", f"{config_path}.quota_evidence_ref", "Recorded quota constraints require a summary and safe evidence reference.")
            if config["quota_status"] == "unknown" and (quota_evidence is not None or config["quota_summary"] is not None):
                _fail("QUOTA_RECORD_INCONSISTENT", f"{config_path}.quota_status", "Unknown quota status cannot carry a summary or evidence reference.")

            eligibility = _validate_status_record(
                cell["eligibility"], path=f"{path}.eligibility",
                keys={"status", "checked_at", "valid_until", "account_ref", "project_ref", "evidence_ref"},
                status_choices={"unknown", "pending", "eligible", "denied"},
                nullable_fields={"checked_at", "valid_until", "account_ref", "project_ref", "evidence_ref"},
            )
            if eligibility["status"] in {"eligible", "denied"}:
                _check_record_identity(
                    record=eligibility,
                    config={"account_ref": account_ref, "project_ref": project_ref, "model": config["model"]},
                    path=f"{path}.eligibility", as_of=as_of, require_model=False,
                )
            elif any(
                eligibility[field] is not None
                for field in ("checked_at", "valid_until", "account_ref", "project_ref", "evidence_ref")
            ):
                _fail("ELIGIBILITY_RECORD_INCONSISTENT", f"{path}.eligibility", "Unknown or pending eligibility cannot carry result fields.")

            metadata = _validate_status_record(
                cell["metadata"], path=f"{path}.metadata",
                keys={"status", "kind", "checked_at", "valid_until", "endpoint", "account_ref", "project_ref", "requested_model", "resolved_model", "evidence_ref"},
                status_choices={"not_run", "observed", "denied", "unsupported"},
                nullable_fields={"checked_at", "valid_until", "endpoint", "account_ref", "project_ref", "requested_model", "resolved_model", "evidence_ref"},
            )
            _enum(metadata["kind"], f"{path}.metadata.kind", {"none", "auth", "model_list"})
            if cell_id not in MODEL_REQUIRED and any(
                metadata[field] is not None for field in ("requested_model", "resolved_model")
            ):
                _fail(
                    "MODEL_UNSUPPORTED_FOR_CAPABILITY",
                    f"{path}.metadata",
                    "This capability does not accept model identity fields in metadata records.",
                )
            if metadata["status"] in {"observed", "denied", "unsupported"}:
                if metadata["kind"] == "none" or metadata["endpoint"] is None:
                    _fail("METADATA_RECORD_INCOMPLETE", f"{path}.metadata", "Recorded metadata outcomes require an operation and endpoint.")
                if not _valid_endpoint(actual_provider, cell_id, metadata["endpoint"], "metadata"):
                    _fail("ENDPOINT_UNSAFE", f"{path}.metadata.endpoint", "Metadata endpoint is not on the provider-specific HTTPS allowlist.")
                if metadata["kind"] == "auth" and (metadata["requested_model"] is not None or metadata["resolved_model"] is not None):
                    _fail("METADATA_RECORD_INCONSISTENT", f"{path}.metadata", "Authentication-only metadata cannot carry model identity fields.")
                _check_record_identity(
                    record=metadata,
                    config={"account_ref": account_ref, "project_ref": project_ref, "model": config["model"]},
                    path=f"{path}.metadata", as_of=as_of,
                    require_model=metadata["kind"] == "model_list",
                    resolved_policy="metadata",
                )
            elif metadata["status"] == "not_run" and any(
                metadata[field] is not None for field in ("checked_at", "valid_until", "endpoint", "account_ref", "project_ref", "requested_model", "resolved_model", "evidence_ref")
            ):
                _fail("METADATA_RECORD_INCONSISTENT", f"{path}.metadata", "Unperformed metadata checks cannot carry result fields.")
            if metadata["status"] == "not_run" and metadata["kind"] != "none":
                _fail("METADATA_RECORD_INCONSISTENT", f"{path}.metadata.kind", "Unperformed metadata checks must use kind none.")

            live = _validate_status_record(
                cell["live"], path=f"{path}.live",
                keys={"status", "dependency_mode", "checked_at", "valid_until", "endpoint", "account_ref", "project_ref", "requested_model", "resolved_model", "evidence_ref"},
                status_choices={"pending", "observed", "denied"},
                nullable_fields={"checked_at", "valid_until", "endpoint", "account_ref", "project_ref", "requested_model", "resolved_model", "evidence_ref"},
            )
            _enum(live["dependency_mode"], f"{path}.live.dependency_mode", {"not_run", "mock", "recorded", "sandbox", "live"})
            if cell_id not in MODEL_REQUIRED and any(
                live[field] is not None for field in ("requested_model", "resolved_model")
            ):
                _fail(
                    "MODEL_UNSUPPORTED_FOR_CAPABILITY",
                    f"{path}.live",
                    "This capability does not accept model identity fields in live records.",
                )
            if live["status"] in {"observed", "denied"}:
                if live["status"] == "observed" and cell_id != "entire" and api_surface is None:
                    _fail(
                        "LIVE_API_BINDING_REQUIRED",
                        f"{path}.live",
                        "Observed live evidence requires a supported provider, API family, and endpoint selection.",
                    )
                if live["status"] == "observed" and config["billing_mode"] == "unknown":
                    _fail("LIVE_BILLING_MODE_UNCONFIRMED", f"{config_path}.billing_mode", "Live provider outcomes require an explicitly recorded billing or non-applicability mode.")
                if live["status"] == "observed" and config["billing_mode"] != "api_metered":
                    _fail(
                        "LIVE_BILLING_MODE_NON_METERED",
                        f"{config_path}.billing_mode",
                        "Observed live capability requires api_metered billing; non-live modes cannot establish it.",
                    )
                if config["billing_mode"] == "unknown":
                    _fail("LIVE_BILLING_MODE_UNCONFIRMED", f"{config_path}.billing_mode", "Live provider outcomes require an explicitly recorded billing or non-applicability mode.")
                if live["status"] == "observed" and cell_id in MODEL_REQUIRED and config["model"]["policy_status"] != "approved":
                    _fail("MODEL_POLICY_PENDING", f"{config_path}.model.policy_status", "Observed live model outcomes require an owner-approved identity policy.")
                if live["status"] == "observed" and config["quota_status"] != "recorded":
                    _fail("LIVE_QUOTA_UNCONFIRMED", f"{config_path}.quota_status", "Observed live provider outcomes require recorded quota constraints.")
                if live["status"] == "observed" and future_usage is not None and future_usage["status"] != "approved":
                    _fail("FUTURE_USAGE_AUTHORIZATION_MISSING", f"{path}.future_usage_authorization", "Observed planner or baseline outcomes require separate approved budget and reservation evidence.")
                if (
                    auth_status != "approved"
                    or reservation is None
                    or auth_evidence is None
                    or cell_id not in auth["authorized_capabilities"]
                ):
                    _fail("LIVE_AUTHORIZATION_MISSING", f"{path}.live", "Recorded live provider outcomes require matching external capability authorization and reservation evidence.")
                if live["dependency_mode"] != "live":
                    _fail("LIVE_DEPENDENCY_MODE_MISMATCH", f"{path}.live.dependency_mode", "Observed provider outcomes require an actual live dependency mode.")
                if cell_id == "entire":
                    _fail("SPONSOR_EVIDENCE_SURFACE_MISMATCH", f"{path}.live", "Entire provenance is a local sponsor artifact, not a provider inference result.")
                if live["endpoint"] is None or not _valid_endpoint(actual_provider, cell_id, live["endpoint"], "inference"):
                    _fail("LIVE_RECORD_ENDPOINT_INVALID", f"{path}.live.endpoint", "Live provider evidence requires an allowlisted inference endpoint.")
                if live["endpoint"] != endpoint:
                    _fail(
                        "LIVE_ENDPOINT_MISMATCH",
                        f"{path}.live.endpoint",
                        "Live evidence endpoint must match the configured provider API endpoint.",
                    )
                _check_record_identity(
                    record=live,
                    config={"account_ref": account_ref, "project_ref": project_ref, "model": config["model"]},
                    path=f"{path}.live", as_of=as_of, require_model=cell_id in MODEL_REQUIRED,
                    resolved_policy="live",
                    response_model_required=(
                        live["status"] == "observed"
                        and (actual_provider, api_family) in LIVE_RESPONSE_MODEL_REQUIRED
                    ),
                )
            elif any(
                live[field] is not None for field in ("checked_at", "valid_until", "endpoint", "account_ref", "project_ref", "requested_model", "resolved_model", "evidence_ref")
            ):
                _fail("LIVE_RECORD_INCONSISTENT", f"{path}.live", "Pending live capability cannot carry result fields.")
            capabilities[cell_id] = cell

        if set(capabilities) != set(EXPECTED_CAPABILITIES):
            _fail("CAPABILITY_SET_INCOMPLETE", "$.capabilities", "The profile must include every required independent capability cell.")
        # Check status fields that need freshness after all cells have been parsed.
        for cell_id, cell in capabilities.items():
            for record_name in ("eligibility", "metadata", "live"):
                record = cell[record_name]
                if record["status"] in {"eligible", "denied", "observed", "unsupported"}:
                    valid_until = _date(record["valid_until"], f"$.capabilities.{cell_id}.{record_name}.valid_until", nullable=True)
                    if valid_until is not None and valid_until < as_of:
                        # Kept as a readiness blocker, not a schema error.
                        continue
        return [], root
    except ProfileError as error:
        return ([{"code": error.code, "path": error.path, "message": error.message}], None)


def _diag(code: str, capability: str | None, path: str, message: str, severity: str = "blocker") -> dict[str, str]:
    result = {"code": code, "path": path, "severity": severity, "message": message}
    if capability is not None:
        result["capability"] = capability
    return result


def assess_profile(profile: dict[str, Any], *, mode: str, as_of: date, environ: Any) -> dict[str, Any]:
    diagnostics: list[dict[str, str]] = []
    cells_out: list[dict[str, Any]] = []
    # os.environ membership can be implemented by looking up and retrieving a value.
    # Enumerate names only, then compare names without indexing the environment.
    try:
        environment_names = frozenset(environ.keys())
    except (AttributeError, TypeError):
        environment_names = frozenset()
    for cell in sorted(profile["capabilities"], key=lambda item: EXPECTED_CAPABILITIES.index(item["id"])):
        capability_id = cell["id"]
        config = cell["configuration"]
        model = config["model"]
        sponsor_config = cell["sponsor_integration"]
        usage_config = cell["future_usage_authorization"]
        if capability_id == "entire":
            client_selection_mode = "not_applicable"
        elif "client_selection" not in config:
            client_selection_mode = "sdk" if config["sdk"] is not None else "unselected"
        else:
            client_selection_mode = config["client_selection"]["mode"]
        blockers: list[dict[str, str]] = []
        config_fields_ready = (
            sponsor_config["status"] == "configured" and config["billing_mode"] != "unknown"
            if capability_id == "entire" else True
        )
        if config["billing_mode"] == "unknown":
            config_fields_ready = False
            blockers.append(_diag("BILLING_MODE_PENDING", capability_id, f"capabilities.{capability_id}.configuration.billing_mode", "Provider billing or non-applicability mode remains unconfirmed."))

        if capability_id != "entire":
            if cell["provider"] is None:
                config_fields_ready = False
                blockers.append(_diag("PROVIDER_SELECTION_PENDING", capability_id, f"capabilities.{capability_id}.provider", "Provider owner must select and record the provider."))
            if config["api_family"] is None:
                config_fields_ready = False
                blockers.append(_diag("API_SELECTION_PENDING", capability_id, f"capabilities.{capability_id}.configuration.api_family", "Owner must select the API family."))
            if config["endpoint"] is None:
                config_fields_ready = False
                blockers.append(_diag("ENDPOINT_MISSING", capability_id, f"capabilities.{capability_id}.configuration.endpoint", "An explicit allowlisted provider endpoint is required."))
            if client_selection_mode == "unselected":
                config_fields_ready = False
                blockers.append(_diag("CLIENT_SELECTION_PENDING", capability_id, f"capabilities.{capability_id}.configuration.client_selection", "Select a pinned supported SDK or explicit versioned direct-HTTP policy."))
            if capability_id in MODEL_REQUIRED:
                if model["requested"] is None:
                    config_fields_ready = False
                    blockers.append(_diag("MODEL_SELECTION_PENDING", capability_id, f"capabilities.{capability_id}.configuration.model.requested", "Owner must select an explicit model or snapshot."))
                elif model["policy_status"] != "approved":
                    config_fields_ready = False
                    blockers.append(_diag("MODEL_POLICY_PENDING", capability_id, f"capabilities.{capability_id}.configuration.model.policy_status", "Owner approval and identity policy are required."))
            env_name = config["credential_env"]
            credential_required = capability_id in SECRET_ENV_REQUIRED or (
                cell["provider"] is not None and config["api_family"] is not None and config["endpoint"] is not None
            )
            if credential_required and env_name is None:
                config_fields_ready = False
                blockers.append(_diag("CREDENTIAL_REFERENCE_MISSING", capability_id, f"capabilities.{capability_id}.configuration.credential_env", "Record a server-side credential environment variable name."))
            elif env_name is not None and env_name not in environment_names:
                config_fields_ready = False
                blockers.append(_diag("CREDENTIAL_ENV_ABSENT", capability_id, f"capabilities.{capability_id}.configuration.credential_env", "The named credential is not present in this process environment."))
            if config["account_ref"] is None or config["project_ref"] is None:
                config_fields_ready = False
                blockers.append(_diag("ACCOUNT_PROJECT_PENDING", capability_id, f"capabilities.{capability_id}.configuration", "Authorized account and project references are not recorded."))
            if config["quota_status"] != "recorded":
                config_fields_ready = False
                blockers.append(_diag("QUOTA_PENDING", capability_id, f"capabilities.{capability_id}.configuration.quota_status", "Provider quota and limit evidence is not recorded."))

        if sponsor_config is not None and sponsor_config["status"] != "configured":
            config_fields_ready = False
            blockers.append(_diag("SPONSOR_CONFIGURATION_PENDING", capability_id, f"capabilities.{capability_id}.sponsor_integration", "Sponsor setup, version, workspace/applicability, and artifact collection plan remain pending."))
        if usage_config is not None and usage_config["status"] != "approved":
            blockers.append(_diag("FUTURE_BUDGET_PENDING", capability_id, f"capabilities.{capability_id}.future_usage_authorization", "Owner-approved future metered budget and reservation protocol are not recorded."))
        model_revalidate_by = model["revalidate_by"]
        if model["policy_status"] == "approved" and model_revalidate_by is not None and date.fromisoformat(model_revalidate_by) < as_of:
            config_fields_ready = False
            blockers.append(_diag("MODEL_POLICY_STALE", capability_id, f"capabilities.{capability_id}.configuration.model.revalidate_by", "Approved model policy has passed its revalidation date."))

        eligibility = cell["eligibility"]
        if eligibility["status"] in {"unknown", "pending"}:
            blockers.append(_diag("ELIGIBILITY_PENDING", capability_id, f"capabilities.{capability_id}.eligibility", "Administrator eligibility evidence is pending."))
        elif eligibility["status"] == "denied":
            blockers.append(_diag("ELIGIBILITY_DENIED", capability_id, f"capabilities.{capability_id}.eligibility", "Administrator reported this capability as denied."))
        elif eligibility["status"] == "eligible":
            valid_until = date.fromisoformat(eligibility["valid_until"])
            if valid_until < as_of:
                blockers.append(_diag("IDENTITY_RECORD_STALE", capability_id, f"capabilities.{capability_id}.eligibility", "Eligibility evidence has passed its revalidation date."))

        metadata = cell["metadata"]
        if metadata["status"] == "observed":
            valid_until = date.fromisoformat(metadata["valid_until"])
            metadata_summary = {
                "status": "observed" if valid_until >= as_of else "stale",
                "kind": metadata["kind"],
                "endpoint": metadata["endpoint"],
                "checked_at": metadata["checked_at"],
                "valid_until": metadata["valid_until"],
                "evidence_reference_present": metadata["evidence_ref"] is not None,
                "requested_model": metadata["requested_model"],
                "resolved_model": None,
            }
            if valid_until < as_of:
                blockers.append(_diag("IDENTITY_RECORD_STALE", capability_id, f"capabilities.{capability_id}.metadata", "Metadata evidence has passed its revalidation date."))
        elif metadata["status"] == "denied":
            metadata_summary = {
                "status": "denied",
                "kind": metadata["kind"],
                "endpoint": metadata["endpoint"],
                "checked_at": metadata["checked_at"],
                "valid_until": metadata["valid_until"],
                "evidence_reference_present": metadata["evidence_ref"] is not None,
                "requested_model": metadata["requested_model"],
                "resolved_model": None,
            }
            blockers.append(_diag("METADATA_ACCESS_DENIED", capability_id, f"capabilities.{capability_id}.metadata", "Recorded metadata access was denied."))
        elif metadata["status"] == "unsupported":
            metadata_summary = {
                "status": "unsupported",
                "kind": metadata["kind"],
                "endpoint": metadata["endpoint"],
                "checked_at": metadata["checked_at"],
                "valid_until": metadata["valid_until"],
                "evidence_reference_present": metadata["evidence_ref"] is not None,
                "requested_model": metadata["requested_model"],
                "resolved_model": None,
            }
            blockers.append(_diag("METADATA_OPERATION_UNSUPPORTED", capability_id, f"capabilities.{capability_id}.metadata", "The recorded metadata operation is unsupported for this account or endpoint."))
        else:
            metadata_summary = {
                "status": metadata["status"],
                "kind": metadata["kind"],
                "endpoint": metadata["endpoint"],
                "checked_at": metadata["checked_at"],
                "valid_until": metadata["valid_until"],
                "evidence_reference_present": metadata["evidence_ref"] is not None,
                "requested_model": metadata["requested_model"],
                "resolved_model": None,
            }

        live = cell["live"]
        live_status = live["status"]
        if live_status == "pending":
            blockers.append(_diag("LIVE_CAPABILITY_PENDING", capability_id, f"capabilities.{capability_id}.live", "Live capability evidence remains pending and belongs to its implementation gate."))
        elif live_status == "denied":
            blockers.append(_diag("LIVE_CAPABILITY_DENIED", capability_id, f"capabilities.{capability_id}.live", "Recorded live provider access was denied."))
        elif live_status == "observed" and date.fromisoformat(live["valid_until"]) < as_of:
            live_status = "stale"
            blockers.append(_diag("IDENTITY_RECORD_STALE", capability_id, f"capabilities.{capability_id}.live", "Live evidence has passed its revalidation date."))

        if capability_id == "entire" and live["status"] != "observed":
            blockers.append(_diag("ENTIRE_ARTIFACT_PENDING", capability_id, f"capabilities.{capability_id}.live", "Actual Entire provenance artifact and linked revision evidence are pending."))

        denied_codes = {"ELIGIBILITY_DENIED", "METADATA_ACCESS_DENIED", "LIVE_CAPABILITY_DENIED"}
        cell_status = "denied" if any(item["code"] in denied_codes for item in blockers) else "pending"
        for blocker in blockers:
            blocker["owner_role"] = cell["owner_role"]
            blocker["action"] = cell["next_action"]
            blocker["evidence_needed"] = EVIDENCE_NEEDED[capability_id]
        diagnostics.extend(blockers)
        cells_out.append(
            {
                "id": capability_id,
                "status": cell_status,
                "owner_role": cell["owner_role"],
                "action": cell["next_action"],
                "evidence_needed": EVIDENCE_NEEDED[capability_id],
                "configuration_status": "configured" if config_fields_ready else "pending",
                "environment": config["environment"],
                "billing_mode": config["billing_mode"],
                "provider": cell["provider"],
                "api_family": config["api_family"],
                "client_selection_mode": client_selection_mode,
                "requested_model": model["requested"],
                "resolved_model": model["resolved"],
                "model_policy": model["policy_status"],
                "model_policy_revalidate_by": model_revalidate_by,
                "credential_reference": config["credential_env"],
                "account_reference_present": config["account_ref"] is not None,
                "project_reference_present": config["project_ref"] is not None,
                "credential_reference_present": (
                    config["credential_env"] in environment_names if config["credential_env"] is not None else None
                ),
                "eligibility": {
                    "status": eligibility["status"],
                    "valid_until": eligibility["valid_until"],
                    "evidence_reference_present": eligibility["evidence_ref"] is not None,
                },
                "quota": {
                    "status": config["quota_status"],
                    "summary_present": config["quota_summary"] is not None,
                    "evidence_reference_present": config["quota_evidence_ref"] is not None,
                },
                "metadata": metadata_summary,
                "live_capability": live_status,
                "live_evidence": {
                    "dependency_mode": live["dependency_mode"],
                    "endpoint": live["endpoint"],
                    "checked_at": live["checked_at"],
                    "valid_until": live["valid_until"],
                    "evidence_reference_present": live["evidence_ref"] is not None,
                },
                "blocked_consumers": cell["blocked_consumers"],
                "blockers": blockers,
                "future_usage_authorization": (
                    {
                        "status": usage_config["status"],
                        "owner_role": usage_config["owner_role"],
                        "live_test_owner_role": usage_config["live_test_owner_role"],
                        "budget_reference_present": usage_config["budget_ref"] is not None,
                        "reservation_protocol_reference_present": usage_config["reservation_protocol_ref"] is not None,
                    }
                    if usage_config is not None else None
                ),
                "sponsor_integration": (
                    {
                        "applicability": sponsor_config["applicability"],
                        "status": sponsor_config["status"],
                        "tool_version_recorded": sponsor_config["tool_version"] is not None,
                        "workspace_reference_present": sponsor_config["workspace_ref"] is not None,
                        "artifact_plan_reference_present": sponsor_config["artifact_plan_ref"] is not None,
                    }
                    if sponsor_config is not None else None
                ),
            }
        )

    status = "pending"
    exit_code = 2
    mode_result = "offline_profile_check"
    if any(cell["status"] == "denied" for cell in cells_out):
        status = "blocked"

    if mode == "metadata":
        metadata_blocker = _diag(
            "METADATA_MODE_UNSUPPORTED", None, "mode",
            "No authorized nonbillable metadata adapter is established; no request was made.", severity="unsupported",
        )
        metadata_blocker.update(
            {
                "owner_role": EXPECTED_AUTH_OWNER,
                "action": "Keep metadata checks blocked until an authorized nonbillable method and account context are established.",
                "evidence_needed": ["Approved metadata-check procedure and scoped account reference."],
            }
        )
        diagnostics.append(metadata_blocker)
        status = "unsupported"
        exit_code = 3
        mode_result = "unsupported_no_network"
    elif mode == "live":
        auth = profile["live_test_authorization"]
        approved_for_profile = auth["status"] == "approved" and auth["reservation_ref"] is not None and auth["evidence_ref"] is not None
        if not approved_for_profile:
            authorization_blocker = _diag(
                "LIVE_AUTHORIZATION_MISSING", None, "live_test_authorization",
                "External authorization and reservation evidence are required; no billable check was dispatched.", severity="unsupported",
            )
            authorization_blocker.update(
                {
                    "owner_role": auth["owner_role"],
                    "action": "Obtain external authorization and reservation evidence before proposing a billable check.",
                    "evidence_needed": ["Approved external authorization reference.", "External reservation reference."],
                }
            )
            diagnostics.append(authorization_blocker)
            mode_result = "refused_without_authorization"
        else:
            executor_blocker = _diag(
                "LIVE_EXECUTOR_UNSUPPORTED", None, "mode",
                "Authorization is recorded, but no live provider executor is implemented; no request was made.", severity="unsupported",
            )
            executor_blocker.update(
                {
                    "owner_role": auth["owner_role"],
                    "action": "A separately reviewed implementation must establish an authorized provider executor before any live request.",
                    "evidence_needed": ["Reviewed live-executor implementation and per-capability authorization."],
                }
            )
            diagnostics.append(executor_blocker)
            mode_result = "unsupported_no_live_executor"
        status = "unsupported"
        exit_code = 3

    return {
        "report_version": REPORT_VERSION,
        "tool_version": TOOL_VERSION,
        "fixture_id": profile["profile_id"],
        "environment": profile["environment"],
        "mode": mode,
        "dependency_mode": "offline" if mode == "offline" else "unsupported_no_network",
        "network_accessed": False,
        "status": status,
        "mode_result": mode_result,
        "exit_code": exit_code,
        "development_execution": {
            "access_preference": profile["development_execution"]["access_preference"],
            "surface": profile["development_execution"]["surface"],
            "model": profile["development_execution"]["model"],
            "status": profile["development_execution"]["status"],
            "engine_api_integration": "pending",
            "provider_metering": "not_established",
        },
        "live_authorization": {
            "status": profile["live_test_authorization"]["status"],
            "reservation_ref_present": profile["live_test_authorization"]["reservation_ref"] is not None,
        },
        "capabilities": cells_out,
        "diagnostics": diagnostics,
    }


def _load_profile(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        if path.is_symlink():
            return None, "PROFILE_FILE_UNSAFE"
        metadata = path.stat()
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o022:
            return None, "PROFILE_FILE_UNSAFE"
        if metadata.st_size > MAX_PROFILE_JSON_BYTES:
            return None, "PROFILE_FILE_TOO_LARGE"
        raw = path.read_bytes()
    except OSError:
        return None, "PROFILE_FILE_UNREADABLE"
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None, "PROFILE_JSON_INVALID"
    if _json_nesting_exceeds_limit(text):
        return None, "PROFILE_JSON_DEPTH_LIMIT"
    try:
        profile = json.loads(
            text,
            object_pairs_hook=_pairs_without_duplicates,
            parse_int=_parse_bounded_json_integer,
        )
    except DuplicateKeyError:
        return None, "PROFILE_JSON_DUPLICATE_KEY"
    except JsonIntegerLimitError:
        return None, "PROFILE_JSON_INTEGER_LIMIT"
    except json.JSONDecodeError:
        return None, "PROFILE_JSON_INVALID"
    except RecursionError:
        return None, "PROFILE_JSON_DEPTH_LIMIT"
    except ValueError:
        return None, "PROFILE_JSON_INVALID"
    if not isinstance(profile, dict):
        return None, "PROFILE_ROOT_INVALID"
    return profile, None


def _component_revision() -> dict[str, str | bool]:
    repo_root = Path(__file__).resolve().parent.parent
    try:
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=repo_root, capture_output=True, text=True,
            check=False, env={"PATH": os.defpath, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
                              "GIT_CONFIG_SYSTEM": os.devnull, "GIT_OPTIONAL_LOCKS": "0", "GIT_TERMINAL_PROMPT": "0"},
        )
        if head.returncode != 0:
            return {"repository": "agent-harness", "revision": "unknown", "working_tree_modified": False}
        status = subprocess.run(
            ["git", "status", "--porcelain"], cwd=repo_root, capture_output=True, text=True,
            check=False, env={"PATH": os.defpath, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
                              "GIT_CONFIG_SYSTEM": os.devnull, "GIT_OPTIONAL_LOCKS": "0", "GIT_TERMINAL_PROMPT": "0"},
        )
        return {
            "repository": "agent-harness",
            "revision": head.stdout.strip() if re.fullmatch(r"[0-9a-fA-F]{40}\n?", head.stdout) else "unknown",
            "working_tree_modified": bool(status.stdout) if status.returncode == 0 else False,
        }
    except OSError:
        return {"repository": "agent-harness", "revision": "unknown", "working_tree_modified": False}


def _report_invalid(code: str, *, mode: str, revision: dict[str, Any]) -> dict[str, Any]:
    return {
        "report_version": REPORT_VERSION,
        "tool_version": TOOL_VERSION,
        "fixture_id": None,
        "mode": mode,
        "dependency_mode": "unsupported_no_network",
        "network_accessed": False,
        "status": "invalid",
        "mode_result": "profile_rejected",
        "exit_code": 1,
        "component": revision,
        "diagnostics": [{"code": code, "path": "$", "severity": "error", "message": "Profile could not be loaded safely."}],
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate provider access profile without making provider calls.")
    parser.add_argument("--profile", required=True, help="Path to a local provider profile JSON file.")
    parser.add_argument("--mode", required=True, choices={"offline", "metadata", "live"})
    parser.add_argument("--as-of", help="Inspection date in YYYY-MM-DD (defaults to current UTC date).")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        as_of = date.fromisoformat(args.as_of) if args.as_of else datetime.now(timezone.utc).date()
    except ValueError:
        report = _report_invalid("INSPECTION_DATE_INVALID", mode=args.mode, revision=_component_revision())
        print(json.dumps(report, sort_keys=True, indent=2))
        return 1
    profile, load_error = _load_profile(Path(args.profile))
    revision = _component_revision()
    if load_error is not None:
        report = _report_invalid(load_error, mode=args.mode, revision=revision)
        print(json.dumps(report, sort_keys=True, indent=2))
        return 1
    errors, validated = validate_profile(profile, as_of=as_of)
    if errors or validated is None:
        error = errors[0] if errors else {"code": "PROFILE_INVALID", "path": "$", "message": "Profile failed validation."}
        report = _report_invalid(error["code"], mode=args.mode, revision=revision)
        report["diagnostics"][0]["path"] = error["path"]
        report["diagnostics"][0]["message"] = error["message"]
        print(json.dumps(report, sort_keys=True, indent=2))
        return 1
    report = assess_profile(validated, mode=args.mode, as_of=as_of, environ=os.environ)
    report["component"] = revision
    print(json.dumps(report, sort_keys=True, indent=2))
    return int(report["exit_code"])


if __name__ == "__main__":
    sys.exit(main())
