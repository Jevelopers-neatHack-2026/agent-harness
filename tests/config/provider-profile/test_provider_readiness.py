import copy
import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "tools"))
import verify_provider_readiness as readiness  # noqa: E402


FIXTURE_PATH = REPO_ROOT / "fixtures" / "provider-profile.json"
AS_OF = date(2026, 10, 10)


def load_fixture():
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def run_cli(profile_bytes, mode, python_executable=None):
    with tempfile.TemporaryDirectory() as temp_dir:
        profile_path = Path(temp_dir) / "profile.json"
        profile_path.write_bytes(profile_bytes)
        profile_path.chmod(0o600)
        return subprocess.run(
            [
                python_executable or sys.executable,
                "-B",
                str(REPO_ROOT / "tools" / "verify_provider_readiness.py"),
                "--profile",
                str(profile_path),
                "--mode",
                mode,
                "--as-of",
                "2026-10-10",
            ],
            cwd=REPO_ROOT,
            env={"PATH": os.defpath, "PYTHONDONTWRITEBYTECODE": "1"},
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )


def cell_for(profile, capability_id):
    return next(item for item in profile["capabilities"] if item["id"] == capability_id)


def select_direct_http(config, *, api_version="v1", policy_ref="evidence:runtime/synthetic-api-policy"):
    config["sdk"] = None
    config["client_selection"] = {
        "mode": "direct_http",
        "api_version": api_version,
        "policy_ref": policy_ref,
    }


def select_pinned_sdk(config, *, name="typesafe-sdk-js", version="1.2.3", source_revision="a" * 40):
    config["sdk"] = {"name": name, "version": version, "source_revision": source_revision}
    config["client_selection"] = {"mode": "sdk", "api_version": None, "policy_ref": None}


def set_approved_model(cell, requested, *, resolved, identity_kind="pinned", expected_resolved=None):
    cell["configuration"]["model"].update(
        {
            "requested": requested,
            "resolved": resolved,
            "identity_kind": identity_kind,
            "policy_status": "approved",
            "approved_model": requested,
            "expected_resolved": expected_resolved,
            "approval_ref": "evidence:synthetic/model-policy",
            "approved_by_role": cell["owner_role"],
            "approved_at": "2026-10-01",
            "revalidate_by": "2026-11-01",
        }
    )


def set_profile_version(profile, version):
    profile["schema_version"] = version
    for cell in profile["capabilities"]:
        config = cell["configuration"]
        selection = config.pop("client_selection", None)
        if version == "1.1":
            config["client_selection"] = selection or {
                "mode": "unselected",
                "api_version": None,
                "policy_ref": None,
            }
        elif selection is not None and selection["mode"] in {"sdk", "direct_http"} and config["sdk"] is None:
            sdk_name = "typesafe-sdk-js" if config["provider"] == "typesafe" else "openai"
            config["sdk"] = {"name": sdk_name, "version": "1.2.3", "source_revision": "a" * 40}
    return profile


def add_jev_admin_record(profile, *, valid_until="2026-10-20"):
    cell = cell_for(profile, "jev")
    config = cell["configuration"]
    config["account_ref"] = "urn:typesafe:account:development"
    config["project_ref"] = "urn:agent-harness:project:acv1"
    config["quota_status"] = "recorded"
    config["quota_summary"] = "Account limit documented; numeric value retained in administrator evidence."
    config["quota_evidence_ref"] = "evidence:admin/jev-quota"
    select_direct_http(config)
    config["model"].update(
        {
            "requested": "jev-1.13.0",
            "identity_kind": "pinned",
            "policy_status": "approved",
            "approved_model": "jev-1.13.0",
            "approval_ref": "evidence:runtime/jev-model-policy",
            "approved_by_role": "Agent and research",
            "approved_at": "2026-10-01",
            "revalidate_by": "2026-11-01",
        }
    )
    cell["eligibility"].update(
        {
            "status": "eligible",
            "checked_at": "2026-10-01",
            "valid_until": valid_until,
            "account_ref": config["account_ref"],
            "project_ref": config["project_ref"],
            "evidence_ref": "evidence:admin/jev-eligibility",
        }
    )
    return cell


def make_live_observation(profile, capability_id, *, api_family, endpoint, model=None):
    cell = cell_for(profile, capability_id)
    config = cell["configuration"]
    if capability_id in {"planner_reasoning", "generative_baseline"}:
        cell["provider"] = "openai"
        config["provider"] = "openai"
    account_ref = f"urn:synthetic:{capability_id}:account"
    project_ref = f"urn:synthetic:{capability_id}:project"
    config.update(
        billing_mode="api_metered",
        api_family=api_family,
        endpoint=endpoint,
        credential_env={"typesafe": "TYPESAFE_API_KEY", "openai": "OPENAI_API_KEY", "neatlogs": "NEATLOGS_API_KEY"}.get(cell["provider"]),
        account_ref=account_ref,
        project_ref=project_ref,
        quota_status="recorded",
        quota_summary="synthetic quota record",
        quota_evidence_ref=f"evidence:synthetic/{capability_id}-quota",
    )
    select_direct_http(config)
    if model is not None:
        config["model"].update(
            {
                "requested": model,
                "resolved": model,
                "identity_kind": "pinned",
                "policy_status": "approved",
                "approved_model": model,
                "approval_ref": f"evidence:synthetic/{capability_id}-model-policy",
                "approved_by_role": cell["owner_role"],
                "approved_at": "2026-10-01",
                "revalidate_by": "2026-11-01",
            }
        )
    cell["eligibility"].update(
        {
            "status": "eligible",
            "checked_at": "2026-10-01",
            "valid_until": "2026-11-01",
            "account_ref": account_ref,
            "project_ref": project_ref,
            "evidence_ref": f"evidence:synthetic/{capability_id}-eligibility",
        }
    )
    if cell["future_usage_authorization"] is not None:
        cell["future_usage_authorization"].update(
            {
                "status": "approved",
                "budget_ref": f"evidence:synthetic/{capability_id}-budget",
                "reservation_protocol_ref": f"evidence:synthetic/{capability_id}-reservation-protocol",
            }
        )
    profile["live_test_authorization"].update(
        {
            "status": "approved",
            "reservation_ref": f"evidence:synthetic/{capability_id}-reservation",
            "evidence_ref": f"evidence:synthetic/{capability_id}-authorization",
            "authorized_capabilities": [capability_id],
        }
    )
    cell["live"].update(
        {
            "status": "observed",
            "dependency_mode": "live",
            "checked_at": "2026-10-01",
            "valid_until": "2026-11-01",
            "endpoint": endpoint,
            "account_ref": account_ref,
            "project_ref": project_ref,
            "requested_model": model,
            "resolved_model": model,
            "evidence_ref": f"evidence:synthetic/{capability_id}-live",
        }
    )
    return cell


def add_jev_model_list_record(cell, *, valid_until="2026-10-20"):
    config = cell["configuration"]
    cell["metadata"].update(
        {
            "status": "observed",
            "kind": "model_list",
            "checked_at": "2026-10-01",
            "valid_until": valid_until,
            "endpoint": "https://api.typesafe.ai/v1/models",
            "account_ref": config["account_ref"],
            "project_ref": config["project_ref"],
            "requested_model": config["model"]["requested"],
            "resolved_model": None,
            "evidence_ref": "evidence:admin/jev-model-list",
        }
    )


class MembershipOnlyEnvironment:
    """Proves the checker asks whether a secret name exists, never reads it."""

    def keys(self):
        return ("TYPESAFE_API_KEY",)

    def __contains__(self, key):
        return key == "TYPESAFE_API_KEY"

    def __getitem__(self, key):
        raise AssertionError("credential values must never be read")


class ProviderReadinessTests(unittest.TestCase):
    def validate(self, profile):
        return readiness.validate_profile(profile, as_of=AS_OF)

    def first_issue(self, profile):
        errors, validated = self.validate(profile)
        self.assertIsNone(validated)
        self.assertTrue(errors)
        return errors[0]

    def test_fixture_is_valid_but_pending_and_keeps_all_gates_independent(self):
        profile = load_fixture()
        errors, validated = self.validate(profile)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})

        self.assertEqual(report["status"], "pending")
        self.assertEqual(report["exit_code"], 2)
        self.assertEqual(report["report_version"], "1.1")
        self.assertFalse(report["network_accessed"])
        self.assertEqual(profile["schema_version"], "1.1")
        self.assertEqual([item["id"] for item in report["capabilities"]], list(readiness.EXPECTED_CAPABILITIES))
        self.assertEqual(report["development_execution"]["access_preference"], "codex_subscription")
        self.assertEqual(report["development_execution"]["model"], "gpt-image-2")
        self.assertEqual(report["development_execution"]["engine_api_integration"], "pending")
        self.assertEqual(report["development_execution"]["provider_metering"], "not_established")

        by_id = {item["id"]: item for item in report["capabilities"]}
        self.assertEqual(by_id["jev"]["eligibility"]["status"], "pending")
        self.assertEqual(by_id["gpt_image"]["requested_model"], None)
        self.assertEqual(by_id["gpt_image"]["billing_mode"], "unknown")
        self.assertEqual(by_id["planner_reasoning"]["provider"], None)
        self.assertEqual(by_id["generative_baseline"]["provider"], None)
        self.assertEqual(by_id["neatlogs"]["blocked_consumers"], ["SPN-01"])
        self.assertEqual(by_id["entire"]["blocked_consumers"], ["SPN-02"])
        self.assertEqual(by_id["gpt_image"]["status"], "pending")
        self.assertEqual(by_id["planner_reasoning"]["status"], "pending")
        self.assertEqual(by_id["generative_baseline"]["status"], "pending")
        self.assertEqual(by_id["gpt_image"]["client_selection_mode"], "unselected")
        self.assertIn("CLIENT_SELECTION_PENDING", {item["code"] for item in by_id["gpt_image"]["blockers"]})
        self.assertIn("CREDENTIAL_ENV_ABSENT", {item["code"] for item in by_id["jev"]["blockers"]})

    def test_offline_serialization_is_deterministic_for_same_inputs(self):
        profile = load_fixture()
        report_a = readiness.assess_profile(profile, mode="offline", as_of=AS_OF, environ={})
        report_b = readiness.assess_profile(profile, mode="offline", as_of=AS_OF, environ={})
        self.assertEqual(
            json.dumps(report_a, sort_keys=True, separators=(",", ":")),
            json.dumps(report_b, sort_keys=True, separators=(",", ":")),
        )

    def test_missing_environment_is_rejected_with_sanitized_typed_error(self):
        profile = load_fixture()
        profile["environment"] = ""
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "CONFIG_VALUE_INVALID")
        self.assertEqual(issue["path"], "$.environment")
        self.assertEqual(issue["message"], "Expected a non-empty string.")

    def test_missing_credential_reference_is_pending_not_an_auth_success(self):
        profile = load_fixture()
        cell_for(profile, "jev")["configuration"]["credential_env"] = None
        errors, validated = self.validate(profile)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
        jev = next(item for item in report["capabilities"] if item["id"] == "jev")
        self.assertIn("CREDENTIAL_REFERENCE_MISSING", {item["code"] for item in jev["blockers"]})
        self.assertIsNone(jev["credential_reference"])
        self.assertEqual(jev["eligibility"]["status"], "pending")

    def test_empty_credential_reference_is_rejected(self):
        profile = load_fixture()
        cell_for(profile, "jev")["configuration"]["credential_env"] = ""
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "CONFIG_VALUE_INVALID")

    def test_placeholder_credential_reference_is_rejected(self):
        profile = load_fixture()
        cell_for(profile, "jev")["configuration"]["credential_env"] = "YOUR_TYPESAFE_API_KEY"
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "CREDENTIAL_REFERENCE_INVALID")
        self.assertNotIn("YOUR_TYPESAFE_API_KEY", issue["message"])

    def test_provider_rejects_arbitrary_and_insecure_endpoints(self):
        for endpoint in (
            "http://api.typesafe.ai/v1/systemone",
            "https://attacker.example/v1/systemone",
            "https://api.typesafe.ai/v1/systemone?token=hidden",
        ):
            with self.subTest(endpoint=endpoint):
                profile = load_fixture()
                cell_for(profile, "jev")["configuration"]["endpoint"] = endpoint
                issue = self.first_issue(profile)
                self.assertEqual(issue["code"], "ENDPOINT_UNSAFE")
                self.assertNotIn(endpoint, issue["message"])

    def test_api_family_provider_and_endpoint_are_bound_to_the_capability(self):
        profile = load_fixture()
        image = cell_for(profile, "gpt_image")["configuration"]
        image.update(api_family="arbitrary_unimplemented_api", endpoint="https://api.openai.com/v1/images/generations")
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "API_FAMILY_UNSUPPORTED")
        self.assertNotIn("arbitrary_unimplemented_api", issue["message"])

        profile = load_fixture()
        image = cell_for(profile, "gpt_image")["configuration"]
        image.update(api_family="images", endpoint="https://api.openai.com/v1/chat/completions")
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "API_ENDPOINT_MISMATCH")

        profile = load_fixture()
        jev = cell_for(profile, "jev")
        jev["configuration"]["provider"] = "openai"
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "PROVIDER_IDENTITY_MISMATCH")

        profile = load_fixture()
        planner = cell_for(profile, "planner_reasoning")
        planner["provider"] = planner["configuration"]["provider"] = "typesafe"
        planner["configuration"].update(
            api_family="systemone_v1",
            endpoint="https://api.typesafe.ai/v1/systemone",
        )
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "API_FAMILY_UNSUPPORTED")

    def test_supported_api_surface_controls_for_each_selected_provider_path(self):
        cases = (
            ("jev", "typesafe", "systemone_v1", "https://api.typesafe.ai/v1/systemone"),
            ("gpt_image", "openai", "images", "https://api.openai.com/v1/images/generations"),
            ("planner_reasoning", "openai", "responses", "https://api.openai.com/v1/responses"),
            ("generative_baseline", "openai", "chat_completions", "https://api.openai.com/v1/chat/completions"),
            ("neatlogs", "neatlogs", "trace_ingest", "https://ingest.neatlogs.com/v1/traces"),
        )
        for capability_id, provider, family, endpoint in cases:
            with self.subTest(capability=capability_id, family=family):
                profile = load_fixture()
                cell = cell_for(profile, capability_id)
                cell["provider"] = provider
                config = cell["configuration"]
                config["provider"] = provider
                config.update(api_family=family, endpoint=endpoint)
                select_direct_http(config)
                errors, _ = self.validate(profile)
                self.assertEqual(errors, [])

    def test_unselected_api_or_client_stays_pending_and_v1_0_remains_compatible(self):
        profile = load_fixture()
        image = cell_for(profile, "gpt_image")["configuration"]
        image.update(api_family="images", endpoint="https://api.openai.com/v1/images/generations")
        errors, validated = self.validate(profile)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
        gpt = next(item for item in report["capabilities"] if item["id"] == "gpt_image")
        self.assertEqual(gpt["configuration_status"], "pending")
        self.assertEqual(gpt["client_selection_mode"], "unselected")
        self.assertIn("CLIENT_SELECTION_PENDING", {item["code"] for item in gpt["blockers"]})

        legacy = load_fixture()
        legacy["schema_version"] = "1.0"
        for cell in legacy["capabilities"]:
            cell["configuration"].pop("client_selection")
        errors, validated = self.validate(legacy)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
        jev = next(item for item in report["capabilities"] if item["id"] == "jev")
        self.assertEqual(jev["client_selection_mode"], "unselected")
        self.assertEqual(jev["configuration_status"], "pending")

        select_pinned_sdk(cell_for(legacy, "jev")["configuration"])
        # Schema 1.0 has no client_selection field; a fully pinned legacy SDK
        # object remains an explicit client selection for backward compatibility.
        cell_for(legacy, "jev")["configuration"].pop("client_selection")
        errors, validated = self.validate(legacy)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
        jev = next(item for item in report["capabilities"] if item["id"] == "jev")
        self.assertEqual(jev["client_selection_mode"], "sdk")

    def test_sdk_and_direct_http_selection_require_pins_and_versioned_policy(self):
        profile = load_fixture()
        image = cell_for(profile, "gpt_image")["configuration"]
        image.update(api_family="images", endpoint="https://api.openai.com/v1/images/generations")
        select_direct_http(image)
        errors, validated = self.validate(profile)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
        gpt = next(item for item in report["capabilities"] if item["id"] == "gpt_image")
        self.assertEqual(gpt["client_selection_mode"], "direct_http")
        self.assertNotIn("CLIENT_SELECTION_PENDING", {item["code"] for item in gpt["blockers"]})

        image["client_selection"]["api_version"] = "v2"
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "DIRECT_HTTP_API_VERSION_MISMATCH")

        profile = load_fixture()
        config = cell_for(profile, "jev")["configuration"]
        select_pinned_sdk(config)
        errors, validated = self.validate(profile)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
        jev = next(item for item in report["capabilities"] if item["id"] == "jev")
        self.assertEqual(jev["client_selection_mode"], "sdk")

        config["sdk"]["version"] = "^1.2.3"
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "CONFIG_VALUE_INVALID")
        self.assertNotIn("^1.2.3", issue["message"])

        config["sdk"]["version"] = "1.2.3"
        config["sdk"]["source_revision"] = "main"
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "CONFIG_VALUE_INVALID")
        self.assertNotIn("main", issue["message"])

        profile = load_fixture()
        config = cell_for(profile, "jev")["configuration"]
        select_pinned_sdk(config, name="unreviewed-sdk")
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "SDK_UNSUPPORTED")

        profile = load_fixture()
        config = cell_for(profile, "jev")["configuration"]
        config["client_selection"].update(mode="sdk")
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "CLIENT_SELECTION_INCONSISTENT")

    def test_image_live_observation_matches_the_configured_api_endpoint(self):
        profile = load_fixture()
        cell = make_live_observation(
            profile,
            "gpt_image",
            api_family="images",
            endpoint="https://api.openai.com/v1/images/generations",
            model="gpt-image-2-2026-04-21",
        )
        errors, validated = self.validate(profile)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
        image = next(item for item in report["capabilities"] if item["id"] == "gpt_image")
        self.assertEqual(image["live_capability"], "observed")
        self.assertEqual(image["live_evidence"]["endpoint"], cell["configuration"]["endpoint"])

        cell["live"]["endpoint"] = "https://api.openai.com/v1/chat/completions"
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "LIVE_ENDPOINT_MISMATCH")

        cell["live"]["endpoint"] = "https://api.openai.com/v1/images/generations"
        cell["configuration"]["endpoint"] = "https://api.openai.com/v1/chat/completions"
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "API_ENDPOINT_MISMATCH")

    def test_sponsor_only_model_fields_are_rejected_by_cli_without_echo(self):
        for capability_id in ("neatlogs", "entire"):
            for record_name in ("model", "metadata", "live"):
                for field in ("requested", "resolved"):
                    marker = f"SYNTHETIC_PRIVATE_{capability_id}_{record_name}_{field}"
                    with self.subTest(capability=capability_id, record=record_name, field=field):
                        profile = load_fixture()
                        cell = cell_for(profile, capability_id)
                        if record_name == "model":
                            record = cell["configuration"]["model"]
                            record[field] = marker
                            if field == "requested":
                                record["identity_kind"] = "pinned"
                        else:
                            cell[record_name][f"{field}_model"] = marker
                        with tempfile.TemporaryDirectory() as temp_dir:
                            profile_path = Path(temp_dir) / "profile.json"
                            profile_path.write_text(json.dumps(profile), encoding="utf-8")
                            stdout = io.StringIO()
                            stderr = io.StringIO()
                            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr), mock.patch(
                                "socket.create_connection", side_effect=AssertionError("network forbidden")
                            ):
                                exit_code = readiness.main(
                                    ["--profile", str(profile_path), "--mode", "offline", "--as-of", "2026-10-10"]
                                )
                        self.assertEqual(exit_code, 1)
                        rendered = stdout.getvalue() + stderr.getvalue()
                        self.assertNotIn(marker, rendered)
                        report = json.loads(stdout.getvalue())
                        self.assertEqual(report["status"], "invalid")
                        self.assertEqual(report["diagnostics"][0]["code"], "MODEL_UNSUPPORTED_FOR_CAPABILITY")

    def test_wrong_provider_model_identifier_is_rejected(self):
        profile = load_fixture()
        cell_for(profile, "jev")["configuration"]["model"].update(
            {"requested": "not-a-typesafe-model", "identity_kind": "pinned"}
        )
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "MODEL_UNSUPPORTED")
        self.assertNotIn("not-a-typesafe-model", issue["message"])

    def test_unselected_planner_provider_cannot_carry_a_proposed_model(self):
        profile = load_fixture()
        cell = cell_for(profile, "planner_reasoning")
        cell["configuration"]["model"].update(
            {"requested": "model-example", "identity_kind": "pinned"}
        )
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "PROVIDER_SELECTION_REQUIRED")
        self.assertNotIn("model-example", issue["message"])

    def test_blocked_consumer_relationships_cannot_drift_from_assigned_issue(self):
        profile = load_fixture()
        cell_for(profile, "planner_reasoning")["blocked_consumers"].append("unrelated-consumer")
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "BLOCKED_CONSUMERS_MISMATCH")

    def test_unsupported_profile_version_is_rejected(self):
        profile = load_fixture()
        profile["schema_version"] = "2.0"
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "PROFILE_VERSION_UNSUPPORTED")

    def test_codex_development_preference_does_not_imply_image_api_access(self):
        profile = load_fixture()
        errors, validated = self.validate(profile)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
        image = next(item for item in report["capabilities"] if item["id"] == "gpt_image")
        self.assertEqual(report["development_execution"]["access_preference"], "codex_subscription")
        self.assertEqual(report["development_execution"]["engine_api_integration"], "pending")
        self.assertEqual(image["api_family"], None)
        self.assertEqual(image["requested_model"], None)
        self.assertEqual(image["eligibility"]["status"], "pending")
        self.assertEqual(image["live_capability"], "pending")

        image_cell = cell_for(profile, "gpt_image")
        config = image_cell["configuration"]
        config.update(api_family="images", endpoint="https://api.openai.com/v1/images/generations")
        select_direct_http(config)
        set_approved_model(
            image_cell,
            "gpt-image-2",
            resolved="gpt-image-2-2026-04-21",
            identity_kind="alias",
            expected_resolved="gpt-image-2-2026-04-21",
        )
        errors, validated = self.validate(profile)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
        image = next(item for item in report["capabilities"] if item["id"] == "gpt_image")
        self.assertEqual(image["configuration_status"], "pending")
        self.assertEqual(image["requested_model"], "gpt-image-2")
        self.assertNotIn("MODEL_SELECTION_PENDING", {item["code"] for item in image["blockers"]})
        self.assertNotIn("MODEL_POLICY_PENDING", {item["code"] for item in image["blockers"]})
        self.assertEqual(image["eligibility"]["status"], "pending")
        self.assertEqual(image["live_capability"], "pending")
        self.assertEqual(report["development_execution"]["provider_metering"], "not_established")

    def test_bounded_model_aliases_require_exact_approved_resolution_for_both_versions(self):
        for version in ("1.0", "1.1"):
            with self.subTest(schema_version=version):
                profile = set_profile_version(load_fixture(), version)
                planner = cell_for(profile, "planner_reasoning")
                planner["provider"] = planner["configuration"]["provider"] = "openai"
                planner["configuration"].update(
                    api_family="responses", endpoint="https://api.openai.com/v1/responses"
                )
                if version == "1.1":
                    select_direct_http(planner["configuration"])
                else:
                    planner["configuration"]["sdk"] = {
                        "name": "openai", "version": "1.2.3", "source_revision": "a" * 40
                    }

                model = planner["configuration"]["model"]
                model.update(
                    {
                        "requested": "gpt-4.1",
                        "resolved": "gpt-4.1-2025-04-14",
                        "identity_kind": "pinned",
                        "policy_status": "approved",
                        "approved_model": "gpt-4.1",
                        "approval_ref": "evidence:synthetic/model-policy",
                        "approved_by_role": planner["owner_role"],
                        "approved_at": "2026-10-01",
                        "revalidate_by": "2026-11-01",
                    }
                )
                issue = self.first_issue(profile)
                self.assertEqual(issue["code"], "MODEL_ALIAS_POLICY_REQUIRED")

                set_approved_model(
                    planner,
                    "gpt-4.1",
                    resolved="gpt-4.1-2025-04-14",
                    identity_kind="alias",
                    expected_resolved="gpt-4.1-2025-04-14",
                )
                errors, _ = self.validate(profile)
                self.assertEqual(errors, [])
                model["revalidate_by"] = "2026-10-09"
                errors, validated = self.validate(profile)
                self.assertEqual(errors, [])
                report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
                planner_report = next(
                    item for item in report["capabilities"] if item["id"] == "planner_reasoning"
                )
                self.assertIn("MODEL_POLICY_STALE", {item["code"] for item in planner_report["blockers"]})
                model["revalidate_by"] = "2026-11-01"

                model["resolved"] = "gpt-4.1-2025-01-01"
                issue = self.first_issue(profile)
                self.assertEqual(issue["code"], "MODEL_RESOLUTION_MISMATCH")
                model["resolved"] = "gpt-4.1-2025-04-14"

                model["expected_resolved"] = "gpt-4.1-2025-01-01"
                issue = self.first_issue(profile)
                self.assertEqual(issue["code"], "MODEL_ALIAS_RESOLUTION_INVALID")

                set_approved_model(
                    planner,
                    "gpt-4.1-latest",
                    resolved="gpt-4.1-latest",
                    identity_kind="alias",
                    expected_resolved="gpt-4.1-2025-04-14",
                )
                issue = self.first_issue(profile)
                self.assertEqual(issue["code"], "MODEL_UNSUPPORTED")
                self.assertNotIn("gpt-4.1-latest", issue["message"])

                set_approved_model(
                    planner,
                    "gpt-4.1-2025-04-14",
                    resolved="gpt-4.1-2025-04-14",
                )
                errors, _ = self.validate(profile)
                self.assertEqual(errors, [])

    def test_image_alias_and_snapshot_policy_is_exact_and_time_bounded_for_both_versions(self):
        for version in ("1.0", "1.1"):
            with self.subTest(schema_version=version):
                profile = set_profile_version(load_fixture(), version)
                image = cell_for(profile, "gpt_image")
                config = image["configuration"]
                config.update(api_family="images", endpoint="https://api.openai.com/v1/images/generations")
                if version == "1.1":
                    select_direct_http(config)
                else:
                    config["sdk"] = {"name": "openai", "version": "1.2.3", "source_revision": "a" * 40}
                set_approved_model(
                    image,
                    "gpt-image-2",
                    resolved="gpt-image-2-2026-04-21",
                    identity_kind="alias",
                    expected_resolved="gpt-image-2-2026-04-21",
                )
                errors, validated = self.validate(profile)
                self.assertEqual(errors, [])
                report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
                self.assertEqual(report["status"], "pending")
                self.assertEqual(report["development_execution"]["provider_metering"], "not_established")

                config["model"]["expected_resolved"] = "gpt-image-2-2026-01-15"
                issue = self.first_issue(profile)
                self.assertEqual(issue["code"], "MODEL_ALIAS_RESOLUTION_INVALID")

                set_approved_model(
                    image,
                    "gpt-image-2-2026-04-21",
                    resolved="gpt-image-2-2026-04-21",
                )
                errors, _ = self.validate(profile)
                self.assertEqual(errors, [])

    def test_capability_and_api_model_policies_reject_text_image_crossings(self):
        for version in ("1.0", "1.1"):
            with self.subTest(schema_version=version, case="image pin"):
                profile = load_fixture()
                image = make_live_observation(
                    profile,
                    "gpt_image",
                    api_family="images",
                    endpoint="https://api.openai.com/v1/images/generations",
                    model="gpt-image-2-2026-04-21",
                )
                set_profile_version(profile, version)
                errors, _ = self.validate(profile)
                self.assertEqual(errors, [])
                self.assertEqual(image["configuration"]["model"]["requested"], "gpt-image-2-2026-04-21")

            with self.subTest(schema_version=version, case="text alias"):
                profile = load_fixture()
                planner = make_live_observation(
                    profile,
                    "planner_reasoning",
                    api_family="responses",
                    endpoint="https://api.openai.com/v1/responses",
                )
                set_approved_model(
                    planner,
                    "gpt-4.1",
                    resolved="gpt-4.1-2025-04-14",
                    identity_kind="alias",
                    expected_resolved="gpt-4.1-2025-04-14",
                )
                planner["live"].update(
                    requested_model="gpt-4.1",
                    resolved_model="gpt-4.1-2025-04-14",
                )
                set_profile_version(profile, version)
                errors, _ = self.validate(profile)
                self.assertEqual(errors, [])

            with self.subTest(schema_version=version, case="text on image endpoint"):
                profile = load_fixture()
                image = make_live_observation(
                    profile,
                    "gpt_image",
                    api_family="images",
                    endpoint="https://api.openai.com/v1/images/generations",
                )
                set_approved_model(
                    image,
                    "gpt-4.1",
                    resolved="gpt-4.1-2025-04-14",
                    identity_kind="alias",
                    expected_resolved="gpt-4.1-2025-04-14",
                )
                image["live"].update(
                    requested_model="gpt-4.1",
                    resolved_model="gpt-4.1-2025-04-14",
                )
                set_profile_version(profile, version)
                issue = self.first_issue(profile)
                self.assertEqual(issue["code"], "MODEL_UNSUPPORTED_FOR_CAPABILITY")

            with self.subTest(schema_version=version, case="image model on text endpoint"):
                profile = load_fixture()
                planner = make_live_observation(
                    profile,
                    "planner_reasoning",
                    api_family="responses",
                    endpoint="https://api.openai.com/v1/responses",
                )
                set_approved_model(
                    planner,
                    "gpt-image-2-2026-04-21",
                    resolved="gpt-image-2-2026-04-21",
                )
                planner["live"].update(
                    requested_model="gpt-image-2-2026-04-21",
                    resolved_model="gpt-image-2-2026-04-21",
                )
                set_profile_version(profile, version)
                issue = self.first_issue(profile)
                self.assertEqual(issue["code"], "MODEL_UNSUPPORTED_FOR_CAPABILITY")

            with self.subTest(schema_version=version, case="unknown model"):
                profile = load_fixture()
                image = make_live_observation(
                    profile,
                    "gpt_image",
                    api_family="images",
                    endpoint="https://api.openai.com/v1/images/generations",
                )
                set_approved_model(
                    image,
                    "o3-2025-04-16",
                    resolved="o3-2025-04-16",
                )
                image["live"].update(
                    requested_model="o3-2025-04-16",
                    resolved_model="o3-2025-04-16",
                )
                set_profile_version(profile, version)
                issue = self.first_issue(profile)
                self.assertEqual(issue["code"], "MODEL_UNSUPPORTED")
                self.assertNotIn("o3-2025-04-16", issue["message"])

    def test_generative_baseline_uses_its_separate_text_api_model_policy(self):
        for version in ("1.0", "1.1"):
            with self.subTest(schema_version=version, case="supported baseline text pin"):
                profile = load_fixture()
                baseline = make_live_observation(
                    profile,
                    "generative_baseline",
                    api_family="chat_completions",
                    endpoint="https://api.openai.com/v1/chat/completions",
                    model="gpt-4.1-2025-04-14",
                )
                set_profile_version(profile, version)
                errors, _ = self.validate(profile)
                self.assertEqual(errors, [])
                self.assertEqual(baseline["configuration"]["model"]["requested"], "gpt-4.1-2025-04-14")

            with self.subTest(schema_version=version, case="image model rejected for baseline"):
                profile = load_fixture()
                baseline = make_live_observation(
                    profile,
                    "generative_baseline",
                    api_family="responses",
                    endpoint="https://api.openai.com/v1/responses",
                )
                set_approved_model(
                    baseline,
                    "gpt-image-2",
                    resolved="gpt-image-2-2026-04-21",
                    identity_kind="alias",
                    expected_resolved="gpt-image-2-2026-04-21",
                )
                baseline["live"].update(
                    requested_model="gpt-image-2",
                    resolved_model="gpt-image-2-2026-04-21",
                )
                set_profile_version(profile, version)
                issue = self.first_issue(profile)
                self.assertEqual(issue["code"], "MODEL_UNSUPPORTED_FOR_CAPABILITY")

    def test_observed_model_identity_is_required_only_on_documented_response_surfaces(self):
        for version in ("1.0", "1.1"):
            with self.subTest(schema_version=version, case="jev pin"):
                profile = load_fixture()
                jev = make_live_observation(
                    profile,
                    "jev",
                    api_family="systemone_v1",
                    endpoint="https://api.typesafe.ai/v1/systemone",
                    model="jev-1.13.0",
                )
                set_profile_version(profile, version)
                errors, _ = self.validate(profile)
                self.assertEqual(errors, [])
                jev["live"]["resolved_model"] = None
                issue = self.first_issue(profile)
                self.assertEqual(issue["code"], "LIVE_RESPONSE_MODEL_MISSING")

            with self.subTest(schema_version=version, case="jev alias"):
                profile = load_fixture()
                jev = make_live_observation(
                    profile,
                    "jev",
                    api_family="systemone_v1",
                    endpoint="https://api.typesafe.ai/v1/systemone",
                )
                set_approved_model(
                    jev,
                    "jev-latest",
                    resolved="jev-1.13.0",
                    identity_kind="alias",
                    expected_resolved="jev-1.13.0",
                )
                jev["live"].update(requested_model="jev-latest", resolved_model="jev-1.13.0")
                set_profile_version(profile, version)
                errors, _ = self.validate(profile)
                self.assertEqual(errors, [])
                jev["live"]["resolved_model"] = None
                issue = self.first_issue(profile)
                self.assertEqual(issue["code"], "LIVE_RESPONSE_MODEL_MISSING")

            with self.subTest(schema_version=version, case="jev mismatched response"):
                profile = load_fixture()
                jev = make_live_observation(
                    profile,
                    "jev",
                    api_family="systemone_v1",
                    endpoint="https://api.typesafe.ai/v1/systemone",
                    model="jev-1.13.0",
                )
                set_profile_version(profile, version)
                jev["live"]["resolved_model"] = "jev-1.12.0"
                issue = self.first_issue(profile)
                self.assertEqual(issue["code"], "MODEL_RESOLUTION_MISMATCH")

            with self.subTest(schema_version=version, case="openai responses missing model"):
                profile = load_fixture()
                planner = make_live_observation(
                    profile,
                    "planner_reasoning",
                    api_family="responses",
                    endpoint="https://api.openai.com/v1/responses",
                    model="gpt-4.1-2025-04-14",
                )
                set_profile_version(profile, version)
                planner["live"]["resolved_model"] = None
                issue = self.first_issue(profile)
                self.assertEqual(issue["code"], "LIVE_RESPONSE_MODEL_MISSING")

            with self.subTest(schema_version=version, case="images API has no response model field"):
                profile = load_fixture()
                image = make_live_observation(
                    profile,
                    "gpt_image",
                    api_family="images",
                    endpoint="https://api.openai.com/v1/images/generations",
                    model="gpt-image-2-2026-04-21",
                )
                set_profile_version(profile, version)
                image["live"]["resolved_model"] = None
                errors, _ = self.validate(profile)
                self.assertEqual(errors, [])

    def test_denied_jev_and_metadata_only_records_do_not_invent_response_identity(self):
        for version in ("1.0", "1.1"):
            with self.subTest(schema_version=version, record="denied"):
                profile = load_fixture()
                jev = make_live_observation(
                    profile,
                    "jev",
                    api_family="systemone_v1",
                    endpoint="https://api.typesafe.ai/v1/systemone",
                    model="jev-1.13.0",
                )
                set_profile_version(profile, version)
                jev["live"].update(status="denied", resolved_model=None)
                errors, _ = self.validate(profile)
                self.assertEqual(errors, [])

            with self.subTest(schema_version=version, record="metadata"):
                profile = load_fixture()
                jev = add_jev_admin_record(profile)
                add_jev_model_list_record(jev)
                set_profile_version(profile, version)
                errors, _ = self.validate(profile)
                self.assertEqual(errors, [])
                jev["metadata"]["resolved_model"] = "jev-1.13.0"
                issue = self.first_issue(profile)
                self.assertEqual(issue["code"], "METADATA_RESOLUTION_UNSUPPORTED")

    def test_observed_response_identity_requires_a_supported_api_binding_in_both_profile_versions(self):
        surfaces = (
            (
                "jev",
                "systemone_v1",
                "https://api.typesafe.ai/v1/systemone",
                "jev-1.13.0",
                "https://api.typesafe.ai/v1/models",
                "LIVE_RECORD_ENDPOINT_INVALID",
            ),
            (
                "planner_reasoning",
                "responses",
                "https://api.openai.com/v1/responses",
                "gpt-4.1-2025-04-14",
                "https://api.openai.com/v1/chat/completions",
                "LIVE_ENDPOINT_MISMATCH",
            ),
            (
                "generative_baseline",
                "chat_completions",
                "https://api.openai.com/v1/chat/completions",
                "gpt-4.1-2025-04-14",
                "https://api.openai.com/v1/responses",
                "LIVE_ENDPOINT_MISMATCH",
            ),
        )

        def observed_profile(capability, api_family, endpoint, model, version):
            profile = load_fixture()
            cell = make_live_observation(
                profile,
                capability,
                api_family=api_family,
                endpoint=endpoint,
                model=model,
            )
            set_profile_version(profile, version)
            return profile, cell

        def leave_client_unselected(config, version):
            config["sdk"] = None
            if version == "1.1":
                config["client_selection"] = {
                    "mode": "unselected",
                    "api_version": None,
                    "policy_ref": None,
                }
            else:
                config.pop("client_selection", None)

        for version in ("1.0", "1.1"):
            for capability, api_family, endpoint, model, mismatched_endpoint, mismatch_code in surfaces:
                with self.subTest(schema_version=version, capability=capability, case="matching response"):
                    profile, cell = observed_profile(capability, api_family, endpoint, model, version)
                    errors, _ = self.validate(profile)
                    self.assertEqual(errors, [])
                    cell["configuration"]["model"]["resolved"] = None
                    cell["live"]["resolved_model"] = None
                    issue = self.first_issue(profile)
                    self.assertEqual(issue["code"], "LIVE_RESPONSE_MODEL_MISSING")

                with self.subTest(schema_version=version, capability=capability, case="unselected client"):
                    profile, cell = observed_profile(capability, api_family, endpoint, model, version)
                    cell["configuration"]["model"]["resolved"] = None
                    cell["live"]["resolved_model"] = None
                    leave_client_unselected(cell["configuration"], version)
                    issue = self.first_issue(profile)
                    self.assertEqual(issue["code"], "LIVE_RESPONSE_MODEL_MISSING")

                with self.subTest(schema_version=version, capability=capability, case="missing API family"):
                    profile, cell = observed_profile(capability, api_family, endpoint, model, version)
                    cell["configuration"]["api_family"] = None
                    cell["configuration"]["model"]["resolved"] = None
                    cell["live"]["resolved_model"] = None
                    leave_client_unselected(cell["configuration"], version)
                    issue = self.first_issue(profile)
                    self.assertEqual(issue["code"], "LIVE_API_BINDING_REQUIRED")

                with self.subTest(schema_version=version, capability=capability, case="mismatched endpoint"):
                    profile, cell = observed_profile(capability, api_family, endpoint, model, version)
                    cell["live"]["endpoint"] = mismatched_endpoint
                    issue = self.first_issue(profile)
                    self.assertEqual(issue["code"], mismatch_code)

    def test_model_policy_requires_a_current_revalidation_date(self):
        profile = load_fixture()
        cell = add_jev_admin_record(profile)
        cell["configuration"]["model"]["revalidate_by"] = "2026-10-09"
        errors, validated = self.validate(profile)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
        jev = next(item for item in report["capabilities"] if item["id"] == "jev")
        self.assertIn("MODEL_POLICY_STALE", {item["code"] for item in jev["blockers"]})

    def test_per_provider_environment_billing_and_quota_are_explicit(self):
        profile = load_fixture()
        report = readiness.assess_profile(profile, mode="offline", as_of=AS_OF, environ={})
        self.assertEqual({item["environment"] for item in report["capabilities"]}, {"development"})
        self.assertEqual({item["billing_mode"] for item in report["capabilities"]}, {"unknown"})

        profile = load_fixture()
        cell_for(profile, "jev")["configuration"]["environment"] = "production"
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "ENVIRONMENT_IDENTITY_MISMATCH")

        profile = load_fixture()
        config = cell_for(profile, "jev")["configuration"]
        config["quota_status"] = "recorded"
        config["quota_summary"] = "documented"
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "QUOTA_EVIDENCE_MISSING")

    def test_planner_and_baseline_keep_separate_future_budget_and_live_owner_records(self):
        profile = load_fixture()
        planner = cell_for(profile, "planner_reasoning")["future_usage_authorization"]
        baseline = cell_for(profile, "generative_baseline")["future_usage_authorization"]
        planner.update(
            {
                "status": "approved",
                "budget_ref": "evidence:finance/planner-budget",
                "reservation_protocol_ref": "evidence:finance/planner-reservation",
            }
        )
        baseline.update(
            {
                "status": "approved",
                "budget_ref": "evidence:finance/baseline-budget",
                "reservation_protocol_ref": "evidence:finance/baseline-reservation",
            }
        )
        errors, validated = self.validate(profile)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
        by_id = {item["id"]: item for item in report["capabilities"]}
        self.assertTrue(by_id["planner_reasoning"]["future_usage_authorization"]["budget_reference_present"])
        self.assertTrue(by_id["generative_baseline"]["future_usage_authorization"]["reservation_protocol_reference_present"])
        self.assertEqual(by_id["planner_reasoning"]["status"], "pending")
        self.assertEqual(by_id["generative_baseline"]["status"], "pending")

        baseline["reservation_protocol_ref"] = None
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "FUTURE_USAGE_EVIDENCE_INCOMPLETE")

    def test_sponsor_setup_and_actual_artifact_gates_remain_pending(self):
        profile = load_fixture()
        errors, validated = self.validate(profile)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
        by_id = {item["id"]: item for item in report["capabilities"]}
        neatlogs = by_id["neatlogs"]
        entire = by_id["entire"]
        self.assertEqual(neatlogs["sponsor_integration"]["status"], "pending")
        self.assertEqual(entire["sponsor_integration"]["applicability"], "required")
        self.assertEqual(entire["client_selection_mode"], "not_applicable")
        self.assertEqual(entire["live_capability"], "pending")
        self.assertIn("SPONSOR_CONFIGURATION_PENDING", {item["code"] for item in neatlogs["blockers"]})
        self.assertIn("ENTIRE_ARTIFACT_PENDING", {item["code"] for item in entire["blockers"]})

        profile = load_fixture()
        neatlogs = cell_for(profile, "neatlogs")["sponsor_integration"]
        neatlogs.update(
            {
                "status": "configured",
                "tool_version": "1.0.0",
                "artifact_plan_ref": "evidence:ops/neatlogs-plan",
            }
        )
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "SPONSOR_CONFIGURATION_INCOMPLETE")

        profile = load_fixture()
        cell_for(profile, "entire")["configuration"]["billing_mode"] = "not_applicable"
        errors, validated = self.validate(profile)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
        entire = next(item for item in report["capabilities"] if item["id"] == "entire")
        self.assertEqual(entire["billing_mode"], "not_applicable")
        self.assertEqual(entire["configuration_status"], "pending")
        self.assertEqual(entire["live_capability"], "pending")
        self.assertIn("ENTIRE_ARTIFACT_PENDING", {item["code"] for item in entire["blockers"]})

    def test_mock_or_sandbox_mode_never_counts_as_observed_live_access(self):
        profile = load_fixture()
        cell_for(profile, "jev")["live"]["dependency_mode"] = "mock"
        errors, validated = self.validate(profile)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
        jev = next(item for item in report["capabilities"] if item["id"] == "jev")
        self.assertEqual(jev["live_evidence"]["dependency_mode"], "mock")
        self.assertEqual(jev["live_capability"], "pending")

        profile = load_fixture()
        cell = add_jev_admin_record(profile)
        cell["configuration"]["billing_mode"] = "sandbox"
        cell["live"]["dependency_mode"] = "sandbox"
        errors, validated = self.validate(profile)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
        jev = next(item for item in report["capabilities"] if item["id"] == "jev")
        self.assertEqual(jev["live_capability"], "pending")
        self.assertIn("LIVE_CAPABILITY_PENDING", {item["code"] for item in jev["blockers"]})

        cell["live"]["status"] = "observed"
        cell["live"].update(
            {
                "dependency_mode": "live",
                "checked_at": "2026-10-01",
                "valid_until": "2026-10-20",
                "endpoint": "https://api.typesafe.ai/v1/systemone",
                "account_ref": cell["configuration"]["account_ref"],
                "project_ref": cell["configuration"]["project_ref"],
                "requested_model": "jev-1.13.0",
                "resolved_model": "jev-1.13.0",
                "evidence_ref": "evidence:synthetic/mock-run",
            }
        )
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "LIVE_BILLING_MODE_NON_METERED")

    def test_live_outcome_requires_matching_capability_authorization_and_live_mode(self):
        profile = load_fixture()
        cell = make_live_observation(
            profile,
            "jev",
            api_family="systemone_v1",
            endpoint="https://api.typesafe.ai/v1/systemone",
            model="jev-1.13.0",
        )
        cell["configuration"]["billing_mode"] = "unknown"
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "LIVE_BILLING_MODE_UNCONFIRMED")

        cell["configuration"]["billing_mode"] = "sandbox"
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "LIVE_BILLING_MODE_NON_METERED")

        cell["configuration"]["billing_mode"] = "api_metered"
        cell["live"]["dependency_mode"] = "mock"
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "LIVE_DEPENDENCY_MODE_MISMATCH")

        cell["live"]["dependency_mode"] = "live"
        errors, validated = self.validate(profile)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
        jev = next(item for item in report["capabilities"] if item["id"] == "jev")
        self.assertEqual(jev["live_capability"], "observed")

        profile["live_test_authorization"]["authorized_capabilities"] = ["gpt_image"]
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "LIVE_AUTHORIZATION_MISSING")

    def test_each_nonlive_billing_classification_cannot_support_observed_metered_inference(self):
        for billing_mode in ("sandbox", "simulated"):
            with self.subTest(billing_mode=billing_mode):
                pending_profile = load_fixture()
                pending_cell = cell_for(pending_profile, "jev")
                pending_cell["configuration"]["billing_mode"] = billing_mode
                pending_cell["live"]["dependency_mode"] = "sandbox" if billing_mode == "sandbox" else "mock"
                errors, validated = self.validate(pending_profile)
                self.assertEqual(errors, [])
                pending_report = readiness.assess_profile(
                    validated, mode="offline", as_of=AS_OF, environ={}
                )
                pending_jev = next(item for item in pending_report["capabilities"] if item["id"] == "jev")
                self.assertEqual(pending_jev["live_capability"], "pending")

                profile = load_fixture()
                cell = make_live_observation(
                    profile,
                    "jev",
                    api_family="systemone_v1",
                    endpoint="https://api.typesafe.ai/v1/systemone",
                    model="jev-1.13.0",
                )
                cell["configuration"]["billing_mode"] = billing_mode
                issue = self.first_issue(profile)
                self.assertEqual(issue["code"], "LIVE_BILLING_MODE_NON_METERED")

        profile = load_fixture()
        cell = make_live_observation(
            profile,
            "jev",
            api_family="systemone_v1",
            endpoint="https://api.typesafe.ai/v1/systemone",
            model="jev-1.13.0",
        )
        cell["configuration"]["billing_mode"] = "not_applicable"
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "BILLING_MODE_NOT_APPLICABLE_UNSUPPORTED")

    def test_metadata_only_evidence_does_not_verify_inference_or_image_generation(self):
        profile = load_fixture()
        jev = add_jev_admin_record(profile)
        add_jev_model_list_record(jev)
        errors, validated = self.validate(profile)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ=MembershipOnlyEnvironment())
        by_id = {item["id"]: item for item in report["capabilities"]}

        self.assertEqual(by_id["jev"]["metadata"]["status"], "observed")
        self.assertEqual(by_id["jev"]["metadata"]["endpoint"], "https://api.typesafe.ai/v1/models")
        self.assertTrue(by_id["jev"]["metadata"]["evidence_reference_present"])
        self.assertIsNone(by_id["jev"]["metadata"]["resolved_model"])
        self.assertEqual(by_id["jev"]["live_capability"], "pending")
        self.assertEqual(by_id["gpt_image"]["live_capability"], "pending")
        self.assertEqual(by_id["neatlogs"]["live_capability"], "pending")
        self.assertEqual(by_id["planner_reasoning"]["status"], "pending")
        self.assertEqual(by_id["generative_baseline"]["status"], "pending")
        self.assertEqual(by_id["jev"]["configuration_status"], "pending")
        self.assertTrue(by_id["jev"]["credential_reference_present"])

    def test_auth_metadata_cannot_smuggle_unvalidated_model_or_secret_text(self):
        marker = "INLINE_SECRET_IN_MODEL_FIELD"
        profile = load_fixture()
        cell = add_jev_admin_record(profile)
        config = cell["configuration"]
        cell["metadata"].update(
            {
                "status": "observed",
                "kind": "auth",
                "checked_at": "2026-10-01",
                "valid_until": "2026-10-20",
                "endpoint": "https://api.typesafe.ai/v1/models",
                "account_ref": config["account_ref"],
                "project_ref": config["project_ref"],
                "requested_model": marker,
                "evidence_ref": "evidence:admin/jev-auth",
            }
        )
        issue = self.first_issue(profile)
        self.assertEqual(issue["code"], "METADATA_RECORD_INCONSISTENT")
        self.assertNotIn(marker, issue["message"])

    def test_stale_and_mismatched_identity_records_block_or_reject(self):
        stale_profile = load_fixture()
        add_jev_admin_record(stale_profile, valid_until="2026-10-09")
        errors, validated = self.validate(stale_profile)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
        self.assertIn("IDENTITY_RECORD_STALE", {item["code"] for item in report["diagnostics"]})

        mismatch_profile = load_fixture()
        jev = add_jev_admin_record(mismatch_profile)
        jev["eligibility"]["account_ref"] = "urn:typesafe:account:other"
        issue = self.first_issue(mismatch_profile)
        self.assertEqual(issue["code"], "IDENTITY_RECORD_MISMATCH")

    def test_denial_and_sponsor_gap_remain_separate_from_available_jev_metadata(self):
        profile = load_fixture()
        jev = add_jev_admin_record(profile)
        add_jev_model_list_record(jev)
        gpt = cell_for(profile, "gpt_image")
        neatlogs = cell_for(profile, "neatlogs")
        for cell, account, project, evidence in (
            (gpt, "urn:openai:account:dev", "urn:openai:project:acv1", "evidence:admin/image-denied"),
            (neatlogs, "urn:neatlogs:account:dev", "urn:neatlogs:project:acv1", "evidence:admin/neatlogs-pending"),
        ):
            cell["configuration"]["account_ref"] = account
            cell["configuration"]["project_ref"] = project
            cell["eligibility"].update(
                {
                    "status": "denied" if cell["id"] == "gpt_image" else "pending",
                    "checked_at": "2026-10-01" if cell["id"] == "gpt_image" else None,
                    "valid_until": "2026-10-20" if cell["id"] == "gpt_image" else None,
                    "account_ref": account if cell["id"] == "gpt_image" else None,
                    "project_ref": project if cell["id"] == "gpt_image" else None,
                    "evidence_ref": evidence if cell["id"] == "gpt_image" else None,
                }
            )

        errors, validated = self.validate(profile)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
        by_id = {item["id"]: item for item in report["capabilities"]}
        self.assertEqual(by_id["jev"]["metadata"]["status"], "observed")
        self.assertEqual(by_id["gpt_image"]["status"], "denied")
        self.assertIn("ELIGIBILITY_DENIED", {item["code"] for item in by_id["gpt_image"]["blockers"]})
        self.assertEqual(by_id["neatlogs"]["eligibility"]["status"], "pending")
        self.assertEqual(by_id["neatlogs"]["blocked_consumers"], ["SPN-01"])
        self.assertEqual(by_id["entire"]["blocked_consumers"], ["SPN-02"])

    def test_denied_jev_entitlement_does_not_change_other_provider_cells(self):
        profile = load_fixture()
        jev = cell_for(profile, "jev")
        config = jev["configuration"]
        config["account_ref"] = "urn:typesafe:account:development"
        config["project_ref"] = "urn:agent-harness:project:acv1"
        jev["eligibility"].update(
            {
                "status": "denied",
                "checked_at": "2026-10-01",
                "valid_until": "2026-10-20",
                "account_ref": config["account_ref"],
                "project_ref": config["project_ref"],
                "evidence_ref": "evidence:admin/jev-denial",
            }
        )
        errors, validated = self.validate(profile)
        self.assertEqual(errors, [])
        report = readiness.assess_profile(validated, mode="offline", as_of=AS_OF, environ={})
        by_id = {item["id"]: item for item in report["capabilities"]}
        self.assertEqual(by_id["jev"]["status"], "denied")
        self.assertIn("ACV1-JEV-EVAL", by_id["jev"]["blocked_consumers"])
        self.assertEqual(by_id["gpt_image"]["eligibility"]["status"], "pending")
        self.assertEqual(by_id["planner_reasoning"]["provider"], None)
        self.assertEqual(by_id["neatlogs"]["eligibility"]["status"], "pending")

    def test_live_mode_refuses_without_authorization_and_never_calls_provider(self):
        profile = load_fixture()
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_path = Path(temp_dir) / "profile.json"
            profile_path.write_text(json.dumps(profile), encoding="utf-8")
            output = io.StringIO()
            with contextlib.redirect_stdout(output), mock.patch("socket.create_connection", side_effect=AssertionError("network forbidden")):
                exit_code = readiness.main(["--profile", str(profile_path), "--mode", "live", "--as-of", "2026-10-10"])
        report = json.loads(output.getvalue())
        self.assertEqual(exit_code, 3)
        self.assertEqual(report["mode_result"], "refused_without_authorization")
        self.assertIn("LIVE_AUTHORIZATION_MISSING", {item["code"] for item in report["diagnostics"]})
        self.assertFalse(report["network_accessed"])

    def test_metadata_mode_is_explicitly_unsupported_without_network(self):
        profile = load_fixture()
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_path = Path(temp_dir) / "profile.json"
            profile_path.write_text(json.dumps(profile), encoding="utf-8")
            output = io.StringIO()
            with contextlib.redirect_stdout(output), mock.patch("socket.create_connection", side_effect=AssertionError("network forbidden")):
                exit_code = readiness.main(["--profile", str(profile_path), "--mode", "metadata", "--as-of", "2026-10-10"])
        report = json.loads(output.getvalue())
        self.assertEqual(exit_code, 3)
        self.assertEqual(report["mode_result"], "unsupported_no_network")
        self.assertIn("METADATA_MODE_UNSUPPORTED", {item["code"] for item in report["diagnostics"]})
        self.assertFalse(report["network_accessed"])

    def test_cli_reports_only_credential_reference_and_never_secret_value(self):
        sentinel = "NEVER_PRINT_THIS_SENTINEL_CREDENTIAL"
        output = io.StringIO()
        with mock.patch.dict(os.environ, {"TYPESAFE_API_KEY": sentinel}):
            with contextlib.redirect_stdout(output):
                exit_code = readiness.main(
                    ["--profile", str(FIXTURE_PATH), "--mode", "offline", "--as-of", "2026-10-10"]
                )
        rendered = output.getvalue()
        report = json.loads(rendered)
        self.assertEqual(exit_code, 2)
        self.assertNotIn(sentinel, rendered)
        self.assertIn("TYPESAFE_API_KEY", rendered)
        jev = next(item for item in report["capabilities"] if item["id"] == "jev")
        self.assertTrue(jev["credential_reference_present"])
        self.assertEqual(jev["eligibility"]["status"], "pending")

    def test_empty_environment_secret_is_not_read_and_does_not_prove_authentication(self):
        output = io.StringIO()
        with mock.patch.dict(os.environ, {"TYPESAFE_API_KEY": ""}):
            with contextlib.redirect_stdout(output):
                exit_code = readiness.main(
                    ["--profile", str(FIXTURE_PATH), "--mode", "offline", "--as-of", "2026-10-10"]
                )
        report = json.loads(output.getvalue())
        jev = next(item for item in report["capabilities"] if item["id"] == "jev")
        self.assertEqual(exit_code, 2)
        self.assertTrue(jev["credential_reference_present"])
        self.assertEqual(jev["eligibility"]["status"], "pending")
        self.assertEqual(jev["live_capability"], "pending")

    def test_real_cli_reports_safe_depth_errors_for_arrays_and_objects_in_all_modes(self):
        marker = b"SYNTHETIC_DEEP_JSON_MARKER"
        self.assertGreater(1500, readiness.MAX_PROFILE_JSON_NESTING)
        deep_inputs = {
            "array": b'{"nested":' + b"[" * 1500 + b'"' + marker + b'"' + b"]" * 1500 + b"}",
            "object": b'{"nested":' + b'{"nested":' * 1500 + b'"' + marker + b'"' + b"}" * 1500 + b"}",
        }
        for shape, raw in deep_inputs.items():
            with self.subTest(shape=shape):
                self.assertLess(len(raw), 1_048_576)
                for mode in ("offline", "metadata", "live"):
                    with self.subTest(mode=mode):
                        result = run_cli(raw, mode)
                        self.assertEqual(result.returncode, 1)
                        self.assertEqual(result.stderr, "")
                        self.assertNotIn("Traceback", result.stdout)
                        self.assertNotIn(marker.decode("ascii"), result.stdout)
                        report = json.loads(result.stdout)
                        self.assertEqual(report["status"], "invalid")
                        self.assertEqual(report["mode_result"], "profile_rejected")
                        self.assertFalse(report["network_accessed"])
                        self.assertEqual(report["diagnostics"][0]["code"], "PROFILE_JSON_DEPTH_LIMIT")

    def test_real_cli_rejects_positive_and_negative_5000_digit_integer_tokens_in_all_modes(self):
        if hasattr(sys, "get_int_max_str_digits"):
            self.assertGreater(sys.get_int_max_str_digits(), 0)
        digits = b"7" * 5000
        cases = {"positive": digits, "negative": b"-" + digits}
        marker = b"SYNTHETIC_INTEGER_MARKER"
        for sign, integer_token in cases.items():
            raw = b'{"nested":' + integer_token + b',"marker":"' + marker + b'"}'
            with self.subTest(sign=sign):
                self.assertLess(len(raw), readiness.MAX_PROFILE_JSON_BYTES)
                for mode in ("offline", "metadata", "live"):
                    with self.subTest(mode=mode):
                        result = run_cli(raw, mode)
                        self.assertEqual(result.returncode, 1)
                        self.assertEqual(result.stderr, "")
                        self.assertNotIn("Traceback", result.stdout)
                        self.assertNotIn(integer_token.decode("ascii"), result.stdout)
                        self.assertNotIn(marker.decode("ascii"), result.stdout)
                        report = json.loads(result.stdout)
                        self.assertEqual(report["status"], "invalid")
                        self.assertEqual(report["mode_result"], "profile_rejected")
                        self.assertFalse(report["network_accessed"])
                        self.assertEqual(
                            report["diagnostics"][0]["code"],
                            "PROFILE_JSON_INTEGER_LIMIT",
                        )

    def test_real_cli_pending_fixture_and_unsupported_modes_remain_non_networked(self):
        raw = json.dumps(load_fixture()).encode("utf-8")
        controls = (
            ("offline", 2, "pending", "offline_profile_check"),
            ("metadata", 3, "unsupported", "unsupported_no_network"),
            ("live", 3, "unsupported", "refused_without_authorization"),
        )
        for mode, expected_exit, expected_status, expected_result in controls:
            with self.subTest(mode=mode):
                result = run_cli(raw, mode)
                self.assertEqual(result.returncode, expected_exit)
                self.assertEqual(result.stderr, "")
                report = json.loads(result.stdout)
                self.assertEqual(report["status"], expected_status)
                self.assertEqual(report["mode_result"], expected_result)
                self.assertFalse(report["network_accessed"])
                if mode == "offline":
                    self.assertEqual(report["development_execution"]["engine_api_integration"], "pending")
                    self.assertEqual(report["development_execution"]["provider_metering"], "not_established")

    def test_profile_parser_rejects_duplicate_keys_without_echoing_input(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_path = Path(temp_dir) / "duplicate.json"
            profile_path.write_text('{"profile_id":"safe","profile_id":"INLINE_SECRET"}', encoding="utf-8")
            loaded, error = readiness._load_profile(profile_path)
        self.assertIsNone(loaded)
        self.assertEqual(error, "PROFILE_JSON_DUPLICATE_KEY")

    def test_unknown_fields_cannot_smuggle_inline_secrets_into_output(self):
        marker = "INLINE_SECRET_DO_NOT_ECHO"
        profile = load_fixture()
        profile["api_key"] = marker
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_path = Path(temp_dir) / "profile.json"
            profile_path.write_text(json.dumps(profile), encoding="utf-8")
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                exit_code = readiness.main(["--profile", str(profile_path), "--mode", "offline"])
        self.assertEqual(exit_code, 1)
        self.assertNotIn(marker, output.getvalue())
        self.assertIn("CONFIG_FIELD_UNSUPPORTED", output.getvalue())

    def test_schema_is_local_strict_and_fixture_shape_matches_profile_contract(self):
        schema = json.loads((REPO_ROOT / "config" / "provider-profiles.schema.json").read_text(encoding="utf-8"))
        self.assertEqual(schema["$schema"], "https://json-schema.org/draft/2020-12/schema")
        self.assertIn("not a shared domain contract", schema["description"])
        self.assertFalse(schema["additionalProperties"])
        profile = load_fixture()
        self.assertEqual(schema["properties"]["schema_version"]["enum"], ["1.0", "1.1"])
        self.assertEqual(profile["schema_version"], "1.1")
        capability_schema = schema["$defs"]["capability"]
        configuration_schema = schema["$defs"]["configuration"]
        model_schema = schema["$defs"]["model"]
        client_selection_schema = schema["$defs"]["clientSelection"]
        self.assertEqual(configuration_schema["properties"]["client_selection"]["$ref"], "#/$defs/clientSelection")
        self.assertEqual(set(client_selection_schema["required"]), {"mode", "api_version", "policy_ref"})
        self.assertEqual(
            client_selection_schema["properties"]["mode"]["enum"],
            ["unselected", "sdk", "direct_http"],
        )
        self.assertTrue(any(item.get("if", {}).get("properties", {}).get("schema_version", {}).get("const") == "1.1" for item in schema["allOf"]))
        self.assertTrue(any(item.get("if", {}).get("properties", {}).get("schema_version", {}).get("const") == "1.0" for item in schema["allOf"]))
        schema_families = set(configuration_schema["properties"]["api_family"]["enum"]) - {None}
        implementation_families = {key[2] for key in readiness.API_SURFACES}
        self.assertEqual(schema_families, implementation_families)
        sdk_schema = configuration_schema["properties"]["sdk"]["oneOf"][1]["properties"]
        self.assertIn("pattern", sdk_schema["version"])
        self.assertIn("pattern", sdk_schema["source_revision"])
        self.assertEqual(len(configuration_schema["allOf"]), 3)
        self.assertIn("bounded by", model_schema["description"])
        self.assertIn("documented pinned target", model_schema["properties"]["expected_resolved"]["description"])
        self.assertIn("API surface", model_schema["properties"]["requested"]["description"])
        for cell in profile["capabilities"]:
            self.assertTrue(set(capability_schema["required"]).issubset(cell))
            self.assertTrue(set(configuration_schema["required"]).issubset(cell["configuration"]))
            self.assertTrue(set(model_schema["required"]).issubset(cell["configuration"]["model"]))
            self.assertIn("client_selection", cell["configuration"])
        self.assertIn("futureUsageAuthorization", schema["$defs"])
        self.assertIn("sponsorIntegration", schema["$defs"])
        errors, validated = self.validate(profile)
        self.assertEqual(errors, [])
        self.assertEqual(len(validated["capabilities"]), 6)

    def test_runbook_describes_the_versioned_schema_api_and_client_selection_policy(self):
        runbook = (REPO_ROOT / "docs" / "runbooks" / "provider-access.md").read_text(encoding="utf-8")
        self.assertIn("Schema 1.0 profiles remain readable", runbook)
        self.assertIn("Schema 1.1 requires `client_selection`", runbook)
        self.assertIn("Readiness report version 1.1", runbook)
        self.assertIn("`direct_http`", runbook)
        self.assertIn("exact semantic version", runbook)
        self.assertIn("`api_metered`", runbook)
        self.assertIn("Bounded model identity policy", runbook)
        self.assertIn("gpt-image-2-2026-04-21", runbook)
        self.assertIn("gpt-4.1-2025-04-14", runbook)
        self.assertIn("PROFILE_JSON_DEPTH_LIMIT", runbook)
        self.assertIn("PROFILE_JSON_INTEGER_LIMIT", runbook)
        self.assertIn("`jev-1.13.0`", runbook)
        self.assertIn("unselected client stays pending", runbook)
        self.assertIn("Codex subscription access does not establish API eligibility", runbook)
        self.assertIn("successful observed inference", runbook)
        for family in {key[2] for key in readiness.API_SURFACES}:
            self.assertIn(f"`{family}`", runbook)


if __name__ == "__main__":
    unittest.main()
