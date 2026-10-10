"""Direct tests for CI's expected-pending classifier behavior."""

import unittest
import tempfile
from pathlib import Path
from unittest import mock

import classify_ci_gates as ci
from tools import verify_contract_release as contracts


class CICheckerClassificationTests(unittest.TestCase):
    def test_sandbox_artifact_preserves_only_bounded_sanitized_failure_detail(self):
        reason = contracts._sandbox_process_failure({
            "ok": False,
            "code": "adapter_nonzero_exit",
            "returncode": 1,
            "stderr": b"bwrap: Creating new namespace failed: Operation not permitted at /home/runner/host.txt",
        })
        with mock.patch.object(ci.platform, "system", return_value="Linux"), \
                mock.patch.object(ci.shutil, "which", return_value="/usr/bin/bwrap"), \
                mock.patch.object(ci.os, "access", return_value=True), \
                mock.patch.object(ci.contracts, "run_bounded", return_value={
                    "ok": True, "stdout": b"bubblewrap 0.9.0", "stderr": b"",
                }), \
                mock.patch.object(ci.contracts, "probe_sandbox", return_value=reason):
            evidence, problems = ci.classify_sandbox()

        self.assertIn("bubblewrap_canary_failed", problems)
        self.assertEqual(evidence["canary"]["status"], "failed")
        detail = evidence["canary"]["details"]
        self.assertIn("namespace_operation_not_permitted", detail)
        self.assertIn('"exit_code":1', detail)
        self.assertNotIn("/home/runner", detail)
        self.assertLessEqual(len(detail), 640)

    def test_sandbox_success_requires_complete_profile_observations_and_sanitizes_audit(self):
        observations = {key: True for key in ci.SANDBOX_OBSERVATION_KEYS}
        observations.update({"apparmor_label": "bwrap//unpriv_bwrap (enforce)",
                             "python_reexec_label": "bwrap//unpriv_bwrap (enforce)",
                             "other_exec_label": "bwrap//unpriv_bwrap (enforce)",
                             "nested_userns_label": "bwrap//unpriv_bwrap (enforce)",
                             "nested_cap_eff": "000001ffffffffff", "nested_userns_process_exit_code": 0,
                             "nested_userns_stderr_class": "empty",
                             "nested_userns_setup_stage": "mount_attempted",
                             "nested_userns_creation_observation": "confirmed",
                             "nested_userns_mapping_observation": "completed",
                             "nested_userns_utility_version": "2.39.3",
                             "nested_userns_utility_version_exit_code": 0,
                             "nested_userns_stderr_bytes": 0,
                             "nested_userns_process_bounded": True,
                             "nested_mount_errno_category": "operation_not_permitted",
                             "nested_bwrap_setup_stage": "loopback_setup_denied",
                             "nested_bwrap_utility_version": "0.9.0",
                             "nested_bwrap_utility_version_exit_code": 0,
                             "nested_bwrap_exit_code": 1,
                             "nested_bwrap_stderr_bytes": 55,
                             "nested_bwrap_stderr_class": "network_namespace_loopback_setup_denied",
                             "host_canary_observation": "unchanged",
                             "nested_capability_sets": {name: "000001ffffffffff" for name in
                                                        ("CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb")},
                             "nested_mount_namespace_created": False,
                             "host_canary_unchanged": True})

        def successful_probe(_root, output):
            output.update(observations)
            return None

        def bounded(command, **_kwargs):
            if "--version" in command:
                return {"ok": True, "stdout": b"bubblewrap 0.9.0", "stderr": b""}
            return {
                "ok": True,
                "stdout": (b'audit: apparmor="DENIED" operation="capable" '
                           b'profile="bwrap//unpriv_bwrap" capname="sys_admin" '
                           b'name="/synthetic/private/path"\n'),
                "stderr": b"",
            }

        with mock.patch.object(ci.platform, "system", return_value="Linux"), \
                mock.patch.object(ci.shutil, "which", return_value="/usr/bin/bwrap"), \
                mock.patch.object(ci.os, "access", return_value=True), \
                mock.patch.object(ci.contracts, "probe_sandbox", side_effect=successful_probe), \
                mock.patch.object(ci.contracts, "run_bounded", side_effect=bounded):
            evidence, problems = ci.classify_sandbox()

        self.assertFalse(problems)
        self.assertEqual(evidence["implementation_check"], "required_linux_isolation_canaries_confirmed")
        self.assertEqual(evidence["canary"]["status"], "synthetic_canaries_confirmed")
        audit = evidence["canary"]["kernel_audit"]
        self.assertEqual(audit["status"], "matching_bwrap_denial_records_observed")
        self.assertEqual(audit["records"], [{"operation": "capable", "profile": "bwrap//unpriv_bwrap",
                                             "capability": "sys_admin"}])
        self.assertNotIn("/synthetic/private/path", str(audit))
        self.assertEqual(audit["prior_loopback_failure_cause"], "unproven_from_frozen_CI_logs")
        self.assertEqual(evidence["canary"]["observations"]["nested_mount_namespace_created"], False)
        self.assertEqual(set(evidence["canary"]["observations"]["nested_capability_sets"]),
                         {"CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"})

    def test_sandbox_success_is_rejected_when_any_required_native_observation_is_missing(self):
        observations = {key: True for key in ci.SANDBOX_OBSERVATION_KEYS}
        observations.pop("nested_mount_namespace_denied")
        observations.update({"apparmor_label": "bwrap//unpriv_bwrap (enforce)", "host_canary_unchanged": True})

        def incomplete_probe(_root, output):
            output.update(observations)
            return None

        with mock.patch.object(ci.platform, "system", return_value="Linux"), \
                mock.patch.object(ci.shutil, "which", return_value="/usr/bin/bwrap"), \
                mock.patch.object(ci.os, "access", return_value=True), \
                mock.patch.object(ci.contracts, "run_bounded", return_value={
                    "ok": True, "stdout": b"bubblewrap 0.9.0", "stderr": b"",
                }), \
                mock.patch.object(ci.contracts, "probe_sandbox", side_effect=incomplete_probe):
            evidence, problems = ci.classify_sandbox()

        self.assertIn("bubblewrap_canary_evidence_incomplete", problems)
        self.assertEqual(evidence["canary"]["status"], "failed")

    def test_missing_host_canary_read_stays_unobserved_and_is_not_reported_as_changed(self):
        safe = ci._safe_sandbox_observations({
            "host_canary_observation": "unavailable",
            "nested_userns_setup_stage": "setup_before_payload",
            "nested_userns_utility_version": "2.39.3",
            "nested_userns_process_exit_code": 1,
            "nested_userns_stderr_bytes": 29,
            "nested_userns_stderr_class": "operation_not_permitted",
            "nested_userns_payload_reached": False,
            "nested_userns_mapping_observation": "not_observed",
            "raw_stderr": "/private/path and sensitive bytes",
        })
        self.assertNotIn("host_canary_unchanged", safe)
        self.assertEqual(safe["host_canary_observation"], "unavailable")
        self.assertEqual(safe["nested_userns_setup_stage"], "setup_before_payload")
        self.assertEqual(safe["nested_userns_process_exit_code"], 1)
        self.assertNotIn("raw_stderr", safe)

    def test_structured_observations_accept_combined_namespace_stage_and_reject_unknown_stage(self):
        safe = ci._safe_sandbox_observations({
            "nested_bwrap_setup_stage": "combined_namespace_creation_failed",
            "nested_bwrap_stderr_class": "other",
        })
        self.assertEqual(safe["nested_bwrap_setup_stage"], "combined_namespace_creation_failed")
        self.assertEqual(safe["nested_bwrap_stderr_class"], "other")
        unsafe = ci._safe_sandbox_observations({
            "nested_bwrap_setup_stage": "user-supplied-stage",
            "nested_bwrap_stderr_class": "user-supplied-class",
        })
        self.assertNotIn("nested_bwrap_setup_stage", unsafe)
        self.assertNotIn("nested_bwrap_stderr_class", unsafe)

    def test_sandbox_probe_reads_synthetic_host_canary_after_canary_failure(self):
        with tempfile.TemporaryDirectory(prefix="acv1-host-canary-observation-") as temp:
            root = Path(temp)
            observed = {}
            with mock.patch.object(contracts.platform, "system", return_value="Linux"), \
                    mock.patch.object(contracts, "_sandbox_prerequisite_error", return_value=None), \
                    mock.patch.object(contracts, "_run_sandbox_canary", return_value="synthetic failure"):
                reason = contracts.probe_sandbox(root, observed)
        self.assertEqual(reason, "synthetic failure")
        self.assertTrue(observed["host_canary_unchanged"])
        self.assertEqual(observed["host_canary_observation"], "unchanged")

    def test_sandbox_probe_distinguishes_observed_host_change_from_unavailable_read(self):
        with tempfile.TemporaryDirectory(prefix="acv1-host-canary-change-") as temp:
            root = Path(temp)
            observed = {}

            def changed_canary(_package_root, outside, _observed):
                outside.write_bytes(b"changed synthetic bytes")
                return "synthetic failure"

            with mock.patch.object(contracts.platform, "system", return_value="Linux"), \
                    mock.patch.object(contracts, "_sandbox_prerequisite_error", return_value=None), \
                    mock.patch.object(contracts, "_run_sandbox_canary", side_effect=changed_canary):
                reason = contracts.probe_sandbox(root, observed)
        self.assertEqual(reason, "synthetic failure")
        self.assertFalse(observed["host_canary_unchanged"])
        self.assertEqual(observed["host_canary_observation"], "changed")

        with tempfile.TemporaryDirectory(prefix="acv1-host-canary-unavailable-") as temp:
            root = Path(temp)
            observed = {}

            def removes_canary(_package_root, outside, _observed):
                outside.unlink()
                return "synthetic failure"

            with mock.patch.object(contracts.platform, "system", return_value="Linux"), \
                    mock.patch.object(contracts, "_sandbox_prerequisite_error", return_value=None), \
                    mock.patch.object(contracts, "_run_sandbox_canary", side_effect=removes_canary):
                reason = contracts.probe_sandbox(root, observed)
        self.assertEqual(reason, "synthetic failure")
        self.assertNotIn("host_canary_unchanged", observed)
        self.assertEqual(observed["host_canary_observation"], "unavailable")

    def test_checked_in_release_is_classified_blocked_with_exit_two(self):
        observed = ci._run_checker("contracts")
        evidence, problems = ci.classify_readiness_report(
            "contracts", observed["exit_code"], observed["report"], observed["stderr_nonempty"])
        self.assertFalse(problems)
        self.assertEqual(evidence["implementation_check"], "expected_pending_diagnostics_confirmed")
        self.assertEqual(evidence["readiness"], {"status": "blocked", "ready": False})
        self.assertEqual(evidence["expected_cli_exit_code"], 2)
        self.assertEqual(evidence["observed_cli_exit_code"], 2)
        self.assertEqual(evidence["diagnostics"]["codes"], ci.EXPECTED_CONTRACT_CODES)

    def test_checked_in_jev_targets_are_classified_blocked_with_exit_two(self):
        observed = ci._run_checker("jev")
        evidence, problems = ci.classify_readiness_report(
            "jev", observed["exit_code"], observed["report"], observed["stderr_nonempty"])
        self.assertFalse(problems)
        self.assertEqual(evidence["implementation_check"], "expected_pending_diagnostics_confirmed")
        self.assertEqual(evidence["readiness"], {"status": "blocked", "ready": False})
        self.assertEqual(evidence["expected_cli_exit_code"], 2)
        self.assertEqual(evidence["observed_cli_exit_code"], 2)
        self.assertEqual(evidence["diagnostics"]["codes"], ci.EXPECTED_JEV_CODES)

    def test_unexpected_success_and_ready_report_cannot_be_classified_pending(self):
        report = {
            "tool": "verify_contract_release",
            "status": "offline_conformance_passed",
            "ready": True,
            "dependency_mode": "offline",
            "result": {"validated_fixture_cases": 27},
            "errors": [],
        }
        evidence, problems = ci.classify_readiness_report("contracts", 0, report)
        self.assertTrue(problems)
        self.assertIn("cli_exit_code_was_not_expected_pending_exit_2", problems)
        self.assertIn("readiness_is_not_typed_blocked", problems)
        self.assertEqual(evidence["implementation_check"], "unexpected_result")
        self.assertTrue(evidence["readiness"]["ready"])

    def test_different_failure_without_expected_owner_blockers_fails(self):
        report = {
            "tool": "verify_contract_release",
            "status": "blocked",
            "ready": False,
            "dependency_mode": "offline",
            "errors": [{"code": "input_invalid", "path": "$", "owner": "Agent and research",
                        "action": "invalid synthetic JSON"}],
        }
        evidence, problems = ci.classify_readiness_report("contracts", 2, report)
        self.assertTrue(problems)
        self.assertIn("owner_blocker_code_counts_differ_from_checked_in_pending_release", problems)
        self.assertIn("owner_action_diagnostics_differ_from_known_pending_release", problems)
        self.assertEqual(evidence["implementation_check"], "unexpected_result")

    def test_pending_jev_report_must_include_every_required_family_blocker(self):
        report = {
            "tool": "validate_jev_targets",
            "status": "blocked",
            "ready": False,
            "structurally_valid": False,
            "question_count": 0,
            "owner_evidence_verified": False,
            "errors": [{"code": "required_question_family_missing", "path": "$.questions",
                        "owner": "Agent and research"}],
        }
        evidence, problems = ci.classify_readiness_report("jev", 2, report)
        self.assertTrue(problems)
        self.assertIn("diagnostic_code_counts_differ_from_checked_in_pending_target_and_dataset", problems)
        self.assertIn("required_question_family_blockers_are_not_complete", problems)
        self.assertEqual(evidence["implementation_check"], "unexpected_result")


if __name__ == "__main__":
    unittest.main()
