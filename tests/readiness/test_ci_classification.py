"""Direct tests for CI's expected-pending classifier behavior."""

import unittest
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
