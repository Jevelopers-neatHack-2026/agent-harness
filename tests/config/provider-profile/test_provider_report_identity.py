import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[3]
CHECKER = "tools/verify_provider_report_identity.py"
VERIFIER = "tools/verify_provider_readiness.py"
PROFILE = "fixtures/provider-profile.json"
WORKFLOW = ROOT / ".github/workflows/provider-readiness.yml"


class ProviderReportIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.repo = self.base / "checkout"
        self.repo.mkdir()
        self.env = {"PATH": os.defpath, "PYTHONDONTWRITEBYTECODE": "1",
                    "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
                    "GIT_CONFIG_SYSTEM": os.devnull}
        for name in [CHECKER, VERIFIER, PROFILE]:
            target = self.repo / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, target)
        self.git("init", "-q")
        self.git("add", ".")
        self.commit()
        self.report = self.base / "provider-readiness.json"
        self.generate()

    def git(self, *args):
        return subprocess.run(["git", *args], cwd=self.repo, env=self.env,
                              check=True, capture_output=True, text=True, timeout=10).stdout.strip()

    def commit(self):
        self.git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                 "commit", "-q", "--allow-empty", "-m", "Controlled identity fixture")

    def generate(self):
        result = subprocess.run([sys.executable, "-B", str(self.repo / VERIFIER),
                                 "--profile", str(self.repo / PROFILE), "--mode", "offline"],
                                cwd=self.base, env=self.env, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 2, result.stderr)
        self.report.write_bytes(result.stdout)

    def check(self, cwd=None, environ=None):
        before = self.report.read_bytes()
        result = subprocess.run([sys.executable, "-B", str(self.repo / CHECKER), str(self.report)],
                                cwd=cwd or self.base, env=environ or self.env,
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(self.report.read_bytes(), before, "identity checking must not relabel a report")
        return result

    def test_real_generated_report_binds_revision_tree_and_unchanged_bytes(self):
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stderr)
        receipt = json.loads(result.stdout)
        self.assertEqual(receipt["revision"], self.git("rev-parse", "HEAD"))
        self.assertEqual(receipt["tree"], self.git("rev-parse", "HEAD^{tree}"))
        self.assertEqual(receipt["report_sha256"], hashlib.sha256(self.report.read_bytes()).hexdigest())
        self.assertEqual(receipt["verifier_sha256"], hashlib.sha256((self.repo / VERIFIER).read_bytes()).hexdigest())
        self.assertEqual(receipt["profile_sha256"], hashlib.sha256((self.repo / PROFILE).read_bytes()).hexdigest())

    def test_same_tree_older_revision_is_rejected_and_current_positive_still_passes(self):
        previous = self.git("rev-parse", "HEAD")
        tree = self.git("rev-parse", "HEAD^{tree}")
        self.commit()
        self.assertNotEqual(previous, self.git("rev-parse", "HEAD"))
        self.assertEqual(tree, self.git("rev-parse", "HEAD^{tree}"))
        self.assertEqual(self.check().returncode, 1)
        self.generate()
        self.assertEqual(self.check().returncode, 0)

    def test_foreign_cwd_and_git_environment_cannot_replace_script_source_identity(self):
        foreign = self.base / "foreign"
        foreign.mkdir()
        subprocess.run(["git", "init", "-q", str(foreign)], env=self.env, check=True)
        result = self.check(cwd=foreign, environ={**self.env, "GIT_DIR": str(foreign / ".git")})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["revision"], self.git("rev-parse", "HEAD"))

    def test_dirty_source_and_malformed_or_duplicate_identity_refuse_without_disclosure(self):
        original = self.report.read_bytes()
        for component in [None, {}, {"repository": "foreign", "revision": "private-sentinel"},
                          {"repository": "agent-harness", "revision": self.git("rev-parse", "HEAD"),
                           "working_tree_modified": 0}]:
            report = json.loads(original)
            report["component"] = component
            self.report.write_text(json.dumps(report))
            result = self.check()
            self.assertEqual(result.returncode, 1)
            self.assertNotIn("private-sentinel", result.stderr + result.stdout)
        self.report.write_text('{"component":{},"component":{}}')
        self.assertEqual(self.check().returncode, 1)
        self.report.write_bytes(original)
        (self.repo / PROFILE).write_text("modified source")
        self.assertEqual(self.check().returncode, 1)

    def test_actual_workflow_step_rejects_stale_generator_output_before_upload(self):
        # Execute the actual checked-in pending step, not a parallel validation recipe.
        workflow = WORKFLOW.read_text()
        step = workflow.split("      - name: Confirm the sample profile stays pending\n", 1)[1]
        step = step.split("      - name: Upload sanitized readiness report", 1)[0]
        shell = textwrap.dedent(step.split("        run: |\n", 1)[1])
        guard = 'python3 -B tools/verify_provider_report_identity.py "$RUNNER_TEMP/provider-readiness.json"'
        self.assertEqual(shell.count(guard), 1)
        old_report = self.report.read_text()
        (self.repo / VERIFIER).write_text("import sys\nsys.stdout.write(" + repr(old_report) + ")\nsys.exit(2)\n")
        self.git("add", VERIFIER)
        self.commit()
        runner_temp = self.base / "runner-temp"
        runner_temp.mkdir()
        env = {**self.env, "RUNNER_TEMP": str(runner_temp)}
        def run(command):
            return subprocess.run(["/bin/bash", "-e", "-c", command], cwd=self.repo, env=env,
                                  capture_output=True, text=True, timeout=15)
        # The former behavior accepts the identical stale fixture; the workflow guard rejects it.
        self.assertEqual(run(shell.replace(guard, "")).returncode, 0)
        failed = run(shell)
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("source identity did not qualify", failed.stderr)
        self.assertEqual(json.loads((runner_temp / "provider-readiness.json").read_text())
                         ["component"]["revision"], json.loads(old_report)["component"]["revision"])


if __name__ == "__main__":
    unittest.main()
