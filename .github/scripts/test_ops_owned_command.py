"""Fake-only protocol and validation checks for the fixed OPS owner.

These tests use standard-library temporary files only. They never launch or
signal a process group and do not invoke npm, Make, Node, a fixture, or CI.
"""
from __future__ import annotations

import ast
import errno
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import posixpath
import tempfile
import threading
import types
import unittest
from contextlib import contextmanager
from unittest.mock import patch


OWNER_PATH = Path(__file__).with_name("run_ops_owned_command.py")
SPEC = importlib.util.spec_from_file_location("ops_owned_command_under_test", OWNER_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("owner source unavailable")
OWNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(OWNER)


def receipt(route: str) -> dict[str, object]:
    return {
        "schema": OWNER.RECEIPT_SCHEMA,
        "route": route,
        "argvClass": OWNER.ROUTES[route]["argvClass"],
        "commandExitCode": 0,
        "wrapperExitCode": 0,
        "timeoutStage": "none",
        "escalationStage": "none",
        "commandDirectReaped": True,
        "commandGroupAbsent": True,
        "watchdogReaped": True,
        "watchdogGroupAbsent": True,
        "outerRegistryEmpty": True,
        "localRegistryEmpty": True,
        "sidecarsReconciled": True,
        "outputOverflow": False,
        "captureReadFailed": False,
        "elapsedMs": 20,
        "elapsedCapped": False,
        "groupsObserved": 0,
        "sidecarsObserved": 0,
        "sidecarsReconciledCount": 0,
        "pendingLaunchesRemaining": 0,
        "stdout": {"bytes": 0, "sha256": hashlib.sha256(b"").hexdigest()},
        "stderr": {"bytes": 0, "sha256": hashlib.sha256(b"").hexdigest()},
    }


class ExitedProcess:
    def poll(self) -> int:
        return 0


class FakePipe:
    def __init__(self, chunks: list[bytes], *, hold_open_after_chunks: bool = False,
                 fail_read: bool = False):
        self.chunks = list(chunks)
        self.hold_open_after_chunks = hold_open_after_chunks
        self.fail_read = fail_read
        self.last_chunk_returned = threading.Event()
        self.release = threading.Event()

    def read1(self, _size: int) -> bytes:
        if self.fail_read:
            raise OSError("fake read failure")
        if self.chunks:
            chunk = self.chunks.pop(0)
            if not self.chunks:
                self.last_chunk_returned.set()
            return chunk
        if self.hold_open_after_chunks:
            # Keep the negative "pipe is still open" assertion meaningful,
            # but never let a broken test cleanup hold interpreter shutdown.
            self.release.wait(1.0)
        return b""


def run_fake_capture_assertions(thread: threading.Thread, pipe: FakePipe,
                                assertions) -> None:
    thread.daemon = True
    thread.start()
    try:
        assertions()
    finally:
        pipe.release.set()
        thread.join(0.5)
    if thread.is_alive():
        raise AssertionError("fake capture thread did not stop after release")


def private_file(path: Path, data: bytes = b"") -> None:
    path.write_bytes(data)
    os.chmod(path, 0o600)


def custody_sidecar(directory: Path, token: str, *, group_id: int | None = None,
                    registered: bool = False, cleared: bool = False,
                    group_absent: bool = False) -> dict[str, object]:
    return {
        "schema": OWNER.SIDECAR_SCHEMA,
        "ownerPid": os.getpid(),
        "directory": str(directory),
        "launches": [{
            "launchToken": token,
            "status": "launch_intent",
            "spawnAttempted": group_id is not None,
            "directPid": group_id,
            "groupId": group_id,
            "registryRegistered": registered,
            "registryCleared": cleared,
            "groupAbsent": group_absent,
        }],
    }


class FakePublicationFileSystem:
    def __init__(self, *, fail_at: str | None = None, cleanup_fails: bool = False):
        self.fail_at = fail_at
        self.cleanup_fails = cleanup_fails
        self.files: dict[str, bytearray] = {}
        self.open_calls: list[tuple[str, int, int | None]] = []
        self.write_calls: list[bytes] = []
        self.close_calls: list[int] = []
        self.rename_calls: list[tuple[str, str]] = []
        self.unlink_calls: list[str] = []
        self.events: list[tuple[object, ...]] = []
        self.temp_fd = 71
        self.directory_fd = 72
        self.temp_path: str | None = None
        self.target_path: str | None = None
        self.primary_write_error = OSError("synthetic primary write failure")
        self.close_error = OSError("synthetic close failure")
        self.rename_error = OSError("synthetic rename failure")
        self.cleanup_error = OSError("synthetic cleanup failure")
        self.acquisition_error = FileExistsError(errno.EEXIST, "synthetic existing sentinel")
        self.sentinel = b"preexisting-sentinel"

    def open(self, path, flags: int, mode: int | None = None) -> int:
        path_text = str(path)
        self.open_calls.append((path_text, flags, mode))
        self.events.append(("open", path_text, flags, mode))
        if flags & os.O_CREAT:
            if not flags & os.O_EXCL:
                raise AssertionError("temporary creation did not request exclusive acquisition")
            if mode != 0o600:
                raise AssertionError("temporary creation did not request private mode")
            self.temp_path = path_text
            if self.fail_at == "acquire":
                self.files[path_text] = bytearray(self.sentinel)
                raise self.acquisition_error
            self.files[path_text] = bytearray()
            return self.temp_fd
        if flags == os.O_RDONLY:
            self.target_path = path_text
            return self.directory_fd
        raise AssertionError("unexpected fake open flags")

    def fchmod(self, descriptor: int, mode: int) -> None:
        self.events.append(("fchmod", descriptor, mode))

    def write(self, descriptor: int, data) -> int:
        if descriptor != self.temp_fd or self.temp_path is None:
            raise AssertionError("write used an unowned fake descriptor")
        chunk = bytes(data)
        self.write_calls.append(chunk)
        self.events.append(("write", descriptor, len(chunk)))
        if self.fail_at in {"write", "write_close_cleanup"} and len(self.write_calls) == 2:
            raise self.primary_write_error
        if self.fail_at == "zero_progress":
            return 0
        if self.fail_at == "invalid_progress":
            return len(chunk) + 1
        written = max(1, len(chunk) // 2)
        self.files[self.temp_path].extend(chunk[:written])
        return written

    def fsync(self, descriptor: int) -> None:
        self.events.append(("fsync", descriptor))
        if self.fail_at == "fsync":
            raise OSError("synthetic fsync failure")

    def close(self, descriptor: int) -> None:
        self.close_calls.append(descriptor)
        self.events.append(("close", descriptor))
        if self.fail_at in {"close", "write_close_cleanup"} and descriptor == self.temp_fd:
            raise self.close_error

    def replace(self, source, destination) -> None:
        source_text = str(source)
        destination_text = str(destination)
        self.rename_calls.append((source_text, destination_text))
        self.events.append(("replace", source_text, destination_text))
        if self.fail_at in {"rename", "rename_cleanup"}:
            raise self.rename_error
        self.files[destination_text] = self.files.pop(source_text)

    def unlink(self, path: Path) -> None:
        path_text = str(path)
        self.unlink_calls.append(path_text)
        self.events.append(("unlink", path_text))
        if self.cleanup_fails or self.fail_at in {"write_close_cleanup", "rename_cleanup"}:
            raise self.cleanup_error
        self.files.pop(path_text, None)

    def embedded_os(self):
        return types.SimpleNamespace(
            O_WRONLY=os.O_WRONLY,
            O_CREAT=os.O_CREAT,
            O_EXCL=os.O_EXCL,
            O_RDONLY=os.O_RDONLY,
            getpid=lambda: 12_345,
            open=self.open,
            fchmod=self.fchmod,
            write=self.write,
            fsync=self.fsync,
            close=self.close,
            replace=self.replace,
            unlink=lambda path: self.unlink(Path(path)),
            path=posixpath,
        )


@contextmanager
def patched_owner_filesystem(fake: FakePublicationFileSystem):
    with (
        patch.object(OWNER.os, "open", side_effect=fake.open),
        patch.object(OWNER.os, "fchmod", side_effect=fake.fchmod),
        patch.object(OWNER.os, "write", side_effect=fake.write),
        patch.object(OWNER.os, "fsync", side_effect=fake.fsync),
        patch.object(OWNER.os, "close", side_effect=fake.close),
        patch.object(OWNER.os, "replace", side_effect=fake.replace),
        patch.object(Path, "unlink", autospec=True, side_effect=fake.unlink),
        patch.object(OWNER.os, "getpid", return_value=12_345),
        patch.object(OWNER.uuid, "uuid4", return_value="deterministic-token"),
    ):
        yield


def extract_watchdog_atomic_write(file_system: FakePublicationFileSystem):
    source = OWNER._watchdog_source()
    module = ast.parse(source)
    function = next(
        node for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == "atomic_write"
    )
    function_module = ast.Module(body=[function], type_ignores=[])
    namespace = {
        "json": json,
        "os": file_system.embedded_os(),
        "uuid": types.SimpleNamespace(uuid4=lambda: "deterministic-token"),
    }
    exec(compile(function_module, "<embedded-watchdog-atomic-write>", "exec"), namespace)
    return namespace["atomic_write"]


class AtomicPublisherTests(unittest.TestCase):
    def assert_no_post_acquisition_calls(self, fake: FakePublicationFileSystem) -> None:
        self.assertEqual(fake.write_calls, [])
        self.assertEqual(fake.close_calls, [])
        self.assertEqual(fake.rename_calls, [])
        self.assertEqual(fake.unlink_calls, [])
        self.assertIsNotNone(fake.temp_path)
        self.assertEqual(fake.files[fake.temp_path], bytearray(fake.sentinel))

    def test_python_publishers_never_remove_eexist_sentinels(self) -> None:
        target = Path("/private/ops-owner/marker.json")
        source = Path("/private/ops-owner/input.tap")
        publishers = (
            ("write_new", lambda: OWNER.write_new(target, b"marker", 0o600)),
            ("atomic_write_json", lambda: OWNER.atomic_write_json(target, {"ok": True})),
            ("copy_fixed_tap", lambda: OWNER.copy_fixed_tap(source, target)),
        )
        for name, publish in publishers:
            with self.subTest(publisher=name):
                fake = FakePublicationFileSystem(fail_at="acquire")
                with patched_owner_filesystem(fake), patch.object(
                    OWNER, "read_bounded", return_value=b"fixed tap"
                ):
                    if name == "write_new":
                        with self.assertRaises(OWNER.OwnerFailure):
                            publish()
                    else:
                        self.assertFalse(publish())
                self.assert_no_post_acquisition_calls(fake)

    def test_python_write_new_bounds_progress_and_preserves_primary_failure(self) -> None:
        payload = b"bounded synthetic payload"
        successful = FakePublicationFileSystem()
        with patched_owner_filesystem(successful):
            OWNER.write_new(Path("/private/ops-owner/complete.json"), payload, 0o600)
        self.assertGreater(len(successful.write_calls), 1)
        self.assertEqual(successful.files[successful.temp_path], bytearray(payload))
        self.assertEqual(successful.close_calls, [successful.temp_fd])
        self.assertLess(
            max(index for index, event in enumerate(successful.events) if event[0] == "write"),
            successful.events.index(("close", successful.temp_fd)),
        )

        for fail_at in ("zero_progress", "invalid_progress"):
            with self.subTest(fail_at=fail_at):
                fake = FakePublicationFileSystem(fail_at=fail_at)
                with patched_owner_filesystem(fake):
                    with self.assertRaises(OWNER.OwnerFailure) as caught:
                        OWNER.write_new(Path("/private/ops-owner/partial.json"), payload, 0o600)
                self.assertEqual(len(fake.write_calls), 1)
                self.assertEqual(fake.close_calls, [fake.temp_fd])
                self.assertEqual(fake.unlink_calls, [fake.temp_path])
                self.assertIsInstance(caught.exception.__cause__, OSError)

        close_failure = FakePublicationFileSystem(fail_at="close", cleanup_fails=True)
        with patched_owner_filesystem(close_failure):
            with self.assertRaises(OWNER.OwnerFailure) as caught:
                OWNER.write_new(Path("/private/ops-owner/close-failure.json"), payload, 0o600)
        self.assertIs(caught.exception.__cause__, close_failure.close_error)
        self.assertEqual(close_failure.close_calls, [close_failure.temp_fd])
        self.assertEqual(close_failure.unlink_calls, [close_failure.temp_path])
        self.assertIn(close_failure.temp_path, close_failure.files,
                      "cleanup failure retains the acquired partial file without replacing close failure")

        combined = FakePublicationFileSystem(fail_at="write_close_cleanup")
        with patched_owner_filesystem(combined):
            with self.assertRaises(OWNER.OwnerFailure) as caught:
                OWNER.write_new(Path("/private/ops-owner/partial.json"), payload, 0o600)
        self.assertIs(caught.exception.__cause__, combined.primary_write_error)
        self.assertEqual(combined.close_calls, [combined.temp_fd])
        self.assertEqual(combined.unlink_calls, [combined.temp_path])
        self.assertEqual(combined.files[combined.temp_path], bytearray(payload[:len(payload) // 2]))

    def test_atomic_json_and_tap_copy_cleanup_only_acquired_temporaries(self) -> None:
        target = Path("/private/ops-owner/published.json")
        source = Path("/private/ops-owner/input.tap")
        for name, publish in (
            ("atomic_write_json", lambda: OWNER.atomic_write_json(target, {"groups": [1, 2]})),
            ("copy_fixed_tap", lambda: OWNER.copy_fixed_tap(source, target)),
        ):
            with self.subTest(publisher=name):
                fake = FakePublicationFileSystem()
                with patched_owner_filesystem(fake), patch.object(
                    OWNER, "read_bounded", return_value=b"small fake tap"
                ):
                    self.assertTrue(publish())
                self.assertGreater(len(fake.write_calls), 1)
                self.assertEqual(len(fake.rename_calls), 1)
                self.assertNotIn(fake.temp_path, fake.files)
                expected = (b"small fake tap" if name == "copy_fixed_tap"
                            else b'{"groups":[1,2]}\n')
                self.assertEqual(fake.files[str(target)], bytearray(expected))
                self.assertEqual(fake.close_calls.count(fake.temp_fd), 1)
                self.assertLess(
                    fake.events.index(("close", fake.temp_fd)),
                    fake.events.index(("replace", fake.temp_path, str(target))),
                )

        for name, publish in (
            ("atomic_write_json", lambda: OWNER.atomic_write_json(target, {"groups": [1, 2]})),
            ("copy_fixed_tap", lambda: OWNER.copy_fixed_tap(source, target)),
        ):
            with self.subTest(partial_write=name):
                fake = FakePublicationFileSystem(fail_at="write")
                with patched_owner_filesystem(fake), patch.object(
                    OWNER, "read_bounded", return_value=b"small fake tap"
                ):
                    self.assertFalse(publish())
                self.assertEqual(len(fake.write_calls), 2)
                self.assertEqual(fake.close_calls, [fake.temp_fd])
                self.assertEqual(fake.unlink_calls, [fake.temp_path],
                                 "the writer that acquired the temp cleans it exactly once")
                self.assertNotIn(fake.temp_path, fake.files)
                self.assertEqual(fake.rename_calls, [])

        for name, publish in (
            ("atomic_write_json", lambda: OWNER.atomic_write_json(target, {"groups": [1, 2]})),
            ("copy_fixed_tap", lambda: OWNER.copy_fixed_tap(source, target)),
        ):
            with self.subTest(rename_cleanup=name):
                fake = FakePublicationFileSystem(fail_at="rename_cleanup")
                with patched_owner_filesystem(fake), patch.object(
                    OWNER, "read_bounded", return_value=b"small fake tap"
                ):
                    self.assertFalse(publish())
                self.assertEqual(len(fake.rename_calls), 1)
                self.assertEqual(fake.unlink_calls, [fake.temp_path])
                self.assertIn(fake.temp_path, fake.files)
                self.assertEqual(fake.close_calls.count(fake.temp_fd), 1)

    def test_embedded_watchdog_publisher_uses_exclusive_acquisition_and_fake_filesystem(self) -> None:
        collision = FakePublicationFileSystem(fail_at="acquire")
        publish_collision = extract_watchdog_atomic_write(collision)
        self.assertFalse(publish_collision("/private/watchdog/ready.json", {"ready": True}))
        self.assert_no_post_acquisition_calls(collision)

        success = FakePublicationFileSystem()
        publish_success = extract_watchdog_atomic_write(success)
        self.assertTrue(publish_success("/private/watchdog/ready.json", {"ready": True}))
        self.assertGreater(len(success.write_calls), 1)
        self.assertEqual(success.close_calls.count(success.temp_fd), 1)
        self.assertEqual(success.rename_calls, [(success.temp_path, "/private/watchdog/ready.json")])
        self.assertNotIn(success.temp_path, success.files)
        self.assertIn("/private/watchdog/ready.json", success.files)
        self.assertEqual(json.loads(success.files["/private/watchdog/ready.json"]), {"ready": True})

        failed = FakePublicationFileSystem(fail_at="write_close_cleanup", cleanup_fails=True)
        publish_failed = extract_watchdog_atomic_write(failed)
        self.assertFalse(publish_failed("/private/watchdog/ready.json", {"ready": True}))
        self.assertEqual(failed.close_calls.count(failed.temp_fd), 1)
        self.assertEqual(failed.rename_calls, [])
        self.assertEqual(failed.unlink_calls, [failed.temp_path])
        self.assertIn(failed.temp_path, failed.files)


def valid_watchdog_result(pid: int, target_pgid: int | None,
                          control_state: str = "finish", *,
                          target_escalation: str = "none",
                          timed_out: bool = False) -> dict[str, object]:
    acknowledged = target_pgid is not None
    cleanup_requested = timed_out or control_state == "cleanup"
    target_cleaned = acknowledged and cleanup_requested
    target_cleanup = {
        "groups": [target_pgid] if target_cleaned else [],
        "final": {str(target_pgid): False} if target_cleaned else {},
        "allAbsent": True,
        "term": ({str(target_pgid): "sent"}
                 if target_cleaned and target_escalation == "term" else {}),
        "kill": ({str(target_pgid): "sent"}
                 if target_cleaned and target_escalation == "kill" else {}),
    }
    if not acknowledged or not cleanup_requested:
        target_cleanup["not_needed"] = True
        target_cleanup.pop("term")
        target_cleanup.pop("kill")
    registry_cleanup: dict[str, object] = {
        "groups": [], "final": {}, "allAbsent": True,
    }
    if timed_out:
        registry_cleanup.update({"term": {}, "kill": {}})
    else:
        registry_cleanup["not_needed"] = True
    return {
        "schema": "ops-owned-group-watchdog-result.v1",
        "pid": pid,
        "pgid": pid,
        "startedMonotonicNs": 1,
        "timedOut": timed_out,
        "controlState": "watchdog_timeout" if timed_out else control_state,
        "targetPgid": target_pgid,
        "targetAckSent": acknowledged,
        "targetCleanup": target_cleanup,
        "registryError": None,
        "registeredGroupCleanup": registry_cleanup,
        "elapsedMs": 25,
    }


def run_fake_route_case(root: Path, *, ack_ready: bool = True,
                        gate_write: bool = True, command_exit: int = 0,
                        result_mode: str = "valid", target_escalation: str = "none",
                        parent_escalation: str = "none",
                        custody_escalation: str = "none",
                        maximum_schedule: bool = False
                        ) -> tuple[int, dict[str, object], dict[str, object]]:
    """Drive run_route with fake processes, probes, signals, and wait budget."""
    events: list[tuple[object, ...]] = []
    processes: list[object] = []
    budget_refs: list[OWNER.WaitBudget] = []
    clock_ms = [0]
    watchdog_pid = 45_001
    target_pid = 45_002
    state: dict[str, object] = {
        "gate_released": False,
        "wait_grants": [],
        "signal_calls": [],
        "group_probes": [],
        "getpgid_calls": [],
        "killpg_calls": [],
        "custody_watchdog_cleanup": None,
        "result_read_after_exit": False,
    }

    class FakeProcess:
        def __init__(self, pid: int, returncode: int | None):
            self.pid = pid
            self.returncode = returncode
            self.stdout = object()
            self.stderr = object()

        def poll(self) -> int | None:
            return self.returncode

    class FakeCapture:
        def __init__(self, paths: dict[str, Path]):
            self.paths = paths
            self.overflow = threading.Event()
            self.read_failed = threading.Event()

        def start(self, process: FakeProcess) -> None:
            events.append(("capture_start", process.pid))

    def consume_waits(budget: OWNER.WaitBudget, waits: list[int]) -> None:
        for requested in waits:
            started = clock_ms[0] / 1000
            granted, limited = budget.reserve(requested)
            state["wait_grants"].append((requested, granted))
            clock_ms[0] += granted
            budget.settle(granted, started, limited, True)

    def fake_popen(_argv, **_kwargs):
        if not processes:
            process = FakeProcess(watchdog_pid, None)
            events.append(("popen_watchdog", watchdog_pid))
        else:
            process = FakeProcess(target_pid, None)
            events.append(("popen_target", target_pid))
        processes.append(process)
        return process

    def fake_wait_file(path: Path, _process: FakeProcess, _timeout_ms: int,
                       _budget: OWNER.WaitBudget | None = None) -> bool:
        result = path.name == "watchdog-ready.json" or (
            path.name == "watchdog-target-ack.json" and ack_ready
        )
        events.append(("wait_file", path.name, result))
        return result

    def fake_read_small_json(path: Path, _limit: int = 4_096):
        events.append(("read_json", path.name))
        if path.name == "watchdog-ready.json":
            return {
                "schema": "ops-owned-group-watchdog-ready.v1",
                "pid": watchdog_pid,
                "pgid": watchdog_pid,
                "startedMonotonicNs": 1,
            }
        if path.name == "watchdog-target-ack.json":
            return {
                "schema": "ops-owned-group-target-ack.v1",
                "targetPgid": target_pid,
                "verified": True,
            }
        if path.name == "watchdog-result.json":
            state["result_read_after_exit"] = processes[0].returncode is not None
            if result_mode == "malformed":
                return {"schema": "invalid"}
            if not ack_ready:
                return valid_watchdog_result(watchdog_pid, None, "cleanup")
            timed_out = result_mode == "timeout"
            control_state = ("finish" if command_exit == 0 and gate_write and not timed_out
                             else "cleanup")
            return valid_watchdog_result(
                watchdog_pid, target_pid, control_state,
                target_escalation=target_escalation, timed_out=timed_out,
            )
        raise AssertionError("unexpected fake protocol path")

    def fake_control(_path: Path, control_state: str, pgid: int | None) -> bool:
        events.append(("control", control_state, pgid))
        return True

    def fake_gate_write(_path: Path, _value: object, _mode: int = 0o600) -> bool:
        state["gate_released"] = gate_write
        if gate_write:
            processes[1].returncode = command_exit
        events.append(("gate_write", gate_write))
        return gate_write

    def fake_signal(signum: int, handler: object) -> object:
        events.append(("signal", signum, handler))
        state["signal_calls"].append((signum, handler))
        return f"previous:{signum}"

    def fake_stop_watchdog(process: FakeProcess, _pgid: int,
                           budget: OWNER.WaitBudget) -> dict[str, object]:
        process.returncode = 0
        events.append(("stop_watchdog", process.pid))
        budget_refs.append(budget)
        if maximum_schedule:
            consume_waits(budget, list(OWNER.WATCHDOG_STOP_SEQUENCE_MS))
        return {
            "exitCode": 0,
            "reaped": True,
            "groupAbsent": True,
            "cleanup": {"allAbsent": True, "groups": []},
        }

    def fake_cleanup_target(pgid: int, process: FakeProcess,
                            previous_exit: int | None, _reaped: bool,
                            _capture: FakeCapture, budget: OWNER.WaitBudget,
                            _cleanup_attempted: bool):
        events.append(("cleanup_target", pgid))
        budget_refs.append(budget)
        if maximum_schedule:
            consume_waits(budget, [
                OWNER.TERM_WAIT_MS, OWNER.KILL_WAIT_MS,
                OWNER.DIRECT_REAP_MS, OWNER.TARGET_RECHECK_MS,
            ])
        if process.returncode is None:
            process.returncode = command_exit
        return (process.returncode if previous_exit is None else previous_exit,
                True, True, True, parent_escalation)

    def fake_reconcile(_tmp_root: Path, _registry_path: Path,
                       _target_pgid: int | None, _watchdog_pgid: int | None,
                       _identities: dict[str, tuple[int, int]],
                       budget: OWNER.WaitBudget,
                       watchdog_cleanup: dict[str, object] | None = None
                       ) -> dict[str, object]:
        events.append(("reconcile_custody",))
        budget_refs.append(budget)
        state["custody_watchdog_cleanup"] = watchdog_cleanup
        if maximum_schedule:
            consume_waits(budget, [
                OWNER.TERM_WAIT_MS, OWNER.KILL_WAIT_MS, OWNER.TARGET_RECHECK_MS,
                *([OWNER.SIDECAR_GROUP_PROBE_MS] * OWNER.SIDECAR_GROUP_PROBE_MAX),
            ])
        return {
            "outerRegistryEmpty": True,
            "localRegistryEmpty": True,
            "sidecarsReconciled": True,
            "groupsObserved": 0,
            "sidecarsObserved": 0,
            "sidecarsReconciledCount": 0,
            "pendingLaunchesRemaining": 0,
            "escalationStage": custody_escalation,
        }

    def fake_group_exists(pgid: int) -> bool:
        state["group_probes"].append(pgid)
        return True

    def fake_killpg(pgid: int, sig: int) -> None:
        state["killpg_calls"].append((pgid, sig))

    def fake_getpgid(pid: int) -> int:
        state["getpgid_calls"].append(pid)
        return pid

    with (
        patch.object(OWNER.subprocess, "Popen", side_effect=fake_popen),
        patch.object(OWNER.shutil, "which", return_value="/fake/bin/npm"),
        patch.object(OWNER, "private_environment", return_value={"PATH": "/fake/bin"}),
        patch.object(OWNER, "Capture", FakeCapture),
        patch.object(OWNER, "wait_file", side_effect=fake_wait_file),
        patch.object(OWNER, "read_small_json", side_effect=fake_read_small_json),
        patch.object(OWNER, "atomic_control", side_effect=fake_control),
        patch.object(OWNER, "atomic_write_json", side_effect=fake_gate_write),
        patch.object(OWNER, "stop_watchdog", side_effect=fake_stop_watchdog),
        patch.object(OWNER, "cleanup_target_group", side_effect=fake_cleanup_target),
        patch.object(OWNER, "reconcile_custody", side_effect=fake_reconcile),
        patch.object(OWNER, "group_exists", side_effect=fake_group_exists),
        patch.object(OWNER.os, "getpgid", side_effect=fake_getpgid),
        patch.object(OWNER.os, "getpgrp", return_value=45_000),
        patch.object(OWNER.os, "killpg", side_effect=fake_killpg),
        patch.object(OWNER.signal, "signal", side_effect=fake_signal),
        patch.object(OWNER.time, "monotonic", side_effect=lambda: clock_ms[0] / 1000),
        patch.object(OWNER.sys, "stderr", io.StringIO()),
    ):
        status = OWNER.run_route("runtime-foundation", root)

    receipt_path = root / ".runtime-artifacts" / str(
        OWNER.ROUTES["runtime-foundation"]["receipt"]
    )
    saved_receipt = OWNER.strict_json_bytes(
        OWNER.read_bounded(receipt_path, OWNER.RECEIPT_MAX_BYTES, mode=0o600)
    )
    state["events"] = events
    state["process_count"] = len(processes)
    state["budget_refs"] = budget_refs
    state["budget_state"] = (
        budget_refs[0].maximum_ms, budget_refs[0].remaining_ms,
        budget_refs[0].used_ms, budget_refs[0].exhausted,
    ) if budget_refs else None
    state["watchdog_pid"] = watchdog_pid
    state["target_pid"] = target_pid
    return status, saved_receipt, state


class RouteValidationTests(unittest.TestCase):
    def test_run_route_fake_readiness_ack_gate_and_signal_restoration(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            status, saved, state = run_fake_route_case(root)

        events = state["events"]
        self.assertEqual(status, 0)
        self.assertTrue(OWNER.validate_receipt_value(saved, "runtime-foundation"))
        self.assertEqual(set(saved), OWNER.RECEIPT_KEYS)
        self.assertEqual(state["process_count"], 2)
        self.assertTrue(state["gate_released"])
        self.assertEqual(state["group_probes"], [state["watchdog_pid"]])
        self.assertEqual(state["getpgid_calls"], [state["watchdog_pid"]])
        self.assertEqual(state["killpg_calls"], [])
        self.assertTrue(state["result_read_after_exit"])
        self.assertLess(
            events.index(("read_json", "watchdog-ready.json")),
            events.index(("popen_target", state["target_pid"])),
        )
        self.assertLess(
            events.index(("read_json", "watchdog-target-ack.json")),
            events.index(("gate_write", True)),
        )
        self.assertLess(
            events.index(("stop_watchdog", state["watchdog_pid"])),
            events.index(("read_json", "watchdog-result.json")),
        )
        for signum in OWNER.PROCESS_SIGNAL_NAMES:
            calls = [(called_signum, handler) for called_signum, handler
                     in state["signal_calls"] if called_signum == signum]
            self.assertEqual(len(calls), 2)
            self.assertIs(calls[0][1], OWNER._signal_handler)
            self.assertEqual(calls[1][1], f"previous:{signum}")

    def test_run_route_fake_failures_before_and_after_ack_retain_cleanup(self) -> None:
        cases = (
            {"ack_ready": False, "gate_write": True, "gate_expected": False},
            {"ack_ready": True, "gate_write": False, "gate_expected": False},
        )
        for case in cases:
            with self.subTest(ack_ready=case["ack_ready"]), tempfile.TemporaryDirectory() as temporary:
                status, saved, state = run_fake_route_case(
                    Path(temporary), ack_ready=case["ack_ready"],
                    gate_write=case["gate_write"],
                )
                events = state["events"]
                self.assertEqual(status, 1)
                self.assertEqual(saved["wrapperExitCode"], 1)
                self.assertEqual(saved["commandExitCode"], 0)
                self.assertEqual(saved["timeoutStage"], "launch_gate")
                self.assertTrue(saved["commandGroupAbsent"])
                self.assertTrue(saved["sidecarsReconciled"])
                self.assertEqual(state["gate_released"], case["gate_expected"])
                self.assertEqual(state["process_count"], 2)
                self.assertIn(("cleanup_target", state["target_pid"]), events)
                if case["ack_ready"]:
                    self.assertLess(
                        events.index(("read_json", "watchdog-target-ack.json")),
                        events.index(("gate_write", False)),
                    )
                else:
                    self.assertFalse(any(
                        event[0] == "gate_write" for event in events
                    ))
                self.assertEqual(state["killpg_calls"], [])

    def test_run_route_fake_timeout_and_malformed_watchdog_result_fail_closed(self) -> None:
        for result_mode in ("timeout", "malformed"):
            with self.subTest(result_mode=result_mode), tempfile.TemporaryDirectory() as temporary:
                status, saved, state = run_fake_route_case(
                    Path(temporary), result_mode=result_mode,
                )
                self.assertEqual(status, 1)
                self.assertEqual(saved["wrapperExitCode"], 1)
                self.assertTrue(saved["commandGroupAbsent"])
                self.assertEqual(state["killpg_calls"], [])
                if result_mode == "timeout":
                    self.assertEqual(saved["timeoutStage"], "watchdog")
                    self.assertTrue(saved["sidecarsReconciled"])
                    self.assertIsNotNone(state["custody_watchdog_cleanup"])
                else:
                    self.assertFalse(saved["sidecarsReconciled"])
                    self.assertIsNone(state["custody_watchdog_cleanup"])

    def test_run_route_fake_receipt_preserves_watchdog_escalation_and_command_status(self) -> None:
        cases = (("term", "none", "term"), ("kill", "term", "kill"))
        for watchdog_stage, registry_stage, expected_stage in cases:
            with self.subTest(watchdog_stage=watchdog_stage), tempfile.TemporaryDirectory() as temporary:
                status, saved, state = run_fake_route_case(
                    Path(temporary), command_exit=37,
                    target_escalation=watchdog_stage,
                    custody_escalation=registry_stage,
                )
                self.assertEqual(status, 37)
                self.assertEqual(saved["commandExitCode"], 37)
                self.assertEqual(saved["wrapperExitCode"], 37)
                self.assertEqual(saved["escalationStage"], expected_stage)
                self.assertTrue(saved["commandGroupAbsent"])
                self.assertEqual(set(saved), OWNER.RECEIPT_KEYS)
                self.assertEqual(state["killpg_calls"], [])

    def test_run_route_fake_uses_one_shared_maximum_wait_schedule(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            status, saved, state = run_fake_route_case(
                Path(temporary), parent_escalation="term", maximum_schedule=True,
            )

        expected = (
            sum(OWNER.WATCHDOG_STOP_SEQUENCE_MS)
            + OWNER.TERM_WAIT_MS + OWNER.KILL_WAIT_MS
            + OWNER.DIRECT_REAP_MS + OWNER.TARGET_RECHECK_MS
            + OWNER.TERM_WAIT_MS + OWNER.KILL_WAIT_MS + OWNER.TARGET_RECHECK_MS
            + OWNER.SIDECAR_GROUP_PROBE_MS * OWNER.SIDECAR_GROUP_PROBE_MAX
        )
        refs = state["budget_refs"]
        self.assertEqual(status, 0)
        self.assertEqual(saved["escalationStage"], "term")
        self.assertEqual(expected, OWNER.POST_COMMAND_WAIT_MAX_MS)
        self.assertEqual(len(refs), 3)
        self.assertTrue(all(budget is refs[0] for budget in refs))
        self.assertEqual(
            state["budget_state"],
            (OWNER.POST_COMMAND_WAIT_MAX_MS, 0, OWNER.POST_COMMAND_WAIT_MAX_MS, False),
        )
        self.assertTrue(all(requested == granted for requested, granted in state["wait_grants"]))
        self.assertEqual(state["killpg_calls"], [])

    def test_escalation_precedence_is_fixed_and_fail_closed(self) -> None:
        self.assertEqual(OWNER.combine_escalation_stages("none", "term"), "term")
        self.assertEqual(OWNER.combine_escalation_stages("term", "kill"), "kill")
        self.assertEqual(OWNER.combine_escalation_stages("kill", "unconfirmed"), "unconfirmed")
        self.assertEqual(OWNER.combine_escalation_stages("term", "private detail"), "unconfirmed")

    def test_exact_route_vectors_only(self) -> None:
        for route in OWNER.ROUTE_IDS:
            self.assertEqual(OWNER.parse_route_args(["--route", route]), route)
        self.assertIsNone(OWNER.parse_route_args(["--route", "npm", "test"]))
        self.assertIsNone(OWNER.parse_route_args(["--route", "runtime-foundation", "extra"]))
        self.assertIsNone(OWNER.parse_route_args(["--route", "unknown"]))

    def test_registry_bounds_and_types(self) -> None:
        valid = {"schema": OWNER.REGISTRY_SCHEMA, "groups": list(range(2, 18)), "pendingLaunches": []}
        self.assertEqual(len(OWNER.validate_registry_value(valid)["groups"]), 16)
        oversized = {**valid, "groups": list(range(2, 19))}
        with self.assertRaises(OWNER.OwnerFailure):
            OWNER.validate_registry_value(oversized)
        malformed = {"schema": OWNER.REGISTRY_SCHEMA, "groups": [True], "pendingLaunches": []}
        with self.assertRaises(OWNER.OwnerFailure):
            OWNER.validate_registry_value(malformed)
        duplicate = {"schema": OWNER.REGISTRY_SCHEMA, "groups": [123, 123], "pendingLaunches": []}
        with self.assertRaises(OWNER.OwnerFailure):
            OWNER.validate_registry_value(duplicate)

    def test_duplicate_json_keys_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            OWNER.strict_json_bytes(b'{"schema":"first","schema":"second"}')

    def test_receipt_requires_exact_fixed_shape(self) -> None:
        candidate = receipt("runtime-foundation")
        self.assertTrue(OWNER.validate_receipt_value(candidate, "runtime-foundation"))
        candidate["privatePath"] = "/private/location"
        self.assertFalse(OWNER.validate_receipt_value(candidate, "runtime-foundation"))

    def test_sidecar_discovery_distinguishes_local_registry_namespace(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            tmp_root = Path(temporary)
            retry_dir = tmp_root / "ah11-ops-temp-custody-retry-fixture"
            retry_dir.mkdir(mode=0o700)
            os.chmod(retry_dir, 0o700)
            registry_path = retry_dir / "owned-groups.json"
            registry = {"schema": OWNER.REGISTRY_SCHEMA, "groups": [], "pendingLaunches": []}
            registry_path.write_text(json.dumps(registry) + "\n", encoding="utf-8")
            os.chmod(registry_path, 0o600)
            directory_text = str(retry_dir)
            tag = hashlib.sha256(directory_text.encode("utf-8")).hexdigest()[:16]
            sidecar_path = retry_dir / (
                f"owned-groups.json.temp-custody.{os.getpid()}.{tag}.json"
            )
            sidecar = {
                "schema": OWNER.SIDECAR_SCHEMA,
                "ownerPid": os.getpid(),
                "directory": directory_text,
                "launches": [{
                    "launchToken": "00000000-0000-4000-8000-000000000001",
                    "status": "launch_intent",
                    "spawnAttempted": False,
                    "directPid": None,
                    "groupId": None,
                    "registryRegistered": False,
                    "registryCleared": False,
                    "groupAbsent": False,
                }],
            }
            sidecar_path.write_text(json.dumps(sidecar) + "\n", encoding="utf-8")
            os.chmod(sidecar_path, 0o600)
            record = OWNER.validate_sidecar_value(
                sidecar_path.read_bytes(), sidecar_path, registry_path, tmp_root, os.getpid()
            )
            self.assertEqual(record["namespace"], "local")

    def test_exact_ops04_and_ops13_custody_prefixes_are_supported(self) -> None:
        prefixes = (
            "ah11-jev-sigterm-",
            "ah11-jev-fixture-failure-",
            "ah11-jev-marker-missing-",
            "ah11-jev-marker-partial-",
            "ah11-jev-owned-group-finalizer-",
        )
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            tmp_root = base / "tmp"
            registry_root = base / "registry"
            tmp_root.mkdir(mode=0o700)
            registry_root.mkdir(mode=0o700)
            os.chmod(tmp_root, 0o700)
            os.chmod(registry_root, 0o700)
            registry_path = registry_root / "owned-groups.json"
            private_file(registry_path, json.dumps({
                "schema": OWNER.REGISTRY_SCHEMA, "groups": [], "pendingLaunches": [],
            }).encode())
            for index, prefix in enumerate(prefixes, start=1):
                directory = tmp_root / f"{prefix}fixture"
                directory.mkdir(mode=0o700)
                os.chmod(directory, 0o700)
                sidecar_path = registry_root / (
                    f"owned-groups.json.temp-custody.{os.getpid()}."
                    f"{hashlib.sha256(str(directory).encode()).hexdigest()[:16]}.json"
                )
                value = custody_sidecar(
                    directory, f"00000000-0000-4000-8000-{index:012x}"
                )
                record = OWNER.validate_sidecar_value(
                    json.dumps(value).encode(), sidecar_path, registry_path,
                    tmp_root, os.getpid(),
                )
                self.assertEqual(record["directory"], directory)
                self.assertEqual(record["namespace"], "outer")
            retry_dir = tmp_root / "ah11-ops-temp-custody-retry-fixture"
            retry_dir.mkdir(mode=0o700)
            os.chmod(retry_dir, 0o700)
            local_registry_path = retry_dir / "owned-groups.json"
            private_file(local_registry_path, json.dumps({
                "schema": OWNER.REGISTRY_SCHEMA, "groups": [], "pendingLaunches": [],
            }).encode())
            local_sidecar_path = retry_dir / (
                f"owned-groups.json.temp-custody.{os.getpid()}."
                f"{hashlib.sha256(str(retry_dir).encode()).hexdigest()[:16]}.json"
            )
            local_record = OWNER.validate_sidecar_value(
                json.dumps(custody_sidecar(
                    retry_dir, "00000000-0000-4000-8000-000000000006",
                )).encode(), local_sidecar_path, local_registry_path,
                tmp_root, os.getpid(),
            )
            self.assertEqual(local_record["namespace"], "local")
            unsupported = tmp_root / "ah11-unrelated-custody-fixture"
            unsupported.mkdir(mode=0o700)
            os.chmod(unsupported, 0o700)
            tag = hashlib.sha256(str(unsupported).encode()).hexdigest()[:16]
            rejected_path = registry_root / (
                f"owned-groups.json.temp-custody.{os.getpid()}.{tag}.json"
            )
            with self.assertRaises(OWNER.OwnerFailure):
                OWNER.validate_sidecar_value(
                    json.dumps(custody_sidecar(
                        unsupported, "00000000-0000-4000-8000-000000000099",
                    )).encode(), rejected_path, registry_path, tmp_root, os.getpid(),
                )
            misplaced_retry = tmp_root / "ah11-ops-temp-custody-retry-outer"
            misplaced_retry.mkdir(mode=0o700)
            os.chmod(misplaced_retry, 0o700)
            misplaced_tag = hashlib.sha256(str(misplaced_retry).encode()).hexdigest()[:16]
            misplaced_path = registry_root / (
                f"owned-groups.json.temp-custody.{os.getpid()}.{misplaced_tag}.json"
            )
            with self.assertRaises(OWNER.OwnerFailure):
                OWNER.validate_sidecar_value(
                    json.dumps(custody_sidecar(
                        misplaced_retry, "00000000-0000-4000-8000-000000000007",
                    )).encode(), misplaced_path, registry_path, tmp_root, os.getpid(),
                )

    def test_sidecar_rejects_retry_path_escape(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            tmp_root = Path(temporary)
            outside = tmp_root.parent / f"{tmp_root.name}-outside"
            outside.mkdir(mode=0o700)
            try:
                registry_dir = tmp_root / "registry"
                registry_dir.mkdir(mode=0o700)
                registry_path = registry_dir / "owned-groups.json"
                registry_path.write_text(
                    json.dumps({"schema": OWNER.REGISTRY_SCHEMA, "groups": [], "pendingLaunches": []}),
                    encoding="utf-8",
                )
                os.chmod(registry_path, 0o600)
                tag = hashlib.sha256(str(outside).encode("utf-8")).hexdigest()[:16]
                sidecar_path = registry_dir / (
                    f"owned-groups.json.temp-custody.{os.getpid()}.{tag}.json"
                )
                value = {
                    "schema": OWNER.SIDECAR_SCHEMA,
                    "ownerPid": os.getpid(),
                    "directory": str(outside),
                    "launches": [{
                        "launchToken": "00000000-0000-4000-8000-000000000002",
                        "status": "launch_intent",
                        "spawnAttempted": False,
                        "directPid": None,
                        "groupId": None,
                        "registryRegistered": False,
                        "registryCleared": False,
                        "groupAbsent": False,
                    }],
                }
                with self.assertRaises(OWNER.OwnerFailure):
                    OWNER.validate_sidecar_value(
                        json.dumps(value).encode(), sidecar_path, registry_path, tmp_root, os.getpid()
                    )
            finally:
                outside.rmdir()

    def test_unregistered_group_needs_durable_absence_and_registry_proof(self) -> None:
        row = {
            "launchToken": "00000000-0000-4000-8000-000000000003",
            "spawnAttempted": True,
            "directPid": 1234,
            "groupId": 1234,
            "registryRegistered": False,
            "registryCleared": False,
            "groupAbsent": False,
        }
        record = {"namespace": "outer", "launches": [row]}
        registry_result = {"empty": True, "groupsAbsent": set()}
        original_registry = {"groups": [], "pendingLaunches": []}
        self.assertFalse(OWNER.sidecar_rows_resolved(
            [record], "outer", registry_result, original_registry, set()
        ))
        row["registryCleared"] = True
        row["groupAbsent"] = True
        # Historical flags alone cannot clear this row; an independent probe
        # must establish current absence before the directory is removable.
        self.assertFalse(OWNER.sidecar_rows_resolved(
            [record], "outer", registry_result, original_registry, set()
        ))
        self.assertTrue(OWNER.sidecar_rows_resolved(
            [record], "outer", registry_result, original_registry, {1234}
        ))

    def test_cleared_sidecar_groups_are_probed_without_signal_authority(self) -> None:
        row = {"spawnAttempted": True, "groupId": 5678}
        records = [{"launches": [row]}]
        counters = {"sidecarGroupProbes": 0, "boundExceeded": 0}
        budget = OWNER.WaitBudget()
        with (patch.object(OWNER, "wait_groups", return_value={5678: False}) as wait_groups,
              patch.object(OWNER, "signal_groups") as signal_groups,
              patch.object(OWNER, "cleanup_groups") as cleanup_groups):
            self.assertEqual(OWNER.probe_sidecar_groups(records, counters, budget), {5678})
        wait_groups.assert_called_once_with([5678], OWNER.SIDECAR_GROUP_PROBE_MS, budget)
        signal_groups.assert_not_called()
        cleanup_groups.assert_not_called()
        self.assertEqual(counters["sidecarGroupProbes"], 1)

        for state in (True, None):
            counters = {"sidecarGroupProbes": 0, "boundExceeded": 0}
            with patch.object(OWNER, "wait_groups", return_value={5678: state}):
                self.assertIsNone(OWNER.probe_sidecar_groups(records, counters, budget))

    def test_wait_file_reads_durable_result_after_producer_exit(self) -> None:
        pid = 54321
        result = valid_watchdog_result(pid, None)
        with tempfile.TemporaryDirectory() as temporary:
            result_path = Path(temporary) / "watchdog-result.json"
            private_file(result_path, json.dumps(result).encode())
            self.assertTrue(OWNER.wait_file(result_path, ExitedProcess(), 4_000))
            decoded = OWNER.read_small_json(result_path)
            self.assertTrue(OWNER.validate_watchdog_result(decoded, pid, None))

            missing = Path(temporary) / "missing.json"
            self.assertFalse(OWNER.wait_file(missing, ExitedProcess(), 4_000))

            private_file(result_path, b"{}")
            self.assertTrue(OWNER.wait_file(result_path, ExitedProcess(), 4_000))
            self.assertFalse(OWNER.validate_watchdog_result(
                OWNER.read_small_json(result_path), pid, None
            ))

    def test_watchdog_keeps_acknowledged_target_after_leader_exit(self) -> None:
        for descendant_survives in (False, True):
            identity_calls: list[int] = []
            state: dict[str, object] = {
                "target": None,
                "ack_sent": False,
                "ack_path": "ack",
                "group_identity": lambda pgid: identity_calls.append(pgid) or True,
                "atomic_write": lambda _path, _value: True,
            }
            exec(OWNER.WATCHDOG_TARGET_STATE_SOURCE, state)
            self.assertTrue(state["running_control_transition"](43210))
            # The leader may be gone while the recorded group is absent or a
            # descendant remains; neither state replaces the acknowledged PGID.
            state["group_identity"] = lambda _pgid: False if not descendant_survives else True
            self.assertTrue(state["running_control_transition"](43210))
            self.assertTrue(state["terminal_control_matches"](43210))
            self.assertFalse(state["running_control_transition"](43211))
            self.assertFalse(state["terminal_control_matches"](43211))
            self.assertEqual(identity_calls, [43210])

    def test_capture_detects_one_over_byte_while_pipe_remains_open(self) -> None:
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(OWNER, "STREAM_MAX_BYTES", 4), \
                patch.object(OWNER, "COMBINED_STREAM_MAX_BYTES", 8):
            output = Path(temporary) / "stdout.raw"
            private_file(output)
            capture = OWNER.Capture({"stdout": output})
            OWNER.Capture._drain(capture, "stdout", FakePipe([b"1234"]))
            self.assertFalse(capture.overflow.is_set())
            self.assertEqual(output.read_bytes(), b"1234")

            private_file(output)
            capture = OWNER.Capture({"stdout": output})
            pipe = FakePipe([b"1234", b"5"], hold_open_after_chunks=True)
            thread = threading.Thread(target=capture._drain, args=("stdout", pipe))

            def assert_overflow_while_open() -> None:
                self.assertTrue(pipe.last_chunk_returned.wait(0.5))
                self.assertTrue(capture.overflow.wait(0.5))
                self.assertTrue(thread.is_alive())
                self.assertEqual(output.read_bytes(), b"1234")

            run_fake_capture_assertions(thread, pipe, assert_overflow_while_open)
            self.assertFalse(thread.is_alive())

            private_file(output)
            forced_pipe = FakePipe([b"1234", b"5"], hold_open_after_chunks=True)
            forced_capture = OWNER.Capture({"stdout": output})
            forced_thread = threading.Thread(
                target=forced_capture._drain, args=("stdout", forced_pipe)
            )

            def force_assertion_error() -> None:
                self.assertTrue(forced_pipe.last_chunk_returned.wait(0.5))
                raise AssertionError("forced fake assertion before normal release")

            with self.assertRaisesRegex(AssertionError, "forced fake assertion"):
                run_fake_capture_assertions(
                    forced_thread, forced_pipe, force_assertion_error
                )
            self.assertFalse(forced_thread.is_alive())

    def test_capture_combined_cap_and_read_write_errors_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(OWNER, "STREAM_MAX_BYTES", 4), \
                patch.object(OWNER, "COMBINED_STREAM_MAX_BYTES", 6):
            stdout = Path(temporary) / "stdout.raw"
            stderr = Path(temporary) / "stderr.raw"
            private_file(stdout)
            private_file(stderr)
            capture = OWNER.Capture({"stdout": stdout, "stderr": stderr})
            capture._drain("stdout", FakePipe([b"1234"]))
            capture._drain("stderr", FakePipe([b"xyz"]))
            self.assertTrue(capture.overflow.is_set())
            self.assertEqual(stdout.read_bytes(), b"1234")
            self.assertEqual(stderr.read_bytes(), b"xy")

            read_failure = OWNER.Capture({"stdout": stdout, "stderr": stderr})
            read_failure._drain("stdout", FakePipe([], fail_read=True))
            self.assertTrue(read_failure.read_failed.is_set())

            write_failure = OWNER.Capture({"stdout": stdout, "stderr": stderr})
            with patch.object(OWNER.os, "write", side_effect=OSError("fake write failure")):
                write_failure._drain("stdout", FakePipe([b"x"]))
            self.assertTrue(write_failure.read_failed.is_set())

    def test_shared_wait_budget_matches_accepted_specified_waits(self) -> None:
        target_ms = OWNER.TERM_WAIT_MS + OWNER.KILL_WAIT_MS + OWNER.DIRECT_REAP_MS + OWNER.TARGET_RECHECK_MS
        registry_ms = OWNER.TERM_WAIT_MS + OWNER.KILL_WAIT_MS + OWNER.TARGET_RECHECK_MS
        watchdog_ms = sum(OWNER.WATCHDOG_STOP_SEQUENCE_MS)
        sidecar_ms = OWNER.SIDECAR_GROUP_PROBE_MAX * OWNER.SIDECAR_GROUP_PROBE_MS
        self.assertEqual((target_ms, registry_ms, watchdog_ms, sidecar_ms),
                         (2_250, 1_750, 9_600, 4_800))
        self.assertEqual(target_ms + registry_ms + watchdog_ms + sidecar_ms,
                         OWNER.POST_COMMAND_WAIT_MAX_MS)
        budget = OWNER.WaitBudget()
        self.assertEqual(budget.reserve(target_ms), (target_ms, False))
        with patch.object(OWNER.time, "monotonic", return_value=10.250):
            budget.settle(target_ms, 10.0, False, True)
        self.assertEqual(budget.used_ms, 250)
        self.assertEqual(budget.remaining_ms, OWNER.POST_COMMAND_WAIT_MAX_MS - 250)

    def test_registry_namespaces_share_one_sixteen_group_cleanup_batch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            outer_path = base / "outer" / "owned-groups.json"
            local_path = base / "local" / "owned-groups.json"
            outer_path.parent.mkdir()
            local_path.parent.mkdir()
            outer = OWNER.validate_registry_value({
                "schema": OWNER.REGISTRY_SCHEMA, "groups": [111, 222], "pendingLaunches": [],
            })
            local = OWNER.validate_registry_value({
                "schema": OWNER.REGISTRY_SCHEMA, "groups": [333], "pendingLaunches": [],
            })
            private_file(outer_path, json.dumps(outer).encode())
            private_file(local_path, json.dumps(local).encode())
            contexts = [("outer", outer_path, outer), ("local", local_path, local)]
            budget = OWNER.WaitBudget()
            cleanup_result = {
                "groups": [111, 222, 333], "final": {111: False, 222: False, 333: False},
                "term": {}, "kill": {}, "allAbsent": True,
            }
            with (patch.object(OWNER, "cleanup_groups", return_value=cleanup_result) as cleanup_groups,
                  patch.object(OWNER, "wait_groups", return_value={111: False, 222: False, 333: False}) as wait_groups):
                result = OWNER.reconcile_registries(contexts, [], set(), budget)
            self.assertTrue(result["ok"])
            self.assertEqual(result["groupsObserved"], 3)
            cleanup_groups.assert_called_once_with([111, 222, 333], budget=budget)
            wait_groups.assert_called_once_with([111, 222, 333], OWNER.TARGET_RECHECK_MS, budget)
            self.assertEqual(OWNER.read_registry(outer_path)["groups"], [])
            self.assertEqual(OWNER.read_registry(local_path)["groups"], [])

    def test_combined_registry_authority_rejects_more_than_sixteen_groups(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            outer_path = base / "outer" / "owned-groups.json"
            local_path = base / "local" / "owned-groups.json"
            outer_path.parent.mkdir()
            local_path.parent.mkdir()
            outer = OWNER.validate_registry_value({
                "schema": OWNER.REGISTRY_SCHEMA, "groups": list(range(1000, 1016)),
                "pendingLaunches": [],
            })
            local = OWNER.validate_registry_value({
                "schema": OWNER.REGISTRY_SCHEMA, "groups": [2000], "pendingLaunches": [],
            })
            private_file(outer_path, json.dumps(outer).encode())
            private_file(local_path, json.dumps(local).encode())
            with patch.object(OWNER, "cleanup_groups") as cleanup_groups:
                result = OWNER.reconcile_registries(
                    [("outer", outer_path, outer), ("local", local_path, local)],
                    [], set(), OWNER.WaitBudget(),
                )
            self.assertFalse(result["ok"])
            cleanup_groups.assert_not_called()

    def test_registry_persistence_failure_never_reports_reconciliation_success(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            outer_path = base / "outer" / "owned-groups.json"
            local_path = base / "local" / "owned-groups.json"
            outer_path.parent.mkdir()
            local_path.parent.mkdir()
            outer = OWNER.validate_registry_value({
                "schema": OWNER.REGISTRY_SCHEMA, "groups": [111], "pendingLaunches": [],
            })
            local = OWNER.validate_registry_value({
                "schema": OWNER.REGISTRY_SCHEMA, "groups": [222], "pendingLaunches": [],
            })
            private_file(outer_path, json.dumps(outer).encode())
            private_file(local_path, json.dumps(local).encode())
            original_write = OWNER.write_registry_state

            def fail_local_write(path, registry, groups, pending):
                if path == local_path:
                    return False
                return original_write(path, registry, groups, pending)

            cleanup_result = {
                "groups": [111, 222], "final": {111: False, 222: False},
                "term": {}, "kill": {}, "allAbsent": True,
            }
            with (patch.object(OWNER, "cleanup_groups", return_value=cleanup_result),
                  patch.object(OWNER, "wait_groups", return_value={111: False, 222: False}),
                  patch.object(OWNER, "write_registry_state", side_effect=fail_local_write)):
                result = OWNER.reconcile_registries(
                    [("outer", outer_path, outer), ("local", local_path, local)],
                    [], set(), OWNER.WaitBudget(),
                )
            self.assertFalse(result["ok"])
            self.assertFalse(result["outerEmpty"])
            self.assertEqual(OWNER.read_registry(local_path)["groups"], [222])

    def test_registry_scan_counter_is_shared_across_namespaces(self) -> None:
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(OWNER, "REGISTRY_DIRECTORY_ENTRY_MAX", 2):
            base = Path(temporary)
            first = base / "first"
            second = base / "second"
            first.mkdir()
            second.mkdir()
            private_file(first / "one")
            private_file(first / "two")
            private_file(second / "three")
            counters = {"registryEntries": 0, "boundExceeded": 0}
            self.assertEqual(len(OWNER.list_entries(first, counters)), 2)
            self.assertIsNone(OWNER.list_entries(second, counters))
            self.assertEqual(counters["registryEntries"], 3)
            self.assertEqual(counters["boundExceeded"], 1)

    def test_reaped_command_leader_still_cleans_live_descendant_group_once(self) -> None:
        pgid = 43210
        cleanup_result = {
            "term": {pgid: "sent"},
            "kill": {},
            "allAbsent": True,
        }
        budget = OWNER.WaitBudget()
        with (
            patch.object(OWNER, "group_exists", return_value=True) as group_exists,
            patch.object(OWNER, "cleanup_groups", return_value=cleanup_result) as cleanup_groups,
            patch.object(OWNER, "wait_groups", return_value={pgid: False}) as wait_groups,
        ):
            (command_exit, command_reaped, group_absent, capture_drained,
             escalation) = OWNER.cleanup_target_group(
                pgid, None, 0, True, budget=budget,
            )

        self.assertEqual((command_exit, command_reaped, group_absent, escalation),
                         (0, True, True, "term"))
        self.assertTrue(capture_drained)
        group_exists.assert_called_once_with(pgid)
        cleanup_groups.assert_called_once_with(
            [pgid], OWNER.TERM_WAIT_MS, OWNER.KILL_WAIT_MS, budget
        )
        wait_groups.assert_called_once_with([pgid], OWNER.TARGET_RECHECK_MS, budget)
        bounded_wait_ms = (OWNER.TERM_WAIT_MS + OWNER.KILL_WAIT_MS
                           + OWNER.DIRECT_REAP_MS + OWNER.TARGET_RECHECK_MS)
        self.assertEqual(bounded_wait_ms, 2_250)

    def test_watchdog_owned_cleanup_is_never_signalled_twice(self) -> None:
        pgid = 43210
        with (
            patch.object(OWNER, "group_exists", return_value=True) as group_exists,
            patch.object(OWNER, "cleanup_groups") as cleanup_groups,
            patch.object(OWNER, "wait_groups", return_value={pgid: True}),
        ):
            result = OWNER.cleanup_target_group(
                pgid, None, 0, True, cleanup_attempted=True,
            )
        self.assertEqual(result, (0, True, False, True, "unconfirmed"))
        group_exists.assert_called_once_with(pgid)
        cleanup_groups.assert_not_called()

    def test_cleanup_failure_keeps_the_original_command_status(self) -> None:
        pgid = 43210
        with (
            patch.object(OWNER, "group_exists", return_value=True),
            patch.object(OWNER, "cleanup_groups", return_value={
                "term": {}, "kill": {}, "allAbsent": False,
            }),
            patch.object(OWNER, "wait_groups", return_value={pgid: True}),
        ):
            result = OWNER.cleanup_target_group(pgid, None, 37, True)
        self.assertEqual(result, (37, True, False, True, "unconfirmed"))


if __name__ == "__main__":
    unittest.main()
