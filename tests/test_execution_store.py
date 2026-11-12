import os
from contextlib import closing
from pathlib import Path
import json
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

from runpod_experiments.contracts import (
    ApprovalScope, AttemptPlan, BillingObservation, ExecutionPlan,
    FailureCategory, ManifestFile, ProjectPolicy, ResourceKind,
    ResourceProfile, RetryPolicy, TransferDirection, TransferManifest,
    encode_document, sha256_bytes,
)
from runpod_experiments.errors import ExecutionError
from runpod_experiments.execution_store import ExecutionStore
from runpod_experiments.ownership import request_delete_for_store_attempt
from runpod_experiments.provider import ProviderAdapter
from runpod_experiments.runpod import ProviderError
from runpod_experiments.resource_policy import resource_profile_violation
from runpod_experiments.state_config import StateStoreConfig
from runpod_experiments.supervision import StoreCleanupMonitor, run_supervisor
from runpod_experiments.supervisor_process import launch_execution_store


ZERO = "0" * 64
ONE = "1" * 64
SOURCE = "a" * 64
CONFIG = "b" * 64
PACKAGE = "c" * 64
COMMIT = "d" * 40


def plan_for(now: int, *, max_total: int = 1_000_000, max_slots: int = 1,
             max_hourly: int = 700_000, hourly_rate: int = 300_000) -> ExecutionPlan:
    project = ProjectPolicy("synthetic-nebula", ("nebula-",), ZERO,
                            ("TEST-HOST",), max_active_resources=max_slots)
    approval = ApprovalScope(
        "approval-one", project.project_id, "TEST-HOST", ZERO,
        sha256_bytes(encode_document("project_policy", project)), ONE, "2" * 64,
        max_total, max_hourly, 3600, 3, now + 7200, max_slots)
    resource = ResourceProfile(
        ResourceKind.CPU, "cpu-small", hourly_rate, ("EU-TEST-1",),
        "sha256:" + ZERO, 20, 4, 8)
    return ExecutionPlan(
        "research-job", project, approval, COMMIT, SOURCE, CONFIG, PACKAGE,
        (resource,), TransferManifest((), 1024, 4096),
        RetryPolicy(3, 3600, (FailureCategory.TRANSIENT_READ,
                               FailureCategory.VERIFIED_CAPACITY_REJECTION)))


def attempt_for(plan: ExecutionPlan, attempt_id: str, number: int, now: int) -> AttemptPlan:
    return AttemptPlan(
        attempt_id, plan.job_id, plan.approval.approval_id, number,
        ZERO, ONE, sha256_bytes(encode_document("project_policy", plan.project)),
        plan.source_sha256, plan.config_sha256, plan.package_sha256,
        plan.resource_options[0], plan.resource_options[0].region_ids[0],
        1200, 60, now - 1, now + 1800)


def pod_name(number: int) -> str:
    return f"nebula-run-{number}"


class ExecutionStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="execution-store-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = int(time.time())
        self.plan = plan_for(self.now)
        self.path = self.root / "execution.sqlite"
        self.store = ExecutionStore(str(self.path), project_id=self.plan.project.project_id,
                                    controller_host="TEST-HOST", clock=lambda: self.now)

    def reserve(self, number=1, *, plan=None):
        plan = plan or self.plan
        attempt = attempt_for(plan, f"attempt-{number}", number, self.now)
        exposure = self.store.reserve_attempt(plan, attempt, pod_name(number))
        return attempt, exposure

    def activate_and_terminate(self, attempt, number=1):
        self.store.claim_create(attempt.attempt_id)
        self.store.bind_pod(attempt.attempt_id, f"pod-{number}", pod_name(number))
        self.store.request_stop(attempt.attempt_id)
        self.store.confirm_termination(attempt.attempt_id, f"pod-{number}", ONE)

    def test_reservation_is_transactional_persistent_and_hash_chained(self):
        attempt, exposure = self.reserve()
        self.assertEqual(exposure, 105_000)
        snapshot = self.store.accounting_snapshot(self.plan.project.project_id,
                                                  self.plan.approval.approval_id)
        self.assertEqual(snapshot.reserved_micro_usd, exposure)
        self.assertEqual(snapshot.active_resources, 1)
        self.assertEqual(snapshot.active_hourly_micro_usd, 300_000)
        self.assertEqual(self.store.get_attempt(attempt.attempt_id)["state"], "reserved")
        self.assertEqual(self.store.reserve_attempt(self.plan, attempt, pod_name(1)), exposure)
        self.assertEqual([event["event_type"] for event in self.store.events()], ["reserved"])

        reopened = ExecutionStore(str(self.path), project_id=self.plan.project.project_id,
                                  controller_host="TEST-HOST", clock=lambda: self.now)
        self.assertEqual(reopened.get_attempt(attempt.attempt_id)["attempt_sha256"],
                         self.store.get_attempt(attempt.attempt_id)["attempt_sha256"])
        self.assertTrue(reopened.verify_event_chain())
        self.assertEqual([event["event_type"] for event in reopened.events()], ["reserved"])

    def test_create_claim_and_lifecycle_transitions_preserve_identity(self):
        attempt, _ = self.reserve()
        self.assertTrue(self.store.verify_attempt_lifecycle_projection(attempt.attempt_id))
        self.store.claim_create(attempt.attempt_id)
        self.assertTrue(self.store.verify_attempt_lifecycle_projection(attempt.attempt_id))
        reopened = ExecutionStore(str(self.path), project_id=self.plan.project.project_id,
                                  controller_host="TEST-HOST", clock=lambda: self.now)
        claimed = reopened.get_attempt(attempt.attempt_id)
        self.assertEqual(claimed["state"], "create_claimed")
        self.assertEqual(claimed["create_claimed_epoch"], self.now)
        with self.assertRaises(ExecutionError):
            reopened.claim_create(attempt.attempt_id)
        with self.assertRaises(ExecutionError):
            self.store.claim_create(attempt.attempt_id)
        self.store.require_reconciliation(attempt.attempt_id, "create_timeout")
        self.assertTrue(self.store.verify_attempt_lifecycle_projection(attempt.attempt_id))
        prior_event_count = len(self.store.events())
        self.store.require_reconciliation(attempt.attempt_id, "create_timeout")
        self.assertEqual(len(self.store.events()), prior_event_count)
        with self.assertRaises(ExecutionError):
            self.store.bind_pod(attempt.attempt_id, "pod-1", pod_name(2))
        self.store.bind_pod(attempt.attempt_id, "pod-1", pod_name(1))
        self.assertTrue(self.store.verify_attempt_lifecycle_projection(attempt.attempt_id))
        self.assertEqual(self.store.get_attempt(attempt.attempt_id)["state"], "active")
        reopened = ExecutionStore(str(self.path), project_id=self.plan.project.project_id,
                                  controller_host="TEST-HOST", clock=lambda: self.now)
        self.assertEqual(reopened.get_attempt(attempt.attempt_id)["pod_id"], "pod-1")
        with self.assertRaises(ExecutionError):
            self.store.confirm_termination(attempt.attempt_id, "pod-2", ONE)
        self.store.request_stop(attempt.attempt_id)
        self.assertTrue(self.store.verify_attempt_lifecycle_projection(attempt.attempt_id))
        prior_event_count = len(self.store.events())
        self.store.request_stop(attempt.attempt_id)
        self.assertEqual(len(self.store.events()), prior_event_count)
        reopened = ExecutionStore(str(self.path), project_id=self.plan.project.project_id,
                                  controller_host="TEST-HOST", clock=lambda: self.now)
        self.assertEqual(reopened.get_attempt(attempt.attempt_id)["state"], "stopping")
        self.store.confirm_termination(attempt.attempt_id, "pod-1", ONE)
        self.assertTrue(self.store.verify_attempt_lifecycle_projection(attempt.attempt_id))
        prior_event_count = len(self.store.events())
        self.store.confirm_termination(attempt.attempt_id, "pod-1", ONE)
        self.assertEqual(len(self.store.events()), prior_event_count)
        record = self.store.get_attempt(attempt.attempt_id)
        self.assertEqual(record["state"], "terminated")
        self.assertEqual(record["exposure_micro_usd"], 105_000)
        reopened = ExecutionStore(str(self.path), project_id=self.plan.project.project_id,
                                  controller_host="TEST-HOST", clock=lambda: self.now)
        self.assertEqual(reopened.get_attempt(attempt.attempt_id)["state"], "terminated")
        self.assertEqual(reopened.get_attempt(attempt.attempt_id)["terminated_epoch"], self.now)
        self.assertTrue(self.store.verify_event_chain())

    def test_attempt_integrity_rejects_mutated_pod_identity_projection(self):
        attempt, _ = self.reserve()
        self.activate_and_terminate(attempt)
        self.assertTrue(self.store.verify_attempt_lifecycle_projection(attempt.attempt_id))
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("UPDATE execution_attempts SET pod_id=? WHERE attempt_id=?",
                       ("pod-forged", attempt.attempt_id))
            db.commit()
        with self.assertRaises(ExecutionError) as caught:
            self.store.verify_attempt_lifecycle_projection(attempt.attempt_id)
        self.assertEqual(caught.exception.code, "attempt_projection_mismatch")
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("UPDATE execution_attempts SET pod_id=?,state=? WHERE attempt_id=?",
                       ("pod-1", "active", attempt.attempt_id))
            db.commit()
        with self.assertRaises(ExecutionError) as caught:
            self.store.verify_attempt_lifecycle_projection(attempt.attempt_id)
        self.assertEqual(caught.exception.code, "attempt_projection_mismatch")

    def test_store_backed_delete_requires_stopping_state_and_exact_live_identity(self):
        attempt, _ = self.reserve()
        self.assertEqual(self.store.project_policy_for_attempt(attempt.attempt_id),
                         self.plan.project)
        self.store.claim_create(attempt.attempt_id)
        self.store.bind_pod(attempt.attempt_id, "pod-1", pod_name(1))

        class Provider:
            def __init__(self):
                self.detail = {"id": "pod-1", "name": "nebula-other", "status": "RUNNING"}
                self.delete_calls = []
                self.delete_error = None

            def pod(self, pod_id):
                if self.detail["id"] != pod_id:
                    raise ProviderError("missing", status=404)
                return dict(self.detail)

            def _delete_pod(self, pod_id):
                self.delete_calls.append(pod_id)
                if self.delete_error is not None:
                    raise self.delete_error

        provider = Provider()
        with self.assertRaises(ExecutionError) as caught:
            request_delete_for_store_attempt(
                provider, self.store, attempt.attempt_id,
                self.plan.project.project_id)
        self.assertEqual(caught.exception.code, "attempt_not_stoppable")
        self.store.request_stop(attempt.attempt_id)
        with self.assertRaises(ExecutionError) as caught:
            request_delete_for_store_attempt(
                provider, self.store, attempt.attempt_id,
                self.plan.project.project_id)
        self.assertEqual(caught.exception.code, "attempt_identity_mismatch")
        self.assertEqual(provider.delete_calls, [])

        provider.detail["name"] = pod_name(1)
        self.assertEqual(request_delete_for_store_attempt(
            provider, self.store, attempt.attempt_id,
            self.plan.project.project_id), "delete_requested")
        self.assertEqual(provider.delete_calls, ["pod-1"])
        self.assertEqual(self.store.get_attempt(attempt.attempt_id)["state"], "stopping")

        provider.detail["status"] = "TERMINATED"
        self.assertEqual(request_delete_for_store_attempt(
            provider, self.store, attempt.attempt_id,
            self.plan.project.project_id), "already_terminated")
        self.assertEqual(provider.delete_calls, ["pod-1"])

        provider.detail["status"] = "RUNNING"
        provider.delete_error = ProviderError("private provider text", status=503)
        with self.assertRaises(ExecutionError) as caught:
            request_delete_for_store_attempt(
                provider, self.store, attempt.attempt_id,
                self.plan.project.project_id)
        self.assertEqual(caught.exception.code, "http_503")
        self.assertEqual(caught.exception.category, FailureCategory.AMBIGUOUS_OPERATION)

    def test_store_cleanup_monitor_reconciles_exact_create_and_confirms_cleanup(self):
        class Provider:
            def __init__(self):
                self.inventory = []
                self.details = {}
                self.delete_calls = []

            def pods(self):
                return [dict(item) for item in self.inventory]

            def pod(self, pod_id):
                if pod_id not in self.details:
                    raise ProviderError("missing", status=404)
                return dict(self.details[pod_id])

            def _delete_pod(self, pod_id):
                self.delete_calls.append(pod_id)

        attempt, _ = self.reserve()
        self.store.claim_create(attempt.attempt_id)
        provider = Provider()
        live = {
            "id": "pod-1", "name": pod_name(1), "status": "RUNNING", "cost": 0.3,
            "cloud": "SECURE", "image": "runpod/base@" + self.plan.resource_options[0].image_digest,
            "disk": 20, "dataCenterId": "EU-TEST-1",
            "cpu": {"id": "cpu-small", "vcpuCount": 4, "memory": 8}, "mounts": {},
        }
        provider.inventory = [dict(live)]
        provider.details["pod-1"] = dict(live)
        monitor = StoreCleanupMonitor(
            provider, self.store, attempt.attempt_id,
            self.plan.project.project_id, clock=lambda: self.now)

        self.assertEqual(monitor.observe(), "active")
        self.assertEqual(self.store.get_attempt(attempt.attempt_id)["pod_id"], "pod-1")
        self.assertEqual(monitor.observe(stop=True), "delete_requested")
        self.assertEqual(provider.delete_calls, ["pod-1"])
        self.assertEqual(self.store.get_attempt(attempt.attempt_id)["state"], "stopping")

        provider.inventory = []
        provider.details["pod-1"]["status"] = "TERMINATED"
        self.assertEqual(monitor.observe(), "confirming_absence")
        self.assertEqual(monitor.observe(), "confirming_absence")
        self.assertEqual(monitor.observe(), "terminated")
        self.assertEqual(self.store.get_attempt(attempt.attempt_id)["state"], "terminated")
        self.assertTrue(self.store.verify_attempt_lifecycle_projection(attempt.attempt_id))

    def test_store_cleanup_monitor_deletes_exact_pod_when_resource_profile_is_violated(self):
        class Provider:
            def __init__(self, pod):
                self.pod_record = dict(pod)
                self.delete_calls = []

            def pods(self):
                return [dict(self.pod_record)]

            def pod(self, _pod_id):
                return dict(self.pod_record)

            def _delete_pod(self, pod_id):
                self.delete_calls.append(pod_id)

        attempt, _ = self.reserve()
        self.store.claim_create(attempt.attempt_id)
        live = {
            "id": "pod-1", "name": pod_name(1), "status": "RUNNING", "cost": 0.3,
            "cloud": "SECURE", "image": "runpod/base@" + self.plan.resource_options[0].image_digest,
            "disk": 20, "dataCenterId": "EU-TEST-1",
            "cpu": {"id": "cpu-other", "vcpuCount": 4, "memory": 8}, "mounts": {},
        }
        provider = Provider(live)
        monitor = StoreCleanupMonitor(
            provider, self.store, attempt.attempt_id,
            self.plan.project.project_id, clock=lambda: self.now)

        self.assertEqual(monitor.observe(), "delete_requested")
        self.assertEqual(monitor.stop_reason, "unexpected_resource_shape")
        self.assertEqual(provider.delete_calls, ["pod-1"])
        self.assertEqual(self.store.get_attempt(attempt.attempt_id)["state"], "stopping")

    def test_selected_resource_is_reconstructed_from_hash_bound_attempt_plan(self):
        attempt, _ = self.reserve()
        profile, region = self.store.selected_resource_for_attempt(attempt.attempt_id)
        self.assertEqual(profile, self.plan.resource_options[0])
        self.assertEqual(region, "EU-TEST-1")
        pod = {
            "status": "RUNNING", "cost": 0.3, "cloud": "SECURE",
            "image": "runpod/base@" + profile.image_digest, "disk": profile.disk_gb,
            "dataCenterId": region, "cpu": {"id": profile.resource_id,
                "vcpuCount": profile.cpu_count, "memory": profile.memory_gb}, "mounts": {},
        }
        self.assertIsNone(resource_profile_violation(pod, profile, region))
        pod["cost"] = 0.300001
        self.assertEqual(resource_profile_violation(pod, profile, region),
                         "invalid_or_excess_hourly_rate")

    def test_resource_profile_guard_checks_gpu_memory_and_identity(self):
        profile = ResourceProfile(
            ResourceKind.GPU, "NVIDIA A40", 600_000, ("EU-TEST-1",),
            "sha256:" + ZERO, 30, 8, 32, 1, 48, "12.4")
        pod = {
            "status": "RUNNING", "cost": 0.59, "cloud": "SECURE",
            "image": "runpod/gpu@" + profile.image_digest, "disk": 30,
            "dataCenterId": "EU-TEST-1",
            "gpu": {"id": "NVIDIA A40", "count": 1, "vcpuCount": 8, "memory": 48},
            "mounts": {},
        }
        self.assertEqual(resource_profile_violation(pod, profile, "EU-TEST-1"),
                         "unverifiable_gpu_profile")
        pod["gpu"]["memory"] = 40
        self.assertEqual(resource_profile_violation(pod, profile, "EU-TEST-1"),
                         "unexpected_resource_shape")

    def test_store_cleanup_monitor_keeps_ambiguous_create_held_until_three_absences(self):
        class Provider:
            def pods(self):
                return []

            def pod(self, pod_id):
                raise ProviderError("missing", status=404)

            def _delete_pod(self, pod_id):
                raise AssertionError("an unbound create must never be deleted")

        attempt, exposure = self.reserve()
        self.store.claim_create(attempt.attempt_id)
        monitor = StoreCleanupMonitor(
            Provider(), self.store, attempt.attempt_id,
            self.plan.project.project_id, clock=lambda: self.now)

        self.assertEqual(monitor.observe(), "unresolved")
        self.assertEqual(self.store.get_attempt(attempt.attempt_id)["exposure_micro_usd"], exposure)
        self.assertEqual(monitor.observe(stop=True), "confirming_absence")
        self.assertEqual(monitor.observe(), "confirming_absence")
        self.assertEqual(monitor.observe(), "not_created")
        record = self.store.get_attempt(attempt.attempt_id)
        self.assertEqual(record["state"], "cancelled")
        self.assertEqual(record["exposure_micro_usd"], 0)
        self.assertTrue(self.store.verify_attempt_lifecycle_projection(attempt.attempt_id))

    def test_store_cleanup_monitor_uses_independent_supervisor_loop(self):
        class Provider:
            def pods(self):
                return []

            def pod(self, pod_id):
                raise ProviderError("missing", status=404)

            def _delete_pod(self, pod_id):
                raise AssertionError("no Pod is bound")

        attempt, _ = self.reserve()
        monitor = StoreCleanupMonitor(
            Provider(), self.store, attempt.attempt_id,
            self.plan.project.project_id, clock=lambda: self.now)
        state = run_supervisor(
            monitor, self.root / "store-supervisor", self.now + 100,
            interval=1, clock=lambda: self.now, monotonic=lambda: 0,
            sleep=lambda _: None, max_polls=1)
        self.assertTrue(state["ready"])
        self.assertEqual(state["outcome"], "reserved")
        self.assertEqual(state["policy_sha256"], monitor.policy_sha256)

    def test_store_launcher_passes_only_state_identity_and_persisted_deadline(self):
        config = StateStoreConfig(self.root / "store-launch-state", "TEST-HOST")
        store = config.open_execution_store(self.plan.project.project_id, clock=lambda: self.now)
        attempt = attempt_for(self.plan, "attempt-launch", 1, self.now)
        store.reserve_attempt(self.plan, attempt, pod_name(1))
        captured = {}

        def popen(argv, **kwargs):
            captured.update(argv=argv, kwargs=kwargs)
            return "child-process"

        result = launch_execution_store(
            config, attempt.attempt_id, self.plan.project.project_id,
            self.root / "store-launch-control", popen=popen)
        command = " ".join(captured["argv"])
        self.assertEqual(result, "child-process")
        self.assertIn(str(config.state_root), command)
        self.assertIn("--controller-host TEST-HOST", command)
        self.assertIn(f"--deadline {attempt.deadline_epoch}", command)
        self.assertNotIn("apikey", command.lower())
        self.assertTrue(captured["kwargs"]["close_fds"])

    def test_real_detached_store_supervisor_survives_launcher_parent_exit(self):
        config = StateStoreConfig(self.root / "detached-state", "TEST-HOST")
        store = config.open_execution_store(self.plan.project.project_id, clock=lambda: self.now)
        attempt = attempt_for(self.plan, "attempt-parent-exit", 1, self.now)
        store.reserve_attempt(self.plan, attempt, pod_name(1))
        control = self.root / "detached-control"
        control.mkdir()
        (control / "stop.request").touch()
        config_home = self.root / "home"
        (config_home / ".runpod").mkdir(parents=True)
        (config_home / ".runpod" / "config.toml").write_text(
            'apikey = "synthetic-test-key"\n', encoding="utf-8")
        gate = self.root / "release-provider-read"
        sitecustomize = self.root / "sitecustomize.py"
        sitecustomize.write_text('''
import json
import os
import time
from pathlib import Path
import urllib.request

class _Response:
    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def read(self, _size=-1):
        return json.dumps({"pods": [], "pagination": {"hasNextPage": False}}).encode()

def _fake_urlopen(request, timeout=None):
    if request.get_method() != "GET" or request.full_url != "https://api.runpod.io/v2/pods":
        raise AssertionError("unexpected provider request in detached test")
    gate = Path(os.environ["RUNPOD_TEST_RELEASE_FILE"])
    deadline = time.monotonic() + 20
    while not gate.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    if not gate.exists():
        raise TimeoutError("parent did not release fake provider read")
    return _Response()

urllib.request.urlopen = _fake_urlopen
''', encoding="utf-8")
        package_parent = Path(__import__("runpod_experiments").__file__).resolve().parent.parent
        source = '''
import os
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from runpod_experiments.state_config import StateStoreConfig
from runpod_experiments.supervisor_process import launch_execution_store
root = Path(sys.argv[2]); state_root = Path(sys.argv[3]); control = Path(sys.argv[4])
project, attempt = sys.argv[5], sys.argv[6]
os.environ["RUNPOD_TEST_RELEASE_FILE"] = sys.argv[7]
child = launch_execution_store(StateStoreConfig(state_root, "TEST-HOST"), attempt,
                               project, control)
(root / "child.pid").write_text(str(child.pid), encoding="ascii")
'''
        env = dict(os.environ)
        env["HOME"] = str(config_home)
        env["USERPROFILE"] = str(config_home)
        env["PYTHONPATH"] = str(self.root) + os.pathsep + str(package_parent)
        env["RUNPOD_TEST_RELEASE_FILE"] = str(gate)
        process = subprocess.run(
            [sys.executable, "-c", source,
             str(package_parent), str(self.root),
             str(config.state_root), str(control), self.plan.project.project_id,
             attempt.attempt_id, str(gate)],
            env=env, capture_output=True, text=True, timeout=30)
        self.assertEqual(process.returncode, 0, process.stderr)
        child_pid = int((self.root / "child.pid").read_text(encoding="ascii"))
        # The fake provider read is blocked until after the launcher parent exits.
        gate.touch()
        state_path = control / "supervisor.json"
        deadline = time.monotonic() + 20
        while not state_path.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertTrue(state_path.exists(), (control / "supervisor-process.log").read_text(
            encoding="utf-8", errors="replace"))
        state = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual(state["pid"], child_pid)
        self.assertTrue(state["ready"])
        self.assertTrue(state["not_created"])
        self.assertEqual(store.get_attempt(attempt.attempt_id)["state"], "cancelled")
        if os.name == "nt":
            import ctypes
            from ctypes import wintypes
            kernel = ctypes.windll.kernel32
            kernel.OpenProcess.restype = wintypes.HANDLE
            kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
            kernel.CloseHandle.argtypes = [wintypes.HANDLE]
            handle = kernel.OpenProcess(0x00100000, False, child_pid)  # SYNCHRONIZE
            if handle:
                try:
                    self.assertEqual(kernel.WaitForSingleObject(handle, 10_000), 0)
                finally:
                    kernel.CloseHandle(handle)
            else:
                self.assertEqual(kernel.GetLastError(), 87)  # ERROR_INVALID_PARAMETER: already exited
        else:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                try:
                    os.kill(child_pid, 0)
                except ProcessLookupError:
                    break
                time.sleep(0.02)
            else:
                self.fail("detached supervisor did not exit after cancelling the reserved attempt")

    def test_cancel_releases_only_an_unclaimed_reservation(self):
        attempt, exposure = self.reserve()
        self.store.cancel_uncreated(attempt.attempt_id)
        self.assertTrue(self.store.verify_attempt_lifecycle_projection(attempt.attempt_id))
        prior_event_count = len(self.store.events())
        self.store.cancel_uncreated(attempt.attempt_id)
        self.assertEqual(len(self.store.events()), prior_event_count)
        self.assertEqual(self.store.get_attempt(attempt.attempt_id)["exposure_micro_usd"], 0)
        self.assertEqual(self.store.accounting_snapshot(
            self.plan.project.project_id, self.plan.approval.approval_id).reserved_micro_usd, 0)
        with self.assertRaises(ExecutionError):
            self.store.claim_create(attempt.attempt_id)
        self.assertGreater(exposure, 0)

    def test_ambiguous_create_absence_releases_hold_only_after_explicit_reconciliation(self):
        attempt, _ = self.reserve()
        self.store.claim_create(attempt.attempt_id)
        with self.assertRaises(ExecutionError):
            self.store.confirm_absence(attempt.attempt_id, ONE)
        self.store.require_reconciliation(attempt.attempt_id, "create_timeout")
        self.assertTrue(self.store.verify_attempt_lifecycle_projection(attempt.attempt_id))
        self.store.confirm_absence(attempt.attempt_id, ONE)
        self.assertTrue(self.store.verify_attempt_lifecycle_projection(attempt.attempt_id))
        prior_event_count = len(self.store.events())
        self.store.confirm_absence(attempt.attempt_id, ONE)
        self.assertEqual(len(self.store.events()), prior_event_count)
        record = self.store.get_attempt(attempt.attempt_id)
        self.assertEqual(record["state"], "cancelled")
        self.assertEqual(record["exposure_micro_usd"], 0)
        self.assertEqual(record["billing_complete"], 1)

    def test_ambiguous_stop_can_reconcile_present_then_confirm_absence(self):
        another, exposure = self.reserve()
        self.store.claim_create(another.attempt_id)
        self.store.bind_pod(another.attempt_id, "pod-1", pod_name(1))
        self.store.request_stop(another.attempt_id)
        self.store.require_reconciliation(another.attempt_id, "delete_timeout")
        self.assertTrue(self.store.verify_attempt_lifecycle_projection(another.attempt_id))
        self.store.bind_pod(another.attempt_id, "pod-1", pod_name(1))
        self.assertTrue(self.store.verify_attempt_lifecycle_projection(another.attempt_id))
        self.assertEqual(self.store.get_attempt(another.attempt_id)["state"], "active")
        self.store.request_stop(another.attempt_id)
        self.store.require_reconciliation(another.attempt_id, "delete_timeout")
        self.store.confirm_absence(another.attempt_id, "2" * 64)
        self.assertTrue(self.store.verify_attempt_lifecycle_projection(another.attempt_id))
        record = self.store.get_attempt(another.attempt_id)
        self.assertEqual(record["state"], "terminated")
        self.assertEqual(record["exposure_micro_usd"], exposure)
        self.store.mark_billing_complete(another.attempt_id, "3" * 64,
                                         self.now - 1, self.now + 1800)

    def test_total_cap_and_project_slots_are_checked_before_admission(self):
        self.reserve()
        second = attempt_for(self.plan, "attempt-2", 2, self.now)
        with self.assertRaises(ExecutionError) as caught:
            self.store.reserve_attempt(self.plan, second, pod_name(2))
        self.assertEqual(caught.exception.code, "project_slots_exhausted")

        two_slots = plan_for(self.now, max_total=150_000, max_slots=2)
        second_store = ExecutionStore(str(self.root / "small-cap.sqlite"),
                                      project_id=two_slots.project.project_id,
                                      controller_host="TEST-HOST", clock=lambda: self.now)
        first = attempt_for(two_slots, "budget-one", 1, self.now)
        second = attempt_for(two_slots, "budget-two", 2, self.now)
        second_store.reserve_attempt(two_slots, first, pod_name(1))
        with self.assertRaises(ExecutionError) as caught:
            second_store.reserve_attempt(two_slots, second, pod_name(2))
        self.assertEqual(caught.exception.code, "total_cap_exceeded")

    def test_hourly_cap_is_checked_across_active_attempts_in_approval_scope(self):
        limited = plan_for(self.now, max_total=1_000_000, max_slots=2,
                           max_hourly=500_000)
        limited_store = ExecutionStore(str(self.root / "hourly-cap.sqlite"),
                                       project_id=limited.project.project_id,
                                       controller_host="TEST-HOST", clock=lambda: self.now)
        first = attempt_for(limited, "hourly-one", 1, self.now)
        second = attempt_for(limited, "hourly-two", 2, self.now)
        limited_store.reserve_attempt(limited, first, pod_name(1))
        with self.assertRaises(ExecutionError) as caught:
            limited_store.reserve_attempt(limited, second, pod_name(2))
        self.assertEqual(caught.exception.code, "hourly_cap_exceeded")

    def test_scope_reuse_cannot_change_caps_or_project_policy(self):
        self.reserve()
        changed_scope = plan_for(self.now, max_total=2_000_000)
        changed = attempt_for(changed_scope, "attempt-2", 2, self.now)
        with self.assertRaises(ExecutionError) as caught:
            self.store.reserve_attempt(changed_scope, changed, pod_name(2))
        self.assertEqual(caught.exception.code, "approval_scope_changed")

    def test_store_binds_canonical_project_and_controller_host_identity(self):
        attempt, _ = self.reserve()
        self.assertEqual(Path(self.store.path), self.path.resolve())
        for project_id, host_id in (("synthetic-other", "TEST-HOST"),
                                    (self.plan.project.project_id, "OTHER-HOST")):
            with self.subTest(project_id=project_id, host_id=host_id):
                with self.assertRaises(ExecutionError) as caught:
                    ExecutionStore(str(self.path), project_id=project_id,
                                   controller_host=host_id, clock=lambda: self.now)
                self.assertEqual(caught.exception.code, "store_identity_mismatch")
        with self.assertRaises(ExecutionError) as caught:
            self.store.accounting_snapshot("synthetic-other", self.plan.approval.approval_id)
        self.assertEqual(caught.exception.code, "store_identity_mismatch")
        self.assertEqual(self.store.get_attempt(attempt.attempt_id)["attempt_id"], attempt.attempt_id)

    def test_normalized_provider_billing_flows_into_transactional_accounting(self):
        attempt, exposure = self.reserve()
        self.activate_and_terminate(attempt)
        bucket_start = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.now - 3600))
        bucket_end = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.now))

        class BillingProvider:
            def __init__(self):
                self.requested_pods = []
            def pod_billing(self, pod_id, **_kwargs):
                self.requested_pods.append(pod_id)
                return [{"podId": pod_id, "time": bucket_start, "amount": 0.15}]

        provider = BillingProvider()
        observation, = ProviderAdapter(provider).observe_pod_billing(
            project_id=self.plan.project.project_id,
            attempt_id=attempt.attempt_id,
            provider_pod_id="pod-1",
            observed_epoch=self.now,
            start_time=bucket_start,
            end_time=bucket_end,
        )
        self.store.record_billing_observation(observation)
        snapshot = self.store.accounting_snapshot(self.plan.project.project_id,
                                                  self.plan.approval.approval_id)
        self.assertEqual(provider.requested_pods, ["pod-1"])
        self.assertEqual(snapshot.billed_micro_usd, 150_000)
        self.assertEqual(snapshot.reserved_micro_usd, exposure)
        self.assertEqual(self.store.get_attempt(attempt.attempt_id)["billing_complete"], 0)

    def test_posted_billing_above_approval_cap_is_preserved_and_blocks_admission(self):
        attempt, exposure = self.reserve()
        self.activate_and_terminate(attempt)
        posted = BillingObservation(
            "bill-over-cap", self.plan.project.project_id, attempt.attempt_id,
            "pod-1", 1_100_000, self.now - 1, self.now + 1,
            self.now + 2, ONE)
        self.store.record_billing_observation(posted)
        pending = self.store.accounting_snapshot(self.plan.project.project_id,
                                                 self.plan.approval.approval_id)
        self.assertEqual(pending.billed_micro_usd, 1_100_000)
        self.assertEqual(pending.reserved_micro_usd, exposure)
        self.assertEqual(pending.remaining_micro_usd, 0)

        self.store.mark_billing_complete(attempt.attempt_id, "2" * 64,
                                         self.now - 1, self.now + 1)
        settled = self.store.accounting_snapshot(self.plan.project.project_id,
                                                 self.plan.approval.approval_id)
        self.assertEqual(settled.committed_micro_usd, 1_100_000)
        self.assertEqual(settled.reserved_micro_usd, 0)
        self.assertEqual(settled.remaining_micro_usd, 0)
        with self.assertRaises(ExecutionError) as caught:
            self.reserve(number=2)
        self.assertEqual(caught.exception.code, "total_cap_exceeded")

    def test_accounting_aggregates_can_exceed_sqlite_integer_range(self):
        from runpod_experiments.execution_store import SQLITE_MAX_INTEGER

        plan = plan_for(self.now, max_slots=1)
        attempts = []
        for number in (1, 2):
            attempt, _ = self.reserve(number, plan=plan)
            self.activate_and_terminate(attempt, number)
            attempts.append((number, attempt))
        for number, attempt in attempts:
            observation = BillingObservation(
                f"bill-large-{number}", plan.project.project_id,
                attempt.attempt_id, f"pod-{number}", SQLITE_MAX_INTEGER - 1,
                self.now - 1, self.now + 1, self.now + 2,
                str(number) * 64)
            self.store.record_billing_observation(observation)

        snapshot = self.store.accounting_snapshot(
            plan.project.project_id, plan.approval.approval_id)
        expected = 2 * (SQLITE_MAX_INTEGER - 1)
        self.assertEqual(snapshot.billed_micro_usd, expected)
        self.assertEqual(snapshot.committed_micro_usd,
                         expected + 2 * 105_000)
        self.assertEqual(snapshot.remaining_micro_usd, 0)

    def test_billing_adjustments_replace_prior_amount_and_reopen_hold(self):
        attempt, exposure = self.reserve()
        self.activate_and_terminate(attempt)
        first = BillingObservation(
            "bill-1", self.plan.project.project_id, attempt.attempt_id, "pod-1",
            150_000, self.now - 1, self.now + 1800, self.now + 2000, ONE)
        self.store.record_billing_observation(first)
        prior_event_count = len(self.store.events())
        self.store.record_billing_observation(first)
        self.assertEqual(len(self.store.events()), prior_event_count)
        reopened = ExecutionStore(str(self.path), project_id=self.plan.project.project_id,
                                  controller_host="TEST-HOST", clock=lambda: self.now)
        pending = reopened.accounting_snapshot(self.plan.project.project_id,
                                               self.plan.approval.approval_id)
        self.assertEqual(pending.billed_micro_usd, 150_000)
        self.assertEqual(pending.reserved_micro_usd, exposure)
        with self.assertRaises(ExecutionError) as caught:
            self.store.mark_billing_complete(attempt.attempt_id, "3" * 64,
                                             self.now + 1, self.now + 1800)
        self.assertEqual(caught.exception.code, "billing_coverage_incomplete")
        self.store.mark_billing_complete(attempt.attempt_id, "3" * 64,
                                         self.now - 1, self.now + 1800)
        self.assertEqual(self.store.get_attempt(attempt.attempt_id)[
            "billing_complete_covered_start_epoch"], self.now - 1)
        reopened = ExecutionStore(str(self.path), project_id=self.plan.project.project_id,
                                  controller_host="TEST-HOST", clock=lambda: self.now)
        self.assertEqual(reopened.get_attempt(attempt.attempt_id)["exposure_micro_usd"], 0)
        settled = self.store.accounting_snapshot(self.plan.project.project_id,
                                                 self.plan.approval.approval_id)
        self.assertEqual(settled.billed_micro_usd, 150_000)
        self.assertEqual(settled.reserved_micro_usd, 0)

        adjusted = BillingObservation(
            "bill-2", self.plan.project.project_id, attempt.attempt_id, "pod-1",
            175_000, first.period_start_epoch, first.period_end_epoch,
            self.now + 2100, "4" * 64, supersedes_observation_id="bill-1")
        self.store.record_billing_observation(adjusted)
        pending = self.store.accounting_snapshot(self.plan.project.project_id,
                                                 self.plan.approval.approval_id)
        self.assertEqual(pending.billed_micro_usd, 175_000)
        self.assertEqual(pending.reserved_micro_usd, exposure)
        reopened = ExecutionStore(str(self.path), project_id=self.plan.project.project_id,
                                  controller_host="TEST-HOST", clock=lambda: self.now)
        self.assertEqual(reopened.accounting_snapshot(
            self.plan.project.project_id, self.plan.approval.approval_id).reserved_micro_usd,
            exposure)
        self.store.mark_billing_complete(attempt.attempt_id, "5" * 64,
                                         self.now - 1, self.now + 1800)
        final = self.store.accounting_snapshot(self.plan.project.project_id,
                                               self.plan.approval.approval_id)
        self.assertEqual(final.committed_micro_usd, 175_000)
        event_count = len(self.store.events())
        self.store.mark_billing_complete(attempt.attempt_id, "5" * 64,
                                         self.now - 1, self.now + 1800)
        self.assertEqual(len(self.store.events()), event_count)
        with self.assertRaises(ExecutionError) as caught:
            self.store.mark_billing_complete(attempt.attempt_id, "5" * 64,
                                             self.now - 2, self.now + 1800)
        self.assertEqual(caught.exception.code, "billing_completion_evidence_changed")
        self.assertTrue(self.store.verify_event_chain())

    def test_billing_requires_exact_pod_periods_and_adjustment_chain(self):
        attempt, _ = self.reserve()
        self.activate_and_terminate(attempt)
        bill = BillingObservation("bill-1", self.plan.project.project_id,
                                  attempt.attempt_id, "pod-1", 100_000,
                                  self.now - 1, self.now + 100, self.now + 200, ONE)
        self.store.record_billing_observation(bill)
        wrong_pod = BillingObservation("bill-wrong", self.plan.project.project_id,
                                       attempt.attempt_id, "pod-2", 10,
                                       self.now + 101, self.now + 200,
                                       self.now + 201, "2" * 64)
        with self.assertRaises(ExecutionError):
            self.store.record_billing_observation(wrong_pod)
        overlapping = BillingObservation("bill-overlap", self.plan.project.project_id,
                                         attempt.attempt_id, "pod-1", 10,
                                         self.now + 50, self.now + 200,
                                         self.now + 201, "2" * 64)
        with self.assertRaises(ExecutionError):
            self.store.record_billing_observation(overlapping)
        invalid_adjustment = BillingObservation(
            "bill-adjusted", self.plan.project.project_id, attempt.attempt_id,
            "pod-1", 110_000, self.now - 1, self.now + 101, self.now + 300,
            "3" * 64, supersedes_observation_id="bill-1")
        with self.assertRaises(ExecutionError):
            self.store.record_billing_observation(invalid_adjustment)

    def test_event_and_billing_rows_reject_mutation(self):
        attempt, _ = self.reserve()
        with closing(sqlite3.connect(self.path)) as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("UPDATE execution_events SET event_type='changed'")
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("DELETE FROM execution_attempts WHERE attempt_id=?",
                           (attempt.attempt_id,))
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("UPDATE execution_attempts SET attempt_sha256=? WHERE attempt_id=?",
                           ("f" * 64, attempt.attempt_id))
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("UPDATE execution_store_meta SET controller_host='OTHER-HOST'")
        self.assertTrue(self.store.verify_event_chain())

    def test_unsupported_or_unversioned_existing_databases_are_not_reinitialized(self):
        future = self.root / "future.sqlite"
        with closing(sqlite3.connect(future)) as db:
            db.execute("PRAGMA user_version=99")
        with self.assertRaises(ExecutionError) as caught:
            ExecutionStore(str(future), project_id="synthetic-nebula",
                           controller_host="TEST-HOST")
        self.assertEqual(caught.exception.code, "unsupported_store_schema")
        with closing(sqlite3.connect(future)) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 99)

        legacy = self.root / "legacy.sqlite"
        with closing(sqlite3.connect(legacy)) as db:
            db.execute("CREATE TABLE existing_data(value TEXT)")
        with self.assertRaises(ExecutionError) as caught:
            ExecutionStore(str(legacy), project_id="synthetic-nebula",
                           controller_host="TEST-HOST")
        self.assertEqual(caught.exception.code, "unversioned_store_not_empty")

        with_view = self.root / "legacy-view.sqlite"
        with closing(sqlite3.connect(with_view)) as db:
            db.execute("CREATE VIEW existing_view AS SELECT 1 AS value")
        with self.assertRaises(ExecutionError) as caught:
            ExecutionStore(str(with_view), project_id="synthetic-nebula",
                           controller_host="TEST-HOST")
        self.assertEqual(caught.exception.code, "unversioned_store_not_empty")

        altered = self.root / "altered.sqlite"
        ExecutionStore(str(altered), project_id="synthetic-nebula",
                       controller_host="TEST-HOST")
        with closing(sqlite3.connect(altered)) as db:
            db.execute("""CREATE TRIGGER unexpected_trigger AFTER UPDATE ON execution_attempts
                BEGIN SELECT 1; END""")
        with self.assertRaises(ExecutionError) as caught:
            ExecutionStore(str(altered), project_id="synthetic-nebula",
                           controller_host="TEST-HOST")
        self.assertEqual(caught.exception.code, "incomplete_store_schema")

    def test_separate_processes_cannot_over_admit_last_budget_or_slot(self):
        now = self.now
        child = r"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'tests'))
from test_execution_store import plan_for, attempt_for, pod_name
from runpod_experiments.execution_store import ExecutionStore
from runpod_experiments.errors import ExecutionError
now = int(sys.argv[2])
max_total = int(sys.argv[3])
max_slots = int(sys.argv[4])
store = ExecutionStore(sys.argv[1], project_id='synthetic-nebula',
                       controller_host='TEST-HOST', clock=lambda: now)
plan = plan_for(now, max_total=max_total, max_slots=max_slots)
attempt = attempt_for(plan, sys.argv[5], int(sys.argv[6]), now)
try:
    store.reserve_attempt(plan, attempt, pod_name(int(sys.argv[6])))
except ExecutionError as error:
    print('denied:' + error.code)
else:
    print('reserved')
"""
        for case, max_total, max_slots, expected_denial in (
                ("budget", 150_000, 2, "total_cap_exceeded"),
                ("slot", 1_000_000, 1, "project_slots_exhausted")):
            with self.subTest(case=case):
                constrained = plan_for(now, max_total=max_total, max_slots=max_slots)
                race_path = self.root / f"race-{case}.sqlite"
                children = []
                for attempt_id, number in ((f"{case}-one", 1), (f"{case}-two", 2)):
                    env = dict(os.environ)
                    env["RUNPOD_EXPERIMENTS_TEST_MODE"] = "1"
                    env["USERPROFILE"] = str(self.root)
                    env["HOME"] = str(self.root)
                    children.append(subprocess.Popen(
                        [sys.executable, "-c", child, str(race_path), str(now),
                         str(max_total), str(max_slots), attempt_id, str(number)],
                        cwd=Path(__file__).resolve().parents[1], env=env,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
                results = []
                for process in children:
                    stdout, stderr = process.communicate(timeout=20)
                    self.assertEqual(process.returncode, 0, stderr)
                    results.append(stdout.strip())
                self.assertEqual(sum(value == "reserved" for value in results), 1, results)
                self.assertEqual(results.count(f"denied:{expected_denial}"), 1, results)
                state = ExecutionStore(
                    str(race_path), project_id=constrained.project.project_id,
                    controller_host="TEST-HOST", clock=lambda: now).accounting_snapshot(
                        constrained.project.project_id, constrained.approval.approval_id)
                self.assertEqual(state.reserved_micro_usd, 105_000)

    def test_process_exit_mid_reservation_rolls_back_all_admission_rows(self):
        crash_path = self.root / "crash.sqlite"
        child = r"""
import os, sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'tests'))
from test_execution_store import plan_for, attempt_for, pod_name
from runpod_experiments.execution_store import ExecutionStore
now = int(sys.argv[2])
store = ExecutionStore(sys.argv[1], project_id='synthetic-nebula',
                       controller_host='TEST-HOST', clock=lambda: now)
append = ExecutionStore._append_event
def crash_after_event(db, entity_type, entity_id, event_type, created_epoch, payload):
    append(db, entity_type, entity_id, event_type, created_epoch, payload)
    os._exit(23)
ExecutionStore._append_event = staticmethod(crash_after_event)
plan = plan_for(now)
attempt = attempt_for(plan, 'crashed-attempt', 1, now)
store.reserve_attempt(plan, attempt, pod_name(1))
"""
        env = dict(os.environ)
        env["USERPROFILE"] = str(self.root)
        env["HOME"] = str(self.root)
        process = subprocess.run(
            [sys.executable, "-c", child, str(crash_path), str(self.now)],
            cwd=Path(__file__).resolve().parents[1], env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=20)
        self.assertEqual(process.returncode, 23, process.stderr)
        reopened = ExecutionStore(str(crash_path), project_id=self.plan.project.project_id,
                                  controller_host="TEST-HOST", clock=lambda: self.now)
        with closing(sqlite3.connect(crash_path)) as db:
            for table in ("execution_approvals", "execution_jobs", "execution_plans",
                          "execution_attempts", "execution_billing", "execution_events"):
                self.assertEqual(db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)
        attempt = attempt_for(self.plan, "crashed-attempt", 1, self.now)
        self.assertEqual(reopened.reserve_attempt(self.plan, attempt, pod_name(1)), 105_000)
        self.assertTrue(reopened.verify_event_chain())

    def test_process_exit_between_transaction_writes_rolls_back_partial_state(self):
        child = r"""
import os, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'tests'))
from test_execution_store import plan_for, attempt_for, pod_name
from runpod_experiments.contracts import BillingObservation
from runpod_experiments.execution_store import ExecutionStore
path, target_sql, action, now_arg = sys.argv[1:]
now = int(now_arg)
connect = sqlite3.connect
def traced_connect(*args, **kwargs):
    db = connect(*args, **kwargs)
    previous = [None]
    def trace(statement):
        if previous[0] is not None and target_sql in previous[0].lower():
            os._exit(24)
        previous[0] = statement.lower()
    db.set_trace_callback(trace)
    return db
sqlite3.connect = traced_connect
plan = plan_for(now)
store = ExecutionStore(path, project_id=plan.project.project_id,
                       controller_host='TEST-HOST', clock=lambda: now)
if action == 'reserve':
    attempt = attempt_for(plan, 'partial-reservation', 1, now)
    store.reserve_attempt(plan, attempt, pod_name(1))
elif action == 'billing':
    observation = BillingObservation('partial-billing', plan.project.project_id,
        'attempt-partial-billing', 'pod-partial', 150000,
        now - 1, now + 1, now + 2, '1' * 64)
    store.record_billing_observation(observation)
else:
    raise SystemExit('unknown test action')
os._exit(25)
"""
        cases = (
            ("reserve", "insert or ignore into execution_jobs"),
            ("billing", "insert into execution_billing"),
        )
        for action, target_sql in cases:
            with self.subTest(action=action):
                path = self.root / f"partial-{action}.sqlite"
                store = ExecutionStore(str(path), project_id=self.plan.project.project_id,
                                       controller_host="TEST-HOST", clock=lambda: self.now)
                attempt_id = "attempt-partial-billing"
                if action == "billing":
                    attempt = attempt_for(self.plan, attempt_id, 1, self.now)
                    store.reserve_attempt(self.plan, attempt, pod_name(1))
                    store.claim_create(attempt_id)
                    store.bind_pod(attempt_id, "pod-partial", pod_name(1))
                    store.request_stop(attempt_id)
                    store.confirm_termination(attempt_id, "pod-partial", ONE)
                    attempt_id = attempt.attempt_id
                prior_attempt = (store.get_attempt(attempt_id)
                                 if action == "billing" else None)
                prior_events = store.events()
                prior_accounting = (store.accounting_snapshot(
                    self.plan.project.project_id, self.plan.approval.approval_id)
                    if action == "billing" else None)
                env = dict(os.environ)
                env["USERPROFILE"] = str(self.root)
                env["HOME"] = str(self.root)
                process = subprocess.run(
                    [sys.executable, "-c", child, str(path), target_sql,
                     action, str(self.now)],
                    cwd=Path(__file__).resolve().parents[1], env=env,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, timeout=20)
                self.assertEqual(process.returncode, 24, process.stderr)
                reopened = ExecutionStore(str(path), project_id=self.plan.project.project_id,
                                          controller_host="TEST-HOST", clock=lambda: self.now)
                self.assertEqual(reopened.events(), prior_events)
                if action == "reserve":
                    with closing(sqlite3.connect(path)) as db:
                        for table in ("execution_approvals", "execution_jobs",
                                      "execution_plans", "execution_attempts",
                                      "execution_billing", "execution_events"):
                            self.assertEqual(db.execute(
                                f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)
                else:
                    self.assertEqual(reopened.accounting_snapshot(
                        self.plan.project.project_id, self.plan.approval.approval_id),
                        prior_accounting)
                    self.assertEqual(reopened.get_attempt(attempt_id), prior_attempt)
                    with closing(sqlite3.connect(path)) as db:
                        self.assertEqual(db.execute(
                            "SELECT COUNT(*) FROM execution_billing").fetchone()[0], 0)
                self.assertTrue(reopened.verify_event_chain())

    def test_process_exit_at_lifecycle_event_boundaries_rolls_back_each_transaction(self):
        child = r"""
import os, sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'tests'))
from test_execution_store import plan_for, pod_name, ONE
from runpod_experiments.contracts import BillingObservation
from runpod_experiments.execution_store import ExecutionStore
path, target_event, target_phase, action, attempt_id, now_arg = sys.argv[1:]
now = int(now_arg)
plan = plan_for(now)
append = ExecutionStore._append_event
def exit_after_target(db, entity_type, entity_id, event_type, created_epoch, payload):
    if event_type == target_event and target_phase == 'before':
        os._exit(23)
    append(db, entity_type, entity_id, event_type, created_epoch, payload)
    if event_type == target_event and target_phase == 'after':
        os._exit(23)
ExecutionStore._append_event = staticmethod(exit_after_target)
store = ExecutionStore(path, project_id=plan.project.project_id,
                       controller_host='TEST-HOST', clock=lambda: now)
pod_id = 'pod-crash'
pod = pod_name(1)
if action == 'claim':
    store.claim_create(attempt_id)
elif action == 'bind':
    store.bind_pod(attempt_id, pod_id, pod)
elif action == 'stop':
    store.request_stop(attempt_id)
elif action == 'terminate':
    store.confirm_termination(attempt_id, pod_id, ONE)
elif action == 'billing':
    bill = BillingObservation('bill-crash', plan.project.project_id, attempt_id,
        pod_id, 150000, now - 1, now + 1, now + 2, ONE)
    store.record_billing_observation(bill)
elif action == 'billing_complete':
    store.mark_billing_complete(attempt_id, '2' * 64, now - 1, now + 1)
elif action == 'reconcile':
    store.require_reconciliation(attempt_id, 'create_timeout')
elif action == 'absence':
    store.confirm_absence(attempt_id, ONE)
elif action == 'cancel':
    store.cancel_uncreated(attempt_id)
else:
    raise SystemExit('unknown test action')
"""
        events = (
            ("claim", "create_claimed"),
            ("bind", "pod_bound"),
            ("stop", "stop_requested"),
            ("terminate", "terminated"),
            ("billing", "observed"),
            ("billing_complete", "billing_marked_complete"),
            ("reconcile", "reconciliation_required"),
            ("absence", "create_absence_confirmed"),
            ("cancel", "cancelled_before_create"),
        )
        cases = tuple((action, event, phase)
                      for action, event in events for phase in ("before", "after"))
        for action, target_event, target_phase in cases:
            with self.subTest(action=action, phase=target_phase):
                path = self.root / f"crash-{action}-{target_phase}.sqlite"
                plan = plan_for(self.now)
                attempt_id = f"attempt-{action}"
                attempt = attempt_for(plan, attempt_id, 1, self.now)
                store = ExecutionStore(str(path), project_id=plan.project.project_id,
                                       controller_host="TEST-HOST", clock=lambda: self.now)
                exposure = store.reserve_attempt(plan, attempt, pod_name(1))
                if action not in ("claim", "cancel"):
                    store.claim_create(attempt_id)
                if action in ("stop", "terminate", "billing", "billing_complete"):
                    store.bind_pod(attempt_id, "pod-crash", pod_name(1))
                if action in ("terminate", "billing", "billing_complete"):
                    store.request_stop(attempt_id)
                if action in ("billing", "billing_complete"):
                    store.confirm_termination(attempt_id, "pod-crash", ONE)
                    bill = BillingObservation(
                        "bill-crash", plan.project.project_id, attempt_id, "pod-crash",
                        150_000, self.now - 1, self.now + 1, self.now + 2, ONE)
                    if action == "billing_complete":
                        store.record_billing_observation(bill)
                if action == "absence":
                    store.require_reconciliation(attempt_id, "create_timeout")

                prior_attempt = store.get_attempt(attempt_id)
                prior_events = store.events()
                prior_accounting = store.accounting_snapshot(
                    plan.project.project_id, plan.approval.approval_id)
                env = dict(os.environ)
                env["USERPROFILE"] = str(self.root)
                env["HOME"] = str(self.root)
                process = subprocess.run(
                    [sys.executable, "-c", child, str(path), target_event, target_phase, action,
                     attempt_id, str(self.now)],
                    cwd=Path(__file__).resolve().parents[1], env=env,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=20)
                self.assertEqual(process.returncode, 23, process.stderr)
                reopened = ExecutionStore(str(path), project_id=plan.project.project_id,
                                          controller_host="TEST-HOST", clock=lambda: self.now)
                self.assertEqual(reopened.get_attempt(attempt_id), prior_attempt)
                self.assertEqual(reopened.events(), prior_events)
                self.assertEqual(reopened.accounting_snapshot(
                    plan.project.project_id, plan.approval.approval_id), prior_accounting)
                self.assertEqual(reopened.accounting_snapshot(
                    plan.project.project_id, plan.approval.approval_id).reserved_micro_usd,
                    exposure if action == "billing_complete" else prior_accounting.reserved_micro_usd)
                self.assertTrue(reopened.verify_event_chain())


if __name__ == "__main__":
    unittest.main()
