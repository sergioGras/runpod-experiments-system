import base64
import io
import json
from pathlib import Path
import struct
import tempfile
from types import SimpleNamespace
import unittest

from runpod_experiments.contracts import (
    ApprovalScope, AttemptPlan, ExecutionPlan, FailureCategory, PodProvisioningSpec,
    ProjectPolicy, QuoteObservation, ResourceKind, ResourceProfile, RetryPolicy,
    TransferManifest, encode_document, sha256_bytes,
)
from runpod_experiments.create_spec import pod_create_request_sha256
from runpod_experiments.errors import ExecutionError
from runpod_experiments.execution_store import ExecutionStore
from runpod_experiments.interactive_ssh import SSHInteractiveProcess
from runpod_experiments.ssh import ssh_public_key_fingerprint
from runpod_experiments.state_config import StateStoreConfig
from runpod_experiments.store_create import StoreCreateController


ZERO = "0" * 64
ONE = "1" * 64
NOW = 1_900_000_000
REGION = "EU-TEST-1"


class FakeCreateClient:
    def __init__(self, kind, key_fingerprint):
        self.kind = kind
        self.keys = {key_fingerprint}
        self.inventory = []
        self.create_calls = []
        self.create_error = None
        self.response = None
        self.gpu_queries = []
        self.detail = None

    def pods(self):
        return [dict(item) for item in self.inventory]

    def ssh_key_fingerprints(self):
        return frozenset(self.keys)

    def pod(self, pod_id):
        if not isinstance(self.detail, dict) or self.detail.get("id") != pod_id:
            raise AssertionError("unexpected fake Pod detail request")
        return dict(self.detail)

    def cpu_offer(self, resource_id, vcpu_count):
        self.assertions = (resource_id, vcpu_count)
        return {"id": resource_id, "price": {"securePerVcpu": 0.075},
                "ramGbPerVcpu": 2, "vcpu": {"min": 4, "max": 4},
                "availability": "HIGH",
                "dataCenters": [{"id": REGION, "availability": "HIGH"}]}

    def gpu_offer(self, resource_id, gpu_count=1, *, min_cuda_version=None):
        self.gpu_queries.append((resource_id, gpu_count, min_cuda_version))
        return {"id": resource_id, "memory": 48, "secure": True,
                "price": {"secure": 0.59}, "maxCount": {"secure": 4},
                "dataCenters": [{"id": REGION, "availability": "HIGH"}]}

    def _create_pod(self, request):
        self.create_calls.append(dict(request))
        if self.create_error is not None:
            raise self.create_error
        return self.response or {"id": "pod-created-001", "name": request["name"]}


class StoreCreateControllerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="store-create-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = NOW
        self.kind = ResourceKind.CPU
        self.key_blob = struct.pack(">I", len(b"ssh-ed25519")) + b"ssh-ed25519" + bytes(range(32))
        self.public_key = "ssh-ed25519 " + base64.b64encode(self.key_blob).decode("ascii") + " fixture"
        self.key_fingerprint = ssh_public_key_fingerprint(self.public_key)
        self.key_path = self.root / "controller.pub"
        self.key_path.write_text(self.public_key + "\n", encoding="utf-8")
        self._open_attempt(ResourceKind.CPU)

    def _open_attempt(self, kind):
        self.kind = kind
        self.state_config = StateStoreConfig(self.root / "state", "TEST-HOST")
        self.project = ProjectPolicy(
            "synthetic-nebula", ("nebula-",), ZERO, ("TEST-HOST",), 1)
        self.scope = ApprovalScope(
            "approval-create", self.project.project_id, "TEST-HOST", ZERO,
            sha256_bytes(encode_document("project_policy", self.project)), ONE, "2" * 64,
            5_000_000, 1_000_000, 600, 1, self.now + 3600, 1)
        if kind is ResourceKind.CPU:
            profile = ResourceProfile(
                kind, "cpu-small", 300_000, (REGION,), "sha256:" + ZERO,
                20, 4, 8)
            quote = QuoteObservation(
                kind, profile.resource_id, 4, 4, 300_000, 0, True,
                (REGION,), self.now - 1, ONE, 8)
            attempt_id = "attempt-cpu"
        else:
            profile = ResourceProfile(
                kind, "NVIDIA A40", 590_000, (REGION,), "sha256:" + ZERO,
                30, 8, 32, 1, 48, "12.4")
            quote = QuoteObservation(
                kind, profile.resource_id, 1, 4, 590_000, 0, True,
                (REGION,), self.now - 1, ONE, 48, "12.4")
            attempt_id = "attempt-gpu"
        self.plan = ExecutionPlan(
            "job-create", self.project, self.scope, "a" * 40, "3" * 64,
            "4" * 64, "5" * 64, (profile,),
            PodProvisioningSpec("runpod/base@sha256:" + ZERO, ("22/tcp",)),
            TransferManifest((), 4096, 4096),
            RetryPolicy(1, 600, (FailureCategory.TRANSIENT_READ,)))
        attempt = AttemptPlan(
            attempt_id, self.plan.job_id, self.scope.approval_id, 1,
            pod_create_request_sha256(self.plan.provisioning, profile, REGION,
                                      "nebula-run-001"),
            sha256_bytes(encode_document("quote_observation", quote)), quote,
            sha256_bytes(encode_document("project_policy", self.project)),
            self.plan.source_sha256, self.plan.config_sha256, self.plan.package_sha256,
            profile, REGION, 300, 60, self.now, self.now + 360)
        self.store = self.state_config.open_execution_store(
            self.project.project_id, clock=lambda: self.now)
        self.store.reserve_attempt(self.plan, attempt, "nebula-run-001")
        self.attempt = self.store.get_attempt(attempt_id)
        self.client = FakeCreateClient(kind, self.key_fingerprint)
        self.supervisor_directory = (
            self.state_config.state_root / self.project.project_id / attempt_id / "supervisor")
        self.supervisor_directory.mkdir(parents=True)
        self.process = SimpleNamespace(pid=4242, poll=lambda: None)
        policy_sha256 = sha256_bytes(encode_document("project_policy", self.project))
        (self.supervisor_directory / "supervisor.json").write_text(json.dumps({
            "ready": True, "stopping": False, "terminated": False, "pid": self.process.pid,
            "attempt_id": attempt_id, "project_id": self.project.project_id,
            "deadline_epoch": self.attempt["deadline_epoch"],
            "attempt_sha256": self.attempt["attempt_sha256"],
            "policy_sha256": policy_sha256, "heartbeat_epoch": self.now,
        }), encoding="utf-8")
        self.controller = StoreCreateController(
            self.client, self.store, self.state_config, clock=lambda: self.now,
            sleep=lambda _duration: None)

    def create(self):
        return self.controller.create(
            self.attempt["attempt_id"], self.process, self.supervisor_directory,
            self.key_path)

    def test_cpu_create_binds_verified_preflight_and_returns_safe_receipt(self):
        receipt = self.create()
        self.assertEqual(receipt.pod_id, "pod-created-001")
        self.assertEqual(receipt.pod_name, "nebula-run-001")
        self.assertEqual(receipt.request_sha256, pod_create_request_sha256(
            self.plan.provisioning, self.plan.resource_options[0], REGION, "nebula-run-001"))
        self.assertEqual(self.client.create_calls[0]["cpu"], {
            "id": "cpu-small", "vcpuCount": 4})
        self.assertEqual(self.store.get_attempt(self.attempt["attempt_id"])["state"], "active")
        self.assertEqual([event["event_type"] for event in self.store.events()][-2:],
                         ["create_claimed", "pod_bound"])
        preflight = next(event["payload"] for event in self.store.events()
                         if event["event_type"] == "create_preflight_checked")
        self.assertNotIn(self.public_key, json.dumps(preflight))
        self.assertEqual(preflight["ssh_key_fingerprint"], self.key_fingerprint)

    def test_missing_registered_key_cancels_before_provider_create(self):
        self.client.keys.clear()
        with self.assertRaises(ExecutionError) as caught:
            self.create()
        self.assertEqual(caught.exception.code, "ssh_public_key_not_registered")
        self.assertEqual(self.client.create_calls, [])
        self.assertEqual(self.store.get_attempt(self.attempt["attempt_id"])["state"], "cancelled")

    def test_unexpected_project_pod_blocks_create_without_deleting_it(self):
        pod = {"id": "unknown-pod", "name": "nebula-manual-run", "status": "RUNNING"}
        self.client.inventory.append(pod)
        with self.assertRaises(ExecutionError) as caught:
            self.create()
        self.assertEqual(caught.exception.code, "project_resource_already_present")
        self.assertEqual(self.client.create_calls, [])
        self.assertEqual(self.client.inventory, [pod])

    def test_supervisor_identity_and_heartbeat_are_checked_before_claim(self):
        path = self.supervisor_directory / "supervisor.json"
        state = json.loads(path.read_text(encoding="utf-8"))
        state["attempt_sha256"] = "f" * 64
        path.write_text(json.dumps(state), encoding="utf-8")
        with self.assertRaises(ExecutionError) as caught:
            self.create()
        self.assertEqual(caught.exception.code, "supervisor_identity_mismatch")
        self.assertEqual(self.client.create_calls, [])
        self.assertEqual(self.store.get_attempt(self.attempt["attempt_id"])["state"], "cancelled")

    def test_supervisor_is_rechecked_after_claim_before_provider_create(self):
        original_claim = self.store.claim_create
        path = self.supervisor_directory / "supervisor.json"

        def claim_then_stale(attempt_id, **kwargs):
            original_claim(attempt_id, **kwargs)
            state = json.loads(path.read_text(encoding="utf-8"))
            state["heartbeat_epoch"] = self.now - 60
            path.write_text(json.dumps(state), encoding="utf-8")

        self.store.claim_create = claim_then_stale
        with self.assertRaises(ExecutionError) as caught:
            self.create()
        self.assertEqual(caught.exception.code, "supervisor_heartbeat_stale")
        self.assertEqual(self.client.create_calls, [])
        self.assertEqual(self.store.get_attempt(self.attempt["attempt_id"])["state"],
                         "reconciliation_required")

    def test_create_timeout_is_ambiguous_and_never_retried(self):
        self.client.create_error = TimeoutError("synthetic timeout")
        with self.assertRaises(ExecutionError) as caught:
            self.create()
        self.assertEqual(caught.exception.code, "create_response_ambiguous")
        self.assertEqual(len(self.client.create_calls), 1)
        attempt = self.store.get_attempt(self.attempt["attempt_id"])
        self.assertEqual(attempt["state"], "reconciliation_required")
        self.assertEqual(attempt["exposure_micro_usd"], attempt["initial_exposure_micro_usd"])
        with self.assertRaises(ExecutionError):
            self.create()
        self.assertEqual(len(self.client.create_calls), 1)

    def test_malformed_create_response_keeps_exposure_for_reconciliation(self):
        self.client.response = {"id": "pod-created-001", "name": "wrong-name"}
        with self.assertRaises(ExecutionError) as caught:
            self.create()
        self.assertEqual(caught.exception.code, "create_response_invalid")
        self.assertEqual(self.store.get_attempt(self.attempt["attempt_id"])["state"],
                         "reconciliation_required")

    def test_gpu_create_scopes_current_quote_to_cuda_and_region(self):
        self.temp = tempfile.TemporaryDirectory(prefix="store-create-gpu-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.key_path = self.root / "controller.pub"
        self.key_path.write_text(self.public_key + "\n", encoding="utf-8")
        self._open_attempt(ResourceKind.GPU)
        receipt = self.create()
        self.assertEqual(receipt.pod_id, "pod-created-001")
        self.assertEqual(self.client.gpu_queries,
                         [("NVIDIA A40", 1, "12.4")])
        self.assertEqual(self.client.create_calls[0]["gpu"], {
            "id": "NVIDIA A40", "count": 1, "minRamPerGpu": 32,
            "minVcpuCountPerGpu": 8, "minCudaVersion": "12.4",
        })

    def test_store_backed_interactive_ssh_checks_live_resource_before_launch(self):
        receipt = self.create()
        self.client.detail = {
            "id": receipt.pod_id, "name": receipt.pod_name, "status": "RUNNING",
            "cost": 0.3, "cloud": "SECURE", "disk": 20, "dataCenterId": REGION,
            "image": "runpod/base@sha256:" + ZERO, "mounts": {},
            "cpu": {"id": "cpu-small", "vcpuCount": 4, "memory": 8},
            "ports": ["22/tcp"],
            "ssh": {"direct": {"host": "127.0.0.1", "port": 2222, "username": "root"}},
        }
        known_hosts = self.root / "known_hosts"
        known_hosts.write_text("[127.0.0.1]:2222 ssh-ed25519 fixture\n", encoding="utf-8")
        private_key = self.root / "id_ed25519"
        private_key.write_bytes(b"private-key-fixture")
        started = {}

        class PipeProcess:
            def __init__(self, argv, kwargs):
                started.update(argv=argv, kwargs=kwargs)
                self.stdin = io.BytesIO()
                self.stdout = io.BytesIO(b"response")
                self.returncode = None

            def poll(self):
                return self.returncode

            def wait(self, timeout=None):
                self.returncode = 0
                return 0

            def terminate(self):
                self.returncode = -15

            def kill(self):
                self.returncode = -9

        def popen(argv, **kwargs):
            return PipeProcess(argv, kwargs)

        session = SSHInteractiveProcess(
            self.client, self.store, self.attempt["attempt_id"], self.project.project_id,
            private_key, known_hosts, "/tmp/nebula", "worker.py", ["--serve"], 60,
            self.root / "ssh.log", popen=popen)
        try:
            self.assertEqual(session.receive(8, 1), b"response")
            self.assertIn("StrictHostKeyChecking=yes", started["argv"])
            self.assertIn("root@127.0.0.1", started["argv"])
        finally:
            session.terminate()

        self.client.detail["cpu"]["vcpuCount"] = 8
        with self.assertRaises(ExecutionError) as caught:
            SSHInteractiveProcess(
                self.client, self.store, self.attempt["attempt_id"], self.project.project_id,
                private_key, known_hosts, "/tmp/nebula", "worker.py", [], 60,
                self.root / "ssh-mismatch.log",
                popen=lambda *_args, **_kwargs: self.fail("mismatched Pod must not launch SSH"))
        self.assertEqual(caught.exception.code, "attempt_resource_shape_unverified")

    def test_missing_region_or_changed_quote_blocks_create(self):
        self.client.cpu_offer = lambda *_args: {
            "id": "cpu-small", "price": {"securePerVcpu": 0.076},
            "ramGbPerVcpu": 2, "vcpu": {"min": 4, "max": 4},
            "availability": "HIGH",
            "dataCenters": [{"id": "US-TEST-1", "availability": "HIGH"}],
        }
        with self.assertRaises(ExecutionError) as caught:
            self.create()
        self.assertIn(caught.exception.code,
                      {"preflight_quote_mismatch", "selected_region_not_available"})
        self.assertEqual(self.client.create_calls, [])


if __name__ == "__main__":
    unittest.main()
