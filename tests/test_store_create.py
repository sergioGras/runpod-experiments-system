import base64
import io
import json
import re
import os
from pathlib import Path
import struct
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from dataclasses import replace

from runpod_experiments.contracts import (
    ApprovalScope, AttemptPlan, BillingStatus, CleanupStatus, ExecutionOutcome,
    ExecutionPlan, FailureCategory, OperationalStatus, PodProvisioningSpec,
    ManifestFile, ProjectPolicy, QuoteObservation, ResourceKind, ResourceProfile,
    RetryPolicy, ScientificVerdict, TransferDirection, TransferManifest,
    StageStatus, encode_document, sha256_bytes,
)
from runpod_experiments.artifacts import ArtifactStore
from runpod_experiments.collection_process import launch_execution_store
from runpod_experiments.create_spec import pod_create_request_sha256
from runpod_experiments.errors import ExecutionError
from runpod_experiments.execution_store import ExecutionStore
from runpod_experiments.interactive_ssh import SSHInteractiveProcess
from runpod_experiments.remote_process import start_python
from runpod_experiments.run_collection import collect_owned_once
from runpod_experiments.serial_engine import (
    AttemptWorkload, SerialAttemptResult, SerialExecutionEngine,
)
from runpod_experiments.ssh import ssh_public_key_fingerprint
from runpod_experiments.state_config import StateStoreConfig
from runpod_experiments.store_create import StoreCreateController
from runpod_experiments.supervision import StoreCleanupMonitor
from runpod_experiments.upload import upload_bundle


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
        self.attempt_plan = attempt
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

    def set_live_cpu_detail(self, receipt):
        self.client.detail = {
            "id": receipt.pod_id, "name": receipt.pod_name, "status": "RUNNING",
            "cost": 0.3, "cloud": "SECURE", "disk": 20, "dataCenterId": REGION,
            "image": "runpod/base@sha256:" + ZERO, "mounts": {},
            "cpu": {"id": "cpu-small", "vcpuCount": 4, "memory": 8},
            "ports": ["22/tcp"],
            "ssh": {"direct": {"host": "127.0.0.1", "port": 2222, "username": "root"}},
        }

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
        self.set_live_cpu_detail(receipt)
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

        self.client.detail["cpu"]["vcpuCount"] = 4
        self.client.detail["ports"] = ["22/tcp", "8080/http"]
        with self.assertRaises(ExecutionError) as caught:
            SSHInteractiveProcess(
                self.client, self.store, self.attempt["attempt_id"], self.project.project_id,
                private_key, known_hosts, "/tmp/nebula", "worker.py", [], 60,
                self.root / "ssh-ports.log",
                popen=lambda *_args, **_kwargs: self.fail("unexpected exposed port must block SSH"))
        self.assertEqual(caught.exception.code, "attempt_ports_unverified")

    def test_store_backed_upload_launch_and_collection_share_the_verified_target(self):
        receipt = self.create()
        self.set_live_cpu_detail(receipt)
        known_hosts = self.root / "known_hosts"
        known_hosts.write_text("[127.0.0.1]:2222 ssh-ed25519 fixture\n", encoding="utf-8")
        private_key = self.root / "id_ed25519"
        private_key.write_bytes(b"private-key-fixture")
        worker = b"synthetic worker source"
        source = self.root / "worker.py"
        source.write_bytes(worker)
        uploaded = []

        def upload_runner(argv, **kwargs):
            uploaded.append((argv, kwargs["input"]))
            return SimpleNamespace(stdout=b"", returncode=0)

        self.assertEqual(upload_bundle(
            self.client, self.store, self.attempt["attempt_id"], self.project.project_id,
            self.root, {"worker.py": sha256_bytes(worker)}, private_key, known_hosts,
            "/tmp/nebula", runner=upload_runner), 1)
        self.assertEqual(uploaded[0][1], worker)
        self.assertIn("root@127.0.0.1", uploaded[0][0])

        launched = {}

        def popen(argv, **kwargs):
            launched.update(argv=argv, kwargs=kwargs)
            return SimpleNamespace(pid=5050)

        process = start_python(
            self.client, self.store, self.attempt["attempt_id"], self.project.project_id,
            private_key, known_hosts, "/tmp/nebula", "worker.py", ["--run"], 30,
            self.root / "worker.log", popen=popen)
        self.assertEqual(process.pid, 5050)
        self.assertIn("root@127.0.0.1", launched["argv"])
        self.assertIn("timeout 30 python3 worker.py --run", launched["argv"][-1])

        class Fetcher:
            def __init__(self, host, port, username, *_args):
                self.endpoint = (host, port, username)

            def __call__(self, _name):
                return b"synthetic result"

        artifacts = ArtifactStore(self.root / "artifacts")
        previous, errors = collect_owned_once(
            self.client, self.store, self.attempt["attempt_id"], self.project.project_id,
            artifacts, ["results/out.bin"], private_key, known_hosts, "/tmp/nebula",
            fetcher_type=Fetcher)
        self.assertEqual(errors, {})
        self.assertEqual(previous["results/out.bin"], sha256_bytes(b"synthetic result"))

    def test_store_proxy_route_supports_bounded_upload_and_collection_resolution(self):
        receipt = self.create()
        self.set_live_cpu_detail(receipt)
        self.client.detail["ssh"]["proxy"] = {
            "host": "ssh.runpod.io", "port": 22, "username": "runpod"}
        known_hosts = self.root / "known_hosts"
        known_hosts.write_text("[ssh.runpod.io]:22 ssh-ed25519 fixture\n", encoding="utf-8")
        private_key = self.root / "id_ed25519"
        private_key.write_bytes(b"private-key-fixture")
        worker = b"proxy worker source"
        (self.root / "worker.py").write_bytes(worker)
        calls = []

        def proxy_runner(argv, **kwargs):
            calls.append((argv, kwargs))
            request = kwargs["input"]
            match = re.search(rb"(RUNPOD_[A-F0-9]{24})_STATUS:%s", request)
            self.assertIsNotNone(match)
            return SimpleNamespace(stdout=match.group(1) + b"_STATUS:0\n",
                                   stderr=b"", returncode=0)

        self.assertEqual(upload_bundle(
            self.client, self.store, self.attempt["attempt_id"], self.project.project_id,
            self.root, {"worker.py": sha256_bytes(worker)}, private_key, known_hosts,
            "/tmp/nebula", ssh_route="proxy", runner=proxy_runner), 1)
        self.assertEqual(len(calls), 1)
        self.assertIn("-tt", calls[0][0])
        self.assertIn("runpod@ssh.runpod.io", calls[0][0])

        class Fetcher:
            def __init__(self, host, port, username, *_args):
                self.endpoint = (host, port, username)

            def __call__(self, _name):
                return b"proxy-result"

        previous, errors = collect_owned_once(
            self.client, self.store, self.attempt["attempt_id"], self.project.project_id,
            ArtifactStore(self.root / "proxy-artifacts"), ["results/out.bin"],
            private_key, known_hosts, "/tmp/nebula", ssh_route="proxy",
            fetcher_type=Fetcher)
        self.assertEqual(errors, {})
        self.assertEqual(previous["results/out.bin"], sha256_bytes(b"proxy-result"))

        launched = {}

        def popen(argv, **kwargs):
            launched["argv"] = argv
            return SimpleNamespace(pid=7070)

        self.assertEqual(launch_execution_store(
            self.state_config, self.attempt["attempt_id"], self.project.project_id,
            self.root / "proxy-artifacts", ["results/out.bin"], private_key,
            known_hosts, "/tmp/nebula", ssh_route="proxy", popen=popen).pid, 7070)
        route_index = launched["argv"].index("--ssh-route")
        self.assertEqual(launched["argv"][route_index + 1], "proxy")

    def test_store_serial_engine_runs_one_fake_attempt_and_confirms_cleanup(self):
        collector_limits = {}
        self.store.cancel_uncreated(self.attempt["attempt_id"])
        worker = b"synthetic serial worker"
        transfer = TransferManifest((
            ManifestFile("worker.py", sha256_bytes(worker), len(worker), TransferDirection.UPLOAD),
            ManifestFile("results.json", ZERO, 0, TransferDirection.DOWNLOAD),
        ), 4096, 8192)
        job_id, attempt_id, pod_name = "job-serial-engine", "attempt-engine", "nebula-engine-001"
        plan = replace(self.plan, job_id=job_id, transfers=transfer)
        attempt_plan = replace(
            self.attempt_plan, attempt_id=attempt_id, job_id=job_id,
            request_sha256=pod_create_request_sha256(
                plan.provisioning, self.attempt_plan.selected_resource, REGION, pod_name))
        self.store.reserve_attempt(plan, attempt_plan, pod_name)
        attempt = self.store.get_attempt(attempt_id)
        self.assertEqual(attempt["state"], "reserved")

        source_root = self.root / "serial-source"
        source_root.mkdir()
        (source_root / "worker.py").write_bytes(worker)

        def create_pod(request):
            receipt = SimpleNamespace(pod_id="pod-engine-001", pod_name=request["name"])
            self.set_live_cpu_detail(receipt)
            self.client.inventory = [{"id": receipt.pod_id, "name": receipt.pod_name,
                                      "status": "RUNNING"}]
            return {"id": receipt.pod_id, "name": receipt.pod_name}

        def delete_pod(pod_id):
            self.assertEqual(pod_id, "pod-engine-001")
            self.client.inventory = []
            self.client.detail["status"] = "TERMINATED"
            return {"deleted": True}

        self.client._create_pod = create_pod
        self.client._delete_pod = delete_pod

        def supervisor_launcher(_config, selected_attempt, project_id, directory):
            directory = Path(directory)
            directory.mkdir(parents=True, exist_ok=True)
            monitor = StoreCleanupMonitor(
                self.client, self.store, selected_attempt, project_id,
                clock=lambda: self.now)
            owner = self

            class FakeSupervisor:
                pid = os.getpid()
                returncode = None

                def poll(self):
                    if self.returncode is not None:
                        return self.returncode
                    if not getattr(self, "ready", False):
                        monitor.client.pods()
                    stop = (directory / "stop.request").exists()
                    outcome = monitor.observe(stop=stop)
                    self.ready = True
                    state = {
                        "ready": True, "stopping": monitor.stopping,
                        "terminated": outcome == "terminated",
                        "pid": self.pid, "attempt_id": selected_attempt,
                        "project_id": project_id,
                        "deadline_epoch": attempt["deadline_epoch"],
                        "attempt_sha256": attempt["attempt_sha256"],
                        "policy_sha256": monitor.policy_sha256,
                        "heartbeat_epoch": owner.now,
                    }
                    (directory / "supervisor.json").write_text(
                        json.dumps(state) + "\n", encoding="utf-8")
                    if outcome in ("terminated", "not_created"):
                        self.returncode = 0
                    return self.returncode

            return FakeSupervisor()

        class Collector:
            def poll(self):
                return None

            def wait(self, timeout=None):
                return 0

        def collector_launcher(*_args, **kwargs):
            collector_limits.update(kwargs)
            return Collector()

        engine = SerialExecutionEngine(
            self.client, self.store, self.state_config,
            supervisor_launcher=supervisor_launcher,
            collector_launcher=collector_launcher,
            uploader=lambda *_args, **_kwargs: 1,
            process_launcher=lambda *_args, **_kwargs: SimpleNamespace(
                returncode=0, poll=lambda: 0),
            clock=lambda: self.now, sleep=lambda _duration: None)
        workload = AttemptWorkload(
            source_root, self.root / "id_ed25519", self.root / "known_hosts",
            "/tmp/nebula", "worker.py", (), 30, ("results.json",),
            self.root / "serial-artifacts")
        self.assertTrue(self.key_path.is_file())
        (self.root / "id_ed25519").write_bytes(b"private-key-fixture")
        (self.root / "known_hosts").write_text(
            "[127.0.0.1]:2222 ssh-ed25519 fixture\n", encoding="utf-8")
        result = engine.run_attempt(
            plan, attempt_id, workload, local_public_key_path=self.key_path,
            cleanup_timeout_seconds=3)
        self.assertEqual(result.pod_id, "pod-engine-001",
                         msg=f"{result.outcome}; stages={result.stage_events}; "
                             f"events={[(e['entity_type'], e['event_type'], e['entity_id']) for e in self.store.events()]}")
        self.assertEqual(self.store.get_attempt(attempt_id)["state"], "terminated")
        self.assertEqual(result.outcome.operational_status.value, "succeeded",
                         msg=f"{result.outcome}; stages={result.stage_events}; "
                             f"attempt={self.store.get_attempt(attempt_id)}")
        self.assertEqual(result.outcome.cleanup_status.value, "confirmed")
        self.assertEqual(result.outcome.scientific_verdict.value, "not_evaluated")
        self.assertTrue(self.store.verify_attempt_lifecycle_projection(attempt_id))
        self.assertEqual(result.stage_events[-1].stage, "cleanup")
        self.assertEqual(result.stage_events[-1].status.value, "succeeded")
        self.assertEqual(collector_limits["max_file_bytes"], plan.transfers.max_file_bytes)
        self.assertEqual(collector_limits["max_total_bytes"], plan.transfers.max_total_bytes)
        self.assertEqual(collector_limits["per_file_limits"], {"results.json": 0})

    def test_serial_job_retries_only_after_a_clean_terminal_attempt(self):
        self.store.cancel_uncreated(self.attempt["attempt_id"])
        retry_scope = replace(self.scope, approval_id="approval-retry", max_attempts=2)
        plan = replace(
            self.plan, job_id="job-retry", approval=retry_scope,
            retry=RetryPolicy(2, 120, (FailureCategory.TRANSIENT_READ,), 1, 2))
        attempt_one = replace(
            self.attempt_plan, attempt_id="attempt-retry-1", job_id=plan.job_id,
            approval_id=retry_scope.approval_id, attempt_number=1,
            request_sha256=pod_create_request_sha256(
                plan.provisioning, self.attempt_plan.selected_resource, REGION,
                "nebula-retry-001"))
        self.store.reserve_attempt(plan, attempt_one, "nebula-retry-001")
        attempt_two = replace(
            attempt_one, attempt_id="attempt-retry-2", attempt_number=2,
            request_sha256=pod_create_request_sha256(
                plan.provisioning, attempt_one.selected_resource, REGION,
                "nebula-retry-002"))
        workload = AttemptWorkload(
            self.root, self.key_path, self.key_path, "/work", "worker.py", (),
            30, ("result.bin",), self.root / "retry-artifacts")
        engine = SerialExecutionEngine(
            self.client, self.store, self.state_config,
            monotonic=lambda: 100.0, sleep=lambda _duration: None)
        attempts_run = []
        reservations = []

        def fail_before_create(_plan, selected_attempt, _workload, **_kwargs):
            attempts_run.append(selected_attempt)
            self.store.cancel_uncreated(selected_attempt)
            outcome = ExecutionOutcome(
                OperationalStatus.FAILED, ScientificVerdict.NOT_EVALUATED,
                CleanupStatus.NOT_REQUIRED, BillingStatus.NOT_APPLICABLE,
                FailureCategory.TRANSIENT_READ)
            return SerialAttemptResult(selected_attempt, None, outcome, ())

        def reserve_next(number, prior_result):
            reservations.append((number, prior_result.attempt_id))
            self.assertEqual(number, 2)
            self.store.reserve_attempt(plan, attempt_two, "nebula-retry-002")
            return "attempt-retry-2", workload

        with patch.object(engine, "run_attempt", side_effect=fail_before_create):
            result = engine.run_job(
                plan, "attempt-retry-1", workload, reserve_next=reserve_next,
                local_public_key_path=self.key_path)
        self.assertEqual(attempts_run, ["attempt-retry-1", "attempt-retry-2"])
        self.assertEqual(reservations, [(2, "attempt-retry-1")])
        self.assertEqual(result.stop_reason, "attempt_limit_reached")
        self.assertEqual(len(result.attempt_results), 2)
        self.assertEqual(self.store.get_attempt("attempt-retry-2")["state"], "cancelled")

    def test_controller_restart_recovery_stops_without_replaying_workload(self):
        receipt = self.create()
        self.set_live_cpu_detail(receipt)
        self.client.inventory = [{"id": receipt.pod_id, "name": receipt.pod_name,
                                  "status": "RUNNING"}]

        def delete_pod(pod_id):
            self.assertEqual(pod_id, receipt.pod_id)
            self.client.inventory = []
            self.client.detail["status"] = "TERMINATED"
            return {"deleted": True}

        self.client._delete_pod = delete_pod
        attempt_id = self.attempt["attempt_id"]
        for stage in ("supervisor", "create", "upload", "collector"):
            self.store.append_stage_event(attempt_id, stage, StageStatus.STARTED)
            self.store.append_stage_event(attempt_id, stage, StageStatus.SUCCEEDED)
        self.store.append_stage_event(attempt_id, "workload", StageStatus.STARTED)
        attempt_directory = (self.state_config.state_root / self.project.project_id / attempt_id)
        supervisor_directory = attempt_directory / "supervisor"
        stop_path = supervisor_directory / "stop.request"
        monitor = StoreCleanupMonitor(
            self.client, self.store, attempt_id, self.project.project_id,
            clock=lambda: self.now)
        ticks = [0.0]

        def recovery_sleep(duration):
            ticks[0] += duration
            self.now += 1
            stop = stop_path.exists()
            observation = monitor.observe(stop=stop)
            supervisor_state = {
                "ready": True, "stopping": monitor.stopping,
                "terminated": observation == "terminated", "pid": self.process.pid,
                "attempt_id": attempt_id, "project_id": self.project.project_id,
                "deadline_epoch": self.attempt["deadline_epoch"],
                "attempt_sha256": self.attempt["attempt_sha256"],
                "policy_sha256": monitor.policy_sha256,
                "heartbeat_epoch": self.now,
            }
            (supervisor_directory / "supervisor.json").write_text(
                json.dumps(supervisor_state) + "\n", encoding="utf-8")

        engine = SerialExecutionEngine(
            self.client, self.store, self.state_config, clock=lambda: self.now,
            monotonic=lambda: ticks[0], sleep=recovery_sleep)
        result = engine.recover_attempt(
            self.plan, attempt_id, cleanup_timeout_seconds=10,
            collector_timeout_seconds=1)
        self.assertEqual(self.store.get_attempt(attempt_id)["state"], "terminated")
        self.assertEqual(result.outcome.operational_status.value, "reconciliation_required")
        self.assertEqual(result.outcome.cleanup_status.value, "confirmed")
        workload_failure = next(
            event for event in reversed(result.stage_events)
            if event.stage == "workload" and event.status is StageStatus.FAILED)
        self.assertEqual(workload_failure.failure_category,
                         FailureCategory.RECONCILIATION_REQUIRED)
        self.assertEqual(result.stage_events[-1].stage, "cleanup")
        self.assertEqual(result.stage_events[-1].status, StageStatus.SUCCEEDED)
        self.assertTrue(self.store.verify_attempt_lifecycle_projection(attempt_id))

    def test_detached_store_collector_binds_allowlist_store_and_attempt(self):
        receipt = self.create()
        self.set_live_cpu_detail(receipt)
        known_hosts = self.root / "known_hosts"
        known_hosts.write_text("[127.0.0.1]:2222 ssh-ed25519 fixture\n", encoding="utf-8")
        private_key = self.root / "id_ed25519"
        private_key.write_bytes(b"private-key-fixture")
        artifact_directory = self.root / "artifacts"
        names = ["results/out.bin"]
        launched = {}

        def popen(argv, **kwargs):
            launched.update(argv=argv, kwargs=kwargs)
            return SimpleNamespace(pid=6060)

        process = launch_execution_store(
            self.state_config, self.attempt["attempt_id"], self.project.project_id,
            artifact_directory, names, private_key, known_hosts, "/tmp/nebula",
            max_file_bytes=64, max_total_bytes=96,
            per_file_limits={"results/out.bin": 32}, popen=popen)
        self.assertEqual(process.pid, 6060)
        self.assertIn("--store-root", launched["argv"])
        collector_directory = (self.state_config.state_root / self.project.project_id /
                               self.attempt["attempt_id"] / "collector")
        names_path = collector_directory / "collection-allowlist.json"
        self.assertEqual(json.loads(names_path.read_text(encoding="utf-8")), names)
        limits_path = collector_directory / "collection-limits.json"
        self.assertEqual(json.loads(limits_path.read_text(encoding="utf-8")), {
            "max_file_bytes": 64, "max_total_bytes": 96,
            "per_file_limits": {"results/out.bin": 32}})
        with self.assertRaises(ValueError):
            launch_execution_store(
                self.state_config, self.attempt["attempt_id"], self.project.project_id,
                artifact_directory, ["different.bin"], private_key, known_hosts,
                "/tmp/nebula", popen=popen)

        from runpod_experiments import collection_process as entry
        from runpod_experiments.ssh import SSHFetcher
        argv = launched["argv"]
        cli = ["collector", *argv[argv.index("--store-root"):]]
        original_collect_loop = entry.collect_loop

        def one_collection(*args, **kwargs):
            return original_collect_loop(*args, max_polls=1, **kwargs)

        with patch.object(entry.RunPodClient, "from_config", return_value=self.client), \
             patch.object(entry, "collect_loop", side_effect=one_collection), \
             patch.object(SSHFetcher, "fetch_limited", return_value=b"stored-result"), \
             patch("sys.argv", cli):
            entry.main()
        manifest = artifact_directory / "manifest.jsonl"
        self.assertTrue(manifest.is_file())
        self.assertEqual(json.loads(manifest.read_text(encoding="utf-8").splitlines()[0])["name"],
                         "results/out.bin")

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
