import hashlib
from dataclasses import replace
from pathlib import Path
import json
import os
import tempfile
import time
import unittest

from runpod_experiments.contracts import (
    AttemptPlan, ManifestFile, PodProvisioningSpec, QuoteObservation,
    ResourceKind, ResourceProfile, StageStatus, TransferDirection,
    TransferManifest, encode_document, sha256_bytes,
)
from runpod_experiments.create_spec import pod_create_request_sha256
from runpod_experiments.errors import ExecutionError
from runpod_experiments.gpu_probe import GPUProbeTransportError
from runpod_experiments.gpu_runtime import parse_nvidia_smi
from runpod_experiments.runpod import ProviderError
from runpod_experiments.state_config import StateStoreConfig
from runpod_experiments.store_transport import (
    load_verified_gpu_attestation, resolve_store_ssh_context,
)
from runpod_experiments.serial_engine import SerialExecutionEngine
from runpod_experiments.serial_engine import AttemptWorkload
from runpod_experiments.supervision import StoreCleanupMonitor
from runpod_experiments.supervisor_process import launch_execution_store

from tests.test_execution_store import ZERO, ONE, plan_for


GOOD_FRAME = (
    b"GPU-a1,NVIDIA A40,46068\n",
    b"NVIDIA-SMI 550.54.14 Driver Version: 550.54.14 CUDA Version: 12.8\n",
)
LOW_VRAM_FRAME = (
    b"GPU-a1,NVIDIA A40,45000\n",
    b"NVIDIA-SMI 550.54.14 Driver Version: 550.54.14 CUDA Version: 12.8\n",
)


class FakeGPUProvider:
    def __init__(self, pod):
        self.detail = dict(pod)
        self.delete_calls = []

    def pods(self):
        if self.detail["status"] == "TERMINATED":
            return []
        return [{"id": self.detail["id"], "name": self.detail["name"],
                 "status": self.detail["status"]}]

    def pod(self, pod_id):
        if pod_id != self.detail["id"]:
            raise ProviderError("missing", status=404)
        return dict(self.detail)

    def _delete_pod(self, pod_id):
        self.delete_calls.append(pod_id)
        self.detail["status"] = "TERMINATED"


class GPUSupervisionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="gpu-supervision-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = int(time.time())
        base = plan_for(self.now)
        self.profile = ResourceProfile(
            ResourceKind.GPU, "NVIDIA A40", 600_000, ("EU-TEST-1",),
            "sha256:" + ZERO, 30, 8, 64, 1, 48, "12.4")
        self.provisioning = PodProvisioningSpec(
            "runpod/gpu@sha256:" + ZERO, ("22/tcp",))
        self.plan = replace(base, resource_options=(self.profile,),
                            provisioning=self.provisioning)
        quote = QuoteObservation(
            ResourceKind.GPU, self.profile.resource_id, 1, 1,
            self.profile.hourly_micro_usd, 0, True, self.profile.region_ids,
            self.now - 1, ONE, self.profile.gpu_memory_gb, "12.4")
        self.attempt = AttemptPlan(
            "gpu-attempt-1", self.plan.job_id, self.plan.approval.approval_id, 1,
            pod_create_request_sha256(
                self.plan.provisioning, self.profile, "EU-TEST-1", "nebula-run-1"),
            sha256_bytes(encode_document("quote_observation", quote)), quote,
            sha256_bytes(encode_document("project_policy", self.plan.project)),
            self.plan.source_sha256, self.plan.config_sha256, self.plan.package_sha256,
            self.profile, "EU-TEST-1", 1200, 60, self.now - 1, self.now + 1800)
        self.state_config = StateStoreConfig(self.root, "TEST-HOST")
        self.store = self.state_config.open_execution_store(
            self.plan.project.project_id, clock=lambda: self.now)
        self.store.reserve_attempt(self.plan, self.attempt, "nebula-run-1")
        self.store.record_create_preflight_checked(
            self.attempt.attempt_id, quote, ZERO,
            self.store.get_attempt(self.attempt.attempt_id)["attempt_sha256"])
        self.store.claim_create(self.attempt.attempt_id)
        self.store.bind_pod(self.attempt.attempt_id, "pod-gpu-1", "nebula-run-1")

        self.supervisor_dir = (
            self.root / self.plan.project.project_id / self.attempt.attempt_id / "supervisor")
        self.supervisor_dir.mkdir(parents=True)
        self.key_file = self.root / "id_ed25519"
        self.key_file.write_bytes(b"synthetic-key-fixture")
        self.known_hosts = self.root / "known_hosts"
        self.known_hosts.write_bytes(b"[gpu.example]:22 ssh-ed25519 AAAA\n")
        self.provider = FakeGPUProvider({
            "id": "pod-gpu-1", "name": "nebula-run-1", "status": "RUNNING",
            "cost": 0.6, "cloud": "SECURE", "image": "runpod/gpu@sha256:" + ZERO,
            "disk": 30, "dataCenterId": "EU-TEST-1", "cudaVersion": "12.8",
            "gpu": {"id": "NVIDIA A40", "count": 1, "vcpuCount": 8, "memory": 64},
            "mounts": {}, "ports": ["22/tcp"],
            "ssh": {"direct": {"host": "gpu.example", "port": 22, "username": "root"}},
        })

    def make_monitor(self, probe):
        return StoreCleanupMonitor(
            self.provider, self.store, self.attempt.attempt_id,
            self.plan.project.project_id, clock=lambda: self.now,
            gpu_ssh_key_file=self.key_file, gpu_known_hosts=self.known_hosts,
            gpu_probe=probe, supervisor_directory=self.supervisor_dir)

    def valid_probe(self, *args, **kwargs):
        observation = parse_nvidia_smi(
            *GOOD_FRAME, observed_epoch=kwargs["observed_epoch"])
        return observation, hashlib.sha256(self.known_hosts.read_bytes()).hexdigest()

    def test_supervisor_probe_binds_evidence_before_ssh_access(self):
        monitor = self.make_monitor(self.valid_probe)
        self.assertEqual(monitor.observe(), "active")
        self.assertEqual(monitor.gpu_runtime_status, "verified")
        verified = load_verified_gpu_attestation(
            self.store, self.attempt.attempt_id, self.known_hosts,
            now_epoch=self.now)
        self.assertEqual(verified.pod_id, "pod-gpu-1")
        target = resolve_store_ssh_context(
            self.provider, self.store, self.attempt.attempt_id,
            self.plan.project.project_id, known_hosts=self.known_hosts)
        self.assertEqual(target.endpoint.host, "gpu.example")
        self.assertEqual(target.endpoint.route, "direct")

    def test_serial_engine_wait_gate_requires_supervisor_bound_evidence(self):
        monitor = self.make_monitor(self.valid_probe)
        self.assertEqual(monitor.observe(), "active")
        policy_sha256 = sha256_bytes(encode_document("project_policy", self.plan.project))
        state = {
            "ready": True, "pid": 123, "attempt_id": self.attempt.attempt_id,
            "project_id": self.plan.project.project_id,
            "attempt_sha256": self.store.get_attempt(self.attempt.attempt_id)["attempt_sha256"],
            "deadline_epoch": self.store.get_attempt(self.attempt.attempt_id)["deadline_epoch"],
            "policy_sha256": policy_sha256, "heartbeat_epoch": self.now,
            "stopping": False, "terminated": False,
        }
        (self.supervisor_dir / "supervisor.json").write_text(
            json.dumps(state), encoding="utf-8")

        class Process:
            pid = 123

            @staticmethod
            def poll():
                return None

        engine = SerialExecutionEngine(self.provider, self.store, self.state_config)
        engine._wait_for_gpu_attestation(
            Process(), self.supervisor_dir, self.store.get_attempt(self.attempt.attempt_id),
            policy_sha256, self.known_hosts)

    def test_gpu_without_supervisor_probe_credentials_keeps_fail_closed_guard(self):
        monitor = StoreCleanupMonitor(
            self.provider, self.store, self.attempt.attempt_id,
            self.plan.project.project_id, clock=lambda: self.now)
        self.assertEqual(monitor.observe(), "delete_requested")
        self.assertEqual(self.provider.delete_calls, ["pod-gpu-1"])

    def test_detached_launcher_passes_gpu_file_paths_without_reading_key_contents(self):
        captured = {}

        def popen(argv, **kwargs):
            captured.update(argv=argv, kwargs=kwargs)
            return object()

        launch_execution_store(
            self.state_config, self.attempt.attempt_id,
            self.plan.project.project_id, self.supervisor_dir,
            gpu_ssh_key_file=self.key_file, gpu_known_hosts=self.known_hosts,
            popen=popen)
        command = " ".join(captured["argv"])
        self.assertIn("--gpu-ssh-key-file", command)
        self.assertIn(os.fspath(self.key_file), command)
        self.assertIn("--gpu-known-hosts", command)
        self.assertIn(os.fspath(self.known_hosts), command)
        self.assertNotIn("synthetic-key-fixture", command)
        self.assertNotIn("ssh-ed25519 AAAA", command)

    def test_changed_known_hosts_policy_blocks_ssh_and_stops_attempt(self):
        self.assertEqual(self.make_monitor(self.valid_probe).observe(), "active")
        self.known_hosts.write_bytes(b"[gpu.example]:22 ssh-ed25519 CHANGED\n")
        with self.assertRaises(ExecutionError):
            resolve_store_ssh_context(
                self.provider, self.store, self.attempt.attempt_id,
                self.plan.project.project_id, known_hosts=self.known_hosts)
        restarted = self.make_monitor(self.valid_probe)
        self.assertEqual(restarted.observe(), "delete_requested")
        self.assertEqual(self.provider.delete_calls, ["pod-gpu-1"])

    def test_complete_fake_gpu_attempt_waits_for_supervisor_proof_before_upload(self):
        case_root = self.root / "engine-case"
        case_root.mkdir()
        state_config = StateStoreConfig(case_root, "TEST-HOST")
        store = state_config.open_execution_store(
            self.plan.project.project_id, clock=lambda: self.now)
        worker = b"print('synthetic GPU workload')\n"
        transfer = TransferManifest((
            ManifestFile("worker.py", sha256_bytes(worker), len(worker),
                         TransferDirection.UPLOAD),
            ManifestFile("result.json", sha256_bytes(b""), 0,
                         TransferDirection.DOWNLOAD),
        ), 4096, 8192)
        plan = replace(self.plan, job_id="gpu-engine-job", transfers=transfer)
        quote = QuoteObservation(
            ResourceKind.GPU, self.profile.resource_id, 1, 1,
            self.profile.hourly_micro_usd, 0, True, self.profile.region_ids,
            self.now - 1, ONE, self.profile.gpu_memory_gb, "12.4")
        attempt = AttemptPlan(
            "gpu-engine-attempt", plan.job_id, plan.approval.approval_id, 1,
            pod_create_request_sha256(
                plan.provisioning, self.profile, "EU-TEST-1", "nebula-gpu-engine"),
            sha256_bytes(encode_document("quote_observation", quote)), quote,
            sha256_bytes(encode_document("project_policy", plan.project)),
            plan.source_sha256, plan.config_sha256, plan.package_sha256,
            self.profile, "EU-TEST-1", 1200, 60, self.now - 1, self.now + 1800)
        store.reserve_attempt(plan, attempt, "nebula-gpu-engine")
        provider = FakeGPUProvider({
            **self.provider.detail, "id": "pod-gpu-engine",
            "name": "nebula-gpu-engine", "status": "RUNNING",
        })
        source_root = case_root / "source"
        source_root.mkdir()
        (source_root / "worker.py").write_bytes(worker)
        key_file = case_root / "id_ed25519"
        key_file.write_bytes(b"synthetic-key-fixture")
        known_hosts = case_root / "known_hosts"
        known_hosts.write_bytes(self.known_hosts.read_bytes())
        artifact_directory = case_root / "artifacts"
        upload_started = []
        controller_errors = []
        supervisor_launches = []
        owner = self

        def supervisor_launcher(_config, attempt_id, project_id, directory, *,
                                gpu_ssh_key_file, gpu_known_hosts):
            supervisor_launches.append((gpu_ssh_key_file, gpu_known_hosts))
            launch_number = len(supervisor_launches)
            monitor = StoreCleanupMonitor(
                provider, store, attempt_id, project_id, clock=lambda: self.now,
                gpu_ssh_key_file=gpu_ssh_key_file, gpu_known_hosts=gpu_known_hosts,
                gpu_probe=self.valid_probe, supervisor_directory=directory)

            class Supervisor:
                pid = 321
                returncode = None

                def poll(self):
                    if self.returncode is not None:
                        return self.returncode
                    if (launch_number == 1 and
                            (Path(directory) / "stop.request").exists()):
                        self.returncode = 1
                        return self.returncode
                    outcome = monitor.observe(
                        stop=(Path(directory) / "stop.request").exists())
                    current = store.get_attempt(attempt_id)
                    state = {
                        "ready": True, "stopping": monitor.stopping,
                        "terminated": outcome == "terminated", "pid": self.pid,
                        "attempt_id": attempt_id, "project_id": project_id,
                        "deadline_epoch": current["deadline_epoch"],
                        "attempt_sha256": current["attempt_sha256"],
                        "policy_sha256": monitor.policy_sha256,
                        "heartbeat_epoch": owner.now,
                    }
                    Path(directory, "supervisor.json").write_text(
                        json.dumps(state), encoding="utf-8")
                    if outcome in ("terminated", "not_created"):
                        self.returncode = 0
                    return self.returncode

            return Supervisor()

        def create_controller_factory(_client, selected_store, _config, **_kwargs):
            class Controller:
                def create(self, attempt_id, supervisor, _directory, _public_key):
                    try:
                        supervisor.poll()
                        selected = selected_store.get_attempt(attempt_id)
                        selected_quote = selected_store.selected_quote_for_attempt(attempt_id)
                        selected_store.record_create_preflight_checked(
                            attempt_id, selected_quote, ZERO, selected["attempt_sha256"])
                        selected_store.claim_create(attempt_id)
                        selected_store.bind_pod(
                            attempt_id, "pod-gpu-engine", "nebula-gpu-engine")
                        supervisor.poll()
                    except Exception as error:
                        controller_errors.append(repr(error))
                        raise

            return Controller()

        class Collector:
            @staticmethod
            def poll():
                return None

            @staticmethod
            def wait(timeout=None):
                return 0

        def uploader(_client, selected_store, attempt_id, project_id, *_args, **_kwargs):
            load_verified_gpu_attestation(
                selected_store, attempt_id, known_hosts, now_epoch=int(time.time()))
            upload_started.append(True)
            return 1

        engine = SerialExecutionEngine(
            provider, store, state_config, supervisor_launcher=supervisor_launcher,
            collector_launcher=lambda *_args, **_kwargs: Collector(),
            create_controller_factory=create_controller_factory, uploader=uploader,
            process_launcher=lambda *_args, **_kwargs: type(
                "Workload", (), {"poll": staticmethod(lambda: 0)})(),
            clock=lambda: self.now, sleep=lambda duration: time.sleep(min(duration, 0.002)))
        workload = AttemptWorkload(
            source_root, key_file, known_hosts, "/work", "worker.py", (), 30,
            ("result.json",), artifact_directory)
        result = engine.run_attempt(
            plan, attempt.attempt_id, workload,
            local_public_key_path=key_file, cleanup_timeout_seconds=5)
        self.assertTrue(upload_started, msg=f"{result.outcome}; {result.stage_events}; "
                        f"attempt={store.get_attempt(attempt.attempt_id)}; "
                        f"controller_errors={controller_errors}")
        self.assertEqual(len(supervisor_launches), 2)
        self.assertEqual(supervisor_launches[1], (key_file, known_hosts))
        self.assertEqual(result.pod_id, "pod-gpu-engine")
        self.assertEqual(store.get_attempt(attempt.attempt_id)["state"], "terminated")
        self.assertEqual(result.outcome.operational_status.value, "succeeded")
        self.assertEqual(result.outcome.cleanup_status.value, "confirmed",
                         msg=f"{result.outcome}; {result.stage_events}; "
                         f"attempt={store.get_attempt(attempt.attempt_id)}")

    def test_insufficient_vram_stops_and_deletes_the_gpu_attempt(self):
        def low_vram(*_args, **kwargs):
            return (parse_nvidia_smi(*LOW_VRAM_FRAME,
                                     observed_epoch=kwargs["observed_epoch"]),
                    hashlib.sha256(self.known_hosts.read_bytes()).hexdigest())

        monitor = self.make_monitor(low_vram)
        self.assertEqual(monitor.observe(), "delete_requested")
        self.assertEqual(monitor.stop_reason, "gpu_runtime_evidence_rejected")
        self.assertEqual(self.provider.delete_calls, ["pod-gpu-1"])
        self.assertEqual(self.store.get_attempt(self.attempt.attempt_id)["state"], "stopping")
        failed = [event for event in self.store.get_stage_events(self.attempt.attempt_id)
                  if event.stage == "gpu_runtime_attestation"][-1]
        self.assertEqual(failed.status, StageStatus.FAILED)
        self.assertEqual(failed.error_code, "gpu_vram_below_profile")

    def test_probe_transport_waits_then_fails_at_durable_grace_deadline(self):
        def unavailable(*_args, **_kwargs):
            raise GPUProbeTransportError("gpu_probe_timeout")

        monitor = self.make_monitor(unavailable)
        self.assertEqual(monitor.observe(), "gpu_runtime_pending")
        events = self.store.get_stage_events(self.attempt.attempt_id)
        grace = [event for event in events if event.stage == "gpu_runtime_grace"]
        self.assertEqual([event.status for event in grace], [StageStatus.STARTED])

        self.now += 300
        restarted = self.make_monitor(unavailable)
        self.assertEqual(restarted.observe(), "delete_requested")
        grace = [event for event in self.store.get_stage_events(self.attempt.attempt_id)
                 if event.stage == "gpu_runtime_grace"]
        self.assertEqual(sum(event.status is StageStatus.STARTED for event in grace), 1)
        self.assertEqual(self.provider.delete_calls, ["pod-gpu-1"])

    def test_supervisor_restart_reuses_first_running_time_and_can_complete_probe(self):
        def unavailable(*_args, **_kwargs):
            raise GPUProbeTransportError("gpu_probe_timeout")

        first = self.make_monitor(unavailable)
        self.assertEqual(first.observe(), "gpu_runtime_pending")
        self.now += 25
        second = self.make_monitor(self.valid_probe)
        self.assertEqual(second.observe(), "active")
        starts = [event for event in self.store.get_stage_events(self.attempt.attempt_id)
                  if event.stage == "gpu_runtime_grace" and
                  event.status is StageStatus.STARTED]
        self.assertEqual(len(starts), 1)
        self.assertEqual(second.gpu_runtime_status, "verified")

    def test_sidecar_tampering_blocks_ssh_and_requests_cleanup(self):
        monitor = self.make_monitor(self.valid_probe)
        self.assertEqual(monitor.observe(), "active")
        sidecar = self.supervisor_dir / "gpu-runtime-attestation.json"
        sidecar.write_bytes(sidecar.read_bytes() + b" ")
        with self.assertRaises(ExecutionError):
            resolve_store_ssh_context(
                self.provider, self.store, self.attempt.attempt_id,
                self.plan.project.project_id, known_hosts=self.known_hosts)

        restarted = self.make_monitor(self.valid_probe)
        self.assertEqual(restarted.observe(), "delete_requested")
        self.assertEqual(self.provider.delete_calls, ["pod-gpu-1"])


if __name__ == "__main__":
    unittest.main()
