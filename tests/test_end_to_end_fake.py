"""Exercise the local control chain against a fake provider, never RunPod."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from runpod_experiments.artifacts import ArtifactStore
from runpod_experiments.attempts import AttemptLedger
from runpod_experiments.budget import BudgetLedger
from runpod_experiments.controller import create_attempt, new_pod_name
from runpod_experiments.planning import cpu_resource_policy, verify_cpu_quote
from runpod_experiments.registry import ProjectRegistry
from runpod_experiments.resource_policy import ResourcePolicy
from runpod_experiments.runpod import ProviderError
from runpod_experiments.supervision import CleanupMonitor
from runpod_experiments.workflow import run_owned_workload


class FakeRoundTests(unittest.TestCase):
    def test_single_round_records_results_and_confirmed_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reg_file = root / "registry.json"
            reg_file.write_text(json.dumps({"schema_version": 1, "projects": {"hybrid-inference": {"pod_name_prefixes": ["hybrid-"]}}}))
            registry = ProjectRegistry(reg_file)
            ledger = AttemptLedger(root / "attempts.sqlite")
            budget = BudgetLedger(root / "budget.sqlite", "10")
            name = new_pod_name("hybrid-", "hello")
            ledger.reserve_registered("round-1", "hybrid-inference", name, registry)
            request = {"name": name, "cpu": {"id": "cpu3c", "vcpuCount": 4}, "cloud": "SECURE",
                       "dataCenterIds": ["EU-CZ-1"], "image": "runpod/base@sha256:" + "a" * 64,
                       "disk": 10, "ports": ["22/tcp"], "startSsh": True, "startJupyter": False}
            quote = {"id": "cpu3c", "price": {"securePerVcpu": 0.03}, "ramGbPerVcpu": 2,
                     "vcpu": {"min": 2, "max": 32}, "dataCenters": [{"id": "EU-CZ-1", "availability": "HIGH"}]}
            policy = root / "policy.json"
            policy.write_text(json.dumps(cpu_resource_policy(request, quote, "0.002")))
            (root / "supervisor.json").write_text(json.dumps({"pid": 123, "ready": True, "stopping": False,
                "terminated": False, "attempt_id": "round-1", "project_id": "hybrid-inference",
                "deadline_epoch": 1500, "heartbeat_epoch": 1000, "policy_sha256": ResourcePolicy.read(policy).fingerprint}))
            class Supervisor:
                pid = 123
                def poll(self): return None
            class Provider:
                current = None
                def cpu_offer(self, cpu_id, count): return quote
                def pods(self): return [self.current] if self.current else []
                def pod(self, pod_id):
                    if self.current is None: raise ProviderError("missing", 404)
                    return self.current
                def _create_pod(self, body):
                    self.current = {"id": "pod-1", "name": body["name"], "status": "RUNNING", "cost": 0.12, "mounts": {},
                                    **ResourcePolicy.read(policy).expected_shape}
                    return self.current
                def _delete_pod(self, pod_id): self.current = None
            provider, supervisor = Provider(), Supervisor()
            create_attempt(provider, ledger, registry, budget, "round-1", "hybrid-inference", request,
                           supervisor, root, 1500, policy,
                           quote_validator=lambda client, req, pol: verify_cpu_quote(client, req, pol, "0.002"), clock=lambda: 1000)
            class Process:
                returncode = 0
                def poll(self): return 0
            store = ArtifactStore(root / "artifacts")
            class Collector:
                def poll(self): return None
                def wait(self, timeout): return 0
            def collect(*args, **kwargs):
                store.collect("results/checkpoint.json", b"synthetic result", "round-1")
                return Collector()
            with patch("runpod_experiments.workflow.upload_bundle"), patch("runpod_experiments.workflow.start_python", return_value=Process()), patch("runpod_experiments.workflow.launch_collector", side_effect=collect):
                result = run_owned_workload(provider, ledger, "round-1", "hybrid-inference", store,
                    source_root=root, manifest={"run.py": "hash"}, key_file="key", known_hosts="hosts", remote_root="/work",
                    script="run.py", arguments=[], timeout_seconds=60, result_names=["results/checkpoint.json"],
                    control_directory=root, supervisor=supervisor)
            self.assertEqual(result["exit_code"], 0)
            self.assertTrue((root / "stop.request").exists())
            self.assertTrue((root / "artifacts/manifest.jsonl").exists())
            monitor = CleanupMonitor(provider, ledger, "round-1", "hybrid-inference", ResourcePolicy.read(policy))
            self.assertEqual(monitor.observe(stop=True), "delete_requested")
            self.assertEqual([monitor.observe() for _ in range(3)][-1], "terminated")
            budget.settle("round-1", "provider-charge-1", "0.02")
            self.assertEqual(str(budget.remaining()), "9.98")


if __name__ == "__main__":
    unittest.main()
