import json
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from examples import remote_hello, remote_hello_runner
from runpod_experiments.resource_policy import ResourcePolicy


class RunnerTests(unittest.TestCase):
    def test_round_envelope_persists_across_retries(self):
        with tempfile.TemporaryDirectory() as directory:
            initial = remote_hello_runner.round_metadata(directory, "10", "approval", clock=lambda: 100)
            resumed = remote_hello_runner.round_metadata(directory, "10.00", "approval", clock=lambda: 200)
            self.assertEqual(initial["started_epoch"], resumed["started_epoch"])
            self.assertEqual(resumed["max_runtime_seconds"], 1800)
            with self.assertRaises(ValueError):
                remote_hello_runner.round_metadata(directory, "11", "approval", clock=lambda: 200)

    def test_full_driver_with_synthetic_provider(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry = root / "registry.json"
            registry.write_text(json.dumps({"schema_version": 1, "projects": {"hybrid-inference": {"pod_name_prefixes": ["hybrid-"]}}}))
            class Provider:
                current = None
                def cpu_offer(self, *_args):
                    return {"id": "cpu3c", "price": {"securePerVcpu": 0.03}, "ramGbPerVcpu": 2,
                            "vcpu": {"min": 2, "max": 32}, "dataCenters": [{"id": "EU-CZ-1", "availability": "HIGH"}]}
                def pods(self): return [self.current] if self.current else []
                def _create_pod(self, request):
                    self.current = {"id": "pod-1", "name": request["name"], "status": "RUNNING",
                        "ssh": {"direct": {"host": "host", "port": 22, "username": "root"}}}
                    return self.current
                def pod(self, pod_id): return self.current
            class Supervisor:
                pid = 123
                def poll(self): return None
            def fake_launch(ledger_path, attempt_id, project, control, deadline, policy):
                Path(control, "supervisor.json").write_text(json.dumps({"pid": 123, "ready": True, "stopping": False,
                    "terminated": False, "attempt_id": attempt_id, "project_id": project,
                    "deadline_epoch": deadline, "heartbeat_epoch": time.time(),
                    "policy_sha256": ResourcePolicy.read(policy).fingerprint}))
                return Supervisor()
            def fake_workload(client, ledger, attempt, project, store, **kwargs):
                output = root / "synthetic"
                remote_hello.run(output)
                store.collect("results/checkpoint.json", (output / "checkpoint.json").read_bytes(), attempt)
                return {"exit_code": 0}
            def fake_git(argv, **kwargs):
                return subprocess.CompletedProcess(argv, 0, stdout=("" if "status" in argv else "a" * 40))
            with patch.object(remote_hello_runner.RunPodClient, "from_config", return_value=Provider()), \
                 patch.object(remote_hello_runner.subprocess, "run", side_effect=fake_git), \
                 patch.object(remote_hello_runner, "launch", side_effect=fake_launch), \
                 patch.object(remote_hello_runner, "bootstrap_tofu"), \
                 patch.object(remote_hello_runner, "run_owned_workload", side_effect=fake_workload), \
                 patch.object(remote_hello_runner, "reconcile_cleanup", return_value={"terminated": True}):
                result = remote_hello_runner.run(root / "round", registry, "10", "test-approval", root / "coordination")
            self.assertIsNone(result["failure_type"])
            self.assertTrue(result["cleanup"]["terminated"])
            self.assertTrue(result["billing_reconciliation_pending"])


if __name__ == "__main__":
    unittest.main()
