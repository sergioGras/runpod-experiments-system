import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from runpod_experiments.registry import ProjectRegistry
from runpod_experiments.supervisor_process import launch
from runpod_experiments.planning import runtime_from_remaining, cpu_resource_policy, verify_cpu_quote
from runpod_experiments.resource_policy import ResourcePolicy
from runpod_experiments.attempts import AttemptLedger


class RegistryAndLaunchTests(unittest.TestCase):
    def test_namespaces_cannot_overlap(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "projects.json"
            value = {"schema_version": 1, "projects": {"project-one": {"pod_name_prefixes": ["one-"]},
                                                         "project-two": {"pod_name_prefixes": ["two-"]}}}
            path.write_text(json.dumps(value))
            registry = ProjectRegistry(path)
            self.assertTrue(registry.owns("project-one", "one-run"))
            self.assertFalse(registry.owns("project-two", "one-run"))
            value["projects"]["project-two"]["pod_name_prefixes"] = ["one-sub-"]
            path.write_text(json.dumps(value))
            with self.assertRaises(ValueError):
                ProjectRegistry(path)

    def test_launcher_passes_no_credential_and_uses_detached_process(self):
        with tempfile.TemporaryDirectory() as directory:
            captured = {}
            def popen(argv, **kwargs):
                captured.update(argv=argv, kwargs=kwargs)
                return "process"
            result = launch(Path(directory) / "attempts.sqlite", "attempt-1", "project-one", directory, 12345, Path(directory) / "policy.json", popen=popen)
            self.assertEqual(result, "process")
            self.assertTrue(any("runpod_experiments.supervisor_process" in arg for arg in captured["argv"]))
            self.assertTrue(captured["kwargs"]["close_fds"])
            self.assertNotIn("apikey", " ".join(captured["argv"]))

    def test_runtime_is_derived_from_remaining_cap_and_quote(self):
        self.assertEqual(runtime_from_remaining("2", "1", cleanup_seconds=120),
                         {"work_seconds": 7080, "total_seconds": 7200})
        with self.assertRaises(ValueError):
            runtime_from_remaining("0.01", "1", cleanup_seconds=120)

    def test_cpu_policy_rejects_vcpu_count_outside_catalog_range(self):
        request = {"name": "one-run", "cpu": {"id": "cpu3c", "vcpuCount": 4}, "cloud": "SECURE",
                   "dataCenterIds": ["EU-CZ-1"], "image": "runpod/base@sha256:" + "a" * 64,
                   "disk": 10, "ports": ["22/tcp"], "startSsh": True, "startJupyter": False}
        offer = {"id": "cpu3c", "price": {"securePerVcpu": 0.03}, "ramGbPerVcpu": 2,
                 "vcpu": {"min": 2, "max": 32}, "dataCenters": [{"id": "EU-CZ-1", "availability": "LOW"}]}
        policy = cpu_resource_policy(request, offer, "0.001")
        self.assertEqual(policy["expected_shape"]["cpu"]["memory"], 8)
        self.assertEqual(policy["max_hourly_usd"], 0.121)
        class Client:
            def cpu_offer(self, *_args): return offer
        verify_cpu_quote(Client(), request, ResourcePolicy(policy, "test"), "0.001")
        offer["price"]["securePerVcpu"] = 0.04
        with self.assertRaises(ValueError):
            verify_cpu_quote(Client(), request, ResourcePolicy(policy, "test"), "0.001")
        request["cpu"]["vcpuCount"] = 64
        with self.assertRaises(ValueError):
            cpu_resource_policy(request, offer, "0.001")
        request["cpu"]["vcpuCount"] = 4
        request["dataCenterIds"] = []
        offer["availability"] = "HIGH"
        offer["dataCenters"] = None
        policy = cpu_resource_policy(request, offer, "0.001")
        self.assertNotIn("dataCenterId", policy["expected_shape"])

    def test_supervisor_entrypoint_in_real_local_subprocess(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger_path = root / "runs.sqlite"
            ledger = AttemptLedger(ledger_path)
            ledger.reserve("attempt-1", "project-one", "one-example", "one-")
            ledger.claim_create("attempt-1")
            ledger.bind("attempt-1", "pod-1")
            policy = root / "policy.json"
            policy.write_text(json.dumps({"max_hourly_usd": 1.0, "expected_shape": {"cloud": "SECURE"}, "request_sha256": "0" * 64}))
            source = '''
import sys
sys.path.insert(0, sys.argv[1])
from runpod_experiments.runpod import ProviderError
import runpod_experiments.supervisor_process as entry
class Fake:
    def pods(self): return []
    def pod(self, pod_id): raise ProviderError("missing", 404)
entry.RunPodClient.from_config = lambda: Fake()
original = entry.run_supervisor
entry.run_supervisor = lambda *a, **kw: original(*a, interval=0.01, **kw)
sys.argv = ["supervisor", "--ledger", sys.argv[2], "--attempt", "attempt-1", "--project", "project-one", "--directory", sys.argv[3], "--deadline", "9999999999", "--policy", sys.argv[4]]
entry.main()
'''
            subprocess.run([sys.executable, "-c", source, str(Path.cwd()), str(ledger_path), str(root / "control"), str(policy)],
                           check=True, timeout=30, capture_output=True)
            self.assertEqual(ledger.get("attempt-1")["state"], "terminated")
            self.assertTrue(json.loads((root / "control/supervisor.json").read_text())["terminated"])

    def test_collector_entrypoint_in_real_local_subprocess(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger_path = root / "runs.sqlite"
            ledger = AttemptLedger(ledger_path)
            ledger.reserve("a", "project-one", "one-example", "one-")
            ledger.claim_create("a")
            ledger.bind("a", "pod-1")
            (root / "key").touch()
            (root / "known_hosts").touch()
            (root / "names.json").write_text('["result.json"]')
            source = '''
import sys
sys.path.insert(0, sys.argv[1])
import runpod_experiments.collection_process as entry
from runpod_experiments.ssh import SSHFetcher
class Fake:
    def pod(self, pod_id): return {"id":pod_id,"name":"one-example","status":"RUNNING","ssh":{"direct":{"host":"host","port":22,"username":"root"}}}
entry.RunPodClient.from_config = lambda: Fake()
SSHFetcher.__call__ = lambda self,name: b"checkpoint"
original = entry.collect_loop
entry.collect_loop = lambda *a, **kw: original(*a, max_polls=1, **kw)
sys.argv = ["collector", "--ledger", sys.argv[2], "--attempt", "a", "--project", "project-one", "--directory", sys.argv[3], "--artifact-directory", sys.argv[4], "--names", sys.argv[5], "--key", sys.argv[6], "--known-hosts", sys.argv[7], "--remote-root", "/work"]
entry.main()
'''
            subprocess.run([sys.executable, "-c", source, str(Path.cwd()), str(ledger_path), str(root / "control"),
                str(root / "artifacts"), str(root / "names.json"), str(root / "key"), str(root / "known_hosts")],
                check=True, timeout=30, capture_output=True)
            entry = json.loads((root / "artifacts/manifest.jsonl").read_text())
            self.assertEqual(entry["name"], "result.json")


if __name__ == "__main__":
    unittest.main()
