import io
import hashlib
import json
import subprocess
import tempfile
import unittest
import urllib.error
from pathlib import Path

from runpod_experiments.artifacts import ArtifactStore
from runpod_experiments.collection import collect_once
from runpod_experiments.runpod import ProviderError, RunPodClient
from runpod_experiments.ssh import SSHFetcher, bootstrap_tofu
from runpod_experiments.attempts import AttemptLedger, AttemptError
from runpod_experiments.run_collection import collect_owned_once
from runpod_experiments.upload import upload_bundle
from runpod_experiments.remote_process import start_python


class ConnectionTests(unittest.TestCase):
    def test_inventory_pagination_and_cursor_loop(self):
        pages = iter([{"pods": [{"id": "a"}], "pagination": {"hasNextPage": True, "nextCursor": "next"}},
                      {"pods": [{"id": "b"}], "pagination": {"hasNextPage": False}}])
        requests = []
        def opener(request, timeout):
            requests.append(request.full_url)
            return io.BytesIO(json.dumps(next(pages)).encode())
        client = RunPodClient("private-token", opener=opener)
        self.assertEqual([p["id"] for p in client.pods()], ["a", "b"])
        self.assertTrue(requests[1].endswith("?cursor=next"))
        client._opener = lambda request, timeout: io.BytesIO(b'{"pods":[],"pagination":{"hasNextPage":true,"nextCursor":"same"}}')
        with self.assertRaises(ProviderError):
            client.pods()

    def test_provider_errors_do_not_include_credentials_or_body(self):
        def opener(request, timeout):
            raise urllib.error.HTTPError(request.full_url, 403, "secret-body", {}, None)
        with self.assertRaises(ProviderError) as caught:
            RunPodClient("private-token", opener=opener).pod("pod-1")
        self.assertEqual(caught.exception.status, 403)
        self.assertNotIn("secret", str(caught.exception))
        self.assertNotIn("private-token", str(caught.exception))

    def test_cpu_catalog_request_is_read_only_and_url_encoded(self):
        calls = []
        def opener(request, timeout):
            calls.append(request)
            return io.BytesIO(b'{"price":{"securePerVcpu":0.03}}')
        offer = RunPodClient("test-key", opener=opener).cpu_offer("cpu3c", 4)
        self.assertEqual(offer["price"]["securePerVcpu"], 0.03)
        self.assertEqual(calls[0].get_method(), "GET")
        self.assertIn("vcpuCount=4", calls[0].full_url)
        with self.assertRaises(ValueError):
            RunPodClient("test-key", opener=opener).cpu_offer("../pod", 4)

    def test_ssh_collection_retries_and_preserves_versions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "key").touch()
            (root / "known_hosts").touch()
            calls = []
            def runner(argv, **kwargs):
                calls.append(argv)
                if len(calls) == 1:
                    raise subprocess.TimeoutExpired("ssh", 1)
                return subprocess.CompletedProcess(argv, 0, stdout=b"partial" if len(calls) == 2 else b"complete")
            fetch = SSHFetcher("host.example", 22, "root", root / "key", root / "known_hosts", "/work", runner=runner)
            store = ArtifactStore(root / "results")
            state, errors = collect_once(store, fetch, ["result.json"], "attempt-1")
            self.assertEqual(errors, {"result.json": "RuntimeError"})
            state, errors = collect_once(store, fetch, ["result.json"], "attempt-1", state)
            state, errors = collect_once(store, fetch, ["result.json"], "attempt-1", state)
            self.assertFalse(errors)
            self.assertEqual(len((root / "results/manifest.jsonl").read_text().splitlines()), 2)
            self.assertIn("StrictHostKeyChecking=yes", calls[0])
            with self.assertRaises(ValueError):
                fetch("../secret")

    def test_collection_requires_the_bound_project_and_live_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = AttemptLedger(root / "runs.sqlite")
            ledger.reserve("a", "hybrid-inference", "hybrid-test", "hybrid-")
            ledger.claim_create("a")
            ledger.bind("a", "pod-1")
            class Client:
                name = "foreign"
                def pod(self, pod_id):
                    return {"id": pod_id, "name": self.name, "status": "RUNNING", "ssh": {"direct": {"host": "host", "port": 22, "username": "root"}}}
            class Fetch:
                def __init__(self, *args): pass
                def __call__(self, name): return b"result"
            client = Client()
            store = ArtifactStore(root / "results")
            with self.assertRaises(AttemptError):
                collect_owned_once(client, ledger, "a", "hybrid-inference", store, ["result.json"], "key", "hosts", "/work", fetcher_type=Fetch)
            client.name = "hybrid-test"
            state, errors = collect_owned_once(client, ledger, "a", "hybrid-inference", store, ["result.json"], "key", "hosts", "/work", fetcher_type=Fetch)
            self.assertFalse(errors)
            self.assertIn("result.json", state)

    def test_upload_checks_full_manifest_before_sending_any_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "key").touch()
            (root / "hosts").touch()
            (root / "one.py").write_bytes(b"source")
            (root / "two.py").write_bytes(b"changed")
            ledger = AttemptLedger(root / "runs.sqlite")
            ledger.reserve("a", "hybrid-inference", "hybrid-test", "hybrid-")
            ledger.claim_create("a")
            ledger.bind("a", "pod-1")
            class Client:
                def pod(self, pod_id):
                    return {"id": pod_id, "name": "hybrid-test", "status": "RUNNING", "ssh": {"direct": {"host": "host", "port": 22, "username": "root"}}}
            calls = []
            def runner(*args, **kwargs):
                calls.append((args, kwargs))
            manifest = {"one.py": hashlib.sha256(b"source").hexdigest(), "two.py": hashlib.sha256(b"old").hexdigest()}
            with self.assertRaises(AttemptError):
                upload_bundle(Client(), ledger, "a", "hyybrid-inference", root, manifest, root / "key", root / "hosts", "/work", runner=runner)
            with self.assertRaises(ValueError):
                upload_bundle(Client(), ledger, "a", "hybrid-inference", root, manifest, root / "key", root / "hosts", "/work", runner=runner)
            self.assertFalse(calls)
            manifest["two.py"] = hashlib.sha256(b"changed").hexdigest()
            self.assertEqual(upload_bundle(Client(), ledger, "a", "hybrid-inference", root, manifest, root / "key", root / "hosts", "/work", runner=runner), 2)
            self.assertEqual(len(calls), 2)

    def test_remote_execution_quotes_arguments_and_checks_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "key").touch()
            (root / "hosts").touch()
            ledger = AttemptLedger(root / "runs.sqlite")
            ledger.reserve("a", "hybrid-inference", "hybrid-test", "hybrid-")
            ledger.claim_create("a")
            ledger.bind("a", "pod-1")
            class Client:
                def pod(self, pod_id):
                    return {"id": pod_id, "name": "hybrid-test", "status": "RUNNING", "ssh": {"direct": {"host": "host", "port": 22, "username": "root"}}}
            calls = []
            def popen(argv, **kwargs):
                calls.append(argv)
                return "process"
            self.assertEqual(start_python(Client(), ledger, "a", "hybrid-inference", root / "key", root / "hosts", "/work", "run.py", ["a; echo BAD"], 60, root / "log", popen=popen), "process")
            self.assertIn("'a; echo BAD'", calls[0][-1])
            with self.assertRaises(ValueError):
                start_python(Client(), ledger, "a", "hybrid-inference", root / "key", root / "hosts", "/work", "../run.py", [], 60, root / "log", popen=popen)

    def test_host_bootstrap_only_accepts_matching_running_pod(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "key").touch()
            ledger = AttemptLedger(root / "runs.sqlite")
            ledger.reserve("a", "hybrid-inference", "hybrid-test", "hybrid-")
            ledger.claim_create("a")
            ledger.bind("a", "pod-1")
            class Client:
                status = "EXITED"
                def pod(self, pod_id):
                    return {"id": pod_id, "name": "hybrid-test", "status": self.status,
                            "ssh": {"direct": {"host": "host", "port": 22, "username": "root"}}}
            client = Client()
            calls = []
            def runner(argv, **kwargs): calls.append(argv)
            with self.assertRaises(AttemptError):
                bootstrap_tofu(client, ledger, "a", "hybrid-inference", root / "key", root / "hosts", runner=runner)
            client.status = "RUNNING"
            bootstrap_tofu(client, ledger, "a", "hybrid-inference", root / "key", root / "hosts", runner=runner)
            self.assertIn("StrictHostKeyChecking=accept-new", calls[0])
            self.assertTrue((root / "hosts").exists())


if __name__ == "__main__":
    unittest.main()
