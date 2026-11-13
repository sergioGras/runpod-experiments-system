import base64
import io
import hashlib
import json
import subprocess
import struct
import tempfile
import unittest
import urllib.parse
import urllib.error
from pathlib import Path

from runpod_experiments.artifacts import ArtifactStore
from runpod_experiments.collection import collect_once
from runpod_experiments.runpod import ProviderError, RunPodClient
from runpod_experiments.ssh import SSHFetcher, bootstrap_tofu, ssh_public_key_fingerprint
from runpod_experiments.attempts import AttemptLedger, AttemptError
from runpod_experiments.run_collection import collect_owned_once
from runpod_experiments.upload import upload_bundle
from upload_test_support import direct_upload_runner
from runpod_experiments.remote_process import start_python


def pod_billing_response(pod_id, records, start_time="2026-11-11T08:00:00Z",
                         end_time="2026-11-11T10:00:00Z", bucket_size="hour"):
    return {"records": records, "metadata": {
        "query": {"podId": pod_id, "bucketSize": bucket_size,
                  "startTime": start_time, "endTime": end_time},
        "recordCount": len(records), "uniquePodCount": int(bool(records)),
        "totals": {"totalAmount": sum(record.get("totalAmount", 0) for record in records)},
    }}


class ConnectionTests(unittest.TestCase):
    def test_collection_applies_per_file_and_aggregate_limits_before_storage(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArtifactStore(Path(directory) / "results")
            requested = []

            class Fetch:
                def __call__(self, name):
                    return self.fetch_limited(name, 8)

                def fetch_limited(self, name, max_bytes):
                    requested.append((name, max_bytes))
                    return name.encode()[:max_bytes]

            previous, errors = collect_once(
                store, Fetch(), ["a.bin", "long-name.bin"], "attempt-1",
                max_file_bytes=8, max_total_bytes=10,
                per_file_limits={"a.bin": 2, "long-name.bin": 8})
            self.assertEqual(errors, {})
            self.assertEqual([limit for _name, limit in requested], [2, 8])
            self.assertEqual(set(previous), {"a.bin", "long-name.bin"})
            entries = [json.loads(line) for line in
                       (Path(directory) / "results" / "manifest.jsonl").read_text().splitlines()]
            self.assertEqual([entry["bytes"] for entry in entries], [2, 8])

            aggregate_store = ArtifactStore(Path(directory) / "aggregate-results")
            aggregate_requested = []

            class AggregateFetch:
                def fetch_limited(self, name, max_bytes):
                    aggregate_requested.append((name, max_bytes))
                    return name.encode()[:max_bytes]

            _previous, aggregate_errors = collect_once(
                aggregate_store, AggregateFetch(), ["one", "two"], "attempt-2",
                max_file_bytes=8, max_total_bytes=5)
            self.assertEqual(aggregate_errors, {})
            self.assertEqual([limit for _name, limit in aggregate_requested], [5, 2])

            empty_store = ArtifactStore(Path(directory) / "empty-results")
            empty_previous, empty_errors = collect_once(
                empty_store, lambda _name: b"", ["empty.bin"], "attempt-3",
                max_file_bytes=8, max_total_bytes=1,
                per_file_limits={"empty.bin": 0})
            self.assertEqual(empty_errors, {})
            self.assertEqual(empty_previous["empty.bin"], hashlib.sha256(b"").hexdigest())

    def test_collection_rejects_unbounded_fallback_fetch_result(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArtifactStore(Path(directory) / "results")
            previous, errors = collect_once(
                store, lambda _name: b"too large", ["result.bin"], "attempt-1",
                max_file_bytes=3, max_total_bytes=3)
            self.assertEqual(previous, {})
            self.assertEqual(errors, {"result.bin": "ValueError"})

    def test_exact_pod_billing_is_a_read_only_validated_observation(self):
        records = [{"podId": "pod-123", "startTime": "2026-11-11T08:00:00Z",
                    "endTime": "2026-11-11T09:00:00Z", "totalAmount": 0.11,
                    "gpuAmount": 0.11, "cpuAmount": 0, "diskAmount": 0}]
        response = pod_billing_response("pod-123", records)
        calls = []
        def opener(request, timeout):
            calls.append(request)
            return io.BytesIO(json.dumps(response).encode())
        client = RunPodClient("private-token", opener=opener)
        result = client.pod_billing("pod-123", start_time="2026-11-11T08:00:00Z",
            end_time="2026-11-11T10:00:00Z")
        self.assertEqual(result, response)
        self.assertEqual(calls[0].get_method(), "GET")
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(calls[0].full_url).query)
        self.assertEqual(urllib.parse.urlsplit(calls[0].full_url).netloc, "api.runpod.io")
        self.assertEqual(urllib.parse.urlsplit(calls[0].full_url).path, "/v2/billing/pods")
        self.assertEqual(query, {"podId": ["pod-123"],
            "bucketSize": ["hour"],
            "startTime": ["2026-11-11T08:00:00Z"], "endTime": ["2026-11-11T10:00:00Z"]})
        self.assertNotIn("private-token", calls[0].full_url)

    def test_pod_billing_rejects_unpaired_times_bad_identifiers_and_foreign_rows(self):
        calls = []
        def opener(request, timeout):
            calls.append(request)
            record = {"podId": "other", "startTime": "2026-11-11T08:00:00Z",
                      "endTime": "2026-11-11T09:00:00Z", "totalAmount": 0}
            return io.BytesIO(json.dumps(pod_billing_response("pod-1", [record])).encode())
        client = RunPodClient("test-key", opener=opener)
        with self.assertRaises(ValueError):
            client.pod_billing("../pod")
        with self.assertRaises(ValueError):
            client.pod_billing("pod-1", start_time="2026-11-11T08:00:00Z")
        with self.assertRaises(ValueError):
            client.pod_billing("pod-1", start_time="2026-11-11T08:00:00",
                end_time="2026-11-11T09:00:00")
        with self.assertRaises(ValueError):
            client.pod_billing("pod-1", start_time="2026-11-11T09:00:00Z",
                end_time="2026-11-11T08:00:00Z")
        self.assertFalse(calls)
        with self.assertRaises(ValueError):
            client.pod_billing("pod-1")
        foreign_record = {"podId": "other", "startTime": "2026-11-11T08:00:00Z",
                          "endTime": "2026-11-11T09:00:00Z", "totalAmount": 0}
        client._opener = lambda request, timeout: io.BytesIO(json.dumps(
            pod_billing_response("pod-1", [foreign_record])).encode())
        with self.assertRaisesRegex(ProviderError, "unexpected identity"):
            client.pod_billing("pod-1", start_time="2026-11-11T08:00:00Z",
                               end_time="2026-11-11T10:00:00Z")

    def test_pod_billing_rejects_missing_amount_and_overlapping_periods(self):
        response = pod_billing_response("pod-1", [{"podId": "pod-1",
            "startTime": "2026-11-11T08:00:00Z", "endTime": "2026-11-11T09:00:00Z"}])
        client = RunPodClient("test-key", opener=lambda *_args, **_kwargs:
                              io.BytesIO(json.dumps(response).encode()))
        with self.assertRaisesRegex(ProviderError, "invalid amount"):
            client.pod_billing("pod-1", start_time="2026-11-11T08:00:00Z",
                               end_time="2026-11-11T10:00:00Z")

        response["records"][0]["totalAmount"] = 0
        response["records"].append(dict(response["records"][0]))
        response["metadata"]["recordCount"] = 2
        with self.assertRaisesRegex(ProviderError, "overlapping periods"):
            client.pod_billing("pod-1", start_time="2026-11-11T08:00:00Z",
                               end_time="2026-11-11T10:00:00Z")

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

    def test_ssh_key_lookup_returns_only_hashes_of_registered_public_keys(self):
        key_blob = struct.pack(">I", len(b"ssh-ed25519")) + b"ssh-ed25519" + bytes(range(32))
        key_line = "ssh-ed25519 " + base64.b64encode(key_blob).decode("ascii") + " fixture-key"
        calls = []

        def opener(request, timeout):
            calls.append(request)
            return io.BytesIO(json.dumps({"keys": [key_line]}).encode())

        client = RunPodClient("test-key", opener=opener)
        fingerprints = client.ssh_key_fingerprints()
        self.assertEqual(fingerprints, frozenset({ssh_public_key_fingerprint(key_line)}))
        self.assertEqual(calls[0].get_method(), "GET")
        self.assertEqual(urllib.parse.urlsplit(calls[0].full_url).path, "/v2/account/ssh-keys")
        self.assertNotIn(key_line, repr(fingerprints))

        client._opener = lambda *_args, **_kwargs: io.BytesIO(b'{"keys":["malformed"]}')
        with self.assertRaises(ProviderError):
            client.ssh_key_fingerprints()

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
            runner = direct_upload_runner(calls)
            manifest = {"one.py": hashlib.sha256(b"source").hexdigest(), "two.py": hashlib.sha256(b"old").hexdigest()}
            with self.assertRaises(AttemptError):
                upload_bundle(Client(), ledger, "a", "hyybrid-inference", root, manifest, root / "key", root / "hosts", "/work", runner=runner)
            with self.assertRaises(ValueError):
                upload_bundle(Client(), ledger, "a", "hybrid-inference", root, manifest, root / "key", root / "hosts", "/work", runner=runner)
            self.assertFalse(calls)
            manifest["two.py"] = hashlib.sha256(b"changed").hexdigest()
            self.assertEqual(upload_bundle(Client(), ledger, "a", "hybrid-inference", root, manifest, root / "key", root / "hosts", "/work", runner=runner), 2)
            self.assertEqual(len(calls), 6)

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
