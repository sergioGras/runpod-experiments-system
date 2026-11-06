import io
import json
import subprocess
import tempfile
import unittest
import urllib.error
from pathlib import Path

from runpod_experiments.artifacts import ArtifactStore
from runpod_experiments.collection import collect_once
from runpod_experiments.runpod import ProviderError, RunPodClient
from runpod_experiments.ssh import SSHFetcher


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


if __name__ == "__main__":
    unittest.main()
