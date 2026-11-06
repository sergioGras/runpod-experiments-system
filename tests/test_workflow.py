import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from runpod_experiments.workflow import run_owned_workload, reconcile_cleanup
from runpod_experiments.attempts import AttemptLedger
from runpod_experiments.runpod import ProviderError


class WorkflowTests(unittest.TestCase):
    def test_failed_upload_still_requests_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch("runpod_experiments.workflow.upload_bundle", side_effect=RuntimeError("failure")):
                with self.assertRaises(RuntimeError):
                    run_owned_workload(None, None, "attempt", "project", None, source_root=directory,
                        manifest={"run.py": "hash"}, key_file="key", known_hosts="hosts", remote_root="/work",
                        script="run.py", arguments=[], timeout_seconds=10, result_names=[],
                        control_directory=directory, supervisor=None)
            self.assertTrue((Path(directory) / "stop.request").exists())

    def test_cleanup_retries_and_confirms_provider_absence(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = AttemptLedger(Path(directory) / "runs.sqlite")
            ledger.reserve("a", "hybrid-inference", "hybrid-test", "hybrid-")
            ledger.claim_create("a")
            ledger.bind("a", "pod-1")
            class Client:
                fails = 1
                def pods(self):
                    if self.fails:
                        self.fails -= 1
                        raise ProviderError("temporary")
                    return []
                def pod(self, pod_id): raise ProviderError("not found", 404)
            now = [0]
            result = reconcile_cleanup(Client(), ledger, "a", "hybrid-inference", directory,
                max_seconds=10, interval=1, clock=lambda: now[0], sleep=lambda n: now.__setitem__(0, now[0] + n))
            self.assertTrue(result["terminated"])
            self.assertEqual(result["last_error_type"], "ProviderError")


if __name__ == "__main__":
    unittest.main()
