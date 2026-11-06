import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from pathlib import Path

from runpod_experiments.attempts import AttemptError, AttemptLedger, reconcile_candidate
from runpod_experiments.ownership import delete_attempt
from runpod_experiments.supervision import CleanupMonitor, run_supervisor
from runpod_experiments.runpod import ProviderError
from runpod_experiments.resource_policy import ResourcePolicy


class AttemptTests(unittest.TestCase):
    def test_ambiguous_create_is_not_retried_and_candidate_requires_unique_name(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = AttemptLedger(Path(directory) / "runs.sqlite")
            ledger.reserve("attempt-1", "hybrid-inference", "hybrid-example-123", "hybrid-")
            ledger.claim_create("attempt-1")
            with self.assertRaises(AttemptError):
                ledger.claim_create("attempt-1")
            self.assertIsNone(reconcile_candidate(ledger, "attempt-1", []))
            candidate = {"name": "hybrid-example-123", "id": "pod-1", "status": "RUNNING"}
            with self.assertRaises(AttemptError):
                reconcile_candidate(ledger, "attempt-1", [candidate, candidate])
            self.assertEqual(reconcile_candidate(ledger, "attempt-1", [candidate]), "pod-1")
            ledger.bind("attempt-1", "pod-1")
            with self.assertRaises(AttemptError):
                ledger.bind("attempt-1", "pod-2")
            self.assertEqual(AttemptLedger(ledger.path).get("attempt-1")["pod_id"], "pod-1")
            ledger.terminated("attempt-1", "pod-1")
            self.assertEqual(ledger.get("attempt-1")["state"], "terminated")
            with closing(sqlite3.connect(ledger.path)) as db:
                self.assertEqual([r[0] for r in db.execute("SELECT event FROM events ORDER BY sequence")],
                                 ["reserved", "create_claimed", "bound", "terminated"])

    def test_concurrent_create_claim_has_one_winner(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = AttemptLedger(Path(directory) / "runs.sqlite")
            ledger.reserve("attempt-1", "hybrid-inference", "hybrid-example-123", "hybrid-")
            results = []
            def claim():
                try:
                    ledger.claim_create("attempt-1")
                    results.append("claimed")
                except AttemptError:
                    results.append("refused")
            threads = [threading.Thread(target=claim) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertCountEqual(results, ["claimed", "refused"])

    def test_delete_requires_live_exact_identity_and_project(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = AttemptLedger(Path(directory) / "runs.sqlite")
            ledger.reserve("attempt-1", "hybrid-inference", "hybrid-example-123", "hybrid-")
            ledger.claim_create("attempt-1")
            ledger.bind("attempt-1", "pod-1")
            class FakeClient:
                name = "foreign-pod"
                deleted = []
                def pod(self, pod_id):
                    return {"id": pod_id, "name": self.name}
                def _delete_pod(self, pod_id):
                    self.deleted.append(pod_id)
            client = FakeClient()
            with self.assertRaises(AttemptError):
                delete_attempt(client, ledger, "attempt-1", "hybrid-inference")
            client.name = "hybrid-example-123"
            with self.assertRaises(AttemptError):
                delete_attempt(client, ledger, "attempt-1", "miap-protocol")
            self.assertEqual(client.deleted, [])
            delete_attempt(client, ledger, "attempt-1", "hybrid-inference")
            self.assertEqual(client.deleted, ["pod-1"])
            self.assertEqual(ledger.get("attempt-1")["state"], "active")

    def test_cleanup_requires_three_confirmed_absences_and_retries_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = AttemptLedger(Path(directory) / "runs.sqlite")
            ledger.reserve("attempt-1", "hybrid-inference", "hybrid-example-123", "hybrid-")
            ledger.claim_create("attempt-1")
            ledger.bind("attempt-1", "pod-1")
            class FakeClient:
                exists = True
                broken = False
                def pods(self):
                    if self.broken:
                        raise ProviderError("timeout")
                    return [{"id": "pod-1", "name": "hybrid-example-123", "status": "RUNNING"}] if self.exists else []
                def pod(self, pod_id):
                    if not self.exists:
                        raise ProviderError("not found", 404)
                    return {"id": pod_id, "name": "hybrid-example-123"}
                def _delete_pod(self, pod_id):
                    self.exists = False
            client = FakeClient()
            monitor = CleanupMonitor(client, ledger, "attempt-1", "hybrid-inference")
            self.assertEqual(monitor.observe(stop=True), "delete_requested")
            self.assertEqual(monitor.observe(), "confirming_absence")
            client.broken = True
            with self.assertRaises(ProviderError):
                monitor.observe()
            client.broken = False
            self.assertEqual(monitor.observe(), "confirming_absence")
            self.assertEqual(monitor.observe(), "confirming_absence")
            self.assertEqual(monitor.observe(), "terminated")
            self.assertEqual(ledger.get("attempt-1")["state"], "terminated")

    def test_supervisor_latches_deadline_and_persists_heartbeat(self):
        with tempfile.TemporaryDirectory() as directory:
            class FakeClient:
                def pods(self):
                    return []
            class FakeMonitor:
                client = FakeClient()
                attempt_id = "attempt-1"
                project_id = "project-one"
                policy = None
                stopping = False
                calls = []
                def observe(self, stop=False):
                    self.calls.append(stop)
                    return "terminated" if len(self.calls) == 2 else "active"
            monitor = FakeMonitor()
            ticks = iter([100, 100, 110, 110])
            state = run_supervisor(monitor, directory, 105, interval=1,
                clock=lambda: next(ticks), sleep=lambda _: None, max_polls=2)
            self.assertEqual(monitor.calls, [False, True])
            self.assertTrue(state["terminated"])
            self.assertTrue((Path(directory) / "stop.request").exists())
            self.assertTrue((Path(directory) / "supervisor.json").exists())

    def test_supervisor_reconciles_claimed_unique_create_after_controller_loss(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = AttemptLedger(Path(directory) / "runs.sqlite")
            name = "hybrid-test-" + "a" * 32
            ledger.reserve("attempt-1", "hybrid-inference", name, "hybrid-")
            ledger.claim_reconcilable_create("attempt-1", [])
            class FakeClient:
                def pods(self):
                    return [{"id": "pod-1", "name": name, "status": "RUNNING"}]
                def pod(self, pod_id):
                    return {"id": pod_id, "name": name, "status": "RUNNING"}
                def _delete_pod(self, pod_id):
                    self.deleted = pod_id
            client = FakeClient()
            monitor = CleanupMonitor(client, ledger, "attempt-1", "hybrid-inference")
            self.assertEqual(monitor.observe(stop=True), "delete_requested")
            self.assertEqual(ledger.get("attempt-1")["pod_id"], "pod-1")
            self.assertEqual(client.deleted, "pod-1")

    def test_instance_limit_counts_unresolved_creates_in_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = AttemptLedger(Path(directory) / "runs.sqlite")
            ledger.reserve("a", "hybrid-inference", "hybrid-test-" + "a" * 32, "hybrid-")
            ledger.reserve("b", "hybrid-inference", "hybrid-test-" + "b" * 32, "hybrid-")
            ledger.claim_reconcilable_create("a", [])
            with self.assertRaises(AttemptError):
                ledger.claim_reconcilable_create("b", [])

    def test_price_or_shape_violation_triggers_exact_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = AttemptLedger(Path(directory) / "runs.sqlite")
            ledger.reserve("a", "hybrid-inference", "hybrid-test-one", "hybrid-")
            ledger.claim_create("a")
            ledger.bind("a", "pod-1")
            class Client:
                deleted = False
                def pods(self): return [{"id": "pod-1", "name": "hybrid-test-one", "status": "RUNNING"}]
                def pod(self, pod_id): return {"id": pod_id, "name": "hybrid-test-one", "status": "RUNNING", "cost": 3, "cloud": "SECURE"}
                def _delete_pod(self, pod_id): self.deleted = True
            client = Client()
            policy = ResourcePolicy({"max_hourly_usd": 1, "expected_shape": {"cloud": "SECURE"}}, "hash")
            self.assertEqual(CleanupMonitor(client, ledger, "a", "hybrid-inference", policy).observe(), "delete_requested")
            self.assertTrue(client.deleted)


if __name__ == "__main__":
    unittest.main()
