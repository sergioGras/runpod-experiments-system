import json
import tempfile
import unittest
from pathlib import Path

from runpod_experiments.attempts import AttemptError, AttemptLedger
from runpod_experiments.budget import BudgetLedger
from runpod_experiments.controller import create_attempt, new_pod_name
from runpod_experiments.registry import ProjectRegistry
from runpod_experiments.resource_policy import ResourcePolicy, request_hash


class ControllerTests(unittest.TestCase):
    def test_stale_supervisor_error_names_failed_predicate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry_path = root / "projects.json"
            registry_path.write_text(json.dumps({"schema_version": 1, "projects": {"project-one": {"pod_name_prefixes": ["one-"]}}}))
            registry = ProjectRegistry(registry_path)
            ledger = AttemptLedger(root / "runs.sqlite")
            budget = BudgetLedger(root / "budget.sqlite", "10")
            name = new_pod_name("one-", "test")
            request = {"name": name, "cloud": "SECURE"}
            policy = root / "policy.json"
            policy.write_text(json.dumps({"max_hourly_usd": 1, "expected_shape": {"cloud": "SECURE"},
                                          "request_sha256": request_hash(request)}))
            ledger.reserve_registered("attempt-1", "project-one", name, registry)
            (root / "supervisor.json").write_text(json.dumps({
                "pid": 123, "ready": True, "stopping": False, "terminated": False,
                "attempt_id": "attempt-1", "project_id": "project-one", "deadline_epoch": 1500,
                "heartbeat_epoch": 900, "policy_sha256": ResourcePolicy.read(policy).fingerprint}))
            class Supervisor:
                pid = 123
                def poll(self): return None
            class Provider:
                pass
            ticks = iter((1000, 1001))
            with self.assertRaisesRegex(AttemptError, "heartbeat_stale"):
                create_attempt(Provider(), ledger, registry, budget, "attempt-1", "project-one",
                    request, Supervisor(), root, 1500, policy, quote_validator=lambda *_: None,
                    clock=lambda: next(ticks))

    def test_accepts_supervisor_heartbeat_written_during_quote_check(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry_path = root / "projects.json"
            registry_path.write_text(json.dumps({"schema_version": 1, "projects": {"project-one": {"pod_name_prefixes": ["one-"]}}}))
            registry = ProjectRegistry(registry_path)
            ledger = AttemptLedger(root / "runs.sqlite")
            budget = BudgetLedger(root / "budget.sqlite", "10")
            name = new_pod_name("one-", "test")
            request = {"name": name, "cloud": "SECURE"}
            policy = root / "policy.json"
            policy.write_text(json.dumps({"max_hourly_usd": 1, "expected_shape": {"cloud": "SECURE"},
                                          "request_sha256": request_hash(request)}))
            ledger.reserve_registered("attempt-1", "project-one", name, registry)
            state = {"pid": 123, "ready": True, "stopping": False, "terminated": False,
                     "attempt_id": "attempt-1", "project_id": "project-one", "deadline_epoch": 1500,
                     "heartbeat_epoch": 1000, "policy_sha256": ResourcePolicy.read(policy).fingerprint}
            state_path = root / "supervisor.json"
            state_path.write_text(json.dumps(state))

            class Supervisor:
                pid = 123
                def poll(self): return None

            class Provider:
                def pods(self): return []
                def _create_pod(self, body): return {"id": "pod-1", "name": body["name"]}

            ticks = iter((1000, 1002))
            def quote_validator(*_):
                state["heartbeat_epoch"] = 1001
                state_path.write_text(json.dumps(state))

            result = create_attempt(Provider(), ledger, registry, budget, "attempt-1", "project-one",
                request, Supervisor(), root, 1500, policy, quote_validator=quote_validator,
                clock=lambda: next(ticks), cleanup_reserve_seconds=180)
            self.assertEqual(str(budget.remaining()), "9.811111")
            self.assertEqual(result["id"], "pod-1")
            self.assertEqual(ledger.get("attempt-1")["state"], "active")

    def test_create_timeout_is_claimed_once_and_reconcilable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry_path = root / "projects.json"
            registry_path.write_text(json.dumps({"schema_version": 1, "projects": {"project-one": {"pod_name_prefixes": ["one-"]}}}))
            registry = ProjectRegistry(registry_path)
            ledger = AttemptLedger(root / "runs.sqlite")
            budget = BudgetLedger(root / "budget.sqlite", "10")
            name = new_pod_name("one-", "test")
            ledger.reserve_registered("attempt-1", "project-one", name, registry)
            policy = root / "policy.json"
            policy.write_text(json.dumps({"max_hourly_usd": 1, "expected_shape": {"cloud": "SECURE"}, "request_sha256": request_hash({"name": name})}))
            state = {"pid": 123, "ready": True, "stopping": False, "terminated": False,
                     "attempt_id": "attempt-1", "project_id": "project-one", "deadline_epoch": 1500,
                     "heartbeat_epoch": 1000, "policy_sha256": ResourcePolicy.read(policy).fingerprint}
            (root / "supervisor.json").write_text(json.dumps(state))
            class Supervisor:
                pid = 123
                def poll(self): return None
            class Provider:
                creates = 0
                def pods(self): return [{"id": "foreign", "name": "other-run", "status": "RUNNING"}]
                def _create_pod(self, request):
                    self.creates += 1
                    raise TimeoutError("ambiguous")
            client = Provider()
            args = (client, ledger, registry, budget, "attempt-1", "project-one", {"name": name},
                    Supervisor(), root, 1500, policy)
            altered = list(args)
            altered[6] = {"name": name, "image": "unapproved"}
            with self.assertRaises(AttemptError):
                create_attempt(*altered, quote_validator=lambda *_: None, clock=lambda: 1000)
            self.assertEqual(ledger.get("attempt-1")["state"], "reserved")
            with self.assertRaises(ValueError):
                create_attempt(*args, quote_validator=lambda *_: None, clock=lambda: 1000, cleanup_reserve_seconds=True)
            self.assertEqual(client.creates, 0)
            with self.assertRaises(TimeoutError):
                create_attempt(*args, quote_validator=lambda *_: None, clock=lambda: 1000)
            self.assertEqual(ledger.get("attempt-1")["state"], "creating")
            self.assertEqual(ledger.get("attempt-1")["reconcilable"], 1)
            self.assertLess(budget.remaining(), 10)
            with self.assertRaises(AttemptError):
                create_attempt(*args, quote_validator=lambda *_: None, clock=lambda: 1000)
            self.assertEqual(client.creates, 1)


if __name__ == "__main__":
    unittest.main()
