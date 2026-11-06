import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from runpod_experiments.artifacts import ArtifactStore
from runpod_experiments.budget import BudgetLedger


class FoundationsTests(unittest.TestCase):
    def test_accounting_survives_restart_and_deduplicates_attempt_costs(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "round.sqlite"
            ledger = BudgetLedger(path, "10")
            ledger.record_charge("attempt-1", "1.25", "setup and compute")
            ledger.record_charge("attempt-1", "1.25", "setup and compute")
            ledger = BudgetLedger(path, "10")
            self.assertEqual(ledger.remaining("0.75"), Decimal("8"))
            with self.assertRaises(ValueError):
                ledger.record_charge("attempt-1", "2", "setup and compute")
            with self.assertRaises(ValueError):
                BudgetLedger(path, "11")
            ledger.record_charge("late-billing", "15")
            self.assertEqual(ledger.remaining(), Decimal("0"))

    def test_artifacts_preserve_versions_across_attempts(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArtifactStore(directory)
            first = store.collect("results/partial.json", b"first", "attempt-1")
            second = store.collect("results/partial.json", b"second", "attempt-2")
            self.assertNotEqual(first["sha256"], second["sha256"])
            self.assertEqual((Path(directory) / "objects" / first["sha256"]).read_bytes(), b"first")
            entries = [json.loads(line) for line in (Path(directory) / "manifest.jsonl").read_text().splitlines()]
            self.assertEqual([entry["attempt_id"] for entry in entries], ["attempt-1", "attempt-2"])
            with self.assertRaises(ValueError):
                store.collect("../secret", b"x", "attempt-1")


if __name__ == "__main__":
    unittest.main()
