import json
import tempfile
import unittest
from pathlib import Path

from examples.remote_hello import run


class RemoteHelloTests(unittest.TestCase):
    def test_resume_validates_and_completes_partial_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "checkpoint.json"
            path.write_text(json.dumps({"config": {"steps": 3, "width": 10, "algorithm": "sum-squares-v1"},
                                        "completed": [sum(i * i for i in range(10))]}))
            result = run(directory, steps=3, width=10)
            self.assertEqual(result["status"], "complete")
            self.assertEqual(len(json.loads(path.read_text())["completed"]), 3)
            self.assertEqual(result, run(directory, steps=3, width=10))
            path.write_text(json.dumps({"config": {"steps": 3, "width": 10, "algorithm": "sum-squares-v1"}, "completed": [0]}))
            with self.assertRaises(ValueError):
                run(directory, steps=3, width=10)


if __name__ == "__main__":
    unittest.main()
