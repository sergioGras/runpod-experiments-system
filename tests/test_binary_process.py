import json
from pathlib import Path
import sys
import tempfile
import time
import unittest

from runpod_experiments.binary_process import BinaryProcess
from runpod_experiments.telemetry import StageJournal


class BinaryProcessTests(unittest.TestCase):
    def test_unread_input_times_out_and_kills_child(self):
        with tempfile.TemporaryDirectory() as directory:
            pipe = BinaryProcess.spawn([sys.executable, "-c", "import time; time.sleep(30)"],
                Path(directory)/"log", lifetime_seconds=10)
            try:
                with self.assertRaises(TimeoutError):
                    pipe.send(b"x" * 2_000_000, timeout=0.2)
                self.assertIsNotNone(pipe.process.poll())
                self.assertFalse(pipe._writer.is_alive())
            finally:
                pipe.terminate()

    def test_full_output_queue_can_shutdown_without_leaked_reader(self):
        with tempfile.TemporaryDirectory() as directory:
            pipe = BinaryProcess.spawn([sys.executable, "-c",
                "import sys,time; sys.stdout.buffer.write(b'x'*2000000); sys.stdout.flush(); time.sleep(30)"],
                Path(directory)/"log", lifetime_seconds=10, output_chunk_bytes=4, queued_chunks=1)
            try:
                deadline = time.monotonic()+3
                while not pipe._queue.full() and time.monotonic()<deadline:
                    time.sleep(0.01)
                self.assertTrue(pipe._queue.full())
            finally:
                pipe.terminate()
            self.assertFalse(pipe._reader.is_alive())

    def test_native_style_stall_preserves_started_stage_and_lifetime_kills_child(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"stages.jsonl"
            journal = StageJournal(path)
            journal.record("native_call", "start")
            pipe = BinaryProcess.spawn([sys.executable, "-c", "import time; time.sleep(30)"],
                Path(directory)/"log", lifetime_seconds=0.2)
            try:
                with self.assertRaises(TimeoutError):
                    pipe.receive(1, 2)
            finally:
                pipe.terminate()
            self.assertEqual(json.loads(path.read_text())["event"], "start")
            self.assertIsNotNone(pipe.process.poll())

    def test_stage_errors_are_durable(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"stages"
            with self.assertRaises(ValueError):
                with StageJournal(path).stage("decode"):
                    raise ValueError("test")
            records=[json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual([r['event'] for r in records], ['start','error'])
            self.assertEqual(records[-1]['error_type'], 'ValueError')


if __name__ == "__main__":
    unittest.main()
