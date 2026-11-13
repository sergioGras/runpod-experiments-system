from __future__ import annotations

import unittest

from runpod_experiments.errors import ExecutionError
from runpod_experiments.interactive_workload import InteractiveDriver, InteractiveSession


class FakeBinaryProcess:
    def __init__(self, response=b"reply"):
        self.response = response
        self.sent = []
        self.returncode = None

    def send(self, data, timeout):
        self.sent.append((data, timeout))

    def receive(self, size, timeout):
        return self.response[:size]

    def poll(self):
        return self.returncode

    def close_stdin(self):
        self.returncode = 0


class InteractiveWorkloadTests(unittest.TestCase):
    def test_driver_streams_through_bounded_session_and_checks_supervisor(self):
        process = FakeBinaryProcess()
        checks = []
        session = InteractiveSession(
            process, check=lambda: checks.append("checked"), deadline=10,
            max_message_bytes=16, max_io_timeout_seconds=5,
            monotonic=lambda: 1.0, sleep=lambda _seconds: None)
        driver = InteractiveDriver(lambda _session: None, 16, 5)

        driver.run(session)
        session.send(b"request")
        response = session.receive(5)
        session.close_stdin()

        self.assertEqual(process.sent[0][0], b"request")
        self.assertLessEqual(process.sent[0][1], 5)
        self.assertEqual(response, b"reply")
        self.assertGreaterEqual(len(checks), 4)

    def test_session_rejects_stream_and_timeout_values_outside_driver_contract(self):
        session = InteractiveSession(
            FakeBinaryProcess(), check=lambda: None, deadline=10,
            max_message_bytes=3, max_io_timeout_seconds=5,
            monotonic=lambda: 1.0, sleep=lambda _seconds: None)
        with self.assertRaises(ValueError):
            session.send(b"four")
        with self.assertRaises(ValueError):
            session.receive(4)
        with self.assertRaises(ValueError):
            session.send(b"x", timeout=6)

    def test_waiting_for_input_rechecks_supervisor_between_bounded_reads(self):
        class SilentProcess(FakeBinaryProcess):
            def receive(self, size, timeout):
                raise TimeoutError("no response yet")

        now = [0.0]
        checks = [0]

        def check():
            checks[0] += 1
            if checks[0] == 4:
                raise RuntimeError("supervisor stopped")

        def sleep(seconds):
            now[0] += seconds

        session = InteractiveSession(
            SilentProcess(), check=check, deadline=20,
            max_message_bytes=16, max_io_timeout_seconds=10,
            monotonic=lambda: now[0], sleep=sleep)
        with self.assertRaisesRegex(RuntimeError, "supervisor stopped"):
            session.receive(1, timeout=5)
        self.assertEqual(checks[0], 4)

    def test_session_stops_at_attempt_deadline(self):
        class SilentProcess(FakeBinaryProcess):
            def receive(self, size, timeout):
                raise TimeoutError("no response yet")

        now = [0.0]
        session = InteractiveSession(
            SilentProcess(), check=lambda: None, deadline=0.5,
            max_message_bytes=16, max_io_timeout_seconds=5,
            monotonic=lambda: now[0], sleep=lambda seconds: now.__setitem__(0, now[0] + seconds))
        with self.assertRaises(ExecutionError) as caught:
            session.receive(1)
        self.assertEqual(caught.exception.code, "attempt_deadline_reached")

    def test_driver_rejects_unbounded_configuration(self):
        with self.assertRaises(ValueError):
            InteractiveDriver(lambda _session: None, True, 5)
        with self.assertRaises(ValueError):
            InteractiveDriver(lambda _session: None, 16, 0)


if __name__ == "__main__":
    unittest.main()
