import io
import tempfile
import unittest
from pathlib import Path

from runpod_experiments.attempts import AttemptError
from runpod_experiments.interactive_ssh import SSHInteractiveProcess


class Process:
    def __init__(self, argv, kwargs, output=b"response"):
        self.argv, self.kwargs = argv, kwargs
        self.stdin = io.BytesIO()
        self.stdout = io.BytesIO(output)
        self.code = None

    def poll(self):
        return self.code

    def wait(self, timeout=None):
        self.code = 0
        return 0

    def terminate(self):
        self.code = -15

    def kill(self):
        self.code = -9


class Client:
    def __init__(self, pod):
        self.value = pod

    def pod(self, pod_id):
        return self.value


class Ledger:
    def __init__(self, attempt):
        self.attempt = attempt

    def get(self, attempt_id):
        if attempt_id != "attempt-1":
            raise AssertionError("unexpected attempt")
        return self.attempt


class InteractiveSSHTests(unittest.TestCase):
    def setup_session(self, directory, *, pod_changes=None, attempt_changes=None, output=b"response"):
        key = Path(directory, "key")
        known = Path(directory, "known_hosts")
        key.write_bytes(b"private key fixture")
        known.write_text("pinned-host-key\n", encoding="utf-8")
        pod = {"id": "pod-1", "name": "hybrid-attempt", "status": "RUNNING",
               "ssh": {"direct": {"host": "127.0.0.1", "port": 2222, "username": "root"}}}
        pod.update(pod_changes or {})
        attempt = {"attempt_id": "attempt-1", "project_id": "hybrid-inference",
                   "pod_name": "hybrid-attempt", "pod_id": "pod-1", "state": "active"}
        attempt.update(attempt_changes or {})
        seen = {}

        def popen(argv, **kwargs):
            seen["process"] = Process(argv, kwargs, output)
            return seen["process"]

        session = SSHInteractiveProcess(Client(pod), Ledger(attempt), "attempt-1", "hybrid-inference",
            key, known, "/tmp/hybrid attempt", "worker.py", ["--server", "two words"], 600,
            Path(directory, "stderr.log"), popen=popen, output_chunk_bytes=4, queued_chunks=2)
        return session, seen

    def test_exact_owned_session_uses_pinned_ssh_and_raw_binary_pipes(self):
        with tempfile.TemporaryDirectory() as directory:
            session, seen = self.setup_session(directory)
            process = seen["process"]
            self.assertEqual(session.receive(8, 1), b"response")
            session.send(b"\x00\xffbinary")
            self.assertEqual(process.stdin.getvalue(), b"\x00\xffbinary")
            self.assertIn("StrictHostKeyChecking=yes", process.argv)
            self.assertIn("timeout 600 python3 worker.py --server 'two words'", process.argv[-1])
            self.assertIs(process.kwargs["stdin"], -1)
            self.assertIs(process.kwargs["stdout"], -1)
            self.assertNotEqual(process.kwargs["stderr"], process.kwargs["stdout"])
            session.terminate()

    def test_wrong_pod_identity_and_unsafe_commands_do_not_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            key = Path(directory, "key")
            known = Path(directory, "known_hosts")
            key.write_bytes(b"key")
            known.write_text("host\n", encoding="utf-8")
            pod = {"id": "other", "name": "hybrid-attempt", "status": "RUNNING"}
            with self.assertRaises(AttemptError):
                SSHInteractiveProcess(Client(pod), Ledger({"project_id": "hybrid-inference", "state": "active",
                    "pod_id": "pod-1", "pod_name": "hybrid-attempt"}), "attempt-1", "hybrid-inference",
                    key, known, "/tmp", "worker.py", [], 60, Path(directory, "log"),
                    popen=lambda *a, **k: self.fail("must not launch"))
            valid_pod = {"id": "pod-1", "name": "hybrid-attempt", "status": "RUNNING",
                         "ssh": {"direct": {"host": "127.0.0.1", "port": 2222, "username": "root"}}}
            attempt = {"project_id": "hybrid-inference", "state": "active",
                       "pod_id": "pod-1", "pod_name": "hybrid-attempt"}
            with self.assertRaises(ValueError):
                SSHInteractiveProcess(Client(valid_pod), Ledger(attempt), "attempt-1", "hybrid-inference",
                    key, known, "/tmp", "../escape.py", [], 60, Path(directory, "log"),
                    popen=lambda *a, **k: self.fail("must not launch"))

    def test_short_stream_raises_eof(self):
        with tempfile.TemporaryDirectory() as directory:
            session, _ = self.setup_session(directory, output=b"short")
            with self.assertRaises(EOFError):
                session.receive(8, 1)
            session.terminate()


if __name__ == "__main__":
    unittest.main()
