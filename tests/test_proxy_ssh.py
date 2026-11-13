import base64
import re
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest

from runpod_experiments.provider import SSHRouteEndpoint
from runpod_experiments.proxy_ssh import (
    SSHProxyError,
    SSHProxySession,
    _run_bounded,
    _payload_from_streams,
    _status_from_streams,
)


class ProxySSHTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="proxy-ssh-test-")
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.key = root / "id_ed25519"
        self.known_hosts = root / "known_hosts"
        self.key.write_bytes(b"fixture-key")
        self.known_hosts.write_text("[ssh.runpod.io]:22 ssh-ed25519 fixture\n")
        self.endpoint = SSHRouteEndpoint("ssh.runpod.io", 22, "runpod", "proxy")

    @staticmethod
    def token_from(request):
        match = re.search(rb"(RUNPOD_[A-F0-9]{24})_STATUS:%s", request)
        if match is None:
            raise AssertionError("proxy status marker missing")
        return match.group(1).decode("ascii")

    def test_execute_uses_pinned_pty_route_and_strict_status_frame(self):
        calls = []

        def runner(argv, **kwargs):
            calls.append((argv, kwargs))
            token = self.token_from(kwargs["input"])
            return SimpleNamespace(stdout=(token + "_STATUS:0\n").encode(),
                                   stderr=b"", returncode=0)

        session = SSHProxySession(self.endpoint, self.key, self.known_hosts, runner=runner)
        session.execute("true")
        argv, kwargs = calls[0]
        self.assertIn("-tt", argv)
        self.assertIn("StrictHostKeyChecking=yes", argv)
        self.assertIn("UserKnownHostsFile=" + str(self.known_hosts), argv)
        self.assertIn(b"stty -echo", kwargs["input"])
        self.assertEqual(kwargs["check"], False)

    def test_read_file_accepts_exact_frame_and_runs_cleanup(self):
        calls = []
        payload = b"binary\x00result"

        def runner(_argv, **kwargs):
            calls.append(kwargs)
            token = self.token_from(kwargs["input"])
            output = token + "_STATUS:0\n"
            if len(calls) == 1:
                encoded = base64.b64encode(payload).decode("ascii")
                output += token + "_BEGIN\n" + encoded + "\n" + token + "_END\n"
            return SimpleNamespace(stdout=output.encode(), stderr=b"", returncode=0)

        session = SSHProxySession(self.endpoint, self.key, self.known_hosts, runner=runner)
        self.assertEqual(session.read_file("/work/result.bin", max_bytes=64), payload)
        self.assertEqual(len(calls), 1)

    def test_rejects_bad_status_duplicate_frame_and_oversized_payload(self):
        with self.assertRaises(SSHProxyError):
            _status_from_streams("RUNPOD_X_STATUS:0\nRUNPOD_X_STATUS:0\n", "", "RUNPOD_X")
        with self.assertRaises(SSHProxyError):
            _status_from_streams("RUNPOD_X_STATUS:256\n", "", "RUNPOD_X")
        frame = "RUNPOD_X_BEGIN\nYQ==\nRUNPOD_X_END\n"
        with self.assertRaises(SSHProxyError):
            _payload_from_streams(frame + frame, "", "RUNPOD_X", 10)
        with self.assertRaises(SSHProxyError):
            _payload_from_streams(frame, "", "RUNPOD_X", 0)

    def test_proxy_session_refuses_direct_route_and_unsafe_result_path(self):
        with self.assertRaises(ValueError):
            SSHProxySession(SSHRouteEndpoint("host", 22, "root", "direct"),
                            self.key, self.known_hosts)
        session = SSHProxySession(self.endpoint, self.key, self.known_hosts)
        with self.assertRaises(ValueError):
            session.read_file("/work/../secret")

    def test_bounded_local_runner_drains_input_and_stops_excess_output(self):
        result = _run_bounded(
            [sys.executable, "-c", "import sys; sys.stdout.buffer.write(sys.stdin.buffer.read())"],
            b"bounded-input", 5, 1024)
        self.assertEqual(result.stdout, b"bounded-input")
        with self.assertRaises(SSHProxyError):
            _run_bounded(
                [sys.executable, "-c", "import sys; sys.stdout.write('x' * 1000000)"],
                b"", 5, 1024)


if __name__ == "__main__":
    unittest.main()
