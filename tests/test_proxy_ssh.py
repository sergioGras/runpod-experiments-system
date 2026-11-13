import base64
import hashlib
import json
import re
import sys
import subprocess
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

    @staticmethod
    def remote_script_from_request(request):
        lines = request.splitlines()
        marker = re.search(rb"<<'([^']+_SCRIPT)'", request).group(1)
        start = next(index for index, line in enumerate(lines)
                     if b'base64 -d > "$__runpod_proxy_script"' in line)
        encoded = []
        for line in lines[start + 1:]:
            if line == marker:
                break
            encoded.append(line)
        return base64.b64decode(b"".join(encoded)).decode("utf-8")

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

    def test_read_large_file_uses_bounded_chunks_and_checks_the_complete_digest(self):
        payload = bytes((index % 251 for index in range(1_000_137)))
        metadata = {
            "size": len(payload), "mtime_ns": 123456, "device": 1, "inode": 2,
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
        calls = []

        def runner(_argv, **kwargs):
            calls.append(kwargs)
            token = self.token_from(kwargs["input"])
            index = len(calls) - 1
            if index == 0:
                chunk = json.dumps(metadata, separators=(",", ":")).encode()
            else:
                offset = (index - 1) * 1_000_000
                chunk = payload[offset:offset + 1_000_000]
            output = token + "_STATUS:0\n" + token + "_BEGIN\n"
            output += base64.b64encode(chunk).decode() + "\n" + token + "_END\n"
            return SimpleNamespace(stdout=output.encode(), stderr=b"", returncode=0)

        session = SSHProxySession(self.endpoint, self.key, self.known_hosts, runner=runner)
        actual = session.read_file("/work/archive.bin", max_bytes=1_100_000, timeout=30)

        self.assertEqual(actual, payload)
        self.assertEqual(len(calls), 3)  # metadata plus two bounded chunks
        self.assertLess(max(len(call["input"]) for call in calls), 2_000_000)
        self.assertTrue(all(b"rm -f -- /work/.runpod-" in call["input"] for call in calls))
        for call in calls:
            remote = self.remote_script_from_request(call["input"])
            match = re.search(r"python3 - <<'([^']+)'\n(.*?)\n\1", remote, re.DOTALL)
            self.assertIsNotNone(match)
            compile(match.group(2), "<proxy-remote-chunk>", "exec")

    def test_read_large_file_rejects_a_chunk_that_breaks_the_remote_digest(self):
        payload = b"a" * 1_000_000 + b"b" * 37
        metadata = {
            "size": len(payload), "mtime_ns": 1, "device": 1, "inode": 1,
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
        calls = []

        def runner(_argv, **kwargs):
            calls.append(kwargs)
            token = self.token_from(kwargs["input"])
            if len(calls) == 1:
                chunk = json.dumps(metadata, separators=(",", ":")).encode()
            else:
                offset = (len(calls) - 2) * 1_000_000
                chunk = payload[offset:offset + 1_000_000]
                if len(calls) == 3:
                    chunk = b"x" + chunk[1:]
            output = token + "_STATUS:0\n" + token + "_BEGIN\n"
            output += base64.b64encode(chunk).decode() + "\n" + token + "_END\n"
            return SimpleNamespace(stdout=output.encode(), stderr=b"", returncode=0)

        session = SSHProxySession(self.endpoint, self.key, self.known_hosts, runner=runner)
        with self.assertRaises(SSHProxyError):
            session.read_file("/work/archive.bin", max_bytes=1_100_000, timeout=30)
        self.assertEqual(len(calls), 3)

    def test_chunked_upload_is_bounded_and_never_replays_an_ambiguous_chunk(self):
        source = Path(self.temp.name) / "large-upload.bin"
        payload = bytes((index % 239 for index in range(2_000_123)))
        source.write_bytes(payload)
        calls = []
        checks = []

        def runner(_argv, **kwargs):
            calls.append(kwargs)
            token = self.token_from(kwargs["input"])
            return SimpleNamespace(stdout=(token + "_STATUS:0\n").encode(),
                                   stderr=b"", returncode=0)

        session = SSHProxySession(self.endpoint, self.key, self.known_hosts, runner=runner)
        size = session.write_file_chunks(
            source, "/work/large-upload.bin", hashlib.sha256(payload).hexdigest(),
            max_bytes=len(payload), check=lambda: checks.append(True))

        self.assertEqual(size, len(payload))
        self.assertEqual(len(calls), 5)  # initialize, three chunks, atomic publish
        self.assertLess(max(len(call["input"]) for call in calls), 2_000_000)
        self.assertGreater(len(checks), 3)
        final_script = self.remote_script_from_request(calls[-1]["input"])
        self.assertIn("sha256sum -c -", final_script)
        self.assertIn("mv --", final_script)

        failed_calls = []

        def ambiguous_runner(_argv, **kwargs):
            failed_calls.append(kwargs)
            token = self.token_from(kwargs["input"])
            if len(failed_calls) == 2:
                raise subprocess.TimeoutExpired("ssh", 1)
            return SimpleNamespace(stdout=(token + "_STATUS:0\n").encode(),
                                   stderr=b"", returncode=0)

        ambiguous = SSHProxySession(
            self.endpoint, self.key, self.known_hosts, runner=ambiguous_runner)
        with self.assertRaises(SSHProxyError):
            ambiguous.write_file_chunks(
                source, "/work/ambiguous.bin", hashlib.sha256(payload).hexdigest(),
                max_bytes=len(payload))
        self.assertEqual(len(failed_calls), 2)

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
