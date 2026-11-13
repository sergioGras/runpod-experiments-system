"""Exercise the actual direct upload receiver commands in a local shell only."""

import hashlib
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
import time
from types import SimpleNamespace
import unittest

from runpod_experiments.upload import _upload_direct_chunks


def _find_bash():
    bash = shutil.which("bash")
    if bash:
        return bash
    candidate = Path(r"C:\Program Files\Git\bin\bash.exe")
    return str(candidate) if candidate.is_file() else None


BASH = _find_bash()


def _bash_path(path):
    result = subprocess.run(
        [BASH, "--noprofile", "--norc", "-c", "cygpath -u -- \"$1\"", "bash", str(path)],
        capture_output=True, text=True, check=True)
    return result.stdout.strip()


@unittest.skipUnless(BASH, "Git Bash or another local Bash is required for receiver-shell checks")
class DirectUploadResumeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="shared-upload-resume-")
        self.root = Path(self.temp.name)
        self.source = self.root / "source.bin"
        self.remote_root_path = self.root / "remote folder with spaces"
        self.remote_root_path.mkdir()
        self.remote_root = _bash_path(self.remote_root_path)
        self.destination = self.remote_root + "/destination.bin"
        self.key = self.root / "fixture-key"
        self.known_hosts = self.root / "fixture-known-hosts"
        self.key.write_text("offline test key", encoding="utf-8")
        self.known_hosts.write_text("offline test host", encoding="utf-8")
        self.fetcher = SimpleNamespace(
            host="127.0.0.1", port=22, username="root",
            key_file=str(self.key), known_hosts=str(self.known_hosts))

    def tearDown(self):
        self.temp.cleanup()

    def _runner(self, command_hook=None):
        def run(argv, *, input, capture_output, timeout, check):
            command = argv[-1]
            payload = input
            if command_hook is not None:
                override = command_hook(command, payload)
                if override is not None:
                    payload = override
            result = subprocess.run(
                [BASH, "--noprofile", "--norc", "-c", command],
                input=payload, capture_output=True, timeout=timeout, check=False)
            if command_hook is not None:
                command_hook(command, input, result)
            return result
        return run

    def _upload(self, payload, *, runner=None, check=lambda: None, chunk_bytes=64):
        self.source.write_bytes(payload)
        return _upload_direct_chunks(
            self.fetcher, self.source, self.destination,
            hashlib.sha256(payload).hexdigest(), len(payload), check,
            runner or self._runner(), deadline=time.monotonic() + 30,
            chunk_bytes=chunk_bytes)

    def test_fresh_transfer_publishes_exact_bytes_and_is_idempotent(self):
        payload = b"small files take the same verified path as chunked files"
        self.assertEqual(self._upload(payload), len(payload))
        destination = self.remote_root_path / "destination.bin"
        self.assertEqual(destination.read_bytes(), payload)
        # A second call observes the exact published hash and sends no file bytes.
        calls = []

        def record_runner(argv, **kwargs):
            calls.append(kwargs.get("input", b""))
            return self._runner()(argv, **kwargs)

        self.assertEqual(self._upload(payload, runner=record_runner), len(payload))
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0], b"")

    def test_resumes_from_a_real_partial_write(self):
        payload = b"a remote pipe can fail after a checked prefix was written"
        first = {"value": True}

        def hook(command, data, result=None):
            if result is None and "RPE1 APPENDED" in command and first["value"]:
                first["value"] = False
                return data[:13]
            return None

        self.assertEqual(self._upload(payload, runner=self._runner(hook)), len(payload))
        self.assertEqual((self.remote_root_path / "destination.bin").read_bytes(), payload)
        digest = hashlib.sha256(payload).hexdigest()
        partial = self.remote_root_path / ("destination.bin.partial-" + digest)
        self.assertFalse(partial.exists())

    def test_corrupt_partial_is_reset_before_resuming(self):
        payload = b"manifest pinned bytes"
        digest = hashlib.sha256(payload).hexdigest()
        partial = self.destination + ".partial-" + digest
        subprocess.run(
            [BASH, "--noprofile", "--norc", "-c",
             "printf '%s' 'not the source prefix' > " + shlex.quote(partial)],
            check=True)
        commands = []

        def record_runner(argv, **kwargs):
            commands.append(argv[-1])
            return self._runner()(argv, **kwargs)

        self.assertEqual(self._upload(payload, runner=record_runner), len(payload))
        self.assertTrue(any("RESET" in command and "RPE1" in command
                            for command in commands))
        self.assertEqual((self.remote_root_path / "destination.bin").read_bytes(), payload)

    def test_oversized_partial_is_rejected_without_deletion(self):
        payload = b"short"
        digest = hashlib.sha256(payload).hexdigest()
        partial = self.remote_root_path / ("destination.bin.partial-" + digest)
        partial.write_bytes(b"larger-than-reviewed")
        with self.assertRaisesRegex(ValueError, "exceeds reviewed file size"):
            self._upload(payload)
        self.assertEqual(partial.read_bytes(), b"larger-than-reviewed")

    def test_existing_mismatched_destination_is_never_overwritten(self):
        payload = b"reviewed bytes"
        destination = self.remote_root_path / "destination.bin"
        destination.write_bytes(b"foreign preexisting destination")
        with self.assertRaisesRegex(ValueError, "differs from reviewed file"):
            self._upload(payload)
        self.assertEqual(destination.read_bytes(), b"foreign preexisting destination")

    def test_lost_publish_ack_is_reconciled_by_exact_destination_hash(self):
        payload = b"publish may succeed while the acknowledgement is lost"
        lost = {"value": True}

        def hook(command, _data, result=None):
            if (result is not None and "ln --" in command and lost["value"]):
                lost["value"] = False
                result.returncode = 255
            return None

        self.assertEqual(self._upload(payload, runner=self._runner(hook)), len(payload))
        self.assertEqual((self.remote_root_path / "destination.bin").read_bytes(), payload)

    def test_transfer_guard_failure_stops_before_the_next_command(self):
        payload = b"guarded transfer"
        calls = []

        def runner(argv, **kwargs):
            calls.append(argv[-1])
            return self._runner()(argv, **kwargs)

        def check():
            if len(calls) >= 1:
                raise RuntimeError("transfer stopped by supervisor")

        with self.assertRaisesRegex(RuntimeError, "stopped by supervisor"):
            self._upload(payload, runner=runner, check=check)
        self.assertEqual(len(calls), 1)
        self.assertIn(" OFFSET ", calls[0])
        self.assertNotIn("RPE1 APPENDED", calls[0])


if __name__ == "__main__":
    unittest.main()
