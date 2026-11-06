"""Bounded binary SSH session to one exact active project Pod."""
from __future__ import annotations

import math
from queue import Empty, Full, Queue
import shlex
import subprocess
import threading
import time
from pathlib import Path, PurePosixPath

from runpod_experiments.attempts import AttemptError
from runpod_experiments.ssh import SSHFetcher


class SSHInteractiveProcess:
    """Long-lived raw stdin/stdout process with strict pinned SSH identity.

    The caller must keep the independent provider supervisor alive, bound its
    message sizes, and monitor the provider-owned attempt while using the pipe.
    stderr is written to a private local log file, never mixed into the binary
    stdout protocol.
    """

    def __init__(self, client, ledger, attempt_id, project_id, key_file, known_hosts,
                 remote_root, script, arguments, timeout_seconds, log_path, *,
                 popen=subprocess.Popen, output_chunk_bytes=65536, queued_chunks=32):
        attempt = ledger.get(attempt_id)
        if attempt["project_id"] != project_id or attempt["state"] != "active" or not attempt["pod_id"]:
            raise AttemptError("No active attempt in this project")
        pod = client.pod(attempt["pod_id"])
        if (pod.get("id") != attempt["pod_id"] or pod.get("name") != attempt["pod_name"] or
                pod.get("status") != "RUNNING"):
            raise AttemptError("Pod identity mismatch before interactive SSH")
        ssh = pod.get("ssh")
        endpoint = ssh.get("direct") if isinstance(ssh, dict) else None
        if not isinstance(endpoint, dict):
            raise AttemptError("Direct SSH endpoint not available")
        connection = SSHFetcher(endpoint.get("host"), endpoint.get("port"), endpoint.get("username"),
                                key_file, known_hosts, remote_root)
        if (not isinstance(script, str) or not script or "\\" in script or "\x00" in script or
                PurePosixPath(script).is_absolute() or ".." in PurePosixPath(script).parts or
                not isinstance(arguments, list) or
                any(not isinstance(arg, str) or "\x00" in arg for arg in arguments) or
                type(timeout_seconds) is not int or timeout_seconds <= 0 or
                type(output_chunk_bytes) is not int or not 1 <= output_chunk_bytes <= 1_048_576 or
                type(queued_chunks) is not int or not 1 <= queued_chunks <= 1024):
            raise ValueError("Invalid interactive command or stream limits")
        command = "cd -- " + shlex.quote(remote_root) + " && exec "
        command += shlex.join(["timeout", str(timeout_seconds), "python3", script, *arguments])
        argv = ["ssh", "-T", "-i", connection.key_file, "-p", str(connection.port),
                "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes",
                "-o", "UserKnownHostsFile=" + connection.known_hosts,
                connection.username + "@" + connection.host, command]
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        self._stderr = Path(log_path).open("ab")
        flags = subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0
        try:
            self.process = popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self._stderr,
                                 close_fds=True, creationflags=flags)
        except BaseException:
            self._stderr.close()
            raise
        if self.process.stdin is None or self.process.stdout is None:
            self.process.kill()
            self._stderr.close()
            raise RuntimeError("Interactive SSH pipes were not created")
        self._queue = Queue(maxsize=queued_chunks)
        self._chunk_bytes = output_chunk_bytes
        self._buffer = bytearray()
        self._reader_error = None
        self._closed = threading.Event()
        self._reader = threading.Thread(target=self._read_stdout, name="runpod-ssh-output", daemon=True)
        self._reader.start()

    def _read_stdout(self):
        try:
            stream = self.process.stdout
            while True:
                read = getattr(stream, "read1", stream.read)
                chunk = read(self._chunk_bytes)
                if not chunk:
                    break
                self._queue.put(chunk)
        except BaseException as error:
            self._reader_error = error
        finally:
            while not self._closed.is_set():
                try:
                    self._queue.put(None, timeout=0.1)
                    break
                except Full:
                    continue

    def send(self, data):
        if not isinstance(data, bytes) or len(data) > 64_000_000:
            raise TypeError("Interactive SSH accepts bytes only")
        if self.process.poll() is not None:
            raise BrokenPipeError("Interactive SSH process has exited")
        try:
            self.process.stdin.write(data)
            self.process.stdin.flush()
        except (OSError, ValueError):
            raise BrokenPipeError("Interactive SSH input pipe failed") from None

    def receive(self, size, timeout):
        if (type(size) is not int or size < 0 or size > 64_000_000 or
                isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or
                not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("Invalid interactive read bound")
        deadline = time.monotonic() + timeout
        while len(self._buffer) < size:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Interactive SSH response timed out")
            try:
                chunk = self._queue.get(timeout=remaining)
            except Empty:
                raise TimeoutError("Interactive SSH response timed out") from None
            if chunk is None:
                if self._reader_error is not None:
                    raise RuntimeError("Interactive SSH output reader failed") from None
                raise EOFError("Interactive SSH closed before the requested bytes arrived")
            self._buffer.extend(chunk)
        result = bytes(self._buffer[:size])
        del self._buffer[:size]
        return result

    def close_stdin(self):
        if self.process.stdin and not self.process.stdin.closed:
            self.process.stdin.close()

    def wait(self, timeout=None):
        return self.process.wait(timeout=timeout)

    def terminate(self, timeout=5):
        self._closed.set()
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=timeout)
        self.close_stdin()
        try:
            self.process.stdout.close()
        finally:
            if not self._stderr.closed:
                self._stderr.close()
