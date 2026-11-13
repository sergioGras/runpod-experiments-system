"""Bounded command and file transport over RunPod's interactive SSH proxy."""
from __future__ import annotations

import base64
import binascii
import math
import re
import secrets
import shlex
import subprocess
import threading
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from runpod_experiments.provider import SSHRouteEndpoint


_ANSI = re.compile(r"\x1b\[(?:\?2004[hl]|[0-9;]*m)")


class SSHProxyError(RuntimeError):
    """A proxy session failed without exposing command or provider details."""

    def __init__(self, message: str, *, returncode: int | None = None):
        self.returncode = returncode
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class SSHProxyResult:
    returncode: int
    payload: bytes | None = None


def _text(value: bytes | str | None) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value if isinstance(value, str) else ""


def _normalized(stream: str) -> str:
    return _ANSI.sub("", stream.replace("\r\n", "\n").replace("\r", "\n"))


def _status_from_streams(stdout: str, stderr: str, token: str) -> int:
    marker = re.compile(r"(?m)^" + re.escape(token) + r"_STATUS:([0-9]{1,3})$")
    starts = re.compile(r"(?m)^" + re.escape(token) + r"_STATUS(?::.*)?$")
    streams = (_normalized(stdout), _normalized(stderr))
    matches = [match.group(1) for stream in streams for match in marker.finditer(stream)]
    marker_count = sum(len(starts.findall(stream)) for stream in streams)
    if len(matches) != 1 or marker_count != len(matches):
        if marker_count:
            raise SSHProxyError("SSH proxy returned a malformed status frame")
        raise SSHProxyError("SSH proxy status frame is missing or ambiguous")
    status = int(matches[0])
    if status > 255:
        raise SSHProxyError("SSH proxy returned an invalid status code")
    return status


def _payload_from_streams(stdout: str, stderr: str, token: str, max_bytes: int) -> bytes:
    frame = re.compile(r"(?m)^" + re.escape(token) +
                       r"_BEGIN\n([A-Za-z0-9+/=\n]*)\n" +
                       re.escape(token) + r"_END$")
    found: list[bytes] = []
    for stream in (stdout, stderr):
        normalized = _normalized(stream)
        for match in frame.finditer(normalized):
            encoded = "".join(match.group(1).split())
            try:
                decoded = base64.b64decode(encoded, validate=True)
            except (ValueError, binascii.Error):
                raise SSHProxyError("SSH proxy payload frame is invalid") from None
            if len(decoded) > max_bytes:
                raise SSHProxyError("SSH proxy payload exceeds its byte limit")
            found.append(decoded)
        markers = re.findall(r"(?m)^" + re.escape(token) + r"_(?:BEGIN|END)$", normalized)
        if len(markers) != 2 * len(frame.findall(normalized)):
            raise SSHProxyError("SSH proxy payload frame is malformed")
    if len(found) != 1:
        raise SSHProxyError("SSH proxy payload frame is missing or ambiguous")
    return found[0]


def _run_bounded(argv: list[str], request: bytes, timeout: int | float,
                 max_output_bytes: int) -> subprocess.CompletedProcess:
    """Run SSH while draining both pipes and killing it at the output limit."""
    flags = subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0
    process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, close_fds=True, creationflags=flags)
    if process.stdin is None or process.stdout is None or process.stderr is None:
        process.kill()
        raise SSHProxyError("SSH proxy pipes were not created")
    output = [bytearray(), bytearray()]
    total = [0]
    lock = threading.Lock()
    overflow = threading.Event()
    errors: list[BaseException] = []

    def drain(pipe, slot):
        try:
            while True:
                chunk = pipe.read(65536)
                if not chunk:
                    return
                with lock:
                    if total[0] + len(chunk) > max_output_bytes:
                        overflow.set()
                        try:
                            process.kill()
                        except OSError:
                            pass
                        return
                    output[slot].extend(chunk)
                    total[0] += len(chunk)
        except (OSError, ValueError) as error:
            errors.append(error)

    def send():
        try:
            process.stdin.write(request)
            process.stdin.flush()
        except (BrokenPipeError, OSError):
            pass
        finally:
            try:
                process.stdin.close()
            except OSError:
                pass

    readers = [threading.Thread(target=drain, args=(process.stdout, 0), daemon=True),
               threading.Thread(target=drain, args=(process.stderr, 1), daemon=True)]
    writer = threading.Thread(target=send, daemon=True)
    for thread in (*readers, writer):
        thread.start()
    timed_out = False
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        try:
            process.kill()
        except OSError:
            pass
        process.wait()
    for thread in (*readers, writer):
        thread.join(timeout=5)
    if any(thread.is_alive() for thread in (*readers, writer)):
        try:
            process.kill()
        except OSError:
            pass
        for pipe in (process.stdout, process.stderr):
            try:
                pipe.close()
            except OSError:
                pass
        for thread in (*readers, writer):
            thread.join(timeout=1)
        raise SSHProxyError("SSH proxy pipes did not close")
    for pipe in (process.stdout, process.stderr):
        try:
            pipe.close()
        except OSError:
            pass
    if timed_out:
        raise SSHProxyError("SSH proxy command timed out")
    if overflow.is_set():
        raise SSHProxyError("SSH proxy output exceeds its byte limit")
    if errors:
        raise SSHProxyError("SSH proxy output could not be read")
    return subprocess.CompletedProcess(argv, process.returncode,
                                       bytes(output[0]), bytes(output[1]))


class SSHProxySession:
    """Run one finite shell command through a PTY-backed proxy route.

    Shell stdout and stderr are suppressed by the remote wrapper. A requested
    file result is returned as one strict base64 frame; arbitrary binary streams
    and long-lived interactive protocols are deliberately unsupported here.
    """

    def __init__(self, endpoint: SSHRouteEndpoint, key_file: str | Path,
                 known_hosts: str | Path, *, max_script_bytes: int = 50_000_000,
                 max_output_bytes: int = 16_000_000, runner=None):
        if (not isinstance(endpoint, SSHRouteEndpoint) or endpoint.route != "proxy" or
                type(max_script_bytes) is not int or max_script_bytes <= 0 or
                type(max_output_bytes) is not int or max_output_bytes <= 0):
            raise ValueError("Invalid proxy SSH configuration")
        key_path, hosts_path = Path(key_file), Path(known_hosts)
        if not key_path.is_file() or not hosts_path.is_file():
            raise ValueError("SSH identity and known-hosts files must exist")
        self.endpoint = endpoint
        self.key_file, self.known_hosts = str(key_path), str(hosts_path)
        self.max_script_bytes, self.max_output_bytes = max_script_bytes, max_output_bytes
        self.runner = runner

    @staticmethod
    def _remote_path(value: str) -> str:
        if (not isinstance(value, str) or not value.startswith("/") or "\x00" in value or
                any(ord(character) < 32 for character in value) or
                "\\" in value or ".." in PurePosixPath(value).parts):
            raise ValueError("Remote result path must be an absolute safe path")
        return value

    def _request(self, script: str, result_file: str | None,
                 max_result_bytes: int, timeout: int | float) -> SSHProxyResult:
        if (not isinstance(script, str) or "\x00" in script or
                len(script.encode("utf-8")) > self.max_script_bytes or
                type(max_result_bytes) is not int or max_result_bytes < 0 or
                not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or
                not math.isfinite(timeout) or timeout <= 0 or timeout > 3600):
            raise ValueError("Invalid proxy command or transfer limit")
        if result_file is not None:
            result_file = self._remote_path(result_file)

        token = "RUNPOD_" + secrets.token_hex(12).upper()
        script_marker = token + "_SCRIPT"
        encoded = base64.b64encode(script.encode("utf-8")).decode("ascii")
        lines = [
            "stty -echo",
            "unset PROMPT_COMMAND",
            "PS1=''",
            "PS2=''",
            "set +e",
            "__runpod_proxy_dir=$(mktemp -d) || exit 1",
            "__runpod_proxy_script=\"$__runpod_proxy_dir/request.sh\"",
            "base64 -d > \"$__runpod_proxy_script\" <<'" + script_marker + "'",
            *[encoded[offset:offset + 76] for offset in range(0, len(encoded), 76)],
            script_marker,
            "bash --noprofile --norc \"$__runpod_proxy_script\" >/dev/null 2>&1",
            "__runpod_proxy_status=$?",
            "printf '\\n" + token + "_STATUS:%s\\n' \"$__runpod_proxy_status\"",
        ]
        if result_file is not None:
            quoted = shlex.quote(result_file)
            lines += [
                "if [ \"$__runpod_proxy_status\" -eq 0 ] && [ -f " + quoted + " ]; then",
                "  printf '\\n" + token + "_BEGIN\\n'",
                "  if (set -o pipefail; head -c " + str(max_result_bytes + 1) +
                " -- " + quoted + " | base64); then",
                "    printf '\\n" + token + "_END\\n'",
                "  else",
                "    printf '\\n" + token + "_ERROR\\n'",
                "  fi",
                "fi",
            ]
        lines += ["rm -rf -- \"$__runpod_proxy_dir\"", "exit"]
        request = ("\n".join(lines) + "\n").encode("utf-8")

        e = self.endpoint
        argv = ["ssh", "-tt", "-i", self.key_file, "-p", str(e.port),
                "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
                "-o", "ConnectionAttempts=1", "-o", "IdentitiesOnly=yes",
                "-o", "StrictHostKeyChecking=yes", "-o",
                "UserKnownHostsFile=" + self.known_hosts, "-o", "LogLevel=ERROR",
                e.username + "@" + e.host]
        try:
            result = (_run_bounded(argv, request, timeout, self.max_output_bytes)
                      if self.runner is None else
                      self.runner(argv, input=request, capture_output=True,
                                  timeout=timeout, check=False))
        except (subprocess.SubprocessError, OSError):
            raise SSHProxyError("SSH proxy command failed") from None
        if result.returncode != 0:
            raise SSHProxyError("SSH proxy command failed", returncode=result.returncode)
        stdout, stderr = _text(result.stdout), _text(result.stderr)
        if len(stdout.encode("utf-8")) + len(stderr.encode("utf-8")) > self.max_output_bytes:
            raise SSHProxyError("SSH proxy output exceeds its byte limit")
        status = _status_from_streams(stdout, stderr, token)
        if status != 0:
            raise SSHProxyError("Remote command failed through SSH proxy", returncode=status)
        payload = (_payload_from_streams(stdout, stderr, token, max_result_bytes)
                   if result_file is not None else None)
        return SSHProxyResult(status, payload)

    def execute(self, script: str, *, timeout: int | float = 30) -> None:
        """Execute a finite remote script and require a successful status frame."""
        self._request(script, None, 0, timeout)

    def read_file(self, remote_path: str, *, max_bytes: int = 10_000_000,
                  timeout: int | float = 30) -> bytes:
        """Read one bounded remote file through a strict framed base64 response."""
        remote_path = self._remote_path(remote_path)
        if type(max_bytes) is not int or max_bytes <= 0:
            raise ValueError("max_bytes must be a positive integer")
        result = self._request("true", remote_path, max_bytes, timeout)
        assert result.payload is not None
        return result.payload


class SSHProxyFetcher:
    """SSHFetcher-compatible bounded artifact reader for an explicit proxy route."""

    def __init__(self, host, port, username, key_file, known_hosts, remote_root,
                 *, max_bytes=10_000_000, timeout=30, runner=None):
        if (not isinstance(remote_root, str) or not remote_root.startswith("/") or
                "\x00" in remote_root or "\\" in remote_root or
                ".." in PurePosixPath(remote_root).parts or
                type(max_bytes) is not int or max_bytes <= 0):
            raise ValueError("Invalid proxy collection root or byte limit")
        self.remote_root = str(PurePosixPath(remote_root))
        self.max_bytes, self.timeout = max_bytes, timeout
        endpoint = SSHRouteEndpoint(host, port, username, "proxy")
        self.session = SSHProxySession(endpoint, key_file, known_hosts, runner=runner)

    def __call__(self, name):
        if (not isinstance(name, str) or not name or "\\" in name or "\x00" in name):
            raise ValueError("Invalid artifact name")
        relative = PurePosixPath(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Artifact must be inside the remote root")
        remote = str(PurePosixPath(self.remote_root) / relative)
        return self.session.read_file(remote, max_bytes=self.max_bytes, timeout=self.timeout)
