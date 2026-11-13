"""Bounded command and file transport over RunPod's interactive SSH proxy."""
from __future__ import annotations

import base64
import binascii
import hashlib
import json
import math
import os
import re
import secrets
import shlex
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from runpod_experiments.provider import SSHRouteEndpoint


_ANSI = re.compile(r"\x1b\[(?:\?2004[hl]|[0-9;]*m)")
PROXY_CHUNK_BYTES = 1_000_000


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

    Shell stdout and stderr are suppressed by the remote wrapper. Bounded file
    reads and writes use strict base64 frames; large files are divided into
    digest-checked chunks. Arbitrary binary streams and long-lived interactive
    protocols remain unsupported here.
    """

    def __init__(self, endpoint: SSHRouteEndpoint, key_file: str | Path,
                 known_hosts: str | Path, *, max_script_bytes: int = 50_000_000,
                 max_output_bytes: int = 16_000_000, runner=None,
                 monotonic=time.monotonic):
        if (not isinstance(endpoint, SSHRouteEndpoint) or endpoint.route != "proxy" or
                type(max_script_bytes) is not int or max_script_bytes < 2048 or
                type(max_output_bytes) is not int or max_output_bytes < 512 or
                not callable(monotonic)):
            raise ValueError("Invalid proxy SSH configuration")
        key_path, hosts_path = Path(key_file), Path(known_hosts)
        if not key_path.is_file() or not hosts_path.is_file():
            raise ValueError("SSH identity and known-hosts files must exist")
        self.endpoint = endpoint
        self.key_file, self.known_hosts = str(key_path), str(hosts_path)
        self.max_script_bytes, self.max_output_bytes = max_script_bytes, max_output_bytes
        self.runner = runner
        output_chunk = max(1, (max_output_bytes - 512) * 3 // 4)
        script_chunk = max(1, (max_script_bytes - 2048) * 3 // 4)
        self.chunk_bytes = min(PROXY_CHUNK_BYTES, output_chunk, script_chunk)
        self.monotonic = monotonic

    @staticmethod
    def _remote_path(value: str) -> str:
        if (not isinstance(value, str) or not value.startswith("/") or "\x00" in value or
                any(ord(character) < 32 for character in value) or
                "\\" in value or ".." in PurePosixPath(value).parts):
            raise ValueError("Remote result path must be an absolute safe path")
        return value

    def _request(self, script: str, result_file: str | None,
                 max_result_bytes: int, timeout: int | float,
                 result_cleanup: str | None = None) -> SSHProxyResult:
        if (not isinstance(script, str) or "\x00" in script or
                len(script.encode("utf-8")) > self.max_script_bytes or
                type(max_result_bytes) is not int or max_result_bytes < 0 or
                not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or
                not math.isfinite(timeout) or timeout <= 0 or timeout > 3600):
            raise ValueError("Invalid proxy command or transfer limit")
        if result_file is not None:
            result_file = self._remote_path(result_file)
        if result_cleanup is not None:
            result_cleanup = self._remote_path(result_cleanup)

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
        if result_cleanup is not None:
            lines.append("rm -f -- " + shlex.quote(result_cleanup))
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
        """Read one bounded file, splitting large results into checked chunks."""
        remote_path = self._remote_path(remote_path)
        if (type(max_bytes) is not int or max_bytes <= 0 or
                isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or
                not math.isfinite(timeout) or timeout <= 0 or timeout > 3600):
            raise ValueError("Invalid proxy file read limit or timeout")
        if max_bytes <= self.chunk_bytes:
            result = self._request("true", remote_path, max_bytes, timeout)
            assert result.payload is not None
            return result.payload

        deadline = self.monotonic() + timeout
        metadata = self._read_file_metadata(remote_path, deadline)
        size = metadata["size"]
        if size > max_bytes:
            raise SSHProxyError("Remote file exceeds its configured size limit")
        if size <= self.chunk_bytes:
            result = self._request("true", remote_path, max_bytes,
                                   min(timeout, self._remaining(deadline)))
            assert result.payload is not None
            payload = result.payload
            if len(payload) != size or hashlib.sha256(payload).hexdigest() != metadata["sha256"]:
                raise SSHProxyError("Remote file changed during collection")
            return payload

        output = bytearray()
        digest = hashlib.sha256()
        for offset in range(0, size, self.chunk_bytes):
            count = min(self.chunk_bytes, size - offset)
            chunk = self._read_file_chunk(
                remote_path, metadata, offset, count,
                min(timeout, self._remaining(deadline)))
            if len(chunk) != count:
                raise SSHProxyError("Remote file chunk length differs")
            digest.update(chunk)
            output.extend(chunk)
        if len(output) != size or digest.hexdigest() != metadata["sha256"]:
            raise SSHProxyError("Remote file changed during collection")
        return bytes(output)

    def _remaining(self, deadline: float) -> float:
        remaining = deadline - self.monotonic()
        if remaining <= 0:
            raise TimeoutError("SSH proxy file transfer exceeded its deadline")
        return remaining

    def _read_file_metadata(self, remote_path: str, deadline: float) -> dict:
        temporary = str(PurePosixPath(remote_path).parent /
                        (".runpod-metadata-" + secrets.token_hex(12)))
        python_program = (
            "import hashlib,json,os,stat\n"
            f"source={remote_path!r}; target={temporary!r}\n"
            "fd=os.open(source,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))\n"
            "try:\n"
            " info=os.fstat(fd)\n"
            " if not stat.S_ISREG(info.st_mode): raise ValueError('not_regular')\n"
            " digest=hashlib.sha256()\n"
            " while True:\n  block=os.read(fd,1048576)\n  if not block: break\n  digest.update(block)\n"
            " after=os.fstat(fd)\n"
            " identity=(info.st_size,info.st_mtime_ns,info.st_dev,info.st_ino)\n"
            " if identity!=(after.st_size,after.st_mtime_ns,after.st_dev,after.st_ino): raise ValueError('changed')\n"
            " data=json.dumps({'size':info.st_size,'mtime_ns':info.st_mtime_ns,'device':info.st_dev,'inode':info.st_ino,'sha256':digest.hexdigest()},separators=(',',':')).encode()\n"
            " out=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)\n"
            " try:\n  view=memoryview(data)\n  while view:\n   written=os.write(out,view)\n   view=view[written:]\n"
            " finally: os.close(out)\n"
            "finally: os.close(fd)"
        )
        script = "python3 - <<'RUNPOD_FILE_METADATA'\n" + python_program + "\nRUNPOD_FILE_METADATA"
        response = self._request(
            script, temporary, 2048, min(60, self._remaining(deadline)),
            result_cleanup=temporary)
        try:
            metadata = json.loads(response.payload.decode("ascii"))
        except (AttributeError, UnicodeError, ValueError):
            raise SSHProxyError("Remote file metadata is invalid") from None
        if (not isinstance(metadata, dict) or type(metadata.get("size")) is not int or
                metadata["size"] < 0 or type(metadata.get("mtime_ns")) is not int or
                type(metadata.get("device")) is not int or type(metadata.get("inode")) is not int or
                not isinstance(metadata.get("sha256"), str) or
                not re.fullmatch(r"[0-9a-f]{64}", metadata["sha256"])):
            raise SSHProxyError("Remote file metadata is invalid")
        return metadata

    def _read_file_chunk(self, remote_path: str, metadata: dict, offset: int,
                         count: int, timeout: int | float) -> bytes:
        temporary = str(PurePosixPath(remote_path).parent /
                        (".runpod-chunk-" + secrets.token_hex(12)))
        python_program = (
            "import os,stat\n"
            f"source={remote_path!r}; target={temporary!r}\n"
            f"expected=({metadata['size']!r},{metadata['mtime_ns']!r},{metadata['device']!r},{metadata['inode']!r})\n"
            f"offset={offset!r}; count={count!r}\n"
            "fd=os.open(source,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))\n"
            "try:\n"
            " info=os.fstat(fd)\n"
            " actual=(info.st_size,info.st_mtime_ns,info.st_dev,info.st_ino)\n"
            " if not stat.S_ISREG(info.st_mode) or actual!=expected: raise ValueError('changed')\n"
            " if offset<0 or count<0 or offset+count>info.st_size: raise ValueError('range')\n"
            " os.lseek(fd,offset,os.SEEK_SET)\n"
            " data=os.read(fd,count)\n"
            " if len(data)!=count: raise ValueError('short_read')\n"
            " out=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)\n"
            " try:\n"
            "  view=memoryview(data)\n"
            "  while view:\n   written=os.write(out,view)\n   view=view[written:]\n"
            " finally: os.close(out)\n"
            "finally: os.close(fd)"
        )
        script = "python3 - <<'RUNPOD_FILE_CHUNK'\n" + python_program + "\nRUNPOD_FILE_CHUNK"
        response = self._request(script, temporary, count, timeout,
                                 result_cleanup=temporary)
        assert response.payload is not None
        return response.payload

    def write_file_chunks(self, source_path: str | Path, remote_path: str,
                          expected_sha256: str, *, max_bytes: int,
                          chunk_bytes: int | None = None, timeout: int | float = 300,
                          check=lambda: None) -> int:
        """Upload a reviewed file in bounded, non-replayed chunks and publish atomically."""
        remote_path = self._remote_path(remote_path)
        source = Path(source_path)
        chunk_size = self.chunk_bytes if chunk_bytes is None else chunk_bytes
        if (source.is_symlink() or not source.is_file() or type(max_bytes) is not int or
                max_bytes <= 0 or type(chunk_size) is not int or
                not 1 <= chunk_size <= self.chunk_bytes or
                not isinstance(expected_sha256, str) or
                not re.fullmatch(r"[0-9a-f]{64}", expected_sha256) or
                isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or
                not math.isfinite(timeout) or timeout <= 0 or timeout > 3600 or
                not callable(check)):
            raise ValueError("Invalid chunked proxy upload contract")
        source = source.resolve(strict=True)
        deadline = self.monotonic() + timeout
        source_digest = hashlib.sha256()
        size = 0
        with source.open("rb") as stream:
            while True:
                check()
                block = stream.read(chunk_size)
                if not block:
                    break
                size += len(block)
                if size > max_bytes:
                    raise ValueError("Upload exceeds its configured size limit")
                source_digest.update(block)
        if size > max_bytes or source_digest.hexdigest() != expected_sha256:
            raise ValueError("Upload source differs from its reviewed digest")

        temporary = remote_path + ".tmp-" + secrets.token_hex(12)
        parent = str(PurePosixPath(remote_path).parent)
        check()
        self.execute("set -e\numask 077\nmkdir -p -- " + shlex.quote(parent) +
                     "\nset -C\n: > " + shlex.quote(temporary) + "\nset +C",
                     timeout=min(90, self._remaining(deadline)))
        digest = hashlib.sha256()
        offset = 0
        with source.open("rb") as stream:
            while True:
                check()
                block = stream.read(chunk_size)
                if not block:
                    break
                if offset + len(block) > size or offset + len(block) > max_bytes:
                    raise ValueError("Upload source changed while streaming")
                digest.update(block)
                marker = "RUNPOD_CHUNK_" + secrets.token_hex(12).upper()
                encoded = base64.b64encode(block).decode("ascii")
                quoted_temporary = shlex.quote(temporary)
                script = ("set -e\nactual=$(wc -c < " + quoted_temporary + ")\ntest \"$actual\" -eq " +
                          str(offset) + "\nbase64 -d >> " + quoted_temporary + " <<'" + marker + "'\n" +
                          "\n".join(encoded[index:index + 76]
                                    for index in range(0, len(encoded), 76)) +
                          "\n" + marker + "\nactual=$(wc -c < " + quoted_temporary +
                          ")\ntest \"$actual\" -eq " + str(offset + len(block)))
                check()
                self.execute(script, timeout=min(90, self._remaining(deadline)))
                offset += len(block)
        if digest.hexdigest() != expected_sha256 or offset != size:
            raise ValueError("Upload source changed while streaming")
        check()
        quoted_temporary, quoted_destination = (shlex.quote(temporary),
                                                shlex.quote(remote_path))
        finalize = ("set -e\nactual=$(wc -c < " + quoted_temporary + ")\ntest \"$actual\" -eq " +
                    str(size) + "\nprintf '%s  %s\\n' " + shlex.quote(expected_sha256) + " " +
                    quoted_temporary + " | sha256sum -c -\nmv -- " + quoted_temporary +
                    " " + quoted_destination)
        self.execute(finalize, timeout=min(90, self._remaining(deadline)))
        check()
        return size


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
        return self.fetch_limited(name, self.max_bytes)

    def fetch_limited(self, name, max_bytes):
        if type(max_bytes) is not int or not 0 < max_bytes <= self.max_bytes:
            raise ValueError("Requested proxy collection limit exceeds configured maximum")
        if (not isinstance(name, str) or not name or "\\" in name or "\x00" in name):
            raise ValueError("Invalid artifact name")
        relative = PurePosixPath(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Artifact must be inside the remote root")
        remote = str(PurePosixPath(self.remote_root) / relative)
        return self.session.read_file(remote, max_bytes=max_bytes, timeout=self.timeout)
