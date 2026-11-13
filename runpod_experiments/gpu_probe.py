"""Strict, bounded direct-SSH collection for the supervisor's GPU probe."""
from __future__ import annotations

from hashlib import sha256
from pathlib import Path
import shlex
import subprocess
import threading
import time

from runpod_experiments.contracts import sha256_bytes
from runpod_experiments.gpu_runtime import (
    GPU_PROBE_LOCAL_TIMEOUT_SECONDS, GPU_PROBE_REMOTE_TIMEOUT_SECONDS,
    GPU_REMOTE_QUERY, GPURuntimeEvidenceError,
    parse_nvidia_smi,
)
from runpod_experiments.provider import SSHRouteEndpoint


_MAX_KNOWN_HOSTS_BYTES = 1_048_576
_MAX_PRIVATE_KEY_BYTES = 1_048_576
_MAX_SSH_OUTPUT_BYTES = 32_768
_DRIVER_MARKER = b"__RPE_DRIVER_BEGIN__"
_GPU_MARKER = b"__RPE_GPU_BEGIN__"


class GPUProbeTransportError(RuntimeError):
    """A bounded strict-SSH probe could not return a complete response."""


def _local_file_path(path: str | Path, limit: int, code: str) -> Path:
    selected = Path(path)
    try:
        if selected.is_symlink() or not selected.is_file():
            raise ValueError
        resolved = selected.resolve(strict=True)
        size = resolved.stat().st_size
        if size <= 0 or size > limit:
            raise ValueError
    except (OSError, RuntimeError, ValueError):
        raise GPUProbeTransportError(code) from None
    return resolved


def _run_bounded(argv: list[str], *, timeout_seconds: int,
                 max_output_bytes: int = _MAX_SSH_OUTPUT_BYTES,
                 popen=subprocess.Popen, monotonic=time.monotonic) -> bytes:
    """Read at most max+1 bytes; terminate the local SSH process on overflow/timeout."""
    if (type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60 or
            type(max_output_bytes) is not int or not 1 <= max_output_bytes <= 1_048_576):
        raise ValueError("GPU probe process bounds are invalid")
    try:
        process = popen(
            argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, close_fds=True,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    except (OSError, subprocess.SubprocessError):
        raise GPUProbeTransportError("gpu_probe_ssh_unavailable") from None
    if process.stdout is None:
        try:
            process.kill()
            process.wait(timeout=5)
        except (OSError, subprocess.SubprocessError):
            pass
        raise GPUProbeTransportError("gpu_probe_stdout_unavailable")

    output = bytearray()
    exceeded = threading.Event()
    reader_errors: list[BaseException] = []

    def read_output() -> None:
        try:
            while True:
                allowance = max_output_bytes + 1 - len(output)
                chunk = process.stdout.read(min(4096, allowance))
                if not chunk:
                    return
                output.extend(chunk)
                if len(output) > max_output_bytes:
                    exceeded.set()
                    try:
                        process.kill()
                    except OSError:
                        pass
                    return
        except BaseException as error:
            reader_errors.append(error)

    reader = threading.Thread(target=read_output, daemon=True, name="runpod-gpu-probe-reader")
    reader.start()
    deadline = monotonic() + timeout_seconds
    try:
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(argv, timeout_seconds)
        return_code = process.wait(timeout=remaining)
    except subprocess.TimeoutExpired:
        try:
            process.kill()
            process.wait(timeout=5)
        except (OSError, subprocess.SubprocessError):
            pass
        try:
            process.stdout.close()
        except OSError:
            pass
        reader.join(timeout=5)
        raise GPUProbeTransportError("gpu_probe_timeout") from None
    except BaseException:
        try:
            process.kill()
            process.wait(timeout=5)
        except (OSError, subprocess.SubprocessError):
            pass
        try:
            process.stdout.close()
        except OSError:
            pass
        reader.join(timeout=5)
        raise
    reader.join(timeout=5)
    if reader.is_alive():
        try:
            process.kill()
            process.stdout.close()
        except OSError:
            pass
        reader.join(timeout=1)
        raise GPUProbeTransportError("gpu_probe_reader_stalled")
    if exceeded.is_set() or len(output) > max_output_bytes:
        raise GPUProbeTransportError("gpu_probe_output_exceeded")
    if reader_errors or return_code != 0:
        raise GPUProbeTransportError("gpu_probe_remote_failed")
    return bytes(output)


def _frame_outputs(raw: bytes) -> tuple[bytes, bytes]:
    if not isinstance(raw, bytes) or len(raw) > _MAX_SSH_OUTPUT_BYTES:
        raise GPURuntimeEvidenceError("probe_output_out_of_bounds")
    driver_prefix = _DRIVER_MARKER + b"\n"
    gpu_prefix = b"\n" + _GPU_MARKER + b"\n"
    if not raw.startswith(driver_prefix):
        raise GPURuntimeEvidenceError("probe_frame_malformed")
    boundary = raw.find(gpu_prefix, len(driver_prefix))
    if boundary < 0 or raw.find(gpu_prefix, boundary + len(gpu_prefix)) >= 0:
        raise GPURuntimeEvidenceError("probe_frame_malformed")
    driver = raw[len(driver_prefix):boundary].strip(b"\r\n")
    gpu_csv = raw[boundary + len(gpu_prefix):].strip(b"\r\n")
    if not driver or not gpu_csv:
        raise GPURuntimeEvidenceError("probe_frame_empty")
    return gpu_csv + b"\n", driver + b"\n"


def probe_gpu_runtime(endpoint: SSHRouteEndpoint, key_file: str | Path,
                      known_hosts: str | Path, *, observed_epoch: int,
                      runner=_run_bounded) -> tuple[object, str]:
    """Run fixed read-only queries on one direct endpoint with strict host keys.

    Returns a normalized observation and the digest of the exact known-hosts
    policy bytes used during the SSH connection. No SSH diagnostics or raw
    provider output are returned or persisted here.
    """
    if not isinstance(endpoint, SSHRouteEndpoint) or endpoint.route != "direct":
        raise GPUProbeTransportError("gpu_probe_direct_route_required")
    key_path = _local_file_path(
        key_file, _MAX_PRIVATE_KEY_BYTES, "gpu_probe_ssh_key_invalid")
    host_path = _local_file_path(
        known_hosts, _MAX_KNOWN_HOSTS_BYTES, "gpu_probe_known_hosts_invalid")
    try:
        host_data = host_path.read_bytes()
    except OSError:
        raise GPUProbeTransportError("gpu_probe_known_hosts_invalid") from None
    known_hosts_sha256 = sha256_bytes(host_data)
    remote = (f"exec timeout --kill-after=2 {GPU_PROBE_REMOTE_TIMEOUT_SECONDS} "
              "sh -c " + shlex.quote(GPU_REMOTE_QUERY))
    argv = [
        "ssh", "-F", "none", "-T", "-i", str(key_path), "-p", str(endpoint.port),
        "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
        "-o", "ConnectionAttempts=1", "-o", "IdentitiesOnly=yes",
        "-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile=" + str(host_path),
        "-o", "GlobalKnownHostsFile=none", "-o", "UpdateHostKeys=no",
        "-o", "ProxyCommand=none", "-o", "ProxyJump=none",
        "-o", "ControlMaster=no", "-o", "ControlPath=none",
        "-o", "ClearAllForwardings=yes", "-o", "ForwardAgent=no",
        "-o", "LogLevel=ERROR", f"{endpoint.username}@{endpoint.host}", remote,
    ]
    result = runner(argv, timeout_seconds=GPU_PROBE_LOCAL_TIMEOUT_SECONDS,
                    max_output_bytes=_MAX_SSH_OUTPUT_BYTES)
    if not isinstance(result, bytes) or len(result) > _MAX_SSH_OUTPUT_BYTES:
        raise GPUProbeTransportError("gpu_probe_output_invalid")
    try:
        gpu_csv, driver_report = _frame_outputs(result)
    except GPURuntimeEvidenceError:
        raise
    observation = parse_nvidia_smi(
        gpu_csv, driver_report, observed_epoch=observed_epoch)
    try:
        if sha256(host_path.read_bytes()).hexdigest() != known_hosts_sha256:
            raise GPUProbeTransportError("gpu_probe_known_hosts_changed")
    except OSError:
        raise GPUProbeTransportError("gpu_probe_known_hosts_unavailable") from None
    return observation, known_hosts_sha256
