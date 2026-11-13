"""Upload only a reviewed, hash-pinned bundle to an exact owned Pod."""
import base64
import hashlib
import os
import re
import shlex
import subprocess
import time
import uuid
from pathlib import Path, PurePosixPath

from runpod_experiments.attempts import AttemptError
from runpod_experiments.execution_store import ExecutionStore
from runpod_experiments.proxy_ssh import SSHProxySession, _run_bounded
from runpod_experiments.ssh import SSHFetcher
from runpod_experiments.store_transport import resolve_store_ssh_context
from runpod_experiments.transfer import (
    DEFAULT_TRANSFER_CHUNK_BYTES, DEFAULT_UPLOAD_TIMEOUT_SECONDS,
    validate_chunk_bytes, validate_timeout_seconds,
)


class _DirectUploadCommandError(RuntimeError):
    """An SSH command may have failed after applying its remote side effect."""


def _upload_direct_chunks(fetcher, source, destination, expected_sha256, size,
                          transfer_check, runner, *, deadline, chunk_bytes):
    """Resume a hash-pinned direct upload and publish without replacing a file."""
    source = Path(source)
    partial = destination + ".partial-" + expected_sha256
    parent = str(PurePosixPath(destination).parent)
    quoted_destination = shlex.quote(destination)
    quoted_partial = shlex.quote(partial)
    quoted_parent = shlex.quote(parent)
    marker = "RPE1"

    def check_transfer_deadline():
        transfer_check()
        if deadline - time.monotonic() <= 0:
            raise TimeoutError("SSH upload exceeded its finite transfer window")

    def execute(command, data=b""):
        check_transfer_deadline()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("SSH upload exceeded its finite transfer window")
        argv = ["ssh", "-T", "-i", fetcher.key_file, "-p", str(fetcher.port),
                "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
                "-o", "ConnectionAttempts=1", "-o", "IdentitiesOnly=yes",
                "-o", "StrictHostKeyChecking=yes", "-o",
                "UserKnownHostsFile=" + fetcher.known_hosts,
                fetcher.username + "@" + fetcher.host, command]
        try:
            result = (_run_bounded(argv, data, min(90, remaining), 65536)
                      if runner is None else
                      runner(argv, input=data, capture_output=True,
                             timeout=min(90, remaining), check=False))
        except (subprocess.SubprocessError, OSError):
            raise _DirectUploadCommandError("SSH upload command failed") from None
        stdout = getattr(result, "stdout", b"") or b""
        stderr = getattr(result, "stderr", b"") or b""
        out_bytes = stdout.encode("utf-8") if isinstance(stdout, str) else stdout
        err_bytes = stderr.encode("utf-8") if isinstance(stderr, str) else stderr
        if (result.returncode != 0 or len(out_bytes) > 65536 or len(err_bytes) > 65536):
            raise _DirectUploadCommandError("SSH upload command failed")
        check_transfer_deadline()
        try:
            return out_bytes.decode("utf-8").strip()
        except UnicodeDecodeError:
            raise _DirectUploadCommandError("SSH upload returned invalid status") from None

    probe_command = (
        "set -eu; umask 077; mkdir -p -- " + quoted_parent +
        "; if [ -L " + quoted_destination + " ]; then exit 71; fi; "
        "if [ -e " + quoted_destination + " ]; then "
        "[ -f " + quoted_destination + " ] || exit 72; "
        "actual=$(wc -c < " + quoted_destination + " | tr -d '[:space:]'); "
        "digest=$(sha256sum -- " + quoted_destination + " | awk '{print $1}'); "
        "printf '%s PUBLISHED %s %s\\n' " + marker +
        " \"$actual\" \"$digest\"; exit 0; fi; "
        "if [ -L " + quoted_partial + " ]; then exit 73; fi; "
        "if [ -e " + quoted_partial + " ]; then [ -f " + quoted_partial +
        " ] || exit 74; else (set -C; : > " + quoted_partial +
        ") 2>/dev/null || [ -f " + quoted_partial + " ]; fi; "
        "chmod 600 -- " + quoted_partial + "; "
        "actual=$(wc -c < " + quoted_partial + " | tr -d '[:space:]'); "
        "digest=$(head -c \"$actual\" -- " + quoted_partial +
        " | sha256sum | awk '{print $1}'); "
        "printf '%s OFFSET %s %s\\n' " + marker + " \"$actual\" \"$digest\"")

    def reset_partial():
        command = (
            "set -eu; if [ -L " + quoted_partial + " ]; then exit 73; fi; "
            "if [ -e " + quoted_partial + " ]; then [ -f " + quoted_partial +
            " ] || exit 74; rm -- " + quoted_partial + "; fi; "
            "(set -C; : > " + quoted_partial + ") 2>/dev/null || exit 75; "
            "printf '%s RESET\\n' " + marker)
        if execute(command) != marker + " RESET":
            raise _DirectUploadCommandError("SSH upload reset acknowledgement was invalid")

    def probe():
        response = execute(probe_command)
        match = re.fullmatch(
            marker + r" (PUBLISHED|OFFSET) (0|[1-9][0-9]*) ([0-9a-f]{64})",
            response)
        if not match:
            raise _DirectUploadCommandError("SSH upload probe returned invalid status")
        kind, raw_offset, remote_digest = match.groups()
        offset = int(raw_offset)
        if kind == "PUBLISHED":
            if offset != size or remote_digest != expected_sha256:
                raise ValueError("Existing remote destination differs from reviewed file")
            return {"published": True, "offset": size}
        if offset > size:
            raise ValueError("Remote partial exceeds reviewed file size")
        with source.open("rb") as stream:
            local_hash = hashlib.sha256()
            remaining = offset
            while remaining:
                check_transfer_deadline()
                block = stream.read(min(chunk_bytes, remaining))
                if not block:
                    raise ValueError("Export file became shorter while checking resume")
                local_hash.update(block)
                remaining -= len(block)
        if local_hash.hexdigest() != remote_digest:
            reset_partial()
            return {"published": False, "offset": 0}
        return {"published": False, "offset": offset}

    def append_command(offset, block_size):
        end = offset + block_size
        return (
            "set -eu; [ ! -L " + quoted_partial + " ]; [ -f " + quoted_partial +
            " ]; actual=$(wc -c < " + quoted_partial + " | tr -d '[:space:]'); "
            "test \"$actual\" -eq " + str(offset) + " && cat >> " + quoted_partial +
            " && actual=$(wc -c < " + quoted_partial +
            " | tr -d '[:space:]') && test \"$actual\" -eq " + str(end) +
            " && printf '%s APPENDED %s\\n' " + marker + " \"$actual\"")

    def publish_command():
        return (
            "set -eu; [ ! -L " + quoted_partial + " ]; [ -f " + quoted_partial +
            " ]; actual=$(wc -c < " + quoted_partial + " | tr -d '[:space:]'); "
            "test \"$actual\" -eq " + str(size) + " && printf '%s  %s\\n' " +
            shlex.quote(expected_sha256) + " " + quoted_partial +
            " | sha256sum -c - >/dev/null; if [ -L " + quoted_destination +
            " ]; then exit 71; fi; if ln -- " + quoted_partial + " " +
            quoted_destination + " 2>/dev/null; then rm -- " + quoted_partial +
            "; elif [ -f " + quoted_destination + " ] && [ ! -L " +
            quoted_destination + " ] && [ \"$(wc -c < " + quoted_destination +
            " | tr -d '[:space:]')\" -eq " + str(size) + " ] && "
            "[ \"$(sha256sum -- " + quoted_destination +
            " | awk '{print $1}')\" = " + shlex.quote(expected_sha256) +
            " ]; then rm -- " + quoted_partial + "; else exit 76; fi; "
            "printf '%s PUBLISHED %s %s\\n' " + marker + " " + str(size) +
            " " + shlex.quote(expected_sha256))

    state = probe()
    if state["published"]:
        return size
    offset = state["offset"]
    for attempt in range(3):
        if offset < size:
            try:
                digest = hashlib.sha256()
                with source.open("rb") as stream:
                    before = os.fstat(stream.fileno())
                    prefix_left = offset
                    while prefix_left:
                        check_transfer_deadline()
                        block = stream.read(min(chunk_bytes, prefix_left))
                        if not block:
                            raise ValueError("Export file became shorter while checking resume")
                        digest.update(block)
                        prefix_left -= len(block)
                    while offset < size:
                        check_transfer_deadline()
                        block = stream.read(min(chunk_bytes, size - offset))
                        if not block:
                            raise ValueError("Export file became shorter while streaming")
                        digest.update(block)
                        response = execute(append_command(offset, len(block)), block)
                        expected = marker + " APPENDED " + str(offset + len(block))
                        if response != expected:
                            raise _DirectUploadCommandError(
                                "SSH upload append acknowledgement was invalid")
                        offset += len(block)
                    after = os.fstat(stream.fileno())
                before_identity = (before.st_size, before.st_mtime_ns,
                                  before.st_dev, before.st_ino)
                after_identity = (after.st_size, after.st_mtime_ns,
                                  after.st_dev, after.st_ino)
                if (offset != size or digest.hexdigest() != expected_sha256 or
                        before_identity != after_identity):
                    raise ValueError("Export file changed while streaming")
            except _DirectUploadCommandError as error:
                state = probe()
                if state["published"]:
                    return size
                offset = state["offset"]
                if attempt == 2:
                    raise RuntimeError("SSH upload retries exhausted") from error
                continue

        try:
            response = execute(publish_command())
            if response != marker + " PUBLISHED " + str(size) + " " + expected_sha256:
                raise _DirectUploadCommandError(
                    "SSH upload publication acknowledgement was invalid")
            return size
        except _DirectUploadCommandError as error:
            state = probe()
            if state["published"]:
                return size
            offset = state["offset"]
            if attempt == 2:
                raise RuntimeError("SSH upload publication retries exhausted") from error
    raise RuntimeError("SSH upload retries exhausted")


def upload_bundle(client, ledger, attempt_id, project_id, source_root, manifest, key_file,
                  known_hosts, remote_root, *, max_file_bytes=10_000_000,
                  max_total_bytes=None, ssh_route="direct", runner=None,
                  transfer_check=lambda: None,
                  timeout_seconds=DEFAULT_UPLOAD_TIMEOUT_SECONDS,
                  chunk_bytes=DEFAULT_TRANSFER_CHUNK_BYTES):
    """Caller must approve the exact bundle and destination before invoking."""
    validate_timeout_seconds(timeout_seconds, "upload_timeout_seconds")
    validate_chunk_bytes(chunk_bytes, "upload_chunk_bytes")
    if isinstance(ledger, ExecutionStore):
        target = resolve_store_ssh_context(
            client, ledger, attempt_id, project_id, route=ssh_route,
            known_hosts=known_hosts)
        endpoint = target.endpoint
        host, port, username = endpoint.host, endpoint.port, endpoint.username
    else:
        if ssh_route != "direct":
            raise ValueError("Proxy SSH is available only for store-backed attempts")
        attempt = ledger.get(attempt_id)
        if attempt["project_id"] != project_id or attempt["state"] != "active" or not attempt["pod_id"]:
            raise AttemptError("No active attempt in this project")
        pod = client.pod(attempt["pod_id"])
        if (pod.get("id") != attempt["pod_id"] or pod.get("name") != attempt["pod_name"] or
                pod.get("status") != "RUNNING"):
            raise AttemptError("Pod identity mismatch before upload")
        endpoint = (pod.get("ssh") or {}).get("direct")
        if not isinstance(endpoint, dict):
            raise AttemptError("Direct SSH endpoint not available")
        host, port, username = (endpoint.get("host"), endpoint.get("port"),
                                endpoint.get("username"))
    effective_runner = runner or subprocess.run
    fetcher = SSHFetcher(host, port, username,
                         key_file, known_hosts, remote_root,
                         runner=effective_runner)
    if not isinstance(manifest, dict) or not manifest:
        raise ValueError("A non-empty reviewed manifest is required")
    if (not isinstance(remote_root, str) or not remote_root.startswith("/") or
            "\x00" in remote_root or "\\" in remote_root or
            ".." in PurePosixPath(remote_root).parts or
            PurePosixPath(remote_root).as_posix() != remote_root or
            not callable(transfer_check)):
        raise ValueError("Invalid remote root or transfer monitor")
    if (type(max_file_bytes) is not int or max_file_bytes <= 0 or
            (max_total_bytes is not None and
             (type(max_total_bytes) is not int or max_total_bytes <= 0))):
        raise ValueError("Upload byte limits must be positive integers")
    total_limit = (30_000_000 if max_total_bytes is None and
                   isinstance(ledger, ExecutionStore) and endpoint.route == "proxy"
                   else max_total_bytes)
    root = Path(source_root).resolve()
    reviewed = []
    total_bytes = 0
    for name, digest in manifest.items():
        if (not isinstance(name, str) or not name or "\\" in name or "\x00" in name or
                PurePosixPath(name).is_absolute() or ".." in PurePosixPath(name).parts or
                not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)):
            raise ValueError("Invalid export manifest")
        path = (root / name).resolve()
        if not path.is_relative_to(root) or path.is_symlink() or not path.is_file():
            raise ValueError("Export path escapes bundle or exceeds limit")
        content_digest = hashlib.sha256()
        size = 0
        with path.open("rb") as stream:
            before = os.fstat(stream.fileno())
            while True:
                transfer_check()
                block = stream.read(1_000_000)
                if not block:
                    break
                size += len(block)
                if size > max_file_bytes:
                    raise ValueError("Export path escapes bundle or exceeds limit")
                content_digest.update(block)
            after = os.fstat(stream.fileno())
        before_identity = (before.st_size, before.st_mtime_ns, before.st_dev, before.st_ino)
        after_identity = (after.st_size, after.st_mtime_ns, after.st_dev, after.st_ino)
        if (before_identity != after_identity or size != before.st_size or
                size > max_file_bytes or content_digest.hexdigest() != digest):
            raise ValueError("Export file changed since review")
        total_bytes += size
        if total_limit is not None and total_bytes > total_limit:
            raise ValueError("Reviewed bundle exceeds the total upload byte limit")
        reviewed.append((name, path, size))
    transfer_deadline = time.monotonic() + timeout_seconds

    def remaining_transfer_seconds():
        remaining = transfer_deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("SSH upload exceeded its finite transfer window")
        return remaining

    if isinstance(ledger, ExecutionStore) and endpoint.route == "proxy":
        proxy = SSHProxySession(endpoint, key_file, known_hosts, runner=runner)
        effective_chunk_bytes = min(chunk_bytes, proxy.chunk_bytes)
        if total_bytes > effective_chunk_bytes:
            for name, path, _size in reviewed:
                transfer_check()
                destination = str(PurePosixPath(remote_root) / PurePosixPath(name))
                proxy.write_file_chunks(
                    path, destination, manifest[name], max_bytes=max_file_bytes,
                    chunk_bytes=effective_chunk_bytes,
                    timeout=remaining_transfer_seconds(), check=transfer_check)
            return len(reviewed)
        operations = []
        for name, path, size in reviewed:
            transfer_check()
            data = path.read_bytes()
            if len(data) != size or hashlib.sha256(data).hexdigest() != manifest[name]:
                raise ValueError("Export file changed since review")
            destination = str(PurePosixPath(remote_root) / PurePosixPath(name))
            temporary = destination + ".tmp-" + uuid.uuid4().hex
            parent = str(PurePosixPath(destination).parent)
            encoded = base64.b64encode(data).decode("ascii")
            marker = "RUNPOD_FILE_" + uuid.uuid4().hex.upper()
            operations.append((name, temporary, parent, destination, encoded, marker))
        cleanup = "rm -f -- " + " ".join(shlex.quote(item[1]) for item in operations)
        script = ["set -e", "umask 077", "trap " + shlex.quote(cleanup) + " EXIT"]
        for name, temporary, parent, destination, encoded, marker in operations:
            script += ["mkdir -p -- " + shlex.quote(parent),
                       "base64 -d > " + shlex.quote(temporary) + " <<'" + marker + "'",
                       *[encoded[offset:offset + 76] for offset in range(0, len(encoded), 76)],
                       marker,
                       "printf '%s  %s\\n' " + shlex.quote(manifest[name]) + " " +
                       shlex.quote(temporary) + " | sha256sum -c -",
                       "mv -- " + shlex.quote(temporary) + " " + shlex.quote(destination)]
        transfer_check()
        proxy.execute("\n".join(script), timeout=min(300, remaining_transfer_seconds()))
        return len(reviewed)
    for name, path, size in reviewed:
        destination = str(PurePosixPath(remote_root) / name)
        _upload_direct_chunks(fetcher, path, destination, manifest[name], size,
                              transfer_check, runner, deadline=transfer_deadline,
                              chunk_bytes=chunk_bytes)
    return len(reviewed)
