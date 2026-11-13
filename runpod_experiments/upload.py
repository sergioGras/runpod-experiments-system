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


def _upload_direct_chunks(fetcher, source, destination, expected_sha256, size,
                          transfer_check, runner):
    temporary = destination + ".tmp-" + uuid.uuid4().hex
    parent = str(PurePosixPath(destination).parent)
    deadline = time.monotonic() + 300

    def execute(command, data=b""):
        transfer_check()
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
            raise RuntimeError("SSH upload failed") from None
        stdout = getattr(result, "stdout", b"") or b""
        stderr = getattr(result, "stderr", b"") or b""
        if (result.returncode != 0 or len(stdout) > 65536 or len(stderr) > 65536):
            raise RuntimeError("SSH upload failed")
        transfer_check()

    quoted_temporary = shlex.quote(temporary)
    execute("set -e; umask 077; mkdir -p -- " + shlex.quote(parent) +
            "; set -C; : > " + quoted_temporary + "; set +C")
    digest = hashlib.sha256()
    offset = 0
    with Path(source).open("rb") as stream:
        while True:
            transfer_check()
            block = stream.read(1_000_000)
            if not block:
                break
            digest.update(block)
            command = ("set -e; actual=$(wc -c < " + quoted_temporary +
                       "); test \"$actual\" -eq " + str(offset) +
                       " && cat >> " + quoted_temporary +
                       " && actual=$(wc -c < " + quoted_temporary +
                       ") && test \"$actual\" -eq " + str(offset + len(block)))
            execute(command, block)
            offset += len(block)
    if offset != size or digest.hexdigest() != expected_sha256:
        raise ValueError("Export file changed while streaming")
    finalize = ("set -e; actual=$(wc -c < " + quoted_temporary +
                "); test \"$actual\" -eq " + str(size) +
                " && printf '%s  %s\\n' " + shlex.quote(expected_sha256) +
                " " + quoted_temporary + " | sha256sum -c - && mv -- " +
                quoted_temporary + " " + shlex.quote(destination))
    execute(finalize)
    return size


def upload_bundle(client, ledger, attempt_id, project_id, source_root, manifest, key_file,
                  known_hosts, remote_root, *, max_file_bytes=10_000_000,
                  max_total_bytes=None, ssh_route="direct", runner=None,
                  transfer_check=lambda: None):
    """Caller must approve the exact bundle and destination before invoking."""
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
    if isinstance(ledger, ExecutionStore) and endpoint.route == "proxy":
        proxy = SSHProxySession(endpoint, key_file, known_hosts, runner=runner)
        if total_bytes > proxy.chunk_bytes:
            for name, path, _size in reviewed:
                transfer_check()
                destination = str(PurePosixPath(remote_root) / PurePosixPath(name))
                proxy.write_file_chunks(
                    path, destination, manifest[name], max_bytes=max_file_bytes,
                    check=transfer_check)
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
        proxy.execute("\n".join(script), timeout=300)
        return len(reviewed)
    if total_bytes > 1_000_000:
        for name, path, size in reviewed:
            destination = str(PurePosixPath(remote_root) / name)
            _upload_direct_chunks(fetcher, path, destination, manifest[name], size,
                                  transfer_check, runner)
        return len(reviewed)
    for name, path, size in reviewed:
        transfer_check()
        data = path.read_bytes()
        if len(data) != size or hashlib.sha256(data).hexdigest() != manifest[name]:
            raise ValueError("Export file changed since review")
        destination = PurePosixPath(remote_root) / name
        temporary = str(destination) + ".tmp-" + uuid.uuid4().hex
        command = "umask 077; mkdir -p -- " + shlex.quote(str(destination.parent))
        command += " && cat > " + shlex.quote(temporary) + " && mv -- " + shlex.quote(temporary) + " " + shlex.quote(str(destination))
        argv = ["ssh", "-T", "-i", fetcher.key_file, "-p", str(fetcher.port), "-o", "BatchMode=yes",
                "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes", "-o",
                "UserKnownHostsFile=" + fetcher.known_hosts, fetcher.username + "@" + fetcher.host, command]
        try:
            effective_runner(argv, input=data, capture_output=True, timeout=30, check=True)
        except (subprocess.SubprocessError, OSError):
            raise RuntimeError("SSH upload failed") from None
        transfer_check()
    return len(reviewed)
