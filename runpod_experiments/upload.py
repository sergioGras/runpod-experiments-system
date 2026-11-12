"""Upload only a reviewed, hash-pinned bundle to an exact owned Pod."""
import hashlib
import re
import shlex
import subprocess
import uuid
from pathlib import Path, PurePosixPath

from runpod_experiments.attempts import AttemptError
from runpod_experiments.execution_store import ExecutionStore
from runpod_experiments.ssh import SSHFetcher
from runpod_experiments.store_transport import resolve_store_ssh_context


def upload_bundle(client, ledger, attempt_id, project_id, source_root, manifest, key_file,
                  known_hosts, remote_root, *, max_file_bytes=10_000_000, runner=subprocess.run):
    """Caller must approve the exact bundle and destination before invoking."""
    if isinstance(ledger, ExecutionStore):
        target = resolve_store_ssh_context(client, ledger, attempt_id, project_id)
        endpoint = target.endpoint
        host, port, username = endpoint.host, endpoint.port, endpoint.username
    else:
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
    fetcher = SSHFetcher(host, port, username,
                         key_file, known_hosts, remote_root, runner=runner)
    if not isinstance(manifest, dict) or not manifest:
        raise ValueError("A non-empty reviewed manifest is required")
    root = Path(source_root).resolve()
    reviewed = []
    for name, digest in manifest.items():
        if (not isinstance(name, str) or not name or "\\" in name or "\x00" in name or
                PurePosixPath(name).is_absolute() or ".." in PurePosixPath(name).parts or
                not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)):
            raise ValueError("Invalid export manifest")
        path = (root / name).resolve()
        if not path.is_relative_to(root) or not path.is_file() or path.stat().st_size > max_file_bytes:
            raise ValueError("Export path escapes bundle or exceeds limit")
        data = path.read_bytes()
        if len(data) > max_file_bytes or hashlib.sha256(data).hexdigest() != digest:
            raise ValueError("Export file changed since review")
        reviewed.append((name, data))
    for name, data in reviewed:
        destination = PurePosixPath(remote_root) / name
        temporary = str(destination) + ".tmp-" + uuid.uuid4().hex
        command = "umask 077; mkdir -p -- " + shlex.quote(str(destination.parent))
        command += " && cat > " + shlex.quote(temporary) + " && mv -- " + shlex.quote(temporary) + " " + shlex.quote(str(destination))
        argv = ["ssh", "-T", "-i", fetcher.key_file, "-p", str(fetcher.port), "-o", "BatchMode=yes",
                "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes", "-o",
                "UserKnownHostsFile=" + fetcher.known_hosts, fetcher.username + "@" + fetcher.host, command]
        try:
            runner(argv, input=data, capture_output=True, timeout=30, check=True)
        except (subprocess.SubprocessError, OSError):
            raise RuntimeError("SSH upload failed") from None
    return len(reviewed)
