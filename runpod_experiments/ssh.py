"""Bounded direct-SSH result download with explicit host-key verification."""
import re
import shlex
import subprocess
from pathlib import Path, PurePosixPath

from runpod_experiments.attempts import AttemptError


class SSHFetcher:
    def __init__(self, host, port, username, key_file, known_hosts, remote_root, *, max_bytes=10_000_000, timeout=30, runner=subprocess.run):
        if (not isinstance(host, str) or not re.fullmatch(r"[A-Za-z0-9.:-]+", host) or host.startswith("-") or
                type(port) is not int or not 1 <= port <= 65535 or
                not isinstance(username, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", username) or
                not isinstance(remote_root, str) or not remote_root.startswith("/") or
                "\x00" in remote_root or ".." in PurePosixPath(remote_root).parts or
                type(max_bytes) is not int or max_bytes <= 0):
            raise ValueError("Invalid SSH destination or collection limit")
        if not Path(key_file).is_file() or not Path(known_hosts).is_file():
            raise ValueError("SSH identity and known-hosts files must exist")
        self.host, self.port, self.username = host, port, username
        self.key_file, self.known_hosts, self.remote_root = str(key_file), str(known_hosts), remote_root
        self.max_bytes, self.timeout, self.runner = max_bytes, timeout, runner

    def __call__(self, name):
        if not isinstance(name, str) or not name or "\\" in name or "\x00" in name:
            raise ValueError("Invalid artifact name")
        relative = PurePosixPath(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Artifact must be inside the remote root")
        remote = str(PurePosixPath(self.remote_root) / relative)
        command = "head -c " + str(self.max_bytes + 1) + " -- " + shlex.quote(remote)
        argv = ["ssh", "-T", "-i", self.key_file, "-p", str(self.port), "-o", "BatchMode=yes",
                "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes", "-o",
                "UserKnownHostsFile=" + self.known_hosts, self.username + "@" + self.host, command]
        try:
            result = self.runner(argv, capture_output=True, timeout=self.timeout, check=True)
        except (subprocess.SubprocessError, OSError):
            raise RuntimeError("SSH collection failed") from None
        if len(result.stdout) > self.max_bytes:
            raise ValueError("Artifact exceeds collection size limit")
        return result.stdout


def bootstrap_tofu(client, ledger, attempt_id, project_id, key_file, known_hosts, *, runner=subprocess.run):
    """Pin the first observed SSH host key for an exact active Pod.

    This trust-on-first-use step cannot authenticate the first key against an
    independent source. Keep the resulting file private and reuse it strictly.
    """
    attempt = ledger.get(attempt_id)
    if attempt["project_id"] != project_id or attempt["state"] != "active" or not attempt["pod_id"]:
        raise AttemptError("No active attempt in this project")
    pod = client.pod(attempt["pod_id"])
    if pod.get("id") != attempt["pod_id"] or pod.get("name") != attempt["pod_name"] or pod.get("status") != "RUNNING":
        raise AttemptError("Pod identity mismatch before SSH bootstrap")
    ssh = pod.get("ssh")
    endpoint = ssh.get("direct") if isinstance(ssh, dict) else None
    if not isinstance(endpoint, dict):
        raise AttemptError("Direct SSH endpoint not available")
    known_hosts = Path(known_hosts)
    known_hosts.parent.mkdir(parents=True, exist_ok=True)
    known_hosts.touch(exist_ok=True)
    connection = SSHFetcher(endpoint.get("host"), endpoint.get("port"), endpoint.get("username"), key_file, known_hosts, "/")
    argv = ["ssh", "-T", "-i", connection.key_file, "-p", str(connection.port), "-o", "BatchMode=yes",
            "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=accept-new", "-o",
            "UserKnownHostsFile=" + connection.known_hosts, connection.username + "@" + connection.host, "true"]
    try:
        runner(argv, stdin=subprocess.DEVNULL, capture_output=True, timeout=20, check=True)
    except (subprocess.SubprocessError, OSError):
        raise RuntimeError("SSH host bootstrap failed") from None
