"""Bounded binary SSH session to one exact active project Pod."""
from __future__ import annotations

import shlex
import subprocess
from pathlib import Path, PurePosixPath

from runpod_experiments.binary_process import BinaryProcess
from runpod_experiments.attempts import AttemptError
from runpod_experiments.execution_store import ExecutionStore
from runpod_experiments.ssh import SSHFetcher
from runpod_experiments.store_transport import resolve_store_ssh_context


class SSHInteractiveProcess(BinaryProcess):
    """Long-lived raw stdin/stdout process with strict pinned SSH identity.

    The caller must keep the independent provider supervisor alive, bound its
    message sizes, and monitor the provider-owned attempt while using the pipe.
    stderr is written to a private local log file, never mixed into the binary
    stdout protocol.
    """

    def __init__(self, client, ledger, attempt_id, project_id, key_file, known_hosts,
                 remote_root, script, arguments, timeout_seconds, log_path, *,
                 popen=subprocess.Popen, output_chunk_bytes=65536, queued_chunks=32):
        if isinstance(ledger, ExecutionStore):
            target = resolve_store_ssh_context(
                client, ledger, attempt_id, project_id, known_hosts=known_hosts)
            endpoint = target.endpoint
            host, port, username = endpoint.host, endpoint.port, endpoint.username
        else:
            attempt = ledger.get(attempt_id)
            if (attempt["project_id"] != project_id or attempt["state"] != "active" or
                    not attempt["pod_id"]):
                raise AttemptError("No active attempt in this project")
            pod = client.pod(attempt["pod_id"])
            if (pod.get("id") != attempt["pod_id"] or pod.get("name") != attempt["pod_name"] or
                    pod.get("status") != "RUNNING"):
                raise AttemptError("Pod identity mismatch before interactive SSH")
            ssh = pod.get("ssh")
            endpoint = ssh.get("direct") if isinstance(ssh, dict) else None
            if not isinstance(endpoint, dict):
                raise AttemptError("Direct SSH endpoint not available")
            host, port, username = (endpoint.get("host"), endpoint.get("port"),
                                    endpoint.get("username"))
        connection = SSHFetcher(host, port, username,
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
        command += shlex.join(["timeout", "--kill-after=5", str(timeout_seconds), "python3", script, *arguments])
        argv = ["ssh", "-T", "-i", connection.key_file, "-p", str(connection.port),
                "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "-o", "ConnectionAttempts=1",
                "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=2",
                "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes",
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
        super().__init__(self.process, stderr=self._stderr, lifetime_seconds=timeout_seconds,
                         output_chunk_bytes=output_chunk_bytes, queued_chunks=queued_chunks)
