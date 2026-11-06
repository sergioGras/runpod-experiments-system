"""Start a bounded command on the exact project-owned Pod via direct SSH."""
import shlex
import subprocess
from pathlib import Path, PurePosixPath

from runpod_experiments.attempts import AttemptError
from runpod_experiments.ssh import SSHFetcher


def start_python(client, ledger, attempt_id, project_id, key_file, known_hosts, remote_root,
                 script, arguments, timeout_seconds, log_path, *, popen=subprocess.Popen):
    attempt = ledger.get(attempt_id)
    if attempt["project_id"] != project_id or attempt["state"] != "active" or not attempt["pod_id"]:
        raise AttemptError("No active attempt in this project")
    pod = client.pod(attempt["pod_id"])
    if pod.get("id") != attempt["pod_id"] or pod.get("name") != attempt["pod_name"] or pod.get("status") != "RUNNING":
        raise AttemptError("Pod identity mismatch before execution")
    ssh = pod.get("ssh")
    endpoint = ssh.get("direct") if isinstance(ssh, dict) else None
    if not isinstance(endpoint, dict):
        raise AttemptError("Direct SSH endpoint not available")
    connection = SSHFetcher(endpoint.get("host"), endpoint.get("port"), endpoint.get("username"),
                            key_file, known_hosts, remote_root)
    if (not isinstance(script, str) or not script or "\\" in script or "\x00" in script or
            PurePosixPath(script).is_absolute() or ".." in PurePosixPath(script).parts or
            not isinstance(arguments, list) or any(not isinstance(arg, str) or "\x00" in arg for arg in arguments) or
            type(timeout_seconds) is not int or timeout_seconds <= 0):
        raise ValueError("Invalid reviewed workload command")
    remote_command = "cd -- " + shlex.quote(remote_root) + " && exec "
    remote_command += shlex.join(["timeout", str(timeout_seconds), "python3", script, *arguments])
    argv = ["ssh", "-T", "-i", connection.key_file, "-p", str(connection.port), "-o", "BatchMode=yes",
            "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes", "-o",
            "UserKnownHostsFile=" + connection.known_hosts, connection.username + "@" + connection.host, remote_command]
    with Path(log_path).open("ab") as log:
        return popen(argv, stdin=subprocess.DEVNULL, stdout=log, stderr=log, close_fds=True,
                     creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0)
