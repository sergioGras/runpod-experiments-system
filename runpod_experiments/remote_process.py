"""Start a bounded command on the exact project-owned Pod via direct SSH."""
import json
import re
import shlex
import subprocess
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

from runpod_experiments.attempts import AttemptError
from runpod_experiments.execution_store import ExecutionStore
from runpod_experiments.ssh import SSHFetcher
from runpod_experiments.store_transport import resolve_store_ssh_context


_SECRET_ENVIRONMENT_BOOTSTRAP = (
    "import json,os,subprocess,sys\n"
    "raw=sys.stdin.buffer.read(16385)\n"
    "if len(raw)>16384: raise SystemExit(125)\n"
    "values=json.loads(raw)\n"
    "if not isinstance(values,dict): raise SystemExit(125)\n"
    "env=os.environ.copy()\n"
    "for key,value in values.items():\n"
    " if not isinstance(key,str) or not isinstance(value,str) or '\\x00' in value: raise SystemExit(125)\n"
    " env[key]=value\n"
    "command=['timeout','--kill-after=5',sys.argv[1],'python3',sys.argv[2],*sys.argv[3:]]\n"
    "raise SystemExit(subprocess.call(command,env=env))\n"
)


def _secret_payload(values):
    if values is None:
        return None
    if not isinstance(values, Mapping):
        raise ValueError("Secret environment is invalid")
    copied = {}
    for name, value in values.items():
        if (not isinstance(name, str) or
                re.fullmatch(r"[A-Z_][A-Z0-9_]{0,63}", name) is None or
                not isinstance(value, str) or not value or "\x00" in value):
            raise ValueError("Secret environment is invalid")
        try:
            if len(value.encode("utf-8")) > 8192:
                raise ValueError("Secret environment exceeds its bound")
        except UnicodeEncodeError:
            raise ValueError("Secret environment is invalid") from None
        copied[name] = value
    try:
        payload = json.dumps(copied, ensure_ascii=True, separators=(",", ":"),
                             allow_nan=False).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError):
        raise ValueError("Secret environment is invalid") from None
    if not payload or len(payload) > 16_384:
        raise ValueError("Secret environment exceeds its bound")
    return payload


def start_python(client, ledger, attempt_id, project_id, key_file, known_hosts, remote_root,
                 script, arguments, timeout_seconds, log_path, *,
                 secret_environment=None, popen=subprocess.Popen):
    secret_payload = _secret_payload(secret_environment)
    if isinstance(ledger, ExecutionStore):
        target = resolve_store_ssh_context(
            client, ledger, attempt_id, project_id, known_hosts=known_hosts)
        endpoint = target.endpoint
        host, port, username = endpoint.host, endpoint.port, endpoint.username
    else:
        attempt = ledger.get(attempt_id)
        if attempt["project_id"] != project_id or attempt["state"] != "active" or not attempt["pod_id"]:
            raise AttemptError("No active attempt in this project")
        pod = client.pod(attempt["pod_id"])
        if (pod.get("id") != attempt["pod_id"] or pod.get("name") != attempt["pod_name"] or
                pod.get("status") != "RUNNING"):
            raise AttemptError("Pod identity mismatch before execution")
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
            not isinstance(arguments, list) or any(not isinstance(arg, str) or "\x00" in arg for arg in arguments) or
            type(timeout_seconds) is not int or timeout_seconds <= 0):
        raise ValueError("Invalid reviewed workload command")
    if secret_payload is None:
        remote_command = "cd -- " + shlex.quote(remote_root) + " && exec "
        remote_command += shlex.join(["timeout", str(timeout_seconds), "python3", script, *arguments])
    else:
        remote_command = "cd -- " + shlex.quote(remote_root) + " && exec "
        remote_command += shlex.join(["python3", "-c", _SECRET_ENVIRONMENT_BOOTSTRAP,
                                      str(timeout_seconds), script, *arguments])
    argv = ["ssh", "-T", "-i", connection.key_file, "-p", str(connection.port), "-o", "BatchMode=yes",
            "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes", "-o",
            "UserKnownHostsFile=" + connection.known_hosts, connection.username + "@" + connection.host, remote_command]
    with Path(log_path).open("ab") as log:
        process = popen(
            argv, stdin=(subprocess.PIPE if secret_payload is not None else subprocess.DEVNULL),
            stdout=(subprocess.DEVNULL if secret_payload is not None else log),
            stderr=(subprocess.DEVNULL if secret_payload is not None else log),
            close_fds=True,
            creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0)
        if secret_payload is not None:
            stream = getattr(process, "stdin", None)
            try:
                if stream is None:
                    raise OSError
                if stream.write(secret_payload) != len(secret_payload):
                    raise OSError
                stream.flush()
                stream.close()
            except (OSError, TypeError, ValueError):
                try:
                    if stream is not None:
                        stream.close()
                except Exception:
                    pass
                try:
                    process.kill()
                except Exception:
                    pass
                raise RuntimeError("Ephemeral secret delivery failed") from None
        return process


start_python.supports_ephemeral_secrets = True
