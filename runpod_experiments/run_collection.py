"""Identity-checked periodic result collection for an owned attempt."""
import json
import os
import time
from pathlib import Path

from runpod_experiments.attempts import AttemptError
from runpod_experiments.collection import collect_once
from runpod_experiments.execution_store import ExecutionStore
from runpod_experiments.proxy_ssh import SSHProxyFetcher
from runpod_experiments.ssh import SSHFetcher
from runpod_experiments.store_transport import resolve_store_ssh_context


def collect_owned_once(client, ledger, attempt_id, project_id, store, names, key_file,
                       known_hosts, remote_root, previous=None, *, ssh_route="direct",
                       fetcher_type=None):
    if isinstance(ledger, ExecutionStore):
        target = resolve_store_ssh_context(client, ledger, attempt_id, project_id, route=ssh_route)
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
            raise AttemptError("Pod identity mismatch during collection")
        ssh = pod.get("ssh")
        endpoint = ssh.get("direct") if isinstance(ssh, dict) else None
        if not isinstance(endpoint, dict):
            raise AttemptError("Direct SSH endpoint not available")
        host, port, username = (endpoint.get("host"), endpoint.get("port"),
                                endpoint.get("username"))
    if fetcher_type is None:
        fetcher_type = (SSHProxyFetcher if isinstance(ledger, ExecutionStore) and endpoint.route == "proxy"
                        else SSHFetcher)
    fetch = fetcher_type(host, port, username,
                         key_file, known_hosts, remote_root)
    return collect_once(store, fetch, names, attempt_id, previous)


def collect_loop(client, ledger, attempt_id, project_id, store, names, key_file, known_hosts,
                 remote_root, control_directory, *, ssh_route="direct", interval=30,
                 sleep=time.sleep, clock=time.time, max_polls=None):
    """Keep collecting until collection.stop is present; then try once more."""
    if ssh_route not in ("direct", "proxy") or (
            ssh_route == "proxy" and not isinstance(ledger, ExecutionStore)):
        raise ValueError("Proxy SSH requires an execution-store attempt")
    if not isinstance(interval, (int, float)) or isinstance(interval, bool) or not 0 < interval < float("inf"):
        raise ValueError("Invalid collection interval")
    directory = Path(control_directory)
    directory.mkdir(parents=True, exist_ok=True)
    previous, polls = {}, 0
    while max_polls is None or polls < max_polls:
        stopping = (directory / "collection.stop").exists()
        terminal = False
        try:
            if isinstance(ledger, ExecutionStore):
                ledger.verify_attempt_lifecycle_projection(attempt_id)
                attempt = ledger.get_attempt(attempt_id)
            else:
                attempt = ledger.get(attempt_id)
            terminal = attempt["project_id"] == project_id and attempt["state"] in {"terminated", "cancelled"}
            if terminal:
                errors = {}
            else:
                previous, errors = collect_owned_once(client, ledger, attempt_id, project_id, store, names,
                                                       key_file, known_hosts, remote_root, previous,
                                                       ssh_route=ssh_route)
        except Exception as error:
            errors = {"collection": type(error).__name__}
        polls += 1
        state = {"attempt_id": attempt_id, "heartbeat_epoch": clock(), "polls": polls,
                 "stopping": stopping, "errors": errors}
        try:
            path = directory / "collection.json"
            temp = path.with_suffix(".tmp")
            temp.write_text(json.dumps(state, sort_keys=True) + "\n", encoding="utf-8")
            os.replace(temp, path)
        except OSError:
            # Keep attempting to save results if heartbeat storage fails.
            pass
        if stopping or terminal or (max_polls is not None and polls >= max_polls):
            return state
        if (directory / "collection.stop").exists():
            continue
        sleep(interval)
