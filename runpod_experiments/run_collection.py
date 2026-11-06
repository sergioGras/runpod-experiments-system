"""Identity-checked periodic result collection for an owned attempt."""
import json
import os
import time
from pathlib import Path

from runpod_experiments.attempts import AttemptError
from runpod_experiments.collection import collect_once
from runpod_experiments.ssh import SSHFetcher


def collect_owned_once(client, ledger, attempt_id, project_id, store, names, key_file,
                       known_hosts, remote_root, previous=None, *, fetcher_type=SSHFetcher):
    attempt = ledger.get(attempt_id)
    if attempt["project_id"] != project_id or attempt["state"] != "active" or not attempt["pod_id"]:
        raise AttemptError("No active attempt in this project")
    pod = client.pod(attempt["pod_id"])
    if pod.get("id") != attempt["pod_id"] or pod.get("name") != attempt["pod_name"]:
        raise AttemptError("Pod identity mismatch during collection")
    ssh = pod.get("ssh")
    endpoint = ssh.get("direct") if isinstance(ssh, dict) else None
    if not isinstance(endpoint, dict):
        raise AttemptError("Direct SSH endpoint not available")
    fetch = fetcher_type(endpoint.get("host"), endpoint.get("port"), endpoint.get("username"),
                         key_file, known_hosts, remote_root)
    return collect_once(store, fetch, names, attempt_id, previous)


def collect_loop(client, ledger, attempt_id, project_id, store, names, key_file, known_hosts,
                 remote_root, control_directory, *, interval=30, sleep=time.sleep, clock=time.time, max_polls=None):
    """Keep collecting until collection.stop is present; then try once more."""
    if not isinstance(interval, (int, float)) or isinstance(interval, bool) or not 0 < interval < float("inf"):
        raise ValueError("Invalid collection interval")
    directory = Path(control_directory)
    directory.mkdir(parents=True, exist_ok=True)
    previous, polls = {}, 0
    while max_polls is None or polls < max_polls:
        stopping = (directory / "collection.stop").exists()
        try:
            previous, errors = collect_owned_once(client, ledger, attempt_id, project_id, store, names,
                                                   key_file, known_hosts, remote_root, previous)
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
        if stopping or (max_polls is not None and polls >= max_polls):
            return state
        sleep(interval)
