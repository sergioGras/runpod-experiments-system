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
from runpod_experiments.transfer import (
    DEFAULT_COLLECTION_TIMEOUT_SECONDS, DEFAULT_TRANSFER_CHUNK_BYTES,
    validate_chunk_bytes, validate_timeout_seconds,
)


class _DeadlineBoundFetcher:
    """Clamp each built-in SSH operation to the remaining attempt deadline."""

    def __init__(self, fetcher, timeout_seconds, deadline_epoch, clock):
        self.fetcher = fetcher
        self.timeout_seconds = timeout_seconds
        self.deadline_epoch = deadline_epoch
        self.clock = clock

    def _clamp(self):
        timeout = self.timeout_seconds
        if self.deadline_epoch is not None:
            remaining = self.deadline_epoch - self.clock()
            if remaining <= 0:
                raise TimeoutError("Attempt deadline reached during collection")
            timeout = min(timeout, remaining)
        self.fetcher.timeout = timeout

    def __call__(self, name):
        self._clamp()
        return self.fetcher(name)

    def fetch_limited(self, name, max_bytes):
        self._clamp()
        return self.fetcher.fetch_limited(name, max_bytes)


def collect_owned_once(client, ledger, attempt_id, project_id, store, names, key_file,
                       known_hosts, remote_root, previous=None, *, ssh_route="direct",
                       fetcher_type=None, max_file_bytes=10_000_000,
                       max_total_bytes=100_000_000, per_file_limits=None,
                       timeout_seconds=DEFAULT_COLLECTION_TIMEOUT_SECONDS,
                       chunk_bytes=DEFAULT_TRANSFER_CHUNK_BYTES,
                       deadline_epoch=None, clock=time.time):
    validate_timeout_seconds(timeout_seconds, "collection_timeout_seconds")
    validate_chunk_bytes(chunk_bytes, "collection_chunk_bytes")
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
            raise AttemptError("Pod identity mismatch during collection")
        ssh = pod.get("ssh")
        endpoint = ssh.get("direct") if isinstance(ssh, dict) else None
        if not isinstance(endpoint, dict):
            raise AttemptError("Direct SSH endpoint not available")
        host, port, username = (endpoint.get("host"), endpoint.get("port"),
                                endpoint.get("username"))
    use_builtin_fetcher = fetcher_type is None
    if use_builtin_fetcher:
        fetcher_type = (SSHProxyFetcher if isinstance(ledger, ExecutionStore) and endpoint.route == "proxy"
                        else SSHFetcher)
    if use_builtin_fetcher:
        options = {"max_bytes": max_file_bytes, "timeout": timeout_seconds}
        if fetcher_type is SSHProxyFetcher:
            options["chunk_bytes"] = chunk_bytes
        fetch = fetcher_type(host, port, username, key_file, known_hosts, remote_root,
                             **options)
        fetch = _DeadlineBoundFetcher(fetch, timeout_seconds, deadline_epoch, clock)
    else:
        fetch = fetcher_type(host, port, username, key_file, known_hosts, remote_root)
    return collect_once(store, fetch, names, attempt_id, previous,
                        max_file_bytes=max_file_bytes, max_total_bytes=max_total_bytes,
                        per_file_limits=per_file_limits)


def collect_loop(client, ledger, attempt_id, project_id, store, names, key_file, known_hosts,
                 remote_root, control_directory, *, ssh_route="direct", interval=30,
                 sleep=time.sleep, clock=time.time, max_polls=None,
                 max_file_bytes=10_000_000, max_total_bytes=100_000_000,
                 per_file_limits=None,
                 timeout_seconds=DEFAULT_COLLECTION_TIMEOUT_SECONDS,
                 chunk_bytes=DEFAULT_TRANSFER_CHUNK_BYTES):
    """Keep collecting until collection.stop is present; then try once more."""
    if ssh_route not in ("direct", "proxy") or (
            ssh_route == "proxy" and not isinstance(ledger, ExecutionStore)):
        raise ValueError("Proxy SSH requires an execution-store attempt")
    validate_timeout_seconds(timeout_seconds, "collection_timeout_seconds")
    validate_chunk_bytes(chunk_bytes, "collection_chunk_bytes")
    if not isinstance(interval, (int, float)) or isinstance(interval, bool) or not 0 < interval < float("inf"):
        raise ValueError("Invalid collection interval")
    directory = Path(control_directory)
    directory.mkdir(parents=True, exist_ok=True)
    previous, polls = {}, 0
    while max_polls is None or polls < max_polls:
        stopping = (directory / "collection.stop").exists()
        terminal = False
        attempt_deadline = None
        try:
            if isinstance(ledger, ExecutionStore):
                ledger.verify_attempt_lifecycle_projection(attempt_id)
                attempt = ledger.get_attempt(attempt_id)
            else:
                attempt = ledger.get(attempt_id)
            terminal = attempt["project_id"] == project_id and attempt["state"] in {"terminated", "cancelled"}
            attempt_deadline = attempt.get("deadline_epoch")
            if terminal:
                errors = {}
            else:
                previous, errors = collect_owned_once(client, ledger, attempt_id, project_id, store, names,
                                                       key_file, known_hosts, remote_root, previous,
                                                       ssh_route=ssh_route,
                                                       max_file_bytes=max_file_bytes,
                                                       max_total_bytes=max_total_bytes,
                                                       per_file_limits=per_file_limits,
                                                       timeout_seconds=timeout_seconds,
                                                       chunk_bytes=chunk_bytes,
                                                       deadline_epoch=attempt_deadline,
                                                       clock=clock)
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
