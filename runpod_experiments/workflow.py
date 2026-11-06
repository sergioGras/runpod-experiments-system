"""Coordinate an already approved, already created single-Pod workload."""
import json
import math
import subprocess
import time
from pathlib import Path

from runpod_experiments.remote_process import start_python
from runpod_experiments.collection_process import launch as launch_collector
from runpod_experiments.upload import upload_bundle
from runpod_experiments.supervision import CleanupMonitor
from runpod_experiments.attempts import AttemptError


def run_owned_workload(client, ledger, attempt_id, project_id, store, *, source_root, manifest,
                       key_file, known_hosts, remote_root, script, arguments, timeout_seconds,
                       result_names, control_directory, supervisor, clock=time.time, sleep=time.sleep):
    """Upload approved files, run, collect incrementally, then request cleanup.

    The caller must have checked approval, budgets, host key and endpoint. The
    supervisor is independent and remains responsible for provider termination.
    """
    directory = Path(control_directory)
    directory.mkdir(parents=True, exist_ok=True)
    collector = None
    process = None
    try:
        if not isinstance(manifest, dict) or script not in manifest:
            raise ValueError("Workload script is outside the reviewed upload manifest")
        upload_started = time.monotonic()
        upload_bundle(client, ledger, attempt_id, project_id, source_root, manifest,
                      key_file, known_hosts, remote_root)
        upload_seconds = time.monotonic() - upload_started
        collector = launch_collector(ledger.path, attempt_id, project_id, directory, store.directory,
                                     result_names, key_file, known_hosts, remote_root)
        execution_started = time.monotonic()
        process = start_python(client, ledger, attempt_id, project_id, key_file, known_hosts,
                               remote_root, script, arguments, timeout_seconds, directory / "workload.log")
        while process.poll() is None:
            if supervisor.poll() is not None:
                raise RuntimeError("Independent supervisor exited during execution")
            if collector.poll() is not None:
                raise RuntimeError("Independent result collector exited during execution")
            state = json.loads((directory / "supervisor.json").read_text(encoding="utf-8"))
            heartbeat = state.get("heartbeat_epoch")
            if (not isinstance(heartbeat, (int, float)) or isinstance(heartbeat, bool) or
                    not 0 <= clock() - heartbeat <= 30 or state.get("stopping")):
                raise RuntimeError("Independent supervisor is stale or stopping")
            sleep(1)
        return {"exit_code": process.returncode, "upload_seconds": round(upload_seconds, 6),
                "execution_seconds": round(time.monotonic() - execution_started, 6)}
    finally:
        try:
            if process is not None and process.poll() is None:
                process.terminate()
            if collector is not None:
                (directory / "collection.stop").touch()
                try:
                    collector.wait(timeout=65)
                except subprocess.TimeoutExpired:
                    pass
        finally:
            (directory / "stop.request").touch()


def reconcile_cleanup(client, ledger, attempt_id, project_id, control_directory, *, max_seconds=180,
                      interval=5, clock=time.monotonic, sleep=time.sleep, budget=None):
    """Retry scoped cleanup and return only confirmed termination or timeout.

    The independent supervisor must remain armed if this helper times out.
    """
    if (isinstance(max_seconds, bool) or not isinstance(max_seconds, (int, float)) or not math.isfinite(max_seconds) or max_seconds <= 0 or
            isinstance(interval, bool) or not isinstance(interval, (int, float)) or not math.isfinite(interval) or interval <= 0):
        raise ValueError("Invalid cleanup wait")
    (Path(control_directory) / "stop.request").touch()
    monitor = CleanupMonitor(client, ledger, attempt_id, project_id)
    attempt = ledger.get(attempt_id)
    if attempt["project_id"] == project_id and attempt["state"] == "reserved" and not attempt["create_claimed"]:
        try:
            ledger.cancel_uncreated(attempt_id)
            if budget is not None:
                budget.release_uncreated(attempt_id)
            return {"terminated": False, "no_create": True, "last_error_type": None}
        except AttemptError:
            # Another controller may have claimed creation concurrently.
            pass
    deadline = clock() + max_seconds
    last_error = None
    while clock() < deadline:
        try:
            outcome = monitor.observe(stop=True)
            if outcome == "not_created":
                if budget is not None:
                    budget.release_uncreated(attempt_id)
                return {"terminated": False, "no_create": True, "last_error_type": last_error}
            if outcome == "terminated":
                return {"terminated": True, "last_error_type": last_error}
        except Exception as error:
            last_error = type(error).__name__
        sleep(interval)
    return {"terminated": False, "last_error_type": last_error}
