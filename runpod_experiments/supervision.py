"""Exact-run cleanup observations for a separately launched supervisor."""
import json
import math
import os
import time
from pathlib import Path
from runpod_experiments.attempts import AttemptError
from runpod_experiments.ownership import delete_attempt
from runpod_experiments.runpod import ProviderError


class CleanupMonitor:
    def __init__(self, client, ledger, attempt_id, project_id):
        self.client, self.ledger = client, ledger
        self.attempt_id, self.project_id = attempt_id, project_id
        self.absent_observations = 0
        self.stopping = False

    def observe(self, stop=False):
        if type(stop) is not bool:
            raise ValueError("Stop must be a boolean")
        self.stopping = self.stopping or stop
        attempt = self.ledger.get(self.attempt_id)
        if attempt["project_id"] != self.project_id:
            raise AttemptError("Wrong project")
        if attempt["state"] == "terminated":
            return "terminated"
        if attempt["state"] != "active" or not attempt["pod_id"]:
            return "unresolved"
        try:
            pods = self.client.pods()
        except Exception:
            self.absent_observations = 0
            raise
        matches = [p for p in pods if p.get("name") == attempt["pod_name"] and p.get("status") != "TERMINATED"]
        if len(matches) > 1 or (matches and matches[0].get("id") != attempt["pod_id"]):
            raise AttemptError("Ambiguous provider inventory")
        try:
            pod = self.client.pod(attempt["pod_id"])
        except ProviderError as error:
            if error.status != 404 or matches:
                self.absent_observations = 0
                raise
            pod = None
        if pod is not None and (pod.get("id") != attempt["pod_id"] or pod.get("name") != attempt["pod_name"]):
            raise AttemptError("Provider detail identity mismatch")
        absent = not matches and (pod is None or pod.get("status") == "TERMINATED")
        if absent:
            self.absent_observations += 1
            if self.absent_observations >= 3:
                self.ledger.terminated(self.attempt_id, attempt["pod_id"])
                return "terminated"
            return "confirming_absence"
        self.absent_observations = 0
        if pod is not None and pod.get("status") == "TERMINATED":
            return "inventory_not_reconciled"
        if self.stopping:
            delete_attempt(self.client, self.ledger, self.attempt_id, self.project_id)
            return "delete_requested"
        return "active"


def run_supervisor(monitor, directory, deadline_epoch, *, interval=10, error_limit=3,
                   clock=time.time, sleep=time.sleep, max_polls=None):
    """Persist heartbeats and keep attempting cleanup after a stop or errors.

    Caller must start and qualify this loop in a separate process before any
    create. An unresolved create is never treated as termination. The control
    directory and deadline must come from a durable approved attempt plan.
    """
    if (isinstance(deadline_epoch, bool) or not isinstance(deadline_epoch, (int, float)) or
            not math.isfinite(deadline_epoch) or deadline_epoch <= 0 or
            isinstance(interval, bool) or not isinstance(interval, (int, float)) or
            not math.isfinite(interval) or interval <= 0 or type(error_limit) is not int or error_limit <= 0):
        raise ValueError("Invalid supervisor deadline or poll settings")
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    stop_path, state_path = directory / "stop.request", directory / "supervisor.json"
    errors, polls = 0, 0
    state = {"ready": False, "stopping": stop_path.exists(), "terminated": False}
    while max_polls is None or polls < max_polls:
        if clock() >= deadline_epoch or stop_path.exists():
            state["stopping"] = True
        try:
            # An unresolved attempt still requires a working provider connection
            # before a caller can consider the supervisor ready.
            if not state["ready"]:
                monitor.client.pods()
            outcome = monitor.observe(stop=state["stopping"])
            state["ready"] = True
            errors = 0
            state["outcome"] = outcome
            state["terminated"] = outcome == "terminated"
        except Exception as error:
            errors += 1
            state["last_error_type"] = type(error).__name__
            if errors >= error_limit:
                state["stopping"] = True
        if state["stopping"]:
            try:
                stop_path.touch(exist_ok=True)
            except OSError:
                pass
        polls += 1
        state.update({"heartbeat_epoch": clock(), "polls": polls, "consecutive_errors": errors})
        try:
            temporary = state_path.with_suffix(".tmp")
            temporary.write_text(json.dumps(state, sort_keys=True) + "\n", encoding="utf-8")
            os.replace(temporary, state_path)
        except OSError:
            # A local telemetry error must cause cleanup, not end monitoring.
            state["stopping"] = True
        if state["terminated"]:
            return state
        if max_polls is not None and polls >= max_polls:
            return state
        sleep(interval)
    return state
