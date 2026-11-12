"""Exact-run cleanup observations for a separately launched supervisor."""
import json
import math
import os
import time
from pathlib import Path
from runpod_experiments.attempts import AttemptError, reconcile_candidate
from runpod_experiments.contracts import (
    FailureCategory, canonical_json_bytes, encode_document, sha256_bytes,
)
from runpod_experiments.errors import ExecutionError
from runpod_experiments.ownership import delete_attempt
from runpod_experiments.ownership import request_delete_for_store_attempt
from runpod_experiments.provider import ProviderAdapter
from runpod_experiments.runpod import ProviderError


class CleanupMonitor:
    def __init__(self, client, ledger, attempt_id, project_id, policy=None):
        self.client, self.ledger = client, ledger
        self.attempt_id, self.project_id = attempt_id, project_id
        self.policy = policy
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
        if attempt["state"] == "cancelled" and not attempt["create_claimed"]:
            return "not_created"
        if attempt["state"] == "reserved" and self.stopping:
            try:
                self.ledger.cancel_uncreated(self.attempt_id)
                return "not_created"
            except AttemptError:
                # A concurrent create claim won; inspect its new state.
                attempt = self.ledger.get(self.attempt_id)
        if attempt["state"] == "creating" and attempt["reconcilable"]:
            candidate = reconcile_candidate(self.ledger, self.attempt_id, self.client.pods())
            if candidate is not None:
                detail = self.client.pod(candidate)
                if detail.get("id") != candidate or detail.get("name") != attempt["pod_name"]:
                    raise AttemptError("Create candidate detail identity mismatch")
                self.ledger.bind(self.attempt_id, candidate)
                attempt = self.ledger.get(self.attempt_id)
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
        if self.policy is not None and self.policy.violation(pod):
            self.stopping = True
        if self.stopping:
            delete_attempt(self.client, self.ledger, self.attempt_id, self.project_id)
            return "delete_requested"
        return "active"


class StoreCleanupMonitor:
    """Cleanup monitor backed by the versioned execution store and provider adapter."""

    def __init__(self, provider, store, attempt_id, project_id, *, clock=time.time):
        if project_id != store.project_id:
            raise ExecutionError(FailureCategory.AUTHORIZATION, "attempt_outside_project")
        self.client, self.store = provider, store
        self.attempt_id, self.project_id = attempt_id, project_id
        self.project_policy = store.project_policy_for_attempt(attempt_id)
        self.resource_profile, self.selected_region = store.selected_resource_for_attempt(attempt_id)
        self.policy = None
        self.policy_sha256 = sha256_bytes(encode_document(
            "project_policy", self.project_policy))
        self._provider = ProviderAdapter(provider)
        self._clock = clock
        self._required_absences = 3
        self._absence_evidence = []
        self.stopping = False
        self.stop_reason = None

    def _reset_absence(self):
        self._absence_evidence.clear()

    def _record_absence(self, attempt, inventory, detail_status):
        self._absence_evidence.append({
            "observed_epoch": int(self._clock()),
            "inventory": [
                {"pod_id": item.pod_id, "name": item.name, "status": item.status}
                for item in inventory
            ],
            "detail_status": detail_status,
        })
        if len(self._absence_evidence) < self._required_absences:
            return "confirming_absence"
        evidence = sha256_bytes(canonical_json_bytes({
            "attempt_id": self.attempt_id,
            "project_id": self.project_id,
            "observations": self._absence_evidence[-self._required_absences:],
        }))
        if attempt["state"] == "stopping" and attempt["pod_id"]:
            self.store.confirm_termination(self.attempt_id, attempt["pod_id"], evidence)
            return "terminated"
        if attempt["state"] == "reconciliation_required":
            self.store.confirm_absence(self.attempt_id, evidence)
            return "not_created" if attempt["pod_id"] is None else "terminated"
        return "unresolved"

    def _list_attempt_candidates(self, pod_name):
        inventory = self._provider.list_project_pods(self.project_policy)
        matches = [item for item in inventory if item.name == pod_name]
        if len(matches) > 1:
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "attempt_candidate_ambiguous")
        return inventory, matches

    def observe(self, stop=False):
        if type(stop) is not bool:
            raise ValueError("Stop must be a boolean")
        self.stopping = self.stopping or stop
        self.store.verify_attempt_lifecycle_projection(self.attempt_id)
        attempt = self.store.get_attempt(self.attempt_id)
        if attempt["project_id"] != self.project_id:
            raise ExecutionError(FailureCategory.AUTHORIZATION, "attempt_outside_project")
        if attempt["state"] == "terminated":
            return "terminated"
        if attempt["state"] == "cancelled":
            return "not_created"

        if attempt["state"] == "reserved":
            if not self.stopping:
                return "reserved"
            try:
                self.store.cancel_uncreated(self.attempt_id)
                return "not_created"
            except ExecutionError:
                attempt = self.store.get_attempt(self.attempt_id)

        if attempt["state"] == "active" and self.stopping:
            try:
                self.store.request_stop(self.attempt_id)
            except ExecutionError:
                attempt = self.store.get_attempt(self.attempt_id)
            else:
                attempt = self.store.get_attempt(self.attempt_id)

        if attempt["state"] in ("create_claimed", "reconciliation_required") and not attempt["pod_id"]:
            try:
                inventory, candidates = self._list_attempt_candidates(attempt["pod_name"])
            except Exception:
                self._reset_absence()
                raise
            if candidates:
                candidate = candidates[0]
                try:
                    detail = self._provider.get_attempt_pod(
                        candidate.pod_id, attempt["pod_name"], self.project_policy,
                        resource_profile=self.resource_profile,
                        selected_region=self.selected_region)
                except Exception:
                    self._reset_absence()
                    raise
                self.store.bind_pod(
                    self.attempt_id, candidate.pod_id, attempt["pod_name"])
                self._reset_absence()
                attempt = self.store.get_attempt(self.attempt_id)
                if candidate.status == "TERMINATED" or detail.status == "TERMINATED":
                    self.store.require_reconciliation(self.attempt_id, "candidate_terminated")
                    return "inventory_not_reconciled"
                if detail.resource_violation is not None:
                    self.stopping = True
                    self.stop_reason = detail.resource_violation
                if self.stopping:
                    self.store.request_stop(self.attempt_id)
                    return request_delete_for_store_attempt(
                        self.client, self.store, self.attempt_id, self.project_id)
                return "active"

            if attempt["state"] == "create_claimed":
                if not self.stopping and self._clock() < attempt["deadline_epoch"]:
                    self._reset_absence()
                    return "unresolved"
                reason = "stop_requested" if self.stopping else "supervisor_deadline"
                self.store.require_reconciliation(self.attempt_id, reason)
                attempt = self.store.get_attempt(self.attempt_id)
            self._absence_evidence.append({
                "observed_epoch": int(self._clock()),
                "inventory": [
                    {"pod_id": item.pod_id, "name": item.name, "status": item.status}
                    for item in inventory
                ],
                "detail_status": "no_candidate",
            })
            if len(self._absence_evidence) < self._required_absences:
                return "confirming_absence"
            evidence = sha256_bytes(canonical_json_bytes({
                "attempt_id": self.attempt_id,
                "project_id": self.project_id,
                "observations": self._absence_evidence[-self._required_absences:],
            }))
            self.store.confirm_absence(self.attempt_id, evidence)
            return "not_created"

        if attempt["state"] not in ("active", "stopping", "reconciliation_required") or not attempt["pod_id"]:
            return "unresolved"

        try:
            inventory, candidates = self._list_attempt_candidates(attempt["pod_name"])
        except Exception:
            self._reset_absence()
            raise
        if candidates and candidates[0].pod_id != attempt["pod_id"]:
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "attempt_identity_mismatch")
        try:
            detail = self._provider.get_attempt_pod(
                attempt["pod_id"], attempt["pod_name"], self.project_policy,
                resource_profile=self.resource_profile,
                selected_region=self.selected_region)
        except ExecutionError as error:
            if error.code != "attempt_pod_missing":
                self._reset_absence()
                raise
            detail = None
        except Exception:
            self._reset_absence()
            raise

        if detail is None:
            if candidates:
                self._reset_absence()
                return "inventory_not_reconciled"
            if attempt["state"] == "active":
                self.store.require_reconciliation(self.attempt_id, "pod_missing")
                attempt = self.store.get_attempt(self.attempt_id)
            return self._record_absence(attempt, inventory, "detail_missing")

        if detail.status == "TERMINATED":
            if candidates:
                self._reset_absence()
                return "inventory_not_reconciled"
            if attempt["state"] == "active":
                self.store.require_reconciliation(self.attempt_id, "pod_terminated")
                attempt = self.store.get_attempt(self.attempt_id)
            return self._record_absence(attempt, inventory, "detail_terminated")

        if not candidates or candidates[0].status == "TERMINATED":
            self._reset_absence()
            return "inventory_not_reconciled"
        if detail.resource_violation is not None:
            self.stopping = True
            self.stop_reason = detail.resource_violation
        self._reset_absence()
        if attempt["state"] == "reconciliation_required":
            self.store.bind_pod(self.attempt_id, attempt["pod_id"], attempt["pod_name"])
            attempt = self.store.get_attempt(self.attempt_id)
        if self.stopping or attempt["state"] == "stopping":
            if attempt["state"] == "active":
                self.store.request_stop(self.attempt_id)
            return request_delete_for_store_attempt(
                self.client, self.store, self.attempt_id, self.project_id)
        return "active"


def run_supervisor(monitor, directory, deadline_epoch, *, interval=10, error_limit=3,
                   clock=time.time, monotonic=time.monotonic, sleep=time.sleep, max_polls=None):
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
    policy_sha256 = getattr(monitor, "policy_sha256", None)
    policy = getattr(monitor, "policy", None)
    if policy_sha256 is None and policy is not None:
        policy_sha256 = policy.fingerprint
    state = {"ready": False, "stopping": stop_path.exists(), "terminated": False,
             "pid": os.getpid(), "attempt_id": monitor.attempt_id, "project_id": monitor.project_id,
             "deadline_epoch": deadline_epoch,
             "policy_sha256": policy_sha256}
    if getattr(monitor, "stop_reason", None) is not None:
        state["stop_reason"] = monitor.stop_reason
    initial_remaining = max(0, deadline_epoch - clock())
    monotonic_start = monotonic()
    while max_polls is None or polls < max_polls:
        if clock() >= deadline_epoch or monotonic() - monotonic_start >= initial_remaining or stop_path.exists():
            state["stopping"] = True
        try:
            # An unresolved attempt still requires a working provider connection
            # before a caller can consider the supervisor ready.
            if not state["ready"]:
                monitor.client.pods()
            outcome = monitor.observe(stop=state["stopping"])
            state["stopping"] = state["stopping"] or monitor.stopping
            if getattr(monitor, "stop_reason", None) is not None:
                state["stop_reason"] = monitor.stop_reason
            state["ready"] = True
            errors = 0
            state["outcome"] = outcome
            state["terminated"] = outcome == "terminated"
            state["not_created"] = outcome == "not_created"
        except Exception as error:
            errors += 1
            state["stopping"] = state["stopping"] or monitor.stopping
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
        if state["terminated"] or state.get("not_created"):
            return state
        if max_polls is not None and polls >= max_polls:
            return state
        sleep(interval)
    return state
