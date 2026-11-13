"""Exact-run cleanup observations for a separately launched supervisor."""
import json
import math
import os
import time
from pathlib import Path
from runpod_experiments.attempts import AttemptError, reconcile_candidate
from runpod_experiments.contracts import (
    FailureCategory, ResourceKind, StageStatus, canonical_json_bytes,
    encode_document, sha256_bytes,
)
from runpod_experiments.errors import ExecutionError
from runpod_experiments.gpu_probe import GPUProbeTransportError, probe_gpu_runtime
from runpod_experiments.gpu_runtime import (
    GPUAttestation, GPU_PROBE_SPEC_SHA256,
    GPU_RUNTIME_ATTESTATION_GRACE_SECONDS, GPURuntimeEvidenceError,
    gpu_profile_sha256, validate_gpu_attestation, validate_gpu_observation,
)
from runpod_experiments.host_identity import (
    current_boot_identity, same_boot_identity, valid_boot_identity,
)
from runpod_experiments.ownership import delete_attempt
from runpod_experiments.ownership import request_delete_for_store_attempt
from runpod_experiments.provider import ProviderAdapter
from runpod_experiments.runpod import ProviderError
from runpod_experiments.store_transport import (
    gpu_attestation_path, load_verified_gpu_attestation,
    write_gpu_attestation,
)


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

    def __init__(self, provider, store, attempt_id, project_id, *, clock=time.time,
                 gpu_ssh_key_file=None, gpu_known_hosts=None,
                 gpu_probe=probe_gpu_runtime, supervisor_directory=None):
        if project_id != store.project_id:
            raise ExecutionError(FailureCategory.AUTHORIZATION, "attempt_outside_project")
        if (gpu_ssh_key_file is None) != (gpu_known_hosts is None):
            raise ExecutionError(FailureCategory.CONFIGURATION,
                                 "gpu_probe_configuration_incomplete")
        if not callable(gpu_probe):
            raise TypeError("gpu_probe must be callable")
        self.client, self.store = provider, store
        self.attempt_id, self.project_id = attempt_id, project_id
        self.attempt_sha256 = store.get_attempt(attempt_id)["attempt_sha256"]
        self.project_policy = store.project_policy_for_attempt(attempt_id)
        self.resource_profile, self.selected_region = store.selected_resource_for_attempt(attempt_id)
        self.policy = None
        self.policy_sha256 = sha256_bytes(encode_document(
            "project_policy", self.project_policy))
        self._provider = ProviderAdapter(provider)
        self._clock = clock
        self.gpu_ssh_key_file = gpu_ssh_key_file
        self.gpu_known_hosts = gpu_known_hosts
        self.gpu_probe = gpu_probe
        self.supervisor_directory = (Path(supervisor_directory)
                                     if supervisor_directory is not None else None)
        self._required_absences = 3
        self._absence_evidence = []
        self.stopping = False
        self.stop_reason = None
        self.gpu_runtime_status = (
            "not_required" if self.resource_profile.kind is ResourceKind.CPU else
            "waiting" if gpu_ssh_key_file is not None else "not_configured")
        self.gpu_runtime_error_code = None

    def _read_attempt_detail(self, pod_id, pod_name):
        if (self.resource_profile.kind is ResourceKind.GPU and
                self.gpu_ssh_key_file is not None):
            _raw, observation = self._provider._read_attempt_detail(
                pod_id, pod_name, self.project_policy,
                resource_profile=self.resource_profile,
                selected_region=self.selected_region, gpu_probe_only=True)
            return observation
        return self._provider.get_attempt_pod(
            pod_id, pod_name, self.project_policy,
            resource_profile=self.resource_profile,
            selected_region=self.selected_region)

    def _gpu_grace_started_epoch(self, attempt_id):
        if not self.store.verify_event_chain():
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "gpu_grace_event_chain_invalid")
        events = self.store.get_stage_events(attempt_id)
        started = [event for event in events
                   if event.stage == "gpu_runtime_grace" and
                   event.status is StageStatus.STARTED]
        if len(started) > 1:
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "gpu_grace_state_ambiguous")
        if started:
            if started[0].occurred_epoch > int(self._clock()):
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                     "gpu_grace_clock_moved_backwards")
            return started[0].occurred_epoch, events
        event = self.store.append_stage_event(
            attempt_id, "gpu_runtime_grace", StageStatus.STARTED)
        return event.occurred_epoch, events + (event,)

    def _record_gpu_failure(self, code, reason):
        self.gpu_runtime_status = "rejected"
        self.gpu_runtime_error_code = code
        self.stop_reason = reason
        self.stopping = True
        try:
            events = self.store.get_stage_events(self.attempt_id)
            latest = [event for event in events
                      if event.stage == "gpu_runtime_attestation"]
            if not latest or latest[-1].status is not StageStatus.FAILED:
                self.store.append_stage_event(
                    self.attempt_id, "gpu_runtime_attestation", StageStatus.FAILED,
                    failure_category=FailureCategory.RESOURCE_MISMATCH,
                    error_code=code)
        except Exception:
            # A failed durable record leaves the attempt fail-closed; the
            # supervisor loop will retry cleanup and report its own error.
            pass

    def _gpu_attestation_outcome(self, attempt):
        """Return pending, verified, or failed for an exact running GPU Pod."""
        if self.resource_profile.kind is not ResourceKind.GPU:
            return "verified"
        if self.gpu_ssh_key_file is None:
            self.gpu_runtime_status = "not_configured"
            self._record_gpu_failure("gpu_probe_not_configured",
                                     "unverifiable_gpu_profile")
            return "failed"
        if attempt["state"] != "active" or not attempt["pod_id"]:
            self.gpu_runtime_status = "waiting"
            return "pending"

        now_epoch = int(self._clock())
        try:
            first_running_epoch, events = self._gpu_grace_started_epoch(self.attempt_id)
        except Exception:
            self.gpu_runtime_status = "error"
            self.gpu_runtime_error_code = "gpu_grace_state_unavailable"
            raise
        grace_deadline = min(
            first_running_epoch + GPU_RUNTIME_ATTESTATION_GRACE_SECONDS,
            attempt["deadline_epoch"])

        try:
            path = gpu_attestation_path(self.store, self.attempt_id)
            stage_events = [event for event in events
                            if event.stage == "gpu_runtime_attestation"]
            if stage_events and stage_events[-1].status is StageStatus.FAILED:
                self._record_gpu_failure("gpu_attestation_previously_rejected",
                                         "gpu_runtime_attestation_rejected")
                return "failed"
            if stage_events and stage_events[-1].status is StageStatus.SUCCEEDED:
                load_verified_gpu_attestation(
                    self.store, self.attempt_id, self.gpu_known_hosts,
                    now_epoch=now_epoch)
                self.gpu_runtime_status = "verified"
                self.gpu_runtime_error_code = None
                return "verified"
            if path.exists() and not stage_events:
                # A crash between the atomic sidecar replace and its hash-chain
                # event is recoverable by probing again; the bytes are not trusted.
                pass
        except ExecutionError as error:
            self._record_gpu_failure("gpu_attestation_invalid", error.code)
            return "failed"

        if self.stopping or attempt["state"] == "stopping":
            self.gpu_runtime_status = "stopping"
            return "failed"
        if now_epoch >= grace_deadline:
            self._record_gpu_failure("gpu_attestation_deadline",
                                     "gpu_runtime_attestation_deadline")
            return "failed"

        try:
            self.store.verify_attempt_lifecycle_projection(self.attempt_id)
            provisioning = self.store.provisioning_for_attempt(self.attempt_id)
            endpoint = self._provider._get_attempt_gpu_probe_endpoint(
                attempt["pod_id"], attempt["pod_name"], self.project_policy,
                resource_profile=self.resource_profile,
                selected_region=self.selected_region,
                expected_ports=provisioning.ports)
            observation, host_key_digest = self.gpu_probe(
                endpoint, self.gpu_ssh_key_file, self.gpu_known_hosts,
                observed_epoch=now_epoch)
            validate_gpu_observation(observation, self.resource_profile)
            expected = {
                "project_id": attempt["project_id"], "job_id": attempt["job_id"],
                "attempt_id": self.attempt_id,
                "attempt_sha256": attempt["attempt_sha256"],
                "plan_sha256": self.store.plan_sha256_for_attempt(self.attempt_id),
                "pod_id": attempt["pod_id"], "pod_name": attempt["pod_name"],
                "host_key_policy_sha256": host_key_digest,
            }
            attestation = GPUAttestation(
                project_id=attempt["project_id"], job_id=attempt["job_id"],
                attempt_id=self.attempt_id,
                attempt_sha256=attempt["attempt_sha256"],
                plan_sha256=expected["plan_sha256"], pod_id=attempt["pod_id"],
                pod_name=attempt["pod_name"],
                profile_sha256=gpu_profile_sha256(self.resource_profile),
                host_key_policy_sha256=host_key_digest,
                probe_sha256=GPU_PROBE_SPEC_SHA256, observation=observation)
            validate_gpu_attestation(
                attestation, profile=self.resource_profile,
                expected=expected, now_epoch=now_epoch)
            directory = (self.supervisor_directory if self.supervisor_directory is not None
                         else path.parent)
            digest = write_gpu_attestation(
                self.store, self.attempt_id, directory, attestation,
                now_epoch=now_epoch)
            self.store.append_stage_event(
                self.attempt_id, "gpu_runtime_grace", StageStatus.SUCCEEDED,
                evidence_sha256=(digest,))
            self.gpu_runtime_status = "verified"
            self.gpu_runtime_error_code = None
            return "verified"
        except GPUProbeTransportError as error:
            code = error.args[0] if error.args and isinstance(error.args[0], str) else "probe_failed"
            if code in {"gpu_probe_ssh_key_invalid", "gpu_probe_known_hosts_invalid",
                        "gpu_probe_known_hosts_changed", "gpu_probe_known_hosts_unavailable",
                        "gpu_probe_direct_route_required", "gpu_probe_output_exceeded",
                        "gpu_probe_output_invalid"}:
                self._record_gpu_failure("gpu_probe_policy_invalid",
                                         "gpu_runtime_probe_policy_invalid")
                return "failed"
            if int(self._clock()) >= grace_deadline:
                self._record_gpu_failure("gpu_attestation_deadline",
                                         "gpu_runtime_attestation_deadline")
                return "failed"
            self.gpu_runtime_status = "waiting"
            self.gpu_runtime_error_code = "probe_unavailable"
            return "pending"
        except ExecutionError as error:
            if error.category in (FailureCategory.TRANSIENT_READ,
                                   FailureCategory.TRANSPORT_FAILURE):
                if int(self._clock()) >= grace_deadline:
                    self._record_gpu_failure("gpu_attestation_deadline",
                                             "gpu_runtime_attestation_deadline")
                    return "failed"
                self.gpu_runtime_status = "waiting"
                self.gpu_runtime_error_code = "provider_not_ready"
                return "pending"
            self._record_gpu_failure("gpu_provider_shape_invalid",
                                     "gpu_runtime_provider_shape_invalid")
            return "failed"
        except GPURuntimeEvidenceError as error:
            code = error.args[0] if error.args and isinstance(error.args[0], str) else "evidence_invalid"
            safe_codes = {
                "gpu_count_mismatch", "gpu_vram_below_profile", "cuda_driver_below_profile",
                "gpu_count_out_of_bounds", "probe_output_malformed", "probe_frame_malformed",
                "driver_report_incomplete", "gpu_row_malformed", "gpu_memory_malformed",
            }
            sanitized = code if code in safe_codes else "gpu_evidence_invalid"
            self._record_gpu_failure(sanitized, "gpu_runtime_evidence_rejected")
            return "failed"
        except Exception:
            self.gpu_runtime_status = "error"
            self.gpu_runtime_error_code = "gpu_probe_internal_error"
            raise

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
                    detail = self._read_attempt_detail(
                        candidate.pod_id, attempt["pod_name"])
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
                elif detail.status == "RUNNING" and not self.stopping:
                    outcome = self._gpu_attestation_outcome(attempt)
                    if outcome == "pending":
                        return "gpu_runtime_pending"
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
            detail = self._read_attempt_detail(
                attempt["pod_id"], attempt["pod_name"])
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
        elif detail.status == "RUNNING" and not self.stopping:
            outcome = self._gpu_attestation_outcome(attempt)
            if outcome == "pending":
                self._reset_absence()
                return "gpu_runtime_pending"
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
                   clock=time.time, monotonic=time.monotonic, sleep=time.sleep,
                   max_polls=None, boot_identity_provider=current_boot_identity):
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
    attempt_sha256 = getattr(monitor, "attempt_sha256", None)
    if attempt_sha256 is not None:
        state["attempt_sha256"] = attempt_sha256
    if getattr(monitor, "stop_reason", None) is not None:
        state["stop_reason"] = monitor.stop_reason
    if hasattr(monitor, "gpu_runtime_status"):
        state["gpu_runtime_status"] = monitor.gpu_runtime_status
        if monitor.gpu_runtime_error_code is None:
            state.pop("gpu_runtime_error_code", None)
        else:
            state["gpu_runtime_error_code"] = monitor.gpu_runtime_error_code
    boot_identity = boot_identity_provider()
    if not valid_boot_identity(boot_identity):
        raise RuntimeError("host_boot_identity_unavailable")
    now = clock()
    monotonic_now = monotonic()
    if (isinstance(now, bool) or not isinstance(now, (int, float)) or not math.isfinite(now) or
            isinstance(monotonic_now, bool) or not isinstance(monotonic_now, (int, float)) or
            not math.isfinite(monotonic_now)):
        raise ValueError("Invalid supervisor clock")

    monotonic_deadline = monotonic_now + max(0.0, deadline_epoch - now)
    last_wall_epoch = now
    if state_path.is_symlink():
        state["stopping"] = True
        state["stop_reason"] = "supervisor_state_symlink"
        monotonic_deadline = monotonic_now
    elif state_path.exists():
        try:
            previous = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            previous = None
        if isinstance(previous, dict):
            if not same_boot_identity(previous.get("boot_identity"), boot_identity):
                state["stopping"] = True
                state["stop_reason"] = "host_boot_identity_changed"
                monotonic_deadline = monotonic_now
            elif (previous.get("attempt_id") != monitor.attempt_id or
                    previous.get("project_id") != monitor.project_id or
                    previous.get("deadline_epoch") != deadline_epoch or
                    previous.get("attempt_sha256") != attempt_sha256 or
                    previous.get("policy_sha256") != policy_sha256):
                state["stopping"] = True
                state["stop_reason"] = "supervisor_state_identity_mismatch"
                monotonic_deadline = monotonic_now
            else:
                prior_deadline = previous.get("monotonic_deadline")
                prior_heartbeat = previous.get("heartbeat_epoch")
                if (isinstance(prior_deadline, bool) or
                        not isinstance(prior_deadline, (int, float)) or
                        not math.isfinite(prior_deadline) or
                        isinstance(prior_heartbeat, bool) or
                        not isinstance(prior_heartbeat, (int, float)) or
                        not math.isfinite(prior_heartbeat)):
                    state["stopping"] = True
                    state["stop_reason"] = "supervisor_state_invalid"
                    monotonic_deadline = monotonic_now
                else:
                    monotonic_deadline = float(prior_deadline)
                    last_wall_epoch = float(prior_heartbeat)
                    state["stopping"] = state["stopping"] or previous.get("stopping") is True
                    if previous.get("stop_reason") is not None:
                        state["stop_reason"] = previous["stop_reason"]
        else:
            state["stopping"] = True
            state["stop_reason"] = "supervisor_state_invalid"
            monotonic_deadline = monotonic_now
    state["boot_identity"] = boot_identity
    state["monotonic_deadline"] = monotonic_deadline

    while max_polls is None or polls < max_polls:
        now = clock()
        monotonic_now = monotonic()
        if (isinstance(now, bool) or not isinstance(now, (int, float)) or not math.isfinite(now) or
                isinstance(monotonic_now, bool) or not isinstance(monotonic_now, (int, float)) or
                not math.isfinite(monotonic_now)):
            state["stopping"] = True
            state["stop_reason"] = "supervisor_clock_invalid"
        elif now < last_wall_epoch:
            state["stopping"] = True
            state["stop_reason"] = "wall_clock_moved_backwards"
        elif now >= deadline_epoch or monotonic_now >= monotonic_deadline:
            state["stopping"] = True
            state.setdefault("stop_reason", "supervisor_deadline")
        if stop_path.exists():
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
            if hasattr(monitor, "gpu_runtime_status"):
                state["gpu_runtime_status"] = monitor.gpu_runtime_status
                if monitor.gpu_runtime_error_code is None:
                    state.pop("gpu_runtime_error_code", None)
                else:
                    state["gpu_runtime_error_code"] = monitor.gpu_runtime_error_code
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
                if not stop_path.is_symlink():
                    stop_path.touch(exist_ok=True)
            except OSError:
                pass
        polls += 1
        heartbeat = clock()
        if (isinstance(heartbeat, bool) or not isinstance(heartbeat, (int, float)) or
                not math.isfinite(heartbeat)):
            state["stopping"] = True
            state["stop_reason"] = "supervisor_clock_invalid"
            heartbeat = last_wall_epoch
        elif heartbeat < last_wall_epoch:
            state["stopping"] = True
            state["stop_reason"] = "wall_clock_moved_backwards"
            heartbeat = last_wall_epoch
        last_wall_epoch = float(heartbeat)
        state.update({"heartbeat_epoch": last_wall_epoch, "polls": polls,
                      "consecutive_errors": errors})
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
