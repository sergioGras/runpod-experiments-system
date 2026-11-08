"""Guarded create operation. Call only under the project's approved plan."""
import json
import math
import time
import uuid
from decimal import Decimal, ROUND_CEILING
from pathlib import Path

from runpod_experiments.attempts import AttemptError
from runpod_experiments.planning import runtime_from_remaining
from runpod_experiments.resource_policy import ResourcePolicy, request_hash


def new_pod_name(prefix, label):
    if not isinstance(prefix, str) or not isinstance(label, str):
        raise ValueError("Invalid Pod name")
    name = prefix + label + "-" + uuid.uuid4().hex
    if len(name) > 128:
        raise ValueError("Pod name too long")
    return name


def create_attempt(client, ledger, registry, budget, attempt_id, project_id, request,
                   supervisor, control_directory, deadline_epoch, policy_path,
                   *, quote_validator, max_instances=1, outstanding_usd="0", clock=time.time, cleanup_reserve_seconds=120):
    """Send at most one create after ownership, supervisor and budget checks.

    The caller must separately enforce experiment approval, resource pricing,
    request shape and export policy. A timeout is ambiguous: do not retry this
    attempt; the separate supervisor can reconcile its unique reserved name.
    """
    attempt = ledger.get(attempt_id)
    name = attempt["pod_name"]
    if (attempt["project_id"] != project_id or attempt["state"] != "reserved" or
            not registry.owns(project_id, name) or not isinstance(request, dict) or request.get("name") != name):
        raise AttemptError("Create request does not match registered reservation")
    if type(cleanup_reserve_seconds) is not int or cleanup_reserve_seconds <= 0:
        raise ValueError("Cleanup reserve must be positive integer seconds")
    if type(max_instances) is not int or max_instances != 1:
        raise ValueError("Concurrent instances are not yet supported by the budget guard")
    if (isinstance(deadline_epoch, bool) or not isinstance(deadline_epoch, (int, float)) or
            not math.isfinite(deadline_epoch)):
        raise ValueError("Invalid deadline")
    now = clock()
    policy = ResourcePolicy.read(policy_path)
    if request_hash(request) != policy.request_sha256:
        raise AttemptError("Create request differs from the supervised policy")
    if not callable(quote_validator):
        raise ValueError("A current provider quote validator is required")
    quote_validator(client, request, policy)
    envelope = runtime_from_remaining(budget.remaining(outstanding_usd), policy.max_hourly_usd, cleanup_seconds=cleanup_reserve_seconds)
    if not now < deadline_epoch <= now + envelope["work_seconds"]:
        raise AttemptError("Deadline exceeds remaining estimated budget")
    # The quote request above can block while the independent supervisor keeps
    # writing heartbeats. Read the clock again so a newer heartbeat is not
    # rejected as if it came from the future relative to this older timestamp.
    supervisor_now = clock()
    state = json.loads((Path(control_directory) / "supervisor.json").read_text(encoding="utf-8"))
    heartbeat = state.get("heartbeat_epoch")
    failures = []
    if supervisor.poll() is not None: failures.append("process_exited")
    if state.get("pid") != supervisor.pid: failures.append("pid_mismatch")
    if state.get("ready") is not True: failures.append("not_ready")
    if state.get("stopping"): failures.append("stopping")
    if state.get("terminated"): failures.append("terminated")
    if state.get("attempt_id") != attempt_id: failures.append("attempt_mismatch")
    if state.get("project_id") != project_id: failures.append("project_mismatch")
    if state.get("deadline_epoch") != deadline_epoch: failures.append("deadline_mismatch")
    if state.get("policy_sha256") != policy.fingerprint: failures.append("policy_mismatch")
    if (isinstance(heartbeat, bool) or not isinstance(heartbeat, (int, float)) or
            not math.isfinite(heartbeat)):
        failures.append("invalid_heartbeat")
        heartbeat_age = None
    else:
        heartbeat_age = supervisor_now - heartbeat
        if heartbeat_age < 0: failures.append("heartbeat_from_future")
        if heartbeat_age > 30: failures.append("heartbeat_stale")
    if failures:
        raise AttemptError("Supervisor readiness check failed: " + ",".join(failures))
    inventory = client.pods()
    active = [p for p in inventory if registry.owns(project_id, p.get("name")) and p.get("status") != "TERMINATED"]
    if len(active) >= max_instances:
        raise AttemptError("Project instance limit reached")
    exposure = ((Decimal(str(deadline_epoch - now)) + Decimal(cleanup_reserve_seconds)) * Decimal(str(policy.max_hourly_usd)) / Decimal(3600)).quantize(Decimal("0.000001"), rounding=ROUND_CEILING)
    budget.reserve_exposure(attempt_id, exposure, outstanding_usd)
    try:
        ledger.claim_reconcilable_create(attempt_id, inventory, max_instances=max_instances)
    except BaseException:
        # No provider create has been sent at this point.
        budget.release_uncreated(attempt_id)
        raise
    response = client._create_pod(request)
    if not isinstance(response, dict) or response.get("name") != name:
        raise AttemptError("Create response is ambiguous; reconcile without retry")
    ledger.bind(attempt_id, response.get("id"))
    return response
