"""Exact-attempt ownership checks around provider operations."""
from runpod_experiments.attempts import AttemptError
from runpod_experiments.contracts import FailureCategory
from runpod_experiments.errors import (
    ExecutionError, ProviderOperation, normalize_provider_error,
)
from runpod_experiments.execution_store import ExecutionStore
from runpod_experiments.provider import ProviderAdapter
from runpod_experiments.runpod import ProviderError


def delete_attempt(client, ledger, attempt_id, project_id):
    """Request deletion only after durable identity and live detail match.

    A DELETE response is not proof of termination. Caller must keep polling and
    record termination only after independent provider reconciliation.
    """
    attempt = ledger.get(attempt_id)
    if attempt["project_id"] != project_id or attempt["state"] != "active" or not attempt["pod_id"]:
        raise AttemptError("Attempt is outside the active project scope")
    pod = client.pod(attempt["pod_id"])
    if not isinstance(pod, dict) or pod.get("id") != attempt["pod_id"] or pod.get("name") != attempt["pod_name"]:
        raise AttemptError("Provider detail does not match reserved attempt")
    return client._delete_pod(attempt["pod_id"])


def request_delete_for_store_attempt(provider, store, attempt_id, project_id):
    """Request deletion for one verified, durably stopping attempt.

    The operation rechecks the event-backed lifecycle projection and exact live
    project/Pod identity before calling the low-level delete endpoint. A delete
    response never changes ledger state or proves provider-side termination.
    """
    if not isinstance(store, ExecutionStore):
        raise TypeError("store must be an ExecutionStore")
    if project_id != store.project_id:
        raise ExecutionError(FailureCategory.AUTHORIZATION, "attempt_outside_project")

    policy = store.project_policy_for_attempt(attempt_id)
    attempt = store.get_attempt(attempt_id)
    if (attempt["project_id"] != project_id or attempt["state"] != "stopping" or
            not attempt["pod_id"]):
        raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                             "attempt_not_stoppable")
    delete_pod = getattr(provider, "_delete_pod", None)
    if not callable(delete_pod):
        raise ExecutionError(FailureCategory.CONFIGURATION, "pod_delete_unavailable")

    live = ProviderAdapter(provider).get_attempt_pod(
        attempt["pod_id"], attempt["pod_name"], policy)
    if live.status == "TERMINATED":
        return "already_terminated"
    try:
        delete_pod(attempt["pod_id"])
    except ProviderError as error:
        raise normalize_provider_error(error, ProviderOperation.DELETE) from None
    return "delete_requested"
