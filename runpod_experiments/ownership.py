"""Exact-attempt ownership checks around provider operations."""
from runpod_experiments.attempts import AttemptError


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
