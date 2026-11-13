"""Resolve bounded SSH access from one hash-verified active store attempt."""
from __future__ import annotations

from dataclasses import dataclass, field

from runpod_experiments.contracts import FailureCategory
from runpod_experiments.errors import ExecutionError
from runpod_experiments.execution_store import ExecutionStore
from runpod_experiments.provider import ProviderAdapter, SSHRouteEndpoint


@dataclass(frozen=True, slots=True)
class StoreSSHContext:
    attempt_id: str
    project_id: str
    pod_id: str
    pod_name: str
    endpoint: SSHRouteEndpoint = field(repr=False)


def resolve_store_ssh_context(client, store: ExecutionStore, attempt_id: str,
                              project_id: str, *, route: str = "direct") -> StoreSSHContext:
    """Verify durable ownership and live shape before exposing SSH coordinates.

    Direct remains the default. A caller selecting ``proxy`` must use the
    PTY-safe bounded command transport rather than a raw binary SSH stream.
    """
    if not isinstance(store, ExecutionStore):
        raise TypeError("store must be an ExecutionStore")
    if project_id != store.project_id:
        raise ExecutionError(FailureCategory.AUTHORIZATION, "attempt_outside_project")
    store.verify_attempt_lifecycle_projection(attempt_id)
    attempt = store.get_attempt(attempt_id)
    if attempt["state"] != "active" or not attempt["pod_id"]:
        raise ExecutionError(FailureCategory.CONFIGURATION, "attempt_not_active")
    policy = store.project_policy_for_attempt(attempt_id)
    profile, region = store.selected_resource_for_attempt(attempt_id)
    provisioning = store.provisioning_for_attempt(attempt_id)
    endpoint = ProviderAdapter(client).get_attempt_ssh_endpoint(
        attempt["pod_id"], attempt["pod_name"], policy,
        resource_profile=profile, selected_region=region,
        expected_ports=provisioning.ports, route=route)
    return StoreSSHContext(attempt_id, project_id, attempt["pod_id"],
                           attempt["pod_name"], endpoint)
