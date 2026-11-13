"""Resolve bounded SSH access from one hash-verified active store attempt."""
from __future__ import annotations

from dataclasses import dataclass, field
import os
from pathlib import Path
import time
import uuid

from runpod_experiments.contracts import (
    FailureCategory, StageStatus, encode_document, sha256_bytes,
)
from runpod_experiments.errors import ExecutionError
from runpod_experiments.execution_store import ExecutionStore
from runpod_experiments.gpu_runtime import (
    GPUAttestation, decode_gpu_attestation, encode_gpu_attestation,
    gpu_profile_sha256, validate_gpu_attestation,
)
from runpod_experiments.provider import ProviderAdapter, SSHRouteEndpoint


@dataclass(frozen=True, slots=True)
class StoreSSHContext:
    attempt_id: str
    project_id: str
    pod_id: str
    pod_name: str
    endpoint: SSHRouteEndpoint = field(repr=False)


def gpu_attestation_path(store: ExecutionStore, attempt_id: str) -> Path:
    """Resolve the single supervisor-owned evidence path for one attempt."""
    if not isinstance(store, ExecutionStore):
        raise TypeError("store must be an ExecutionStore")
    attempt = store.get_attempt(attempt_id)
    raw_database_path = Path(store.path)
    if (raw_database_path.is_symlink() or raw_database_path.parent.is_symlink() or
            raw_database_path.parent.parent.is_symlink()):
        raise ExecutionError(FailureCategory.AUTHORIZATION,
                             "gpu_attestation_store_path_invalid")
    database_path = raw_database_path.resolve(strict=True)
    project_directory = database_path.parent
    if (project_directory.name != attempt["project_id"] or
            project_directory.is_symlink()):
        raise ExecutionError(FailureCategory.AUTHORIZATION,
                             "gpu_attestation_store_path_invalid")
    state_root = project_directory.parent
    attempt_directory = project_directory / attempt_id
    supervisor_directory = attempt_directory / "supervisor"
    path = supervisor_directory / "gpu-runtime-attestation.json"
    for selected in (Path(store.path), project_directory, state_root, attempt_directory,
                     supervisor_directory, path):
        if selected.is_symlink():
            raise ExecutionError(FailureCategory.AUTHORIZATION,
                                 "gpu_attestation_path_symlink")
    try:
        resolved_root = state_root.resolve(strict=True)
        resolved_project = project_directory.resolve(strict=True)
        resolved_parent = supervisor_directory.resolve(strict=True)
        if (resolved_project.parent != resolved_root or
                resolved_project.name != attempt["project_id"] or
                resolved_parent == resolved_project or
                resolved_project not in resolved_parent.parents):
            raise ValueError
    except (OSError, ValueError):
        raise ExecutionError(FailureCategory.AUTHORIZATION,
                             "gpu_attestation_path_invalid") from None
    return path


def _attestation_expected(store: ExecutionStore, attempt_id: str,
                           host_key_policy_sha256: str) -> tuple[dict[str, str], object]:
    attempt = store.get_attempt(attempt_id)
    profile, _region = store.selected_resource_for_attempt(attempt_id)
    expected = {
        "project_id": attempt["project_id"], "job_id": attempt["job_id"],
        "attempt_id": attempt_id, "attempt_sha256": attempt["attempt_sha256"],
        "plan_sha256": store.plan_sha256_for_attempt(attempt_id),
        "pod_id": attempt["pod_id"], "pod_name": attempt["pod_name"],
        "host_key_policy_sha256": host_key_policy_sha256,
    }
    if not attempt["pod_id"]:
        raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                             "gpu_attestation_attempt_unbound")
    return expected, profile


def load_verified_gpu_attestation(store: ExecutionStore, attempt_id: str,
                                  known_hosts: str | Path, *,
                                  now_epoch: int | None = None) -> GPUAttestation:
    """Validate sidecar bytes, store event hash, approved profile and host-key policy."""
    if not isinstance(store, ExecutionStore):
        raise TypeError("store must be an ExecutionStore")
    attempt = store.get_attempt(attempt_id)
    if attempt["state"] != "active" or not attempt["pod_id"]:
        raise ExecutionError(FailureCategory.CONFIGURATION, "attempt_not_active")
    path = gpu_attestation_path(store, attempt_id)
    try:
        policy_path = Path(known_hosts)
        if policy_path.is_symlink() or not policy_path.is_file() or policy_path.stat().st_size > 1_048_576:
            raise ValueError
        policy_bytes = policy_path.read_bytes()
        if not policy_bytes or path.is_symlink() or not path.is_file() or path.stat().st_size > 65_536:
            raise ValueError
        raw = path.read_bytes()
    except (OSError, ValueError, TypeError):
        raise ExecutionError(FailureCategory.RESOURCE_MISMATCH,
                             "gpu_attestation_missing_or_invalid") from None
    digest = sha256_bytes(raw)
    try:
        if not store.verify_event_chain():
            raise ValueError
        events = store.get_stage_events(attempt_id)
        if not store.verify_event_chain():
            raise ValueError
    except Exception:
        raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                             "gpu_attestation_event_chain_invalid") from None
    stage_events = [event for event in events if event.stage == "gpu_runtime_attestation"]
    if (not stage_events or stage_events[-1].status is not StageStatus.SUCCEEDED or
            stage_events[-1].evidence_sha256 != (digest,)):
        raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                             "gpu_attestation_not_hash_bound")
    try:
        attestation = decode_gpu_attestation(raw)
        host_key_digest = sha256_bytes(policy_bytes)
        expected, profile = _attestation_expected(store, attempt_id, host_key_digest)
        validate_gpu_attestation(
            attestation, profile=profile, expected=expected,
            now_epoch=int(time.time()) if now_epoch is None else now_epoch)
    except (ValueError, TypeError, KeyError):
        raise ExecutionError(FailureCategory.RESOURCE_MISMATCH,
                             "gpu_attestation_scope_invalid") from None
    return attestation


def write_gpu_attestation(store: ExecutionStore, attempt_id: str,
                          directory: str | Path,
                          attestation: GPUAttestation, *,
                          now_epoch: int) -> str:
    """Atomically persist supervisor evidence and bind its digest in the event chain."""
    path = gpu_attestation_path(store, attempt_id)
    if Path(directory).resolve(strict=True) != path.parent.resolve(strict=True):
        raise ExecutionError(FailureCategory.AUTHORIZATION,
                             "gpu_attestation_directory_mismatch")
    attempt = store.get_attempt(attempt_id)
    if attempt["state"] != "active" or not attempt["pod_id"]:
        raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                             "gpu_attestation_attempt_not_active")
    expected, profile = _attestation_expected(
        store, attempt_id, attestation.host_key_policy_sha256)
    try:
        validate_gpu_attestation(attestation, profile=profile,
                                 expected=expected, now_epoch=now_epoch)
        raw = encode_gpu_attestation(attestation)
    except (ValueError, TypeError):
        raise ExecutionError(FailureCategory.RESOURCE_MISMATCH,
                             "gpu_attestation_scope_invalid") from None
    if path.is_symlink():
        raise ExecutionError(FailureCategory.AUTHORIZATION,
                             "gpu_attestation_path_symlink")
    temporary = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    try:
        with temporary.open("xb") as stream:
            if os.name != "nt":
                os.chmod(temporary, 0o600)
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except OSError:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                             "gpu_attestation_write_failed") from None
    digest = sha256_bytes(raw)
    store.append_stage_event(
        attempt_id, "gpu_runtime_attestation", StageStatus.SUCCEEDED,
        evidence_sha256=(digest,))
    return digest


def resolve_store_ssh_context(client, store: ExecutionStore, attempt_id: str,
                              project_id: str, *, route: str = "direct",
                              known_hosts: str | Path | None = None) -> StoreSSHContext:
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
    adapter = ProviderAdapter(client)
    if profile.kind.value == "gpu":
        if known_hosts is None:
            raise ExecutionError(FailureCategory.RESOURCE_MISMATCH,
                                 "gpu_runtime_evidence_required")
        attestation = load_verified_gpu_attestation(
            store, attempt_id, known_hosts)
        endpoint = adapter._get_attempt_ssh_endpoint_after_gpu_attestation(
            attempt["pod_id"], attempt["pod_name"], policy,
            resource_profile=profile, selected_region=region,
            expected_ports=provisioning.ports,
            gpu_observation=attestation.observation, route=route)
    else:
        endpoint = adapter.get_attempt_ssh_endpoint(
            attempt["pod_id"], attempt["pod_name"], policy,
            resource_profile=profile, selected_region=region,
            expected_ports=provisioning.ports, route=route)
    return StoreSSHContext(attempt_id, project_id, attempt["pod_id"],
                           attempt["pod_name"], endpoint)
