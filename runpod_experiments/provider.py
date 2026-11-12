"""Read-only normalized boundary for provider inventory observations."""
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Protocol

from runpod_experiments.billing import billing_query_bounds, normalize_pod_billing
from runpod_experiments.contracts import (
    BillingObservation, ContractError, FailureCategory, ProjectPolicy, QuoteObservation,
    ResourceProfile,
)
from runpod_experiments.errors import (
    ExecutionError, ProviderOperation, normalize_provider_error,
)
from runpod_experiments.runpod import ProviderError
from runpod_experiments.quotes import normalize_cpu_quote, normalize_gpu_quote
from runpod_experiments.resource_policy import resource_profile_violation


_POD_ID = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")
_STATUS = re.compile(r"[A-Za-z][A-Za-z0-9_-]{0,63}\Z")
_SSH_HOST = re.compile(r"[A-Za-z0-9.:-]{1,255}\Z")
_SSH_USERNAME = re.compile(r"[A-Za-z_][A-Za-z0-9_-]{0,63}\Z")


class ReadOnlyProvider(Protocol):
    """Minimum transport surface used by the normalized observation adapter."""

    def pods(self) -> list[dict]: ...

    def pod(self, pod_id: str) -> dict: ...

    def cpu_offer(self, cpu_id: str, vcpu_count: int) -> dict: ...

    def gpu_offer(self, gpu_id: str, gpu_count: int = 1, *,
                  min_cuda_version: str | None = None) -> dict: ...

    def pod_billing(self, pod_id: str, *, bucket_size: str = "hour",
                    start_time: str | None = None,
                    end_time: str | None = None) -> dict: ...


@dataclass(frozen=True, slots=True)
class PodObservation:
    """Safe identity/status projection; raw provider payload is not retained."""

    pod_id: str
    name: str
    status: str
    resource_violation: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.pod_id, str) or _POD_ID.fullmatch(self.pod_id) is None:
            raise ContractError("provider Pod ID is malformed")
        if (not isinstance(self.name, str) or not self.name or self.name != self.name.strip() or
                len(self.name) > 128 or not all(character.isprintable() for character in self.name)):
            raise ContractError("provider Pod name is malformed")
        if not isinstance(self.status, str) or _STATUS.fullmatch(self.status) is None:
            raise ContractError("provider Pod status is malformed")
        if (self.resource_violation is not None and
                (not isinstance(self.resource_violation, str) or
                 re.fullmatch(r"[a-z][a-z0-9_]{0,63}", self.resource_violation) is None)):
            raise ContractError("provider Pod resource violation is malformed")


@dataclass(frozen=True, slots=True)
class DirectSSHEndpoint:
    """Validated direct SSH coordinates; no raw provider record is retained."""

    host: str
    port: int
    username: str

    def __post_init__(self) -> None:
        if (not isinstance(self.host, str) or _SSH_HOST.fullmatch(self.host) is None or
                self.host.startswith("-") or type(self.port) is not int or
                not 1 <= self.port <= 65535 or not isinstance(self.username, str) or
                _SSH_USERNAME.fullmatch(self.username) is None):
            raise ContractError("direct SSH endpoint is malformed")


class ProviderAdapter:
    """Project-scoped read methods over the low-level RunPod transport.

    This adapter deliberately exposes no create/delete methods. Existing guarded
    controllers remain responsible for mutations until their migration gate.
    """

    def __init__(self, provider: ReadOnlyProvider):
        self._provider = provider

    def list_account_pods(self) -> tuple[PodObservation, ...]:
        """Return normalized account inventory, failing closed on ambiguity."""
        try:
            raw_pods = self._provider.pods()
        except ProviderError as error:
            raise normalize_provider_error(error, ProviderOperation.READ) from None
        if not isinstance(raw_pods, list):
            raise ExecutionError(FailureCategory.RESOURCE_MISMATCH, "inventory_malformed")
        observations = []
        for raw in raw_pods:
            try:
                if not isinstance(raw, dict):
                    raise ContractError("provider Pod record is not an object")
                observations.append(PodObservation(raw.get("id"), raw.get("name"), raw.get("status")))
            except ContractError:
                raise ExecutionError(FailureCategory.RESOURCE_MISMATCH, "inventory_malformed") from None
        ids = [item.pod_id for item in observations]
        names = [item.name for item in observations]
        if len(ids) != len(set(ids)) or len(names) != len(set(names)):
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "inventory_identity_ambiguous")
        return tuple(observations)

    def list_project_pods(self, policy: ProjectPolicy) -> tuple[PodObservation, ...]:
        """Filter observational account inventory by one registered namespace."""
        if not isinstance(policy, ProjectPolicy):
            raise ContractError("policy must be a ProjectPolicy")
        return tuple(item for item in self.list_account_pods()
                     if any(item.name.startswith(prefix) and len(item.name) > len(prefix)
                            for prefix in policy.pod_name_prefixes))

    def _read_attempt_detail(self, pod_id: str, pod_name: str, policy: ProjectPolicy, *,
                             resource_profile: ResourceProfile | None = None,
                             selected_region: str | None = None) -> tuple[dict, PodObservation]:
        if not isinstance(policy, ProjectPolicy):
            raise ContractError("policy must be a ProjectPolicy")
        if (not isinstance(pod_name, str) or not any(
                pod_name.startswith(prefix) and len(pod_name) > len(prefix)
                for prefix in policy.pod_name_prefixes)):
            raise ExecutionError(FailureCategory.AUTHORIZATION, "attempt_outside_project")
        if not isinstance(pod_id, str) or _POD_ID.fullmatch(pod_id) is None:
            raise ExecutionError(FailureCategory.RESOURCE_MISMATCH, "attempt_pod_id_invalid")
        try:
            raw = self._provider.pod(pod_id)
        except ProviderError as error:
            if error.status == 404:
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                     "attempt_pod_missing") from None
            raise normalize_provider_error(error, ProviderOperation.READ) from None
        try:
            if not isinstance(raw, dict):
                raise ContractError("provider Pod detail is not an object")
            if ((resource_profile is None) != (selected_region is None) or
                    (resource_profile is not None and not isinstance(resource_profile, ResourceProfile))):
                raise ContractError("resource profile and selected region must be supplied together")
            violation = (resource_profile_violation(raw, resource_profile, selected_region)
                         if resource_profile is not None else None)
            observation = PodObservation(raw.get("id"), raw.get("name"), raw.get("status"), violation)
        except ContractError:
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "attempt_pod_malformed") from None
        if (observation.pod_id != pod_id or observation.name != pod_name or
                not any(observation.name.startswith(prefix) and len(observation.name) > len(prefix)
                        for prefix in policy.pod_name_prefixes)):
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "attempt_identity_mismatch")
        return raw, observation

    def get_attempt_pod(self, pod_id: str, pod_name: str, policy: ProjectPolicy, *,
                        resource_profile: ResourceProfile | None = None,
                        selected_region: str | None = None) -> PodObservation:
        """Read one candidate and require its live ID/name/project identity to agree."""
        _, observation = self._read_attempt_detail(
            pod_id, pod_name, policy, resource_profile=resource_profile,
            selected_region=selected_region)
        return observation

    def get_attempt_direct_ssh(self, pod_id: str, pod_name: str, policy: ProjectPolicy, *,
                               resource_profile: ResourceProfile,
                               selected_region: str) -> DirectSSHEndpoint:
        """Return a direct endpoint only after exact live resource-shape checks."""
        raw, observation = self._read_attempt_detail(
            pod_id, pod_name, policy, resource_profile=resource_profile,
            selected_region=selected_region)
        if observation.status != "RUNNING":
            raise ExecutionError(FailureCategory.TRANSIENT_READ, "attempt_pod_not_running")
        if observation.resource_violation is not None:
            raise ExecutionError(FailureCategory.RESOURCE_MISMATCH,
                                 "attempt_resource_shape_unverified")
        ssh = raw.get("ssh")
        direct = ssh.get("direct") if isinstance(ssh, dict) else None
        if not isinstance(direct, dict):
            raise ExecutionError(FailureCategory.TRANSPORT_FAILURE,
                                 "direct_ssh_endpoint_unavailable")
        try:
            return DirectSSHEndpoint(direct.get("host"), direct.get("port"),
                                     direct.get("username"))
        except ContractError:
            raise ExecutionError(FailureCategory.RESOURCE_MISMATCH,
                                 "direct_ssh_endpoint_invalid") from None

    def quote_cpu(self, resource_id: str, vcpu_count: int, *, observed_epoch: int,
                  storage_hourly_usd: str | int | float = "0") -> QuoteObservation:
        try:
            offer = self._provider.cpu_offer(resource_id, vcpu_count)
        except ProviderError as error:
            raise normalize_provider_error(error, ProviderOperation.READ) from None
        return normalize_cpu_quote(
            offer, resource_id=resource_id, vcpu_count=vcpu_count,
            observed_epoch=observed_epoch, storage_hourly_usd=storage_hourly_usd)

    def quote_gpu(self, resource_id: str, gpu_count: int = 1, *, observed_epoch: int,
                  storage_hourly_usd: str | int | float = "0",
                  minimum_cuda: str | None = None) -> QuoteObservation:
        try:
            offer = (self._provider.gpu_offer(resource_id, gpu_count) if minimum_cuda is None else
                     self._provider.gpu_offer(resource_id, gpu_count,
                                              min_cuda_version=minimum_cuda))
        except ProviderError as error:
            raise normalize_provider_error(error, ProviderOperation.READ) from None
        return normalize_gpu_quote(
            offer, resource_id=resource_id, gpu_count=gpu_count,
            observed_epoch=observed_epoch, storage_hourly_usd=storage_hourly_usd,
            minimum_cuda=minimum_cuda)

    def observe_pod_billing(
        self,
        *,
        project_id: str,
        attempt_id: str,
        provider_pod_id: str,
        observed_epoch: int,
        start_time: str,
        end_time: str,
        bucket_size: str = "hour",
        supersedes_observation_ids: dict[tuple[int, int], str] | None = None,
    ) -> tuple[BillingObservation, ...]:
        """Read and normalize exact-Pod billing; never settles or releases holds."""
        billing_query_bounds(start_time, end_time)
        try:
            raw = self._provider.pod_billing(
                provider_pod_id, bucket_size=bucket_size,
                start_time=start_time, end_time=end_time)
        except ProviderError as error:
            raise normalize_provider_error(error, ProviderOperation.READ) from None
        try:
            return normalize_pod_billing(
                raw, project_id=project_id, attempt_id=attempt_id,
                provider_pod_id=provider_pod_id, observed_epoch=observed_epoch,
                bucket_size=bucket_size, start_time=start_time, end_time=end_time,
                supersedes_observation_ids=supersedes_observation_ids)
        except ContractError:
            raise ExecutionError(FailureCategory.RESOURCE_MISMATCH,
                                 "pod_billing_malformed") from None
