"""One-shot store-backed Pod creation with durable, verified preflight guards."""
from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
import re
import time

from runpod_experiments.contracts import (
    FailureCategory, ResourceKind, encode_document, sha256_bytes,
    strict_json_loads,
)
from runpod_experiments.errors import ExecutionError, ProviderOperation, normalize_provider_error
from runpod_experiments.execution_store import ExecutionStore
from runpod_experiments.provider import ProviderAdapter
from runpod_experiments.runpod import ProviderError
from runpod_experiments.resource_policy import request_hash
from runpod_experiments.ssh import ssh_public_key_fingerprint
from runpod_experiments.state_config import StateStoreConfig


_POD_ID = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")


@dataclass(frozen=True, slots=True)
class CreatedPod:
    attempt_id: str
    pod_id: str
    pod_name: str
    request_sha256: str
    quote_sha256: str
    ssh_key_fingerprint: str


class StoreCreateController:
    """Run a bounded preflight and send at most one create for a reserved attempt.

    The separate store-backed supervisor must already be running. Provider
    errors after the durable claim remain ambiguous and are never retried here.
    """

    def __init__(
        self,
        client,
        store: ExecutionStore,
        state_config: StateStoreConfig,
        *,
        clock=time.time,
        monotonic=time.monotonic,
        sleep=time.sleep,
        quote_max_age_seconds: int = 60,
        supervisor_heartbeat_max_age_seconds: int = 30,
        supervisor_readiness_timeout_seconds: int = 30,
    ):
        if not isinstance(store, ExecutionStore) or not isinstance(state_config, StateStoreConfig):
            raise TypeError("store and state_config must be configured shared-library objects")
        if (store.project_id == "" or store.controller_host != state_config.controller_host or
                Path(store.path) != state_config.database_path(store.project_id)):
            raise ExecutionError(FailureCategory.AUTHORIZATION, "execution_store_identity_mismatch")
        for value, name, maximum in (
                (quote_max_age_seconds, "quote_max_age_seconds", 3600),
                (supervisor_heartbeat_max_age_seconds, "supervisor_heartbeat_max_age_seconds", 300),
                (supervisor_readiness_timeout_seconds, "supervisor_readiness_timeout_seconds", 300)):
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError(f"{name} is outside its supported range")
        self.client = client
        self.store = store
        self.state_config = state_config
        self.provider = ProviderAdapter(client)
        self._clock = clock
        self._monotonic = monotonic
        self._sleep = sleep
        self.quote_max_age_seconds = quote_max_age_seconds
        self.supervisor_heartbeat_max_age_seconds = supervisor_heartbeat_max_age_seconds
        self.supervisor_readiness_timeout_seconds = supervisor_readiness_timeout_seconds

    def _read_supervisor(self, directory: Path, process, attempt: dict, policy_sha256: str) -> dict | None:
        if process.poll() is not None:
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED, "supervisor_process_exited")
        if type(getattr(process, "pid", None)) is not int or process.pid <= 0:
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED, "supervisor_pid_invalid")
        root = self.state_config.state_root / self.store.project_id
        try:
            candidate = directory.expanduser()
            if not candidate.is_absolute() or root.is_symlink():
                raise ValueError
            relative = candidate.relative_to(root)
            cursor = root
            for part in relative.parts:
                cursor = cursor / part
                if cursor.is_symlink():
                    raise ValueError
            resolved = candidate.resolve(strict=False)
        except (OSError, ValueError):
            raise ExecutionError(FailureCategory.AUTHORIZATION, "supervisor_directory_invalid") from None
        if resolved == root or root not in resolved.parents:
            raise ExecutionError(FailureCategory.AUTHORIZATION, "supervisor_directory_outside_state")
        cursor = resolved
        while cursor != root:
            if cursor.is_symlink():
                raise ExecutionError(FailureCategory.AUTHORIZATION, "supervisor_directory_symlink")
            cursor = cursor.parent
        path = resolved / "supervisor.json"
        if path.is_symlink():
            raise ExecutionError(FailureCategory.AUTHORIZATION, "supervisor_state_symlink")
        if not path.exists():
            return None
        try:
            if not path.is_file() or path.stat().st_size > 65_536:
                raise ValueError
            state = strict_json_loads(path.read_bytes())
        except (OSError, ValueError, TypeError):
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "supervisor_state_malformed") from None
        if not isinstance(state, dict):
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "supervisor_state_malformed")
        if (state.get("pid") != process.pid or state.get("attempt_id") != attempt["attempt_id"] or
                state.get("project_id") != self.store.project_id or
                state.get("deadline_epoch") != attempt["deadline_epoch"] or
                state.get("attempt_sha256") != attempt["attempt_sha256"] or
                state.get("policy_sha256") != policy_sha256):
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "supervisor_identity_mismatch")
        if state.get("stopping") is True or state.get("terminated") is True:
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "supervisor_stopping")
        heartbeat = state.get("heartbeat_epoch")
        now = self._clock()
        if (isinstance(heartbeat, bool) or not isinstance(heartbeat, (int, float)) or
                not math.isfinite(heartbeat) or isinstance(now, bool) or
                not isinstance(now, (int, float)) or not math.isfinite(now) or
                now >= attempt["deadline_epoch"] or now < heartbeat or
                now - heartbeat > self.supervisor_heartbeat_max_age_seconds):
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "supervisor_heartbeat_stale")
        return state

    def _wait_for_supervisor(self, directory: Path, process, attempt: dict,
                             policy_sha256: str) -> None:
        until = self._monotonic() + self.supervisor_readiness_timeout_seconds
        while True:
            state = self._read_supervisor(directory, process, attempt, policy_sha256)
            if state is not None and state.get("ready") is True:
                return
            remaining = until - self._monotonic()
            if remaining <= 0:
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                     "supervisor_readiness_timeout")
            self._sleep(min(0.25, remaining))

    def _local_ssh_key_fingerprint(self, public_key_path: str | Path) -> str:
        path = Path(public_key_path)
        try:
            if path.is_symlink() or not path.is_file() or path.stat().st_size > 16_384:
                raise ValueError
            lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
            if len(lines) != 1:
                raise ValueError
            return ssh_public_key_fingerprint(lines[0])
        except (OSError, UnicodeError, ValueError):
            raise ExecutionError(FailureCategory.AUTHORIZATION,
                                 "local_ssh_public_key_invalid") from None

    def _current_quote(self, profile, selected_quote):
        storage_rate = selected_quote.storage_hourly_micro_usd
        storage_usd = f"{storage_rate // 1_000_000}.{storage_rate % 1_000_000:06d}"
        observed_epoch = int(self._clock())
        if profile.kind is ResourceKind.CPU:
            quote = self.provider.quote_cpu(
                profile.resource_id, profile.cpu_count, observed_epoch=observed_epoch,
                storage_hourly_usd=storage_usd)
        else:
            quote = self.provider.quote_gpu(
                profile.resource_id, profile.gpu_count, observed_epoch=observed_epoch,
                storage_hourly_usd=storage_usd, minimum_cuda=profile.minimum_cuda)
        now = self._clock()
        if (isinstance(now, bool) or not isinstance(now, (int, float)) or not math.isfinite(now) or
                quote.observed_epoch > now or now - quote.observed_epoch > self.quote_max_age_seconds):
            raise ExecutionError(FailureCategory.RESOURCE_MISMATCH, "preflight_quote_stale")
        units = profile.cpu_count if profile.kind is ResourceKind.CPU else profile.gpu_count
        required_memory = profile.memory_gb if profile.kind is ResourceKind.CPU else profile.gpu_memory_gb
        if (quote.kind is not profile.kind or quote.resource_id != profile.resource_id or
                quote.requested_units != units or not quote.catalog_available or
                quote.total_hourly_micro_usd != profile.hourly_micro_usd or
                quote.memory_gb < required_memory or quote.minimum_cuda != profile.minimum_cuda):
            raise ExecutionError(FailureCategory.RESOURCE_MISMATCH, "preflight_quote_mismatch")
        return quote

    def _cancel_unclaimed_attempt(self, attempt_id: str) -> None:
        try:
            attempt = self.store.get_attempt(attempt_id)
            if attempt["state"] == "reserved" and not attempt["create_claimed"]:
                self.store.cancel_uncreated(attempt_id)
        except Exception:
            return

    def _require_reconciliation(self, attempt_id: str, reason: str) -> None:
        try:
            self.store.require_reconciliation(attempt_id, reason)
        except Exception:
            return

    def create(self, attempt_id: str, supervisor_process, supervisor_directory: str | Path,
               local_public_key_path: str | Path) -> CreatedPod:
        """Create exactly one Pod after current, persisted preflight evidence."""
        create_claimed = False
        try:
            attempt = self.store.get_attempt(attempt_id)
            if attempt["state"] != "reserved" or attempt["create_claimed"]:
                raise ExecutionError(FailureCategory.AMBIGUOUS_OPERATION, "create_claim_unavailable")
            self.store.verify_attempt_lifecycle_projection(attempt_id)
            policy = self.store.project_policy_for_attempt(attempt_id)
            profile, region = self.store.selected_resource_for_attempt(attempt_id)
            quote_before = self.store.selected_quote_for_attempt(attempt_id)
            request = self.store.create_request_for_attempt(attempt_id)
            if (not any(attempt["pod_name"].startswith(prefix) and
                        len(attempt["pod_name"]) > len(prefix)
                        for prefix in policy.pod_name_prefixes) or
                    request.get("name") != attempt["pod_name"] or
                    self.store.project_id != policy.project_id):
                raise ExecutionError(FailureCategory.AUTHORIZATION, "attempt_outside_project")
            policy_sha256 = self.store.project_policy_sha256_for_attempt(attempt_id)
            directory = Path(supervisor_directory)
            self._wait_for_supervisor(directory, supervisor_process, attempt, policy_sha256)

            observations = self.provider.list_account_pods()
            project_pods = [item for item in observations if any(
                item.name.startswith(prefix) and len(item.name) > len(prefix)
                for prefix in policy.pod_name_prefixes)]
            if any(item.name == attempt["pod_name"] for item in observations):
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED, "pod_name_already_exists")
            if any(item.status != "TERMINATED" for item in project_pods):
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                     "project_resource_already_present")

            local_fingerprint = self._local_ssh_key_fingerprint(local_public_key_path)
            try:
                registered_fingerprints = self.client.ssh_key_fingerprints()
            except ProviderError as error:
                raise normalize_provider_error(error, ProviderOperation.READ) from None
            if (not isinstance(registered_fingerprints, (set, frozenset, tuple)) or
                    local_fingerprint not in registered_fingerprints):
                raise ExecutionError(FailureCategory.AUTHORIZATION,
                                     "ssh_public_key_not_registered")

            quote = self._current_quote(profile, quote_before)
            if region not in quote.region_ids:
                raise ExecutionError(FailureCategory.RESOURCE_MISMATCH,
                                     "selected_region_not_available")
            self._read_supervisor(directory, supervisor_process, attempt, policy_sha256)
            self.store.record_create_preflight_checked(
                attempt_id, quote, local_fingerprint, attempt["attempt_sha256"])
            self.store.claim_create(
                attempt_id, max_preflight_age_seconds=self.quote_max_age_seconds)
            create_claimed = True
            # Recheck after the irreversible local claim. If the supervisor
            # stopped while the claim was being committed, do not send create;
            # leave the exposure held for explicit reconciliation.
            self._read_supervisor(directory, supervisor_process, attempt, policy_sha256)
        except BaseException as error:
            if create_claimed:
                self._require_reconciliation(attempt_id, "create_not_sent_after_claim")
            else:
                self._cancel_unclaimed_attempt(attempt_id)
            if isinstance(error, ExecutionError):
                raise
            if isinstance(error, (KeyboardInterrupt, SystemExit)):
                raise
            raise ExecutionError(FailureCategory.INTERNAL, "create_preflight_failed") from None

        try:
            response = self.client._create_pod(request)
        except BaseException as error:
            self._require_reconciliation(attempt_id, "create_response_ambiguous")
            if isinstance(error, (KeyboardInterrupt, SystemExit)):
                raise
            raise ExecutionError(FailureCategory.AMBIGUOUS_OPERATION,
                                 "create_response_ambiguous") from None
        if (not isinstance(response, dict) or response.get("name") != attempt["pod_name"] or
                not isinstance(response.get("id"), str) or _POD_ID.fullmatch(response["id"]) is None):
            self._require_reconciliation(attempt_id, "create_response_invalid")
            raise ExecutionError(FailureCategory.AMBIGUOUS_OPERATION, "create_response_invalid")
        try:
            self.store.bind_pod(attempt_id, response["id"], attempt["pod_name"])
        except BaseException as error:
            self._require_reconciliation(attempt_id, "create_binding_failed")
            if isinstance(error, (KeyboardInterrupt, SystemExit)):
                raise
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "create_binding_failed") from None
        return CreatedPod(
            attempt_id, response["id"], attempt["pod_name"],
            request_hash(request),
            sha256_bytes(encode_document("quote_observation", quote)), local_fingerprint)
