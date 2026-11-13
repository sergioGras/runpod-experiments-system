"""One reserved-attempt lifecycle coordinator for the store-backed engine."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import math
from pathlib import Path, PurePosixPath
import subprocess
import time
from typing import Callable

from runpod_experiments.collection_process import launch_execution_store
from runpod_experiments.contracts import (
    BillingStatus, CleanupStatus, ExecutionOutcome, ExecutionPlan,
    FailureCategory, OperationalStatus, ResourceKind, ScientificVerdict, StageEvent, StageStatus,
    TransferDirection, encode_document, sha256_bytes, strict_json_loads,
)
from runpod_experiments.errors import ExecutionError
from runpod_experiments.execution_store import ExecutionStore
from runpod_experiments.interactive_ssh import SSHInteractiveProcess
from runpod_experiments.interactive_workload import InteractiveDriver, InteractiveSession
from runpod_experiments.remote_process import start_python
from runpod_experiments.state_config import StateStoreConfig
from runpod_experiments.store_create import StoreCreateController
from runpod_experiments.store_transport import load_verified_gpu_attestation
from runpod_experiments.supervisor_process import launch_execution_store as launch_supervisor
from runpod_experiments.upload import upload_bundle


@dataclass(frozen=True, slots=True)
class AttemptWorkload:
    """Non-secret inputs for one reviewed single-Pod workload attempt."""

    source_root: Path
    key_file: Path
    known_hosts: Path
    remote_root: str
    script: str
    arguments: tuple[str, ...]
    timeout_seconds: int
    result_names: tuple[str, ...]
    artifact_directory: Path
    ssh_route: str = "direct"

    def __post_init__(self) -> None:
        for field in ("source_root", "key_file", "known_hosts", "artifact_directory"):
            value = getattr(self, field)
            if not isinstance(value, (str, Path)):
                raise ValueError(f"{field} must be a filesystem path")
            object.__setattr__(self, field, Path(value))
        if (not isinstance(self.remote_root, str) or not self.remote_root.startswith("/") or
                "\x00" in self.remote_root or "\\" in self.remote_root or
                ".." in PurePosixPath(self.remote_root).parts or
                PurePosixPath(self.remote_root).as_posix() != self.remote_root):
            raise ValueError("remote_root must be an absolute safe POSIX path")
        script = PurePosixPath(self.script) if isinstance(self.script, str) else None
        if (script is None or not self.script or "\\" in self.script or "\x00" in self.script or
                script.is_absolute() or ".." in script.parts or script.as_posix() != self.script):
            raise ValueError("script must be a normalized relative POSIX path")
        if (not isinstance(self.arguments, tuple) or
                any(not isinstance(value, str) or "\x00" in value for value in self.arguments)):
            raise ValueError("arguments must be an immutable tuple of strings")
        if type(self.timeout_seconds) is not int or self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be a positive integer")
        if (not isinstance(self.result_names, tuple) or not self.result_names or
                any(not isinstance(name, str) or not name for name in self.result_names) or
                len(set(self.result_names)) != len(self.result_names)):
            raise ValueError("result_names must be a non-empty unique tuple")
        if self.ssh_route not in ("direct", "proxy"):
            raise ValueError("ssh_route must be direct or proxy")


@dataclass(frozen=True, slots=True)
class SerialAttemptResult:
    attempt_id: str
    pod_id: str | None
    outcome: ExecutionOutcome
    stage_events: tuple[StageEvent, ...]


@dataclass(frozen=True, slots=True)
class SerialJobResult:
    """Finite job-level retries over separately reserved store attempts."""

    job_id: str
    attempt_results: tuple[SerialAttemptResult, ...]
    stop_reason: str

    def __post_init__(self) -> None:
        if not self.job_id or not self.attempt_results:
            raise ValueError("a serial job result requires a job and at least one attempt")
        if any(not isinstance(item, SerialAttemptResult) for item in self.attempt_results):
            raise TypeError("attempt_results must contain SerialAttemptResult values")
        if not isinstance(self.stop_reason, str) or not self.stop_reason:
            raise ValueError("stop_reason must be a non-empty stable code")

    @property
    def final_attempt(self) -> SerialAttemptResult:
        return self.attempt_results[-1]

    @property
    def outcome(self) -> ExecutionOutcome:
        return self.final_attempt.outcome


def _error_details(error: BaseException, stage: str) -> tuple[FailureCategory, str]:
    if isinstance(error, ExecutionError):
        return error.category, error.code
    if stage == "create":
        return FailureCategory.INTERNAL, "create_unclassified"
    if stage == "upload":
        return FailureCategory.SETUP_FAILURE, "upload_failed"
    if stage == "collector":
        return FailureCategory.TRANSPORT_FAILURE, "collector_failed"
    if stage == "workload" and isinstance(error, RuntimeError):
        return FailureCategory.WORKLOAD_FAILURE, "workload_failed"
    if stage == "supervisor":
        return FailureCategory.RECONCILIATION_REQUIRED, "supervisor_failed"
    return FailureCategory.INTERNAL, "stage_failed"


class SerialExecutionEngine:
    """Coordinate supervision, one guarded create, workload I/O and cleanup.

    The caller reserves the immutable attempt first and remains responsible for
    approval, disclosure review, host-key trust and the scientific verdict. This
    coordinator never retries a scientific attempt or treats a process exit as
    provider termination. Proxy workloads must supply a process launcher;
    interactive binary sessions are supported only on the direct SSH route.
    """

    def __init__(self, client, store: ExecutionStore, state_config: StateStoreConfig, *,
                 supervisor_launcher: Callable = launch_supervisor,
                 collector_launcher: Callable = launch_execution_store,
                 create_controller_factory: Callable = StoreCreateController,
                 uploader: Callable = upload_bundle,
                 process_launcher: Callable = start_python,
                 interactive_process_factory: Callable = SSHInteractiveProcess,
                 clock=time.time, monotonic=time.monotonic, sleep=time.sleep):
        if not isinstance(store, ExecutionStore) or not isinstance(state_config, StateStoreConfig):
            raise TypeError("store and state_config must be configured shared-library objects")
        if (store.controller_host != state_config.controller_host or
                Path(store.path) != state_config.database_path(store.project_id)):
            raise ExecutionError(FailureCategory.AUTHORIZATION, "execution_store_identity_mismatch")
        self.client, self.store, self.state_config = client, store, state_config
        self.supervisor_launcher = supervisor_launcher
        self.collector_launcher = collector_launcher
        self.create_controller_factory = create_controller_factory
        self.uploader, self.process_launcher = uploader, process_launcher
        self.interactive_process_factory = interactive_process_factory
        self.clock, self.monotonic, self.sleep = clock, monotonic, sleep

    def _validate_workload(self, plan: ExecutionPlan, attempt: dict,
                           workload: AttemptWorkload,
                           interactive_driver: InteractiveDriver | None = None) -> dict[str, str]:
        if not isinstance(plan, ExecutionPlan) or not isinstance(workload, AttemptWorkload):
            raise TypeError("plan and workload must use the shared typed contracts")
        if (interactive_driver is not None and
                not isinstance(interactive_driver, InteractiveDriver)):
            raise TypeError("interactive_driver must use the bounded shared contract")
        if interactive_driver is not None and workload.ssh_route != "direct":
            raise ExecutionError(FailureCategory.CONFIGURATION,
                                 "interactive_proxy_route_unsupported")
        self.store.verify_execution_plan(attempt["attempt_id"], plan)
        if (plan.project.project_id != self.store.project_id or
                plan.approval.controller_host != self.store.controller_host or
                plan.job_id != attempt["job_id"] or
                plan.approval.approval_id != attempt["approval_id"]):
            raise ExecutionError(FailureCategory.AUTHORIZATION, "attempt_plan_identity_mismatch")
        if workload.timeout_seconds > plan.approval.max_runtime_seconds:
            raise ExecutionError(FailureCategory.CONFIGURATION, "workload_timeout_exceeds_approval")
        if workload.timeout_seconds > self.store.attempt_runtime_seconds(attempt["attempt_id"]):
            raise ExecutionError(FailureCategory.CONFIGURATION, "workload_timeout_exceeds_attempt")
        if (workload.key_file.is_symlink() or workload.known_hosts.is_symlink() or
                not workload.key_file.is_file() or not workload.known_hosts.is_file()):
            raise ExecutionError(FailureCategory.CONFIGURATION, "ssh_identity_or_known_hosts_missing")

        upload_files = {item.path: item for item in plan.transfers.files
                        if item.direction is TransferDirection.UPLOAD}
        download_names = {item.path for item in plan.transfers.files
                          if item.direction is TransferDirection.DOWNLOAD}
        if workload.script not in upload_files or not set(workload.result_names) <= download_names:
            raise ExecutionError(FailureCategory.AUTHORIZATION, "workload_transfer_outside_plan")
        bundle = {name: item.sha256 for name, item in upload_files.items()}
        if workload.source_root.is_symlink():
            raise ExecutionError(FailureCategory.AUTHORIZATION, "upload_source_root_symlink")
        root = workload.source_root.resolve(strict=True)
        if not root.is_dir():
            raise ExecutionError(FailureCategory.SETUP_FAILURE, "upload_source_root_missing")
        total = 0
        for item in upload_files.values():
            path = root.joinpath(*PurePosixPath(item.path).parts)
            cursor = root
            for part in PurePosixPath(item.path).parts:
                cursor = cursor / part
                if cursor.is_symlink():
                    raise ExecutionError(FailureCategory.AUTHORIZATION, "upload_source_symlink")
            try:
                resolved = path.resolve(strict=True)
                if not resolved.is_relative_to(root) or not resolved.is_file():
                    raise ValueError
                data = resolved.read_bytes()
            except (OSError, ValueError):
                raise ExecutionError(FailureCategory.SETUP_FAILURE, "upload_source_missing") from None
            if (len(data) != item.size_bytes or len(data) > plan.transfers.max_file_bytes or
                    hashlib.sha256(data).hexdigest() != item.sha256):
                raise ExecutionError(FailureCategory.SETUP_FAILURE, "upload_source_changed")
            total += len(data)
            if total > plan.transfers.max_total_bytes:
                raise ExecutionError(FailureCategory.SETUP_FAILURE, "upload_bundle_too_large")
        if workload.ssh_route == "proxy" and self.process_launcher is start_python:
            raise ExecutionError(FailureCategory.CONFIGURATION,
                                 "proxy_process_adapter_required")
        return bundle

    @staticmethod
    def _read_state(path: Path) -> dict:
        try:
            if path.is_symlink() or not path.is_file() or path.stat().st_size > 65_536:
                raise ValueError
            state = strict_json_loads(path.read_bytes())
        except (OSError, ValueError):
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "supervisor_state_unavailable") from None
        if not isinstance(state, dict):
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "supervisor_state_malformed")
        return state

    def _check_supervisor(self, process, directory: Path, attempt: dict,
                          policy_sha256: str) -> None:
        if process.poll() is not None:
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "supervisor_process_exited")
        state = self._read_state(directory / "supervisor.json")
        now = self.clock()
        heartbeat = state.get("heartbeat_epoch")
        if (state.get("ready") is not True or state.get("pid") != process.pid or
                state.get("attempt_id") != attempt["attempt_id"] or
                state.get("project_id") != self.store.project_id or
                state.get("attempt_sha256") != attempt["attempt_sha256"] or
                state.get("deadline_epoch") != attempt["deadline_epoch"] or
                state.get("policy_sha256") != policy_sha256 or
                isinstance(heartbeat, bool) or not isinstance(heartbeat, (int, float)) or
                not math.isfinite(heartbeat) or isinstance(now, bool) or
                not isinstance(now, (int, float)) or not math.isfinite(now) or
                now < heartbeat or now - heartbeat > 30 or now >= attempt["deadline_epoch"] or
                state.get("stopping") or state.get("terminated")):
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "supervisor_heartbeat_stale")

    def _wait_for_gpu_attestation(self, process, directory: Path, attempt: dict,
                                  policy_sha256: str, known_hosts: Path) -> None:
        """Wait for supervisor evidence, bounded by the immutable attempt deadline."""
        end = self.monotonic() + max(0, attempt["deadline_epoch"] - self.clock())
        while self.monotonic() < end and self.clock() < attempt["deadline_epoch"]:
            self._check_supervisor(process, directory, attempt, policy_sha256)
            try:
                events = self.store.get_stage_events(attempt["attempt_id"])
            except ExecutionError:
                raise
            attestation_events = [event for event in events
                                  if event.stage == "gpu_runtime_attestation"]
            if attestation_events:
                latest = attestation_events[-1]
                if latest.status is StageStatus.FAILED:
                    raise ExecutionError(FailureCategory.RESOURCE_MISMATCH,
                                         "gpu_runtime_attestation_rejected")
                if latest.status is StageStatus.SUCCEEDED:
                    load_verified_gpu_attestation(
                        self.store, attempt["attempt_id"], known_hosts,
                        now_epoch=int(self.clock()))
                    return
            self.sleep(min(0.25, max(0, end - self.monotonic())))
        raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                             "gpu_runtime_attestation_timeout")

    def _prepare_attempt_directory(self, attempt_id: str) -> tuple[Path, Path]:
        root = self.state_config.state_root
        project = root / self.store.project_id
        attempt = project / attempt_id
        supervisor = attempt / "supervisor"
        if any(path.is_symlink() for path in (root, project, attempt, supervisor)):
            raise ExecutionError(FailureCategory.AUTHORIZATION,
                                 "attempt_state_path_symlink")
        try:
            attempt.mkdir(parents=True, exist_ok=True, mode=0o700)
            supervisor.mkdir(parents=True, exist_ok=True, mode=0o700)
            resolved_root = root.resolve(strict=True)
            resolved_supervisor = supervisor.resolve(strict=True)
            if (resolved_supervisor == resolved_root or
                    resolved_root not in resolved_supervisor.parents):
                raise ValueError
        except (OSError, ValueError):
            raise ExecutionError(FailureCategory.AUTHORIZATION,
                                 "attempt_state_path_invalid") from None
        return attempt, supervisor

    def _record(self, attempt_id: str, stage: str, status: StageStatus, *,
                error: BaseException | None = None) -> StageEvent:
        category, code = _error_details(error, stage) if error is not None else (None, None)
        return self.store.append_stage_event(
            attempt_id, stage, status, failure_category=category, error_code=code)

    def _cancel_uncreated(self, attempt_id: str) -> None:
        """Release a reservation only while the durable store proves no create claim."""
        current = self.store.get_attempt(attempt_id)
        if current["state"] == "reserved" and not current["create_claimed"]:
            self.store.cancel_uncreated(attempt_id)

    def _cleanup(self, attempt: dict, supervisor_directory: Path,
                 supervisor_process, collector_process, workload_process,
                 cleanup_timeout_seconds: int, *, gpu_ssh_key_file=None,
                 gpu_known_hosts=None) -> CleanupStatus:
        attempt_id = attempt["attempt_id"]
        if workload_process is not None and workload_process.poll() is None:
            try:
                workload_process.terminate()
                workload_process.wait(timeout=5)
            except (OSError, subprocess.SubprocessError):
                try:
                    workload_process.kill()
                    workload_process.wait(timeout=5)
                except (OSError, subprocess.SubprocessError):
                    pass
        if collector_process is not None:
            try:
                (supervisor_directory.parent / "collector" / "collection.stop").touch(exist_ok=True)
                collector_process.wait(timeout=65)
            except (OSError, subprocess.SubprocessError):
                pass

        try:
            current = self.store.get_attempt(attempt_id)
            if current["state"] == "reserved" and not current["create_claimed"]:
                self.store.cancel_uncreated(attempt_id)
            elif current["state"] == "active":
                self.store.request_stop(attempt_id)
            current = self.store.get_attempt(attempt_id)
            if current["state"] in ("terminated", "cancelled"):
                return CleanupStatus.CONFIRMED if current["state"] == "terminated" else CleanupStatus.NOT_REQUIRED
        except Exception:
            current = self.store.get_attempt(attempt_id)

        try:
            (supervisor_directory / "stop.request").touch(exist_ok=True)
        except OSError:
            return CleanupStatus.FAILED

        deadline = self.monotonic() + cleanup_timeout_seconds
        restarts = 0
        while self.monotonic() < deadline:
            current = self.store.get_attempt(attempt_id)
            if current["state"] in ("terminated", "cancelled"):
                return CleanupStatus.CONFIRMED if current["state"] == "terminated" else CleanupStatus.NOT_REQUIRED
            if supervisor_process is None or supervisor_process.poll() is not None:
                # The poll may itself have completed the final absence check and
                # persisted termination. Re-read lifecycle state before deciding
                # that an exited supervisor needs a restart.
                current = self.store.get_attempt(attempt_id)
                if current["state"] in ("terminated", "cancelled"):
                    return (CleanupStatus.CONFIRMED if current["state"] == "terminated"
                            else CleanupStatus.NOT_REQUIRED)
                if restarts >= 1:
                    return CleanupStatus.FAILED
                try:
                    if gpu_ssh_key_file is not None:
                        supervisor_process = self.supervisor_launcher(
                            self.state_config, attempt_id, self.store.project_id,
                            supervisor_directory, gpu_ssh_key_file=gpu_ssh_key_file,
                            gpu_known_hosts=gpu_known_hosts)
                    else:
                        supervisor_process = self.supervisor_launcher(
                            self.state_config, attempt_id, self.store.project_id,
                            supervisor_directory)
                    restarts += 1
                except Exception:
                    return CleanupStatus.FAILED
            try:
                (supervisor_directory / "stop.request").touch(exist_ok=True)
            except OSError:
                return CleanupStatus.FAILED
            self.sleep(min(0.25, max(0, deadline - self.monotonic())))
        return CleanupStatus.PENDING

    def run_attempt(self, plan: ExecutionPlan, attempt_id: str, workload: AttemptWorkload, *,
                    local_public_key_path: str | Path,
                    interactive_driver: InteractiveDriver | None = None,
                    cleanup_timeout_seconds: int = 180) -> SerialAttemptResult:
        """Run one new reserved attempt; do not retry or replay a started stage."""
        if type(cleanup_timeout_seconds) is not int or not 1 <= cleanup_timeout_seconds <= 3600:
            raise ValueError("cleanup_timeout_seconds is outside its supported range")
        self.store.verify_attempt_lifecycle_projection(attempt_id)
        attempt = self.store.get_attempt(attempt_id)
        if attempt["state"] != "reserved" or attempt["create_claimed"]:
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "attempt_not_new_and_reserved")
        if self.store.get_stage_events(attempt_id):
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "attempt_stage_recovery_required")
        try:
            bundle = self._validate_workload(plan, attempt, workload,
                                             interactive_driver=interactive_driver)
            attempt_directory, supervisor_directory = self._prepare_attempt_directory(attempt_id)
        except BaseException:
            try:
                self._cancel_uncreated(attempt_id)
            except Exception:
                pass
            raise
        project = self.store.project_id
        resource_profile, _ = self.store.selected_resource_for_attempt(attempt_id)
        gpu_attempt = resource_profile.kind is ResourceKind.GPU
        supervisor_process = None
        collector_process = None
        workload_process = None
        current_stage = None
        failure_stage = None
        primary_error: BaseException | None = None
        cleanup_status = CleanupStatus.NOT_REQUIRED
        try:
            controller = self.create_controller_factory(
                self.client, self.store, self.state_config,
                clock=self.clock, monotonic=self.monotonic, sleep=self.sleep)
            policy_sha256 = sha256_bytes(encode_document(
                "project_policy", self.store.project_policy_for_attempt(attempt_id)))
        except BaseException:
            try:
                self._cancel_uncreated(attempt_id)
            except Exception:
                pass
            raise

        def start(stage: str) -> None:
            nonlocal current_stage
            current_stage = stage
            self._record(attempt_id, stage, StageStatus.STARTED)

        def succeed() -> None:
            nonlocal current_stage
            if current_stage is not None:
                self._record(attempt_id, current_stage, StageStatus.SUCCEEDED)
                current_stage = None

        try:
            start("supervisor")
            if gpu_attempt:
                supervisor_process = self.supervisor_launcher(
                    self.state_config, attempt_id, project, supervisor_directory,
                    gpu_ssh_key_file=workload.key_file,
                    gpu_known_hosts=workload.known_hosts)
            else:
                supervisor_process = self.supervisor_launcher(
                    self.state_config, attempt_id, project, supervisor_directory)
            if (not callable(getattr(supervisor_process, "poll", None)) or
                    type(getattr(supervisor_process, "pid", None)) is not int or
                    supervisor_process.pid <= 0):
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                     "supervisor_process_invalid")
            succeed()

            start("create")
            controller.create(attempt_id, supervisor_process,
                              supervisor_directory, local_public_key_path)
            succeed()
            self._check_supervisor(supervisor_process, supervisor_directory,
                                   attempt, policy_sha256)

            if gpu_attempt:
                self._wait_for_gpu_attestation(
                    supervisor_process, supervisor_directory, attempt,
                    policy_sha256, workload.known_hosts)

            start("upload")
            upload_deadline = self.monotonic() + min(
                self.store.attempt_runtime_seconds(attempt_id),
                max(0, attempt["deadline_epoch"] - self.clock()))

            def check_upload_health() -> None:
                if (self.clock() >= attempt["deadline_epoch"] or
                        self.monotonic() >= upload_deadline):
                    raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                         "attempt_deadline_reached")
                self._check_supervisor(supervisor_process, supervisor_directory,
                                       attempt, policy_sha256)

            self.uploader(
                self.client, self.store, attempt_id, project,
                workload.source_root, bundle, workload.key_file,
                workload.known_hosts, workload.remote_root,
                max_file_bytes=plan.transfers.max_file_bytes,
                max_total_bytes=plan.transfers.max_total_bytes,
                ssh_route=workload.ssh_route,
                transfer_check=check_upload_health)
            check_upload_health()
            succeed()

            start("collector")
            collector_process = self.collector_launcher(
                self.state_config, attempt_id, project, workload.artifact_directory,
                list(workload.result_names), workload.key_file, workload.known_hosts,
                workload.remote_root, ssh_route=workload.ssh_route,
                max_file_bytes=plan.transfers.max_file_bytes,
                max_total_bytes=plan.transfers.max_total_bytes,
                per_file_limits={item.path: item.size_bytes
                                 for item in plan.transfers.files
                                 if item.direction is TransferDirection.DOWNLOAD and
                                 item.path in workload.result_names})
            if not callable(getattr(collector_process, "poll", None)):
                raise ExecutionError(FailureCategory.TRANSPORT_FAILURE,
                                     "collector_process_invalid")
            succeed()
            self._check_supervisor(supervisor_process, supervisor_directory,
                                   attempt, policy_sha256)

            start("workload")
            end = self.monotonic() + min(
                self.store.attempt_runtime_seconds(attempt_id),
                max(0, attempt["deadline_epoch"] - self.clock()))

            def check_workload_deadline() -> None:
                if (self.clock() >= attempt["deadline_epoch"] or
                        self.monotonic() >= end):
                    raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                         "attempt_deadline_reached")

            if interactive_driver is None:
                check_workload_deadline()
                remaining = end - self.monotonic()
                workload_timeout = min(
                    workload.timeout_seconds, max(1, math.ceil(remaining)))
                workload_process = self.process_launcher(
                    self.client, self.store, attempt_id, project, workload.key_file,
                    workload.known_hosts, workload.remote_root, workload.script,
                    list(workload.arguments), workload_timeout,
                    attempt_directory / "workload.log")
            else:
                check_workload_deadline()
                remaining = end - self.monotonic()
                interactive_timeout = min(workload.timeout_seconds,
                                           max(1, math.ceil(remaining)))
                workload_process = self.interactive_process_factory(
                    self.client, self.store, attempt_id, project, workload.key_file,
                    workload.known_hosts, workload.remote_root, workload.script,
                    list(workload.arguments), interactive_timeout,
                    attempt_directory / "workload.log")
            if not callable(getattr(workload_process, "poll", None)):
                raise ExecutionError(FailureCategory.TRANSPORT_FAILURE,
                                     "workload_process_invalid")
            if interactive_driver is not None:
                def check_interactive_health() -> None:
                    check_workload_deadline()
                    self._check_supervisor(supervisor_process, supervisor_directory,
                                           attempt, policy_sha256)
                    if collector_process.poll() is not None:
                        raise ExecutionError(FailureCategory.TRANSPORT_FAILURE,
                                             "collector_process_exited")

                session = InteractiveSession(
                    workload_process, check=check_interactive_health, deadline=end,
                    max_message_bytes=interactive_driver.max_message_bytes,
                    max_io_timeout_seconds=interactive_driver.max_io_timeout_seconds,
                    monotonic=self.monotonic, sleep=self.sleep)
                interactive_driver.run(session)
                check_interactive_health()
            exit_code = workload_process.poll()
            while exit_code is None:
                check_workload_deadline()
                self._check_supervisor(supervisor_process, supervisor_directory,
                                       attempt, policy_sha256)
                if collector_process.poll() is not None:
                    raise ExecutionError(FailureCategory.TRANSPORT_FAILURE,
                                         "collector_process_exited")
                self.sleep(min(0.25, max(0, end - self.monotonic())))
                exit_code = workload_process.poll()
            check_workload_deadline()
            self._check_supervisor(supervisor_process, supervisor_directory,
                                   attempt, policy_sha256)
            if collector_process.poll() is not None:
                raise ExecutionError(FailureCategory.TRANSPORT_FAILURE,
                                     "collector_process_exited")
            if exit_code != 0:
                raise ExecutionError(FailureCategory.WORKLOAD_FAILURE,
                                     "workload_exit_nonzero")
            succeed()
        except BaseException as error:
            primary_error = error
            failure_stage = current_stage
            if current_stage is not None and not isinstance(error, (KeyboardInterrupt, SystemExit)):
                try:
                    self._record(attempt_id, current_stage, StageStatus.FAILED, error=error)
                except Exception:
                    pass
                current_stage = None
        finally:
            if supervisor_process is not None or attempt["create_claimed"] or attempt["state"] == "reserved":
                try:
                    self._record(attempt_id, "cleanup", StageStatus.STARTED)
                except Exception:
                    pass
                try:
                    cleanup_status = self._cleanup(
                        attempt, supervisor_directory, supervisor_process,
                        collector_process, workload_process, cleanup_timeout_seconds,
                        gpu_ssh_key_file=workload.key_file if gpu_attempt else None,
                        gpu_known_hosts=workload.known_hosts if gpu_attempt else None)
                except Exception:
                    cleanup_status = CleanupStatus.FAILED
                try:
                    if cleanup_status in (CleanupStatus.CONFIRMED, CleanupStatus.NOT_REQUIRED):
                        self._record(attempt_id, "cleanup", StageStatus.SUCCEEDED)
                    else:
                        self._record(attempt_id, "cleanup", StageStatus.FAILED,
                                     error=ExecutionError(
                                         FailureCategory.RECONCILIATION_REQUIRED,
                                         "cleanup_not_confirmed"))
                except Exception:
                    pass

        if isinstance(primary_error, (KeyboardInterrupt, SystemExit)):
            raise primary_error
        final_attempt = self.store.get_attempt(attempt_id)
        if primary_error is None:
            status = OperationalStatus.SUCCEEDED
            category = None
        else:
            category, _ = _error_details(primary_error, failure_stage or "workload")
            status = (OperationalStatus.RECONCILIATION_REQUIRED
                      if category is FailureCategory.RECONCILIATION_REQUIRED
                      or cleanup_status in (CleanupStatus.PENDING, CleanupStatus.FAILED)
                      else OperationalStatus.FAILED)
        if (final_attempt["state"] == "cancelled" and final_attempt["pod_id"] is None):
            billing_status = BillingStatus.NOT_APPLICABLE
        else:
            billing_status = BillingStatus.PENDING
        billing_digest = None
        if final_attempt.get("billing_complete") and final_attempt.get(
                "billing_complete_evidence_sha256"):
            billing_status = BillingStatus.POSTED
            billing_digest = final_attempt.get("billing_complete_evidence_sha256")
        outcome = ExecutionOutcome(
            operational_status=status,
            scientific_verdict=ScientificVerdict.NOT_EVALUATED,
            cleanup_status=cleanup_status,
            billing_status=billing_status,
            failure_category=category,
            evidence_sha256=tuple(sha256_bytes(encode_document("stage_event", event))
                                  for event in self.store.get_stage_events(attempt_id)),
            billing_evidence_sha256=billing_digest)
        return SerialAttemptResult(attempt_id, final_attempt["pod_id"], outcome,
                                   self.store.get_stage_events(attempt_id))

    def run_job(self, plan: ExecutionPlan, first_attempt_id: str,
                workload: AttemptWorkload, *,
                reserve_next: Callable[[int, SerialAttemptResult],
                                       tuple[str, AttemptWorkload] | None],
                local_public_key_path: str | Path,
                interactive_driver: InteractiveDriver | None = None,
                cleanup_timeout_seconds: int = 180) -> SerialJobResult:
        """Run a finite retry policy across newly reserved, sequential attempts.

        ``reserve_next`` owns fresh quote evaluation and immutable reservation;
        it is called only after the previous retryable failure is terminal and
        cleanup is confirmed. Ambiguous create, active-resource, and scientific
        failures never trigger a retry here.
        """
        if not isinstance(plan, ExecutionPlan) or not callable(reserve_next):
            raise TypeError("run_job requires a typed plan and a reservation callback")
        self.store.verify_execution_plan(first_attempt_id, plan)
        first = self.store.get_attempt(first_attempt_id)
        if (first["state"] != "reserved" or first["create_claimed"] or
                first["attempt_number"] != 1 or first["job_id"] != plan.job_id):
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "first_job_attempt_not_new")

        started = self.monotonic()
        attempt_id, current_workload = first_attempt_id, workload
        results: list[SerialAttemptResult] = []
        stop_reason = "failure_not_retryable"
        for attempt_number in range(1, plan.retry.max_attempts + 1):
            result = self.run_attempt(
                plan, attempt_id, current_workload,
                local_public_key_path=local_public_key_path,
                interactive_driver=interactive_driver,
                cleanup_timeout_seconds=cleanup_timeout_seconds)
            results.append(result)
            if result.outcome.operational_status is OperationalStatus.SUCCEEDED:
                stop_reason = "completed"
                break
            if result.outcome.failure_category not in plan.retry.retryable_categories:
                stop_reason = "failure_not_retryable"
                break
            terminal = self.store.get_attempt(attempt_id)
            if (result.outcome.cleanup_status not in
                    (CleanupStatus.CONFIRMED, CleanupStatus.NOT_REQUIRED) or
                    terminal["state"] not in ("terminated", "cancelled")):
                stop_reason = "cleanup_not_confirmed"
                break
            if attempt_number >= plan.retry.max_attempts:
                stop_reason = "attempt_limit_reached"
                break

            elapsed = self.monotonic() - started
            exponent = attempt_number - 1
            delay_ms = (plan.retry.maximum_backoff_ms if exponent >= 64 else
                        min(plan.retry.maximum_backoff_ms,
                            plan.retry.initial_backoff_ms * (2 ** exponent)))
            remaining = plan.retry.max_elapsed_seconds - elapsed
            if remaining <= 0 or delay_ms / 1000 >= remaining:
                stop_reason = "retry_window_expired"
                break
            self.sleep(delay_ms / 1000)
            if self.monotonic() - started >= plan.retry.max_elapsed_seconds:
                stop_reason = "retry_window_expired"
                break

            reserved = reserve_next(attempt_number + 1, result)
            if reserved is None:
                stop_reason = "next_attempt_unavailable"
                break
            if (not isinstance(reserved, tuple) or len(reserved) != 2 or
                    not isinstance(reserved[0], str) or
                    not isinstance(reserved[1], AttemptWorkload)):
                raise TypeError("reserve_next must return (attempt_id, AttemptWorkload) or None")
            next_attempt_id, next_workload = reserved
            self.store.verify_execution_plan(next_attempt_id, plan)
            next_attempt = self.store.get_attempt(next_attempt_id)
            if (next_attempt["state"] != "reserved" or next_attempt["create_claimed"] or
                    next_attempt["attempt_number"] != attempt_number + 1 or
                    next_attempt["job_id"] != plan.job_id or
                    next_attempt["approval_id"] != plan.approval.approval_id):
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                     "next_job_attempt_not_new_and_sequential")
            attempt_id, current_workload = next_attempt_id, next_workload
        return SerialJobResult(plan.job_id, tuple(results), stop_reason)

    def recover_attempt(self, plan: ExecutionPlan, attempt_id: str, *,
                        cleanup_timeout_seconds: int = 180,
                        collector_timeout_seconds: int = 65) -> SerialAttemptResult:
        """Recover a controller restart without replaying any workload stage.

        An independently running supervisor must still be present for a
        provider-bound attempt. Recovery asks the detached collector to finish,
        requests stop through the store, and waits for provider-confirmed
        termination. It never creates or reruns a Pod workload.
        """
        if (type(cleanup_timeout_seconds) is not int or
                not 1 <= cleanup_timeout_seconds <= 3600 or
                type(collector_timeout_seconds) is not int or
                not 1 <= collector_timeout_seconds <= 300):
            raise ValueError("recovery timeouts are outside their supported ranges")
        self.store.verify_execution_plan(attempt_id, plan)
        attempt = self.store.get_attempt(attempt_id)
        events = list(self.store.get_stage_events(attempt_id))
        if attempt["job_id"] != plan.job_id:
            raise ExecutionError(FailureCategory.AUTHORIZATION, "attempt_plan_identity_mismatch")

        open_stages: dict[str, StageEvent] = {}
        for event in events:
            if event.status is StageStatus.STARTED:
                open_stages[event.stage] = event
            else:
                open_stages.pop(event.stage, None)
        for stage in sorted(open_stages):
            error_code = ("controller_restarted_cleanup" if stage == "cleanup" else
                          "controller_restarted_stage_not_replayed")
            self._record(
                attempt_id, stage, StageStatus.FAILED,
                error=ExecutionError(FailureCategory.RECONCILIATION_REQUIRED, error_code))

        attempt_directory, supervisor_directory = self._prepare_attempt_directory(attempt_id)
        cleanup_event = next((event for event in reversed(self.store.get_stage_events(attempt_id))
                              if event.stage == "cleanup"), None)
        cleanup_complete = cleanup_event is not None and cleanup_event.status is StageStatus.SUCCEEDED
        state = attempt["state"]
        if (state not in ("cancelled", "terminated") or not cleanup_complete):
            self._record(attempt_id, "cleanup", StageStatus.STARTED)
        if state == "reserved" and not attempt["create_claimed"]:
            self.store.cancel_uncreated(attempt_id)
            cleanup_status = CleanupStatus.NOT_REQUIRED
        elif state in ("cancelled", "terminated"):
            cleanup_status = (CleanupStatus.NOT_REQUIRED if state == "cancelled"
                              else CleanupStatus.CONFIRMED)
        elif cleanup_complete:
            # A cleanup success journal paired with a nonterminal store is
            # inconsistent. Keep the exposure held for a human reconciliation.
            cleanup_status = CleanupStatus.PENDING
        else:
            collector_directory = attempt_directory / "collector"
            if collector_directory.is_symlink():
                cleanup_status = CleanupStatus.FAILED
            else:
                collector_directory.mkdir(parents=True, exist_ok=True, mode=0o700)
                collector_log = collector_directory / "collection-process.log"
                collector_state_path = collector_directory / "collection.json"
                collector_stop = collector_directory / "collection.stop"
                if collector_stop.is_symlink() or collector_state_path.is_symlink():
                    cleanup_status = CleanupStatus.FAILED
                else:
                    try:
                        collector_stop.touch(exist_ok=True)
                    except OSError:
                        cleanup_status = CleanupStatus.FAILED
                    else:
                        collector_stopped = not collector_log.exists()
                        collector_deadline = self.monotonic() + collector_timeout_seconds
                        while not collector_stopped and self.monotonic() < collector_deadline:
                            try:
                                collector_state = strict_json_loads(
                                    collector_state_path.read_bytes())
                                collector_stopped = (
                                    isinstance(collector_state, dict) and
                                    collector_state.get("attempt_id") == attempt_id and
                                    collector_state.get("stopping") is True)
                            except (OSError, ValueError):
                                pass
                            if not collector_stopped:
                                self.sleep(min(0.25, max(0, collector_deadline - self.monotonic())))

                        current = self.store.get_attempt(attempt_id)
                        if current["state"] == "active":
                            try:
                                self.store.request_stop(attempt_id)
                            except ExecutionError:
                                current = self.store.get_attempt(attempt_id)
                        try:
                            stop_path = supervisor_directory / "stop.request"
                            if stop_path.is_symlink():
                                raise OSError
                            stop_path.touch(exist_ok=True)
                        except OSError:
                            cleanup_status = CleanupStatus.FAILED
                        else:
                            current = self.store.get_attempt(attempt_id)
                            if current["state"] in ("cancelled", "terminated"):
                                cleanup_status = (CleanupStatus.NOT_REQUIRED
                                                  if current["state"] == "cancelled"
                                                  else CleanupStatus.CONFIRMED)
                            elif current["state"] not in (
                                    "create_claimed", "reconciliation_required", "active", "stopping"):
                                cleanup_status = CleanupStatus.FAILED
                            else:
                                supervisor_state_path = supervisor_directory / "supervisor.json"
                                try:
                                    supervisor_state = self._read_state(supervisor_state_path)
                                except ExecutionError:
                                    supervisor_state = {}
                                policy_sha256 = sha256_bytes(encode_document(
                                    "project_policy", self.store.project_policy_for_attempt(attempt_id)))
                                heartbeat = supervisor_state.get("heartbeat_epoch")
                                if (supervisor_state.get("ready") is not True or
                                        type(supervisor_state.get("pid")) is not int or
                                        supervisor_state.get("pid", 0) <= 0 or
                                        supervisor_state.get("attempt_id") != attempt_id or
                                        supervisor_state.get("project_id") != self.store.project_id or
                                        supervisor_state.get("attempt_sha256") != attempt["attempt_sha256"] or
                                        supervisor_state.get("deadline_epoch") != attempt["deadline_epoch"] or
                                        supervisor_state.get("policy_sha256") != policy_sha256 or
                                        isinstance(heartbeat, bool) or
                                        not isinstance(heartbeat, (int, float)) or
                                        not math.isfinite(heartbeat) or
                                        self.clock() < heartbeat or self.clock() - heartbeat > 30):
                                    cleanup_status = CleanupStatus.PENDING
                                else:
                                    deadline = self.monotonic() + cleanup_timeout_seconds
                                    last_heartbeat = heartbeat
                                    heartbeat_progress = self.monotonic()
                                    cleanup_status = CleanupStatus.PENDING
                                    while self.monotonic() < deadline:
                                        current = self.store.get_attempt(attempt_id)
                                        if current["state"] in ("cancelled", "terminated"):
                                            cleanup_status = (CleanupStatus.NOT_REQUIRED
                                                              if current["state"] == "cancelled"
                                                              else CleanupStatus.CONFIRMED)
                                            break
                                        try:
                                            observed = self._read_state(supervisor_state_path)
                                        except ExecutionError:
                                            cleanup_status = CleanupStatus.PENDING
                                            break
                                        heartbeat = observed.get("heartbeat_epoch")
                                        if (observed.get("pid") != supervisor_state["pid"] or
                                                observed.get("attempt_id") != attempt_id or
                                                observed.get("project_id") != self.store.project_id or
                                                observed.get("attempt_sha256") != attempt["attempt_sha256"] or
                                                observed.get("deadline_epoch") != attempt["deadline_epoch"] or
                                                observed.get("policy_sha256") != policy_sha256 or
                                                isinstance(heartbeat, bool) or
                                                not isinstance(heartbeat, (int, float)) or
                                                not math.isfinite(heartbeat) or
                                                self.clock() < heartbeat or self.clock() - heartbeat > 30):
                                            cleanup_status = CleanupStatus.PENDING
                                            break
                                        if heartbeat > last_heartbeat:
                                            last_heartbeat = heartbeat
                                            heartbeat_progress = self.monotonic()
                                        elif self.monotonic() - heartbeat_progress > 30:
                                            cleanup_status = CleanupStatus.PENDING
                                            break
                                        self.sleep(min(0.25, max(0, deadline - self.monotonic())))

        latest = list(self.store.get_stage_events(attempt_id))
        cleanup_event = next((event for event in reversed(latest)
                              if event.stage == "cleanup"), None)
        if cleanup_status in (CleanupStatus.CONFIRMED, CleanupStatus.NOT_REQUIRED):
            if cleanup_event is None or cleanup_event.status is not StageStatus.SUCCEEDED:
                self._record(attempt_id, "cleanup", StageStatus.SUCCEEDED)
        else:
            self._record(
                attempt_id, "cleanup", StageStatus.FAILED,
                error=ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                     "cleanup_not_confirmed"))

        final_attempt = self.store.get_attempt(attempt_id)
        latest = list(self.store.get_stage_events(attempt_id))
        workload_events = [event for event in latest if event.stage == "workload"]
        failed = next((event for event in reversed(latest)
                       if event.status is StageStatus.FAILED and event.stage != "cleanup"), None)
        if failed is not None:
            category = failed.failure_category
            status = (OperationalStatus.RECONCILIATION_REQUIRED
                      if category is FailureCategory.RECONCILIATION_REQUIRED
                      else OperationalStatus.FAILED)
        elif (workload_events and workload_events[-1].status is StageStatus.SUCCEEDED and
              cleanup_status in (CleanupStatus.CONFIRMED, CleanupStatus.NOT_REQUIRED)):
            category, status = None, OperationalStatus.SUCCEEDED
        else:
            category, status = FailureCategory.RECONCILIATION_REQUIRED, \
                OperationalStatus.RECONCILIATION_REQUIRED
        billing_status = (BillingStatus.NOT_APPLICABLE
                          if final_attempt["state"] == "cancelled" and
                          final_attempt["pod_id"] is None else BillingStatus.PENDING)
        billing_digest = final_attempt.get("billing_complete_evidence_sha256")
        if final_attempt.get("billing_complete") and billing_digest:
            billing_status = BillingStatus.POSTED
        outcome = ExecutionOutcome(
            operational_status=status, scientific_verdict=ScientificVerdict.NOT_EVALUATED,
            cleanup_status=cleanup_status, billing_status=billing_status,
            failure_category=category,
            evidence_sha256=tuple(sha256_bytes(encode_document("stage_event", event))
                                  for event in latest),
            billing_evidence_sha256=billing_digest if billing_status is BillingStatus.POSTED else None)
        return SerialAttemptResult(attempt_id, final_attempt["pod_id"], outcome, tuple(latest))
