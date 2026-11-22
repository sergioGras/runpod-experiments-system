"""Bounded independent execution of child jobs under one admission group."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from runpod_experiments.contracts import (
    AdmissionGroupScope, ContractError, ExecutionPlan, FailureCategory,
)
from runpod_experiments.errors import ExecutionError
from runpod_experiments.execution_store import ExecutionStore
from runpod_experiments.interactive_workload import InteractiveDriver
from runpod_experiments.serial_engine import (
    AttemptWorkload, SerialExecutionEngine, SerialJobResult,
    SerialAttemptResult,
)


@dataclass(frozen=True, slots=True)
class ParallelJob:
    """One child approval's first reserved attempt and sequential retry policy."""

    plan: ExecutionPlan
    first_attempt_id: str
    workload: AttemptWorkload
    local_public_key_path: str | Path
    reserve_next: Callable[[int, SerialAttemptResult],
                           tuple[str, AttemptWorkload] | None] | None = None
    interactive_driver: InteractiveDriver | None = None
    cleanup_timeout_seconds: int = 180

    def __post_init__(self) -> None:
        if not isinstance(self.plan, ExecutionPlan):
            raise TypeError("plan must be an ExecutionPlan")
        if not isinstance(self.first_attempt_id, str) or not self.first_attempt_id:
            raise ValueError("first_attempt_id must be a non-empty string")
        if not isinstance(self.workload, AttemptWorkload):
            raise TypeError("workload must be an AttemptWorkload")
        if not isinstance(self.local_public_key_path, (str, Path)):
            raise TypeError("local_public_key_path must be a path")
        if self.reserve_next is not None and not callable(self.reserve_next):
            raise TypeError("reserve_next must be callable or None")
        if (self.interactive_driver is not None and
                not isinstance(self.interactive_driver, InteractiveDriver)):
            raise TypeError("interactive_driver must use the shared bounded contract")
        if (type(self.cleanup_timeout_seconds) is not int or
                not 1 <= self.cleanup_timeout_seconds <= 3600):
            raise ValueError("cleanup_timeout_seconds is outside its supported range")


@dataclass(frozen=True, slots=True)
class ParallelJobOutcome:
    job_id: str
    approval_id: str
    result: SerialJobResult | None
    failure_category: FailureCategory | None = None
    error_code: str | None = None

    def __post_init__(self) -> None:
        if not self.job_id or not self.approval_id:
            raise ValueError("parallel outcome identities must be non-empty")
        if self.result is not None:
            if (not isinstance(self.result, SerialJobResult) or
                    self.failure_category is not None or self.error_code is not None):
                raise ValueError("successful dispatch outcome cannot include an error")
        elif (not isinstance(self.failure_category, FailureCategory) or
              not isinstance(self.error_code, str) or not self.error_code):
            raise ValueError("failed dispatch outcome requires sanitized error details")


@dataclass(frozen=True, slots=True)
class ParallelExecutionResult:
    group_id: str
    outcomes: tuple[ParallelJobOutcome, ...]

    def __post_init__(self) -> None:
        if not self.group_id or not self.outcomes:
            raise ValueError("parallel result requires a group and at least one child")
        if any(not isinstance(item, ParallelJobOutcome) for item in self.outcomes):
            raise TypeError("outcomes must contain ParallelJobOutcome values")
        if len({item.approval_id for item in self.outcomes}) != len(self.outcomes):
            raise ValueError("parallel result child approvals must be unique")


class ParallelExecutionEngine:
    """Run admitted child jobs concurrently while isolating failures.

    Each child goes through the serial engine, which creates and supervises its
    own resource. The store remains the authoritative parent/child slot and
    budget gate; this runner does not cancel a sibling when one child raises.
    """

    def __init__(self, engine: SerialExecutionEngine, store: ExecutionStore):
        if (not isinstance(engine, SerialExecutionEngine) or
                not isinstance(store, ExecutionStore) or engine.store is not store):
            raise TypeError("parallel execution requires one serial engine and its exact store")
        self.engine, self.store = engine, store

    def _run_child(self, group: AdmissionGroupScope, job: ParallelJob) -> ParallelJobOutcome:
        try:
            reserve_next = job.reserve_next
            if reserve_next is None:
                reserve_next = lambda _number, _previous: None
            else:
                callback = reserve_next

                def reserve_next(number, previous):
                    value = callback(number, previous)
                    if value is not None:
                        if not isinstance(value, tuple) or len(value) != 2:
                            raise TypeError("reserve_next must return (attempt_id, workload) or None")
                        attempt_id = value[0]
                        self.store.verify_admission_group_binding(
                            group, job.plan.approval.approval_id)
                        record = self.store.get_attempt(attempt_id)
                        if record["approval_id"] != job.plan.approval.approval_id:
                            raise ExecutionError(FailureCategory.AUTHORIZATION,
                                                 "retry_child_approval_changed")
                    return value

            result = self.engine.run_job(
                job.plan, job.first_attempt_id, job.workload,
                reserve_next=reserve_next,
                local_public_key_path=job.local_public_key_path,
                interactive_driver=job.interactive_driver,
                cleanup_timeout_seconds=job.cleanup_timeout_seconds)
            if not isinstance(result, SerialJobResult) or result.job_id != job.plan.job_id:
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                     "parallel_child_result_identity_mismatch")
            for attempt_result in result.attempt_results:
                record = self.store.get_attempt(attempt_result.attempt_id)
                if (record["job_id"] != job.plan.job_id or
                        record["approval_id"] != job.plan.approval.approval_id):
                    raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                         "parallel_child_attempt_identity_mismatch")
                self.store.verify_admission_group_binding(
                    group, job.plan.approval.approval_id)
            return ParallelJobOutcome(job.plan.job_id, job.plan.approval.approval_id,
                                      result)
        except ExecutionError as error:
            return ParallelJobOutcome(
                job.plan.job_id, job.plan.approval.approval_id, None,
                error.category, error.code)
        except Exception:
            return ParallelJobOutcome(
                job.plan.job_id, job.plan.approval.approval_id, None,
                FailureCategory.INTERNAL, "parallel_child_unhandled")

    def run_group(self, group: AdmissionGroupScope,
                  jobs: tuple[ParallelJob, ...]) -> ParallelExecutionResult:
        """Run one bounded wave of already reserved child jobs concurrently."""
        if not isinstance(group, AdmissionGroupScope):
            raise ContractError("group must be an AdmissionGroupScope")
        if not isinstance(jobs, tuple) or not jobs or any(
                not isinstance(item, ParallelJob) for item in jobs):
            raise TypeError("jobs must be a non-empty tuple of ParallelJob values")
        if len(jobs) > group.max_active_resources:
            raise ExecutionError(FailureCategory.CONFIGURATION,
                                 "parallel_wave_exceeds_group_slots")
        approval_ids = [item.plan.approval.approval_id for item in jobs]
        if len(approval_ids) != len(set(approval_ids)):
            raise ExecutionError(FailureCategory.AUTHORIZATION,
                                 "parallel_wave_reuses_child_approval")
        if (group.project_id != self.store.project_id or
                group.controller_host != self.store.controller_host):
            raise ExecutionError(FailureCategory.AUTHORIZATION,
                                 "admission_group_scope_mismatch")
        for item in jobs:
            if (item.plan.project.project_id != group.project_id or
                    item.plan.approval.max_active_resources != 1):
                raise ExecutionError(FailureCategory.AUTHORIZATION,
                                     "parallel_child_scope_mismatch")
            self.store.verify_admission_group_binding(
                group, item.plan.approval.approval_id)
            self.store.verify_execution_plan(item.first_attempt_id, item.plan)
            attempt = self.store.get_attempt(item.first_attempt_id)
            if (attempt["state"] != "reserved" or attempt["create_claimed"] or
                    attempt["job_id"] != item.plan.job_id or
                    attempt["approval_id"] != item.plan.approval.approval_id):
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                     "parallel_child_attempt_not_new")

        outcomes: dict[str, ParallelJobOutcome] = {}
        with ThreadPoolExecutor(max_workers=min(group.max_active_resources, len(jobs))) as pool:
            futures = {pool.submit(self._run_child, group, item): item for item in jobs}
            for future in as_completed(futures):
                job = futures[future]
                try:
                    outcome = future.result()
                except Exception:
                    outcome = ParallelJobOutcome(
                        job.plan.job_id, job.plan.approval.approval_id, None,
                        FailureCategory.INTERNAL, "parallel_child_unhandled")
                outcomes[job.first_attempt_id] = outcome
        return ParallelExecutionResult(
            group.group_id, tuple(outcomes[item.first_attempt_id] for item in jobs))
