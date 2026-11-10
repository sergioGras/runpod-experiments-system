"""Validated, immutable application specifications for execution planning.

These records describe constraints; constructing them is not owner approval and
does not contact RunPod. The consumer must validate its own approval evidence.
"""
from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass
from enum import Enum
import hashlib
import json
import math
from pathlib import PurePosixPath
import re
from typing import Any


SCHEMA_VERSION = 1
MAX_DOCUMENT_BYTES = 1_048_576
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_PROJECT_ID = re.compile(r"[a-z][a-z0-9-]{2,63}\Z")
_PREFIX = re.compile(r"[a-z][a-z0-9-]+-\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_HOST = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,252}\Z")


class ContractError(ValueError):
    """A specification is malformed or internally inconsistent."""


class ResourceKind(str, Enum):
    CPU = "cpu"
    GPU = "gpu"


class TransferDirection(str, Enum):
    UPLOAD = "upload"
    DOWNLOAD = "download"


class FailureCategory(str, Enum):
    TRANSIENT_READ = "transient_read"
    VERIFIED_CAPACITY_REJECTION = "verified_capacity_rejection"
    SETUP_FAILURE = "setup_failure"
    CONFIGURATION = "configuration"
    AUTHORIZATION = "authorization"
    RESOURCE_MISMATCH = "resource_mismatch"
    AMBIGUOUS_OPERATION = "ambiguous_operation"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    TRANSPORT_FAILURE = "transport_failure"
    WORKLOAD_FAILURE = "workload_failure"
    SCIENTIFIC_FAILURE = "scientific_failure"
    BILLING_PENDING = "billing_pending"
    INTERNAL = "internal"


class OperationalStatus(str, Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RECONCILIATION_REQUIRED = "reconciliation_required"


class ScientificVerdict(str, Enum):
    NOT_EVALUATED = "not_evaluated"
    PASSED = "passed"
    FAILED = "failed"
    INCONCLUSIVE = "inconclusive"


class CleanupStatus(str, Enum):
    NOT_REQUIRED = "not_required"
    PENDING = "pending"
    CONFIRMED = "confirmed"
    FAILED = "failed"


class BillingStatus(str, Enum):
    NOT_APPLICABLE = "not_applicable"
    ESTIMATED = "estimated"
    PENDING = "pending"
    POSTED = "posted"


class StageStatus(str, Enum):
    STARTED = "started"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"


def _text(value: Any, name: str, pattern: re.Pattern[str] | None = None) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ContractError(f"{name} must be a non-empty trimmed string")
    if pattern is not None and pattern.fullmatch(value) is None:
        raise ContractError(f"{name} has an invalid format")


def _positive_int(value: Any, name: str) -> None:
    if type(value) is not int or value <= 0:
        raise ContractError(f"{name} must be a positive integer")


def _sha(value: Any, name: str) -> None:
    _text(value, name, _SHA256)


def _unique_text_tuple(values: Any, name: str, pattern: re.Pattern[str] | None = None,
                       *, allow_empty: bool = False) -> None:
    if not isinstance(values, tuple) or (not allow_empty and not values):
        raise ContractError(f"{name} must be an immutable tuple")
    for value in values:
        _text(value, name, pattern)
    if len(values) != len(set(values)):
        raise ContractError(f"{name} entries must be unique")


@dataclass(frozen=True, slots=True)
class ProjectPolicy:
    project_id: str
    pod_name_prefixes: tuple[str, ...]
    namespace_registry_sha256: str
    controller_hosts: tuple[str, ...]
    max_active_resources: int = 1
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _text(self.project_id, "project_id", _PROJECT_ID)
        _unique_text_tuple(self.pod_name_prefixes, "pod_name_prefixes", _PREFIX)
        for index, prefix in enumerate(self.pod_name_prefixes):
            if any(prefix.startswith(other) or other.startswith(prefix)
                   for other in self.pod_name_prefixes[index + 1:]):
                raise ContractError("project prefixes may not overlap")
        _sha(self.namespace_registry_sha256, "namespace_registry_sha256")
        _unique_text_tuple(self.controller_hosts, "controller_hosts", _HOST)
        if type(self.max_active_resources) is not int or self.max_active_resources not in (1, 2):
            raise ContractError("max_active_resources must select a supported serial or two-slot profile")
        if type(self.schema_version) is not int or self.schema_version != SCHEMA_VERSION:
            raise ContractError("unsupported ProjectPolicy schema version")


@dataclass(frozen=True, slots=True)
class ApprovalScope:
    approval_id: str
    project_id: str
    controller_host: str
    approval_evidence_sha256: str
    project_policy_sha256: str
    objective_sha256: str
    disclosure_sha256: str
    max_total_micro_usd: int
    max_hourly_micro_usd: int
    max_runtime_seconds: int
    max_attempts: int
    expires_epoch: int
    max_active_resources: int = 1
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _text(self.approval_id, "approval_id", _IDENTIFIER)
        _text(self.project_id, "project_id", _PROJECT_ID)
        _text(self.controller_host, "controller_host", _HOST)
        for field in ("approval_evidence_sha256", "project_policy_sha256", "objective_sha256",
                      "disclosure_sha256"):
            _sha(getattr(self, field), field)
        for field in ("max_total_micro_usd", "max_hourly_micro_usd", "max_runtime_seconds",
                      "max_attempts", "expires_epoch"):
            _positive_int(getattr(self, field), field)
        if type(self.max_active_resources) is not int or self.max_active_resources not in (1, 2):
            raise ContractError("max_active_resources must be 1 or 2")
        if type(self.schema_version) is not int or self.schema_version != SCHEMA_VERSION:
            raise ContractError("unsupported ApprovalScope schema version")


@dataclass(frozen=True, slots=True)
class ResourceProfile:
    kind: ResourceKind
    resource_id: str
    hourly_micro_usd: int
    region_ids: tuple[str, ...]
    image_digest: str
    disk_gb: int
    cpu_count: int
    memory_gb: int
    gpu_count: int = 0
    gpu_memory_gb: int = 0
    minimum_cuda: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.kind, ResourceKind):
            raise ContractError("kind must be a ResourceKind")
        _text(self.resource_id, "resource_id", re.compile(r"[A-Za-z0-9][A-Za-z0-9 _.-]{0,127}\Z"))
        for field in ("hourly_micro_usd", "disk_gb", "cpu_count", "memory_gb"):
            _positive_int(getattr(self, field), field)
        _unique_text_tuple(self.region_ids, "region_ids", _IDENTIFIER)
        _text(self.image_digest, "image_digest", re.compile(r"sha256:[0-9a-f]{64}\Z"))
        if type(self.gpu_count) is not int or self.gpu_count < 0:
            raise ContractError("gpu_count must be a nonnegative integer")
        if type(self.gpu_memory_gb) is not int or self.gpu_memory_gb < 0:
            raise ContractError("gpu_memory_gb must be a nonnegative integer")
        if self.kind is ResourceKind.CPU:
            if self.gpu_count or self.gpu_memory_gb or self.minimum_cuda is not None:
                raise ContractError("CPU profile cannot include GPU requirements")
        else:
            if self.gpu_count <= 0 or self.gpu_memory_gb <= 0:
                raise ContractError("GPU profile requires a positive count and memory floor")
            _text(self.minimum_cuda, "minimum_cuda", re.compile(r"[0-9]+\.[0-9]+\Z"))


@dataclass(frozen=True, slots=True)
class QuoteObservation:
    """Normalized provider quote; storage remains a separately identified estimate."""

    kind: ResourceKind
    resource_id: str
    requested_units: int
    max_configurable_units: int
    compute_hourly_micro_usd: int
    storage_hourly_micro_usd: int
    catalog_available: bool
    region_ids: tuple[str, ...]
    observed_epoch: int
    response_sha256: str
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.kind, ResourceKind):
            raise ContractError("kind must be a ResourceKind")
        _text(self.resource_id, "resource_id")
        _positive_int(self.requested_units, "requested_units")
        if type(self.max_configurable_units) is not int or self.max_configurable_units < 0:
            raise ContractError("max_configurable_units must be a nonnegative integer")
        if type(self.catalog_available) is not bool:
            raise ContractError("catalog_available must be a boolean")
        if self.catalog_available and self.max_configurable_units < self.requested_units:
            raise ContractError("available quote cannot be below the requested resource count")
        _positive_int(self.compute_hourly_micro_usd, "compute_hourly_micro_usd")
        if type(self.storage_hourly_micro_usd) is not int or self.storage_hourly_micro_usd < 0:
            raise ContractError("storage_hourly_micro_usd must be a nonnegative integer")
        _unique_text_tuple(self.region_ids, "region_ids", _IDENTIFIER, allow_empty=True)
        _positive_int(self.observed_epoch, "observed_epoch")
        _sha(self.response_sha256, "response_sha256")
        if type(self.schema_version) is not int or self.schema_version != SCHEMA_VERSION:
            raise ContractError("unsupported QuoteObservation schema version")

    @property
    def total_hourly_micro_usd(self) -> int:
        return self.compute_hourly_micro_usd + self.storage_hourly_micro_usd


@dataclass(frozen=True, slots=True)
class BillingObservation:
    """Immutable, exact-Pod billed amount observation from reviewed evidence."""

    observation_id: str
    project_id: str
    attempt_id: str
    provider_pod_id: str
    amount_micro_usd: int
    period_start_epoch: int
    period_end_epoch: int
    observed_epoch: int
    evidence_sha256: str
    supersedes_observation_id: str | None = None
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _text(self.observation_id, "observation_id", _IDENTIFIER)
        _text(self.project_id, "project_id", _PROJECT_ID)
        _text(self.attempt_id, "attempt_id", _IDENTIFIER)
        _text(self.provider_pod_id, "provider_pod_id", re.compile(r"[A-Za-z0-9_-]{1,128}\Z"))
        if type(self.amount_micro_usd) is not int or self.amount_micro_usd < 0:
            raise ContractError("amount_micro_usd must be a nonnegative integer")
        for field in ("period_start_epoch", "period_end_epoch", "observed_epoch"):
            _positive_int(getattr(self, field), field)
        if self.period_end_epoch <= self.period_start_epoch:
            raise ContractError("billing period must have positive duration")
        _sha(self.evidence_sha256, "evidence_sha256")
        if self.supersedes_observation_id is not None:
            _text(self.supersedes_observation_id, "supersedes_observation_id", _IDENTIFIER)
            if self.supersedes_observation_id == self.observation_id:
                raise ContractError("billing observation cannot supersede itself")
        if type(self.schema_version) is not int or self.schema_version != SCHEMA_VERSION:
            raise ContractError("unsupported BillingObservation schema version")


@dataclass(frozen=True, slots=True)
class ManifestFile:
    path: str
    sha256: str
    size_bytes: int
    direction: TransferDirection

    def __post_init__(self) -> None:
        _text(self.path, "path")
        pure = PurePosixPath(self.path)
        if ("\\" in self.path or "\x00" in self.path or "\n" in self.path or
                "\r" in self.path or ":" in self.path or pure.is_absolute() or
                pure.as_posix() != self.path or any(part in ("", ".", "..") for part in self.path.split("/"))):
            raise ContractError("manifest path must be a normalized relative POSIX path")
        _sha(self.sha256, "sha256")
        if type(self.size_bytes) is not int or self.size_bytes < 0:
            raise ContractError("size_bytes must be a nonnegative integer")
        if not isinstance(self.direction, TransferDirection):
            raise ContractError("direction must be a TransferDirection")


@dataclass(frozen=True, slots=True)
class TransferManifest:
    files: tuple[ManifestFile, ...]
    max_file_bytes: int
    max_total_bytes: int

    def __post_init__(self) -> None:
        if not isinstance(self.files, tuple) or any(not isinstance(x, ManifestFile) for x in self.files):
            raise ContractError("files must be an immutable tuple of ManifestFile")
        paths = [item.path for item in self.files]
        if len(paths) != len(set(paths)):
            raise ContractError("manifest paths must be unique")
        _positive_int(self.max_file_bytes, "max_file_bytes")
        _positive_int(self.max_total_bytes, "max_total_bytes")
        if any(item.size_bytes > self.max_file_bytes for item in self.files):
            raise ContractError("a manifest file exceeds max_file_bytes")
        if sum(item.size_bytes for item in self.files) > self.max_total_bytes:
            raise ContractError("manifest exceeds max_total_bytes")


_RETRYABLE = frozenset({FailureCategory.TRANSIENT_READ,
                        FailureCategory.VERIFIED_CAPACITY_REJECTION})


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int
    max_elapsed_seconds: int
    retryable_categories: tuple[FailureCategory, ...]
    initial_backoff_ms: int = 250
    maximum_backoff_ms: int = 10_000

    def __post_init__(self) -> None:
        for field in ("max_attempts", "max_elapsed_seconds", "initial_backoff_ms",
                      "maximum_backoff_ms"):
            _positive_int(getattr(self, field), field)
        if self.initial_backoff_ms > self.maximum_backoff_ms:
            raise ContractError("initial backoff must not exceed maximum backoff")
        if not isinstance(self.retryable_categories, tuple):
            raise ContractError("retryable_categories must be an immutable tuple")
        if self.max_attempts > 1 and not self.retryable_categories:
            raise ContractError("a multi-attempt policy must declare retryable categories")
        if any(not isinstance(item, FailureCategory) or item not in _RETRYABLE
               for item in self.retryable_categories):
            raise ContractError("retry policy includes a category that cannot be retried safely")
        if len(set(self.retryable_categories)) != len(self.retryable_categories):
            raise ContractError("retryable categories must be unique")


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    job_id: str
    project: ProjectPolicy
    approval: ApprovalScope
    source_commit: str
    source_sha256: str
    config_sha256: str
    package_sha256: str
    resource_options: tuple[ResourceProfile, ...]
    transfers: TransferManifest
    retry: RetryPolicy
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _text(self.job_id, "job_id", _IDENTIFIER)
        if not isinstance(self.project, ProjectPolicy):
            raise ContractError("project must be a ProjectPolicy")
        if not isinstance(self.approval, ApprovalScope):
            raise ContractError("approval must be an ApprovalScope")
        if (self.project.project_id != self.approval.project_id or
                self.approval.controller_host not in self.project.controller_hosts or
                self.approval.max_active_resources > self.project.max_active_resources or
                self.approval.project_policy_sha256 !=
                sha256_bytes(encode_document("project_policy", self.project))):
            raise ContractError("approval scope differs from registered project policy")
        _text(self.source_commit, "source_commit", _COMMIT)
        for field in ("source_sha256", "config_sha256", "package_sha256"):
            _sha(getattr(self, field), field)
        if (not isinstance(self.resource_options, tuple) or not self.resource_options or
                any(not isinstance(item, ResourceProfile) for item in self.resource_options)):
            raise ContractError("resource_options must be a non-empty immutable tuple")
        if len(set(self.resource_options)) != len(self.resource_options):
            raise ContractError("resource_options must be unique")
        if not isinstance(self.transfers, TransferManifest):
            raise ContractError("transfers must be a TransferManifest")
        if not isinstance(self.retry, RetryPolicy):
            raise ContractError("retry must be a RetryPolicy")
        if self.retry.max_attempts > self.approval.max_attempts:
            raise ContractError("retry attempts exceed the validated approval scope")
        if self.retry.max_elapsed_seconds > self.approval.max_runtime_seconds:
            raise ContractError("retry window exceeds the approved runtime ceiling")
        if any(option.hourly_micro_usd > self.approval.max_hourly_micro_usd
               for option in self.resource_options):
            raise ContractError("resource option exceeds the approved hourly ceiling")
        if type(self.schema_version) is not int or self.schema_version != SCHEMA_VERSION:
            raise ContractError("unsupported ExecutionPlan schema version")


@dataclass(frozen=True, slots=True)
class AttemptPlan:
    attempt_id: str
    job_id: str
    approval_id: str
    attempt_number: int
    request_sha256: str
    quote_sha256: str
    policy_sha256: str
    source_sha256: str
    config_sha256: str
    package_sha256: str
    selected_resource: ResourceProfile
    selected_region: str
    runtime_seconds: int
    cleanup_reserve_seconds: int
    created_epoch: int
    deadline_epoch: int

    def __post_init__(self) -> None:
        _text(self.attempt_id, "attempt_id", _IDENTIFIER)
        _text(self.job_id, "job_id", _IDENTIFIER)
        _text(self.approval_id, "approval_id", _IDENTIFIER)
        _positive_int(self.attempt_number, "attempt_number")
        for field in ("request_sha256", "quote_sha256", "policy_sha256", "source_sha256",
                      "config_sha256", "package_sha256"):
            _sha(getattr(self, field), field)
        if not isinstance(self.selected_resource, ResourceProfile):
            raise ContractError("selected_resource must be a ResourceProfile")
        _text(self.selected_region, "selected_region", _IDENTIFIER)
        for field in ("runtime_seconds", "cleanup_reserve_seconds", "created_epoch", "deadline_epoch"):
            _positive_int(getattr(self, field), field)
        if self.deadline_epoch <= self.created_epoch:
            raise ContractError("attempt deadline must follow creation")
        if self.runtime_seconds + self.cleanup_reserve_seconds > self.deadline_epoch - self.created_epoch:
            raise ContractError("attempt runtime and cleanup reserve exceed its deadline")


def validate_attempt(plan: ExecutionPlan, attempt: AttemptPlan) -> None:
    """Check that a concrete attempt remains inside its immutable plan and scope."""
    if not isinstance(plan, ExecutionPlan) or not isinstance(attempt, AttemptPlan):
        raise ContractError("validate_attempt requires an ExecutionPlan and AttemptPlan")
    approval = plan.approval
    policy_hash = sha256_bytes(encode_document("project_policy", plan.project))
    if (attempt.job_id != plan.job_id or attempt.approval_id != approval.approval_id or
            attempt.attempt_number > plan.retry.max_attempts or
            attempt.attempt_number > approval.max_attempts):
        raise ContractError("attempt identity or number exceeds the execution scope")
    if (attempt.selected_resource not in plan.resource_options or
            attempt.selected_region not in attempt.selected_resource.region_ids):
        raise ContractError("attempt selected an unapproved resource or region")
    if attempt.selected_resource.hourly_micro_usd > approval.max_hourly_micro_usd:
        raise ContractError("attempt resource exceeds the approved hourly ceiling")
    if attempt.runtime_seconds > approval.max_runtime_seconds:
        raise ContractError("attempt runtime exceeds the approved runtime ceiling")
    if attempt.runtime_seconds + attempt.cleanup_reserve_seconds > approval.max_runtime_seconds:
        raise ContractError("attempt runtime and cleanup reserve exceed the approved time ceiling")
    if (attempt.created_epoch >= approval.expires_epoch or
            attempt.deadline_epoch > approval.expires_epoch):
        raise ContractError("attempt exceeds the approval expiration")
    if (attempt.policy_sha256 != policy_hash or attempt.source_sha256 != plan.source_sha256 or
            attempt.config_sha256 != plan.config_sha256 or
            attempt.package_sha256 != plan.package_sha256):
        raise ContractError("attempt provenance differs from the execution plan")


@dataclass(frozen=True, slots=True)
class ExecutionOutcome:
    operational_status: OperationalStatus
    scientific_verdict: ScientificVerdict
    cleanup_status: CleanupStatus
    billing_status: BillingStatus
    failure_category: FailureCategory | None = None
    evidence_sha256: tuple[str, ...] = ()
    billing_evidence_sha256: str | None = None

    def __post_init__(self) -> None:
        for field, expected in (("operational_status", OperationalStatus),
                                ("scientific_verdict", ScientificVerdict),
                                ("cleanup_status", CleanupStatus),
                                ("billing_status", BillingStatus)):
            if not isinstance(getattr(self, field), expected):
                raise ContractError(f"{field} has an invalid value")
        if self.failure_category is not None and not isinstance(self.failure_category, FailureCategory):
            raise ContractError("failure_category has an invalid value")
        if self.operational_status is OperationalStatus.SUCCEEDED and self.failure_category is not None:
            raise ContractError("a successful operational outcome cannot carry a failure category")
        if (self.operational_status is OperationalStatus.FAILED and
                self.failure_category is None):
            raise ContractError("a failed operational outcome requires a failure category")
        if self.billing_evidence_sha256 is not None:
            _sha(self.billing_evidence_sha256, "billing_evidence_sha256")
        if self.billing_status is BillingStatus.POSTED and self.billing_evidence_sha256 is None:
            raise ContractError("posted billing requires evidence")
        if not isinstance(self.evidence_sha256, tuple):
            raise ContractError("evidence_sha256 must be an immutable tuple")
        for digest in self.evidence_sha256:
            _sha(digest, "evidence_sha256 entry")
        if len(self.evidence_sha256) != len(set(self.evidence_sha256)):
            raise ContractError("evidence digests must be unique")


@dataclass(frozen=True, slots=True)
class StageEvent:
    """Redacted append-only stage observation; free-form messages are excluded."""

    event_id: str
    job_id: str
    attempt_id: str | None
    sequence: int
    occurred_epoch: int
    stage: str
    status: StageStatus
    failure_category: FailureCategory | None = None
    error_code: str | None = None
    evidence_sha256: tuple[str, ...] = ()
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _text(self.event_id, "event_id", _IDENTIFIER)
        _text(self.job_id, "job_id", _IDENTIFIER)
        if self.attempt_id is not None:
            _text(self.attempt_id, "attempt_id", _IDENTIFIER)
        _positive_int(self.sequence, "sequence")
        _positive_int(self.occurred_epoch, "occurred_epoch")
        _text(self.stage, "stage", re.compile(r"[a-z][a-z0-9_.-]{0,63}\Z"))
        if not isinstance(self.status, StageStatus):
            raise ContractError("status must be a StageStatus")
        if self.failure_category is not None and not isinstance(self.failure_category, FailureCategory):
            raise ContractError("failure_category has an invalid value")
        if self.error_code is not None:
            _text(self.error_code, "error_code", re.compile(r"[a-z][a-z0-9_.-]{0,63}\Z"))
        if self.status is StageStatus.FAILED:
            if self.failure_category is None or self.error_code is None:
                raise ContractError("failed stage events require a category and sanitized error code")
        elif self.failure_category is not None or self.error_code is not None:
            raise ContractError("only failed stage events may include failure details")
        if not isinstance(self.evidence_sha256, tuple):
            raise ContractError("evidence_sha256 must be an immutable tuple")
        for digest in self.evidence_sha256:
            _sha(digest, "evidence_sha256 entry")
        if len(self.evidence_sha256) != len(set(self.evidence_sha256)):
            raise ContractError("evidence digests must be unique")
        if type(self.schema_version) is not int or self.schema_version != SCHEMA_VERSION:
            raise ContractError("unsupported StageEvent schema version")


def _plain(value: Any) -> Any:
    """Convert supported immutable values to JSON data without lossy coercion."""
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _plain(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, tuple):
        return [_plain(item) for item in value]
    if isinstance(value, list):
        return [_plain(item) for item in value]
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise ContractError("JSON object keys must be strings")
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, float) and not math.isfinite(value):
        raise ContractError("nonfinite numbers are not supported")
    if value is None or type(value) in (str, int, float, bool):
        return value
    raise ContractError(f"unsupported JSON value type: {type(value).__name__}")


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize supported values to deterministic UTF-8 JSON bytes."""
    try:
        text = json.dumps(_plain(value), sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError, UnicodeError):
        raise ContractError("value cannot be encoded as canonical JSON") from None
    try:
        encoded = text.encode("utf-8")
    except UnicodeEncodeError:
        raise ContractError("value cannot be encoded as canonical JSON") from None
    if len(encoded) > MAX_DOCUMENT_BYTES:
        raise ContractError("canonical document exceeds the maximum size")
    return encoded


def encode_document(kind: str, payload: Any) -> bytes:
    """Add the contract schema marker to a deterministic document."""
    _text(kind, "kind", re.compile(r"[a-z][a-z0-9_]{0,63}\Z"))
    if not is_dataclass(payload) or isinstance(payload, type):
        raise ContractError("document payload must be a validated dataclass")
    return canonical_json_bytes({"kind": kind, "payload": payload,
                                 "schema_version": SCHEMA_VERSION})


def _object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ContractError("duplicate JSON object key")
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    raise ContractError("nonfinite JSON number is not supported")


def strict_json_loads(raw: bytes | str) -> Any:
    """Parse JSON while rejecting duplicate keys and nonfinite numbers."""
    if isinstance(raw, bytes):
        if len(raw) > MAX_DOCUMENT_BYTES:
            raise ContractError("document exceeds the maximum size")
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError:
            raise ContractError("document must be UTF-8") from None
    if not isinstance(raw, str):
        raise ContractError("document must be bytes or text")
    if len(raw) > MAX_DOCUMENT_BYTES:
        raise ContractError("document exceeds the maximum size")
    try:
        if len(raw.encode("utf-8")) > MAX_DOCUMENT_BYTES:
            raise ContractError("document exceeds the maximum size")
    except UnicodeEncodeError:
        raise ContractError("document must be valid Unicode") from None
    try:
        value = json.loads(raw, object_pairs_hook=_object_pairs,
                           parse_constant=_reject_constant)
    except ContractError:
        raise
    except (ValueError, TypeError, RecursionError):
        raise ContractError("invalid JSON document") from None

    def check(item: Any) -> None:
        if isinstance(item, float) and not math.isfinite(item):
            raise ContractError("nonfinite JSON number is not supported")
        if isinstance(item, list):
            for element in item:
                check(element)
        elif isinstance(item, dict):
            for element in item.values():
                check(element)

    check(value)
    return value


def decode_document(raw: bytes | str, *, expected_kind: str) -> dict[str, Any]:
    """Validate the document envelope and return its payload for typed parsing."""
    _text(expected_kind, "expected_kind", re.compile(r"[a-z][a-z0-9_]{0,63}\Z"))
    value = strict_json_loads(raw)
    if (not isinstance(value, dict) or set(value) != {"kind", "payload", "schema_version"} or
            type(value.get("schema_version")) is not int or
            value["schema_version"] != SCHEMA_VERSION or value.get("kind") != expected_kind or
            not isinstance(value.get("payload"), dict)):
        raise ContractError("document kind, payload or schema version is unsupported")
    return value["payload"]


def sha256_bytes(data: bytes) -> str:
    if not isinstance(data, bytes):
        raise ContractError("digest input must be bytes")
    return hashlib.sha256(data).hexdigest()
