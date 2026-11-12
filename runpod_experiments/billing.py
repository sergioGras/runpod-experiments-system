"""Normalize exact-Pod billing responses into immutable common observations."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING
import hashlib
import json
import math
from typing import Any, Mapping

from runpod_experiments.contracts import (
    BillingObservation, ContractError, canonical_json_bytes,
)


_BUCKETS = frozenset({"hour", "day", "week", "month", "year"})
_MAX_RECORDS = 10_000
_MAX_EVIDENCE_BYTES = 8_388_608
_MAX_SQLITE_INTEGER = (1 << 63) - 1
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _amount_micro_usd(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ContractError("billing amount is malformed")
    if isinstance(value, float) and not math.isfinite(value):
        raise ContractError("billing amount is malformed")
    try:
        amount = Decimal(str(value))
        if not amount.is_finite() or amount < 0:
            raise ContractError("billing amount is malformed")
        amount_micro_usd = int((amount * 1_000_000).to_integral_value(rounding=ROUND_CEILING))
        if amount_micro_usd > _MAX_SQLITE_INTEGER:
            raise ContractError("billing amount is outside the supported range")
        return amount_micro_usd
    except (InvalidOperation, OverflowError, ValueError):
        raise ContractError("billing amount is malformed") from None


def _epoch_seconds(value: datetime, *, end: bool = False) -> int:
    delta = value.astimezone(timezone.utc) - _EPOCH
    seconds = delta.days * 86_400 + delta.seconds
    if end and delta.microseconds:
        seconds += 1
    if seconds <= 0:
        raise ContractError("billing time bound is outside the supported range")
    return seconds


def billing_query_bounds(start_time: Any, end_time: Any) -> tuple[int, int]:
    """Validate and normalize the required start/end query for attempt billing."""
    start, end = billing_interval_datetimes(start_time, end_time)
    try:
        start_epoch = _epoch_seconds(start)
        end_epoch = _epoch_seconds(end, end=True)
    except (OverflowError, ValueError):
        raise ContractError("billing query bounds are outside the supported range") from None
    if end_epoch <= start_epoch:
        raise ContractError("billing query interval must have positive duration")
    return start_epoch, end_epoch


def billing_interval_datetimes(start_time: Any, end_time: Any) -> tuple[datetime, datetime]:
    """Parse and compare RFC 3339 bounds without losing subsecond precision."""
    if not isinstance(start_time, str) or not isinstance(end_time, str):
        raise ContractError("billing query bounds are required")
    try:
        start = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        end = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
    except ValueError:
        raise ContractError("billing query bounds are malformed") from None
    if (start.tzinfo is None or start.utcoffset() is None or
            end.tzinfo is None or end.utcoffset() is None):
        raise ContractError("billing query bounds are malformed")
    try:
        start = start.astimezone(timezone.utc)
        end = end.astimezone(timezone.utc)
    except (OverflowError, ValueError):
        raise ContractError("billing query bounds are outside the supported range") from None
    if end <= start:
        raise ContractError("billing query interval must have positive duration")
    return start, end


def billing_period_bounds(start_value: Any, end_value: Any) -> tuple[int, int]:
    """Normalize the explicit period bounds returned by RunPod REST v2."""
    if (not isinstance(start_value, str) or not start_value or
            not isinstance(end_value, str) or not end_value):
        raise ContractError("billing time bounds are malformed")
    start, end = billing_interval_datetimes(start_value, end_value)
    start_epoch = _epoch_seconds(start)
    end_epoch = _epoch_seconds(end, end=True)
    if end_epoch <= start_epoch:
        raise ContractError("billing period is malformed")
    return start_epoch, end_epoch


def normalize_pod_billing(
    response: Any,
    *,
    project_id: str,
    attempt_id: str,
    provider_pod_id: str,
    observed_epoch: int,
    bucket_size: str = "hour",
    start_time: str | None = None,
    end_time: str | None = None,
    supersedes_observation_ids: Mapping[tuple[int, int], str] | None = None,
) -> tuple[BillingObservation, ...]:
    """Return safe observations without retaining raw provider response data.

    Empty responses produce no observation: they do not prove zero final cost or
    permit a caller to release an exposure hold. Replacement links are explicit
    because only the caller can identify which persisted observation is replaced.
    """
    if not isinstance(response, dict):
        raise ContractError("Pod billing response is malformed")
    if not isinstance(bucket_size, str) or bucket_size not in _BUCKETS:
        raise ContractError("billing bucket size is malformed")
    if type(observed_epoch) is not int or not 0 < observed_epoch <= _MAX_SQLITE_INTEGER:
        raise ContractError("billing observation time is malformed")
    requested_start, requested_end = billing_interval_datetimes(start_time, end_time)
    records = response.get("records")
    if not isinstance(records, list) or len(records) > _MAX_RECORDS:
        raise ContractError("Pod billing response is malformed")
    metadata = response.get("metadata")
    query = metadata.get("query") if isinstance(metadata, dict) else None
    if not isinstance(query, dict):
        raise ContractError("Pod billing query metadata is malformed")
    if (query.get("podId") != provider_pod_id or query.get("bucketSize") != bucket_size):
        raise ContractError("Pod billing query scope differs from the requested Pod")
    effective_start, effective_end = billing_interval_datetimes(
        query.get("startTime"), query.get("endTime"))
    if effective_start > requested_start or effective_end < requested_end:
        raise ContractError("Pod billing query metadata does not cover the requested interval")
    record_count = metadata.get("recordCount")
    if type(record_count) is not int or record_count != len(records):
        raise ContractError("Pod billing record count differs from query metadata")

    try:
        evidence = json.dumps(response, sort_keys=True, separators=(",", ":"),
                              ensure_ascii=False, allow_nan=False).encode("utf-8")
        if len(evidence) > _MAX_EVIDENCE_BYTES:
            raise ValueError("billing evidence exceeds the supported size")
        evidence_sha256 = hashlib.sha256(evidence).hexdigest()
    except (TypeError, ValueError, UnicodeError):
        raise ContractError("Pod billing evidence is malformed") from None

    if supersedes_observation_ids is not None and not isinstance(supersedes_observation_ids, Mapping):
        raise ContractError("billing replacement links are malformed")
    observations = []
    covered = []
    for record in records:
        if not isinstance(record, dict) or record.get("podId") != provider_pod_id:
            raise ContractError("Pod billing record identity is malformed")
        start_time = record.get("startTime")
        end_time = record.get("endTime")
        period_start, period_end = billing_interval_datetimes(start_time, end_time)
        start, end = billing_period_bounds(start_time, end_time)
        if period_start < effective_start or period_end > effective_end:
            raise ContractError("billing response period is outside its query metadata")
        if period_end <= requested_start or period_start >= requested_end:
            raise ContractError("billing response period is outside the requested interval")
        covered.append((start, end))
        amount = _amount_micro_usd(record.get("totalAmount"))
        supersedes = (supersedes_observation_ids or {}).get((start, end))
        material = {
            "schema": "runpod-pod-billing-observation-v2",
            "project_id": project_id,
            "attempt_id": attempt_id,
            "provider_pod_id": provider_pod_id,
            "amount_micro_usd": amount,
            "period_start_epoch": start,
            "period_end_epoch": end,
            "observed_epoch": observed_epoch,
            "evidence_sha256": evidence_sha256,
            "supersedes_observation_id": supersedes,
        }
        observation_id = "bill-" + hashlib.sha256(canonical_json_bytes(material)).hexdigest()
        try:
            observations.append(BillingObservation(
                observation_id=observation_id,
                project_id=project_id,
                attempt_id=attempt_id,
                provider_pod_id=provider_pod_id,
                amount_micro_usd=amount,
                period_start_epoch=start,
                period_end_epoch=end,
                observed_epoch=observed_epoch,
                evidence_sha256=evidence_sha256,
                supersedes_observation_id=supersedes,
            ))
        except ContractError:
            raise
    ordered = sorted(covered)
    if any(current[0] < previous[1] for previous, current in zip(ordered, ordered[1:])):
        raise ContractError("Pod billing periods overlap")
    if supersedes_observation_ids is not None and set(supersedes_observation_ids) - set(covered):
        raise ContractError("billing replacement link has no matching period")
    return tuple(observations)
