"""Normalize exact-Pod billing responses into immutable common observations."""
from __future__ import annotations

import calendar
from datetime import datetime, timedelta, timezone
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
        start_epoch = _epoch_seconds(start)
        end_epoch = _epoch_seconds(end, end=True)
    except (OverflowError, ValueError):
        raise ContractError("billing query bounds are outside the supported range") from None
    if end_epoch <= start_epoch:
        raise ContractError("billing query interval must have positive duration")
    return start_epoch, end_epoch


def billing_period_bounds(value: Any, bucket_size: str) -> tuple[int, int]:
    """Map the documented REST v1 bucket start to conservative epoch bounds."""
    if not isinstance(value, str) or not value:
        raise ContractError("billing time bound is malformed")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ContractError("billing time bound is malformed") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ContractError("billing time bound is malformed")
    try:
        start = parsed.astimezone(timezone.utc)
    except (OverflowError, ValueError):
        raise ContractError("billing time bound is outside the supported range") from None
    try:
        if bucket_size == "hour":
            end = start + timedelta(hours=1)
        elif bucket_size == "day":
            end = start + timedelta(days=1)
        elif bucket_size == "week":
            end = start + timedelta(days=7)
        elif bucket_size == "month":
            year, month = start.year + (start.month == 12), (start.month % 12) + 1
            day = min(start.day, calendar.monthrange(year, month)[1])
            end = start.replace(year=year, month=month, day=day)
        elif bucket_size == "year":
            day = min(start.day, calendar.monthrange(start.year + 1, start.month)[1])
            end = start.replace(year=start.year + 1, day=day)
        else:
            raise ContractError("billing bucket size is malformed")
    except (OverflowError, ValueError):
        raise ContractError("billing period is outside the supported range") from None
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
    if not isinstance(response, list):
        raise ContractError("Pod billing response is malformed")
    if not isinstance(bucket_size, str) or bucket_size not in _BUCKETS:
        raise ContractError("billing bucket size is malformed")
    if type(observed_epoch) is not int or not 0 < observed_epoch <= _MAX_SQLITE_INTEGER:
        raise ContractError("billing observation time is malformed")
    requested_start, requested_end = billing_query_bounds(start_time, end_time)
    records = response
    if not isinstance(records, list) or len(records) > _MAX_RECORDS:
        raise ContractError("Pod billing response is malformed")

    try:
        evidence = json.dumps(records, sort_keys=True, separators=(",", ":"),
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
        start, end = billing_period_bounds(record.get("time"), bucket_size)
        if end <= requested_start or start >= requested_end:
            raise ContractError("billing response period is outside the requested interval")
        covered.append((start, end))
        amount = _amount_micro_usd(record.get("amount"))
        supersedes = (supersedes_observation_ids or {}).get((start, end))
        material = {
            "schema": "runpod-pod-billing-observation-v1",
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
