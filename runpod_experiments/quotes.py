"""Convert raw RunPod catalog records into bounded normalized quote values."""
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
import math
import re
from typing import Any

from runpod_experiments.contracts import (
    ContractError, QuoteObservation, ResourceKind, canonical_json_bytes,
    sha256_bytes,
)
from runpod_experiments.errors import ExecutionError
from runpod_experiments.contracts import FailureCategory


_AVAILABLE = frozenset({"HIGH", "MEDIUM", "LOW"})


def _invalid(kind: str) -> ExecutionError:
    return ExecutionError(FailureCategory.RESOURCE_MISMATCH, f"{kind}_quote_invalid")


def _positive_count(value: Any) -> bool:
    return type(value) is int and value > 0


def _host_memory_floor_gb(per_vcpu: Any, vcpu_count: int) -> int:
    if isinstance(per_vcpu, bool) or not isinstance(per_vcpu, (str, int, float, Decimal)):
        raise ValueError("unsupported CPU memory value")
    if isinstance(per_vcpu, float) and not math.isfinite(per_vcpu):
        raise ValueError("nonfinite CPU memory value")
    try:
        total = Decimal(str(per_vcpu)) * vcpu_count
        if not total.is_finite() or total <= 0 or total > 1_000_000:
            raise ValueError("CPU memory value is outside its range")
        return int(total.to_integral_value(rounding=ROUND_FLOOR))
    except (InvalidOperation, OverflowError):
        raise ValueError("invalid CPU memory value") from None


def _amount_micro_usd(value: Any, *, allow_zero: bool) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("unsupported monetary value")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("nonfinite monetary value")
    try:
        amount = Decimal(str(value))
        if not amount.is_finite() or amount < 0 or (not allow_zero and amount == 0):
            raise ValueError("monetary value is outside its range")
        return int((amount * 1_000_000).to_integral_value(rounding=ROUND_CEILING))
    except (InvalidOperation, OverflowError):
        raise ValueError("invalid monetary value") from None


def _regions(offer: dict[str, Any], kind: str) -> tuple[str, ...]:
    centers = offer.get("dataCenters")
    if centers is None:
        return ()
    if not isinstance(centers, list):
        raise _invalid(kind)
    result = []
    for center in centers:
        if (not isinstance(center, dict) or not isinstance(center.get("id"), str) or
                not center["id"] or not isinstance(center.get("availability"), str)):
            raise _invalid(kind)
        if center["availability"] in _AVAILABLE:
            result.append(center["id"])
    if len(result) != len(set(result)):
        raise _invalid(kind)
    return tuple(result)


def _response_hash(offer: dict[str, Any], kind: str) -> str:
    try:
        return sha256_bytes(canonical_json_bytes(offer))
    except ContractError:
        raise _invalid(kind) from None


def normalize_cpu_quote(offer: Any, *, resource_id: str, vcpu_count: int,
                        observed_epoch: int, storage_hourly_usd: str | int | float | Decimal = "0") -> QuoteObservation:
    """Normalize a CPU offer using the Secure per-vCPU rate and current shape."""
    kind = "cpu"
    if (not isinstance(offer, dict) or offer.get("id") != resource_id or
            not isinstance(resource_id, str) or not resource_id or
            not _positive_count(vcpu_count)):
        raise _invalid(kind)
    price = (offer.get("price") or {}).get("securePerVcpu") if isinstance(offer.get("price"), dict) else None
    shape = offer.get("vcpu")
    memory = offer.get("ramGbPerVcpu")
    if (not isinstance(shape, dict) or not _positive_count(shape.get("min")) or
            not _positive_count(shape.get("max")) or shape["min"] > shape["max"] or
            not shape["min"] <= vcpu_count <= shape["max"]):
        raise _invalid(kind)
    centers = _regions(offer, kind)
    status = offer.get("availability")
    if status is not None and not isinstance(status, str):
        raise _invalid(kind)
    try:
        per_vcpu = _amount_micro_usd(price, allow_zero=False)
        storage = _amount_micro_usd(storage_hourly_usd, allow_zero=True)
        compute = per_vcpu * vcpu_count
        available = bool(centers) if offer.get("dataCenters") else status in _AVAILABLE
        memory_gb = _host_memory_floor_gb(memory, vcpu_count)
        return QuoteObservation(
            ResourceKind.CPU, resource_id, vcpu_count, shape["max"], compute,
            storage, available, centers, observed_epoch, _response_hash(offer, kind), memory_gb)
    except (ContractError, ValueError):
        raise _invalid(kind) from None


def normalize_gpu_quote(offer: Any, *, resource_id: str, gpu_count: int,
                        observed_epoch: int, storage_hourly_usd: str | int | float | Decimal = "0",
                        minimum_cuda: str | None = None) -> QuoteObservation:
    """Normalize a GPU Secure rate and capacity; regions stay separate evidence."""
    kind = "gpu"
    memory = offer.get("memory") if isinstance(offer, dict) else None
    if (not isinstance(offer, dict) or offer.get("id") != resource_id or
            not isinstance(resource_id, str) or not resource_id or
            not _positive_count(gpu_count) or gpu_count > 64 or offer.get("secure") is not True or
            not _positive_count(memory) or
            (minimum_cuda is not None and
             (not isinstance(minimum_cuda, str) or
              re.fullmatch(r"[0-9]+\.[0-9]+", minimum_cuda) is None))):
        raise _invalid(kind)
    price = (offer.get("price") or {}).get("secure") if isinstance(offer.get("price"), dict) else None
    maximum = (offer.get("maxCount") or {}).get("secure") if isinstance(offer.get("maxCount"), dict) else None
    if type(maximum) is not int or maximum < 0:
        raise _invalid(kind)
    centers = _regions(offer, kind)
    try:
        per_gpu = _amount_micro_usd(price, allow_zero=False)
        storage = _amount_micro_usd(storage_hourly_usd, allow_zero=True)
        compute = per_gpu * gpu_count
        return QuoteObservation(
            ResourceKind.GPU, resource_id, gpu_count, maximum, compute, storage,
            maximum >= gpu_count, centers, observed_epoch, _response_hash(offer, kind), memory,
            minimum_cuda)
    except (ContractError, ValueError):
        raise _invalid(kind) from None
