"""Validate a supervisor's pinned resource scope and hourly estimate ceiling."""
import hashlib
import json
import math
import re
from pathlib import Path
from decimal import Decimal, InvalidOperation

from runpod_experiments.contracts import ResourceKind, ResourceProfile


def _matches_subset(actual, expected):
    """Match nested provider shapes while allowing documented extra fields."""
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            key in actual and _matches_subset(actual[key], value)
            for key, value in expected.items())
    return actual == expected


def request_hash(request):
    raw = json.dumps(request, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def resource_profile_static_violation(pod, profile, selected_region):
    """Check the provider-visible profile fields, excluding per-card GPU evidence."""
    if not isinstance(profile, ResourceProfile) or not isinstance(pod, dict):
        return "invalid_or_unknown_resource_profile"
    if not isinstance(selected_region, str) or selected_region not in profile.region_ids:
        return "invalid_or_unknown_resource_profile"
    status = pod.get("status")
    if not isinstance(status, str) or status not in {"PROVISIONING", "STARTING", "RUNNING"}:
        return "provider_failure_or_unknown_state"
    cost = pod.get("cost")
    if isinstance(cost, bool) or not isinstance(cost, (int, float)):
        return "invalid_or_excess_hourly_rate"
    try:
        hourly_micro_usd = Decimal(str(cost)) * Decimal(1_000_000)
        if not hourly_micro_usd.is_finite() or hourly_micro_usd <= 0:
            return "invalid_or_excess_hourly_rate"
    except (InvalidOperation, ValueError):
        return "invalid_or_excess_hourly_rate"
    if hourly_micro_usd > profile.hourly_micro_usd:
        return "invalid_or_excess_hourly_rate"
    if status != "RUNNING":
        return None

    image = pod.get("image")
    image_digests = re.findall(r"sha256:[0-9a-f]{64}", image) if isinstance(image, str) else []
    mounts = pod.get("mounts")
    if (pod.get("cloud") != "SECURE" or type(pod.get("disk")) is not int or
            pod.get("disk") != profile.disk_gb or
            pod.get("dataCenterId") != selected_region or
            image_digests != [profile.image_digest] or
            not isinstance(mounts, dict) or any(mounts.values())):
        return "unexpected_resource_shape"

    if profile.kind is ResourceKind.CPU:
        cpu = pod.get("cpu")
        if (not isinstance(cpu, dict) or cpu.get("id") != profile.resource_id or
                type(cpu.get("vcpuCount")) is not int or cpu.get("vcpuCount") != profile.cpu_count or
                type(cpu.get("memory")) is not int or cpu.get("memory") != profile.memory_gb):
            return "unexpected_resource_shape"
    else:
        gpu = pod.get("gpu")
        if (not isinstance(gpu, dict) or gpu.get("id") != profile.resource_id or
                type(gpu.get("count")) is not int or gpu.get("count") != profile.gpu_count or
                type(gpu.get("vcpuCount")) is not int or gpu.get("vcpuCount") < profile.cpu_count or
                type(gpu.get("memory")) is not int or gpu["memory"] < profile.memory_gb):
            return "unexpected_resource_shape"
        cuda = pod.get("cudaVersion")
        if isinstance(cuda, str) and re.fullmatch(r"[0-9]+\.[0-9]+", cuda):
            actual_cuda = tuple(int(part) for part in cuda.split("."))
            minimum_cuda = tuple(int(part) for part in profile.minimum_cuda.split("."))
            if actual_cuda < minimum_cuda:
                return "unexpected_resource_shape"
    return None


def resource_profile_violation(pod, profile, selected_region):
    """Check provider-visible shape and keep GPU access fail-closed by default.

    REST v2 GPU memory reports host system RAM, not per-card VRAM. Callers must
    use an exact-attempt attestation path before treating a GPU as verified.
    """
    violation = resource_profile_static_violation(pod, profile, selected_region)
    if violation is not None:
        return violation
    if profile.kind is ResourceKind.GPU and pod.get("status") == "RUNNING":
        return "unverifiable_gpu_profile"
    return None


class ResourcePolicy:
    def __init__(self, data, fingerprint):
        rate = data.get("max_hourly_usd") if isinstance(data, dict) else None
        shape = data.get("expected_shape") if isinstance(data, dict) else None
        request_digest = data.get("request_sha256") if isinstance(data, dict) else None
        no_mounts = data.get("no_persistent_mounts", False) if isinstance(data, dict) else None
        if (isinstance(rate, bool) or not isinstance(rate, (int, float)) or not math.isfinite(rate) or rate <= 0 or
                not isinstance(shape, dict) or not shape or not isinstance(request_digest, str) or
                not re.fullmatch(r"[0-9a-f]{64}", request_digest) or type(no_mounts) is not bool):
            raise ValueError("Invalid supervisor resource policy")
        self.max_hourly_usd, self.expected_shape, self.fingerprint, self.request_sha256 = rate, shape, fingerprint, request_digest
        self.no_persistent_mounts = no_mounts

    @classmethod
    def read(cls, path):
        raw = Path(path).read_bytes()
        return cls(json.loads(raw), hashlib.sha256(raw).hexdigest())

    def violation(self, pod):
        rate = pod.get("cost")
        if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not math.isfinite(rate) or rate <= 0 or rate > self.max_hourly_usd:
            return "invalid_or_excess_hourly_rate"
        if pod.get("status") not in {"PROVISIONING", "STARTING", "RUNNING"}:
            return "provider_failure_or_unknown_state"
        if pod.get("status") == "RUNNING" and any(
                key not in pod or not _matches_subset(pod[key], value)
                for key, value in self.expected_shape.items()):
            return "unexpected_resource_shape"
        if self.no_persistent_mounts and pod.get("status") == "RUNNING":
            mounts = pod.get("mounts")
            if not isinstance(mounts, dict) or any(mounts.values()):
                return "unexpected_or_unknown_mounts"
        return None
