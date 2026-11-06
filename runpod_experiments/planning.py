"""Derive a conservative local time envelope from the single round cap."""
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
import math
import re

from runpod_experiments.resource_policy import request_hash


def runtime_from_remaining(remaining_usd, hourly_usd, *, cleanup_seconds=120):
    """Return work seconds and total seconds based on a current hourly estimate.

    This is a planning estimate. Provider charges and price changes can exceed
    it; the estimate must include storage and other known ongoing charges.
    """
    if type(cleanup_seconds) is not int or cleanup_seconds < 0:
        raise ValueError("Invalid cleanup margin")
    try:
        remaining = Decimal(str(remaining_usd))
        hourly = Decimal(str(hourly_usd))
        if not remaining.is_finite() or not hourly.is_finite() or remaining <= 0 or hourly <= 0:
            raise ValueError("Remaining budget and hourly estimate must be positive")
        total_seconds = int((remaining * 3600 / hourly).to_integral_value(rounding=ROUND_FLOOR))
    except (InvalidOperation, ValueError, OverflowError) as error:
        raise ValueError("Invalid monetary estimate") from error
    if total_seconds <= cleanup_seconds:
        raise ValueError("Remaining budget cannot cover the cleanup margin")
    return {"work_seconds": total_seconds - cleanup_seconds, "total_seconds": total_seconds}


def cpu_resource_policy(request, offer, storage_hourly_usd):
    """Build a policy from a freshly retrieved CPU quote and fixed request."""
    allowed = {"name", "cpu", "cloud", "dataCenterIds", "image", "disk", "ports", "startSsh", "startJupyter"}
    if not isinstance(request, dict) or set(request) != allowed:
        raise ValueError("CPU request has missing or unreviewed fields")
    cpu, regions = request.get("cpu"), request.get("dataCenterIds")
    if (not isinstance(cpu, dict) or set(cpu) != {"id", "vcpuCount"} or
            not isinstance(cpu.get("id"), str) or type(cpu.get("vcpuCount")) is not int or
            cpu["vcpuCount"] < 2 or cpu["vcpuCount"] & (cpu["vcpuCount"] - 1) or
            not isinstance(regions, list) or len(regions) > 1 or any(not isinstance(region, str) for region in regions) or
            request.get("cloud") != "SECURE" or request.get("ports") != ["22/tcp"] or
            request.get("startSsh") is not True or request.get("startJupyter") is not False or
            type(request.get("disk")) is not int or request["disk"] <= 0 or
            not isinstance(request.get("image"), str) or not re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", request["image"])):
        raise ValueError("CPU request does not match the supported scope")
    if (not isinstance(offer, dict) or offer.get("id") != cpu["id"] or
            not isinstance(offer.get("price"), dict) or not isinstance(offer.get("vcpu"), dict)):
        raise ValueError("Invalid provider quote")
    price, memory = offer["price"].get("securePerVcpu"), offer.get("ramGbPerVcpu")
    minimum, maximum = offer["vcpu"].get("min"), offer["vcpu"].get("max")
    available = (any(isinstance(dc, dict) and dc.get("id") == regions[0] and dc.get("availability") in {"HIGH", "MEDIUM", "LOW"} for dc in offer.get("dataCenters", []) or [])
                 if regions else offer.get("availability") in {"HIGH", "MEDIUM", "LOW"})
    if (isinstance(price, bool) or not isinstance(price, (int, float)) or not math.isfinite(price) or price <= 0 or
            type(memory) is not int or memory <= 0 or not available or
            type(minimum) is not int or type(maximum) is not int or not minimum <= cpu["vcpuCount"] <= maximum or
            not isinstance(offer.get("dataCenters") or [], list)):
        raise ValueError("Quote lacks a current price, memory or available region")
    try:
        storage = Decimal(str(storage_hourly_usd))
        if not storage.is_finite() or storage < 0:
            raise ValueError("Invalid storage estimate")
        total = Decimal(str(price)) * cpu["vcpuCount"] + storage
    except InvalidOperation as error:
        raise ValueError("Invalid storage estimate") from error
    shape = {"cpu": {**cpu, "memory": memory * cpu["vcpuCount"]}, "cloud": "SECURE",
             "image": request["image"], "disk": request["disk"], "ports": ["22/tcp"]}
    if regions:
        shape["dataCenterId"] = regions[0]
    return {"request_sha256": request_hash(request), "max_hourly_usd": float(total), "no_persistent_mounts": True,
            "expected_shape": shape}


def verify_cpu_quote(client, request, policy, storage_hourly_usd):
    """Recheck current CPU price, region and shape against the armed policy."""
    cpu = request["cpu"]
    current = cpu_resource_policy(request, client.cpu_offer(cpu["id"], cpu["vcpuCount"]), storage_hourly_usd)
    if (current["max_hourly_usd"] > policy.max_hourly_usd or
            current["expected_shape"] != policy.expected_shape or not policy.no_persistent_mounts or
            current["request_sha256"] != policy.request_sha256):
        raise ValueError("Current CPU offer differs from the supervised policy")
    return current


def gpu_resource_policy(request, offer, storage_hourly_usd):
    """Bind a reviewed one-Pod GPU request to its current Secure catalog quote.

    Regional stock is not consistently returned by the GPU catalog endpoint;
    callers with a requested data center must independently validate current
    regional availability before create and the supervisor verifies placement.
    """
    allowed = {"name", "gpu", "cloud", "dataCenterIds", "image", "disk", "ports", "startSsh", "startJupyter"}
    if not isinstance(request, dict) or set(request) != allowed:
        raise ValueError("GPU request has missing or unreviewed fields")
    gpu, regions = request.get("gpu"), request.get("dataCenterIds")
    if (not isinstance(gpu, dict) or set(gpu) - {"id", "count", "minRamPerGpu", "minVcpuCountPerGpu"} or
            not isinstance(gpu.get("id"), str) or not gpu["id"] or
            type(gpu.get("count")) is not int or not 1 <= gpu["count"] <= 64 or
            not isinstance(regions, list) or len(regions) > 1 or
            any(not isinstance(region, str) or not re.fullmatch(r"[A-Z]{2,4}-[A-Z0-9]+-\d+", region) for region in regions) or
            request.get("cloud") != "SECURE" or request.get("ports") != ["22/tcp"] or
            request.get("startSsh") is not True or request.get("startJupyter") is not False or
            type(request.get("disk")) is not int or not 1 <= request["disk"] <= 1000 or
            not isinstance(request.get("image"), str) or not re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", request["image"])):
        raise ValueError("GPU request does not match the supported scope")
    for key in ("minRamPerGpu", "minVcpuCountPerGpu"):
        if key in gpu and (type(gpu[key]) is not int or not 1 <= gpu[key] <= 4096):
            raise ValueError("GPU minimum resource request is invalid")
    if (not isinstance(offer, dict) or offer.get("id") != gpu["id"] or offer.get("secure") is not True or
            not isinstance(offer.get("price"), dict) or not isinstance(offer.get("maxCount"), dict)):
        raise ValueError("Invalid provider GPU quote")
    price, maximum = offer["price"].get("secure"), offer["maxCount"].get("secure")
    if (isinstance(price, bool) or not isinstance(price, (int, float)) or not math.isfinite(price) or price <= 0 or
            type(maximum) is not int or maximum < gpu["count"]):
        raise ValueError("GPU quote lacks a valid Secure price or capacity")
    try:
        storage = Decimal(str(storage_hourly_usd))
        if not storage.is_finite() or storage < 0:
            raise ValueError("Invalid storage estimate")
        total = Decimal(str(price)) * gpu["count"] + storage
    except InvalidOperation as error:
        raise ValueError("Invalid storage estimate") from error
    shape = {"gpu": {key: gpu[key] for key in ("id", "count")}, "cloud": "SECURE",
             "image": request["image"], "disk": request["disk"], "ports": ["22/tcp"]}
    if regions:
        shape["dataCenterId"] = regions[0]
    return {"request_sha256": request_hash(request), "max_hourly_usd": float(total),
            "no_persistent_mounts": True, "expected_shape": shape}


def verify_gpu_quote(client, request, policy, storage_hourly_usd):
    """Recheck a GPU rate and pinned resource shape before the create claim."""
    gpu = request["gpu"]
    current = gpu_resource_policy(request, client.gpu_offer(gpu["id"], gpu["count"]), storage_hourly_usd)
    if (current["max_hourly_usd"] > policy.max_hourly_usd or
            current["expected_shape"] != policy.expected_shape or not policy.no_persistent_mounts or
            current["request_sha256"] != policy.request_sha256):
        raise ValueError("Current GPU offer differs from the supervised policy")
    return current
