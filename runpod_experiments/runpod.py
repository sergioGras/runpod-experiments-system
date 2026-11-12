"""Read-only RunPod REST transport. No resource mutation is exposed."""
import math
import json
import os
import re
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from runpod_experiments.billing import (
    billing_interval_datetimes, billing_period_bounds, billing_query_bounds,
)

API_URL = "https://api.runpod.io/v2"
MAX_INVENTORY_PAGES = 100
MAX_INVENTORY_PODS = 10_000
MAX_BILLING_RECORDS = 10_000
MAX_SSH_PUBLIC_KEYS = 256
MAX_RESPONSE_BYTES = 8_388_608


class ProviderError(RuntimeError):
    def __init__(self, message, status=None, *, kind="provider"):
        self.status = status
        self.kind = kind
        super().__init__(message)


class RunPodClient:
    def __init__(self, api_key, *, opener=urllib.request.urlopen, timeout=20):
        if os.environ.get("RUNPOD_EXPERIMENTS_TEST_MODE") == "1":
            raise ProviderError("Provider access is disabled in test mode")
        if not isinstance(api_key, str) or not api_key.strip():
            raise ValueError("RunPod API credential missing")
        self._key = api_key
        self._opener = opener
        self._timeout = timeout

    @classmethod
    def from_config(cls, path=None, **kwargs):
        if os.environ.get("RUNPOD_EXPERIMENTS_TEST_MODE") == "1":
            raise ProviderError("Provider access is disabled in test mode")
        path = Path(path) if path else Path.home() / ".runpod" / "config.toml"
        config = tomllib.loads(path.read_text(encoding="utf-8"))
        return cls(config.get("apikey"), **kwargs)

    def _get(self, path):
        return self._request("GET", path)

    def _request(self, method, path, body=None):
        payload = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(API_URL + path, data=payload, method=method,
            headers={"Authorization": "Bearer " + self._key, "Content-Type": "application/json"})
        try:
            with self._opener(request, timeout=self._timeout) as response:
                raw = response.read(MAX_RESPONSE_BYTES + 1)
                if len(raw) > MAX_RESPONSE_BYTES:
                    raise ProviderError("RunPod response exceeds the size limit", kind="invalid_response")
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as error:
            raise ProviderError("RunPod request failed", error.code, kind="http") from None
        except (urllib.error.URLError, TimeoutError, OSError):
            raise ProviderError("RunPod connection failed", kind="transport") from None
        except (ValueError, UnicodeDecodeError):
            raise ProviderError("Invalid RunPod response", kind="invalid_response") from None

    def _delete_pod(self, pod_id):
        if not isinstance(pod_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", pod_id):
            raise ValueError("Invalid Pod ID")
        return self._request("DELETE", "/pods/" + pod_id)

    def ssh_key_fingerprints(self):
        """Read registered account keys and return only their SHA-256 digests."""
        from runpod_experiments.ssh import ssh_public_key_fingerprint

        result = self._get("/account/ssh-keys")
        keys = result.get("keys") if isinstance(result, dict) else None
        if not isinstance(keys, list) or len(keys) > MAX_SSH_PUBLIC_KEYS:
            raise ProviderError("Invalid SSH key response", kind="invalid_response")
        try:
            fingerprints = tuple(ssh_public_key_fingerprint(key) for key in keys)
        except ValueError:
            raise ProviderError("Invalid SSH key response", kind="invalid_response") from None
        if len(fingerprints) != len(set(fingerprints)):
            raise ProviderError("SSH key response contains duplicate keys", kind="invalid_response")
        return frozenset(fingerprints)

    def _create_pod(self, request):
        if not isinstance(request, dict):
            raise ValueError("Invalid Pod request")
        return self._request("POST", "/pods", request)

    def pod(self, pod_id):
        if not isinstance(pod_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", pod_id):
            raise ValueError("Invalid Pod ID")
        result = self._get("/pods/" + pod_id)
        if not isinstance(result, dict) or result.get("id") != pod_id:
            raise ProviderError("Pod detail has an unexpected identity", kind="invalid_response")
        return result

    def cpu_offer(self, cpu_id, vcpu_count):
        """Return the provider's current catalog record; callers validate prices."""
        if (not isinstance(cpu_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", cpu_id) or
                type(vcpu_count) is not int or not 1 <= vcpu_count <= 1024):
            raise ValueError("Invalid CPU catalog request")
        query = urllib.parse.urlencode({"include": "AVAILABILITY", "product": "POD", "vcpuCount": vcpu_count})
        result = self._get("/catalog/cpus/" + cpu_id + "?" + query)
        if not isinstance(result, dict):
            raise ProviderError("Invalid CPU catalog response", kind="invalid_response")
        return result

    def gpu_offer(self, gpu_id, gpu_count=1, *, min_cuda_version=None):
        """Return the current GPU catalog quote for the requested GPU count."""
        if (not isinstance(gpu_id, str) or not re.fullmatch(r"[A-Za-z0-9 _.-]{1,128}", gpu_id) or
                type(gpu_count) is not int or not 1 <= gpu_count <= 64 or
                (min_cuda_version is not None and
                 (not isinstance(min_cuda_version, str) or
                  re.fullmatch(r"[0-9]+\.[0-9]+", min_cuda_version) is None))):
            raise ValueError("Invalid GPU catalog request")
        gpu_path = urllib.parse.quote(gpu_id, safe="")
        query = urllib.parse.urlencode({"include": "AVAILABILITY", "product": "POD", "count": gpu_count})
        if min_cuda_version is not None:
            query += "&" + urllib.parse.urlencode({"minCudaVersion": min_cuda_version})
        result = self._get("/catalog/gpus/" + gpu_path + "?" + query)
        if not isinstance(result, dict):
            raise ProviderError("Invalid GPU catalog response", kind="invalid_response")
        return result

    def pods(self, *, max_pages=MAX_INVENTORY_PAGES, max_pods=MAX_INVENTORY_PODS):
        if type(max_pages) is not int or max_pages <= 0 or type(max_pods) is not int or max_pods <= 0:
            raise ValueError("Invalid inventory bounds")
        result, cursor, seen, page_count = [], None, set(), 0
        while True:
            if page_count >= max_pages:
                raise ProviderError("Pod inventory exceeds the page limit", kind="invalid_response")
            page_count += 1
            query = "?" + urllib.parse.urlencode({"cursor": cursor}) if cursor else ""
            page = self._get("/pods" + query)
            if not isinstance(page, dict) or not isinstance(page.get("pods"), list) or any(not isinstance(x, dict) for x in page["pods"]):
                raise ProviderError("Invalid Pod inventory")
            if len(result) + len(page["pods"]) > max_pods:
                raise ProviderError("Pod inventory exceeds the item limit", kind="invalid_response")
            result.extend(page["pods"])
            pagination = page.get("pagination", {})
            if not isinstance(pagination, dict) or type(pagination.get("hasNextPage", False)) is not bool:
                raise ProviderError("Invalid Pod pagination")
            if not pagination.get("hasNextPage", False):
                return result
            cursor = pagination.get("nextCursor")
            if not isinstance(cursor, str) or not cursor or cursor in seen:
                raise ProviderError("Invalid or repeated Pod cursor")
            seen.add(cursor)

    def pod_billing(self, pod_id, *, bucket_size="hour", start_time=None, end_time=None):
        """Read documented REST v2 time-bucketed billing for exactly one Pod.

        Billing records can be absent or change while provider posting is
        delayed. This method only retrieves and validates an observation; it
        does not settle a local budget reservation.
        """
        if not isinstance(pod_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", pod_id):
            raise ValueError("Invalid Pod ID")
        if not isinstance(bucket_size, str) or bucket_size not in {"hour", "day", "week", "month", "year"}:
            raise ValueError("Invalid billing bucket size")
        if start_time is None or end_time is None:
            raise ValueError("Billing start and end times are required")
        try:
            requested_start, requested_end = billing_interval_datetimes(start_time, end_time)
        except ValueError:
            raise ValueError("Billing start and end times must be valid RFC 3339 bounds") from None
        query = {"podId": pod_id, "bucketSize": bucket_size,
                 "startTime": start_time, "endTime": end_time}
        result = self._request("GET", "/billing/pods?" + urllib.parse.urlencode(query))
        if (not isinstance(result, dict) or not isinstance(result.get("records"), list) or
                len(result["records"]) > MAX_BILLING_RECORDS):
            raise ProviderError("Invalid Pod billing response")
        records = result["records"]
        metadata = result.get("metadata")
        query_metadata = metadata.get("query") if isinstance(metadata, dict) else None
        if (not isinstance(query_metadata, dict) or query_metadata.get("podId") != pod_id or
                query_metadata.get("bucketSize") != bucket_size):
            raise ProviderError("Pod billing query metadata has an unexpected identity")
        try:
            effective_start, effective_end = billing_interval_datetimes(
                query_metadata.get("startTime"), query_metadata.get("endTime"))
        except ValueError:
            raise ProviderError("Pod billing query metadata has invalid time bounds") from None
        if effective_start > requested_start or effective_end < requested_end:
            raise ProviderError("Pod billing query metadata does not cover the request")
        if type(metadata.get("recordCount")) is not int or metadata["recordCount"] != len(records):
            raise ProviderError("Pod billing record count is inconsistent")
        parsed_periods = []
        for record in records:
            if not isinstance(record, dict) or record.get("podId") != pod_id:
                raise ProviderError("Pod billing record has an unexpected identity")
            if not _valid_billing_amount(record.get("totalAmount")):
                raise ProviderError("Pod billing record has invalid amount")
            try:
                period_start, period_end = billing_interval_datetimes(
                    record.get("startTime"), record.get("endTime"))
                period = billing_period_bounds(record.get("startTime"), record.get("endTime"))
            except ValueError:
                raise ProviderError("Pod billing record has invalid period bounds") from None
            if period_start < effective_start or period_end > effective_end:
                raise ProviderError("Pod billing record falls outside its query bounds")
            if period_end <= requested_start or period_start >= requested_end:
                raise ProviderError("Pod billing record falls outside the requested interval")
            parsed_periods.append(period)
        ordered_periods = sorted(parsed_periods)
        if any(current[0] < previous[1]
               for previous, current in zip(ordered_periods, ordered_periods[1:])):
            raise ProviderError("Pod billing records contain overlapping periods")
        return result


def _valid_billing_amount(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        return False
    try:
        return math.isfinite(float(value))
    except (OverflowError, ValueError):
        return False
