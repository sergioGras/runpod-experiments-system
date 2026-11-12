"""Read-only RunPod REST transport. No resource mutation is exposed."""
import math
import json
import os
import re
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

from runpod_experiments.billing import billing_period_bounds

API_URL = "https://api.runpod.io/v2"
BILLING_API_URL = "https://rest.runpod.io/v1"
MAX_INVENTORY_PAGES = 100
MAX_INVENTORY_PODS = 10_000
MAX_BILLING_RECORDS = 10_000
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

    def _request(self, method, path, body=None, *, base_url=API_URL):
        payload = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(base_url + path, data=payload, method=method,
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

    def gpu_offer(self, gpu_id, gpu_count=1):
        """Return the current GPU catalog quote for the requested GPU count."""
        if (not isinstance(gpu_id, str) or not re.fullmatch(r"[A-Za-z0-9 _.-]{1,128}", gpu_id) or
                type(gpu_count) is not int or not 1 <= gpu_count <= 64):
            raise ValueError("Invalid GPU catalog request")
        gpu_path = urllib.parse.quote(gpu_id, safe="")
        query = urllib.parse.urlencode({"include": "AVAILABILITY", "product": "POD", "count": gpu_count})
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
        """Read documented REST v1 time-bucketed billing for exactly one Pod.

        Billing records can be absent or change while provider posting is
        delayed. This method only retrieves and validates an observation; it
        does not settle a local budget reservation.
        """
        if not isinstance(pod_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", pod_id):
            raise ValueError("Invalid Pod ID")
        if not isinstance(bucket_size, str) or bucket_size not in {"hour", "day", "week", "month", "year"}:
            raise ValueError("Invalid billing bucket size")
        if (start_time is None) != (end_time is None):
            raise ValueError("Billing start and end times must be provided together")
        query = {"podId": pod_id, "grouping": "podId", "bucketSize": bucket_size}
        if start_time is not None:
            parsed_bounds = []
            for name, value in (("startTime", start_time), ("endTime", end_time)):
                if not isinstance(value, str) or not value:
                    raise ValueError("Billing times must be RFC 3339 strings")
                try:
                    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                except ValueError:
                    raise ValueError("Billing times must be RFC 3339 strings") from None
                if parsed.tzinfo is None or parsed.utcoffset() is None:
                    raise ValueError("Billing times must include a timezone")
                parsed_bounds.append(parsed)
                query[name] = value
            if parsed_bounds[1] <= parsed_bounds[0]:
                raise ValueError("Billing end time must be after start time")
        result = self._request("GET", "/billing/pods?" + urllib.parse.urlencode(query),
                               base_url=BILLING_API_URL)
        if not isinstance(result, list) or len(result) > MAX_BILLING_RECORDS:
            raise ProviderError("Invalid Pod billing response")
        parsed_periods = []
        for record in result:
            if not isinstance(record, dict) or record.get("podId") != pod_id:
                raise ProviderError("Pod billing record has an unexpected identity")
            value = record.get("time")
            if not isinstance(value, str) or not value:
                raise ProviderError("Pod billing record has invalid period start")
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                raise ProviderError("Pod billing record has invalid period start") from None
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                raise ProviderError("Pod billing record has invalid period start")
            if not _valid_billing_amount(record.get("amount")):
                raise ProviderError("Pod billing record has invalid amount")
            # The official API specifies the bucket start. The normalizer derives
            # the bucket end from this request's bucketSize.
            try:
                parsed_periods.append(billing_period_bounds(value, bucket_size))
            except ValueError:
                raise ProviderError("Pod billing record has invalid period") from None
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
