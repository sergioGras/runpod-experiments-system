"""Read-only RunPod REST transport. No resource mutation is exposed."""
import json
import re
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

API_URL = "https://api.runpod.io/v2"


class ProviderError(RuntimeError):
    def __init__(self, message, status=None):
        self.status = status
        super().__init__(message)


class RunPodClient:
    def __init__(self, api_key, *, opener=urllib.request.urlopen, timeout=20):
        if not isinstance(api_key, str) or not api_key.strip():
            raise ValueError("RunPod API credential missing")
        self._key = api_key
        self._opener = opener
        self._timeout = timeout

    @classmethod
    def from_config(cls, path=None, **kwargs):
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
                raw = response.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as error:
            raise ProviderError("RunPod request failed", error.code) from None
        except (urllib.error.URLError, TimeoutError, OSError):
            raise ProviderError("RunPod connection failed") from None
        except (ValueError, UnicodeDecodeError):
            raise ProviderError("Invalid RunPod response") from None

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
            raise ProviderError("Pod detail has an unexpected identity")
        return result

    def cpu_offer(self, cpu_id, vcpu_count):
        """Return the provider's current catalog record; callers validate prices."""
        if (not isinstance(cpu_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", cpu_id) or
                type(vcpu_count) is not int or not 1 <= vcpu_count <= 1024):
            raise ValueError("Invalid CPU catalog request")
        query = urllib.parse.urlencode({"include": "AVAILABILITY", "product": "POD", "vcpuCount": vcpu_count})
        result = self._get("/catalog/cpus/" + cpu_id + "?" + query)
        if not isinstance(result, dict):
            raise ProviderError("Invalid CPU catalog response")
        return result

    def pods(self):
        result, cursor, seen = [], None, set()
        while True:
            query = "?" + urllib.parse.urlencode({"cursor": cursor}) if cursor else ""
            page = self._get("/pods" + query)
            if not isinstance(page, dict) or not isinstance(page.get("pods"), list) or any(not isinstance(x, dict) for x in page["pods"]):
                raise ProviderError("Invalid Pod inventory")
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
