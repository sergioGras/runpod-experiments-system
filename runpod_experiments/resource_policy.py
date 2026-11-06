"""Validate a supervisor's pinned resource scope and hourly estimate ceiling."""
import hashlib
import json
import math
import re
from pathlib import Path


def request_hash(request):
    raw = json.dumps(request, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


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
        if pod.get("status") == "RUNNING" and any(pod.get(key) != value for key, value in self.expected_shape.items()):
            return "unexpected_resource_shape"
        if self.no_persistent_mounts and pod.get("status") == "RUNNING":
            mounts = pod.get("mounts")
            if not isinstance(mounts, dict) or any(mounts.values()):
                return "unexpected_or_unknown_mounts"
        return None
