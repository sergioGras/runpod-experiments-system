"""Validate a supervisor's pinned resource scope and hourly estimate ceiling."""
import hashlib
import json
import math
from pathlib import Path


class ResourcePolicy:
    def __init__(self, data, fingerprint):
        rate = data.get("max_hourly_usd") if isinstance(data, dict) else None
        shape = data.get("expected_shape") if isinstance(data, dict) else None
        if (isinstance(rate, bool) or not isinstance(rate, (int, float)) or not math.isfinite(rate) or rate <= 0 or
                not isinstance(shape, dict) or not shape):
            raise ValueError("Invalid supervisor resource policy")
        self.max_hourly_usd, self.expected_shape, self.fingerprint = rate, shape, fingerprint

    @classmethod
    def read(cls, path):
        raw = Path(path).read_bytes()
        return cls(json.loads(raw), hashlib.sha256(raw).hexdigest())

    def violation(self, pod):
        rate = pod.get("cost")
        if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not math.isfinite(rate) or rate <= 0 or rate > self.max_hourly_usd:
            return "invalid_or_excess_hourly_rate"
        if any(pod.get(key) != value for key, value in self.expected_shape.items()):
            return "unexpected_resource_shape"
        if pod.get("status") in {"EXITED", "ERROR"}:
            return "provider_failure"
        return None
