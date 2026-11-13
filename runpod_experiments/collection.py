"""Collect bounded snapshots via a caller-supplied fetch transport."""
import hashlib
import time


def _positive_limit(value, name):
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _validate_limits(names, max_file_bytes, max_total_bytes, per_file_limits):
    _positive_limit(max_file_bytes, "max_file_bytes")
    _positive_limit(max_total_bytes, "max_total_bytes")
    if per_file_limits is None:
        per_file_limits = {}
    if (not isinstance(per_file_limits, dict) or
            any(not isinstance(name, str) or not name for name in per_file_limits) or
            not set(per_file_limits) <= set(names)):
        raise ValueError("per_file_limits must map allowed names to byte limits")
    for value in per_file_limits.values():
        if type(value) is not int or value < 0:
            raise ValueError("per-file limit must be a nonnegative integer")
        if value > max_file_bytes:
            raise ValueError("per-file limit exceeds max_file_bytes")
    if sum(per_file_limits.values()) > max_total_bytes:
        raise ValueError("per-file limits exceed max_total_bytes")
    return per_file_limits


def collect_once(store, fetch, names, attempt_id, previous=None, *, max_file_bytes=10_000_000,
                 max_total_bytes=100_000_000, per_file_limits=None):
    """Fetch named bytes independently; persist changed snapshots and report failures.

    `previous` is an optional name-to-hash map. On restart callers can restore it
    from their manifest; without it unchanged data is safely recorded again.
    """
    if (not isinstance(names, (list, tuple)) or not names or
            any(not isinstance(name, str) or not name for name in names) or
            len(set(names)) != len(names)):
        raise ValueError("names must be a non-empty unique allowlist")
    per_file_limits = _validate_limits(names, max_file_bytes, max_total_bytes,
                                       per_file_limits)
    previous = dict(previous or {})
    errors = {}
    total_bytes = 0
    for name in names:
        try:
            remaining = max_total_bytes - total_bytes
            declared_limit = per_file_limits.get(name, max_file_bytes)
            if remaining < 0 or (remaining == 0 and declared_limit > 0):
                errors[name] = "TransferLimitExceeded"
                continue
            limit = min(max_file_bytes, declared_limit, remaining)
            fetch_limited = getattr(fetch, "fetch_limited", None)
            if callable(fetch_limited) and limit < max_file_bytes:
                data = fetch_limited(name, max(1, limit))
            else:
                data = fetch(name)
            if not isinstance(data, bytes):
                raise ValueError("Fetch did not return bytes")
            if len(data) > limit:
                raise ValueError("Fetched file exceeds its transfer limit")
            total_bytes += len(data)
            digest = hashlib.sha256(data).hexdigest()
            if previous.get(name) != digest:
                store.collect(name, data, attempt_id)
                previous[name] = digest
        except Exception as error:
            # Exception messages can contain remote paths, credentials, or output.
            errors[name] = type(error).__name__
    return previous, errors


def collect_until_stopped(store, fetch, names, attempt_id, stop, *, interval_seconds=30,
                          sleep=time.sleep, max_file_bytes=10_000_000,
                          max_total_bytes=100_000_000, per_file_limits=None):
    """Poll until stop() is true, then make one final collection attempt."""
    if isinstance(interval_seconds, bool) or not isinstance(interval_seconds, (int, float)) or not 0 < interval_seconds < float('inf'):
        raise ValueError("Invalid polling interval")
    previous, errors = {}, {}
    while not stop():
        previous, errors = collect_once(
            store, fetch, names, attempt_id, previous,
            max_file_bytes=max_file_bytes, max_total_bytes=max_total_bytes,
            per_file_limits=per_file_limits)
        sleep(interval_seconds)
    return collect_once(store, fetch, names, attempt_id, previous,
                        max_file_bytes=max_file_bytes, max_total_bytes=max_total_bytes,
                        per_file_limits=per_file_limits)
