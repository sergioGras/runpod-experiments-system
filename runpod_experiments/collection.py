"""Collect bounded snapshots via a caller-supplied fetch transport."""
import hashlib
import time


def collect_once(store, fetch, names, attempt_id, previous=None):
    """Fetch named bytes independently; persist changed snapshots and report failures.

    `previous` is an optional name-to-hash map. On restart callers can restore it
    from their manifest; without it unchanged data is safely recorded again.
    """
    previous = dict(previous or {})
    errors = {}
    for name in names:
        try:
            data = fetch(name)
            if not isinstance(data, bytes):
                raise ValueError("Fetch did not return bytes")
            digest = hashlib.sha256(data).hexdigest()
            if previous.get(name) != digest:
                store.collect(name, data, attempt_id)
                previous[name] = digest
        except Exception as error:
            # Exception messages can contain remote paths, credentials, or output.
            errors[name] = type(error).__name__
    return previous, errors


def collect_until_stopped(store, fetch, names, attempt_id, stop, *, interval_seconds=30, sleep=time.sleep):
    """Poll until stop() is true, then make one final collection attempt."""
    if isinstance(interval_seconds, bool) or not isinstance(interval_seconds, (int, float)) or not 0 < interval_seconds < float('inf'):
        raise ValueError("Invalid polling interval")
    previous, errors = {}, {}
    while not stop():
        previous, errors = collect_once(store, fetch, names, attempt_id, previous)
        sleep(interval_seconds)
    return collect_once(store, fetch, names, attempt_id, previous)
