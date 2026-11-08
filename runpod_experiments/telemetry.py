"""Small durable stage journal with process memory observations; no provider access."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import time


def peak_rss_bytes():
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes
        class Counters(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("faults", wintypes.DWORD)] + [
                (name, ctypes.c_size_t) for name in ("peak", "working", "pool_peak", "pool",
                    "nonpaged_peak", "nonpaged", "pagefile", "pagefile_peak")]
        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.GetCurrentProcess.restype = wintypes.HANDLE
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD]
        if not psapi.GetProcessMemoryInfo(kernel.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
            return None
        return counters.peak
    import resource
    import sys
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


class StageJournal:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def record(self, stage, event, **fields):
        record = {"stage": stage, "event": event, "epoch": time.time(),
                  "peak_rss_bytes": peak_rss_bytes(), **fields}
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, sort_keys=True, allow_nan=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    @contextmanager
    def stage(self, name):
        self.record(name, "start")
        started = time.monotonic()
        try:
            yield
        except BaseException as error:
            self.record(name, "error", error_type=type(error).__name__, seconds=time.monotonic()-started)
            raise
        else:
            self.record(name, "end", seconds=time.monotonic()-started)
