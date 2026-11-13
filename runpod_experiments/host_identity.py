"""Host boot identity helpers used to keep supervisor deadlines nonrenewable."""
from __future__ import annotations

import ctypes
from contextlib import contextmanager
import math
import os
from pathlib import Path
import re
import sys
import time
from typing import Any, Mapping


_LINUX_BOOT_ID = Path("/proc/sys/kernel/random/boot_id")
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")


def _windows_uptime_ms() -> int:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_tick_count64 = kernel32.GetTickCount64
    get_tick_count64.argtypes = []
    get_tick_count64.restype = ctypes.c_ulonglong
    return int(get_tick_count64())


def current_boot_identity(*, clock=time.time, uptime_ms=None, platform_name=None,
                          linux_boot_id_path: str | Path = _LINUX_BOOT_ID) -> dict[str, Any] | None:
    """Return a stable boot token on supported hosts, otherwise ``None``.

    Linux exposes a kernel-generated boot UUID. Windows does not expose an
    equivalent through Python's standard library, so use the wall-clock boot
    time estimated from GetTickCount64. The estimate is compared with a small
    tolerance; a clock adjustment therefore fails closed instead of renewing
    a supervisor's execution window.
    """
    selected_platform = os.name if platform_name is None else platform_name
    if selected_platform == "nt":
        elapsed_ms = _windows_uptime_ms() if uptime_ms is None else uptime_ms
        if isinstance(elapsed_ms, bool) or not isinstance(elapsed_ms, int) or elapsed_ms < 0:
            return None
        try:
            boot_epoch = float(clock()) - elapsed_ms / 1000.0
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(boot_epoch):
            return None
        return {"kind": "windows_boot_time_estimate", "value": boot_epoch}

    if selected_platform == "linux" or (
            selected_platform == "posix" and sys.platform.startswith("linux")):
        try:
            token = Path(linux_boot_id_path).read_text(encoding="ascii").strip().lower()
        except (OSError, UnicodeError):
            return None
        if _UUID.fullmatch(token) is None:
            return None
        return {"kind": "linux_boot_uuid", "value": token}
    return None


def valid_boot_identity(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    if value.get("kind") == "linux_boot_uuid":
        token = value.get("value")
        return isinstance(token, str) and _UUID.fullmatch(token) is not None
    if value.get("kind") == "windows_boot_time_estimate":
        boot_epoch = value.get("value")
        return (isinstance(boot_epoch, (int, float)) and not isinstance(boot_epoch, bool)
                and math.isfinite(boot_epoch))
    return False


def same_boot_identity(left: Any, right: Any) -> bool:
    if not valid_boot_identity(left) or not valid_boot_identity(right):
        return False
    if left["kind"] != right["kind"]:
        return False
    if left["kind"] == "linux_boot_uuid":
        return left["value"] == right["value"]
    # The Windows token is derived from two system clocks. Small sampling and
    # clock-discipline differences are tolerated; larger changes fail closed.
    return abs(float(left["value"]) - float(right["value"])) <= 5.0


@contextmanager
def supervisor_process_lock(directory: str | Path):
    """Hold one OS-released lock for the lifetime of a supervisor process."""
    root = Path(directory)
    if root.is_symlink():
        raise RuntimeError("supervisor_directory_symlink")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock_path = root / "supervisor.lock"
    if lock_path.is_symlink():
        raise RuntimeError("supervisor_lock_symlink")
    flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError:
        raise RuntimeError("supervisor_lock_unavailable") from None
    stream = os.fdopen(descriptor, "r+b", buffering=0)
    try:
        if os.name == "nt":
            import msvcrt

            stream.seek(0, os.SEEK_END)
            if stream.tell() == 0:
                stream.write(b"\0")
                stream.flush()
            stream.seek(0)
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            unlock = lambda: msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            unlock = lambda: fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
    except OSError:
        stream.close()
        raise RuntimeError("supervisor_already_running") from None
    try:
        yield
    finally:
        try:
            unlock()
        finally:
            stream.close()
