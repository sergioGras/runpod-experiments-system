"""Finite interactive workload driver contract for a supervised attempt."""
from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Callable

from runpod_experiments.binary_process import positive_seconds
from runpod_experiments.errors import ExecutionError
from runpod_experiments.contracts import FailureCategory


@dataclass(frozen=True, slots=True)
class InteractiveDriver:
    """Consumer-owned protocol callback with explicit stream bounds."""

    run: Callable[["InteractiveSession"], None]
    max_message_bytes: int
    max_io_timeout_seconds: int

    def __post_init__(self) -> None:
        if not callable(self.run):
            raise ValueError("interactive driver must be callable")
        if type(self.max_message_bytes) is not int or not 1 <= self.max_message_bytes <= 64_000_000:
            raise ValueError("interactive message limit is outside the binary transport bound")
        if type(self.max_io_timeout_seconds) is not int or not 1 <= self.max_io_timeout_seconds <= 3600:
            raise ValueError("interactive I/O timeout must be a finite positive integer")


class InteractiveSession:
    """Bounded send/receive view over a binary process owned by the engine."""

    def __init__(self, process, *, check: Callable[[], None], deadline: float,
                 max_message_bytes: int, max_io_timeout_seconds: int,
                 monotonic: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep):
        if (not callable(getattr(process, "send", None)) or
                not callable(getattr(process, "receive", None)) or
                not callable(getattr(process, "poll", None)) or
                not callable(check) or not callable(monotonic) or not callable(sleep)):
            raise TypeError("interactive process and monitor are invalid")
        self._process = process
        self._check_callback = check
        self._deadline = deadline
        self._max_message_bytes = max_message_bytes
        self._max_io_timeout_seconds = max_io_timeout_seconds
        self._monotonic = monotonic
        self._sleep = sleep

    def _check(self) -> None:
        self._check_callback()
        if self._monotonic() >= self._deadline:
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "attempt_deadline_reached")

    def _timeout(self, value: int | float | None) -> float:
        timeout = (self._max_io_timeout_seconds if value is None
                   else positive_seconds(value))
        if timeout > self._max_io_timeout_seconds:
            raise ValueError("interactive I/O timeout exceeds its declared bound")
        remaining = self._deadline - self._monotonic()
        if remaining <= 0:
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "attempt_deadline_reached")
        return min(float(timeout), remaining)

    def send(self, data: bytes, *, timeout: int | float | None = None) -> None:
        self._check()
        if not isinstance(data, bytes) or len(data) > self._max_message_bytes:
            raise ValueError("interactive message exceeds its declared byte limit")
        if self._process.poll() is not None:
            raise BrokenPipeError("Interactive workload process has exited")
        self._process.send(data, timeout=self._timeout(timeout))
        self._check()

    def receive(self, size: int, *, timeout: int | float | None = None) -> bytes:
        self._check()
        if type(size) is not int or not 0 <= size <= self._max_message_bytes:
            raise ValueError("interactive read exceeds its declared byte limit")
        deadline = min(self._deadline, self._monotonic() + self._timeout(timeout))
        while True:
            self._check()
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                raise TimeoutError("Interactive response timed out")
            try:
                result = self._process.receive(size, timeout=min(0.25, remaining))
                if not isinstance(result, bytes) or len(result) != size:
                    raise EOFError("Interactive process returned an incomplete frame")
                self._check()
                return result
            except TimeoutError:
                if self._process.poll() is not None:
                    raise
                remaining = deadline - self._monotonic()
                if remaining > 0:
                    self._sleep(min(0.25, remaining))

    def close_stdin(self) -> None:
        self._check()
        close = getattr(self._process, "close_stdin", None)
        if not callable(close):
            raise TypeError("interactive process cannot close stdin")
        close()
        self._check_callback()

    def poll(self) -> int | None:
        self._check_callback()
        if self._monotonic() >= self._deadline:
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "attempt_deadline_reached")
        return self._process.poll()

    def wait(self, timeout: int | float | None = None) -> int:
        requested = self._deadline - self._monotonic() if timeout is None else positive_seconds(timeout)
        if requested <= 0:
            raise TimeoutError("Interactive workload deadline elapsed")
        if timeout is not None and requested > self._max_io_timeout_seconds:
            raise ValueError("interactive wait exceeds its declared I/O timeout")
        deadline = min(self._deadline, self._monotonic() + requested)
        while True:
            self._check_callback()
            code = self._process.poll()
            if code is not None:
                return code
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                raise TimeoutError("Interactive workload did not exit before its deadline")
            self._sleep(min(0.25, remaining))
