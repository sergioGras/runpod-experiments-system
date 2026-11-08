"""Finite binary subprocess I/O, also used by persistent SSH transports."""
from __future__ import annotations

import math
from pathlib import Path
from queue import Empty, Full, Queue
import subprocess
import threading
import time


def positive_seconds(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError("Timeout must be positive and finite")
    return value


class BinaryProcess:
    def __init__(self, process, *, stderr=None, lifetime_seconds=600,
                 output_chunk_bytes=65536, queued_chunks=32):
        positive_seconds(lifetime_seconds)
        if (type(output_chunk_bytes) is not int or not 1 <= output_chunk_bytes <= 1048576 or
                type(queued_chunks) is not int or not 1 <= queued_chunks <= 1024):
            raise ValueError("Invalid binary stream bounds")
        self.process, self._stderr = process, stderr
        self._closed = threading.Event()
        self._queue = Queue(maxsize=queued_chunks)
        self._buffer = bytearray()
        self._chunk_bytes = output_chunk_bytes
        self._reader_error = None
        self._eof = False
        self._sending = threading.Lock()
        self._writer = None
        self._reader = threading.Thread(target=self._read_stdout, daemon=True, name="runpod-binary-reader")
        self._reader.start()
        self._timer = threading.Timer(lifetime_seconds, self._expire)
        self._timer.daemon = True
        self._timer.start()

    @classmethod
    def spawn(cls, argv, log_path, *, lifetime_seconds, **bounds):
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        log = Path(log_path).open("ab")
        try:
            positive_seconds(lifetime_seconds)
            chunk = bounds.get("output_chunk_bytes", 65536)
            queued = bounds.get("queued_chunks", 32)
            if (set(bounds) - {"output_chunk_bytes", "queued_chunks"} or
                    type(chunk) is not int or not 1 <= chunk <= 1048576 or
                    type(queued) is not int or not 1 <= queued <= 1024):
                raise ValueError("Invalid binary stream bounds")
            process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=log, close_fds=True,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            return cls(process, stderr=log, lifetime_seconds=lifetime_seconds, **bounds)
        except BaseException:
            log.close()
            raise

    def _expire(self):
        self._closed.set()
        if self.process.poll() is None:
            self.process.kill()

    def _read_stdout(self):
        try:
            read = getattr(self.process.stdout, "read1", self.process.stdout.read)
            while not self._closed.is_set():
                chunk = read(self._chunk_bytes)
                if not chunk:
                    break
                while not self._closed.is_set():
                    try:
                        self._queue.put(chunk, timeout=0.1)
                        break
                    except Full:
                        pass
        except BaseException as error:
            self._reader_error = error
        finally:
            while not self._closed.is_set():
                try:
                    self._queue.put(None, timeout=0.1)
                    break
                except Full:
                    pass

    def send(self, data, timeout=60):
        positive_seconds(timeout)
        if not isinstance(data, bytes) or len(data) > 64000000:
            raise TypeError("Binary transport accepts bounded bytes only")
        if self._closed.is_set() or self.process.poll() is not None:
            raise BrokenPipeError("Binary process has exited")
        if not self._sending.acquire(blocking=False):
            raise RuntimeError("Concurrent sends are unsupported")
        done, errors = threading.Event(), []
        def write():
            try:
                view = memoryview(data)
                while view:
                    count = self.process.stdin.write(view[:65536])
                    if not isinstance(count, int) or count <= 0:
                        raise BrokenPipeError("Short input write")
                    view = view[count:]
                self.process.stdin.flush()
            except BaseException as error:
                errors.append(error)
            finally:
                self._sending.release()
                done.set()
        self._writer = threading.Thread(target=write, daemon=True, name="runpod-binary-writer")
        self._writer.start()
        if not done.wait(timeout):
            self.terminate()
            raise TimeoutError("Binary process send timed out")
        if errors:
            raise BrokenPipeError("Binary process input failed") from None

    def receive(self, size, timeout):
        positive_seconds(timeout)
        if type(size) is not int or not 0 <= size <= 64000000:
            raise ValueError("Invalid binary read bound")
        deadline = time.monotonic() + timeout
        while len(self._buffer) < size:
            if self._eof:
                raise EOFError("Binary process closed before requested bytes arrived")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Binary response timed out")
            try:
                chunk = self._queue.get(timeout=min(0.1, remaining))
            except Empty:
                if self._closed.is_set():
                    raise TimeoutError("Binary process lifetime expired or was terminated")
                continue
            if chunk is None:
                self._eof = True
                if self._reader_error is not None:
                    raise RuntimeError("Binary output reader failed") from None
                raise EOFError("Binary process closed before requested bytes arrived")
            self._buffer.extend(chunk)
        result = bytes(self._buffer[:size])
        del self._buffer[:size]
        return result

    def close_stdin(self):
        if self.process.stdin and not self.process.stdin.closed:
            self.process.stdin.close()

    def wait(self, timeout=None):
        result = self.process.wait(timeout=timeout)
        self._timer.cancel()
        return result

    def terminate(self, timeout=5):
        self._closed.set()
        self._timer.cancel()
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=timeout)
        if self._writer:
            self._writer.join(timeout=timeout)
        self._reader.join(timeout=timeout)
        # Do not acquire a BufferedIO lock still held by a blocked thread.
        if not self._writer or not self._writer.is_alive():
            self.close_stdin()
        if not self._reader.is_alive():
            self.process.stdout.close()
        if self._stderr is not None and not self._stderr.closed:
            self._stderr.close()
