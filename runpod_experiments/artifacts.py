"""Incrementally preserve collected bytes in a local private directory."""
import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath


class ArtifactStore:
    def __init__(self, directory):
        self.directory = Path(directory)
        (self.directory / "objects").mkdir(parents=True, exist_ok=True)

    def collect(self, name, data, attempt_id):
        path = PurePosixPath(name)
        if (not isinstance(name, str) or not name or path.is_absolute() or
                any(part in (".", "..") for part in path.parts) or "\\" in name or
                not isinstance(attempt_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", attempt_id) or
                not isinstance(data, bytes)):
            raise ValueError("Invalid artifact name, attempt ID or bytes")
        digest = hashlib.sha256(data).hexdigest()
        target = self.directory / "objects" / digest
        if target.exists() and hashlib.sha256(target.read_bytes()).hexdigest() != digest:
            raise ValueError("Existing artifact object failed integrity verification")
        if not target.exists():
            fd, temp = tempfile.mkstemp(dir=target.parent, prefix=".collect-")
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temp, target)
            finally:
                if os.path.exists(temp):
                    os.unlink(temp)
        entry = {"name": name, "attempt_id": attempt_id, "sha256": digest, "bytes": len(data),
                 "collected_utc": datetime.now(timezone.utc).isoformat()}
        with (self.directory / "manifest.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(entry, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        return entry
