"""Bounded direct-SSH result download with explicit host-key verification."""
import re
import shlex
import subprocess
from pathlib import Path, PurePosixPath


class SSHFetcher:
    def __init__(self, host, port, username, key_file, known_hosts, remote_root, *, max_bytes=10_000_000, timeout=30, runner=subprocess.run):
        if (not isinstance(host, str) or not re.fullmatch(r"[A-Za-z0-9.:-]+", host) or host.startswith("-") or
                type(port) is not int or not 1 <= port <= 65535 or
                not isinstance(username, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", username) or
                not isinstance(remote_root, str) or not remote_root.startswith("/") or
                "\x00" in remote_root or ".." in PurePosixPath(remote_root).parts or
                type(max_bytes) is not int or max_bytes <= 0):
            raise ValueError("Invalid SSH destination or collection limit")
        if not Path(key_file).is_file() or not Path(known_hosts).is_file():
            raise ValueError("SSH identity and known-hosts files must exist")
        self.host, self.port, self.username = host, port, username
        self.key_file, self.known_hosts, self.remote_root = str(key_file), str(known_hosts), remote_root
        self.max_bytes, self.timeout, self.runner = max_bytes, timeout, runner

    def __call__(self, name):
        if not isinstance(name, str) or not name or "\\" in name or "\x00" in name:
            raise ValueError("Invalid artifact name")
        relative = PurePosixPath(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Artifact must be inside the remote root")
        remote = str(PurePosixPath(self.remote_root) / relative)
        command = "head -c " + str(self.max_bytes + 1) + " -- " + shlex.quote(remote)
        argv = ["ssh", "-T", "-i", self.key_file, "-p", str(self.port), "-o", "BatchMode=yes",
                "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes", "-o",
                "UserKnownHostsFile=" + self.known_hosts, self.username + "@" + self.host, command]
        try:
            result = self.runner(argv, capture_output=True, timeout=self.timeout, check=True)
        except (subprocess.SubprocessError, OSError):
            raise RuntimeError("SSH collection failed") from None
        if len(result.stdout) > self.max_bytes:
            raise ValueError("Artifact exceeds collection size limit")
        return result.stdout
