"""Offline response helper for the direct upload protocol tests."""

import re
from types import SimpleNamespace


EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def direct_upload_runner(calls=None):
    """A protocol-shaped fake; receiver behavior is exercised separately in Bash."""
    calls = [] if calls is None else calls

    def run(argv, **kwargs):
        command = argv[-1]
        calls.append((argv, kwargs))
        if " OFFSET " in command and "RPE1" in command:
            stdout = f"RPE1 OFFSET 0 {EMPTY_SHA256}\n".encode()
        elif " APPENDED " in command and "RPE1" in command:
            ends = re.findall(r'test "\$actual" -eq ([0-9]+)', command)
            if not ends:
                return SimpleNamespace(stdout=b"", stderr=b"fake offset missing", returncode=1)
            stdout = f"RPE1 APPENDED {ends[-1]}\n".encode()
        elif " PUBLISHED " in command and "RPE1" in command:
            match = re.search(
                r"printf '%s PUBLISHED %s %s\\n' RPE1 ([0-9]+) ([0-9a-f]{64})",
                command)
            if not match:
                return SimpleNamespace(stdout=b"", stderr=b"fake publish missing", returncode=1)
            stdout = f"RPE1 PUBLISHED {match.group(1)} {match.group(2)}\n".encode()
        else:
            stdout = b"RPE1 RESET\n"
        return SimpleNamespace(stdout=stdout, stderr=b"", returncode=0)

    return run
