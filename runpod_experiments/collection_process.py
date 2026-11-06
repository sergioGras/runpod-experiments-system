"""Run an identity-checked result collector independently of the controller."""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from runpod_experiments.artifacts import ArtifactStore
from runpod_experiments.attempts import AttemptLedger
from runpod_experiments.run_collection import collect_loop
from runpod_experiments.runpod import RunPodClient


def launch(ledger_path, attempt_id, project_id, directory, artifact_directory, names, key_file, known_hosts, remote_root,
           *, popen=subprocess.Popen):
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    if not isinstance(names, list) or not names or any(not isinstance(n, str) or not n for n in names):
        raise ValueError("Expected a non-empty result allowlist")
    names_path = directory / "collection-allowlist.json"
    names_path.write_text(json.dumps(names) + "\n", encoding="utf-8")
    bootstrap = "import sys; sys.path.insert(0, sys.argv.pop(1)); from runpod_experiments.collection_process import main; main()"
    argv = [sys.executable, "-c", bootstrap, str(Path(__file__).resolve().parent.parent),
            "--ledger", str(Path(ledger_path).resolve()), "--attempt", attempt_id, "--project", project_id,
            "--directory", str(directory), "--artifact-directory", str(Path(artifact_directory).resolve()),
            "--names", str(names_path), "--key", str(Path(key_file).resolve()),
            "--known-hosts", str(Path(known_hosts).resolve()), "--remote-root", remote_root]
    flags = (subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW) if os.name == "nt" else 0
    with (directory / "collection-process.log").open("a", encoding="utf-8") as log:
        return popen(argv, stdin=subprocess.DEVNULL, stdout=log, stderr=log, creationflags=flags,
                     start_new_session=os.name != "nt", close_fds=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("ledger", "attempt", "project", "directory", "artifact-directory", "names", "key", "known-hosts", "remote-root"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    names = json.loads(Path(args.names).read_text(encoding="utf-8"))
    if not isinstance(names, list) or not names or any(not isinstance(n, str) or not n for n in names):
        raise ValueError("Invalid collection allowlist")
    ledger = AttemptLedger(args.ledger)
    attempt = ledger.get(args.attempt)
    if attempt["project_id"] != args.project:
        raise ValueError("Collector project does not match attempt")
    collect_loop(RunPodClient.from_config(), ledger, args.attempt, args.project,
                 ArtifactStore(args.artifact_directory), names, args.key,
                 args.known_hosts, args.remote_root, args.directory)


if __name__ == "__main__":
    main()
