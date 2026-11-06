"""Launch the supervisor as a separate local process; never creates a Pod."""
import argparse
import os
import subprocess
import sys
from pathlib import Path

from runpod_experiments.attempts import AttemptLedger
from runpod_experiments.runpod import RunPodClient
from runpod_experiments.resource_policy import ResourcePolicy
from runpod_experiments.supervision import CleanupMonitor, run_supervisor


def launch(ledger_path, attempt_id, project_id, directory, deadline_epoch, policy_path, *, popen=subprocess.Popen):
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    argv = [sys.executable, "-m", "runpod_experiments.supervisor_process", "--ledger", str(Path(ledger_path).resolve()),
            "--attempt", attempt_id, "--project", project_id, "--directory", str(directory), "--deadline", str(deadline_epoch),
            "--policy", str(Path(policy_path).resolve())]
    flags = (subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW) if os.name == "nt" else 0
    with (directory / "supervisor-process.log").open("a", encoding="utf-8") as log:
        return popen(argv, stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                     creationflags=flags, start_new_session=os.name != "nt", close_fds=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--attempt", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--deadline", type=float, required=True)
    parser.add_argument("--policy", required=True)
    args = parser.parse_args()
    ledger = AttemptLedger(args.ledger)
    attempt = ledger.get(args.attempt)
    if attempt["project_id"] != args.project:
        raise ValueError("Supervisor project does not match attempt")
    client = RunPodClient.from_config()
    monitor = CleanupMonitor(client, ledger, args.attempt, args.project, ResourcePolicy.read(args.policy))
    run_supervisor(monitor, args.directory, args.deadline)


if __name__ == "__main__":
    main()
