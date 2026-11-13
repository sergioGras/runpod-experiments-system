"""Launch the supervisor as a separate local process; never creates a Pod."""
import argparse
import os
import subprocess
import sys
import ctypes
from pathlib import Path

from runpod_experiments.attempts import AttemptLedger
from runpod_experiments.contracts import ResourceKind
from runpod_experiments.execution_store import ExecutionStore
from runpod_experiments.runpod import RunPodClient
from runpod_experiments.resource_policy import ResourcePolicy
from runpod_experiments.state_config import StateStoreConfig
from runpod_experiments.supervision import (
    CleanupMonitor, StoreCleanupMonitor, run_supervisor,
)


def _launch(argv, directory, *, popen):
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    flags = (subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW) if os.name == "nt" else 0
    with (directory / "supervisor-process.log").open("a", encoding="utf-8") as log:
        return popen(argv, stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                     creationflags=flags, start_new_session=os.name != "nt", close_fds=True)


def _bootstrap_argv(arguments):
    package_parent = str(Path(__file__).resolve().parent.parent)
    # On Windows, a venv's python.exe may be a launcher whose PID differs from
    # the actual detached interpreter that writes supervisor.json. Launch the
    # base interpreter directly so Popen.pid remains the identity checked by
    # the create controller. The supervisor imports only stdlib plus this
    # explicitly added shared package path.
    interpreter = Path(getattr(sys, "_base_executable", sys.executable))
    if not interpreter.is_file():
        interpreter = Path(sys.executable)
    bootstrap = "import sys; sys.path.insert(0, sys.argv.pop(1)); from runpod_experiments.supervisor_process import main; main()"
    return [str(interpreter), "-c", bootstrap, package_parent, *arguments]


def launch(ledger_path, attempt_id, project_id, directory, deadline_epoch, policy_path, *, popen=subprocess.Popen):
    """Launch the legacy ledger supervisor without changing its process contract."""
    directory = Path(directory).resolve()
    arguments = ["--ledger", str(Path(ledger_path).resolve()),
        "--attempt", attempt_id, "--project", project_id, "--directory", str(directory),
        "--deadline", str(deadline_epoch), "--policy", str(Path(policy_path).resolve())]
    return _launch(_bootstrap_argv(arguments), directory, popen=popen)


def launch_execution_store(state_config, attempt_id, project_id, directory, *,
                           gpu_ssh_key_file=None, gpu_known_hosts=None,
                           popen=subprocess.Popen):
    """Launch a supervisor for an existing, hash-verified execution-store attempt."""
    if (gpu_ssh_key_file is None) != (gpu_known_hosts is None):
        raise ValueError("GPU probe key and known-hosts paths must be supplied together")
    if not isinstance(state_config, StateStoreConfig):
        raise TypeError("state_config must be a StateStoreConfig")
    path = state_config.database_path(project_id)
    if not path.is_file():
        raise FileNotFoundError("Configured execution store does not exist")
    store = ExecutionStore(str(path), project_id=project_id,
                           controller_host=state_config.controller_host)
    store.verify_attempt_lifecycle_projection(attempt_id)
    store.project_policy_for_attempt(attempt_id)
    profile, _region = store.selected_resource_for_attempt(attempt_id)
    if gpu_ssh_key_file is not None and profile.kind is not ResourceKind.GPU:
        raise ValueError("GPU probe credentials require a GPU attempt")
    attempt = store.get_attempt(attempt_id)
    if attempt["state"] in ("cancelled", "terminated"):
        raise ValueError("Cannot launch supervision for a terminal attempt")
    directory = Path(directory).absolute()
    if gpu_ssh_key_file is not None:
        expected_directory = (state_config.state_root / project_id / attempt_id / "supervisor")
        if (any(path.is_symlink() for path in (
                state_config.state_root, expected_directory.parent.parent,
                expected_directory.parent, expected_directory)) or
                directory != expected_directory.absolute()):
            raise ValueError("GPU supervisor directory does not match the attempt state path")
    directory = directory.resolve()
    arguments = ["--store-root", str(state_config.state_root),
        "--controller-host", state_config.controller_host, "--attempt", attempt_id,
        "--project", project_id, "--directory", str(directory),
        "--deadline", str(attempt["deadline_epoch"])]
    if gpu_ssh_key_file is not None:
        arguments.extend(["--gpu-ssh-key-file", os.path.abspath(os.fspath(gpu_ssh_key_file)),
                          "--gpu-known-hosts", os.path.abspath(os.fspath(gpu_known_hosts))])
    return _launch(_bootstrap_argv(arguments), directory, popen=popen)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger")
    parser.add_argument("--store-root")
    parser.add_argument("--attempt", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--deadline", type=float, required=True)
    parser.add_argument("--policy")
    parser.add_argument("--controller-host")
    parser.add_argument("--gpu-ssh-key-file")
    parser.add_argument("--gpu-known-hosts")
    args = parser.parse_args()
    if (args.gpu_ssh_key_file is None) != (args.gpu_known_hosts is None):
        parser.error("GPU probe requires both --gpu-ssh-key-file and --gpu-known-hosts")
    if (args.ledger is None) == (args.store_root is None):
        parser.error("select exactly one of --ledger or --store-root")
    if args.ledger is not None:
        if args.gpu_ssh_key_file is not None:
            parser.error("GPU probe credentials are only valid in --store-root mode")
        if args.policy is None or args.controller_host is not None:
            parser.error("legacy --ledger mode requires --policy and does not accept --controller-host")
        ledger = AttemptLedger(args.ledger)
        attempt = ledger.get(args.attempt)
        if attempt["project_id"] != args.project:
            raise ValueError("Supervisor project does not match attempt")
        monitor_factory = lambda client: CleanupMonitor(
            client, ledger, args.attempt, args.project, ResourcePolicy.read(args.policy))
    else:
        if args.controller_host is None or args.policy is not None:
            parser.error("--store-root mode requires --controller-host and does not accept --policy")
        state_config = StateStoreConfig(Path(args.store_root), args.controller_host)
        store_path = state_config.database_path(args.project)
        if not store_path.is_file():
            raise FileNotFoundError("Configured execution store does not exist")
        store = ExecutionStore(str(store_path), project_id=args.project,
                               controller_host=state_config.controller_host)
        store.verify_attempt_lifecycle_projection(args.attempt)
        store.project_policy_for_attempt(args.attempt)
        profile, _region = store.selected_resource_for_attempt(args.attempt)
        attempt = store.get_attempt(args.attempt)
        if attempt["deadline_epoch"] != args.deadline:
            raise ValueError("Supervisor deadline does not match stored attempt")
        if args.gpu_ssh_key_file is not None and profile.kind is not ResourceKind.GPU:
            parser.error("GPU probe credentials require a GPU attempt")
        if args.gpu_ssh_key_file is not None:
            expected_directory = (state_config.state_root / args.project /
                                  args.attempt / "supervisor").resolve(strict=False)
            if Path(args.directory).resolve(strict=False) != expected_directory:
                raise ValueError("GPU supervisor directory does not match the attempt state path")
        monitor_factory = lambda client: StoreCleanupMonitor(
            client, store, args.attempt, args.project,
            gpu_ssh_key_file=args.gpu_ssh_key_file,
            gpu_known_hosts=args.gpu_known_hosts,
            supervisor_directory=args.directory)
    client = RunPodClient.from_config()
    monitor = monitor_factory(client)
    if os.name == "nt":
        # Prevent automatic idle sleep while the independently supervised Pod exists.
        if not ctypes.windll.kernel32.SetThreadExecutionState(0x80000001):
            raise RuntimeError("Could not prevent automatic idle sleep")
    try:
        run_supervisor(monitor, args.directory, args.deadline)
    finally:
        if os.name == "nt":
            ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)


if __name__ == "__main__":
    main()
