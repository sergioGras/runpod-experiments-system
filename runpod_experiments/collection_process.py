"""Run an identity-checked result collector independently of the controller."""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
import uuid

from runpod_experiments.artifacts import ArtifactStore
from runpod_experiments.attempts import AttemptLedger
from runpod_experiments.execution_store import ExecutionStore
from runpod_experiments.run_collection import collect_loop
from runpod_experiments.runpod import RunPodClient
from runpod_experiments.provider_factory import provider_client_for_attempt
from runpod_experiments.state_config import StateStoreConfig
from runpod_experiments.transfer import (
    DEFAULT_COLLECTION_TIMEOUT_SECONDS, DEFAULT_TRANSFER_CHUNK_BYTES,
    validate_chunk_bytes, validate_timeout_seconds,
)

COLLECTION_LIMITS_SCHEMA_VERSION = 1


def launch(ledger_path, attempt_id, project_id, directory, artifact_directory, names, key_file, known_hosts, remote_root,
           *, max_file_bytes=10_000_000, max_total_bytes=100_000_000,
           per_file_limits=None,
           timeout_seconds=DEFAULT_COLLECTION_TIMEOUT_SECONDS,
           chunk_bytes=DEFAULT_TRANSFER_CHUNK_BYTES, popen=subprocess.Popen):
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    if not isinstance(names, list) or not names or any(not isinstance(n, str) or not n for n in names):
        raise ValueError("Expected a non-empty result allowlist")
    names_path = directory / "collection-allowlist.json"
    names_path.write_text(json.dumps(names) + "\n", encoding="utf-8")
    limits = _validate_transfer_limits(names, max_file_bytes, max_total_bytes,
                                       per_file_limits, timeout_seconds, chunk_bytes)
    limits_path = directory / "collection-limits.json"
    limits_path.write_text(json.dumps(limits, sort_keys=True, separators=(",", ":")) + "\n",
                           encoding="utf-8")
    bootstrap = ("import json,sys; roots=json.loads(sys.argv.pop(1)); "
                 "package=sys.argv.pop(1); sys.path[:0]=[package]+roots; "
                 "from runpod_experiments.collection_process import main; main()")
    argv = [sys.executable, "-c", bootstrap, "[]",
            str(Path(__file__).resolve().parent.parent),
            "--ledger", str(Path(ledger_path).resolve()), "--attempt", attempt_id, "--project", project_id,
            "--directory", str(directory), "--artifact-directory", str(Path(artifact_directory).resolve()),
            "--names", str(names_path), "--key", str(Path(key_file).resolve()),
            "--known-hosts", str(Path(known_hosts).resolve()), "--remote-root", remote_root,
            "--max-file-bytes", str(max_file_bytes), "--max-total-bytes", str(max_total_bytes),
            "--timeout-seconds", str(timeout_seconds), "--chunk-bytes", str(chunk_bytes),
            "--limits", str(limits_path)]
    flags = (subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW) if os.name == "nt" else 0
    with (directory / "collection-process.log").open("a", encoding="utf-8") as log:
        return popen(argv, stdin=subprocess.DEVNULL, stdout=log, stderr=log, creationflags=flags,
                     start_new_session=os.name != "nt", close_fds=True)


def launch_execution_store(state_config, attempt_id, project_id, artifact_directory, names,
                           key_file, known_hosts, remote_root, *, ssh_route="direct",
                           max_file_bytes=10_000_000, max_total_bytes=100_000_000,
                           per_file_limits=None,
                           timeout_seconds=DEFAULT_COLLECTION_TIMEOUT_SECONDS,
                           chunk_bytes=DEFAULT_TRANSFER_CHUNK_BYTES, popen=subprocess.Popen):
    """Launch an independent collector bound to one active execution-store attempt."""
    if not isinstance(state_config, StateStoreConfig):
        raise TypeError("state_config must be a StateStoreConfig")
    if ssh_route not in ("direct", "proxy"):
        raise ValueError("ssh_route must be direct or proxy")
    if not isinstance(names, list) or not names or any(not isinstance(n, str) or not n for n in names):
        raise ValueError("Expected a non-empty result allowlist")
    limits = _validate_transfer_limits(names, max_file_bytes, max_total_bytes,
                                       per_file_limits, timeout_seconds, chunk_bytes)
    database_path = state_config.database_path(project_id)
    if not database_path.is_file():
        raise FileNotFoundError("Configured execution store does not exist")
    store = ExecutionStore(str(database_path), project_id=project_id,
                           controller_host=state_config.controller_host)
    store.verify_attempt_lifecycle_projection(attempt_id)
    policy = store.project_policy_for_attempt(attempt_id)
    attempt = store.get_attempt(attempt_id)
    if attempt["state"] != "active" or not attempt["pod_id"]:
        raise ValueError("Execution-store collector requires an active bound attempt")

    project_directory = state_config.state_root / project_id
    attempt_directory = project_directory / attempt_id
    directory = attempt_directory / "collector"
    for path in (project_directory, attempt_directory, directory):
        if path.is_symlink():
            raise ValueError("Collector state path cannot contain symlinks")
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    resolved_root = state_config.state_root.resolve(strict=True)
    resolved_directory = directory.resolve(strict=True)
    if resolved_directory == resolved_root or resolved_root not in resolved_directory.parents:
        raise ValueError("Collector state path escaped the configured root")

    names_path = directory / "collection-allowlist.json"
    limits_path = directory / "collection-limits.json"
    if names_path.is_symlink():
        raise ValueError("Collector allowlist cannot be a symlink")
    encoded_names = json.dumps(names, separators=(",", ":")) + "\n"
    if names_path.exists():
        if names_path.read_text(encoding="utf-8") != encoded_names:
            raise ValueError("Collector allowlist differs from the existing attempt")
    else:
        temporary = names_path.with_name(names_path.name + ".tmp-" + uuid.uuid4().hex)
        with temporary.open("x", encoding="utf-8") as stream:
            stream.write(encoded_names)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, names_path)

    encoded_limits = json.dumps(limits, sort_keys=True, separators=(",", ":")) + "\n"
    if limits_path.is_symlink():
        raise ValueError("Collector limits cannot be a symlink")
    if limits_path.exists():
        if limits_path.read_text(encoding="utf-8") != encoded_limits:
            raise ValueError("Collector limits differ from the existing attempt")
    else:
        temporary = limits_path.with_name(limits_path.name + ".tmp-" + uuid.uuid4().hex)
        with temporary.open("x", encoding="utf-8") as stream:
            stream.write(encoded_limits)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, limits_path)

    bootstrap = ("import json,sys; roots=json.loads(sys.argv.pop(1)); "
                 "package=sys.argv.pop(1); sys.path[:0]=[package]+roots; "
                 "from runpod_experiments.collection_process import main; main()")
    argv = [sys.executable, "-c", bootstrap,
            json.dumps(list(policy.provider_factory_module_roots)),
            str(Path(__file__).resolve().parent.parent),
            "--store-root", str(state_config.state_root),
            "--controller-host", state_config.controller_host,
            "--attempt", attempt_id, "--project", project_id,
            "--directory", str(directory), "--artifact-directory", str(Path(artifact_directory).resolve()),
            "--names", str(names_path), "--key", str(Path(key_file).resolve()),
            "--known-hosts", str(Path(known_hosts).resolve()), "--remote-root", remote_root,
            "--ssh-route", ssh_route, "--max-file-bytes", str(max_file_bytes),
            "--max-total-bytes", str(max_total_bytes),
            "--timeout-seconds", str(timeout_seconds), "--chunk-bytes", str(chunk_bytes),
            "--limits", str(limits_path)]
    flags = (subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW) if os.name == "nt" else 0
    with (directory / "collection-process.log").open("a", encoding="utf-8") as log:
        return popen(argv, stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                     creationflags=flags, start_new_session=os.name != "nt", close_fds=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("attempt", "project", "directory", "artifact-directory", "names", "key", "known-hosts", "remote-root"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--ledger")
    parser.add_argument("--store-root")
    parser.add_argument("--controller-host")
    parser.add_argument("--ssh-route", choices=("direct", "proxy"), default="direct")
    parser.add_argument("--max-file-bytes", type=int, default=10_000_000)
    parser.add_argument("--max-total-bytes", type=int, default=100_000_000)
    parser.add_argument("--timeout-seconds", type=int,
                        default=DEFAULT_COLLECTION_TIMEOUT_SECONDS)
    parser.add_argument("--chunk-bytes", type=int, default=DEFAULT_TRANSFER_CHUNK_BYTES)
    parser.add_argument("--limits")
    args = parser.parse_args()
    if (args.ledger is None) == (args.store_root is None):
        parser.error("select exactly one of --ledger or --store-root")
    if (args.ledger is not None and args.controller_host is not None) or (
            args.store_root is not None and args.controller_host is None):
        parser.error("--ledger excludes --controller-host; --store-root requires it")
    names = json.loads(Path(args.names).read_text(encoding="utf-8"))
    if not isinstance(names, list) or not names or any(not isinstance(n, str) or not n for n in names):
        raise ValueError("Invalid collection allowlist")
    saved_limits = (json.loads(Path(args.limits).read_text(encoding="utf-8"))
                    if args.limits else None)
    if saved_limits is not None:
        legacy_keys = {"max_file_bytes", "max_total_bytes", "per_file_limits"}
        versioned_keys = legacy_keys | {
            "schema_version", "collection_timeout_seconds", "collection_chunk_bytes"}
        if not isinstance(saved_limits, dict):
            raise ValueError("Collector limit file has an unsupported schema")
        keys = set(saved_limits)
        legacy_schema = keys == legacy_keys
        current_schema = (keys == versioned_keys and
                          type(saved_limits.get("schema_version")) is int and
                          saved_limits["schema_version"] == COLLECTION_LIMITS_SCHEMA_VERSION)
        if (not legacy_schema and not current_schema):
            raise ValueError("Collector limit file has an unsupported schema")
        if (saved_limits.get("max_file_bytes") != args.max_file_bytes or
                saved_limits.get("max_total_bytes") != args.max_total_bytes or
                saved_limits.get("collection_timeout_seconds",
                                 DEFAULT_COLLECTION_TIMEOUT_SECONDS) != args.timeout_seconds or
                saved_limits.get("collection_chunk_bytes",
                                 DEFAULT_TRANSFER_CHUNK_BYTES) != args.chunk_bytes):
            raise ValueError("Collector limit file does not match its command line")
        saved_limits = saved_limits["per_file_limits"]
    limits = _validate_transfer_limits(
        names, args.max_file_bytes, args.max_total_bytes, saved_limits,
        args.timeout_seconds, args.chunk_bytes)
    if args.ledger is not None:
        ledger = AttemptLedger(args.ledger)
        attempt = ledger.get(args.attempt)
        if attempt["project_id"] != args.project:
            raise ValueError("Collector project does not match attempt")
    else:
        state_config = StateStoreConfig(Path(args.store_root), args.controller_host)
        database_path = state_config.database_path(args.project)
        if not database_path.is_file():
            raise FileNotFoundError("Configured execution store does not exist")
        ledger = ExecutionStore(str(database_path), project_id=args.project,
                                controller_host=state_config.controller_host)
        ledger.verify_attempt_lifecycle_projection(args.attempt)
        attempt = ledger.get_attempt(args.attempt)
        if attempt["state"] not in {"active", "stopping", "terminated", "cancelled"}:
            raise ValueError("Collector attempt has an invalid lifecycle state")
    client = (RunPodClient.from_config() if args.ledger is not None else
              provider_client_for_attempt(ledger, args.attempt, "collector"))
    collect_loop(client, ledger, args.attempt, args.project,
                 ArtifactStore(args.artifact_directory), names, args.key,
                 args.known_hosts, args.remote_root, args.directory, ssh_route=args.ssh_route,
                 max_file_bytes=args.max_file_bytes, max_total_bytes=args.max_total_bytes,
                 per_file_limits=limits["per_file_limits"],
                 timeout_seconds=args.timeout_seconds, chunk_bytes=args.chunk_bytes)


def _validate_transfer_limits(names, max_file_bytes, max_total_bytes, per_file_limits,
                              timeout_seconds=DEFAULT_COLLECTION_TIMEOUT_SECONDS,
                              chunk_bytes=DEFAULT_TRANSFER_CHUNK_BYTES):
    if type(max_file_bytes) is not int or max_file_bytes <= 0:
        raise ValueError("max_file_bytes must be a positive integer")
    if type(max_total_bytes) is not int or max_total_bytes <= 0:
        raise ValueError("max_total_bytes must be a positive integer")
    validate_timeout_seconds(timeout_seconds, "collection_timeout_seconds")
    validate_chunk_bytes(chunk_bytes, "collection_chunk_bytes")
    if per_file_limits is None:
        per_file_limits = {}
    if (not isinstance(per_file_limits, dict) or
            any(not isinstance(name, str) or not name for name in per_file_limits) or
            not set(per_file_limits) <= set(names) or
            any(type(value) is not int or value < 0 or value > max_file_bytes
                for value in per_file_limits.values()) or
            sum(per_file_limits.values()) > max_total_bytes):
        raise ValueError("Invalid per-file result limits")
    return {"schema_version": COLLECTION_LIMITS_SCHEMA_VERSION,
            "max_file_bytes": max_file_bytes, "max_total_bytes": max_total_bytes,
            "per_file_limits": per_file_limits,
            "collection_timeout_seconds": timeout_seconds,
            "collection_chunk_bytes": chunk_bytes}


if __name__ == "__main__":
    main()
