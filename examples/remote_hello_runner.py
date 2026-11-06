"""Bounded RunPod hello attempt. Use only after project-specific approval.

This runner is prepared for a future paid round; merely invoking it would
create a billable resource and disclose the reviewed example source.
"""
import argparse
import hashlib
import json
import math
import os
import subprocess
import sys
import time
import uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path

# Support this checked-out package with the workstation's isolated Python.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from runpod_experiments.artifacts import ArtifactStore
from runpod_experiments.attempts import AttemptLedger
from runpod_experiments.budget import BudgetLedger
from runpod_experiments.controller import create_attempt, new_pod_name
from runpod_experiments.planning import cpu_resource_policy, verify_cpu_quote
from runpod_experiments.registry import ProjectRegistry
from runpod_experiments.resource_policy import ResourcePolicy
from runpod_experiments.runpod import RunPodClient
from runpod_experiments.ssh import bootstrap_tofu
from runpod_experiments.supervisor_process import launch
from runpod_experiments.workflow import reconcile_cleanup, run_owned_workload

SOURCE_SHA256 = "c2023081533c108d711e9a34fd5a69d8bfdb3ee725309c3b9dd81bb3a5c2c0ae"
RESULT_SHA256 = "600e7063e52c1a4778896a3f35b2602ef0b022ea3d01b029b378cb9ae7f3e5a0"
IMAGE = "runpod/base@sha256:81db6f425b06addb56faef7b2330360fce96e20361ce974bbe8919ec66545e01"
PROJECT = "hybrid-inference"


def round_metadata(root, max_usd, approval_reference, *, clock=time.time):
    """Keep retries under the same cap, approval and finite round deadline."""
    try:
        amount = Decimal(max_usd)
        if not amount.is_finite() or amount <= 0:
            raise ValueError("Invalid round cap")
        cap = str(amount.normalize())
    except (InvalidOperation, TypeError) as error:
        raise ValueError("Invalid round cap") from error
    path = Path(root) / "round.json"
    proposed = {"project_id": PROJECT, "max_usd": cap, "approval_reference": approval_reference,
                "started_epoch": clock(), "max_runtime_seconds": 1800}
    try:
        with path.open("x", encoding="utf-8") as stream:
            json.dump(proposed, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        pass
    value = json.loads(path.read_text(encoding="utf-8"))
    if (value.get("project_id") != PROJECT or value.get("max_usd") != cap or
            value.get("approval_reference") != approval_reference or value.get("max_runtime_seconds") != 1800 or
            isinstance(value.get("started_epoch"), bool) or not isinstance(value.get("started_epoch"), (int, float)) or
            not math.isfinite(value["started_epoch"])):
        raise ValueError("Round metadata differs from the approved envelope")
    return value


def run(round_directory, registry_path, max_usd, approval_reference, coordination_directory=None):
    if not isinstance(approval_reference, str) or not approval_reference.strip():
        raise ValueError("Record the owner's explicit approval reference")
    source_root = Path(__file__).resolve().parent.parent
    try:
        dirty = subprocess.run(["git", "status", "--porcelain", "--untracked-files=normal"], cwd=source_root,
                               capture_output=True, text=True, check=True, timeout=20)
        revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=source_root, capture_output=True,
                                  text=True, check=True, timeout=20).stdout.strip()
    except (subprocess.SubprocessError, OSError):
        raise RuntimeError("Cannot verify shared source revision") from None
    if dirty.stdout.strip() or len(revision) != 40:
        raise RuntimeError("Commit the reviewed shared library before running")
    source = source_root / "examples/remote_hello.py"
    if hashlib.sha256(source.read_bytes()).hexdigest() != SOURCE_SHA256:
        raise ValueError("Reviewed workload source differs from manifest")
    root = Path(round_directory).resolve()
    root.mkdir(parents=True, exist_ok=True)
    round_info = round_metadata(root, max_usd, approval_reference)
    if time.time() >= round_info["started_epoch"] + round_info["max_runtime_seconds"] - 120:
        raise RuntimeError("The round runtime leaves no cleanup margin")
    coordination = Path(coordination_directory).resolve() if coordination_directory else Path.home() / ".runpod-experiments"
    coordination.mkdir(parents=True, exist_ok=True)
    budget = BudgetLedger(root / "budget.sqlite", max_usd)
    ledger = AttemptLedger(coordination / "attempts.sqlite")
    registry = ProjectRegistry(registry_path)
    client = RunPodClient.from_config()
    attempt_id = "hello-" + uuid.uuid4().hex
    name = new_pod_name("hybrid-", "runpod-hello")
    offer = client.cpu_offer("cpu3c", 4)
    regions = sorted([dc.get("id") for dc in offer.get("dataCenters", []) or []
                      if isinstance(dc, dict) and isinstance(dc.get("id"), str) and
                      dc.get("availability") in {"HIGH", "MEDIUM", "LOW"}],
                     key=lambda region: (not region.startswith(("EU-", "EUR-")), region))
    if not regions and offer.get("availability") not in {"HIGH", "MEDIUM", "LOW"}:
        raise RuntimeError("No CPU3C region currently available")
    request = {"name": name, "cpu": {"id": "cpu3c", "vcpuCount": 4}, "cloud": "SECURE",
               "dataCenterIds": regions[:1], "image": IMAGE, "disk": 10, "ports": ["22/tcp"],
               "startSsh": True, "startJupyter": False}
    policy_data = cpu_resource_policy(request, offer, "0.002")
    # A 30-minute total envelope includes a 2-minute cleanup margin.
    armed = time.time()
    started_monotonic = time.monotonic()
    work_deadline = round_info["started_epoch"] + round_info["max_runtime_seconds"] - 120
    directory = root / attempt_id
    directory.mkdir(exist_ok=False)
    policy_path = directory / "policy.json"
    policy_path.write_text(json.dumps(policy_data, sort_keys=True) + "\n", encoding="utf-8")
    ledger.reserve_registered(attempt_id, PROJECT, name, registry)
    plan = {"approval_reference": approval_reference, "attempt_id": attempt_id, "project_id": PROJECT,
            "request": request, "max_usd": str(max_usd), "armed_epoch": armed, "deadline_epoch": work_deadline,
            "source_revision": revision, "source_sha256": SOURCE_SHA256, "expected_checkpoint_sha256": RESULT_SHA256,
            "policy_sha256": ResourcePolicy.read(policy_path).fingerprint}
    (directory / "plan.json").write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    supervisor = launch(ledger.path, attempt_id, PROJECT, directory, work_deadline, policy_path)
    result, failure = None, None
    try:
        ready_deadline = min(work_deadline, time.time() + 90)
        while time.time() < ready_deadline:
            if supervisor.poll() is not None:
                raise RuntimeError("Supervisor exited before creation")
            state_file = directory / "supervisor.json"
            if state_file.exists() and json.loads(state_file.read_text(encoding="utf-8")).get("ready"):
                break
            time.sleep(1)
        else:
            raise TimeoutError("Supervisor did not become ready")
        create_attempt(client, ledger, registry, budget, attempt_id, PROJECT, request, supervisor,
                       directory, work_deadline, policy_path,
                       quote_validator=lambda c, r, p: verify_cpu_quote(c, r, p, "0.002"))
        boot_deadline = min(work_deadline, time.time() + 600)
        private_key = Path.home() / ".runpod/ssh/runpodctl-ssh-key"
        known_hosts = directory / "known_hosts"
        while time.time() < boot_deadline:
            if supervisor.poll() is not None:
                raise RuntimeError("Supervisor exited during startup")
            state = json.loads((directory / "supervisor.json").read_text(encoding="utf-8"))
            if state.get("stopping") or time.time() - state.get("heartbeat_epoch", 0) > 30:
                raise RuntimeError("Supervisor stopped or heartbeat expired")
            attempt = ledger.get(attempt_id)
            pod = client.pod(attempt["pod_id"])
            safe_fields = ("id", "name", "status", "cloud", "cpu", "dataCenterId", "image", "disk", "ports", "cost", "createdAt", "startedAt")
            (directory / "provider-summary.json").write_text(json.dumps({k: pod.get(k) for k in safe_fields}, sort_keys=True) + "\n", encoding="utf-8")
            ssh = pod.get("ssh")
            if pod.get("status") == "RUNNING" and isinstance(ssh, dict) and ssh.get("direct"):
                try:
                    bootstrap_tofu(client, ledger, attempt_id, PROJECT, private_key, known_hosts)
                    break
                except RuntimeError:
                    pass
            time.sleep(10)
        else:
            raise TimeoutError("Direct SSH did not become ready")
        remote_root = "/tmp/" + name
        store = ArtifactStore(directory / "artifacts")
        result = run_owned_workload(client, ledger, attempt_id, PROJECT, store, source_root=source_root,
            manifest={"examples/remote_hello.py": SOURCE_SHA256}, key_file=private_key,
            known_hosts=known_hosts, remote_root=remote_root, script="examples/remote_hello.py",
            arguments=["--directory", "results"], timeout_seconds=300,
            result_names=["results/checkpoint.json"], control_directory=directory, supervisor=supervisor)
        if result["exit_code"] != 0 or not (directory / "artifacts/objects" / RESULT_SHA256).is_file():
            raise RuntimeError("Synthetic output was not collected and verified")
    except Exception as error:
        failure = type(error).__name__
    finally:
        (directory / "stop.request").touch()
        cleanup = reconcile_cleanup(client, ledger, attempt_id, PROJECT, directory, budget=budget)
        summary = {"attempt_id": attempt_id, "result": result, "failure_type": failure,
                   "cleanup": cleanup, "elapsed_seconds": round(time.monotonic() - started_monotonic, 6),
                   "actual_cost_usd": None, "billing_reconciliation_pending": bool(ledger.get(attempt_id)["create_claimed"])}
        (directory / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--round-directory", required=True)
    parser.add_argument("--registry", required=True)
    parser.add_argument("--max-usd", required=True)
    parser.add_argument("--approval-reference", required=True)
    parser.add_argument("--coordination-directory", help="Shared private directory for all local controllers")
    args = parser.parse_args()
    result = run(args.round_directory, args.registry, args.max_usd, args.approval_reference, args.coordination_directory)
    print(json.dumps(result, sort_keys=True))
    if result["failure_type"] or not result["cleanup"]["terminated"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
