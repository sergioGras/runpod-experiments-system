"""Synthetic shared fixtures; none reads credentials or contacts a provider."""
from __future__ import annotations

from dataclasses import dataclass, field
import os
from pathlib import Path
import tempfile
from typing import Any

from runpod_experiments.runpod import ProviderError


TEST_MODE_ENV = "RUNPOD_EXPERIMENTS_TEST_MODE"


@dataclass
class FakeProvider:
    """Small in-memory adapter fixture for contract and engine tests."""

    inventory: list[dict[str, Any]] = field(default_factory=list)
    offers: list[dict[str, Any]] = field(default_factory=list)
    project_prefixes: tuple[str, ...] = ()
    requests: list[tuple[str, Any]] = field(default_factory=list)

    def list_account_pods(self) -> list[dict[str, Any]]:
        self.requests.append(("list_account_pods", None))
        return [dict(pod) for pod in self.inventory]

    def pods(self) -> list[dict[str, Any]]:
        return self.list_account_pods()

    def pod(self, pod_id: str) -> dict[str, Any]:
        matches = [pod for pod in self.inventory if pod.get("id") == pod_id]
        if len(matches) != 1:
            raise ProviderError("Fake Pod detail is missing or ambiguous", 404)
        return dict(matches[0])

    def list_project_pods(self, prefixes: tuple[str, ...]) -> list[dict[str, Any]]:
        self.requests.append(("list_project_pods", tuple(prefixes)))
        return [dict(pod) for pod in self.inventory
                if isinstance(pod.get("name"), str) and
                any(pod["name"].startswith(prefix) for prefix in prefixes)]

    def create_once(self, attempt_id: str, request: dict[str, Any]) -> dict[str, Any]:
        if any(name == "create_once" and payload[0] == attempt_id for name, payload in self.requests
               if isinstance(payload, tuple)):
            raise RuntimeError("fake provider rejects a duplicate attempt")
        name = request.get("name")
        if (not isinstance(name, str) or not self.project_prefixes or
                not any(name.startswith(prefix) and len(name) > len(prefix)
                        for prefix in self.project_prefixes)):
            raise ValueError("fake provider rejects a name outside the project scope")
        self.requests.append(("create_once", (attempt_id, dict(request))))
        pod = {**request, "id": "fake-pod-" + str(len(self.inventory) + 1)}
        self.inventory.append(pod)
        return dict(pod)

    def delete_exact(self, pod_id: str, pod_name: str) -> bool:
        matches = [pod for pod in self.inventory
                   if pod.get("id") == pod_id and pod.get("name") == pod_name]
        self.requests.append(("delete_exact", (pod_id, pod_name)))
        if (len(matches) != 1 or not self.project_prefixes or
                not any(matches[0]["name"].startswith(prefix) for prefix in self.project_prefixes)):
            return False
        self.inventory.remove(matches[0])
        return True


@dataclass(frozen=True)
class SyntheticWorkload:
    """Deterministic workload fixture whose stages can fail by named stage."""

    result: bytes = b"synthetic-result"
    fail_at: str | None = None

    def run_stage(self, stage: str) -> bytes | None:
        if self.fail_at == stage:
            raise RuntimeError("synthetic stage failure: " + stage)
        if stage == "collect":
            return self.result
        return None


def temporary_state() -> tempfile.TemporaryDirectory[str]:
    """Return an isolated temporary location for future persistence tests."""
    return tempfile.TemporaryDirectory(prefix="runpod-experiments-test-")


def test_mode_environment() -> dict[str, str]:
    """Build a child environment that cannot open the user's RunPod config."""
    result = dict(os.environ)
    result[TEST_MODE_ENV] = "1"
    result["HOME"] = tempfile.gettempdir()
    result["USERPROFILE"] = tempfile.gettempdir()
    return result
