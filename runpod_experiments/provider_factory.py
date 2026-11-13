"""Resolve a project-owned guarded provider client for an exact attempt.

The import path is part of ``ProjectPolicy`` and therefore its approval and plan
hashes. Consumer factories must apply that project's existing provider mutation
guard and return the shared client protocol. The default factory uses the
library's direct RunPod client for synthetic/internal development only.
"""
from __future__ import annotations

from importlib import import_module
from pathlib import Path
import sys
from typing import Any

from runpod_experiments.contracts import FailureCategory
from runpod_experiments.errors import ExecutionError


_PURPOSES = frozenset({"controller", "supervisor", "collector"})
_CLIENT_METHODS = {
    "controller": (
        "pods", "pod", "ssh_key_fingerprints", "_create_pod", "_delete_pod",
    ),
    "supervisor": ("pods", "pod", "_delete_pod"),
    "collector": ("pod",),
}


def _load_factory(identifier: str, module_roots: tuple[str, ...] = ()):
    module_name, attribute_path = identifier.split(":", 1)
    try:
        trusted_paths = (str(Path(__file__).resolve().parent.parent), *module_roots)
        for root in reversed(trusted_paths):
            if root in sys.path:
                sys.path.remove(root)
            sys.path.insert(0, root)
        value: Any = import_module(module_name)
        for part in attribute_path.split("."):
            value = getattr(value, part)
    except Exception:
        raise ExecutionError(FailureCategory.CONFIGURATION,
                             "provider_factory_unavailable") from None
    if not callable(value):
        raise ExecutionError(FailureCategory.CONFIGURATION,
                             "provider_factory_not_callable")
    return value


def default_provider_factory(*, store, attempt_id: str, purpose: str):
    """Build the standard client when a project has no guarded API adapter."""
    del store, attempt_id
    if purpose not in _PURPOSES:
        raise ValueError("unsupported provider factory purpose")
    from runpod_experiments.runpod import RunPodClient

    return RunPodClient.from_config()


def provider_client_for_attempt(store, attempt_id: str, purpose: str):
    """Load the client factory bound to one stored project policy and attempt."""
    if purpose not in _PURPOSES:
        raise ExecutionError(FailureCategory.CONFIGURATION,
                             "provider_factory_purpose_invalid")
    policy = store.project_policy_for_attempt(attempt_id)
    factory = _load_factory(policy.provider_factory,
                            policy.provider_factory_module_roots)
    try:
        client = factory(store=store, attempt_id=attempt_id, purpose=purpose)
    except ExecutionError:
        raise
    except Exception:
        raise ExecutionError(FailureCategory.CONFIGURATION,
                             "provider_factory_failed") from None
    if any(not callable(getattr(client, name, None))
           for name in _CLIENT_METHODS[purpose]):
        raise ExecutionError(FailureCategory.CONFIGURATION,
                             "provider_client_protocol_invalid")
    return client
