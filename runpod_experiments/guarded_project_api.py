"""Adapter for a consumer's existing project-guarded RunPod API.

All HTTP operations are delegated to the supplied API's ``call`` method. The
attached run guard binds mutation routes to one hash-verified ExecutionStore
attempt. This is an accident-prevention integration point, not provider IAM.
"""
from __future__ import annotations

import re

from runpod_experiments.contracts import FailureCategory
from runpod_experiments.errors import ExecutionError
from runpod_experiments.execution_store import ExecutionStore
from runpod_experiments.runpod import ProviderError, RunPodClient


_POD_ID = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")


class StoreProjectRunGuard:
    """Constrain consumer ProjectPodApi mutations to one shared-store attempt."""

    def __init__(self, store: ExecutionStore, attempt_id: str):
        if not isinstance(store, ExecutionStore):
            raise TypeError("store must be an ExecutionStore")
        self.store = store
        self.attempt_id = attempt_id

    def _identity(self):
        self.store.verify_attempt_lifecycle_projection(self.attempt_id)
        attempt = self.store.get_attempt(self.attempt_id)
        policy = self.store.project_policy_for_attempt(self.attempt_id)
        if attempt["project_id"] != policy.project_id or policy.project_id != self.store.project_id:
            raise ExecutionError(FailureCategory.AUTHORIZATION, "attempt_project_mismatch")
        return attempt, policy

    def authorize_create(self, scope, request, inventory):
        attempt, policy = self._identity()
        expected = self.store.create_request_for_attempt(self.attempt_id)
        claimed = (attempt.get("create_claimed") is True or
                   type(attempt.get("create_claimed")) is int and
                   attempt.get("create_claimed") == 1)
        if (scope.project_id != policy.project_id or attempt["state"] != "create_claimed" or
                not claimed or not isinstance(inventory, list) or inventory or
                request != expected or request.get("name") != attempt["pod_name"] or
                not any(attempt["pod_name"].startswith(prefix) and
                        len(attempt["pod_name"]) > len(prefix)
                        for prefix in policy.pod_name_prefixes)):
            raise ExecutionError(FailureCategory.AUTHORIZATION,
                                 "project_api_create_outside_attempt")

    def created(self, response):
        attempt, _policy = self._identity()
        if (not isinstance(response, dict) or response.get("name") != attempt["pod_name"] or
                not isinstance(response.get("id"), str) or
                _POD_ID.fullmatch(response["id"]) is None):
            raise ExecutionError(FailureCategory.AMBIGUOUS_OPERATION,
                                 "project_api_create_identity_invalid")

    def create_failed(self):
        # The shared create controller owns ambiguity and never releases a claim
        # after the guarded API has begun the provider request.
        return None

    def authorize_mutation(self, method, pod):
        attempt, policy = self._identity()
        if (method != "DELETE" or attempt["state"] != "stopping" or
                not attempt.get("pod_id") or not isinstance(pod, dict) or
                pod.get("id") != attempt["pod_id"] or
                pod.get("name") != attempt["pod_name"] or
                not any(attempt["pod_name"].startswith(prefix) and
                        len(attempt["pod_name"]) > len(prefix)
                        for prefix in policy.pod_name_prefixes)):
            raise ExecutionError(FailureCategory.AUTHORIZATION,
                                 "project_api_mutation_outside_attempt")


class GuardedProjectApiClient(RunPodClient):
    """Shared client protocol routed through a consumer's guarded API object."""

    def __init__(self, api, store: ExecutionStore, attempt_id: str):
        if not callable(getattr(api, "call", None)):
            raise TypeError("project API must provide guarded call(method, path, body)")
        if getattr(api, "run_guard", None) is not None:
            raise ValueError("project API already has a run guard")
        self._project_api = api
        api.run_guard = StoreProjectRunGuard(store, attempt_id)

    def _request(self, method, path, body=None):
        try:
            return self._project_api.call(method, path, body)
        except ProviderError:
            raise
        except ExecutionError:
            # Preserve the shared authorization/reconciliation decision raised
            # by a consumer guard before a provider request is made.
            raise
        except Exception as error:
            status = getattr(error, "status", None)
            if type(status) is int and 100 <= status <= 599:
                raise ProviderError("Guarded project API request failed", status,
                                    kind="http") from None
            raise ProviderError("Guarded project API request failed",
                                kind="transport") from None
