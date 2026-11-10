"""Stable failure categories for operational retry and outcome decisions."""
from __future__ import annotations

import re
from enum import Enum

from runpod_experiments.contracts import FailureCategory


_SAFE_CODE = re.compile(r"[a-z][a-z0-9_.-]{0,63}\Z")


class ExecutionError(Exception):
    """A sanitized operational error; raw provider text belongs in private evidence."""

    __slots__ = ("category", "code")

    def __init__(self, category: FailureCategory, code: str):
        if not isinstance(category, FailureCategory):
            raise ValueError("category must be a FailureCategory")
        if not isinstance(code, str) or _SAFE_CODE.fullmatch(code) is None:
            raise ValueError("code must be a short sanitized identifier")
        self.category = category
        self.code = code
        super().__init__(f"{self.category.value}:{self.code}")


class ProviderOperation(str, Enum):
    READ = "read"
    CREATE = "create"
    DELETE = "delete"


def normalize_provider_error(error: BaseException, operation: ProviderOperation) -> ExecutionError:
    """Map provider failures to sanitized categories without guessing rejection.

    A mutation error is ambiguous unless a later provider-specific adapter adds
    verified rejection evidence. HTTP status alone never means capacity rejection.
    """
    if not isinstance(operation, ProviderOperation):
        raise ValueError("operation must be a ProviderOperation")
    status = getattr(error, "status", None)
    if type(status) is not int or not 100 <= status <= 599:
        status = None
    kind = getattr(error, "kind", None)
    if kind not in {"http", "transport", "invalid_response"}:
        kind = "provider"
    code = f"http_{status}" if status is not None else kind

    if operation is not ProviderOperation.READ:
        category = (FailureCategory.AUTHORIZATION if status in (401, 403)
                    else FailureCategory.AMBIGUOUS_OPERATION)
    elif status in (401, 403):
        category = FailureCategory.AUTHORIZATION
    elif status == 404:
        category = FailureCategory.RECONCILIATION_REQUIRED
    elif status in (408, 429, 500, 502, 503, 504) or kind == "transport":
        category = FailureCategory.TRANSIENT_READ
    elif status is not None and 400 <= status < 500:
        category = FailureCategory.CONFIGURATION
    elif kind == "invalid_response":
        category = FailureCategory.INTERNAL
    else:
        category = FailureCategory.INTERNAL
    return ExecutionError(category, code)

