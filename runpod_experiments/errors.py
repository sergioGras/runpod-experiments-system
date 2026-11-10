"""Stable failure categories for operational retry and outcome decisions."""
from __future__ import annotations

import re

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

