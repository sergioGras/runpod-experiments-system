"""Explicit local state-root configuration for cooperating project checkouts."""
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
from typing import TYPE_CHECKING, Any, Mapping

if TYPE_CHECKING:
    from runpod_experiments.execution_store import ExecutionStore


STATE_ROOT_ENV = "RUNPOD_EXPERIMENTS_STATE_ROOT"
CONTROLLER_HOST_ENV = "RUNPOD_EXPERIMENTS_CONTROLLER_HOST"
_PROJECT_ID = re.compile(r"[a-z][a-z0-9-]{2,63}\Z")
_HOST_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,252}\Z")
_DATABASE_NAME = "execution.sqlite3"


@dataclass(frozen=True, slots=True)
class StateStoreConfig:
    """Configured state root and operator-assigned single-host identity.

    Set the same absolute state root and controller-host identity in every
    checkout that must coordinate. This local SQLite configuration does not
    provide a cross-host lock or prove the configured host identity.
    """

    state_root: Path
    controller_host: str

    def __post_init__(self) -> None:
        root = Path(self.state_root).expanduser()
        if not root.is_absolute():
            raise ValueError("state_root must be an absolute path")
        root = root.resolve(strict=False)
        if root.exists() and not root.is_dir():
            raise ValueError("state_root must be a directory")
        if (not isinstance(self.controller_host, str) or
                _HOST_ID.fullmatch(self.controller_host) is None):
            raise ValueError("controller_host must be a configured host identity")
        object.__setattr__(self, "state_root", root)

    @classmethod
    def from_environment(cls, environ: Mapping[str, str] | None = None) -> "StateStoreConfig":
        """Load explicit state configuration without reading it at import time."""
        values = os.environ if environ is None else environ
        root = values.get(STATE_ROOT_ENV)
        host = values.get(CONTROLLER_HOST_ENV)
        if not isinstance(root, str) or not root.strip():
            raise ValueError(f"{STATE_ROOT_ENV} must be configured")
        if root != root.strip():
            raise ValueError(f"{STATE_ROOT_ENV} cannot have surrounding whitespace")
        if not isinstance(host, str) or not host.strip():
            raise ValueError(f"{CONTROLLER_HOST_ENV} must be configured")
        return cls(Path(root), host)

    def database_path(self, project_id: str) -> Path:
        """Return the shared per-project file path without creating state."""
        if not isinstance(project_id, str) or _PROJECT_ID.fullmatch(project_id) is None:
            raise ValueError("project_id is invalid")
        project_dir = self.state_root / project_id
        if project_dir.is_symlink():
            raise ValueError("project state directory cannot be a symlink")
        if project_dir.exists() and not project_dir.is_dir():
            raise ValueError("project state path must be a directory")
        path = project_dir / _DATABASE_NAME
        if path.is_symlink():
            raise ValueError("execution database cannot be a symlink")
        if path.exists() and not path.is_file():
            raise ValueError("execution database path must be a file")
        resolved = path.resolve(strict=False)
        if resolved.parent != project_dir:
            raise ValueError("execution database path escaped the configured state root")
        return resolved

    def prepare_database_path(self, project_id: str) -> Path:
        """Create the project directory and return its configured database path."""
        path = self.database_path(project_id)
        if not self.state_root.exists():
            self.state_root.mkdir(mode=0o700, exist_ok=True)
        if not self.state_root.is_dir():
            raise ValueError("state_root must be a directory")
        path.parent.mkdir(mode=0o700, exist_ok=True)
        return self.database_path(project_id)

    def open_execution_store(self, project_id: str, **store_options: Any) -> "ExecutionStore":
        """Open this project's store with the configured path and host identity."""
        from runpod_experiments.execution_store import ExecutionStore

        path = self.prepare_database_path(project_id)
        return ExecutionStore(
            str(path), project_id=project_id,
            controller_host=self.controller_host, **store_options)
