"""Validate cooperating project namespaces without granting provider access."""
import json
import re
from pathlib import Path


class ProjectRegistry:
    def __init__(self, path):
        value = json.loads(Path(path).read_text(encoding="utf-8"))
        projects = value.get("projects") if isinstance(value, dict) else None
        if not isinstance(value, dict) or value.get("schema_version") != 1 or not isinstance(projects, dict) or not projects:
            raise ValueError("Invalid project registry")
        prefixes = []
        self.projects = {}
        for project, spec in projects.items():
            if (not isinstance(project, str) or not re.fullmatch(r"[a-z][a-z0-9-]{2,63}", project) or
                    not isinstance(spec, dict) or not isinstance(spec.get("pod_name_prefixes"), list) or
                    not spec["pod_name_prefixes"]):
                raise ValueError("Invalid project registry entry")
            for prefix in spec["pod_name_prefixes"]:
                if not isinstance(prefix, str) or not re.fullmatch(r"[a-z][a-z0-9-]+-", prefix):
                    raise ValueError("Invalid project namespace")
                if any(prefix.startswith(other) or other.startswith(prefix) for other in prefixes):
                    raise ValueError("Overlapping project namespaces")
                prefixes.append(prefix)
            self.projects[project] = tuple(spec["pod_name_prefixes"])

    def owns(self, project, name):
        return (isinstance(name, str) and re.fullmatch(r"[a-z0-9][a-z0-9-]{0,127}", name) is not None and
                any(name.startswith(prefix) and len(name) > len(prefix) for prefix in self.projects.get(project, ())))
