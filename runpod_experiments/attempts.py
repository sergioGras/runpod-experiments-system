"""Durable attempt identity and create reconciliation for one research round."""
from contextlib import closing
import re
import sqlite3


class AttemptError(RuntimeError):
    pass


def _identity(value, pattern):
    return isinstance(value, str) and re.fullmatch(pattern, value) is not None


class AttemptLedger:
    def __init__(self, path):
        self.path = str(path)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("""CREATE TABLE IF NOT EXISTS attempts (
                attempt_id TEXT PRIMARY KEY, project_id TEXT NOT NULL,
                pod_name TEXT NOT NULL UNIQUE, state TEXT NOT NULL,
                pod_id TEXT UNIQUE, create_claimed INTEGER NOT NULL DEFAULT 0,
                reconcilable INTEGER NOT NULL DEFAULT 0)""")
            columns = {row[1] for row in db.execute("PRAGMA table_info(attempts)")}
            if "reconcilable" not in columns:
                db.execute("ALTER TABLE attempts ADD COLUMN reconcilable INTEGER NOT NULL DEFAULT 0")
            db.execute("""CREATE TABLE IF NOT EXISTS events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT, attempt_id TEXT NOT NULL,
                event TEXT NOT NULL, created_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)""")

    def reserve(self, attempt_id, project_id, pod_name, prefix):
        if (not _identity(attempt_id, r"[A-Za-z0-9_-]{1,128}") or
                not _identity(project_id, r"[a-z][a-z0-9-]{2,63}") or
                not _identity(prefix, r"[a-z][a-z0-9-]+-") or
                not _identity(pod_name, r"[a-z0-9][a-z0-9-]{0,127}") or
                not pod_name.startswith(prefix) or len(pod_name) <= len(prefix)):
            raise ValueError("Invalid project or attempt identity")
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("INSERT INTO attempts(attempt_id,project_id,pod_name,state,pod_id,create_claimed) VALUES (?, ?, ?, 'reserved', NULL, 0)",
                       (attempt_id, project_id, pod_name))
            db.execute("INSERT INTO events(attempt_id,event) VALUES (?, 'reserved')", (attempt_id,))

    def reserve_registered(self, attempt_id, project_id, pod_name, registry):
        """Reserve a name from a validated shared registry."""
        if not registry.owns(project_id, pod_name):
            raise AttemptError("Pod name is outside the registered project namespace")
        prefix = next(p for p in registry.projects[project_id] if pod_name.startswith(p))
        self.reserve(attempt_id, project_id, pod_name, prefix)

    def get(self, attempt_id):
        with closing(sqlite3.connect(self.path)) as db:
            db.row_factory = sqlite3.Row
            row = db.execute("SELECT * FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
        if row is None:
            raise AttemptError("Unknown attempt")
        return dict(row)

    def claim_create(self, attempt_id):
        """Permanently consume this attempt's one create request before sending it."""
        with closing(sqlite3.connect(self.path)) as db, db:
            changed = db.execute("UPDATE attempts SET state='creating', create_claimed=1 WHERE attempt_id=? AND state='reserved' AND create_claimed=0", (attempt_id,)).rowcount
            if changed != 1:
                raise AttemptError("Create already claimed or attempt unavailable")
            db.execute("INSERT INTO events(attempt_id,event) VALUES (?, 'create_claimed')", (attempt_id,))

    def cancel_uncreated(self, attempt_id):
        """Atomically close a reservation only if no create was ever claimed."""
        with closing(sqlite3.connect(self.path)) as db, db:
            changed = db.execute("UPDATE attempts SET state='cancelled' WHERE attempt_id=? AND state='reserved' AND create_claimed=0", (attempt_id,)).rowcount
            if changed != 1:
                raise AttemptError("Attempt may already have a create in flight")
            db.execute("INSERT INTO events(attempt_id,event) VALUES (?, 'cancelled_before_create')", (attempt_id,))

    def claim_reconcilable_create(self, attempt_id, inventory, max_instances=1):
        """Persist a claim after confirming a random attempt name is unused."""
        attempt = self.get(attempt_id)
        if not re.search(r"-[0-9a-f]{32}$", attempt["pod_name"]):
            raise AttemptError("Reconciliation needs a unique random attempt name")
        if not isinstance(inventory, list) or any(not isinstance(p, dict) for p in inventory):
            raise AttemptError("Invalid provider inventory")
        if any(p.get("name") == attempt["pod_name"] for p in inventory):
            raise AttemptError("Attempt name already exists at provider")
        if type(max_instances) is not int or max_instances < 1:
            raise ValueError("Invalid instance limit")
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            active = db.execute("SELECT COUNT(*) FROM attempts WHERE project_id=? AND state IN ('creating','active')", (attempt["project_id"],)).fetchone()[0]
            if active >= max_instances:
                raise AttemptError("Project has too many active or unresolved attempts")
            changed = db.execute("UPDATE attempts SET state='creating',create_claimed=1,reconcilable=1 WHERE attempt_id=? AND state='reserved' AND create_claimed=0", (attempt_id,)).rowcount
            if changed != 1:
                raise AttemptError("Create already claimed or attempt unavailable")
            db.execute("INSERT INTO events(attempt_id,event) VALUES (?, 'create_claimed_after_inventory')", (attempt_id,))

    def bind(self, attempt_id, pod_id):
        if not _identity(pod_id, r"[A-Za-z0-9_-]{1,128}"):
            raise ValueError("Invalid provider ID")
        with closing(sqlite3.connect(self.path)) as db, db:
            row = db.execute("SELECT state, pod_id, create_claimed FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
            if row is None or row[2] != 1 or row[0] not in ("creating", "active") or row[1] not in (None, pod_id):
                raise AttemptError("Attempt cannot bind this provider ID")
            if row[1] is None:
                db.execute("UPDATE attempts SET pod_id=?, state='active' WHERE attempt_id=?", (pod_id, attempt_id))
                db.execute("INSERT INTO events(attempt_id,event) VALUES (?, 'bound')", (attempt_id,))

    def terminated(self, attempt_id, pod_id):
        with closing(sqlite3.connect(self.path)) as db, db:
            changed = db.execute("UPDATE attempts SET state='terminated' WHERE attempt_id=? AND pod_id=? AND state='active'", (attempt_id, pod_id)).rowcount
            if changed != 1:
                raise AttemptError("Cannot confirm termination for this attempt")
            db.execute("INSERT INTO events(attempt_id,event) VALUES (?, 'terminated')", (attempt_id,))


def reconcile_candidate(ledger, attempt_id, inventory):
    """Identify a unique candidate after a claimed create; no provider mutation."""
    attempt = ledger.get(attempt_id)
    if attempt["state"] != "creating" or attempt["create_claimed"] != 1:
        raise AttemptError("No unresolved claimed create")
    if not isinstance(inventory, list) or any(not isinstance(p, dict) for p in inventory):
        raise AttemptError("Invalid provider inventory")
    matches = [p for p in inventory if p.get("name") == attempt["pod_name"] and p.get("status") != "TERMINATED"]
    if len(matches) > 1:
        raise AttemptError("Duplicate name requires manual reconciliation")
    if not matches:
        return None
    pod_id = matches[0].get("id")
    if not _identity(pod_id, r"[A-Za-z0-9_-]{1,128}"):
        raise AttemptError("Candidate lacks a valid provider ID")
    return pod_id
