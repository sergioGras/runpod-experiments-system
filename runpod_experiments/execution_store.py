"""Versioned transactional execution state, admission and accounting ledger.

This local store coordinates cooperating processes on one authorized host. It is
not provider IAM, a cross-host lock, or a guarantee against delayed billing.
"""
from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
import re
import sqlite3
import math
import time
from typing import Any, Iterator

from runpod_experiments.contracts import (
    ApprovalScope, AttemptPlan, BillingObservation, ContractError, ExecutionPlan,
    FailureCategory,
    canonical_json_bytes, decode_document, encode_document, sha256_bytes,
    strict_json_loads, validate_attempt,
)
from runpod_experiments.errors import ExecutionError


STORE_SCHEMA_VERSION = 2
SQLITE_MAX_INTEGER = 9_223_372_036_854_775_807
_POD_ID = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")
_POD_NAME = re.compile(r"[a-z0-9][a-z0-9-]{0,127}\Z")
_PROJECT_ID = re.compile(r"[a-z][a-z0-9-]{2,63}\Z")
_HOST_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,252}\Z")
_ACTIVE_SQL = "('reserved','create_claimed','reconciliation_required','active','stopping')"
_ZERO_HASH = "0" * 64


@dataclass(frozen=True, slots=True)
class AccountingSnapshot:
    project_id: str
    approval_id: str
    max_total_micro_usd: int
    max_hourly_micro_usd: int
    billed_micro_usd: int
    reserved_micro_usd: int
    committed_micro_usd: int
    remaining_micro_usd: int
    active_resources: int
    active_hourly_micro_usd: int


class ExecutionStore:
    """File-backed, schema-pinned SQLite state for one project's execution work."""

    def __init__(self, path: str, *, project_id: str, controller_host: str,
                 clock=time.time, timeout_seconds: int = 15):
        if not isinstance(path, str) or not path.strip():
            raise ValueError("A dedicated execution-store path is required")
        if not isinstance(project_id, str) or _PROJECT_ID.fullmatch(project_id) is None:
            raise ValueError("A valid project_id is required")
        if not isinstance(controller_host, str) or _HOST_ID.fullmatch(controller_host) is None:
            raise ValueError("A valid controller_host identity is required")
        if type(timeout_seconds) is not int or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be a positive integer")
        self.path = str(Path(path).resolve())
        self.project_id = project_id
        self.controller_host = controller_host
        self._clock = clock
        self._timeout_seconds = timeout_seconds
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=self._timeout_seconds, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON")
        db.execute("PRAGMA synchronous = FULL")
        db.execute(f"PRAGMA busy_timeout = {self._timeout_seconds * 1000}")
        return db

    def _now(self) -> int:
        value = self._clock()
        if (isinstance(value, bool) or not isinstance(value, (int, float)) or
                not math.isfinite(value) or value < 0 or value > SQLITE_MAX_INTEGER):
            raise ExecutionError(FailureCategory.INTERNAL, "clock_invalid")
        return int(value)

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    def _initialize(self) -> None:
        with closing(sqlite3.connect(self.path, timeout=self._timeout_seconds,
                                     isolation_level=None)) as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            tables = {row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
            views = {row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='view'")}
            required = {"execution_store_meta", "execution_approvals", "execution_jobs", "execution_plans",
                        "execution_attempts", "execution_billing", "execution_events"}
            if version not in (0, STORE_SCHEMA_VERSION):
                raise ExecutionError(FailureCategory.INTERNAL, "unsupported_store_schema")
            if version == STORE_SCHEMA_VERSION:
                if tables != required or views:
                    raise ExecutionError(FailureCategory.INTERNAL, "incomplete_store_schema")
                required_columns = {
                    "execution_store_meta": {"singleton", "project_id", "controller_host"},
                    "execution_approvals": {"project_id", "approval_id", "scope_sha256", "policy_sha256",
                        "document", "max_total_micro_usd", "max_hourly_micro_usd",
                        "max_active_resources", "expires_epoch"},
                    "execution_jobs": {"project_id", "job_id", "approval_id"},
                    "execution_plans": {"plan_sha256", "project_id", "job_id", "document"},
                    "execution_attempts": {"attempt_id", "project_id", "job_id", "approval_id",
                        "plan_sha256", "attempt_sha256", "attempt_document", "attempt_number",
                        "pod_name", "pod_id", "state", "create_claimed",
                        "project_max_active_resources", "approval_max_active_resources",
                        "hourly_micro_usd", "initial_exposure_micro_usd", "exposure_micro_usd",
                        "billing_complete", "billing_complete_evidence_sha256",
                        "billing_complete_covered_start_epoch", "billing_complete_covered_end_epoch",
                        "create_claimed_epoch", "terminated_epoch", "created_epoch", "deadline_epoch"},
                    "execution_billing": {"observation_id", "attempt_id", "project_id",
                        "provider_pod_id", "amount_micro_usd", "period_start_epoch",
                        "period_end_epoch", "observed_epoch", "evidence_sha256",
                        "supersedes_observation_id", "document"},
                    "execution_events": {"sequence", "entity_type", "entity_id", "event_type",
                        "created_epoch", "previous_sha256", "payload", "payload_sha256",
                        "event_sha256"},
                }
                for table, columns in required_columns.items():
                    found = {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
                    if not columns.issubset(found):
                        raise ExecutionError(FailureCategory.INTERNAL, "incomplete_store_schema")
                meta = db.execute("""SELECT project_id,controller_host FROM execution_store_meta
                    WHERE singleton=1""").fetchone()
                if meta is None:
                    raise ExecutionError(FailureCategory.INTERNAL, "incomplete_store_schema")
                if (meta[0] != self.project_id or meta[1] != self.controller_host):
                    raise ExecutionError(FailureCategory.AUTHORIZATION, "store_identity_mismatch")
                trigger_names = {row[0] for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='trigger'")}
                required_triggers = {
                    "execution_store_meta_no_update", "execution_store_meta_no_delete",
                    "execution_events_no_update", "execution_events_no_delete",
                    "execution_billing_no_update", "execution_billing_no_delete",
                    "execution_plans_no_update", "execution_plans_no_delete",
                    "execution_approvals_no_update", "execution_approvals_no_delete",
                    "execution_jobs_no_update", "execution_jobs_no_delete",
                    "execution_attempts_identity_immutable", "execution_attempts_no_delete",
                }
                if trigger_names != required_triggers:
                    raise ExecutionError(FailureCategory.INTERNAL, "incomplete_store_schema")
                db.commit()
                return
            if tables or views:
                # The schema may have committed between these separate reads
                # while another process initialized this same fresh path.
                if db.execute("PRAGMA user_version").fetchone()[0] == STORE_SCHEMA_VERSION:
                    return self._initialize()
                raise ExecutionError(FailureCategory.INTERNAL, "unversioned_store_not_empty")

            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN EXCLUSIVE")
            version = db.execute("PRAGMA user_version").fetchone()[0]
            tables = {row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
            views = {row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='view'")}
            if version == STORE_SCHEMA_VERSION:
                db.commit()
                return self._initialize()
            if version != 0 or tables or views:
                raise ExecutionError(FailureCategory.INTERNAL, "unversioned_store_not_empty")

            try:
                statements = (
                    """CREATE TABLE execution_store_meta (
                        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                        project_id TEXT NOT NULL,
                        controller_host TEXT NOT NULL)""",
                    """CREATE TABLE execution_approvals (
                        project_id TEXT NOT NULL,
                        approval_id TEXT NOT NULL,
                        scope_sha256 TEXT NOT NULL,
                        policy_sha256 TEXT NOT NULL,
                        document BLOB NOT NULL,
                        max_total_micro_usd INTEGER NOT NULL,
                        max_hourly_micro_usd INTEGER NOT NULL,
                        max_active_resources INTEGER NOT NULL,
                        expires_epoch INTEGER NOT NULL,
                        PRIMARY KEY(project_id, approval_id))""",
                    """CREATE TABLE execution_jobs (
                        project_id TEXT NOT NULL,
                        job_id TEXT NOT NULL,
                        approval_id TEXT NOT NULL,
                        PRIMARY KEY(project_id, job_id),
                        FOREIGN KEY(project_id, approval_id)
                          REFERENCES execution_approvals(project_id, approval_id))""",
                    """CREATE TABLE execution_plans (
                        plan_sha256 TEXT PRIMARY KEY,
                        project_id TEXT NOT NULL,
                        job_id TEXT NOT NULL,
                        document BLOB NOT NULL,
                        FOREIGN KEY(project_id, job_id)
                          REFERENCES execution_jobs(project_id, job_id))""",
                    """CREATE TABLE execution_attempts (
                        attempt_id TEXT PRIMARY KEY,
                        project_id TEXT NOT NULL,
                        job_id TEXT NOT NULL,
                        approval_id TEXT NOT NULL,
                        plan_sha256 TEXT NOT NULL,
                        attempt_sha256 TEXT NOT NULL,
                        attempt_document BLOB NOT NULL,
                        attempt_number INTEGER NOT NULL,
                        pod_name TEXT NOT NULL,
                        pod_id TEXT UNIQUE,
                        state TEXT NOT NULL,
                        create_claimed INTEGER NOT NULL CHECK(create_claimed IN (0,1)),
                        project_max_active_resources INTEGER NOT NULL,
                        approval_max_active_resources INTEGER NOT NULL,
                        hourly_micro_usd INTEGER NOT NULL,
                        initial_exposure_micro_usd INTEGER NOT NULL,
                        exposure_micro_usd INTEGER NOT NULL,
                        billing_complete INTEGER NOT NULL CHECK(billing_complete IN (0,1)),
                        billing_complete_evidence_sha256 TEXT,
                        billing_complete_covered_start_epoch INTEGER,
                        billing_complete_covered_end_epoch INTEGER,
                        create_claimed_epoch INTEGER,
                        terminated_epoch INTEGER,
                        created_epoch INTEGER NOT NULL,
                        deadline_epoch INTEGER NOT NULL,
                        UNIQUE(project_id, pod_name),
                        UNIQUE(project_id, job_id, attempt_number),
                        FOREIGN KEY(project_id, job_id)
                          REFERENCES execution_jobs(project_id, job_id),
                        FOREIGN KEY(plan_sha256) REFERENCES execution_plans(plan_sha256))""",
                    """CREATE TABLE execution_billing (
                        observation_id TEXT PRIMARY KEY,
                        attempt_id TEXT NOT NULL,
                        project_id TEXT NOT NULL,
                        provider_pod_id TEXT NOT NULL,
                        amount_micro_usd INTEGER NOT NULL,
                        period_start_epoch INTEGER NOT NULL,
                        period_end_epoch INTEGER NOT NULL,
                        observed_epoch INTEGER NOT NULL,
                        evidence_sha256 TEXT NOT NULL,
                        supersedes_observation_id TEXT UNIQUE,
                        document BLOB NOT NULL,
                        FOREIGN KEY(attempt_id) REFERENCES execution_attempts(attempt_id),
                        FOREIGN KEY(supersedes_observation_id)
                          REFERENCES execution_billing(observation_id))""",
                    """CREATE TABLE execution_events (
                        sequence INTEGER PRIMARY KEY,
                        entity_type TEXT NOT NULL,
                        entity_id TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        created_epoch INTEGER NOT NULL,
                        previous_sha256 TEXT NOT NULL,
                        payload BLOB NOT NULL,
                        payload_sha256 TEXT NOT NULL,
                        event_sha256 TEXT NOT NULL UNIQUE)""",
                    "CREATE INDEX execution_attempt_scope ON execution_attempts(project_id, approval_id, state)",
                    "CREATE INDEX execution_billing_attempt ON execution_billing(attempt_id)",
                    """CREATE TRIGGER execution_events_no_update BEFORE UPDATE ON execution_events
                        BEGIN SELECT RAISE(ABORT, 'append_only'); END""",
                    """CREATE TRIGGER execution_events_no_delete BEFORE DELETE ON execution_events
                        BEGIN SELECT RAISE(ABORT, 'append_only'); END""",
                    """CREATE TRIGGER execution_billing_no_update BEFORE UPDATE ON execution_billing
                        BEGIN SELECT RAISE(ABORT, 'append_only'); END""",
                    """CREATE TRIGGER execution_billing_no_delete BEFORE DELETE ON execution_billing
                        BEGIN SELECT RAISE(ABORT, 'append_only'); END""",
                    """CREATE TRIGGER execution_plans_no_update BEFORE UPDATE ON execution_plans
                        BEGIN SELECT RAISE(ABORT, 'immutable'); END""",
                    """CREATE TRIGGER execution_plans_no_delete BEFORE DELETE ON execution_plans
                        BEGIN SELECT RAISE(ABORT, 'immutable'); END""",
                    """CREATE TRIGGER execution_approvals_no_update BEFORE UPDATE ON execution_approvals
                        BEGIN SELECT RAISE(ABORT, 'immutable'); END""",
                    """CREATE TRIGGER execution_approvals_no_delete BEFORE DELETE ON execution_approvals
                        BEGIN SELECT RAISE(ABORT, 'immutable'); END""",
                    """CREATE TRIGGER execution_jobs_no_update BEFORE UPDATE ON execution_jobs
                        BEGIN SELECT RAISE(ABORT, 'immutable'); END""",
                    """CREATE TRIGGER execution_jobs_no_delete BEFORE DELETE ON execution_jobs
                        BEGIN SELECT RAISE(ABORT, 'immutable'); END""",
                    """CREATE TRIGGER execution_attempts_no_delete BEFORE DELETE ON execution_attempts
                        BEGIN SELECT RAISE(ABORT, 'append_only'); END""",
                    """CREATE TRIGGER execution_attempts_identity_immutable
                        BEFORE UPDATE OF attempt_id,project_id,job_id,approval_id,plan_sha256,
                        attempt_sha256,attempt_document,attempt_number,pod_name,
                        project_max_active_resources,approval_max_active_resources,
                        hourly_micro_usd,initial_exposure_micro_usd,created_epoch,deadline_epoch
                        ON execution_attempts BEGIN SELECT RAISE(ABORT, 'immutable'); END""",
                    """CREATE TRIGGER execution_store_meta_no_update BEFORE UPDATE ON execution_store_meta
                        BEGIN SELECT RAISE(ABORT, 'immutable'); END""",
                    """CREATE TRIGGER execution_store_meta_no_delete BEFORE DELETE ON execution_store_meta
                        BEGIN SELECT RAISE(ABORT, 'immutable'); END""",
                )
                for statement in statements:
                    db.execute(statement)
                db.execute("""INSERT INTO execution_store_meta(singleton,project_id,controller_host)
                    VALUES (1,?,?)""", (self.project_id, self.controller_host))
                db.execute(f"PRAGMA user_version = {STORE_SCHEMA_VERSION}")
                db.commit()
            except BaseException:
                db.rollback()
                raise

    @staticmethod
    def _ensure_sqlite_int(value: int, name: str) -> int:
        if type(value) is not int or not 0 <= value <= SQLITE_MAX_INTEGER:
            raise ExecutionError(FailureCategory.CONFIGURATION, f"{name}_out_of_range")
        return value

    @staticmethod
    def _sum(values: list[int]) -> int:
        # Persisted values are individually bounded by SQLite's integer range,
        # but actual posted charges can exceed an approved total across rows.
        # Python integers preserve that aggregate so over-cap costs remain visible.
        return sum(values)

    @staticmethod
    def _append_event(db: sqlite3.Connection, entity_type: str, entity_id: str,
                      event_type: str, created_epoch: int, payload: dict[str, Any]) -> None:
        previous_row = db.execute(
            "SELECT sequence, event_sha256 FROM execution_events ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        sequence = 1 if previous_row is None else previous_row["sequence"] + 1
        previous_sha256 = _ZERO_HASH if previous_row is None else previous_row["event_sha256"]
        payload_bytes = canonical_json_bytes(payload)
        payload_sha256 = sha256_bytes(payload_bytes)
        event_material = {
            "sequence": sequence, "entity_type": entity_type, "entity_id": entity_id,
            "event_type": event_type, "created_epoch": created_epoch,
            "previous_sha256": previous_sha256, "payload_sha256": payload_sha256,
        }
        event_sha256 = sha256_bytes(canonical_json_bytes(event_material))
        db.execute("""INSERT INTO execution_events(sequence,entity_type,entity_id,event_type,
            created_epoch,previous_sha256,payload,payload_sha256,event_sha256)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (sequence, entity_type, entity_id, event_type, created_epoch,
             previous_sha256, payload_bytes, payload_sha256, event_sha256))

    def reserve_attempt(self, plan: ExecutionPlan, attempt: AttemptPlan, pod_name: str) -> int:
        """Atomically persist immutable provenance, one slot and a cost exposure."""
        validate_attempt(plan, attempt)
        if (plan.project.project_id != self.project_id or
                plan.approval.controller_host != self.controller_host):
            raise ExecutionError(FailureCategory.AUTHORIZATION, "store_identity_mismatch")
        if (not isinstance(pod_name, str) or _POD_NAME.fullmatch(pod_name) is None or
                not any(pod_name.startswith(prefix) and len(pod_name) > len(prefix)
                        for prefix in plan.project.pod_name_prefixes)):
            raise ExecutionError(FailureCategory.AUTHORIZATION, "pod_name_outside_project")
        now = self._now()
        if (attempt.created_epoch > now or now >= attempt.deadline_epoch or
                now >= plan.approval.expires_epoch):
            raise ExecutionError(FailureCategory.CONFIGURATION, "attempt_time_invalid")
        scope = plan.approval
        if not isinstance(scope, ApprovalScope):
            raise ContractError("plan approval is malformed")
        self._ensure_sqlite_int(attempt.attempt_number, "attempt_number")
        self._ensure_sqlite_int(attempt.created_epoch, "attempt_created_epoch")
        self._ensure_sqlite_int(attempt.deadline_epoch, "attempt_deadline_epoch")
        self._ensure_sqlite_int(scope.expires_epoch, "approval_expiry")
        rate = self._ensure_sqlite_int(attempt.selected_resource.hourly_micro_usd, "hourly_rate")
        seconds = attempt.runtime_seconds + attempt.cleanup_reserve_seconds
        exposure = self._ensure_sqlite_int((rate * seconds + 3599) // 3600, "exposure")
        max_total = self._ensure_sqlite_int(scope.max_total_micro_usd, "total_cap")
        max_hourly = self._ensure_sqlite_int(scope.max_hourly_micro_usd, "hourly_cap")
        scope_document = encode_document("approval_scope", scope)
        policy_document = encode_document("project_policy", plan.project)
        plan_document = encode_document("execution_plan", plan)
        attempt_document = encode_document("attempt_plan", attempt)
        scope_hash = sha256_bytes(scope_document)
        policy_hash = sha256_bytes(policy_document)
        plan_hash = sha256_bytes(plan_document)

        with self._transaction() as db:
            db.execute("""INSERT OR IGNORE INTO execution_approvals(
                project_id,approval_id,scope_sha256,policy_sha256,document,
                max_total_micro_usd,max_hourly_micro_usd,max_active_resources,expires_epoch)
                VALUES (?,?,?,?,?,?,?,?,?)""",
                (plan.project.project_id, scope.approval_id, scope_hash, policy_hash,
                 scope_document, max_total, max_hourly, scope.max_active_resources,
                 scope.expires_epoch))
            stored_scope = db.execute("""SELECT scope_sha256,policy_sha256 FROM execution_approvals
                WHERE project_id=? AND approval_id=?""",
                (plan.project.project_id, scope.approval_id)).fetchone()
            if stored_scope is None or stored_scope["scope_sha256"] != scope_hash or stored_scope["policy_sha256"] != policy_hash:
                raise ExecutionError(FailureCategory.AUTHORIZATION, "approval_scope_changed")

            db.execute("""INSERT OR IGNORE INTO execution_jobs(project_id,job_id,approval_id)
                VALUES (?,?,?)""", (plan.project.project_id, plan.job_id, scope.approval_id))
            job = db.execute("SELECT approval_id FROM execution_jobs WHERE project_id=? AND job_id=?",
                             (plan.project.project_id, plan.job_id)).fetchone()
            if job is None or job["approval_id"] != scope.approval_id:
                raise ExecutionError(FailureCategory.AUTHORIZATION, "job_approval_changed")
            db.execute("INSERT OR IGNORE INTO execution_plans(plan_sha256,project_id,job_id,document) VALUES (?,?,?,?)",
                       (plan_hash, plan.project.project_id, plan.job_id, plan_document))
            stored_plan = db.execute("SELECT document FROM execution_plans WHERE plan_sha256=?",
                                     (plan_hash,)).fetchone()
            if stored_plan is None or bytes(stored_plan["document"]) != plan_document:
                raise ExecutionError(FailureCategory.INTERNAL, "plan_digest_collision")

            duplicate = db.execute("""SELECT attempt_sha256,attempt_document,pod_name,
                exposure_micro_usd FROM execution_attempts WHERE attempt_id=?""",
                                   (attempt.attempt_id,)).fetchone()
            if duplicate:
                if (duplicate["attempt_sha256"] == sha256_bytes(attempt_document) and
                        bytes(duplicate["attempt_document"]) == attempt_document and
                        duplicate["pod_name"] == pod_name):
                    return duplicate["exposure_micro_usd"]
                raise ExecutionError(FailureCategory.CONFIGURATION, "attempt_id_conflict")

            active_rows = db.execute(f"""SELECT project_max_active_resources
                FROM execution_attempts WHERE project_id=? AND state IN {_ACTIVE_SQL}""",
                (plan.project.project_id,)).fetchall()
            slot_limit = min([plan.project.max_active_resources] +
                             [row["project_max_active_resources"] for row in active_rows])
            if len(active_rows) + 1 > slot_limit:
                raise ExecutionError(FailureCategory.CONFIGURATION, "project_slots_exhausted")
            scope_active = db.execute(f"""SELECT COUNT(*) FROM execution_attempts
                WHERE project_id=? AND approval_id=? AND state IN {_ACTIVE_SQL}""",
                (plan.project.project_id, scope.approval_id)).fetchone()[0]
            if scope_active + 1 > scope.max_active_resources:
                raise ExecutionError(FailureCategory.CONFIGURATION, "approval_slots_exhausted")
            active_hourly = self._sum([row["hourly_micro_usd"] for row in db.execute(
                f"""SELECT hourly_micro_usd FROM execution_attempts
                    WHERE project_id=? AND approval_id=? AND state IN {_ACTIVE_SQL}""",
                (plan.project.project_id, scope.approval_id)).fetchall()])
            if active_hourly + rate > max_hourly:
                raise ExecutionError(FailureCategory.CONFIGURATION, "hourly_cap_exceeded")

            bill_rows = db.execute("""SELECT b.amount_micro_usd FROM execution_billing b
                JOIN execution_attempts a ON a.attempt_id=b.attempt_id
                WHERE a.project_id=? AND a.approval_id=? AND NOT EXISTS (
                    SELECT 1 FROM execution_billing successor
                    WHERE successor.supersedes_observation_id=b.observation_id)""",
                (plan.project.project_id, scope.approval_id)).fetchall()
            billed = self._sum([row["amount_micro_usd"] for row in bill_rows])
            exposures = db.execute("""SELECT exposure_micro_usd FROM execution_attempts
                WHERE project_id=? AND approval_id=?""",
                (plan.project.project_id, scope.approval_id)).fetchall()
            reserved = self._sum([row["exposure_micro_usd"] for row in exposures])
            if billed + reserved + exposure > max_total:
                raise ExecutionError(FailureCategory.CONFIGURATION, "total_cap_exceeded")

            try:
                db.execute("""INSERT INTO execution_attempts(
                    attempt_id,project_id,job_id,approval_id,plan_sha256,attempt_sha256,
                    attempt_document,attempt_number,pod_name,pod_id,state,create_claimed,
                    project_max_active_resources,approval_max_active_resources,hourly_micro_usd,
                    initial_exposure_micro_usd,exposure_micro_usd,billing_complete,
                    billing_complete_evidence_sha256,billing_complete_covered_start_epoch,
                    billing_complete_covered_end_epoch,create_claimed_epoch,terminated_epoch,
                    created_epoch,deadline_epoch)
                    VALUES (?,?,?,?,?,?,?,?,?,NULL,'reserved',0,?,?,?,?,?,0,NULL,NULL,NULL,NULL,NULL,?,?)""",
                    (attempt.attempt_id, plan.project.project_id, plan.job_id, scope.approval_id,
                     plan_hash, sha256_bytes(attempt_document), attempt_document,
                     attempt.attempt_number, pod_name, plan.project.max_active_resources,
                     scope.max_active_resources, rate, exposure, exposure,
                     attempt.created_epoch, attempt.deadline_epoch))
            except sqlite3.IntegrityError:
                raise ExecutionError(FailureCategory.CONFIGURATION, "attempt_scope_conflict") from None
            self._append_event(db, "attempt", attempt.attempt_id, "reserved", now,
                               {"project_id": plan.project.project_id, "job_id": plan.job_id,
                                "approval_id": scope.approval_id, "pod_name": pod_name,
                                "plan_sha256": plan_hash,
                                "attempt_sha256": sha256_bytes(attempt_document),
                                "exposure_micro_usd": exposure,
                                "hourly_micro_usd": rate})
        return exposure

    def claim_create(self, attempt_id: str) -> None:
        """Consume the sole create claim before the caller sends one request."""
        now = self._now()
        with self._transaction() as db:
            row = db.execute("""SELECT a.state,a.create_claimed,a.deadline_epoch,
                s.expires_epoch FROM execution_attempts a JOIN execution_approvals s
                ON s.project_id=a.project_id AND s.approval_id=a.approval_id
                WHERE a.attempt_id=?""", (attempt_id,)).fetchone()
            if row is None or row["state"] != "reserved" or row["create_claimed"] != 0:
                raise ExecutionError(FailureCategory.AMBIGUOUS_OPERATION, "create_claim_unavailable")
            if now >= row["deadline_epoch"] or now >= row["expires_epoch"]:
                raise ExecutionError(FailureCategory.CONFIGURATION, "attempt_expired_before_create")
            db.execute("""UPDATE execution_attempts SET state='create_claimed',create_claimed=1,
                create_claimed_epoch=? WHERE attempt_id=?""", (now, attempt_id))
            self._append_event(db, "attempt", attempt_id, "create_claimed", now, {})

    def require_reconciliation(self, attempt_id: str, reason_code: str) -> None:
        if not isinstance(reason_code, str) or re.fullmatch(r"[a-z][a-z0-9_.-]{0,63}", reason_code) is None:
            raise ValueError("reason_code must be a sanitized identifier")
        now = self._now()
        with self._transaction() as db:
            row = db.execute("SELECT state,create_claimed FROM execution_attempts WHERE attempt_id=?",
                             (attempt_id,)).fetchone()
            if row is None or row["state"] not in (
                    "create_claimed", "active", "stopping", "reconciliation_required") or row["create_claimed"] != 1:
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED, "attempt_not_reconcilable")
            if row["state"] == "reconciliation_required":
                latest = db.execute("""SELECT payload FROM execution_events
                    WHERE entity_type='attempt' AND entity_id=? AND event_type='reconciliation_required'
                    ORDER BY sequence DESC LIMIT 1""", (attempt_id,)).fetchone()
                if latest is not None and strict_json_loads(bytes(latest["payload"])) == {
                        "reason_code": reason_code}:
                    return
            db.execute("UPDATE execution_attempts SET state='reconciliation_required' WHERE attempt_id=?",
                       (attempt_id,))
            self._append_event(db, "attempt", attempt_id, "reconciliation_required", now,
                               {"reason_code": reason_code})

    def bind_pod(self, attempt_id: str, pod_id: str, pod_name: str) -> None:
        if not isinstance(pod_id, str) or _POD_ID.fullmatch(pod_id) is None:
            raise ExecutionError(FailureCategory.RESOURCE_MISMATCH, "provider_pod_id_invalid")
        now = self._now()
        with self._transaction() as db:
            row = db.execute("""SELECT a.project_id,a.pod_name,a.pod_id,a.state,a.create_claimed,
                p.document FROM execution_attempts a JOIN execution_plans p ON p.plan_sha256=a.plan_sha256
                WHERE a.attempt_id=?""", (attempt_id,)).fetchone()
            if row is None or row["create_claimed"] != 1 or row["state"] not in (
                    "create_claimed", "reconciliation_required", "active"):
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED, "attempt_not_bindable")
            plan_payload = decode_document(bytes(row["document"]), expected_kind="execution_plan")
            project = plan_payload.get("project")
            prefixes = project.get("pod_name_prefixes") if isinstance(project, dict) else None
            if (pod_name != row["pod_name"] or not isinstance(prefixes, list) or not any(
                    isinstance(prefix, str) and pod_name.startswith(prefix) and len(pod_name) > len(prefix)
                    for prefix in prefixes)):
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED, "provider_pod_identity_mismatch")
            if row["pod_id"] not in (None, pod_id):
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED, "provider_pod_id_changed")
            if row["pod_id"] is None or row["state"] == "reconciliation_required":
                db.execute("UPDATE execution_attempts SET pod_id=?,state='active' WHERE attempt_id=?",
                           (pod_id, attempt_id))
                self._append_event(db, "attempt", attempt_id,
                                   "pod_bound" if row["pod_id"] is None else "pod_reconciled_active", now,
                                   {"provider_pod_id": pod_id, "pod_name": pod_name})

    def confirm_absence(self, attempt_id: str, evidence_sha256: str) -> None:
        """Record caller-verified absence after exact provider reconciliation.

        The caller must enforce repeated, scoped provider observations before this
        method releases a never-bound create hold or records termination.
        """
        if not isinstance(evidence_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", evidence_sha256) is None:
            raise ValueError("A SHA-256 absence evidence digest is required")
        now = self._now()
        with self._transaction() as db:
            row = db.execute("""SELECT pod_id,state,create_claimed FROM execution_attempts
                WHERE attempt_id=?""", (attempt_id,)).fetchone()
            if row is None:
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                     "attempt_not_reconcilable")
            if row["state"] in ("cancelled", "terminated"):
                terminal_event = "create_absence_confirmed" if row["pod_id"] is None else "termination_absence_confirmed"
                prior = db.execute("""SELECT payload FROM execution_events WHERE entity_type='attempt'
                    AND entity_id=? AND event_type=? ORDER BY sequence DESC LIMIT 1""",
                                   (attempt_id, terminal_event)).fetchone()
                if prior is not None and strict_json_loads(bytes(prior["payload"])) == {
                        "provider_pod_id": row["pod_id"], "evidence_sha256": evidence_sha256}:
                    return
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                     "attempt_not_reconcilable")
            if row["state"] != "reconciliation_required" or row["create_claimed"] != 1:
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                     "attempt_not_reconcilable")
            if row["pod_id"] is None:
                db.execute("""UPDATE execution_attempts SET state='cancelled',billing_complete=1,
                    exposure_micro_usd=0 WHERE attempt_id=?""", (attempt_id,))
                outcome = "create_absence_confirmed"
            else:
                db.execute("""UPDATE execution_attempts SET state='terminated',terminated_epoch=?
                    WHERE attempt_id=?""", (now, attempt_id))
                outcome = "termination_absence_confirmed"
            self._append_event(db, "attempt", attempt_id, outcome, now,
                               {"provider_pod_id": row["pod_id"],
                                "evidence_sha256": evidence_sha256})

    def request_stop(self, attempt_id: str) -> None:
        now = self._now()
        with self._transaction() as db:
            state = db.execute("SELECT state FROM execution_attempts WHERE attempt_id=?",
                                (attempt_id,)).fetchone()
            if state is not None and state["state"] == "stopping":
                return
            changed = db.execute("UPDATE execution_attempts SET state='stopping' WHERE attempt_id=? AND state='active'",
                                 (attempt_id,)).rowcount
            if changed != 1:
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED, "attempt_not_active")
            self._append_event(db, "attempt", attempt_id, "stop_requested", now, {})

    def confirm_termination(self, attempt_id: str, pod_id: str, evidence_sha256: str) -> None:
        if not isinstance(evidence_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", evidence_sha256) is None:
            raise ValueError("A SHA-256 termination evidence digest is required")
        now = self._now()
        with self._transaction() as db:
            row = db.execute("SELECT pod_id,state FROM execution_attempts WHERE attempt_id=?",
                             (attempt_id,)).fetchone()
            if row is not None and row["state"] == "terminated" and row["pod_id"] == pod_id:
                prior = db.execute("""SELECT payload FROM execution_events WHERE entity_type='attempt'
                    AND entity_id=? AND event_type='terminated' ORDER BY sequence DESC LIMIT 1""",
                                   (attempt_id,)).fetchone()
                if prior is not None and strict_json_loads(bytes(prior["payload"])) == {
                        "provider_pod_id": pod_id, "evidence_sha256": evidence_sha256}:
                    return
            changed = db.execute("""UPDATE execution_attempts SET state='terminated',terminated_epoch=?
                WHERE attempt_id=? AND pod_id=? AND state='stopping'""",
                (now, attempt_id, pod_id)).rowcount
            if changed != 1:
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED, "termination_identity_mismatch")
            self._append_event(db, "attempt", attempt_id, "terminated", now,
                               {"provider_pod_id": pod_id, "evidence_sha256": evidence_sha256})

    def cancel_uncreated(self, attempt_id: str) -> None:
        now = self._now()
        with self._transaction() as db:
            state = db.execute("SELECT state,create_claimed FROM execution_attempts WHERE attempt_id=?",
                                (attempt_id,)).fetchone()
            if state is not None and state["state"] == "cancelled" and state["create_claimed"] == 0:
                return
            changed = db.execute("""UPDATE execution_attempts SET state='cancelled',
                exposure_micro_usd=0,billing_complete=1 WHERE attempt_id=?
                AND state='reserved' AND create_claimed=0""", (attempt_id,)).rowcount
            if changed != 1:
                raise ExecutionError(FailureCategory.AMBIGUOUS_OPERATION, "create_may_have_been_sent")
            self._append_event(db, "attempt", attempt_id, "cancelled_before_create", now, {})

    def record_billing_observation(self, observation: BillingObservation) -> None:
        if not isinstance(observation, BillingObservation):
            raise ContractError("observation must be a BillingObservation")
        document = encode_document("billing_observation", observation)
        now = self._now()
        with self._transaction() as db:
            attempt = db.execute("""SELECT project_id,approval_id,pod_id,billing_complete,
                initial_exposure_micro_usd,exposure_micro_usd FROM execution_attempts
                WHERE attempt_id=?""", (observation.attempt_id,)).fetchone()
            if (attempt is None or attempt["project_id"] != observation.project_id or
                    attempt["pod_id"] != observation.provider_pod_id):
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                     "billing_attempt_identity_mismatch")
            existing = db.execute("SELECT document FROM execution_billing WHERE observation_id=?",
                                  (observation.observation_id,)).fetchone()
            if existing is not None:
                if bytes(existing["document"]) == document:
                    return
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                     "billing_observation_conflict")
            if observation.amount_micro_usd > SQLITE_MAX_INTEGER:
                raise ExecutionError(FailureCategory.CONFIGURATION, "billing_amount_out_of_range")
            for field in ("period_start_epoch", "period_end_epoch", "observed_epoch"):
                self._ensure_sqlite_int(getattr(observation, field), f"billing_{field}")
            if observation.supersedes_observation_id is not None:
                previous = db.execute("""SELECT attempt_id,project_id,provider_pod_id,
                    period_start_epoch,period_end_epoch FROM execution_billing
                    WHERE observation_id=?""", (observation.supersedes_observation_id,)).fetchone()
                already_replaced = db.execute("""SELECT 1 FROM execution_billing
                    WHERE supersedes_observation_id=?""",
                    (observation.supersedes_observation_id,)).fetchone()
                if (previous is None or already_replaced or previous["attempt_id"] != observation.attempt_id or
                        previous["project_id"] != observation.project_id or
                        previous["provider_pod_id"] != observation.provider_pod_id or
                        previous["period_start_epoch"] != observation.period_start_epoch or
                        previous["period_end_epoch"] != observation.period_end_epoch):
                    raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                         "billing_adjustment_identity_mismatch")
            else:
                overlapping = db.execute("""SELECT 1 FROM execution_billing b
                    WHERE b.attempt_id=? AND NOT EXISTS (
                      SELECT 1 FROM execution_billing s WHERE s.supersedes_observation_id=b.observation_id)
                      AND ? < b.period_end_epoch AND b.period_start_epoch < ? LIMIT 1""",
                    (observation.attempt_id, observation.period_start_epoch,
                     observation.period_end_epoch)).fetchone()
                if overlapping:
                    raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                         "billing_period_overlap")
            try:
                db.execute("""INSERT INTO execution_billing(observation_id,attempt_id,project_id,
                    provider_pod_id,amount_micro_usd,period_start_epoch,period_end_epoch,
                    observed_epoch,evidence_sha256,supersedes_observation_id,document)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (observation.observation_id, observation.attempt_id, observation.project_id,
                     observation.provider_pod_id, observation.amount_micro_usd,
                     observation.period_start_epoch, observation.period_end_epoch,
                     observation.observed_epoch, observation.evidence_sha256,
                     observation.supersedes_observation_id, document))
            except sqlite3.IntegrityError:
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                     "billing_observation_conflict") from None
            resumed_exposure = (attempt["initial_exposure_micro_usd"]
                                if attempt["billing_complete"] else attempt["exposure_micro_usd"])
            db.execute("""UPDATE execution_attempts SET billing_complete=0,
                billing_complete_evidence_sha256=NULL,billing_complete_covered_start_epoch=NULL,
                billing_complete_covered_end_epoch=NULL,exposure_micro_usd=? WHERE attempt_id=?""",
                (resumed_exposure, observation.attempt_id))
            self._append_event(db, "billing", observation.observation_id, "observed", now,
                               {"attempt_id": observation.attempt_id,
                                "provider_pod_id": observation.provider_pod_id,
                                "amount_micro_usd": observation.amount_micro_usd,
                                "evidence_sha256": observation.evidence_sha256,
                                "supersedes_observation_id": observation.supersedes_observation_id})

    def mark_billing_complete(self, attempt_id: str, evidence_sha256: str,
                              covered_start_epoch: int, covered_end_epoch: int) -> None:
        if not isinstance(evidence_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", evidence_sha256) is None:
            raise ValueError("A SHA-256 billing evidence digest is required")
        if (type(covered_start_epoch) is not int or type(covered_end_epoch) is not int or
                covered_start_epoch < 0 or covered_end_epoch < covered_start_epoch or
                covered_end_epoch > SQLITE_MAX_INTEGER):
            raise ValueError("A valid billing coverage interval is required")
        now = self._now()
        with self._transaction() as db:
            row = db.execute("""SELECT state,billing_complete,billing_complete_evidence_sha256,
                billing_complete_covered_start_epoch,billing_complete_covered_end_epoch,
                create_claimed_epoch,terminated_epoch
                FROM execution_attempts WHERE attempt_id=?""",
                             (attempt_id,)).fetchone()
            if row is None or row["state"] != "terminated":
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                     "attempt_not_terminated_for_billing")
            if (row["create_claimed_epoch"] is None or row["terminated_epoch"] is None or
                    covered_start_epoch > row["create_claimed_epoch"] or
                    covered_end_epoch < row["terminated_epoch"]):
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                     "billing_coverage_incomplete")
            observed_period = db.execute("""SELECT MIN(period_start_epoch),MAX(period_end_epoch)
                FROM execution_billing WHERE attempt_id=?""", (attempt_id,)).fetchone()
            if (observed_period[0] is not None and
                    (covered_start_epoch > observed_period[0] or
                     covered_end_epoch < observed_period[1])):
                raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                     "billing_coverage_incomplete")
            if row["billing_complete"] == 1:
                if (row["billing_complete_evidence_sha256"] != evidence_sha256 or
                        row["billing_complete_covered_start_epoch"] != covered_start_epoch or
                        row["billing_complete_covered_end_epoch"] != covered_end_epoch):
                    raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                         "billing_completion_evidence_changed")
                return
            db.execute("""UPDATE execution_attempts SET billing_complete=1,
                exposure_micro_usd=0,billing_complete_evidence_sha256=?,
                billing_complete_covered_start_epoch=?,billing_complete_covered_end_epoch=?
                WHERE attempt_id=?""",
                (evidence_sha256, covered_start_epoch, covered_end_epoch, attempt_id))
            self._append_event(db, "attempt", attempt_id, "billing_marked_complete", now,
                               {"evidence_sha256": evidence_sha256,
                                "covered_start_epoch": covered_start_epoch,
                                "covered_end_epoch": covered_end_epoch})

    def accounting_snapshot(self, project_id: str, approval_id: str) -> AccountingSnapshot:
        if project_id != self.project_id:
            raise ExecutionError(FailureCategory.AUTHORIZATION, "store_identity_mismatch")
        with closing(self._connect()) as db:
            db.execute("BEGIN")
            try:
                scope = db.execute("""SELECT max_total_micro_usd,max_hourly_micro_usd
                    FROM execution_approvals WHERE project_id=? AND approval_id=?""",
                    (project_id, approval_id)).fetchone()
                if scope is None:
                    raise ExecutionError(FailureCategory.CONFIGURATION, "unknown_approval_scope")
                bills = db.execute("""SELECT b.amount_micro_usd FROM execution_billing b
                    JOIN execution_attempts a ON a.attempt_id=b.attempt_id
                    WHERE a.project_id=? AND a.approval_id=? AND NOT EXISTS (
                        SELECT 1 FROM execution_billing s
                        WHERE s.supersedes_observation_id=b.observation_id)""",
                    (project_id, approval_id)).fetchall()
                exposures = db.execute("""SELECT exposure_micro_usd FROM execution_attempts
                    WHERE project_id=? AND approval_id=?""", (project_id, approval_id)).fetchall()
                active = db.execute(f"""SELECT hourly_micro_usd FROM execution_attempts
                    WHERE project_id=? AND approval_id=? AND state IN {_ACTIVE_SQL}""",
                    (project_id, approval_id)).fetchall()
                db.commit()
            except BaseException:
                db.rollback()
                raise
        billed = self._sum([row["amount_micro_usd"] for row in bills])
        reserved = self._sum([row["exposure_micro_usd"] for row in exposures])
        committed = self._sum([billed, reserved])
        active_hourly = self._sum([row["hourly_micro_usd"] for row in active])
        return AccountingSnapshot(
            project_id, approval_id, scope["max_total_micro_usd"],
            scope["max_hourly_micro_usd"], billed, reserved, committed,
            max(0, scope["max_total_micro_usd"] - committed), len(active), active_hourly)

    def get_attempt(self, attempt_id: str) -> dict[str, Any]:
        with closing(self._connect()) as db:
            row = db.execute("""SELECT attempt_id,project_id,job_id,approval_id,attempt_number,
                attempt_sha256,pod_name,pod_id,state,create_claimed,hourly_micro_usd,
                initial_exposure_micro_usd,exposure_micro_usd,billing_complete,
                billing_complete_evidence_sha256,billing_complete_covered_start_epoch,
                billing_complete_covered_end_epoch,create_claimed_epoch,terminated_epoch,
                created_epoch,deadline_epoch
                FROM execution_attempts WHERE attempt_id=? AND project_id=?""",
                             (attempt_id, self.project_id)).fetchone()
        if row is None:
            raise ExecutionError(FailureCategory.CONFIGURATION, "unknown_attempt")
        return dict(row)

    def verify_attempt_lifecycle_projection(self, attempt_id: str) -> bool:
        """Check mutable lifecycle state against the authenticated event history.

        Recovery and cleanup code should call this before using a stored Pod ID.
        It detects accidental edits to mutable attempt state and identity without
        treating a caller-supplied ledger row as sufficient provider authority.
        """
        with closing(self._connect()) as db:
            db.execute("BEGIN")
            row = db.execute("""SELECT a.*,p.document AS plan_document
                FROM execution_attempts a JOIN execution_plans p
                  ON p.plan_sha256=a.plan_sha256
                WHERE a.attempt_id=? AND a.project_id=?""",
                (attempt_id, self.project_id)).fetchone()
            event_rows = db.execute(
                "SELECT * FROM execution_events ORDER BY sequence").fetchall()
            db.commit()
        if row is None:
            raise ExecutionError(FailureCategory.CONFIGURATION, "unknown_attempt")
        self._verify_event_rows(event_rows)
        if (sha256_bytes(bytes(row["attempt_document"])) != row["attempt_sha256"] or
                sha256_bytes(bytes(row["plan_document"])) != row["plan_sha256"]):
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "attempt_projection_mismatch")

        attempt_events = [event for event in event_rows
                          if event["entity_type"] == "attempt" and
                          event["entity_id"] == attempt_id]
        expected_state = None
        expected_claimed = 0
        expected_claimed_epoch = None
        expected_pod_id = None
        expected_terminated_epoch = None

        def mismatch():
            raise ExecutionError(FailureCategory.RECONCILIATION_REQUIRED,
                                 "attempt_projection_mismatch")

        for index, event in enumerate(attempt_events):
            try:
                payload = strict_json_loads(bytes(event["payload"]))
            except (TypeError, ValueError):
                mismatch()
            if not isinstance(payload, dict):
                mismatch()
            event_type = event["event_type"]
            if event_type == "reserved":
                if index != 0 or expected_state is not None:
                    mismatch()
                if (payload.get("project_id") != row["project_id"] or
                        payload.get("job_id") != row["job_id"] or
                        payload.get("approval_id") != row["approval_id"] or
                        payload.get("plan_sha256") != row["plan_sha256"] or
                        payload.get("attempt_sha256") != row["attempt_sha256"] or
                        payload.get("pod_name") != row["pod_name"] or
                        payload.get("exposure_micro_usd") != row["initial_exposure_micro_usd"] or
                        payload.get("hourly_micro_usd") != row["hourly_micro_usd"]):
                    mismatch()
                expected_state = "reserved"
            elif event_type == "create_claimed":
                if expected_state != "reserved" or expected_claimed:
                    mismatch()
                expected_state = "create_claimed"
                expected_claimed = 1
                expected_claimed_epoch = event["created_epoch"]
            elif event_type == "reconciliation_required":
                if not expected_claimed or expected_state not in (
                        "create_claimed", "active", "stopping", "reconciliation_required"):
                    mismatch()
                expected_state = "reconciliation_required"
            elif event_type == "pod_bound":
                pod_id = payload.get("provider_pod_id")
                if (expected_state not in ("create_claimed", "reconciliation_required") or
                        expected_pod_id is not None or not isinstance(pod_id, str) or
                        payload.get("pod_name") != row["pod_name"]):
                    mismatch()
                expected_pod_id = pod_id
                expected_state = "active"
            elif event_type == "pod_reconciled_active":
                if (expected_state != "reconciliation_required" or expected_pod_id is None or
                        payload.get("provider_pod_id") != expected_pod_id or
                        payload.get("pod_name") != row["pod_name"]):
                    mismatch()
                expected_state = "active"
            elif event_type == "stop_requested":
                if expected_state != "active":
                    mismatch()
                expected_state = "stopping"
            elif event_type == "terminated":
                if (expected_state != "stopping" or expected_pod_id is None or
                        payload.get("provider_pod_id") != expected_pod_id):
                    mismatch()
                expected_state = "terminated"
                expected_terminated_epoch = event["created_epoch"]
            elif event_type == "termination_absence_confirmed":
                if (expected_state != "reconciliation_required" or expected_pod_id is None or
                        payload.get("provider_pod_id") != expected_pod_id):
                    mismatch()
                expected_state = "terminated"
                expected_terminated_epoch = event["created_epoch"]
            elif event_type == "create_absence_confirmed":
                if (expected_state != "reconciliation_required" or expected_pod_id is not None or
                        payload.get("provider_pod_id") is not None):
                    mismatch()
                expected_state = "cancelled"
            elif event_type == "cancelled_before_create":
                if expected_state != "reserved" or expected_claimed:
                    mismatch()
                expected_state = "cancelled"
            elif event_type == "billing_marked_complete":
                if expected_state != "terminated" or expected_pod_id is None:
                    mismatch()
            else:
                mismatch()

        if not attempt_events:
            mismatch()
        if (row["state"] != expected_state or row["create_claimed"] != expected_claimed or
                row["create_claimed_epoch"] != expected_claimed_epoch or
                row["pod_id"] != expected_pod_id or
                row["terminated_epoch"] != expected_terminated_epoch):
            mismatch()
        return True

    def events(self, *, after_sequence: int = 0, limit: int = 1000) -> tuple[dict[str, Any], ...]:
        if (type(after_sequence) is not int or after_sequence < 0 or
                after_sequence > SQLITE_MAX_INTEGER):
            raise ValueError("after_sequence must be a nonnegative integer")
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        with closing(self._connect()) as db:
            rows = db.execute("""SELECT sequence,entity_type,entity_id,event_type,
                created_epoch,previous_sha256,payload,payload_sha256,event_sha256
                FROM execution_events WHERE sequence>? ORDER BY sequence LIMIT ?""",
                (after_sequence, limit)).fetchall()
        return tuple({
            "sequence": row["sequence"], "entity_type": row["entity_type"],
            "entity_id": row["entity_id"], "event_type": row["event_type"],
            "created_epoch": row["created_epoch"],
            "previous_sha256": row["previous_sha256"],
            "payload": strict_json_loads(bytes(row["payload"])),
            "payload_sha256": row["payload_sha256"],
            "event_sha256": row["event_sha256"],
        } for row in rows)

    @staticmethod
    def _verify_event_rows(rows: list[sqlite3.Row]) -> bool:
        previous = _ZERO_HASH
        for expected_sequence, row in enumerate(rows, start=1):
            payload = bytes(row["payload"])
            if sha256_bytes(payload) != row["payload_sha256"]:
                raise ExecutionError(FailureCategory.INTERNAL, "event_payload_hash_mismatch")
            if row["sequence"] != expected_sequence or row["previous_sha256"] != previous:
                raise ExecutionError(FailureCategory.INTERNAL, "event_chain_order_mismatch")
            material = {
                "sequence": row["sequence"], "entity_type": row["entity_type"],
                "entity_id": row["entity_id"], "event_type": row["event_type"],
                "created_epoch": row["created_epoch"],
                "previous_sha256": row["previous_sha256"],
                "payload_sha256": row["payload_sha256"],
            }
            computed = sha256_bytes(canonical_json_bytes(material))
            if computed != row["event_sha256"]:
                raise ExecutionError(FailureCategory.INTERNAL, "event_hash_mismatch")
            previous = row["event_sha256"]
        return True

    def verify_event_chain(self) -> bool:
        with closing(self._connect()) as db:
            rows = db.execute("SELECT * FROM execution_events ORDER BY sequence").fetchall()
        return self._verify_event_rows(rows)
