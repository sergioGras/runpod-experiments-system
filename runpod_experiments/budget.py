"""Persistent round-level accounting; not a provider billing limit."""
from decimal import Decimal, InvalidOperation
from contextlib import closing
import sqlite3


def _micro(value):
    try:
        if isinstance(value, (bool, float)):
            raise ValueError("Use a decimal string or Decimal for USD")
        amount = Decimal(value)
        scaled = amount * 1_000_000
        if not amount.is_finite() or amount < 0 or scaled != scaled.to_integral_value():
            raise ValueError("USD must be nonnegative with at most six decimal places")
        return int(scaled)
    except (InvalidOperation, TypeError, OverflowError) as error:
        raise ValueError("Invalid USD amount") from error


class BudgetLedger:
    def __init__(self, path, max_usd):
        cap = _micro(max_usd)
        if cap <= 0:
            raise ValueError("Maximum must be positive")
        self.path = str(path)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("CREATE TABLE IF NOT EXISTS round_budget (id INTEGER PRIMARY KEY CHECK(id=1), cap INTEGER NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS charges (charge_id TEXT PRIMARY KEY, micro_usd INTEGER NOT NULL, note TEXT NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS exposures (attempt_id TEXT PRIMARY KEY, micro_usd INTEGER NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS settlements (attempt_id TEXT PRIMARY KEY, charge_id TEXT NOT NULL UNIQUE)")
            db.execute("INSERT OR IGNORE INTO round_budget VALUES (1, ?)", (cap,))
            if db.execute("SELECT cap FROM round_budget WHERE id=1").fetchone()[0] != cap:
                raise ValueError("Existing round has a different maximum")

    def record_charge(self, charge_id, amount_usd, note=""):
        if not isinstance(charge_id, str) or not charge_id or not isinstance(note, str):
            raise ValueError("Charge ID and note must be strings")
        amount = _micro(amount_usd)
        with closing(sqlite3.connect(self.path)) as db, db:
            previous = db.execute("SELECT micro_usd, note FROM charges WHERE charge_id=?", (charge_id,)).fetchone()
            if previous is not None:
                if previous != (amount, note):
                    raise ValueError("Charge ID already has different details")
                return
            db.execute("INSERT INTO charges VALUES (?, ?, ?)", (charge_id, amount, note))

    def remaining(self, outstanding_usd="0"):
        outstanding = _micro(outstanding_usd)
        with closing(sqlite3.connect(self.path)) as db, db:
            cap = db.execute("SELECT cap FROM round_budget WHERE id=1").fetchone()[0]
            spent = db.execute("SELECT COALESCE(SUM(micro_usd), 0) FROM charges").fetchone()[0]
            reserved = db.execute("SELECT COALESCE(SUM(micro_usd), 0) FROM exposures").fetchone()[0]
        return Decimal(max(0, cap - spent - reserved - outstanding)) / Decimal(1_000_000)

    def reserve_exposure(self, attempt_id, amount_usd, outstanding_usd="0"):
        """Atomically reserve estimated unbilled costs before a create request."""
        amount, outstanding = _micro(amount_usd), _micro(outstanding_usd)
        if not isinstance(attempt_id, str) or not attempt_id or amount <= 0:
            raise ValueError("Invalid attempt or exposure")
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            existing = db.execute("SELECT micro_usd FROM exposures WHERE attempt_id=?", (attempt_id,)).fetchone()
            if existing is not None:
                raise ValueError("Attempt already has an exposure reservation")
            cap = db.execute("SELECT cap FROM round_budget WHERE id=1").fetchone()[0]
            spent = db.execute("SELECT COALESCE(SUM(micro_usd),0) FROM charges").fetchone()[0]
            reserved = db.execute("SELECT COALESCE(SUM(micro_usd),0) FROM exposures").fetchone()[0]
            if spent + reserved + outstanding + amount > cap:
                raise ValueError("Estimated exposure exceeds remaining round maximum")
            db.execute("INSERT INTO exposures VALUES (?,?)", (attempt_id, amount))

    def release_uncreated(self, attempt_id):
        """Release only when caller has proven no create request was sent."""
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("DELETE FROM exposures WHERE attempt_id=?", (attempt_id,))

    def settle(self, attempt_id, charge_id, amount_usd, note=""):
        """Record reconciled charges and release exposure in one transaction."""
        amount = _micro(amount_usd)
        if not all(isinstance(x, str) and x for x in (attempt_id, charge_id)) or not isinstance(note, str):
            raise ValueError("Invalid reconciliation details")
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            settlement = db.execute("SELECT charge_id FROM settlements WHERE attempt_id=?", (attempt_id,)).fetchone()
            exposure = db.execute("SELECT micro_usd FROM exposures WHERE attempt_id=?", (attempt_id,)).fetchone()
            if (settlement is not None and settlement[0] != charge_id) or (settlement is None and exposure is None):
                raise ValueError("Attempt has no matching exposure or settlement")
            previous = db.execute("SELECT micro_usd, note FROM charges WHERE charge_id=?", (charge_id,)).fetchone()
            if previous is not None and previous != (amount, note):
                raise ValueError("Charge ID already has different details")
            if previous is None:
                db.execute("INSERT INTO charges VALUES (?,?,?)", (charge_id, amount, note))
            if settlement is None:
                db.execute("INSERT INTO settlements VALUES (?,?)", (attempt_id, charge_id))
            db.execute("DELETE FROM exposures WHERE attempt_id=?", (attempt_id,))
