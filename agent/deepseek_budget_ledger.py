"""Durable, cross-process reservations. Never expire/refund uncertain spend automatically."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta
import json
import logging
from pathlib import Path
import sqlite3
import uuid

from agent.deepseek_budget_policy import (
    BudgetDenied, LIMIT, WARNING, NANO_USD, RATES, RATES_EXPIRE, MODES, local_day, utc_now, usage_cost,
)

logger = logging.getLogger(__name__)
SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS reservations (
 id TEXT PRIMARY KEY, day TEXT NOT NULL, account TEXT NOT NULL, model TEXT NOT NULL,
 output_cap INTEGER NOT NULL, reserved INTEGER NOT NULL CHECK(reserved>0),
 state TEXT NOT NULL DEFAULT 'held', cost INTEGER, finished_day TEXT,
 response_id TEXT, cache_hit INTEGER, cache_miss INTEGER, output INTEGER,
 rate_hit INTEGER NOT NULL, rate_miss INTEGER NOT NULL, rate_output INTEGER NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS receipt_once ON reservations(account, response_id)
 WHERE response_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS warnings (day TEXT PRIMARY KEY, allocated INTEGER NOT NULL);
"""


class Ledger:
    def __init__(self, path: Path, *, clock=utc_now, mode="enforce"):
        if not isinstance(mode, str) or mode not in MODES:
            raise BudgetDenied("DeepSeek policy: unsupported accounting mode")
        self.path = Path(path)
        self.clock = clock
        self.mode = mode

    @contextmanager
    def _transaction(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        try:
            db.execute("PRAGMA synchronous=FULL")
            # DDL is idempotent; BEGIN IMMEDIATE serializes budgets across all workers.
            db.executescript(SCHEMA)
            db.execute("BEGIN IMMEDIATE")
            today = local_day(self.clock())
            db.execute("INSERT OR IGNORE INTO metadata VALUES ('hold_day', ?)", (today,))
            db.execute("INSERT OR IGNORE INTO metadata VALUES ('version', '1')")
            yield db, today
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def _totals(db, day):
        # Unknown/in-flight calls carry forward across midnight. Settled calls spanning
        # dates count on EACH date (conservative attribution, not claimed daily charges).
        held = db.execute("SELECT COALESCE(SUM(reserved),0) FROM reservations "
                          "WHERE state='held' AND day<=?", (day,)).fetchone()[0]
        settled = db.execute("SELECT COALESCE(SUM(cost),0) FROM reservations "
                             "WHERE state='settled' AND day<=? AND finished_day>=?", (day, day)).fetchone()[0]
        return held, settled

    @staticmethod
    def _warning(db, day, allocated):
        if allocated < WARNING:
            return False
        return db.execute("INSERT OR IGNORE INTO warnings VALUES (?,?)", (day, allocated)).rowcount == 1

    def initialize(self) -> str:
        """Establish the first-day hold without making a request or authorizing spending."""
        with self._transaction() as (db, _today):
            return db.execute("SELECT value FROM metadata WHERE key='hold_day'").fetchone()[0]

    @staticmethod
    def _day_limit(metadata, today):
        """Never reinterpret unknown earlier charges as zero or reset existing reservations."""
        hold_day = metadata["hold_day"]
        if "transition" not in metadata:
            return LIMIT if today > hold_day else 0
        try:
            transition = json.loads(metadata["transition"])
            cents = transition["limit_cents"]
            through = transition["authorized_through_day"]
            if (transition["day"] != hold_day or type(cents) is not int or not 0 < cents <= 1000
                    or not isinstance(transition["authorization_ref"], str)
                    or not transition["authorization_ref"].strip()
                    or date.fromisoformat(through) not in (date.fromisoformat(hold_day),
                                                          date.fromisoformat(hold_day) + timedelta(days=1))):
                raise ValueError("invalid transition")
        except (ValueError, TypeError, KeyError):
            raise BudgetDenied("DeepSeek budget: transition record invalid; requests held") from None
        if today < hold_day or today > through:
            return 0
        return cents * 10_000_000 if today == hold_day else LIMIT

    def activate_transition(self, limit_cents: int, authorization_ref: str, authorized_through_day: str) -> str:
        """Record an already-authorized prospective allowance, NOT a spending approval.

        Only the initial accounting day can have one. Its cap and evidence reference are
        immutable; repetition is idempotent and never refreshes spent/reserved capacity.
        The caller must obtain action-specific authority before invoking this operation.
        """
        if (type(limit_cents) is not int or not 0 < limit_cents <= 1000
                or not isinstance(authorization_ref, str) or not authorization_ref.strip()
                or len(authorization_ref) > 200):
            raise BudgetDenied("DeepSeek budget: transition requires 1-1000 cents and an authorization reference")
        if not self.path.exists():
            raise BudgetDenied("DeepSeek budget: initialize the incomplete-day ledger first")
        with self._transaction() as (db, today):
            metadata = dict(db.execute("SELECT key,value FROM metadata"))
            if today != metadata["hold_day"] or self.clock() >= RATES_EXPIRE:
                raise BudgetDenied("DeepSeek budget: transition only applies to the current initial accounting day")
            if metadata.get("version") != "1" or "breach" in metadata:
                raise BudgetDenied("DeepSeek budget: accounting requires reconciliation")
            try:
                through = date.fromisoformat(authorized_through_day)
                if through not in (date.fromisoformat(today), date.fromisoformat(today) + timedelta(days=1)):
                    raise ValueError('outside the bounded transition window')
            except (TypeError, ValueError):
                raise BudgetDenied("DeepSeek budget: explicit authority ends on the initial day or following Pacific day") from None
            proposed = {"day": today, "limit_cents": limit_cents,
                        "authorization_ref": authorization_ref.strip(), "authorized_through_day": through.isoformat()}
            if "transition" in metadata:
                self._day_limit(metadata, today)
                if json.loads(metadata["transition"]) != proposed:
                    raise BudgetDenied("DeepSeek budget: transition is immutable; cannot raise or reset it")
            else:
                db.execute("INSERT INTO metadata VALUES ('transition', ?)", (json.dumps(proposed, sort_keys=True),))
                db.execute("INSERT INTO metadata VALUES ('transition_started_at', ?)", (self.clock().isoformat(),))
        return today

    def reserve(self, account: str, family: str, amount: int, output_cap: int) -> str:
        denied = None
        warning = False
        reservation = uuid.uuid4().hex  # every physical attempt, including SDK retries, is new
        with self._transaction() as (db, today):
            metadata = dict(db.execute("SELECT key,value FROM metadata"))
            limit = self._day_limit(metadata, today) if self.mode == "enforce" else None
            if metadata.get('version') != '1':
                denied = "ledger version unsupported"
            elif 'breach' in metadata:
                denied = "receipt exceeded its bound; reconciliation required"
            elif self.mode == "enforce" and limit == 0:
                denied = ("authorization window ended; further paid work requires explicit authority"
                          if 'transition' in metadata and today > metadata['hold_day']
                          else "initial accounting day incomplete; held until the next Pacific day")
            elif self.mode == "enforce" and self.clock() >= RATES_EXPIRE:
                denied = "verified rate snapshot expired; review current official pricing"
            held, settled = self._totals(db, today)
            allocated = held + settled
            if limit and allocated + amount > limit:
                denied = ("prospective transition allowance would be exceeded" if today == metadata['hold_day']
                          else "shared $10 daily allocation would be exceeded")
            if not denied:
                db.execute("INSERT INTO reservations(id,day,account,model,output_cap,reserved,rate_hit,rate_miss,rate_output) VALUES (?,?,?,?,?,?,?,?,?)",
                           (reservation, today, account, family, output_cap, amount, *RATES[family]))
                allocated += amount
            warning = self._warning(db, today, allocated)
        # Persist first: a crash cannot turn a lost log into a duplicate warning on restart.
        # The durable warnings table remains visible through read-only status if logging fails.
        if warning:
            if self.mode == "accounting_only":
                logger.warning("DeepSeek usage warning: Pacific day %s has recorded at least $8 "
                               "in estimates plus unresolved reservations; informational, not a spending cap. "
                               "Pricing snapshot stale: %s.", today, self.clock() >= RATES_EXPIRE)
            else:
                logger.warning("DeepSeek shared budget: Pacific day %s has allocated at least $8 of $10 "
                               "(settled upper estimates plus pending reservations).", today)
        if denied:
            raise BudgetDenied("DeepSeek budget: " + denied)
        return reservation

    def settle(self, reservation: str, response_id: str, usage: dict) -> bool:
        """Idempotent receipt; conflicts/unknown usage retain the full reservation."""
        if not isinstance(response_id, str) or not response_id or len(response_id) > 256:
            return False
        with self._transaction() as (db, today):
            row = db.execute("SELECT state,model,output_cap,reserved,account,day,rate_hit,rate_miss,rate_output FROM reservations WHERE id=?",
                             (reservation,)).fetchone()
            if row is None:
                raise BudgetDenied("DeepSeek budget: missing reservation")
            state, family, cap, bound, account, start_day, *rates = row
            if state == 'settled':
                return True
            try:
                cost, hit, miss, output = usage_cost(family, usage, cap, rates=rates)
            except (BudgetDenied, TypeError, AttributeError):
                # Any evidence beyond a bound is an invariant violation, not a refund.
                if isinstance(usage, dict) and (
                    isinstance(usage.get('completion_tokens'), int) and usage['completion_tokens'] > cap
                    or isinstance(usage.get('prompt_tokens'), int) and usage['prompt_tokens'] > 1_048_576
                ):
                    db.execute("INSERT OR REPLACE INTO metadata VALUES ('breach','usage bound exceeded')")
                return False
            if today < start_day or cost > bound:
                db.execute("INSERT OR REPLACE INTO metadata VALUES ('breach','clock or cost bound exceeded')")
                return False
            if db.execute("SELECT id FROM reservations WHERE account=? AND response_id=?",
                          (account, response_id)).fetchone():
                return False  # never refund a second physical send on a duplicate provider ID
            db.execute("UPDATE reservations SET state='settled',cost=?,finished_day=?,response_id=?,"
                       "cache_hit=?,cache_miss=?,output=? WHERE id=?",
                       (cost, today, response_id, hit, miss, output, reservation))
            return True

    def status(self, now: datetime | None = None) -> dict:
        """No mkdir, schema migration, ledger creation, or accounting mutation."""
        day = local_day(now or self.clock())
        accounting_only = self.mode == "accounting_only"
        stale = (now or self.clock()) >= RATES_EXPIRE
        result = {"day": day, "timezone": "America/Los_Angeles", "currency": "USD",
                  "mode": self.mode, "monetary_enforcement": not accounting_only,
                  "pricing_stale": stale,
                  "limit_usd": None if accounting_only else 10, "warning_usd": 8,
                  "accounting": "stale-rate estimates; not verified upper bounds or account charges" if accounting_only and stale
                    else "peak-rate upper estimates; not account charges"}
        if not self.path.exists():
            return {**result, "state": "uninitialized; earlier usage unknown" if accounting_only else "uninitialized; first day will be held"}
        db = sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True)
        try:
            db.execute("BEGIN")
            metadata = dict(db.execute("SELECT key,value FROM metadata"))
            held, settled = self._totals(db, day)
            if accounting_only:
                try:
                    historical_through = json.loads(metadata['transition'])['authorized_through_day'] if 'transition' in metadata else None
                    invalid_history = False
                except (ValueError, TypeError, KeyError):
                    historical_through, invalid_history = None, True
                return {**result,
                        "state": "accounting integrity hold" if 'breach' in metadata or metadata.get('version') != '1' else "accounting only",
                        "effective_limit_usd": None, "available_usd": None,
                        "initial_hold_day": metadata.get('hold_day'),
                        "accounting_period": "Pacific calendar day",
                        "historical_initial_day_spend": "unknown; not included in this ledger",
                        "transition_recorded": "transition" in metadata,
                        "historical_authorized_through_day": historical_through,
                        "historical_authorization_record_invalid": invalid_history,
                        "pending_usd": held / NANO_USD, "settled_estimate_usd": settled / NANO_USD,
                        "warning_recorded": bool(db.execute("SELECT 1 FROM warnings WHERE day=?", (day,)).fetchone()),
                        "accounts": [r[0] for r in db.execute("SELECT DISTINCT account FROM reservations ORDER BY account")]}
            limit = self._day_limit(metadata, day)
            transition_day = day == metadata["hold_day"] and bool(limit)
            state = "held" if not limit or "breach" in metadata or metadata.get('version') != '1' else "ready"
            if not limit and 'transition' in metadata and day > metadata['hold_day']:
                state = 'authorization expired'
            if state == "ready" and held + settled >= limit:
                state = "exhausted"
            if (now or self.clock()) >= RATES_EXPIRE:
                state = "rate review required"
            return {**result, "state": state, "initial_hold_day": metadata["hold_day"],
                    "effective_limit_usd": limit / NANO_USD,
                    "accounting_period": "prospective activation allowance" if transition_day else "Pacific calendar day",
                    "historical_initial_day_spend": "unknown; not included in this ledger",
                    "transition_recorded": "transition" in metadata,
                    "authorized_through_day": json.loads(metadata['transition'])['authorized_through_day'] if 'transition' in metadata else None,
                    "pending_usd": held / NANO_USD, "settled_upper_usd": settled / NANO_USD,
                    "available_usd": max(0, limit-held-settled) / NANO_USD if state == "ready" else 0,
                    "warning_recorded": bool(db.execute("SELECT 1 FROM warnings WHERE day=?", (day,)).fetchone()),
                    "accounts": [r[0] for r in db.execute("SELECT DISTINCT account FROM reservations ORDER BY account")]}
        finally:
            db.close()
