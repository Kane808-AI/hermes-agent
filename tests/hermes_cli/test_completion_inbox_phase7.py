"""Phase 7 contracts: actionability diagnostics, Atlas outcomes, and repair dry-run."""
import contextlib
import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from hermes_cli import kanban_db as kb, kanban_db_connect as kbc
from hermes_cli.kanban_delivery import (
    board_uuid, completion_inbox_diagnostics, completion_inbox_repair_dry_run,
    configure_producer, trusted_board_controller, trusted_return_route,
)
from hermes_state import SessionDB


@pytest.fixture
def stores(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("HERMES_KANBAN_TASK", raising=False)
    with kbc.connect_closing(tmp_path / "board.db") as board, \
            contextlib.closing(SessionDB(tmp_path / "state.db")) as state:
        state.create_session("session", source="cli")
        route = state.create_kanban_return_route(board_uuid(board), "session", surface="cli", interactive=True)
        with trusted_board_controller(state):
            configure_producer(board, {"kanban": {"completion_inbox": {"producer_enabled": True}}})
        with trusted_return_route(state, board, route):
            task = kb.create_task(board, title="Worker result")
        with kb.write_txn(board):
            kb._append_event(board, task, "completed", {"result": "untrusted worker result"})
        delivery = board.execute("SELECT id FROM kanban_deliveries").fetchone()[0]
        yield SimpleNamespace(board=board, state=state, route=route, task=task, delivery=delivery, root=tmp_path)


def owner(stores, *, fence=1, expires=None):
    from hermes_cli.completion_inbox import process_identity
    pid, started = process_identity()
    incarnation = stores.state.get_session("session")["started_at"]
    stores.state._execute_write(lambda conn: conn.execute(
        "INSERT OR REPLACE INTO kanban_session_consumers VALUES (?,?,?,?,?,?,?)",
        ("session", fence, "owner", expires or int(time.time()) + 600, pid, started, incarnation)))


def inbox(stores):
    from hermes_cli.completion_inbox import CompletionInbox
    return CompletionInbox(stores.board, stores.state, stores.route)


@contextlib.contextmanager
def live_turn(consumer):
    import uuid
    holder = f"test-turn:{uuid.uuid4().hex}"
    session = consumer.route["session_id"]
    if not consumer.state.try_acquire_session_turn_lease(session, holder):
        raise PermissionError("test turn lease unavailable")
    try:
        yield holder
    finally:
        consumer.state.release_session_turn_lease(session, holder)


def publish(consumer, delivery, text="answer"):
    with live_turn(consumer) as holder:
        return consumer.publish(delivery, [{"role": "assistant", "content": text}], 0, turn_lease_holder=holder)


def row(stores):
    return dict(stores.board.execute("SELECT * FROM kanban_deliveries WHERE id=?", (stores.delivery,)).fetchone())


def _processed(stores):
    owner(stores)
    consumer = inbox(stores)
    consumer.claim(stores.delivery)
    consumer.admit(stores.delivery)
    publish(consumer, stores.delivery)
    consumer.reconcile(stores.delivery)
    return consumer


# --- Diagnostics ---

def test_diagnostics_reports_pending_age_and_count(stores):
    d = completion_inbox_diagnostics(stores.board)
    assert d["pending"] == 1
    assert d["pending_ready"] == 1
    assert d["pending_oldest_age_seconds"] is not None
    assert d["dead_letter"] == 0
    assert d["last_success_at"] is None
    assert d["last_error_code"] is None


def test_diagnostics_reports_last_success_after_acknowledgement(stores):
    consumer = _processed(stores)
    consumer.acknowledge(stores.delivery)
    d = completion_inbox_diagnostics(stores.board)
    assert d["acknowledged"] == 1
    assert d["processed_unacknowledged"] == 0
    assert d["last_success_at"] is not None
    assert d["last_success_age_seconds"] is not None


def test_diagnostics_reports_dead_letter_and_reason(stores):
    stores.board.execute(
        "UPDATE kanban_deliveries SET state='dead_letter', last_error_code='route_missing' WHERE id=?",
        (stores.delivery,))
    d = completion_inbox_diagnostics(stores.board)
    assert d["dead_letter"] == 1
    assert d["dead_letter_reasons"] == {"route_missing": 1}


def test_diagnostics_reports_active_lease(stores):
    stores.board.execute(
        "UPDATE kanban_deliveries SET state='leased', lease_expires_at=? WHERE id=?",
        (int(time.time()) + 600, stores.delivery))
    d = completion_inbox_diagnostics(stores.board)
    assert d["active_leases"] == 1
    assert d["admitted_unprocessed"] == 1
    assert d["leased"] == 1


# --- Atlas outcomes ---

def test_record_outcome_verified(stores):
    consumer = _processed(stores)
    result = consumer.record_outcome(stores.delivery, "verified")
    assert result["outcome"] == "verified"
    assert row(stores)["outcome"] == "verified"
    assert row(stores)["outcome_at"] is not None


def test_record_outcome_followup_and_user_attention(stores):
    consumer = _processed(stores)
    consumer.record_outcome(stores.delivery, "followup_created")
    assert row(stores)["outcome"] == "followup_created"
    consumer.record_outcome(stores.delivery, "user_attention_required")
    assert row(stores)["outcome"] == "user_attention_required"


def test_record_outcome_dismissed_requires_reason(stores):
    consumer = _processed(stores)
    with pytest.raises(ValueError, match="reason"):
        consumer.record_outcome(stores.delivery, "dismissed_with_reason")
    consumer.record_outcome(stores.delivery, "dismissed_with_reason", reason="duplicate of task X")
    assert row(stores)["outcome"] == "dismissed_with_reason"
    assert row(stores)["outcome_reason"] == "duplicate of task X"


def test_record_outcome_rejects_unknown(stores):
    consumer = _processed(stores)
    with pytest.raises(ValueError, match="unknown"):
        consumer.record_outcome(stores.delivery, "bogus")


def test_record_outcome_requires_processed_delivery(stores):
    owner(stores)
    consumer = inbox(stores)
    consumer.claim(stores.delivery)  # leased, not yet processed
    with pytest.raises(RuntimeError, match="processed"):
        consumer.record_outcome(stores.delivery, "verified")


# --- Repair dry run ---

def test_repair_dry_run_reports_historical_miss_without_mutating(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("HERMES_KANBAN_TASK", raising=False)
    with kbc.connect_closing(tmp_path / "board.db") as board:
        tid = kb.create_task(board, title="historical")
        kb._append_event(board, tid, "completed", {"result": "missed"})
        report = completion_inbox_repair_dry_run(board)
        assert report["dry_run"] is True
        assert report["miss_count"] == 1
        assert report["misses"][0]["kind"] == "completed"
        assert report["misses"][0]["has_return_route"] is False
        # Dry run must not create a delivery.
        assert not board.execute("SELECT 1 FROM kanban_deliveries").fetchone()


def test_repair_dry_run_excludes_delivered_events(stores):
    report = completion_inbox_repair_dry_run(stores.board)
    assert report["miss_count"] == 0


# --- v2 -> v3 delivery-schema migration ---

def test_v2_upgrade_adds_outcome_columns_and_preserves_identity(tmp_path):
    from hermes_cli import kanban_delivery_schema as schema

    path = tmp_path / "v2.db"
    with kbc.connect_closing(path) as conn:
        identity = conn.execute("SELECT board_uuid FROM kanban_board_identity").fetchone()[0]
        assert conn.execute("SELECT schema_version FROM kanban_delivery_control").fetchone()[0] == 3
        # Downgrade to the pre-Phase 7 shape: drop the outcome columns, drop the version.
        conn.execute("ALTER TABLE kanban_deliveries DROP COLUMN outcome")
        conn.execute("ALTER TABLE kanban_deliveries DROP COLUMN outcome_reason")
        conn.execute("ALTER TABLE kanban_deliveries DROP COLUMN outcome_at")
        conn.execute("UPDATE kanban_delivery_control SET schema_version=2")
    kb._INITIALIZED_PATHS.discard(str(path.resolve()))
    with kbc.connect_closing(path) as conn:
        assert conn.execute("SELECT board_uuid FROM kanban_board_identity").fetchone()[0] == identity
        assert conn.execute("SELECT schema_version FROM kanban_delivery_control").fetchone()[0] == 3
        cols = {r[1] for r in conn.execute("PRAGMA table_info(kanban_deliveries)")}
        assert {"outcome", "outcome_reason", "outcome_at"} <= cols
        assert schema.check_delivery_schema(conn, require_capture=True)
    assert path.with_name("v2.db.before-kanban-delivery-v3.bak").is_file()
