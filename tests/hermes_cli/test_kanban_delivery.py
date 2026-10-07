"""Phase 0/1 contracts for the completion producer, never a live consumer."""
import contextlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb, kanban_db_connect as kbc
from hermes_state import SessionDB


@pytest.fixture
def board(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("HERMES_KANBAN_TASK", raising=False)
    with kbc.connect_closing(tmp_path / "board.db") as conn:
        yield conn


def routed_task(conn, tmp_path, **kwargs):
    from hermes_cli.kanban_delivery import board_uuid, trusted_return_route

    with contextlib.closing(SessionDB(tmp_path / "origin.db")) as state:
        state.create_session("same-session", source="cli")
        route = state.create_kanban_return_route(
            board_uuid(conn), "same-session", surface="cli", interactive=True)
        with trusted_return_route(state, conn, route):
            tid = kb.create_task(conn, title="origin", **kwargs)
    return tid, route


def enable(conn):
    from hermes_cli.kanban_delivery import configure_producer

    configure_producer(conn, {"kanban": {"completion_inbox": {"producer_enabled": True}}})


def test_board_identity_stable_across_reopen_and_distinct_across_boards(board, tmp_path):
    from hermes_cli.kanban_delivery import board_uuid

    identity = board_uuid(board)
    with kbc.connect_closing(tmp_path / "board.db") as reopened:
        assert board_uuid(reopened) == identity
    with kbc.connect_closing(tmp_path / "other.db") as other:
        assert board_uuid(other) != identity
    for mutation in ("UPDATE kanban_board_identity SET board_uuid='different'", "DELETE FROM kanban_board_identity"):
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            board.execute(mutation)
    assert board_uuid(board) == identity


@pytest.mark.parametrize("kind", ["completed", "blocked", "gave_up", "review_requested", "changes_requested"])
def test_each_actionable_event_has_immutable_atomic_delivery(board, tmp_path, kind):
    tid, route = routed_task(board, tmp_path)
    enable(board)
    with kb.write_txn(board):
        kb._append_event(board, tid, kind)
        kb._append_event(board, tid, kind)
    rows = board.execute("SELECT * FROM kanban_deliveries ORDER BY event_id").fetchall()
    assert len(rows) == 2
    assert rows[0]["id"] != rows[1]["id"]
    assert all(r["return_route_id"] == route and r["state"] == "pending" for r in rows)
    assert all(r["consumer_fence"] is None and r["turn_id"] is None for r in rows)
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        board.execute("UPDATE kanban_deliveries SET return_route_id='different'")
    with pytest.raises(sqlite3.IntegrityError):
        board.execute("INSERT INTO kanban_deliveries SELECT * FROM kanban_deliveries LIMIT 1")


def test_event_and_task_transition_rollback_when_outbox_fails(board, tmp_path):
    tid, _ = routed_task(board, tmp_path)
    enable(board)
    board.execute("CREATE TRIGGER fail_delivery BEFORE INSERT ON kanban_deliveries "
                  "BEGIN SELECT RAISE(ABORT, 'injected outbox failure'); END")
    with pytest.raises(sqlite3.IntegrityError, match="injected outbox failure"):
        kb.complete_task(board, tid, summary="result")
    assert kb.get_task(board, tid).status == "ready"
    assert not board.execute("SELECT 1 FROM task_events WHERE kind='completed'").fetchone()
    assert not board.execute("SELECT 1 FROM kanban_deliveries").fetchone()


def test_raw_mixed_version_writer_is_captured_and_blocks_rollout(board, tmp_path):
    from hermes_cli.kanban_delivery import producer_diagnostics, require_producer_ready

    tid, _ = routed_task(board, tmp_path)
    enable(board)
    board.execute("INSERT INTO task_events(task_id,kind,created_at) VALUES (?,'completed',1)", (tid,))
    assert board.execute("SELECT COUNT(*) FROM kanban_deliveries").fetchone()[0] == 1
    assert producer_diagnostics(board)["mixed_version_events"] == 1
    with pytest.raises(RuntimeError, match="mixed_version_events"):
        require_producer_ready(board)


def test_disable_preserves_rows_and_does_not_backfill_history(board, tmp_path):
    from hermes_cli.kanban_delivery import configure_producer

    tid, _ = routed_task(board, tmp_path)
    kb._append_event(board, tid, "completed")
    enable(board)
    assert not board.execute("SELECT 1 FROM kanban_deliveries").fetchone()
    kb._append_event(board, tid, "completed")
    before = dict(board.execute("SELECT * FROM kanban_deliveries").fetchone())
    configure_producer(board, {})
    kb._append_event(board, tid, "completed")
    enable(board)
    assert [dict(r) for r in board.execute("SELECT * FROM kanban_deliveries")] == [before]


@pytest.mark.parametrize("kind", ["heartbeat", "claimed", "status", "commented", "crashed", "timed_out"])
def test_passive_or_retry_events_do_not_produce_deliveries(board, tmp_path, kind):
    tid, _ = routed_task(board, tmp_path)
    enable(board)
    kb._append_event(board, tid, kind)
    assert not board.execute("SELECT 1 FROM kanban_deliveries").fetchone()


@pytest.mark.parametrize("creation", ["creator", "parent", "decompose"])
def test_task_tree_keeps_one_delivery_per_event_not_per_root(board, tmp_path, creation):
    from hermes_cli.kanban_db_graph import decompose_triage_task

    root, route = routed_task(board, tmp_path, triage=creation == "decompose")
    if creation == "parent":
        assert kb.complete_task(board, root, summary="parent ready")
    enable(board)
    if creation == "decompose":
        children = decompose_triage_task(board, root, root_assignee="default",
                                         children=[{"title": "a"}, {"title": "b"}])
    else:
        args = {"creator_task_id": root} if creation == "creator" else {"parents": [root]}
        children = [kb.create_task(board, title=title, **args) for title in ("a", "b")]
    for tid in children:
        assert kb.get_task(board, tid).return_route_id == route
        kb.block_task(board, tid, reason="needs input")
    assert board.execute("SELECT COUNT(*) FROM kanban_deliveries").fetchone()[0] == 2


def test_missing_route_and_missing_delivery_are_visible(board):
    from hermes_cli.kanban_delivery import producer_diagnostics, require_producer_ready

    tid = kb.create_task(board, title="legacy origin")
    enable(board)
    kb.complete_task(board, tid, summary="legacy result")
    assert producer_diagnostics(board)["unrouted_actionable_events"] == 1
    with pytest.raises(RuntimeError, match="unrouted_actionable_events"):
        require_producer_ready(board)


def test_gc_and_task_delete_cannot_remove_unprocessed_delivery(board, tmp_path):
    from hermes_cli.kanban_delivery import gc_acknowledged_deliveries

    tid, _ = routed_task(board, tmp_path)
    enable(board)
    kb.complete_task(board, tid, summary="result")
    board.execute("UPDATE task_events SET created_at=1")
    assert gc_acknowledged_deliveries(board, retention_days=1, now=10**10) == 0
    kb.gc_events(board, older_than_seconds=1)
    assert board.execute("SELECT 1 FROM task_events WHERE kind='completed'").fetchone()
    with pytest.raises(sqlite3.IntegrityError):
        kb.delete_task(board, tid)
    assert kb.get_task(board, tid) is not None


@pytest.mark.parametrize("version_field", ["schema_version", "min_writer_version"])
def test_future_schema_refused_even_when_connection_path_cached(board, tmp_path, version_field):
    board.execute(f"UPDATE kanban_delivery_control SET {version_field}=999")
    with pytest.raises(RuntimeError, match="schema"):
        kbc.connect(tmp_path / "board.db")
    with pytest.raises(RuntimeError, match="schema"):
        kb._append_event(board, "unknown", "completed")


def test_concurrent_initializers_keep_one_board_identity(tmp_path):
    from hermes_cli.kanban_delivery import board_uuid

    path = tmp_path / "racing.db"
    def initialize(_):
        with kbc.connect_closing(path) as conn:
            return board_uuid(conn)
    with ThreadPoolExecutor(max_workers=4) as pool:
        assert len(set(pool.map(initialize, range(8)))) == 1


def test_later_consumer_flags_cannot_activate_in_phase_one(board):
    from hermes_cli.kanban_delivery import configure_producer

    for flag in ("cli_enabled", "gateway_enabled", "tui_enabled", "repair_enabled"):
        with pytest.raises(RuntimeError, match="later phase"):
            configure_producer(board, {"kanban": {"completion_inbox": {flag: True}}})
    assert board.execute("SELECT producer_enabled FROM kanban_delivery_control").fetchone()[0] == 0


def test_missing_trigger_is_visible_even_for_direct_sql(board, tmp_path):
    from hermes_cli.kanban_delivery import producer_diagnostics, require_producer_ready

    tid, _ = routed_task(board, tmp_path)
    enable(board)
    board.execute("DROP TRIGGER kanban_capture_delivery")
    board.execute("INSERT INTO task_events(task_id,kind,created_at) VALUES (?,'completed',1)", (tid,))
    assert producer_diagnostics(board)["mixed_version_events"] == 1
    with pytest.raises(RuntimeError, match="missing_capture_trigger"):
        require_producer_ready(board)


def test_native_writer_missing_delivery_stays_visible_after_trigger_is_restored(board, tmp_path):
    from hermes_cli.kanban_delivery import producer_diagnostics, require_producer_ready
    from hermes_cli.kanban_delivery_schema import DELIVERY_TRIGGERS
    from hermes_cli.sqlite_migration_backup import execute_ddl

    tid, _ = routed_task(board, tmp_path)
    enable(board)
    board.execute("DROP TRIGGER kanban_capture_delivery")
    kb._append_event(board, tid, "completed")
    execute_ddl(board, DELIVERY_TRIGGERS)
    assert producer_diagnostics(board)["missing_deliveries"] == 1
    with pytest.raises(RuntimeError, match="missing_deliveries"):
        require_producer_ready(board)


def test_explicit_ack_retention_removes_only_acknowledged_delivery_and_event(board, tmp_path):
    from hermes_cli.kanban_delivery import gc_acknowledged_deliveries, require_producer_ready

    tid, _ = routed_task(board, tmp_path)
    enable(board)
    kb._append_event(board, tid, "completed")
    kb._append_event(board, tid, "blocked")
    # Storage fixture only. No consumer/state transition API ships in this phase.
    board.execute("UPDATE kanban_deliveries SET state='acknowledged',acknowledged_at=1 "
                  "WHERE event_id=(SELECT MIN(event_id) FROM kanban_deliveries)")
    assert gc_acknowledged_deliveries(board, retention_days=0, now=10**10) == 0
    assert gc_acknowledged_deliveries(board, retention_days=30, now=10**10) == 1
    assert board.execute("SELECT state FROM kanban_deliveries").fetchone()[0] == "pending"
    assert not board.execute("SELECT 1 FROM task_events WHERE kind='completed'").fetchone()
    require_producer_ready(board)


def test_initially_blocked_child_has_route_before_its_first_actionable_event(board, tmp_path):
    root, route = routed_task(board, tmp_path)
    enable(board)
    child = kb.create_task(board, title="blocked child", creator_task_id=root, initial_status="blocked")
    row = board.execute("SELECT * FROM kanban_deliveries").fetchone()
    assert row["task_id"] == child and row["return_route_id"] == route


def test_cancellation_rolls_back_event_and_outbox(board, tmp_path):
    tid, _ = routed_task(board, tmp_path)
    enable(board)
    with pytest.raises(KeyboardInterrupt):
        with kb.write_txn(board):
            kb._append_event(board, tid, "completed")
            raise KeyboardInterrupt()
    assert not board.in_transaction
    assert not board.execute("SELECT 1 FROM kanban_deliveries").fetchone()


def test_worker_task_arguments_cannot_supply_route_fields(board, tmp_path, monkeypatch):
    from hermes_cli.kanban_delivery import configure_producer, trusted_return_route
    from tools import kanban_tools  # noqa: F401
    from tools.registry import registry
    import json

    root, route = routed_task(board, tmp_path)
    monkeypatch.setenv("HERMES_KANBAN_TASK", root)
    for key in ("return_route_id", "origin_interactive"):
        result = json.loads(registry.dispatch("kanban_create", {"title": "forged", "assignee": "default", key: route}))
        assert "unknown parameter" in result.get("error", "")
    with contextlib.closing(SessionDB(tmp_path / "worker.db")) as state:
        state.create_session("worker", source="cli")
        with pytest.raises(PermissionError, match="workers"):
            state.create_kanban_return_route("board", "worker", surface="cli", interactive=True)
        with pytest.raises(PermissionError, match="workers"):
            with trusted_return_route(state, board, route):
                pass
    with pytest.raises(PermissionError, match="workers"):
        configure_producer(board, {})


def test_worker_cannot_borrow_another_origins_route(board, tmp_path, monkeypatch):
    root, route = routed_task(board, tmp_path)
    unbound = kb.create_task(board, title="worker without route")
    monkeypatch.setenv("HERMES_KANBAN_TASK", unbound)
    with pytest.raises(PermissionError, match="workers"):
        kb.create_task(board, title="foreign creator", creator_task_id=root)
    with pytest.raises(PermissionError, match="workers"):
        kb.create_task(board, title="foreign parent", parents=[root])
    monkeypatch.setenv("HERMES_KANBAN_TASK", root)
    child = kb.create_task(board, title="own child", creator_task_id=root)
    assert kb.get_task(board, child).return_route_id == route


@pytest.mark.parametrize("crash_boundary", ["before_commit", "after_commit"])
def test_process_death_preserves_transactional_event_delivery(board, tmp_path, crash_boundary):
    import os
    import subprocess
    import sys

    tid, _ = routed_task(board, tmp_path)
    enable(board)
    script = """
import os, sys
from pathlib import Path
from hermes_cli import kanban_db as kb, kanban_db_connect as kbc
conn = kbc.connect(Path(sys.argv[1]))
with kb.write_txn(conn):
    conn.execute("UPDATE tasks SET status='done' WHERE id=?", (sys.argv[2],))
    kb._append_event(conn, sys.argv[2], 'completed')
    if sys.argv[3] == 'before_commit':
        os._exit(19)
os._exit(19)
"""
    env = {**os.environ, "HERMES_HOME": str(tmp_path / "child-home")}
    result = subprocess.run([sys.executable, "-c", script, str(tmp_path / "board.db"), tid, crash_boundary],
                            env=env, capture_output=True, text=True, timeout=15)
    assert result.returncode == 19, result.stderr
    expected = int(crash_boundary == "after_commit")
    assert board.execute("SELECT COUNT(*) FROM task_events WHERE kind='completed'").fetchone()[0] == expected
    assert board.execute("SELECT COUNT(*) FROM kanban_deliveries").fetchone()[0] == expected
    assert kb.get_task(board, tid).status == ("done" if expected else "ready")
