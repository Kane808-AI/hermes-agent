"""Additive migration and snapshot rollback contracts on real SQLite files."""
import contextlib
from pathlib import Path
import sqlite3

import pytest

from hermes_cli import kanban_db as kb, kanban_db_connect as kbc
from hermes_cli import kanban_delivery_schema as schema
from hermes_state import SessionDB
from hermes_state_common import SCHEMA_SQL
from hermes_state_kanban import KANBAN_ROUTE_SCHEMA_SQL


def legacy_board(path):
    ddl = kb.SCHEMA_SQL.replace("    return_route_id      TEXT,\n", "").replace(
        "    origin_interactive   INTEGER NOT NULL DEFAULT 0,\n", "").replace(
        "    producer_version INTEGER NOT NULL DEFAULT 0,\n", "").replace(
        "    delivery_expected INTEGER NOT NULL DEFAULT 0,\n", "")
    with contextlib.closing(sqlite3.connect(path, isolation_level=None)) as conn:
        conn.executescript(ddl)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("INSERT INTO tasks(id,title,status,created_at) VALUES ('old','old task','done',1)")
        conn.execute("INSERT INTO task_events(task_id,kind,created_at) VALUES ('old','completed',1)")


def test_migration_preserves_history_without_replay_and_private_snapshot(tmp_path):
    path = tmp_path / "legacy.db"
    legacy_board(path)
    with kbc.connect_closing(path) as conn:
        identity = conn.execute("SELECT board_uuid FROM kanban_board_identity").fetchone()[0]
        assert kb.get_task(conn, "old").return_route_id is None
        assert conn.execute("SELECT id FROM task_events").fetchone()[0] == 1
        assert not conn.execute("SELECT 1 FROM kanban_deliveries").fetchone()
    backup = path.with_name("legacy.db.before-kanban-delivery-v1.bak")
    assert backup.is_file()
    with contextlib.closing(sqlite3.connect(backup)) as old:
        assert old.execute("SELECT title FROM tasks").fetchone()[0] == "old task"
        assert not old.execute("SELECT 1 FROM sqlite_master WHERE name='kanban_deliveries'").fetchone()
    kb._INITIALIZED_PATHS.discard(str(path.resolve()))
    with kbc.connect_closing(path) as conn:
        assert conn.execute("SELECT board_uuid FROM kanban_board_identity").fetchone()[0] == identity
    assert len(list(tmp_path.glob("*.bak"))) == 1


@pytest.mark.platforms("posix")
def test_private_snapshot_includes_uncheckpointed_wal(tmp_path):
    path = tmp_path / "legacy.db"
    legacy_board(path)
    with contextlib.closing(sqlite3.connect(path, isolation_level=None)) as writer:
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("INSERT INTO tasks(id,title,status,created_at) VALUES ('wal','WAL task','done',2)")
        assert path.with_name("legacy.db-wal").stat().st_size > 0
        with kbc.connect_closing(path):
            pass
        backup = path.with_name("legacy.db.before-kanban-delivery-v1.bak")
        assert backup.stat().st_mode & 0o777 == 0o600
        with contextlib.closing(sqlite3.connect(backup)) as snapshot:
            assert snapshot.execute("SELECT title FROM tasks WHERE id='wal'").fetchone()[0] == "WAL task"


def test_backup_failure_prevents_any_delivery_migration(tmp_path, monkeypatch):
    path = tmp_path / "legacy.db"
    legacy_board(path)
    def fail(*args):
        raise OSError("injected backup failure")
    monkeypatch.setattr(schema, "migration_backup", fail)
    with pytest.raises(OSError, match="backup failure"):
        kbc.connect(path)
    with contextlib.closing(sqlite3.connect(path)) as conn:
        assert not conn.execute("SELECT 1 FROM sqlite_master WHERE name='kanban_deliveries'").fetchone()
        assert "return_route_id" not in {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}


def test_failure_during_additive_transaction_rolls_back_then_retry_succeeds(tmp_path, monkeypatch):
    path = tmp_path / "legacy.db"
    legacy_board(path)
    original = schema.execute_ddl
    def fail_after_tables(conn, ddl):
        original(conn, ddl)
        raise RuntimeError("injected migration crash")
    monkeypatch.setattr(schema, "execute_ddl", fail_after_tables)
    with pytest.raises(RuntimeError, match="migration crash"):
        kbc.connect(path)
    with contextlib.closing(sqlite3.connect(path)) as conn:
        assert not conn.execute("SELECT 1 FROM sqlite_master WHERE name='kanban_deliveries'").fetchone()
        assert "return_route_id" not in {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
    monkeypatch.setattr(schema, "execute_ddl", original)
    with kbc.connect_closing(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 1
        assert conn.execute("SELECT producer_enabled FROM kanban_delivery_control").fetchone()[0] == 0


def test_profile_migration_preserves_session_and_has_snapshot(tmp_path):
    path = tmp_path / "state.db"
    with contextlib.closing(sqlite3.connect(path)) as conn:
        conn.executescript(SCHEMA_SQL.removesuffix(KANBAN_ROUTE_SCHEMA_SQL))
        conn.execute("INSERT INTO sessions(id,source,started_at) VALUES ('s','cli',1)")
        conn.commit()
    with contextlib.closing(SessionDB(path)) as db:
        assert db.get_session("s")["source"] == "cli"
        route = db.create_kanban_return_route("board", "s", surface="cli", interactive=True)
        assert db.resolve_kanban_return_route(route, "board")["session_id"] == "s"
    assert path.with_name("state.db.before-kanban-routes-v1.bak").exists()


def test_future_profile_route_schema_refuses_writable_open(tmp_path):
    path = tmp_path / "state.db"
    with contextlib.closing(SessionDB(path)):
        pass
    with contextlib.closing(sqlite3.connect(path)) as conn:
        conn.execute("UPDATE kanban_route_schema SET version=999")
        conn.commit()
    with pytest.raises(RuntimeError, match="schema"):
        SessionDB(path)


def test_export_scrubs_routes_without_touching_live_source(tmp_path, monkeypatch):
    from hermes_cli.kanban_delivery import board_uuid, configure_producer, trusted_return_route, trusted_board_controller
    from hermes_cli.kanban_transfer import export_board, import_board

    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("HERMES_KANBAN_HOME", str(tmp_path / "home"))
    kb.create_board("source")
    with kbc.connect_closing(board="source") as conn, contextlib.closing(SessionDB(tmp_path / "state.db")) as state:
        identity = board_uuid(conn)
        state.create_session("s", source="cli")
        route = state.create_kanban_return_route(identity, "s", surface="cli", interactive=True)
        with trusted_return_route(state, conn, route):
            tid = kb.create_task(conn, title="routed source")
        with trusted_board_controller(state):
            configure_producer(conn, {"kanban": {"completion_inbox": {"producer_enabled": True}}})
        controller = conn.execute("SELECT controller_id FROM kanban_delivery_control").fetchone()[0]
        kb.complete_task(conn, tid, summary="pending delivery")
    archive = export_board("source", str(tmp_path / "export.tar.gz"))
    imported = import_board(archive["archive"], slug="destination")
    with kbc.connect_closing(board="source") as conn:
        assert board_uuid(conn) == identity
        assert conn.execute("SELECT COUNT(*) FROM kanban_deliveries").fetchone()[0] == 1
        assert conn.execute("SELECT controller_id FROM kanban_delivery_control").fetchone()[0] == controller
    with kbc.connect_closing(board=imported["board"]) as conn:
        assert board_uuid(conn) != identity
        assert kb.get_task(conn, tid).return_route_id is None
        assert not conn.execute("SELECT 1 FROM kanban_deliveries").fetchone()
        assert conn.execute("SELECT producer_enabled FROM kanban_delivery_control").fetchone()[0] == 0
        assert conn.execute("SELECT controller_id FROM kanban_delivery_control").fetchone()[0] is None


def test_reviewed_v1_upgrade_preserves_identity_and_obligations_but_requires_controller(tmp_path):
    from hermes_cli.kanban_delivery import board_uuid, configure_producer, trusted_board_controller, trusted_return_route

    path = tmp_path / "reviewed.db"
    with kbc.connect_closing(path) as conn, contextlib.closing(SessionDB(tmp_path / "state.db")) as state:
        identity = board_uuid(conn)
        state.create_session("s", source="cli")
        route = state.create_kanban_return_route(identity, "s", surface="cli", interactive=True)
        with trusted_return_route(state, conn, route):
            tid = kb.create_task(conn, title="pending from reviewed foundation")
        with trusted_board_controller(state):
            configure_producer(conn, {"kanban": {"completion_inbox": {"producer_enabled": True}}})
        kb.complete_task(conn, tid, summary="pending")
        obligation = dict(conn.execute("SELECT * FROM kanban_deliveries").fetchone())
        # Reproduce the reviewed extension's metadata and unconditional route guard.
        conn.execute("DROP TRIGGER kanban_controller_immutable")
        conn.execute("ALTER TABLE kanban_delivery_control DROP COLUMN controller_id")
        conn.execute("UPDATE kanban_delivery_control SET schema_version=1,min_writer_version=1")
        conn.execute("DROP TRIGGER kanban_task_route_immutable")
        conn.execute("CREATE TRIGGER kanban_task_route_immutable BEFORE UPDATE OF return_route_id,origin_interactive "
                     "ON tasks WHEN OLD.return_route_id IS NOT NEW.return_route_id "
                     "OR OLD.origin_interactive IS NOT NEW.origin_interactive "
                     "BEGIN SELECT RAISE(ABORT, 'return route is immutable'); END")
        conn.execute("DROP TRIGGER kanban_capture_delivery")
        capture = schema.DELIVERY_TRIGGERS.split("CREATE TRIGGER IF NOT EXISTS kanban_task_route_immutable")[0]
        conn.execute(capture.replace(f"!={schema.DELIVERY_SCHEMA_VERSION}", "!=1"))
    kb._INITIALIZED_PATHS.discard(str(path.resolve()))
    with kbc.connect_closing(path) as conn:
        assert board_uuid(conn) == identity
        assert dict(conn.execute("SELECT * FROM kanban_deliveries").fetchone()) == obligation
        assert conn.execute("SELECT producer_enabled,controller_id FROM kanban_delivery_control").fetchone()[:] == (0, None)
        assert schema.check_delivery_schema(conn, require_capture=True)
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute("UPDATE tasks SET return_route_id=NULL WHERE id=?", (tid,))
    backup = path.with_name("reviewed.db.before-kanban-delivery-v2.bak")
    with contextlib.closing(sqlite3.connect(backup)) as snapshot:
        assert snapshot.execute("SELECT schema_version,producer_enabled FROM kanban_delivery_control").fetchone() == (1, 1)
