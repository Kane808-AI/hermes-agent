"""Additive board outbox schema. No polling, wake, replay, or transcript writes."""
from __future__ import annotations

from pathlib import Path
import sqlite3
import time
import uuid

from hermes_cli.sqlite_migration_backup import execute_ddl, migration_backup

DELIVERY_SCHEMA_VERSION = 1
DELIVERY_WRITER_VERSION = 1
ACTIONABLE_EVENT_KINDS = ("completed", "blocked", "gave_up", "review_requested", "changes_requested")

DELIVERY_DDL = """
CREATE TABLE IF NOT EXISTS kanban_board_identity (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    board_uuid TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS kanban_delivery_control (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    schema_version INTEGER NOT NULL,
    min_writer_version INTEGER NOT NULL,
    producer_enabled INTEGER NOT NULL DEFAULT 0 CHECK(producer_enabled IN (0,1)),
    first_event_id INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS kanban_deliveries (
    id TEXT PRIMARY KEY,
    board_uuid TEXT NOT NULL REFERENCES kanban_board_identity(board_uuid),
    task_id TEXT NOT NULL REFERENCES tasks(id),
    event_id INTEGER NOT NULL REFERENCES task_events(id),
    return_route_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN
        ('pending','leased','transcript_admitted','processed','acknowledged','dead_letter')),
    consumer_fence INTEGER,
    marker_message_id INTEGER,
    turn_id TEXT,
    lease_owner TEXT,
    lease_expires_at INTEGER,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    next_attempt_at INTEGER,
    transcript_message_id INTEGER,
    last_error_code TEXT,
    processed_at INTEGER,
    acknowledged_at INTEGER,
    UNIQUE(board_uuid, event_id, return_route_id)
);
CREATE INDEX IF NOT EXISTS idx_kanban_deliveries_pending
    ON kanban_deliveries(state, next_attempt_at, event_id);
CREATE INDEX IF NOT EXISTS idx_kanban_deliveries_route
    ON kanban_deliveries(return_route_id, state, event_id);
CREATE INDEX IF NOT EXISTS idx_kanban_deliveries_task ON kanban_deliveries(task_id);
"""

_ACTIONABLE_SQL = ",".join(f"'{kind}'" for kind in ACTIONABLE_EVENT_KINDS)
DELIVERY_TRIGGERS = f"""
CREATE TRIGGER IF NOT EXISTS kanban_capture_delivery AFTER INSERT ON task_events
WHEN NEW.kind IN ({_ACTIONABLE_SQL})
 AND (SELECT producer_enabled FROM kanban_delivery_control WHERE singleton=1)=1
BEGIN
    SELECT CASE WHEN (SELECT schema_version FROM kanban_delivery_control WHERE singleton=1)!=1
        THEN RAISE(ABORT, 'incompatible delivery schema') END;
    UPDATE task_events SET delivery_expected=1 WHERE id=NEW.id;
    INSERT INTO kanban_deliveries(id,board_uuid,task_id,event_id,return_route_id,created_at)
    SELECT lower(hex(randomblob(16))), b.board_uuid, t.id, NEW.id, t.return_route_id, NEW.created_at
    FROM tasks t CROSS JOIN kanban_board_identity b
    WHERE t.id=NEW.task_id AND t.return_route_id IS NOT NULL;
END;
CREATE TRIGGER IF NOT EXISTS kanban_task_route_immutable
BEFORE UPDATE OF return_route_id, origin_interactive ON tasks
WHEN OLD.return_route_id IS NOT NEW.return_route_id OR OLD.origin_interactive IS NOT NEW.origin_interactive
BEGIN SELECT RAISE(ABORT, 'return route is immutable'); END;
CREATE TRIGGER IF NOT EXISTS kanban_delivery_identity_immutable
BEFORE UPDATE OF id,board_uuid,task_id,event_id,return_route_id,created_at ON kanban_deliveries
WHEN OLD.id IS NOT NEW.id OR OLD.board_uuid IS NOT NEW.board_uuid OR OLD.task_id IS NOT NEW.task_id
 OR OLD.event_id IS NOT NEW.event_id OR OLD.return_route_id IS NOT NEW.return_route_id
 OR OLD.created_at IS NOT NEW.created_at
BEGIN SELECT RAISE(ABORT, 'delivery identity is immutable'); END;
CREATE TRIGGER IF NOT EXISTS kanban_delivery_preserve_unprocessed BEFORE DELETE ON kanban_deliveries
WHEN OLD.state!='acknowledged' OR OLD.acknowledged_at IS NULL
BEGIN SELECT RAISE(ABORT, 'unprocessed delivery must be preserved'); END;
CREATE TRIGGER IF NOT EXISTS kanban_event_identity_immutable
BEFORE UPDATE OF task_id,kind,producer_version,delivery_expected ON task_events
WHEN OLD.delivery_expected=1 AND (OLD.task_id IS NOT NEW.task_id OR OLD.kind IS NOT NEW.kind
 OR OLD.producer_version IS NOT NEW.producer_version OR OLD.delivery_expected IS NOT NEW.delivery_expected)
BEGIN SELECT RAISE(ABORT, 'captured event is immutable'); END;
CREATE TRIGGER IF NOT EXISTS kanban_board_identity_immutable BEFORE UPDATE ON kanban_board_identity
BEGIN SELECT RAISE(ABORT, 'board identity is immutable'); END;
CREATE TRIGGER IF NOT EXISTS kanban_board_identity_preserve BEFORE DELETE ON kanban_board_identity
BEGIN SELECT RAISE(ABORT, 'board identity is immutable'); END;
CREATE TRIGGER IF NOT EXISTS kanban_event_preserve_delivery BEFORE DELETE ON task_events
WHEN EXISTS (SELECT 1 FROM kanban_deliveries WHERE event_id=OLD.id)
BEGIN SELECT RAISE(ABORT, 'event still has a durable delivery'); END;
CREATE TRIGGER IF NOT EXISTS kanban_task_preserve_delivery BEFORE DELETE ON tasks
WHEN EXISTS (SELECT 1 FROM kanban_deliveries WHERE task_id=OLD.id)
BEGIN SELECT RAISE(ABORT, 'task still has a durable delivery'); END;
"""
DELIVERY_TRIGGER_NAMES = (
    "kanban_capture_delivery", "kanban_task_route_immutable", "kanban_delivery_identity_immutable",
    "kanban_delivery_preserve_unprocessed", "kanban_event_identity_immutable", "kanban_board_identity_immutable",
    "kanban_board_identity_preserve", "kanban_event_preserve_delivery", "kanban_task_preserve_delivery",
)


def check_delivery_schema(conn: sqlite3.Connection) -> bool:
    """Fail before schema writes on an unknown newer extension, including cached opens."""
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='kanban_delivery_control'").fetchone():
        return False
    row = conn.execute("SELECT schema_version,min_writer_version FROM kanban_delivery_control").fetchone()
    if row is None or row[0] != DELIVERY_SCHEMA_VERSION or row[1] > DELIVERY_WRITER_VERSION:
        raise RuntimeError("incompatible Kanban delivery schema or writer version")
    return True


def prepare_delivery_migration(conn: sqlite3.Connection, path: Path) -> None:
    if not check_delivery_schema(conn):
        migration_backup(conn, path, "kanban-delivery-v1")


def migrate_delivery_schema(conn: sqlite3.Connection) -> None:
    from hermes_cli.kanban_db import write_txn
    from hermes_cli.sqlite_util import add_column_if_missing

    if check_delivery_schema(conn):
        return
    with write_txn(conn):
        if check_delivery_schema(conn):
            return
        for table, name, ddl in (
            ("tasks", "return_route_id", "return_route_id TEXT"),
            ("tasks", "origin_interactive", "origin_interactive INTEGER NOT NULL DEFAULT 0"),
            ("task_events", "producer_version", "producer_version INTEGER NOT NULL DEFAULT 0"),
            ("task_events", "delivery_expected", "delivery_expected INTEGER NOT NULL DEFAULT 0"),
        ):
            add_column_if_missing(conn, table, name, ddl)
        execute_ddl(conn, DELIVERY_DDL)
        conn.execute("INSERT INTO kanban_board_identity VALUES (1,?,?)", (uuid.uuid4().hex, int(time.time())))
        execute_ddl(conn, DELIVERY_TRIGGERS)
        conn.execute("INSERT INTO kanban_delivery_control VALUES (1,?,?,0,0)",
                     (DELIVERY_SCHEMA_VERSION, DELIVERY_WRITER_VERSION))


def reset_transferred_delivery_state(conn: sqlite3.Connection) -> None:
    """Only for the export/import snapshot: a new board cannot own the source's routes."""
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='kanban_delivery_control'").fetchone():
        return
    for name in DELIVERY_TRIGGER_NAMES:
        conn.execute(f"DROP TRIGGER IF EXISTS {name}")
    for table in ("kanban_deliveries", "kanban_delivery_control", "kanban_board_identity"):
        conn.execute(f"DROP TABLE {table}")
    conn.execute("UPDATE tasks SET return_route_id=NULL, origin_interactive=0")
    conn.execute("UPDATE task_events SET delivery_expected=0")
