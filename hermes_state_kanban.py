"""Profile-local trusted return routes. The board receives only opaque IDs."""
from __future__ import annotations

import sqlite3
import time
import uuid

from hermes_cli.sqlite_migration_backup import migration_backup

KANBAN_ROUTE_SCHEMA_VERSION = 1
KANBAN_ROUTE_SCHEMA_SQL = """
BEGIN IMMEDIATE;
CREATE TABLE IF NOT EXISTS kanban_route_schema (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    version INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS kanban_board_controller (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    controller_id TEXT NOT NULL UNIQUE,
    owner_state_path TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS kanban_return_routes (
    return_route_id TEXT PRIMARY KEY,
    board_uuid TEXT NOT NULL,
    session_id TEXT NOT NULL,
    session_started_at REAL NOT NULL,
    owner_state_path TEXT NOT NULL,
    surface TEXT NOT NULL CHECK(surface IN ('cli','tui','desktop')),
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kanban_routes_session ON kanban_return_routes(session_id);
CREATE TABLE IF NOT EXISTS kanban_session_consumers (
    session_id TEXT PRIMARY KEY,
    consumer_fence INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_expires_at INTEGER,
    owner_pid INTEGER,
    owner_started_at REAL
);
CREATE TRIGGER IF NOT EXISTS kanban_return_route_immutable BEFORE UPDATE ON kanban_return_routes
BEGIN SELECT RAISE(ABORT, 'return route is immutable'); END;
INSERT OR IGNORE INTO kanban_route_schema VALUES (1,1);
COMMIT;
"""


def prepare_route_migration(conn, path) -> None:
    if conn.execute("SELECT 1 FROM sqlite_master WHERE name='kanban_route_schema'").fetchone():
        row = conn.execute("SELECT version FROM kanban_route_schema WHERE singleton=1").fetchone()
        if row is None or row[0] != KANBAN_ROUTE_SCHEMA_VERSION:
            raise RuntimeError("incompatible Kanban return-route schema")
        return
    migration_backup(conn, path, "kanban-routes-v1")


def assert_trusted_route_writer() -> None:
    from agent.delegation_context import is_delegated_child_process_context, explicit_board_intent_is_pinned

    if is_delegated_child_process_context() or explicit_board_intent_is_pinned():
        raise PermissionError("workers cannot create or set return routes")


class SessionKanbanRoutesMixin:
    def _kanban_controller_id(self) -> str:
        """Trusted orchestration derives identity from its explicit profile store, never env/args."""
        assert_trusted_route_writer()
        owner = str(self.db_path.resolve())
        def identify(conn):
            row = conn.execute("SELECT controller_id,owner_state_path FROM kanban_board_controller "
                               "WHERE singleton=1").fetchone()
            if row is not None and row[1] == owner:
                return row[0]
            # A copied or relocated profile cannot impersonate the original board controller.
            identity = uuid.uuid4().hex
            conn.execute("INSERT INTO kanban_board_controller VALUES (1,?,?) "
                         "ON CONFLICT(singleton) DO UPDATE SET controller_id=excluded.controller_id,"
                         "owner_state_path=excluded.owner_state_path", (identity, owner))
            return identity
        return self._execute_write(identify)

    def create_kanban_return_route(
        self, board_uuid: str, session_id: str, *, surface: str, interactive: bool,
    ) -> str:
        """Native orchestrator API. Surface capture is wired in Phase 3, never from tool args."""
        assert_trusted_route_writer()
        if not interactive or surface not in {"cli", "tui", "desktop"} or not board_uuid:
            raise ValueError("return routes require an eligible interactive origin")
        route_id = uuid.uuid4().hex
        def create(conn):
            row = conn.execute("SELECT source,started_at FROM sessions WHERE id=?", (session_id,)).fetchone()
            sources = {"cli": {"cli"}, "tui": {"tui"}, "desktop": {"tui", "desktop"}}
            if row is None or row[0] not in sources[surface]:
                raise ValueError("return-route session does not belong to this interactive surface")
            version = conn.execute("SELECT version FROM kanban_route_schema WHERE singleton=1").fetchone()
            if version is None or version[0] != KANBAN_ROUTE_SCHEMA_VERSION:
                raise RuntimeError("incompatible Kanban return-route schema")
            conn.execute("INSERT INTO kanban_return_routes VALUES (?,?,?,?,?,?,?)",
                         (route_id, board_uuid, session_id, row[1], str(self.db_path.resolve()),
                          surface, int(time.time())))
        self._execute_write(create)
        return route_id

    def resolve_kanban_return_route(self, route_id: str, board_uuid: str) -> dict | None:
        version = self._read_one("SELECT version FROM kanban_route_schema WHERE singleton=1")
        if version is None or version[0] != KANBAN_ROUTE_SCHEMA_VERSION:
            raise RuntimeError("incompatible Kanban return-route schema")
        row = self._read_one("SELECT r.* FROM kanban_return_routes r JOIN sessions s "
                             "ON s.id=r.session_id AND s.started_at=r.session_started_at "
                             "WHERE r.return_route_id=? AND r.board_uuid=? AND r.owner_state_path=?",
                             (route_id, board_uuid, str(self.db_path.resolve())))
        if row is None:
            return None
        return dict(row)
