"""Profile-local trusted return routes. The board receives only opaque IDs."""
from __future__ import annotations

import sqlite3
import time
import uuid

from hermes_cli.sqlite_migration_backup import migration_backup

KANBAN_ROUTE_SCHEMA_VERSION = 1
KANBAN_CONSUMER_LEASE_SECONDS = 30
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
CREATE TABLE IF NOT EXISTS kanban_return_route_owners (
    return_route_id TEXT PRIMARY KEY,
    board_uuid TEXT NOT NULL,
    owner_state_path TEXT NOT NULL
);
INSERT OR IGNORE INTO kanban_return_route_owners
    SELECT return_route_id,board_uuid,owner_state_path FROM kanban_return_routes;
CREATE TRIGGER IF NOT EXISTS kanban_route_owner_immutable BEFORE UPDATE ON kanban_return_route_owners
BEGIN SELECT RAISE(ABORT, 'route ownership is immutable'); END;
CREATE TRIGGER IF NOT EXISTS kanban_route_owner_retained BEFORE DELETE ON kanban_return_route_owners
BEGIN SELECT RAISE(ABORT, 'route ownership is retained'); END;
CREATE TABLE IF NOT EXISTS kanban_session_consumers (
    session_id TEXT PRIMARY KEY,
    consumer_fence INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_expires_at INTEGER,
    owner_pid INTEGER,
    owner_started_at REAL,
    session_started_at REAL
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
        consumers = {r[1] for r in conn.execute("PRAGMA table_info(kanban_session_consumers)")}
        owners = conn.execute("SELECT 1 FROM sqlite_master WHERE name='kanban_return_route_owners'").fetchone()
        if "session_started_at" not in consumers or owners is None:
            migration_backup(conn, path, "kanban-completion-profile-v1")
        return
    migration_backup(conn, path, "kanban-routes-v1")


def assert_trusted_route_writer() -> None:
    from agent.delegation_context import is_delegated_child_process_context, explicit_board_intent_is_pinned

    if is_delegated_child_process_context() or explicit_board_intent_is_pinned():
        raise PermissionError("workers cannot create or set return routes")


def process_identity() -> tuple[int, int]:
    """Resolve the live process incarnation at the authority boundary."""
    from hermes_cli.completion_inbox import process_identity as _process_identity

    return _process_identity()


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
            conn.execute("INSERT INTO kanban_return_route_owners VALUES (?,?,?)",
                         (route_id, board_uuid, str(self.db_path.resolve())))
        self._execute_write(create)
        return route_id

    def _discard_kanban_return_route(self, route_id: str, board_uuid: str) -> None:
        """Remove only a provisional route whose board task failed to commit.

        The immutable ownership witness remains, so the opaque id can never be
        rebound to another board or profile.
        """
        assert_trusted_route_writer()
        owner = str(self.db_path.resolve())

        def discard(conn):
            row = conn.execute(
                "SELECT board_uuid,owner_state_path FROM kanban_return_routes "
                "WHERE return_route_id=?", (route_id,),
            ).fetchone()
            if row is None:
                return
            if row[0] != board_uuid or row[1] != owner:
                raise PermissionError("provisional return route belongs to a different owner")
            conn.execute("DELETE FROM kanban_return_routes WHERE return_route_id=?", (route_id,))

        self._execute_write(discard)

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

    @staticmethod
    def _consumer_ttl(ttl_seconds: int) -> int:
        if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, int):
            raise ValueError("consumer lease duration must be an integer")
        if ttl_seconds < 5 or ttl_seconds > 300:
            raise ValueError("consumer lease duration must be between 5 and 300 seconds")
        return ttl_seconds

    def acquire_kanban_cli_consumer(
        self,
        session_id: str,
        lease_owner: str,
        *,
        ttl_seconds: int = KANBAN_CONSUMER_LEASE_SECONDS,
    ) -> dict:
        """Acquire or reacquire the classic CLI consumer with a monotonic fence.

        The active-session registry proves that ``lease_owner`` is the one live
        foreground owner before it calls this method. Process identity and the
        persisted CLI session incarnation are derived here, not accepted from a
        caller. Reacquiring an expired lease always advances the fence.
        """
        assert_trusted_route_writer()
        if not isinstance(lease_owner, str) or not lease_owner.strip():
            raise ValueError("consumer lease owner is required")
        ttl = self._consumer_ttl(ttl_seconds)
        pid, started = process_identity()
        now = int(time.time())

        def acquire(conn):
            session = conn.execute(
                "SELECT source,started_at FROM sessions WHERE id=?", (session_id,)
            ).fetchone()
            if session is None:
                raise ValueError("consumer requires a persisted current session")
            if str(session["source"] or "").lower() != "cli":
                raise ValueError("consumer session is not an interactive CLI session")
            current = conn.execute(
                "SELECT * FROM kanban_session_consumers WHERE session_id=?", (session_id,)
            ).fetchone()
            same_live_owner = bool(
                current is not None
                and current["lease_owner"] == lease_owner
                and current["owner_pid"] == pid
                and current["owner_started_at"] == started
                and current["session_started_at"] == session["started_at"]
                and (current["lease_expires_at"] or 0) > now
            )
            fence = int(current["consumer_fence"] or 0) if same_live_owner else (
                int(current["consumer_fence"] or 0) + 1 if current is not None else 1
            )
            expires = now + ttl
            conn.execute(
                "INSERT INTO kanban_session_consumers "
                "(session_id,consumer_fence,lease_owner,lease_expires_at,owner_pid,"
                "owner_started_at,session_started_at) VALUES (?,?,?,?,?,?,?) "
                "ON CONFLICT(session_id) DO UPDATE SET "
                "consumer_fence=excluded.consumer_fence,lease_owner=excluded.lease_owner,"
                "lease_expires_at=excluded.lease_expires_at,owner_pid=excluded.owner_pid,"
                "owner_started_at=excluded.owner_started_at,session_started_at=excluded.session_started_at",
                (session_id, fence, lease_owner, expires, pid, started, session["started_at"]),
            )
            return dict(conn.execute(
                "SELECT * FROM kanban_session_consumers WHERE session_id=?", (session_id,)
            ).fetchone())

        return self._execute_write(acquire)

    def renew_kanban_cli_consumer(
        self,
        session_id: str,
        lease_owner: str,
        consumer_fence: int,
        *,
        ttl_seconds: int = KANBAN_CONSUMER_LEASE_SECONDS,
    ) -> dict | None:
        """Renew only the exact, unexpired process/session incarnation."""
        assert_trusted_route_writer()
        ttl = self._consumer_ttl(ttl_seconds)
        pid, started = process_identity()
        now = int(time.time())

        def renew(conn):
            session = conn.execute(
                "SELECT started_at FROM sessions WHERE id=? AND source='cli'", (session_id,)
            ).fetchone()
            if session is None:
                return None
            cursor = conn.execute(
                "UPDATE kanban_session_consumers SET lease_expires_at=? "
                "WHERE session_id=? AND consumer_fence=? AND lease_owner=? "
                "AND owner_pid=? AND owner_started_at=? AND session_started_at=? "
                "AND lease_expires_at>?",
                (
                    now + ttl,
                    session_id,
                    consumer_fence,
                    lease_owner,
                    pid,
                    started,
                    session["started_at"],
                    now,
                ),
            )
            if cursor.rowcount != 1:
                return None
            return dict(conn.execute(
                "SELECT * FROM kanban_session_consumers WHERE session_id=?", (session_id,)
            ).fetchone())

        return self._execute_write(renew)

    def release_kanban_cli_consumer(
        self, session_id: str, lease_owner: str, consumer_fence: int
    ) -> bool:
        """Release only the exact live process incarnation, retaining its fence."""
        assert_trusted_route_writer()
        pid, started = process_identity()

        def release(conn):
            cursor = conn.execute(
                "UPDATE kanban_session_consumers SET lease_owner=NULL,lease_expires_at=NULL,"
                "owner_pid=NULL,owner_started_at=NULL "
                "WHERE session_id=? AND consumer_fence=? AND lease_owner=? "
                "AND owner_pid=? AND owner_started_at=?",
                (session_id, consumer_fence, lease_owner, pid, started),
            )
            return cursor.rowcount == 1

        return bool(self._execute_write(release))
