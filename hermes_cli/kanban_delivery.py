"""Producer controls, trusted origin binding, and phased consumer rollout policy."""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import json
import sqlite3
import time

from hermes_cli.kanban_delivery_schema import ACTIONABLE_EVENT_KINDS, DELIVERY_WRITER_VERSION, check_delivery_schema

_RETURN_ROUTE: ContextVar[tuple | None] = ContextVar("kanban_trusted_return_route", default=None)
_CONTROLLER: ContextVar[tuple | None] = ContextVar("kanban_trusted_board_controller", default=None)
_ROUTE_BINDING: ContextVar[tuple | None] = ContextVar("kanban_route_binding", default=None)


@contextmanager
def trusted_board_controller(state):
    """Native orchestration supplies the owning SessionDB, never an ID from tools or env."""
    from hermes_state_kanban import assert_trusted_route_writer

    assert_trusted_route_writer()
    identity = state._kanban_controller_id()
    token = _CONTROLLER.set((state, identity))
    try:
        yield
    finally:
        _CONTROLLER.reset(token)


def install_route_binding_guard(conn: sqlite3.Connection) -> None:
    """A connection-local, normally closed capability. Protection triggers stay installed."""
    conn.create_function("kanban_route_binding_authorized", 2,
                         lambda task, route: int(_ROUTE_BINDING.get() == (conn, task, route)))


@dataclass(frozen=True)
class DeliveryPolicy:
    producer_enabled: bool = False
    cli_enabled: bool = False
    gateway_enabled: bool = False
    tui_enabled: bool = False
    repair_enabled: bool = False
    acknowledged_retention_days: int = 30

    @classmethod
    def from_config(cls, config=None):
        if config is None:
            from hermes_cli.config import load_config_readonly
            config = load_config_readonly()
        raw = (config.get("kanban") or {}).get("completion_inbox") or {}
        values = {}
        for key in ("producer_enabled", "cli_enabled", "gateway_enabled", "tui_enabled", "repair_enabled"):
            value = raw.get(key, False)
            if not isinstance(value, bool):
                raise ValueError(f"kanban.completion_inbox.{key} must be a boolean")
            values[key] = value
        retention = raw.get("acknowledged_retention_days", 30)
        if isinstance(retention, bool) or not isinstance(retention, int) or retention < 0:
            raise ValueError("acknowledged_retention_days must be a nonnegative integer")
        return cls(**values, acknowledged_retention_days=retention)

    def assert_supported(self):
        if self.tui_enabled or self.repair_enabled:
            raise RuntimeError("TUI completion consumption and repair require a later phase")


def board_uuid(conn: sqlite3.Connection) -> str:
    check_delivery_schema(conn)
    return conn.execute("SELECT board_uuid FROM kanban_board_identity WHERE singleton=1").fetchone()[0]


def configure_producer(conn: sqlite3.Connection, config=None) -> DeliveryPolicy:
    """Explicit board-owner operation, never an ambient worker/profile startup side effect."""
    from hermes_cli.kanban_db import write_txn
    from hermes_state_kanban import assert_trusted_route_writer

    assert_trusted_route_writer()
    policy = DeliveryPolicy.from_config(config)
    policy.assert_supported()
    controller = _CONTROLLER.get()
    if controller is None:
        raise PermissionError("producer configuration requires a trusted board controller scope")
    state, identity = controller
    if state._kanban_controller_id() != identity:
        raise PermissionError("board controller identity changed")
    with write_txn(conn):
        if not check_delivery_schema(conn, require_capture=policy.producer_enabled):
            raise RuntimeError("incomplete Kanban delivery schema")
        owner = conn.execute("SELECT controller_id FROM kanban_delivery_control WHERE singleton=1").fetchone()[0]
        if owner not in (None, identity):
            raise PermissionError("board belongs to a different controller")
        conn.execute("UPDATE kanban_delivery_control SET controller_id=?,producer_enabled=?, first_event_id="
                     "CASE WHEN first_event_id=0 AND ?=1 THEN "
                     "(SELECT COALESCE(MAX(id),0)+1 FROM task_events) ELSE first_event_id END WHERE singleton=1",
                     (identity, int(policy.producer_enabled), int(policy.producer_enabled)))
    return policy


@contextmanager
def trusted_return_route(state, conn: sqlite3.Connection, route_id: str):
    from hermes_state_kanban import assert_trusted_route_writer

    assert_trusted_route_writer()
    identity = board_uuid(conn)
    if state.resolve_kanban_return_route(route_id, identity) is None:
        raise ValueError("return route is not owned by this profile and board")
    token = _RETURN_ROUTE.set((state, identity, route_id))
    try:
        yield
    finally:
        _RETURN_ROUTE.reset(token)


def task_delivery_origin(conn: sqlite3.Connection, parents, creator_task_id=None) -> tuple[str | None, int]:
    """Called before INSERT, so even an initially blocked child has its immutable origin."""
    sources = set(parents)
    if creator_task_id:
        sources.add(creator_task_id)
    from agent.delegation_context import owned_kanban_task
    owned = owned_kanban_task()
    if owned:
        if creator_task_id not in (None, owned):
            raise PermissionError("workers cannot select another task's return route")
        sources.add(owned)
    routes = set()
    for tid in sources:
        row = conn.execute("SELECT return_route_id FROM tasks WHERE id=?", (tid,)).fetchone()
        if row and row[0]:
            routes.add(row[0])
    if owned:
        owner = conn.execute("SELECT return_route_id FROM tasks WHERE id=?", (owned,)).fetchone()
        if routes and (owner is None or routes != {owner[0]}):
            raise PermissionError("workers cannot select another task's return route")
    bound = _RETURN_ROUTE.get()
    if bound:
        from hermes_state_kanban import assert_trusted_route_writer
        assert_trusted_route_writer()
        state, identity, route = bound
        if identity != board_uuid(conn) or state.resolve_kanban_return_route(route, identity) is None:
            raise ValueError("return route is not owned by this profile and board")
        routes.add(route)
    if len(routes) > 1:
        raise ValueError("task creation cannot combine different return routes")
    route = next(iter(routes), None)
    return route, int(route is not None)


def bind_task_return_route(conn: sqlite3.Connection, task_id: str) -> None:
    """Explicit one-time binding for a legacy task, using only the trusted route scope."""
    from hermes_cli.kanban_db import parent_ids, write_txn
    from hermes_state_kanban import assert_trusted_route_writer

    assert_trusted_route_writer()
    bound = _RETURN_ROUTE.get()
    if bound is None:
        raise PermissionError("binding requires a trusted return route scope")
    state, identity, route = bound
    with write_txn(conn):
        if not check_delivery_schema(conn, require_capture=True):
            raise RuntimeError("incomplete Kanban delivery schema")
        if identity != board_uuid(conn) or state.resolve_kanban_return_route(route, identity) is None:
            raise ValueError("return route is not owned by this profile and board")
        row = conn.execute("SELECT return_route_id,origin_interactive FROM tasks WHERE id=?", (task_id,)).fetchone()
        if row is None:
            raise ValueError("unknown task")
        if row[0] is not None or row[1]:
            raise ValueError("task already has a bound or conflicting origin")
        if conn.execute("SELECT 1 FROM task_events WHERE task_id=? AND delivery_expected=1 LIMIT 1",
                        (task_id,)).fetchone():
            raise ValueError("task already emitted an actionable delivery obligation")
        created = conn.execute("SELECT payload FROM task_events WHERE task_id=? AND kind='created' "
                               "ORDER BY id LIMIT 1", (task_id,)).fetchone()
        creator = (json.loads(created[0] or "{}").get("creator_task_id") if created else None)
        task_delivery_origin(conn, parent_ids(conn, task_id), creator)
        token = _ROUTE_BINDING.set((conn, task_id, route))
        try:
            conn.execute("UPDATE tasks SET return_route_id=?,origin_interactive=1 WHERE id=?", (route, task_id))
        finally:
            _ROUTE_BINDING.reset(token)


def _append_event(conn, task_id, kind, payload=None, *, run_id=None) -> None:
    """SQLite's capture trigger admits the delivery within this exact INSERT transaction."""
    from hermes_cli.kanban_db import write_txn

    if kind in ACTIONABLE_EVENT_KINDS:
        # Even a standalone native INSERT checks readiness under the writer lock.
        with write_txn(conn, allow_nested=True):
            _insert_event(conn, task_id, kind, payload, run_id=run_id)
    else:
        _insert_event(conn, task_id, kind, payload, run_id=run_id)


def _insert_event(conn, task_id, kind, payload=None, *, run_id=None) -> None:
    if not check_delivery_schema(conn, require_capture=True if kind in ACTIONABLE_EVENT_KINDS else None):
        raise RuntimeError("incomplete Kanban delivery schema")
    expected = "(SELECT producer_enabled FROM kanban_delivery_control WHERE singleton=1)" \
        if kind in ACTIONABLE_EVENT_KINDS else "0"
    conn.execute("INSERT INTO task_events(task_id,run_id,kind,payload,created_at,producer_version,delivery_expected) "
                 f"VALUES (?,?,?,?,?,?,{expected})",
                 (task_id, run_id, kind, json.dumps(payload, ensure_ascii=False) if payload else None,
                  int(time.time()), DELIVERY_WRITER_VERSION))


def producer_diagnostics(conn: sqlite3.Connection) -> dict:
    # Diagnostics must remain readable when a missing capture trigger blocks writes.
    check_delivery_schema(conn, require_capture=False)
    placeholders = ",".join("?" for _ in ACTIONABLE_EVENT_KINDS)
    counts = conn.execute(f"""
        SELECT
          COALESCE(SUM(e.producer_version < c.min_writer_version),0),
          COALESCE(SUM(t.return_route_id IS NULL),0),
          COALESCE(SUM(t.return_route_id IS NOT NULL AND d.id IS NULL),0)
        FROM task_events e JOIN tasks t ON t.id=e.task_id
        CROSS JOIN kanban_delivery_control c
        LEFT JOIN kanban_deliveries d ON d.event_id=e.id AND d.return_route_id=t.return_route_id
        WHERE e.delivery_expected=1 OR
          (c.first_event_id>0 AND e.id>=c.first_event_id AND e.producer_version<c.min_writer_version
           AND e.kind IN ({placeholders}))
    """, ACTIONABLE_EVENT_KINDS).fetchone()
    diagnostics = dict(zip(("mixed_version_events", "unrouted_actionable_events", "missing_deliveries"), counts))
    diagnostics["missing_capture_trigger"] = int(not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name='kanban_capture_delivery'").fetchone())
    return diagnostics


def require_producer_ready(conn: sqlite3.Connection) -> None:
    issues = {key: count for key, count in producer_diagnostics(conn).items() if count}
    if issues:
        raise RuntimeError(f"completion inbox rollout blocked: {issues}")
    check_delivery_schema(conn, require_capture=True)


def gc_acknowledged_deliveries(conn: sqlite3.Connection, *, retention_days=None, now=None) -> int:
    from hermes_cli.kanban_db import write_txn

    if retention_days is None:
        retention_days = DeliveryPolicy.from_config().acknowledged_retention_days
    if isinstance(retention_days, bool) or not isinstance(retention_days, int) or retention_days < 0:
        raise ValueError("retention_days must be a nonnegative integer")
    if retention_days == 0:
        return 0
    cutoff = (int(time.time()) if now is None else int(now)) - retention_days * 86400
    with write_txn(conn):
        events = [row[0] for row in conn.execute(
            "DELETE FROM kanban_deliveries WHERE state='acknowledged' "
            "AND acknowledged_at IS NOT NULL AND acknowledged_at < ? RETURNING event_id", (cutoff,))]
        conn.executemany("DELETE FROM task_events WHERE id=? AND NOT EXISTS "
                         "(SELECT 1 FROM kanban_deliveries d WHERE d.event_id=task_events.id)",
                         [(event,) for event in events])
    return len(events)


def gc_events(conn: sqlite3.Connection, *, older_than_seconds=30 * 24 * 3600) -> int:
    """Event retention never removes an event still linked to a durable delivery."""
    from hermes_cli.kanban_db import _retention_seconds, write_txn

    cutoff = int(time.time()) - _retention_seconds(older_than_seconds)
    with write_txn(conn):
        cur = conn.execute("DELETE FROM task_events WHERE created_at < ? AND kind != 'decomposed' "
                           "AND delivery_expected=0 "
                           "AND task_id IN (SELECT id FROM tasks WHERE status IN ('done','archived')) "
                           "AND NOT EXISTS (SELECT 1 FROM kanban_deliveries d WHERE d.event_id=task_events.id)",
                           (cutoff,))
    return int(cur.rowcount or 0)
