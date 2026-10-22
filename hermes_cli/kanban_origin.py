"""Trusted interactive origin capture for Kanban root-task creation."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


_ELIGIBLE_SURFACES = frozenset({"cli", "tui", "desktop"})
_INELIGIBLE_SESSION_SOURCES = frozenset({
    "acp", "cron", "kanban", "oneshot", "subagent", "tool",
})
_SESSION_SOURCES_BY_SURFACE = {
    "cli": frozenset({"cli"}),
    "tui": frozenset({"tui"}),
    "desktop": frozenset({"desktop", "tui"}),
}


@dataclass(frozen=True)
class InteractiveKanbanOrigin:
    state: Any
    session_id: str
    surface: str


def trusted_interactive_origin(agent: Any) -> InteractiveKanbanOrigin | None:
    """Derive capture authority from the live agent, never environment or tool args."""
    if agent is None or getattr(agent, "side_agent", False):
        return None
    from agent.delegation_context import is_delegated_child_process_context, owned_kanban_task
    from agent.oneshot_footprint import is_single_query_session

    if is_single_query_session() or is_delegated_child_process_context() or owned_kanban_task():
        return None
    surface = str(getattr(agent, "platform", "") or "").strip().lower()
    if surface not in _ELIGIBLE_SURFACES:
        return None
    state = getattr(agent, "_session_db", None)
    session_id = str(getattr(agent, "session_id", "") or "").strip()
    if state is None or not session_id:
        raise ValueError(
            f"eligible interactive {surface} origin is missing its trusted session context"
        )
    row = state.get_session(session_id)
    if row is None:
        raise ValueError(
            f"eligible interactive {surface} origin has no persisted current session"
        )
    source = str(row["source"] or "").strip().lower()
    if source in _INELIGIBLE_SESSION_SOURCES:
        return None
    if source not in _SESSION_SOURCES_BY_SURFACE[surface]:
        raise ValueError(
            f"eligible interactive {surface} origin does not match its persisted session surface"
        )
    return InteractiveKanbanOrigin(state=state, session_id=session_id, surface=surface)


def _route_matches_origin(origin: InteractiveKanbanOrigin, identity: str, route_id: str) -> bool:
    route = origin.state.resolve_kanban_return_route(route_id, identity)
    return bool(
        route
        and route["session_id"] == origin.session_id
        and route["surface"] == origin.surface
    )


def _discard_unreferenced_route(origin, conn, identity: str, route_id: str) -> None:
    if conn.execute(
        "SELECT 1 FROM tasks WHERE return_route_id=? LIMIT 1", (route_id,)
    ).fetchone() is None:
        origin.state._discard_kanban_return_route(route_id, identity)


def create_task_with_interactive_origin(
    origin: InteractiveKanbanOrigin,
    conn,
    *,
    parents: tuple[str, ...],
    creator_task_id: str | None,
    create_task: Callable[[], str],
) -> str:
    """Create an eligible task with a resolvable route or commit no new task."""
    from hermes_cli.kanban_delivery import (
        board_uuid,
        task_delivery_origin,
        trusted_return_route,
    )
    from hermes_cli.kanban_db import write_txn

    identity = board_uuid(conn)
    inherited_route, _interactive = task_delivery_origin(conn, parents, creator_task_id)
    if inherited_route is not None:
        if not _route_matches_origin(origin, identity, inherited_route):
            raise ValueError("eligible interactive task has a conflicting inherited return route")
        with write_txn(conn):
            task_id = create_task()
            row = conn.execute(
                "SELECT return_route_id,origin_interactive FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
            if row is None or row[0] != inherited_route or not row[1]:
                raise RuntimeError(
                    "eligible interactive task committed without its inherited return route"
                )
        return task_id

    route_id = origin.state.create_kanban_return_route(
        identity,
        origin.session_id,
        surface=origin.surface,
        interactive=True,
    )
    try:
        with write_txn(conn):
            with trusted_return_route(origin.state, conn, route_id):
                task_id = create_task()
            row = conn.execute(
                "SELECT return_route_id,origin_interactive FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
            if row is None:
                raise RuntimeError("eligible interactive task creation returned no durable task")
            if row[0] == route_id and row[1]:
                return task_id

            # An idempotent retry may return the task committed by the first call.
            _discard_unreferenced_route(origin, conn, identity, route_id)
            if not row[0] or not row[1] or not _route_matches_origin(origin, identity, row[0]):
                raise RuntimeError(
                    "eligible interactive task retry found no matching durable return route"
                )
        return task_id
    except BaseException:
        _discard_unreferenced_route(origin, conn, identity, route_id)
        raise
