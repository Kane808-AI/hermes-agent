"""Phase 4 fenced completion consumption for the classic interactive CLI only."""
from __future__ import annotations

import contextlib
import threading
import time
from pathlib import Path
from typing import Any

from hermes_cli.completion_inbox import CompletionInbox
from hermes_cli.kanban_delivery import DeliveryPolicy


_MISSING = object()
_CONSUMER_HEARTBEAT_INTERVAL_SECONDS = 10.0


def cli_completion_enabled(cli) -> bool:
    """Read the opt-in without extending authority to one-shot CLI runs."""
    if getattr(cli, "_single_query_mode", False):
        return False
    policy = DeliveryPolicy.from_config(getattr(cli, "config", None))
    policy.assert_supported()
    return policy.cli_enabled


def ensure_consumer_authority(cli) -> bool:
    """Acquire or heartbeat the durable lease owned by this classic CLI."""
    if not cli_completion_enabled(cli):
        return False
    state = getattr(cli, "_session_db", None)
    lease = getattr(cli, "_active_session_lease", None)
    session_id = str(getattr(cli, "session_id", "") or "")
    if state is None or lease is None or not session_id:
        raise RuntimeError("completion consumer is missing its trusted CLI context")
    if lease.surface != "cli":
        raise PermissionError("completion consumer requires the classic CLI surface")
    if lease.session_id != session_id:
        from hermes_cli.active_sessions import transfer_active_session

        if not transfer_active_session(
            lease,
            session_id=session_id,
            metadata={"live_session_id": session_id},
        ):
            raise PermissionError("completion consumer could not transfer session ownership")
    if lease.consumer_state is state and lease.consumer_fence is not None:
        if not lease.heartbeat_completion_consumer():
            raise PermissionError("completion consumer authority could not be renewed")
        return True
    return lease.activate_completion_consumer(state, getattr(cli, "config", None))


def _transcript_cursor(state, session_id: str) -> tuple[str, int]:
    tip = state.get_compression_tip(session_id) or session_id
    row = state._read_one(
        "SELECT COALESCE(MAX(id),0) FROM messages WHERE session_id=? AND active=1",
        (tip,),
    )
    return tip, int(row[0] if row is not None else 0)


def _transfer_live_session(cli, new_session_id: str) -> None:
    old_session_id = str(getattr(cli, "session_id", "") or "")
    if not new_session_id or new_session_id == old_session_id:
        return
    lease = getattr(cli, "_active_session_lease", None)
    if lease is None:
        if cli_completion_enabled(cli):
            raise PermissionError("compression handoff has no active CLI owner")
        cli.session_id = new_session_id
        agent = getattr(cli, "agent", None)
        if agent is not None:
            agent.session_id = new_session_id
        return
    from hermes_cli.active_sessions import transfer_active_session

    if not transfer_active_session(
        lease,
        session_id=new_session_id,
        metadata={"live_session_id": new_session_id},
    ):
        raise PermissionError("compression handoff could not transfer consumer authority")
    cli.session_id = new_session_id
    agent = getattr(cli, "agent", None)
    if agent is not None:
        agent.session_id = new_session_id
        if hasattr(agent, "_session_db_created"):
            agent._session_db_created = True


def refresh_external_transcript(cli, *, force: bool = False) -> bool:
    """Reload when the persisted active-row revision or compression tip changes."""
    if not cli_completion_enabled(cli):
        return False
    state = getattr(cli, "_session_db", None)
    session_id = str(getattr(cli, "session_id", "") or "")
    if state is None or not session_id:
        raise RuntimeError("completion transcript refresh is missing its profile session")
    cursor = _transcript_cursor(state, session_id)
    if not force and cursor == getattr(cli, "_kanban_completion_cursor", None):
        return False
    tip, _revision = cursor
    _transfer_live_session(cli, tip)
    restored = state.get_messages_as_conversation(tip, repair_alternation=True)
    cli.conversation_history = restored
    agent = getattr(cli, "agent", None)
    if agent is not None:
        agent._session_messages = list(restored)
    cli._kanban_completion_cursor = cursor
    return True


def prepare_local_turn(cli) -> bool:
    """Fence and refresh before the typed message can be staged or executed."""
    if not cli_completion_enabled(cli):
        return False
    agent = getattr(cli, "agent", None)
    if agent is None:
        raise RuntimeError("completion consumer has no initialized local agent")
    agent._ensure_db_session()
    state = getattr(cli, "_session_db", None)
    session_id = str(getattr(cli, "session_id", "") or "")
    if (
        not getattr(agent, "_session_db_created", False)
        or state is None
        or state.get_session(session_id) is None
    ):
        raise RuntimeError("completion consumer requires a persisted current session")
    ensure_consumer_authority(cli)
    refresh_external_transcript(cli, force=getattr(cli, "_kanban_completion_cursor", None) is None)
    return True


def discover_board_paths() -> list[Path]:
    """Return existing default and named board stores without creating a board."""
    from hermes_cli import kanban_db as kb

    candidates = [kb.kanban_db_path()]
    for board in kb.list_boards():
        path = board.get("db_path")
        if path:
            candidates.append(Path(path))
    result: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        path = Path(candidate).expanduser()
        try:
            key = path.resolve()
        except OSError:
            continue
        if key in seen or not key.is_file():
            continue
        seen.add(key)
        result.append(key)
    return result


def _route_map(state, session_id: str) -> dict[str, str]:
    session = state.get_session(session_id)
    if session is None or str(session["source"] or "").lower() != "cli":
        return {}
    lineage = state.get_compression_lineage(session_id)
    if not lineage or lineage[-1] != session_id:
        return {}
    placeholders = ",".join("?" for _ in lineage)
    rows = state._read_all(
        "SELECT r.return_route_id,r.board_uuid FROM kanban_return_routes r "
        "JOIN sessions s ON s.id=r.session_id AND s.started_at=r.session_started_at "
        f"WHERE r.session_id IN ({placeholders}) AND r.surface='cli' "
        "AND r.owner_state_path=?",
        (*lineage, str(state.db_path.resolve())),
    )
    return {str(row["return_route_id"]): str(row["board_uuid"]) for row in rows}


def _pending_delivery(board, routes: dict[str, str]) -> dict | None:
    if not routes:
        return None
    from hermes_cli.kanban_delivery_schema import check_delivery_schema

    if not check_delivery_schema(board):
        return None
    identity = board.execute(
        "SELECT board_uuid FROM kanban_board_identity WHERE singleton=1"
    ).fetchone()[0]
    route_ids = [route for route, board_id in routes.items() if board_id == identity]
    if not route_ids:
        return None
    placeholders = ",".join("?" for _ in route_ids)
    now = int(time.time())
    row = board.execute(
        f"SELECT id,return_route_id FROM kanban_deliveries "
        f"WHERE return_route_id IN ({placeholders}) "
        "AND state IN ('pending','leased','transcript_admitted','processed') "
        "AND (next_attempt_at IS NULL OR next_attempt_at<=?) "
        "ORDER BY event_id,id LIMIT 1",
        (*route_ids, now),
    ).fetchone()
    return dict(row) if row is not None else None


def _ensure_poll_agent(cli):
    agent = getattr(cli, "agent", None)
    if agent is not None:
        return agent
    if not hasattr(cli, "_resolve_turn_agent_config") or not hasattr(cli, "_init_agent"):
        raise RuntimeError("completion consumer has no native agent factory")
    route = cli._resolve_turn_agent_config("")
    if not cli._init_agent(
        model_override=route["model"],
        runtime_override=route["runtime"],
        request_overrides=route.get("request_overrides"),
    ):
        raise RuntimeError("completion consumer could not initialize its native agent")
    if cli.agent is None:
        raise RuntimeError("completion consumer initialized no native agent")
    return cli.agent


def _present_receipt(cli, receipt: dict) -> None:
    response_id = receipt.get("response_message_id")
    if not response_id:
        raise RuntimeError("completion receipt has no response message")
    state = cli._session_db
    response = next(
        (
            message
            for message in state.get_messages(cli.session_id)
            if message.get("id") == response_id and message.get("role") == "assistant"
        ),
        None,
    )
    if response is None:
        raise RuntimeError("completion receipt response is absent from the live transcript")
    presenter = getattr(cli, "_present_completion_inbox_response", None)
    if presenter is None:
        raise RuntimeError("classic CLI completion presenter is unavailable")
    presenter(response.get("content") or "")


@contextlib.contextmanager
def _guarded_completion_agent(agent):
    """Disable the automatic effects that the Phase 2 admission contract refuses."""
    saved: list[tuple[Any, str, Any]] = []

    def set_temporarily(target, name, value):
        if target is None:
            return
        old = getattr(target, name, _MISSING)
        saved.append((target, name, old))
        setattr(target, name, value)

    set_temporarily(agent, "compression_enabled", False)
    set_temporarily(getattr(agent, "context_compressor", None), "_micro_compact_enabled", False)
    set_temporarily(agent, "_memory_manager", None)
    set_temporarily(agent, "skip_background_review", True)
    try:
        yield agent
    finally:
        for target, name, old in reversed(saved):
            if old is _MISSING:
                delattr(target, name)
            else:
                setattr(target, name, old)


@contextlib.contextmanager
def _maintain_consumer_authority(cli):
    """Renew the durable fence while a silent completion turn is running."""
    lease = getattr(cli, "_active_session_lease", None)
    if lease is None:
        raise RuntimeError("completion execution has no active CLI owner")
    stop = threading.Event()
    failures: list[Exception] = []

    def heartbeat() -> None:
        while not stop.wait(_CONSUMER_HEARTBEAT_INTERVAL_SECONDS):
            try:
                if not lease.heartbeat_completion_consumer():
                    raise PermissionError("completion consumer authority could not be renewed")
            except Exception as exc:  # health: allow BLE001 -- any heartbeat failure revokes this in-flight authority
                failures.append(exc)
                return

    from agent.memory_provider import spawn_context_thread

    thread = spawn_context_thread(
        heartbeat,
        name="kanban-completion-consumer-heartbeat",
        daemon=True,
    )
    thread.start()
    try:
        yield
        if failures:
            raise PermissionError("completion consumer heartbeat failed") from failures[0]
    finally:
        stop.set()
        thread.join(timeout=1)


def poll_completion_inbox(cli) -> int:
    """Consume at most one pending delivery, then refresh and acknowledge it."""
    if not cli_completion_enabled(cli):
        return 0
    state = getattr(cli, "_session_db", None)
    session_id = str(getattr(cli, "session_id", "") or "")
    if state is None or not session_id or state.get_session(session_id) is None:
        return 0
    ensure_consumer_authority(cli)
    routes = _route_map(state, session_id)
    if not routes:
        return 0
    from hermes_cli.kanban_db_connect import connect_closing

    for path in discover_board_paths():
        with connect_closing(path) as board:
            candidate = _pending_delivery(board, routes)
            if candidate is None:
                continue
            agent = _ensure_poll_agent(cli)
            ensure_consumer_authority(cli)
            inbox = CompletionInbox(board, state, candidate["return_route_id"])
            try:
                with _maintain_consumer_authority(cli), _guarded_completion_agent(agent):
                    receipt = inbox.run(candidate["id"], agent)
            except Exception:
                with contextlib.suppress(Exception):
                    inbox.retry(
                        candidate["id"], "cli_execution_failed", delay_seconds=30
                    )
                raise
            refresh_external_transcript(cli, force=True)
            _present_receipt(cli, receipt)
            inbox.acknowledge(candidate["id"])
            return 1
    return 0
