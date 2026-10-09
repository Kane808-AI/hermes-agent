"""Opt-in foreground TUI/Desktop adapter for the shared durable completion saga."""
from __future__ import annotations

import contextlib
import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace

from hermes_cli.active_sessions import transfer_active_session
from hermes_cli.cli_completion_consumer import (
    _guarded_completion_agent, _maintain_consumer_authority, _pending_delivery,
    discover_board_paths,
)
from hermes_cli.completion_inbox import CompletionInbox
from hermes_cli.kanban_delivery import DeliveryPolicy


def enabled(session: dict, config: dict) -> bool:
    policy = DeliveryPolicy.from_config(config)
    policy.assert_supported()
    return policy.tui_enabled and session.get("source") in {"tui", "desktop"}


def legacy_events(board, events, config):
    """Keep unrouted/diagnostic notifications while the durable inbox owns enrolled events."""
    from hermes_cli.kanban_delivery_schema import check_delivery_schema

    if not DeliveryPolicy.from_config(config).tui_enabled or not check_delivery_schema(board):
        return events
    return [event for event in events if board.execute(
        "SELECT 1 FROM kanban_deliveries WHERE event_id=?", (event.id,),
    ).fetchone() is None]


def _foreground_lease(session):
    lease = session.get("active_session_lease")
    if lease is None or not lease.enabled or lease.released:
        return None
    if lease.surface not in {"tui", "desktop"} or lease.surface != session.get("source"):
        raise PermissionError("completion consumer requires its owning foreground surface")
    return lease


@contextlib.contextmanager
def _authority(sid, session, lease, *, allow_stopped=False):
    with lease.completion_authority_guard() as owner:
        if (session.get("active_session_lease") is not lease
                or session.get("session_key") != lease.session_id
                or session.get("source") != lease.surface
                or (owner.get("metadata") or {}).get("live_session_id") != sid):
            raise PermissionError("completion foreground runtime changed")
        if session.get("_closing") or session.get("_finalized") or (
                session.get("_turn_cancel_requested") and not allow_stopped):
            raise PermissionError("completion session stopped")
        yield


def _refresh(session, state, lease, *, force=False):
    key = str(session.get("session_key") or "")
    tip = state.get_compression_tip(key) or key
    if lease.session_id != key:
        raise PermissionError("completion foreground lease does not own this session")
    if tip != key:
        if not transfer_active_session(lease, session_id=tip):
            raise PermissionError("completion compression ownership transfer failed")
        session["session_key"] = tip
        session["resume_session_id"] = tip
    row = state._read_one("SELECT COALESCE(MAX(id),0) FROM messages WHERE session_id=? AND active=1", (tip,))
    cursor = (tip, int(row[0]))
    if force or session.get("_completion_cursor") != cursor:
        history = state.get_messages_as_conversation(tip, include_row_ids=True, repair_alternation=True)
        session["history"] = history
        session["history_version"] = int(session.get("history_version", 0)) + 1
        session["_completion_cursor"] = cursor
        agent = session.get("agent")
        if agent is not None and not session.get("_compute_host_active"):
            agent.session_id = tip
            agent._session_db_created = True
            agent._session_messages = list(history)
            if tip != key:
                from agent.conversation_loop import _stored_prompt_matches_runtime

                prompt = (state.get_session(tip) or {}).get("system_prompt")
                agent._cached_system_prompt = prompt if prompt and _stored_prompt_matches_runtime(agent, prompt) else None
                agent._cached_system_prompt_static = None
                agent._static_rebuild_failed_for = None
    return tip


def prepare_local_turn(sid: str, session: dict) -> None:
    """Refresh before a local user row or truncation is staged. Caller owns admission."""
    from tui_gateway import server

    with server._session_profile_runtime_scope(session):
        config = server._load_cfg()
        if not enabled(session, config) or (lease := _foreground_lease(session)) is None:
            return
        if session.get("running"):
            raise PermissionError("completion session is busy")
        with server._session_db(session) as state:
            if state is None:
                raise RuntimeError("completion consumer session store is unavailable")
            if state.get_session(str(session.get("session_key") or "")) is None:
                return  # A new draft cannot have a routed delivery yet.
            try:
                if not lease.activate_completion_consumer(state, config, live_session_id=sid):
                    raise PermissionError("foreground completion authority is unavailable")
                _refresh(session, state, lease)
                with _authority(sid, session, lease, allow_stopped=True):
                    pass
            finally:
                lease.release_completion_consumer(live_session_id=sid)


def _routes(state, tip: str) -> dict[str, str]:
    rows = state._read_all(
        "SELECT r.return_route_id,r.board_uuid,r.session_id FROM kanban_return_routes r "
        "JOIN sessions s ON s.id=r.session_id AND s.started_at=r.session_started_at "
        "WHERE r.owner_state_path=? AND r.surface IN ('tui','desktop')",
        (str(Path(state.db_path).resolve()),),
    )
    return {
        str(row["return_route_id"]): str(row["board_uuid"])
        for row in rows if state.get_compression_tip(str(row["session_id"])) == tip
    }


def _build_temporary_agent(sid, session, state, tip):
    """The parent owns isolated-worker receipts. Build no memory or surface services."""
    from run_agent import AIAgent
    from tui_gateway import server

    stored = state.get_session(tip)
    if not stored or not stored.get("system_prompt"):
        raise PermissionError("completion requires the persisted session system prompt")
    overrides = server._stored_session_runtime_overrides(stored)
    model, runtime = server._resolve_agent_model_runtime(
        overrides.get("model_override"), overrides.get("provider_override")
    )
    config = server._load_cfg()
    routing = server._load_provider_routing()
    native_runtime = {key: runtime.get(key) for key in (
        "provider", "requested_provider", "base_url", "api_key", "api_mode", "credential_pool", "request_overrides"
    )}
    agent = AIAgent(
        model=model, **native_runtime,
        max_iterations=server._cfg_max_turns(config, 500),
        quiet_mode=True, verbose_logging=False, skip_context_files=True,
        skip_memory=True, skip_background_review=True,
        enabled_toolsets=server._load_enabled_toolsets(session["source"]),
        disabled_toolsets=server._load_disabled_toolsets(),
        reasoning_config=overrides.get("reasoning_config_override", server._load_reasoning_config(str(model or ""))),
        service_tier=overrides.get("service_tier_override", server._load_service_tier()),
        providers_allowed=routing.get("only"), providers_ignored=routing.get("ignore"),
        providers_order=routing.get("order"), provider_sort=routing.get("sort"),
        provider_require_parameters=routing.get("require_parameters", False),
        provider_data_collection=routing.get("data_collection"),
        session_id=tip, session_db=state, platform=session["source"],
        cwd=server._session_cwd(session), user_id=server._session_auth_user_id(session),
    )
    try:
        from agent.conversation_loop import _stored_prompt_matches_runtime

        if not _stored_prompt_matches_runtime(agent, stored["system_prompt"]):
            raise PermissionError("completion prompt does not match its persisted runtime")
        agent._cached_system_prompt = stored["system_prompt"]
        # Completion admission intentionally skips ordinary turn-start prompt restore.
        # Preserve the saved schema prefix without a tool-refresh write or new services.
        pin = json.loads(stored.get("tool_names") or "null")
        if isinstance(pin, dict) and isinstance(pin.get("tools"), list):
            agent.tools = pin["tools"]
            agent.valid_tool_names = {tool["function"]["name"] for tool in agent.tools}
        return agent
    except BaseException:
        agent.release_clients()
        raise


@contextlib.contextmanager
def _execution_agent(sid, session, state, tip):
    from agent.completion_inbox_silence import silent_scope

    # Reapers/finalizers may persist the resident agent from another thread.
    # Never expose staged completion rows through that ordinary persistence path.
    with silent_scope():
        agent = _build_temporary_agent(sid, session, state, tip)
        with session["history_lock"]:
            session["_completion_agent"] = agent
        try:
            if session.get("_turn_cancel_requested"):
                raise PermissionError("completion session stopped during agent construction")
            yield agent
        finally:
            with session["history_lock"]:
                session.pop("_completion_agent", None)
            agent.release_clients()  # close() would end the shared session and its child resources.


def _present(sid, session, inbox, candidate, receipt):
    from hermes_cli import kanban_db as kb
    from tui_gateway import server

    response_id = receipt["response_message_id"]
    presented = session.setdefault("_completion_presented", set())
    if response_id in presented:
        return
    rows = inbox.state.get_messages(receipt["session_id"])
    response = next((row for row in rows if row["id"] == response_id and row["role"] == "assistant"), None)
    if response is None:
        raise RuntimeError("exact committed completion response is missing")
    event = inbox.board.execute(
        "SELECT e.* FROM task_events e JOIN kanban_deliveries d ON d.event_id=e.id WHERE d.id=?",
        (candidate["id"],),
    ).fetchone()
    task = kb.get_task(inbox.board, event["task_id"])
    notice = server._format_kanban_event_text(
        {"task_id": event["task_id"]}, task,
        SimpleNamespace(kind=event["kind"], payload=json.loads(event["payload"] or "{}")),
        next((b.get("slug", "") for b in kb.list_boards()
              if b.get("db_path") and Path(b["db_path"]).resolve() == inbox.board_path.resolve()), ""),
    )
    with inbox._authority_guard():
        if notice:
            server._emit("status.update", sid, {"kind": "process", "text": notice})
        server._emit("session.info", sid, server._session_info(session.get("agent"), session))
        server._emit("message.start", sid)
        text = response.get("content") or ""
        payload = {
            "text": text, "status": "complete",
            "persisted_turn": {
                "row_ids": [receipt["marker_message_id"], response_id], "complete": False,
                "user_row_id": receipt["marker_message_id"], "final_assistant_row_id": response_id,
            },
        }
        if rendered := server.render_message(text, session.get("cols", 80)):
            payload["rendered"] = rendered
        # Seed a separate committed bubble even when consecutive replies have
        # identical text. Frontends otherwise coalesce non-streaming duplicates.
        server._emit("message.delta", sid, {"text": text})
        # write_json stamps the replay ring before attempting transport delivery.
        # A disconnected client can replay this frame or hydrate the durable row.
        server._emit("message.complete", sid, payload)
        presented.add(response_id)


def _consume(sid, session, state, lease, config):
    from hermes_cli.kanban_db_connect import connect_closing

    if not lease.activate_completion_consumer(state, config, live_session_id=sid):
        raise PermissionError("foreground completion authority is unavailable")
    with session["history_lock"]:
        tip = _refresh(session, state, lease)
    routes = _routes(state, tip)
    for path in discover_board_paths():
        with connect_closing(path) as board:
            candidate = _pending_delivery(board, routes)
            if candidate is None:
                continue
            session["last_active"] = time.time()
            inbox = CompletionInbox(board, state, candidate["return_route_id"],
                                    authority_guard=lambda: _authority(sid, session, lease))
            try:
                with _maintain_consumer_authority(SimpleNamespace(_active_session_lease=lease)):
                    inbox.claim(candidate["id"])
                    record = inbox.admit(candidate["id"])
                    if record["response_message_id"] is not None:
                        receipt = inbox.reconcile(candidate["id"])
                    else:
                        with _execution_agent(sid, session, state, tip) as agent, _guarded_completion_agent(agent):
                            receipt = inbox.run(candidate["id"], agent)
                    with session["history_lock"]:
                        _refresh(session, state, lease, force=True)
                    _present(sid, session, inbox, candidate, receipt)
                    inbox.acknowledge(candidate["id"])
                return 1
            except Exception:
                with contextlib.suppress(Exception):
                    inbox.retry(candidate["id"], "tui_execution_failed", delay_seconds=30)
                raise
    return 0


def poll(sid: str, session: dict) -> int:
    """Run at most one completion while owning the same admission as a local turn."""
    from tui_gateway import server
    from hermes_cli.backend_retirement import retirement

    with retirement.work() as admitted, server._session_profile_runtime_scope(session):
        if not admitted:
            return 0
        config = server._load_cfg()
        if not enabled(session, config) or (lease := _foreground_lease(session)) is None:
            return 0
        with session["history_lock"]:
            if any(session.get(key) for key in ("running", "_turn_cancel_requested", "_closing", "_finalized",
                                                "queued_prompt", "queued_prompts", "_auto_continue_scheduled")):
                return 0
            # Resume already hydrated these exact response rows. Seed once before
            # refreshing so an acknowledgement retry cannot duplicate their bubbles.
            session.setdefault("_completion_presented", {
                row["_row_id"] for row in session.get("history", [])
                if row.get("_row_id") and row.get("role") == "assistant" and row.get("display_kind") != "hidden"
                and (row.get("display_metadata") or {}).get("completion_inbox", {}).get("kind") == "response"
            })
            session["running"] = session["_completion_running"] = True
            done = session["_completion_done"] = threading.Event()
        try:
            with server._session_db(session) as state:
                if state is None:
                    raise RuntimeError("completion consumer session store is unavailable")
                try:
                    return _consume(sid, session, state, lease, config)
                finally:
                    lease.release_completion_consumer(live_session_id=sid)
        finally:
            with session["history_lock"]:
                session["running"] = False
                session.pop("_completion_running", None)
                done.set()
            server._drain_queued_prompt("__completion__", sid, session)


def poll_guarded(sid: str, session: dict) -> None:
    from tui_gateway import server

    try:
        poll(sid, session)
    except Exception as exc:  # health: allow BLE001 -- notification boundary logs failure and leaves the durable retry intact
        server._notif_log_failure("durable completion poll failed", exc)


def kick(sid: str, session: dict) -> None:
    """Keep isolated-host output readers free while a completion runs."""
    from tui_gateway import server

    with server._session_profile_runtime_scope(session):
        if enabled(session, server._load_cfg()):
            server._start_session_work(lambda: poll_guarded(sid, session), name=f"tui-completion-{sid}")


def start_isolated_poller(sid: str, session: dict) -> None:
    """Isolated parents have no native-agent notification poller to service receipts."""
    from agent.memory_provider import spawn_context_thread
    from tui_gateway import server

    with server._session_profile_runtime_scope(session):
        if not enabled(session, server._load_cfg()) or not server._session_uses_compute_host(session):
            return
        if _foreground_lease(session) is None:
            return
    with session["history_lock"]:
        if session.get("_completion_stop") is not None:
            return
        stop = session["_completion_stop"] = threading.Event()

    def tick():
        while not stop.wait(server._KANBAN_POLL_SECONDS):
            if session.get("_closing") or session.get("_finalized"):
                return
            poll_guarded(sid, session)

    spawn_context_thread(tick, name=f"tui-completion-{sid}", daemon=True).start()
