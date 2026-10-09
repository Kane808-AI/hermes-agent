"""Fenced Phase 5 gateway fallback for routed Kanban completions."""
from __future__ import annotations

import contextlib
import json
import logging
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from hermes_cli.active_sessions import (
    GatewayCompletionConsumerLease,
    acquire_gateway_completion_consumer,
)
from hermes_cli.completion_inbox import CompletionInbox
from hermes_cli.kanban_delivery import DeliveryPolicy

logger = logging.getLogger("gateway.run")

_CONSUMER_HEARTBEAT_INTERVAL_SECONDS = 10.0


@dataclass(frozen=True)
class _GatewayCompletionWork:
    board_path: Path
    delivery_id: str
    route_id: str
    claim: GatewayCompletionConsumerLease


def _route_map(state) -> dict[str, str]:
    rows = state._read_all(
        "SELECT r.return_route_id,r.board_uuid FROM kanban_return_routes r "
        "JOIN sessions s ON s.id=r.session_id AND s.started_at=r.session_started_at "
        "WHERE r.owner_state_path=? AND r.surface IN ('cli','tui','desktop')",
        (str(Path(state.db_path).resolve()),),
    )
    return {str(row["return_route_id"]): str(row["board_uuid"]) for row in rows}


def _pending_deliveries(board, routes: dict[str, str]) -> list[dict]:
    if not routes:
        return []
    from hermes_cli.kanban_delivery_schema import check_delivery_schema

    if not check_delivery_schema(board):
        return []
    identity = board.execute(
        "SELECT board_uuid FROM kanban_board_identity WHERE singleton=1"
    ).fetchone()[0]
    route_ids = [route for route, board_id in routes.items() if board_id == identity]
    if not route_ids:
        return []
    placeholders = ",".join("?" for _ in route_ids)
    rows = board.execute(
        f"SELECT id,return_route_id FROM kanban_deliveries "
        f"WHERE return_route_id IN ({placeholders}) "
        "AND state IN ('pending','leased','transcript_admitted','processed') "
        "AND (next_attempt_at IS NULL OR next_attempt_at<=?) "
        "ORDER BY event_id,id LIMIT 100",
        (*route_ids, int(time.time())),
    ).fetchall()
    return [dict(row) for row in rows]


def _claim_next(
    state,
    config: dict,
    profile_home: Path,
    board_paths: Iterable[Path] | None,
) -> _GatewayCompletionWork | None:
    routes = _route_map(state)
    if not routes:
        return None
    if board_paths is None:
        from hermes_cli.cli_completion_consumer import discover_board_paths

        board_paths = discover_board_paths()
    from hermes_cli.kanban_db_connect import connect_closing

    for path in board_paths:
        board_path = Path(path).resolve()
        with connect_closing(board_path) as board:
            for candidate in _pending_deliveries(board, routes):
                board_id = routes[candidate["return_route_id"]]
                try:
                    claim = acquire_gateway_completion_consumer(
                        state,
                        candidate["return_route_id"],
                        board_id,
                        config,
                        registry_home=profile_home,
                    )
                except PermissionError as exc:
                    logger.debug(
                        "gateway completion skipped route %s: %s",
                        candidate["return_route_id"],
                        exc,
                    )
                    continue
                if claim is None:
                    continue
                return _GatewayCompletionWork(
                    board_path=board_path,
                    delivery_id=candidate["id"],
                    route_id=candidate["return_route_id"],
                    claim=claim,
                )
    return None


def _parse_model_config(session: dict) -> dict:
    raw = session.get("model_config")
    if isinstance(raw, dict):
        return dict(raw)
    try:
        parsed = json.loads(raw or "{}")
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _build_completion_agent(runner, state, claim: GatewayCompletionConsumerLease):
    """Build a quiet native agent from the owning profile and persisted session route."""
    from agent.skill_utils import parse_config_string_list
    from gateway.run import (
        _checkpoint_agent_kwargs,
        _current_max_iterations,
        _load_gateway_config,
        _resolve_runtime_agent_kwargs_for_provider,
    )
    from hermes_cli.tools_config import _get_platform_tools
    from run_agent import AIAgent

    user_config = _load_gateway_config()
    session = state.get_session(claim.session_id)
    if session is None:
        raise PermissionError("gateway completion live session disappeared")
    model, runtime = runner._resolve_session_agent_runtime(user_config=user_config)
    model = str(session.get("model") or model)
    stored_route = _parse_model_config(session)
    stored_runtime = stored_route.get("gateway_runtime") or stored_route
    provider = str((stored_runtime or {}).get("provider") or "").strip()
    if provider:
        runtime = _resolve_runtime_agent_kwargs_for_provider(provider, target_model=model)
        if stored_runtime.get("base_url"):
            runtime["base_url"] = stored_runtime["base_url"]
        if stored_runtime.get("api_mode"):
            runtime["api_mode"] = stored_runtime["api_mode"]

    reasoning = runner._load_reasoning_config(model)
    runner._reasoning_config = reasoning
    runner._service_tier = runner._resolve_session_service_tier()
    turn_route = runner._resolve_turn_agent_config("", model, runtime)
    platform_key = claim.surface
    enabled = sorted(_get_platform_tools(user_config, platform_key))
    disabled = parse_config_string_list(
        (user_config.get("agent") or {}).get("disabled_toolsets")
    ) or None
    provider_routing = getattr(runner, "_provider_routing", {}) or {}
    agent = AIAgent(
        model=turn_route["model"],
        **turn_route["runtime"],
        **_checkpoint_agent_kwargs(user_config),
        max_iterations=_current_max_iterations(),
        quiet_mode=True,
        verbose_logging=False,
        enabled_toolsets=enabled,
        disabled_toolsets=disabled,
        ephemeral_system_prompt=runner._load_ephemeral_system_prompt() or None,
        reasoning_config=reasoning,
        service_tier=runner._service_tier,
        request_overrides=turn_route.get("request_overrides"),
        providers_allowed=provider_routing.get("only"),
        providers_ignored=provider_routing.get("ignore"),
        providers_order=provider_routing.get("order"),
        provider_sort=provider_routing.get("sort"),
        provider_require_parameters=provider_routing.get("require_parameters", False),
        provider_data_collection=provider_routing.get("data_collection"),
        session_id=claim.session_id,
        platform=platform_key,
        session_db=state,
        fallback_model=runner._refresh_fallback_model(),
        skip_memory=True,
        skip_background_review=True,
    )
    stored_prompt = session.get("system_prompt")
    agent._cached_system_prompt = stored_prompt or agent._build_system_prompt()
    return agent


@contextlib.contextmanager
def _maintain_authority(claim: GatewayCompletionConsumerLease):
    stop = threading.Event()
    failures: list[Exception] = []

    def heartbeat() -> None:
        while not stop.wait(_CONSUMER_HEARTBEAT_INTERVAL_SECONDS):
            try:
                if not claim.heartbeat_completion_consumer():
                    raise PermissionError("gateway completion authority could not be renewed")
            except Exception as exc:  # health: allow BLE001 -- any heartbeat failure revokes this authority
                failures.append(exc)
                return

    from agent.memory_provider import spawn_context_thread

    thread = spawn_context_thread(
        heartbeat,
        name="kanban-gateway-completion-heartbeat",
        daemon=True,
    )
    thread.start()
    try:
        yield
        if failures:
            raise PermissionError("gateway completion heartbeat failed") from failures[0]
    finally:
        stop.set()
        thread.join(timeout=1)


def _execute_work(
    runner,
    state,
    work: _GatewayCompletionWork,
    agent_factory: Callable[[Any, Any, GatewayCompletionConsumerLease], Any] | None,
) -> int:
    from hermes_cli.cli_completion_consumer import _guarded_completion_agent
    from hermes_cli.kanban_db_connect import connect_closing

    agent = None
    with connect_closing(work.board_path) as board:
        inbox = CompletionInbox(
            board,
            state,
            work.route_id,
            authority_guard=work.claim.authority_guard,
        )
        try:
            agent = (agent_factory or _build_completion_agent)(runner, state, work.claim)
            with _maintain_authority(work.claim), _guarded_completion_agent(agent):
                inbox.run(work.delivery_id, agent)
            inbox.acknowledge(work.delivery_id)
            return 1
        except Exception:
            with contextlib.suppress(Exception):
                inbox.retry(
                    work.delivery_id,
                    "gateway_execution_failed",
                    delay_seconds=30,
                )
            raise
        finally:
            if agent is not None:
                with contextlib.suppress(Exception):
                    runner._cleanup_agent_resources(agent)


async def poll_gateway_completion_profile(
    runner,
    profile_home: str | Path,
    *,
    state=None,
    config: dict | None = None,
    board_paths: Iterable[Path] | None = None,
    agent_factory: Callable[[Any, Any, GatewayCompletionConsumerLease], Any] | None = None,
) -> int:
    """Consume at most one delivery inside its owning profile's runtime scope."""
    from gateway.run import _async_profile_runtime_scope, _load_gateway_config

    home = Path(profile_home)
    async with _async_profile_runtime_scope(home):
        effective_config = _load_gateway_config() if config is None else config
        policy = DeliveryPolicy.from_config(effective_config)
        policy.assert_supported()
        if not policy.gateway_enabled:
            return 0
        if state is None:
            from hermes_cli.goals import _get_session_db

            state = _get_session_db()
        work = await runner._run_in_executor_with_context(
            _claim_next, state, effective_config, home, board_paths
        )
        if work is None:
            return 0

        registry = getattr(runner, "_turn_leases", None)
        if registry is None:
            work.claim.release()
            raise RuntimeError("gateway completion requires the session turn-lease registry")
        owner_key = f"kanban-completion:{work.claim.route_id}"
        token = None
        try:
            token = await registry.acquire(
                work.claim.session_id,
                owner_key=owner_key,
                generation=time.monotonic_ns(),
            )
            return await runner._run_in_executor_with_context(
                _execute_work, runner, state, work, agent_factory
            )
        finally:
            if token is not None:
                registry.release(token)
            work.claim.release()
