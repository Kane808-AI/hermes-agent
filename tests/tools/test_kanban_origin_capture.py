"""Phase 3 contracts for trusted interactive Kanban return-route capture."""
from __future__ import annotations

import contextlib
import importlib
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def origin_world(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_PROFILE", "capture-test")
    for key in (
        "HERMES_KANBAN_TASK",
        "HERMES_KANBAN_RUN_ID",
        "HERMES_SESSION_ID",
        "HERMES_SESSION_KEY",
        "HERMES_SESSION_PLATFORM",
        "HERMES_SESSION_SOURCE",
        "HERMES_SINGLE_QUERY_SESSION",
    ):
        monkeypatch.delenv(key, raising=False)

    from hermes_cli import kanban_db as kb
    from hermes_state import SessionDB

    kb._INITIALIZED_PATHS.clear()
    kb.init_db()
    state = SessionDB(home / "state.db")
    try:
        yield SimpleNamespace(home=home, state=state)
    finally:
        state.close()


def _agent(state, session_id: str, platform: str, *, side_agent: bool = False):
    return SimpleNamespace(
        _session_db=state,
        platform=platform,
        session_id=session_id,
        side_agent=side_agent,
    )


def _create(agent, **overrides):
    importlib.import_module("tools.kanban_tools")
    from tools.registry import registry

    args = {"title": "capture origin", "assignee": "peer", **overrides}
    return json.loads(registry.dispatch("kanban_create", args, parent_agent=agent))


def _task(task_id: str):
    from hermes_cli import kanban_db as kb, kanban_db_connect as kbc

    with kbc.connect_closing() as conn:
        return kb.get_task(conn, task_id)


def _board_task_count() -> int:
    from hermes_cli import kanban_db_connect as kbc

    with kbc.connect_closing() as conn:
        return conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]


def _route_count(path: Path) -> int:
    with contextlib.closing(sqlite3.connect(path)) as conn:
        return conn.execute("SELECT COUNT(*) FROM kanban_return_routes").fetchone()[0]


def _route_for_task(world, task_id: str):
    from hermes_cli import kanban_db_connect as kbc
    from hermes_cli.kanban_delivery import board_uuid

    task = _task(task_id)
    with kbc.connect_closing() as conn:
        identity = board_uuid(conn)
    return task, world.state.resolve_kanban_return_route(task.return_route_id, identity)


@pytest.mark.parametrize(
    ("surface", "source"),
    (("cli", "cli"), ("tui", "tui"), ("desktop", "desktop"), ("telegram", "telegram")),
)
def test_eligible_interactive_roots_capture_opaque_route(origin_world, surface, source):
    origin_world.state.create_session("live", source=source)

    result = _create(_agent(origin_world.state, "live", surface))

    assert result["ok"] is True, result
    task, route = _route_for_task(origin_world, result["task_id"])
    assert task.origin_interactive is True
    assert route["session_id"] == "live"
    assert route["surface"] == surface


@pytest.mark.parametrize(
    ("platform", "source", "side_agent"),
    (
        ("acp", "acp", False),
        ("cron", "cron", False),
        ("cli", "kanban", False),
        ("tool", "tool", False),
        ("subagent", "subagent", False),
        ("cli", "cli", True),
    ),
)
def test_ineligible_origins_cannot_mint_routes(
    origin_world, platform, source, side_agent,
):
    origin_world.state.create_session("ineligible", source=source)

    result = _create(
        _agent(origin_world.state, "ineligible", platform, side_agent=side_agent)
    )

    assert result["ok"] is True, result
    task = _task(result["task_id"])
    assert task.return_route_id is None
    assert task.origin_interactive is False
    assert _route_count(origin_world.state.db_path) == 0


def test_one_shot_cli_cannot_mint_a_route(origin_world, monkeypatch):
    origin_world.state.create_session("oneshot", source="cli")
    monkeypatch.setenv("HERMES_SINGLE_QUERY_SESSION", "1")

    result = _create(_agent(origin_world.state, "oneshot", "cli"))

    assert result["ok"] is True, result
    assert _task(result["task_id"]).return_route_id is None
    assert _route_count(origin_world.state.db_path) == 0


def test_dispatcher_run_cannot_mint_a_new_origin(origin_world, monkeypatch):
    origin_world.state.create_session("dispatcher", source="cli")
    monkeypatch.setenv("HERMES_KANBAN_TASK", "missing-dispatcher-task")

    result = _create(_agent(origin_world.state, "dispatcher", "cli"))

    assert result["ok"] is True, result
    assert _task(result["task_id"]).return_route_id is None
    assert _route_count(origin_world.state.db_path) == 0


def test_delegated_subagent_is_rejected_without_creating_a_task(origin_world):
    from agent.delegation_context import delegated_child_context

    origin_world.state.create_session("child", source="subagent")
    before = _board_task_count()
    with delegated_child_context("child"):
        result = _create(_agent(origin_world.state, "child", "subagent"))

    assert "error" in result
    assert _board_task_count() == before
    assert _route_count(origin_world.state.db_path) == 0


def test_environment_and_tool_arguments_are_not_routing_authority(
    origin_world, monkeypatch,
):
    origin_world.state.create_session("live", source="cli")
    origin_world.state.create_session("spoof", source="cli")
    monkeypatch.setenv("HERMES_SESSION_ID", "spoof")

    result = _create(
        _agent(origin_world.state, "live", "cli"),
        session_id="spoof",
    )

    assert result["ok"] is True, result
    task, route = _route_for_task(origin_world, result["task_id"])
    assert task.session_id == "live"
    assert route["session_id"] == "live"


def test_profile_store_comes_from_the_live_agent(origin_world, tmp_path):
    from hermes_state import SessionDB

    other_path = tmp_path / "other-profile" / "state.db"
    other_path.parent.mkdir()
    with contextlib.closing(SessionDB(other_path)) as other:
        other.create_session("same", source="desktop")
        origin_world.state.create_session("same", source="desktop")

        result = _create(_agent(other, "same", "desktop"))

        assert result["ok"] is True, result
        task = _task(result["task_id"])
        from hermes_cli import kanban_db_connect as kbc
        from hermes_cli.kanban_delivery import board_uuid

        with kbc.connect_closing() as conn:
            identity = board_uuid(conn)
        assert other.resolve_kanban_return_route(task.return_route_id, identity)
        assert origin_world.state.resolve_kanban_return_route(task.return_route_id, identity) is None


@pytest.mark.parametrize("missing", ("state", "session_id", "session_row"))
def test_eligible_surface_with_missing_trusted_context_is_visible_error(
    origin_world, missing,
):
    origin_world.state.create_session("live", source="cli")
    agent = _agent(origin_world.state, "live", "cli")
    if missing == "state":
        agent._session_db = None
    elif missing == "session_id":
        agent.session_id = ""
    else:
        agent.session_id = "absent"
    before = _board_task_count()

    result = _create(agent)

    assert "error" in result
    assert "interactive" in result["error"].lower()
    assert _board_task_count() == before
    assert _route_count(origin_world.state.db_path) == 0


def test_direct_tool_dispatch_with_only_environment_identity_stays_unrouted(
    origin_world, monkeypatch,
):
    origin_world.state.create_session("env-only", source="cli")
    monkeypatch.setenv("HERMES_SESSION_ID", "env-only")

    result = _create(None)

    assert result["ok"] is True, result
    assert _task(result["task_id"]).return_route_id is None
    assert _route_count(origin_world.state.db_path) == 0


def test_route_failure_rolls_back_eligible_task_creation(origin_world, monkeypatch):
    origin_world.state.create_session("live", source="cli")
    before = _board_task_count()

    def fail_route(*_args, **_kwargs):
        raise RuntimeError("route write failed")

    monkeypatch.setattr(origin_world.state, "create_kanban_return_route", fail_route)
    result = _create(_agent(origin_world.state, "live", "cli"))

    assert "route write failed" in result["error"]
    assert _board_task_count() == before
    assert _route_count(origin_world.state.db_path) == 0


def test_task_failure_discards_the_provisional_route(origin_world, monkeypatch):
    from hermes_cli import kanban_db as kb

    origin_world.state.create_session("live", source="cli")
    calls = 0
    create_route = origin_world.state.create_kanban_return_route

    def count_route(*args, **kwargs):
        nonlocal calls
        calls += 1
        return create_route(*args, **kwargs)

    def fail_created_event(*_args, **_kwargs):
        raise RuntimeError("task transaction failed after insert")

    monkeypatch.setattr(origin_world.state, "create_kanban_return_route", count_route)
    monkeypatch.setattr(kb, "_append_event", fail_created_event)
    result = _create(_agent(origin_world.state, "live", "cli"))

    assert "task transaction failed after insert" in result["error"]
    assert calls == 1
    assert _board_task_count() == 0
    assert _route_count(origin_world.state.db_path) == 0


def test_idempotent_retry_reuses_the_committed_route(origin_world):
    origin_world.state.create_session("live", source="tui")
    agent = _agent(origin_world.state, "live", "tui")

    first = _create(agent, idempotency_key="stable-create")
    second = _create(agent, idempotency_key="stable-create")

    assert first["ok"] is True and second["ok"] is True
    assert first["task_id"] == second["task_id"]
    assert _board_task_count() == 1
    assert _route_count(origin_world.state.db_path) == 1
