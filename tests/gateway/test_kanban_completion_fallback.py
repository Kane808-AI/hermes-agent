"""Phase 5 contracts for the fenced gateway completion fallback."""
from __future__ import annotations

import asyncio
import contextlib
import json
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from gateway.turn_lease import SessionTurnLeaseRegistry
from hermes_cli import kanban_db as kb, kanban_db_connect as kbc
from hermes_cli.active_sessions import try_acquire_active_session
from hermes_cli.kanban_delivery import (
    DeliveryPolicy,
    board_uuid,
    configure_producer,
    trusted_board_controller,
    trusted_return_route,
)
from hermes_state import SessionDB
from run_agent import AIAgent


GATEWAY_ENABLED = {"kanban": {"completion_inbox": {"gateway_enabled": True}}}
PRODUCER_ENABLED = {"kanban": {"completion_inbox": {"producer_enabled": True}}}


def _native_agent(state, session_id):
    with patch("model_tools.get_tool_definitions", return_value=[]), \
            patch("model_tools.check_toolset_requirements", return_value={}), \
            patch("agent.process_bootstrap.OpenAI"):
        agent = AIAgent(
            api_key="test-key",
            base_url="https://openrouter.ai/api/v1",
            quiet_mode=True,
            skip_context_files=True,
            skip_memory=True,
            skip_background_review=True,
            session_id=session_id,
            session_db=state,
            platform="cli",
        )
    agent.client = MagicMock()
    agent.valid_tool_names = {"kanban_comment"}
    agent._cached_system_prompt = "Stored byte-stable prompt."
    agent._use_prompt_caching = False
    agent.tool_delay = 0
    agent.compression_enabled = False
    agent.context_compressor._micro_compact_enabled = False
    agent._memory_manager = None
    agent.save_trajectories = False
    return agent


@pytest.fixture
def world(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    board_path = tmp_path / "board.db"
    with contextlib.closing(SessionDB(tmp_path / "state.db")) as state, \
            kbc.connect_closing(board_path) as board:
        state.create_session("route-parent", source="cli")
        route = state.create_kanban_return_route(
            board_uuid(board), "route-parent", surface="cli", interactive=True
        )
        with trusted_board_controller(state):
            configure_producer(board, PRODUCER_ENABLED)
        with trusted_return_route(state, board, route):
            task = kb.create_task(board, title="gateway fallback")
        with kb.write_txn(board):
            kb._append_event(board, task, "completed", {"result": "worker done"})
        delivery = board.execute("SELECT id FROM kanban_deliveries").fetchone()[0]
        yield SimpleNamespace(
            root=tmp_path,
            state=state,
            board=board,
            board_path=board_path,
            route=route,
            task=task,
            delivery=delivery,
            board_id=board_uuid(board),
        )


def _compress(state, parent="route-parent", child="live-tip"):
    state.end_session(parent, "compression")
    state.create_session(child, source="cli", parent_session_id=parent)
    state.append_message(child, "assistant", "compression summary")
    assert state.get_compression_tip(parent) == child
    return child


def test_gateway_flag_is_supported_but_later_consumers_remain_refused():
    policy = DeliveryPolicy.from_config(GATEWAY_ENABLED)
    policy.assert_supported()
    assert policy.gateway_enabled
    for flag in ("tui_enabled", "repair_enabled"):
        with pytest.raises(RuntimeError, match="later phase"):
            DeliveryPolicy.from_config(
                {"kanban": {"completion_inbox": {flag: True}}}
            ).assert_supported()
    assert not DeliveryPolicy.from_config({}).gateway_enabled


@pytest.mark.parametrize(
    ("surface", "source"),
    [("cli", "cli"), ("tui", "tui"), ("desktop", "desktop")],
)
def test_gateway_claim_derives_each_eligible_route_tip_from_the_profile(
    tmp_path, monkeypatch, surface, source
):
    from hermes_cli.active_sessions import acquire_gateway_completion_consumer

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    with contextlib.closing(SessionDB(tmp_path / "state.db")) as state, \
            kbc.connect_closing(tmp_path / "board.db") as board:
        state.create_session("origin", source=source)
        route = state.create_kanban_return_route(
            board_uuid(board), "origin", surface=surface, interactive=True
        )
        claim = acquire_gateway_completion_consumer(
            state,
            route,
            board_uuid(board),
            GATEWAY_ENABLED,
            registry_home=tmp_path,
        )
        assert claim.session_id == "origin"
        assert claim.surface == surface
        assert claim.lineage == ("origin",)
        assert claim.consumer_fence == 1
        claim.release()


@pytest.mark.parametrize("foreground_session", ["route-parent", "live-tip"])
def test_gateway_never_claims_while_a_valid_foreground_lineage_lease_exists(
    world, foreground_session
):
    from hermes_cli.active_sessions import acquire_gateway_completion_consumer

    _compress(world.state)
    lease, refusal = try_acquire_active_session(
        session_id=foreground_session,
        surface="cli",
        config={},
        registry_home=world.root,
        metadata={"live_session_id": foreground_session},
        track_liveness=True,
    )
    assert refusal is None and lease is not None
    with pytest.raises(PermissionError, match="foreground"):
        acquire_gateway_completion_consumer(
            world.state,
            world.route,
            world.board_id,
            GATEWAY_ENABLED,
            registry_home=world.root,
        )
    assert world.state._read_one(
        "SELECT 1 FROM kanban_session_consumers WHERE session_id='live-tip'"
    ) is None
    lease.release()


def test_gateway_reaches_route_after_100_foreground_blocked_deliveries(world):
    from gateway.kanban_completion_fallback import _claim_next

    with kb.write_txn(world.board):
        for _ in range(99):
            kb._append_event(world.board, world.task, "completed", {"result": "worker done"})
    world.state.create_session("route-b", source="cli")
    route_b = world.state.create_kanban_return_route(
        world.board_id, "route-b", surface="cli", interactive=True
    )
    with trusted_return_route(world.state, world.board, route_b):
        task_b = kb.create_task(world.board, title="reachable fallback")
    with kb.write_txn(world.board):
        kb._append_event(world.board, task_b, "completed", {"result": "worker done"})

    deliveries = world.board.execute(
        "SELECT id,return_route_id FROM kanban_deliveries ORDER BY event_id,id"
    ).fetchall()
    assert [row["return_route_id"] for row in deliveries] == [world.route] * 100 + [route_b]
    with contextlib.ExitStack() as cleanup:
        foreground, refusal = try_acquire_active_session(
            session_id="route-parent",
            surface="cli",
            config={},
            registry_home=world.root,
            metadata={"live_session_id": "route-parent"},
            track_liveness=True,
        )
        assert refusal is None and foreground is not None
        cleanup.callback(foreground.release)
        work = _claim_next(
            world.state, GATEWAY_ENABLED, world.root, [world.board_path]
        )
        assert work is not None
        cleanup.callback(work.claim.release)
        assert work.board_path == world.board_path.resolve()
        assert work.route_id == route_b
        assert work.delivery_id == deliveries[-1]["id"]
        assert work.claim.session_id == "route-b"
        assert world.state._read_one(
            "SELECT 1 FROM kanban_session_consumers WHERE session_id='route-parent'"
        ) is None
        assert world.board.execute(
            "SELECT COUNT(*) FROM kanban_deliveries WHERE state='pending'"
        ).fetchone()[0] == 101
        # With A foreground-owned and B consumer-owned, the finite scan exhausts.
        assert _claim_next(
            world.state, GATEWAY_ENABLED, world.root, [world.board_path]
        ) is None


def test_gateway_claim_is_profile_pinned_and_fails_closed_on_registry_mismatch(world, tmp_path):
    from hermes_cli.active_sessions import acquire_gateway_completion_consumer

    other_home = tmp_path / "other-profile"
    other_home.mkdir()
    with pytest.raises(PermissionError, match="profile"):
        acquire_gateway_completion_consumer(
            world.state,
            world.route,
            world.board_id,
            GATEWAY_ENABLED,
            registry_home=other_home,
        )


@pytest.mark.parametrize("boundary", ["marker", "receipt", "effect"])
def test_new_foreground_owner_fences_gateway_marker_receipt_and_effect(world, boundary):
    from hermes_cli.active_sessions import acquire_gateway_completion_consumer
    from hermes_cli.completion_inbox import CompletionInbox

    claim = acquire_gateway_completion_consumer(
        world.state,
        world.route,
        world.board_id,
        GATEWAY_ENABLED,
        registry_home=world.root,
    )
    inbox = CompletionInbox(
        world.board,
        world.state,
        world.route,
        authority_guard=claim.authority_guard,
    )
    inbox.claim(world.delivery)
    if boundary != "marker":
        inbox.admit(world.delivery)

    foreground, refusal = try_acquire_active_session(
        session_id="route-parent",
        surface="cli",
        config={},
        registry_home=world.root,
        metadata={"live_session_id": "route-parent"},
        track_liveness=True,
    )
    assert refusal is None and foreground is not None
    with pytest.raises(PermissionError, match="foreground"):
        if boundary == "marker":
            inbox.admit(world.delivery)
        elif boundary == "receipt":
            inbox.publish(
                world.delivery,
                [{"role": "assistant", "content": "must not commit"}],
                0,
                turn_lease_holder="stale",
            )
        else:
            inbox.effect(
                world.delivery,
                0,
                "kanban_comment",
                {"task_id": world.task, "body": "must not run"},
                turn_lease_holder="stale",
            )

    delivery = dict(world.board.execute(
        "SELECT * FROM kanban_deliveries WHERE id=?", (world.delivery,)
    ).fetchone())
    record = world.state._read_one(
        "SELECT response_message_id FROM kanban_inbox_transcripts WHERE delivery_id=?",
        (world.delivery,),
    )
    assert delivery["transcript_message_id"] is None
    assert record is None or record["response_message_id"] is None
    assert kb.list_comments(world.board, world.task) == []
    foreground.release()
    claim.release()


def test_real_gateway_path_runs_once_on_live_tip_then_acknowledges(world):
    from gateway.kanban_completion_fallback import poll_gateway_completion_profile
    from tests.agent.test_run_agent import _mock_response, _mock_tool_call

    child = _compress(world.state)
    registry = SessionTurnLeaseRegistry()

    class Runner:
        _turn_leases = registry

        async def _run_in_executor_with_context(self, func, *args):
            return await asyncio.to_thread(func, *args)

        def _cleanup_agent_resources(self, agent):
            return None

    runner = Runner()
    agent = _native_agent(world.state, child)
    tool = _mock_tool_call(
        name="kanban_comment",
        call_id="gateway-effect",
        arguments=json.dumps({"task_id": world.task, "body": "one gateway effect"}),
    )
    responses = [
        _mock_response(content=None, tool_calls=[tool], finish_reason="tool_calls"),
        _mock_response(content="gateway receipt"),
    ]

    def model_call(*args, **kwargs):
        held = registry._leases[child].holder
        assert held is not None and held.owner_key.startswith("kanban-completion:")
        durable_turn = world.state._read_one(
            "SELECT holder,expires_at FROM session_turn_leases WHERE holder IS NOT NULL"
        )
        assert durable_turn is not None and durable_turn["expires_at"] > time.time()
        authority = world.state._read_one(
            "SELECT lease_owner,consumer_fence FROM kanban_session_consumers WHERE session_id=?",
            (child,),
        )
        assert authority["lease_owner"].startswith("gateway:")
        assert authority["consumer_fence"] == 1
        return responses.pop(0)

    agent.client.chat.completions.create.side_effect = model_call
    calls = []

    def agent_factory(_runner, state, claim):
        assert state is world.state
        assert claim.session_id == child
        calls.append(claim.consumer_fence)
        return agent

    async def run():
        first = await poll_gateway_completion_profile(
            runner,
            world.root,
            state=world.state,
            config=GATEWAY_ENABLED,
            board_paths=[world.board_path],
            agent_factory=agent_factory,
        )
        second = await poll_gateway_completion_profile(
            runner,
            world.root,
            state=world.state,
            config=GATEWAY_ENABLED,
            board_paths=[world.board_path],
            agent_factory=agent_factory,
        )
        return first, second

    assert asyncio.run(run()) == (1, 0)
    assert calls == [1]
    assert agent.client.chat.completions.create.call_count == 2
    assert world.board.execute(
        "SELECT state FROM kanban_deliveries WHERE id=?", (world.delivery,)
    ).fetchone()[0] == "acknowledged"
    record = world.state._read_one(
        "SELECT * FROM kanban_inbox_transcripts WHERE delivery_id=?", (world.delivery,)
    )
    assert record["session_id"] == child
    messages = world.state.get_messages(child)
    assert [
        message["display_metadata"]["completion_inbox"]["kind"]
        for message in messages
        if (message.get("display_metadata") or {}).get("completion_inbox")
    ] == ["marker", "response"]
    assert len(kb.list_comments(world.board, world.task)) == 1
    assert registry._leases[child].holder is None
    authority = world.state._read_one(
        "SELECT lease_owner,consumer_fence FROM kanban_session_consumers WHERE session_id=?",
        (child,),
    )
    assert authority["lease_owner"] is None
    assert authority["consumer_fence"] == 1


def test_gateway_recovers_committed_receipt_without_rerunning_inference(world):
    from gateway.kanban_completion_fallback import poll_gateway_completion_profile
    from hermes_cli.active_sessions import acquire_gateway_completion_consumer
    from hermes_cli.cli_completion_consumer import _guarded_completion_agent
    from hermes_cli.completion_inbox import CompletionInbox
    from tests.agent.test_run_agent import _mock_response

    first_claim = acquire_gateway_completion_consumer(
        world.state,
        world.route,
        world.board_id,
        GATEWAY_ENABLED,
        registry_home=world.root,
    )
    first_agent = _native_agent(world.state, "route-parent")
    first_agent.client.chat.completions.create.return_value = _mock_response(
        content="receipt committed before crash"
    )
    first_inbox = CompletionInbox(
        world.board,
        world.state,
        world.route,
        authority_guard=first_claim.authority_guard,
    )
    with _guarded_completion_agent(first_agent):
        receipt = first_inbox.run(world.delivery, first_agent)
    assert receipt["response_message_id"]
    assert world.board.execute(
        "SELECT state FROM kanban_deliveries WHERE id=?", (world.delivery,)
    ).fetchone()[0] == "processed"
    first_claim.release()

    registry = SessionTurnLeaseRegistry()

    class Runner:
        _turn_leases = registry

        async def _run_in_executor_with_context(self, func, *args):
            return await asyncio.to_thread(func, *args)

        def _cleanup_agent_resources(self, agent):
            return None

    recovery_agent = _native_agent(world.state, "route-parent")
    recovery_agent.client.chat.completions.create.side_effect = AssertionError(
        "committed receipt reran inference"
    )
    assert asyncio.run(poll_gateway_completion_profile(
        Runner(),
        world.root,
        state=world.state,
        config=GATEWAY_ENABLED,
        board_paths=[world.board_path],
        agent_factory=lambda *_: recovery_agent,
    )) == 1
    recovery_agent.client.chat.completions.create.assert_not_called()
    assert world.board.execute(
        "SELECT state FROM kanban_deliveries WHERE id=?", (world.delivery,)
    ).fetchone()[0] == "acknowledged"
    responses = [
        message for message in world.state.get_messages("route-parent")
        if (message.get("display_metadata") or {}).get("completion_inbox", {}).get("kind")
        == "response"
    ]
    assert len(responses) == 1
    assert responses[0]["id"] == receipt["response_message_id"]


def test_gateway_profile_poll_is_disabled_by_default(world):
    from gateway.kanban_completion_fallback import poll_gateway_completion_profile
    from gateway.run_startup import GatewayStartupMixin

    assert "_kanban_completion_watcher" in GatewayStartupMixin._PRE_RECONNECT_WATCHERS

    class Runner:
        _turn_leases = SessionTurnLeaseRegistry()

        async def _run_in_executor_with_context(self, func, *args):
            return await asyncio.to_thread(func, *args)

    assert asyncio.run(poll_gateway_completion_profile(
        Runner(),
        world.root,
        state=world.state,
        config={},
        board_paths=[world.board_path],
        agent_factory=lambda *_: pytest.fail("disabled fallback created an agent"),
    )) == 0
    assert world.board.execute(
        "SELECT state FROM kanban_deliveries WHERE id=?", (world.delivery,)
    ).fetchone()[0] == "pending"
