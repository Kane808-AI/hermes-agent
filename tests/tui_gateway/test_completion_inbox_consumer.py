"""Opt-in TUI/Desktop completions commit a fenced receipt before pushing output."""
from __future__ import annotations

import contextlib
import json
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_notify as kbn
from hermes_cli.active_sessions import try_acquire_active_session
from hermes_cli.kanban_delivery import (
    board_uuid, configure_producer, trusted_board_controller, trusted_return_route,
)
from hermes_state import SessionDB


TUI_ENABLED = {"kanban": {"completion_inbox": {"tui_enabled": True}}}
PRODUCER_ENABLED = {"kanban": {"completion_inbox": {"producer_enabled": True}}}


@pytest.fixture(params=["tui", "desktop"])
def ui_world(tmp_path, monkeypatch, request):
    from run_agent import AIAgent
    from tui_gateway import server

    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    surface = request.param
    with contextlib.ExitStack() as cleanup:
        state = cleanup.enter_context(contextlib.closing(SessionDB(tmp_path / "state.db")))
        board_path = tmp_path / "board.db"
        board = cleanup.enter_context(kbc.connect_closing(board_path))
        state.create_session("origin", source=surface, system_prompt="Stored byte-stable prompt.")
        route = state.create_kanban_return_route(
            board_uuid(board), "origin", surface=surface, interactive=True
        )
        with trusted_board_controller(state):
            configure_producer(board, PRODUCER_ENABLED)
        with trusted_return_route(state, board, route):
            task = kb.create_task(board, title="durable UI completion")
        kbn.add_notify_sub(board, task_id=task, platform="tui", chat_id="origin")
        kb.complete_task(board, task, summary="worker finished")
        delivery = board.execute("SELECT id FROM kanban_deliveries").fetchone()[0]
        lease, refusal = try_acquire_active_session(
            session_id="origin", surface=surface, config={}, registry_home=tmp_path,
            metadata={"live_session_id": "ui"}, track_liveness=True,
        )
        assert refusal is None and lease is not None
        cleanup.callback(lease.release)
        with patch("model_tools.get_tool_definitions", return_value=[]), \
                patch("model_tools.check_toolset_requirements", return_value={}), \
                patch("agent.process_bootstrap.OpenAI"):
            agent = AIAgent(
                api_key="test-key", base_url="https://openrouter.ai/api/v1",
                quiet_mode=True, skip_context_files=True, skip_memory=True,
                skip_background_review=True, session_id="origin", session_db=state,
                platform=surface,
            )
        agent.client = MagicMock()
        agent.valid_tool_names = {"kanban_comment"}
        agent._cached_system_prompt = "Stored byte-stable prompt."
        agent._use_prompt_caching = False
        agent.tool_delay = 0
        agent.save_trajectories = False

        class CompletionAgent(AIAgent):
            def __init__(self, **kwargs):
                with patch("model_tools.get_tool_definitions", return_value=[]), \
                        patch("model_tools.check_toolset_requirements", return_value={}), \
                        patch("agent.process_bootstrap.OpenAI"):
                    super().__init__(**kwargs)
                self.client = agent.client  # One deterministic provider double, separate native turn state.
                self.valid_tool_names = {"kanban_comment"}
                self._use_prompt_caching = False
                self.tool_delay = 0
                self.save_trajectories = False

        monkeypatch.setattr("run_agent.AIAgent", CompletionAgent)
        monkeypatch.setattr(server, "_resolve_agent_model_runtime", lambda *_a: (
            "test/model", {"api_key": "test-key", "base_url": "https://openrouter.ai/api/v1", "api_mode": "chat_completions"}
        ))
        session = {
            "session_key": "origin", "source": surface, "profile_home": str(tmp_path),
            "active_session_lease": lease, "agent": agent, "history": [],
            "history_lock": threading.RLock(), "history_version": 0,
            "running": False, "cols": 80,
        }
        emitted = []
        monkeypatch.setattr(server, "_load_cfg", lambda: TUI_ENABLED)
        monkeypatch.setattr(server, "_session_db", lambda _session: contextlib.nullcontext(state))
        monkeypatch.setattr(server, "_emit", lambda event, sid, payload=None: emitted.append((event, payload)) or True)
        monkeypatch.setattr(server, "_notif_submit", lambda *_a, **_kw: pytest.fail("used legacy in-memory dispatch"))
        monkeypatch.setattr(server, "_drain_queued_prompt", lambda *_a: False)
        monkeypatch.setattr(kb, "list_boards", lambda **_kw: [{"slug": "default", "db_path": str(board_path)}])
        monkeypatch.setattr(kb, "kanban_db_path", lambda *_a, **_kw: board_path)
        yield SimpleNamespace(
            root=tmp_path, state=state, board=board, board_path=board_path, route=route,
            task=task, delivery=delivery, lease=lease, agent=agent, session=session,
            emitted=emitted, server=server, surface=surface,
        )


def response_with_effect(world):
    from tests.agent.test_run_agent import _mock_response, _mock_tool_call

    tool = _mock_tool_call(
        name="kanban_comment", call_id="ui-effect",
        arguments=json.dumps({"task_id": world.task, "body": "one UI effect"}),
    )
    world.agent.client.chat.completions.create.side_effect = [
        _mock_response(content=None, tool_calls=[tool], finish_reason="tool_calls"),
        _mock_response(content="committed UI answer"),
    ]


def test_ui_completion_runs_once_and_pushes_only_exact_committed_receipt(ui_world):
    world = ui_world
    response_with_effect(world)
    before = world.agent.client.chat.completions.create.side_effect

    def infer(*_args, **_kwargs):
        assert world.session["running"]
        assert not world.emitted
        consumer = world.state._read_one("SELECT * FROM kanban_session_consumers WHERE session_id='origin'")
        assert consumer["lease_owner"] == world.lease.lease_id
        assert world.state._read_one(
            "SELECT holder FROM session_turn_leases WHERE conversation_id='origin'"
        )["holder"]
        return next(before)

    world.agent.client.chat.completions.create.side_effect = infer
    world.server._notif_poll_kanban("ui", world.session)
    world.server._notif_poll_kanban("ui", world.session)

    assert world.agent.client.chat.completions.create.call_count == 2
    assert len(kb.list_comments(world.board, world.task)) == 1
    assert world.board.execute("SELECT state FROM kanban_deliveries").fetchone()[0] == "acknowledged"
    receipt = world.state._read_one("SELECT * FROM kanban_inbox_transcripts WHERE delivery_id=?", (world.delivery,))
    complete = [payload for event, payload in world.emitted if event == "message.complete"]
    assert len(complete) == 1
    assert complete[0]["text"] == "committed UI answer"
    assert complete[0]["persisted_turn"]["final_assistant_row_id"] == receipt["response_message_id"]
    assert any(world.task in payload["text"] for event, payload in world.emitted if event == "status.update")
    assert world.session["history"][-1]["content"] == "committed UI answer"
    assert world.session["_completion_done"].is_set()
    assert not world.session["running"]
    assert not world.lease.released
    assert world.lease.consumer_state is None


@pytest.mark.parametrize("busy", ["running", "_turn_cancel_requested", "_closing", "_finalized"])
def test_ui_busy_or_stopped_session_leaves_delivery_pending(ui_world, busy):
    world = ui_world
    world.session[busy] = True
    world.server._notif_poll_kanban("ui", world.session)
    world.agent.client.chat.completions.create.assert_not_called()
    assert world.board.execute("SELECT state FROM kanban_deliveries").fetchone()[0] == "pending"
    assert not world.emitted


def test_ui_view_without_foreground_ownership_does_not_claim(ui_world):
    world = ui_world
    world.lease.release()
    world.session.pop("active_session_lease")
    world.server._notif_poll_kanban("ui", world.session)
    world.agent.client.chat.completions.create.assert_not_called()
    assert world.board.execute("SELECT state FROM kanban_deliveries").fetchone()[0] == "pending"
    assert not world.emitted


def test_ui_ack_retry_reuses_receipt_without_inference_or_repeat_push(ui_world, monkeypatch):
    from hermes_cli.completion_inbox import CompletionInbox

    world = ui_world
    response_with_effect(world)
    acknowledge = CompletionInbox.acknowledge
    monkeypatch.setattr(CompletionInbox, "acknowledge", lambda *_a: (_ for _ in ()).throw(RuntimeError("ack interrupted")))
    world.server._notif_poll_kanban("ui", world.session)
    assert world.agent.client.chat.completions.create.call_count == 2
    monkeypatch.setattr(CompletionInbox, "acknowledge", acknowledge)
    with kb.write_txn(world.board):
        world.board.execute("UPDATE kanban_deliveries SET next_attempt_at=NULL")
    world.server._notif_poll_kanban("ui", world.session)
    assert world.agent.client.chat.completions.create.call_count == 2
    assert len(kb.list_comments(world.board, world.task)) == 1
    assert len([1 for event, _ in world.emitted if event == "message.complete"]) == 1
    assert world.board.execute("SELECT state FROM kanban_deliveries").fetchone()[0] == "acknowledged"


def test_ui_compression_refresh_reanchors_lease_and_history(ui_world):
    from tui_gateway.completion_inbox import prepare_local_turn

    world = ui_world
    world.state.end_session("origin", "compression")
    world.state.create_session("tip", source=world.surface, parent_session_id="origin", system_prompt="Compressed prompt.")
    world.agent._cached_system_prompt_static = "old static prefix"
    world.state.append_message("tip", "assistant", "external compression summary")
    prepare_local_turn("ui", world.session)
    assert world.session["session_key"] == world.agent.session_id == world.lease.session_id == "tip"
    assert world.session["history"][-1]["content"] == "external compression summary"
    assert world.agent._cached_system_prompt == "Compressed prompt."
    assert world.agent._cached_system_prompt_static is None


def test_ui_committed_history_hides_internal_marker(ui_world):
    from tests.agent.test_run_agent import _mock_response

    world = ui_world
    world.agent.client.chat.completions.create.return_value = _mock_response(content="visible answer")
    world.server._notif_poll_kanban("ui", world.session)
    rows = world.state.get_messages_as_conversation("origin", include_row_ids=True)
    visible = world.server._history_to_messages(rows)
    assert [message["text"] for message in visible] == ["visible answer"]
