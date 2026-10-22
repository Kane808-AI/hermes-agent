"""Foreground UI completion ownership, runtime construction, and admission contracts."""
import contextlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from hermes_cli import kanban_db as kb, kanban_db_notify as kbn
from hermes_cli.completion_inbox import CompletionInbox
from hermes_state import SessionDB
from tests.agent.test_run_agent import _mock_response
from tests.tui_gateway.test_completion_inbox_consumer import response_with_effect, ui_world as ui_world
from tui_gateway import completion_inbox as consumer


def test_isolated_parent_builds_native_agent_with_pinned_prompt_and_releases_only_clients(ui_world, monkeypatch):
    from run_agent import AIAgent

    world = ui_world
    world.session.update(agent=None, _compute_host_active=True)
    pinned_tools = [{"type": "function", "function": {
        "name": "kanban_comment", "description": "Saved schema bytes", "parameters": {"type": "object"},
    }}]
    world.state.update_session_tool_names("origin", {"version": "saved", "tools": pinned_tools})
    built = []
    monkeypatch.setattr(world.server, "_resolve_agent_model_runtime", lambda *_a: (
        "test/model", {"api_key": "test-key", "base_url": "https://openrouter.ai/api/v1", "api_mode": "chat_completions"}
    ))

    class CapturedAgent(AIAgent):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.client = MagicMock()
            self.client.chat.completions.create.return_value = _mock_response(content="isolated committed answer")
            self.release_clients = MagicMock()
            self.close = lambda: pytest.fail("completion cannot end the shared session")
            built.append(self)

    monkeypatch.setattr("run_agent.AIAgent", CapturedAgent)
    with patch("model_tools.get_tool_definitions", return_value=[]), \
            patch("model_tools.check_toolset_requirements", return_value={}), \
            patch("agent.process_bootstrap.OpenAI"):
        assert consumer.poll("ui", world.session) == 1
    assert len(built) == 1
    agent = built[0]
    assert agent._session_db is world.state and agent.session_id == "origin"
    assert agent._cached_system_prompt == "Stored byte-stable prompt."
    assert agent.tools == pinned_tools
    agent.release_clients.assert_called_once()
    assert world.session["agent"] is None
    assert "_completion_agent" not in world.session
    assert world.state.get_session("origin")["ended_at"] is None
    assert world.session["history"][-1]["content"] == "isolated committed answer"


def test_committed_retry_needs_no_agent_or_saved_prompt(ui_world, monkeypatch):
    world = ui_world
    response_with_effect(world)
    acknowledge = CompletionInbox.acknowledge
    monkeypatch.setattr(CompletionInbox, "acknowledge", lambda *_a: (_ for _ in ()).throw(RuntimeError("crash")))
    with pytest.raises(RuntimeError, match="crash"):
        consumer.poll("ui", world.session)
    world.session.update(agent=None, _compute_host_active=True)
    world.state.update_system_prompt("origin", None)
    monkeypatch.setattr(CompletionInbox, "acknowledge", acknowledge)
    monkeypatch.setattr(consumer, "_build_temporary_agent", lambda *_a: pytest.fail("receipt recovery built an agent"))
    assert consumer.poll("ui", world.session) == 1
    assert world.agent.client.chat.completions.create.call_count == 2
    assert len(kb.list_comments(world.board, world.task)) == 1
    assert len([e for e, _ in world.emitted if e == "message.complete"]) == 1


def test_foreign_profile_handle_cannot_claim_or_refresh(ui_world, monkeypatch, tmp_path):
    world = ui_world
    with contextlib.closing(SessionDB(tmp_path / "foreign" / "state.db")) as foreign:
        foreign.create_session("origin", source=world.surface)
        monkeypatch.setattr(world.server, "_session_db", lambda _s: contextlib.nullcontext(foreign))
        with pytest.raises(PermissionError, match="profile"):
            consumer.poll("ui", world.session)
        with pytest.raises(PermissionError, match="profile"):
            consumer.prepare_local_turn("ui", world.session)
        assert foreign._read_one("SELECT 1 FROM kanban_session_consumers") is None
    assert world.board.execute("SELECT state FROM kanban_deliveries").fetchone()[0] == "pending"
    assert world.session["history"] == []
    assert not world.emitted


def test_empty_idle_poll_does_not_extend_session_lifetime(ui_world):
    world = ui_world
    world.session["last_active"] = 123
    with kb.write_txn(world.board):
        world.board.execute("UPDATE kanban_deliveries SET next_attempt_at=9999999999")
    assert consumer.poll("ui", world.session) == 0
    assert world.session["last_active"] == 123
    assert world.lease.consumer_state is None


@pytest.mark.parametrize("queued", ["queued_prompt", "queued_prompts", "_auto_continue_scheduled"])
def test_user_queue_has_priority_over_completion(ui_world, queued):
    world = ui_world
    world.session[queued] = True
    assert consumer.poll("ui", world.session) == 0
    assert world.board.execute("SELECT state FROM kanban_deliveries").fetchone()[0] == "pending"


def test_unrouted_legacy_notification_survives_tui_opt_in(ui_world, monkeypatch):
    world = ui_world
    legacy = kb.create_task(world.board, title="legacy task without a route")
    kbn.add_notify_sub(world.board, task_id=legacy, platform="tui", chat_id="origin")
    kb.complete_task(world.board, legacy, summary="legacy finished")
    response_with_effect(world)
    submitted = []
    monkeypatch.setattr(world.server, "_notif_submit", lambda *_a, **_kw: submitted.append(_a))
    world.server._notif_poll_kanban("ui", world.session)
    assert len(submitted) == 1
    assert legacy in submitted[0][3]
    assert world.task not in submitted[0][3]


def test_user_turn_refresh_uses_session_profile_before_staging(ui_world, monkeypatch):
    from hermes_constants import get_hermes_home, set_hermes_home_override, reset_hermes_home_override
    from tests.tui_gateway.test_completion_inbox_consumer import TUI_ENABLED

    world = ui_world
    world.session["_turn_cancel_requested"] = True  # A prior Stop must not wedge the next user turn.
    world.state.append_message("origin", "assistant", "externally committed reply")
    monkeypatch.setattr(world.server, "_load_cfg", lambda: TUI_ENABLED if str(get_hermes_home()) == str(world.root) else {})
    token = set_hermes_home_override(str(world.root / "launch"))
    try:
        error, _ = world.server._lock_in_submit_turn(
            "request", "ui", world.session, "next user input", {}, False, None, None, None,
        )
        assert str(get_hermes_home()) == str(world.root / "launch")
    finally:
        reset_hermes_home_override(token)
    assert error is None
    assert world.session["history"][-1]["content"] == "externally committed reply"
    assert world.session["inflight_turn"]["user"] == "next user input"
    assert not world.session["_turn_cancel_requested"]


def test_replaced_runtime_cannot_publish_or_release_successor_consumer(ui_world, monkeypatch):
    from hermes_cli.active_sessions import transfer_active_session

    world = ui_world
    successor = {}

    def take_over(*_a, **_kw):
        assert transfer_active_session(world.lease, session_id="origin", metadata={"live_session_id": "successor"})
        successor["lease"] = world.session.pop("active_session_lease")
        return _mock_response(content="stale completion response")

    world.agent.client.chat.completions.create.side_effect = take_over
    with pytest.raises((PermissionError, RuntimeError)):
        consumer.poll("ui", world.session)
    assert not world.emitted
    assert all(m["role"] != "assistant" for m in world.state.get_messages("origin"))
    assert successor["lease"].consumer_state is world.state
    successor["lease"].release_completion_consumer(live_session_id="successor")


def test_disconnected_transport_keeps_exact_answer_for_replay_and_hydration(ui_world, monkeypatch):
    world = ui_world
    frames = []
    world.session["transport"] = SimpleNamespace(write=lambda frame: frames.append(frame) or False)
    monkeypatch.setattr(world.server, "_sessions", {"ui": world.session})
    monkeypatch.setattr(world.server, "_emit", lambda event, sid, payload=None:
                        world.server.write_json(world.server._event_frame(event, sid, payload)))
    world.agent.client.chat.completions.create.return_value = _mock_response(content="durable despite disconnect")
    assert consumer.poll("ui", world.session) == 1
    types = [frame["params"]["type"] for frame in frames]
    assert types == ["status.update", "session.info", "message.start", "message.delta", "message.complete"]
    assert all(frame["params"]["seq"] > 0 for frame in frames)
    assert world.board.execute("SELECT state FROM kanban_deliveries").fetchone()[0] == "acknowledged"
    visible = world.server._history_to_messages(world.state.get_messages_as_conversation("origin"))
    assert [row["text"] for row in visible] == ["durable despite disconnect"]


def test_identical_completions_push_distinct_committed_bubbles(ui_world):
    from hermes_cli.kanban_delivery import trusted_return_route

    world = ui_world
    with trusted_return_route(world.state, world.board, world.route):
        second = kb.create_task(world.board, title="second completion")
    kb.complete_task(world.board, second, summary="finished too")
    world.agent.client.chat.completions.create.return_value = _mock_response(content="Done.")
    assert consumer.poll("ui", world.session) == 1
    assert consumer.poll("ui", world.session) == 1
    pushes = [p for e, p in world.emitted if e == "message.complete"]
    assert [p["text"] for p in pushes] == ["Done.", "Done."]
    assert len({p["persisted_turn"]["final_assistant_row_id"] for p in pushes}) == 2
    assert [p for e, p in world.emitted if e == "message.delta"] == [{"text": "Done."}, {"text": "Done."}]


def test_failed_completion_never_exposes_staged_rows_to_ordinary_exit_flush(ui_world, monkeypatch):
    class CrashBeforeReceipt(BaseException):
        pass

    world = ui_world
    response_with_effect(world)

    def crash(*_a, **_kw):
        assert world.session["_completion_agent"] is not world.session["agent"]
        assert not any(m.get("role") in {"assistant", "tool"} for m in world.agent._session_messages)
        raise CrashBeforeReceipt()

    monkeypatch.setattr(CompletionInbox, "publish", crash)
    with pytest.raises(CrashBeforeReceipt):
        consumer.poll("ui", world.session)
    world.server._flush_session_messages(world.session)
    assert [m["role"] for m in world.state.get_messages("origin")] == ["user"]
    assert world.state._read_one("SELECT response_message_id FROM kanban_inbox_transcripts")[0] is None
    assert len(kb.list_comments(world.board, world.task)) == 1
    assert not world.emitted
    assert world.session["_completion_done"].is_set()


def test_resumed_committed_receipt_acknowledges_without_duplicate_hydrated_bubble(ui_world, monkeypatch):
    world = ui_world
    response_with_effect(world)
    acknowledge = CompletionInbox.acknowledge
    monkeypatch.setattr(CompletionInbox, "acknowledge", lambda *_a: (_ for _ in ()).throw(RuntimeError("crash")))
    with pytest.raises(RuntimeError, match="crash"):
        consumer.poll("ui", world.session)
    # A restarted/resumed runtime starts from persisted history, with no prior
    # process-local presented set. Hydration has already displayed this row.
    world.session.pop("_completion_presented")
    world.session["history"] = world.state.get_messages_as_conversation("origin", include_row_ids=True)
    assert world.server._history_to_messages(world.session["history"])[-1]["text"] == "committed UI answer"
    world.emitted.clear()
    monkeypatch.setattr(CompletionInbox, "acknowledge", acknowledge)
    assert consumer.poll("ui", world.session) == 1
    assert not world.emitted
    assert world.agent.client.chat.completions.create.call_count == 2
    assert world.board.execute("SELECT state FROM kanban_deliveries").fetchone()[0] == "acknowledged"
