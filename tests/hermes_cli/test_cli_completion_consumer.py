"""Phase 4 behavior contracts for classic CLI completion consumption."""
import contextlib
import threading
from types import SimpleNamespace

import pytest

import agent.conversation_loop  # preload bootstrap-sensitive imports before the home-I/O guard
import cli as cli_module
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


ENABLED = {
    "kanban": {
        "completion_inbox": {
            "producer_enabled": True,
            "cli_enabled": True,
        }
    }
}


def _active_lease(tmp_path, state, session_id="session"):
    lease, refusal = try_acquire_active_session(
        session_id=session_id,
        surface="cli",
        config=ENABLED,
        registry_home=tmp_path,
        metadata={"live_session_id": session_id},
    )
    assert lease is not None and refusal is None
    lease.activate_completion_consumer(state, ENABLED)
    return lease


def _cli(state, lease, session_id="session"):
    agent = SimpleNamespace(
        session_id=session_id,
        _session_db=state,
        _session_messages=[],
        _ensure_db_session=lambda: None,
        _session_db_created=True,
    )
    return SimpleNamespace(
        config=ENABLED,
        _single_query_mode=False,
        _session_db=state,
        _active_session_lease=lease,
        session_id=session_id,
        agent=agent,
        conversation_history=[],
        _kanban_completion_cursor=None,
        _present_completion_inbox_response=lambda content: None,
    )


def test_delivery_policy_enables_only_classic_cli_consumer():
    DeliveryPolicy.from_config(ENABLED).assert_supported()
    for key in ("gateway_enabled", "tui_enabled", "repair_enabled"):
        with pytest.raises(RuntimeError, match="later phase"):
            DeliveryPolicy.from_config(
                {"kanban": {"completion_inbox": {key: True}}}
            ).assert_supported()


def test_one_shot_cli_cannot_activate_consumer(tmp_path):
    from hermes_cli.cli_completion_consumer import ensure_consumer_authority

    with contextlib.closing(SessionDB(tmp_path / "state.db")) as state:
        state.create_session("session", source="cli")
        lease, refusal = try_acquire_active_session(
            session_id="session",
            surface="cli",
            config=ENABLED,
            registry_home=tmp_path,
            metadata={"live_session_id": "session"},
        )
        assert lease is not None and refusal is None
        cli = _cli(state, lease)
        cli._single_query_mode = True
        assert not ensure_consumer_authority(cli)
        assert state._read_one("SELECT 1 FROM kanban_session_consumers") is None
        lease.release()


def test_before_turn_requires_persisted_context_and_consumer_lease(tmp_path):
    from hermes_cli.cli_completion_consumer import prepare_local_turn

    with contextlib.closing(SessionDB(tmp_path / "state.db")) as state:
        lease, refusal = try_acquire_active_session(
            session_id="session",
            surface="cli",
            config=ENABLED,
            registry_home=tmp_path,
            metadata={"live_session_id": "session"},
        )
        assert lease is not None and refusal is None
        cli = _cli(state, lease)
        cli.agent._session_db_created = False
        with pytest.raises(RuntimeError, match="persisted current session"):
            prepare_local_turn(cli)
        assert state._read_one("SELECT 1 FROM kanban_session_consumers") is None
        lease.release()


def test_transcript_revision_reload_happens_only_after_external_change(tmp_path):
    from hermes_cli.cli_completion_consumer import refresh_external_transcript

    with contextlib.closing(SessionDB(tmp_path / "state.db")) as state:
        state.create_session("session", source="cli")
        state.append_message("session", "user", "old")
        lease = _active_lease(tmp_path, state)
        cli = _cli(state, lease)

        assert refresh_external_transcript(cli, force=True)
        assert [m["content"] for m in cli.conversation_history] == ["old"]
        assert not refresh_external_transcript(cli)

        state.append_message("session", "assistant", "external")
        assert refresh_external_transcript(cli)
        assert [m["content"] for m in cli.conversation_history] == ["old", "external"]
        assert cli.agent._session_messages == cli.conversation_history
        lease.release()


def test_compression_tip_reload_transfers_registry_and_consumer_fence(tmp_path):
    from hermes_cli.cli_completion_consumer import refresh_external_transcript

    with contextlib.closing(SessionDB(tmp_path / "state.db")) as state:
        state.create_session("session", source="cli")
        state.append_message("session", "user", "parent")
        lease = _active_lease(tmp_path, state)
        cli = _cli(state, lease)
        assert refresh_external_transcript(cli, force=True)

        state.end_session("session", "compression")
        state.create_session("tip", source="cli", parent_session_id="session")
        state.append_message("tip", "assistant", "summary")
        assert refresh_external_transcript(cli)
        assert cli.session_id == cli.agent.session_id == lease.session_id == "tip"
        old = state._read_one(
            "SELECT lease_owner FROM kanban_session_consumers WHERE session_id='session'"
        )
        new = state._read_one(
            "SELECT consumer_fence,lease_owner FROM kanban_session_consumers WHERE session_id='tip'"
        )
        assert old["lease_owner"] is None
        assert new["consumer_fence"] == 1 and new["lease_owner"] == lease.lease_id
        assert [m["content"] for m in cli.conversation_history] == ["summary"]
        lease.release()


def test_poll_runs_pending_route_refreshes_transcript_then_acknowledges(tmp_path, monkeypatch):
    from hermes_cli import cli_completion_consumer as consumer_module

    board_path = tmp_path / "board.db"
    with contextlib.closing(SessionDB(tmp_path / "state.db")) as state, \
            kbc.connect_closing(board_path) as board:
        state.create_session("session", source="cli")
        route = state.create_kanban_return_route(
            board_uuid(board), "session", surface="cli", interactive=True
        )
        with trusted_board_controller(state):
            configure_producer(board, ENABLED)
        with trusted_return_route(state, board, route):
            task = kb.create_task(board, title="worker")
        with kb.write_txn(board):
            kb._append_event(board, task, "completed", {"result": "done"})
        delivery_id = board.execute("SELECT id FROM kanban_deliveries").fetchone()[0]

        lease = _active_lease(tmp_path, state)
        cli = _cli(state, lease)
        calls = []
        heartbeat_seen = threading.Event()
        real_heartbeat = lease.heartbeat_completion_consumer

        def heartbeat():
            result = real_heartbeat()
            heartbeat_seen.set()
            return result

        lease.heartbeat_completion_consumer = heartbeat
        cli._present_completion_inbox_response = lambda content: calls.append(
            ("present", content)
        )

        class FakeInbox:
            def __init__(self, board_conn, profile_state, return_route_id):
                assert return_route_id == route
                self.state = profile_state

            def run(self, candidate, agent):
                assert candidate == delivery_id
                calls.append("run")
                assert heartbeat_seen.wait(1)
                response_id = self.state.append_message(
                    "session", "assistant", "durable completion"
                )
                return {"response_message_id": response_id}

            def acknowledge(self, candidate):
                assert candidate == delivery_id
                assert [m["content"] for m in cli.conversation_history] == ["durable completion"]
                calls.append("ack")

        monkeypatch.setattr(consumer_module, "CompletionInbox", FakeInbox)
        monkeypatch.setattr(consumer_module, "discover_board_paths", lambda: [board_path])
        monkeypatch.setattr(consumer_module, "_CONSUMER_HEARTBEAT_INTERVAL_SECONDS", 0.01)
        assert consumer_module.poll_completion_inbox(cli) == 1
        assert calls == ["run", ("present", "durable completion"), "ack"]
        lease.release()


def test_chat_stops_before_staging_when_consumer_authority_is_unavailable(monkeypatch):
    from hermes_cli.cli_chat_turn_mixin import CLIChatTurnMixin

    events = []

    class Dummy(CLIChatTurnMixin):
        agent = SimpleNamespace()
        _active_agent_route_signature = "route"
        _last_turn_interrupted = False
        _secret_capture_callback = None

        def _ensure_runtime_credentials(self): return True
        def _resolve_turn_agent_config(self, message):
            return {"signature": "route", "model": "m", "runtime": {}, "request_overrides": None}
        def _init_agent(self, **kwargs): return True
        def _prepare_completion_before_local_turn(self):
            events.append("prepare")
            return False
        def _chat_stage_user_message(self, agent, message): events.append("stage")

    monkeypatch.setattr(cli_module, "set_secret_capture_callback", lambda callback: None)
    assert Dummy().chat("hello") is None
    assert events == ["prepare"]


@pytest.mark.parametrize("phase", ["idle", "post-turn"])
def test_classic_cli_polls_at_idle_and_post_turn_boundaries(phase):
    from hermes_cli.cli_tui_runtime_mixin import CLITuiRuntimeMixin

    calls = []

    class App:
        def invalidate(self): pass

    class Dummy(CLITuiRuntimeMixin):
        _agent_running = False
        _pet_reasoning = False
        _last_turn_interrupted = False
        _voice_mode = False
        _voice_continuous = False
        _voice_recording = False
        _spinner_text = ""
        _last_scrollback_tool = ""
        _tool_start_time = 0.0
        _pending_tool_info = {}
        _app = App()

        def _check_config_mcp_changes(self): pass
        def _check_termios_drift(self): pass
        def _drain_process_notifications(self, source): pass
        def _maybe_fire_loop_tick(self): pass
        def _maybe_resume_parked_goal(self): pass
        def _pet_react_turn_end(self): pass
        def _turn_summary_emit(self): pass
        def _drain_interrupt_queue_to_pending_input(self): pass
        def _maybe_continue_goal_after_turn(self): pass
        def _maybe_complete_loop_tick_after_turn(self): pass
        def _maybe_poll_completion_inbox(self, source, *, force=False):
            calls.append((source, force))

    cli = Dummy()
    if phase == "idle":
        cli._tui_idle_tick()
        assert calls == [("cli-idle", False)]
    else:
        cli._tui_after_turn()
        assert calls == [("cli-post-turn", True)]
