"""Stop must retain admission and target parent-owned completion inference."""
from __future__ import annotations

import contextlib
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


@pytest.fixture(params=[False, True], ids=["warm", "isolated"])
def completion_session(request, monkeypatch):
    from tui_gateway import server

    isolated = request.param
    resident = SimpleNamespace(session_id="origin", hard_interrupt=MagicMock())
    native = SimpleNamespace(session_id="origin", hard_interrupt=MagicMock()) if isolated else resident
    host = SimpleNamespace(interrupt=MagicMock())
    session = {
        "session_key": "origin", "source": "desktop" if isolated else "tui",
        "history_lock": threading.RLock(), "running": True,
        "agent": resident, "_completion_agent": native, "_completion_running": True,
        "_run_thread": SimpleNamespace(is_alive=lambda: False),
        "_compute_host_active": isolated, "queued_prompt": {"text": "old queued prompt"},
        "queued_prompts": [{"text": "another old prompt"}], "_queued_prompt_generation": 7,
    }
    monkeypatch.setattr(server, "_session_uses_compute_host", lambda _s: isolated)
    monkeypatch.setattr(server, "_get_compute_host_supervisor", lambda: host)
    monkeypatch.setattr(server, "_session_profile_runtime_scope", lambda *_a, **_kw: contextlib.nullcontext())
    monkeypatch.setattr(server, "_sess_nowait", lambda *_a: (session, None))
    monkeypatch.setattr(server, "_sess", lambda *_a: (session, None))
    for name in (
        "_clear_pending", "_announce_cancelled_gateway_approvals", "_tts_stream_stop",
        "_resume_wake_after_interrupt", "_retire_turn_marker", "_persist_queued_user_row",
    ):
        monkeypatch.setattr(server, name, lambda *_a, **_kw: None)
    monkeypatch.setattr(server, "_load_busy_input_mode", lambda: "interrupt")
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *_a, **_kw: None)
    monkeypatch.setattr("tools.async_delegation.interrupt_for_session", lambda *_a, **_kw: None)
    monkeypatch.setattr("tools.approval.resolve_gateway_approval", lambda *_a, **_kw: None)
    monkeypatch.setattr("agent.background_review.cancel_background_review_for_live_turn", lambda *_a, **_kw: None)
    return SimpleNamespace(server=server, session=session, resident=resident, native=native, host=host)


def test_stop_completion_reaches_native_agent_and_keeps_new_prompt_queued(completion_session):
    world = completion_session
    result = world.server._methods["session.interrupt"]("stop", {"session_id": "ui"})

    assert result["result"]["status"] == "interrupted"
    world.native.hard_interrupt.assert_called_once_with()
    world.host.interrupt.assert_not_called()
    if world.resident is not world.native:
        world.resident.hard_interrupt.assert_not_called()
    assert world.session["_turn_cancel_requested"] is True
    assert world.session["running"] is True
    assert world.session["queued_prompt"] is None
    assert "queued_prompts" not in world.session
    assert world.session["_queued_prompt_generation"] == 8

    # Stop has been requested, but the old native execution has not unwound. A new
    # prompt must wait for the poller's finally even when ordinary busy mode interrupts.
    queued = world.server._handle_busy_submit("next", "ui", world.session, "next user turn", None)
    assert queued["result"]["status"] == "queued"
    assert world.session["queued_prompt"]["text"] == "next user turn"
    assert world.session["running"] is True
    world.native.hard_interrupt.assert_called_once_with()
    world.host.interrupt.assert_not_called()


def test_stop_during_completion_agent_construction_keeps_admission(completion_session):
    world = completion_session
    world.session.pop("_completion_agent")

    world.server._methods["session.interrupt"]("stop", {"session_id": "ui"})

    assert world.session["_turn_cancel_requested"] is True
    assert world.session["running"] is True
    assert world.session["_completion_running"] is True
    world.resident.hard_interrupt.assert_not_called()
    world.native.hard_interrupt.assert_not_called()
    world.host.interrupt.assert_not_called()


@pytest.mark.parametrize(
    ("queued", "tui_enabled"), [(False, True), (True, True), (False, False)],
    ids=["schedule-idle", "user-queue-wins", "disabled"],
)
def test_isolated_turn_done_schedules_completion_without_blocking_reader(monkeypatch, queued, tui_enabled):
    from tui_gateway import completion_inbox, server

    session = {
        "history_lock": threading.RLock(), "session_key": "origin", "source": "desktop",
        "running": True, "_compute_host_active": True,
    }
    scheduled = []
    poll = MagicMock()
    drain = MagicMock(return_value=queued)
    monkeypatch.setattr(completion_inbox, "poll_guarded", poll)
    monkeypatch.setattr(server, "_session_profile_runtime_scope", lambda *_a, **_kw: contextlib.nullcontext())
    monkeypatch.setattr(server, "_load_cfg", lambda: {"kanban": {"completion_inbox": {"tui_enabled": tui_enabled}}})
    monkeypatch.setattr(server, "_start_session_work", lambda target, **_kw: scheduled.append(target))
    monkeypatch.setattr(server, "_drain_queued_prompt", drain)
    monkeypatch.setattr(server, "_compute_host_session_info", lambda _s: {"stored_session_id": "origin"})
    for name in (
        "_compute_host_adopt_frame_meta", "_clear_inflight_turn", "_apply_compute_host_metadata_mirror",
        "_release_deferred_active_session_lease", "_emit",
    ):
        monkeypatch.setattr(server, name, lambda *_a, **_kw: None)

    # This hook runs on the supervisor's sole stdout reader. It must return before
    # inference so other sessions' messages and control acknowledgements keep flowing.
    server._on_compute_host_turn_done("turn", "ui", session, {"type": "turn.end"})

    poll.assert_not_called()
    drain.assert_called_once_with("turn", "ui", session)
    assert len(scheduled) == int(tui_enabled and not queued)
    if scheduled:
        scheduled[0]()
        poll.assert_called_once_with("ui", session)


def _isolate_completion_close(world, monkeypatch):
    """Exercise actual finalize/close while keeping unrelated lifecycle services inert."""
    for name in ("_lock_vault_managers", "_release_active_session_slot", "_notify_session_boundary",
                 "_announce_session_reclaimed"):
        monkeypatch.setattr(world.server, name, lambda *_a, **_kw: None)
    monkeypatch.setattr(world.server, "_session_db", lambda _s: contextlib.nullcontext(None))
    monkeypatch.setattr("tools.approval.clear_session", lambda *_a, **_kw: None)
    monkeypatch.setattr("tools.approval.unregister_gateway_notify", lambda *_a, **_kw: None)
    world.session.update(_closing=True, _sid="ui")


def test_close_interrupts_temporary_completion_and_waits_for_cleanup(completion_session, monkeypatch):
    from tui_gateway.completion_inbox import _authority

    world = completion_session
    _isolate_completion_close(world, monkeypatch)
    interrupted, done = threading.Event(), threading.Event()
    native = SimpleNamespace(hard_interrupt=MagicMock(side_effect=lambda *_a, **_kw: interrupted.set()),
                             release_clients=MagicMock())
    lease = SimpleNamespace(session_id="origin", surface=world.session["source"],
                            completion_authority_guard=lambda: contextlib.nullcontext(
                                {"metadata": {"live_session_id": "ui"}}))
    world.session.update(_completion_agent=native, _completion_done=done, active_session_lease=lease)
    closed_after_cleanup = []
    world.resident.close = MagicMock(side_effect=lambda: closed_after_cleanup.append(done.is_set()))
    outcomes = []

    def finish_completion():
        try:
            if not interrupted.wait(5):
                outcomes.append("not interrupted")
                return
            try:
                with _authority("ui", world.session, lease):
                    outcomes.append("stale publication allowed")
            except PermissionError:
                outcomes.append("publication fenced")
            native.release_clients()
        finally:
            with world.session["history_lock"]:
                world.session["running"] = False
                world.session.pop("_completion_running", None)
                world.session.pop("_completion_agent", None)
            done.set()

    worker = threading.Thread(target=finish_completion)
    worker.start()
    try:
        assert world.server._teardown_popped_session(world.session, end_reason="tui_close") is True
    finally:
        interrupted.set()
        worker.join(5)

    assert not worker.is_alive()
    assert outcomes == ["publication fenced"]
    assert closed_after_cleanup == [True]
    native.hard_interrupt.assert_called_once_with("session ended (tui_close)", tool_reason="session ended")
    native.release_clients.assert_called_once_with()
    world.resident.hard_interrupt.assert_not_called()
    world.host.interrupt.assert_not_called()


def test_close_timeout_leaves_completion_cleanup_with_its_worker(completion_session, monkeypatch):
    world = completion_session
    _isolate_completion_close(world, monkeypatch)
    entered, release, exited = threading.Event(), threading.Event(), threading.Event()

    def blocked_abort(*_a, **_kw):
        entered.set()
        release.wait(5)
        exited.set()

    native = SimpleNamespace(hard_interrupt=blocked_abort, release_clients=MagicMock())
    done = SimpleNamespace(wait=MagicMock(return_value=False))
    world.session.update(_completion_agent=native, _completion_done=done)
    world.resident.close = MagicMock()
    try:
        # The bounded event wait expires while the provider abort itself is still blocked.
        assert world.server._teardown_popped_session(world.session, end_reason="tui_close") is True
        assert entered.wait(5)
        done.wait.assert_called_once_with(timeout=world.server._TURN_SETTLE_BEFORE_CLOSE_SECONDS)
        assert world.session["running"] is True
        assert world.session["_completion_running"] is True
        assert world.session["_completion_agent"] is native
        assert world.session["_turn_cancel_requested"] is True
        native.release_clients.assert_not_called()
        world.resident.close.assert_called_once_with()
        world.host.interrupt.assert_not_called()
    finally:
        release.set()
        assert exited.wait(5)
