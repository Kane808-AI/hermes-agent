"""Reused isolated children reload parent completion receipts without taking authority."""
from __future__ import annotations

import contextlib
import json
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from hermes_cli.active_sessions import transfer_active_session, try_acquire_active_session
from hermes_state import SessionDB
from tui_gateway import server
from tui_gateway.compute_host import ComputeHost


TUI_ENABLED = {"kanban": {"completion_inbox": {"tui_enabled": True}}}


@pytest.fixture
def child_world(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    with contextlib.ExitStack() as cleanup:
        state = cleanup.enter_context(contextlib.closing(SessionDB(tmp_path / "state.db")))
        state.create_session("origin", source="tui", system_prompt="Original cached prompt")
        state.append_message("origin", "user", "original question")
        state.append_message("origin", "assistant", "original answer")
        history = state.get_messages_as_conversation("origin", include_row_ids=True)
        parent, refusal = try_acquire_active_session(
            session_id="origin", surface="desktop", config={}, registry_home=tmp_path,
            metadata={"live_session_id": "ui"}, track_liveness=True,
        )
        assert parent is not None and refusal is None
        cleanup.callback(parent.release)
        agent = SimpleNamespace(
            session_id="origin", _session_db=state, _session_messages=list(history),
            _cached_system_prompt="Original cached prompt",
        )
        session = {
            "session_key": "origin", "source": "desktop", "profile_home": str(tmp_path),
            "agent": agent, "history": list(history), "history_lock": threading.Lock(),
            "history_version": 1, "running": False,
        }
        frame = {
            "sid": "ui", "source": "desktop", "session_key": "origin", "profile_home": str(tmp_path),
            "active_session_lease": {"lease_id": parent.lease_id, "session_id": "origin"},
            "history": [{"role": "assistant", "content": "untrusted frame snapshot"}],
        }
        server._install_borrowed_lease("ui", session, frame)
        monkeypatch.setattr(server, "_sessions", {"ui": session})
        monkeypatch.setattr(server, "_load_cfg", lambda: TUI_ENABLED)
        monkeypatch.setattr(server, "_session_db", lambda _session: contextlib.nullcontext(state))
        host = ComputeHost.__new__(ComputeHost)
        host._transport = SimpleNamespace()
        yield SimpleNamespace(
            state=state, agent=agent, parent=parent, session=session, frame=frame, host=host,
            home=tmp_path,
        )


def test_existing_child_reloads_parent_receipt_and_retains_inert_borrow(child_world):
    world = child_world
    state, child = world.state, world.session
    marker = state.append_message(
        "origin", "user", "internal completion marker", display_kind="completion_inbox",
        display_metadata={"completion_inbox": {"kind": "marker"}},
    )
    response = state.append_message(
        "origin", "assistant", "parent committed response", display_kind="completion_inbox",
        display_metadata={"completion_inbox": {"kind": "response"}},
    )
    registry_before = world.parent.state_path.read_text()
    assert world.host._ensure_server_session(server, world.frame) is child
    assert [row["_row_id"] for row in child["history"][-2:]] == [marker, response]
    assert child["history"][-1]["content"] == "parent committed response"
    assert world.agent._session_messages == child["history"]
    assert world.agent._cached_system_prompt == "Original cached prompt"
    assert child["history_version"] > 1
    assert not child["active_session_lease"].enabled
    assert not child["active_session_lease"].released
    assert world.parent.state_path.read_text() == registry_before
    assert state._read_one("SELECT 1 FROM kanban_session_consumers") is None


def test_existing_child_follows_only_parent_vouched_canonical_compression_tip(child_world):
    world = child_world
    world.state.end_session("origin", "compression")
    world.state.create_session("stale", source="tui", parent_session_id="origin")
    world.state.end_session("stale", "ws_orphan_reap")
    world.state.create_session(
        "tip", source="desktop", parent_session_id="origin", system_prompt="Compressed prompt",
    )
    world.state.append_message("tip", "assistant", "compressed context and parent receipt")
    assert transfer_active_session(world.parent, session_id="tip")
    world.frame.update(session_key="tip", active_session_lease={
        "lease_id": world.parent.lease_id, "session_id": "tip",
    })
    registry_before = world.parent.state_path.read_text()
    child = world.host._ensure_server_session(server, world.frame)
    assert child["session_key"] == child["resume_session_id"] == world.agent.session_id == "tip"
    assert child["active_session_lease"].session_id == "tip"
    assert not child["active_session_lease"].enabled
    assert world.agent._cached_system_prompt == "Compressed prompt"
    assert child["history"][-1]["content"] == "compressed context and parent receipt"
    assert world.parent.state_path.read_text() == registry_before
    assert [entry["session_id"] for entry in json.loads(registry_before)["entries"]] == ["tip"]


@pytest.mark.parametrize("damage", ["vouch", "session", "surface", "profile", "running"])
def test_existing_child_refuses_stale_or_cross_profile_refresh(child_world, damage):
    world = child_world

    def unrelated_session():
        world.state.create_session("unrelated", source="desktop")
        world.frame["session_key"] = "unrelated"
        world.frame["active_session_lease"]["session_id"] = "unrelated"

    damage_handlers = {
        "vouch": lambda: world.frame["active_session_lease"].update(lease_id="different-parent"),
        "session": unrelated_session,
        "surface": lambda: world.frame.update(source="tui"),
        "profile": lambda: world.frame.update(profile_home=str(world.home / "other-profile")),
        "running": lambda: world.session.update(running=True),
    }
    damage_handlers[damage]()
    before = list(world.session["history"])
    registry_before = world.parent.state_path.read_text()
    with pytest.raises(PermissionError):
        world.host._ensure_server_session(server, world.frame)
    assert world.session["history"] == before
    assert world.session["session_key"] == world.agent.session_id == "origin"
    assert world.session["profile_home"] == str(world.home)
    assert world.parent.state_path.read_text() == registry_before


def test_existing_child_refresh_remains_disabled_by_default(child_world, monkeypatch):
    world = child_world
    monkeypatch.setattr(server, "_load_cfg", lambda: {})
    world.state.append_message("origin", "assistant", "later durable response")
    before = list(world.session["history"])
    child = world.host._ensure_server_session(server, world.frame)
    assert child["history"] == before
    assert child["history_version"] == 1
