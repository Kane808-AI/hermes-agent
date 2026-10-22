"""Foreground authority contracts for opted-in TUI and Desktop completion consumers."""
import contextlib
import json
from pathlib import Path

import pytest

from hermes_cli.active_sessions import (
    ActiveSessionLease,
    transfer_active_session,
    try_acquire_active_session,
)
from hermes_cli.kanban_delivery import DeliveryPolicy
from hermes_state import SessionDB


TUI_ENABLED = {"kanban": {"completion_inbox": {"tui_enabled": True}}}
CLI_ENABLED = {"kanban": {"completion_inbox": {"cli_enabled": True}}}


@pytest.fixture
def state(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    with contextlib.closing(SessionDB(tmp_path / "state.db")) as db:
        yield db


@pytest.fixture
def lease_factory(state, tmp_path):
    leases = []

    def acquire(surface, session_id="session", *, registry_home=None):
        lease, refusal = try_acquire_active_session(
            session_id=session_id,
            surface=surface,
            config=TUI_ENABLED,
            registry_home=registry_home or tmp_path,
            metadata={"live_session_id": session_id},
            track_liveness=True,
        )
        assert lease is not None and refusal is None
        leases.append(lease)
        return lease

    yield acquire
    for lease in leases:
        lease.release()


def _consumer(state, session_id="session"):
    row = state._read_one(
        "SELECT * FROM kanban_session_consumers WHERE session_id=?", (session_id,)
    )
    return dict(row) if row is not None else None


def test_real_config_loader_enables_tui_but_keeps_repair_gated(state, tmp_path):
    defaults = DeliveryPolicy.from_config({})
    assert not any((defaults.producer_enabled, defaults.cli_enabled, defaults.gateway_enabled,
                    defaults.tui_enabled, defaults.repair_enabled))
    (tmp_path / "config.yaml").write_text(
        "kanban:\n  completion_inbox:\n    tui_enabled: true\n"
    )
    policy = DeliveryPolicy.from_config()
    policy.assert_supported()
    assert policy.tui_enabled and not policy.cli_enabled and not policy.gateway_enabled
    with pytest.raises(RuntimeError, match="later phase"):
        DeliveryPolicy.from_config(
            {"kanban": {"completion_inbox": {"repair_enabled": True}}}
        ).assert_supported()


@pytest.mark.parametrize("surface,source", [("tui", "tui"), ("desktop", "tui"), ("desktop", "desktop")])
def test_foreground_consumer_renews_reacquires_and_detaches(state, lease_factory, surface, source):
    state.create_session("session", source=source)
    lease = lease_factory(surface)
    assert lease.activate_completion_consumer(state, TUI_ENABLED)
    first = _consumer(state)
    assert first["lease_owner"] == lease.lease_id
    assert lease.heartbeat_completion_consumer()
    assert _consumer(state)["consumer_fence"] == first["consumer_fence"]
    with lease.completion_authority_guard():
        assert lease.consumer_state is state

    state._execute_write(lambda conn: conn.execute(
        "UPDATE kanban_session_consumers SET lease_expires_at=0"
    ))
    assert lease.heartbeat_completion_consumer()
    assert lease.consumer_fence > first["consumer_fence"]
    live_fence = lease.consumer_fence
    lease.release_completion_consumer()
    assert lease.consumer_state is lease.consumer_fence is lease.consumer_expires_at is None
    assert not lease.released
    assert _consumer(state)["lease_owner"] is None
    entries = json.loads(lease.state_path.read_text())["entries"]
    assert [entry["lease_id"] for entry in entries] == [lease.lease_id]
    with pytest.raises(PermissionError, match="not bound"):
        with lease.completion_authority_guard():
            pytest.fail("detached consumer retained authority")
    assert lease.activate_completion_consumer(state, TUI_ENABLED)
    assert lease.consumer_fence > live_fence


@pytest.mark.parametrize("surface,source", [("tui", "tui"), ("desktop", "desktop")])
def test_store_api_still_defaults_to_cli(state, surface, source):
    state.create_session("session", source=source)
    with pytest.raises(ValueError, match="interactive CLI"):
        state.acquire_kanban_cli_consumer("session", "owner")
    authority = state.acquire_kanban_cli_consumer("session", "owner", surface=surface)
    assert state.renew_kanban_cli_consumer("session", "owner", authority["consumer_fence"]) is None
    state.end_session("session", "compression")
    state.create_session("tip", source=source, parent_session_id="session")
    with pytest.raises(PermissionError, match="interactive surface"):
        state.transfer_kanban_cli_consumer("session", "tip", "owner", authority["consumer_fence"])
    assert _consumer(state, "tip") is None
    assert state.release_kanban_cli_consumer("session", "owner", authority["consumer_fence"])


@pytest.mark.parametrize("surface,source", [
    ("cli", "tui"), ("tui", "cli"), ("tui", "desktop"), ("desktop", "cli"),
    ("desktop", "cron"), ("gateway", "tui"),
])
def test_store_refuses_surface_source_mismatch(state, surface, source):
    state.create_session("session", source=source)
    with pytest.raises(ValueError, match="interactive"):
        state.acquire_kanban_cli_consumer("session", "owner", surface=surface)
    assert _consumer(state) is None


@pytest.mark.parametrize("surface", ["cli", "tui", "desktop"])
@pytest.mark.parametrize("config", [{}, "other_surface"])
def test_surface_opt_in_is_independent(state, lease_factory, surface, config):
    state.create_session("session", source=surface)
    lease = lease_factory(surface)
    selected = config if config != "other_surface" else (
        TUI_ENABLED if surface == "cli" else CLI_ENABLED
    )
    assert not lease.activate_completion_consumer(state, selected)
    assert _consumer(state) is None


@pytest.mark.parametrize("changes", [
    {"enabled": False}, {"released": True}, {"surface": "gateway"},
    {"lease_id": "borrowed:upstream", "enabled": False},
])
def test_inert_borrowed_released_and_other_surfaces_cannot_activate(state, changes):
    fields = {"lease_id": "inert", "session_id": "session", "surface": "desktop"}
    fields.update(changes)
    lease = ActiveSessionLease(**fields)
    state.create_session("session", source="tui")
    assert not lease.activate_completion_consumer(state, TUI_ENABLED)
    assert _consumer(state) is None


@pytest.mark.parametrize("surface,source,tip_source", [
    ("tui", "tui", "tui"), ("desktop", "tui", "desktop"), ("desktop", "desktop", "tui"),
])
def test_canonical_compression_transfer_and_reopen_keep_monotonic_fence(
    state, lease_factory, surface, source, tip_source,
):
    state.create_session("session", source=source)
    lease = lease_factory(surface)
    assert lease.activate_completion_consumer(state, TUI_ENABLED)
    original_fence = lease.consumer_fence
    assert transfer_active_session(lease, session_id="session", metadata={"live_session_id": "same"})
    assert lease.consumer_fence == original_fence
    state.end_session("session", "compression")
    state.create_session("stale", source=tip_source, parent_session_id="session")
    state.end_session("stale", "ws_orphan_reap")
    state.create_session("tip", source=tip_source, parent_session_id="session")
    assert transfer_active_session(lease, session_id="tip")
    assert lease.consumer_fence > original_fence
    assert _consumer(state)["lease_owner"] is None
    assert _consumer(state, "stale") is None
    assert lease.heartbeat_completion_consumer()
    with lease.completion_authority_guard():
        assert lease.session_id == "tip"
    tip_fence = lease.consumer_fence
    lease.release()
    reopened = lease_factory(surface, "tip")
    assert reopened.activate_completion_consumer(state, TUI_ENABLED)
    assert reopened.consumer_fence > tip_fence


@pytest.mark.parametrize("source", ["cli", "desktop"])
def test_tui_compression_transfer_refuses_changed_source(state, lease_factory, source):
    state.create_session("session", source="tui")
    lease = lease_factory("tui")
    assert lease.activate_completion_consumer(state, TUI_ENABLED)
    state.end_session("session", "compression")
    state.create_session("tip", source=source, parent_session_id="session")
    assert not transfer_active_session(lease, session_id="tip")
    assert lease.session_id == "session"
    assert _consumer(state, "tip") is None


def test_profile_mismatch_is_refused_and_ambient_profile_switch_preserves_pinning(
    state, lease_factory, tmp_path, monkeypatch,
):
    state.create_session("session", source="tui")
    lease = lease_factory("desktop")
    other_home = tmp_path / "other-profile"
    other_home.mkdir()
    with contextlib.closing(SessionDB(other_home / "state.db")) as other:
        other.create_session("session", source="tui")
        with pytest.raises(PermissionError, match="profile"):
            lease.activate_completion_consumer(other, TUI_ENABLED)
        assert _consumer(other) is None
        assert lease.activate_completion_consumer(state, TUI_ENABLED)
        monkeypatch.setenv("HERMES_HOME", str(other_home))
        assert lease.heartbeat_completion_consumer()
        with lease.completion_authority_guard():
            assert lease.consumer_state is state
        lease.release_completion_consumer()
        assert _consumer(other) is None
        assert _consumer(state)["lease_owner"] is None
        assert not (other_home / "runtime" / "active_sessions.json").exists()


@pytest.mark.parametrize("damage", ["lease", "session", "surface", "process_start", "missing"])
def test_stale_registry_owner_cannot_renew_transfer_or_guard(state, lease_factory, damage):
    state.create_session("session", source="tui")
    lease = lease_factory("desktop")
    assert lease.activate_completion_consumer(state, TUI_ENABLED)
    original = _consumer(state)
    entries = json.loads(lease.state_path.read_text())["entries"]
    if damage == "missing":
        entries = []
    elif damage == "process_start":
        entries[0]["process_start_time"] += 100
    else:
        field, value = {
            "lease": ("lease_id", "replacement"),
            "session": ("session_id", "replacement"),
            "surface": ("surface", "tui"),
        }[damage]
        entries[0][field] = value
    lease.state_path.write_text(json.dumps({"entries": entries}))
    with pytest.raises(PermissionError):
        lease.heartbeat_completion_consumer()
    with pytest.raises(PermissionError):
        with lease.completion_authority_guard():
            pytest.fail("stale registry lease retained authority")
    state.create_session("other", source="tui")
    assert not transfer_active_session(lease, session_id="other")
    assert _consumer(state, "other") is None
    lease.release_completion_consumer()
    assert lease.consumer_state is None
    assert _consumer(state) == original


@pytest.mark.parametrize("sql", [
    "DELETE FROM kanban_session_consumers",
    "UPDATE kanban_session_consumers SET consumer_fence=consumer_fence+1",
    "UPDATE kanban_session_consumers SET lease_owner='replacement'",
    "UPDATE kanban_session_consumers SET lease_expires_at=0",
    "UPDATE kanban_session_consumers SET owner_pid=-1",
    "UPDATE kanban_session_consumers SET owner_started_at=owner_started_at+1",
    "UPDATE sessions SET started_at=started_at+1 WHERE id='session'",
    "UPDATE sessions SET source='cli' WHERE id='session'",
])
def test_guard_refuses_stale_durable_authority(state, lease_factory, sql):
    state.create_session("session", source="tui")
    lease = lease_factory("desktop")
    assert lease.activate_completion_consumer(state, TUI_ENABLED)
    state._execute_write(lambda conn: conn.execute(sql))
    with pytest.raises(PermissionError, match="authority"):
        with lease.completion_authority_guard():
            pytest.fail("stale durable consumer retained authority")


def test_released_foreground_guard_cannot_reenter(state, lease_factory):
    state.create_session("session", source="tui")
    lease = lease_factory("tui")
    assert lease.activate_completion_consumer(state, TUI_ENABLED)
    lease.release()
    with pytest.raises(PermissionError, match="no longer owns"):
        with lease.completion_authority_guard():
            pytest.fail("released lease retained authority")


def test_stale_runtime_cannot_rebind_or_detach_a_transferred_lease(state, lease_factory):
    state.create_session("session", source="tui")
    lease = lease_factory("desktop")
    assert lease.activate_completion_consumer(state, TUI_ENABLED, live_session_id="session")
    assert transfer_active_session(
        lease, session_id="session", metadata={"live_session_id": "successor"}
    )
    with contextlib.closing(SessionDB(state.db_path)) as successor_state:
        assert lease.activate_completion_consumer(
            successor_state, TUI_ENABLED, live_session_id="successor"
        )
        successor = _consumer(successor_state)
        with pytest.raises(PermissionError, match="another live runtime"):
            lease.activate_completion_consumer(state, TUI_ENABLED, live_session_id="session")
        assert lease.consumer_state is successor_state
        with lease.completion_authority_guard() as owner:
            assert owner["metadata"]["live_session_id"] == "successor"
            assert owner["lease_id"] == lease.lease_id
        lease.release_completion_consumer(live_session_id="session")
        assert lease.consumer_state is successor_state
        assert _consumer(successor_state) == successor
        lease.release_completion_consumer(live_session_id="successor")
        assert lease.consumer_state is lease.consumer_fence is lease.consumer_expires_at is None
        assert _consumer(successor_state)["lease_owner"] is None
        assert not lease.released


def test_runtime_mismatch_cannot_create_consumer_authority(state, lease_factory):
    state.create_session("session", source="tui")
    lease = lease_factory("desktop")
    with pytest.raises(PermissionError, match="another live runtime"):
        lease.activate_completion_consumer(state, TUI_ENABLED, live_session_id="stale-runtime")
    assert lease.consumer_state is None
    assert _consumer(state) is None
