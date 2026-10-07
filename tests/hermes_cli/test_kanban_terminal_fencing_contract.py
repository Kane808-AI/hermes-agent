"""Phase 4 contracts for the classic CLI's durable monotonic consumer fence."""
import contextlib
import time

import pytest

from hermes_cli.active_sessions import try_acquire_active_session
from hermes_state import SessionDB


ENABLED = {"kanban": {"completion_inbox": {"cli_enabled": True}}}


def _row(state, session_id="terminal"):
    row = state._read_one(
        "SELECT * FROM kanban_session_consumers WHERE session_id=?", (session_id,)
    )
    return dict(row) if row is not None else None


def _lease(tmp_path, state, *, session_id="terminal", surface="cli", config=ENABLED):
    lease, refusal = try_acquire_active_session(
        session_id=session_id,
        surface=surface,
        config=config,
        registry_home=tmp_path,
        metadata={"live_session_id": session_id},
    )
    assert lease is not None and refusal is None
    lease.activate_completion_consumer(state, config)
    return lease


@pytest.mark.parametrize("terminal_state", ["idle", "busy", "closing", "closed", "reopened"])
def test_terminal_owner_keeps_monotonic_fence_across_lifecycle(tmp_path, terminal_state):
    with contextlib.closing(SessionDB(tmp_path / "state.db")) as state:
        state.create_session("terminal", source="cli")
        lease = _lease(tmp_path, state)
        first = _row(state)
        assert first["consumer_fence"] == 1
        assert first["lease_owner"] == lease.lease_id

        if terminal_state in {"idle", "busy"}:
            assert lease.heartbeat_completion_consumer()
            assert _row(state)["consumer_fence"] == 1
        if terminal_state in {"closing", "closed", "reopened"}:
            lease.release()
            assert _row(state)["lease_owner"] is None
        if terminal_state == "reopened":
            lease = _lease(tmp_path, state)
            assert _row(state)["consumer_fence"] == 2
        lease.release()


def test_expired_heartbeat_reacquires_with_a_new_fence(tmp_path):
    with contextlib.closing(SessionDB(tmp_path / "state.db")) as state:
        state.create_session("terminal", source="cli")
        lease = _lease(tmp_path, state)
        state._execute_write(
            lambda conn: conn.execute(
                "UPDATE kanban_session_consumers SET lease_expires_at=? WHERE session_id='terminal'",
                (int(time.time()) - 1,),
            )
        )
        assert lease.heartbeat_completion_consumer()
        assert _row(state)["consumer_fence"] == 2
        lease.release()


def test_pid_reuse_cannot_renew_or_release_current_authority(tmp_path, monkeypatch):
    with contextlib.closing(SessionDB(tmp_path / "state.db")) as state:
        state.create_session("terminal", source="cli")
        lease = _lease(tmp_path, state)
        original = _row(state)
        monkeypatch.setattr(
            "hermes_state_kanban.process_identity",
            lambda: (original["owner_pid"], original["owner_started_at"] + 1),
        )
        assert not state.renew_kanban_cli_consumer(
            "terminal", lease.lease_id, original["consumer_fence"]
        )
        assert not state.release_kanban_cli_consumer(
            "terminal", lease.lease_id, original["consumer_fence"]
        )
        assert _row(state)["lease_owner"] == lease.lease_id


@pytest.mark.parametrize("surface", ["tui", "desktop", "gateway", "cron", "worker"])
def test_non_classic_cli_surfaces_never_acquire_consumer_authority(tmp_path, surface):
    with contextlib.closing(SessionDB(tmp_path / "state.db")) as state:
        state.create_session("terminal", source="cli")
        lease, refusal = try_acquire_active_session(
            session_id="terminal",
            surface=surface,
            config=ENABLED,
            registry_home=tmp_path,
            metadata={"live_session_id": "terminal"},
        )
        assert lease is not None and refusal is None
        assert not lease.activate_completion_consumer(state, ENABLED)
        assert _row(state) is None
        lease.release()


def test_disabled_cli_does_not_acquire_consumer_authority(tmp_path):
    with contextlib.closing(SessionDB(tmp_path / "state.db")) as state:
        state.create_session("terminal", source="cli")
        lease, refusal = try_acquire_active_session(
            session_id="terminal",
            surface="cli",
            config={},
            registry_home=tmp_path,
            metadata={"live_session_id": "terminal"},
        )
        assert lease is not None and refusal is None
        assert not lease.activate_completion_consumer(state, {})
        assert _row(state) is None
        lease.release()


def test_missing_or_wrong_session_context_fails_without_partial_authority(tmp_path):
    with contextlib.closing(SessionDB(tmp_path / "state.db")) as state:
        state.create_session("cron-session", source="cron")
        missing, refusal = try_acquire_active_session(
            session_id="missing",
            surface="cli",
            config=ENABLED,
            registry_home=tmp_path,
            metadata={"live_session_id": "missing"},
        )
        assert missing is not None and refusal is None
        with pytest.raises(ValueError, match="persisted current session"):
            missing.activate_completion_consumer(state, ENABLED)
        assert _row(state, "missing") is None
        missing.release()

        cron, refusal = try_acquire_active_session(
            session_id="cron-session",
            surface="cli",
            config=ENABLED,
            registry_home=tmp_path,
            metadata={"live_session_id": "cron-session"},
        )
        assert cron is not None and refusal is None
        with pytest.raises(ValueError, match="interactive CLI"):
            cron.activate_completion_consumer(state, ENABLED)
        assert _row(state, "cron-session") is None
        cron.release()


def test_consumer_row_creation_is_atomic(tmp_path):
    with contextlib.closing(SessionDB(tmp_path / "state.db")) as state:
        state.create_session("terminal", source="cli")
        state._execute_write(lambda conn: conn.execute(
            "CREATE TRIGGER reject_consumer BEFORE INSERT ON kanban_session_consumers "
            "BEGIN SELECT RAISE(ABORT,'consumer crash'); END"
        ))
        lease, refusal = try_acquire_active_session(
            session_id="terminal",
            surface="cli",
            config=ENABLED,
            registry_home=tmp_path,
            metadata={"live_session_id": "terminal"},
        )
        assert lease is not None and refusal is None
        with pytest.raises(Exception, match="consumer crash"):
            lease.activate_completion_consumer(state, ENABLED)
        assert _row(state) is None
        lease.release()


def test_consumer_cannot_be_attached_to_another_profile_store(tmp_path):
    profile = tmp_path / "profile"
    profile.mkdir()
    with contextlib.closing(SessionDB(profile / "state.db")) as state, \
            contextlib.closing(SessionDB(tmp_path / "other.db")) as other:
        state.create_session("terminal", source="cli")
        other.create_session("terminal", source="cli")
        lease, refusal = try_acquire_active_session(
            session_id="terminal",
            surface="cli",
            config=ENABLED,
            registry_home=profile,
            metadata={"live_session_id": "terminal"},
        )
        assert lease is not None and refusal is None
        with pytest.raises(PermissionError, match="profile"):
            lease.activate_completion_consumer(other, ENABLED)
        assert _row(other) is None
        lease.release()
