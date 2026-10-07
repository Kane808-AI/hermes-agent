"""Phase 0 locks for the absent durable monotonic Terminal consumer fence."""
import contextlib

import pytest

from hermes_cli.active_sessions import try_acquire_active_session
from hermes_state import SessionDB


class MissingConsumerFence(AssertionError):
    """Only the known missing fence may be an expected failure."""


@pytest.mark.xfail(strict=True, raises=MissingConsumerFence,
                   reason="Phase 4: foreground ownership has no durable consumer fence")
@pytest.mark.parametrize("terminal_state", ["idle", "busy", "closing", "closed", "reopened"])
def test_terminal_owner_keeps_monotonic_fence_across_lifecycle(tmp_path, terminal_state):
    with contextlib.closing(SessionDB(tmp_path / "state.db")) as state:
        state.create_session("terminal", source="cli")
        lease, refusal = try_acquire_active_session(
            session_id="terminal", surface="cli", config={}, registry_home=tmp_path)
        assert lease is not None and refusal is None
        leases = [lease]
        try:
            if terminal_state in {"closing", "closed", "reopened"}:
                lease.release()
            if terminal_state == "reopened":
                lease, refusal = try_acquire_active_session(
                    session_id="terminal", surface="cli", config={}, registry_home=tmp_path)
                assert lease is not None and refusal is None
                leases.append(lease)
            row = state._read_one("SELECT * FROM kanban_session_consumers WHERE session_id='terminal'")
            if row is None or row["consumer_fence"] <= 0:
                raise MissingConsumerFence("foreground ownership has no durable consumer fence")
        finally:
            for lease in leases:
                lease.release()
