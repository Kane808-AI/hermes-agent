"""Phase 0 regression locks for the legacy claim-before-receipt loss window.

These strict expected failures remain until the fenced saga reaches TUI in Phase 6.
They exercise the actual collector, SQLite store, and persisted transcripts.
"""
import contextlib

import pytest

from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_notify as kbn
from hermes_state import SessionDB
from tui_gateway.server import _collect_kanban_notifications


class MissingCompletionReceipt(AssertionError):
    """Only the known lost completion may be an expected failure."""


@pytest.mark.xfail(strict=True, raises=MissingCompletionReceipt,
                   reason="Phases 2/6: collector claims before a linked durable receipt")
@pytest.mark.parametrize("crash_boundary", ["after_claim", "after_marker", "after_answer", "before_receipt"])
def test_uncommitted_completion_remains_collectable_after_crash(tmp_path, crash_boundary):
    session_id = "crash-origin"
    with kbc.connect_closing() as conn:
        tid = kb.create_task(conn, title="crash boundary")
        kbn.add_notify_sub(conn, task_id=tid, platform="tui", chat_id=session_id)
        kb.complete_task(conn, tid, summary="worker result")
    assert _collect_kanban_notifications({"session_key": session_id})
    with contextlib.closing(SessionDB(tmp_path / "state.db")) as state:
        state.create_session(session_id, source="tui")
        if crash_boundary != "after_claim":
            state.append_message(session_id, role="system", content="unlinked completion marker")
        if crash_boundary in {"after_answer", "before_receipt"}:
            state.append_message(session_id, role="assistant", content="unlinked response")
    # A process restart loses in-memory admission. An unlinked assistant response
    # cannot prove processing of this delivery, so the completion is still owed.
    if not _collect_kanban_notifications({"session_key": session_id}):
        raise MissingCompletionReceipt("completion was consumed without a linked durable receipt")


def test_crash_before_claim_preserves_legacy_event():
    with kbc.connect_closing() as conn:
        tid = kb.create_task(conn, title="before claim")
        kbn.add_notify_sub(conn, task_id=tid, platform="tui", chat_id="before-claim")
        kb.complete_task(conn, tid, summary="still owed")
    assert _collect_kanban_notifications({"session_key": "before-claim"})
