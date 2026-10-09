"""TUI/Desktop recovery preserves exact completion identities across crash boundaries."""

import pytest

from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_notify as kbn
from hermes_cli import completion_inbox_store
from hermes_cli.completion_inbox import CompletionInbox
from tests.tui_gateway.test_completion_inbox_consumer import response_with_effect, ui_world as ui_world
from tui_gateway.completion_inbox import poll
from tui_gateway.server import _collect_kanban_notifications


class InjectedCrash(BaseException):
    """Bypass adapter retries while still unwinding in-process ownership guards."""


def _inject_crash(monkeypatch, boundary):
    def crash():
        raise InjectedCrash(boundary)

    if boundary == "after_marker":
        admit = CompletionInbox.admit
        monkeypatch.setattr(CompletionInbox, "admit", lambda inbox, delivery: admit(inbox, delivery, after_marker=crash))
        return

    target, name = {
        "after_claim": (CompletionInbox, "claim"),
        "after_answer": (CompletionInbox, "publish"),
        # This function runs inside the profile transaction. Aborting after its
        # writes but before commit must roll back both the answer and its receipt.
        "before_receipt": (completion_inbox_store, "publish_response"),
    }[boundary]
    original = getattr(target, name)

    def finish_then_crash(*args, **kwargs):
        original(*args, **kwargs)
        crash()

    monkeypatch.setattr(target, name, finish_then_crash)


@pytest.mark.parametrize("crash_boundary", ["after_claim", "after_marker", "after_answer", "before_receipt"])
def test_completion_recovers_exact_receipt_after_crash(ui_world, monkeypatch, crash_boundary):
    world = ui_world
    response_with_effect(world)
    acknowledgements = []
    acknowledge = CompletionInbox.acknowledge

    def acknowledge_processed(inbox, delivery):
        row = world.board.execute("SELECT * FROM kanban_deliveries WHERE id=?", (delivery,)).fetchone()
        receipt = world.state._read_one(
            "SELECT * FROM kanban_inbox_transcripts WHERE delivery_id=?", (delivery,)
        )
        assert row["state"] == "processed"
        assert row["transcript_message_id"] == receipt["response_message_id"]
        assert receipt["response_message_id"] is not None
        acknowledgements.append(delivery)
        return acknowledge(inbox, delivery)

    monkeypatch.setattr(CompletionInbox, "acknowledge", acknowledge_processed)
    with monkeypatch.context() as crash_patch:
        _inject_crash(crash_patch, crash_boundary)
        with pytest.raises(InjectedCrash, match=crash_boundary):
            poll("ui", world.session)

    inferred = crash_boundary in {"after_answer", "before_receipt"}
    assert world.agent.client.chat.completions.create.call_count == (2 if inferred else 0)
    assert len(kb.list_comments(world.board, world.task)) == (1 if inferred else 0)
    assert not acknowledgements
    assert not world.emitted
    assert not world.session["running"]
    assert world.lease.consumer_state is None
    receipt_before = world.state._read_one(
        "SELECT * FROM kanban_inbox_transcripts WHERE delivery_id=?", (world.delivery,)
    )
    if crash_boundary == "after_claim":
        assert receipt_before is None
    else:
        assert receipt_before["marker_message_id"] is not None
        assert (receipt_before["response_message_id"] is not None) == (crash_boundary == "after_answer")
    if crash_boundary == "before_receipt":
        assert [row["role"] for row in world.state.get_messages("origin")] == ["user"]

    if crash_boundary == "after_answer":
        world.agent.client.chat.completions.create.side_effect = lambda *_a, **_kw: pytest.fail(
            "a committed response must reconcile without inference"
        )
    else:
        response_with_effect(world)
    assert poll("ui", world.session) == 1
    assert poll("ui", world.session) == 0

    expected_calls = 4 if crash_boundary == "before_receipt" else 2
    assert world.agent.client.chat.completions.create.call_count == expected_calls
    assert len(kb.list_comments(world.board, world.task)) == 1
    assert acknowledgements == [world.delivery]
    receipts = world.state._read_all(
        "SELECT * FROM kanban_inbox_transcripts WHERE delivery_id=?", (world.delivery,)
    )
    assert len(receipts) == 1
    receipt = receipts[0]
    messages = world.state.get_messages("origin")
    linked = [row for row in messages if (row.get("display_metadata") or {}).get("completion_inbox")]
    assert [row["display_metadata"]["completion_inbox"]["kind"] for row in linked] == ["marker", "response"]
    assert [row["id"] for row in linked] == [receipt["marker_message_id"], receipt["response_message_id"]]
    assert all(row["display_metadata"]["completion_inbox"]["turn_id"] == receipt["turn_id"] for row in linked)
    if receipt_before is not None:
        assert receipt["marker_message_id"] == receipt_before["marker_message_id"]
    if crash_boundary == "after_answer":
        assert receipt["response_message_id"] == receipt_before["response_message_id"]
    delivery = world.board.execute("SELECT * FROM kanban_deliveries WHERE id=?", (world.delivery,)).fetchone()
    assert delivery["state"] == "acknowledged"
    assert delivery["transcript_message_id"] == receipt["response_message_id"]
    pushes = [payload for event, payload in world.emitted if event == "message.complete"]
    assert len(pushes) == 1
    assert pushes[0]["persisted_turn"]["final_assistant_row_id"] == receipt["response_message_id"]


def test_crash_before_claim_preserves_legacy_event():
    with kbc.connect_closing() as conn:
        tid = kb.create_task(conn, title="before claim")
        kbn.add_notify_sub(conn, task_id=tid, platform="tui", chat_id="before-claim")
        kb.complete_task(conn, tid, summary="still owed")
    assert _collect_kanban_notifications({"session_key": "before-claim"})
