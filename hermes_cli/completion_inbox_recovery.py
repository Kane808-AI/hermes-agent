"""Explicit classification of unreachable routes by the durable board controller."""
import time

from hermes_cli import completion_inbox_store as store
from hermes_cli import kanban_db as kb
from hermes_cli.kanban_delivery import _CONTROLLER, board_uuid
from hermes_state_kanban import assert_trusted_route_writer


def _route_failure(state, conn, delivery):
    witness = conn.execute("SELECT board_uuid,owner_state_path FROM kanban_return_route_owners WHERE return_route_id=?",
                           (delivery["return_route_id"],)).fetchone()
    if witness is None or witness[0] != delivery["board_uuid"] or witness[1] != str(state.db_path.resolve()):
        raise PermissionError("owning-profile route witness is absent")
    route = conn.execute("SELECT * FROM kanban_return_routes WHERE return_route_id=?",
                         (delivery["return_route_id"],)).fetchone()
    if route is None:
        return "route_missing"
    if route["board_uuid"] != delivery["board_uuid"] or route["owner_state_path"] != str(state.db_path.resolve()):
        raise PermissionError("route belongs to another board/profile")
    session = conn.execute("SELECT started_at FROM sessions WHERE id=?", (route["session_id"],)).fetchone()
    if session is None:
        return "session_missing"
    if session[0] != route["session_started_at"]:
        return "session_incarnation_changed"
    raise RuntimeError("route is recoverable")


def _recover_receipt(board, conn, delivery):
    record = store.ledger(conn, delivery["board_uuid"], delivery["id"])
    if record is None or record["response_message_id"] is None:
        return False
    if record["return_route_id"] != delivery["return_route_id"] \
            or record["turn_id"] != delivery["turn_id"] or record["marker_message_id"] != delivery["marker_message_id"]:
        raise RuntimeError("completion receipt identity mismatch")
    store.verify_messages(conn, record, receipt=True)
    board.execute("UPDATE kanban_deliveries SET state='processed',transcript_message_id=?,processed_at=? WHERE id=? AND state=?",
                  (record["response_message_id"], int(time.time()), delivery["id"], delivery["state"]))
    return True


def retire_unroutable(board, state, delivery_id):
    assert_trusted_route_writer()
    controller = _CONTROLLER.get()
    if controller is None or controller[0]._kanban_controller_id() != controller[1]:
        raise PermissionError("unroutable recovery requires trusted board-controller scope")
    identity = board_uuid(board)
    owner = board.execute("SELECT controller_id FROM kanban_delivery_control WHERE singleton=1").fetchone()
    if owner is None or owner[0] != controller[1]:
        raise PermissionError("different board controller")
    delivery = board.execute("SELECT return_route_id FROM kanban_deliveries WHERE id=? AND board_uuid=?",
                             (delivery_id, identity)).fetchone()
    witness = state._read_one("SELECT board_uuid,owner_state_path FROM kanban_return_route_owners WHERE return_route_id=?",
                              (delivery[0] if delivery is not None else None,))
    if witness is None or witness[0] != identity or witness[1] != str(state.db_path.resolve()):
        raise PermissionError("owning-profile route witness is absent")
    with state._lock:
        store.initialize_store(state._conn, state.db_path, profile=True)
    def classify(conn):
        with kb.write_txn(board):
            owner = board.execute("SELECT controller_id FROM kanban_delivery_control WHERE singleton=1").fetchone()
            if owner is None or owner[0] != controller[1] or board_uuid(board) != identity:
                raise PermissionError("different board controller")
            delivery = board.execute("SELECT * FROM kanban_deliveries WHERE id=? AND board_uuid=?", (delivery_id, identity)).fetchone()
            if delivery is None:
                raise PermissionError("completion delivery identity mismatch")
            delivery = dict(delivery)
            reason = _route_failure(state, conn, delivery)
            if delivery["state"] in {"processed", "acknowledged", "dead_letter"}:
                store.reconcile_retirements(conn, board, identity, owner_state_path=str(state.db_path.resolve()))
                return delivery
            if not _recover_receipt(board, conn, delivery):
                board.execute("UPDATE kanban_deliveries SET state='dead_letter',last_error_code=? WHERE id=? AND state=?",
                              (reason, delivery_id, delivery["state"]))
                conn.execute("UPDATE kanban_inbox_transcripts SET retired_reason=? WHERE board_uuid=? AND delivery_id=? "
                             "AND response_message_id IS NULL", (reason, identity, delivery_id))
            return dict(board.execute("SELECT * FROM kanban_deliveries WHERE id=?", (delivery_id,)).fetchone())
    return state._execute_write(classify)
