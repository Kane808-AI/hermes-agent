"""Profile-local completion transcript identities. No consumer lifecycle operations."""
from __future__ import annotations

import json
import time

from hermes_cli.sqlite_migration_backup import migration_backup

TRANSCRIPT_SQL = """CREATE TABLE IF NOT EXISTS kanban_inbox_transcripts (
    board_uuid TEXT NOT NULL,
    delivery_id TEXT NOT NULL,
    return_route_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    session_started_at REAL NOT NULL,
    turn_id TEXT NOT NULL,
    marker_message_id INTEGER NOT NULL,
    marker_message_uid TEXT NOT NULL,
    response_message_id INTEGER,
    response_message_uid TEXT,
    publication_fence INTEGER,
    tool_plan TEXT NOT NULL DEFAULT '[]',
    retired_reason TEXT,
    PRIMARY KEY(board_uuid,delivery_id), UNIQUE(board_uuid,turn_id)
)"""
EFFECT_SQL = """CREATE TABLE IF NOT EXISTS kanban_inbox_effects (
    delivery_id TEXT NOT NULL REFERENCES kanban_deliveries(id) ON DELETE CASCADE,
    turn_id TEXT NOT NULL,
    operation_slot INTEGER NOT NULL CHECK(operation_slot>=0),
    fingerprint TEXT NOT NULL,
    result TEXT NOT NULL,
    PRIMARY KEY(delivery_id,turn_id,operation_slot)
)"""


def initialize_store(conn, path, *, profile: bool) -> None:
    """Explicit native initialization only. Back up before additive Phase 2 DDL."""
    table, ddl = ("kanban_inbox_transcripts", TRANSCRIPT_SQL) if profile else ("kanban_inbox_effects", EFFECT_SQL)
    names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE name IN (?,?)",
                                           (table, "kanban_inbox_schema"))}
    schema, existing = "kanban_inbox_schema" in names, table in names
    if bool(schema) != bool(existing):
        raise RuntimeError("incomplete completion inbox schema")
    if existing:
        version = conn.execute("SELECT version FROM kanban_inbox_schema WHERE singleton=1").fetchone()
        if version is None or version[0] != 1:
            raise RuntimeError("incompatible completion inbox schema")
        # An incomplete ledger must fail, never silently replace committed identities.
        columns = [line.strip().split()[0] for line in ddl.splitlines()[1:] if line.startswith("    ")]
        required = {column for column in columns if column not in {"PRIMARY", ")"}}
        actual = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
        if not required <= actual:
            raise RuntimeError("incomplete completion inbox ledger")
        return
    migration_backup(conn, path, "completion-inbox-v1")
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(ddl)
        conn.execute("CREATE TABLE IF NOT EXISTS kanban_inbox_schema (singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
                     "version INTEGER NOT NULL)")
        conn.execute("INSERT OR IGNORE INTO kanban_inbox_schema VALUES (1,1)")
        version = conn.execute("SELECT version FROM kanban_inbox_schema WHERE singleton=1").fetchone()
        if version[0] != 1:
            raise RuntimeError("incompatible completion inbox schema")
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def ledger(conn, board_id: str, delivery_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM kanban_inbox_transcripts WHERE board_uuid=? AND delivery_id=?",
                       (board_id, delivery_id)).fetchone()
    return dict(row) if row is not None else None


def check_version(conn):
    version = conn.execute("SELECT version FROM kanban_inbox_schema WHERE singleton=1").fetchone()
    if version is None or version[0] != 1:
        raise RuntimeError("incompatible completion inbox schema")


def _verify_retired_identity(conn, delivery, record, owner_state_path):
    witness = conn.execute("SELECT board_uuid,owner_state_path FROM kanban_return_route_owners WHERE return_route_id=?",
                           (record["return_route_id"],)).fetchone()
    if witness is None or tuple(witness) != (record["board_uuid"], owner_state_path):
        raise PermissionError("owning-profile route witness is absent")
    if delivery["return_route_id"] != record["return_route_id"]:
        raise RuntimeError("retired completion identity mismatch")
    if delivery["turn_id"] is None and delivery["marker_message_id"] is None:
        # Marker publication precedes board admission. Prove its exact durable witness.
        verify_messages(conn, dict(record))
    elif delivery["turn_id"] != record["turn_id"] or delivery["marker_message_id"] != record["marker_message_id"]:
        raise RuntimeError("retired completion identity mismatch")


def reconcile_retirements(conn, board, board_id, session_id=None, *, owner_state_path, session_started_at=None):
    """Repair board-retirement→profile gaps under profile-before-board writer locks."""
    records = conn.execute("SELECT * FROM kanban_inbox_transcripts WHERE board_uuid=? AND (? IS NULL OR session_id=?) "
                           "AND (? IS NULL OR session_started_at=?) AND response_message_id IS NULL AND retired_reason IS NULL",
                           (board_id, session_id, session_id, session_started_at, session_started_at)).fetchall()
    for record in records:
        delivery = board.execute("SELECT * FROM kanban_deliveries WHERE id=? AND board_uuid=? AND state='dead_letter'",
                                 (record["delivery_id"], board_id)).fetchone()
        if delivery is None:
            continue
        if not delivery["last_error_code"]:
            raise RuntimeError("retired completion has no durable reason")
        _verify_retired_identity(conn, delivery, record, owner_state_path)
        conn.execute("UPDATE kanban_inbox_transcripts SET retired_reason=? WHERE board_uuid=? AND delivery_id=?",
                     (delivery["last_error_code"], board_id, record["delivery_id"]))


def verify_messages(conn, record: dict, *, receipt: bool = False) -> None:
    """Exact row/UID/session/metadata witnesses, never adjacency or content search."""
    kinds = ["marker"] + (["response"] if receipt else [])
    for kind in kinds:
        row = conn.execute("SELECT session_id,role,message_uid,display_metadata,active FROM messages WHERE id=?",
                           (record[f"{kind}_message_id"],)).fetchone()
        expected = {"board_uuid": record["board_uuid"], "delivery_id": record["delivery_id"],
                    "turn_id": record["turn_id"], "kind": kind}
        if row is None or row[0] != record["session_id"] or row[1] != ("user" if kind == "marker" else "assistant") \
                or row[2] != record[f"{kind}_message_uid"] \
                or json.loads(row[3] or "{}").get("completion_inbox") != expected:
            raise RuntimeError("completion transcript identity mismatch")


def linkage(record: dict, kind: str) -> dict:
    return {"completion_inbox": {"board_uuid": record["board_uuid"], "delivery_id": record["delivery_id"],
                                 "turn_id": record["turn_id"], "kind": kind}}


def insert_marker(state, conn, record: dict, content: str) -> dict:
    state._check_transcript_write_guards(conn, record["session_id"], None,
                                        reject_active_turn_lease=True, reject_active_compression_lock=True)
    message = {"role": "user", "content": content, "display_kind": "completion_inbox",
               "display_metadata": linkage(record, "marker"), "timestamp": time.time()}
    inserted, tools = state._insert_message_rows(conn, record["session_id"], [message])
    state._bump_session_counters(conn, record["session_id"], inserted, tools, unit=False)
    record.update(marker_message_id=message["_row_id"], marker_message_uid=message["message_uid"])
    columns = tuple(record)
    conn.execute(f"INSERT INTO kanban_inbox_transcripts ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                 tuple(record.values()))
    return ledger(conn, record["board_uuid"], record["delivery_id"])


def publish_response(state, conn, record: dict, rows: list[dict], terminal_index: int, *,
                     fence: int, turn_lease_holder: str | None, compression_lock_holder: str | None) -> dict:
    if record["response_message_id"] is not None:
        verify_messages(conn, record, receipt=True)
        return record
    terminal = rows[terminal_index]
    if terminal.get("role") != "assistant" or terminal.get("tool_calls") or not terminal.get("content"):
        raise RuntimeError("completion receipt requires a terminal assistant response")
    for message in rows:
        if message.get("_row_id") is not None or (message.get("message_uid") and conn.execute(
            "SELECT 1 FROM messages WHERE message_uid=?", (message["message_uid"],)).fetchone()):
            raise RuntimeError("completion receipt cannot adopt an existing assistant or history row")
    state._check_transcript_write_guards(conn, record["session_id"], compression_lock_holder,
                                        turn_lease_holder=turn_lease_holder, reject_active_turn_lease=True)
    terminal["display_metadata"] = {**(terminal.get("display_metadata") or {}), **linkage(record, "response")}
    # These are fresh, staged turn rows. Reuse native repair and row identity handling.
    from agent.transcript_repair import resolve_and_repair_transcript_batch
    fresh = resolve_and_repair_transcript_batch(
        conn, record["session_id"], rows, encode_content_fn=state._encode_content,
        decode_content_fn=state._decode_content,
        serialize_message_fn=lambda msg, timestamp: state._serialized_message_row(record["session_id"], msg, timestamp),
        decode_row_fn=state._decoded_repair_row)
    inserted, tools = state._insert_message_rows(conn, record["session_id"], fresh)
    state._bump_session_counters(conn, record["session_id"], inserted, tools, unit=False)
    conn.execute("UPDATE kanban_inbox_transcripts SET response_message_id=?,response_message_uid=?,publication_fence=? "
                 "WHERE board_uuid=? AND delivery_id=? AND response_message_id IS NULL",
                 (terminal["_row_id"], terminal["message_uid"], fence, record["board_uuid"], record["delivery_id"]))
    return ledger(conn, record["board_uuid"], record["delivery_id"])
