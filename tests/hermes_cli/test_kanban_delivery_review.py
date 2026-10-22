"""Phase 0/1 review regressions using disposable boards and explicit orchestration."""
import contextlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb, kanban_db_connect as kbc
from hermes_cli import kanban_delivery as delivery
from hermes_cli import kanban_delivery_schema as schema
from hermes_state import SessionDB

ENABLED = {"kanban": {"completion_inbox": {"producer_enabled": True}}}


@pytest.fixture
def stores(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("HERMES_KANBAN_TASK", raising=False)
    with kbc.connect_closing(tmp_path / "board.db") as conn, \
            contextlib.closing(SessionDB(tmp_path / "a.db")) as a, \
            contextlib.closing(SessionDB(tmp_path / "b.db")) as b:
        for state in (a, b):
            state.create_session("same-session", source="cli")
        yield conn, a, b


def configure(conn, state, config=ENABLED):
    with delivery.trusted_board_controller(state):
        delivery.configure_producer(conn, config)


def route(conn, state):
    return state.create_kanban_return_route(
        delivery.board_uuid(conn), "same-session", surface="cli", interactive=True)


def bind(conn, state, tid, rid):
    with delivery.trusted_return_route(state, conn, rid):
        delivery.bind_task_return_route(conn, tid)


def damage(conn, fault):
    if fault == "trigger":
        conn.execute("DROP TRIGGER kanban_capture_delivery")
    elif fault == "table":
        conn.execute("DROP TABLE kanban_deliveries")
    elif fault == "column":
        conn.execute("ALTER TABLE kanban_deliveries DROP COLUMN last_error_code")
    else:
        conn.execute("DELETE FROM kanban_delivery_control")


@pytest.mark.parametrize("fault", ["trigger", "table", "column", "control_row"])
def test_incomplete_capture_rolls_back_transition_and_rejects_cached_open(stores, tmp_path, fault):
    conn, state, _ = stores
    tid = kb.create_task(conn, title="active pre-capture task")
    rid = route(conn, state)
    with delivery.trusted_return_route(state, conn, rid):
        routed = kb.create_task(conn, title="routed task")
    configure(conn, state)
    damage(conn, fault)
    before = [tuple(row) for row in conn.execute("SELECT * FROM task_events")]
    with pytest.raises(RuntimeError, match="delivery schema"):
        kb.complete_task(conn, routed, summary="must roll back")
    assert kb.get_task(conn, routed).status == "ready"
    assert kb.get_task(conn, tid).return_route_id is None
    assert [tuple(row) for row in conn.execute("SELECT * FROM task_events")] == before
    assert not conn.in_transaction
    with pytest.raises(RuntimeError, match="delivery schema"):
        kbc.connect(tmp_path / "board.db")


@pytest.mark.parametrize("fault", ["trigger", "table", "column"])
def test_enable_refuses_incomplete_capture_without_claiming_board(stores, fault):
    conn, state, _ = stores
    damage(conn, fault)
    before = tuple(conn.execute("SELECT * FROM kanban_delivery_control").fetchone())
    with pytest.raises(RuntimeError, match="delivery schema"):
        configure(conn, state)
    assert tuple(conn.execute("SELECT * FROM kanban_delivery_control").fetchone()) == before


def test_legacy_migration_discovery_is_not_corruption(tmp_path):
    with contextlib.closing(sqlite3.connect(tmp_path / "legacy.db")) as conn:
        conn.execute("CREATE TABLE tasks(id TEXT PRIMARY KEY)")
        assert schema.check_delivery_schema(conn) is False


def test_two_profiles_have_one_durable_controller(stores, tmp_path):
    conn, a, b = stores
    configure(conn, a, {})  # the first authorized disable may also claim
    controller = conn.execute("SELECT controller_id FROM kanban_delivery_control").fetchone()[0]
    assert controller and str(tmp_path) not in controller
    configure(conn, a)
    before = tuple(conn.execute("SELECT * FROM kanban_delivery_control").fetchone())
    for config in ({}, ENABLED):
        with pytest.raises(PermissionError, match="controller"):
            configure(conn, b, config)
        assert tuple(conn.execute("SELECT * FROM kanban_delivery_control").fetchone()) == before
    with contextlib.closing(SessionDB(tmp_path / "a.db")) as reopened:
        configure(conn, reopened, {})
    assert conn.execute("SELECT producer_enabled,controller_id FROM kanban_delivery_control").fetchone()[:] == (0, controller)


def test_ambient_profile_and_forged_controller_claims_do_not_authorize(stores, monkeypatch):
    conn, _, _ = stores
    monkeypatch.setenv("HERMES_KANBAN_CONTROLLER", "forged")
    with pytest.raises(PermissionError, match="trusted.*controller"):
        delivery.configure_producer(conn, ENABLED)
    with pytest.raises(TypeError):
        delivery.configure_producer(conn, ENABLED, controller_id="forged")
    assert conn.execute("SELECT producer_enabled,controller_id FROM kanban_delivery_control").fetchone()[:] == (0, None)


@pytest.mark.parametrize("context", ["worker", "delegate", "descendant", "non_dispatcher"])
def test_worker_context_cannot_use_inherited_controller_or_route_scope(stores, monkeypatch, context):
    from agent.delegation_context import delegated_child_context, non_dispatcher_owned_context

    conn, state, _ = stores
    tid = kb.create_task(conn, title="unbound")
    rid = route(conn, state)
    with delivery.trusted_board_controller(state), delivery.trusted_return_route(state, conn, rid):
        if context in {"worker", "non_dispatcher"}:
            monkeypatch.setenv("HERMES_KANBAN_TASK", tid)
        if context == "descendant":
            monkeypatch.setenv("HERMES_DELEGATED_CHILD_CONTEXT", "1")
        scope = delegated_child_context() if context == "delegate" else contextlib.nullcontext()
        if context == "non_dispatcher":
            scope = non_dispatcher_owned_context()
        with scope:
            with pytest.raises(PermissionError, match="workers"):
                delivery.configure_producer(conn, ENABLED)
            with pytest.raises(PermissionError, match="workers"):
                delivery.bind_task_return_route(conn, tid)
    assert kb.get_task(conn, tid).return_route_id is None
    assert conn.execute("SELECT producer_enabled,controller_id FROM kanban_delivery_control").fetchone()[:] == (0, None)


def test_copied_profile_cannot_control_original_board(stores, tmp_path):
    conn, state, _ = stores
    configure(conn, state)
    with contextlib.closing(sqlite3.connect(tmp_path / "a.db")) as source, \
            contextlib.closing(sqlite3.connect(tmp_path / "clone.db")) as copy:
        source.backup(copy)
    with contextlib.closing(SessionDB(tmp_path / "clone.db")) as clone:
        with pytest.raises(PermissionError, match="controller"):
            configure(conn, clone, {})
    assert conn.execute("SELECT producer_enabled FROM kanban_delivery_control").fetchone()[0] == 1


def test_existing_task_binds_once_and_next_event_captures_atomically(stores):
    conn, state, _ = stores
    tid = kb.create_task(conn, title="active before Phase 3")
    rid = route(conn, state)
    configure(conn, state)
    bind(conn, state, tid, rid)
    assert kb.get_task(conn, tid).return_route_id == rid
    assert kb.get_task(conn, tid).origin_interactive == 1
    assert not conn.execute("SELECT 1 FROM kanban_deliveries").fetchone()
    with pytest.raises(ValueError, match="already.*bound"):
        bind(conn, state, tid, rid)
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        conn.execute("UPDATE tasks SET return_route_id=NULL,origin_interactive=0 WHERE id=?", (tid,))
    conn.execute("CREATE TRIGGER fail_delivery BEFORE INSERT ON kanban_deliveries "
                 "BEGIN SELECT RAISE(ABORT, 'outbox failure'); END")
    with pytest.raises(sqlite3.IntegrityError, match="outbox failure"):
        kb.complete_task(conn, tid, summary="not committed")
    assert kb.get_task(conn, tid).status == "ready"
    assert not conn.execute("SELECT 1 FROM task_events WHERE delivery_expected=1").fetchone()
    conn.execute("DROP TRIGGER fail_delivery")
    assert kb.complete_task(conn, tid, summary="committed")
    row = conn.execute("SELECT d.task_id,d.return_route_id,e.kind FROM kanban_deliveries d "
                       "JOIN task_events e ON e.id=d.event_id").fetchone()
    assert tuple(row) == (tid, rid, "completed")


def test_binding_requires_explicit_scope_and_rejects_route_arguments(stores):
    conn, state, _ = stores
    tid = kb.create_task(conn, title="unbound")
    rid = route(conn, state)
    with pytest.raises(PermissionError, match="trusted.*route"):
        delivery.bind_task_return_route(conn, tid)
    with pytest.raises(TypeError):
        delivery.bind_task_return_route(conn, tid, return_route_id=rid)
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        conn.execute("UPDATE tasks SET return_route_id=?,origin_interactive=1 WHERE id=?", (rid, tid))
    assert kb.get_task(conn, tid).return_route_id is None


def test_cross_board_and_cross_profile_binding_rejected(stores, tmp_path):
    conn, a, b = stores
    tid = kb.create_task(conn, title="unbound")
    rid = route(conn, a)
    with pytest.raises(ValueError, match="profile and board"):
        bind(conn, b, tid, rid)
    with kbc.connect_closing(tmp_path / "other.db") as other:
        other_tid = kb.create_task(other, title="other board")
        with pytest.raises(ValueError, match="profile and board"):
            bind(other, a, other_tid, rid)
    assert kb.get_task(conn, tid).return_route_id is None


@pytest.mark.parametrize("origin", ["creator", "parent"])
def test_binding_rejects_conflicting_origin(stores, origin):
    conn, state, _ = stores
    root = kb.create_task(conn, title="unbound creator")
    args = {"creator_task_id": root} if origin == "creator" else {"parents": [root]}
    child = kb.create_task(conn, title="pre-Phase-3 child", **args)
    first, other = route(conn, state), route(conn, state)
    bind(conn, state, root, first)
    with pytest.raises(ValueError, match="different return routes"):
        bind(conn, state, child, other)
    assert kb.get_task(conn, child).return_route_id is None
    bind(conn, state, child, first)


def test_binding_rejects_previously_actionable_delivery_obligation(stores):
    conn, state, _ = stores
    tid = kb.create_task(conn, title="unrouted legacy task")
    rid = route(conn, state)
    configure(conn, state)
    kb.block_task(conn, tid, reason="needs input")
    configure(conn, state, {})
    conn.execute("UPDATE tasks SET status='done'")
    conn.execute("UPDATE task_events SET created_at=1 WHERE delivery_expected=1")
    kb.gc_events(conn, older_than_seconds=0)
    with pytest.raises(ValueError, match="actionable delivery obligation"):
        bind(conn, state, tid, rid)
    assert kb.get_task(conn, tid).return_route_id is None
    assert conn.execute("SELECT COUNT(*) FROM task_events WHERE delivery_expected=1").fetchone()[0] == 1


def test_noop_capture_trigger_cannot_enable_or_commit_a_transition(stores):
    conn, state, _ = stores
    tid = kb.create_task(conn, title="capture must work")
    configure(conn, state)
    conn.execute("DROP TRIGGER kanban_capture_delivery")
    conn.execute("CREATE TRIGGER kanban_capture_delivery AFTER INSERT ON task_events BEGIN SELECT 1; END")
    with pytest.raises(RuntimeError, match="invalid capture trigger"):
        kb.complete_task(conn, tid, summary="must roll back")
    assert kb.get_task(conn, tid).status == "ready"
    assert not conn.execute("SELECT 1 FROM task_events WHERE kind='completed'").fetchone()
    with pytest.raises(RuntimeError, match="invalid capture trigger"):
        configure(conn, state)


def test_controller_can_disable_broken_capture_but_cannot_reenable(stores):
    conn, state, other = stores
    configure(conn, state)
    conn.execute("DROP TRIGGER kanban_capture_delivery")
    with pytest.raises(PermissionError, match="different controller"):
        configure(conn, other, {})
    configure(conn, state, {})
    assert conn.execute("SELECT producer_enabled FROM kanban_delivery_control").fetchone()[0] == 0
    with pytest.raises(RuntimeError, match="delivery schema"):
        configure(conn, state)


def test_binding_failure_rolls_back_and_closes_connection_capability(stores):
    conn, state, _ = stores
    tid = kb.create_task(conn, title="binding fails")
    rid = route(conn, state)
    conn.execute("CREATE TRIGGER reject_binding AFTER UPDATE OF return_route_id ON tasks "
                 "BEGIN SELECT RAISE(ABORT, 'injected binding failure'); END")
    with pytest.raises(sqlite3.IntegrityError, match="binding failure"):
        bind(conn, state, tid, rid)
    assert kb.get_task(conn, tid).return_route_id is None
    conn.execute("DROP TRIGGER reject_binding")
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        conn.execute("UPDATE tasks SET return_route_id=?,origin_interactive=1 WHERE id=?", (rid, tid))
    bind(conn, state, tid, rid)


def test_concurrent_controller_claims_have_exactly_one_owner(stores, tmp_path):
    conn, a, b = stores
    def claim(state):
        with kbc.connect_closing(tmp_path / "board.db") as other:
            try:
                configure(other, state)
                return "claimed"
            except PermissionError:
                return "rejected"
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(claim, (a, b))) == ["claimed", "rejected"]
    assert conn.execute("SELECT producer_enabled FROM kanban_delivery_control").fetchone()[0] == 1
