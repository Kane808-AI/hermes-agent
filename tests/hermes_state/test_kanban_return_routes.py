"""Opaque routes resolve only in the owning profile's explicit state database."""
import contextlib
import sqlite3

import pytest

from hermes_state import SessionDB


def test_equal_session_strings_do_not_collide_across_profiles(tmp_path):
    with contextlib.closing(SessionDB(tmp_path / "a.db")) as a, \
            contextlib.closing(SessionDB(tmp_path / "b.db")) as b:
        for db in (a, b):
            db.create_session("same-session", source="cli")
        ra = a.create_kanban_return_route("board", "same-session", surface="cli", interactive=True)
        rb = b.create_kanban_return_route("board", "same-session", surface="cli", interactive=True)
        assert ra != rb
        assert a.resolve_kanban_return_route(ra, "board")
        assert b.resolve_kanban_return_route(rb, "board")
        assert a.resolve_kanban_return_route(rb, "board") is None
        assert b.resolve_kanban_return_route(ra, "board") is None
        assert a.resolve_kanban_return_route(ra, "other-board") is None


@pytest.mark.parametrize("source", ["cron", "kanban", "acp", "subagent", "tool"])
def test_noninteractive_sources_cannot_mint_cli_route(tmp_path, source):
    with contextlib.closing(SessionDB(tmp_path / "state.db")) as db:
        db.create_session("s", source=source)
        with pytest.raises(ValueError, match="interactive surface"):
            db.create_kanban_return_route("board", "s", surface="cli", interactive=True)


def test_one_shot_and_missing_session_cannot_mint_route(tmp_path):
    with contextlib.closing(SessionDB(tmp_path / "state.db")) as db:
        db.create_session("s", source="cli")
        with pytest.raises(ValueError, match="interactive origin"):
            db.create_kanban_return_route("board", "s", surface="cli", interactive=False)
        with pytest.raises(ValueError, match="interactive surface"):
            db.create_kanban_return_route("board", "missing", surface="cli", interactive=True)


def test_route_is_immutable_and_deleted_target_does_not_redirect(tmp_path):
    path = tmp_path / "state.db"
    with contextlib.closing(SessionDB(path)) as db:
        db.create_session("s", source="cli")
        route = db.create_kanban_return_route("board", "s", surface="cli", interactive=True)
        with contextlib.closing(sqlite3.connect(path)) as conn:
            with pytest.raises(sqlite3.IntegrityError, match="immutable"):
                conn.execute("UPDATE kanban_return_routes SET session_id='other'")
        db.delete_session("s")
        assert db.resolve_kanban_return_route(route, "board") is None
        db.create_session("s", source="cli")
        assert db.resolve_kanban_return_route(route, "board") is None


def test_copied_profile_database_cannot_resolve_original_route(tmp_path):
    source, copied = tmp_path / "source.db", tmp_path / "copied.db"
    with contextlib.closing(SessionDB(source)) as db:
        db.create_session("s", source="cli")
        route = db.create_kanban_return_route("board", "s", surface="cli", interactive=True)
        with contextlib.closing(sqlite3.connect(source)) as src, contextlib.closing(sqlite3.connect(copied)) as dst:
            src.backup(dst)
    with contextlib.closing(SessionDB(copied)) as clone:
        assert clone.resolve_kanban_return_route(route, "board") is None


@pytest.mark.parametrize("surface,source", [("cli", "cli"), ("tui", "tui"), ("desktop", "tui")])
def test_trusted_storage_accepts_interactive_surface(tmp_path, surface, source):
    with contextlib.closing(SessionDB(tmp_path / "state.db")) as db:
        db.create_session("s", source=source)
        route = db.create_kanban_return_route("board", "s", surface=surface, interactive=True)
        assert db.resolve_kanban_return_route(route, "board")["surface"] == surface
