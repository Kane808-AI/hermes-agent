"""Producer opt-in crosses the real readonly config loader in an isolated home."""
from hermes_cli import kanban_db_connect as kbc
from hermes_cli.kanban_delivery import DeliveryPolicy, configure_producer, trusted_board_controller
from hermes_state import SessionDB
import contextlib

import pytest


def test_config_loader_controls_explicit_board_opt_in(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    (tmp_path / "config.yaml").write_text("kanban:\n  completion_inbox:\n    producer_enabled: true\n"
                                        "    acknowledged_retention_days: 12\n")
    with kbc.connect_closing(tmp_path / "board.db") as conn, \
            contextlib.closing(SessionDB(tmp_path / "state.db")) as state, trusted_board_controller(state):
        # Merely opening the board never opts it in.
        assert conn.execute("SELECT producer_enabled FROM kanban_delivery_control").fetchone()[0] == 0
        policy = configure_producer(conn)
        assert policy.producer_enabled and policy.acknowledged_retention_days == 12
        assert conn.execute("SELECT producer_enabled FROM kanban_delivery_control").fetchone()[0] == 1


@pytest.mark.parametrize("value", ["false", 1, None])
def test_malformed_producer_flag_fails_closed(value):
    with pytest.raises(ValueError, match="boolean"):
        DeliveryPolicy.from_config({"kanban": {"completion_inbox": {"producer_enabled": value}}})
