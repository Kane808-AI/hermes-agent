"""Guard opt-in through normal config writing and the real read-only loader."""

import json
from unittest.mock import Mock

import pytest

from agent.external_action_guard import Store, guarded_execute
from hermes_cli.config import _validate_config_key, load_config_readonly, set_config_value
from hermes_constants import reset_hermes_home_override, set_hermes_home_override


@pytest.mark.parametrize("profile", ["builder", "builder-2"])
def test_default_off_then_normal_config_set_enables_guard(tmp_path, monkeypatch, capsys, profile):
    home = tmp_path / "profiles" / profile
    home.mkdir(parents=True)
    monkeypatch.delenv("HERMES_IGNORE_USER_CONFIG", raising=False)
    token = set_hermes_home_override(home)
    store = Store(tmp_path / "actions.sqlite3")
    args = {"command": "gh pr create --repo example/repo --head feature --base main"}
    try:
        assert _validate_config_key("external_action_guard.enabled") == (True, None)
        assert load_config_readonly()["external_action_guard"]["enabled"] is False
        disabled = Mock(return_value="uncovered")
        assert guarded_execute("terminal", args, disabled, store=store) == "uncovered"
        assert not store.path.exists()
        set_config_value("external_action_guard.enabled", "true")
        captured = capsys.readouterr()
        assert "not a recognized" not in captured.out + captured.err
        assert load_config_readonly()["external_action_guard"]["enabled"] is True
        execute = Mock(return_value=json.dumps({"error": "unknown outcome"}))
        guarded_execute("terminal", args, execute, store=store)
        again = json.loads(guarded_execute("terminal", args, execute, store=store))
        assert again["executed"] is False and execute.call_count == 1
    finally:
        reset_hermes_home_override(token)
