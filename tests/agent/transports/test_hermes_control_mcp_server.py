"""Behavior contracts for the scoped Hermes MCP server."""

import json
import subprocess

import pytest

from agent.transports.hermes_control_mcp_server import HermesControlBridge, build_server


@pytest.fixture
def bridge(tmp_path, monkeypatch):
    home = tmp_path / "profiles" / "research"
    home.mkdir(parents=True)
    (home / "config.yaml").write_text("{}\n")
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_KANBAN_HOME", str(tmp_path / "shared"))
    from hermes_cli import kanban_db as kb

    kb.create_board("fixture_research")
    kb.create_board("other_entity")
    return HermesControlBridge(home, ["fixture_research"], ["research"])


def test_fixture_board_create_and_result_path(bridge):
    assert [b["slug"] for b in bridge.kanban_boards()["boards"]] == ["fixture_research"]
    created = bridge.kanban_create("fixture_research", "Fixture request", "Read local fixture", "research", "req-1")
    repeated = bridge.kanban_create("fixture_research", "Fixture request", "Read local fixture", "research", "req-1")
    assert created["task_id"] == repeated["task_id"]
    assert created["result_tool"] == "kanban_task_status"
    assert bridge.kanban_tasks("fixture_research")["tasks"][0]["id"] == created["task_id"]
    assert bridge.kanban_task_status("fixture_research", created["task_id"])["status"] == "ready"
    assert bridge.kanban_comment("fixture_research", created["task_id"], "Fixture note")["comment_id"] > 0
    with pytest.raises(ValueError, match="allow-board"):
        bridge.kanban_tasks("other_entity")
    with pytest.raises(ValueError, match="allow-assignee"):
        bridge.kanban_create("fixture_research", "Wrong profile", "x", "atlas", "req-2")
    with pytest.raises(ValueError, match="not found"):
        bridge.kanban_task_status("fixture_research", "not-a-task")


def test_research_turn_is_bounded_and_returns_structured_result(bridge, monkeypatch):
    seen = {}

    def fake_run(command, **kwargs):
        seen.update(command=command, kwargs=kwargs)
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps({
            "type": "result", "session_id": "fixture-session", "exit_code": 0,
            "text": "Fixture answer", "tokens": {"total": 12}, "duration_ms": 10,
        }), stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr("tools.environments.local.served_profile_child_env",
                        lambda **kwargs: {"HERMES_HOME": str(bridge.profile_home)})
    result = bridge.research_run("Summarize fixture")
    assert result["status"] == "completed"
    assert result["answer"] == "Fixture answer"
    assert "--max-turns" in seen["command"] and "--run-budget" in seen["command"]
    assert seen["command"][-1] == "session_search"
    assert seen["kwargs"]["env"]["HERMES_HOME"] == str(bridge.profile_home)
    assert seen["kwargs"]["cwd"] == bridge.profile_home
    bridge.research_run("Public research", web=True)
    assert seen["command"][-1] == "web,session_search"
    with pytest.raises(ValueError, match="1 to 4000"):
        bridge.research_run(" ")


def test_server_registers_only_bridge_tools(bridge):
    server = build_server(bridge)
    names = {tool.name for tool in server._tool_manager.list_tools()}
    assert names == {"abilities_list", "ability_view", "research_run", "kanban_boards",
                     "kanban_tasks", "kanban_create", "kanban_task_status", "kanban_comment"}
