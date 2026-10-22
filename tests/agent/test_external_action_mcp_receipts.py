"""PR receipts through the real MCP renderer; no remote calls."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from agent.external_action_guard import Store, guarded_execute
from tools.mcp_tool_handlers import _render_call_tool_result

ARGS = {"owner": "example", "repo": "repo", "head": "feature", "base": "main"}
URL = "https://github.com/example/repo/pull/17"
PR = {"number": 17, "html_url": URL}


def rendered(text=None, structured=None, error=False):
    blocks = [] if text is None else [SimpleNamespace(type="text", text=text)]
    return _render_call_tool_result(
        SimpleNamespace(content=blocks, structuredContent=structured, isError=error), "github",
    )


@pytest.mark.parametrize("response", [
    rendered(structured=PR),
    rendered(text=json.dumps(PR)),
    rendered(text=json.dumps(PR), structured=PR),
    rendered(text="Pull request created", structured=PR),
])
def test_rendered_success_settles_and_survives_restart(tmp_path, response):
    store = Store(tmp_path / "actions.sqlite3")
    execute = Mock(return_value=response)
    first = guarded_execute("mcp_github_create_pull_request", ARGS, execute, store=store, enabled=True)
    assert first == response
    assert store.status()[0]["state"] == "succeeded"
    fresh = Store(store.path)
    again = json.loads(guarded_execute(
        "mcp_github_create_pull_request", ARGS, execute, store=fresh, enabled=True,
    ))
    assert again["already_completed"] and again["url"] == URL
    assert execute.call_count == 1


@pytest.mark.parametrize("response", [
    rendered(text=json.dumps(PR), structured=PR, error=True),
    rendered(text=URL),  # prose URL alone is not a structured create result
    rendered(structured={"number": 17, "html_url": URL.replace("example/repo", "other/repo")}),
    rendered(structured={"number": 18, "html_url": URL}),
    rendered(structured={"number": True, "html_url": URL}),
    rendered(structured={**PR, "success": False}),
    rendered(structured={**PR, "isError": True}),
    rendered(structured={"result": PR}),  # no recursive search through arbitrary objects
    rendered(text=json.dumps({"number": 18, "html_url": URL.replace("17", "18")}), structured=PR),
    json.dumps({"error": "timeout", "result": PR}),
    json.dumps({"isError": True, "result": PR}),
    json.dumps({"structuredContent": "truncated", "result": json.dumps(PR)}),
])
def test_failed_malformed_or_conflicting_results_keep_hold(tmp_path, response):
    store = Store(tmp_path / "actions.sqlite3")
    execute = Mock(return_value=response)
    guarded_execute("mcp_github_create_pull_request", ARGS, execute, store=store, enabled=True)
    again = json.loads(guarded_execute(
        "mcp_github_create_pull_request", ARGS, execute, store=store, enabled=True,
    ))
    assert store.status()[0]["state"] == "uncertain"
    assert again["executed"] is False
    assert execute.call_count == 1
