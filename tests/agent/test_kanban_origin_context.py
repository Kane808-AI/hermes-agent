"""The agent tool executor passes live runtime identity outside model arguments."""
from types import SimpleNamespace


def test_registry_dispatch_receives_the_live_agent_as_trusted_context(monkeypatch):
    from agent.tool_executor import _ToolCallRef, _resolve_sequential_dispatch
    import model_tools

    agent = SimpleNamespace(
        _context_engine_tool_names=set(),
        _memory_manager=None,
        _current_turn_id="turn",
        _current_api_request_id="request",
        enabled_toolsets=["kanban"],
        disabled_toolsets=[],
        quiet_mode=False,
        session_id="session",
        valid_tool_names={"kanban_create"},
    )
    captured = {}

    def dispatch(*_args, **kwargs):
        captured.update(kwargs)
        return "{}"

    monkeypatch.setattr(model_tools, "handle_function_call", dispatch)
    ref = _ToolCallRef("kanban_create", {"title": "x"}, "task", "call", [])

    resolved = _resolve_sequential_dispatch(agent, ref, [])
    resolved.execute(ref.args)

    assert captured["parent_agent"] is agent
