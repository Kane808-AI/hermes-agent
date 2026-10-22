"""Provider request dumps guarded for silent completion turns."""
from __future__ import annotations


def dump_api_request_debug(agent, api_kwargs, *, reason: str, error=None):
    """Delegate ordinary debug dumps while completion execution suppresses them."""
    from agent.completion_inbox_silence import is_silent
    if is_silent():
        return None
    from agent.agent_runtime_helpers import dump_api_request_debug as _dump
    return _dump(agent, api_kwargs, reason=reason, error=error)
