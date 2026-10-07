"""Provider request debug dumps, refused in silent completion execution."""
from __future__ import annotations

import copy
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from utils import atomic_json_write, env_var_enabled

logger = logging.getLogger(__name__)


def dump_api_request_debug(
    agent, api_kwargs: Dict[str, Any], *, reason: str, error: Optional[Exception] = None
) -> Optional[Path]:
    """Dump the request body from api_kwargs (minus transport keys) for debugging provider 4xx failures."""
    from agent.completion_inbox_silence import is_silent
    if is_silent():
        return None
    from agent.agent_runtime_helpers import _api_error_debug_info, _ra
    try:
        body = {k: v for k, v in copy.deepcopy(api_kwargs).items() if v is not None and k != "timeout"}
        api_key = None
        # anthropic_messages keeps its SDK client on ``_anthropic_client`` (``client`` is None):
        # read the key from there so the dump does not say "Bearer None" (#24293).
        anthropic = agent.api_mode == "anthropic_messages"
        try:
            live = getattr(agent, "_anthropic_client", None) if anthropic else agent.client
            api_key = getattr(live, "api_key", None) or getattr(live, "auth_token", None)
        except Exception as e:
            _ra().logger.debug("Could not extract API key for debug dump: %s", e)
        endpoint = {"codex_responses": "/responses", "anthropic_messages": "/messages"}.get(
            agent.api_mode, "/chat/completions"
        )
        dump_payload: Dict[str, Any] = {
            "timestamp": datetime.now().isoformat(), "session_id": agent.session_id, "reason": reason,
            "request": {
                "method": "POST", "url": f"{agent.base_url.rstrip('/')}{endpoint}",
                "headers": {
                    "Authorization": f"Bearer {agent._mask_api_key_for_logs(api_key)}",
                    "Content-Type": "application/json",
                },
                "body": body,
            },
        }
        if error is not None:
            dump_payload["error"] = _api_error_debug_info(error)
        # Sanitize the session ID (may come from an untrusted X-Hermes-Session-Id header) so a
        # "../"-shaped ID cannot write outside logs_dir.
        from agent.session_persistence import _safe_session_filename_component
        safe_sid = _safe_session_filename_component(agent.session_id)
        dump_file = agent.logs_dir / f"request_dump_{safe_sid}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.json"
        # Redact secrets first: this fires unconditionally on API errors and captures the full
        # request body, so context-embedded secrets would otherwise land in cleartext on disk.
        from agent.redact import redact_sensitive_text
        _serialized = json.dumps(dump_payload, ensure_ascii=False, indent=2, default=str)
        _redacted_payload = json.loads(redact_sensitive_text(_serialized, force=True))
        atomic_json_write(dump_file, _redacted_payload, default=str)
        agent._vprint(f"{agent.log_prefix}🧾 Request debug dump written to: {dump_file}")
        if env_var_enabled("HERMES_DUMP_REQUEST_STDOUT"):
            print(json.dumps(_redacted_payload, ensure_ascii=False, indent=2, default=str))
        return dump_file
    except Exception as dump_error:
        if agent.verbose_logging:
            logger.warning("Failed to dump API request debug payload: %s", dump_error)
        return None
