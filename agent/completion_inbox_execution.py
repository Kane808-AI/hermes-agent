"""Native completion-turn scope: exact admission, staged output, guarded effects."""
from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from contextvars import ContextVar

_EXECUTION = ContextVar("completion_inbox_execution", default=None)


def current_execution():
    return _EXECUTION.get()


def append_terminal_response(messages, final_message):
    from agent.message_metadata import append_message
    append_message(messages, final_message)
    execution = current_execution()
    if execution is not None:
        execution.terminal_message = final_message


class CompletionExecution:
    def __init__(self, inbox, delivery_id):
        self.inbox, self.delivery_id = inbox, delivery_id
        self.record = inbox._locked(lambda conn, expires: inbox._record(conn, inbox._delivery(delivery_id)))
        self.terminal_message = None
        self.agent = None
        self._slot = 0
        self._slot_lock = threading.Lock()

    def validate_agent(self, agent):
        if agent._session_db is not self.inbox.state or agent.session_id != self.record["session_id"] \
                or getattr(agent, "_persist_disabled", False):
            raise PermissionError("completion execution must use the owning profile/session")
        from agent.context_compressor import ContextCompressor
        if agent.api_mode != "chat_completions" or agent.provider == "moa" \
                or agent.compression_enabled or agent._memory_manager \
                or type(agent.context_compressor) is not ContextCompressor \
                or agent.context_compressor._micro_compact_enabled or not agent.skip_background_review:
            raise PermissionError("completion execution requires the native runtime without automatic effects")
        if not agent._cached_system_prompt:
            raise PermissionError("completion execution requires an existing system prompt")
        from hermes_cli.plugins import VALID_HOOKS, VALID_MIDDLEWARE, get_plugin_manager
        manager = get_plugin_manager()
        if any(manager.iter_hook_callbacks(name) for name in VALID_HOOKS) \
                or any(manager.has_middleware(kind) for kind in VALID_MIDDLEWARE):
            raise PermissionError("completion execution refuses unreviewed plugin hooks or middleware")

    def marker_index(self, agent, messages):
        self.validate_agent(agent)
        self.inbox._locked(lambda conn, expires: self.inbox._record(conn, self.inbox._delivery(self.delivery_id)))
        matches = [i for i, message in enumerate(messages) if message.get("_row_id") == self.record["marker_message_id"]
                   and message.get("message_uid") == self.record["marker_message_uid"]]
        if len(matches) != 1:
            raise RuntimeError("exact completion marker is absent from native history")
        return matches[0]

    def flush(self, rows, messages, *, turn_lease_holder, compression_lock_holder=None):
        # Tool-loop flushes stage output in memory. Only a designated terminal
        # response can commit it, together with its receipt in one transaction.
        if self.terminal_message is None:
            self.inbox.authorize_staged_turn(self.delivery_id, rows, turn_lease_holder=turn_lease_holder)
            return False
        indices = [i for i, message in enumerate(messages) if message is self.terminal_message]
        if len(indices) != 1:
            raise RuntimeError("terminal completion response identity is absent")
        self.record = self.inbox.publish(self.delivery_id, rows, indices[0], turn_lease_holder=turn_lease_holder,
                                         compression_lock_holder=compression_lock_holder)
        return True

    def dispatch(self, name, args):
        # This allowlist intentionally has one transactional DB-only mutation.
        # New effect adapters need independent durable idempotency review.
        if name != "kanban_comment":
            return json.dumps({"error": "completion_replay_unsafe", "tool": name})
        with self._slot_lock:
            slot = self._slot
            self._slot += 1
        try:
            holder = getattr(self.agent, "_active_session_turn_lease_holder", None)
            if self.agent is not None and holder is None:
                raise PermissionError("native completion effect requires its admitted turn lease")
            return self.inbox.effect(self.delivery_id, slot, name, args, turn_lease_holder=holder)
        except (PermissionError, RuntimeError, ValueError) as error:
            return json.dumps({"error": "completion_replay_unsafe", "reason": str(error)})

    def run(self, agent):
        self.validate_agent(agent)
        self.agent = agent
        if self.record["response_message_id"] is not None:
            return
        history = self.inbox.state.get_messages_as_conversation(
            self.record["session_id"], include_row_ids=True, repair_alternation=False)
        index = self.marker_index(agent, history)
        agent.run_conversation(history[index]["content"], conversation_history=history, task_id=self.delivery_id)


@contextmanager
def completion_execution(inbox, delivery_id):
    if current_execution() is not None:
        raise PermissionError("nested completion execution is prohibited")
    execution = CompletionExecution(inbox, delivery_id)
    token = _EXECUTION.set(execution)
    try:
        yield execution
    finally:
        _EXECUTION.reset(token)


def dispatch_completion_tool(name, args):
    """Called BEFORE native dispatch/hooks, including inline and nested tools."""
    execution = current_execution()
    return None if execution is None else execution.dispatch(name, args)


def completion_tool_rejection(name):
    if current_execution() is not None and name != "kanban_comment":
        return json.dumps({"error": "completion_replay_unsafe", "tool": name})
    return None


def completion_attempt_holder(holder):
    if current_execution() is not None:
        import uuid
        return f"{holder}:attempt={uuid.uuid4().hex}"
    return holder


def completion_turn_id(agent, ordinary_turn_id):
    execution = current_execution()
    if execution is None:
        return ordinary_turn_id
    execution.validate_agent(agent)
    return execution.record["turn_id"]


def committed_completion_result(agent):
    execution = current_execution()
    if execution is None:
        return None
    execution.validate_agent(agent)
    record = execution.inbox._locked(
        lambda conn, expires: execution.inbox._record(conn, execution.inbox._delivery(execution.delivery_id)))
    if record["response_message_id"] is None:
        return None
    return {"final_response": "", "completion_receipt": record, "messages": [], "interrupted": False}
