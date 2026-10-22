"""Context-local silence for internal completion turns, including inherited workers.

Ordinary threads keep their original output sinks. Policies remain effective in a
copied context after its parent exits, without changing the driver's callbacks.
"""
from __future__ import annotations

import logging
import sys
import threading
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
from functools import partial

_SILENT = ContextVar("completion_inbox_silent", default=False)
_INSTALL_LOCK = threading.Lock()
_MISSING = object()


def is_silent():
    return _SILENT.get()


def worker_target(callback, *, context=None):
    inherited = copy_context() if context is None else context
    def invoke():
        try:
            return callback()
        except BaseException:
            # threading.excepthook runs after Context.run has unwound. Completion
            # workers must contain failures here rather than expose a traceback.
            if not is_silent():
                raise
    return lambda: inherited.run(invoke)


def _discard(*args, **kwargs):
    return None


def _false(*args, **kwargs):
    return False


class _ContextOutput:
    """Transparent output router. Only the completion context discards writes."""
    def __init__(self, sink):
        self.sink = sink

    def write(self, value):
        return len(value) if is_silent() else self.sink.write(value)

    def flush(self):
        if not is_silent():
            return self.sink.flush()

    @property
    def buffer(self):
        return _ContextOutput(self.sink.buffer)

    def __getattr__(self, name):
        return getattr(self.sink, name)


class _ContextLogFilter(logging.Filter):
    def filter(self, record):
        return not is_silent()


_LOG_FILTER = _ContextLogFilter()


def _install_context_sinks():
    # Keep the transparent policies installed for late inherited workers. No
    # unrelated session's stdout, stderr or logging is redirected or disabled.
    with _INSTALL_LOCK:
        for name in ("stdout", "stderr"):
            sink = getattr(sys, name)
            if not isinstance(sink, _ContextOutput):
                setattr(sys, name, _ContextOutput(sink))
        loggers = [logging.getLogger(), *list(logging.Logger.manager.loggerDict.values())]
        for logger in loggers:
            if isinstance(logger, logging.Logger):
                for handler in logger.handlers:
                    handler.addFilter(_LOG_FILTER)


@contextmanager
def silent_scope():
    _install_context_sinks()
    token = _SILENT.set(True)
    try:
        yield
    finally:
        _SILENT.reset(token)


@contextmanager
def preserve_callbacks(agent):
    from agent.agent_init import _CALLBACK_PARAMS
    names = (*_CALLBACK_PARAMS, "_stream_callback", "_print_fn", "background_review_callback",
             "_on_session_title", "_title_failure_callback")
    saved = {name: vars(agent).get(name, _MISSING) for name in names}
    try:
        yield
    finally:
        for name, value in saved.items():
            if value is _MISSING:
                vars(agent).pop(name, None)
            else:
                setattr(agent, name, value)


_FLAGS = {"quiet_mode": True, "verbose_logging": False, "save_trajectories": False,
          "suppress_status_output": True, "_mute_notification_reply": True,
          "_deferred_title_upgrade": None, "_session_db_created": True}
_NO_EFFECT_METHODS = frozenset({
    "_save_trajectory", "_cleanup_task_resources", "_dump_api_request_debug",
    "_sync_external_memory_for_turn", "_spawn_background_review", "_persist_session_activity_if_due",
    "_safe_print", "_vprint", "_buffer_vprint", "_flush_buffered_vprint",
    "_emit_status", "_emit_warning", "_emit_diagnostic_status", "_emit_notice", "_emit_notice_clear",
    "_emit_wait_notice", "_emit_diagnostic_wait", "_emit_event", "_enqueue_stream_hook",
    "_fire_stream_delta", "_fire_reasoning_delta", "_fire_tool_gen_started", "_deliver_interim",
    "_emit_interim_assistant_message", "_deliver_to_stream_callbacks", "_emit_stream_start", "_emit_stream_end",
})
_FALSE_METHODS = frozenset({"_has_stream_consumers", "_should_start_quiet_spinner", "_should_emit_quiet_tool_messages",
                          "_try_activate_fallback", "_has_pending_fallback", "_restore_primary_runtime"})


def _persist_completion(agent, messages, conversation_history=None):
    # Keep native transcript guards/receipt publication, without async accounting
    # flushes or finalizer effects. The in-flight marker is process-local cleanup.
    from agent.agent_runtime_helpers import note_turn_persisted
    agent._drop_trailing_empty_response_scaffolding(messages)
    agent._flush_messages_to_session_db(messages, conversation_history)
    note_turn_persisted(agent)


def _reset_completion_activity(agent):
    from agent.session_activity import ActivityProvenance
    agent._last_activity_desc = ""
    agent._last_activity_provenance = ActivityProvenance.UNKNOWN


def _silent_attribute(agent, name):
    if name.endswith("_callback") or name in {"_on_session_title", "_title_failure_callback"}:
        return None
    if name in _FLAGS:
        return _FLAGS[name]
    if name == "_print_fn" or name in _NO_EFFECT_METHODS:
        return _discard
    if name in _FALSE_METHODS:
        return _false
    if name == "_persist_session":
        return partial(_persist_completion, agent)
    if name == "_reset_activity_labels_after_turn":
        return partial(_reset_completion_activity, agent)
    return _MISSING


class CompletionSilenceMixin:
    """Read scoped presentation policy at the actual callback/method lookup.

    No driver attributes are mutated. This also covers delayed native workers
    looking up callbacks after the parent's scope has finished.
    """
    def __getattribute__(self, name):
        if is_silent():
            value = _silent_attribute(self, name)
            if value is not _MISSING:
                return value
        return super().__getattribute__(name)


def accounting_store(agent):
    return None if is_silent() else agent._session_db


def can_persist_context_probe(compressor):
    return not is_silent() and getattr(compressor, "_context_probe_persistable", False)


def completion_finalizer(function):
    from functools import wraps

    @wraps(function)
    def finalize(agent, *args, **kwargs):
        if not is_silent():
            return function(agent, *args, **kwargs)
        from agent.completion_inbox_execution import current_execution
        execution = current_execution()
        execution.require_bound_turn(getattr(agent, "_active_session_turn_lease_holder", None))
        # The ordinary successful text boundary already published atomically.
        # Failed/incomplete turns publish nothing and are retried by the saga.
        from agent.agent_runtime_helpers import note_turn_persisted
        note_turn_persisted(agent)
        return {"final_response": "", "messages": [], "interrupted": kwargs["interrupted"],
                "failed": kwargs["failed"], "completed": not kwargs["failed"] and not kwargs["interrupted"]}
    return finalize
