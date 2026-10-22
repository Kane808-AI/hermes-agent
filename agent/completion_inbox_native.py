"""Completion-only native admission. Retain turn leases, omit surface/relay effects."""
from contextlib import suppress


def run_native_turn(execution, agent, user_message, system_message, conversation_history, task_id, **kwargs):
    from agent.conversation_loop import run_conversation
    from agent.interrupt_scope import track_in_interrupt_scope
    from agent.turn_facade_lease import admit_durable_turn_lease
    from agent.completion_inbox_execution import committed_completion_result
    execution.validate_agent(agent)
    if agent is not execution.agent:
        raise PermissionError("completion turn requires its bound native agent")
    turn_id = execution.record["turn_id"]
    agent._relay_pending_turn_id = turn_id
    lease = None
    try:
        admission = admit_durable_turn_lease(
            agent, session_id=agent.session_id, relay_turn_id=turn_id,
            task_context={"session_id": agent.session_id, "task_id": task_id, "platform": agent.platform},
            conversation_history=conversation_history)
        if admission.early_result is not None:
            return {"final_response": "", "messages": [], "failed": True}
        lease = admission.lease
        execution.require_bound_turn(getattr(agent, "_active_session_turn_lease_holder", None))
        committed = committed_completion_result(agent)
        if committed is not None:
            return committed
        with track_in_interrupt_scope(agent):
            lease.start()
            # Per-call streaming/TTS arguments cannot opt out of the silent scope.
            kwargs["stream_callback"] = None
            result = run_conversation(agent, user_message, system_message, admission.conversation_history, task_id, **kwargs)
            # Early error/preflight returns also bypass the ordinary finalizer.
            return {"final_response": "", "messages": [], "failed": bool(result.get("failed", False)),
                    "interrupted": bool(result.get("interrupted", False))}
    finally:
        if lease is not None:
            lease.stop_refresher()
            lease.join_threads()
            lease.clear_interrupt()
            lease.release()
        with suppress(Exception):
            agent._reset_activity_labels_after_turn()
        agent._relay_pending_turn_id = None
