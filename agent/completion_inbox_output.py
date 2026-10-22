"""Bound newly staged completion tool output without spill files or remote writes."""
import json

from agent.completion_inbox_silence import is_silent


def bounded_result(result, limit):
    text = result if isinstance(result, str) else json.dumps(result, ensure_ascii=False)
    limit = max(0, min(int(limit), 8192))
    if len(text) <= limit:
        return text
    suffix = " [truncated]"[:limit]
    return text[:limit - len(suffix)] + suffix


def bound_completion_batch(batch, budget):
    if not is_silent():
        return False
    remaining = max(0, min(int(budget.turn_budget), 16384))
    for message in batch:
        message["content"] = bounded_result(message.get("content", ""), remaining)
        remaining -= len(message["content"])
    return True
