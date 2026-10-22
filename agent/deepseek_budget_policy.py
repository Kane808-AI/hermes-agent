"""Shared DeepSeek accounting and protection policy. Never spending authorization."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import hashlib
import re
from zoneinfo import ZoneInfo

PACIFIC = ZoneInfo("America/Los_Angeles")
NANO_USD = 1_000_000_000
LIMIT = 10 * NANO_USD
WARNING = 8 * NANO_USD
OUTPUT_CAP = 8192
# Full supported context is the only defensible tokenizer-independent input bound.
# UTF-8 length / chars-per-token heuristics are NOT reservation bounds (especially images).
INPUT_CEILING = 1_048_576
PRICING_SOURCE = "https://api-docs.deepseek.com/quick_start/pricing/"
PRICING_REVIEWED = "2026-10-20"
# Enforcement stops at the published pricing change. Accounting-only labels estimates stale.
RATES_EXPIRE = datetime(2026, 10, 31, 16, tzinfo=UTC)
ALIASES = {
    "deepseek-flash": "flash",
    "deepseek-v4-flash": "flash",
    "deepseek-v4-flash-vision-exp": "flash",
    "deepseek-v4-pro": "pro",
}
# Nanodollars per token, always PEAK prices. Settlement remains an upper estimate,
# NOT provider-reported charges. No holiday table or time-of-day discount assumptions.
RATES = {"flash": (6, 300, 1200), "pro": (44, 1320, 3960)}
MODES = frozenset({"enforce", "accounting_only"})


class BudgetDenied(RuntimeError):
    """Terminal local policy stop: must not invoke provider fallback or credential rotation."""


def budget_denial(exc: BaseException) -> BudgetDenied | None:
    seen = set()
    while exc is not None and id(exc) not in seen:
        if isinstance(exc, BudgetDenied):
            return exc
        seen.add(id(exc))
        exc = exc.__cause__ or exc.__context__
    return None


def utc_now() -> datetime:
    return datetime.now(UTC)


def local_day(now: datetime) -> str:
    if now.tzinfo is None:
        raise BudgetDenied("Budget clock must be timezone-aware")
    return now.astimezone(PACIFIC).date().isoformat()


@dataclass(frozen=True)
class Policy:
    root: Path
    accounts: dict[str, str]  # SHA256 of already-authorized bearer key -> operator-verified account label
    mode: str = "enforce"

    def __post_init__(self):
        if not isinstance(self.mode, str) or self.mode not in MODES:
            raise BudgetDenied("DeepSeek policy: unsupported accounting mode")

    @property
    def ledger_path(self) -> Path:
        return self.root / "deepseek-budget.sqlite3"

    def account(self, authorization: str) -> str:
        prefix, _, key = authorization.partition(" ")
        if prefix.lower() != "bearer" or not key:
            raise BudgetDenied("DeepSeek budget: missing account mapping")
        # Never persist/log the key, Authorization header, or even its digest.
        account = self.accounts.get(hashlib.sha256(key.encode()).hexdigest())
        if not account:
            raise BudgetDenied("DeepSeek budget: credential has no verified account mapping")
        return account


def load_policy(root: Path | None = None) -> Policy | None:
    """One root policy across profiles; profile config cannot enlarge or reset the pool."""
    if root is None:
        from hermes_constants import get_default_hermes_root
        root = get_default_hermes_root()
    path = root / "deepseek-budget.yaml"
    try:
        if not path.exists():
            return None
        import hermes_yaml as yaml
        data = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
        if not isinstance(data, dict) or type(data.get("enabled")) is not bool:
            raise ValueError("enabled must be a boolean")
        if not data["enabled"]:
            return None
        allowed = {"enabled", "credential_accounts", "mode"}
        if set(data) - allowed:
            raise ValueError("unknown shared policy keys")
        accounts = data.get("credential_accounts", {})
        if not isinstance(accounts, dict) or not accounts:
            raise ValueError("verified credential_accounts mapping required")
        for fingerprint, label in accounts.items():
            if not isinstance(fingerprint, str) or not re.fullmatch(r"[a-f0-9]{64}", fingerprint):
                raise ValueError("invalid credential fingerprint")
            if not isinstance(label, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", label):
                raise ValueError("account label must be a short non-secret identifier")
        return Policy(root.resolve(), accounts, mode=data.get("mode", "enforce"))
    except Exception as exc:
        if isinstance(exc, BudgetDenied):
            raise
        raise BudgetDenied("DeepSeek budget configuration invalid; check local policy") from None


def request_bound(body: dict) -> tuple[dict, str, int]:
    """Validate supported wire shape and impose an output cap without changing the prompt."""
    model = body.get("model")
    family = ALIASES.get(model)
    if family is None:
        raise BudgetDenied("DeepSeek budget: unknown model/rate")
    allowed = {"model", "messages", "max_tokens", "max_completion_tokens", "stream", "stream_options",
               "temperature", "top_p", "presence_penalty", "frequency_penalty", "stop", "seed",
               "tools", "tool_choice", "parallel_tool_calls", "response_format", "thinking",
               "reasoning_effort", "logprobs", "top_logprobs", "n", "user"}
    if set(body) - allowed or body.get("n", 1) != 1:
        raise BudgetDenied("DeepSeek budget: unsupported potentially billable request option")
    if not isinstance(body.get("messages"), list) or not body["messages"]:
        raise BudgetDenied("DeepSeek budget: missing messages")
    for tool in body.get("tools") or []:
        if not isinstance(tool, dict) or tool.get("type") != "function":
            raise BudgetDenied("DeepSeek budget: server tools have no covered cost bound")
    caps = [OUTPUT_CAP]
    for field in ("max_tokens", "max_completion_tokens"):
        if field in body:
            value = body[field]
            if type(value) is not int or value <= 0:
                raise BudgetDenied("DeepSeek budget: invalid output cap")
            caps.append(value)
    capped = dict(body)
    capped.pop("max_completion_tokens", None)
    capped["max_tokens"] = min(caps)
    if capped.get("stream"):
        capped["stream_options"] = {**(capped.get("stream_options") or {}), "include_usage": True}
    _, miss, output = RATES[family]
    return capped, family, INPUT_CEILING * miss + capped["max_tokens"] * output


def usage_cost(family: str, usage: dict, output_cap: int, *, rates=None) -> tuple[int, int, int, int]:
    """Strict receipt validation; uncertain receipts keep the full reservation."""
    names = ("prompt_tokens", "completion_tokens", "prompt_cache_hit_tokens", "prompt_cache_miss_tokens")
    values = [usage.get(name) for name in names]
    if any(type(value) is not int or value < 0 for value in values):
        raise BudgetDenied("DeepSeek budget: incomplete token receipt")
    prompt, output, hit, miss = values
    if hit + miss != prompt or prompt > INPUT_CEILING or output > output_cap:
        raise BudgetDenied("DeepSeek budget: usage exceeded/contradicted reservation")
    hit_rate, miss_rate, output_rate = rates or RATES[family]
    return hit * hit_rate + miss * miss_rate + output * output_rate, hit, miss, output
