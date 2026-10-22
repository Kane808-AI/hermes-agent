"""DeepSeek budget interception at each physical HTTP attempt, sync and async.

Only installed on Hermes-built clients. External SDKs/CLIs/curl and old processes are
outside this boundary. No monkey patching global httpx or operating-system network policy.
"""
from __future__ import annotations

import json
import sqlite3
from urllib.parse import urlparse
from typing import Any

import httpx

from agent.deepseek_budget_policy import BudgetDenied, Policy, load_policy, request_bound
from agent.deepseek_budget_ledger import Ledger

_ALLOWED_PATHS = {"/chat/completions", "/v1/chat/completions", "/beta/chat/completions"}
_READ_PATHS = {"/models", "/v1/models", "/user/balance"}


class Receipt:
    """Observe only billing fields, never persist prompts or response text."""
    def __init__(self, ledger: Ledger, reservation: str, *, sse: bool):
        self.ledger, self.reservation, self.sse = ledger, reservation, sse
        self.buffer = b""
        self.usage = None
        self.response_id = None
        self.invalid = False
        self.done = False

    def _object(self, value):
        if not isinstance(value, dict) or not isinstance(value.get("usage"), dict):
            return
        usage, response_id = value["usage"], value.get("id")
        if self.usage is not None and (usage != self.usage or response_id != self.response_id):
            self.invalid = True
        self.usage, self.response_id = usage, response_id

    def feed(self, chunk: bytes):
        if self.invalid or self.done:
            return
        self.buffer += chunk
        if len(self.buffer) > 8 * 1024 * 1024:
            self.invalid = True
            self.buffer = b""
            return
        if not self.sse:
            return
        while b"\n" in self.buffer:
            line, self.buffer = self.buffer.split(b"\n", 1)
            line = line.rstrip(b"\r")
            if not line.startswith(b"data:"):
                continue
            data = line[5:].strip()
            if data == b"[DONE]":
                self.done = True
                self._settle()
                return
            try:
                self._object(json.loads(data))
            except (ValueError, UnicodeError):
                self.invalid = True

    def finish(self):
        if self.invalid or self.done:
            return
        if not self.sse:
            try:
                self._object(json.loads(self.buffer))
            except (ValueError, UnicodeError):
                self.invalid = True
            self._settle()
        # SSE without [DONE] is incomplete, even if an intermediate usage object exists.
        self.buffer = b""

    def _settle(self):
        if not self.invalid and self.usage is not None:
            try:
                self.ledger.settle(self.reservation, self.response_id, self.usage)
            except (OSError, sqlite3.Error):
                raise BudgetDenied("DeepSeek budget: receipt could not be recorded; reservation retained") from None


class _SyncReceiptStream(httpx.SyncByteStream):
    def __init__(self, inner, receipt):
        self.inner, self.receipt = inner, receipt

    def __iter__(self):
        for chunk in self.inner:
            self.receipt.feed(chunk)
            yield chunk
        self.receipt.finish()

    def close(self):
        self.inner.close()  # Never refund on early close/cancel/failure.


class _AsyncReceiptStream(httpx.AsyncByteStream):
    def __init__(self, inner, receipt):
        self.inner, self.receipt = inner, receipt

    async def __aiter__(self):
        async for chunk in self.inner:
            self.receipt.feed(chunk)
            yield chunk
        self.receipt.finish()

    async def aclose(self):
        await self.inner.aclose()


class Guard:
    def __init__(self, policy: Policy, *, ledger=None):
        self.policy = policy
        self.ledger = ledger or Ledger(policy.ledger_path, mode=policy.mode)

    def prepare(self, request: httpx.Request) -> tuple[httpx.Request, str | None]:
        try:
            return self._prepare(request)
        except BudgetDenied:
            raise
        except (OSError, sqlite3.Error, ValueError, TypeError, KeyError, AttributeError):
            raise BudgetDenied("DeepSeek budget: accounting/request validation unavailable; request held") from None

    def _prepare(self, request: httpx.Request) -> tuple[httpx.Request, str | None]:
        official_host = request.url.host == "api.deepseek.com"
        body = None
        if request.method == "POST":
            try:
                body = json.loads(request.content)
            except (ValueError, UnicodeError, httpx.RequestNotRead):
                if official_host:
                    raise BudgetDenied("DeepSeek budget: request body cannot be bounded") from None
        model = body.get("model", "") if isinstance(body, dict) else ""
        looks_deepseek = isinstance(model, str) and "deepseek" in model.lower()
        if not official_host and not looks_deepseek:
            return request, None
        if (not official_host or request.url.scheme != "https" or request.url.port not in (None, 443)
                or request.url.username or request.url.password):
            raise BudgetDenied("DeepSeek budget: unverified provider/account endpoint")
        if request.method == "GET" and request.url.path in _READ_PATHS:
            return request, None
        if (request.method != "POST" or request.url.path not in _ALLOWED_PATHS
                or request.url.query or not isinstance(body, dict)):
            raise BudgetDenied("DeepSeek budget: unsupported endpoint; cost unknown")
        account = self.policy.account(request.headers.get("authorization", ""))
        capped, family, amount = request_bound(body)
        data = json.dumps(capped, ensure_ascii=False, separators=(",", ":")).encode()
        headers = request.headers.copy()
        headers.pop("content-length", None)
        headers.pop("transfer-encoding", None)
        # Clone so retries re-validate the actual wire payload. SDK idempotency keys do
        # NOT prove the provider won't charge another physical attempt.
        guarded = httpx.Request(request.method, request.url, headers=headers, content=data,
                                extensions=dict(request.extensions))
        reservation = self.ledger.reserve(account, family, amount, capped["max_tokens"])
        return guarded, reservation

    def observe(self, response, reservation, *, async_mode=False):
        if reservation is None or not 200 <= response.status_code < 300:
            return response
        # Compressed wire bytes cannot be parsed here. Keep the reservation; httpx still
        # decodes normally for the caller. Ask for identity below, never assume it is honored.
        if response.headers.get("content-encoding", "identity").lower() != "identity":
            return response
        sse = "text/event-stream" in response.headers.get("content-type", "")
        receipt = Receipt(self.ledger, reservation, sse=sse)
        if response.is_stream_consumed:
            receipt.feed(response.content)
            receipt.finish()
            return response
        stream_cls = _AsyncReceiptStream if async_mode else _SyncReceiptStream
        response.stream = stream_cls(response.stream, receipt)
        return response


class BudgetTransport(httpx.BaseTransport):
    def __init__(self, inner, guard):
        self._inner, self.guard = inner, guard

    @property
    def _pool(self):  # Preserve existing Hermes socket-abort discovery.
        return getattr(self._inner, "_pool", None)

    def handle_request(self, request):
        request, reservation = self.guard.prepare(request)
        if reservation:
            request.headers["accept-encoding"] = "identity"
        response = self._inner.handle_request(request)
        return self.guard.observe(response, reservation)

    def close(self):
        self._inner.close()


class AsyncBudgetTransport(httpx.AsyncBaseTransport):
    def __init__(self, inner, guard):
        self._inner, self.guard = inner, guard

    @property
    def _pool(self):
        return getattr(self._inner, "_pool", None)

    async def handle_async_request(self, request):
        request, reservation = self.guard.prepare(request)
        if reservation:
            request.headers["accept-encoding"] = "identity"
        response = await self._inner.handle_async_request(request)
        return self.guard.observe(response, reservation, async_mode=True)

    async def aclose(self):
        await self._inner.aclose()


def install_guard(client: Any, *, policy: Policy | None = None):
    """Cover every mount, including explicit/environment proxies. No silent SDK fallback."""
    policy = policy or load_policy()
    if policy is None:
        return client
    if not isinstance(client, (httpx.Client, httpx.AsyncClient)):
        raise BudgetDenied("DeepSeek budget: unguardable HTTP client; SDK-default bypass refused")
    if getattr(client, "_hermes_deepseek_budget", False):
        return client
    guard = Guard(policy)
    wrapper = AsyncBudgetTransport if isinstance(client, httpx.AsyncClient) else BudgetTransport
    wrapped = {}

    def wrap(transport):
        if transport is None:
            return None
        if id(transport) not in wrapped:
            wrapped[id(transport)] = wrapper(transport, guard)
        return wrapped[id(transport)]

    client._transport = wrap(client._transport)
    client._mounts = {pattern: wrap(transport) for pattern, transport in client._mounts.items()}
    client._hermes_deepseek_budget = True
    return client


def guard_client_kwargs(kwargs: dict):
    """Validate supplied custom clients too; used by primary and auxiliary constructors."""
    if load_policy() is None:
        return
    kwargs["http_client"] = install_guard(kwargs.get("http_client"))

def reject_uncovered_anthropic_route(base_url):
    """Native Anthropic SDK uses a different client path; block official DeepSeek there."""
    if urlparse(str(base_url or "")).hostname == "api.deepseek.com" and load_policy() is not None:
        raise BudgetDenied("DeepSeek budget: Anthropic wire is not covered; request held")
