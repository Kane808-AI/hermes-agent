"""Offline safety contract: real SQLite, HTTPX/SDK with mock transports, no paid probes."""
import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from agent.deepseek_budget_policy import (
    ALIASES, BudgetDenied, INPUT_CEILING, LIMIT, OUTPUT_CAP, Policy,
    budget_denial, load_policy, local_day, request_bound, usage_cost,
)
from agent.deepseek_budget_ledger import Ledger
from agent.deepseek_budget_http import (
    AsyncBudgetTransport, BudgetTransport, Guard, guard_client_kwargs, install_guard,
)

NOW = datetime(2026, 10, 21, 19, tzinfo=UTC)
KEY = "test-only-not-a-real-credential"
BODY = {"model": "deepseek-flash", "messages": [{"role": "user", "content": "Hi"}]}


def initialize(path):
    ledger = Ledger(path, clock=lambda: NOW - timedelta(days=1))
    with pytest.raises(BudgetDenied, match="initial accounting day"):
        ledger.reserve("fixture-account", "flash", 100, 10)
    return Ledger(path, clock=lambda: NOW)


@pytest.fixture
def ledger(tmp_path):
    return initialize(tmp_path / "budget.sqlite3")


@pytest.fixture
def policy(tmp_path):
    return Policy(tmp_path, {hashlib.sha256(KEY.encode()).hexdigest(): "fixture-account"})


def request(body=None, url="https://api.deepseek.com/v1/chat/completions", key=KEY):
    return httpx.Request("POST", url, headers={"authorization": f"Bearer {key}"}, json=body or BODY)


def usage(hit=200, miss=100, output=50):
    return {"prompt_tokens": hit+miss, "completion_tokens": output,
            "prompt_cache_hit_tokens": hit, "prompt_cache_miss_tokens": miss}


def response(identifier="fixture-response", tokens=None):
    return {"id": identifier, "model": "deepseek-flash", "object": "chat.completion",
            "created": 1, "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"},
                                      "finish_reason": "stop"}], "usage": tokens or usage()}


def test_initial_day_hold_persists_and_status_never_creates(tmp_path):
    path = tmp_path / "no-dir" / "ledger.db"
    before = Ledger(path, clock=lambda: NOW).status()
    assert "uninitialized" in before["state"] and not path.parent.exists()
    ledger = Ledger(path, clock=lambda: NOW)
    for _ in range(2):
        with pytest.raises(BudgetDenied, match="initial accounting day"):
            ledger.reserve("account", "flash", 100, 10)
    original = path.read_bytes()
    assert ledger.status()["state"] == "held"
    assert path.read_bytes() == original
    assert Ledger(path, clock=lambda: NOW + timedelta(days=1)).reserve("account", "flash", 100, 10)


def test_ten_dollars_boundary_eight_warning_once_and_restart(ledger, caplog):
    ledger.reserve("one", "flash", 8_000_000_000, 8192)
    ledger.reserve("two", "pro", 2_000_000_000, 8192)
    with pytest.raises(BudgetDenied, match=r"shared \$10"):
        Ledger(ledger.path, clock=lambda: NOW).reserve("three", "flash", 1, 1)
    assert ledger.status()["available_usd"] == 0
    assert ledger.status()["warning_recorded"]
    assert sum("allocated at least $8" in record.message for record in caplog.records) == 1


def _reserve_process(args):
    path, amount = args
    try:
        Ledger(Path(path), clock=lambda: NOW).reserve("account", "flash", amount, 1)
        return True
    except BudgetDenied:
        return False


def test_atomic_across_processes_and_threads(ledger):
    # Separate SQLite connections/processes, not a mocked in-process mutex.
    with ProcessPoolExecutor(max_workers=4) as pool:
        admitted = list(pool.map(_reserve_process, [(str(ledger.path), LIMIT//10)] * 24))
    assert sum(admitted) == 10
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert not any(pool.map(_reserve_process, [(str(ledger.path), 1)] * 16))
    assert ledger.status()["pending_usd"] == 10


@pytest.mark.parametrize("model", list(ALIASES))
def test_aliases_output_bound_prompt_stability_and_peak(model):
    original = {**BODY, "model": model, "max_tokens": 100_000, "stream": True}
    capped, family, bound = request_bound(original)
    assert capped["messages"] == original["messages"]
    assert original["max_tokens"] == 100_000
    assert capped["max_tokens"] == OUTPUT_CAP and capped["stream_options"]["include_usage"]
    assert bound == INPUT_CEILING * (1320 if family == 'pro' else 300) + OUTPUT_CAP * (3960 if family == 'pro' else 1200)


def test_cache_pricing_at_peak_even_offpeak(ledger):
    body, family, bound = request_bound({**BODY, "model": "deepseek-v4-pro"})
    reservation = ledger.reserve("account", family, bound, body["max_tokens"])
    cost, *_ = usage_cost(family, usage(), body["max_tokens"])
    assert cost == 200*44 + 100*1320 + 50*3960
    assert ledger.settle(reservation, "unique", usage())
    assert ledger.status()["settled_upper_usd"] == cost / 1_000_000_000
    assert ledger.status()["pending_usd"] == 0


@pytest.mark.parametrize("change", [
    {"model": "deepseek-new"}, {"model": "deepseek-v3"}, {"n": 2}, {"max_tokens": 0},
    {"max_tokens": True}, {"search": True}, {"tools": [{"type": "web_search"}]},
    {"max_completion_tokens": -1}, {"messages": []},
])
def test_unknown_cost_shape_rejected(change):
    with pytest.raises(BudgetDenied):
        request_bound({**BODY, **change})


def test_lower_output_cap_preserved_and_function_tools_allowed():
    capped, _, _ = request_bound({**BODY, "max_tokens": 21,
                                  "tools": [{"type": "function", "function": {"name": "local_read"}}]})
    assert capped["max_tokens"] == 21


def test_receipts_idempotent_duplicate_retry_not_free(ledger):
    a = ledger.reserve("account", "flash", 400_000_000, OUTPUT_CAP)
    b = ledger.reserve("account", "flash", 400_000_000, OUTPUT_CAP)
    assert ledger.settle(a, "same-id", usage())
    before = ledger.status()
    assert ledger.settle(a, "same-id", usage())
    assert ledger.status() == before
    assert not ledger.settle(b, "same-id", usage())
    assert ledger.status()["pending_usd"] == 0.4


@pytest.mark.parametrize("tokens", [{}, {"prompt_tokens": 100}, usage(hit=-1), usage(output=100_000)])
def test_missing_invalid_or_excess_usage_keeps_reservation(ledger, tokens):
    reservation = ledger.reserve("account", "flash", 400_000_000, OUTPUT_CAP)
    assert not ledger.settle(reservation, "id", tokens)
    assert ledger.status()["pending_usd"] == 0.4
    if tokens.get("completion_tokens", 0) > OUTPUT_CAP:
        with pytest.raises(BudgetDenied, match="receipt exceeded"):
            ledger.reserve("account", "flash", 1, 1)


def test_crash_and_cross_midnight_do_not_refund(ledger):
    reservation = ledger.reserve("account", "flash", 400_000_000, OUTPUT_CAP)
    tomorrow = Ledger(ledger.path, clock=lambda: NOW+timedelta(days=1))
    assert tomorrow.status()["pending_usd"] == 0.4
    assert tomorrow.settle(reservation, "late", usage())
    assert tomorrow.status()["settled_upper_usd"] == ledger.status()["settled_upper_usd"] > 0
    assert Ledger(ledger.path, clock=lambda: NOW+timedelta(days=2)).status()["available_usd"] == 10


def test_pacific_midnight_dst_and_clock_rollback(ledger, monkeypatch):
    assert local_day(datetime.fromisoformat("2026-11-01T06:59:59+00:00")) == "2026-10-31"
    assert local_day(datetime.fromisoformat("2026-11-01T07:00:00+00:00")) == "2026-11-01"
    assert local_day(datetime.fromisoformat("2026-11-02T07:59:59+00:00")) == "2026-11-01"
    assert local_day(datetime.fromisoformat("2026-11-02T08:00:00+00:00")) == "2026-11-02"
    assert local_day(datetime.fromisoformat("2026-03-08T07:59:59+00:00")) == "2026-03-07"
    assert local_day(datetime.fromisoformat("2026-03-09T07:00:00+00:00")) == "2026-03-09"
    with pytest.raises(BudgetDenied):
        local_day(datetime(2026, 10, 20))
    reservation = ledger.reserve("account", "flash", 100_000_000, OUTPUT_CAP)
    previous = Ledger(ledger.path, clock=lambda: NOW-timedelta(days=1))
    assert not previous.settle(reservation, "rollback", usage())


def test_rate_expiry_fails_closed(ledger):
    with pytest.raises(BudgetDenied, match="rate snapshot expired"):
        Ledger(ledger.path, clock=lambda: datetime(2026, 11, 1, 19, tzinfo=UTC)).reserve("account", "flash", 1, 1)


def test_account_mapping_no_key_or_digest_in_ledger(ledger, policy):
    guard = Guard(policy, ledger=ledger)
    with pytest.raises(BudgetDenied, match="account mapping"):
        guard.prepare(request(key="unmapped-test-key"))
    guard.prepare(request())
    assert KEY.encode() not in ledger.path.read_bytes()
    assert hashlib.sha256(KEY.encode()).hexdigest().encode() not in ledger.path.read_bytes()


@pytest.mark.parametrize("url", [
    "https://router.invalid/v1/chat/completions", "http://api.deepseek.com/v1/chat/completions",
    "https://api.deepseek.com:444/v1/chat/completions", "https://api.deepseek.com/embeddings",
    "https://api.deepseek.com/search", "https://api.deepseek.com/v1/chat/completions?override=1",
])
def test_unmapped_route_and_endpoint_bypass_detection(ledger, policy, url):
    with pytest.raises(BudgetDenied):
        Guard(policy, ledger=ledger).prepare(request(url=url))
    assert ledger.status()["pending_usd"] == 0


def test_other_provider_and_balance_reads_unchanged(ledger, policy):
    guard = Guard(policy, ledger=ledger)
    original = request(body={**BODY, "model": "other"}, url="https://example.invalid/v1/chat/completions")
    assert guard.prepare(original) == (original, None)
    balance = httpx.Request("GET", "https://api.deepseek.com/user/balance")
    assert guard.prepare(balance) == (balance, None)
    assert ledger.status()["pending_usd"] == 0


def test_sync_httpx_sdk_wire_cap_and_success_settlement(ledger, policy):
    from openai import OpenAI
    seen = []
    def handler(req):
        seen.append(json.loads(req.content))
        return httpx.Response(200, json=response())
    transport = BudgetTransport(httpx.MockTransport(handler), Guard(policy, ledger=ledger))
    with httpx.Client(transport=transport) as http_client:
        client = OpenAI(api_key=KEY, base_url="https://api.deepseek.com/v1", http_client=http_client, max_retries=0)
        result = client.chat.completions.create(**BODY, max_tokens=50_000)
    assert result.choices[0].message.content == 'ok'
    assert seen[0]["max_tokens"] == OUTPUT_CAP
    assert ledger.status()["pending_usd"] == 0
    assert ledger.status()["settled_upper_usd"] > 0


def test_sdk_retries_reserve_every_physical_attempt(ledger, policy, monkeypatch):
    from openai import OpenAI
    calls = []
    def handler(req):
        calls.append(req)
        if len(calls) == 1:
            return httpx.Response(500, json={"error": {"message": "mock failure"}})
        return httpx.Response(200, json=response())
    transport = BudgetTransport(httpx.MockTransport(handler), Guard(policy, ledger=ledger))
    with httpx.Client(transport=transport) as http_client:
        client = OpenAI(api_key=KEY, base_url="https://api.deepseek.com/v1", http_client=http_client, max_retries=1)
        monkeypatch.setattr(client, '_calculate_retry_timeout', lambda *a, **k: 0)
        client.chat.completions.create(**BODY)
    assert len(calls) == 2
    assert ledger.status()["pending_usd"] == request_bound(BODY)[2] / 1_000_000_000


class Chunks(httpx.SyncByteStream):
    def __init__(self, chunks, fail=False):
        self.chunks, self.fail = chunks, fail
    def __iter__(self):
        yield from self.chunks
        if self.fail:
            raise httpx.ReadError("mock partial stream")


@pytest.mark.parametrize("complete", [True, False])
def test_stream_done_vs_partial(ledger, policy, complete):
    payload = b'data: ' + json.dumps(response()).encode() + b'\n\n'
    chunks = [payload[:13], payload[13:]] + ([b'data: [DONE]\n\n'] if complete else [])
    def handler(req):
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Chunks(chunks))
    with httpx.Client(transport=BudgetTransport(httpx.MockTransport(handler), Guard(policy, ledger=ledger))) as client:
        client.send(request({**BODY, "stream": True})).read()
    assert (ledger.status()["pending_usd"] == 0) is complete


def test_partial_exception_and_close_keep_full_bound(ledger, policy):
    def handler(req):
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Chunks([b'data: {}\n\n'], fail=True))
    with httpx.Client(transport=BudgetTransport(httpx.MockTransport(handler), Guard(policy, ledger=ledger))) as client:
        with pytest.raises(httpx.ReadError):
            client.send(request()).read()
    assert ledger.status()["pending_usd"] == request_bound(BODY)[2]/1_000_000_000


def test_async_sdk_and_transport(ledger, policy):
    from openai import AsyncOpenAI
    async def run():
        async def handler(req):
            assert json.loads(req.content)["max_tokens"] == OUTPUT_CAP
            return httpx.Response(200, json=response())
        transport = AsyncBudgetTransport(httpx.MockTransport(handler), Guard(policy, ledger=ledger))
        async with httpx.AsyncClient(transport=transport) as http_client:
            client = AsyncOpenAI(api_key=KEY, base_url="https://api.deepseek.com/v1", http_client=http_client, max_retries=0)
            await client.chat.completions.create(**BODY)
    asyncio.run(run())
    assert ledger.status()["pending_usd"] == 0


def test_wraps_custom_client_mounts_and_refuses_sdk_default_bypass(policy, monkeypatch):
    import agent.deepseek_budget_http as module
    monkeypatch.setattr(module, "load_policy", lambda: policy)
    with pytest.raises(BudgetDenied, match="bypass refused"):
        guard_client_kwargs({})
    with httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(200)),
                      mounts={"https://api.deepseek.com": httpx.MockTransport(lambda r: httpx.Response(200))}) as client:
        kwargs = {"http_client": client}
        guard_client_kwargs(kwargs)
        assert isinstance(client._transport, BudgetTransport)
        assert all(t is None or isinstance(t, BudgetTransport) for t in client._mounts.values())
        before = client._transport
        install_guard(client)
        assert client._transport is before


def test_policy_root_shared_across_profiles_and_malformed_hold(tmp_path, monkeypatch):
    root = tmp_path/'team'
    root.mkdir()
    account = {hashlib.sha256(KEY.encode()).hexdigest(): "team-account"}
    (root/'deepseek-budget.yaml').write_text(json.dumps({"enabled": True, "credential_accounts": account}))
    import hermes_constants
    monkeypatch.setattr(hermes_constants, "get_default_hermes_root", lambda: root)
    for profile in ['default','builder','verify']:
        monkeypatch.setenv('HERMES_HOME', str(root/'profiles'/profile))
        assert load_policy().ledger_path == root/'deepseek-budget.sqlite3'
    (root/'deepseek-budget.yaml').write_text('{enabled: true, daily_limit: 100}')
    with pytest.raises(BudgetDenied, match="configuration invalid"):
        load_policy()


def test_budget_error_survives_sdk_wrap_and_aux_recovery(ledger):
    from agent.auxiliary_client import _drive_ladder, _drive_ladder_async, _is_transient_transport_error
    denied = BudgetDenied("local limit")
    wrapper = RuntimeError("Connection error")
    wrapper.__cause__ = denied
    assert budget_denial(wrapper) is denied
    assert not _is_transient_transport_error(wrapper)
    def ladder():
        try:
            yield 'first'
        except Exception:
            pytest.fail('budget denial must never reach fallback ladder')
    def perform(step):
        raise wrapper
    with pytest.raises(BudgetDenied):
        _drive_ladder(ladder(), perform)
    async def aperform(step):
        raise wrapper
    with pytest.raises(BudgetDenied):
        asyncio.run(_drive_ladder_async(ladder(), aperform))


def test_main_budget_denial_is_terminal_before_recovery(monkeypatch):
    import inspect
    from agent import turn_api_error
    monkeypatch.setattr(turn_api_error, 'recover_before_classification', lambda *a, **k: pytest.fail('no retry/fallback'))
    arguments = {name: None for name in inspect.signature(turn_api_error.handle_api_error).parameters}
    arguments.update(agent=SimpleNamespace(thinking_callback=None), api_error=BudgetDenied('local limit'),
                     messages=[], api_call_count=0)
    verdict = turn_api_error.handle_api_error(**arguments)
    assert verdict.action == 'return'
    assert verdict.result['failure_reason'] == 'deepseek_budget'
    assert not verdict.result['failure_retryable']


def test_corrupt_ledger_blocks_before_transport(policy, tmp_path):
    bad = tmp_path/'corrupt.db'
    bad.write_text('not sqlite')
    guard = Guard(policy, ledger=Ledger(bad, clock=lambda: NOW))
    with pytest.raises(BudgetDenied, match='accounting/request validation unavailable'):
        guard.prepare(request())


def test_builder_failure_cannot_fall_through_to_sdk_default(policy, monkeypatch):
    import agent.deepseek_budget_http as module
    from agent import process_bootstrap
    monkeypatch.setattr(module, 'load_policy', lambda: policy)
    monkeypatch.setattr(process_bootstrap, '_build_keepalive_http_client', lambda *a, **k: None)
    with pytest.raises(BudgetDenied, match='bypass refused'):
        process_bootstrap.build_keepalive_http_client('https://api.deepseek.com')


def test_anthropic_wire_blocked_until_priced(policy, monkeypatch):
    import agent.deepseek_budget_http as module
    monkeypatch.setattr(module, 'load_policy', lambda: policy)
    with pytest.raises(BudgetDenied, match='Anthropic wire is not covered'):
        module.reject_uncovered_anthropic_route('https://api.deepseek.com/anthropic')
    module.reject_uncovered_anthropic_route('https://api.anthropic.com')


def test_actual_primary_and_aux_client_factories_are_guarded(ledger, policy, monkeypatch):
    import agent.deepseek_budget_http as module
    from agent import process_bootstrap, auxiliary_client
    from agent.agent_runtime_helpers import create_openai_client
    from unittest.mock import Mock
    monkeypatch.setattr(module, 'load_policy', lambda: policy)
    monkeypatch.setattr(module, 'Ledger', lambda *a, **k: ledger)
    seen = []
    def handler(req):
        seen.append(json.loads(req.content))
        return httpx.Response(200, json=response(identifier='factory-'+str(len(seen)) ))
    def build(*a, **k):
        return httpx.Client(transport=httpx.MockTransport(handler))
    monkeypatch.setattr(process_bootstrap, '_build_keepalive_http_client', build)
    agent = Mock(provider='deepseek', model='deepseek-flash')
    agent._build_keepalive_http_client = process_bootstrap.build_keepalive_http_client
    primary = create_openai_client(agent, {'api_key': KEY, 'base_url': 'https://api.deepseek.com/v1'},
                                   reason='test', shared=False)
    aux = auxiliary_client._create_openai_client(api_key=KEY, base_url='https://api.deepseek.com/v1')
    try:
        primary.chat.completions.create(**BODY)
        aux.chat.completions.create(**BODY)
    finally:
        primary.close()
        aux.close()
    assert len(seen) == 2
    assert ledger.status()['pending_usd'] == 0
    assert ledger.status()['settled_upper_usd'] > 0


def test_status_cli_has_no_writes_or_delivery(policy, ledger, monkeypatch, capsys):
    from agent import deepseek_budget
    monkeypatch.setattr(deepseek_budget, 'load_policy', lambda: policy)
    monkeypatch.setattr(deepseek_budget, 'Ledger', lambda *a, **k: ledger)
    original = ledger.path.read_bytes()
    deepseek_budget.main([])
    value = json.loads(capsys.readouterr().out)
    assert value['enabled'] and value['limit_usd'] == 10
    assert ledger.path.read_bytes() == original


def test_saved_rate_snapshot_used_after_restart(ledger, monkeypatch):
    import agent.deepseek_budget_policy as policy_module
    reservation = ledger.reserve('account', 'flash', 400_000_000, OUTPUT_CAP)
    monkeypatch.setitem(policy_module.RATES, 'flash', (1, 1, 1))
    restored = Ledger(ledger.path, clock=lambda: NOW)
    assert restored.settle(reservation, 'saved-rate', usage())
    assert restored.status()['settled_upper_usd'] == (200*6+100*300+50*1200)/1_000_000_000


def test_initialization_without_request_never_clears_hold(tmp_path):
    ledger = Ledger(tmp_path/'ledger.db', clock=lambda: NOW)
    assert ledger.initialize() == local_day(NOW)
    tomorrow = Ledger(ledger.path, clock=lambda: NOW+timedelta(days=1))
    assert tomorrow.initialize() == local_day(NOW)
    assert ledger.status()['state'] == 'held'
    assert tomorrow.status()['state'] == 'ready'


def test_malformed_request_cost_fails_closed(ledger, policy):
    with pytest.raises(BudgetDenied):
        Guard(policy, ledger=ledger).prepare(request({**BODY, 'model': {}}))


def test_async_partial_stream_preserves_reservation(ledger, policy):
    class Partial(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'data: {}\n\n'
            raise httpx.ReadError('mock cancelled stream')
    async def run():
        async def handler(req):
            return httpx.Response(200, headers={'content-type': 'text/event-stream'}, stream=Partial())
        async with httpx.AsyncClient(transport=AsyncBudgetTransport(httpx.MockTransport(handler), Guard(policy, ledger=ledger))) as client:
            with pytest.raises(httpx.ReadError):
                await client.send(request({**BODY, 'stream': True}))
    asyncio.run(run())
    assert ledger.status()['pending_usd'] == request_bound(BODY)[2]/1_000_000_000


def test_sdk_budget_denial_sends_nothing_and_preserves_cause(policy, tmp_path):
    from openai import APIConnectionError, OpenAI
    held = Ledger(tmp_path/'held.db', clock=lambda: NOW)
    transport = BudgetTransport(httpx.MockTransport(lambda r: pytest.fail('no physical send while held')),
                                Guard(policy, ledger=held))
    with httpx.Client(transport=transport) as http_client:
        client = OpenAI(api_key=KEY, base_url='https://api.deepseek.com/v1', http_client=http_client, max_retries=0)
        with pytest.raises(APIConnectionError) as error:
            client.chat.completions.create(**BODY)
    assert isinstance(budget_denial(error.value), BudgetDenied)


def test_auxiliary_initial_denial_never_enters_recovery():
    import inspect
    from agent.auxiliary_client import _aux_recovery_ladder
    kwargs = {name: None for name in inspect.signature(_aux_recovery_ladder).parameters}
    kwargs['first_err'] = BudgetDenied('held')
    with pytest.raises(BudgetDenied):
        next(_aux_recovery_ladder(**kwargs))


def test_actual_anthropic_constructor_rejects_uncovered_route(policy, monkeypatch):
    import agent.deepseek_budget_http as module
    from agent.anthropic_adapter import build_anthropic_client
    monkeypatch.setattr(module, 'load_policy', lambda: policy)
    with pytest.raises(BudgetDenied, match='Anthropic wire is not covered'):
        build_anthropic_client(KEY, base_url='https://api.deepseek.com/anthropic')
