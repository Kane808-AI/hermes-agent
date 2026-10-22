"""Accounting-only mode: isolated SQLite and mock HTTP, no network or live policy."""
import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path
import sqlite3

import httpx
import pytest

from agent.deepseek_budget_policy import BudgetDenied, Policy, load_policy, NANO_USD, OUTPUT_CAP
from agent.deepseek_budget_ledger import Ledger
from agent.deepseek_budget_http import Guard, BudgetTransport, AsyncBudgetTransport
from tests.agent.test_deepseek_budget import BODY, KEY, usage, response, request

NOW = datetime(2026, 10, 22, 19, tzinfo=UTC)
ACCOUNTS = {hashlib.sha256(KEY.encode()).hexdigest(): 'fixture-account'}


def make_ledger(path, now=NOW):
    return Ledger(path, clock=lambda: now, mode='accounting_only')


def rows(path, table):
    with sqlite3.connect(path) as db:
        return db.execute(f'SELECT * FROM {table} ORDER BY 1').fetchall()


def test_default_and_explicit_policy_loading(tmp_path):
    path = tmp_path / 'deepseek-budget.yaml'
    data = {'enabled': True, 'credential_accounts': ACCOUNTS}
    path.write_text(json.dumps(data))
    assert load_policy(tmp_path).mode == 'enforce'
    path.write_text('\ufeff'+json.dumps(data), encoding='utf-8')
    assert load_policy(tmp_path).mode == 'enforce'
    data['mode'] = 'accounting_only'
    path.write_text(json.dumps(data))
    policy = load_policy(tmp_path)
    assert policy.mode == Guard(policy).ledger.mode == 'accounting_only'
    assert Policy(tmp_path, ACCOUNTS).mode == Ledger(tmp_path/'x').mode == 'enforce'


@pytest.mark.parametrize('mode', [None, '', 'off', True, [], 'ACCOUNTING_ONLY'])
def test_invalid_mode_fails_closed(tmp_path, mode):
    with pytest.raises(BudgetDenied):
        Policy(tmp_path, ACCOUNTS, mode=mode)
    with pytest.raises(BudgetDenied):
        Ledger(tmp_path/'x', mode=mode)
    (tmp_path/'deepseek-budget.yaml').write_text(json.dumps(
        {'enabled': True, 'credential_accounts': ACCOUNTS, 'mode': mode}))
    with pytest.raises(BudgetDenied):
        load_policy(tmp_path)


def test_above_monetary_total_warning_once_and_no_balance_claim(tmp_path, caplog):
    ledger = make_ledger(tmp_path/'ledger')
    for _ in range(3):
        ledger.reserve('account', 'pro', 8*NANO_USD, OUTPUT_CAP)
    status = ledger.status()
    assert status['pending_usd'] == 24
    assert status['available_usd'] is None and status['limit_usd'] is None
    assert not status['monetary_enforcement']
    assert len(rows(ledger.path, 'warnings')) == 1
    assert sum('informational, not a spending cap' in r.message for r in caplog.records) == 1


def test_expiry_midnight_history_and_reverting_enforcement(tmp_path):
    path = tmp_path/'ledger'
    initial = Ledger(path, clock=lambda: NOW-timedelta(days=2))
    initial.initialize()
    initial.activate_transition(1000, 'fixture-approval', '2026-10-21')
    old = initial.reserve('account', 'flash', NANO_USD, OUTPUT_CAP)
    old_rows, old_metadata = rows(path, 'reservations'), rows(path, 'metadata')
    current = make_ledger(path)
    current.reserve('account', 'pro', 12*NANO_USD, OUTPUT_CAP)
    next_day = make_ledger(path, datetime(2026, 10, 23, 7, tzinfo=UTC))
    next_day.reserve('account', 'flash', NANO_USD, OUTPUT_CAP)
    assert next_day.status()['pending_usd'] == 14
    assert next_day.status()['historical_authorized_through_day'] == '2026-10-21'
    assert rows(path, 'metadata') == old_metadata
    assert [r for r in rows(path, 'reservations') if r[0] == old] == old_rows
    with pytest.raises(BudgetDenied, match='authorization window ended'):
        Ledger(path, clock=lambda: NOW).reserve('account', 'flash', 1, 1)


def test_unresolved_and_duplicate_receipts_survive_restart(tmp_path):
    ledger = make_ledger(tmp_path/'ledger')
    a, b, c = [ledger.reserve('account', 'flash', NANO_USD, OUTPUT_CAP) for _ in range(3)]
    assert ledger.settle(a, 'receipt', usage())
    settled = rows(ledger.path, 'reservations')
    assert not ledger.settle(b, 'receipt', usage())
    assert not ledger.settle(c, 'missing', {})
    restarted = make_ledger(ledger.path, NOW+timedelta(days=1))
    assert restarted.status()['pending_usd'] == 2
    restarted.reserve('account', 'flash', 11*NANO_USD, OUTPUT_CAP)
    assert [r for r in rows(ledger.path, 'reservations') if r[0] == a] == [r for r in settled if r[0] == a]
    assert restarted.status()['pending_usd'] == 13


def test_stale_rates_are_estimates_not_an_expiry_gate(tmp_path):
    later = datetime(2026, 11, 2, 19, tzinfo=UTC)
    ledger = make_ledger(tmp_path/'ledger', later)
    ledger.reserve('account', 'flash', NANO_USD, OUTPUT_CAP)
    assert ledger.status()['pricing_stale']
    assert 'not verified upper bounds' in ledger.status()['accounting']
    with pytest.raises(BudgetDenied, match='rate snapshot expired'):
        Ledger(ledger.path, clock=lambda: later+timedelta(days=1)).reserve('account', 'flash', 1, 1)


def _reserve_process(path):
    return make_ledger(Path(path)).reserve('account', 'flash', NANO_USD, OUTPUT_CAP)


def test_concurrent_processes_and_threads_preserve_every_attempt(tmp_path):
    path = tmp_path/'ledger'
    make_ledger(path).initialize()
    with ProcessPoolExecutor(max_workers=4) as pool:
        identifiers = list(pool.map(_reserve_process, [str(path)]*16))
    with ThreadPoolExecutor(max_workers=4) as pool:
        identifiers += list(pool.map(_reserve_process, [str(path)]*16))
    assert len(set(identifiers)) == len(rows(path, 'reservations')) == 32
    assert make_ledger(path).status()['pending_usd'] == 32
    assert len(rows(path, 'warnings')) == 1


@pytest.mark.parametrize('async_mode', [False, True])
@pytest.mark.parametrize('always_fail', [False, True])
def test_sdk_retry_bound_and_each_physical_attempt(tmp_path, monkeypatch, async_mode, always_fail):
    from openai import OpenAI, AsyncOpenAI, InternalServerError
    guard = Guard(Policy(tmp_path, ACCOUNTS, mode='accounting_only'))
    calls = []
    def handler(req):
        calls.append(json.loads(req.content))
        return httpx.Response(500, json={'error': {'message': 'mock'}}) if always_fail or len(calls)==1 else httpx.Response(200, json=response())
    async def run():
        async with httpx.AsyncClient(transport=AsyncBudgetTransport(httpx.MockTransport(handler), guard)) as http:
            client = AsyncOpenAI(api_key=KEY, base_url='https://api.deepseek.com/v1', http_client=http, max_retries=1)
            monkeypatch.setattr(client, '_calculate_retry_timeout', lambda *a, **kw: 0)
            if always_fail:
                with pytest.raises(InternalServerError):
                    await client.chat.completions.create(**BODY, max_tokens=50000)
            else:
                assert (await client.chat.completions.create(**BODY, max_tokens=50000)).choices[0].message.content == 'ok'
    if async_mode:
        asyncio.run(run())
    else:
        with httpx.Client(transport=BudgetTransport(httpx.MockTransport(handler), guard)) as http:
            client = OpenAI(api_key=KEY, base_url='https://api.deepseek.com/v1', http_client=http, max_retries=1)
            monkeypatch.setattr(client, '_calculate_retry_timeout', lambda *a, **kw: 0)
            if always_fail:
                with pytest.raises(InternalServerError):
                    client.chat.completions.create(**BODY, max_tokens=50000)
            else:
                assert client.chat.completions.create(**BODY, max_tokens=50000).choices[0].message.content == 'ok'
    assert len(calls) == 2 and all(c['max_tokens']==OUTPUT_CAP for c in calls)
    saved = rows(guard.ledger.path, 'reservations')
    assert len(saved) == 2
    assert sum(r[6]=='held' for r in saved) == (2 if always_fail else 1)


@pytest.mark.parametrize('change', [{'model':'unknown'}, {'n':2}, {'max_tokens':True}, {'tools':[{'type':'web_search'}]}])
def test_request_limits_remain_before_wire(tmp_path, change):
    guard = Guard(Policy(tmp_path, ACCOUNTS, mode='accounting_only'))
    calls = []
    with httpx.Client(transport=BudgetTransport(httpx.MockTransport(lambda req: calls.append(req)), guard)) as client:
        with pytest.raises(BudgetDenied):
            client.send(request({**BODY, **change}))
    assert calls == []


@pytest.mark.parametrize('kind', ['breach', 'version', 'corrupt'])
def test_accounting_integrity_holds_before_transmission(tmp_path, kind):
    guard = Guard(Policy(tmp_path, ACCOUNTS, mode='accounting_only'))
    guard.ledger.initialize()
    if kind == 'corrupt':
        guard.ledger.path.write_bytes(b'not sqlite')
    else:
        with sqlite3.connect(guard.ledger.path) as db:
            db.execute('INSERT OR REPLACE INTO metadata VALUES (?,?)', (kind, 'invalid'))
    calls = []
    with httpx.Client(transport=BudgetTransport(httpx.MockTransport(lambda req: calls.append(req)), guard)) as client:
        with pytest.raises(BudgetDenied):
            client.send(request())
    assert calls == []


@pytest.mark.parametrize('complete', [False, True])
def test_partial_stream_retains_receipt_hold(tmp_path, complete):
    guard = Guard(Policy(tmp_path, ACCOUNTS, mode='accounting_only'))
    data = b'data: '+json.dumps(response()).encode()+b'\n\n'
    if complete:
        data += b'data: [DONE]\n\n'
    with httpx.Client(transport=BudgetTransport(httpx.MockTransport(lambda req: httpx.Response(200, content=data, headers={'content-type':'text/event-stream'})), guard)) as client:
        client.send(request({**BODY, 'stream':True}))
    assert rows(guard.ledger.path, 'reservations')[0][6] == ('settled' if complete else 'held')


def test_status_is_read_only_and_unknown_earlier_usage_preserved(tmp_path):
    ledger = make_ledger(tmp_path/'missing'/'ledger')
    assert 'earlier usage unknown' in ledger.status()['state']
    assert not ledger.path.parent.exists()
    ledger.reserve('account', 'flash', NANO_USD, OUTPUT_CAP)
    before = ledger.path.read_bytes()
    assert ledger.status()['historical_initial_day_spend'].startswith('unknown')
    assert ledger.path.read_bytes() == before


def test_settled_total_over_cap_stays_accounted_and_reversible(tmp_path):
    ledger = make_ledger(tmp_path/'ledger', NOW-timedelta(days=1))
    ledger.initialize()
    ledger = make_ledger(ledger.path)
    for index in range(10):
        reservation = ledger.reserve('account', 'pro', 2*NANO_USD, OUTPUT_CAP)
        assert ledger.settle(reservation, f'receipt-{index}', usage(hit=0, miss=1_000_000, output=0))
    assert ledger.status()['settled_estimate_usd'] == 13.2
    assert ledger.status()['pending_usd'] == 0
    before = rows(ledger.path, 'reservations')
    with pytest.raises(BudgetDenied, match=r'shared \$10'):
        Ledger(ledger.path, clock=lambda: NOW).reserve('account', 'flash', 1, 1)
    assert rows(ledger.path, 'reservations') == before


def test_cli_status_resolves_mode_without_mutation(tmp_path, monkeypatch, capsys):
    from agent import deepseek_budget
    policy = Policy(tmp_path, ACCOUNTS, mode='accounting_only')
    Guard(policy).ledger.initialize()
    before = policy.ledger_path.read_bytes()
    monkeypatch.setattr(deepseek_budget, 'load_policy', lambda: policy)
    deepseek_budget.main([])
    result = json.loads(capsys.readouterr().out)
    assert result['mode'] == 'accounting_only' and result['available_usd'] is None
    assert result['authority'] == 'protection only; project external-spend holds still apply'
    assert policy.ledger_path.read_bytes() == before


@pytest.mark.parametrize('complete', [False, True])
def test_async_stream_completion_and_interruption(tmp_path, complete):
    guard = Guard(Policy(tmp_path, ACCOUNTS, mode='accounting_only'))
    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'data: '+json.dumps(response()).encode()+b'\n\n'
            if complete:
                yield b'data: [DONE]\n\n'
            else:
                raise httpx.ReadError('simulated interrupted stream')
    async def run():
        async with httpx.AsyncClient(transport=AsyncBudgetTransport(httpx.MockTransport(
                lambda req: httpx.Response(200, stream=Stream(), headers={'content-type':'text/event-stream'})), guard)) as client:
            if complete:
                await client.send(request({**BODY, 'stream': True}))
            else:
                with pytest.raises(httpx.ReadError):
                    await client.send(request({**BODY, 'stream': True}))
    asyncio.run(run())
    assert rows(guard.ledger.path, 'reservations')[0][6] == ('settled' if complete else 'held')


@pytest.mark.parametrize('kwargs', [{'key':'unknown'}, {'url':'https://api.deepseek.com/v1/embeddings'}])
def test_account_and_endpoint_restrictions_unchanged(tmp_path, kwargs):
    guard = Guard(Policy(tmp_path, ACCOUNTS, mode='accounting_only'))
    calls = []
    with httpx.Client(transport=BudgetTransport(httpx.MockTransport(lambda req: calls.append(req)), guard)) as client:
        with pytest.raises(BudgetDenied):
            client.send(request(**kwargs))
    assert not calls
