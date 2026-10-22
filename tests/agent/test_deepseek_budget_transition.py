"""Prospective activation accounting, without rewriting incomplete history or making calls."""
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
import sqlite3

import pytest

from agent.deepseek_budget import main
from agent.deepseek_budget_ledger import Ledger
from agent.deepseek_budget_policy import BudgetDenied, NANO_USD, Policy

NOW = datetime(2026, 10, 21, 2, tzinfo=UTC)  # October 20 Pacific
REF = "test-only-human-decision-reference"


@pytest.fixture
def ledger(tmp_path):
    result = Ledger(tmp_path / 'ledger.sqlite3', clock=lambda: NOW)
    result.initialize()
    return result


def test_initial_hold_has_zero_admissible_capacity_and_preserves_history(ledger):
    before = ledger.status()
    assert before['state'] == 'held' and before['available_usd'] == 0
    ledger.activate_transition(1000, REF, '2026-10-21')
    after = ledger.status()
    assert after['initial_hold_day'] == before['initial_hold_day'] == '2026-10-20'
    assert after['historical_initial_day_spend'].startswith('unknown')
    assert after['accounting_period'] == 'prospective activation allowance'
    assert after['limit_usd'] == after['effective_limit_usd'] == 10
    assert after['state'] == 'ready'


def test_transition_repetition_cannot_reset_or_increase_allowance(ledger):
    ledger.activate_transition(300, REF, '2026-10-21')
    ledger.reserve('Atlas', 'pro', 3 * NANO_USD, 8192)
    ledger.activate_transition(300, REF, '2026-10-21')
    assert ledger.status()['state'] == 'exhausted'
    with pytest.raises(BudgetDenied, match='transition allowance'):
        ledger.reserve('Career', 'pro', 1, 8192)
    for cents, ref in [(1000, REF), (300, 'another-decision'), (200, REF)]:
        with pytest.raises(BudgetDenied, match='immutable'):
            ledger.activate_transition(cents, ref, '2026-10-21')
    assert ledger.status()['pending_usd'] == 3


@pytest.mark.parametrize('cents,ref', [(0, REF), (-1, REF), (1001, REF), (True, REF),
                                      (100.0, REF), (100, ''), (100, '  '), (100, 'x' * 201)])
def test_invalid_activation_does_not_release_hold(ledger, cents, ref):
    with pytest.raises(BudgetDenied):
        ledger.activate_transition(cents, ref, '2026-10-21')
    assert ledger.status()['state'] == 'held'


def test_activation_requires_existing_initial_day_and_stays_immutable_after_midnight(ledger, tmp_path):
    missing = Ledger(tmp_path / 'missing.sqlite3', clock=lambda: NOW)
    with pytest.raises(BudgetDenied, match='initialize'):
        missing.activate_transition(1000, REF, '2026-10-21')
    assert not missing.path.exists()
    for instant in (NOW - timedelta(days=1), NOW + timedelta(days=1)):
        with pytest.raises(BudgetDenied, match='current initial'):
            Ledger(ledger.path, clock=lambda: instant).activate_transition(1000, REF, '2026-10-21')


def test_shared_transition_concurrency_across_agents_and_restart(ledger):
    ledger.activate_transition(1000, REF, '2026-10-21')
    def reserve(i):
        worker = Ledger(ledger.path, clock=lambda: NOW)
        try:
            worker.reserve('Atlas' if i % 2 else 'Career', 'pro', NANO_USD, 8192)
            return True
        except BudgetDenied:
            return False
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert sum(pool.map(reserve, range(24))) == 10
    resumed = Ledger(ledger.path, clock=lambda: NOW)
    assert resumed.status()['pending_usd'] == 10
    assert resumed.status()['warning_recorded']


def test_midnight_carries_pending_cost_and_restores_calendar_cap(ledger):
    ledger.activate_transition(300, REF, '2026-10-21')
    reservation = ledger.reserve('Career', 'pro', 3 * NANO_USD, 8192)
    midnight = datetime(2026, 10, 21, 7, tzinfo=UTC)
    tomorrow = Ledger(ledger.path, clock=lambda: midnight)
    status = tomorrow.status()
    assert status['day'] == '2026-10-21'
    assert status['accounting_period'] == 'Pacific calendar day'
    assert status['effective_limit_usd'] == 10 and status['available_usd'] == 7
    tomorrow.reserve('Atlas', 'flash', 7 * NANO_USD, 8192)
    with pytest.raises(BudgetDenied, match='shared \\$10'):
        tomorrow.reserve('Atlas', 'flash', 1, 1)
    assert tomorrow.settle(reservation, 'receipt', {'prompt_tokens': 100, 'completion_tokens': 10,
                                                 'prompt_cache_hit_tokens': 0, 'prompt_cache_miss_tokens': 100})
    assert tomorrow.status()['pending_usd'] == 7
    assert tomorrow.status()['settled_upper_usd'] > 0
    assert ledger.status()['settled_upper_usd'] > 0


def test_concurrent_identical_activation_is_idempotent(ledger):
    def activate(_):
        return Ledger(ledger.path, clock=lambda: NOW).activate_transition(1000, REF, '2026-10-21')
    with ThreadPoolExecutor(max_workers=4) as pool:
        assert set(pool.map(activate, range(8))) == {'2026-10-20'}
    with sqlite3.connect(ledger.path) as db:
        assert db.execute("SELECT count(*) FROM metadata WHERE key='transition'").fetchone()[0] == 1


def test_corrupt_transition_is_fail_closed(ledger):
    with sqlite3.connect(ledger.path) as db:
        db.execute("INSERT INTO metadata VALUES ('transition','broken')")
    with pytest.raises(BudgetDenied, match='transition record invalid'):
        ledger.reserve('Atlas', 'pro', 1, 1)
    with pytest.raises(BudgetDenied):
        ledger.status()


def test_cli_requires_paired_arguments_without_mutation(ledger, monkeypatch, capsys):
    import agent.deepseek_budget as cli
    monkeypatch.setattr(cli, 'load_policy', lambda: Policy(ledger.path.parent, {'a' * 64: 'test'}))
    for args in (['--activate-transition-cents', '1000'], ['--authorization-ref', REF]):
        with pytest.raises(SystemExit):
            main(args)
    assert ledger.status()['state'] == 'held'


def test_warning_threshold_and_rate_expiry_unchanged(ledger, caplog):
    ledger.activate_transition(1000, REF, '2026-10-21')
    ledger.reserve('Atlas', 'pro', 8 * NANO_USD, 8192)
    ledger.reserve('Career', 'pro', 2 * NANO_USD, 8192)
    assert sum('allocated at least $8' in r.message for r in caplog.records) == 1
    expired = Ledger(ledger.path, clock=lambda: datetime(2026, 11, 1, tzinfo=UTC))
    assert expired.status()['state'] == 'rate review required'
    assert expired.status()['available_usd'] == 0


def test_paid_authority_ends_after_second_pacific_day(ledger):
    ledger.activate_transition(1000, REF, '2026-10-21')
    end = datetime(2026, 10, 22, 7, tzinfo=UTC)
    before = Ledger(ledger.path, clock=lambda: end - timedelta(microseconds=1))
    assert before.status()['state'] == 'ready'
    after = Ledger(ledger.path, clock=lambda: end)
    assert after.status()['state'] == 'authorization expired'
    assert after.status()['available_usd'] == 0
    with pytest.raises(BudgetDenied, match='authorization window ended'):
        after.reserve('Career', 'pro', 1, 1)
    with pytest.raises(BudgetDenied, match='current initial'):
        after.activate_transition(1000, 'new-reference', '2026-10-23')


@pytest.mark.parametrize('through', ['2026-10-19', '2026-10-22', 'forever', '', None])
def test_activation_cannot_become_perpetual(ledger, through):
    with pytest.raises(BudgetDenied, match='explicit authority ends'):
        ledger.activate_transition(1000, REF, through)
    assert ledger.status()['state'] == 'held'
