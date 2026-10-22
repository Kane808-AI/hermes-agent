"""Compatibility with separately installed project controls; no authority is created."""
import hashlib
import json
import sqlite3

import pytest

from agent.deepseek_budget_policy import load_policy
from agent.deepseek_budget_ledger import Ledger


def test_enabled_budget_never_authorizes_project_spend(tmp_path):
    project = pytest.importorskip('hermes_cli.kanban_project_contract', reason='project-control overlay not installed')
    (tmp_path/'deepseek-budget.yaml').write_text(json.dumps({
        'enabled': True, 'credential_accounts': {hashlib.sha256(b'fixture-only').hexdigest(): 'fixture'},
    }))
    policy = load_policy(tmp_path)
    assert policy is not None
    assert 'uninitialized' in Ledger(policy.ledger_path).status()['state']
    with sqlite3.connect(':memory:') as conn:
        conn.execute('CREATE TABLE project_decisions(project_id TEXT,revision TEXT,gate TEXT,receipt TEXT)')
        contract = {'id': 'fixture', 'budget': {'max_runs': 8, 'external_spend_cents': 0},
                    'permissions': ['paid_api'], 'approval_gate': 'brief', 'gates': {'brief': {}}}
        spec = {'permissions': ['paid_api'], 'gates': []}
        reasons = project.authority_reasons(conn, contract, 'fixture-revision', spec, 100)
        assert 'external action is held for its native action-specific approval' in reasons
        assert 'human decision required: brief' in reasons
        contract['budget']['external_spend_cents'] = 1000
        reasons = project.authority_reasons(conn, contract, 'fixture-revision', spec, 100)
        assert 'external spending requires a capped execution adapter; unsupported here' in reasons
        assert conn.execute('SELECT count(*) FROM project_decisions').fetchone()[0] == 0
