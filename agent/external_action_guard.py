"""Durable retry gate for explicitly identified PR creation, not general exactly-once I/O.

Enabled by external_action_guard.enabled in the served profile. No provider calls.
Reconciliation is an operator action based on external evidence, never a time lease.
"""
from __future__ import annotations

import argparse
from contextlib import closing
import hashlib
import json
from pathlib import Path
import re
import shlex
import sqlite3
import uuid


class ReconciliationRequired(RuntimeError):
    pass


def identify(name, args):
    """Return stable natural identity, ignoring editable PR title/body.

    Shell compounds, wrappers, aliases and arbitrary MCP servers are unsupported.
    Recognized simple gh commands require explicit repo/head/base; never guess CWD.
    """
    if name == 'mcp_github_create_pull_request':
        owner, repo = args.get('owner'), args.get('repo')
        if not owner or not repo:
            raise ReconciliationRequired('PR creation needs explicit owner and repo.')
        values = {'repo': f'{owner}/{repo}', 'head': args.get('head'), 'base': args.get('base')}
    elif name == 'terminal':
        command = args.get('command', '')
        if not isinstance(command, str):
            return None
        try:
            # shlex default keeps quoted strings intact, but split ordinary whitespace.
            lex = shlex.shlex(command, posix=True, punctuation_chars=True)
            lex.whitespace_split = True
            lex.commenters = ''
            tokens = list(lex)
        except ValueError:
            return None
        if tokens[:3] != ['gh', 'pr', 'create']:
            return None
        if any(t in {';', '&&', '||', '|', '&', '>', '>>', '<', '(', ')'} for t in tokens):
            raise ReconciliationRequired('Use one explicit gh pr create command, without shell compounds/redirection.')
        if any(c in command for c in ('\n', '$', '`')):
            raise ReconciliationRequired('Shell expansion and multiline commands are outside guarded PR creation.')
        values = {}
        flags = {'--repo':'repo', '-R':'repo', '--head':'head', '-H':'head', '--base':'base', '-B':'base'}
        for i, token in enumerate(tokens[3:], 3):
            flag, sep, value = token.partition('=')
            if flag in flags:
                value = value if sep else (tokens[i+1] if i+1 < len(tokens) else '')
                if flags[flag] in values:
                    raise ReconciliationRequired('Duplicate PR identity flags are ambiguous.')
                values[flags[flag]] = value
        if args.get('background'):
            raise ReconciliationRequired('Guarded PR creation must run in the foreground.')
    else:
        return None
    if not all(isinstance(values.get(k), str) and values[k].strip() for k in ('repo','head','base')):
        raise ReconciliationRequired('PR creation requires explicit --repo OWNER/REPO --head BRANCH --base BRANCH.')
    if not re.fullmatch(r'[\w.-]+/[\w.-]+', values['repo']):
        raise ReconciliationRequired('Only explicit github.com OWNER/REPO identities are covered.')
    return {'kind':'github_pr_create', 'repo':values['repo'].lower(), 'head':values['head'], 'base':values['base']}


def receipt(result, identity):
    """Only a matching GitHub PR URL plus an unambiguous success envelope settles."""
    try:
        data = json.loads(result) if isinstance(result, str) else result
    except (TypeError, ValueError):
        return None
    if not isinstance(data, dict) or data.get('error') or data.get('success') is False:
        return None
    if 'exit_code' in data:
        if type(data['exit_code']) is not int or data['exit_code'] != 0:
            return None
        text = str(data.get('output', ''))
    else:
        # Standard GitHub create-PR response. A bare model assertion is not a receipt.
        if not data.get('number') or not data.get('html_url'):
            return None
        text = str(data['html_url'])
    match = re.search(r'https://github\.com/([^/\s]+/[^/\s]+)/pull/([1-9][0-9]*)(?:\s|$)', text)
    if match and match[1].lower() == identity['repo']:
        return f'https://github.com/{match[1]}/pull/{match[2]}'
    return None


class Store:
    def __init__(self, path):
        self.path = Path(path)

    def connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=3, isolation_level=None)
        db.execute('PRAGMA synchronous=FULL')
        db.execute('CREATE TABLE IF NOT EXISTS actions (key TEXT PRIMARY KEY, identity TEXT NOT NULL, attempt TEXT NOT NULL, state TEXT NOT NULL, receipt TEXT)')
        db.execute('CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, key TEXT, attempt TEXT, event TEXT, evidence TEXT)')
        return db

    def begin(self, identity):
        canonical = json.dumps(identity, sort_keys=True, separators=(',', ':'))
        key = hashlib.sha256(canonical.encode()).hexdigest()
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT attempt,state,receipt FROM actions WHERE key=?', (key,)).fetchone()
            if row:
                db.commit()
                if row[1] == 'succeeded':
                    return key, None, row[2]
                raise ReconciliationRequired(f'Action {key} attempt {row[0]} is {row[1]}. Reconcile its external outcome before retrying. Reads and independent work may continue.')
            attempt = uuid.uuid4().hex
            db.execute('INSERT INTO actions VALUES (?,?,?,?,NULL)', (key,canonical,attempt,'inflight'))
            db.execute('INSERT INTO events(key,attempt,event) VALUES (?,?,?)',(key,attempt,'reserved_before_dispatch'))
            db.commit()
        return key, attempt, None

    def finish(self, key, attempt, url=None):
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            db.execute("UPDATE actions SET state=?,receipt=? WHERE key=? AND attempt=? AND state IN ('inflight','uncertain')", ('succeeded' if url else 'uncertain',url,key,attempt))
            db.execute('INSERT INTO events(key,attempt,event,evidence) VALUES (?,?,?,?)', (key,attempt,'receipt' if url else 'unknown',url))
            db.commit()

    def resolve(self, key, attempt, outcome, evidence, confirmed_stopped=False):
        if not evidence.strip():
            raise ValueError('Authoritative evidence is required; age or missing receipt is not evidence.')
        if not confirmed_stopped:
            raise ValueError('Confirm the original attempt has stopped before reconciliation.')
        if outcome not in {'succeeded','not_executed'}:
            raise ValueError('Invalid reconciliation outcome.')
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT identity,state FROM actions WHERE key=? AND attempt=?',(key,attempt)).fetchone()
            if not row or row[1] not in {'inflight','uncertain'}:
                raise ValueError('Stale attempt or action already reconciled; inspect current status.')
            if outcome == 'succeeded':
                identity = json.loads(row[0])
                url = receipt({'exit_code':0,'output':evidence}, identity)
                if not url:
                    raise ValueError('Succeeded reconciliation requires the matching GitHub PR URL.')
                db.execute("UPDATE actions SET state='succeeded',receipt=? WHERE key=? AND attempt=?",(url,key,attempt))
            else:
                db.execute('DELETE FROM actions WHERE key=? AND attempt=?',(key,attempt))
            db.execute('INSERT INTO events(key,attempt,event,evidence) VALUES (?,?,?,?)',(key,attempt,'operator_'+outcome,evidence))
            db.commit()

    def status(self):
        with closing(self.connect()) as db, db:
            return [dict(zip(('key','identity','attempt','state','receipt'),row)) for row in db.execute('SELECT * FROM actions')]


def default_store():
    from hermes_constants import get_default_hermes_root
    # Shared root across builder profiles and restarted workers; not their spend ledger.
    return Store(get_default_hermes_root() / 'external-actions.sqlite3')


def guarded_execute(name, args, execute, *, store=None, enabled=None):
    # No config/state I/O for unrelated tools and ordinary terminal work.
    candidate = name == 'mcp_github_create_pull_request' or (name == 'terminal' and re.match(r'^\s*gh\s+pr\s+create(?:\s|$)', str(args.get('command',''))))
    if not candidate:
        return execute(args)
    try:
        if enabled is None:
            from hermes_cli.config import load_config_readonly
            enabled = load_config_readonly().get('external_action_guard', {}).get('enabled', False) is True
        if not enabled:
            return execute(args)
        identity = identify(name, args)
        if identity is None:
            raise ReconciliationRequired('PR command could not be identified safely. Use a simple explicit command.')
        store = store or default_store()
        key, attempt, prior = store.begin(identity)
    except Exception as exc:  # health: allow BLE001 -- fail closed before dispatch on config/storage/identity errors; structured executed=false response
        return json.dumps({'error':'external_action_reconciliation_required','message':str(exc),'executed':False})
    if prior:
        return json.dumps({'success':True,'already_completed':True,'url':prior,'message':'Prior confirmed PR creation returned; no new mutation executed.'})
    try:
        result = execute(args)
    except BaseException:
        store.finish(key, attempt)
        raise
    store.finish(key, attempt, receipt(result, identity))
    return result


def main():
    parser = argparse.ArgumentParser(description='Operator-only PR reconciliation. Never clears by age; no network calls.')
    parser.add_argument('action', choices=['status','resolve'])
    parser.add_argument('--key'); parser.add_argument('--attempt')
    parser.add_argument('--outcome', choices=['succeeded','not_executed'])
    parser.add_argument('--evidence', default='')
    parser.add_argument('--confirmed-stopped', action='store_true')
    args = parser.parse_args()
    store = default_store()
    if args.action == 'status':
        print(json.dumps(store.status(), indent=2))
    else:
        store.resolve(args.key,args.attempt,args.outcome,args.evidence,args.confirmed_stopped)
        print('Reconciliation recorded. This does not grant permission for another mutation.')


if __name__ == '__main__':
    main()
