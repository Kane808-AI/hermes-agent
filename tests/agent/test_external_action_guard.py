import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from agent.external_action_guard import Store, guarded_execute, identify, receipt, ReconciliationRequired

CMD = 'gh pr create --repo owner/repo --head feature --base main --title Example --body Details'
ARGS = {'command':CMD}
IDENTITY = identify('terminal', ARGS)
URL = 'https://github.com/owner/repo/pull/17'
OK = json.dumps({'exit_code':0,'output':URL+'\n'})

@pytest.fixture
def store(tmp_path):
    return Store(tmp_path/'actions.sqlite3')

def call(store, fn, args=None, name='terminal'):
    return guarded_execute(name,args or ARGS,fn,store=store,enabled=True)

def test_confirmed_success_returns_prior_receipt_without_duplicate(store):
    execute=Mock(return_value=OK)
    assert call(store,execute)==OK
    again=json.loads(call(store,execute))
    assert again['already_completed'] and again['url']==URL
    assert execute.call_count==1

@pytest.mark.parametrize('result', [json.dumps({'error':'timeout'}),json.dumps({'exit_code':1,'output':URL}),json.dumps({'exit_code':0,'output':'started'}),None,'unparseable',json.dumps({'exit_code':0,'output':'https://github.com/elsewhere/repo/pull/17'})])
def test_unknown_response_blocks_only_repeat(store,result):
    execute=Mock(return_value=result)
    call(store,execute)
    assert json.loads(call(store,execute))['executed'] is False
    assert execute.call_count==1
    assert store.status()[0]['state']=='uncertain'
    assert call(store,lambda args:'read ok',{'command':'git status'})=='read ok'
    assert call(store,lambda args:'tests ok',{'command':'pytest tests/'})=='tests ok'
    other={'command':CMD.replace('feature','independent')}
    assert call(store,Mock(return_value=OK),other)==OK

@pytest.mark.parametrize('exception', [TimeoutError,KeyboardInterrupt,SystemExit])
def test_interruption_after_possible_side_effect_holds(store,exception):
    effects=[]
    def action(args):
        effects.append('external effect simulated')
        raise exception()
    with pytest.raises(exception):call(store,action)
    assert json.loads(call(store,action))['executed'] is False
    assert len(effects)==1

def test_restart_between_reservation_and_execution_is_conservative(store):
    store.begin(IDENTITY) # simulates death anywhere after durable write, before a receipt
    fresh=Store(store.path)
    execute=Mock(return_value=OK)
    assert json.loads(call(fresh,execute))['executed'] is False
    execute.assert_not_called()

def test_title_body_edits_cannot_evade_same_pr_identity(store):
    call(store,Mock(return_value='timeout'))
    execute=Mock(return_value=OK)
    changed={'command':CMD.replace('Example','Different').replace('Details','Reworded')}
    assert json.loads(call(store,execute,changed))['executed'] is False
    execute.assert_not_called()

def test_mcp_and_gh_share_identity(store):
    call(store,Mock(return_value='timeout'))
    args={'owner':'owner','repo':'repo','head':'feature','base':'main','title':'Changed'}
    execute=Mock(return_value=json.dumps({'number':17,'html_url':URL}))
    assert json.loads(call(store,execute,args,'mcp_github_create_pull_request'))['executed'] is False
    execute.assert_not_called()

def test_reconcile_success_returns_existing_without_execution(store):
    key,attempt,_=store.begin(IDENTITY)
    store.resolve(key,attempt,'succeeded',URL,confirmed_stopped=True)
    fn=Mock()
    assert json.loads(call(store,fn))['url']==URL
    fn.assert_not_called()

def test_nonexecution_evidence_allows_one_new_attempt_and_fences_old_completion(store):
    key,old,_=store.begin(IDENTITY)
    with pytest.raises(ValueError):store.resolve(key,old,'not_executed','',True)
    with pytest.raises(ValueError):store.resolve(key,old,'not_executed','request never transmitted',False)
    store.resolve(key,old,'not_executed','Operator verified request was never transmitted and stopped',True)
    _,new,_=store.begin(IDENTITY)
    assert new!=old
    store.finish(key,old,URL) # stale completion may not clear replacement attempt
    assert store.status()[0]['state']=='inflight'
    assert store.status()[0]['attempt']==new
    with pytest.raises(ValueError):store.resolve(key,old,'not_executed','stale evidence',True)

def test_concurrent_threads_only_one_admission(store):
    def begin(_):
        try:store.begin(IDENTITY);return 'admitted'
        except ReconciliationRequired:return 'blocked'
    with ThreadPoolExecutor(max_workers=8) as pool:results=list(pool.map(begin,range(8)))
    assert results.count('admitted')==1

def test_concurrent_processes_only_one_admission(store):
    # Tiny offline local processes only; never Hermes workers, credentials or providers.
    code='''import sys,json
from agent.external_action_guard import Store,ReconciliationRequired
try:
 Store(sys.argv[1]).begin(json.loads(sys.argv[2]));print('admitted')
except ReconciliationRequired: print('blocked')
'''
    procs=[subprocess.Popen([sys.executable,'-c',code,str(store.path),json.dumps(IDENTITY)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) for _ in range(4)]
    results=[]
    for proc in procs:
        out,err=proc.communicate(timeout=10)
        assert proc.returncode==0,err
        results.append(out.strip())
    assert results.count('admitted')==1

@pytest.mark.parametrize('command',['gh pr create','gh pr create --repo owner/repo --head feature','gh pr create --repo owner/repo --head feature --base main; echo unsafe','gh pr create --repo owner/repo --head feature --base main --body "$(echo unsafe)"'])
def test_recognized_but_ambiguous_command_refused(store,command):
    fn=Mock()
    assert json.loads(call(store,fn,{'command':command}))['executed'] is False
    fn.assert_not_called()

def test_opaque_shell_is_explicitly_uncovered(store):
    fn=Mock(return_value='uncovered')
    assert call(store,fn,{'command':'bash deploy.sh'})=='uncovered'
    assert store.status()==[]

def test_store_failure_fails_closed_for_mutation_only(store):
    with patch.object(store,'begin',side_effect=OSError('disk unavailable')):
        fn=Mock()
        assert json.loads(call(store,fn))['executed'] is False
        fn.assert_not_called()
        assert call(store,lambda args:'ok',{'command':'git diff'})=='ok'

def test_guard_at_native_dispatch_boundary(tmp_path,monkeypatch):
    from tests.agent.test_fallback_429_after_timeout import _make_agent_with_fallback
    from agent.tool_executor import _run_agent_tool_execution_middleware
    agent=_make_agent_with_fallback([{'provider':'deepseek','model':'deepseek-v4-pro'}])
    monkeypatch.setattr('agent.external_action_guard.default_store',lambda:Store(tmp_path/'shared.sqlite3'))
    execute=Mock(return_value=json.dumps({'error':'timeout after transmission'}))
    with patch('hermes_cli.config.load_config_readonly',return_value={'external_action_guard':{'enabled':True}}),patch('agent.tool_executor._begin_tool_execution'):
        for call_id in ('codex-call','pro-retry'):
            result=_run_agent_tool_execution_middleware(agent,function_name='terminal',function_args=ARGS,effective_task_id='test-task',tool_call_id=call_id,execute=execute)
    assert execute.call_count==1
    assert json.loads(result.result)['executed'] is False

def test_disabled_profile_does_not_create_state(store):
    fn=Mock(return_value=OK)
    assert guarded_execute('terminal',ARGS,fn,store=store,enabled=False)==OK
    assert not store.path.exists()

def test_whitespace_variation_cannot_evade_pending_action(store):
    call(store,Mock(return_value='timeout'))
    fn=Mock()
    assert json.loads(call(store,fn,{'command':CMD.replace('gh pr create','gh  pr\tcreate')}))['executed'] is False
    fn.assert_not_called()

def test_real_fallback_loop_blocks_uncertain_pr_retry(tmp_path, monkeypatch):
    from tests.agent.test_fallback_429_after_timeout import _make_agent_with_fallback, RateLimitError
    from tests.agent.test_conversation_fallback_state import _response, _tool_call
    from unittest.mock import MagicMock
    agent=_make_agent_with_fallback([{'provider':'deepseek','model':'deepseek-v4-pro'}])
    agent.provider='openai-codex';agent.model='gpt-6-astra'
    agent._primary_runtime.update(provider='openai-codex',model='gpt-6-astra')
    agent.valid_tool_names={'terminal'}
    agent._cached_system_prompt='Offline PR test'
    agent.compression_enabled=False;agent.save_trajectories=False
    monkeypatch.setattr('agent.external_action_guard.default_store',lambda:Store(tmp_path/'actions.sqlite3'))
    calls=[]
    def api(kwargs):
        calls.append(agent.provider)
        if len(calls)==2:raise RateLimitError()
        if len(calls) in (1,3):
            tc=_tool_call('terminal',str(len(calls)))
            tc.function.arguments=json.dumps(ARGS)
            return _response(content='',finish_reason='tool_calls',tool_calls=[tc])
        assert 'external_action_reconciliation_required' in json.dumps(kwargs)
        return _response(content='PR outcome requires reconciliation; independent coding may continue.',finish_reason='stop')
    client=MagicMock(api_key='test-only',base_url='https://api.deepseek.com/v1')
    client._custom_headers=None;client.default_headers=None
    with (
        patch('hermes_cli.config.load_config_readonly',return_value={'external_action_guard':{'enabled':True}}),
        patch.object(agent,'_interruptible_api_call',side_effect=api),
        patch.object(agent,'_persist_session'),patch.object(agent,'_save_trajectory'),patch.object(agent,'_cleanup_task_resources'),
        patch('model_tools.handle_function_call',return_value=json.dumps({'error':'timeout after possible creation'})) as action,
        patch('agent.auxiliary_client.resolve_provider_client',return_value=(client,'deepseek-v4-pro')),
        patch('agent.agent_runtime_helpers.time.sleep'),
        patch('agent.model_metadata.get_model_context_length',return_value=1048576),
    ):
        result=agent.run_conversation('Offline fake PR creation')
    assert result['completed']
    assert action.call_count==1
    assert calls==['openai-codex','openai-codex','deepseek','deepseek']

@pytest.mark.parametrize('profile',['builder','builder-2'])
def test_real_profile_loader_enables_shared_gate(tmp_path,monkeypatch,profile):
    from hermes_constants import set_hermes_home_override,reset_hermes_home_override
    home=tmp_path/'profiles'/profile
    home.mkdir(parents=True)
    (home/'config.yaml').write_text('external_action_guard:\n  enabled: true\n')
    monkeypatch.delenv('HERMES_IGNORE_USER_CONFIG',raising=False)
    shared=Store(tmp_path/'root-actions.sqlite3')
    monkeypatch.setattr('agent.external_action_guard.default_store',lambda:shared)
    token=set_hermes_home_override(home)
    try:
        action=Mock(return_value=json.dumps({'error':'unknown'}))
        guarded_execute('terminal',ARGS,action)
        again=guarded_execute('terminal',ARGS,action)
        assert json.loads(again)['executed'] is False
        assert action.call_count==1
    finally:
        reset_hermes_home_override(token)
