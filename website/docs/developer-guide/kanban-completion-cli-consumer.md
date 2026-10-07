# Kanban Completion Classic CLI Consumer (Phase 4)

## Status and scope

Phase 4 starts from approved revision
`25db90f21ad52cd75c7aa4ede95c070f713bb4e9` and is implemented by
`c542a76d0419893ce86bbafffa5c86dd6e2c0e35`.

Only the classic interactive CLI can consume completion deliveries in this phase. The
`cli_enabled` flag remains false by default and must be explicitly enabled. One-shot CLI,
ACP, cron, workers, dispatcher runs, tool executions, subagents, gateway, TUI, and Desktop
do not receive consumer authority. The gateway, TUI, and repair flags remain refused.

The CLI binds consumer authority to its exact active-session registry lease, profile state
store, persisted CLI session incarnation, process PID, and process start time. The durable
consumer row assigns a monotonic fence. Renewal requires the exact live authority. Expiry or
reopen advances the fence. Release clears authority while retaining the last fence. A
context-preserving heartbeat renews the fence during a silent completion model turn.

The classic CLI polls while idle and after every user turn. Before staging every local user
message it verifies authority and compares the live compression tip plus active transcript
watermark, transferring ownership to a new compression tip and reloading external transcript
changes when needed. A failed authority check is visible and prevents the local turn.

Completion execution continues to use the Phase 2 saga. It preserves fenced admission,
current-turn authority, exact durable tool-plan matching, atomic linked receipts, explicit
acknowledgement, crash recovery, transactional effect deduplication, and suppression of
completion-execution output. The CLI renders only the committed assistant receipt, then
acknowledges the delivery.

## Verification results

| Verification | Exact result |
| --- | --- |
| Start gate | Worktree clean and `HEAD` exactly `25db90f21ad52cd75c7aa4ede95c070f713bb4e9`. |
| Final Phase 4 contracts on approved base | **1 passed, 24 failed**. The passing one-shot rejection was already enforced. All 24 failures were missing Phase 4 behavior. |
| Final Phase 4 contracts on implementation | **25 passed, 0 failed**. |
| Foundation, saga, and Phase 4 combined suite | **203 passed, 0 failed** on the exact implementation commit. |
| Isolated 14-file affected suite | **192 passed, 0 failed, 4 platform skips**. |
| Documented 26-file candidate suite | **301 passed, 17 failed, 1 skipped, 4 strict xfailed**. |
| Same 26 files on approved base | **285 passed, 17 failed, 1 skipped, 9 strict xfailed**. |
| Supplemental isolated 26-file candidate | **317 passed, 1 failed, 1 skipped, 4 strict xfailed**. The 16 real-home guard failures disappear outside the worktree layout. |
| `python scripts/check` | **11 checks passed, 0 blockers, 0 advisories**. One reviewed BLE001 waiver covers fail-closed heartbeat exception capture. |
| Unstaged and staged `git diff --check` | **Passed**. |

The candidate and approved base have the same 17 failing test IDs. Fourteen failures in
`tests/hermes_cli/test_kanban_notify.py` and two failures in
`tests/tools/test_kanban_tools.py` are the documented real-home manifest guard. The remaining
`tests/hermes_cli/test_kanban_db.py::test_infrastructure_spawn_refusal_never_charges_the_card`
failure is the documented `auto_blocked` assertion. The candidate adds 11 terminal contracts
and turns five prior terminal strict expected failures into passes. This accounts for 16
additional passes and the reduction from nine to four strict expected failures.

The exact shared failure list is:

```text
tests/hermes_cli/test_kanban_db.py::test_infrastructure_spawn_refusal_never_charges_the_card
tests/hermes_cli/test_kanban_notify.py::test_gateway_autosubscribe_roundtrips_user_id_alt_for_session_key[channel-None-False]
tests/hermes_cli/test_kanban_notify.py::test_gateway_autosubscribe_roundtrips_user_id_alt_for_session_key[group-th-42-True]
tests/hermes_cli/test_kanban_notify.py::test_gateway_create_autosubscribes_on_explicit_board
tests/hermes_cli/test_kanban_notify.py::test_notifier_artifact_delivery_skips_missing_files
tests/hermes_cli/test_kanban_notify.py::test_notifier_notify_plus_wake_sends_and_wakes
tests/hermes_cli/test_kanban_notify.py::test_notifier_notify_wake_does_not_wake_on_status_event
tests/hermes_cli/test_kanban_notify.py::test_notifier_plain_notify_never_wakes_even_with_session_id
tests/hermes_cli/test_kanban_notify.py::test_notifier_unsubs_after_abnormal_events[crashed]
tests/hermes_cli/test_kanban_notify.py::test_notifier_unsubs_after_abnormal_events[gave_up]
tests/hermes_cli/test_kanban_notify.py::test_notifier_unsubs_after_abnormal_events[timed_out]
tests/hermes_cli/test_kanban_notify.py::test_notifier_uploads_review_handoff_artifacts
tests/hermes_cli/test_kanban_notify.py::test_notifier_wake_forwards_persisted_chat_type_and_user_id
tests/hermes_cli/test_kanban_notify.py::test_notifier_wake_only_skips_send_and_advances_cursor
tests/hermes_cli/test_kanban_notify.py::test_notifier_wakes_origin_for_review_and_keeps_subscription
tests/tools/test_kanban_tools.py::test_create_tui_subscription_binds_to_live_session_after_compaction_fork
tests/tools/test_kanban_tools.py::test_live_tui_session_key_uses_profile_store
```

The historical WAL-preflight race occurred once during a combined-suite run:
`test_concurrent_initializers_keep_one_board_identity` saw its temporary `racing.db-wal`
between existence and writability probes. The exact rerun passed all 203 tests, and the exact
implementation-commit run also passed all 203 tests. This is the documented intermittent race,
not a new Phase 4 failure.

## Red evidence

The final two Phase 4 test files were copied without implementation changes into an archive of
the exact approved revision at
`/private/tmp/hermes-completion-phase4-red-final.8gGWJx`. The run returned exit 1 with
**1 pass and 24 failures**. Failures showed the CLI flag still refused, no consumer lease API,
no durable fence lifecycle, no transcript refresh or compression transfer, no CLI poll hooks,
and no fenced poll/run/present/ack path.

The archive has no `.git` directory, so the runner's optional precompile printed
`fatal: not a git repository`. Collection and execution continued normally. Complete red
output is in `/private/tmp/hermes-completion-phase4-red-final.log`.

## Exact commands

Start gate:

```bash
git status --short
git rev-parse HEAD
```

Red on the exact approved archive:

```bash
git archive 25db90f21ad52cd75c7aa4ede95c070f713bb4e9 | \
  tar -x -C /private/tmp/hermes-completion-phase4-red-final.8gGWJx
cp tests/hermes_cli/test_kanban_terminal_fencing_contract.py \
  tests/hermes_cli/test_cli_completion_consumer.py \
  /private/tmp/hermes-completion-phase4-red-final.8gGWJx/tests/hermes_cli/
cd /private/tmp/hermes-completion-phase4-red-final.8gGWJx
HERMES_PYTHON=/private/tmp/hermes-completion-phase01.G1Ybro/test-env/bin/python \
scripts/run_tests.sh \
  tests/hermes_cli/test_kanban_terminal_fencing_contract.py \
  tests/hermes_cli/test_cli_completion_consumer.py \
  -q --tb=short --file-retries 0 -j 2
```

Final Phase 4 contracts:

```bash
HERMES_PYTHON=/private/tmp/hermes-completion-phase01.G1Ybro/test-env/bin/python \
scripts/run_tests.sh \
  tests/hermes_cli/test_kanban_terminal_fencing_contract.py \
  tests/hermes_cli/test_cli_completion_consumer.py \
  -q --tb=short --file-retries 0 -j 2
```

Foundation, Phase 2 saga, and Phase 4 contracts:

```bash
HERMES_PYTHON=/private/tmp/hermes-completion-phase01.G1Ybro/test-env/bin/python \
scripts/run_tests.sh \
  tests/hermes_cli/test_kanban_delivery.py \
  tests/hermes_cli/test_kanban_delivery_migration.py \
  tests/hermes_cli/test_kanban_delivery_config.py \
  tests/hermes_state/test_kanban_return_routes.py \
  tests/hermes_cli/test_kanban_delivery_review.py \
  tests/hermes_cli/test_sqlite_migration_backup.py \
  tests/hermes_cli/test_completion_inbox_saga.py \
  tests/hermes_cli/test_kanban_terminal_fencing_contract.py \
  tests/hermes_cli/test_cli_completion_consumer.py \
  -q --tb=short --file-retries 0 -j 4
```

Affected suites in the isolated candidate at
`/private/tmp/hermes-completion-phase4-candidate-final.u3mq3W`:

```bash
HERMES_PYTHON=/private/tmp/hermes-completion-phase01.G1Ybro/test-env/bin/python \
scripts/run_tests.sh \
  tests/hermes_cli/test_kanban_terminal_fencing_contract.py \
  tests/hermes_cli/test_cli_completion_consumer.py \
  tests/hermes_cli/test_active_sessions.py \
  tests/hermes_cli/test_active_session_exclusivity.py \
  tests/hermes_cli/test_cli_active_session_limit.py \
  tests/hermes_cli/test_single_query_session_finalize.py \
  tests/hermes_cli/test_chat_q_exit_clear.py \
  tests/hermes_cli/test_stream_json.py \
  tests/hermes_cli/test_cli_init.py \
  tests/hermes_cli/test_goals.py \
  tests/hermes_cli/test_process_notification_display.py \
  tests/hermes_cli/test_subagent_notification_display.py \
  tests/tools/test_bot_live_owner_delivery.py \
  tests/tui_gateway/test_compute_host_borrowed_lease.py \
  -q --tb=short --file-retries 0 -j 4
```

Candidate and approved-base 26-file regression command:

```bash
HERMES_PYTHON=/private/tmp/hermes-completion-phase01.G1Ybro/test-env/bin/python \
scripts/run_tests.sh \
  tests/hermes_cli/test_kanban_delivery.py \
  tests/hermes_cli/test_kanban_delivery_migration.py \
  tests/hermes_cli/test_kanban_delivery_config.py \
  tests/hermes_state/test_kanban_return_routes.py \
  tests/hermes_cli/test_kanban_terminal_fencing_contract.py \
  tests/tui_gateway/test_kanban_completion_crash_contract.py \
  tests/tools/test_kanban_tools.py \
  tests/hermes_cli/test_kanban_db.py \
  tests/hermes_cli/test_kanban_notify.py \
  tests/hermes_cli/test_kanban_db_init.py \
  tests/hermes_cli/test_kanban_decompose_db.py \
  tests/hermes_cli/test_kanban_creator_origin.py \
  tests/hermes_cli/test_kanban_review_lifecycle.py \
  tests/hermes_cli/test_kanban_write_txn_busy_retry.py \
  tests/hermes_cli/test_kanban_transfer.py \
  tests/hermes_cli/test_kanban_gc_retention.py \
  tests/hermes_cli/test_kanban_task_updated_hook.py \
  tests/plugins/test_kanban_dashboard_task_updated_hook.py \
  tests/tools/test_kanban_unknown_arguments.py \
  tests/hermes_state/test_schema_read_probe.py \
  tests/hermes_state/test_dedupe_migration_contention.py \
  tests/gateway/test_kanban_wake_acceptance.py \
  tests/gateway/test_kanban_changes_requested_notifier.py \
  tests/gateway/test_kanban_notifier_wake_only_ordering.py \
  tests/gateway/test_kanban_route_anchor_capture.py \
  tests/tui_gateway/test_kanban_notify_poller.py \
  -q --tb=short --file-retries 0 -j 4
```

The base used the exact approved archive in the same worktree layout:

```bash
phase4_base_dir=$(mktemp -d \
  /Users/chriskaneshiro/.hermes/hermes-agent/.worktrees/completion-inbox-phase4-base.XXXXXX)
git archive 25db90f21ad52cd75c7aa4ede95c070f713bb4e9 | \
  tar -x -C "$phase4_base_dir"
```

Repository checks:

```bash
/private/tmp/hermes-completion-phase01.G1Ybro/test-env/bin/python scripts/check
git diff --check
git diff --cached --check
```

Final logs:

- `/private/tmp/hermes-completion-phase4-red-final.log`
- `/private/tmp/hermes-completion-phase4-narrow-final.log`
- `/private/tmp/hermes-completion-phase4-foundation-final-exact-commit.log`
- `/private/tmp/hermes-completion-phase4-affected-isolated-final.log`
- `/private/tmp/hermes-completion-phase4-regression-candidate-final-home.log`
- `/private/tmp/hermes-completion-phase4-regression-base-final.log`
- `/private/tmp/hermes-completion-phase4-check-final.log`

## Changed files

Implementation revision:

- `hermes_cli/active_sessions.py`
- `hermes_cli/cli_chat_turn_mixin.py`
- `hermes_cli/cli_completion_consumer.py`
- `hermes_cli/cli_tui_runtime_mixin.py`
- `hermes_cli/completion_inbox.py`
- `hermes_cli/config_defaults.py`
- `hermes_cli/kanban_delivery.py`
- `hermes_state_kanban.py`
- `tests/hermes_cli/test_cli_completion_consumer.py`
- `tests/hermes_cli/test_kanban_delivery.py`
- `tests/hermes_cli/test_kanban_terminal_fencing_contract.py`
- `website/docs/developer-guide/kanban-completion-foundation.md`

The separate receipt revision adds only
`website/docs/developer-guide/kanban-completion-cli-consumer.md`.

## Limitations

- The consumer discovers existing default and named local board stores. It does not create a
  board, replay historical events, or scan hosted stores.
- Each poll consumes at most one actionable delivery. The two-second idle cadence and forced
  post-turn poll bound prompt-loop work without adding a dispatcher.
- Rendering occurs before explicit acknowledgement. A crash after successful display and before
  acknowledgement can display the same committed receipt again. The Phase 2 receipt prevents
  repeated inference and tool side effects.
- If active ownership or heartbeat renewal is lost during a model turn, the operation fails
  closed. The delivery remains retryable, and any already committed Phase 2 receipt is recovered
  without re-running inference.

No TUI, Desktop, gateway, repair, JEV, hosted egress, or historical replay consumer was added.
No live database migration, service restart, production configuration change, push, or merge
occurred. All database execution used disposable test stores. Phase 5 has not started.
