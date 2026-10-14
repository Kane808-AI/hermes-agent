# Kanban Completion Inbox Phase 7: Actionability, diagnostics, and repair

## Status and scope

Phase 7 starts from the independently approved Phase 6 base `68b69fcaf5` (implementation
`126f1f9904`) and its resume checkpoint `ca3fcc121d` on `feat/completion-inbox-phase01`.
This phase is implemented by `addca8269f`.

Phase 7 adds three read-mostly capabilities on top of the existing fenced completion saga:

1. **Actionability classification** is confirmed, not changed. The actionable producer set
   remains exactly `completed`, `blocked`, `gave_up`, `review_requested`, and
   `changes_requested` (`ACTIONABLE_EVENT_KINDS`). Transient crash/timeout/stale/reclaim events
   stay passive while a retry is still pending; heartbeat, claim, progress, and routine status
   are never captured as deliveries. The terminal `gave_up` (retries exhausted) is the one
   actionable dispatcher signal.
2. **Diagnostics** expose the live queue: pending age/count, active delivery leases,
   admitted-but-unprocessed deliveries, dead letters, last success, and last error.
3. **Atlas outcomes** record what Atlas did with a processed completion:
   `verified`, `followup_created`, `user_attention_required`, or `dismissed_with_reason`.
4. **Historical repair begins as a dry run** that enumerates actionable events with no durable
   delivery without mutating anything. It never wakes historical sessions and never replays a
   turn; actual replay remains a later, gated operation that selects individual current tasks.

All producer and consumer defaults remain false. `repair_enabled` is still refused. No runtime
was activated, no live database migration, configuration change, service restart, historical
replay, JEV, hosted egress, push, or merge was performed. AgentVault was not modified.

## What changed

- `hermes_cli/kanban_delivery_schema.py` — delivery schema version 2 → 3. `kanban_deliveries`
  gains nullable `outcome`, `outcome_reason`, and `outcome_at` columns with a CHECK on the
  outcome kind. A new version-2 migration adds those columns and recreates the capture trigger
  for the new schema version; the version-1 upgrade path also adds them so it lands directly on
  version 3. `delivery_schema_version` now accepts every version in `1..3` for migration
  discovery. The producer writer version stays 2 (outcome columns are consumer-side, not part
  of the capture contract).
- `hermes_cli/kanban_delivery.py` — `completion_inbox_diagnostics(conn)` (read-only queue
  rollup) and `completion_inbox_repair_dry_run(conn)` (read-only miss enumeration).
- `hermes_cli/completion_inbox.py` — `OUTCOME_KINDS` and `CompletionInbox.record_outcome`,
  which requires the fenced owner and a committed receipt and refuses an unknown outcome or a
  dismissal without a bounded reason.
- `tests/hermes_cli/test_completion_inbox_phase7.py` — new regression coverage.
- `tests/hermes_cli/test_kanban_delivery_migration.py` — the reviewed v1-upgrade backup name is
  now `before-kanban-delivery-v3.bak` (the migration lands on version 3).

## Verification results

All tests used `scripts/run_tests.sh`, disposable board/profile stores, and `--file-retries 0`
on macOS.

| Verification | Exact result |
| --- | --- |
| Phase 7 file plus delivery, migration, and saga files | **146 passed, 0 failed** (12 new Phase 7 cases, 33 delivery, 8 migration, 93 saga) |
| Broader 19-file kanban/delivery/completion suite | **346 passed, 1 failed, 1 skipped** |
| `scripts/check` | **11 checks passed, 0 blocking, 0 advisory** |
| `git diff --check` (implementation and staged) | **Passed** |

The sole failure on the broader run is the pre-existing
`tests/hermes_cli/test_kanban_db.py::test_infrastructure_spawn_refusal_never_charges_the_card`
(`assert res.auto_blocked == []` receives one generated task ID). It is outside Phase 7 and
matches the documented baseline across Phases 5 and 6. The skipped test is Windows-only.

## Reproduction commands

```bash
scripts/run_tests.sh \
  tests/hermes_cli/test_completion_inbox_phase7.py \
  tests/hermes_cli/test_kanban_delivery.py \
  tests/hermes_cli/test_kanban_delivery_migration.py \
  tests/hermes_cli/test_completion_inbox_saga.py \
  -q --tb=short --file-retries 0 -j 4

scripts/run_tests.sh \
  tests/hermes_cli/test_kanban_delivery.py \
  tests/hermes_cli/test_kanban_delivery_migration.py \
  tests/hermes_cli/test_kanban_delivery_config.py \
  tests/hermes_cli/test_kanban_delivery_review.py \
  tests/hermes_cli/test_sqlite_migration_backup.py \
  tests/hermes_cli/test_completion_inbox_saga.py \
  tests/hermes_cli/test_completion_inbox_phase7.py \
  tests/hermes_cli/test_kanban_db.py \
  tests/hermes_cli/test_kanban_db_init.py \
  tests/hermes_cli/test_kanban_decompose_db.py \
  tests/hermes_cli/test_kanban_creator_origin.py \
  tests/hermes_cli/test_kanban_review_lifecycle.py \
  tests/hermes_cli/test_kanban_write_txn_busy_retry.py \
  tests/hermes_cli/test_kanban_transfer.py \
  tests/hermes_cli/test_kanban_gc_retention.py \
  tests/hermes_cli/test_kanban_task_updated_hook.py \
  tests/hermes_cli/test_kanban_terminal_fencing_contract.py \
  tests/hermes_state/test_kanban_return_routes.py \
  tests/hermes_cli/test_cli_completion_consumer.py \
  -q --tb=short --file-retries 0 -j 6

scripts/check
git diff --check
```

## Remaining limitations

- `repair_enabled` remains refused and no repair loop or automatic replay exists. The dry run
  only reports candidates; replaying even a single historical session is outside this phase and
  remains unauthorized.
- Diagnostics and the dry run are board-side and read-only; they do not expose profile-side
  consumer-lease state.
- All producer and consumer flags remain false by default, so no production completion
  consumption occurs without a separate future activation.
- The plan document `plan-0045009c16e345dd` was not consulted for this phase's implementation;
  its "47 known misses" figure is treated as context only, and the dry run discovers misses
  generically rather than against a hardcoded list.
