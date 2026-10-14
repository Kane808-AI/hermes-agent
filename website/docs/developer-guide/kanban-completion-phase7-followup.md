# Kanban Completion Inbox Phase 7 Follow-Up: dry-run miss definition correction

## Status and scope

Follow-up to the approved Phase 7 base `705d4b8c0e` on `feat/completion-inbox-phase01`.
Implementation: `6af2daeaf5`. This corrects one defect and three small items found in
review, plus the four follow-up findings are unchanged from the approved head otherwise.

## Defect: loose miss definition

`completion_inbox_repair_dry_run` in `hermes_cli/kanban_delivery.py` defined a miss as any
actionable `task_events` row with no `kanban_deliveries` row. That is looser than the
producer's own obligation and counted events the producer could never have captured: it
included `delivery_expected=0` events (pre-enablement history) and events on unrouted tasks.

The corrected definition now matches `producer_diagnostics` (same file): an eligible miss is
an actionable event with `delivery_expected=1` and a bound task return route, with no durable
delivery for that event and route (the join matches on both `event_id` and `return_route_id`).
Unroutable actionable events (no bound route) are reported as a separate `unroutable_count`
and `unroutable` list, never merged into the miss figure.

Both `completion_inbox_repair_dry_run` and `completion_inbox_diagnostics` now guard on
`check_delivery_schema(conn, require_capture=False)`: a board without the delivery extension
returns a structured report (`dry_run=True, extension_installed=False` for the dry run;
`extension_installed=False` for diagnostics) instead of raising
`sqlite3.OperationalError: no such table: kanban_deliveries`. Both remain strictly read-only.

## Small items

- `completion_inbox_diagnostics` no longer ships a misreading `last_error_at` field. There is
  no per-delivery error timestamp, so the field is renamed `last_error_created_at` and a
  comment states it is the creation time of the delivery that most recently recorded an error.
- The v1 -> v3 migration resets `producer_enabled` (it introduces `controller_id`, so capture
  ownership must be re-claimed) while the v2 -> v3 migration preserves it (additive consumer
  columns only). Both branches now document why, and the v2 -> v3 preserve behavior is pinned
  by `test_v2_upgrade_preserves_producer_enabled`; the v1 reset is pinned by the existing
  `test_reviewed_v1_upgrade_preserves_identity_and_obligations_but_requires_controller`.
- `kanban-completion-foundation.md` now names the `*.before-kanban-delivery-v3.bak` snapshot
  the code actually produces.

## Verification

All tests used `scripts/run_tests.sh`, disposable board/profile stores, and `--file-retries 0`.

Red evidence (new cases run against the pre-fix source, 5 failed):

- `test_repair_dry_run_counts_eligible_miss`
- `test_repair_dry_run_reports_unroutable_separately`
- `test_repair_dry_run_excludes_delivered_events`
- `test_repair_dry_run_unmigrated_board_returns_report`
- `test_diagnostics_unmigrated_board_returns_report`

Green evidence (after the fix):

| Verification | Exact result |
| --- | --- |
| Phase 7 file plus delivery, migration, and saga files | **151 passed, 0 failed** (17 Phase 7, 33 delivery, 8 migration, 93 saga) |
| `scripts/check` | **11 checks passed, 0 blocking, 0 advisory** |
| `git diff --check` | **Passed** |

The pre-existing `test_infrastructure_spawn_refusal_never_charges_the_card` failure and the
worktree home I/O guard are outside this change and were not run as part of this follow-up's
focused scope.

## Reproduction commands

```bash
scripts/run_tests.sh \
  tests/hermes_cli/test_completion_inbox_phase7.py \
  tests/hermes_cli/test_kanban_delivery.py \
  tests/hermes_cli/test_kanban_delivery_migration.py \
  tests/hermes_cli/test_completion_inbox_saga.py \
  -q --tb=short --file-retries 0 -j 4

scripts/check
git diff --check
```

## Boundaries unchanged

No flag value changed: `producer_enabled`, `cli_enabled`, `gateway_enabled`, `tui_enabled`,
and `repair_enabled` all remain false by default, and `repair_enabled` keeps refusing. The
exact known-miss enumeration, any operator-visible diagnostics/outcomes surface,
`DELIVERY_SCHEMA_VERSION`, the capture trigger, and every consumer path are untouched. No
activation, live migration, push, or merge.
