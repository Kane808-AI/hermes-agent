# Kanban Completion Gateway Fallback (Phase 5)

## Status and scope

Phase 5 starts from independently approved revision
`59ee60ac1ce2915bd2efb960dfe6a913614abf33` and is implemented by
`0dea60bb7111593588f2fa689e27cd28a427ce85`.

This phase adds the gateway as an opt-in fallback completion consumer. The gateway claims a
routed delivery only when no valid foreground CLI, TUI, or Desktop active-session lease exists
anywhere on the route's canonical compression lineage. It derives the immutable route and live
compression tip from the owning profile's durable state, acquires both the durable
session-consumer fence and the existing per-turn lease, and executes the completion turn inside
that profile's runtime scope.

The foreground registry guard is rechecked before authority binding and before every locked
marker, receipt, effect, and acknowledgement transition. A newly valid foreground owner therefore
prevents a stale gateway process from committing any observable completion state. Durable gateway
renewal additionally requires the exact profile, route session and incarnation, live tip and
incarnation, lease owner, PID, process start time, and consumer fence.

The Phase 2 and Phase 4 invariants remain in force: exact linked receipts, crash recovery,
explicit acknowledgement, transactional side-effect idempotency, current-turn authority,
canonical compression continuity, and completion-output suppression. The gateway can recover an
already committed receipt without rerunning inference or effects. It does not render a second
external completion surface.

`producer_enabled`, `cli_enabled`, `gateway_enabled`, `tui_enabled`, and `repair_enabled` all
remain false by default. TUI and Desktop foreground consumers, JEV, hosted egress, historical
replay, repair loops, production activation, live migration, service restart, configuration
changes, push, merge, and Phase 6 are outside this change.

## Behavior contracts

The Phase 5 contract file covers:

1. Gateway flag acceptance while later consumer modes remain refused.
2. Trusted route-to-tip discovery inside the owning profile.
3. Refusal while any valid foreground lineage lease exists, including compression ancestors.
4. Profile pinning and fail-closed registry mismatch handling.
5. Foreground takeover fencing marker, receipt, and effect commits.
6. One real gateway execution on the live tip, exact receipt commit, explicit acknowledgement,
   and no second-poll inference or effects.
7. Recovery of an already committed receipt without rerunning inference.
8. Disabled-by-default behavior.

## Red evidence

The red directory `/private/tmp/hermes-completion-phase5-red.fn4IFr` was created from a direct
archive of the approved base. Only
`tests/gateway/test_kanban_completion_fallback.py` was copied into it. The valid base subset avoids
importing the new gateway module and proves the missing flag, claim, and fence behavior:

```bash
git archive 59ee60ac1ce2915bd2efb960dfe6a913614abf33 | \
  tar -x -C /private/tmp/hermes-completion-phase5-red.fn4IFr
cp tests/gateway/test_kanban_completion_fallback.py \
  /private/tmp/hermes-completion-phase5-red.fn4IFr/tests/gateway/

HERMES_PYTHON=/private/tmp/hermes-completion-phase01.G1Ybro/test-env/bin/python \
scripts/run_tests.sh tests/gateway/test_kanban_completion_fallback.py \
  -q --tb=short --file-retries 0 -j 1 \
  -k 'gateway_flag or gateway_claim_derives or gateway_never_claims or profile_pinned or fences_gateway'
```

Result: **10 failed, 0 passed, 3 deselected**. The failures were the base policy rejecting
`gateway_enabled` and the absence of gateway consumer acquisition and foreground-fencing APIs.
The log is `/private/tmp/hermes-completion-phase5-red-valid.log`.

An attempted full red-file run is not valid evidence. The shared virtual environment's editable
installation resolved the missing gateway module from the live worktree, and the test IO guard
then aborted collection. The filtered run above is isolated from that fallback and is the recorded
red proof.

## Exact candidate and base provenance

The final candidate directory
`/private/tmp/hermes-completion-phase5-final-candidate.A9DoEA` was extracted directly from
`git archive 0dea60bb7111593588f2fa689e27cd28a427ce85`. Its archive SHA-256 is
`5158b2a3d66d9eec7680499414a8e516a3b2392d533c5239258d80cde90345be`.

The comparison directory `/private/tmp/hermes-completion-phase5-base.uUsl9r` was extracted
directly from `git archive 59ee60ac1ce2915bd2efb960dfe6a913614abf33`. Its archive SHA-256 is
`b74068e171388d34c3ef9b2ac3f68e31c192d223851e3beb7ea3df46fe7154c6`.

No files were copied or overlaid after either exact extraction. Both archives intentionally lack
`.git`, so the optional precompile printed `fatal: not a git repository`; test collection and
execution continued normally.

## Verification results

| Verification | Exact result |
| --- | --- |
| Phase 5 contracts | **13 passed, 0 failed** in 9.2 seconds. |
| Combined foundation, saga, Phase 4, and Phase 5 suite | **224 passed, 0 failed** in 31.6 seconds. |
| Byte-exact affected suite | **235 passed, 0 failed** in 34.9 seconds. |
| 26-file candidate regression | **320 passed, 1 failed, 1 skipped, 4 strict xfailed** in 24.5 seconds. |
| Same 26 files on the approved base | **320 passed, 1 failed, 1 skipped, 4 strict xfailed** in 34.3 seconds. |
| Candidate/base failure-ID comparison | **Identical single failure ID; no new failure.** |
| `scripts/check` | **11 checks passed, 0 blockers, 0 advisories**. |
| Committed-range, unstaged, and staged whitespace checks | **Passed**. |

The shared regression failure is
`tests/hermes_cli/test_kanban_db.py::test_infrastructure_spawn_refusal_never_charges_the_card`,
the documented `auto_blocked` assertion. The prior worktree-layout comparison reported 17
failures because 16 real-home manifest guards fail in that layout. Both exact archive runs clear
those 16 and retain only the shared assertion above. The documented intermittent WAL-preflight
race did not occur in either final comparison run.

## Exact verification commands

All test commands below were run from the exact candidate archive with
`HERMES_PYTHON=/private/tmp/hermes-completion-phase01.G1Ybro/test-env/bin/python` and disposable
stores provided by `scripts/run_tests.sh`.

Phase 5 contracts:

```bash
scripts/run_tests.sh \
  tests/gateway/test_kanban_completion_fallback.py \
  -q --tb=short --file-retries 0 -j 1
```

Combined foundation, saga, Phase 4, and Phase 5 suite:

```bash
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
  tests/gateway/test_kanban_completion_fallback.py \
  -q --tb=short --file-retries 0 -j 4
```

Affected suite:

```bash
scripts/run_tests.sh \
  tests/gateway/test_kanban_completion_fallback.py \
  tests/gateway/test_turn_lease.py \
  tests/gateway/test_kanban_multiplex_served_profile_parity.py \
  tests/hermes_cli/test_active_sessions.py \
  tests/hermes_cli/test_active_session_exclusivity.py \
  tests/hermes_cli/test_cli_active_session_limit.py \
  tests/hermes_cli/test_cli_completion_consumer.py \
  tests/hermes_cli/test_completion_inbox_saga.py \
  tests/hermes_cli/test_kanban_delivery.py \
  tests/hermes_cli/test_kanban_delivery_config.py \
  tests/hermes_cli/test_kanban_terminal_fencing_contract.py \
  tests/hermes_state/test_kanban_return_routes.py \
  -q --tb=short --file-retries 0 -j 4
```

Candidate and approved-base regression comparison, run once from each exact archive:

```bash
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

Failure comparison and repository checks:

```bash
diff -u \
  <(grep '^FAILED ' /private/tmp/hermes-completion-phase5-regression-base.log | sort -u) \
  <(grep '^FAILED ' /private/tmp/hermes-completion-phase5-final-regression-candidate.log | sort -u)
scripts/check
git diff --check 59ee60ac1ce2915bd2efb960dfe6a913614abf33..0dea60bb7111593588f2fa689e27cd28a427ce85
git diff --check
git diff --cached --check
```

Verification logs:

- `/private/tmp/hermes-completion-phase5-final-narrow.log`
- `/private/tmp/hermes-completion-phase5-final-combined.log`
- `/private/tmp/hermes-completion-phase5-final-affected.log`
- `/private/tmp/hermes-completion-phase5-final-regression-candidate.log`
- `/private/tmp/hermes-completion-phase5-regression-base.log`
- `/private/tmp/hermes-completion-phase5-final-check.log`

## Implementation inventory

The implementation commit changes:

- `gateway/kanban_completion_fallback.py`
- `gateway/kanban_watchers.py`
- `gateway/run_startup.py`
- `hermes_cli/active_sessions.py`
- `hermes_cli/completion_inbox.py`
- `hermes_cli/config_defaults.py`
- `hermes_cli/kanban_delivery.py`
- `hermes_state_kanban.py`
- `tests/gateway/test_kanban_completion_fallback.py`
- `tests/hermes_cli/test_cli_completion_consumer.py`
- `tests/hermes_cli/test_kanban_delivery.py`

This verification receipt is the only file in the separate receipt commit.

## Remaining limitations

- All producer and consumer flags remain disabled by default, so this code performs no production
  completion consumption without an explicit future activation.
- The watcher handles at most one eligible delivery per served profile on each five-second tick.
- Phase 6 foreground TUI and Desktop consumers are absent. A gateway-written receipt is available
  in the live tip transcript when that surface resumes, but this phase adds no live surface push.
- The existing classic CLI render-before-ack duplicate-display crash window remains documented.
- Windows-only paths were not executed on this macOS verification host.
