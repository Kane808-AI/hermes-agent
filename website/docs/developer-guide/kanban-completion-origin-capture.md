---
title: Kanban completion origin capture
---

Implementation scope: Phase 3 of plan `plan-0045009c16e345dd`.
Approved starting revision: `8ffaa715af707dfbdde67259f7fb440d5fe485d0` on
`feat/completion-inbox-phase01`.
Implementation revision: `40619e6a9e8760ecf80a7d434757633b06cfe4b4`.
Verification date: 2026-10-07, macOS arm64, Python 3.14.7.

Phase 3 captures completion origins only at `kanban_create`. The internal tool executor
passes the live agent object to the registry outside model arguments. Capture derives the
profile store, current persisted session, and current surface from that object. Tool arguments
and ambient session IDs cannot grant routing authority. The task stores only the opaque route
ID. Profile-local storage retains the session coordinate, session incarnation, surface, board
identity, and profile ownership.

Interactive CLI, TUI, and Desktop roots are eligible. One-shot CLI, ACP, cron, Kanban workers,
dispatcher-owned runs, tool sessions, delegated subagents, and side agents cannot mint a route.
An eligible surface without its live state store, session ID, persisted session row, or matching
surface provenance fails visibly before task creation.

Route creation precedes the board transaction. The board task insert, route inheritance, and
post-insert linkage check remain inside one board writer transaction. A route failure creates no
task. A board failure rolls back the task and removes its provisional live route. The immutable
route ownership witness remains so an opaque ID cannot be rebound. An idempotent retry discards
its unused provisional route and accepts only the previously committed task's matching route.
Actionable delivery creation remains a separate event transaction under the Phase 0 through
Phase 2 capture and saga contracts.

No consumer, producer, gateway fallback, repair loop, JEV path, hosted egress, or historical
replay was enabled. No live migration, service restart, production configuration change, push,
or merge occurred.

## Verification results

| Verification | Exact result |
| --- | --- |
| Start gate | Worktree clean and `HEAD` exactly `8ffaa715af707dfbdde67259f7fb440d5fe485d0`. |
| Phase 3 contracts on approved base | **10 passed, 12 failed**. All 12 failures were the intended missing capture, trusted-context propagation, visible rejection, and atomicity behavior. |
| Phase 3 contracts on implementation | **22 passed, 0 failed**. |
| Full foundation and Phase 2 saga suite | **178 passed, 0 failed**. |
| Isolated 21-file affected suite | **302 passed, 0 failed, 1 Windows-only skip**. This includes all **55** Kanban tool tests. |
| Documented 26-file candidate suite | **285 passed, 17 failed, 1 skipped, 9 strict xfailed**. |
| Same 26 files on approved base | **284 passed, 17 failed, 1 skipped, 10 strict xfailed**. |
| `python scripts/check` | **11 checks passed, 0 blockers, 0 advisories**. |
| Unstaged and staged `git diff --check` | **Passed**. |

The candidate and approved base have the same 17 failing test IDs and causes after normalizing
generated task IDs. Fourteen failures in `tests/hermes_cli/test_kanban_notify.py` and two in
`tests/tools/test_kanban_tools.py` are the documented real-home manifest guard. The remaining
`tests/hermes_cli/test_kanban_db.py::test_infrastructure_spawn_refusal_never_charges_the_card`
failure is the documented `auto_blocked` assertion. Phase 3 changes one prior strict expected
failure into a pass, accounting for the candidate's additional pass and reduction from 10 to 9
strict expected failures. There are no new failures. The historical intermittent WAL-preflight
race did not occur.

The Phase 3 tests cover all three permitted surfaces, every prohibited origin class, one-shot
runtime detection, profile and session spoof resistance, missing trusted context, delegated
mutation rejection, registry propagation, route-write failure, rollback after a task row insert,
direct dispatch without runtime authority, and idempotent retry.

## Red evidence

The final tests were copied without source changes into an archive of the exact approved revision
at `/private/tmp/hermes-completion-phase3.1Yq9A9`. The run returned exit 1 with **10 passes and
12 failures**. Three eligible-surface cases had no route. A tool-supplied session won over the
live session. Missing live state was accepted. The route writer was never called. Post-insert
failure had no provisional route to clean up. Idempotent retry had no route. The executor omitted
the live agent context. These are the behaviors implemented by Phase 3.

The archive has no `.git` directory, so the runner's optional bytecode precompile printed
`fatal: not a git repository`. Test collection and execution continued normally. The complete
red output is in
`/private/tmp/hermes-completion-phase3.1Yq9A9/phase3-red-approved-head-final.log`.

## Exact commands

Start gate:

```bash
git status --short
git rev-parse HEAD
```

Red on the exact approved archive:

```bash
HERMES_PYTHON=/private/tmp/hermes-completion-phase01.G1Ybro/test-env/bin/python \
  /private/tmp/hermes-completion-phase3.1Yq9A9/scripts/run_tests.sh \
  /private/tmp/hermes-completion-phase3.1Yq9A9/tests/tools/test_kanban_origin_capture.py \
  /private/tmp/hermes-completion-phase3.1Yq9A9/tests/agent/test_kanban_origin_context.py \
  -q --tb=short --file-retries 0 -j 2
```

Final Phase 3 contracts:

```bash
HERMES_PYTHON=/private/tmp/hermes-completion-phase01.G1Ybro/test-env/bin/python \
scripts/run_tests.sh \
  tests/tools/test_kanban_origin_capture.py \
  tests/agent/test_kanban_origin_context.py \
  -q --tb=short --file-retries 0 -j 2
```

Foundation and Phase 2 saga:

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
  -q --tb=short --file-retries 0 -j 4
```

Affected suites in the isolated candidate at
`/private/tmp/hermes-completion-phase3-candidate.yQRxPW`:

```bash
HERMES_PYTHON=/private/tmp/hermes-completion-phase01.G1Ybro/test-env/bin/python \
scripts/run_tests.sh \
  tests/hermes_state/test_session_turn_lease.py \
  tests/agent/test_cross_process_turn_lease.py \
  tests/agent/test_turn_facade_lease.py \
  tests/agent/test_text_turn_incremental_persistence.py \
  tests/agent/test_tool_call_incremental_persistence.py \
  tests/agent/test_identity_flush.py \
  tests/agent/test_transform_llm_output_persistence.py \
  tests/agent/test_turn_finalizer_final_response_persistence.py \
  tests/agent/test_tool_executor_checkpoint_paths.py \
  tests/agent/test_tool_batch_segmentation.py \
  tests/agent/test_tool_dispatch_helpers.py \
  tests/hermes_cli/test_lifecycle.py \
  tests/hermes_state/test_restore_alternation_repair.py \
  tests/hermes_state/test_repaired_history_commit_coverage.py \
  tests/hermes_state/test_hermes_state_compression_locks.py \
  tests/hermes_state/test_compression_watermark_commit.py \
  tests/tools/test_model_tools.py \
  tests/tools/test_kanban_tools.py \
  tests/tools/test_kanban_origin_capture.py \
  tests/agent/test_kanban_origin_context.py \
  tests/hermes_state/test_kanban_return_routes.py \
  -q --tb=short --file-retries 0 -j 4
```

Candidate and approved-base regression command:

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

The base command ran from a same-layout archive created with:

```bash
phase3_base_dir=$(mktemp -d \
  /Users/chriskaneshiro/.hermes/hermes-agent/.worktrees/completion-inbox-phase3-base.XXXXXX)
git archive 8ffaa715af707dfbdde67259f7fb440d5fe485d0 | tar -x -C "$phase3_base_dir"
```

Repository checks:

```bash
/private/tmp/hermes-completion-phase01.G1Ybro/test-env/bin/python scripts/check
git diff --check
git diff --cached --check
```

Final logs:

- `/private/tmp/hermes-completion-phase3.1Yq9A9/phase3-red-approved-head-final.log`
- `/private/tmp/hermes-completion-phase3-narrow-final.log`
- `/private/tmp/hermes-completion-phase3-foundation.log`
- `/private/tmp/hermes-completion-phase3-affected-isolated.log`
- `/private/tmp/hermes-completion-phase3-regression-candidate.log`
- `/private/tmp/hermes-completion-phase3-regression-base.log`
- `/private/tmp/hermes-completion-phase3-check.log`

## Changed files

Implementation revision:

- `agent/agent_runtime_helpers.py`
- `agent/tool_executor.py`
- `hermes_cli/kanban_origin.py`
- `hermes_state_kanban.py`
- `model_tools.py`
- `tests/agent/test_kanban_origin_context.py`
- `tests/tools/test_kanban_origin_capture.py`
- `tests/tools/test_kanban_tools.py`
- `tools/kanban_tools.py`

The separate receipt revision adds only
`website/docs/developer-guide/kanban-completion-origin-capture.md`.

## Limitations

Profile routes and board tasks live in separate SQLite files. Phase 3 orders the route first,
then commits the verified task atomically in the board transaction, and compensates ordinary
task failures by deleting the provisional live route. A process death after the profile commit
but before the board commit can leave an unreferenced opaque route and its immutable ownership
witness. It cannot leave an eligible task without a resolvable route, create a delivery, or
activate a consumer.

The documented 17 unrelated regression failures remain. The intermittent pre-existing
WAL-preflight race remains possible even though it did not occur here. Archive-based isolated
runs print the benign missing-`.git` precompile message. No live consumer or surface-delivery
end-to-end test was run because consumer activation is outside Phase 3 and remains disabled.
