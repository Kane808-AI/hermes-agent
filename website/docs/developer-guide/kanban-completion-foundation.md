---
title: Kanban completion foundation
---

Implementation scope: Phases 0 and 1 of approved plan `plan-0045009c16e345dd`.
This foundation captures delivery obligations. It does not execute completion turns.
The separately authorized [Phase 2 saga](kanban-completion-inbox-saga.md) now supplies
explicit native execution. Consumers remain disabled.

## Producer inventory

The runtime inventory was checked across repository Python, SQL, JavaScript, and TypeScript.
All actionable domain transitions use the `kanban_db._append_event` seam, now implemented
in `hermes_cli/kanban_delivery.py`. A board-owned SQLite trigger creates the delivery in the
same transaction as the event, including direct SQL and older writers.

| Writer | Entry points and events | Capture behavior |
| --- | --- | --- |
| `kanban_db.py` | Task creation, claiming, completion, block/unblock, review/request-changes, archive, links, comments, attachments, scheduling, edits | Versioned repository helper. Actionable events are `completed`, `blocked`, `review_requested`, and `changes_requested`. |
| `kanban_db_dispatch.py` | Dispatcher, spawn/reclaim/reaper, timeout/crash, retry breaker | Versioned helper. `gave_up` is actionable. Transient crash/timeout/stale/reclaim events stay passive. |
| `kanban_db_graph.py` | Built-in decomposition and child creation | Versioned helper. The trusted opaque route is inherited before the child's first event. |
| `kanban_db_workspace.py` | Workspace creation/cleanup diagnostics | Versioned helper, passive events. |
| `kanban_pr_acceptance_store.py` | PR acceptance and verification receipts | Versioned helper, passive events. |
| `kanban_swarm.py` | Swarm coordination events | Versioned helper, passive events. |
| `plugins/kanban/dashboard/plugin_api.py` | Direct `edited` and `status` inserts | Passive kinds. Dashboard complete/block/review actions call domain transitions above. A future raw actionable write is captured and reported as an older writer. |
| `kanban_transfer.py` | `imported` event after importing a snapshot | Passive. Export/import scrub routes and pending obligations only in the copied snapshot, preserve the source, and mint a new board identity. |
| `kanban_db_connect.py` | Legacy kind renames and table-shape repair | Migration-only. Delivery setup happens after legacy normalization. No historical outbox backfill. |
| `evals/kanban_graph_identity.py` | Changes event timestamps in an evaluation board | Evaluation-only, not a runtime producer. |

CLI, tools, gateway dispatch, TUI/Desktop, and REST callers reach these repository paths.
Existing `session_id` provenance and legacy notification subscriptions remain compatible.
Neither is used as a new internal return route.

## Storage and opt-in

`kanban_board_identity` has one stable UUID per board. A new board receives a new UUID.
Every delivery has a unique `(board_uuid, event_id, return_route_id)`. Each event remains a
separate obligation even when multiple descendants share the same route.

`SessionDB.create_kanban_return_route` is a native trusted-orchestrator API. It stores the
session, incarnation timestamp, originating surface, and owning state-database path only
in that profile's state database. Workers and delegated children cannot mint or bind
routes. Tool arguments cannot supply `return_route_id` or `origin_interactive`.
`trusted_return_route` scopes repository task creation. Creator/dependency/decomposition
children inherit the immutable route. Ambiguous multiple origins are rejected.
A deleted/recreated session or copied profile database cannot resolve the original route.

An existing task can receive its first route through the explicit native
`bind_task_return_route(conn, task_id)` operation inside `trusted_return_route(state, conn, route_id)`.
The operation accepts no route argument. It revalidates the board and owning profile state
under `BEGIN IMMEDIATE`, rejects a bound/conflicting origin or any prior actionable delivery
obligation, and checks both dependency and creator origins. A connection-local capability
permits exactly the matching NULL-to-route update. The protection trigger stays installed
throughout binding and closes again on success or failure. Subsequent actionable events
use normal atomic capture. No old event is replayed.

Runtime surface capture is Phase 3. The Phase 1 API is exercised only by explicit trusted
callers and tests. It does not infer eligibility from environment session strings.

All `kanban.completion_inbox` flags default to false. Merely opening a board never enables
its producer. Native orchestration explicitly enters `trusted_board_controller(state)` and
calls `configure_producer` with the profile's effective config. The first authorized enable
or disable claims an unowned board. The board stores only an opaque controller UUID. Its
profile-local identity comes from the explicit state database, never tool arguments or
environment claims. Reopens preserve it. Copied/relocated profile stores receive a new
identity. A different controller cannot change any board control state. Workers, delegated
children, and descendant processes cannot use even an inherited trusted scope. Export/import
scrub controller ownership along with routes. The owning controller can disable a board
whose capture trigger has been lost, but enabling it still requires complete capture.
`cli_enabled` remains false by default and enables only the fenced classic interactive CLI
consumer added in Phase 4. `gateway_enabled`, `tui_enabled`, and `repair_enabled` remain
reserved and refused. No gateway fallback, external request, JEV package, or replay path is
installed or enabled.

Capture is forward-only. Enabling it does not generate rows for old events. An actionable
event without a route remains visible in `producer_diagnostics`; it cannot silently pass
the rollout gate. Raw old writers have `producer_version=0`. The scanner reports them,
missing deliveries, missing routes, and a missing capture trigger. `require_producer_ready`
refuses rollout on any violation. It performs no repairs or writes.

Before enabling capture or inserting a native actionable event, readiness checks verify
the required tables, columns, identity/control rows, protection triggers, and capture-trigger
definition. Actionable writes check under the SQLite writer lock, including standalone
native event inserts. An incomplete capture installation rolls back the task transition and
event together. Cached opens refuse enabled incomplete schemas. Migration discovery still
recognizes a genuinely legacy board as not yet migrated.

## Migration and rollback

Board and profile-route extensions have independent version markers. Unknown schema or
minimum-writer versions are refused before migration, including cached board opens.
Existing files receive private SQLite online-backup snapshots that include committed WAL
frames, named `*.before-kanban-delivery-v1.bak` and `*.before-kanban-routes-v1.bak`.
The review follow-up uses board schema/writer version 2. Upgrading the reviewed v1 extension
also creates `*.before-kanban-delivery-v2.bak`, preserves board identity and obligations, and
disables capture until a trusted controller explicitly claims it. The v1 protection triggers
are replaced only inside this schema-upgrade transaction, without a task mutation.
Backup publication fsyncs the containing directory after the create-once hard link, including
a retry that finds an existing final name. Windows cannot open directories through `os.open`.
An explicitly unsupported directory fsync on other filesystems warns and retains the private,
file-synced snapshot with weaker power-loss publication durability. Other I/O errors prevent
migration, including on retry. Snapshots are never overwritten.

New board columns, outbox tables, identity, capture triggers, and version marker commit
under one `BEGIN IMMEDIATE`. Profile route tables and the reserved consumer-lease table
also commit under `BEGIN IMMEDIATE`. Repeated/concurrent initialization preserves identity.
Existing subscription cursors and historical events are not replayed or repurposed.

Rollback calls `configure_producer` with the producer disabled and leaves the additive
schema and all deliveries intact. There is no down migration. Unprocessed and dead-letter
rows cannot be removed by retry count or age. Linked events/tasks cannot be deleted while
deliveries remain, even by a connection with foreign keys disabled. Event GC also preserves
unrouted events marked `delivery_expected=1`, so GC cannot make a previously obligated task
eligible for late binding. Explicit acknowledged-row GC uses `acknowledged_retention_days` (30 by default,
0 disables GC), removing only acknowledged rows and their now-unreferenced events.

## Phase 0 regression locks and Phase 2 boundary

The initial selected legacy notification baseline passed 30 tests on unchanged code.
The first 23 producer contracts failed on base because the delivery API/schema did not
exist, then passed after implementation. Strict expected failures keep later gaps visible:

- Interactive CLI capture is missing, despite a persisted originating CLI session (Phase 3).
- The legacy TUI cursor consumes a completion before a durable linked receipt, at the
  after-claim, after-marker, after-answer, and before-receipt crash boundaries (Phase 6).
- Terminal idle/busy/closing/closed/reopened ownership has no durable monotonic consumer
  fence (Phase 4). Existing liveness leases are not upgraded in this slice.

The [Phase 2 saga](kanban-completion-inbox-saga.md) implements explicit native admission,
linked receipts, fenced transitions, exact reconciliation, and transactional comment
idempotency. Its passing crash tests are separate from the four legacy TUI expected
failures. Consumer lease acquisition, heartbeat, foreground ownership, and surface
integration remain in later phases. A route record without an execution receipt is not
proof that Atlas processed a completion. Consumer activation is still refused.

## Verification receipt

Verified on 2026-10-06 on macOS arm64 with Python 3.14.7, using a disposable
PM-built test environment and isolated Hermes homes. Base and remote `main` were
`f7e12a645492578ada323678950f9bf8f0c6c030`. The main checkout remained unchanged.

The original reviewed commit was `6f074bd903a2e8910cd1181726f4ecd9ace84d29`.
Its relevant run covered 26 files: **283 passed, 17 pre-existing failures,
1 Windows-only skip, and 10 strict expected failures for later phases**. All 57
newly passing foundation contracts passed. The 30-test legacy notification baseline
also remains green. `scripts/check` passed all 11 checks with no code-health blockers
or advisories. `git diff --check` passed.

Use a prepared test interpreter in `HERMES_PYTHON`. The final test command was:

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

### Phase 0/1 review follow-up

Follow-up revision: `4500a0f640f2d15a22e694435cc7a061586c35a5`, directly after the reviewed commit.
The follow-up remains on `feat/completion-inbox-phase01`.

The six foundation files passed **85 tests with no failures**. This run includes the original
delivery, migration, configuration, and profile-route files plus
`tests/hermes_cli/test_kanban_delivery_review.py` and
`tests/hermes_cli/test_sqlite_migration_backup.py`. Coverage includes incomplete capture,
cached opens, rollback, independent and competing controllers, copied profile identity,
export/import scrubbing, one-time binding, inherited worker/delegate denial, conflicting
origins, obligation retention, v1 upgrade, and backup publication/fallback behavior.
The initial review regressions failed on the reviewed implementation. Six capture regression
cases were also reproduced separately using its existing configuration API.

The exact 26-file command above passed **284 tests**, with **17 pre-existing failures,
1 Windows-only skip, and 10 strict expected failures**. An unchanged detached checkout of
the reviewed commit under the same `.hermes/hermes-agent/.worktrees/` layout reproduced
**283 passes and the identical 17 failing test IDs** with the same interpreter and runner.
A temporary checkout outside the Hermes home avoided the 16 home-I/O guard failures, confirming their path
dependence. No new failure was introduced or hidden.

One intervening broad run had **283 passes and 18 failures** because
`test_concurrent_initializers_keep_one_board_identity` also hit an intermittent existing
WAL-preflight race. A real SQLite connection closing between the sidecar's `is_file` and
`os.access` checks reproduced the same false read-only refusal on unchanged reviewed code.
Twenty uninstrumented baseline concurrency probes passed, so the failure is intermittent.
The final unchanged repeat returned to the 17 baseline failures above. This pre-existing
race remains a verification limitation and was not modified in this scoped follow-up.

`scripts/check` passed all **11 checks**, with **0 code-health blockers and 0 advisories**.
`git diff --check` passed. All database verification used temporary stores. No live database
migration, service restart, production configuration change, or Phase 2 implementation occurred.
The follow-up commit contains only the intended fixes, tests, and this receipt.

The failing cases were reproduced on unchanged `main` with the same interpreter and
canonical runner. They were not modified or hidden:

| Existing file | Failures | Evidence |
| --- | --- | --- |
| `tests/hermes_cli/test_kanban_notify.py` | 14 | Importing `gateway.run` reaches dependency-bootstrap `payload_venv`, which probes a real-home parent manifest. The test I/O guard correctly refuses that probe. |
| `tests/tools/test_kanban_tools.py` | 2 | `test_create_tui_subscription_binds_to_live_session_after_compaction_fork` and `test_live_tui_session_key_uses_profile_store` reach the same refused manifest probe through user-message compression imports. |
| `tests/hermes_cli/test_kanban_db.py` | 1 | `test_infrastructure_spawn_refusal_never_charges_the_card` expects an empty `auto_blocked` list but receives the task ID. |

These pre-existing failures remain a verification limitation for subsequent work.
At revision `4500a0f640f2d15a22e694435cc7a061586c35a5`, the four Phase 0/1 review blockers
were resolved and Phase 2 implementation was unblocked. Consumer activation remained blocked by the
unimplemented linked-receipt saga, fencing, reconciliation, and side-effect guards.
Phase 2 was not started. No JEV, hosted egress, gateway fallback, historical replay,
or production-configuration change was introduced.

### Phase 2 verification

The separately authorized Phase 2 implementation and its verification receipt are recorded
in [Kanban Completion Inbox saga](kanban-completion-inbox-saga.md). The receipt compares
the approved base `4500a0f640f2d15a22e694435cc7a061586c35a5` with the Phase 2 revision,
including exact failing test IDs and causes. All consumer flags remain false, activation
remains refused, and no Phase 3 or later integration is included.

Phase 2 implementation: `29b19d1ffbdbfae866729cf03ee63d6720493cc3`.
Verified on 2026-10-07: **146 foundation/saga tests passed** (85 foundation plus 61 new
Phase 2 cases). The documented 26-file command and unchanged approved base both returned
**284 passed, 17 failed, 1 skipped, 10 strict xfailed**, with identical failure IDs and causes.
The 16 affected persistence/effect files and their unchanged base both returned **142 passed,
46 failed, 2 setup errors, 1 skipped**, all failures/errors from the existing home-I/O guard.
An isolated candidate snapshot passed **190 affected tests, with 1 skip**. Red-on-base evidence
and exact commands are in the saga receipt. `python scripts/check` passed all **11 checks**
with **0 code-health blockers and 0 advisories**. Both unstaged and staged whitespace checks
passed. No Phase 2 run encountered the historical WAL-preflight race.

Consumers remain disabled and activation remains refused. The initial Phase 2 implementation
was submitted for independent review, which was not approved. No Phase 3 work, live database migration, service restart, production configuration
change, push, or merge occurred.


### Phase 2 blocking-review follow-up

Follow-up implementation: `fd51556f4b94dc0e264a41520ffc6ad23ea03cd2`, starting from reviewed HEAD
`1693719b355349600f12cfff3018d7eb0b7ebcd7` on `feat/completion-inbox-phase01`.

Publication, tool-plan admission, and effects now require a nonempty current session-turn
holder. Native dispatch requires its bound agent and admitted lease. A new mutation must
match its exact durable sanitized tool-plan slot. Matching committed keys replay once under
live authority. The explicit silent scope suppresses callbacks, stdout/stderr, inherited
worker failure output, metrics, titles, trajectories, request dumps, finalizer effects,
spill storage, remote output writes, automatic fallback, and prompt reconstruction. It
retains ordinary native lease and transcript guards. No response is delivered to a surface.

The original 146-test foundation/saga command now passes **178 tests**, including 32 new
review cases. The isolated affected suite passes **190 tests with 1 skip**, and 10 additional
observer/provider files pass **255 tests**. Candidate and clean approved base both return
**284 passed, 17 failed, 1 skipped, 10 strict xfailed** on the documented 26-file command.
Exact failing IDs and normalized assertion causes match. No new failure or WAL-preflight
race occurred. All **11 checks pass**, and unstaged/staged whitespace checks pass.
The [saga receipt](kanban-completion-inbox-saga.md#review-follow-up-verification-receipt)
records exact commands, focused red evidence, file changes, and remaining limitations.

Phase 2 remains subject to independent review. All consumers remain disabled and activation
is refused. No Phase 3 work, surface delivery, live migration, service restart, production
configuration change, JEV, hosted egress, gateway fallback, historical replay, push, or merge
occurred. Database execution used only disposable test stores.
