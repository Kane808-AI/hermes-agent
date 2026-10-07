---
title: Kanban completion foundation
---

Implementation scope: Phases 0 and 1 of approved plan `plan-0045009c16e345dd`.
This foundation captures delivery obligations. It does not execute completion turns.

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

Runtime surface capture is Phase 3. The Phase 1 API is exercised only by explicit trusted
callers and tests. It does not infer eligibility from environment session strings.

All `kanban.completion_inbox` flags default to false. Merely opening a board never enables
its producer. Its owner explicitly calls `configure_producer` with the profile's effective
config. This avoids competing workers or profiles changing a shared board's capture policy.
`cli_enabled`, `gateway_enabled`, `tui_enabled`, and `repair_enabled` are reserved and refused
in this slice. No new consumer, gateway fallback, external request, JEV package, or replay
path is installed or enabled.

Capture is forward-only. Enabling it does not generate rows for old events. An actionable
event without a route remains visible in `producer_diagnostics`; it cannot silently pass
the rollout gate. Raw old writers have `producer_version=0`. The scanner reports them,
missing deliveries, missing routes, and a missing capture trigger. `require_producer_ready`
refuses rollout on any violation. It performs no repairs or writes.

## Migration and rollback

Board and profile-route extensions have independent version markers. Unknown schema or
minimum-writer versions are refused before migration, including cached board opens.
Existing files receive private SQLite online-backup snapshots that include committed WAL
frames, named `*.before-kanban-delivery-v1.bak` and `*.before-kanban-routes-v1.bak`.
Backup failure prevents migration. Snapshots are never overwritten.

New board columns, outbox tables, identity, capture triggers, and version marker commit
under one `BEGIN IMMEDIATE`. Profile route tables and the reserved consumer-lease table
also commit under `BEGIN IMMEDIATE`. Repeated/concurrent initialization preserves identity.
Existing subscription cursors and historical events are not replayed or repurposed.

Rollback calls `configure_producer` with the producer disabled and leaves the additive
schema and all deliveries intact. There is no down migration. Unprocessed and dead-letter
rows cannot be removed by retry count or age. Linked events/tasks cannot be deleted while
deliveries remain, even by a connection with foreign keys disabled. Event GC skips those
events. Explicit acknowledged-row GC uses `acknowledged_retention_days` (30 by default,
0 disables GC), removing only acknowledged rows and their now-unreferenced events.

## Phase 0 regression locks and Phase 2 boundary

The initial selected legacy notification baseline passed 30 tests on unchanged code.
The first 23 producer contracts failed on base because the delivery API/schema did not
exist, then passed after implementation. Strict expected failures keep later gaps visible:

- Interactive CLI capture is missing, despite a persisted originating CLI session (Phase 3).
- The legacy TUI cursor consumes a completion before a durable linked receipt, at the
  after-claim, after-marker, after-answer, and before-receipt crash boundaries (Phases 2/6).
- Terminal idle/busy/closing/closed/reopened ownership has no durable monotonic consumer
  fence (Phase 4). Existing liveness leases are not upgraded in this slice.

The Phase 2 saga remains unimplemented. Before activating any consumer it must provide
idempotent transcript markers, delivery/turn-linked assistant receipts, fenced CAS
transitions, reconciliation by exact IDs, and side-effect idempotency. Session lease
operations and surface integration follow their separately approved phases. A route
record without an execution receipt is not proof that Atlas processed a completion.

## Verification receipt

Verified on 2026-10-06 on macOS arm64 with Python 3.14.7, using a disposable
PM-built test environment and isolated Hermes homes. Base and remote `main` were
`f7e12a645492578ada323678950f9bf8f0c6c030`. The main checkout remained unchanged.

The final relevant run covered 26 files: **283 passed, 17 pre-existing failures,
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

The failing cases were reproduced on unchanged `main` with the same interpreter and
canonical runner. They were not modified or hidden:

| Existing file | Failures | Evidence |
| --- | --- | --- |
| `tests/hermes_cli/test_kanban_notify.py` | 14 | Importing `gateway.run` reaches dependency-bootstrap `payload_venv`, which probes a real-home parent manifest. The test I/O guard correctly refuses that probe. |
| `tests/tools/test_kanban_tools.py` | 2 | `test_create_tui_subscription_binds_to_live_session_after_compaction_fork` and `test_live_tui_session_key_uses_profile_store` reach the same refused manifest probe through user-message compression imports. |
| `tests/hermes_cli/test_kanban_db.py` | 1 | `test_infrastructure_spawn_refusal_never_charges_the_card` expects an empty `auto_blocked` list but receives the task ID. |

These pre-existing failures remain a verification limitation for subsequent work.
No access blocker for Phase 2 was found. Consumer activation remains blocked by the
unimplemented linked-receipt saga, fencing, reconciliation, and side-effect guards.
Phase 2 was not started. No JEV, hosted egress, gateway fallback, historical replay,
or production-configuration change was introduced.
