# Kanban Completion TUI and Desktop Consumer (Phase 6)

## Status and scope

Implementation: `126f1f990453a4685cfa89e00e086c04dcb6fdbe`.
Approved Phase 5 base: `e26b6c58127956aa17d7bb280f924beb5c70a3a8`.
Plan: [Completion Inbox plan](https://plan.agent-native.com/plans/plan-0045009c16e345dd).
This verification receipt is committed separately from the implementation.

Phase 6 replaces claim-before-memory-buffer delivery for enrolled TUI and Desktop completions
with the existing fenced completion saga. The separate `tui_enabled` opt-in controls both
surfaces. `producer_enabled`, `cli_enabled`, `gateway_enabled`, `tui_enabled`, and
`repair_enabled` remain false by default. Repair remains unsupported.

No runtime was activated. No live database migration, configuration change, service restart,
history replay, repair loop, JEV, hosted egress, or later-phase implementation was performed.
AgentVault was not modified. No push or merge was performed.

## Delivery and ownership behavior

The adapter requires an already-owned foreground lease. An idle viewer, inert isolated-child
token, released lease, wrong profile, stale process incarnation, or superseded UI runtime
cannot acquire completion authority. The foreground registry, owning state path, source,
session incarnation, PID/start time, consumer fence, and `live_session_id` are checked together.
The existing per-turn lease and marker, receipt, side-effect, and acknowledgement fences remain
in force. Foreground exclusion in the gateway remains unchanged.

Polling admits at most one completion and respects local user turns, queued input, Stop,
auto-continuation, and closing sessions. The existing notification poller serves warm agents.
Isolated parents use the same cadence and a context-preserving worker, keeping the compute-host
output reader free. Empty polls do not renew idle activity. The durable consumer detaches after
the poll while the foreground slot remains owned.

Completion execution uses a temporary native agent under output suppression. It restores the
persisted session prompt, runtime selection, and saved tool-schema pin. The resident agent never
holds staged completion output that an ordinary exit flush could publish outside the receipt
transaction. Cleanup releases provider clients without ending the shared session. Already
committed receipts recover without constructing an agent or rerunning inference or effects.

The UI receives the existing Kanban status formatting followed by only the exact committed
assistant response and its persisted row IDs. A committed delta preserves separate bubbles for
identical consecutive answers. Internal markers stay hidden in JSON-RPC and REST history, while
committed answers remain visible. Replay buffering and durable hydration cover disconnects.
Acknowledgement retries do not repeat inference, side effects, or already-presented answers,
including responses already hydrated after resume.

Before local input is staged, the owning profile refreshes the canonical compression tip,
foreground lease, and durable history. A reused isolated child refreshes the same history from
its pinned store using its parent's borrowed token, without acquiring consumer authority.
Stop targets the active completion agent. Close revokes publication authority before teardown
and uses the existing bounded grace period even if provider interruption blocks.

## Regression and exact snapshot provenance

Artifacts are under:

```text
/private/tmp/hermes-completion-phase01.G1Ybro/phase6.OWmwSG/
```

The regression is
`tests/tui_gateway/test_completion_inbox_consumer.py::test_ui_completion_runs_once_and_pushes_only_exact_committed_receipt`,
parametrized for `tui` and `desktop`. It enters through the existing notification poll path,
uses real temporary board/state stores, foreground and turn leases, the native agent loop, and
the durable saga with a deterministic model double. It requires one side effect, an exact linked
receipt, output only after commit, explicit acknowledgement, and no second-poll execution.

`base/` was extracted from the exact approved base and overlaid with the final consumer test
file and an unused authority test file. The red selector runs only the two regression cases,
which import the existing notification seam and do not import the new adapter. Both fail with
`Failed: used legacy in-memory dispatch` from `_notif_submit`. This is a behavioral failure,
not a missing-module collection failure.

`baseline/` is a clean exact-base archive without new tests. `release-candidate/` was extracted
directly from the frozen staged tree. No source overlay was applied to that candidate after
extraction. The implementation commit has exactly that tree:
`6847d574c6a7ad1276fb3e5928dd91a93f255426`.
All 25 implementation file blobs were checked against the candidate. Their existing
counterparts in both base snapshots were also checked against the approved base.

| Artifact | SHA-256 |
| --- | --- |
| `approved-base.tar` | `7d736c1a4871831f8b7925fb5e39d1e1a2295b84bc5501119e1f28fa196ef266` |
| `release.tar` | `9160ebad1d8d81d5017ac7febea1417d797fe9380332e36c445bfaf345e8726c` |
| Final consumer regression test file | `bdac6b11d6faa2c81e444ef4b7af59244658c510dcc60496559ba870df62a50e` |
| `affected-files.txt` | `faed3619a05b107543027d9e82b291bb4c6bc3b6cd8d6d1561d0fcca48df680c` |
| `affected-base-files.txt` | `8fbd6643cc7f5ab573bbd67f604527dab69c9e7a601157aa94ccb2bd9402650f` |

Archive runs print `fatal: not a git repository` during optional bytecode precompilation.
Collection and execution continue normally. The isolated archives avoid the pre-existing
real-home manifest guards associated with this worktree's location beneath `.hermes`.

## Verification results

All tests used `scripts/run_tests.sh`, the required Python interpreter below, disposable stores,
and `--file-retries 0` on macOS. Results below are from the final candidate unless marked base.

| Verification | Exact result | Log |
| --- | --- | --- |
| Two regression cases on exact base plus test overlay | **0 passed, 2 failed, 16 deselected**, exit 1 | `red-final.log` |
| Same regression on final candidate | **2 passed, 0 failed**, exit 0 | `green-release.log` |
| Seven focused Phase 6 files | **120 passed, 0 failed**, exit 0 | `focused-release.log` |
| Entire TUI gateway suite, 276 files | **2,514 passed, 0 failed, 11 skipped**, exit 0 | `tui-release.log` |
| Shared affected suite, 39 files | **594 passed, 1 failed, 1 skipped**, exit 1 | `affected-release.log` |
| Same existing 38 files on clean approved base | **550 passed, 1 failed, 1 skipped**, exit 1 | `affected-base.log` |
| Phase 5 fallback file within affected suite | **14 passed, 0 failed** | `affected-release.log` |
| Implementation and receipt staged repository checks | **11 passed in each run, 0 health blockers, 0 advisories**, exit 0 | `check-release.log`, `check-receipt.log` |
| Implementation-range, unstaged, and staged whitespace checks | **Passed** | Git checks before each commit |

The 44 additional shared-suite passes are the new foreground authority contracts. The sole
failure on both base and candidate is
`tests/hermes_cli/test_kanban_db.py::test_infrastructure_spawn_refusal_never_charges_the_card`.
At line 578, `assert res.auto_blocked == []` receives one generated task ID. The failure IDs
and assertion sections match after normalizing that ID, recorded in `baseline-comparison.txt`.
This failure is pre-existing and outside Phase 6. The intermittent WAL-preflight test
`test_concurrent_initializers_keep_one_board_identity` passed in both affected runs.

The TUI suite includes the formerly expected-failing crash contracts, now passing for both
surfaces at claim, marker commit, receipt commit, and profile transaction rollback boundaries.
It also covers isolated-child rehydration, stale-runtime takeover, profile pinning, Stop/close,
queued-input priority, compression, replay/hydration, receipt-only retry, default-off behavior,
legacy notification coexistence, and staged-output isolation from ordinary finalization.

Platform skips remain skips. No Linux or Windows runtime result is claimed. No new failure or
remaining implementation blocker was found in the verified scope.

## Reproduction commands

Use this interpreter for every test invocation:

```bash
export HERMES_PYTHON=/private/tmp/hermes-completion-phase01.G1Ybro/test-env/bin/python
```

Run the regression from `base/` with its test overlay, then from `release-candidate/`:

```bash
scripts/run_tests.sh \
  tests/tui_gateway/test_completion_inbox_consumer.py::test_ui_completion_runs_once_and_pushes_only_exact_committed_receipt \
  -q --tb=short --file-retries 0
```

The runner converts the node selector to a file-level `-k` filter. Its green summary reports
the two passing cases without a deselection count.

From `release-candidate/`:

```bash
scripts/run_tests.sh \
  tests/hermes_cli/test_tui_completion_authority.py \
  tests/tui_gateway/test_completion_inbox_consumer.py \
  tests/tui_gateway/test_completion_inbox_lifecycle.py \
  tests/tui_gateway/test_completion_inbox_compute_rehydrate.py \
  tests/tui_gateway/test_completion_inbox_interrupt.py \
  tests/tui_gateway/test_completion_inbox_presentation.py \
  tests/tui_gateway/test_kanban_completion_crash_contract.py \
  -q --tb=short --file-retries 0 -j 4

scripts/run_tests.sh tests/tui_gateway -q --tb=short --file-retries 0 -j 6
scripts/run_tests.sh $(cat ../affected-files.txt) -q --tb=short --file-retries 0 -j 6
```

From `baseline/`:

```bash
scripts/run_tests.sh $(cat ../affected-base-files.txt) -q --tb=short --file-retries 0 -j 4
```

The affected manifest is below. The base manifest omits only
`tests/hermes_cli/test_tui_completion_authority.py`, which does not exist on the base.

```text
tests/agent/test_kanban_origin_context.py
tests/gateway/test_kanban_changes_requested_notifier.py
tests/gateway/test_kanban_completion_fallback.py
tests/gateway/test_kanban_multiplex_served_profile_parity.py
tests/gateway/test_kanban_notifier_wake_only_ordering.py
tests/gateway/test_kanban_route_anchor_capture.py
tests/gateway/test_kanban_wake_acceptance.py
tests/gateway/test_turn_lease.py
tests/hermes_cli/test_active_session_exclusivity.py
tests/hermes_cli/test_active_sessions.py
tests/hermes_cli/test_cli_active_session_limit.py
tests/hermes_cli/test_cli_completion_consumer.py
tests/hermes_cli/test_completion_inbox_saga.py
tests/hermes_cli/test_kanban_creator_origin.py
tests/hermes_cli/test_kanban_db.py
tests/hermes_cli/test_kanban_db_init.py
tests/hermes_cli/test_kanban_decompose_db.py
tests/hermes_cli/test_kanban_delivery.py
tests/hermes_cli/test_kanban_delivery_config.py
tests/hermes_cli/test_kanban_delivery_migration.py
tests/hermes_cli/test_kanban_delivery_review.py
tests/hermes_cli/test_kanban_gc_retention.py
tests/hermes_cli/test_kanban_notify.py
tests/hermes_cli/test_kanban_review_lifecycle.py
tests/hermes_cli/test_kanban_task_updated_hook.py
tests/hermes_cli/test_kanban_terminal_fencing_contract.py
tests/hermes_cli/test_kanban_transfer.py
tests/hermes_cli/test_kanban_write_txn_busy_retry.py
tests/hermes_cli/test_sessions_rest_scaffold_notices.py
tests/hermes_cli/test_sqlite_migration_backup.py
tests/hermes_cli/test_tui_completion_authority.py
tests/hermes_state/test_dedupe_migration_contention.py
tests/hermes_state/test_kanban_return_routes.py
tests/hermes_state/test_schema_read_probe.py
tests/hermes_state/test_session_turn_lease.py
tests/plugins/test_kanban_dashboard_task_updated_hook.py
tests/tools/test_kanban_origin_capture.py
tests/tools/test_kanban_tools.py
tests/tools/test_kanban_unknown_arguments.py
```

Repository checks from the worktree:

```bash
/private/tmp/hermes-completion-phase01.G1Ybro/test-env/bin/python scripts/check --staged
git diff --check
git diff --cached --check
```

The checker used the repository's default comparison with `origin/main` at merge-base
`f7e12a645492`, covering 81 changed files across the accumulated phases. Its accepted broad
exception comments at the new UI boundaries explain that notification errors preserve the
durable retry and local admission errors fail closed before staging user input.

## Evidence hashes

| Log | SHA-256 |
| --- | --- |
| `red-final.log` | `57f65950e99219f869117c64f6ca5cee042eda5c95f2634aece46256a935bf71` |
| `green-release.log` | `b4a6c67521e4cabf6b4d14afac4161c8f13f98bb26fd3205ab139f9c4e1c0747` |
| `focused-release.log` | `063116bfc8e36de7b375fc5f07cd7f845a0099e9346e0c139a01b582ef58227e` |
| `tui-release.log` | `acfab98e7e1603a590de7b48de0c367dfd597e772eb064d5237645cff4c945be` |
| `affected-release.log` | `00b4daf737e8672ba13f05c90e485a1eb4f3c3e237aa770e096d1dbd41fb929e` |
| `affected-base.log` | `94fcceae4ec0ae35877b74461014dfa9a4e8e9a411f5aeb3c9d5bdce5e858f4a` |
| `check-release.log` | `067618c14e3f80bf663edfb853636c9195b7af6fbc531651ea8a19a6fcaf6b32` |
| `baseline-comparison.txt` | `c2ef0b61f8d8613c95c2a1041b9c51f817d2fe5ffe18f926fa9eb928e2b8d7c2` |
