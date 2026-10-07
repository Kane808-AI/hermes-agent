---
title: Kanban Completion Inbox saga
---

Implementation scope: Phase 2 of plan `plan-0045009c16e345dd`, starting from
`4500a0f640f2d15a22e694435cc7a061586c35a5` on `feat/completion-inbox-phase01`.
Initial implementation revision: `29b19d1ffbdbfae866729cf03ee63d6720493cc3`.
Independent review of Phase 2 is not approved. The follow-up below fixes its two blocking
findings and remains subject to independent review before any later phase.
This is an explicit native seam. No consumer is started or enabled.

## Durable identities and authority

`hermes_cli.completion_inbox.CompletionInbox` advances:

```text
pending -> leased -> transcript_admitted -> processed -> acknowledged
```

Native orchestration supplies an explicit board, owning `SessionDB`, and opaque route.
Authority comes from the existing durable `kanban_session_consumers` row, including its
consumer fence, lease owner, expiry, PID, process start identity, and session incarnation.
The API accepts no fence or ownership claim from its caller. It does not acquire or renew
consumer ownership. Tests provision reserved records directly in disposable stores.
Workers, delegated contexts, absent/expired/superseded authority, wrong process owners,
cross-board routes, copied profile coordinates, and recreated sessions fail closed.

Every consumer write obtains the profile writer lock before the board writer lock,
revalidates ownership in that transaction, and checks the expected delivery state and owner.
Native tool threads open a separate connection to the pinned board file. Board identity
and schema versions are checked under the writer lock. Existing session-turn and transcript
write guards still apply. A stable logical turn ID has a unique holder for each physical
execution attempt, preventing two native agents in one process from sharing a turn lease.

Additive profile storage records exact board, route, delivery, session incarnation, turn,
marker row/UID, response row/UID, and publication fence. An immutable profile-local route
ownership witness survives removal of the live route. Machine-local coordinates remain
in the owning profile. Board storage adds operation keys and mutation results.
Each ledger and its version marker are created atomically after a private online backup.
Incomplete or unknown ledger versions are refused. Offline export/import snapshots scrub
the board effect ledger with transferred delivery state. The source remains untouched.

## Admission, execution, and publication

Admission appends one durable marker keyed by `(board_uuid, delivery_id)`. Its turn ID is
stable across retries. A subsequent board CAS records that exact turn and marker ID.
Repeated admission returns the same marker. Per-session serialization retains an unfinished
admission until it has a committed response or has been explicitly retired.

The marker contains structured task/event identities and a bounded, redacted worker payload
labelled untrusted evidence. It is appended to the existing conversation. Earlier transcript
rows are neither relabelled nor rewritten, and the cached system prompt is reused.

`agent.completion_inbox_execution` scopes the ordinary native turn facade. It requires
the owning store, exact marker history, existing cached system prompt, native chat completion
runtime, and ordinary session-turn admission. Automatic compression, memory effects,
micro-compaction, background review, custom context engines, MoA, and registered plugin
hooks/middleware are refused. The completion-only native seam retains admission, renewal,
release, and transcript guards while omitting ordinary surface/Relay/metrics scopes.
Automatic fallback and agents already on fallback are refused. Existing full prompt bytes
are reused without reconstructing missing static tiers. Lifecycle observers, MCP refresh,
and every automatic title path are suppressed inside this explicit scope.

Tool-loop transcript output stays staged. Before a permitted mutation, the profile
transaction records the ordered safe tool plan and verifies current consumer and turn
authority. Publication, plan admission, and effects always require a nonempty, current
session-turn holder in their write transaction. Dispatch and flush also require the bound
native agent and its admitted holder. A new mutation requires the exact sanitized fingerprint
at its durable operation slot. An already committed matching key can replay under live
authority even if the plan is absent. Terminal publication commits fresh staged transcript rows, the assistant response,
and its linked receipt together in one profile transaction. Borrowed row IDs or message UIDs
cannot turn an unrelated committed response into a receipt. Repeated publication returns
the original committed identities.

Only a verified response receipt permits the board's processed CAS. Acknowledgement is a
separate explicit operation and verifies that receipt again. A route, in-memory model result,
unrelated assistant row, textual similarity, or transcript position never proves processing.

## Recovery, retries, and effects

Each cross-store gap is reconciled by exact durable identities. A marker committed before
board admission is reused. A receipt committed before the processed CAS completes board
reconciliation without inference. A competing native execution rechecks for a committed
receipt after waiting for its turn lease and skips inference if the first attempt finished.
Expired delivery claims retain admitted identities and receipts while advancing durable
attempt counts. Retrying preserves the obligation and records `next_attempt_at` and
`last_error_code`. There is no startup sweep, automatic retry loop, or age/count deletion.

Missing routes and lost session incarnations can be explicitly classified by the existing
durable board controller, using an immutable ownership witness in the supplied owning
profile. A valid route in another profile cannot be retired. A matching committed receipt
takes precedence over retirement. Otherwise the delivery remains an inspectable dead letter
with an explicit reason. Its profile retirement record releases serialized session admission.
If board retirement commits before profile retirement, reconciliation repairs the gap.
When board admission IDs are both NULL, it verifies the exact committed marker and ownership
witness. Partial or conflicting identities are rejected. Replacement admission only reconciles
the matching session incarnation.

Replay protection runs at native tool execution and inline/nested dispatch boundaries,
before hooks or ordinary tool execution. The single permitted mutation is `kanban_comment`
on a task with the same route. Its durable key is `(delivery_id, turn_id, operation_slot)`.
The sanitized operation fingerprint and result commit in the same board transaction as
the comment/event mutation. Replaying that key returns its result without another mutation.
Changing the operation for the same key is rejected. Tool calls run in stable sequential order.
External effects and every other tool return `completion_replay_unsafe`. No durable adapter
for those effects is claimed by this phase.

## Review follow-up verification receipt

Follow-up implementation revision: `fd51556f4b94dc0e264a41520ffc6ad23ea03cd2`.
Reviewed starting HEAD: `1693719b355349600f12cfff3018d7eb0b7ebcd7`.
Verification date: 2026-10-07, macOS arm64, Python 3.14.7.

The explicit silent scope suppresses stream, reasoning, tool-generation, status, interim,
reaction, TTS, plugin-stream, and presentation callbacks. Context-local stdout/stderr routing
and logging filters also cover inherited workers after the parent exits. Provider watchdogs
and periodic callbacks retain the scope through error handling, including uncaught worker
failures. Ordinary contexts retain their original output and callbacks.

The audited direct boundaries refuse metrics consent reconciliation, lifecycle accounting,
activity projections, titles, request dumps, trajectories, background review, finalizer
cleanup, and provider cache/rate-limit publication. Native turn-lease renewal/release,
process-local bookkeeping, provider transport cleanup, and the fenced saga writes remain.
Oversized tool results are bounded inline, bypassing spill files, pruning, remote-environment
resolution, and filesystem budget persistence. Native result envelopes, including early
errors, contain no response text or transcript messages. No committed response is delivered
to a CLI, TUI, desktop, or gateway surface in this phase.

| Verification | Exact result |
| --- | --- |
| Original 146-test foundation/saga command, with new cases | **178 passed, 0 failed**: 85 foundation, 93 saga, including 32 added review cases. |
| Affected 16-file suite in isolated candidate snapshot | **190 passed, 0 failed, 1 skipped**. |
| Additional 10 observer/provider files in isolation | **255 passed, 0 failed**. |
| Final provider/scheduler subset after health fixes | **9 passed, 0 failed**. |
| Ordinary relocated request-debug behavior | **1 passed, 0 failed**, 62 deselected. |
| Documented 26-file candidate | **284 passed, 17 failed, 1 skipped, 10 strict xfailed**. |
| Unchanged approved Phase 0/1 base, same 26 files/layout | **284 passed, 17 failed, 1 skipped, 10 strict xfailed**. |
| `python scripts/check` | **11 checks passed, 0 blockers, 0 advisories**. |
| Unstaged and staged `git diff --check` | **Passed**. |

Candidate and base have identical failing test IDs and assertion causes after normalizing
only generated task IDs. The exact 17 IDs remain in the comparison table below: 14 notifier
home-I/O guard failures, two Kanban-tool home-I/O guard failures, and the infrastructure
spawn/card assertion. There are no new failures. The historical WAL-preflight race did not
occur. No file retries were enabled. The 10 strict expected failures remain four legacy TUI
crash cases for Phase 6, five Terminal lifecycle cases for Phase 4, and one interactive CLI
route-capture case for Phase 3. Their files and expected-failure contracts are unchanged.

The first nine focused review cases failed on the reviewed implementation before source
changes, for the intended holder, missing agent, plan-slot, and premature-output reasons.
Additional audit regressions failed before their corresponding fixes: five observer/tool
spill/native-envelope cases, two worker exception cases, the native error fallback case,
and the cached-prompt reconstruction case. Successful publication/effect fixtures now take
real durable turn leases and persist their plans. Tests exercise the actual native loop,
effect guard, callback lookups, provider watchdogs, and scheduler workers against disposable
board/profile stores. A crash immediately before receipt followed by retry commits one
linked response receipt and one comment. Intermediate callback-restoration and scheduler
worker-start regressions were corrected and their tests pass.

The foundation and affected commands are exactly those listed below. The latest foundation
run used the worktree's prepared `test-env/bin/python`. The affected and observer runs used
`phase2-base-env/bin/python` in the isolated source snapshot
`/private/tmp/hermes-completion-phase01.G1Ybro/phase2-candidate`. The final base comparison
ran only after verifying a clean detached checkout of
`4500a0f640f2d15a22e694435cc7a061586c35a5` in the same Hermes worktree layout.
The 26-file command is unchanged from the foundation receipt, using `test-env/bin/python`
for candidate and `phase2-base-env/bin/python` for base.

Additional observer/provider command:

```bash
HERMES_PYTHON=/private/tmp/hermes-completion-phase01.G1Ybro/phase2-base-env/bin/python \
scripts/run_tests.sh \
  tests/agent/test_cron_inline_api_call.py \
  tests/agent/test_periodic_scheduler.py \
  tests/agent/test_periodic_scheduler_context.py \
  tests/agent/test_nous_rate_guard.py \
  tests/agent/test_system_prompt_restore.py \
  tests/agent/test_prompt_cache_boundary.py \
  tests/agent/test_plugin_stream_hooks.py \
  tests/agent/test_provider_fallback.py \
  tests/hermes_cli/test_relay_shared_metrics.py \
  tests/hermes_cli/test_relay_shared_metrics_runtime.py \
  -q --tb=short --file-retries 0 -j 4
```

The final worker subset used that interpreter and the first three files with
`-q --tb=short --file-retries 0 -j 3`. Ordinary debug verification used
`tests/agent/test_run_agent_codex_responses.py` with
`-k test_dump_api_request_debug_reads_the_anthropic_client_and_messages_url`
and `-q --tb=short --file-retries 0 -j 1` in the isolated candidate.

Final logs under `/private/tmp/hermes-completion-phase01.G1Ybro/`:
`phase2-review-foundation-final-source.log`, `phase2-review-affected-isolated-final.log`,
`phase2-review-observers-isolated.log`, `phase2-review-workers-final.log`,
`phase2-review-request-debug.log`, `phase2-review-regression-candidate-final.log`,
`phase2-review-regression-base-final.log`, and `phase2-review-check-receipt.log`.
Red evidence: `phase2-review-red.log`, `phase2-review-audit-red.log`,
`phase2-review-worker-red.log`, `phase2-review-fallback-red.log`, and
`phase2-review-prompt-red.log`. The owned red-run trajectory artifact was preserved outside
the repository as `phase2-review-red-trajectory.jsonl`.

## Initial Phase 2 verification

Verified on 2026-10-07 on macOS arm64, Python 3.14.7. The prepared interpreter was
`/private/tmp/hermes-completion-phase01.G1Ybro/test-env/bin/python`. Tests used the canonical
runner, isolated Hermes homes, and disposable SQLite stores. Native inference was mocked
at the client boundary while executing the actual turn/tool/persistence seams.

The isolated base interpreter used the same dependencies without the candidate's editable
source mapping. Base regression verification used an unchanged detached checkout at the
approved revision in the same `.hermes/hermes-agent/.worktrees/` layout. An isolated candidate
source snapshot outside that home layout also checked affected persistence/effect files.

| Verification | Result |
| --- | --- |
| Six foundation files plus new saga file | **146 passed, 0 failed**: 85 foundation and 61 new saga cases. |
| New saga contracts on approved base | **61 failed**, all because the new consumer/execution APIs were absent. |
| Combined admission/retirement crash regression before final fix | **1 failed, 3 passed**; exact failure was `retired completion identity mismatch` at the combined gap. |
| Documented 26-file suite | **284 passed, 17 failed, 1 skipped, 10 strict xfailed**. |
| Same 26 files on unchanged approved base | **284 passed, 17 failed, 1 skipped, 10 strict xfailed**. |
| Affected 16 persistence/effect files | **142 passed, 46 failed, 2 setup errors, 1 skipped**. |
| Same affected files on unchanged approved base | **142 passed, 46 failed, 2 setup errors, 1 skipped**. |
| Affected files in isolated candidate snapshot | **190 passed, 0 failed, 1 skipped**. |
| `python scripts/check` | **11 checks passed, 0 code-health blockers, 0 advisories**. |
| `git diff --check` | **Passed**; staged additions checked before committing as well. |

Exact failure IDs and causes match the unchanged base in both suites, including both setup
errors. The regression suite has 16 home-I/O guard failures and the existing infrastructure
spawn/card assertion. All 46 affected failures and both setup errors have the same refused
home manifest probe. No new failure IDs or causes appeared. Isolation makes all affected
contracts pass without bypassing or changing the I/O guard.

The new tests cover eight crash boundaries, durable reopen/recovery, concurrent admission
and execution, fences at marker/receipt/effect/CAS writes, exact receipt linkage, unrelated
rows, duplicate session strings across profiles, recreated sessions, missing routes,
scheduled retries, retained dead letters, mutation rollback, native effect replay, borrowed
row rejection, publication idempotency, and combined admission/retirement gaps.

The existing strict expected failures remain unchanged: four legacy TUI collector crash
contracts for Phase 6, five Terminal lease lifecycle contracts for Phase 4, and one
interactive CLI route-capture contract for Phase 3. Phase 2 has separate passing saga tests.
The pre-existing WAL-preflight race did not occur in these Phase 2 verification runs.
Its historical Phase 0/1 limitation remains documented in the foundation receipt.

### Exact commands

Foundation and Phase 2:

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

The regression command is the exact 26-file command in the
[foundation receipt](kanban-completion-foundation.md#verification-receipt), with the same
`HERMES_PYTHON` and flags. Affected persistence/effect files:

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
  -q --tb=short --file-retries 0 -j 4
```

The unchanged base used `phase2-base-env/bin/python` in the detached baseline checkout.
The isolated candidate used that interpreter in `phase2-candidate`. Red-on-base ran the
new saga file copied into the approved-base archive with `-j 1` and the same other flags.
The combined-gap reproduction ran that file with
`-k retired_delivery_releases_session -q --tb=short --file-retries 0 -j 1`
against the candidate snapshot before the last fix.

```bash
/private/tmp/hermes-completion-phase01.G1Ybro/test-env/bin/python scripts/check
git diff --check
```

Logs are under `/private/tmp/hermes-completion-phase01.G1Ybro/`: `phase2-red-complete-final.log`,
`phase2-combined-gap-red.log`, `phase2-foundation-complete.log`, `phase2-regression-complete.log`,
`phase2-regression-base.log`, `phase2-affected-complete.log`, `phase2-affected-base.log`,
`phase2-affected-isolated-complete.log`, and `phase2-check-complete.log`.

### Exact baseline failure comparison

The IDs below appeared on both the approved base and the final candidate. `home guard` means
`AssertionError: TEST BUG: file I/O against the REAL hermes home` for the worktree-parent
`manifest.json` probe through dependency bootstrap. `spawn assertion` means
`test_infrastructure_spawn_refusal_never_charges_the_card` received a task ID in
`auto_blocked` instead of an empty list. Generated task IDs were normalized for comparison.

26-file regression:

| Test ID | Outcome | Same cause |
| --- | --- | --- |
| `tests/hermes_cli/test_kanban_db.py::test_infrastructure_spawn_refusal_never_charges_the_card` | failed | spawn assertion |
| `tests/hermes_cli/test_kanban_notify.py::test_gateway_autosubscribe_roundtrips_user_id_alt_for_session_key[channel-None-False]` | failed | home guard |
| `tests/hermes_cli/test_kanban_notify.py::test_gateway_autosubscribe_roundtrips_user_id_alt_for_session_key[group-th-42-True]` | failed | home guard |
| `tests/hermes_cli/test_kanban_notify.py::test_gateway_create_autosubscribes_on_explicit_board` | failed | home guard |
| `tests/hermes_cli/test_kanban_notify.py::test_notifier_artifact_delivery_skips_missing_files` | failed | home guard |
| `tests/hermes_cli/test_kanban_notify.py::test_notifier_notify_plus_wake_sends_and_wakes` | failed | home guard |
| `tests/hermes_cli/test_kanban_notify.py::test_notifier_notify_wake_does_not_wake_on_status_event` | failed | home guard |
| `tests/hermes_cli/test_kanban_notify.py::test_notifier_plain_notify_never_wakes_even_with_session_id` | failed | home guard |
| `tests/hermes_cli/test_kanban_notify.py::test_notifier_unsubs_after_abnormal_events[crashed]` | failed | home guard |
| `tests/hermes_cli/test_kanban_notify.py::test_notifier_unsubs_after_abnormal_events[gave_up]` | failed | home guard |
| `tests/hermes_cli/test_kanban_notify.py::test_notifier_unsubs_after_abnormal_events[timed_out]` | failed | home guard |
| `tests/hermes_cli/test_kanban_notify.py::test_notifier_uploads_review_handoff_artifacts` | failed | home guard |
| `tests/hermes_cli/test_kanban_notify.py::test_notifier_wake_forwards_persisted_chat_type_and_user_id` | failed | home guard |
| `tests/hermes_cli/test_kanban_notify.py::test_notifier_wake_only_skips_send_and_advances_cursor` | failed | home guard |
| `tests/hermes_cli/test_kanban_notify.py::test_notifier_wakes_origin_for_review_and_keeps_subscription` | failed | home guard |
| `tests/tools/test_kanban_tools.py::test_create_tui_subscription_binds_to_live_session_after_compaction_fork` | failed | home guard |
| `tests/tools/test_kanban_tools.py::test_live_tui_session_key_uses_profile_store` | failed | home guard |

Affected persistence/effect files:

| Test ID | Outcome | Same cause |
| --- | --- | --- |
| `tests/agent/test_identity_flush.py::TestFlushCursorMarkerPop::test_filled_row_repersists_after_marker_pop_and_cursor_invalidation` | failed | home guard |
| `tests/agent/test_identity_flush.py::TestIdentityFlush::test_cursor_reset_starts_new_turn_identity_window` | failed | home guard |
| `tests/agent/test_identity_flush.py::TestIdentityFlush::test_flush_does_not_retain_object_ids_across_turns` | failed | home guard |
| `tests/agent/test_identity_flush.py::TestIdentityFlush::test_repair_shrunk_messages_below_history_length_still_persists_assistant` | failed | home guard |
| `tests/agent/test_identity_flush.py::TestIdentityFlush::test_repeated_flush_same_turn_writes_once` | failed | home guard |
| `tests/agent/test_identity_flush.py::TestIdentityFlush::test_stale_seed_id_from_prior_flush_cannot_suppress_new_message` | failed | home guard |
| `tests/agent/test_identity_flush.py::TestIdentityFlush::test_summary_flush_hides_pure_handoff_but_not_composite_live_ask` | failed | home guard |
| `tests/agent/test_text_turn_incremental_persistence.py::TestCompletedTextTurnIncrementalPersistence::test_completed_text_turn_is_flushed_before_finalization` | error | home guard |
| `tests/agent/test_text_turn_incremental_persistence.py::TestCompletedTextTurnIncrementalPersistence::test_flush_failure_does_not_abort_the_completed_turn` | error | home guard |
| `tests/agent/test_tool_executor_checkpoint_paths.py::test_container_session_refuses_host_rollback_on_every_surface` | failed | home guard |
| `tests/agent/test_turn_finalizer_final_response_persistence.py::test_advisory_exit_reasons_keep_failed_false_but_carry_a_failure_code` | failed | home guard |
| `tests/agent/test_turn_finalizer_final_response_persistence.py::test_delivery_only_reasoning_excerpt_does_not_fill_blank_assistant` | failed | home guard |
| `tests/agent/test_turn_finalizer_final_response_persistence.py::test_empty_final_response_recovers_stream_buffer_into_blank_assistant_row` | failed | home guard |
| `tests/agent/test_turn_finalizer_final_response_persistence.py::test_failed_turn_does_not_recover_stream_buffer_as_final_response` | failed | home guard |
| `tests/agent/test_turn_finalizer_final_response_persistence.py::test_fallback_timestamp_survives_delayed_sqlite_persistence` | failed | home guard |
| `tests/agent/test_turn_finalizer_final_response_persistence.py::test_final_response_closes_tool_tail_before_persistence` | failed | home guard |
| `tests/agent/test_turn_finalizer_final_response_persistence.py::test_final_response_fill_invalidates_flush_scan_cursor` | failed | home guard |
| `tests/agent/test_turn_finalizer_final_response_persistence.py::test_final_response_fills_pure_tool_call_tail` | failed | home guard |
| `tests/agent/test_turn_finalizer_final_response_persistence.py::test_hard_failure_exit_reasons_still_fail_the_turn` | failed | home guard |
| `tests/agent/test_turn_finalizer_final_response_persistence.py::test_stream_recovered_final_response_survives_persist_step_failure` | failed | home guard |
| `tests/hermes_state/test_compression_watermark_commit.py::TestCommitFence::test_commit_allowed_for_live_holder` | failed | home guard |
| `tests/hermes_state/test_compression_watermark_commit.py::TestCommitFence::test_commit_refused_when_lease_expired` | failed | home guard |
| `tests/hermes_state/test_compression_watermark_commit.py::TestCommitFence::test_commit_refused_when_lease_lost` | failed | home guard |
| `tests/hermes_state/test_compression_watermark_commit.py::TestCommitFence::test_refused_commit_rolls_back_atomically` | failed | home guard |
| `tests/hermes_state/test_compression_watermark_commit.py::TestConcurrentAppendDuringCompaction::test_append_racing_the_commit_transaction` | failed | home guard |
| `tests/hermes_state/test_compression_watermark_commit.py::TestRotationPathWatermark::test_ceiling_excludes_the_rotators_own_flush` | failed | home guard |
| `tests/hermes_state/test_compression_watermark_commit.py::TestRotationPathWatermark::test_no_watermark_keeps_historical_rotation` | failed | home guard |
| `tests/hermes_state/test_compression_watermark_commit.py::TestRotationPathWatermark::test_tail_clones_into_the_child[multimodal]` | failed | home guard |
| `tests/hermes_state/test_compression_watermark_commit.py::TestRotationPathWatermark::test_tail_clones_into_the_child[text]` | failed | home guard |
| `tests/hermes_state/test_compression_watermark_commit.py::TestWatermarkCommit::test_archived_rows_stay_recoverable` | failed | home guard |
| `tests/hermes_state/test_compression_watermark_commit.py::TestWatermarkCommit::test_concurrent_tail_survives_compaction` | failed | home guard |
| `tests/hermes_state/test_compression_watermark_commit.py::TestWatermarkCommit::test_conversation_load_is_correct_after_commit` | failed | home guard |
| `tests/hermes_state/test_compression_watermark_commit.py::TestWatermarkCommit::test_no_tail_behaves_identically_to_legacy` | failed | home guard |
| `tests/hermes_state/test_compression_watermark_commit.py::TestWatermarkCommit::test_none_watermark_preserves_historical_behavior` | failed | home guard |
| `tests/hermes_state/test_compression_watermark_commit.py::TestWatermarkCommit::test_session_counters_include_tail` | failed | home guard |
| `tests/hermes_state/test_compression_watermark_commit.py::TestWatermarkCommit::test_tail_clone_preserves_every_column` | failed | home guard |
| `tests/hermes_state/test_repaired_history_commit_coverage.py::test_repaired_reload_commit_archives_only_held_rows[composed_drops_record_the_original_row_once]` | failed | home guard |
| `tests/hermes_state/test_repaired_history_commit_coverage.py::test_repaired_reload_commit_archives_only_held_rows[dropped_row_is_named_not_a_watermark_fallback]` | failed | home guard |
| `tests/hermes_state/test_repaired_history_commit_coverage.py::test_repaired_reload_commit_archives_only_held_rows[folded_assistant_text_never_claims_a_later_row]` | failed | home guard |
| `tests/hermes_state/test_repaired_history_commit_coverage.py::test_repaired_reload_commit_archives_only_held_rows[timestamp_rendered_merged_user_names_its_run]` | failed | home guard |
| `tests/hermes_state/test_repaired_history_commit_coverage.py::test_repaired_reload_commit_archives_only_held_rows[users_adjacent_only_after_a_drop_skip_the_dropped_row]` | failed | home guard |
| `tests/hermes_state/test_repaired_history_commit_coverage.py::test_repaired_reload_commit_archives_only_held_rows[watermark_tail_rewind_widens_by_rows_dropped_behind_a_carried_dict]` | failed | home guard |
| `tests/hermes_state/test_restore_alternation_repair.py::test_acp_restore_heals_alternation_for_live_replay` | failed | home guard |
| `tests/hermes_state/test_restore_alternation_repair.py::test_repair_alternation_merges_user_pair` | failed | home guard |
| `tests/hermes_state/test_restore_alternation_repair.py::test_repaired_load_is_stable_under_prerequest_repair` | failed | home guard |
| `tests/hermes_state/test_session_turn_lease.py::test_turn_lease_fence_walks_compression_child_to_root` | failed | home guard |
| `tests/hermes_state/test_session_turn_lease.py::test_turn_lease_fence_walks_continuation_that_inherited_fork_markers` | failed | home guard |
| `tests/hermes_state/test_session_turn_lease.py::test_turn_lease_fences_stale_transcript_flush_after_reclaim` | failed | home guard |

## Files changed

The initial Phase 2 implementation and receipt changed these 19 files:

- Saga/storage: `hermes_cli/completion_inbox.py`, `hermes_cli/completion_inbox_store.py`, `hermes_cli/completion_inbox_recovery.py`, `hermes_state_kanban.py`, `hermes_cli/kanban_delivery_schema.py`.
- Native turn/persistence: `agent/completion_inbox_execution.py`, `agent/session_persistence.py`, `agent/turn_context.py`, `agent/turn_facade.py`, `agent/turn_facade_lease.py`, `agent/turn_final_response.py`.
- Actual effect boundaries and observers: `agent/tool_executor.py`, `agent/tool_dispatch_helpers.py`, `model_tools.py`, `hermes_cli/lifecycle.py`, `agent/title_generator.py`.
- Behavior tests: `tests/hermes_cli/test_completion_inbox_saga.py`.
- Documentation: `website/docs/developer-guide/kanban-completion-foundation.md`, `website/docs/developer-guide/kanban-completion-inbox-saga.md`.


The review follow-up changes 25 intended files:

- Durable authority and plan: `hermes_cli/completion_inbox.py`, `agent/completion_inbox_execution.py`.
- Silent/native/output policy: `agent/completion_inbox_silence.py`, `agent/completion_inbox_native.py`, `agent/completion_inbox_output.py`, `run_agent.py`, `agent/turn_facade.py`, `agent/turn_finalizer.py`, `agent/session_persistence.py`.
- Provider and inherited worker boundaries: `agent/chat_completion_helpers.py`, `agent/periodic_scheduler.py`, `agent/nous_rate_guard.py`, `agent/system_prompt.py`, `agent/request_debug.py`, `agent/agent_runtime_helpers.py`.
- Observers and tool output: `agent/tool_executor.py`, `agent/plugin_stream_hooks.py`, `agent/relay_runtime.py`, `agent/turn_context.py`, `agent/turn_usage.py`, `hermes_cli/observability/relay_shared_metrics.py`, `hermes_cli/observability/shared_metrics_process.py`.
- Tests and receipts: `tests/hermes_cli/test_completion_inbox_saga.py`, `website/docs/developer-guide/kanban-completion-foundation.md`, `website/docs/developer-guide/kanban-completion-inbox-saga.md`.

## Remaining boundary

All `completion_inbox` flags remain false by default. `cli_enabled`, `gateway_enabled`,
`tui_enabled`, and `repair_enabled` still reject activation. No acquisition/heartbeat of
consumer leases, polling, foreground lifecycle, interactive route capture, TUI/Desktop
integration, historical replay/repair, gateway fallback, JEV, or hosted egress is included.
There was no live database migration, service restart, production configuration change,
push, or merge. Phase 3 and consumer activation remain unauthorized. Every builder commit
requires independent review before another phase is authorized.
