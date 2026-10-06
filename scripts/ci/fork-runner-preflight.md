# Fork standard-runner candidate: preflight and publication gate

Scope: Kane808-AI/hermes-agent, personal/internal, hermes-migration. This is a
local candidate, NOT permission to enable CI, push, open a PR, dispatch, rerun,
merge, change rules/settings, provision runners, or perform a release.

## Candidate and resources

Base/default-branch snapshot: `f7e12a645492578ada323678950f9bf8f0c6c030`.
Branch: `ci/fork-standard-runners-20261006`.

Only exact upstream repository identity selects inherited larger runners. Every
other repository uses `ubuntu-24.04`, `windows-2025`, or native `windows-11-arm`
for the changed jobs. Existing standard Ubuntu/macOS/Windows aliases remain
unchanged; no architecture or lane is substituted. The legacy desktop lane
remains disabled. The desktop update advisory policy remains unchanged.

- Python: both original unit slices, four file workers, 60-minute job limit.
- Python E2E/upgrade and Windows journeys: one file/process tree at a time,
  90 minutes for heavy jobs (60 for Windows core E2E). Existing per-file limits
  and zero-retry race policies remain intact.
- Windows OS tests: both x64 and ARM64 plus both existing macOS slices; four
  Windows workers, 60-minute limits. Matrix concurrency capped at two.
- Upgrade: all dynamically discovered suite shards, maximum two jobs at once.
- JS: all discovered checks, one outer check at a time, 4 GiB V8 heap per Node
  process, 60 minutes. The heap cap is not a whole-job RAM guarantee.
- Rust: two build jobs, 60 minutes.
- Desktop core: existing single-worker suite, 60 minutes. Update: all four
  existing cells, maximum two jobs at once, 90 minutes.
- Nix: one concurrent derivation, two cores per build, 90 minutes. Existing
  cache/store retention policy is unchanged.

Upstream retains its larger labels and worker counts. Timeouts and matrix caps
are deliberately bounded for both repositories. These are starting resource
hypotheses, not proof of hosted capacity or suite success.

## Exact event closure

For an opened/reopened/synchronize PR containing only this candidate diff,
assuming proposed CI activation and the freshly observed disabled
`install-e2e.yml` remains disabled:

- CI: recursively includes all required callable workflows, nested Windows
  install/update, both native Windows architectures, and every shard. Unknown
  applicability outputs are conservatively included by the preflight.
- Nix: independent active PR listener; `.github/` changes force its lane on.
  Its formerly unavailable label is patched here.
- Docker: independent active PR listener. Only `mode` and `detect` run on this
  fork. Its build requires upstream repository identity or release test mode;
  publish/merge require upstream main push or explicit release phase. The
  preflight executes the real mode resolver with empty PR inputs and evaluates
  those conditions. Larger labels remain in unreachable Docker release/upstream
  jobs; this candidate does NOT certify release/main-push/manual events.
- CI review comment: default-branch `workflow_run` listener, standard Ubuntu;
  may post/update a PR comment with its write token. Reads trusted default branch.
- Label rerun: labeled events only, excluded for the proposed event. Do not use
  `run-e2e` as the restoration trigger: it can rerun older unsafe revisions.
- JS auto-fix: main-push/manual only, NOT triggered by this candidate PR. Its
  publication/merge side effects are outside this event and this task.
- PM Bundle, Windows bundle SDK, Termux, plugin catalog, sandbox image: their
  positive path filters do not match this exact diff. PM Bundle and Windows SDK
  still contain unavailable larger labels. ANY expanded diff matching them is
  a hard preflight failure until separately repaired/reviewed. Sandbox image
  additionally gates builds/publishing to upstream.
- Install E2E: observed `disabled_manually`, explicitly supplied as an exclusion.
  It has additional dynamic/release descendants with larger labels. Enabling it
  invalidates this preflight and is a hard gate, not an implicit permission.
- Other workflow_run listeners target install/release workflows, not CI/Nix/
  Docker; no transitive matching listener was found at the pinned base. No
  pull_request_target listener exists at that snapshot.

The machine inventory is deliberately narrow, not a general GitHub Actions
interpreter. It rejects unresolved runner/matrix expressions and unhandled PR
filters; unknown job conditions remain included. It parses all workflow YAML,
expands current matrices, recursively follows local workflow calls, loads
workflow_run listeners from the pinned default-branch source, and emits every
resolved job cell. Re-audit changed workflow semantics, new filters, new callers,
or new listeners rather than extending assumptions silently. It executes ONLY
the inspected local upgrade planner and Docker mode resolver, never test/build/
publish payloads; use it only on a reviewed checkout, not arbitrary untrusted PRs.

## Reproduction (inside the isolated worktree)

Use the checkout's PM-managed test interpreter, not a legacy installed venv.
The preflight uses the project's `ruamel.yaml` dependency in explicit YAML 1.2
safe mode (preserving the Actions `on` key and boolean input defaults); PyYAML
is not required. Resolve the PM test interpreter for this checkout, then set
`TEST_PYTHON` to its absolute path. Imports and tests use this checkout.

    "$TEST_PYTHON" -m scripts.ci.fork_runner_preflight \
      --base f7e12a645492578ada323678950f9bf8f0c6c030 \
      --disabled-workflow install-e2e.yml
    env -u __HERMES_ACTIVATED HERMES_PYTHON="$TEST_PYTHON" scripts/run_tests.sh -j 1 \
      tests/ci/test_fork_runner_preflight.py tests/ci/test_required_results.py
    go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.7 -shellcheck= \
      .github/workflows/tests.yml .github/workflows/tests-os.yml \
      .github/workflows/windows-install-update-e2e.yml .github/workflows/js-tests.yml \
      .github/workflows/rust-tests.yml .github/workflows/e2e-desktop-core.yml \
      .github/workflows/e2e-desktop-update.yml .github/workflows/e2e-desktop.yml \
      .github/workflows/nix.yml
    python scripts/check --only health
    python scripts/check --only ruff
    git diff --check

ShellCheck was unavailable and is explicitly disabled in actionlint; extracted
Bash payloads were syntax-checked with `bash -n`. Native PowerShell payloads were
unchanged and were not executed on this macOS host. No hosted tests ran.

## Mandatory Atlas gate before ANY hosted event

1. Independently review the exact diff. Freeze candidate and current default
   branch SHAs; fresh GET-only readback must confirm repository is still public,
   personal standard-runner eligibility, workflow states, and no new listeners.
   The preflight's `--base` is BOTH diff base and trusted listener snapshot; if
   these diverge, stop and re-audit rather than reuse the old receipt.
2. Re-run the preflight against the actual full PR diff, not just this patch.
   Confirm install E2E remains disabled and no PM/SDK/release path becomes active.
   Review cache/artifact retention and external-service costs separately: free
   standard compute does not guarantee a zero total bill.
3. Coordinate updater and governance work separately. Only Atlas may authorize
   the single fresh eligible PR event and enable the intended CI workflow. Do
   not rerun historical runs or dispatch the old main source.
4. Observe assignment and real labels, disk/RAM use and timeouts. Native Windows
   ARM cold sdists, Nix cold closure/disk usage, serialized upgrade duration,
   Rust compilation and Electron startup remain unmeasured. A timeout/OOM/test
   failure is a failed canary, not a reason to skip tests, add retries, loosen
   assertions, change advisory policy, or buy larger runners.
5. Require actual success of `All required checks pass (v2)` from Actions App
   15368 at the applicable current PR head/test-merge SHA, with intended heavy
   lanes actually executed. Static success, manual dispatch or a no-patch green
   is not that evidence. Strict rule/auto-merge rollout remains Atlas-owned.
