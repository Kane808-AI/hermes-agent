# PR creation reconciliation across retries

The optional `external_action_guard.enabled: true` profile setting adds a durable
reconciliation gate immediately before the existing authorized tool dispatcher.
It does not grant action permission, replace approvals, or select a model.

## Supported actions

- A simple foreground `gh pr create` terminal command with explicit `--repo
  OWNER/REPO`, `--head BRANCH` and `--base BRANCH` (supported short flags also work).
- The exact canonical tool name `mcp_github_create_pull_request` with explicit
  owner, repo, head and base fields.

The natural identity is repository, head and base; changing title/body does not
create a new identity. A SQLite reservation is committed before execution and
shared across profiles/processes through `external-actions.sqlite3` in the default
Hermes root. A successful matching GitHub PR URL becomes a reusable receipt. An
exception, timeout or unrecognized result leaves an uncertain/inflight record;
repeating that identity is blocked pending reconciliation. Reads, ordinary coding,
tests and unrelated actions continue normally.

Shell wrappers/aliases, arbitrary MCP servers, deployments and messages are outside
coverage. Shell compounds and expansions in a recognized PR command are rejected.
This is a limited accident-prevention mechanism, not exactly-once execution across
arbitrary tools or an OS security boundary. A process with ledger filesystem access
is not contained by this module. Successful receipts have no automatic expiry;
reusing a branch pair later requires an explicit lifecycle decision, not ledger deletion.

## Reconciliation

`python -m agent.external_action_guard status` displays action records. Unlike the
budget status command, it may create the store/schema when absent. It is therefore
not a filesystem-read-only probe.

The operator's `resolve` command requires the action key, attempt, outcome, evidence
and `--confirmed-stopped`. A successful outcome requires a matching PR URL. A
`not_executed` outcome permits another attempt only after authoritative evidence
shows no external action occurred and the original attempt has stopped. These are
trusted operator attestations; this module performs no remote lookup. Age, process
termination or a missing receipt alone is insufficient evidence. Reconciliation
records an event and never grants permission for the next mutation.

Preserve action records during source rollback/restart. Do not clear the database
to make a blocked action run. Coverage tests use fake dispatch functions and SQLite;
no real PR, provider call or deployment is needed.

## Relationship to model fallback

Provider fallback preserves a conversation but cannot determine whether an external
action succeeded. This guard supplies only the supported PR reconciliation behavior.
Native fallback configuration remains a separate operator choice. Pinned delegated
workers need their own `delegation.fallback_providers`; a parent's chain alone does
not cover them. No task classifier, fallback configuration, credentials or runtime
profile files are included in this source change.
