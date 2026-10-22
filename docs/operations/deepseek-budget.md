# Shared DeepSeek request accounting

This optional transport integration records supported DeepSeek requests in a shared
SQLite ledger. It does not grant spending or external-action permission, choose a
model, change billing, or intercept arbitrary network clients.

## Policy modes

The shared root policy is `deepseek-budget.yaml` under
`get_default_hermes_root()`. Profile configuration cannot override it. The policy
contains `enabled`, `credential_accounts` and an optional `mode`:

- `enforce` is the default. The fixed allocation is USD 10 per Pacific calendar
  day, including unresolved reservations carried forward. The initial accounting
  day is held because earlier usage is unknown. An optional immutable transition
  record can admit the initial day and following day within an explicitly recorded
  authorization window. An expired transition or pricing snapshot denies admission.
- `accounting_only` records requests without imposing the custom monetary limit,
  initial-day admission hold, transition-window expiry or pricing-expiry gate.
  It retains historical authorization records as history, not ongoing permission.
  Status returns null monetary limits and available allowance, not an infinite
  balance. Estimates based on expired pricing are explicitly labeled stale.

Both modes keep the durable USD 8 warning threshold, once per Pacific day. In
accounting-only mode this is informational. Neither mode sends a notification or
changes provider auto top-up. The default remains enforcement unless an operator
explicitly selects accounting-only; invalid mode values fail closed.

See `deepseek-budget.yaml.example`. Do not put API keys in the policy. An operator
must verify the SHA256-to-account-label mapping of already-authorized credentials;
the guard only inspects the request header in memory and never logs the credential
or its digest. Only the non-secret account label is persisted.

## Accounting and request bounds

Every physical HTTP attempt reserves durably before transmission, serialized with
SQLite `BEGIN IMMEDIATE`. SDK retries receive distinct reservations. The reservation
uses the supported 1,048,576-token input ceiling at cache-miss rates and caps outgoing
output at 8,192 tokens, preserving a lower requested cap. Prompts and tool schemas
are unchanged. This is accounting serialization, not an independent concurrency limit.

Complete receipts settle cache-hit, cache-miss and output tokens at the rates stored
with that reservation. These are estimates, not provider-reported charges. Missing
or malformed receipts, HTTP errors, connection failures, early closes and interrupted
streams retain the reservation. SSE settlement requires the final `[DONE]` marker.
A duplicate provider response ID cannot refund a second physical attempt.

Unresolved reservations survive restart and carry forward across Pacific midnight.
Cross-day settled requests are conservatively attributed to each day between admission
and settlement. Historical unknown usage is never relabeled zero. Switching mode does
not delete, reset, refund or rewrite ledger history.

Unavailable/corrupt accounting, unsupported schema or an observed receipt-bound violation
remains a terminal integrity stop in both modes. Such a stop must not trigger provider
fallback or credential rotation. Unresolved monetary estimates alone do not prevent
independent requests in accounting-only mode. Normal SDK retry and agent loop limits
are unchanged; they do not establish an aggregate spending ceiling.

## Supported transport and pricing snapshot

The bundled snapshot is dated 2026-10-20 and references
[DeepSeek pricing](https://api-docs.deepseek.com/quick_start/pricing/). Its nanodollar
rates per token are Flash: cache hit 6, cache miss 300, output 1200; Pro: cache hit
44, cache miss 1320, output 3960. This document describes the bundled snapshot,
not a fresh verification of current provider prices. Its expiry is 2026-10-31
16:00 UTC. Enforcement stops at expiry; accounting-only reports stale estimates.

Recognized aliases are `deepseek-flash`, `deepseek-v4-flash`,
`deepseek-v4-flash-vision-exp` and `deepseek-v4-pro`. Unknown models fail closed.
The covered endpoints are HTTPS `api.deepseek.com:443` at `/chat/completions`,
`/v1/chat/completions` and `/beta/chat/completions`. Standalone server tools,
unsupported endpoints and DeepSeek Anthropic-wire construction are not covered and
are rejected by the guarded paths. Local function tools retain separate action authority.

## Coverage and exclusions

The patched primary and auxiliary constructors wrap sync/async HTTPX transports,
including their mounts. Requests from delegated children, cron workers and fallback
clients are covered only when they use those constructors and this installed source.
Old processes, alternate installations, external SDKs/CLIs, arbitrary shell/Python HTTP
calls and plugins supplying their own clients are not universally intercepted.
This is not an OS security boundary or an account-wide billing cap.

## Rollout, status and rollback

Use a reviewed source commit and an explicitly approved policy change. Preserve existing
account mapping and ledger files. Coordinate idle gateway/CLI/worker processes: existing
clients capture policy objects, so changing YAML alone is not a complete rollout. Older
source rejects the new mode key. Do not expose the new policy to concurrently starting
old clients. No paid probe, schedule resumption or model-routing change is required.

`python -m agent.deepseek_budget` reads status without creating a ledger or mutating it.
`--initialize` creates initial accounting metadata; it does not authorize spending or
establish that previous charges were zero. The existing transition command records only
an already-authorized, immutable initial-day allowance, not recurring authority.

Rollback restores the previous source and policy after draining affected work and starts
fresh processes. Never replace the live ledger with an older backup to roll back code.
Re-enabling enforcement reads all existing usage, holds and the original expiry, and can
therefore block requests immediately. Keep SQLite on a local filesystem with locking.

## Offline verification

Use `scripts/run_tests.sh` with an isolated test environment. Tests cover both modes,
Pacific midnight/expiry, immutable history, concurrent processes/threads, complete and
partial streams, sync/async SDK retries, duplicate receipts, invalid request shapes,
account mapping, constructor integration and terminal integrity errors. HTTP responses
are mocked; no provider requests are required. The optional project-control compatibility
test skips when that separate overlay is absent; a skip is not evidence it is installed.
