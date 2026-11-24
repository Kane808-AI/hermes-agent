# Hermes control MCP server

The Hermes control bridge is a local stdio MCP server. It uses the existing Hermes Research profile for one bounded agent turn and the existing shared Kanban database for board work. Hermes keeps ownership of dispatch, cron, heartbeat, retries, and worker completion.

## Start

After this change is present in the Hermes checkout the client will use, start it with:

```sh
/Users/chriskaneshiro/.hermes/hermes-agent/scripts/hermes-control-mcp \
  --profile-home /Users/chriskaneshiro/.hermes/profiles/research \
  --allow-board sandbox-pilot-20261114 \
  --allow-assignee research
```

The script starts the checkout's pinned Python environment and runs the MCP server on stdio. The MCP client starts and stops the process. To attach it to the two current Orca bots after the code is installed, run:

```sh
orca bot tools add '/Users/chriskaneshiro/.hermes/hermes-agent/scripts/hermes-control-mcp --profile-home /Users/chriskaneshiro/.hermes/profiles/research --allow-board sandbox-pilot-20261114 --allow-assignee research' --name hermes-bridge --bot Atlas
orca bot tools add '/Users/chriskaneshiro/.hermes/hermes-agent/scripts/hermes-control-mcp --profile-home /Users/chriskaneshiro/.hermes/profiles/research --allow-board sandbox-pilot-20261114 --allow-assignee research' --name hermes-bridge --bot Research
orca bot tools --bot Atlas --json
orca bot tools --bot Research --json
```

Replace `sandbox-pilot-20261114` with each approved board slug. Repeat `--allow-board` for more boards and `--allow-assignee` for more Hermes worker profiles. These arguments are startup permissions, not model-chosen inputs. Use separate server entries if the two bots need different board scopes. The bridge refuses missing or archived boards and will not select a current/default board implicitly.

## Tools and results

| Tool | Result |
| --- | --- |
| `research_run(task, web=false)` | One Research-profile Hermes turn with up to six tool iterations, a 120 second run budget, and a 150 second process timeout. Returns `status`, `session_id`, `answer`, token counts, and duration. `web=true` explicitly enables Hermes web tools. |
| `abilities_list()` | Research skill names, categories, and descriptions. |
| `ability_view(name)` | The named skill's `SKILL.md` content, without path fields or linked-file reads. |
| `kanban_boards()` | Only startup-allowed active board slugs and names. |
| `kanban_tasks(board, status)` | Up to 100 task IDs, titles, states, and assignees on one selected board. |
| `kanban_create(board, title, body, assignee, idempotency_key)` | Creates one Hermes task for an allowed assignee. Returns `board`, `task_id`, initial `status`, and `result_tool`. Use a stable idempotency key when retrying. |
| `kanban_task_status(board, task_id)` | Current state, latest worker run, result summary, and `terminal` flag. |
| `kanban_comment(board, task_id, body)` | Adds a comment to an existing task on an allowed board. |

The bridge exposes no arbitrary shell, SQL, file, messaging, or scheduler tool. Skill text and result strings use Hermes' forced secret redaction. The no-web Research mode admits only `session_search`; web mode admits `web` and `session_search`. A Research turn can still spend configured provider tokens.

## Asynchronous completion

`kanban_create` leaves the card ready for Hermes' existing dispatcher. It does not start a second dispatcher. During an active Orca bot turn, call `kanban_task_status` using the returned `board` and `task_id` until `terminal=true`; `result` contains the worker's latest summary. A blocked card also has `terminal=true` and needs an explicit decision before retry. This is the working completion path without an Orca callback URL.

The bridge cannot wake an idle Orca bot when a Hermes worker finishes. Hermes' existing gateway notifier can wake supported Hermes gateway sessions, but no Orca notification route or webhook URL is configured here. Atlas should use the status tool when it next runs or arrange a separate Orca-native wake route once its target address and ownership are available. No schedule is created by this bridge.
