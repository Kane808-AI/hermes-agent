"""Profile-pinned MCP surface for Hermes Research and Kanban clients.

Run as ``python -m agent.transports.hermes_control_mcp_server``. One server process serves one
Hermes profile and an explicit set of boards; the existing Hermes dispatcher owns work.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any


def _safe_text(value: Any) -> str:
    from agent.redact import redact_sensitive_text

    return redact_sensitive_text(str(value), force=True, redact_url_credentials=True)


class HermesControlBridge:
    def __init__(self, profile_home: Path, boards: list[str], assignees: list[str] | None = None):
        from hermes_cli import kanban_db as kb

        home = profile_home.expanduser().resolve()
        if not home.is_dir() or not (home / "config.yaml").is_file():
            raise ValueError("--profile-home must be an existing Hermes profile with config.yaml")
        if home.name != "research" or home.parent.name != "profiles":
            raise ValueError("this bridge requires the named Hermes Research profile")
        if not boards:
            raise ValueError("at least one --allow-board is required")
        self.profile_home = home
        self.boards = frozenset(kb._require_slug(board) for board in boards)
        self.assignees = frozenset(assignees or [home.name])
        if not self.assignees or any(not a or "/" in a or ".." in a for a in self.assignees):
            raise ValueError("assignee names must be explicit profile names")

    def _board(self, board: str) -> str:
        from hermes_cli import kanban_db as kb

        slug = kb._require_slug(board)
        if slug not in self.boards:
            raise ValueError("board is not in this bot's --allow-board list")
        if slug not in {b["slug"] for b in kb.list_boards(include_archived=False)}:
            raise ValueError("board does not exist or is archived")
        return slug

    def abilities_list(self) -> dict:
        from tools.skills_tool import skills_list

        raw = json.loads(skills_list())
        return {"profile": self.profile_home.name, "skills": [
            {"name": s["name"], "category": s.get("category"),
             "description": _safe_text(s.get("description") or "")}
            for s in raw.get("skills", [])
        ]}

    def ability_view(self, name: str) -> dict:
        from tools.skills_tool import skill_view

        if not name or len(name) > 160 or any(part in {".", ".."} for part in name.split("/")):
            raise ValueError("invalid skill name")
        raw = json.loads(skill_view(name, preprocess=False))  # No file_path or shell preprocessing.
        if not raw.get("success"):
            raise ValueError("skill not available")
        # Never return local source paths or linked files. The existing redactor is forced on.
        content = raw.get("content") or ""
        return {"profile": self.profile_home.name, "name": name, "content": _safe_text(content)}

    def research_run(self, task: str, web: bool = False) -> dict:
        """Run one bounded Research turn; public web access is an explicit choice."""
        if not task.strip() or len(task) > 4000:
            raise ValueError("task must contain 1 to 4000 characters")
        command = [sys.executable, "-m", "hermes_cli.main", "chat", "--query-file", "-",
                   "--format", "stream-json", "--max-turns", "6", "--run-budget", "120",
                   "--source", "tool", "--toolsets", "web,session_search" if web else "session_search"]
        from tools.environments.local import served_profile_child_env

        env = served_profile_child_env(target_home=self.profile_home, inherit_credentials=True)
        repo_root = str(Path(__file__).resolve().parents[2])
        env["PYTHONPATH"] = os.pathsep.join(p for p in (repo_root, env.get("PYTHONPATH")) if p)
        env["HERMES_REDACT_SECRETS"] = "true"
        env["HERMES_QUIET"] = "1"
        # Neither toolset admits terminal, file, messaging, skill_manage or Kanban writes.
        try:
            result = subprocess.run(command, input=task, text=True, encoding="utf-8",
                                    errors="replace", capture_output=True,
                                    timeout=150, env=env, cwd=self.profile_home)
        except subprocess.TimeoutExpired:
            return {"status": "timed_out", "profile": self.profile_home.name, "timeout_seconds": 150}
        terminal = None
        for line in result.stdout.splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("type") == "result":
                terminal = event
        if terminal is None:
            return {"status": "failed", "profile": self.profile_home.name,
                    "error": _safe_text(result.stderr[-1000:])}
        return {"status": "completed" if result.returncode == 0 and terminal.get("exit_code") == 0 else "failed",
                "profile": self.profile_home.name, "session_id": terminal.get("session_id"),
                "answer": _safe_text(terminal.get("text") or ""), "tokens": terminal.get("tokens"),
                "duration_ms": terminal.get("duration_ms"),
                "error": _safe_text(terminal["error"]) if terminal.get("error") else None}

    def kanban_boards(self) -> dict:
        from hermes_cli import kanban_db as kb

        return {"boards": [{"slug": b["slug"], "name": b["name"],
                            "project_id": b.get("project_id")}
                           for b in kb.list_boards(include_archived=False)
                           if b["slug"] in self.boards]}

    def kanban_tasks(self, board: str, status: str | None = None) -> dict:
        from hermes_cli import kanban_db as kb
        from hermes_cli.kanban_db_connect import connect_closing

        board = self._board(board)
        with connect_closing(board=board) as conn:
            tasks = kb.list_tasks(conn, status=status, limit=100)
        return {"board": board, "tasks": [{"id": t.id, "title": _safe_text(t.title),
                "status": t.status, "assignee": t.assignee, "created_at": t.created_at}
                for t in tasks], "truncated": len(tasks) == 100}

    def kanban_create(self, board: str, title: str, body: str, assignee: str,
                      idempotency_key: str) -> dict:
        from hermes_cli import kanban_db as kb
        from hermes_cli.kanban_db_connect import connect_closing

        board = self._board(board)
        if assignee not in self.assignees:
            raise ValueError("assignee is not in this bot's --allow-assignee list")
        if not title.strip() or len(title) > 200 or len(body) > 8000:
            raise ValueError("title or body exceeds bridge limits")
        if not idempotency_key.strip() or len(idempotency_key) > 160:
            raise ValueError("idempotency_key must contain 1 to 160 characters")
        with connect_closing(board=board) as conn:
            task_id = kb.create_task(conn, title=title, body=body, assignee=assignee,
                    created_by=f"mcp:{self.profile_home.name}", idempotency_key=idempotency_key,
                    max_runtime_seconds=900, board=board)
            task = kb.get_task(conn, task_id)
        return {"board": board, "task_id": task_id, "status": task.status,
                "result_tool": "kanban_task_status", "wake": "poll kanban_task_status; Hermes owns dispatch"}

    def kanban_task_status(self, board: str, task_id: str) -> dict:
        from hermes_cli import kanban_db as kb
        from hermes_cli.kanban_db_connect import connect_closing

        board = self._board(board)
        with connect_closing(board=board) as conn:
            task = kb.get_task(conn, task_id)
            if task is None:
                raise ValueError("task not found on selected board")
            run = kb.latest_run(conn, task_id)
            summary = kb.latest_summary(conn, task_id)
        return {"board": board, "task_id": task_id, "title": _safe_text(task.title),
                "status": task.status, "assignee": task.assignee,
                "result": _safe_text(summary or task.result or ""),
                "run": {"id": run.id, "status": run.status, "outcome": run.outcome,
                        "error": _safe_text(run.error) if run.error else None} if run else None,
                "terminal": task.status in {"done", "blocked", "archived"}}

    def kanban_comment(self, board: str, task_id: str, body: str) -> dict:
        from hermes_cli import kanban_db as kb
        from hermes_cli.kanban_db_connect import connect_closing

        board = self._board(board)
        if not body.strip() or len(body) > 4000:
            raise ValueError("comment must contain 1 to 4000 characters")
        with connect_closing(board=board) as conn:
            if kb.get_task(conn, task_id) is None:
                raise ValueError("task not found on selected board")
            comment_id = kb.add_comment(conn, task_id, f"mcp:{self.profile_home.name}", body)
        return {"board": board, "task_id": task_id, "comment_id": comment_id}


def build_server(bridge: HermesControlBridge):
    from mcp.server import MCPServer

    server = MCPServer("hermes-control-mcp", instructions=(
        "Use explicit board names. Research is one bounded Hermes turn. Kanban creation returns a "
        "task ID; check kanban_task_status for the Hermes worker result. No client push wake is installed."))
    for name in ("abilities_list", "ability_view", "research_run", "kanban_boards",
                 "kanban_tasks", "kanban_create", "kanban_task_status", "kanban_comment"):
        server.add_tool(getattr(bridge, name), name=name)
    return server


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile-home", type=Path, required=True)
    parser.add_argument("--allow-board", action="append", required=True)
    parser.add_argument("--allow-assignee", action="append")
    args = parser.parse_args(argv)
    for key in ("HERMES_KANBAN_DB", "HERMES_KANBAN_BOARD", "HERMES_KANBAN_TASK",
                "HERMES_KANBAN_RUN_ID", "HERMES_KANBAN_WORKER"):
        os.environ.pop(key, None)
    os.environ["HERMES_HOME"] = str(args.profile_home.expanduser().resolve())
    os.environ["HERMES_REDACT_SECRETS"] = "true"
    os.environ["HERMES_QUIET"] = "1"
    bridge = HermesControlBridge(args.profile_home, args.allow_board, args.allow_assignee)
    build_server(bridge).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
