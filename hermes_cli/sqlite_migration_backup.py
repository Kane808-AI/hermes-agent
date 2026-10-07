"""Consistent, private, create-once backups before additive SQLite migrations."""
from __future__ import annotations

import contextlib
import os
from pathlib import Path
import sqlite3
import tempfile


def migration_backup(conn: sqlite3.Connection, path: Path, label: str) -> Path | None:
    """Include committed WAL frames, never overwrite an earlier rollback snapshot."""
    if conn.in_transaction:
        raise RuntimeError("migration backup must precede the write transaction")
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' LIMIT 1").fetchone():
        return None
    target = path.with_name(f"{path.name}.before-{label}.bak")
    if target.exists():
        return target
    fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=path.parent)
    os.close(fd)
    staged = Path(temporary)
    try:
        with contextlib.closing(sqlite3.connect(staged)) as backup:
            conn.backup(backup)
        with staged.open("rb") as handle:
            os.fsync(handle.fileno())
        try:
            os.link(staged, target)
        except FileExistsError:
            pass  # another initializer already preserved the pre-migration store
        return target
    finally:
        staged.unlink(missing_ok=True)


def execute_ddl(conn: sqlite3.Connection, ddl: str) -> None:
    """Unlike executescript(), keep the caller's BEGIN IMMEDIATE transaction."""
    statement = ""
    for line in ddl.splitlines():
        statement += line + "\n"
        if sqlite3.complete_statement(statement):
            conn.execute(statement)
            statement = ""
    if statement.strip():
        raise ValueError("incomplete migration DDL")
