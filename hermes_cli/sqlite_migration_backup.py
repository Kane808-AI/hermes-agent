"""Consistent, private, create-once backups before additive SQLite migrations."""
from __future__ import annotations

import contextlib
import errno
import os
from pathlib import Path
import sqlite3
import tempfile
import warnings


def _sync_backup_directory(path: Path) -> None:
    """Unsupported hosts retain the file-synced, create-once link with weaker power-loss durability.

    Windows cannot open directories with os.open. On filesystems that explicitly refuse
    directory fsync, warn and retain the private snapshot. Other I/O errors stop migration.
    """
    if os.name == "nt":
        return
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError as exc:
        if exc.errno not in {errno.EINVAL, errno.ENOTSUP, errno.ENOSYS}:
            raise
        warnings.warn("directory fsync unsupported; backup file is synced but publication has "
                      "weaker power-loss durability", RuntimeWarning, stacklevel=2)


def migration_backup(conn: sqlite3.Connection, path: Path, label: str) -> Path | None:
    """Include committed WAL frames, never overwrite an earlier rollback snapshot."""
    if conn.in_transaction:
        raise RuntimeError("migration backup must precede the write transaction")
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' LIMIT 1").fetchone():
        return None
    target = path.with_name(f"{path.name}.before-{label}.bak")
    if target.exists():
        _sync_backup_directory(path.parent)
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
        _sync_backup_directory(path.parent)
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
