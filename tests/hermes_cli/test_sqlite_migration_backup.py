"""Backup publication syncs its directory without weakening create-once snapshots."""
import contextlib
import errno
import os
import sqlite3
import stat

import pytest

from hermes_cli import sqlite_migration_backup as backup


@pytest.mark.platforms("posix")
def test_backup_syncs_directory_after_final_link_and_keeps_private_snapshot(tmp_path, monkeypatch):
    path = tmp_path / "source.db"
    target = path.with_name("source.db.before-test.bak")
    real_fsync = os.fsync
    calls = []
    def sync(fd):
        is_directory = stat.S_ISDIR(os.fstat(fd).st_mode)
        calls.append((is_directory, target.exists()))
        real_fsync(fd)
    monkeypatch.setattr(backup.os, "fsync", sync)
    with contextlib.closing(sqlite3.connect(path, isolation_level=None)) as conn:
        conn.execute("CREATE TABLE records(value TEXT)")
        conn.execute("INSERT INTO records VALUES ('before')")
        assert backup.migration_backup(conn, path, "test") == target
        assert calls == [(False, False), (True, True)]
        assert target.stat().st_mode & 0o777 == 0o600
        conn.execute("INSERT INTO records VALUES ('after')")
        backup.migration_backup(conn, path, "test")
    with contextlib.closing(sqlite3.connect(target)) as snapshot:
        assert snapshot.execute("SELECT value FROM records").fetchall() == [("before",)]


@pytest.mark.platforms("posix")
@pytest.mark.parametrize("error", [errno.EINVAL, errno.EIO])
def test_directory_sync_unsupported_fallback_but_real_io_error_stops_migration(tmp_path, monkeypatch, error):
    path = tmp_path / "source.db"
    real_fsync = os.fsync
    def sync(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError(error, "directory sync failure")
        real_fsync(fd)
    monkeypatch.setattr(backup.os, "fsync", sync)
    with contextlib.closing(sqlite3.connect(path, isolation_level=None)) as conn:
        conn.execute("CREATE TABLE records(value TEXT)")
        if error == errno.EINVAL:
            with pytest.warns(RuntimeWarning, match="directory fsync"):
                target = backup.migration_backup(conn, path, "test")
            assert target.is_file()
        else:
            with pytest.raises(OSError, match="directory sync failure"):
                backup.migration_backup(conn, path, "test")
            # Retry must sync the published name too, rather than trust exists().
            with pytest.raises(OSError, match="directory sync failure"):
                backup.migration_backup(conn, path, "test")
    assert not list(tmp_path.glob(".*.bak.*"))
