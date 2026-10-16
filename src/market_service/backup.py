"""Consistent, non-destructive SQLite snapshots, including an active WAL writer."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import tempfile
from pathlib import Path

from .store import Journal


def backup_journal(source: str | Path, destination: str | Path) -> dict:
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if destination.exists():
        raise FileExistsError(f"refusing to replace {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=".journal-backup-", dir=destination.parent)
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        # backup() sees a consistent SQLite transaction; copying only the .sqlite
        # file while WAL is active does not provide that guarantee.
        with Journal(source, readonly=True) as origin:
            copy = sqlite3.connect(temporary)
            try:
                origin.db.backup(copy, pages=256)
                if copy.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise RuntimeError("backup SQLite check failed")
                copy.commit()
            finally:
                copy.close()
        with temporary.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
            os.fsync(stream.fileno())
        # Atomic, no-clobber publication on the destination filesystem.
        os.link(temporary, destination)
        with Journal(destination, readonly=True) as journal:
            return {
                "path": str(destination),
                "dataset_id": journal.dataset_id,
                "market_seq": journal.frontier,
                "bytes": destination.stat().st_size,
                "sha256": digest,
                "pending_raw": journal.db.execute(
                    "SELECT COUNT(*) FROM raw_frames WHERE processed=0"
                ).fetchone()[0],
            }
    finally:
        temporary.unlink(missing_ok=True)
