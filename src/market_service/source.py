"""The shared local replay interface and independent durable consumer state."""

from __future__ import annotations

import fcntl
import hashlib
import sqlite3
from collections.abc import Iterator
from pathlib import Path

from .book import IntegrityError, Projection
from .contracts import Batch, Record, Subscribe
from .store import ZERO_HASH, Journal, chained, event_hash


class Consumer:
    """Checkpoint/cursor transaction belongs to the consumer, not the publisher.

    Downstream trading must commit its business changes with the input cursor in
    its own unit of work. This reference consumer commits only its market view.
    """

    projection_type = Projection
    record_type = Record
    journal_type = Journal
    subscribe_type = Subscribe
    batch_type = Batch

    def __init__(self, checkpoint_path: str | Path | None = None, *, record_trace: bool = False):
        self.projection = self.projection_type()
        self.chain_hash = ZERO_HASH
        self.trace = []
        self.record_trace = record_trace
        self.db = None
        self._lock = None
        if checkpoint_path is not None:
            path = Path(checkpoint_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            self._lock = path.with_suffix(path.suffix + ".lockfile").open("a+b")
            try:
                fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                self._lock.close()
                raise RuntimeError("another consumer owns this checkpoint") from None
            self.db = sqlite3.connect(path)
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS cursor(id INTEGER PRIMARY KEY CHECK(id=1), state BLOB NOT NULL, digest TEXT NOT NULL, chain_hash TEXT NOT NULL)"
            )
            row = self.db.execute(
                "SELECT state,digest,chain_hash FROM cursor WHERE id=1"
            ).fetchone()
            if row:
                if hashlib.sha256(row[0]).hexdigest() != row[1]:
                    raise IntegrityError("consumer checkpoint checksum mismatch")
                self.projection = self.projection_type.restore(row[0])
                self.chain_hash = row[2]

    def apply(self, record: Record):
        if type(record) is not self.record_type:
            raise IntegrityError(
                "live/replay consumer accepts only original Record, not historical repair views"
            )
        if event_hash(record.event) != record.event_hash:
            raise IntegrityError("wire event checksum mismatch")
        if chained(self.chain_hash, record.event_hash) != record.chain_hash:
            raise IntegrityError("wire chain mismatch")
        self.projection.apply(record.event)
        self.chain_hash = record.chain_hash
        if self.record_trace:
            self.trace.append((record.event.market_seq, record.event_hash, record.event.book_hash))

    def commit(self):
        if self.db:
            state = self.projection.checkpoint()
            with self.db:
                self.db.execute(
                    "INSERT OR REPLACE INTO cursor VALUES(1,?,?,?)",
                    (state, hashlib.sha256(state).hexdigest(), self.chain_hash),
                )

    def close(self):
        if self.db:
            self.db.close()
        if self._lock:
            self._lock.close()


def replay(journal_path: str | Path, consumer: Consumer) -> Iterator[Record]:
    """Replay the exact v1 records also sent over the live socket; no re-sorting."""
    with consumer.journal_type(journal_path, readonly=True) as journal:
        if consumer.projection.dataset_id not in (None, journal.dataset_id):
            raise IntegrityError("consumer cursor belongs to another dataset")
        for record in journal.records(consumer.projection.seq):
            consumer.apply(record)
            yield record
        consumer.commit()
