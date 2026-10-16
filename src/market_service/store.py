"""Single-writer SQLite append journal. Raw, canonical and cursors have explicit boundaries."""

from __future__ import annotations

import fcntl
import hashlib
import sqlite3
import uuid
from collections.abc import Iterator
from pathlib import Path

import msgspec

from .book import IntegrityError, Projection
from .contracts import Event, RawFrame, Record, Trade, decode_event, encode

ZERO_HASH = "0" * 64


def event_hash(event: Event) -> str:
    return hashlib.sha256(encode(event)).hexdigest()


def chained(previous: str, digest: str) -> str:
    return hashlib.sha256(bytes.fromhex(previous) + bytes.fromhex(digest)).hexdigest()


class Journal:
    schema_version = "1"
    event_decoder = staticmethod(decode_event)
    record_type = Record

    def __init__(self, path: str | Path, *, readonly: bool = False):
        self.path = Path(path).resolve()
        self.readonly = readonly
        self._lock = None
        if readonly:
            self.db = sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._lock = self.path.with_suffix(self.path.suffix + ".lockfile").open("a+b")
            try:
                fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                self._lock.close()
                raise RuntimeError("another market writer owns this journal") from None
            self.db = sqlite3.connect(self.path)
            if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table'").fetchone():
                try:
                    version = self.db.execute(
                        "SELECT value FROM metadata WHERE key='schema_version'"
                    ).fetchone()
                except sqlite3.DatabaseError:
                    version = None
                if not version or version[0] != self.schema_version:
                    self.close()
                    raise IntegrityError("journal schema mismatch")
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.executescript("""
                CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS raw_frames(
                    raw_seq INTEGER PRIMARY KEY, session_id TEXT NOT NULL, kind TEXT NOT NULL,
                    wall_ns INTEGER NOT NULL, mono_ns INTEGER NOT NULL, boot_id TEXT NOT NULL,
                    payload BLOB NOT NULL, digest TEXT NOT NULL, processed INTEGER NOT NULL DEFAULT 0);
                CREATE INDEX IF NOT EXISTS pending_raw ON raw_frames(raw_seq) WHERE processed=0;
                CREATE TABLE IF NOT EXISTS events(
                    market_seq INTEGER PRIMARY KEY, raw_seq INTEGER NOT NULL REFERENCES raw_frames(raw_seq),
                    payload BLOB NOT NULL, digest TEXT NOT NULL, chain_hash TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS trade_keys(trade_id INTEGER PRIMARY KEY, payload BLOB NOT NULL, digest TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS checkpoints(
                    market_seq INTEGER PRIMARY KEY, payload BLOB NOT NULL, digest TEXT NOT NULL);
            """)
            with self.db:
                self.db.execute(
                    "INSERT OR IGNORE INTO metadata VALUES('dataset_id', ?)", (str(uuid.uuid4()),)
                )
                self.db.execute(
                    "INSERT OR IGNORE INTO metadata VALUES('schema_version',?)",
                    (self.schema_version,),
                )
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.dataset_id = self.db.execute(
            "SELECT value FROM metadata WHERE key='dataset_id'"
        ).fetchone()[0]
        if (
            self.db.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()[0]
            != self.schema_version
        ):
            raise IntegrityError("journal schema mismatch")

    @property
    def frontier(self) -> int:
        return self.db.execute("SELECT COALESCE(MAX(market_seq),0) FROM events").fetchone()[0]

    def raw(self, session_id, kind, payload, wall_ns, mono_ns, boot_id) -> RawFrame:
        with self.db:
            cur = self.db.execute(
                "INSERT INTO raw_frames(session_id,kind,wall_ns,mono_ns,boot_id,payload,digest) VALUES(?,?,?,?,?,?,?)",
                (
                    session_id,
                    kind,
                    wall_ns,
                    mono_ns,
                    boot_id,
                    payload,
                    hashlib.sha256(payload).hexdigest(),
                ),
            )
        return RawFrame(cur.lastrowid, session_id, kind, wall_ns, mono_ns, boot_id, payload)

    def pending(self) -> Iterator[RawFrame]:
        for row in self.db.execute(
            "SELECT raw_seq,session_id,kind,wall_ns,mono_ns,boot_id,payload,digest FROM raw_frames WHERE processed=0 ORDER BY raw_seq"
        ):
            if hashlib.sha256(row[6]).hexdigest() != row[7]:
                raise IntegrityError(f"raw checksum mismatch {row[0]}")
            yield RawFrame(*row[:7])

    def commit(
        self, raw_seq: int, events: list[Event], checkpoint: bytes | None = None
    ) -> list[Record]:
        """Commit all outputs for one raw input and its processed watermark together."""
        records = []
        with self.db:
            row = self.db.execute(
                "SELECT processed FROM raw_frames WHERE raw_seq=?", (raw_seq,)
            ).fetchone()
            if row is None or row[0]:
                raise IntegrityError("missing or already processed raw frame")
            previous_row = self.db.execute(
                "SELECT chain_hash FROM events ORDER BY market_seq DESC LIMIT 1"
            ).fetchone()
            previous = previous_row[0] if previous_row else ZERO_HASH
            expected = self.frontier + 1
            for event in events:
                if (
                    event.market_seq != expected
                    or event.dataset_id != self.dataset_id
                    or event.raw_seq != raw_seq
                ):
                    raise IntegrityError("invalid append position or provenance")
                blob = encode(event)
                self.event_decoder(blob)  # Validate the selected contract before publication.
                digest = hashlib.sha256(blob).hexdigest()
                chain = chained(previous, digest)
                self.db.execute(
                    "INSERT INTO events VALUES(?,?,?,?,?)",
                    (expected, raw_seq, blob, digest, chain),
                )
                if isinstance(event.payload, Trade):
                    trade_blob = encode(event.payload)
                    self.db.execute(
                        "INSERT INTO trade_keys VALUES(?,?,?)",
                        (
                            event.payload.trade_id,
                            trade_blob,
                            hashlib.sha256(trade_blob).hexdigest(),
                        ),
                    )
                records.append(self.record_type(event, digest, chain))
                previous, expected = chain, expected + 1
            if checkpoint is not None:
                self.db.execute(
                    "INSERT OR REPLACE INTO checkpoints VALUES(?,?,?)",
                    (expected - 1, checkpoint, hashlib.sha256(checkpoint).hexdigest()),
                )
            self.db.execute("UPDATE raw_frames SET processed=1 WHERE raw_seq=?", (raw_seq,))
        return records

    def records(self, after: int = 0, limit: int | None = None) -> Iterator[Record]:
        if after < 0 or after > self.frontier:
            raise IntegrityError("cursor outside committed frontier")
        previous = (
            ZERO_HASH
            if after == 0
            else self.db.execute(
                "SELECT chain_hash FROM events WHERE market_seq=?", (after,)
            ).fetchone()[0]
        )
        sql = "SELECT market_seq,payload,digest,chain_hash FROM events WHERE market_seq>? ORDER BY market_seq"
        params = (after,) if limit is None else (after, limit)
        if limit is not None:
            sql += " LIMIT ?"
        expected = after + 1
        for seq, blob, digest, chain in self.db.execute(sql, params):
            event = self.event_decoder(blob)
            if seq != expected or event.market_seq != seq or event.dataset_id != self.dataset_id:
                raise IntegrityError("event identity/sequence mismatch")
            if event_hash(event) != digest or chained(previous, digest) != chain:
                raise IntegrityError(f"event/chain checksum mismatch at {seq}")
            yield self.record_type(event, digest, chain)
            expected, previous = seq + 1, chain

    def trade(self, trade_id: int) -> Trade | None:
        row = self.db.execute(
            "SELECT payload,digest FROM trade_keys WHERE trade_id=?", (trade_id,)
        ).fetchone()
        if row is None:
            return None
        if hashlib.sha256(row[0]).hexdigest() != row[1]:
            raise IntegrityError("trade index checksum mismatch")
        return msgspec.msgpack.decode(row[0], type=Trade)

    def verify_raw(self) -> int:
        count = 0
        for seq, payload, digest in self.db.execute(
            "SELECT raw_seq,payload,digest FROM raw_frames ORDER BY raw_seq"
        ):
            if hashlib.sha256(payload).hexdigest() != digest:
                raise IntegrityError(f"raw checksum mismatch at {seq}")
            count += 1
        if self.db.execute("PRAGMA foreign_key_check").fetchone():
            raise IntegrityError("dangling raw reference")
        return count

    def recover(self) -> Projection:
        row = self.db.execute(
            "SELECT market_seq,payload,digest FROM checkpoints ORDER BY market_seq DESC LIMIT 1"
        ).fetchone()
        if row:
            if hashlib.sha256(row[1]).hexdigest() != row[2]:
                raise IntegrityError("checkpoint checksum mismatch")
            obj = Projection.restore(row[1])
            if obj.seq != row[0] or obj.dataset_id != self.dataset_id:
                raise IntegrityError("checkpoint identity mismatch")
        else:
            obj = Projection()
        for record in self.records(obj.seq):
            obj.apply(record.event)
        return obj

    def close(self):
        self.db.close()
        if self._lock:
            self._lock.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
