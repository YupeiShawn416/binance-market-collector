"""Separate single-writer repair catalog; the observation journal is always read-only.

Raw replies are committed before interpretation. Facts, provenance and job cursor
then commit together. Historical views have a different type and no live capability.
"""

from __future__ import annotations

import fcntl
import hashlib
import sqlite3
import time
from pathlib import Path
from typing import Literal

import msgspec

from .archive import parse_archive_trades
from .book import IntegrityError
from .contracts import Instrument, Quality, Trade, encode
from .store import ZERO_HASH, Journal
from .trade_history import Reply, compatible, parse_trades


class ResearchTrade(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    view: Literal["observed", "repaired"]
    execution_allowed: Literal[False]
    dataset_id: str
    revision: int
    payload: Trade
    source_ids: tuple[int, ...]
    known_at_ns: int


class RepairStore:
    def __init__(self, path: str | Path, journal_path: str | Path, *, readonly=False):
        self.path, self.journal_path = Path(path).resolve(), Path(journal_path).resolve()
        self.lock = None
        if readonly:
            self.db = sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)
            self.db.row_factory = sqlite3.Row
            try:
                if self.meta("schema_version") != "repair-1":
                    raise IntegrityError("not a repair catalog")
                with Journal(self.journal_path, readonly=True) as source:
                    if self.meta("dataset_id") != source.dataset_id:
                        raise IntegrityError("catalog belongs to a different dataset")
            except Exception:
                self.close()
                raise
            return
        if self.path == self.journal_path or (
            self.path.exists() and self.path.samefile(self.journal_path)
        ):
            raise ValueError("repair catalog must not be the original journal")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = self.path.with_suffix(self.path.suffix + ".lockfile").open("a+b")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            raise RuntimeError("another repair writer owns this catalog") from None
        self.db = sqlite3.connect(self.path)
        self.db.row_factory = sqlite3.Row
        # Refuse an existing journal/foreign database before applying any DDL or PRAGMA mutation.
        if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table'").fetchone():
            try:
                version = self.db.execute(
                    "SELECT value FROM metadata WHERE key='schema_version'"
                ).fetchone()
                if version is None or version[0] != "repair-1":
                    raise IntegrityError("not a repair catalog")
            except (sqlite3.DatabaseError, IntegrityError):
                self.close()
                raise IntegrityError("not a repair catalog") from None
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS sources(
                id INTEGER PRIMARY KEY, kind TEXT NOT NULL, uri TEXT NOT NULL,
                fetched_ns INTEGER NOT NULL, known_ns INTEGER NOT NULL,
                resolution INTEGER NOT NULL, status INTEGER NOT NULL,
                payload BLOB NOT NULL, digest TEXT NOT NULL,
                gap_id INTEGER, first_id INTEGER, stop_id INTEGER, page_limit INTEGER,
                processed INTEGER NOT NULL DEFAULT 0, error TEXT);
            CREATE TABLE IF NOT EXISTS facts(
                trade_id INTEGER PRIMARY KEY, payload BLOB NOT NULL, digest TEXT NOT NULL,
                resolution INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS provenance(
                source_id INTEGER NOT NULL REFERENCES sources(id), trade_id INTEGER NOT NULL REFERENCES facts(trade_id),
                payload BLOB NOT NULL, digest TEXT NOT NULL,
                PRIMARY KEY(source_id,trade_id));
            CREATE INDEX IF NOT EXISTS provenance_trade ON provenance(trade_id);
            CREATE TABLE IF NOT EXISTS gaps(
                id INTEGER PRIMARY KEY, first_id INTEGER NOT NULL, last_id INTEGER NOT NULL,
                detected_seq INTEGER NOT NULL, detected_ns INTEGER NOT NULL,
                state TEXT NOT NULL, cursor INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_ns INTEGER NOT NULL DEFAULT 0, error TEXT,
                UNIQUE(first_id,last_id));
            CREATE TABLE IF NOT EXISTS boundaries(
                event_seq INTEGER PRIMARY KEY, scope TEXT NOT NULL, state TEXT NOT NULL, reason TEXT NOT NULL);
        """)
        with self.db:
            for key, value in (
                ("schema_version", "repair-1"),
                ("cursor", "0"),
                ("chain", ZERO_HASH),
                ("revision", "0"),
            ):
                self.db.execute("INSERT OR IGNORE INTO metadata VALUES(?,?)", (key, value))
        if self.meta("schema_version") != "repair-1":
            self.close()
            raise IntegrityError("not a repair catalog")
        with Journal(self.journal_path, readonly=True) as source:
            existing = self.meta("dataset_id")
            if existing is not None and existing != source.dataset_id:
                self.close()
                raise IntegrityError("catalog belongs to a different dataset")
            with self.db:
                self.db.execute(
                    "INSERT OR IGNORE INTO metadata VALUES('dataset_id',?)", (source.dataset_id,)
                )

    def meta(self, key):
        row = self.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def _set(self, key, value):
        self.db.execute("INSERT OR REPLACE INTO metadata VALUES(?,?)", (key, value))

    @property
    def spec(self) -> Instrument:
        if self.meta("instrument") is None:
            raise IntegrityError("instrument metadata has not been observed")
        return msgspec.msgpack.decode(self.meta("instrument"), type=Instrument)

    def _source(
        self,
        kind,
        uri,
        fetched_ns,
        resolution,
        status,
        payload,
        *,
        gap_id=None,
        first_id=None,
        stop_id=None,
        page_limit=None,
        known_ns=None,
    ):
        return self.db.execute(
            "INSERT INTO sources(kind,uri,fetched_ns,known_ns,resolution,status,payload,digest,gap_id,first_id,stop_id,page_limit) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                kind,
                uri,
                fetched_ns,
                known_ns if known_ns is not None else max(fetched_ns, time.time_ns()),
                resolution,
                status,
                payload,
                hashlib.sha256(payload).hexdigest(),
                gap_id,
                first_id,
                stop_id,
                page_limit,
            ),
        ).lastrowid

    def _fact(self, trade, source_id, resolution):
        blob = encode(trade)
        digest = hashlib.sha256(blob).hexdigest()
        existing = self.db.execute(
            "SELECT * FROM facts WHERE trade_id=?", (trade.trade_id,)
        ).fetchone()
        if existing:
            if hashlib.sha256(existing["payload"]).hexdigest() != existing["digest"]:
                raise IntegrityError("stored fact checksum mismatch")
            previous = msgspec.msgpack.decode(existing["payload"], type=Trade)
            if not compatible(previous, existing["resolution"], trade, resolution):
                raise IntegrityError(f"conflicting_trade:{trade.trade_id}")
        else:
            self.db.execute(
                "INSERT INTO facts VALUES(?,?,?,?)", (trade.trade_id, blob, digest, resolution)
            )
        self.db.execute(
            "INSERT OR IGNORE INTO provenance VALUES(?,?,?,?)",
            (source_id, trade.trade_id, blob, digest),
        )
        return existing is None

    def sync_observed(self):
        """Incrementally import committed observed facts/boundaries, never write upstream."""
        cursor = int(self.meta("cursor"))
        with Journal(self.journal_path, readonly=True) as source:
            if source.dataset_id != self.meta("dataset_id") or cursor > source.frontier:
                raise IntegrityError("original journal identity/frontier changed")
            if cursor:
                row = source.db.execute(
                    "SELECT chain_hash FROM events WHERE market_seq=?", (cursor,)
                ).fetchone()
                if row[0] != self.meta("chain"):
                    raise IntegrityError("original imported prefix changed")
            with self.db:
                previous_id = self.meta("last_observed_trade")
                previous_id = int(previous_id) if previous_id is not None else None
                imported = 0
                for record in source.records(cursor):
                    event, data = record.event, record.event.payload
                    if isinstance(data, Instrument):
                        if self.meta("instrument") not in (None, encode(data)):
                            raise IntegrityError("instrument changed")
                        self._set("instrument", encode(data))
                    elif isinstance(data, Trade):
                        if previous_id is not None and data.trade_id > previous_id + 1:
                            first, last = previous_id + 1, data.trade_id - 1
                            self.db.execute(
                                "INSERT OR IGNORE INTO gaps(first_id,last_id,detected_seq,detected_ns,state,cursor) VALUES(?,?,?,?,'OPEN',?)",
                                (first, last, event.market_seq, event.normalized_wall_ns, first),
                            )
                        if previous_id is not None and data.trade_id <= previous_id:
                            raise IntegrityError("observed trade IDs regress")
                        sid = self._source(
                            "observed",
                            f"journal:{source.dataset_id}:{event.market_seq}",
                            event.receive_wall_ns,
                            event.source_resolution_ns or 1_000_000,
                            200,
                            encode(data),
                            known_ns=event.normalized_wall_ns,
                        )
                        self._fact(data, sid, event.source_resolution_ns or 1_000_000)
                        self.db.execute("UPDATE sources SET processed=1 WHERE id=?", (sid,))
                        self._set("last_observed_trade", str(data.trade_id))
                        previous_id = data.trade_id
                        imported += 1
                    elif isinstance(data, Quality):
                        self.db.execute(
                            "INSERT INTO boundaries VALUES(?,?,?,?)",
                            (event.market_seq, data.scope, data.state, data.reason),
                        )
                    self._set("cursor", str(event.market_seq))
                    self._set("chain", record.chain_hash)
                if imported:
                    self._set("revision", str(int(self.meta("revision")) + 1))
                self._refresh_gaps()
        return imported

    def gaps(self):
        return [dict(row) for row in self.db.execute("SELECT * FROM gaps ORDER BY first_id")]

    def _missing_cursor(self, first, last):
        expected = first
        for row in self.db.execute(
            "SELECT trade_id FROM facts WHERE trade_id BETWEEN ? AND ? ORDER BY trade_id",
            (first, last),
        ):
            if row[0] != expected:
                break
            expected += 1
        return expected

    def _refresh_gaps(self):
        for gap in self.gaps():
            if gap["state"] == "CONFLICT":
                continue
            cursor = self._missing_cursor(gap["first_id"], gap["last_id"])
            state = (
                "REPAIRED"
                if cursor > gap["last_id"]
                else ("REPAIRING" if cursor > gap["first_id"] else "OPEN")
            )
            self.db.execute(
                "UPDATE gaps SET cursor=?,state=? WHERE id=?", (cursor, state, gap["id"])
            )

    def capture_reply(self, gap_id: int, first_id: int, limit: int, reply: Reply, *, stop_id: int):
        """Durable raw reply before parsing. Test hooks can kill the process after this."""
        if reply.source_kind not in ("historical-rest", "historical-archive"):
            raise ValueError("unsupported historical source kind")
        with self.db:
            if reply.status in (418, 429):
                self._set(
                    "endpoint_blocked_until_ns",
                    str(
                        max(
                            int(self.meta("endpoint_blocked_until_ns") or 0),
                            reply.retry_after_ns or reply.fetched_ns + 60_000_000_000,
                        )
                    ),
                )
            return self._source(
                reply.source_kind,
                reply.url,
                reply.fetched_ns,
                reply.resolution_ns,
                reply.status,
                reply.body,
                gap_id=gap_id,
                first_id=first_id,
                stop_id=stop_id,
                page_limit=limit,
            )

    def defer(self, gap_id, error, *, conflict=False, until_ns=0):
        with self.db:
            gap = self.db.execute("SELECT attempts FROM gaps WHERE id=?", (gap_id,)).fetchone()
            delay = min(300, 5 * 2 ** min(gap[0], 6))
            self.db.execute(
                "UPDATE gaps SET state=?,attempts=attempts+1,error=?,next_attempt_ns=? WHERE id=?",
                (
                    "CONFLICT" if conflict else "OPEN",
                    str(error),
                    max(until_ns, time.time_ns() + delay * 1_000_000_000),
                    gap_id,
                ),
            )

    def process_reply(self, source_id):
        row = self.db.execute("SELECT * FROM sources WHERE id=?", (source_id,)).fetchone()
        if row is None or row["kind"] not in ("historical-rest", "historical-archive"):
            raise ValueError("unknown historical reply")
        if row["processed"]:
            return {"inserted": 0, "duplicates": 0, "already_processed": True}
        if hashlib.sha256(row["payload"]).hexdigest() != row["digest"]:
            raise IntegrityError("raw historical reply checksum mismatch")
        try:
            if row["status"] != 200:
                raise ValueError(f"historical_http_status:{row['status']}")
            parser = parse_archive_trades if row["kind"] == "historical-archive" else parse_trades
            trades = parser(row["payload"], self.spec, row["resolution"])
            if not trades or len(trades) > row["page_limit"]:
                raise ValueError("empty or oversized page; cannot advance")
            expected = row["first_id"]
            for trade in trades:
                if trade.trade_id != expected or trade.trade_id > row["stop_id"]:
                    raise ValueError("historical page contains an unresolved ID gap or exceeds job")
                expected += 1
            with self.db:
                inserted = sum(self._fact(t, source_id, row["resolution"]) for t in trades)
                self.db.execute(
                    "UPDATE sources SET processed=1,known_ns=? WHERE id=?",
                    (max(time.time_ns(), row["fetched_ns"]), source_id),
                )
                self._refresh_gaps()
                self.db.execute(
                    "UPDATE gaps SET error=NULL,next_attempt_ns=0 WHERE id=?", (row["gap_id"],)
                )
                self._set("revision", str(int(self.meta("revision")) + 1))
            return {
                "inserted": inserted,
                "duplicates": len(trades) - inserted,
                "already_processed": False,
            }
        except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
            # The fact/provenance/cursor transaction rolls back; keep the rejected raw evidence.
            with self.db:
                self.db.execute(
                    "UPDATE sources SET processed=1,error=?,known_ns=? WHERE id=?",
                    (str(exc), max(time.time_ns(), row["fetched_ns"]), source_id),
                )
            self.defer(row["gap_id"], exc, conflict="conflicting_trade" in str(exc))
            raise

    def recover_pending(self):
        total = 0
        rows = self.db.execute(
            "SELECT id FROM sources WHERE processed=0 AND kind IN ('historical-rest','historical-archive') ORDER BY id"
        ).fetchall()
        for row in rows:
            try:
                total += self.process_reply(row[0])["inserted"]
            except (ValueError, KeyError, TypeError, ArithmeticError):
                status = self.db.execute(
                    "SELECT processed,error FROM sources WHERE id=?", (row[0],)
                ).fetchone()
                if not status[0] or not status[1]:
                    raise
        return total

    def research(self, first_id, last_id, *, view="repaired", known_by_ns=None):
        if view not in ("observed", "repaired") or first_id < 0 or last_id < first_id:
            raise ValueError("explicit observed/repaired view and bounded ID range required")
        sql = """SELECT p.trade_id,p.payload,p.digest,s.id,s.known_ns,s.kind FROM provenance p
                 JOIN sources s ON s.id=p.source_id WHERE s.processed=1 AND s.error IS NULL
                 AND p.trade_id BETWEEN ? AND ?"""
        params = [first_id, last_id]
        if view == "observed":
            sql += " AND s.kind='observed'"
        if known_by_ns is not None:
            sql += " AND s.known_ns<=?"
            params.append(known_by_ns)
        sql += " ORDER BY p.trade_id,s.known_ns,s.id"
        grouped = {}
        for row in self.db.execute(sql, params):
            if hashlib.sha256(row["payload"]).hexdigest() != row["digest"]:
                raise IntegrityError("provenance checksum mismatch")
            grouped.setdefault(row["trade_id"], []).append(row)
        for rows in grouped.values():
            row = rows[0]
            yield ResearchTrade(
                view,
                False,
                self.meta("dataset_id"),
                int(self.meta("revision")),
                msgspec.msgpack.decode(row["payload"], type=Trade),
                tuple(r["id"] for r in rows),
                row["known_ns"],
            )

    def continuity(self, first_id, last_id, *, view="repaired", known_by_ns=None):
        intervals, missing = [], []
        expected = first_id
        for record in self.research(first_id, last_id, view=view, known_by_ns=known_by_ns):
            tid = record.payload.trade_id
            if tid > expected:
                missing.append([expected, tid - 1])
            if intervals and intervals[-1][1] + 1 == tid:
                intervals[-1][1] = tid
            else:
                intervals.append([tid, tid])
            expected = tid + 1
        if expected <= last_id:
            missing.append([expected, last_id])
        conflicts = 0
        if view == "repaired":
            conflicts = self.db.execute(
                "SELECT COUNT(DISTINCT g.id) FROM gaps g JOIN sources s ON s.gap_id=g.id "
                "WHERE s.error LIKE 'conflicting_trade:%' AND g.first_id<=? AND g.last_id>=? AND s.known_ns<=?",
                (last_id, first_id, known_by_ns if known_by_ns is not None else 2**63 - 1),
            ).fetchone()[0]
        return {
            "view": view,
            "first_id": first_id,
            "last_id": last_id,
            "known_by_ns": known_by_ns,
            "continuous_intervals": intervals,
            "missing_ranges": missing,
            "complete": not missing and not conflicts,
            "conflicted_jobs": conflicts,
            "execution_allowed": False,
            "scope": "raw trade ID coverage only; not L2 path, freshness, account reconciliation or trade permission",
        }

    def close(self):
        self.db.close()
        if self.lock:
            self.lock.close()

    def verify_integrity(self):
        counts = {}
        for table in ("sources", "facts", "provenance"):
            count = 0
            for row in self.db.execute(f"SELECT payload,digest FROM {table}"):
                if hashlib.sha256(row["payload"]).hexdigest() != row["digest"]:
                    raise IntegrityError(f"{table} checksum mismatch")
                count += 1
            counts[table] = count
        if self.db.execute("PRAGMA foreign_key_check").fetchone():
            raise IntegrityError("catalog provenance has dangling references")
        if self.db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise IntegrityError("catalog SQLite integrity check failed")
        return {"checksums_verified": True, "sqlite_integrity": "ok", **counts}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
