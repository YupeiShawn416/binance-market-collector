"""L3 uses the existing atomic journal, recovery, consumer and socket protocol machinery."""

import time
import uuid
from collections import Counter

import msgspec

from ..book import IntegrityError
from ..contracts import Quality, encode
from ..source import Consumer, replay
from ..store import Journal
from .book import L3Projection
from .contracts import BatchV2, EventV2, RecordV2, SubscribeV2, decode_event
from .kraken import normalize


class L3Journal(Journal):
    schema_version = "2"
    event_decoder = staticmethod(decode_event)
    record_type = RecordV2

    def __init__(self, path, *, readonly=False):
        super().__init__(path, readonly=readonly)
        if readonly:
            origin = self.db.execute(
                "SELECT value FROM metadata WHERE key='data_origin'"
            ).fetchone()
            completed = self.db.execute(
                "SELECT value FROM metadata WHERE key='import_complete'"
            ).fetchone()
            if origin and origin[0] == "historical" and (not completed or completed[0] != "1"):
                self.close()
                raise IntegrityError(
                    "historical L3 import incomplete; restart into a fresh dataset"
                )

    def recover(self):
        # Full replay is intentional for this initial L3 implementation.
        projection = L3Projection()
        for record in self.records():
            projection.apply(record.event)
        return projection


class L3Consumer(Consumer):
    projection_type = L3Projection
    record_type = RecordV2
    journal_type = L3Journal
    subscribe_type = SubscribeV2
    batch_type = BatchV2


def verify(path):
    consumer = L3Consumer(record_trace=True)
    with L3Journal(path, readonly=True) as journal:
        raw_frames = journal.verify_raw()
        pending = journal.db.execute(
            "SELECT COUNT(*) FROM raw_frames WHERE processed=0"
        ).fetchone()[0]
    counts = Counter()
    source_crc_messages = 0
    for record in replay(path, consumer):
        counts[type(record.event.payload).__name__] += 1
        if getattr(record.event.payload, "checksum", None) is not None:
            source_crc_messages += 1
    return {
        "schema_version": 2,
        "dataset_id": consumer.projection.dataset_id,
        "events": consumer.projection.seq,
        "raw_frames": raw_frames,
        "pending_raw": pending,
        "counts": dict(counts),
        "final_order_state_hash": consumer.projection.book_hash,
        "final_event_chain": consumer.chain_hash,
        "quality": consumer.projection.quality,
        "checksums_and_replay_verified": True,
        "source_crc_messages": source_crc_messages,
        "queue_order_known": consumer.projection.queue_order_known,
        "last_exchange_sequence": consumer.projection.source_sequence,
        "scope": "local raw/event/order hashes verified; exchange CRC only when supplied; visible coverage only",
    }


class L3Service:
    def __init__(self, journal, *, depth=100, venue="KRAKEN"):
        if depth not in (0, 10, 100, 1000):
            raise ValueError("unsupported L3 depth")
        self.journal = journal
        self.projection = journal.recover()
        if self.projection.depth and self.projection.depth != depth:
            raise IntegrityError("L3 subscription depth changed; use a new dataset")
        self.depth = depth
        self.venue = venue
        origin = journal.db.execute("SELECT value FROM metadata WHERE key='data_origin'").fetchone()
        self.data_origin = origin[0] if origin else "unknown"
        self.boot_id = str(uuid.uuid4())
        self.poisoned = False

    def capture(self, kind, body, session_id, *, wall_ns=None, mono_ns=None):
        if self.poisoned:
            raise RuntimeError("restart L3 service after failed commit")
        return self.journal.raw(
            session_id,
            kind,
            body,
            time.time_ns() if wall_ns is None else wall_ns,
            time.monotonic_ns() if mono_ns is None else mono_ns,
            self.boot_id,
        )

    def emit(
        self, frame, emissions, *, epoch=None, historical_capture_ns=None, exchange_resolution_ns=1
    ):
        # Work on an isolated candidate. A failed CRC/batch never leaks a half-updated book.
        candidate = L3Projection.restore(self.projection.checkpoint())
        events = []
        for payload, exchange_ns in emissions:
            event = EventV2(
                2,
                self.journal.dataset_id,
                candidate.seq + 1,
                self.venue,
                "BTC-USD",
                frame.session_id,
                candidate.epoch if epoch is None else epoch,
                frame.raw_seq,
                frame.receive_wall_ns,
                frame.receive_mono_ns,
                frame.boot_id,
                exchange_ns,
                exchange_resolution_ns if exchange_ns is not None else None,
                self.venue.lower() + "-l3/1",
                time.time_ns(),
                time.monotonic_ns(),
                self.boot_id,
                payload,
                "",
                historical_capture_ns,
                self.data_origin,
            )
            candidate.apply(event, verify=False)
            events.append(msgspec.structs.replace(event, book_hash=candidate.book_hash))
        try:
            self.journal.commit(frame.raw_seq, events)
        except Exception:
            self.poisoned = True
            raise
        self.projection = candidate

    def process(self, frame):
        if frame.kind == "l3-spec":
            from ..contracts import Instrument

            spec = msgspec.msgpack.decode(frame.payload, type=Instrument)
            self.emit(frame, [(spec, None)])
            return False
        if frame.kind == "l3-control":
            data = msgspec.json.decode(frame.payload)
            connected = data["state"] == "LIVE"
            self.emit(
                frame,
                [
                    (Quality("connection", data["state"], data["reason"]), None),
                    (Quality("book", "SYNCING" if connected else "INVALID", data["reason"]), None),
                ],
                epoch=self.projection.epoch + int(connected),
            )
            return False
        if frame.kind != "kraken-l3-ws":
            raise IntegrityError("unknown L3 raw frame kind")
        try:
            emissions = normalize(frame.payload, self.projection.instrument, self.depth)
            self.emit(frame, emissions)
        except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
            if self.poisoned:
                raise
            self.emit(
                frame, [(Quality("book", "INVALID", type(exc).__name__ + ":" + str(exc)), None)]
            )
            return True
        return False

    def control(self, session_id, state, reason):
        frame = self.capture(
            "l3-control", msgspec.json.encode({"state": state, "reason": reason}), session_id
        )
        self.process(frame)

    def recover_pending(self):
        for frame in list(self.journal.pending()):
            self.process(frame)

    def install_spec(self, spec):
        if self.projection.instrument not in (None, spec):
            raise IntegrityError("L3 instrument precision changed")
        if self.projection.instrument is None:
            self.process(self.capture("l3-spec", encode(spec), "metadata"))
