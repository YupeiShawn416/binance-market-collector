"""Orchestrates raw input -> normalization -> atomic journal append.

A failed append poisons this instance. Restart from the journal rather than using
an in-memory state that advanced past its durable frontier.
"""

from __future__ import annotations

import time
import uuid

import msgspec

from .binance import ResyncRequired, StrictBinance, instrument_from_metadata
from .contracts import PARSER_VERSION, SCHEMA_VERSION, Event, Quality, RawFrame
from .store import ZERO_HASH, Journal


class MarketService:
    def __init__(
        self, journal: Journal, *, checkpoint_every: int = 250, record_trace: bool = False
    ):
        self.journal = journal
        self.projection = journal.recover()
        self.epoch = self.projection.epoch
        self.boot_id = str(uuid.uuid4())
        self.checkpoint_every = checkpoint_every
        self.adapter = (
            StrictBinance(self.projection.instrument, self.projection, journal.trade)
            if self.projection.instrument
            else None
        )
        self.poisoned = False
        self.record_trace = record_trace
        self.trace: list[tuple[int, str, str]] = []
        row = journal.db.execute(
            "SELECT chain_hash FROM events ORDER BY market_seq DESC LIMIT 1"
        ).fetchone()
        self.final_chain = row[0] if row else ZERO_HASH

    def capture(
        self, kind: str, payload: bytes, session_id: str, *, wall_ns=None, mono_ns=None
    ) -> RawFrame:
        if self.poisoned:
            raise RuntimeError("restart required after failed commit")
        return self.journal.raw(
            session_id,
            kind,
            payload,
            wall_ns if wall_ns is not None else time.time_ns(),
            mono_ns if mono_ns is not None else time.monotonic_ns(),
            self.boot_id,
        )

    async def recover_pending(self):
        # Durable receive records that were queued at crash time must not be deleted.
        for frame in list(self.journal.pending()):
            await self.process(frame)

    async def control(self, action: str, session_id: str, reason: str = ""):
        raw = self.capture(
            "control", msgspec.json.encode({"action": action, "reason": reason}), session_id
        )
        return await self.process(raw)

    async def process(self, frame: RawFrame) -> bool:
        """Return True when the transport must fetch a fresh snapshot/reconnect."""
        if self.poisoned:
            raise RuntimeError("restart required")
        resync = False
        try:
            if frame.kind == "metadata":
                spec = instrument_from_metadata(frame.payload)
                if self.projection.instrument and self.projection.instrument != spec:
                    raise ValueError("spec changed; use a new dataset")
                emissions = [] if self.projection.instrument else [(spec, None, None)]
            elif frame.kind == "control":
                data = msgspec.json.decode(frame.payload)
                if data["action"] == "start":
                    self.epoch += 1
                    if self.adapter:
                        self.adapter.reset_connection()
                    emissions = [
                        (Quality("connection", "LIVE", "websocket_connected"), None, None),
                        (
                            Quality("book", "SYNCING", "new_connection_requires_snapshot"),
                            None,
                            None,
                        ),
                        (Quality("trades", "SYNCING", "new_connection_trade_boundary"), None, None),
                    ]
                elif data["action"] == "stop":
                    emissions = [
                        (Quality("connection", "DISCONNECTED", data["reason"]), None, None),
                        (Quality("book", "INVALID", "connection_lost"), None, None),
                        (Quality("trades", "INVALID", "connection_lost"), None, None),
                    ]
                else:
                    raise ValueError("unknown control action")
            else:
                if self.adapter is None:
                    raise ValueError("metadata required before market input")
                try:
                    emissions = await self.adapter.normalize(frame)
                except (ResyncRequired, ValueError, KeyError, ArithmeticError) as exc:
                    self.epoch += 1
                    self.adapter.reset_connection()
                    emissions = [
                        (Quality("book", "INVALID", str(exc)), None, None),
                        (Quality("trades", "INVALID", "resync_boundary"), None, None),
                    ]
                    resync = True
            events = []
            start_seq = self.projection.seq
            for payload, expected_book_hash, exchange_ns in emissions:
                event = Event(
                    schema_version=SCHEMA_VERSION,
                    dataset_id=self.journal.dataset_id,
                    market_seq=self.projection.seq + 1,
                    venue="BINANCE",
                    instrument="BTC-USDT",
                    session_id=frame.session_id,
                    book_epoch=self.epoch,
                    raw_seq=frame.raw_seq,
                    receive_wall_ns=frame.receive_wall_ns,
                    receive_mono_ns=frame.receive_mono_ns,
                    boot_id=frame.boot_id,
                    exchange_time_ns=exchange_ns,
                    source_resolution_ns=1_000_000 if exchange_ns is not None else None,
                    parser_version=PARSER_VERSION,
                    normalized_wall_ns=time.time_ns(),
                    normalized_mono_ns=time.monotonic_ns(),
                    normalizer_instance_id=self.boot_id,
                    payload=payload,
                    book_hash="",
                )
                self.projection.apply(event, verify=False)
                if (
                    expected_book_hash is not None
                    and expected_book_hash != self.projection.book_hash
                ):
                    raise ValueError("upstream/native book differs from canonical reducer")
                event = msgspec.structs.replace(event, book_hash=self.projection.book_hash)
                events.append(event)
            checkpoint = (
                self.projection.checkpoint()
                if events
                and start_seq // self.checkpoint_every
                != self.projection.seq // self.checkpoint_every
                else None
            )
            records = self.journal.commit(frame.raw_seq, events, checkpoint)
            if records:
                self.final_chain = records[-1].chain_hash
            if self.record_trace:
                self.trace.extend(
                    (r.event.market_seq, r.event_hash, r.event.book_hash) for r in records
                )
            if frame.kind == "metadata" and self.adapter is None:
                self.adapter = StrictBinance(
                    self.projection.instrument, self.projection, self.journal.trade
                )
            return resync
        except BaseException:
            self.poisoned = True
            raise
