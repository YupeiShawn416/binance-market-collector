"""Exchange-independent reducer using the real orderbook C extension."""

from __future__ import annotations

import hashlib
from decimal import Decimal

import msgspec
from order_book import OrderBook

from .contracts import Delta, Event, Instrument, Quality, Snapshot, Trade, encode


class IntegrityError(ValueError):
    """A journal, cursor, or book cannot be used as a trusted continuation."""


def units(text: str, quantum: str) -> int:
    # Decimal division can round under a user-modified context. Integer ratios cannot.
    n, d = Decimal(text).as_integer_ratio()
    qn, qd = Decimal(quantum).as_integer_ratio()
    if qn <= 0:
        raise ValueError("quantum must be positive")
    value, remainder = divmod(n * qd, d * qn)
    if remainder or value < 0:
        raise ValueError(f"unaligned/nonpositive value: {text} / {quantum}")
    return value


def decimal_text(value: int, quantum: str) -> str:
    parts = Decimal(quantum).as_tuple()
    coefficient = int("".join(map(str, parts.digits))) * value
    if parts.exponent >= 0:
        return str(coefficient) + "0" * parts.exponent
    places = -parts.exponent
    text = str(coefficient).zfill(places + 1)
    return text[:-places] + "." + text[-places:]


def levels_hash(bids, asks) -> str:
    return hashlib.sha256(encode((tuple(bids), tuple(asks)))).hexdigest()


class Projection:
    """Consumer-owned book, quality and cursor. No exchange or network dependency."""

    def __init__(self):
        self.book = OrderBook(max_depth=0)
        self.instrument: Instrument | None = None
        self.dataset_id: str | None = None
        self.seq = 0
        self.epoch = 0
        self.snapshot_epoch = None
        self.session_id = ""
        self.last_update_id: int | None = None
        self.last_trade_id: int | None = None
        self.quality = {"connection": "DISCONNECTED", "book": "SYNCING", "trades": "SYNCING"}
        self.coverage = (None, None, 0)
        self.book_hash = levels_hash((), ())

    @property
    def ready(self) -> bool:
        return all(state == "LIVE" for state in self.quality.values())

    def apply(self, event: Event, *, verify: bool = True) -> None:
        if event.schema_version != 1:
            raise IntegrityError("schema version mismatch")
        if self.dataset_id is not None and self.dataset_id != event.dataset_id:
            raise IntegrityError("dataset changed; a cursor cannot cross datasets")
        if event.market_seq != self.seq + 1:
            raise IntegrityError(f"event gap: expected {self.seq + 1}, got {event.market_seq}")
        data = event.payload
        if isinstance(data, Instrument):
            if self.instrument and self.instrument != data:
                raise IntegrityError("instrument spec changed; start a new dataset")
            self.instrument = data
        elif isinstance(data, Snapshot):
            self._validate(data.bids, data.asks)
            candidate = OrderBook(max_depth=0)
            candidate.bids = dict(data.bids)
            candidate.asks = dict(data.asks)
            self._check_spread(candidate)
            self.book = candidate
            self.epoch = event.book_epoch
            self.snapshot_epoch = event.book_epoch
            self.last_update_id = data.last_update_id
            self.coverage = (data.known_bid_floor, data.known_ask_ceiling, data.source_depth)
            self.quality["book"] = "SYNCING"
            self._hash()
        elif isinstance(data, Delta):
            if event.book_epoch != self.epoch or self.last_update_id is None:
                raise IntegrityError("delta without same-epoch snapshot")
            if not data.first_update_id <= self.last_update_id + 1 <= data.last_update_id:
                raise IntegrityError("non-contiguous canonical delta")
            self._validate(data.bids, data.asks, allow_zero=True)
            for side, levels in ((self.book.bids, data.bids), (self.book.asks, data.asks)):
                for price, qty in levels:
                    if qty:
                        side[price] = qty
                    elif price in side:
                        del side[price]
            self._check_spread(self.book)
            self.last_update_id = data.last_update_id
            self._hash()
        elif isinstance(data, Trade):
            if data.price_ticks <= 0 or data.quantity_units <= 0:
                raise IntegrityError("nonpositive trade")
            if self.last_trade_id is not None and data.trade_id <= self.last_trade_id:
                raise IntegrityError("duplicate/regressing canonical trade")
            self.last_trade_id = data.trade_id
        elif isinstance(data, Quality):
            self.quality[data.scope] = data.state
        self.epoch = event.book_epoch
        self.dataset_id = event.dataset_id
        self.seq = event.market_seq
        self.session_id = event.session_id
        if verify and event.book_hash != self.book_hash:
            raise IntegrityError(f"book hash mismatch at {self.seq}")

    @staticmethod
    def _validate(bids, asks, *, allow_zero=False):
        for side in (bids, asks):
            if len({p for p, _ in side}) != len(side):
                raise IntegrityError("duplicate price in batch")
            for price, qty in side:
                if price <= 0 or qty < 0 or (not allow_zero and qty == 0):
                    raise IntegrityError("invalid level")

    @staticmethod
    def _check_spread(book):
        if len(book.bids) and len(book.asks) and book.bids.index(0)[0] >= book.asks.index(0)[0]:
            raise IntegrityError("crossed or locked book")

    def _hash(self):
        self.book_hash = levels_hash(self.book.bids.to_list(), self.book.asks.to_list())

    def checkpoint(self) -> bytes:
        return encode(
            {
                "v": 1,
                "dataset_id": self.dataset_id,
                "seq": self.seq,
                "epoch": self.epoch,
                "snapshot_epoch": self.snapshot_epoch,
                "session_id": self.session_id,
                "last_update_id": self.last_update_id,
                "last_trade_id": self.last_trade_id,
                "quality": self.quality,
                "coverage": self.coverage,
                "instrument": self.instrument,
                "bids": self.book.bids.to_list(),
                "asks": self.book.asks.to_list(),
                "book_hash": self.book_hash,
            }
        )

    @classmethod
    def restore(cls, blob: bytes) -> Projection:
        data = msgspec.msgpack.decode(blob)
        if data["v"] != 1:
            raise IntegrityError("unsupported checkpoint")
        obj = cls()
        for name in (
            "dataset_id",
            "seq",
            "epoch",
            "session_id",
            "last_update_id",
            "last_trade_id",
            "quality",
        ):
            setattr(obj, name, data[name])
        obj.snapshot_epoch = data.get("snapshot_epoch", data["epoch"])
        obj.coverage = tuple(data["coverage"])
        if data["instrument"] is not None:
            obj.instrument = msgspec.convert(data["instrument"], Instrument)
        obj.book.bids = dict(data["bids"])
        obj.book.asks = dict(data["asks"])
        obj._hash()
        if obj.book_hash != data["book_hash"]:
            raise IntegrityError("checkpoint book hash mismatch")
        return obj
