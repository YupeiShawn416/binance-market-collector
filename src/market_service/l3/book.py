"""Visible-order reducer, Kraken queue checksum and independent replay hashes."""

import hashlib
import zlib
from collections import defaultdict

import msgspec
from order_book import OrderBook

from ..book import IntegrityError, Projection, units
from ..contracts import Instrument, Quality, encode
from .contracts import Order, OrderBatch, OrderSnapshot


class L3Projection:
    def __init__(self):
        self.orders = {}
        self.book = OrderBook(max_depth=0)
        self.instrument = None
        self.dataset_id = None
        self.venue = None
        self.data_origin = None
        self.seq, self.epoch, self.depth = 0, 0, 0
        self.source_sequence = None
        self.coverage = (None, None)
        self.queue_order_known = True
        self.session_id = ""
        self.quality = {"connection": "DISCONNECTED", "book": "SYNCING"}
        self.book_hash = self._hash()

    @property
    def ready(self):
        return all(self.quality.get(k) == "LIVE" for k in ("connection", "book"))

    def _queues(self):
        sides = {"buy": defaultdict(list), "sell": defaultdict(list)}
        for order in self.orders.values():
            sides[order.side][order.price_ticks].append(order)
        for levels in sides.values():
            for queue in levels.values():
                # Stable equal-time ordering retains snapshot/message order.
                queue.sort(key=lambda order: order.priority_time_ns)
        return sides

    def _hash(self):
        sides = self._queues()
        rows = tuple(
            (
                side,
                tuple(
                    (price, tuple(sides[side][price]))
                    for price in sorted(sides[side], reverse=side == "buy")
                ),
            )
            for side in ("buy", "sell")
        )
        return hashlib.sha256(
            encode((rows, self.depth, self.coverage, self.queue_order_known))
        ).hexdigest()

    def _rebuild(self):
        sides = self._queues()
        for name, side in (("bids", "buy"), ("asks", "sell")):
            prices = sorted(sides[side], reverse=side == "buy")
            retained = set(prices[: self.depth] if self.depth else prices)
            for price, queue in sides[side].items():
                if price not in retained:
                    for order in queue:
                        del self.orders[order.order_id]
            setattr(
                self.book,
                name,
                {price: sum(o.quantity_units for o in sides[side][price]) for price in retained},
            )
        Projection._check_spread(self.book)

    def checksum(self):
        sides, chunks = self._queues(), []
        for side in ("sell", "buy"):
            for price in sorted(sides[side], reverse=side == "buy")[:10]:
                for order in sides[side][price]:
                    chunks.extend(
                        (
                            order.price_text.replace(".", "").lstrip("0"),
                            order.quantity_text.replace(".", "").lstrip("0"),
                        )
                    )
        return zlib.crc32("".join(chunks).encode()) & 0xFFFFFFFF

    def _validate_order(self, order, *, deletion=False):
        if not self.instrument or not order.order_id or order.price_ticks <= 0:
            raise IntegrityError("invalid L3 order identity/price/specification")
        if order.quantity_units < 0 or (not deletion and order.quantity_units == 0):
            raise IntegrityError("invalid L3 order quantity")
        if units(order.price_text, self.instrument.price_tick) != order.price_ticks:
            raise IntegrityError("L3 raw price text mismatch")
        if units(order.quantity_text, self.instrument.quantity_unit) != order.quantity_units:
            raise IntegrityError("L3 raw quantity text mismatch")

    def apply(self, event, *, verify=True):
        if event.schema_version != 2 or event.market_seq != self.seq + 1:
            raise IntegrityError("L3 contract version or local event sequence mismatch")
        if self.dataset_id not in (None, event.dataset_id):
            raise IntegrityError("L3 dataset changed")
        if self.venue not in (None, event.venue):
            raise IntegrityError("L3 venue changed inside dataset")
        if self.data_origin not in (None, event.data_origin):
            raise IntegrityError("L3 data origin changed inside dataset")
        data = event.payload
        if isinstance(data, (OrderSnapshot, OrderBatch)):
            if event.venue == "KRAKEN" and data.checksum is None:
                raise IntegrityError("Kraken L3 requires its exchange CRC")
            if event.venue == "COINBASE" and data.source_sequence is None:
                raise IntegrityError("Coinbase L3 requires its exchange sequence")
        if isinstance(data, Instrument):
            if self.instrument not in (None, data):
                raise IntegrityError("L3 precision specification changed")
            self.instrument = data
        elif isinstance(data, Quality):
            self.quality[data.scope] = data.state
        elif isinstance(data, OrderSnapshot):
            self.orders = {}
            self.depth = data.depth
            self.source_sequence = data.source_sequence
            self.coverage = (data.known_bid_floor, data.known_ask_ceiling)
            self.queue_order_known = data.queue_order_known
            for order in data.orders:
                self._validate_order(order)
                if order.order_id in self.orders:
                    raise IntegrityError("duplicate L3 snapshot order")
                self.orders[order.order_id] = order
            self._rebuild()
            if data.checksum is not None and self.checksum() != data.checksum:
                raise IntegrityError("L3 snapshot exchange CRC mismatch")
            self.quality["book"] = "LIVE"
        elif isinstance(data, OrderBatch):
            if not self.ready or event.book_epoch != self.epoch:
                raise IntegrityError("L3 update without healthy same-epoch snapshot")
            if (
                self.source_sequence is not None
                and data.source_sequence != self.source_sequence + 1
            ):
                raise IntegrityError("L3 exchange source sequence gap")
            for change in data.changes:
                order = change.order
                self._validate_order(order, deletion=change.action == "delete")
                previous = self.orders.get(order.order_id)
                if change.action == "add":
                    if previous is not None:
                        raise IntegrityError("duplicate L3 add")
                    self.orders[order.order_id] = order
                else:
                    if previous is None or (previous.side, previous.price_ticks) != (
                        order.side,
                        order.price_ticks,
                    ):
                        raise IntegrityError("L3 update references missing or relocated order")
                    if change.action == "delete":
                        del self.orders[order.order_id]
                    else:
                        self.orders[order.order_id] = order
            # One exchange message is atomic: truncate and validate only after all changes.
            self._rebuild()
            if data.checksum is not None and self.checksum() != data.checksum:
                raise IntegrityError("L3 update exchange CRC mismatch")
            self.source_sequence = data.source_sequence
        else:
            raise IntegrityError("unsupported L3 payload")
        self.book_hash = self._hash()
        self.dataset_id, self.seq = event.dataset_id, event.market_seq
        self.venue = event.venue
        self.data_origin = event.data_origin
        self.epoch, self.session_id = event.book_epoch, event.session_id
        if verify and self.book_hash != event.book_hash:
            raise IntegrityError("L3 order/queue replay hash mismatch")

    def checkpoint(self):
        return encode(
            {
                "v": 2,
                "orders": tuple(self.orders.values()),
                "instrument": self.instrument,
                "dataset_id": self.dataset_id,
                "venue": self.venue,
                "data_origin": self.data_origin,
                "seq": self.seq,
                "epoch": self.epoch,
                "depth": self.depth,
                "source_sequence": self.source_sequence,
                "coverage": self.coverage,
                "queue_order_known": self.queue_order_known,
                "session_id": self.session_id,
                "quality": self.quality,
                "book_hash": self.book_hash,
            }
        )

    @classmethod
    def restore(cls, blob):
        data = msgspec.msgpack.decode(blob)
        if data["v"] != 2:
            raise IntegrityError("unsupported L3 checkpoint")
        obj = cls()
        obj.instrument = (
            msgspec.convert(data["instrument"], Instrument) if data["instrument"] else None
        )
        obj.orders = {o.order_id: o for o in msgspec.convert(data["orders"], tuple[Order, ...])}
        for key in (
            "dataset_id",
            "venue",
            "data_origin",
            "seq",
            "epoch",
            "depth",
            "session_id",
            "quality",
        ):
            setattr(obj, key, data[key])
        obj.source_sequence = data["source_sequence"]
        obj.coverage = tuple(data["coverage"])
        obj.queue_order_known = data["queue_order_known"]
        if obj.depth or obj.orders:
            obj._rebuild()
        obj.book_hash = obj._hash()
        if obj.book_hash != data["book_hash"]:
            raise IntegrityError("L3 checkpoint book hash mismatch")
        return obj
