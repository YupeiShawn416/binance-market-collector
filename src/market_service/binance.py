"""Thin strict adapter around Cryptofeed's actual Binance L2 engine.

No upstream files are patched. Snapshot parsing, depth mutation and update-ID
bridging are inherited. Transport and durable publication belong to other modules.
"""

from __future__ import annotations

import hashlib
from decimal import Decimal

import msgspec
from cryptofeed.defines import BINANCE, L2_BOOK, TRADES
from cryptofeed.exchanges.binance import Binance
from cryptofeed.symbols import Symbols

from .book import Projection, decimal_text, levels_hash, units
from .contracts import Delta, Instrument, Quality, RawFrame, Snapshot, Trade, encode


class ResyncRequired(ValueError):
    pass


def instrument_from_metadata(payload: bytes) -> Instrument:
    symbols = msgspec.json.decode(payload)["symbols"]
    item = next(x for x in symbols if x["symbol"] == "BTCUSDT")
    if item["status"] != "TRADING" or not item.get("isSpotTradingAllowed", True):
        raise ValueError("BTCUSDT spot is not trading")
    filters = {x["filterType"]: x for x in item["filters"]}
    fields = (
        "BTC-USDT",
        item["baseAsset"],
        item["quoteAsset"],
        filters["PRICE_FILTER"]["tickSize"],
        format(Decimal(1).scaleb(-item["baseAssetPrecision"]), "f"),
        filters["LOT_SIZE"]["stepSize"],
    )
    return Instrument(*fields, hashlib.sha256(encode(fields)).hexdigest())


class StrictBinance(Binance):
    websocket_channels = {**Binance.websocket_channels, TRADES: "trade"}

    def __init__(self, spec: Instrument, projection: Projection, trade_lookup):
        Symbols.set(
            BINANCE, {spec.symbol: "BTCUSDT"}, {"tick_size": {spec.symbol: spec.price_tick}}
        )
        super().__init__(symbols=[spec.symbol], channels=[L2_BOOK, TRADES], max_depth=0)
        self.spec = spec
        self.trade_lookup = trade_lookup
        self.symbol = spec.symbol
        self.emissions: list[tuple[object, str | None, int | None]] = []
        self.frame: RawFrame | None = None
        self.last_trade_id = projection.last_trade_id
        self.trade_live = projection.quality["trades"] == "LIVE"
        if (
            projection.last_update_id is not None
            and projection.snapshot_epoch == projection.epoch
            and projection.quality["book"] in ("LIVE", "SYNCING")
        ):
            # Rebuild the inherited decoder state before recovering durable raw tail.
            data = {
                "lastUpdateId": projection.last_update_id,
                "bids": [
                    [decimal_text(p, spec.price_tick), decimal_text(q, spec.quantity_unit)]
                    for p, q in projection.book.bids.to_list()
                ],
                "asks": [
                    [decimal_text(p, spec.price_tick), decimal_text(q, spec.quantity_unit)]
                    for p, q in projection.book.asks.to_list()
                ],
            }
            book = self._parse_snapshot(spec.symbol, msgspec.json.encode(data))
            book.delta = {} if projection.quality["book"] == "LIVE" else None
            self._l2_book[spec.symbol] = book
            self.last_update_id[spec.symbol] = projection.last_update_id
        self.book_live = projection.quality["book"] == "LIVE"

    def reset_connection(self):
        self._reset()
        self.book_live = False
        self.trade_live = False

    def _check_update_id(self, pair: str, msg: dict) -> bool:
        previous = self.last_update_id[pair]
        skip = super()._check_update_id(pair, msg)
        if pair not in self._l2_book:
            raise ResyncRequired(
                f"depth_gap: expected {previous + 1}, received {msg['U']}..{msg['u']}"
            )
        return skip

    async def _snapshot(self, pair: str):
        # Network I/O must not be hidden inside normalization: raw snapshots are durable inputs.
        raise ResyncRequired("snapshot_required")

    def _levels(self, levels):
        return tuple(
            (units(p, self.spec.price_tick), units(q, self.spec.quantity_unit)) for p, q in levels
        )

    async def book_callback(
        self,
        book_type,
        book,
        receipt_timestamp,
        timestamp=None,
        raw=None,
        sequence_number=None,
        checksum=None,
        delta=None,
    ):
        # Gate BEFORE any business callback. Upstream's optional crossed gate is post-callback.
        bids = self._levels(book.book.bids.to_list())
        asks = self._levels(book.book.asks.to_list())
        Projection._validate(bids, asks)
        if bids and asks and bids[0][0] >= asks[0][0]:
            raise ResyncRequired("crossed_or_locked_book")
        digest = levels_hash(bids, asks)
        book.raw, book.delta, book.sequence_number = raw, delta, sequence_number
        exchange_ns = raw.get("E") * 1_000_000 if raw and "E" in raw else None
        if delta is None:
            payload = Snapshot(
                sequence_number,
                bids,
                asks,
                max(len(bids), len(asks)),
                bids[-1][0] if bids else None,
                asks[-1][0] if asks else None,
            )
            self.book_live = False
        else:
            payload = Delta(raw["U"], raw["u"], self._levels(raw["b"]), self._levels(raw["a"]))
        self.emissions.append((payload, digest, exchange_ns))
        if delta is not None and not self.book_live:
            self.emissions.append(
                (Quality("book", "LIVE", "snapshot_delta_bridge_verified"), None, exchange_ns)
            )
            self.book_live = True

    async def normalize(self, frame: RawFrame):
        self.emissions = []
        self.frame = frame
        if frame.kind == "snapshot":
            book = self._parse_snapshot(self.symbol, frame.payload)
            self._l2_book[self.symbol] = book
            self.last_update_id[self.symbol] = book.sequence_number
            await self.book_callback(
                L2_BOOK, book, 0.0, raw=book.raw, sequence_number=book.sequence_number
            )
            return self.emissions
        msg = msgspec.json.decode(frame.payload)
        msg = msg.get("data", msg)
        event = msg.get("e")
        if msg.get("s") not in (None, "BTCUSDT"):
            raise ValueError("unexpected instrument")
        if event == "depthUpdate":
            await self._book(msg, "BTCUSDT", 0.0)
        elif event == "trade":
            trade_id = msg["t"]
            price, qty = (
                units(msg["p"], self.spec.price_tick),
                units(msg["q"], self.spec.quantity_unit),
            )
            if not isinstance(trade_id, int) or price <= 0 or qty <= 0:
                raise ValueError("invalid raw trade")
            fact = Trade(trade_id, price, qty, "sell" if msg["m"] else "buy", msg["T"] * 1_000_000)
            if self.last_trade_id is not None and trade_id <= self.last_trade_id:
                known = self.trade_lookup(trade_id)
                if known != fact:
                    raise ResyncRequired(f"conflicting_or_unresolved_late_trade:{trade_id}")
                return []
            if self.last_trade_id is not None and trade_id != self.last_trade_id + 1:
                self.emissions.append(
                    (
                        Quality(
                            "trades",
                            "INVALID",
                            f"trade_id_gap:{self.last_trade_id + 1}..{trade_id - 1}",
                        ),
                        None,
                        msg["E"] * 1_000_000,
                    )
                )
                self.trade_live = False
            if not self.trade_live:
                self.emissions.append(
                    (
                        Quality(
                            "trades", "LIVE", "new_observed_trade_segment; prior_gap_not_backfilled"
                        ),
                        None,
                        msg["E"] * 1_000_000,
                    )
                )
                self.trade_live = True
            self.emissions.append((fact, None, msg["E"] * 1_000_000))
            self.last_trade_id = trade_id
        elif event == "serverShutdown":
            raise ResyncRequired("server_shutdown")
        else:
            raise ValueError(f"unexpected stream event {event!r}")
        return self.emissions
