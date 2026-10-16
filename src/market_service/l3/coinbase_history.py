"""Import public Tardis Coinbase full-channel captures, preserving original lines.

REST snapshots arrive behind WebSocket updates. Buffer the prefix, bridge by the
exchange sequence, and publish the initial state only when the snapshot is known.
Research uses a fixed initial price band, never a moving truncated queue.
"""

import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from ..book import IntegrityError, decimal_text, units
from ..contracts import Instrument, Quality, encode
from .contracts import Change, Order, OrderBatch, OrderSnapshot
from .kraken import timestamp_ns
from .service import L3Journal, L3Service


def full_changes(message, orders, spec, bounds):
    """Normalize resting-order effects; received/non-resting done stay in raw input."""
    kind, side = message["type"], message.get("side")
    stamp = timestamp_ns(message["time"]) if message.get("time") else 0

    def inside(side, price):
        return price >= bounds[0] if side == "buy" else price <= bounds[1]

    def make(number, side, price, size):
        return Order(
            number,
            side,
            units(price, spec.price_tick),
            units(size, spec.quantity_unit),
            stamp,
            price,
            size,
        )

    if kind == "received":
        return []
    if kind == "open":
        price = units(message["price"], spec.price_tick)
        if not inside(side, price):
            return []
        return [
            Change(
                "add", make(message["order_id"], side, message["price"], message["remaining_size"])
            )
        ]
    if kind == "done":
        previous = orders.get(message["order_id"])
        return [Change("delete", previous)] if previous else []
    if kind == "match":
        price = units(message["price"], spec.price_tick)
        previous = orders.get(message["maker_order_id"])
        if previous is None:
            if inside(side, price):
                raise IntegrityError("Coinbase match references missing in-band maker")
            return []
        quantity = previous.quantity_units - units(message["size"], spec.quantity_unit)
        if quantity < 0 or previous.price_ticks != price or previous.side != side:
            raise IntegrityError("Coinbase match exceeds remaining maker or changes its price/side")
        changed = Order(
            previous.order_id,
            previous.side,
            previous.price_ticks,
            quantity,
            previous.priority_time_ns,
            previous.price_text,
            decimal_text(quantity, spec.quantity_unit),
        )
        return [Change("modify" if quantity else "delete", changed)]
    if kind == "change":
        previous = orders.get(message["order_id"])
        if not previous and "new_price" not in message:
            return []  # non-resting STP/funds change
        old_price = message.get("old_price", message.get("price"))
        new_price = message.get("new_price", old_price)
        if new_price is None or "new_size" not in message:
            return []
        if (
            previous
            and "old_size" in message
            and previous.quantity_units != units(message["old_size"], spec.quantity_unit)
        ):
            raise IntegrityError("Coinbase change old_size mismatch")
        updated = make(message["order_id"], side, new_price, message["new_size"])
        result = [Change("delete", previous)] if previous else []
        if inside(side, updated.price_ticks) and updated.quantity_units:
            result.append(Change("add", updated))
        return result
    raise IntegrityError("unsupported Coinbase full event: " + kind)


def apply_bridge(orders, changes):
    for change in changes:
        previous = orders.get(change.order.order_id)
        if change.action == "add" and previous is not None:
            raise IntegrityError("duplicate buffered Coinbase add")
        if change.action != "add" and previous is None:
            raise IntegrityError("missing buffered Coinbase order")
        if change.action == "delete":
            del orders[change.order.order_id]
        else:
            orders[change.order.order_id] = change.order


@dataclass
class Captured:
    timestamp: int
    message: dict
    line: bytes


def import_capture(source, target, *, initial_levels=100):
    if initial_levels < 1:
        raise ValueError("initial price band must contain at least one level")
    if Path(target).exists():
        raise ValueError("historical import requires a fresh dataset path")
    body = Path(source).read_bytes()
    rows, previous, counts = [], None, Counter()
    for line in body.splitlines(keepends=True):
        if not line.strip():
            raise IntegrityError("capture contains disconnect marker; split into valid segments")
        text_time, payload = line.split(b" ", 1)
        message = json.loads(payload)
        if message.get("product_id") != "BTC-USD":
            raise IntegrityError("historical symbol mismatch")
        stamp = timestamp_ns(text_time.decode())
        if rows and stamp < rows[-1].timestamp:
            raise IntegrityError("capture-time regression; do not silently sort")
        if message["type"] != "full_snapshot":
            sequence = message["sequence"]
            if previous is not None and sequence != previous + 1:
                raise IntegrityError("Coinbase historical exchange sequence gap")
            previous = sequence
        rows.append(Captured(stamp, message, line))
        counts[message["type"]] += 1
    snapshots = [i for i, row in enumerate(rows) if row.message["type"] == "full_snapshot"]
    if len(snapshots) != 1:
        raise IntegrityError(
            "this importer requires one snapshot in one contiguous capture segment"
        )
    index = snapshots[0]
    snapshot = rows[index].message
    spec = Instrument(
        "BTC-USD",
        "BTC",
        "USD",
        "0.01",
        "0.00000001",
        "unknown",
        "historical-research-precision-only",
    )
    bid_prices = sorted({units(r[0], spec.price_tick) for r in snapshot["bids"]}, reverse=True)
    ask_prices = sorted({units(r[0], spec.price_tick) for r in snapshot["asks"]})
    bounds = (
        bid_prices[min(initial_levels, len(bid_prices)) - 1],
        ask_prices[min(initial_levels, len(ask_prices)) - 1],
    )
    orders = {}
    for name, side in (("bids", "buy"), ("asks", "sell")):
        for price, size, number in snapshot[name]:
            ticks = units(price, spec.price_tick)
            if (side == "buy" and ticks >= bounds[0]) or (side == "sell" and ticks <= bounds[1]):
                if number in orders:
                    raise IntegrityError("duplicate snapshot order ID")
                # Coinbase REST L3 has no order-entry timestamp: keep priority explicitly unknown.
                orders[number] = Order(
                    number, side, ticks, units(size, spec.quantity_unit), 0, price, size
                )
    bridged, snapshot_sequence = 0, snapshot["sequence"]
    sequence = snapshot_sequence
    for row in rows[:index]:
        if row.message["sequence"] <= snapshot_sequence:
            continue
        if row.message["sequence"] != sequence + 1:
            raise IntegrityError("snapshot cannot bridge buffered full stream")
        apply_bridge(orders, full_changes(row.message, orders, spec, bounds))
        sequence = row.message["sequence"]
        bridged += 1
    with L3Journal(target) as journal:
        with journal.db:
            journal.db.execute("INSERT INTO metadata VALUES('data_origin','historical')")
            journal.db.execute("INSERT INTO metadata VALUES('import_complete','0')")
            journal.db.execute(
                "INSERT INTO metadata VALUES('source_sha256',?)",
                (hashlib.sha256(body).hexdigest(),),
            )
        service = L3Service(journal, depth=0, venue="COINBASE")
        first_time = rows[0].timestamp
        frame = service.capture("l3-spec", encode(spec), "tardis-history")
        service.emit(frame, [(spec, None)], historical_capture_ns=first_time)
        frame = service.capture(
            "l3-control", b'{"source":"historical_capture","state":"SYNCING"}', "tardis-history"
        )
        service.emit(
            frame,
            [
                (Quality("connection", "LIVE", "historical_provider_capture"), None),
                (Quality("book", "SYNCING", "awaiting_snapshot_and_sequence_bridge"), None),
            ],
            epoch=1,
            historical_capture_ns=first_time,
        )
        for number, row in enumerate(rows):
            frame = service.capture("coinbase-tardis-raw", row.line, "tardis-history")
            if number < index:
                service.emit(frame, [], historical_capture_ns=row.timestamp)
            elif number == index:
                initial = OrderSnapshot(
                    tuple(orders.values()),
                    0,
                    None,
                    source_sequence=sequence,
                    known_bid_floor=bounds[0],
                    known_ask_ceiling=bounds[1],
                    queue_order_known=False,
                )
                service.emit(
                    frame,
                    [(initial, None)],
                    historical_capture_ns=row.timestamp,
                    exchange_resolution_ns=1000,
                )
            else:
                changes = full_changes(row.message, service.projection.orders, spec, bounds)
                payload = OrderBatch(tuple(changes), None, row.message["sequence"])
                service.emit(
                    frame,
                    [(payload, timestamp_ns(row.message["time"]))],
                    historical_capture_ns=row.timestamp,
                    exchange_resolution_ns=1000,
                )
        frame = service.capture(
            "l3-control",
            b'{"state":"DISCONNECTED","source":"historical_capture_end"}',
            "tardis-history",
        )
        service.emit(
            frame,
            [
                (Quality("connection", "DISCONNECTED", "historical_capture_end"), None),
                (Quality("book", "INVALID", "historical_capture_end"), None),
            ],
            historical_capture_ns=rows[-1].timestamp,
        )
        final = service.projection
        with journal.db:
            journal.db.execute("UPDATE metadata SET value='1' WHERE key='import_complete'")
        return {
            "data_origin": "historical",
            "source_format": "Tardis Coinbase full capture",
            "provider_verified_by_importer": False,
            "exchange": "COINBASE",
            "source_sha256": hashlib.sha256(body).hexdigest(),
            "raw_rows": len(rows),
            "counts": dict(counts),
            "exchange_sequence_contiguous": True,
            "snapshot_sequence": snapshot_sequence,
            "bridged_prefix_messages": bridged,
            "first_published_sequence": sequence,
            "last_exchange_sequence": final.source_sequence,
            "snapshot_total_orders": len(snapshot["bids"]) + len(snapshot["asks"]),
            "initial_price_levels_per_side": initial_levels,
            "fixed_price_band_ticks": bounds,
            "final_in_band_orders": len(final.orders),
            "final_order_hash": final.book_hash,
            "provider_capture_start_ns": rows[0].timestamp,
            "book_available_from_ns": rows[index].timestamp,
            "provider_capture_end_ns": rows[-1].timestamp,
            "queue_order_known": False,
            "scope": "all event frequencies and visible order IDs inside a fixed initial price band; not full depth, not known snapshot FIFO",
            "real_time_access_verified": False,
            "execution_allowed": False,
        }
