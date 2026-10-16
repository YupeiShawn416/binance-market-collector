"""Precision-preserving Kraken L3 decoder. No floats, network or order methods."""

import json
import re
from datetime import UTC, datetime
from decimal import Decimal

from ..book import IntegrityError, units
from .contracts import Change, Order, OrderBatch, OrderSnapshot


def timestamp_ns(text):
    match = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?Z", text)
    if not match:
        raise IntegrityError("unsupported Kraken timestamp")
    base = datetime.strptime(match[1], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)
    delta = base - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86400 + delta.seconds) * 1_000_000_000 + int(
        (match[2] or "").ljust(9, "0")
    )


def normalize(body, spec, depth):
    message = json.loads(body, parse_float=Decimal)
    if message.get("channel") != "level3":
        if message.get("success") is False or message.get("error"):
            raise IntegrityError("Kraken L3 subscription rejected")
        return []
    if message.get("type") not in ("snapshot", "update"):
        raise IntegrityError("unknown Kraken L3 message type")
    results = []
    for data in message["data"]:
        if data["symbol"] != "BTC/USD":
            raise IntegrityError("unexpected L3 instrument")
        orders, changes = [], []
        for name, side in (("bids", "buy"), ("asks", "sell")):
            for row in data.get(name, []):
                price = format(Decimal(str(row["limit_price"])), "f")
                quantity = format(Decimal(str(row["order_qty"])), "f")
                order = Order(
                    row["order_id"],
                    side,
                    units(price, spec.price_tick),
                    units(quantity, spec.quantity_unit),
                    timestamp_ns(row["timestamp"]),
                    price,
                    quantity,
                )
                if message["type"] == "snapshot":
                    orders.append(order)
                else:
                    if row["event"] not in ("add", "modify", "delete"):
                        raise IntegrityError("unknown L3 order event")
                    changes.append(Change(row["event"], order))
        checksum = data["checksum"]
        if type(checksum) is not int or not 0 <= checksum <= 0xFFFFFFFF:
            raise IntegrityError("invalid Kraken L3 checksum")
        payload = (
            OrderSnapshot(tuple(orders), depth, checksum)
            if message["type"] == "snapshot"
            else OrderBatch(tuple(changes), checksum)
        )
        results.append(
            (payload, timestamp_ns(data["timestamp"]) if data.get("timestamp") else None)
        )
    return results
