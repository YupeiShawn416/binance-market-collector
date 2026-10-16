"""Independent full-depth Decimal reconstruction, compared with the bounded C view.

This reference deliberately retains ALL snapshot orders and uses no production
normalizer, Order/Change types, checksum, integer reducer or bridge helper.
It does not independently certify the provider's capture itself.
"""

import argparse
import hashlib
import json
from decimal import Decimal
from pathlib import Path

from market_service.l3.service import L3Journal


def check(source, db):
    raw = Path(source).read_bytes()
    messages = [json.loads(line.split(b" ", 1)[1]) for line in raw.splitlines() if line]
    snapshot = next(m for m in messages if m["type"] == "full_snapshot")
    sequence = snapshot["sequence"]
    orders = {}
    for key, side in (("bids", "buy"), ("asks", "sell")):
        for price, size, number in snapshot[key]:
            assert number not in orders
            orders[number] = (side, Decimal(price), Decimal(size))
    applied = 0
    for message in messages:
        if message["type"] == "full_snapshot" or message["sequence"] <= snapshot["sequence"]:
            continue
        assert message["sequence"] == sequence + 1
        sequence = message["sequence"]
        kind, number = message["type"], message.get("order_id")
        if kind == "open":
            assert number not in orders
            orders[number] = (
                message["side"],
                Decimal(message["price"]),
                Decimal(message["remaining_size"]),
            )
        elif kind == "done":
            orders.pop(number, None)
        elif kind == "match":
            number = message["maker_order_id"]
            side, price, size = orders[number]
            assert side == message["side"] and price == Decimal(message["price"])
            size -= Decimal(message["size"])
            assert size >= 0
            if size:
                orders[number] = (side, price, size)
            else:
                del orders[number]
        elif kind == "change" and number in orders:
            side, price, size = orders[number]
            if "old_size" in message:
                assert size == Decimal(message["old_size"])
            orders[number] = (
                side,
                Decimal(message.get("new_price", str(price))),
                Decimal(message["new_size"]),
            )
        elif kind not in ("received", "change"):
            raise AssertionError("unsupported source message")
        applied += 1
    with L3Journal(db, readonly=True) as journal:
        projection = journal.recover()
        floor = Decimal(projection.coverage[0]) * Decimal(projection.instrument.price_tick)
        ceiling = Decimal(projection.coverage[1]) * Decimal(projection.instrument.price_tick)
        selected = {
            number: row
            for number, row in orders.items()
            if (row[0] == "buy" and row[1] >= floor) or (row[0] == "sell" and row[1] <= ceiling)
        }
        actual = {
            o.order_id: (o.side, Decimal(o.price_text), Decimal(o.quantity_text))
            for o in projection.orders.values()
        }
        assert selected == actual
        assert sequence == projection.source_sequence
        assert journal.db.execute(
            "SELECT COUNT(*) FROM raw_frames WHERE kind='coinbase-tardis-raw'"
        ).fetchone()[0] == len(messages)
    return {
        "source_sha256": hashlib.sha256(raw).hexdigest(),
        "full_snapshot_orders": len(snapshot["bids"]) + len(snapshot["asks"]),
        "full_reference_final_orders": len(orders),
        "applied_source_messages_after_snapshot_sequence": applied,
        "matched_in_band_order_ids_price_quantity": len(selected),
        "mismatches": 0,
        "last_source_sequence": sequence,
        "scope": "independent full-depth Decimal reconstruction against same source; queue priority excluded",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--report", required=True)
    args = parser.parse_args()
    result = check(args.source, args.db)
    Path(args.report).write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False))
