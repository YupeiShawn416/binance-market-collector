"""Compare historical aggregates with observed raw trades without expanding aggregates."""

from collections import Counter

from .book import IntegrityError, decimal_text, units


def compare_aggregates(trades, aggregates, spec):
    if not trades or not aggregates:
        raise ValueError("both observed raw trades and downloaded aggregates are required")
    by_id = {trade.trade_id: trade for trade in trades}
    if len(by_id) != len(trades):
        raise IntegrityError("duplicate observed raw trade IDs")
    first_id, last_id = min(by_id), max(by_id)
    first_time = min(t.trade_time_ns for t in trades) // 1_000_000
    last_time = max(t.trade_time_ns for t in trades) // 1_000_000
    full_groups, partial_groups, missing_ids = 0, [], []
    mismatches = []
    known_aggregate_units = 0
    missing_volume_units = 0
    matched_fields = 0
    previous_aggregate_id = None
    previous_raw_id = None
    reasons = Counter()
    for aggregate in aggregates:
        if not {"a", "p", "q", "f", "l", "T", "m"} <= aggregate.keys():
            raise IntegrityError("invalid aggTrades fields")
        start, end = aggregate["f"], aggregate["l"]
        if start > end or (
            previous_aggregate_id is not None and aggregate["a"] != previous_aggregate_id + 1
        ):
            raise IntegrityError("aggregate IDs are not contiguous")
        if previous_raw_id is not None and start != previous_raw_id + 1:
            raise IntegrityError("aggregate raw-ID ranges are not contiguous")
        previous_aggregate_id, previous_raw_id = aggregate["a"], end
        if aggregate["T"] < first_time or aggregate["T"] > last_time:
            raise IntegrityError("download contains trades outside requested exchange-time window")
        price, quantity = (
            units(aggregate["p"], spec.price_tick),
            units(aggregate["q"], spec.quantity_unit),
        )
        side = "sell" if aggregate["m"] else "buy"
        observed = [
            by_id[i] for i in range(max(start, first_id), min(end, last_id) + 1) if i in by_id
        ]
        for trade in observed:
            different = []
            if trade.price_ticks != price:
                different.append("price")
            if trade.aggressor != side:
                different.append("aggressor")
            if trade.trade_time_ns // 1_000_000 != aggregate["T"]:
                different.append("trade_time_ms")
            if different:
                mismatches.append({"trade_id": trade.trade_id, "fields": different})
                reasons.update(different)
            else:
                matched_fields += 1
        # Only compare total volume when the whole aggregate range lies inside the captured ID bounds.
        if first_id <= start <= end <= last_id:
            known_aggregate_units += quantity
            observed_quantity = sum(t.quantity_units for t in observed)
            absent = [i for i in range(start, end + 1) if i not in by_id]
            missing_ids.extend(absent)
            if absent:
                if quantity < observed_quantity:
                    raise IntegrityError("observed volume exceeds historical aggregate volume")
                missing_volume_units += quantity - observed_quantity
                partial_groups.append(
                    {
                        "aggregate_id": aggregate["a"],
                        "first_id": start,
                        "last_id": end,
                        "missing_ids": absent,
                        "missing_volume_btc": decimal_text(
                            quantity - observed_quantity, spec.quantity_unit
                        ),
                        "individual_quantities_recoverable": start == end,
                    }
                )
            else:
                full_groups += 1
                if observed_quantity != quantity:
                    mismatches.append(
                        {"aggregate_id": aggregate["a"], "fields": ["aggregate_quantity"]}
                    )
                    reasons["aggregate_quantity"] += 1
    bounds_match = aggregates[0]["f"] == first_id and aggregates[-1]["l"] == last_id
    return {
        "historical_kind": "aggTrades; not raw historicalTrades",
        "historical_rows": len(aggregates),
        "observed_raw_trades": len(trades),
        "first_raw_id": first_id,
        "last_raw_id": last_id,
        "exchange_time_start_ms": first_time,
        "exchange_time_end_ms": last_time,
        "historical_raw_id_bounds_match": bounds_match,
        "historical_raw_id_count": sum(r["l"] - r["f"] + 1 for r in aggregates),
        "observed_price_side_time_matches": matched_fields,
        "fully_observed_aggregate_groups": full_groups,
        "groups_with_missing_raw_trades": len(partial_groups),
        "missing_raw_ids": missing_ids,
        "missing_raw_count": len(missing_ids),
        "missing_volume_btc": decimal_text(missing_volume_units, spec.quantity_unit),
        "historical_aggregate_volume_btc": decimal_text(known_aggregate_units, spec.quantity_unit),
        "observed_volume_btc": decimal_text(
            sum(t.quantity_units for t in trades), spec.quantity_unit
        ),
        "mismatch_count": len(mismatches),
        "mismatches": mismatches,
        "mismatch_fields": dict(reasons),
        "missing_groups": partial_groups,
        "raw_gap_repaired": False,
        "execution_allowed": False,
        "caveats": [
            "Aggregate volume and ID coverage cannot recover individual sizes inside multi-trade aggregates.",
            "Historical responses do not contain original local arrival/normalization times, connection epochs or L2 path.",
            "Comparison timestamp is exchange trade time (T), not event time E or local reception time.",
        ],
    }
