"""Paired visible-liquidity taker probes: L3, event-by-event L2, causal 100ms L2.

Each probe is an independent counterfactual, never a sequence of actual trades.
The shared sweep accounts for every visible quantity once within that probe.
This measures information loss, not actual exchange execution or maker fills.
"""

import csv
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

import msgspec

from ..book import IntegrityError, units
from .book import L3Projection
from .contracts import OrderBatch, OrderSnapshot
from .service import L3Journal


def sweep(levels, quantity):
    remaining, notional = quantity, 0
    for price, available in levels:
        taken = min(remaining, available)
        remaining -= taken
        notional += price * taken
        if not remaining:
            break
    return {
        "filled": quantity - remaining,
        "notional": notional,
        "unknown_remainder": remaining > 0,
    }


def order_levels(projection, side):
    book_side = "sell" if side == "buy" else "buy"
    queues = projection._queues()
    return [
        (price, order.quantity_units)
        for price in sorted(queues[book_side], reverse=side == "sell")
        for order in queues[book_side][price]
    ]


def level_view(projection):
    return {"buy": projection.book.asks.to_list(), "sell": projection.book.bids.to_list()}


def vwap(result, tick):
    return (
        str(Decimal(result["notional"]) / result["filled"] * Decimal(tick))
        if result["filled"]
        else None
    )


def compare(
    path,
    output,
    *,
    sizes=("0.0001", "0.001", "0.01"),
    grid_ms=10,
    sample_ms=100,
    allow_synthetic=False,
):
    if grid_ms <= 0 or sample_ms <= 0 or sample_ms % grid_ms:
        raise ValueError("positive sample interval must be divisible by probe grid")
    with L3Journal(path, readonly=True) as journal:
        origin = journal.db.execute("SELECT value FROM metadata WHERE key='data_origin'").fetchone()
        origin = origin[0] if origin else "unknown"
        if origin not in ("live", "historical") and not (allow_synthetic and origin == "synthetic"):
            raise ValueError(
                "comparison needs explicitly live data; use --allow-synthetic only for labelled protocol fixtures"
            )
        records = list(journal.records())
        dataset_id = journal.dataset_id
    market_records = [
        r for r in records if isinstance(r.event.payload, (OrderSnapshot, OrderBatch))
    ]
    snapshots = [r for r in market_records if isinstance(r.event.payload, OrderSnapshot)]
    if not snapshots:
        raise IntegrityError("no valid L3 snapshot; no empirical comparison is possible")
    if len({r.event.boot_id for r in records}) != 1:
        raise IntegrityError("split comparison at capture-process clock boundaries")
    historical = origin == "historical"
    clock = [
        r.event.historical_capture_ns if historical else r.event.receive_mono_ns for r in records
    ]
    if any(t is None for t in clock):
        raise IntegrityError("historical capture timeline missing")
    if any(a > b for a, b in zip(clock, clock[1:], strict=False)):
        raise IntegrityError("receive clock regressed; cannot silently sort the log")
    first_ns = (
        snapshots[0].event.historical_capture_ns
        if historical
        else snapshots[0].event.receive_mono_ns
    )
    end_ns = (
        market_records[-1].event.historical_capture_ns
        if historical
        else market_records[-1].event.receive_mono_ns
    )
    if end_ns - first_ns < sample_ms * 1_000_000:
        raise IntegrityError("sample shorter than one downsampling interval")
    projection, cursor, sampled, sampled_epoch, sample_at = L3Projection(), 0, None, None, None
    next_boundary, step = first_ns, grid_ms * 1_000_000
    interval, rows, rejected = sample_ms * 1_000_000, [], 0
    event_l2_disagreements, samples = 0, []

    def refresh_sample(boundary):
        nonlocal sampled, sampled_epoch, sample_at
        sampled = level_view(projection) if projection.ready else None
        sampled_epoch, sample_at = projection.epoch, boundary
        samples.append(
            {
                "boundary_mono_ns": boundary,
                "source_market_seq": projection.seq,
                "source_receive_mono_ns": clock[cursor - 1] if cursor else None,
                "book_epoch": sampled_epoch,
                "valid": sampled is not None,
                "levels": sampled,
            }
        )

    for arrival in range(first_ns, end_ns + 1, step):
        while cursor < len(records) and clock[cursor] <= arrival:
            event = records[cursor].event
            while next_boundary < clock[cursor] and next_boundary <= arrival:
                refresh_sample(next_boundary)
                next_boundary += interval
            projection.apply(event)
            cursor += 1
        while next_boundary <= arrival:
            refresh_sample(next_boundary)
            next_boundary += interval
        if not projection.ready or sampled is None or sampled_epoch != projection.epoch:
            rejected += 1
            continue
        event_l2 = level_view(projection)
        for side in ("buy", "sell"):
            for size in sizes:
                quantity = units(size, projection.instrument.quantity_unit)
                if quantity <= 0:
                    raise ValueError("probe quantity must be positive")
                reference = sweep(order_levels(projection, side), quantity)
                full_l2 = sweep(event_l2[side], quantity)
                if reference != full_l2:
                    event_l2_disagreements += 1
                sampled_result = sweep(sampled[side], quantity)
                bps = None
                if reference["filled"] and sampled_result["filled"]:
                    a = Fraction(reference["notional"], reference["filled"])
                    b = Fraction(sampled_result["notional"], sampled_result["filled"])
                    bps = float((b - a) / a * 10000 * (1 if side == "buy" else -1))
                rows.append(
                    {
                        "arrival_offset_ms": (arrival - first_ns) / 1_000_000,
                        "book_epoch": projection.epoch,
                        "side": side,
                        "size_btc": size,
                        "l2_age_ms": (arrival - sample_at) / 1_000_000,
                        "l3_filled_units": reference["filled"],
                        "l2_100ms_filled_units": sampled_result["filled"],
                        "l3_vwap": vwap(reference, projection.instrument.price_tick),
                        "l2_100ms_vwap": vwap(sampled_result, projection.instrument.price_tick),
                        "vwap_differs": bool(
                            reference["filled"]
                            and sampled_result["filled"]
                            and reference["notional"] * sampled_result["filled"]
                            != sampled_result["notional"] * reference["filled"]
                        ),
                        "signed_adverse_error_bps": bps,
                        "l3_coverage_exhausted": reference["unknown_remainder"],
                        "l2_coverage_exhausted": sampled_result["unknown_remainder"],
                    }
                )
    if not rows:
        raise IntegrityError("no paired probes in healthy visible coverage")
    if event_l2_disagreements:
        raise IntegrityError("L3 sweep and full-frequency L2 aggregation disagree")
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    samples_path = target.with_suffix(".l2-100ms.msgpack")
    samples_path.write_bytes(
        msgspec.msgpack.encode(
            {
                "parent_dataset_id": dataset_id,
                "data_origin": origin,
                "price_quantum": projection.instrument.price_tick,
                "quantity_quantum": projection.instrument.quantity_unit,
                "samples": samples,
            }
        )
    )
    complete = [
        r for r in rows if not r["l3_coverage_exhausted"] and not r["l2_coverage_exhausted"]
    ]
    errors = sorted(
        abs(r["signed_adverse_error_bps"])
        for r in complete
        if r["signed_adverse_error_bps"] is not None
    )
    differences = [r for r in complete if r["vwap_differs"]]
    return {
        "data_origin": origin,
        "dataset_id": dataset_id,
        "simulation": "independent hypothetical aggressive orders against observed visible liquidity; no real orders",
        "time_axis": "provider capture/availability timestamp; not exchange order arrival"
        if historical
        else "application receive_mono_ns; not exchange arrival time",
        "downsampling": "last book state at/before each boundary; no future bucket-end values",
        "sample_ms": sample_ms,
        "grid_ms": grid_ms,
        "sizes_btc": list(sizes),
        "duration_seconds": (end_ns - first_ns) / 1e9,
        "derived_l2_samples_file": samples_path.name,
        "derived_l2_samples": len(samples),
        "market_messages": len(market_records),
        "paired_probes": len(rows),
        "skipped_invalid_grid_points": rejected,
        "event_l2_vs_l3_disagreements": event_l2_disagreements,
        "fully_covered_probes": len(complete),
        "vwap_different_probes": len(differences),
        "vwap_difference_rate": len(differences) / len(complete) if complete else None,
        "visible_filled_quantity_differences": sum(
            r["l3_filled_units"] != r["l2_100ms_filled_units"] for r in rows
        ),
        "coverage_exhausted_probes": len(rows) - len(complete),
        "mean_absolute_error_bps": sum(errors) / len(errors) if errors else None,
        "max_absolute_error_bps": errors[-1] if errors else None,
        "quantity_unit": projection.instrument.quantity_unit,
        "worst_cases": sorted(
            rows, key=lambda r: abs(r["signed_adverse_error_bps"] or 0), reverse=True
        )[:10],
        "limits": [
            "Synthetic results are protocol demonstrations, never measured market error.",
            "L3 means full event frequency inside the subscribed visible depth; hidden and out-of-depth orders are excluded.",
            "Each probe starts independently; results cannot be summed as portfolio fills.",
            "No maker queue-fill model, latency calibration or market-response simulation is claimed.",
            "Short samples without stress cannot establish black-swan accuracy.",
            "Coverage exhaustion means unknown remaining execution, not proof of no liquidity.",
        ],
        "execution_allowed": False,
    }
