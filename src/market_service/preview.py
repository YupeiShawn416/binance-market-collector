"""Bounded, read-only quality aggregation using the existing event/book consumer.

Depth is a time-weighted mean of visible base quantity, never a sum over updates.
A price/time cell with incomplete valid coverage remains unknown, not zero.
"""

from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from statistics import median_low

from .book import IntegrityError, Projection
from .contracts import Delta, Quality, Snapshot, Trade, decode_event
from .source import Consumer
from .store import Journal


@dataclass(frozen=True)
class PreviewOptions:
    max_events: int = 20_000
    time_bin_ms: int = 100
    max_time_bins: int = 600
    price_bins: int = 120
    band_bps: float = 20
    stale_seconds: float = 10

    def validate(self):
        for name, low, high in (
            ("max_events", 1, 100_000),
            ("time_bin_ms", 1, 60_000),
            ("max_time_bins", 1, 1200),
            ("price_bins", 10, 300),
        ):
            value = getattr(self, name)
            if not low <= value <= high:
                raise ValueError(f"{name} must be within [{low}, {high}]")
        if not math.isfinite(self.band_bps) or not 0 < self.band_bps <= 1000:
            raise ValueError("band_bps must be finite and in (0, 1000]")
        if not math.isfinite(self.stale_seconds) or self.stale_seconds <= 0:
            raise ValueError("stale_seconds must be finite and positive")


def _consumer_at(journal: Journal, before: int) -> Consumer:
    consumer = Consumer()
    row = journal.db.execute(
        "SELECT market_seq,payload,digest FROM checkpoints WHERE market_seq<=? ORDER BY market_seq DESC LIMIT 1",
        (before,),
    ).fetchone()
    if row:
        seq, payload, digest = row
        if hashlib.sha256(payload).hexdigest() != digest:
            raise IntegrityError("preview checkpoint checksum mismatch")
        projection = Projection.restore(payload)
        record = next(journal.records(seq - 1, limit=1))
        if (
            projection.seq != seq
            or projection.dataset_id != journal.dataset_id
            or projection.book_hash != record.event.book_hash
        ):
            raise IntegrityError("preview checkpoint identity/book mismatch")
        consumer.projection = projection
        consumer.chain_hash = record.chain_hash
    return consumer


def _records(journal, consumer, first, frontier):
    for record in journal.records(
        consumer.projection.seq, limit=frontier - consumer.projection.seq
    ):
        consumer.apply(record)
        if record.event.market_seq >= first:
            yield record.event


def _depth_valid(projection):
    return (
        projection.quality["connection"] == projection.quality["book"] == "LIVE"
        and projection.snapshot_epoch == projection.epoch
    )


def _expiry(event, clock, stale_ns):
    age = (
        event.normalized_mono_ns - event.receive_mono_ns
        if event.normalizer_instance_id == event.boot_id
        else event.normalized_wall_ns - event.receive_wall_ns
    )
    return clock + max(0, stale_ns - max(0, age))


# Status severity includes STALE and PARTIAL, which are display states, not Event v1 changes.
STATUS_RANK = {"LIVE": 0, "SYNCING": 1, "PARTIAL": 2, "STALE": 3, "INVALID": 4, "DISCONNECTED": 5}


def build_preview(path, options: PreviewOptions | None = None) -> dict:
    options = options or PreviewOptions()
    options.validate()
    with Journal(path, readonly=True) as journal:
        # One WAL read transaction fixes the frontier, raw checks and both replay passes.
        journal.db.execute("BEGIN")
        frontier = journal.frontier
        if not frontier:
            raise ValueError("journal contains no committed events")
        first = max(1, frontier - options.max_events + 1)
        consumer = _consumer_at(journal, first - 1)
        anchor = consumer.projection.seq
        start, end, previous = None, None, None
        mids, reasons, regressions = [], [], 0
        counts = {"Snapshot": 0, "Delta": 0, "Trade": 0, "Quality": 0}
        missing_trades = 0
        gap_count = 0
        for event in _records(journal, consumer, first, frontier):
            clock = event.normalized_wall_ns
            if previous is not None and clock < previous:
                regressions += 1
                clock = previous
            previous = end = clock
            start = clock if start is None else start
            data, projection = event.payload, consumer.projection
            kind = type(data).__name__
            if kind in counts:
                counts[kind] += 1
            if isinstance(data, (Snapshot, Delta)) and _depth_valid(projection):
                if len(projection.book.bids) and len(projection.book.asks):
                    mids.append(
                        (projection.book.bids.index(0)[0] + projection.book.asks.index(0)[0]) // 2
                    )
            if isinstance(data, Quality) and data.state != "LIVE":
                gap = re.fullmatch(r"trade_id_gap:(\d+)\.\.(\d+)", data.reason)
                if gap:
                    missing_trades += int(gap[2]) - int(gap[1]) + 1
                    gap_count += 1
                if len(reasons) < 200:
                    reasons.append(
                        {
                            "seq": event.market_seq,
                            "time_ns": clock,
                            "scope": data.scope,
                            "state": data.state,
                            "reason": data.reason,
                        }
                    )
        spec = consumer.projection.instrument
        if spec is None:
            raise ValueError("no instrument metadata in selected journal")
        # The first bridging Delta is followed by Quality(LIVE); collect its BBO too.
        if not mids and len(consumer.projection.book.bids) and len(consumer.projection.book.asks):
            mids.append(
                (
                    consumer.projection.book.bids.index(0)[0]
                    + consumer.projection.book.asks.index(0)[0]
                )
                // 2
            )
        if not mids:
            raise ValueError("no usable two-sided snapshot for a price range")
        center = median_low(mids)
        radius = max(1, math.ceil(center * options.band_bps / 10_000))
        price_step = max(1, math.ceil((2 * radius + 1) / options.price_bins))
        price_low = max(1, (center - radius) // price_step * price_step)
        base_step = options.time_bin_ms * 1_000_000
        span = max(1, end - start)
        time_step = base_step * max(1, math.ceil(span / (base_step * options.max_time_bins)))
        columns = max(1, math.ceil(span / time_step))
        edges = [start + i * time_step for i in range(columns + 1)]
        # Never imply observations after the last event; final partial bin ends there.
        edges[-1] = max(end, start + 1)
        rows = options.price_bins
        weighted = [[0] * columns for _ in range(rows)]
        known_ns = [[0] * columns for _ in range(rows)]
        book_ns = [0] * columns
        statuses = {scope: ["LIVE"] * columns for scope in ("connection", "book", "trades")}
        status_ns = {scope: [0] * columns for scope in statuses}
        buy, sell = [0] * columns, [0] * columns
        trade_counts = [0] * columns
        bids, asks = [None] * columns, [None] * columns
        stale_ns = int(options.stale_seconds * 1e9)
        consumer = _consumer_at(journal, first - 1)
        current, valid_until = start, start
        quantities, known = [0] * rows, [False] * rows
        last_bid = last_ask = None

        def aggregate_book():
            nonlocal quantities, known, last_bid, last_ask
            projection = consumer.projection
            quantities = [0] * rows
            floor, ceiling, _ = projection.coverage
            known = [
                floor is not None
                and ceiling is not None
                and price_low + r * price_step >= floor
                and price_low + (r + 1) * price_step - 1 <= ceiling
                for r in range(rows)
            ]
            for side in (projection.book.bids, projection.book.asks):
                for price, quantity in side.to_list():
                    row = (price - price_low) // price_step
                    if 0 <= row < rows:
                        quantities[row] += quantity
            last_bid = projection.book.bids.index(0)[0] if len(projection.book.bids) else None
            last_ask = projection.book.asks.index(0)[0] if len(projection.book.asks) else None

        def integrate(until):
            nonlocal current
            projection = consumer.projection
            while current < until:
                column = min(columns - 1, (current - start) // time_step)
                stop = min(until, edges[column + 1])
                # Split at the freshness deadline so a quiet interval cannot stay green.
                if current < valid_until < stop:
                    stop = valid_until
                duration = stop - current
                fresh = current < valid_until
                usable = _depth_valid(projection) and fresh
                for scope, values in statuses.items():
                    state = projection.quality[scope]
                    if scope == "book" and state == "LIVE" and not fresh:
                        state = "STALE"
                    status_ns[scope][column] += duration
                    if STATUS_RANK[state] > STATUS_RANK[values[column]]:
                        values[column] = state
                if usable:
                    book_ns[column] += duration
                    bids[column], asks[column] = last_bid, last_ask
                    for row in range(rows):
                        if known[row]:
                            weighted[row][column] += quantities[row] * duration
                            known_ns[row][column] += duration
                current = stop

        consumer = _consumer_at(journal, first - 1)
        aggregate_book()
        # A checkpoint contains book state, but not the last depth reception time.
        # Search a bounded prefix for a freshness anchor; otherwise leave it unknown.
        for row in journal.db.execute(
            "SELECT market_seq,payload FROM events WHERE market_seq<? ORDER BY market_seq DESC LIMIT 100000",
            (first,),
        ):
            candidate = decode_event(row[1])
            if isinstance(candidate.payload, (Snapshot, Delta)):
                checked = next(journal.records(row[0] - 1, limit=1)).event
                valid_until = _expiry(checked, checked.normalized_wall_ns, stale_ns)
                break
        for record in journal.records(
            consumer.projection.seq, limit=frontier - consumer.projection.seq
        ):
            event = record.event
            if event.market_seq < first:
                consumer.apply(record)
                if isinstance(event.payload, (Snapshot, Delta)):
                    aggregate_book()
                    valid_until = _expiry(event, event.normalized_wall_ns, stale_ns)
                continue
            clock = max(current, event.normalized_wall_ns)
            integrate(clock)
            consumer.apply(record)
            data = event.payload
            if isinstance(data, (Snapshot, Delta)):
                aggregate_book()
                valid_until = _expiry(event, clock, stale_ns)
            if isinstance(data, Trade):
                column = min(columns - 1, max(0, (clock - start) // time_step))
                (buy if data.aggressor == "buy" else sell)[column] += data.quantity_units
                trade_counts[column] += 1
        quantity_unit = float(spec.quantity_unit)
        depth = [[None] * columns for _ in range(rows)]
        coverage = [[0.0] * columns for _ in range(rows)]
        for column in range(columns):
            width = edges[column + 1] - edges[column]
            for scope in statuses:
                if status_ns[scope][column] != width:
                    statuses[scope][column] = "PARTIAL"
            if book_ns[column] != width:
                bids[column] = asks[column] = None
                if book_ns[column] and statuses["book"][column] == "LIVE":
                    statuses["book"][column] = "PARTIAL"
            for row in range(rows):
                covered = known_ns[row][column]
                coverage[row][column] = covered / width
                if covered == width:
                    depth[row][column] = weighted[row][column] / width * quantity_unit
        raw_count = 0
        for seq, payload, digest in journal.db.execute(
            "SELECT raw_seq,payload,digest FROM raw_frames WHERE raw_seq IN "
            "(SELECT raw_seq FROM events WHERE market_seq BETWEEN ? AND ?)",
            (first, frontier),
        ):
            if hashlib.sha256(payload).hexdigest() != digest:
                raise IntegrityError(f"preview raw checksum mismatch {seq}")
            raw_count += 1
        return {
            "metadata": {
                "preview_version": 1,
                "dataset_id": journal.dataset_id,
                "instrument": spec.symbol,
                "base": spec.base,
                "quote": spec.quote,
                "price_tick": spec.price_tick,
                "first_seq": first,
                "last_seq": frontier,
                "checkpoint_seq": anchor,
                "selected_events": frontier - first + 1,
                "counts": counts,
                "raw_inputs_checked": raw_count,
                "pending_raw": journal.db.execute(
                    "SELECT COUNT(*) FROM raw_frames WHERE processed=0"
                ).fetchone()[0],
                "hash_scope": "checkpoint anchor and following records through fixed frontier",
                "event_and_book_hashes_checked": True,
                "final_event_chain": consumer.chain_hash,
                "final_book_hash": consumer.projection.book_hash,
                "start_ns": start,
                "end_ns": end,
                "wall_clock_regressions_clamped": regressions,
                "time_bin_ms": time_step / 1e6,
                "price_bin_quote": price_step * float(spec.price_tick),
                "missing_trade_ids": missing_trades,
                "trade_gap_events": gap_count,
                "valid_book_fraction": sum(book_ns) / span,
                "stale_seconds": options.stale_seconds,
                "quality_events_shown": len(reasons),
                "quality_events_total": counts["Quality"],
            },
            "time_edges_ns": edges,
            "price_centers": [
                (price_low + (r + 0.5) * price_step) * float(spec.price_tick) for r in range(rows)
            ],
            "depth_base": depth,
            "known_fraction": coverage,
            "statuses": statuses,
            "best_bid": [p * float(spec.price_tick) if p is not None else None for p in bids],
            "best_ask": [p * float(spec.price_tick) if p is not None else None for p in asks],
            "buy_base": [q * quantity_unit for q in buy],
            "sell_base": [q * quantity_unit for q in sell],
            "trade_counts": trade_counts,
            "quality_events": reasons,
        }
