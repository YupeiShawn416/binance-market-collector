"""Independent full-journal replay and hash transcript comparison."""

from __future__ import annotations

from collections import Counter

from .contracts import Quality
from .source import Consumer, replay
from .store import Journal


def verify(path, expected_trace=None, *, expected_chain=None):
    consumer = Consumer(record_trace=expected_trace is not None)
    counts = Counter()
    boundaries = []
    with Journal(path, readonly=True) as journal:
        dataset_id = journal.dataset_id
        raw_count = journal.verify_raw()
        pending = journal.db.execute(
            "SELECT COUNT(*) FROM raw_frames WHERE processed=0"
        ).fetchone()[0]
    for record in replay(path, consumer):
        payload = record.event.payload
        counts[type(payload).__name__] += 1
        if isinstance(payload, Quality) and payload.state in ("INVALID", "DISCONNECTED"):
            boundaries.append(
                {"seq": record.event.market_seq, "scope": payload.scope, "reason": payload.reason}
            )
    if expected_trace is not None and consumer.trace != expected_trace:
        raise AssertionError("live normalization transcript differs from historical replay")
    if expected_chain is not None and consumer.chain_hash != expected_chain:
        raise AssertionError("live event/book hash chain differs from full replay")
    return {
        "dataset_id": dataset_id,
        "events": consumer.projection.seq,
        "raw_frames": raw_count,
        "pending_raw": pending,
        "counts": dict(counts),
        "final_book_hash": consumer.projection.book_hash,
        "final_event_chain": consumer.chain_hash,
        "internal_hashes_verified": True,
        "hash_transcript_equal": True
        if expected_trace is not None or expected_chain is not None
        else None,
        "quality_boundaries": boundaries,
    }
