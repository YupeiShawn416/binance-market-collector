"""Synthetic full-channel captures exercise late snapshots and source sequencing."""

import json

import pytest

from market_service.book import IntegrityError
from market_service.l3.coinbase_history import import_capture
from market_service.l3.contracts import OrderSnapshot
from market_service.l3.experiment import compare
from market_service.l3.service import L3Journal


def capture_rows():
    def event(kind, sequence, **fields):
        return {
            "type": kind,
            "sequence": sequence,
            "product_id": "BTC-USD",
            "time": "2024-01-01T00:00:00.000001Z",
            **fields,
        }

    return [
        ("001", event("received", 99, order_id="a", side="sell")),
        (
            "010",
            event("open", 100, order_id="a", side="sell", price="100.00", remaining_size="0.01"),
        ),
        (
            "020",
            event(
                "done",
                101,
                order_id="a",
                side="sell",
                price="100.00",
                remaining_size="0.01",
                reason="canceled",
            ),
        ),
        (
            "030",
            event("open", 102, order_id="a2", side="sell", price="100.00", remaining_size="0.02"),
        ),
        (
            "040",
            event(
                "full_snapshot",
                100,
                generated=True,
                bids=[["99.00", "1", "b"]],
                asks=[["100.00", "0.01", "a"], ["105.00", "1", "f"]],
            ),
        ),
        (
            "050",
            event(
                "match",
                103,
                maker_order_id="a2",
                taker_order_id="t",
                side="sell",
                price="100.00",
                size="0.001",
            ),
        ),
        ("250", event("received", 104, order_id="later", side="buy")),
    ]


def write_capture(path, rows):
    path.write_bytes(
        b"".join(
            (f"2024-01-01T00:00:00.{millis}0000Z " + json.dumps(message) + "\n").encode()
            for millis, message in rows
        )
    )


def test_late_snapshot_bridges_before_publishing_and_preserves_source(tmp_path):
    source, db = tmp_path / "capture.ndjson", tmp_path / "history.sqlite"
    write_capture(source, capture_rows())
    result = import_capture(source, db, initial_levels=1)
    assert result["bridged_prefix_messages"] == 2
    assert result["first_published_sequence"] == 102
    with L3Journal(db, readonly=True) as journal:
        projection = journal.recover()
        assert set(projection.orders) == {"b", "a2"}
        assert projection.orders["a2"].quantity_units == 1_900_000
        assert projection.source_sequence == 104
        assert not projection.queue_order_known
        assert not projection.ready
        records = list(journal.records())
        snapshot = next(r.event for r in records if isinstance(r.event.payload, OrderSnapshot))
        assert snapshot.historical_capture_ns % 1_000_000_000 == 40_000_000
        assert snapshot.exchange_time_ns is None
        assert snapshot.payload.source_sequence == 102
        assert {o.order_id for o in snapshot.payload.orders} == {"b", "a2"}
        assert all(r.event.data_origin == "historical" for r in records)
        stored = b"".join(
            r[0]
            for r in journal.db.execute(
                "SELECT payload FROM raw_frames WHERE kind='coinbase-tardis-raw' ORDER BY raw_seq"
            )
        )
        assert stored == source.read_bytes()
    comparison = compare(db, tmp_path / "probes.csv", sizes=("0.001",))
    assert comparison["data_origin"] == "historical"
    assert comparison["event_l2_vs_l3_disagreements"] == 0


def test_historical_sequence_gap_is_rejected_before_creating_dataset(tmp_path):
    source, db = tmp_path / "bad.ndjson", tmp_path / "bad.sqlite"
    rows = capture_rows()
    rows[-1][1]["sequence"] = 105
    write_capture(source, rows)
    with pytest.raises(IntegrityError, match="sequence gap"):
        import_capture(source, db)
    assert not db.exists()


def test_historical_disconnect_marker_is_not_hidden(tmp_path):
    source, db = tmp_path / "bad.ndjson", tmp_path / "bad.sqlite"
    write_capture(source, capture_rows())
    source.write_bytes(source.read_bytes() + b"\n")
    with pytest.raises(IntegrityError, match="disconnect marker"):
        import_capture(source, db)
    assert not db.exists()


def test_missing_in_band_maker_cannot_be_assumed_filled(tmp_path):
    source, db = tmp_path / "bad.ndjson", tmp_path / "bad.sqlite"
    rows = capture_rows()
    rows[-2][1]["maker_order_id"] = "not-observed"
    write_capture(source, rows)
    with pytest.raises(IntegrityError, match="missing in-band maker"):
        import_capture(source, db)
    with pytest.raises(IntegrityError, match="import incomplete"):
        L3Journal(db, readonly=True)
