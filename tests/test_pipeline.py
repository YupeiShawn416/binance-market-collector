import asyncio
from decimal import localcontext

import msgspec
import pytest
from conftest import SNAPSHOT, delta, feed, trade

from market_service.book import IntegrityError, units
from market_service.contracts import Delta, Event, Quality, Trade, decode_event, encode
from market_service.service import MarketService
from market_service.source import Consumer, replay
from market_service.store import Journal
from market_service.verification import verify


def test_live_normalization_matches_every_replayed_event_and_book(setup):
    path, journal, service = setup
    assert service.projection.ready
    assert service.projection.book.asks.to_list() == [(10000, 100000000), (15000, 200000000)]
    feed(service, "ws", delta(102, asks=[["100.00", "0.00000000"]]))
    assert service.projection.book.asks.index(0)[0] == 15000
    result = verify(path, service.trace)
    assert result["hash_transcript_equal"] and result["pending_raw"] == 0
    # Both consumers have independent state and consume the same immutable contract.
    decision, trading = Consumer(record_trace=True), Consumer(record_trace=True)
    list(replay(path, decision))
    list(replay(path, trading))
    assert decision.trace == trading.trace == service.trace


def test_schema_roundtrip_is_frozen_and_preserves_ns(setup):
    _, journal, _ = setup
    event = list(journal.records())[-1].event
    assert decode_event(encode(event)) == event
    assert event.receive_wall_ns == 1792000000000000123
    with pytest.raises(AttributeError):
        event.market_seq = 1
    assert msgspec.json.schema(Event)["$defs"]["Event"]


def test_gap_invalidates_before_new_book_and_requires_bridge(setup):
    path, journal, service = setup
    before = service.projection.book_hash
    assert feed(service, "ws", delta(105, asks=[["100.00", "7.00000000"]]))
    assert not service.projection.ready
    assert service.projection.book_hash == before
    assert "depth_gap" in list(journal.records())[-2].event.payload.reason
    snapshot = {**SNAPSHOT, "lastUpdateId": 200}
    feed(service, "snapshot", snapshot)
    assert service.projection.quality["book"] == "SYNCING"
    feed(service, "ws", delta(201))
    assert service.projection.quality["book"] == "LIVE"
    verify(path, service.trace)


def test_old_depth_and_duplicate_trade_do_not_duplicate_events(setup):
    _, journal, service = setup
    seq = journal.frontier
    feed(service, "ws", delta(99, last=100))
    feed(service, "ws", trade())
    assert journal.frontier == seq
    assert not list(journal.pending())


def test_trade_gap_is_explicit_and_does_not_fabricate_missing_trades(setup):
    _, journal, service = setup
    feed(service, "ws", trade(4))
    tail = [r.event.payload for r in journal.records()][-3:]
    assert isinstance(tail[0], Quality) and tail[0].state == "INVALID"
    assert "2..3" in tail[0].reason
    assert isinstance(tail[-1], Trade) and tail[-1].trade_id == 4


@pytest.mark.parametrize(
    "bad",
    [
        delta(102, asks=[["98.00", "1.00000000"]]),
        delta(102, asks=[["100.001", "1"]]),
        delta(102, asks=[["100", "-1"]]),
    ],
)
def test_invalid_book_is_not_published(setup, bad):
    _, journal, service = setup
    before = service.projection.book_hash
    assert feed(service, "ws", bad)
    assert service.projection.book_hash == before
    assert not service.projection.ready
    assert not isinstance(list(journal.records())[-1].event.payload, Delta)


def test_reconnect_new_epoch_no_delta_before_snapshot(setup):
    path, _, service = setup
    old_epoch = service.epoch
    asyncio.run(service.control("stop", "session-1", "test_disconnect"))
    asyncio.run(service.control("start", "session-2"))
    assert service.epoch > old_epoch
    assert feed(service, "ws", delta(102), "session-2")
    assert not service.projection.ready
    feed(service, "snapshot", {**SNAPSHOT, "lastUpdateId": 300}, "session-2")
    feed(service, "ws", delta(301), "session-2")
    feed(service, "ws", trade(2), "session-2")
    assert service.projection.ready
    verify(path, service.trace)


def test_writer_restart_recovers_durable_unprocessed_raw_tail(setup):
    path, journal, service = setup
    raw = service.capture(
        "ws", msgspec.json.encode(delta(102, asks=[["100.00", "0"]])), "session-1"
    )
    old_trace = service.trace[:]
    assert [r.raw_seq for r in journal.pending()] == [raw.raw_seq]
    journal.close()
    with Journal(path) as reopened:
        recovered = MarketService(reopened, record_trace=True)
        asyncio.run(recovered.recover_pending())
        assert recovered.projection.last_update_id == 102
        assert recovered.projection.book.asks.index(0)[0] == 15000
        assert not list(reopened.pending())
        verify(path, old_trace + recovered.trace)
    # Fixture owns the final close; sqlite close is idempotent.


def test_consumer_restart_uses_cursor_and_full_book_atomically(setup, tmp_path):
    path, journal, service = setup
    state_path = tmp_path / "decision.sqlite"
    first = Consumer(state_path, record_trace=True)
    prefix = list(journal.records())[:5]
    for record in prefix:
        first.apply(record)
    first.commit()
    first.close()
    second = Consumer(state_path, record_trace=True)
    list(replay(path, second))
    assert prefix[-1].event.market_seq + len(second.trace) == journal.frontier
    assert second.projection.book_hash == service.projection.book_hash
    assert second.trace == service.trace[len(prefix) :]
    second.close()


def test_checkpoint_plus_tail_equals_full_replay(setup):
    path, journal, service = setup
    for i in range(102, 112):
        feed(service, "ws", delta(i, bids=[["98.00", str(i)]]))
    assert journal.db.execute("SELECT COUNT(*) FROM checkpoints").fetchone()[0] > 0
    recovered = journal.recover()
    assert recovered.checkpoint() == service.projection.checkpoint()
    verify(path, service.trace)


def test_transaction_failure_does_not_advance_durable_frontier(setup, monkeypatch):
    _, journal, service = setup
    seq = journal.frontier
    raw = service.capture("ws", msgspec.json.encode(trade(2)), "session-1")

    def fail(*a, **k):
        raise OSError("injected disk failure")

    monkeypatch.setattr(journal, "commit", fail)
    with pytest.raises(OSError):
        asyncio.run(service.process(raw))
    assert journal.frontier == seq
    assert journal.recover().last_trade_id == 1
    assert list(journal.pending())
    with pytest.raises(RuntimeError):
        service.capture("ws", b"{}", "session-1")


def test_checksum_corruption_is_detected(setup):
    _, journal, _ = setup
    with journal.db:
        journal.db.execute("UPDATE events SET digest=? WHERE market_seq=2", ("f" * 64,))
    with pytest.raises(IntegrityError):
        list(journal.records())


def test_two_market_writers_are_rejected(setup):
    path, _, _ = setup
    with pytest.raises(RuntimeError, match="another market writer"):
        Journal(path)


def test_integer_conversion_does_not_depend_on_decimal_context():
    with localcontext() as ctx:
        ctx.prec = 3
        assert units("123456.12345678", "0.00000001") == 12345612345678
    with pytest.raises(ValueError):
        units("0.000000001", "0.00000001")


def test_conflicting_trade_identity_invalidates_instead_of_silent_dedup(setup):
    _, journal, service = setup
    message = {**trade(), "q": "0.00000002"}
    assert feed(service, "ws", message)
    assert not service.projection.ready
    assert (
        "conflicting_or_unresolved_late_trade" in list(journal.records())[-2].event.payload.reason
    )


def test_committed_raw_corruption_is_detected(setup):
    path, journal, _ = setup
    with journal.db:
        journal.db.execute("UPDATE raw_frames SET payload=? WHERE raw_seq=1", (b"tampered",))
    with pytest.raises(IntegrityError, match="raw checksum"):
        verify(path)


def test_quantity_recovery_format_is_context_independent():
    from market_service.book import decimal_text

    with localcontext() as ctx:
        ctx.prec = 3
        assert decimal_text(12345612345678, "0.00000001") == "123456.12345678"
    with pytest.raises(ValueError, match="positive"):
        units("1", "0")


def test_contract_rejects_unknown_fields_and_unsupported_schema(setup):
    _, journal, _ = setup
    event = list(journal.records())[-1].event
    for update in ({"schema_version": 2}, {"unrecognized_field": True}, {"market_seq": -1}):
        data = msgspec.to_builtins(event)
        data.update(update)
        with pytest.raises(msgspec.ValidationError):
            decode_event(msgspec.msgpack.encode(data))


def test_consumer_checkpoint_has_single_writer(tmp_path):
    state = tmp_path / "consumer.sqlite"
    first = Consumer(state)
    try:
        with pytest.raises(RuntimeError, match="another consumer"):
            Consumer(state)
    finally:
        first.close()


def test_consumer_does_not_resume_into_another_dataset(setup, tmp_path):
    path, _, _ = setup
    first = Consumer()
    list(replay(path, first))
    with Journal(tmp_path / "different.sqlite"):
        with pytest.raises(IntegrityError, match="another dataset"):
            list(replay(tmp_path / "different.sqlite", first))


def test_restart_in_new_epoch_cannot_seed_previous_connection_book(setup):
    path, journal, service = setup
    asyncio.run(service.control("start", "session-2"))
    service.capture("ws", msgspec.json.encode(delta(102)), "session-2")
    journal.close()
    with Journal(path) as reopened:
        recovered = MarketService(reopened)
        assert recovered.projection.snapshot_epoch != recovered.projection.epoch
        asyncio.run(recovered.recover_pending())
        assert recovered.projection.quality["book"] == "INVALID"
        assert recovered.projection.last_update_id == 101
        assert not recovered.projection.ready
