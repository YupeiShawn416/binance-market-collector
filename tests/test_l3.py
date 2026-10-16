"""L3 protocol tests. All time-series messages below are SYNTHETIC, not live data."""

import asyncio
import csv
import json
import time
import zlib
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import msgspec
import pytest

from market_service.book import IntegrityError
from market_service.cli import l3_publisher
from market_service.contracts import Instrument
from market_service.ipc import consume
from market_service.l3.book import L3Projection
from market_service.l3.experiment import compare, sweep
from market_service.l3.kraken import timestamp_ns
from market_service.l3.network import credentials, token_provider
from market_service.l3.service import L3Consumer, L3Journal, L3Service, verify
from market_service.source import Consumer, replay
from market_service.store import Journal

SPEC = Instrument(
    "BTC-USD", "BTC", "USD", "0.1", "0.00000001", "unknown", "synthetic-research-precision"
)
STAMP = "2026-10-16T00:00:00.000000001Z"


def order(number, price, size, action=None):
    row = {"order_id": number, "limit_price": price, "order_qty": size, "timestamp": STAMP}
    if action:
        row["event"] = action
    return row


def fixture_crc(bids, asks):
    # Independent wire-oriented oracle; does not call the production projection.
    parts = []
    for rows, reverse in ((asks, False), (bids, True)):
        prices = sorted({Decimal(r["limit_price"]) for r in rows}, reverse=reverse)[:10]
        for price in prices:
            queue = [r for r in rows if Decimal(r["limit_price"]) == price]
            for row in sorted(queue, key=lambda r: r["timestamp"]):
                parts += [row[k].replace(".", "").lstrip("0") for k in ("limit_price", "order_qty")]
    return zlib.crc32("".join(parts).encode()) & 0xFFFFFFFF


def message(kind, bids, asks, checksum):
    return {
        "channel": "level3",
        "type": kind,
        "data": [{"symbol": "BTC/USD", "bids": bids, "asks": asks, "checksum": checksum}],
    }


def make_synthetic(path):
    with L3Journal(path) as journal:
        with journal.db:
            journal.db.execute("INSERT INTO metadata VALUES('data_origin','synthetic')")
        service = L3Service(journal, depth=10)
        service.install_spec(SPEC)
        service.control("fixture", "LIVE", "SYNTHETIC protocol test")
        base = time.monotonic_ns() + 1_000_000
        bids = [order("bid", "99.0", "1.00000000")]
        near, far = order("near", "100.0", "0.01000000"), order("far", "105.0", "1.00000000")
        frames = [
            (0, message("snapshot", bids, [near, far], fixture_crc(bids, [near, far]))),
            (20, message("update", [], [dict(near, event="delete")], fixture_crc(bids, [far]))),
            (
                80,
                message(
                    "update",
                    [],
                    [order("new", "100.0", "0.01000000", "add")],
                    fixture_crc(bids, [order("new", "100.0", "0.01000000"), far]),
                ),
            ),
            (
                200,
                message(
                    "update",
                    [order("bid", "99.0", "0.90000000", "modify")],
                    [],
                    fixture_crc(
                        [order("bid", "99.0", "0.90000000")],
                        [order("new", "100.0", "0.01000000"), far],
                    ),
                ),
            ),
        ]
        for offset, value in frames:
            raw = service.capture(
                "kraken-l3-ws",
                json.dumps(value).encode(),
                "fixture",
                mono_ns=base + offset * 1_000_000,
            )
            assert service.process(raw) is False
        return service.projection.book_hash


def test_official_kraken_crc_vector_and_nanoseconds(tmp_path):
    with L3Journal(tmp_path / "official.sqlite") as journal:
        service = L3Service(journal, depth=10)
        service.install_spec(SPEC)
        service.control("official-doc", "LIVE", "official documentation fixture, not live capture")
        body = (Path(__file__).parent / "fixtures/kraken-official-l3-snapshot.json").read_bytes()
        assert not service.process(service.capture("kraken-l3-ws", body, "official-doc"))
        assert service.projection.ready
        assert service.projection.checksum() == 1063832831
        assert len(service.projection.orders) == 35
        assert timestamp_ns("2024-01-08T12:26:39.526146327Z") % 1_000_000_000 == 526146327
        assert (
            L3Projection.restore(service.projection.checkpoint()).book_hash
            == service.projection.book_hash
        )


def test_synthetic_three_arm_comparison_has_no_lookahead(tmp_path):
    path = tmp_path / "synthetic.sqlite"
    make_synthetic(path)
    with pytest.raises(ValueError, match="explicitly live"):
        compare(path, tmp_path / "bad.csv")
    result = compare(path, tmp_path / "probes.csv", sizes=("0.001",), allow_synthetic=True)
    assert result["data_origin"] == "synthetic"
    assert result["event_l2_vs_l3_disagreements"] == 0
    assert result["vwap_different_probes"] == 6
    rows = list(csv.DictReader((tmp_path / "probes.csv").open()))
    at_50 = next(r for r in rows if r["arrival_offset_ms"] == "50.0" and r["side"] == "buy")
    assert Decimal(at_50["l3_vwap"]) == 105
    assert Decimal(at_50["l2_100ms_vwap"]) == 100
    assert at_50["l2_age_ms"] == "50.0"
    assert result["coverage_exhausted_probes"] == 0
    sampled = msgspec.msgpack.decode((tmp_path / result["derived_l2_samples_file"]).read_bytes())
    assert sampled["data_origin"] == "synthetic"
    assert all(s["source_receive_mono_ns"] <= s["boundary_mono_ns"] for s in sampled["samples"])
    assert sampled["samples"][0]["levels"]["buy"][0][0] == 1000
    assert sweep([(100, 2), (105, 3)], 6) == {
        "filled": 5,
        "notional": 515,
        "unknown_remainder": True,
    }


def test_l3_raw_pending_recovery_and_v1_isolation(tmp_path):
    path = tmp_path / "l3.sqlite"
    final_hash = make_synthetic(path)
    with L3Journal(path) as journal:
        service = L3Service(journal, depth=10)
        service.capture("kraken-l3-ws", b'{"channel":"heartbeat"}', "fixture")
    with L3Journal(path) as journal:
        service = L3Service(journal, depth=10)
        assert len(list(journal.pending())) == 1
        service.recover_pending()
        assert list(journal.pending()) == []
        assert service.projection.book_hash == final_hash
        record = next(journal.records())
        with pytest.raises(IntegrityError):
            Consumer().apply(record)
    with pytest.raises(IntegrityError, match="schema mismatch"):
        with Journal(path, readonly=True):
            pass
    assert verify(path)["final_order_state_hash"] == final_hash


def test_corrupt_crc_preserves_book_and_requires_snapshot(tmp_path):
    path = tmp_path / "l3.sqlite"
    final_hash = make_synthetic(path)
    with L3Journal(path) as journal:
        service = L3Service(journal, depth=10)
        bad = message("update", [], [order("far", "105.0", "0.80000000", "modify")], 0)
        assert service.process(service.capture("kraken-l3-ws", json.dumps(bad).encode(), "fixture"))
        assert not service.projection.ready
        assert service.projection.book_hash == final_hash
        assert service.projection.orders["far"].quantity_units == 100_000_000
        assert journal.recover().quality["book"] == "INVALID"


def test_depth_eviction_is_not_a_cancel(tmp_path):
    with L3Journal(tmp_path / "l3.sqlite") as journal:
        service = L3Service(journal, depth=10)
        service.install_spec(SPEC)
        service.control("s", "LIVE", "synthetic")
        bids = [order("b", "99.0", "1.00000000")]
        asks = [order(str(i), str(101 + i) + ".0", "1.00000000") for i in range(10)]
        snap = message("snapshot", bids, asks, fixture_crc(bids, asks))
        assert not service.process(service.capture("kraken-l3-ws", json.dumps(snap).encode(), "s"))
        better = order("better", "100.0", "1.00000000")
        change = message(
            "update", [], [dict(better, event="add")], fixture_crc(bids, [better] + asks)
        )
        assert not service.process(
            service.capture("kraken-l3-ws", json.dumps(change).encode(), "s")
        )
        assert "9" not in service.projection.orders
        assert "better" in service.projection.orders
        last = list(journal.records())[-1].event.payload
        assert len(last.changes) == 1 and last.changes[0].action == "add"


def test_l3_socket_two_consumers_and_checkpoint_resume(tmp_path, socket_path):
    path, checkpoint = tmp_path / "l3.sqlite", tmp_path / "cursor.sqlite"
    final_hash = make_synthetic(path)

    async def scenario():
        publisher = await l3_publisher(path, socket_path).start()
        try:
            first = L3Consumer(checkpoint, record_trace=True)
            await consume(socket_path, first, follow=False, stop_after=4)
            first.close()
            decision, trading = L3Consumer(checkpoint), L3Consumer(record_trace=True)
            await asyncio.gather(
                consume(socket_path, decision, follow=False),
                consume(socket_path, trading, follow=False),
            )
            assert decision.projection.book_hash == trading.projection.book_hash == final_hash
            assert decision.chain_hash == trading.chain_hash
            old = Consumer()
            with pytest.raises((asyncio.IncompleteReadError, ConnectionError)):
                await consume(socket_path, old, follow=False)
            decision.close()
            trading.close()
        finally:
            await publisher.close()

    asyncio.run(scenario())
    independent = L3Consumer()
    list(replay(path, independent))
    assert independent.projection.book_hash == final_hash


def test_credentials_local_only_and_permission_gate(tmp_path, monkeypatch):
    monkeypatch.delenv("KRAKEN_MARKET_KEY", raising=False)
    monkeypatch.delenv("KRAKEN_MARKET_SECRET", raising=False)
    with pytest.raises(ValueError, match="local credentials"):
        credentials()
    path = tmp_path / "synthetic-credentials.json"
    path.write_text(json.dumps({"key": "SYNTHETIC_KEY", "secret": "SYNTHETIC_SECRET"}))
    path.chmod(0o644)
    with pytest.raises(ValueError, match="0600"):
        credentials(path)
    path.chmod(0o600)
    assert credentials(path) == ("SYNTHETIC_KEY", "SYNTHETIC_SECRET")


def test_sdk_reuse_calls_only_token_endpoint(monkeypatch):
    sdk = pytest.importorskip("kraken.spot")
    requests = []

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def request(self, **kwargs):
            requests.append(kwargs)
            return {"token": "SYNTHETIC_TOKEN"}

    monkeypatch.setattr(sdk, "SpotAsyncClient", FakeClient)
    assert asyncio.run(token_provider("SYNTHETIC_KEY", "SYNTHETIC_SECRET")) == "SYNTHETIC_TOKEN"
    assert requests == [{"method": "POST", "uri": "/0/private/GetWebSocketsToken", "timeout": 10}]


def test_network_retains_market_bytes_but_never_auth_material(tmp_path, monkeypatch):
    import aiohttp

    from market_service.l3.network import collect

    body = (Path(__file__).parent / "fixtures/kraken-official-l3-snapshot.json").read_text()
    sent = []

    class FakeWS:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def send_json(self, value):
            sent.append(value)

        def __aiter__(self):
            async def messages():
                yield SimpleNamespace(
                    type=aiohttp.WSMsgType.TEXT,
                    data=json.dumps(
                        {"method": "subscribe", "success": True, "token": "SYNTHETIC_TOKEN"}
                    ),
                )
                yield SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=body)

            return messages()

    class FakeSession:
        def __init__(self, **kwargs):
            self.calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        def ws_connect(self, url, **kwargs):
            self.calls += 1
            if self.calls > 1:
                raise ConnectionError("synthetic end")
            assert url == "wss://ws-l3.kraken.com/v2"
            return FakeWS()

    async def fake_token(key, secret):
        return "SYNTHETIC_TOKEN"

    monkeypatch.setattr(aiohttp, "ClientSession", FakeSession)
    with L3Journal(tmp_path / "network.sqlite") as journal:
        service = L3Service(journal, depth=10)
        service.install_spec(SPEC)
        result = asyncio.run(
            collect(
                service,
                key="SYNTHETIC_KEY",
                secret="SYNTHETIC_SECRET",
                get_token=fake_token,
                seconds=0.05,
            )
        )
        assert result["received_l3_frames"] == 1
        assert sent[0]["params"]["token"] == "SYNTHETIC_TOKEN"
        saved = journal.db.execute(
            "SELECT payload FROM raw_frames WHERE kind='kraken-l3-ws'"
        ).fetchall()
        assert saved == [(body.encode(),)]
        for table in ("raw_frames", "events"):
            payloads = b"".join(r[0] for r in journal.db.execute(f"SELECT payload FROM {table}"))
            assert b"SYNTHETIC_TOKEN" not in payloads
            assert b"SYNTHETIC_KEY" not in payloads
            assert b"SYNTHETIC_SECRET" not in payloads
        assert service.projection.quality["connection"] == "DISCONNECTED"


def test_wrong_version_writer_does_not_modify_existing_log(tmp_path):
    import hashlib

    path = tmp_path / "l3.sqlite"
    make_synthetic(path)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(IntegrityError):
        Journal(path)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


def test_snapshot_queue_order_changes_crc_even_when_l2_is_same(tmp_path):
    with L3Journal(tmp_path / "queue.sqlite") as journal:
        service = L3Service(journal, depth=10)
        service.install_spec(SPEC)
        service.control("s", "LIVE", "synthetic")
        bids = [order("b", "99.0", "1.00000000")]
        asks = [order("a1", "100.0", "0.10000000"), order("a2", "100.0", "0.20000000")]
        body = json.dumps(message("snapshot", bids, asks, fixture_crc(bids, asks))).encode()
        assert not service.process(service.capture("kraken-l3-ws", body, "s"))
        old_crc, old_hash = service.projection.checksum(), service.projection.book_hash
        changed = dict(asks[0], timestamp="2026-10-16T00:00:00.000000002Z", event="modify")
        body = json.dumps(
            message("update", [], [changed], fixture_crc(bids, [changed, asks[1]]))
        ).encode()
        assert not service.process(service.capture("kraken-l3-ws", body, "s"))
        assert service.projection.book.asks.to_list() == [(1000, 30_000_000)]
        assert service.projection.checksum() != old_crc
        assert service.projection.book_hash != old_hash


def test_numeric_scientific_notation_keeps_exact_satoshi(tmp_path):
    with L3Journal(tmp_path / "precision.sqlite") as journal:
        service = L3Service(journal, depth=10)
        service.install_spec(SPEC)
        service.control("s", "LIVE", "synthetic")
        bids = [order("b", "99.0", "0.00000001")]
        asks = [order("a", "100.0", "0.00000001")]
        body = (
            json.dumps(message("snapshot", bids, asks, fixture_crc(bids, asks)))
            .replace('"0.00000001"', "1e-8")
            .encode()
        )
        assert not service.process(service.capture("kraken-l3-ws", body, "s"))
        assert service.projection.orders["a"].quantity_units == 1
        assert service.projection.orders["a"].quantity_text == "0.00000001"
