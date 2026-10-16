"""Offline protocol tests use explicitly synthetic trades, never real acceptance evidence."""

import asyncio
import hashlib
import signal
import subprocess
import sys
import time

import msgspec
import pytest
from conftest import feed, trade

from market_service.book import IntegrityError
from market_service.repair import RepairWorker
from market_service.repair_store import RepairStore
from market_service.source import Consumer
from market_service.store import Journal
from market_service.trade_history import Reply, parse_trades


def rows(first, limit):
    return [
        {
            "id": i,
            "price": "100.00",
            "qty": "0.00000001",
            "time": 1792000000000,
            "isBuyerMaker": False,
        }
        for i in range(first, first + limit)
    ]


class SyntheticClient:
    def __init__(self):
        self.requests = []

    async def fetch(self, first, limit):
        self.requests.append((first, limit))
        return Reply(
            "synthetic://offline-test", 200, msgspec.json.encode(rows(first, limit)), time.time_ns()
        )


@pytest.fixture
def gap112(setup, tmp_path):
    path, journal, service = setup
    feed(service, "ws", trade(114))
    catalog = tmp_path / "repairs.sqlite"
    return path, journal, service, catalog


def test_112_pagination_duplicate_and_knowledge_cutoff(gap112):
    path, journal, service, target = gap112
    original = hashlib.sha256(path.read_bytes()).hexdigest()
    with RepairStore(target, path) as store:
        store.sync_observed()
        assert store.continuity(1, 114, view="observed")["missing_ranges"] == [[2, 113]]
        cutoff = time.time_ns()
        time.sleep(0.001)
        client = SyntheticClient()
        result = asyncio.run(RepairWorker(store, client, page_size=32).run_once())
        assert result["inserted"] == 112
        assert client.requests == [(2, 32), (34, 32), (66, 32), (98, 16)]
        assert store.gaps()[0]["state"] == "REPAIRED"
        assert store.continuity(1, 114)["complete"]
        assert not store.continuity(1, 114, known_by_ns=cutoff)["complete"]
        assert store.continuity(1, 114, view="observed")["missing_ranges"] == [[2, 113]]
        sid = store.capture_reply(
            1,
            2,
            32,
            Reply("synthetic://overlap", 200, msgspec.json.encode(rows(2, 32)), time.time_ns()),
            stop_id=113,
        )
        assert store.process_reply(sid)["duplicates"] == 32
        assert store.process_reply(sid)["already_processed"]
        assert len(list(store.research(1, 114))) == 114
        assert not list(store.research(1, 114))[0].execution_allowed
        with pytest.raises(IntegrityError, match="historical repair"):
            Consumer().apply(next(store.research(1, 114)))
        with pytest.raises(IntegrityError):
            with Journal(target, readonly=True):
                pass
    assert hashlib.sha256(path.read_bytes()).hexdigest() == original


def test_cursor_resume_requests_only_remaining_ids(gap112):
    path, _, _, target = gap112
    with RepairStore(target, path) as store:
        result = asyncio.run(
            RepairWorker(store, SyntheticClient(), page_size=32).run_once(max_pages=1)
        )
        assert result["inserted"] == 32 and store.gaps()[0]["cursor"] == 34
    client = SyntheticClient()
    with RepairStore(target, path) as store:
        assert store.sync_observed() == 0
        asyncio.run(RepairWorker(store, client, page_size=32).run_once())
        assert client.requests[0] == (34, 32)
        assert store.continuity(1, 114)["complete"]
        assert store.db.execute("SELECT COUNT(*) FROM facts").fetchone()[0] == 114


@pytest.mark.parametrize("bad", ["empty", "hole", "duplicate", "agg", "negative", "boolean_id"])
def test_bad_pages_do_not_advance(gap112, bad):
    path, _, _, target = gap112
    with RepairStore(target, path) as store:
        store.sync_observed()
        data = rows(2, 3)
        if bad == "empty":
            data = []
        if bad == "hole":
            data.pop(1)
        if bad == "duplicate":
            data[1] = data[0]
        if bad == "agg":
            data = [{"a": 2, "f": 2, "l": 4, "q": "1", "p": "100"}]
        if bad == "negative":
            data[0]["qty"] = "-1"
        if bad == "boolean_id":
            data[0]["id"] = True
        sid = store.capture_reply(
            1,
            2,
            3,
            Reply("synthetic://bad", 200, msgspec.json.encode(data), time.time_ns()),
            stop_id=113,
        )
        with pytest.raises((ValueError, TypeError)):
            store.process_reply(sid)
        assert store.gaps()[0]["cursor"] == 2
        assert store.db.execute("SELECT COUNT(*) FROM facts").fetchone()[0] == 2
        assert store.db.execute("SELECT error FROM sources WHERE id=?", (sid,)).fetchone()[0]


def test_conflict_rolls_back_whole_page(gap112):
    path, _, _, target = gap112
    with RepairStore(target, path) as store:
        store.sync_observed()
        # Deliberately include an existing observed identity with a conflicting price.
        data = rows(1, 2)
        data[0]["price"] = "101.00"
        sid = store.capture_reply(
            1,
            1,
            2,
            Reply("synthetic://conflict", 200, msgspec.json.encode(data), time.time_ns()),
            stop_id=113,
        )
        with pytest.raises(IntegrityError, match="conflicting_trade"):
            store.process_reply(sid)
        assert store.gaps()[0]["state"] == "CONFLICT"
        assert not store.continuity(1, 114)["complete"]
        assert store.db.execute("SELECT COUNT(*) FROM facts").fetchone()[0] == 2


def test_http_error_preserves_evidence_and_backoff(gap112):
    path, _, _, target = gap112

    class Unavailable:
        blocked_until_ns = time.time_ns() + 60_000_000_000

        async def fetch(self, first, limit):
            return Reply("synthetic://rate-limit", 429, b'{"code":-1003}', time.time_ns())

    with RepairStore(target, path) as store:
        asyncio.run(RepairWorker(store, Unavailable()).run_once())
        gap = store.gaps()[0]
        assert gap["state"] == "OPEN" and gap["cursor"] == 2
        assert gap["next_attempt_ns"] >= Unavailable.blocked_until_ns
        assert store.db.execute("SELECT COUNT(*) FROM sources WHERE status=429").fetchone()[0] == 1


def test_bounds_lock_dataset_and_original_rejection(gap112, tmp_path):
    path, _, _, target = gap112
    with pytest.raises(ValueError):
        RepairStore(path, path)
    with RepairStore(target, path) as store:
        store.sync_observed()
        with pytest.raises(RuntimeError):
            RepairStore(target, path)
        assert not store.continuity(0, 115)["complete"]
        with pytest.raises(ValueError):
            store.continuity(114, 1)
    other = tmp_path / "other.sqlite"
    with Journal(other):
        pass
    with pytest.raises(IntegrityError, match="different dataset"):
        RepairStore(target, other)


@pytest.mark.parametrize("phase", ["after_raw", "before_commit", "after_commit"])
def test_sigkill_repair_recovers_exactly_once(gap112, phase):
    path, _, _, target = gap112
    with RepairStore(target, path) as store:
        store.sync_observed()
    code = """
import os,signal,time,sys
import msgspec
from market_service.repair_store import RepairStore
from market_service.trade_history import Reply
catalog,source,phase=sys.argv[1:]
with RepairStore(catalog,source) as store:
 rows=[{"id":i,"price":"100.00","qty":"0.00000001","time":1792000000000,"isBuyerMaker":False} for i in range(2,34)]
 sid=store.capture_reply(1,2,32,Reply("synthetic://kill-test",200,msgspec.json.encode(rows),time.time_ns()),stop_id=113)
 if phase=="after_raw":os.kill(os.getpid(),signal.SIGKILL)
 if phase=="before_commit":
  def hook(sql):
   if sql=="COMMIT":os.kill(os.getpid(),signal.SIGKILL)
  store.db.set_trace_callback(hook)
 store.process_reply(sid)
 os.kill(os.getpid(),signal.SIGKILL)
"""
    proc = subprocess.run([sys.executable, "-c", code, str(target), str(path), phase], check=False)
    assert proc.returncode == -signal.SIGKILL
    with RepairStore(target, path) as store:
        asyncio.run(RepairWorker(store, SyntheticClient(), page_size=32).run_once())
        assert store.continuity(1, 114)["complete"]
        assert store.db.execute("SELECT COUNT(*) FROM facts").fetchone()[0] == 114
        assert all(not item.execution_allowed for item in store.research(1, 114))


def test_timestamp_resolution_compatible_but_not_invented(gap112):
    from market_service.trade_history import compatible

    path, _, _, target = gap112
    with RepairStore(target, path) as store:
        store.sync_observed()
        data = rows(1, 1)
        data[0]["time"] = 1792000000000123
        precise = parse_trades(msgspec.json.encode(data), store.spec, 1000)[0]
        coarse = next(store.research(1, 1)).payload
        assert compatible(coarse, 1_000_000, precise, 1000)
        assert precise.trade_time_ns != coarse.trade_time_ns


def test_catalog_readers_do_not_take_writer_lease(gap112):
    import sqlite3

    path, _, _, target = gap112
    with RepairStore(target, path) as writer:
        writer.sync_observed()
        with RepairStore(target, path, readonly=True) as reader:
            assert reader.continuity(1, 114)["missing_ranges"] == [[2, 113]]
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                reader.db.execute("DELETE FROM facts")
            asyncio.run(RepairWorker(writer, SyntheticClient()).run_once())
            assert reader.continuity(1, 114)["complete"]


def test_retry_after_survives_restart_and_manual_retry(gap112):
    path, _, _, target = gap112
    with RepairStore(target, path) as store:
        store.sync_observed()
        until = time.time_ns() + 120_000_000_000
        sid = store.capture_reply(
            1,
            2,
            10,
            Reply("synthetic://429", 429, b"[]", time.time_ns(), retry_after_ns=until),
            stop_id=113,
        )
        # Crash between raw response commit and processing is also covered by this boundary.
    client = SyntheticClient()
    with RepairStore(target, path) as store:
        result = asyncio.run(RepairWorker(store, client).run_once(retry_now=True))
        assert not client.requests and not result["all_discovered_gaps_repaired"]
        assert result["endpoint_blocked_until_ns"] == until
        assert (
            store.db.execute("SELECT processed FROM sources WHERE id=?", (sid,)).fetchone()[0] == 1
        )


def test_pending_raw_corruption_is_not_hidden(gap112):
    path, _, _, target = gap112
    with RepairStore(target, path) as store:
        store.sync_observed()
        sid = store.capture_reply(
            1,
            2,
            2,
            Reply("synthetic://corruption", 200, msgspec.json.encode(rows(2, 2)), time.time_ns()),
            stop_id=113,
        )
        with store.db:
            store.db.execute("UPDATE sources SET payload=? WHERE id=?", (b"[]", sid))
        with pytest.raises(IntegrityError, match="checksum"):
            store.recover_pending()


def test_future_conflict_does_not_leak_into_observed_or_asof(gap112):
    path, _, _, target = gap112
    with RepairStore(target, path) as store:
        store.sync_observed()
        asyncio.run(RepairWorker(store, SyntheticClient()).run_once())
        cutoff = time.time_ns()
        bad = rows(1, 1)
        bad[0]["price"] = "101.00"
        sid = store.capture_reply(
            1,
            1,
            1,
            Reply("synthetic://late-conflict", 200, msgspec.json.encode(bad), time.time_ns()),
            stop_id=113,
        )
        with pytest.raises(IntegrityError):
            store.process_reply(sid)
        assert store.continuity(1, 114, known_by_ns=cutoff)["complete"]
        assert not store.continuity(1, 114)["complete"]
        assert store.continuity(1, 1, view="observed")["complete"]


def test_historical_client_only_calls_public_get():
    from market_service.trade_history import HistoricalClient

    class Response:
        status = 200
        url = "https://example.invalid/api/v3/historicalTrades?symbol=BTCUSDT&fromId=2&limit=4"
        headers = {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def read(self):
            return msgspec.json.encode(rows(2, 4))

    class HTTP:
        def get(self, url, **kwargs):
            assert url == "https://example.invalid/api/v3/historicalTrades"
            assert kwargs["params"] == {"symbol": "BTCUSDT", "fromId": 2, "limit": 4}
            assert "headers" not in kwargs
            return Response()

    reply = asyncio.run(HistoricalClient(HTTP(), "https://example.invalid").fetch(2, 4))
    assert reply.status == 200 and len(msgspec.json.decode(reply.body)) == 4


def test_aggregate_comparison_reports_missing_without_expanding(gap112):
    from market_service.history_compare import compare_aggregates

    path, _, _, target = gap112
    with RepairStore(target, path) as store:
        store.sync_observed()
        trades = [r.payload for r in store.research(1, 114, view="observed")]
        aggregate = [
            {
                "a": 10,
                "f": 1,
                "l": 114,
                "p": "100.00",
                "q": "0.00000114",
                "T": 1792000000000,
                "m": False,
            }
        ]
        result = compare_aggregates(trades, aggregate, store.spec)
        assert result["missing_raw_count"] == 112
        assert result["missing_volume_btc"] == "0.00000112"
        assert not result["raw_gap_repaired"] and not result["execution_allowed"]
        assert not store.continuity(1, 114)["complete"]


def test_foreign_database_rejected_before_schema_changes(gap112, tmp_path):
    import sqlite3

    path, _, _, _ = gap112
    foreign = tmp_path / "foreign.sqlite"
    with sqlite3.connect(foreign) as db:
        db.execute("CREATE TABLE important(value TEXT)")
        db.execute("INSERT INTO important VALUES('keep')")
    before = hashlib.sha256(foreign.read_bytes()).hexdigest()
    with pytest.raises(IntegrityError, match="not a repair"):
        RepairStore(foreign, path)
    assert hashlib.sha256(foreign.read_bytes()).hexdigest() == before


def test_history_download_does_not_truncate_end_millisecond(tmp_path, monkeypatch):
    from pathlib import Path

    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1]))
    from scripts import repair_acceptance

    def aggregate(i):
        return {"a": i, "f": i, "l": i, "p": "100.00", "q": "1", "T": 1000, "m": False}

    pages = [[aggregate(i) for i in range(1000)], [aggregate(1000)]]

    class Response:
        status = 200
        url = "https://example.invalid/api/v3/aggTrades"

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def read(self):
            return msgspec.json.encode(pages.pop(0))

        def raise_for_status(self):
            pass

    class HTTP:
        calls = 0

        def get(self, url, *, params):
            if self.calls:
                assert params["fromId"] == 1000
            self.calls += 1
            return Response()

    async def no_wait(*args):
        pass

    monkeypatch.setattr(repair_acceptance.asyncio, "sleep", no_wait)
    result = asyncio.run(
        repair_acceptance.download_aggregates(
            HTTP(), "https://example.invalid", tmp_path, "test", 1000, 1000
        )
    )
    assert len(result) == 1001 and result[-1]["a"] == 1000
