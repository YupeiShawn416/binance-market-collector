"""Real local HTTP/WebSocket faults exercise the transport, not a mocked collector."""

import asyncio
import time
from contextlib import asynccontextmanager

import msgspec
import pytest
from aiohttp import web
from conftest import METADATA, SNAPSHOT, delta, trade

from market_service.network import CaptureStats, collect, retry_after
from market_service.service import MarketService
from market_service.store import Journal
from market_service.verification import verify


@asynccontextmanager
async def exchange(*, metadata_failures=0, rate_limit=False, gap=False, silent=False):
    calls = {"metadata": 0, "ws": 0, "snapshot": 0, "trade": 0}

    async def metadata(request):
        calls["metadata"] += 1
        if calls["metadata"] <= metadata_failures:
            return web.Response(status=429 if rate_limit else 503, headers={"Retry-After": "2"})
        return web.json_response(METADATA)

    async def depth(request):
        calls["snapshot"] += 1
        return web.json_response(SNAPSHOT)

    async def websocket(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        calls["ws"] += 1
        if silent:
            async for _ in ws:
                pass
            return ws

        async def send():
            update = 101
            try:
                while not ws.closed:
                    await ws.send_bytes(
                        msgspec.json.encode(delta(update + (10 if gap and calls["ws"] == 1 else 0)))
                    )
                    calls["trade"] += 1
                    await ws.send_bytes(msgspec.json.encode(trade(calls["trade"])))
                    update += 1
                    await asyncio.sleep(0.01)
            except (ConnectionResetError, RuntimeError):
                pass

        sender = asyncio.create_task(send())
        try:
            async for _ in ws:
                pass
        finally:
            sender.cancel()
            await asyncio.gather(sender, return_exceptions=True)
        return ws

    app = web.Application()
    app.router.add_get("/api/v3/exchangeInfo", metadata)
    app.router.add_get("/api/v3/depth", depth)
    app.router.add_get("/stream", websocket)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}", f"ws://127.0.0.1:{port}", calls
    finally:
        await runner.cleanup()


def test_metadata_retry_reconnect_gap_and_hash_replay(tmp_path):
    async def scenario():
        async with exchange(metadata_failures=1, gap=True) as (rest, ws, calls):
            path = tmp_path / "market.sqlite"
            with Journal(path) as journal:
                service = MarketService(journal, record_trace=True)
                stats = await collect(
                    service,
                    seconds=0.3,
                    rest_base=rest,
                    ws_base=ws,
                    retry_initial=0.01,
                    retry_max=0.02,
                )
                result = verify(path, service.trace, expected_chain=service.final_chain)
                assert calls["metadata"] == 2
                assert calls["ws"] >= 2
                assert stats["reconnects"] >= 2
                assert result["pending_raw"] == 0
                assert result["counts"]["Trade"] > 0
                assert result["hash_transcript_equal"]
                assert service.projection.quality["connection"] == "DISCONNECTED"
                assert any(
                    "update" in b["reason"] or "gap" in b["reason"]
                    for b in result["quality_boundaries"]
                )

    asyncio.run(scenario())


def test_shutdown_interrupts_rate_limit_without_early_retry(tmp_path):
    async def scenario():
        async with exchange(metadata_failures=5, rate_limit=True) as (rest, ws, calls):
            with Journal(tmp_path / "market.sqlite") as journal:
                stop, stats = asyncio.Event(), CaptureStats()
                task = asyncio.create_task(
                    collect(
                        MarketService(journal),
                        seconds=None,
                        stop=stop,
                        stats=stats,
                        rest_base=rest,
                        ws_base=ws,
                    )
                )
                for _ in range(100):
                    if stats.retry_at_mono:
                        break
                    await asyncio.sleep(0.005)
                assert stats.retry_at_mono is not None
                assert stats.retry_at_mono - time.monotonic() > 1
                stop.set()
                await asyncio.wait_for(task, 1)
                assert calls["metadata"] == 1
                assert calls["ws"] == 0

    asyncio.run(scenario())


def test_stale_book_reconnects_and_stops_promptly(tmp_path):
    async def scenario():
        async with exchange(silent=True) as (rest, ws, calls):
            with Journal(tmp_path / "market.sqlite") as journal:
                result = await collect(
                    MarketService(journal),
                    seconds=0.18,
                    rest_base=rest,
                    ws_base=ws,
                    stale_seconds=0.04,
                    retry_initial=0.005,
                    retry_max=0.01,
                )
                assert calls["snapshot"] >= 2
                assert result["reconnects"] >= 1
                assert not list(journal.pending())

    asyncio.run(scenario())


def test_shutdown_drains_a_frame_cancelled_before_queue_publication(tmp_path, monkeypatch):
    original = asyncio.Queue.put
    entered = asyncio.Event()

    async def delayed_put(queue, frame):
        if frame is not None:
            entered.set()
            await asyncio.sleep(60)
        await original(queue, frame)

    async def scenario():
        async with exchange() as (rest, ws, _):
            with Journal(tmp_path / "market.sqlite") as journal:
                stop = asyncio.Event()
                monkeypatch.setattr(asyncio.Queue, "put", delayed_put)
                task = asyncio.create_task(
                    collect(
                        MarketService(journal), stop=stop, seconds=None, rest_base=rest, ws_base=ws
                    )
                )
                await asyncio.wait_for(entered.wait(), 1)
                stop.set()
                await asyncio.wait_for(task, 1)
                assert not list(journal.pending())
                assert verify(journal.path)["pending_raw"] == 0

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "header, expected", [("2", 2), (None, 120), ("bad", 120), ("nan", 120), ("-1", 120)]
)
def test_retry_after(header, expected):
    assert retry_after(header) == expected


def test_retry_after_http_date():
    assert retry_after("Thu, 01 Jan 1970 00:01:00 GMT", now=30) == 30
