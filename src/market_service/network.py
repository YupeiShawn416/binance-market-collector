"""Public-only Binance transport; normalization never performs network I/O."""

from __future__ import annotations

import asyncio
import time
import uuid
from contextlib import suppress

import aiohttp

from .service import MarketService

REST_BASE = "https://data-api.binance.vision"
WS_BASE = "wss://data-stream.binance.vision"


async def collect(
    service: MarketService,
    *,
    seconds: float = 60,
    reconnect_after: float | None = None,
    rest_base: str = REST_BASE,
    ws_base: str = WS_BASE,
    stale_seconds: float = 10,
) -> dict:
    """Record a bounded live run; no keys, account requests, or order methods exist."""
    await service.recover_pending()
    started = time.monotonic()
    deadline = started + seconds
    reconnects = 0
    forced = False
    received = 0
    errors = []
    async with aiohttp.ClientSession(
        trust_env=True, timeout=aiohttp.ClientTimeout(total=20)
    ) as http:

        async def get(path: str, session_id: str, kind: str):
            async with http.get(rest_base + path) as response:
                payload = await response.read()
                wall, mono = time.time_ns(), time.monotonic_ns()
                response.raise_for_status()
            raw = service.capture(kind, payload, session_id, wall_ns=wall, mono_ns=mono)
            await service.process(raw)

        await get("/api/v3/exchangeInfo?symbol=BTCUSDT", "metadata", "metadata")
        while time.monotonic() < deadline:
            session_id = str(uuid.uuid4())
            queue = asyncio.Queue(maxsize=2048)
            receiver = None
            connected = False
            try:
                url = ws_base + "/stream?streams=btcusdt@depth@100ms/btcusdt@trade"
                async with http.ws_connect(
                    url, autoping=True, heartbeat=None, max_msg_size=2**20
                ) as ws:
                    connected = True
                    await service.control("start", session_id)

                    async def receive(ws=ws, queue=queue, session_id=session_id):
                        nonlocal received
                        try:
                            async for message in ws:
                                if message.type in (
                                    aiohttp.WSMsgType.TEXT,
                                    aiohttp.WSMsgType.BINARY,
                                ):
                                    wall, mono = time.time_ns(), time.monotonic_ns()
                                    payload = (
                                        message.data.encode()
                                        if isinstance(message.data, str)
                                        else message.data
                                    )
                                    raw = service.capture(
                                        "ws", payload, session_id, wall_ns=wall, mono_ns=mono
                                    )
                                    received += 1
                                    await queue.put(raw)
                                elif message.type == aiohttp.WSMsgType.ERROR:
                                    raise ConnectionError(str(ws.exception()))
                        finally:
                            with suppress(asyncio.QueueFull):
                                queue.put_nowait(None)

                    receiver = asyncio.create_task(receive(), name="binance-public-receive")
                    await get("/api/v3/depth?symbol=BTCUSDT&limit=5000", session_id, "snapshot")
                    last_book = time.monotonic()
                    while time.monotonic() < deadline:
                        remaining = deadline - time.monotonic()
                        raw = await asyncio.wait_for(queue.get(), min(stale_seconds, remaining))
                        if raw is None:
                            if receiver.done() and not receiver.cancelled():
                                receiver.result()
                            raise ConnectionError("websocket_closed")
                        before = service.projection.last_update_id
                        resync = await service.process(raw)
                        if service.projection.last_update_id != before:
                            last_book = time.monotonic()
                        if resync:
                            # The current connection is an explicit data-quality boundary.
                            raise ConnectionError("normalizer_requested_resync")
                        if time.monotonic() - last_book > stale_seconds:
                            raise ConnectionError("book_stale")
                        if (
                            reconnect_after
                            and not forced
                            and time.monotonic() - started >= reconnect_after
                        ):
                            forced = True
                            raise ConnectionError("acceptance_forced_reconnect")
            except (TimeoutError, aiohttp.ClientError, ConnectionError) as exc:
                if time.monotonic() < deadline:
                    errors.append(f"{type(exc).__name__}: {exc}")
                    reconnects += 1
            finally:
                if receiver is not None:
                    receiver.cancel()
                    with suppress(asyncio.CancelledError, ConnectionError, aiohttp.ClientError):
                        await receiver
                # All accepted frames are already durable. Normalize queued tail before the boundary.
                while not queue.empty():
                    frame = queue.get_nowait()
                    if frame is not None:
                        await service.process(frame)
                if connected and not service.poisoned:
                    await service.control(
                        "stop", session_id, "connection_closed_or_capture_complete"
                    )
            if time.monotonic() < deadline:
                await asyncio.sleep(min(1.0, deadline - time.monotonic()))
    return {
        "received_ws_frames": received,
        "reconnects": reconnects,
        "forced_reconnect": forced,
        "network_boundaries": errors,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
