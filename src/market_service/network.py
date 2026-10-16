"""Public Binance transport. Retries and cancellation do not alter event semantics."""

from __future__ import annotations

import asyncio
import random
import time
import uuid
from collections import deque
from collections.abc import Awaitable
from contextlib import suppress
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime

import aiohttp

from .service import MarketService

REST_BASE = "https://data-api.binance.vision"
WS_BASE = "wss://data-stream.binance.vision"


@dataclass
class CaptureStats:
    received: int = 0
    reconnects: int = 0
    forced: bool = False
    last_ws_mono: float | None = None
    last_book_mono: float | None = None
    last_trade_mono: float | None = None
    retry_at_mono: float | None = None
    errors: deque[str] = field(default_factory=lambda: deque(maxlen=64))

    def report(self) -> dict:
        return {
            "received_ws_frames": self.received,
            "reconnects": self.reconnects,
            "forced_reconnect": self.forced,
            "network_boundaries": list(self.errors),
        }


class Stopped(Exception):
    """Internal cooperative shutdown, including during REST calls and retry waits."""


async def interruptible(operation: Awaitable, stop: asyncio.Event, timeout: float):
    task = asyncio.ensure_future(operation)
    stopper = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait(
            (task, stopper), timeout=max(0, timeout), return_when=asyncio.FIRST_COMPLETED
        )
        if task in done:
            return task.result()
        if stopper in done:
            raise Stopped
        if task not in done:
            raise TimeoutError("transport deadline")
        return task.result()
    finally:
        for pending in (task, stopper):
            if not pending.done():
                pending.cancel()
        await asyncio.gather(task, stopper, return_exceptions=True)


def retry_after(value: str | None, *, now: float | None = None) -> float:
    """Never shorten the server's ban/rate-limit window. Invalid headers get 120s."""
    if value:
        try:
            seconds = float(value)
            if 0 <= seconds < float("inf"):
                return seconds
        except ValueError:
            try:
                return max(0, parsedate_to_datetime(value).timestamp() - (now or time.time()))
            except (ValueError, TypeError, OverflowError):
                pass
    return 120.0


class RateLimited(ConnectionError):
    def __init__(self, status: int, delay: float):
        self.delay = delay
        super().__init__(f"HTTP {status}; Retry-After {delay:.1f}s")


async def collect(
    service: MarketService,
    *,
    seconds: float | None = 60,
    reconnect_after: float | None = None,
    rest_base: str = REST_BASE,
    ws_base: str = WS_BASE,
    stale_seconds: float = 10,
    stop: asyncio.Event | None = None,
    stats: CaptureStats | None = None,
    retry_initial: float = 1,
    retry_max: float = 60,
) -> dict:
    """Bounded capture or an indefinitely supervised stream when seconds=None.

    A single journal/dataset survives restarts. Shutdown drains durable raw inputs
    before recording the disconnect; retry history is bounded independently of uptime.
    """
    if seconds is not None and seconds <= 0:
        raise ValueError("seconds must be positive or None")
    if stale_seconds <= 0 or not 0 < retry_initial <= retry_max:
        raise ValueError("invalid transport intervals")
    stop = stop if stop is not None else asyncio.Event()
    stats = stats if stats is not None else CaptureStats()
    await service.recover_pending()
    started = time.monotonic()
    deadline = started + seconds if seconds is not None else float("inf")
    metadata_loaded = False
    failures = 0
    async with aiohttp.ClientSession(
        trust_env=True, timeout=aiohttp.ClientTimeout(total=20)
    ) as http:

        async def get(path: str, session_id: str, kind: str):
            async with http.get(rest_base + path) as response:
                if response.status in (418, 429):
                    raise RateLimited(
                        response.status, retry_after(response.headers.get("Retry-After"))
                    )
                response.raise_for_status()
                # StreamReader.read(n) can return a partial body, so iterate to EOF.
                parts, size = [], 0
                async for chunk in response.content.iter_chunked(65536):
                    size += len(chunk)
                    if size > 8 * 1024 * 1024:
                        raise ConnectionError("REST response exceeds 8 MiB")
                    parts.append(chunk)
                payload = b"".join(parts)
                wall, mono = time.time_ns(), time.monotonic_ns()
            raw = service.capture(kind, payload, session_id, wall_ns=wall, mono_ns=mono)
            await service.process(raw)

        while not stop.is_set() and time.monotonic() < deadline:
            session_id = str(uuid.uuid4())
            queue = asyncio.Queue(maxsize=2048)
            receiver = None
            connected = False
            delay = 0.0
            try:
                if not metadata_loaded:
                    await interruptible(
                        get("/api/v3/exchangeInfo?symbol=BTCUSDT", "metadata", "metadata"),
                        stop,
                        deadline - time.monotonic(),
                    )
                    metadata_loaded = True
                url = ws_base + "/stream?streams=btcusdt@depth@100ms/btcusdt@trade"
                ws = await interruptible(
                    http.ws_connect(url, autoping=True, heartbeat=None, max_msg_size=2**20),
                    stop,
                    min(20, deadline - time.monotonic()),
                )
                try:
                    connected = True
                    await service.control("start", session_id)
                    stats.last_ws_mono = stats.last_book_mono = stats.last_trade_mono = None
                    session_started = time.monotonic()

                    async def receive(ws=ws, queue=queue, session_id=session_id):
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
                                    stats.received += 1
                                    stats.last_ws_mono = time.monotonic()
                                    await queue.put(raw)
                                elif message.type == aiohttp.WSMsgType.ERROR:
                                    raise ConnectionError(str(ws.exception()))
                        finally:
                            with suppress(asyncio.QueueFull):
                                queue.put_nowait(None)

                    receiver = asyncio.create_task(receive(), name="binance-public-receive")
                    await interruptible(
                        get("/api/v3/depth?symbol=BTCUSDT&limit=5000", session_id, "snapshot"),
                        stop,
                        deadline - time.monotonic(),
                    )
                    last_book = time.monotonic()
                    while not stop.is_set() and time.monotonic() < deadline:
                        raw = await interruptible(
                            queue.get(), stop, min(stale_seconds, deadline - time.monotonic())
                        )
                        if raw is None:
                            if receiver.done() and not receiver.cancelled():
                                receiver.result()
                            raise ConnectionError("websocket_closed")
                        before = service.projection.last_update_id
                        trade_before = service.projection.last_trade_id
                        resync = await service.process(raw)
                        now = time.monotonic()
                        if service.projection.last_update_id != before:
                            stats.last_book_mono = last_book = raw.receive_mono_ns / 1e9
                        if service.projection.last_trade_id != trade_before:
                            stats.last_trade_mono = raw.receive_mono_ns / 1e9
                        if resync:
                            raise ConnectionError("normalizer_requested_resync")
                        if now - last_book > stale_seconds:
                            raise ConnectionError("book_stale")
                        if now - session_started >= stale_seconds:
                            failures = 0
                        if (
                            reconnect_after
                            and not stats.forced
                            and now - started >= reconnect_after
                        ):
                            stats.forced = True
                            raise ConnectionError("acceptance_forced_reconnect")
                finally:
                    if receiver is not None:
                        receiver.cancel()
                        await asyncio.gather(receiver, return_exceptions=True)
                    # Bound the entire handshake, not each read: a peer may keep
                    # sending frames forever without acknowledging CLOSE.
                    with suppress(TimeoutError):
                        async with asyncio.timeout(1):
                            await ws.close()
            except Stopped:
                break
            except (TimeoutError, aiohttp.ClientError, ConnectionError) as exc:
                if time.monotonic() < deadline and not stop.is_set():
                    stats.errors.append(f"{type(exc).__name__}: {exc}")
                    stats.reconnects += 1
                    delay = (
                        exc.delay
                        if isinstance(exc, RateLimited)
                        else random.uniform(
                            retry_initial / 2,
                            min(retry_max, retry_initial * 2 ** min(failures, 16)),
                        )
                    )
                    failures += 1
            finally:
                if receiver is not None:
                    receiver.cancel()
                    # A failed receiver is observed by its consumer; never mask a storage failure.
                    result = (await asyncio.gather(receiver, return_exceptions=True))[0]
                    if service.poisoned:
                        if isinstance(result, BaseException) and not isinstance(
                            result, asyncio.CancelledError
                        ):
                            raise result
                        raise RuntimeError("capture writer poisoned; restart required")
                if not service.poisoned:
                    # Includes a frame persisted just before cancellation at queue.put().
                    await service.recover_pending()
                    if connected:
                        await service.control(
                            "stop", session_id, "connection_closed_or_capture_complete"
                        )
            if delay and not stop.is_set():
                stats.retry_at_mono = time.monotonic() + delay
                try:
                    await interruptible(asyncio.sleep(delay), stop, deadline - time.monotonic())
                except (Stopped, TimeoutError):
                    break
                finally:
                    stats.retry_at_mono = None
    return {**stats.report(), "elapsed_seconds": round(time.monotonic() - started, 3)}
