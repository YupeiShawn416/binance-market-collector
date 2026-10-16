"""Market-data-only Kraken L3 transport; credentials and tokens never enter journals."""

import asyncio
import json
import os
import stat
import time
import uuid
from contextlib import suppress
from pathlib import Path

import aiohttp

from ..book import IntegrityError

WS_URL = "wss://ws-l3.kraken.com/v2"


def credentials(path=None):
    if path:
        source = Path(path)
        info = source.stat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError("credential file must be an owned regular file with mode 0600")
        data = json.loads(source.read_bytes())
        key, secret = data.get("key"), data.get("secret")
    else:
        key, secret = os.environ.get("KRAKEN_MARKET_KEY"), os.environ.get("KRAKEN_MARKET_SECRET")
    if not isinstance(key, str) or not isinstance(secret, str) or not key or not secret:
        raise ValueError(
            "Kraken L3 needs local credentials; use a 0600 key/secret JSON file or KRAKEN_MARKET_KEY/KRAKEN_MARKET_SECRET"
        )
    return key, secret


async def token_provider(key, secret):
    # Reuse upstream nonce, HMAC signing, error handling and session cleanup.
    # Upstream's websocket JSON callback discards raw bytes/Decimal precision;
    # therefore only the REST auth client is reused here.
    from kraken.spot import SpotAsyncClient

    try:
        async with SpotAsyncClient(key=key, secret=secret) as client:
            result = await client.request(
                method="POST", uri="/0/private/GetWebSocketsToken", timeout=10
            )
    except Exception as exc:
        raise ConnectionError("Kraken token request failed: " + type(exc).__name__) from None
    return result["token"]


async def collect(
    service, *, key, secret, seconds=60, get_token=token_provider, reconnect_after=None
):
    if seconds <= 0:
        raise ValueError("L3 capture duration must be positive")
    service.recover_pending()
    service.control("restart", "DISCONNECTED", "fresh_connection_required_after_restart")
    started, received, reconnects, errors, forced = time.monotonic(), 0, 0, [], False
    deadline = started + seconds
    async with aiohttp.ClientSession(
        trust_env=True, timeout=aiohttp.ClientTimeout(total=12)
    ) as http:
        while time.monotonic() < deadline:
            session_id = str(uuid.uuid4())
            receiver = None
            queue = asyncio.Queue(maxsize=256)
            try:
                token = await asyncio.wait_for(
                    get_token(key, secret), min(12, deadline - time.monotonic())
                )
                async with http.ws_connect(
                    WS_URL, max_msg_size=16 * 1024 * 1024, heartbeat=20
                ) as ws:
                    service.control(session_id, "LIVE", "kraken_l3_connected_requires_snapshot")
                    await ws.send_json(
                        {
                            "method": "subscribe",
                            "params": {
                                "channel": "level3",
                                "symbol": ["BTC/USD"],
                                "depth": service.depth,
                                "snapshot": True,
                                "token": token,
                            },
                            "req_id": 1,
                        }
                    )
                    del token

                    async def receive(ws=ws, queue=queue, session_id=session_id):
                        nonlocal received
                        try:
                            async for message in ws:
                                if message.type != aiohttp.WSMsgType.TEXT:
                                    raise ConnectionError("Kraken L3 connection closed")
                                wall, mono = time.time_ns(), time.monotonic_ns()
                                # Route control frames without persisting authentication material.
                                header = json.loads(message.data)
                                if header.get("success") is False:
                                    raise ConnectionError("Kraken L3 subscription rejected")
                                if header.get("channel") != "level3":
                                    continue
                                frame = service.capture(
                                    "kraken-l3-ws",
                                    message.data.encode(),
                                    session_id,
                                    wall_ns=wall,
                                    mono_ns=mono,
                                )
                                received += 1
                                await queue.put(frame)
                        finally:
                            with suppress(asyncio.QueueFull):
                                queue.put_nowait(None)

                    receiver = asyncio.create_task(receive(), name="kraken-l3-raw-receive")
                    last_update = time.monotonic()
                    while time.monotonic() < deadline:
                        frame = await asyncio.wait_for(
                            queue.get(), min(10, deadline - time.monotonic())
                        )
                        if frame is None:
                            if receiver.done() and not receiver.cancelled():
                                receiver.result()
                            raise ConnectionError("Kraken L3 stream ended")
                        if service.process(frame):
                            raise IntegrityError("Kraken L3 resynchronization required")
                        last_update = time.monotonic()
                        if (
                            reconnect_after
                            and not forced
                            and last_update - started >= reconnect_after
                        ):
                            forced = True
                            raise ConnectionError("acceptance_forced_reconnect")
            except (aiohttp.ClientError, TimeoutError, ConnectionError, ValueError) as exc:
                # Do not log exception text from authentication or upstream clients.
                errors.append(type(exc).__name__)
                reconnects += 1
            finally:
                if receiver is not None:
                    receiver.cancel()
                    with suppress(
                        asyncio.CancelledError, ConnectionError, ValueError, aiohttp.ClientError
                    ):
                        await receiver
                while not queue.empty():
                    frame = queue.get_nowait()
                    if frame is not None:
                        service.process(frame)
                if not service.poisoned:
                    service.control(
                        session_id, "DISCONNECTED", "capture_complete_or_connection_lost"
                    )
            if time.monotonic() < deadline:
                await asyncio.sleep(min(1, deadline - time.monotonic()))
    return {
        "received_l3_frames": received,
        "reconnects": reconnects,
        "errors": errors,
        "forced_reconnect": forced,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "real_l3_received": received > 0,
    }
