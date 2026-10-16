import asyncio

import msgspec
import pytest

from market_service.service import MarketService
from market_service.store import Journal

METADATA = {
    "symbols": [
        {
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "isSpotTradingAllowed": True,
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "baseAssetPrecision": 8,
            "filters": [
                {"filterType": "LOT_SIZE", "stepSize": "0.00001000"},
                {"filterType": "PRICE_FILTER", "tickSize": "0.01000000"},
            ],
        }
    ]
}
SNAPSHOT = {
    "lastUpdateId": 100,
    "bids": [["99.00", "1.00000000"]],
    "asks": [["100.00", "1.00000000"], ["150.00", "2.00000000"]],
}


def delta(first=101, last=None, bids=None, asks=None):
    return {
        "e": "depthUpdate",
        "E": 1792000000000,
        "s": "BTCUSDT",
        "U": first,
        "u": last or first,
        "b": bids or [],
        "a": asks or [],
    }


def trade(number=1):
    return {
        "e": "trade",
        "E": 1792000000001,
        "s": "BTCUSDT",
        "t": number,
        "p": "100.00",
        "q": "0.00000001",
        "T": 1792000000000,
        "m": False,
    }


def feed(service, kind, payload, session="session-1"):
    raw = service.capture(
        kind,
        msgspec.json.encode(payload),
        session,
        wall_ns=1792000000000000123,
        mono_ns=1000000000000123,
    )
    return asyncio.run(service.process(raw))


@pytest.fixture
def setup(tmp_path):
    path = tmp_path / "market.sqlite"
    journal = Journal(path)
    service = MarketService(journal, checkpoint_every=7, record_trace=True)
    feed(service, "metadata", METADATA)
    asyncio.run(service.control("start", "session-1"))
    feed(service, "snapshot", SNAPSHOT)
    feed(service, "ws", delta())
    feed(service, "ws", trade())
    yield path, journal, service
    journal.close()


@pytest.fixture
def socket_path():
    from pathlib import Path
    from tempfile import TemporaryDirectory

    with TemporaryDirectory(prefix="mds-", dir="/tmp") as folder:
        yield Path(folder) / "feed.sock"
