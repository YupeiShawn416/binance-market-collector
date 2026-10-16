"""Public historical-trade transport and exact normalization, independent of storage."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import aiohttp
import msgspec

from .book import IntegrityError, units
from .contracts import Instrument, Trade, encode


@dataclass(frozen=True)
class Reply:
    url: str
    status: int
    body: bytes
    fetched_ns: int
    resolution_ns: int = 1_000_000
    retry_after_ns: int = 0
    source_kind: str = "historical-rest"


def parse_trades(body: bytes, spec: Instrument, resolution_ns: int) -> list[Trade]:
    """Accept raw trades only; timestamp resolution is explicit, never guessed."""
    if resolution_ns not in (1_000_000, 1_000):
        raise ValueError("historical timestamp unit must be ms or us")
    rows = msgspec.json.decode(body)
    if not isinstance(rows, list):
        raise IntegrityError("expected a raw historicalTrades list, not aggTrades or an error")
    result = []
    previous = None
    for row in rows:
        if (
            not isinstance(row, dict)
            or not {"id", "price", "qty", "time", "isBuyerMaker"} <= row.keys()
        ):
            raise IntegrityError("missing raw-trade fields")
        if (
            type(row["id"]) is not int
            or type(row["time"]) is not int
            or type(row["isBuyerMaker"]) is not bool
        ):
            raise IntegrityError("invalid historical trade field types")
        trade = Trade(
            row["id"],
            units(row["price"], spec.price_tick),
            units(row["qty"], spec.quantity_unit),
            "sell" if row["isBuyerMaker"] else "buy",
            row["time"] * resolution_ns,
        )
        msgspec.msgpack.decode(encode(trade), type=Trade)  # checked i64 fields
        if trade.price_ticks <= 0 or trade.quantity_units <= 0:
            raise IntegrityError("nonpositive historical trade")
        if previous is not None and trade.trade_id <= previous:
            raise IntegrityError("unordered or duplicate IDs inside historical page")
        result.append(trade)
        previous = trade.trade_id
    return result


def compatible(a: Trade, a_resolution: int, b: Trade, b_resolution: int) -> bool:
    quantum = max(a_resolution, b_resolution)
    return (
        a.trade_id,
        a.price_ticks,
        a.quantity_units,
        a.aggressor,
        a.trade_time_ns // quantum,
    ) == (b.trade_id, b.price_ticks, b.quantity_units, b.aggressor, b.trade_time_ns // quantum)


class HistoricalClient:
    """One public GET only. No credentials, account methods, or order methods.

    429/418 are surfaced with Retry-After, never bypassed via another host.
    Transient failures are retried by the durable job scheduler after backoff.
    """

    def __init__(
        self, session: aiohttp.ClientSession, base_url="https://api.binance.com", *, interval=0.3
    ):
        self.session, self.base_url, self.interval = session, base_url.rstrip("/"), interval
        self.next_request = 0.0
        self.blocked_until_ns = 0

    async def fetch(self, first_id: int, limit: int) -> Reply:
        if not 1 <= limit <= 1000 or first_id < 0:
            raise ValueError("invalid historical page request")
        delay = max(
            self.next_request - time.monotonic(), (self.blocked_until_ns - time.time_ns()) / 1e9, 0
        )
        if delay > 0:
            # Long Retry-After is handled by the durable scheduler, not a blocked service loop.
            if delay > 5:
                raise ConnectionError("historical endpoint is in Retry-After backoff")
            await asyncio.sleep(delay)
        self.next_request = time.monotonic() + self.interval
        params = {"symbol": "BTCUSDT", "fromId": first_id, "limit": limit}
        async with self.session.get(
            self.base_url + "/api/v3/historicalTrades",
            params=params,
        ) as response:
            body = await response.read()
            now = time.time_ns()
            if response.status in (418, 429):
                try:
                    seconds = max(1.0, float(response.headers.get("Retry-After", "60")))
                except ValueError:
                    seconds = 60.0
                self.blocked_until_ns = now + int(seconds * 1e9)
            return Reply(
                str(response.url), response.status, body, now, retry_after_ns=self.blocked_until_ns
            )
