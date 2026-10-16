"""Real public capture + two socket consumers + resume + full historical replay.

Run from the project root: uv run python scripts/acceptance.py --seconds 30.
This uses no private API. A forced reconnect is deliberate and missing public
trade IDs are reported, never filled with invented trades.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
from collections import Counter
from pathlib import Path

from market_service.ipc import Publisher, consume
from market_service.network import collect
from market_service.service import MarketService
from market_service.source import Consumer
from market_service.store import Journal
from market_service.verification import verify


async def acceptance(db_path: Path, seconds: float, report_path: Path):
    if db_path.exists():
        raise FileExistsError("use a fresh acceptance database")
    with (
        Journal(db_path) as journal,
        tempfile.TemporaryDirectory(prefix="mdsa-", dir="/tmp") as temp,
    ):
        service = MarketService(journal, record_trace=True)
        socket = Path(temp) / "feed.sock"
        publisher = await Publisher(db_path, socket).start()
        decision_path = db_path.with_name("decision-state.sqlite")
        trading_path = db_path.with_name("trading-state.sqlite")
        if decision_path.exists() or trading_path.exists():
            raise FileExistsError("acceptance consumer states must also be fresh")
        trading = Consumer(trading_path, record_trace=True)
        decision_prefix = []
        decision_holder = []
        tasks = []
        try:

            async def decision_run():
                first = Consumer(decision_path, record_trace=True)
                try:
                    await consume(socket, first, stop_after=25)
                    decision_prefix.extend(first.trace)
                finally:
                    first.close()
                # Restart using only the consumer-owned durable checkpoint + cursor.
                second = Consumer(decision_path, record_trace=True)
                decision_holder.append(second)
                await consume(socket, second)

            tasks = [
                asyncio.create_task(decision_run()),
                asyncio.create_task(consume(socket, trading)),
            ]
            stats = await collect(service, seconds=seconds, reconnect_after=seconds / 2)
            frontier = journal.frontier
            async with asyncio.timeout(15):
                while (
                    not decision_holder
                    or decision_holder[0].projection.seq != frontier
                    or trading.projection.seq != frontier
                ):
                    for task in tasks:
                        if task.done():
                            task.result()
                    await asyncio.sleep(0.02)
            decision = decision_holder[0]
            assert decision_prefix + decision.trace == trading.trace == service.trace
            result = verify(db_path, service.trace, expected_chain=service.final_chain)
            assert result["counts"].get("Snapshot", 0) >= 2
            assert result["counts"].get("Delta", 0) > 0
            assert result["counts"].get("Trade", 0) > 0
            assert stats["forced_reconnect"] and stats["reconnects"] >= 1
            result.update(
                {
                    "network": stats,
                    "two_consumer_transcripts_equal": True,
                    "consumer_resume_after_seq": 25,
                    "consumer_resume_equal": True,
                    "live_event_book_hashes_equal_history": True,
                }
            )
            # Observed sample timings, NOT a throughput benchmark or a service SLO.
            latencies = []
            kinds = Counter()
            for record in journal.records():
                event = record.event
                kinds[type(event.payload).__name__] += 1
                if event.boot_id == event.normalizer_instance_id:
                    latencies.append((event.normalized_mono_ns - event.receive_mono_ns) / 1e6)
            latencies.sort()
            result["observed_receive_to_normalize_ms"] = {
                "p50": round(latencies[len(latencies) // 2], 3),
                "p99": round(latencies[min(len(latencies) - 1, int(len(latencies) * 0.99))], 3),
                "max": round(max(latencies), 3),
                "scope": "one live sample; excludes commit/socket/network/model latency",
            }
            report_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
            print(json.dumps(result, ensure_ascii=False, indent=2))
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await publisher.close()
            trading.close()
            for decision in decision_holder:
                decision.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("evidence/live.sqlite"))
    parser.add_argument("--seconds", type=float, default=30)
    parser.add_argument("--report", type=Path, default=Path("evidence/live-verification.json"))
    args = parser.parse_args()
    args.db.parent.mkdir(parents=True, exist_ok=True)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    asyncio.run(acceptance(args.db, args.seconds, args.report))


if __name__ == "__main__":
    main()
