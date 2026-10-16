"""Real public-data acceptance. Never insert generated rows into the real repair catalog.

Attempts the exact missing raw IDs, then independently downloads same-period
history. A reachable aggTrades endpoint is comparison evidence, not raw repair.
"""

import argparse
import asyncio
import csv
import hashlib
import json
import time
from pathlib import Path

import aiohttp
import msgspec

from market_service.contracts import Trade
from market_service.history_compare import compare_aggregates
from market_service.repair import RepairWorker
from market_service.repair_store import RepairStore
from market_service.store import Journal
from market_service.trade_history import HistoricalClient, parse_trades
from market_service.verification import verify


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


async def download_aggregates(session, base_url, folder, prefix, start_ms, end_ms):
    params = {"symbol": "BTCUSDT", "startTime": start_ms, "endTime": end_ms, "limit": 1000}
    aggregates, number, previous = [], 0, None
    while True:
        number += 1
        async with session.get(
            base_url.rstrip("/") + "/api/v3/aggTrades", params=params
        ) as response:
            body = await response.read()
            path = folder / f"{prefix}-{number}.json"
            path.write_bytes(body)
            save(
                path.with_suffix(".meta.json"),
                {
                    "url": str(response.url),
                    "status": response.status,
                    "fetched_ns": time.time_ns(),
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "resolution_ns": 1_000_000,
                    "requested_start_ms": start_ms,
                    "requested_end_ms": end_ms,
                },
            )
            response.raise_for_status()
        rows = msgspec.json.decode(body)
        if not isinstance(rows, list):
            raise ValueError("not an aggTrades list")
        for row in rows:
            if previous is not None and row["a"] != previous + 1:
                raise ValueError("historical aggregate pagination is not contiguous")
            previous = row["a"]
            if start_ms <= row["T"] <= end_ms:
                aggregates.append(row)
        # Equality is insufficient: another full page may share the exact end millisecond.
        if len(rows) < 1000 or rows[-1]["T"] > end_ms:
            break
        if len(aggregates) > 100000:
            raise ValueError("comparison scope exceeded")
        params = {"symbol": "BTCUSDT", "fromId": rows[-1]["a"] + 1, "limit": 1000}
        await asyncio.sleep(0.3)
    if not aggregates:
        raise ValueError("no historical aggregates in requested window")
    with (folder / f"{prefix}.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(aggregates[0]))
        writer.writeheader()
        writer.writerows(aggregates)
    return aggregates


async def main(args):
    folder = Path(args.output)
    folder.mkdir(parents=True, exist_ok=True)
    journal_path = Path(args.db)
    original_hash = hashlib.sha256(journal_path.read_bytes()).hexdigest()
    with Journal(journal_path, readonly=True) as journal:
        original_records = list(journal.records())
    trades = [r.event.payload for r in original_records if isinstance(r.event.payload, Trade)]
    first_id, last_id = trades[0].trade_id, trades[-1].trade_id
    report = {
        "original_sha256_before": original_hash,
        "synthetic_data_used": False,
        "raw_history_base_url": args.base_url,
        "aggregate_history_base_url": args.history_base,
    }
    async with aiohttp.ClientSession(
        trust_env=True, timeout=aiohttp.ClientTimeout(total=12)
    ) as session:
        client = HistoricalClient(session, args.base_url)
        with RepairStore(folder / "repairs.sqlite", journal_path) as catalog:
            catalog.sync_observed()
            report["before"] = catalog.continuity(first_id, last_id, view="observed")
            report["repair_attempt"] = await RepairWorker(catalog, client, page_size=32).run_once(
                retry_now=True
            )
            report["after_repaired"] = catalog.continuity(first_id, last_id)
            report["after_observed"] = catalog.continuity(first_id, last_id, view="observed")
            report["real_112_raw_backfill_verified"] = report["after_repaired"]["complete"]
            save(folder / "gap-catalog.json", catalog.gaps())
            report["book_connection_boundaries_preserved"] = catalog.db.execute(
                "SELECT COUNT(*) FROM boundaries"
            ).fetchone()[0]
            spec = catalog.spec
            report["catalog_integrity"] = catalog.verify_integrity()
        # Raw historical download is independent of the sidecar. No imported repair facts here.
        raw_rows = []
        cursor = first_id
        try:
            while cursor <= last_id:
                reply = await client.fetch(cursor, min(1000, last_id - cursor + 1))
                page = folder / f"historical-raw-{cursor}.json"
                page.write_bytes(reply.body)
                save(
                    page.with_suffix(".meta.json"),
                    {
                        "url": reply.url,
                        "status": reply.status,
                        "fetched_ns": reply.fetched_ns,
                        "sha256": hashlib.sha256(reply.body).hexdigest(),
                        "resolution_ns": reply.resolution_ns,
                    },
                )
                if reply.status != 200:
                    raise ValueError(f"HTTP {reply.status}")
                facts = parse_trades(reply.body, spec, reply.resolution_ns)
                if not facts or any(t.trade_id != cursor + i for i, t in enumerate(facts)):
                    raise ValueError("historical raw page is not contiguous")
                raw_rows.extend(msgspec.json.decode(reply.body))
                cursor = facts[-1].trade_id + 1
            report["historical_raw_download"] = {"complete": True, "rows": len(raw_rows)}
        except (aiohttp.ClientError, TimeoutError, ValueError, ConnectionError) as exc:
            report["historical_raw_download"] = {
                "complete": False,
                "error": f"{type(exc).__name__}:{exc}",
                "rows": len(raw_rows),
                "next_id": cursor,
            }
        # Obtain historical aggregates from the documented public-data-only endpoint.
        start_ms = min(t.trade_time_ns for t in trades) // 1_000_000
        end_ms = max(t.trade_time_ns for t in trades) // 1_000_000
        aggregates = await download_aggregates(
            session, args.history_base, folder, "historical-agg", start_ms, end_ms
        )
        report["historical_comparison"] = compare_aggregates(trades, aggregates, spec)
        save(folder / "historical-comparison.json", report["historical_comparison"])
        wall_start = min(r.event.receive_wall_ns for r in original_records) // 1_000_000
        wall_end = max(r.event.normalized_wall_ns for r in original_records) // 1_000_000
        wall_rows = await download_aggregates(
            session, args.history_base, folder, "historical-wall-window", wall_start, wall_end
        )
        report["wall_window_history"] = {
            "requested_start_ms": wall_start,
            "requested_end_ms": wall_end,
            "aggregate_rows": len(wall_rows),
            "first_raw_id": wall_rows[0]["f"],
            "last_raw_id": wall_rows[-1]["l"],
            "raw_id_count": sum(r["l"] - r["f"] + 1 for r in wall_rows),
            "outside_observed_id_bounds": sum(
                max(0, min(r["l"], first_id - 1) - r["f"] + 1)
                + max(0, r["l"] - max(r["f"], last_id + 1) + 1)
                for r in wall_rows
            ),
            "caveat": "Same numeric wall-clock bounds queried against exchange T; includes connection startup/tail and clock/arrival effects. Not all extra IDs are an interior data gap.",
        }
    report["original_sha256_after"] = hashlib.sha256(journal_path.read_bytes()).hexdigest()
    assert report["original_sha256_after"] == original_hash
    report["original_journal_unchanged"] = True
    report["original_verify"] = verify(journal_path, expected_chain=original_records[-1].chain_hash)
    save(folder / "acceptance.json", report)
    print(
        json.dumps(
            {k: v for k, v in report.items() if k != "historical_comparison"},
            ensure_ascii=False,
            indent=2,
        )
    )
    if not report["real_112_raw_backfill_verified"]:
        raise SystemExit(
            "INCOMPLETE: raw history is unavailable; real 112-ID repair remains unverified. See acceptance.json."
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="evidence/live.sqlite")
    parser.add_argument("--output", default="evidence/repair")
    parser.add_argument("--base-url", default="https://api.binance.com")
    parser.add_argument("--history-base", default="https://data-api.binance.vision")
    asyncio.run(main(parser.parse_args()))
