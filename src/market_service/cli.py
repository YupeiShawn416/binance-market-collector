"""Market-data commands. Optional L3 credentials; no trading operations."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

import aiohttp
import msgspec

from .contracts import Event
from .ipc import Publisher, consume
from .network import collect
from .repair import RepairWorker
from .repair_store import RepairStore
from .service import MarketService
from .source import Consumer, replay
from .store import Journal
from .trade_history import HistoricalClient
from .verification import verify


def output(value, path=None):
    text = json.dumps(value, ensure_ascii=False, indent=2)
    if path:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(text + "\n")
    print(text)


async def run(args):
    if args.command in ("run", "health"):
        from .config import load_config
        from .runtime import DiskReserveError, read_health, run_with_signals

        config = load_config(args.config)
        if args.command == "health":
            try:
                report = read_health(config)
            except (FileNotFoundError, ValueError, KeyError) as exc:
                output({"ready": False, "state": "UNAVAILABLE", "reason": str(exc)})
                raise SystemExit(1) from None
            output(report)
            if not report["ready"]:
                raise SystemExit(1)
        else:
            try:
                output(await run_with_signals(config, seconds=args.seconds))
            except DiskReserveError as exc:
                print(f"DISK_LOW: {exc}", file=sys.stderr)
                raise SystemExit(78) from None
    elif args.command == "backup":
        from .backup import backup_journal

        output(backup_journal(args.db, args.output))
    elif args.command == "l3-collect":
        from .contracts import Instrument
        from .l3.network import collect as collect_l3
        from .l3.network import credentials
        from .l3.service import L3Journal, L3Service
        from .l3.service import verify as verify_l3

        key, secret = credentials(args.credentials_file)
        with L3Journal(args.db) as journal:
            origin = journal.db.execute(
                "SELECT value FROM metadata WHERE key='data_origin'"
            ).fetchone()
            if origin and origin[0] != "live":
                raise ValueError("refusing to mix synthetic and live L3 datasets")
            with journal.db:
                journal.db.execute("INSERT OR IGNORE INTO metadata VALUES('data_origin','live')")
            service = L3Service(journal, depth=args.depth)
            service.install_spec(
                Instrument(
                    "BTC-USD",
                    "BTC",
                    "USD",
                    args.price_quantum,
                    "0.00000001",
                    "unknown",
                    "research-units-only",
                )
            )
            publisher = await l3_publisher(args.db, args.socket).start() if args.socket else None
            try:
                network = await collect_l3(
                    service,
                    key=key,
                    secret=secret,
                    seconds=args.seconds,
                    reconnect_after=args.reconnect_after,
                )
                result = verify_l3(args.db)
                result["network"] = network
                output(result, args.report)
                if not network["real_l3_received"] or not result["counts"].get("OrderSnapshot"):
                    raise SystemExit("INCOMPLETE: no verified real L3 snapshot received")
            finally:
                if publisher:
                    await publisher.close()
    elif args.command == "l3-import-coinbase":
        from .l3.coinbase_history import import_capture

        output(import_capture(args.source, args.db, initial_levels=args.levels), args.report)
    elif args.command == "l3-compare":
        from .l3.experiment import compare

        output(
            compare(
                args.db,
                args.csv,
                sizes=args.sizes,
                grid_ms=args.grid_ms,
                allow_synthetic=args.allow_synthetic,
            ),
            args.report,
        )
    elif args.command == "l3-verify":
        from .l3.service import verify as verify_l3

        output(verify_l3(args.db), args.report)
    elif args.command == "repair":
        with RepairStore(args.catalog, args.db) as catalog:
            async with aiohttp.ClientSession(
                trust_env=True, timeout=aiohttp.ClientTimeout(total=15)
            ) as http:
                worker = RepairWorker(
                    catalog, HistoricalClient(http, args.base_url), page_size=args.page_size
                )
                if args.follow:
                    await worker.follow(asyncio.Event())
                else:
                    result = await worker.run_once(
                        max_pages=args.max_pages, retry_now=args.retry_now
                    )
                    output(result, args.report)
                    if not result["all_discovered_gaps_repaired"]:
                        raise SystemExit(
                            "INCOMPLETE: unresolved raw-trade gaps remain in the catalog"
                        )
    elif args.command == "coverage":
        with RepairStore(args.catalog, args.db, readonly=True) as catalog:
            output(
                catalog.continuity(
                    args.first_id, args.last_id, view=args.view, known_by_ns=args.known_by_ns
                ),
                args.report,
            )
    elif args.command == "collect":
        with Journal(args.db) as journal:
            service = MarketService(journal)
            publisher = await Publisher(args.db, args.socket).start() if args.socket else None
            try:
                stats = await collect(
                    service, seconds=args.seconds, reconnect_after=args.reconnect_after
                )
                result = verify(args.db, expected_chain=service.final_chain)
                result["network"] = stats
                result["capture_has_required_event_types"] = (
                    all(result["counts"].get(k, 0) for k in ("Snapshot", "Delta", "Trade"))
                    and result["pending_raw"] == 0
                )
                result["exchange_history_continuity_verified"] = False
                output(result, args.report)
                if not result["capture_has_required_event_types"]:
                    raise SystemExit("capture did not produce the required event types")
            finally:
                if publisher:
                    await publisher.close()
    elif args.command == "serve":
        publisher = await (
            l3_publisher(args.db, args.socket)
            if args.schema_version == 2
            else Publisher(args.db, args.socket)
        ).start()
        try:
            await publisher.server.serve_forever()
        finally:
            await publisher.close()
    elif args.command in ("consume", "replay"):
        if args.schema_version == 2:
            from .l3.service import L3Consumer

            consumer = L3Consumer(args.state)
        else:
            consumer = Consumer(args.state)
        try:
            if args.command == "consume":
                await consume(
                    args.socket, consumer, follow=not args.no_follow, stop_after=args.limit
                )
            else:
                for _ in replay(args.db, consumer):
                    pass
            output(
                {
                    "seq": consumer.projection.seq,
                    "book_hash": consumer.projection.book_hash,
                    "chain_hash": consumer.chain_hash,
                    "quality": consumer.projection.quality,
                    "data_origin": getattr(consumer.projection, "data_origin", None),
                },
                args.report,
            )
        finally:
            consumer.close()
    elif args.command == "verify":
        reference = json.loads(Path(args.reference).read_text()) if args.reference else None
        result = verify(
            args.db, expected_chain=reference["final_event_chain"] if reference else None
        )
        if reference and result["final_book_hash"] != reference["final_book_hash"]:
            raise ValueError("reference book hash differs")
        output(result, args.report)
    elif args.command == "schema":
        output(msgspec.json.schema(Event), args.report)


def l3_publisher(db, socket):
    from .l3.contracts import BatchV2, SubscribeV2
    from .l3.service import L3Journal

    return Publisher(
        db,
        socket,
        journal_type=L3Journal,
        subscribe_type=SubscribeV2,
        batch_type=BatchV2,
        schema_version=2,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    daemon = sub.add_parser("run", help="run the supervised Binance spot collector")
    daemon.add_argument("--config", required=True)
    daemon.add_argument("--seconds", type=float, help="optional bounded smoke run")
    health = sub.add_parser("health", help="read heartbeat; exit 0 only when fresh and ready")
    health.add_argument("--config", required=True)
    backup = sub.add_parser(
        "backup", help="create a consistent SQLite snapshot without replacing files"
    )
    backup.add_argument("--db", required=True)
    backup.add_argument("--output", required=True)
    capture = sub.add_parser("collect", help="capture public BTCUSDT L2 + raw trades")
    capture.add_argument("--db", required=True)
    capture.add_argument("--seconds", type=float, default=60)
    capture.add_argument("--reconnect-after", type=float)
    capture.add_argument("--socket")
    capture.add_argument("--report")
    serve = sub.add_parser("serve", help="publish durable history + live journal tail")
    serve.add_argument("--db", required=True)
    serve.add_argument("--socket", required=True)
    serve.add_argument("--schema-version", type=int, choices=(1, 2), default=1)
    socket_consumer = sub.add_parser(
        "consume", help="independent decision/trading reference consumer"
    )
    socket_consumer.add_argument("--socket", required=True)
    socket_consumer.add_argument("--state", required=True)
    socket_consumer.add_argument("--no-follow", action="store_true")
    socket_consumer.add_argument("--limit", type=int)
    socket_consumer.add_argument("--report")
    socket_consumer.add_argument("--schema-version", type=int, choices=(1, 2), default=1)
    history = sub.add_parser("replay", help="replay with the exact same consumer reducer")
    history.add_argument("--db", required=True)
    history.add_argument("--state")
    history.add_argument("--report")
    history.add_argument("--schema-version", type=int, choices=(1, 2), default=1)
    check = sub.add_parser("verify")
    check.add_argument("--db", required=True)
    check.add_argument("--report")
    check.add_argument("--reference", help="compare an external capture verification report")
    schema = sub.add_parser("schema")
    schema.add_argument("--report")
    repair = sub.add_parser(
        "repair", help="repair raw-trade gaps into an independent catalog; never publish live"
    )
    repair.add_argument("--db", required=True)
    repair.add_argument("--catalog", required=True)
    repair.add_argument("--base-url", default="https://api.binance.com")
    repair.add_argument("--page-size", type=int, default=1000)
    repair.add_argument("--max-pages", type=int)
    repair.add_argument("--retry-now", action="store_true")
    repair.add_argument(
        "--follow",
        action="store_true",
        help="continuously discover gaps and run durable repair jobs alongside collect",
    )
    repair.add_argument("--report")
    coverage = sub.add_parser(
        "coverage", help="bounded trade-ID continuity in observed/repaired view"
    )
    coverage.add_argument("--db", required=True)
    coverage.add_argument("--catalog", required=True)
    coverage.add_argument("--first-id", type=int, required=True)
    coverage.add_argument("--last-id", type=int, required=True)
    coverage.add_argument("--view", choices=("observed", "repaired"), required=True)
    coverage.add_argument("--known-by-ns", type=int)
    coverage.add_argument("--report")
    l3_capture = sub.add_parser(
        "l3-collect", help="Kraken authenticated visible L3; no order operations"
    )
    l3_capture.add_argument("--db", required=True)
    l3_capture.add_argument("--credentials-file")
    l3_capture.add_argument("--seconds", type=float, default=60)
    l3_capture.add_argument("--depth", type=int, choices=(10, 100, 1000), default=100)
    l3_capture.add_argument(
        "--price-quantum", default="0.00000001", help="representation unit, not an order tick rule"
    )
    l3_capture.add_argument("--reconnect-after", type=float)
    l3_capture.add_argument("--socket")
    l3_capture.add_argument("--report")
    l3_check = sub.add_parser("l3-verify")
    l3_check.add_argument("--db", required=True)
    l3_check.add_argument("--report")
    experiment = sub.add_parser(
        "l3-compare", help="paired L3/full-frequency L2/100ms L2 taker probes"
    )
    experiment.add_argument("--db", required=True)
    experiment.add_argument("--csv", required=True)
    experiment.add_argument("--report")
    experiment.add_argument("--sizes", nargs="+", default=["0.0001", "0.001", "0.01"])
    experiment.add_argument("--grid-ms", type=int, default=10)
    experiment.add_argument("--allow-synthetic", action="store_true")
    imported = sub.add_parser(
        "l3-import-coinbase",
        help="import public historical full stream with snapshot sequence bridge",
    )
    imported.add_argument("--source", required=True)
    imported.add_argument("--db", required=True)
    imported.add_argument("--levels", type=int, default=100)
    imported.add_argument("--report")
    args = parser.parse_args()
    if args.command in ("collect", "run") and args.seconds is not None and args.seconds <= 0:
        parser.error("--seconds must be positive")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
