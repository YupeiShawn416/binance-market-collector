"""Real archived trades acceptance: compare first, then repair the existing sidecar.

The original journal is read-only. A child is killed after durable CSV capture to
verify pending-page recovery with real data. No synthetic rows are generated.
"""

import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import msgspec

from market_service.archive import parse_archive_trades, read_archive_slice
from market_service.book import IntegrityError, decimal_text
from market_service.contracts import Instrument, Trade
from market_service.history_compare import compare_aggregates
from market_service.repair_store import RepairStore
from market_service.source import Consumer
from market_service.store import Journal
from market_service.trade_history import Reply, compatible
from market_service.verification import verify


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def capture_page(catalog, args, *, kill=False):
    gap = next(g for g in catalog.gaps() if g["state"] not in ("REPAIRED", "CONFLICT"))
    lines = (args.output / "same-id-range.csv").read_bytes().splitlines(keepends=True)
    by_id = {int(line.split(b",", 1)[0]): line for line in lines}
    first = gap["cursor"]
    last = min(gap["last_id"], first + 31)
    body = b"".join(by_id[i] for i in range(first, last + 1))
    meta = json.loads((args.output / "archive-profile.json").read_text())
    uri = (
        "https://data.binance.vision/data/spot/daily/trades/BTCUSDT/"
        + args.archive.name
        + f"#member={meta['member']}&sha256={meta['sha256']}"
    )
    sid = catalog.capture_reply(
        gap["id"],
        first,
        last - first + 1,
        Reply(uri, 200, body, time.time_ns(), 1000, source_kind="historical-archive"),
        stop_id=gap["last_id"],
    )
    if kill:
        os.kill(os.getpid(), signal.SIGKILL)
    return {"first_id": first, "last_id": last, **catalog.process_reply(sid)}


def main(args):
    if args.kill_after_capture:
        with RepairStore(args.catalog, args.db) as catalog:
            capture_page(catalog, args, kill=True)
        return
    args.output.mkdir(parents=True, exist_ok=True)
    original_hash = hashlib.sha256(args.db.read_bytes()).hexdigest()
    with Journal(args.db, readonly=True) as journal:
        records = list(journal.records())
    observed = [r.event.payload for r in records if isinstance(r.event.payload, Trade)]
    spec = next(r.event.payload for r in records if isinstance(r.event.payload, Instrument))
    first, last = observed[0].trade_id, observed[-1].trade_id
    cutoff = max(r.event.normalized_wall_ns for r in records)
    csv_body, profile = read_archive_slice(args.archive, args.checksum, first, last)
    profile["inspected_at_utc"] = datetime.now(UTC).isoformat()
    profile["timestamp_resolution_ns"] = 1000
    save(args.output / "archive-profile.json", profile)
    (args.output / "same-id-range.csv").write_bytes(csv_body)
    historical = parse_archive_trades(csv_body, spec, 1000)
    by_id = {t.trade_id: t for t in historical}
    mismatches = [
        t.trade_id for t in observed if not compatible(t, 1_000_000, by_id[t.trade_id], 1000)
    ]
    if mismatches:
        save(args.output / "conflicts.json", mismatches)
        raise IntegrityError("archive conflicts with observed facts; repair not attempted")
    missing = sorted(set(by_id) - {t.trade_id for t in observed})
    # The archive's quoteQty is checked independently without binary floats.
    for line in csv_body.decode().splitlines():
        row = line.split(",")
        if Decimal(row[1]) * Decimal(row[2]) != Decimal(row[3]):
            raise IntegrityError("archive quote quantity differs from price * quantity")
    aggregates = msgspec.json.decode(args.aggregates.read_bytes())
    aggregate_comparison = compare_aggregates(historical, aggregates, spec)
    if aggregate_comparison["mismatch_count"] or aggregate_comparison["missing_raw_count"]:
        raise IntegrityError("raw archive does not reconcile with saved official aggregates")
    report = {
        "executed_at_utc": datetime.now(UTC).isoformat(),
        "synthetic_data_used": False,
        "archive": profile,
        "original_sha256_before": original_hash,
        "observed_raw_count": len(observed),
        "historical_raw_count": len(historical),
        "matched_observed_count": len(observed) - len(mismatches),
        "mismatches": mismatches,
        "missing_count": len(missing),
        "missing_first_id": missing[0] if missing else None,
        "missing_last_id": missing[-1] if missing else None,
        "missing_volume_btc": decimal_text(
            sum(by_id[i].quantity_units for i in missing), spec.quantity_unit
        ),
        "observed_volume_btc": decimal_text(
            sum(t.quantity_units for t in observed), spec.quantity_unit
        ),
        "historical_volume_btc": decimal_text(
            sum(t.quantity_units for t in historical), spec.quantity_unit
        ),
        "microsecond_nonzero_remainders": sum(t.trade_time_ns % 1_000_000 != 0 for t in historical),
        "aggregate_comparison": aggregate_comparison,
    }
    with RepairStore(args.catalog, args.db) as catalog:
        catalog.sync_observed()
        report["before"] = catalog.continuity(first, last)
        report["first_committed_page"] = None
        if not report["before"]["complete"]:
            report["first_committed_page"] = capture_page(catalog, args)
            report["cursor_after_first_commit"] = catalog.gaps()
    # A new process sees the committed cursor, captures the next page, then dies.
    if report["first_committed_page"] is not None:
        result = subprocess.run(
            [sys.executable, __file__, *sys.argv[1:], "--kill-after-capture"], check=False
        )
        if result.returncode != -signal.SIGKILL:
            raise AssertionError(f"expected SIGKILL, got {result.returncode}")
        report["pending_capture_child_exit"] = result.returncode
    with RepairStore(args.catalog, args.db) as catalog:
        report["pending_before_recovery"] = catalog.db.execute(
            "SELECT COUNT(*) FROM sources WHERE processed=0 AND kind='historical-archive'"
        ).fetchone()[0]
        report["recovered_pending_facts"] = catalog.recover_pending()
        report["remaining_pages"] = []
        while not catalog.continuity(first, last)["complete"]:
            report["remaining_pages"].append(capture_page(catalog, args))
        facts_before = catalog.db.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
        # Reimport all missing rows in bounded pages to test duplicate handling.
        duplicate_results = []
        gap = catalog.gaps()[0]
        csv_lines = {
            int(line.split(b",", 1)[0]): line for line in csv_body.splitlines(keepends=True)
        }
        for start in range(0, len(missing), 32):
            ids = missing[start : start + 32]
            body = b"".join(csv_lines[i] for i in ids)
            sid = catalog.capture_reply(
                gap["id"],
                ids[0],
                len(ids),
                Reply(
                    "https://data.binance.vision/data/spot/daily/trades/BTCUSDT/"
                    + args.archive.name
                    + "#sha256="
                    + profile["sha256"],
                    200,
                    body,
                    time.time_ns(),
                    1000,
                    source_kind="historical-archive",
                ),
                stop_id=ids[-1],
            )
            duplicate_results.append(catalog.process_reply(sid))
        report["duplicate_import"] = {
            "inserted": sum(r["inserted"] for r in duplicate_results),
            "duplicates": sum(r["duplicates"] for r in duplicate_results),
            "fact_count_before": facts_before,
            "fact_count_after": catalog.db.execute("SELECT COUNT(*) FROM facts").fetchone()[0],
        }
        report["after_repaired"] = catalog.continuity(first, last)
        report["after_observed"] = catalog.continuity(first, last, view="observed")
        report["as_known_at_capture_end"] = catalog.continuity(first, last, known_by_ns=cutoff)
        try:
            Consumer().apply(next(catalog.research(first, last)))
        except IntegrityError:
            report["research_type_rejected_by_live_consumer"] = True
        else:
            raise AssertionError("historical research record accepted by live consumer")
        report["quality_boundaries_preserved"] = catalog.db.execute(
            "SELECT COUNT(*) FROM boundaries"
        ).fetchone()[0]
        report["catalog_integrity"] = catalog.verify_integrity()
        report["gaps"] = catalog.gaps()
        save(args.output / "gap-catalog.json", catalog.gaps())
    reference = json.loads(args.reference.read_text())
    report["original_verify"] = verify(args.db, expected_chain=reference["final_event_chain"])
    report["original_sha256_after"] = hashlib.sha256(args.db.read_bytes()).hexdigest()
    report["original_journal_unchanged"] = report["original_sha256_after"] == original_hash
    report["real_112_raw_backfill_verified"] = (
        len(missing) == 112
        and report["after_repaired"]["complete"]
        and not report["after_observed"]["complete"]
        and not report["as_known_at_capture_end"]["complete"]
        and report["duplicate_import"]["inserted"] == 0
        and report["duplicate_import"]["duplicates"] == 112
        and report["original_journal_unchanged"]
    )
    save(args.output / "acceptance.json", report)
    print(
        json.dumps(
            {
                k: report[k]
                for k in (
                    "matched_observed_count",
                    "missing_count",
                    "missing_volume_btc",
                    "real_112_raw_backfill_verified",
                    "duplicate_import",
                    "original_journal_unchanged",
                )
            },
            ensure_ascii=False,
        )
    )
    if not report["real_112_raw_backfill_verified"]:
        raise AssertionError("real archive acceptance failed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--checksum", type=Path, required=True)
    parser.add_argument("--aggregates", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--kill-after-capture", action="store_true", help=argparse.SUPPRESS)
    main(parser.parse_args())
