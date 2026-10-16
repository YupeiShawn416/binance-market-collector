"""Synthetic CSV protocol tests; real archive acceptance is kept in evidence/."""

import hashlib
import time
import zipfile

import pytest
from conftest import feed, trade

from market_service.archive import parse_archive_trades, read_archive_slice
from market_service.book import IntegrityError
from market_service.repair_store import RepairStore
from market_service.trade_history import Reply


def csv_rows(first, count):
    return b"".join(
        f"{i},100.00,0.00000001,0.00000100,1792000000000123,False,True\n".encode()
        for i in range(first, first + count)
    )


def test_archive_checksum_and_raw_id_coverage(tmp_path):
    path = tmp_path / "BTCUSDT-trades-2026-10-15.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(path.stem + ".csv", csv_rows(1, 114))
    checksum = path.with_suffix(".zip.CHECKSUM")
    checksum.write_text(hashlib.sha256(path.read_bytes()).hexdigest() + "  " + path.name)
    body, profile = read_archive_slice(path, checksum, 2, 113)
    assert body == csv_rows(2, 112)
    assert profile["archive_rows"] == 114 and profile["member_crc_verified"]
    with pytest.raises(IntegrityError, match="does not cover"):
        read_archive_slice(path, checksum, 1, 115)
    checksum.write_text("0" * 64 + "  " + path.name)
    with pytest.raises(IntegrityError, match="SHA256 mismatch"):
        read_archive_slice(path, checksum, 2, 113)


def test_archive_pending_recovery_and_duplicate_source(setup, tmp_path):
    path, _, service = setup
    feed(service, "ws", trade(114))
    target = tmp_path / "repair.sqlite"
    with RepairStore(target, path) as store:
        store.sync_observed()
        cutoff = time.time_ns()
        reply = Reply(
            "synthetic://csv",
            200,
            csv_rows(2, 32),
            time.time_ns(),
            1000,
            source_kind="historical-archive",
        )
        store.capture_reply(1, 2, 32, reply, stop_id=113)
    with RepairStore(target, path) as store:
        assert store.recover_pending() == 32
        assert store.gaps()[0]["cursor"] == 34
        assert not store.continuity(1, 114, known_by_ns=cutoff)["complete"]
        sid = store.capture_reply(1, 2, 32, reply, stop_id=113)
        assert store.process_reply(sid)["duplicates"] == 32
        assert (
            store.db.execute("SELECT kind FROM sources WHERE id=?", (sid,)).fetchone()[0]
            == "historical-archive"
        )
        assert (
            parse_archive_trades(reply.body, store.spec, 1000)[0].trade_time_ns % 1_000_000
            == 123000
        )
        assert store.verify_integrity()["facts"] == 34


@pytest.mark.parametrize(
    "body",
    [b"1,2,3,4\n", csv_rows(2, 1).replace(b"False", b"false"), csv_rows(2, 1) + csv_rows(2, 1)],
)
def test_archive_malformed_page_never_advances(setup, tmp_path, body):
    path, _, service = setup
    feed(service, "ws", trade(114))
    with RepairStore(tmp_path / "repair.sqlite", path) as store:
        store.sync_observed()
        sid = store.capture_reply(
            1,
            2,
            32,
            Reply(
                "synthetic://bad-csv",
                200,
                body,
                time.time_ns(),
                1000,
                source_kind="historical-archive",
            ),
            stop_id=113,
        )
        with pytest.raises(ValueError):
            store.process_reply(sid)
        assert store.gaps()[0]["cursor"] == 2
        assert store.verify_integrity()["facts"] == 2
