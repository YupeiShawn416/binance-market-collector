import asyncio
import hashlib

import msgspec
import pytest
from conftest import trade

from market_service.backup import backup_journal
from market_service.service import MarketService
from market_service.store import Journal
from market_service.verification import verify


def test_backup_of_active_writer_preserves_wal_and_replays_identically(setup, tmp_path):
    path, journal, service = setup
    destination = tmp_path / "backup.sqlite"
    result = backup_journal(path, destination)
    assert result["market_seq"] == journal.frontier
    assert result["dataset_id"] == journal.dataset_id
    assert result["sha256"] == hashlib.sha256(destination.read_bytes()).hexdigest()
    assert (
        verify(destination, expected_chain=service.final_chain)["final_book_hash"]
        == service.projection.book_hash
    )
    with pytest.raises(FileExistsError):
        backup_journal(path, destination)
    assert not list(tmp_path.glob(".journal-backup-*"))


def test_backup_preserves_pending_raw_for_recovery(setup, tmp_path):
    path, journal, service = setup
    service.capture("ws", msgspec.json.encode(trade(2)), "session-1")
    destination = tmp_path / "backup.sqlite"
    result = backup_journal(path, destination)
    assert result["pending_raw"] == 1
    with Journal(destination) as restored:
        recovered = MarketService(restored)
        asyncio.run(recovered.recover_pending())
        assert recovered.projection.last_trade_id == 2
        assert not list(restored.pending())
    assert len(list(journal.pending())) == 1  # Copy/recovery never mutates the source.
