"""Cell values and unknown coverage, checked against a deterministic tiny book."""

import asyncio
import hashlib

import msgspec
import pytest
from conftest import METADATA, SNAPSHOT, delta, trade

from market_service.book import IntegrityError
from market_service.preview import PreviewOptions, build_preview
from market_service.preview_html import preview_html
from market_service.service import MarketService
from market_service.store import Journal


@pytest.fixture
def capture(tmp_path, monkeypatch):
    clock = [1_800_000_000_000_000_000]
    monkeypatch.setattr("market_service.service.time.time_ns", lambda: clock[0])
    monkeypatch.setattr("market_service.service.time.monotonic_ns", lambda: clock[0])
    path = tmp_path / "market.sqlite"
    journal = Journal(path)
    service = MarketService(journal, checkpoint_every=4)

    def send(ms, kind, payload, session="s"):
        clock[0] = 1_800_000_000_000_000_000 + int(ms * 1e6)
        frame = service.capture(
            kind, msgspec.json.encode(payload), session, wall_ns=clock[0], mono_ns=clock[0]
        )
        return asyncio.run(service.process(frame))

    send(0, "metadata", METADATA)
    send(0, "control", {"action": "start", "reason": ""})
    send(0, "snapshot", SNAPSHOT)
    send(0, "ws", delta())
    send(0, "ws", trade())
    yield path, journal, service, send
    journal.close()


def options(**overrides):
    return PreviewOptions(price_bins=120, band_bps=200, **overrides)


def row_at(data, price):
    half = data["metadata"]["price_bin_quote"] / 2
    return next(
        i
        for i, center in enumerate(data["price_centers"])
        if center - half <= price < center + half
    )


def test_time_weighted_depth_and_zero_are_not_unknown(capture):
    path, _, service, send = capture
    send(50, "ws", delta(102, bids=[["99.00", "3.00000000"]]))
    send(100, "ws", trade(2))
    data = build_preview(path, options())
    row = row_at(data, 99)
    assert data["depth_base"][row][0] == pytest.approx(2)  # 50ms*1 + 50ms*3, not 1+3.
    assert data["depth_base"][row_at(data, 99.5)][0] == 0
    assert data["depth_base"][row_at(data, 98)][0] is None  # Below snapshot coverage.
    assert data["known_fraction"][row_at(data, 98)][0] == 0
    assert data["metadata"]["final_book_hash"] == service.projection.book_hash
    assert data["metadata"]["final_event_chain"] == service.final_chain
    assert sum(data["trade_counts"]) == 2


def test_disconnect_and_partial_bins_are_never_filled(capture):
    path, _, _, send = capture
    send(100, "ws", delta(102))
    send(150, "control", {"action": "stop", "reason": "disconnected"})
    send(300, "control", {"action": "start", "reason": ""})
    send(300, "snapshot", SNAPSHOT, session="s2")
    send(300, "ws", delta(), session="s2")
    send(400, "ws", trade(2), session="s2")
    data = build_preview(path, options())
    row = row_at(data, 99)
    assert data["depth_base"][row] == [1, None, None, 1]
    assert data["known_fraction"][row][1] == 0.5
    assert data["best_bid"][1:3] == [None, None]
    assert data["metadata"]["valid_book_fraction"] == 0.625
    assert "DISCONNECTED" in data["statuses"]["connection"]


def test_stale_book_cannot_be_kept_fresh_by_trades(capture):
    path, _, _, send = capture
    send(100, "ws", trade(2))
    send(200, "ws", trade(3))
    send(300, "ws", trade(4))
    data = build_preview(path, options(stale_seconds=0.15))
    assert data["depth_base"][row_at(data, 99)] == [1, None, None]
    assert data["statuses"]["book"] == ["LIVE", "STALE", "STALE"]


def test_zero_duration_trade_gap_is_preserved_as_marker(capture):
    path, _, _, send = capture
    send(100, "ws", trade(5))
    send(200, "ws", delta(102))
    data = build_preview(path, options())
    assert data["metadata"]["missing_trade_ids"] == 3
    assert any(q["reason"] == "trade_id_gap:2..4" for q in data["quality_events"])
    assert sum(data["trade_counts"]) == 2


def test_checkpoint_tail_matches_full_replay_and_keeps_freshness(capture):
    path, _, service, send = capture
    for number in range(2, 20):
        send(number * 100, "ws", trade(number))
    full = build_preview(path, options())
    tail = build_preview(path, options(max_events=4))
    assert tail["metadata"]["checkpoint_seq"] > 0
    assert tail["metadata"]["selected_events"] == 4
    assert (
        tail["metadata"]["final_event_chain"]
        == full["metadata"]["final_event_chain"]
        == service.final_chain
    )
    assert all(value is not None for value in tail["depth_base"][row_at(tail, 99)])
    assert tail["metadata"]["missing_trade_ids"] == 0


def test_preview_is_read_only_and_html_is_offline(capture, tmp_path):
    pytest.importorskip("plotly")
    path, journal, _, send = capture
    send(200, "ws", trade(2))
    before = (journal.frontier, journal.db.total_changes)
    destination = tmp_path / "preview.html"
    preview_html(path, destination, options())
    assert before == (journal.frontier, journal.db.total_changes)
    text = destination.read_text()
    assert "plotly.js" in text
    from html.parser import HTMLParser

    class Scripts(HTMLParser):
        external = []

        def handle_starttag(self, tag, attrs):
            if tag == "script":
                self.external.extend(v for k, v in attrs if k == "src")

    scripts = Scripts()
    scripts.feed(text)
    assert not scripts.external
    assert "质量边界" in text
    assert "已观测挂单量" in text
    with pytest.raises(FileExistsError):
        preview_html(path, destination, options())
    with pytest.raises(ValueError, match="overlaps"):
        preview_html(path, path, options())


def test_corrupted_input_is_not_presented_as_valid(capture):
    path, journal, _, send = capture
    send(100, "ws", trade(2))
    with journal.db:
        journal.db.execute(
            "UPDATE raw_frames SET digest=? WHERE raw_seq=(SELECT MAX(raw_seq) FROM raw_frames)",
            ("0" * 64,),
        )
    with pytest.raises(IntegrityError, match="raw checksum"):
        build_preview(path, options())


def test_corrupted_checkpoint_is_rejected(capture):
    path, journal, _, send = capture
    for number in range(2, 12):
        send(number * 100, "ws", trade(number))
    with journal.db:
        journal.db.execute("UPDATE checkpoints SET digest=?", (hashlib.sha256(b"bad").hexdigest(),))
    with pytest.raises(IntegrityError, match="checkpoint checksum"):
        build_preview(path, options(max_events=3))


def test_time_grid_is_bounded_without_changing_event_counts(capture):
    path, _, _, send = capture
    send(5000, "ws", trade(2))
    data = build_preview(path, options(max_time_bins=4))
    assert len(data["time_edges_ns"]) <= 5
    assert data["metadata"]["time_bin_ms"] > 100
    assert sum(data["trade_counts"]) == 2


def test_preview_options_reject_unbounded_or_invalid_values():
    for value in (
        PreviewOptions(max_events=100001),
        PreviewOptions(time_bin_ms=0),
        PreviewOptions(price_bins=10000),
        PreviewOptions(band_bps=float("nan")),
    ):
        with pytest.raises(ValueError):
            value.validate()


def test_wall_clock_regression_does_not_reorder_events(capture):
    path, _, _, send = capture
    send(100, "ws", delta(102, bids=[["99.00", "3.00000000"]]))
    send(50, "ws", delta(103, bids=[["99.00", "1.00000000"]]))
    send(200, "ws", trade(2))
    data = build_preview(path, options())
    assert data["metadata"]["wall_clock_regressions_clamped"] == 1
    assert data["depth_base"][row_at(data, 99)] == [1, 1]


def test_png_uses_same_grid_without_browser(capture, tmp_path):
    pytest.importorskip("matplotlib")
    path, _, _, send = capture
    send(200, "ws", trade(2))
    png = tmp_path / "preview.png"
    result = preview_html(path, tmp_path / "preview.html", options(), png=png)
    assert result["png"] == str(png)
    assert png.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
    with pytest.raises(ValueError, match="overlaps"):
        preview_html(path, tmp_path / "another.html", options(), png=path)


def test_cli_report_cannot_overwrite_source(capture, tmp_path):
    import argparse

    from market_service.cli import run

    path, journal, _, _ = capture
    args = argparse.Namespace(
        command="preview", db=str(path), output=str(tmp_path / "x.html"), png=None, report=str(path)
    )
    with pytest.raises(ValueError, match="overlap"):
        asyncio.run(run(args))
    assert journal.frontier > 0
