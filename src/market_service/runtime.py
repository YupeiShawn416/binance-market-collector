"""Collector lifecycle and observability, independent of normalization and replay."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import time
from pathlib import Path

from .config import CollectorConfig
from .ipc import Publisher
from .network import CaptureStats, collect
from .service import MarketService
from .store import Journal


class DiskReserveError(RuntimeError):
    pass


def check_disk(config: CollectorConfig) -> int:
    available = shutil.disk_usage(Path(config.db).parent).free
    if available < config.min_free_bytes:
        raise DiskReserveError(f"free disk {available} bytes below reserve {config.min_free_bytes}")
    return available


def atomic_json(path: str | Path, value: dict):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def snapshot(
    service: MarketService, stats: CaptureStats, config: CollectorConfig, state: str
) -> dict:
    now = time.monotonic()
    ages = {
        name: round(max(0, now - value), 3) if value is not None else None
        for name, value in (
            ("ws", stats.last_ws_mono),
            ("book", stats.last_book_mono),
            ("trade", stats.last_trade_mono),
        )
    }
    fresh = all(age is not None and age <= config.stale_seconds for age in ages.values())
    journal = service.journal
    latest = journal.db.execute("SELECT COALESCE(MAX(raw_seq),0) FROM raw_frames").fetchone()[0]
    pending = journal.db.execute("SELECT COUNT(*) FROM raw_frames WHERE processed=0").fetchone()[0]
    ready = state == "RUNNING" and fresh and service.projection.ready and pending == 0
    return {
        "health_version": 1,
        "updated_wall_ns": time.time_ns(),
        "pid": os.getpid(),
        "boot_id": service.boot_id,
        "state": state if state != "RUNNING" or ready else "DEGRADED",
        "ready": ready,
        "dataset_id": journal.dataset_id,
        "db": str(journal.path),
        "market_seq": journal.frontier,
        "writer_poisoned": service.poisoned,
        "raw_seq": latest,
        "pending_raw": pending,
        "quality": dict(service.projection.quality),
        "book_epoch": service.projection.epoch,
        "last_update_id": service.projection.last_update_id,
        "last_trade_id": service.projection.last_trade_id,
        "book_hash": service.projection.book_hash,
        "event_chain": service.final_chain,
        "age_seconds": ages,
        "retry_in_seconds": round(max(0, stats.retry_at_mono - now), 3)
        if stats.retry_at_mono
        else None,
        "disk_free_bytes": shutil.disk_usage(journal.path.parent).free,
        "journal_bytes": sum(
            p.stat().st_size for p in (journal.path, Path(str(journal.path) + "-wal")) if p.exists()
        ),
        **stats.report(),
    }


def read_health(config: CollectorConfig) -> dict:
    data = json.loads(Path(config.health).read_text())
    age = (time.time_ns() - data["updated_wall_ns"]) / 1e9
    # A dead process or a stuck event loop cannot retain a green cached heartbeat.
    if age < -1 or age > config.heartbeat_seconds * 3:
        data = {**data, "state": "STALE", "ready": False}
    data["heartbeat_age_seconds"] = round(age, 3)
    return data


async def run_collector(
    config: CollectorConfig, *, stop: asyncio.Event | None = None, seconds: float | None = None
) -> dict:
    config.validate()
    stop = stop if stop is not None else asyncio.Event()
    Path(config.db).parent.mkdir(parents=True, exist_ok=True)
    # Acquire the writer lock before touching the shared health file.
    with Journal(config.db) as journal:
        service = MarketService(journal, checkpoint_every=config.checkpoint_every)
        stats = CaptureStats()
        state, failure = "RUNNING", None
        publisher = None
        collector = monitor = None

        def write_health():
            report = snapshot(service, stats, config, state)
            atomic_json(config.health, report)
            return report

        async def observe():
            while not stop.is_set():
                check_disk(config)
                write_health()
                try:
                    await asyncio.wait_for(stop.wait(), config.heartbeat_seconds)
                except TimeoutError:
                    continue

        try:
            check_disk(config)
            # Recovery is verified by Journal.recover(), not by a second implementation.
            await service.recover_pending()
            # Persist a boundary immediately, even if metadata/WS never connect on this boot.
            if service.projection.instrument is not None:
                await service.control(
                    "stop", f"startup-{service.boot_id}", "process_restart_requires_resync"
                )
            write_health()
            publisher = await Publisher(config.db, config.socket).start() if config.socket else None
            collector = asyncio.create_task(
                collect(
                    service,
                    seconds=seconds,
                    stop=stop,
                    stats=stats,
                    rest_base=config.rest_base.rstrip("/"),
                    ws_base=config.ws_base.rstrip("/"),
                    stale_seconds=config.stale_seconds,
                    retry_initial=config.retry_initial,
                    retry_max=config.retry_max,
                ),
                name="binance-capture",
            )
            monitor = asyncio.create_task(observe(), name="collector-health")
            done, _ = await asyncio.wait((collector, monitor), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
            stop.set()
            await collector
            state = "STOPPED"
        except Exception as exc:
            state = "DISK_LOW" if isinstance(exc, DiskReserveError) else "FAILED"
            failure = exc
        finally:
            stop.set()
            if collector is not None:
                # Cooperatively interrupt sockets/HTTP/backoff; retain durable queued tail.
                results = await asyncio.gather(collector, return_exceptions=True)
                if isinstance(results[0], Exception) and failure is None:
                    state, failure = "FAILED", results[0]
            if monitor is not None:
                results = await asyncio.gather(monitor, return_exceptions=True)
                if isinstance(results[0], Exception) and failure is None:
                    failure = results[0]
                    state = "DISK_LOW" if isinstance(failure, DiskReserveError) else "FAILED"
            if publisher is not None:
                await publisher.close()
            report = write_health()
        if failure is not None:
            report["failure"] = f"{type(failure).__name__}: {failure}"
            atomic_json(config.health, report)
            raise failure
        return report


async def run_with_signals(config: CollectorConfig, *, seconds: float | None = None) -> dict:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    signals = (signal.SIGINT, signal.SIGTERM)
    for sig in signals:
        loop.add_signal_handler(sig, stop.set)
    try:
        return await run_collector(config, stop=stop, seconds=seconds)
    finally:
        for sig in signals:
            loop.remove_signal_handler(sig)
