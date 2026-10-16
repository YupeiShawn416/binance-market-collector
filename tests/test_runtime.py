import asyncio
import json
import time
from pathlib import Path
from unittest.mock import patch

import pytest
from test_network import exchange

from market_service.config import CollectorConfig, load_config
from market_service.runtime import DiskReserveError, read_health, run_collector
from market_service.store import Journal
from market_service.verification import verify


def test_config_is_strict_and_paths_are_relative_to_file(tmp_path):
    path = tmp_path / "collector.toml"
    path.write_text('db="db.sqlite"\nhealth="health.json"\n')
    config = load_config(path)
    assert config.db == str(tmp_path / "db.sqlite")
    path.write_text('symbol="ETHUSDT"')
    with pytest.raises(ValueError, match="unknown field"):
        load_config(path)


@pytest.mark.parametrize(
    "changes",
    [
        {"db": "same", "health": "same"},
        {"db": "same", "health": "same-wal"},
        {"heartbeat_seconds": float("nan")},
        {"retry_max": 0.1},
        {"min_free_bytes": -1},
        {"rest_base": "https://user:secret@example.com"},
        {"ws_base": "ws://external.example"},
    ],
)
def test_config_rejects_unsafe_settings(changes):
    with pytest.raises(ValueError):
        CollectorConfig(**changes).validate()


def test_runtime_restart_preserves_dataset_and_replay_hashes(tmp_path):
    async def scenario():
        async with exchange() as (rest, ws, _):
            config = CollectorConfig(
                db=str(tmp_path / "btc.sqlite"),
                health=str(tmp_path / "health.json"),
                min_free_bytes=0,
                heartbeat_seconds=0.02,
                rest_base=rest,
                ws_base=ws,
            )
            first = await run_collector(config, seconds=0.14)
            second = await run_collector(config, seconds=0.14)
            assert first["state"] == second["state"] == "STOPPED"
            assert not read_health(config)["ready"]
            assert first["dataset_id"] == second["dataset_id"]
            assert second["market_seq"] > first["market_seq"]
            assert second["book_epoch"] > first["book_epoch"]
            result = verify(config.db, expected_chain=second["event_chain"])
            assert result["pending_raw"] == 0
            assert result["final_book_hash"] == second["book_hash"]

    asyncio.run(scenario())


def test_disk_guard_stops_capture_and_preserves_journal(tmp_path):
    async def scenario():
        async with exchange() as (rest, ws, _):
            config = CollectorConfig(
                db=str(tmp_path / "btc.sqlite"),
                health=str(tmp_path / "health.json"),
                min_free_bytes=0,
                heartbeat_seconds=0.02,
                rest_base=rest,
                ws_base=ws,
            )
            calls = 0

            def check(_):
                nonlocal calls
                calls += 1
                if calls >= 4:
                    raise DiskReserveError("injected low space")
                return 100

            with patch("market_service.runtime.check_disk", check), pytest.raises(DiskReserveError):
                await run_collector(config)
            health = read_health(config)
            assert health["state"] == "DISK_LOW"
            assert not health["ready"]
            assert verify(config.db)["pending_raw"] == 0
            # Writer lock has been released, so a recovery process can take ownership.
            with Journal(config.db):
                pass

    asyncio.run(scenario())


def test_second_writer_cannot_overwrite_health(tmp_path):
    config = CollectorConfig(
        db=str(tmp_path / "btc.sqlite"), health=str(tmp_path / "health.json"), min_free_bytes=0
    )
    Path(config.health).write_text("original")
    with Journal(config.db), pytest.raises(RuntimeError, match="another market writer"):
        asyncio.run(run_collector(config, seconds=0.01))
    assert Path(config.health).read_text() == "original"


def test_cached_green_health_expires(tmp_path):
    config = CollectorConfig(health=str(tmp_path / "health.json"))
    Path(config.health).write_text(
        json.dumps(
            {"ready": True, "state": "RUNNING", "updated_wall_ns": time.time_ns() - 60_000_000_000}
        )
    )
    assert read_health(config)["state"] == "STALE"
    assert not read_health(config)["ready"]


def test_cli_sigterm_flushes_and_restart_uses_same_dataset(tmp_path):
    import os
    import signal
    import sys

    async def scenario():
        async with exchange() as (rest, ws, _):
            path = tmp_path / "collector.toml"
            path.write_text(
                f'db="btc.sqlite"\nhealth="health.json"\nrest_base="{rest}"\n'
                f'ws_base="{ws}"\nheartbeat_seconds=0.02\nmin_free_bytes=0\n'
            )
            config = load_config(path)
            reports = []
            for _ in range(2):
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-m",
                    "market_service.cli",
                    "run",
                    "--config",
                    str(path),
                    env={
                        **os.environ,
                        "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src"),
                    },
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                try:
                    deadline = time.monotonic() + 5
                    while time.monotonic() < deadline:
                        if Path(config.health).exists():
                            health = read_health(config)
                            if health.get("pid") == process.pid and health["ready"]:
                                break
                        if process.returncode is not None:
                            raise AssertionError((await process.communicate())[1].decode())
                        await asyncio.sleep(0.01)
                    else:
                        raise AssertionError("collector did not reach fresh LIVE state")
                    process.send_signal(signal.SIGTERM)
                    stdout, stderr = await asyncio.wait_for(process.communicate(), 3)
                    assert process.returncode == 0, stderr.decode()
                    reports.append(json.loads(stdout))
                finally:
                    if process.returncode is None:
                        process.kill()
                        await process.wait()
            assert reports[0]["dataset_id"] == reports[1]["dataset_id"]
            assert reports[0]["boot_id"] != reports[1]["boot_id"]
            assert reports[1]["market_seq"] > reports[0]["market_seq"]
            assert reports[1]["state"] == "STOPPED"
            assert reports[1]["pending_raw"] == 0
            verify(config.db, expected_chain=reports[1]["event_chain"])

    asyncio.run(scenario())
