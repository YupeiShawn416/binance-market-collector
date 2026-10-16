"""Kill a separate writer process at real SQLite boundaries, then recover."""

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest

from market_service.service import MarketService
from market_service.store import Journal
from market_service.verification import verify


@pytest.mark.parametrize("point", ["raw_only", "before_commit", "after_commit"])
def test_sigkill_writer_recovery_at_commit_boundaries(setup, point):
    path, journal, service = setup
    previous = journal.frontier
    journal.close()
    child = r"""
import asyncio,os,signal,sys,msgspec
from market_service.store import Journal
from market_service.service import MarketService
j=Journal(sys.argv[1]);s=MarketService(j)
payload={"e":"trade","E":1792000000001,"s":"BTCUSDT","t":2,"p":"100.00","q":"0.00000001","T":1792000000000,"m":False}
r=s.capture("ws",msgspec.json.encode(payload),"session-1")
point=sys.argv[2]
if point=="raw_only":os.kill(os.getpid(),signal.SIGKILL)
if point=="before_commit":
 def crash(sql):
  if sql=="COMMIT":os.kill(os.getpid(),signal.SIGKILL)
 j.db.set_trace_callback(crash)
asyncio.run(s.process(r))
os.kill(os.getpid(),signal.SIGKILL)
"""
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).parents[1] / "src")}
    process = subprocess.run([sys.executable, "-c", child, str(path), point], env=env, timeout=10)
    assert process.returncode == -9
    with Journal(path) as reopened:
        if point != "after_commit":
            assert reopened.frontier == previous
            assert len(list(reopened.pending())) == 1
        recovered = MarketService(reopened)
        asyncio.run(recovered.recover_pending())
        assert recovered.projection.last_trade_id == 2
        assert reopened.frontier == previous + 1
        assert not list(reopened.pending())
        result = verify(path, expected_chain=recovered.final_chain)
        assert result["counts"]["Trade"] == 2
