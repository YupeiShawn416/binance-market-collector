"""Two real-data consumers, one resumable cursor, one independent replay transcript."""

import argparse
import asyncio
import json
import tempfile
from pathlib import Path

from market_service.cli import l3_publisher
from market_service.ipc import consume
from market_service.l3.service import L3Consumer
from market_service.source import replay


async def run(args):
    folder = Path(args.output)
    folder.mkdir(parents=True, exist_ok=True)
    state = folder / "decision-state.sqlite"
    if state.exists():
        raise ValueError("use a new output directory for independent cursor acceptance")
    reference = L3Consumer(record_trace=True)
    list(replay(args.db, reference))
    with tempfile.TemporaryDirectory(prefix="l3-", dir="/tmp") as directory:
        socket = Path(directory) / "market.sock"
        publisher = await l3_publisher(args.db, socket).start()
        try:
            first = L3Consumer(state, record_trace=True)
            await consume(socket, first, follow=False, stop_after=25)
            prefix = first.trace
            first.close()
            decision = L3Consumer(state, record_trace=True)
            trading = L3Consumer(folder / "trading-state.sqlite", record_trace=True)
            try:
                await asyncio.gather(
                    consume(socket, decision, follow=False), consume(socket, trading, follow=False)
                )
                assert prefix + decision.trace == trading.trace == reference.trace
                assert decision.chain_hash == trading.chain_hash == reference.chain_hash
                assert (
                    decision.projection.book_hash
                    == trading.projection.book_hash
                    == reference.projection.book_hash
                )
                assert (
                    decision.projection.data_origin
                    == trading.projection.data_origin
                    == "historical"
                )
                result = {
                    "dataset_id": reference.projection.dataset_id,
                    "data_origin": reference.projection.data_origin,
                    "events": reference.projection.seq,
                    "resumed_after_seq": 25,
                    "all_event_and_order_hash_transcripts_equal": True,
                    "final_order_state_hash": reference.projection.book_hash,
                    "final_event_chain": reference.chain_hash,
                    "quality_at_end": reference.projection.quality,
                    "execution_allowed": False,
                }
            finally:
                decision.close()
                trading.close()
        finally:
            await publisher.close()
    (folder / "acceptance.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--output", required=True)
    asyncio.run(run(parser.parse_args()))
