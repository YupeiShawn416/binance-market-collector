"""Durable automatic trade-gap repair; transport failures cannot stop live capture."""

from __future__ import annotations

import asyncio
import time

import aiohttp

from .repair_store import RepairStore


class RepairWorker:
    def __init__(self, catalog: RepairStore, client, *, page_size=1000):
        if not 1 <= page_size <= 1000:
            raise ValueError("page_size must be 1..1000")
        self.catalog, self.client, self.page_size = catalog, client, page_size

    async def run_once(self, *, max_pages=None, retry_now=False):
        imported = self.catalog.sync_observed()
        recovered = self.catalog.recover_pending()
        pages, inserted, duplicates = 0, recovered, 0
        for gap in self.catalog.gaps():
            if int(self.catalog.meta("endpoint_blocked_until_ns") or 0) > time.time_ns():
                break
            if gap["state"] in ("REPAIRED", "CONFLICT"):
                continue
            if not retry_now and gap["next_attempt_ns"] > time.time_ns():
                continue
            while True:
                current = next(g for g in self.catalog.gaps() if g["id"] == gap["id"])
                if current["state"] in ("REPAIRED", "CONFLICT") or (
                    max_pages is not None and pages >= max_pages
                ):
                    break
                first = current["cursor"]
                limit = min(self.page_size, current["last_id"] - first + 1)
                try:
                    reply = await self.client.fetch(first, limit)
                except (aiohttp.ClientError, TimeoutError, ConnectionError) as exc:
                    self.catalog.defer(
                        current["id"],
                        f"{type(exc).__name__}:{exc}",
                        until_ns=getattr(self.client, "blocked_until_ns", 0),
                    )
                    break
                sid = self.catalog.capture_reply(
                    current["id"], first, limit, reply, stop_id=current["last_id"]
                )
                pages += 1
                try:
                    result = self.catalog.process_reply(sid)
                except (ValueError, KeyError, TypeError, ArithmeticError):
                    # Rejected evidence/job error are durable; don't retry in a tight loop.
                    if getattr(self.client, "blocked_until_ns", 0):
                        with self.catalog.db:
                            self.catalog.db.execute(
                                "UPDATE gaps SET next_attempt_ns=MAX(next_attempt_ns,?) WHERE id=?",
                                (self.client.blocked_until_ns, current["id"]),
                            )
                    break
                inserted += result["inserted"]
                duplicates += result["duplicates"]
        gaps = self.catalog.gaps()
        return {
            "observed_imported": imported,
            "pages": pages,
            "inserted": inserted,
            "duplicates": duplicates,
            "gaps": gaps,
            "all_discovered_gaps_repaired": all(g["state"] == "REPAIRED" for g in gaps),
            "endpoint_blocked_until_ns": int(self.catalog.meta("endpoint_blocked_until_ns") or 0),
        }

    async def follow(self, stop: asyncio.Event):
        """The same bounded work loop can run alongside capture or as a separate command."""
        while not stop.is_set():
            await self.run_once(max_pages=1)
            try:
                await asyncio.wait_for(stop.wait(), timeout=1)
            except TimeoutError:
                pass
