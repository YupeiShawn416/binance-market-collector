"""Length-prefixed MessagePack over local Unix sockets, backed by committed log tail.

No separate volatile publish queue: catch-up and follow query the same frontier.
Each client reads independently; drain timeouts disconnect slow clients instead of
blocking capture. Durable client cursor permits reconnect without message loss.
"""

from __future__ import annotations

import asyncio
import os
import stat
import struct
from pathlib import Path

import msgspec

from .book import IntegrityError
from .contracts import Batch, Subscribe, encode
from .source import Consumer
from .store import Journal

MAX_FRAME = 16 * 1024 * 1024


async def read_frame(reader: asyncio.StreamReader) -> bytes:
    size = struct.unpack("!I", await reader.readexactly(4))[0]
    if size == 0 or size > MAX_FRAME:
        raise IntegrityError("invalid wire frame length")
    return await reader.readexactly(size)


async def write_frame(writer: asyncio.StreamWriter, value: object):
    payload = encode(value)
    if len(payload) > MAX_FRAME:
        raise IntegrityError("wire frame too large")
    writer.write(struct.pack("!I", len(payload)) + payload)
    await asyncio.wait_for(writer.drain(), 5)


class Publisher:
    def __init__(
        self,
        journal_path: str | Path,
        socket_path: str | Path,
        *,
        journal_type=Journal,
        subscribe_type=Subscribe,
        batch_type=Batch,
        schema_version=1,
    ):
        self.journal_path, self.socket_path = journal_path, Path(socket_path)
        self.journal_type, self.subscribe_type = journal_type, subscribe_type
        self.batch_type, self.schema_version = batch_type, schema_version
        self.server = None
        self.clients = set()
        self._closing = False
        self._socket_inode = None

    async def start(self):
        if len(os.fsencode(self.socket_path)) > 100:
            raise ValueError("Unix socket path is too long; use a short /tmp path")
        if self.socket_path.exists() or self.socket_path.is_symlink():
            info = self.socket_path.lstat()
            if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid():
                raise FileExistsError(f"refusing to replace non-owned socket: {self.socket_path}")
            try:
                _, probe = await asyncio.wait_for(
                    asyncio.open_unix_connection(str(self.socket_path)), 1
                )
            except ConnectionRefusedError:
                if self.socket_path.lstat().st_ino != info.st_ino:
                    raise FileExistsError("socket changed during stale check") from None
                self.socket_path.unlink()
            else:
                probe.close()
                await probe.wait_closed()
                raise FileExistsError(f"socket already has a listener: {self.socket_path}")
        self.server = await asyncio.start_unix_server(self._serve, path=str(self.socket_path))
        self._socket_inode = self.socket_path.lstat().st_ino
        os.chmod(self.socket_path, 0o600)
        return self

    async def _serve(self, reader, writer):
        task = asyncio.current_task()
        if self._closing or len(self.clients) >= 32:
            writer.close()
            return
        self.clients.add(task)
        try:
            request = msgspec.msgpack.decode(
                await asyncio.wait_for(read_frame(reader), 5), type=self.subscribe_type
            )
            if request.schema_version != self.schema_version:
                raise IntegrityError("unsupported subscription version")
            with self.journal_type(self.journal_path, readonly=True) as journal:
                if request.dataset_id not in (None, journal.dataset_id):
                    raise IntegrityError("subscription dataset mismatch")
                cursor = request.after_seq
                while not reader.at_eof():
                    records = tuple(journal.records(cursor, limit=128))
                    frontier = journal.frontier
                    complete = not request.follow and (
                        not records or records[-1].event.market_seq == frontier
                    )
                    if records or complete:
                        await write_frame(
                            writer, self.batch_type(journal.dataset_id, records, frontier, complete)
                        )
                    if records:
                        cursor = records[-1].event.market_seq
                    if complete:
                        break
                    if not records:
                        await asyncio.sleep(0.02)
        except (
            ConnectionError,
            asyncio.IncompleteReadError,
            TimeoutError,
            IntegrityError,
            msgspec.DecodeError,
        ):
            pass  # A client retries from its OWN committed cursor; capture is unaffected.
        finally:
            writer.close()
            self.clients.discard(task)

    async def close(self):
        self._closing = True
        if self.server:
            self.server.close()
        for task in list(self.clients):
            task.cancel()
        await asyncio.gather(*self.clients, return_exceptions=True)
        if self.server:
            await self.server.wait_closed()
        if (
            self._socket_inode is not None
            and self.socket_path.exists()
            and self.socket_path.lstat().st_ino == self._socket_inode
        ):
            self.socket_path.unlink()


async def consume(socket_path, consumer: Consumer, *, follow=True, stop_after: int | None = None):
    reader, writer = await asyncio.open_unix_connection(str(socket_path))
    try:
        await write_frame(
            writer,
            consumer.subscribe_type(
                dataset_id=consumer.projection.dataset_id,
                after_seq=consumer.projection.seq,
                follow=follow,
            ),
        )
        count = 0
        while True:
            batch = msgspec.msgpack.decode(await read_frame(reader), type=consumer.batch_type)
            if consumer.projection.dataset_id not in (None, batch.dataset_id):
                raise IntegrityError("server changed dataset")
            for record in batch.records:
                consumer.apply(record)
                count += 1
                if stop_after and count >= stop_after:
                    consumer.commit()
                    return count
            consumer.commit()
            if batch.complete:
                return count
    finally:
        writer.close()
        await writer.wait_closed()
