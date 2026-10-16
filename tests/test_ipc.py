import asyncio

from conftest import delta

from market_service.ipc import Publisher, consume
from market_service.source import Consumer


def test_two_socket_consumers_resume_and_match_history(setup, tmp_path, socket_path):
    path, journal, service = setup
    # macOS Unix socket paths are short: use pytest's short /tmp base in test invocation.
    socket = socket_path
    state = tmp_path / "cursor.sqlite"

    async def scenario():
        publisher = await Publisher(path, socket).start()
        try:
            first = Consumer(state, record_trace=True)
            await consume(socket, first, follow=False, stop_after=4)
            first.close()
            decision, trading = Consumer(state, record_trace=True), Consumer(record_trace=True)
            await asyncio.gather(
                consume(socket, decision, follow=False), consume(socket, trading, follow=False)
            )
            assert decision.projection.seq == trading.projection.seq == journal.frontier
            assert decision.projection.book_hash == trading.projection.book_hash
            assert decision.trace == service.trace[4:]
            assert trading.trace == service.trace
            decision.close()
            trading.close()
        finally:
            await publisher.close()

    asyncio.run(scenario())
    assert not socket.exists()


def test_follow_catches_new_commits_without_catchup_tail_gap(setup, tmp_path, socket_path):
    path, journal, service = setup

    async def scenario():
        publisher = await Publisher(path, socket_path).start()
        consumer = Consumer(record_trace=True)
        try:
            task = asyncio.create_task(
                consume(socket_path, consumer, follow=True, stop_after=journal.frontier + 1)
            )
            await asyncio.sleep(0.05)
            raw = service.capture("ws", __import__("msgspec").json.encode(delta(102)), "session-1")
            await service.process(raw)
            await asyncio.wait_for(task, 3)
            assert consumer.trace == service.trace
        finally:
            await publisher.close()

    asyncio.run(scenario())


def test_stale_socket_recovers_without_replacing_active_listener(setup, socket_path):
    import socket

    import pytest

    path, _, _ = setup
    stale = socket.socket(socket.AF_UNIX)
    stale.bind(str(socket_path))
    stale.close()

    async def scenario():
        publisher = await Publisher(path, socket_path).start()
        try:
            duplicate = Publisher(path, socket_path)
            with pytest.raises(FileExistsError, match="listener"):
                await duplicate.start()
            await duplicate.close()  # must not unlink the first publisher's socket
            consumer = Consumer()
            await consume(socket_path, consumer, follow=False)
            assert consumer.projection.seq > 0
        finally:
            await publisher.close()

    asyncio.run(scenario())
