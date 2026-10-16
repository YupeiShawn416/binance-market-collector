"""Version 1 wire contract. Frozen messages contain integers, strings and tuples only."""

from __future__ import annotations

from typing import Annotated, Literal

import msgspec

SCHEMA_VERSION = 1
PARSER_VERSION = "binance-spot-cryptofeed-3.0.2/1"
I64 = Annotated[int, msgspec.Meta(ge=-(2**63), le=2**63 - 1)]
N64 = Annotated[int, msgspec.Meta(ge=0, le=2**63 - 1)]


class Instrument(msgspec.Struct, frozen=True, forbid_unknown_fields=True, tag="instrument"):
    symbol: str
    base: str
    quote: str
    price_tick: str
    quantity_unit: str
    order_step: str
    spec_version: str


class Snapshot(msgspec.Struct, frozen=True, forbid_unknown_fields=True, tag="snapshot"):
    last_update_id: N64
    bids: tuple[tuple[N64, N64], ...]
    asks: tuple[tuple[N64, N64], ...]
    source_depth: int
    known_bid_floor: int | None
    known_ask_ceiling: int | None


class Delta(msgspec.Struct, frozen=True, forbid_unknown_fields=True, tag="delta"):
    first_update_id: N64
    last_update_id: N64
    bids: tuple[tuple[N64, N64], ...]
    asks: tuple[tuple[N64, N64], ...]


class Trade(msgspec.Struct, frozen=True, forbid_unknown_fields=True, tag="trade"):
    trade_id: N64
    price_ticks: N64
    quantity_units: N64
    aggressor: Literal["buy", "sell"]
    trade_time_ns: I64


class Quality(msgspec.Struct, frozen=True, forbid_unknown_fields=True, tag="quality"):
    scope: Literal["connection", "book", "trades"]
    state: Literal["SYNCING", "LIVE", "INVALID", "DISCONNECTED"]
    reason: str


Payload = Instrument | Snapshot | Delta | Trade | Quality


class Event(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema_version: Literal[1]
    dataset_id: str
    market_seq: N64
    venue: str
    instrument: str
    session_id: str
    book_epoch: N64
    raw_seq: N64
    receive_wall_ns: I64
    receive_mono_ns: I64
    boot_id: str
    exchange_time_ns: I64 | None
    source_resolution_ns: int | None
    parser_version: str
    normalized_wall_ns: I64
    normalized_mono_ns: I64
    normalizer_instance_id: str
    payload: Payload
    book_hash: str


class RawFrame(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    raw_seq: N64
    session_id: str
    kind: str
    receive_wall_ns: I64
    receive_mono_ns: I64
    boot_id: str
    payload: bytes


class Record(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    event: Event
    event_hash: str
    chain_hash: str


class Subscribe(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema_version: Literal[1] = SCHEMA_VERSION
    dataset_id: str | None = None
    after_seq: N64 = 0
    follow: bool = True


class Batch(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    dataset_id: str
    records: tuple[Record, ...]
    frontier: N64
    complete: bool


EVENT_ENCODER = msgspec.msgpack.Encoder()
EVENT_DECODER = msgspec.msgpack.Decoder(Event)
RECORD_DECODER = msgspec.msgpack.Decoder(Record)


def encode(value: object) -> bytes:
    return EVENT_ENCODER.encode(value)


def decode_event(data: bytes) -> Event:
    event = EVENT_DECODER.decode(data)
    if event.schema_version != SCHEMA_VERSION:
        raise ValueError(f"unsupported event schema {event.schema_version}")
    return event
