"""L3 contract v2 preserves identities, raw decimal text and message atomicity."""

from typing import Literal

import msgspec

from ..contracts import I64, N64, Batch, Event, Instrument, Quality, Record, Subscribe


class Order(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    order_id: str
    side: Literal["buy", "sell"]
    price_ticks: N64
    quantity_units: N64
    priority_time_ns: I64
    price_text: str
    quantity_text: str


class Change(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    action: Literal["add", "modify", "delete"]
    order: Order


class OrderSnapshot(msgspec.Struct, frozen=True, forbid_unknown_fields=True, tag="l3_snapshot"):
    orders: tuple[Order, ...]
    depth: Literal[0, 10, 100, 1000]
    checksum: N64 | None
    visibility: Literal["visible_only"] = "visible_only"
    source_sequence: N64 | None = None
    known_bid_floor: N64 | None = None
    known_ask_ceiling: N64 | None = None
    queue_order_known: bool = True


class OrderBatch(msgspec.Struct, frozen=True, forbid_unknown_fields=True, tag="l3_batch"):
    changes: tuple[Change, ...]
    checksum: N64 | None
    source_sequence: N64 | None = None


class EventV2(Event):
    schema_version: Literal[2]
    payload: Instrument | Quality | OrderSnapshot | OrderBatch
    historical_capture_ns: I64 | None = None
    data_origin: Literal["live", "historical", "synthetic", "unknown"] = "unknown"


class RecordV2(Record):
    event: EventV2


class SubscribeV2(Subscribe):
    schema_version: Literal[2] = 2


class BatchV2(Batch):
    records: tuple[RecordV2, ...]


DECODER = msgspec.msgpack.Decoder(EventV2)


def decode_event(blob):
    return DECODER.decode(blob)
