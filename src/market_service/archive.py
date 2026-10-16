"""Read checksum-verified Binance daily raw-trade archives without expanding aggregates.

ZIP/CSV handling uses the standard library. The original ZIP remains the evidence;
extracted pages retain its exact CSV bytes and identify its SHA256 in their URI.
"""

from __future__ import annotations

import csv
import hashlib
import io
import re
import zipfile
from pathlib import Path

import msgspec

from .book import IntegrityError
from .trade_history import parse_trades


def parse_archive_trades(body, spec, resolution_ns):
    """Strict seven-column, headerless spot trades CSV; time unit is explicit."""
    rows = []
    for row in csv.reader(io.StringIO(body.decode("utf-8"))):
        if (
            len(row) != 7
            or not row[0].isdigit()
            or not row[4].isdigit()
            or row[5] not in ("True", "False")
            or row[6] not in ("True", "False")
        ):
            raise IntegrityError("invalid spot raw-trade archive CSV")
        rows.append(
            {
                "id": int(row[0]),
                "price": row[1],
                "qty": row[2],
                "time": int(row[4]),
                "isBuyerMaker": row[5] == "True",
            }
        )
    return parse_trades(msgspec.json.encode(rows), spec, resolution_ns)


def read_archive_slice(path, checksum_path, first_id, last_id):
    """Verify ZIP SHA256/CRC and full-file ID continuity, retain a bounded CSV slice.

    This does not certify the exchange's archive against an independent source.
    The caller must validate selected rows and their declared timestamp unit.
    """
    path, checksum_path = Path(path), Path(checksum_path)
    if first_id < 0 or last_id < first_id or last_id - first_id >= 100_000:
        raise ValueError("explicit bounded archive ID range required")
    fields = checksum_path.read_text().split()
    if (
        len(fields) != 2
        or not re.fullmatch(r"[0-9a-fA-F]{64}", fields[0])
        or fields[1].lstrip("*") != path.name
    ):
        raise IntegrityError("invalid archive checksum file or filename")
    with path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    if digest != fields[0].lower():
        raise IntegrityError("archive SHA256 mismatch")
    selected, count, previous, first, first_time, last_time = [], 0, None, None, None, None
    member = path.stem + ".csv"
    with zipfile.ZipFile(path) as archive:
        if archive.namelist() != [member]:
            raise IntegrityError("unexpected archive members")
        # Reading to EOF verifies the member CRC, including data after the requested slice.
        with archive.open(member) as stream:
            for line in stream:
                columns = line.rstrip(b"\r\n").split(b",")
                if len(columns) != 7:
                    raise IntegrityError("archive column count changed")
                number, timestamp = int(columns[0]), int(columns[4])
                if previous is not None and number != previous + 1:
                    raise IntegrityError("archive raw IDs are not contiguous")
                if first is None:
                    first, first_time = number, timestamp
                previous, last_time = number, timestamp
                count += 1
                if first_id <= number <= last_id:
                    selected.append(line)
    if len(selected) != last_id - first_id + 1:
        raise IntegrityError("archive does not cover requested raw trade IDs")
    return b"".join(selected), {
        "sha256": digest,
        "checksum_verified": True,
        "member_crc_verified": True,
        "member": member,
        "archive_rows": count,
        "archive_first_id": first,
        "archive_last_id": previous,
        "archive_first_time_us": first_time,
        "archive_last_time_us": last_time,
        "archive_ids_contiguous": True,
        "selected_first_id": first_id,
        "selected_last_id": last_id,
        "selected_rows": len(selected),
        "selected_csv_sha256": hashlib.sha256(b"".join(selected)).hexdigest(),
    }
