"""Strict, versioned runtime settings; paths are relative to the TOML file."""

from __future__ import annotations

import math
import tomllib
from pathlib import Path
from urllib.parse import urlsplit

import msgspec

from .network import REST_BASE, WS_BASE


class CollectorConfig(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    config_version: int = 1
    db: str = "data/btc.sqlite"
    health: str = "data/health.json"
    socket: str | None = None
    rest_base: str = REST_BASE
    ws_base: str = WS_BASE
    heartbeat_seconds: float = 5
    stale_seconds: float = 10
    retry_initial: float = 1
    retry_max: float = 60
    min_free_bytes: int = 2 * 1024**3
    checkpoint_every: int = 250

    def validate(self):
        if self.config_version != 1:
            raise ValueError("unsupported config_version")
        for name in ("heartbeat_seconds", "stale_seconds", "retry_initial", "retry_max"):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if self.retry_initial > self.retry_max or self.heartbeat_seconds > self.stale_seconds:
            raise ValueError(
                "retry_initial <= retry_max and heartbeat_seconds <= stale_seconds required"
            )
        if self.min_free_bytes < 0 or self.checkpoint_every < 1:
            raise ValueError("invalid disk reserve or checkpoint interval")
        paths = [Path(p).resolve() for p in (self.db, self.health, self.socket) if p]
        if len(set(paths)) != len(paths):
            raise ValueError("db, health and socket must be distinct paths")
        reserved = {Path(self.db + suffix).resolve() for suffix in ("-wal", "-shm", ".lockfile")}
        if any(p in reserved for p in paths[1:]):
            raise ValueError("output overlaps SQLite sidecar")
        for name, schemes in (("rest_base", ("https",)), ("ws_base", ("wss",))):
            parsed = urlsplit(getattr(self, name))
            local = parsed.hostname in ("127.0.0.1", "localhost", "::1")
            if (
                not parsed.hostname
                or parsed.username
                or parsed.password
                or parsed.query
                or parsed.fragment
                or parsed.path not in ("", "/")
                or parsed.scheme not in (*schemes, *(("http", "ws") if local else ()))
            ):
                raise ValueError(f"invalid public endpoint: {name}")
        return self


def load_config(path: str | Path) -> CollectorConfig:
    path = Path(path).resolve()
    with path.open("rb") as stream:
        config = msgspec.convert(tomllib.load(stream), type=CollectorConfig, strict=True)
    resolved = {
        name: str((path.parent / getattr(config, name)).resolve())
        for name in ("db", "health", "socket")
        if getattr(config, name)
    }
    return msgspec.structs.replace(config, **resolved).validate()
