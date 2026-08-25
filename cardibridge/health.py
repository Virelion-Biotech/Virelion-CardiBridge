from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .protocol import PROTOCOL_NAME, PROTOCOL_VERSION
from .registry import ContractRegistry
from .store import EventStore


@dataclass(frozen=True)
class HealthSnapshot:
    status: str
    protocol: str
    protocol_version: str
    contracts: int
    checked_at: datetime
    store_ok: bool

    def as_dict(self) -> dict[str, Any]:
        return {"status": self.status, "protocol": self.protocol, "protocol_version": self.protocol_version,
                "contracts": self.contracts, "store_ok": self.store_ok, "checked_at": self.checked_at.isoformat()}


def health(registry: ContractRegistry, store: EventStore | None = None) -> HealthSnapshot:
    store_ok = True
    if store is not None:
        try:
            store.list_events(limit=1)
        except Exception:
            store_ok = False
    return HealthSnapshot("ok" if store_ok else "degraded", PROTOCOL_NAME, PROTOCOL_VERSION,
                          len(registry._schemas), datetime.now(timezone.utc), store_ok)


@dataclass
class DependencyHealth:
    name: str
    healthy: bool
    latency_ms: float | None = None
    detail: str | None = None


class Readiness:
    """Aggregate liveness/readiness for external dependencies."""

    def __init__(self) -> None:
        self.dependencies: dict[str, DependencyHealth] = {}
        self.started_at = datetime.now(timezone.utc)

    def register(self, name: str, healthy: bool, latency_ms: float | None = None, detail: str | None = None) -> None:
        self.dependencies[name] = DependencyHealth(name, healthy, latency_ms, detail)

    @property
    def ready(self) -> bool:
        return all(item.healthy for item in self.dependencies.values())

    def snapshot(self) -> dict[str, Any]:
        uptime = (datetime.now(timezone.utc) - self.started_at).total_seconds()
        return {"ready": self.ready, "uptime_seconds": round(uptime, 3),
                "dependencies": {name: item.__dict__.copy() for name, item in self.dependencies.items()}}
