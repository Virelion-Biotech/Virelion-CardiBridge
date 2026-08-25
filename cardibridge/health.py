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
        return {
            "status": self.status,
            "protocol": self.protocol,
            "protocol_version": self.protocol_version,
            "contracts": self.contracts,
            "store_ok": self.store_ok,
            "checked_at": self.checked_at.isoformat(),
        }


def health(registry: ContractRegistry, store: EventStore | None = None) -> HealthSnapshot:
    store_ok = True
    if store is not None:
        try:
            store.list_events(limit=1)
        except Exception:
            store_ok = False
    return HealthSnapshot(
        status="ok" if store_ok else "degraded",
        protocol=PROTOCOL_NAME,
        protocol_version=PROTOCOL_VERSION,
        contracts=len(registry._schemas),
        checked_at=datetime.now(timezone.utc),
        store_ok=store_ok,
    )
