from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .contracts import BridgeEnvelope
from .reliability import DeliveryAttempt


@dataclass(frozen=True)
class DeadLetter:
    envelope: BridgeEnvelope
    reason: str
    attempts: tuple[DeliveryAttempt, ...] = ()
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.envelope.message_id,
            "message_type": self.envelope.message_type,
            "producer": self.envelope.producer,
            "consumer": self.envelope.consumer,
            "reason": self.reason,
            "attempts": [a.as_dict() for a in self.attempts],
            "created_at": self.created_at.isoformat(),
            "metadata": self.metadata or {},
        }


class DeadLetterQueue:
    """In-process DLQ abstraction; production transports can persist/stream these records."""

    def __init__(self) -> None:
        self._items: dict[str, DeadLetter] = {}

    def put(self, item: DeadLetter) -> None:
        self._items[item.envelope.message_id] = item

    def get(self, message_id: str) -> DeadLetter | None:
        return self._items.get(message_id)

    def list(self, limit: int = 100) -> list[DeadLetter]:
        return list(self._items.values())[-limit:]

    def remove(self, message_id: str) -> DeadLetter | None:
        return self._items.pop(message_id, None)

    def __len__(self) -> int:
        return len(self._items)
