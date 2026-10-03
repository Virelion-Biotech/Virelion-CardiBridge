from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import RLock
from typing import TYPE_CHECKING, Any

from .contracts import BridgeEnvelope

if TYPE_CHECKING:
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
            "metadata": dict(self.metadata or {}),
        }


class DeadLetterQueue:
    """Thread-safe in-process DLQ abstraction."""

    def __init__(self, max_items: int = 10_000) -> None:
        if max_items < 1:
            raise ValueError("max_items must be >= 1")
        self.max_items = max_items
        self._items: OrderedDict[str, DeadLetter] = OrderedDict()
        self._lock = RLock()

    def put(self, item: DeadLetter) -> None:
        with self._lock:
            self._items.pop(item.envelope.message_id, None)
            self._items[item.envelope.message_id] = item
            while len(self._items) > self.max_items:
                self._items.popitem(last=False)

    def get(self, message_id: str) -> DeadLetter | None:
        with self._lock:
            return self._items.get(message_id)

    def list(self, limit: int = 100) -> list[DeadLetter]:
        if limit < 1:
            return []
        with self._lock:
            return list(self._items.values())[-limit:]

    def remove(self, message_id: str) -> DeadLetter | None:
        with self._lock:
            return self._items.pop(message_id, None)

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)
