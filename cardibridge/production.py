from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from typing import Any

from .contracts import BridgeEnvelope
from .observability import BridgeMetrics
from .protocol import DeliveryReceipt, content_hash, topic_for
from .registry import ContractRegistry
from .store import EventStore

Handler = Callable[[BridgeEnvelope], Any]


class ProductionRouter:
    """Validated, durable, observable router for Agent/Vex/Eval pipelines."""

    _FINAL_STATUSES = frozenset({"processed", "dead_letter"})
    _CLAIMABLE_STATUSES = frozenset({"accepted", "handler_failed"})

    def __init__(
        self,
        registry: ContractRegistry,
        store: EventStore | None = None,
        metrics: BridgeMetrics | None = None,
    ) -> None:
        self.registry = registry
        self.store = store or EventStore()
        self.metrics = metrics or BridgeMetrics()
        self._handlers: dict[tuple[str, str], Handler] = {}

    def register(self, message_type: str, consumer: str, handler: Handler) -> None:
        self.registry.model(message_type)
        if not consumer.strip():
            raise ValueError("consumer must not be empty")
        self._handlers[(message_type, consumer)] = handler

    def _is_same_envelope(self, envelope: BridgeEnvelope) -> bool:
        stored = self.store.get(envelope.idempotency_key)
        if stored is None:
            return False
        return content_hash(stored) == content_hash(envelope.model_dump(mode="json"))

    def dispatch(self, envelope: BridgeEnvelope) -> Any:
        started = time.perf_counter()
        report = self.registry.validate(envelope.message_type, envelope.payload)
        if not report.valid:
            self.metrics.observe("validation_failures")
            raise ValueError(report.model_dump_json())

        existing = self.store.status_by_key(envelope.idempotency_key)
        if existing is not None and not self._is_same_envelope(envelope):
            raise ValueError("idempotency_key is already associated with a different envelope")
        if existing in self._FINAL_STATUSES or existing == "processing":
            self.metrics.observe("duplicates")
            return {"status": "duplicate", "message_id": envelope.message_id}
        if existing is None:
            self.store.append(envelope, status="accepted")

        if not self.store.claim(envelope.idempotency_key, self._CLAIMABLE_STATUSES):
            self.metrics.observe("duplicates")
            return {"status": "duplicate", "message_id": envelope.message_id}

        handler = self._handlers.get((envelope.message_type, envelope.consumer))
        if handler is None:
            self.store.mark(envelope.message_id, "handler_failed")
            self.metrics.observe("delivery_failures")
            raise LookupError(f"no handler for {envelope.message_type!r} -> {envelope.consumer!r}")

        try:
            result = handler(envelope)
            self.store.mark(envelope.message_id, "processed")
            self.metrics.observe("published")
            return result
        except Exception:
            self.store.mark(envelope.message_id, "handler_failed")
            self.metrics.observe("handler_failures")
            raise
        finally:
            self.metrics.latency(started)

    def receipt(self, envelope: BridgeEnvelope) -> DeliveryReceipt:
        accepted = self.store.append(envelope, status="outbox")
        return DeliveryReceipt(
            envelope.message_id,
            envelope.idempotency_key,
            topic_for(envelope),
            envelope.timestamp.isoformat(),
            duplicate=not accepted,
            sequence=None,
        )

    def replay(
        self, topic: str | None = None, after: int = 0, limit: int | None = None
    ) -> Iterable[tuple[int, BridgeEnvelope]]:
        return self.store.replay(topic, after, limit)
