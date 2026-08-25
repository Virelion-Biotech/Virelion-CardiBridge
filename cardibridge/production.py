from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from .contracts import BridgeEnvelope
from .observability import BridgeMetrics
from .protocol import DeliveryReceipt, topic_for
from .registry import ContractRegistry
from .store import EventStore

Handler = Callable[[BridgeEnvelope], Any]


class ProductionRouter:
    """Validated, durable, observable router for Agent/Vex/Eval pipelines."""

    def __init__(self, registry: ContractRegistry, store: EventStore | None = None, metrics: BridgeMetrics | None = None) -> None:
        self.registry = registry
        self.store = store or EventStore()
        self.metrics = metrics or BridgeMetrics()
        self._handlers: dict[tuple[str, str], Handler] = {}

    def register(self, message_type: str, consumer: str, handler: Handler) -> None:
        self.registry.model(message_type)
        self._handlers[(message_type, consumer)] = handler

    def dispatch(self, envelope: BridgeEnvelope) -> Any:
        started = time.perf_counter()
        report = self.registry.validate(envelope.message_type, envelope.payload)
        if not report.valid:
            self.metrics.observe("validation_failures")
            raise ValueError(report.model_dump_json())
        if not self.store.append(envelope, status="accepted"):
            self.metrics.observe("duplicates")
            return {"status": "duplicate", "message_id": envelope.message_id}
        handler = self._handlers.get((envelope.message_type, envelope.consumer))
        if handler is None:
            self.store.mark(envelope.message_id, "dead_letter")
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
        return DeliveryReceipt(envelope.message_id, envelope.idempotency_key, topic_for(envelope), "", not accepted, None)

    def replay(self, topic: str | None = None, after: int = 0):
        return self.store.replay(topic, after)
