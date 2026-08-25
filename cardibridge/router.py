from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .contracts import BridgeEnvelope
from .registry import ContractRegistry

Handler = Callable[[BridgeEnvelope], Any]


class BridgeRouter:
    """Deterministic in-process router with validation and idempotency protection."""

    def __init__(self, registry: ContractRegistry) -> None:
        self.registry = registry
        self._handlers: dict[tuple[str, str], Handler] = {}
        self._seen: set[str] = set()

    def register(self, message_type: str, consumer: str, handler: Handler) -> None:
        self._handlers[(message_type, consumer)] = handler

    def dispatch(self, envelope: BridgeEnvelope) -> Any:
        if envelope.idempotency_key in self._seen:
            return {"status": "duplicate", "message_id": envelope.message_id}
        report = self.registry.validate(envelope.message_type, envelope.payload)
        if not report.valid:
            raise ValueError(report.model_dump_json())
        handler = self._handlers.get((envelope.message_type, envelope.consumer))
        if handler is None:
            raise LookupError(
                f"no handler for {envelope.message_type!r} -> {envelope.consumer!r}"
            )
        self._seen.add(envelope.idempotency_key)
        return handler(envelope)

    @property
    def processed_keys(self) -> frozenset[str]:
        return frozenset(self._seen)
