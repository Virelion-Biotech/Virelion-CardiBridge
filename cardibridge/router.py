from __future__ import annotations

from collections.abc import Callable
from threading import Lock
from typing import Any

from .contracts import BridgeEnvelope
from .registry import ContractRegistry

Handler = Callable[[BridgeEnvelope], Any]


class BridgeRouter:
    """Deterministic in-process router with thread-safe, failure-safe idempotency."""

    def __init__(self, registry: ContractRegistry) -> None:
        self.registry = registry
        self._handlers: dict[tuple[str, str], Handler] = {}
        self._seen: set[str] = set()
        self._processing: set[str] = set()
        self._lock = Lock()

    def register(self, message_type: str, consumer: str, handler: Handler) -> None:
        self.registry.model(message_type)
        if not consumer.strip():
            raise ValueError("consumer must not be empty")
        with self._lock:
            self._handlers[(message_type, consumer)] = handler

    def dispatch(self, envelope: BridgeEnvelope) -> Any:
        key = envelope.idempotency_key
        with self._lock:
            if key in self._seen or key in self._processing:
                return {"status": "duplicate", "message_id": envelope.message_id}
            handler = self._handlers.get((envelope.message_type, envelope.consumer))
            if handler is None:
                raise LookupError(
                    f"no handler for {envelope.message_type!r} -> {envelope.consumer!r}"
                )
            self._processing.add(key)

        try:
            report = self.registry.validate(envelope.message_type, envelope.payload)
            if not report.valid:
                raise ValueError(report.model_dump_json())
            return_value = handler(envelope)
        except Exception:
            with self._lock:
                self._processing.discard(key)
            raise
        else:
            with self._lock:
                self._processing.discard(key)
                self._seen.add(key)
            return return_value

    @property
    def processed_keys(self) -> frozenset[str]:
        with self._lock:
            return frozenset(self._seen)
