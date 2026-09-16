from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from .contracts import BridgeEnvelope
from .registry import ContractRegistry

AsyncHandler = Callable[[BridgeEnvelope], Awaitable[Any]]


class AsyncBridgeRouter:
    """Async router with serialized idempotency claims and retry-safe failures."""

    def __init__(self, registry: ContractRegistry) -> None:
        self.registry = registry
        self._handlers: dict[tuple[str, str], AsyncHandler] = {}
        self._seen: set[str] = set()
        self._processing: set[str] = set()
        self._lock = asyncio.Lock()

    def register(self, message_type: str, consumer: str, handler: AsyncHandler) -> None:
        self.registry.model(message_type)
        if not consumer.strip():
            raise ValueError("consumer must not be empty")
        self._handlers[(message_type, consumer)] = handler

    async def dispatch(self, envelope: BridgeEnvelope) -> Any:
        key = envelope.idempotency_key
        async with self._lock:
            if key in self._seen or key in self._processing:
                return {"status": "duplicate", "message_id": envelope.message_id}
            report = self.registry.validate(envelope.message_type, envelope.payload)
            if not report.valid:
                raise ValueError(report.model_dump_json())
            handler = self._handlers.get((envelope.message_type, envelope.consumer))
            if handler is None:
                raise LookupError(
                    f"no handler for {envelope.message_type!r} -> {envelope.consumer!r}"
                )
            self._processing.add(key)

        try:
            result = await handler(envelope)
        except Exception:
            async with self._lock:
                self._processing.discard(key)
            raise
        else:
            async with self._lock:
                self._processing.discard(key)
                self._seen.add(key)
            return result

    @property
    def processed_keys(self) -> frozenset[str]:
        return frozenset(self._seen)
