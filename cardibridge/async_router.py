from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from .contracts import BridgeEnvelope
from .registry import ContractRegistry

AsyncHandler = Callable[[BridgeEnvelope], Awaitable[Any]]


class AsyncBridgeRouter:
    """Async counterpart of BridgeRouter for queue/network adapters."""

    def __init__(self, registry: ContractRegistry) -> None:
        self.registry = registry
        self._handlers: dict[tuple[str, str], AsyncHandler] = {}
        self._seen: set[str] = set()
        self._lock = asyncio.Lock()

    def register(self, message_type: str, consumer: str, handler: AsyncHandler) -> None:
        self._handlers[(message_type, consumer)] = handler

    async def dispatch(self, envelope: BridgeEnvelope) -> Any:
        async with self._lock:
            if envelope.idempotency_key in self._seen:
                return {"status": "duplicate", "message_id": envelope.message_id}
            report = self.registry.validate(envelope.message_type, envelope.payload)
            if not report.valid:
                raise ValueError(report.model_dump_json())
            handler = self._handlers.get((envelope.message_type, envelope.consumer))
            if handler is None:
                raise LookupError(f"no handler for {envelope.message_type} -> {envelope.consumer}")
            self._seen.add(envelope.idempotency_key)
        return await handler(envelope)
