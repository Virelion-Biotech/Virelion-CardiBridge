from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator
from typing import Protocol

from .contracts import BridgeEnvelope
from .protocol import DeliveryError, DeliveryReceipt, topic_for
from .store import EventStore


class AsyncTransport(Protocol):
    async def publish(self, envelope: BridgeEnvelope) -> DeliveryReceipt: ...
    async def subscribe(self, topic: str) -> AsyncIterator[BridgeEnvelope]: ...


class InMemoryTransport:
    """Reference transport for deterministic local integration tests."""

    def __init__(self) -> None:
        self._queues: dict[str, asyncio.Queue[BridgeEnvelope]] = defaultdict(asyncio.Queue)
        self._sequence = 0
        self._seen: set[str] = set()

    async def publish(self, envelope: BridgeEnvelope) -> DeliveryReceipt:
        if envelope.idempotency_key in self._seen:
            return DeliveryReceipt(envelope.message_id, envelope.idempotency_key, topic_for(envelope), "", True, None)
        self._seen.add(envelope.idempotency_key)
        self._sequence += 1
        await self._queues[topic_for(envelope)].put(envelope)
        return DeliveryReceipt(envelope.message_id, envelope.idempotency_key, topic_for(envelope), "", False, self._sequence)

    async def subscribe(self, topic: str) -> AsyncIterator[BridgeEnvelope]:
        queue = self._queues[topic]
        while True:
            yield await queue.get()


class DurableTransportAdapter:
    """Persist-before-publish adapter implementing an application-level outbox."""

    def __init__(self, store: EventStore, transport: AsyncTransport) -> None:
        self.store = store
        self.transport = transport

    async def publish(self, envelope: BridgeEnvelope) -> DeliveryReceipt:
        self.store.append(envelope, status="outbox")
        try:
            receipt = await self.transport.publish(envelope)
        except Exception as exc:
            self.store.mark(envelope.message_id, "failed")
            raise DeliveryError(str(exc)) from exc
        self.store.mark(envelope.message_id, "published")
        return receipt
