from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .contracts import BridgeEnvelope
from .deadletter import DeadLetter, DeadLetterQueue
from .protocol import DeliveryError, DeliveryReceipt, topic_for
from .reliability import DeliveryAttempt, RetryPolicy
from .store import EventStore


class AsyncTransport(Protocol):
    async def publish(self, envelope: BridgeEnvelope) -> DeliveryReceipt: ...
    async def subscribe(self, topic: str) -> AsyncIterator[BridgeEnvelope]: ...


@dataclass(frozen=True)
class TransportHealth:
    name: str
    healthy: bool
    detail: str | None = None


class InMemoryTransport:
    """Reference transport for deterministic local integration tests."""

    def __init__(self) -> None:
        self._queues: dict[str, asyncio.Queue[BridgeEnvelope]] = defaultdict(asyncio.Queue)
        self._sequence = 0
        self._seen: set[str] = set()

    async def publish(self, envelope: BridgeEnvelope) -> DeliveryReceipt:
        if envelope.idempotency_key in self._seen:
            return DeliveryReceipt(
                envelope.message_id,
                envelope.idempotency_key,
                topic_for(envelope),
                datetime.now(timezone.utc).isoformat(),
                True,
                None,
            )
        self._seen.add(envelope.idempotency_key)
        self._sequence += 1
        await self._queues[topic_for(envelope)].put(envelope)
        return DeliveryReceipt(
            envelope.message_id,
            envelope.idempotency_key,
            topic_for(envelope),
            datetime.now(timezone.utc).isoformat(),
            False,
            self._sequence,
        )

    async def subscribe(self, topic: str) -> AsyncIterator[BridgeEnvelope]:
        queue = self._queues[topic]
        while True:
            yield await queue.get()


class CallbackTransport:
    """Transport adapter around an injected async publisher; useful for brokers/SDKs."""

    def __init__(self, publish_fn: Callable[[str, bytes], Awaitable[Any]]) -> None:
        self.publish_fn = publish_fn

    async def publish(self, envelope: BridgeEnvelope) -> DeliveryReceipt:
        from .codec import EnvelopeCodec

        result = await self.publish_fn(topic_for(envelope), EnvelopeCodec.encode(envelope))
        sequence = getattr(result, "sequence", None)
        return DeliveryReceipt(
            envelope.message_id,
            envelope.idempotency_key,
            topic_for(envelope),
            datetime.now(timezone.utc).isoformat(),
            bool(getattr(result, "duplicate", False)),
            sequence,
        )

    async def subscribe(self, topic: str) -> AsyncIterator[BridgeEnvelope]:
        raise NotImplementedError("callback transports must provide broker-specific subscription adapters")


class HttpTransport:
    """Dependency-free HTTP publisher; HTTP is deliberately kept outside the core contract layer."""

    def __init__(self, endpoint: str, timeout_seconds: float = 10.0) -> None:
        if not endpoint.startswith(("http://", "https://")):
            raise ValueError("endpoint must use http:// or https://")
        self.endpoint = endpoint
        self.timeout_seconds = timeout_seconds

    def _post(self, body: bytes) -> bytes:
        request = Request(
            self.endpoint,
            data=body,
            method="POST",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                return response.read()
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            raise DeliveryError(str(exc)) from exc

    async def publish(self, envelope: BridgeEnvelope) -> DeliveryReceipt:
        from .codec import EnvelopeCodec

        await asyncio.to_thread(self._post, EnvelopeCodec.encode(envelope))
        return DeliveryReceipt(
            envelope.message_id,
            envelope.idempotency_key,
            topic_for(envelope),
            datetime.now(timezone.utc).isoformat(),
        )

    async def subscribe(self, topic: str) -> AsyncIterator[BridgeEnvelope]:
        raise NotImplementedError("HTTP transport does not provide durable subscription semantics")


class NatsTransport(CallbackTransport):
    """Thin optional NATS adapter around an injected async client."""

    def __init__(self, client: Any) -> None:
        async def publish(topic: str, body: bytes) -> Any:
            return await client.publish(topic, body)

        super().__init__(publish)
        self.client = client


class KafkaTransport(CallbackTransport):
    """Thin optional Kafka adapter around an injected async-capable producer."""

    def __init__(self, producer: Any) -> None:
        async def publish(topic: str, body: bytes) -> Any:
            result = producer.send(topic, body)
            if hasattr(result, "get"):
                return await asyncio.to_thread(result.get)
            if hasattr(result, "__await__"):
                return await result
            return result

        super().__init__(publish)
        self.producer = producer


class DurableTransportAdapter:
    """Persist-before-publish adapter with bounded retries and optional DLQ."""

    def __init__(
        self,
        store: EventStore,
        transport: AsyncTransport,
        retry_policy: RetryPolicy | None = None,
        dead_letter_queue: DeadLetterQueue | None = None,
    ) -> None:
        self.store = store
        self.transport = transport
        self.retry_policy = retry_policy or RetryPolicy()
        self.dead_letter_queue = dead_letter_queue or DeadLetterQueue()

    async def publish(self, envelope: BridgeEnvelope) -> DeliveryReceipt:
        self.store.append(envelope, status="outbox")
        try:
            receipt = await self.transport.publish(envelope)
        except Exception as exc:  # noqa: BLE001 - adapter boundary must capture arbitrary client failures
            self.store.mark(envelope.message_id, "failed")
            raise DeliveryError(str(exc)) from exc
        self.store.mark(envelope.message_id, "published")
        self.store.record_attempt(DeliveryAttempt(envelope.message_id, 1, True))
        return receipt

    async def publish_with_retry(self, envelope: BridgeEnvelope) -> DeliveryReceipt:
        self.store.append(envelope, status="outbox")
        for attempt in range(1, self.retry_policy.max_attempts + 1):
            try:
                receipt = await self.transport.publish(envelope)
            except Exception as exc:  # noqa: BLE001 - adapter boundary must capture arbitrary client failures
                retry_at = (
                    self.retry_policy.next_retry_at(attempt, key=envelope.idempotency_key)
                    if attempt < self.retry_policy.max_attempts
                    else None
                )
                self.store.record_attempt(
                    DeliveryAttempt(envelope.message_id, attempt, False, str(exc), next_retry_at=retry_at)
                )
                if retry_at is None:
                    self.store.mark(envelope.message_id, "deadletter")
                    self.dead_letter_queue.put(
                        DeadLetter(
                            envelope=envelope,
                            reason=str(exc),
                            attempts=tuple(self.store.attempts(envelope.message_id)),
                        )
                    )
                    raise DeliveryError(str(exc)) from exc
                self.store.mark(envelope.message_id, "retry")
                await asyncio.sleep(self.retry_policy.delay(attempt, key=envelope.idempotency_key))
                continue
            self.store.record_attempt(DeliveryAttempt(envelope.message_id, attempt, True))
            self.store.mark(envelope.message_id, "published")
            return receipt
        raise DeliveryError("retry policy exhausted")

    def replay_outbox(self, limit: int = 100) -> list[BridgeEnvelope]:
        return self.store.pending_outbox(limit=limit)
