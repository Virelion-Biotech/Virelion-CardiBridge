from __future__ import annotations

import asyncio
import inspect
from collections import defaultdict
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .contracts import BridgeEnvelope
from .deadletter import DeadLetterQueue
from .protocol import DeliveryError, DeliveryReceipt, topic_for
from .reliability import RetryPolicy
from .store import EventStore


class AsyncTransport(Protocol):
    async def publish(self, envelope: BridgeEnvelope) -> DeliveryReceipt: ...

    def subscribe(self, topic: str) -> AsyncIterator[BridgeEnvelope]: ...


@dataclass(frozen=True)
class TransportHealth:
    name: str
    healthy: bool
    detail: str = ""


class InMemoryTransport:
    """Deterministic in-process transport for tests and local integration."""

    def __init__(self) -> None:
        self.messages: list[BridgeEnvelope] = []
        self.subscribers: dict[str, list[Callable[[BridgeEnvelope], Awaitable[None]]]] = defaultdict(list)
        self._queues: dict[str, list[asyncio.Queue[BridgeEnvelope]]] = defaultdict(list)
        self._seen_keys: set[str] = set()

    async def publish(self, envelope: BridgeEnvelope) -> DeliveryReceipt:
        topic = topic_for(envelope)
        now = datetime.now(timezone.utc).isoformat()
        if envelope.idempotency_key in self._seen_keys:
            return DeliveryReceipt(
                envelope.message_id,
                envelope.idempotency_key,
                topic,
                now,
                duplicate=True,
            )
        self._seen_keys.add(envelope.idempotency_key)
        self.messages.append(envelope)
        for callback in tuple(self.subscribers.get(topic, ())):
            await callback(envelope)
        for queue in tuple(self._queues.get(topic, ())):
            queue.put_nowait(envelope)
        return DeliveryReceipt(envelope.message_id, envelope.idempotency_key, topic, now)

    def subscribe(self, topic: str) -> AsyncIterator[BridgeEnvelope]:
        queue: asyncio.Queue[BridgeEnvelope] = asyncio.Queue()
        self._queues[topic].append(queue)

        async def stream() -> AsyncIterator[BridgeEnvelope]:
            try:
                while True:
                    yield await queue.get()
            finally:
                subscribers = self._queues.get(topic)
                if subscribers is not None:
                    try:
                        subscribers.remove(queue)
                    except ValueError:
                        pass
                    if not subscribers:
                        self._queues.pop(topic, None)

        return stream()

    def on(self, topic: str, callback: Callable[[BridgeEnvelope], Awaitable[None]]) -> None:
        if not topic.strip():
            raise ValueError("topic must not be empty")
        self.subscribers[topic].append(callback)


class CallbackTransport:
    def __init__(self, callback: Callable[[BridgeEnvelope], Awaitable[None]]) -> None:
        self.callback = callback

    async def publish(self, envelope: BridgeEnvelope) -> DeliveryReceipt:
        await self.callback(envelope)
        return DeliveryReceipt(
            envelope.message_id,
            envelope.idempotency_key,
            topic_for(envelope),
            datetime.now(timezone.utc).isoformat(),
        )

    def subscribe(self, topic: str) -> AsyncIterator[BridgeEnvelope]:
        raise NotImplementedError("callback transport is publish-only")


class HttpTransport:
    """Dependency-free HTTP publisher; HTTP stays outside the core contract model."""

    def __init__(
        self,
        endpoint: str,
        timeout_seconds: float = 10.0,
        headers: Mapping[str, str] | None = None,
        gateway_key: str | None = None,
    ) -> None:
        if not endpoint.startswith(("http://", "https://")):
            raise ValueError("endpoint must use http:// or https://")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be > 0")
        self.endpoint = endpoint
        self.timeout_seconds = timeout_seconds
        self.headers = dict(headers or {})
        if gateway_key is not None:
            if not gateway_key:
                raise ValueError("gateway_key must not be empty")
            self.headers["X-Bridge-Key"] = gateway_key

    def _post(self, body: bytes) -> bytes:
        request_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            **self.headers,
        }
        request = Request(self.endpoint, data=body, method="POST", headers=request_headers)
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

    def subscribe(self, topic: str) -> AsyncIterator[BridgeEnvelope]:
        raise NotImplementedError("HTTP transport does not provide durable subscription semantics")


class DurableTransportAdapter:
    def __init__(
        self,
        store: EventStore,
        transport: AsyncTransport,
        retry: RetryPolicy | None = None,
        dead_letter: DeadLetterQueue | None = None,
    ) -> None:
        self.transport = transport
        self.store = store
        self.retry = retry or RetryPolicy()
        self.dead_letter = dead_letter or DeadLetterQueue()

    async def publish(self, envelope: BridgeEnvelope) -> DeliveryReceipt:
        from .reliability import attempt_with_retry

        return await attempt_with_retry(
            self.transport, envelope, self.store, self.retry, self.dead_letter
        )

    def subscribe(self, topic: str) -> AsyncIterator[BridgeEnvelope]:
        return self.transport.subscribe(topic)


class NatsTransport:
    def __init__(self, client: Any) -> None:
        self.client = client

    async def publish(self, envelope: BridgeEnvelope) -> DeliveryReceipt:
        from .codec import EnvelopeCodec

        topic = topic_for(envelope)
        await self.client.publish(topic, EnvelopeCodec.encode(envelope))
        return DeliveryReceipt(
            envelope.message_id,
            envelope.idempotency_key,
            topic,
            datetime.now(timezone.utc).isoformat(),
        )

    async def subscribe(self, topic: str) -> AsyncIterator[BridgeEnvelope]:
        subscription = await self.client.subscribe(topic)
        async for message in subscription:
            from .codec import EnvelopeCodec

            yield EnvelopeCodec.decode(message.data)


class KafkaTransport:
    """Thin adapter around an aiokafka-compatible producer and optional consumer factory."""

    def __init__(
        self,
        producer: Any,
        consumer_factory: Callable[[str], Any] | Callable[[str], Awaitable[Any]] | None = None,
    ) -> None:
        self.producer = producer
        self.consumer_factory = consumer_factory

    async def publish(self, envelope: BridgeEnvelope) -> DeliveryReceipt:
        from .codec import EnvelopeCodec

        topic = topic_for(envelope)
        await self.producer.send_and_wait(topic, EnvelopeCodec.encode(envelope))
        return DeliveryReceipt(
            envelope.message_id,
            envelope.idempotency_key,
            topic,
            datetime.now(timezone.utc).isoformat(),
        )

    async def subscribe(self, topic: str) -> AsyncIterator[BridgeEnvelope]:
        if self.consumer_factory is None:
            raise NotImplementedError("Kafka subscription requires a consumer_factory")
        consumer = self.consumer_factory(topic)
        if inspect.isawaitable(consumer):
            consumer = await consumer
        async for message in consumer:
            from .codec import EnvelopeCodec

            yield EnvelopeCodec.decode(message.value)
