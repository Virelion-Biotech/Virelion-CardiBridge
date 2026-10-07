from __future__ import annotations

import asyncio
import inspect
import json
import math
from collections import defaultdict
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .contracts import BridgeEnvelope
from .deadletter import DeadLetterQueue
from .protocol import DeliveryError, DeliveryReceipt, content_hash, topic_for
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
        self.subscribers: dict[str, list[Callable[[BridgeEnvelope], Awaitable[None]]]] = (
            defaultdict(list)
        )
        self._queues: dict[str, list[asyncio.Queue[BridgeEnvelope]]] = defaultdict(list)
        self._seen_keys: dict[str, str] = {}
        self._publishing: dict[str, asyncio.Task[Any]] = {}

    async def publish(self, envelope: BridgeEnvelope) -> DeliveryReceipt:
        envelope = BridgeEnvelope.model_validate(envelope.model_dump(mode="json"))
        key = envelope.idempotency_key
        identity = content_hash(envelope.model_dump(mode="json"))
        pending = self._publishing.get(key)
        if pending is not None:
            if pending is asyncio.current_task():
                raise DeliveryError("recursive publication of the same idempotency key")
            await asyncio.shield(pending)
            return await self.publish(envelope)
        if key in self._seen_keys:
            if self._seen_keys[key] != identity:
                raise ValueError("idempotency_key is associated with a different envelope")
            return DeliveryReceipt(
                envelope.message_id,
                key,
                topic_for(envelope),
                datetime.now(timezone.utc).isoformat(),
                duplicate=True,
            )

        async def deliver() -> DeliveryReceipt:
            topic = topic_for(envelope)
            try:
                for callback in tuple(self.subscribers.get(topic, ())):
                    await callback(envelope.model_copy(deep=True))
                for queue in tuple(self._queues.get(topic, ())):
                    queue.put_nowait(envelope.model_copy(deep=True))
                self.messages.append(envelope.model_copy(deep=True))
                self._seen_keys[key] = identity
                return DeliveryReceipt(
                    envelope.message_id, key, topic, datetime.now(timezone.utc).isoformat()
                )
            except Exception as exc:
                raise DeliveryError(str(exc)) from exc
            finally:
                self._publishing.pop(key, None)

        task = asyncio.create_task(deliver())
        self._publishing[key] = task
        return await task

    def subscribe(self, topic: str) -> AsyncIterator[BridgeEnvelope]:
        if not topic.strip():
            raise ValueError("topic must not be empty")
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
        try:
            await self.callback(envelope)
        except Exception as exc:
            raise DeliveryError(str(exc)) from exc
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
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
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
                return cast(bytes, response.read())
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            raise DeliveryError(str(exc)) from exc

    async def publish(self, envelope: BridgeEnvelope) -> DeliveryReceipt:
        from .codec import EnvelopeCodec

        body = await asyncio.to_thread(self._post, EnvelopeCodec.encode(envelope))
        try:
            response = json.loads(body)
            if (
                not isinstance(response, dict)
                or response.get("message_id") != envelope.message_id
                or response.get("status") not in {"processed", "duplicate"}
            ):
                raise ValueError("invalid gateway acknowledgement")
        except (ValueError, UnicodeError) as exc:
            raise DeliveryError("invalid gateway acknowledgement") from exc
        return DeliveryReceipt(
            envelope.message_id,
            envelope.idempotency_key,
            topic_for(envelope),
            datetime.now(timezone.utc).isoformat(),
            duplicate=response["status"] == "duplicate",
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
        self.dead_letter = dead_letter if dead_letter is not None else DeadLetterQueue()

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
        try:
            await self.client.publish(topic, EnvelopeCodec.encode(envelope))
            flush = getattr(self.client, "flush", None)
            if flush is not None:
                await flush()
        except Exception as exc:
            raise DeliveryError(str(exc)) from exc
        return DeliveryReceipt(
            envelope.message_id,
            envelope.idempotency_key,
            topic,
            datetime.now(timezone.utc).isoformat(),
        )

    async def subscribe(self, topic: str) -> AsyncIterator[BridgeEnvelope]:
        subscription = await self.client.subscribe(topic)
        try:
            async for message in subscription:
                from .codec import EnvelopeCodec

                yield EnvelopeCodec.decode(message.data)
        finally:
            await subscription.unsubscribe()


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
        try:
            await self.producer.send_and_wait(topic, EnvelopeCodec.encode(envelope))
        except Exception as exc:
            raise DeliveryError(str(exc)) from exc
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
