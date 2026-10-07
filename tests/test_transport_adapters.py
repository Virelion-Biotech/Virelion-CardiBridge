from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from test_delivery_regressions import message

from cardibridge import DeliveryError
from cardibridge.codec import EnvelopeCodec
from cardibridge.transport import (
    CallbackTransport,
    InMemoryTransport,
    KafkaTransport,
    NatsTransport,
)


def test_memory_concurrency_subscriptions_and_conflicts():
    async def run():
        transport = InMemoryTransport()
        item = message()
        stream = transport.subscribe("virelion.agent.challenge.v1")
        received = asyncio.create_task(anext(stream))
        first, second = await asyncio.gather(transport.publish(item), transport.publish(item))
        assert not first.duplicate and second.duplicate
        assert await received == item
        await stream.aclose()
        assert not transport._queues
        changed = item.model_copy(deep=True)
        changed.payload["intended_task"] = "changed"
        with pytest.raises(ValueError):
            await transport.publish(changed)

    asyncio.run(run())


def test_recursive_memory_publication_fails_without_deadlock():
    async def run():
        transport = InMemoryTransport()
        item = message()

        async def recurse(_):
            await transport.publish(item)

        transport.on("virelion.agent.challenge.v1", recurse)
        with pytest.raises(DeliveryError):
            await asyncio.wait_for(transport.publish(item), 2)
        assert not transport.messages

    asyncio.run(run())


def test_callback_delivery_and_failure():
    async def run():
        received = []

        async def callback(item):
            received.append(item)

        transport = CallbackTransport(callback)
        item = message()
        assert (await transport.publish(item)).message_id == item.message_id
        assert received == [item]

        async def failed(_):
            raise RuntimeError("offline")

        with pytest.raises(DeliveryError):
            await CallbackTransport(failed).publish(item)
        with pytest.raises(NotImplementedError):
            transport.subscribe("topic")

    asyncio.run(run())


def test_nats_adapter_wire_roundtrip_and_failure():
    async def run():
        class Subscription:
            closed = False

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self.closed:
                    raise StopAsyncIteration
                self.closed = True
                return SimpleNamespace(data=client.body)

            async def unsubscribe(self):
                self.unsubscribed = True

        class Client:
            body = b""
            topic = ""
            fail = False
            flushed = False
            subscription = Subscription()

            async def publish(self, topic, body):
                if self.fail:
                    raise RuntimeError("offline")
                self.topic, self.body = topic, body

            async def flush(self):
                self.flushed = True

            async def subscribe(self, topic):
                return self.subscription

        client = Client()
        transport = NatsTransport(client)
        item = message()
        receipt = await transport.publish(item)
        assert receipt.topic == client.topic == "virelion.agent.challenge.v1"
        assert EnvelopeCodec.decode(client.body) == item
        assert [value async for value in transport.subscribe(client.topic)] == [item]
        assert client.flushed and client.subscription.unsubscribed
        client.fail = True
        with pytest.raises(DeliveryError):
            await transport.publish(item)

    asyncio.run(run())


def test_kafka_adapter_acknowledgement_and_async_consumer_factory():
    async def run():
        class Producer:
            fail = False

            async def send_and_wait(self, topic, body):
                if self.fail:
                    raise RuntimeError("offline")
                self.topic, self.body = topic, body

        class Consumer:
            done = False

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self.done:
                    raise StopAsyncIteration
                self.done = True
                return SimpleNamespace(value=producer.body)

        async def factory(topic):
            return Consumer()

        producer = Producer()
        item = message()
        transport = KafkaTransport(producer, factory)
        assert (await transport.publish(item)).topic == "virelion.agent.challenge.v1"
        assert [value async for value in transport.subscribe(producer.topic)] == [item]
        producer.fail = True
        with pytest.raises(DeliveryError):
            await transport.publish(item)
        with pytest.raises(NotImplementedError):
            await anext(KafkaTransport(producer).subscribe("topic"))

    asyncio.run(run())
