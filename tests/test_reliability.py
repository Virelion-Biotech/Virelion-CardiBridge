from __future__ import annotations

import asyncio

import pytest

from cardibridge import DeliveryError, EventStore, RetryPolicy
from cardibridge.contracts import AgentChallenge, BridgeEnvelope, TraceContext
from cardibridge.deadletter import DeadLetterQueue
from cardibridge.reliability import attempt_with_retry
from cardibridge.transport import InMemoryTransport


def envelope(key: str) -> BridgeEnvelope:
    trace = TraceContext(source="test")
    payload = AgentChallenge(
        challenge_type="retry",
        population=[{"cell": "cardiomyocyte"}],
        intended_task="exercise retry path",
        trace=trace,
    ).model_dump(mode="json")
    return BridgeEnvelope(
        message_type="agent.challenge",
        producer="test",
        consumer="test",
        idempotency_key=key,
        payload=payload,
        trace=trace,
    )


class FlakyTransport(InMemoryTransport):
    def __init__(self, failures: int) -> None:
        super().__init__()
        self.failures = failures
        self.calls = 0

    async def publish(self, message: BridgeEnvelope):
        self.calls += 1
        if self.calls <= self.failures:
            raise RuntimeError(f"transient-{self.calls}")
        return await super().publish(message)


def test_retry_records_failures_then_success() -> None:
    async def run() -> None:
        store = EventStore()
        transport = FlakyTransport(failures=2)
        receipt = await attempt_with_retry(
            transport,
            envelope("retry-success"),
            store,
            RetryPolicy(max_attempts=3, base_delay_seconds=0, jitter=0),
            DeadLetterQueue(),
        )
        assert not receipt.duplicate
        assert transport.calls == 3
        attempts = store.attempts(receipt.message_id)
        assert [attempt.success for attempt in attempts] == [False, False, True]
        assert store.status(receipt.message_id) == "published"

    asyncio.run(run())


def test_retry_exhaustion_enters_dead_letter() -> None:
    async def run() -> None:
        store = EventStore()
        transport = FlakyTransport(failures=5)
        dlq = DeadLetterQueue()
        message = envelope("retry-failed")
        with pytest.raises(DeliveryError):
            await attempt_with_retry(
                transport,
                message,
                store,
                RetryPolicy(max_attempts=2, base_delay_seconds=0, jitter=0),
                dlq,
            )
        assert transport.calls == 2
        assert store.status(message.message_id) == "dead_letter"
        item = dlq.get(message.message_id)
        assert item is not None
        assert len(item.attempts) == 2

    asyncio.run(run())
