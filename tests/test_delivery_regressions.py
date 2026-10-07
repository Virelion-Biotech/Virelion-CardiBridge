from __future__ import annotations

import asyncio
import json

import pytest

from cardibridge import (
    AgentChallenge,
    BridgeEnvelope,
    DeliveryError,
    EventStore,
    RetryPolicy,
    TraceContext,
)
from cardibridge.async_router import AsyncBridgeRouter
from cardibridge.codec import EnvelopeCodec
from cardibridge.compatibility import CompatibilityManager
from cardibridge.contracts import LineageEvent
from cardibridge.deadletter import DeadLetterQueue
from cardibridge.defaults import default_registry
from cardibridge.migrations import Migration, MigrationRegistry
from cardibridge.production import ProductionRouter
from cardibridge.router import BridgeRouter
from cardibridge.transport import DurableTransportAdapter, InMemoryTransport
from cardibridge.wire import verify


def message(key: str = "fixed") -> BridgeEnvelope:
    trace = TraceContext(source="regression")
    payload = AgentChallenge(
        challenge_type="mi", population=[{"sample": "S1"}], intended_task="classify", trace=trace
    )
    return BridgeEnvelope(
        message_type="agent.challenge",
        producer="test",
        consumer="worker",
        idempotency_key=key,
        payload=payload.model_dump(mode="json"),
        trace=trace,
    )


def test_callback_failure_is_retryable_in_memory():
    async def run():
        transport = InMemoryTransport()
        calls = 0

        async def callback(_):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("temporary")

        transport.on("virelion.agent.challenge.v1", callback)
        item = message()
        with pytest.raises(DeliveryError):
            await transport.publish(item)
        receipt = await transport.publish(item)
        assert calls == 2 and not receipt.duplicate
        assert len(transport.messages) == 1

    asyncio.run(run())


@pytest.mark.parametrize("router_type", [BridgeRouter, AsyncBridgeRouter])
def test_in_memory_router_rejects_conflicting_duplicate(router_type):
    router = router_type(default_registry())
    first = message()
    changed = first.model_copy(deep=True)
    changed.payload["intended_task"] = "changed"
    if router_type is BridgeRouter:
        router.register("agent.challenge", "worker", lambda _: "ok")
        router.dispatch(first)
        with pytest.raises(ValueError):
            router.dispatch(changed)
    else:

        async def run():
            async def handle(_):
                return "ok"

            router.register("agent.challenge", "worker", handle)
            await router.dispatch(first)
            with pytest.raises(ValueError):
                await router.dispatch(changed)

        asyncio.run(run())


def test_async_cancelled_handler_releases_claim():
    async def run():
        router = AsyncBridgeRouter(default_registry())
        entered = asyncio.Event()
        calls = 0

        async def handle(_):
            nonlocal calls
            calls += 1
            if calls == 1:
                entered.set()
                await asyncio.Event().wait()
            return "ok"

        router.register("agent.challenge", "worker", handle)
        item = message()
        task = asyncio.create_task(router.dispatch(item))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await router.dispatch(item) == "ok"

    asyncio.run(run())


def test_durable_concurrent_publication_has_one_owner():
    # Use the exact same envelope identity, not two random messages.
    async def exact():
        class Blocking(InMemoryTransport):
            calls = 0

            async def publish(self, envelope):
                self.calls += 1
                await asyncio.sleep(0.02)
                return await super().publish(envelope)

        store = EventStore()
        transport = Blocking()
        adapter = DurableTransportAdapter(store, transport, RetryPolicy(base_delay_seconds=0))
        item = message()
        results = await asyncio.gather(adapter.publish(item), adapter.publish(item))
        assert transport.calls == 1
        assert len(store.attempts(item.message_id)) == 1
        assert sum(r.duplicate for r in results) == 1
        store.close()

    asyncio.run(exact())


def test_event_store_rejects_tampered_replay():
    store = EventStore()
    item = message()
    store.append(item)
    raw = item.model_dump(mode="json")
    raw["payload"]["intended_task"] = "forged"
    store.db.execute("UPDATE events SET payload=?", (json.dumps(raw),))
    store.db.commit()
    with pytest.raises(ValueError):
        list(store.replay())
    with pytest.raises(ValueError):
        store.get(item.idempotency_key)
    with pytest.raises(ValueError):
        store.get_envelope(item.message_id)
    store.close()


def test_lineage_identity_conflicts_fail_closed():
    store = EventStore()
    first = LineageEvent(
        run_id="run", job_namespace="test", job_name="work", event_type="START", producer="test"
    )
    store.append_lineage(first)
    changed = first.model_copy(update={"job_name": "forged"})
    with pytest.raises(ValueError):
        store.append_lineage(changed)
    store.close()


def test_migration_cannot_modify_original_nested_payload():
    migrations = MigrationRegistry()

    def mutate(payload):
        payload["nested"]["value"] = 2
        return payload

    migrations.register(Migration("test", "1.0.0", "2.0.0", mutate))
    source = {"nested": {"value": 1}}
    assert migrations.migrate("test", "1.0.0", "2.0.0", source)["nested"]["value"] == 2
    assert source["nested"]["value"] == 1


def test_compatibility_cannot_claim_unregistered_target_version():
    manager = CompatibilityManager(default_registry())
    assert not manager.check("agent.challenge", "9.0.0", "9.0.0").compatible


@pytest.mark.parametrize("field", ["base_delay_seconds", "max_delay_seconds"])
def test_retry_policy_rejects_nonfinite_delays(field):
    with pytest.raises(ValueError):
        RetryPolicy(**{field: float("nan")})


def test_retry_policy_handles_large_attempt_without_overflow():
    assert RetryPolicy(jitter=0).delay(100000) == 300


def test_codec_rejects_duplicate_json_object_keys():
    encoded = EnvelopeCodec.encode(message()).decode()
    encoded = encoded.replace('"producer":"test"', '"producer":"changed","producer":"test"')
    with pytest.raises(ValueError):
        EnvelopeCodec.decode(encoded)


def test_signature_verification_returns_false_for_nonascii():
    assert verify({"x": 1}, "é", b"secret") is False


def test_supplied_empty_dlq_is_retained():
    dlq = DeadLetterQueue()
    adapter = DurableTransportAdapter(EventStore(), InMemoryTransport(), dead_letter=dlq)
    assert adapter.dead_letter is dlq
    adapter.store.close()


def test_router_rejects_unnegotiated_schema_version():
    router = ProductionRouter(default_registry())
    router.register("agent.challenge", "worker", lambda _: "ok")
    item = message()
    item.trace.schema_version = "9.0.0"
    with pytest.raises(ValueError):
        router.dispatch(item)
    router.store.close()


@pytest.mark.parametrize("boundary", ["codec", "store", "signature", "registry", "memory"])
def test_nested_key_coercion_cannot_lose_scientific_payload(boundary):
    item = message()
    item.payload["population"][0]["nested"] = {1: "first", "1": "second"}
    if boundary == "codec":
        with pytest.raises(ValueError):
            EnvelopeCodec.encode(item)
    elif boundary == "store":
        store = EventStore()
        with pytest.raises(ValueError):
            store.append(item)
        assert not store.list_events()
        store.close()
    elif boundary == "signature":
        from cardibridge.security import sign_envelope

        with pytest.raises(ValueError):
            sign_envelope(item, b"secret")
    elif boundary == "registry":
        assert not default_registry().validate(item.message_type, item.payload).valid
    else:
        with pytest.raises(ValueError):
            asyncio.run(InMemoryTransport().publish(item))


def test_registered_migration_does_not_imply_target_schema_support():
    from cardibridge.protocol import ContractMismatch

    manager = CompatibilityManager(default_registry())
    manager.register_migration("agent.challenge", "1.0.0", "2.0.0", lambda data: data)
    result = manager.check("agent.challenge", "1.0.0", "2.0.0")
    assert result.migrated and not result.compatible
    with pytest.raises(ContractMismatch):
        manager.migrate("agent.challenge", message().payload, "1.0.0", "2.0.0")
