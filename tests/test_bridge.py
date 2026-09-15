from __future__ import annotations

import asyncio

from cardibridge.builtin import default_registry
from cardibridge.compatibility import CompatibilityManager
from cardibridge.contracts import AgentChallenge, ArtifactRef, BridgeEnvelope, ExecutionContext, TraceContext
from cardibridge.production import ProductionRouter
from cardibridge.protocol import content_hash, envelope_digest, topic_for
from cardibridge.reliability import RetryPolicy
from cardibridge.store import EventStore
from cardibridge.transport import DurableTransportAdapter, InMemoryTransport


def challenge():
    trace = TraceContext(source="CardiAgent")
    return AgentChallenge(
        challenge_type="stress",
        population=[{"cell": "cardiomyocyte"}],
        intended_task="detect",
        trace=trace,
    )


def envelope(key: str = "case-1"):
    c = challenge()
    artifact = ArtifactRef(
        uri="s3://example/input.h5ad",
        producer="CardiAtlas",
        media_type="application/x-hdf5",
    )
    execution = ExecutionContext(workflow_id="wf-1", task_id="task-1")
    return BridgeEnvelope(
        message_type="agent.challenge",
        producer="CardiAgent",
        consumer="CardiVex",
        idempotency_key=key,
        payload=c.model_dump(mode="json"),
        trace=c.trace,
        execution=execution,
        artifact_refs=[artifact],
    )


def test_builtin_contract_and_fingerprint():
    registry = default_registry()
    assert registry.validate("agent.challenge", challenge().model_dump(mode="json")).valid
    assert len(registry.fingerprint("agent.challenge")) == 64


def test_production_router_is_durable_and_idempotent():
    registry = default_registry()
    router = ProductionRouter(registry)
    router.register("agent.challenge", "CardiVex", lambda e: {"ok": True})
    e = envelope()
    assert router.dispatch(e) == {"ok": True}
    assert router.dispatch(e)["status"] == "duplicate"
    assert list(router.replay())


def test_store_replay_and_attempt_history():
    store = EventStore()
    e = envelope()
    assert store.append(e)
    assert not store.append(e)
    assert store.get_envelope(e.message_id).message_id == e.message_id
    assert store.status(e.message_id) == "accepted"
    store.record_attempt(__import__("cardibridge").DeliveryAttempt(e.message_id, 1, True))
    assert store.attempts(e.message_id)[0].success


def test_lineage_store_round_trip():
    from cardibridge import LineageEvent

    store = EventStore()
    event = LineageEvent(
        run_id="run-1",
        job_namespace="virelion",
        job_name="CardiLearn",
        event_type="COMPLETE",
        producer="CardiLearn",
        outputs=[envelope().artifact_refs[0]],
    )
    assert store.append_lineage(event)
    assert not store.append_lineage(event)
    assert list(store.lineage("run-1"))[0].event_id == event.event_id


def test_protocol_hashes_are_stable():
    e = envelope()
    assert content_hash({"b": 2, "a": 1}) == content_hash({"a": 1, "b": 2})
    assert envelope_digest(e).startswith("sha256:")
    assert topic_for(e) == "virelion.agent.challenge.v1"


def test_compatibility_requires_explicit_migration():
    manager = CompatibilityManager(default_registry())
    assert not manager.check("agent.challenge", "1.0.0", "2.0.0").compatible
    manager.register_migration("agent.challenge", "1.0.0", "2.0.0", lambda p: p)
    assert manager.check("agent.challenge", "1.0.0", "2.0.0").migrated


def test_transport_deduplicates():
    async def run():
        transport = InMemoryTransport()
        e = envelope()
        first = await transport.publish(e)
        second = await transport.publish(e)
        assert not first.duplicate
        assert second.duplicate

    asyncio.run(run())


def test_durable_adapter_records_success():
    async def run():
        store = EventStore()
        transport = DurableTransportAdapter(store, InMemoryTransport(), RetryPolicy(max_attempts=2))
        receipt = await transport.publish(envelope("durable-1"))
        assert not receipt.duplicate
        e = store.get_envelope(receipt.message_id)
        assert e is not None
        assert store.status(receipt.message_id) == "published"
        assert store.attempts(receipt.message_id)[0].success

    asyncio.run(run())
