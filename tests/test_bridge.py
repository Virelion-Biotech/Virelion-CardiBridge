from __future__ import annotations

import asyncio

from cardibridge.builtin import default_registry
from cardibridge.contracts import AgentChallenge, BridgeEnvelope, TraceContext
from cardibridge.production import ProductionRouter
from cardibridge.protocol import content_hash, envelope_digest, topic_for
from cardibridge.store import EventStore
from cardibridge.transport import InMemoryTransport
from cardibridge.compatibility import CompatibilityManager


def challenge():
    trace = TraceContext(source="CardiAgent")
    return AgentChallenge(challenge_type="stress", population=[{"cell": "cardiomyocyte"}], intended_task="detect", trace=trace)


def envelope():
    c = challenge()
    return BridgeEnvelope(message_type="agent.challenge", producer="CardiAgent", consumer="CardiVex", idempotency_key="case-1", payload=c.model_dump(mode="json"), trace=c.trace)


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


def test_store_replay():
    store = EventStore()
    e = envelope()
    assert store.append(e)
    assert not store.append(e)
    assert store.get_envelope(e.message_id).message_id == e.message_id


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
