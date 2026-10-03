from __future__ import annotations

from datetime import datetime, timezone

from fastapi.testclient import TestClient

from cardibridge import AgentChallenge, BridgeEnvelope, TraceContext
from cardibridge.defaults import default_registry
from cardibridge.gateway import create_app
from cardibridge.production import ProductionRouter
from cardibridge.store import EventStore


def make_envelope(key: str = "gateway-test") -> BridgeEnvelope:
    trace = TraceContext(source="gateway-test")
    payload = AgentChallenge(
        challenge_type="integration",
        population=[{"cell": "cardiomyocyte"}],
        intended_task="gateway test",
        trace=trace,
    ).model_dump(mode="json")
    return BridgeEnvelope(
        message_type="agent.challenge",
        producer="gateway-test",
        consumer="gateway-worker",
        idempotency_key=key,
        payload=payload,
        trace=trace,
        timestamp=datetime.now(timezone.utc),
    )


def test_gateway_preserves_duplicate_status() -> None:
    registry = default_registry()
    router = ProductionRouter(registry, EventStore())
    router.register("agent.challenge", "gateway-worker", lambda _: {"ok": True})
    client = TestClient(create_app(registry=registry, router=router))
    payload = make_envelope().model_dump(mode="json")

    first = client.post("/v1/messages", json=payload)
    second = client.post("/v1/messages", json=payload)

    assert first.status_code == 200
    assert first.json()["status"] == "processed"
    assert second.status_code == 200
    assert second.json()["status"] == "duplicate"
    assert second.json()["result"]["status"] == "duplicate"


def test_gateway_returns_503_when_handler_is_missing() -> None:
    registry = default_registry()
    router = ProductionRouter(registry, EventStore())
    client = TestClient(create_app(registry=registry, router=router))

    response = client.post("/v1/messages", json=make_envelope("missing-handler").model_dump(mode="json"))

    assert response.status_code == 503
    assert router.store.status_by_key("missing-handler") == "handler_failed"
