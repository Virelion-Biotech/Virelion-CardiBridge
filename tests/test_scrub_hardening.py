from __future__ import annotations

import asyncio

import pytest

from cardibridge import (
    AgentChallenge,
    BridgeEnvelope,
    BridgeRouter,
    ContractRegistry,
    ExecutionContext,
    TraceContext,
)
from cardibridge.async_router import AsyncBridgeRouter
from cardibridge.observability import BridgeMetrics
from cardibridge.registry import ContractRegistry
from cardibridge.store import EventStore


def envelope(key: str = "k1") -> BridgeEnvelope:
    trace = TraceContext(source="test")
    payload = AgentChallenge(
        challenge_type="scrub",
        population=[{"cell": "cardiomyocyte"}],
        intended_task="test",
        trace=trace,
    ).model_dump(mode="json")
    return BridgeEnvelope(
        message_type="agent.challenge",
        producer="test",
        consumer="worker",
        idempotency_key=key,
        payload=payload,
        trace=trace,
    )


def registry() -> ContractRegistry:
    reg = ContractRegistry()
    reg.register("agent.challenge", AgentChallenge, "1.0.0")
    return reg


def test_sync_router_retries_after_handler_failure() -> None:
    router = BridgeRouter(registry())
    calls = 0

    def handler(_: BridgeEnvelope) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("first failure")
        return "ok"

    router.register("agent.challenge", "worker", handler)
    with pytest.raises(RuntimeError):
        router.dispatch(envelope())
    assert router.dispatch(envelope()) == "ok"
    assert calls == 2


def test_async_router_retries_after_handler_failure() -> None:
    async def run() -> None:
        router = AsyncBridgeRouter(registry())
        calls = 0

        async def handler(_: BridgeEnvelope) -> str:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("first failure")
            return "ok"

        router.register("agent.challenge", "worker", handler)
        with pytest.raises(RuntimeError):
            await router.dispatch(envelope())
        assert await router.dispatch(envelope()) == "ok"
        assert calls == 2

    asyncio.run(run())


def test_store_claim_is_single_winner() -> None:
    store = EventStore()
    message = envelope()
    assert store.append(message, status="accepted")
    assert store.claim(message.idempotency_key, ["accepted"])
    assert not store.claim(message.idempotency_key, ["accepted"])
    assert store.status(message.message_id) == "processing"


def test_registry_rejects_same_version_with_changed_model() -> None:
    class Alternate(AgentChallenge):
        alternate: str = "x"

    registry = ContractRegistry()
    registry.register("agent.challenge", AgentChallenge, "1.0.0")
    with pytest.raises(ValueError):
        registry.register("agent.challenge", Alternate, "1.0.0")


def test_validation_report_serializes_schema_key() -> None:
    report = registry().validate("agent.challenge", {})
    assert not report.valid
    assert report.schema == "agent.challenge"
    assert '"schema":"agent.challenge"' in report.model_dump_json()


def test_metrics_reject_unknown_names_and_bound_latency() -> None:
    metrics = BridgeMetrics(max_latency_samples=2)
    with pytest.raises(ValueError):
        metrics.observe("does_not_exist")
    metrics.latency(0.0)
    metrics.latency(0.0)
    metrics.latency(0.0)
    assert metrics.snapshot()["latency_ms"]["count"] == 2


def test_execution_timestamps_are_consistent() -> None:
    with pytest.raises(ValueError):
        ExecutionContext(
            workflow_id="wf",
            task_id="task",
            started_at="2026-01-02T00:00:00Z",
            completed_at="2026-01-01T00:00:00Z",
        )
