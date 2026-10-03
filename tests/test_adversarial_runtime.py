"""Adversarial runtime scrub ported to pytest (concurrency, persistence, gateway, fuzz)."""
from __future__ import annotations

import asyncio
import concurrent.futures
import os
import tempfile
import threading
import time
from datetime import datetime, timezone

import pytest

from cardibridge import AgentChallenge, BridgeEnvelope, TraceContext
from cardibridge.async_router import AsyncBridgeRouter
from cardibridge.batch import partition_payload, validate_batch
from cardibridge.defaults import default_registry
from cardibridge.gateway import create_app
from cardibridge.production import ProductionRouter
from cardibridge.protocol import canonical_json, content_hash
from cardibridge.reliability import DeliveryAttempt
from cardibridge.store import EventStore


def _env(key: str = "runtime-test") -> BridgeEnvelope:
    trace = TraceContext(source="pytest-scrub")
    payload = AgentChallenge(
        challenge_type="integration",
        population=[{"cell": "cardiomyocyte"}],
        intended_task="runtime scrub",
        trace=trace,
    ).model_dump(mode="json")
    return BridgeEnvelope(
        message_type="agent.challenge",
        producer="pytest",
        consumer="worker",
        idempotency_key=key,
        payload=payload,
        trace=trace,
        timestamp=datetime.now(timezone.utc),
    )


@pytest.fixture(scope="module")
def reg():
    return default_registry()


def test_sync_concurrent_exactly_once(reg):
    router = ProductionRouter(reg, EventStore())
    calls = 0
    lock = threading.Lock()

    def handler(_):
        nonlocal calls
        with lock:
            calls += 1
        time.sleep(0.01)
        return "ok"

    router.register("agent.challenge", "worker", handler)
    message = _env("same-concurrent-key")
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as ex:
        results = list(ex.map(lambda _: router.dispatch(message), range(100)))
    processed = [r for r in results if r == "ok"]
    duplicates = [r for r in results if isinstance(r, dict) and r.get("status") == "duplicate"]
    assert len(processed) == 1
    assert len(duplicates) == 99
    assert calls == 1


def test_async_concurrency_and_retry(reg):
    async def run():
        router = AsyncBridgeRouter(reg)
        calls = 0
        lock = asyncio.Lock()

        async def handler(_):
            nonlocal calls
            async with lock:
                calls += 1
            await asyncio.sleep(0.01)
            return "ok"

        router.register("agent.challenge", "worker", handler)
        msg = _env("async-same-key")
        results = await asyncio.gather(*[router.dispatch(msg) for _ in range(100)])
        assert sum(r == "ok" for r in results) == 1
        assert sum(isinstance(r, dict) and r.get("status") == "duplicate" for r in results) == 99
        assert calls == 1

        fail_router = AsyncBridgeRouter(reg)
        attempts = 0

        async def flaky(_):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("intentional first failure")
            return "recovered"

        fail_router.register("agent.challenge", "worker", flaky)
        msg2 = _env("async-retry-key")
        with pytest.raises(RuntimeError):
            await fail_router.dispatch(msg2)
        assert await fail_router.dispatch(msg2) == "recovered"
        assert attempts == 2

    asyncio.run(run())


def test_sqlite_persistence_reopen_replay_attempt_uniqueness():
    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, "events.sqlite3")
        s = EventStore(db)
        m = _env("persistent-key")
        assert s.append(m, status="retry") is True
        s.record_attempt(
            DeliveryAttempt(message_id=m.message_id, attempt=1, success=False, error="retry")
        )
        s.close()

        s2 = EventStore(db)
        assert s2.status_by_key("persistent-key") == "retry"
        assert s2.get("persistent-key") is not None
        attempts = s2.attempts(m.message_id)
        assert len(attempts) == 1 and attempts[0].attempt == 1
        with pytest.raises(Exception) as excinfo:
            s2.record_attempt(
                DeliveryAttempt(message_id=m.message_id, attempt=1, success=False, error="retry")
            )
        msg = str(excinfo.value).lower()
        assert "already recorded" in msg or "unique" in msg
        replayed = list(s2.replay(limit=10))
        assert len(replayed) == 1 and replayed[0][1].message_id == m.message_id
        s2.close()


def test_gateway_duplicate_and_catalog(reg):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    r = ProductionRouter(reg, EventStore())
    r.register("agent.challenge", "worker", lambda _: {"ok": True})
    client = TestClient(create_app(registry=reg, router=r))
    body = _env("http-duplicate").model_dump(mode="json")
    a = client.post("/v1/messages", json=body)
    b = client.post("/v1/messages", json=body)
    assert a.status_code == 200 and a.json()["status"] == "processed"
    assert b.status_code == 200 and b.json()["status"] == "duplicate"
    assert client.get("/health").status_code == 200
    assert client.get("/ready").status_code == 200
    assert client.get("/v1/contracts").status_code == 200
    asyncapi = client.get("/v1/asyncapi")
    assert asyncapi.status_code == 200
    assert asyncapi.json()["asyncapi"] == "3.0.0"


def test_canonical_and_nonfinite():
    objects = [
        {"b": 2, "a": 1},
        {"unicode": "βeta", "nested": {"z": [3, 2, 1], "a": True}},
        {"zero": 0, "negative": -7, "float": 0.125},
    ]
    for obj in objects:
        raw = canonical_json(obj)
        assert isinstance(raw, bytes)
        assert raw == canonical_json(dict(reversed(list(obj.items()))))
        assert content_hash(obj).startswith("sha256:")
    for bad in [float("nan"), float("inf"), float("-inf")]:
        with pytest.raises((ValueError, TypeError)):
            canonical_json({"x": bad})


def test_batch_and_partition():
    m1, m2 = _env("batch-1"), _env("batch-2")
    result = validate_batch([m1, m2])
    assert result.accepted == 2 and result.rejected == 0
    dup = validate_batch([m1, m1])
    assert dup.accepted == 1 and dup.rejected == 1
    with pytest.raises(ValueError):
        validate_batch([m1, m2], max_size=1)
    for n in range(1, 50):
        payload = {f"k{i}": i for i in range(n)}
        routing, body = partition_payload(payload, [f"k{i}" for i in range(0, n, 3)])
        assert set(routing) | set(body) == set(payload)
        assert set(routing) & set(body) == set()


def test_hypothesis_canonical():
    pytest.importorskip("hypothesis")
    from hypothesis import given, settings
    from hypothesis import strategies as st

    json_scalars = st.one_of(st.none(), st.booleans(), st.integers(-10**6, 10**6), st.text(max_size=20))
    json_values = st.recursive(
        json_scalars,
        lambda children: st.one_of(
            st.lists(children, max_size=6),
            st.dictionaries(st.text(max_size=12), children, max_size=6),
        ),
        max_leaves=30,
    )

    @settings(max_examples=100, deadline=None)
    @given(json_values)
    def fuzz(value):
        raw = canonical_json(value)
        assert isinstance(raw, bytes)
        assert raw == canonical_json(value)
        assert content_hash(value) == content_hash(value)

    fuzz()
