from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import time

import pytest
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator
from test_delivery_regressions import message

from cardibridge import DeliveryError, EventStore, RetryPolicy
from cardibridge.catalog import export_asyncapi
from cardibridge.cli import main
from cardibridge.codec import EnvelopeCodec
from cardibridge.defaults import default_registry
from cardibridge.gateway import create_app
from cardibridge.production import ProductionRouter
from cardibridge.protocol import canonical_json
from cardibridge.reliability import DeliveryAttempt
from cardibridge.transport import DurableTransportAdapter, HttpTransport, InMemoryTransport


def test_crashed_consumer_claim_expires_and_recovers(tmp_path):
    path = tmp_path / "recovery.db"
    item = message()
    with_store = EventStore(path)
    with_store.append(item)
    with_store.close()
    code = (
        "from cardibridge import EventStore; import sys,os; "
        "s=EventStore(sys.argv[1]); "
        'assert s.claim("fixed", {"accepted"},owner="crashed",lease_seconds=0.05); '
        "os._exit(0)"
    )
    subprocess.run([sys.executable, "-c", code, str(path)], check=True)
    time.sleep(0.06)
    store = EventStore(path)
    router = ProductionRouter(default_registry(), store)
    router.register("agent.challenge", "worker", lambda _: {"recovered": True})
    assert router.dispatch(item) == {"recovered": True}
    assert router.dispatch(item)["result"] == {"recovered": True}
    store.close()


def test_claim_ownership_and_renewal(tmp_path):
    store = EventStore(tmp_path / "claims.db")
    other = EventStore(tmp_path / "claims.db")
    item = message()
    store.append(item)
    assert store.claim("fixed", {"accepted"}, owner="first", lease_seconds=0.06)
    with store.lease("fixed", "first", seconds=0.06):
        time.sleep(0.12)
        assert not other.claim("fixed", {"accepted"}, owner="second")
        with pytest.raises(ValueError):
            other.complete("fixed", "wrong", owner="second")
        with pytest.raises(ValueError):
            other.mark(item.message_id, "processed", owner="second")
    time.sleep(0.07)
    assert other.claim("fixed", {"accepted"}, owner="second")
    with pytest.raises(ValueError):
        store.complete("fixed", "stale", owner="first")
    other.complete("fixed", "ok", owner="second")
    store.close()
    other.close()


def test_recover_expired_publication_and_honor_backoff(tmp_path, monkeypatch):
    async def run():
        from datetime import datetime, timedelta, timezone

        store = EventStore(tmp_path / "outbox.db")
        item = message()
        store.append(item, status="outbox")
        assert store.claim(
            "fixed", {"outbox"}, owner="crashed", processing_status="publishing", lease_seconds=0.01
        )
        store.record_attempt(
            DeliveryAttempt(
                item.message_id,
                1,
                False,
                "temporary",
                next_retry_at=datetime.now(timezone.utc) + timedelta(seconds=1),
            ),
            owner="crashed",
        )
        store.db.execute("UPDATE events SET lease_until=0")
        store.db.commit()
        assert len(store.pending_outbox()) == 1
        delays = []

        async def sleep(delay):
            delays.append(delay)

        monkeypatch.setattr("cardibridge.reliability.asyncio.sleep", sleep)
        adapter = DurableTransportAdapter(
            store, InMemoryTransport(), RetryPolicy(base_delay_seconds=0)
        )
        assert not (await adapter.publish(item)).duplicate
        assert delays and 0 < delays[0] <= 1
        assert [a.attempt for a in store.attempts(item.message_id)] == [1, 2]
        store.close()

    asyncio.run(run())


def test_cancelled_publication_can_resume():
    async def run():
        transport = InMemoryTransport()
        entered = asyncio.Event()

        async def blocked(_):
            entered.set()
            await asyncio.Event().wait()

        transport.on("virelion.agent.challenge.v1", blocked)
        store = EventStore()
        adapter = DurableTransportAdapter(store, transport)
        item = message()
        task = asyncio.create_task(adapter.publish(item))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert store.status(item.message_id) == "retry"
        transport.subscribers.clear()
        assert not (await adapter.publish(item)).duplicate
        store.close()

    asyncio.run(run())


def test_consumer_result_integrity():
    store = EventStore()
    item = message()
    store.append(item)
    store.complete("fixed", {"value": 1})
    store.db.execute("UPDATE consumer_results SET payload='{}'")
    store.db.commit()
    with pytest.raises(ValueError):
        store.duplicate_receipt("fixed")
    store.close()


def test_gateway_auth_validation_conflicts_and_replay(monkeypatch):
    monkeypatch.setenv("CARDIBRIDGE_GATEWAY_KEY", "secret")
    router = ProductionRouter(default_registry())
    router.register("agent.challenge", "worker", lambda _: {"ok": True})
    with TestClient(create_app(router=router)) as client:
        item = message()
        raw = item.model_dump(mode="json")
        headers = {"X-Bridge-Key": "secret"}
        assert client.get("/v1/replay").status_code == 401
        assert client.post("/v1/messages", json=raw).status_code == 401
        assert client.get("/v1/replay?after=-1", headers=headers).status_code == 400
        assert (
            client.post("/v1/messages", json=raw, headers=headers).json()["status"] == "processed"
        )
        assert (
            client.post("/v1/messages", json=raw, headers=headers).json()["status"] == "duplicate"
        )
        raw["payload"]["intended_task"] = "changed"
        assert client.post("/v1/messages", json=raw, headers=headers).status_code == 409
        raw["message_type"] = "unknown"
        assert client.post("/v1/messages", json=raw, headers=headers).status_code == 404
        raw["message_type"] = "agent.challenge"
        raw["trace"]["schema_version"] = "9.0.0"
        assert client.post("/v1/validate", json=raw).status_code == 422
        assert client.post("/v1/encode", json=raw, headers=headers).status_code == 422
        raw["producer"] = ""
        assert client.post("/v1/validate", json=raw).status_code == 422
        assert client.post("/v1/encode", json=raw, headers=headers).status_code == 422
    router.store.close()


@pytest.mark.parametrize("response", [b"{}", b"not-json", b"[]", b'{"status":"accepted"}'])
def test_http_rejects_false_acknowledgements(response, monkeypatch):
    transport = HttpTransport("https://example.test/v1/messages")
    monkeypatch.setattr(transport, "_post", lambda _: response)
    with pytest.raises(DeliveryError):
        asyncio.run(transport.publish(message()))


def test_http_preserves_duplicate_receipt(monkeypatch):
    item = message()
    transport = HttpTransport("https://example.test/v1/messages")
    monkeypatch.setattr(
        transport,
        "_post",
        lambda _: json.dumps({"message_id": item.message_id, "status": "duplicate"}).encode(),
    )
    assert asyncio.run(transport.publish(item)).duplicate


def test_asyncapi_every_ref_resolves_and_envelope_validates():
    document = export_asyncapi(default_registry())

    def inspect(value):
        if isinstance(value, dict):
            if "$ref" in value:
                resolved = document
                for part in value["$ref"].removeprefix("#/").split("/"):
                    resolved = resolved[part.replace("~1", "/").replace("~0", "~")]
            for child in value.values():
                inspect(child)
        elif isinstance(value, list):
            for child in value:
                inspect(child)

    inspect(document)
    assert document["channels"]["agent_challenge"]["address"] == "virelion.agent.challenge.v1"
    schema = dict(document["components"]["messages"]["agent_challenge"]["payload"])
    schema["components"] = document["components"]
    validator = Draft202012Validator(schema)
    raw = message().model_dump(mode="json")
    validator.validate(raw)
    raw["payload"]["population"] = []
    assert list(validator.iter_errors(raw))


@pytest.mark.parametrize("wire", ['{"payload":{"x":1e999}}', '{"payload":{"x":NaN}}'])
def test_codec_rejects_nonfinite_wire(wire):
    with pytest.raises(ValueError):
        EnvelopeCodec.decode(wire)


@pytest.mark.parametrize("value", [{1: "value"}, {"x": (1, 2)}, {"x": float("nan")}])
def test_canonical_json_rejects_ambiguous_values(value):
    with pytest.raises((TypeError, ValueError)):
        canonical_json(value)


def test_cli_schema_asyncapi_validation_and_errors(tmp_path, capsys):
    assert main(["schema", "agent.challenge"]) == 0
    assert json.loads(capsys.readouterr().out)["title"] == "AgentChallenge"
    output = tmp_path / "asyncapi.json"
    assert main(["asyncapi", "--output", str(output)]) == 0
    assert json.loads(output.read_text())["asyncapi"] == "3.0.0"
    payload = message().payload
    assert main(["validate", "agent.challenge", json.dumps(payload)]) == 0
    capsys.readouterr()
    for args in [
        ["schema", "missing"],
        ["validate", "agent.challenge", "[]"],
        ["validate", "agent.challenge", '{"a":1,"a":2}'],
        ["validate", "agent.challenge", "@" + str(tmp_path / "missing.json")],
        ["validate", "agent.challenge", "{}"],
    ]:
        assert main(args) == 2
        assert json.loads(capsys.readouterr().out)["valid"] is False


def test_inflight_gateway_is_not_successful_delivery():
    router = ProductionRouter(default_registry())
    item = message()
    router.store.append(item)
    router.store.claim("fixed", {"accepted"}, owner="active")
    with TestClient(create_app(router=router)) as client:
        assert client.post("/v1/messages", json=item.model_dump(mode="json")).status_code == 503
    router.store.close()


def test_dead_letter_diagnostics_survive_restart(tmp_path):
    async def run():
        path = tmp_path / "dead.db"
        store = EventStore(path)
        transport = InMemoryTransport()

        async def failed(_):
            raise RuntimeError("offline")

        transport.on("virelion.agent.challenge.v1", failed)
        adapter = DurableTransportAdapter(store, transport, RetryPolicy(max_attempts=1))
        item = message()
        with pytest.raises(DeliveryError):
            await adapter.publish(item)
        store.close()
        reopened = EventStore(path)
        letters = reopened.dead_letters()
        assert len(letters) == 1 and letters[0].envelope.message_id == item.message_id
        assert letters[0].reason == "offline" and len(letters[0].attempts) == 1
        reopened.close()

    asyncio.run(run())


def test_hmac_and_wire_tampering():
    from cardibridge.security import sign_envelope, verify_envelope
    from cardibridge.security_policy import sign_bytes, verify_bytes
    from cardibridge.wire import frame, validate_frame

    item = message()
    item.signature = sign_envelope(item, b"secret")
    assert verify_envelope(item, b"secret")
    item.payload["intended_task"] = "forged"
    assert not verify_envelope(item, b"secret")
    item.signature = "é"
    assert not verify_envelope(item, b"secret")
    signature = sign_bytes(b"payload", b"secret")
    assert verify_bytes(b"payload", signature, b"secret")
    assert not verify_bytes(b"forged", signature, b"secret")
    assert not verify_bytes(b"payload", "é", b"secret")
    framed = frame(item, secret=b"secret")
    validate_frame(framed, secret=b"secret")
    framed["wire_version"] = True
    with pytest.raises(ValueError):
        validate_frame(framed)
    framed["wire_version"] = 1
    framed["payload"]["producer"] = "forged"
    with pytest.raises(ValueError):
        validate_frame(framed, secret=b"secret")


def test_legacy_gateway_can_route_injected_handlers():
    from cardibridge.http import create_app as legacy_app
    from cardibridge.router import BridgeRouter

    router = BridgeRouter(default_registry())
    router.register("agent.challenge", "worker", lambda _: "ok")
    with TestClient(legacy_app(router)) as client:
        assert client.get("/health").status_code == 200
        assert client.get("/contracts").status_code == 200
        assert client.get("/asyncapi").status_code == 200
        assert client.post("/validate/agent.challenge", json=message().payload).json()["valid"]
        assert (
            client.post("/route", json=message().model_dump(mode="json")).json()["result"] == "ok"
        )


def test_large_retry_handles_subnormal_base():
    assert RetryPolicy(base_delay_seconds=1e-320, jitter=0).delay(100000) == 300


def test_live_http_gateway_transport():
    import socket
    import threading

    import uvicorn

    router = ProductionRouter(default_registry())
    router.register("agent.challenge", "worker", lambda _: {"ok": True})
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    endpoint = f"http://127.0.0.1:{sock.getsockname()[1]}/v1/messages"
    server = uvicorn.Server(
        uvicorn.Config(create_app(router=router), log_level="critical", lifespan="off")
    )
    worker = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    worker.start()
    try:
        deadline = time.monotonic() + 10
        while not server.started and time.monotonic() < deadline:
            time.sleep(0.01)
        assert server.started
        item = message()
        transport = HttpTransport(endpoint)
        assert not asyncio.run(transport.publish(item)).duplicate
        assert asyncio.run(transport.publish(item)).duplicate
    finally:
        server.should_exit = True
        worker.join(timeout=10)
        sock.close()
        router.store.close()
        assert not worker.is_alive()


def test_upgrade_recovers_legacy_inflight_record(tmp_path):
    import sqlite3

    path = tmp_path / "legacy.db"
    item = message()
    db = sqlite3.connect(path)
    db.execute(
        "CREATE TABLE events(seq INTEGER PRIMARY KEY AUTOINCREMENT,key TEXT UNIQUE NOT NULL,message_id TEXT UNIQUE NOT NULL,topic TEXT NOT NULL,payload TEXT NOT NULL,digest TEXT NOT NULL,status TEXT NOT NULL,created_at TEXT NOT NULL)"
    )
    from cardibridge.protocol import content_hash

    raw = item.model_dump(mode="json")
    db.execute(
        "INSERT INTO events(key,message_id,topic,payload,digest,status,created_at) VALUES(?,?,?,?,?,?,?)",
        (
            "fixed",
            item.message_id,
            "virelion.agent.challenge.v1",
            json.dumps(raw),
            content_hash(raw),
            "processing",
            item.timestamp.isoformat(),
        ),
    )
    db.commit()
    db.close()
    store = EventStore(path)
    router = ProductionRouter(default_registry(), store)
    router.register("agent.challenge", "worker", lambda _: "recovered")
    assert router.dispatch(item) == "recovered"
    store.close()
