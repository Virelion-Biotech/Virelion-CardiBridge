"""Build-independent smoke test in a clean environment outside the source tree."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
import venv
from pathlib import Path


def smoke() -> None:
    from fastapi.testclient import TestClient

    import cardibridge
    from cardibridge import AgentChallenge, BridgeEnvelope, EventStore, TraceContext
    from cardibridge.codec import EnvelopeCodec
    from cardibridge.defaults import default_registry
    from cardibridge.gateway import create_app
    from cardibridge.production import ProductionRouter
    from cardibridge.transport import DurableTransportAdapter, InMemoryTransport

    source = Path(__file__).resolve().parents[1]
    assert source not in Path(cardibridge.__file__).resolve().parents
    assert cardibridge.__version__ == "0.3.1"
    trace = TraceContext(source="wheel-smoke")
    challenge = AgentChallenge(
        challenge_type="synthetic",
        population=[{"sample": "1"}],
        intended_task="round-trip",
        trace=trace,
    )
    envelope = BridgeEnvelope(
        message_type="agent.challenge",
        producer="smoke",
        consumer="worker",
        idempotency_key="smoke",
        payload=challenge.model_dump(mode="json"),
        trace=trace,
    )
    assert EnvelopeCodec.decode(EnvelopeCodec.encode(envelope)) == envelope
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "store.db"
        store = EventStore(path)
        adapter = DurableTransportAdapter(store, InMemoryTransport())
        assert not asyncio.run(adapter.publish(envelope)).duplicate
        store.close()
        reopened = EventStore(path)
        assert reopened.status(envelope.message_id) == "published"
        assert len(list(reopened.replay())) == 1
        reopened.close()
    router = ProductionRouter(default_registry())
    router.register("agent.challenge", "worker", lambda _: {"ok": True})
    with TestClient(create_app(router=router)) as client:
        raw = envelope.model_dump(mode="json")
        assert client.post("/v1/messages", json=raw).json()["status"] == "processed"
        assert client.post("/v1/messages", json=raw).json()["status"] == "duplicate"
    router.store.close()
    output = subprocess.check_output(
        [sys.executable, "-m", "cardibridge.cli", "schema", "agent.challenge"], text=True
    )
    assert json.loads(output)["title"] == "AgentChallenge"
    print("Installed wheel: imports, codec, durable delivery, restart, gateway, and CLI passed.")


def main() -> None:
    if "--smoke" in sys.argv:
        smoke()
        return
    source = Path(__file__).resolve().parents[1]
    wheels = list((source / "dist").glob("*.whl"))
    assert len(wheels) == 1, "Build exactly one wheel before validation"
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        venv.EnvBuilder(with_pip=True).create(root / "env")
        python = root / "env" / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
        subprocess.run(
            [str(python), "-m", "pip", "install", str(wheels[0]) + "[server]", "httpx"],
            check=True,
            cwd=root,
        )
        subprocess.run([str(python), "-m", "pip", "check"], check=True, cwd=root)
        subprocess.run(
            [str(python), str(Path(__file__).resolve()), "--smoke"], check=True, cwd=root
        )


if __name__ == "__main__":
    main()
