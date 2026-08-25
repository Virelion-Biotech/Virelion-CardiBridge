"""HTTP gateway for the CardiBridge protocol."""
from __future__ import annotations

import os
from typing import Any

from .codec import EnvelopeCodec
from .health import health
from .production import ProductionRouter
from .protocol import BridgeEnvelope
from .registry import ContractRegistry


def create_app(
    registry: ContractRegistry | None = None,
    codec: EnvelopeCodec | None = None,
    router: ProductionRouter | None = None,
):
    """Create a FastAPI gateway; transport remains optional to the core package."""
    try:
        from fastapi import FastAPI, Header, HTTPException
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Install the 'http' extra to use the HTTP gateway") from exc

    registry = registry or ContractRegistry()
    codec = codec or EnvelopeCodec()
    router = router or ProductionRouter(registry)
    app = FastAPI(title="CardiBridge Protocol Gateway", version="1.0")

    def credential_ok(key: str | None) -> bool:
        required = os.getenv("CARDIBRIDGE_GATEWAY_KEY")
        return not required or key == required

    @app.get("/health")
    def health_endpoint() -> dict[str, Any]:
        return health(registry).model_dump(mode="json")

    @app.get("/ready")
    def ready_endpoint() -> dict[str, Any]:
        snapshot = health(registry)
        if not snapshot.ready:
            raise HTTPException(status_code=503, detail=snapshot.model_dump(mode="json"))
        return snapshot.model_dump(mode="json")

    @app.get("/v1/contracts")
    def contracts() -> dict[str, Any]:
        return registry.catalog()

    @app.post("/v1/validate")
    def validate(envelope: dict[str, Any]) -> dict[str, Any]:
        try:
            obj = BridgeEnvelope.model_validate(envelope)
            report = registry.validate(obj.message_type, obj.payload)
            return {"valid": report.valid, "report": report.model_dump(mode="json")}
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/v1/encode")
    def encode(envelope: dict[str, Any], x_bridge_key: str | None = Header(default=None)) -> dict[str, str]:
        if not credential_ok(x_bridge_key):
            raise HTTPException(status_code=401, detail="invalid gateway credential")
        try:
            obj = BridgeEnvelope.model_validate(envelope)
            report = registry.validate(obj.message_type, obj.payload)
            if not report.valid:
                raise ValueError(report.model_dump_json())
            return {"payload": codec.encode(obj)}
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/v1/messages")
    def publish(envelope: BridgeEnvelope, x_bridge_key: str | None = Header(default=None)) -> dict[str, Any]:
        if not credential_ok(x_bridge_key):
            raise HTTPException(status_code=401, detail="invalid gateway credential")
        report = registry.validate(envelope.message_type, envelope.payload)
        if not report.valid:
            raise HTTPException(status_code=422, detail=report.model_dump(mode="json"))
        try:
            result = router.dispatch(envelope)
            return {"status": "processed", "message_id": envelope.message_id, "result": result}
        except LookupError as exc:
            return {"status": "dead_letter", "message_id": envelope.message_id, "detail": str(exc)}

    @app.get("/v1/replay")
    def replay(topic: str | None = None, after: int = 0) -> list[Any]:
        return list(router.replay(topic, after))

    return app
