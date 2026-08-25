"""Optional HTTP gateway for the CardiBridge protocol."""
from __future__ import annotations

import os
from typing import Any

from .codec import EnvelopeCodec
from .health import health
from .protocol import BridgeEnvelope
from .registry import ContractRegistry


def create_app(registry: ContractRegistry | None = None, codec: EnvelopeCodec | None = None):
    """Create a FastAPI gateway without making FastAPI a core dependency."""
    try:
        from fastapi import FastAPI, Header, HTTPException
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Install the 'http' extra to use the HTTP gateway") from exc

    registry = registry or ContractRegistry()
    codec = codec or EnvelopeCodec()
    app = FastAPI(title="CardiBridge Protocol Gateway", version="1.0")

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
            registry.validate(obj)
            return {"valid": True, "contract": obj.contract, "version": obj.version}
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/v1/encode")
    def encode(envelope: dict[str, Any], x_bridge_key: str | None = Header(default=None)) -> dict[str, str]:
        required = os.getenv("CARDIBRIDGE_GATEWAY_KEY")
        if required and x_bridge_key != required:
            raise HTTPException(status_code=401, detail="invalid gateway credential")
        try:
            obj = BridgeEnvelope.model_validate(envelope)
            registry.validate(obj)
            return {"payload": codec.encode(obj)}
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    return app
