"""Backward-compatible optional FastAPI adapter.

New deployments should use :func:`cardibridge.gateway.create_app`, which includes
stable /v1 routes, durable storage, readiness checks, and gateway authentication.
"""

from __future__ import annotations

from typing import Any

from .catalog import export_asyncapi, export_catalog
from .contracts import BridgeEnvelope
from .defaults import default_registry
from .registry import ContractRegistry
from .router import BridgeRouter


def create_app() -> Any:
    try:
        from fastapi import FastAPI, HTTPException
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Install the 'server' extra to use the HTTP gateway") from exc

    registry = default_registry()
    bridge_router = BridgeRouter(registry)
    app = FastAPI(title="Virelion CardiBridge", version="0.3.0")

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "service": "CardiBridge",
            "contracts": len(registry.names()),
        }

    @app.get("/contracts")
    def contracts() -> dict[str, Any]:
        return export_catalog(registry)

    @app.get("/asyncapi")
    def asyncapi() -> dict[str, Any]:
        return export_asyncapi(registry)

    @app.post("/validate/{contract}")
    def validate(contract: str, payload: dict[str, Any]) -> Any:
        try:
            return registry.validate(contract, payload)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/envelope/validate")
    def validate_envelope(envelope: BridgeEnvelope) -> Any:
        try:
            return registry.validate(envelope.message_type, envelope.payload)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/route")
    def route(envelope: BridgeEnvelope) -> dict[str, Any]:
        try:
            return {"result": bridge_router.dispatch(envelope)}
        except LookupError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    return app
