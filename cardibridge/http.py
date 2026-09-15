"""Optional FastAPI adapter. Core CardiBridge remains framework-independent."""

from typing import Any

from .catalog import export_asyncapi, export_catalog
from .contracts import BridgeEnvelope
from .registry import ContractRegistry
from .router import BridgeRouter
from .schemas import SCHEMAS


def create_app() -> Any:
    from fastapi import FastAPI, HTTPException

    registry = ContractRegistry()
    for name, model in SCHEMAS.items():
        registry.register(name, model)
    bridge_router = BridgeRouter(registry)
    app = FastAPI(title="Virelion CardiBridge", version="0.3.0")

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "service": "CardiBridge", "contracts": len(SCHEMAS)}

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
        return registry.validate(envelope.message_type, envelope.payload)

    @app.post("/route")
    def route(envelope: BridgeEnvelope) -> dict[str, Any]:
        return {"result": bridge_router.dispatch(envelope)}

    return app
