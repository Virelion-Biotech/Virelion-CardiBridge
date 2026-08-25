"""Optional FastAPI adapter. Core CardiBridge remains framework-independent."""

from typing import Any

from .contracts import BridgeEnvelope
from .registry import ContractRegistry
from .schemas import SCHEMAS
from .router import BridgeRouter


def create_app():
    from fastapi import FastAPI, HTTPException

    registry = ContractRegistry()
    for name, model in SCHEMAS.items():
        registry.register(name, model)
    router = BridgeRouter(registry)
    app = FastAPI(title="Virelion CardiBridge", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "service": "CardiBridge", "contracts": len(SCHEMAS)}

    @app.get("/contracts")
    def contracts() -> dict[str, Any]:
        return {name: registry.model(name).model_json_schema() for name in SCHEMAS}

    @app.post("/validate/{contract}")
    def validate(contract: str, payload: dict[str, Any]):
        try:
            return registry.validate(contract, payload)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/envelope/validate")
    def validate_envelope(envelope: BridgeEnvelope):
        return registry.validate(envelope.message_type, envelope.payload)

    return app
