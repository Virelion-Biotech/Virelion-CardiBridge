from __future__ import annotations

from typing import Any

from .registry import ContractRegistry


def export_catalog(registry: ContractRegistry) -> dict[str, Any]:
    """Return a machine-readable contract catalog suitable for OpenAPI/registries."""
    contracts: dict[str, Any] = {}
    for name in sorted(registry._schemas):
        model = registry.model(name)
        contracts[name] = {
            "version": registry._versions[name],
            "fingerprint": registry.fingerprint(name),
            "schema": model.model_json_schema(),
        }
    return {"protocol": "virelion-cardibridge", "contracts": contracts}
