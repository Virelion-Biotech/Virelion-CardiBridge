from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

from .contracts import BridgeEnvelope
from .protocol import ContractMismatch
from .registry import ContractRegistry


@dataclass(frozen=True)
class CompatibilityResult:
    compatible: bool
    source_version: str
    target_version: str
    migrated: bool = False
    warnings: tuple[str, ...] = ()


class CompatibilityManager:
    """Explicit compatibility gate. Never silently changes scientific payloads."""

    def __init__(self, registry: ContractRegistry) -> None:
        self.registry = registry
        self._migrations: dict[tuple[str, str, str], Any] = {}

    def register_migration(self, contract: str, source: str, target: str, fn: Any) -> None:
        self._migrations[(contract, source, target)] = fn

    def check(self, contract: str, source: str, target: str) -> CompatibilityResult:
        if source == target:
            return CompatibilityResult(True, source, target)
        if (contract, source, target) in self._migrations:
            return CompatibilityResult(True, source, target, migrated=True)
        return CompatibilityResult(False, source, target, warnings=("no registered migration",))

    def migrate(self, contract: str, payload: dict[str, Any], source: str, target: str) -> dict[str, Any]:
        result = self.check(contract, source, target)
        if not result.compatible:
            raise ContractMismatch(f"no migration for {contract} {source} -> {target}")
        if source == target:
            return payload
        return self._migrations[(contract, source, target)](payload)

    def validate_target(self, contract: str, payload: dict[str, Any]) -> BaseModel:
        return self.registry.model(contract).model_validate(payload)
