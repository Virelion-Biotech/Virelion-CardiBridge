from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel


@dataclass(frozen=True)
class Migration:
    contract: str
    source_version: str
    target_version: str
    transform: Any


class MigrationRegistry:
    def __init__(self) -> None:
        self._items: dict[tuple[str, str, str], Migration] = {}

    def register(self, migration: Migration) -> None:
        key = (migration.contract, migration.source_version, migration.target_version)
        if key in self._items:
            raise ValueError(f"migration already registered: {key}")
        self._items[key] = migration

    def migrate(self, contract: str, source_version: str, target_version: str, payload: dict[str, Any]) -> dict[str, Any]:
        if source_version == target_version:
            return payload
        migration = self._items.get((contract, source_version, target_version))
        if migration is None:
            raise KeyError(f"no migration: {contract} {source_version} -> {target_version}")
        result = migration.transform(payload)
        if not isinstance(result, dict):
            raise TypeError("migration transform must return a dict")
        return result
