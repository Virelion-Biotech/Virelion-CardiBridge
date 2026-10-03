from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

MigrationTransform = Callable[[dict[str, Any]], dict[str, Any]]


@dataclass(frozen=True)
class Migration:
    contract: str
    source_version: str
    target_version: str
    transform: MigrationTransform


class MigrationRegistry:
    """Explicit, deterministic registry for contract version migrations."""

    def __init__(self) -> None:
        self._items: dict[tuple[str, str, str], Migration] = {}

    def register(self, migration: Migration) -> None:
        key = (migration.contract, migration.source_version, migration.target_version)
        if not migration.contract.strip():
            raise ValueError("migration contract must not be empty")
        if migration.source_version == migration.target_version:
            raise ValueError("migration source and target versions must differ")
        if key in self._items:
            raise ValueError(f"migration already registered: {key}")
        self._items[key] = migration

    def migrate(
        self,
        contract: str,
        source_version: str,
        target_version: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        if source_version == target_version:
            return dict(payload)
        migration = self._items.get((contract, source_version, target_version))
        if migration is None:
            raise KeyError(f"no migration: {contract} {source_version} -> {target_version}")
        result = migration.transform(dict(payload))
        if not isinstance(result, dict):
            raise TypeError("migration transform must return a dict")
        return result
