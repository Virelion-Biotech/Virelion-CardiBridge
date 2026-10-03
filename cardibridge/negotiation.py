from __future__ import annotations

import re
from dataclasses import dataclass

_SEMVER = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")


@dataclass(frozen=True)
class ContractCapability:
    name: str
    versions: tuple[str, ...]
    required: bool = True

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("contract name must not be empty")
        if not self.versions:
            raise ValueError("at least one contract version is required")
        if any(not _SEMVER.fullmatch(version) for version in self.versions):
            raise ValueError("contract versions must use strict semantic versioning")


@dataclass(frozen=True)
class NegotiatedContract:
    name: str
    version: str
    migrated: bool = False


class ContractNegotiator:
    """Select the highest mutually supported semantic version."""

    def negotiate(
        self, producer: ContractCapability, consumer: ContractCapability
    ) -> NegotiatedContract:
        if producer.name != consumer.name:
            raise ValueError(f"contract mismatch: {producer.name} != {consumer.name}")
        common = set(producer.versions).intersection(consumer.versions)
        if common:
            version = max(common, key=self._version_key)
            return NegotiatedContract(producer.name, version)
        raise ValueError(f"no compatible version for {producer.name}")

    @staticmethod
    def _version_key(version: str) -> tuple[int, int, int]:
        match = _SEMVER.fullmatch(version)
        if match is None:
            raise ValueError(f"invalid semantic version: {version}")
        return tuple(int(part) for part in match.groups())  # type: ignore[return-value]
