from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ContractCapability:
    name: str
    versions: tuple[str, ...]
    required: bool = True


@dataclass(frozen=True)
class NegotiatedContract:
    name: str
    version: str
    migrated: bool = False


class ContractNegotiator:
    """Select the safest mutually supported contract version."""

    def negotiate(self, producer: ContractCapability, consumer: ContractCapability) -> NegotiatedContract:
        if producer.name != consumer.name:
            raise ValueError(f"contract mismatch: {producer.name} != {consumer.name}")
        common = set(producer.versions).intersection(consumer.versions)
        if common:
            version = sorted(common, key=self._version_key, reverse=True)[0]
            return NegotiatedContract(producer.name, version, migrated=False)
        if producer.required or consumer.required:
            raise ValueError(f"no compatible version for {producer.name}")
        return NegotiatedContract(producer.name, "0.0.0", migrated=True)

    @staticmethod
    def _version_key(version: str) -> tuple[int, int, int]:
        parts = version.split(".")
        if len(parts) != 3 or any(not p.isdigit() for p in parts):
            raise ValueError(f"invalid semantic version: {version}")
        return tuple(int(p) for p in parts)  # type: ignore[return-value]
