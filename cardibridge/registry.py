from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from pydantic import BaseModel, ValidationError

from .contracts import BridgeEnvelope, ValidationReport

_SEMVER = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")


class ContractRegistry:
    """Runtime registry for immutable, versioned message contracts."""

    def __init__(self) -> None:
        self._schemas: dict[str, type[BaseModel]] = {}
        self._versions: dict[str, str] = {}
        self._fingerprints: dict[str, str] = {}

    def register(self, name: str, model: type[BaseModel], version: str = "1.0.0") -> None:
        if not name.strip() or not version.strip():
            raise ValueError("contract name and version are required")
        if not _SEMVER.fullmatch(version):
            raise ValueError(f"invalid semantic version: {version}")
        if not isinstance(model, type) or not issubclass(model, BaseModel):
            raise TypeError("model must be a Pydantic BaseModel subclass")

        fingerprint = self._fingerprint_model(model)
        if name in self._schemas:
            if self._versions[name] != version or self._fingerprints[name] != fingerprint:
                raise ValueError(f"contract {name} is immutable once registered")
            return
        self._schemas[name] = model
        self._versions[name] = version
        self._fingerprints[name] = fingerprint

    def model(self, name: str) -> type[BaseModel]:
        try:
            return self._schemas[name]
        except KeyError as exc:
            raise KeyError(f"unknown contract: {name}") from exc

    def version(self, name: str) -> str:
        self.model(name)
        return self._versions[name]

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._schemas))

    def validate(self, name: str, payload: dict[str, Any]) -> ValidationReport:
        model = self.model(name)
        version = self._versions[name]
        try:
            model.model_validate(payload)
        except ValidationError as exc:
            return ValidationReport(
                valid=False,
                schema_name=name,
                schema_version=version,
                errors=[
                    {"type": error["type"], "message": error["msg"], "loc": error["loc"]}
                    for error in exc.errors()
                ],
            )
        return ValidationReport(valid=True, schema_name=name, schema_version=version)

    def fingerprint(self, name: str) -> str:
        self.model(name)
        return self._fingerprints[name]

    @staticmethod
    def _fingerprint_model(model: type[BaseModel]) -> str:
        schema = model.model_json_schema()
        canonical = json.dumps(
            schema, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    def catalog(self) -> dict[str, dict[str, Any]]:
        return {
            name: {
                "version": self._versions[name],
                "fingerprint": self._fingerprints[name],
                "schema": self.model(name).model_json_schema(),
            }
            for name in self.names()
        }

    def wrap(
        self,
        *,
        name: str,
        producer: str,
        consumer: str,
        payload: BaseModel,
        idempotency_key: str,
        trace: Any,
    ) -> BridgeEnvelope:
        model = self.model(name)
        if not isinstance(payload, model):
            raise TypeError(f"payload must be an instance of {model.__name__}")
        validated = model.model_validate(payload.model_dump(mode="json"))
        return BridgeEnvelope(
            message_type=name,
            producer=producer,
            consumer=consumer,
            idempotency_key=idempotency_key,
            payload=validated.model_dump(mode="json"),
            trace=trace,
        )
