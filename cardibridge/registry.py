from __future__ import annotations

import hashlib
import json
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

from .contracts import BridgeEnvelope, ValidationReport

T = TypeVar("T", bound=BaseModel)


class ContractRegistry:
    """Runtime registry for versioned message contracts and safe validation."""

    def __init__(self) -> None:
        self._schemas: dict[str, type[BaseModel]] = {}
        self._versions: dict[str, str] = {}

    def register(self, name: str, model: type[BaseModel], version: str = "1.0.0") -> None:
        if not name or not version:
            raise ValueError("contract name and version are required")
        if name in self._schemas and self._versions[name] != version:
            raise ValueError(f"contract {name} is already registered at another version")
        self._schemas[name] = model
        self._versions[name] = version

    def model(self, name: str) -> type[BaseModel]:
        try:
            return self._schemas[name]
        except KeyError as exc:
            raise KeyError(f"unknown contract: {name}") from exc

    def version(self, name: str) -> str:
        self.model(name)
        return self._versions[name]

    def validate(self, name: str, payload: dict[str, Any]) -> ValidationReport:
        model = self.model(name)
        version = self._versions[name]
        try:
            model.model_validate(payload)
        except ValidationError as exc:
            return ValidationReport(
                valid=False,
                schema=name,
                schema_version=version,
                errors=[{"type": error["type"], "message": error["msg"], "loc": error["loc"]} for error in exc.errors()],
            )
        return ValidationReport(valid=True, schema=name, schema_version=version)

    def fingerprint(self, name: str) -> str:
        schema = self.model(name).model_json_schema()
        canonical = json.dumps(schema, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(canonical).hexdigest()

    def catalog(self) -> dict[str, dict[str, Any]]:
        return {
            name: {
                "version": self._versions[name],
                "fingerprint": self.fingerprint(name),
                "schema": self.model(name).model_json_schema(),
            }
            for name in sorted(self._schemas)
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
        self.model(name).model_validate(payload.model_dump())
        return BridgeEnvelope(
            message_type=name,
            producer=producer,
            consumer=consumer,
            idempotency_key=idempotency_key,
            payload=payload.model_dump(mode="json"),
            trace=trace,
        )
