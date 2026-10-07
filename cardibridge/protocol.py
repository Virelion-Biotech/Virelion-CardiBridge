from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

from .contracts import BridgeEnvelope

PROTOCOL_NAME = "Virelion CardiBridge Protocol"
PROTOCOL_VERSION = "1.0.0"
_SEMVER = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")


def canonical_json(value: Any) -> bytes:
    """Return a deterministic UTF-8 JSON encoding; reject non-JSON values."""

    def check(item: Any) -> None:
        if isinstance(item, dict):
            if any(not isinstance(key, str) for key in item):
                raise TypeError("JSON object keys must be strings")
            for child in item.values():
                check(child)
        elif isinstance(item, list):
            for child in item:
                check(child)
        elif item is not None and type(item) not in (str, bool, int, float):
            raise TypeError("value is not a JSON primitive")

    check(value)
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def model_json(model: BaseModel, *, exclude: set[str] | None = None) -> dict[str, Any]:
    """Reject key coercion before Pydantic's JSON serialization can lose data."""

    def check_keys(value: Any) -> None:
        if isinstance(value, dict):
            if any(not isinstance(key, str) for key in value):
                raise ValueError("JSON object keys must be strings before serialization")
            for child in value.values():
                check_keys(child)
        elif isinstance(value, (list, tuple)):
            for child in value:
                check_keys(child)

    check_keys(model.model_dump(mode="python", exclude=exclude))
    return model.model_dump(mode="json", exclude=exclude)


def content_hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value)).hexdigest()


def envelope_digest(envelope: BridgeEnvelope) -> str:
    return content_hash(model_json(envelope, exclude={"signature"}))


def topic_for(envelope: BridgeEnvelope) -> str:
    """Stable transport topic derived from the trace schema major version."""
    match = _SEMVER.fullmatch(envelope.trace.schema_version)
    if match is None:
        raise ValueError(f"invalid trace schema version: {envelope.trace.schema_version}")
    return f"virelion.{envelope.message_type}.v{match.group(1)}"


@dataclass(frozen=True)
class DeliveryReceipt:
    message_id: str
    idempotency_key: str
    topic: str
    accepted_at: str
    duplicate: bool = False
    sequence: int | None = None


class ProtocolError(Exception):
    """Base error for protocol-level failures."""


class ContractMismatch(ProtocolError):
    pass


class AuthenticationError(ProtocolError):
    pass


class DeliveryError(ProtocolError):
    pass
