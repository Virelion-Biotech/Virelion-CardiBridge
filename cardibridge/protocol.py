from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

from .contracts import BridgeEnvelope

PROTOCOL_NAME = "Virelion CardiBridge Protocol"
PROTOCOL_VERSION = "1.0.0"
_SEMVER = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")


def canonical_json(value: Any) -> bytes:
    """Return a deterministic UTF-8 JSON encoding; reject non-JSON values."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def content_hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value)).hexdigest()


def envelope_digest(envelope: BridgeEnvelope) -> str:
    return content_hash(envelope.model_dump(mode="json", exclude={"signature"}))


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
