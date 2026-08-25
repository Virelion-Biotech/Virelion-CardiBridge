from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from .contracts import BridgeEnvelope

PROTOCOL_NAME = "Virelion CardiBridge Protocol"
PROTOCOL_VERSION = "1.0.0"


def canonical_json(value: Any) -> bytes:
    """Stable UTF-8 representation used for hashes, signatures and audit records."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str).encode("utf-8")


def content_hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value)).hexdigest()


def envelope_digest(envelope: BridgeEnvelope) -> str:
    data = envelope.model_dump(mode="json", exclude={"signature"})
    return content_hash(data)


def topic_for(envelope: BridgeEnvelope) -> str:
    """Stable transport topic; consumers can map this to Kafka/NATS/etc."""
    return f"virelion.{envelope.message_type}.v{envelope.trace.schema_version.split('.')[0]}"


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
