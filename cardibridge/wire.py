"""Stable wire-level protocol primitives for external transports."""
from __future__ import annotations

import hashlib
import hmac
from typing import Any

from .protocol import canonical_json


PROTOCOL = "cardibridge"
WIRE_VERSION = 1
CONTENT_TYPE = "application/json"


def canonical_bytes(value: Any) -> bytes:
    return canonical_json(value)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def sign(value: Any, secret: bytes) -> str:
    if not secret:
        raise ValueError("signing secret must not be empty")
    return hmac.new(secret, canonical_bytes(value), hashlib.sha256).hexdigest()


def verify(value: Any, signature: str, secret: bytes) -> bool:
    if not secret or not signature:
        return False
    expected = sign(value, secret)
    return hmac.compare_digest(expected, signature)


def frame(envelope: Any, *, key_id: str | None = None, secret: bytes | None = None) -> dict[str, Any]:
    payload = envelope.model_dump(mode="json") if hasattr(envelope, "model_dump") else envelope
    result: dict[str, Any] = {
        "protocol": PROTOCOL,
        "wire_version": WIRE_VERSION,
        "content_type": CONTENT_TYPE,
        "payload": payload,
        "content_digest": digest(payload),
    }
    if secret is not None:
        result["signature"] = sign(payload, secret)
        result["key_id"] = key_id or "default"
    return result


def validate_frame(value: dict[str, Any], *, secret: bytes | None = None) -> None:
    if not isinstance(value, dict):
        raise TypeError("wire frame must be an object")
    if value.get("protocol") != PROTOCOL or value.get("wire_version") != WIRE_VERSION:
        raise ValueError("unsupported CardiBridge wire protocol")
    if value.get("content_type") != CONTENT_TYPE:
        raise ValueError("unsupported CardiBridge content type")
    payload = value.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("wire frame payload must be an object")
    if value.get("content_digest") != digest(payload):
        raise ValueError("content digest mismatch")
    if secret is not None and not verify(payload, value.get("signature", ""), secret):
        raise ValueError("signature verification failed")
