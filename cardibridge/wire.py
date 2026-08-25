"""Stable wire-level protocol primitives for external transports."""
from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def sign(value: Any, secret: bytes) -> str:
    return hmac.new(secret, canonical_bytes(value), hashlib.sha256).hexdigest()


def verify(value: Any, signature: str, secret: bytes) -> bool:
    expected = sign(value, secret)
    return hmac.compare_digest(expected, signature)


def frame(envelope: Any, *, key_id: str | None = None, secret: bytes | None = None) -> dict[str, Any]:
    payload = envelope.model_dump(mode="json") if hasattr(envelope, "model_dump") else envelope
    result: dict[str, Any] = {
        "protocol": "cardibridge",
        "wire_version": 1,
        "content_type": "application/json",
        "payload": payload,
        "content_digest": digest(payload),
    }
    if secret is not None:
        result["signature"] = sign(payload, secret)
        result["key_id"] = key_id or "default"
    return result


def validate_frame(value: dict[str, Any], *, secret: bytes | None = None) -> None:
    if value.get("protocol") != "cardibridge" or value.get("wire_version") != 1:
        raise ValueError("unsupported CardiBridge wire protocol")
    payload = value.get("payload")
    if digest(payload) != value.get("content_digest"):
        raise ValueError("content digest mismatch")
    if secret is not None and not verify(payload, value.get("signature", ""), secret):
        raise ValueError("signature verification failed")
