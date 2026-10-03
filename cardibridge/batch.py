from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from .contracts import BridgeEnvelope


@dataclass(frozen=True)
class BatchResult:
    accepted: int
    rejected: int
    errors: tuple[str, ...]


def validate_batch(envelopes: Iterable[BridgeEnvelope], max_size: int = 1000) -> BatchResult:
    """Validate batch size and reject duplicate message or idempotency identities."""
    if max_size < 1:
        raise ValueError("max_size must be >= 1")

    seen_message_ids: set[str] = set()
    seen_keys: set[str] = set()
    errors: list[str] = []
    accepted = 0
    rejected = 0

    for index, envelope in enumerate(envelopes, start=1):
        if index > max_size:
            raise ValueError(f"batch exceeds maximum size {max_size}")

        duplicate_message = envelope.message_id in seen_message_ids
        duplicate_key = envelope.idempotency_key in seen_keys
        seen_message_ids.add(envelope.message_id)
        seen_keys.add(envelope.idempotency_key)

        if duplicate_message:
            errors.append(f"duplicate message_id: {envelope.message_id}")
            rejected += 1
            continue
        if duplicate_key:
            errors.append(f"duplicate idempotency_key: {envelope.idempotency_key}")
            rejected += 1
            continue
        if not envelope.payload:
            errors.append(f"empty payload: {envelope.message_id}")
            rejected += 1
            continue
        accepted += 1

    return BatchResult(accepted, rejected, tuple(errors))


def partition_payload(
    payload: dict[str, Any], keys: Iterable[str]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split a payload into routing metadata and the remaining scientific body."""
    routing_keys = set(keys)
    routing = {key: payload[key] for key in routing_keys if key in payload}
    body = {key: value for key, value in payload.items() if key not in routing_keys}
    return routing, body
