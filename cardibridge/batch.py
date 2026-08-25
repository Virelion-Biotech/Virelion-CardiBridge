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
    items = list(envelopes)
    if len(items) > max_size:
        raise ValueError(f"batch exceeds maximum size {max_size}")
    seen: set[str] = set()
    errors: list[str] = []
    accepted = 0
    for envelope in items:
        if envelope.message_id in seen:
            errors.append(f"duplicate message_id: {envelope.message_id}")
            continue
        seen.add(envelope.message_id)
        if not envelope.payload:
            errors.append(f"empty payload: {envelope.message_id}")
            continue
        accepted += 1
    return BatchResult(accepted, len(items) - accepted, tuple(errors))


def partition_payload(payload: dict[str, Any], keys: list[str]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split a payload into transport-safe routing metadata and scientific body."""
    routing = {key: payload[key] for key in keys if key in payload}
    body = {key: value for key, value in payload.items() if key not in routing}
    return routing, body
