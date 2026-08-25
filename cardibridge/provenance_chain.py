from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class ProvenanceBlock:
    sequence: int
    event_id: str
    event_type: str
    actor: str
    payload_digest: str
    previous_digest: str
    created_at: str
    digest: str


class ProvenanceChain:
    """Append-only hash chain for scientific auditability.

    The chain does not store payloads; it stores cryptographic commitments to them.
    This makes tampering detectable without forcing sensitive scientific data into
    an audit log.
    """

    def __init__(self) -> None:
        self._blocks: list[ProvenanceBlock] = []

    @staticmethod
    def _digest(value: Any) -> str:
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
        return hashlib.sha256(raw).hexdigest()

    def append(self, event_id: str, event_type: str, actor: str, payload: Any) -> ProvenanceBlock:
        previous = self._blocks[-1].digest if self._blocks else "0" * 64
        created = datetime.now(timezone.utc).isoformat()
        payload_digest = self._digest(payload)
        body = {
            "sequence": len(self._blocks), "event_id": event_id,
            "event_type": event_type, "actor": actor,
            "payload_digest": payload_digest, "previous_digest": previous,
            "created_at": created,
        }
        block = ProvenanceBlock(**body, digest=self._digest(body))
        self._blocks.append(block)
        return block

    def verify(self) -> bool:
        previous = "0" * 64
        for index, block in enumerate(self._blocks):
            body = {
                "sequence": block.sequence, "event_id": block.event_id,
                "event_type": block.event_type, "actor": block.actor,
                "payload_digest": block.payload_digest, "previous_digest": block.previous_digest,
                "created_at": block.created_at,
            }
            if block.sequence != index or block.previous_digest != previous or block.digest != self._digest(body):
                return False
            previous = block.digest
        return True

    def export(self) -> list[dict[str, Any]]:
        return [block.__dict__.copy() for block in self._blocks]
