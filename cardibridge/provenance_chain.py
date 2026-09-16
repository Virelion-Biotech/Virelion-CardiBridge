from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock
from typing import Any

from .contracts import LineageEvent
from .protocol import canonical_json, content_hash


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
    """Append-only hash chain for scientific auditability and tamper detection."""

    def __init__(self) -> None:
        self._blocks: list[ProvenanceBlock] = []
        self._lock = RLock()

    @staticmethod
    def _digest(value: Any) -> str:
        import hashlib

        return hashlib.sha256(canonical_json(value)).hexdigest()

    def append(self, event_id: str, event_type: str, actor: str, payload: Any) -> ProvenanceBlock:
        if not event_id.strip() or not event_type.strip() or not actor.strip():
            raise ValueError("event_id, event_type and actor must not be empty")
        with self._lock:
            previous = self._blocks[-1].digest if self._blocks else "0" * 64
            created = datetime.now(timezone.utc).isoformat()
            body: dict[str, int | str] = {
                "sequence": len(self._blocks),
                "event_id": event_id,
                "event_type": event_type,
                "actor": actor,
                "payload_digest": content_hash(payload),
                "previous_digest": previous,
                "created_at": created,
            }
            block = ProvenanceBlock(
                sequence=len(self._blocks),
                event_id=event_id,
                event_type=event_type,
                actor=actor,
                payload_digest=body["payload_digest"],
                previous_digest=previous,
                created_at=created,
                digest=self._digest(body),
            )
            self._blocks.append(block)
            return block

    def append_lineage(self, event: LineageEvent) -> ProvenanceBlock:
        return self.append(
            event.event_id,
            f"lineage.{event.event_type.lower()}",
            event.producer,
            event.to_dict(),
        )

    def verify(self) -> bool:
        with self._lock:
            previous = "0" * 64
            for index, block in enumerate(self._blocks):
                body: dict[str, int | str] = {
                    "sequence": block.sequence,
                    "event_id": block.event_id,
                    "event_type": block.event_type,
                    "actor": block.actor,
                    "payload_digest": block.payload_digest,
                    "previous_digest": block.previous_digest,
                    "created_at": block.created_at,
                }
                if (
                    block.sequence != index
                    or block.previous_digest != previous
                    or block.digest != self._digest(body)
                ):
                    return False
                previous = block.digest
            return True

    def tip(self) -> str | None:
        with self._lock:
            return self._blocks[-1].digest if self._blocks else None

    def export(self) -> list[dict[str, Any]]:
        with self._lock:
            return [block.__dict__.copy() for block in self._blocks]
