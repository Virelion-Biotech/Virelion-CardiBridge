from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Protocol

from .contracts import BridgeEnvelope
from .deadletter import DeadLetter, DeadLetterQueue
from .protocol import DeliveryError, DeliveryReceipt

if TYPE_CHECKING:
    from .store import EventStore


@dataclass(frozen=True)
class RetryPolicy:
    """Bounded exponential retry policy with deterministic optional jitter."""

    max_attempts: int = 5
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 300.0
    exponential: bool = True
    jitter: float = 0.10

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay_seconds < 0 or self.max_delay_seconds < 0:
            raise ValueError("retry delays must be non-negative")
        if self.jitter < 0 or self.jitter > 1:
            raise ValueError("jitter must be between 0 and 1")

    def delay(self, attempt: int, key: str | None = None) -> float:
        if attempt < 1:
            return 0.0
        multiplier = 2 ** (attempt - 1) if self.exponential else 1
        delay = min(self.max_delay_seconds, self.base_delay_seconds * multiplier)
        if not key or self.jitter == 0:
            return delay
        digest = hashlib.sha256(f"{key}:{attempt}".encode()).digest()
        fraction = int.from_bytes(digest[:8], "big") / 2**64
        factor = 1.0 + ((fraction * 2.0) - 1.0) * self.jitter
        return max(0.0, min(self.max_delay_seconds, delay * factor))

    def next_retry_at(
        self,
        attempt: int,
        now: datetime | None = None,
        key: str | None = None,
    ) -> datetime:
        now = now or datetime.now(timezone.utc)
        return now + timedelta(seconds=self.delay(attempt, key=key))


@dataclass(frozen=True)
class DeliveryAttempt:
    message_id: str
    attempt: int
    success: bool
    error: str | None = None
    attempted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    next_retry_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "attempt": self.attempt,
            "success": self.success,
            "error": self.error,
            "attempted_at": self.attempted_at.isoformat(),
            "next_retry_at": self.next_retry_at.isoformat() if self.next_retry_at else None,
        }


class PublishTransport(Protocol):
    async def publish(self, envelope: BridgeEnvelope) -> DeliveryReceipt: ...


async def attempt_with_retry(
    transport: PublishTransport,
    envelope: BridgeEnvelope,
    store: EventStore,
    retry: RetryPolicy,
    dead_letter: DeadLetterQueue,
) -> DeliveryReceipt:
    """Publish with durable attempt history, bounded retry, and terminal DLQ capture."""
    attempts: list[DeliveryAttempt] = []
    for attempt_number in range(1, retry.max_attempts + 1):
        attempted_at = datetime.now(timezone.utc)
        try:
            receipt = await transport.publish(envelope)
        except Exception as exc:
            next_retry_at = (
                retry.next_retry_at(attempt_number, now=attempted_at, key=envelope.idempotency_key)
                if attempt_number < retry.max_attempts
                else None
            )
            attempt = DeliveryAttempt(
                message_id=envelope.message_id,
                attempt=attempt_number,
                success=False,
                error=str(exc),
                attempted_at=attempted_at,
                next_retry_at=next_retry_at,
            )
            attempts.append(attempt)
            store.record_attempt(attempt)
            if next_retry_at is None:
                store.mark(envelope.message_id, "dead_letter")
                dead_letter.put(
                    DeadLetter(
                        envelope=envelope,
                        reason=str(exc),
                        attempts=tuple(attempts),
                    )
                )
                raise DeliveryError(str(exc)) from exc
            await asyncio.sleep(max(0.0, (next_retry_at - datetime.now(timezone.utc)).total_seconds()))
        else:
            attempt = DeliveryAttempt(
                message_id=envelope.message_id,
                attempt=attempt_number,
                success=True,
                attempted_at=attempted_at,
            )
            store.record_attempt(attempt)
            store.mark(envelope.message_id, "delivered")
            return receipt

    raise RuntimeError("retry loop exited without terminal result")
