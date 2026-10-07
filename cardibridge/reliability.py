from __future__ import annotations

import asyncio
import hashlib
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Protocol
from uuid import uuid4

from .contracts import BridgeEnvelope
from .deadletter import DeadLetter, DeadLetterQueue
from .protocol import DeliveryError, DeliveryReceipt, topic_for

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
        if type(self.max_attempts) is not int or self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if not all(
            math.isfinite(v) for v in (self.base_delay_seconds, self.max_delay_seconds, self.jitter)
        ):
            raise ValueError("retry values must be finite")
        if self.base_delay_seconds < 0 or self.max_delay_seconds < 0:
            raise ValueError("retry delays must be non-negative")
        if self.base_delay_seconds > self.max_delay_seconds:
            raise ValueError("base_delay_seconds cannot exceed max_delay_seconds")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter must be between 0 and 1")

    def delay(self, attempt: int, key: str | None = None) -> float:
        if attempt < 1:
            return 0.0
        try:
            raw = (
                math.ldexp(self.base_delay_seconds, attempt - 1)
                if self.exponential and self.base_delay_seconds
                else self.base_delay_seconds
            )
        except OverflowError:
            raw = self.max_delay_seconds
        delay = min(self.max_delay_seconds, raw)
        if not key or self.jitter == 0 or delay == 0:
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
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        return current + timedelta(seconds=self.delay(attempt, key=key))


@dataclass(frozen=True)
class DeliveryAttempt:
    message_id: str
    attempt: int
    success: bool
    error: str | None = None
    attempted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    next_retry_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.attempt < 1:
            raise ValueError("attempt must be >= 1")
        if self.attempted_at.tzinfo is None:
            raise ValueError("attempted_at must be timezone-aware")
        if self.next_retry_at is not None and self.next_retry_at.tzinfo is None:
            raise ValueError("next_retry_at must be timezone-aware")

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
    """Persist an outbox record, resume existing retries, and capture terminal failures."""
    accepted = store.append(envelope, status="outbox")
    current = store.get_envelope(envelope.message_id)
    if current is None:
        raise ValueError("outbox envelope is missing")

    if not accepted:
        status = store.status_by_key(envelope.idempotency_key)
        stored = store.get_envelope(envelope.message_id)
        if stored is None:
            raise ValueError("idempotency record exists but its envelope is missing")
        if status in {"published", "dead_letter", "processed", "processing"}:
            return DeliveryReceipt(
                stored.message_id,
                stored.idempotency_key,
                topic_for(stored),
                stored.timestamp.isoformat(),
                duplicate=True,
                sequence=None,
            )
        if status not in {"outbox", "retry", "publishing"}:
            raise ValueError(f"cannot resume delivery in status {status!r}")
        current = stored

    owner = uuid4().hex
    if not store.claim(
        current.idempotency_key, {"outbox", "retry"}, owner=owner, processing_status="publishing"
    ):
        return DeliveryReceipt(
            current.message_id,
            current.idempotency_key,
            topic_for(current),
            current.timestamp.isoformat(),
            duplicate=True,
        )
    try:
        with store.lease(current.idempotency_key, owner):
            return await _publish_claimed(
                transport,
                current,
                store,
                retry,
                dead_letter,
                store.attempts(current.message_id),
                owner,
            )
    except BaseException:
        if store.status(current.message_id) == "publishing":
            store.mark(current.message_id, "retry", owner=owner)
        raise


async def _publish_claimed(
    transport: PublishTransport,
    current: BridgeEnvelope,
    store: EventStore,
    retry: RetryPolicy,
    dead_letter: DeadLetterQueue,
    prior_attempts: list[DeliveryAttempt],
    owner: str,
) -> DeliveryReceipt:
    if prior_attempts and prior_attempts[-1].success:
        store.mark(current.message_id, "published", owner=owner)
        return DeliveryReceipt(
            current.message_id,
            current.idempotency_key,
            topic_for(current),
            current.timestamp.isoformat(),
            duplicate=True,
        )
    if prior_attempts and prior_attempts[-1].next_retry_at:
        await asyncio.sleep(
            max(
                0.0, (prior_attempts[-1].next_retry_at - datetime.now(timezone.utc)).total_seconds()
            )
        )
    if len(prior_attempts) >= retry.max_attempts:
        reason = prior_attempts[-1].error if prior_attempts else "retry budget exhausted"
        store.mark(current.message_id, "dead_letter", owner=owner)
        dead_letter.put(
            DeadLetter(
                envelope=current,
                reason=reason or "retry budget exhausted",
                attempts=tuple(prior_attempts),
            )
        )
        raise DeliveryError(reason or "retry budget exhausted")

    attempts = list(prior_attempts)
    start = len(attempts) + 1
    for attempt_number in range(start, retry.max_attempts + 1):
        attempted_at = datetime.now(timezone.utc)
        try:
            receipt = await transport.publish(current)
        except DeliveryError as exc:
            next_retry_at = (
                retry.next_retry_at(
                    attempt_number, now=datetime.now(timezone.utc), key=current.idempotency_key
                )
                if attempt_number < retry.max_attempts
                else None
            )
            attempt = DeliveryAttempt(
                message_id=current.message_id,
                attempt=attempt_number,
                success=False,
                error=str(exc),
                attempted_at=attempted_at,
                next_retry_at=next_retry_at,
            )
            attempts.append(attempt)
            store.record_attempt(attempt, owner=owner)
            if next_retry_at is None:
                store.mark(current.message_id, "dead_letter", owner=owner)
                dead_letter.put(
                    DeadLetter(envelope=current, reason=str(exc), attempts=tuple(attempts))
                )
                raise DeliveryError(str(exc)) from exc
            await asyncio.sleep(
                max(0.0, (next_retry_at - datetime.now(timezone.utc)).total_seconds())
            )
        else:
            attempt = DeliveryAttempt(
                message_id=current.message_id,
                attempt=attempt_number,
                success=True,
                attempted_at=attempted_at,
            )
            store.record_attempt(attempt, owner=owner)
            store.mark(current.message_id, "published", owner=owner)
            return receipt

    raise RuntimeError("retry loop exited without terminal result")
