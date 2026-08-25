from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass(frozen=True)
class RetryPolicy:
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

    def delay(self, attempt: int) -> float:
        if attempt < 1:
            return 0.0
        multiplier = 2 ** (attempt - 1) if self.exponential else 1
        return min(self.max_delay_seconds, self.base_delay_seconds * multiplier)

    def next_retry_at(self, attempt: int, now: datetime | None = None) -> datetime:
        now = now or datetime.now(timezone.utc)
        return now + timedelta(seconds=self.delay(attempt))


@dataclass(frozen=True)
class DeliveryAttempt:
    message_id: str
    attempt: int
    success: bool
    error: str | None = None
    attempted_at: datetime = datetime.now(timezone.utc)
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
