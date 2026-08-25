from __future__ import annotations

from dataclasses import dataclass
from time import monotonic


@dataclass
class CircuitState:
    failures: int = 0
    opened_at: float | None = None


class CircuitBreaker:
    """Small dependency-free circuit breaker for downstream Virelion services."""

    def __init__(self, failure_threshold: int = 5, recovery_seconds: float = 30.0) -> None:
        if failure_threshold < 1 or recovery_seconds <= 0:
            raise ValueError("invalid circuit breaker configuration")
        self.failure_threshold = failure_threshold
        self.recovery_seconds = recovery_seconds
        self.state = CircuitState()

    @property
    def open(self) -> bool:
        if self.state.opened_at is None:
            return False
        if monotonic() - self.state.opened_at >= self.recovery_seconds:
            self.state.opened_at = None
            self.state.failures = 0
            return False
        return True

    def allow(self) -> bool:
        return not self.open

    def success(self) -> None:
        self.state = CircuitState()

    def failure(self) -> None:
        self.state.failures += 1
        if self.state.failures >= self.failure_threshold:
            self.state.opened_at = monotonic()
