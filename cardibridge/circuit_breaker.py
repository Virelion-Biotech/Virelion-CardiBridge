from __future__ import annotations

from dataclasses import dataclass
from threading import RLock
from time import monotonic


@dataclass
class CircuitState:
    failures: int = 0
    opened_at: float | None = None


class CircuitBreaker:
    """Thread-safe dependency-free circuit breaker."""

    def __init__(self, failure_threshold: int = 5, recovery_seconds: float = 30.0) -> None:
        if failure_threshold < 1 or recovery_seconds <= 0:
            raise ValueError("invalid circuit breaker configuration")
        self.failure_threshold = failure_threshold
        self.recovery_seconds = recovery_seconds
        self.state = CircuitState()
        self._lock = RLock()

    @property
    def open(self) -> bool:
        with self._lock:
            if self.state.opened_at is None:
                return False
            if monotonic() - self.state.opened_at >= self.recovery_seconds:
                self.state = CircuitState()
                return False
            return True

    def allow(self) -> bool:
        return not self.open

    def success(self) -> None:
        with self._lock:
            self.state = CircuitState()

    def failure(self) -> None:
        with self._lock:
            self.state.failures += 1
            if self.state.failures >= self.failure_threshold:
                self.state.opened_at = monotonic()
