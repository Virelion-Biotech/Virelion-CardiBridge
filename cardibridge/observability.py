from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from threading import Lock
from typing import Any


@dataclass
class BridgeMetrics:
    published: int = 0
    duplicates: int = 0
    validation_failures: int = 0
    delivery_failures: int = 0
    handler_failures: int = 0
    max_latency_samples: int = 10_000
    latency_ms: deque[float] = field(default_factory=deque, repr=False)
    _lock: Lock = field(default_factory=Lock, repr=False)

    def __post_init__(self) -> None:
        if self.max_latency_samples < 1:
            raise ValueError("max_latency_samples must be >= 1")
        if not isinstance(self.latency_ms, deque) or self.latency_ms.maxlen != self.max_latency_samples:
            self.latency_ms = deque(self.latency_ms, maxlen=self.max_latency_samples)

    def observe(self, name: str, value: int = 1) -> None:
        if name not in {
            "published",
            "duplicates",
            "validation_failures",
            "delivery_failures",
            "handler_failures",
        }:
            raise ValueError(f"unknown metric: {name}")
        if not isinstance(value, int):
            raise TypeError("metric increments must be integers")
        with self._lock:
            setattr(self, name, getattr(self, name) + value)

    def latency(self, started: float) -> None:
        elapsed = (time.perf_counter() - started) * 1000
        with self._lock:
            self.latency_ms.append(elapsed)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            values = list(self.latency_ms)
            return {
                "published": self.published,
                "duplicates": self.duplicates,
                "validation_failures": self.validation_failures,
                "delivery_failures": self.delivery_failures,
                "handler_failures": self.handler_failures,
                "latency_ms": {
                    "count": len(values),
                    "p50": _percentile(values, 0.50),
                    "p95": _percentile(values, 0.95),
                    "p99": _percentile(values, 0.99),
                },
            }


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    if not 0.0 <= p <= 1.0:
        raise ValueError("percentile must be between 0 and 1")
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(p * len(ordered) + 0.999999) - 1))
    return round(ordered[index], 3)
