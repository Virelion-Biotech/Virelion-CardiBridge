from __future__ import annotations

import time
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
    latency_ms: list[float] = field(default_factory=list)
    _lock: Lock = field(default_factory=Lock, repr=False)

    def observe(self, name: str, value: int = 1) -> None:
        with self._lock:
            setattr(self, name, getattr(self, name) + value)

    def latency(self, started: float) -> None:
        with self._lock:
            self.latency_ms.append((time.perf_counter() - started) * 1000)

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
    values.sort()
    return round(values[min(len(values) - 1, int(len(values) * p))], 3)
