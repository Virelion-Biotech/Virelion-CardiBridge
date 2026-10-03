from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .contracts import BridgeEnvelope
from .registry import ContractRegistry


@dataclass(frozen=True)
class ConformanceCase:
    name: str
    contract: str
    payload: dict[str, Any]
    expected_valid: bool = True

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("conformance case name must not be empty")
        if not self.contract.strip():
            raise ValueError("conformance contract must not be empty")


@dataclass(frozen=True)
class ConformanceResult:
    passed: bool
    cases: tuple[dict[str, Any], ...]


def run_conformance(
    registry: ContractRegistry, cases: list[ConformanceCase]
) -> ConformanceResult:
    results: list[dict[str, Any]] = []
    for case in cases:
        try:
            report = registry.validate(case.contract, case.payload)
            passed = report.valid == case.expected_valid
            results.append(
                {
                    "name": case.name,
                    "passed": passed,
                    "valid": report.valid,
                    "errors": report.errors,
                }
            )
        except (KeyError, TypeError, ValueError) as exc:
            passed = not case.expected_valid
            results.append({"name": case.name, "passed": passed, "error": str(exc)})
    return ConformanceResult(all(item["passed"] for item in results), tuple(results))


def assert_envelope_shape(envelope: BridgeEnvelope) -> None:
    """Validate basic protocol identity without inventing routing policy."""
    if not envelope.message_type.strip():
        raise AssertionError("message_type is required")
    if not envelope.producer.strip():
        raise AssertionError("producer is required")
    if not envelope.consumer.strip():
        raise AssertionError("consumer is required")
    if not envelope.idempotency_key.strip():
        raise AssertionError("idempotency_key is required")
