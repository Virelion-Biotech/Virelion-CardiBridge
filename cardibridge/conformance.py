from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .contracts import BridgeEnvelope
from .registry import ContractRegistry


@dataclass(frozen=True)
class ConformanceCase:
    name: str
    contract: str
    payload: dict[str, Any]
    expected_valid: bool = True


@dataclass(frozen=True)
class ConformanceResult:
    passed: bool
    cases: tuple[dict[str, Any], ...]


def run_conformance(registry: ContractRegistry, cases: list[ConformanceCase]) -> ConformanceResult:
    results: list[dict[str, Any]] = []
    for case in cases:
        try:
            report = registry.validate(case.contract, case.payload)
            passed = report.valid == case.expected_valid
            results.append({"name": case.name, "passed": passed, "valid": report.valid, "errors": report.errors})
        except Exception as exc:
            results.append({"name": case.name, "passed": not case.expected_valid, "error": str(exc)})
    return ConformanceResult(all(x["passed"] for x in results), tuple(results))


def assert_envelope_shape(envelope: BridgeEnvelope) -> None:
    if envelope.producer == envelope.consumer:
        raise AssertionError("producer and consumer must be distinct protocol actors")
    if not envelope.message_type:
        raise AssertionError("message_type is required")
