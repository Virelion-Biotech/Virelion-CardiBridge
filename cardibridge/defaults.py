from __future__ import annotations

from .contracts import AgentChallenge, BenchmarkEvidence, BenchmarkResultRecord, EvaluationRequest, EvaluationResult, VexObservation
from .registry import ContractRegistry

AGENT_CHALLENGE = "agent.challenge"
EVAL_REQUEST = "eval.request"
EVAL_RESULT = "eval.result"
VEX_OBSERVATION = "vex.observation"\nBENCHMARK_EVIDENCE = "benchmark.evidence"\nBENCHMARK_RESULT = "benchmark.result"


def default_registry() -> ContractRegistry:
    """Return a registry containing the canonical Agent/Vex/Eval contracts."""
    registry = ContractRegistry()
    registry.register(AGENT_CHALLENGE, AgentChallenge)
    registry.register(EVAL_REQUEST, EvaluationRequest)
    registry.register(EVAL_RESULT, EvaluationResult)
    registry.register(VEX_OBSERVATION, VexObservation)\n    registry.register(BENCHMARK_EVIDENCE, BenchmarkEvidence)\n    registry.register(BENCHMARK_RESULT, BenchmarkResultRecord)
    return registry


__all__ = ["AGENT_CHALLENGE", "EVAL_REQUEST", "EVAL_RESULT", "VEX_OBSERVATION", "BENCHMARK_EVIDENCE", "BENCHMARK_RESULT", "default_registry"]
