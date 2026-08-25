from __future__ import annotations

from .contracts import AgentChallenge, EvaluationRequest, EvaluationResult, VexObservation
from .registry import ContractRegistry

CONTRACTS = {
    "agent.challenge": AgentChallenge,
    "vex.observation": VexObservation,
    "eval.request": EvaluationRequest,
    "eval.result": EvaluationResult,
}


def default_registry() -> ContractRegistry:
    registry = ContractRegistry()
    for name, model in CONTRACTS.items():
        registry.register(name, model, "1.0.0")
    return registry
