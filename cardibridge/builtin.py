from __future__ import annotations

from pydantic import BaseModel

from .contracts import AgentChallenge, EvaluationRequest, EvaluationResult, VexObservation
from .registry import ContractRegistry

CONTRACTS: dict[str, type[BaseModel]] = {
    "agent.challenge": AgentChallenge,
    "vex.observation": VexObservation,
    "eval.request": EvaluationRequest,
    "eval.result": EvaluationResult,
}


def default_registry() -> ContractRegistry:
    from .defaults import default_registry as canonical_registry

    return canonical_registry()
