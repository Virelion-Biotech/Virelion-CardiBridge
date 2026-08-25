from __future__ import annotations

from .contracts import AgentChallenge, EvaluationRequest, EvaluationResult, VexObservation
from .registry import ContractRegistry

AGENT_CHALLENGE = "agent.challenge"
VEX_OBSERVATION = "vex.observation"
EVAL_REQUEST = "eval.request"
EVAL_RESULT = "eval.result"


def default_registry() -> ContractRegistry:
    """Return a registry containing the canonical Agent/Vex/Eval contracts."""
    registry = ContractRegistry()
    registry.register(AGENT_CHALLENGE, AgentChallenge)
    registry.register(VEX_OBSERVATION, VexObservation)
    registry.register(EVAL_REQUEST, EvaluationRequest)
    registry.register(EVAL_RESULT, EvaluationResult)
    return registry


__all__ = [
    "AGENT_CHALLENGE", "VEX_OBSERVATION", "EVAL_REQUEST", "EVAL_RESULT", "default_registry",
]
