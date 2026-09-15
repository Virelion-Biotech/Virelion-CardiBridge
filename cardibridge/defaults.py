from __future__ import annotations

from .contracts import AgentChallenge, EvaluationRequest, EvaluationResult, VexObservation
from .registry import ContractRegistry

AGENT_CHALLENGE = "agent.challenge"
EVAL_REQUEST = "eval.request"
EVAL_RESULT = "eval.result"
VEX_OBSERVATION = "vex.observation"


def default_registry() -> ContractRegistry:
    """Return a registry containing the canonical Agent/Vex/Eval contracts."""
    registry = ContractRegistry()
    registry.register(AGENT_CHALLENGE, AgentChallenge)
    registry.register(EVAL_REQUEST, EvaluationRequest)
    registry.register(EVAL_RESULT, EvaluationResult)
    registry.register(VEX_OBSERVATION, VexObservation)
    return registry


__all__ = ["AGENT_CHALLENGE", "EVAL_REQUEST", "EVAL_RESULT", "VEX_OBSERVATION", "default_registry"]
