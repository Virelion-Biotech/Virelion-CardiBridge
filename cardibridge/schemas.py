from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from .catalog import export_asyncapi as _export_asyncapi
from .contracts import AgentChallenge, EvaluationRequest, EvaluationResult, VexObservation

SCHEMAS: dict[str, type[BaseModel]] = {
    "agent.challenge": AgentChallenge,
    "eval.request": EvaluationRequest,
    "eval.result": EvaluationResult,
    "vex.observation": VexObservation,
}


def export_json_schemas() -> dict[str, Any]:
    return {name: SCHEMAS[name].model_json_schema() for name in sorted(SCHEMAS)}


def export_asyncapi(registry: Any) -> dict[str, Any]:
    return _export_asyncapi(registry)
