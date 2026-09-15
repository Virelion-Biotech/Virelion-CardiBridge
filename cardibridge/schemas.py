from .contracts import *

SCHEMAS = {
    "vex.observation": VexObservation,
    "agent.challenge": AgentChallenge,
    "eval.request": EvaluationRequest,
    "eval.result": EvaluationResult,
}


def export_json_schemas() -> dict:
    return {name: model.model_json_schema() for name, model in SCHEMAS.items()}
