from cardibridge import (
    AgentChallenge,
    EvaluationRequest,
    EvaluationResult,
    ProductionRouter,
    TraceContext,
    VexObservation,
    default_registry,
)
from cardibridge.defaults import AGENT_CHALLENGE, EVAL_REQUEST, EVAL_RESULT, VEX_OBSERVATION


def test_canonical_agent_vex_eval_flow_is_routable():
    registry = default_registry()
    router = ProductionRouter(registry)
    seen: list[str] = []

    def vex_handler(envelope):
        seen.append(envelope.message_type)
        return {"observation": "accepted"}

    def eval_handler(envelope):
        seen.append(envelope.message_type)
        return {"evaluation": "accepted"}

    router.register(AGENT_CHALLENGE, "CardiVex", vex_handler)
    router.register(EVAL_REQUEST, "CardiEval", eval_handler)

    trace = TraceContext(source="CardiAgent")
    challenge = AgentChallenge(
        challenge_type="myocardial_injury",
        population=[{"sample": "synthetic-001"}],
        intended_task="detect injury phenotype",
        trace=trace,
    )
    challenge_env = registry.wrap(
        name=AGENT_CHALLENGE,
        producer="CardiAgent",
        consumer="CardiVex",
        payload=challenge,
        idempotency_key="challenge-001",
        trace=trace,
    )
    assert router.dispatch(challenge_env)["observation"] == "accepted"

    observation = VexObservation(
        challenge_type="myocardial_injury",
        severity=0.7,
        phenotype={"injury_zone": "infarct"},
        confidence=0.92,
        trace=TraceContext(source="CardiVex", parent_span_id=trace.span_id),
    )
    assert registry.validate(VEX_OBSERVATION, observation.model_dump()).valid

    request = EvaluationRequest(
        task="injury_detection",
        predictions=[{"target": "injury", "value": True, "probability": 0.9, "model_id": "m1", "model_version": "1"}],
        metrics=["auroc"],
        split="external",
        trace=TraceContext(source="CardiVex"),
    )
    eval_env = registry.wrap(
        name=EVAL_REQUEST,
        producer="CardiVex",
        consumer="CardiEval",
        payload=request,
        idempotency_key="eval-001",
        trace=request.trace,
    )
    assert router.dispatch(eval_env)["evaluation"] == "accepted"
    assert seen == [AGENT_CHALLENGE, EVAL_REQUEST]


def test_result_contract_is_registered_and_valid():
    registry = default_registry()
    result = EvaluationResult(
        evaluation_id="eval-001",
        metrics={"auroc": 0.91},
        trace=TraceContext(source="CardiEval"),
    )
    report = registry.validate(EVAL_RESULT, result.model_dump())
    assert report.valid
