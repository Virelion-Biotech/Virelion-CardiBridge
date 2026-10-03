from cardibridge import (
    AgentChallenge,
    EvaluationRequest,
    EvaluationResult,
    BenchmarkResultRecord,
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


def test_cardibench_result_contract_preserves_evaluator_source():
    result = BenchmarkResultRecord(
        result_id="result-001",
        benchmark_id="mi-vs-reference",
        benchmark_version="1.0",
        benchmark_provenance_sha256="a" * 64,
        model_id="model-1",
        model_version="1.0",
        split="test",
        metrics={"auroc": 0.91},
        sample_count=20,
        protocol_id="binary-cardiac-state-detection",
        source="CardiEval/0.4",
        recorded_at=TraceContext(source="test").created_at,
        trace=TraceContext(source="CardiBench"),
    )
    report = default_registry().validate("benchmark.result", result.model_dump(mode="json"))
    assert report.valid
    assert result.source == "CardiEval/0.4"
