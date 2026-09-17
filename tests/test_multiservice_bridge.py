"""Multi-service contract exercise through CardiBridge only."""
from __future__ import annotations

from datetime import datetime, timezone

from cardibridge import (
    AgentChallenge,
    BridgeEnvelope,
    EvaluationRequest,
    EvaluationResult,
    Prediction,
    TraceContext,
    VexObservation,
)
from cardibridge.defaults import default_registry
from cardibridge.production import ProductionRouter
from cardibridge.store import EventStore


def _trace(src: str = "multiservice") -> TraceContext:
    return TraceContext(source=src)


def test_challenge_observe_eval_pipeline():
    reg = default_registry()
    store = EventStore()
    router = ProductionRouter(reg, store)

    observed: list[dict] = []
    evaluated: list[dict] = []

    def on_challenge(env: BridgeEnvelope) -> dict:
        obs = VexObservation(
            challenge_type=str(env.payload.get("challenge_type") or "unknown"),
            severity=0.4,
            phenotype={"cell": "cardiomyocyte"},
            evidence=[{"from_challenge": env.idempotency_key}],
            confidence=0.91,
            trace=_trace("cardivex"),
        )
        observed.append(obs.model_dump(mode="json"))
        return {"status": "observed", "observation_id": obs.observation_id}

    def on_observation(env: BridgeEnvelope) -> dict:
        req = EvaluationRequest(
            task="arrhythmia_risk",
            predictions=[
                Prediction(
                    target="arrhythmia_risk",
                    value=0.2,
                    probability=0.2,
                    model_id="baseline",
                    model_version="1.0.0",
                )
            ],
            ground_truth=[{"target": "arrhythmia_risk", "value": 0.0}],
            metrics=["mae"],
            split="test",
            trace=_trace("orchestrator"),
        )
        return {"status": "queued_eval", "request": req.model_dump(mode="json")}

    def on_eval_request(env: BridgeEnvelope) -> dict:
        result = EvaluationResult(
            evaluation_id=str(env.payload.get("evaluation_id") or env.message_id),
            metrics={"mae": 0.2},
            uncertainty={"mae": 0.05},
            reproducibility={"seed": 42},
            trace=_trace("cardieval"),
        )
        evaluated.append(result.model_dump(mode="json"))
        return result.model_dump(mode="json")

    router.register("agent.challenge", "cardivex", on_challenge)
    router.register("vex.observation", "orchestrator", on_observation)
    router.register("eval.request", "cardieval", on_eval_request)

    challenge = BridgeEnvelope(
        message_type="agent.challenge",
        producer="cardiagent",
        consumer="cardivex",
        idempotency_key="ms-challenge-1",
        payload=AgentChallenge(
            challenge_type="phenotype-shift",
            population=[{"cell": "cardiomyocyte"}],
            intended_task="multiservice-pipeline",
            trace=_trace("cardiagent"),
        ).model_dump(mode="json"),
        trace=_trace("cardiagent"),
        timestamp=datetime.now(timezone.utc),
    )
    r1 = router.dispatch(challenge)
    assert r1["status"] == "observed"
    assert len(observed) == 1

    r1b = router.dispatch(challenge)
    assert isinstance(r1b, dict) and r1b.get("status") == "duplicate"
    assert len(observed) == 1

    obs_env = BridgeEnvelope(
        message_type="vex.observation",
        producer="cardivex",
        consumer="orchestrator",
        idempotency_key="ms-obs-1",
        payload=observed[0],
        trace=_trace("cardivex"),
        timestamp=datetime.now(timezone.utc),
    )
    r2 = router.dispatch(obs_env)
    assert r2["status"] == "queued_eval"

    eval_env = BridgeEnvelope(
        message_type="eval.request",
        producer="orchestrator",
        consumer="cardieval",
        idempotency_key="ms-eval-1",
        payload=r2["request"],
        trace=_trace("orchestrator"),
        timestamp=datetime.now(timezone.utc),
    )
    r3 = router.dispatch(eval_env)
    assert "metrics" in r3
    assert r3["metrics"]["mae"] == 0.2
    assert len(evaluated) == 1

    replayed = list(store.replay(limit=10))
    assert len(replayed) >= 3
    ids = {env.message_id for _, env in replayed}
    assert challenge.message_id in ids
