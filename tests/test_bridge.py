from cardibridge.contracts import AgentChallenge, TraceContext
from cardibridge.registry import ContractRegistry
from cardibridge.router import BridgeRouter
from cardibridge.schemas import SCHEMAS


def registry():
    r = ContractRegistry()
    for name, model in SCHEMAS.items():
        r.register(name, model)
    return r


def test_contract_roundtrip():
    r = registry()
    obj = AgentChallenge(
        challenge_type="ischemia",
        population=[{"species": "mouse", "n": 10}],
        intended_task="benchmark",
        trace=TraceContext(source="CardiAgent"),
    )
    report = r.validate("agent.challenge", obj.model_dump())
    assert report.valid
    assert len(r.fingerprint("agent.challenge")) == 64


def test_router_idempotency():
    r = registry()
    router = BridgeRouter(r)
    seen = []
    router.register("agent.challenge", "CardiVex", lambda e: seen.append(e.message_id) or "ok")
    obj = AgentChallenge(
        challenge_type="ischemia",
        population=[{"n": 1}],
        intended_task="detect",
        trace=TraceContext(source="CardiAgent"),
    )
    env = r.wrap(name="agent.challenge", producer="CardiAgent", consumer="CardiVex",
                 payload=obj, idempotency_key="abc", trace=obj.trace)
    assert router.dispatch(env) == "ok"
    assert router.dispatch(env)["status"] == "duplicate"
    assert len(seen) == 1
