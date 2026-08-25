from cardibridge import (
    BridgeEnvelope, CircuitBreaker, ContractCapability, ContractNegotiator,
    ProvenanceChain, TraceContext, validate_batch,
)


def envelope(message_id: str) -> BridgeEnvelope:
    return BridgeEnvelope(message_id=message_id, message_type="agent.challenge", producer="agent",
                          consumer="vex", idempotency_key=message_id, payload={"x": 1},
                          trace=TraceContext(source="agent"))


def test_provenance_chain_is_tamper_evident() -> None:
    chain = ProvenanceChain()
    chain.append("1", "agent.challenge", "agent", {"severity": 0.4})
    chain.append("2", "vex.observation", "vex", {"severity": 0.3})
    assert chain.verify()
    exported = chain.export()
    assert len(exported) == 2
    assert exported[1]["previous_digest"] == exported[0]["digest"]


def test_contract_negotiation_prefers_highest_common_version() -> None:
    result = ContractNegotiator().negotiate(
        ContractCapability("agent.challenge", ("1.0.0", "1.1.0")),
        ContractCapability("agent.challenge", ("1.0.0", "1.1.0", "2.0.0")),
    )
    assert result.version == "1.1.0"


def test_circuit_breaker_opens_after_threshold() -> None:
    breaker = CircuitBreaker(failure_threshold=2)
    breaker.failure()
    assert breaker.allow()
    breaker.failure()
    assert not breaker.allow()


def test_batch_rejects_duplicate_message_ids() -> None:
    result = validate_batch([envelope("a"), envelope("a"), envelope("b")])
    assert result.accepted == 2
    assert result.rejected == 1
