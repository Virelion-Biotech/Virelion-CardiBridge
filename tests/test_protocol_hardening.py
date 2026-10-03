import binascii
import math

import pytest

from cardibridge import (
    AgentChallenge,
    BridgeEnvelope,
    DeadLetter,
    DeadLetterQueue,
    EnvelopeCodec,
    RetryPolicy,
    TraceContext,
)
from cardibridge.builtin import default_registry
from cardibridge.conformance import ConformanceCase, run_conformance
from cardibridge.contracts import EvaluationResult, Prediction
from cardibridge.health import health


def make_envelope() -> BridgeEnvelope:
    trace = TraceContext(source="test")
    payload = AgentChallenge(
        challenge_type="mi",
        population=[{"sample": "S1"}],
        intended_task="classify",
        trace=trace,
    ).model_dump(mode="json")
    return BridgeEnvelope(
        message_type="agent.challenge",
        producer="CardiAgent",
        consumer="CardiVex",
        idempotency_key="idem-1",
        payload=payload,
        trace=trace,
    )


def test_envelope_codec_round_trip() -> None:
    envelope = make_envelope()
    encoded = EnvelopeCodec.encode(envelope)
    assert isinstance(encoded, bytes)
    assert EnvelopeCodec.decode(encoded) == envelope
    assert EnvelopeCodec.decode_base64(EnvelopeCodec.encode_base64(envelope)) == envelope


def test_codec_rejects_non_finite_json_constants() -> None:
    with pytest.raises(ValueError, match="non-finite"):
        EnvelopeCodec.decode('{"value":NaN}')


def test_retry_policy_is_bounded() -> None:
    policy = RetryPolicy(max_attempts=5, base_delay_seconds=2, max_delay_seconds=5, jitter=0)
    assert policy.delay(1) == 2
    assert policy.delay(3) == 5


def test_dlq_lifecycle() -> None:
    queue = DeadLetterQueue()
    envelope = make_envelope()
    queue.put(DeadLetter(envelope=envelope, reason="handler failure"))
    assert len(queue) == 1
    assert queue.get(envelope.message_id) is not None
    assert queue.remove(envelope.message_id) is not None
    assert len(queue) == 0


def test_conformance_and_health() -> None:
    registry = default_registry()
    result = run_conformance(
        registry,
        [ConformanceCase("valid-agent", "agent.challenge", make_envelope().payload)],
    )
    assert result.passed
    snapshot = health(registry)
    assert snapshot.status == "ok"
    assert snapshot.contracts >= 4


def test_invalid_base64_rejected() -> None:
    with pytest.raises(binascii.Error):
        EnvelopeCodec.decode_base64("not-valid-base64")


def test_predictions_and_metrics_reject_non_finite_values() -> None:
    with pytest.raises(ValueError):
        Prediction(
            target="target",
            value=1,
            probability=math.nan,
            model_id="model",
            model_version="1.0.0",
        )
    with pytest.raises(ValueError):
        EvaluationResult(
            evaluation_id="eval",
            metrics={"auroc": math.inf},
            trace=TraceContext(source="test"),
        )
