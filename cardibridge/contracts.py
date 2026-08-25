from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class TraceContext(StrictModel):
    trace_id: str = Field(default_factory=lambda: uuid4().hex)
    span_id: str = Field(default_factory=lambda: uuid4().hex[:16])
    parent_span_id: str | None = None
    source: str
    schema_version: str = "1.0.0"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    provenance: dict[str, Any] = Field(default_factory=dict)


class VexObservation(StrictModel):
    observation_id: str = Field(default_factory=lambda: uuid4().hex)
    challenge_type: str
    severity: float = Field(ge=0, le=1)
    phenotype: dict[str, Any]
    evidence: list[dict[str, Any]] = Field(default_factory=list)
    confidence: float = Field(ge=0, le=1)
    trace: TraceContext


class AgentChallenge(StrictModel):
    challenge_id: str = Field(default_factory=lambda: uuid4().hex)
    challenge_type: str
    population: list[dict[str, Any]] = Field(min_length=1)
    constraints: dict[str, Any] = Field(default_factory=dict)
    intended_task: str
    trace: TraceContext


class Prediction(StrictModel):
    target: str
    value: Any
    probability: float | None = Field(default=None, ge=0, le=1)
    uncertainty: dict[str, Any] = Field(default_factory=dict)
    model_id: str
    model_version: str


class EvaluationRequest(StrictModel):
    evaluation_id: str = Field(default_factory=lambda: uuid4().hex)
    task: str
    predictions: list[Prediction] = Field(min_length=1)
    ground_truth: list[Any] | None = None
    metrics: list[str] = Field(default_factory=list)
    split: Literal["train", "validation", "test", "external", "unknown"] = "unknown"
    trace: TraceContext


class EvaluationResult(StrictModel):
    evaluation_id: str
    metrics: dict[str, float]
    uncertainty: dict[str, Any] = Field(default_factory=dict)
    errors: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    reproducibility: dict[str, Any] = Field(default_factory=dict)
    trace: TraceContext


class ValidationReport(StrictModel):
    valid: bool
    schema: str
    schema_version: str
    errors: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class BridgeEnvelope(StrictModel):
    message_id: str = Field(default_factory=lambda: uuid4().hex)
    message_type: str
    producer: str
    consumer: str
    idempotency_key: str
    payload: dict[str, Any]
    trace: TraceContext
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    signature: str | None = None

    @field_validator("idempotency_key")
    @classmethod
    def nonempty_key(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("idempotency_key must not be empty")
        return value
