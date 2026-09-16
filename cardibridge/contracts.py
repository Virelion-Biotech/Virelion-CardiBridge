from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


def _nonempty(value: str) -> str:
    if not value.strip():
        raise ValueError("value must not be empty")
    return value


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value


def _finite(value: float) -> float:
    if not math.isfinite(value):
        raise ValueError("value must be finite")
    return value


class TraceContext(StrictModel):
    trace_id: str = Field(default_factory=lambda: uuid4().hex)
    span_id: str = Field(default_factory=lambda: uuid4().hex[:16])
    parent_span_id: str | None = None
    source: str
    schema_version: str = "1.0.0"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    provenance: dict[str, Any] = Field(default_factory=dict)

    _validate_source = field_validator("source")(_nonempty)
    _validate_created_at = field_validator("created_at")(_aware)


class ArtifactRef(StrictModel):
    """Stable reference to a scientific artifact without embedding its bytes."""

    artifact_id: str = Field(default_factory=lambda: uuid4().hex)
    uri: str
    media_type: str = "application/octet-stream"
    digest: str | None = None
    size_bytes: int | None = Field(default=None, ge=0)
    producer: str
    version: str = "1.0.0"
    metadata: dict[str, Any] = Field(default_factory=dict)

    _validate_text = field_validator("artifact_id", "uri", "media_type", "producer", "version")(
        _nonempty
    )


class LineageFacet(StrictModel):
    name: str
    value: dict[str, Any]
    version: str = "1.0.0"

    _validate_text = field_validator("name", "version")(_nonempty)


class LineageEvent(StrictModel):
    """Portable run/job/input/output lineage record."""

    event_id: str = Field(default_factory=lambda: uuid4().hex)
    run_id: str
    job_namespace: str
    job_name: str
    event_type: Literal["START", "RUNNING", "COMPLETE", "FAIL"]
    producer: str
    event_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    inputs: list[ArtifactRef] = Field(default_factory=list)
    outputs: list[ArtifactRef] = Field(default_factory=list)
    facets: list[LineageFacet] = Field(default_factory=list)

    _validate_text = field_validator(
        "event_id", "run_id", "job_namespace", "job_name", "producer"
    )(_nonempty)
    _validate_event_time = field_validator("event_time")(_aware)

    def to_dict(self) -> dict[str, Any]:
        return {
            "eventType": self.event_type,
            "eventTime": self.event_time.isoformat(),
            "producer": self.producer,
            "eventId": self.event_id,
            "run": {"runId": self.run_id},
            "job": {"namespace": self.job_namespace, "name": self.job_name},
            "inputs": [item.model_dump(mode="json") for item in self.inputs],
            "outputs": [item.model_dump(mode="json") for item in self.outputs],
            "facets": {
                facet.name: {"_version": facet.version, **facet.value} for facet in self.facets
            },
        }


class ExecutionContext(StrictModel):
    """Workflow/task identity propagated across services."""

    execution_id: str = Field(default_factory=lambda: uuid4().hex)
    workflow_id: str
    task_id: str
    attempt: int = Field(default=1, ge=1)
    status: Literal["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"] = "PENDING"
    parent_execution_id: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)
    environment: dict[str, str] = Field(default_factory=dict)

    _validate_text = field_validator("execution_id", "workflow_id", "task_id")(_nonempty)
    _validate_started = field_validator("started_at")(_aware)
    _validate_completed = field_validator("completed_at")(_aware)

    @model_validator(mode="after")
    def validate_timestamps(self) -> ExecutionContext:
        if self.started_at and self.completed_at and self.completed_at < self.started_at:
            raise ValueError("completed_at cannot precede started_at")
        return self


class VexObservation(StrictModel):
    observation_id: str = Field(default_factory=lambda: uuid4().hex)
    challenge_type: str
    severity: float = Field(ge=0, le=1)
    phenotype: dict[str, Any]
    evidence: list[dict[str, Any]] = Field(default_factory=list)
    confidence: float = Field(ge=0, le=1)
    trace: TraceContext

    _validate_text = field_validator("observation_id", "challenge_type")(_nonempty)
    _validate_numbers = field_validator("severity", "confidence")(_finite)


class AgentChallenge(StrictModel):
    challenge_id: str = Field(default_factory=lambda: uuid4().hex)
    challenge_type: str
    population: list[dict[str, Any]] = Field(min_length=1)
    constraints: dict[str, Any] = Field(default_factory=dict)
    intended_task: str
    trace: TraceContext

    _validate_text = field_validator("challenge_id", "challenge_type", "intended_task")(_nonempty)


class Prediction(StrictModel):
    target: str
    value: Any
    probability: float | None = Field(default=None, ge=0, le=1)
    uncertainty: dict[str, Any] = Field(default_factory=dict)
    model_id: str
    model_version: str

    _validate_text = field_validator("target", "model_id", "model_version")(_nonempty)
    _validate_probability = field_validator("probability")(_finite)


class EvaluationRequest(StrictModel):
    evaluation_id: str = Field(default_factory=lambda: uuid4().hex)
    task: str
    predictions: list[Prediction] = Field(min_length=1)
    ground_truth: list[Any] | None = None
    metrics: list[str] = Field(default_factory=list)
    split: Literal["train", "validation", "test", "external", "unknown"] = "unknown"
    artifacts: list[ArtifactRef] = Field(default_factory=list)
    execution: ExecutionContext | None = None
    trace: TraceContext

    _validate_text = field_validator("evaluation_id", "task")(_nonempty)


class EvaluationResult(StrictModel):
    evaluation_id: str
    metrics: dict[str, float]
    uncertainty: dict[str, Any] = Field(default_factory=dict)
    errors: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    reproducibility: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[ArtifactRef] = Field(default_factory=list)
    execution: ExecutionContext | None = None
    trace: TraceContext

    _validate_text = field_validator("evaluation_id")(_nonempty)

    @field_validator("metrics")
    @classmethod
    def validate_metrics(cls, value: dict[str, float]) -> dict[str, float]:
        for name, metric in value.items():
            if not name.strip():
                raise ValueError("metric names must not be empty")
            _finite(metric)
        return value


class ValidationReport(StrictModel):
    valid: bool
    schema_name: str = Field(alias="schema", serialization_alias="schema")
    schema_version: str
    errors: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        populate_by_name=True,
        serialize_by_alias=True,
    )

    @property
    def schema(self) -> str:  # type: ignore[override]
        return self.schema_name


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
    execution: ExecutionContext | None = None
    artifact_refs: list[ArtifactRef] = Field(default_factory=list)

    _validate_text = field_validator(
        "message_id", "message_type", "producer", "consumer", "idempotency_key"
    )(_nonempty)
    _validate_timestamp = field_validator("timestamp")(_aware)

    @field_validator("idempotency_key")
    @classmethod
    def normalize_key(cls, value: str) -> str:
        return value.strip()
