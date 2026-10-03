from datetime import datetime, timezone

from cardibridge.defaults import default_registry


def _trace():
    return {
        "trace_id": "a" * 32,
        "span_id": "b" * 16,
        "source": "CardiBench",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def test_benchmark_evidence_contract():
    payload = {
        "observation_id": "obs-1",
        "kind": "dataset",
        "source": "geo",
        "source_record_id": "GSE1",
        "title": "Cardiac dataset",
        "identifiers": {"geo": "GSE1"},
        "evidence_state": "observed",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "trace": _trace(),
    }
    assert default_registry().validate("benchmark.evidence", payload).valid


def test_benchmark_result_contract():
    payload = {
        "result_id": "result-1",
        "benchmark_id": "mi-vs-reference",
        "benchmark_version": "1",
        "benchmark_provenance_sha256": "a" * 64,
        "model_id": "model",
        "model_version": "1",
        "split": "test",
        "metrics": {"auroc": 0.9},
        "sample_count": 10,
        "protocol_id": "cardieval-task",
        "source": "CardiEval/0.4",
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "trace": _trace(),
    }
    assert default_registry().validate("benchmark.result", payload).valid


def test_benchmark_admission_contract_ready():
    payload = {
        "assessment_id": "admission-1",
        "benchmark_id": "candidate",
        "benchmark_version": "1.0",
        "policy": "subject_heldout",
        "status": "ready_for_review",
        "ready_for_review": True,
        "blockers": [],
        "warnings": [],
        "required_fields": ["sample_id", "group_id", "study_id", "label"],
        "missing_by_field": {},
        "statistics": {"samples": 8, "groups": 8},
        "materialization_preview": {"metadata_sha256": "a" * 64},
        "trace": _trace(),
    }
    assert default_registry().validate("benchmark.admission", payload).valid


def test_benchmark_admission_contract_rejects_contradiction():
    payload = {
        "assessment_id": "admission-2",
        "benchmark_id": "candidate",
        "benchmark_version": "1.0",
        "policy": "subject_heldout",
        "status": "blocked",
        "ready_for_review": True,
        "blockers": ["missing group"],
        "trace": _trace(),
    }
    assert not default_registry().validate("benchmark.admission", payload).valid
