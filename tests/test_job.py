"""Unit tests for the Job model and its serialization contract."""
import json

from taskq.job import Job, JobStatus


def test_round_trip_preserves_every_field():
    job = Job(type="send_email", args=["a@example.com"],
              kwargs={"subject": "Hi"}, priority="high",
              idempotency_key="order-1", max_retries=5)
    restored = Job.from_json(job.to_json())
    assert restored.id == job.id
    assert restored.type == job.type
    assert restored.args == job.args
    assert restored.kwargs == job.kwargs
    assert restored.priority == job.priority
    assert restored.idempotency_key == job.idempotency_key
    assert restored.max_retries == job.max_retries


def test_serializes_to_plain_json():
    # The payload must be readable by any language and safe to parse -
    # that is the whole reason it is JSON rather than pickle.
    payload = json.loads(Job(type="add", args=[1, 2]).to_json())
    assert payload["type"] == "add"
    assert payload["args"] == [1, 2]


def test_each_job_gets_a_unique_id():
    assert len({Job(type="x").id for _ in range(500)}) == 500


def test_defaults_are_sane():
    job = Job(type="x")
    assert job.status == JobStatus.QUEUED.value
    assert job.attempts == 0
    assert job.priority == "normal"
    assert job.idempotency_key is None
    assert job.error is None


def test_from_hash_overrides_stale_blob_state():
    # The JSON blob is written once at enqueue time and never rewritten, so
    # its attempts/status are stale by design. The hash fields are the
    # authoritative values and must win.
    job = Job(type="x")
    restored = Job.from_hash(job.to_json(), attempts=7, status="running",
                             error="boom")
    assert restored.attempts == 7
    assert restored.status == "running"
    assert restored.error == "boom"


def test_from_hash_accepts_string_attempts():
    # Redis returns everything as strings; the model must coerce.
    restored = Job.from_hash(Job(type="x").to_json(), attempts="3")
    assert restored.attempts == 3
