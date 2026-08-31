"""Integration tests: every job state transition, against a real Redis."""
import time

import pytest

from taskq.job import Job


def test_enqueue_puts_the_job_in_its_priority_lane(broker):
    broker.enqueue(Job(type="noop", priority="high"))
    assert broker.lane_depths() == {"high": 1, "normal": 0, "low": 0}
    assert broker.queue_depth() == 1


def test_claim_moves_the_job_to_inflight_and_counts_the_attempt(broker):
    job = Job(type="noop")
    broker.enqueue(job)
    claimed = broker.claim()
    assert claimed.id == job.id
    assert claimed.attempts == 1, "attempts must be counted at claim time"
    assert broker.queue_depth() == 0
    assert broker.inflight_count() == 1


def test_claim_on_an_empty_queue_returns_none(broker):
    assert broker.claim() is None


def test_ack_releases_the_lease_and_deletes_the_payload(broker):
    job = Job(type="noop")
    broker.enqueue(job)
    broker.ack(broker.claim())
    assert broker.inflight_count() == 0
    assert broker.load_job(job.id) is None


def test_failure_with_budget_left_schedules_a_retry(broker):
    job = Job(type="noop", max_retries=2)
    broker.enqueue(job)
    outcome = broker.fail(broker.claim(), RuntimeError("boom"))
    assert outcome == "retried"
    assert broker.scheduled_count() == 1
    assert broker.inflight_count() == 0
    assert broker.dead_count() == 0


def test_failure_with_no_budget_left_buries_the_job(broker):
    job = Job(type="noop", max_retries=0)
    broker.enqueue(job)
    outcome = broker.fail(broker.claim(), RuntimeError("boom"))
    assert outcome == "dead"
    assert broker.dead_count() == 1
    dead = broker.load_job(job.id)
    assert dead.status == "dead"
    assert "boom" in dead.error, "the original error must survive for triage"


def test_expired_lease_is_recovered_to_the_queue(broker, config):
    # The core reliability property: a claimed job whose worker never acks
    # must come back on its own.
    job = Job(type="noop")
    broker.enqueue(job)
    broker.claim()
    assert broker.requeue_expired() == 0, "not due yet"
    time.sleep(config.lease_seconds + 0.3)
    assert broker.requeue_expired() == 1
    assert broker.queue_depth() == 1
    assert broker.inflight_count() == 0


def test_recovery_returns_the_job_to_its_own_priority_lane(broker, config):
    # Rescue must not silently demote work to the back of 'normal'.
    broker.enqueue(Job(type="noop", priority="high"))
    broker.claim()
    time.sleep(config.lease_seconds + 0.3)
    broker.requeue_expired()
    assert broker.lane_depths()["high"] == 1


def test_scheduled_retries_are_promoted_once_due(broker):
    broker.enqueue(Job(type="noop", max_retries=1))
    broker.fail(broker.claim(), RuntimeError("boom"))
    assert broker.promote_scheduled() == 0, "backoff has not elapsed"
    time.sleep(0.7)
    assert broker.promote_scheduled() == 1
    assert broker.queue_depth() == 1


def test_claim_drains_lanes_in_strict_priority_order(broker):
    for prio in ("low", "normal", "high"):
        broker.enqueue(Job(type="noop", priority=prio))
    order = [broker.claim().priority for _ in range(3)]
    assert order == ["high", "normal", "low"]


def test_delayed_job_waits_in_the_scheduled_set(broker):
    broker.enqueue(Job(type="noop"), delay_seconds=30)
    assert broker.queue_depth() == 0
    assert broker.scheduled_count() == 1


def test_delay_and_run_at_are_mutually_exclusive(broker):
    with pytest.raises(ValueError):
        broker.enqueue(Job(type="noop"), delay_seconds=5, run_at=time.time())


def test_unknown_priority_is_rejected(broker):
    with pytest.raises(ValueError, match="unknown priority"):
        broker.enqueue(Job(type="noop", priority="urgent"))


def test_idempotency_key_suppresses_duplicate_enqueues(broker):
    a = Job(type="noop", idempotency_key="order-42")
    b = Job(type="noop", idempotency_key="order-42")
    first = broker.enqueue(a)
    second = broker.enqueue(b)
    assert first == a.id
    assert second == a.id, "duplicate must return the ACTIVE job's id"
    assert broker.queue_depth() == 1


def test_idempotency_slot_is_released_on_success(broker):
    job = Job(type="noop", idempotency_key="order-43")
    broker.enqueue(job)
    broker.ack(broker.claim())
    fresh = Job(type="noop", idempotency_key="order-43")
    assert broker.enqueue(fresh) == fresh.id, "slot should be free again"


def test_idempotency_slot_is_released_when_the_job_dies(broker):
    job = Job(type="noop", idempotency_key="order-44", max_retries=0)
    broker.enqueue(job)
    broker.fail(broker.claim(), RuntimeError("boom"))
    fresh = Job(type="noop", idempotency_key="order-44")
    assert broker.enqueue(fresh) == fresh.id


def test_replay_returns_a_dead_job_with_a_fresh_budget(broker):
    job = Job(type="noop", priority="high", max_retries=0)
    broker.enqueue(job)
    broker.fail(broker.claim(), RuntimeError("boom"))
    assert broker.replay_dead(job.id) == "replayed"
    assert broker.dead_count() == 0
    assert broker.lane_depths()["high"] == 1, "must return to its own lane"
    assert broker.load_job(job.id).attempts == 0, "budget must be reset"


def test_replaying_twice_requeues_only_once(broker):
    # Two operators double-clicking replay must not run the job twice.
    job = Job(type="noop", max_retries=0)
    broker.enqueue(job)
    broker.fail(broker.claim(), RuntimeError("boom"))
    assert broker.replay_dead(job.id) == "replayed"
    assert broker.replay_dead(job.id) == "not_found"
    assert broker.queue_depth() == 1


def test_replay_reports_a_missing_payload_instead_of_queueing_it(broker):
    job = Job(type="noop", max_retries=0)
    broker.enqueue(job)
    broker.fail(broker.claim(), RuntimeError("boom"))
    broker.r.delete(broker.job_key(job.id))
    assert broker.replay_dead(job.id) == "payload_missing"
    assert broker.queue_depth() == 0, "an unrunnable id must not be queued"


def test_discard_removes_a_dead_job_permanently(broker):
    job = Job(type="noop", max_retries=0)
    broker.enqueue(job)
    broker.fail(broker.claim(), RuntimeError("boom"))
    assert broker.discard_dead(job.id) is True
    assert broker.dead_count() == 0
    assert broker.load_job(job.id) is None
    assert broker.discard_dead(job.id) is False


def test_dead_jobs_listing_carries_triage_detail(broker):
    job = Job(type="always_fail", args=["ctx"], max_retries=0)
    broker.enqueue(job)
    broker.fail(broker.claim(), RuntimeError("upstream down"))
    row = broker.dead_jobs()[0]
    assert row["id"] == job.id
    assert row["type"] == "always_fail"
    assert row["attempts"] == 1
    assert "upstream down" in row["error"]


def test_metrics_track_outcomes(broker):
    broker.enqueue(Job(type="noop"))
    broker.ack(broker.claim(), duration_seconds=0.05)
    broker.enqueue(Job(type="noop", max_retries=0))
    broker.fail(broker.claim(), RuntimeError("boom"))
    counters = broker.metrics.counters()
    assert counters["succeeded"] == 1
    assert counters["failed"] == 1
    assert counters["dead"] == 1
    assert broker.metrics.latency()["count"] == 1


def test_stats_snapshot_shape(broker):
    broker.enqueue(Job(type="noop"))
    snapshot = broker.stats()
    for key in ("queue", "lanes", "ready", "inflight", "scheduled", "dead",
                "counters", "latency", "workers"):
        assert key in snapshot
    assert snapshot["ready"] == 1
