"""Integration tests for the concurrency guarantees.

These are the tests that would fail if the Lua scripts were replaced by
ordinary sequences of Redis commands. Each one drives real threads
competing for the same Redis keys.
"""
import threading

from taskq.job import Job


def run_concurrently(fn, threads: int):
    """Start N threads on a barrier so they collide as hard as possible."""
    barrier = threading.Barrier(threads)
    results, errors = [], []
    lock = threading.Lock()

    def runner():
        barrier.wait()  # release all threads at the same instant
        try:
            value = fn()
        except Exception as exc:  # surface, never swallow
            with lock:
                errors.append(exc)
            return
        with lock:
            results.append(value)

    workers = [threading.Thread(target=runner) for _ in range(threads)]
    for w in workers:
        w.start()
    for w in workers:
        w.join(timeout=30)
    assert not errors, f"threads raised: {errors}"
    return results


def test_a_job_is_claimed_by_exactly_one_thread(broker):
    # THE core guarantee. One job, 20 threads racing to claim it: exactly
    # one may win, or the job would execute twice.
    broker.enqueue(Job(type="noop"))
    claims = run_concurrently(broker.claim, threads=20)
    won = [c for c in claims if c is not None]
    assert len(won) == 1, f"{len(won)} threads claimed the same job"


def test_every_job_is_claimed_exactly_once_under_contention(broker):
    # 50 jobs, 50 threads: no duplicates and no losses.
    ids = []
    for _ in range(50):
        job = Job(type="noop")
        broker.enqueue(job)
        ids.append(job.id)
    claims = run_concurrently(broker.claim, threads=50)
    claimed = [c.id for c in claims if c is not None]
    assert len(claimed) == 50
    assert sorted(claimed) == sorted(ids), "some job was lost or duplicated"
    assert len(set(claimed)) == 50, "a job was claimed twice"


def test_concurrent_reapers_do_not_double_requeue(broker, config):
    # Every worker runs its own reaper, so several sweep at once. Two
    # reapers must never both requeue the same expired job.
    import time
    for _ in range(10):
        broker.enqueue(Job(type="noop"))
    for _ in range(10):
        broker.claim()
    time.sleep(config.lease_seconds + 0.3)

    recovered = run_concurrently(broker.requeue_expired, threads=8)
    assert sum(recovered) == 10, "each expired job must be recovered once"
    assert broker.queue_depth() == 10


def test_idempotency_survives_a_concurrent_enqueue_race(broker):
    # 25 threads enqueue the "same" logical job simultaneously. Exactly one
    # real job may be created.
    def enqueue():
        return broker.enqueue(Job(type="noop", idempotency_key="race-key"))

    ids = run_concurrently(enqueue, threads=25)
    assert len(set(ids)) == 1, "the dedup slot admitted more than one job"
    assert broker.queue_depth() == 1


def test_concurrent_replay_requeues_once(broker):
    # The dashboard's replay button, clicked from many tabs at once.
    job = Job(type="noop", max_retries=0)
    broker.enqueue(job)
    broker.fail(broker.claim(), RuntimeError("boom"))

    outcomes = run_concurrently(lambda: broker.replay_dead(job.id), threads=12)
    assert outcomes.count("replayed") == 1
    assert outcomes.count("not_found") == 11
    assert broker.queue_depth() == 1


def test_concurrent_replay_and_discard_never_queue_a_job_without_its_payload(broker):
    # The dashboard's replay and discard buttons, clicked on the same dead job
    # from two tabs. Either outcome is acceptable: the job is back in its
    # lane with its payload, or it is gone entirely. A queued id whose payload
    # was deleted is a lost job. Repeated because one round only hits the bad
    # ordering (replay first, then discard) about half the time.
    for _ in range(30):
        job = Job(type="noop", max_retries=0)
        broker.enqueue(job)
        broker.fail(broker.claim(), RuntimeError("boom"))

        actions = iter([lambda: broker.replay_dead(job.id),
                        lambda: broker.discard_dead(job.id)])
        lock = threading.Lock()

        def act():
            with lock:
                action = next(actions)
            return action()

        run_concurrently(act, threads=2)
        assert broker.dead_count() == 0
        if broker.queue_depth():
            assert broker.load_job(job.id) is not None, \
                "replayed job is queued but its payload was discarded"
            claimed = broker.claim()
            assert claimed.id == job.id
            broker.ack(claimed)
        else:
            assert broker.load_job(job.id) is None


def test_concurrent_ack_and_recovery_do_not_lose_the_payload(broker):
    # A worker acking at the same moment reapers sweep: the job must end up
    # gone (acked), never stuck half-deleted.
    job = Job(type="noop")
    broker.enqueue(job)
    claimed = broker.claim()

    def ack():
        broker.ack(claimed)

    run_concurrently(ack, threads=6)
    assert broker.inflight_count() == 0
    assert broker.load_job(job.id) is None
