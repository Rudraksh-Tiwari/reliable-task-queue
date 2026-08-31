"""Example tasks used for demos and verification.

Importing this module registers each function in the task registry (that's the
side effect of the @task decorator). The worker imports it so it knows how to
run these jobs.
"""
import time

from .config import load_config
from .redis_client import make_redis
from .registry import task

# One connection per worker process, created on first use. Opening a fresh
# connection inside every job would dominate the timings in the load test
# and exhaust file descriptors at high concurrency.
_conn = None


def _redis():
    global _conn
    if _conn is None:
        _conn = make_redis(load_config())
    return _conn


@task
def send_email(to: str, subject: str = "Hello") -> str:
    # Stand-in for real work (calling an email API, etc.).
    print(f"  [send_email] to={to!r} subject={subject!r}")
    return f"sent to {to}"


@task
def add(a: int, b: int) -> int:
    return a + b


@task(name="slow_task")
def slow(seconds: float = 1.0) -> str:
    # Simulates a job that takes a while — useful later for observing
    # concurrency and, in Stage 2, for killing a worker mid-execution.
    time.sleep(seconds)
    return f"slept {seconds}s"


@task
def always_fail(msg: str = "boom") -> None:
    # Exists to demonstrate the failure path end-to-end: retries with
    # exponential backoff, then burial in the dead-letter queue.
    raise RuntimeError(msg)


@task
def slow_marker(seconds: float, key: str) -> str:
    # Sleeps, then writes a marker key to Redis. Used by the crash test:
    # kill the worker mid-sleep, and the marker's later appearance proves
    # the job was recovered and re-run to completion by another worker.
    time.sleep(seconds)
    _redis().set(key, "1")
    return f"marker {key} set"


@task
def incr(key: str) -> int:
    # Increments a counter. The Stage 3 contention test enqueues one of
    # these per unique key across several competing workers: every counter
    # ending at exactly 1 proves no job was double-claimed OR lost.
    return _redis().incr(key)


@task
def noop() -> None:
    # Does nothing on purpose. The load test uses it so the measured
    # throughput reflects the QUEUE's overhead (claim, ack, metrics) rather
    # than whatever work a realistic handler would do.
    return None


@task
def chaos_job(key: str, seconds: float = 0.1) -> int:
    # Sleeps (so a worker can be killed mid-execution), then increments a
    # per-job counter. After a chaos run, every counter existing proves
    # ZERO LOSS; any counter above 1 is a duplicate execution, which
    # at-least-once delivery explicitly permits and the test quantifies.
    time.sleep(seconds)
    return _redis().incr(key)


@task
def flaky(gate_key: str, marker_key: str) -> str:
    # Fails while `gate_key` exists, succeeds once it's cleared. Models the
    # real dead-letter workflow: a job dies from a broken dependency, an
    # operator fixes the dependency, then replays the job from the
    # dashboard and it goes through.
    r = _redis()
    if r.exists(gate_key):
        raise RuntimeError("dependency unavailable")
    r.set(marker_key, "1")
    return "completed after the fix"


@task
def record(label: str, list_key: str) -> int:
    # Appends a label to a results list - execution order made observable.
    # The priority test asserts all "high" labels precede all "normal", etc.
    return _redis().rpush(list_key, label)
