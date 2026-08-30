"""Example tasks used for demos and verification.

Importing this module registers each function in the task registry (that's the
side effect of the @task decorator). The worker imports it so it knows how to
run these jobs.
"""
import time

from .config import load_config
from .redis_client import make_redis
from .registry import task


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
    r = make_redis(load_config())
    r.set(key, "1")
    return f"marker {key} set"


@task
def incr(key: str) -> int:
    # Increments a counter. The Stage 3 contention test enqueues one of
    # these per unique key across several competing workers: every counter
    # ending at exactly 1 proves no job was double-claimed OR lost.
    # (A fresh connection per call is fine at demo scale.)
    return make_redis(load_config()).incr(key)


@task
def flaky(gate_key: str, marker_key: str) -> str:
    # Fails while `gate_key` exists, succeeds once it's cleared. Models the
    # real dead-letter workflow: a job dies from a broken dependency, an
    # operator fixes the dependency, then replays the job from the
    # dashboard and it goes through.
    r = make_redis(load_config())
    if r.exists(gate_key):
        raise RuntimeError("dependency unavailable")
    r.set(marker_key, "1")
    return "completed after the fix"


@task
def record(label: str, list_key: str) -> int:
    # Appends a label to a results list - execution order made observable.
    # The priority test asserts all "high" labels precede all "normal", etc.
    return make_redis(load_config()).rpush(list_key, label)
