"""Example tasks used for demos and verification.

Importing this module registers each function in the task registry (that's the
side effect of the @task decorator). The worker imports it so it knows how to
run these jobs. All of these succeed on purpose — Stage 1 only covers the happy
path; failing tasks belong to the Stage 2 retry/dead-letter demo.
"""
import time

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
