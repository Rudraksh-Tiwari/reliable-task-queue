"""Producer-facing queue API.

Deliberately tiny: a producer may add work and check depth - nothing else.
Everything worker-side (claim/ack/fail/recovery) lives in broker.py, so
producer code physically cannot touch the reliability machinery.

Stage 1's BRPOP-based dequeue() is gone on purpose: a naked pop hands the
job to a worker with no record anywhere that it exists - if that worker
dies, the job dies with it. Stage 2 replaced it with the broker's atomic
claim + lease, which is the crash-safe version of the same idea.
"""
import redis

from .broker import Broker
from .config import Config
from .job import Job


class Queue:
    def __init__(self, redis_conn: redis.Redis, config: Config,
                 name: str = "default"):
        self.name = name
        self._broker = Broker(redis_conn, config, queue_name=name)

    def enqueue(self, job: Job, *, delay_seconds: float | None = None,
                run_at: float | None = None) -> str:
        """Enqueue now, or later via delay_seconds / run_at (unix time).

        Priority and idempotency travel ON the job (job.priority,
        job.idempotency_key). Returns the live job id - if an idempotency
        key suppressed this enqueue, that's the EXISTING job's id.
        """
        return self._broker.enqueue(job, delay_seconds=delay_seconds,
                                    run_at=run_at)

    def size(self) -> int:
        return self._broker.queue_depth()
