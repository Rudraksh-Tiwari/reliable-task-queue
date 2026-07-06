"""The Job: the single unit of work that flows through the system."""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum


class JobStatus(str, Enum):
    # str mixin so each value serializes directly to a plain JSON string.
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD = "dead"  # exhausted all retries -> parked in the dead-letter queue


@dataclass
class Job:
    """A fully self-describing unit of work.

    Everything needed to execute the job lives *inside the job* (the task name
    plus its arguments) — never in a worker's memory. That is precisely what
    lets any worker, on any machine, pick up any job. It's the core property
    that makes the queue distributable rather than tied to one process.
    """

    type: str  # registered task name, e.g. "send_email"
    args: list = field(default_factory=list)
    kwargs: dict = field(default_factory=dict)

    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: str = JobStatus.QUEUED.value
    attempts: int = 0  # how many times execution has been attempted
    max_retries: int = 3  # retries allowed after the first attempt fails
    error: str | None = None  # last failure, e.g. "RuntimeError: boom"
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    def to_json(self) -> str:
        # JSON over pickle on purpose:
        #   - language-agnostic (a Go worker could read the same payload),
        #   - human-readable when debugging straight in redis-cli,
        #   - safe: deserializing JSON can't execute arbitrary code, whereas
        #     unpickling an attacker-controlled payload can.
        # Tradeoff: arguments must be JSON-serializable primitives — an
        # acceptable constraint for a job queue.
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str) -> "Job":
        return cls(**json.loads(raw))

    @classmethod
    def from_hash(cls, data: str, attempts: int | str = 0,
                  status: str | None = None, error: str | None = None) -> "Job":
        """Rebuild a Job from its Redis hash fields.

        The JSON 'data' blob is the immutable job description written once
        at enqueue time. Mutable execution state (attempts/status/error)
        lives in separate hash fields so Redis can update it atomically
        (HINCRBY) without parsing or rewriting JSON. The fields are the
        authoritative values, so they override whatever stale copies the
        blob still carries.
        """
        job = cls.from_json(data)
        job.attempts = int(attempts)
        if status:
            job.status = status
        if error:
            job.error = error
        return job

    def touch(self) -> None:
        """Refresh the modified timestamp whenever the job's state changes."""
        self.updated_at = time.time()
