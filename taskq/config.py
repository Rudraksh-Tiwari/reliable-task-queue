"""Application configuration.

All settings come from the environment (12-factor style), so the exact same
code runs unchanged across local / docker / prod — only the env vars differ.
"""
import os
from dataclasses import dataclass

from dotenv import load_dotenv

# Load a local .env file if one exists. Real deployments inject config straight
# through the environment; .env is purely a local-dev convenience.
load_dotenv()


@dataclass(frozen=True)
class Config:
    redis_host: str
    redis_port: int
    redis_db: int
    # Every Redis key we create is prefixed with this namespace. It lets a
    # single Redis instance be shared across apps without key collisions, and
    # makes our keys easy to find/flush in isolation (e.g. KEYS "taskq:*").
    namespace: str

    # -- reliability knobs (Stage 2) --
    # How long a claimed job may run before it is presumed crashed and its
    # lease can be reaped. MUST exceed your slowest job's runtime, or healthy
    # long jobs get "recovered" while still running and execute twice.
    # (Real systems refine this with worker heartbeats - a later stage.)
    lease_seconds: float
    # How often each worker's reaper thread sweeps for expired leases and
    # due retries. Recovery latency is at most lease_seconds + this.
    reaper_interval: float
    # Worker sleep between claim attempts when the queue is empty. The atomic
    # claim is a script and scripts cannot block like BRPOP, so workers poll.
    poll_interval: float
    # Retry backoff: delay = min(cap, base * 2^(failures-1)), +/- jitter.
    retry_base: float
    retry_cap: float
    retry_jitter: float

    # -- scale & scheduling knobs (Stage 3) --
    # How long an idempotency key suppresses duplicates, even if the job
    # somehow never reaches a terminal state (crash between reserving the
    # key and writing the job). A TTL'd dedup WINDOW is the honest promise
    # real systems make (e.g. Stripe: 24h) - not dedup-forever.
    idem_ttl_seconds: int
    # How often a worker refreshes its presence key (dashboards use this).
    heartbeat_interval: float


def load_config() -> Config:
    return Config(
        redis_host=os.getenv("TQ_REDIS_HOST", "localhost"),
        redis_port=int(os.getenv("TQ_REDIS_PORT", "6379")),
        redis_db=int(os.getenv("TQ_REDIS_DB", "0")),
        namespace=os.getenv("TQ_NAMESPACE", "taskq"),
        lease_seconds=float(os.getenv("TQ_LEASE_SECONDS", "30")),
        reaper_interval=float(os.getenv("TQ_REAPER_INTERVAL", "5")),
        poll_interval=float(os.getenv("TQ_POLL_INTERVAL", "0.5")),
        retry_base=float(os.getenv("TQ_RETRY_BASE", "2")),
        retry_cap=float(os.getenv("TQ_RETRY_CAP", "120")),
        retry_jitter=float(os.getenv("TQ_RETRY_JITTER", "0.25")),
        idem_ttl_seconds=int(os.getenv("TQ_IDEM_TTL", "86400")),
        heartbeat_interval=float(os.getenv("TQ_HEARTBEAT_INTERVAL", "5")),
    )
