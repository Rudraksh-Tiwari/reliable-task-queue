"""Shared test fixtures.

Every test that touches Redis gets its OWN namespace, so tests are fully
isolated from each other and from any real queue running on the same
Redis. That is what makes the suite safe to run against a developer's
local instance and safe to run in parallel later.
"""
import uuid

import pytest
import redis as redis_lib

from taskq.broker import Broker
from taskq.config import Config
from taskq.redis_client import make_redis


def make_test_config(namespace: str) -> Config:
    # Deliberately short timings so lease expiry and retry backoff are
    # observable within a test's runtime instead of the 30s production
    # defaults. Everything else mirrors the real configuration.
    return Config(
        redis_host="localhost",
        redis_port=6379,
        redis_db=0,
        namespace=namespace,
        lease_seconds=2.0,
        reaper_interval=0.5,
        poll_interval=0.05,
        retry_base=0.1,
        retry_cap=0.5,
        retry_jitter=0.0,   # deterministic delays keep timing assertions stable
        idem_ttl_seconds=60,
        heartbeat_interval=1.0,
    )


@pytest.fixture(scope="session")
def redis_available():
    """Skip the Redis-backed tests cleanly when no broker is running."""
    conn = make_redis(make_test_config("probe"))
    try:
        conn.ping()
    except redis_lib.RedisError:
        pytest.skip("Redis is not reachable on localhost:6379")
    return True


@pytest.fixture
def config(redis_available):
    return make_test_config(f"taskq-test-{uuid.uuid4().hex[:10]}")


@pytest.fixture
def conn(config):
    return make_redis(config)


@pytest.fixture
def broker(conn, config):
    b = Broker(conn, config)
    yield b
    # Tear down only this test's namespace. Never FLUSHDB - the same Redis
    # may hold a developer's real queues.
    keys = list(conn.scan_iter(f"{config.namespace}:*", count=500))
    if keys:
        conn.delete(*keys)
