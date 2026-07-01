"""Stage 0 smoke test — prove the foundation is wired together.

Verifies two things, and nothing more (no queue logic yet):
  1. We can reach Redis.
  2. A Job survives a full serialize -> store -> load -> deserialize round-trip.

Run (with Redis up):  python -m taskq.smoke
"""
from .config import load_config
from .job import Job
from .redis_client import make_redis


def main() -> int:
    config = load_config()
    redis_conn = make_redis(config)

    # 1. Can we reach Redis at all? PING is the cheapest possible liveness check.
    redis_conn.ping()
    print(f"[ok] connected to Redis at {config.redis_host}:{config.redis_port}")

    # 2. Jobs must move through Redis losslessly — that's the bedrock everything
    #    else (enqueue, claim, retry) is built on. Prove it with one round-trip.
    job = Job(type="send_email", args=["alice@example.com"])
    key = f"{config.namespace}:smoke:{job.id}"
    redis_conn.set(key, job.to_json())
    restored = Job.from_json(redis_conn.get(key))
    redis_conn.delete(key)  # leave Redis clean — this is only a test artifact.

    assert restored.id == job.id, "job id changed across the round-trip"
    assert restored.type == job.type, "job type changed across the round-trip"
    assert restored.args == job.args, "job args changed across the round-trip"
    print(f"[ok] round-tripped job {job.id} ({job.type}) through Redis")

    print("\nStage 0 smoke test passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
