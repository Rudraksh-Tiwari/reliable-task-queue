"""Seed a realistic mix of jobs so the dashboard has something to show.

    python -m taskq.demo_seed          # add jobs to the current state
    python -m taskq.demo_seed --reset  # wipe the queue first

Then run a worker and open the dashboard:
    python -m taskq.worker --concurrency 4
    python -m taskq.api
"""
import argparse
import random

from .broker import Broker
from .config import load_config
from .job import Job
from .redis_client import make_redis


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="taskq.demo_seed")
    parser.add_argument("--reset", action="store_true",
                        help="delete all jobs and metrics for this namespace")
    parser.add_argument("--jobs", type=int, default=40,
                        help="how many normal jobs to enqueue")
    args = parser.parse_args(argv)

    config = load_config()
    redis_conn = make_redis(config)
    broker = Broker(redis_conn, config)

    if args.reset:
        keys = list(redis_conn.scan_iter(f"{config.namespace}:*", count=500))
        if keys:
            redis_conn.delete(*keys)
        print(f"reset: cleared {len(keys)} key(s) in '{config.namespace}'")

    for _ in range(args.jobs):
        broker.enqueue(Job(
            type="slow_task",
            kwargs={"seconds": round(random.uniform(0.02, 0.3), 3)},
            priority=random.choice(["high", "normal", "normal", "low"]),
        ))
    # A few jobs that always fail, so the dead-letter queue and its replay
    # controls have something in them to demonstrate.
    for i in range(4):
        broker.enqueue(Job(type="always_fail",
                           args=[f"upstream timeout (batch {i})"],
                           max_retries=1))
    # Delayed jobs sit in the scheduled set, so that panel is non-zero.
    for _ in range(6):
        broker.enqueue(Job(type="slow_task", kwargs={"seconds": 0.1}),
                       delay_seconds=600)

    print(f"seeded {args.jobs} normal + 4 failing + 6 delayed jobs")
    print(f"lanes: {broker.lane_depths()} | scheduled: "
          f"{broker.scheduled_count()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
