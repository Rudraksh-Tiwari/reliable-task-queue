"""Stage 1 producer demo: enqueue a few jobs, then exit.

Two-terminal flow (the real experience):
    terminal 1:  python -m taskq.worker
    terminal 2:  python -m taskq.enqueue_demo

Notice this producer does NOT import example_tasks. A producer only needs the
task *name* (a string) to enqueue — it never needs the handler function. That
asymmetry is the whole point: producers and workers are decoupled through Redis.
"""
from .config import load_config
from .job import Job
from .queue import Queue
from .redis_client import make_redis


def main() -> int:
    config = load_config()
    queue = Queue(make_redis(config), config)

    jobs = [
        Job(type="send_email", args=["alice@example.com"], kwargs={"subject": "Welcome"}),
        Job(type="add", args=[2, 3]),
        Job(type="slow_task", kwargs={"seconds": 0.5}),
    ]

    for job in jobs:
        queue.enqueue(job)
        print(f"enqueued {job.id} ({job.type}); queue depth = {queue.size()}")

    print(f"\nenqueued {len(jobs)} jobs. Watch the worker terminal for output.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
