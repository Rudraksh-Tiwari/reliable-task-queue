"""Load test: measure real throughput and latency.

    python -m taskq.loadtest --jobs 10000 --workers 4 --concurrency 8

Uses the `noop` task so the numbers describe the QUEUE's overhead (claim,
acknowledge, metrics) rather than whatever work a real handler would do.
Runs in its own namespace and never touches your real queues.
"""
import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from .broker import Broker
from .config import load_config
from .job import Job
from .redis_client import make_redis

REPO_ROOT = Path(__file__).resolve().parents[1]


def cleanup(conn, namespace: str) -> None:
    batch = []
    for key in conn.scan_iter(f"{namespace}:*", count=1000):
        batch.append(key)
        if len(batch) >= 1000:
            conn.delete(*batch)
            batch = []
    if batch:
        conn.delete(*batch)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="taskq.loadtest")
    parser.add_argument("--jobs", type=int, default=10000)
    parser.add_argument("--workers", type=int, default=4,
                        help="worker PROCESSES to run")
    parser.add_argument("--concurrency", type=int, default=8,
                        help="threads per worker process")
    args = parser.parse_args(argv)

    # Isolated namespace + a long lease so nothing is spuriously recovered
    # while the queue is saturated and jobs wait behind others.
    env = {
        "TQ_NAMESPACE": "taskq-load",
        "TQ_LEASE_SECONDS": "120",
        "TQ_REAPER_INTERVAL": "10",
        "TQ_POLL_INTERVAL": "0.05",
        "PYTHONIOENCODING": "utf-8",
    }
    os.environ.update(env)

    config = load_config()
    conn = make_redis(config)
    try:
        conn.ping()
    except Exception:
        print(f"Cannot reach Redis at {config.redis_host}:{config.redis_port}.")
        print("Start it with: docker compose up -d")
        return 1

    cleanup(conn, config.namespace)
    broker = Broker(conn, config)

    print(f"Load test: {args.jobs:,} jobs | {args.workers} worker "
          f"process(es) x {args.concurrency} threads")
    print("-" * 60)

    # ---- enqueue ----
    started = time.monotonic()
    for _ in range(args.jobs):
        broker.enqueue(Job(type="noop"))
    enqueue_elapsed = time.monotonic() - started
    print(f"enqueued  {args.jobs:,} jobs in {enqueue_elapsed:6.2f}s  "
          f"= {args.jobs / enqueue_elapsed:9,.0f} jobs/sec")

    # ---- process ----
    started = time.monotonic()
    procs = [
        subprocess.Popen(
            [sys.executable, "-m", "taskq.worker", "--burst",
             "--concurrency", str(args.concurrency)],
            cwd=REPO_ROOT, env=os.environ.copy(),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(args.workers)
    ]
    for proc in procs:
        proc.wait()
    process_elapsed = time.monotonic() - started

    counters = broker.metrics.counters()
    latency = broker.metrics.latency()
    done = counters["succeeded"]

    print(f"processed {done:,} jobs in {process_elapsed:6.2f}s  "
          f"= {done / process_elapsed:9,.0f} jobs/sec")
    print("-" * 60)
    print(f"  mean job execution   : {latency['avg_ms']:.3f} ms")
    print(f"  p95 job execution    : <= {latency['p95_ms']:.0f} ms")
    print(f"  remaining in queue   : {broker.queue_depth()}")
    print(f"  failed / dead        : {counters['failed']} / {counters['dead']}")

    ok = done == args.jobs and broker.queue_depth() == 0
    print("-" * 60)
    print(f"  every job accounted for: {'YES' if ok else 'NO'}")

    cleanup(conn, config.namespace)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
