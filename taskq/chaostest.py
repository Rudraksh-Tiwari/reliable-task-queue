"""Chaos test: hard-kill workers at random and prove no job is lost.

    python -m taskq.chaostest --jobs 200 --workers 4 --kills 8

Each job increments its own counter, so afterwards:
  * every counter existing        => ZERO LOSS (the guarantee)
  * a counter above 1             => that job ran twice

Duplicates are not failures. This system provides at-least-once delivery,
which explicitly permits a job to run again if a worker dies after doing
the work but before acknowledging it. The test measures how often that
actually happens rather than pretending it cannot.
"""
import argparse
import os
import random
import subprocess
import sys
import time
from pathlib import Path

from .broker import Broker
from .config import load_config
from .job import Job
from .redis_client import make_redis

REPO_ROOT = Path(__file__).resolve().parents[1]


class WorkerPool:
    """A fixed-size pool of real worker processes we can kill at will."""

    def __init__(self, size: int, concurrency: int):
        self.concurrency = concurrency
        self.procs: list[subprocess.Popen] = []
        for _ in range(size):
            self.procs.append(self._spawn())

    def _spawn(self) -> subprocess.Popen:
        return subprocess.Popen(
            [sys.executable, "-m", "taskq.worker",
             "--concurrency", str(self.concurrency)],
            cwd=REPO_ROOT, env=os.environ.copy(),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def kill_one(self) -> int:
        """SIGKILL a random worker and replace it. No graceful shutdown."""
        victim = random.choice(self.procs)
        pid = victim.pid
        victim.kill()          # TerminateProcess / SIGKILL - no cleanup runs
        victim.wait(timeout=10)
        self.procs.remove(victim)
        self.procs.append(self._spawn())
        return pid

    def shutdown(self) -> None:
        for proc in self.procs:
            try:
                proc.kill()
                proc.wait(timeout=10)
            except Exception:
                pass


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
    parser = argparse.ArgumentParser(prog="taskq.chaostest")
    parser.add_argument("--jobs", type=int, default=200)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--kills", type=int, default=8)
    parser.add_argument("--kill-every", type=float, default=1.5,
                        help="seconds between kills")
    parser.add_argument("--job-seconds", type=float, default=0.1,
                        help="how long each job takes (so kills land mid-run)")
    args = parser.parse_args(argv)

    # A short lease means a killed worker's jobs are recovered quickly, so
    # the test finishes fast. In production the lease must exceed the
    # slowest job instead.
    os.environ.update({
        "TQ_NAMESPACE": "taskq-chaos",
        "TQ_LEASE_SECONDS": "3",
        "TQ_REAPER_INTERVAL": "0.5",
        "TQ_POLL_INTERVAL": "0.05",
        "TQ_RETRY_BASE": "0.2",
        "TQ_RETRY_CAP": "1",
        "PYTHONIOENCODING": "utf-8",
    })

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

    print(f"Chaos test: {args.jobs} jobs | {args.workers} workers x "
          f"{args.concurrency} threads | {args.kills} random kills")
    print("-" * 62)

    keys = [f"{config.namespace}:ran:{i}" for i in range(args.jobs)]
    for key in keys:
        # A generous retry budget: attempts are counted at CLAIM time, so
        # every kill consumes one. Without headroom, a repeatedly-unlucky
        # job would be dead-lettered for the crash rather than for failing.
        broker.enqueue(Job(type="chaos_job",
                           args=[key, args.job_seconds],
                           max_retries=50))
    print(f"enqueued {args.jobs} jobs")

    pool = WorkerPool(args.workers, args.concurrency)
    started = time.monotonic()
    kills_done = 0
    try:
        # Kill workers on a timer while the queue drains.
        while kills_done < args.kills:
            time.sleep(args.kill_every)
            if broker.queue_depth() == 0 and broker.inflight_count() == 0:
                break
            pid = pool.kill_one()
            kills_done += 1
            print(f"  [{time.monotonic() - started:5.1f}s] killed worker "
                  f"pid {pid:<6} (kill {kills_done}/{args.kills}) - "
                  f"queue {broker.queue_depth()}, "
                  f"in-flight {broker.inflight_count()}")

        # Let the survivors finish and the reaper rescue the last casualties.
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            done = sum(1 for k in keys if conn.exists(k))
            if done == args.jobs:
                break
            time.sleep(0.25)
    finally:
        pool.shutdown()

    elapsed = time.monotonic() - started
    values = [conn.get(k) for k in keys]
    missing = [k for k, v in zip(keys, values) if v is None]
    duplicated = [(k, int(v)) for k, v in zip(keys, values)
                  if v is not None and int(v) > 1]
    extra_runs = sum(count - 1 for _, count in duplicated)

    print("-" * 62)
    print(f"  workers killed        : {kills_done}")
    print(f"  elapsed               : {elapsed:.1f}s")
    print(f"  jobs completed        : {args.jobs - len(missing)}/{args.jobs}")
    print(f"  JOBS LOST             : {len(missing)}")
    print(f"  jobs run more than once: {len(duplicated)} "
          f"({extra_runs} extra execution(s))")
    print(f"  dead-lettered         : {broker.dead_count()}")
    if duplicated[:3]:
        print("  sample duplicates     : "
              + ", ".join(f"{k.rsplit(':', 1)[-1]}x{c}"
                          for k, c in duplicated[:3]))
    print("-" * 62)

    passed = not missing
    if passed:
        rate = 100 * len(duplicated) / args.jobs
        print(f"PASS - zero job loss across {kills_done} hard worker kills.")
        print(f"       {rate:.1f}% of jobs ran twice, which is the expected "
              f"cost of at-least-once delivery.")
    else:
        print(f"FAIL - {len(missing)} job(s) were lost.")

    cleanup(conn, config.namespace)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
