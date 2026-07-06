"""Stage 2 "Done when" verification - run: python -m taskq.verify_stage2

Proves the two reliability guarantees against real OS processes:

  A. CRASH RECOVERY - enqueue a slow job, hard-kill the worker process
     mid-execution (the Windows equivalent of `kill -9`), start a fresh
     worker, and prove the job still runs to completion. Zero jobs lost.

  B. RETRY -> DEAD-LETTER - enqueue a job that always raises, and prove it
     is retried with backoff until its budget is spent, then parked in the
     dead-letter queue with attempts and the original error preserved.

Runs in an isolated namespace ("taskq-verify") with short lease/backoff
settings, so it finishes in ~30s and never touches your real queues.
"""
import os
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

from .broker import Broker
from .config import load_config
from .job import Job
from .redis_client import make_redis

REPO_ROOT = Path(__file__).resolve().parents[1]

# Short timings so lease expiry and backoff are observable in seconds, and a
# dedicated namespace so cleanup can never touch real data. Set via env so
# the spawned worker processes inherit identical settings.
OVERRIDES = {
    "TQ_NAMESPACE": "taskq-verify",
    "TQ_LEASE_SECONDS": "5",
    "TQ_REAPER_INTERVAL": "1",
    "TQ_POLL_INTERVAL": "0.2",
    "TQ_RETRY_BASE": "0.4",
    "TQ_RETRY_CAP": "2",
    "TQ_RETRY_JITTER": "0.1",
    "PYTHONIOENCODING": "utf-8",
}


def wait_for(desc, predicate, timeout, step=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(step)
    print(f"      TIMEOUT waiting for: {desc}")
    return False


class WorkerProc:
    """A real worker OS process whose death we control."""

    def __init__(self, tag: str, log_dir: str):
        self.log_path = Path(log_dir) / f"worker-{tag}.log"
        self._log_file = open(self.log_path, "w", encoding="utf-8")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "taskq.worker"],
            cwd=REPO_ROOT,
            env=os.environ.copy(),
            stdout=self._log_file,
            stderr=subprocess.STDOUT,
        )

    def hard_kill(self) -> None:
        # Popen.kill() is TerminateProcess on Windows / SIGKILL on POSIX:
        # the process gets no chance to clean up. A genuine crash - the
        # same as `kill -9`.
        try:
            self.proc.kill()
            self.proc.wait(timeout=10)
        except Exception:
            pass
        if not self._log_file.closed:
            self._log_file.close()

    def tail(self, n: int = 25) -> str:
        try:
            lines = self.log_path.read_text(encoding="utf-8").splitlines()
            return "\n".join(lines[-n:])
        except OSError:
            return "<no log captured>"


def cleanup_namespace(r, ns: str) -> None:
    # Delete only OUR namespace's keys via SCAN. Never FLUSHDB: the Redis
    # instance may be shared with real queues or other apps.
    batch = []
    for key in r.scan_iter(f"{ns}:*", count=500):
        batch.append(key)
        if len(batch) >= 500:
            r.delete(*batch)
            batch = []
    if batch:
        r.delete(*batch)


def test_crash_recovery(broker, r, cfg, log_dir) -> bool:
    print("\n[A] crash recovery: hard-kill a worker mid-job; "
          "the job must still complete")
    marker = f"{cfg.namespace}:marker:{uuid.uuid4().hex[:8]}"
    job = Job(type="slow_marker", kwargs={"seconds": 3, "key": marker})
    broker.enqueue(job)
    print(f"    enqueued slow job {job.id} (3s of 'work')")

    w1 = WorkerProc("crash-victim", log_dir)
    killed = False
    try:
        if not wait_for("worker 1 to claim the job",
                        lambda: broker.inflight_count() == 1, 15):
            print("    worker 1 log:\n" + w1.tail())
            return False
        time.sleep(0.5)  # let it get genuinely mid-execution
        if r.exists(marker):
            print("    job finished before the kill - test invalid")
            return False
        w1.hard_kill()
        killed = True
        print(f"    hard-killed worker 1 (pid {w1.proc.pid}) mid-execution")
    finally:
        if not killed:
            w1.hard_kill()

    if broker.inflight_count() != 1:
        print("    FAIL: expected the job stranded in-flight under the dead "
              "worker's lease")
        return False
    print(f"    job is stranded in-flight; lease expires in "
          f"~{cfg.lease_seconds:.0f}s")

    w2 = WorkerProc("rescuer", log_dir)
    try:
        if not wait_for("a fresh worker to recover and finish the job",
                        lambda: r.get(marker) == "1", timeout=30, step=0.2):
            print("    worker 2 log:\n" + w2.tail())
            return False
        if not wait_for("the recovered job to be acked and cleaned up",
                        lambda: (broker.inflight_count() == 0
                                 and not r.exists(broker.job_key(job.id))),
                        timeout=10):
            return False
        print("    PASS: job survived a hard worker kill and ran to "
              "completion on another worker")
        return True
    finally:
        w2.hard_kill()


def test_retry_then_dlq(broker, r, cfg, log_dir) -> bool:
    print("\n[B] retry + dead-letter: an always-failing job must retry with "
          "backoff, then be buried")
    job = Job(type="always_fail", args=["boom"], max_retries=2)
    broker.enqueue(job)
    print(f"    enqueued always-failing job {job.id} "
          f"(max_retries=2 -> 3 total attempts)")

    w = WorkerProc("dlq", log_dir)
    try:
        if not wait_for("the job to land in the dead-letter queue",
                        lambda: broker.dead_count() == 1, timeout=30,
                        step=0.2):
            print("    worker log:\n" + w.tail())
            return False
    finally:
        w.hard_kill()

    dead = broker.load_job(job.id)
    checks = [
        (dead is not None,
         "dead job payload still inspectable"),
        (dead is not None and dead.status == "dead",
         f"status is 'dead' (got {getattr(dead, 'status', None)!r})"),
        (dead is not None and dead.attempts == 3,
         f"exactly 3 attempts recorded (got {getattr(dead, 'attempts', None)})"),
        (dead is not None and "boom" in (dead.error or ""),
         "original error preserved in the DLQ"),
        (broker.dead_ids() == [job.id],
         "dead-letter list holds exactly this job"),
    ]
    all_ok = True
    for ok, desc in checks:
        print(f"    {'ok' if ok else 'FAIL'}: {desc}")
        all_ok = all_ok and bool(ok)
    if all_ok:
        print("    PASS: retries exhausted -> dead-letter with full forensics")
    return all_ok


def main() -> int:
    os.environ.update(OVERRIDES)  # must precede load_config()

    cfg = load_config()
    r = make_redis(cfg)
    try:
        r.ping()
    except Exception:
        print(f"Cannot reach Redis at {cfg.redis_host}:{cfg.redis_port}.")
        print("Is Docker Desktop running? Start it, then: docker compose up -d")
        return 1

    print(f"Stage 2 verification (namespace '{cfg.namespace}', "
          f"lease={cfg.lease_seconds:.0f}s, reaper every "
          f"{cfg.reaper_interval:.0f}s)")

    log_dir = tempfile.mkdtemp(prefix="taskq-verify-")
    cleanup_namespace(r, cfg.namespace)
    broker = Broker(r, cfg)

    result_a = test_crash_recovery(broker, r, cfg, log_dir)
    cleanup_namespace(r, cfg.namespace)
    result_b = test_retry_then_dlq(broker, r, cfg, log_dir)
    cleanup_namespace(r, cfg.namespace)

    print("\n" + "=" * 58)
    print(f"  A. crash recovery (kill -9)   : "
          f"{'PASS' if result_a else 'FAIL'}")
    print(f"  B. retry -> dead-letter queue : "
          f"{'PASS' if result_b else 'FAIL'}")
    print(f"  worker logs: {log_dir}")
    print("=" * 58)
    if result_a and result_b:
        print("Stage 2 verification passed.")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
