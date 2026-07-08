"""Stage 3 "Done when" verification - run: python -m taskq.verify_stage3

Proves the scale & scheduling features against real OS processes:

  A. CONTENTION SAFETY - 45 jobs, 3 competing worker processes x 4 threads
     each: every job runs EXACTLY once (no double-claims, no losses), and
     all 3 workers appear in the heartbeat registry.
  B. THROUGHPUT SCALING - the same batch of I/O-bound jobs, drained with
     concurrency 1 vs concurrency 8: the concurrent run must be
     substantially faster.
  C. PRIORITY LANES - low/normal/high jobs enqueued (in that order); a
     single-threaded worker must execute every high before every normal,
     and every normal before every low.
  D. DELAYED JOBS - enqueue_in(2s): the job must NOT run early, and must
     run once due (promoted by the reaper).
  E. IDEMPOTENCY - three enqueues sharing a key collapse into one job and
     one execution; after completion the key is released and a fresh
     enqueue works again.

Runs in an isolated namespace ("taskq-verify3") with short timings, so it
finishes in about a minute and never touches your real queues.
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

OVERRIDES = {
    "TQ_NAMESPACE": "taskq-verify3",
    "TQ_LEASE_SECONDS": "20",
    "TQ_REAPER_INTERVAL": "1",
    "TQ_POLL_INTERVAL": "0.1",
    "TQ_RETRY_BASE": "0.4",
    "TQ_RETRY_CAP": "2",
    "TQ_RETRY_JITTER": "0.1",
    "TQ_HEARTBEAT_INTERVAL": "1",
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
    """A real worker OS process."""

    def __init__(self, tag: str, log_dir: str, extra_args=()):
        self.log_path = Path(log_dir) / f"worker-{tag}.log"
        self._log_file = open(self.log_path, "w", encoding="utf-8")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "taskq.worker", *extra_args],
            cwd=REPO_ROOT,
            env=os.environ.copy(),
            stdout=self._log_file,
            stderr=subprocess.STDOUT,
        )

    def kill(self) -> None:
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


def run_burst(concurrency: int) -> float:
    """Run a burst worker to completion; return wall-clock seconds."""
    start = time.monotonic()
    proc = subprocess.run(
        [sys.executable, "-m", "taskq.worker", "--burst",
         "--concurrency", str(concurrency)],
        cwd=REPO_ROOT, env=os.environ.copy(),
        capture_output=True, text=True, timeout=120,
    )
    elapsed = time.monotonic() - start
    if proc.returncode != 0:
        print(proc.stdout[-2000:])
        raise RuntimeError(f"burst worker exited {proc.returncode}")
    return elapsed


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


def test_contention(broker, r, cfg, log_dir) -> bool:
    print("\n[A] contention: 45 jobs, 3 workers x 4 threads - every job "
          "exactly once")
    n = 45
    keys = [f"{cfg.namespace}:count:{i}" for i in range(n)]
    for key in keys:
        broker.enqueue(Job(type="incr", args=[key]))
    workers = [WorkerProc(f"contend-{i}", log_dir,
                          extra_args=("--concurrency", "4"))
               for i in range(3)]
    try:
        if not wait_for("all 45 counters to exist",
                        lambda: sum(r.exists(k) for k in keys) == n,
                        timeout=40, step=0.2):
            print("    worker 0 log:\n" + workers[0].tail())
            return False
        # Give any hypothetical duplicate execution a moment to land before
        # we assert exactly-once.
        time.sleep(1.0)
        values = [r.get(k) for k in keys]
        dupes = [k for k, v in zip(keys, values) if v != "1"]
        live = broker.live_workers()
        checks = [
            (not dupes, f"every counter is exactly 1 "
                        f"({len(dupes)} duplicates/losses)"),
            (broker.queue_depth() == 0, "all lanes drained"),
            (broker.inflight_count() == 0, "nothing left in-flight"),
            (broker.dead_count() == 0, "nothing dead-lettered"),
            (len(live) == 3, f"3 workers in heartbeat registry "
                             f"(got {len(live)})"),
        ]
        ok = all(c for c, _ in checks)
        for c, desc in checks:
            print(f"    {'ok' if c else 'FAIL'}: {desc}")
        if ok:
            print("    PASS: atomic claim holds under real multi-process "
                  "contention")
        return ok
    finally:
        for w in workers:
            w.kill()


def test_throughput(broker, cfg) -> bool:
    print("\n[B] throughput: 32 I/O-bound jobs, concurrency 1 vs 8")
    n, secs = 32, 0.25

    def load():
        for _ in range(n):
            broker.enqueue(Job(type="slow_task", kwargs={"seconds": secs}))

    load()
    t1 = run_burst(concurrency=1)
    print(f"    concurrency 1: {t1:5.1f}s for {n} x {secs}s jobs")
    load()
    t8 = run_burst(concurrency=8)
    print(f"    concurrency 8: {t8:5.1f}s for the same batch")
    speedup = t1 / t8 if t8 > 0 else 0
    ok = speedup >= 2.5
    print(f"    {'ok' if ok else 'FAIL'}: speedup {speedup:.1f}x "
          f"(needs >= 2.5x)")
    if ok:
        print("    PASS: one worker parallelizes I/O-bound jobs across "
              "threads")
    return ok


def test_priorities(broker, r, cfg, log_dir) -> bool:
    print("\n[C] priority lanes: highs must run before normals before lows")
    results_key = f"{cfg.namespace}:order:{uuid.uuid4().hex[:8]}"
    # Enqueue in REVERSE priority order to make the reordering undeniable.
    for prio in ("low", "normal", "high"):
        for _ in range(4):
            broker.enqueue(Job(type="record", args=[prio, results_key],
                               priority=prio))
    proc = subprocess.run(
        [sys.executable, "-m", "taskq.worker", "--burst"],
        cwd=REPO_ROOT, env=os.environ.copy(),
        capture_output=True, text=True, timeout=60,
    )
    if proc.returncode != 0:
        print(proc.stdout[-2000:])
        return False
    order = r.lrange(results_key, 0, -1)
    expected = ["high"] * 4 + ["normal"] * 4 + ["low"] * 4
    ok = order == expected
    print(f"    execution order: {' '.join(order)}")
    print(f"    {'ok' if ok else 'FAIL'}: strict priority ordering")
    if ok:
        print("    PASS: a single claim drains lanes in priority order")
    return ok


def test_delayed(broker, r, cfg, log_dir) -> bool:
    print("\n[D] delayed job: enqueue_in(2s) must not run early, must run "
          "when due")
    marker = f"{cfg.namespace}:delayed:{uuid.uuid4().hex[:8]}"
    t0 = time.monotonic()
    broker.enqueue(Job(type="slow_marker",
                       kwargs={"seconds": 0, "key": marker}),
                   delay_seconds=2.0)
    w = WorkerProc("delayed", log_dir)
    try:
        time.sleep(1.4)  # well inside the delay window
        if r.exists(marker):
            print("    FAIL: job executed before its delay elapsed")
            return False
        print("    ok: not executed at t+1.4s")
        if not wait_for("the delayed job to run once due",
                        lambda: r.get(marker) == "1", timeout=10, step=0.1):
            print("    worker log:\n" + w.tail())
            return False
        elapsed = time.monotonic() - t0
        ok = elapsed >= 1.9
        print(f"    {'ok' if ok else 'FAIL'}: executed at t+{elapsed:.1f}s "
              f"(delay was 2.0s, reaper granularity 1s)")
        if ok:
            print("    PASS: scheduled zset + reaper promotion delivers "
                  "delayed jobs")
        return ok
    finally:
        w.kill()


def test_idempotency(broker, r, cfg, log_dir) -> bool:
    print("\n[E] idempotency: 3 enqueues, 1 execution; key released after "
          "completion")
    counter = f"{cfg.namespace}:idem-count:{uuid.uuid4().hex[:8]}"
    idem = f"order-{uuid.uuid4().hex[:8]}"

    j1 = Job(type="incr", args=[counter], idempotency_key=idem)
    j2 = Job(type="incr", args=[counter], idempotency_key=idem)
    j3 = Job(type="incr", args=[counter], idempotency_key=idem)
    id1 = broker.enqueue(j1)
    id2 = broker.enqueue(j2)
    id3 = broker.enqueue(j3)

    checks = [
        (id1 == j1.id, "first enqueue accepted"),
        (id2 == id1 and id3 == id1,
         "duplicates suppressed (both returned the first job's id)"),
        (broker.queue_depth() == 1,
         f"exactly 1 job queued (got {broker.queue_depth()})"),
    ]
    proc = subprocess.run(
        [sys.executable, "-m", "taskq.worker", "--burst"],
        cwd=REPO_ROOT, env=os.environ.copy(),
        capture_output=True, text=True, timeout=60,
    )
    if proc.returncode != 0:
        print(proc.stdout[-2000:])
        return False
    checks.append((r.get(counter) == "1",
                   f"ran exactly once (counter={r.get(counter)})"))

    # Terminal state reached -> the slot must be free for deliberate re-use.
    j4 = Job(type="incr", args=[counter], idempotency_key=idem)
    id4 = broker.enqueue(j4)
    checks.append((id4 == j4.id, "key released after completion; "
                                 "fresh enqueue accepted"))
    subprocess.run([sys.executable, "-m", "taskq.worker", "--burst"],
                   cwd=REPO_ROOT, env=os.environ.copy(),
                   capture_output=True, text=True, timeout=60)
    checks.append((r.get(counter) == "2",
                   f"re-enqueued job ran (counter={r.get(counter)})"))

    ok = all(c for c, _ in checks)
    for c, desc in checks:
        print(f"    {'ok' if c else 'FAIL'}: {desc}")
    if ok:
        print("    PASS: dedup while active, released on terminal state")
    return ok


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

    print(f"Stage 3 verification (namespace '{cfg.namespace}')")
    log_dir = tempfile.mkdtemp(prefix="taskq-verify3-")

    results = {}
    tests = [
        ("A. contention (exactly-once under 3x4 workers)",
         lambda: test_contention(broker, r, cfg, log_dir)),
        ("B. throughput scaling (concurrency 1 vs 8)",
         lambda: test_throughput(broker, cfg)),
        ("C. priority lanes (high > normal > low)",
         lambda: test_priorities(broker, r, cfg, log_dir)),
        ("D. delayed jobs (enqueue_in 2s)",
         lambda: test_delayed(broker, r, cfg, log_dir)),
        ("E. idempotency keys (3 enqueues -> 1 run)",
         lambda: test_idempotency(broker, r, cfg, log_dir)),
    ]
    for name, fn in tests:
        cleanup_namespace(r, cfg.namespace)
        broker = Broker(r, cfg)
        results[name] = fn()
    cleanup_namespace(r, cfg.namespace)

    print("\n" + "=" * 58)
    for name, ok in results.items():
        print(f"  {name:<47}: {'PASS' if ok else 'FAIL'}")
    print(f"  worker logs: {log_dir}")
    print("=" * 58)
    if all(results.values()):
        print("Stage 3 verification passed.")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
