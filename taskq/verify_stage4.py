"""Stage 4 "Done when" verification - run: python -m taskq.verify_stage4

Starts the real dashboard server and drives it over HTTP:

  A. METRICS - run a mixed batch of jobs; the API must report accurate
     fleet-wide counters, latency stats, and a throughput time series.
  B. DASHBOARD - "/" serves the dashboard page.
  C. PROMETHEUS - "/metrics" exposes valid exposition-format series.
  D. DLQ REPLAY - a job dies from a broken dependency; fix the dependency,
     replay it through the API, and it completes.
  E. DOUBLE-CLICK SAFETY - replaying the same dead job twice requeues it
     exactly once (the atomic LREM gate in replay.lua).

Runs in an isolated namespace ("taskq-verify4") and never touches your
real queues.
"""
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

from .broker import Broker
from .config import load_config
from .job import Job
from .redis_client import make_redis

REPO_ROOT = Path(__file__).resolve().parents[1]
PORT = 8077
BASE = f"http://127.0.0.1:{PORT}"

OVERRIDES = {
    "TQ_NAMESPACE": "taskq-verify4",
    "TQ_LEASE_SECONDS": "20",
    "TQ_REAPER_INTERVAL": "1",
    "TQ_POLL_INTERVAL": "0.1",
    "TQ_RETRY_BASE": "0.3",
    "TQ_RETRY_CAP": "1",
    "TQ_RETRY_JITTER": "0.1",
    "TQ_HEARTBEAT_INTERVAL": "1",
    "PYTHONIOENCODING": "utf-8",
}


def http(path: str, method: str = "GET", raw: bool = False):
    req = urllib.request.Request(BASE + path, method=method)
    with urllib.request.urlopen(req, timeout=10) as resp:
        body = resp.read().decode("utf-8")
        return body if raw else json.loads(body)


def wait_for(desc, predicate, timeout, step=0.15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return True
        except Exception:
            pass
        time.sleep(step)
    print(f"      TIMEOUT waiting for: {desc}")
    return False


class Proc:
    def __init__(self, tag, log_dir, args):
        self.log_path = Path(log_dir) / f"{tag}.log"
        self._log = open(self.log_path, "w", encoding="utf-8")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", *args], cwd=REPO_ROOT,
            env=os.environ.copy(), stdout=self._log,
            stderr=subprocess.STDOUT)

    def kill(self):
        try:
            self.proc.kill()
            self.proc.wait(timeout=10)
        except Exception:
            pass
        if not self._log.closed:
            self._log.close()

    def tail(self, n=25):
        try:
            return "\n".join(
                self.log_path.read_text(encoding="utf-8").splitlines()[-n:])
        except OSError:
            return "<no log>"


def cleanup_namespace(r, ns):
    batch = []
    for key in r.scan_iter(f"{ns}:*", count=500):
        batch.append(key)
        if len(batch) >= 500:
            r.delete(*batch)
            batch = []
    if batch:
        r.delete(*batch)


def run_burst(concurrency=4, timeout=90):
    subprocess.run(
        [sys.executable, "-m", "taskq.worker", "--burst",
         "--concurrency", str(concurrency)],
        cwd=REPO_ROOT, env=os.environ.copy(),
        capture_output=True, text=True, timeout=timeout)


def test_metrics(broker, r, cfg) -> bool:
    print("\n[A] metrics: counters, latency, and throughput via the API")
    for _ in range(12):
        broker.enqueue(Job(type="slow_task", kwargs={"seconds": 0.05}))
    for _ in range(3):
        broker.enqueue(Job(type="always_fail", args=["metrics demo"],
                           max_retries=0))
    run_burst()

    stats = http("/api/stats")
    c = stats["counters"]
    lat = stats["latency"]
    tp = http("/api/throughput?window=300")
    active = [p for p in tp["points"] if p["succeeded"] or p["failed"]]

    checks = [
        (c["succeeded"] == 12, f"12 succeeded recorded (got {c['succeeded']})"),
        (c["failed"] == 3, f"3 failed recorded (got {c['failed']})"),
        (c["dead"] == 3, f"3 dead recorded (got {c['dead']})"),
        (lat["count"] == 12, f"12 latency samples (got {lat['count']})"),
        (40 <= lat["avg_ms"] <= 400,
         f"avg latency plausible for 50ms jobs (got {lat['avg_ms']:.0f}ms)"),
        (lat["p95_ms"] > 0, f"p95 estimated (got {lat['p95_ms']}ms)"),
        (len(active) >= 1,
         f"throughput series has data ({len(active)} active bucket(s))"),
        (sum(p["succeeded"] for p in tp["points"]) == 12,
         "throughput buckets sum to 12 successes"),
        (stats["dead"] == 3, f"3 jobs in the DLQ (got {stats['dead']})"),
    ]
    ok = all(c_ for c_, _ in checks)
    for c_, desc in checks:
        print(f"    {'ok' if c_ else 'FAIL'}: {desc}")
    if ok:
        print("    PASS: fleet-wide metrics aggregate correctly in Redis")
    return ok


def test_dashboard() -> bool:
    print("\n[B] dashboard page")
    html = http("/", raw=True)
    checks = [
        ("<title>taskq" in html, "page served with the taskq title"),
        ("/api/stats" in html, "page polls the stats API"),
        ("replay-all" in html, "page exposes the replay-all control"),
        ("cdn" not in html.lower(),
         "self-contained (no external CDN dependency)"),
    ]
    ok = all(c for c, _ in checks)
    for c, desc in checks:
        print(f"    {'ok' if c else 'FAIL'}: {desc}")
    if ok:
        print("    PASS: dashboard renders from the server")
    return ok


def test_prometheus() -> bool:
    print("\n[C] prometheus exposition endpoint")
    text = http("/metrics", raw=True)
    required = [
        "# TYPE taskq_queue_depth gauge",
        "# TYPE taskq_jobs_total counter",
        "# TYPE taskq_job_duration_ms histogram",
        "taskq_job_duration_ms_bucket{",
        'le="+Inf"',
        "taskq_workers{",
    ]
    checks = [(token in text, f"exposes {token!r}") for token in required]
    # Every non-comment line must be "name{labels} value" - a malformed
    # line would make a real Prometheus scrape drop the whole payload.
    bad = [ln for ln in text.splitlines()
           if ln and not ln.startswith("#") and len(ln.rsplit(" ", 1)) != 2]
    checks.append((not bad, f"all sample lines well-formed ({len(bad)} bad)"))
    ok = all(c for c, _ in checks)
    for c, desc in checks:
        print(f"    {'ok' if c else 'FAIL'}: {desc}")
    if ok:
        print("    PASS: scrapeable by Prometheus without a client library")
    return ok


def test_replay(broker, r, cfg, log_dir) -> bool:
    print("\n[D] dead-letter replay: break a dependency, fix it, replay")
    gate = f"{cfg.namespace}:gate:{uuid.uuid4().hex[:8]}"
    marker = f"{cfg.namespace}:done:{uuid.uuid4().hex[:8]}"
    r.set(gate, "1")  # dependency is "down"
    job = Job(type="flaky", args=[gate, marker], max_retries=1)
    broker.enqueue(job)

    w = Proc("replay-worker", log_dir, ["taskq.worker"])
    try:
        if not wait_for("the job to exhaust retries and die",
                        lambda: broker.dead_count() >= 1, 25):
            print("    worker log:\n" + w.tail())
            return False
        print(f"    ok: job died after {broker.load_job(job.id).attempts} "
              f"attempts (dependency down)")

        listing = http("/api/dead")
        shown = [j for j in listing["jobs"] if j["id"] == job.id]
        if not shown:
            print("    FAIL: dead job not listed by the API")
            return False
        print(f"    ok: API lists it with error {shown[0]['error']!r}")

        r.delete(gate)  # operator fixes the dependency
        out = http(f"/api/dead/{job.id}/replay", method="POST")
        if out.get("status") != "replayed":
            print(f"    FAIL: replay returned {out}")
            return False
        print("    ok: replayed via the API")

        if not wait_for("the replayed job to complete",
                        lambda: r.get(marker) == "1", 25):
            print("    worker log:\n" + w.tail())
            return False
        replayed_job_attempts = 1
        checks = [
            (broker.dead_count() == 0, "DLQ is empty again"),
            (http("/api/stats")["counters"]["replayed"] == 1,
             "replay counted in metrics"),
        ]
        ok = all(c for c, _ in checks)
        for c, desc in checks:
            print(f"    {'ok' if c else 'FAIL'}: {desc}")
        if ok:
            print("    PASS: operator can recover dead jobs from the UI")
        return ok
    finally:
        w.kill()


def test_double_replay(broker, r, cfg) -> bool:
    print("\n[E] double-click safety: replaying twice must requeue once")
    job = Job(type="always_fail", args=["dlq"], max_retries=0)
    broker.enqueue(job)
    run_burst(concurrency=1)
    if broker.dead_count() != 1:
        print(f"    FAIL: expected 1 dead job, got {broker.dead_count()}")
        return False

    first = http(f"/api/dead/{job.id}/replay", method="POST")
    depth_after_first = broker.queue_depth()
    try:
        http(f"/api/dead/{job.id}/replay", method="POST")
        second_status = 200
    except urllib.error.HTTPError as e:
        second_status = e.code
    depth_after_second = broker.queue_depth()

    checks = [
        (first.get("status") == "replayed", "first replay succeeded"),
        (depth_after_first == 1, f"queued exactly once (got {depth_after_first})"),
        (second_status == 404,
         f"second replay rejected with 404 (got {second_status})"),
        (depth_after_second == 1,
         f"still queued exactly once (got {depth_after_second})"),
    ]
    ok = all(c for c, _ in checks)
    for c, desc in checks:
        print(f"    {'ok' if c else 'FAIL'}: {desc}")
    if ok:
        print("    PASS: the atomic LREM gate prevents duplicate requeues")
    return ok


def main() -> int:
    os.environ.update(OVERRIDES)

    cfg = load_config()
    r = make_redis(cfg)
    try:
        r.ping()
    except Exception:
        print(f"Cannot reach Redis at {cfg.redis_host}:{cfg.redis_port}.")
        print("Is Docker Desktop running? Start it, then: docker compose up -d")
        return 1

    print(f"Stage 4 verification (namespace '{cfg.namespace}', API on {BASE})")
    log_dir = tempfile.mkdtemp(prefix="taskq-verify4-")
    cleanup_namespace(r, cfg.namespace)

    api = Proc("api", log_dir, ["taskq.api", "--port", str(PORT)])
    results = {}
    try:
        if not wait_for("the dashboard API to come up",
                        lambda: http("/api/stats") is not None, 30):
            print("    api log:\n" + api.tail())
            return 1
        print("[ok] dashboard API is up")

        broker = Broker(r, cfg)
        results["A. metrics (counters/latency/throughput)"] = \
            test_metrics(broker, r, cfg)
        results["B. dashboard page served"] = test_dashboard()
        results["C. prometheus /metrics endpoint"] = test_prometheus()

        cleanup_namespace(r, cfg.namespace)
        broker = Broker(r, cfg)
        results["D. dead-letter replay via API"] = \
            test_replay(broker, r, cfg, log_dir)

        cleanup_namespace(r, cfg.namespace)
        broker = Broker(r, cfg)
        results["E. replay double-click safety"] = \
            test_double_replay(broker, r, cfg)
    finally:
        api.kill()
        cleanup_namespace(r, cfg.namespace)

    print("\n" + "=" * 60)
    for name, ok in results.items():
        print(f"  {name:<45}: {'PASS' if ok else 'FAIL'}")
    print(f"  logs: {log_dir}")
    print("=" * 60)
    if all(results.values()):
        print("Stage 4 verification passed.")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
