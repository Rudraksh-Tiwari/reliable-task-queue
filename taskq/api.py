"""Read-and-operate HTTP layer for the dashboard.

Deliberately thin: it reads broker/metrics state and exposes the two
operator actions the dead-letter queue needs (replay, discard). It runs no
jobs itself, which is the point - the dashboard is an OBSERVER, so it can
be restarted, scaled, or killed without touching job processing at all.

Run:  python -m taskq.api          (then open http://127.0.0.1:8000)
"""
import argparse
import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse

from .broker import Broker
from .config import load_config
from .metrics import LATENCY_BUCKETS_MS
from .redis_client import make_redis

log = logging.getLogger("taskq.api")

_STATIC_DIR = Path(__file__).resolve().parent / "static"

app = FastAPI(title="taskq dashboard", docs_url="/api/docs")

# Set by main(); a module-level singleton keeps one Redis connection pool
# for the whole process instead of reconnecting per request.
_broker: Broker | None = None


def get_broker() -> Broker:
    global _broker
    if _broker is None:
        config = load_config()
        _broker = Broker(make_redis(config), config,
                         queue_name=_queue_name)
    return _broker


_queue_name = "default"


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    return (_STATIC_DIR / "dashboard.html").read_text(encoding="utf-8")


@app.get("/api/stats")
def stats() -> dict:
    return get_broker().stats()


@app.get("/api/throughput")
def throughput(window: int = 300) -> dict:
    # Clamp: the window drives one Redis round-trip per bucket, so an
    # unbounded value from the query string would let a caller issue a huge
    # pipeline. Validate at the edge.
    window = max(30, min(window, 1800))
    return {"window": window,
            "points": get_broker().metrics.throughput(window)}


@app.get("/api/dead")
def dead(limit: int = 50) -> dict:
    limit = max(1, min(limit, 500))
    broker = get_broker()
    return {"total": broker.dead_count(), "jobs": broker.dead_jobs(limit)}


@app.post("/api/dead/{job_id}/replay")
def replay(job_id: str) -> dict:
    outcome = get_broker().replay_dead(job_id)
    if outcome == "not_found":
        # Most often a double-click: another request already replayed it.
        # 404 is honest - this id is no longer on the dead list.
        raise HTTPException(404, "job is not in the dead-letter queue")
    if outcome == "payload_missing":
        raise HTTPException(409, "job payload no longer exists")
    return {"status": outcome, "id": job_id}


@app.post("/api/dead/replay-all")
def replay_all() -> dict:
    return get_broker().replay_all_dead()


@app.delete("/api/dead/{job_id}")
def discard(job_id: str) -> dict:
    if not get_broker().discard_dead(job_id):
        raise HTTPException(404, "job is not in the dead-letter queue")
    return {"status": "discarded", "id": job_id}


@app.get("/metrics", response_class=PlainTextResponse)
def prometheus_metrics() -> str:
    """Prometheus exposition format, written by hand.

    The format is deliberately simple text - HELP/TYPE lines then samples -
    so a client library buys nothing here except a dependency, and would
    reintroduce the per-process state problem that metrics.py avoids by
    aggregating in Redis.
    """
    broker = get_broker()
    snapshot = broker.stats()
    q = snapshot["queue"]
    lines: list[str] = []

    def add(name: str, help_text: str, mtype: str, samples: list[tuple]):
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {mtype}")
        for labels, value in samples:
            label_str = ",".join(f'{k}="{v}"' for k, v in labels.items())
            lines.append(f"{name}{{{label_str}}} {value}")

    add("taskq_queue_depth", "Jobs waiting in each priority lane.", "gauge",
        [({"queue": q, "priority": p}, n)
         for p, n in snapshot["lanes"].items()])
    add("taskq_jobs", "Jobs by current state.", "gauge",
        [({"queue": q, "state": state}, snapshot[state])
         for state in ("ready", "inflight", "scheduled", "dead")])
    add("taskq_jobs_total", "Cumulative job outcomes.", "counter",
        [({"queue": q, "outcome": name}, value)
         for name, value in snapshot["counters"].items()])
    add("taskq_workers", "Workers currently heartbeating.", "gauge",
        [({"queue": q}, len(snapshot["workers"]))])

    latency = snapshot["latency"]
    # A real Prometheus histogram: cumulative _bucket series plus _count.
    # (_sum is emitted from the running total we already track.)
    lines.append("# HELP taskq_job_duration_ms Job execution time.")
    lines.append("# TYPE taskq_job_duration_ms histogram")
    for bound in LATENCY_BUCKETS_MS:
        lines.append(f'taskq_job_duration_ms_bucket{{queue="{q}",'
                     f'le="{bound}"}} {latency["buckets"][bound]}')
    lines.append(f'taskq_job_duration_ms_bucket{{queue="{q}",le="+Inf"}} '
                 f'{latency["count"]}')
    lines.append(f'taskq_job_duration_ms_sum{{queue="{q}"}} '
                 f'{latency["avg_ms"] * latency["count"]:.3f}')
    lines.append(f'taskq_job_duration_ms_count{{queue="{q}"}} '
                 f'{latency["count"]}')
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    global _queue_name
    parser = argparse.ArgumentParser(prog="taskq.api")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--queue", default="default")
    args = parser.parse_args(argv)
    _queue_name = args.queue

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    import uvicorn
    log.info("dashboard on http://%s:%d (queue '%s')",
             args.host, args.port, args.queue)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
