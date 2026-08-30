"""Fleet-wide metrics, aggregated in Redis.

WHY REDIS AND NOT A PROMETHEUS CLIENT LIBRARY:
The usual client libraries keep counters in PROCESS memory. This system
runs many worker processes (often on many machines), so in-process
counters would each describe one worker and nothing would describe the
fleet. Scraping would also hit whichever worker the load balancer picked,
so the numbers would jump around at random, and every counter would reset
whenever a worker restarted.

Counting in Redis instead makes the counters shared state: any worker (or
the dashboard, which runs no jobs at all) reads the same fleet-wide truth,
and the numbers survive worker restarts. HINCRBY is atomic, so concurrent
workers increment safely without locks.

Metrics are BEST-EFFORT: every recorder swallows its own errors. A broken
metrics write must never fail a job that actually succeeded.
"""
import logging
import time

log = logging.getLogger("taskq.metrics")

#: Upper bounds (milliseconds) for the execution-time histogram. Cumulative
#: Prometheus-style buckets: each counts "jobs at or under this duration",
#: which is what lets a percentile be interpolated on read.
LATENCY_BUCKETS_MS = (10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000)

#: Counter fields tracked per queue. Kept in one hash so a dashboard read
#: is a single HGETALL rather than N round-trips.
COUNTERS = ("succeeded", "failed", "retried", "dead", "recovered", "replayed")


class Metrics:
    def __init__(self, redis_conn, config, queue_name: str = "default"):
        self.r = redis_conn
        ns = config.namespace
        self.key_counters = f"{ns}:metrics:{queue_name}:counters"
        self.key_latency = f"{ns}:metrics:{queue_name}:latency"
        self.tp_prefix = f"{ns}:metrics:{queue_name}:tp:"
        # Throughput is bucketed into fixed 5-second slots so the dashboard
        # can draw a time series without storing one key per job. Buckets
        # carry a TTL, so old history evicts itself - no cleanup job needed.
        self.bucket_seconds = 5
        self.retention_seconds = 1800  # 30 minutes of chart history

    # ---- recording (called from the hot path) ---------------------------

    def _bucket_key(self, now: float) -> str:
        # Floor the timestamp to a bucket boundary so every worker writing
        # at the same moment lands in the SAME bucket, wherever it runs.
        slot = int(now // self.bucket_seconds) * self.bucket_seconds
        return f"{self.tp_prefix}{slot}"

    def record_success(self, duration_seconds: float) -> None:
        try:
            bucket = self._bucket_key(time.time())
            ms = duration_seconds * 1000
            # One pipeline = one network round-trip for ~15 increments.
            # transaction=False: these are independent counters, so we want
            # batching, not atomicity - a partial metrics write is harmless.
            pipe = self.r.pipeline(transaction=False)
            pipe.hincrby(self.key_counters, "succeeded", 1)
            pipe.hincrby(bucket, "succeeded", 1)
            pipe.expire(bucket, self.retention_seconds)
            pipe.hincrbyfloat(self.key_latency, "sum_ms", ms)
            pipe.hincrby(self.key_latency, "count", 1)
            for bound in LATENCY_BUCKETS_MS:
                # Cumulative buckets: a 30ms job increments le_50, le_100,
                # ... too. Storing them cumulatively is what makes
                # percentile interpolation possible on read.
                if ms <= bound:
                    pipe.hincrby(self.key_latency, f"le_{bound}", 1)
            pipe.hincrby(self.key_latency, "le_inf", 1)
            pipe.execute()
        except Exception:
            log.debug("metrics: record_success failed", exc_info=True)

    def record_failure(self, outcome: str) -> None:
        """outcome is 'retried' or 'dead'."""
        try:
            bucket = self._bucket_key(time.time())
            pipe = self.r.pipeline(transaction=False)
            pipe.hincrby(self.key_counters, "failed", 1)
            pipe.hincrby(self.key_counters, outcome, 1)
            pipe.hincrby(bucket, "failed", 1)
            pipe.expire(bucket, self.retention_seconds)
            pipe.execute()
        except Exception:
            log.debug("metrics: record_failure failed", exc_info=True)

    def incr(self, field: str, amount: int = 1) -> None:
        """Bump a plain counter, e.g. 'recovered' or 'replayed'."""
        try:
            self.r.hincrby(self.key_counters, field, amount)
        except Exception:
            log.debug("metrics: incr(%s) failed", field, exc_info=True)

    # ---- reading (called by the dashboard) -------------------------------

    def counters(self) -> dict:
        raw = self.r.hgetall(self.key_counters) or {}
        return {name: int(raw.get(name, 0)) for name in COUNTERS}

    def latency(self) -> dict:
        raw = self.r.hgetall(self.key_latency) or {}
        count = int(raw.get("count", 0))
        total_ms = float(raw.get("sum_ms", 0.0))
        buckets = {b: int(raw.get(f"le_{b}", 0)) for b in LATENCY_BUCKETS_MS}
        return {
            "count": count,
            "avg_ms": (total_ms / count) if count else 0.0,
            "p95_ms": self._percentile(buckets, count, 0.95),
            "buckets": buckets,
        }

    @staticmethod
    def _percentile(buckets: dict, count: int, q: float) -> float:
        # Histogram percentiles are ESTIMATES: we know how many jobs fell in
        # each range, not their exact values. We find the first bucket whose
        # cumulative count crosses the target rank and report its upper
        # bound. That is the standard tradeoff - a true p95 would require
        # storing every observation, which does not scale.
        if count == 0:
            return 0.0
        target = q * count
        for bound in sorted(buckets):
            if buckets[bound] >= target:
                return float(bound)
        return float("inf")

    def throughput(self, window_seconds: int = 300) -> list[dict]:
        """Per-bucket succeeded/failed counts over the recent window."""
        newest = (int(time.time() // self.bucket_seconds)
                  * self.bucket_seconds)
        starts = [newest - offset for offset in
                  range(0, window_seconds, self.bucket_seconds)][::-1]
        pipe = self.r.pipeline(transaction=False)
        for start in starts:
            pipe.hgetall(f"{self.tp_prefix}{start}")
        rows = pipe.execute()
        return [
            {
                "t": start,
                "succeeded": int((row or {}).get("succeeded", 0)),
                "failed": int((row or {}).get("failed", 0)),
            }
            for start, row in zip(starts, rows)
        ]

    def reset(self) -> None:
        """Clear all metrics for this queue (used by tests/demos)."""
        keys = [self.key_counters, self.key_latency]
        keys += list(self.r.scan_iter(f"{self.tp_prefix}*", count=500))
        if keys:
            self.r.delete(*keys)
