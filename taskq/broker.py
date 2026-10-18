"""The broker: every state transition a job can make, in one place.

This is the reliability core of the project. A job is always in exactly one
of these Redis structures, and only broker operations move it between them:

    ready lanes (3 lists) --claim--> in-flight (zset, scored by lease deadline)
    in-flight --ack--> gone (success; payload deleted)
    in-flight --fail--> scheduled (zset, scored by retry time)  [retries left]
    in-flight --fail/bury--> dead-letter (list)                 [budget spent]
    scheduled --promote--> ready lanes   (backoff elapsed / delay arrived)
    in-flight --promote--> ready lanes   (lease expired = worker died)

The claim and promote transitions are Lua scripts because they MUST be
atomic (see the .lua files for the full reasoning). The rest are MULTI/EXEC
pipelines: atomic enough, and easier to read in Python.
"""
import json
import logging
from pathlib import Path

import redis

from .config import Config
from .job import Job, JobStatus
from .metrics import Metrics
from .retry import backoff_delay

log = logging.getLogger("taskq.broker")

_SCRIPTS_DIR = Path(__file__).resolve().parent / "scripts"

#: Lanes in strict dequeue order. Workers fully drain "high" before touching
#: "normal", and "normal" before "low" (starvation tradeoff: see claim.lua).
PRIORITIES = ("high", "normal", "low")


class Broker:
    def __init__(self, redis_conn: redis.Redis, config: Config,
                 queue_name: str = "default"):
        self.r = redis_conn
        self.cfg = config
        self.queue_name = queue_name

        ns = config.namespace
        self.lane_prefix = f"{ns}:queue:{queue_name}:"
        self.lane_keys = {p: f"{self.lane_prefix}{p}" for p in PRIORITIES}
        self.key_inflight = f"{ns}:inflight:{queue_name}"   # zset id -> lease deadline
        self.key_scheduled = f"{ns}:scheduled:{queue_name}"  # zset id -> run-at time
        self.key_dead = f"{ns}:dead:{queue_name}"            # list of dead job ids
        self.job_prefix = f"{ns}:job:"     # hash per job: data/attempts/status/error/priority
        self.idem_prefix = f"{ns}:idem:"   # idempotency slots: user key -> job id
        self.worker_prefix = f"{ns}:worker:"  # heartbeat keys, TTL'd

        # register_script is transport, not logic: it uploads the script once
        # and afterwards invokes it by SHA (EVALSHA), transparently
        # re-uploading if Redis restarts. The reliability logic itself lives
        # in the .lua files, where we own every line.
        self._claim_script = redis_conn.register_script(
            (_SCRIPTS_DIR / "claim.lua").read_text())
        self._promote_script = redis_conn.register_script(
            (_SCRIPTS_DIR / "promote.lua").read_text())
        self._release_script = redis_conn.register_script(
            (_SCRIPTS_DIR / "release.lua").read_text())
        self._replay_script = redis_conn.register_script(
            (_SCRIPTS_DIR / "replay.lua").read_text())
        self._discard_script = redis_conn.register_script(
            (_SCRIPTS_DIR / "discard.lua").read_text())

        self.metrics = Metrics(redis_conn, config, queue_name)

    def job_key(self, job_id: str) -> str:
        return f"{self.job_prefix}{job_id}"

    def _server_now(self) -> float:
        # Schedule against REDIS's clock, the same clock the Lua scripts
        # use. Mixing worker-machine clocks with the server clock would
        # make lease/retry timing drift with clock skew.
        secs, micros = self.r.time()
        return secs + micros / 1_000_000

    # ---- producer side ----------------------------------------------------

    def enqueue(self, job: Job, *, delay_seconds: float | None = None,
                run_at: float | None = None) -> str:
        """Add a job. Returns the id that is actually live for this work.

        - Immediate (default): the job enters its priority lane now.
        - Delayed: with delay_seconds/run_at it parks in the scheduled zset
          and the reaper promotes it when due (same path retries use).
        - Idempotent: if job.idempotency_key is set and another job with the
          same key is still active, nothing is enqueued and the EXISTING
          job's id is returned (compare it to job.id to detect suppression).
        """
        if delay_seconds is not None and run_at is not None:
            raise ValueError("pass delay_seconds or run_at, not both")
        if job.priority not in PRIORITIES:
            raise ValueError(f"unknown priority {job.priority!r}; "
                             f"expected one of {PRIORITIES}")

        if job.idempotency_key:
            idem_key = f"{self.idem_prefix}{job.idempotency_key}"
            # SET NX = "reserve this key only if free" in one atomic step.
            # The TTL bounds the dedup window even if this producer crashes
            # one line below (before writing the job): a reservation that
            # never became a job must not suppress real work forever.
            reserved = self.r.set(idem_key, job.id, nx=True,
                                  ex=self.cfg.idem_ttl_seconds)
            if not reserved:
                existing = self.r.get(idem_key)
                log.info("enqueue: duplicate suppressed by idempotency key "
                         "%r (active job %s)", job.idempotency_key, existing)
                # The reservation may expire between our failed SET and this
                # GET; treat that vanishing race as "slot free" and proceed.
                if existing is not None:
                    return existing

        if delay_seconds is not None:
            run_at = self._server_now() + delay_seconds

        pipe = self.r.pipeline(transaction=True)
        # Payload hash and queue entry land in one MULTI so no observer
        # (including the claim script) can ever see an id whose payload
        # doesn't exist yet, or a payload that never got queued.
        pipe.hset(self.job_key(job.id), mapping={
            "data": job.to_json(),   # immutable description, written once
            "attempts": 0,            # mutable counter, HINCRBY'd by claim.lua
            "status": (JobStatus.QUEUED.value if run_at is None
                       else "scheduled"),
            "error": "",
            "priority": job.priority,  # promote.lua routes recoveries by this
        })
        if run_at is None:
            pipe.lpush(self.lane_keys[job.priority], job.id)
        else:
            pipe.zadd(self.key_scheduled, {job.id: run_at})
        pipe.execute()
        return job.id

    # ---- worker side ------------------------------------------------------

    def claim(self) -> Job | None:
        """Atomically pop the next job (high lane first) and lease it."""
        res = self._claim_script(
            keys=[self.lane_keys["high"], self.lane_keys["normal"],
                  self.lane_keys["low"], self.key_inflight],
            args=[self.cfg.lease_seconds, self.job_prefix],
        )
        if res is None:
            return None
        job_id, data, attempts = res[0], res[1], int(res[2])
        job = Job.from_hash(data, attempts=attempts,
                            status=JobStatus.RUNNING.value)
        job.id = job_id  # defensive: the popped id is authoritative
        return job

    def ack(self, job: Job, duration_seconds: float = 0.0) -> None:
        """Success: release the lease and delete the payload."""
        self.metrics.record_success(duration_seconds)
        pipe = self.r.pipeline(transaction=True)
        pipe.zrem(self.key_inflight, job.id)
        # Completed payloads are deleted rather than kept: a result/history
        # backend would grow without bound. Real systems make retention an
        # opt-in feature with a TTL; our dashboard (Stage 4) uses counters.
        pipe.delete(self.job_key(job.id))
        removed, _ = pipe.execute()
        if removed == 0:
            # Our lease had already expired and the reaper requeued the job:
            # someone else may run it again. This is the honest cost of
            # at-least-once delivery (the alternative - at-most-once - loses
            # jobs instead of duplicating them).
            log.warning("ack: lease for %s had already expired; a duplicate "
                        "execution is possible", job.id)
        self._release_idem(job)

    def fail(self, job: Job, exc: BaseException) -> str:
        """Failure: schedule a retry, or bury the job if its budget is spent.

        Called by the worker that holds the lease, inside the lease window
        in the normal case.
        """
        job.error = f"{type(exc).__name__}: {exc}"[:500]
        if job.attempts <= job.max_retries:
            delay = backoff_delay(job.attempts, self.cfg.retry_base,
                                  self.cfg.retry_cap, self.cfg.retry_jitter)
            run_at = self._server_now() + delay
            pipe = self.r.pipeline(transaction=True)
            pipe.zrem(self.key_inflight, job.id)            # release the lease
            pipe.hset(self.job_key(job.id), mapping={
                "status": JobStatus.FAILED.value,            # = awaiting retry
                "error": job.error,
            })
            pipe.zadd(self.key_scheduled, {job.id: run_at})  # park until due
            removed = pipe.execute()[0]
            if removed == 0:
                log.warning("fail: lease for %s was already reclaimed; "
                            "duplicate execution possible", job.id)
            # Recorded here (not in bury) so each failure counts exactly
            # once: this path retries, the bury path below records 'dead'.
            self.metrics.record_failure("retried")
            log.info("job %s attempt %d/%d failed (%s); retrying in %.2fs",
                     job.id, job.attempts, job.max_retries + 1,
                     job.error, delay)
            return "retried"
        self.bury(job, job.error)
        return "dead"

    def bury(self, job: Job, reason: str) -> None:
        """Dead-letter the job: park it, fully inspectable, for a human.

        Failure is data. A job that failed all its retries is evidence of a
        real problem; deleting it would destroy exactly what an operator
        needs to diagnose the issue (and possibly replay the job later).
        """
        pipe = self.r.pipeline(transaction=True)
        pipe.zrem(self.key_inflight, job.id)
        pipe.hset(self.job_key(job.id), mapping={
            "status": JobStatus.DEAD.value,
            "error": (reason or "")[:500],
        })
        pipe.lpush(self.key_dead, job.id)
        pipe.execute()
        self.metrics.record_failure("dead")
        # Dead is a terminal state: free the idempotency slot so the caller
        # can deliberately enqueue a fresh instance (e.g. after a fix).
        self._release_idem(job)
        log.error("job %s moved to dead-letter after %d attempt(s): %s",
                  job.id, job.attempts, reason)

    def _release_idem(self, job: Job) -> None:
        if job.idempotency_key:
            self._release_script(
                keys=[f"{self.idem_prefix}{job.idempotency_key}"],
                args=[job.id])

    # ---- recovery (called by the reaper) -----------------------------------

    def requeue_expired(self, batch: int = 100) -> int:
        """Move jobs whose lease deadline passed back to their lanes."""
        recovered = int(self._promote_script(
            keys=[self.key_inflight],
            args=[batch, self.job_prefix, self.lane_prefix]))
        if recovered:
            # 'recovered' is the headline reliability number: how many jobs
            # a crashed worker would have lost without the lease + reaper.
            self.metrics.incr("recovered", recovered)
        return recovered

    def promote_scheduled(self, batch: int = 100) -> int:
        """Move due scheduled jobs (retries + delayed) back to their lanes."""
        return int(self._promote_script(
            keys=[self.key_scheduled],
            args=[batch, self.job_prefix, self.lane_prefix]))

    # ---- worker presence (heartbeats) ---------------------------------------

    def heartbeat(self, worker_id: str, info: dict, ttl: int) -> None:
        # A plain TTL'd key per worker: if the worker stops beating (crash,
        # network split), its key simply expires. Liveness = key existence;
        # no cleanup protocol needed for dead workers.
        self.r.set(f"{self.worker_prefix}{worker_id}", json.dumps(info),
                   ex=ttl)

    def live_workers(self) -> list[dict]:
        workers = []
        for key in self.r.scan_iter(f"{self.worker_prefix}*", count=100):
            raw = self.r.get(key)
            if raw:
                info = json.loads(raw)
                info["worker_id"] = key[len(self.worker_prefix):]
                workers.append(info)
        return sorted(workers, key=lambda w: w["worker_id"])

    # ---- inspection ---------------------------------------------------------

    def lane_depths(self) -> dict[str, int]:
        pipe = self.r.pipeline(transaction=False)
        for p in PRIORITIES:
            pipe.llen(self.lane_keys[p])
        return dict(zip(PRIORITIES, pipe.execute()))

    def queue_depth(self) -> int:
        return sum(self.lane_depths().values())

    def inflight_count(self) -> int:
        return self.r.zcard(self.key_inflight)

    def scheduled_count(self) -> int:
        return self.r.zcard(self.key_scheduled)

    def dead_count(self) -> int:
        return self.r.llen(self.key_dead)

    def dead_ids(self) -> list[str]:
        return self.r.lrange(self.key_dead, 0, -1)

    # ---- dead-letter operations (driven by the dashboard) -------------------

    def replay_dead(self, job_id: str) -> str:
        """Move one dead job back to its lane with a fresh retry budget.

        Returns 'replayed', 'not_found' (not on the dead list - e.g. a
        double click already took it), or 'payload_missing'.
        """
        result = int(self._replay_script(
            keys=[self.key_dead],
            args=[job_id, self.job_prefix, self.lane_prefix]))
        if result == 1:
            self.metrics.incr("replayed")
            log.info("job %s replayed from the dead-letter queue", job_id)
            return "replayed"
        if result == -1:
            log.warning("job %s could not be replayed: payload missing",
                        job_id)
            return "payload_missing"
        return "not_found"

    def replay_all_dead(self, limit: int = 500) -> dict:
        outcomes = {"replayed": 0, "not_found": 0, "payload_missing": 0}
        for job_id in self.dead_ids()[:limit]:
            outcomes[self.replay_dead(job_id)] += 1
        return outcomes

    def discard_dead(self, job_id: str) -> bool:
        """Permanently drop a dead job (operator decided it's not wanted).

        Returns False, and changes nothing, if the id is not on the dead
        list - e.g. a concurrent replay already put it back in its lane.
        The check and the delete are one script (see discard.lua for the
        job-losing race a plain LREM + DEL pipeline allowed).
        """
        removed = int(self._discard_script(
            keys=[self.key_dead, self.job_key(job_id)],
            args=[job_id]))
        if removed:
            log.info("job %s discarded from the dead-letter queue", job_id)
        return bool(removed)

    def dead_jobs(self, limit: int = 50) -> list[dict]:
        """Dead jobs with enough detail for an operator to triage them."""
        rows = []
        for job_id in self.dead_ids()[:limit]:
            fields = self.r.hgetall(self.job_key(job_id))
            if not fields or "data" not in fields:
                # Payload deleted out from under the dead list; surface it
                # rather than hiding the inconsistency from the operator.
                rows.append({"id": job_id, "type": "<payload missing>",
                             "attempts": 0, "error": "payload deleted",
                             "priority": "normal", "args": [], "kwargs": {}})
                continue
            job = Job.from_hash(fields["data"],
                                attempts=fields.get("attempts", 0),
                                status=fields.get("status") or None,
                                error=fields.get("error") or None)
            rows.append({
                "id": job_id,
                "type": job.type,
                "attempts": job.attempts,
                "error": job.error or "",
                "priority": job.priority,
                "args": job.args,
                "kwargs": job.kwargs,
                "created_at": job.created_at,
                "updated_at": job.updated_at,
            })
        return rows

    def stats(self) -> dict:
        """One-shot snapshot for the dashboard."""
        lanes = self.lane_depths()
        return {
            "queue": self.queue_name,
            "lanes": lanes,
            "ready": sum(lanes.values()),
            "inflight": self.inflight_count(),
            "scheduled": self.scheduled_count(),
            "dead": self.dead_count(),
            "counters": self.metrics.counters(),
            "latency": self.metrics.latency(),
            "workers": self.live_workers(),
        }

    def load_job(self, job_id: str) -> Job | None:
        fields = self.r.hgetall(self.job_key(job_id))
        if not fields or "data" not in fields:
            return None
        return Job.from_hash(fields["data"],
                             attempts=fields.get("attempts", 0),
                             status=fields.get("status") or None,
                             error=fields.get("error") or None)
