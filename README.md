# taskq — a reliable distributed task queue (built from scratch)

A background job system in the spirit of Sidekiq / Celery, implemented from
first principles. The point of the project is the **reliability machinery** —
atomic job claim, crash recovery, retries with backoff, and a dead-letter
queue — written by hand rather than delegated to an existing job-queue library.

> Status: Stage 2 of 6 complete — jobs now survive worker crashes. See [Roadmap](#roadmap).

## Why this exists

Anyone can call an existing library's `enqueue()`. This project demonstrates the
harder thing: building the guarantees underneath it — that **once a job is
accepted, it runs to completion even if a worker is killed mid-execution.**

## Architecture

Producers enqueue jobs into a Redis broker (priority / scheduled / in-flight /
dead-letter structures). A pool of workers atomically claims jobs, runs them,
and acks on success; failures retry with backoff or fall through to the
dead-letter queue. A dashboard and metrics layer observe the system.

_(Full architecture diagram and design notes are filled in as the stages land.)_

## Quickstart

Prerequisites: Docker Desktop, Python 3.11+.

```bash
# 1. start Redis
docker compose up -d

# 2. install dependencies
pip install -r requirements.txt

# 3. run the Stage 0 smoke test
python -m taskq.smoke
```

### Running jobs (Stage 1)

Two terminals — one worker, one producer:

```bash
# terminal 1: start a worker that waits for jobs
python -m taskq.worker

# terminal 2: enqueue a few demo jobs
python -m taskq.enqueue_demo
```

Or drain the queue once and exit (handy for a quick check):

```bash
python -m taskq.enqueue_demo
python -m taskq.worker --burst
```

## The reliability model (Stage 2)

The core guarantee: **once a job is accepted, it runs to completion even if
its worker is hard-killed mid-execution** (at-least-once delivery).

A job id lives in exactly one structure at a time; atomic operations move it:

| Redis key | Type | Holds |
|---|---|---|
| `taskq:queue:<name>` | list | ids ready to run (FIFO) |
| `taskq:inflight:<name>` | zset | claimed ids, scored by lease deadline |
| `taskq:scheduled:<name>` | zset | failed ids, scored by retry-at time |
| `taskq:dead:<name>` | list | ids that exhausted their retry budget |
| `taskq:job:<id>` | hash | payload (`data`) + mutable `attempts`/`status`/`error` |

The mechanisms, each hand-written:

- **Atomic claim** ([claim.lua](taskq/scripts/claim.lua)) — pop + lease-record
  + attempt-count as one indivisible script, so there is no instant where a
  job is in nobody's hands. Attempts are counted **at claim time** (HINCRBY),
  so even a "poison pill" job that kills its worker has its runs counted and
  its retry budget enforced.
- **Visibility timeout + reaper** ([promote.lua](taskq/scripts/promote.lua),
  [reaper.py](taskq/reaper.py)) — a claimed job holds a lease; if the worker
  dies, the lease expires and any live worker's reaper thread atomically
  requeues it. No locks, no leader election: the promote script is atomic,
  so concurrent reapers are safe.
- **Retries with capped exponential backoff + jitter**
  ([retry.py](taskq/retry.py)) — failures retry at growing, randomized
  delays to avoid thundering-herd stampedes on a struggling dependency.
- **Dead-letter queue** ([broker.py](taskq/broker.py)) — a job that spends
  its retry budget is parked with its attempt count and last error intact:
  failure is data, not noise.
- **Graceful shutdown** — Ctrl+C stops claiming, finishes the current job,
  then exits. A hard kill is survivable by design.

Prove it end-to-end (spawns real workers, hard-kills one mid-job, checks a
permanently-failing job reaches the DLQ):

```bash
python -m taskq.verify_stage2
```

## Project layout

```
reliable-task-queue/
├── docker-compose.yml      # local Redis broker
├── requirements.txt
├── .env.example            # configurable env vars (defaults match docker)
└── taskq/
    ├── config.py           # environment-driven config (incl. reliability knobs)
    ├── job.py              # the Job model + JSON serialization
    ├── redis_client.py     # Redis connection factory
    ├── registry.py         # @task decorator + name -> handler lookup
    ├── queue.py            # producer-facing API (enqueue/size)
    ├── broker.py           # ALL job state transitions: claim/ack/fail/bury/recover
    ├── scripts/
    │   ├── claim.lua       # atomic pop + lease + attempt-count (the heart)
    │   └── promote.lua     # atomic "move due ids from zset to queue"
    ├── retry.py            # capped exponential backoff + jitter
    ├── reaper.py           # recovery thread: expired leases + due retries
    ├── worker.py           # the worker loop that runs jobs
    ├── example_tasks.py    # demo tasks
    ├── enqueue_demo.py     # demo producer
    ├── smoke.py            # Stage 0 verification
    └── verify_stage2.py    # Stage 2 proof: crash recovery + DLQ, automated
```

## Roadmap

- [x] **Stage 0 — Foundation:** project scaffolding, Redis via Docker, Job model, smoke test.
- [x] **Stage 1 — MVP happy path:** `@task` registry, `enqueue()` onto a Redis list, single worker that runs jobs.
- [x] **Stage 2 — Reliability core:** atomic claim (Lua), lease + reaper crash recovery, retries with backoff + jitter, dead-letter queue, automated chaos verification.
- [ ] **Stage 3 — Scale & scheduling:** concurrent workers, priorities, delayed jobs, idempotency.
- [ ] **Stage 4 — Observability:** metrics + web dashboard.
- [ ] **Stage 5 — Proof & polish:** load test, chaos test, docs, benchmarks.
- [ ] **Stage 6 — Distributed (stretch):** multi-node workers.

## Design notes

_Accumulated as the project grows — each notable tradeoff is also commented
inline at the relevant code._

## License

MIT
