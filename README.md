# taskq — a reliable distributed task queue (built from scratch)

A background job system in the spirit of Sidekiq / Celery, implemented from
first principles. The point of the project is the **reliability machinery** —
atomic job claim, crash recovery, retries with backoff, and a dead-letter
queue — written by hand rather than delegated to an existing job-queue library.

> Status: Stage 1 of 6 complete — jobs now run end-to-end. See [Roadmap](#roadmap).

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

## Project layout

```
reliable-task-queue/
├── docker-compose.yml      # local Redis broker
├── requirements.txt
├── .env.example            # configurable env vars (defaults match docker)
└── taskq/
    ├── config.py           # environment-driven config
    ├── job.py              # the Job model + JSON serialization
    ├── redis_client.py     # Redis connection factory
    ├── registry.py         # @task decorator + name -> handler lookup
    ├── queue.py            # Redis-list-backed FIFO queue (enqueue/dequeue)
    ├── worker.py           # the worker loop that runs jobs
    ├── example_tasks.py    # demo tasks
    ├── enqueue_demo.py     # demo producer
    └── smoke.py            # Stage 0 verification
```

## Roadmap

- [x] **Stage 0 — Foundation:** project scaffolding, Redis via Docker, Job model, smoke test.
- [x] **Stage 1 — MVP happy path:** `@task` registry, `enqueue()` onto a Redis list, single worker that runs jobs.
- [ ] **Stage 2 — Reliability core:** atomic claim, visibility timeout, retries + backoff, dead-letter.
- [ ] **Stage 3 — Scale & scheduling:** concurrent workers, priorities, delayed jobs, idempotency.
- [ ] **Stage 4 — Observability:** metrics + web dashboard.
- [ ] **Stage 5 — Proof & polish:** load test, chaos test, docs, benchmarks.
- [ ] **Stage 6 — Distributed (stretch):** multi-node workers.

## Design notes

_Accumulated as the project grows — each notable tradeoff is also commented
inline at the relevant code._

## License

MIT
