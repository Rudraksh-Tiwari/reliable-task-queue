"""The worker: pulls jobs off a queue and runs them.

Run continuously (the real mode):   python -m taskq.worker
Drain the queue once, then exit:     python -m taskq.worker --burst
"""
import argparse
import logging

from .config import load_config
from .job import Job
from .queue import Queue
from .redis_client import make_redis
from .registry import get_handler

log = logging.getLogger("taskq.worker")


class Worker:
    def __init__(self, queue: Queue, burst: bool = False, dequeue_timeout: int = 5):
        self.queue = queue
        # burst=True: process everything currently queued, then exit. Handy for
        # demos and tests. burst=False: run forever, waiting for new work.
        self.burst = burst
        self.dequeue_timeout = dequeue_timeout
        self._running = False

    def run(self) -> None:
        self._running = True
        log.info("worker started (burst=%s), waiting on queue '%s'", self.burst, self.queue.name)
        while self._running:
            # In burst mode block only briefly so an empty queue exits quickly.
            timeout = 1 if self.burst else self.dequeue_timeout
            job = self.queue.dequeue(timeout=timeout)
            if job is None:
                if self.burst:
                    log.info("queue empty - burst worker exiting")
                    break
                continue  # normal mode: nothing yet, loop and wait again
            self._process(job)

    def _process(self, job: Job) -> None:
        # STAGE 1 = HAPPY PATH ONLY.
        # We pop the job, look up its handler by name, and run it. There is
        # deliberately NO failure handling here: if the handler raises, the
        # exception propagates and the job is simply gone — because we already
        # removed it from Redis with BRPOP. Making that safe (atomic claim,
        # retry, dead-letter) is the entire purpose of Stage 2, so we don't
        # build any of it early.
        handler = get_handler(job.type)
        log.info("running job %s (%s)", job.id, job.type)
        result = handler(*job.args, **job.kwargs)
        log.info("job %s done -> %r", job.id, result)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="taskq.worker")
    parser.add_argument("--burst", action="store_true",
                        help="process all currently queued jobs, then exit")
    parser.add_argument("--queue", default="default", help="queue name to consume from")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Importing the tasks module runs the @task decorators, which populates the
    # registry. A production system would let you point the worker at any app
    # module; for now the demo tasks are wired in directly.
    from . import example_tasks  # noqa: F401

    config = load_config()
    queue = Queue(make_redis(config), config, name=args.queue)
    worker = Worker(queue, burst=args.burst)
    try:
        worker.run()
    except KeyboardInterrupt:
        # Clean exit on Ctrl+C instead of dumping a traceback. Note: this does
        # NOT protect a job that's mid-execution — safe in-flight shutdown is a
        # Stage 2 concern (SIGTERM handling), intentionally not solved here.
        log.info("interrupted - exiting")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
