"""The worker: claims jobs under a lease, runs them, acks or fails them.

Run continuously:  python -m taskq.worker [--queue NAME] [--concurrency N]
Drain, then exit:  python -m taskq.worker --burst
"""
import argparse
import logging
import os
import signal
import socket
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

from .broker import Broker
from .config import load_config
from .heartbeat import Heartbeat
from .job import Job
from .reaper import Reaper
from .redis_client import make_redis
from .registry import get_handler

log = logging.getLogger("taskq.worker")


class Worker:
    def __init__(self, broker: Broker, burst: bool = False,
                 concurrency: int = 1):
        self.broker = broker
        # burst=True: process whatever is ready now, then exit (demos/tests).
        # Note: burst does not wait for scheduled retries to become due.
        self.burst = burst
        # THREADS, deliberately: queue jobs are overwhelmingly I/O-bound
        # (network calls, DB writes, sleeps), where Python threads give real
        # parallelism because the GIL is released while blocked on I/O.
        # CPU-bound work should scale with worker PROCESSES instead (also
        # supported: just start more `python -m taskq.worker`) - that split
        # is exactly how Sidekiq (threads) vs. resque (processes) differ.
        self.concurrency = max(1, concurrency)
        self._pool = ThreadPoolExecutor(max_workers=self.concurrency,
                                        thread_name_prefix="taskq-exec")
        self._futures: set = set()
        self._running = False

    def stop(self) -> None:
        # Graceful shutdown = stop CLAIMING new work, never abandon work in
        # progress. In-flight jobs finish and ack before run() returns. A
        # hard kill mid-job is survivable anyway (lease + reaper), but
        # exiting gracefully avoids pointless duplicate runs.
        self._running = False

    def run(self) -> None:
        self._running = True
        cfg = self.broker.cfg
        log.info("worker started on queue '%s' (burst=%s, concurrency=%d, "
                 "lease=%.0fs)", self.broker.queue_name, self.burst,
                 self.concurrency, cfg.lease_seconds)
        try:
            while self._running:
                self._collect_finished()
                if len(self._futures) >= self.concurrency:
                    # All executor threads busy: claiming more now would only
                    # sit in memory while its lease burns down. Wait for a
                    # slot instead - backpressure, not buffering.
                    wait(self._futures, timeout=0.2,
                         return_when=FIRST_COMPLETED)
                    continue
                job = self.broker.claim()
                if job is None:
                    if self.burst:
                        if self._futures:  # queue empty but work still running
                            wait(self._futures, timeout=0.2,
                                 return_when=FIRST_COMPLETED)
                            continue
                        log.info("queue drained - burst worker exiting")
                        break
                    # The atomic claim is a Lua script, and scripts cannot
                    # block the way BRPOP does. So the worker polls on a
                    # short sleep: the price of a race-free claim is a little
                    # idle latency instead of a lost-job window.
                    time.sleep(cfg.poll_interval)
                    continue
                self._futures.add(self._pool.submit(self._process, job))
        finally:
            # Drain: let every claimed job finish + ack before we return.
            if self._futures:
                log.info("waiting for %d in-flight job(s) to finish",
                         len(self._futures))
                wait(self._futures)
                self._collect_finished()
            self._pool.shutdown(wait=True)

    def _collect_finished(self) -> None:
        done = {f for f in self._futures if f.done()}
        self._futures -= done
        for f in done:
            # Job-level exceptions are handled inside _process; anything
            # surfacing here is infrastructure trouble (e.g. Redis down
            # during ack) and must be visible, not silently swallowed.
            exc = f.exception()
            if exc is not None:
                log.error("executor task crashed outside job handling: %r",
                          exc)

    def _process(self, job: Job) -> None:
        total_allowed = job.max_retries + 1  # first run + retries
        if job.attempts > total_allowed:
            # Crash-loop guard. claim.lua increments `attempts` BEFORE
            # execution, so even a job that always kills its worker has its
            # runs counted. When reaper requeues push it past the budget,
            # bury it instead of letting it crash workers forever.
            self.broker.bury(job, "retry budget exhausted after repeated "
                                  f"worker crashes (execution {job.attempts} "
                                  f"> {total_allowed} allowed)")
            return
        log.info("running job %s (%s) attempt %d/%d",
                 job.id, job.type, job.attempts, total_allowed)
        # Measure only the HANDLER, not the surrounding Redis bookkeeping,
        # so the latency metric reflects how long the actual work takes.
        started = time.monotonic()
        try:
            # Handler lookup happens inside the try: a job naming an
            # unregistered task follows the normal failure path (retry, then
            # dead-letter) instead of crashing the worker loop.
            handler = get_handler(job.type)
            result = handler(*job.args, **job.kwargs)
        except Exception as exc:
            self.broker.fail(job, exc)
            return
        self.broker.ack(job, duration_seconds=time.monotonic() - started)
        log.info("job %s done -> %r", job.id, result)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="taskq.worker")
    parser.add_argument("--burst", action="store_true",
                        help="process all currently ready jobs, then exit")
    parser.add_argument("--queue", default="default",
                        help="queue name to consume from")
    parser.add_argument("--concurrency", type=int, default=1,
                        help="number of jobs this worker runs in parallel "
                             "(threads; best for I/O-bound tasks)")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Importing the tasks module runs its @task decorators, which populates
    # the registry so this worker knows how to execute those job types.
    from . import example_tasks  # noqa: F401

    config = load_config()
    broker = Broker(make_redis(config), config, queue_name=args.queue)
    worker = Worker(broker, burst=args.burst, concurrency=args.concurrency)
    reaper = Reaper(broker, interval=config.reaper_interval)
    worker_id = f"{socket.gethostname().lower()}-{os.getpid()}"
    heartbeat = Heartbeat(broker, worker_id, config.heartbeat_interval, info={
        "pid": os.getpid(),
        "queue": args.queue,
        "concurrency": max(1, args.concurrency),
        "burst": args.burst,
    })

    def _request_stop(signum, frame):
        log.info("signal %s received - finishing in-flight jobs, then "
                 "exiting", signum)
        worker.stop()

    # SIGINT = Ctrl+C, works everywhere. SIGTERM is the standard polite-kill
    # on POSIX; Windows has no real SIGTERM delivery (TerminateProcess is a
    # hard kill with no cleanup) - which is exactly the crash case the
    # lease + reaper machinery exists to survive.
    signal.signal(signal.SIGINT, _request_stop)
    try:
        signal.signal(signal.SIGTERM, _request_stop)
    except (ValueError, OSError):  # not available on some platforms/threads
        pass

    reaper.start_thread()
    heartbeat.start_thread()
    try:
        worker.run()
    finally:
        heartbeat.stop()
        reaper.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
