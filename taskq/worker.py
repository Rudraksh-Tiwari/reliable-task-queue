"""The worker: claims jobs under a lease, runs them, acks or fails them.

Run continuously (the real mode):   python -m taskq.worker [--queue NAME]
Drain the ready queue, then exit:    python -m taskq.worker --burst
"""
import argparse
import logging
import signal
import time

from .broker import Broker
from .config import load_config
from .job import Job
from .reaper import Reaper
from .redis_client import make_redis
from .registry import get_handler

log = logging.getLogger("taskq.worker")


class Worker:
    def __init__(self, broker: Broker, burst: bool = False):
        self.broker = broker
        # burst=True: process whatever is ready now, then exit (demos/tests).
        # Note: burst does not wait for scheduled retries to become due.
        self.burst = burst
        self._running = False

    def stop(self) -> None:
        # Graceful shutdown = stop CLAIMING new work, never abandon work in
        # progress. The current job (if any) finishes and acks before the
        # loop exits. A hard kill mid-job is survivable anyway (lease +
        # reaper), but exiting gracefully avoids pointless duplicate runs.
        self._running = False

    def run(self) -> None:
        self._running = True
        cfg = self.broker.cfg
        log.info("worker started on queue '%s' (burst=%s, lease=%.0fs)",
                 self.broker.queue_name, self.burst, cfg.lease_seconds)
        while self._running:
            job = self.broker.claim()
            if job is None:
                if self.burst:
                    log.info("queue empty - burst worker exiting")
                    break
                # The atomic claim is a Lua script, and scripts cannot block
                # the way BRPOP does. So the worker polls on a short sleep:
                # the price of a race-free claim is a little idle latency
                # (bounded by poll_interval) instead of a lost-job window.
                time.sleep(cfg.poll_interval)
                continue
            self._process(job)

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
        try:
            # Handler lookup happens inside the try: a job naming an
            # unregistered task follows the normal failure path (retry, then
            # dead-letter) instead of crashing the worker loop.
            handler = get_handler(job.type)
            result = handler(*job.args, **job.kwargs)
        except Exception as exc:
            self.broker.fail(job, exc)
            return
        self.broker.ack(job)
        log.info("job %s done -> %r", job.id, result)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="taskq.worker")
    parser.add_argument("--burst", action="store_true",
                        help="process all currently ready jobs, then exit")
    parser.add_argument("--queue", default="default",
                        help="queue name to consume from")
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
    worker = Worker(broker, burst=args.burst)
    reaper = Reaper(broker, interval=config.reaper_interval)

    def _request_stop(signum, frame):
        log.info("signal %s received - finishing current job, then exiting",
                 signum)
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
    try:
        worker.run()
    finally:
        reaper.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
