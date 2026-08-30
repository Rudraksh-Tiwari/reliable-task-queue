"""The reaper: the recovery loop that makes crashed workers survivable.

Two sweeps on one cadence, both via the same atomic promote script:
  1. requeue_expired  - in-flight jobs whose lease deadline passed (their
     worker died mid-run) go back to the ready queue.
  2. promote_scheduled - failed jobs whose retry backoff has elapsed go
     back to the ready queue.

It runs as a daemon THREAD inside every worker rather than as a separate
process to deploy. That choice is only safe because both sweeps are single
atomic Lua scripts: any number of reapers can run concurrently without
locks or leader election - two reapers can't double-requeue a job, the
script guarantees it. The result: the fleet is self-healing as long as at
least one worker is alive, with zero extra infrastructure.
"""
import logging
import threading

from .broker import Broker

log = logging.getLogger("taskq.reaper")


class Reaper:
    def __init__(self, broker: Broker, interval: float):
        self.broker = broker
        self.interval = interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def run_once(self) -> tuple[int, int]:
        # requeue_expired records the 'recovered' metric itself; promotion
        # of due retries/delays is normal scheduling, not a recovery, so it
        # is deliberately not counted as one.
        recovered = self.broker.requeue_expired()
        promoted = self.broker.promote_scheduled()
        if recovered:
            log.info("recovered %d job(s) from expired leases "
                     "(their worker died mid-run)", recovered)
        if promoted:
            log.info("promoted %d due retr%s to the ready queue",
                     promoted, "y" if promoted == 1 else "ies")
        return recovered, promoted

    def _loop(self) -> None:
        # Sweep immediately on start: a freshly booted worker should rescue
        # already-expired leases at once, not after a full interval.
        self._safe_run()
        # Event.wait doubles as an interruptible sleep: it returns early
        # (True) the moment stop() sets the event.
        while not self._stop_event.wait(self.interval):
            self._safe_run()

    def _safe_run(self) -> None:
        try:
            self.run_once()
        except Exception:
            # The reaper IS the safety net - it must never die quietly.
            # Log loudly and try again next interval (e.g. transient
            # Redis connection blips).
            log.exception("reaper sweep failed; retrying next interval")

    def start_thread(self) -> None:
        self._thread = threading.Thread(target=self._loop,
                                        name="taskq-reaper", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
