"""Worker heartbeats: a presence registry with zero cleanup protocol.

Each worker periodically SETs a TTL'd key describing itself. Liveness is
simply "does the key still exist": a crashed or partitioned worker stops
refreshing and its key expires on its own. Contrast with a registry that
workers must deregister from - that design needs a janitor for workers
that died without saying goodbye; TTLs make the janitor unnecessary.
"""
import logging
import threading
import time

from .broker import Broker

log = logging.getLogger("taskq.heartbeat")


class Heartbeat:
    def __init__(self, broker: Broker, worker_id: str, interval: float,
                 info: dict):
        self.broker = broker
        self.worker_id = worker_id
        self.interval = interval
        self.info = info
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def beat(self) -> None:
        # TTL = 3 intervals: one missed beat (GC pause, brief Redis blip)
        # doesn't flap the worker to "dead"; three missed beats does.
        self.broker.heartbeat(
            self.worker_id,
            {**self.info, "last_beat": time.time()},
            ttl=max(int(self.interval * 3), 1),
        )

    def _loop(self) -> None:
        self._safe_beat()  # announce presence immediately, not one interval in
        while not self._stop_event.wait(self.interval):
            self._safe_beat()

    def _safe_beat(self) -> None:
        try:
            self.beat()
        except Exception:
            log.exception("heartbeat failed; retrying next interval")

    def start_thread(self) -> None:
        self._thread = threading.Thread(target=self._loop,
                                        name="taskq-heartbeat", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
