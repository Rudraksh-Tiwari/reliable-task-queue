"""The work queue: a thin, explicit wrapper over a Redis list."""
import redis

from .config import Config
from .job import Job


class Queue:
    """A single FIFO work queue backed by one Redis list.

    The whole queue is just two Redis operations:
      - enqueue -> LPUSH  (push onto the left/head)
      - dequeue -> BRPOP  (blocking pop from the right/tail)
    Push-left + pop-right yields FIFO order: the oldest job is always next out.
    Both operations are atomic in Redis, so many producers and many workers can
    hit the same list concurrently without corrupting it or losing ordering.
    """

    def __init__(self, redis_conn: redis.Redis, config: Config, name: str = "default"):
        self.redis = redis_conn
        self.name = name
        # Namespaced key, e.g. "taskq:queue:default". Keeping the queue name in
        # the key means we get multiple independent queues for free later.
        self.key = f"{config.namespace}:queue:{name}"

    def enqueue(self, job: Job) -> None:
        # LPUSH is O(1). We store the entire job as its JSON string; the list
        # holds opaque payloads and knows nothing about job structure.
        self.redis.lpush(self.key, job.to_json())

    def dequeue(self, timeout: int = 5) -> Job | None:
        # BRPOP *blocks* up to `timeout` seconds waiting for a job, rather than
        # busy-polling in a tight loop. Busy-polling would hammer Redis and burn
        # CPU for nothing; blocking lets Redis wake us the instant work arrives.
        # On timeout it returns None, so the worker can loop back and check for
        # a shutdown request instead of blocking forever.
        result = self.redis.brpop(self.key, timeout=timeout)
        if result is None:
            return None
        _list_key, raw = result  # BRPOP returns (which_list, popped_value)
        return Job.from_json(raw)

    def size(self) -> int:
        return self.redis.llen(self.key)
