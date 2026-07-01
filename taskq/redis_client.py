"""Single place that knows how to build a Redis connection."""
import redis

from .config import Config


def make_redis(config: Config) -> redis.Redis:
    # decode_responses=True makes Redis return str instead of raw bytes, so the
    # rest of the codebase never has to sprinkle .decode() everywhere. Fine at
    # our scale; a throughput-critical system might keep bytes to avoid the
    # per-response decode cost.
    return redis.Redis(
        host=config.redis_host,
        port=config.redis_port,
        db=config.redis_db,
        decode_responses=True,
    )
