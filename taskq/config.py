"""Application configuration.

All settings come from the environment (12-factor style), so the exact same
code runs unchanged across local / docker / prod — only the env vars differ.
"""
import os
from dataclasses import dataclass

from dotenv import load_dotenv

# Load a local .env file if one exists. Real deployments inject config straight
# through the environment; .env is purely a local-dev convenience.
load_dotenv()


@dataclass(frozen=True)
class Config:
    redis_host: str
    redis_port: int
    redis_db: int
    # Every Redis key we create is prefixed with this namespace. It lets a
    # single Redis instance be shared across apps without key collisions, and
    # makes our keys easy to find/flush in isolation (e.g. KEYS "taskq:*").
    namespace: str


def load_config() -> Config:
    return Config(
        redis_host=os.getenv("TQ_REDIS_HOST", "localhost"),
        redis_port=int(os.getenv("TQ_REDIS_PORT", "6379")),
        redis_db=int(os.getenv("TQ_REDIS_DB", "0")),
        namespace=os.getenv("TQ_NAMESPACE", "taskq"),
    )
