"""Retry policy: capped exponential backoff with jitter."""
import random


def backoff_delay(attempt: int, base: float, cap: float, jitter: float,
                  rng=random) -> float:
    """Seconds to park a job before retrying it.

    `attempt` = how many executions have failed so far (1 = first failure).
    """
    # Exponential growth: 1st failure -> base, 2nd -> 2x, 3rd -> 4x...
    # Backing off harder each time rides out real outages instead of
    # hammering a dependency that is already struggling.
    delay = base * (2 ** (attempt - 1))

    # Cap it: beyond some ceiling, waiting longer adds staleness, not safety.
    delay = min(delay, cap)

    # Jitter (the interview-worthy part): when a shared dependency dies,
    # its failures are synchronized - hundreds of jobs fail together, back
    # off by the same delay, and stampede back at the same instant (the
    # "thundering herd"), often re-killing the dependency. Randomizing each
    # delay by +/-jitter decorrelates the retries and spreads the load.
    if jitter > 0:
        delay *= rng.uniform(1.0 - jitter, 1.0 + jitter)

    return max(delay, 0.05)
