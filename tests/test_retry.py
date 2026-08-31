"""Unit tests for the retry policy. No Redis needed - pure arithmetic."""
import random

import pytest

from taskq.retry import backoff_delay


class FixedRng:
    """Stand-in for `random` that returns a chosen point in the range."""

    def __init__(self, value: float):
        self.value = value

    def uniform(self, low: float, high: float) -> float:
        return low + (high - low) * self.value


def test_delay_doubles_each_attempt():
    # No jitter, generous cap: the delay should be exactly base * 2^(n-1).
    delays = [backoff_delay(n, base=1.0, cap=1000, jitter=0.0)
              for n in (1, 2, 3, 4, 5)]
    assert delays == [1.0, 2.0, 4.0, 8.0, 16.0]


def test_delay_is_capped():
    # Growth must stop at the cap, or a job retried many times would be
    # scheduled effectively forever into the future.
    assert backoff_delay(20, base=1.0, cap=60, jitter=0.0) == 60


def test_jitter_stays_within_bounds():
    # With 25% jitter the 4s delay must land inside [3.0, 5.0], never outside.
    for _ in range(200):
        delay = backoff_delay(3, base=1.0, cap=1000, jitter=0.25)
        assert 3.0 <= delay <= 5.0


def test_jitter_spreads_delays_apart():
    # The whole point of jitter is decorrelation: identical failures must
    # NOT produce identical delays, or they stampede back together.
    delays = {backoff_delay(3, base=1.0, cap=1000, jitter=0.25)
              for _ in range(50)}
    assert len(delays) > 40, "jitter should produce distinct delays"


def test_jitter_endpoints_are_exact():
    # Verify the jitter window is centred on the un-jittered delay rather
    # than biased to one side.
    low = backoff_delay(1, base=10, cap=1000, jitter=0.5, rng=FixedRng(0.0))
    high = backoff_delay(1, base=10, cap=1000, jitter=0.5, rng=FixedRng(1.0))
    mid = backoff_delay(1, base=10, cap=1000, jitter=0.5, rng=FixedRng(0.5))
    assert (low, mid, high) == (5.0, 10.0, 15.0)


def test_delay_never_returns_zero():
    # A zero delay would mean an immediate hot retry loop.
    assert backoff_delay(1, base=0.0, cap=10, jitter=0.0) > 0
    assert backoff_delay(1, base=0.001, cap=10, jitter=0.9,
                         rng=FixedRng(0.0)) > 0


def test_cap_applies_before_jitter_so_result_can_exceed_cap_slightly():
    # Documents real behaviour: jitter is applied AFTER the cap, so the
    # final delay can sit a little above it. That is intentional - the cap
    # bounds growth, and jitter must still decorrelate capped retries.
    delay = backoff_delay(50, base=1.0, cap=10, jitter=0.25,
                          rng=FixedRng(1.0))
    assert delay == pytest.approx(12.5)
