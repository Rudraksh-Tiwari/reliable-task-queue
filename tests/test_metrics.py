"""Unit tests for metrics maths. The percentile estimator needs no Redis."""
from taskq.metrics import Metrics


def pct(buckets, count, q=0.95):
    return Metrics._percentile(buckets, count, q)


def test_percentile_of_empty_histogram_is_zero():
    assert pct({10: 0, 100: 0}, 0) == 0.0


def test_percentile_picks_the_first_bucket_crossing_the_rank():
    # Cumulative buckets: 90 jobs <= 10ms, all 100 <= 100ms.
    # The 95th job falls in the 100ms bucket, so p95 is reported as 100.
    assert pct({10: 90, 100: 100, 1000: 100}, 100) == 100.0


def test_percentile_when_everything_is_fast():
    # All 100 jobs under 10ms - p95 must be the smallest bucket, not larger.
    assert pct({10: 100, 100: 100}, 100) == 10.0


def test_percentile_returns_infinity_when_buckets_do_not_cover_the_rank():
    # Only 50 of 100 jobs fit the largest bucket, so the p95 is beyond the
    # histogram's range. Reporting inf is honest; guessing would not be.
    assert pct({10: 20, 100: 50}, 100) == float("inf")


def test_median_uses_the_same_rule():
    assert pct({10: 40, 100: 60, 1000: 100}, 100, q=0.5) == 100.0


def test_percentile_is_an_upper_bound_estimate():
    # Documents the trade-off: with one job of 1ms, p95 reports the bucket
    # bound (10ms), not the true value. Exactness would need every sample.
    assert pct({10: 1, 100: 1}, 1) == 10.0
