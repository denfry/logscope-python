from app.domain.retry import RetryPolicy


def test_retry_delay_is_bounded() -> None:
    policy = RetryPolicy(base_seconds=0.01, max_seconds=0.05, jitter_seconds=0)
    assert policy.next_delay(20) == 0.05


def test_retry_delay_grows_exponentially() -> None:
    policy = RetryPolicy(base_seconds=0.1, max_seconds=10, jitter_seconds=0)
    assert policy.next_delay(1) == 0.1
    assert policy.next_delay(3) == 0.4
