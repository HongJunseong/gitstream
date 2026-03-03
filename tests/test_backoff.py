import pytest
from unittest.mock import patch
from poller.utils.backoff import calc_backoff, retry_with_backoff


# ── calc_backoff ─────────────────────────────────────────────────────────── #

def test_calc_backoff_no_jitter_doubles():
    """jitter 없이 attempt 마다 2배씩 증가."""
    assert calc_backoff(0, base=1.0, cap=60.0, jitter=False) == 1.0
    assert calc_backoff(1, base=1.0, cap=60.0, jitter=False) == 2.0
    assert calc_backoff(2, base=1.0, cap=60.0, jitter=False) == 4.0


def test_calc_backoff_caps_at_max():
    """cap 초과 시 cap 값으로 고정."""
    assert calc_backoff(10, base=1.0, cap=60.0, jitter=False) == 60.0


def test_calc_backoff_with_jitter_in_range():
    """jitter 있으면 [0, ceiling] 범위 안에 있어야 한다."""
    for _ in range(30):
        result = calc_backoff(3, base=1.0, cap=60.0, jitter=True)
        assert 0.0 <= result <= 8.0  # ceiling = min(60, 1 * 2^3) = 8


# ── retry_with_backoff ───────────────────────────────────────────────────── #

def test_retry_success_on_first_call():
    """첫 번째 호출에 성공하면 재시도 없이 반환."""
    with patch("poller.utils.backoff.time.sleep"):
        calls = []

        def func():
            calls.append(1)
            return "ok"

        assert retry_with_backoff(func, max_retries=3) == "ok"
        assert len(calls) == 1


def test_retry_succeeds_after_failures():
    """실패 후 재시도해서 성공하는 경우."""
    with patch("poller.utils.backoff.time.sleep"):
        calls = []

        def func():
            calls.append(1)
            if len(calls) < 3:
                raise ValueError("not yet")
            return "ok"

        result = retry_with_backoff(func, max_retries=5, retryable=(ValueError,))
        assert result == "ok"
        assert len(calls) == 3


def test_retry_exhausts_and_raises():
    """max_retries 소진 시 마지막 예외를 raise."""
    with patch("poller.utils.backoff.time.sleep"):
        def func():
            raise RuntimeError("always fails")

        with pytest.raises(RuntimeError, match="always fails"):
            retry_with_backoff(func, max_retries=3)


def test_retry_non_retryable_exception_propagates_immediately():
    """retryable 에 포함되지 않은 예외는 즉시 전파."""
    with patch("poller.utils.backoff.time.sleep"):
        calls = []

        def func():
            calls.append(1)
            raise TypeError("not retryable")

        with pytest.raises(TypeError):
            retry_with_backoff(func, max_retries=5, retryable=(ValueError,))

        assert len(calls) == 1
