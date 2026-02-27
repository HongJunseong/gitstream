"""
Exponential backoff utilities.

Strategy: truncated exponential backoff with full jitter.
  delay = random(0, min(cap, base * 2^attempt))

Reference: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
"""

import time
import random
import logging
from typing import Callable, Tuple, Type

logger = logging.getLogger(__name__)


def calc_backoff(
    attempt: int,
    base: float = 1.0,
    cap: float = 60.0,
    jitter: bool = True,
) -> float:
    """Return sleep duration for the given retry attempt (0-indexed)."""
    ceiling = min(cap, base * (2 ** attempt))
    return random.uniform(0.0, ceiling) if jitter else ceiling


def retry_with_backoff(
    func: Callable,
    *,
    max_retries: int = 5,
    base: float = 1.0,
    cap: float = 60.0,
    retryable: Tuple[Type[Exception], ...] = (Exception,),
):
    """
    Call *func* up to *max_retries* times, sleeping between failures.

    Raises the last exception if all attempts are exhausted.
    """
    last_exc: Exception | None = None

    for attempt in range(max_retries):
        try:
            return func()
        except retryable as exc:
            last_exc = exc
            if attempt == max_retries - 1:
                break
            delay = calc_backoff(attempt, base=base, cap=cap)
            logger.warning(
                "Attempt %d/%d failed (%s). Retrying in %.2fs …",
                attempt + 1,
                max_retries,
                exc,
                delay,
            )
            time.sleep(delay)

    raise last_exc  # type: ignore[misc]
