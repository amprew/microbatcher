"""Retry policy for failed items."""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass, field


def retry_all(exc: BaseException) -> bool:
    """Default ``retry_on`` predicate: every exception is retryable."""
    return True


@dataclass(frozen=True)
class Backoff:
    """Exponential backoff: ``initial * factor ** (attempt - 1)``, capped at ``maximum``.

    ``jitter`` is the fraction of the delay that may be randomly shaved off (0 disables jitter,
    0.5 means the delay lands somewhere between 50% and 100% of the computed value).
    """

    initial: float = 0.1
    factor: float = 2.0
    maximum: float = 30.0
    jitter: float = 0.5

    def __post_init__(self) -> None:
        if self.initial < 0 or self.maximum < 0 or self.factor < 1:
            raise ValueError("initial/maximum must be >= 0 and factor >= 1")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter must be between 0 and 1")

    def delay(self, attempt: int, rng: Callable[[], float] = random.random) -> float:
        """Delay before the retry that follows failed attempt number ``attempt`` (1-based)."""
        base = min(self.maximum, self.initial * self.factor ** max(0, attempt - 1))
        return base * (1.0 - self.jitter * rng())


@dataclass(frozen=True)
class RetryPolicy:
    """How many times, how fast, and for which errors an item is retried.

    ``max_attempts`` counts the first try, so ``max_attempts=3`` means up to two retries.
    Retried items stay counted against ``max_pending`` the whole time, so retries can never
    bypass backpressure.
    """

    max_attempts: int = 3
    backoff: Backoff = field(default_factory=Backoff)
    retry_on: Callable[[BaseException], bool] = retry_all

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

    def should_retry(self, attempts_made: int, exc: BaseException) -> bool:
        return attempts_made < self.max_attempts and self.retry_on(exc)
