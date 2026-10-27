"""Clock abstraction so every timer in the library can be driven deterministically in tests."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Protocol


class TimerHandle(Protocol):
    """Anything with a ``cancel()`` method (``asyncio.TimerHandle`` qualifies)."""

    def cancel(self) -> None: ...


class Clock(Protocol):
    """Source of time and one-shot timers.

    ``call_later`` is always invoked from inside the running event loop. Callbacks must be
    plain synchronous callables.
    """

    def now(self) -> float: ...

    def call_later(self, delay: float, callback: Callable[[], None]) -> TimerHandle: ...


class AsyncioClock:
    """Default clock: ``time.monotonic`` plus ``loop.call_later``."""

    __slots__ = ()

    def now(self) -> float:
        return time.monotonic()

    def call_later(self, delay: float, callback: Callable[[], None]) -> TimerHandle:
        return asyncio.get_running_loop().call_later(delay, callback)
