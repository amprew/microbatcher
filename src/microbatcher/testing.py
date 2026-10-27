"""Test helpers. Import from ``microbatcher.testing`` in your own test suite.

``FakeClock`` replaces real time for every timer inside a ``Batcher`` (max_wait, retry backoff,
flush_timeout, drain deadline), so time-dependent behaviour is tested without sleeping::

    clock = FakeClock()
    batcher = Batcher(flush, max_wait=0.05, clock=clock)
    fut = batcher.submit_nowait(1)
    await clock.advance(0.05)      # fires the max_wait timer, lets the flush run
    assert fut.done()
"""

from __future__ import annotations

import asyncio
import heapq
from collections.abc import Callable


class _FakeTimer:
    __slots__ = ("when", "seq", "callback", "cancelled")

    def __init__(self, when: float, seq: int, callback: Callable[[], None]) -> None:
        self.when = when
        self.seq = seq
        self.callback = callback
        self.cancelled = False

    def cancel(self) -> None:
        self.cancelled = True

    def __lt__(self, other: _FakeTimer) -> bool:
        return (self.when, self.seq) < (other.when, other.seq)


class FakeClock:
    """Manually advanced clock implementing the ``Clock`` protocol."""

    def __init__(self, start: float = 0.0) -> None:
        self._now = start
        self._seq = 0
        self._timers: list[_FakeTimer] = []

    def now(self) -> float:
        return self._now

    def call_later(self, delay: float, callback: Callable[[], None]) -> _FakeTimer:
        self._seq += 1
        timer = _FakeTimer(self._now + max(0.0, delay), self._seq, callback)
        heapq.heappush(self._timers, timer)
        return timer

    @property
    def pending_timers(self) -> int:
        return sum(1 for t in self._timers if not t.cancelled)

    async def settle(self, rounds: int = 20) -> None:
        """Yield to the event loop so ready tasks and callbacks can run."""
        for _ in range(rounds):
            await asyncio.sleep(0)

    async def advance(self, delta: float) -> None:
        """Move time forward, firing due timers in order and letting tasks run after each."""
        target = self._now + delta
        await self.settle()
        while True:
            while self._timers and self._timers[0].cancelled:
                heapq.heappop(self._timers)
            if not self._timers or self._timers[0].when > target:
                break
            timer = heapq.heappop(self._timers)
            self._now = max(self._now, timer.when)
            timer.callback()
            await self.settle()
        self._now = target
        await self.settle()
