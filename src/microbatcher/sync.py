"""Blocking facade over ``Batcher`` for threaded (non-asyncio) code.

A private event loop runs in a daemon thread; ``SyncBatcher`` just marshals calls onto it, so both
flavours share exactly one implementation of the batching logic.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from collections.abc import Iterable
from typing import Any, Generic, TypeVar

from .batcher import _UNSET, Batcher, FlushCallable
from .errors import BatcherClosedError
from .stats import Stats

T = TypeVar("T")
R = TypeVar("R")


class SyncBatcher(Generic[T, R]):
    """Thread-safe, blocking micro-batcher. Takes the same options as ``Batcher``.

    ``flush`` may be a plain function (it runs in a worker thread) or an ``async def``.
    ``submit`` blocks the calling thread until that item's batch has been flushed; call it from
    many threads at once to get batching. Do not call it from inside ``flush``.
    """

    def __init__(self, flush: FlushCallable[T], **options: Any) -> None:
        self._loop = asyncio.new_event_loop()
        self._batcher: Batcher[T, R] = Batcher(flush, **options)
        self._closed = False
        self._lock = threading.Lock()
        self._thread = threading.Thread(
            target=self._run_loop, name="microbatcher-loop", daemon=True
        )
        self._thread.start()

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_forever()
        finally:
            try:
                self._loop.run_until_complete(self._loop.shutdown_default_executor())
            finally:
                self._loop.close()

    def _check_open(self) -> None:
        if self._closed:
            raise BatcherClosedError("batcher is closed")

    def submit_future(self, item: T) -> concurrent.futures.Future[R]:
        """Submit and return a ``concurrent.futures.Future`` for the item's result.

        Backpressure (``on_full``) is applied on the batcher's loop, so ``"block"`` delays the
        future instead of blocking the caller, and ``"raise"``/``"drop"`` fail the future.
        """
        self._check_open()
        return asyncio.run_coroutine_threadsafe(self._batcher.submit(item), self._loop)

    def submit(self, item: T, timeout: float | None = None) -> R:
        """Submit one item and block until its result is available."""
        return self.submit_future(item).result(timeout)

    def submit_many(
        self,
        items: Iterable[T],
        *,
        return_exceptions: bool = False,
        timeout: float | None = None,
    ) -> list[R]:
        self._check_open()
        coro = self._batcher.submit_many(items, return_exceptions=return_exceptions)
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout)

    def flush(self, timeout: float | None = None) -> None:
        """Flush everything submitted so far and wait for it to settle."""
        self._check_open()
        asyncio.run_coroutine_threadsafe(self._batcher.flush(), self._loop).result(timeout)

    def close(self, timeout: float | None = _UNSET) -> None:
        """Drain, then stop the background loop. Raises ``DrainTimeoutError`` like ``Batcher``."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
        try:
            asyncio.run_coroutine_threadsafe(self._batcher.close(timeout), self._loop).result()
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join()

    def __enter__(self) -> SyncBatcher[T, R]:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    @property
    def stats(self) -> Stats:
        return self._batcher.stats
