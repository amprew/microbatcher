"""The asyncio micro-batcher.

Design in one paragraph: there is no long-lived background task. All state transitions happen in
one synchronous method, ``_pump``, which is called whenever something changes (an item arrives, a
flush finishes, a timer fires, ``flush()``/``close()`` is called). ``_pump`` starts as many
batches as the triggers and the concurrency limit allow, then (re)arms a single timer for the
oldest eligible item. Because the timer callback only calls ``_pump`` (and logs if it fails),
there is no background task whose exception can vanish silently.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
from collections import deque
from collections.abc import Awaitable, Callable, Hashable, Iterable, Sequence
from typing import Any, Generic, Literal, TypeVar

from ._clock import AsyncioClock, Clock, TimerHandle
from .errors import (
    BackpressureError,
    BatcherClosedError,
    DrainTimeoutError,
    FlushResultError,
    FlushTimeoutError,
    ItemDroppedError,
)
from .retry import RetryPolicy
from .stats import Stats

T = TypeVar("T")
R = TypeVar("R")

logger = logging.getLogger("microbatcher")

OnFull = Literal["block", "raise", "drop"]
FlushOutcome = None | Sequence[Any]
FlushCallable = Callable[[list[T]], FlushOutcome | Awaitable[FlushOutcome]]

_EPS = 1e-9  # tolerance for float time comparisons
_UNSET: Any = object()


def default_sizer(value: Any) -> int:
    """Size in bytes for ``bytes``-like and ``str`` values; ``len()`` for anything else."""
    if isinstance(value, str):
        return len(value.encode("utf-8"))
    if isinstance(value, memoryview):
        return value.nbytes
    return len(value)  # bytes, bytearray, dict, list, ... ; raises TypeError otherwise


def _retrieve_exception(fut: asyncio.Future[Any]) -> None:
    """Mark a future's exception as retrieved so asyncio never logs 'never retrieved' noise.

    Failures are never lost: they reach callers via ``submit``, and ``on_error`` / logging
    cover fire-and-forget submissions.
    """
    if not fut.cancelled():
        fut.exception()


class _Item:
    __slots__ = ("value", "future", "key", "size", "seq", "enqueued_at", "attempts", "urgent")

    def __init__(
        self,
        value: Any,
        future: asyncio.Future[Any],
        key: Hashable,
        size: int,
        seq: int,
        enqueued_at: float,
    ) -> None:
        self.value = value
        self.future = future
        self.key = key
        self.size = size
        self.seq = seq
        self.enqueued_at = enqueued_at
        self.attempts = 0
        self.urgent = False  # set on retried items: flush as soon as the backoff has elapsed


class Batcher(Generic[T, R]):
    """Collect items submitted one at a time and hand them to ``flush`` in batches.

    Args:
        flush: Callable taking ``list[T]``. May be ``async def`` or a plain function (plain
            functions run in a worker thread so they cannot block the event loop). It may
            return ``None`` (everything succeeded, results are ``None``) or a sequence with
            exactly one entry per item, in order. An entry that is an ``Exception`` instance
            marks that single item as failed; any other value is that item's result. If it
            raises, the whole batch is treated as failed.
        max_size: Flush as soon as this many items are waiting.
        max_wait: Flush at most this many seconds after the *first* item of a batch arrived
            (measured from the oldest waiting item, so a trickle of items cannot starve a
            flush). ``0`` means "everything submitted in the same event-loop tick".
        max_bytes: Optional flush trigger / batch cap on summed item size. A single item larger
            than ``max_bytes`` is still sent, alone.
        sizer: ``item -> int`` used with ``max_bytes`` (default: byte length of str/bytes).
        max_pending: Upper bound on items accepted but not yet resolved (queued + in flight +
            waiting to retry). This is the backpressure bound.
        on_full: What ``submit`` does at ``max_pending``: ``"block"`` (wait for room, FIFO),
            ``"raise"`` (``BackpressureError``) or ``"drop"`` (``ItemDroppedError``).
        max_concurrent_flushes: Number of flush calls allowed in flight at once.
        key: Optional ``item -> hashable``. When given, items with the same key are never in two
            in-flight batches at once and keep their submission order across batches.
        retry: Optional ``RetryPolicy``. Only failed items (or the whole batch, if ``flush``
            raised) are retried.
        flush_timeout: Per-flush deadline in seconds; exceeding it fails the batch with
            ``FlushTimeoutError`` (retryable by default). A plain-function flush running in a
            thread cannot be interrupted, only abandoned.
        drain_timeout: Default deadline for ``close()`` / leaving ``async with``.
        on_error: ``(item, exc)`` callback for every item that finally fails or is dropped.
            Useful for fire-and-forget submissions. Exceptions it raises are logged.
        clock: Injectable time source (see ``microbatcher.testing.FakeClock``).
    """

    def __init__(
        self,
        flush: FlushCallable[T],
        *,
        max_size: int = 100,
        max_wait: float = 0.05,
        max_bytes: int | None = None,
        sizer: Callable[[T], int] | None = None,
        max_pending: int = 10_000,
        on_full: OnFull = "block",
        max_concurrent_flushes: int = 1,
        key: Callable[[T], Hashable] | None = None,
        retry: RetryPolicy | None = None,
        flush_timeout: float | None = None,
        drain_timeout: float | None = 30.0,
        on_error: Callable[[T, BaseException], None] | None = None,
        clock: Clock | None = None,
    ) -> None:
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        if max_wait < 0:
            raise ValueError("max_wait must be >= 0")
        if max_bytes is not None and max_bytes < 1:
            raise ValueError("max_bytes must be >= 1")
        if max_pending < max_size:
            raise ValueError("max_pending must be >= max_size")
        if max_concurrent_flushes < 1:
            raise ValueError("max_concurrent_flushes must be >= 1")
        if on_full not in ("block", "raise", "drop"):
            raise ValueError("on_full must be 'block', 'raise' or 'drop'")
        if flush_timeout is not None and flush_timeout <= 0:
            raise ValueError("flush_timeout must be > 0")

        self._flush = flush
        self._flush_is_async = inspect.iscoroutinefunction(flush) or inspect.iscoroutinefunction(
            getattr(flush, "__call__", None)  # noqa: B004
        )
        self._max_size = max_size
        self._max_wait = max_wait
        self._max_bytes = max_bytes
        self._sizer: Callable[[Any], int] | None = sizer
        if max_bytes is not None and sizer is None:
            self._sizer = default_sizer
        self._max_pending = max_pending
        self._on_full = on_full
        self._max_concurrent = max_concurrent_flushes
        self._key_fn: Callable[[Any], Hashable] | None = key
        self._retry = retry
        self._flush_timeout = flush_timeout
        self._drain_timeout = drain_timeout
        self._on_error = on_error
        self._clock: Clock = clock if clock is not None else AsyncioClock()

        # --- state -------------------------------------------------------------------------
        self._queue: deque[_Item] = deque()
        self._queue_bytes = 0
        self._live: set[_Item] = set()  # accepted and unresolved (queued, in flight, retrying)
        self._pending = 0  # slots taken; == len(self._live) except while a waiter is mid-grant
        self._waiters: deque[asyncio.Future[None]] = deque()
        self._inflight: set[asyncio.Task[None]] = set()
        self._busy_keys: set[Hashable] = set()
        self._retry_handles: set[TimerHandle] = set()
        self._timer: TimerHandle | None = None
        self._timer_deadline: float | None = None
        self._seq = 0
        self._force_seq = 0  # items with seq <= this are flushed without waiting (flush())
        self._closing = False
        self._hard_stopped = False
        self._idle = asyncio.Event()
        self._idle.set()
        # counters
        self._n_submitted = 0
        self._n_succeeded = 0
        self._n_failed = 0
        self._n_dropped = 0
        self._n_retried = 0
        self._n_batches = 0

    # ======================================================================================
    # Public API
    # ======================================================================================

    async def submit(self, item: T) -> R:
        """Submit one item and wait for *its* result.

        Applies the ``on_full`` policy when the batcher is at ``max_pending``. Cancelling the
        caller does **not** withdraw an item that was already accepted; it will still be flushed.
        """
        fut = await self._enqueue(item)
        return await asyncio.shield(fut)

    def submit_nowait(self, item: T) -> asyncio.Future[R]:
        """Submit without waiting and get a future for the item's result.

        Never blocks: at ``max_pending`` it raises ``BackpressureError`` (``on_full="drop"``
        returns an already-failed future instead). Use ``on_error`` to observe failures of
        items whose future you do not await.
        """
        return self._enqueue_nowait(item)

    async def submit_many(self, items: Iterable[T], *, return_exceptions: bool = False) -> list[R]:
        """Submit several items (respecting backpressure) and gather their results in order."""
        futures = [await self._enqueue(item) for item in items]
        results = await asyncio.gather(
            *(asyncio.shield(f) for f in futures), return_exceptions=return_exceptions
        )
        return list(results)  # type: ignore[arg-type]

    async def flush(self) -> None:
        """Flush everything submitted so far without waiting for the triggers, then wait until
        those items have settled (including retries). Item failures do not raise here; they
        surface on the item futures."""
        if not self._live:
            return
        futures = [item.future for item in self._live]
        self._force_seq = self._seq
        self._pump()
        await asyncio.wait(futures)

    async def close(self, timeout: float | None = _UNSET) -> None:
        """Stop accepting items, flush everything pending and wait for it to settle.

        Callers blocked on capacity are failed with ``BatcherClosedError``. If the drain does
        not finish within ``timeout`` (default: ``drain_timeout``), in-flight flushes are
        cancelled, every unfinished item fails with ``DrainTimeoutError`` and that error is also
        raised from here. Safe to call more than once.
        """
        if timeout is _UNSET:
            timeout = self._drain_timeout
        if not self._closing:
            self._closing = True
            err = BatcherClosedError("batcher closed while waiting for capacity")
            while self._waiters:
                waiter = self._waiters.popleft()
                if not waiter.done():
                    waiter.set_exception(err)
            self._pump()

        timed_out = False
        handle: TimerHandle | None = None
        if timeout is not None:

            def _expire() -> None:
                nonlocal timed_out
                timed_out = True
                self._idle.set()

            handle = self._clock.call_later(timeout, _expire)
        try:
            await self._idle.wait()
        finally:
            if handle is not None:
                handle.cancel()

        if timed_out and self._live:
            abandoned = len(self._live)
            drain_err = DrainTimeoutError(
                f"drain did not finish within {timeout}s; {abandoned} item(s) abandoned"
            )
            self._abort(drain_err)
            raise drain_err

    async def __aenter__(self) -> Batcher[T, R]:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    @property
    def stats(self) -> Stats:
        return Stats(
            pending=self._pending,
            queued=len(self._queue),
            in_flight=len(self._inflight),
            submitted=self._n_submitted,
            succeeded=self._n_succeeded,
            failed=self._n_failed,
            dropped=self._n_dropped,
            retried=self._n_retried,
            batches=self._n_batches,
        )

    @property
    def pending(self) -> int:
        return self._pending

    @property
    def closed(self) -> bool:
        return self._closing

    def __repr__(self) -> str:
        s = self.stats
        return (
            f"<Batcher pending={s.pending} queued={s.queued} in_flight={s.in_flight} "
            f"closed={self._closing}>"
        )

    # ======================================================================================
    # Admission control / backpressure
    # ======================================================================================

    def _check_open(self) -> None:
        if self._closing:
            raise BatcherClosedError("batcher is closed")

    def _measure(self, value: Any) -> tuple[Hashable, int]:
        key = self._key_fn(value) if self._key_fn is not None else None
        size = self._sizer(value) if self._sizer is not None else 0
        return key, size

    def _try_take_slot(self) -> bool:
        # Waiters get strict FIFO priority, so a newcomer may not jump the queue.
        if self._pending < self._max_pending and not self._waiters:
            self._pending += 1
            self._idle.clear()
            return True
        return False

    def _release(self, n: int) -> None:
        self._pending -= n
        self._grant_waiters()
        if self._pending == 0:
            self._idle.set()

    def _grant_waiters(self) -> None:
        while self._waiters and self._pending < self._max_pending:
            waiter = self._waiters.popleft()
            if waiter.done():  # cancelled while waiting
                continue
            self._pending += 1
            self._idle.clear()
            waiter.set_result(None)

    async def _wait_for_slot(self) -> None:
        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append(waiter)
        try:
            await waiter
        except asyncio.CancelledError:
            if waiter.done() and not waiter.cancelled() and waiter.exception() is None:
                self._release(1)  # slot was granted just as we were cancelled: give it back
            else:
                with contextlib.suppress(ValueError):
                    self._waiters.remove(waiter)
            raise

    async def _enqueue(self, value: T) -> asyncio.Future[R]:
        self._check_open()
        key, size = self._measure(value)
        if not self._try_take_slot():
            if self._on_full == "raise":
                raise BackpressureError(f"batcher is full ({self._max_pending} pending)")
            if self._on_full == "drop":
                return self._drop(value)
            await self._wait_for_slot()
            if self._closing:  # closed between the grant and this coroutine resuming
                self._release(1)
                raise BatcherClosedError("batcher is closed")
        return self._admit(value, key, size)

    def _enqueue_nowait(self, value: T) -> asyncio.Future[R]:
        self._check_open()
        key, size = self._measure(value)
        if not self._try_take_slot():
            if self._on_full == "drop":
                return self._drop(value)
            raise BackpressureError(f"batcher is full ({self._max_pending} pending)")
        return self._admit(value, key, size)

    def _drop(self, value: T) -> asyncio.Future[R]:
        exc = ItemDroppedError(f"batcher is full ({self._max_pending} pending); item dropped")
        fut: asyncio.Future[R] = asyncio.get_running_loop().create_future()
        fut.set_exception(exc)
        fut.exception()  # mark retrieved
        self._n_dropped += 1
        self._notify_error(value, exc)
        return fut

    def _admit(self, value: T, key: Hashable, size: int) -> asyncio.Future[R]:
        fut: asyncio.Future[R] = asyncio.get_running_loop().create_future()
        fut.add_done_callback(_retrieve_exception)
        self._seq += 1
        item = _Item(value, fut, key, size, self._seq, self._clock.now())
        self._live.add(item)
        self._queue.append(item)
        self._queue_bytes += size
        self._n_submitted += 1
        self._after_enqueue()
        return fut

    def _after_enqueue(self) -> None:
        if self._max_wait > 0 or self._size_hit():
            self._pump()
        else:
            # max_wait == 0: let everything submitted in this loop tick coalesce first.
            self._arm_timer()

    # ======================================================================================
    # The state machine: triggers, batch selection, timer
    # ======================================================================================

    def _size_hit(self) -> bool:
        return len(self._queue) >= self._max_size or (
            self._max_bytes is not None and self._queue_bytes >= self._max_bytes
        )

    def _due(self, item: _Item, now: float) -> bool:
        return now - item.enqueued_at >= self._max_wait - _EPS

    def _forced(self, item: _Item) -> bool:
        return item.urgent or item.seq <= self._force_seq

    def _pump(self) -> None:
        """Start every batch that is ready and re-arm the timer. Synchronous and idempotent."""
        while len(self._inflight) < self._max_concurrent:
            batch = self._select_batch()
            if not batch:
                break
            self._start_batch(batch)
        self._arm_timer()

    def _pump_guarded(self) -> None:
        try:
            self._pump()
        except Exception:
            logger.exception("microbatcher: error while dispatching batches")

    def _select_batch(self) -> list[_Item]:
        queue = self._queue
        if not queue:
            return []
        now = self._clock.now()
        ordered = self._key_fn is not None
        forced = self._closing or self._forced(queue[0])

        if not ordered and not forced:  # O(1) rejection for the common "nothing to do" case
            if not (self._size_hit() or self._due(queue[0], now)):
                return []

        batch: list[_Item] = []
        nbytes = 0
        limit_hit = False
        for item in queue:
            if ordered and item.key in self._busy_keys:
                continue
            if batch and self._max_bytes is not None and nbytes + item.size > self._max_bytes:
                limit_hit = True
                break
            batch.append(item)
            nbytes += item.size
            if len(batch) >= self._max_size or (
                self._max_bytes is not None and nbytes >= self._max_bytes
            ):
                limit_hit = True
                break
        if not batch:
            return []
        if not (
            forced or limit_hit or self._due(batch[0], now) or any(self._forced(i) for i in batch)
        ):
            return []

        if ordered:
            chosen = set(batch)
            self._queue = deque(i for i in queue if i not in chosen)
        else:
            for _ in batch:
                queue.popleft()
        self._queue_bytes -= nbytes
        return batch

    def _first_eligible(self) -> _Item | None:
        if not self._queue:
            return None
        if self._key_fn is None:
            return self._queue[0]
        for item in self._queue:
            if item.key not in self._busy_keys:
                return item
        return None

    def _arm_timer(self) -> None:
        if len(self._inflight) >= self._max_concurrent:
            self._cancel_timer()  # a finishing flush will pump again
            return
        head = self._first_eligible()
        if head is None:
            self._cancel_timer()
            return
        deadline = head.enqueued_at + self._max_wait
        if self._timer is not None and self._timer_deadline == deadline:
            return
        self._cancel_timer()
        now = self._clock.now()
        delay = max(0.0, self._max_wait - (now - head.enqueued_at))
        self._timer_deadline = deadline
        self._timer = self._clock.call_later(delay, self._on_timer)

    def _cancel_timer(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
            self._timer_deadline = None

    def _on_timer(self) -> None:
        self._timer = None
        self._timer_deadline = None
        self._pump_guarded()

    # ======================================================================================
    # Running flushes
    # ======================================================================================

    def _start_batch(self, batch: list[_Item]) -> None:
        keys: set[Hashable] = set()
        if self._key_fn is not None:
            keys = {i.key for i in batch}
            self._busy_keys |= keys
        for item in batch:
            item.attempts += 1
        self._n_batches += 1
        task = asyncio.get_running_loop().create_task(self._run_batch(batch, keys))
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)

    async def _call_flush(self, values: list[Any]) -> FlushOutcome:
        if self._flush_is_async:
            return await self._flush(values)  # type: ignore[no-any-return, misc]
        result = await asyncio.to_thread(self._flush, values)
        if inspect.isawaitable(result):
            result = await result
        return result

    async def _timed_flush(self, values: list[Any]) -> FlushOutcome:
        if self._flush_timeout is None:
            return await self._call_flush(values)
        task = asyncio.ensure_future(self._call_flush(values))
        timed_out = False

        def _expire() -> None:
            nonlocal timed_out
            timed_out = True
            task.cancel()

        handle = self._clock.call_later(self._flush_timeout, _expire)
        try:
            return await task
        except asyncio.CancelledError:
            if timed_out:
                raise FlushTimeoutError(f"flush exceeded {self._flush_timeout}s") from None
            raise
        finally:
            handle.cancel()

    async def _run_batch(self, batch: list[_Item], keys: set[Hashable]) -> None:
        me = asyncio.current_task()
        retry_items: list[_Item] = []
        try:
            try:
                outcome = await self._timed_flush([i.value for i in batch])
            except Exception as exc:
                logger.debug("flush of %d item(s) failed: %r", len(batch), exc)
                retry_items = self._handle_failures([(i, exc) for i in batch])
            else:
                retry_items = self._handle_outcome(batch, outcome)
        except BaseException as exc:
            err: BaseException = exc
            if isinstance(exc, asyncio.CancelledError):
                err = BatcherClosedError("flush task was cancelled")
            for item in batch:
                self._reject(item, err)
            raise
        finally:
            if me is not None:
                self._inflight.discard(me)
            self._finish_batch(keys, retry_items)
            self._pump_guarded()

    def _handle_outcome(self, batch: list[_Item], outcome: FlushOutcome) -> list[_Item]:
        n = len(batch)
        results: Sequence[Any]
        if outcome is None:
            results = [None] * n
        elif (
            isinstance(outcome, (str, bytes, bytearray))
            or not isinstance(outcome, Sequence)
            or len(outcome) != n
        ):
            err = FlushResultError(
                f"flush must return None or a sequence of exactly {n} outcome(s) "
                f"(one per item, in order); got {type(outcome).__name__}"
                + (f" of length {len(outcome)}" if isinstance(outcome, Sequence) else "")
            )
            for item in batch:
                self._reject(item, err)
            return []
        else:
            results = outcome

        failures: list[tuple[_Item, BaseException]] = []
        for item, res in zip(batch, results, strict=True):
            if isinstance(res, Exception):
                failures.append((item, res))
            else:
                self._resolve(item, res)
        return self._handle_failures(failures)

    def _handle_failures(self, failures: list[tuple[_Item, BaseException]]) -> list[_Item]:
        retry: list[_Item] = []
        for item, exc in failures:
            if (
                self._retry is not None
                and not self._hard_stopped
                and self._retry.should_retry(item.attempts, exc)
            ):
                item.urgent = True
                retry.append(item)
            else:
                self._reject(item, exc)
        return retry

    def _finish_batch(self, keys: set[Hashable], retry_items: list[_Item]) -> None:
        """Release the batch's keys and park retries until their backoff has elapsed.

        Keys of items that will be retried stay busy until the retry is re-queued, so later
        items with the same key cannot overtake them.
        """
        retry_keys: set[Hashable] = set()
        if retry_items and self._key_fn is not None:
            retry_keys = {i.key for i in retry_items}
        self._busy_keys -= keys - retry_keys
        if not retry_items:
            return

        attempt = max(i.attempts for i in retry_items)
        assert self._retry is not None
        delay = self._retry.backoff.delay(attempt)
        self._n_retried += len(retry_items)
        handle: TimerHandle | None = None

        def _requeue() -> None:
            if handle is not None:
                self._retry_handles.discard(handle)
            self._busy_keys -= retry_keys
            alive = [i for i in retry_items if i in self._live]
            if alive:
                self._queue.extendleft(reversed(alive))  # retries go to the front, in order
                self._queue_bytes += sum(i.size for i in alive)
            self._pump_guarded()

        handle = self._clock.call_later(delay, _requeue)
        self._retry_handles.add(handle)

    # ======================================================================================
    # Settling items
    # ======================================================================================

    def _settle(self, item: _Item) -> bool:
        if item not in self._live:
            return False
        self._live.discard(item)
        self._release(1)
        return True

    def _resolve(self, item: _Item, value: Any) -> None:
        if self._settle(item):
            self._n_succeeded += 1
            if not item.future.done():
                item.future.set_result(value)

    def _reject(self, item: _Item, exc: BaseException) -> None:
        if self._settle(item):
            self._n_failed += 1
            if not item.future.done():
                item.future.set_exception(exc)
            self._notify_error(item.value, exc)

    def _notify_error(self, value: Any, exc: BaseException) -> None:
        if self._on_error is None:
            return
        try:
            self._on_error(value, exc)
        except Exception:
            logger.exception("microbatcher: on_error callback raised")

    def _abort(self, exc: BaseException) -> None:
        """Hard stop after a drain timeout: cancel flushes, fail everything unfinished."""
        self._hard_stopped = True
        self._cancel_timer()
        for handle in list(self._retry_handles):
            handle.cancel()
        self._retry_handles.clear()
        for task in list(self._inflight):
            task.cancel()
        self._inflight.clear()
        self._queue.clear()
        self._queue_bytes = 0
        self._busy_keys.clear()
        for item in list(self._live):
            self._reject(item, exc)
