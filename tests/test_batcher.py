from __future__ import annotations

import asyncio
import threading
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor

import pytest

from microbatcher import (
    Backoff,
    BackpressureError,
    Batcher,
    BatcherClosedError,
    DrainTimeoutError,
    FlushResultError,
    FlushTimeoutError,
    ItemDroppedError,
    RetryPolicy,
    SyncBatcher,
)
from microbatcher.testing import FakeClock


class Recorder:
    """Async flush that doubles every item and remembers each batch."""

    def __init__(self) -> None:
        self.batches: list[list] = []

    async def __call__(self, items):
        self.batches.append(list(items))
        return [x * 2 for x in items]


# ---------------------------------------------------------------- triggers


async def test_flushes_when_max_size_reached():
    rec = Recorder()
    b = Batcher(rec, max_size=3, max_wait=10, clock=FakeClock())
    results = await asyncio.gather(b.submit(1), b.submit(2), b.submit(3))
    assert results == [2, 4, 6]
    assert rec.batches == [[1, 2, 3]]
    await b.close()


async def test_max_wait_is_measured_from_first_item():
    clock, rec = FakeClock(), Recorder()
    b = Batcher(rec, max_size=100, max_wait=0.05, clock=clock)
    t1 = asyncio.ensure_future(b.submit(1))
    await clock.advance(0.03)
    t2 = asyncio.ensure_future(b.submit(2))  # a trickle must not push the deadline back
    await clock.advance(0.019)
    assert rec.batches == []
    await clock.advance(0.002)  # t = 0.051
    assert rec.batches == [[1, 2]]
    assert await t1 == 2 and await t2 == 4


async def test_max_wait_zero_coalesces_same_tick():
    clock, rec = FakeClock(), Recorder()
    b = Batcher(rec, max_size=100, max_wait=0, clock=clock)
    futs = [b.submit_nowait(i) for i in range(5)]
    assert rec.batches == []
    await clock.advance(0)
    assert rec.batches == [[0, 1, 2, 3, 4]]
    assert [f.result() for f in futs] == [0, 2, 4, 6, 8]


async def test_max_bytes_caps_batch_and_oversize_item_ships_alone():
    clock, rec = FakeClock(), Recorder()

    async def flush(items):
        rec.batches.append(list(items))

    b = Batcher(flush, max_size=100, max_wait=1, max_bytes=10, clock=clock)
    b.submit_nowait("aaaaaa")
    b.submit_nowait("bbbbbb")  # 12 bytes queued -> first batch must not exceed 10
    await clock.settle()
    assert rec.batches == [["aaaaaa"]]
    await clock.advance(1)
    assert rec.batches == [["aaaaaa"], ["bbbbbb"]]
    big = b.submit_nowait("x" * 50)
    await clock.settle()
    assert rec.batches[-1] == ["x" * 50] and big.done()
    await b.close()


async def test_manual_flush_does_not_wait_for_triggers():
    clock, rec = FakeClock(), Recorder()
    b = Batcher(rec, max_size=100, max_wait=100, clock=clock)
    f1, f2 = b.submit_nowait(1), b.submit_nowait(2)
    await b.flush()
    assert rec.batches == [[1, 2]]
    assert (f1.result(), f2.result()) == (2, 4)


# ---------------------------------------------------------------- results & failure


async def test_flush_returning_none_resolves_with_none():
    clock = FakeClock()

    async def flush(items):
        return None

    b = Batcher(flush, max_size=2, max_wait=10, clock=clock)
    assert await asyncio.gather(b.submit("a"), b.submit("b")) == [None, None]


async def test_partial_failure_fails_only_affected_items():
    async def flush(items):
        return [x if x % 2 else ValueError(f"bad {x}") for x in items]

    b = Batcher(flush, max_size=4, max_wait=10, clock=FakeClock())
    res = await b.submit_many([1, 2, 3, 4], return_exceptions=True)
    assert res[0] == 1 and res[2] == 3
    assert isinstance(res[1], ValueError) and isinstance(res[3], ValueError)
    assert b.stats.succeeded == 2 and b.stats.failed == 2


async def test_whole_batch_exception_fails_every_item():
    async def flush(items):
        raise RuntimeError("down")

    b = Batcher(flush, max_size=2, max_wait=10, clock=FakeClock())
    res = await b.submit_many([1, 2], return_exceptions=True)
    assert all(isinstance(r, RuntimeError) for r in res)


@pytest.mark.parametrize("bad", [[1], "ab", 5])
async def test_wrong_result_shape_is_rejected_not_retried(bad):
    calls = 0

    async def flush(items):
        nonlocal calls
        calls += 1
        return bad

    retry = RetryPolicy(max_attempts=5, backoff=Backoff(jitter=0))
    b = Batcher(flush, max_size=2, max_wait=10, retry=retry, clock=FakeClock())
    res = await b.submit_many([1, 2], return_exceptions=True)
    assert all(isinstance(r, FlushResultError) for r in res)
    assert calls == 1


async def test_on_error_called_for_fire_and_forget_failures():
    seen = []

    async def flush(items):
        return [ValueError("x")] * len(items)

    b = Batcher(
        flush, max_size=1, max_wait=10, clock=FakeClock(), on_error=lambda v, e: seen.append(v)
    )
    b.submit_nowait("lost")
    await b.flush()
    assert seen == ["lost"]


# ---------------------------------------------------------------- retries


async def test_only_failed_items_are_retried_with_backoff():
    clock = FakeClock()
    attempts: Counter = Counter()
    batches: list[list] = []

    async def flush(items):
        batches.append(list(items))
        out = []
        for x in items:
            attempts[x] += 1
            out.append(ConnectionError("flaky") if x == 2 and attempts[x] < 3 else x)
        return out

    retry = RetryPolicy(max_attempts=3, backoff=Backoff(initial=0.1, factor=2, jitter=0))
    b = Batcher(flush, max_size=3, max_wait=10, retry=retry, clock=clock)
    tasks = [asyncio.ensure_future(b.submit(i)) for i in (1, 2, 3)]
    await clock.settle()
    assert tasks[0].done() and tasks[2].done() and not tasks[1].done()
    assert batches == [[1, 2, 3]]

    await clock.advance(0.099)
    assert batches == [[1, 2, 3]]  # backoff not elapsed
    await clock.advance(0.002)
    assert batches == [[1, 2, 3], [2]]  # only the failed item, as soon as backoff elapsed
    await clock.advance(0.2)
    assert batches == [[1, 2, 3], [2], [2]]
    assert await tasks[1] == 2
    assert b.stats.retried == 2


async def test_retry_gives_up_after_max_attempts():
    clock = FakeClock()

    async def flush(items):
        raise TimeoutError("nope")

    retry = RetryPolicy(max_attempts=2, backoff=Backoff(initial=0.1, jitter=0))
    b = Batcher(flush, max_size=1, max_wait=10, retry=retry, clock=clock)
    t = asyncio.ensure_future(b.submit(1))
    await clock.advance(1)
    with pytest.raises(TimeoutError):
        await t
    assert b.stats.batches == 2


async def test_retry_on_predicate_can_make_errors_permanent():
    clock = FakeClock()
    calls = 0

    async def flush(items):
        nonlocal calls
        calls += 1
        raise ValueError("permanent")

    retry = RetryPolicy(max_attempts=5, retry_on=lambda e: not isinstance(e, ValueError))
    b = Batcher(flush, max_size=1, max_wait=10, retry=retry, clock=clock)
    with pytest.raises(ValueError):
        await b.submit(1)
    assert calls == 1


async def test_retries_count_against_backpressure():
    clock = FakeClock()
    gate = asyncio.Event()

    async def flush(items):
        if not gate.is_set():
            raise RuntimeError("fail")
        return items

    retry = RetryPolicy(max_attempts=10, backoff=Backoff(initial=1, jitter=0))
    b = Batcher(
        flush, max_size=1, max_wait=10, max_pending=1, on_full="raise", retry=retry, clock=clock
    )
    b.submit_nowait(1)
    await clock.settle()  # failed once, now parked in retry backoff
    assert b.pending == 1
    with pytest.raises(BackpressureError):
        b.submit_nowait(2)
    gate.set()
    await clock.advance(1)
    assert b.pending == 0


# ---------------------------------------------------------------- backpressure


async def test_block_policy_waits_for_room():
    clock = FakeClock()
    gate = asyncio.Event()

    async def flush(items):
        await gate.wait()
        return items

    b = Batcher(flush, max_size=2, max_wait=10, max_pending=2, on_full="block", clock=clock)
    t1 = asyncio.ensure_future(b.submit(1))
    t2 = asyncio.ensure_future(b.submit(2))
    await clock.settle()
    t3 = asyncio.ensure_future(b.submit(3))
    await clock.settle()
    assert not t3.done() and b.pending == 2
    gate.set()
    await clock.settle()
    assert (await t1, await t2) == (1, 2)
    await clock.advance(10)
    assert await t3 == 3


async def test_cancelled_waiter_gives_slot_back():
    clock = FakeClock()
    gate = asyncio.Event()

    async def flush(items):
        await gate.wait()
        return items

    b = Batcher(flush, max_size=1, max_wait=10, max_pending=1, clock=clock)
    t1 = asyncio.ensure_future(b.submit(1))
    await clock.settle()
    t2 = asyncio.ensure_future(b.submit(2))
    await clock.settle()
    t2.cancel()
    await clock.settle()
    gate.set()
    await clock.settle()
    assert await t1 == 1
    assert b.pending == 0
    assert await asyncio.wait_for(b.submit(3), 1) == 3  # capacity is intact


async def test_raise_and_drop_policies():
    clock = FakeClock()
    gate = asyncio.Event()
    dropped = []

    async def flush(items):
        await gate.wait()
        return items

    r = Batcher(flush, max_size=1, max_pending=1, on_full="raise", clock=clock)
    r.submit_nowait(1)
    with pytest.raises(BackpressureError):
        r.submit_nowait(2)
    with pytest.raises(BackpressureError):
        await r.submit(2)

    d = Batcher(
        flush,
        max_size=1,
        max_pending=1,
        on_full="drop",
        clock=clock,
        on_error=lambda v, e: dropped.append((v, type(e))),
    )
    d.submit_nowait(1)
    with pytest.raises(ItemDroppedError):
        await d.submit(2)
    assert dropped == [(2, ItemDroppedError)] and d.stats.dropped == 1
    gate.set()
    await clock.settle()


async def test_cancelling_caller_does_not_withdraw_accepted_item():
    clock, rec = FakeClock(), Recorder()
    b = Batcher(rec, max_size=10, max_wait=1, clock=clock)
    t = asyncio.ensure_future(b.submit(7))
    await clock.settle()
    t.cancel()
    await clock.advance(1)
    assert rec.batches == [[7]]


# ---------------------------------------------------------------- concurrency & ordering


async def test_concurrency_limit_is_respected():
    clock = FakeClock()
    gate = asyncio.Event()
    active = peak = 0

    async def flush(items):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await gate.wait()
        active -= 1
        return items

    b = Batcher(flush, max_size=1, max_wait=10, max_concurrent_flushes=2, clock=clock)
    futs = [b.submit_nowait(i) for i in range(5)]
    await clock.settle()
    assert active == 2 and b.stats.in_flight == 2
    gate.set()
    await clock.settle()
    assert peak == 2 and [f.result() for f in futs] == list(range(5))


async def test_same_key_items_never_overlap_and_keep_order():
    clock = FakeClock()
    active: set[str] = set()
    order: dict[str, list[int]] = defaultdict(list)
    overlap = False

    async def flush(items):
        nonlocal overlap
        keys = {k for k, _ in items}
        if keys & active:
            overlap = True
        active.update(keys)
        for k, n in items:
            order[k].append(n)
        for _ in range(5):
            await asyncio.sleep(0)
        active.difference_update(keys)
        return items

    b = Batcher(
        flush, max_size=2, max_wait=0, max_concurrent_flushes=4, key=lambda x: x[0], clock=clock
    )
    items = [("a", 1), ("a", 2), ("b", 1), ("a", 3), ("b", 2), ("a", 4), ("c", 1), ("a", 5)]
    futs = [b.submit_nowait(i) for i in items]
    await clock.advance(0)
    await b.flush()
    assert all(f.done() for f in futs)
    assert not overlap
    assert order["a"] == [1, 2, 3, 4, 5] and order["b"] == [1, 2]


async def test_retrying_item_blocks_later_items_with_same_key():
    clock = FakeClock()
    seen: list[int] = []
    fail_once = {1}

    async def flush(items):
        out = []
        for _k, n in items:
            if n in fail_once:
                fail_once.discard(n)
                out.append(RuntimeError("once"))
            else:
                seen.append(n)
                out.append(n)
        return out

    retry = RetryPolicy(max_attempts=2, backoff=Backoff(initial=1, jitter=0))
    b = Batcher(
        flush,
        max_size=1,
        max_wait=0,
        max_concurrent_flushes=2,
        key=lambda x: x[0],
        retry=retry,
        clock=clock,
    )
    b.submit_nowait(("a", 1))
    await clock.advance(0)
    b.submit_nowait(("a", 2))  # arrives while ("a", 1) waits for its retry
    await clock.advance(0.5)
    assert seen == []  # a2 must not overtake a1
    await clock.advance(1)
    assert seen == [1, 2]


# ---------------------------------------------------------------- flush callable kinds


async def test_sync_flush_runs_in_worker_thread():
    threads = []

    def flush(items):
        threads.append(threading.current_thread())
        return [x + 1 for x in items]

    b = Batcher(flush, max_size=2, max_wait=10, clock=FakeClock())
    res = await asyncio.wait_for(asyncio.gather(b.submit(1), b.submit(2)), 5)
    assert res == [2, 3]
    assert threads[0] is not threading.main_thread()


async def test_flush_timeout_fails_batch():
    clock = FakeClock()

    async def flush(items):
        await asyncio.Event().wait()

    b = Batcher(flush, max_size=1, max_wait=10, flush_timeout=1, clock=clock)
    t = asyncio.ensure_future(b.submit(1))
    await clock.settle()
    await clock.advance(1)
    with pytest.raises(FlushTimeoutError):
        await t


# ---------------------------------------------------------------- shutdown


async def test_close_drains_without_waiting_for_max_wait():
    clock, rec = FakeClock(), Recorder()
    b = Batcher(rec, max_size=100, max_wait=1000, clock=clock)
    fut = b.submit_nowait(21)
    await b.close()
    assert fut.result() == 42
    assert b.pending == 0
    with pytest.raises(BatcherClosedError):
        b.submit_nowait(1)
    with pytest.raises(BatcherClosedError):
        await b.submit(1)


async def test_async_with_drains_on_exit():
    clock, rec = FakeClock(), Recorder()
    async with Batcher(rec, max_size=100, max_wait=1000, clock=clock) as b:
        fut = b.submit_nowait(1)
    assert fut.done() and fut.result() == 2


async def test_close_wakes_blocked_submitters():
    clock = FakeClock()
    gate = asyncio.Event()

    async def flush(items):
        await gate.wait()
        return items

    b = Batcher(flush, max_size=1, max_wait=10, max_pending=1, clock=clock)
    t1 = asyncio.ensure_future(b.submit(1))
    await clock.settle()
    t2 = asyncio.ensure_future(b.submit(2))
    await clock.settle()
    closer = asyncio.ensure_future(b.close(timeout=None))
    await clock.settle()
    with pytest.raises(BatcherClosedError):
        await t2
    gate.set()
    await closer
    assert await t1 == 1


async def test_close_deadline_abandons_work_and_reports():
    clock = FakeClock()

    async def flush(items):
        await asyncio.Event().wait()

    b = Batcher(flush, max_size=1, max_wait=10, clock=clock)
    t = asyncio.ensure_future(b.submit(1))
    queued = b.submit_nowait(2)
    await clock.settle()
    closer = asyncio.ensure_future(b.close(timeout=1))
    await clock.settle()
    await clock.advance(1)
    with pytest.raises(DrainTimeoutError):
        await closer
    with pytest.raises(DrainTimeoutError):
        await t
    assert isinstance(queued.exception(), DrainTimeoutError)
    assert b.pending == 0 and b.stats.in_flight == 0


async def test_close_is_idempotent():
    b = Batcher(Recorder(), clock=FakeClock())
    await b.close()
    await b.close()


# ---------------------------------------------------------------- misc


async def test_stats_counters():
    clock, rec = FakeClock(), Recorder()
    b = Batcher(rec, max_size=2, max_wait=10, clock=clock)
    await b.submit_many([1, 2, 3, 4])
    s = b.stats
    assert (s.submitted, s.succeeded, s.failed, s.batches, s.pending) == (4, 4, 0, 2, 0)


def test_option_validation():
    rec = Recorder()
    with pytest.raises(ValueError):
        Batcher(rec, max_size=0)
    with pytest.raises(ValueError):
        Batcher(rec, max_size=10, max_pending=5)
    with pytest.raises(ValueError):
        Batcher(rec, on_full="explode")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        Batcher(rec, max_concurrent_flushes=0)


async def test_real_clock_smoke():
    rec = Recorder()
    async with Batcher(rec, max_size=1000, max_wait=0.01) as b:
        res = await asyncio.wait_for(asyncio.gather(*(b.submit(i) for i in range(50))), 5)
    assert res == [i * 2 for i in range(50)]
    assert sum(len(x) for x in rec.batches) == 50 and len(rec.batches) < 50


# ---------------------------------------------------------------- sync wrapper


def test_sync_batcher_batches_across_threads():
    batches: list[int] = []

    def flush(items):
        batches.append(len(items))
        return [x * 2 for x in items]

    with SyncBatcher(flush, max_size=10, max_wait=0.05) as sb:
        with ThreadPoolExecutor(20) as pool:
            results = list(pool.map(sb.submit, range(40)))
    assert results == [i * 2 for i in range(40)]
    assert sum(batches) == 40 and max(batches) <= 10 and len(batches) < 40


def test_sync_batcher_flush_close_and_errors():
    def flush(items):
        return [ValueError("no") if x < 0 else x for x in items]

    sb = SyncBatcher(flush, max_size=100, max_wait=10)
    f_ok = sb.submit_future(1)
    f_bad = sb.submit_future(-1)
    sb.flush(timeout=5)
    assert f_ok.result(1) == 1
    with pytest.raises(ValueError):
        f_bad.result(1)
    sb.close()
    sb.close()
    with pytest.raises(BatcherClosedError):
        sb.submit(1)
