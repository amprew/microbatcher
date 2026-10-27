# microbatcher

Collect items submitted one at a time, flush them to a bulk API in batches, and hand every caller **its own result**.

Zero dependencies. Python 3.10+. Fully typed. asyncio-native with a thread-friendly sync wrapper.

```python
import asyncio
from microbatcher import Batcher


async def insert_rows(rows: list[dict]) -> list[int]:
    ids = await db.bulk_insert(rows)  # one round trip for the whole batch
    return ids  # one result per item, in order


async def main():
    async with Batcher(
        flush=insert_rows,
        max_size=500,
        max_wait=0.05
    ) as batcher:
        new_id = await batcher.submit({"name": "ada"})  # resolves when *this* item's batch is done
```

## Why

Every codebase has a hand-rolled "list + timer + lock" for `INSERT` batching, SQS `SendMessageBatch`, Kinesis `PutRecords`, or bulk HTTP endpoints, and most get the same details wrong. microbatcher implements those details once:

| Problem | What microbatcher does |
|---|---|
| Caller needs to know if *its* item succeeded | `submit()` returns a per-item future-backed result (DataLoader style) |
| Bulk APIs accept 97 of 100 items | `flush` returns per-item outcomes; only failed items fail or retry |
| Trickle of items starves the flush | `max_wait` is measured from the **first** item in the batch |
| Producer outruns the sink | Bounded `max_pending` with explicit `block` / `raise` / `drop` policy |
| SQS 256 KB / Kinesis 5 MB limits | `max_bytes` + `sizer` batch cap (oversize items ship alone) |
| Retry storms | Retries only re-send failed items, with backoff + jitter, and still count against `max_pending` |
| Per-account ordering | `key=` guarantees same-key items are never in flight twice and keep their order |
| Data lost on shutdown | `close()` drains, with a deadline; `async with` does it for you |
| Timer task dies silently | No background task: one synchronous state machine, timer callback is guarded and logged |
| Untestable timing | Every timer goes through an injectable `Clock`; `FakeClock` is included |

## Install

```bash
# THIS PACKAGE IS NOT YET DISTRIBUTED...
pip install TODO
```

## The flush contract

`flush` receives `list[T]` and may be `async def` or a plain function (plain functions run in a worker thread so they can't block the loop). It may:

* return `None` → every item succeeded; each caller gets `None`
* return a sequence with **exactly one entry per item, in order** → entries that are `Exception` instances fail just that item; anything else is that item's result
* raise → the whole batch failed

```python
async def send_batch(msgs: list[Msg]) -> list[str | Exception]:
    resp = await sqs.send_message_batch(Entries=[to_entry(i, m) for i, m in enumerate(msgs)])
    out: list[str | Exception] = [RuntimeError("missing")] * len(msgs)
    for ok in resp.get("Successful", []):
        out[int(ok["Id"])] = ok["MessageId"]
    for bad in resp.get("Failed", []):
        out[int(bad["Id"])] = SendError(bad["Code"])
    return out
```

Returning anything else (wrong length, a string) fails the batch with `FlushResultError`; it is not retried.

## Options

```python
Batcher(
    flush,
    max_size=100,  # flush when this many items are waiting
    max_wait=0.05,  # ...or this long after the FIRST waiting item (0 = same loop tick)
    max_bytes=None,  # ...or when summed size reaches this
    sizer=None,  # item -> int (default: byte length of str/bytes)
    max_pending=10_000,  # backpressure bound: queued + in flight + waiting to retry
    on_full="block",  # "block" | "raise" | "drop"
    max_concurrent_flushes=1,
    key=None,  # item -> hashable; enables per-key ordering
    retry=None,  # RetryPolicy(max_attempts=3, backoff=Backoff(...), retry_on=...)
    flush_timeout=None,  # per-flush deadline -> FlushTimeoutError
    drain_timeout=30.0,  # default deadline for close() / async with
    on_error=None,  # (item, exc) called for every item that finally fails or is dropped
    clock=None,  # inject FakeClock in tests
)
```

### Submitting

```python
result = await batcher.submit(item)  # wait for this item's result
fut = batcher.submit_nowait(item)  # returns an asyncio.Future, never blocks
results = await batcher.submit_many(items)  # ordered results, respects backpressure
await batcher.flush()  # flush everything submitted so far, wait for it
```

* Cancelling a waiting `submit()` does **not** withdraw an item that was already accepted. Once accepted, it will be flushed. (If you cancel while still *waiting for capacity*, nothing was accepted and nothing is sent.)
* `submit_nowait` raises `BackpressureError` at capacity (with `on_full="drop"` it returns an already-failed future instead). Fire-and-forget callers should pass `on_error` so failures aren't invisible.

### Backpressure

`max_pending` counts every unresolved item, including ones in flight and ones waiting to be retried. At the limit:

* `"block"`: `submit` waits for room. Waiters are served strictly FIFO.
* `"raise"`: `BackpressureError`.
* `"drop"`: the item is discarded and `ItemDroppedError` is raised from `submit` (and passed to `on_error`).

### Retries

```python
from microbatcher import Backoff, RetryPolicy

Batcher(
    flush,
    retry=RetryPolicy(
        max_attempts=4,  # first try + 3 retries
        backoff=Backoff(initial=0.1, factor=2, maximum=10, jitter=0.5),
        retry_on=lambda exc: not isinstance(exc, ValueError),
    ),
)
```

Only the failed items are re-sent. They go to the front of the queue and flush as soon as their backoff has elapsed (coalescing with whatever else is waiting). Because they keep their slot in `max_pending`, retries can never bypass backpressure.

### Per-key ordering

```python
Batcher(flush, key=lambda e: e.account_id, max_concurrent_flushes=4)
```

Items with the same key are never in two in-flight batches at once, and across batches they flush in submission order, including while a failed item waits for its retry. Different keys flush in parallel.

> Caveat: if one batch contains `[a1, a2]` and only `a1` fails, `a2` has already succeeded when `a1` is retried. Ordering is guaranteed *between* flush calls, not for partial-failure retries *within* one batch. If that matters, make the flush all-or-nothing per key.

### Shutdown

```python
await batcher.close(timeout=10)  # or: async with Batcher(...) as b:
```

`close()` stops accepting items, wakes callers blocked on capacity with `BatcherClosedError`, flushes everything immediately (no waiting for `max_wait`), and waits for it all to settle, including retries. If the deadline passes, in-flight flushes are cancelled, unfinished items fail with `DrainTimeoutError`, and `DrainTimeoutError` is raised from `close()`.

### Observability

```python
batcher.stats  # Stats(pending, queued, in_flight, submitted, succeeded, failed, dropped, retried, batches)
```

The logger name is `microbatcher`.

## Threads / sync code

```python
from microbatcher import SyncBatcher

with SyncBatcher(write_rows, max_size=200, max_wait=0.02) as batcher:
    with ThreadPoolExecutor(32) as pool:
        ids = list(pool.map(batcher.submit, rows))  # blocks per call, batches across threads
```

`SyncBatcher` runs the same async core on a private event loop in a daemon thread. It accepts the same options. Also: `submit_future()` (returns a `concurrent.futures.Future`), `submit_many()`, `flush()`, `close()`, `stats`.

## Testing your code (no sleeping)

```python
from microbatcher import Batcher
from microbatcher.testing import FakeClock


async def test_flushes_after_max_wait():
    clock = FakeClock()
    seen = []

    async def flush(items):
        seen.append(items)

    b = Batcher(flush, max_wait=0.05, clock=clock)
    b.submit_nowait("x")
    await clock.advance(0.049)
    assert seen == []
    await clock.advance(0.002)
    assert seen == [["x"]]
```

`max_wait`, retry backoff, `flush_timeout` and the drain deadline all run on the injected clock.

## Performance

Overhead is a few microseconds per item: on a small sandbox VM, a no-op `flush` with `max_size=500` sustained roughly 180k items/s through `submit_many`. Your real flush call will dominate; benchmark on your own hardware.

## Guarantees and non-guarantees

* **Delivery is at-least-once once retries are enabled.** If a flush partially succeeded server-side and then raised, a retry re-sends the whole batch. Make the sink idempotent (idempotency keys, upserts).
* A plain-function `flush` running in a thread cannot be interrupted; on `flush_timeout` or a drain timeout it is abandoned, not killed.
* In-memory only. If the process dies, queued items are lost. (Pair with an outbox if you need durability.)
* One event loop: a `Batcher` must be used from the loop it first ran on. Use `SyncBatcher` for threads.

## How it works (for contributors)

There is no worker task. All logic lives in one synchronous method, `Batcher._pump()`, called whenever something changes: an item arrives, a flush ends, a timer fires, `flush()`/`close()` is called. It starts every batch whose trigger is met (size, bytes, age of the oldest eligible item, or forced) while concurrency allows, then arms a single timer for the oldest eligible item. Flush calls run as tasks; on completion they settle items, schedule retries, release keys and call `_pump()` again.

## Roadmap

* v0.2: priority lanes, per-key batching (group by key instead of ordering by key), OpenTelemetry hooks
* v0.3: optional durable spool (SQLite) for crash-safe queued items

## License

MIT
