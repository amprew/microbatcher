"""Retry an entire batch when the bulk operation raises a transient error."""

import asyncio

from microbatcher import Backoff, Batcher, RetryPolicy

attempts = 0


async def write_rows(rows: list[int]) -> list[int]:
    global attempts
    attempts += 1
    print(f"attempt {attempts}: writing {len(rows)} rows")
    if attempts < 3:
        raise ConnectionError("temporary database outage")
    return [row * 10 for row in rows]


async def main() -> None:
    retry = RetryPolicy(
        max_attempts=3,
        backoff=Backoff(initial=0.01, factor=2, jitter=0),
    )
    async with Batcher(write_rows, max_size=3, max_wait=0.01, retry=retry) as batcher:
        results = await batcher.submit_many([1, 2, 3])
    print("results:", results)


asyncio.run(main())
