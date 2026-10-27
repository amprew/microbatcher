"""Plain threaded code: many callers, one batched write."""

from concurrent.futures import ThreadPoolExecutor

import asyncio

from microbatcher import SyncBatcher


async def write_rows(rows: list[int]) -> list[int]:
    print(f"writing {len(rows)} rows")
    # await asyncio.sleep(0.9)
    return [r * 10 for r in rows]


with SyncBatcher(write_rows, max_size=100, max_wait=5) as batcher:
    with ThreadPoolExecutor(100) as pool:
        print(
            sum(
                sum(results)
                for results in pool.map(
                    batcher.submit_many,
                    [[1] * 100 for _ in range(300)],
                )
            )
        )
