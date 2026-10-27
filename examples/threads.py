"""Plain threaded code: many callers, one batched write."""

from concurrent.futures import ThreadPoolExecutor
import threading
import time

from microbatcher import SyncBatcher


def write_rows(rows: list[int]) -> list[int]:
    print(f"writing {len(rows)} rows on {threading.current_thread().name}")
    time.sleep(1)
    return [r * 10 for r in rows]


with SyncBatcher(
    write_rows,
    max_size=100,
    max_wait=5,
    max_concurrent_flushes=10, # threading flushing
) as batcher:
    # threading submission
    with ThreadPoolExecutor(20) as pool:
        print(
            sum(
                sum(results)
                for results in pool.map(
                    batcher.submit_many,
                    [[1] * 50 for _ in range(100)],
                )
            )
        )
