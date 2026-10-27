"""Plain threaded code: many callers, one batched write."""

from concurrent.futures import ThreadPoolExecutor

from microbatcher import SyncBatcher


def write_rows(rows: list[int]) -> list[int]:
    print(f"writing {len(rows)} rows")
    return [r * 10 for r in rows]


with SyncBatcher(write_rows, max_size=25, max_wait=0.02) as batcher:
    with ThreadPoolExecutor(32) as pool:
        print(sum(pool.map(batcher.submit, range(100))))
