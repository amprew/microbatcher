"""One round trip per batch, one result per caller."""

import asyncio

from microbatcher import Batcher


async def bulk_insert(rows: list[dict]) -> list[int]:
    print(f"INSERT {len(rows)} rows in one round trip")
    await asyncio.sleep(0.01)  # pretend network
    return [1000 + r["n"] for r in rows]  # one id per row, in order


async def main() -> None:
    async with Batcher(bulk_insert, max_size=200, max_wait=5) as batcher:
        ids = await asyncio.gather(*(batcher.submit({"n": i}) for i in range(120)))
    print(ids[:5], "...", len(ids), "ids;", batcher.stats)


asyncio.run(main())
