"""Submit a collection of items and receive their results in submission order."""

import asyncio

from microbatcher import Batcher


async def fetch_users(user_ids: list[int]) -> list[str]:
    print(f"fetching users {user_ids}")
    return [f"user-{user_id}" for user_id in user_ids]


async def main() -> None:
    user_ids = [42, 17, 8, 99, 23, 5, 61, 14]

    async with Batcher(fetch_users, max_size=3, max_wait=0.1) as batcher:
        users = await batcher.submit_many(user_ids)

    print("results:", users)
    print(f"{len(users)} results for {len(user_ids)} submitted IDs")


asyncio.run(main())
