"""A bulk API that accepts some items and rejects others (SQS/Kinesis style)."""

import asyncio

from microbatcher import Backoff, Batcher, RetryPolicy

calls = 0


async def send_batch(msgs: list[str]) -> list[str | Exception]:
    global calls
    calls += 1
    out: list[str | Exception] = []
    for m in msgs:
        if m == "flaky" and calls < 3:
            out.append(ConnectionError("throttled"))  # retried
        elif m == "poison":
            out.append(ValueError("rejected"))  # permanent: retry_on says no
        else:
            out.append(f"id-{m}")
    return out


async def main() -> None:
    retry = RetryPolicy(
        max_attempts=3,
        backoff=Backoff(initial=0.01, jitter=0),
        retry_on=lambda e: not isinstance(e, ValueError),
    )
    async with Batcher(send_batch, max_size=10, max_wait=0.01, retry=retry) as b:
        results = await b.submit_many(["a", "flaky", "poison", "b"], return_exceptions=True)
    print(results)  # ['id-a', 'id-flaky', ValueError('rejected'), 'id-b']


asyncio.run(main())
