"""microbatcher: tiny micro-batching for asyncio and threads."""

from ._clock import AsyncioClock, Clock
from .batcher import Batcher, OnFull, default_sizer
from .errors import (
    BackpressureError,
    BatcherClosedError,
    DrainTimeoutError,
    FlushResultError,
    FlushTimeoutError,
    ItemDroppedError,
    MicroBatcherError,
)
from .retry import Backoff, RetryPolicy
from .stats import Stats
from .sync import SyncBatcher

__version__ = "0.1.0"

__all__ = [
    "AsyncioClock",
    "Backoff",
    "BackpressureError",
    "Batcher",
    "BatcherClosedError",
    "Clock",
    "DrainTimeoutError",
    "FlushResultError",
    "FlushTimeoutError",
    "ItemDroppedError",
    "MicroBatcherError",
    "OnFull",
    "RetryPolicy",
    "Stats",
    "SyncBatcher",
    "__version__",
    "default_sizer",
]
