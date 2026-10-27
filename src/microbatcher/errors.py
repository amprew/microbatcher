"""Exception hierarchy; everything raised by the library derives from ``MicroBatcherError``."""

from __future__ import annotations


class MicroBatcherError(Exception):
    """Base class for all microbatcher errors."""


class BatcherClosedError(MicroBatcherError):
    """The batcher is closing or closed and no longer accepts items (or a flush was cancelled)."""


class BackpressureError(MicroBatcherError):
    """``max_pending`` items are already in the batcher and the overflow policy refuses more."""


class ItemDroppedError(MicroBatcherError):
    """The item was discarded because the batcher was full and ``on_full="drop"`` is set."""


class FlushResultError(MicroBatcherError):
    """The flush callable returned something other than ``None`` or one outcome per item."""


class FlushTimeoutError(MicroBatcherError, TimeoutError):
    """A single flush call exceeded ``flush_timeout``."""


class DrainTimeoutError(MicroBatcherError):
    """``close()`` hit its deadline; unfinished items were abandoned and failed with this error."""
