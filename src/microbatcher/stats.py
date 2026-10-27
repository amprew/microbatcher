"""Point-in-time counters for observability."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Stats:
    """Snapshot returned by ``Batcher.stats``.

    Gauges (``pending``, ``queued``, ``in_flight``) describe the present; the rest are
    monotonically increasing counters since the batcher was created.
    """

    pending: int  # accepted but not yet resolved (queued + in flight + waiting to retry)
    queued: int  # waiting in the queue for a batch
    in_flight: int  # batches currently being flushed
    submitted: int
    succeeded: int
    failed: int
    dropped: int
    retried: int  # item-retries scheduled (one item retried twice counts twice)
    batches: int  # flush calls started (retries included)
