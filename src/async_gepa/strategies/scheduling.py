"""Execution scheduling, independent of proposal sampling and selection."""

import asyncio
from dataclasses import dataclass

from async_gepa.core.adapter import AsyncGEPAAdapter


@dataclass(frozen=True)
class ContinuousRollouts:
    """Continuously fill evaluation slots across active proposal groups."""

    max_in_flight_proposals: int = 8
    max_in_flight_rollouts: int = 64
    max_proposal_age: int | None = 3

    def __post_init__(self):
        if self.max_in_flight_proposals < 1 or self.max_in_flight_rollouts < 1:
            raise ValueError("Concurrency limits must be positive")
        if self.max_proposal_age is not None and self.max_proposal_age < 0:
            raise ValueError("max_proposal_age must be nonnegative or None")


class RolloutQueue:
    """Run individual adapter evaluations through one bounded FIFO queue."""

    def __init__(self, adapter: AsyncGEPAAdapter, concurrency: int):
        self.adapter = adapter
        self.concurrency = concurrency
        self.pending = asyncio.Queue()
        self.workers: list[asyncio.Task[None]] = []

    def submit(self, example, candidate, *, capture_traces=False):
        # start inside the running event loop, on first submission.
        if not self.workers:
            self.workers = [asyncio.create_task(self.submit_task()) for _ in range(self.concurrency)]
        future = asyncio.get_running_loop().create_future()
        self.pending.put_nowait((example, candidate, capture_traces, future))
        return future

    async def submit_task(self):
        """Evaluate queued examples one at a time, alongside the other workers."""
        while True:
            example, candidate, capture_traces, future = await self.pending.get()
            try:
                if future.cancelled():
                    continue
                result = await self.adapter.evaluate([example], candidate, capture_traces=capture_traces)
                if not future.done():
                    future.set_result(result)
            except asyncio.CancelledError:
                future.cancel()
                raise
            except Exception as error:
                if not future.done():
                    future.set_exception(error)
            finally:
                self.pending.task_done()

    async def close(self):
        for worker in self.workers:
            worker.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)
        self.workers.clear()
        while not self.pending.empty():
            *_, future = self.pending.get_nowait()
            future.cancel()
            self.pending.task_done()
