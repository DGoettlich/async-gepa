import asyncio
from types import SimpleNamespace

import pytest

from async_gepa.core.adapter import EvaluationBatch
from async_gepa.core.engine import AsyncGEPAEngine
from async_gepa.strategies.scheduling import ContinuousRollouts, RolloutQueue


def engine_for(adapter, limit=2):
    engine = AsyncGEPAEngine.__new__(AsyncGEPAEngine)
    engine.adapter = adapter
    engine.scheduler = ContinuousRollouts(max_in_flight_rollouts=limit)
    engine.rollouts = RolloutQueue(adapter, concurrency=limit)
    return engine


def test_slots_refill_before_slow_sibling_finishes():
    async def run():
        slow = asyncio.Event()
        third_started = asyncio.Event()
        started = []
        active = 0
        peak = 0

        async def evaluate(batch, candidate, capture_traces=False):
            nonlocal active, peak
            item = batch[0]
            active += 1
            peak = max(peak, active)
            started.append(item)
            if item == 0:
                await slow.wait()
            if item == 2:
                third_started.set()
            await asyncio.sleep(0)
            active -= 1
            return EvaluationBatch([item], [float(item)], [item], [{"value": item}], num_metric_calls=2)

        engine = engine_for(SimpleNamespace(evaluate=evaluate))
        task = asyncio.create_task(engine.invoke_batch_evaluate([({}, [0, 1, 2])], capture_traces=True))
        try:
            await asyncio.wait_for(third_started.wait(), 1)
            assert not task.done()
            assert started == [0, 1, 2]
            assert peak == 2
            slow.set()
            (result,) = await task
            assert result.outputs == [0, 1, 2]
            assert result.scores == [0, 1, 2]
            assert result.trajectories == [0, 1, 2]
            assert result.objective_scores == [{"value": i} for i in range(3)]
            assert result.num_metric_calls == 6
        finally:
            await engine.rollouts.close()

    asyncio.run(run())


def test_failure_does_not_consume_worker_slot():
    async def run():
        async def evaluate(batch, candidate, capture_traces=False):
            if batch == [0]:
                raise ValueError("bad example")
            return EvaluationBatch(batch, [1.0])

        engine = engine_for(SimpleNamespace(evaluate=evaluate), limit=1)
        try:
            with pytest.raises(ValueError, match="bad example"):
                await engine.invoke_batch_evaluate([({}, [0])])
            (result,) = await asyncio.wait_for(engine.invoke_batch_evaluate([({}, [1, 2])]), 1)
            assert result.outputs == [1, 2]
        finally:
            await engine.rollouts.close()

    asyncio.run(run())


def test_concurrent_callers_keep_batches_candidates_and_order_separate():
    async def run():
        release_first = asyncio.Event()
        active = 0
        peak = 0

        async def evaluate(batch, candidate, capture_traces=False):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            value = (candidate["name"], batch[0])
            try:
                if value == ("A", 0):
                    await release_first.wait()
                await asyncio.sleep(0)
                return EvaluationBatch(
                    outputs=[value],
                    scores=[float(batch[0])],
                    trajectories=[value] if capture_traces else None,
                )
            finally:
                active -= 1

        engine = engine_for(SimpleNamespace(evaluate=evaluate), limit=2)
        first = asyncio.create_task(
            engine.invoke_batch_evaluate(
                [({"name": "A"}, [0, 1]), ({"name": "B"}, [0, 1])],
                capture_traces=True,
            )
        )
        second = asyncio.create_task(engine.invoke_batch_evaluate([({"name": "C"}, [0])]))
        try:
            (c,) = await asyncio.wait_for(second, 1)
            assert not first.done()
            assert c.outputs == [("C", 0)]
            assert c.trajectories is None
            assert peak == 2
            release_first.set()
            a, b = await first
            assert a.outputs == a.trajectories == [("A", 0), ("A", 1)]
            assert b.outputs == b.trajectories == [("B", 0), ("B", 1)]
            assert a.scores == b.scores == [0.0, 1.0]
            # returning earlier batches does not close the shared queue.
            (later,) = await engine.invoke_batch_evaluate([({"name": "D"}, [2])])
            assert later.outputs == [("D", 2)]
        finally:
            await engine.rollouts.close()
            await asyncio.gather(first, second, return_exceptions=True)

    asyncio.run(run())


def test_close_cancels_active_and_queued_requests():
    async def run():
        started = asyncio.Event()
        stopped = asyncio.Event()

        async def evaluate(batch, candidate, capture_traces=False):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

        queue = RolloutQueue(SimpleNamespace(evaluate=evaluate), concurrency=1)
        active = queue.submit(0, {})
        waiting = queue.submit(1, {})
        try:
            await asyncio.wait_for(started.wait(), 1)
        finally:
            await queue.close()
        assert stopped.is_set()
        assert active.cancelled()
        assert waiting.cancelled()
        assert not queue.workers
        assert queue.pending.empty()

    asyncio.run(run())
