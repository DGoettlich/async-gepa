import asyncio
from types import SimpleNamespace

import pytest
from test_async_evaluation import engine_for

from async_gepa.core.adapter import EvaluationBatch
from async_gepa.core.data_loader import ListDataLoader
from async_gepa.core.engine import GEPAEngine
from async_gepa.core.state import EvaluationCache


@pytest.mark.parametrize("traces", [False, True])
@pytest.mark.parametrize("cached", [False, True])
def test_async_validation_matches_sync_policy_cache_and_outputs(traces, cached):
    calls = []

    def evaluate(batch, candidate, capture_traces=False):
        calls.extend(batch)
        values = [value + candidate["offset"] for value in batch]
        return EvaluationBatch(values, values, values if capture_traces else None, [{"score": v} for v in values])

    async def aevaluate(*args, **kwargs):
        return evaluate(*args, **kwargs)

    def setup(adapter):
        engine = engine_for(adapter)
        engine.valset = ListDataLoader([10, 20, 30])
        engine.val_evaluation_policy = SimpleNamespace(get_eval_batch=lambda loader, state: [2, 0])
        engine.write_agent_state = traces
        engine.valset_cache_split = "val"
        state = SimpleNamespace(evaluation_cache=EvaluationCache() if cached else None)
        if cached:
            state.evaluation_cache.put_batch({"offset": 1}, [0], [11], [11], [{"score": 11}], split="val")
        return engine, state

    sync_engine, sync_state = setup(SimpleNamespace(evaluate=evaluate))
    expected = GEPAEngine._evaluate_programs_on_valset(sync_engine, [{"offset": 1}], sync_state)
    expected_calls = list(calls)
    calls.clear()

    async def run():
        engine, state = setup(SimpleNamespace(evaluate=aevaluate))
        try:
            actual = await engine._evaluate_programs_on_valset([{"offset": 1}], state)
            assert actual == expected
            assert calls == expected_calls
            if cached and not traces:
                calls.clear()
                repeated = await engine._evaluate_programs_on_valset([{"offset": 1}], state)
                assert repeated[0][1] == 0
                assert calls == []
        finally:
            await engine.rollouts.close()

    asyncio.run(run())
