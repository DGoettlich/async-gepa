import asyncio

import pytest
from test_parallel_valset_eval import ImprovingAdapter

from async_gepa import optimize
from async_gepa.core.state import GEPAState
from async_gepa.strategies.proposal_sampling import PxNSampling
from async_gepa.strategies.proposal_selection import AllImprovements, BestImprovement, TopKImprovements
from async_gepa.strategies.scheduling import ContinuousRollouts


class AsyncAdapter(ImprovingAdapter):
    def __init__(self):
        super().__init__()
        self.metric_calls = 0

    async def evaluate(self, batch, candidate, capture_traces=False):
        await asyncio.sleep(0)
        self.metric_calls += len(batch)
        return super().evaluate(batch, candidate, capture_traces)


class IndependentSelector:
    independent = True

    def select(self, proposals, state, acceptance_criterion):
        assert len(proposals) == 1
        return AllImprovements().select(proposals, state, acceptance_criterion)


class GroupSelector:
    def select(self, proposals, state, acceptance_criterion):
        assert len(proposals) == 4
        return AllImprovements().select(proposals, state, acceptance_criterion)


@pytest.mark.parametrize("merging", [False, True])
@pytest.mark.parametrize("selection", [AllImprovements(), BestImprovement(), TopKImprovements(2), IndependentSelector(), GroupSelector()])
def test_continuous_loop_budget_history_and_large_groups(monkeypatch, tmp_path, selection, merging):
    adapter = AsyncAdapter()
    kwargs = {
        "seed_candidate": {"system_prompt": "s"},
        "trainset": [{"split": "train"} for _ in range(4)],
        "valset": [{"split": "val"} for _ in range(4)],
        "adapter": adapter,
        "max_metric_calls": 45,
        "reflection_lm": None,
        "sampling_strategy": PxNSampling(2, 2),
        "selection_strategy": selection,
        "run_dir": str(tmp_path),
        "cache_evaluation": True,
        "use_merge": merging,
        "scheduler": ContinuousRollouts(max_in_flight_proposals=2, max_in_flight_rollouts=3),
    }
    result = optimize(**kwargs)
    assert len(result.candidates) > 1
    assert result.total_metric_calls == adapter.metric_calls
    state = GEPAState.load(str(tmp_path))
    assert state.is_consistent()
    assert len(set(state.iteration_ids_by_candidate_idx)) == len(state.program_candidates)
    before = adapter.metric_calls
    resumed = optimize(**kwargs)
    assert adapter.metric_calls == before
    assert resumed.candidates == result.candidates


@pytest.mark.parametrize("merging", [False, True])
def test_one_active_proposal_preserves_sync_sampling_and_results(tmp_path, merging):
    class RecordingAdapter(ImprovingAdapter):
        def __init__(self):
            super().__init__()
            self.requests = []

        def evaluate(self, batch, candidate, capture_traces=False):
            self.requests.extend((row["id"], candidate["system_prompt"], capture_traces) for row in batch)
            return super().evaluate(batch, candidate, capture_traces)

    class AsyncRecordingAdapter(RecordingAdapter):
        async def evaluate(self, batch, candidate, capture_traces=False):
            await asyncio.sleep(0)
            return super().evaluate(batch, candidate, capture_traces)

    sync, asynchronous = RecordingAdapter(), AsyncRecordingAdapter()
    kwargs = {
        "seed_candidate": {"system_prompt": "s"},
        "trainset": [{"id": i, "split": "train"} for i in range(15)],
        "valset": [{"id": 100 + i, "split": "val"} for i in range(4)],
        "max_metric_calls": 65,
        "reflection_lm": None,
        "use_merge": merging,
    }
    expected = optimize(**kwargs, adapter=sync, run_dir=str(tmp_path / "sync"))
    actual = optimize(
        **kwargs,
        adapter=asynchronous,
        run_dir=str(tmp_path / "async"),
        scheduler=ContinuousRollouts(max_in_flight_proposals=1),
    )
    assert asynchronous.requests == sync.requests
    assert actual.candidates == expected.candidates
    assert actual.val_aggregate_scores == expected.val_aggregate_scores
    assert actual.total_metric_calls == expected.total_metric_calls


def test_empty_sampling_iteration_does_not_stop_optimization(tmp_path):
    from async_gepa.strategies.proposal_sampling import SingleMutationSampling

    class Sampling:
        def __init__(self):
            self.first = True
            self.delegate = SingleMutationSampling()

        def sample_tasks(self, *args):
            if self.first:
                self.first = False
                return []
            return self.delegate.sample_tasks(*args)

    result = optimize(
        seed_candidate={"system_prompt": "s"},
        trainset=[{"split": "train"} for _ in range(3)],
        valset=[{"split": "val"}],
        adapter=AsyncAdapter(), sampling_strategy=Sampling(),
        max_metric_calls=8, reflection_lm=None, run_dir=str(tmp_path),
        scheduler=ContinuousRollouts(max_in_flight_proposals=1),
    )
    assert len(result.candidates) == 2
    assert result.total_metric_calls == 8
