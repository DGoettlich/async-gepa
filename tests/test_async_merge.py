import asyncio
import random

from async_gepa.core.data_loader import ListDataLoader
from async_gepa.core.state import GEPAState, ValsetEvaluation
from async_gepa.logging.logger import StdOutLogger
from async_gepa.proposer.merge import AsyncMergeProposer, MergeProposer


def setup(proposer_type, evaluator):
    candidates = [{"a": "old", "b": "old"}, {"a": "new", "b": "old"}, {"a": "old", "b": "new"}]

    def evaluation(scores):
        return ValsetEvaluation(outputs_by_val_id={0: "x", 1: "y"}, scores_by_val_id=dict(enumerate(scores)))

    state = GEPAState(candidates[0], evaluation([0, 0]))
    for i, scores in enumerate(([2, 1], [1, 2]), 1):
        state.update_state_with_new_program([0], candidates[i], evaluation(scores), None, 2, iteration_id=f"child-{i}")
    state.total_num_evals = 0
    state.full_program_trace.append({"i": 0, "iteration_id": "merge"})
    proposer = proposer_type(
        logger=StdOutLogger(),
        valset=ListDataLoader([0, 1]),
        evaluator=evaluator,
        use_merge=True,
        max_merge_invocations=1,
        val_overlap_floor=2,
        rng=random.Random(0),
    )
    proposer.last_iter_found_new_program = True
    proposer.schedule_if_needed()
    return proposer, state


def test_merge_matches_sync_and_retains_improvement_arriving_during_evaluation():
    def evaluate(batch, candidate):
        return batch, [3] * len(batch), None

    sync, sync_state = setup(MergeProposer, evaluate)
    expected = sync.propose(sync_state)

    async def run():
        async def aevaluate(batch, candidate):
            assert not proposer.last_iter_found_new_program
            # another proposal completes while the merge request is outstanding.
            proposer.last_iter_found_new_program = True
            await asyncio.sleep(0)
            return evaluate(batch, candidate)

        proposer, state = setup(AsyncMergeProposer, aevaluate)
        actual = await proposer.propose(state)
        assert actual == expected
        assert state.total_num_evals == sync_state.total_num_evals
        assert proposer.last_iter_found_new_program

    asyncio.run(run())
