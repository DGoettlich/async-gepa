import asyncio
from types import SimpleNamespace

import pytest

from async_gepa.core.engine import AsyncGEPAEngine
from async_gepa.proposer.base import CandidateProposal
from async_gepa.strategies.acceptance import StrictImprovementAcceptance
from async_gepa.strategies.proposal_selection import AllImprovements, BestImprovement, TopKImprovements
from async_gepa.strategies.scheduling import ContinuousRollouts


@pytest.mark.parametrize("strategy", [AllImprovements(), BestImprovement(), TopKImprovements(2)])
def test_selection_uses_existing_strategy_verdicts(strategy):
    proposals = [
        CandidateProposal(
            candidate={"prompt": str(score)},
            parent_program_ids=[0],
            subsample_scores_before=[1],
            subsample_scores_after=[score],
        )
        for score in [2, 0, 4, 3]
    ]
    state = SimpleNamespace(i=9, full_program_trace=[])
    trace = {"i": 0, "iteration_id": "proposal"}
    expected = strategy.select(proposals, state, StrictImprovementAcceptance())
    accepted = []
    engine = AsyncGEPAEngine.__new__(AsyncGEPAEngine)
    engine.scheduler = ContinuousRollouts(max_proposal_age=None)
    engine.selection_strategy = strategy
    engine.acceptance_criterion = StrictImprovementAcceptance()
    engine.write_agent_state = False
    engine.callbacks = []
    engine.merge_proposer = None
    engine.logger = SimpleNamespace(log=lambda _: None)
    engine._log_proposal_lm_calls = lambda *a, **kw: None

    async def evaluate(programs, state):
        return [(None, 1) for _ in programs]

    def add(**kwargs):
        assert kwargs["proposal_trace"] is trace
        accepted.append(kwargs["new_program"])
        return len(accepted), 0

    engine._evaluate_programs_on_valset = evaluate
    engine._add_evaluated_program = add
    asyncio.run(engine._run_reflective_batch(proposals, state, proposal_trace=trace))
    assert accepted == [p.candidate for p in expected]
