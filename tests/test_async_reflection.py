import asyncio
import threading
from types import SimpleNamespace

from async_gepa.proposer.reflective_mutation.reflection_lm import ReflectionProposal
from async_gepa.proposer.reflective_mutation.reflective_mutation import AsyncReflectiveMutationProposer

JOB = ({"prompt": "old"}, {"prompt": [{"feedback": "improve"}]}, ["prompt"])


def proposer_for(reflector):
    proposer = AsyncReflectiveMutationProposer.__new__(AsyncReflectiveMutationProposer)
    proposer.adapter = SimpleNamespace(propose_new_texts=None)
    proposer.custom_candidate_proposer = None
    proposer._reflection_lm = reflector
    proposer.logger = SimpleNamespace(log=lambda _: None)
    return proposer


def test_concurrent_reflector_calls_overlap():
    barrier = threading.Barrier(2, timeout=2)

    class Reflector:
        independent = True

        def reflect(self, *args):
            barrier.wait()
            return ReflectionProposal({"prompt": "new"}), self

    async def run():
        proposer = proposer_for(Reflector())
        results = await asyncio.gather(*(proposer._propose_texts_batch_safe([JOB]) for _ in range(2)))
        assert [result[0][0] for result in results] == [{"prompt": "new"}] * 2

    asyncio.run(run())


def test_stateful_reflector_successors_are_chained():
    class Reflector:
        def __init__(self, count=0):
            self.count = count

        def reflect(self, *args):
            successor = Reflector(self.count + 1)
            return ReflectionProposal({"prompt": str(successor.count)}), successor

    async def run():
        proposer = proposer_for(Reflector())
        results = await asyncio.gather(*(proposer._propose_texts_batch_safe([JOB]) for _ in range(3)))
        assert [result[0][0]["prompt"] for result in results] == ["1", "2", "3"]
        assert proposer._reflection_lm.count == 3

    asyncio.run(run())


def test_custom_batch_method_is_preserved():
    class Reflector:
        def __init__(self):
            self.batches = []

        def reflect(self, *args):
            raise AssertionError("must use the supplied batch method")

        def reflect_many(self, jobs):
            self.batches.append(jobs)
            return [(ReflectionProposal({"prompt": str(i)}), self) for i, _ in enumerate(jobs)]

    async def run():
        reflector = Reflector()
        results = await proposer_for(reflector)._propose_texts_batch_safe([JOB, JOB])
        assert len(reflector.batches) == 1
        assert [result[0]["prompt"] for result in results] == ["0", "1"]

    asyncio.run(run())
