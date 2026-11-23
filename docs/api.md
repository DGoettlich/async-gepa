# AsyncGEPA API reference

This reference documents the asynchronous additions and their differences from GEPA. Existing synchronous classes remain available under the `async_gepa` package name. For configuration examples, see the [practical guide](usage.md).

- [ContinuousRollouts](#continuousrollouts)
- [AsyncGEPAAdapter](#asyncgepaadapter)
- [AsyncGEPAEngine](#asyncgepaengine)
- [AsyncReflectiveMutationProposer](#asyncreflectivemutationproposer)
- [AsyncMergeProposer](#asyncmergeproposer)
- [RolloutQueue](#rolloutqueue)
- [independent](#independent)

## ContinuousRollouts

*New scheduling configuration; no synchronous GEPA counterpart.*

Enables continuous execution and controls how much proposal and evaluation work can overlap. Pass an instance as `optimize(scheduler=...)`.

```python
ContinuousRollouts(
    max_in_flight_proposals: int = 8,
    max_in_flight_rollouts: int = 64,
    max_proposal_age: int | None = 3,
)
```

### Parameters

| Parameter | Default | Description |
|---|---:|---|
| `max_in_flight_proposals` | `8` | Limits active proposal execution. |
| `max_in_flight_rollouts` | `64` | Limits simultaneous individual adapter evaluations across proposals. |
| `max_proposal_age` | `3` | Limits proposal staleness. `None` disables the check. |

Concurrency limits must be positive. Proposal age must be nonnegative or `None`.

A proposal becomes stale when more than `max_proposal_age` candidates have entered the pool since its admission **and none of its parents remains on the frontier**.

**Concurrency:** sampling determines which proposals are created; these settings control their execution. Evaluation slots are shared across proposals and refilled from ready queued work.

## AsyncGEPAAdapter

*Async counterpart of [GEPAAdapter](https://gepa.dev/api/core/GEPAAdapter/).*

Defines how AsyncGEPA executes and scores candidates, captures execution traces, and prepares feedback for reflection.

### Changes from GEPA

| Member | Change |
|---|---|
| `evaluate()` | Asynchronous; called concurrently with singleton batches. |
| `make_reflective_dataset()` | Unchanged synchronous interface. |
| `propose_new_texts` | Unchanged optional customization interface. |

### evaluate

```python
async def evaluate(
    self,
    batch: list[DataInst],
    candidate: dict[str, str],
    capture_traces: bool = False,
) -> EvaluationBatch[Trajectory, RolloutOutput]
```

Executes the candidate on the supplied examples and returns their outputs, scores and optional trajectories.

| Parameter | Description |
|---|---|
| `batch` | Examples to evaluate. Continuous scheduling supplies one example per call. |
| `candidate` | Mapping from component names to their current text. |
| `capture_traces` | Whether to include trajectories for reflection. |

**Returns:** GEPA's existing `EvaluationBatch`, with results corresponding to the supplied examples.

**Concurrency:** multiple calls may execute on the same adapter instance. Evaluations must work independently, and per-call temporary state must remain separate. Model calls should be awaitable so they do not block other evaluations.

### make_reflective_dataset

Remains synchronous and uses the existing GEPA contract. No async implementation is required.

See the [upstream reference](https://gepa.dev/api/core/GEPAAdapter/) for the unchanged feedback and optional proposal interfaces.

## AsyncGEPAEngine

*Extends GEPA's `GEPAEngine`.*

Runs overlapping proposal work using one shared evaluation queue. `optimize()` constructs this engine automatically when given `ContinuousRollouts`.

### Changes from GEPA

| Method | Change |
|---|---|
| `__init__()` | Adds the keyword-only `scheduler` argument and creates the shared queue. |
| `run()` | Asynchronous; admits and advances overlapping proposal work. |
| `_evaluate_programs_on_valset()` | Asynchronous; retains validation-policy and cache handling while awaiting queued evaluations. |
| `_run_reflective_batch()` | Asynchronous; processes selected proposals, applies age checks and preserves group-scoped deduplication. |
| `_run_full_eval_and_add()` | Asynchronous; awaits validation before updating candidate state. |
| `_add_evaluated_program()` | Accepts an explicit `proposal_trace` so results update their originating history entry. |
| `invoke_batch_evaluate()` | New engine method replacing standalone batch dispatch on the async path. |
| `_run_merge_attempt()` | New async method containing merge handling performed inside GEPA's synchronous loop. |

### Construction

```python
AsyncGEPAEngine(*args, scheduler, **kwargs)
```

`scheduler` is a `ContinuousRollouts` instance. Other constructor arguments follow `GEPAEngine`. Normal callers should use `optimize()` rather than construct the engine directly.

### run

```python
async def run(self) -> GEPAState[RolloutOutput, DataId]
```

Runs optimization and returns the final state. It manages proposal admission, selection, validation, merging and saving. The public `optimize()` wrapper converts the state into the existing `GEPAResult`.

**Concurrency:** model work can overlap, while completed candidate-state updates execute sequentially. When a stop condition is reached, new admission stops and already-admitted work finishes. This can exceed the requested metric-call budget.

### invoke_batch_evaluate

```python
async def invoke_batch_evaluate(self, items, *, capture_traces=False)
```

| Parameter | Description |
|---|---|
| `items` | A sequence of `(candidate, examples)` pairs. |
| `capture_traces` | Whether the adapter should return trajectories. |

**Returns:** a list of `EvaluationBatch` objects in the same order as `items`, with examples ordered as supplied.

Each example enters `RolloutQueue` separately. This method awaits its submitted results without preventing other callers from submitting work. It does not dispatch through the adapter's optional synchronous `batch_evaluate()`.

### Selection and validation

`run()` decides whether selection can proceed individually: the built-in `AllImprovements`, or a custom selector with `independent = True`, can process completed proposals without waiting for their siblings. Other strategies receive the completed group.

`_run_reflective_batch()` applies selection, group-scoped deduplication and proposal-age checks, then awaits the configured validation policy. `_evaluate_programs_on_valset()` retains evaluation-cache handling; only missing evaluations require model work.

With individual selection, the first selected duplicate to finish is retained. Even though `a == b` and `a` started before `b`, `b` could finish before `a`. AsyncGEPA would then keep `b`, while synchronous GEPA would keep `a`. Since both prompts are identical, this is unlikely under a seeded engine and calls.

### History and saving

`proposal_trace` is the history dictionary associated with the work being processed. Explicitly passing it prevents a late completion from being attributed to a more recently started iteration.

The engine uses GEPA's existing state-saving mechanism after applying completed-state updates. Outstanding requests are not serialized or restored on resume. Iteration identifiers indicate where work originated, not its completion order.

## AsyncReflectiveMutationProposer

*Extends [ReflectiveMutationProposer](https://gepa.dev/api/proposers/ReflectiveMutationProposer/).*

Builds reflective proposals using GEPA's parent evaluation, reflection and rewrite-evaluation sequence, with awaitable execution.

### Changes from GEPA

| Method | Change |
|---|---|
| `propose()` | Asynchronous; accepts explicit tasks, proposal history and shared parent-evaluation results. |
| `_batch_evaluate()` | Asynchronous; dispatches evaluations through the engine's shared queue. |
| `_propose_texts_batch_safe()` | Asynchronous; runs synchronous reflection in a worker thread, with serialization when required. |

### propose

```python
async def propose(
    self,
    state: GEPAState,
    *,
    tasks=None,
    proposal_trace=None,
    parent_evaluations=None,
) -> list[CandidateProposal]
```

| Parameter | Description |
|---|---|
| `state` | Current optimization state. |
| `tasks` | Proposal tasks supplied by the scheduler. |
| `proposal_trace` | History entry for the work being executed. |
| `parent_evaluations` | Shared parent-evaluation results for the group, avoiding repeated evaluation of the same parent work. |

**Returns:** evaluated candidate proposals. Selection and validation remain the engine's responsibility.

**Concurrency:** each proposal awaits its own dependencies while other proposal work progresses. Reflection operations run in worker threads. Independent reflectors may overlap; otherwise a lock covers the reflection operation and successor-reflector update.

A custom `reflect_many()` retains its batch boundary. The engine schedules its group together and receives results when that method returns. Otherwise, proposal execution can proceed through individual reflection calls.

Constructor settings and the underlying reflection interfaces follow the upstream class.

## AsyncMergeProposer

*Extends [MergeProposer](https://gepa.dev/api/proposers/MergeProposer/).*

Constructs merge candidates using GEPA's merge rules, with evaluations executed through the shared rollout queue.

### Changes from GEPA

| Method | Change |
|---|---|
| `propose()` | Asynchronous; awaits evaluations and accepts an explicit `proposal_trace`. |
| `schedule_if_needed()` | Inherited unchanged. |
| `select_eval_subsample_for_merged_program()` | Inherited unchanged. |

### propose

```python
async def propose(
    self,
    state: GEPAState[RolloutOutput, DataId],
    *,
    proposal_trace=None,
) -> CandidateProposal[DataId] | None
```

Attempts a scheduled merge using the current optimization state.

| Parameter | Description |
|---|---|
| `state` | Current candidates, scores and merge-relevant optimization state. |
| `proposal_trace` | History entry for this merge attempt. When omitted, uses the latest entry. |

**Returns:** a merge proposal, or `None` when no proposal is produced. The engine handles subsequent acceptance, validation and candidate insertion.

**Concurrency:** merge evaluations share the same queue and rollout limit as other evaluations. The engine permits one merge attempt at a time, while reflective proposals continue. A merge awaiting evaluation therefore does not block the rest of the optimizer.

Constructor settings and merge construction rules follow the upstream class. The scheduling signal is consumed before awaiting, so improvements arriving during an attempt remain available for later scheduling.

## RolloutQueue

*New engine-owned evaluation queue; no synchronous GEPA counterpart.*

Runs individual adapter evaluations through one bounded set of workers. The engine creates one queue for the run.

```python
RolloutQueue(adapter: AsyncGEPAAdapter, concurrency: int)
```

| Parameter | Description |
|---|---|
| `adapter` | Adapter used by all queue workers. |
| `concurrency` | Number of simultaneous singleton evaluations. |

### Methods

| Method | Returns | Behavior |
|---|---|---|
| `submit(example, candidate, *, capture_traces=False)` | An `asyncio.Future` | Queues one example and returns immediately. The future resolves to its `EvaluationBatch` or raises its evaluation exception. |
| `async submit_task()` | Runs until cancelled | Worker loop: awaits one singleton evaluation, then takes the next queued request. |
| `async close()` | `None` | Cancels workers and unresolved queued work during cleanup. |

Workers start on the first submission, inside the running event loop. Waiting requests are FIFO; completion order can differ. `concurrency` bounds active adapter calls, not the number of waiting queue entries.

Callers normally use the engine rather than interact with this queue directly. `close()` is cleanup, not the normal stopping policy: the engine finishes admitted work before closing on a normal stop.

## independent

*Scheduling capability on existing custom reflectors and proposal selectors; no separate async class.*

```python
independent = True
```

| Component | Meaning when true | Behavior when absent or false |
|---|---|---|
| Custom reflector | Reflection operations may overlap. Its implementation must coordinate shared state. | The reflection operation and successor update are serialized. |
| Custom proposal selector | Selection can be applied to each proposal independently. | Selection waits for the group's evaluated proposals. |

The built-in stateless reflector permits overlap. The engine also recognizes the exact built-in `AllImprovements` strategy for individual selection; custom implementations can opt in explicitly.

The attribute does not make synchronous methods native async methods, and it does not turn `reflect_many()` into a streaming interface.
