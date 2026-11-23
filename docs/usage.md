# Using AsyncGEPA

AsyncGEPA runs GEPA optimization with overlapping proposal execution. While one proposal waits for reflection, others can keep evaluating. Individual evaluation requests share a queue and start as capacity becomes available.

This guide covers enabling that execution mode and adapting your own components. For GEPA's optimization concepts and unchanged options, see the [upstream documentation](https://gepa.dev/).

## Choose an execution mode

Use `async_gepa.optimize()` for both standard and continuous execution.

```python
from async_gepa import optimize
from async_gepa.strategies.scheduling import ContinuousRollouts

# Standard GEPA execution.
result = optimize(
    **settings,
    adapter=sync_adapter,
)

# Continuous asynchronous execution.
result = optimize(
    **settings,
    adapter=async_adapter,
    scheduler=ContinuousRollouts(),
)
```

`settings` contains your usual optimization arguments, including the initial candidate, datasets, reflection model and budget.

| Mode | Adapter requirement | Execution |
|---|---|---|
| Standard | `def evaluate(...)` | GEPA's coordinated optimization steps. |
| Continuous | `async def evaluate(...)` | Overlapping proposal work and a shared evaluation queue. |

`optimize()` remains a synchronous entry point: it returns the final `GEPAResult` when optimization finishes. It creates its own event loop for continuous execution; call it outside an already-running event loop.

## Adapt your evaluation method

Change `evaluate()` into an asynchronous method and await the model request inside it:

```diff
-def evaluate(self, batch, candidate, capture_traces=False):
+async def evaluate(self, batch, candidate, capture_traces=False):
     ...
-    output = client.generate(...)
+    output = await async_client.generate(...)
```

The client calls above illustrate the change; use your client's actual asynchronous interface.

Everything returned by the method retains GEPA's existing format: outputs, scores and optional trajectories inside an `EvaluationBatch`. `make_reflective_dataset()` remains synchronous.

Continuous scheduling calls `evaluate()` with **one example per call**, with multiple calls active on the same adapter instance. Consequently:

- Each example must be independently evaluable.
- Per-call temporary results must not overwrite one another.
- Model requests must yield while waiting; a blocking call inside `async def` still blocks the event loop.

You do not need to implement a separate batch scheduler. AsyncGEPA handles request concurrency and assembles results for each proposal.

See [GEPA's adapter guide](https://gepa.dev/guides/adapters/) for the unchanged adapter contract.

## Configure how much work can overlap

```python
scheduler = ContinuousRollouts(
    max_in_flight_proposals=8,
    max_in_flight_rollouts=64,
    max_proposal_age=3,
)
```

These are the defaults.

**Proposal concurrency** controls how much proposal work can be active. **Rollout concurrency** controls how many individual adapter evaluations can execute simultaneously.

For example, a proposal can wait for reflection while other proposals use the evaluation slots. Those slots are refilled from the shared FIFO queue as requests finish.

Sampling remains a separate choice:

```python
from async_gepa.strategies.proposal_sampling import PxNSampling

result = optimize(
    **settings,
    adapter=async_adapter,
    sampling_strategy=PxNSampling(p=2, n=2),
    scheduler=ContinuousRollouts(
        max_in_flight_proposals=8,
        max_in_flight_rollouts=64,
    ),
)
```

Here, each sampling call creates four proposals. The scheduler can overlap work from different sampling groups; the rollout limit applies across all of them.

### Adjusting concurrency

| What you observe | What to examine |
|---|---|
| Evaluation slots are occupied, but the model server has spare capacity | Increase `max_in_flight_rollouts`. |
| Evaluations run out while active proposals wait for reflection | Increase `max_in_flight_proposals` to allow more work to overlap. |
| The model server is already saturated or requests are backing up | More client concurrency may add waiting without improving throughput. |

These settings control capacity. Whether it is filled depends on the work available and its dependencies.

### Allowing older proposals to finish

Other candidates may enter the pool while a proposal is running. `max_proposal_age` controls whether that proposal has become outdated.

Rejection requires **both**:

- More than `max_proposal_age` candidates have been added since admission.
- None of the proposal's parents remains on the frontier.

Disable this check with:

```python
scheduler = ContinuousRollouts(max_proposal_age=None)
```

## Choose when selection can proceed

Your selection strategy determines whether a finished proposal can advance immediately.

| Strategy | Behavior under continuous scheduling |
|---|---|
| `AllImprovements` | An accepted improvement can proceed to validation without waiting for siblings. |
| `BestImprovement` | Waits for the group so it can choose the best improvement. |
| `TopKImprovements` | Waits for the group so it can choose the top K. |

A group-level wait does not stop other groups from progressing.

Custom selectors retain GEPA's `select(...)` interface. If your selector can make its decision independently for each proposal, declare this on the class:

```python
independent = True
```

Leave it unset when the decision requires comparing siblings. Selecting the first K completed improvements, for example, is a different rule from selecting the best K improvements.

See [GEPA's sampling and selection guide](https://gepa.dev/guides/parallel-proposals/) for the underlying strategies.

## Use a custom reflector

The built-in stateless reflector supports overlapping calls automatically. You can continue supplying a reflection model through `reflection_lm`.

For custom reflection behavior, use GEPA's existing `reflection_strategy` interface. Its concurrency behavior depends on the implementation:

| Custom reflector | Execution |
|---|---|
| No `independent` flag, or `independent = False` | Reflection operations and successor updates execute sequentially. Other evaluations continue. |
| `independent = True` | Reflection operations may overlap. The implementation must coordinate any shared state. |
| Implements `reflect_many()` | The batch method is retained; its results become available when it returns. |

A reflector that builds each request from the previous call's updated history should remain sequential. A reflector that safely supports overlapping requests can opt in with `independent = True`.

The flag does not change `reflect_many()` into an interface that returns individual results early.

Reflection can use a hosted service or a local model; the scheduling behavior is the same.

## Keep existing optimization features

Validation policies and evaluation caching remain available. Required evaluations enter the shared queue; cached results are reused.

Merging remains optional:

```python
result = optimize(
    **settings,
    adapter=async_adapter,
    scheduler=ContinuousRollouts(),
    use_merge=True,
)
```

One merge attempt runs at a time. Its evaluations share the rollout queue and can overlap reflective proposal work.

## Stop, save and resume

When a stopping condition is reached, AsyncGEPA stops admitting new work and finishes work already admitted. The final metric-call count can therefore exceed the requested budget.

Use GEPA's existing `run_dir` mechanism for checkpoints. Saved state contains completed work, not outstanding requests. Resuming after an interruption does not restore calls that were in flight.

The result remains a `GEPAResult`:

```python
print(result.best_candidate)
```

For class signatures and the changes behind this behavior, see the [AsyncGEPA API reference](api.md).
