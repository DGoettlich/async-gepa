"""Engine infrastructure for :mod:`async_gepa.optimize_anything`.

Most users should import from :mod:`async_gepa.optimize_anything`. This package holds
the shared engine protocol, eval server, budget tracker, and registries used by
that public API.
"""

from async_gepa.oa.budget import BudgetExhausted, BudgetTracker
from async_gepa.oa.config import OptimizeAnythingConfig
from async_gepa.oa.engine import Engine, Result
from async_gepa.oa.eval_server import EvalServer
from async_gepa.oa.registry import (
    get_engine_cls,
    get_task,
    list_engines,
    list_tasks,
    register_engine,
    register_task,
    register_task_factory,
)
from async_gepa.oa.task import Task

__all__ = [
    "BudgetExhausted",
    "BudgetTracker",
    "Engine",
    "EvalServer",
    "OptimizeAnythingConfig",
    "Result",
    "Task",
    "get_engine_cls",
    "get_task",
    "list_engines",
    "list_tasks",
    "register_engine",
    "register_task",
    "register_task_factory",
]
