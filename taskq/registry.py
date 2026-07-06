"""The task registry: maps a task *name* to the function that runs it.

Why a registry at all? A job travelling through Redis only carries the task's
*name* (a string) plus its arguments — never the function itself. So both the
producer and the worker must share a lookup from name -> function. The producer
needs the name to enqueue; the worker needs the function to execute. This is
the decoupling that lets them run as separate processes on separate machines.
"""
from collections.abc import Callable

# Module-level map. It's populated at import time: importing a module that
# contains @task-decorated functions registers them as a side effect.
_REGISTRY: dict[str, Callable] = {}


def task(func: Callable | None = None, *, name: str | None = None):
    """Register a plain function as a runnable task.

    Supports both bare and parameterized use:
        @task
        def send_email(to): ...

        @task(name="emails.welcome")
        def welcome(to): ...
    """
    def register(fn: Callable) -> Callable:
        task_name = name or fn.__name__
        if task_name in _REGISTRY:
            # Fail loudly on a duplicate name. A silent overwrite would mean the
            # wrong function runs for a given job — a brutal bug to track down.
            raise ValueError(f"task '{task_name}' is already registered")
        _REGISTRY[task_name] = fn
        return fn  # return the original function unchanged, so it stays callable directly

    # @task            -> func is the function, register immediately
    # @task(name=...)  -> func is None, return the decorator to apply next
    if func is not None:
        return register(func)
    return register


def get_handler(name: str) -> Callable:
    try:
        return _REGISTRY[name]
    except KeyError:
        # A missing handler almost always means the worker process forgot to
        # import the module that defines the task. Say so explicitly.
        raise KeyError(
            f"no task registered under '{name}'. Did the worker import the "
            f"module where it's defined?"
        )


def registered_names() -> list[str]:
    return sorted(_REGISTRY)
