from typing import Any, Optional, Callable

from .exceptions import ConfigurationError, TaskNotFound
from .task import Task


_ALL_TASKS: dict = {}


def task(*args, name: Optional[str] = None) -> Callable:
    """ Decorator for dispatch task functions. Any function may be converted into a task by adding this decorator

    .. code:: python

        @dispatch.taskmanager.task
        def send_email(to: str, subject: str, from: str = None) -> None:
            ...

    Additional methods available on tasks are described by :class:`dispatch.Task` class
    """

    def _decorator(fn: Any) -> Callable:
        task_name = name or f"{fn.__module__}.{fn.__name__}"
        existing_task = _ALL_TASKS.get(task_name)
        if existing_task is not None:
            func = existing_task.fn
            raise ConfigurationError(f'Task named "{task_name}" already exists: {func.__module__}.{func.__name__}')

        fn.task = Task(fn, task_name)
        fn.dispatch = fn.task.dispatch
        fn.with_headers = fn.task.with_headers
        _ALL_TASKS[fn.task.name] = fn.task
        return fn

    if len(args) == 1 and callable(args[0]):
        # No arguments, this is the decorator
        return _decorator(args[0])

    return _decorator


def find_by_name(name: str) -> Task:
    """ Finds a task by name

    :param name: task name (including module)
    :return: Task
    :raises TaskNotFound: if task isn't registered
    """
    if name in _ALL_TASKS:
        return _ALL_TASKS[name]
    raise TaskNotFound
