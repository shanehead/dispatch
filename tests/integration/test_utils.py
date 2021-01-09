import pytest

from dispatch import utils

path = "a.b.c.d.e"
taskname = "taskname"

module_routes = {path[:x]: path[:x] for x in range(9, 0, -2)}
taskname_routes = {f"{x}::{taskname}": f"{x}.{taskname}" for x in module_routes}


@pytest.mark.parametrize("task", module_routes.keys())
def test_recursive_search_module_routes(task):
    assert utils._recursive_search(f"{task}.{taskname}", module_routes) == task


@pytest.mark.parametrize("task", module_routes.keys())
def test_queue_name_for_task_modules(task):
    assert utils.queue_name_for_task(f"{task}.{taskname}", module_routes) == task


@pytest.mark.parametrize("task", module_routes.keys())
def test_queue_name_for_specific_task(task):
    task_path = f"{task}.{taskname}"
    assert utils.queue_name_for_task(task_path, taskname_routes) == task_path


def test_get_sqs_resource():
    assert utils._get_sqs_resource() is not None


def test_get_queue(queue):
    assert utils.get_queue(queue[1]) is not None
