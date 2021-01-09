import pytest
from unittest import mock

from dispatch import task, utils

TASK_NAME = "test_task_name"
QUEUE_NAME = "fn_task"


def fn_task(arg1: int, kw1: int = None, headers: dict = None, metadata: dict = None) -> tuple:
    return arg1, kw1, headers, metadata


@pytest.fixture
def task_obj():
    with mock.patch("tests.integration.test_task.fn_task", autospec=True) as mock_fn:
        yield task.Task(mock_fn, TASK_NAME), mock_fn


def test_task(task_obj):
    t, mock_fn = task_obj
    assert t.name == TASK_NAME
    assert t.fn == fn_task
    assert t.accepts_headers
    assert t.accepts_metadata
    t.with_synchronous(True).dispatch(1, 2)
    mock_fn.assert_called_once_with(1, 2, headers={}, metadata=mock.ANY)


def test_with_headers(task_obj):
    t, mock_fn = task_obj
    t.with_headers(header="header").with_synchronous(True).dispatch(1, 2)
    mock_fn.assert_called_once_with(1, 2, headers={"header": "header"}, metadata=mock.ANY)
