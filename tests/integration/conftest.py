from dispatch import utils
import faker
from mypy_boto3.sqs.service_resource import Queue
import os
import pytest

from dispatch.spec import Message

os.environ["SETTINGS_MODULE"] = "tests/test.yaml"


@pytest.fixture
def queue() -> Queue:
    name = "test_queue" + faker.Faker().word()
    queue = utils.create_queue(name)
    assert queue is not None

    yield queue, name

    queue.delete()


@pytest.fixture
def message() -> Message:
    return Message(
        "tests.tasks.sub1.sub1_tasks", ("arg1",), kwargs={"kwarg1": "kwarg1"}, headers={"header1": "header1"}
    )
