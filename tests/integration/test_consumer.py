import json
import pytest

from dispatch import consumer, spec, exceptions
from ..tasks.test_tasks import square


def test_message_handler():
    task = square.task
    message = spec.Message(task.name, args=(5,), kwargs={}, headers={}, reply_to="bad")
    with pytest.raises(exceptions.QueueNotFound):
        consumer.message_handler(json.dumps(message.as_dict()), None)
