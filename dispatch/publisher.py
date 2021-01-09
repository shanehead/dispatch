import json
from decimal import Decimal

from .conf import cfg
from .logger import log
from .spec import Message
from .utils import get_queue, queue_name_for_task, publish_message


def _log_published_message(message_body: dict) -> None:
    log.debug("Sent message", extra={"message_body": message_body})


def _decimal_json_default(obj):
    if isinstance(obj, Decimal):
        int_val = int(obj)
        if int_val == obj:
            return int_val
        else:
            return float(obj)
    raise TypeError


def _convert_to_json(data: dict) -> str:
    return json.dumps(data, default=_decimal_json_default)


def publish(message: Message) -> None:
    """ Publishes a message on the queue for the task"""
    message_body = message.as_dict()
    payload = _convert_to_json(message_body)

    queue_name = queue_name_for_task(message.task_name, cfg.routes)
    queue = get_queue(queue_name)
    publish_message(queue, payload, message.headers)

    _log_published_message(message_body)
