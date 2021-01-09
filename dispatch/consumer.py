import json
from mypy_boto3.sqs.service_resource import Queue
from typing import Any, Callable, Optional, Tuple


from .exceptions import RetryException, ValidationError, IgnoreException
from .logger import log
from .spec import Message, ResponseMessage
from .task_manager import find_by_name
from .utils import get_queue, get_queue_messages, publish_message


def log_received_message(message_body: dict) -> None:
    log.info(f"Received message: {message_body}")


def log_invalid_message(message_json: str) -> None:
    log.error(f"Received invalid message {message_json}")


def message_handler(message_json: str, receipt: Optional[str]) -> None:
    try:
        message_body = json.loads(message_json)
        log.debug(f"Handle message: {message_body}")
        message = Message(
            message_body["task"],
            args=message_body["args"],
            kwargs=message_body["kwargs"],
            headers=message_body["headers"],
            reply_to=message_body["reply_to"],
            expiration=message_body["expiration"],
            id_=message_body["id"],
        )
    except (ValidationError, ValueError):
        log_invalid_message(message_json)
        raise

    log_received_message(message_body)

    try:
        task = find_by_name(message.task_name)
        result = task.call(message, receipt)
        if message.reply_to:
            log.info(f"Send response: reply_to={message.reply_to}")
            response_message = ResponseMessage(message, result)
            publish_message(get_queue(message.reply_to), json.dumps(response_message.as_dict()), {})
    except IgnoreException:
        log.info(f"Ignoring task {message.id}")
        return
    except RetryException:
        # Retry without logging exception
        log.info("Retrying due to exception")
        raise
    except Exception:
        log.error(f"Exception while processing message")
        raise


def message_handler_sqs(body: str, receipt: str) -> None:
    message_handler(body, receipt)


def fetch_and_process_messages(
    queue: Queue,
    num_messages: int = 1,
    visibility_timeout: int = None,
    handler: Callable = message_handler_sqs,
    handler_args: Tuple[Any] = None,
    get_wait_time: int = None,
) -> None:
    log.info(f"Queue count: {queue.url}: {queue.attributes['ApproximateNumberOfMessages']}")
    if handler_args is None:
        handler_args = []
    for queue_message in get_queue_messages(
        queue, num_messages, visibility_timeout=visibility_timeout, wait_time_seconds=get_wait_time
    ):
        try:
            handler(queue_message, *handler_args)
        except:
            # already logged in message_handler
            pass


def listen_for_messages(
    queue_name: str,
    num_messages: int = 1,
    visibility_timeout: int = None,
    handler: Callable = message_handler_sqs,
    handler_args: Tuple[Any] = None,
    get_wait_time: int = None,
) -> None:
    """
    Starts a dispatch listener for message types provided and calls the task function with given `args` and `kwargs`.

    If the task function accepts a param called `metadata`, it'll be passed in with a dict containing the metadata
    fields: id, timestamp, version, receipt.

    The message is explicitly deleted only if task function ran successfully. In case of an exception the message is
    kept on queue and processed again. If the task function keeps failing, SQS dead letter queue mechanism kicks in and
    the message is moved to the dead-letter queue.

    :param queue_name: The queue name to consume messages from
    :param num_messages: Maximum number of messages to fetch in one SQS API call. Defaults to 1
    :param visibility_timeout: The number of seconds the message should remain invisible to other queue readers.
        Defaults to None, which is queue default
    :param handler: Method to call to handle the message.  Takes one argument the SQSMessage
    :param handler_args: Tuple of arguments to pass to handler
    :param get_wait_time: Number of seconds to wait for a queue message
    """
    queue = get_queue(queue_name)
    log.info(f"Process message from {queue}")
    fetch_and_process_messages(
        queue,
        num_messages=num_messages,
        visibility_timeout=visibility_timeout,
        handler=handler,
        handler_args=handler_args,
        get_wait_time=get_wait_time
    )
