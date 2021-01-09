from botocore.exceptions import ClientError
import boto3
from functools import lru_cache
from mypy_boto3 import sqs
from mypy_boto3.sqs.service_resource import Queue
from mypy_boto3.sqs.service_resource import Message as SQSMessage
from mypy_boto3.sqs.type_defs import SendMessageResultTypeDef
from retrying import retry
from typing import Dict, List, Optional

from .conf import cfg
from .exceptions import QueueNotFound
from .logger import log

# TODO: Reuse connections


@lru_cache(maxsize=1)
def _get_sqs_resource() -> sqs.ServiceResource:
    return boto3.resource(
        "sqs",
    )


def create_queue(queue_name: str) -> Queue:
    resource = _get_sqs_resource()
    url = resource.create_queue(QueueName=queue_name)
    log.debug(f"Create queue: {queue_name} ({url}")
    return url


def get_queue(queue_name: str) -> Queue:
    resource = _get_sqs_resource()
    try:
        return resource.get_queue_by_name(QueueName=queue_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
            msg = f"QueueName does not exist: {queue_name}"
            log.error(msg)
            raise QueueNotFound(msg)


def delete_message(queue_url: str, receipt: str) -> None:
    log.debug(f"delete message: {queue_url} {receipt}")
    client = _get_sqs_resource().meta.client
    client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)


@retry(stop_max_attempt_number=3, stop_max_delay=3000)
def publish_message(queue: Queue, message_json: str, message_attributes: dict) -> SendMessageResultTypeDef:
    # transform (http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#SQS.Client.send_message)
    log.info(f"Publish message {message_json} to {queue}")
    message_attributes = {k: {"DataType": "String", "StringValue": str(v)} for k, v in message_attributes.items()}
    return queue.send_message(MessageBody=message_json, MessageAttributes=message_attributes)


def _recursive_search(task: str, routes: Dict[str, str]) -> Optional[str]:
    try:
        return routes[task]
    except KeyError:
        module = ".".join(task.split(".")[:-1])
        if not module:
            raise KeyError(f"Unable to find queue name for task '{task}'")
        return _recursive_search(module, routes)


def queue_name_for_task(task: str, routes: Dict[str, str]) -> Optional[str]:
    """ Get the queue name to use for the task from cfg """
    route_paths = {key.replace("::", "."): value for key, value in routes.items()}
    return _recursive_search(task, route_paths)


def get_queue_messages(
    queue: Queue, num_messages: int, visibility_timeout: int = None, wait_time_seconds: int = None
) -> List[SQSMessage]:
    params = {
        "MaxNumberOfMessages": num_messages,
        "WaitTimeSeconds": wait_time_seconds or cfg.aws.sqs.poll_time,
        "MessageAttributeNames": ["All"],
    }
    if visibility_timeout is not None:
        params["VisibilityTimeout"] = visibility_timeout
    return queue.receive_messages(**params)
