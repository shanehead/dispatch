import atexit
import copy
from functools import lru_cache
import inspect
import json
from mypy_boto3.sqs.service_resource import Queue
import time
from typing import Any, Callable, Optional
import uuid

from .exceptions import ConfigurationError
from .logger import log
from .publisher import publish
from .spec import Message
from .utils import create_queue, get_queue_messages


class AsyncResult:
    def __init__(self, message: Message, reply_to: Queue = None, value: Any = None):
        self.message = message
        self.reply_to = reply_to
        self.value = value  # For synchronous testing

    def get(self, timeout: int = None) -> Any:
        """ Get the result from the reply_to queue

        Returns None immediately if the task does not define a response
        Returns the response value of the task immediately if we are running synchronously
        Wait timeout seconds (or cfg.aws.sqs.poll_time if not set) on receiving the result
            from the reply_to queue for the task

        :param timeout:
        :return:
        """
        if self.value:
            return self.value

        if not self.reply_to:
            return None

        start_time = time.time()

        log.info(f"Get result from {self.reply_to}")
        messages = get_queue_messages(self.reply_to, 1, wait_time_seconds=timeout)
        if messages:
            message = messages[0]
            response_json = json.loads(message.body)
            log.debug(f"Got message {response_json}")
            if not response_json["original"]["id"] == self.message.id:
                log.info(f"Mismatch ID {response_json['original']['id']} {self.message.id}")
                time_passed = int(time.time() - start_time)
                if time_passed >= timeout:
                    raise TimeoutError
                return self.get(timeout - time_passed)

            message.delete()
            return response_json["result"]

        raise TimeoutError


class AsyncInvocation:
    """ Represents one particular invocation of a task.

    An invocation may be customized using `with_` functions, and these won't affect other invocations of the same task.
    Invocations may also be saved and re-used multiple times.
    """

    def __init__(self, task: "Task") -> None:
        self._task = task
        self._headers: dict = {}
        self._synchronous: bool = False

    @staticmethod
    @lru_cache(maxsize=128)
    def reply_to_queue(queue_name: Optional[str]) -> Optional[Queue]:
        if not queue_name:
            return None
        queue = create_queue(queue_name)
        atexit.register(queue.delete)
        return queue

    def with_synchronous(self, synchronous=True):
        """
        Override the cfg synchronous setting
        :param synchronous: True to make this call be synchronous
        :return: Updated invocation with synchronous setting
        """
        self._synchronous = synchronous
        return self

    def with_headers(self, **headers) -> "AsyncInvocation":
        """
        Customize headers for this invocation
        :param headers: Arbitrary headers dict
        :return: updated invocation that uses custom headers
        """
        self._headers.update(headers)
        return self

    def dispatch(self, *args, **kwargs) -> AsyncResult:
        """ Dispatch task for async execution

        :param args: arguments to pass to the task
        :param kwargs: keyword args to pass to the task
        """
        message = Message(
            self._task.name,
            copy.deepcopy(args),
            copy.deepcopy(kwargs),
            headers={**self._headers},
            reply_to=self._task.reply_to
        )
        log.debug(f"Dispatch: {message}")
        if self._synchronous:  # For testing
            log.info("Warning: Running synchronously")
            ret = self._task.call(message, None)
            return AsyncResult(message, self.reply_to_queue(message.reply_to), value=ret)
        else:
            publish(message)
            return AsyncResult(message, self.reply_to_queue(message.reply_to))


class Task:
    """
    Represents a dispatch task. This class provides methods to dispatch tasks asynchronously,
    You can also chain customizations such as:

    .. code:: python

        send_email.with_headers(request_id='1234').dispatch('example@email.com')

    These customizations may also be saved and re-used multiple times

    """

    def __init__(self, fn: Callable, name: str) -> None:
        self._name = name
        self._fn = fn
        signature = inspect.signature(fn)
        self._accepts_metadata = False
        self._accepts_headers = False
        self._reply_to = None

        for p in signature.parameters.values():
            # if **kwargs is specified, just pass all things by default since function can always inspect arg names
            if p.kind == inspect.Parameter.VAR_KEYWORD:
                self._accepts_metadata = self._accepts_headers = True
            elif p.kind == inspect.Parameter.VAR_POSITIONAL:
                # disallow use of *args
                raise ConfigurationError("Use of *args is not allowed")
            elif p.name == "metadata":
                if p.annotation is not inspect.Signature.empty and p.annotation is not dict:
                    raise ConfigurationError("Signature for 'metadata' param must be dict")
                self._accepts_metadata = True
            elif p.name == "headers":
                if p.annotation is not inspect.Signature.empty and p.annotation is not dict:
                    raise ConfigurationError("Signature for 'headers' param must be dict")
                self._accepts_headers = True

        # If the task returns data, we need to set up a reply queue for the response
        if signature.return_annotation not in (None, signature.empty):
            self._reply_to = self.name.split(".")[-1] + "-" + str(uuid.uuid4())[:8]

    @property
    def name(self) -> str:
        """
        :return: Task name
        """
        return self._name

    @property
    def fn(self) -> Callable:
        """"
        return: Task function
        """
        return self._fn

    @property
    def accepts_metadata(self) -> bool:
        """
        :return: Flag indicating if task accepts metadata
        """
        return self._accepts_metadata

    @property
    def accepts_headers(self) -> bool:
        """
        :return: Flag indicating if task accepts headers
        """
        return self._accepts_headers

    @property
    def reply_to(self) -> Optional[str]:
        """
        :return: The reply_to queue name, or None if no return
        """
        return self._reply_to

    def with_synchronous(self, synchronous=True) -> AsyncInvocation:
        """ Create a task invocation and override synchronous setting

        :param synchronous: True to make this call be synchronous
        :return: an invocation that uses custom headers
        """
        return AsyncInvocation(self).with_synchronous(synchronous)

    def with_headers(self, **headers) -> AsyncInvocation:
        """ Create a task invocation that uses custom headers

        :param headers: Arbitrary headers
        :return: an invocation that uses custom headers
        """
        return AsyncInvocation(self).with_headers(**headers)

    def dispatch(self, *args, **kwargs) -> Any:
        """ Dispatch task for async execution

        :param args: arguments to pass to the task
        :param kwargs: keyword args to pass to the task
        """
        return AsyncInvocation(self).dispatch(*args, **kwargs)

    def call(self, message: Message, receipt: Optional[str]) -> Any:
        """ Calls the task with this message

        :param message: The message
        :param receipt: SQS receipt
        """
        args = copy.deepcopy(message.args)
        kwargs = copy.deepcopy(message.kwargs)
        if self.accepts_metadata:
            kwargs["metadata"] = {
                "id": message.id,
                "timestamp": message.timestamp,
                "version": message.version,
                "receipt": receipt,
            }
        if self.accepts_headers:
            kwargs["headers"] = copy.deepcopy(message.headers)
        return self.fn(*args, **kwargs)

    def __str__(self) -> str:
        return f"dispatch task: {self.name}"
