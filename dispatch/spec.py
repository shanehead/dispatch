import time
from typing import Any, Optional, ItemsView, cast
import uuid


from .exceptions import ValidationError


class Message:
    """ Model for Task messages. All properties of a message should be considered immutable """

    CURRENT_VERSION = "1.0"
    VERSIONS = ["1.0"]

    def __init__(
        self,
        task_name: str,
        args: tuple = None,
        kwargs: dict = None,
        headers: dict = None,
        reply_to: str = None,
        expiration: int = None,
        id_: str = None,
        timestamp: int = None,
    ) -> None:
        """
        data will look like this:
        {
            "id": "b1328174-a21c-43d3-b303-964dfcc76efc",
            "metadata": {
                "timestamp": 1589084716206902000,
                "version": "1.0",
            },
            "headers": {
                ...
            },
            "expiration": 4000,
            "reply_to": "queue_url",
            "task": "tasks.send_email",
            "args": [
                "email@automatic.com",
                "Hello!"
            ],
            "kwargs": {
                "from_email": "spam@example.com"
            }
        }


        :param reply_to: The name of the reply_to queue for the message (not the URL)
        """
        self._id = id_ or str(uuid.uuid4())
        self._task_name = task_name
        self._args = args
        self._kwargs = kwargs or {}
        self._headers = headers or {}
        self._timestamp = timestamp or time.time_ns()
        self._expiration = expiration
        self._metadata = {"timestamp": self._timestamp, "version": self.CURRENT_VERSION}
        self._reply_to = reply_to

        self.validate()

    def validate(self) -> None:
        """
        Validate that message object contains all the right things.
        :raises exceptions.ValidationError: when message fails validation
        """

        # support string datetimes
        if isinstance(self.timestamp, str):
            try:
                self.metadata["timestamp"] = int(self.timestamp)
            except ValueError:
                raise ValidationError

        required = ("id", "version", "timestamp", "headers", "task_name", "args", "kwargs")
        for attr in required:
            if getattr(self, attr, None) is None:
                raise ValidationError(f"Missing attribute: {attr}")

        if self.version not in self.VERSIONS:
            raise ValidationError(f"Invalid version: {self.version}")

        if self.expiration and time.time_ns() > self._timestamp + self.expiration:
            raise ValidationError(f"Expired message: timestamp={self._timestamp} expiration={self.expiration}")

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.as_dict() == cast(Message, other).as_dict()

    @property
    def id(self) -> str:
        return self._id

    @property
    def metadata(self) -> dict:
        return self._metadata

    @property
    def timestamp(self) -> Optional[int]:
        return self._metadata.get("timestamp")

    @property
    def version(self) -> Optional[int]:
        return self._metadata.get("version")

    @property
    def headers(self) -> dict:
        return self._headers

    @property
    def task_name(self) -> str:
        return self._task_name

    @property
    def args(self) -> tuple:
        return self._args

    @property
    def kwargs(self) -> dict:
        return self._kwargs

    @property
    def reply_to(self) -> Optional[str]:
        return self._reply_to

    @property
    def expiration(self) -> Optional[int]:
        return self._expiration

    def items(self) -> ItemsView:
        return self.as_dict().items()

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "metadata": self.metadata,
            "headers": self.headers,
            "task": self.task_name,
            "args": self.args,
            "kwargs": self.kwargs,
            "expiration": self.expiration,
            "reply_to": self.reply_to,
        }


class ResponseMessage:
    CURRENT_VERSION = "1.0"
    VERSIONS = ["1.0"]

    def __init__(self, original: Message, result: Any) -> None:
        """

        :param original: The original Message
        :param result: Result of the task.  Must be JSON serializable

        {
            "id": "3afbd974-a21c-43d3-8f93-964dfcc76efc",
            "metadata": {
                "timestamp": 1589081345789123450,
                "version": "1.0",
            },
            "result": 5,
            "original": {
                "id": "b1328174-a21c-43d3-b303-964dfcc76efc",
                "metadata": {
                    "timestamp": 1589084716206902000,
                    "version": "1.0",
                },
                "headers": {
                    ...
                },
                ...
            }
        }
        """
        self._id = str(uuid.uuid4())
        self._timestamp = time.time_ns()
        self._metadata = {"timestamp": self._timestamp, "version": self.CURRENT_VERSION, "expiration": None}
        self._original = original
        self._result = result

    @property
    def id(self) -> str:
        return self._id

    @property
    def timestamp(self) -> int:
        return self._timestamp

    @property
    def metadata(self) -> dict:
        return self._metadata

    @property
    def original(self) -> Message:
        return self._original

    @property
    def result(self) -> Any:
        return self._result

    def as_dict(self) -> dict:
        return {"id": self.id, "metadata": self.metadata, "result": self.result, "original": self.original.as_dict()}
