class ValidationError(Exception):
    pass


class TaskNotFound(Exception):
    pass


class ConfigurationError(Exception):
    pass


class RetryException(Exception):
    pass


class IgnoreException(Exception):
    pass


class QueueNotFound(Exception):
    pass
