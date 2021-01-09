from config import config
import os

from .exceptions import ConfigurationError

"""
YAML:
# AWS and SQS information and config
aws:
  sqs:
    endpoint: "string"
    connect_timeout: 2.0
    poll_time: 20.0

# Tasks to make available to workers
tasks:
  - galileo.app.app1.tasks
  - galileo.app.app2.tasks

# Routing for tasks to queues for producers
# Keys are python modules.  To route a specific method in that module, use ::
routes:
  # Routes are chosen based on longest match
  galileo.app.app1.tasks: all_app1_tasks_queue
  galileo.app.app2.tasks: most_app2_tasks_queue
  galileo.app.app2.tasks::other_task: other_task_queue

# Databases to connect to
dbs:
  - sofi
  - galileo


JSON:
{
    "aws": {
        "sqs": {
            "endpoint": "string",
            "connect_timeout": 2.0,
            "poll_time": 20.0
        }
    },
    "tasks": ["galileo.app.app1.tasks", "galileo.app.app2.tasks"], 
    "routes": {
        "galileo.app.app1.tasks": "all_app1_tasks_queue",
        "galileo.app.app2.tasks": "most_app2_tasks_queue",
        "galileo.app.app2.tasks::other_task": "other_task_queue",
    },
    "dbs": ["sofi", "galileo"]
}

ENV:
# Environment variables must start with DISPATCH__
# Two underscores (__) separate objects.  A single underscore is literal
DISPATCH__AWS__REGION="string"
DISPATCH__AWS__ACCOUNT_ID="string"
...
DISPATCH__SQS__POLL_TIME=20.0
...
"""

PREFIX = "DISPATCH"


DEFAULT = {"aws": {"sqs": {"connect_timeout": 2.0, "poll_time": 20.0}}, "synchronous": False}

REQUIRED = {
    "aws.sqs": ["endpoint"],
    "tasks": [],
    "routes": [],
    "dbs": [],
}


def validate_config(c):
    failures = []
    for key, required_fields in REQUIRED.items():
        try:
            attr = getattr(c, key, None)
        except KeyError:
            failures.append(key)
            continue
        if not attr:
            failures.append(key)
            continue
        for field in required_fields:
            try:
                if not getattr(attr, field, None):
                    failures.append(f"{attr}.{field}")
            except KeyError:
                failures.append(f"{attr}.{field}")

    if failures:
        missing = ", ".join(failures)
        raise ConfigurationError(f"Required field(s) missing: {missing}")


# SETTINGS_MODULE is a path to a file (yaml, JSON, etc..)
settings_module = os.environ.get("SETTINGS_MODULE")
if settings_module:
    cfg = config("env", settings_module, DEFAULT, prefix=PREFIX, lowercase_keys=True, separator="__")
else:
    cfg = config("env", DEFAULT, prefix=PREFIX, lowercase_keys=True, separator="__")

validate_config(cfg)
