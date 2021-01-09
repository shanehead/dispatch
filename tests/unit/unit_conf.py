import os
import pytest
import tempfile
import yaml

from dispatch.exceptions import ConfigurationError


def good_config():
    return {
        "aws": {
            "region": "string",
            "account_id": "string",
            "access_key": "string",
            "secret_key": "string",
            "session_token": "string",
            "sqs": {"endpoint": "string", "connect_timeout": 2.0, "poll_time": 10.0},
        },
        "tasks": ["galileo.app.app1.tasks", "galileo.app.app2.tasks"],
        "routes": {
            "galileo.app.app1.tasks": "all_app1_tasks_queue",
            "galileo.app.app2.tasks::other_task": "other_task_queue",
            "galileo.app.app2.tasks": "most_app2_tasks_queue",
        },
        "dbs": ["sofi", "galileo"],
    }


@pytest.fixture
def as_yaml(request):
    config = request.param
    with tempfile.NamedTemporaryFile("w", suffix=".yaml") as f:
        yaml.dump(config, f)
        os.environ["SETTINGS_MODULE"] = f.name

        yield config


def compare_cfg(cfg, data):
    for key, value in data.items():
        if isinstance(value, dict):
            compare_cfg(getattr(cfg, key), value)
        else:
            assert dict(cfg)[key] == value


def bad_config():
    return {"aws": None}


@pytest.mark.parametrize("as_yaml", [good_config()], indirect=["as_yaml"])
def test_good_yaml(as_yaml):
    from dispatch.conf import cfg

    compare_cfg(cfg, as_yaml)


@pytest.mark.parametrize("as_yaml", [bad_config()], indirect=["as_yaml"])
def test_bad_yaml(as_yaml):
    with pytest.raises(ConfigurationError):
        from dispatch.conf import cfg  # noqa
