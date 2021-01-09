import subprocess
import sys

from dispatch.conf import cfg
from dispatch.logger import log
from dispatch.utils import create_queue, queue_name_for_task
from tests.tasks.test_tasks import square, cube


def test_worker():
    log.info("Starting test")
    square_queue = create_queue(queue_name_for_task("tests.tasks.test_tasks.square", cfg.routes))
    cube_queue = create_queue(queue_name_for_task("tests.tasks.test_tasks.cube", cfg.routes))
    square_result = square.dispatch(3)
    cube_result = cube.dispatch(5)
    subprocess.run([sys.executable, "-m", "dispatch.worker", "--loop-count=1", "-w", "1"])
    square_value = square_result.get(2)
    cube_value = cube_result.get(2)
    assert square_value == 9
    assert cube_value == 125
    try:
        square_queue.purge()
    except:
        pass
    try:
        cube_queue.purge()
    except:
        pass
