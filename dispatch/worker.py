import argparse
import atexit
from dataclasses import dataclass
import importlib
import itertools
from multiprocessing import Queue, Process, cpu_count, current_process
from mypy_boto3.sqs.service_resource import Message as SQSMessage
import threading
from typing import List

from dispatch.conf import cfg
from dispatch.consumer import listen_for_messages, message_handler_sqs
from dispatch.exceptions import QueueNotFound
from dispatch.logger import log
from dispatch.utils import create_queue, delete_message

"""
Main process fetches and locally enqueues data for workers
We employ this model so that we don't stall reads on a queue and can implement thresholds
for how many records are locally enqueued to minimize message loss during a restart.
"""


# TODO: High water mark to throttle ourselves from fetching


@dataclass
class QueueMessage:
    """ SQS Queue classes can't be pickled, so encapsulate the required info """

    body: str
    receipt: str
    queue_url: str


def finish(worker_pool: List[Process], fetcher_pool: List[Process], work_queue: Queue) -> None:
    log.debug("Finish")
    # Stop the workers
    [work_queue.put(None) for _ in worker_pool]

    # Kill the fetchers
    for p in fetcher_pool:
        if p.is_alive():
            log.debug(f"Terminating fetcher: {p.name}")
            p.terminate()

    # Join them all
    for p in fetcher_pool + worker_pool:
        log.debug(f"Joining: {p.name}")
        p.join()


def worker(work_queue: Queue) -> None:
    # Import our tasks so they get registered by the decorator
    for task in cfg.tasks:
        log.debug(f"{current_process().name}: Import {task}")
        importlib.import_module(task)

    while True:
        message: QueueMessage = work_queue.get()
        log.info(f"Message(worker): {message}")
        if message is None:
            log.info(f"{current_process().name}: Received None. Exiting")
            return
        try:
            message_handler_sqs(message.body, message.receipt)
        except QueueNotFound:
            log.error("Reply_to queue not found")
        except Exception as e:
            log.error(f"Error while handling message: {message}: {e}")
        finally:
            delete_message(message.queue_url, message.receipt)


def _enqueue(message: SQSMessage, work_queue: Queue) -> None:
    """ SQSMessage can't be pickled, so we need to extract what we need

    :param message: SQSMessage from the SQS Queue
    """
    data = QueueMessage(message.body, message.receipt_handle, message.queue_url)
    log.info(f"Put to work_queue: {data}")
    work_queue.put(data)


def fetcher(
        queue_name: str,
        work_queue: Queue,
        loop_count: int,
        num_messages: int,
        get_wait_time: int,
        shutdown_event: threading.Event = None,
) -> None:
    log.info(f"{current_process().name}: Fetching {queue_name}. loop_count={loop_count}")
    # Make sure the queue is created
    queue = create_queue(queue_name)
    # Can't directly put, because boto objects can't be pickled
    for count in itertools.count():
        if (loop_count is None or count < loop_count) and not shutdown_event.is_set():
            log.info(f"Queue count: {queue.url}: {queue.attributes['ApproximateNumberOfMessages']}")
            listen_for_messages(
                queue_name,
                num_messages=num_messages,
                handler=_enqueue,
                handler_args=(work_queue,),
                visibility_timeout=cfg.aws.sqs.visibility_timeout,
                get_wait_time=get_wait_time,
            )
        else:
            log.info("Done fetching")
            return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-w, --workers", metavar="workers", dest="workers", help="Number of workers", type=int, default=cpu_count()
    )
    parser.add_argument(
        "-m, --max-workers",
        metavar="max_workers",
        dest="max_workers",
        help="Max number of workers",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--loop-count", metavar="loop_count", dest="loop_count", help="How many times to check for message", type=int
    )
    parser.add_argument(
        "-n, --num-messages",
        metavar="num_messages",
        dest="num_messages",
        help="Number of messages to grab each loop",
        type=int,
        default=5,
    )
    parser.add_argument(
        "--get-wait-time",
        metavar="get_wait_time",
        dest="get_wait_time",
        help="Number of seconds to wait for messages",
        type=int,
        default=2,
    )

    args = parser.parse_args()
    work_queue = Queue()

    log.info(f"Starting: tasks={cfg.tasks} workers={args.workers}")
    worker_pool = []
    for _ in range(args.workers):
        p = Process(target=worker, args=(work_queue,))
        p.start()
        worker_pool.append(p)

    fetcher_pool = []
    for queue_name in cfg.routes.values():
        p = Process(
            target=fetcher,
            args=(queue_name, work_queue, args.loop_count, args.num_messages, args.get_wait_time, threading.Event()),
        )
        p.start()
        fetcher_pool.append(p)

    atexit.register(finish, fetcher_pool, worker_pool, work_queue)

    [p.join() for p in fetcher_pool]


if __name__ == "__main__":
    main()
