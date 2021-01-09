```mermaid

```


Entities:
cfg: Configuration from yaml
Task Manager: exposes task decorator and maintains global list of tasks
Task: A task that can be dispatched. The decorated function
AsyncInvocation: An instance of calling a task
AsyncResult: The future for retrieving a task result


@task decorator enrolls the method into *task_manager._ALL_TASKS*
task.dispatch() -> create Message -> publisher.publish() -> SQS
consumer.message_handler() -> create Message from JSON data -> task_manager.find_task_by_name()
    -> task.call(message) -> calls task function
    
    
# TODO: Fix testing for queue create/delete 60s time issue
# TODO: dead-letter queue config
# TODO: more graceful handling of reply_to queue being deleted before worker procs
# TODO: Message -> RequestMessage
# TODO: Worker initialization (db, caching, etc..)


Questions:
How is the connection to SQS managed? Persistent? does Session() return the same connection each call?


Worker:

Pool of N workers
One fetcher process per SQS queue we are reading from
The fetcher processes enqueue (multiprocessing.Queue) the Message for a worker to get()
The worker calls the task in the Message
The worker deletes the message from the queue


reply_to:
each task has it's own reply_to
RequestMessage and ResponseMessage are separate entities
Each publisher registers a reply queue per task
* Make sure publishers delete reply_to queues when exiting
