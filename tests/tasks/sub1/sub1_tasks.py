from dispatch.task_manager import task


@task
def print_stuff(words: str) -> None:
    print(words)