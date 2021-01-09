from dispatch.task_manager import task


@task
def square(x: int) -> int:
    print(f"square({x})")
    return x ** 2


@task
def cube(x: int) -> int:
    print(f"cube({x})")
    return x ** 3
