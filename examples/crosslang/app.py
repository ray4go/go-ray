import time
import ray
import goray


@goray.remote(num_cpus=1)
def echo(*args) -> list:
    print(f"python echo receive: {args}")
    return list(args)


@goray.remote
def hello(name: str):
    return f"hello {name}"


@goray.remote
def busy(sec: int):
    time.sleep(sec)


@goray.remote
class PyActor:
    def __init__(self, *args):
        self.args = args

    def get_args(self):
        return self.args

    def echo(self, *args):
        return args

    def hello(self, name: str):
        return f"hello {name}"

    def busy(self, sec: int):
        time.sleep(sec)


@goray.remote
def py_call_golang():
    # python call go ray task
    hello_task = goray.golang_task("Hello")
    obj = hello_task.remote("wang")
    print(f"{ray.get(obj)=}")

    # python call go ray actor
    actor = (
        goray.golang_actor_class("Counter", num_cpus=1)
        .options(name="myactor")
        .remote(20)
    )
    obj = actor.Incr.remote(10)
    obj = actor.Incr.options().remote(obj)
    print(f"{ray.get(obj)=}")

    # python get go named ray actor handle
    a = goray.get_golang_actor("myactor")
    obj = a.Incr.remote(10)
    print(f"{ray.get(obj)=}")

    # python call golang function in current process
    goray.golang_local_run_task("CallPython")
    goray.golang_local_run_task("RemoteCallPython")

    # python call golang class in current process and call its method
    actor = goray.golang_local_new_actor("Counter", 20)
    print(f"{actor.Incr(10)=}")
