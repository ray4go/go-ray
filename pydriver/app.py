from . import api
import ray


@api.remote(num_cpus=100)
def test(*args) -> list:
    print(f"python task receive: {args}")
    res = api.golang_remote_task("Echo", [1, "str", b"bytes", [1, 2], {"k": 3}], {})
    print(f"python task return: {res}")
    return list(args)


@api.remote
def local(name, *args):
    res = api.golang_local_task(name, *args)
    print(f"golang_local_task task return: {res}")
    return res


@api.remote
def local2(name, *args):
    return "hello"


@api.remote
def golang_actor_test():
    hello_task = api.golang_task("Hello")
    # obj = api.golang_remote_task("Hello", ("wang",), {})
    obj = hello_task.remote("wang")
    print(f"{ray.get(obj)=}")

    for _ in range(3):
        actor = api.GolangLocalActor("actor", 20)
        print(actor)
        print(f"{actor.Incr(10)=}")
        # print(f"{actor.Incr(10)=}")

    actor = api.GolangActorClass("actor", num_cpus=1).options(name="myactor").remote(20)
    obj = actor.Incr.remote(10)
    obj = actor.Incr.options().remote(obj)
    print(f"{ray.get(obj)=}")

    a = ray.get_actor("myactor")
    print(f"{actor=}")
    a = api.GolangRemoteActorHandle(a)
    obj = a.Incr.remote(10)
    print(f"{ray.get(obj)=}")


@api.remote
class PyActor:
    def __init__(self, *args):
        self.args = args

    def get_args(self):
        return self.args

    def echo(self, *args):
        return args

    def busy(self, sec: int) -> int:
        import time

        time.sleep(sec)
        return sec
