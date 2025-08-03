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
def no_return(name: str):
    return


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

    def busy(self, sec: int) -> int:
        import time

        time.sleep(sec)
        return sec


def main():
    # python调用go声明的ray task
    hello_task = goray.golang_task("Hello")
    obj = hello_task.remote("wang")
    print(f"{ray.get(obj)=}")

    # python调用go声明的ray actor
    actor = (
        goray.golang_actor_class("Counter", num_cpus=1)
        .options(name="myactor")
        .remote(20)
    )
    obj = actor.Incr.remote(10)
    obj = actor.Incr.options().remote(obj)
    print(f"{ray.get(obj)=}")

    # python获取go named ray actor句柄
    a = goray.get_golang_actor("myactor")
    obj = a.Incr.remote(10)
    print(f"{ray.get(obj)=}")

    # python在当前进程内调用golang函数
    goray.golang_local_run_task("CallPython")
    goray.golang_local_run_task("RemoteCallPython")

    # python在当前进程内调初始化golang类并调用其方法
    actor = goray.golang_local_new_actor("Counter", 20)
    print(f"{actor.Incr(10)=}")


if __name__ == "__main__":
    import os

    here = os.path.dirname(os.path.abspath(__file__))
    goray.init(f"{here}/../out/raytask")
    main()
