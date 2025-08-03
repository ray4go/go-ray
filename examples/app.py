import pydriver
import ray


@pydriver.remote(num_cpus=1)
def echo(*args) -> list:
    print(f"python echo receive: {args}")
    return list(args)

@pydriver.remote
def hello(name: str):
    return f"hello {name}"

@pydriver.remote
def no_return(name: str):
    return

@pydriver.remote
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
    print("CallPython", '='*80)
    pydriver.golang_local_run_task("CallPython")

    print("RemoteCallPython", '='*80)
    pydriver.golang_local_run_task("RemoteCallPython")

    print("Pyhton call go", '='*80)
    hello_task = pydriver.golang_task("Hello")

    # obj = api.golang_remote_task("Hello", ("wang",), {})
    obj = hello_task.remote("wang")
    print(f"{ray.get(obj)=}")

    for _ in range(3):
        actor = pydriver.golang_local_new_actor("Counter", 20)
        print(actor)
        print(f"{actor.Incr(10)=}")
        # print(f"{actor.Incr(10)=}")

    actor = pydriver.golang_actor_class("Counter", num_cpus=1).options(name="myactor").remote(20)
    obj = actor.Incr.remote(10)
    obj = actor.Incr.options().remote(obj)
    print(f"{ray.get(obj)=}")

    a = pydriver.get_golang_actor("myactor")
    obj = a.Incr.remote(10)
    print(f"{ray.get(obj)=}")


if __name__ == "__main__":
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    pydriver.init(f"{here}/../out/raytask")
    main()
