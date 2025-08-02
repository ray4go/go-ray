from . import api


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
