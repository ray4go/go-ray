import threading
import time

import goray


@goray.remote(num_cpus=1000)
def overwrite_ray_options():
    pass

@goray.remote
def echo(*args) -> tuple:
    return args

@goray.remote
def single(arg):
    return arg


@goray.remote
def hello(name: str):
    return f"hello {name}"


@goray.remote
def no_return(name: str):
    return

@goray.remote
def no_args():
    return 42

@goray.remote
def busy(sec: int) -> int:
    time.sleep(sec)
    return threading.current_thread().ident


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

    def single(self,arg):
        return arg

    def busy(self, sec: int) -> int:
        time.sleep(sec)
        return sec

    def no_return(self,name: str):
        return

    def no_args(self):
        return 42
