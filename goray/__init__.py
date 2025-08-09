import functools
import logging
import sys
import ray

from . import x
from . import state
from . import utils
from .raycore import common
from .raycore import go2py, py2go, common


def init(libpath: str, ray_init_args: dict = None, debug=False):
    ray_init_args = ray_init_args or {}

    state.golibpath = libpath
    state.debug = debug

    ray_init_args.setdefault("runtime_env", {})
    ray_init_args["runtime_env"].setdefault("env_vars", {})
    ray_init_args["runtime_env"]["env_vars"]["GORAY_BIN_PATH"] = libpath
    if debug:
        ray_init_args["runtime_env"]["env_vars"]["GORAY_DEBUG_LOGGING"] = "1"

    ray.init(**ray_init_args)

    lib = common.load_go_lib()

    try:
        msg, code = lib.start_driver()
        if code != 0:
            logging.error(f"[py] driver error[{code}]: {msg}")
            sys.exit(1)
        else:
            sys.exit(int(msg))
    except KeyboardInterrupt:
        print("Exiting...")


def remote(*args, **kwargs):
    """
    Same as @ray.remote, but with registering the task or actor in goray, which you can call from go.
    """
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        # This is the case where the decorator is just @remote.
        # "args[0]" is the class or function under the decorator.
        return go2py.make_remote(args[0], {})
    return functools.partial(go2py.make_remote, options=kwargs)


def golang_actor_class(name: str, **options) -> py2go.GolangActorClass:
    return py2go.GolangActorClass(name, **options)


def get_golang_actor(name: str):
    return py2go.GolangRemoteActorHandle(ray.get_actor(name))


def golang_task(name: str) -> py2go.GolangRemoteFunc:
    return py2go.golang_task(name)


def golang_local_run_task(name: str, *args):
    lib = common.load_go_lib()
    return x.CrossLanguageClient(lib).func_call(name, *args)


def golang_local_new_actor(name: str, *args) -> x.GolangLocalActor:
    lib = common.load_go_lib()
    return x.CrossLanguageClient(lib).new_type(name, *args)
