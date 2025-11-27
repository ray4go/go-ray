import functools
import logging
import sys

import ray
from . import consts, state, utils, x
from .raycore import common
from .raycore import py2go
from .raycore import registry


def init(
        libpath: str,
        py_module_path: str = "",
        **ray_init_args: dict,
):
    """
    Initialize GoRay and the ray environment.

    :param libpath: path to the goray application binary (built from `go build -buildmode=c-shared`).
    :param py_module_path: path to the python module to import python ray tasks and actors.
    :param ray_init_args: arguments to pass to [ray.init](https://docs.ray.io/en/latest/ray-core/api/doc/ray.init.html#ray.init).
    """
    debug = False
    state.golibpath = libpath
    state.pymodulepath = py_module_path
    state.debug = debug

    ray_init_args.setdefault("runtime_env", {})
    ray_init_args["runtime_env"].setdefault("env_vars", {})
    ray_init_args["runtime_env"]["env_vars"][consts.GORAY_BIN_PATH_ENV] = libpath
    ray_init_args["runtime_env"]["env_vars"][
        consts.GORAY_PY_MUDULE_PATH_ENV
    ] = py_module_path
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
        return registry.make_remote(args[0], {})
    return functools.partial(registry.make_remote, options=kwargs)


def golang_actor_class(name: str, **options):
    """
    Get a golang actor class, which you can use to create a golang actor.
    The usage of returned class is same as python actor class.

    Example:

    ```python
    Counter = golang_actor_class("Counter", max_restarts=-1, max_task_retries=-1)
    counter = Counter.remote(...)
    counter.Inc.remote(1)
    obj = counter.Get.remote()
    print(ray.get(obj))
    ```
    :param name: golang actor type name.
    :param options: [`ray.actor.ActorClass.options`](https://docs.ray.io/en/latest/ray-core/api/doc/ray.actor.ActorClass.options.html#ray.actor.ActorClass.options)
    """
    return py2go.GolangActorClass(name, **options)


def golang_task(name: str, **options):
    """
    Get a golang task, which you can run the task remotely.

    The returned task can be used in same way as python ray task function.

    Example:
    ```python
    task = golang_task("task_name")
    obj = task.remote(...)
    print(ray.get(obj))
    ```
    """
    return py2go.get_golang_remote_task(name, options)


def golang_local_run_task(name: str, *args):
    """
    Run a golang task locally (in current process).
    Unlike remote tasks, this function is blocking and returns the result directly.
    """
    lib = common.load_go_lib()
    return x.CrossLanguageClient(lib).func_call(name, *args)


def golang_local_new_actor(name: str, *args) -> x.GolangLocalActor:
    """
    Create a golang actor locally (in current process).

    Unlike remote actors, the method calls of local actor are blocking and return the result directly.

    Example:
    ```python
    actor = golang_local_new_actor("Counter", 0)
    actor.Inc(1)
    print(actor.Get())
    ```
    """
    lib = common.load_go_lib()
    return x.CrossLanguageClient(lib).new_type(name, *args)
