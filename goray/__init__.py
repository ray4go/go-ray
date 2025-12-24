import functools
import logging

import ray
import gorayffi
import gorayffi.actor
from gorayffi import consts
import gorayffi.handlers
from . import state
from .raycore import common
from .raycore import py2go
from .raycore import registry


def start(
    libpath: str,
    py_defs_path: str = "",
    **ray_init_args: dict,
):
    """
    Initialize GoRay and Ray environment. Start the Go driver function.

    :param libpath: path to the goray application binary (built from `go build -buildmode=c-shared`).
    :param py_defs_path: path to a python file to import python ray tasks and actors.
    :param ray_init_args: arguments to pass to [`ray.init()`](https://docs.ray.io/en/latest/ray-core/api/doc/ray.init.html#ray.init).
       The arguments here will overwrite the ones provided in golang side (via [`ray.Init()`](https://pkg.go.dev/github.com/ray4go/go-ray/ray#Init)) per key.
    :return: int, the return code of the driver function.
    """
    state.golibpath = libpath
    state.pymodulepath = py_defs_path
    state.debug = False  # not expose to user yet

    lib = common.load_go_lib()
    init_args = lib.get_init_options()
    init_args.update(ray_init_args)

    # inject goray runtime env
    init_args.setdefault("runtime_env", {})
    init_args["runtime_env"].setdefault("env_vars", {})
    init_args["runtime_env"]["env_vars"][consts.GORAY_BIN_PATH_ENV] = libpath
    init_args["runtime_env"]["env_vars"][consts.GORAY_PY_MUDULE_PATH_ENV] = py_defs_path
    if state.debug:
        init_args["runtime_env"]["env_vars"]["GORAY_DEBUG_LOGGING"] = "1"

    ray.init(**init_args)

    msg, code = lib.start_driver()
    if code != 0:
        logging.error(f"go driver error[{code}]: {msg}")
        return code
    else:
        return int(msg)


def remote(*args, **kwargs):
    """
    Same as @ray.remote, plus registering the task or actor in goray, which can be called from Go side:

    For Ray task, you can call it from Go using:

    - `ray.RemoteCallPyTask(name, args...)`: calls the python Ray task remotely.
    - `ray.LocalCallPyTask(name, args...)`: calls the python Ray task locally (in-process).

    For Ray actor, you can create it from Go using:

    - `ray.NewPyActor(name, args...)`: creates the python Ray actor remotely.
    """
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        # This is the case where the decorator is just @remote.
        # "args[0]" is the class or function under the decorator.
        return registry.make_remote(args[0], {})
    return functools.partial(registry.make_remote, options=kwargs)


def local(func):
    """
    Decorator to register a python function to be called from go.

    The function can be called from Go using `ray.LocalCallPyTask(name, args...) `.
    """
    gorayffi.handlers.export_python_func(func)
    return func


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
    :param options: actor options, see [`ray.actor.ActorClass.options`](https://docs.ray.io/en/latest/ray-core/api/doc/ray.actor.ActorClass.options.html#ray.actor.ActorClass.options)
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
    return lib.call_golang_func(name, args)


def golang_local_new_actor(name: str, *args) -> gorayffi.actor.GolangLocalActor:
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
    return gorayffi.actor.GolangLocalActor(lib, name, *args)
