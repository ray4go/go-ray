import functools
import io
import logging

import msgpack
import ray

from . import funccall
from . import state
from . import utils
from .consts import *

logger = logging.getLogger(__name__)
utils.init_logger(logger)

# name -> (func_or_class, options)
_user_tasks_actors = {}


def _make_remote(function_or_class, options: dict):
    print(f"add_task {function_or_class} {options=}")
    _user_tasks_actors[function_or_class.__name__] = (function_or_class, options)
    return ray.remote(**options)(function_or_class)


def remote(*args, **kwargs):
    """
    Same as @ray.remote, but with registering the task or actor in goray, which you can call from go.
    """
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        # This is the case where the decorator is just @remote.
        # "args[0]" is the class or function under the decorator.
        return _make_remote(args[0], {})
    return functools.partial(_make_remote, options=kwargs)


@ray.remote
def ray_run_task_from_go(
    func_name: str,
    raw_args: bytes,
    object_positions: list[int],
    *object_refs: list[tuple[bytes, int]],
) -> tuple[bytes, int]:
    # very wierd, the _user_tasks_actors can be access with value in here,
    # while become empty inside run_task()
    return run_task(func_name, raw_args, object_positions, *object_refs, tasks=_user_tasks_actors)


def decode_args(
    raw_args: bytes, object_positions: list[int], object_refs: list[tuple[bytes, int]]
) -> list:
    reader = io.BytesIO(raw_args)
    unpacker = msgpack.Unpacker(reader)
    args = []
    for unpacked in unpacker:
        args.append(unpacked)

    print("decode_args", args)
    return args


def run_task(
    func_name: str,
    raw_args: bytes,
    object_positions: list[int],
    *object_refs: tuple[bytes, int],
    tasks=None,
) -> tuple[bytes, int]:
    global _user_tasks_actors
    if not tasks:
        tasks = _user_tasks_actors

    data, err = funccall.pack_golang_funccall_data(
        raw_args, object_positions, *object_refs
    )
    if err != 0:
        return data, err

    print(f"run_task {func_name}, all task {tasks}")
    # print(f"run_task {func_name}, all task {_user_tasks_actors}")
    func, _ = tasks.get(func_name)
    if func is None:
        return f"[py] task {func_name} not found".encode("utf-8"), ErrCode.Failed

    args = decode_args(raw_args, object_positions, object_refs)

    try:
        res = func(*args)
    except Exception as e:
        logging.exception(f"[py] execute error {e}")
        return (
            f"[goray error] python run task error: {e}".encode("utf-8"),
            ErrCode.Failed,
        )

    return msgpack.packb(res, use_bin_type=True), ErrCode.Success


def handle_run_py_task(data: bytes, _: int, mock=False) -> tuple[bytes, int]:
    args_data, options, object_positions, object_refs = funccall.decode_funccall_args(
        data
    )
    func_name = options.pop("task_name")
    _, opts = _user_tasks_actors.get(func_name)
    opts = dict(opts)
    opts.update(options)
    if mock:
        fut = run_task(func_name, args_data, object_positions, *object_refs)
    else:
        fut = ray_run_task_from_go.options(**opts).remote(
            func_name, args_data, object_positions, *object_refs
        )
    fut_local_id = state.futures.add(fut)
    # show.remote(fut)
    return str(fut_local_id).encode(), 0


class GolangActor:
    def __getattr__(self, name) -> 'GolangRemoteFunc':
        return GolangRemoteFunc()

class GolangActorClass:
    def __init__(self, *args, **kwargs):
        pass
    def options(self, **kwargs) -> 'GolangActorClass':
        pass
    def remote(self, *args, **kwargs) -> GolangActor:
        pass

class GolangRemoteFunc:
    def __init__(self, *args, **kwargs):
        pass
    def options(self, **kwargs) -> 'GolangRemoteFunc':
        pass
    def remote(self, *args, **kwargs):
        pass


def golang_actor_class(name: str) -> GolangActorClass:
    pass

def golang_task(name: str) -> GolangRemoteFunc:
    pass