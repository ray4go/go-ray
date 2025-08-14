import inspect
import io
import logging

import msgpack
import ray

from . import common
from . import registry
from .. import funccall
from .. import state
from .. import utils
from ..consts import *

logger = logging.getLogger(__name__)
utils.init_logger(logger)


def ray_run_task_from_go(
    func_name: str,
    raw_args: bytes,
    object_positions: list[int],
    *object_refs: list[tuple[bytes, int]],
) -> tuple[bytes, int]:
    common.load_go_lib()
    # wired, run_task can't access global _user_tasks_actors, so we pass it as an argument
    return run_task(
        func_name,
        raw_args,
        object_positions,
        *object_refs,
    )


def decode_args(
    raw_args: bytes, object_positions: list[int], object_refs: list[tuple[bytes, int]]
) -> list:
    reader = io.BytesIO(raw_args)
    unpacker = msgpack.Unpacker(reader)
    args = []
    for unpacked in unpacker:
        args.append(unpacked)

    for idx, (raw_res, code) in zip(object_positions, object_refs):
        if code != 0:  # ray task for this object failed
            origin_err_msg = raw_res.decode("utf-8")
            err_msg = f"ray task for the object in {idx}th argument error[{ErrCode(code).name}]: {origin_err_msg}"
            raise Exception(err_msg)
        args.insert(idx, msgpack.unpackb(raw_res))
    return args


def run_task(
    func_name: str,
    raw_args: bytes,
    object_positions: list[int],
    *object_refs: tuple[bytes, int],
) -> tuple[bytes, int]:
    func_and_opts = registry.get_user_tasks_or_actors(func_name)
    if func_and_opts is None:
        return f"[py] task {func_name} not found".encode("utf-8"), ErrCode.Failed
    func, _ = func_and_opts
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

    task_and_opts = registry.get_user_tasks_or_actors(func_name)
    if task_and_opts is None:
        return utils.error_msg(f"python task {func_name} not found, all tasks and actors: {registry.all_user_tasks_or_actors()}"), ErrCode.Failed
    _, opts = task_and_opts
    opts = dict(opts)
    opts.update(options)
    common.inject_runtime_env(options)
    if mock:
        fut = run_task(func_name, args_data, object_positions, *object_refs)
    else:
        task_func = ray.remote(
            common.copy_function(ray_run_task_from_go, func_name, "py")
        )
        fut = task_func.options(**opts).remote(
            func_name, args_data, object_positions, *object_refs
        )
    fut_local_id = state.futures.add(fut)
    return str(fut_local_id).encode(), 0


class PyActorWrapper:

    def __init__(
        self,
        class_name: str,
        raw_args: bytes,
        object_positions: list[int],
        *object_refs: tuple[bytes, int],
    ):
        common.load_go_lib()

        cls_and_opts = registry.get_user_tasks_or_actors(class_name)
        if not cls_and_opts:
            raise Exception(f"python actor {class_name} not found, all tasks and actors: {registry.all_user_tasks_or_actors()}")
        cls, opts = cls_and_opts
        if not inspect.isclass(cls):
            raise Exception(f"python actor {class_name} not found")

        args = decode_args(raw_args, object_positions, object_refs)
        self._actor = cls(*args)

    def method(
        self,
        method_name: str,
        raw_args: bytes,
        object_positions: list[int],
        *object_refs: tuple[bytes, int],
    ) -> tuple[bytes, int]:
        args = decode_args(raw_args, object_positions, object_refs)
        try:
            res = getattr(self._actor, method_name)(*args)
        except Exception as e:
            logging.exception(f"[py] execute error {e}")
            return (
                f"[goray error] python run task error: {e}".encode("utf-8"),
                ErrCode.Failed,
            )
        return msgpack.packb(res, use_bin_type=True), ErrCode.Success

    def get_go_class_index(self):
        return -1


def handle_new_py_actor(data: bytes, _: int, mock=False) -> tuple[bytes, int]:
    args_data, options, object_positions, object_refs = funccall.decode_funccall_args(
        data
    )
    class_name = options.pop("actor_class_name")

    cls_and_opts = registry.get_user_tasks_or_actors(class_name)
    if not cls_and_opts:
        return f"python actor {class_name} not found".encode("utf-8"), ErrCode.Failed
    cls, opts = cls_and_opts
    if not inspect.isclass(cls):
        return f"python actor {class_name} not found".encode("utf-8"), ErrCode.Failed

    common.inject_runtime_env(options)
    ActorCls = ray.remote(common.copy_class(PyActorWrapper, class_name, "py"))
    actor_handle = ActorCls.options(**options).remote(
        class_name, args_data, object_positions, *object_refs
    )
    actor_local_id = state.actors.add(actor_handle)
    return str(actor_local_id).encode(), 0
