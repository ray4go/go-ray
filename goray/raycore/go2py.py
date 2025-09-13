import logging

import msgpack
import ray

from . import common, registry
from .. import funccall, state, utils
from ..consts import *
from ..x import handlers as x_handlers

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



def run_task(
    func_name: str,
    raw_args: bytes,
    object_positions: list[int],
    *object_refs: tuple[bytes, int],
) -> tuple[bytes, int]:
    func, _ = registry.get_py_task(func_name)
    if func is None:
        return f"[py] task {func_name} not found".encode("utf-8"), ErrCode.Failed
    args = x_handlers.decode_args(raw_args, object_positions, object_refs)

    try:
        res = func(*args)
    except Exception as e:
        logging.exception(f"[py] execute error {e}")
        return (
            f"[goray error] python run task error: {e}".encode("utf-8"),
            ErrCode.Failed,
        )

    return msgpack.packb(res, use_bin_type=True), ErrCode.Success


def handle_run_py_task(data: bytes) -> tuple[bytes, int]:
    args_data, options, object_positions, object_refs = funccall.decode_funccall_args(
        data
    )
    func_name = options.pop("task_name")
    func, opts = registry.get_py_task(func_name)
    if func is None:
        return (
            utils.error_msg(
                f"python task {func_name} not found, all py tasks: {registry.all_py_tasks()}"
            ),
            ErrCode.Failed,
        )
    opts = dict(opts)
    opts.update(options)
    common.inject_runtime_env(options)
    task_func = ray.remote(common.copy_function(ray_run_task_from_go, func_name, "py"))
    fut = task_func.options(**opts).remote(
        func_name, args_data, object_positions, *object_refs
    )
    fut_local_id = state.futures.add(fut)
    return common.uint64_le_packer.pack(fut_local_id), 0


class PyActorWrapper:
    """Python actor wrapper for golang remote call"""

    def __init__(
        self,
        class_name: str,
        raw_args: bytes,
        object_positions: list[int],
        *object_refs: tuple[bytes, int],
    ):
        common.load_go_lib()

        cls, opts = registry.get_py_actor(class_name)
        if cls is None:
            raise Exception(
                f"python actor {class_name} not found, all py actors: {registry.all_py_actors()}"
            )

        args = x_handlers.decode_args(raw_args, object_positions, object_refs)
        self._actor = cls(*args)

    def call_method(
        self,
        method_name: str,
        raw_args: bytes,
        object_positions: list[int],
        *object_refs: tuple[bytes, int],
    ) -> tuple[bytes, int]:
        args = x_handlers.decode_args(raw_args, object_positions, object_refs)
        try:
            res = getattr(self._actor, method_name)(*args)
        except Exception as e:
            logging.exception(f"[py] execute error {e}")
            return (
                f"[goray error] python run task error: {e}".encode("utf-8"),
                ErrCode.Failed,
            )
        return msgpack.packb(res, use_bin_type=True), ErrCode.Success


def handle_new_py_actor(data: bytes) -> tuple[bytes, int]:
    args_data, options, object_positions, object_refs = funccall.decode_funccall_args(
        data
    )
    class_name = options.pop(ACTOR_NAME_OPTION_KEY)

    cls, opts = registry.get_py_actor(class_name)
    if cls is None:
        return utils.error_msg(f"python actor {class_name} not found"), ErrCode.Failed

    common.inject_runtime_env(options)
    method_names = common.get_class_methods(cls)
    methods = {name: PyActorWrapper.call_method for name in method_names}
    ActorCls = ray.remote(
        common.copy_class(
            PyActorWrapper, class_name, namespace=TaskActorSource.Py2Go, **methods
        )
    )
    actor_handle = ActorCls.options(**options).remote(
        class_name, args_data, object_positions, *object_refs
    )
    actor_local_id = state.actors.add(actor_handle)
    return common.uint64_le_packer.pack(actor_local_id), ErrCode.Success
