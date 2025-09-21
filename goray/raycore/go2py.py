import logging

import msgpack
import ray

from . import common, registry, actor_wrappers
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
    ActorCls = actor_wrappers.new_remote_actor_type(
        actor_wrappers.PyActor,
        class_name,
        method_names,
        namespace=TaskActorSource.Py2Go,
    )
    actor_handle = ActorCls.options(**options).remote(
        class_name, args_data, object_positions, *object_refs
    )
    actor_local_id = state.actors.add(actor_handle)
    return common.uint64_le_packer.pack(actor_local_id), ErrCode.Success
