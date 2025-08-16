import logging

import ray

from .. import funccall, state
from ..consts import *
from . import common

logger = logging.getLogger(__name__)


def run_task(
    func_name: str,
    raw_args: bytes,
    object_positions: list[int],
    *resolved_object_refs: tuple[bytes, int],
) -> tuple[bytes, int]:
    """run task, get error code and encoded result bytes"""
    return common.load_go_lib().raw_call_golang_func(
        func_name, raw_args, object_positions, *resolved_object_refs
    )


def handle_run_remote_task(data: bytes, _: int) -> tuple[bytes, int]:
    args_data, options, object_positions, object_refs = funccall.decode_funccall_args(
        data
    )
    task_name = options.pop(TASK_NAME_OPTION_KEY)

    logger.debug(f"[py] run remote task {task_name}, {options=}, {object_positions=}")
    common.inject_runtime_env(options)

    task_func = ray.remote(
        common.copy_function(run_task, task_name, namespace=Language.PYTHON)
    )
    fut = task_func.options(**options).remote(
        task_name, args_data, object_positions, *object_refs
    )
    fut_local_id = state.futures.add(fut)
    # show.remote(fut)
    return str(fut_local_id).encode(), 0
