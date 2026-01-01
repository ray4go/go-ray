import logging

import ray

from gorayffi.consts import *
from . import common
from . import state

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


def handle_run_remote_task(data: bytes) -> tuple[bytes, int]:
    args_data, options, object_positions, object_refs = (
        common.decode_remote_func_call_args(data)
    )
    task_name = options.pop(TASK_NAME_OPTION_KEY)

    logger.debug(f"[py] run remote task {task_name}, {options=}, {object_positions=}")
    common.inject_runtime_env(options)

    task_func = ray.remote(
        common.copy_function(run_task, task_name, namespace="GolangTask")
    )
    fut = task_func.options(**options).remote(
        task_name, args_data, object_positions, *object_refs
    )
    fut_local_id = state.futures.add(fut)
    return common.uint64_le_packer.pack(fut_local_id), 0
