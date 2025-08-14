import logging

import ray

from . import common
from .. import funccall
from .. import state
from ..consts import *

logger = logging.getLogger(__name__)


def run_task(
    func_id: int,
    raw_args: bytes,
    object_positions: list[int],
    *object_refs: tuple[bytes, int],
) -> tuple[bytes, int]:
    data, err = funccall.pack_golang_funccall_data(
        raw_args, object_positions, *object_refs
    )
    if err != 0:
        return data, err

    try:
        res, code = common.load_go_lib().execute(Py2GoCmd.CMD_RUN_TASK, func_id, data)
        logger.debug(f"[py] local_run_task {func_id=}, {res=} {code=}")
    except Exception as e:
        logging.exception(f"[py] execute error {e}")
        return (
            f"[goray error] python ffi.execute() error: {e}".encode("utf-8"),
            ErrCode.Failed,
        )
    return res, code


def handle_run_remote_task(data: bytes, func_id: int, mock=False) -> tuple[bytes, int]:
    args_data, options, object_positions, object_refs = funccall.decode_funccall_args(
        data
    )
    logger.debug(f"[py] run remote task {func_id}, {options=}, {object_positions=}")
    common.inject_runtime_env(options)
    task_name = options.pop("goray_task_name", None)
    if mock:
        fut = run_task(func_id, args_data, object_positions, *object_refs)
    else:
        task_func = ray.remote(
            common.copy_function(run_task, task_name or "task", "Go")
        )
        fut = task_func.options(**options).remote(
            func_id, args_data, object_positions, *object_refs
        )
    fut_local_id = state.futures.add(fut)
    # show.remote(fut)
    return str(fut_local_id).encode(), 0
