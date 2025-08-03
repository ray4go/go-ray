import ray
from .. import funccall
from ..consts import *
import logging
from . import common
from .. import state


logger = logging.getLogger(__name__)


@ray.remote
def ray_run_task(
    func_id: int,
    raw_args: bytes,
    object_positions: list[int],
    *object_refs: list[tuple[bytes, int]],
) -> tuple[bytes, int]:
    """
    :param func_id:
    :param data:
    :param object_positions: 调用的go task func中, 传入的object_ref作为参数的位置列表, 有序
    :param object_refs: object_ref列表, 会被ray core替换为实际的数据
    :return:
    """
    return run_task(func_id, raw_args, object_positions, *object_refs)


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
    if mock:
        fut = run_task(func_id, args_data, object_positions, *object_refs)
    else:
        fut = ray_run_task.options(**options).remote(
            func_id, args_data, object_positions, *object_refs
        )
    fut_local_id = state.futures.add(fut)
    # show.remote(fut)
    return str(fut_local_id).encode(), 0
