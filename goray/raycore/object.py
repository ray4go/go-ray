import json
import logging

import ray

from .. import consts, state

logger = logging.getLogger(__name__)


def handle_get_objects(data: bytes, _: int) -> tuple[bytes, int]:
    fut_local_id, timeout = json.loads(data)
    if fut_local_id not in state.futures:
        return b"object_ref not found!", 1

    obj_ref = state.futures[
        fut_local_id
    ]  # todo: consider to pop it to avoid memory leak
    logger.debug(f"[Py] get obj {obj_ref.hex()}")
    if timeout < 0:
        timeout = None
    try:
        res, code = ray.get(obj_ref, timeout=timeout)
    except ray.exceptions.GetTimeoutError:
        return b"timeout to get object", consts.ErrCode.Timeout
    except ray.exceptions.TaskCancelledError:
        return b"task cancelled", consts.ErrCode.Cancelled
    return res, code


def handle_put_object(data: bytes, _: int) -> tuple[bytes, int]:
    fut = ray.put([data, 0])
    # side effect: make future outlive this function (on purpose)
    fut_local_id = state.futures.add(fut)
    return str(fut_local_id).encode(), 0


def handle_wait_object(data: bytes, _: int) -> tuple[bytes, int]:
    opts = json.loads(data)
    fut_local_ids = opts.pop("object_ref_local_ids")

    futs = []
    fut_hex2idx = {}
    for idx, fut_local_id in enumerate(fut_local_ids):
        if fut_local_id not in state.futures:
            return b"object_ref not found!", 1
        fut = state.futures[fut_local_id]
        futs.append(fut)
        fut_hex2idx[fut.hex()] = idx

    ready, not_ready = ray.wait(futs, **opts)

    ready_ids = [fut_hex2idx[i.hex()] for i in ready]
    not_ready_ids = [fut_hex2idx[i.hex()] for i in not_ready]
    ret_data = json.dumps([ready_ids, not_ready_ids]).encode()
    return ret_data, 0


def handle_cancel_object(data: bytes, _: int) -> tuple[bytes, int]:
    opts = json.loads(data)
    fut_local_id = opts.pop("object_ref_local_id")
    if fut_local_id not in state.futures:
        return b"object_ref not found!", 1
    fut = state.futures[fut_local_id]
    ray.cancel(fut, **opts)
    return b"", 0
